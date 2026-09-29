//! Sample-first spans. tracing-opentelemetry builds a span builder (attributes,
//! timings) for every span it sees, and an SDK span when the span is first
//! entered, and only then does the SDK's sampler drop 99% of them. Here a
//! trace is decided once, when its root span is created, by the configured
//! sampler and for the trace id the SDK will build that root with, so the
//! OpenTelemetry layer only ever sees the spans of sampled traces: the SDK
//! makes the same decision again for the same trace id, and children follow
//! their parent. A span of an unsampled trace keeps, in the registry, the ids
//! its non-recording SDK span would have had, which outgoing traceparent
//! headers and batch links use (see [`context_of`]).

use std::{
    cell::{Cell, RefCell},
    sync::atomic::{AtomicUsize, Ordering},
};

use opentelemetry::{
    Context,
    trace::{SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use opentelemetry_sdk::trace::{
    IdGenerator, RandomIdGenerator, Sampler, SamplingDecision, ShouldSample,
};
use tracing::span;
use tracing_subscriber::{Layer, layer, registry::LookupSpan};

/// Marks a span of a sampled trace, which the OpenTelemetry layer builds. A
/// root holds the new trace id it was decided with (a root under a remote
/// parent takes that parent's), for the SDK to build it with.
struct Recorded(Option<TraceId>);

/// A span of an unsampled trace: the ids its non-recording SDK span would carry.
#[derive(Clone)]
struct Unsampled {
    trace_id: TraceId,
    span_id: SpanId,
    trace_state: TraceState,
}

impl Unsampled {
    fn span_context(&self) -> SpanContext {
        SpanContext::new(
            self.trace_id,
            self.span_id,
            TraceFlags::default(),
            false,
            self.trace_state.clone(),
        )
    }
}

/// Spans of sampled traces not yet closed. While there are none, which is
/// most of the time at a 1% ratio, entering, leaving, recording on and closing
/// a span need not look it up. A span handed to another thread was created
/// before that, so the thread sees its count.
static RECORDED: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The remote parent (a request's traceparent) of the span being created.
    static REMOTE_PARENT: RefCell<Option<SpanContext>> = const { RefCell::new(None) };
    /// The trace id the SDK must give a root it builds now (PresampledIds::new_trace_id).
    static FORCED_TRACE: Cell<Option<TraceId>> = const { Cell::new(None) };
}

/// Creates a span whose parent is the remote `parent` when that carries a
/// valid span context: the span's trace is decided by that parent, as the SDK
/// decides it once `set_parent` gives the OpenTelemetry layer the same parent.
pub(super) fn with_remote_parent(
    parent: &Context,
    create: impl FnOnce() -> tracing::Span,
) -> tracing::Span {
    let span_context = parent.span().span_context().clone();
    if !span_context.is_valid() {
        return create();
    }
    REMOTE_PARENT.with(|remote| *remote.borrow_mut() = Some(span_context));
    let span = create();
    REMOTE_PARENT.with(|remote| remote.borrow_mut().take());
    span
}

/// The SDK's random ids, except for the trace id of a presampled root that
/// SampleFirst lets the OpenTelemetry layer build (see [`forcing`]).
#[derive(Debug, Default)]
pub(super) struct PresampledIds(RandomIdGenerator);

impl IdGenerator for PresampledIds {
    fn new_trace_id(&self) -> TraceId {
        FORCED_TRACE
            .with(Cell::take)
            .unwrap_or_else(|| self.0.new_trace_id())
    }

    fn new_span_id(&self) -> SpanId {
        self.0.new_span_id()
    }
}

/// Runs `f`, a call into the OpenTelemetry layer that may build the SDK span
/// of a root decided with `trace_id`, so that the SDK builds it with that id.
/// The SDK asks for a trace id only when it builds a span without a parent,
/// and the layer builds at most one span per call: the span it is called for
/// (on_enter, on_close, context), a new span's explicit parent (on_new_span),
/// or the span another follows (on_follows_from).
fn forcing<R>(trace_id: Option<TraceId>, f: impl FnOnce() -> R) -> R {
    if trace_id.is_none() {
        return f();
    }
    FORCED_TRACE.with(|slot| slot.set(trace_id));
    let result = f();
    FORCED_TRACE.with(|slot| slot.set(None));
    result
}

/// Decides the trace of each span before the OpenTelemetry layer `L` sees it
/// (see the module's documentation).
pub(super) struct SampleFirst<L> {
    inner: L,
    sampler: Sampler,
    ids: RandomIdGenerator,
}

impl<L> SampleFirst<L> {
    pub(super) fn new(inner: L, sampler: Sampler) -> Self {
        Self {
            inner,
            sampler,
            ids: RandomIdGenerator::default(),
        }
    }

    /// A root's decision, by the configured sampler for the trace id the SDK
    /// will build it with: the remote parent's, or a new one. `Ok` holds the
    /// new trace id.
    fn root(&self, name: &str, remote: Option<SpanContext>) -> Result<Option<TraceId>, Unsampled> {
        let (parent, trace_id, fresh) = match remote {
            Some(remote) => {
                let trace_id = remote.trace_id();
                (
                    Some(Context::new().with_remote_span_context(remote)),
                    trace_id,
                    false,
                )
            }
            None => (None, self.ids.new_trace_id(), true),
        };
        let result = self.sampler.should_sample(
            parent.as_ref(),
            trace_id,
            name,
            &SpanKind::Internal,
            &[],
            &[],
        );
        match result.decision {
            SamplingDecision::Drop => Err(Unsampled {
                trace_id,
                span_id: self.ids.new_span_id(),
                trace_state: result.trace_state,
            }),
            SamplingDecision::RecordOnly | SamplingDecision::RecordAndSample => {
                Ok(fresh.then_some(trace_id))
            }
        }
    }

    /// `None` for a span the OpenTelemetry layer does not build, else the
    /// trace id to build it with, if it is a root.
    fn recorded<S>(id: &span::Id, ctx: &layer::Context<'_, S>) -> Option<Option<TraceId>>
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        if RECORDED.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let span = ctx.span(id)?;
        let extensions = span.extensions();
        extensions.get::<Recorded>().map(|recorded| recorded.0)
    }
}

impl<S, L> Layer<S> for SampleFirst<L>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    L: Layer<S>,
{
    fn on_register_dispatch(&self, subscriber: &tracing::Dispatch) {
        self.inner.on_register_dispatch(subscriber);
    }

    fn on_layer(&mut self, subscriber: &mut S) {
        self.inner.on_layer(subscriber);
    }

    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        self.inner.register_callsite(metadata)
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>, ctx: layer::Context<'_, S>) -> bool {
        self.inner.enabled(metadata, ctx)
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        self.inner.max_level_hint()
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        // A remote parent replaces the local one, as set_parent does in the OpenTelemetry layer.
        let remote = REMOTE_PARENT.with(|remote| remote.borrow_mut().take());
        let mut explicit_parent_root = None;
        let decision = if remote.is_some() {
            self.root(attrs.metadata().name(), remote)
        } else {
            // The parent the OpenTelemetry layer would see (the per-layer filter hides others).
            let parent = match attrs.parent() {
                Some(parent) => ctx.span(parent),
                None if attrs.is_contextual() => ctx.lookup_current(),
                None => None,
            };
            let inherited = parent.map(|parent| {
                let extensions = parent.extensions();
                match extensions.get::<Unsampled>() {
                    Some(unsampled) => Err(unsampled.clone()),
                    None => Ok(extensions.get::<Recorded>().map(|recorded| recorded.0)),
                }
            });
            match inherited {
                // A child of an unsampled span has its trace's ids and nothing more.
                Some(Err(unsampled)) => Err(Unsampled {
                    span_id: self.ids.new_span_id(),
                    ..unsampled
                }),
                Some(Ok(Some(root))) => {
                    // The OpenTelemetry layer builds an explicit parent it has not built yet.
                    if attrs.parent().is_some() {
                        explicit_parent_root = root;
                    }
                    Ok(None)
                }
                Some(Ok(None)) | None => self.root(attrs.metadata().name(), None),
            }
        };
        match decision {
            Err(unsampled) => span.extensions_mut().insert(unsampled),
            Ok(trace_id) => {
                span.extensions_mut().insert(Recorded(trace_id));
                RECORDED.fetch_add(1, Ordering::Relaxed);
                drop(span);
                forcing(explicit_parent_root, || {
                    self.inner.on_new_span(attrs, id, ctx)
                });
            }
        }
    }

    // The OpenTelemetry layer visits recorded values before it looks for its
    // own data, so only the spans it built reach it.
    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: layer::Context<'_, S>) {
        if Self::recorded(id, &ctx).is_some() {
            self.inner.on_record(id, values, ctx);
        }
    }

    fn on_follows_from(&self, id: &span::Id, follows: &span::Id, ctx: layer::Context<'_, S>) {
        if Self::recorded(id, &ctx).is_some() {
            let followed = Self::recorded(follows, &ctx).flatten();
            forcing(followed, || self.inner.on_follows_from(id, follows, ctx));
        }
    }

    fn event_enabled(&self, event: &tracing::Event<'_>, ctx: layer::Context<'_, S>) -> bool {
        self.inner.event_enabled(event, ctx)
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: layer::Context<'_, S>) {
        let recorded = match event.parent() {
            Some(parent) => Self::recorded(parent, &ctx).is_some(),
            None if event.is_contextual() && RECORDED.load(Ordering::Relaxed) > 0 => ctx
                .lookup_current()
                .is_some_and(|span| span.extensions().get::<Recorded>().is_some()),
            None => false,
        };
        if recorded {
            self.inner.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        if let Some(root) = Self::recorded(id, &ctx) {
            forcing(root, || self.inner.on_enter(id, ctx));
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        if Self::recorded(id, &ctx).is_some() {
            self.inner.on_exit(id, ctx);
        }
    }

    fn on_close(&self, id: span::Id, ctx: layer::Context<'_, S>) {
        if let Some(root) = Self::recorded(&id, &ctx) {
            forcing(root, || self.inner.on_close(id, ctx));
            RECORDED.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn on_id_change(&self, old: &span::Id, new: &span::Id, ctx: layer::Context<'_, S>) {
        self.inner.on_id_change(old, new, ctx);
    }

    unsafe fn downcast_raw(&self, id: std::any::TypeId) -> Option<*const ()> {
        if id == std::any::TypeId::of::<Self>() {
            return Some(self as *const Self as *const ());
        }
        // SAFETY: forwarded unchanged; the inner layer answers for its own
        // types, among them the one OpenTelemetrySpanExt looks for.
        unsafe { self.inner.downcast_raw(id) }
    }
}

/// What SampleFirst keeps of a span.
enum Kept {
    Recorded(Option<TraceId>),
    Unsampled(Unsampled),
}

fn kept(span: &tracing::Span) -> Option<Kept> {
    span.with_subscriber(|(id, dispatch)| {
        let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>()?;
        let span = registry.span(id)?;
        let extensions = span.extensions();
        match extensions.get::<Unsampled>() {
            Some(unsampled) => Some(Kept::Unsampled(unsampled.clone())),
            None => extensions
                .get::<Recorded>()
                .map(|recorded| Kept::Recorded(recorded.0)),
        }
    })
    .flatten()
}

/// The OpenTelemetry context of `span`: its SDK span's when its trace is
/// sampled, else a context holding the ids of its unsampled trace, which is
/// what outgoing traceparent headers and batch links need. Use it rather than
/// OpenTelemetrySpanExt::context, which knows nothing of unsampled spans and
/// would build a sampled root that was never entered under a trace id of its
/// own.
pub fn context_of(span: &tracing::Span) -> Context {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    match kept(span) {
        Some(Kept::Unsampled(unsampled)) => {
            Context::new().with_remote_span_context(unsampled.span_context())
        }
        Some(Kept::Recorded(root)) => forcing(root, || span.context()),
        None => span.context(),
    }
}

/// Whether `span` is in a sampled trace, so that what it records is exported
/// (callers may skip recording fields otherwise).
pub fn records(span: &tracing::Span) -> bool {
    if RECORDED.load(Ordering::Relaxed) == 0 {
        return false;
    }
    span.with_subscriber(|(id, dispatch)| {
        let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>()?;
        let span = registry.span(id)?;
        let extensions = span.extensions();
        Some(extensions.get::<Recorded>().is_some())
    })
    .flatten()
    .unwrap_or(false)
}
