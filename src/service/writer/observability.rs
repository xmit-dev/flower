//! Writer telemetry has bounded metric dimensions. Request content, identifiers,
//! principals, and error messages never enter telemetry; resolved method names
//! appear only in sampled traces.
use super::*;
use opentelemetry::{
    KeyValue,
    metrics::{BoundCounter, BoundHistogram, Counter, Histogram},
    trace::TraceContextExt,
};
use std::{cell::RefCell, sync::OnceLock};
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct Instruments {
    stage: Histogram<f64>,
    request: Histogram<f64>,
    requests: Counter<u64>,
    rejected: Counter<u64>,
    batches: Counter<u64>,
    batch_size: Histogram<u64>,
    batch_bytes: Histogram<u64>,
    queued: Histogram<u64>,
    lag: Histogram<u64>,
    speculative: Counter<u64>,
    serial_jobs: Counter<u64>,
    serial_requests: Counter<u64>,
    maintenance_runs: Counter<u64>,
    maintenance_wakes: Counter<u64>,
}

fn instruments() -> Option<&'static Instruments> {
    if !crate::telemetry::enabled() {
        return None;
    }
    static METRICS: OnceLock<Instruments> = OnceLock::new();
    Some(METRICS.get_or_init(|| {
        let meter = opentelemetry::global::meter("flower");
        let seconds = vec![0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025,
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        let counts = vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0];
        Instruments {
            stage: meter.f64_histogram("flower.writer.stage.duration")
                .with_unit("s").with_boundaries(seconds.clone()).with_description("Writer stage wall time; execution separates inline and dispatched work").build(),
            request: meter.f64_histogram("flower.writer.request.duration")
                .with_unit("s").with_boundaries(seconds).with_description("Writer submit through receipt delivery, including queueing and commitment").build(),
            requests: meter.u64_counter("flower.writer.requests")
                .with_description("Writer submissions completed, by bounded outcome and request kind").build(),
            rejected: meter.u64_counter("flower.writer.queue.rejected")
                .with_description("Mutation queue enqueue failures").build(),
            batches: meter.u64_counter("flower.writer.batches")
                .with_description("Writer groups by commitment outcome and stop reason").build(),
            batch_size: meter.u64_histogram("flower.writer.batch.size")
                .with_unit("{request}").with_boundaries(counts.clone()).with_description("Group requests, commands, duplicate receipts, validation errors, deferred requests, and target size").build(),
            batch_bytes: meter.u64_histogram("flower.writer.batch.bytes")
                .with_unit("By").with_boundaries(vec![0.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0]).with_description("Encoded command bytes per writer group").build(),
            queued: meter.u64_histogram("flower.writer.queue.depth")
                .with_unit("{request}").with_boundaries(counts.clone()).with_description("Queued requests observed by the batching controller").build(),
            lag: meter.u64_histogram("flower.writer.replication.lag")
                .with_unit("{log}").with_boundaries(counts).with_description("Local unapplied and quorum unmatched log entries at batch selection").build(),
            speculative: meter.u64_counter("flower.writer.speculation")
                .with_description("Speculative candidate starts, reuse, validation conflicts, and preparation failures").build(),
            serial_jobs: meter.u64_counter("flower.writer.serial.jobs")
                .with_description("Serial blocking-worker batches").build(),
            serial_requests: meter.u64_counter("flower.writer.serial.requests")
                .with_description("Requests prepared inside serial blocking-worker batches").build(),
            maintenance_runs: meter.u64_counter("flower.writer.maintenance.runs")
                .with_description("Maintenance turns by outcome: idle, committed, failed, follower (nothing run), or skipped (a writer window yielded with nothing due)").build(),
            maintenance_wakes: meter.u64_counter("flower.writer.maintenance.wakes")
                .with_description("Writes and leadership changes of a database seen by its maintenance: write (touched what the last run read), unrelated (left its timer alone), leadership").build(),
        }
    }))
}

pub(super) fn kind(deployment: bool) -> &'static str {
    if deployment { "deployment" } else { "mutation" }
}

pub(super) fn outcome<T>(result: &Result<T, ApiError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(error) if error.status.is_client_error() => "client_error",
        Err(_) => "server_error",
    }
}

pub(super) fn status(span: &tracing::Span, outcome: &'static str) {
    span.record("status", outcome);
    if matches!(
        outcome,
        "client_error" | "server_error" | "cancelled" | "error"
    ) {
        span.set_status(opentelemetry::trace::Status::error(outcome));
    }
}

// OpenTelemetry attributes use signed integers. Explicit conversion keeps
// tracing's unsigned values from falling back to debug strings in the bridge.
pub(super) fn count(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Maintenance turns and wakes, each bound once per value of their one
/// bounded attribute; a value outside the set records unbound.
fn bounded_add<const N: usize>(
    counter: &Counter<u64>,
    bound: &'static [OnceLock<BoundCounter<u64>>; N],
    values: [&'static str; N],
    key: &'static str,
    value: &'static str,
) {
    match values.iter().position(|known| *known == value) {
        Some(index) => bound[index]
            .get_or_init(|| counter.bind(&[KeyValue::new(key, value)]))
            .add(1),
        None => counter.add(1, &[KeyValue::new(key, value)]),
    }
}

pub(super) fn maintenance_run(outcome: &'static str) {
    if let Some(metrics) = instruments() {
        static BOUND: [OnceLock<BoundCounter<u64>>; 5] = [const { OnceLock::new() }; 5];
        bounded_add(
            &metrics.maintenance_runs,
            &BOUND,
            ["idle", "committed", "failed", "follower", "skipped"],
            "outcome",
            outcome,
        );
    }
}

pub(super) fn maintenance_wake(reason: &'static str) {
    if let Some(metrics) = instruments() {
        static BOUND: [OnceLock<BoundCounter<u64>>; 3] = [const { OnceLock::new() }; 3];
        bounded_add(
            &metrics.maintenance_wakes,
            &BOUND,
            ["write", "unrelated", "leadership"],
            "reason",
            reason,
        );
    }
}

thread_local! {
    /// Stage histograms bound per (stage, execution), both from the bounded
    /// set of literals the writer passes, found by address: recording a stage
    /// then builds no attribute set and takes no series-map lock. A literal
    /// at two addresses only binds the same series twice.
    static STAGES: RefCell<Vec<(&'static str, &'static str, BoundHistogram<f64>)>> =
        const { RefCell::new(Vec::new()) };
    /// Batch counters bound per (outcome, stop_reason, mode, successor, early_drain), likewise.
    static BATCHES: RefCell<Vec<(BatchKey, BoundCounter<u64>)>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn stage(stage: &'static str, duration: Duration, execution: &'static str) {
    if let Some(metrics) = instruments() {
        let attributes = || {
            [
                KeyValue::new("stage", stage),
                KeyValue::new("execution", execution),
            ]
        };
        let seconds = duration.as_secs_f64();
        let bound = STAGES.try_with(|stages| {
            let mut stages = stages.borrow_mut();
            let index = match stages.iter().position(|(known, known_execution, _)| {
                std::ptr::eq(*known, stage) && std::ptr::eq(*known_execution, execution)
            }) {
                Some(index) => index,
                None => {
                    stages.push((stage, execution, metrics.stage.bind(&attributes())));
                    stages.len() - 1
                }
            };
            stages[index].2.record(seconds);
        });
        if bound.is_err() {
            // This thread's bound instruments are gone: it is exiting.
            metrics.stage.record(seconds, &attributes());
        }
    }
}

const OUTCOMES: [&str; 4] = ["ok", "client_error", "server_error", "cancelled"];

/// A batch counter's attributes.
#[derive(Clone, Copy)]
struct BatchKey {
    outcome: &'static str,
    stop_reason: &'static str,
    mode: &'static str,
    successor: bool,
    early_drain: bool,
}

impl BatchKey {
    fn same(&self, other: &Self) -> bool {
        std::ptr::eq(self.outcome, other.outcome)
            && std::ptr::eq(self.stop_reason, other.stop_reason)
            && std::ptr::eq(self.mode, other.mode)
            && self.successor == other.successor
            && self.early_drain == other.early_drain
    }

    fn attributes(&self) -> [KeyValue; 5] {
        [
            KeyValue::new("outcome", self.outcome),
            KeyValue::new("stop_reason", self.stop_reason),
            KeyValue::new("mode", self.mode),
            KeyValue::new("successor", self.successor),
            KeyValue::new("early_drain", self.early_drain),
        ]
    }
}

pub(super) fn request(duration: Duration, deployment: bool, outcome: &'static str) {
    if let Some(metrics) = instruments() {
        // Each kind and outcome is bound once, when first recorded.
        static BOUND: [OnceLock<(BoundHistogram<f64>, BoundCounter<u64>)>; 2 * OUTCOMES.len()] =
            [const { OnceLock::new() }; 2 * OUTCOMES.len()];
        let attributes = || {
            [
                KeyValue::new("kind", kind(deployment)),
                KeyValue::new("outcome", outcome),
            ]
        };
        match OUTCOMES.iter().position(|known| *known == outcome) {
            Some(index) => {
                let (request, requests) = BOUND[usize::from(deployment) * OUTCOMES.len() + index]
                    .get_or_init(|| {
                        let attributes = attributes();
                        (
                            metrics.request.bind(&attributes),
                            metrics.requests.bind(&attributes),
                        )
                    });
                request.record(duration.as_secs_f64());
                requests.add(1);
            }
            None => {
                let attributes = attributes();
                metrics.request.record(duration.as_secs_f64(), &attributes);
                metrics.requests.add(1, &attributes);
            }
        }
    }
}

pub(super) struct Submission {
    started: Instant,
    deployment: bool,
    trace: tracing::Span,
    outcome: &'static str,
}

impl Submission {
    pub(super) fn new(deployment: bool, trace: tracing::Span) -> Self {
        Self {
            started: Instant::now(),
            deployment,
            trace,
            outcome: "cancelled",
        }
    }

    pub(super) fn finish<T>(&mut self, result: &Result<T, ApiError>) {
        self.outcome = outcome(result);
    }
}

impl Drop for Submission {
    fn drop(&mut self) {
        status(&self.trace, self.outcome);
        request(self.started.elapsed(), self.deployment, self.outcome);
    }
}

pub(super) fn rejected(reason: &'static str) {
    if let Some(metrics) = instruments() {
        metrics.rejected.add(1, &[KeyValue::new("reason", reason)]);
    }
}

pub(super) fn speculation(outcome: &'static str, count: usize) {
    if let Some(metrics) = instruments() {
        metrics
            .speculative
            .add(count as u64, &[KeyValue::new("outcome", outcome)]);
    }
}

pub(super) fn link(span: &tracing::Span, request: &tracing::Span) {
    if crate::telemetry::enabled() && crate::telemetry::records(span) {
        link_request_context(span, request);
    }
}

fn link_request_context(span: &tracing::Span, request: &tracing::Span) {
    // An unsampled request keeps its trace's ids, which a sampled batch links to.
    let context = crate::telemetry::context_of(request);
    let parent = context.span();
    let context = parent.span_context();
    if context.is_valid() {
        span.add_link(context.clone());
    }
}

pub(super) fn batch(pending: &[Pending]) -> tracing::Span {
    if !crate::telemetry::enabled() {
        return tracing::Span::none();
    }
    linked_batch(pending.iter().map(|request| &request.input.trace))
}

fn linked_batch<'a>(requests: impl Iterator<Item = &'a tracing::Span>) -> tracing::Span {
    // One group contains multiple independently traced requests. Links preserve
    // that relationship without inventing a shared request parent.
    let span = tracing::info_span!(target: "flower::otel", parent: None, "flower.writer.batch",
        requests = tracing::field::Empty, commands = tracing::field::Empty,
        bytes = tracing::field::Empty, duplicates = tracing::field::Empty,
        errors = tracing::field::Empty, stop_reason = tracing::field::Empty,
        status = tracing::field::Empty, speculative_candidates = tracing::field::Empty,
        speculative_reused = tracing::field::Empty);
    // Only a batch of a sampled trace exports its links.
    if crate::telemetry::records(&span) {
        for request in requests {
            link_request_context(&span, request);
        }
    }
    span
}

pub(super) fn batch_completed(
    group: &Group,
    committed: &Result<(), ApiError>,
    commit_us: u64,
    count: usize,
) {
    let Some(metrics) = instruments() else { return };
    let duplicates = group
        .results
        .iter()
        .filter(|result| {
            result
                .as_ref()
                .is_ok_and(|prepared| prepared.response["duplicate"] == true)
        })
        .count();
    let errors = group
        .results
        .iter()
        .filter(|result| result.is_err())
        .count();
    let status = outcome(committed);
    // An unsampled batch's span records nothing: skip building its fields.
    if crate::telemetry::records(&group.trace) {
        group
            .trace
            .record("requests", self::count(group.results.len()));
        group.trace.record("commands", self::count(count));
        group.trace.record("bytes", self::count(group.bytes));
        group.trace.record("duplicates", self::count(duplicates));
        group.trace.record("errors", self::count(errors));
        group.trace.record("stop_reason", group.stop_reason);
        self::status(&group.trace, status);
        group.trace.record(
            "speculative_candidates",
            self::count(group.speculative_candidates),
        );
        group
            .trace
            .record("speculative_reused", self::count(group.speculative_reused));
    }
    let key = BatchKey {
        outcome: status,
        stop_reason: group.stop_reason,
        mode: group.decision.mode,
        successor: group.successor,
        early_drain: group.early_drain,
    };
    let counted = BATCHES.try_with(|batches| {
        let mut batches = batches.borrow_mut();
        let index = match batches.iter().position(|(known, _)| known.same(&key)) {
            Some(index) => index,
            None => {
                batches.push((key, metrics.batches.bind(&key.attributes())));
                batches.len() - 1
            }
        };
        batches[index].1.add(1);
    });
    if counted.is_err() {
        metrics.batches.add(1, &key.attributes());
    }
    // Every batch records every component and lag kind, so they are bound together, at the first batch.
    static BOUND: OnceLock<([BoundHistogram<u64>; 6], [BoundHistogram<u64>; 2])> = OnceLock::new();
    let (sizes, lags) = BOUND.get_or_init(|| {
        let components = [
            "requests",
            "commands",
            "duplicates",
            "errors",
            "deferred",
            "target",
        ];
        (
            components.map(|component| {
                metrics
                    .batch_size
                    .bind(&[KeyValue::new("component", component)])
            }),
            ["local_unapplied", "quorum_unmatched"]
                .map(|kind| metrics.lag.bind(&[KeyValue::new("kind", kind)])),
        )
    });
    for (size, count) in sizes.iter().zip([
        group.results.len(),
        count,
        duplicates,
        errors,
        group.deferred,
        group.decision.count,
    ]) {
        size.record(count as u64);
    }
    metrics.batch_bytes.record(group.bytes as u64, &[]);
    metrics.queued.record(group.decision.queued as u64, &[]);
    lags[0].record(group.decision.lag.local_unapplied);
    lags[1].record(group.decision.lag.quorum_unmatched);
    for (name, micros) in [
        ("snapshot_read", group.read_us),
        ("batch_prepare", group.prepare_us),
        ("batch_fill_wait", group.fill_wait_us),
        ("batch_commit", commit_us),
    ] {
        stage(name, Duration::from_micros(micros), "batch");
    }
    metrics
        .serial_jobs
        .add(group.serial_worker_jobs as u64, &[]);
    metrics
        .serial_requests
        .add(group.serial_worker_requests as u64, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::SpanId;
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter},
    };

    #[derive(Clone, Debug, Default)]
    struct Exported(Arc<std::sync::Mutex<Vec<SpanData>>>);

    impl SpanExporter for Exported {
        async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(spans);
            Ok(())
        }
    }

    #[test]
    fn batches_link_all_requests_without_inheriting_the_active_request_parent() {
        let exported = Exported::default();
        let (provider, subscriber) = crate::telemetry::traced_for_test(
            SdkTracerProvider::builder().with_simple_exporter(exported.clone()),
            Sampler::AlwaysOn,
        );
        let contexts = tracing::subscriber::with_default(subscriber, || {
            let first = tracing::info_span!(target: "flower::otel", parent: None, "request.first");
            let second =
                tracing::info_span!(target: "flower::otel", parent: None, "request.second");
            let context = |span: &tracing::Span| {
                crate::telemetry::context_of(span)
                    .span()
                    .span_context()
                    .clone()
            };
            let contexts = [context(&first), context(&second)];
            // A group may be assembled while some caller's span is active. It
            // still represents both requests and must start a separate trace.
            first.in_scope(|| {
                let group = linked_batch([&first, &second].into_iter());
                group.record("requests", count(2));
                group.record("status", "ok");
            });
            contexts
        });
        provider.force_flush().unwrap();
        let spans = exported.0.lock().unwrap();
        let group = spans
            .iter()
            .find(|span| span.name == "flower.writer.batch")
            .unwrap();
        assert_eq!(group.parent_span_id, SpanId::INVALID);
        assert_eq!(group.links.links.len(), 2);
        for (link, context) in group.links.links.iter().zip(contexts) {
            assert_eq!(link.span_context, context);
            assert_ne!(group.span_context.trace_id(), context.trace_id());
        }
        assert!(
            group
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "requests"
                    && attribute.value == opentelemetry::Value::I64(2))
        );
    }

    #[test]
    fn a_sampled_batch_links_the_requests_of_unsampled_traces() {
        use crate::telemetry::context_of;
        let span_context = |span: &tracing::Span| context_of(span).span().span_context().clone();
        let exported = Exported::default();
        let (provider, subscriber) = crate::telemetry::traced_for_test(
            SdkTracerProvider::builder().with_simple_exporter(exported.clone()),
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(0.5))),
        );
        let links = tracing::subscriber::with_default(subscriber, || {
            let requests: Vec<_> =
                std::iter::repeat_with(|| tracing::info_span!(target: "flower::otel", "request"))
                    .filter(|request| !span_context(request).is_sampled())
                    .take(2)
                    .collect();
            let batch = std::iter::repeat_with(|| linked_batch(requests.iter()))
                .find(|batch| span_context(batch).is_sampled())
                .unwrap();
            drop(batch);
            requests.iter().map(span_context).collect::<Vec<_>>()
        });
        provider.force_flush().unwrap();
        let spans = exported.0.lock().unwrap();
        // The sampled requests the filter passed over export too; one batch does.
        let batches: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "flower.writer.batch")
            .collect();
        assert_eq!(batches.len(), 1);
        let linked: Vec<_> = batches[0]
            .links
            .links
            .iter()
            .map(|link| link.span_context.clone())
            .collect();
        assert_eq!(linked.len(), 2);
        assert_eq!(linked, links);
        assert!(
            linked
                .iter()
                .all(|link| link.is_valid() && !link.is_sampled())
        );
    }
    #[test]
    fn bound_maintenance_counts_land_in_the_series_unbound_counts_did() {
        use opentelemetry::metrics::MeterProvider;
        let (provider, exported) = crate::telemetry::exported_metrics::provider();
        let meter = provider.meter("maintenance-test");
        let bound_runs = meter.u64_counter("runs.bound").build();
        let unbound_runs = meter.u64_counter("runs.unbound").build();
        static BOUND: [OnceLock<BoundCounter<u64>>; 2] = [const { OnceLock::new() }; 2];
        for outcome in ["idle", "idle", "committed", "unknown"] {
            bounded_add(
                &bound_runs,
                &BOUND,
                ["idle", "committed"],
                "outcome",
                outcome,
            );
            unbound_runs.add(1, &[KeyValue::new("outcome", outcome)]);
        }
        provider.force_flush().unwrap();
        let exported = exported.0.lock().unwrap();
        let series = |name: &str| {
            let mut points = exported[name].clone();
            points.sort();
            points
        };
        assert_eq!(series("runs.bound").len(), 3);
        assert_eq!(series("runs.bound"), series("runs.unbound"));
    }
}
