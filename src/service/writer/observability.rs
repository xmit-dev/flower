//! Writer telemetry has bounded metric dimensions. Request content, identifiers,
//! principals, and error messages never enter telemetry; resolved method names
//! appear only in sampled traces.
use super::*;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
    trace::TraceContextExt,
};
use std::sync::OnceLock;
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

pub(super) fn stage(stage: &'static str, duration: Duration, execution: &'static str) {
    if let Some(metrics) = instruments() {
        metrics.stage.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("stage", stage),
                KeyValue::new("execution", execution),
            ],
        );
    }
}

pub(super) fn request(duration: Duration, deployment: bool, outcome: &'static str) {
    if let Some(metrics) = instruments() {
        let attributes = [
            KeyValue::new("kind", kind(deployment)),
            KeyValue::new("outcome", outcome),
        ];
        metrics.request.record(duration.as_secs_f64(), &attributes);
        metrics.requests.add(1, &attributes);
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
    if crate::telemetry::enabled() {
        link_request_context(span, request);
    }
}

fn link_request_context(span: &tracing::Span, request: &tracing::Span) {
    let context = request.context();
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
    for request in requests {
        link_request_context(&span, request);
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
    metrics.batches.add(
        1,
        &[
            KeyValue::new("outcome", status),
            KeyValue::new("stop_reason", group.stop_reason),
            KeyValue::new("mode", group.decision.mode),
            KeyValue::new("successor", group.successor),
            KeyValue::new("early_drain", group.early_drain),
        ],
    );
    for (component, count) in [
        ("requests", group.results.len()),
        ("commands", count),
        ("duplicates", duplicates),
        ("errors", errors),
        ("deferred", group.deferred),
        ("target", group.decision.count),
    ] {
        metrics
            .batch_size
            .record(count as u64, &[KeyValue::new("component", component)]);
    }
    metrics.batch_bytes.record(group.bytes as u64, &[]);
    metrics.queued.record(group.decision.queued as u64, &[]);
    metrics.lag.record(
        group.decision.lag.local_unapplied,
        &[KeyValue::new("kind", "local_unapplied")],
    );
    metrics.lag.record(
        group.decision.lag.quorum_unmatched,
        &[KeyValue::new("kind", "quorum_unmatched")],
    );
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
    use opentelemetry::trace::{SpanId, TracerProvider};
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter},
    };
    use tracing_subscriber::layer::SubscriberExt;

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
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exported.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("writer-test")));
        let contexts = tracing::subscriber::with_default(subscriber, || {
            let first = tracing::info_span!(parent: None, "request.first");
            let second = tracing::info_span!(parent: None, "request.second");
            let contexts = [
                first.context().span().span_context().clone(),
                second.context().span().span_context().clone(),
            ];
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
}
