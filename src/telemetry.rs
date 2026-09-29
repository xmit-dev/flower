//! Opt-in OTLP traces and metrics. Application inputs, credentials and error
//! text are deliberately absent. Only the `flower::otel` trace target exports;
//! ordinary diagnostic logs keep their separate RUST_LOG filter.

use std::{
    cell::RefCell,
    future::Future,
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use axum::{
    extract::{MatchedPath, Request},
    http::HeaderMap,
    middleware::Next,
    response::Response,
};
use opentelemetry::{
    KeyValue, global,
    metrics::{
        BoundCounter, BoundHistogram, BoundUpDownCounter, Counter, Histogram, UpDownCounter,
    },
    propagation::{Extractor, Injector},
    trace::TracerProvider,
};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::SdkMeterProvider,
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{Layer, filter::FilterExt, layer::SubscriberExt, util::SubscriberInitExt};

mod sampling;
pub use sampling::{context_of, records};

static ENABLED: AtomicBool = AtomicBool::new(false);

#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

struct Config {
    enabled: bool,
    sampler: Sampler,
    trace_protocol: Protocol,
    metric_protocol: Protocol,
}

impl Config {
    fn load(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let boolean = |name, default| -> anyhow::Result<bool> {
            match get(name).as_deref() {
                None => Ok(default),
                Some("1" | "true") => Ok(true),
                Some("0" | "false") => Ok(false),
                _ => anyhow::bail!("{name} must be true, false, 1, or 0"),
            }
        };
        let enabled =
            boolean("FLOWER_OTEL_ENABLED", false)? && !boolean("OTEL_SDK_DISABLED", false)?;
        let mut config = Self {
            enabled,
            sampler: Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(0.01))),
            trace_protocol: Protocol::HttpBinary,
            metric_protocol: Protocol::HttpBinary,
        };
        // No exporter configuration is needed, and no network threads are
        // created, when telemetry is off (including OTEL_SDK_DISABLED).
        if !enabled {
            return Ok(config);
        }
        let protocol = |signal: &str| -> anyhow::Result<Protocol> {
            match get(signal)
                .or_else(|| get("OTEL_EXPORTER_OTLP_PROTOCOL"))
                .as_deref()
            {
                None | Some("http/protobuf") => Ok(Protocol::HttpBinary),
                Some("http/json") => Ok(Protocol::HttpJson),
                _ => anyhow::bail!(
                    "{signal}/OTEL_EXPORTER_OTLP_PROTOCOL must be http/protobuf or http/json"
                ),
            }
        };
        config.trace_protocol = protocol("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")?;
        config.metric_protocol = protocol("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")?;
        let ratio = || -> anyhow::Result<f64> {
            let value = get("OTEL_TRACES_SAMPLER_ARG")
                .unwrap_or_else(|| "0.01".into())
                .parse::<f64>()
                .map_err(|_| {
                    anyhow::anyhow!("OTEL_TRACES_SAMPLER_ARG must be a number between 0 and 1")
                })?;
            anyhow::ensure!(
                value.is_finite() && (0.0..=1.0).contains(&value),
                "OTEL_TRACES_SAMPLER_ARG must be between 0 and 1"
            );
            Ok(value)
        };
        config.sampler = match get("OTEL_TRACES_SAMPLER").as_deref() {
            None | Some("parentbased_traceidratio") => {
                Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio()?)))
            }
            Some("traceidratio") => Sampler::TraceIdRatioBased(ratio()?),
            Some("always_on") => Sampler::AlwaysOn,
            Some("always_off") => Sampler::AlwaysOff,
            Some("parentbased_always_on") => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
            Some("parentbased_always_off") => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
            _ => anyhow::bail!("unsupported OTEL_TRACES_SAMPLER"),
        };
        Ok(config)
    }
}

pub struct Telemetry {
    traces: Option<SdkTracerProvider>,
    metrics: Option<SdkMeterProvider>,
}

/// Call once per server, before creating actors. Batch exporters own dedicated
/// threads; a slow collector does not execute HTTP on the database request path.
pub fn init(node_id: u64, listen: &str) -> anyhow::Result<Telemetry> {
    let config = Config::load(|name| std::env::var(name).ok())?;
    let (traces, metrics) = if config.enabled {
        let resource = Resource::builder()
            .with_service_name(
                std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "flower".into()),
            )
            .with_attributes([
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                KeyValue::new(
                    "service.instance.id",
                    format!("{listen}/{node_id}/{}", std::process::id()),
                ),
                KeyValue::new("flower.node.id", node_id.to_string()),
                KeyValue::new("server.address", listen.to_owned()),
                KeyValue::new("process.pid", i64::from(std::process::id())),
            ])
            .build();
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(config.trace_protocol)
            .build()?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(config.metric_protocol)
            .build()?;
        // The SDK samples with the same sampler that sampling::SampleFirst
        // decided each trace with, and for the same trace id.
        let traces = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_sampler(config.sampler.clone())
            .with_id_generator(sampling::PresampledIds::default())
            .with_batch_exporter(span_exporter)
            .build();
        let metrics = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(metric_exporter)
            .build();
        global::set_text_map_propagator(TraceContextPropagator::new());
        global::set_tracer_provider(traces.clone());
        global::set_meter_provider(metrics.clone());
        (Some(traces), Some(metrics))
    } else {
        (None, None)
    };
    let log_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "flower=info,openraft=warn".into());
    subscriber(
        traces.as_ref().map(|traces| (traces, config.sampler)),
        log_filter,
    )
    .try_init()?;
    ENABLED.store(config.enabled, Ordering::Relaxed);
    Ok(Telemetry { traces, metrics })
}

/// `traces` holds the provider and the sampler it was built with.
fn subscriber(
    traces: Option<(&SdkTracerProvider, Sampler)>,
    log_filter: tracing_subscriber::EnvFilter,
) -> Box<dyn tracing::Subscriber + Send + Sync> {
    // With no exporter, use global filtering so rejected spans never reach the
    // registry at all, including when another thread uses a local subscriber.
    let Some((provider, sampler)) = traces else {
        return Box::new(
            tracing_subscriber::registry()
                .with(log_filter)
                .with(tracing_subscriber::filter::filter_fn(|meta| {
                    meta.target() != "flower::otel"
                }))
                .with(tracing_subscriber::fmt::layer()),
        );
    };
    let logs = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_filter(
        log_filter.and(tracing_subscriber::filter::filter_fn(|meta| {
            meta.target() != "flower::otel"
        })),
    ));
    // Do not install an Option::None layer: its Interest::always can construct
    // registry entries for spans rejected by logging, even with OTEL disabled.
    // Likewise combine log filters before registration instead of nesting two
    // independently registered filters whose interests can enable each other.
    // SampleFirst shows the OpenTelemetry layer only the spans of sampled
    // traces; the per-layer filter shows both only `flower::otel` spans.
    let otel = tracing_opentelemetry::layer().with_tracer(provider.tracer("flower"));
    Box::new(
        logs.with(sampling::SampleFirst::new(otel, sampler).with_filter(
            tracing_subscriber::filter::filter_fn(|meta| meta.target() == "flower::otel"),
        )),
    )
}

impl Telemetry {
    /// Flush after the server and consensus stop, including on startup failures.
    /// Blocking exporter shutdown is kept off Tokio's async workers.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        ENABLED.store(false, Ordering::Relaxed);
        tokio::task::spawn_blocking(move || {
            let traces = self.traces.map(|p| p.shutdown()).transpose();
            let metrics = self.metrics.map(|p| p.shutdown()).transpose();
            traces?;
            metrics?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }
}

struct Headers<'a>(&'a HeaderMap);
impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}
struct OutgoingHeaders(HeaderMap);
impl Injector for OutgoingHeaders {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (
            axum::http::HeaderName::try_from(key),
            axum::http::HeaderValue::try_from(value),
        ) {
            self.0.insert(key, value);
        }
    }
}

/// Propagate only W3C trace context, never baggage or application headers.
pub fn inject(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if !enabled() {
        return request;
    }
    let mut headers = OutgoingHeaders(HeaderMap::new());
    global::get_text_map_propagator(|p| {
        p.inject_context(&context_of(&tracing::Span::current()), &mut headers)
    });
    request.headers(headers.0)
}

struct Instruments {
    http_duration: Histogram<f64>,
    http_stage_duration: Histogram<f64>,
    active: UpDownCounter<i64>,
    query_duration: Histogram<f64>,
    cache: Counter<u64>,
    rpc_duration: Histogram<f64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("flower");
        Instruments {
            http_duration: meter
                .f64_histogram("flower.http.server.request.duration")
                .with_unit("s")
                .with_description(
                    "HTTP time until response headers; excludes streaming body lifetime",
                )
                .with_boundaries(vec![
                    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
                    2.5, 5.0, 10.0,
                ])
                .build(),
            http_stage_duration: meter
                .f64_histogram("flower.http.server.stage.duration")
                .with_unit("s")
                .with_description(
                    "Request body reception and JSON extraction, including scheduling waits",
                )
                .with_boundaries(duration_boundaries())
                .build(),
            active: meter
                .i64_up_down_counter("flower.http.server.active_requests")
                .build(),
            query_duration: meter
                .f64_histogram("flower.query.stage.duration")
                .with_unit("s")
                .with_boundaries(duration_boundaries())
                .build(),
            cache: meter.u64_counter("flower.query.cache.lookups").build(),
            rpc_duration: meter
                .f64_histogram("flower.raft.rpc.duration")
                .with_unit("s")
                .with_boundaries(duration_boundaries())
                .build(),
        }
    })
}

fn duration_boundaries() -> Vec<f64> {
    vec![
        0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025,
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

fn http_method(value: &str) -> &'static str {
    match value {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "PATCH" => "PATCH",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "_OTHER",
    }
}

/// A route and method's instruments, bound on each thread when first used
/// there: recording then builds no attribute set and takes no series-map lock.
struct HttpBound {
    route: Box<str>,
    method: &'static str,
    active: BoundUpDownCounter<i64>,
    /// By outcome and status code.
    durations: Vec<(&'static str, Option<u16>, BoundHistogram<f64>)>,
}

thread_local! {
    static HTTP: RefCell<Vec<HttpBound>> = const { RefCell::new(Vec::new()) };
    /// Stage histograms by kind, dimension, value and outcome (all literals, found by address).
    static STAGES: RefCell<Vec<(StageKind, &'static str, &'static str, &'static str, BoundHistogram<f64>)>> =
        const { RefCell::new(Vec::new()) };
    /// Query cache counters by outcome (a literal, found by address).
    static CACHE: RefCell<Vec<(&'static str, BoundCounter<u64>)>> = const { RefCell::new(Vec::new()) };
}

/// A request's attributes; the outcome and status code are its duration's.
fn http_attributes(
    route: &str,
    method: &'static str,
    outcome: Option<(&'static str, Option<u16>)>,
) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("http.route", route.to_owned()),
        KeyValue::new("http.request.method", method),
    ];
    if let Some((outcome, status)) = outcome {
        attributes.push(KeyValue::new("outcome", outcome));
        if let Some(status) = status {
            attributes.push(KeyValue::new(
                "http.response.status_code",
                i64::from(status),
            ));
        }
    }
    attributes
}

/// Runs `f` on this thread's instruments for `route` and `method`, or returns
/// false when the thread is exiting and they are gone.
fn with_http(route: &str, method: &'static str, f: impl FnOnce(&mut HttpBound)) -> bool {
    HTTP.try_with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = match cache
            .iter()
            .position(|bound| std::ptr::eq(bound.method, method) && &*bound.route == route)
        {
            Some(index) => index,
            None => {
                cache.push(HttpBound {
                    route: route.into(),
                    method,
                    active: instruments()
                        .active
                        .bind(&http_attributes(route, method, None)),
                    durations: Vec::new(),
                });
                cache.len() - 1
            }
        };
        f(&mut cache[index]);
    })
    .is_ok()
}

struct HttpObservation {
    started: Instant,
    route: Option<MatchedPath>,
    method: &'static str,
    status: Option<u16>,
    span: tracing::Span,
}
impl Drop for HttpObservation {
    fn drop(&mut self) {
        let outcome = match self.status {
            None => "cancelled",
            Some(s) if s >= 500 => "server_error",
            Some(s) if s >= 400 => "client_error",
            _ => "ok",
        };
        self.span.record("outcome", outcome);
        if self.status.is_none_or(|s| s >= 500) {
            self.span.record("otel.status_code", "ERROR");
        }
        let seconds = self.started.elapsed().as_secs_f64();
        let route = self.route.as_ref().map_or("unmatched", MatchedPath::as_str);
        let (method, status) = (self.method, self.status);
        let bound = with_http(route, method, |bound| {
            bound.active.add(-1);
            let index = match bound.durations.iter().position(|(known, known_status, _)| {
                std::ptr::eq(*known, outcome) && *known_status == status
            }) {
                Some(index) => index,
                None => {
                    let attributes = http_attributes(route, method, Some((outcome, status)));
                    let duration = instruments().http_duration.bind(&attributes);
                    bound.durations.push((outcome, status, duration));
                    bound.durations.len() - 1
                }
            };
            bound.durations[index].2.record(seconds);
        });
        if !bound {
            let meter = instruments();
            meter.active.add(-1, &http_attributes(route, method, None));
            meter.http_duration.record(
                seconds,
                &http_attributes(route, method, Some((outcome, status))),
            );
        }
    }
}

pub async fn http(request: Request, next: Next) -> Response {
    if !enabled() {
        return next.run(request).await;
    }
    // Matched route templates, never arbitrary URL paths or query strings.
    let matched = request.extensions().get::<MatchedPath>().cloned();
    let route = matched.as_ref().map_or("unmatched", MatchedPath::as_str);
    let method = http_method(request.method().as_str());
    let create = || {
        tracing::info_span!(target: "flower::otel", "flower.http.request", otel.kind="server", http.route=%route, http.request.method=method,
            http.response.status_code=tracing::field::Empty, outcome=tracing::field::Empty, otel.status_code=tracing::field::Empty)
    };
    // A request's traceparent decides its span's trace, and set_parent then
    // gives the OpenTelemetry layer that parent when the trace is sampled.
    let span = if request.headers().contains_key("traceparent") {
        let parent = global::get_text_map_propagator(|p| p.extract(&Headers(request.headers())));
        let span = sampling::with_remote_parent(&parent, create);
        let _ = span.set_parent(parent);
        span
    } else {
        create()
    };
    if !with_http(route, method, |bound| bound.active.add(1)) {
        instruments()
            .active
            .add(1, &http_attributes(route, method, None));
    }
    let mut observation = HttpObservation {
        started: Instant::now(),
        route: matched,
        method,
        status: None,
        span: span.clone(),
    };
    let response = next.run(request).instrument(span.clone()).await;
    observation.status = Some(response.status().as_u16());
    span.record(
        "http.response.status_code",
        i64::from(response.status().as_u16()),
    );
    response
}

pub fn query_cache(outcome: &'static str) {
    if enabled() {
        let counted = CACHE.try_with(|cache| {
            let mut cache = cache.borrow_mut();
            let index = match cache
                .iter()
                .position(|(known, _)| std::ptr::eq(*known, outcome))
            {
                Some(index) => index,
                None => {
                    let counter = instruments()
                        .cache
                        .bind(&[KeyValue::new("outcome", outcome)]);
                    cache.push((outcome, counter));
                    cache.len() - 1
                }
            };
            cache[index].1.add(1);
        });
        if counted.is_err() {
            instruments()
                .cache
                .add(1, &[KeyValue::new("outcome", outcome)]);
        }
    }
}

pub fn query_duration(stage: &'static str, seconds: f64, outcome: &'static str) {
    if enabled() {
        stage_duration(StageKind::Query, "stage", stage, outcome, seconds);
    }
}

/// Records a stage's duration in its kind's histogram, under `dimension` = `value` and `outcome`.
fn stage_duration(
    kind: StageKind,
    dimension: &'static str,
    value: &'static str,
    outcome: &'static str,
    seconds: f64,
) {
    let histogram = |metrics: &'static Instruments| match kind {
        StageKind::Http => &metrics.http_stage_duration,
        StageKind::Query => &metrics.query_duration,
        StageKind::Rpc => &metrics.rpc_duration,
    };
    let attributes = || {
        [
            KeyValue::new(dimension, value),
            KeyValue::new("outcome", outcome),
        ]
    };
    let bound = STAGES.try_with(|stages| {
        let mut stages = stages.borrow_mut();
        let index = match stages.iter().position(|(known, d, v, o, _)| {
            *known == kind
                && std::ptr::eq(*d, dimension)
                && std::ptr::eq(*v, value)
                && std::ptr::eq(*o, outcome)
        }) {
            Some(index) => index,
            None => {
                let bound = histogram(instruments()).bind(&attributes());
                stages.push((kind, dimension, value, outcome, bound));
                stages.len() - 1
            }
        };
        stages[index].4.record(seconds);
    });
    if bound.is_err() {
        histogram(instruments()).record(seconds, &attributes());
    }
}

struct StageObservation {
    started: Instant,
    span: tracing::Span,
    dimension: &'static str,
    value: &'static str,
    outcome: &'static str,
    kind: StageKind,
}

#[derive(Clone, Copy, PartialEq)]
enum StageKind {
    Http,
    Query,
    Rpc,
}

impl Drop for StageObservation {
    fn drop(&mut self) {
        self.span.record("outcome", self.outcome);
        if self.outcome != "ok" {
            self.span
                .set_status(opentelemetry::trace::Status::error(self.outcome));
        }
        stage_duration(
            self.kind,
            self.dimension,
            self.value,
            self.outcome,
            self.started.elapsed().as_secs_f64(),
        );
    }
}

pub async fn query_stage<T, E>(
    stage: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if !enabled() {
        return future.await;
    }
    let span = tracing::info_span!(target:"flower::otel", "flower.query.stage", stage, outcome=tracing::field::Empty);
    let mut observation = StageObservation {
        started: Instant::now(),
        span: span.clone(),
        dimension: "stage",
        value: stage,
        outcome: "cancelled",
        kind: StageKind::Query,
    };
    let result = future.instrument(span).await;
    observation.outcome = if result.is_ok() { "ok" } else { "error" };
    result
}

pub async fn http_stage<T, E>(
    stage: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if !enabled() {
        return future.await;
    }
    let span = tracing::info_span!(target:"flower::otel", "flower.http.stage", stage, outcome=tracing::field::Empty);
    let mut observation = StageObservation {
        started: Instant::now(),
        span: span.clone(),
        dimension: "stage",
        value: stage,
        outcome: "cancelled",
        kind: StageKind::Http,
    };
    let result = future.instrument(span).await;
    observation.outcome = if result.is_ok() { "ok" } else { "error" };
    result
}

/// Delegate all parsing, body limits, and rejection semantics to Axum. Ingress
/// has already buffered the body, so this isolates extraction from transport.
pub struct RequestJson<T>(pub T);

impl<S, T> axum::extract::FromRequest<S> for RequestJson<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = axum::extract::rejection::JsonRejection;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        http_stage(
            "json_extract",
            axum::Json::<T>::from_request(request, state),
        )
        .await
        .map(|axum::Json(value)| Self(value))
    }
}

pub async fn raft_rpc<T, E>(
    operation: &str,
    target: u64,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if !enabled() {
        return future.await;
    }
    let operation = match operation {
        "append" => "append",
        "vote" => "vote",
        "snapshot" => "snapshot",
        "read-fence" => "read-fence",
        _ => "other",
    };
    let span = tracing::info_span!(target:"flower::otel", "flower.raft.rpc", otel.kind="client", operation, flower.peer.id=target, outcome=tracing::field::Empty, otel.status_code=tracing::field::Empty);
    let mut observation = StageObservation {
        started: Instant::now(),
        span: span.clone(),
        dimension: "operation",
        value: operation,
        outcome: "cancelled",
        kind: StageKind::Rpc,
    };
    let result = future.instrument(span).await;
    observation.outcome = if result.is_ok() { "ok" } else { "error" };
    result
}

/// The server's traces for tests: the provider `builder` builds with the
/// server's sampling and ids, and the subscriber that feeds it.
#[cfg(test)]
pub(crate) fn traced_for_test(
    builder: opentelemetry_sdk::trace::TracerProviderBuilder,
    sampler: Sampler,
) -> (
    SdkTracerProvider,
    Box<dyn tracing::Subscriber + Send + Sync>,
) {
    let provider = builder
        .with_sampler(sampler.clone())
        .with_id_generator(sampling::PresampledIds::default())
        .build();
    let subscriber = subscriber(
        Some((&provider, sampler)),
        tracing_subscriber::EnvFilter::new("off"),
    );
    (provider, subscriber)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn measured_json_preserves_axum_limits_and_rejections() {
        use axum::{
            Json,
            body::{Body, to_bytes},
            extract::FromRequest,
            response::IntoResponse,
        };
        use serde_json::Value;
        let oversized = format!("\"{}\"", "x".repeat(2 * 1024 * 1024));
        for (content_type, body, status) in [
            (Some("application/json"), r#"{"ok":true}"#, 200),
            (Some("application/problem+json"), "null", 200),
            (None, "{}", 415),
            (Some("text/plain"), "{}", 415),
            (Some("application/json"), "{", 400),
            (Some("application/json"), oversized.as_str(), 413),
        ] {
            let mut responses = Vec::new();
            for measured in [false, true] {
                let mut request = Request::builder().method("POST").uri("/");
                if let Some(content_type) = content_type {
                    request = request.header("content-type", content_type);
                }
                let request = request.body(Body::from(body.to_owned())).unwrap();
                let result = if measured {
                    RequestJson::<Value>::from_request(request, &())
                        .await
                        .map(|RequestJson(value)| Json(value))
                } else {
                    Json::<Value>::from_request(request, &()).await
                };
                let response = result.into_response();
                assert_eq!(response.status().as_u16(), status);
                responses.push(to_bytes(response.into_body(), 4096).await.unwrap());
            }
            assert_eq!(responses[0], responses[1]);
        }
    }

    #[test]
    fn logging_alone_does_not_construct_otel_spans() {
        tracing::subscriber::with_default(
            subscriber(
                None,
                tracing_subscriber::EnvFilter::new("flower=info,openraft=warn"),
            ),
            || {
                assert!(tracing::info_span!(target: "flower::otel", "excluded").is_disabled());
                assert!(tracing::debug_span!(target: "flower", "debug").is_disabled());
                assert!(!tracing::info_span!(target: "flower", "ordinary").is_disabled());
            },
        );
    }

    #[test]
    fn otel_and_logging_filters_remain_independent() {
        let traces = SdkTracerProvider::default();
        tracing::subscriber::with_default(
            subscriber(
                Some((&traces, Sampler::AlwaysOn)),
                tracing_subscriber::EnvFilter::new("off"),
            ),
            || {
                assert!(!tracing::info_span!(target: "flower::otel", "included").is_disabled());
                assert!(tracing::info_span!(target: "flower", "ordinary").is_disabled());
            },
        );
    }
    fn config(values: &[(&str, &str)]) -> anyhow::Result<Config> {
        Config::load(|name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    }
    #[test]
    fn opt_in_and_disabled_ignore_export_configuration() {
        assert!(!config(&[]).unwrap().enabled);
        assert!(
            !config(&[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_SDK_DISABLED", "true"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
            ])
            .unwrap()
            .enabled
        );
        assert!(config(&[("FLOWER_OTEL_ENABLED", "1")]).unwrap().enabled);
        assert!(config(&[("FLOWER_OTEL_ENABLED", "yes")]).is_err());
    }
    #[test]
    fn sampling_and_protocol_fail_closed_on_bad_configuration() {
        for ratio in ["NaN", "inf", "-0.1", "1.1", "secret"] {
            assert!(
                config(&[
                    ("FLOWER_OTEL_ENABLED", "1"),
                    ("OTEL_TRACES_SAMPLER_ARG", ratio)
                ])
                .is_err()
            );
        }
        assert!(
            config(&[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_TRACES_SAMPLER", "unsupported")
            ])
            .is_err()
        );
        assert!(
            config(&[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
            ])
            .is_err()
        );
        let config = config(&[
            ("FLOWER_OTEL_ENABLED", "1"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json"),
            ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf"),
        ])
        .unwrap();
        assert_eq!(config.trace_protocol, Protocol::HttpBinary);
        assert_eq!(config.metric_protocol, Protocol::HttpJson);
    }
    #[test]
    fn methods_are_bounded_and_w3c_headers_round_trip_without_baggage() {
        use opentelemetry::{propagation::TextMapPropagator, trace::TraceContextExt};
        assert_eq!(http_method("arbitrary-user-method"), "_OTHER");
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        headers.insert("baggage", "secret=value".parse().unwrap());
        let propagator = TraceContextPropagator::new();
        let context = propagator.extract(&Headers(&headers));
        assert!(context.span().span_context().is_remote());
        let mut outgoing = OutgoingHeaders(HeaderMap::new());
        propagator.inject_context(&context, &mut outgoing);
        assert_eq!(outgoing.0.get("traceparent"), headers.get("traceparent"));
        assert!(!outgoing.0.contains_key("baggage"));
    }

    #[derive(Clone, Debug, Default)]
    struct Exported(std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);

    impl opentelemetry_sdk::trace::SpanExporter for Exported {
        async fn export(
            &self,
            spans: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.lock().unwrap().extend(spans);
            Ok(())
        }
    }

    /// The server's subscriber, exporting what `sampler` samples.
    fn traced(
        sampler: Sampler,
    ) -> (
        SdkTracerProvider,
        Exported,
        Box<dyn tracing::Subscriber + Send + Sync>,
    ) {
        let exported = Exported::default();
        let (provider, subscriber) = traced_for_test(
            SdkTracerProvider::builder().with_simple_exporter(exported.clone()),
            sampler,
        );
        (provider, exported, subscriber)
    }

    fn span_context(span: &tracing::Span) -> opentelemetry::trace::SpanContext {
        use opentelemetry::trace::TraceContextExt;
        context_of(span).span().span_context().clone()
    }

    fn traceparent(span: &tracing::Span) -> String {
        use opentelemetry::propagation::TextMapPropagator;
        let mut outgoing = OutgoingHeaders(HeaderMap::new());
        TraceContextPropagator::new().inject_context(&context_of(span), &mut outgoing);
        outgoing.0["traceparent"].to_str().unwrap().to_owned()
    }

    fn samples(sampler: &Sampler, trace_id: opentelemetry::trace::TraceId) -> bool {
        use opentelemetry::trace::SpanKind;
        use opentelemetry_sdk::trace::{SamplingDecision, ShouldSample};
        sampler
            .should_sample(None, trace_id, "root", &SpanKind::Internal, &[], &[])
            .decision
            == SamplingDecision::RecordAndSample
    }

    #[test]
    fn traces_are_decided_at_their_root_as_the_sdk_decides_them() {
        let ratio = Sampler::TraceIdRatioBased(0.5);
        let (provider, exported, subscriber) =
            traced(Sampler::ParentBased(Box::new(ratio.clone())));
        let mut sampled = std::collections::HashMap::new();
        tracing::subscriber::with_default(subscriber, || {
            for index in 0..400 {
                let root = tracing::info_span!(target: "flower::otel", "root", index);
                let root_context = span_context(&root);
                assert!(root_context.is_valid());
                // The SDK built the root with the trace id it was decided with.
                assert_eq!(records(&root), root_context.is_sampled());
                assert_eq!(
                    samples(&ratio, root_context.trace_id()),
                    root_context.is_sampled()
                );
                let child =
                    root.in_scope(|| tracing::info_span!(target: "flower::otel", "child", index));
                let grandchild =
                    tracing::info_span!(target: "flower::otel", parent: &child, "grandchild");
                // A root created in another trace's scope, as writer batches are.
                let other = root.in_scope(
                    || tracing::info_span!(target: "flower::otel", parent: None, "other"),
                );
                grandchild.in_scope(|| {});
                for span in [&child, &grandchild] {
                    let context = span_context(span);
                    assert_eq!(context.trace_id(), root_context.trace_id());
                    assert_eq!(context.is_sampled(), root_context.is_sampled());
                    assert_ne!(context.span_id(), root_context.span_id());
                    assert_eq!(records(span), root_context.is_sampled());
                }
                let other_context = span_context(&other);
                assert_ne!(other_context.trace_id(), root_context.trace_id());
                assert_eq!(
                    samples(&ratio, other_context.trace_id()),
                    other_context.is_sampled()
                );
                if root_context.is_sampled() {
                    sampled.insert(root_context.trace_id(), root_context.span_id());
                }
                if other_context.is_sampled() {
                    sampled.insert(other_context.trace_id(), other_context.span_id());
                }
            }
        });
        provider.force_flush().unwrap();
        // 800 roots at 0.5: far outside 300–500 only if decisions ignore the ratio.
        assert!((300..=500).contains(&sampled.len()), "{}", sampled.len());
        let spans = exported.0.lock().unwrap();
        let roots = spans
            .iter()
            .filter(|span| span.name == "root" || span.name == "other")
            .count();
        assert_eq!(roots, sampled.len());
        for span in spans.iter() {
            let root = sampled[&span.span_context.trace_id()];
            match span.name.as_ref() {
                "root" | "other" => assert_eq!(span.span_context.span_id(), root),
                "child" => assert_eq!(span.parent_span_id, root),
                _ => assert_ne!(span.parent_span_id, root),
            }
        }
        let children = spans.iter().filter(|span| span.name == "child").count();
        assert_eq!(spans.len(), roots + 2 * children);
    }

    #[test]
    fn a_remote_traceparent_decides_the_trace_and_unsampled_spans_still_propagate() {
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
        let (provider, exported, subscriber) =
            traced(Sampler::ParentBased(Box::new(Sampler::AlwaysOff)));
        let remote = |trace: u128, flags| {
            opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
                TraceId::from(trace),
                SpanId::from(7u64),
                flags,
                true,
                TraceState::default(),
            ))
        };
        let request = |parent: &opentelemetry::Context| {
            let span = sampling::with_remote_parent(
                parent,
                || tracing::info_span!(target: "flower::otel", "request"),
            );
            let _ = span.set_parent(parent.clone());
            span
        };
        tracing::subscriber::with_default(subscriber, || {
            let sampled = request(&remote(1, TraceFlags::SAMPLED));
            let child = sampled.in_scope(|| tracing::info_span!(target: "flower::otel", "work"));
            assert!(records(&sampled) && records(&child));
            assert_eq!(span_context(&child).trace_id(), TraceId::from(1u128));
            assert!(span_context(&child).is_sampled());

            let unsampled = request(&remote(2, TraceFlags::default()));
            let child = unsampled.in_scope(|| tracing::info_span!(target: "flower::otel", "work"));
            assert!(!records(&unsampled) && !records(&child));
            let (parent, context) = (span_context(&unsampled), span_context(&child));
            assert_eq!(parent.trace_id(), TraceId::from(2u128));
            assert_eq!(context.trace_id(), TraceId::from(2u128));
            assert_ne!(context.span_id(), parent.span_id());
            assert_ne!(context.span_id(), SpanId::from(7u64));
            // What telemetry::inject sends from inside the unsampled request.
            assert_eq!(
                child.in_scope(|| traceparent(&tracing::Span::current())),
                format!("00-{}-{}-00", context.trace_id(), context.span_id())
            );

            // A local root the sampler drops still has ids to propagate.
            let local = tracing::info_span!(target: "flower::otel", "local");
            let context = span_context(&local);
            assert!(context.is_valid() && !context.is_sampled());
            assert_eq!(
                traceparent(&local),
                format!("00-{}-{}-00", context.trace_id(), context.span_id())
            );
            drop((sampled, unsampled, local));
        });
        provider.force_flush().unwrap();
        let spans = exported.0.lock().unwrap();
        assert_eq!(spans.len(), 2, "only the sampled remote trace exports");
        let request = spans.iter().find(|span| span.name == "request").unwrap();
        assert_eq!(request.span_context.trace_id(), TraceId::from(1u128));
        assert_eq!(request.parent_span_id, SpanId::from(7u64));
        assert!(request.parent_span_is_remote);
        let work = spans.iter().find(|span| span.name == "work").unwrap();
        assert_eq!(work.parent_span_id, request.span_context.span_id());
    }
}

/// Metric points as the SDK exports them, for tests: by instrument name, each
/// point's attributes and its count (histograms) or value (sums).
#[cfg(test)]
pub(crate) mod exported_metrics {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use opentelemetry_sdk::{
        error::OTelSdkResult,
        metrics::{
            SdkMeterProvider, Temporality,
            data::{AggregatedMetrics, MetricData, ResourceMetrics},
            exporter::PushMetricExporter,
        },
    };

    pub(crate) type Points = BTreeMap<String, Vec<(BTreeMap<String, String>, u64)>>;

    #[derive(Clone, Debug, Default)]
    pub(crate) struct Exported(pub(crate) Arc<Mutex<Points>>);

    pub(crate) fn provider() -> (SdkMeterProvider, Exported) {
        let exported = Exported::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exported.clone())
            .build();
        (provider, exported)
    }

    fn attributes<'a>(
        values: impl Iterator<Item = &'a opentelemetry::KeyValue>,
    ) -> BTreeMap<String, String> {
        values
            .map(|value| (value.key.to_string(), value.value.as_str().into_owned()))
            .collect()
    }

    impl PushMetricExporter for Exported {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let mut captured = self.0.lock().unwrap();
            for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
                let points = match metric.data() {
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                        .data_points()
                        .map(|point| (attributes(point.attributes()), point.count()))
                        .collect(),
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                        .data_points()
                        .map(|point| (attributes(point.attributes()), point.count()))
                        .collect(),
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| (attributes(point.attributes()), point.value()))
                        .collect(),
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| (attributes(point.attributes()), point.value() as u64))
                        .collect(),
                    _ => continue,
                };
                captured.insert(metric.name().into(), points);
            }
            Ok(())
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }
}
