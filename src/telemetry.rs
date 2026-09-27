//! Opt-in OTLP traces and metrics. Application inputs, credentials and error
//! text are deliberately absent. Only the `flower::otel` trace target exports;
//! ordinary diagnostic logs keep their separate RUST_LOG filter.

use std::{
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
    metrics::{Counter, Histogram, UpDownCounter},
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
        let traces = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_sampler(config.sampler)
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
    subscriber(traces.as_ref(), log_filter).try_init()?;
    ENABLED.store(config.enabled, Ordering::Relaxed);
    Ok(Telemetry { traces, metrics })
}

fn subscriber(
    traces: Option<&SdkTracerProvider>,
    log_filter: tracing_subscriber::EnvFilter,
) -> Box<dyn tracing::Subscriber + Send + Sync> {
    // With no exporter, use global filtering so rejected spans never reach the
    // registry at all, including when another thread uses a local subscriber.
    let Some(provider) = traces else {
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
    Box::new(
        logs.with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("flower"))
                .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
                    meta.target() == "flower::otel"
                })),
        ),
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
        p.inject_context(&tracing::Span::current().context(), &mut headers)
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

struct HttpObservation {
    started: Instant,
    attrs: Vec<KeyValue>,
    status: Option<u16>,
    span: tracing::Span,
}
impl Drop for HttpObservation {
    fn drop(&mut self) {
        let meter = instruments();
        meter.active.add(-1, &self.attrs);
        let outcome = match self.status {
            None => "cancelled",
            Some(s) if s >= 500 => "server_error",
            Some(s) if s >= 400 => "client_error",
            _ => "ok",
        };
        self.attrs.push(KeyValue::new("outcome", outcome));
        if let Some(status) = self.status {
            self.attrs.push(KeyValue::new(
                "http.response.status_code",
                i64::from(status),
            ));
        }
        self.span.record("outcome", outcome);
        if self.status.is_none_or(|s| s >= 500) {
            self.span.record("otel.status_code", "ERROR");
        }
        meter
            .http_duration
            .record(self.started.elapsed().as_secs_f64(), &self.attrs);
    }
}

pub async fn http(request: Request, next: Next) -> Response {
    if !enabled() {
        return next.run(request).await;
    }
    // Matched route templates, never arbitrary URL paths or query strings.
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let method = http_method(request.method().as_str());
    let span = tracing::info_span!(target: "flower::otel", "flower.http.request", otel.kind="server", http.route=%route, http.request.method=method,
        http.response.status_code=tracing::field::Empty, outcome=tracing::field::Empty, otel.status_code=tracing::field::Empty);
    let parent = global::get_text_map_propagator(|p| p.extract(&Headers(request.headers())));
    let _ = span.set_parent(parent);
    let attrs = vec![
        KeyValue::new("http.route", route),
        KeyValue::new("http.request.method", method),
    ];
    instruments().active.add(1, &attrs);
    let mut observation = HttpObservation {
        started: Instant::now(),
        attrs,
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
        instruments()
            .cache
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

pub fn query_duration(stage: &'static str, seconds: f64, outcome: &'static str) {
    if enabled() {
        instruments().query_duration.record(
            seconds,
            &[
                KeyValue::new("stage", stage),
                KeyValue::new("outcome", outcome),
            ],
        );
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
        let metrics = instruments();
        let histogram = match self.kind {
            StageKind::Http => &metrics.http_stage_duration,
            StageKind::Query => &metrics.query_duration,
            StageKind::Rpc => &metrics.rpc_duration,
        };
        histogram.record(
            self.started.elapsed().as_secs_f64(),
            &[
                KeyValue::new(self.dimension, self.value),
                KeyValue::new("outcome", self.outcome),
            ],
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
            subscriber(Some(&traces), tracing_subscriber::EnvFilter::new("off")),
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
}
