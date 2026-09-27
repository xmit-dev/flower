//! Optional wall-clock stage measurements. Nested cell sums overlap and are
//! reported as sums, never interpreted as independent CPU percentages.

use opentelemetry::{
    KeyValue, global,
    metrics::{BoundCounter, BoundHistogram, Counter, Histogram, Meter},
};
use std::{
    borrow::Cow,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Copy)]
pub(super) enum Stage {
    CoordinatorExecute,
    CoordinatorReceiveWait,
    CellWall,
    CellRuntime,
    CellLoad,
    CellExecute,
    CellReadWait,
    CellReset,
    BundlePrepare,
    Manifest,
    ResultValidation,
}

impl Stage {
    const ALL: [Self; 11] = [
        Self::CoordinatorExecute,
        Self::CoordinatorReceiveWait,
        Self::CellWall,
        Self::CellRuntime,
        Self::CellLoad,
        Self::CellExecute,
        Self::CellReadWait,
        Self::CellReset,
        Self::BundlePrepare,
        Self::Manifest,
        Self::ResultValidation,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::CoordinatorExecute => "coordinator_execute",
            Self::CoordinatorReceiveWait => "coordinator_receive_wait",
            Self::CellWall => "cell_wall_sum",
            Self::CellRuntime => "cell_runtime_sum",
            Self::CellLoad => "cell_load_sum",
            Self::CellExecute => "cell_execute_sum",
            Self::CellReadWait => "cell_read_wait_sum",
            Self::CellReset => "cell_reset_sum",
            Self::BundlePrepare => "bundle_prepare",
            Self::Manifest => "manifest",
            Self::ResultValidation => "result_validation",
        }
    }
}

const MODES: [&str; 8] = [
    "query",
    "mutation",
    "transaction",
    "key_update",
    "deployment",
    "graph",
    "call",
    "other",
];
const OUTCOMES: [&str; 3] = ["ok", "error", "panic"];
const WORK_KINDS: [&str; 8] = [
    "cells",
    "reads",
    "max_cell_nesting",
    "cell_created",
    "cell_reused",
    "cell_recycle_eligible",
    "cell_grew",
    "cell_reset",
];
const MEMORY_KINDS: [&str; 2] = ["initial", "reset"];

fn mode_index(mode: &str) -> usize {
    match mode {
        "query" => 0,
        "mutation" => 1,
        "transaction" => 2,
        "keyUpdate" | "key_update" => 3,
        "deployment" => 4,
        "graph" => 5,
        "call" => 6,
        _ => 7,
    }
}

fn bounded_mode(mode: &str) -> &'static str {
    MODES[mode_index(mode)]
}

pub(super) struct Invocation {
    started: Instant,
    mode: Cow<'static, str>,
    name: String,
    legacy: bool,
    telemetry: bool,
    span: tracing::Span,
    succeeded: AtomicBool,
    state_keys: usize,
    stages: [AtomicU64; 11],
    cells: AtomicU64,
    reads: AtomicU64,
    depth: AtomicU64,
    max_depth: AtomicU64,
    cell_created: AtomicU64,
    cell_reused: AtomicU64,
    cell_recycle_eligible: AtomicU64,
    cell_grew: AtomicU64,
    cell_reset: AtomicU64,
    cell_initial_bytes: AtomicU64,
    cell_reset_bytes: AtomicU64,
}

/// `state_keys` runs only for the opt-in log: counting records served from
/// disk walks the tree of writes not yet there, too costly for telemetry.
pub(super) fn invocation(
    mode: &str,
    name: &str,
    state_keys: impl FnOnce() -> usize,
) -> Option<Arc<Invocation>> {
    static LEGACY: OnceLock<bool> = OnceLock::new();
    let legacy = *LEGACY.get_or_init(|| {
        std::env::var("FLOWER_PROFILE_EVALUATOR")
            .is_ok_and(|value| !value.is_empty() && value != "0" && value != "false")
    });
    let telemetry = crate::telemetry::enabled();
    (legacy || telemetry).then(|| {
        let state_keys = if legacy { state_keys() } else { 0 };
        let span = if telemetry {
            tracing::info_span!(target: "flower::otel", "flower.evaluator.invocation",
                backend = "quickjs-wasm", mode = bounded_mode(mode),
                outcome = tracing::field::Empty, otel.status_code = tracing::field::Empty,
                total_seconds = tracing::field::Empty,
                coordinator_execute_seconds = tracing::field::Empty,
                coordinator_receive_wait_seconds = tracing::field::Empty,
                coordinator_active_wall_seconds = tracing::field::Empty,
                cell_wall_sum_seconds = tracing::field::Empty,
                cell_runtime_sum_seconds = tracing::field::Empty,
                cell_load_sum_seconds = tracing::field::Empty,
                cell_execute_sum_seconds = tracing::field::Empty,
                cell_read_wait_sum_seconds = tracing::field::Empty,
                cell_reset_sum_seconds = tracing::field::Empty,
                cell_active_wall_sum_seconds = tracing::field::Empty,
                bundle_prepare_seconds = tracing::field::Empty,
                manifest_seconds = tracing::field::Empty,
                result_validation_seconds = tracing::field::Empty,
                cells = tracing::field::Empty, reads = tracing::field::Empty,
                max_cell_nesting = tracing::field::Empty,
                cell_created = tracing::field::Empty, cell_reused = tracing::field::Empty,
                cell_recycle_eligible = tracing::field::Empty,
                cell_grew = tracing::field::Empty, cell_reset = tracing::field::Empty,
                cell_initial_bytes_sum = tracing::field::Empty,
                cell_reset_bytes_sum = tracing::field::Empty)
        } else {
            tracing::Span::none()
        };
        Arc::new(Invocation {
            started: Instant::now(),
            mode: if legacy {
                Cow::Owned(mode.into())
            } else {
                Cow::Borrowed(bounded_mode(mode))
            },
            // OTEL never retains method names; legacy profiling remains opt-in.
            name: if legacy { name.into() } else { String::new() },
            legacy,
            telemetry,
            span,
            succeeded: AtomicBool::new(false),
            state_keys,
            stages: std::array::from_fn(|_| AtomicU64::new(0)),
            cells: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            depth: AtomicU64::new(0),
            max_depth: AtomicU64::new(0),
            cell_created: AtomicU64::new(0),
            cell_reused: AtomicU64::new(0),
            cell_recycle_eligible: AtomicU64::new(0),
            cell_grew: AtomicU64::new(0),
            cell_reset: AtomicU64::new(0),
            cell_initial_bytes: AtomicU64::new(0),
            cell_reset_bytes: AtomicU64::new(0),
        })
    })
}

pub(super) fn span(invocation: &Option<Arc<Invocation>>) -> tracing::Span {
    invocation
        .as_ref()
        .map_or_else(tracing::Span::none, |invocation| invocation.span.clone())
}

pub(super) fn success(invocation: &Option<Arc<Invocation>>) {
    if let Some(invocation) = invocation {
        invocation.succeeded.store(true, Ordering::Relaxed);
    }
}

struct Metrics {
    invocations: Counter<u64>,
    duration: Histogram<f64>,
    stage: Histogram<f64>,
    work: Histogram<u64>,
    memory: Histogram<u64>,
}

fn duration_boundaries() -> Vec<f64> {
    vec![
        0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500,
        0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0,
        60.0,
    ]
}

fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| Metrics::new(global::meter("flower")))
}

impl Metrics {
    fn new(meter: Meter) -> Self {
        Self {
            invocations: meter.u64_counter("flower.evaluator.invocations")
                .with_description("Evaluator invocations by mode and outcome").build(),
            duration: meter.f64_histogram("flower.evaluator.invocation.duration")
                .with_unit("s").with_boundaries(duration_boundaries())
                .with_description("Evaluator invocation elapsed time").build(),
            stage: meter.f64_histogram("flower.evaluator.stage.duration")
                .with_unit("s").with_boundaries(duration_boundaries())
                .with_description("Evaluator wall-clock stages; nested cell sums overlap and active wall is not CPU time").build(),
            work: meter.u64_histogram("flower.evaluator.work.count")
                .with_boundaries(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 1_024.0, 4_096.0, 16_384.0, 65_536.0, 1_048_576.0])
                .with_description("Work counts per invocation, including cell reuse and maximum nesting").build(),
            memory: meter.u64_histogram("flower.evaluator.memory.bytes")
                .with_unit("By")
                .with_boundaries(vec![0.0, 1_024.0, 4_096.0, 16_384.0, 65_536.0, 262_144.0, 1_048_576.0, 4_194_304.0, 16_777_216.0, 67_108_864.0, 268_435_456.0, 1_073_741_824.0])
                .with_description("Sum of initial cell memory or bytes copied during reset; not peak resident memory").build(),
        }
    }

    fn bind(&self, mode: &'static str, outcome: &'static str) -> BoundMetrics {
        let attributes = [
            KeyValue::new("backend", "quickjs-wasm"),
            KeyValue::new("mode", mode),
            KeyValue::new("outcome", outcome),
        ];
        let dimension = |key, value| {
            [
                attributes[0].clone(),
                attributes[1].clone(),
                attributes[2].clone(),
                KeyValue::new(key, value),
            ]
        };
        BoundMetrics {
            invocations: self.invocations.bind(&attributes),
            duration: self.duration.bind(&attributes),
            stages: std::array::from_fn(|index| {
                let name = if index < Stage::ALL.len() {
                    Stage::ALL[index].name()
                } else if index == Stage::ALL.len() {
                    "coordinator_active_wall"
                } else {
                    "cell_active_wall_sum"
                };
                self.stage.bind(&dimension("stage", name))
            }),
            work: std::array::from_fn(|index| {
                self.work.bind(&dimension("kind", WORK_KINDS[index]))
            }),
            memory: std::array::from_fn(|index| {
                self.memory.bind(&dimension("kind", MEMORY_KINDS[index]))
            }),
        }
    }
}

struct BoundMetrics {
    invocations: BoundCounter<u64>,
    duration: BoundHistogram<f64>,
    stages: [BoundHistogram<f64>; Stage::ALL.len() + 2],
    work: [BoundHistogram<u64>; WORK_KINDS.len()],
    memory: [BoundHistogram<u64>; MEMORY_KINDS.len()],
}

fn bound_metrics(mode: &str, outcome: usize) -> &'static BoundMetrics {
    // There are exactly eight modes and three outcomes. Each set is lazily
    // bound once, so recording needs neither attribute hashing nor the SDK's
    // series-map lock. No request-derived strings can expand this registry.
    static BOUND: [OnceLock<BoundMetrics>; MODES.len() * OUTCOMES.len()] =
        [const { OnceLock::new() }; MODES.len() * OUTCOMES.len()];
    let mode = mode_index(mode);
    BOUND[mode * OUTCOMES.len() + outcome]
        .get_or_init(|| metrics().bind(MODES[mode], OUTCOMES[outcome]))
}

pub(super) struct Timer {
    started: Instant,
    invocation: Arc<Invocation>,
    stage: Stage,
}

pub(super) fn timer(invocation: &Option<Arc<Invocation>>, stage: Stage) -> Option<Timer> {
    invocation.as_ref().map(|invocation| Timer {
        started: Instant::now(),
        invocation: invocation.clone(),
        stage,
    })
}

pub(super) fn record_nanos(invocation: &Option<Arc<Invocation>>, stage: Stage, nanos: u64) {
    if let Some(invocation) = invocation {
        invocation.stages[stage as usize].fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.invocation.stages[self.stage as usize].fetch_add(elapsed, Ordering::Relaxed);
    }
}

pub(super) struct CellDepth(Arc<Invocation>);

pub(super) fn enter_cell(invocation: &Option<Arc<Invocation>>) -> Option<CellDepth> {
    invocation.as_ref().map(|invocation| {
        invocation.cells.fetch_add(1, Ordering::Relaxed);
        let depth = invocation.depth.fetch_add(1, Ordering::Relaxed) + 1;
        invocation.max_depth.fetch_max(depth, Ordering::Relaxed);
        CellDepth(invocation.clone())
    })
}

impl Drop for CellDepth {
    fn drop(&mut self) {
        self.0.depth.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) fn read(invocation: &Option<Arc<Invocation>>) {
    if let Some(invocation) = invocation {
        invocation.reads.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) fn cell_storage(
    invocation: &Option<Arc<Invocation>>,
    reused: bool,
    eligible: bool,
    initial_bytes: usize,
) {
    if let Some(invocation) = invocation {
        let counter = if reused {
            &invocation.cell_reused
        } else {
            &invocation.cell_created
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if eligible {
            invocation
                .cell_recycle_eligible
                .fetch_add(1, Ordering::Relaxed);
        }
        invocation
            .cell_initial_bytes
            .fetch_add(initial_bytes as u64, Ordering::Relaxed);
    }
}

pub(super) fn cell_reset(invocation: &Option<Arc<Invocation>>, grew: bool, bytes: usize) {
    if let Some(invocation) = invocation {
        if grew {
            invocation.cell_grew.fetch_add(1, Ordering::Relaxed);
        }
        if bytes != 0 {
            invocation.cell_reset.fetch_add(1, Ordering::Relaxed);
            invocation
                .cell_reset_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        let total = self.started.elapsed().as_secs_f64();
        if self.telemetry {
            let outcome_index = if std::thread::panicking() {
                2
            } else if self.succeeded.load(Ordering::Relaxed) {
                0
            } else {
                1
            };
            let outcome = OUTCOMES[outcome_index];
            let metrics = bound_metrics(&self.mode, outcome_index);
            metrics.invocations.add(1);
            metrics.duration.record(total);
            self.span.record("total_seconds", total);
            self.span.record("outcome", outcome);
            if outcome != "ok" {
                self.span.record("otel.status_code", "ERROR");
            }
            let seconds = |stage: Stage| {
                self.stages[stage as usize].load(Ordering::Relaxed) as f64 / 1_000_000_000.0
            };
            for stage in Stage::ALL {
                metrics.stages[stage as usize].record(seconds(stage));
            }
            let coordinator_active = (seconds(Stage::CoordinatorExecute)
                - seconds(Stage::CoordinatorReceiveWait))
            .max(0.0);
            let cell_active = (seconds(Stage::CellWall) - seconds(Stage::CellReadWait)).max(0.0);
            metrics.stages[Stage::ALL.len()].record(coordinator_active);
            metrics.stages[Stage::ALL.len() + 1].record(cell_active);
            for (field, value) in [
                (
                    "coordinator_execute_seconds",
                    seconds(Stage::CoordinatorExecute),
                ),
                (
                    "coordinator_receive_wait_seconds",
                    seconds(Stage::CoordinatorReceiveWait),
                ),
                ("coordinator_active_wall_seconds", coordinator_active),
                ("cell_wall_sum_seconds", seconds(Stage::CellWall)),
                ("cell_runtime_sum_seconds", seconds(Stage::CellRuntime)),
                ("cell_load_sum_seconds", seconds(Stage::CellLoad)),
                ("cell_execute_sum_seconds", seconds(Stage::CellExecute)),
                ("cell_read_wait_sum_seconds", seconds(Stage::CellReadWait)),
                ("cell_reset_sum_seconds", seconds(Stage::CellReset)),
                ("cell_active_wall_sum_seconds", cell_active),
                ("bundle_prepare_seconds", seconds(Stage::BundlePrepare)),
                ("manifest_seconds", seconds(Stage::Manifest)),
                (
                    "result_validation_seconds",
                    seconds(Stage::ResultValidation),
                ),
            ] {
                self.span.record(field, value);
            }
            for ((kind, value), metric) in [
                ("cells", self.cells.load(Ordering::Relaxed)),
                ("reads", self.reads.load(Ordering::Relaxed)),
                ("max_cell_nesting", self.max_depth.load(Ordering::Relaxed)),
                ("cell_created", self.cell_created.load(Ordering::Relaxed)),
                ("cell_reused", self.cell_reused.load(Ordering::Relaxed)),
                (
                    "cell_recycle_eligible",
                    self.cell_recycle_eligible.load(Ordering::Relaxed),
                ),
                ("cell_grew", self.cell_grew.load(Ordering::Relaxed)),
                ("cell_reset", self.cell_reset.load(Ordering::Relaxed)),
            ]
            .into_iter()
            .zip(&metrics.work)
            {
                metric.record(value);
                // OTEL attributes are signed; u64 falls back to a string in
                // the tracing OTEL visitor. Keep histogram counters unsigned.
                self.span
                    .record(kind, i64::try_from(value).unwrap_or(i64::MAX));
            }
            for ((field, value), metric) in [
                (
                    "cell_initial_bytes_sum",
                    self.cell_initial_bytes.load(Ordering::Relaxed),
                ),
                (
                    "cell_reset_bytes_sum",
                    self.cell_reset_bytes.load(Ordering::Relaxed),
                ),
            ]
            .into_iter()
            .zip(&metrics.memory)
            {
                metric.record(value);
                self.span
                    .record(field, i64::try_from(value).unwrap_or(i64::MAX));
            }
        }
        if !self.legacy {
            return;
        }
        let ms =
            |stage: Stage| self.stages[stage as usize].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        tracing::debug!(target: "flower::evaluator_profile",
            backend = "quickjs-wasm",
            mode = %self.mode, name = %self.name, state_keys = self.state_keys,
            total_ms = total * 1000.0,
            coordinator_execute_ms = ms(Stage::CoordinatorExecute),
            coordinator_receive_wait_ms = ms(Stage::CoordinatorReceiveWait),
            coordinator_active_wall_ms = (ms(Stage::CoordinatorExecute) - ms(Stage::CoordinatorReceiveWait)).max(0.0),
            cell_count = self.cells.load(Ordering::Relaxed),
            max_cell_nesting = self.max_depth.load(Ordering::Relaxed),
            cell_created_count = self.cell_created.load(Ordering::Relaxed),
            cell_reused_count = self.cell_reused.load(Ordering::Relaxed),
            cell_recycle_eligible_count = self.cell_recycle_eligible.load(Ordering::Relaxed),
            cell_grew_count = self.cell_grew.load(Ordering::Relaxed),
            cell_reset_count = self.cell_reset.load(Ordering::Relaxed),
            cell_initial_bytes_sum = self.cell_initial_bytes.load(Ordering::Relaxed),
            cell_reset_bytes_sum = self.cell_reset_bytes.load(Ordering::Relaxed),
            read_count = self.reads.load(Ordering::Relaxed),
            cell_wall_sum_ms = ms(Stage::CellWall),
            cell_runtime_sum_ms = ms(Stage::CellRuntime),
            cell_load_sum_ms = ms(Stage::CellLoad),
            cell_execute_sum_ms = ms(Stage::CellExecute),
            cell_read_wait_sum_ms = ms(Stage::CellReadWait),
            cell_reset_sum_ms = ms(Stage::CellReset),
            cell_active_wall_sum_ms = (ms(Stage::CellWall) - ms(Stage::CellReadWait)).max(0.0),
            "evaluator wall-clock stages; nested cell sums overlap; active wall is not CPU time");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        metrics::{
            SdkMeterProvider, Temporality,
            data::{AggregatedMetrics, MetricData, ResourceMetrics},
            exporter::PushMetricExporter,
        },
    };
    use std::{collections::BTreeMap, sync::Mutex, time::Duration};

    #[derive(Debug)]
    struct Point {
        attributes: BTreeMap<String, String>,
        count: u64,
        sum: f64,
    }

    fn attributes<'a>(values: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
        values
            .map(|value| (value.key.to_string(), value.value.as_str().into_owned()))
            .collect()
    }

    #[derive(Clone, Debug, Default)]
    struct Exporter(Arc<Mutex<BTreeMap<String, Vec<Point>>>>);

    impl PushMetricExporter for Exporter {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let mut captured = self.0.lock().unwrap();
            for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
                let points = match metric.data() {
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                        .data_points()
                        .map(|point| Point {
                            attributes: attributes(point.attributes()),
                            count: point.count(),
                            sum: point.sum(),
                        })
                        .collect(),
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                        .data_points()
                        .map(|point| Point {
                            attributes: attributes(point.attributes()),
                            count: point.count(),
                            sum: point.sum() as f64,
                        })
                        .collect(),
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| Point {
                            attributes: attributes(point.attributes()),
                            count: 0,
                            sum: point.value() as f64,
                        })
                        .collect(),
                    _ => panic!("unexpected evaluator aggregation"),
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

    #[test]
    fn bound_metrics_preserve_zero_counts_dimensions_and_cumulative_exports() {
        let exporter = Exporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let instruments = Metrics::new(provider.meter("flower-bound-test"));
        let query = instruments.bind("query", "ok");
        let mutation = instruments.bind("mutation", "error");
        let record = |metrics: &BoundMetrics, value: u64| {
            metrics.invocations.add(1);
            metrics.duration.record(value as f64);
            for stage in &metrics.stages {
                stage.record(value as f64);
            }
            for work in &metrics.work {
                work.record(value);
            }
            for memory in &metrics.memory {
                memory.record(value);
            }
        };
        record(&query, 0);
        record(&mutation, 0);
        provider.force_flush().unwrap();
        {
            let captured = exporter.0.lock().unwrap();
            for (name, count) in [
                ("flower.evaluator.stage.duration", 26),
                ("flower.evaluator.work.count", 16),
                ("flower.evaluator.memory.bytes", 4),
                ("flower.evaluator.invocation.duration", 2),
            ] {
                let points = &captured[name];
                assert_eq!(points.len(), count, "{name}");
                assert!(
                    points
                        .iter()
                        .all(|point| point.count == 1 && point.sum == 0.0)
                );
            }
        }
        // Bound handles must still refer to the live aggregate after export.
        record(&query, 2);
        provider.force_flush().unwrap();
        {
            let captured = exporter.0.lock().unwrap();
            for (name, points) in captured.iter() {
                for point in points {
                    assert_eq!(point.attributes["backend"], "quickjs-wasm");
                    let is_query = point.attributes["mode"] == "query";
                    assert_eq!(
                        point.attributes["outcome"],
                        if is_query { "ok" } else { "error" }
                    );
                    if name == "flower.evaluator.invocations" {
                        assert_eq!(point.sum, if is_query { 2.0 } else { 1.0 });
                    } else {
                        assert_eq!(point.count, if is_query { 2 } else { 1 });
                        assert_eq!(point.sum, if is_query { 2.0 } else { 0.0 });
                    }
                }
            }
            let stages = &captured["flower.evaluator.stage.duration"];
            for stage in Stage::ALL {
                assert_eq!(
                    stages
                        .iter()
                        .filter(|point| point.attributes["stage"] == stage.name())
                        .count(),
                    2
                );
            }
            for kind in WORK_KINDS {
                assert_eq!(
                    captured["flower.evaluator.work.count"]
                        .iter()
                        .filter(|point| point.attributes["kind"] == kind)
                        .count(),
                    2
                );
            }
            for kind in MEMORY_KINDS {
                assert_eq!(
                    captured["flower.evaluator.memory.bytes"]
                        .iter()
                        .filter(|point| point.attributes["kind"] == kind)
                        .count(),
                    2
                );
            }
        }
        provider.shutdown().unwrap();
    }
}
