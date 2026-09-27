//! Cold preparation stages only; the ordinary prepared-image hit stays untouched.
use anyhow::Result;
use opentelemetry::{KeyValue, global, metrics::Histogram};
use std::{sync::OnceLock, time::Instant};

struct Observation {
    stage: &'static str,
    started: Instant,
    span: tracing::Span,
    success: bool,
}

impl Drop for Observation {
    fn drop(&mut self) {
        let outcome = if self.success { "ok" } else { "error" };
        self.span.record("outcome", outcome);
        if !self.success {
            self.span.record("otel.status_code", "ERROR");
        }
        static DURATION: OnceLock<Histogram<f64>> = OnceLock::new();
        DURATION.get_or_init(|| global::meter("flower")
            .f64_histogram("flower.evaluator.bundle.preparation.duration")
            .with_description("Cold bundle preparation wall time; total includes nested stages and coalesced_wait is waiting only")
            .with_unit("s")
            .with_boundaries(vec![0.00001, 0.0001, 0.001, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
            .build())
            .record(self.started.elapsed().as_secs_f64(), &[
                KeyValue::new("stage", self.stage), KeyValue::new("outcome", outcome)
            ]);
    }
}

pub(super) fn observe<T, E>(
    stage: &'static str,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    if !crate::telemetry::enabled() {
        return operation();
    }
    let mut observation = Observation {
        stage,
        started: Instant::now(),
        span: tracing::info_span!(target: "flower::otel", "flower.evaluator.bundle.prepare", stage,
            outcome = tracing::field::Empty, otel.status_code = tracing::field::Empty),
        success: false,
    };
    let result = observation.span.in_scope(operation);
    observation.success = result.is_ok();
    result
}
