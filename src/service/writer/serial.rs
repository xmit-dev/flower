//! Keep consecutive serial calls on one blocking worker. The writer still owns
//! ordering and the sole private overlay; only task dispatch is amortized.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy)]
pub(super) enum Execution {
    Dispatch,
    // Only the blocking batch worker may select this mode.
    Inline,
}

impl Execution {
    pub async fn evaluate<F, T>(self, evaluate: F) -> Result<T, ApiError>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        if !crate::telemetry::enabled() {
            return self.evaluate_inner(evaluate).await;
        }
        let execution = match self {
            Self::Dispatch => "dispatch",
            Self::Inline => "inline",
        };
        let trace = tracing::info_span!(target: "flower::otel", "flower.writer.evaluate",
            execution, status = tracing::field::Empty);
        let worker_trace = trace.clone();
        let queued = Instant::now();
        let result = self
            .evaluate_inner(move || {
                worker_trace.in_scope(|| {
                    if matches!(self, Self::Dispatch) {
                        observability::stage("blocking_queue", queued.elapsed(), execution);
                    }
                    let started = Instant::now();
                    let result = evaluate();
                    observability::stage("evaluation", started.elapsed(), execution);
                    result
                })
            })
            .instrument(trace.clone())
            .await;
        observability::status(&trace, observability::outcome(&result));
        result
    }

    async fn evaluate_inner<F, T>(self, evaluate: F) -> Result<T, ApiError>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let result = match self {
            Self::Dispatch => tokio::task::spawn_blocking(evaluate)
                .await
                .map_err(|error| error.to_string()),
            Self::Inline => std::panic::catch_unwind(std::panic::AssertUnwindSafe(evaluate))
                .map_err(|_| "serial evaluation worker panicked".to_owned()),
        };
        result
            .map_err(|error| {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "WORKER_FAILED", error)
            })?
            .map_err(evaluation_error)
    }
}

pub(super) struct Batch {
    pub state: Snapshot,
    pub commands: Vec<SharedCommit>,
    pub results: Vec<Result<Prepared, ApiError>>,
    pub bytes: usize,
    pub admission: Option<admission::Permit>,
    pub stop_reason: Option<&'static str>,
}

// A canceled preparation may finish its current evaluation, as before, but
// must not keep spending its reservation on the rest of the detached batch.
struct Cancellation {
    stopped: AtomicBool,
    wakeup: tokio::sync::Notify,
}
struct CancelOnDrop(Arc<Cancellation>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.wakeup.notify_one();
    }
}

impl Batch {
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare(
        mut self,
        app: Arc<App>,
        inputs: Vec<PendingInput>,
        preparing: Instant,
        budget: Duration,
        prior_results: usize,
        maintaining_graph: bool,
        // A pipelined successor's predecessor; see `Arrivals`.
        predecessor_done: Option<Arc<AtomicBool>>,
    ) -> Self {
        let canceled = CancelOnDrop(Arc::new(Cancellation {
            stopped: AtomicBool::new(false),
            wakeup: tokio::sync::Notify::new(),
        }));
        let cancellation = canceled.0.clone();
        let runtime = tokio::runtime::Handle::current();
        let telemetry = crate::telemetry::enabled();
        let queued = telemetry.then(Instant::now);
        let trace = if telemetry {
            tracing::info_span!(target: "flower::otel", "flower.writer.serial_batch",
                requests = observability::count(inputs.len()), prepared = tracing::field::Empty)
        } else {
            tracing::Span::none()
        };
        if telemetry {
            for input in &inputs {
                observability::link(&trace, &input.trace);
            }
        }
        let worker = tokio::task::spawn_blocking(move || {
            if let Some(queued) = queued {
                observability::stage("blocking_queue", queued.elapsed(), "serial_batch");
            }
            let started = telemetry.then(Instant::now);
            // Public serial calls have no asynchronous authorization evaluator.
            // The shared guards can still wait for the per-app evaluation gate;
            // blocking here never occupies a Tokio scheduler worker.
            let result = runtime.block_on(
                async {
                    for input in inputs {
                        // A failed target replay restores ordinary optimistic
                        // preparation on the very next call in this same group.
                        if maintaining_graph
                            && !evaluator::staging::maintaining_graph(&self.state.data)
                        {
                            break;
                        }
                        if cancellation.stopped.load(Ordering::Acquire) {
                            break;
                        }
                        if prior_results + self.results.len() > 0 {
                            // A successor keeps preparing while its predecessor
                            // is durable-pending, then stops without delaying
                            // its own submission. Other groups keep the budget.
                            let stop = match &predecessor_done {
                                Some(done) => done
                                    .load(Ordering::Acquire)
                                    .then_some("predecessor_completed"),
                                None => {
                                    (preparing.elapsed() >= budget).then_some("preparation_time")
                                }
                            };
                            if stop.is_some() {
                                self.stop_reason = stop;
                                break;
                            }
                        }
                        if authorization::required(&self.state) {
                            break;
                        }
                        let preparing = prepare_serial_input(
                            &app,
                            &self.state,
                            &input,
                            false,
                            &mut self.admission,
                            None,
                            Execution::Inline,
                        );
                        let prepared = tokio::select! {
                            biased;
                            _ = cancellation.wakeup.notified() => break,
                            prepared = preparing => prepared,
                        };
                        let Some(result) = stage_prepared(
                            &app,
                            &mut self.state,
                            prepared,
                            &mut self.commands,
                            &mut self.bytes,
                        ) else {
                            self.stop_reason = Some("encoded_bytes");
                            break;
                        };
                        self.results.push(result);
                    }
                    self
                }
                .instrument(trace.clone()),
            );
            if let Some(started) = started {
                trace.record("prepared", observability::count(result.results.len()));
                observability::stage("serial_batch", started.elapsed(), "worker");
            }
            result
        });
        let result = worker.await.expect("serial preparation worker failed");
        drop(canceled);
        result
    }
}
