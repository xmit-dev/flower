//! Helpers shared by the ported SDK tests: the TS tests' `start`, `until`, `deferred`.
#![allow(dead_code)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use flower_worker::testing::{FakeClient, FakeError};
use flower_worker::{
    Claim, JobControl, JobStop, Load, QueueWorkerEvent, QueueWorkerOptions, WorkError, WorkerError,
    run_queue_worker,
};
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub type Outcome = Result<(), WorkerError<FakeError>>;

/// `until(condition)`: poll every 2 ms, give up after 3 s.
pub async fn until(condition: impl Fn() -> bool) {
    let started = tokio::time::Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "Timed out waiting for the worker"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

pub async fn sleep(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await
}

/// `deferred()`.
#[derive(Clone, Default)]
pub struct Deferred(CancellationToken);

impl Deferred {
    pub fn new() -> Self {
        Deferred::default()
    }
    pub fn resolve(&self) {
        self.0.cancel()
    }
    pub async fn wait(&self) {
        self.0.cancelled().await
    }
    pub fn resolved(&self) -> bool {
        self.0.is_cancelled()
    }
}

/// `leaseEnd(signal)`: fail once the job's stop fires, with its reason.
pub async fn lease_end<T>(stop: JobStop) -> Result<T, WorkError> {
    stop.stopped().await;
    Err(WorkError::new(stop.reason().unwrap_or_default()))
}

pub fn idle() -> Load {
    Load::idle()
}

/// A running worker (`start(client, options)`).
pub struct Started {
    pub events: Arc<Mutex<Vec<QueueWorkerEvent>>>,
    stop: CancellationToken,
    done: Option<JoinHandle<Outcome>>,
}

impl Started {
    pub fn events(&self) -> Vec<QueueWorkerEvent> {
        self.events.lock().clone()
    }
    pub fn types(&self) -> Vec<&'static str> {
        self.events
            .lock()
            .iter()
            .filter(|event| event.kind() != "limit")
            .map(|event| event.kind())
            .collect()
    }
    pub fn count(&self, kind: &str) -> usize {
        self.events
            .lock()
            .iter()
            .filter(|event| event.kind() == kind)
            .count()
    }
    pub fn limits(&self) -> Vec<u64> {
        self.events
            .lock()
            .iter()
            .filter_map(|event| match event {
                QueueWorkerEvent::Limit { limit, .. } => Some(*limit),
                _ => None,
            })
            .collect()
    }
    /// Stop and wait for the worker.
    pub async fn stop(mut self) -> Outcome {
        self.stop.cancel();
        self.done
            .take()
            .expect("not stopped yet")
            .await
            .expect("the worker task ran")
    }
    /// Stop without waiting; await the returned handle later.
    pub fn stop_later(mut self) -> JoinHandle<Outcome> {
        self.stop.cancel();
        self.done.take().expect("not stopped yet")
    }
    /// Wait for the worker to end on its own.
    pub async fn done(mut self) -> Outcome {
        self.done
            .take()
            .expect("not stopped yet")
            .await
            .expect("the worker task ran")
    }
}

/// `start(client, options)`: queue `jobs`, an idle health, events recorded, the fake's clock.
pub fn start<P, R, W, F>(
    client: &FakeClient,
    configure: impl FnOnce(QueueWorkerOptions) -> QueueWorkerOptions,
    work: W,
) -> Started
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    W: Fn(Claim<P>, JobStop, JobControl) -> F + Send + Sync + 'static,
    F: Future<Output = Result<R, WorkError>> + Send + 'static,
{
    let stop = CancellationToken::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = events.clone();
    let mut options = QueueWorkerOptions::new("jobs", stop.clone());
    options.health = Some(Arc::new(idle));
    options.clock = client.flower().clock().clone();
    options.on_event = Some(Arc::new(move |event| recorded.lock().push(event)));
    let options = configure(options);
    let done = tokio::spawn(run_queue_worker(client.clone(), options, work));
    Started {
        events,
        stop,
        done: Some(done),
    }
}
