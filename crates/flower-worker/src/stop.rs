//! What a job gets besides its claim: why it must stop, and a line back to its worker.

use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, OnceLock};

use tokio_util::sync::CancellationToken;

/// Node's reason for a bare `AbortController.abort()`.
pub(crate) const ABORTED: &str = "This operation was aborted";

/// The job's `AbortSignal`: stopped once its lease runs out or is lost, or once a stopping worker's
/// `drain_ms` ends. The first reason given stays, like `signal.reason`.
#[derive(Clone, Debug, Default)]
pub struct JobStop {
    token: CancellationToken,
    reason: Arc<OnceLock<String>>,
}

impl JobStop {
    pub fn new() -> Self {
        JobStop::default()
    }

    /// A stop that also fires when `parent` is cancelled, with no reason of its own.
    pub(crate) fn child_of(parent: &CancellationToken) -> Self {
        JobStop {
            token: parent.child_token(),
            reason: Arc::default(),
        }
    }

    /// `signal.aborted`.
    pub fn is_stopped(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves once the job must stop.
    pub async fn stopped(&self) {
        self.token.cancelled().await
    }

    /// `signal.reason`'s message: "The lease ran out", "The lease was lost", "The worker stopped
    /// before the job finished", or Node's generic abort message when the stop came from a parent.
    pub fn reason(&self) -> Option<&str> {
        if !self.is_stopped() {
            return None;
        }
        Some(self.reason.get().map(String::as_str).unwrap_or(ABORTED))
    }

    /// The token behind this stop, to hand to APIs that take one (cancel a child token, not this).
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// `controller.abort(new Error(reason))`: only the first stop counts.
    pub fn stop(&self, reason: &str) {
        if self.token.is_cancelled() {
            return;
        }
        if self.reason.set(reason.to_owned()).is_ok() {
            self.token.cancel();
        }
    }
}

/// Why a job or computation failed: the text a `fail` report and a `failed` event carry. Any
/// `std::error::Error` converts with `?`; [`WorkError::new`] takes a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkError {
    message: String,
}

impl WorkError {
    pub fn new(message: impl Into<String>) -> Self {
        WorkError {
            message: message.into(),
        }
    }

    /// A client error, described as `worker.ts`'s `message()` does.
    pub fn client<E: crate::ClientError>(error: &E) -> Self {
        WorkError::new(error.describe())
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for WorkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl<E: StdError + Send + Sync + 'static> From<E> for WorkError {
    fn from(error: E) -> Self {
        WorkError::new(crate::describe_error(&error))
    }
}

/// What a job can tell the worker running it (`QueueWorkerControl`).
#[derive(Clone)]
pub struct JobControl {
    pub(crate) target: Arc<dyn Control>,
    pub(crate) key: u64,
}

pub(crate) trait Control: Send + Sync {
    fn throttle(&self, ms: u64, reason: &str);
    fn idle(&self, key: u64);
}

impl JobControl {
    /// The service behind this job refused it for being asked too much: claim nothing for `ms`,
    /// and hold fewer jobs.
    pub fn throttle(&self, ms: u64, reason: &str) {
        self.target.throttle(ms, reason)
    }

    /// From now on this job mostly waits, on a child process or a server say, and loads the
    /// process little: it stops counting toward concurrency, so long jobs can't keep the worker
    /// from taking short ones.
    pub fn idle(&self) {
        self.target.idle(self.key)
    }
}

impl fmt::Debug for JobControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobControl").field("key", &self.key).finish()
    }
}
