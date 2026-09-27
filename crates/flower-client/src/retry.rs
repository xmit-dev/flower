//! Retry policies and `backoff`, the twins of `RetryPolicy`/`attempt()`/`backoff()` in `client.ts`.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio_util::sync::CancellationToken;

use crate::error::FlowerError;

/// Jittered exponential backoff between half and all of `initial·2^attempt`, capped at `max`
/// ("equal jitter"), rounded to whole milliseconds like `Math.round`.
pub fn backoff(attempt: u32, initial: Duration, max: Duration) -> Duration {
    Duration::from_millis(backoff_ms(
        attempt,
        initial.as_millis() as f64,
        max.as_millis() as f64,
    ))
}

/// [`backoff`] in milliseconds, with the TS defaults available as [`DEFAULT_INITIAL_DELAY`] and
/// [`DEFAULT_MAX_DELAY`].
pub fn backoff_ms(attempt: u32, initial_ms: f64, max_ms: f64) -> u64 {
    let ceiling = max_ms.min(initial_ms * 2f64.powi(attempt.min(30) as i32));
    let delay = ceiling / 2.0 + fastrand::f64() * ceiling / 2.0;
    // Math.round: halves round toward +∞.
    (delay + 0.5).floor().max(0.0) as u64
}

/// 250 ms, doubling with jitter.
pub const DEFAULT_INITIAL_DELAY: Duration = Duration::from_millis(250);
/// 30 s.
pub const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(30);
/// Attempts including the first.
pub const DEFAULT_ATTEMPTS: u32 = 8;
/// Per attempt: room for a fresh read fence and a full evaluation.
pub const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);

/// Whether a failed attempt may be retried; defaults to [`FlowerError::is_transient`].
pub type Retryable = Arc<dyn Fn(&FlowerError) -> bool + Send + Sync>;

/// `RetryPolicy` from `client.ts`. `RetryPolicy::default()` is what `retry: true` means.
#[derive(Clone)]
pub struct RetryPolicy {
    /// Attempts including the first. Default 8.
    pub attempts: u32,
    /// No retry starts at or after this wall-clock instant (`Date.now() + delay >= until`); the
    /// first attempt always runs.
    pub until: Option<SystemTime>,
    /// Default 250 ms, doubling with jitter.
    pub initial_delay: Duration,
    /// Default 30 s.
    pub max_delay: Duration,
    /// Abort each attempt (request and reply body) after this long. Default 20 s.
    pub timeout: Duration,
    /// Defaults to [`FlowerError::is_transient`].
    pub retryable: Option<Retryable>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            attempts: DEFAULT_ATTEMPTS,
            until: None,
            initial_delay: DEFAULT_INITIAL_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
            timeout: DEFAULT_ATTEMPT_TIMEOUT,
            retryable: None,
        }
    }
}

impl RetryPolicy {
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn until(mut self, until: SystemTime) -> Self {
        self.until = Some(until);
        self
    }

    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn retryable(
        mut self,
        retryable: impl Fn(&FlowerError) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retryable = Some(Arc::new(retryable));
        self
    }

    fn may_retry(&self, error: &FlowerError) -> bool {
        match &self.retryable {
            Some(retryable) => retryable(error),
            None => error.is_transient(),
        }
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("attempts", &self.attempts)
            .field("until", &self.until)
            .field("initial_delay", &self.initial_delay)
            .field("max_delay", &self.max_delay)
            .field("timeout", &self.timeout)
            .field("retryable", &self.retryable.as_ref().map(|_| "custom"))
            .finish()
    }
}

/// `retry?: RetryPolicy | boolean`.
#[derive(Clone, Debug, Default)]
pub enum Retry {
    /// `false` / omitted: one attempt, no per-attempt timeout (only the transport's deadline).
    #[default]
    Off,
    /// `true`: [`RetryPolicy::default()`].
    Default,
    Policy(RetryPolicy),
}

impl From<bool> for Retry {
    fn from(retry: bool) -> Self {
        if retry { Retry::Default } else { Retry::Off }
    }
}

impl From<RetryPolicy> for Retry {
    fn from(policy: RetryPolicy) -> Self {
        Retry::Policy(policy)
    }
}

impl Retry {
    /// The policy in effect, or `None` for [`Retry::Off`].
    pub fn policy(&self) -> Option<RetryPolicy> {
        match self {
            Retry::Off => None,
            Retry::Default => Some(RetryPolicy::default()),
            Retry::Policy(policy) => Some(policy.clone()),
        }
    }
}

/// Resolve `future` unless `cancel` fires first.
pub(crate) async fn cancellable<T>(
    cancel: Option<&CancellationToken>,
    future: impl Future<Output = Result<T, FlowerError>>,
) -> Result<T, FlowerError> {
    match cancel {
        None => future.await,
        Some(cancel) => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(FlowerError::aborted()),
                result = future => result,
            }
        }
    }
}

/// `attempt()` from `client.ts`: run `send` under the retry setting. Never retries a caller abort.
pub async fn run<T, F, Fut>(
    retry: &Retry,
    cancel: Option<&CancellationToken>,
    mut send: F,
) -> Result<T, FlowerError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FlowerError>>,
{
    let default;
    let policy = match retry {
        Retry::Off => {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err(FlowerError::aborted());
            }
            return cancellable(cancel, send()).await;
        }
        Retry::Default => {
            default = RetryPolicy::default();
            &default
        }
        Retry::Policy(policy) => policy,
    };
    let mut attempt = 0u32;
    loop {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(FlowerError::aborted());
        }
        let attempted = cancellable(cancel, async {
            match tokio::time::timeout(policy.timeout, send()).await {
                Ok(result) => result,
                Err(_) => Err(FlowerError::timeout()),
            }
        })
        .await;
        let error = match attempted {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if error.is_aborted()
            || cancel.is_some_and(CancellationToken::is_cancelled)
            || !policy.may_retry(&error)
            || attempt.saturating_add(1) >= policy.attempts
        {
            return Err(error);
        }
        let delay = backoff(attempt, policy.initial_delay, policy.max_delay);
        if let Some(until) = policy.until
            && SystemTime::now() + delay >= until
        {
            return Err(error);
        }
        cancellable(cancel, async {
            tokio::time::sleep(delay).await;
            Ok(())
        })
        .await?;
        attempt += 1;
    }
}
