//! What the worker runtime needs from a Flower client.

use std::error::Error as StdError;
use std::future::Future;

use serde_json::Value;
use serde_json::value::RawValue;

/// An error from a client call, as the worker reads it: the twin of the TS `FlowerError` plus
/// `isTransient`.
pub trait ClientError: StdError + Send + Sync + 'static {
    /// The HTTP status; 0 when there was none.
    fn status(&self) -> u16;
    /// The server's `error.code`, or an SDK or transport code.
    fn code(&self) -> &str;
    /// `failure.code`, when the method itself failed (`LEASE_LOST`, ...).
    fn failure_code(&self) -> Option<&str>;
    /// `isTransient`: network failures, timeouts, stalls, 408, 425, 429 and 5xx; never a method's
    /// own failure.
    fn is_transient(&self) -> bool;
    /// How worker events describe the error: `worker.ts`'s `message()`, which gives
    /// `"<failure.code>: <failure.message>"` or `"<code>: <message>"` for a `FlowerError`, the
    /// Node code (`ECONNREFUSED`) for a network failure, and the message otherwise.
    fn describe(&self) -> String;
}

/// The retry policy of one call, the twin of the TS `RetryPolicy` (`retryable` aside: it is always
/// `isTransient`). `None` fields take the client's defaults: 8 attempts, 250 ms doubling with
/// jitter up to 30 s, 20 s per attempt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts including the first.
    pub attempts: Option<u32>,
    /// No retry starts after this epoch-millisecond deadline (on the worker's [`Clock`]); the first
    /// attempt always runs.
    ///
    /// [`Clock`]: crate::Clock
    pub until: Option<i64>,
    pub initial_delay_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
    /// Abort each attempt after this long.
    pub timeout_ms: Option<u64>,
}

/// Whether a watched value is the one awaited.
pub type Predicate = fn(&Value) -> bool;

/// The calls the worker makes. Arguments arrive serialized, with their keys in the order the TS
/// SDK sends them; implementations pass them through verbatim.
pub trait QueueClient: Clone + Send + Sync + 'static {
    type Error: ClientError;

    /// `client.mutate(name, args, {retry})`: the mutation's value. One request ID serves every
    /// attempt, so a retried call applies once.
    fn mutate(
        &self,
        name: &str,
        args: Box<RawValue>,
        retry: RetryPolicy,
    ) -> impl Future<Output = Result<Value, Self::Error>> + Send;

    /// `client.waitUntil(name, args, predicate)`: the first value of the query's subscription that
    /// satisfies `predicate`. The subscription reconnects on transient errors (with the client's
    /// backoff) and fails on the others. The worker drops the future to stop waiting.
    fn wait_until(
        &self,
        name: &str,
        args: Box<RawValue>,
        predicate: Predicate,
    ) -> impl Future<Output = Result<Value, Self::Error>> + Send;
}

/// JavaScript truthiness of a JSON value: `Boolean(value)`.
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Value::String(text) => !text.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// `Array.isArray(items) && items.length > 0`: a pool's `next` has work.
pub fn non_empty_array(value: &Value) -> bool {
    value.as_array().is_some_and(|items| !items.is_empty())
}

/// `backoff(attempt, initialDelayMs, maxDelayMs)`: jittered exponential backoff between half and
/// all of `initial·2^attempt`, capped (equal jitter).
pub fn backoff(attempt: u32, initial_delay_ms: u64, max_delay_ms: u64) -> u64 {
    let ceiling =
        (initial_delay_ms as f64 * 2f64.powi(attempt.min(30) as i32)).min(max_delay_ms as f64);
    js_round(ceiling / 2.0 + fastrand::f64() * ceiling / 2.0) as u64
}

/// `backoff(attempt)` with the SDK's defaults: 250 ms up to 30 s.
pub fn default_backoff(attempt: u32) -> u64 {
    backoff(attempt, 250, 30_000)
}

/// `Math.round`: halves round up.
pub(crate) fn js_round(value: f64) -> f64 {
    (value + 0.5).floor()
}
