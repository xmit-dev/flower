//! [`QueueClient`] for [`flower_client::FlowerClient`], and [`ClientError`] for its errors.

use std::time::{Duration, SystemTime};

use flower_client::{ErrorKind, FlowerClient, FlowerError, MutationOptions, Retry, SubscribeOptions};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::client::{ClientError, Predicate, QueueClient, RetryPolicy};

impl ClientError for FlowerError {
    fn status(&self) -> u16 {
        self.status
    }

    fn code(&self) -> &str {
        &self.code
    }

    fn failure_code(&self) -> Option<&str> {
        FlowerError::failure_code(self)
    }

    fn is_transient(&self) -> bool {
        FlowerError::is_transient(self)
    }

    /// `message(error)` in `worker.ts`: a `FlowerError` gives `"<failure.code>: <failure.message>"`
    /// or `"<code>: <message>"`; a network failure its Node code (`error.cause.code`); a timeout,
    /// cancel, decode or argument error (TS: a `DOMException`, `SyntaxError` or `TypeError`) its message.
    fn describe(&self) -> String {
        match self.kind {
            ErrorKind::Flower => match &self.failure {
                Some(failure) => format!("{}: {}", failure.code, failure.message),
                None => format!("{}: {}", self.code, self.message),
            },
            ErrorKind::Transport => self.code.clone(),
            _ => self.message.clone(),
        }
    }
}

/// The worker's retry policy on the client's: unset fields keep the client's defaults (8 attempts,
/// 250 ms to 30 s, 20 s per attempt), `until` is epoch milliseconds on the system clock (the
/// worker's default [`Clock`](crate::Clock)).
pub fn retry_policy(retry: &RetryPolicy) -> flower_client::RetryPolicy {
    let mut policy = flower_client::RetryPolicy::default();
    if let Some(attempts) = retry.attempts {
        policy = policy.attempts(attempts);
    }
    if let Some(until) = retry.until {
        policy = policy.until(SystemTime::UNIX_EPOCH + Duration::from_millis(until.max(0) as u64));
    }
    if let Some(ms) = retry.initial_delay_ms {
        policy = policy.initial_delay(Duration::from_millis(ms));
    }
    if let Some(ms) = retry.max_delay_ms {
        policy = policy.max_delay(Duration::from_millis(ms));
    }
    if let Some(ms) = retry.timeout_ms {
        policy = policy.timeout(Duration::from_millis(ms));
    }
    policy
}

/// Arguments go out verbatim (a [`RawValue`] serializes as itself), so their keys keep the order
/// the TS SDK sends. Keep the worker on the system clock with this client: retry deadlines are
/// wall-clock instants.
impl QueueClient for FlowerClient {
    type Error = FlowerError;

    async fn mutate(&self, name: &str, args: Box<RawValue>, retry: RetryPolicy) -> Result<Value, FlowerError> {
        let options = MutationOptions::new().retry(Retry::Policy(retry_policy(&retry)));
        let reply = FlowerClient::mutate::<RawValue, Value>(self, name, &args, options).await?;
        Ok(reply.value)
    }

    async fn wait_until(&self, name: &str, args: Box<RawValue>, predicate: Predicate) -> Result<Value, FlowerError> {
        let update = FlowerClient::wait_until::<RawValue, Value, _>(self, name, &args, predicate, SubscribeOptions::default()).await?;
        Ok(update.value)
    }
}
