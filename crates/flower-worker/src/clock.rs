//! The worker's `Date.now()`.

use std::time::{SystemTime, UNIX_EPOCH};

/// Epoch milliseconds, like `Date.now()`. [`Clock::system`] reads the wall clock, exactly as the TS
/// SDK does; [`Clock::tokio`] counts from an anchor on tokio's clock, so a test under
/// `tokio::time::pause` sees time move only when the runtime advances it.
#[derive(Clone, Debug)]
pub struct Clock {
    anchor: Option<(i64, tokio::time::Instant)>,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::system()
    }
}

/// The wall clock in epoch milliseconds.
pub fn system_now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_millis() as i64,
        Err(before) => -(before.duration().as_millis() as i64),
    }
}

impl Clock {
    /// `Date.now()`: the wall clock.
    pub fn system() -> Self {
        Clock { anchor: None }
    }

    /// The wall clock at this moment, then tokio's clock: deterministic under `tokio::time::pause`.
    pub fn tokio() -> Self {
        Clock {
            anchor: Some((system_now_ms(), tokio::time::Instant::now())),
        }
    }

    /// A tokio clock that reads `epoch_ms` now.
    pub fn tokio_at(epoch_ms: i64) -> Self {
        Clock {
            anchor: Some((epoch_ms, tokio::time::Instant::now())),
        }
    }

    pub fn now_ms(&self) -> i64 {
        match &self.anchor {
            None => system_now_ms(),
            Some((epoch, at)) => epoch + at.elapsed().as_millis() as i64,
        }
    }
}
