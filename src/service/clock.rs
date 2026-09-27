use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::consensus::Snapshot;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Wall time supplies epoch meaning; elapsed time and the observed floor prevent
/// local clock corrections from moving time backwards within this process.
pub(super) struct Clock {
    started: Instant,
    epoch_ms: u64,
    observed: AtomicU64,
}

impl Clock {
    pub(super) fn new() -> Self {
        Self {
            started: Instant::now(),
            epoch_ms: wall_ms(),
            observed: AtomicU64::new(0),
        }
    }

    pub(super) fn sample(&self, state: &Snapshot) -> anyhow::Result<u64> {
        self.sample_after(committed(state))
    }

    /// A sample no earlier than a snapshot's committed clock, taken later.
    pub(super) fn sample_after(&self, committed: u64) -> anyhow::Result<u64> {
        let elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let sampled = wall_ms()
            .max(self.epoch_ms.saturating_add(elapsed_ms))
            .max(committed);
        anyhow::ensure!(
            sampled <= MAX_SAFE_INTEGER,
            "server clock exceeds the safe integer range"
        );
        Ok(self
            .observed
            .fetch_max(sampled, Ordering::Relaxed)
            .max(sampled))
    }
}

pub(super) fn committed(state: &Snapshot) -> u64 {
    state
        .data
        .get("clock")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_committed_future_timestamp_stays_a_floor_for_subsequent_samples() {
        let clock = Clock::new();
        let mut future = Snapshot::default();
        let deadline = wall_ms() + 60_000;
        future.data.insert("clock".into(), json!(deadline));
        assert_eq!(clock.sample(&future).unwrap(), deadline);
        assert_eq!(clock.sample(&Snapshot::default()).unwrap(), deadline);
    }
}
