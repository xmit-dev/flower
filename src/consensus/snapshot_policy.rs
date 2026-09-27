//! Additional soft snapshot triggers. OpenRaft's entry-count trigger remains
//! independent and may bypass this scheduler's measured duty-cycle cooldown.
use super::{FlowerRaft, Limits};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(super) struct Accounting(Mutex<State>);
struct State {
    total: u128,
    snapshotted: u128,
    applied: Option<u64>,
    snapshot: Option<u64>,
    completed: Instant,
    last_finished: Instant,
    duration: Duration,
    active: usize,
    builds: u64,
}
#[derive(Clone, Copy)]
pub(super) struct Capture {
    pub total: u128,
}
struct Timer {
    accounting: Arc<Accounting>,
    started: Instant,
}
pub(super) struct Activity(Arc<Timer>);
impl Clone for Activity {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let mut state = self.accounting.0.lock().expect("snapshot accounting");
        state.active -= 1;
        state.last_finished = Instant::now();
        state.duration = self.started.elapsed();
        state.builds = state.builds.saturating_add(1);
    }
}
impl Accounting {
    pub fn new(bytes: u128, applied: Option<u64>, snapshot: Option<u64>) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self(Mutex::new(State {
            total: bytes,
            snapshotted: 0,
            applied,
            snapshot,
            completed: now,
            last_finished: now,
            duration: Duration::ZERO,
            active: 0,
            builds: 0,
        })))
    }
    pub fn applied(&self, bytes: u128, index: Option<u64>) {
        let mut state = self.0.lock().expect("snapshot accounting");
        state.total = state.total.saturating_add(bytes);
        state.applied = index;
    }
    pub fn capture(&self) -> Capture {
        Capture {
            total: self.0.lock().expect("snapshot accounting").total,
        }
    }
    pub fn published(&self, capture: Capture, index: Option<u64>) {
        let mut state = self.0.lock().expect("snapshot accounting");
        state.snapshotted = capture.total;
        state.snapshot = index;
        state.completed = Instant::now();
    }
    pub fn installed(&self, index: Option<u64>) {
        let mut state = self.0.lock().expect("snapshot accounting");
        state.total = 0;
        state.snapshotted = 0;
        state.applied = index;
        state.snapshot = index;
        state.completed = Instant::now();
    }
    pub fn begin(self: &Arc<Self>) -> Activity {
        self.0.lock().expect("snapshot accounting").active += 1;
        Activity(Arc::new(Timer {
            accounting: self.clone(),
            started: Instant::now(),
        }))
    }
    pub fn reason(&self, limits: &Limits, now: Instant) -> Option<&'static str> {
        self.0
            .lock()
            .expect("snapshot accounting")
            .reason(limits, now)
    }
    pub fn metrics(&self, limits: &Limits) -> Value {
        let state = self.0.lock().expect("snapshot accounting");
        json!({"unsnapshottedLogBytes":state.total.saturating_sub(state.snapshotted).min(u64::MAX as u128) as u64,
            "afterBytes":limits.snapshot_after_bytes,"maxAgeMs":limits.snapshot_max_age.as_millis(),
            "checkMs":limits.snapshot_check_interval.as_millis(),"dutyPercent":limits.snapshot_duty_percent,
            "snapshotAgeMs":state.completed.elapsed().as_millis(),"lastBuildMs":state.duration.as_millis(),
            "cooldownMs":cooldown(state.duration,limits.snapshot_duty_percent).as_millis(),
            "activeBuilds":state.active,"completedBuildAttempts":state.builds,
            "appliedIndex":state.applied,"snapshotIndex":state.snapshot,"due":state.reason(limits,Instant::now())})
    }
}
fn cooldown(duration: Duration, percent: u8) -> Duration {
    // Duration arithmetic saturates rather than wrapping on unusually long jobs.
    duration.saturating_mul((100 - percent) as u32) / u32::from(percent)
}
impl State {
    fn reason(&self, limits: &Limits, now: Instant) -> Option<&'static str> {
        if self.active > 0
            || self.applied <= self.snapshot
            || now.saturating_duration_since(self.last_finished)
                < cooldown(self.duration, limits.snapshot_duty_percent)
        {
            return None;
        }
        if limits.snapshot_after_bytes > 0
            && self.total.saturating_sub(self.snapshotted)
                >= u128::from(limits.snapshot_after_bytes)
        {
            Some("applied_bytes")
        } else if !limits.snapshot_max_age.is_zero()
            && now.saturating_duration_since(self.completed) >= limits.snapshot_max_age
        {
            Some("age")
        } else {
            None
        }
    }
}
pub(super) fn spawn(
    raft: FlowerRaft,
    accounting: Arc<Accounting>,
    limits: Arc<Limits>,
) -> Arc<tokio::sync::watch::Sender<()>> {
    let (lifetime, mut dropped) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        let mut metrics = raft.metrics();
        let mut timer = tokio::time::interval(limits.snapshot_check_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if metrics.borrow().running_state.is_err()
                || metrics.borrow().state == openraft::ServerState::Shutdown
            {
                return;
            }
            tokio::select! {
                _=dropped.changed()=>return,
                changed=metrics.changed()=>{if changed.is_err(){return;}},
                _=timer.tick()=>{
                    if let Some(reason)=accounting.reason(&limits,Instant::now()) {
                        tracing::debug!(reason,policy=%accounting.metrics(&limits),"additional snapshot trigger");
                        if raft.trigger().snapshot().await.is_err(){return;}
                    }
                }
            }
        }
    });
    Arc::new(lifetime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_prefix_preserves_bytes_applied_while_building() {
        let limits = Limits {
            snapshot_after_bytes: 100,
            snapshot_max_age: Duration::from_secs(60),
            ..Limits::default()
        };
        let accounting = Accounting::new(0, None, None);
        accounting.applied(100, Some(1));
        assert_eq!(
            accounting.reason(&limits, Instant::now()),
            Some("applied_bytes")
        );
        let capture = accounting.capture();
        let activity = accounting.begin();
        accounting.applied(25, Some(2));
        assert_eq!(accounting.reason(&limits, Instant::now()), None);
        accounting.published(capture, Some(1));
        drop(activity);
        assert_eq!(accounting.metrics(&limits)["unsnapshottedLogBytes"], 25);
        assert_eq!(accounting.metrics(&limits)["snapshotIndex"], 1);
        assert_eq!(accounting.metrics(&limits)["appliedIndex"], 2);
        accounting.installed(Some(50));
        assert_eq!(accounting.metrics(&limits)["unsnapshottedLogBytes"], 0);
        assert_eq!(
            accounting.reason(&limits, Instant::now() + Duration::from_secs(600)),
            None,
            "idle snapshots must not rebuild indefinitely"
        );
    }

    #[test]
    fn age_and_byte_triggers_obey_cost_cooldown_and_can_be_disabled() {
        let now = Instant::now();
        let accounting = Accounting::new(200, Some(2), Some(1));
        let mut limits = Limits {
            snapshot_after_bytes: 100,
            snapshot_max_age: Duration::from_secs(1),
            snapshot_duty_percent: 20,
            ..Limits::default()
        };
        {
            let mut state = accounting.0.lock().unwrap();
            state.completed = now;
            state.last_finished = now;
            state.duration = Duration::from_secs(2);
        }
        assert_eq!(cooldown(Duration::from_secs(2), 20), Duration::from_secs(8));
        assert_eq!(
            accounting.reason(&limits, now + Duration::from_secs(7)),
            None,
            "even overdue soft thresholds defer to duty protection"
        );
        assert_eq!(
            accounting.reason(&limits, now + Duration::from_secs(8)),
            Some("applied_bytes")
        );
        limits.snapshot_after_bytes = 0;
        assert_eq!(
            accounting.reason(&limits, now + Duration::from_secs(8)),
            Some("age")
        );
        limits.snapshot_max_age = Duration::ZERO;
        assert_eq!(
            accounting.reason(&limits, now + Duration::from_secs(800)),
            None
        );
        limits.snapshot_after_bytes = 100;
        limits.snapshot_duty_percent = 100;
        assert_eq!(accounting.reason(&limits, now), Some("applied_bytes"));
    }

    #[test]
    fn blocking_work_owns_activity_after_async_waiter_cancellation() {
        let accounting = Accounting::new(100, Some(1), None);
        let caller = accounting.begin();
        let worker = caller.clone();
        drop(caller);
        assert_eq!(accounting.metrics(&Limits::default())["activeBuilds"], 1);
        drop(worker);
        assert_eq!(accounting.metrics(&Limits::default())["activeBuilds"], 0);
        assert_eq!(
            accounting.metrics(&Limits::default())["completedBuildAttempts"],
            1
        );
    }

    #[tokio::test]
    async fn soft_scheduler_builds_a_real_raft_snapshot_below_log_threshold() {
        let directory = tempfile::tempdir().unwrap();
        let consensus = super::super::Consensus::open(
            1,
            "127.0.0.1:17101".into(),
            directory.path().into(),
            "snapshot-test".into(),
        )
        .await
        .unwrap();
        let limits = Arc::new(Limits {
            snapshot_after_bytes: 1,
            snapshot_max_age: Duration::ZERO,
            snapshot_check_interval: Duration::from_millis(10),
            snapshot_duty_percent: 100,
            ..Limits::default()
        });
        let _scheduler = spawn(
            consensus.raft.clone(),
            consensus.store.snapshot_accounting(),
            limits,
        );
        consensus
            .initialize(std::collections::BTreeMap::from([(
                1,
                "127.0.0.1:17101".into(),
            )]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while consensus.metrics().snapshot.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("small committed history should snapshot without reaching the log-count threshold");
        assert!(
            consensus.metrics().snapshot.unwrap().index < Limits::default().snapshot_after_logs
        );
        consensus.shutdown().await.unwrap();
    }
}
