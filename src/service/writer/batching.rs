//! Work-conserving batch sizing. Timing is a soft boundary between methods;
//! evaluation and encoded-byte budgets continue to bound individual methods.
use super::super::tuning::{BatchMode, Settings};
use std::time::Duration;

pub(super) struct Controller {
    mode: BatchMode,
    cap: Option<usize>,
    queue_capacity: usize,
    maximum: Duration,
    preparation_seconds: Option<f64>,
    preparation_calls: Option<f64>,
    commit_seconds: Option<f64>,
    pub(super) speculation: super::speculation::Policy,
}

#[derive(Clone, Copy)]
pub(super) struct Decision {
    pub mode: &'static str,
    pub reason: &'static str,
    pub count: usize,
    // Hard ceiling on one group's requests, including a successor that
    // continues past the adaptive targets while its predecessor commits.
    pub limit: usize,
    pub budget: Duration,
    pub queued: usize,
    pub lag: Lag,
}

impl Decision {
    /// Adaptive count and time targets bound work that could delay a group's
    /// submission. A pipelined successor cannot be submitted before its
    /// predecessor completes, so until then it keeps preparing arrivals, up
    /// to `limit`. Fixed mode keeps its exact thresholds for comparisons.
    pub fn work_conserving(&self) -> bool {
        self.mode == "adaptive"
    }
}

/// Log progress is a backpressure signal, not a read-consistency proof. Remote
/// matched positions describe replication, not remote state-machine application.
#[derive(Clone, Copy, Default)]
pub(super) struct Lag {
    pub local_unapplied: u64,
    pub quorum_unmatched: u64,
}

impl Lag {
    pub fn from_metrics(metrics: &openraft::RaftMetrics<u64, openraft::BasicNode>) -> Self {
        let end = metrics
            .last_log_index
            .map_or(0, |index| index.saturating_add(1));
        let applied = metrics
            .last_applied
            .map_or(0, |log| log.index.saturating_add(1));
        let quorum = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .iter()
            .filter(|members| !members.is_empty())
            .map(|members| {
                let mut matched: Vec<_> = members
                    .iter()
                    .map(|id| {
                        if *id == metrics.id {
                            end
                        } else {
                            metrics
                                .replication
                                .as_ref()
                                .and_then(|peers| peers.get(id))
                                .and_then(|log| *log)
                                .map_or(0, |log| log.index.saturating_add(1))
                        }
                    })
                    .collect();
                matched.sort_unstable_by(|a, b| b.cmp(a));
                matched[members.len() / 2]
            })
            .min()
            .unwrap_or(end);
        Self {
            local_unapplied: end.saturating_sub(applied),
            quorum_unmatched: end.saturating_sub(quorum),
        }
    }

    pub fn allows_successor(self) -> bool {
        // One outstanding group is the normal overlap. More work already in
        // Raft/application (including other logical writers) gets a chance to
        // drain before this writer allocates another speculative state overlay.
        self.local_unapplied <= 1 && self.quorum_unmatched <= 1
    }
}

impl Controller {
    pub fn new(settings: &Settings) -> Self {
        Self {
            mode: settings.batch_mode,
            cap: settings.batch_size,
            queue_capacity: settings.queue_capacity,
            maximum: settings.writer_batch_time,
            preparation_seconds: None,
            preparation_calls: None,
            commit_seconds: None,
            speculation: super::speculation::Policy::new(
                settings.writer_preparation_workers,
                settings.queue_capacity,
            ),
        }
    }

    #[cfg(test)]
    pub fn fixed_for_test(&mut self, size: usize) {
        self.mode = BatchMode::Fixed;
        self.cap = Some(size);
    }

    #[cfg(test)]
    pub fn adaptive_for_test(&mut self, capacity: usize, maximum: Duration) {
        self.mode = BatchMode::Adaptive;
        self.cap = None;
        self.queue_capacity = capacity;
        self.maximum = maximum;
    }

    pub fn adaptive(&self) -> bool {
        self.mode == BatchMode::Adaptive
    }

    /// Near the maintenance boundary, a successor whose entire preparation
    /// window is shorter than one learned durability round adds another small
    /// durable batch before yielding. Commit the already prepared group and
    /// start the next full window after maintenance instead.
    pub fn drain_before_deadline(&self, remaining: Duration) -> bool {
        self.adaptive()
            && self
                .commit_seconds
                .is_some_and(|commit| remaining <= Duration::from_secs_f64(commit))
    }

    pub fn decide(&self, queued: usize) -> Decision {
        if !self.adaptive() {
            let count = self.cap.expect("fixed mode resolves a batch size");
            return Decision {
                mode: "fixed",
                reason: "fixed_count",
                count,
                limit: count,
                budget: self.maximum,
                queued,
                lag: Lag::default(),
            };
        }
        let (budget, count, reason) = match (self.preparation_per_call(), self.commit_seconds) {
            (Some(cost), Some(commit)) => {
                // Cover the previous durability wait, then use half the queued
                // work to drain backlog without jumping straight to the ceiling.
                let seconds = (commit + cost * queued as f64 / 2.0)
                    .max(cost)
                    .min(self.maximum.as_secs_f64());
                let budget = Duration::from_secs_f64(seconds);
                let count = (seconds / cost).ceil() as usize;
                (
                    budget,
                    count.max(1),
                    if queued > 1 {
                        "queue_pressure"
                    } else {
                        "commit_overlap"
                    },
                )
            }
            _ => (self.maximum, self.queue_capacity, "learning"),
        };
        Decision {
            mode: "adaptive",
            reason,
            count: self.bounded_count(count),
            limit: self.bounded_count(usize::MAX),
            budget,
            queued,
            lag: Lag::default(),
        }
    }

    fn bounded_count(&self, count: usize) -> usize {
        count
            .min(self.queue_capacity)
            .min(self.cap.unwrap_or(usize::MAX))
            .max(1)
    }

    pub fn decide_aged(&self, queued: usize, oldest: Duration, maintenance: Duration) -> Decision {
        let mut decision = self.decide(queued);
        if !self.adaptive() {
            return decision;
        }
        // Age is a soft latency preference, not a response deadline. Once even
        // a durability round-trip cannot meet it, aim to drain the queued work
        // rather than only half of it. Preserve useful commit overlap at low
        // load; the preparation ceiling and maintenance boundary still bound
        // recovery, along with the queue and explicit batch-count limits.
        let remaining_age = self.maximum.saturating_sub(oldest);
        let commit = self.commit_seconds.map(Duration::from_secs_f64);
        let recover = commit.is_none_or(|commit| remaining_age <= commit);
        let latency = if recover {
            if let Some(cost) = self.preparation_per_call() {
                let seconds = (cost * queued as f64)
                    .max(decision.budget.as_secs_f64())
                    .min(self.maximum.as_secs_f64());
                decision.budget = Duration::from_secs_f64(seconds);
                decision.count = self.bounded_count((seconds / cost).ceil() as usize);
            }
            decision.reason = "backlog_recovery";
            decision.budget
        } else {
            remaining_age
        };
        let remaining = latency.min(maintenance);
        if remaining < decision.budget {
            decision.budget = remaining;
            decision.reason = if maintenance <= latency {
                "maintenance_deadline"
            } else {
                "oldest_request_age"
            };
            if let Some(cost) = self.preparation_per_call() {
                decision.count = decision
                    .count
                    .min((remaining.as_secs_f64() / cost).ceil() as usize)
                    .max(1);
            }
        }
        decision
    }

    fn preparation_per_call(&self) -> Option<f64> {
        self.preparation_seconds
            .zip(self.preparation_calls)
            .map(|(seconds, calls)| seconds / calls)
    }

    pub fn observe(&mut self, calls: usize, prepare: Duration, commit: Duration, committed: bool) {
        if !committed || calls == 0 || prepare.is_zero() || commit.is_zero() {
            return;
        }
        fn smooth(previous: Option<f64>, sample: f64) -> Option<f64> {
            Some(previous.map_or(sample, |previous| previous * 0.75 + sample * 0.25))
        }
        // Weight work by the requests it represents. A one-request successor
        // delayed by admission/scheduling must not teach the controller that
        // every method costs that whole delay, shrinking the next large batch
        // into many expensive durability rounds. Equal weighting of each
        // group's average amplifies precisely that small-batch feedback loop.
        self.preparation_seconds = smooth(self.preparation_seconds, prepare.as_secs_f64());
        self.preparation_calls = smooth(self.preparation_calls, calls as f64);
        self.commit_seconds = smooth(self.commit_seconds, commit.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_feedback_uses_every_voting_majority_and_ignores_a_lone_slow_replica() {
        use openraft::{
            BasicNode, CommittedLeaderId, LogId, Membership, RaftMetrics, StoredMembership,
        };
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::Arc;
        let log = |index| Some(LogId::new(CommittedLeaderId::new(1, 1), index));
        let mut metrics = RaftMetrics::<u64, BasicNode>::new_initial(1);
        metrics.last_log_index = Some(10);
        metrics.last_applied = log(9);
        metrics.membership_config = Arc::new(StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1, 2, 3])],
                BTreeMap::<u64, BasicNode>::new(),
            ),
        ));
        metrics.replication = Some(BTreeMap::from([(2, log(10)), (3, log(0)), (4, None)]));
        let healthy = Lag::from_metrics(&metrics);
        assert_eq!(healthy.quorum_unmatched, 0);
        assert_eq!(healthy.local_unapplied, 1);
        assert!(healthy.allows_successor());

        // The old majority is caught up, but the new majority in a joint
        // configuration is not. A learner alone must never impose this gate.
        metrics.membership_config = Arc::new(StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 3, 4])],
                BTreeMap::<u64, BasicNode>::new(),
            ),
        ));
        let joint = Lag::from_metrics(&metrics);
        assert_eq!(joint.quorum_unmatched, 10);
        assert!(!joint.allows_successor());
        metrics.replication.as_mut().unwrap().insert(3, log(10));
        metrics.last_applied = log(7);
        let applying = Lag::from_metrics(&metrics);
        assert_eq!(applying.quorum_unmatched, 0);
        assert_eq!(applying.local_unapplied, 3);
        assert!(!applying.allows_successor());
        metrics.last_applied = log(10);
        assert!(Lag::from_metrics(&metrics).allows_successor());
    }

    fn adaptive() -> Controller {
        Controller {
            speculation: super::super::speculation::Policy::new(4, 1024),
            mode: BatchMode::Adaptive,
            cap: None,
            queue_capacity: 1024,
            maximum: Duration::from_millis(50),
            preparation_seconds: None,
            preparation_calls: None,
            commit_seconds: None,
        }
    }

    #[test]
    fn adapts_to_durability_cost_and_queue_pressure_without_a_fixed_count_wall() {
        let mut controller = adaptive();
        controller.observe(
            100,
            Duration::from_millis(10),
            Duration::from_millis(20),
            true,
        );
        let idle = controller.decide(1);
        let busy = controller.decide(1000);
        assert!(idle.count > 96);
        assert!(busy.count > idle.count);
        assert!(busy.budget <= Duration::from_millis(50));
        assert!(idle.budget < busy.budget);
        controller.observe(
            100,
            Duration::from_millis(1),
            Duration::from_millis(1),
            true,
        );
        assert!(controller.decide(1).budget < idle.budget);
    }

    #[test]
    fn a_delayed_single_request_does_not_turn_large_batches_into_tiny_flushes() {
        let mut controller = adaptive();
        controller.maximum = Duration::from_millis(200);
        controller.observe(
            200,
            Duration::from_millis(100),
            Duration::from_millis(40),
            true,
        );
        let before = controller.decide(300);
        // A queued successor can contain just one call but spend a durability
        // round waiting for admission/scheduling. Per-group average weighting
        // would infer 15 ms per call and collapse the next batch to 14 calls.
        controller.observe(
            1,
            Duration::from_millis(60),
            Duration::from_millis(40),
            true,
        );
        let after = controller.decide(300);
        assert!(after.count >= 200);
        assert!(after.count <= before.count);
        assert!(after.budget < Duration::from_millis(140));
        assert!(controller.preparation_per_call().unwrap() < 0.0007);
    }

    #[test]
    fn tiny_replay_groups_cannot_dominate_the_preparation_estimate() {
        for replay_preparation in [Duration::from_micros(1), Duration::from_millis(60)] {
            let mut controller = adaptive();
            controller.maximum = Duration::from_millis(200);
            controller.observe(
                200,
                Duration::from_millis(100),
                Duration::from_millis(40),
                true,
            );
            // Replay-only groups have no application evaluation; a scheduling
            // delay can still dominate their measured group preparation time.
            controller.observe(1, replay_preparation, Duration::from_micros(200), true);
            let cost = controller.preparation_per_call().unwrap();
            assert!((0.00049..0.0007).contains(&cost));
            assert!(controller.decide(300).count >= 190);
            assert!(controller.commit_seconds.unwrap() < 0.031);
        }
    }

    #[test]
    fn weighted_preparation_still_adapts_to_a_sustained_change_in_method_cost() {
        let mut controller = adaptive();
        controller.maximum = Duration::from_millis(200);
        controller.observe(
            200,
            Duration::from_millis(100),
            Duration::from_millis(40),
            true,
        );
        for _ in 0..24 {
            controller.observe(
                200,
                Duration::from_millis(400),
                Duration::from_millis(40),
                true,
            );
        }
        assert!(controller.preparation_per_call().unwrap() > 0.0019);
        assert!(controller.decide(300).count <= 105);
        assert_eq!(controller.decide(300).budget, controller.maximum);
    }

    #[test]
    fn closing_window_drains_only_with_a_learned_adaptive_durability_cost() {
        let mut controller = adaptive();
        assert!(!controller.drain_before_deadline(Duration::from_millis(1)));
        controller.observe(
            100,
            Duration::from_millis(10),
            Duration::from_millis(20),
            true,
        );
        assert!(controller.drain_before_deadline(Duration::from_millis(19)));
        assert!(controller.drain_before_deadline(Duration::from_millis(20)));
        assert!(!controller.drain_before_deadline(Duration::from_millis(21)));
        assert!(!controller.drain_before_deadline(Duration::from_millis(250)));
        // Failed/empty work cannot invent a long durability estimate and stop
        // useful low-load overlap; the prepared group always commits first.
        controller.observe(0, Duration::from_secs(1), Duration::from_secs(1), true);
        controller.observe(100, Duration::from_secs(1), Duration::from_secs(1), false);
        assert!(!controller.drain_before_deadline(Duration::from_millis(21)));
        controller.fixed_for_test(32);
        assert!(!controller.drain_before_deadline(Duration::ZERO));
    }

    #[test]
    fn explicit_caps_fixed_mode_and_failed_samples_do_not_change_semantics() {
        let mut controller = adaptive();
        controller.cap = Some(7);
        assert_eq!(controller.decide(1000).count, 7);
        controller.observe(100, Duration::from_secs(1), Duration::from_secs(1), false);
        assert!(controller.preparation_per_call().is_none());
        controller.mode = BatchMode::Fixed;
        let fixed = controller.decide(0);
        assert_eq!(fixed.count, 7);
        assert_eq!(fixed.budget, controller.maximum);
        assert_eq!(fixed.reason, "fixed_count");
    }
    #[test]
    fn cheap_replays_cannot_remove_the_admission_memory_bound() {
        let mut controller = adaptive();
        controller.observe(
            100_000,
            Duration::from_micros(1),
            Duration::from_secs(10),
            true,
        );
        assert_eq!(
            controller.decide(usize::MAX).count,
            controller.queue_capacity
        );
        assert!(controller.decide(usize::MAX).budget <= controller.maximum);
    }

    #[test]
    fn oldest_request_age_and_due_maintenance_shorten_the_window() {
        let mut controller = adaptive();
        controller.maximum = Duration::from_millis(200);
        controller.observe(
            100,
            Duration::from_millis(10),
            Duration::from_millis(5),
            true,
        );
        let fresh = controller.decide_aged(100, Duration::ZERO, Duration::from_secs(1));
        let aged = controller.decide_aged(100, Duration::from_millis(190), Duration::from_secs(1));
        assert_eq!(fresh.budget, Duration::from_millis(10));
        assert_eq!(aged.budget, Duration::from_millis(10));
        let overdue = controller.decide_aged(1000, Duration::from_secs(10), Duration::from_secs(1));
        assert_eq!(overdue.budget, Duration::from_millis(100));
        assert_eq!(overdue.count, 1000);
        assert_eq!(overdue.reason, "backlog_recovery");
        let due = controller.decide_aged(100, Duration::ZERO, Duration::ZERO);
        assert_eq!(due.budget, Duration::ZERO);
        assert_eq!(due.reason, "maintenance_deadline");
    }

    #[test]
    fn recovery_drains_queued_work_without_changing_healthy_sizing_or_any_ceiling() {
        let mut controller = adaptive();
        controller.adaptive_for_test(1000, Duration::from_millis(200));
        controller.observe(
            100,
            Duration::from_millis(100),
            Duration::from_millis(20),
            true,
        );
        let window = Duration::from_secs(1);
        let fresh = controller.decide_aged(100, Duration::ZERO, window);
        assert_eq!(fresh.budget, controller.decide(100).budget);
        assert_eq!(fresh.count, controller.decide(100).count);
        assert_eq!(fresh.budget, Duration::from_millis(70));
        assert_eq!(fresh.reason, "queue_pressure");

        // At the threshold, even an immediate commit cannot meet the age
        // preference. Catch up in one full queued group rather than two.
        let overdue = Duration::from_millis(180);
        let recovery = controller.decide_aged(100, overdue, window);
        assert_eq!(recovery.budget, Duration::from_millis(100));
        assert_eq!(recovery.count, 100);
        assert_eq!(recovery.reason, "backlog_recovery");
        assert_eq!(
            controller.decide_aged(1, overdue, window).budget,
            controller.decide(1).budget,
            "low-load recovery must preserve the existing overlap budget"
        );

        let saturated = controller.decide_aged(usize::MAX, overdue, window);
        assert_eq!(saturated.budget, controller.maximum);
        assert_eq!(saturated.count, 200);
        let maintenance = controller.decide_aged(100, overdue, Duration::from_millis(30));
        assert_eq!(maintenance.budget, Duration::from_millis(30));
        assert_eq!(maintenance.count, 30);
        assert_eq!(maintenance.reason, "maintenance_deadline");
        controller.queue_capacity = 80;
        assert_eq!(controller.decide_aged(100, overdue, window).count, 80);
        controller.cap = Some(7);
        assert_eq!(controller.decide_aged(100, overdue, window).count, 7);
        controller.fixed_for_test(7);
        let fixed = controller.decide_aged(100, overdue, Duration::ZERO);
        assert_eq!(fixed.budget, controller.maximum);
        assert_eq!(fixed.count, 7);
        assert_eq!(fixed.reason, "fixed_count");
    }
}
