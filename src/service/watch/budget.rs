//! One budget of time for the watch refreshes writes start, shared by every
//! hub on this server.
//!
//! A hub paces its own write-woken refreshes to FLOWER_WATCH_DUTY_PERCENT of
//! the time, which bounds what one hub costs while what it read keeps
//! changing, but not what many cost together: N such hubs took N times the
//! duty. Each write-woken refresh also takes a turn of this budget,
//! FLOWER_WATCH_BUDGET_PERCENT of one core, so that together they take no more
//! however many hubs are hot. It counts the CPU time a refresh takes (its
//! evaluation on its worker thread, then its diff and encoding), so that a
//! busy host, which stretches refreshes without their costing more, does not
//! hold them back further; where threads have no CPU clock, the time they
//! take.
//!
//! The budget is shared as a processor would share its time between the hubs
//! that use it (generalized processor sharing): a virtual clock advances at
//! the budget's rate divided by the hubs in service, a turn puts its hub in
//! service until that clock has advanced by what its refresh took, and the
//! hub's next turn waits until then. N hubs refreshing for writes all the time
//! thus get an equal share each: one whose refresh takes T refreshes about
//! every N × T × 100 / percent, or T × 100 / duty if that is longer, so a
//! costly hub refreshes proportionally less often and a cheap one never waits
//! for a costly one. A hub waits only for its own last refresh, however many
//! others wait. While the hubs in service together use less than the budget,
//! nothing waits beyond its own duty; once none is in service, what they used
//! no longer counts.
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{sync::Notify, time::Instant};

pub(crate) struct Budget {
    /// Share of one core, in percent; 0 means no budget.
    percent: u32,
    ledger: Mutex<Ledger>,
    /// Raised when a hub leaves service sooner than it reserved.
    changed: Notify,
}

/// A hub's place in the budget: the virtual time it may refresh again at.
pub(super) struct Share {
    id: u64,
    finish: Mutex<Duration>,
}

impl Default for Share {
    fn default() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            finish: Mutex::new(Duration::ZERO),
        }
    }
}

impl Share {
    fn finish(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.finish.lock().expect("watch share mutex")
    }
}

/// The budget's state. Its methods take the time and the budget's percent,
/// so that tests can run it on a clock of their own.
struct Ledger {
    /// The virtual time at `at`. It advances at percent / 100 divided by the
    /// hubs in service, and stays put while none is.
    virtual_time: Duration,
    at: Instant,
    /// Hubs in service, by the virtual time each leaves service at, then id.
    serving: BTreeSet<(Duration, u64)>,
    turns: u64,
    spent: Duration,
    waited: Duration,
}

/// `duration` × `numerator` / `denominator`, saturating.
fn scale(duration: Duration, numerator: u64, denominator: u64) -> Duration {
    let nanos =
        duration.as_nanos().saturating_mul(u128::from(numerator)) / u128::from(denominator.max(1));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn later(at: Instant, by: Duration) -> Instant {
    at.checked_add(by).unwrap_or(at)
}

impl Ledger {
    fn new(now: Instant) -> Self {
        Self {
            virtual_time: Duration::ZERO,
            at: now,
            serving: BTreeSet::new(),
            turns: 0,
            spent: Duration::ZERO,
            waited: Duration::ZERO,
        }
    }

    /// Advance the virtual time to `now`, taking hubs out of service as it
    /// reaches their finish tags.
    fn advance(&mut self, now: Instant, percent: u32) {
        let now = now.max(self.at);
        while let Some(&(finish, _)) = self.serving.first() {
            let served = self.serving.len() as u64;
            let reach = later(
                self.at,
                scale(
                    finish.saturating_sub(self.virtual_time),
                    100 * served,
                    percent.into(),
                ),
            );
            if reach > now {
                self.virtual_time += scale(now - self.at, percent.into(), 100 * served);
                break;
            }
            self.virtual_time = finish;
            self.at = reach;
            self.serving.pop_first();
        }
        self.at = now;
    }

    /// When the virtual time reaches `target` if no hub enters service
    /// meanwhile; more hubs in service can only make it later.
    fn reaches(&self, target: Duration, percent: u32) -> Instant {
        let (mut virtual_time, mut at) = (self.virtual_time, self.at);
        let mut served = self.serving.len() as u64;
        for &(finish, _) in &self.serving {
            let upto = finish.min(target);
            at = later(
                at,
                scale(
                    upto.saturating_sub(virtual_time),
                    100 * served,
                    percent.into(),
                ),
            );
            virtual_time = upto;
            if finish >= target {
                break;
            }
            served -= 1;
        }
        at
    }

    /// Put the hub whose finish tag is `finish` in service for `cost` if it
    /// is out of service, returning the virtual time it started at, or else
    /// when to look again.
    fn take(
        &mut self,
        (id, finish): (u64, &mut Duration),
        cost: Duration,
        now: Instant,
        percent: u32,
    ) -> Result<Duration, Instant> {
        self.advance(now, percent);
        if *finish > self.virtual_time {
            return Err(self.reaches(*finish, percent));
        }
        let start = self.virtual_time;
        self.serve((id, finish), start.saturating_add(cost));
        self.turns += 1;
        Ok(start)
    }

    /// Move the hub's finish tag to `to`, keeping it in service until then.
    fn serve(&mut self, (id, finish): (u64, &mut Duration), to: Duration) {
        self.serving.remove(&(*finish, id));
        *finish = to;
        if to > self.virtual_time {
            self.serving.insert((to, id));
        }
    }

    /// Replace what a turn started at `start` reserved by what it spent:
    /// true if its hub leaves service sooner.
    fn settle(
        &mut self,
        (id, finish): (u64, &mut Duration),
        start: Duration,
        spent: Duration,
        now: Instant,
        percent: u32,
    ) -> bool {
        self.advance(now, percent);
        self.spent += spent;
        let to = start.saturating_add(spent);
        let sooner = to < *finish;
        self.serve((id, finish), to);
        sooner
    }

    /// Charge the hub for a refresh that took no turn.
    fn charge(
        &mut self,
        (id, finish): (u64, &mut Duration),
        cost: Duration,
        now: Instant,
        percent: u32,
    ) {
        self.advance(now, percent);
        self.spent += cost;
        let to = (*finish).max(self.virtual_time).saturating_add(cost);
        self.serve((id, finish), to);
    }
}

impl Budget {
    pub(super) fn new(percent: usize) -> Arc<Self> {
        Arc::new(Self {
            percent: u32::try_from(percent).unwrap_or(u32::MAX),
            ledger: Mutex::new(Ledger::new(Instant::now())),
            changed: Notify::new(),
        })
    }

    /// The budget every watch on this server shares.
    pub(super) fn server() -> Arc<Self> {
        static SERVER: OnceLock<Arc<Budget>> = OnceLock::new();
        SERVER
            .get_or_init(|| {
                Self::new(
                    crate::service::tuning::settings()
                        .expect("validated watch budget")
                        .watch_budget_percent,
                )
            })
            .clone()
    }

    fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().expect("watch budget mutex")
    }

    /// Wait until the hub holding `share` is out of service, and put it in
    /// service for `cost`, what its refresh is expected to take.
    pub(super) async fn turn<'a>(&'a self, share: &'a Share, cost: Duration) -> Turn<'a> {
        let mut turn = Turn {
            budget: self,
            share,
            start: None,
            spent: Duration::ZERO,
        };
        if self.percent == 0 {
            return turn;
        }
        let arrived = Instant::now();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let taken = {
                let mut ledger = self.ledger();
                let now = Instant::now();
                let taken = ledger.take((share.id, &mut *share.finish()), cost, now, self.percent);
                if taken.is_ok() {
                    ledger.waited += now.saturating_duration_since(arrived);
                }
                taken
            };
            match taken {
                Ok(start) => {
                    turn.start = Some(start);
                    return turn;
                }
                Err(at) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(at) => {}
                        _ = &mut notified => {}
                    }
                }
            }
        }
    }

    /// Charge the hub holding `share` for a write-woken refresh that took
    /// no turn of its own.
    pub(super) fn charge(&self, share: &Share, cost: Duration) {
        if self.percent == 0 || cost.is_zero() {
            return;
        }
        self.ledger().charge(
            (share.id, &mut *share.finish()),
            cost,
            Instant::now(),
            self.percent,
        );
    }

    /// Its setting, the hubs in service, and the turns granted, refresh time
    /// charged and turns' waits since the server started.
    pub(crate) fn metrics(&self) -> Value {
        let mut ledger = self.ledger();
        ledger.advance(Instant::now(), self.percent.max(1));
        json!({
            "budgetPercent": self.percent,
            "serving": ledger.serving.len(),
            "turns": ledger.turns,
            "spentMs": ledger.spent.as_millis() as u64,
            "waitedMs": ledger.waited.as_millis() as u64,
        })
    }
}

/// A turn of the budget. When dropped, it settles for what `spend` recorded.
pub(super) struct Turn<'a> {
    budget: &'a Budget,
    share: &'a Share,
    /// The virtual time it started at; None without a budget.
    start: Option<Duration>,
    spent: Duration,
}

impl Turn<'_> {
    /// Record what a refresh under this turn took.
    pub fn spend(&mut self, cost: Duration) {
        self.spent = self.spent.saturating_add(cost);
    }
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        let Some(start) = self.start else { return };
        let sooner = self.budget.ledger().settle(
            (self.share.id, &mut *self.share.finish()),
            start,
            self.spent,
            Instant::now(),
            self.budget.percent,
        );
        if sooner {
            self.budget.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    /// A hub of the simulation, woken by writes all the time until `until`.
    struct Hot {
        cost: Duration,
        until: Duration,
        id: u64,
        finish: Duration,
        /// When its own duty lets it refresh again.
        next: Instant,
        running: Option<Instant>,
        refreshes: u64,
        spent: Duration,
        /// Refresh time in the second half of the run.
        late: Duration,
        /// The longest time between two refreshes, after the first ten
        /// seconds.
        period: Duration,
        last: Option<Instant>,
    }

    /// Run hubs through a budget of `percent` for `seconds`, each also paced
    /// by its own `duty`, on a clock of the simulation's own in 1 ms steps.
    fn simulate(percent: u32, duty: u32, hubs: &[(u64, u64)], seconds: u64) -> Vec<Hot> {
        let start = Instant::now();
        let mut ledger = Ledger::new(start);
        let mut hubs: Vec<Hot> = hubs
            .iter()
            .enumerate()
            .map(|(id, (cost, until))| Hot {
                cost: ms(*cost),
                until: Duration::from_secs(*until),
                id: id as u64,
                finish: Duration::ZERO,
                next: start,
                running: None,
                refreshes: 0,
                spent: Duration::ZERO,
                late: Duration::ZERO,
                period: Duration::ZERO,
                last: None,
            })
            .collect();
        let end = Duration::from_secs(seconds);
        let mut elapsed = Duration::ZERO;
        while elapsed < end {
            let now = start + elapsed;
            for hub in &mut hubs {
                if let Some(started) = hub.running
                    && now >= started + hub.cost
                {
                    hub.running = None;
                    // Its refresh took what it expected: settling changes
                    // nothing, and its duty paces it.
                    let turn = hub.finish - hub.cost;
                    ledger.settle((hub.id, &mut hub.finish), turn, hub.cost, now, percent);
                    hub.next = started + hub.cost * 100 / duty;
                }
                if elapsed >= hub.until || hub.running.is_some() || now < hub.next {
                    continue;
                }
                if ledger
                    .take((hub.id, &mut hub.finish), hub.cost, now, percent)
                    .is_ok()
                {
                    if let Some(last) = hub.last
                        && elapsed >= Duration::from_secs(10)
                    {
                        hub.period = hub.period.max(now - last);
                    }
                    hub.last = Some(now);
                    hub.running = Some(now);
                    hub.refreshes += 1;
                    hub.spent += hub.cost;
                    if elapsed >= end / 2 {
                        hub.late += hub.cost;
                    }
                }
            }
            elapsed += ms(1);
        }
        hubs
    }

    fn core(spent: Duration, seconds: u64) -> f64 {
        spent.as_secs_f64() / seconds as f64
    }

    #[test]
    fn hot_hubs_share_the_budget_equally_by_time_and_stay_within_it() {
        // Three session lists of 60 ms and three inboxes of 90 ms, as on the
        // live Ultimator Flower, at 30% of a core: their duty (10% each)
        // alone would let them take 60%.
        let hubs = simulate(
            30,
            10,
            &[
                (60, 600),
                (60, 600),
                (60, 600),
                (90, 600),
                (90, 600),
                (90, 600),
            ],
            600,
        );
        let total: Duration = hubs.iter().map(|hub| hub.spent).sum();
        let total = core(total, 600);
        assert!((0.29..=0.31).contains(&total), "took {total} of a core");
        for hub in &hubs {
            let used = core(hub.spent, 600);
            assert!((0.048..=0.052).contains(&used), "a hub took {used}");
            // Each refreshes every N × T × 100 / percent: 1.2 s and 1.8 s.
            let period = hub.cost * 6 * 100 / 30;
            assert!(
                hub.period <= period + ms(5),
                "{:?} > {period:?}",
                hub.period
            );
        }
        // A costly hub refreshes proportionally less often.
        let ratio = hubs[0].refreshes as f64 / hubs[3].refreshes as f64;
        assert!(
            (1.45..=1.55).contains(&ratio),
            "60 ms vs 90 ms refreshes {ratio}"
        );
    }

    #[test]
    fn a_cheap_hub_never_waits_for_costly_ones() {
        // One 5 ms hub beside four of 400 ms at 20%: it keeps an equal share
        // of time, refreshing every 125 ms, 80 times as often as they do.
        let hubs = simulate(
            20,
            10,
            &[(5, 600), (400, 600), (400, 600), (400, 600), (400, 600)],
            600,
        );
        let cheap = &hubs[0];
        let used = core(cheap.spent, 600);
        assert!((0.038..=0.042).contains(&used), "the cheap hub took {used}");
        assert!(cheap.period <= ms(125 + 5), "{:?}", cheap.period);
        for hub in &hubs[1..] {
            assert!(cheap.refreshes > 75 * hub.refreshes);
            // They refresh too, every N × T × 100 / percent.
            assert!(hub.period <= ms(10_000 + 5), "{:?}", hub.period);
        }
    }

    #[test]
    fn hubs_below_the_budget_keep_their_own_duty_and_quiet_ones_leave_it_to_the_rest() {
        // Two 10 ms hubs at 10% duty take 20% of a core: a budget of 50%
        // never makes them wait.
        for hub in simulate(50, 10, &[(10, 60), (10, 60)], 60) {
            let used = core(hub.spent, 60);
            assert!((0.098..=0.102).contains(&used), "a hub took {used}");
            assert!(hub.period <= ms(100 + 5), "{:?}", hub.period);
        }
        // Four at 20% get 5% each; once two go quiet, the other two get 10%
        // each, their duty.
        let hubs = simulate(20, 10, &[(10, 120), (10, 120), (10, 60), (10, 60)], 120);
        for hub in &hubs[..2] {
            let early = core(hub.spent - hub.late, 60);
            let late = core(hub.late, 60);
            assert!(
                (0.048..=0.052).contains(&early),
                "took {early} while four were hot"
            );
            assert!((0.098..=0.102).contains(&late), "took {late} once two were");
        }
    }

    #[test]
    fn a_hub_waits_only_for_what_its_own_last_refresh_took() {
        let start = Instant::now();
        let mut ledger = Ledger::new(start);
        let (mut costly, mut cheap) = (Duration::ZERO, Duration::ZERO);
        // A 500 ms refresh at 50% keeps its hub in service for 1 s alone.
        let begun = ledger.take((0, &mut costly), ms(500), start, 50).unwrap();
        assert_eq!(
            ledger.take((0, &mut costly), ms(500), start, 50),
            Err(start + ms(1_000))
        );
        // Another hub goes at once, and both share the budget: the costly
        // one's wait grows, the cheap one waits for its own 10 ms (40 ms at
        // 25% each).
        let at = start + ms(100);
        let cheap_begun = ledger.take((1, &mut cheap), ms(10), at, 50).unwrap();
        assert_eq!(
            ledger.take((1, &mut cheap), ms(10), at, 50),
            Err(at + ms(40))
        );
        assert_eq!(
            ledger.take((0, &mut costly), ms(500), at, 50),
            Err(at + ms(40) + ms(900) - ms(20))
        );
        // A refresh that took less than reserved leaves service sooner.
        assert!(ledger.settle((1, &mut cheap), cheap_begun, ms(1), at, 50));
        assert_eq!(
            ledger
                .take((1, &mut cheap), ms(10), at + ms(4), 50)
                .map(|_| ()),
            Ok(())
        );
        // Once no hub is in service, what they used no longer counts.
        let idle = start + ms(5_000);
        ledger.settle((0, &mut costly), begun, ms(500), idle, 50);
        assert!(ledger.take((0, &mut costly), ms(500), idle, 50).is_ok());
        assert_eq!(ledger.serving.len(), 1);
        // A charge without a turn keeps the hub in service for it.
        let mut rider = Duration::ZERO;
        ledger.charge((2, &mut rider), ms(100), idle, 50);
        assert!(ledger.take((2, &mut rider), ms(1), idle, 50).is_err());
    }

    #[tokio::test]
    async fn turns_are_immediate_out_of_service_and_without_a_budget() {
        let share = Share::default();
        for percent in [0, 50] {
            let begun = std::time::Instant::now();
            drop(Budget::new(percent).turn(&share, ms(1)).await);
            assert!(begun.elapsed() < ms(500));
        }
        // Without a budget, costly refreshes charge nothing.
        let off = Budget::new(0);
        let mut turn = off.turn(&share, ms(10_000)).await;
        turn.spend(ms(10_000));
        drop(turn);
        off.charge(&share, ms(10_000));
        tokio::time::timeout(ms(500), off.turn(&share, ms(1)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_turn_waits_for_what_its_hub_spent_and_settling_sooner_wakes_it() {
        let budget = Budget::new(100);
        let (costly, other) = (Share::default(), Share::default());
        let quick = Arc::new(Share::default());
        let mut turn = budget.turn(&costly, ms(0)).await;
        turn.spend(ms(300));
        drop(turn);
        // 300 ms at 100% of a core: its next turn waits for it.
        let begun = std::time::Instant::now();
        let next = budget.turn(&costly, ms(0));
        tokio::pin!(next);
        assert!(tokio::time::timeout(ms(100), next.as_mut()).await.is_err());
        // Another hub does not.
        tokio::time::timeout(ms(100), budget.turn(&other, ms(0)))
            .await
            .expect("another hub goes at once");
        drop(next.await);
        let waited = begun.elapsed();
        assert!(waited >= ms(250), "went after {waited:?}");
        assert!(waited < ms(5_000), "went after {waited:?}");
        // A turn that reserved 10 s and spent nothing wakes whoever waits
        // for its hub.
        let turn = budget.turn(&quick, ms(10_000)).await;
        let waiter = tokio::spawn({
            let budget = budget.clone();
            let quick = quick.clone();
            async move {
                drop(budget.turn(&quick, ms(0)).await);
                std::time::Instant::now()
            }
        });
        tokio::time::sleep(ms(50)).await;
        let settled = std::time::Instant::now();
        drop(turn);
        let went = waiter.await.unwrap();
        assert!(went - settled < ms(1_000), "waited {:?}", went - settled);
        let metrics = budget.metrics();
        assert_eq!(metrics["budgetPercent"], 100);
        assert!(metrics["spentMs"].as_u64().unwrap() >= 300);
        assert!(metrics["turns"].as_u64().unwrap() >= 4);
    }
}
