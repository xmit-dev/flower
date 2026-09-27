//! The twin of `sdk/capacity.test.ts`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_worker::{
    Adaptive, Concurrency, HealthLimits, Limiter, LimitChange, Load, idle_health, process_health,
};
use parking_lot::Mutex;

fn change(limit: u64, reason: &str) -> Option<LimitChange> {
    Some(LimitChange {
        limit,
        reason: reason.into(),
    })
}

fn adaptive(min: Option<u64>, initial: Option<u64>, max: Option<u64>) -> Concurrency {
    Concurrency::Adaptive(Adaptive { min, max, initial })
}

#[test]
fn a_limit_doubles_while_work_waits_then_grows_by_a_sixteenth_after_a_cut_and_shrinks_when_behind() {
    let now = Arc::new(Mutex::new(0.0_f64));
    let load = Arc::new(Mutex::new(Load::new(0.1, "event loop 9% busy")));
    let health = {
        let load = load.clone();
        move || load.lock().clone()
    };
    let clock = {
        let now = now.clone();
        move || *now.lock()
    };
    let advance = |ms: f64| *now.lock() += ms;
    let mut limiter = Limiter::with_now(adaptive(Some(2), Some(4), Some(40)), Arc::new(health), clock).unwrap();
    assert_eq!(limiter.adjust(), None, "no demand, no growth");
    limiter.want();
    assert_eq!(limiter.adjust(), change(8, "more work is waiting"));
    for expected in [16, 32, 40, 40] {
        limiter.want();
        limiter.adjust();
        assert_eq!(limiter.limit(), expected);
    }
    *load.lock() = Load::new(1.08, "event loop 97% busy");
    assert_eq!(limiter.adjust(), change(30, "event loop 97% busy"));
    assert_eq!(limiter.adjust(), None, "one cut per second");
    advance(1_000.0);
    limiter.adjust();
    assert_eq!(limiter.limit(), 22);
    *load.lock() = Load::new(0.1, "event loop 9% busy");
    advance(1_000.0);
    limiter.want();
    limiter.adjust();
    assert_eq!(limiter.limit(), 23, "right after a cut the limit grows by a sixteenth, at least one");
    advance(5_000.0);
    limiter.want();
    limiter.adjust();
    assert_eq!(limiter.limit(), 40, "then doubles again, up to max");
    limiter.throttle(0, "RATE_LIMITED");
    assert_eq!(limiter.limit(), 20);
    advance(29_000.0);
    limiter.want();
    limiter.adjust();
    assert_eq!(limiter.limit(), 21, "under a provider's limit it settles for longer");
    advance(1_000.0);
    limiter.want();
    limiter.adjust();
    assert_eq!(limiter.limit(), 40);
    let clock = {
        let now = now.clone();
        move || *now.lock()
    };
    let mut busy = Limiter::with_now(
        adaptive(None, Some(32), Some(64)),
        Arc::new(|| Load::new(0.7, "event loop 63% busy")),
        clock,
    )
    .unwrap();
    busy.want();
    assert_eq!(
        busy.adjust(),
        change(34, "more work is waiting"),
        "a process past half its load grows by a sixteenth"
    );
}

#[test]
fn a_provider_pushing_back_halves_the_limit_and_pauses_claims_even_a_fixed_ones() {
    let now = Arc::new(Mutex::new(0.0_f64));
    let clock = || {
        let now = now.clone();
        move || *now.lock()
    };
    let mut limiter = Limiter::with_now(adaptive(Some(1), Some(32), Some(64)), idle_health(), clock()).unwrap();
    assert_eq!(limiter.throttle(5_000, "RATE_LIMITED"), change(16, "RATE_LIMITED"));
    assert_eq!(limiter.pause(), 5_000);
    assert_eq!(limiter.throttle(1_000, "RATE_LIMITED"), None, "a burst of refusals cuts once");
    limiter.want();
    assert_eq!(limiter.adjust(), None, "no growth while paused");
    *now.lock() += 5_000.0;
    assert_eq!(limiter.pause(), 0);
    let mut fixed = Limiter::with_now(Concurrency::Fixed(8), Arc::new(|| Load::new(3.0, "heap 99% full")), clock()).unwrap();
    assert_eq!(fixed.throttle(2_000, "OVERLOADED"), None);
    assert_eq!(fixed.pause(), 2_000);
    fixed.want();
    assert_eq!(fixed.adjust(), None);
    assert_eq!(fixed.limit(), 8);
}

#[test]
fn bounds_default_to_1_to_16_starting_at_min_and_must_be_whole_and_ordered() {
    let limiter = Limiter::new(adaptive(None, None, None), idle_health()).unwrap();
    assert_eq!([limiter.min, limiter.limit(), limiter.max], [1, 1, 16]);
    assert_eq!(Limiter::new(adaptive(Some(32), None, None), idle_health()).unwrap().max, 32);
    // `{ max: 1.5 }` cannot be written: the bounds are whole numbers by type.
    let too_big = 1_u64 << 53; // Number.MAX_SAFE_INTEGER + 1
    for bad in [
        Concurrency::Fixed(0),
        adaptive(Some(0), None, None),
        adaptive(Some(4), None, Some(2)),
        adaptive(None, Some(20), None),
        adaptive(None, None, Some(too_big)),
    ] {
        let error = Limiter::new(bad, idle_health()).err();
        assert_eq!(
            error.as_deref(),
            Some("concurrency needs whole numbers with 1 <= min <= initial <= max"),
            "{bad:?}"
        );
    }
}

fn matches_percent(reason: &str, prefix: &str, suffix: &str) -> bool {
    reason
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_suffix(suffix))
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn the_processes_health_reads_its_runtime_and_memory_since_the_last_read_and_names_what_loads_it_most() {
    tokio::time::sleep(Duration::from_millis(1)).await;
    let health = process_health(HealthLimits::default()).unwrap();
    // Spin on a runtime worker thread, as the TS spins its event loop.
    tokio::spawn(async {
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(30) {}
    })
    .await
    .unwrap();
    // A worker thread hands in its busy time when it next parks, not as each task ends.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let busy = flower_worker::Health::load(&health);
    assert!(busy.load > 0.5, "a spinning runtime reads as busy: {busy:?}");
    assert!(matches_percent(&busy.reason, "event loop ", "% busy"), "{busy:?}");
    let full = process_health(HealthLimits {
        busy: 1e9,
        memory: 1e-9,
    })
    .unwrap();
    let reason = flower_worker::Health::load(&full).reason;
    assert!(matches_percent(&reason, "memory ", "% used"), "{reason}");
    assert_eq!(
        process_health(HealthLimits {
            busy: 0.0,
            memory: 0.85
        })
        .err()
        .as_deref(),
        Some("Health limits must be positive")
    );
}
