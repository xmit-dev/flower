//! The in-memory Flower against the SDK's own queue and external-value tests: ports of the
//! worker-facing cases of `sdk/reactive-queue.test.ts` (on `examples/workers.ts`) and
//! `sdk/external.test.ts` (adapted to `docs/reactive-worker.ts`'s `digest`, whose input is
//! `{ recipe, text }`). Where the TS reads raw storage (`stored(db, id)`, `keys(db, ...)`) the port
//! reads what the methods return instead: the fake keeps no separate maintenance state.

use std::sync::Arc;
use std::time::Duration;

use flower_worker::testing::{FakeClient, FakeError, FakeFlower, canonical_json, shard_of};
use flower_worker::{ClientError, Clock, QueueClient, RetryPolicy, truthy};
use parking_lot::Mutex;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

const T0: i64 = 1_000_000;

fn workers() -> FakeFlower {
    FakeFlower::workers(Clock::tokio_at(T0))
}

fn raw(value: &Value) -> Box<RawValue> {
    RawValue::from_string(value.to_string()).unwrap()
}

/// `lease(job)`: the identity a report presents.
fn lease(job: &Value) -> Value {
    json!({ "id": job["id"], "owner": job["owner"], "token": job["token"] })
}

fn with(mut base: Value, extra: Value) -> Value {
    for (key, value) in extra.as_object().unwrap() {
        base[key] = value.clone();
    }
    base
}

fn claim(flower: &FakeFlower, owner: &str) -> Value {
    let job = flower.mutate("jobs.claim", json!({ "owner": owner, "leaseMs": 100 }));
    assert!(!job.is_null(), "{owner} found nothing to claim");
    job
}

/// `fails(action, code)`: the call fails with this failure code.
fn fails(result: Result<Value, FakeError>, code: &str) -> FakeError {
    let error = result.expect_err("expected a FlowerError");
    assert_eq!(error.failure_code(), Some(code), "{}", error.message);
    error
}

fn set_now(flower: &FakeFlower, now: i64) {
    flower.advance(now - flower.now());
}

fn ready(flower: &FakeFlower) -> bool {
    flower.query("jobs.ready", Value::Null) == json!(true)
}

fn at(job: &Value) -> i64 {
    job["expiresAt"].as_i64().unwrap()
}

async fn until(condition: impl Fn() -> bool) {
    let started = tokio::time::Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "Timed out waiting for the workers"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn ready_is(client: &FakeClient, wanted: bool) {
    let predicate = if wanted {
        truthy
    } else {
        |value: &Value| value == &json!(false)
    };
    let value = tokio::time::timeout(
        Duration::from_secs(3),
        client.wait_until("jobs.ready", raw(&Value::Null), predicate),
    )
    .await
    .expect("readiness changed in time")
    .unwrap();
    assert_eq!(value, json!(wanted));
}

// ---- sdk/reactive-queue.test.ts

#[tokio::test(start_paused = true)]
async fn readiness_follows_pending_work_and_lease_expiry_without_waiting_for_a_sweep() {
    let db = workers();
    assert!(!ready(&db));
    db.mutate(
        "jobs.enqueue",
        json!({ "id": "one", "payload": { "work": true } }),
    );
    assert!(ready(&db));

    let first = claim(&db, "worker-a");
    assert!(!ready(&db));
    set_now(&db, at(&first) - 1);
    assert!(!ready(&db));
    set_now(&db, at(&first));
    assert!(ready(&db), "expiry alone makes the job claimable");

    let second = claim(&db, "worker-b");
    assert_eq!(
        (&second["id"], &second["attempt"]),
        (&json!("one"), &json!(2))
    );
    assert!(second["token"].as_u64() > first["token"].as_u64());
    assert!(!ready(&db));
    fails(
        db.call(
            "jobs.complete",
            with(lease(&first), json!({ "result": "stale" })),
        ),
        "LEASE_LOST",
    );
    db.mutate(
        "jobs.complete",
        with(lease(&second), json!({ "result": "finished" })),
    );
    set_now(&db, at(&second));
    assert!(!ready(&db), "finished work never becomes ready");
}

#[tokio::test(start_paused = true)]
async fn readiness_wakes_for_new_work_delayed_work_reaching_its_time_and_expired_leases() {
    let db = workers();
    let client = db.client();
    ready_is(&client, false).await;
    db.mutate(
        "jobs.enqueue",
        json!({ "id": "later", "payload": null, "delayMs": 500 }),
    );
    assert_eq!(
        db.query("jobs.stats", Value::Null),
        json!({ "ready": false, "oldestReadyAt": null, "nextAvailableAt": T0 + 500, "readyCount": 0, "leasedCount": 0, "delayedCount": 1 })
    );
    db.advance(499);
    assert!(!ready(&db));
    db.advance(1);
    ready_is(&client, true).await;

    let first = claim(&db, "worker-a");
    ready_is(&client, false).await;
    assert_eq!(
        db.query("jobs.stats", Value::Null),
        json!({ "ready": false, "oldestReadyAt": null, "nextAvailableAt": at(&first), "readyCount": 0, "leasedCount": 1, "delayedCount": 0 })
    );
    db.advance(100);
    ready_is(&client, true).await;
    assert_eq!(
        db.job("later")["state"],
        "pending",
        "an expired lease reads as pending"
    );
    let second = claim(&db, "worker-b");
    ready_is(&client, false).await;
    db.mutate(
        "jobs.complete",
        with(lease(&second), json!({ "result": null })),
    );
    db.mutate("jobs.enqueue", json!({ "id": "now", "payload": null }));
    ready_is(&client, true).await;
}

#[tokio::test(start_paused = true)]
async fn workers_driven_by_readiness_claim_each_job_exactly_once_as_work_becomes_ready() {
    let db = workers();
    let stop = CancellationToken::new();
    let claimed = Arc::new(Mutex::new(Vec::<String>::new()));
    let loops: Vec<_> = (0..2)
        .map(|index| {
            let (client, stop, claimed) = (db.client(), stop.clone(), claimed.clone());
            tokio::spawn(async move {
                loop {
                    let woke = tokio::select! {
                        _ = stop.cancelled() => return,
                        woke = client.wait_until("jobs.ready", raw(&Value::Null), truthy) => woke,
                    };
                    woke.unwrap();
                    loop {
                        let args = json!({ "owner": format!("worker-{index}") });
                        let job = client
                            .mutate("jobs.claim", raw(&args), RetryPolicy::default())
                            .await
                            .unwrap();
                        if job.is_null() {
                            break;
                        }
                        claimed.lock().push(job["id"].as_str().unwrap().to_owned());
                        let report = with(lease(&job), json!({ "result": index }));
                        client
                            .mutate("jobs.complete", raw(&report), RetryPolicy::default())
                            .await
                            .unwrap();
                    }
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();
    let completed = |ids: &[&str]| ids.iter().all(|id| db.job(id)["state"] == "completed");
    for id in ["a", "b", "c"] {
        db.mutate("jobs.enqueue", json!({ "id": id, "payload": null }));
    }
    db.mutate(
        "jobs.enqueue",
        json!({ "id": "later", "payload": null, "delayMs": 1_000 }),
    );
    until(|| completed(&["a", "b", "c"])).await;
    assert_eq!(db.job("later")["state"], "pending");
    db.advance(1_000);
    until(|| completed(&["later"])).await;
    stop.cancel();
    for handle in loops {
        handle.await.unwrap();
    }
    let mut claimed = claimed.lock().clone();
    claimed.sort();
    assert_eq!(claimed, ["a", "b", "c", "later"]);
    assert!(!ready(&db));
}

#[tokio::test(start_paused = true)]
async fn a_worker_blocked_on_readiness_wakes_when_a_lease_expires_and_fences_out_the_old_owner() {
    let db = workers();
    db.mutate("jobs.enqueue", json!({ "id": "one", "payload": null }));
    let first = claim(&db, "worker-a");
    let client = db.client();
    let woke = tokio::spawn(async move {
        client
            .wait_until("jobs.ready", raw(&Value::Null), truthy)
            .await
    });
    db.advance(99);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!woke.is_finished(), "still waiting");
    db.advance(1);
    assert_eq!(woke.await.unwrap().unwrap(), json!(true));

    let second = claim(&db, "worker-b");
    assert_eq!(
        (&second["id"], &second["attempt"]),
        (&first["id"], &json!(2))
    );
    assert!(second["token"].as_u64() > first["token"].as_u64());
    assert_eq!(
        db.mutate("jobs.renew", json!({ "leases": [lease(&first)] })),
        json!([null])
    );
    fails(
        db.call(
            "jobs.fail",
            with(lease(&first), json!({ "error": "stale" })),
        ),
        "LEASE_LOST",
    );
    db.mutate(
        "jobs.complete",
        with(lease(&second), json!({ "result": "done" })),
    );
    let job = db.job("one");
    assert_eq!(
        (&job["state"], &job["result"]),
        (&json!("completed"), &json!("done"))
    );
}

#[tokio::test(start_paused = true)]
async fn readiness_stays_true_until_drained_failed_work_returns_after_its_backoff_and_final_failures_need_a_retry()
 {
    let db = workers();
    for id in ["one", "two"] {
        db.mutate("jobs.enqueue", json!({ "id": id, "payload": id }));
    }
    let first = claim(&db, "worker-a");
    assert!(ready(&db), "another pending job keeps the queue ready");
    let second = claim(&db, "worker-b");
    assert!(!ready(&db));
    db.mutate(
        "jobs.complete",
        with(lease(&first), json!({ "result": null })),
    );

    let failed = db.mutate(
        "jobs.fail",
        with(
            lease(&second),
            json!({ "error": { "code": "EXTERNAL_FAILURE" } }),
        ),
    );
    assert_eq!(
        (
            &failed["state"],
            &failed["attempts"],
            &failed["availableAt"]
        ),
        (&json!("pending"), &json!(1), &json!(T0 + 1_000))
    );
    assert_eq!(
        db.query("jobs.stats", Value::Null),
        json!({ "ready": false, "oldestReadyAt": null, "nextAvailableAt": T0 + 1_000, "readyCount": 0, "leasedCount": 0, "delayedCount": 1 })
    );
    db.advance(999);
    assert!(!ready(&db));
    db.advance(1);
    assert!(ready(&db));
    let third = claim(&db, "worker-c");
    assert_eq!(
        (&third["id"], &third["attempt"]),
        (&json!("two"), &json!(2))
    );

    let last = db.mutate(
        "jobs.fail",
        with(
            lease(&third),
            json!({ "error": { "code": "PERMANENT" }, "retry": false }),
        ),
    );
    assert_eq!(
        (&last["state"], &last["availableAt"]),
        (&json!("failed"), &Value::Null)
    );
    db.advance(3_600_000);
    assert!(!ready(&db), "failed work does not come back on its own");
    fails(
        db.call("jobs.retry", json!({ "id": "one" })),
        "JOB_NOT_FAILED",
    );
    db.mutate("jobs.retry", json!({ "id": "two" }));
    assert!(ready(&db));
    let retried = claim(&db, "worker-d");
    assert_eq!(
        (&retried["id"], &retried["attempt"]),
        (&json!("two"), &json!(1)),
        "a retry starts a fresh attempt budget"
    );
    assert!(!ready(&db));
}

#[tokio::test(start_paused = true)]
async fn a_lease_that_keeps_expiring_spends_the_attempt_budget_and_then_fails_for_good() {
    let db = workers();
    db.mutate("jobs.enqueue", json!({ "id": "doomed", "payload": null }));
    for attempt in 1..=5 {
        assert_eq!(
            claim(&db, &format!("worker-{attempt}"))["attempt"],
            json!(attempt)
        );
        db.advance(100);
    }
    let job = db.job("doomed");
    assert_eq!(
        (&job["state"], &job["attempts"], &job["error"]["code"]),
        (&json!("failed"), &json!(5), &json!("LEASE_EXPIRED"))
    );
    assert!(!ready(&db));
    assert_eq!(
        db.mutate("jobs.claim", json!({ "owner": "worker-6" })),
        Value::Null
    );
}

// ---- sdk/external.test.ts, on the digest

fn reactive() -> FakeFlower {
    FakeFlower::reactive(Clock::tokio_at(T0))
}

fn put(db: &FakeFlower, id: &str, text: &str) {
    db.mutate("document.put", json!({ "id": id, "text": text }));
}

fn input(text: &str) -> Value {
    json!({ "recipe": "sha256-v1", "text": text })
}

fn key(id: &str, text: &str) -> String {
    canonical_json(&json!([id, input(text)]))
}

fn sha(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn state(db: &FakeFlower, id: &str) -> Value {
    let document = db.query("document.get", json!(id));
    if document.is_null() {
        Value::Null
    } else {
        document["digest"].clone()
    }
}

fn publish(db: &FakeFlower, args: &str, key: &str, value: &str) -> Value {
    db.mutate(
        "digest.publish",
        json!({ "args": args, "key": key, "value": value }),
    )
}

fn ids(work: &Value) -> Vec<String> {
    work.as_array()
        .unwrap()
        .iter()
        .map(|each| each["args"].as_str().unwrap().to_owned())
        .collect()
}

/// `lease(claim)`: the identity HTTP lease methods take.
fn identity(claim: &Value) -> Value {
    json!({ "args": claim["args"], "key": claim["key"], "owner": claim["owner"], "attempt": claim["attempt"] })
}

#[test]
fn shards_hash_keys_like_the_ts() {
    // Values from `shardOf` in sdk/external.ts.
    for (key, seven, big) in [
        ("\"a\"", 4, 989_563),
        ("\"doc-7\"", 3, 770_258),
        ("[\"o1\",\"l1\"]", 3, 581_328),
        ("\"é😀\"", 2, 568_132),
        ("", 2, 129_763),
    ] {
        assert_eq!(
            (shard_of(key, 7), shard_of(key, 1_000_003)),
            (seven, big),
            "{key}"
        );
    }
}

#[test]
fn external_values_stay_pending_until_a_result_for_the_current_input_is_published() {
    let db = reactive();
    assert_eq!(state(&db, "a"), Value::Null);
    assert_eq!(db.query("digest.pending", json!("a")), Value::Null);
    put(&db, "a", "one two");
    assert_eq!(state(&db, "a"), json!({ "status": "pending" }));
    let work = db.query("digest.pending", json!("a"));
    assert_eq!(
        work,
        json!({ "args": "a", "key": key("a", "one two"), "input": input("one two") })
    );
    let work_key = work["key"].as_str().unwrap();
    assert_eq!(
        publish(&db, "a", "stale", &sha("x")),
        json!({ "accepted": false })
    );
    assert_eq!(
        publish(&db, "b", work_key, &sha("x")),
        json!({ "accepted": false })
    );
    assert_eq!(state(&db, "a"), json!({ "status": "pending" }));
    assert_eq!(
        publish(&db, "a", work_key, &sha("2")),
        json!({ "accepted": true })
    );
    assert_eq!(
        state(&db, "a"),
        json!({ "status": "ready", "value": sha("2") })
    );
    assert_eq!(db.query("digest.pending", json!("a")), Value::Null);
    assert_eq!(
        publish(&db, "a", work_key, &sha("3")),
        json!({ "accepted": true })
    );
    assert_eq!(
        state(&db, "a"),
        json!({ "status": "ready", "value": sha("2") }),
        "the first result for an input wins"
    );

    put(&db, "a", "one two three");
    assert_eq!(state(&db, "a"), json!({ "status": "pending" }));
    assert_eq!(
        publish(&db, "a", work_key, &sha("2")),
        json!({ "accepted": false })
    );
    let next = db.query("digest.pending", json!("a"));
    assert_ne!(next["key"], work["key"]);
    publish(&db, "a", next["key"].as_str().unwrap(), &sha("3"));
    assert_eq!(
        state(&db, "a"),
        json!({ "status": "ready", "value": sha("3") })
    );
    db.mutate("document.delete", json!("a"));
    assert_eq!(state(&db, "a"), Value::Null, "a null input means no value");
    assert_eq!(db.query("digest.pending", json!("a")), Value::Null);
    fails(db.call("digest.pending", json!(1)), "INVALID_ARGUMENT");
}

#[test]
fn publish_validates_results_and_its_arguments() {
    let db = reactive();
    put(&db, "a", "x");
    let key = db.query("digest.pending", json!("a"))["key"].clone();
    let invalid = fails(
        db.call(
            "digest.publish",
            json!({ "args": "a", "key": key, "value": "not a digest" }),
        ),
        "INVALID_ARGUMENT",
    );
    assert_eq!(invalid.status, 422);
    let missing = fails(
        db.call("digest.publish", json!({ "args": "a", "value": sha("x") })),
        "INVALID_ARGUMENT",
    );
    assert_eq!(missing.failure.unwrap().1, "is missing \"key\"");
    assert_eq!(state(&db, "a"), json!({ "status": "pending" }));
}

#[test]
fn each_marks_changed_rows_stale_for_worker_pools_oldest_first_with_limits_and_shards() {
    let db = reactive();
    for id in ["c", "a", "e", "b", "d"] {
        put(&db, id, id);
        db.advance(1);
    }
    let all = db.query("digest.next", Value::Null);
    assert_eq!(ids(&all), ["c", "a", "e", "b", "d"]);
    assert_eq!(
        all[0],
        json!({ "args": "c", "key": key("c", "c"), "input": input("c") })
    );
    assert_eq!(
        ids(&db.query("digest.next", json!({ "limit": 2 }))),
        ["c", "a"]
    );
    let mut shards: Vec<String> = (0..3)
        .flat_map(|index| ids(&db.query("digest.next", json!({ "shard": [index, 3] }))))
        .collect();
    shards.sort();
    assert_eq!(
        shards,
        ["a", "b", "c", "d", "e"],
        "shards partition the pending rows"
    );
    assert_eq!(
        ids(&db.query("digest.next", json!({ "limit": 1, "shard": [0, 1] }))),
        ["c"]
    );

    assert_eq!(
        publish(&db, "c", all[0]["key"].as_str().unwrap(), &sha("C")),
        json!({ "accepted": true })
    );
    assert_eq!(
        state(&db, "c"),
        json!({ "status": "ready", "value": sha("C") })
    );
    assert_eq!(
        ids(&db.query("digest.next", Value::Null)),
        ["a", "e", "b", "d"]
    );
    put(&db, "c", "changed");
    assert_eq!(
        ids(&db.query("digest.next", Value::Null)),
        ["a", "e", "b", "d", "c"]
    );
    put(&db, "a", "a2");
    assert_eq!(
        db.query("digest.next", Value::Null)[0]["input"],
        input("a2"),
        "rewriting a stale row keeps its place and refreshes its input"
    );
    for bad in [
        json!({ "limit": 0 }),
        json!({ "shard": [0, 0] }),
        json!({ "limit": 1025 }),
    ] {
        fails(db.call("digest.next", bad), "INVALID_ARGUMENT");
    }
}

#[test]
fn deleting_a_tracked_row_removes_its_result_and_stale_marker() {
    let db = reactive();
    put(&db, "a", "x");
    let work = db.query("digest.next", Value::Null)[0].clone();
    publish(&db, "a", work["key"].as_str().unwrap(), &sha("X"));
    db.mutate("document.delete", json!("a"));
    assert_eq!(state(&db, "a"), Value::Null);
    put(&db, "a", "y");
    assert_eq!(
        state(&db, "a"),
        json!({ "status": "pending" }),
        "the old result went with the row"
    );
    let inputs: Vec<Value> = db
        .query("digest.next", Value::Null)
        .as_array()
        .unwrap()
        .iter()
        .map(|each| each["input"].clone())
        .collect();
    assert_eq!(inputs, [input("y")]);
    db.mutate("document.delete", json!("a"));
    assert_eq!(db.query("digest.next", Value::Null), json!([]));
    assert_eq!(
        db.query("digest.stats", Value::Null)["ready"],
        json!(false),
        "no stale marker is left"
    );
    assert_eq!(state(&db, "a"), Value::Null);
}

#[test]
fn claims_lease_the_longest_waiting_keys_to_one_owner_until_the_lease_ends() {
    let db = reactive();
    for id in ["c", "a", "b"] {
        put(&db, id, id);
        db.advance(1);
    }
    assert_eq!(db.query("digest.ready", Value::Null), json!(true));
    let start = db.now();
    let first = db.mutate(
        "digest.claim",
        json!({ "owner": "w1", "limit": 2, "leaseMs": 1_000 }),
    );
    assert_eq!(
        first,
        json!([
            { "args": "c", "key": key("c", "c"), "input": input("c"), "owner": "w1", "attempt": 1, "expiresAt": start + 1_000 },
            { "args": "a", "key": key("a", "a"), "input": input("a"), "owner": "w1", "attempt": 1, "expiresAt": start + 1_000 },
        ])
    );
    assert_eq!(
        ids(&db.mutate("digest.claim", json!({ "owner": "w2" }))),
        ["b"],
        "leased keys are skipped"
    );
    assert_eq!(
        db.mutate("digest.claim", json!({ "owner": "w3" })),
        json!([])
    );
    assert_eq!(db.query("digest.ready", Value::Null), json!(false));
    assert_eq!(
        db.query("digest.stats", Value::Null),
        json!({ "ready": false, "oldestReadyAt": null, "nextAvailableAt": start + 1_000 })
    );
    assert_eq!(
        db.query("digest.next", Value::Null)
            .as_array()
            .unwrap()
            .len(),
        3,
        "next still lists leased keys"
    );

    db.advance(1_000);
    assert_eq!(
        db.query("digest.stats", Value::Null),
        json!({ "ready": true, "oldestReadyAt": start + 1_000, "nextAvailableAt": start + 30_000 })
    );
    let again = db.mutate("digest.claim", json!({ "owner": "w3" }));
    let summary: Vec<Value> = again
        .as_array()
        .unwrap()
        .iter()
        .map(|work| json!([work["args"], work["owner"], work["attempt"]]))
        .collect();
    assert_eq!(
        summary,
        [json!(["a", "w3", 2]), json!(["c", "w3", 2])],
        "expired leases go back in line; equal expiries follow key order"
    );
    assert_eq!(
        db.mutate(
            "digest.renew",
            json!({ "leases": [identity(&first[0]), identity(&again[1])] })
        ),
        json!([null, db.now() + 30_000]),
        "renew skips lost leases"
    );
    fails(
        db.call("digest.renew", json!({ "leases": [again[1]] })),
        "INVALID_ARGUMENT",
    );

    // publish needs no lease: the input key decides. It also retires the lease.
    assert_eq!(
        publish(&db, "a", first[1]["key"].as_str().unwrap(), &sha("A")),
        json!({ "accepted": true })
    );
    assert_eq!(
        db.mutate("digest.renew", json!({ "leases": [identity(&again[0])] })),
        json!([null])
    );
    assert_eq!(
        db.mutate("digest.release", identity(&again[0])),
        json!(false)
    );
    assert_eq!(
        state(&db, "a"),
        json!({ "status": "ready", "value": sha("A") })
    );
}

#[test]
fn release_hands_a_key_back_at_once_or_after_a_delay_and_a_changed_input_voids_its_lease() {
    let db = reactive();
    put(&db, "a", "one");
    let claim = db.mutate("digest.claim", json!({ "owner": "w1" }))[0].clone();
    assert_eq!(
        db.mutate(
            "digest.release",
            with(identity(&claim), json!({ "owner": "w2" }))
        ),
        json!(false),
        "only the owner can release"
    );
    assert_eq!(
        db.mutate(
            "digest.release",
            with(identity(&claim), json!({ "delayMs": 500 }))
        ),
        json!(true)
    );
    assert_eq!(
        db.mutate("digest.release", identity(&claim)),
        json!(false),
        "a released lease is gone"
    );
    assert_eq!(
        db.mutate("digest.claim", json!({ "owner": "w2" })),
        json!([])
    );
    assert_eq!(
        db.query("digest.stats", Value::Null),
        json!({ "ready": false, "oldestReadyAt": null, "nextAvailableAt": db.now() + 500 })
    );
    db.advance(500);
    let retry = db.mutate("digest.claim", json!({ "owner": "w2" }))[0].clone();
    assert_eq!(
        (&retry["owner"], &retry["attempt"]),
        (&json!("w2"), &json!(2)),
        "attempts count claims of one input"
    );
    assert_eq!(db.mutate("digest.release", identity(&retry)), json!(true));

    let held = db.mutate("digest.claim", json!({ "owner": "w1" }))[0].clone();
    assert_eq!(held["attempt"], json!(3));
    put(&db, "a", "one");
    assert_eq!(
        db.mutate("digest.renew", json!({ "leases": [identity(&held)] })),
        json!([db.now() + 30_000]),
        "rewriting the same input keeps the lease"
    );
    put(&db, "a", "two");
    assert_eq!(
        db.mutate("digest.renew", json!({ "leases": [identity(&held)] })),
        json!([null]),
        "a new input voids the lease"
    );
    let fresh = db.mutate("digest.claim", json!({ "owner": "w2" }))[0].clone();
    assert_eq!(
        (&fresh["input"], &fresh["attempt"]),
        (&input("two"), &json!(1)),
        "and is claimable at once, with a fresh attempt count"
    );
    assert_eq!(
        db.mutate(
            "digest.release",
            with(identity(&fresh), json!({ "delayMs": 60_000 }))
        ),
        json!(true)
    );
    put(&db, "a", "three");
    assert_eq!(
        db.mutate("digest.claim", json!({ "owner": "w3" }))[0]["input"],
        input("three"),
        "a new input also skips a retry delay"
    );
}

#[test]
fn claims_drop_keys_that_are_no_longer_pending_and_respect_lease_limits() {
    let db = reactive();
    put(&db, "a", "x");
    let claim = db.mutate("digest.claim", json!({ "owner": "w1" }))[0].clone();
    assert_eq!(
        claim["expiresAt"],
        json!(db.now() + 30_000),
        "the default lease applies"
    );
    fails(
        db.call("digest.claim", json!({ "owner": "w1", "leaseMs": 300_001 })),
        "LEASE_TOO_LONG",
    );
    fails(
        db.call(
            "digest.renew",
            json!({ "leases": [identity(&claim)], "leaseMs": 300_001 }),
        ),
        "LEASE_TOO_LONG",
    );
    fails(
        db.call("digest.claim", json!({ "owner": "w1", "limit": 1_025 })),
        "INVALID_ARGUMENT",
    );
    fails(
        db.call(
            "digest.release",
            with(identity(&claim), json!({ "delayMs": -1 })),
        ),
        "INVALID_ARGUMENT",
    );
    fails(
        db.call("digest.claim", json!({ "owner": "" })),
        "INVALID_ARGUMENT",
    );

    put(&db, "b", "y");
    db.mutate("document.delete", json!("b"));
    db.mutate("document.delete", json!("a"));
    assert_eq!(db.query("digest.ready", Value::Null), json!(false));
    assert_eq!(
        db.mutate("digest.claim", json!({ "owner": "w2" })),
        json!([])
    );
    assert_eq!(
        db.mutate("digest.renew", json!({ "leases": [identity(&claim)] })),
        json!([null])
    );
}
