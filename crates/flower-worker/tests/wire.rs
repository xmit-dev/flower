//! The bytes on the wire: the same scenarios as `parity/wire.ts`, which ran the TS SDK's
//! `runQueueWorker` and `reconcile` against the reference test database, run here on the Rust
//! worker and the in-memory Flower. Every call's arguments must equal the TS ones byte for byte
//! (per method, the distinct arguments in the order first sent: `tests/golden/wire.json`).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use flower_worker::testing::{FakeClient, FakeFlower, QueueConfig};
use flower_worker::{
    Claim, Clock, Concurrency, ExternalWork, JobStop, Load, QueueWorkerEvent, QueueWorkerOptions, ReconcileEvent,
    ReconcileOptions, WorkError, reconcile, run_queue_worker,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use support::until;
use tokio_util::sync::CancellationToken;

type Summary = BTreeMap<String, Vec<String>>;

const T0: i64 = 1_000_000;

fn golden() -> BTreeMap<String, Summary> {
    serde_json::from_str(include_str!("golden/wire.json")).expect("tests/golden/wire.json parses")
}

/// Per `"{kind} {name}"`, the distinct arguments in the order first sent.
fn summary(client: &FakeClient) -> Summary {
    let mut out = Summary::new();
    for call in client.calls() {
        let kind = if call.watch { "watch" } else { "mutate" };
        let list = out.entry(format!("{kind} {}", call.name)).or_default();
        if !list.contains(&call.args) {
            list.push(call.args);
        }
    }
    out
}

fn check(name: &str, client: &FakeClient) {
    let expected = golden().remove(name).unwrap_or_else(|| panic!("no golden for {name}"));
    let actual = summary(client);
    assert_eq!(actual, expected, "{name}: the Rust worker's calls differ from the TS worker's");
}

#[derive(Deserialize)]
struct Payload {
    id: String,
}

/// `{ zeta: 1, alpha: { b: 2, a: [1.5, "é\u2028", 1e21] } }`, in that field order.
#[derive(Serialize)]
struct Tricky {
    zeta: u32,
    alpha: Alpha,
}

#[derive(Serialize)]
struct Alpha {
    b: u32,
    a: (f64, &'static str, f64),
}

const TRICKY: Tricky = Tricky {
    zeta: 1,
    alpha: Alpha {
        b: 2,
        a: (1.5, "é\u{2028}", 1e21),
    },
};

#[derive(Serialize)]
#[serde(untagged)]
enum Outcome {
    Done { done: String },
    Tricky(Tricky),
}

fn events<E: Send + 'static>() -> (Arc<Mutex<Vec<E>>>, impl Fn(E) + Send + Sync + 'static) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = events.clone();
    (events, move |event| recorded.lock().push(event))
}

fn scoped() -> FakeFlower {
    let flower = FakeFlower::new(Clock::tokio_at(T0));
    flower.add_queue(
        "sjobs",
        QueueConfig {
            scope_argument: true,
            ..QueueConfig::workers()
        },
    );
    flower
}

fn options(flower: &FakeFlower, queue: &str, signal: &CancellationToken) -> QueueWorkerOptions {
    QueueWorkerOptions {
        health: Some(Arc::new(Load::idle)),
        clock: flower.clock().clone(),
        concurrency: Some(Concurrency::Fixed(1)),
        claimers: Some(1),
        ..QueueWorkerOptions::new(queue, signal.clone())
    }
}

#[tokio::test(start_paused = true)]
async fn a_scoped_worker_in_line_chaining_claims_into_reports() {
    let flower = scoped();
    for id in ["a", "b", "c"] {
        flower.mutate("sjobs.enqueue", json!({ "scope": "s1", "id": id, "payload": { "id": id } }));
    }
    let client = flower.client();
    let stop = CancellationToken::new();
    let (seen, record) = events::<QueueWorkerEvent>();
    let options = QueueWorkerOptions {
        scope: Some("s1".into()),
        owner: Some("w1".into()),
        wait: true,
        wait_ms: Some(20_000),
        chain: true,
        batch: Some(4),
        lease_ms: Some(10_000),
        ..options(&flower, "sjobs", &stop)
    }
    .on_event(record);
    let done = tokio::spawn(run_queue_worker(client.clone(), options, |job: Claim<Payload>, _, _| async move {
        match job.payload.id.as_str() {
            "b" => Err(WorkError::new("boom")),
            "c" => Ok(Outcome::Tricky(TRICKY)),
            id => Ok(Outcome::Done { done: id.to_owned() }),
        }
    }));
    until(|| {
        seen.lock().iter().any(|event| matches!(event, QueueWorkerEvent::Completed { id } if id == "c"))
            && client.calls().iter().any(|call| call.watch)
    })
    .await;
    stop.cancel();
    done.await.unwrap().unwrap();
    check("scoped line chain", &client);
}

#[tokio::test(start_paused = true)]
async fn short_leases_renewed_and_a_drain_that_releases_the_job_still_running() {
    let flower = FakeFlower::workers(Clock::tokio_at(T0));
    flower.enqueue(&["quick", "long"]);
    let client = flower.client();
    let stop = CancellationToken::new();
    let options = QueueWorkerOptions {
        owner: Some("w2".into()),
        chain: true,
        lease_ms: Some(300),
        drain_ms: Some(50),
        release: true,
        ..options(&flower, "jobs", &stop)
    };
    let done = tokio::spawn(run_queue_worker(client.clone(), options, |job: Claim<Payload>, stop: JobStop, _| async move {
        if job.payload.id == "quick" {
            return Ok(TRICKY);
        }
        stop.stopped().await;
        Err(WorkError::new(stop.reason().unwrap_or_default()))
    }));
    until(|| !client.calls_to("jobs.renew").is_empty()).await;
    stop.cancel();
    done.await.unwrap().unwrap();
    check("renew drain release", &client);
}

#[tokio::test(start_paused = true)]
async fn a_scoped_worker_outside_any_line_claiming_one_job_per_call() {
    let flower = scoped();
    flower.mutate("sjobs.enqueue", json!({ "scope": "s3", "id": "x", "payload": { "id": "x" } }));
    let client = flower.client();
    let stop = CancellationToken::new();
    let (seen, record) = events::<QueueWorkerEvent>();
    let options = QueueWorkerOptions {
        scope: Some("s3".into()),
        owner: Some("w3".into()),
        ..options(&flower, "sjobs", &stop)
    }
    .on_event(record);
    let done = tokio::spawn(run_queue_worker(client.clone(), options, |_: Claim<Value>, _, _| async { Ok(()) }));
    until(|| {
        seen.lock().iter().any(|event| event.kind() == "completed") && client.calls().iter().filter(|call| call.watch).count() >= 2
    })
    .await;
    stop.cancel();
    done.await.unwrap().unwrap();
    check("scoped claims", &client);
}

#[derive(Clone, Deserialize)]
struct Input {
    text: String,
}

fn sha(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn reactive(documents: &[(&str, &str)]) -> FakeFlower {
    let flower = FakeFlower::reactive(Clock::tokio_at(T0));
    for (id, text) in documents {
        flower.mutate("document.put", json!({ "id": id, "text": text }));
    }
    flower
}

fn published(seen: &Mutex<Vec<ReconcileEvent>>) -> bool {
    seen.lock().iter().any(|event| event.kind() == "published")
}

#[tokio::test(start_paused = true)]
async fn reconcile_one_key() {
    let flower = reactive(&[("one", "A")]);
    let client = flower.client();
    let stop = CancellationToken::new();
    let (seen, record) = events::<ReconcileEvent>();
    let options = ReconcileOptions {
        clock: flower.clock().clone(),
        ..ReconcileOptions::new("digest", stop.clone()).with_args("one").on_event(record)
    };
    let done = tokio::spawn(reconcile(client.clone(), options, |input: Input, _: ExternalWork<Value, Input>, _| async move {
        Ok(sha(&input.text))
    }));
    until(|| published(&seen)).await;
    stop.cancel();
    done.await.unwrap().unwrap();
    check("reconcile one key", &client);
}

#[tokio::test(start_paused = true)]
async fn reconcile_a_sharded_pool() {
    let flower = reactive(&[("p", "P")]);
    let client = flower.client();
    let stop = CancellationToken::new();
    let (seen, record) = events::<ReconcileEvent>();
    let options = ReconcileOptions {
        clock: flower.clock().clone(),
        shard: Some((0, 1)),
        batch: Some(2),
        ..ReconcileOptions::new("digest", stop.clone()).on_event(record)
    };
    let done = tokio::spawn(reconcile(client.clone(), options, |input: Input, _: ExternalWork<Value, Input>, _| async move {
        Ok(sha(&input.text))
    }));
    until(|| published(&seen)).await;
    stop.cancel();
    done.await.unwrap().unwrap();
    check("reconcile pool", &client);
}

#[tokio::test(start_paused = true)]
async fn reconcile_a_leased_pool_that_renews_and_hands_a_key_back() {
    let flower = reactive(&[("fast", "fast"), ("slow", "slow")]);
    let client = flower.client();
    let stop = CancellationToken::new();
    let options = ReconcileOptions {
        clock: flower.clock().clone(),
        lease: true,
        owner: Some("r1".into()),
        concurrency: Some(Concurrency::Fixed(1)),
        lease_ms: Some(300),
        ..ReconcileOptions::new("digest", stop.clone())
    };
    let done = tokio::spawn(reconcile(
        client.clone(),
        options,
        |input: Input, _: ExternalWork<Value, Input>, stop: JobStop| async move {
            if input.text == "fast" {
                return Ok(sha(&input.text));
            }
            stop.stopped().await;
            Err(WorkError::new(stop.reason().unwrap_or_default()))
        },
    ));
    until(|| !client.calls_to("digest.renew").is_empty()).await;
    stop.cancel();
    done.await.unwrap().unwrap();
    check("reconcile leased", &client);
}
