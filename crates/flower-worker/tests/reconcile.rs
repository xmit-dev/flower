//! Ports of `sdk/reactive-worker-client.test.ts`: `reconcile` keeping `docs/reactive-worker.ts`'s
//! digest current, on paused tokio time against the in-memory Flower.

mod support;

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use flower_worker::testing::{FakeClient, FakeError, FakeFlower};
use flower_worker::{
    Adaptive, ClientError, Clock, Concurrency, ExternalWork, JobStop, Load, ReconcileEvent, ReconcileOptions, WorkError,
    WorkerError, reconcile,
};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use support::{Deferred, sleep, until};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type Outcome = Result<(), WorkerError<FakeError>>;

#[derive(Clone, Debug, Deserialize)]
struct Input {
    recipe: String,
    text: String,
}

type Work = ExternalWork<String, Input>;

fn sha(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn application() -> FakeFlower {
    FakeFlower::reactive(Clock::tokio())
}

fn put(flower: &FakeFlower, id: &str, text: &str) {
    flower.mutate("document.put", json!({ "id": id, "text": text }));
}

/// `digestOf(db, id, text)`: wait until the document's digest is ready and matches.
async fn digest_of(flower: &FakeFlower, id: &str, text: &str) {
    let wanted = json!({ "status": "ready", "value": sha(text) });
    until(|| flower.query("document.get", json!(id))["digest"] == wanted).await;
}

/// `webcrypto.subtle.digest`, which some tests intercept.
type DigestFn = Arc<dyn Fn(String) -> BoxFuture<'static, Result<String, WorkError>> + Send + Sync>;

fn real_digest() -> DigestFn {
    Arc::new(|text: String| async move { Ok(sha(&text)) }.boxed())
}

/// `runWorker(client, signal, id)` from `docs/reactive-worker-client.ts`, logging event types.
fn run_worker(client: &FakeClient, signal: CancellationToken, id: Option<&str>, digest: DigestFn, log: Arc<Mutex<Vec<&'static str>>>) -> JoinHandle<Outcome> {
    let mut options = ReconcileOptions::new("digest", signal).on_event(move |event| log.lock().push(event.kind()));
    options.clock = client.flower().clock().clone();
    match id {
        None => options.lease = true,
        Some(id) => options = options.with_args(id),
    }
    tokio::spawn(reconcile(client.clone(), options, move |input: Input, _work: Work, _stop| {
        let digest = digest.clone();
        async move {
            if input.recipe != "sha256-v1" {
                return Err(WorkError::new("Unsupported worker recipe"));
            }
            digest(input.text).await
        }
    }))
}

fn log() -> Arc<Mutex<Vec<&'static str>>> {
    Arc::default()
}

/// A publication as the recording client saw it: its request ID, value and Flower's answer.
#[derive(Clone, Debug)]
struct Publication {
    request_id: String,
    value: String,
    reply: Result<Value, FakeError>,
}

/// `recording(db, intercept)`: record each publication and its reply; `intercept` may replace the
/// reply with an error (given the publication's count).
fn recording(flower: &FakeFlower, intercept: impl Fn(usize) -> Option<FakeError> + Send + Sync + 'static) -> (FakeClient, Arc<Mutex<Vec<Publication>>>) {
    let publications = Arc::new(Mutex::new(Vec::<Publication>::new()));
    let recorded = publications.clone();
    let client = flower.client().after(move |call, reply| {
        if call.name != "digest.publish" {
            return None;
        }
        let count = {
            let mut publications = recorded.lock();
            publications.push(Publication {
                request_id: call.request_id.clone().unwrap_or_default(),
                value: call.args()["value"].as_str().unwrap_or_default().to_owned(),
                reply: reply.clone(),
            });
            publications.len()
        };
        intercept(count)
    });
    (client, publications)
}

fn accepted(reply: &Result<Value, FakeError>) -> bool {
    reply.as_ref().expect("a reply")["accepted"] == json!(true)
}

#[tokio::test(start_paused = true)]
async fn a_single_key_worker_keeps_one_documents_digest_current_as_it_changes() {
    let flower = application();
    let stop = CancellationToken::new();
    let log = log();
    let worker = run_worker(&flower.client(), stop.clone(), Some("one"), real_digest(), log.clone());
    put(&flower, "one", "A");
    put(&flower, "two", "not mine");
    digest_of(&flower, "one", "A").await;
    put(&flower, "one", "B");
    digest_of(&flower, "one", "B").await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(
        flower.query("document.get", json!("two")),
        json!({ "text": "not mine", "digest": { "status": "pending" } })
    );
    assert_eq!(*log.lock(), ["published", "published"]);
}

#[tokio::test(start_paused = true)]
async fn a_pool_worker_drains_every_pending_document_and_follows_edits_additions_and_deletions() {
    let flower = application();
    for id in ["one", "two", "three"] {
        put(&flower, id, id);
    }
    let stop = CancellationToken::new();
    let worker = run_worker(&flower.client(), stop.clone(), None, real_digest(), log());
    for id in ["one", "two", "three"] {
        digest_of(&flower, id, id).await;
    }
    put(&flower, "two", "second draft");
    flower.mutate("document.delete", json!("three"));
    put(&flower, "four", "four");
    digest_of(&flower, "two", "second draft").await;
    digest_of(&flower, "four", "four").await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(flower.query("digest.next", Value::Null), json!([]));
    assert_eq!(flower.query("document.get", json!("three")), Value::Null);
}

#[tokio::test(start_paused = true)]
async fn a_result_computed_for_a_superseded_input_is_rejected_and_the_worker_computes_the_current_one() {
    let flower = application();
    let (client, publications) = recording(&flower, |_| None);
    let (started, release) = (Deferred::new(), Deferred::new());
    let computations = Arc::new(AtomicUsize::new(0));
    let digest: DigestFn = {
        let (started, release, computations) = (started.clone(), release.clone(), computations.clone());
        Arc::new(move |text: String| {
            let (started, release) = (started.clone(), release.clone());
            let first = computations.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    started.resolve();
                    release.wait().await;
                }
                Ok(sha(&text))
            }
            .boxed()
        })
    };
    put(&flower, "one", "A");
    let stop = CancellationToken::new();
    let worker = run_worker(&client, stop.clone(), Some("one"), digest, log());
    started.wait().await;
    put(&flower, "one", "B");
    release.resolve();
    digest_of(&flower, "one", "B").await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    let seen: Vec<(String, bool)> = publications.lock().iter().map(|p| (p.value.clone(), accepted(&p.reply))).collect();
    assert_eq!(seen, [(sha("A"), false), (sha("B"), true)]);
    assert_eq!(computations.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_publication_whose_reply_is_lost_is_retried_with_the_same_request_id_and_applied_once() {
    let flower = application();
    let (client, publications) = recording(&flower, |count| (count == 1).then(FakeError::fetch_failed));
    put(&flower, "one", "A");
    let stop = CancellationToken::new();
    let log = log();
    let worker = run_worker(&client, stop.clone(), Some("one"), real_digest(), log.clone());
    until(|| log.lock().len() == 1).await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    let publications = publications.lock().clone();
    assert_eq!(publications.len(), 2);
    assert_eq!(publications[0].request_id, publications[1].request_id);
    // Flower ran the first attempt; the second got its receipt (`duplicate: true`).
    assert!(publications.iter().all(|p| accepted(&p.reply)));
    assert_eq!(flower.replays(), [publications[0].request_id.clone()]);
    assert_eq!(
        flower.query("document.get", json!("one")),
        json!({ "text": "A", "digest": { "status": "ready", "value": sha("A") } })
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_computation_is_reported_and_retried_until_it_succeeds() {
    let computations = Arc::new(AtomicUsize::new(0));
    let digest: DigestFn = {
        let computations = computations.clone();
        Arc::new(move |text: String| {
            let first = computations.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    return Err(WorkError::new("hardware hiccup"));
                }
                Ok(sha(&text))
            }
            .boxed()
        })
    };
    let flower = application();
    put(&flower, "one", "A");
    let stop = CancellationToken::new();
    let log = log();
    let worker = run_worker(&flower.client(), stop.clone(), Some("one"), digest, log.clone());
    digest_of(&flower, "one", "A").await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(*log.lock(), ["failed", "published"]);
    assert_eq!(computations.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn the_worker_rides_out_transient_watch_failures_and_stops_on_a_permanent_one() {
    let flower = application();
    let watches = Arc::new(AtomicUsize::new(0));
    let client = {
        let watches = watches.clone();
        flower.client().before(move |call| {
            if !call.watch {
                return None;
            }
            Some(match watches.fetch_add(1, Ordering::SeqCst) + 1 {
                1 => FakeError::http(503, "UNAVAILABLE", "No leader yet"),
                _ => FakeError::denied(403, "FORBIDDEN", "Authorization denied", ("FORBIDDEN", "Access denied")),
            })
        })
    };
    let error = run_worker(&client, CancellationToken::new(), Some("one"), real_digest(), log())
        .await
        .unwrap()
        .unwrap_err();
    let error = error.client().expect("a client error");
    assert_eq!((error.status(), error.failure_code()), (403, Some("FORBIDDEN")));
    assert_eq!(watches.load(Ordering::SeqCst), 2);
}

/// Tracks how many computations run at once.
#[derive(Clone, Default)]
struct Peak {
    running: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl Peak {
    async fn during<T>(&self, work: impl Future<Output = T>) -> T {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let result = work.await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        result
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

#[tokio::test(start_paused = true)]
async fn sharded_reconcile_pools_split_the_keys_between_them_and_compute_concurrently() {
    let flower = application();
    let ids: Vec<String> = (0..8).map(|index| format!("doc-{index}")).collect();
    for id in &ids {
        put(&flower, id, id);
    }
    let seen = [Arc::new(Mutex::new(BTreeSet::new())), Arc::new(Mutex::new(BTreeSet::new()))];
    let peak = Peak::default();
    let stop = CancellationToken::new();
    let pools: Vec<JoinHandle<Outcome>> = (0..2)
        .map(|index| {
            let mut options = ReconcileOptions::new("digest", stop.clone());
            options.shard = Some((index as u64, 2));
            options.concurrency = Some(Concurrency::Fixed(3));
            options.clock = flower.clock().clone();
            let (seen, peak) = (seen[index].clone(), peak.clone());
            tokio::spawn(reconcile(flower.client(), options, move |input: Input, work: Work, _stop| {
                seen.lock().insert(work.args);
                let peak = peak.clone();
                async move {
                    peak.during(sleep(5)).await;
                    Ok(sha(&input.text))
                }
            }))
        })
        .collect();
    for id in &ids {
        digest_of(&flower, id, id).await;
    }
    stop.cancel();
    for pool in pools {
        pool.await.unwrap().unwrap();
    }
    let (first, second) = (seen[0].lock().clone(), seen[1].lock().clone());
    let mut all: Vec<String> = first.iter().chain(second.iter()).cloned().collect();
    all.sort();
    assert_eq!(all, ids);
    assert_eq!(first.intersection(&second).count(), 0, "shards are disjoint");
    assert!(peak.peak() > 1, "peak concurrency {}", peak.peak());
}

/// The document id in an event's key (`JSON.parse(event.key)[0]`).
fn key_id(key: &str) -> String {
    let key: Value = serde_json::from_str(key).unwrap();
    key[0].as_str().unwrap_or_default().to_owned()
}

#[tokio::test(start_paused = true)]
async fn a_pool_keeps_publishing_other_keys_when_one_computed_value_fails_the_result_schema() {
    let flower = application();
    for id in ["good-1", "bad", "good-2"] {
        put(&flower, id, id);
    }
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let stop = CancellationToken::new();
    let recorded = events.clone();
    let mut options = ReconcileOptions::new("digest", stop.clone()).on_event(move |event| {
        let key = match &event {
            ReconcileEvent::Claimed { key, .. }
            | ReconcileEvent::Published { key, .. }
            | ReconcileEvent::Failed { key, .. }
            | ReconcileEvent::Lost { key } => key_id(key),
            _ => String::new(),
        };
        recorded.lock().push(format!("{}:{key}", event.kind()));
    });
    options.clock = flower.clock().clone();
    let pool = tokio::spawn(reconcile(flower.client(), options, |input: Input, work: Work, _stop| async move {
        Ok(if work.args == "bad" { "not a digest".to_owned() } else { sha(&input.text) })
    }));
    digest_of(&flower, "good-1", "good-1").await;
    digest_of(&flower, "good-2", "good-2").await;
    until(|| events.lock().iter().any(|event| event == "failed:bad")).await;
    stop.cancel();
    pool.await.unwrap().unwrap();
    assert_eq!(
        flower.query("document.get", json!("bad"))["digest"],
        json!({ "status": "pending" })
    );
}

/// A running lease-mode pool over the digest, recording its events (`leased(client, options)`).
struct Leased {
    events: Arc<Mutex<Vec<ReconcileEvent>>>,
    stop: CancellationToken,
    done: Option<JoinHandle<Outcome>>,
}

impl Leased {
    fn events(&self) -> Vec<ReconcileEvent> {
        self.events.lock().clone()
    }
    fn types(&self) -> Vec<&'static str> {
        self.events.lock().iter().map(ReconcileEvent::kind).collect()
    }
    fn claims(&self) -> Vec<u64> {
        self.events
            .lock()
            .iter()
            .filter_map(|event| match event {
                ReconcileEvent::Claimed { attempt, .. } => Some(*attempt),
                _ => None,
            })
            .collect()
    }
    fn limits(&self) -> Vec<String> {
        self.events
            .lock()
            .iter()
            .filter_map(|event| match event {
                ReconcileEvent::Limit { limit, reason } => Some(format!("{limit} {reason}")),
                _ => None,
            })
            .collect()
    }
    async fn stop(&mut self) -> Outcome {
        self.stop.cancel();
        self.done.take().expect("not stopped yet").await.expect("the pool task ran")
    }
}

fn leased<F, Fut>(client: &FakeClient, configure: impl FnOnce(ReconcileOptions) -> ReconcileOptions, compute: F) -> Leased
where
    F: Fn(Input, Work, JobStop) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, WorkError>> + Send + 'static,
{
    let stop = CancellationToken::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = events.clone();
    let mut options = ReconcileOptions::new("digest", stop.clone()).on_event(move |event| recorded.lock().push(event));
    options.lease = true;
    options.clock = client.flower().clock().clone();
    let done = tokio::spawn(reconcile(client.clone(), configure(options), compute));
    Leased {
        events,
        stop,
        done: Some(done),
    }
}

/// A computation that runs until its stop fires, recording why (`endless(reasons)`).
fn endless(reasons: Arc<Mutex<Vec<String>>>) -> impl Fn(Input, Work, JobStop) -> BoxFuture<'static, Result<String, WorkError>> + Send + Sync + 'static {
    move |_input, _work, stop: JobStop| {
        let reasons = reasons.clone();
        async move {
            stop.stopped().await;
            let reason = stop.reason().unwrap_or_default().to_owned();
            reasons.lock().push(reason.clone());
            Err(WorkError::new(reason))
        }
        .boxed()
    }
}

fn owner(owner: &str, concurrency: u64) -> impl FnOnce(ReconcileOptions) -> ReconcileOptions {
    let owner = owner.to_owned();
    move |options| ReconcileOptions {
        owner: Some(owner),
        concurrency: Some(Concurrency::Fixed(concurrency)),
        ..options
    }
}

#[tokio::test(start_paused = true)]
async fn leased_pools_compute_each_key_once_between_them_concurrently() {
    let flower = application();
    let ids: Vec<String> = (0..12).map(|index| format!("doc-{index}")).collect();
    for id in &ids {
        put(&flower, id, id);
    }
    let seen = [Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(Vec::new()))];
    let peak = Peak::default();
    let mut pools: Vec<Leased> = (0..2)
        .map(|index| {
            let (seen, peak) = (seen[index].clone(), peak.clone());
            leased(&flower.client(), owner(&format!("pool-{index}"), 3), move |input: Input, work: Work, _stop| {
                seen.lock().push(work.args);
                let peak = peak.clone();
                async move {
                    peak.during(sleep(5)).await;
                    Ok(sha(&input.text))
                }
            })
        })
        .collect();
    for id in &ids {
        digest_of(&flower, id, id).await;
    }
    for pool in &mut pools {
        pool.stop().await.unwrap();
    }
    let (first, second) = (seen[0].lock().clone(), seen[1].lock().clone());
    let mut all: Vec<String> = first.iter().chain(second.iter()).cloned().collect();
    all.sort();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(all, sorted, "every key was computed exactly once");
    assert!(!first.is_empty() && !second.is_empty(), "both pools took work: {} and {}", first.len(), second.len());
    assert!(peak.peak() > 3, "peak concurrency {}", peak.peak());
    assert_eq!(
        flower.query("digest.stats", Value::Null),
        json!({ "ready": false, "oldestReadyAt": null, "nextAvailableAt": null })
    );
}

#[tokio::test(start_paused = true)]
async fn when_a_pool_goes_silent_another_takes_its_keys_once_their_leases_end() {
    let flower = application();
    for id in ["a", "b"] {
        put(&flower, id, id);
    }
    let dead = Arc::new(AtomicBool::new(false));
    let silent = {
        let dead = dead.clone();
        flower.client().before(move |_| dead.load(Ordering::SeqCst).then(FakeError::fetch_failed))
    };
    let mut first = leased(
        &silent,
        |options| ReconcileOptions {
            lease_ms: Some(60_000),
            ..owner("silent", 2)(options)
        },
        endless(Arc::default()),
    );
    until(|| first.claims().len() == 2).await;
    dead.store(true, Ordering::SeqCst);
    let mut second = leased(&flower.client(), owner("rescuer", 2), |input: Input, _work: Work, _stop| async move {
        Ok(sha(&input.text))
    });
    sleep(20).await;
    assert_eq!(second.events(), [], "leased keys are left alone");
    flower.advance(60_000);
    digest_of(&flower, "a", "a").await;
    digest_of(&flower, "b", "b").await;
    first.stop().await.unwrap();
    second.stop().await.unwrap();
    assert_eq!(second.claims(), [2, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_stopping_pool_hands_its_keys_to_another_at_once() {
    let flower = application();
    for id in ["a", "b"] {
        put(&flower, id, id);
    }
    let mut first = leased(&flower.client(), owner("leaving", 2), endless(Arc::default()));
    until(|| first.claims().len() == 2).await;
    let mut second = leased(&flower.client(), owner("staying", 2), |input: Input, _work: Work, _stop| async move {
        Ok(sha(&input.text))
    });
    sleep(10).await;
    assert_eq!(second.events(), []);
    let now = flower.now();
    first.stop().await.unwrap();
    digest_of(&flower, "a", "a").await;
    digest_of(&flower, "b", "b").await;
    second.stop().await.unwrap();
    assert_eq!(flower.now(), now, "no lease had to run out");
    assert_eq!(first.types(), ["claimed", "claimed"], "shutdown is not a failure");
}

#[tokio::test(start_paused = true)]
async fn an_edit_voids_the_lease_on_the_old_input_and_aborts_its_computation() {
    let flower = application();
    put(&flower, "a", "old");
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let stale = endless(reasons.clone());
    let mut pool = leased(
        &flower.client(),
        |options| ReconcileOptions {
            lease_ms: Some(300),
            ..owner("solo", 2)(options)
        },
        move |input: Input, work: Work, stop| {
            if input.text == "old" {
                stale(input, work, stop)
            } else {
                async move { Ok(sha(&input.text)) }.boxed()
            }
        },
    );
    until(|| pool.claims().len() == 1).await;
    put(&flower, "a", "new");
    digest_of(&flower, "a", "new").await;
    until(|| pool.types().contains(&"lost")).await;
    pool.stop().await.unwrap();
    assert_eq!(*reasons.lock(), ["The lease was lost"]);
    assert_eq!(pool.types(), ["claimed", "claimed", "published", "lost"]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_key_waits_out_its_backoff_for_every_pool_then_succeeds() {
    let flower = application();
    flower.follow_clock();
    put(&flower, "a", "a");
    let times = Arc::new(Mutex::new(Vec::<tokio::time::Instant>::new()));
    let computations = Arc::new(AtomicUsize::new(0));
    let mut pools: Vec<Leased> = ["p1", "p2"]
        .into_iter()
        .map(|name| {
            let (times, computations) = (times.clone(), computations.clone());
            leased(
                &flower.client(),
                |options| ReconcileOptions {
                    owner: Some(name.into()),
                    ..options
                },
                move |input: Input, _work: Work, _stop| {
                    times.lock().push(tokio::time::Instant::now());
                    let first = computations.fetch_add(1, Ordering::SeqCst) == 0;
                    async move {
                        if first {
                            return Err(WorkError::new("hiccup"));
                        }
                        Ok(sha(&input.text))
                    }
                },
            )
        })
        .collect();
    digest_of(&flower, "a", "a").await;
    for pool in &mut pools {
        pool.stop().await.unwrap();
    }
    let events: Vec<ReconcileEvent> = pools.iter().flat_map(Leased::events).collect();
    let mut attempts: Vec<u64> = events
        .iter()
        .filter_map(|event| match event {
            ReconcileEvent::Claimed { attempt, .. } => Some(*attempt),
            _ => None,
        })
        .collect();
    attempts.sort();
    assert_eq!(attempts, [1, 2]);
    assert_eq!(events.iter().filter(|event| event.kind() == "failed").count(), 1);
    assert_eq!(computations.load(Ordering::SeqCst), 2);
    let times = times.lock().clone();
    let waited = times[1] - times[0];
    assert!(waited.as_millis() >= 100, "retried after {waited:?}");
}

#[tokio::test(start_paused = true)]
async fn an_adaptive_leased_pool_computes_more_keys_at_once_while_keys_wait_and_fewer_once_the_process_falls_behind() {
    let flower = application();
    let ids: Vec<String> = (0..16).map(|index| format!("doc-{index}")).collect();
    for id in &ids {
        put(&flower, id, id);
    }
    let hold = Deferred::new();
    let load = Arc::new(Mutex::new(0.0_f64));
    let health = {
        let load = load.clone();
        move || {
            let load = *load.lock();
            Load::new(load, if load < 1.0 { "idle" } else { "event loop busy" })
        }
    };
    let mut pool = {
        let hold = hold.clone();
        leased(
            &flower.client(),
            move |options| ReconcileOptions {
                owner: Some("adaptive".into()),
                concurrency: Some(Concurrency::Adaptive(Adaptive {
                    min: Some(1),
                    max: Some(8),
                    initial: None,
                })),
                adjust_every_ms: Some(5),
                health: Some(Arc::new(health)),
                ..options
            },
            move |input: Input, _work: Work, _stop| {
                let hold = hold.clone();
                async move {
                    hold.wait().await;
                    Ok(sha(&input.text))
                }
            },
        )
    };
    until(|| pool.claims().len() == 8).await;
    *load.lock() = 1.5;
    until(|| pool.limits().iter().any(|limit| limit == "6 event loop busy")).await;
    *load.lock() = 0.0;
    // Full, it claims nothing: its readiness watch shows keys still wait, and it grows back gently.
    until(|| pool.limits().len() == 6).await;
    assert_eq!(pool.claims().len(), 8);
    hold.resolve();
    for id in &ids {
        digest_of(&flower, id, id).await;
    }
    pool.stop().await.unwrap();
    assert_eq!(
        pool.limits(),
        [
            "2 more work is waiting",
            "4 more work is waiting",
            "8 more work is waiting",
            "6 event loop busy",
            "7 more work is waiting",
            "8 more work is waiting"
        ]
    );
    assert_eq!(pool.claims().len(), 16);
}

#[tokio::test(start_paused = true)]
async fn lease_mode_checks_its_options_and_stops_on_a_permanent_claim_failure() {
    let flower = application();
    async fn check(client: &FakeClient, configure: impl FnOnce(ReconcileOptions) -> ReconcileOptions) -> Outcome {
        let options = configure(ReconcileOptions::new("digest", CancellationToken::new()));
        reconcile(client.clone(), options, |_input: Input, _work: Work, _stop| async { Ok(String::new()) }).await
    }
    async fn rejects(client: &FakeClient, configure: impl FnOnce(ReconcileOptions) -> ReconcileOptions, pattern: &str) {
        match check(client, configure).await {
            Err(WorkerError::Invalid(message)) => assert!(message.contains(pattern), "{message:?} lacks {pattern:?}"),
            other => panic!("expected {pattern:?}, got {other:?}"),
        }
    }
    let client = flower.client();
    let adaptive = |min, max| {
        Some(Concurrency::Adaptive(Adaptive {
            min,
            max,
            initial: None,
        }))
    };
    rejects(&client, |o| ReconcileOptions { lease: true, ..o }.with_args("a"), "omit args and shard").await;
    rejects(&client, |o| ReconcileOptions { lease: true, shard: Some((0, 2)), ..o }, "omit args and shard").await;
    rejects(&client, |o| ReconcileOptions { owner: Some("x".into()), ..o }, "need lease: true").await;
    rejects(
        &client,
        |o| ReconcileOptions {
            lease: true,
            lease_ms: Some(100),
            margin_ms: Some(100),
            ..o
        },
        "leaseMs must exceed marginMs",
    )
    .await;
    rejects(
        &client,
        |o| ReconcileOptions {
            lease: true,
            concurrency: Some(Concurrency::Fixed(1_025)),
            ..o
        },
        "at most 1024",
    )
    .await;
    rejects(
        &client,
        |o| ReconcileOptions {
            lease: true,
            concurrency: adaptive(None, Some(1_025)),
            ..o
        },
        "at most 1024",
    )
    .await;
    rejects(
        &client,
        |o| ReconcileOptions {
            lease: true,
            concurrency: adaptive(Some(4), Some(2)),
            ..o
        },
        "1 <= min <= initial <= max",
    )
    .await;
    rejects(
        &client,
        |o| ReconcileOptions {
            concurrency: adaptive(None, Some(4)),
            ..o
        },
        "Adaptive concurrency needs lease: true",
    )
    .await;
    rejects(
        &client,
        |o| ReconcileOptions {
            health: Some(Arc::new(Load::idle)),
            ..o
        },
        "need lease: true",
    )
    .await;
    rejects(
        &client,
        |o| ReconcileOptions {
            concurrency: Some(Concurrency::Fixed(0)),
            ..o
        },
        "positive safe integer",
    )
    .await;
    assert!(client.calls().is_empty(), "invalid options send nothing");
    let denied = flower.client().before(|call| {
        (!call.watch && call.name == "digest.claim")
            .then(|| FakeError::denied(403, "FORBIDDEN", "Authorization denied", ("FORBIDDEN", "Access denied")))
    });
    let error = check(&denied, |o| ReconcileOptions { lease: true, ..o }).await.unwrap_err();
    assert_eq!(error.client().and_then(|error| error.failure_code()), Some("FORBIDDEN"));
}
