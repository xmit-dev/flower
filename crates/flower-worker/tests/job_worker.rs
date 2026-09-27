//! Ports of `sdk/job-worker.test.ts`, run on paused tokio time against the in-memory Flower.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

use flower_worker::testing::{FakeError, FakeFlower};
use flower_worker::{
    Adaptive, Claim, ClientError, Clock, Concurrency, Load, QueueWorkerEvent, QueueWorkerOptions, RetryPolicy, WorkError,
    WorkerError, run_queue_worker,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use support::{Deferred, lease_end, sleep, start, until};
use tokio_util::sync::CancellationToken;

type Job = Claim<Value>;

fn workers(ids: &[&str]) -> FakeFlower {
    let flower = FakeFlower::workers(Clock::tokio());
    flower.enqueue(ids);
    flower
}

fn state(flower: &FakeFlower, id: &str) -> (String, u64) {
    let job = flower.job(id);
    (job["state"].as_str().unwrap().to_owned(), job["attempts"].as_u64().unwrap())
}

#[tokio::test(start_paused = true)]
async fn a_worker_drains_the_queue_concurrently_and_completes_every_job_exactly_once() {
    let ids = ["a", "b", "c", "d", "e"];
    let flower = workers(&ids);
    let both = Deferred::new();
    let running = Arc::new(AtomicUsize::new(0));
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            owner: Some("test-worker".into()),
            concurrency: Some(Concurrency::Fixed(2)),
            ..options
        },
        move |claim: Job, _, _| {
            let (both, running) = (both.clone(), running.clone());
            async move {
                if running.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                    both.resolve();
                }
                both.wait().await;
                sleep(1).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(json!({ "done": claim.id, "by": claim.owner }))
            }
        },
    );
    until(|| worker.count("completed") == ids.len()).await;
    worker.stop().await.unwrap();
    for id in ids {
        let job = flower.job(id);
        assert_eq!(
            (job["state"].clone(), job["attempts"].clone(), job["result"].clone()),
            (json!("completed"), json!(1), json!({ "done": id, "by": "test-worker" }))
        );
    }
    assert_eq!(flower.query("jobs.ready", Value::Null), json!(false));
}

#[tokio::test(start_paused = true)]
async fn a_failed_attempt_is_reported_requeued_with_backoff_and_retried_once_it_becomes_ready() {
    let flower = workers(&["flaky"]);
    let worker = start(&flower.client(), |options| options, |claim: Job, _, _| async move {
        if claim.attempt == 1 {
            return Err(WorkError::new("upstream said no"));
        }
        Ok(json!({ "attempt": claim.attempt }))
    });
    until(|| worker.types().contains(&"failed")).await;
    let failed = flower.job("flaky");
    assert_eq!(
        (failed["state"].clone(), failed["attempts"].clone(), failed["error"].clone(), failed["availableAt"].clone()),
        (json!("pending"), json!(1), json!({ "message": "upstream said no" }), json!(flower.now() + 1_000))
    );
    flower.advance(999);
    sleep(20).await;
    assert_eq!(worker.types(), ["claimed", "failed"], "the worker waits out the backoff");
    flower.advance(1);
    until(|| worker.types().contains(&"completed")).await;
    let events = worker.events();
    let failed_event = events.iter().filter(|event| event.kind() != "limit").nth(1).cloned();
    worker.stop().await.unwrap();
    assert_eq!(
        failed_event,
        Some(QueueWorkerEvent::Failed {
            id: "flaky".into(),
            error: "upstream said no".into()
        })
    );
    let done = flower.job("flaky");
    assert_eq!(
        (done["state"].clone(), done["attempts"].clone(), done["result"].clone()),
        (json!("completed"), json!(2), json!({ "attempt": 2 }))
    );
}

#[tokio::test(start_paused = true)]
async fn a_result_flower_cannot_store_fails_the_attempt_with_the_reason_instead_of_going_unreported() {
    let flower = workers(&["odd"]);
    let worker = start(&flower.client(), |options| options, |_: Job, _, _| async { Ok(vec![1.0, f64::NAN]) });
    until(|| worker.types().contains(&"failed")).await;
    let events = worker.events();
    worker.stop().await.unwrap();
    let reason = "The result cannot be stored: Flower values require finite numbers";
    assert!(events.contains(&QueueWorkerEvent::Failed {
        id: "odd".into(),
        error: reason.into()
    }));
    let failed = flower.job("odd");
    assert_eq!(
        (failed["state"].clone(), failed["attempts"].clone(), failed["error"].clone()),
        (json!("pending"), json!(1), json!({ "message": reason }))
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_keeps_a_job_alive_through_many_short_leases() {
    let flower = workers(&["long"]);
    let client = flower.client();
    flower.follow_clock();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            lease_ms: Some(300),
            ..options
        },
        |_: Job, stop, _| async move {
            tokio::select! {
                _ = sleep(900) => Ok("survived"),
                _ = stop.stopped() => Err(WorkError::new(stop.reason().unwrap_or_default())),
            }
        },
    );
    until(|| worker.events().len() == 2).await;
    let events = worker.events.clone();
    worker.stop().await.unwrap();
    let types: Vec<_> = events.lock().iter().filter(|event| event.kind() != "limit").map(|event| event.kind()).collect();
    assert_eq!(types, ["claimed", "completed"]);
    let done = flower.job("long");
    assert_eq!(
        (done["state"].clone(), done["attempts"].clone(), done["result"].clone()),
        (json!("completed"), json!(1), json!("survived"))
    );
    let renewals = client.calls_to("jobs.renew");
    assert!(renewals.len() >= 5, "renewed well past the first lease: {}", renewals.len());
    let owner = renewals[0].args()["leases"][0]["owner"].clone();
    assert_eq!(renewals[0].args(), json!({ "leases": [{ "id": "long", "owner": owner, "token": 1 }], "leaseMs": 300 }));
    assert_eq!(renewals[0].args, format!(r#"{{"leases":[{{"id":"long","owner":{owner},"token":1}}],"leaseMs":300}}"#));
}

#[tokio::test(start_paused = true)]
async fn work_still_running_at_the_end_of_its_lease_is_aborted_and_failed_with_the_reason() {
    let flower = workers(&["slow"]);
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            lease_ms: Some(100),
            renew: false,
            ..options
        },
        |_: Job, stop, _| async move {
            tokio::select! {
                _ = sleep(5_000) => Ok(Value::Null),
                _ = stop.stopped() => Err(WorkError::new("aborted")),
            }
        },
    );
    until(|| worker.types().contains(&"failed")).await;
    worker.stop().await.unwrap();
    let failed = flower.job("slow");
    assert_eq!(
        (failed["state"].clone(), failed["attempts"].clone(), failed["error"].clone()),
        (json!("pending"), json!(1), json!({ "message": "The lease ran out" }))
    );
}

#[tokio::test(start_paused = true)]
async fn a_completion_after_another_worker_took_over_the_expired_lease_is_reported_as_lost() {
    let flower = workers(&["contested"]);
    let (started, release) = (Deferred::new(), Deferred::new());
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            renew: false,
            lease_ms: Some(10_000),
            ..options
        },
        {
            let (started, release) = (started.clone(), release.clone());
            move |_: Job, _, _| {
                let (started, release) = (started.clone(), release.clone());
                async move {
                    started.resolve();
                    release.wait().await;
                    Ok("too late")
                }
            }
        },
    );
    started.wait().await;
    flower.advance(10_000);
    let thief = flower.mutate("jobs.claim", json!({ "owner": "thief" }));
    assert_eq!(thief["attempt"], json!(2));
    release.resolve();
    until(|| worker.types().contains(&"lost")).await;
    let types = worker.types();
    worker.stop().await.unwrap();
    assert_eq!(types, ["claimed", "lost"]);
    assert_eq!(flower.job("contested")["lease"]["owner"], json!("thief"), "the lost completion changed nothing");
    flower.mutate(
        "jobs.complete",
        json!({ "id": thief["id"], "owner": thief["owner"], "token": thief["token"], "result": "rescued" }),
    );
    assert_eq!(flower.job("contested")["result"], json!("rescued"));
}

#[tokio::test(start_paused = true)]
async fn renewal_notices_a_lease_taken_over_after_expiry_and_stops_the_work_early() {
    let flower = workers(&["contested"]);
    let started = Deferred::new();
    let reason = Arc::new(Mutex::new(None::<String>));
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            lease_ms: Some(300),
            ..options
        },
        {
            let (started, reason) = (started.clone(), reason.clone());
            move |_: Job, stop, _| {
                let (started, reason) = (started.clone(), reason.clone());
                async move {
                    started.resolve();
                    let result: Result<Value, WorkError> = lease_end(stop).await;
                    *reason.lock() = result.as_ref().err().map(|error| error.message().to_owned());
                    result
                }
            }
        },
    );
    started.wait().await;
    flower.advance(300);
    assert!(!flower.mutate("jobs.claim", json!({ "owner": "thief" })).is_null());
    until(|| worker.types().contains(&"lost")).await;
    let types = worker.types();
    worker.stop().await.unwrap();
    assert_eq!(types, ["claimed", "lost"]);
    assert_eq!(reason.lock().as_deref(), Some("The lease was lost"));
    assert_eq!(flower.job("contested")["lease"]["owner"], json!("thief"));
}

#[tokio::test(start_paused = true)]
async fn a_completion_whose_reply_is_lost_is_retried_with_the_same_request_id_and_applied_once() {
    let flower = workers(&["a"]);
    let dropped = Arc::new(AtomicUsize::new(0));
    let client = flower.client().after(move |call, _| {
        (call.name == "jobs.complete" && dropped.fetch_add(1, Ordering::SeqCst) == 0).then(FakeError::fetch_failed)
    });
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            retry: RetryPolicy {
                initial_delay_ms: Some(1),
                ..RetryPolicy::default()
            },
            ..options
        },
        |_: Job, _, _| async { Ok("ok") },
    );
    until(|| worker.count("completed") == 1).await;
    let types = worker.types();
    worker.stop().await.unwrap();
    assert_eq!(types, ["claimed", "completed"]);
    let completions = client.calls_to("jobs.complete");
    assert_eq!(completions.len(), 2);
    assert_eq!(completions[0].request_id, completions[1].request_id);
    let done = flower.job("a");
    assert_eq!(
        (done["state"].clone(), done["attempts"].clone(), done["result"].clone()),
        (json!("completed"), json!(1), json!("ok"))
    );
}

#[tokio::test(start_paused = true)]
async fn stopping_lets_the_held_job_finish_and_claims_nothing_new() {
    let flower = workers(&["a", "b"]);
    let client = flower.client();
    let (started, release) = (Deferred::new(), Deferred::new());
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(1)),
            ..options
        },
        {
            let (started, release) = (started.clone(), release.clone());
            move |_: Job, stop, _| {
                let (started, release) = (started.clone(), release.clone());
                async move {
                    started.resolve();
                    release.wait().await;
                    Ok(json!({ "interrupted": stop.is_stopped() }))
                }
            }
        },
    );
    started.wait().await;
    let stopped = worker.stop_later();
    release.resolve();
    stopped.await.unwrap().unwrap();
    assert_eq!((state(&flower, "a").0, state(&flower, "b").0), ("completed".into(), "pending".into()));
    assert_eq!(flower.job("a")["result"], json!({ "interrupted": false }), "stopping does not abort work in progress");
    let names: Vec<String> = client.calls().into_iter().filter(|call| !call.watch).map(|call| call.name).collect();
    assert_eq!(names, ["jobs.claim", "jobs.complete"]);
}

#[tokio::test(start_paused = true)]
async fn a_permanent_claim_error_rejects_the_worker_instead_of_spinning() {
    let flower = workers(&["a"]);
    let client = flower.client();
    let mut options = QueueWorkerOptions::new("jobs", CancellationToken::new());
    options.lease_ms = Some(60_000);
    options.clock = flower.clock().clone();
    options.health = Some(Arc::new(Load::idle));
    let error = run_queue_worker(client.clone(), options, |_: Job, _, _| async { Ok(Value::Null) })
        .await
        .unwrap_err();
    assert_eq!(error.client().and_then(|error| error.failure_code()), Some("LEASE_TOO_LONG"));
    let names: Vec<String> = client.calls().into_iter().filter(|call| !call.watch).map(|call| call.name).collect();
    assert_eq!(names, ["jobs.claim"]);
    assert_eq!(state(&flower, "a").0, "pending");
}

#[tokio::test(start_paused = true)]
async fn a_permanent_claim_error_stops_every_claimer_once_held_jobs_finish() {
    let flower = workers(&["a", "b", "c"]);
    let claims = Arc::new(AtomicUsize::new(0));
    let client = flower.client().before(move |call| {
        (!call.watch && call.name == "jobs.claim" && claims.fetch_add(1, Ordering::SeqCst) + 1 == 2)
            .then(|| FakeError::denied(403, "FORBIDDEN", "Authorization denied", ("FORBIDDEN", "Access revoked")))
    });
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(2)),
            claimers: Some(2),
            ..options
        },
        |claim: Job, _, _| async move {
            sleep(20).await;
            Ok(claim.id)
        },
    );
    let events = worker.events.clone();
    let error = worker.done().await.unwrap_err();
    let error = error.client().expect("a client error");
    assert_eq!((error.status(), error.failure_code()), (403, Some("FORBIDDEN")));
    let types: Vec<_> = events.lock().iter().filter(|event| event.kind() != "limit").map(|event| event.kind()).collect();
    assert_eq!(types, ["claimed", "completed"]);
    let states: Vec<String> = ["a", "b", "c"].iter().map(|id| state(&flower, id).0).collect();
    assert_eq!(states, ["completed", "pending", "pending"]);
}

#[tokio::test(start_paused = true)]
async fn a_job_arriving_at_an_idle_worker_costs_one_claim_not_one_per_claimer() {
    let flower = workers(&[]);
    let client = flower.client();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(4)),
            claimers: Some(4),
            batch: Some(4),
            ..options
        },
        |claim: Job, _, _| async move { Ok(claim.id) },
    );
    for (index, id) in ["a", "b", "c"].into_iter().enumerate() {
        sleep(10).await;
        flower.mutate("jobs.enqueue", json!({ "id": id, "payload": { "id": id } }));
        until(|| worker.count("completed") == index + 1).await;
    }
    worker.stop().await.unwrap();
    let maxes: Vec<Value> = client.calls_to("jobs.claim").iter().map(|call| call.args()["max"].clone()).collect();
    assert_eq!(maxes, [json!(4), json!(4), json!(4)]);
}

#[tokio::test(start_paused = true)]
async fn a_worker_takes_several_jobs_per_claim_and_renews_every_lease_it_holds_in_one_call() {
    let ids = ["a", "b", "c", "d", "e"];
    let flower = workers(&ids);
    let client = flower.client();
    flower.follow_clock();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(8)),
            claimers: Some(1),
            batch: Some(4),
            lease_ms: Some(300),
            ..options
        },
        // The first two outlive their first lease, which only renewals keep.
        |claim: Job, stop, _| async move {
            let ms = if claim.id == "a" || claim.id == "b" { 400 } else { 10 };
            tokio::select! {
                _ = sleep(ms) => Ok(claim.id),
                _ = stop.stopped() => Err(WorkError::new("stopped")),
            }
        },
    );
    until(|| worker.count("completed") == 5).await;
    let claimed = worker.count("claimed");
    worker.stop().await.unwrap();
    assert_eq!(claimed, 5);
    assert_eq!(client.calls_to("jobs.claim")[0].args()["max"], json!(4));
    let renewed: Vec<String> = client
        .calls_to("jobs.renew")
        .iter()
        .map(|call| {
            call.args()["leases"]
                .as_array()
                .unwrap()
                .iter()
                .map(|lease| lease["id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    assert!(renewed.len() >= 2 && renewed.iter().all(|ids| ids == "a,b"), "renewals {renewed:?}");
    for id in ids {
        assert_eq!(state(&flower, id), ("completed".into(), 1));
    }
}

#[tokio::test(start_paused = true)]
async fn with_chain_each_report_claims_the_next_job_so_a_busy_queue_costs_one_claim() {
    let ids = ["a", "b", "c", "d", "e", "f"];
    let flower = workers(&ids);
    let client = flower.client();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(1)),
            chain: true,
            wait: true,
            ..options
        },
        |claim: Job, _, _| async move {
            sleep(2).await;
            Ok(claim.id)
        },
    );
    until(|| worker.count("completed") == ids.len()).await;
    flower.mutate("jobs.enqueue", json!({ "id": "late", "payload": { "id": "late" } }));
    until(|| worker.count("completed") == ids.len() + 1).await;
    worker.stop().await.unwrap();
    let claims: Vec<_> = client
        .calls_to("jobs.claim")
        .into_iter()
        .filter(|call| call.args()["max"] != json!(0))
        .collect();
    assert_eq!(claims.len(), 2, "one to start, one for the job that arrived once the queue ran dry");
    let reports: Vec<Value> = client.calls_to("jobs.complete").iter().map(|call| call.args()["next"].clone()).collect();
    assert_eq!(reports, vec![json!({ "max": 1, "leaseMs": 30_000, "waitMs": 60_000 }); ids.len() + 1]);
    for id in ids.iter().chain(&["late"]) {
        assert_eq!(state(&flower, id), ("completed".into(), 1));
    }
}

#[tokio::test(start_paused = true)]
async fn with_chain_a_stopping_workers_reports_claim_nothing_more() {
    let flower = workers(&["a", "b"]);
    let client = flower.client();
    let finish = Deferred::new();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(1)),
            chain: true,
            ..options
        },
        {
            let finish = finish.clone();
            move |claim: Job, _, _| {
                let finish = finish.clone();
                async move {
                    finish.wait().await;
                    Ok(claim.id)
                }
            }
        },
    );
    until(|| worker.count("claimed") == 1).await;
    let stopped = worker.stop_later();
    finish.resolve();
    stopped.await.unwrap().unwrap();
    let nexts: Vec<Option<Value>> = client.calls_to("jobs.complete").iter().map(|call| call.args().get("next").cloned()).collect();
    assert_eq!(nexts, [None]);
    assert_eq!((state(&flower, "a").0, state(&flower, "b").0), ("completed".into(), "pending".into()));
}

#[tokio::test(start_paused = true)]
async fn a_worker_takes_more_jobs_at_once_while_its_queue_holds_more_than_it_runs() {
    let ids: Vec<String> = (0..120).map(|index| format!("job-{index:03}")).collect();
    let flower = workers(&ids.iter().map(String::as_str).collect::<Vec<_>>());
    let running = Arc::new(AtomicI64::new(0));
    let peak = Arc::new(AtomicI64::new(0));
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::up_to(2, 24)),
            batch: Some(8),
            adjust_every_ms: Some(10),
            ..options
        },
        {
            let (running, peak) = (running.clone(), peak.clone());
            move |claim: Job, _, _| {
                let (running, peak) = (running.clone(), peak.clone());
                async move {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    sleep(20).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    Ok(claim.id)
                }
            }
        },
    );
    until(|| worker.count("completed") == ids.len()).await;
    let limits = worker.limits();
    worker.stop().await.unwrap();
    assert_eq!((limits.first().copied(), limits.last().copied()), (Some(4), Some(24)), "limits {limits:?}");
    let peak = peak.load(Ordering::SeqCst);
    assert!(peak > 2 && peak <= 24, "peak {peak}");
}

#[tokio::test(start_paused = true)]
async fn a_job_whose_provider_pushes_back_stops_the_worker_claiming_for_a_while() {
    let ids = ["a", "b", "c", "d"];
    let flower = workers(&ids);
    let clock = flower.clock().clone();
    let claimed_at = Arc::new(Mutex::new(Vec::<i64>::new()));
    let throttled_at = Arc::new(AtomicI64::new(0));
    let worker = start(
        &flower.client(),
        |options| {
            let (clock, claimed_at) = (clock.clone(), claimed_at.clone());
            QueueWorkerOptions {
                concurrency: Some(Concurrency::Fixed(1)),
                ..options
            }
            .on_event(move |event| {
                if event.kind() == "claimed" {
                    claimed_at.lock().push(clock.now_ms());
                }
            })
        },
        {
            let (clock, throttled_at) = (clock.clone(), throttled_at.clone());
            move |claim: Job, _, control| {
                if claim.id == "b" {
                    throttled_at.store(clock.now_ms(), Ordering::SeqCst);
                    control.throttle(150, "RATE_LIMITED");
                }
                async move { Ok(claim.id) }
            }
        },
    );
    until(|| claimed_at.lock().len() == ids.len()).await;
    worker.stop().await.unwrap();
    let throttled_at = throttled_at.load(Ordering::SeqCst);
    let next = claimed_at.lock().iter().copied().find(|at| *at > throttled_at).unwrap();
    assert!(next - throttled_at >= 140, "claimed again {} ms after the provider pushed back", next - throttled_at);
}

#[tokio::test(start_paused = true)]
async fn a_worker_cut_below_its_long_running_jobs_takes_more_again_once_work_waits_and_the_process_keeps_up() {
    let flower = workers(&["a", "b", "c", "d", "e", "f"]);
    let release = Deferred::new();
    let done = Arc::new(Mutex::new(Vec::<String>::new()));
    let busy = Arc::new(AtomicBool::new(true));
    {
        let busy = busy.clone();
        tokio::spawn(async move {
            sleep(100).await;
            busy.store(false, Ordering::SeqCst);
        });
    }
    let worker = start(
        &flower.client(),
        |options| {
            let busy = busy.clone();
            QueueWorkerOptions {
                concurrency: Some(Concurrency::adaptive(1, 2, 8)),
                claimers: Some(1),
                adjust_every_ms: Some(20),
                health: Some(Arc::new(move || {
                    if busy.load(Ordering::SeqCst) {
                        Load::new(2.0, "event loop 100% busy")
                    } else {
                        Load::idle()
                    }
                })),
                ..options
            }
        },
        {
            let (release, done) = (release.clone(), done.clone());
            move |claim: Job, _, _| {
                let (release, done) = (release.clone(), done.clone());
                async move {
                    // The first two run until the others are done, like watchers or background commands.
                    if claim.id == "a" || claim.id == "b" {
                        release.wait().await;
                    }
                    let count = {
                        let mut done = done.lock();
                        done.push(claim.id.clone());
                        done.len()
                    };
                    if count == 4 {
                        release.resolve();
                    }
                    Ok(claim.id)
                }
            }
        },
    );
    until(|| done.lock().len() == 6).await;
    let events = worker.events();
    worker.stop().await.unwrap();
    let mut first: Vec<String> = done.lock()[..4].to_vec();
    first.sort();
    assert_eq!(first, ["c", "d", "e", "f"]);
    assert!(
        events.iter().any(|event| matches!(event, QueueWorkerEvent::Limit { limit: 1, .. })),
        "the busy process was cut to one job"
    );
}

#[tokio::test(start_paused = true)]
async fn a_claim_flower_could_not_answer_is_waited_out_without_holding_room() {
    let flower = workers(&["a"]);
    let claims = Arc::new(AtomicUsize::new(0));
    let client = flower.client().before(move |call| {
        (!call.watch && call.name == "jobs.claim" && claims.fetch_add(1, Ordering::SeqCst) == 0)
            .then(|| FakeError::http(503, "UNAVAILABLE", "No leader"))
    });
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(1)),
            retry: RetryPolicy {
                attempts: Some(1),
                ..RetryPolicy::default()
            },
            ..options
        },
        |claim: Job, _, _| async move { Ok(claim.id) },
    );
    until(|| worker.count("completed") == 1).await;
    let events = worker.events();
    let types = worker.types();
    worker.stop().await.unwrap();
    assert_eq!(types, ["waiting", "claimed", "completed"]);
    assert_eq!(events[0], QueueWorkerEvent::Waiting { error: "UNAVAILABLE: No leader".into() });
}

#[tokio::test(start_paused = true)]
async fn waiting_workers_line_up_a_new_job_wakes_the_first_in_line_only_and_a_worker_leaves_the_line_when_it_stops() {
    let flower = workers(&[]);
    let (alpha, beta) = (flower.client(), flower.client());
    let line = |owner: &str| {
        let owner = owner.to_owned();
        move |options: QueueWorkerOptions| QueueWorkerOptions {
            owner: Some(owner),
            wait: true,
            ..options
        }
    };
    let first = start(&alpha, line("alpha"), |claim: Job, _, _| async move { Ok(claim.id) });
    until(|| alpha.calls_to("jobs.claim").len() == 1).await;
    let second = start(&beta, line("beta"), |claim: Job, _, _| async move { Ok(claim.id) });
    until(|| beta.calls_to("jobs.claim").len() == 1).await;
    assert_eq!(beta.calls_to("jobs.claim")[0].args, r#"{"owner":"beta","leaseMs":30000,"max":1,"waitMs":60000}"#);
    flower.mutate("jobs.enqueue", json!({ "id": "x", "payload": {} }));
    until(|| first.count("completed") == 1).await;
    sleep(20).await;
    assert_eq!(beta.calls_to("jobs.claim").len(), 1, "the job woke alpha alone");
    assert_eq!(second.count("claimed"), 0);
    first.stop().await.unwrap();
    assert_eq!(
        alpha.calls_to("jobs.claim").last().unwrap().args,
        r#"{"owner":"alpha","max":0,"waitMs":0}"#,
        "alpha gave up its place"
    );
    flower.mutate("jobs.enqueue", json!({ "id": "y", "payload": {} }));
    until(|| second.count("completed") == 1).await;
    second.stop().await.unwrap();
    assert_eq!((flower.job("x")["lease"].clone(), flower.job("y")["state"].clone()), (Value::Null, json!("completed")));
}

#[tokio::test(start_paused = true)]
async fn a_stopping_worker_gives_held_jobs_drain_ms_then_aborts_and_fails_them() {
    let flower = workers(&["server"]);
    let clock = flower.clock().clone();
    let reason = Arc::new(Mutex::new(None::<String>));
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            drain_ms: Some(50),
            ..options
        },
        {
            let reason = reason.clone();
            move |_: Job, stop, _| {
                let reason = reason.clone();
                async move {
                    let result: Result<Value, WorkError> = lease_end(stop).await;
                    *reason.lock() = result.as_ref().err().map(|error| error.message().to_owned());
                    result
                }
            }
        },
    );
    until(|| worker.count("claimed") == 1).await;
    let stopped_at = clock.now_ms();
    let events = worker.events.clone();
    worker.stop().await.unwrap();
    assert!(clock.now_ms() - stopped_at >= 45, "held jobs had their time");
    assert_eq!(reason.lock().as_deref(), Some("The worker stopped before the job finished"));
    let types: Vec<_> = events.lock().iter().filter(|event| event.kind() != "limit").map(|event| event.kind()).collect();
    assert_eq!(types, ["claimed", "failed"]);
    let failed = flower.job("server");
    assert_eq!(
        (failed["state"].clone(), failed["error"].clone()),
        (json!("pending"), json!({ "message": "The worker stopped before the job finished" }))
    );
}

#[tokio::test(start_paused = true)]
async fn with_release_jobs_still_running_when_drain_ms_ends_go_back_to_the_queue_instead_of_failing() {
    let flower = workers(&["server", "short"]);
    let finish = Deferred::new();
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(2)),
            drain_ms: Some(50),
            release: true,
            ..options
        },
        {
            let finish = finish.clone();
            move |claim: Job, stop, _| {
                let finish = finish.clone();
                async move {
                    if claim.id != "short" {
                        return lease_end(stop).await;
                    }
                    finish.wait().await;
                    Ok(claim.id)
                }
            }
        },
    );
    until(|| worker.count("claimed") == 2).await;
    let events = worker.events.clone();
    let stopped = worker.stop_later();
    finish.resolve();
    stopped.await.unwrap().unwrap();
    let mut types: Vec<_> = events.lock().iter().filter(|event| event.kind() != "limit").map(|event| event.kind()).collect();
    types.sort();
    assert_eq!(types, ["claimed", "claimed", "completed", "released"]);
    let released = flower.job("server");
    assert_eq!(
        (released["state"].clone(), released["error"].clone(), released["attempts"].clone(), released["lease"].clone()),
        (json!("pending"), Value::Null, json!(1), Value::Null)
    );
    assert_eq!(state(&flower, "short").0, "completed");
    let signal = CancellationToken::new();
    signal.cancel();
    let mut options = QueueWorkerOptions::new("jobs", signal);
    options.release = true;
    let error = run_queue_worker(flower.client(), options, |_: Job, _, _| async { Ok(Value::Null) })
        .await
        .unwrap_err();
    assert!(matches!(&error, WorkerError::Invalid(message) if message == "release needs drainMs"), "{error}");
}

#[tokio::test(start_paused = true)]
async fn a_stopping_worker_gives_up_on_work_that_ignores_its_abort_so_it_stops_anyway() {
    let flower = workers(&["stuck"]);
    let clock = flower.clock().clone();
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            drain_ms: Some(20),
            ..options
        },
        |_: Job, _, _| std::future::pending::<Result<Value, WorkError>>(),
    );
    until(|| worker.count("claimed") == 1).await;
    let stopped_at = clock.now_ms();
    let events = worker.events.clone();
    worker.stop().await.unwrap();
    let took = clock.now_ms() - stopped_at;
    assert!((4_900..7_000).contains(&took), "stopped after {took} ms");
    let unreported: Vec<_> = events.lock().iter().filter(|event| event.kind() == "unreported").cloned().collect();
    assert_eq!(
        unreported,
        [QueueWorkerEvent::Unreported {
            id: "stuck".into(),
            error: "The job did not stop when its worker did".into()
        }]
    );
    assert_eq!(state(&flower, "stuck").0, "leased", "its lease runs out on its own");
}

#[tokio::test(start_paused = true)]
async fn a_job_that_goes_idle_stops_counting_toward_concurrency_so_long_jobs_cant_keep_out_short_ones() {
    let flower = workers(&["long", "a", "b"]);
    let release = Deferred::new();
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(1)),
            ..options
        },
        {
            let release = release.clone();
            move |claim: Job, _, control| {
                let release = release.clone();
                async move {
                    if claim.id != "long" {
                        return Ok(claim.id);
                    }
                    control.idle();
                    release.wait().await;
                    Ok(claim.id)
                }
            }
        },
    );
    until(|| worker.count("completed") == 2).await;
    let states: Vec<String> = ["long", "a", "b"].iter().map(|id| state(&flower, id).0).collect();
    assert_eq!(states, ["leased", "completed", "completed"]);
    release.resolve();
    until(|| worker.count("completed") == 3).await;
    worker.stop().await.unwrap();
}

// ---- Beyond the TS suite: Rust-specific edges of the same behaviour.

#[tokio::test(start_paused = true)]
async fn a_payload_the_work_cannot_read_fails_the_attempt_with_the_reason() {
    let flower = workers(&["a"]);
    #[derive(serde::Deserialize)]
    struct Wanted {
        #[allow(dead_code)]
        missing: String,
    }
    let worker = start(&flower.client(), |options| options, |_: Claim<Wanted>, _, _| async { Ok(Value::Null) });
    until(|| worker.count("failed") == 1).await;
    worker.stop().await.unwrap();
    assert_eq!(flower.job("a")["error"], json!({ "message": "The payload cannot be read: missing field `missing`" }));
}

#[tokio::test(start_paused = true)]
async fn a_panicking_job_fails_with_the_panic_message_and_the_worker_goes_on() {
    let flower = workers(&["boom", "fine"]);
    let worker = start(
        &flower.client(),
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::Fixed(1)),
            ..options
        },
        |claim: Job, _, _| async move {
            if claim.id == "boom" {
                panic!("the job blew up");
            }
            Ok(claim.id)
        },
    );
    until(|| worker.count("completed") == 1 && worker.count("failed") == 1).await;
    worker.stop().await.unwrap();
    assert_eq!(flower.job("boom")["error"], json!({ "message": "the job blew up" }));
}

#[tokio::test(start_paused = true)]
async fn leases_carry_their_history_through_renewals_and_reports() {
    let flower = workers(&["a"]);
    flower.set_history(json!({ "database": "d".repeat(32), "incarnation": "i".repeat(32) }));
    flower.follow_clock();
    let client = flower.client();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            lease_ms: Some(300),
            ..options
        },
        |_: Job, _, _| async {
            sleep(200).await;
            Ok("kept")
        },
    );
    until(|| worker.count("completed") == 1).await;
    worker.stop().await.unwrap();
    let history = format!(r#""history":{{"database":"{}","incarnation":"{}"}}"#, "d".repeat(32), "i".repeat(32));
    let renew = &client.calls_to("jobs.renew")[0];
    assert!(renew.args.contains(&history), "{}", renew.args);
    let complete = &client.calls_to("jobs.complete")[0];
    assert!(complete.args.contains(&format!(r#""token":1,{history},"result":"kept""#)), "{}", complete.args);
    assert_eq!(state(&flower, "a"), ("completed".into(), 1));
}

#[tokio::test(start_paused = true)]
async fn a_scoped_worker_in_line_sends_its_scope_everywhere() {
    let flower = FakeFlower::new(Clock::tokio());
    flower.add_queue(
        "tools",
        flower_worker::testing::QueueConfig {
            scope_argument: true,
            ..Default::default()
        },
    );
    flower.mutate("tools.enqueue", json!({ "scope": "computer:1", "id": "t1", "payload": { "n": 1 } }));
    flower.mutate("tools.enqueue", json!({ "scope": "computer:2", "id": "t2", "payload": { "n": 2 } }));
    let client = flower.client();
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            queue: "tools".into(),
            scope: Some("computer:1".into()),
            owner: Some("w".into()),
            wait: true,
            chain: true,
            batch: Some(8),
            concurrency: Some(Concurrency::adaptive(8, 8, 64)),
            ..options
        },
        |claim: Job, _, _| async move { Ok(claim.payload) },
    );
    until(|| worker.count("completed") == 1).await;
    worker.stop().await.unwrap();
    let claims: Vec<String> = client.calls_to("tools.claim").into_iter().map(|call| call.args).collect();
    assert_eq!(claims[0], r#"{"owner":"w","leaseMs":30000,"scope":"computer:1","max":8,"waitMs":60000}"#);
    assert_eq!(claims.last().unwrap(), r#"{"owner":"w","max":0,"waitMs":0,"scope":"computer:1"}"#);
    let complete = &client.calls_to("tools.complete")[0];
    assert_eq!(
        complete.args,
        r#"{"id":"t1","owner":"w","token":1,"scope":"computer:1","result":{"n":1},"next":{"max":8,"leaseMs":30000,"waitMs":60000}}"#
    );
    let watches: Vec<String> = client.calls().into_iter().filter(|call| call.watch).map(|call| call.args).collect();
    assert!(watches.iter().all(|args| args == r#"{"scope":"computer:1","owner":"w"}"#), "{watches:?}");
    assert_eq!(flower.query("tools.get", json!({ "scope": "computer:2", "id": "t2" }))["state"], json!("pending"));
}

#[tokio::test(start_paused = true)]
async fn options_are_checked_like_the_sdk_checks_them() {
    let flower = workers(&[]);
    let check = |configure: fn(QueueWorkerOptions) -> QueueWorkerOptions, expected: &'static str| {
        let client = flower.client();
        async move {
            let options = configure(QueueWorkerOptions::new("jobs", CancellationToken::new()));
            let error = run_queue_worker(client, options, |_: Job, _, _| async { Ok(Value::Null) }).await.unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
    };
    check(|o| QueueWorkerOptions { lease_ms: Some(100), margin_ms: Some(100), ..o }, "leaseMs must exceed marginMs").await;
    check(|o| QueueWorkerOptions { batch: Some(0), ..o }, "batch must be a positive safe integer").await;
    check(|o| QueueWorkerOptions { wait_ms: Some(1), ..o }, "waitMs must be a safe integer of at least 2").await;
    check(|o| QueueWorkerOptions { release: true, ..o }, "release needs drainMs").await;
    check(
        |o| QueueWorkerOptions {
            concurrency: Some(Concurrency::Adaptive(Adaptive { min: Some(4), max: Some(2), initial: None })),
            ..o
        },
        "concurrency needs whole numbers with 1 <= min <= initial <= max",
    )
    .await;
    check(|o| QueueWorkerOptions { concurrency: Some(Concurrency::Fixed(0)), ..o }, "concurrency needs whole numbers with 1 <= min <= initial <= max").await;
}

/// Real threads and real time: the lock keeps the accounting right under parallel jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_jobs_on_many_threads_complete_exactly_once() {
    let ids: Vec<String> = (0..400).map(|index| format!("job-{index:03}")).collect();
    let flower = FakeFlower::workers(Clock::system());
    flower.enqueue(&ids.iter().map(String::as_str).collect::<Vec<_>>());
    flower.follow_clock();
    let client = flower.client();
    let ran = Arc::new(Mutex::new(std::collections::HashMap::<String, usize>::new()));
    let worker = start(
        &client,
        |options| QueueWorkerOptions {
            concurrency: Some(Concurrency::adaptive(4, 8, 64)),
            batch: Some(8),
            chain: true,
            wait: true,
            adjust_every_ms: Some(5),
            ..options
        },
        {
            let ran = ran.clone();
            move |claim: Job, _, control| {
                let ran = ran.clone();
                async move {
                    *ran.lock().entry(claim.id.clone()).or_default() += 1;
                    if claim.id.ends_with('7') {
                        control.idle();
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1 + (claim.token % 3))).await;
                    Ok(claim.id)
                }
            }
        },
    );
    let started = std::time::Instant::now();
    while worker.count("completed") < ids.len() {
        assert!(started.elapsed() < std::time::Duration::from_secs(20), "completed {}", worker.count("completed"));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    worker.stop().await.unwrap();
    assert!(ran.lock().values().all(|count| *count == 1));
    for id in &ids {
        assert_eq!(state(&flower, id), ("completed".into(), 1));
    }
}
