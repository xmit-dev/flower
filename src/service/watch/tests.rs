use super::*;
use crate::service::{QueryResult, Validity};
use futures_util::StreamExt;
use hubs::prepare_sync;

fn bundle() -> String {
    r#"
    const records = {kind:'collection',name:'records'};
    var __flowerBundle = {default:{definitions:{
      stable:{name:'stable',kind:'derived',compute:(ctx)=>ctx.get(records,'value') * 2},
      read:{name:'read',kind:'queryMethod',compute:(ctx)=>({value:ctx.get(records,'value'),padding:'x'.repeat(300)})},
      clock:{name:'clock',kind:'queryMethod',compute:(ctx)=>ctx.now()},
      due:{name:'due',kind:'queryMethod',compute:(ctx)=>{
        const now=ctx.clock(), at=ctx.get(records,'deadline');
        if(at===null) return null;
        ctx.changesAt(at);
        return now>=at;
      }},
      schedule:{name:'schedule',kind:'mutationMethod',compute:(ctx,at)=>{ctx.set(records,'deadline',at);return at;}},
      write:{name:'write',kind:'mutationMethod',compute:(ctx,args)=>{ctx.set(records,'value',args);return args;}},
      fail:{name:'fail',kind:'queryMethod',compute:()=>{throw new Error('query failure');}}
    },http:{
      read:{name:'read',kind:'query'},clock:{name:'clock',kind:'query'},due:{name:'due',kind:'query'},
      schedule:{name:'schedule',kind:'mutation'},
      write:{name:'write',kind:'mutation'},fail:{name:'fail',kind:'query'}
    }}};
    "#.into()
}

async fn fixture() -> (tempfile::TempDir, Arc<App>) {
    crate::service::tests::application(bundle()).await
}

fn query_result(value: Value, revision: u64) -> QueryResult {
    QueryResult {
        value,
        revision,
        validity: Validity::Stable,
        certificate: None,
    }
}

async fn event_text(event: Bytes) -> String {
    String::from_utf8(event.to_vec()).unwrap()
}

fn payload(text: &str) -> Value {
    serde_json::from_str(
        text.lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn downstream_bytes_retain_the_frame_budget_after_disconnect() {
    let pool = admission::Pool::new([1, 1], [1, 1], [1024, 1024], 1);
    let mut frame = prepare_sync(None, query_result(json!({"n":1}), 1)).unwrap();
    frame.retained = Some(pool.retain(admission::Class::User, 512).unwrap());
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(wire(Arc::new(frame), None).unwrap())
        .await
        .ok()
        .unwrap();
    let (_terminal, finished) = oneshot::channel();
    let (_draining, draining) = tokio::sync::watch::channel(false);
    let mut stream = Box::pin(events(receiver, finished, draining));
    let bytes = stream.next().await.unwrap().unwrap();
    drop(stream);
    drop(sender);
    assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 512);
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 512);
    drop(clone);
    assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
}

#[tokio::test]
async fn diff_events_have_contiguous_sequences_and_snapshot_fallback() {
    let first = prepare_sync(
        None,
        query_result(json!({"unchanged":"x".repeat(300),"n":1}), 10),
    )
    .unwrap();
    let text = event_text(first.bytes_after(None).unwrap()).await;
    assert!(text.contains("event: snapshot\n"));
    assert!(text.contains("id: 0\n"));
    let same = prepare_sync(
        Some(&first),
        query_result(json!({"unchanged":"x".repeat(300),"n":1}), 12),
    )
    .unwrap();
    assert!(same.bytes_after(Some(first.sequence)).is_none());
    let changed = prepare_sync(
        Some(&same),
        query_result(json!({"unchanged":"x".repeat(300),"n":2}), 12),
    )
    .unwrap();
    let text = event_text(changed.bytes_after(Some(same.sequence)).unwrap()).await;
    assert!(text.contains("event: patch\n"));
    assert_eq!(
        payload(&text),
        json!({"sequence":1,"baseSequence":0,"revision":12,
        "patch":[{"op":"replace","path":"/n","value":2}]})
    );
    let snapshot = prepare_sync(Some(&changed), query_result(json!(true), 13)).unwrap();
    let text = event_text(snapshot.bytes_after(Some(changed.sequence)).unwrap()).await;
    assert!(text.contains("event: snapshot\n"));
    assert_eq!(
        payload(&text),
        json!({"sequence":2,"revision":13,"value":true})
    );
}

#[tokio::test]
async fn initial_errors_are_json() {
    let (_directory, app) = fixture().await;
    for (name, status, code) in [
        ("missing", StatusCode::NOT_FOUND, "METHOD_NOT_FOUND"),
        (
            "write",
            StatusCode::UNPROCESSABLE_ENTITY,
            "METHOD_KIND_MISMATCH",
        ),
        (
            "fail",
            StatusCode::UNPROCESSABLE_ENTITY,
            "EVALUATION_FAILED",
        ),
    ] {
        let error = watch(State(app.clone()), Json(json!({"name":name})))
            .await
            .unwrap_err();
        assert_eq!((error.status, error.code), (status, code));
    }
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn watches_are_not_rejected_at_an_arbitrary_connection_count() {
    let (_directory, app) = fixture().await;
    let mut bodies = Vec::new();
    for _ in 0..65 {
        let response = watch(State(app.clone()), Json(json!({"name":"read"})))
            .await
            .unwrap();
        let mut body = response.into_body().into_data_stream();
        let initial = body.next().await.unwrap().unwrap();
        assert!(
            std::str::from_utf8(&initial)
                .unwrap()
                .contains("event: snapshot")
        );
        bodies.push(body);
    }
    drop(bodies);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscription_observes_commits_and_disconnect_releases_producer() {
    let (_directory, app) = fixture().await;
    let owners = Arc::strong_count(&app);
    let response = watch(State(app.clone()), Json(json!({"name":"read"})))
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(response.headers()["cache-control"], "no-cache");
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    let mut body = response.into_body().into_data_stream();
    let initial = body.next().await.unwrap().unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&initial).unwrap())["value"]["value"],
        6
    );
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"write","args":7,"requestId":"watch-update"}),
        false,
    )
    .await
    .unwrap();
    let update = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let update = std::str::from_utf8(&update).unwrap();
    assert!(update.contains("event: patch"));
    assert_eq!(
        payload(update)["patch"],
        json!([{"op":"replace","path":"/value","value":7}])
    );
    drop(body);
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&app) > owners {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn clock_updates_can_keep_revision_and_redeployment_closes_with_error() {
    let (_directory, app) = fixture().await;
    let response = watch(State(app.clone()), Json(json!({"name":"clock"})))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let first = body.next().await.unwrap().unwrap();
    let first = payload(std::str::from_utf8(&first).unwrap());
    let next = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let next = payload(std::str::from_utf8(&next).unwrap());
    assert_eq!(next["sequence"], 1);
    assert_eq!(next["revision"], first["revision"]);
    assert!(next["value"].as_u64().unwrap() > first["value"].as_u64().unwrap());
    let javascript = bundle().replace("clock:{name:'clock',kind:'query'},", "");
    let _ = super::super::commit_method(
        app.clone(),
        json!({"requestId":"hide-clock","bundle":{
            "hash":crate::evaluator::hash(javascript.as_bytes()),"javascript":javascript
        }}),
        true,
    )
    .await
    .unwrap();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = std::str::from_utf8(&chunk).unwrap();
        if text.contains("event: error") {
            assert_eq!(payload(text)["error"]["code"], "METHOD_NOT_FOUND");
            assert_eq!(payload(text)["error"]["status"], 404);
            break;
        }
    }
    assert!(body.next().await.is_none());
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn slow_consumer_has_a_bounded_terminal_lane_after_the_initial_snapshot() {
    let (sender, receiver) = mpsc::channel(1);
    let frame = Arc::new(prepare_sync(None, query_result(json!("initial"), 1)).unwrap());
    sender
        .send(wire(frame.clone(), None).unwrap())
        .await
        .ok()
        .unwrap();
    let (terminal, terminal_receiver) = oneshot::channel();
    let error = reserve(&sender, Duration::from_millis(10))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, "WATCH_SLOW_CONSUMER");
    terminal.send(error_event(error)).unwrap();
    drop(sender);
    let (_draining, draining) = tokio::sync::watch::channel(false);
    let body = events(receiver, terminal_receiver, draining);
    futures_util::pin_mut!(body);
    assert!(
        event_text(body.next().await.unwrap().unwrap())
            .await
            .contains("event: snapshot")
    );
    assert!(
        event_text(body.next().await.unwrap().unwrap())
            .await
            .contains("WATCH_SLOW_CONSUMER")
    );
    assert!(body.next().await.is_none());
}

#[test]
fn sequence_exhaustion_is_a_terminal_error_instead_of_an_unsafe_json_number() {
    for sequence in [9_007_199_254_740_991, u64::MAX] {
        let mut previous = prepare_sync(None, query_result(json!(0), 1)).unwrap();
        previous.sequence = sequence;
        let error = prepare_sync(Some(&previous), query_result(json!(1), 2))
            .err()
            .unwrap();
        assert_eq!(error.code, "WATCH_SEQUENCE_EXHAUSTED");
    }
}

#[tokio::test]
async fn stable_clock_ticks_do_not_acquire_evaluation_permits_and_shutdown_is_terminal() {
    let (_directory, app) = fixture().await;
    let response = watch(State(app.clone()), Json(json!({"name":"read"})))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let _initial = body.next().await.unwrap().unwrap();
    let held = app
        .query_evaluations
        .clone()
        .acquire_many_owned(4)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    app.consensus.shutdown().await.unwrap();
    let chunk = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&chunk)
            .unwrap()
            .contains("event: error")
    );
    assert!(body.next().await.is_none());
    drop(held);
}

#[tokio::test]
async fn commits_between_subscription_and_producer_start_are_not_lost() {
    let (_directory, app) = fixture().await;
    let everything = app.watch_hubs.everything(&app);
    let progress = app.consensus.progress();
    let input = json!({"name":"read"});
    let subscriber = app.watch_hubs.subscriber();
    let Refreshed {
        hub,
        frame,
        wake,
        access,
        ..
    } = refresh(&app, &input, None, Some(Instant::now()), Some(&subscriber))
        .await
        .unwrap();
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"write","args":8,"requestId":"between"}),
        false,
    )
    .await
    .unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    let producer_app = app.clone();
    let producer = tokio::spawn(async move {
        let watched = Watched {
            sequence: frame.sequence,
            revision: frame.query.revision,
            access,
            wake,
        };
        let subscribed = (subscriber, everything, progress);
        produce(producer_app, input, hub, watched, subscribed, &sender).await
    });
    let update = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        payload(&event_text(update.bytes).await)["patch"],
        json!([{"op":"replace","path":"/value","value":8}])
    );
    drop(receiver);
    assert!(producer.await.unwrap().is_ok());
    app.consensus.shutdown().await.unwrap();
}

async fn queued_producer(
    app: &Arc<App>,
) -> (
    Arc<Hub>,
    mpsc::Receiver<Wire>,
    tokio::task::JoinHandle<Result<(), ApiError>>,
) {
    let input = json!({"name":"read"});
    let everything = app.watch_hubs.everything(app);
    let progress = app.consensus.progress();
    let subscriber = app.watch_hubs.subscriber();
    let Refreshed {
        hub,
        frame,
        wake,
        access,
        ..
    } = refresh(app, &input, None, Some(Instant::now()), Some(&subscriber))
        .await
        .unwrap();
    let watched = Watched {
        sequence: frame.sequence,
        revision: frame.query.revision,
        access,
        wake,
    };
    let (sender, receiver) = mpsc::channel(1);
    sender.try_send(wire(frame, None).unwrap()).ok().unwrap();
    let producer_app = app.clone();
    let producer_hub = hub.clone();
    let producer = tokio::spawn(async move {
        let subscribed = (subscriber, everything, progress);
        produce(
            producer_app,
            input,
            producer_hub,
            watched,
            subscribed,
            &sender,
        )
        .await
    });
    (hub, receiver, producer)
}

async fn wait_for_evaluations(hub: &Hub, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while hub.evaluations().await < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn write_value(app: &Arc<App>, value: u64) {
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"write","args":value,"requestId":format!("backpressure-{value}")}),
        false,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn backpressure_batches_to_the_latest_value_and_resumes_contiguous_patches() {
    for shared in [false, true] {
        let (_directory, app) = fixture().await;
        let (hub, mut receiver, producer) = queued_producer(&app).await;
        write_value(&app, 7).await;
        // The initial snapshot fills the queue, so the next frame must wait.
        wait_for_evaluations(&hub, 2).await;
        let input = json!({"name":"read"});
        let blocked = refresh(&app, &input, Some(&hub.scope), None, None)
            .await
            .unwrap()
            .frame;
        assert_eq!(Arc::strong_count(&blocked), 2, "no unsent frame retained");
        let abandoned = Arc::downgrade(&blocked);
        drop(blocked);
        for value in [8, 9] {
            write_value(&app, value).await;
            if shared {
                // Another subscriber advances the producer while this one waits.
                refresh(&app, &input, Some(&hub.scope), None, None)
                    .await
                    .unwrap();
            }
        }
        if shared {
            assert!(abandoned.upgrade().is_none());
        } else {
            assert_eq!(hub.evaluations().await, 2, "no evaluation while blocked");
        }
        let initial = receiver.recv().await.unwrap();
        assert_eq!(
            payload(&event_text(initial.bytes).await)["value"]["value"],
            6
        );
        let latest = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let latest = event_text(latest.bytes).await;
        assert!(
            latest.contains("event: snapshot"),
            "skipped patch base resets"
        );
        let latest = payload(&latest);
        assert_eq!(
            latest["value"]["value"], 9,
            "stale pending update is discarded"
        );
        assert_eq!(latest["sequence"], if shared { 3 } else { 2 });
        assert!(
            receiver.try_recv().is_err(),
            "one event catches up the batch"
        );

        write_value(&app, 10).await;
        let next = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let next = event_text(next.bytes).await;
        assert!(next.contains("event: patch"));
        let next = payload(&next);
        assert_eq!(next["baseSequence"], latest["sequence"]);
        assert_eq!(
            next["sequence"].as_u64(),
            latest["sequence"].as_u64().map(|n| n + 1)
        );
        assert_eq!(
            next["patch"],
            json!([{"op":"replace","path":"/value","value":10}])
        );
        drop(receiver);
        assert!(producer.await.unwrap().is_ok());
        app.consensus.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn backpressured_subscriber_rechecks_access_before_sending() {
    let (_directory, app) = fixture().await;
    let (hub, mut receiver, producer) = queued_producer(&app).await;
    write_value(&app, 7).await;
    wait_for_evaluations(&hub, 2).await;
    let javascript = bundle().replace("read:{name:'read',kind:'query'},", "");
    let _ = super::super::commit_method(
        app.clone(),
        json!({"requestId":"hide-backpressured-query","bundle":{
            "hash":crate::evaluator::hash(javascript.as_bytes()),"javascript":javascript
        }}),
        true,
    )
    .await
    .unwrap();
    let initial = receiver.recv().await.unwrap();
    assert_eq!(
        payload(&event_text(initial.bytes).await)["value"]["value"],
        6
    );
    let error = tokio::time::timeout(Duration::from_secs(2), producer)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, "METHOD_NOT_FOUND");
    assert!(
        receiver.recv().await.is_none(),
        "no stale update after revocation"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn identical_subscribers_share_one_value_diff_and_immutable_wire_buffer() {
    let (_directory, app) = fixture().await;
    let input = json!({"name":"read"});
    let Refreshed {
        hub, frame: first, ..
    } = refresh(&app, &input, None, Some(Instant::now()), None)
        .await
        .unwrap();
    assert_eq!(hub.evaluations().await, 1);
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"write","args":13,"requestId":"shared"}),
        false,
    )
    .await
    .unwrap();
    let updates = futures_util::future::join_all(
        (0..24).map(|_| refresh(&app, &input, Some(&hub.scope), None, None)),
    )
    .await;
    let updates: Vec<_> = updates.into_iter().map(Result::unwrap).collect();
    assert_eq!(
        hub.evaluations().await,
        2,
        "one producer encodes the committed change"
    );
    assert_eq!(app.watch_hubs.len(), 1);
    let frame = &updates[0].frame;
    let bytes = frame.bytes_after(Some(first.sequence)).unwrap();
    for Refreshed {
        hub: producer,
        frame: update,
        ..
    } in &updates
    {
        assert!(Arc::ptr_eq(&hub, producer));
        assert!(Arc::ptr_eq(frame, update));
        assert_eq!(
            bytes.as_ptr(),
            update.bytes_after(Some(first.sequence)).unwrap().as_ptr()
        );
    }
    // Joining a warm producer is a complete snapshot at its current sequence.
    let joined = refresh(&app, &input, None, Some(Instant::now()), None)
        .await
        .unwrap()
        .frame;
    assert_eq!(joined.sequence, 1);
    assert!(
        std::str::from_utf8(&joined.bytes_after(None).unwrap())
            .unwrap()
            .contains("event: snapshot")
    );
    drop(updates);
    drop(hub);
    assert_eq!(
        app.watch_hubs.len(),
        0,
        "registry holds no producer or value alive"
    );
    app.consensus.shutdown().await.unwrap();
}

#[test]
fn skipped_shared_updates_send_a_reset_and_never_a_patch_with_a_missing_base() {
    let first = prepare_sync(
        None,
        query_result(json!({"padding":"x".repeat(300),"n":0}), 1),
    )
    .unwrap();
    let second = prepare_sync(
        Some(&first),
        query_result(json!({"padding":"x".repeat(300),"n":1}), 2),
    )
    .unwrap();
    let third = prepare_sync(
        Some(&second),
        query_result(json!({"padding":"x".repeat(300),"n":2}), 3),
    )
    .unwrap();
    assert!(third.patch.is_some());
    let reset = third.bytes_after(Some(first.sequence)).unwrap();
    let reset = std::str::from_utf8(&reset).unwrap();
    assert!(reset.contains("event: snapshot"));
    assert_eq!(
        payload(reset),
        json!({"sequence":2,"revision":3,"value":{"padding":"x".repeat(300),"n":2}})
    );
    let patch = third.bytes_after(Some(second.sequence)).unwrap();
    assert!(
        std::str::from_utf8(&patch)
            .unwrap()
            .contains("event: patch")
    );
    assert_eq!(
        payload(std::str::from_utf8(&patch).unwrap())["baseSequence"],
        1
    );
}

fn authorized_bundle() -> String {
    bundle()
        .replace("stable:{name:", "authorize:{name:'authorize',kind:'queryMethod',compute:(ctx,req)=>{const c=req.credentials;if(!c || c.expires<=ctx.now())return null;return {subject:c.subject,claims:{role:c.role}};}},stable:{name:")
        .replace("padding:'x'.repeat(300)", "padding:'x'.repeat(300),principal:ctx.principal()")
        .replace("},http:{", "},authorize:{name:'authorize'},http:{")
}

#[tokio::test]
async fn shared_credentials_expire_independently_and_full_claims_isolate_hubs() {
    let (_directory, app) = crate::service::tests::application(authorized_bundle()).await;
    let now = app
        .clock
        .sample(&app.consensus.read_query().await.unwrap())
        .unwrap();
    let expired_soon =
        json!({"name":"read","credentials":{"subject":"alice","role":"reader","expires":now+1000}});
    let mut long_lived = expired_soon.clone();
    long_lived["credentials"]["expires"] = json!(now + 60_000);
    let first = watch(State(app.clone()), Json(expired_soon)).await.unwrap();
    let second = watch(State(app.clone()), Json(long_lived.clone()))
        .await
        .unwrap();
    let mut first = first.into_body().into_data_stream();
    let mut second = second.into_body().into_data_stream();
    let _ = first.next().await.unwrap().unwrap();
    let _ = second.next().await.unwrap().unwrap();
    assert_eq!(
        app.watch_hubs.len(),
        1,
        "credentials differ but admitted principals agree"
    );
    let mut different_claims = long_lived.clone();
    different_claims["credentials"]["role"] = json!("writer");
    let separate = watch(State(app.clone()), Json(different_claims))
        .await
        .unwrap();
    let mut separate = separate.into_body().into_data_stream();
    let value = separate.next().await.unwrap().unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&value).unwrap())["value"]["principal"]["claims"]["role"],
        "writer"
    );
    assert_eq!(
        app.watch_hubs.len(),
        2,
        "same subject with different claims must never share"
    );
    let terminal = tokio::time::timeout(Duration::from_secs(3), first.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&terminal).unwrap())["error"]["code"],
        "FORBIDDEN"
    );
    assert!(first.next().await.is_none());
    let mut write = long_lived;
    write["name"] = json!("write");
    write["args"] = json!(21);
    write["requestId"] = json!("other-credential-survives");
    let _ = super::super::commit_method(app.clone(), write, false)
        .await
        .unwrap();
    let update = tokio::time::timeout(Duration::from_secs(2), second.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&update)
            .unwrap()
            .contains("event: patch")
    );
    drop(first);
    drop(second);
    drop(separate);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn declared_changes_wake_the_watch_exactly_then_and_not_before() {
    let (_directory, app) = fixture().await;
    let now = app
        .clock
        .sample(&app.consensus.read_query().await.unwrap())
        .unwrap();
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"schedule","args":now + 600,"requestId":"deadline"}),
        false,
    )
    .await
    .unwrap();
    let input = json!({"name":"due"});
    let response = watch(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let started = Instant::now();
    let mut body = response.into_body().into_data_stream();
    let first = body.next().await.unwrap().unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&first).unwrap())["value"],
        false
    );
    let Refreshed { hub, wake, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    assert!(wake.is_some(), "the declared time is the only wake-up");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        hub.evaluations().await,
        1,
        "no evaluation before the declared time"
    );
    let next = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(payload(std::str::from_utf8(&next).unwrap())["value"], true);
    assert!(
        elapsed >= Duration::from_millis(550),
        "woke after {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(900),
        "woke after {elapsed:?}"
    );
    assert_eq!(hub.evaluations().await, 2);
    drop(body);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_authorized_watches_keep_stable_results_without_reevaluating() {
    let (_directory, app) = crate::service::tests::application(authorized_bundle()).await;
    let now = app
        .clock
        .sample(&app.consensus.read_query().await.unwrap())
        .unwrap();
    let input = json!({"name":"read","credentials":{"subject":"alice","role":"reader","expires":now+60_000}});
    let response = watch(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let _ = body.next().await.unwrap().unwrap();
    // This authorize reads ctx.now(), so access is polled; the result is not.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let Refreshed { hub, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    assert_eq!(hub.evaluations().await, 1);
    drop(body);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn declared_credential_expiry_is_the_access_deadline() {
    let javascript = authorized_bundle().replace(
        "if(!c || c.expires<=ctx.now())return null;",
        "const now=ctx.clock();if(!c || c.expires<=now)return null;ctx.changesAt(c.expires);",
    );
    let (_directory, app) = crate::service::tests::application(javascript).await;
    let state = app.consensus.read_query().await.unwrap();
    let now = app.clock.sample(&state).unwrap();
    let input =
        json!({"name":"read","credentials":{"subject":"alice","role":"reader","expires":now+400}});
    let permit = admission::acquire(&app, admission::Class::User, &input)
        .await
        .unwrap();
    let access = authorization::authorize_watch(&app, &state, &input, &permit)
        .await
        .unwrap()
        .validity;
    assert_eq!(access, Validity::Until(now + 400));
    drop(permit);
    let response = watch(State(app.clone()), Json(input)).await.unwrap();
    let started = Instant::now();
    let mut body = response.into_body().into_data_stream();
    let _ = body.next().await.unwrap().unwrap();
    let terminal = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&terminal).unwrap())["error"]["code"],
        "FORBIDDEN"
    );
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "{:?}",
        started.elapsed()
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn writes_wake_only_the_watches_that_read_them() {
    let (_directory, app) = fixture().await;
    let input = json!({"name":"read"});
    let response = watch(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let _ = body.next().await.unwrap().unwrap();
    let Refreshed { hub, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    let signal = hub.signal();
    let woken = *signal.borrow();
    for deadline in 0..5 {
        let _ = super::super::commit_method(
            app.clone(),
            json!({"name":"schedule","args":deadline,"requestId":format!("unrelated-{deadline}")}),
            false,
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(*signal.borrow(), woken, "writes it never read wake nothing");
    assert_eq!(hub.evaluations().await, 1);
    write_value(&app, 7).await;
    let update = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&update).unwrap())["patch"],
        json!([{"op":"replace","path":"/value","value":7}])
    );
    assert!(*signal.borrow() > woken);
    assert_eq!(hub.evaluations().await, 2);
    drop(body);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn registrations_catch_writes_published_while_their_result_evaluated() {
    use crate::consensus::changes::{Changes, Scope};
    let (_directory, app) = fixture().await;
    let read = super::super::read_query(&app, json!({"name":"read"}))
        .await
        .unwrap();
    let certificate = read.certificate.expect("a certified read");
    let observed = certificate
        .observations()
        .find_map(|observation| match observation {
            crate::evaluator::Observation::Key(key) if key.contains("value") => {
                Some(key.to_owned())
            }
            _ => None,
        })
        .expect("the read record");
    let at = read.revision;
    let changes = |revision, keys: &[&str]| {
        Arc::new(Changes {
            scope: Scope::Root,
            revision,
            keys: Some(keys.iter().map(|key| key.to_string()).collect()),
        })
    };
    let signal = || -> wakes::Signal { Arc::new(notifications::Sender::new(0)) };
    let mut wakes = wakes::Wakes::default();
    wakes.restart(at);
    let unrelated = r#"source:["records","deadline"]"#;
    // Published after the evaluation at `at`, before its registration.
    wakes.publish(changes(at + 1, &[unrelated]));
    let first = signal();
    wakes.register(0, &first, at, Some(&certificate));
    assert_eq!(*first.borrow(), 0, "an unrelated write");
    wakes.publish(changes(at + 2, &[&observed]));
    let late = signal();
    wakes.register(1, &late, at, Some(&certificate));
    assert_eq!((*first.borrow(), *late.borrow()), (at + 2, at + 2));
    // Code and policy writes touch every result.
    wakes.publish(changes(at + 3, &["bundle"]));
    assert_eq!((*first.borrow(), *late.borrow()), (at + 3, at + 3));
    // A result without a certificate wakes on any write of its state.
    let uncertified = signal();
    wakes.register(2, &uncertified, at + 3, None);
    wakes.publish(changes(at + 4, &[unrelated]));
    assert_eq!((*first.borrow(), *uncertified.borrow()), (at + 3, at + 4));
    // Once the log no longer covers a registration, it wakes at once.
    wakes.restart(at + 6);
    let stale = signal();
    wakes.register(3, &stale, at + 5, Some(&certificate));
    assert_eq!(*stale.borrow(), at + 6);
    wakes.remove(0);
    wakes.publish(changes(at + 7, &[&observed]));
    assert_eq!((*first.borrow(), *late.borrow()), (at + 6, at + 7));
    assert_eq!(wakes.watched(), 3);
    app.consensus.shutdown().await.unwrap();
}

/// `cargo test --release --lib reregistering -- --ignored --nocapture`: what registering a hub again costs.
#[test]
#[ignore]
fn reregistering_a_hub_that_read_many_records() {
    use crate::evaluator::DependencyCertificate;
    let keys: Vec<String> = (0..500)
        .map(|index| format!(r#"source:["sessions","{index:016}"]"#))
        .collect();
    let certificate =
        DependencyCertificate::of_records(&keys.iter().map(String::as_str).collect::<Vec<_>>());
    let signals: Vec<wakes::Signal> = (0..20)
        .map(|_| Arc::new(notifications::Sender::new(0)))
        .collect();
    let mut wakes = wakes::Wakes::default();
    wakes.restart(1);
    for (hub, signal) in signals.iter().enumerate() {
        wakes.register(hub as u64, signal, 1, Some(&certificate));
    }
    let started = std::time::Instant::now();
    for _ in 0..500 {
        for (hub, signal) in signals.iter().enumerate() {
            wakes.register(hub as u64, signal, 1, Some(&certificate));
        }
    }
    eprintln!(
        "{:?} per registration of 500 keys",
        started.elapsed() / 10_000
    );
}

#[test]
fn writes_refresh_a_hub_within_its_duty_cycle() {
    let started = tokio::time::Instant::now();
    let ms = Duration::from_millis;
    assert_eq!(hubs::pace(started, ms(20), 10), Some(started + ms(200)));
    assert_eq!(hubs::pace(started, ms(1), 10), Some(started + ms(10)));
    assert_eq!(hubs::pace(started, ms(20), 25), Some(started + ms(80)));
    assert_eq!(
        hubs::pace(started, ms(20), 100),
        None,
        "100% turns pacing off"
    );
    assert_eq!(hubs::pace(started, ms(20), 0), None);
    assert_eq!(hubs::pace(started, Duration::MAX, 10), None, "no overflow");
}

#[test]
fn a_hub_registered_again_watches_exactly_what_its_new_result_read() {
    use crate::consensus::changes::{Changes, Scope};
    use crate::evaluator::DependencyCertificate;
    let key = |name: &str| format!(r#"source:["records","{name}"]"#);
    let certificate = |names: &[&str]| {
        let keys: Vec<String> = names.iter().map(|name| key(name)).collect();
        DependencyCertificate::of_records(&keys.iter().map(String::as_str).collect::<Vec<_>>())
    };
    let changes = |revision, names: &[&str]| {
        Arc::new(Changes {
            scope: Scope::Root,
            revision,
            keys: Some(names.iter().map(|name| key(name)).collect()),
        })
    };
    let signal: wakes::Signal = Arc::new(notifications::Sender::new(0));
    let other: wakes::Signal = Arc::new(notifications::Sender::new(0));
    let mut wakes = wakes::Wakes::default();
    wakes.restart(1);
    wakes.register(7, &signal, 1, Some(&certificate(&["a", "b", "c"])));
    wakes.register(8, &other, 1, Some(&certificate(&["a"])));
    // Its next result read b and d: a and c no longer wake it, b and d do.
    wakes.register(7, &signal, 1, Some(&certificate(&["b", "d"])));
    wakes.publish(changes(2, &["c"]));
    assert_eq!(*signal.borrow(), 0);
    wakes.publish(changes(3, &["a"]));
    assert_eq!(
        (*signal.borrow(), *other.borrow()),
        (0, 3),
        "a still wakes the other hub"
    );
    wakes.publish(changes(4, &["d"]));
    assert_eq!(*signal.borrow(), 4);
    wakes.publish(changes(5, &["b"]));
    assert_eq!(*signal.borrow(), 5);
    // Uncertified, it wakes on anything; certified again, on what it read.
    wakes.register(7, &signal, 5, None);
    wakes.publish(changes(6, &["z"]));
    assert_eq!(*signal.borrow(), 6);
    wakes.register(7, &signal, 6, Some(&certificate(&["a", "e"])));
    wakes.publish(changes(7, &["z", "b", "d"]));
    assert_eq!(*signal.borrow(), 6);
    wakes.publish(changes(8, &["e"]));
    assert_eq!(*signal.borrow(), 8);
    assert_eq!(wakes.watched_keys(), 2, "a (both hubs) and e");
    wakes.remove(7);
    wakes.publish(changes(9, &["a", "e"]));
    assert_eq!((*signal.borrow(), *other.borrow()), (8, 9));
    assert_eq!(wakes.watched_keys(), 1);
    wakes.remove(8);
    assert_eq!((wakes.watched(), wakes.watched_keys()), (0, 0));
}

#[tokio::test]
async fn revoking_what_authorization_read_ends_a_watch_whose_result_never_read_it() {
    let javascript = authorized_bundle()
        .replace(
            "if(!c || c.expires<=ctx.now())return null;",
            "if(!c || ctx.get(records,'revoked')===c.subject)return null;",
        )
        .replace(
            "write:{name:'write',kind:'mutationMethod'",
            "revoke:{name:'revoke',kind:'mutationMethod',compute:(ctx,s)=>{ctx.set(records,'revoked',s);return s;}},write:{name:'write',kind:'mutationMethod'",
        )
        .replace(
            "write:{name:'write',kind:'mutation'}",
            "write:{name:'write',kind:'mutation'},revoke:{name:'revoke',kind:'mutation'}",
        );
    let (_directory, app) = crate::service::tests::application(javascript).await;
    let input = json!({"name":"read","credentials":{"subject":"alice","role":"reader"}});
    let response = watch(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let _ = body.next().await.unwrap().unwrap();
    let Refreshed { hub, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    let bob = json!({"subject":"bob","role":"writer"});
    let commit = |name: &str, args: Value, id: &str| {
        super::super::commit_method(
            app.clone(),
            json!({"name":name,"args":args,"credentials":bob,"requestId":id}),
            false,
        )
    };
    let _ = commit("schedule", json!(1), "unrelated").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(hub.evaluations().await, 1, "a write neither read");
    // Access read this record, so the watch checks it again, and keeps going.
    let _ = commit("revoke", json!("carol"), "someone-else")
        .await
        .unwrap();
    wait_for_evaluations(&hub, 2).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), body.next())
            .await
            .is_err()
    );
    let _ = commit("revoke", json!("alice"), "alice").await.unwrap();
    let terminal = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        payload(std::str::from_utf8(&terminal).unwrap())["error"]["code"],
        "FORBIDDEN"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn open_watches_hold_their_own_budget_and_never_the_requests() {
    let (_directory, app) = fixture().await;
    let response = watch(State(app.clone()), Json(json!({"name":"read"})))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let _ = body.next().await.unwrap().unwrap();
    let requests = || app.admission.metrics()["classes"][0]["retainedInputBytes"].clone();
    let watches = || app.watch_hubs.metrics()["retainedBytes"].as_u64().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while requests() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Its input, its hub and one copy of its current value.
    let held = watches();
    assert!(held > 1024 && held < 4096, "{held}");
    drop(body);
    tokio::time::timeout(Duration::from_secs(2), async {
        while watches() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn draining_ends_open_watches_with_a_reconnectable_error() {
    let (_directory, app) = fixture().await;
    let response = watch(State(app.clone()), Json(json!({"name":"read"})))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let first = body.next().await.unwrap().unwrap();
    assert!(
        std::str::from_utf8(&first)
            .unwrap()
            .contains("event: snapshot")
    );
    app.consensus.drain();
    let last = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let last = std::str::from_utf8(&last).unwrap();
    assert!(last.contains("event: error"), "{last}");
    assert_eq!(payload(last)["error"]["code"], "UNAVAILABLE");
    assert!(payload(last)["error"]["status"].as_u64() >= Some(500));
    // The response ends, so a graceful HTTP drain need not wait for it.
    let end = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .unwrap();
    assert!(end.is_none());
    // A watch opened while draining ends at once too.
    let late = watch(State(app.clone()), Json(json!({"name":"read"})))
        .await
        .unwrap();
    let mut late = late.into_body().into_data_stream();
    late.next().await.unwrap().unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), late.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&error)
            .unwrap()
            .contains("event: error")
    );
    app.consensus.shutdown().await.unwrap();
}

/// An application whose write-woken refreshes share a budget of `percent`
/// of one core, apart from every other test's.
async fn budgeted(percent: usize) -> (tempfile::TempDir, Arc<App>) {
    crate::service::tests::application_with_watches(
        bundle(),
        hubs::Registry::with_budget(super::budget::Budget::new(percent)),
    )
    .await
}

async fn next_event(
    body: &mut (impl futures_util::Stream<Item = Result<Bytes, axum::Error>> + Unpin),
    within: Duration,
) -> Option<String> {
    let event = tokio::time::timeout(within, body.next()).await.ok()?;
    Some(String::from_utf8(event.unwrap().unwrap().to_vec()).unwrap())
}

#[tokio::test]
async fn a_write_waits_for_its_hub_to_leave_the_budget_while_first_results_do_not() {
    let (_directory, app) = budgeted(1).await;
    let input = json!({"name":"read"});
    let begun = Instant::now();
    let response = watch(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let first = next_event(&mut body, Duration::from_secs(2)).await.unwrap();
    assert_eq!(payload(&first)["value"]["value"], 6);
    let Refreshed { hub, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    // As if its last write-woken refresh took 40 ms: at 1% of a core, alone,
    // it stays in service for 4 s.
    hub.charge(Duration::from_millis(40));
    // A new subscriber of another hub still gets its first result at once.
    let other = watch(State(app.clone()), Json(json!({"name":"clock"})))
        .await
        .unwrap();
    let mut other = other.into_body().into_data_stream();
    assert!(
        next_event(&mut other, Duration::from_secs(2))
            .await
            .is_some()
    );
    assert!(
        begun.elapsed() < Duration::from_secs(3),
        "{:?}",
        begun.elapsed()
    );
    drop(other);
    write_value(&app, 7).await;
    let written = Instant::now();
    assert!(
        next_event(&mut body, Duration::from_millis(1_000))
            .await
            .is_none(),
        "the write waits for the budget"
    );
    let update = next_event(&mut body, Duration::from_secs(8)).await.unwrap();
    assert_eq!(
        payload(&update)["patch"],
        json!([{"op":"replace","path":"/value","value":7}])
    );
    let waited = written.elapsed();
    assert!(waited >= Duration::from_millis(1_000), "{waited:?}");
    assert_eq!(hub.evaluations().await, 2);
    let refreshes = app.watch_hubs.metrics()["refreshes"].clone();
    assert_eq!(refreshes["budgetPercent"], 1);
    assert!(refreshes["turns"].as_u64() >= Some(1), "{refreshes}");
    drop(body);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_declared_time_refreshes_at_once_while_a_write_waits_for_the_budget() {
    let (_directory, app) = budgeted(1).await;
    let now = app
        .clock
        .sample(&app.consensus.read_query().await.unwrap())
        .unwrap();
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"schedule","args":now + 600,"requestId":"deadline"}),
        false,
    )
    .await
    .unwrap();
    let input = json!({"name":"due"});
    let response = watch(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let first = next_event(&mut body, Duration::from_secs(2)).await.unwrap();
    assert_eq!(payload(&first)["value"], false);
    let Refreshed { hub, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    // 100 ms at 1% of a core: a write would wait 10 s for the budget.
    hub.charge(Duration::from_millis(100));
    let _ = super::super::commit_method(
        app.clone(),
        json!({"name":"schedule","args":now + 1_200,"requestId":"later"}),
        false,
    )
    .await
    .unwrap();
    // The declared time of the result it holds (600 ms) refreshes it at once,
    // which reads the new deadline, and that one refreshes it at 1.2 s.
    let due = next_event(&mut body, Duration::from_secs(5)).await.unwrap();
    assert_eq!(payload(&due)["value"], true);
    let at = app
        .clock
        .sample(&app.consensus.read_query().await.unwrap())
        .unwrap();
    assert!(at >= now + 1_200, "due at {} ms", at - now);
    assert!(at < now + 5_000, "due at {} ms", at - now);
    drop(body);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscribers_of_one_hub_share_one_turn_and_one_evaluation_per_write() {
    let (_directory, app) = budgeted(50).await;
    let input = json!({"name":"read"});
    let mut bodies = Vec::new();
    for _ in 0..3 {
        let response = watch(State(app.clone()), Json(input.clone()))
            .await
            .unwrap();
        let mut body = response.into_body().into_data_stream();
        next_event(&mut body, Duration::from_secs(2)).await.unwrap();
        bodies.push(body);
    }
    let Refreshed { hub, .. } = refresh(&app, &input, None, None, None).await.unwrap();
    assert_eq!(hub.evaluations().await, 1);
    // Unrelated commits keep moving the revision, so that a subscriber that
    // came second would find the first one's result older than its own
    // snapshot.
    let busy = tokio::spawn({
        let app = app.clone();
        async move {
            for deadline in 0.. {
                let committed = super::super::commit_method(
                    app.clone(),
                    json!({"name":"schedule","args":deadline,"requestId":format!("busy-{deadline}")}),
                    false,
                )
                .await;
                if committed.is_err() {
                    break;
                }
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    write_value(&app, 7).await;
    for body in &mut bodies {
        let update = next_event(body, Duration::from_secs(5)).await.unwrap();
        assert_eq!(
            payload(&update)["patch"],
            json!([{"op":"replace","path":"/value","value":7}])
        );
    }
    assert_eq!(hub.evaluations().await, 2, "one evaluation for the write");
    busy.abort();
    drop(bodies);
    app.consensus.shutdown().await.unwrap();
}
