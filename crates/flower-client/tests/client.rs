//! Ports of `sdk/client.test.ts` onto the scripted `MockTransport`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use common::*;
use flower_client::testing::{MockReply, MockTransport, Step};
use flower_client::{
    Bundle, CancellationToken, Credentials, DeployOptions, ErrorKind, FlowerAdmin, FlowerClient, FlowerError,
    JavaScriptBundle, MutationOptions, MutationResult, Preparation, QueryResult, Reconnect, RequestOptions, Retry,
    RetryPolicy, SubscribeOptions, Update, backoff,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

fn client(mock: &Arc<MockTransport>) -> FlowerClient {
    FlowerClient::builder("http://db").transport(mock.clone()).build().unwrap()
}

fn failure(status: u16, code: &str, message: &str) -> Step {
    MockReply::failure(status, code, message, json!({})).into()
}

fn failing(status: u16, code: &str, message: &str, extra: Value) -> Step {
    MockReply::failure(status, code, message, extra).into()
}

fn events(text: &str) -> Step {
    MockReply::events(text).into()
}

fn fast(initial: u64) -> Reconnect {
    Reconnect::On {
        initial_delay: ms(initial),
        max_delay: Duration::from_secs(30),
    }
}

fn update(revision: u64, value: Value, reset: bool) -> Update {
    Update { revision, value, reset }
}

fn quick() -> RetryPolicy {
    RetryPolicy::default().initial_delay(ms(1))
}

#[tokio::test]
async fn flower_error_carries_the_methods_structured_failure_from_json_error_bodies() {
    let mock = MockTransport::new([
        failing(422, "EVALUATION_FAILED", "OUT_OF_STOCK: no dough", json!({"failure": {"code": "OUT_OF_STOCK", "message": "no dough", "details": {"left": 0}}})),
        failing(403, "FORBIDDEN", "Authorization denied", json!({"failure": {"code": "UNAUTHENTICATED", "message": "Authentication required"}})),
        failing(422, "TRANSACTION_ABORTED", "participant failed", json!({"failure": {"code": "INSUFFICIENT_FUNDS", "message": "balance too low"}})),
        failure(503, "UNAVAILABLE", "no quorum"),
        failing(422, "EVALUATION_FAILED", "malformed", json!({"failure": {"code": 7, "message": "not a failure"}})),
        MockReply::text(502, "gateway exploded").into(),
        MockReply::json(400, &json!({"error": "plain string"})).into(),
        MockReply::json(409, &json!({"message": "top-level", "code": "TOP"})).into(),
        MockReply::text(503, "").into(),
    ]);
    let client = client(&mock);
    let mut errors = Vec::new();
    for _ in 0..9 {
        errors.push(client.mutate_value("order", &(), MutationOptions::new()).await.unwrap_err());
    }
    let [evaluation, forbidden, aborted, unavailable, malformed, gateway, plain, top, empty] = &errors[..] else { unreachable!() };
    assert_eq!((evaluation.status, evaluation.code.as_str(), evaluation.message.as_str()), (422, "EVALUATION_FAILED", "OUT_OF_STOCK: no dough"));
    let failure = evaluation.failure.as_ref().unwrap();
    assert_eq!((failure.code.as_str(), failure.message.as_str(), &failure.details), ("OUT_OF_STOCK", "no dough", &Some(json!({"left": 0}))));
    assert_eq!((forbidden.status, forbidden.code.as_str(), forbidden.failure_code()), (403, "FORBIDDEN", Some("UNAUTHENTICATED")));
    assert_eq!(forbidden.failure.as_ref().unwrap().details, None);
    assert_eq!((aborted.code.as_str(), aborted.failure_code()), ("TRANSACTION_ABORTED", Some("INSUFFICIENT_FUNDS")));
    assert_eq!((unavailable.status, unavailable.code.as_str(), unavailable.message.as_str()), (503, "UNAVAILABLE", "no quorum"));
    assert!(unavailable.failure.is_none());
    assert!(malformed.failure.is_none());
    assert_eq!((gateway.status, gateway.code.as_str(), gateway.message.as_str()), (502, "HTTP_ERROR", "gateway exploded"));
    assert_eq!((plain.code.as_str(), plain.message.as_str()), ("HTTP_ERROR", "plain string"));
    assert_eq!((top.code.as_str(), top.message.as_str()), ("TOP", "top-level"));
    assert_eq!(empty.message, "Service Unavailable", "empty bodies fall back to statusText");
    assert!(errors.iter().all(|error| error.kind == ErrorKind::Flower));
}

#[tokio::test]
async fn flower_error_carries_failures_from_sse_error_events_and_watch_http_errors() {
    let limit = json!({"code": "LIMIT", "message": "too many toppings", "details": {"max": 3}});
    let mock = MockTransport::new([
        events(&(snapshot(json!(1), 1, 0) + &sse("error", &json!({"error": {"code": "EVALUATION_FAILED", "message": "LIMIT: too many toppings", "status": 422, "failure": limit}})))),
        failing(403, "FORBIDDEN", "Authorization denied", json!({"failure": {"code": "FORBIDDEN", "message": "Access denied"}})),
        events(&sse("error", &json!({"error": {"code": "UNAVAILABLE", "message": "leader lost", "status": 503}}))),
    ]);
    let client = client(&mock);
    let mut watcher = client.watch::<_, Value>("pizza", &(), Default::default());
    assert_eq!(bounded(watcher.next()).await.unwrap().unwrap(), QueryResult { revision: 1, value: json!(1) });
    let error = bounded(watcher.next()).await.unwrap().unwrap_err();
    assert_eq!((error.status, error.code.as_str(), error.message.as_str()), (422, "EVALUATION_FAILED", "LIMIT: too many toppings"));
    assert_eq!(serde_json::to_value(error.failure.as_deref().unwrap()).unwrap(), limit);
    assert!(bounded(watcher.next()).await.is_none());
    let error = bounded(client.watch_deltas("pizza", &(), Default::default()).next()).await.unwrap().unwrap_err();
    assert_eq!((error.status, error.failure_code()), (403, Some("FORBIDDEN")));
    assert_eq!(error.failure.unwrap().message, "Access denied");
    let error = bounded(client.watch::<_, Value>("pizza", &(), Default::default()).next()).await.unwrap().unwrap_err();
    assert_eq!(error.status, 503);
    assert!(error.failure.is_none());
}

#[test]
fn is_transient_retries_transport_trouble_never_answers_or_the_callers_own_abort() {
    let with_failure = |status: u16, code: &str| {
        FlowerError::new(code, status, code).with_failure(Some(flower_client::Failure {
            code: "APP_CODE".into(),
            message: "the method said no".into(),
            details: None,
        }))
    };
    let mut cases: Vec<(FlowerError, bool)> = vec![
        (FlowerError::transport("UND_ERR_SOCKET", "fetch failed"), true),
        (FlowerError::transport("H2_STREAM_ABORTED", "HTTP/2 stream aborted"), true),
        (FlowerError::transport("ERR_HTTP2_GOAWAY_SESSION", "goaway"), true),
        (FlowerError::transport("ERR_HTTP2_STREAM_ERROR", "Stream closed with error code NGHTTP2_REFUSED_STREAM"), true),
        (FlowerError::transport("ECONNREFUSED", "refused"), true),
        (FlowerError::transport("ECONNRESET", "reset"), true),
        (FlowerError::transport("EAI_AGAIN", "dns"), true),
        (FlowerError::transport("ENOTFOUND", "dns"), false),
        (FlowerError::transport("ERR_TLS_CERT_INVALID", "bad certificate"), false),
        (FlowerError::transport("H2_REQUEST_TOO_LARGE", "too large"), false),
        (FlowerError::transport("H2_RESPONSE_TOO_LARGE", "too large"), false),
        (FlowerError::transport("H2_UNSUPPORTED_ENCODING", "gzip"), false),
        (FlowerError::timeout(), true),
        (FlowerError::aborted(), false),
        (FlowerError::decode("bad reply", 200), false),
        (FlowerError::invalid("bad argument"), false),
        (FlowerError::new("Watch stalled", 0, "WATCH_STALLED"), true),
        (FlowerError::new("The subscription ended", 0, "WATCH_ENDED"), true),
        (FlowerError::new("bad frame", 0, "WATCH_PROTOCOL_ERROR"), false),
        (FlowerError::new("gave up", 0, "PARTITION_WAIT_TIMEOUT"), false),
        (with_failure(422, "EVALUATION_FAILED"), false),
        (with_failure(422, "TRANSACTION_ABORTED"), false),
        (with_failure(403, "FORBIDDEN"), false),
        (with_failure(503, "UNAVAILABLE"), false),
    ];
    for status in [408, 425, 429, 500, 502, 503, 504] {
        cases.push((FlowerError::new(format!("HTTP {status}"), status, "HTTP_ERROR"), true));
    }
    for status in [400, 401, 403, 404, 409, 413, 422] {
        cases.push((FlowerError::new(format!("HTTP {status}"), status, "HTTP_ERROR"), false));
    }
    for (error, expected) in cases {
        assert_eq!(error.is_transient(), expected, "{error:?}");
    }
    assert_eq!(FlowerError::timeout().message, "The operation was aborted due to timeout");
}

#[test]
fn backoff_is_jittered_exponential_growth_between_half_and_all_of_its_capped_ceiling() {
    for attempt in 0..40u32 {
        let ceiling = 30_000f64.min(250.0 * 2f64.powi(attempt.min(30) as i32));
        for _ in 0..20 {
            let delay = backoff(attempt, ms(250), ms(30_000)).as_millis() as f64;
            assert!(delay >= (ceiling / 2.0).round() && delay <= ceiling, "{attempt}: {delay}");
        }
    }
    let samples: std::collections::BTreeSet<u128> = (0..50).map(|_| backoff(3, ms(10), ms(40)).as_millis()).collect();
    assert!(samples.iter().all(|delay| (20..=40).contains(delay)) && samples.len() > 1);
    assert_eq!(backoff(1_000, ms(1), ms(1)), ms(1));
}

#[tokio::test]
async fn retries_keep_one_request_id_and_refresh_credentials_on_every_attempt() {
    let token = Arc::new(AtomicU64::new(0));
    let mock = MockTransport::new([
        FlowerError::transport("UND_ERR_SOCKET", "fetch failed").into(),
        failure(502, "BAD_GATEWAY", "BAD_GATEWAY"),
        MockReply::ok(json!(7)).into(),
    ]);
    let counter = token.clone();
    let client = FlowerClient::builder("http://db")
        .transport(mock.clone())
        .credentials(Credentials::from_fn(move || {
            let token = counter.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(json!({ "token": token })) }
        }))
        .build()
        .unwrap();
    let result = client.mutate_value("add", &json!({"by": 1}), MutationOptions::new().retry(quick())).await.unwrap();
    assert_eq!(result, MutationResult { revision: 1, value: json!(7), duplicate: false });
    let requests = mock.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| request.url == "http://db/v1/mutate"));
    let ids: std::collections::BTreeSet<_> = requests.iter().map(|request| request.json["requestId"].to_string()).collect();
    assert_eq!(ids.len(), 1);
    let id = requests[0].json["requestId"].as_str().unwrap();
    assert!(uuid_like(id), "{id}");
    let tokens: Vec<_> = requests.iter().map(|request| request.json["credentials"].clone()).collect();
    assert_eq!(tokens, [json!({"token": 1}), json!({"token": 2}), json!({"token": 3})]);
    assert!(requests.iter().all(|request| request.json["args"] == json!({"by": 1})));
    // The body is JSON.stringify's, key order included.
    assert_eq!(
        std::str::from_utf8(&requests[0].body).unwrap(),
        format!(r#"{{"name":"add","args":{{"by":1}},"requestId":"{id}","credentials":{{"token":1}}}}"#)
    );

    let explicit = MockTransport::new([failure(503, "UNAVAILABLE", "UNAVAILABLE"), MockReply::ok(json!(null)).into()]);
    let client = FlowerClient::builder("http://db").transport(explicit.clone()).retry(quick()).build().unwrap();
    client
        .call::<_, Value>("add", &json!({"by": 1}), MutationOptions::new().request_id("intent-1").expected_revision(4))
        .await
        .unwrap();
    let seen: Vec<_> = explicit
        .requests()
        .iter()
        .map(|request| (request.url.clone(), request.json["requestId"].clone(), request.json["expectedRevision"].clone()))
        .collect();
    assert_eq!(seen, vec![("http://db/v1/call".into(), json!("intent-1"), json!(4)); 2]);
    assert_eq!(
        std::str::from_utf8(&explicit.requests()[0].body).unwrap(),
        r#"{"name":"add","args":{"by":1},"requestId":"intent-1","expectedRevision":4}"#
    );
}

fn uuid_like(id: &str) -> bool {
    let parts: Vec<_> = id.split('-').collect();
    parts.iter().map(|part| part.len()).collect::<Vec<_>>() == [8, 4, 4, 4, 12]
        && id.chars().all(|c| c == '-' || c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

#[tokio::test]
async fn retries_stop_at_answers_failures_non_transient_statuses_and_custom_predicates() {
    let answers = [
        (failing(422, "EVALUATION_FAILED", "SOLD_OUT: none left", json!({"failure": {"code": "SOLD_OUT", "message": "none left"}})), "EVALUATION_FAILED"),
        (failing(403, "FORBIDDEN", "denied", json!({"failure": {"code": "UNAUTHENTICATED", "message": "Authentication required"}})), "FORBIDDEN"),
        (failing(503, "UNAVAILABLE", "participant said no", json!({"failure": {"code": "DOWNSTREAM", "message": "no"}})), "UNAVAILABLE"),
        (failure(409, "REQUEST_ID_REUSED", "requestId was already used for different content"), "REQUEST_ID_REUSED"),
        (failure(400, "INVALID_REQUEST", "INVALID_REQUEST"), "INVALID_REQUEST"),
    ];
    for (answer, code) in answers {
        let mock = MockTransport::new([answer, MockReply::ok(json!(null)).into()]);
        let client = FlowerClient::builder("http://db").transport(mock.clone()).retry(true).build().unwrap();
        assert_eq!(client.mutate_value("order", &(), MutationOptions::new()).await.unwrap_err().code, code);
        assert_eq!(mock.count(), 1, "{code}");
    }
    let busy = MockTransport::new([failure(409, "BUSY", "BUSY"), failure(409, "BUSY", "BUSY"), MockReply::ok(json!("done")).into()]);
    let policy = quick().retryable(|error| error.code == "BUSY");
    assert_eq!(client(&busy).mutate_value("order", &(), MutationOptions::new().retry(policy)).await.unwrap().value, json!("done"));
    assert_eq!(busy.count(), 3);
    let unavailable = MockTransport::new([failure(503, "UNAVAILABLE", "UNAVAILABLE")]);
    let error = client(&unavailable)
        .query_value("read", &(), RequestOptions::new().retry(quick().retryable(|_| false)))
        .await
        .unwrap_err();
    assert_eq!(error.status, 503);
    assert_eq!(unavailable.count(), 1);
}

#[tokio::test]
async fn retry_attempts_until_and_per_attempt_timeouts_bound_the_work() {
    let down = || MockTransport::new((0..20).map(|_| failure(503, "UNAVAILABLE", "UNAVAILABLE")));
    let server = down();
    let error = client(&server).query_value("read", &(), RequestOptions::new().retry(quick().attempts(3))).await.unwrap_err();
    assert_eq!((error.status, server.count()), (503, 3));
    let server = down();
    client(&server)
        .query_value("read", &(), RequestOptions::new().retry(quick().max_delay(ms(1))))
        .await
        .unwrap_err();
    assert_eq!(server.count(), 8, "eight attempts by default");
    let server = down();
    let started = std::time::Instant::now();
    let policy = RetryPolicy::default().initial_delay(ms(1_000)).until(SystemTime::now() + ms(100));
    let error = bounded(client(&server).query_value("read", &(), RequestOptions::new().retry(policy))).await.unwrap_err();
    assert_eq!((error.status, server.count()), (503, 1), "no attempt may start after until");
    assert!(started.elapsed() < ms(500), "until does not sleep toward a retry it cannot make");

    let lost = MockTransport::new([Step::Hang, MockReply::result(3, json!("applied"), true).into()]);
    let result = bounded(client(&lost).mutate_value(
        "pay",
        &json!({"cents": 5}),
        MutationOptions::new().retry(quick().timeout(ms(30))),
    ))
    .await
    .unwrap();
    assert_eq!(result, MutationResult { revision: 3, value: json!("applied"), duplicate: true });
    let requests = lost.requests();
    assert!(requests[0].aborted(), "the timed-out attempt was cancelled");
    assert_eq!(requests[0].json["requestId"], requests[1].json["requestId"]);

    // Without retry, only the transport deadline applies: one timed-out attempt is final.
    let hung = MockTransport::new([Step::Hang]);
    let cancel = CancellationToken::new();
    let pending = tokio::spawn({
        let client = client(&hung);
        let cancel = cancel.clone();
        async move { client.query_value("read", &(), RequestOptions::new().cancel(cancel)).await }
    });
    tokio::time::sleep(ms(20)).await;
    cancel.cancel();
    assert!(bounded(pending).await.unwrap().unwrap_err().is_aborted());
    assert!(hung.requests()[0].aborted());
}

#[tokio::test]
async fn retries_honor_the_callers_cancellation_while_waiting_between_attempts() {
    let mock = MockTransport::new([failure(503, "UNAVAILABLE", "UNAVAILABLE")]);
    let cancel = CancellationToken::new();
    let pending = tokio::spawn({
        let client = client(&mock);
        let options = RequestOptions::new().cancel(cancel.clone()).retry(RetryPolicy::default().initial_delay(ms(60_000)));
        async move { client.query_value("read", &(), options).await }
    });
    tokio::time::sleep(ms(10)).await;
    cancel.cancel();
    let error = bounded(pending).await.unwrap().unwrap_err();
    assert!(error.is_aborted() && !error.is_transient(), "{error:?}");
    assert_eq!(mock.count(), 1);
    let error = client(&mock).query_value("read", &(), RequestOptions::retrying().cancel(cancel)).await.unwrap_err();
    assert!(error.is_aborted());
    assert_eq!(mock.count(), 1, "a cancelled call sends nothing");
}

#[tokio::test]
async fn the_client_retry_default_applies_to_partitions_and_calls_can_opt_out() {
    let mock = MockTransport::new([
        failure(503, "UNAVAILABLE", "UNAVAILABLE"),
        MockReply::ok(json!(1)).into(),
        failure(429, "RATE_LIMITED", "RATE_LIMITED"),
        MockReply::ok(json!(2)).into(),
        failure(503, "UNAVAILABLE", "UNAVAILABLE"),
    ]);
    let client = FlowerClient::builder("http://primary/").transport(mock.clone()).retry(quick()).build().unwrap();
    assert_eq!(client.url(), "http://primary");
    assert_eq!(client.query_value("read", &(), RequestOptions::new()).await.unwrap().value, json!(1));
    assert_eq!(client.partition("west").mutate_value("write", &(), MutationOptions::new()).await.unwrap().value, json!(2));
    let error = client.query_value("read", &(), RequestOptions::new().retry(Retry::Off)).await.unwrap_err();
    assert_eq!(error.status, 503);
    let urls: Vec<_> = mock.requests().iter().map(|request| request.url.clone()).collect();
    assert_eq!(
        urls,
        [
            "http://primary/v1/query",
            "http://primary/v1/query",
            "http://primary/partitions/west/v1/mutate",
            "http://primary/partitions/west/v1/mutate",
            "http://primary/v1/query"
        ]
    );
    let requests = mock.requests();
    assert_eq!(requests[2].json["requestId"], requests[3].json["requestId"]);
    let built = FlowerClient::builder("http://primary").transport(mock.clone()).partition("tenant/🌻").build().unwrap();
    assert_eq!(built.url(), "http://primary/partitions/tenant%2F%F0%9F%8C%BB");
}

#[tokio::test]
async fn subscribe_marks_the_first_value_of_every_connection_as_a_reset() {
    let mock = MockTransport::new([
        events(&(snapshot(json!({"n": 1}), 1, 0) + &patch(json!(2), 2, 1, "/n"))),
        events(&(snapshot(json!({"n": 5}), 5, 0) + &patch(json!(6), 6, 1, "/n"))),
    ]);
    let mut updates = client(&mock).subscribe::<_, Value>("counter", &json!({"shop": "north"}), SubscribeOptions::new().reconnect(fast(1)));
    let values: Vec<_> = take(&mut updates, 4).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(
        values,
        [
            update(1, json!({"n": 1}), true),
            update(2, json!({"n": 2}), false),
            update(5, json!({"n": 5}), true),
            update(6, json!({"n": 6}), false)
        ]
    );
    drop(updates);
    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(request.url, "http://db/v1/watch");
        assert_eq!(request.header("accept"), Some("text/event-stream"));
        assert_eq!(request.header("content-type"), Some("application/json"));
        assert_eq!(request.json, json!({"name": "counter", "args": {"shop": "north"}}));
        assert_eq!(request.lane, flower_client::Lane::Watch);
    }
}

#[tokio::test]
async fn subscribe_reconnects_after_end_of_stream_network_errors_transient_statuses_and_error_events() {
    let mock = MockTransport::new([
        events(&snapshot(json!("first"), 1, 0)),
        FlowerError::transport("UND_ERR_SOCKET", "fetch failed").into(),
        failure(503, "UNAVAILABLE", "leader lost"),
        failure(429, "RATE_LIMITED", "slow down"),
        events(&sse("error", &json!({"error": {"code": "UNAVAILABLE", "message": "shutting down", "status": 503}}))),
        events(&snapshot(json!("second"), 2, 0)),
    ]);
    let options = SubscribeOptions::new().reconnect(Reconnect::On { initial_delay: ms(1), max_delay: ms(2) });
    let mut updates = client(&mock).subscribe::<_, Value>("value", &(), options);
    let values: Vec<_> = take(&mut updates, 2).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(values, [update(1, json!("first"), true), update(2, json!("second"), true)]);
    assert_eq!(mock.count(), 6);
}

#[tokio::test]
async fn subscribe_surfaces_answers_and_protocol_violations_instead_of_reconnecting() {
    type Check = fn(&FlowerError) -> bool;
    let cases: Vec<(Step, Check)> = vec![
        (failure(404, "METHOD_NOT_FOUND", "no such alias"), |e| e.status == 404 && e.code == "METHOD_NOT_FOUND"),
        (failing(403, "FORBIDDEN", "denied", json!({"failure": {"code": "UNAUTHENTICATED", "message": "Authentication required"}})), |e| e.failure_code() == Some("UNAUTHENTICATED")),
        (
            events(&(snapshot(json!(1), 1, 0) + &sse("error", &json!({"error": {"code": "EVALUATION_FAILED", "message": "BAD: no", "status": 422, "failure": {"code": "BAD", "message": "no"}}})))),
            |e| e.status == 422 && e.failure_code() == Some("BAD"),
        ),
        (events(&(snapshot(json!({}), 1, 0) + &patch(json!(1), 2, 1, "/missing"))), |e| e.code == "WATCH_PROTOCOL_ERROR"),
        (events(&(snapshot(json!(1), 5, 0) + &snapshot(json!(2), 4, 1))), |e| e.code == "WATCH_PROTOCOL_ERROR"),
    ];
    for (step, check) in cases {
        let mock = MockTransport::new([step, events(&snapshot(json!("reconnected"), 1, 0))]);
        let mut updates = client(&mock).subscribe::<_, Value>("value", &(), SubscribeOptions::new().reconnect(fast(1)));
        let items = take(&mut updates, 3).await;
        let error = items.into_iter().find_map(Result::err).expect("an error");
        assert!(check(&error), "{error:?}");
        assert_eq!(mock.count(), 1);
        assert!(bounded(updates.next()).await.is_none());
    }
}

#[tokio::test]
async fn subscribe_without_reconnect_ends_at_end_of_stream_and_returns_transient_errors() {
    let ended = MockTransport::new([events(&snapshot(json!(1), 1, 0)), events(&snapshot(json!(2), 1, 0))]);
    let mut updates = client(&ended).subscribe::<_, Value>("value", &(), SubscribeOptions::new().reconnect(Reconnect::Off));
    let values: Vec<_> = take(&mut updates, 5).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(values, [update(1, json!(1), true)]);
    assert_eq!(ended.count(), 1);
    let down = MockTransport::new([failure(503, "UNAVAILABLE", "UNAVAILABLE"), events(&snapshot(json!(2), 1, 0))]);
    let mut updates = client(&down).subscribe::<_, Value>("value", &(), SubscribeOptions::new().reconnect(Reconnect::Off));
    assert_eq!(bounded(updates.next()).await.unwrap().unwrap_err().status, 503);
    assert_eq!(down.count(), 1);
}

#[tokio::test(start_paused = true)]
async fn subscribe_reconnects_silent_streams_and_stalled_error_bodies_after_stall_while_heartbeats_keep_it_alive() {
    let (silent, silent_handle) = MockReply::event_stream(&snapshot(json!("stale"), 1, 0), true);
    let stalled = MockTransport::new([silent.into(), events(&snapshot(json!("fresh"), 2, 0))]);
    let options = SubscribeOptions::new().stall(ms(50)).reconnect(fast(1));
    let mut updates = client(&stalled).subscribe::<_, Value>("value", &(), options.clone());
    let values: Vec<_> = take(&mut updates, 2).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(values, [update(1, json!("stale"), true), update(2, json!("fresh"), true)]);
    assert_eq!(silent_handle.cancelled(), 1);
    assert!(stalled.requests()[0].aborted());

    let (stuck, stuck_handle) = MockReply::stream(503, None, "", true);
    let erroring = MockTransport::new([stuck.into(), events(&snapshot(json!("recovered"), 3, 0))]);
    let mut recovering = client(&erroring).subscribe::<_, Value>("value", &(), options.clone());
    let values: Vec<_> = take(&mut recovering, 1).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(values, [update(3, json!("recovered"), true)]);
    assert_eq!(stuck_handle.cancelled(), 1);

    let (live, handle) = MockReply::event_stream(&snapshot(json!(1), 1, 0), true);
    let beating = MockTransport::new([live.into()]);
    let mut watcher = client(&beating).subscribe::<_, Value>("value", &(), SubscribeOptions::new().stall(ms(150)));
    assert_eq!(bounded(watcher.next()).await.unwrap().unwrap(), update(1, json!(1), true));
    let pusher = tokio::spawn(async move {
        for _ in 0..12 {
            tokio::time::sleep(ms(25)).await;
            handle.push(": heartbeat\n\n");
        }
        handle.push(&patch(json!(2), 2, 1, ""));
        handle
    });
    assert_eq!(bounded(watcher.next()).await.unwrap().unwrap(), update(2, json!(2), false));
    pusher.await.unwrap();
    assert_eq!(beating.count(), 1);
    for stall in [Duration::ZERO, Duration::from_micros(500)] {
        let mut invalid = client(&beating).subscribe::<_, Value>("value", &(), SubscribeOptions::new().stall(stall));
        assert!(bounded(invalid.next()).await.unwrap().unwrap_err().message.contains("stallMs"));
        assert!(bounded(invalid.next()).await.is_none());
    }
    assert_eq!(beating.count(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_stall_without_reconnect_is_an_error() {
    let (silent, _handle) = MockReply::event_stream(&snapshot(json!(1), 1, 0), true);
    let mock = MockTransport::new([silent.into()]);
    let mut updates = client(&mock).subscribe::<_, Value>("value", &(), SubscribeOptions::new().stall(ms(50)).reconnect(Reconnect::Off));
    assert_eq!(bounded(updates.next()).await.unwrap().unwrap(), update(1, json!(1), true));
    let error = bounded(updates.next()).await.unwrap().unwrap_err();
    assert_eq!((error.code.as_str(), error.is_transient()), ("WATCH_STALLED", true));
    assert!(bounded(updates.next()).await.is_none());
}

#[tokio::test]
async fn monotonic_subscriptions_skip_values_older_than_any_already_delivered() {
    let lagging = || {
        [
            events(&snapshot(json!("fresh"), 5, 0)),
            events(&(snapshot(json!("old"), 3, 0) + &patch(json!("older"), 4, 1, "") + &patch(json!("newest"), 6, 2, ""))),
        ]
    };
    let monotonic = MockTransport::new(lagging());
    let mut updates = client(&monotonic).subscribe::<_, Value>("value", &(), SubscribeOptions::new().monotonic(true).reconnect(fast(1)));
    let values: Vec<_> = take(&mut updates, 2).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(values, [update(5, json!("fresh"), true), update(6, json!("newest"), true)]);
    let plain = MockTransport::new(lagging());
    let mut all = client(&plain).subscribe::<_, Value>("value", &(), SubscribeOptions::new().reconnect(fast(1)));
    let seen: Vec<_> = take(&mut all, 4).await.into_iter().map(|item| item.map(|u| (u.revision, u.reset)).unwrap()).collect();
    assert_eq!(seen, [(5, true), (3, true), (4, false), (6, false)]);
}

#[tokio::test]
async fn subscribe_refreshes_credentials_on_every_reconnect() {
    let token = Arc::new(AtomicU64::new(0));
    let mock = MockTransport::new([events(&snapshot(json!(1), 1, 0)), failure(503, "UNAVAILABLE", "UNAVAILABLE"), events(&snapshot(json!(2), 2, 0))]);
    let counter = token.clone();
    let client = FlowerClient::builder("http://primary")
        .transport(mock.clone())
        .credentials(Credentials::from_fn(move || {
            let token = counter.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(json!({ "token": token })) }
        }))
        .build()
        .unwrap();
    let mut updates = client.subscribe::<_, u64>("value", &(), SubscribeOptions::new().reconnect(fast(1)));
    let values: Vec<_> = take(&mut updates, 2).await.into_iter().map(|item| item.unwrap().value).collect();
    assert_eq!(values, [1, 2]);
    drop(updates);
    let tokens: Vec<_> = mock.requests().iter().map(|request| request.json["credentials"].clone()).collect();
    assert_eq!(tokens, [json!({"token": 1}), json!({"token": 2}), json!({"token": 3})]);
}

#[tokio::test]
async fn cancelling_a_subscription_stops_it_cleanly_while_connected_while_backing_off_and_before_starting() {
    let (live, handle) = MockReply::event_stream(&snapshot(json!(1), 1, 0), true);
    let connected = MockTransport::new([live.into()]);
    let cancel = CancellationToken::new();
    let mut updates = client(&connected).subscribe::<_, Value>("value", &(), SubscribeOptions::new().cancel(cancel.clone()));
    bounded(updates.next()).await.unwrap().unwrap();
    let pending = tokio::spawn(async move { updates.next().await.is_none() });
    tokio::time::sleep(ms(10)).await;
    cancel.cancel();
    assert!(bounded(pending).await.unwrap());
    assert!(connected.requests()[0].aborted());
    assert_eq!(handle.cancelled(), 1);

    let down = MockTransport::new([failure(503, "UNAVAILABLE", "UNAVAILABLE")]);
    let stop = CancellationToken::new();
    let mut waiting = client(&down).subscribe::<_, Value>("value", &(), SubscribeOptions::new().cancel(stop.clone()).reconnect(fast(60_000)));
    let next = tokio::spawn(async move { waiting.next().await.is_none() });
    tokio::time::sleep(ms(10)).await;
    stop.cancel();
    assert!(bounded(next).await.unwrap());
    // Dropping a stream blocked in backoff stops it too.
    let mut returned = client(&down).subscribe::<_, Value>("value", &(), SubscribeOptions::new().reconnect(fast(60_000)));
    assert!(tokio::time::timeout(ms(20), returned.next()).await.is_err());
    drop(returned);
    tokio::time::sleep(ms(20)).await;
    assert_eq!(connected.count(), 1);
    assert_eq!(down.count(), 2);

    let idle = MockTransport::new([]);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut updates = client(&idle).subscribe::<_, Value>("value", &(), SubscribeOptions::new().cancel(cancelled));
    assert!(updates.next().await.is_none());
    assert_eq!(idle.count(), 0);
}

#[tokio::test]
async fn wait_until_resolves_with_the_first_matching_update_and_closes_its_subscription() {
    let (live, handle) = MockReply::event_stream(
        &(snapshot(json!({"n": 1}), 1, 0) + &patch(json!(2), 2, 1, "/n") + &patch(json!(3), 3, 2, "/n")),
        true,
    );
    let counting = MockTransport::new([live.into()]);
    let found = bounded(client(&counting).wait_until("counter", &(), |value: &Value| value["n"].as_u64() >= Some(2), SubscribeOptions::new()))
        .await
        .unwrap();
    assert_eq!(found, update(2, json!({"n": 2}), false));
    assert!(counting.requests()[0].aborted());
    assert_eq!(handle.cancelled(), 1);

    let truthy = MockTransport::new([events(&(snapshot(json!(0), 1, 0) + &patch(json!(7), 2, 1, "")))]);
    assert_eq!(bounded(client(&truthy).wait_until_truthy("count", &(), SubscribeOptions::new())).await.unwrap().value, json!(7));
    assert_eq!(truthy.requests()[0].json["args"], Value::Null);

    let ended = MockTransport::new([events(&snapshot(json!(false), 1, 0))]);
    let error = bounded(client(&ended).wait_until_truthy("flag", &(), SubscribeOptions::new().reconnect(Reconnect::Off))).await.unwrap_err();
    assert!(error.code == "WATCH_ENDED" && error.is_transient(), "{error:?}");
    let gone = MockTransport::new([failure(404, "METHOD_NOT_FOUND", "METHOD_NOT_FOUND")]);
    assert_eq!(bounded(client(&gone).wait_until_truthy("flag", &(), SubscribeOptions::new())).await.unwrap_err().code, "METHOD_NOT_FOUND");

    let cancel = CancellationToken::new();
    let (open, _open) = MockReply::event_stream(&snapshot(json!(false), 1, 0), true);
    let never = MockTransport::new([events(&snapshot(json!(false), 1, 0)), open.into()]);
    let waiting = tokio::spawn({
        let client = client(&never);
        let options = SubscribeOptions::new().cancel(cancel.clone()).reconnect(fast(1));
        async move { client.wait_until_truthy("flag", &(), options).await }
    });
    tokio::time::sleep(ms(30)).await;
    cancel.cancel();
    assert!(bounded(waiting).await.unwrap().unwrap_err().is_aborted());
    assert_eq!(never.count(), 2);

    // Typed values decode; a mismatch ends the subscription with a decode error.
    let typed = MockTransport::new([events(&snapshot(json!("text"), 1, 0))]);
    let error = bounded(client(&typed).wait_until("count", &(), |n: &u64| *n > 2, SubscribeOptions::new())).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Decode);
}

#[tokio::test]
async fn flower_admin_scopes_operator_calls_to_partitions_and_alone_sends_the_operator_token() {
    let mock = MockTransport::new((0..10).map(|_| MockReply::ok(json!(null)).into()));
    let admin = FlowerAdmin::builder("http://seed:7101/").admin_token("operator").transport(mock.clone()).build().unwrap();
    let tenant = admin.partition("tenant/🌻");
    let path = "/partitions/tenant%2F%F0%9F%8C%BB";
    assert_eq!(admin.url(), "http://seed:7101");
    assert_eq!(tenant.url(), format!("http://seed:7101{path}"));
    let bundle = Bundle::from(JavaScriptBundle { hash: "h".into(), javascript: "code".into() });
    tenant
        .deploy(&bundle, DeployOptions { request_id: Some("deploy-1".into()), preparation: Some(Preparation::Blocking) })
        .await
        .unwrap();
    tenant.admin::<_, Value>("/admin/keys", &json!({"operation": "list"})).await.unwrap();
    tenant.admin::<_, Value>("/admin/retention", &json!({"operation": "status"})).await.unwrap();
    tenant.admin::<_, Value>("/admin/transactions", &json!({"operation": "status"})).await.unwrap();
    admin.admin::<_, Value>("/admin/partitions/catalog", &json!({"action": "list"})).await.unwrap();
    let requests = mock.requests();
    let urls: Vec<_> = requests.iter().map(|request| request.url.clone()).collect();
    assert_eq!(
        urls,
        [
            format!("http://seed:7101{path}/admin/deploy"),
            format!("http://seed:7101{path}/admin/keys"),
            format!("http://seed:7101{path}/admin/retention"),
            format!("http://seed:7101{path}/admin/transactions"),
            "http://seed:7101/admin/partitions/catalog".into()
        ]
    );
    assert!(requests.iter().all(|request| request.header("authorization") == Some("Bearer operator")));
    assert_eq!(
        std::str::from_utf8(&requests[0].body).unwrap(),
        r#"{"requestId":"deploy-1","bundle":{"hash":"h","javascript":"code"},"preparation":"blocking"}"#
    );
    for name in ["", " ", "\u{feff}", "a\u{0}", "a\n", "a\u{7f}"] {
        assert!(admin.try_partition(name).is_err(), "{name:?}");
    }
    assert!(admin.try_partition("\u{85}").is_ok(), "JavaScript's trim keeps NEL");

    FlowerAdmin::builder("http://seed:7101").transport(mock.clone()).build().unwrap().partition("a").key_list().await.unwrap_err();
    assert_eq!(mock.requests().last().unwrap().header("authorization"), None);
    let client = FlowerClient::builder("http://seed:7101").transport(mock.clone()).build().unwrap();
    client.partition("tenant/🌻").mutate_value("set", &1, MutationOptions::new().request_id("r")).await.unwrap();
    let last = mock.requests().last().unwrap().clone();
    assert_eq!(last.url, format!("http://seed:7101{path}/v1/mutate"));
    assert_eq!(last.header("authorization"), None);
}

#[tokio::test]
async fn admin_bodies_match_the_typescript() {
    let mock = MockTransport::new((0..8).map(|_| MockReply::ok(json!(null)).into()));
    let admin = FlowerAdmin::builder("http://seed").admin_token("t").transport(mock.clone()).build().unwrap();
    let catalog = json!({"domain": null, "revision": 3, "keys": {}, "bindings": {}});
    mock.push(MockReply::result(1, catalog, false));
    let _ = admin.key_generate("trinity-tokens", "Ed25519", None, Some("key::trinity-tokens")).await;
    let _ = admin.key_bind("tokens", "trinity-tokens", &["sign", "verify"], Some("bind::tokens")).await;
    let _ = admin
        .control_retention(
            7,
            &flower_client::RetentionAction::Initialize { database: "d".into(), incarnation: "i".into(), max_receipt_bytes: None },
        )
        .await;
    let _ = admin
        .control_retention(8, &flower_client::RetentionAction::Rotate { incarnation: "i".into(), epoch_ms: Some(3_600_000), keep_epochs: 24 })
        .await;
    let _ = admin.initialize(&[("1".to_owned(), "127.0.0.1:7101".to_owned())].into()).await;
    let bodies: Vec<String> = mock.requests().iter().map(|request| String::from_utf8(request.body.to_vec()).unwrap()).collect();
    assert_eq!(
        bodies,
        [
            r#"{"operation":"generate","name":"trinity-tokens","algorithm":"Ed25519","requestId":"key::trinity-tokens"}"#,
            r#"{"operation":"bind","name":"tokens","key":"trinity-tokens","usages":["sign","verify"],"requestId":"bind::tokens"}"#,
            r#"{"expected_revision":7,"action":{"operation":"initialize","database":"d","incarnation":"i","max_receipt_bytes":null}}"#,
            r#"{"expected_revision":8,"action":{"operation":"rotate","incarnation":"i","epoch_ms":3600000,"keep_epochs":24}}"#,
            r#"{"1":"127.0.0.1:7101"}"#,
        ]
    );
    let urls: Vec<_> = mock.requests().iter().map(|request| request.url.clone()).collect();
    assert_eq!(urls.last().unwrap(), "http://seed/raft/initialize");
}

#[tokio::test]
async fn typed_clients_encode_arguments_and_decode_results() {
    #[derive(serde::Serialize)]
    struct Add {
        by: u32,
    }
    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct Row {
        n: u64,
    }
    let mock = MockTransport::new([
        MockReply::ok(json!(3)).into(),
        MockReply::ok(json!({"n": 2})).into(),
        MockReply::ok(json!(null)).into(),
        MockReply::ok(json!(4)).into(),
        MockReply::ok(json!("x")).into(),
        MockReply::text(200, "{").into(),
    ]);
    let client = client(&mock);
    let counted: QueryResult<u64> = client.query("count", &(), RequestOptions::new()).await.unwrap();
    let found: QueryResult<Option<Row>> = client.query("find", "main", RequestOptions::new()).await.unwrap();
    let missing: QueryResult<Option<Row>> = client.query("find", "other", RequestOptions::new()).await.unwrap();
    let added: MutationResult<u64> = client.mutate("add", &Add { by: 2 }, MutationOptions::new().request_id("r1")).await.unwrap();
    assert_eq!((counted.value, found.value, missing.value, added.value), (3, Some(Row { n: 2 }), None, 4));
    let wrong = client.query::<_, u64>("count", &(), RequestOptions::new()).await.unwrap_err();
    assert_eq!((wrong.kind, wrong.status), (ErrorKind::Decode, 200));
    let malformed = client.query::<_, Value>("count", &(), RequestOptions::new()).await.unwrap_err();
    assert_eq!(malformed.kind, ErrorKind::Decode);
    let seen: Vec<_> = mock
        .requests()
        .iter()
        .map(|request| (request.url["http://db".len()..].to_owned(), request.json["name"].clone(), request.json["args"].clone()))
        .collect();
    assert_eq!(seen[..4], [
        ("/v1/query".into(), json!("count"), json!(null)),
        ("/v1/query".into(), json!("find"), json!("main")),
        ("/v1/query".into(), json!("find"), json!("other")),
        ("/v1/mutate".into(), json!("add"), json!({"by": 2}))
    ]);
    assert_eq!(mock.requests()[3].json["requestId"], json!("r1"));
    // Arguments too deep for canonicalJson are refused before sending.
    let mut deep = json!("x");
    for _ in 0..128 {
        deep = json!([deep]);
    }
    let error = client.query_value("deep", &deep, RequestOptions::new()).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Invalid);
    assert_eq!(mock.count(), 6);
}
