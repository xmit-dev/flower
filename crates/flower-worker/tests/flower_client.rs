//! The adapter to `flower_client::FlowerClient`, on its scripted transport: arguments go out
//! verbatim with one request ID for every attempt, the worker's retry policy maps onto the
//! client's, and errors read as `worker.ts`'s `message()` gives them.
#![cfg(feature = "flower-client")]

use std::sync::Arc;

use flower_client::testing::{MockReply, MockTransport, Step};
use flower_client::{FlowerClient, FlowerError};
use flower_worker::{ClientError, QueueClient, RetryPolicy, WorkError, to_js_raw, truthy};
use serde::Serialize;
use serde_json::json;

fn client(transport: &Arc<MockTransport>) -> FlowerClient {
    FlowerClient::builder("http://flower.test")
        .transport(transport.clone())
        .build()
        .unwrap()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaimArgs {
    owner: &'static str,
    lease_ms: u64,
    max: u64,
    wait_ms: u64,
}

const ARGS: ClaimArgs = ClaimArgs {
    owner: "w",
    lease_ms: 300,
    max: 1,
    wait_ms: 60_000,
};

fn unavailable() -> Step {
    Step::Reply(MockReply::failure(
        503,
        "UNAVAILABLE",
        "No leader yet",
        json!({}),
    ))
}

#[tokio::test(start_paused = true)]
async fn mutations_send_the_arguments_verbatim_and_retry_with_one_request_id() {
    let transport = MockTransport::new([
        unavailable(),
        Step::Reply(MockReply::ok(json!({ "id": "a" }))),
    ]);
    let value = QueueClient::mutate(
        &client(&transport),
        "jobs.claim",
        to_js_raw(&ARGS).unwrap(),
        RetryPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(value, json!({ "id": "a" }));
    let requests = transport.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0].url.ends_with("/v1/mutate"),
        "{}",
        requests[0].url
    );
    let body = String::from_utf8(requests[0].body.to_vec()).unwrap();
    let prefix = r#"{"name":"jobs.claim","args":{"owner":"w","leaseMs":300,"max":1,"waitMs":60000},"requestId":""#;
    assert!(body.starts_with(prefix), "{body}");
    assert_eq!(
        requests[0].body, requests[1].body,
        "the retry repeats the request, request ID included"
    );
}

#[tokio::test(start_paused = true)]
async fn the_workers_retry_policy_bounds_the_attempts() {
    let transport = MockTransport::new([unavailable(), unavailable(), unavailable()]);
    let retry = RetryPolicy {
        attempts: Some(2),
        ..RetryPolicy::default()
    };
    let error = QueueClient::mutate(
        &client(&transport),
        "jobs.renew",
        to_js_raw(&json!({})).unwrap(),
        retry,
    )
    .await
    .unwrap_err();
    assert_eq!((error.status(), error.code()), (503, "UNAVAILABLE"));
    assert!(error.is_transient());
    assert_eq!(transport.count(), 2);

    // A deadline already past: the first attempt runs, no retry starts.
    let transport = MockTransport::new([unavailable(), unavailable()]);
    let retry = RetryPolicy {
        until: Some(flower_worker::system_now_ms() - 1),
        ..RetryPolicy::default()
    };
    QueueClient::mutate(
        &client(&transport),
        "jobs.renew",
        to_js_raw(&json!({})).unwrap(),
        retry,
    )
    .await
    .unwrap_err();
    assert_eq!(transport.count(), 1);
}

#[tokio::test(start_paused = true)]
async fn wait_until_returns_the_first_value_that_satisfies_the_predicate() {
    let events = "event: snapshot\ndata: {\"sequence\":0,\"revision\":1,\"value\":false}\n\n\
                  event: patch\ndata: {\"sequence\":1,\"baseSequence\":0,\"revision\":2,\"patch\":[{\"op\":\"replace\",\"path\":\"\",\"value\":true}]}\n\n";
    let (reply, _handle) = MockReply::event_stream(events, true);
    let transport = MockTransport::new([Step::Reply(reply)]);
    let value = QueueClient::wait_until(
        &client(&transport),
        "jobs.ready",
        to_js_raw(&json!({ "owner": "w" })).unwrap(),
        truthy,
    )
    .await
    .unwrap();
    assert_eq!(value, json!(true));
    let request = &transport.requests()[0];
    assert!(request.url.ends_with("/v1/watch"), "{}", request.url);
    assert_eq!(
        request.json,
        json!({ "name": "jobs.ready", "args": { "owner": "w" } })
    );
}

#[test]
fn errors_read_as_the_ts_worker_describes_them() {
    let lost = FlowerError::new("The lease moved on", 422, "EVALUATION_FAILED").with_failure(Some(
        flower_client::Failure {
            code: "LEASE_LOST".into(),
            message: "Lease lost".into(),
            details: None,
        },
    ));
    assert_eq!(ClientError::describe(&lost), "LEASE_LOST: Lease lost");
    assert_eq!(ClientError::failure_code(&lost), Some("LEASE_LOST"));
    assert!(!ClientError::is_transient(&lost));
    let unavailable = FlowerError::new("No leader yet", 503, "UNAVAILABLE");
    assert_eq!(
        ClientError::describe(&unavailable),
        "UNAVAILABLE: No leader yet"
    );
    assert_eq!(
        ClientError::describe(&FlowerError::transport(
            "ECONNREFUSED",
            "connection refused"
        )),
        "ECONNREFUSED"
    );
    assert_eq!(
        ClientError::describe(&FlowerError::timeout()),
        "The operation was aborted due to timeout"
    );
    assert_eq!(
        ClientError::describe(&FlowerError::aborted()),
        "This operation was aborted"
    );
    // A job that fails with `?` on a client error reports it the same way.
    let failed: WorkError = unavailable.into();
    assert_eq!(failed.message(), "UNAVAILABLE: No leader yet");
}
