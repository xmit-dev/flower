//! Ports of `sdk/watch.test.ts`, plus SSE chunk-boundary fuzzing.

mod common;

use std::collections::VecDeque;
use std::sync::Arc;

use common::*;
use flower_client::sse::{SseFrame, SseParser};
use flower_client::testing::{MockReply, MockTransport, Step};
use flower_client::watch::{WatchEvent, apply_patch, decode_watch_event};
use flower_client::{
    CancellationToken, FlowerClient, FlowerError, PatchOperation, QueryResult, WatchBudgets, WatchDelta, WatchOptions,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

fn encode(event: &str, value: &Value, ending: &str) -> String {
    format!("event: {event}{ending}data: {value}{ending}{ending}")
}

fn snap(value: Value, sequence: i64, revision: i64) -> String {
    encode("snapshot", &json!({"sequence": sequence, "revision": revision, "value": value}), "\n")
}

fn patch_event(operations: Value, sequence: i64, base: i64, revision: i64) -> String {
    encode("patch", &json!({"sequence": sequence, "baseSequence": base, "revision": revision, "patch": operations}), "\n")
}

/// A client whose only watch replies with `text` (open unless `close`), with `status`.
fn fixture(text: &str, close: bool, status: u16, content_type: &str) -> (FlowerClient, Arc<MockTransport>, flower_client::testing::StreamHandle) {
    let (reply, handle) = MockReply::stream(status, Some(content_type), text, !close);
    let mock = MockTransport::new([reply.into()]);
    let client = FlowerClient::builder("http://localhost:7101").transport(mock.clone()).build().unwrap();
    (client, mock, handle)
}

fn events(text: &str, close: bool) -> (FlowerClient, Arc<MockTransport>, flower_client::testing::StreamHandle) {
    fixture(text, close, 200, "text/event-stream; charset=utf-8")
}

fn budgets(event: usize, value: usize, operations: usize) -> WatchOptions {
    WatchOptions {
        budgets: WatchBudgets { max_event_bytes: event, max_value_bytes: value, max_patch_operations: operations },
        ..Default::default()
    }
}

#[tokio::test]
async fn watch_reconstructs_deltas_same_revision_clocks_and_snapshot_fallback() {
    let (client, mock, handle) = events(
        &(snap(json!({"list": [1], "label": "first"}), 0, 1)
            + &patch_event(json!([{"op": "add", "path": "/list/-", "value": 2}]), 1, 0, 1)
            + &snap(json!({"fresh": true}), 2, 2)),
        false,
    );
    let mut watcher = client.watch::<_, Value>("clock", &json!({"key": "x"}), WatchOptions::default());
    let first = bounded(watcher.next()).await.unwrap().unwrap();
    assert_eq!(first.value, json!({"list": [1], "label": "first"}));
    assert_eq!(bounded(watcher.next()).await.unwrap().unwrap(), QueryResult { revision: 1, value: json!({"list": [1, 2], "label": "first"}) });
    assert_eq!(bounded(watcher.next()).await.unwrap().unwrap(), QueryResult { revision: 2, value: json!({"fresh": true}) });
    drop(watcher);
    assert_eq!(handle.cancelled(), 1);
    let request = &mock.requests()[0];
    assert!(request.aborted());
    assert_eq!(request.url, "http://localhost:7101/v1/watch");
    assert_eq!(request.header("accept"), Some("text/event-stream"));
    assert_eq!(request.json, json!({"name": "clock", "args": {"key": "x"}}));
}

#[tokio::test]
async fn raw_events_survive_one_byte_utf8_chunks_crlf_comments_and_multiline_data() {
    let text = ": heartbeat\r\n\r\nid: 0\r\nevent: snapshot\r\ndata: {\"sequence\":0,\r\ndata: \"revision\":3,\"value\":\"😀\"}\r\n\r\n";
    let (client, _mock, handle) = events("", false);
    for byte in text.as_bytes() {
        handle.push_bytes(&[*byte]);
    }
    handle.close();
    let events: Vec<_> = client.watch_deltas("value", &(), WatchOptions::default()).collect().await;
    let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
    assert_eq!(events, [WatchDelta::Snapshot { sequence: 0, revision: 3, value: json!("😀") }]);
}

#[tokio::test]
async fn dropping_interrupts_a_pending_next_and_cancels_both_reader_and_request() {
    let (client, mock, handle) = events(&snap(Value::Null, 0, 1), false);
    let mut watcher = client.watch::<_, Value>("idle", &(), WatchOptions::default());
    bounded(watcher.next()).await.unwrap().unwrap();
    assert!(tokio::time::timeout(ms(20), watcher.next()).await.is_err());
    drop(watcher);
    assert_eq!(handle.cancelled(), 1);
    assert!(mock.requests()[0].aborted());
}

#[tokio::test]
async fn cancel_ends_cleanly_before_startup_during_sse_and_during_a_stalled_http_error() {
    let (unused, mock, _handle) = events("", false);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut early = unused.watch::<_, Value>("early", &(), WatchOptions { cancel: Some(cancel), ..Default::default() });
    assert!(early.next().await.is_none());
    assert_eq!(mock.count(), 0);
    for status in [200, 503] {
        let (client, _mock, handle) = fixture("", false, status, "text/event-stream");
        let cancel = CancellationToken::new();
        let mut watcher = client.watch_deltas("idle", &(), WatchOptions { cancel: Some(cancel.clone()), ..Default::default() });
        let next = tokio::spawn(async move { watcher.next().await.is_none() });
        tokio::time::sleep(ms(10)).await;
        cancel.cancel();
        assert!(bounded(next).await.unwrap(), "{status}");
        assert_eq!(handle.cancelled(), 1, "{status}");
    }
}

#[tokio::test]
async fn terminal_server_errors_retain_code_and_status_and_do_not_reconnect() {
    let (client, mock, handle) = events(
        &(snap(json!(1), 0, 1) + &encode("error", &json!({"error": {"code": "METHOD_NOT_FOUND", "message": "alias revoked", "status": 404}}), "\n")),
        false,
    );
    let mut watcher = client.watch::<_, Value>("gone", &(), WatchOptions::default());
    bounded(watcher.next()).await.unwrap().unwrap();
    let error = bounded(watcher.next()).await.unwrap().unwrap_err();
    assert_eq!((error.status, error.code.as_str()), (404, "METHOD_NOT_FOUND"));
    assert_eq!(handle.cancelled(), 1);
    assert!(bounded(watcher.next()).await.is_none());
    assert_eq!(mock.count(), 1);
}

#[tokio::test]
async fn invalid_sequence_revision_pointer_event_id_incomplete_events_and_unsupported_content_fail_closed() {
    let cases = [
        snap(json!(1), -1, 1),
        snap(json!(1), 2, 1) + &snap(json!(2), 2, 1),
        snap(json!(1), 3, 1) + &snap(json!(2), 2, 1),
        patch_event(json!([]), 0, -1, 1),
        snap(json!(1), 0, 1) + &patch_event(json!([]), 2, 0, 1),
        snap(json!(1), 0, 1) + &patch_event(json!([]), 1, 3, 1),
        snap(json!(1), 0, 3) + &patch_event(json!([]), 1, 0, 2),
        snap(json!({}), 0, 1) + &patch_event(json!([{"op": "replace", "path": "/missing", "value": 1}]), 1, 0, 1),
        "id: 2\n".to_owned() + &snap(json!(1), 0, 1),
        "event: snapshot\ndata: {}".to_owned(),
        "event: snapshot\ndata: {bad}\n\n".to_owned(),
        encode("unknown", &json!({}), "\n"),
        encode("snapshot", &json!({"sequence": 0, "revision": 1, "value": 1e300}), "\n").replace("1e+300", "1e999"),
        encode("snapshot", &json!({"sequence": 0.5, "revision": 1, "value": 1}), "\n"),
        encode("snapshot", &json!({"sequence": 9_007_199_254_740_992u64, "revision": 1, "value": 1}), "\n"),
        encode("snapshot", &json!({"sequence": 0, "revision": 1}), "\n"),
        encode("error", &json!({"error": {"code": "X", "message": "m", "status": 99}}), "\n"),
        encode("snapshot", &json!([]), "\n"),
    ];
    for text in cases {
        let (client, mock, _handle) = events(&text, true);
        let items: Vec<_> = client.watch::<_, Value>("broken", &(), WatchOptions::default()).collect().await;
        let error = items.into_iter().find_map(Result::err).unwrap_or_else(|| panic!("no error for {text:?}"));
        assert_eq!(error.code, "WATCH_PROTOCOL_ERROR", "{text:?}: {error:?}");
        assert!(!error.is_transient());
        assert_eq!(mock.count(), 1);
    }
    let (wrong, _mock, handle) = fixture("x", false, 200, "application/json");
    let error = bounded(wrong.watch::<_, Value>("wrong", &(), WatchOptions::default()).next()).await.unwrap().unwrap_err();
    assert!(error.message.contains("text/event-stream"), "{error:?}");
    assert_eq!(handle.cancelled(), 1);
}

#[test]
fn sse_limits_cover_comments_and_lines_and_malformed_utf8_without_buffering_indefinitely() {
    let mut frames = VecDeque::new();
    let mut parser = SseParser::new(64);
    let error = parser.push(format!(":{}", "x".repeat(100)).as_bytes(), &mut frames).unwrap_err();
    assert!(error.0.contains("byte limit"));
    let mut parser = SseParser::new(1024);
    assert!(parser.push(&[0xc3, 0x28], &mut frames).unwrap_err().0.contains("UTF-8"));
    let mut parser = SseParser::new(1024);
    parser.push(&[0xf0, 0x9f], &mut frames).unwrap();
    assert!(parser.finish().unwrap_err().0.contains("UTF-8"), "a character cut by the end");
    let mut parser = SseParser::new(1024);
    parser.push(b"data: x", &mut frames).unwrap();
    assert!(parser.finish().unwrap_err().0.contains("ended during an event"));
    // Frames completed before a failure in the same chunk are still delivered.
    let mut parser = SseParser::new(16);
    let error = parser.push(b"data: a\n\n:too long for the budget", &mut frames);
    assert!(error.is_err());
    assert_eq!(frames.pop_front().unwrap().data, "a");
}

#[tokio::test]
async fn an_interrupted_body_surfaces_its_own_error() {
    let (client, _mock, handle) = events("", false);
    handle.error(FlowerError::transport("ECONNRESET", "network terminated"));
    let error = bounded(client.watch_deltas("value", &(), WatchOptions::default()).next()).await.unwrap().unwrap_err();
    assert_eq!((error.code.as_str(), error.message.as_str(), error.is_transient()), ("ECONNRESET", "network terminated", true));
}

fn frame(event: &str, data: String) -> SseFrame {
    SseFrame { event: event.into(), data, id: None }
}

#[test]
fn snapshot_size_and_value_depth_budgets_exclude_only_the_trusted_envelope() {
    let budgets = WatchBudgets::default();
    let data = json!({"sequence": 0, "revision": 1, "value": "x".repeat(16 * 1024 * 1024)}).to_string();
    assert!(decode_watch_event(&frame("snapshot", data), -1, -1, &budgets).unwrap_err().0.contains("maxValueBytes"));
    let mut value = json!(1);
    for _ in 0..128 {
        value = json!([value]);
    }
    let ok = json!({"sequence": 0, "revision": 1, "value": value}).to_string();
    assert!(matches!(decode_watch_event(&frame("snapshot", ok), -1, -1, &budgets), Ok(WatchEvent::Delta(WatchDelta::Snapshot { .. }))));
    let deep = json!({"sequence": 0, "revision": 1, "value": [value]}).to_string();
    assert!(decode_watch_event(&frame("snapshot", deep), -1, -1, &budgets).unwrap_err().0.contains("128"));
    let hostile = format!(r#"{{"sequence":0,"revision":1,"value":{}1{}}}"#, "[".repeat(10_000), "]".repeat(10_000));
    assert!(decode_watch_event(&frame("snapshot", hostile), -1, -1, &budgets).unwrap_err().0.contains("128"));
}

fn op(value: Value) -> PatchOperation {
    serde_json::from_value(value).unwrap()
}

#[test]
fn json_patches_handle_arrays_escaped_pointers_root_replacement_and_prototype_looking_keys() {
    let budgets = WatchBudgets::default();
    let initial = json!({"__proto__": {"safe": 1}, "constructor": {"prototype": {"safe": 2}}, "a/b": {"~": [1, 3]}});
    let result = apply_patch(
        &initial,
        &[
            op(json!({"op": "replace", "path": "/__proto__/safe", "value": 9})),
            op(json!({"op": "add", "path": "/constructor/prototype/owned", "value": true})),
            op(json!({"op": "add", "path": "/a~1b/~0/1", "value": 2})),
            op(json!({"op": "remove", "path": "/a~1b/~0/0"})),
        ],
        &budgets,
    )
    .unwrap();
    assert_eq!(result["__proto__"]["safe"], json!(9));
    assert_eq!(result["constructor"]["prototype"]["owned"], json!(true));
    assert_eq!(result["a/b"]["~"], json!([2, 3]));
    assert_eq!(initial["__proto__"]["safe"], json!(1));
    assert_eq!(apply_patch(&json!({}), &[op(json!({"op": "add", "path": "/__proto__", "value": {"x": 1}}))], &budgets).unwrap(), json!({"__proto__": {"x": 1}}));
    assert_eq!(apply_patch(&result, &[op(json!({"op": "replace", "path": "", "value": [null]}))], &budgets).unwrap(), json!([null]));
    for path in ["/constructor/prototype/polluted", "/__proto__/polluted"] {
        assert!(apply_patch(&json!({}), &[op(json!({"op": "add", "path": path, "value": true}))], &budgets).is_err());
    }
    for path in ["/01", "/-", "/3", "/length", "/~2", "/999999999999999999", "/99999999999999999999999"] {
        assert!(apply_patch(&json!([0]), &[op(json!({"op": "replace", "path": path, "value": true}))], &budgets).is_err(), "{path}");
    }
    assert_eq!(apply_patch(&json!([0]), &[op(json!({"op": "add", "path": "/1", "value": 1}))], &budgets).unwrap(), json!([0, 1]));
    assert_eq!(apply_patch(&json!([0]), &[op(json!({"op": "add", "path": "/0", "value": 1}))], &budgets).unwrap(), json!([1, 0]));
    assert!(apply_patch(&json!(1), &[op(json!({"op": "remove", "path": ""}))], &budgets).is_err());
    assert!(apply_patch(&json!({"a": 1}), &[op(json!({"op": "add", "path": "/a/b", "value": 1}))], &budgets).unwrap_err().0.contains("not a container"));
    let oversized = json!({"sequence": 1, "baseSequence": 0, "revision": 1, "patch": (0..257).map(|_| json!({"op": "remove", "path": "/x"})).collect::<Vec<_>>()});
    assert!(decode_watch_event(&frame("patch", oversized.to_string()), 0, 1, &budgets).unwrap_err().0.contains("oversized"));
    for (operation, message) in [
        (json!("remove"), "Invalid patch operation"),
        (json!({"op": "move", "path": "/x"}), "Unsupported patch operation"),
        (json!({"op": "add", "path": 1, "value": 1}), "JSON pointer"),
        (json!({"op": "add", "path": "x", "value": 1}), "begin with /"),
        (json!({"op": "add", "path": "/x"}), "no value"),
    ] {
        let data = json!({"sequence": 1, "baseSequence": 0, "revision": 1, "patch": [operation]}).to_string();
        assert!(decode_watch_event(&frame("patch", data), 0, 1, &budgets).unwrap_err().0.contains(message), "{message}");
    }
}

#[test]
fn removing_a_key_keeps_the_order_of_the_others() {
    let mut value = json!({"a": 1, "b": 2, "c": 3});
    flower_client::watch::apply_patch_in_place(&mut value, &[op(json!({"op": "remove", "path": "/a"}))], &WatchBudgets::default()).unwrap();
    assert_eq!(value.as_object().unwrap().keys().collect::<Vec<_>>(), ["b", "c"]);
}

#[tokio::test]
async fn watch_byte_budgets_cover_utf8_snapshots_and_reconstructed_values() {
    let (small, _mock, _handle) = events(&snap(json!("🌼"), 0, 1), true);
    let error = small.watch::<_, Value>("small", &(), budgets(17 << 20, 5, 256)).next().await.unwrap().unwrap_err();
    assert!(error.message.contains("maxValueBytes"), "{error:?}");
    let (client, mock, _handle) = events(
        &(snap(json!({"text": ""}), 0, 1) + &patch_event(json!([{"op": "replace", "path": "/text", "value": "🌼🌼"}]), 1, 0, 1)),
        true,
    );
    let mut watcher = client.watch::<_, Value>("small", &(), budgets(1024, 15, 256));
    assert_eq!(watcher.next().await.unwrap().unwrap().value, json!({"text": ""}));
    assert!(watcher.next().await.unwrap().unwrap_err().message.contains("maxValueBytes"));
    assert_eq!(mock.requests()[0].json, json!({"name": "small", "args": null}));
    let (event, _mock, _handle) = events(&snap(json!(1), 0, 1), true);
    let error = event.watch_deltas("small", &(), budgets(20, 16 << 20, 256)).next().await.unwrap().unwrap_err();
    assert!(error.message.contains("byte limit"), "{error:?}");
}

#[tokio::test]
async fn watch_limits_can_grow_beyond_the_safe_defaults() {
    let value = "x".repeat(17 * 1024 * 1024);
    let (client, _mock, _handle) = events(&snap(json!(value), 0, 1), true);
    let mut watch = client.watch::<_, String>("large", &(), budgets(19 << 20, 18 << 20, 256));
    assert_eq!(watch.next().await.unwrap().unwrap().value, value);
    assert!(watch.next().await.is_none());
    let operations: Vec<_> = (0..300).map(|index| json!({"op": "add", "path": format!("/{index}"), "value": index})).collect();
    let (client, _mock, _handle) = events(&(snap(json!({}), 0, 1) + &patch_event(json!(operations), 1, 0, 1)), true);
    let values: Vec<_> = client.watch::<_, Value>("many", &(), budgets(17 << 20, 16 << 20, 300)).collect().await;
    assert_eq!(values[1].as_ref().unwrap().value.as_object().unwrap().len(), 300);
    let (client, _mock, _handle) = events(&(snap(json!({}), 0, 1) + &patch_event(json!(operations), 1, 0, 1)), true);
    let values: Vec<_> = client.watch::<_, Value>("many", &(), budgets(17 << 20, 16 << 20, 299)).collect().await;
    assert!(values[1].as_ref().unwrap_err().message.contains("oversized"));
}

#[tokio::test]
async fn watch_budgets_reject_invalid_values_before_opening_http_and_bound_http_error_bodies() {
    for options in [budgets(0, 1, 1), budgets(1, 0, 1), budgets(1, 1, 0), budgets(usize::MAX, 1, 1)] {
        let (client, mock, _handle) = events(&snap(json!(1), 0, 1), true);
        let error = client.watch::<_, Value>("invalid", &(), options.clone()).next().await.unwrap().unwrap_err();
        assert!(error.message.contains("must be a positive safe integer"), "{error:?}");
        assert!(client.watch_deltas("invalid", &(), options).next().await.unwrap().is_err());
        assert_eq!(mock.count(), 0);
    }
    let body = json!({"error": {"message": "x".repeat(100), "code": "BIG"}}).to_string();
    let (small, _mock, _handle) = fixture(&body, true, 503, "application/json");
    let error = small.watch::<_, Value>("error", &(), budgets(32, 1, 1)).next().await.unwrap().unwrap_err();
    assert!(error.message.contains("maxEventBytes"), "{error:?}");
    let (allowed, _mock, _handle) = fixture(&body, true, 503, "application/json");
    let error = allowed.watch::<_, Value>("error", &(), budgets(1024, 1, 1)).next().await.unwrap().unwrap_err();
    assert_eq!((error.code.as_str(), error.status), ("BIG", 503));
}

#[tokio::test]
async fn shared_producers_allow_nonzero_joins_and_reset_gaps_while_patches_require_an_exact_base() {
    let text = snap(json!({"n": 7}), 7, 3)
        + &patch_event(json!([{"op": "replace", "path": "/n", "value": 8}]), 8, 7, 4)
        + &snap(json!({"n": 20}), 20, 8)
        + &patch_event(json!([{"op": "replace", "path": "/n", "value": 21}]), 21, 20, 8);
    let (client, _mock, _handle) = events(&text, true);
    let values: Vec<_> = client.watch::<_, Value>("shared", &(), WatchOptions::default()).collect().await;
    let values: Vec<_> = values.into_iter().map(Result::unwrap).collect();
    assert_eq!(
        values,
        [
            QueryResult { revision: 3, value: json!({"n": 7}) },
            QueryResult { revision: 4, value: json!({"n": 8}) },
            QueryResult { revision: 8, value: json!({"n": 20}) },
            QueryResult { revision: 8, value: json!({"n": 21}) },
        ]
    );
    let data = json!({"sequence": 20, "baseSequence": 8, "revision": 8, "patch": []}).to_string();
    assert!(decode_watch_event(&frame("patch", data), 8, 4, &WatchBudgets::default()).unwrap_err().0.contains("consecutive"));
}

#[tokio::test]
async fn watch_ends_before_its_first_snapshot_as_watch_ended() {
    let (client, _mock, _handle) = events(": only a comment\n\n", true);
    let error = client.watch_deltas("value", &(), WatchOptions::default()).next().await.unwrap().unwrap_err();
    assert_eq!((error.code.as_str(), error.is_transient()), ("WATCH_ENDED", true));
    let mock = MockTransport::new([Step::Reply(MockReply::events(""))]);
    let client = FlowerClient::builder("http://db").transport(mock).build().unwrap();
    assert_eq!(client.watch_deltas("value", &(), WatchOptions::default()).next().await.unwrap().unwrap_err().code, "WATCH_ENDED");
}

/// Split `bytes` at every boundary pair and check the parser sees the same frames.
#[test]
fn sse_parsing_is_independent_of_chunk_boundaries() {
    let transcript = "\u{feff}: hello\r\n\r\nid: 1\revent: snapshot\rdata: {\"a\":\"é😀\"}\r\rdata: line one\r\ndata:line two\n\nevent:\ndata\nid: bad\u{0}id\n\n: tail\n"
        .as_bytes();
    let reference = parse(&[transcript]).unwrap();
    assert_eq!(
        reference,
        [
            SseFrame { event: "snapshot".into(), data: "{\"a\":\"é😀\"}".into(), id: Some("1".into()) },
            SseFrame { event: "message".into(), data: "line one\nline two".into(), id: None },
            SseFrame { event: "message".into(), data: "".into(), id: None },
        ]
    );
    for first in 0..=transcript.len() {
        for second in first..=transcript.len() {
            let chunks = [&transcript[..first], &transcript[first..second], &transcript[second..]];
            assert_eq!(parse(&chunks).unwrap(), reference, "split at {first}/{second}");
        }
    }
    // Byte accounting: a bare CR costs two bytes, so the same event fits 30 bytes with LF but not CR.
    let lf = b"data: 0123456789abcdef\n\n";
    let cr = b"data: 0123456789abcdef\r\r";
    assert!(parse_limited(lf, lf.len()).is_ok());
    assert!(parse_limited(cr, lf.len()).is_err());
    assert!(parse_limited(cr, lf.len() + 2).is_ok());
}

fn parse(chunks: &[&[u8]]) -> Result<Vec<SseFrame>, String> {
    let mut parser = SseParser::new(1 << 20);
    let mut frames = VecDeque::new();
    for chunk in chunks {
        parser.push(chunk, &mut frames).map_err(|error| error.0.to_owned())?;
    }
    parser.finish().map_err(|error| error.0.to_owned())?;
    Ok(frames.into())
}

fn parse_limited(bytes: &[u8], limit: usize) -> Result<Vec<SseFrame>, String> {
    let mut parser = SseParser::new(limit);
    let mut frames = VecDeque::new();
    parser.push(bytes, &mut frames).map_err(|error| error.0.to_owned())?;
    parser.finish().map_err(|error| error.0.to_owned())?;
    Ok(frames.into())
}
