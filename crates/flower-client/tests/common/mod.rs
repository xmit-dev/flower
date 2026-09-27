#![allow(dead_code)]
//! Shared helpers: SSE text builders (like `client.test.ts`'s) and bounded waits.

use std::time::Duration;

use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};

pub fn sse(event: &str, value: &Value) -> String {
    format!("event: {event}\ndata: {value}\n\n")
}

pub fn snapshot(value: Value, revision: u64, sequence: u64) -> String {
    sse(
        "snapshot",
        &json!({ "sequence": sequence, "revision": revision, "value": value }),
    )
}

pub fn patch(value: Value, revision: u64, sequence: u64, path: &str) -> String {
    sse(
        "patch",
        &json!({ "sequence": sequence, "baseSequence": sequence - 1, "revision": revision,
                 "patch": [{ "op": "replace", "path": path, "value": value }] }),
    )
}

pub async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("timed out")
}

/// Up to `count` items; stops at the end of the stream.
pub async fn take<S: Stream + Unpin>(stream: &mut S, count: usize) -> Vec<S::Item> {
    let mut items = Vec::new();
    while items.len() < count {
        match bounded(stream.next()).await {
            Some(item) => items.push(item),
            None => break,
        }
    }
    items
}

pub fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}
