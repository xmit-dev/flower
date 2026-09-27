//! Watches: `watch_deltas`, `watch`, `subscribe` and `wait_until`, the twins of `client.ts`'s
//! watch methods and `watch.ts`'s decoding and JSON Patch application.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{self, BoxStream, Stream, StreamExt};
use http::Method;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::client::{self, FlowerClient, QueryResult};
use crate::error::{Failure, FlowerError};
use crate::json;
use crate::retry::{DEFAULT_INITIAL_DELAY, DEFAULT_MAX_DELAY, backoff};
use crate::sse::{SseError, SseFrame, SseParser};
use crate::transport::{Lane, ResponseBody, media_type};

/// Maximum wire bytes in one SSE event (also bounds an HTTP error body).
pub const MAX_WATCH_EVENT_BYTES: usize = 17 * 1024 * 1024;
/// Maximum UTF-8 bytes of a snapshot or reconstructed value.
pub const MAX_WATCH_VALUE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum operations in one JSON Patch event.
pub const MAX_PATCH_OPERATIONS: usize = 256;
/// `subscribe`'s default stall timeout: any bytes, heartbeats included, count as activity.
pub const DEFAULT_STALL: Duration = Duration::from_secs(45);

/// Local client allowances; never sent to the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchBudgets {
    pub max_event_bytes: usize,
    pub max_value_bytes: usize,
    pub max_patch_operations: usize,
}

impl Default for WatchBudgets {
    fn default() -> Self {
        WatchBudgets {
            max_event_bytes: MAX_WATCH_EVENT_BYTES,
            max_value_bytes: MAX_WATCH_VALUE_BYTES,
            max_patch_operations: MAX_PATCH_OPERATIONS,
        }
    }
}

impl WatchBudgets {
    /// `watchBudgets()`: every limit a positive safe integer.
    pub fn validate(&self) -> Result<(), FlowerError> {
        for (name, value) in [
            ("maxEventBytes", self.max_event_bytes),
            ("maxValueBytes", self.max_value_bytes),
            ("maxPatchOperations", self.max_patch_operations),
        ] {
            if value < 1 || value as u64 > MAX_SAFE {
                return Err(FlowerError::invalid(format!(
                    "{name} must be a positive safe integer"
                )));
            }
        }
        Ok(())
    }
}

const MAX_SAFE: u64 = (1 << 53) - 1;

/// A violation of the watch protocol (TS: `WatchProtocolError`), reported as
/// `WATCH_PROTOCOL_ERROR`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchProtocolError(pub String);

impl std::fmt::Display for WatchProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WatchProtocolError {}

impl From<WatchProtocolError> for FlowerError {
    fn from(error: WatchProtocolError) -> Self {
        FlowerError::new(error.0, 0, "WATCH_PROTOCOL_ERROR")
    }
}

impl From<SseError> for WatchProtocolError {
    fn from(error: SseError) -> Self {
        WatchProtocolError(error.0.to_owned())
    }
}

fn invalid<T>(message: &str) -> Result<T, WatchProtocolError> {
    Err(WatchProtocolError(message.to_owned()))
}

/// An RFC 6902 operation of the subset Flower sends.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum PatchOperation {
    Add { path: String, value: Value },
    Remove { path: String },
    Replace { path: String, value: Value },
}

impl PatchOperation {
    pub fn path(&self) -> &str {
        match self {
            PatchOperation::Add { path, .. }
            | PatchOperation::Remove { path }
            | PatchOperation::Replace { path, .. } => path,
        }
    }
}

/// A raw watch event.
#[derive(Clone, Debug, PartialEq)]
pub enum WatchDelta {
    Snapshot {
        sequence: u64,
        revision: u64,
        value: Value,
    },
    Patch {
        sequence: u64,
        base_sequence: u64,
        revision: u64,
        patch: Vec<PatchOperation>,
    },
}

impl WatchDelta {
    pub fn sequence(&self) -> u64 {
        match self {
            WatchDelta::Snapshot { sequence, .. } | WatchDelta::Patch { sequence, .. } => *sequence,
        }
    }

    pub fn revision(&self) -> u64 {
        match self {
            WatchDelta::Snapshot { revision, .. } | WatchDelta::Patch { revision, .. } => *revision,
        }
    }
}

/// A decoded frame: a delta, or the terminal error event.
#[derive(Clone, Debug)]
pub enum WatchEvent {
    Delta(WatchDelta),
    Error(FlowerError),
}

/// One value of a subscription.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Update<T = Value> {
    pub revision: u64,
    pub value: T,
    /// A full snapshot after (re)connecting; intermediate values may have been skipped.
    pub reset: bool,
}

/// Options of `watch` and `watch_deltas`.
#[derive(Clone, Debug, Default)]
pub struct WatchOptions {
    pub cancel: Option<CancellationToken>,
    /// Replaces the client's credentials for this watch.
    pub credentials: Option<Value>,
    pub budgets: WatchBudgets,
}

/// Reconnection after disconnects, stalls and transient errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reconnect {
    Off,
    On {
        initial_delay: Duration,
        max_delay: Duration,
    },
}

impl Default for Reconnect {
    fn default() -> Self {
        Reconnect::On {
            initial_delay: DEFAULT_INITIAL_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
        }
    }
}

/// Options of `subscribe` and `wait_until`.
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    pub cancel: Option<CancellationToken>,
    pub credentials: Option<Value>,
    pub budgets: WatchBudgets,
    /// Default on, with `backoff(failures, 250 ms, 30 s)` between connections.
    pub reconnect: Reconnect,
    /// Reconnect when no bytes, heartbeats included, arrive for this long. Default 45 s.
    pub stall: Duration,
    /// Skip values older than the newest revision already delivered.
    pub monotonic: bool,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        SubscribeOptions {
            cancel: None,
            credentials: None,
            budgets: WatchBudgets::default(),
            reconnect: Reconnect::default(),
            stall: DEFAULT_STALL,
            monotonic: false,
        }
    }
}

impl SubscribeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    pub fn monotonic(mut self, monotonic: bool) -> Self {
        self.monotonic = monotonic;
        self
    }

    pub fn stall(mut self, stall: Duration) -> Self {
        self.stall = stall;
        self
    }

    pub fn reconnect(mut self, reconnect: Reconnect) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn credentials(mut self, credentials: Value) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn budgets(mut self, budgets: WatchBudgets) -> Self {
        self.budgets = budgets;
        self
    }
}

// ---- Decoding and patches

fn safe_integer(value: Option<&Value>) -> Option<i64> {
    let number = value?.as_number()?;
    if let Some(integer) = number.as_i64() {
        return (integer.unsigned_abs() <= MAX_SAFE).then_some(integer);
    }
    if number.as_u64().is_some() {
        return None;
    }
    let float = number.as_f64()?;
    (float.fract() == 0.0 && float.abs() <= MAX_SAFE as f64).then_some(float as i64)
}

fn is_integer(value: Option<&Value>) -> Option<f64> {
    let float = value?.as_f64()?;
    (float.fract() == 0.0).then_some(float)
}

fn validate_json(value: &Value) -> Result<(), WatchProtocolError> {
    json::check_depth(value, "")
        .map_err(|_| WatchProtocolError("Watch JSON nesting exceeds 128".to_owned()))
}

fn validate_value_size(value: &Value, max_bytes: usize) -> Result<(), WatchProtocolError> {
    if json::stringify_len(value) > max_bytes {
        return invalid("Watch value exceeds maxValueBytes");
    }
    Ok(())
}

fn pointer(path: &str) -> Result<Vec<String>, WatchProtocolError> {
    if path.is_empty() {
        return Ok(Vec::new());
    }
    let Some(rest) = path.strip_prefix('/') else {
        return invalid("Patch path must begin with /");
    };
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() > 128 {
        return invalid("Patch path exceeds 128 levels");
    }
    parts
        .into_iter()
        .map(|part| {
            let bytes = part.as_bytes();
            for (index, &byte) in bytes.iter().enumerate() {
                if byte == b'~' && !matches!(bytes.get(index + 1), Some(b'0' | b'1')) {
                    return invalid("Invalid JSON pointer escape");
                }
            }
            Ok(part.replace("~1", "/").replace("~0", "~"))
        })
        .collect()
}

/// `validatePatch` over raw JSON, returning typed operations.
fn validate_patch(
    value: Option<&Value>,
    max_operations: usize,
) -> Result<Vec<PatchOperation>, WatchProtocolError> {
    let Some(Value::Array(operations)) = value else {
        return invalid("Invalid or oversized watch patch");
    };
    if operations.len() > max_operations {
        return invalid("Invalid or oversized watch patch");
    }
    let mut typed = Vec::with_capacity(operations.len());
    for operation in operations {
        let Some(object) = operation.as_object() else {
            return invalid("Invalid patch operation");
        };
        let op = match object.get("op").and_then(Value::as_str) {
            Some(op @ ("add" | "remove" | "replace")) => op,
            _ => return invalid("Unsupported patch operation"),
        };
        let Some(path) = object.get("path").and_then(Value::as_str) else {
            return invalid("Patch path must be a JSON pointer");
        };
        pointer(path)?;
        if op == "remove" {
            typed.push(PatchOperation::Remove {
                path: path.to_owned(),
            });
            continue;
        }
        let Some(value) = object.get("value") else {
            return invalid("Patch operation has no value");
        };
        validate_json(value)?;
        let (path, value) = (path.to_owned(), value.clone());
        typed.push(if op == "add" {
            PatchOperation::Add { path, value }
        } else {
            PatchOperation::Replace { path, value }
        });
    }
    Ok(typed)
}

/// `decodeWatchEvent`: check one frame against the previous sequence and revision (`-1` before the
/// first event).
pub fn decode_watch_event(
    frame: &SseFrame,
    previous_sequence: i64,
    previous_revision: i64,
    budgets: &WatchBudgets,
) -> Result<WatchEvent, WatchProtocolError> {
    let data = frame.data.as_bytes();
    if json::nesting(data) > json::MAX_PARSE_DEPTH {
        return invalid("Watch JSON nesting exceeds 128");
    }
    let Ok(value) = json::from_slice_deep::<Value>(data) else {
        return invalid("Watch event contains invalid JSON");
    };
    let Some(object) = value.as_object() else {
        return invalid("Invalid watch event");
    };
    if frame.event == "error" {
        let error = object.get("error").and_then(Value::as_object);
        let code = error
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str);
        let message = error
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str);
        let status = is_integer(error.and_then(|error| error.get("status")));
        return match (code, message, status) {
            (Some(code), Some(message), Some(status)) if (100.0..=599.0).contains(&status) => Ok(
                WatchEvent::Error(FlowerError::new(message, status as u16, code).with_failure(
                    Failure::from_value(error.and_then(|error| error.get("failure"))),
                )),
            ),
            _ => invalid("Invalid terminal watch error"),
        };
    }
    if frame.event != "snapshot" && frame.event != "patch" {
        return invalid("Unknown watch event type");
    }
    let sequence = match safe_integer(object.get("sequence")) {
        Some(sequence) if sequence >= 0 && sequence > previous_sequence => sequence,
        _ => return invalid("Watch event sequence must increase"),
    };
    if let Some(id) = &frame.id
        && *id != sequence.to_string()
    {
        return invalid("Watch event ID does not match its sequence");
    }
    let revision = match safe_integer(object.get("revision")) {
        Some(revision) if revision >= previous_revision && revision >= 0 => revision,
        _ => return invalid("Invalid watch revision"),
    };
    if frame.event == "snapshot" {
        let Some(value) = object.get("value") else {
            return invalid("Watch snapshot has no value");
        };
        validate_json(value)?;
        validate_value_size(value, budgets.max_value_bytes)?;
        return Ok(WatchEvent::Delta(WatchDelta::Snapshot {
            sequence: sequence as u64,
            revision: revision as u64,
            value: value.clone(),
        }));
    }
    if sequence != previous_sequence + 1 {
        return invalid("Watch patch sequence is not consecutive");
    }
    if previous_sequence < 0
        || object.get("baseSequence").and_then(Value::as_f64) != Some(previous_sequence as f64)
    {
        return invalid("Watch patch has the wrong base sequence");
    }
    let patch = validate_patch(object.get("patch"), budgets.max_patch_operations)?;
    Ok(WatchEvent::Delta(WatchDelta::Patch {
        sequence: sequence as u64,
        base_sequence: previous_sequence as u64,
        revision: revision as u64,
        patch,
    }))
}

fn index_for(length: usize, key: &str, add: bool) -> Result<usize, WatchProtocolError> {
    if add && key == "-" {
        return Ok(length);
    }
    let bytes = key.as_bytes();
    let canonical = !bytes.is_empty()
        && bytes.iter().all(u8::is_ascii_digit)
        && (bytes[0] != b'0' || bytes.len() == 1);
    if !canonical {
        return invalid("Invalid patch array index");
    }
    match key.parse::<u64>() {
        Ok(index)
            if index <= MAX_SAFE
                && (index as usize) <= length
                && (add || index as usize != length) =>
        {
            Ok(index as usize)
        }
        _ => invalid("Patch array index out of bounds"),
    }
}

/// `applyWatchPatch`: apply `add`/`remove`/`replace` in place. Own JSON properties only;
/// `__proto__` and `constructor` are ordinary keys. On error `value` may be partly patched.
pub fn apply_patch_in_place(
    value: &mut Value,
    operations: &[PatchOperation],
    budgets: &WatchBudgets,
) -> Result<(), WatchProtocolError> {
    if operations.len() > budgets.max_patch_operations {
        return invalid("Invalid or oversized watch patch");
    }
    let mut paths = Vec::with_capacity(operations.len());
    for operation in operations {
        paths.push(pointer(operation.path())?);
        if let PatchOperation::Add { value, .. } | PatchOperation::Replace { value, .. } = operation
        {
            validate_json(value)?;
        }
    }
    for (operation, parts) in operations.iter().zip(paths) {
        let Some((last, parents)) = parts.split_last() else {
            match operation {
                PatchOperation::Remove { .. } => {
                    return invalid("Cannot remove the entire watched value");
                }
                PatchOperation::Add { value: next, .. }
                | PatchOperation::Replace { value: next, .. } => *value = next.clone(),
            }
            continue;
        };
        let mut parent: &mut Value = value;
        for key in parents {
            parent = match parent {
                Value::Array(items) => {
                    let index = index_for(items.len(), key, false)?;
                    &mut items[index]
                }
                Value::Object(map) => match map.get_mut(key.as_str()) {
                    Some(child) => child,
                    None => return invalid("Patch parent does not exist"),
                },
                _ => return invalid("Patch parent is not a container"),
            };
        }
        match parent {
            Value::Array(items) => {
                let index = index_for(
                    items.len(),
                    last,
                    matches!(operation, PatchOperation::Add { .. }),
                )?;
                match operation {
                    PatchOperation::Remove { .. } => {
                        items.remove(index);
                    }
                    PatchOperation::Add { value, .. } => items.insert(index, value.clone()),
                    PatchOperation::Replace { value, .. } => items[index] = value.clone(),
                }
            }
            Value::Object(map) => {
                if !matches!(operation, PatchOperation::Add { .. })
                    && !map.contains_key(last.as_str())
                {
                    return invalid("Patch target does not exist");
                }
                match operation {
                    PatchOperation::Remove { .. } => {
                        shift_remove(map, last);
                    }
                    PatchOperation::Add { value, .. } | PatchOperation::Replace { value, .. } => {
                        map.insert(last.clone(), value.clone());
                    }
                }
            }
            _ => return invalid("Patch target is not a container"),
        }
    }
    validate_json(value)?;
    validate_value_size(value, budgets.max_value_bytes)
}

/// Remove keeping the other keys' order: serde_json's `remove` swaps the last key into place under
/// `preserve_order`, and `shift_remove` only exists with that feature.
fn shift_remove(map: &mut Map<String, Value>, key: &str) {
    let entries = std::mem::take(map);
    *map = entries
        .into_iter()
        .filter(|(name, _)| name != key)
        .collect();
}

/// [`apply_patch_in_place`] on a copy.
pub fn apply_patch(
    value: &Value,
    operations: &[PatchOperation],
    budgets: &WatchBudgets,
) -> Result<Value, WatchProtocolError> {
    let mut result = value.clone();
    apply_patch_in_place(&mut result, operations, budgets)?;
    Ok(result)
}

/// The value after one delta.
fn advance(
    baseline: &mut Value,
    delta: &WatchDelta,
    budgets: &WatchBudgets,
) -> Result<(), FlowerError> {
    match delta {
        WatchDelta::Snapshot { value, .. } => {
            *baseline = value.clone();
            Ok(())
        }
        WatchDelta::Patch { patch, .. } => {
            apply_patch_in_place(baseline, patch, budgets).map_err(Into::into)
        }
    }
}

// ---- Connections

/// When bytes last arrived, for the stall timer.
struct Activity {
    base: Instant,
    last: AtomicU64,
}

impl Activity {
    fn new() -> Self {
        Activity {
            base: Instant::now(),
            last: AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        self.last
            .store(self.base.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    async fn stalled(&self, stall: Duration) {
        loop {
            let deadline =
                self.base + Duration::from_nanos(self.last.load(Ordering::Relaxed)) + stall;
            if Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep_until(deadline).await;
        }
    }
}

/// One open SSE response.
struct Connection {
    body: ResponseBody,
    parser: SseParser,
    frames: VecDeque<SseFrame>,
    failed: Option<WatchProtocolError>,
    sequence: i64,
    revision: i64,
    budgets: WatchBudgets,
    ended: bool,
}

impl Connection {
    /// POST `/v1/watch`; errors before streaming become `FlowerError`s.
    async fn open(
        client: &FlowerClient,
        name: &str,
        args: &Bytes,
        credentials: Option<&Value>,
        budgets: WatchBudgets,
    ) -> Result<Connection, FlowerError> {
        let credentials = client.authorization(credentials).await?;
        let body = client::body(name, args, None, credentials.as_ref(), None);
        let request = client::request(
            Method::POST,
            format!("{}/v1/watch", client.url()),
            body,
            Lane::Watch,
            None,
        )?;
        let response = client.transport().send(request).await?;
        let status = response.status;
        if !status.is_success() {
            let status_text = response.status_text();
            let bytes = response
                .body
                .collect(budgets.max_event_bytes, || {
                    FlowerError::new(
                        "Watch HTTP error body exceeds maxEventBytes",
                        status.as_u16(),
                        "HTTP_ERROR",
                    )
                })
                .await?;
            return Err(FlowerError::from_watch_response(
                status.as_u16(),
                status_text,
                &bytes,
            ));
        }
        if media_type(&response.headers).as_deref() != Some("text/event-stream")
            || matches!(status.as_u16(), 204 | 205)
        {
            return Err(FlowerError::new(
                "Expected a text/event-stream response body",
                status.as_u16(),
                "WATCH_PROTOCOL_ERROR",
            ));
        }
        Ok(Connection {
            body: response.body,
            parser: SseParser::new(budgets.max_event_bytes),
            frames: VecDeque::new(),
            failed: None,
            sequence: -1,
            revision: -1,
            budgets,
            ended: false,
        })
    }

    /// The next delta; `None` at a clean end after a snapshot.
    async fn next(
        &mut self,
        activity: Option<&Activity>,
    ) -> Option<Result<WatchDelta, FlowerError>> {
        loop {
            if let Some(frame) = self.frames.pop_front() {
                return Some(
                    match decode_watch_event(&frame, self.sequence, self.revision, &self.budgets) {
                        Ok(WatchEvent::Delta(delta)) => {
                            self.sequence = delta.sequence() as i64;
                            self.revision = delta.revision() as i64;
                            Ok(delta)
                        }
                        Ok(WatchEvent::Error(error)) => Err(error),
                        Err(error) => Err(error.into()),
                    },
                );
            }
            if let Some(error) = self.failed.take() {
                return Some(Err(error.into()));
            }
            if self.ended {
                return None;
            }
            let chunk = self.body.chunk().await;
            if let Some(activity) = activity {
                activity.touch();
            }
            match chunk {
                Some(Ok(chunk)) => {
                    if let Err(error) = self.parser.push(&chunk, &mut self.frames) {
                        self.failed = Some(error.into());
                    }
                }
                Some(Err(error)) => return Some(Err(error)),
                None => {
                    self.ended = true;
                    if let Err(error) = self.parser.finish() {
                        self.failed = Some(error.into());
                    } else if self.frames.is_empty() && self.sequence < 0 {
                        return Some(Err(FlowerError::new(
                            "Watch ended before its initial snapshot",
                            0,
                            "WATCH_ENDED",
                        )));
                    }
                }
            }
        }
    }
}

fn cancelled(cancel: &Option<CancellationToken>) -> bool {
    cancel.as_ref().is_some_and(CancellationToken::is_cancelled)
}

async fn until_cancelled(cancel: &Option<CancellationToken>) {
    match cancel {
        Some(cancel) => cancel.cancelled().await,
        None => std::future::pending().await,
    }
}

// ---- Streams

/// A typed stream over a JSON stream: each item decoded into `T`; a decoding failure ends it.
pub struct Typed<S, T> {
    inner: BoxStream<'static, Result<S, FlowerError>>,
    done: bool,
    _type: PhantomData<fn() -> T>,
}

impl<S, T> Typed<S, T> {
    fn new(inner: BoxStream<'static, Result<S, FlowerError>>) -> Self {
        Typed {
            inner,
            done: false,
            _type: PhantomData,
        }
    }

    fn failed(error: FlowerError) -> Self
    where
        S: Send + 'static,
    {
        Self::new(stream::once(async move { Err(error) }).boxed())
    }
}

/// Items that carry a JSON value to decode.
pub trait Decodable {
    type Output<T>;
    fn decode<T: DeserializeOwned>(self) -> Result<Self::Output<T>, FlowerError>;
}

impl Decodable for Update<Value> {
    type Output<T> = Update<T>;
    fn decode<T: DeserializeOwned>(self) -> Result<Update<T>, FlowerError> {
        Ok(Update {
            revision: self.revision,
            value: serde_json::from_value(self.value).map_err(|error| {
                FlowerError::decode(format!("Invalid watched value: {error}"), 0)
            })?,
            reset: self.reset,
        })
    }
}

impl Decodable for QueryResult<Value> {
    type Output<T> = QueryResult<T>;
    fn decode<T: DeserializeOwned>(self) -> Result<QueryResult<T>, FlowerError> {
        Ok(QueryResult {
            revision: self.revision,
            value: serde_json::from_value(self.value).map_err(|error| {
                FlowerError::decode(format!("Invalid watched value: {error}"), 0)
            })?,
        })
    }
}

impl<S: Decodable, T: DeserializeOwned> Stream for Typed<S, T> {
    type Item = Result<S::Output<T>, FlowerError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(item)) => {
                let item = item.and_then(S::decode::<T>);
                if item.is_err() {
                    self.done = true;
                }
                Poll::Ready(Some(item))
            }
        }
    }
}

/// `subscribe`'s stream. Dropping it closes the connection.
pub type Subscription<T = Value> = Typed<Update<Value>, T>;
/// `watch`'s stream. Dropping it closes the connection.
pub type Watch<T = Value> = Typed<QueryResult<Value>, T>;

/// `watch_deltas`'s stream. Dropping it closes the connection.
pub type Deltas = BoxStream<'static, Result<WatchDelta, FlowerError>>;

struct Watcher {
    client: FlowerClient,
    name: String,
    args: Bytes,
    options: WatchOptions,
    connection: Option<Connection>,
    started: bool,
    done: bool,
}

impl Watcher {
    async fn delta(&mut self) -> Option<Result<WatchDelta, FlowerError>> {
        if self.done || cancelled(&self.options.cancel) {
            self.done = true;
            return None;
        }
        let cancel = self.options.cancel.clone();
        if !self.started {
            self.started = true;
            let opened = tokio::select! {
                biased;
                _ = until_cancelled(&cancel) => None,
                opened = Connection::open(&self.client, &self.name, &self.args, self.options.credentials.as_ref(), self.options.budgets) => Some(opened),
            };
            match opened {
                None => {
                    self.done = true;
                    return None;
                }
                Some(Err(error)) => {
                    self.done = true;
                    return Some(Err(error));
                }
                Some(Ok(connection)) => self.connection = Some(connection),
            }
        }
        let connection = self.connection.as_mut()?;
        let next = tokio::select! {
            biased;
            _ = until_cancelled(&cancel) => None,
            next = connection.next(None) => Some(next),
        };
        match next {
            None | Some(None) => {
                self.done = true;
                self.connection = None;
                None
            }
            Some(Some(Err(error))) => {
                self.done = true;
                self.connection = None;
                Some(Err(error))
            }
            Some(Some(Ok(delta))) => Some(Ok(delta)),
        }
    }
}

struct Subscriber {
    client: FlowerClient,
    name: String,
    args: Bytes,
    options: SubscribeOptions,
    connection: Option<Connection>,
    baseline: Value,
    reset: bool,
    failures: u32,
    newest: i64,
    connected: bool,
    done: bool,
    activity: Arc<Activity>,
}

enum Step {
    Cancelled,
    Stalled,
    Next(Option<Result<WatchDelta, FlowerError>>),
}

impl Subscriber {
    /// A disconnect: `Some(error)` ends the subscription with it, `None` reconnects.
    fn fail(&mut self, error: FlowerError) -> Option<FlowerError> {
        self.connection = None;
        if self.options.reconnect == Reconnect::Off || !error.is_transient() {
            self.done = true;
            Some(error)
        } else {
            None
        }
    }

    async fn next(&mut self) -> Option<Result<Update<Value>, FlowerError>> {
        let cancel = self.options.cancel.clone();
        loop {
            if self.done || cancelled(&cancel) {
                self.done = true;
                self.connection = None;
                return None;
            }
            if self.connection.is_none() {
                if self.connected {
                    let Reconnect::On {
                        initial_delay,
                        max_delay,
                    } = self.options.reconnect
                    else {
                        self.done = true;
                        return None;
                    };
                    let delay = backoff(self.failures, initial_delay, max_delay);
                    self.failures += 1;
                    tokio::select! {
                        biased;
                        _ = until_cancelled(&cancel) => {
                            self.done = true;
                            return None;
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                self.connected = true;
                self.activity.touch();
                let opened = tokio::select! {
                    biased;
                    _ = until_cancelled(&cancel) => Err(None),
                    _ = self.activity.stalled(self.options.stall) => Err(Some(stalled())),
                    opened = Connection::open(&self.client, &self.name, &self.args, self.options.credentials.as_ref(), self.options.budgets) => opened.map_err(Some),
                };
                match opened {
                    Ok(connection) => {
                        self.connection = Some(connection);
                        self.reset = true;
                        self.baseline = Value::Null;
                    }
                    Err(None) => {
                        self.done = true;
                        return None;
                    }
                    Err(Some(error)) => {
                        if let Some(error) = self.fail(error) {
                            return Some(Err(error));
                        }
                        continue;
                    }
                }
            }
            let connection = self.connection.as_mut().expect("connected");
            let step = tokio::select! {
                biased;
                _ = until_cancelled(&cancel) => Step::Cancelled,
                _ = self.activity.stalled(self.options.stall) => Step::Stalled,
                next = connection.next(Some(&self.activity)) => Step::Next(next),
            };
            let delta = match step {
                Step::Cancelled => {
                    self.done = true;
                    self.connection = None;
                    return None;
                }
                Step::Stalled => {
                    if let Some(error) = self.fail(stalled()) {
                        return Some(Err(error));
                    }
                    continue;
                }
                Step::Next(None) => {
                    self.connection = None;
                    if self.options.reconnect == Reconnect::Off {
                        self.done = true;
                        return None;
                    }
                    continue;
                }
                Step::Next(Some(Err(error))) => {
                    if let Some(error) = self.fail(error) {
                        return Some(Err(error));
                    }
                    continue;
                }
                Step::Next(Some(Ok(delta))) => delta,
            };
            if let Err(error) = advance(&mut self.baseline, &delta, &self.options.budgets) {
                if let Some(error) = self.fail(error) {
                    return Some(Err(error));
                }
                continue;
            }
            self.failures = 0;
            let revision = delta.revision() as i64;
            if self.options.monotonic && revision < self.newest {
                continue;
            }
            self.newest = self.newest.max(revision);
            let update = Update {
                revision: revision as u64,
                value: self.baseline.clone(),
                reset: self.reset,
            };
            self.reset = false;
            return Some(Ok(update));
        }
    }
}

fn stalled() -> FlowerError {
    FlowerError::new("Watch stalled", 0, "WATCH_STALLED")
}

impl FlowerClient {
    /// Raw snapshot and patch events of one connection (TS: `watchDeltas`). Errors end the stream;
    /// cancelling ends it cleanly. Argument and budget errors are its first item.
    pub fn watch_deltas<A: Serialize + ?Sized>(
        &self,
        name: &str,
        args: &A,
        options: WatchOptions,
    ) -> Deltas {
        let watcher = match self.watcher(name, args, options) {
            Ok(watcher) => watcher,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };
        stream::unfold(watcher, |mut watcher| async move {
            let item = watcher.delta().await?;
            Some((item, watcher))
        })
        .boxed()
    }

    fn watcher<A: Serialize + ?Sized>(
        &self,
        name: &str,
        args: &A,
        options: WatchOptions,
    ) -> Result<Watcher, FlowerError> {
        options.budgets.validate()?;
        Ok(Watcher {
            client: self.clone(),
            name: name.to_owned(),
            args: client::encode_args(args)?,
            options,
            connection: None,
            started: false,
            done: false,
        })
    }

    /// A query's values over one SSE connection (TS: `watch`): patches applied, no reconnects.
    pub fn watch<A, T>(&self, name: &str, args: &A, options: WatchOptions) -> Watch<T>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let budgets = options.budgets;
        let watcher = match self.watcher(name, args, options) {
            Ok(watcher) => watcher,
            Err(error) => return Typed::failed(error),
        };
        Typed::new(
            stream::unfold(
                (watcher, Value::Null),
                move |(mut watcher, mut baseline)| async move {
                    let item = match watcher.delta().await? {
                        Ok(delta) => match advance(&mut baseline, &delta, &budgets) {
                            Ok(()) => Ok(QueryResult {
                                revision: delta.revision(),
                                value: baseline.clone(),
                            }),
                            Err(error) => {
                                watcher.done = true;
                                watcher.connection = None;
                                Err(error)
                            }
                        },
                        Err(error) => Err(error),
                    };
                    Some((item, (watcher, baseline)))
                },
            )
            .boxed(),
        )
    }

    /// A live value that survives disconnects (TS: `subscribe`): reconnects with backoff after
    /// disconnects, stalls and transient errors, and marks each fresh snapshot as a reset. Answers
    /// (4xx other than 408/425/429, failures) and protocol violations end it with that error;
    /// cancelling or dropping ends it cleanly. Argument and option errors are its first item.
    pub fn subscribe<A, T>(
        &self,
        name: &str,
        args: &A,
        options: SubscribeOptions,
    ) -> Subscription<T>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let subscriber = match self.subscriber(name, args, options) {
            Ok(subscriber) => subscriber,
            Err(error) => return Typed::failed(error),
        };
        Typed::new(
            stream::unfold(subscriber, |mut subscriber| async move {
                let item = subscriber.next().await?;
                Some((item, subscriber))
            })
            .boxed(),
        )
    }

    fn subscriber<A: Serialize + ?Sized>(
        &self,
        name: &str,
        args: &A,
        options: SubscribeOptions,
    ) -> Result<Subscriber, FlowerError> {
        if options.stall.is_zero() || options.stall.as_millis() as u64 > MAX_SAFE {
            return Err(FlowerError::invalid(
                "stallMs must be a positive safe integer",
            ));
        }
        options.budgets.validate()?;
        Ok(Subscriber {
            client: self.clone(),
            name: name.to_owned(),
            args: client::encode_args(args)?,
            options,
            connection: None,
            baseline: Value::Null,
            reset: true,
            failures: 0,
            newest: -1,
            connected: false,
            done: false,
            activity: Arc::new(Activity::new()),
        })
    }

    /// Resolve with the first update whose value satisfies `predicate`, reconnecting as needed.
    /// Fails with `WATCH_ENDED` if the subscription ends, or `ABORTED` when cancelled.
    pub async fn wait_until<A, T, P>(
        &self,
        name: &str,
        args: &A,
        mut predicate: P,
        options: SubscribeOptions,
    ) -> Result<Update<T>, FlowerError>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
        P: FnMut(&T) -> bool,
    {
        let cancel = options.cancel.clone();
        let mut updates = self.subscribe::<A, T>(name, args, options);
        while let Some(update) = updates.next().await {
            let update = update?;
            if predicate(&update.value) {
                return Ok(update);
            }
        }
        if cancelled(&cancel) {
            return Err(FlowerError::aborted());
        }
        Err(FlowerError::new("The subscription ended", 0, "WATCH_ENDED"))
    }

    /// [`FlowerClient::wait_until`] with the TS default predicate `Boolean` (JavaScript truthiness).
    pub async fn wait_until_truthy<A: Serialize + ?Sized>(
        &self,
        name: &str,
        args: &A,
        options: SubscribeOptions,
    ) -> Result<Update<Value>, FlowerError> {
        self.wait_until(name, args, json::truthy, options).await
    }
}
