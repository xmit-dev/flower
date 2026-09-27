//! An in-memory Flower for tests: the queue methods `queue.http()` generates and the
//! `external().http()` methods of `docs/reactive-worker.ts`, ported from `sdk/temporal.ts` and
//! `sdk/external.ts`, plus [`FakeClient`], a [`QueueClient`] over it that retries, reconnects and
//! records calls the way `sdk/client.ts` does. Time is Flower's own, frozen until
//! [`FakeFlower::advance`] like `testDatabase`, or following the worker's [`Clock`] like the TS tests'
//! `wallClock(db)`.

mod external;
mod queue;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};
use tokio::sync::watch;

use crate::client::{ClientError, Predicate, QueueClient, RetryPolicy, backoff};
use crate::clock::Clock;

pub use external::{Digest, shard_of};
pub use queue::{FakeQueue, QueueConfig, QueueRetry};

/// A client error as the TS SDK would raise it: a `FlowerError` (status, code, message,
/// failure), or a network failure (`TypeError: fetch failed`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakeError {
    pub status: u16,
    pub code: String,
    pub message: String,
    /// `(failure.code, failure.message)`.
    pub failure: Option<(String, String)>,
    /// A network failure rather than a `FlowerError`.
    pub network: bool,
}

impl FakeError {
    /// An HTTP error answer without a failure.
    pub fn http(status: u16, code: &str, message: &str) -> Self {
        FakeError {
            status,
            code: code.into(),
            message: message.into(),
            failure: None,
            network: false,
        }
    }

    /// A method's own failure: `422 EVALUATION_FAILED` with `failure`.
    pub fn failure(code: &str, message: &str) -> Self {
        FakeError {
            status: 422,
            code: "EVALUATION_FAILED".into(),
            message: message.into(),
            failure: Some((code.into(), message.into())),
            network: false,
        }
    }

    /// An error answer with both a code and a failure, like `403 FORBIDDEN`.
    pub fn denied(status: u16, code: &str, message: &str, failure: (&str, &str)) -> Self {
        FakeError {
            failure: Some((failure.0.into(), failure.1.into())),
            ..FakeError::http(status, code, message)
        }
    }

    /// `new TypeError("fetch failed")`: transient.
    pub fn fetch_failed() -> Self {
        FakeError {
            status: 0,
            code: String::new(),
            message: "fetch failed".into(),
            failure: None,
            network: true,
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        let message = message.into();
        FakeError::failure("INVALID_ARGUMENT", &message)
    }
}

impl fmt::Display for FakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FakeError {}

impl ClientError for FakeError {
    fn status(&self) -> u16 {
        self.status
    }

    fn code(&self) -> &str {
        &self.code
    }

    fn failure_code(&self) -> Option<&str> {
        self.failure.as_ref().map(|(code, _)| code.as_str())
    }

    fn is_transient(&self) -> bool {
        if self.network {
            return true;
        }
        if self.failure.is_some() {
            return false;
        }
        if self.status == 0 {
            return self.code == "WATCH_STALLED" || self.code == "WATCH_ENDED";
        }
        matches!(self.status, 408 | 425 | 429) || self.status >= 500
    }

    fn describe(&self) -> String {
        match (&self.failure, self.network) {
            (_, true) => self.message.clone(),
            (Some((code, message)), _) => format!("{code}: {message}"),
            (None, _) => format!("{}: {}", self.code, self.message),
        }
    }
}

/// One call a [`FakeClient`] made.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    pub name: String,
    /// The arguments exactly as sent.
    pub args: String,
    /// The mutation's request ID; `None` for a watch.
    pub request_id: Option<String>,
    pub watch: bool,
}

impl Call {
    pub fn args(&self) -> Value {
        serde_json::from_str(&self.args).expect("recorded arguments are JSON")
    }
}

enum Now {
    Frozen(i64),
    Follow(i64),
}

pub(crate) struct World {
    now: Now,
    clock: Clock,
    pub(crate) queues: BTreeMap<String, FakeQueue>,
    pub(crate) digest: Option<Digest>,
    receipts: HashMap<String, Value>,
    /// Request IDs answered from their receipt (`duplicate: true`), in order.
    replays: Vec<String>,
    /// What `ctx.history()` returns: null unless retry retention is initialized.
    pub(crate) history: Option<Value>,
}

impl World {
    pub(crate) fn now(&self) -> i64 {
        match self.now {
            Now::Frozen(now) => now,
            Now::Follow(offset) => self.clock.now_ms() + offset,
        }
    }

    fn call(&mut self, name: &str, args: &Value) -> Result<Value, FakeError> {
        let now = self.now();
        let history = self.history.clone();
        if let Some((prefix, method)) = name.rsplit_once('.') {
            if let Some(queue) = self.queues.get_mut(prefix) {
                return queue.call(method, args, now, history.as_ref());
            }
            if let Some(digest) = &mut self.digest
                && let Some(value) = digest.call(name, args, now)
            {
                return value;
            }
        }
        Err(FakeError::http(
            404,
            "METHOD_NOT_FOUND",
            &format!("No method {name}"),
        ))
    }
}

/// The in-memory Flower. Cheap to clone; clones share the database.
#[derive(Clone)]
pub struct FakeFlower {
    world: Arc<Mutex<World>>,
    changes: Arc<watch::Sender<u64>>,
    clock: Clock,
    /// Request IDs are unique across clients, as random UUIDs are.
    requests: Arc<AtomicU64>,
}

impl FakeFlower {
    /// An empty database whose time starts at `clock`'s and stays there until advanced.
    pub fn new(clock: Clock) -> Self {
        let now = clock.now_ms();
        FakeFlower {
            world: Arc::new(Mutex::new(World {
                now: Now::Frozen(now),
                clock: clock.clone(),
                queues: BTreeMap::new(),
                digest: None,
                receipts: HashMap::new(),
                replays: Vec::new(),
                history: None,
            })),
            changes: Arc::new(watch::channel(0).0),
            clock,
            requests: Arc::new(AtomicU64::new(0)),
        }
    }

    /// `examples/workers.ts`: queue `jobs` with every method, 10 s leases up to 30 s, five attempts.
    pub fn workers(clock: Clock) -> Self {
        let flower = FakeFlower::new(clock);
        flower.add_queue("jobs", QueueConfig::workers());
        flower
    }

    /// `docs/reactive-worker.ts`: documents and the `digest` external value tracking them.
    pub fn reactive(clock: Clock) -> Self {
        let flower = FakeFlower::new(clock);
        flower.world.lock().digest = Some(Digest::default());
        flower
    }

    pub fn add_queue(&self, prefix: &str, config: QueueConfig) {
        self.world
            .lock()
            .queues
            .insert(prefix.to_owned(), FakeQueue::new(config));
    }

    /// Leases carry this history and reports must present it (retry retention initialized).
    pub fn set_history(&self, history: Value) {
        self.world.lock().history = Some(history);
    }

    /// The worker's clock, which [`FakeClient`] reads for `until`.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Flower's time: `db.now`.
    pub fn now(&self) -> i64 {
        self.world.lock().now()
    }

    /// Move Flower's time forward: `db.advance(ms)`.
    pub fn advance(&self, ms: i64) {
        {
            let mut world = self.world.lock();
            world.now = match world.now {
                Now::Frozen(now) => Now::Frozen(now + ms),
                Now::Follow(offset) => Now::Follow(offset + ms),
            };
        }
        self.changed();
    }

    /// Keep Flower's time in step with the worker's clock from now on (`wallClock(db)`).
    pub fn follow_clock(&self) {
        {
            let mut world = self.world.lock();
            let now = world.now();
            let offset = now - world.clock.now_ms();
            world.now = Now::Follow(offset);
        }
        self.changed();
    }

    /// Freeze Flower's time where it is.
    pub fn freeze(&self) {
        let mut world = self.world.lock();
        let now = world.now();
        world.now = Now::Frozen(now);
    }

    fn changed(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// Run a method directly, like `db.mutate` / `db.query`.
    pub fn call(&self, name: &str, args: Value) -> Result<Value, FakeError> {
        let result = self.world.lock().call(name, &args);
        if result.is_ok() {
            self.changed();
        }
        result
    }

    /// Run a mutation that must succeed.
    pub fn mutate(&self, name: &str, args: Value) -> Value {
        self.call(name, args.clone())
            .unwrap_or_else(|error| panic!("{name}({args}) failed: {error:?}"))
    }

    /// Run a query that must succeed.
    pub fn query(&self, name: &str, args: Value) -> Value {
        self.mutate(name, args)
    }

    /// `db.mutate("jobs.enqueue", { id, payload: { id } })` for each id.
    pub fn enqueue(&self, ids: &[&str]) {
        for id in ids {
            self.mutate("jobs.enqueue", json!({ "id": id, "payload": { "id": id } }));
        }
    }

    /// `jobs.get`.
    pub fn job(&self, id: &str) -> Value {
        let job = self.query("jobs.get", json!({ "id": id }));
        assert!(!job.is_null(), "no job {id}");
        job
    }

    /// The request IDs Flower answered from an earlier attempt's receipt (a reply with
    /// `duplicate: true`), in order.
    pub fn replays(&self) -> Vec<String> {
        self.world.lock().replays.clone()
    }

    /// A client over this database.
    pub fn client(&self) -> FakeClient {
        FakeClient {
            inner: Arc::new(ClientInner {
                flower: self.clone(),
                calls: Mutex::new(Vec::new()),
                before: Mutex::new(None),
                after: Mutex::new(None),
            }),
        }
    }

    fn mutate_once(&self, name: &str, args: &Value, request_id: &str) -> Result<Value, FakeError> {
        let result = {
            let mut world = self.world.lock();
            if let Some(receipt) = world.receipts.get(request_id).cloned() {
                world.replays.push(request_id.to_owned());
                return Ok(receipt);
            }
            let result = world.call(name, args);
            if let Ok(value) = &result {
                world.receipts.insert(request_id.to_owned(), value.clone());
            }
            result
        };
        if result.is_ok() {
            self.changed();
        }
        result
    }
}

type Before = Arc<dyn Fn(&Call) -> Option<FakeError> + Send + Sync>;
type After = Arc<dyn Fn(&Call, &Result<Value, FakeError>) -> Option<FakeError> + Send + Sync>;

struct ClientInner {
    flower: FakeFlower,
    calls: Mutex<Vec<Call>>,
    before: Mutex<Option<Before>>,
    after: Mutex<Option<After>>,
}

/// A [`QueueClient`] over a [`FakeFlower`], like a `FlowerClient` over `db.fetch`: mutations retry
/// transient failures with one request ID (Flower answers a repeated ID with the first result),
/// watches reconnect on transient errors, and every attempt and connection is recorded.
#[derive(Clone)]
pub struct FakeClient {
    inner: Arc<ClientInner>,
}

impl FakeClient {
    /// Fail an attempt or a watch connection before it reaches Flower.
    pub fn before(self, hook: impl Fn(&Call) -> Option<FakeError> + Send + Sync + 'static) -> Self {
        *self.inner.before.lock() = Some(Arc::new(hook));
        self
    }

    /// Replace an attempt's answer once Flower ran it (a lost reply, say).
    pub fn after(
        self,
        hook: impl Fn(&Call, &Result<Value, FakeError>) -> Option<FakeError> + Send + Sync + 'static,
    ) -> Self {
        *self.inner.after.lock() = Some(Arc::new(hook));
        self
    }

    /// Every attempt and watch connection so far.
    pub fn calls(&self) -> Vec<Call> {
        self.inner.calls.lock().clone()
    }

    /// The attempts at one method (watches excluded).
    pub fn calls_to(&self, name: &str) -> Vec<Call> {
        self.calls()
            .into_iter()
            .filter(|call| call.name == name && !call.watch)
            .collect()
    }

    pub fn flower(&self) -> &FakeFlower {
        &self.inner.flower
    }

    fn record(&self, call: &Call) -> Option<FakeError> {
        self.inner.calls.lock().push(call.clone());
        let before = self.inner.before.lock().clone();
        before.and_then(|hook| hook(call))
    }

    fn attempt(&self, call: &Call, args: &Value) -> Result<Value, FakeError> {
        if let Some(error) = self.record(call) {
            return Err(error);
        }
        let result = self.inner.flower.mutate_once(
            &call.name,
            args,
            call.request_id.as_deref().unwrap_or_default(),
        );
        let after = self.inner.after.lock().clone();
        match after.and_then(|hook| hook(call, &result)) {
            Some(error) => Err(error),
            None => result,
        }
    }
}

impl QueueClient for FakeClient {
    type Error = FakeError;

    async fn mutate(
        &self,
        name: &str,
        args: Box<RawValue>,
        retry: RetryPolicy,
    ) -> Result<Value, FakeError> {
        let request_id = format!(
            "request-{}",
            self.inner.flower.requests.fetch_add(1, Ordering::Relaxed) + 1
        );
        let call = Call {
            name: name.to_owned(),
            args: args.get().to_owned(),
            request_id: Some(request_id),
            watch: false,
        };
        let value: Value = serde_json::from_str(args.get()).expect("arguments are JSON");
        let attempts = retry.attempts.unwrap_or(8);
        let initial = retry.initial_delay_ms.unwrap_or(250);
        let max = retry.max_delay_ms.unwrap_or(30_000);
        let mut attempt = 0;
        loop {
            // A round trip: other tasks run before the answer comes, as they would over the network.
            tokio::task::yield_now().await;
            match self.attempt(&call, &value) {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if !error.is_transient() || attempt + 1 >= attempts {
                        return Err(error);
                    }
                    let delay = backoff(attempt, initial, max);
                    if retry.until.is_some_and(|until| {
                        self.inner.flower.clock.now_ms() + delay as i64 >= until
                    }) {
                        return Err(error);
                    }
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn wait_until(
        &self,
        name: &str,
        args: Box<RawValue>,
        predicate: Predicate,
    ) -> Result<Value, FakeError> {
        let call = Call {
            name: name.to_owned(),
            args: args.get().to_owned(),
            request_id: None,
            watch: true,
        };
        let value: Value = serde_json::from_str(args.get()).expect("arguments are JSON");
        let flower = &self.inner.flower;
        let mut failures = 0;
        loop {
            tokio::task::yield_now().await;
            let mut changes = flower.changes.subscribe();
            let connected = match self.record(&call) {
                Some(error) => Err(error),
                None => Ok(()),
            };
            let error = match connected {
                Err(error) => error,
                Ok(()) => loop {
                    changes.borrow_and_update();
                    let following = matches!(flower.world.lock().now, Now::Follow(_));
                    match flower.world.lock().call(name, &value) {
                        Ok(current) => {
                            failures = 0;
                            if predicate(&current) {
                                return Ok(current);
                            }
                        }
                        Err(error) => break error,
                    }
                    if following {
                        tokio::select! {
                            _ = changes.changed() => {}
                            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                        }
                    } else {
                        let _ = changes.changed().await;
                    }
                },
            };
            if !error.is_transient() {
                return Err(error);
            }
            tokio::time::sleep(Duration::from_millis(backoff(failures, 250, 30_000))).await;
            failures += 1;
        }
    }
}

// ---- argument checks shared by the fake methods (`v.object` is strict)

pub(crate) type Obj = Map<String, Value>;

pub(crate) fn object<'a>(
    args: &'a Value,
    allowed: &[&str],
    label: &str,
) -> Result<&'a Obj, FakeError> {
    let Some(object) = args.as_object() else {
        return Err(FakeError::invalid(format!("{label} must be an object")));
    };
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(FakeError::invalid(format!(
            "{label} has unexpected property {key:?}"
        )));
    }
    Ok(object)
}

/// `null`, or a strict object.
pub(crate) fn nullable<'a>(
    args: &'a Value,
    allowed: &[&str],
    label: &str,
) -> Result<Option<&'a Obj>, FakeError> {
    if args.is_null() {
        Ok(None)
    } else {
        object(args, allowed, label).map(Some)
    }
}

pub(crate) fn string<'a>(object: &'a Obj, key: &str, min: usize) -> Result<&'a str, FakeError> {
    match object.get(key) {
        Some(Value::String(text)) if text.encode_utf16().count() >= min => Ok(text),
        Some(_) => Err(FakeError::invalid(format!(
            "{key} must be a string of at least {min}"
        ))),
        None => Err(FakeError::invalid(format!("is missing {key:?}"))),
    }
}

pub(crate) fn optional_int(
    object: &Obj,
    key: &str,
    min: i64,
    max: i64,
) -> Result<Option<i64>, FakeError> {
    match object.get(key) {
        None => Ok(None),
        Some(value) => match value.as_i64() {
            Some(n)
                if n >= min
                    && n <= max
                    && n.unsigned_abs() <= crate::capacity::MAX_SAFE_INTEGER =>
            {
                Ok(Some(n))
            }
            _ => Err(FakeError::invalid(format!(
                "{key} must be an integer from {min} to {max}"
            ))),
        },
    }
}

pub(crate) fn int(object: &Obj, key: &str, min: i64, max: i64) -> Result<i64, FakeError> {
    optional_int(object, key, min, max)?
        .ok_or_else(|| FakeError::invalid(format!("is missing {key:?}")))
}

pub(crate) fn optional_bool(object: &Obj, key: &str) -> Result<Option<bool>, FakeError> {
    match object.get(key) {
        None => Ok(None),
        Some(Value::Bool(flag)) => Ok(Some(*flag)),
        Some(_) => Err(FakeError::invalid(format!("{key} must be a boolean"))),
    }
}

/// `canonicalJson` for the values these fakes key by.
pub fn canonical_json(value: &Value) -> String {
    serde_json::to_string(value).expect("JSON values serialize")
}
