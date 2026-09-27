//! `reconcile`: the twin of `sdk/worker.ts` lines 406-668. Keep external values current: wait for
//! pending work, compute it, and publish it guarded by its input key. Stale results are rejected,
//! never stored.
//!
//! The leased pool shares state between its claim loop, computations, renewals and adjustments,
//! so like the queue worker it runs each piece holding one reentrant lock ("the JS thread").

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::{Mutex, ReentrantMutex, ReentrantMutexGuard};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::Notify;
use tokio::time::{MissedTickBehavior, interval_at, sleep};
use tokio_util::sync::CancellationToken;

use crate::capacity::{Concurrency, Health, Limiter, MAX_SAFE_INTEGER, default_process_health, idle_health};
use crate::client::{ClientError, QueueClient, RetryPolicy, default_backoff, non_empty_array, truthy};
use crate::clock::Clock;
use crate::json::{checked_raw, raw};
use crate::queue::EventHandler;
use crate::stop::{JobStop, WorkError};
use crate::types::{ExternalWork, ReconcileEvent};
use crate::{WorkerError, new_uuid, panic_message};

/// `ReconcileOptions`, `compute` aside (it is [`reconcile`]'s third argument).
#[derive(Clone)]
pub struct ReconcileOptions {
    /// The external.http() prefix; uses pending and publish, or next for pools, or claim, renew,
    /// release and ready in lease mode.
    pub external: String,
    pub signal: CancellationToken,
    /// Keep one key current. Omit to drain every tracked key through next. See
    /// [`ReconcileOptions::with_args`].
    pub args: Option<Box<RawValue>>,
    /// Pool mode: work only on keys in this hash shard, `(index, count)`.
    pub shard: Option<(u64, u64)>,
    /// Pool mode: parallel computations. Default 1. In lease mode, bounds let the number follow
    /// the backlog and the process's load.
    pub concurrency: Option<Concurrency>,
    /// Pool mode: keys fetched per round. Default 16.
    pub batch: Option<u64>,
    /// Pool mode: lease keys, so each is computed by one process at a time.
    pub lease: bool,
    /// Lease mode: unique per process. Defaults to `reconciler-<uuid>`.
    pub owner: Option<String>,
    /// Lease mode: lease length requested per claim and renewal. Default 30,000.
    pub lease_ms: Option<u64>,
    /// Lease mode: abort compute this long before a lease ends. Default a fifth of `lease_ms`.
    pub margin_ms: Option<u64>,
    /// Lease mode with adaptive concurrency: the process's load. Default [`default_process_health`].
    pub health: Option<Arc<dyn Health>>,
    /// Lease mode with adaptive concurrency: how often the limit follows the backlog. Default 250.
    pub adjust_every_ms: Option<u64>,
    /// Publications retry with this policy (default: the client's default policy); lease mode's
    /// claims, renewals and releases with it and an `until`.
    pub retry: Option<RetryPolicy>,
    pub on_event: Option<EventHandler<ReconcileEvent>>,
    /// `Date.now()`. Default [`Clock::system`].
    pub clock: Clock,
}

impl ReconcileOptions {
    pub fn new(external: impl Into<String>, signal: CancellationToken) -> Self {
        ReconcileOptions {
            external: external.into(),
            signal,
            args: None,
            shard: None,
            concurrency: None,
            batch: None,
            lease: false,
            owner: None,
            lease_ms: None,
            margin_ms: None,
            health: None,
            adjust_every_ms: None,
            retry: None,
            on_event: None,
            clock: Clock::system(),
        }
    }

    /// Keep the key of these arguments current.
    pub fn with_args<A: Serialize + ?Sized>(mut self, args: &A) -> Self {
        self.args = Some(raw(args));
        self
    }

    /// Set the event handler.
    pub fn on_event(mut self, handler: impl Fn(ReconcileEvent) + Send + Sync + 'static) -> Self {
        self.on_event = Some(Arc::new(handler));
        self
    }
}

/// Why the loop stops or waits.
enum Fail<E> {
    Client(E),
    /// The signal fired (TS: an `AbortError`).
    Aborted,
}

/// One unit of work as the server sent it: its raw arguments go back verbatim.
#[derive(Clone)]
struct Item {
    args: Value,
    key: String,
    input: Value,
}

/// `(input, work, stop)` → the value to publish, type-erased.
type ErasedCompute = Arc<dyn Fn(Value, Item, JobStop) -> BoxFuture<'static, Result<Box<RawValue>, Computed>> + Send + Sync>;

/// A computation that did not produce a publishable value.
enum Computed {
    /// compute failed (or its input did not deserialize): the failed event's text.
    Failed(String),
    /// The value cannot be sent (TS: `canonicalJson` throws in `mutate`), which stops `reconcile`.
    Invalid(String),
}

/// Keep external values current (`reconcile`). With `args`, one key; without, every tracked key
/// through the pool's `next`, or with `lease`, through leases shared by every process.
///
/// `compute(input, work, stop)` may run more than once for the same input.
pub async fn reconcile<C, A, I, R, F, Fut>(client: C, options: ReconcileOptions, compute: F) -> Result<(), WorkerError<C::Error>>
where
    C: QueueClient,
    A: DeserializeOwned + Send + 'static,
    I: DeserializeOwned + Clone + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(I, ExternalWork<A, I>, JobStop) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, WorkError>> + Send + 'static,
{
    let compute: ErasedCompute = Arc::new(move |input: Value, item: Item, stop: JobStop| {
        let typed = serde_json::from_value::<A>(item.args).and_then(|args| {
            let input = serde_json::from_value::<I>(input)?;
            Ok((args, input))
        });
        match typed {
            Ok((args, input)) => {
                let work = ExternalWork {
                    args,
                    key: item.key,
                    input: input.clone(),
                };
                let future = compute(input, work, stop);
                async move {
                    match future.await {
                        Ok(value) => checked_raw(&value).map_err(Computed::Invalid),
                        Err(error) => Err(Computed::Failed(error.message().to_owned())),
                    }
                }
                .boxed()
            }
            Err(error) => {
                let message = format!("The input cannot be read: {error}");
                async move { Err(Computed::Failed(message)) }.boxed()
            }
        }
    });
    let invalid = |message: &str| Err(WorkerError::Invalid(message.to_owned()));
    let concurrency = options.concurrency.unwrap_or(Concurrency::Fixed(1));
    if let Concurrency::Fixed(n) = concurrency
        && (n > MAX_SAFE_INTEGER || n < 1)
    {
        return invalid("concurrency must be a positive safe integer");
    }
    if options.lease {
        if options.args.is_some() || options.shard.is_some() {
            return invalid("Lease mode spreads the whole pool: omit args and shard");
        }
        return LeasedPool::new(client, options, concurrency, compute)?.run().await;
    }
    let Concurrency::Fixed(concurrency) = concurrency else {
        return invalid("Adaptive concurrency needs lease: true");
    };
    if options.owner.is_some()
        || options.lease_ms.is_some()
        || options.margin_ms.is_some()
        || options.health.is_some()
        || options.adjust_every_ms.is_some()
    {
        return invalid("owner, leaseMs, marginMs, health and adjustEveryMs need lease: true");
    }
    Unleased {
        client,
        on_event: options.on_event.clone().unwrap_or_else(|| Arc::new(|_| {})),
        failures: Mutex::new(HashMap::new()),
        compute,
        options,
    }
    .run(concurrency)
    .await
}

fn items(value: Value) -> Result<Vec<Item>, String> {
    match value {
        Value::Array(values) => values.into_iter().map(item).collect(),
        other => Err(format!("Expected work items, got {other}")),
    }
}

fn item(value: Value) -> Result<Item, String> {
    let Value::Object(mut object) = value else {
        return Err(format!("Expected a work item, got {value}"));
    };
    let key = match object.remove("key") {
        Some(Value::String(key)) => key,
        other => return Err(format!("A work item's key is {other:?}")),
    };
    Ok(Item {
        args: object.remove("args").unwrap_or(Value::Null),
        key,
        input: object.remove("input").unwrap_or(Value::Null),
    })
}

#[derive(Serialize)]
struct Publish<'a> {
    args: &'a Value,
    key: &'a str,
    value: &'a RawValue,
}

async fn pause(ms: u64, signal: &CancellationToken) {
    tokio::select! {
        _ = sleep(Duration::from_millis(ms)) => {}
        _ = signal.cancelled() => {}
    }
}

/// Single-key and pool modes.
struct Unleased<C: QueueClient> {
    client: C,
    options: ReconcileOptions,
    on_event: EventHandler<ReconcileEvent>,
    failures: Mutex<HashMap<String, u32>>,
    compute: ErasedCompute,
}

impl<C: QueueClient> Unleased<C> {
    async fn failed(&self, key: &str, error: String, stop: &CancellationToken) {
        let count = {
            let mut failures = self.failures.lock();
            let count = failures.get(key).copied().unwrap_or(0) + 1;
            failures.insert(key.to_owned(), count);
            count
        };
        (self.on_event)(ReconcileEvent::Failed { key: key.to_owned(), error });
        pause(default_backoff(count - 1), stop).await;
    }

    async fn settle(&self, work: Item, stop: &CancellationToken) -> Result<(), WorkerFail<C::Error>> {
        let computed = AssertUnwindSafe((self.compute)(work.input.clone(), work.clone(), JobStop::child_of(stop)))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(Computed::Failed(panic_message(&*panic))));
        let value = match computed {
            Ok(value) => value,
            Err(Computed::Failed(error)) => {
                if !stop.is_cancelled() {
                    self.failed(&work.key, error, stop).await;
                }
                return Ok(());
            }
            Err(Computed::Invalid(message)) => return Err(WorkerFail::Invalid(message)),
        };
        let name = format!("{}.publish", self.options.external);
        let args = raw(&Publish {
            args: &work.args,
            key: &work.key,
            value: &value,
        });
        let retry = self.options.retry.clone().unwrap_or_default();
        let receipt = tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(WorkerFail::Fail(Fail::Aborted)),
            receipt = self.client.mutate(&name, args, retry) => receipt,
        };
        match receipt {
            Ok(receipt) => {
                self.failures.lock().remove(&work.key);
                (self.on_event)(ReconcileEvent::Published {
                    key: work.key,
                    accepted: accepted(&receipt),
                });
                Ok(())
            }
            // The publish method rejected this result (e.g. its schema); other keys can still progress.
            Err(error) if error.code() == "EVALUATION_FAILED" && !stop.is_cancelled() => {
                self.failed(&work.key, error.describe(), stop).await;
                Ok(())
            }
            Err(error) => Err(WorkerFail::Fail(Fail::Client(error))),
        }
    }

    async fn run(self, concurrency: u64) -> Result<(), WorkerError<C::Error>> {
        let signal = &self.options.signal;
        let mut attempt = 0u32;
        while !signal.is_cancelled() {
            let round: Result<(), WorkerFail<C::Error>> = async {
                if let Some(args) = &self.options.args {
                    let name = format!("{}.pending", self.options.external);
                    let value = tokio::select! {
                        biased;
                        _ = signal.cancelled() => return Err(WorkerFail::Fail(Fail::Aborted)),
                        value = self.client.wait_until(&name, args.clone(), truthy) => value.map_err(|error| WorkerFail::Fail(Fail::Client(error)))?,
                    };
                    let work = item(value).map_err(WorkerFail::Protocol)?;
                    self.settle(work, signal).await
                } else {
                    #[derive(Serialize)]
                    struct Next {
                        limit: u64,
                        #[serde(skip_serializing_if = "Option::is_none")]
                        shard: Option<[u64; 2]>,
                    }
                    let request = raw(&Next {
                        limit: self.options.batch.unwrap_or(16),
                        shard: self.options.shard.map(|(index, count)| [index, count]),
                    });
                    let name = format!("{}.next", self.options.external);
                    let value = tokio::select! {
                        biased;
                        _ = signal.cancelled() => return Err(WorkerFail::Fail(Fail::Aborted)),
                        value = self.client.wait_until(&name, request, non_empty_array) => value.map_err(|error| WorkerFail::Fail(Fail::Client(error)))?,
                    };
                    let queue = Mutex::new(VecDeque::from(items(value).map_err(WorkerFail::Protocol)?));
                    let batch = signal.child_token();
                    let lanes = (concurrency as usize).min(queue.lock().len());
                    let mut running: FuturesUnordered<_> = (0..lanes)
                        .map(|_| async {
                            loop {
                                let Some(work) = queue.lock().pop_front() else { return Ok(()) };
                                if batch.is_cancelled() {
                                    return Ok(());
                                }
                                if let Err(error) = self.settle(work, &batch).await {
                                    batch.cancel();
                                    return Err(error);
                                }
                            }
                        })
                        .collect();
                    // Like Promise.all, the first failure ends the round.
                    while let Some(lane) = running.next().await {
                        lane?;
                    }
                    Ok(())
                }
            }
            .await;
            match round {
                Ok(()) => attempt = 0,
                Err(WorkerFail::Invalid(message)) => return Err(WorkerError::Invalid(message)),
                Err(WorkerFail::Protocol(message)) => return Err(WorkerError::Protocol(message)),
                Err(WorkerFail::Fail(fail)) => {
                    if signal.is_cancelled() {
                        return Ok(());
                    }
                    let error = match fail {
                        Fail::Client(error) => error,
                        Fail::Aborted => continue,
                    };
                    if !error.is_transient() {
                        return Err(WorkerError::Client(error));
                    }
                    (self.on_event)(ReconcileEvent::Waiting { error: error.describe() });
                    pause(default_backoff(attempt), signal).await;
                    attempt += 1;
                }
            }
        }
        Ok(())
    }
}

enum WorkerFail<E> {
    Fail(Fail<E>),
    Invalid(String),
    Protocol(String),
}

fn accepted(receipt: &Value) -> bool {
    receipt.get("accepted").is_some_and(truthy)
}

/// A leased key (`Held`).
struct Held {
    item: Item,
    owner: String,
    attempt: u64,
    /// `entry.lost`: the lease ran out or was lost.
    lost: JobStop,
    /// What compute gets: the pool's signal or `lost`.
    stop: JobStop,
    deadline: i64,
    timer: CancellationToken,
}

impl Held {
    fn lose(&self, reason: &str) {
        self.lost.stop(reason);
        self.stop.stop(reason);
    }

    fn lease(&self, delay_ms: Option<u64>) -> Lease<'_> {
        Lease {
            args: &self.item.args,
            key: &self.item.key,
            owner: &self.owner,
            attempt: self.attempt,
            delay_ms,
        }
    }
}

/// `{ args, key, owner, attempt }`, and `delayMs` for release.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Lease<'a> {
    args: &'a Value,
    key: &'a str,
    owner: &'a str,
    attempt: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    delay_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolClaim<'a> {
    owner: &'a str,
    lease_ms: u64,
    limit: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolRenew<'a> {
    leases: Vec<Lease<'a>>,
    lease_ms: u64,
}

/// The readiness watch's outcome (a shared promise that sets `shown`, or resets `ready` and rejects).
enum Ready<E> {
    Pending,
    Shown,
    Failed(Option<E>),
    Aborted,
}

struct ReadyCell<E> {
    done: CancellationToken,
    outcome: Mutex<Ready<E>>,
}

struct PoolState<E> {
    held: BTreeMap<u64, Held>,
    running: BTreeSet<u64>,
    ready: Option<Arc<ReadyCell<E>>>,
    /// Holds from the watch firing to the next short claim.
    shown: bool,
    limiter: Limiter,
    next_key: u64,
    failed: Option<WorkerError<E>>,
}

type Guard<'a, E> = ReentrantMutexGuard<'a, RefCell<PoolState<E>>>;

/// Lease mode (`leasedPool`): claim only as many keys as there are free computations, so no
/// process hoards work; one renewal covers every held lease, and held keys are handed back on the
/// way out.
struct LeasedPool<C: QueueClient> {
    client: C,
    external: String,
    owner: String,
    lease_ms: u64,
    margin_ms: u64,
    batch: u64,
    retry: Option<RetryPolicy>,
    adjust_every_ms: u64,
    clock: Clock,
    on_event: EventHandler<ReconcileEvent>,
    /// `options.signal` or a halt.
    signal: CancellationToken,
    lock: ReentrantMutex<RefCell<PoolState<C::Error>>>,
    /// A computation ended or the limit changed (`roomChanged` and the running promises).
    room_changed: Notify,
    compute: ErasedCompute,
}

impl<C: QueueClient> LeasedPool<C> {
    fn new(client: C, options: ReconcileOptions, concurrency: Concurrency, compute: ErasedCompute) -> Result<Arc<Self>, WorkerError<C::Error>> {
        let invalid = |message: &str| WorkerError::Invalid(message.to_owned());
        let owner = options.owner.clone().unwrap_or_else(|| format!("reconciler-{}", new_uuid()));
        let lease_ms = options.lease_ms.unwrap_or(30_000);
        let margin_ms = options.margin_ms.unwrap_or(lease_ms / 5);
        let batch = options.batch.unwrap_or(16);
        if lease_ms > MAX_SAFE_INTEGER || margin_ms > MAX_SAFE_INTEGER || lease_ms <= margin_ms {
            return Err(invalid("leaseMs must exceed marginMs"));
        }
        if batch > MAX_SAFE_INTEGER || batch < 1 {
            return Err(invalid("batch must be a positive safe integer"));
        }
        let health = options.health.clone().unwrap_or_else(|| match concurrency {
            Concurrency::Fixed(_) => idle_health(),
            Concurrency::Adaptive(_) => default_process_health(),
        });
        let limiter = Limiter::with_clock(concurrency, health, &options.clock).map_err(WorkerError::Invalid)?;
        if limiter.max > 1024 {
            return Err(invalid("Lease mode runs at most 1024 computations at once"));
        }
        Ok(Arc::new(LeasedPool {
            client,
            external: options.external,
            owner,
            lease_ms,
            margin_ms,
            batch,
            retry: options.retry,
            adjust_every_ms: options.adjust_every_ms.unwrap_or(250),
            clock: options.clock,
            on_event: options.on_event.unwrap_or_else(|| Arc::new(|_| {})),
            signal: options.signal.child_token(),
            lock: ReentrantMutex::new(RefCell::new(PoolState {
                held: BTreeMap::new(),
                running: BTreeSet::new(),
                ready: None,
                shown: false,
                limiter,
                next_key: 0,
                failed: None,
            })),
            room_changed: Notify::new(),
            compute,
        }))
    }

    fn enter(&self) -> Guard<'_, C::Error> {
        self.lock.lock()
    }

    fn emit(&self, event: ReconcileEvent) {
        (self.on_event)(event)
    }

    fn now(&self) -> i64 {
        self.clock.now_ms()
    }

    /// `send(method, args, until, abort?)`.
    fn send(&self, method: &str, args: Box<RawValue>, until: i64) -> impl Future<Output = Result<Value, C::Error>> + Send + 'static {
        let client = self.client.clone();
        let name = format!("{}.{}", self.external, method);
        let retry = RetryPolicy {
            until: Some(until),
            ..self.retry.clone().unwrap_or_default()
        };
        async move { client.mutate(&name, args, retry).await }
    }

    /// `failed ??= { error }; halt.abort()`.
    fn halt(&self, error: WorkerError<C::Error>) {
        {
            let guard = self.enter();
            let mut state = guard.borrow_mut();
            if state.failed.is_none() {
                state.failed = Some(error);
            }
        }
        self.signal.cancel();
    }

    /// `arm(entry, deadline)`.
    fn arm(self: &Arc<Self>, key: u64, entry: &mut Held, deadline: i64) {
        entry.deadline = deadline;
        entry.timer.cancel();
        let timer = CancellationToken::new();
        entry.timer = timer.clone();
        let delay = (deadline - self.now()).max(0) as u64;
        let pool = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = timer.cancelled() => {}
                _ = sleep(Duration::from_millis(delay)) => {
                    let guard = pool.enter();
                    if timer.is_cancelled() {
                        return;
                    }
                    if let Some(entry) = guard.borrow().held.get(&key) {
                        entry.lose("The lease ran out");
                    }
                }
            }
        });
    }

    /// `whenReady()`: the shared readiness watch.
    fn when_ready(self: &Arc<Self>, guard: &Guard<'_, C::Error>) -> Arc<ReadyCell<C::Error>> {
        let mut state = guard.borrow_mut();
        if let Some(ready) = &state.ready {
            return ready.clone();
        }
        let cell = Arc::new(ReadyCell {
            done: CancellationToken::new(),
            outcome: Mutex::new(Ready::Pending),
        });
        state.ready = Some(cell.clone());
        let pool = self.clone();
        let watched = cell.clone();
        tokio::spawn(async move {
            let name = format!("{}.ready", pool.external);
            let result = tokio::select! {
                biased;
                _ = pool.signal.cancelled() => None,
                result = pool.client.wait_until(&name, raw(&Value::Null), truthy) => Some(result),
            };
            let guard = pool.enter();
            let mut state = guard.borrow_mut();
            let outcome = match result {
                Some(Ok(_)) => {
                    state.shown = true;
                    Ready::Shown
                }
                failed => {
                    if state.ready.as_ref().is_some_and(|ready| Arc::ptr_eq(ready, &watched)) {
                        state.ready = None;
                    }
                    match failed {
                        Some(Err(error)) => Ready::Failed(Some(error)),
                        _ => Ready::Aborted,
                    }
                }
            };
            *watched.outcome.lock() = outcome;
            watched.done.cancel();
        });
        cell
    }

    async fn release(&self, key: u64, delay_ms: u64) {
        // While running, retry until the lease would end anyway; on the way out, try once.
        let sent = {
            let guard = self.enter();
            let state = guard.borrow();
            let Some(entry) = state.held.get(&key) else { return };
            let until = if self.signal.is_cancelled() {
                self.now()
            } else {
                entry.deadline + self.margin_ms as i64
            };
            self.send("release", raw(&entry.lease(Some(delay_ms))), until)
        };
        // The lease runs out on its own.
        let _ = sent.await;
    }

    fn start(self: &Arc<Self>, guard: &Guard<'_, C::Error>, item: Item, owner: String, attempt: u64, sent_at: i64) {
        let key = {
            let mut state = guard.borrow_mut();
            let key = state.next_key;
            state.next_key += 1;
            let mut entry = Held {
                item: item.clone(),
                owner,
                attempt,
                lost: JobStop::new(),
                stop: JobStop::child_of(&self.signal),
                deadline: 0,
                timer: CancellationToken::new(),
            };
            self.arm(key, &mut entry, sent_at + (self.lease_ms - self.margin_ms) as i64);
            state.held.insert(key, entry);
            key
        };
        self.emit(ReconcileEvent::Claimed {
            key: item.key.clone(),
            attempt,
        });
        guard.borrow_mut().running.insert(key);
        let pool = self.clone();
        tokio::spawn(async move {
            if let Err(error) = pool.run_one(key).await {
                pool.halt(error);
            }
            {
                let guard = pool.enter();
                let mut state = guard.borrow_mut();
                if let Some(entry) = state.held.remove(&key) {
                    entry.timer.cancel();
                }
                state.running.remove(&key);
            }
            pool.room_changed.notify_waiters();
        });
    }

    /// `run(entry)`.
    async fn run_one(self: &Arc<Self>, key: u64) -> Result<(), WorkerError<C::Error>> {
        let (item, attempt, stop, lost) = {
            let guard = self.enter();
            let state = guard.borrow();
            let entry = &state.held[&key];
            (entry.item.clone(), entry.attempt, entry.stop.clone(), entry.lost.clone())
        };
        let computed = AssertUnwindSafe((self.compute)(item.input.clone(), item.clone(), stop.clone()))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(Computed::Failed(panic_message(&*panic))));
        let value = match computed {
            Ok(value) if !stop.is_stopped() => value,
            Err(Computed::Invalid(message)) if !stop.is_stopped() => return Err(WorkerError::Invalid(message)),
            other => {
                if lost.is_stopped() {
                    self.emit(ReconcileEvent::Lost { key: item.key });
                    return Ok(());
                }
                if self.signal.is_cancelled() {
                    self.release(key, 0).await;
                    return Ok(());
                }
                let error = match other {
                    Err(Computed::Failed(error)) => error,
                    _ => stop.reason().unwrap_or_default().to_owned(),
                };
                {
                    let _guard = self.enter();
                    self.emit(ReconcileEvent::Failed { key: item.key, error });
                }
                // The delay applies to every process, and attempt counts across them.
                self.release(key, default_backoff(attempt.saturating_sub(1) as u32)).await;
                return Ok(());
            }
        };
        let name = format!("{}.publish", self.external);
        let args = raw(&Publish {
            args: &item.args,
            key: &item.key,
            value: &value,
        });
        let retry = self.retry.clone().unwrap_or_default();
        let receipt = tokio::select! {
            biased;
            _ = self.signal.cancelled() => None,
            receipt = self.client.mutate(&name, args, retry) => Some(receipt),
        };
        let receipt = match receipt {
            Some(Ok(receipt)) => receipt,
            _ if self.signal.is_cancelled() => {
                self.release(key, 0).await;
                return Ok(());
            }
            Some(Err(error)) if error.code() == "EVALUATION_FAILED" => {
                {
                    let _guard = self.enter();
                    self.emit(ReconcileEvent::Failed {
                        key: item.key,
                        error: error.describe(),
                    });
                }
                self.release(key, default_backoff(attempt.saturating_sub(1) as u32)).await;
                return Ok(());
            }
            // Once the lease runs out, another process computes the key again.
            Some(Err(error)) if error.is_transient() => {
                let _guard = self.enter();
                self.emit(ReconcileEvent::Waiting { error: error.describe() });
                return Ok(());
            }
            Some(Err(error)) => return Err(WorkerError::Client(error)),
            None => return Ok(()),
        };
        let accepted = accepted(&receipt);
        {
            let _guard = self.enter();
            self.emit(ReconcileEvent::Published { key: item.key, accepted });
        }
        // A rejected result was computed for an older input: hand the key back for the current one.
        if !accepted {
            self.release(key, 0).await;
        }
        Ok(())
    }

    async fn renewing(self: Arc<Self>, stop: CancellationToken) {
        let every = Duration::from_millis(((self.lease_ms - self.margin_ms) / 3).max(1));
        let mut ticks = interval_at(tokio::time::Instant::now() + every, every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            let (keys, sent) = {
                let guard = self.enter();
                let state = guard.borrow();
                if state.held.is_empty() {
                    continue;
                }
                let renewed_at = self.now();
                let args = raw(&PoolRenew {
                    leases: state.held.values().map(|entry| entry.lease(None)).collect(),
                    lease_ms: self.lease_ms,
                });
                let until = state.held.values().map(|entry| entry.deadline).min().unwrap_or(renewed_at);
                ((state.held.keys().copied().collect::<Vec<_>>(), renewed_at), self.send("renew", args, until))
            };
            let pool = self.clone();
            tokio::spawn(async move {
                let (keys, renewed_at) = keys;
                // Each lease's deadline aborts its computation if renewals stop landing.
                let expiries = tokio::select! {
                    biased;
                    _ = pool.signal.cancelled() => return,
                    result = sent => match result { Ok(expiries) => expiries, Err(_) => return },
                };
                let guard = pool.enter();
                let mut state = guard.borrow_mut();
                for (index, key) in keys.into_iter().enumerate() {
                    let Some(entry) = state.held.get_mut(&key) else { continue };
                    if entry.lost.is_stopped() {
                        continue;
                    }
                    if expiries.get(index).is_some_and(Value::is_null) {
                        entry.lose("The lease was lost");
                    } else {
                        pool.arm(key, entry, renewed_at + (pool.lease_ms - pool.margin_ms) as i64);
                    }
                }
            });
        }
    }

    async fn adjusting(self: Arc<Self>, stop: CancellationToken) {
        let every = Duration::from_millis(self.adjust_every_ms.max(1));
        let mut ticks = interval_at(tokio::time::Instant::now() + every, every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            let guard = self.enter();
            let (full, shown) = {
                let state = guard.borrow();
                (state.running.len() as u64 >= state.limiter.limit(), state.shown)
            };
            if full {
                if shown {
                    guard.borrow_mut().limiter.want();
                } else {
                    self.when_ready(&guard);
                }
            }
            let change = guard.borrow_mut().limiter.adjust();
            let Some(change) = change else { continue };
            self.emit(ReconcileEvent::Limit {
                limit: change.limit,
                reason: change.reason,
            });
            drop(guard);
            self.room_changed.notify_waiters();
        }
    }

    async fn run(self: Arc<Self>) -> Result<(), WorkerError<C::Error>> {
        let signal = self.signal.clone();
        let _stop_on_drop = signal.clone().drop_guard();
        let renewal = CancellationToken::new();
        tokio::spawn(self.clone().renewing(renewal.clone()));
        let adjusting = CancellationToken::new();
        if !self.enter().borrow().limiter.fixed() {
            tokio::spawn(self.clone().adjusting(adjusting.clone()));
        }
        let mut attempt = 0u32;
        while !signal.is_cancelled() {
            let notified = self.room_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (sent_at, limit) = {
                let guard = self.enter();
                let state = guard.borrow();
                let running = state.running.len() as u64;
                if running >= state.limiter.limit() {
                    None
                } else {
                    Some((self.now(), self.batch.min(state.limiter.limit() - running)))
                }
            }
            .unzip();
            let (Some(sent_at), Some(limit)) = (sent_at, limit) else {
                // Wait for a computation to end, or for the limit to grow.
                notified.await;
                continue;
            };
            let claimed = self
                .send(
                    "claim",
                    raw(&PoolClaim {
                        owner: &self.owner,
                        lease_ms: self.lease_ms,
                        limit,
                    }),
                    sent_at + self.lease_ms as i64,
                )
                .await;
            let error = match claimed {
                Ok(value) => {
                    attempt = 0;
                    let claims = match claims(value) {
                        Ok(claims) => claims,
                        Err(message) => {
                            self.halt(WorkerError::Protocol(message));
                            break;
                        }
                    };
                    let cell = {
                        let guard = self.enter();
                        let count = claims.len() as u64;
                        for (item, owner, attempt) in claims {
                            self.start(&guard, item, owner, attempt, sent_at);
                        }
                        if count < limit {
                            // A short batch means the backlog is drained; claim again once more work is ready.
                            {
                                let mut state = guard.borrow_mut();
                                state.ready = None;
                                state.shown = false;
                            }
                            Some(self.when_ready(&guard))
                        } else {
                            let mut state = guard.borrow_mut();
                            if state.running.len() as u64 >= state.limiter.limit() {
                                state.limiter.want();
                            }
                            None
                        }
                    };
                    let Some(cell) = cell else { continue };
                    cell.done.cancelled().await;
                    let outcome = std::mem::replace(&mut *cell.outcome.lock(), Ready::Aborted);
                    match outcome {
                        Ready::Shown | Ready::Pending => continue,
                        Ready::Aborted | Ready::Failed(None) => None,
                        Ready::Failed(Some(error)) => Some(error),
                    }
                }
                Err(error) => Some(error),
            };
            if signal.is_cancelled() {
                break;
            }
            let Some(error) = error else { continue };
            if !error.is_transient() {
                self.halt(WorkerError::Client(error));
                break;
            }
            {
                let _guard = self.enter();
                self.emit(ReconcileEvent::Waiting { error: error.describe() });
            }
            pause(default_backoff(attempt), &signal).await;
            attempt += 1;
        }
        adjusting.cancel();
        loop {
            let notified = self.room_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.enter().borrow().running.is_empty() {
                break;
            }
            notified.await;
        }
        renewal.cancel();
        match self.enter().borrow_mut().failed.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// The claims of a pool's claim: `(work, owner, attempt)`.
fn claims(value: Value) -> Result<Vec<(Item, String, u64)>, String> {
    let Value::Array(values) = value else {
        return Err(format!("A claim returned {value}"));
    };
    values
        .into_iter()
        .map(|value| {
            let owner = value.get("owner").and_then(Value::as_str).map(str::to_owned);
            let attempt = value.get("attempt").and_then(Value::as_u64);
            let item = item(value)?;
            match (owner, attempt) {
                (Some(owner), Some(attempt)) => Ok((item, owner, attempt)),
                _ => Err("A claim returned a lease without owner or attempt".to_owned()),
            }
        })
        .collect()
}
