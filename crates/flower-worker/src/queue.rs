//! `runQueueWorker`: the twin of `sdk/worker.ts` lines 1-404.
//!
//! The TS worker's correctness leans on JavaScript running one piece of code at a time: room
//! accounting (`busy = running - idle + claiming`), probing, chain reservations and the order of
//! events all assume nothing interleaves between two `await`s. Here every such piece runs holding
//! one reentrant lock, "the JS thread": state changes, the events they cause and the synchronous
//! part of a job's `work` happen in the same critical section, in the TS order. The lock is
//! reentrant so that an event handler or `work` may call [`JobControl`] as TS code may; it is never
//! held across an `await` (its guard is not `Send`).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, join_all};
use parking_lot::{Mutex, ReentrantMutex, ReentrantMutexGuard};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::{Notify, oneshot};
use tokio::time::{MissedTickBehavior, interval_at, sleep};
use tokio_util::sync::CancellationToken;

use crate::capacity::{
    Concurrency, Health, LimitChange, Limiter, MAX_SAFE_INTEGER, default_process_health,
};
use crate::client::{ClientError, QueueClient, RetryPolicy, default_backoff, truthy};
use crate::clock::Clock;
use crate::json::{raw, storable};
use crate::stop::{Control, JobControl, JobStop, WorkError};
use crate::types::{Claim, LeaseIdentity, QueueWorkerEvent};
use crate::{WorkerError, new_uuid, panic_message};

/// How long work may take to stop once a drain aborted it, before a stopping worker gives up on it.
const ABANDON_AFTER_MS: u64 = 5_000;

/// Receives every [`QueueWorkerEvent`], in order.
pub type EventHandler<E> = Arc<dyn Fn(E) + Send + Sync>;

/// `QueueWorkerOptions`, `work` aside (it is [`run_queue_worker`]'s third argument). Build it with
/// [`QueueWorkerOptions::new`] and struct update syntax.
#[derive(Clone)]
pub struct QueueWorkerOptions {
    /// The queue.http() prefix; the worker uses its claim, renew, complete, fail and ready methods.
    pub queue: String,
    /// Stop claiming, finish held jobs, then return.
    pub signal: CancellationToken,
    /// Once stopping, how long held jobs may keep running before their stop fires and they fail.
    /// Work that still runs 5 s after its stop is given up on (an `unreported` event): the worker
    /// returns anyway, and the job runs again once its lease ends. Default: until they finish.
    pub drain_ms: Option<u64>,
    /// When `drain_ms` ends, hand the jobs still running back with the queue's release method
    /// instead of failing them. Needs `drain_ms`. Default false.
    pub release: bool,
    /// Unique per process. Defaults to `worker-<uuid>`.
    pub owner: Option<String>,
    /// Jobs this process runs at once. Default `{ min: 1, max: 16 }`.
    pub concurrency: Option<Concurrency>,
    /// Jobs one claim may take. Default 1, sent without max.
    pub batch: Option<u64>,
    /// Claims in flight at once, once claims come back full. Default 4 (at most the limiter's max).
    pub claimers: Option<u64>,
    /// Report each outcome with a claim for this process's next jobs, in the same commit. Needs
    /// complete and fail methods that take `next`. Default false.
    pub chain: bool,
    /// Wait in the queue's line, so a new job wakes one process instead of all. Default false.
    pub wait: bool,
    /// How long a place in line lasts; an idle worker claims again at half of it. Default 60,000.
    pub wait_ms: Option<u64>,
    /// Lease length requested per claim and renewal. Default 30,000.
    pub lease_ms: Option<u64>,
    /// Extend leases while work runs, all of them in one call. Default true.
    pub renew: bool,
    /// Stop working this long before a lease ends. Default a fifth of `lease_ms`.
    pub margin_ms: Option<u64>,
    /// For queue.http(prefix, { scope: "argument" }).
    pub scope: Option<String>,
    /// The retry policy of every call; the worker sets `until`.
    pub retry: RetryPolicy,
    /// The process's load, read at each adjustment. Default [`default_process_health`].
    pub health: Option<Arc<dyn Health>>,
    /// How often the limit follows demand and load. Default 250.
    pub adjust_every_ms: Option<u64>,
    pub on_event: Option<EventHandler<QueueWorkerEvent>>,
    /// `Date.now()`. Default [`Clock::system`]; tests use [`Clock::tokio`].
    pub clock: Clock,
}

impl QueueWorkerOptions {
    pub fn new(queue: impl Into<String>, signal: CancellationToken) -> Self {
        QueueWorkerOptions {
            queue: queue.into(),
            signal,
            drain_ms: None,
            release: false,
            owner: None,
            concurrency: None,
            batch: None,
            claimers: None,
            chain: false,
            wait: false,
            wait_ms: None,
            lease_ms: None,
            renew: true,
            margin_ms: None,
            scope: None,
            retry: RetryPolicy::default(),
            health: None,
            adjust_every_ms: None,
            on_event: None,
            clock: Clock::system(),
        }
    }

    /// Set the event handler.
    pub fn on_event(mut self, handler: impl Fn(QueueWorkerEvent) + Send + Sync + 'static) -> Self {
        self.on_event = Some(Arc::new(handler));
        self
    }
}

/// Run jobs from a queue until `options.signal` is cancelled. One readiness watch serves the whole
/// process: after it fires, one claim at a time asks until claims come back full, several jobs per
/// claim with `batch`. How many jobs run at once follows demand and the process's load (see
/// `concurrency`). Leases renew together in one call; outcomes are reported with retries that keep
/// one request ID.
///
/// `work` gets the claim, the job's stop and its control. It can run again after a crash, so give
/// external services `job.id` as an idempotency key. A payload that does not deserialize into `P`
/// fails the attempt with the reason.
pub async fn run_queue_worker<C, P, R, W, F>(
    client: C,
    options: QueueWorkerOptions,
    work: W,
) -> Result<(), WorkerError<C::Error>>
where
    C: QueueClient,
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    W: Fn(Claim<P>, JobStop, JobControl) -> F + Send + Sync + 'static,
    F: Future<Output = Result<R, WorkError>> + Send + 'static,
{
    let work: ErasedWork = Arc::new(move |claim: Claim<Value>, stop, control| {
        let Claim {
            attempt,
            expires_at,
            history,
            id,
            owner,
            payload,
            scope,
            token,
        } = claim;
        match serde_json::from_value::<P>(payload) {
            Ok(payload) => {
                let claim = Claim {
                    attempt,
                    expires_at,
                    history,
                    id,
                    owner,
                    payload,
                    scope,
                    token,
                };
                let future = work(claim, stop, control);
                async move {
                    match future.await {
                        Ok(result) => storable(&result),
                        Err(error) => Err(error.message().to_owned()),
                    }
                }
                .boxed()
            }
            Err(error) => {
                let message = format!("The payload cannot be read: {error}");
                async move { Err(message) }.boxed()
            }
        }
    });
    let worker = Worker::new(client, options, work)?;
    worker.run().await
}

type ErasedWork = Arc<
    dyn Fn(Claim<Value>, JobStop, JobControl) -> BoxFuture<'static, Result<Box<RawValue>, String>>
        + Send
        + Sync,
>;

struct Settings {
    queue: String,
    owner: String,
    lease_ms: u64,
    margin_ms: u64,
    batch: u64,
    wait: bool,
    wait_ms: u64,
    chain: bool,
    renew: bool,
    drain_ms: Option<u64>,
    release: bool,
    scope: Option<String>,
    retry: RetryPolicy,
    claimers: u64,
    adjust_every_ms: u64,
    ready_args: Box<RawValue>,
}

/// A held job (`Held`).
struct Held {
    job: Claim<Value>,
    identity: LeaseIdentity,
    stop: JobStop,
    deadline: i64,
    renewed_at: i64,
    timer: CancellationToken,
    drained: bool,
    idled: bool,
}

/// A readiness watch's shared outcome (`ready`, a shared promise).
struct ReadyCell<E> {
    done: CancellationToken,
    outcome: Mutex<Outcome<E>>,
}

enum Outcome<E> {
    Pending,
    Ready,
    /// The watch failed: the first claimer to see it rethrows the error, the others stop too.
    Failed(Option<E>),
}

impl<E> ReadyCell<E> {
    fn pending() -> Arc<Self> {
        Arc::new(ReadyCell {
            done: CancellationToken::new(),
            outcome: Mutex::new(Outcome::Pending),
        })
    }

    fn resolved() -> Arc<Self> {
        let cell = ReadyCell::pending();
        *cell.outcome.lock() = Outcome::Ready;
        cell.done.cancel();
        cell
    }
}

/// Why a claimer stopped the worker.
enum Halt<E> {
    Client(E),
    Protocol(String),
    /// Another claimer rethrows the error.
    Shared,
}

struct State<E> {
    held: BTreeMap<u64, Held>,
    running: BTreeSet<u64>,
    room_waiters: VecDeque<oneshot::Sender<()>>,
    claiming: i64,
    /// Held jobs that said they mostly wait: they don't count toward the limit.
    idle: i64,
    ready: Option<Arc<ReadyCell<E>>>,
    /// The queue showed work since the last short claim.
    shown: bool,
    /// A readiness watch is open.
    watching: bool,
    failures: u32,
    /// One claimer asks at a time until claims come back full.
    probing: Option<CancellationToken>,
    /// Full claims in a row.
    full: u64,
    limiter: Limiter,
    next_key: u64,
    failed: Option<Halt<E>>,
}

impl<E> State<E> {
    fn busy(&self) -> i64 {
        self.running.len() as i64 - self.idle + self.claiming
    }

    fn limit(&self) -> i64 {
        self.limiter.limit() as i64
    }

    /// `wake(count)`: resolve the first `count` room waiters, or all of them.
    fn wake(&mut self, count: Option<i64>) {
        let count = count.map_or(self.room_waiters.len(), |count| count.max(0) as usize);
        for _ in 0..count {
            match self.room_waiters.pop_front() {
                Some(waiter) => {
                    let _ = waiter.send(());
                }
                None => break,
            }
        }
    }

    /// A claim that came back short means the queue ran dry: watch for work again, keeping a
    /// watch still open.
    fn claimed(&mut self, got: usize, asked: i64) {
        if (got as i64) < asked {
            self.full = 0;
            if !self.watching {
                self.ready = None;
            }
            self.shown = false;
        } else {
            self.full += 1;
        }
    }

    fn fail(&mut self, halt: Halt<E>) {
        if self.failed.is_none() {
            self.failed = Some(halt);
        }
    }
}

/// The state every task shares, independent of the client's type, so [`JobControl`] can reach it.
struct Core<E> {
    lock: ReentrantMutex<RefCell<State<E>>>,
    on_event: EventHandler<QueueWorkerEvent>,
    /// `options.signal` or a halt.
    signal: CancellationToken,
    running_changed: Notify,
}

type Guard<'a, E> = ReentrantMutexGuard<'a, RefCell<State<E>>>;

impl<E> Core<E> {
    fn enter(&self) -> Guard<'_, E> {
        self.lock.lock()
    }

    /// `onEvent`: called holding the lock, borrowing nothing, so the handler may use a control.
    fn emit(&self, event: QueueWorkerEvent) {
        (self.on_event)(event)
    }

    /// `changed(change)`: report a new limit and let every claimer look again.
    fn changed(&self, guard: &Guard<'_, E>, change: Option<LimitChange>) {
        let Some(change) = change else { return };
        self.emit(QueueWorkerEvent::Limit {
            limit: change.limit,
            reason: change.reason,
        });
        guard.borrow_mut().wake(None);
    }
}

impl<E: Send + Sync + 'static> Control for Core<E> {
    fn throttle(&self, ms: u64, reason: &str) {
        let guard = self.enter();
        let change = guard.borrow_mut().limiter.throttle(ms, reason);
        self.changed(&guard, change);
    }

    fn idle(&self, key: u64) {
        let guard = self.enter();
        let mut state = guard.borrow_mut();
        let Some(entry) = state.held.get_mut(&key) else {
            return;
        };
        if entry.idled {
            return;
        }
        entry.idled = true;
        state.idle += 1;
        state.wake(Some(1));
    }
}

struct Worker<C: QueueClient> {
    client: C,
    settings: Settings,
    clock: Clock,
    core: Arc<Core<C::Error>>,
    work: ErasedWork,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaimArgs<'a> {
    owner: &'a str,
    lease_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LeaveArgs<'a> {
    owner: &'a str,
    max: u64,
    wait_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RenewArgs<'a> {
    leases: Vec<&'a LeaseIdentity>,
    lease_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadyArgs<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<&'a str>,
}

/// A report's claim for the next jobs (`NextClaim`).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NextClaim {
    max: i64,
    lease_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_ms: Option<u64>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    message: &'a str,
}

/// complete, fail and release arguments: `{ ...identity, ...scope, result | error, next? }`.
#[derive(Serialize)]
struct Report<'a> {
    id: &'a str,
    owner: &'a str,
    token: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorBody<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<NextClaim>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Method {
    Complete,
    Fail,
    Release,
}

impl Method {
    fn name(self) -> &'static str {
        match self {
            Method::Complete => "complete",
            Method::Fail => "fail",
            Method::Release => "release",
        }
    }
}

/// `{ more = [], ...first }` of a `Claimed`: the jobs a claim took.
pub(crate) fn claimed_jobs(value: Value) -> Result<Vec<Claim<Value>>, String> {
    let Value::Object(mut first) = value else {
        return match value {
            Value::Null => Ok(Vec::new()),
            other => Err(format!("A claim returned {other}")),
        };
    };
    let more = first.remove("more");
    let mut jobs = vec![
        serde_json::from_value(Value::Object(first))
            .map_err(|error| format!("A claim returned an invalid job: {error}"))?,
    ];
    match more {
        None => {}
        Some(Value::Array(more)) => {
            for job in more {
                jobs.push(
                    serde_json::from_value(job)
                        .map_err(|error| format!("A claim returned an invalid job: {error}"))?,
                );
            }
        }
        Some(other) => return Err(format!("A claim returned more: {other}")),
    }
    Ok(jobs)
}

async fn pause(ms: u64, signal: &CancellationToken) {
    tokio::select! {
        _ = sleep(Duration::from_millis(ms)) => {}
        _ = signal.cancelled() => {}
    }
}

fn safe(value: u64) -> bool {
    value <= MAX_SAFE_INTEGER
}

impl<C: QueueClient> Worker<C> {
    fn new(
        client: C,
        options: QueueWorkerOptions,
        work: ErasedWork,
    ) -> Result<Arc<Self>, WorkerError<C::Error>> {
        let invalid = |message: &str| WorkerError::Invalid(message.to_owned());
        let lease_ms = options.lease_ms.unwrap_or(30_000);
        let margin_ms = options.margin_ms.unwrap_or(lease_ms / 5);
        let batch = options.batch.unwrap_or(1);
        let wait = options.wait;
        let wait_ms = options.wait_ms.unwrap_or(60_000);
        if !safe(lease_ms) || lease_ms <= margin_ms {
            return Err(invalid("leaseMs must exceed marginMs"));
        }
        if !safe(batch) || batch < 1 {
            return Err(invalid("batch must be a positive safe integer"));
        }
        if !safe(wait_ms) || wait_ms < 2 {
            return Err(invalid("waitMs must be a safe integer of at least 2"));
        }
        if options.drain_ms.is_some_and(|ms| !safe(ms)) {
            return Err(invalid("drainMs must be a nonnegative safe integer"));
        }
        if options.release && options.drain_ms.is_none() {
            return Err(invalid("release needs drainMs"));
        }
        let health = options
            .health
            .clone()
            .unwrap_or_else(default_process_health);
        let concurrency = options
            .concurrency
            .unwrap_or(Concurrency::Adaptive(crate::Adaptive {
                min: Some(1),
                max: Some(16),
                initial: None,
            }));
        let limiter = Limiter::with_clock(concurrency, health, &options.clock)
            .map_err(WorkerError::Invalid)?;
        let claimers = options.claimers.unwrap_or(4).min(limiter.max).max(1);
        if !safe(claimers) {
            return Err(invalid("claimers must be a positive safe integer"));
        }
        let owner = options
            .owner
            .clone()
            .unwrap_or_else(|| format!("worker-{}", new_uuid()));
        let ready_args = if wait {
            raw(&ReadyArgs {
                scope: options.scope.as_deref(),
                owner: Some(&owner),
            })
        } else if options.scope.is_some() {
            raw(&ReadyArgs {
                scope: options.scope.as_deref(),
                owner: None,
            })
        } else {
            raw(&Value::Null)
        };
        let signal = options.signal.child_token();
        let state = State {
            held: BTreeMap::new(),
            running: BTreeSet::new(),
            room_waiters: VecDeque::new(),
            claiming: 0,
            idle: 0,
            // Waiting in line starts with a claim, which takes a place when it comes back short.
            ready: wait.then(ReadyCell::resolved),
            shown: false,
            watching: false,
            failures: 0,
            probing: None,
            full: 0,
            limiter,
            next_key: 0,
            failed: None,
        };
        Ok(Arc::new(Worker {
            client,
            settings: Settings {
                queue: options.queue,
                owner,
                lease_ms,
                margin_ms,
                batch,
                wait,
                wait_ms,
                chain: options.chain,
                renew: options.renew,
                drain_ms: options.drain_ms,
                release: options.release,
                scope: options.scope,
                retry: options.retry,
                claimers,
                adjust_every_ms: options.adjust_every_ms.unwrap_or(250),
                ready_args,
            },
            clock: options.clock,
            core: Arc::new(Core {
                lock: ReentrantMutex::new(RefCell::new(state)),
                on_event: options.on_event.unwrap_or_else(|| Arc::new(|_| {})),
                signal,
                running_changed: Notify::new(),
            }),
            work,
        }))
    }

    fn now(&self) -> i64 {
        self.clock.now_ms()
    }

    fn scope(&self) -> Option<&str> {
        self.settings.scope.as_deref()
    }

    /// `send(method, args, until)`: a mutation retried until `until`.
    fn send(
        &self,
        method: &str,
        args: Box<RawValue>,
        until: i64,
    ) -> impl Future<Output = Result<Value, C::Error>> + Send + 'static {
        let client = self.client.clone();
        let name = format!("{}.{}", self.settings.queue, method);
        let retry = RetryPolicy {
            until: Some(until),
            ..self.settings.retry.clone()
        };
        async move { client.mutate(&name, args, retry).await }
    }

    async fn run(self: Arc<Self>) -> Result<(), WorkerError<C::Error>> {
        let core = &self.core;
        let signal = core.signal.clone();
        // Dropping this future stops the tasks it started, as a stop would.
        let _stop_on_drop = signal.clone().drop_guard();
        {
            let core = core.clone();
            let signal = signal.clone();
            tokio::spawn(async move {
                signal.cancelled().await;
                let guard = core.enter();
                guard.borrow_mut().wake(None);
            });
        }
        let adjusting = CancellationToken::new();
        tokio::spawn(self.clone().adjusting(adjusting.clone()));
        let renewal = CancellationToken::new();
        if self.settings.renew {
            tokio::spawn(self.clone().renewing(renewal.clone()));
        }

        join_all((0..self.settings.claimers).map(|_| {
            let worker = self.clone();
            async move {
                if let Err(halt) = worker.clone().claimer().await {
                    let guard = worker.core.enter();
                    guard.borrow_mut().fail(halt);
                    worker.core.signal.cancel();
                }
            }
        }))
        .await;

        adjusting.cancel();
        let running: Vec<u64> = core.enter().borrow().running.iter().copied().collect();
        let settled = async {
            loop {
                let notified = core.running_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if running
                    .iter()
                    .all(|key| !core.enter().borrow().running.contains(key))
                {
                    return;
                }
                notified.await;
            }
        };
        // Work that ignores its stop, such as a command whose child still holds its output, must
        // not keep the worker from stopping.
        let abandoned = async {
            let Some(drain_ms) = self.settings.drain_ms else {
                return std::future::pending().await;
            };
            sleep(Duration::from_millis(drain_ms)).await;
            {
                let guard = core.enter();
                for entry in guard.borrow_mut().held.values_mut() {
                    entry.drained = true;
                    entry
                        .stop
                        .stop("The worker stopped before the job finished");
                }
            }
            sleep(Duration::from_millis(ABANDON_AFTER_MS)).await;
            let guard = core.enter();
            let ids: Vec<String> = guard
                .borrow()
                .held
                .values()
                .map(|entry| entry.job.id.clone())
                .collect();
            for id in ids {
                core.emit(QueueWorkerEvent::Unreported {
                    id,
                    error: "The job did not stop when its worker did".into(),
                });
            }
        };
        tokio::select! {
            _ = settled => {}
            _ = abandoned => {}
        }
        renewal.cancel();
        let failed = core.enter().borrow_mut().failed.take();
        // Give up the place in line at once, rather than when it runs out.
        if self.settings.wait && failed.is_none() {
            let args = raw(&LeaveArgs {
                owner: &self.settings.owner,
                max: 0,
                wait_ms: 0,
                scope: self.scope(),
            });
            let _ = self.send("claim", args, self.now() + 2_000).await;
        }
        match failed {
            None | Some(Halt::Shared) => Ok(()),
            Some(Halt::Client(error)) => Err(WorkerError::Client(error)),
            Some(Halt::Protocol(message)) => Err(WorkerError::Protocol(message)),
        }
    }

    /// Wait until there is room: no pause, and fewer busy jobs than the limit.
    async fn room(&self) {
        loop {
            enum Wait {
                Pause(u64),
                Room(oneshot::Receiver<()>),
            }
            let wait = {
                let guard = self.core.enter();
                let mut state = guard.borrow_mut();
                if self.core.signal.is_cancelled() {
                    return;
                }
                let pause = state.limiter.pause();
                if pause > 0 {
                    Wait::Pause(pause)
                } else if state.busy() >= state.limit() {
                    let (sender, receiver) = oneshot::channel();
                    state.room_waiters.push_back(sender);
                    Wait::Room(receiver)
                } else {
                    return;
                }
            };
            match wait {
                Wait::Pause(ms) => pause(ms, &self.core.signal).await,
                Wait::Room(receiver) => {
                    let _ = receiver.await;
                }
            }
        }
    }

    /// `whenReady()`: the shared readiness watch, opened if none is.
    fn when_ready(self: &Arc<Self>, guard: &Guard<'_, C::Error>) -> Arc<ReadyCell<C::Error>> {
        let mut state = guard.borrow_mut();
        if let Some(ready) = &state.ready {
            return ready.clone();
        }
        let cell = ReadyCell::pending();
        state.ready = Some(cell.clone());
        state.watching = true;
        tokio::spawn(self.clone().watching(cell.clone()));
        cell
    }

    async fn await_ready(self: &Arc<Self>) -> Result<(), Halt<C::Error>> {
        let cell = {
            let guard = self.core.enter();
            self.when_ready(&guard)
        };
        cell.done.cancelled().await;
        let mut outcome = cell.outcome.lock();
        match &mut *outcome {
            Outcome::Failed(error) => Err(error.take().map_or(Halt::Shared, Halt::Client)),
            Outcome::Ready | Outcome::Pending => Ok(()),
        }
    }

    async fn watching(self: Arc<Self>, cell: Arc<ReadyCell<C::Error>>) {
        enum End<E> {
            Shown,
            Refresh,
            Stopped,
            Failed(E),
        }
        let settings = &self.settings;
        let signal = &self.core.signal;
        let name = format!("{}.ready", settings.queue);
        let end = loop {
            // In line, claim again at half the place's life to keep it, and watch anew after.
            let refresh = async {
                if settings.wait {
                    sleep(Duration::from_millis(settings.wait_ms / 2)).await
                } else {
                    std::future::pending().await
                }
            };
            let result = tokio::select! {
                biased;
                _ = signal.cancelled() => break End::Stopped,
                _ = refresh => break End::Refresh,
                result = self.client.wait_until(&name, settings.ready_args.clone(), truthy) => result,
            };
            match result {
                Ok(_) => break End::Shown,
                Err(error) => {
                    if signal.is_cancelled() {
                        break End::Stopped;
                    }
                    if !error.is_transient() {
                        break End::Failed(error);
                    }
                    let delay = {
                        let guard = self.core.enter();
                        self.core.emit(QueueWorkerEvent::Waiting {
                            error: error.describe(),
                        });
                        let mut state = guard.borrow_mut();
                        let failures = state.failures;
                        state.failures += 1;
                        default_backoff(failures)
                    };
                    pause(delay, signal).await;
                }
            }
        };
        let guard = self.core.enter();
        let mut state = guard.borrow_mut();
        let outcome = match end {
            End::Shown => {
                state.shown = true;
                Outcome::Ready
            }
            End::Refresh => {
                state.ready = None;
                Outcome::Ready
            }
            End::Stopped => Outcome::Ready,
            End::Failed(error) => Outcome::Failed(Some(error)),
        };
        state.watching = false;
        *cell.outcome.lock() = outcome;
        cell.done.cancel();
    }

    async fn adjusting(self: Arc<Self>, stop: CancellationToken) {
        let every = Duration::from_millis(self.settings.adjust_every_ms.max(1));
        let mut ticks = interval_at(tokio::time::Instant::now() + every, every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            let guard = self.core.enter();
            // A full pool claims nothing, so claims cannot show that work waits: its readiness
            // watch does. Without it, a pool cut below the jobs it holds would never grow again.
            let full = {
                let state = guard.borrow();
                state.busy() >= state.limit()
            };
            if full {
                self.when_ready(&guard);
                let mut state = guard.borrow_mut();
                if state.shown {
                    state.limiter.want();
                }
            }
            let change = guard.borrow_mut().limiter.adjust();
            self.core.changed(&guard, change);
        }
    }

    /// One call renews every lease held since at least a renewal interval, every half interval.
    async fn renewing(self: Arc<Self>, stop: CancellationToken) {
        let settings = &self.settings;
        let renew_every = ((settings.lease_ms - settings.margin_ms) / 3).max(1) as i64;
        let every = Duration::from_millis((renew_every as u64 / 2).max(1));
        let mut ticks = interval_at(tokio::time::Instant::now() + every, every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            let renewed_at = self.now();
            let (due, args, until) = {
                let guard = self.core.enter();
                let state = guard.borrow();
                let due: Vec<(u64, &Held)> = state
                    .held
                    .iter()
                    .filter(|(_, entry)| {
                        renewed_at - entry.renewed_at >= renew_every && !entry.stop.is_stopped()
                    })
                    .map(|(key, entry)| (*key, entry))
                    .collect();
                if due.is_empty() {
                    continue;
                }
                let args = raw(&RenewArgs {
                    leases: due.iter().map(|(_, entry)| &entry.identity).collect(),
                    lease_ms: settings.lease_ms,
                    scope: self.scope(),
                });
                let until = due
                    .iter()
                    .map(|(_, entry)| entry.deadline)
                    .min()
                    .unwrap_or(renewed_at);
                (
                    due.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
                    args,
                    until,
                )
            };
            let worker = self.clone();
            let sent = self.send("renew", args, until);
            tokio::spawn(async move {
                // Each job's deadline stops its work if renewals stop landing.
                let Ok(expiries) = sent.await else { return };
                let guard = worker.core.enter();
                let mut state = guard.borrow_mut();
                for (index, key) in due.into_iter().enumerate() {
                    let Some(entry) = state.held.get_mut(&key) else {
                        continue;
                    };
                    if expiries.get(index).is_some_and(Value::is_null) {
                        entry.stop.stop("The lease was lost");
                        continue;
                    }
                    entry.renewed_at = renewed_at;
                    entry.deadline =
                        renewed_at + (worker.settings.lease_ms - worker.settings.margin_ms) as i64;
                    worker.arm(key, entry);
                }
            });
        }
    }

    /// `arm(entry)`: the job stops once its deadline passes, unless a renewal re-arms it first.
    fn arm(&self, key: u64, entry: &mut Held) {
        entry.timer.cancel();
        let timer = CancellationToken::new();
        entry.timer = timer.clone();
        let delay = (entry.deadline - self.now()).max(0) as u64;
        let core = self.core.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = timer.cancelled() => {}
                _ = sleep(Duration::from_millis(delay)) => {
                    let guard = core.enter();
                    if timer.is_cancelled() {
                        return;
                    }
                    if let Some(entry) = guard.borrow().held.get(&key) {
                        entry.stop.stop("The lease ran out");
                    }
                }
            }
        });
    }

    /// `start(job, sentAt)`: hold the job and run its work. Call holding the lock, borrowing nothing.
    fn start(self: &Arc<Self>, guard: &Guard<'_, C::Error>, job: Claim<Value>, sent_at: i64) {
        let deadline = sent_at + (self.settings.lease_ms - self.settings.margin_ms) as i64;
        if deadline <= self.now() {
            return self.core.emit(QueueWorkerEvent::Lost { id: job.id });
        }
        let stop = JobStop::new();
        let key = {
            let mut state = guard.borrow_mut();
            let key = state.next_key;
            state.next_key += 1;
            let mut entry = Held {
                identity: LeaseIdentity::from(&job),
                job: job.clone(),
                stop: stop.clone(),
                deadline,
                renewed_at: sent_at,
                timer: CancellationToken::new(),
                drained: false,
                idled: false,
            };
            self.arm(key, &mut entry);
            state.held.insert(key, entry);
            key
        };
        self.core
            .emit(QueueWorkerEvent::Claimed { job: job.clone() });
        let control = JobControl {
            target: self.core.clone(),
            key,
        };
        guard.borrow_mut().running.insert(key);
        let future = (self.work)(job, stop, control);
        tokio::spawn(self.clone().job(key, future));
    }

    async fn job(
        self: Arc<Self>,
        key: u64,
        future: BoxFuture<'static, Result<Box<RawValue>, String>>,
    ) {
        let result = match AssertUnwindSafe(future).catch_unwind().await {
            Ok(result) => result,
            Err(panic) => Err(panic_message(&*panic)),
        };
        let settings = &self.settings;
        let (method, args, take, until, id) = {
            let guard = self.core.enter();
            let mut state = guard.borrow_mut();
            let entry = state
                .held
                .remove(&key)
                .expect("a running job is held until its work ends");
            entry.timer.cancel();
            if entry.idled {
                state.idle -= 1;
            }
            // Abortable APIs fail with a generic error; the lease's reason says why.
            let reason = entry.stop.reason().map(str::to_owned);
            let (method, result, message) = match (&reason, &result) {
                (None, Ok(value)) => (Method::Complete, Some(&**value), None),
                _ if entry.drained && settings.release => (Method::Release, None, None),
                (Some(reason), _) => (Method::Fail, None, Some(reason.as_str())),
                (None, Err(message)) => (Method::Fail, None, Some(message.as_str())),
            };
            // This job's room passes to the jobs its report claims; the claimers keep out of the rest.
            let take = if settings.chain
                && method != Method::Release
                && !self.core.signal.is_cancelled()
                && state.limiter.pause() == 0
            {
                (settings.batch as i64).min(state.limit() - state.busy() + 1)
            } else {
                0
            };
            let next = (take > 0).then(|| {
                state.claiming += take - 1;
                NextClaim {
                    max: take,
                    lease_ms: settings.lease_ms,
                    wait_ms: settings.wait.then_some(settings.wait_ms),
                }
            });
            let identity = &entry.identity;
            let args = raw(&Report {
                id: &identity.id,
                owner: &identity.owner,
                token: identity.token,
                history: identity.history.as_ref(),
                scope: self.scope(),
                result,
                error: message.map(|message| ErrorBody { message }),
                next,
            });
            let error = message.map(str::to_owned);
            (
                method,
                args,
                take,
                entry.deadline + settings.margin_ms as i64,
                (entry.job.id.clone(), error),
            )
        };
        let sent_at = self.now();
        let reply = self.send(method.name(), args, until).await;
        let (id, error) = id;
        {
            let guard = self.core.enter();
            match reply {
                Ok(reply) => {
                    self.core.emit(match method {
                        Method::Complete => QueueWorkerEvent::Completed { id },
                        Method::Release => QueueWorkerEvent::Released { id },
                        Method::Fail => QueueWorkerEvent::Failed {
                            id,
                            error: error.unwrap_or_default(),
                        },
                    });
                    if take > 0 {
                        let next = match reply {
                            Value::Object(mut reply) => reply.remove("next").unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        match claimed_jobs(next) {
                            Ok(jobs) => {
                                guard.borrow_mut().claimed(jobs.len(), take);
                                let count = jobs.len() as i64;
                                for job in jobs {
                                    self.start(&guard, job, sent_at);
                                }
                                let mut state = guard.borrow_mut();
                                if count == take && state.busy() >= state.limit() {
                                    state.limiter.want();
                                }
                            }
                            Err(message) => {
                                guard.borrow_mut().fail(Halt::Protocol(message));
                                self.core.signal.cancel();
                            }
                        }
                    }
                }
                Err(error) => {
                    // Refused (the lease moved on) or never answered: either way the queue hands the
                    // job out again once the lease ends.
                    if error.failure_code() == Some("LEASE_LOST") {
                        self.core.emit(QueueWorkerEvent::Lost { id });
                    } else {
                        self.core.emit(QueueWorkerEvent::Unreported {
                            id,
                            error: error.describe(),
                        });
                    }
                }
            }
            {
                let mut state = guard.borrow_mut();
                if take > 0 {
                    state.claiming -= take - 1;
                    state.wake(Some(take - 1));
                }
            }
        }
        // TS leaves `running` in the task promise's `.finally`, a microtask after the report code:
        // jobs the report chained in get their first step while this job still counts as busy (a
        // chained job that fails at once chains nothing, for one). Yield once to keep that order.
        tokio::task::yield_now().await;
        let guard = self.core.enter();
        let mut state = guard.borrow_mut();
        state.running.remove(&key);
        state.wake(Some(1));
        drop(state);
        drop(guard);
        self.core.running_changed.notify_waiters();
    }

    async fn claimer(self: Arc<Self>) -> Result<(), Halt<C::Error>> {
        let settings = &self.settings;
        let signal = &self.core.signal;
        while !signal.is_cancelled() {
            self.room().await;
            self.await_ready().await?;
            enum Step {
                Stop,
                Probe(CancellationToken),
                Again,
                Claim {
                    take: i64,
                    sent_at: i64,
                    probe: Option<CancellationToken>,
                },
            }
            let step = {
                let guard = self.core.enter();
                let mut state = guard.borrow_mut();
                let confirmed = state.full >= 2 || (state.full >= 1 && settings.batch > 1);
                if signal.is_cancelled() {
                    Step::Stop
                } else if let (false, Some(probing)) = (confirmed, &state.probing) {
                    Step::Probe(probing.clone())
                } else if state.limiter.pause() > 0 || state.busy() >= state.limit() {
                    // Room was checked before waiting: check again now the queue has work.
                    Step::Again
                } else {
                    let probe = (!confirmed).then(|| {
                        let probe = CancellationToken::new();
                        state.probing = Some(probe.clone());
                        probe
                    });
                    // Room for this many, held until the claim answers.
                    let take = (settings.batch as i64)
                        .min(state.limit() - state.busy())
                        .max(1);
                    state.claiming += take;
                    Step::Claim {
                        take,
                        sent_at: self.now(),
                        probe,
                    }
                }
            };
            let (take, sent_at, probe) = match step {
                Step::Stop => return Ok(()),
                Step::Probe(probing) => {
                    probing.cancelled().await;
                    continue;
                }
                Step::Again => continue,
                Step::Claim {
                    take,
                    sent_at,
                    probe,
                } => (take, sent_at, probe),
            };
            let args = raw(&ClaimArgs {
                owner: &settings.owner,
                lease_ms: settings.lease_ms,
                scope: self.scope(),
                max: (settings.batch > 1 || settings.wait).then_some(take),
                wait_ms: settings.wait.then_some(settings.wait_ms),
            });
            // `await send(...)` lets the claimers already woken look first, as JavaScript runs the
            // continuations queued before this one: they see this claim probing and wait for it.
            tokio::task::yield_now().await;
            let reply = self
                .send("claim", args, sent_at + settings.lease_ms as i64)
                .await;
            let retry_in = {
                let guard = self.core.enter();
                let mut jobs = Vec::new();
                let mut retry_in = None;
                let mut exit = None;
                match reply.map(claimed_jobs) {
                    Ok(Ok(claimed)) => {
                        jobs = claimed;
                        guard.borrow_mut().failures = 0;
                    }
                    Ok(Err(message)) => exit = Some(Err(Halt::Protocol(message))),
                    Err(error) => {
                        if signal.is_cancelled() {
                            exit = Some(Ok(()));
                        } else if !error.is_transient() {
                            exit = Some(Err(Halt::Client(error)));
                        } else {
                            self.core.emit(QueueWorkerEvent::Waiting {
                                error: error.describe(),
                            });
                            let mut state = guard.borrow_mut();
                            retry_in = Some(default_backoff(state.failures));
                            state.failures += 1;
                        }
                    }
                }
                {
                    let mut state = guard.borrow_mut();
                    state.claiming -= take;
                    state.wake(Some(take));
                    if let Some(probe) = probe {
                        state.probing = None;
                        probe.cancel();
                    }
                }
                if let Some(exit) = exit {
                    return exit;
                }
                if retry_in.is_none() {
                    guard.borrow_mut().claimed(jobs.len(), take);
                    let count = jobs.len() as i64;
                    for job in jobs {
                        self.start(&guard, job, sent_at);
                    }
                    // The claim took all it asked for and filled the room: the queue may hold more.
                    let mut state = guard.borrow_mut();
                    if count == take && state.busy() >= state.limit() {
                        state.limiter.want();
                    }
                }
                retry_in
            };
            // Wait out a failed claim without holding its room.
            if let Some(ms) = retry_in {
                pause(ms, signal).await;
            }
        }
        Ok(())
    }
}
