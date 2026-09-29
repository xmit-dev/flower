//! Node-wide preparation admission. Waiting requests own bytes, never snapshots.
//! FIFO partition lanes rotate fairly; maintenance/control has separate capacity,
//! and so has deployment work, which can hold a slot for seconds.
use super::{ApiError, App, tuning};
use axum::http::StatusCode;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::Instant,
};
use tokio::sync::oneshot;

pub(super) mod ingress;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Class {
    User,
    /// Short operator work: maintenance, keys, retention, an online
    /// deployment's cutover (evaluating for at most FLOWER_DEPLOYMENT_CUTOVER_MS).
    /// Each logical database's writer waits for it while holding its lane.
    Control,
    /// Preparing a bundle: online, blocking or staged deployment work. It
    /// compiles and evaluates for seconds, so it must not hold a slot that
    /// writers of other logical databases wait for.
    Deployment,
}
impl Class {
    const ALL: [Self; 3] = [Self::Control, Self::Deployment, Self::User];
    fn index(self) -> usize {
        match self {
            Self::User => 0,
            Self::Control => 1,
            Self::Deployment => 2,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Control => "control",
            Self::Deployment => "deployment",
        }
    }
}

pub(super) struct Pool {
    state: Mutex<State>,
    workers: [usize; 3],
    memory: [usize; 3],
    input: [usize; 3],
    per_job: usize,
    exhausted: &'static str,
}
#[derive(Default)]
struct State {
    classes: [Queue; 3],
    next: u64,
    rejected: u64,
    canceled: u64,
    admitted: u64,
    scheduling: bool,
}
#[derive(Default)]
struct Queue {
    lanes: BTreeMap<String, VecDeque<Waiter>>,
    ready: VecDeque<String>,
    active: usize,
    input: usize,
    queued: usize,
}
struct Waiter {
    id: u64,
    queued_at: Instant,
    input: Input,
    send: oneshot::Sender<Permit>,
}
pub(super) struct Input {
    pool: Weak<Pool>,
    class: Class,
    bytes: usize,
}
struct Active {
    pool: Weak<Pool>,
    class: Class,
    _input: Input,
}
#[derive(Clone)]
pub(super) struct Permit {
    _active: Arc<Active>,
}
struct Waiting {
    pool: Arc<Pool>,
    class: Class,
    lane: String,
    id: u64,
    armed: bool,
}

fn overloaded(message: &str) -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "ADMISSION_OVERLOADED",
        message.into(),
    )
}

pub(super) fn input_bytes(value: &Value) -> usize {
    fn size(value: &Value) -> usize {
        match value {
            Value::String(value) => value.capacity(),
            Value::Array(values) => values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(values.iter().map(size).fold(0usize, usize::saturating_add)),
            Value::Object(values) => values.iter().fold(0usize, |bytes, (key, value)| {
                bytes
                    .saturating_add(key.capacity())
                    .saturating_add(128)
                    .saturating_add(size(value))
            }),
            _ => 0,
        }
    }
    // Account for the future, lane entry and small requests as well as JSON.
    size(value).saturating_add(512)
}
fn lane(app: &App) -> String {
    // This binding was established by the authenticated partition router and
    // checked against Raft ownership. Request JSON never determines the lane.
    match app.consensus.partition_binding() {
        Some(binding) => format!("partition:{}", binding.partition),
        None => "root".into(),
    }
}
pub(super) async fn acquire(app: &App, class: Class, input: &Value) -> Result<Permit, ApiError> {
    app.admission
        .acquire(lane(app), class, input_bytes(input))
        .await
}
pub(super) async fn acquire_retained(app: &App, class: Class) -> Result<Permit, ApiError> {
    app.admission.acquire(lane(app), class, 0).await
}

impl Pool {
    pub(super) fn configured() -> anyhow::Result<Arc<Self>> {
        let settings = tuning::settings()?;
        let evaluator = crate::evaluator::config::settings()?;
        Ok(Self::with_deployment(
            [
                settings.preparation_workers,
                settings.control_workers,
                settings.deployment_workers,
            ],
            [
                settings.preparation_memory_bytes,
                settings.control_memory_bytes,
                settings.deployment_memory_bytes,
            ],
            // Deployment inputs (bundles) are retained as control input.
            [
                settings.queued_bytes,
                settings.control_queued_bytes,
                settings.control_queued_bytes,
            ],
            evaluator
                .guest_memory_bytes
                .checked_add(evaluator.rust_memory_bytes)
                .ok_or_else(|| anyhow::anyhow!("evaluation memory reservation overflow"))?,
        ))
    }
    /// User and control capacity; deployment work gets as much as control.
    #[cfg(test)]
    pub(super) fn new(
        workers: [usize; 2],
        memory: [usize; 2],
        input: [usize; 2],
        per_job: usize,
    ) -> Arc<Self> {
        Self::with_deployment(
            [workers[0], workers[1], workers[1]],
            [memory[0], memory[1], memory[1]],
            [input[0], input[1], input[1]],
            per_job,
        )
    }
    pub(super) fn with_deployment(
        workers: [usize; 3],
        memory: [usize; 3],
        input: [usize; 3],
        per_job: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            workers,
            memory,
            input,
            per_job,
            exhausted: "node queued-input byte budget exhausted; retry later",
        })
    }
    /// Bytes that open watches hold, apart from what requests queue in, so
    /// that watches can never exhaust the requests' budget.
    pub(super) fn watches(bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            workers: [0; 3],
            memory: [0; 3],
            input: [bytes, 0, 0],
            per_job: 0,
            exhausted: "watch byte budget exhausted (FLOWER_WATCH_RETAINED_BYTES); retry later",
        })
    }
    /// Bytes retained, and the budget, of a pool made by `watches`.
    pub(super) fn retained(&self) -> (usize, usize) {
        let state = self.state.lock().expect("admission mutex");
        (state.classes[0].input, self.input[0])
    }
    pub(super) fn retain(self: &Arc<Self>, class: Class, bytes: usize) -> Result<Input, ApiError> {
        let mut state = self.state.lock().expect("admission mutex");
        let queue = &mut state.classes[class.index()];
        let Some(total) = queue
            .input
            .checked_add(bytes)
            .filter(|total| *total <= self.input[class.index()])
        else {
            state.rejected += 1;
            return Err(overloaded(self.exhausted));
        };
        queue.input = total;
        Ok(Input {
            pool: Arc::downgrade(self),
            class,
            bytes,
        })
    }
    pub(in crate::service) async fn acquire(
        self: &Arc<Self>,
        lane: String,
        class: Class,
        bytes: usize,
    ) -> Result<Permit, ApiError> {
        let input = self.retain(class, bytes.saturating_add(lane.len()))?;
        let (send, receive) = oneshot::channel();
        let id = {
            let mut state = self.state.lock().expect("admission mutex");
            state.next = state
                .next
                .checked_add(1)
                .expect("admission sequence exhausted");
            let id = state.next;
            let queue = &mut state.classes[class.index()];
            let pending = queue.lanes.entry(lane.clone()).or_default();
            if pending.is_empty() {
                queue.ready.push_back(lane.clone());
            }
            pending.push_back(Waiter {
                id,
                input,
                send,
                queued_at: Instant::now(),
            });
            queue.queued += 1;
            id
        };
        let mut waiting = Waiting {
            pool: self.clone(),
            class,
            lane,
            id,
            armed: true,
        };
        self.schedule();
        let result = receive
            .await
            .map_err(|_| overloaded("preparation admission stopped"));
        // The scheduler has already removed a successfully delivered waiter.
        // Only cancellation must search the pending lane; scanning it here
        // makes every admitted request O(queue length) under the global mutex.
        if result.is_ok() {
            waiting.armed = false;
        }
        drop(waiting);
        result
    }
    fn schedule(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().expect("admission mutex");
            if state.scheduling {
                return;
            }
            state.scheduling = true;
        }
        loop {
            let mut deliveries = Vec::new();
            {
                let mut state = self.state.lock().expect("admission mutex");
                for class in Class::ALL {
                    let queue = &mut state.classes[class.index()];
                    let capacity =
                        self.workers[class.index()].min(self.memory[class.index()] / self.per_job);
                    while queue.active < capacity {
                        let Some(lane) = queue.ready.pop_front() else {
                            break;
                        };
                        let pending = queue.lanes.get_mut(&lane).expect("ready lane");
                        let waiter = pending.pop_front().expect("nonempty ready lane");
                        if pending.is_empty() {
                            queue.lanes.remove(&lane);
                        } else {
                            queue.ready.push_back(lane);
                        }
                        queue.queued -= 1;
                        queue.active += 1;
                        let permit = Permit {
                            _active: Arc::new(Active {
                                pool: Arc::downgrade(self),
                                class,
                                _input: waiter.input,
                            }),
                        };
                        deliveries.push((waiter.send, permit));
                    }
                }
                if deliveries.is_empty() {
                    state.scheduling = false;
                    return;
                }
                state.admitted = state.admitted.saturating_add(deliveries.len() as u64);
            }
            // Failed delivery drops a lease, which reschedules. Never drop leases
            // while holding the mutex, including cancellation and closed channels.
            for (send, permit) in deliveries {
                let _ = send.send(permit);
            }
        }
    }
    pub(super) fn metrics(&self) -> Value {
        let state = self.state.lock().expect("admission mutex");
        let classes:Vec<_> = [Class::User, Class::Control, Class::Deployment].into_iter().map(|class| {
            let (i, queue) = (class.index(), &state.classes[class.index()]);
            let oldest = queue.lanes.values().filter_map(|lane|lane.front()).map(|waiter|waiter.queued_at.elapsed().as_millis()).max().unwrap_or(0);
            json!({"class":class.name(),"active":queue.active,"queued":queue.queued,
                "lanes":queue.lanes.len(),"retainedInputBytes":queue.input,"inputBudgetBytes":self.input[i],
                "reservedEvaluationBytes":queue.active.saturating_mul(self.per_job),"memoryBudgetBytes":self.memory[i],
                "workerBudget":self.workers[i],"oldestQueuedMs":oldest})
        }).collect();
        json!({"classes":classes,"admitted":state.admitted,"rejected":state.rejected,"canceled":state.canceled})
    }
}
impl Drop for Input {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.state.lock().expect("admission mutex").classes[self.class.index()].input -=
                self.bytes;
        }
    }
}

impl Input {
    fn grow(&mut self, additional: usize) -> Result<(), ApiError> {
        let Some(pool) = self.pool.upgrade() else {
            return Err(overloaded("request admission stopped"));
        };
        let mut added = pool.retain(self.class, additional)?;
        self.bytes += added.bytes;
        added.bytes = 0;
        Ok(())
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.state.lock().expect("admission mutex").classes[self.class.index()].active -= 1;
            pool.schedule();
        }
    }
}
impl Drop for Waiting {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let removed = {
            let mut state = self.pool.state.lock().expect("admission mutex");
            let queue = &mut state.classes[self.class.index()];
            let removed = queue.lanes.get_mut(&self.lane).and_then(|pending| {
                pending
                    .iter()
                    .position(|waiter| waiter.id == self.id)
                    .and_then(|index| pending.remove(index))
            });
            if removed.is_some() {
                queue.queued -= 1;
                if queue.lanes.get(&self.lane).is_some_and(VecDeque::is_empty) {
                    queue.lanes.remove(&self.lane);
                    queue.ready.retain(|lane| lane != &self.lane);
                }
                state.canceled += 1;
            }
            removed
        };
        drop(removed);
        self.pool.schedule();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Poll;
    fn pool() -> Arc<Pool> {
        Pool::new([1, 1], [10, 10], [4096, 4096], 10)
    }

    #[tokio::test]
    async fn partition_lanes_are_fifo_and_round_robin_with_reserved_control_capacity() {
        let pool = pool();
        let first = pool.acquire("busy".into(), Class::User, 100).await.unwrap();
        let mut a = Box::pin(pool.acquire("a".into(), Class::User, 100));
        let mut a2 = Box::pin(pool.acquire("a".into(), Class::User, 100));
        let mut b = Box::pin(pool.acquire("b".into(), Class::User, 100));
        assert!(matches!(futures_util::poll!(&mut a), Poll::Pending));
        assert!(matches!(futures_util::poll!(&mut a2), Poll::Pending));
        assert!(matches!(futures_util::poll!(&mut b), Poll::Pending));
        let control = pool
            .acquire("maintenance".into(), Class::Control, 100)
            .await
            .unwrap();
        assert_eq!(pool.metrics()["classes"][1]["active"], 1);
        drop(first);
        let a = a.await.unwrap();
        assert!(matches!(futures_util::poll!(&mut b), Poll::Pending));
        drop(a);
        let b = b.await.unwrap();
        assert!(matches!(futures_util::poll!(&mut a2), Poll::Pending));
        drop(b);
        drop(a2.await.unwrap());
        drop(control);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
    }

    #[tokio::test]
    async fn cancellation_releases_queued_bytes_but_active_clones_keep_their_reservation() {
        let pool = pool();
        let active = pool.acquire("a".into(), Class::User, 100).await.unwrap();
        let running = active.clone();
        let mut canceled = Box::pin(pool.acquire("b".into(), Class::User, 2000));
        assert!(matches!(futures_util::poll!(&mut canceled), Poll::Pending));
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 2102);
        drop(canceled);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 101);
        assert_eq!(pool.metrics()["classes"][0]["queued"], 0);
        let mut next = Box::pin(pool.acquire("b".into(), Class::User, 100));
        drop(active);
        assert!(matches!(futures_util::poll!(&mut next), Poll::Pending));
        drop(running);
        drop(next.await.unwrap());
        assert_eq!(pool.metrics()["classes"][0]["active"], 0);
        assert_eq!(pool.metrics()["canceled"], 1);
    }

    #[tokio::test]
    async fn memory_and_input_budgets_are_shared_across_lanes() {
        let pool = Pool::new([8, 1], [10, 10], [1024, 1024], 10);
        let active = pool.acquire("a".into(), Class::User, 700).await.unwrap();
        assert!(
            pool.acquire("another-partition".into(), Class::User, 700)
                .await
                .is_err()
        );
        let mut next = Box::pin(pool.acquire("b".into(), Class::User, 100));
        assert!(matches!(futures_util::poll!(&mut next), Poll::Pending));
        assert_eq!(pool.metrics()["classes"][0]["reservedEvaluationBytes"], 10);
        drop(active);
        drop(next.await.unwrap());
    }

    #[tokio::test]
    async fn cancellation_after_delivery_releases_the_slot_and_preserves_queued_successors() {
        let pool = pool();
        let first = pool.acquire("a".into(), Class::User, 100).await.unwrap();
        let mut canceled = Box::pin(pool.acquire("a".into(), Class::User, 200));
        let mut next = Box::pin(pool.acquire("a".into(), Class::User, 300));
        assert!(matches!(futures_util::poll!(&mut canceled), Poll::Pending));
        assert!(matches!(futures_util::poll!(&mut next), Poll::Pending));
        drop(first); // Its successor's permit is delivered but not yet polled.
        assert_eq!(pool.metrics()["classes"][0]["queued"], 1);
        drop(canceled);
        let admitted = next.await.unwrap();
        assert_eq!(pool.metrics()["classes"][0]["queued"], 0);
        assert_eq!(pool.metrics()["classes"][0]["active"], 1);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 301);
        drop(admitted);
        assert_eq!(pool.metrics()["classes"][0]["active"], 0);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
    }
}
