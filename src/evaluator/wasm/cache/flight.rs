//! Coalesce cold image preparation without sharing invocation budgets or errors.
use anyhow::{anyhow, Result};
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

pub(super) struct Flights<T>(Mutex<HashMap<[u8; 32], Arc<Pending<T>>>>);

impl<T> Default for Flights<T> {
    fn default() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

pub(super) enum Attempt<'a, T> {
    Lead(Leader<'a, T>),
    Wait(Arc<Pending<T>>),
}

impl<T> Flights<T> {
    pub(super) fn join(&self, key: [u8; 32]) -> Result<Attempt<'_, T>> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| anyhow!("Wasm preparation lock poisoned"))?;
        if let Some(pending) = entries.get(&key) {
            return Ok(Attempt::Wait(pending.clone()));
        }
        let pending = Arc::new(Pending {
            state: Mutex::new(State::Running),
            changed: Condvar::new(),
        });
        entries.insert(key, pending.clone());
        Ok(Attempt::Lead(Leader {
            flights: self,
            key,
            pending,
            result: None,
        }))
    }
}

enum State<T> {
    Running,
    Done(Option<Arc<T>>),
}

pub(super) struct Pending<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
}

impl<T> Pending<T> {
    pub(super) fn wait(
        &self,
        deadline: Instant,
        check: impl Fn() -> Result<()>,
    ) -> Result<Option<Arc<T>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Wasm preparation wait lock poisoned"))?;
        loop {
            // Budget failure remains local: abandoning a wait must never cancel
            // another invocation's preparation or extend this one's deadline.
            check()?;
            if let State::Done(prepared) = &*state {
                return Ok(prepared.clone());
            }
            let timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(25));
            state = self
                .changed
                .wait_timeout(state, timeout)
                .map_err(|_| anyhow!("Wasm preparation wait lock poisoned"))?
                .0;
        }
    }
}

pub(super) struct Leader<'a, T> {
    flights: &'a Flights<T>,
    key: [u8; 32],
    pending: Arc<Pending<T>>,
    result: Option<Arc<T>>,
}

impl<T> Leader<'_, T> {
    pub(super) fn publish(mut self, result: Arc<T>) {
        self.result = Some(result);
    }
}

impl<T> Drop for Leader<'_, T> {
    fn drop(&mut self) {
        // This also runs on errors and unwinding. Publish only completed images;
        // waiters retry failed work using their own memory/deadline allowance.
        self.flights
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.key);
        *self
            .pending
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = State::Done(self.result.take());
        self.pending.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn same_bundle_has_one_preparer_and_distinct_bundles_are_independent() {
        let flights = Flights::<usize>::default();
        let Attempt::Lead(leader) = flights.join([1; 32]).unwrap() else {
            panic!("first request prepares")
        };
        let barrier = Barrier::new(17);
        std::thread::scope(|scope| {
            let waiters = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        let Attempt::Wait(pending) = flights.join([1; 32]).unwrap() else {
                            panic!("duplicate preparation")
                        };
                        barrier.wait();
                        pending
                            .wait(Instant::now() + Duration::from_secs(10), || Ok(()))
                            .unwrap()
                            .unwrap()
                    })
                })
                .collect::<Vec<_>>();
            let Attempt::Lead(other) = flights.join([2; 32]).unwrap() else {
                panic!("unrelated preparation blocked")
            };
            other.publish(Arc::new(2));
            barrier.wait();
            let result = Arc::new(1);
            leader.publish(result.clone());
            for waiter in waiters {
                assert!(Arc::ptr_eq(&waiter.join().unwrap(), &result));
            }
        });
        assert!(
            flights.0.lock().unwrap().is_empty(),
            "completed flights retain no images"
        );
    }

    #[test]
    fn failure_and_unwinding_wake_waiters_and_allow_retry() {
        for unwind in [false, true] {
            let flights = Flights::<usize>::default();
            let Attempt::Lead(leader) = flights.join([1; 32]).unwrap() else {
                unreachable!()
            };
            let Attempt::Wait(pending) = flights.join([1; 32]).unwrap() else {
                unreachable!()
            };
            if unwind {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _leader = leader;
                    panic!("preparation failure");
                }));
            } else {
                drop(leader);
            }
            assert!(pending
                .wait(Instant::now() + Duration::from_secs(1), || Ok(()))
                .unwrap()
                .is_none());
            assert!(matches!(flights.join([1; 32]).unwrap(), Attempt::Lead(_)));
        }
    }

    #[test]
    fn waiters_keep_their_own_deadlines_and_do_not_cancel_preparation() {
        let flights = Flights::<usize>::default();
        let Attempt::Lead(leader) = flights.join([1; 32]).unwrap() else {
            unreachable!()
        };
        let Attempt::Wait(pending) = flights.join([1; 32]).unwrap() else {
            unreachable!()
        };
        let deadline = Instant::now() + Duration::from_millis(1);
        assert!(pending
            .wait(deadline, || {
                anyhow::ensure!(Instant::now() < deadline, "local deadline exhausted");
                Ok(())
            })
            .is_err());
        assert!(matches!(flights.join([1; 32]).unwrap(), Attempt::Wait(_)));
        leader.publish(Arc::new(3));
        assert!(pending
            .wait(deadline, || Err(anyhow!("sticky local failure")))
            .is_err());
        assert_eq!(
            *pending
                .wait(Instant::now() + Duration::from_secs(1), || Ok(()))
                .unwrap()
                .unwrap(),
            3
        );
    }
}
