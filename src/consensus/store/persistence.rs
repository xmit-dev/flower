//! Persist applied state behind its in-memory publication.
//!
//! Applied state never needs its own flush: Raft logs already make every
//! applied entry durable on a quorum, and these writes use no-sync commits
//! that the next Immediate commit persists. They therefore need not delay the
//! apply either. Readers and Raft see a new state as soon as it is published;
//! one task writes queued states in apply order, coalescing whatever queued
//! while the disk was busy, for example behind a leader's deferred log flush.
//!
//! Recovery restarts from the last persisted state and replays later entries
//! from the log. Operations that would let Raft discard those entries or that
//! replace the whole state (snapshot checkpoints, purges and installations)
//! first drain the queue, and so do shutdown and tests that inspect or reopen
//! storage. Writes are deltas, so the first failure stops every later write
//! and fails the next apply; the node then stops and replays on restart.
use super::*;
use std::collections::VecDeque;
use std::sync::Weak;

/// One apply's encoded redb projection.
pub(super) struct StateWrite {
    pub(super) data: Vec<(String, Option<Vec<u8>>)>,
    pub(super) requests: Vec<(String, Vec<u8>)>,
    pub(super) deleted_requests: Vec<String>,
    pub(super) metadata: Vec<u8>,
    pub(super) partitions: BTreeMap<String, PartitionWrite>,
}

#[derive(Default)]
struct Queue {
    writes: VecDeque<StateWrite>,
    running: bool,
}

#[derive(Default)]
pub(super) struct Persistence {
    queue: std::sync::Mutex<Queue>,
    submitted: AtomicU64,
    persisted: AtomicU64,
    failure: std::sync::Mutex<Option<String>>,
    persisted_changed: tokio::sync::Notify,
    // When the last batch was taken, and whether a drain waits for the next.
    last: std::sync::Mutex<Option<std::time::Instant>>,
    urgent: std::sync::atomic::AtomicBool,
    wake: tokio::sync::Notify,
}

/// How long queued states wait to be written together. Each write rewrites
/// the B-tree pages of every key it changes; writing several applies at once
/// writes a key changed by all of them once, and shares their branch pages.
/// Servers validated it at startup (`Limits`); storage opened on its own
/// takes the default for anything else.
fn interval() -> std::time::Duration {
    static INTERVAL: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *INTERVAL.get_or_init(|| {
        super::super::limits::Limits::from_env().map_or_else(
            |_| super::super::limits::Limits::default().persist_interval,
            |limits| limits.persist_interval,
        )
    })
}

/// Background tasks reach the database only through a counted strong Store,
/// so closing storage can wait until none remains.
#[derive(Default)]
pub(super) struct Holders {
    count: std::sync::atomic::AtomicUsize,
    released: tokio::sync::Notify,
}

pub(super) struct Held {
    holders: Arc<Holders>,
    store: Option<Store>,
}

impl Held {
    pub(super) fn store(&self) -> &Store {
        self.store.as_ref().expect("held store")
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        // Release the database before announcing it.
        drop(self.store.take());
        self.holders.release();
    }
}

impl Holders {
    pub(super) fn hold(self: &Arc<Self>, inner: &Weak<Inner>) -> Option<Held> {
        self.count.fetch_add(1, Ordering::AcqRel);
        let held = Held {
            holders: self.clone(),
            store: inner.upgrade().map(|inner| Store {
                inner,
                raft_lifetime: None,
            }),
        };
        held.store.is_some().then_some(held)
    }

    fn release(&self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.released.notify_waiters();
    }

    /// Wait until no background task holds the database.
    pub(super) async fn idle(&self) {
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            released.await;
        }
    }
}

impl Persistence {
    pub(super) fn check(&self) -> anyhow::Result<()> {
        match &*self.failure.lock().expect("persistence failure lock") {
            Some(error) => bail!("applied state persistence failed: {error}"),
            None => Ok(()),
        }
    }

    /// Queue one write after every earlier one. Never waits for the disk.
    pub(super) fn submit(
        self: &Arc<Self>,
        inner: Weak<Inner>,
        holders: Arc<Holders>,
        write: StateWrite,
    ) {
        self.submitted.fetch_add(1, Ordering::AcqRel);
        let mut queue = self.queue.lock().expect("persistence queue lock");
        queue.writes.push_back(write);
        if queue.running {
            return;
        }
        queue.running = true;
        drop(queue);
        let persistence = self.clone();
        tokio::spawn(async move { persistence.run(inner, holders).await });
    }

    async fn run(self: Arc<Self>, inner: Weak<Inner>, holders: Arc<Holders>) {
        loop {
            let due = self
                .last
                .lock()
                .expect("persistence pacing lock")
                .map(|last| last + interval());
            if let Some(due) = due
                && !self.urgent.load(Ordering::Acquire)
                && due > std::time::Instant::now()
            {
                tokio::select! {
                    _ = tokio::time::sleep_until(due.into()) => {}
                    _ = self.wake.notified() => {}
                }
            }
            self.urgent.store(false, Ordering::Release);
            *self.last.lock().expect("persistence pacing lock") = Some(std::time::Instant::now());
            let writes: Vec<StateWrite> = {
                let mut queue = self.queue.lock().expect("persistence queue lock");
                // Writes are deltas: never write past one that failed.
                if queue.writes.is_empty() || self.check().is_err() {
                    queue.running = false;
                    return;
                }
                queue.writes.drain(..).collect()
            };
            let count = writes.len() as u64;
            let Some(held) = holders.hold(&inner) else {
                // Storage closed: unpersisted states replay from the log.
                self.fail("storage closed".into());
                self.queue.lock().expect("persistence queue lock").running = false;
                return;
            };
            let store = held.store();
            let tables = store.inner.tables;
            // No flush: the next durable batch of this database persists it.
            let result = store
                .batched(
                    false,
                    StorageTrace::new(store.inner.id, "persist"),
                    move |transaction, profile| {
                        for write in &writes {
                            {
                                let mut data = transaction.open_table(tables.data)?;
                                for (key, value) in &write.data {
                                    match value {
                                        Some(value) => {
                                            data.insert(key.as_bytes(), value.as_slice())?;
                                        }
                                        None => {
                                            data.remove(key.as_bytes())?;
                                        }
                                    }
                                }
                                let mut requests = transaction.open_table(tables.requests)?;
                                for key in &write.deleted_requests {
                                    requests.remove(key.as_bytes())?;
                                }
                                for (key, value) in &write.requests {
                                    requests.insert(key.as_bytes(), value.as_slice())?;
                                }
                                let mut meta = transaction.open_table(tables.meta)?;
                                meta.insert(STATE_META, write.metadata.as_slice())?;
                            }
                            write_partitions(transaction, tables, &write.partitions, profile)?;
                        }
                        Ok(())
                    },
                )
                .await;
            drop(held);
            if let Err(error) = result {
                // Record the failure before a later submit can start a writer.
                self.fail(format!("{error:#}"));
                self.queue.lock().expect("persistence queue lock").running = false;
                return;
            }
            self.persisted.fetch_add(count, Ordering::AcqRel);
            self.persisted_changed.notify_waiters();
        }
    }

    fn fail(&self, error: String) {
        self.failure
            .lock()
            .expect("persistence failure lock")
            .get_or_insert(error);
        self.persisted_changed.notify_waiters();
    }

    /// How many states have been submitted.
    pub(super) fn submitted_count(&self) -> u64 {
        self.submitted.load(Ordering::Acquire)
    }

    /// How many submitted states have been written.
    pub(super) fn persisted_count(&self) -> u64 {
        self.persisted.load(Ordering::Acquire)
    }

    /// Wait until every state submitted so far has been written.
    pub(super) async fn drain(&self) -> anyhow::Result<()> {
        let target = self.submitted.load(Ordering::Acquire);
        self.urgent.store(true, Ordering::Release);
        self.wake.notify_one();
        loop {
            let changed = self.persisted_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.check()?;
            if self.persisted.load(Ordering::Acquire) >= target {
                return Ok(());
            }
            changed.await;
        }
    }
}
