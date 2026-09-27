//! Defer the leader's own log flush while followers can commit without it.
//!
//! A Raft leader may write its log in parallel with replication, and need not
//! be part of the durable commit quorum at all: OpenRaft counts the leader's
//! own vote only when its append callback reports durability. With at least
//! three voters, the followers alone form a majority, so a commit normally
//! completes on their parallel flushes. The leader's appends commit to redb
//! without an fsync; a later Immediate commit makes them durable and then
//! completes their callbacks. It runs when an append stays uncommitted past a
//! short grace period (for example, a follower is down, so the leader's vote
//! is needed) and otherwise at a bounded interval, which also lets redb reuse
//! the pages that no-sync commits free.
//!
//! Every acknowledged write remains durable on a majority. A leader that
//! crashes can lose its unflushed tail, exactly like a replica that never
//! received it; it returns as a follower and catches up. Durable redb state is
//! always a prefix of its commit order, so applied state never runs ahead of
//! the recovered log.
use super::*;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{OnceLock, Weak};
use std::time::Duration;

pub(super) struct Settings {
    pub enabled: bool,
    pub grace: Duration,
    pub interval: Duration,
}

pub(super) fn settings() -> &'static Settings {
    static SETTINGS: OnceLock<Settings> = OnceLock::new();
    SETTINGS.get_or_init(|| {
        let millis = |name: &str, default: u64| {
            Duration::from_millis(
                std::env::var(name)
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(default),
            )
        };
        Settings {
            enabled: std::env::var("FLOWER_LEADER_FLUSH").map_or(true, |mode| mode != "immediate"),
            grace: millis("FLOWER_LEADER_FLUSH_GRACE_MS", 30),
            interval: millis("FLOWER_LEADER_FLUSH_INTERVAL_MS", 500),
        }
    })
}

struct Held {
    last_index: u64,
    appended: Instant,
    callback: LogFlushed<TypeConfig>,
}

#[derive(Default)]
pub(super) struct LazyFlush {
    held: std::sync::Mutex<VecDeque<Held>>,
    // One past the last committed index reported by save_committed().
    committed: AtomicU64,
    // True while applied membership has a majority without any single voter.
    follower_quorum: AtomicBool,
    wake: tokio::sync::Notify,
    // Appends ever held, for tests.
    deferred: AtomicU64,
}

impl LazyFlush {
    pub(super) fn update_membership(&self, membership: &StoredMembership<u64, BasicNode>) {
        let configs = membership.membership().get_joint_config();
        let quorum = !configs.is_empty() && configs.iter().all(|voters| voters.len() >= 3);
        self.follower_quorum.store(quorum, Ordering::Release);
    }

    pub(super) fn applies(&self, callback: &LogFlushed<TypeConfig>) -> bool {
        settings().enabled
            && callback.is_leader_append()
            && self.follower_quorum.load(Ordering::Acquire)
    }

    pub(super) fn committed(&self, committed: Option<LogId<u64>>) {
        let next = committed.map_or(0, |log| log.index.saturating_add(1));
        if self.committed.fetch_max(next, Ordering::AcqRel) < next {
            self.wake.notify_one();
        }
    }

    /// The append is already visible in redb. Pushing after releasing the I/O
    /// guard is safe: a flush that drains this entry starts its Immediate
    /// commit later, and one that drained earlier leaves it for the next.
    pub(super) fn hold(&self, last_index: u64, callback: LogFlushed<TypeConfig>) {
        self.deferred.fetch_add(1, Ordering::Relaxed);
        self.held.lock().expect("lazy flush lock").push_back(Held {
            last_index,
            appended: Instant::now(),
            callback,
        });
        self.wake.notify_one();
    }

    #[cfg(test)]
    pub(super) fn deferred(&self) -> u64 {
        self.deferred.load(Ordering::Relaxed)
    }

    fn due(&self) -> Option<Instant> {
        let held = self.held.lock().expect("lazy flush lock");
        let oldest = held.front()?.appended + settings().interval;
        let committed = self.committed.load(Ordering::Acquire);
        let uncommitted = held
            .iter()
            .find(|entry| entry.last_index >= committed)
            .map(|entry| entry.appended + settings().grace);
        Some(uncommitted.map_or(oldest, |grace| grace.min(oldest)))
    }
}

pub(super) fn spawn(inner: Weak<Inner>, holders: Arc<persistence::Holders>, lazy: Arc<LazyFlush>) {
    tokio::spawn(async move {
        // Recheck at least this often, so the task exits once storage closes.
        const IDLE: Duration = Duration::from_secs(1);
        loop {
            let wait = lazy.due().map_or(IDLE, |due| {
                due.saturating_duration_since(Instant::now()).min(IDLE)
            });
            if !wait.is_zero() {
                tokio::select! {
                    _ = lazy.wake.notified() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
            }
            if lazy.due().is_some_and(|due| due <= Instant::now()) {
                let Some(held) = holders.hold(&inner) else {
                    return;
                };
                flush(held.store(), &lazy).await;
                drop(held);
            } else if inner.strong_count() == 0 {
                return;
            }
        }
    });
}

/// Make every held append durable and complete its callback, with an I/O
/// error if the flush failed. The flush joins the database's next batch, so
/// it shares a fsync with any other replica's durable write.
pub(super) async fn flush(store: &Store, lazy: &Arc<LazyFlush>) {
    let id = store.inner.id;
    let tables = store.inner.tables;
    // Each held append already committed, without a flush, before it was held.
    let held = std::mem::take(&mut *lazy.held.lock().expect("lazy flush lock"));
    if held.is_empty() {
        return;
    }
    let flushed = store
        .batched(
            true,
            StorageTrace::new(id, "leader_flush"),
            move |transaction, _| {
                // Rewrite an existing value so redb performs a real durable
                // commit, persisting every earlier deferred one.
                let mut meta = transaction.open_table(tables.meta)?;
                meta.insert("node_id", serde_json::to_vec(&id)?.as_slice())?;
                Ok(())
            },
        )
        .await
        .map_err(|error| format!("{error:#}"));
    for entry in held {
        entry
            .callback
            .log_io_completed(flushed.clone().map_err(std::io::Error::other));
    }
}
