//! Redb storage: every acknowledgement follows immediate-durability commits on
//! a quorum. Votes, follower appends, snapshots and purges flush before they
//! complete. A leader of at least three voters defers its own log flush
//! (`lazy_flush`), and applied state reaches redb behind its in-memory
//! publication (`persistence`). Disk work uses Tokio's blocking pool so fsync
//! never blocks Raft heartbeats.
//!
//! A process hosting replicas of several groups keeps them in one database
//! under per-replica table prefixes (`Storage::Shared`). Their appends and
//! applied-state writes commit in shared batches (`shared`): one transaction,
//! and one flush when anything in it must be durable.

mod profile;
use profile::{StoragePhase, StorageTrace};
mod lazy_flush;
mod persistence;
mod shared;
pub use shared::SharedDatabase;

mod partition_storage;
use partition_storage::*;
mod retention;
use retention::*;
mod snapshots;
use snapshots::*;
mod snapshot_data;
use snapshot_data::DeferredImage;
pub use snapshot_data::SnapshotData;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock as PublishedLock};
use std::time::Instant;

use anyhow::{Context, bail};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, Snapshot as RaftSnapshot, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, TableHandle,
    WriteTransaction,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, watch};

use super::{
    ApplyResult, Commit, CommitResult, CompactBatch, PartitionBinding, PartitionInfo, RaftCommand,
    Receipt, Receipts, Records, Snapshot, TypeConfig,
    partitions::{self, PartitionState, Partitions},
};

/// Where a replica keeps its state.
pub enum Storage {
    /// A directory of its own, with the database `flower.redb` inside it.
    Directory(PathBuf),
    /// The database of the process that hosts it, shared with its other
    /// replicas under a table prefix of its own. Snapshot transfers use
    /// `directory`.
    Shared {
        database: Arc<SharedDatabase>,
        prefix: String,
        directory: PathBuf,
    },
}

impl From<PathBuf> for Storage {
    fn from(directory: PathBuf) -> Self {
        Storage::Directory(directory)
    }
}

impl From<&std::path::Path> for Storage {
    fn from(directory: &std::path::Path) -> Self {
        Storage::Directory(directory.to_owned())
    }
}

type Table<K> = TableDefinition<'static, K, &'static [u8]>;
// Application keys are UTF-8, stored as bytes: redb compares bytes as they
// are, where a string key is checked as UTF-8 at every comparison.
type PairTable = Table<(&'static [u8], &'static [u8])>;

/// One replica's tables. Replicas that share a database prefix their names;
/// the empty prefix keeps the names of a database holding a single replica.
#[derive(Clone, Copy)]
pub(super) struct Tables {
    meta: Table<&'static str>,
    logs: Table<u64>,
    data: Table<&'static [u8]>,
    requests: Table<&'static [u8]>,
    partition_meta: Table<&'static str>,
    partition_data: PairTable,
    partition_requests: PairTable,
    partition_base_data: PairTable,
    partition_base_requests: PairTable,
    partition_chunks: PairTable,
}

impl Tables {
    fn new(prefix: &str) -> Self {
        // A replica opens once per storage lifetime; its prefixed names live
        // as long as the process.
        let name = |table: &'static str| -> &'static str {
            if prefix.is_empty() {
                table
            } else {
                Box::leak(format!("{prefix}{table}").into_boxed_str())
            }
        };
        Self {
            meta: TableDefinition::new(name("raft_meta_v1")),
            logs: TableDefinition::new(name("raft_logs_v1")),
            data: TableDefinition::new(name("application_data_v4")),
            requests: TableDefinition::new(name("application_requests_v4")),
            partition_meta: TableDefinition::new(name("logical_partitions_v1")),
            partition_data: TableDefinition::new(name("logical_partition_data_v3")),
            partition_requests: TableDefinition::new(name("logical_partition_requests_v3")),
            partition_base_data: TableDefinition::new(name("partition_copy_base_data_v3")),
            partition_base_requests: TableDefinition::new(name("partition_copy_base_requests_v3")),
            partition_chunks: TableDefinition::new(name("partition_difference_chunks_v3")),
        }
    }
}

// Tests inspect a single-replica database by its unprefixed names.
#[cfg(test)]
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta_v1");
#[cfg(test)]
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs_v1");
#[cfg(test)]
const DATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("application_data_v4");
#[cfg(test)]
const REQUESTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("application_requests_v4");
const STATE_META: &str = "state_v4";
/// How far above the stored version bound a restarted process starts.
const RESTART_MARGIN: u64 = 1 << 24;
// Present when index entries use the current layout.
const INDEX_LAYOUT_META: &str = "index_layout_v2";
const PREVIOUS_STATE_META: &str = "state_v3";
const LEGACY_STATE_META: &str = "state_v2";
// An older binary must fail to decode the retired blob instead of silently
// opening an empty or stale application after this one-way storage migration.
const RETIRED_STATE: &[u8] = br#"{"storage_format":4,"error":"application state uses materialized graph generations and deferred projection recovery; use a current Flower binary"}"#;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredState {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    application: Snapshot,
    #[serde(default)]
    partitions: Partitions,
}

#[derive(Clone, Serialize, Deserialize)]
struct StateMetadata {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    revision: u64,
    // Above every version stored so far, which a restart must not reuse;
    // persistence raises it to cover what it writes (persistence.rs).
    #[serde(default)]
    versions: u64,
}

impl From<&StoredState> for StateMetadata {
    fn from(state: &StoredState) -> Self {
        Self {
            last_applied: state.last_applied,
            membership: state.membership.clone(),
            revision: state.application.revision,
            versions: super::versions_high_water(),
        }
    }
}

/// The application tables as stored now, served from a read snapshot rather
/// than loaded into memory.
fn stored_application(db: &Database, tables: Tables, revision: u64) -> anyhow::Result<Snapshot> {
    let (data, requests) = stored_backings(db, tables)?;
    Ok(Snapshot {
        revision,
        data: Records::backed(data),
        requests: Receipts::backed(requests),
    })
}

/// The application's data and receipt tables in one read snapshot.
fn stored_backings(
    db: &Database,
    tables: Tables,
) -> anyhow::Result<(Arc<super::backing::Backing>, Arc<super::backing::Backing>)> {
    let transaction = db.begin_read()?;
    Ok((
        Arc::new(super::backing::Backing::plain(
            transaction.open_table(tables.data)?,
        )),
        Arc::new(super::backing::Backing::plain(
            transaction.open_table(tables.requests)?,
        )),
    ))
}

/// Versions for a delta's writes, stored with them and published with them,
/// so a record keeps its version when it moves from memory to disk.
#[derive(Default)]
struct DeltaVersions {
    data: BTreeMap<String, u64>,
    requests: BTreeMap<String, u64>,
}

/// The only application allocations needed to stage an apply are changed keys
/// and new receipts. The locked state remains untouched until atomic commit.
struct ApplicationDelta {
    revision: u64,
    data: BTreeMap<String, Option<Value>>,
    requests: BTreeMap<String, Receipt>,
    deleted_requests: BTreeSet<String>,
}

impl ApplicationDelta {
    fn new(revision: u64) -> Self {
        Self {
            revision,
            data: BTreeMap::new(),
            requests: BTreeMap::new(),
            deleted_requests: BTreeSet::new(),
        }
    }

    /// Publish into `state`, with the versions stored with these writes, or
    /// fresh ones for state that is not stored that way.
    /// Apply the delta to `state` as persistence batch `sequence` writes it.
    fn publish(self, state: &mut Snapshot, versions: Option<&DeltaVersions>, sequence: u64) {
        for (key, value) in self.data {
            match value {
                Some(value) => {
                    let version =
                        versions.map_or_else(super::next_version, |versions| versions.data[&key]);
                    state
                        .data
                        .insert_versioned(key.clone(), Arc::new(value), version);
                }
                None => {
                    state.data.remove(&key);
                }
            }
            state.data.stamp(&key, sequence);
        }
        for key in self.deleted_requests {
            state.requests.remove(&key);
            state.requests.stamp(&key, sequence);
        }
        for (key, receipt) in self.requests {
            let version =
                versions.map_or_else(super::next_version, |versions| versions.requests[&key]);
            state
                .requests
                .insert_versioned(key.clone(), Arc::new(receipt), version);
            state.requests.stamp(&key, sequence);
        }
        state.revision = self.revision;
    }

    fn receipt<'a>(&'a self, state: &'a Snapshot, id: &str) -> Option<&'a Receipt> {
        self.requests.get(id).or_else(|| {
            (!self.deleted_requests.contains(id))
                .then(|| state.requests.get(id))
                .flatten()
        })
    }
}

/// A checkpoint and the state it describes, held only while a transfer
/// reads it.
struct SnapshotImage {
    meta: SnapshotMeta<u64, BasicNode>,
    state: Arc<StoredState>,
}

struct SnapshotCapture {
    state: StoredState,
    installation: u64,
    accounting: super::snapshot_policy::Capture,
}

/// Immutable data and retry history move together only after atomic apply.
/// An already-linearized reader or pipelined writer can retain this view while
/// a later apply is committing, without waiting on its state lock.
#[derive(Clone)]
struct Published {
    partitions: Partitions,
    last_applied: Option<LogId<u64>>,
    revision: u64,
    data: Records,
    requests: Receipts,
}

impl From<&StoredState> for Published {
    fn from(state: &StoredState) -> Self {
        Self {
            partitions: state.partitions.clone(),
            last_applied: state.last_applied,
            revision: state.application.revision,
            data: state.application.data.clone(),
            requests: state.application.requests.clone(),
        }
    }
}

fn publish(lock: &PublishedLock<Published>, state: &StoredState) {
    let next = Published::from(state);
    let previous = std::mem::replace(&mut *lock.write().expect("published state lock"), next);
    // A snapshot installation can replace the entire tree. Free its old nodes
    // outside the publication lock rather than making new readers wait.
    drop(previous);
}

/// A log entry as stored: its JSON, packed. Snapshot byte triggers count the
/// JSON, which is what replaying it costs.
fn encode_log(profile: &mut StorageTrace, entry: &Entry<TypeConfig>) -> anyhow::Result<Vec<u8>> {
    let json = profile.encode(entry)?;
    Ok(match super::packed::pack(&json) {
        std::borrow::Cow::Owned(packed) => packed,
        std::borrow::Cow::Borrowed(_) => json,
    })
}

fn decode_log(bytes: &[u8]) -> anyhow::Result<Entry<TypeConfig>> {
    Ok(serde_json::from_slice(&super::packed::unpack(bytes)?)?)
}

/// Appends submitted but not yet committed, in log order.
type AppendingLogs = Arc<PublishedLock<Vec<Arc<Vec<Entry<TypeConfig>>>>>>;

/// One append readable before it commits. Removed once its batch commits
/// (or fails), including when a storage worker panics.
struct PendingAppend {
    logs: AppendingLogs,
    entries: Arc<Vec<Entry<TypeConfig>>>,
}

impl PendingAppend {
    fn publish(logs: &AppendingLogs, entries: Arc<Vec<Entry<TypeConfig>>>) -> Self {
        logs.write()
            .expect("appending log lock")
            .push(entries.clone());
        Self {
            logs: logs.clone(),
            entries,
        }
    }
}

impl Drop for PendingAppend {
    fn drop(&mut self) {
        let mut logs = self.logs.write().expect("appending log lock");
        if let Some(position) = logs
            .iter()
            .position(|entries| Arc::ptr_eq(entries, &self.entries))
        {
            drop(logs.remove(position));
        }
    }
}

/// This replica's writes queued in shared batches but not yet committed.
/// Batches commit in submission order, so queued writes need no further
/// ordering among themselves; a direct transaction waits until none remain.
#[derive(Default)]
struct PendingWrites {
    count: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

struct PendingWrite(Arc<PendingWrites>);

impl Drop for PendingWrite {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

impl PendingWrites {
    fn begin(self: &Arc<Self>) -> PendingWrite {
        self.count.fetch_add(1, Ordering::AcqRel);
        PendingWrite(self.clone())
    }

    async fn idle(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

struct Inner {
    id: u64,
    shared: Arc<SharedDatabase>,
    tables: Tables,
    snapshot_directory: PathBuf,
    io: Arc<Mutex<()>>,
    // Appends queued in shared batches. Their readable suffix lets
    // replication overlap the flush; only the completion callback
    // establishes durability for Raft.
    appending: AppendingLogs,
    pending: Arc<PendingWrites>,
    // An old asynchronous builder must never replace an installed snapshot,
    // including an installation with the same log position.
    snapshot_installation: AtomicU64,
    // The latest durable checkpoint, without its state: a retained state pins
    // a redb read transaction, and redb reuses no page freed after it began.
    // Between checkpoints that grew the file by every page each commit
    // rewrote. A transfer captures the current state instead.
    current_snapshot: PublishedLock<Option<SnapshotMeta<u64, BasicNode>>>,
    snapshot_accounting: Arc<super::snapshot_policy::Accounting>,
    // This lock orders apply, snapshot capture and install_snapshot. Readers only
    // see a new value after its complete redb projection has committed atomically.
    state: Arc<RwLock<StoredState>>,
    // No await, I/O or user code runs under this lock, only a root-pointer swap
    // or clone. Complete retry history shares the same published revision.
    published: Arc<PublishedLock<Published>>,
    // The current leader's own appends whose flush is deferred.
    lazy: Arc<lazy_flush::LazyFlush>,
    // Applied states published in memory but not yet written to redb.
    persistence: Arc<persistence::Persistence>,
    // How many written states the application's read snapshot includes.
    backing_persisted: Arc<AtomicU64>,
    // Rebases so far, which choose the settled partitions each one moves.
    rebase_rounds: Arc<AtomicU64>,
    // Partitions replaced wholesale, by the sequence number of that write.
    replaced_partitions: Arc<std::sync::Mutex<BTreeMap<String, u64>>>,
    // Background writers currently holding this storage.
    holders: Arc<persistence::Holders>,
    // What each publication wrote, for watches.
    changes: super::changes::Sender,
}

#[derive(Clone)]
pub(super) struct Store {
    inner: Arc<Inner>,
    // Only handles passed into Raft carry this lifetime. Ordinary Consensus and
    // HTTP router clones must not delay shutdown. The receiver closes when the
    // final Raft log reader/state-machine/snapshot worker releases storage and
    // the store has closed.
    raft_lifetime: Option<Arc<RaftLifetime>>,
}

/// Once Raft releases its last storage handle, close the store before
/// signaling drain, so a waiting shutdown finds every applied state written
/// and the leader's deferred appends durable.
struct RaftLifetime {
    inner: Arc<Inner>,
    drained: Option<watch::Sender<()>>,
}

impl Drop for RaftLifetime {
    fn drop(&mut self) {
        let drained = self.drained.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let store = Store {
            inner: self.inner.clone(),
            raft_lifetime: None,
        };
        runtime.spawn(async move {
            if let Err(error) = store.close().await {
                tracing::error!(target: "flower::storage", error = %format!("{error:#}"), "Raft storage closed without persisting applied state; it replays from the log");
            }
            drop(store);
            drop(drained);
        });
    }
}

// Field order matters: release the database reference before signaling drain,
// including during unwinding. Cancellation cannot stop a spawn_blocking task.
struct DatabaseWork {
    shared: Arc<SharedDatabase>,
    _raft_lifetime: Option<Arc<RaftLifetime>>,
}

impl DatabaseWork {
    fn run<T>(self, operation: impl FnOnce(&Database) -> T) -> T {
        operation(self.shared.database())
    }
}

impl Store {
    pub(super) async fn open(id: u64, storage: Storage) -> anyhow::Result<Self> {
        let (shared, tables, state, directory, snapshot_accounting, current_snapshot) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let (shared, prefix, directory) = match storage {
                Storage::Directory(directory) => (None, String::new(), directory),
                Storage::Shared { database, prefix, directory } => (Some(database), prefix, directory),
            };
            let tables = Tables::new(&prefix);
            let directory = if directory.is_absolute() {
                directory
            } else {
                std::env::current_dir()?.join(directory)
            };
            #[cfg(unix)]
            let directories_to_sync = {
                let mut paths = vec![directory.clone()];
                let mut path = directory.as_path();
                while !path.exists() {
                    let Some(parent) = path.parent() else { break };
                    paths.push(parent.to_owned());
                    path = parent;
                }
                paths
            };
            std::fs::create_dir_all(&directory).context("create Raft data directory")?;
            let shared = match shared {
                Some(shared) => shared,
                None => SharedDatabase::open(&directory.join("flower.redb"))?,
            };
            let db = shared.database();
            // Hosted replicas and a lone replica never mix in one file.
            let names: Vec<String> = db
                .begin_read()?
                .list_tables()?
                .map(|table| table.name().to_owned())
                .filter(|name| name != shared::FILE_META.name())
                .collect();
            if prefix.is_empty() && names.iter().any(|name| name.contains('/')) {
                bail!("{} holds the database of several hosted replicas; start it with --replica", directory.display());
            }
            if !prefix.is_empty() && names.iter().any(|name| !name.contains('/')) {
                bail!("{} holds a single replica's database; host replicas in a new data directory", directory.display());
            }
            let mut transaction = db.begin_write()?;
            transaction.set_durability(Durability::Immediate)?;
            {
                transaction.open_table(tables.logs)?;
                let mut meta = transaction.open_table(tables.meta)?;
                let prior_id = meta
                    .get("node_id")?
                    .map(|value| serde_json::from_slice::<u64>(value.value()))
                    .transpose()?;
                if let Some(prior_id) = prior_id {
                    if prior_id != id {
                        bail!("data directory belongs to node {prior_id}, not node {id}");
                    }
                } else {
                    meta.insert("node_id", serde_json::to_vec(&id)?.as_slice())?;
                }
                // A leader flushes its own appends lazily, so after a crash
                // its log can lack entries its followers hold. Resuming the
                // same term at startup would append different entries under
                // their log ids. Its own vote comes back uncommitted: it must
                // win a later term, which only an up-to-date log can.
                let vote = meta
                    .get("vote")?
                    .map(|value| serde_json::from_slice::<Vote<u64>>(value.value()))
                    .transpose()?;
                if let Some(vote) = vote
                    && vote.committed
                    && vote.leader_id.node_id == id
                {
                    let vote = Vote { committed: false, ..vote };
                    meta.insert("vote", serde_json::to_vec(&vote)?.as_slice())?;
                }
            }
            let mut state = load_application(&transaction, tables)?;
            transaction.open_table(tables.data)?;
            transaction.open_table(tables.requests)?;
            for table in [
                tables.partition_data,
                tables.partition_requests,
                tables.partition_base_data,
                tables.partition_base_requests,
                tables.partition_chunks,
            ] {
                transaction.open_table(table)?;
            }
            let checkpoint = recover_checkpoint(&transaction, tables, id, &state)?;
            transaction.commit()?;
            // Serve the committed tables instead of loading every record.
            // Its records were checked when applied; reading every receipt
            // again would make startup time grow with retained history.
            state.application = stored_application(db, tables, state.application.revision)?;
            partition_storage::load_served_partitions(db, tables, &mut state.partitions)?;
            let current_snapshot = checkpoint;
            let snapshot_index=read_snapshot_metadata(db, tables)?.and_then(|meta|meta.last_log_id.map(|id|id.index));
            let applied_index=state.last_applied.map(|id|id.index);
            let mut unsnapshotted_bytes=0u128;
            if let Some(applied)=applied_index && snapshot_index.is_none_or(|snapshot|snapshot<applied) {
                let transaction=db.begin_read()?;
                let logs=transaction.open_table(tables.logs)?;
                let start=snapshot_index.map_or(std::ops::Bound::Unbounded,std::ops::Bound::Excluded);
                for entry in logs.range((start,std::ops::Bound::Included(applied)))? {
                    unsnapshotted_bytes=unsnapshotted_bytes.saturating_add(super::packed::json_len(entry?.1.value()) as u128);
                }
            }
            let snapshot_accounting=super::snapshot_policy::Accounting::new(unsnapshotted_bytes,applied_index,snapshot_index);
            // A file's fsync does not persist newly created directory entries.
            // Flush the database's directory and each newly created ancestor.
            #[cfg(unix)]
            for path in directories_to_sync {
                std::fs::File::open(path)?.sync_all()?;
            }
            Ok((shared, tables, state, directory, snapshot_accounting, current_snapshot))
        })
        .await
        .context("open database worker")??;
        let lazy = Arc::new(lazy_flush::LazyFlush::default());
        lazy.update_membership(&state.membership);
        let store = Self {
            inner: Arc::new(Inner {
                id,
                shared,
                tables,
                snapshot_directory: directory,
                io: Arc::new(Mutex::new(())),
                appending: Arc::new(PublishedLock::new(Vec::new())),
                pending: Arc::default(),
                snapshot_installation: AtomicU64::new(0),
                current_snapshot: PublishedLock::new(current_snapshot),
                snapshot_accounting,
                published: Arc::new(PublishedLock::new(Published::from(&state))),
                state: Arc::new(RwLock::new(state)),
                lazy: lazy.clone(),
                persistence: Arc::default(),
                backing_persisted: Arc::default(),
                rebase_rounds: Arc::default(),
                replaced_partitions: Arc::default(),
                holders: Arc::default(),
                changes: super::changes::sender(),
            }),
            raft_lifetime: None,
        };
        lazy_flush::spawn(
            Arc::downgrade(&store.inner),
            store.inner.holders.clone(),
            lazy,
        );
        Ok(store)
    }

    /// After Raft has stopped: write every applied state, make the leader's
    /// held appends durable, and wait until no background task holds the
    /// database, so it can be reopened at once. Raft's storage lifetime runs
    /// this before signaling drain.
    pub(super) async fn close(&self) -> anyhow::Result<()> {
        self.inner.pending.idle().await;
        let persisted = self.inner.persistence.drain().await;
        lazy_flush::flush(self, &self.inner.lazy).await;
        self.inner.holders.idle().await;
        persisted
    }

    /// Leader appends that did not wait for their own flush.
    #[cfg(test)]
    pub(super) fn deferred_leader_appends(&self) -> u64 {
        self.inner.lazy.deferred()
    }

    /// Wait until every applied state so far has been written.
    #[cfg(test)]
    pub(super) async fn persisted(&self) -> anyhow::Result<()> {
        self.inner.persistence.drain().await
    }

    pub(super) fn raft_storage(&self) -> (Self, watch::Receiver<()>) {
        let (sender, drained) = watch::channel(());
        (
            Self {
                inner: self.inner.clone(),
                raft_lifetime: Some(Arc::new(RaftLifetime {
                    inner: self.inner.clone(),
                    drained: Some(sender),
                })),
            },
            drained,
        )
    }

    /// Batched commits of this replica's database, shared with any other
    /// replica hosted in it.
    pub(super) fn storage_metrics(&self) -> Value {
        self.inner.shared.metrics()
    }

    /// The writes of every publication from now on.
    pub(super) fn changes(&self) -> tokio::sync::broadcast::Receiver<Arc<super::changes::Changes>> {
        self.inner.changes.subscribe()
    }

    pub(super) fn snapshot_accounting(&self) -> Arc<super::snapshot_policy::Accounting> {
        self.inner.snapshot_accounting.clone()
    }

    fn database_work(&self) -> DatabaseWork {
        DatabaseWork {
            shared: self.inner.shared.clone(),
            _raft_lifetime: self.raft_lifetime.clone(),
        }
    }

    pub(super) async fn snapshot(&self) -> Snapshot {
        self.inner.state.read().await.application.clone()
    }

    /// Queries need no receipts; a projected mutation read retains only the
    /// requested receipt allocation from the same published data revision.
    pub(super) async fn snapshot_for(&self, request_id: Option<&str>) -> Snapshot {
        let state = self.inner.published.read().expect("published state lock");
        Snapshot {
            revision: state.revision,
            data: state.data.clone(),
            requests: request_id
                .and_then(|id| state.requests.get_shared(id).map(|value| (id, value)))
                .map(|(id, receipt)| (id.to_owned(), receipt.clone()))
                .into_iter()
                .collect(),
        }
    }

    /// The log position and application revision come from the same atomic
    /// publication as query data, including blank/membership/rejected entries.
    pub(super) fn read_fence(&self) -> anyhow::Result<super::read::ReadFence> {
        let state = self.inner.published.read().expect("published state lock");
        Ok(super::read::ReadFence {
            applied: state
                .last_applied
                .context("unavailable: no applied Raft state")?,
            revision: state.revision,
        })
    }

    /// Test the fence and retain its immutable data root under one short lock.
    /// A metrics read followed by an unrelated state read would not establish
    /// that the captured state belongs to the observed applied log position.
    pub(super) fn snapshot_after_fence(
        &self,
        fence: &super::read::ReadFence,
    ) -> anyhow::Result<Option<Snapshot>> {
        let state = self.inner.published.read().expect("published state lock");
        let Some(applied) = state.last_applied else {
            return Ok(None);
        };
        if applied.index < fence.applied.index {
            return Ok(None);
        }
        if (applied.index == fence.applied.index && applied != fence.applied)
            || applied.leader_id < fence.applied.leader_id
        {
            bail!("unavailable: local applied log does not match the read fence");
        }
        if state.revision < fence.revision
            || (applied == fence.applied && state.revision != fence.revision)
        {
            bail!("unavailable: local application revision does not match the read fence");
        }
        Ok(Some(Snapshot {
            revision: state.revision,
            data: state.data.clone(),
            requests: Receipts::default(),
        }))
    }

    pub(super) async fn snapshot_for_many(&self, request_ids: &[String]) -> Snapshot {
        let state = self.inner.published.read().expect("published state lock");
        Snapshot {
            revision: state.revision,
            data: state.data.clone(),
            requests: request_ids
                .iter()
                .filter_map(|id| state.requests.get_shared(id).map(|value| (id, value)))
                .map(|(id, receipt)| (id.clone(), receipt.clone()))
                .collect(),
        }
    }

    /// Full writer baseline: cloning both persistent roots is O(1), including
    /// arbitrarily old request IDs that can arrive in later pipeline groups.
    pub(super) async fn snapshot_for_writer(&self) -> Snapshot {
        let state = self.inner.published.read().expect("published state lock");
        Snapshot {
            revision: state.revision,
            data: state.data.clone(),
            requests: state.requests.clone(),
        }
    }

    pub(super) fn partition_infos(&self) -> Vec<PartitionInfo> {
        self.inner
            .published
            .read()
            .expect("published state lock")
            .partitions
            .iter()
            .map(|(_, state)| state.info.clone())
            .collect()
    }

    pub(super) fn partition_info(&self, id: &str) -> anyhow::Result<PartitionInfo> {
        self.inner
            .published
            .read()
            .expect("published state lock")
            .partitions
            .get(id)
            .map(|state| state.info.clone())
            .context("partition does not exist")
    }

    pub(super) fn partition_state(&self, id: &str) -> anyhow::Result<PartitionState> {
        self.inner
            .published
            .read()
            .expect("published state lock")
            .partitions
            .get(id)
            .cloned()
            .context("partition does not exist")
    }

    pub(super) fn partition_snapshot(
        &self,
        binding: &PartitionBinding,
        request_id: Option<&str>,
        all: bool,
    ) -> anyhow::Result<Snapshot> {
        let state = self.inner.published.read().expect("published state lock");
        let snapshot = partitions::active(state.partitions.get(&binding.partition), binding)?;
        let requests = if all {
            snapshot.requests.clone()
        } else {
            request_id
                .and_then(|id| {
                    snapshot
                        .requests
                        .get(id)
                        .map(|receipt| (id.to_owned(), receipt.clone()))
                })
                .into_iter()
                .collect()
        };
        Ok(Snapshot {
            revision: snapshot.revision,
            data: snapshot.data.clone(),
            requests,
        })
    }

    pub(super) fn snapshot_after_fence_scoped(
        &self,
        fence: &super::read::ReadFence,
        binding: Option<&PartitionBinding>,
    ) -> anyhow::Result<Option<Snapshot>> {
        let state = self.inner.published.read().expect("published state lock");
        let Some(applied) = state.last_applied else {
            return Ok(None);
        };
        if applied.index < fence.applied.index {
            return Ok(None);
        }
        anyhow::ensure!(
            !(applied.index == fence.applied.index && applied != fence.applied)
                && applied.leader_id >= fence.applied.leader_id
                && state.revision >= fence.revision
                && (applied != fence.applied || state.revision == fence.revision),
            "local publication does not match read fence"
        );
        match binding {
            Some(binding) => {
                let snapshot =
                    partitions::active(state.partitions.get(&binding.partition), binding)?;
                Ok(Some(Snapshot {
                    revision: snapshot.revision,
                    data: snapshot.data.clone(),
                    requests: Receipts::default(),
                }))
            }
            None => Ok(Some(Snapshot {
                revision: state.revision,
                data: state.data.clone(),
                requests: Receipts::default(),
            })),
        }
    }

    async fn disk_profiled<T, F>(
        &self,
        mut profile: StorageTrace,
        operation: F,
    ) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database, &mut StorageTrace) -> anyhow::Result<T> + Send + 'static,
    {
        // Move the serialization guard into the blocking task. Even cancellation
        // of an awaiting Raft future cannot let a later I/O overtake this one.
        let guard = self.inner.io.clone().lock_owned().await;
        // A direct transaction follows every write already queued in batches.
        self.inner.pending.idle().await;
        profile.phase(StoragePhase::IoQueue);
        let work = self.database_work();
        tokio::task::spawn_blocking(move || {
            work.run(|db| {
                let span = profile.span();
                let _entered = span.enter();
                profile.phase(StoragePhase::BlockingQueue);
                let result = operation(db, &mut profile);
                drop(guard);
                profile.report(result.is_ok());
                result
            })
        })
        .await
        .context("Raft storage worker")?
    }

    /// Stage a write into the shared database's next batch, in this replica's
    /// storage order. It is queued under the order guard, which is released
    /// as soon as it is queued: later writes queue behind it, and direct
    /// transactions wait until it has committed.
    async fn batched(
        &self,
        durable: bool,
        mut profile: StorageTrace,
        write: impl FnMut(&WriteTransaction, &mut StorageTrace) -> anyhow::Result<()> + Send + 'static,
    ) -> anyhow::Result<()> {
        let guard = self.inner.io.clone().lock_owned().await;
        profile.phase(StoragePhase::IoQueue);
        let pending = self.inner.pending.begin();
        let submitted =
            self.inner
                .shared
                .submit(durable, profile, self.raft_lifetime.clone(), write);
        drop(guard);
        // Count it as pending until it commits, even if this caller is cancelled.
        tokio::spawn(async move {
            let result = submitted.committed().await;
            drop(pending);
            result
        })
        .await
        .context("Raft storage batch")?
    }

    async fn read_disk<T, F>(&self, operation: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> anyhow::Result<T> + Send + 'static,
    {
        // redb read transactions see one committed MVCC revision even while a
        // writer is staging or flushing another one. Do not make replication
        // log reads queue behind application commits. Writes retain disk_profiled()'s
        // ordering/cancellation guard. Log readers also merge the single
        // in-flight append, whose callback still waits for the durable flush.
        let work = self.database_work();
        tokio::task::spawn_blocking(move || work.run(operation))
            .await
            .context("Raft storage reader")?
    }

    async fn start_append(
        &self,
        entries: Vec<Entry<TypeConfig>>,
        durability: Durability,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let deferred = matches!(durability, Durability::None);
        let mut profile = StorageTrace::new(
            self.inner.id,
            if deferred { "leader_append" } else { "append" },
        );
        profile.entries(&entries);
        profile.phase(StoragePhase::Prepare);
        // Acquire before publishing or submitting: appends, votes, truncation,
        // purge and application retain their storage order on cancellation.
        let guard = self.inner.io.clone().lock_owned().await;
        profile.phase(StoragePhase::IoQueue);
        let entries = Arc::new(entries);
        let appended = PendingAppend::publish(&self.inner.appending, entries.clone());
        let pending = self.inner.pending.begin();
        let shared = self.inner.shared.clone();
        let tables = self.inner.tables;
        let lifetime = self.raft_lifetime.clone();
        // The append joins the next batch of every replica sharing this
        // database; a durable append shares that batch's one flush. Encode
        // first: batches stage one at a time, so staging only copies bytes.
        // Once queued, the next append may follow at once; it cannot commit
        // before this one.
        tokio::spawn(async move {
            let encoded = tokio::task::spawn_blocking(move || {
                let encoded = entries
                    .iter()
                    .map(|entry| Ok((entry.log_id.index, encode_log(&mut profile, entry)?)))
                    .collect::<anyhow::Result<Vec<_>>>();
                (encoded, profile)
            })
            .await;
            let submitted = match encoded {
                Ok((Ok(encoded), profile)) => {
                    Ok(
                        shared.submit(!deferred, profile, lifetime, move |transaction, _| {
                            let mut table = transaction.open_table(tables.logs)?;
                            for (index, entry) in &encoded {
                                table.insert(*index, entry.as_slice())?;
                            }
                            Ok(())
                        }),
                    )
                }
                Ok((Err(error), profile)) => {
                    profile.report(false);
                    Err(error)
                }
                Err(error) => Err(anyhow::Error::new(error).context("Raft append encoder")),
            };
            drop(guard);
            let result = match submitted {
                Ok(submitted) => submitted.committed().await,
                Err(error) => Err(error),
            };
            // Log readers pair the pending list with an MVCC snapshot under
            // its lock: an append leaves the list only once committed.
            drop(appended);
            drop(pending);
            result
        })
    }
}

fn load_application(transaction: &WriteTransaction, tables: Tables) -> anyhow::Result<StoredState> {
    let meta = transaction.open_table(tables.meta)?;
    let metadata = meta
        .get(STATE_META)?
        .map(|value| serde_json::from_slice::<StateMetadata>(value.value()))
        .transpose()?;
    let Some(metadata) = metadata else {
        // Older storage formats kept their state under other keys.
        for older in [PREVIOUS_STATE_META, LEGACY_STATE_META, "state"] {
            anyhow::ensure!(
                meta.get(older)?.is_none(),
                "the data directory uses an older storage format; start from an empty one"
            );
        }
        drop(meta);
        let state = StoredState::default();
        replace_application(transaction, tables, &state)?;
        check_index_layout(transaction, tables, true)?;
        return Ok(state);
    };
    drop(meta);
    // open_table() creates missing tables; reject incomplete storage instead
    // of recovering missing durable records as an empty database.
    let existing = transaction
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    anyhow::ensure!(
        existing.contains(tables.data.name()) && existing.contains(tables.requests.name()),
        "incomplete application storage tables"
    );
    check_index_layout(transaction, tables, false)?;
    // The caller serves the tables once this transaction commits. Servers
    // before 8ae042e could store versions above the bound they stored with
    // them (up to a block's worth per apply, more for partitions under a busy
    // disk); starting well above it keeps their data safe on the first
    // restart too, and costs nothing: versions are only compared.
    super::versions_after(metadata.versions.saturating_add(RESTART_MARGIN));
    Ok(StoredState {
        last_applied: metadata.last_applied,
        membership: metadata.membership,
        application: Snapshot {
            revision: metadata.revision,
            ..Snapshot::default()
        },
        partitions: load_partitions(transaction, tables)?,
    })
}

/// Whole-table replacement is reserved for new databases and snapshot installation.
/// Their metadata, data and receipts share the caller's one durable transaction.
fn replace_application(
    transaction: &WriteTransaction,
    tables: Tables,
    state: &StoredState,
) -> anyhow::Result<()> {
    replace_application_profiled(transaction, tables, state, &mut StorageTrace::default())
}

fn replace_application_profiled(
    transaction: &WriteTransaction,
    tables: Tables,
    state: &StoredState,
    profile: &mut StorageTrace,
) -> anyhow::Result<()> {
    super::retention::validate_snapshot(&state.application)?;
    transaction.delete_table(tables.data)?;
    transaction.delete_table(tables.requests)?;
    replace_partitions(transaction, tables, &state.partitions, profile)?;
    {
        let mut data = transaction.open_table(tables.data)?;
        for record in state.application.data.encoded() {
            data.insert(record.key().as_bytes(), record.bytes())?;
        }
        let mut requests = transaction.open_table(tables.requests)?;
        for record in state.application.requests.encoded() {
            requests.insert(record.key().as_bytes(), record.bytes())?;
        }
        let mut meta = transaction.open_table(tables.meta)?;
        meta.insert(
            STATE_META,
            profile.encode(&StateMetadata::from(state))?.as_slice(),
        )?;
        for key in [PREVIOUS_STATE_META, LEGACY_STATE_META, "state"] {
            meta.insert(key, RETIRED_STATE)?;
        }
    }
    Ok(())
}

fn read_meta<T: DeserializeOwned>(
    db: &Database,
    tables: Tables,
    key: &str,
) -> anyhow::Result<Option<T>> {
    let transaction = db.begin_read()?;
    let table = transaction.open_table(tables.meta)?;
    let value = table
        .get(key)?
        .map(|value| serde_json::from_slice(value.value()))
        .transpose()?;
    Ok(value)
}

fn io_error(subject: ErrorSubject<u64>, verb: ErrorVerb, error: impl Display) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, std::io::Error::other(error.to_string()))
}

/// Refuse a database whose index entries use an older layout, which this
/// binary would misread; mark a new one.
fn check_index_layout(
    transaction: &WriteTransaction,
    tables: Tables,
    new: bool,
) -> anyhow::Result<()> {
    let mut meta = transaction.open_table(tables.meta)?;
    if meta.get(INDEX_LAYOUT_META)?.is_none() {
        anyhow::ensure!(
            new,
            "the data directory stores index entries in an older layout; start from an empty one"
        );
        meta.insert(INDEX_LAYOUT_META, b"2".as_slice())?;
    }
    Ok(())
}

/// Pure deterministic application logic. Client receipts are checked before
/// revision; internal maintenance always checks revision and stores no receipt.
fn apply_commit(state: &Snapshot, delta: &mut ApplicationDelta, commit: Commit) -> ApplyResult {
    let mut retention = match prepare_retention(state, delta, &commit.puts, &commit.deletes) {
        Ok(retention) => retention,
        Err(error) => return ApplyResult::Rejected(error.to_string()),
    };
    if !commit.internal
        && let Err(error) = admit_request(state, delta, retention.as_ref(), &commit.request_id)
    {
        return ApplyResult::Rejected(error.to_string());
    }
    if !commit.internal
        && let Some(receipt) = delta.receipt(state, &commit.request_id)
    {
        return if receipt.fingerprint == commit.fingerprint {
            ApplyResult::Committed(CommitResult {
                revision: receipt.revision,
                duplicate: true,
                result: receipt.result.clone(),
            })
        } else {
            ApplyResult::Rejected(
                "conflict: request ID was already used for different content".into(),
            )
        };
    }
    if commit.expected_revision != delta.revision {
        return ApplyResult::Rejected(format!(
            "conflict: expected revision {}, current revision {}",
            commit.expected_revision, delta.revision
        ));
    }
    let Some(revision) = delta.revision.checked_add(1) else {
        return ApplyResult::Rejected("conflict: application revision exhausted".into());
    };
    let receipt = (!commit.internal).then(|| Receipt {
        fingerprint: commit.fingerprint,
        revision,
        result: commit.result.clone(),
        epoch: retention
            .as_ref()
            .and_then(|policy| super::retention::receipt_epoch(policy, &commit.request_id)),
    });
    let reserved = match reserved_after(state, delta, &commit.puts, &commit.deletes) {
        Ok(bytes) => bytes,
        Err(error) => return ApplyResult::Rejected(error.to_string()),
    };
    if let Some(receipt) = &receipt
        && let Some(policy) = &mut retention
        && let Err(error) = super::retention::receipt_bytes(&commit.request_id, receipt)
            .and_then(|bytes| policy.charge(bytes, 1, reserved))
    {
        return ApplyResult::Rejected(error.to_string());
    }
    for key in commit.deletes {
        delta.data.insert(key, None);
    }
    delta.data.extend(
        commit
            .puts
            .into_iter()
            .map(|(key, value)| (key, Some(value))),
    );
    delta.revision = revision;
    if let Some(receipt) = receipt {
        delta.requests.insert(commit.request_id, receipt);
        publish_retention(delta, retention);
    }
    ApplyResult::Committed(CommitResult {
        revision,
        duplicate: false,
        result: commit.result,
    })
}

/// Check every precondition before touching the accumulated delta. A rejected
/// batch must preserve both the base snapshot and preceding entries in this
/// same storage transaction. There are no partially applied batch outcomes.
fn apply_batch(state: &Snapshot, delta: &mut ApplicationDelta, batch: CompactBatch) -> ApplyResult {
    let reject = |reason: String| {
        if batch.items.is_empty() {
            ApplyResult::Rejected(reason)
        } else {
            ApplyResult::Batch(
                batch
                    .items
                    .iter()
                    .map(|_| ApplyResult::Rejected(reason.clone()))
                    .collect(),
            )
        }
    };
    let revision = match batch.validate() {
        Ok(revision) => revision,
        Err(error) => return reject(error.to_string()),
    };
    let mut retention = match prepare_retention(state, delta, &batch.puts, &batch.deletes) {
        Ok(retention) => retention,
        Err(error) => return reject(error.to_string()),
    };
    let mut duplicates = Vec::with_capacity(batch.items.len());
    for item in &batch.items {
        if item.internal {
            continue;
        }
        if let Err(error) = admit_request(state, delta, retention.as_ref(), &item.request_id) {
            return reject(error.to_string());
        }
        if let Some(receipt) = delta.receipt(state, &item.request_id) {
            if receipt.fingerprint != item.fingerprint {
                return reject(
                    "conflict: request ID was already used for different content".into(),
                );
            }
            duplicates.push(ApplyResult::Committed(CommitResult {
                revision: receipt.revision,
                duplicate: true,
                result: receipt.result.clone(),
            }));
        }
    }
    if duplicates.len() == batch.items.len() {
        return ApplyResult::Batch(duplicates);
    }
    if !duplicates.is_empty() {
        return reject(
            "conflict: atomic batch overlaps committed requests; rebase against fresh receipts"
                .into(),
        );
    }
    if batch.expected_revision != delta.revision {
        return reject(format!(
            "conflict: expected revision {}, current revision {}",
            batch.expected_revision, delta.revision
        ));
    }
    if let Some(policy) = &mut retention {
        let reserved = match reserved_after(state, delta, &batch.puts, &batch.deletes) {
            Ok(bytes) => bytes,
            Err(error) => return reject(error.to_string()),
        };
        for (index, item) in batch.items.iter().enumerate() {
            if item.internal {
                continue;
            }
            let receipt = Receipt {
                fingerprint: item.fingerprint.clone(),
                revision: batch.expected_revision + index as u64 + 1,
                result: item.result.clone(),
                epoch: super::retention::receipt_epoch(policy, &item.request_id),
            };
            if let Err(error) = super::retention::receipt_bytes(&item.request_id, &receipt)
                .and_then(|bytes| policy.charge(bytes, 1, reserved))
            {
                return reject(error.to_string());
            }
        }
    }
    for key in batch.deletes {
        delta.data.insert(key, None);
    }
    delta.data.extend(
        batch
            .puts
            .into_iter()
            .map(|(key, value)| (key, Some(value))),
    );
    let results = batch
        .items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            let revision = batch.expected_revision + index as u64 + 1;
            if !item.internal {
                let epoch = retention
                    .as_ref()
                    .and_then(|policy| super::retention::receipt_epoch(policy, &item.request_id));
                delta.requests.insert(
                    item.request_id,
                    Receipt {
                        fingerprint: item.fingerprint,
                        revision,
                        result: item.result.clone(),
                        epoch,
                    },
                );
            }
            ApplyResult::Committed(CommitResult {
                revision,
                duplicate: false,
                result: item.result,
            })
        })
        .collect();
    delta.revision = revision;
    publish_retention(delta, retention);
    ApplyResult::Batch(results)
}

impl RaftLogReader<TypeConfig> for Store {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let tables = self.inner.tables;
        // Own the bounds before moving the work to a blocking thread.
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        let appending = self.inner.appending.clone();
        self.read_disk(move |db| {
            let buffered = appending.read().expect("appending log lock");
            let transaction = db.begin_read()?;
            let pending = buffered.clone();
            drop(buffered);
            let table = transaction.open_table(tables.logs)?;
            if pending.is_empty() {
                // Preserve the allocation-light disk-only path for catch-up
                // and recovery when no append is in progress.
                return table
                    .range((start, end))?
                    .map(|item| {
                        let (_, bytes) = item?;
                        decode_log(bytes.value())
                    })
                    .collect();
            }
            // Later appends win: a truncation drains the list, so pending
            // appends never overlap, but order would decide if they did.
            let mut result: BTreeMap<_, _> = pending
                .iter()
                .flat_map(|entries| entries.iter())
                .filter(|entry| (start, end).contains(&entry.log_id.index))
                .map(|entry| (entry.log_id.index, entry.clone()))
                .collect();
            for item in table.range((start, end))? {
                let (index, bytes) = item?;
                // The buffer may overlap an already committed MVCC version.
                // Reuse its decoded entries instead of parsing them again.
                if let std::collections::btree_map::Entry::Vacant(slot) =
                    result.entry(index.value())
                {
                    slot.insert(decode_log(bytes.value())?);
                }
            }
            Ok(result.into_values().collect())
        })
        .await
        .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }
}

impl RaftLogStorage<TypeConfig> for Store {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let tables = self.inner.tables;
        let appending = self.inner.appending.clone();
        self.read_disk(move |db| {
            let buffered = appending.read().expect("appending log lock");
            let transaction = db.begin_read()?;
            let pending = buffered.clone();
            drop(buffered);
            let meta = transaction.open_table(tables.meta)?;
            let logs = transaction.open_table(tables.logs)?;
            let last_purged = meta
                .get("last_purged")?
                .map(|bytes| serde_json::from_slice::<LogId<u64>>(bytes.value()))
                .transpose()?;
            let last = logs
                .iter()?
                .next_back()
                .transpose()?
                .map(|(_, bytes)| decode_log(bytes.value()))
                .transpose()?;
            let buffered = pending
                .iter()
                .filter_map(|entries| entries.last())
                .map(|entry| entry.log_id)
                .max_by_key(|log| log.index);
            Ok(LogState {
                last_purged_log_id: last_purged,
                last_log_id: last
                    .map(|entry| entry.log_id)
                    .into_iter()
                    .chain(buffered)
                    .max_by_key(|log| log.index)
                    .or(last_purged),
            })
        })
        .await
        .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let tables = self.inner.tables;
        let vote = *vote;
        self.disk_profiled(
            StorageTrace::new(self.inner.id, "vote"),
            move |db, profile| {
                let bytes = profile.encode(&vote)?;
                profile.phase(StoragePhase::Prepare);
                let mut transaction = db.begin_write()?;
                transaction.set_durability(Durability::Immediate)?;
                profile.phase(StoragePhase::Begin);
                {
                    let mut table = transaction.open_table(tables.meta)?;
                    table.insert("vote", bytes.as_slice())?;
                }
                profile.phase(StoragePhase::Write);
                let result = transaction.commit();
                profile.phase(StoragePhase::Flush);
                result?;
                Ok(())
            },
        )
        .await
        .map_err(|error| io_error(ErrorSubject::Vote, ErrorVerb::Write, error))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let tables = self.inner.tables;
        self.read_disk(move |db| read_meta(db, tables, "vote"))
            .await
            .map_err(|error| io_error(ErrorSubject::Vote, ErrorVerb::Read, error))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        // Only the leader holds deferred appends; see lazy_flush.
        self.inner.lazy.committed(committed);
        // Committed entries are already durable in the quorum's Raft logs.
        // apply() materializes them without a second flush; once its queued
        // write commits, the next Immediate transaction persists the entire
        // redb state, including last_applied.
        // Startup floors committed to that atomic durable state and Raft
        // re-establishes any newer committed prefix. Consensus gates local
        // application reads until a fresh recovery fence when logs are ahead.
        // https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftLogStorage.html#method.save_committed
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let tables = self.inner.tables;
        // Retain compatibility with databases written before save_committed
        // became optional. OpenRaft replays a legacy marker ahead of the applied
        // state, or raises an older marker to the durable applied position.
        self.read_disk(move |db| {
            Ok(read_meta::<Option<LogId<u64>>>(db, tables, "committed")?.flatten())
        })
        .await
        .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if self.inner.lazy.applies(&callback)
            && let Some(last_index) = entries.last().map(|entry| entry.log_id.index)
        {
            // The leader's own append: visible now, durable at a later flush.
            let worker = self.start_append(entries, Durability::None).await;
            let lazy = self.inner.lazy.clone();
            tokio::spawn(async move {
                match worker
                    .await
                    .context("Raft append worker")
                    .and_then(|result| result)
                {
                    Ok(()) => lazy.hold(last_index, callback),
                    Err(error) => {
                        callback.log_io_completed(Err(std::io::Error::other(error.to_string())))
                    }
                }
            });
            return Ok(());
        }
        let worker = self.start_append(entries, Durability::Immediate).await;
        // Replication can start once entries are readable. Raft's local durable
        // position advances only on this callback after Immediate commit. Keep
        // observing the worker on caller cancellation; panic/storage errors
        // must reach Raft's fatal I/O path.
        tokio::spawn(async move {
            let result = worker
                .await
                .context("Raft append worker")
                .and_then(|result| result);
            callback
                .log_io_completed(result.map_err(|error| std::io::Error::other(error.to_string())));
        });
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let tables = self.inner.tables;
        self.disk_profiled(
            StorageTrace::new(self.inner.id, "truncate"),
            move |db, profile| {
                let mut transaction = db.begin_write()?;
                transaction.set_durability(Durability::Immediate)?;
                profile.phase(StoragePhase::Begin);
                {
                    let mut table = transaction.open_table(tables.logs)?;
                    let keys = table
                        .range(log_id.index..)?
                        .map(|item| item.map(|(key, _)| key.value()))
                        .collect::<Result<Vec<_>, _>>()?;
                    for key in keys {
                        table.remove(key)?;
                    }
                }
                profile.phase(StoragePhase::Write);
                let result = transaction.commit();
                profile.phase(StoragePhase::Flush);
                result?;
                Ok(())
            },
        )
        .await
        .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let tables = self.inner.tables;
        // Keep the log until the state it would replay is written. This purge's
        // Immediate commit then persists that state too.
        self.inner
            .persistence
            .drain()
            .await
            .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
        self.disk_profiled(
            StorageTrace::new(self.inner.id, "purge"),
            move |db, profile| {
                let mut transaction = db.begin_write()?;
                transaction.set_durability(Durability::Immediate)?;
                profile.phase(StoragePhase::Begin);
                {
                    let mut table = transaction.open_table(tables.logs)?;
                    let keys = table
                        .range(..=log_id.index)?
                        .map(|item| item.map(|(key, _)| key.value()))
                        .collect::<Result<Vec<_>, _>>()?;
                    for key in keys {
                        table.remove(key)?;
                    }
                    let mut meta = transaction.open_table(tables.meta)?;
                    meta.insert("last_purged", profile.encode(&log_id)?.as_slice())?;
                }
                profile.phase(StoragePhase::Write);
                let result = transaction.commit();
                profile.phase(StoragePhase::Flush);
                result?;
                Ok(())
            },
        )
        .await
        .map_err(|error| io_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }
}

impl RaftStateMachine<TypeConfig> for Store {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state = self.inner.state.read().await;
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let tables = self.inner.tables;
        let entries: Vec<_> = entries.into_iter().collect();
        let mut profile = StorageTrace::new(self.inner.id, "apply");
        profile.entries(&entries);
        profile.phase(StoragePhase::Prepare);
        self.inner
            .persistence
            .check()
            .map_err(|error| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        let mut guard = self.inner.state.clone().write_owned().await;
        profile.phase(StoragePhase::StateLock);
        let published = self.inner.published.clone();
        let lazy = self.inner.lazy.clone();
        let work = self.database_work();
        // Apply, publish and queue the redb projection without taking the I/O
        // guard: persistence follows in order, off Raft's critical path. The
        // blocking task owns the state guard, so dropping this future cannot
        // leave a later apply to observe a half-published state.
        let snapshot_accounting = self.inner.snapshot_accounting.clone();
        let persistence = self.inner.persistence.clone();
        let backing_persisted = self.inner.backing_persisted.clone();
        let rebase_rounds = self.inner.rebase_rounds.clone();
        let replaced_partitions = self.inner.replaced_partitions.clone();
        let weak = Arc::downgrade(&self.inner);
        let holders = self.inner.holders.clone();
        let changes = self.inner.changes.clone();
        let responses = tokio::task::spawn_blocking(move || work.run(|db| -> anyhow::Result<_> {
            let span = profile.span();
            let _entered = span.enter();
            profile.phase(StoragePhase::BlockingQueue);
            // Serve what persistence has written from disk: the in-memory
            // tree then holds only states still queued behind it. This only
            // saves memory; a failure to read leaves the state to the next
            // apply, while the persistence task reports storage errors.
            let persisted = persistence.persisted_count();
            if persisted != backing_persisted.load(Ordering::Acquire)
                && let Ok((data, requests)) = stored_backings(db, tables)
            {
                guard.application.data.rebase(data, persisted);
                guard.application.requests.rebase(requests, persisted);
                // Rebase partitions too, so none pins an old snapshot and
                // keeps redb from reusing the pages written since.
                let mut replaced = replaced_partitions.lock().expect("replaced partitions lock");
                replaced.retain(|_, sequence| *sequence > persisted);
                let unwritten = |id: &str| replaced.contains_key(id);
                let round = rebase_rounds.fetch_add(1, Ordering::Relaxed);
                if partition_storage::serve_partitions(db, tables, &mut guard.partitions, &unwritten, persisted, Some(round)).is_ok() {
                    backing_persisted.store(persisted, Ordering::Release);
                }
            }
            // Submissions happen only under this guard, so this apply's write
            // is the next one.
            let sequence = persistence.submitted_count() + 1;
            let applied_bytes={
                let transaction=db.begin_read()?;
                let logs=transaction.open_table(tables.logs)?;
                let mut bytes=0u128;
                for entry in &entries {
                    // Normal Raft application reads the already encoded durable
                    // record length. The fallback supports direct storage calls.
                    let size=match logs.get(entry.log_id.index)? {Some(value)=>super::packed::json_len(value.value()),None=>super::limits::encoded_json_len(entry)?};
                    bytes=bytes.saturating_add(size as u128);
                }
                bytes
            };
            let mut metadata = StateMetadata::from(&*guard);
            let mut delta = ApplicationDelta::new(guard.application.revision);
            let mut responses = Vec::with_capacity(entries.len());
            let mut partition_writes = BTreeMap::new();
            for entry in entries {
                metadata.last_applied = Some(entry.log_id);
                responses.push(match entry.payload {
                    EntryPayload::Blank => ApplyResult::Internal,
                    EntryPayload::Normal(RaftCommand::RecoveryBarrier { .. }) => ApplyResult::Internal,
                    EntryPayload::Membership(membership) => {
                        metadata.membership = StoredMembership::new(Some(entry.log_id), membership);
                        ApplyResult::Internal
                    }
                    EntryPayload::Normal(RaftCommand::Scoped { partition, epoch, command }) => {
                        apply_scoped(&guard.partitions, &mut partition_writes, PartitionBinding { partition, epoch }, *command, entry.log_id.leader_id)
                    }
                    EntryPayload::Normal(RaftCommand::PartitionControl { partition_control, leader_id }) => {
                        if leader_id.is_some_and(|leader| leader != entry.log_id.leader_id) {
                            ApplyResult::Rejected("partition control was authorized under another leader".into())
                        } else {
                            apply_partition_control(&guard.partitions, &mut partition_writes, partition_control)
                        }
                    }
                    EntryPayload::Normal(RaftCommand::Single(commit)) => {
                        apply_commit(&guard.application, &mut delta, commit)
                    }
                    EntryPayload::Normal(RaftCommand::Retention { retention }) => {
                        apply_retention(&guard.application, &mut delta, retention)
                    }
                    EntryPayload::Normal(RaftCommand::Fenced { leader_id, commit }) => {
                        if entry.log_id.leader_id == leader_id {
                            apply_commit(&guard.application, &mut delta, commit)
                        } else {
                            ApplyResult::Rejected(format!(
                                "conflict: preparation was authorized for leader {leader_id}, but proposed by {}",
                                entry.log_id.leader_id
                            ))
                        }
                    }
                    EntryPayload::Normal(RaftCommand::Batch { batch }) => {
                        apply_batch(&guard.application, &mut delta, batch)
                    }
                });
            }
            metadata.revision = delta.revision;
            profile.application(
                guard.application.revision,
                delta.revision,
                delta.data.len(),
                delta.requests.len(),
            );
            profile.phase(StoragePhase::Prepare);
            // Log append has already made every applied entry quorum-durable.
            // This atomic projection may roll back on process/power loss; the
            // next Immediate log/vote/snapshot/purge commit also persists it.
            // Snapshot checkpoints, purges and installations drain the queue
            // first, so a log is never discarded before the state it would
            // replay.
            let mut versions = DeltaVersions::default();
            let write = persistence::StateWrite {
                data: delta
                    .data
                    .iter()
                    .map(|(key, value)| {
                        let Some(value) = value else {
                            return Ok((key.clone(), None));
                        };
                        let version = super::next_version();
                        versions.data.insert(key.clone(), version);
                        Ok((key.clone(), Some(super::backing::encode(version, &profile.encode(value)?))))
                    })
                    .collect::<anyhow::Result<_>>()?,
                requests: delta
                    .requests
                    .iter()
                    .map(|(key, value)| {
                        let version = super::next_version();
                        versions.requests.insert(key.clone(), version);
                        Ok((key.clone(), super::backing::encode(version, &profile.encode(value)?)))
                    })
                    .collect::<anyhow::Result<_>>()?,
                deleted_requests: delta.deleted_requests.iter().cloned().collect(),
                metadata: metadata.clone(),
                partitions: partition_writes.clone(),
            };
            profile.phase(StoragePhase::Write);
            {
                let mut replaced = replaced_partitions.lock().expect("replaced partitions lock");
                for (id, write) in &partition_writes {
                    if write.replaces() {
                        replaced.insert(id.clone(), sequence);
                    }
                }
            }
            let mut written = Vec::new();
            if changes.receiver_count() > 0 {
                if !delta.data.is_empty() {
                    written.push(super::changes::Changes {
                        scope: super::changes::Scope::Root,
                        revision: delta.revision,
                        keys: Some(delta.data.keys().cloned().collect()),
                    });
                }
                for (id, write) in &partition_writes {
                    written.push(write.changes(id));
                }
            }
            for (_, mut write) in partition_writes {
                write.stamp(sequence);
                guard.partitions.insert(write.state);
            }
            // No fallible operation follows. Move the small overlay into the
            // locked cache before readers or later applies run.
            delta.publish(&mut guard.application, Some(&versions), sequence);
            guard.last_applied = metadata.last_applied;
            guard.membership = metadata.membership;
            lazy.update_membership(&guard.membership);
            snapshot_accounting.applied(applied_bytes,guard.last_applied.map(|id|id.index));
            publish(&published, &guard);
            // Under the guard, so readers learn of writes in publication order.
            for written in written {
                let _ = changes.send(Arc::new(written));
            }
            // Queue under the guard: whoever next observes this state, even
            // after this future is cancelled, also finds its write submitted.
            persistence.submit(weak, holders, write);
            profile.phase(StoragePhase::Publish);
            profile.report(true);
            Ok(responses)
        }))
        .await
        .context("Raft apply worker")
        .and_then(|result| result)
        .map_err(|error| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotData>, StorageError<u64>> {
        let directory = self.inner.snapshot_directory.clone();
        let file = self
            .read_disk(move |_| temporary(&directory))
            .await
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        Ok(Box::new(SnapshotData::from_std(file)))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        let tables = self.inner.tables;
        let mut profile = StorageTrace::new(self.inner.id, "install_snapshot");
        let mut file = (*snapshot).into_std().await.map_err(|error| {
            io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                error,
            )
        })?;
        profile.phase(StoragePhase::Prepare);
        let lifetime = self.raft_lifetime.clone();
        let directory = self.inner.snapshot_directory.clone();
        let (next, mut profile) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let _lifetime = lifetime;
            let span = profile.span();
            let _entered = span.enter();
            profile.phase(StoragePhase::BlockingQueue);
            let result = decode_state(&mut file, &directory);
            profile.phase(StoragePhase::Prepare);
            match result {
                Ok(state) => Ok((state, profile)),
                Err(error) => {
                    profile.report(false);
                    Err(error)
                }
            }
        })
        .await
        .map_err(|error| {
            io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                error,
            )
        })?
        .map_err(|error| {
            io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                error,
            )
        })?;
        let snapshot_meta = meta.clone();
        if next.last_applied != meta.last_log_id || next.membership != meta.last_membership {
            profile.report(false);
            return Err(io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                "snapshot metadata does not match its state",
            ));
        }
        if let Some(timing) = &mut profile.0 {
            timing.first_log_index = next.last_applied.map(|id| id.index);
            timing.last_log_index = timing.first_log_index;
        }
        profile.phase(StoragePhase::Prepare);
        let mut guard = self.inner.state.clone().write_owned().await;
        // No apply can queue another state while this guard is held. An older
        // queued state must not land after the installed one.
        self.inner.persistence.drain().await.map_err(|error| {
            io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                error,
            )
        })?;
        profile.phase(StoragePhase::StateLock);
        profile.application(
            guard.application.revision,
            next.application.revision,
            next.application.data.len(),
            next.application.requests.len(),
        );
        let published = self.inner.published.clone();
        let inner = self.inner.clone();
        let next_installation = inner
            .snapshot_installation
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| {
                io_error(
                    ErrorSubject::Snapshot(Some(meta.signature())),
                    ErrorVerb::Write,
                    "snapshot installation generation exhausted",
                )
            })?;
        self.disk_profiled(profile, move |db, profile| {
            profile.phase(StoragePhase::Prepare);
            let mut transaction = db.begin_write()?;
            transaction.set_durability(Durability::Immediate)?;
            profile.phase(StoragePhase::Begin);
            replace_application_profiled(&transaction, tables, &next, profile)?;
            write_checkpoint(&transaction, tables, &snapshot_meta, profile)?;
            profile.phase(StoragePhase::Write);
            let result = transaction.commit();
            profile.phase(StoragePhase::Flush);
            result?;
            // Serve the installed tables rather than the decoded state.
            let mut next = next;
            next.application = stored_application(db, tables, next.application.revision)?;
            partition_storage::serve_partitions(
                db,
                tables,
                &mut next.partitions,
                &|_| false,
                super::backing::UNSEQUENCED,
                None,
            )?;
            inner
                .replaced_partitions
                .lock()
                .expect("replaced partitions lock")
                .clear();
            inner
                .backing_persisted
                .store(inner.persistence.persisted_count(), Ordering::Release);
            *guard = next;
            inner.lazy.update_membership(&guard.membership);
            publish(&published, &guard);
            let _ = inner.changes.send(Arc::new(super::changes::Changes {
                scope: super::changes::Scope::All,
                revision: 0,
                keys: None,
            }));
            *inner.current_snapshot.write().expect("snapshot image lock") = Some(snapshot_meta);
            inner
                .snapshot_installation
                .store(next_installation, Ordering::Release);
            inner
                .snapshot_accounting
                .installed(guard.last_applied.map(|id| id.index));
            profile.phase(StoragePhase::Publish);
            Ok(())
        })
        .await
        .map_err(|error| {
            io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                error,
            )
        })?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<RaftSnapshot<TypeConfig>>, StorageError<u64>> {
        self.current_transfer()
            .await
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))
    }
}

impl Store {
    async fn capture_snapshot(&self) -> SnapshotCapture {
        let guard = self.inner.state.read().await;
        SnapshotCapture {
            state: guard.clone(),
            installation: self.inner.snapshot_installation.load(Ordering::Acquire),
            accounting: self.inner.snapshot_accounting.capture(),
        }
    }

    /// The current checkpoint with the state it describes. Once applies have
    /// moved past it, the current state is checkpointed and sent instead: a
    /// newer snapshot also covers every purged entry.
    async fn current_transfer(&self) -> anyhow::Result<Option<RaftSnapshot<TypeConfig>>> {
        loop {
            let capture = self.capture_snapshot().await;
            let meta = self
                .inner
                .current_snapshot
                .read()
                .expect("snapshot image lock")
                .clone();
            let Some(meta) = meta else {
                return Ok(None);
            };
            // One log ID names one state: the checkpoint describes this one.
            if meta.last_log_id == capture.state.last_applied {
                return Ok(Some(self.transfer_snapshot(SnapshotImage {
                    meta,
                    state: Arc::new(capture.state),
                })));
            }
            let profile = StorageTrace::new(self.inner.id, "build_snapshot");
            if let Some(image) = self.checkpoint_captured(capture, profile).await? {
                return Ok(Some(self.transfer_snapshot(image)));
            }
            // A newer checkpoint or an installation replaced the capture.
        }
    }

    fn transfer_snapshot(&self, image: SnapshotImage) -> RaftSnapshot<TypeConfig> {
        RaftSnapshot {
            meta: image.meta,
            snapshot: Box::new(SnapshotData::deferred(DeferredImage {
                state: image.state,
                directory: self.inner.snapshot_directory.clone(),
                node: self.inner.id,
            })),
        }
    }

    async fn build_captured_snapshot(
        &self,
        capture: SnapshotCapture,
        profile: StorageTrace,
    ) -> anyhow::Result<RaftSnapshot<TypeConfig>> {
        match self.checkpoint_captured(capture, profile).await? {
            Some(image) => Ok(self.transfer_snapshot(image)),
            None => self
                .current_transfer()
                .await?
                .context("newer snapshot disappeared after serialized publication"),
        }
    }

    /// Checkpoint a captured state, unless a newer checkpoint or an
    /// installation has replaced it.
    async fn checkpoint_captured(
        &self,
        capture: SnapshotCapture,
        mut profile: StorageTrace,
    ) -> anyhow::Result<Option<SnapshotImage>> {
        let tables = self.inner.tables;
        // The checkpoint lets Raft prune the logs up to the captured state, so
        // that state must be written first. Its Immediate commit persists it.
        self.inner.persistence.drain().await?;
        let activity = self.inner.snapshot_accounting.begin();
        profile.phase(StoragePhase::StateLock);
        if let Some(timing) = &mut profile.0 {
            timing.first_log_index = capture.state.last_applied.map(|id| id.index);
            timing.last_log_index = timing.first_log_index;
        }
        profile.application(
            capture.state.application.revision,
            capture.state.application.revision,
            capture.state.application.data.len(),
            capture.state.application.requests.len(),
        );
        let inner = self.inner.clone();
        let image = self
            .disk_profiled(profile, move |db, profile| {
                let _activity = activity;
                let last_applied = capture.state.last_applied;
                let replaced =
                    inner.snapshot_installation.load(Ordering::Acquire) != capture.installation;
                let newer = read_snapshot_metadata(db, tables)?.is_some_and(|meta| {
                    meta.last_log_id.map(|id| id.index) > last_applied.map(|id| id.index)
                });
                if replaced || newer {
                    return Ok(None);
                }
                let mut transaction = db.begin_write()?;
                transaction.set_durability(Durability::Immediate)?;
                profile.phase(StoragePhase::Begin);
                let meta = allocate_snapshot_meta(&transaction, tables, inner.id, &capture.state)?;
                write_checkpoint(&transaction, tables, &meta, profile)?;
                profile.phase(StoragePhase::Write);
                // This flush makes the existing application tables durable before
                // Raft can prune their source log entries. No full image is written.
                let result = transaction.commit();
                profile.phase(StoragePhase::Flush);
                result?;
                *inner.current_snapshot.write().expect("snapshot image lock") = Some(meta.clone());
                inner
                    .snapshot_accounting
                    .published(capture.accounting, last_applied.map(|id| id.index));
                Ok(Some(SnapshotImage {
                    meta,
                    state: Arc::new(capture.state),
                }))
            })
            .await
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
        Ok(image)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Store {
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot<TypeConfig>, StorageError<u64>> {
        let profile = StorageTrace::new(self.inner.id, "build_snapshot");
        let captured = self.capture_snapshot().await;
        self.build_captured_snapshot(captured, profile)
            .await
            .map_err(|error| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }
}

#[cfg(test)]
mod tests;
