//! What backups (`consensus::backup`) need of a replica's storage: when each
//! entry was applied, the log entries Raft has purged but the backup has not
//! shipped yet (the hold), the stored bytes of entries, a captured state to
//! write as a base, and, for a restore, installing a base, replaying entries
//! and leaving the state to a new single-node cluster.
//!
//! The hold keeps purged entries in the log table, below the purge point
//! Raft is told of, until the leader's backup has shipped them (`release`),
//! so that Raft's aggressive log compaction never outruns shipping, and a
//! leader that restarts can find the entry its generation ended with. A
//! replica that ships nothing (a follower) keeps the newest entries up to
//! the hold's limit, so it can continue the generation if it becomes
//! leader. Past the limit the oldest held entries go: the next leader to
//! ship finds them missing and starts a new generation.
use std::collections::VecDeque;

use super::*;

// Apply times kept at most: one per apply call.
const CLOCK_RECORDS: usize = 1 << 20;

#[derive(Default)]
pub(in crate::consensus) struct Hooks {
    enabled: std::sync::atomic::AtomicBool,
    // (last index of an apply, when it was applied), in log order.
    clock: std::sync::Mutex<VecDeque<(u64, u64)>>,
    hold: std::sync::Mutex<Hold>,
    // The last index of the latest snapshot installed: entries at or below
    // it may be another history's, which the backup must never ship.
    installed: AtomicU64,
}

#[derive(Default)]
struct Hold {
    max_bytes: u64,
    // Entries at or below this may go.
    released: Option<u64>,
    // Purged entries kept, with their stored sizes.
    held: VecDeque<(u64, u64)>,
    bytes: u64,
    // Entries let go before they were released, because the hold was full.
    dropped: u64,
}

impl Hooks {
    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub(super) fn applied(&self, last_index: u64) {
        if !self.enabled() {
            return;
        }
        let at = crate::consensus::backup::now_ms();
        let mut clock = self.clock.lock().expect("backup clock lock");
        match clock.back_mut() {
            Some(back) if back.1 == at => back.0 = last_index,
            _ => clock.push_back((last_index, at)),
        }
        if clock.len() > CLOCK_RECORDS {
            clock.pop_front();
        }
    }

    pub(super) fn installed(&self, index: Option<u64>) {
        if let Some(index) = index {
            self.installed.fetch_max(index, Ordering::AcqRel);
            // What the log holds at or below it can be let go.
            if self.enabled() {
                self.release(index);
            }
        }
    }

    fn release(&self, index: u64) {
        let mut hold = self.hold.lock().expect("backup hold lock");
        hold.released = Some(hold.released.map_or(index, |released| released.max(index)));
        while let Some(&(front, bytes)) = hold.held.front()
            && front <= index
        {
            hold.held.pop_front();
            hold.bytes -= bytes;
        }
    }

    /// Purge the log up to `index` as Raft asks, keeping what the hold keeps.
    pub(super) fn purge(
        &self,
        table: &mut redb::Table<'_, u64, &'static [u8]>,
        index: u64,
    ) -> anyhow::Result<()> {
        let remove =
            |table: &mut redb::Table<'_, u64, &'static [u8]>, upto: u64| -> anyhow::Result<()> {
                let keys = table
                    .range(..=upto)?
                    .map(|item| item.map(|(key, _)| key.value()))
                    .collect::<Result<Vec<_>, _>>()?;
                for key in keys {
                    table.remove(key)?;
                }
                Ok(())
            };
        if !self.enabled() {
            return remove(table, index);
        }
        let mut hold = self.hold.lock().expect("backup hold lock");
        if let Some(released) = hold.released {
            remove(table, released.min(index))?;
        }
        let from = hold
            .held
            .back()
            .map(|(held, _)| held + 1)
            .into_iter()
            .chain(hold.released.map(|released| released + 1))
            .max()
            .unwrap_or(0);
        if from <= index {
            for item in table.range(from..=index)? {
                let (key, value) = item?;
                let bytes = value.value().len() as u64;
                hold.held.push_back((key.value(), bytes));
                hold.bytes += bytes;
            }
        }
        let mut dropped = 0;
        while hold.bytes > hold.max_bytes
            && let Some((front, bytes)) = hold.held.pop_front()
        {
            hold.bytes -= bytes;
            hold.released = Some(front);
            dropped += 1;
        }
        if dropped > 0 {
            hold.dropped += dropped;
            let released = hold.released.expect("set above");
            drop(hold);
            remove(table, released)?;
            tracing::warn!(target: "flower::backup", dropped, through = released,
                "the backup hold is full: purged log entries the backup had not shipped; the next \
                 leader to ship starts a new generation unless it still holds them");
        }
        Ok(())
    }
}

/// Stored entries read for shipping.
pub(in crate::consensus) enum Shippable {
    /// Consecutive entries from the one asked for (none if it is not
    /// written yet), as (index, stored bytes).
    Entries(Vec<(u64, Vec<u8>)>),
    /// The first entry asked for is gone: purged and not held, or below an
    /// installed snapshot.
    Lost,
}

/// A log entry's identity, as backups compare it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::consensus) struct EntryMark {
    pub log_id: LogId<u64>,
    /// The SHA-256 of its JSON.
    pub sha256: String,
}

impl EntryMark {
    pub(in crate::consensus) fn of(stored: &[u8]) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Head {
            log_id: LogId<u64>,
        }
        let json = super::super::packed::unpack(stored)?;
        let head: Head = serde_json::from_slice(&json).context("decode a log entry's ID")?;
        Ok(Self {
            log_id: head.log_id,
            sha256: crate::consensus::backup::sha256_hex(&json),
        })
    }
}

/// How much the hold keeps.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::consensus) struct HoldStatus {
    pub released: Option<u64>,
    pub entries: usize,
    pub bytes: u64,
    pub max_bytes: u64,
    pub dropped: u64,
}

/// A state captured for a base.
pub(in crate::consensus) struct BackupImage {
    state: Arc<StoredState>,
}

impl BackupImage {
    pub(in crate::consensus) fn last_applied(&self) -> Option<LogId<u64>> {
        self.state.last_applied
    }

    pub(in crate::consensus) fn membership(&self) -> StoredMembership<u64, BasicNode> {
        self.state.membership.clone()
    }

    /// Write the state as a snapshot transfer encodes it.
    pub(in crate::consensus) fn encode(&self, writer: impl std::io::Write) -> anyhow::Result<()> {
        serde_json::to_writer(writer, &*self.state).context("encode the backup base")
    }
}

impl Store {
    /// Record apply times and hold purged entries up to `max_bytes`. Call
    /// before Raft starts. Entries purged but kept from an earlier run are
    /// held again.
    pub(in crate::consensus) async fn enable_backup(&self, max_bytes: u64) -> anyhow::Result<()> {
        let tables = self.inner.tables;
        let held = self
            .read_disk(move |db| {
                let Some(purged) = read_meta::<LogId<u64>>(db, tables, "last_purged")? else {
                    return Ok(Vec::new());
                };
                let transaction = db.begin_read()?;
                let logs = transaction.open_table(tables.logs)?;
                logs.range(..=purged.index)?
                    .map(|item| {
                        let (key, value) = item?;
                        Ok((key.value(), value.value().len() as u64))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .await?;
        let hooks = &self.inner.backup;
        let mut hold = hooks.hold.lock().expect("backup hold lock");
        hold.max_bytes = max_bytes;
        hold.bytes = held.iter().map(|(_, bytes)| bytes).sum();
        hold.held = held.into();
        drop(hold);
        hooks.enabled.store(true, Ordering::Release);
        Ok(())
    }

    /// Let the hold go of entries at or below `index`: the backup has them.
    pub(in crate::consensus) fn release_backup_hold(&self, index: u64) {
        self.inner.backup.release(index);
    }

    pub(in crate::consensus) fn backup_hold(&self) -> HoldStatus {
        let hold = self.inner.backup.hold.lock().expect("backup hold lock");
        HoldStatus {
            released: hold.released,
            entries: hold.held.len(),
            bytes: hold.bytes,
            max_bytes: hold.max_bytes,
            dropped: hold.dropped,
        }
    }

    /// When the entry at `index` was applied here, if this process applied it.
    pub(in crate::consensus) fn applied_at(&self, index: u64) -> Option<u64> {
        let clock = self.inner.backup.clock.lock().expect("backup clock lock");
        let position = clock.partition_point(|(last, _)| *last < index);
        clock.get(position).map(|(_, at)| *at)
    }

    /// Forget apply times of entries at or below `index`.
    pub(in crate::consensus) fn forget_applied_at(&self, index: u64) {
        let mut clock = self.inner.backup.clock.lock().expect("backup clock lock");
        while clock.front().is_some_and(|(last, _)| *last <= index) {
            clock.pop_front();
        }
    }

    /// Stored entries from `first` through `last`, consecutive, stopping
    /// once they hold `max_bytes`.
    pub(in crate::consensus) async fn shippable(
        &self,
        first: u64,
        last: u64,
        max_bytes: usize,
    ) -> anyhow::Result<Shippable> {
        if first <= self.inner.backup.installed.load(Ordering::Acquire) {
            return Ok(Shippable::Lost);
        }
        let tables = self.inner.tables;
        self.read_disk(move |db| {
            let transaction = db.begin_read()?;
            let logs = transaction.open_table(tables.logs)?;
            let mut entries = Vec::new();
            let mut bytes = 0;
            for item in logs.range(first..=last)? {
                let (key, value) = item?;
                if key.value() != first + entries.len() as u64 || bytes >= max_bytes {
                    break;
                }
                bytes += value.value().len();
                entries.push((key.value(), value.value().to_vec()));
            }
            if entries.is_empty() {
                let meta = transaction.open_table(tables.meta)?;
                let purged = meta
                    .get("last_purged")?
                    .map(|bytes| serde_json::from_slice::<LogId<u64>>(bytes.value()))
                    .transpose()?;
                if purged.is_some_and(|purged| first <= purged.index) {
                    return Ok(Shippable::Lost);
                }
            }
            Ok(Shippable::Entries(entries))
        })
        .await
    }

    /// The identity of the stored entry at `index`, if the log holds it and
    /// no installed snapshot covers it.
    pub(in crate::consensus) async fn entry_mark(
        &self,
        index: u64,
    ) -> anyhow::Result<Option<EntryMark>> {
        if index <= self.inner.backup.installed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let tables = self.inner.tables;
        self.read_disk(move |db| {
            let transaction = db.begin_read()?;
            let logs = transaction.open_table(tables.logs)?;
            logs.get(index)?
                .map(|value| EntryMark::of(value.value()))
                .transpose()
        })
        .await
    }

    /// The current state, to write as a base.
    pub(in crate::consensus) async fn backup_image(&self) -> BackupImage {
        BackupImage {
            state: Arc::new(self.capture_snapshot().await.state),
        }
    }

    /// Restore: replace this (new) replica's state with a base's.
    pub(in crate::consensus) async fn restore_base(
        &mut self,
        last_log_id: Option<LogId<u64>>,
        membership: StoredMembership<u64, BasicNode>,
        image: std::fs::File,
    ) -> anyhow::Result<()> {
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id: format!(
                "restore-{}",
                last_log_id.map_or_else(|| "empty".into(), |id| id.to_string())
            ),
        };
        self.install_snapshot(&meta, Box::new(SnapshotData::from_std(image)))
            .await
            .map_err(|error| anyhow::anyhow!("install the backup base: {error}"))
    }

    /// Restore: apply entries as Raft would.
    pub(in crate::consensus) async fn restore_apply(
        &mut self,
        entries: Vec<Entry<TypeConfig>>,
    ) -> anyhow::Result<()> {
        RaftStateMachine::apply(self, entries)
            .await
            .map_err(|error| anyhow::anyhow!("apply backed-up entries: {error}"))?;
        Ok(())
    }

    /// The last applied entry and the state's revision.
    pub(in crate::consensus) async fn restored(&self) -> (Option<LogId<u64>>, u64) {
        let state = self.inner.state.read().await;
        (state.last_applied, state.application.revision)
    }

    /// Restore: make the restored state that of a new cluster whose only
    /// voter is node `id` at `address`. Raft's log starts after the last
    /// applied entry, and the vote at `term`, above every term the backed-up
    /// history used, so no entry of the new cluster shares a log ID with the
    /// old one's. `origin` is recorded for the backups the node ships.
    pub(in crate::consensus) async fn restore_finish(
        &self,
        id: u64,
        address: String,
        term: u64,
        origin: Value,
    ) -> anyhow::Result<LogId<u64>> {
        self.close().await?;
        let (last_applied, _) = self.restored().await;
        let last_applied = last_applied.context("the restored state applied no entry")?;
        let membership = StoredMembership::new(
            Some(last_applied),
            openraft::Membership::new(
                vec![BTreeSet::from([id])],
                BTreeMap::from([(id, BasicNode::new(address))]),
            ),
        );
        let tables = self.inner.tables;
        let stored = membership.clone();
        self.disk_profiled(StorageTrace::new(id, "restore"), move |db, _| {
            let mut transaction = db.begin_write()?;
            transaction.set_durability(Durability::Immediate)?;
            {
                let mut meta = transaction.open_table(tables.meta)?;
                let mut metadata: StateMetadata = serde_json::from_slice(
                    meta.get(STATE_META)?
                        .context("restored state without metadata")?
                        .value(),
                )?;
                anyhow::ensure!(
                    metadata.last_applied == Some(last_applied),
                    "restored state metadata is behind the applied state"
                );
                metadata.membership = stored.clone();
                meta.insert(STATE_META, serde_json::to_vec(&metadata)?.as_slice())?;
                let checkpoint = SnapshotMeta::<u64, BasicNode> {
                    last_log_id: Some(last_applied),
                    last_membership: stored,
                    snapshot_id: format!("{id}-restored-{last_applied}"),
                };
                meta.insert(CHECKPOINT_KEY, serde_json::to_vec(&checkpoint)?.as_slice())?;
                meta.insert("last_purged", serde_json::to_vec(&last_applied)?.as_slice())?;
                meta.insert("vote", serde_json::to_vec(&Vote::new(term, id))?.as_slice())?;
                meta.remove("committed")?;
                meta.insert(RESTORED_META, serde_json::to_vec(&origin)?.as_slice())?;
            }
            {
                let mut logs = transaction.open_table(tables.logs)?;
                let keys = logs
                    .iter()?
                    .map(|item| item.map(|(key, _)| key.value()))
                    .collect::<Result<Vec<_>, _>>()?;
                for key in keys {
                    logs.remove(key)?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
        .await?;
        Ok(last_applied)
    }

    /// Where this replica's state was restored from, if it was and no
    /// generation has said so yet.
    pub(in crate::consensus) async fn restored_from(&self) -> anyhow::Result<Option<Value>> {
        let tables = self.inner.tables;
        self.read_disk(move |db| read_meta(db, tables, RESTORED_META))
            .await
    }

    /// A generation has said where the state was restored from.
    pub(in crate::consensus) async fn forget_restored_from(&self) -> anyhow::Result<()> {
        let tables = self.inner.tables;
        self.disk_profiled(StorageTrace::new(self.inner.id, "restore"), move |db, _| {
            let mut transaction = db.begin_write()?;
            transaction.set_durability(Durability::Immediate)?;
            transaction.open_table(tables.meta)?.remove(RESTORED_META)?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }
}

const RESTORED_META: &str = "backup_restored_v1";

/// A log entry from its stored bytes.
pub(in crate::consensus) fn decode_stored_entry(bytes: &[u8]) -> anyhow::Result<Entry<TypeConfig>> {
    decode_log(bytes)
}

/// Whether a database already holds tables of the replica with this prefix
/// (every table when the prefix is empty).
pub(in crate::consensus) fn holds_replica(
    database: &SharedDatabase,
    prefix: &str,
) -> anyhow::Result<bool> {
    let transaction = database.database().begin_read()?;
    for table in transaction.list_tables()? {
        let name = table.name();
        if name != shared::FILE_META.name() && name.starts_with(prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use openraft::storage::RaftLogStorageExt;

    fn entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(2, 1), index),
            payload: EntryPayload::Normal(
                Commit {
                    internal: false,
                    request_id: format!("request-{index}"),
                    fingerprint: format!("fingerprint-{index}"),
                    expected_revision: index - 1,
                    puts: BTreeMap::from([(format!("key-{index}"), serde_json::json!(index))]),
                    deletes: Vec::new(),
                    result: Value::Null,
                }
                .into(),
            ),
        }
    }

    async fn stored(store: &mut Store, entries: std::ops::RangeInclusive<u64>) {
        let entries: Vec<_> = entries.map(entry).collect();
        store.blocking_append(entries.clone()).await.unwrap();
        store.apply(entries).await.unwrap();
    }

    fn indexes(shippable: Shippable) -> Option<Vec<u64>> {
        match shippable {
            Shippable::Entries(entries) => {
                Some(entries.into_iter().map(|(index, _)| index).collect())
            }
            Shippable::Lost => None,
        }
    }

    fn log_id(index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(2, 1), index)
    }

    #[tokio::test]
    async fn purged_entries_stay_for_backups_until_released_and_within_the_hold() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        store.enable_backup(1 << 30).await.unwrap();
        stored(&mut store, 1..=10).await;
        assert!(store.applied_at(1).is_some() && store.applied_at(10).is_some());
        assert_eq!(store.applied_at(11), None);
        store.purge(log_id(6)).await.unwrap();
        // Raft sees the purge; the backup still reads every entry.
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(6)));
        assert_eq!(state.last_log_id, Some(log_id(10)));
        assert_eq!(
            indexes(store.shippable(1, 10, usize::MAX).await.unwrap()),
            Some((1..=10).collect())
        );
        // Segments stop at their size limit, after at least one entry.
        assert_eq!(
            indexes(store.shippable(2, 10, 1).await.unwrap()),
            Some(vec![2])
        );
        let mark = store.entry_mark(3).await.unwrap().unwrap();
        assert_eq!(mark.log_id, log_id(3));
        assert_eq!(store.backup_hold().entries, 6);

        // Released entries go at the next purge.
        store.release_backup_hold(4);
        assert_eq!(store.backup_hold().entries, 2);
        store.purge(log_id(8)).await.unwrap();
        assert_eq!(
            indexes(store.shippable(1, 10, usize::MAX).await.unwrap()),
            None
        );
        assert_eq!(
            indexes(store.shippable(4, 10, usize::MAX).await.unwrap()),
            None
        );
        assert_eq!(
            indexes(store.shippable(5, 10, usize::MAX).await.unwrap()),
            Some((5..=10).collect())
        );
        assert_eq!(
            indexes(store.shippable(11, 12, usize::MAX).await.unwrap()),
            Some(vec![])
        );
        // With every entry purged, the log ends at the purge point, above
        // the entries still held.
        store.purge(log_id(10)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(10)));
        assert_eq!(state.last_purged_log_id, Some(log_id(10)));
        assert!(store.try_get_log_entries(11..).await.unwrap().is_empty());
        let hold = store.backup_hold();
        assert_eq!((hold.entries, hold.released, hold.dropped), (6, Some(4), 0));
        store.forget_applied_at(9);
        assert!(store.applied_at(9).is_none() || store.applied_at(9) == store.applied_at(10));

        // A restart holds what the log still has below the purge point.
        store.close().await.unwrap();
        drop(store);
        let store = Store::open(1, directory.path().into()).await.unwrap();
        store.enable_backup(1 << 30).await.unwrap();
        assert_eq!(store.backup_hold().entries, 6);
        assert_eq!(
            indexes(store.shippable(5, 10, usize::MAX).await.unwrap()),
            Some((5..=10).collect())
        );
        // Entries at or below an installed snapshot are never shipped.
        store.inner.backup.installed(Some(7));
        assert_eq!(
            indexes(store.shippable(7, 10, usize::MAX).await.unwrap()),
            None
        );
        assert!(store.entry_mark(7).await.unwrap().is_none());
        assert_eq!(
            indexes(store.shippable(8, 10, usize::MAX).await.unwrap()),
            Some(vec![8, 9, 10])
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_full_hold_lets_the_oldest_entries_go_and_no_hold_purges_at_once() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        let size = encode_log(&mut StorageTrace::default(), &entry(5))
            .unwrap()
            .len() as u64;
        store.enable_backup(3 * size + size / 2).await.unwrap();
        stored(&mut store, 1..=9).await;
        store.purge(log_id(9)).await.unwrap();
        let hold = store.backup_hold();
        assert_eq!((hold.entries, hold.dropped, hold.released), (3, 6, Some(6)));
        assert_eq!(
            indexes(store.shippable(6, 9, usize::MAX).await.unwrap()),
            None
        );
        assert_eq!(
            indexes(store.shippable(7, 9, usize::MAX).await.unwrap()),
            Some(vec![7, 8, 9])
        );
        store.close().await.unwrap();

        let plain = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, plain.path().into()).await.unwrap();
        stored(&mut store, 1..=4).await;
        assert_eq!(store.applied_at(1), None);
        store.purge(log_id(3)).await.unwrap();
        assert_eq!(
            indexes(store.shippable(1, 4, usize::MAX).await.unwrap()),
            None
        );
        assert_eq!(
            indexes(store.shippable(4, 4, usize::MAX).await.unwrap()),
            Some(vec![4])
        );
        store.close().await.unwrap();
    }
}
