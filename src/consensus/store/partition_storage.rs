use super::*;
use crate::consensus::{PartitionCommand, PartitionPhase};
use std::collections::BTreeSet;
use crate::consensus::backing::{UNSEQUENCED, encode};
use std::ops::Bound;

#[derive(Serialize, Deserialize)]
struct Metadata {
    #[serde(flatten)]
    info: PartitionInfo,
    #[serde(default)]
    base_revision: Option<u64>,
    #[serde(default)]
    last_import: Option<partitions::copy::ImportManifest>,
}
impl From<&PartitionState> for Metadata {
    fn from(state: &PartitionState) -> Self {
        Self {
            info: state.info.clone(),
            base_revision: state.base.as_ref().map(|base| base.revision),
            last_import: state.last_import.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct PartitionWrite {
    pub(super) state: PartitionState,
    replace: bool,
    replace_base: bool,
    data: BTreeSet<String>,
    requests: BTreeSet<String>,
    chunks: BTreeSet<String>,
    replace_chunks: bool,
}

impl PartitionWrite {
    /// Assign this write's records to persistence batch `sequence`. Records
    /// not yet served from disk keep deletions from now on, as served ones do.
    pub(super) fn stamp(&mut self, sequence: u64) {
        let state = &mut self.state;
        state.snapshot.data.keep_deletions();
        state.snapshot.requests.keep_deletions();
        if let Some(base) = &mut state.base {
            base.data.keep_deletions();
            base.requests.keep_deletions();
        }
        state.chunks.keep_deletions();
        for key in &self.data {
            state.snapshot.data.stamp(key, sequence);
        }
        for key in &self.requests {
            state.snapshot.requests.stamp(key, sequence);
        }
        for key in &self.chunks {
            state.chunks.stamp(key, sequence);
        }
    }

    /// Whether this write replaces the partition's stored records, copy base
    /// or chunks wholesale rather than changing some of them.
    pub(super) fn replaces(&self) -> bool {
        self.replace || self.replace_base || self.replace_chunks
    }

    /// What this write changed in partition `id`, for watches. A write of
    /// no records changed its binding or bookkeeping, which any read may see.
    pub(super) fn changes(&self, id: &str) -> crate::consensus::changes::Changes {
        crate::consensus::changes::Changes {
            scope: crate::consensus::changes::Scope::Partition(id.to_owned()),
            revision: self.state.snapshot.revision,
            keys: (!self.replace && !self.replace_base && !self.data.is_empty())
                .then(|| self.data.iter().cloned().collect()),
        }
    }
}

pub(super) fn load_partitions(
    transaction: &WriteTransaction,
    tables: Tables,
) -> anyhow::Result<Partitions> {
    let existing = transaction
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    if existing.contains(tables.partition_meta.name()) {
        anyhow::ensure!(
            existing.contains(tables.partition_data.name())
                && existing.contains(tables.partition_requests.name()),
            "incomplete partition storage tables"
        );
    }
    let mut states = BTreeMap::new();
    for item in transaction.open_table(tables.partition_meta)?.iter()? {
        let (id, bytes) = item?;
        let metadata: Metadata = serde_json::from_slice(bytes.value())?;
        let info = metadata.info;
        if metadata.base_revision.is_some() {
            anyhow::ensure!(
                existing.contains(tables.partition_base_data.name())
                    && existing.contains(tables.partition_base_requests.name()),
                "incomplete partition copy base storage tables"
            );
        }
        anyhow::ensure!(
            id.value() == info.partition,
            "partition metadata key mismatch"
        );
        let revision = if info.phase == PartitionPhase::Importing {
            if metadata
                .last_import
                .as_ref()
                .is_some_and(|manifest| manifest.kind == partitions::ExportKind::Delta)
            {
                anyhow::ensure!(
                    existing.contains(tables.partition_chunks.name()),
                    "missing partition difference staging table"
                );
                metadata
                    .last_import
                    .as_ref()
                    .and_then(|manifest| manifest.base_revision)
                    .context("difference import missing base revision")?
            } else {
                0
            }
        } else {
            info.revision
        };
        states.insert(
            info.partition.clone(),
            PartitionState {
                info,
                snapshot: Snapshot {
                    revision,
                    ..Snapshot::default()
                },
                base: metadata.base_revision.map(|revision| Snapshot {
                    revision,
                    ..Snapshot::default()
                }),
                last_import: metadata.last_import,
                chunks: Records::default(),
            },
        );
    }
    // The caller serves each partition's records once this transaction
    // commits, and validates them then.
    let mut partitions = Partitions::default();
    for (_, state) in states {
        partitions.insert(state);
    }
    Ok(partitions)
}

/// Serve every partition's records, receipts, copy base and difference chunks
/// from one read snapshot of their tables, instead of from memory.
///
/// A settled partition, with nothing in memory beyond its snapshot, reads
/// the same from any later one; moving it only releases the older snapshot.
/// Rebase round `round` moves only the partitions with records in memory,
/// and the settled ones at every `SETTLED_ROUNDS`th position, so that an
/// apply costs time in what changed rather than in every partition, and no
/// snapshot stays pinned for more than that many rounds.
const SETTLED_ROUNDS: u64 = 64;

/// A partition is served from a snapshot only if its in-memory state derives
/// from that snapshot's: `unwritten` names those replaced wholesale by a write
/// the snapshot may not include yet, whose old rows would otherwise reappear.
/// Without a `round`, every other partition moves.
pub(super) fn serve_partitions(
    db: &Database,
    tables: Tables,
    partitions: &mut Partitions,
    unwritten: &dyn Fn(&str) -> bool,
    persisted: u64,
    round: Option<u64>,
) -> anyhow::Result<()> {
    let transaction = db.begin_read()?;
    let open = |definition: PairTable| -> anyhow::Result<Arc<crate::consensus::backing::PairTable>> {
        Ok(Arc::new(transaction.open_table(definition)?))
    };
    let (data, requests, base_data, base_requests, chunks) = (
        open(tables.partition_data)?,
        open(tables.partition_requests)?,
        open(tables.partition_base_data)?,
        open(tables.partition_base_requests)?,
        open(tables.partition_chunks)?,
    );
    let backing = |table: &Arc<crate::consensus::backing::PairTable>, id: &str| {
        Arc::new(crate::consensus::backing::Backing::pair(table.clone(), id.to_owned()))
    };
    let select = |position: usize, state: &PartitionState| {
        !unwritten(&state.info.partition)
            && round.is_none_or(|round| {
                !settled(state) || (position as u64).wrapping_add(round) % SETTLED_ROUNDS == 0
            })
    };
    partitions.update_some(select, |state| {
        let id = state.info.partition.clone();
        state.snapshot.data.rebase(backing(&data, &id), persisted);
        state.snapshot.requests.rebase(backing(&requests, &id), persisted);
        if let Some(base) = &mut state.base {
            base.data.rebase(backing(&base_data, &id), persisted);
            base.requests.rebase(backing(&base_requests, &id), persisted);
        }
        state.chunks.rebase(backing(&chunks, &id), persisted);
        Ok(())
    })
}

fn settled(state: &PartitionState) -> bool {
    state.snapshot.data.is_settled()
        && state.snapshot.requests.is_settled()
        && state
            .base
            .as_ref()
            .is_none_or(|base| base.data.is_settled() && base.requests.is_settled())
        && state.chunks.is_settled()
}

/// Serve freshly loaded partitions, then check their metadata.
pub(super) fn load_served_partitions(
    db: &Database,
    tables: Tables,
    partitions: &mut Partitions,
) -> anyhow::Result<()> {
    serve_partitions(db, tables, partitions, &|_| false, UNSEQUENCED, None)?;
    for (_, state) in partitions.iter() {
        partitions::validate_stored_state(state)?;
    }
    Ok(())
}

pub(super) fn replace_partitions(
    transaction: &WriteTransaction,
    tables: Tables,
    partitions: &Partitions,
    profile: &mut StorageTrace,
) -> anyhow::Result<()> {
    transaction.delete_table(tables.partition_meta)?;
    transaction.delete_table(tables.partition_data)?;
    transaction.delete_table(tables.partition_requests)?;
    transaction.delete_table(tables.partition_base_data)?;
    transaction.delete_table(tables.partition_base_requests)?;
    transaction.delete_table(tables.partition_chunks)?;
    let mut metadata = transaction.open_table(tables.partition_meta)?;
    let mut data = transaction.open_table(tables.partition_data)?;
    let mut requests = transaction.open_table(tables.partition_requests)?;
    transaction.open_table(tables.partition_base_data)?;
    transaction.open_table(tables.partition_base_requests)?;
    transaction.open_table(tables.partition_chunks)?;
    for (id, state) in partitions.iter() {
        // Decoding the received state checked its records already.
        partitions::validate_stored_state(state)?;
        metadata.insert(
            id.as_str(),
            profile.encode(&Metadata::from(state))?.as_slice(),
        )?;
        for record in state.snapshot.data.encoded() {
            data.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
        }
        for record in state.snapshot.requests.encoded() {
            requests.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
        }
        write_base(transaction, tables, id, state.base.as_ref())?;
        write_chunks(
            transaction,
            tables,
            id,
            &state.chunks,
            true,
            &BTreeSet::new(),
            profile,
        )?;
    }
    Ok(())
}

fn write_base(
    transaction: &WriteTransaction,
    tables: Tables,
    id: &str,
    base: Option<&Snapshot>,
) -> anyhow::Result<()> {
    for definition in [tables.partition_base_data, tables.partition_base_requests] {
        let mut table = transaction.open_table(definition)?;
        let upper = format!("{id}\0");
        let range = (
            Bound::Included((id.as_bytes(), &b""[..])),
            Bound::Excluded((upper.as_bytes(), &b""[..])),
        );
        let keys = table
            .range(range)?
            .map(|entry| entry.map(|(key, _)| key.value().1.to_vec()))
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            table.remove((id.as_bytes(), key.as_slice()))?;
        }
    }
    if let Some(base) = base {
        let mut data = transaction.open_table(tables.partition_base_data)?;
        let mut receipts = transaction.open_table(tables.partition_base_requests)?;
        for record in base.data.encoded() {
            data.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
        }
        for record in base.requests.encoded() {
            receipts.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
        }
    }
    Ok(())
}

fn write_chunks(
    transaction: &WriteTransaction,
    tables: Tables,
    id: &str,
    chunks: &Records,
    replace: bool,
    keys: &BTreeSet<String>,
    profile: &mut StorageTrace,
) -> anyhow::Result<()> {
    if !replace && keys.is_empty() {
        return Ok(());
    }
    let mut table = transaction.open_table(tables.partition_chunks)?;
    if replace {
        let upper = format!("{id}\0");
        let range = (
            Bound::Included((id.as_bytes(), &b""[..])),
            Bound::Excluded((upper.as_bytes(), &b""[..])),
        );
        let stale = table
            .range(range)?
            .map(|entry| entry.map(|(key, _)| key.value().1.to_vec()))
            .collect::<Result<Vec<_>, _>>()?;
        for key in stale {
            table.remove((id.as_bytes(), key.as_slice()))?;
        }
        for record in chunks.encoded() {
            table.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
        }
    } else {
        for key in keys {
            if let Some(value) = chunks.get_raw_shared(key) {
                let version = chunks.raw_version(key).expect("stored chunk");
                table.insert(
                    (id.as_bytes(), key.as_bytes()),
                    encode(version, &profile.encode(value.as_ref())?).as_slice(),
                )?;
            } else {
                table.remove((id.as_bytes(), key.as_bytes()))?;
            }
        }
    }
    Ok(())
}

pub(super) fn write_partitions(
    transaction: &WriteTransaction,
    tables: Tables,
    writes: &BTreeMap<String, PartitionWrite>,
    profile: &mut StorageTrace,
) -> anyhow::Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    let mut metadata = transaction.open_table(tables.partition_meta)?;
    let mut data = transaction.open_table(tables.partition_data)?;
    let mut requests = transaction.open_table(tables.partition_requests)?;
    for (id, write) in writes {
        let state = &write.state;
        if write.replace {
            // The first tuple member isolates the range regardless of arbitrary
            // Unicode/NUL bytes in the partition or application key.
            let upper = format!("{id}\0");
            let range = (
                Bound::Included((id.as_bytes(), &b""[..])),
                Bound::Excluded((upper.as_bytes(), &b""[..])),
            );
            let data_keys = data
                .range(range)?
                .map(|entry| entry.map(|(key, _)| key.value().1.to_vec()))
                .collect::<Result<Vec<_>, _>>()?;
            for key in data_keys {
                data.remove((id.as_bytes(), key.as_slice()))?;
            }
            let request_keys = requests
                .range(range)?
                .map(|entry| entry.map(|(key, _)| key.value().1.to_vec()))
                .collect::<Result<Vec<_>, _>>()?;
            for key in request_keys {
                requests.remove((id.as_bytes(), key.as_slice()))?;
            }
            for record in state.snapshot.data.encoded() {
                data.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
            }
            for record in state.snapshot.requests.encoded() {
                requests.insert((id.as_bytes(), record.key().as_bytes()), record.bytes())?;
            }
        } else {
            // A record is stored with the version it holds in memory, so it
            // keeps its version once served from disk.
            for key in &write.data {
                match state.snapshot.data.get_raw_shared(key) {
                    Some(value) => {
                        let version = state.snapshot.data.raw_version(key).expect("stored record");
                        data.insert(
                            (id.as_bytes(), key.as_bytes()),
                            encode(version, &profile.encode(value.as_ref())?).as_slice(),
                        )?;
                    }
                    None => {
                        data.remove((id.as_bytes(), key.as_bytes()))?;
                    }
                }
            }
            for key in &write.requests {
                if let Some(value) = state.snapshot.requests.get(key) {
                    let version = state.snapshot.requests.version(key).expect("stored receipt");
                    requests.insert(
                        (id.as_bytes(), key.as_bytes()),
                        encode(version, &profile.encode(value)?).as_slice(),
                    )?;
                } else {
                    requests.remove((id.as_bytes(), key.as_bytes()))?;
                }
            }
        }
        if write.replace_base {
            write_base(transaction, tables, id, state.base.as_ref())?;
        }
        write_chunks(
            transaction,
            tables,
            id,
            &state.chunks,
            write.replace_chunks,
            &write.chunks,
            profile,
        )?;
        metadata.insert(
            id.as_str(),
            profile.encode(&Metadata::from(state))?.as_slice(),
        )?;
    }
    Ok(())
}

pub(super) fn apply_scoped(
    partitions: &Partitions,
    writes: &mut BTreeMap<String, PartitionWrite>,
    binding: PartitionBinding,
    command: RaftCommand,
    leader: openraft::CommittedLeaderId<u64>,
) -> ApplyResult {
    let current = writes
        .get(&binding.partition)
        .map(|write| &write.state)
        .or_else(|| partitions.get(&binding.partition));
    if let Err(error) = partitions::validate_binding(&binding)
        .and_then(|_| partitions::active(current, &binding).map(|_| ()))
    {
        return ApplyResult::Rejected(error.to_string());
    }
    let current = current.expect("validated partition exists").clone();
    let mut delta = ApplicationDelta::new(current.snapshot.revision);
    let result = match command {
        RaftCommand::Single(commit) => apply_commit(&current.snapshot, &mut delta, commit),
        RaftCommand::Batch { batch } => apply_batch(&current.snapshot, &mut delta, batch),
        RaftCommand::Retention { retention } => {
            apply_retention(&current.snapshot, &mut delta, retention)
        }
        RaftCommand::Fenced { leader_id, commit } if leader_id == leader => {
            apply_commit(&current.snapshot, &mut delta, commit)
        }
        RaftCommand::Fenced { .. } => ApplyResult::Rejected(
            "partition preparation was authorized under another leader".into(),
        ),
        _ => ApplyResult::Rejected(
            "nested partition scopes and partition controls are invalid application commands"
                .into(),
        ),
    };
    if delta.revision > 9_007_199_254_740_991 {
        return ApplyResult::Rejected("partition application revision exhausted".into());
    }
    let mut write = writes
        .remove(&binding.partition)
        .unwrap_or_else(|| PartitionWrite {
            state: current,
            replace: false,
            replace_base: false,
            data: BTreeSet::new(),
            requests: BTreeSet::new(),
            chunks: BTreeSet::new(),
            replace_chunks: false,
        });
    write.data.extend(delta.data.keys().cloned());
    write.requests.extend(delta.requests.keys().cloned());
    write
        .requests
        .extend(delta.deleted_requests.iter().cloned());
    // Stamped with the whole apply's batch once it is known.
    delta.publish(&mut write.state.snapshot, None, UNSEQUENCED);
    write.state.info.revision = write.state.snapshot.revision;
    writes.insert(binding.partition, write);
    result
}

pub(super) fn apply_partition_control(
    partitions: &Partitions,
    writes: &mut BTreeMap<String, PartitionWrite>,
    command: PartitionCommand,
) -> ApplyResult {
    let id = command.partition().to_owned();
    let current = writes
        .get(&id)
        .map(|write| &write.state)
        .or_else(|| partitions.get(&id));
    match partitions::transition(current, command) {
        Err(error) => ApplyResult::Rejected(error.to_string()),
        Ok(change) => {
            let mut write = writes.remove(&id).unwrap_or_else(|| PartitionWrite {
                state: change.state.clone(),
                replace: false,
                replace_base: false,
                data: BTreeSet::new(),
                requests: BTreeSet::new(),
                chunks: BTreeSet::new(),
                replace_chunks: false,
            });
            write.replace |= change.replace;
            write.replace_base |= change.replace_base;
            write.data.extend(change.data_keys);
            write.requests.extend(change.request_keys);
            write.chunks.extend(change.chunk_keys);
            write.replace_chunks |= change.replace_chunks;
            write.state = change.state;
            let info = write.state.info.clone();
            writes.insert(id, write);
            ApplyResult::Partition(info)
        }
    }
}
