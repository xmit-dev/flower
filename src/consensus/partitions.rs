//! Native ownership-fenced logical databases sharing one physical Raft log.
use super::*;
use im::OrdMap;
use serde::ser::SerializeMap;

pub(super) mod copy;
pub use copy::ExportKind;
use copy::ImportManifest;

const MAX_REVISION: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionBinding {
    pub partition: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionTransfer {
    pub partition: String,
    pub operation: String,
    pub source: String,
    pub destination: String,
    pub source_epoch: u64,
    pub epoch: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionPhase {
    Importing,
    Staged,
    Copied,
    Active,
    Frozen,
    Retired,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionInfo {
    pub partition: String,
    pub epoch: u64,
    pub phase: PartitionPhase,
    pub operation: String,
    pub transfer: Option<PartitionTransfer>,
    pub revision: u64,
    pub digest: Option<String>,
    pub bytes: u64,
    pub received_bytes: u64,
    pub next_chunk: u64,
    #[serde(default)]
    pub base_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PartitionImage {
    pub info: PartitionInfo,
    pub snapshot: Snapshot,
    pub revision: u64,
    pub digest: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PartitionCommand {
    Create {
        partition: String,
        epoch: u64,
        operation: String,
    },
    Capture {
        transfer: PartitionTransfer,
        expected_revision: u64,
        max_bytes: Option<u64>,
    },
    BeginCopy {
        transfer: PartitionTransfer,
        revision: u64,
        digest: String,
        bytes: u64,
    },
    BeginDelta {
        transfer: PartitionTransfer,
        base_revision: u64,
        revision: u64,
        digest: String,
        bytes: u64,
    },
    FinalizeCopy {
        transfer: PartitionTransfer,
        revision: u64,
        digest: String,
        bytes: u64,
    },
    Freeze {
        transfer: PartitionTransfer,
        expected_revision: u64,
    },
    BeginImport {
        transfer: PartitionTransfer,
        revision: u64,
        digest: String,
        bytes: u64,
    },
    ImportChunk {
        partition: String,
        epoch: u64,
        operation: String,
        index: u64,
        data: String,
    },
    SealImport {
        partition: String,
        epoch: u64,
        operation: String,
    },
    Activate {
        partition: String,
        epoch: u64,
        operation: String,
    },
    Retire {
        transfer: PartitionTransfer,
    },
}

impl PartitionCommand {
    pub fn partition(&self) -> &str {
        match self {
            Self::Create { partition, .. }
            | Self::ImportChunk { partition, .. }
            | Self::SealImport { partition, .. }
            | Self::Activate { partition, .. } => partition,
            Self::Capture { transfer, .. }
            | Self::BeginCopy { transfer, .. }
            | Self::BeginDelta { transfer, .. }
            | Self::FinalizeCopy { transfer, .. }
            | Self::Freeze { transfer, .. }
            | Self::BeginImport { transfer, .. }
            | Self::Retire { transfer } => &transfer.partition,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(super) struct PartitionState {
    pub info: PartitionInfo,
    pub snapshot: Snapshot,
    #[serde(default)]
    pub base: Option<Snapshot>,
    #[serde(default)]
    pub last_import: Option<ImportManifest>,
    #[serde(default)]
    pub chunks: Records,
}

#[derive(Clone, Debug, Default)]
pub(super) struct Partitions(OrdMap<String, Arc<PartitionState>>);

impl Serialize for Partitions {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value.as_ref())?;
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for Partitions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = BTreeMap::<String, PartitionState>::deserialize(deserializer)?;
        for (key, value) in &map {
            if key != &value.info.partition {
                return Err(serde::de::Error::custom(
                    "partition key does not match its identity",
                ));
            }
            validate_state(value).map_err(serde::de::Error::custom)?;
        }
        Ok(Self(
            map.into_iter()
                .map(|(key, value)| (key, Arc::new(value)))
                .collect(),
        ))
    }
}
impl Partitions {
    pub fn get(&self, id: &str) -> Option<&PartitionState> {
        self.0.get(id).map(Arc::as_ref)
    }
    pub fn insert(&mut self, state: PartitionState) {
        self.0.insert(state.info.partition.clone(), Arc::new(state));
    }
    pub fn iter(&self) -> impl Iterator<Item = (&String, &PartitionState)> {
        self.0.iter().map(|(id, state)| (id, state.as_ref()))
    }

    /// Change in place the states `select` picks, given each one and its
    /// position in id order. Others stay shared with earlier copies.
    pub(super) fn update_some(
        &mut self,
        mut select: impl FnMut(usize, &PartitionState) -> bool,
        mut update: impl FnMut(&mut PartitionState) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let ids: Vec<String> = self
            .0
            .iter()
            .enumerate()
            .filter(|(position, (_, state))| select(*position, state))
            .map(|(_, (id, _))| id.clone())
            .collect();
        for id in ids {
            let state = self.0.get_mut(&id).expect("listed partition");
            update(Arc::make_mut(state))?;
        }
        Ok(())
    }
}

pub(super) fn validate_binding(binding: &PartitionBinding) -> anyhow::Result<()> {
    anyhow::ensure!(
        !binding.partition.is_empty() && binding.epoch > 0 && binding.epoch <= MAX_REVISION,
        "partition name must be nonempty and epoch must be a positive safe integer"
    );
    Ok(())
}

pub(super) fn active<'a>(
    state: Option<&'a PartitionState>,
    binding: &PartitionBinding,
) -> anyhow::Result<&'a Snapshot> {
    let state = state.context("partition unavailable: logical partition does not exist here")?;
    anyhow::ensure!(
        state.info.epoch == binding.epoch,
        "partition unavailable: ownership epoch changed from {} to {}",
        binding.epoch,
        state.info.epoch
    );
    anyhow::ensure!(
        state.info.phase == PartitionPhase::Active,
        "partition unavailable: partition is {:?}",
        state.info.phase
    );
    Ok(&state.snapshot)
}

fn validate_transfer(transfer: &PartitionTransfer) -> anyhow::Result<()> {
    validate_binding(&PartitionBinding {
        partition: transfer.partition.clone(),
        epoch: transfer.epoch,
    })?;
    anyhow::ensure!(
        transfer.source_epoch > 0 && transfer.epoch > transfer.source_epoch,
        "partition transfer epoch must advance ownership"
    );
    anyhow::ensure!(
        !transfer.operation.is_empty()
            && !transfer.source.is_empty()
            && !transfer.destination.is_empty()
            && transfer.source != transfer.destination,
        "partition transfer requires an operation and distinct source/destination groups"
    );
    Ok(())
}

fn validate_snapshot(snapshot: &Snapshot) -> anyhow::Result<()> {
    retention::validate_snapshot(snapshot)?;
    anyhow::ensure!(
        snapshot.revision <= MAX_REVISION
            && snapshot.data.has_valid_depth()
            && snapshot.data.has_valid_source_ids()
            && snapshot.data.has_valid_graph_pointer(),
        "invalid partition snapshot revision, source key, graph pointer or JSON depth"
    );
    // Stored receipts were checked when written; decoded ones are checked here.
    if snapshot.requests.backing().is_none() {
        for (_, receipt) in &snapshot.requests {
            anyhow::ensure!(
                receipt.revision > 0 && receipt.revision <= snapshot.revision,
                "partition receipt revision is outside the imported history"
            );
        }
    }
    Ok(())
}

pub(super) fn validate_state(state: &PartitionState) -> anyhow::Result<()> {
    validate_state_with(state, true)
}

/// Check a partition read back from storage, whose records were checked
/// when written: its metadata only, without reading every receipt.
pub(super) fn validate_stored_state(state: &PartitionState) -> anyhow::Result<()> {
    validate_state_with(state, false)
}

fn validate_state_with(state: &PartitionState, records: bool) -> anyhow::Result<()> {
    let info = &state.info;
    validate_binding(&PartitionBinding {
        partition: info.partition.clone(),
        epoch: info.epoch,
    })?;
    anyhow::ensure!(
        !info.operation.is_empty() && info.revision <= MAX_REVISION,
        "partition operation or revision is invalid"
    );
    if let Some(transfer) = &info.transfer {
        validate_transfer(transfer)?;
        let expected_epoch = match info.phase {
            PartitionPhase::Frozen | PartitionPhase::Retired => transfer.source_epoch,
            PartitionPhase::Active if state.base.is_some() => transfer.source_epoch,
            _ => transfer.epoch,
        };
        anyhow::ensure!(
            transfer.partition == info.partition
                && transfer.operation == info.operation
                && expected_epoch == info.epoch,
            "partition transfer identity does not match its ownership metadata"
        );
    } else {
        anyhow::ensure!(
            matches!(info.phase, PartitionPhase::Staged | PartitionPhase::Active),
            "partition lifecycle phase requires a transfer"
        );
    }
    if let Some(manifest) = &state.last_import {
        anyhow::ensure!(
            manifest.bytes > 0
                && manifest.revision <= MAX_REVISION
                && manifest.digest.len() == 64
                && manifest
                    .digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
            "invalid retained partition import manifest"
        );
        anyhow::ensure!(
            match manifest.kind {
                ExportKind::Base => manifest.base_revision.is_none(),
                ExportKind::Delta => manifest
                    .base_revision
                    .is_some_and(|revision| revision < manifest.revision),
                ExportKind::Snapshot => false,
            },
            "invalid retained partition import kind"
        );
        if matches!(
            info.phase,
            PartitionPhase::Importing | PartitionPhase::Copied
        ) {
            anyhow::ensure!(
                info.revision == manifest.revision
                    && info.digest.as_ref() == Some(&manifest.digest)
                    && info.bytes == manifest.bytes,
                "partition import metadata diverged from its manifest"
            );
        }
    }
    let delta_import = info.phase == PartitionPhase::Importing
        && state
            .last_import
            .as_ref()
            .is_some_and(|manifest| manifest.kind == ExportKind::Delta);
    if info.phase == PartitionPhase::Importing {
        anyhow::ensure!(
            info.digest
                .as_ref()
                .is_some_and(|digest| digest.len() == 64)
                && info.bytes > 0
                && info.received_bytes <= info.bytes
                && (delta_import
                    || (state.snapshot.revision == 0 && state.snapshot.requests.is_empty())),
            "partition import progress metadata is invalid"
        );
    } else {
        anyhow::ensure!(
            info.revision == state.snapshot.revision,
            "partition metadata revision mismatch"
        );
        if records {
            validate_snapshot(&state.snapshot)?;
        }
    }
    if delta_import {
        anyhow::ensure!(
            state
                .last_import
                .as_ref()
                .and_then(|manifest| manifest.base_revision)
                == Some(state.snapshot.revision),
            "delta import base revision mismatch"
        );
        if records {
            validate_snapshot(&state.snapshot)?;
        }
    } else {
        anyhow::ensure!(
            state.chunks.is_empty(),
            "partition retains orphan difference chunks"
        );
    }
    if let Some(base) = &state.base {
        if records {
            validate_snapshot(base)?;
        }
        anyhow::ensure!(info.base_bytes > 0, "partition base has no size accounting");
    } else {
        anyhow::ensure!(
            info.base_bytes == 0,
            "partition base size without retained base"
        );
    }
    if info.phase == PartitionPhase::Copied {
        anyhow::ensure!(
            state
                .last_import
                .as_ref()
                .is_some_and(|manifest| manifest.kind == ExportKind::Base),
            "copied partition has no base manifest"
        );
    }
    if info.phase == PartitionPhase::Retired {
        anyhow::ensure!(
            state.snapshot.data.is_empty() && state.snapshot.requests.is_empty(),
            "retired partition retains application state"
        );
    }
    Ok(())
}

fn can_move(snapshot: &Snapshot) -> anyhow::Result<()> {
    anyhow::ensure!(
        !snapshot.data.contains_key("transaction:participant"),
        "partition migration waits for its prepared transaction"
    );
    for (_, value) in snapshot
        .data
        .entries::<(std::ops::Bound<&str>, std::ops::Bound<&str>)>((
            std::ops::Bound::Included("transaction:coordinator:"),
            std::ops::Bound::Unbounded,
        ))
        .take_while(|(key, _)| key.starts_with("transaction:coordinator:"))
    {
        anyhow::ensure!(
            value.get("complete").and_then(Value::as_bool) == Some(true)
                && matches!(
                    value.get("phase").and_then(Value::as_str),
                    Some("commit" | "abort")
                ),
            "partition migration waits for its incomplete transaction coordinator"
        );
    }
    Ok(())
}

pub(super) struct Change {
    pub state: PartitionState,
    pub replace: bool,
    pub replace_base: bool,
    pub data_keys: Vec<String>,
    pub request_keys: Vec<String>,
    pub chunk_keys: Vec<String>,
    pub replace_chunks: bool,
}

pub(super) fn transition(
    current: Option<&PartitionState>,
    command: PartitionCommand,
) -> anyhow::Result<Change> {
    if matches!(
        &command,
        PartitionCommand::Capture { .. }
            | PartitionCommand::BeginCopy { .. }
            | PartitionCommand::BeginDelta { .. }
            | PartitionCommand::FinalizeCopy { .. }
    ) {
        return copy::transition(current, command);
    }
    let mut replace = false;
    let mut replace_base = false;
    let mut data_keys = Vec::new();
    let mut request_keys = Vec::new();
    let mut chunk_keys = Vec::new();
    let mut replace_chunks = false;
    let state = match command {
        PartitionCommand::Capture { .. }
        | PartitionCommand::BeginCopy { .. }
        | PartitionCommand::BeginDelta { .. }
        | PartitionCommand::FinalizeCopy { .. } => unreachable!(),
        PartitionCommand::Create {
            partition,
            epoch,
            operation,
        } => {
            validate_binding(&PartitionBinding {
                partition: partition.clone(),
                epoch,
            })?;
            anyhow::ensure!(
                !operation.is_empty(),
                "partition creation requires an operation"
            );
            if let Some(current) = current {
                anyhow::ensure!(
                    current.info.epoch == epoch
                        && current.info.operation == operation
                        && current.info.transfer.is_none()
                        && matches!(
                            current.info.phase,
                            PartitionPhase::Staged | PartitionPhase::Active
                        ),
                    "partition already exists with another ownership operation"
                );
                current.clone()
            } else {
                replace = true;
                PartitionState {
                    info: PartitionInfo {
                        partition,
                        epoch,
                        phase: PartitionPhase::Staged,
                        operation,
                        transfer: None,
                        revision: 0,
                        digest: None,
                        bytes: 0,
                        received_bytes: 0,
                        next_chunk: 0,
                        base_bytes: 0,
                    },
                    snapshot: Snapshot::default(),
                    base: None,
                    last_import: None,
                    chunks: Records::default(),
                }
            }
        }
        PartitionCommand::Freeze {
            transfer,
            expected_revision,
        } => {
            validate_transfer(&transfer)?;
            let mut next = current
                .context("partition to freeze does not exist")?
                .clone();
            if next.info.phase == PartitionPhase::Frozen
                && next.info.transfer.as_ref() == Some(&transfer)
            {
                return Ok(Change {
                    state: next,
                    replace,
                    replace_base,
                    data_keys,
                    request_keys,
                    chunk_keys,
                    replace_chunks,
                });
            }
            anyhow::ensure!(
                next.info.epoch == transfer.source_epoch
                    && next.info.phase == PartitionPhase::Active,
                "partition is not active at the source ownership epoch"
            );
            anyhow::ensure!(
                next.snapshot.revision == expected_revision,
                "partition changed before freeze"
            );
            can_move(&next.snapshot)?;
            next.info.phase = PartitionPhase::Frozen;
            next.info.operation = transfer.operation.clone();
            next.info.digest = None;
            next.info.bytes = 0;
            next.info.received_bytes = 0;
            next.info.next_chunk = 0;
            next.info.transfer = Some(transfer);
            next
        }
        PartitionCommand::BeginImport {
            transfer,
            revision,
            digest,
            bytes,
        } => {
            validate_transfer(&transfer)?;
            anyhow::ensure!(
                revision <= MAX_REVISION
                    && bytes > 0
                    && digest.len() == 64
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "invalid partition image manifest"
            );
            let replacing_base = current.is_some_and(|current| {
                current.info.phase == PartitionPhase::Copied
                    && current.info.transfer.as_ref() == Some(&transfer)
                    && revision > current.snapshot.revision
            });
            if let Some(current) = current
                && current.info.epoch == transfer.epoch
                && current.info.transfer.as_ref() == Some(&transfer)
                && !replacing_base
            {
                anyhow::ensure!(
                    current.info.digest.as_ref() == Some(&digest)
                        && current.info.bytes == bytes
                        && (current.info.revision == revision
                            || current.info.phase == PartitionPhase::Active)
                        && matches!(
                            current.info.phase,
                            PartitionPhase::Importing
                                | PartitionPhase::Staged
                                | PartitionPhase::Active
                        ),
                    "partition import manifest changed"
                );
                return Ok(Change {
                    state: current.clone(),
                    replace,
                    replace_base,
                    data_keys,
                    request_keys,
                    chunk_keys,
                    replace_chunks,
                });
            }
            if let Some(current) = current {
                anyhow::ensure!(
                    replacing_base
                        || (current.info.phase == PartitionPhase::Retired
                            && current.info.epoch < transfer.epoch),
                    "destination already has a live or newer partition"
                );
            }
            replace = true;
            PartitionState {
                info: PartitionInfo {
                    partition: transfer.partition.clone(),
                    epoch: transfer.epoch,
                    phase: PartitionPhase::Importing,
                    operation: transfer.operation.clone(),
                    transfer: Some(transfer),
                    revision,
                    digest: Some(digest),
                    bytes,
                    received_bytes: 0,
                    next_chunk: 0,
                    base_bytes: 0,
                },
                snapshot: Snapshot::default(),
                base: None,
                last_import: None,
                chunks: Records::default(),
            }
        }
        PartitionCommand::ImportChunk {
            partition,
            epoch,
            operation,
            index,
            data,
        } => {
            let mut next = matching(current, &partition, epoch, &operation)?.clone();
            anyhow::ensure!(
                next.info.phase == PartitionPhase::Importing && !data.is_empty(),
                "partition is not accepting import chunks"
            );
            let delta = next
                .last_import
                .as_ref()
                .is_some_and(|manifest| manifest.kind == ExportKind::Delta);
            let chunks = if delta {
                &mut next.chunks
            } else {
                &mut next.snapshot.data
            };
            let key = format!("chunk:{index}");
            if index < next.info.next_chunk {
                anyhow::ensure!(
                    chunks.get(&key).and_then(Value::as_str) == Some(&data),
                    "partition chunk replay changed content"
                );
            } else {
                anyhow::ensure!(
                    index == next.info.next_chunk,
                    "partition chunks must be contiguous"
                );
                let received = next
                    .info
                    .received_bytes
                    .checked_add(data.len() as u64)
                    .context("partition import length overflow")?;
                anyhow::ensure!(
                    received <= next.info.bytes,
                    "partition import exceeds manifest byte count"
                );
                next.info.next_chunk = index
                    .checked_add(1)
                    .context("partition chunk index exhausted")?;
                next.info.received_bytes = received;
                chunks.insert(key.clone(), Value::String(data));
                if delta {
                    chunk_keys.push(key);
                } else {
                    data_keys.push(key);
                }
            }
            next
        }
        PartitionCommand::SealImport {
            partition,
            epoch,
            operation,
        } => {
            let mut next = matching(current, &partition, epoch, &operation)?.clone();
            if matches!(
                next.info.phase,
                PartitionPhase::Staged | PartitionPhase::Copied | PartitionPhase::Active
            ) && next.info.transfer.is_some()
            {
                return Ok(Change {
                    state: next,
                    replace,
                    replace_base,
                    data_keys,
                    request_keys,
                    chunk_keys,
                    replace_chunks,
                });
            }
            anyhow::ensure!(
                next.info.phase == PartitionPhase::Importing
                    && next.info.received_bytes == next.info.bytes,
                "partition import is incomplete"
            );
            let capacity = usize::try_from(next.info.bytes)
                .context("partition image exceeds platform address space")?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(capacity)
                .context("allocate imported partition image")?;
            let delta_import = next
                .last_import
                .as_ref()
                .is_some_and(|manifest| manifest.kind == ExportKind::Delta);
            let chunks = if delta_import {
                &next.chunks
            } else {
                &next.snapshot.data
            };
            for index in 0..next.info.next_chunk {
                bytes.extend_from_slice(
                    chunks
                        .get(&format!("chunk:{index}"))
                        .and_then(Value::as_str)
                        .context("partition import chunk is missing")?
                        .as_bytes(),
                );
            }
            anyhow::ensure!(
                next.info.digest.as_deref() == Some(&digest(&bytes)),
                "partition image hash mismatch"
            );
            let (snapshot, base_only, data_changes, receipt_changes) =
                copy::decode_import(&next, &bytes)?;
            data_keys.extend(data_changes);
            request_keys.extend(receipt_changes);
            validate_snapshot(&snapshot)?;
            if !base_only {
                can_move(&snapshot)?;
            }
            anyhow::ensure!(
                snapshot.revision == next.info.revision,
                "partition image revision differs from manifest"
            );
            if next
                .last_import
                .as_ref()
                .is_some_and(|manifest| manifest.kind == ExportKind::Delta)
            {
                let (digest, bytes) = copy::snapshot_manifest(&snapshot)?;
                next.info.digest = Some(digest);
                next.info.bytes = bytes;
                next.info.received_bytes = bytes;
            }
            next.snapshot = snapshot;
            next.info.phase = if base_only {
                PartitionPhase::Copied
            } else {
                PartitionPhase::Staged
            };
            next.base = None;
            next.chunks = Records::default();
            next.info.base_bytes = 0;
            replace_base = true;
            replace_chunks = true;
            replace = !delta_import;
            next
        }
        PartitionCommand::Activate {
            partition,
            epoch,
            operation,
        } => {
            let mut next = matching(current, &partition, epoch, &operation)?.clone();
            anyhow::ensure!(
                matches!(
                    next.info.phase,
                    PartitionPhase::Staged | PartitionPhase::Active
                ),
                "partition is not staged for activation"
            );
            can_move(&next.snapshot)?;
            next.info.phase = PartitionPhase::Active;
            next
        }
        PartitionCommand::Retire { transfer } => {
            validate_transfer(&transfer)?;
            let mut next = current
                .context("partition to retire does not exist")?
                .clone();
            anyhow::ensure!(
                next.info.epoch == transfer.source_epoch
                    && next.info.transfer.as_ref() == Some(&transfer)
                    && matches!(
                        next.info.phase,
                        PartitionPhase::Frozen | PartitionPhase::Retired
                    ),
                "partition is not frozen for this transfer"
            );
            next.info.phase = PartitionPhase::Retired;
            next.snapshot.data = Records::default();
            next.snapshot.requests = Receipts::default();
            next.base = None;
            next.last_import = None;
            next.info.base_bytes = 0;
            replace_base = true;
            replace = true;
            next
        }
    };
    validate_state(&state)?;
    Ok(Change {
        state,
        replace,
        replace_base,
        data_keys,
        request_keys,
        chunk_keys,
        replace_chunks,
    })
}

fn matching<'a>(
    current: Option<&'a PartitionState>,
    partition: &str,
    epoch: u64,
    operation: &str,
) -> anyhow::Result<&'a PartitionState> {
    let current = current.context("partition does not exist")?;
    anyhow::ensure!(
        current.info.partition == partition
            && current.info.epoch == epoch
            && current.info.operation == operation,
        "partition ownership operation changed"
    );
    Ok(current)
}

pub fn partition_image_digest(bytes: &[u8]) -> String {
    digest(bytes)
}
fn digest(bytes: &[u8]) -> String {
    crate::evaluator::hash(bytes)
}

impl Consensus {
    pub fn partition(&self, partition: impl Into<String>, epoch: u64) -> anyhow::Result<Self> {
        let binding = PartitionBinding {
            partition: partition.into(),
            epoch,
        };
        validate_binding(&binding)?;
        let mut scoped = self.clone();
        scoped.partition_overhead = encoded_json_len(&serde_json::json!({
            "partition":binding.partition,"epoch":binding.epoch,"command":null
        }))?
        .checked_sub(4)
        .context("invalid partition envelope size")?;
        scoped.partition = Some(binding);
        Ok(scoped)
    }

    pub(crate) fn physical(&self) -> Self {
        let mut physical = self.clone();
        physical.partition = None;
        physical.partition_overhead = 0;
        physical
    }

    pub fn partition_binding(&self) -> Option<&PartitionBinding> {
        self.partition.as_ref()
    }

    pub async fn list_partitions(&self) -> anyhow::Result<Vec<PartitionInfo>> {
        self.read_barrier().await?;
        Ok(self.store.partition_infos())
    }

    pub async fn partition_info(&self, partition: &str) -> anyhow::Result<PartitionInfo> {
        self.read_barrier().await?;
        self.store.partition_info(partition)
    }

    /// Local cache reclamation only; this observation does not authorize work.
    pub(crate) fn partition_info_local(&self, partition: &str) -> anyhow::Result<PartitionInfo> {
        self.store.partition_info(partition)
    }

    /// Control-plane readiness check for a staged import. This is deliberately
    /// unavailable through application methods and never performs key I/O in
    /// the deterministic replicated state machine.
    pub(crate) async fn partition_key_catalog(
        &self,
        partition: &str,
    ) -> anyhow::Result<Option<Value>> {
        self.read_barrier().await?;
        Ok(self
            .store
            .partition_state(partition)?
            .snapshot
            .data
            .get("managedKeys")
            .cloned())
    }

    pub async fn export_partition(
        &self,
        partition: &str,
        operation: &str,
    ) -> anyhow::Result<PartitionImage> {
        self.read_barrier().await?;
        let state = self.store.partition_state(partition)?;
        anyhow::ensure!(
            state.info.phase == PartitionPhase::Frozen && state.info.operation == operation,
            "partition must be frozen for this export operation"
        );
        let bytes = serde_json::to_vec(&state.snapshot)?;
        Ok(PartitionImage {
            revision: state.snapshot.revision,
            info: state.info,
            snapshot: state.snapshot,
            digest: digest(&bytes),
            bytes: bytes.len() as u64,
        })
    }

    pub async fn control_partition(
        &self,
        command: PartitionCommand,
        leader_id: Option<CommittedLeaderId<u64>>,
    ) -> anyhow::Result<PartitionInfo> {
        anyhow::ensure!(
            self.partition.is_none(),
            "partition controls require an unscoped physical consensus handle"
        );
        let command = RaftCommand::PartitionControl {
            partition_control: command,
            leader_id,
        };
        anyhow::ensure!(
            encoded_json_len(&command)? <= self.limits.transaction_max_bytes,
            "partition command exceeds FLOWER_TRANSACTION_MAX_BYTES"
        );
        let response =
            tokio::time::timeout(self.limits.commit_timeout, self.raft.client_write(command))
                .await
                .context("partition command timed out; retry the same operation")??;
        match response.data {
            ApplyResult::Partition(info) => Ok(info),
            ApplyResult::Rejected(reason) => bail!(reason),
            _ => bail!("unexpected partition control response"),
        }
    }

    pub(super) fn scope_command(&self, command: RaftCommand) -> RaftCommand {
        match &self.partition {
            Some(binding) => RaftCommand::Scoped {
                partition: binding.partition.clone(),
                epoch: binding.epoch,
                command: Box::new(command),
            },
            None => command,
        }
    }
}
