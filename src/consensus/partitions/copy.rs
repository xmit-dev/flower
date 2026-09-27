//! One durable base plus a final state difference avoids retaining an unbounded
//! per-write journal. A copied base is never a serving/activatable partition.
use super::*;
use sha2::{Digest, Sha256};
use std::io::Write;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    #[default]
    Snapshot,
    Base,
    Delta,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::consensus) struct ImportManifest {
    pub kind: ExportKind,
    pub revision: u64,
    pub digest: String,
    pub bytes: u64,
    pub base_revision: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delta {
    base_revision: u64,
    revision: u64,
    final_digest: String,
    data: Records,
    deletes: Vec<String>,
    requests: Receipts,
    deleted_requests: Vec<String>,
}

struct Hasher {
    hash: Sha256,
    bytes: u64,
}
impl Write for Hasher {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("snapshot byte count overflow"))?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) fn snapshot_manifest(snapshot: &Snapshot) -> anyhow::Result<(String, u64)> {
    let mut writer = Hasher {
        hash: Sha256::new(),
        bytes: 0,
    };
    serde_json::to_writer(&mut writer, snapshot)?;
    Ok((
        writer
            .hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        writer.bytes,
    ))
}

fn change(state: PartitionState, replace: bool, replace_base: bool) -> anyhow::Result<Change> {
    validate_state(&state)?;
    Ok(Change {
        state,
        replace,
        replace_base,
        data_keys: vec![],
        request_keys: vec![],
        chunk_keys: vec![],
        replace_chunks: false,
    })
}

pub(super) fn transition(
    current: Option<&PartitionState>,
    command: PartitionCommand,
) -> anyhow::Result<Change> {
    match command {
        PartitionCommand::Capture {
            transfer,
            expected_revision,
            max_bytes,
        } => {
            validate_transfer(&transfer)?;
            let mut next = current.context("partition to copy does not exist")?.clone();
            if next.info.transfer.as_ref() == Some(&transfer)
                && next.base.is_some()
                && matches!(
                    next.info.phase,
                    PartitionPhase::Active | PartitionPhase::Frozen
                )
            {
                return change(next, false, false);
            }
            anyhow::ensure!(
                next.info.phase == PartitionPhase::Active
                    && next.info.epoch == transfer.source_epoch,
                "copy source ownership changed"
            );
            anyhow::ensure!(
                next.snapshot.revision == expected_revision,
                "partition changed before capture"
            );
            let bytes = encoded_json_len(&next.snapshot)? as u64;
            anyhow::ensure!(
                max_bytes.is_none_or(|limit| bytes <= limit),
                "partition base exceeds FLOWER_PARTITION_BASE_MAX_BYTES"
            );
            next.info.operation = transfer.operation.clone();
            next.info.transfer = Some(transfer);
            next.info.base_bytes = bytes;
            next.base = Some(next.snapshot.clone());
            next.last_import = None;
            change(next, false, true)
        }
        PartitionCommand::BeginCopy {
            transfer,
            revision,
            digest,
            bytes,
        } => {
            let manifest = ImportManifest {
                kind: ExportKind::Base,
                revision,
                digest: digest.clone(),
                bytes,
                base_revision: None,
            };
            if let Some(current) = current
                && current.info.transfer.as_ref() == Some(&transfer)
                && current.last_import.as_ref() == Some(&manifest)
                && matches!(
                    current.info.phase,
                    PartitionPhase::Importing | PartitionPhase::Copied | PartitionPhase::Staged
                )
            {
                return change(current.clone(), false, false);
            }
            let mut next = super::transition(
                current,
                PartitionCommand::BeginImport {
                    transfer,
                    revision,
                    digest,
                    bytes,
                },
            )?;
            next.state.last_import = Some(manifest);
            Ok(next)
        }
        PartitionCommand::BeginDelta {
            transfer,
            base_revision,
            revision,
            digest,
            bytes,
        } => {
            validate_transfer(&transfer)?;
            let manifest = ImportManifest {
                kind: ExportKind::Delta,
                revision,
                digest: digest.clone(),
                bytes,
                base_revision: Some(base_revision),
            };
            let mut next = matching(
                current,
                &transfer.partition,
                transfer.epoch,
                &transfer.operation,
            )?
            .clone();
            anyhow::ensure!(
                next.info.transfer.as_ref() == Some(&transfer),
                "delta transfer differs"
            );
            if next.last_import.as_ref() == Some(&manifest)
                && matches!(
                    next.info.phase,
                    PartitionPhase::Importing | PartitionPhase::Staged
                )
            {
                return change(next, false, false);
            }
            anyhow::ensure!(
                next.info.phase == PartitionPhase::Copied
                    && next.snapshot.revision == base_revision
                    && revision > base_revision
                    && revision <= MAX_REVISION,
                "delta does not follow the copied base"
            );
            anyhow::ensure!(
                bytes > 0
                    && digest.len() == 64
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "invalid delta manifest"
            );
            next.chunks = Records::default();
            next.last_import = Some(manifest);
            next.info.phase = PartitionPhase::Importing;
            next.info.revision = revision;
            next.info.digest = Some(digest);
            next.info.bytes = bytes;
            next.info.received_bytes = 0;
            next.info.next_chunk = 0;
            let mut change = change(next, false, false)?;
            change.replace_chunks = true;
            Ok(change)
        }
        PartitionCommand::FinalizeCopy {
            transfer,
            revision,
            digest,
            bytes,
        } => {
            let mut next = matching(
                current,
                &transfer.partition,
                transfer.epoch,
                &transfer.operation,
            )?
            .clone();
            anyhow::ensure!(
                next.info.transfer.as_ref() == Some(&transfer)
                    && matches!(
                        next.info.phase,
                        PartitionPhase::Copied | PartitionPhase::Staged
                    ),
                "partition has no copied base to finalize"
            );
            anyhow::ensure!(
                next.snapshot.revision == revision
                    && next.info.digest.as_ref() == Some(&digest)
                    && next.info.bytes == bytes,
                "frozen source differs from copied base"
            );
            can_move(&next.snapshot)?;
            next.info.phase = PartitionPhase::Staged;
            change(next, false, false)
        }
        _ => unreachable!(),
    }
}

pub(super) fn decode_import(
    state: &PartitionState,
    bytes: &[u8],
) -> anyhow::Result<(Snapshot, bool, Vec<String>, Vec<String>)> {
    match state.last_import.as_ref().map(|manifest| manifest.kind) {
        Some(ExportKind::Delta) => {
            let manifest = state.last_import.as_ref().expect("delta manifest");
            let delta: Delta =
                serde_json::from_slice(bytes).context("decode partition difference")?;
            let mut snapshot = state.snapshot.clone();
            anyhow::ensure!(
                Some(snapshot.revision) == manifest.base_revision
                    && delta.base_revision == snapshot.revision
                    && delta.revision == manifest.revision,
                "partition difference revision mismatch"
            );
            let data_keys = delta
                .data
                .keys()
                .chain(delta.deletes.iter().cloned())
                .collect();
            let request_keys = delta
                .requests
                .iter()
                .map(|(key, _)| key.clone())
                .chain(delta.deleted_requests.iter().cloned())
                .collect();
            for (key, value) in &delta.data {
                snapshot.data.insert(key.clone(), value.clone());
            }
            for key in delta.deletes {
                snapshot.data.remove(&key);
            }
            for (key, receipt) in &delta.requests {
                snapshot.requests.insert(key.clone(), receipt.clone());
            }
            for key in delta.deleted_requests {
                snapshot.requests.remove(&key);
            }
            snapshot.revision = delta.revision;
            let (digest, _) = snapshot_manifest(&snapshot)?;
            anyhow::ensure!(
                digest == delta.final_digest,
                "partition difference final digest mismatch"
            );
            Ok((snapshot, false, data_keys, request_keys))
        }
        kind => Ok((
            serde_json::from_slice(bytes).context("decode partition image")?,
            kind == Some(ExportKind::Base),
            vec![],
            vec![],
        )),
    }
}

fn delta(base: &Snapshot, current: &Snapshot) -> anyhow::Result<Delta> {
    let mut data = Records::new();
    let mut deletes = Vec::new();
    // An unchanged record keeps its write's version; a record written back
    // with the same value is unchanged too.
    for (key, value) in current.data.entries(..) {
        let unchanged = base.data.raw_version(&key) == current.data.raw_version(&key)
            || base
                .data
                .get_raw_shared(&key)
                .is_some_and(|previous| Arc::ptr_eq(previous, &value) || previous == &value);
        if !unchanged {
            data.insert_shared(key, value);
        }
    }
    for key in base.data.keys() {
        if current.data.raw_version(&key).is_none() {
            deletes.push(key);
        }
    }
    let mut requests = Receipts::new();
    let mut deleted_requests = Vec::new();
    for (key, receipt) in &current.requests {
        let shared = current.requests.get_shared(key).expect("present");
        let unchanged = base
            .requests
            .get_shared(key)
            .is_some_and(|previous| Arc::ptr_eq(previous, shared) || previous.as_ref() == receipt);
        if !unchanged {
            requests.insert_shared(
                key.clone(),
                current.requests.get_shared(key).expect("present").clone(),
            );
        }
    }
    for (key, _) in &base.requests {
        if !current.requests.contains_key(key) {
            deleted_requests.push(key.clone());
        }
    }
    Ok(Delta {
        base_revision: base.revision,
        revision: current.revision,
        final_digest: snapshot_manifest(current)?.0,
        data,
        deletes,
        requests,
        deleted_requests,
    })
}

#[cfg(test)]
pub(in crate::consensus) fn export_payload(
    state: PartitionState,
    kind: ExportKind,
) -> anyhow::Result<(u64, Option<u64>, String)> {
    let base_revision = state.base.as_ref().map(|base| base.revision);
    let (revision, bytes) = match kind {
        ExportKind::Base => {
            let base = state.base.context("copy base disappeared")?;
            (base.revision, serde_json::to_string(&base)?)
        }
        ExportKind::Snapshot => (
            state.snapshot.revision,
            serde_json::to_string(&state.snapshot)?,
        ),
        ExportKind::Delta => {
            let difference = delta(
                state.base.as_ref().context("copy base missing")?,
                &state.snapshot,
            )?;
            (state.snapshot.revision, serde_json::to_string(&difference)?)
        }
    };
    Ok((revision, base_revision, bytes))
}

pub(crate) struct ReservedExport<R> {
    pub revision: u64,
    pub base_revision: Option<u64>,
    pub bytes: String,
    pub retained: R,
}

fn encode_reserved<T: Serialize, R>(
    value: &T,
    reserve: &impl Fn(usize) -> anyhow::Result<R>,
) -> anyhow::Result<(String, R)> {
    let size = crate::consensus::encoded_json_len(value)?;
    let retained = reserve(size.saturating_add(512))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size)?;
    serde_json::to_writer(&mut bytes, value)?;
    Ok((String::from_utf8(bytes)?, retained))
}

fn reserved_export<R>(
    state: PartitionState,
    kind: ExportKind,
    reserve: impl Fn(usize) -> anyhow::Result<R>,
) -> anyhow::Result<ReservedExport<R>> {
    let base_revision = state.base.as_ref().map(|base| base.revision);
    let (revision, (bytes, retained)) = match kind {
        ExportKind::Base => {
            let base = state.base.context("copy base disappeared")?;
            (base.revision, encode_reserved(&base, &reserve)?)
        }
        ExportKind::Snapshot => (
            state.snapshot.revision,
            encode_reserved(&state.snapshot, &reserve)?,
        ),
        ExportKind::Delta => {
            let base = state.base.as_ref().context("copy base missing")?;
            // Charge workspace before constructing difference trees and deletion
            // lists. Shared values avoid full clones, but keys/index metadata allocate.
            let estimate = crate::consensus::encoded_json_len(base)?
                .saturating_add(crate::consensus::encoded_json_len(&state.snapshot)?)
                .saturating_mul(2)
                .saturating_add(
                    base.data
                        .len()
                        .saturating_add(base.requests.len())
                        .saturating_add(state.snapshot.data.len())
                        .saturating_add(state.snapshot.requests.len())
                        .saturating_mul(512),
                );
            let _workspace = reserve(estimate)?;
            let difference = delta(base, &state.snapshot)?;
            (
                state.snapshot.revision,
                encode_reserved(&difference, &reserve)?,
            )
        }
    };
    Ok(ReservedExport {
        revision,
        base_revision,
        bytes,
        retained,
    })
}

impl Consensus {
    /// The returned roots are immutable. Encoding/difference construction runs
    /// on a blocking worker after releasing every apply/publication lock.
    pub(crate) async fn export_partition_payload<R: Send + 'static>(
        &self,
        partition: &str,
        operation: &str,
        kind: ExportKind,
        reserve: impl Fn(usize) -> anyhow::Result<R> + Send + 'static,
        admitted: impl Send + 'static,
    ) -> anyhow::Result<ReservedExport<R>> {
        self.read_barrier().await?;
        let state = self.store.partition_state(partition)?;
        anyhow::ensure!(
            state.info.operation == operation,
            "partition export operation changed"
        );
        match kind {
            ExportKind::Base => anyhow::ensure!(
                state.base.is_some()
                    && matches!(
                        state.info.phase,
                        PartitionPhase::Active | PartitionPhase::Frozen
                    ),
                "partition has no captured base"
            ),
            _ => anyhow::ensure!(
                state.info.phase == PartitionPhase::Frozen,
                "partition must be frozen for final export"
            ),
        }
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let _admitted = admitted;
            reserved_export(state, kind, reserve)
        })
        .await?
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn graph_generation_differences_preserve_physical_rows_and_legacy_cleanup() {
        let generation = "a".repeat(64);
        let cell = r#"cell:["leaf",null]"#;
        let root = r#"root:["leaf",null]"#;
        let value = |number| serde_json::json!({"name":"leaf","args":null,
            "outcome":{"ok":true,"value":number},"deps":[]});
        let mut base = Snapshot::default();
        base.data.insert(cell.into(), value(1));
        base.data.insert(root.into(), serde_json::json!({"name":"leaf","args":null}));
        let mut active = base.clone();
        active.revision = 1;
        active.data.insert(format!("graph:{generation}:{cell}"), value(2));
        active.data.insert(format!("graph:{generation}:{root}"), serde_json::json!({"name":"leaf","args":null}));
        active.data.insert("reactive:active".into(), serde_json::json!(generation));
        let activation = delta(&base, &active).unwrap();
        assert!(activation.data.get_raw_shared(cell).is_none(), "unchanged legacy cells must not become copies of the active graph");
        assert!(activation.data.get_raw_shared(root).is_none());
        for (key, value) in &activation.data {
            base.data.insert(key.clone(), value.clone());
        }
        base.revision = activation.revision;
        assert_eq!(snapshot_manifest(&base).unwrap().0, activation.final_digest);

        let mut cleaned = active.clone();
        cleaned.revision = 2;
        cleaned.data.remove(cell);
        cleaned.data.remove(root);
        let cleanup = delta(&active, &cleaned).unwrap();
        assert_eq!(cleanup.deletes, vec![cell.to_owned(), root.to_owned()]);
        for key in cleanup.deletes {
            active.data.remove(&key);
        }
        active.revision = cleanup.revision;
        assert_eq!(snapshot_manifest(&active).unwrap().0, cleanup.final_digest);
        assert_eq!(active.data.get(cell), Some(&value(2)));
    }

    struct Held {
        used: Arc<AtomicUsize>,
        bytes: usize,
    }
    impl Drop for Held {
        fn drop(&mut self) {
            self.used.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }
    fn state() -> PartitionState {
        let mut snapshot = Snapshot::default();
        snapshot
            .data
            .insert("record".into(), serde_json::json!("a".repeat(4096)));
        let base = snapshot.clone();
        snapshot.revision = 1;
        snapshot
            .data
            .insert("record".into(), serde_json::json!("b".repeat(4096)));
        PartitionState {
            info: PartitionInfo {
                partition: "tenant".into(),
                epoch: 1,
                phase: PartitionPhase::Frozen,
                operation: "move".into(),
                transfer: None,
                revision: 1,
                digest: None,
                bytes: 0,
                received_bytes: 0,
                next_chunk: 0,
                base_bytes: 0,
            },
            snapshot,
            base: Some(base),
            last_import: None,
            chunks: Records::default(),
        }
    }
    #[test]
    fn export_holds_output_reservation_and_releases_workspace_on_errors() {
        for kind in [ExportKind::Base, ExportKind::Snapshot, ExportKind::Delta] {
            let used = Arc::new(AtomicUsize::new(0));
            let reserve = |bytes| {
                used.fetch_add(bytes, Ordering::SeqCst);
                Ok(Held {
                    used: used.clone(),
                    bytes,
                })
            };
            let expected = export_payload(state(), kind).unwrap().2;
            let result = reserved_export(state(), kind, reserve).unwrap();
            assert_eq!(result.bytes, expected);
            assert_eq!(
                used.load(Ordering::SeqCst),
                result.bytes.len() + 512,
                "only the output should remain charged after encoding"
            );
            drop(result);
            assert_eq!(used.load(Ordering::SeqCst), 0);
        }
        let used = Arc::new(AtomicUsize::new(0));
        let calls = AtomicUsize::new(0);
        let result = reserved_export(state(), ExportKind::Delta, |bytes| {
            if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                anyhow::bail!("output budget exhausted");
            }
            used.fetch_add(bytes, Ordering::SeqCst);
            Ok(Held {
                used: used.clone(),
                bytes,
            })
        });
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            used.load(Ordering::SeqCst),
            0,
            "failed output reservation must release the difference workspace"
        );
    }
}
