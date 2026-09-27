//! Durable redb application checkpoints. A checkpoint stores metadata only;
//! transfer images are transient and encoded lazily.
use super::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::Path;

pub(super) const CHECKPOINT_KEY: &str = "snapshot_checkpoint_v1";
// This is an I/O buffer size, not a limit on snapshot or application size.
const CHUNK_BYTES: usize = 256 * 1024;

pub(super) fn allocate_snapshot_meta(
    transaction: &WriteTransaction,
    tables: Tables,
    node: u64,
    state: &StoredState,
) -> anyhow::Result<SnapshotMeta<u64, BasicNode>> {
    let mut table = transaction.open_table(tables.meta)?;
    let sequence = table
        .get("snapshot_sequence")?
        .map(|v| serde_json::from_slice::<u64>(v.value()))
        .transpose()?
        .unwrap_or(0)
        .checked_add(1)
        .context("snapshot sequence exhausted")?;
    table.insert(
        "snapshot_sequence",
        serde_json::to_vec(&sequence)?.as_slice(),
    )?;
    Ok(SnapshotMeta {
        last_log_id: state.last_applied,
        last_membership: state.membership.clone(),
        snapshot_id: format!(
            "{node}-{}-{sequence}",
            state
                .last_applied
                .map(|id| id.to_string())
                .unwrap_or_else(|| "empty".into())
        ),
    })
}

pub(super) fn write_checkpoint(
    transaction: &WriteTransaction,
    tables: Tables,
    meta: &SnapshotMeta<u64, BasicNode>,
    profile: &mut StorageTrace,
) -> anyhow::Result<()> {
    // The existing application tables are the recovery source, not this marker.
    let mut table = transaction.open_table(tables.meta)?;
    table.insert(CHECKPOINT_KEY, profile.encode(meta)?.as_slice())?;
    Ok(())
}

/// The checkpoint to advertise for the recovered state; the caller pairs it
/// with that state once the state can be served.
pub(super) fn recover_checkpoint(
    transaction: &WriteTransaction,
    tables: Tables,
    node: u64,
    state: &StoredState,
) -> anyhow::Result<Option<SnapshotMeta<u64, BasicNode>>> {
    let (checkpoint, purged) = {
        let table = transaction.open_table(tables.meta)?;
        let checkpoint = table
            .get(CHECKPOINT_KEY)?
            .map(|v| serde_json::from_slice::<SnapshotMeta<u64, BasicNode>>(v.value()))
            .transpose()?;
        let purged = table
            .get("last_purged")?
            .map(|v| serde_json::from_slice::<LogId<u64>>(v.value()))
            .transpose()?;
        (checkpoint, purged)
    };
    let Some(mut meta) = checkpoint else {
        return Ok(None);
    };
    for floor in [meta.last_log_id, purged] {
        let covered = match (state.last_applied, floor) {
            (_, None) => true,
            (Some(applied), Some(floor)) => applied.index > floor.index || applied == floor,
            (None, Some(_)) => false,
        };
        anyhow::ensure!(
            covered,
            "durable application state is behind its checkpoint or purged Raft logs"
        );
    }
    if state.last_applied == meta.last_log_id {
        anyhow::ensure!(
            state.membership == meta.last_membership,
            "checkpoint membership disagrees with durable state"
        );
    } else {
        // Later durable applies can survive a restart. Advertise exactly that
        // recovered state, never old metadata paired with a newer payload.
        meta = allocate_snapshot_meta(transaction, tables, node, state)?;
        transaction
            .open_table(tables.meta)?
            .insert(CHECKPOINT_KEY, serde_json::to_vec(&meta)?.as_slice())?;
    }
    Ok(Some(meta))
}

pub(super) fn temporary(directory: &Path) -> anyhow::Result<File> {
    tempfile::tempfile_in(directory).context("create temporary snapshot in database directory")
}

pub(super) fn encode_state(
    directory: &Path,
    state: &StoredState,
    profile: &mut StorageTrace,
) -> anyhow::Result<File> {
    let started = Instant::now();
    let file = temporary(directory)?;
    let mut writer = BufWriter::with_capacity(CHUNK_BYTES, file);
    serde_json::to_writer(&mut writer, state).context("encode snapshot state")?;
    writer.flush()?;
    let mut file = writer.into_inner()?;
    if let Some(timing) = &mut profile.0 {
        timing.encode_ns += started.elapsed().as_nanos() as u64;
        timing.encoded_bytes += usize::try_from(file.metadata()?.len()).unwrap_or(usize::MAX);
    }
    file.rewind()?;
    Ok(file)
}

/// Decode a received snapshot, its records spooled to a file in `directory`
/// and served from there rather than held in memory.
pub(super) fn decode_state(file: &mut File, directory: &Path) -> anyhow::Result<StoredState> {
    file.rewind()?;
    let spooling = crate::consensus::backing::Spool::activate(directory)
        .context("create snapshot spool in database directory")?;
    let state = serde_json::from_reader(BufReader::with_capacity(CHUNK_BYTES, &mut *file))
        .context("decode received snapshot")?;
    drop(spooling);
    file.rewind()?;
    Ok(state)
}

pub(super) fn read_snapshot_metadata(
    db: &Database,
    tables: Tables,
) -> anyhow::Result<Option<SnapshotMeta<u64, BasicNode>>> {
    let transaction = db.begin_read()?;
    let table = transaction.open_table(tables.meta)?;
    table
        .get(CHECKPOINT_KEY)?
        .map(|value| {
            serde_json::from_slice(value.value()).context("decode application checkpoint metadata")
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn received_images_install_and_truncated_or_trailing_streams_fail() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        let large = serde_json::json!("🌸".repeat(CHUNK_BYTES));
        store
            .apply([Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                payload: openraft::EntryPayload::Normal(
                    Commit {
                        internal: false,
                        request_id: "large".into(),
                        fingerprint: "large".into(),
                        expected_revision: 0,
                        puts: BTreeMap::from([("large".into(), large)]),
                        deletes: Vec::new(),
                        result: Value::Null,
                    }
                    .into(),
                ),
            }])
            .await
            .unwrap();
        let expected = store.snapshot().await;
        let mut built = store.build_snapshot().await.unwrap();
        let mut wire = Vec::new();
        built.snapshot.read_to_end(&mut wire).await.unwrap();
        assert!(wire.len() > CHUNK_BYTES * 3);
        let follower_dir = tempfile::tempdir().unwrap();
        let mut follower = Store::open(2, follower_dir.path().into()).await.unwrap();
        let mut receiving = follower.begin_receiving_snapshot().await.unwrap();
        // Exercise the same write/read transport contract OpenRaft uses.
        for chunk in wire.chunks(17_113) {
            receiving.write_all(chunk).await.unwrap();
        }
        receiving.flush().await.unwrap();
        follower
            .install_snapshot(&built.meta, receiving)
            .await
            .unwrap();
        assert_eq!(follower.snapshot().await, expected);
        // The checkpoint holds metadata only, regardless of image size.
        {
            let transaction = follower.inner.shared.database().begin_read().unwrap();
            let table = transaction.open_table(tables().meta).unwrap();
            assert!(table.get(CHECKPOINT_KEY).unwrap().unwrap().value().len() < 1024);
        }
        // A truncated or trailing incoming stream cannot replace the state.
        for invalid in [
            &wire[..wire.len() - 1],
            &[wire.as_slice(), b"junk"].concat(),
        ] {
            let mut receiving = follower.begin_receiving_snapshot().await.unwrap();
            receiving.write_all(invalid).await.unwrap();
            receiving.flush().await.unwrap();
            assert!(
                follower
                    .install_snapshot(&built.meta, receiving)
                    .await
                    .is_err()
            );
            assert_eq!(follower.snapshot().await, expected);
        }
        drop(follower);
        let mut follower = Store::open(2, follower_dir.path().into()).await.unwrap();
        assert_eq!(follower.snapshot().await, expected);
        let restored = follower.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(restored.meta, built.meta);
    }

    fn tables() -> Tables {
        Tables::new("")
    }
}
