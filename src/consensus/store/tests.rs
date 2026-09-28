use super::*;
use openraft::storage::RaftLogStorageExt;
use openraft::{CommittedLeaderId, Membership};
use redb::ReadableTableMetadata;
use serde_json::json;
use std::collections::BTreeSet;
use std::time::Duration;

async fn snapshot_bytes(mut file: Box<SnapshotData>) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    file.rewind().await.unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await.unwrap();
    bytes
}

fn snapshot_file(bytes: Vec<u8>) -> Box<SnapshotData> {
    use std::io::{Seek, Write};
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&bytes).unwrap();
    file.rewind().unwrap();
    Box::new(SnapshotData::from_std(file))
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(3, 1), index)
}

fn command(id: &str, revision: u64, puts: &[(&str, Value)], deletes: &[&str]) -> Commit {
    Commit {
        internal: false,
        request_id: id.into(),
        fingerprint: format!("fingerprint-{id}"),
        expected_revision: revision,
        puts: puts
            .iter()
            .map(|(key, value)| ((*key).into(), value.clone()))
            .collect(),
        deletes: deletes.iter().map(|key| (*key).into()).collect(),
        result: json!({"request": id}),
    }
}

fn entry(index: u64, command: Commit) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(command.into()),
    }
}

#[tokio::test]
async fn pending_append_is_readable_before_flush_but_never_acknowledged_or_applied() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .blocking_append([entry(1, command("first", 0, &[], &[]))])
        .await
        .unwrap();
    // Hold the real database writer so the append cannot reach its commit.
    // MVCC readers and the pending suffix must remain available to replication.
    let blocker = store.inner.shared.database().begin_write().unwrap();
    let mut appender = store.clone();
    let entries = vec![
        entry(2, command("second", 1, &[], &[])),
        entry(3, command("third", 2, &[], &[])),
    ];
    let mut append = Box::pin(appender.blocking_append(entries.clone()));
    assert!(futures_util::poll!(&mut append).is_pending());
    let read = tokio::time::timeout(Duration::from_secs(2), store.try_get_log_entries(1..=3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read.iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        serde_json::to_value(&read[1..]).unwrap(),
        serde_json::to_value(&entries).unwrap()
    );
    let read = store
        .try_get_log_entries((std::ops::Bound::Excluded(1), std::ops::Bound::Excluded(3)))
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].log_id.index, 2);
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(3))
    );
    assert_eq!(
        store
            .inner
            .shared
            .database()
            .begin_read()
            .unwrap()
            .open_table(LOGS)
            .unwrap()
            .len()
            .unwrap(),
        1
    );
    assert_eq!(
        store.snapshot().await.revision,
        0,
        "replication visibility is not application visibility"
    );
    assert!(
        futures_util::poll!(&mut append).is_pending(),
        "no durable callback before flush"
    );
    drop(blocker);
    append.await.unwrap();
    assert!(store.inner.appending.read().unwrap().is_empty());
    drop(appender);
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.try_get_log_entries(..).await.unwrap().len(), 3);
}

#[tokio::test]
async fn cancelled_append_still_flushes_before_truncation_and_releases_its_buffer() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .blocking_append([entry(1, command("first", 0, &[], &[]))])
        .await
        .unwrap();
    let blocker = store.inner.shared.database().begin_write().unwrap();
    let mut appender = store.clone();
    let mut append = Box::pin(appender.blocking_append([entry(2, command("second", 1, &[], &[]))]));
    assert!(futures_util::poll!(&mut append).is_pending());
    drop(append);
    let mut truncator = store.clone();
    let mut truncate = Box::pin(truncator.truncate(log_id(2)));
    assert!(
        futures_util::poll!(&mut truncate).is_pending(),
        "truncation cannot overtake pending append"
    );
    assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 2);
    drop(blocker);
    truncate.await.unwrap();
    assert!(store.inner.appending.read().unwrap().is_empty());
    assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 1);
    drop(appender);
    drop(truncator);
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.try_get_log_entries(..).await.unwrap().len(), 1);
}

#[tokio::test]
async fn successive_appends_share_one_buffer_and_readers_never_lose_the_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let blocker = store.inner.shared.database().begin_write().unwrap();
    let first = store
        .start_append(
            vec![entry(1, command("first", 0, &[], &[]))],
            Durability::Immediate,
        )
        .await;
    let next = store.clone();
    let mut second = Box::pin(next.start_append(
        vec![entry(2, command("second", 1, &[], &[]))],
        Durability::Immediate,
    ));
    assert!(futures_util::poll!(&mut second).is_pending());
    assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 1);
    drop(blocker);
    let reader = async {
        for _ in 0..100 {
            let read = store.try_get_log_entries(..).await.unwrap();
            assert!(!read.is_empty());
            assert_eq!(read[0].log_id.index, 1);
            for pair in read.windows(2) {
                assert_eq!(pair[1].log_id.index, pair[0].log_id.index + 1);
            }
        }
    };
    let writer = async {
        first.await.unwrap().unwrap();
        second.await.await.unwrap().unwrap();
    };
    tokio::join!(reader, writer);
    assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 2);
}

fn legacy_state() -> StoredState {
    let membership = Membership::new(
        vec![BTreeSet::from([1, 2])],
        BTreeMap::from([
            (1, BasicNode::new("127.0.0.1:7101")),
            (2, BasicNode::new("127.0.0.1:7102")),
        ]),
    );
    StoredState {
        partitions: Partitions::default(),
        last_applied: Some(log_id(9)),
        membership: StoredMembership::new(Some(log_id(2)), membership),
        application: Snapshot {
            revision: 2,
            data: super::super::Records::from([
                ("keep".into(), json!({"nested": [1, 2, "hello"]})),
                ("remove".into(), json!(23)),
                ("東京/🌸".into(), Value::Null),
            ]),
            requests: Receipts::from([
                (
                    "old-a".into(),
                    Receipt {
                        fingerprint: "fingerprint-old-a".into(),
                        revision: 1,
                        result: json!("original result"),
                        epoch: None,
                    },
                ),
                (
                    "old-b".into(),
                    Receipt {
                        fingerprint: "fingerprint-old-b".into(),
                        revision: 2,
                        result: Value::Null,
                        epoch: None,
                    },
                ),
            ]),
        },
    }
}

fn has_checkpoint(store: &Store) -> bool {
    read_meta::<SnapshotMeta<u64, BasicNode>>(
        store.inner.shared.database(),
        Tables::new(""),
        CHECKPOINT_KEY,
    )
    .unwrap()
    .is_some()
}

#[tokio::test]
async fn malformed_stored_snapshot_is_an_error_and_never_an_absent_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store.build_snapshot().await.unwrap();
    let original = {
        let transaction = store.inner.shared.database().begin_read().unwrap();
        let table = transaction.open_table(META).unwrap();
        table.get(CHECKPOINT_KEY).unwrap().unwrap().value().to_vec()
    };
    let mut corrupt = original.clone();
    corrupt.pop();
    {
        let mut transaction = store.inner.shared.database().begin_write().unwrap();
        transaction.set_durability(Durability::Immediate).unwrap();
        transaction
            .open_table(META)
            .unwrap()
            .insert(CHECKPOINT_KEY, corrupt.as_slice())
            .unwrap();
        transaction.commit().unwrap();
    }
    assert!(read_snapshot_metadata(store.inner.shared.database(), Tables::new("")).is_err());
    store.close().await.unwrap();
    drop(store);
    // Recovery now reads the snapshot position to reconstruct byte accounting.
    // Invalid metadata must fail startup, never masquerade as no snapshot.
    assert!(Store::open(1, directory.path().into()).await.is_err());
}

#[tokio::test]
async fn stored_version_bound_covers_every_version_the_batch_stored() {
    // A restart hands out versions above the stored bound, so the bound must
    // exceed each version stored with it, including those of a block a
    // thread began while versioning the batch (more writes than a block
    // holds make sure one begins).
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let keys = (0..1100)
        .map(|index| format!("key-{index}"))
        .collect::<Vec<_>>();
    let puts = keys
        .iter()
        .map(|key| (key.as_str(), json!(1)))
        .collect::<Vec<_>>();
    store
        .apply([entry(1, command("many", 0, &puts, &[]))])
        .await
        .unwrap();
    store.inner.persistence.drain().await.unwrap();
    let metadata =
        read_meta::<StateMetadata>(store.inner.shared.database(), Tables::new(""), STATE_META)
            .unwrap()
            .unwrap();
    let guard = store.inner.state.read().await;
    let highest = keys
        .iter()
        .map(|key| guard.application.data.version(key).unwrap())
        .chain(guard.application.requests.version("many"))
        .max()
        .unwrap();
    assert!(
        highest < metadata.versions,
        "version {highest} stored above the bound {}",
        metadata.versions
    );
}

#[tokio::test]
async fn a_reopened_store_versions_well_above_its_stored_bound() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, command("one", 0, &[("key", json!(1))], &[]))])
        .await
        .unwrap();
    store.close().await.unwrap();
    drop(store);
    let stored = {
        let db = Database::open(directory.path().join("flower.redb")).unwrap();
        read_meta::<StateMetadata>(&db, Tables::new(""), STATE_META)
            .unwrap()
            .unwrap()
            .versions
    };
    let _reopened = Store::open(1, directory.path().into()).await.unwrap();
    // A fresh thread takes a fresh block, from the shared counter.
    let next = std::thread::spawn(crate::consensus::next_version)
        .join()
        .unwrap();
    assert!(next > stored + RESTART_MARGIN, "{next} ≤ {stored} + margin");
}

#[tokio::test]
async fn grouped_deltas_preserve_order_dedup_cas_and_untouched_allocations() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(
            1,
            command(
                "original",
                0,
                &[
                    ("untouched", json!("a long-lived value")),
                    ("gone", json!(1)),
                ],
                &[],
            ),
        )])
        .await
        .unwrap();
    // Versions, not allocations: a record keeps its version when it moves
    // from memory to disk, which persistence may do at any moment.
    let (value_version, receipt_version) = {
        let guard = store.inner.state.read().await;
        (
            guard.application.data.version("untouched").unwrap(),
            guard.application.requests.version("original").unwrap(),
        )
    };
    let retained = store.snapshot_for_writer().await;
    assert!(retained.data.ptr_eq(&store.snapshot_for(None).await.data));
    assert!(
        retained
            .requests
            .ptr_eq(&store.snapshot_for_writer().await.requests)
    );
    let mut internal = command("original", 2, &[("changing", json!(2))], &[]);
    internal.internal = true;
    let batch = CompactBatch::new(vec![
        command("a", 1, &[("changing", json!(1))], &["gone"]),
        internal,
        command("b", 3, &[("changing", json!(3))], &["changing"]),
        command("c", 4, &[], &["changing"]),
    ])
    .unwrap();
    let results = store
        .apply([
            Entry {
                log_id: log_id(2),
                payload: EntryPayload::Normal(RaftCommand::Batch { batch }),
            },
            Entry {
                log_id: log_id(3),
                payload: EntryPayload::Blank,
            },
        ])
        .await
        .unwrap();
    let ApplyResult::Batch(results) = &results[0] else {
        panic!("expected batch")
    };
    assert_eq!(results.len(), 4);
    for (index, result) in results.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if result.revision == index as u64 + 2 && !result.duplicate));
    }
    store.inner.persistence.drain().await.unwrap();
    {
        let guard = store.inner.state.read().await;
        assert_eq!(
            guard.application.data.version("untouched"),
            Some(value_version)
        );
        assert_eq!(
            guard.application.requests.version("original"),
            Some(receipt_version)
        );
        assert_eq!(guard.application.data.len(), 1);
        assert_eq!(guard.application.requests.len(), 4);
        let transaction = store.inner.shared.database().begin_read().unwrap();
        assert_eq!(transaction.open_table(DATA).unwrap().len().unwrap(), 1);
        assert_eq!(transaction.open_table(REQUESTS).unwrap().len().unwrap(), 4);
        let metadata =
            read_meta::<StateMetadata>(store.inner.shared.database(), Tables::new(""), STATE_META)
                .unwrap()
                .unwrap();
        assert_eq!(metadata.revision, 5);
        assert_eq!(metadata.last_applied, Some(log_id(3)));
    }
    let expected = store.snapshot().await;
    assert_eq!(retained.revision, 1);
    assert_eq!(retained.data["gone"], 1);
    assert!(!retained.data.contains_key("changing"));
    assert_eq!(
        retained.data.version("untouched"),
        expected.data.version("untouched")
    );
    assert_eq!(retained.requests.len(), 1);
    assert!(!retained.requests.contains_key("a"));
    assert_eq!(
        retained.requests.version("original"),
        expected.requests.version("original")
    );
    store.close().await.unwrap();
    drop(store);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, expected);
}

#[tokio::test]
async fn snapshot_install_replaces_all_tables_and_preserves_legacy_wire_format() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(2, directory.path().into()).await.unwrap();
    store
        .apply([entry(
            1,
            command("obsolete-receipt", 0, &[("obsolete", json!(123))], &[]),
        )])
        .await
        .unwrap();
    let old = legacy_state();
    let meta = SnapshotMeta {
        last_log_id: old.last_applied,
        last_membership: old.membership.clone(),
        snapshot_id: "old-wire-snapshot".into(),
    };
    let bytes = serde_json::to_vec(&old).unwrap();
    store
        .install_snapshot(&meta, snapshot_file(bytes.clone()))
        .await
        .unwrap();
    assert!(has_checkpoint(&store));
    assert_eq!(store.snapshot().await, old.application);
    let published = store.snapshot_for(None).await;
    assert_eq!(published.revision, old.application.revision);
    assert_eq!(published.data, old.application.data);
    assert!(published.requests.is_empty());
    let writer = store.snapshot_for_writer().await;
    assert_eq!(writer, old.application);
    assert!(!writer.requests.contains_key("obsolete-receipt"));
    let built = store.build_snapshot().await.unwrap();
    assert_eq!(snapshot_bytes(built.snapshot).await, bytes);
    assert_eq!(built.meta.last_log_id, old.last_applied);
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(2, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, old.application);
    assert_eq!(
        reopened.applied_state().await.unwrap(),
        (old.last_applied, old.membership)
    );
    assert_eq!(
        snapshot_bytes(
            reopened
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .snapshot
        )
        .await,
        bytes
    );
    reopened
        .apply([entry(
            10,
            command("after-snapshot", 2, &[("new", json!(1))], &[]),
        )])
        .await
        .unwrap();
    assert_eq!(reopened.snapshot().await.revision, 3);
}

#[tokio::test]
async fn apply_publishes_before_its_write_and_cancelled_drains_still_persist_it() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    // Hold redb's writer lock: the apply must still complete and publish.
    let shared = store.inner.shared.clone();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let blocker = tokio::task::spawn_blocking(move || {
        let transaction = shared.database().begin_write().unwrap();
        ready.send(()).unwrap();
        release_rx.recv().unwrap();
        transaction.abort().unwrap();
    });
    ready_rx.await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        store.apply([entry(
            1,
            command("published", 0, &[("durable", json!(7))], &[]),
        )]),
    )
    .await
    .expect("apply waits for no disk write")
    .unwrap();
    let expected = store.snapshot().await;
    assert_eq!(expected.revision, 1);
    assert_eq!(expected.data["durable"], json!(7));
    assert_eq!(expected.requests["published"].revision, 1);
    assert_eq!(store.snapshot_for(None).await.data, expected.data);
    assert_eq!(store.applied_state().await.unwrap().0, Some(log_id(1)));
    // The queued write waits for the disk; abandoning a drain cannot drop it.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), store.inner.persistence.drain())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    blocker.await.unwrap();
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, expected);
    assert_eq!(reopened.applied_state().await.unwrap().0, Some(log_id(1)));
}

#[tokio::test]
async fn write_error_rolls_back_changed_records_and_fails_later_applies() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, command("original", 0, &[("keep", json!(1))], &[]))])
        .await
        .unwrap();
    store.inner.persistence.drain().await.unwrap();
    let original = store.inner.state.read().await.clone();
    // A mismatched table type injects an error after DATA writes have been
    // staged, before metadata/receipts and commit. redb must roll them all back.
    const WRONG_REQUESTS: TableDefinition<&str, &str> =
        TableDefinition::new("application_requests_v4");
    {
        let transaction = store.inner.shared.database().begin_write().unwrap();
        transaction.delete_table(REQUESTS).unwrap();
        drop(transaction.open_table(WRONG_REQUESTS).unwrap());
        transaction.commit().unwrap();
    }
    let update = entry(2, command("retry", 1, &[("bad", json!(2))], &["keep"]));
    // The log already made the entry durable: its state publishes, and the
    // failed projection stops this store before any later delta is written.
    store.apply([update.clone()]).await.unwrap();
    assert_eq!(store.snapshot().await.revision, 2);
    assert!(store.inner.persistence.drain().await.is_err());
    let later = entry(3, command("later", 2, &[("later", json!(3))], &[]));
    assert!(store.apply([later]).await.is_err());
    {
        let transaction = store.inner.shared.database().begin_read().unwrap();
        let table = transaction.open_table(DATA).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        assert!(table.get(&b"keep"[..]).unwrap().is_some());
        assert!(table.get(&b"bad"[..]).unwrap().is_none());
        let metadata =
            read_meta::<StateMetadata>(store.inner.shared.database(), Tables::new(""), STATE_META)
                .unwrap()
                .unwrap();
        assert_eq!(metadata.revision, 1);
        assert_eq!(metadata.last_applied, original.last_applied);
    }
    {
        let transaction = store.inner.shared.database().begin_write().unwrap();
        transaction.delete_table(WRONG_REQUESTS).unwrap();
        replace_application(&transaction, Tables::new(""), &original).unwrap();
        transaction.commit().unwrap();
    }
    assert!(store.close().await.is_err());
    drop(store);
    // Recovery restarts from the last written state and replays the log.
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, original.application);
    assert_eq!(
        reopened.applied_state().await.unwrap().0,
        original.last_applied
    );
    reopened.apply([update]).await.unwrap();
    let expected = reopened.snapshot().await;
    assert_eq!(expected.revision, 2);
    reopened.close().await.unwrap();
    drop(reopened);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, expected);
}

#[tokio::test]
async fn incomplete_v2_layout_is_rejected_without_resetting_application_state() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(1, directory.path().into()).await.unwrap();
    {
        let transaction = store.inner.shared.database().begin_write().unwrap();
        transaction.delete_table(DATA).unwrap();
        transaction.commit().unwrap();
    }
    store.close().await.unwrap();
    drop(store);
    let result = Store::open(1, directory.path().into()).await;
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("incomplete application storage tables")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_reads_progress_under_write_gate_and_see_only_committed_mvcc_state() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let initial = entry(1, command("initial", 0, &[], &[]));
    store.blocking_append([initial.clone()]).await.unwrap();
    store.apply([initial]).await.unwrap();
    store.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
    let old_snapshot = store.build_snapshot().await.unwrap();
    // Hold the very same owned I/O gate used through production flush, plus a
    // live redb writer with staged log/metadata changes. MVCC readers must not
    // await the gate or expose those changes before this transaction commits.
    let guard = store.inner.io.clone().lock_owned().await;
    let shared = store.inner.shared.clone();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let writer = tokio::task::spawn_blocking(move || {
        let mut transaction = shared.database().begin_write().unwrap();
        transaction.set_durability(Durability::Immediate).unwrap();
        {
            let mut logs = transaction.open_table(LOGS).unwrap();
            logs.insert(
                2,
                serde_json::to_vec(&entry(2, command("staged", 1, &[], &[])))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let mut meta = transaction.open_table(META).unwrap();
            meta.insert(
                "vote",
                serde_json::to_vec(&Vote::new_committed(4, 1))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            meta.insert(
                "committed",
                serde_json::to_vec(&Some(log_id(2))).unwrap().as_slice(),
            )
            .unwrap();
        }
        ready.send(()).unwrap();
        release_rx.recv().unwrap();
        transaction.commit().unwrap();
        drop(guard);
    });
    ready_rx.await.unwrap();
    let before = tokio::time::timeout(Duration::from_secs(2), async {
        let logs = store.try_get_log_entries(1..3).await.unwrap();
        let log_state = store.get_log_state().await.unwrap();
        let vote = store.read_vote().await.unwrap();
        let committed = store.read_committed().await.unwrap();
        let snapshot = store.get_current_snapshot().await.unwrap().unwrap();
        (logs, log_state, vote, committed, snapshot)
    })
    .await;
    // Always release the blocking task, including when a regression times out.
    release.send(()).unwrap();
    writer.await.unwrap();
    let (logs, log_state, vote, committed, snapshot) =
        before.expect("MVCC reads must not wait for the storage write gate");
    assert_eq!(logs.len(), 1);
    assert_eq!(log_state.last_log_id, Some(log_id(1)));
    assert_eq!(vote, Some(Vote::new_committed(3, 1)));
    assert_eq!(committed, None);
    assert_eq!(snapshot.meta, old_snapshot.meta);
    assert_eq!(
        snapshot_bytes(snapshot.snapshot).await,
        snapshot_bytes(old_snapshot.snapshot).await
    );
    assert_eq!(store.try_get_log_entries(1..3).await.unwrap().len(), 2);
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2))
    );
    assert_eq!(
        store.read_vote().await.unwrap(),
        Some(Vote::new_committed(4, 1))
    );
    assert_eq!(store.read_committed().await.unwrap(), Some(log_id(2)));
    assert_eq!(
        store
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta
            .snapshot_id,
        old_snapshot.meta.snapshot_id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_log_reads_remain_coherent_through_append_and_truncate() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    fn generation(index: u64, value: u64) -> Entry<TypeConfig> {
        entry(
            index,
            command(
                &format!("generation-{value}"),
                0,
                &[("generation", json!(value))],
                &[],
            ),
        )
    }
    store
        .blocking_append((1..=8).map(|index| generation(index, 0)))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..2 {
        let mut reader = store.clone();
        let barrier = barrier.clone();
        let finished = finished.clone();
        readers.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut rounds = 0;
            while !finished.load(std::sync::atomic::Ordering::Acquire) || rounds < 10 {
                let entries = reader.try_get_log_entries(1..9).await.unwrap();
                assert!(
                    matches!(entries.len(), 1 | 8),
                    "read saw only part of an atomic append/truncate"
                );
                let mut generation = None;
                for (offset, entry) in entries.iter().enumerate() {
                    assert_eq!(
                        entry.log_id.index,
                        offset as u64 + 1,
                        "no holes in a committed log view"
                    );
                    if offset == 0 {
                        continue;
                    }
                    let EntryPayload::Normal(RaftCommand::Single(command)) = &entry.payload else {
                        panic!("expected generation command")
                    };
                    let value = command.puts["generation"].as_u64().unwrap();
                    assert_eq!(
                        *generation.get_or_insert(value),
                        value,
                        "one read must not mix generations"
                    );
                }
                let last = reader
                    .get_log_state()
                    .await
                    .unwrap()
                    .last_log_id
                    .unwrap()
                    .index;
                assert!(matches!(last, 1 | 8));
                rounds += 1;
                tokio::task::yield_now().await;
            }
            rounds
        }));
    }
    barrier.wait().await;
    for value in 1..=8 {
        store.truncate(log_id(2)).await.unwrap();
        store
            .blocking_append((2..=8).map(|index| generation(index, value)))
            .await
            .unwrap();
        // blocking_append includes the flush callback: reads after it returns
        // must observe every entry, and reopening below verifies persistence.
        assert_eq!(store.try_get_log_entries(1..9).await.unwrap().len(), 8);
    }
    finished.store(true, std::sync::atomic::Ordering::Release);
    for reader in readers {
        assert!(reader.await.unwrap() >= 10);
    }
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    let entries = reopened.try_get_log_entries(1..9).await.unwrap();
    assert_eq!(entries.len(), 8);
    let EntryPayload::Normal(RaftCommand::Single(command)) = &entries[7].payload else {
        panic!("expected persisted command")
    };
    assert_eq!(command.puts["generation"], json!(8));
}

#[derive(Debug)]
struct PausedSync {
    ready: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[derive(Debug)]
struct PausingBackend {
    file: redb::backends::FileBackend,
    pause: Arc<std::sync::Mutex<Option<PausedSync>>>,
}

impl redb::StorageBackend for PausingBackend {
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }
    fn read(&self, offset: u64, output: &mut [u8]) -> std::io::Result<()> {
        self.file.read(offset, output)
    }
    fn set_len(&self, length: u64) -> std::io::Result<()> {
        self.file.set_len(length)
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.file.write(offset, data)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        let pause = self.pause.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.ready.send(());
            pause.release.recv().map_err(std::io::Error::other)?;
        }
        self.file.sync_data()
    }
}

fn open_pausing_store(
    directory: &std::path::Path,
) -> (Store, Arc<std::sync::Mutex<Option<PausedSync>>>) {
    let pause = Arc::new(std::sync::Mutex::new(None));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("flower.redb"))
        .unwrap();
    let db = Database::builder()
        .create_with_backend(PausingBackend {
            file: redb::backends::FileBackend::new(file).unwrap(),
            pause: pause.clone(),
        })
        .unwrap();
    (store_with_database(directory, db), pause)
}

fn store_with_database(directory: &std::path::Path, db: Database) -> Store {
    let transaction = db.begin_write().unwrap();
    transaction.open_table(LOGS).unwrap();
    let mut state = load_application(&transaction, Tables::new("")).unwrap();
    transaction.open_table(DATA).unwrap();
    transaction.open_table(REQUESTS).unwrap();
    let checkpoint = recover_checkpoint(&transaction, Tables::new(""), 1, &state).unwrap();
    transaction.commit().unwrap();
    state.application =
        stored_application(&db, Tables::new(""), state.application.revision).unwrap();
    let current_snapshot = checkpoint;
    Store {
        inner: Arc::new(Inner {
            appending: Arc::new(PublishedLock::new(Vec::new())),
            pending: Default::default(),
            id: 1,
            shared: SharedDatabase::with_database(db),
            tables: Tables::new(""),
            snapshot_directory: directory.to_path_buf(),
            io: Arc::new(Mutex::new(())),
            snapshot_installation: AtomicU64::new(0),
            current_snapshot: PublishedLock::new(current_snapshot),
            snapshot_accounting: super::super::snapshot_policy::Accounting::new(0, None, None),
            published: Arc::new(PublishedLock::new(Published::from(&state))),
            state: Arc::new(RwLock::new(state)),
            lazy: Default::default(),
            persistence: Default::default(),
            backing_persisted: Default::default(),
            rebase_rounds: Default::default(),
            replaced_partitions: Default::default(),
            holders: Default::default(),
            changes: super::super::changes::sender(),
        }),
        raft_lifetime: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_publishes_atomically_without_a_second_disk_sync() {
    let directory = tempfile::tempdir().unwrap();
    drop(Store::open(1, directory.path().into()).await.unwrap());
    let (mut store, pause) = open_pausing_store(directory.path());
    let (ready, mut ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *pause.lock().unwrap() = Some(PausedSync {
        ready,
        release: release_rx,
    });
    let mut applying = store.clone();
    let apply = tokio::spawn(async move {
        applying
            .apply([entry(1, command("one", 0, &[("value", json!(1))], &[]))])
            .await
    });
    let completed = tokio::time::timeout(Duration::from_secs(2), async {
        while !apply.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let unused = pause.lock().unwrap().take().is_some();
    // Always unblock a regressed implementation before asserting.
    let _ = release.send(());
    apply.await.unwrap().unwrap();
    assert!(
        completed && unused,
        "apply attempted a second fsync after the durable Raft append"
    );
    assert!(ready_rx.try_recv().is_err());
    assert_eq!(store.applied_state().await.unwrap().0, Some(log_id(1)));
    assert_eq!(store.snapshot().await.requests["one"].revision, 1);
}

#[tokio::test]
async fn retired_graph_metadata_cannot_fall_back_to_stale_supported_layout() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(1, directory.path().into()).await.unwrap();
    {
        let transaction = store.inner.shared.database().begin_write().unwrap();
        let mut meta = transaction.open_table(META).unwrap();
        let metadata = meta.get(STATE_META).unwrap().unwrap().value().to_vec();
        meta.remove(STATE_META).unwrap();
        // A retirement marker is authoritative even if an older readable
        // checkpoint survives. Falling back would silently resurrect stale data.
        meta.insert(LEGACY_STATE_META, metadata.as_slice()).unwrap();
        drop(meta);
        transaction.commit().unwrap();
    }
    store.close().await.unwrap();
    drop(store);
    assert!(Store::open(1, directory.path().into()).await.is_err());
}

#[tokio::test]
async fn raft_storage_drain_waits_for_clones_and_cancelled_blocking_io_only() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(1, directory.path().into()).await.unwrap();
    let (tracked, mut drained) = store.raft_storage();
    let other_worker = tracked.clone();
    let ordinary_router = store.clone();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        tracked
            .read_disk(move |_| {
                ready.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    ready_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(other_worker);
    assert!(
        matches!(drained.has_changed(), Ok(false)),
        "cancelled I/O still owns the database"
    );
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), drained.changed())
            .await
            .unwrap()
            .is_err(),
        "drain closes after the final Raft worker, even with ordinary router handles alive"
    );
    drop(ordinary_router);
    store.close().await.unwrap();
    drop(store);
    Store::open(1, directory.path().into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_reads_progress_during_fsync_but_append_ack_waits_for_durability() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .blocking_append([entry(1, command("initial", 0, &[], &[]))])
        .await
        .unwrap();
    store.close().await.unwrap();
    drop(store);
    let (mut store, pause) = open_pausing_store(directory.path());
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *pause.lock().unwrap() = Some(PausedSync {
        ready,
        release: release_rx,
    });
    let mut writing = store.clone();
    let writer = tokio::spawn(async move {
        writing
            .blocking_append([entry(2, command("durable", 0, &[], &[]))])
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .unwrap()
        .unwrap();
    // Queued in its batch: later writes may queue behind it at once, while it
    // stays pending, readable and unacknowledged until the flush completes.
    assert!(store.inner.io.try_lock().is_ok());
    assert_eq!(store.inner.appending.read().unwrap().len(), 1);
    let before =
        tokio::time::timeout(Duration::from_secs(2), store.try_get_log_entries(1..3)).await;
    let acknowledged_before_flush = writer.is_finished();
    // Unblock before assertions so a regression cannot strand the blocking pool.
    release.send(()).unwrap();
    writer.await.unwrap().unwrap();
    assert!(
        !acknowledged_before_flush,
        "append acknowledged before its durable flush"
    );
    let before = before
        .expect("replication read waited behind a paused fsync")
        .unwrap();
    assert_eq!(
        before.len(),
        2,
        "replication must see pending logs without treating them as durable"
    );
    assert_eq!(store.try_get_log_entries(1..3).await.unwrap().len(), 2);
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.try_get_log_entries(1..3).await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asynchronous_append_reports_flush_failure_to_raft() {
    let directory = tempfile::tempdir().unwrap();
    drop(Store::open(1, directory.path().into()).await.unwrap());
    let (store, pause) = open_pausing_store(directory.path());
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *pause.lock().unwrap() = Some(PausedSync {
        ready,
        release: release_rx,
    });
    let mut writing = store.clone();
    let writer = tokio::spawn(async move {
        writing
            .blocking_append([entry(1, command("unacknowledged", 0, &[], &[]))])
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(!writer.is_finished());
    // Disconnecting the backend gate makes the actual sync operation fail.
    drop(release);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), writer)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(store.inner.appending.read().unwrap().is_empty());
    assert_eq!(store.snapshot().await.revision, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_readers_progress_during_apply_without_exposing_staged_records_or_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, command("initial", 0, &[("value", json!(1))], &[]))])
        .await
        .unwrap();
    store.close().await.unwrap();
    drop(store);
    let store = Store::open(1, directory.path().into()).await.unwrap();
    let initial = store.snapshot_for(None).await;
    let initial_writer = store.snapshot_for_writer().await;
    let initial_fence = store.read_fence().unwrap();
    let next_fence = super::super::read::ReadFence {
        applied: log_id(2),
        revision: 2,
    };
    // Hold the state lock as an apply does while it computes and publishes.
    let held = store.inner.state.clone().write_owned().await;
    let mut applying = store.clone();
    let writer = tokio::spawn(async move {
        applying
            .apply([entry(2, command("next", 1, &[("value", json!(2))], &[]))])
            .await
    });
    assert_eq!(store.read_fence().unwrap().applied, initial_fence.applied);
    assert!(store.snapshot_after_fence(&next_fence).unwrap().is_none());
    let before = tokio::time::timeout(Duration::from_secs(2), async {
        let ids = ["initial".into(), "next".into()];
        tokio::join!(
            store.snapshot_for(None),
            store.snapshot_for_writer(),
            store.snapshot_for(Some("initial")),
            store.snapshot_for_many(&ids),
        )
    })
    .await;
    let acknowledged_before_publication = writer.is_finished();
    drop(held);
    writer.await.unwrap().unwrap();
    let durable = tokio::time::timeout(Duration::from_secs(2), store.snapshot())
        .await
        .unwrap();
    assert!(!acknowledged_before_publication);
    let (before, before_writer, before_receipt, before_many) =
        before.expect("published reader waited behind application transaction");
    assert_eq!(before.revision, initial.revision);
    assert_eq!(before.data["value"], 1);
    assert!(before.data.ptr_eq(&initial.data));
    assert_eq!(before_writer, initial_writer);
    assert!(before_writer.requests.ptr_eq(&initial_writer.requests));
    assert!(!before_writer.requests.contains_key("next"));
    assert_eq!(before_receipt, initial_writer);
    assert_eq!(before_many, initial_writer);
    let published = store.snapshot_for(None).await;
    assert_eq!(store.read_fence().unwrap().applied, next_fence.applied);
    assert_eq!(
        store.snapshot_after_fence(&next_fence).unwrap().unwrap(),
        published
    );
    assert_eq!(published.revision, 2);
    assert_eq!(published.data["value"], 2);
    assert!(published.data.ptr_eq(&durable.data));
    assert!(published.requests.is_empty());
    let published_writer = store.snapshot_for_writer().await;
    assert_eq!(published_writer, durable);
    assert_eq!(published_writer.requests["next"].revision, 2);
    assert!(published_writer.requests.ptr_eq(&durable.requests));
    // The unchanged receipt was not rewritten: it keeps its write's version.
    assert_eq!(
        initial_writer.requests.version("initial"),
        published_writer.requests.version("initial")
    );
    assert_eq!(initial.data["value"], 1);
    store.close().await.unwrap();
    drop(store);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, durable);
    assert_eq!(reopened.snapshot_for(None).await, published);
    assert_eq!(reopened.snapshot_for_writer().await, published_writer);
}

#[tokio::test]
async fn cancelled_snapshot_install_publishes_data_and_receipts_together_after_fsync() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(
            1,
            command("obsolete", 0, &[("obsolete", json!(1))], &[]),
        )])
        .await
        .unwrap();
    store.close().await.unwrap();
    drop(store);
    let (store, pause) = open_pausing_store(directory.path());
    let initial = store.snapshot_for_writer().await;
    let next = legacy_state();
    let expected = next.application.clone();
    let meta = SnapshotMeta {
        last_log_id: next.last_applied,
        last_membership: next.membership,
        snapshot_id: "cancelled-install".into(),
    };
    let bytes = serde_json::to_vec(&StoredState {
        partitions: Partitions::default(),
        last_applied: meta.last_log_id,
        membership: meta.last_membership.clone(),
        application: expected.clone(),
    })
    .unwrap();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *pause.lock().unwrap() = Some(PausedSync {
        ready,
        release: release_rx,
    });
    let mut installing = store.clone();
    let writer = tokio::spawn(async move {
        installing
            .install_snapshot(&meta, snapshot_file(bytes))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(store.inner.state.try_read().is_err());
    let before = tokio::time::timeout(Duration::from_secs(2), store.snapshot_for_writer()).await;
    let acknowledged_before_flush = writer.is_finished();
    writer.abort();
    release.send(()).unwrap();
    assert!(writer.await.unwrap_err().is_cancelled());
    let durable = tokio::time::timeout(Duration::from_secs(2), store.snapshot())
        .await
        .unwrap();
    assert!(!acknowledged_before_flush);
    let before = before.expect("writer baseline waited behind snapshot fsync");
    assert_eq!(before, initial);
    assert!(before.requests.ptr_eq(&initial.requests));
    assert_eq!(durable, expected);
    assert_eq!(store.snapshot_for_writer().await, expected);
    assert!(initial.requests.contains_key("obsolete"));
    assert!(!durable.requests.contains_key("obsolete"));
    assert_eq!(durable.requests["old-b"].revision, durable.revision);
    drop(store.inner.io.lock().await);
    store.close().await.unwrap();
    drop(store);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot_for_writer().await, expected);
    assert!(has_checkpoint(&reopened));
    let restored = reopened
        .clone()
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    let state: StoredState =
        serde_json::from_slice(&snapshot_bytes(restored.snapshot).await).unwrap();
    assert_eq!(state.application, expected);
    assert_eq!(state.last_applied, restored.meta.last_log_id);
    assert_eq!(state.membership, restored.meta.last_membership);
}

#[tokio::test]
async fn detached_snapshot_capture_allows_apply_and_never_regresses_newer_builder_or_installation()
{
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, command("first", 0, &[("value", json!(1))], &[]))])
        .await
        .unwrap();
    let old = store.capture_snapshot().await;
    // Capturing the immutable root cannot retain the lock until encoding/fsync.
    tokio::time::timeout(
        Duration::from_secs(2),
        store.apply([entry(2, command("second", 1, &[("value", json!(2))], &[]))]),
    )
    .await
    .unwrap()
    .unwrap();
    let fresh = store.build_snapshot().await.unwrap();
    let stale_result = store
        .build_captured_snapshot(old, StorageTrace::default())
        .await
        .unwrap();
    assert_eq!(stale_result.meta, fresh.meta);
    let fresh_bytes = snapshot_bytes(fresh.snapshot).await;
    assert_eq!(snapshot_bytes(stale_result.snapshot).await, fresh_bytes);
    assert_eq!(store.snapshot().await.data["value"], json!(2));

    let captured_before_install = store.capture_snapshot().await;
    // Installing a snapshot at the SAME log position must invalidate captures
    // too: comparing only log indexes would let the stale builder replace it.
    let mut installed: StoredState = serde_json::from_slice(&fresh_bytes).unwrap();
    installed
        .application
        .data
        .insert("value".into(), json!(999));
    let mut meta = fresh.meta.clone();
    meta.snapshot_id = "authoritative-installed-image".into();
    let bytes = serde_json::to_vec(&installed).unwrap();
    store
        .install_snapshot(&meta, snapshot_file(bytes.clone()))
        .await
        .unwrap();
    let after = store
        .build_captured_snapshot(captured_before_install, StorageTrace::default())
        .await
        .unwrap();
    assert_eq!(after.meta, meta);
    assert_eq!(snapshot_bytes(after.snapshot).await, bytes);
    assert_eq!(
        store.get_current_snapshot().await.unwrap().unwrap().meta,
        meta
    );
    store.close().await.unwrap();
    drop(store);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await.data["value"], json!(999));
}

#[tokio::test]
async fn snapshot_byte_accounting_captures_only_published_prefix_and_recovers_from_log_lengths() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let first = entry(
        1,
        command("first", 0, &[("value", json!("a".repeat(1024)))], &[]),
    );
    store.blocking_append([first.clone()]).await.unwrap();
    store.apply([first]).await.unwrap();
    let captured = store.capture_snapshot().await;
    let second = entry(
        2,
        command("second", 1, &[("value", json!("b".repeat(2048)))], &[]),
    );
    let expected = super::super::limits::encoded_json_len(&second).unwrap();
    store.blocking_append([second.clone()]).await.unwrap();
    store.apply([second]).await.unwrap();
    store
        .build_captured_snapshot(captured, StorageTrace::default())
        .await
        .unwrap();
    let limits = super::super::Limits::default();
    assert_eq!(
        store.snapshot_accounting().metrics(&limits)["unsnapshottedLogBytes"],
        expected
    );
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(
        reopened.snapshot_accounting().metrics(&limits)["unsnapshottedLogBytes"],
        0
    );
    reopened.build_snapshot().await.unwrap();
    assert_eq!(
        reopened.snapshot_accounting().metrics(&limits)["unsnapshottedLogBytes"],
        0
    );
}

/// Models power loss by retaining exactly the backend bytes present at the last
/// successful sync. Taking that image before Drop prevents clean shutdown from
/// accidentally making a recovery test pass.
#[derive(Debug)]
struct PowerLossBackend {
    working: std::sync::Mutex<Vec<u8>>,
    durable: Arc<std::sync::Mutex<Vec<u8>>>,
    fail_sync: Arc<std::sync::atomic::AtomicBool>,
}
impl redb::StorageBackend for PowerLossBackend {
    fn len(&self) -> std::io::Result<u64> {
        Ok(self.working.lock().unwrap().len() as u64)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        let image = self.working.lock().unwrap();
        let start = usize::try_from(offset).map_err(std::io::Error::other)?;
        let bytes = image
            .get(start..start + out.len())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        out.copy_from_slice(bytes);
        Ok(())
    }
    fn set_len(&self, length: u64) -> std::io::Result<()> {
        self.working
            .lock()
            .unwrap()
            .resize(usize::try_from(length).map_err(std::io::Error::other)?, 0);
        Ok(())
    }
    fn write(&self, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        let mut image = self.working.lock().unwrap();
        let start = usize::try_from(offset).map_err(std::io::Error::other)?;
        let target = image
            .get_mut(start..start + bytes.len())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        target.copy_from_slice(bytes);
        Ok(())
    }
    fn sync_data(&self) -> std::io::Result<()> {
        if self.fail_sync.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected checkpoint sync failure"));
        }
        *self.durable.lock().unwrap() = self.working.lock().unwrap().clone();
        Ok(())
    }
}

async fn power_loss_store(
    directory: &std::path::Path,
) -> (
    Store,
    Arc<std::sync::Mutex<Vec<u8>>>,
    Arc<std::sync::atomic::AtomicBool>,
) {
    drop(Store::open(1, directory.into()).await.unwrap());
    let initial = std::fs::read(directory.join("flower.redb")).unwrap();
    let durable = Arc::new(std::sync::Mutex::new(initial.clone()));
    let fail_sync = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let db = Database::builder()
        .create_with_backend(PowerLossBackend {
            working: std::sync::Mutex::new(initial),
            durable: durable.clone(),
            fail_sync: fail_sync.clone(),
        })
        .unwrap();
    (store_with_database(directory, db), durable, fail_sync)
}

fn assert_metadata_only_snapshot(store: &Store) {
    let transaction = store.inner.shared.database().begin_read().unwrap();
    let table = transaction.open_table(META).unwrap();
    assert!(table.get("snapshot").unwrap().is_none());
    assert!(table.get(CHECKPOINT_KEY).unwrap().unwrap().value().len() < 1024);
    assert!(
        !transaction
            .list_tables()
            .unwrap()
            .any(|table| table.name() == "raft_snapshot_chunks_v1")
    );
}

#[tokio::test]
async fn metadata_checkpoints_recover_after_power_loss_before_and_after_log_pruning() {
    let directory = tempfile::tempdir().unwrap();
    let (mut store, durable, _) = power_loss_store(directory.path()).await;
    let first = entry(
        1,
        command("first", 0, &[("value", json!("a".repeat(1_000_000)))], &[]),
    );
    store.blocking_append([first.clone()]).await.unwrap();
    store.apply([first]).await.unwrap();
    let snapshot = store.build_snapshot().await.unwrap();
    assert!(format!("{:?}", snapshot.snapshot).contains("materialized: false"));
    store.purge(log_id(1)).await.unwrap();
    assert_metadata_only_snapshot(&store);
    let second = entry(2, command("second", 1, &[("value", json!("new"))], &[]));
    store.blocking_append([second.clone()]).await.unwrap();
    store.apply([second]).await.unwrap();
    store.inner.persistence.drain().await.unwrap();
    let unflushed_apply = durable.lock().unwrap().clone();
    store.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
    let advanced_projection = durable.lock().unwrap().clone();
    assert_eq!(
        read_meta::<SnapshotMeta<u64, BasicNode>>(
            store.inner.shared.database(),
            Tables::new(""),
            CHECKPOINT_KEY
        )
        .unwrap()
        .unwrap()
        .last_log_id,
        Some(log_id(1))
    );
    // Snapshot construction flushes the existing projection; pruning and the
    // checkpoint never depend on a separately persisted full image.
    let current = store.build_snapshot().await.unwrap();
    assert!(format!("{:?}", current.snapshot).contains("materialized: false"));
    store.purge(log_id(2)).await.unwrap();
    let pruned = durable.lock().unwrap().clone();
    assert_metadata_only_snapshot(&store);
    assert!(store.try_get_log_entries(1..3).await.unwrap().is_empty());
    // Old transfer handles retain their exact captured state despite new applies.
    let old: StoredState =
        serde_json::from_slice(&snapshot_bytes(snapshot.snapshot).await).unwrap();
    assert_eq!(old.last_applied, Some(log_id(1)));
    assert_eq!(old.application.requests.len(), 1);
    store.close().await.unwrap();
    drop(store);

    for (image, expected_applied, log_count) in [
        (unflushed_apply, 1, 1),
        (advanced_projection, 2, 1),
        (pruned, 2, 0),
    ] {
        let recovered_dir = tempfile::tempdir().unwrap();
        std::fs::write(recovered_dir.path().join("flower.redb"), image).unwrap();
        let mut recovered = Store::open(1, recovered_dir.path().into()).await.unwrap();
        assert_eq!(
            recovered.applied_state().await.unwrap().0,
            Some(log_id(expected_applied))
        );
        let logs = recovered.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(logs.len(), log_count);
        let image = recovered.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(image.meta.last_log_id, Some(log_id(expected_applied)));
        let wire: StoredState =
            serde_json::from_slice(&snapshot_bytes(image.snapshot).await).unwrap();
        assert_eq!(wire.application.requests.len(), expected_applied as usize);
        assert_metadata_only_snapshot(&recovered);
        // The second mutation was committed before the modeled crash. If its
        // projection was not durable, its durable log still reconstructs it.
        if expected_applied < 2 {
            recovered.apply(logs).await.unwrap();
        }
        let state = recovered.snapshot().await;
        assert_eq!(state.data["value"], json!("new"));
        assert!(state.requests.contains_key("first") && state.requests.contains_key("second"));
    }
}

#[tokio::test]
async fn failed_checkpoint_flush_cannot_publish_an_image_or_retire_recovery_logs() {
    let directory = tempfile::tempdir().unwrap();
    let (mut store, durable, fail_sync) = power_loss_store(directory.path()).await;
    let first = entry(1, command("first", 0, &[("value", json!(1))], &[]));
    store.blocking_append([first.clone()]).await.unwrap();
    store.apply([first]).await.unwrap();
    fail_sync.store(true, Ordering::SeqCst);
    assert!(store.build_snapshot().await.is_err());
    assert!(store.inner.current_snapshot.read().unwrap().is_none());
    let image = durable.lock().unwrap().clone();
    store.close().await.unwrap();
    drop(store);
    let recovered_dir = tempfile::tempdir().unwrap();
    std::fs::write(recovered_dir.path().join("flower.redb"), image).unwrap();
    let mut recovered = Store::open(1, recovered_dir.path().into()).await.unwrap();
    assert!(recovered.get_current_snapshot().await.unwrap().is_none());
    let logs = recovered.try_get_log_entries(1..2).await.unwrap();
    assert_eq!(logs.len(), 1);
    recovered.apply(logs).await.unwrap();
    assert_eq!(recovered.snapshot().await.data["value"], json!(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_checkpoint_waits_for_flush_and_publishes_only_after_success() {
    let directory = tempfile::tempdir().unwrap();
    drop(Store::open(1, directory.path().into()).await.unwrap());
    let (mut store, pause) = open_pausing_store(directory.path());
    let first = entry(1, command("first", 0, &[("value", json!(1))], &[]));
    store.blocking_append([first.clone()]).await.unwrap();
    store.apply([first]).await.unwrap();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *pause.lock().unwrap() = Some(PausedSync {
        ready,
        release: release_rx,
    });
    let mut worker = store.clone();
    let task = tokio::spawn(async move { worker.build_snapshot().await });
    ready_rx.await.unwrap();
    assert!(!task.is_finished());
    assert!(store.inner.current_snapshot.read().unwrap().is_none());
    task.abort();
    release.send(()).unwrap();
    let _ = task.await;
    // The same I/O gate waits for the detached checkpoint transaction to finish.
    store.purge(log_id(1)).await.unwrap();
    assert_metadata_only_snapshot(&store);
    assert!(store.inner.current_snapshot.read().unwrap().is_some());
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await.data["value"], json!(1));
    assert!(reopened.try_get_log_entries(1..2).await.unwrap().is_empty());
    assert_eq!(
        reopened
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta
            .last_log_id,
        Some(log_id(1))
    );
}

#[tokio::test]
async fn checkpoint_ahead_of_durable_application_state_fails_closed_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut meta = store.build_snapshot().await.unwrap().meta;
    meta.last_log_id = Some(log_id(99));
    let transaction = store.inner.shared.database().begin_write().unwrap();
    transaction
        .open_table(META)
        .unwrap()
        .insert(
            CHECKPOINT_KEY,
            serde_json::to_vec(&meta).unwrap().as_slice(),
        )
        .unwrap();
    transaction.commit().unwrap();
    store.close().await.unwrap();
    drop(store);
    assert!(Store::open(1, directory.path().into()).await.is_err());
}

fn shared_storage(
    shared: &Arc<SharedDatabase>,
    directory: &std::path::Path,
    prefix: &str,
) -> Storage {
    Storage::Shared {
        database: shared.clone(),
        prefix: prefix.into(),
        directory: directory.join(prefix.trim_end_matches('/')),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicas_sharing_a_database_keep_separate_logs_and_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flower.redb");
    {
        let shared = SharedDatabase::open(&path).unwrap();
        // Replicas of different groups may reuse node IDs.
        let mut first = Store::open(1, shared_storage(&shared, directory.path(), "a/"))
            .await
            .unwrap();
        let mut second = Store::open(1, shared_storage(&shared, directory.path(), "b/"))
            .await
            .unwrap();
        first
            .blocking_append([entry(1, command("first", 0, &[("key", json!("a"))], &[]))])
            .await
            .unwrap();
        second
            .blocking_append([
                entry(1, command("one", 0, &[("key", json!("b"))], &[])),
                entry(2, command("two", 1, &[("other", json!(2))], &[])),
            ])
            .await
            .unwrap();
        first
            .apply([entry(1, command("first", 0, &[("key", json!("a"))], &[]))])
            .await
            .unwrap();
        second
            .apply([
                entry(1, command("one", 0, &[("key", json!("b"))], &[])),
                entry(2, command("two", 1, &[("other", json!(2))], &[])),
            ])
            .await
            .unwrap();
        first.close().await.unwrap();
        second.close().await.unwrap();
    }
    let shared = SharedDatabase::open(&path).unwrap();
    let mut first = Store::open(1, shared_storage(&shared, directory.path(), "a/"))
        .await
        .unwrap();
    let mut second = Store::open(1, shared_storage(&shared, directory.path(), "b/"))
        .await
        .unwrap();
    assert_eq!(
        first.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1))
    );
    assert_eq!(
        second.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2))
    );
    let (first, second) = (first.snapshot().await, second.snapshot().await);
    assert_eq!(first.data.get("key"), Some(&json!("a")));
    assert_eq!(first.data.get("other"), None);
    assert_eq!(second.data.get("key"), Some(&json!("b")));
    assert_eq!(second.data.get("other"), Some(&json!(2)));
    // A replica's own directory still refuses another node ID.
    assert!(
        Store::open(2, shared_storage(&shared, directory.path(), "a/"))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_durable_writes_share_a_flush_and_failures_stay_isolated() {
    const PROBE: TableDefinition<&str, u64> = TableDefinition::new("probe");
    let directory = tempfile::tempdir().unwrap();
    let shared = SharedDatabase::open(&directory.path().join("flower.redb")).unwrap();
    // Hold redb's writer so every submission queues behind the first batch.
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel::<()>();
    let blocker = {
        let shared = shared.clone();
        tokio::task::spawn_blocking(move || {
            let transaction = shared.database().begin_write().unwrap();
            ready.send(()).unwrap();
            release_rx.recv().unwrap();
            transaction.abort().unwrap();
        })
    };
    ready_rx.await.unwrap();
    let writes: Vec<_> = (0..16u64)
        .map(|index| {
            let shared = shared.clone();
            tokio::spawn(async move {
                shared
                    .submit(
                        true,
                        StorageTrace::default(),
                        None,
                        move |transaction, _| {
                            let mut table = transaction.open_table(PROBE)?;
                            table.insert(format!("key-{index}").as_str(), index)?;
                            // A failure after a partial write must leave nothing behind.
                            anyhow::ensure!(index != 7, "write 7 fails");
                            Ok(())
                        },
                    )
                    .committed()
                    .await
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    release.send(()).unwrap();
    blocker.await.unwrap();
    for (index, write) in writes.into_iter().enumerate() {
        let result = write.await.unwrap();
        assert_eq!(result.is_ok(), index != 7, "write {index}: {result:?}");
    }
    let (batches, durable, staged) = shared.batches();
    assert_eq!(staged, 15);
    assert!(
        durable <= 2 && batches == durable,
        "{batches} batches, {durable} durable"
    );
    let transaction = shared.database().begin_read().unwrap();
    let table = transaction.open_table(PROBE).unwrap();
    assert_eq!(table.len().unwrap(), 15);
    assert!(table.get("key-7").unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_and_single_replica_databases_do_not_mix() {
    let single = tempfile::tempdir().unwrap();
    Store::open(1, single.path().into())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let shared = SharedDatabase::open(&single.path().join("flower.redb")).unwrap();
    let error = Store::open(1, shared_storage(&shared, single.path(), "a/"))
        .await
        .err()
        .unwrap();
    assert!(
        format!("{error:#}").contains("single replica's database"),
        "{error:#}"
    );
    drop(shared);
    let hosted = tempfile::tempdir().unwrap();
    {
        let shared = SharedDatabase::open(&hosted.path().join("flower.redb")).unwrap();
        Store::open(1, shared_storage(&shared, hosted.path(), "a/"))
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    }
    let error = Store::open(1, hosted.path().into()).await.err().unwrap();
    assert!(format!("{error:#}").contains("--replica"), "{error:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_appends_publish_without_waiting_and_direct_writes_wait_for_them() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(1, directory.path().into()).await.unwrap();
    // Hold redb's writer so queued appends cannot commit.
    let shared = store.inner.shared.clone();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel::<()>();
    let blocker = tokio::task::spawn_blocking(move || {
        let transaction = shared.database().begin_write().unwrap();
        ready.send(()).unwrap();
        release_rx.recv().unwrap();
        transaction.abort().unwrap();
    });
    ready_rx.await.unwrap();
    let pending = |count: usize| {
        let store = store.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                while store.inner.appending.read().unwrap().len() != count {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap()
        }
    };
    let (mut first, mut second, mut third) = (store.clone(), store.clone(), store.clone());
    let one = tokio::spawn(async move {
        first
            .blocking_append([entry(1, command("one", 0, &[], &[]))])
            .await
    });
    pending(1).await;
    // The second append is published while the first cannot commit.
    let two = tokio::spawn(async move {
        second
            .blocking_append([entry(2, command("two", 1, &[], &[]))])
            .await
    });
    pending(2).await;
    let mut reader = store.clone();
    assert_eq!(reader.try_get_log_entries(1..3).await.unwrap().len(), 2);
    assert_eq!(
        reader.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2))
    );
    // A truncation is a direct transaction: it waits for both queued appends.
    let truncated = tokio::spawn(async move { third.truncate(log_id(2)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!one.is_finished() && !two.is_finished() && !truncated.is_finished());
    release.send(()).unwrap();
    blocker.await.unwrap();
    one.await.unwrap().unwrap();
    two.await.unwrap().unwrap();
    truncated.await.unwrap().unwrap();
    assert!(store.inner.appending.read().unwrap().is_empty());
    assert_eq!(
        reader.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1))
    );
    assert_eq!(reader.try_get_log_entries(1..3).await.unwrap().len(), 1);
}

/// Space in a stored database: `FLOWER_REDB_STATS=path cargo test --release
/// --lib -- --ignored --nocapture redb_file_space`.
#[test]
#[ignore = "measurement"]
fn redb_file_space() {
    let path = std::env::var("FLOWER_REDB_STATS").expect("FLOWER_REDB_STATS");
    let file = std::fs::metadata(&path).unwrap().len();
    let database = redb::Database::open(&path).unwrap();
    let stats = database.begin_write().unwrap().stats().unwrap();
    let mib = |bytes: u64| bytes as f64 / f64::from(1 << 20);
    eprintln!(
        "file {:.0} MiB, allocated {:.0} MiB, stored leaf data {:.0} MiB, metadata {:.0} MiB, fragmented {:.0} MiB",
        mib(file),
        mib(stats.allocated_pages() * stats.page_size() as u64),
        mib(stats.stored_bytes()),
        mib(stats.metadata_bytes()),
        mib(stats.fragmented_bytes()),
    );
}

#[tokio::test]
async fn a_database_with_an_older_index_layout_fails_to_open() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(
            1,
            command(
                "rows",
                0,
                &[(r#"source:["orders","k"]"#, json!({"shop":"a"}))],
                &[],
            ),
        )])
        .await
        .unwrap();
    store.close().await.unwrap();
    drop(store);
    // As a binary before state machine 13 left it: no layout marker.
    {
        let db = Database::open(directory.path().join("flower.redb")).unwrap();
        let transaction = db.begin_write().unwrap();
        transaction
            .open_table(META)
            .unwrap()
            .remove(INDEX_LAYOUT_META)
            .unwrap();
        transaction.commit().unwrap();
    }
    let error = Store::open(1, directory.path().into()).await.err().unwrap();
    assert!(format!("{error:#}").contains("older layout"), "{error:#}");
}

#[test]
fn a_mostly_free_file_compacts_when_opened_after_doubling() {
    const FILLER: TableDefinition<u64, &[u8]> = TableDefinition::new("filler");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flower.redb");
    let grow = |rows: u64| {
        let shared = SharedDatabase::open(&path).unwrap();
        let db = shared.database();
        let transaction = db.begin_write().unwrap();
        {
            let mut table = transaction.open_table(FILLER).unwrap();
            for row in 0..rows {
                table.insert(row, [7u8; 4096].as_slice()).unwrap();
            }
        }
        transaction.commit().unwrap();
        let transaction = db.begin_write().unwrap();
        transaction.delete_table(FILLER).unwrap();
        transaction.commit().unwrap();
        std::fs::metadata(&path).unwrap().len()
    };
    let size = || std::fs::metadata(&path).unwrap().len();
    assert!(grow(8192) > 32 << 20);
    drop(SharedDatabase::open(&path).unwrap());
    let compacted = size();
    assert!(compacted < 4 << 20, "{compacted}");
    // Recorded without opening it as a replica would, which checks first.
    let checked = |bytes: u64| {
        let db = Database::open(&path).unwrap();
        let transaction = db.begin_write().unwrap();
        transaction
            .open_table(shared::FILE_META)
            .unwrap()
            .insert("checked_bytes", bytes.to_le_bytes().as_slice())
            .unwrap();
        transaction.commit().unwrap();
    };
    // A file that has not doubled since its last check is left to reuse its
    // free pages; the check walks every table.
    let grown = grow(8192);
    checked(grown);
    drop(SharedDatabase::open(&path).unwrap());
    assert!(size() > 32 << 20, "{} of {grown}", size());
    checked(grown / 2);
    drop(SharedDatabase::open(&path).unwrap());
    assert!(size() < 4 << 20, "{} of {grown}", size());
}
