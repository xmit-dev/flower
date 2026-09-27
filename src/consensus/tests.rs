use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::VoteRequest;
use openraft::storage::{RaftLogStorage, RaftStateMachine, StorageHelper};
use openraft::testing::{StoreBuilder, Suite};
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, StorageError, Vote,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::store::Store;
use super::{
    ApplyResult, Commit, CompactBatch, Consensus, RaftCommand, SharedDatabase, Storage, TypeConfig,
};
use std::sync::Arc;

mod compact;
mod deep_json;
mod fenced;
mod membership;
mod partitions;
mod read;
mod recovery;
mod retention;

const TEST_TOKEN: &str = "flower-consensus-test-only-secret";

struct Builder;

impl StoreBuilder<TypeConfig, Store, Store, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, Store, Store), StorageError<u64>> {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(0, directory.path().into()).await.unwrap();
        Ok((directory, store.clone(), store))
    }
}

#[test]
fn openraft_storage_conformance() {
    Suite::test_all(Builder).unwrap();
}

fn commit(request: &str, expected_revision: u64, value: u64) -> Commit {
    Commit {
        internal: false,
        request_id: request.into(),
        fingerprint: format!("fingerprint-{request}"),
        expected_revision,
        puts: BTreeMap::from([
            ("source:[\"counter\",\"one\"]".into(), json!(value)),
            (
                "cell:[\"double\",null]".into(),
                json!({"ok": true, "value": value * 2}),
            ),
        ]),
        deletes: vec![],
        result: json!({"written": value}),
    }
}

fn entry(index: u64, command: Commit) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command.into()),
    }
}

fn group_entry(index: u64, batch: Vec<Commit>) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(RaftCommand::Batch {
            batch: CompactBatch::new(batch).unwrap(),
        }),
    }
}

#[test]
fn grouped_commands_decode_compact_metadata_and_single_defaults() {
    let old = commit("old", 0, 9);
    assert_eq!(
        serde_json::to_value(RaftCommand::Single(old.clone())).unwrap(),
        serde_json::to_value(&old).unwrap()
    );
    let mut legacy = serde_json::to_value(old).unwrap();
    legacy.as_object_mut().unwrap().remove("internal");
    legacy.as_object_mut().unwrap().remove("result");
    let encoded = json!({
        "log_id": LogId::new(CommittedLeaderId::new(1, 1), 1),
        "payload": {"Normal": legacy},
    });
    let decoded: Entry<TypeConfig> = serde_json::from_value(encoded).unwrap();
    assert!(
        matches!(decoded.payload, EntryPayload::Normal(RaftCommand::Single(command))
        if command.request_id == "old" && !command.internal && command.result.is_null())
    );
    let mut mixed = serde_json::to_value(commit("ignored-single", 0, 99)).unwrap();
    mixed["batch"] =
        serde_json::to_value(CompactBatch::new(vec![commit("actual-group", 0, 1)]).unwrap())
            .unwrap();
    assert!(
        matches!(serde_json::from_value::<RaftCommand>(mixed).unwrap(),
        RaftCommand::Batch { batch } if batch.items.len() == 1 && batch.items[0].request_id == "actual-group")
    );
}

#[tokio::test]
async fn grouped_commits_reject_atomically_and_keep_durable_individual_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let commands = vec![commit("a", 0, 1), commit("b", 1, 2), commit("c", 2, 3)];
    let results = store
        .apply([group_entry(1, commands.clone())])
        .await
        .unwrap();
    let ApplyResult::Batch(results) = &results[0] else {
        panic!("expected grouped results")
    };
    assert_eq!(results.len(), 3);
    for (index, result) in results.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if !result.duplicate && result.revision == index as u64 + 1
                && result.result == json!({"written":index+1})));
    }
    let first = store.snapshot().await;
    assert_eq!(first.revision, 3);
    assert_eq!(first.requests.len(), 3);
    assert_eq!(store.applied_state().await.unwrap().0.unwrap().index, 1);

    let results = store.apply([group_entry(2, commands)]).await.unwrap();
    let ApplyResult::Batch(results) = &results[0] else {
        panic!("expected grouped duplicate results")
    };
    assert!(results.iter().all(|result| matches!(result,
        ApplyResult::Committed(result) if result.duplicate)));
    assert_eq!(store.snapshot().await, first);

    // A partially matching retry cannot apply a hidden intermediate patch.
    // Reject the complete batch and let the writer rebase from fresh receipts.
    let results = store
        .apply([group_entry(
            3,
            vec![
                commit("d", 3, 4),
                commit("a", 4, 999),
                commit("must-retry", 5, 6),
            ],
        )])
        .await
        .unwrap();
    let ApplyResult::Batch(results) = &results[0] else {
        panic!("expected independent grouped results")
    };
    assert!(
        results
            .iter()
            .all(|result| matches!(result, ApplyResult::Rejected(_)))
    );
    assert_eq!(store.snapshot().await, first);
    let mut reused = commit("b", 3, 999);
    reused.fingerprint = "different".into();
    let results = store
        .apply([group_entry(4, vec![reused, commit("also-retry", 4, 6)])])
        .await
        .unwrap();
    assert!(matches!(&results[0], ApplyResult::Batch(results)
        if results.iter().all(|result| matches!(result, ApplyResult::Rejected(_)))));
    store.apply([entry(5, commit("e", 3, 5))]).await.unwrap();
    let expected = store.snapshot().await;
    let snapshot = store.build_snapshot().await.unwrap();
    store.close().await.unwrap();
    drop(store);

    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, expected);
    assert_eq!(expected.revision, 4);
    assert_eq!(expected.requests.len(), 4);
    let destination = tempfile::tempdir().unwrap();
    let mut follower = Store::open(2, destination.path().into()).await.unwrap();
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(follower.snapshot().await, expected);
}

#[test]
fn old_commit_entries_default_to_client_receipts() {
    let original = commit("old-log-entry", 0, 1);
    let mut encoded = serde_json::to_value(original).unwrap();
    encoded.as_object_mut().unwrap().remove("internal");
    let decoded: Commit = serde_json::from_value(encoded).unwrap();
    assert!(!decoded.internal);
}

#[tokio::test]
async fn durable_applied_state_recovers_without_a_separate_committed_marker() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store.save_vote(&Vote::new_committed(1, 1)).await.unwrap();
    let applied = entry(1, commit("durable-receipt", 0, 17));
    store.save_committed(Some(applied.log_id)).await.unwrap();
    assert_eq!(store.read_committed().await.unwrap(), None);
    store.apply([applied.clone()]).await.unwrap();
    let expected = store.snapshot().await;
    store.close().await.unwrap();
    drop(store);

    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.read_committed().await.unwrap(), None);
    assert_eq!(reopened.snapshot().await, expected);
    let initial = StorageHelper::new(&mut reopened.clone(), &mut reopened)
        .get_initial_state()
        .await
        .unwrap();
    assert_eq!(initial.committed, Some(applied.log_id));
    let result = reopened
        .apply([entry(2, commit("durable-receipt", 0, 999))])
        .await
        .unwrap();
    assert!(matches!(&result[0], ApplyResult::Committed(result)
        if result.duplicate && result.revision == 1 && result.result == json!({"written":17})));
    assert_eq!(reopened.snapshot().await, expected);
}

#[tokio::test]
async fn legacy_committed_markers_remain_readable_and_cannot_regress_applied_state() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store.save_vote(&Vote::new_committed(1, 1)).await.unwrap();
    let first = entry(1, commit("first", 0, 10));
    let second = entry(2, commit("second", 1, 20));
    store.apply([first.clone(), second.clone()]).await.unwrap();
    let expected = store.snapshot().await;
    store.close().await.unwrap();
    drop(store);

    // Reproduce the metadata format of a database created by an earlier binary.
    let db = redb::Database::open(directory.path().join("flower.redb")).unwrap();
    let mut transaction = db.begin_write().unwrap();
    transaction
        .set_durability(redb::Durability::Immediate)
        .unwrap();
    {
        let mut meta = transaction
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("raft_meta_v1"))
            .unwrap();
        meta.insert(
            "committed",
            serde_json::to_vec(&Some(first.log_id)).unwrap().as_slice(),
        )
        .unwrap();
    }
    transaction.commit().unwrap();
    drop(db);

    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.read_committed().await.unwrap(), Some(first.log_id));
    reopened.save_committed(Some(second.log_id)).await.unwrap();
    assert_eq!(reopened.read_committed().await.unwrap(), Some(first.log_id));
    let initial = StorageHelper::new(&mut reopened.clone(), &mut reopened)
        .get_initial_state()
        .await
        .unwrap();
    assert_eq!(initial.committed, Some(second.log_id));
    assert_eq!(reopened.snapshot().await, expected);
}

#[tokio::test]
async fn legacy_committed_marker_replays_a_durable_log_not_yet_applied_before_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store.save_vote(&Vote::new_committed(1, 1)).await.unwrap();
    let first = entry(1, commit("first", 0, 10));
    let second = entry(2, commit("second", 1, 20));
    store.apply([first.clone()]).await.unwrap();
    store.build_snapshot().await.unwrap();
    store.purge(first.log_id).await.unwrap();
    store.close().await.unwrap();
    drop(store);

    // An old binary could crash after flushing its commit marker, before apply.
    let db = redb::Database::open(directory.path().join("flower.redb")).unwrap();
    let mut transaction = db.begin_write().unwrap();
    transaction
        .set_durability(redb::Durability::Immediate)
        .unwrap();
    {
        let mut logs = transaction
            .open_table(redb::TableDefinition::<u64, &[u8]>::new("raft_logs_v1"))
            .unwrap();
        logs.insert(
            second.log_id.index,
            serde_json::to_vec(&second).unwrap().as_slice(),
        )
        .unwrap();
        let mut meta = transaction
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("raft_meta_v1"))
            .unwrap();
        meta.insert(
            "committed",
            serde_json::to_vec(&Some(second.log_id)).unwrap().as_slice(),
        )
        .unwrap();
    }
    transaction.commit().unwrap();
    drop(db);

    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await.revision, 1);
    let initial = StorageHelper::new(&mut reopened.clone(), &mut reopened)
        .get_initial_state()
        .await
        .unwrap();
    assert_eq!(initial.committed, Some(second.log_id));
    let recovered = reopened.snapshot().await;
    assert_eq!(recovered.revision, 2);
    assert_eq!(recovered.data["source:[\"counter\",\"one\"]"], json!(20));
    assert_eq!(recovered.requests["first"].result, json!({"written":10}));
    assert_eq!(recovered.requests["second"].result, json!({"written":20}));
}

#[tokio::test]
async fn internal_maintenance_preserves_cas_and_client_receipts_across_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, commit("shared-id", 0, 10))])
        .await
        .unwrap();
    let receipt = store.snapshot().await.requests["shared-id"].clone();

    // Even a colliding client ID and fingerprint cannot deduplicate maintenance
    // or overwrite the client's original result.
    let mut maintenance = commit("shared-id", 1, 20);
    maintenance.internal = true;
    let result = store.apply([entry(2, maintenance.clone())]).await.unwrap();
    assert!(matches!(
        &result[0],
        ApplyResult::Committed(result)
            if result.revision == 2 && !result.duplicate
                && result.result == json!({"written": 20})
    ));
    assert_eq!(store.snapshot().await.requests["shared-id"], receipt);

    // Replaying an internal command cannot apply its patch a second time: the
    // application revision still guards commands without retry receipts.
    assert!(matches!(
        store.apply([entry(3, maintenance.clone())]).await.unwrap()[0],
        ApplyResult::Rejected(_)
    ));
    assert_eq!(store.snapshot().await.revision, 2);
    maintenance.expected_revision = 2;
    maintenance.fingerprint = "different-maintenance-content".into();
    maintenance.deletes = vec!["cell:[\"double\",null]".into()];
    maintenance.puts = BTreeMap::from([("clock".into(), json!(1000))]);
    let result = store.apply([entry(4, maintenance)]).await.unwrap();
    assert!(matches!(
        &result[0],
        ApplyResult::Committed(result) if result.revision == 3 && !result.duplicate
    ));
    assert!(
        !store
            .snapshot()
            .await
            .data
            .contains_key("cell:[\"double\",null]")
    );

    // An internal ID also cannot reserve a future external request ID.
    let mut maintenance = commit("future-client-id", 3, 30);
    maintenance.internal = true;
    store.apply([entry(5, maintenance)]).await.unwrap();
    assert_eq!(store.snapshot().await.requests.len(), 1);
    let result = store
        .apply([entry(6, commit("future-client-id", 4, 40))])
        .await
        .unwrap();
    assert!(matches!(
        &result[0],
        ApplyResult::Committed(result) if result.revision == 5 && !result.duplicate
    ));
    let expected = store.snapshot().await;
    assert_eq!(expected.requests.len(), 2);
    assert_eq!(expected.requests["shared-id"], receipt);
    assert_eq!(expected.data["clock"], json!(1000));
    assert_eq!(expected.data["source:[\"counter\",\"one\"]"], json!(40));
    let snapshot = store.build_snapshot().await.unwrap();
    store.close().await.unwrap();
    drop(store);

    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, expected);
    // A client retry still returns its original value after maintenance,
    // unrelated writes, snapshot creation, and reopening durable storage.
    let result = reopened
        .apply([entry(7, commit("shared-id", 0, 999))])
        .await
        .unwrap();
    assert!(matches!(
        &result[0],
        ApplyResult::Committed(result)
            if result.revision == 1 && result.duplicate
                && result.result == json!({"written": 10})
    ));
    assert_eq!(reopened.snapshot().await, expected);

    let destination = tempfile::tempdir().unwrap();
    let mut follower = Store::open(2, destination.path().into()).await.unwrap();
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(follower.snapshot().await, expected);
}

#[tokio::test]
async fn durable_state_receipts_vote_and_snapshot_install() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, first.path().into()).await.unwrap();
    store.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
    let results = store.apply([entry(1, commit("a", 0, 10))]).await.unwrap();
    assert!(
        matches!(results[0], ApplyResult::Committed(ref result) if result.revision == 1 && !result.duplicate && result.result == json!({"written": 10}))
    );

    // A retry carries the original base revision and must be deduplicated first.
    let results = store.apply([entry(2, commit("a", 0, 999))]).await.unwrap();
    assert!(
        matches!(results[0], ApplyResult::Committed(ref result) if result.revision == 1 && result.duplicate && result.result == json!({"written": 10}))
    );
    let mut reused = commit("a", 1, 999);
    reused.fingerprint = "different".into();
    assert!(matches!(
        store.apply([entry(3, reused)]).await.unwrap()[0],
        ApplyResult::Rejected(_)
    ));
    assert!(matches!(
        store
            .apply([entry(4, commit("stale", 0, 50))])
            .await
            .unwrap()[0],
        ApplyResult::Rejected(_)
    ));
    let snapshot = store.build_snapshot().await.unwrap();
    let expected = store.snapshot().await;
    assert_eq!(expected.revision, 1);
    assert_eq!(expected.data["source:[\"counter\",\"one\"]"], json!(10));
    assert_eq!(expected.requests["a"].result, json!({"written": 10}));
    assert!(!expected.requests.contains_key("stale"));
    store.close().await.unwrap();
    drop(store);

    let mut reopened = Store::open(1, first.path().into()).await.unwrap();
    assert_eq!(
        reopened.read_vote().await.unwrap(),
        // Its own vote comes back uncommitted, to be won again.
        Some(Vote::new(3, 1))
    );
    assert_eq!(reopened.snapshot().await, expected);
    assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 4);
    assert!(reopened.get_current_snapshot().await.unwrap().is_some());

    let mut destination = Store::open(2, second.path().into()).await.unwrap();
    destination
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    destination.close().await.unwrap();
    drop(destination);
    let mut destination = Store::open(2, second.path().into()).await.unwrap();
    assert_eq!(destination.snapshot().await, expected);
    assert_eq!(
        destination.applied_state().await.unwrap().0.unwrap().index,
        4
    );
    assert!(destination.get_current_snapshot().await.unwrap().is_some());
    reopened.close().await.unwrap();
    drop(reopened);
    let wrong_id = Store::open(99, first.path().into()).await.err().unwrap();
    assert!(wrong_id.to_string().contains("belongs to node 1"));
}

struct Node {
    id: u64,
    address: String,
    directory: TempDir,
    // A process hosting this replica with others: its database and prefix.
    host: Option<(Host, String)>,
    consensus: Option<Consensus>,
    server: Option<JoinHandle<()>>,
    stop_server: Option<tokio::sync::oneshot::Sender<()>>,
}

/// One database shared by the replicas of a simulated host process. Stopping
/// every replica and forgetting the database closes it, as an exit would.
#[derive(Clone)]
struct Host {
    path: std::path::PathBuf,
    database: Arc<std::sync::Mutex<Option<Arc<SharedDatabase>>>>,
}

impl Host {
    fn new(directory: &TempDir) -> Self {
        Self {
            path: directory.path().join("flower.redb"),
            database: Arc::default(),
        }
    }

    fn database(&self) -> Arc<SharedDatabase> {
        let mut database = self.database.lock().unwrap();
        database
            .get_or_insert_with(|| SharedDatabase::open(&self.path).unwrap())
            .clone()
    }

    fn forget(&self) {
        self.database.lock().unwrap().take();
    }
}

impl Node {
    async fn new(id: u64) -> Self {
        Self::start_new(id, None).await
    }

    async fn hosted(id: u64, host: &Host, prefix: &str) -> Self {
        Self::start_new(id, Some((host.clone(), prefix.into()))).await
    }

    async fn start_new(id: u64, host: Option<(Host, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut node = Self {
            id,
            address,
            directory: tempfile::tempdir().unwrap(),
            host,
            consensus: None,
            server: None,
            stop_server: None,
        };
        node.start_with_listener(listener).await;
        node
    }

    async fn start_with_listener(&mut self, listener: TcpListener) {
        let storage = match &self.host {
            Some((host, prefix)) => Storage::Shared {
                database: host.database(),
                prefix: prefix.clone(),
                directory: self.directory.path().into(),
            },
            None => self.directory.path().into(),
        };
        let consensus = Consensus::open(self.id, self.address.clone(), storage, TEST_TOKEN.into())
            .await
            .unwrap();
        let router = consensus.router();
        let (stop_server, stopped) = tokio::sync::oneshot::channel();
        self.stop_server = Some(stop_server);
        self.server = Some(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap()
        }));
        self.consensus = Some(consensus);
    }

    async fn restart(&mut self) {
        let listener = TcpListener::bind(&self.address).await.unwrap();
        self.start_with_listener(listener).await;
    }

    async fn stop(&mut self) {
        if let Some(stop_server) = self.stop_server.take() {
            let _ = stop_server.send(());
        }
        if let Some(consensus) = self.consensus.take() {
            consensus.shutdown().await.unwrap();
            if let Some(server) = self.server.take() {
                server.await.unwrap();
            }
            drop(consensus);
        }
    }

    fn raft(&self) -> &Consensus {
        self.consensus.as_ref().unwrap()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(server) = &self.server {
            server.abort();
        }
    }
}

#[tokio::test]
async fn commit_group_limits_are_checked_before_proposing_a_log_entry() {
    let mut node = Node::new(1).await;
    let error = node.raft().commit_many(vec![]).await.unwrap_err();
    assert!(error.to_string().contains("at least one"));
    let error = node
        .raft()
        .commit_many(vec![commit("a", 0, 1), commit("b", 2, 2)])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("contiguous"));
    let mut oversized = commit("oversized", 0, 1);
    oversized.puts.insert(
        "large".into(),
        json!("x".repeat(node.raft().limits().transaction_max_bytes)),
    );
    let error = node.raft().commit_many(vec![oversized]).await.unwrap_err();
    assert!(error.to_string().contains("FLOWER_TRANSACTION_MAX_BYTES"));
    assert_eq!(node.raft().local_snapshot().await.revision, 0);
    assert_eq!(node.raft().metrics().last_applied, None);
    node.stop().await;
}

#[tokio::test]
async fn projected_reads_preserve_quorum_revision_data_and_only_the_requested_receipt() {
    let mut node = Node::new(1).await;
    // A local empty snapshot exists, but projections must establish exactly the
    // same serving quorum as full reads before exposing even that empty data.
    assert!(node.raft().read_for(None).await.is_err());
    assert!(node.raft().read_for(Some("first")).await.is_err());
    assert!(node.raft().read_for_writer().await.is_err());
    node.raft()
        .initialize(BTreeMap::from([(node.id, node.address.clone())]))
        .await
        .unwrap();
    leader(std::slice::from_ref(&node)).await;
    for (index, request) in ["first", "second", "third"].into_iter().enumerate() {
        node.raft()
            .commit(commit(request, index as u64, 10 + index as u64))
            .await
            .unwrap();
    }
    let full = node.raft().read().await.unwrap();
    assert_eq!(full.requests.len(), 3);
    let writer = node.raft().read_for_writer().await.unwrap();
    assert_eq!(writer, full);
    assert!(writer.data.ptr_eq(&full.data));
    assert!(writer.requests.ptr_eq(&full.requests));
    for request in [None, Some("first"), Some("second"), Some("missing")] {
        let projected = node.raft().read_for(request).await.unwrap();
        assert_eq!(projected.revision, full.revision);
        assert_eq!(projected.data, full.data);
        let expected = request
            .and_then(|id| full.requests.get_key_value(id))
            .map(|(id, receipt)| (id.clone(), receipt.clone()))
            .into_iter()
            .collect();
        assert_eq!(projected.requests, expected);
    }
    let many = node
        .raft()
        .read_for_many(&[
            "first".into(),
            "third".into(),
            "missing".into(),
            "first".into(),
        ])
        .await
        .unwrap();
    assert_eq!(many.revision, full.revision);
    assert_eq!(many.data, full.data);
    assert_eq!(many.requests.len(), 2);
    assert_eq!(many.requests["first"], full.requests["first"]);
    assert_eq!(many.requests["third"], full.requests["third"]);
    // Projection never removes receipts from the authoritative state: an older
    // request still returns its original result, even with its old revision.
    let replay = node.raft().commit(commit("first", 0, 999)).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.revision, 1);
    assert_eq!(replay.result, json!({"written": 10}));
    assert_eq!(node.raft().local_snapshot().await, full);
    node.stop().await;
}

#[tokio::test]
async fn full_sized_commit_group_preserves_all_receipts_across_restart() {
    let mut node = Node::new(1).await;
    node.raft()
        .initialize(BTreeMap::from([(node.id, node.address.clone())]))
        .await
        .unwrap();
    leader(std::slice::from_ref(&node)).await;
    let group: Vec<_> = (0..300_u64)
        .map(|index| commit(&format!("item-{index}"), index, index))
        .collect();
    let committed = node.raft().commit_many(group.clone()).await.unwrap();
    assert_eq!(committed.len(), 300);
    for (index, result) in committed.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if !result.duplicate && result.revision == index as u64 + 1));
    }
    let expected = node.raft().read_for_writer().await.unwrap();
    assert_eq!(expected.revision, 300_u64);
    assert_eq!(expected.requests.len(), 300);
    node.stop().await;
    node.restart().await;
    leader(std::slice::from_ref(&node)).await;
    assert_eq!(node.raft().read_for_writer().await.unwrap(), expected);
    let replayed = node.raft().commit_many(group).await.unwrap();
    for (index, result) in replayed.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if result.duplicate && result.revision == index as u64 + 1
            && result.result == json!({"written": index})));
    }
    assert_eq!(node.raft().read_for_writer().await.unwrap(), expected);
    node.stop().await;
}

#[tokio::test]
async fn raft_routes_require_operator_token() {
    let mut node = Node::new(1).await;
    let h1 = reqwest::Client::builder().http1_only().build().unwrap();
    let h2 = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    for (client, expected_version) in [
        (&h1, reqwest::Version::HTTP_11),
        (&h2, reqwest::Version::HTTP_2),
    ] {
        for (method, path) in [
            (reqwest::Method::GET, "metrics"),
            (reqwest::Method::POST, "initialize"),
            (reqwest::Method::GET, "membership"),
            (reqwest::Method::POST, "membership"),
            (reqwest::Method::GET, "version"),
            (reqwest::Method::POST, "append"),
            (reqwest::Method::POST, "vote"),
            (reqwest::Method::POST, "snapshot"),
        ] {
            for token in [None, Some("incorrect-token")] {
                let mut request = client.request(
                    method.clone(),
                    format!("http://{}/raft/{path}", node.address),
                );
                if let Some(token) = token {
                    request = request.bearer_auth(token);
                }
                let response = request.json(&json!({})).send().await.unwrap();
                assert_eq!(response.version(), expected_version);
                assert_eq!(
                    response.status(),
                    reqwest::StatusCode::UNAUTHORIZED,
                    "{method} /raft/{path} over {expected_version:?}"
                );
                assert_eq!(
                    response.json::<serde_json::Value>().await.unwrap()["error"],
                    "unauthorized"
                );
            }
        }
        let response = client
            .get(format!("http://{}/raft/metrics", node.address))
            .bearer_auth(TEST_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(response.version(), expected_version);
        assert!(response.status().is_success());
        assert_eq!(response.json::<serde_json::Value>().await.unwrap()["id"], 1);
    }
    assert!(!node.raft().raft.is_initialized().await.unwrap());
    let client = h2;
    let response = client
        .post(format!("http://{}/raft/initialize", node.address))
        .bearer_auth(TEST_TOKEN)
        .json(&BTreeMap::from([(node.id, node.address.clone())]))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    leader(std::slice::from_ref(&node)).await;
    node.stop().await;

    let directory = tempfile::tempdir().unwrap();
    let error = Consensus::open(
        1,
        "127.0.0.1:7101".into(),
        directory.path().into(),
        "".into(),
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("nonempty"));
}

#[tokio::test]
async fn duplicate_member_addresses_are_rejected_before_bootstrap() {
    let mut node = Node::new(1).await;
    let error = node
        .raft()
        .initialize(BTreeMap::from([
            (1, node.address.clone()),
            (2, node.address.clone()),
        ]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("distinct advertised address"));
    assert!(!node.raft().raft.is_initialized().await.unwrap());
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostname_alias_of_one_process_cannot_supply_a_second_vote() {
    let mut node = Node::new(1).await;
    let port = node.address.rsplit_once(':').unwrap().1;
    let alias = format!("localhost:{port}");
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    // Prove the distinct hostname reaches this process; the failed election
    // below must be caused by identity checking, not an unreachable address.
    let response = client
        .get(format!("http://{alias}/raft/metrics"))
        .bearer_auth(TEST_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.json::<serde_json::Value>().await.unwrap()["id"], 1);

    let initial_term = node.raft().metrics().current_term;
    let response = client
        .post(format!("http://{alias}/raft/vote"))
        .bearer_auth(TEST_TOKEN)
        .header(super::RPC_TARGET_HEADER, "2")
        .json(&VoteRequest {
            vote: Vote::new(999, 1),
            last_log_id: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(response.headers()[super::RPC_NODE_HEADER], "1");
    assert_eq!(
        node.raft().metrics().current_term,
        initial_term,
        "misaddressed vote must not reach Raft"
    );

    node.raft()
        .initialize(BTreeMap::from([(1, node.address.clone()), (2, alias)]))
        .await
        .unwrap();
    node.raft().raft.trigger().elect().await.unwrap();
    assert!(
        node.raft()
            .raft
            .wait(Some(Duration::from_secs(3)))
            .current_leader(1, "one process must not impersonate two voters")
            .await
            .is_err()
    );
    assert!(node.raft().metrics().running_state.is_ok());
    assert!(node.raft().read().await.is_err());
    assert!(
        node.raft()
            .commit(commit("false-quorum", 0, 1))
            .await
            .is_err()
    );
    assert_eq!(node.raft().local_snapshot().await.revision, 0);
    node.stop().await;
}

#[tokio::test]
async fn transport_rejects_success_from_a_different_node() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let router = axum::Router::new().route(
        "/raft/vote",
        axum::routing::post(|| async { ([(super::RPC_NODE_HEADER, "1")], axum::Json(json!({}))) }),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let mut network = super::network::Network::new(TEST_TOKEN).unwrap();
    let mut connection = network
        .new_client(2, &openraft::BasicNode::new(address))
        .await;
    let error = connection
        .vote(
            VoteRequest {
                vote: Vote::new(1, 3),
                last_log_id: None,
            },
            RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identity mismatch"), "{error}");
    let _ = stop.send(());
    server.await.unwrap();
}

/// Warm a peer connection, then force two RPCs to be in flight simultaneously.
/// Both streams must reach the same TCP socket before either response completes.
#[tokio::test]
async fn raft_http2_multiplexes_authenticated_rpcs_and_preserves_deadlines() {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::extract::{ConnectInfo, State};
    use axum::http::{HeaderMap, Version};
    use openraft::raft::VoteResponse;
    use tokio::sync::{Barrier, Mutex};

    #[derive(Clone)]
    struct Peer {
        peers: Arc<Mutex<Vec<SocketAddr>>>,
        barrier: Arc<Barrier>,
    }
    let peer = Peer {
        peers: Arc::new(Mutex::new(Vec::new())),
        barrier: Arc::new(Barrier::new(2)),
    };
    let router = axum::Router::new()
        .route(
            "/raft/vote",
            axum::routing::post(
                |State(peer): State<Peer>,
                 ConnectInfo(remote): ConnectInfo<SocketAddr>,
                 version: Version,
                 headers: HeaderMap,
                 axum::Json(request): axum::Json<VoteRequest<u64>>| async move {
                    assert_eq!(version, Version::HTTP_2);
                    assert_eq!(
                        headers[axum::http::header::AUTHORIZATION],
                        format!("Bearer {TEST_TOKEN}")
                    );
                    assert_eq!(headers[super::RPC_TARGET_HEADER], "2");
                    assert_eq!(
                        headers[super::membership::COMPATIBILITY_HEADER],
                        super::membership::contract()
                    );
                    peer.peers.lock().await.push(remote);
                    match request.vote.leader_id.term {
                        2 => {
                            peer.barrier.wait().await;
                        }
                        3 => tokio::time::sleep(Duration::from_secs(2)).await,
                        _ => {}
                    }
                    let response: Result<VoteResponse<u64>, openraft::error::RaftError<u64>> =
                        Ok(VoteResponse::new(request.vote, None, true));
                    (
                        [
                            (super::RPC_NODE_HEADER, "2"),
                            (
                                super::membership::COMPATIBILITY_HEADER,
                                super::membership::contract(),
                            ),
                        ],
                        axum::Json(response),
                    )
                },
            ),
        )
        .with_state(peer.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut network = super::network::Network::new(TEST_TOKEN).unwrap();
    let node = openraft::BasicNode::new(address);
    let mut first = network.new_client(2, &node).await;
    let mut second = network.new_client(2, &node).await;
    let request = |term| VoteRequest {
        vote: Vote::new(term, 1),
        last_log_id: None,
    };
    let option = || RPCOption::new(Duration::from_secs(2));
    first.vote(request(1), option()).await.unwrap();
    let (left, right) = tokio::join!(
        first.vote(request(2), option()),
        second.vote(request(2), option())
    );
    assert!(left.unwrap().vote_granted);
    assert!(right.unwrap().vote_granted);
    let peers = peer.peers.lock().await;
    assert_eq!(peers.len(), 3);
    assert!(
        peers.iter().all(|address| address == &peers[0]),
        "RPCs must share the warmed HTTP/2 socket"
    );
    drop(peers);

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        first.vote(request(3), RPCOption::new(Duration::from_millis(75))),
    )
    .await
    .expect("the RPC hard deadline must stop a stalled HTTP/2 stream")
    .unwrap_err();
    assert!(
        matches!(error, openraft::error::RPCError::Network(_)),
        "{error}"
    );
    // Cancelling one stream must not poison the shared connection.
    assert!(
        second
            .vote(request(4), option())
            .await
            .unwrap()
            .vote_granted
    );
    server.abort();
    let _ = server.await;
}

#[test]
fn old_receipts_and_commands_default_to_null_results() {
    let receipt: super::Receipt =
        serde_json::from_value(json!({"fingerprint": "old", "revision": 4})).unwrap();
    assert_eq!(receipt.result, serde_json::Value::Null);
    let mut command = serde_json::to_value(commit("legacy", 0, 3)).unwrap();
    command.as_object_mut().unwrap().remove("result");
    assert_eq!(
        serde_json::from_value::<Commit>(command).unwrap().result,
        serde_json::Value::Null
    );
}

async fn leader(nodes: &[Node]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            let Some(consensus) = &node.consensus else {
                continue;
            };
            if consensus.metrics().current_leader == Some(node.id)
                && matches!(
                    tokio::time::timeout(Duration::from_secs(1), consensus.read()).await,
                    Ok(Ok(_))
                )
            {
                return index;
            }
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not elect a readable leader"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

async fn wait_revision(node: &Node, revision: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if node.raft().local_snapshot().await.revision == revision {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {} did not apply revision {revision}: {:?}",
            node.id,
            node.raft().metrics()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_group_commit_replays_after_failover_and_full_restart() {
    let mut nodes = vec![Node::new(1).await, Node::new(2).await, Node::new(3).await];
    nodes[0]
        .raft()
        .initialize(
            nodes
                .iter()
                .map(|node| (node.id, node.address.clone()))
                .collect(),
        )
        .await
        .unwrap();
    let first_leader = leader(&nodes).await;
    let first_group = vec![commit("a", 0, 1), commit("b", 1, 2), commit("c", 2, 3)];
    let results = nodes[first_leader]
        .raft()
        .commit_many(first_group.clone())
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| matches!(result,
        ApplyResult::Committed(result) if !result.duplicate)));
    for node in &nodes {
        wait_revision(node, 3).await;
    }
    let expected = nodes[first_leader].raft().read().await.unwrap();
    nodes[first_leader].stop().await;
    let next = leader(&nodes).await;
    assert_ne!(next, first_leader);
    assert_eq!(nodes[next].raft().read().await.unwrap(), expected);
    let replay = nodes[next].raft().commit_many(first_group).await.unwrap();
    for (index, result) in replay.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if result.duplicate && result.revision == index as u64 + 1
                && result.result == json!({"written":index+1})));
    }
    nodes[next]
        .raft()
        .commit_many(vec![commit("d", 3, 4), commit("e", 4, 5)])
        .await
        .unwrap();
    nodes[first_leader].restart().await;
    for node in &nodes {
        wait_revision(node, 5).await;
    }
    let expected = nodes[next].raft().read().await.unwrap();
    for node in &mut nodes {
        node.stop().await;
    }
    for node in &mut nodes {
        node.restart().await;
    }
    let restored = leader(&nodes).await;
    assert_eq!(nodes[restored].raft().read().await.unwrap(), expected);
    assert_eq!(expected.revision, 5);
    assert_eq!(expected.requests.len(), 5);
    let result = nodes[restored]
        .raft()
        .commit(commit("a", 0, 1))
        .await
        .unwrap();
    assert!(result.duplicate);
    assert_eq!(result.revision, 1);
    for node in &mut nodes {
        node.stop().await;
    }
}

/// A leader of three voters commits on its followers' flushes and defers its
/// own, but still flushes in time to commit while one follower is down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_leader_defers_its_own_flush_but_keeps_quorum_with_one_follower() {
    let mut nodes = vec![Node::new(1).await, Node::new(2).await, Node::new(3).await];
    nodes[0]
        .raft()
        .initialize(
            nodes
                .iter()
                .map(|node| (node.id, node.address.clone()))
                .collect(),
        )
        .await
        .unwrap();
    let first = leader(&nodes).await;
    nodes[first].raft().commit(commit("a", 0, 1)).await.unwrap();
    assert!(nodes[first].raft().store.deferred_leader_appends() > 0);
    let down = (first + 1) % nodes.len();
    nodes[down].stop().await;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        nodes[first].raft().commit(commit("b", 1, 2)),
    )
    .await
    .expect("leader flushed its own append to complete the quorum")
    .unwrap();
    assert_eq!(result.revision, 2);
    // Graceful shutdown makes the leader's deferred appends durable.
    nodes[first].stop().await;
    nodes[first].restart().await;
    nodes[down].restart().await;
    for node in &nodes {
        wait_revision(node, 2).await;
    }
    let restored = leader(&nodes).await;
    assert!(
        nodes[restored]
            .raft()
            .commit(commit("a", 0, 1))
            .await
            .unwrap()
            .duplicate
    );
    for node in &mut nodes {
        node.stop().await;
    }
}

/// Real HTTP replication with fsynced files, a follower catching up exclusively
/// through a snapshot, leader loss, whole-cluster restart, and quorum loss.
/// A leader flushes its own appends lazily, so a crash can leave it with an
/// older log than its followers but its committed vote. Restoring an older
/// copy of its database reproduces that. It must not resume leading the same
/// term, which would put different entries under log ids its followers hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_restarted_with_lost_appends_does_not_resume_its_term() {
    let mut nodes = vec![Node::new(1).await, Node::new(2).await, Node::new(3).await];
    let members = nodes
        .iter()
        .map(|node| (node.id, node.address.clone()))
        .collect();
    nodes[0].raft().initialize(members).await.unwrap();
    let first = leader(&nodes).await;
    nodes[first]
        .raft()
        .commit(commit("initial", 0, 1))
        .await
        .unwrap();
    for node in &nodes {
        wait_revision(node, 1).await;
    }
    // A copy while it holds its own committed vote and the log so far.
    let path = nodes[first].directory.path().join("flower.redb");
    let backup = nodes[first].directory.path().join("backup.redb");
    nodes[first].stop().await;
    std::fs::copy(&path, &backup).unwrap();
    nodes[first].restart().await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while nodes[first].raft().metrics().current_leader != Some(nodes[first].id) {
        assert!(Instant::now() < deadline, "the first leader did not lead again");
        let _ = nodes[first].raft().raft.trigger().elect().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    for revision in 1..5 {
        nodes[first]
            .raft()
            .commit(commit(&format!("later-{revision}"), revision, revision + 1))
            .await
            .unwrap();
    }
    // The crash: every write since the copy is gone.
    nodes[first].stop().await;
    std::fs::copy(&backup, &path).unwrap();
    nodes[first].restart().await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let current = leader(&nodes).await;
        let expected = nodes[current].raft().read().await.ok();
        let local = nodes[first].raft().local_snapshot().await;
        if expected.is_some_and(|expected| expected.revision == 5 && local == expected) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the restarted leader diverged: revision {} at {:?}, leader {:?}",
            local.revision,
            nodes[first].raft().metrics().last_applied,
            nodes[current].raft().metrics().last_applied
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A follower catches up through a snapshot that takes longer to send and
/// install than an election timeout. Snapshot chunks count as contact from
/// the leader, so the follower does not start elections that restart the
/// transfer indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_snapshot_catch_up_does_not_restart_itself_through_elections() {
    const CHILD: &str = "FLOWER_TEST_SLOW_SNAPSHOT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "consensus::tests::slow_snapshot_catch_up_does_not_restart_itself_through_elections",
                "--nocapture",
            ])
            .env(CHILD, "1")
            // Thousands of segments take seconds to send, past any timeout.
            .env("FLOWER_SNAPSHOT_CHUNK_BYTES", "1024")
            // A retained append would deliver the purged logs instead.
            .env("FLOWER_APPEND_TIMEOUT_MS", "500")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let mut nodes = vec![Node::new(1).await, Node::new(2).await, Node::new(3).await];
    let members = nodes
        .iter()
        .map(|node| (node.id, node.address.clone()))
        .collect();
    nodes[0].raft().initialize(members).await.unwrap();
    let first_leader = leader(&nodes).await;
    nodes[first_leader]
        .raft()
        .commit(commit("initial", 0, 1))
        .await
        .unwrap();
    for node in &nodes {
        wait_revision(node, 1).await;
    }
    let lagging = (first_leader + 1) % 3;
    nodes[lagging].stop().await;
    let text = "x".repeat(1000);
    for revision in 1..3 {
        let puts = (0..1000)
            .map(|n| (format!(r#"source:["blobs","{revision}-{n:04}"]"#), json!(text)))
            .collect();
        let command = Commit {
            internal: false,
            request_id: format!("blobs-{revision}"),
            fingerprint: format!("fingerprint-blobs-{revision}"),
            expected_revision: revision,
            puts,
            deletes: vec![],
            result: json!(null),
        };
        nodes[first_leader].raft().commit(command).await.unwrap();
    }
    let raft = &nodes[first_leader].raft().raft;
    raft.trigger().snapshot().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let snapshot_index = loop {
        let metrics = nodes[first_leader].raft().metrics();
        if metrics.snapshot == metrics.last_applied {
            break metrics.snapshot.unwrap().index;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    raft.trigger().purge_log(snapshot_index).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes[first_leader].raft().metrics().purged.map(|id| id.index) < Some(snapshot_index) {
        assert!(Instant::now() < deadline, "logs were not purged");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    nodes[lagging].restart().await;
    let deadline = Instant::now() + Duration::from_secs(60);
    // Installing publishes the state before Raft's metrics show the snapshot.
    while nodes[lagging].raft().local_snapshot().await.revision != 3
        || nodes[lagging].raft().metrics().snapshot.is_none()
    {
        assert!(
            Instant::now() < deadline,
            "the lagging follower did not catch up through a snapshot: {:?}",
            nodes[lagging].raft().metrics()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_snapshot_failover_restart_and_quorum_loss() {
    let mut nodes = vec![Node::new(1).await, Node::new(2).await, Node::new(3).await];
    let members = nodes
        .iter()
        .map(|node| (node.id, node.address.clone()))
        .collect();
    nodes[0].raft().initialize(members).await.unwrap();
    let first_leader = leader(&nodes).await;
    nodes[first_leader]
        .raft()
        .commit(commit("initial", 0, 1))
        .await
        .unwrap();
    for node in &nodes {
        wait_revision(node, 1).await;
    }

    let lagging = (first_leader + 1) % 3;
    nodes[lagging].stop().await;
    for revision in 1..6 {
        nodes[first_leader]
            .raft()
            .commit(commit(&format!("next-{revision}"), revision, revision + 1))
            .await
            .unwrap();
    }
    nodes[first_leader]
        .raft()
        .raft
        .trigger()
        .snapshot()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let snapshot_index = loop {
        let metrics = nodes[first_leader].raft().metrics();
        if metrics.snapshot == metrics.last_applied {
            break metrics.snapshot.unwrap().index;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    nodes[first_leader]
        .raft()
        .raft
        .trigger()
        .purge_log(snapshot_index)
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if nodes[first_leader]
            .raft()
            .metrics()
            .purged
            .map(|id| id.index)
            .unwrap_or(0)
            >= snapshot_index
        {
            break;
        }
        assert!(Instant::now() < deadline, "logs were not purged");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    nodes[lagging].restart().await;
    wait_revision(&nodes[lagging], 6).await;
    assert_eq!(
        nodes[lagging].raft().local_snapshot().await,
        nodes[first_leader].raft().read().await.unwrap()
    );
    assert!(
        nodes[lagging].raft().metrics().snapshot.is_some(),
        "lagging follower should have installed a snapshot"
    );

    nodes[first_leader].stop().await;
    let second_leader = leader(&nodes).await;
    assert_ne!(second_leader, first_leader);
    nodes[second_leader]
        .raft()
        .commit(commit("after-failover", 6, 7))
        .await
        .unwrap();
    let duplicate = nodes[second_leader]
        .raft()
        .commit(commit("initial", 0, 1))
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, 1);
    assert_eq!(duplicate.result, json!({"written": 1}));
    nodes[first_leader].restart().await;
    for node in &nodes {
        wait_revision(node, 7).await;
    }

    for node in &mut nodes {
        node.stop().await;
    }
    for node in &mut nodes {
        node.restart().await;
    }
    let restarted_leader = leader(&nodes).await;
    let restored = nodes[restarted_leader].raft().read().await.unwrap();
    assert_eq!(restored.revision, 7);
    assert_eq!(restored.requests["initial"].revision, 1);
    assert_eq!(restored.requests["initial"].result, json!({"written": 1}));
    assert_eq!(restored.data["cell:[\"double\",null]"]["value"], json!(14));
    nodes[restarted_leader]
        .raft()
        .commit(commit("after-restart", 7, 8))
        .await
        .unwrap();
    for node in &nodes {
        wait_revision(node, 8).await;
    }

    for (index, node) in nodes.iter_mut().enumerate() {
        if index != restarted_leader {
            node.stop().await;
        }
    }
    assert!(
        nodes[restarted_leader].raft().read().await.is_err(),
        "a minority cannot serve a strong read"
    );
    assert!(
        nodes[restarted_leader]
            .raft()
            .commit(commit("no-quorum", 8, 9))
            .await
            .is_err(),
        "a minority cannot acknowledge a write"
    );
    assert_eq!(
        nodes[restarted_leader]
            .raft()
            .local_snapshot()
            .await
            .revision,
        8
    );
    nodes[restarted_leader].stop().await;
}

#[tokio::test]
async fn distinct_peer_credentials_cannot_administer_and_operator_cannot_send_raft() {
    let directory = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let consensus = Consensus::open_with_tokens(
        1,
        address.to_string(),
        directory.path().into(),
        "operator-only".into(),
        "peer-only".into(),
    )
    .await
    .unwrap();
    let router = consensus.router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    for route in ["metrics", "membership"] {
        assert_eq!(
            client
                .get(format!("http://{address}/raft/{route}"))
                .bearer_auth("peer-only")
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        assert!(
            client
                .get(format!("http://{address}/raft/{route}"))
                .bearer_auth("operator-only")
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }
    assert_eq!(
        client
            .post(format!("http://{address}/raft/initialize"))
            .bearer_auth("peer-only")
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let request = |token: &str| {
        client
            .get(format!("http://{address}/raft/version"))
            .bearer_auth(token)
            .header(super::RPC_TARGET_HEADER, "1")
            .header(
                super::membership::COMPATIBILITY_HEADER,
                super::membership::contract(),
            )
    };
    assert_eq!(request("operator-only").send().await.unwrap().status(), 401);
    assert!(
        request("peer-only")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    consensus.shutdown().await.unwrap();
    server.abort();
}

/// Two groups on three hosts, each host serving one replica of both from one
/// shared database: a host outage and full restart keep the groups apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_hosted_on_shared_databases_survive_host_loss_and_restart() {
    let directories: Vec<TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let hosts: Vec<Host> = directories.iter().map(Host::new).collect();
    let mut groups = Vec::new();
    for name in ["orders/", "billing/"] {
        let mut nodes = Vec::new();
        for (index, host) in hosts.iter().enumerate() {
            nodes.push(Node::hosted(index as u64 + 1, host, name).await);
        }
        nodes[0]
            .raft()
            .initialize(
                nodes
                    .iter()
                    .map(|node| (node.id, node.address.clone()))
                    .collect(),
            )
            .await
            .unwrap();
        groups.push(nodes);
    }
    for (group, nodes) in groups.iter().enumerate() {
        let leader = leader(nodes).await;
        let value = group as u64 * 10;
        nodes[leader]
            .raft()
            .commit_many(vec![commit("a", 0, value + 1), commit("b", 1, value + 2)])
            .await
            .unwrap();
    }
    // Lose host 1: both groups keep serving on the other two.
    for nodes in &mut groups {
        nodes[0].stop().await;
    }
    hosts[0].forget();
    for (group, nodes) in groups.iter().enumerate() {
        let leader = leader(nodes).await;
        assert_ne!(leader, 0);
        nodes[leader]
            .raft()
            .commit(commit("c", 2, group as u64 * 10 + 3))
            .await
            .unwrap();
    }
    for nodes in &mut groups {
        nodes[0].restart().await;
    }
    for nodes in &groups {
        for node in nodes {
            wait_revision(node, 3).await;
        }
    }
    // Stop every host, then bring them all back.
    for nodes in &mut groups {
        for node in nodes.iter_mut() {
            node.stop().await;
        }
    }
    for host in &hosts {
        host.forget();
    }
    for nodes in &mut groups {
        for node in nodes.iter_mut() {
            node.restart().await;
        }
    }
    for (group, nodes) in groups.iter().enumerate() {
        let leader = leader(nodes).await;
        let state = nodes[leader].raft().read().await.unwrap();
        assert_eq!(state.revision, 3);
        assert_eq!(
            state.data.get("source:[\"counter\",\"one\"]"),
            Some(&json!(group as u64 * 10 + 3))
        );
        assert!(
            nodes[leader]
                .raft()
                .commit(commit("a", 0, 1))
                .await
                .unwrap()
                .duplicate
        );
    }
    for nodes in &mut groups {
        for node in nodes.iter_mut() {
            node.stop().await;
        }
    }
}

/// Reopening a store whose application holds many receipts:
/// `cargo test --release --lib -- --ignored --nocapture reopen_cost_with_many_receipts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement"]
async fn reopen_cost_with_many_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut revision = 0;
    for index in 1..=200u64 {
        let batch = (0..1000)
            .map(|_| {
                revision += 1;
                commit(&format!("request-{revision}"), revision - 1, revision)
            })
            .collect();
        store.apply([group_entry(index, batch)]).await.unwrap();
    }
    store.persisted().await.unwrap();
    store.close().await.unwrap();
    drop(store);
    let started = Instant::now();
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    eprintln!(
        "reopened with {} receipts in {:.1} ms",
        reopened.snapshot().await.requests.len(),
        started.elapsed().as_secs_f64() * 1e3
    );
}
