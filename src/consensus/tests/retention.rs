use super::*;
mod sessions;
use crate::consensus::retention::{self as policy, Action, Command};
use crate::consensus::{
    PartitionBinding, PartitionCommand, PartitionTransfer, partition_image_digest,
};

fn raft_entry(index: u64, command: RaftCommand) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}
fn control(revision: u64, action: Action) -> RaftCommand {
    RaftCommand::Retention {
        retention: Command {
            expected_revision: revision,
            action,
        },
    }
}
async fn initialize(store: &mut Store, index: u64, budget: Option<u64>) -> policy::State {
    let state = store.snapshot().await;
    let command = policy::initialize(state.revision, budget).unwrap();
    let encoded = serde_json::to_vec(&RaftCommand::Retention {
        retention: command.clone(),
    })
    .unwrap();
    let decoded: RaftCommand = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, RaftCommand::Retention { retention: command });
    let result = store.apply([raft_entry(index, decoded)]).await.unwrap();
    assert!(matches!(result[0], ApplyResult::Committed(_)), "{result:?}");
    policy::status(&store.snapshot().await).unwrap().unwrap()
}
fn advance(revision: u64, state: &policy::State, epoch: u64) -> RaftCommand {
    control(
        revision,
        Action::Advance {
            incarnation: state.incarnation.clone(),
            current_epoch: epoch,
            min_epoch: epoch,
        },
    )
}
fn collect(revision: u64, state: &policy::State, limit: usize) -> RaftCommand {
    control(
        revision,
        Action::Collect {
            incarnation: state.incarnation.clone(),
            limit,
        },
    )
}
fn rejected(result: &ApplyResult, code: &str) {
    assert!(
        matches!(result, ApplyResult::Rejected(error) if error.contains(code)),
        "expected {code}, got {result:?}"
    );
}

#[tokio::test]
async fn retention_floor_precedes_receipt_lookup_and_survives_gc_snapshot_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let initial = initialize(&mut store, 1, None).await;
    let old_id = policy::scope_request_id(&initial, "same business intent");
    let original = commit(&old_id, 1, 10);
    let mut next = initial.clone();
    next.current_epoch = 1;
    let new_id = policy::scope_request_id(&next, "new intent");
    // One physical storage transaction includes insertion, retirement, deletion,
    // a delayed replay and a new request. Admission must see preceding deltas.
    let results = store
        .apply([
            entry(2, original.clone()),
            raft_entry(3, advance(2, &initial, 1)),
            entry(4, original.clone()),
            raft_entry(5, collect(3, &initial, 1)),
            entry(6, commit(&old_id, 4, 999)),
            entry(7, commit(&new_id, 4, 20)),
        ])
        .await
        .unwrap();
    rejected(&results[2], "RETRY_WINDOW_EXPIRED");
    rejected(&results[4], "RETRY_WINDOW_EXPIRED");
    assert!(matches!(results[5], ApplyResult::Committed(_)));
    let expected = store.snapshot().await;
    assert_eq!(expected.revision, 5);
    assert_eq!(expected.requests.len(), 1);
    assert!(!expected.requests.contains_key(&old_id));
    policy::validate_snapshot(&expected).unwrap();
    let snapshot = store.build_snapshot().await.unwrap();
    store.close().await.unwrap();
    drop(store);
    let mut reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, expected);
    let result = reopened
        .apply([entry(8, commit(&old_id, expected.revision, 999))])
        .await
        .unwrap();
    rejected(&result[0], "RETRY_WINDOW_EXPIRED");
    let follower_dir = tempfile::tempdir().unwrap();
    let mut follower = Store::open(2, follower_dir.path().into()).await.unwrap();
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(follower.snapshot().await, expected);
    let result = follower.apply([entry(8, original)]).await.unwrap();
    rejected(&result[0], "RETRY_WINDOW_EXPIRED");
    assert_eq!(follower.snapshot().await, expected);
}

#[tokio::test]
async fn retention_atomic_batch_rejects_retired_or_over_budget_member_without_partial_effects() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut policy = initialize(&mut store, 1, Some(750)).await;
    let old_id = policy::scope_request_id(&policy, "old");
    store
        .apply([raft_entry(2, advance(1, &policy, 1))])
        .await
        .unwrap();
    policy.current_epoch = 1;
    let current_id = policy::scope_request_id(&policy, "current");
    let before = store.snapshot().await;
    let expired = store
        .apply([group_entry(
            3,
            vec![commit(&current_id, 2, 20), commit(&old_id, 3, 30)],
        )])
        .await
        .unwrap();
    let ApplyResult::Batch(results) = &expired[0] else {
        panic!("{expired:?}")
    };
    for result in results {
        rejected(result, "RETRY_WINDOW_EXPIRED");
    }
    assert_eq!(store.snapshot().await, before);
    let second_id = policy::scope_request_id(&policy, "second");
    let mut first = commit(&current_id, 2, 20);
    let mut second = commit(&second_id, 3, 30);
    first.result = json!("x".repeat(300));
    second.result = json!("x".repeat(300));
    let oversize = store
        .apply([group_entry(4, vec![first, second])])
        .await
        .unwrap();
    let ApplyResult::Batch(results) = &oversize[0] else {
        panic!("{oversize:?}")
    };
    for result in results {
        rejected(result, "RECEIPT_BUDGET_EXCEEDED");
    }
    assert_eq!(store.snapshot().await, before);
}

#[tokio::test]
async fn retention_reservations_protect_irrevocable_transaction_results() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let initial = initialize(&mut store, 1, Some(2000)).await;
    let mut reserve = commit("internal", 1, 0);
    reserve.internal = true;
    reserve.puts = BTreeMap::from([(policy::RESERVED_BYTES.into(), json!(1600))]);
    store.apply([entry(2, reserve)]).await.unwrap();
    let state = store.snapshot().await;
    let id = policy::scope_request_id(&initial, "final");
    let mut final_commit = commit(&id, state.revision, 10);
    final_commit.result = json!("x".repeat(800));
    assert!(
        policy::validate_capacity_for(
            &state,
            &id,
            &final_commit.fingerprint,
            &final_commit.result,
            0
        )
        .is_err()
    );
    policy::validate_capacity_for(
        &state,
        &id,
        &final_commit.fingerprint,
        &final_commit.result,
        1600,
    )
    .unwrap();
    let blocked = store.apply([entry(3, final_commit.clone())]).await.unwrap();
    rejected(&blocked[0], "RECEIPT_BUDGET_EXCEEDED");
    assert_eq!(store.snapshot().await, state);
    let reduce = control(
        2,
        Action::SetBudget {
            incarnation: initial.incarnation.clone(),
            max_receipt_bytes: Some(1000),
        },
    );
    let blocked = store.apply([raft_entry(4, reduce)]).await.unwrap();
    rejected(&blocked[0], "RECEIPT_BUDGET_EXCEEDED");
    final_commit.deletes.push(policy::RESERVED_BYTES.into());
    let committed = store.apply([entry(5, final_commit.clone())]).await.unwrap();
    assert!(matches!(committed[0], ApplyResult::Committed(_)));
    let actual = store.snapshot().await;
    policy::validate_snapshot(&actual).unwrap();
    assert!(!actual.data.contains_key(policy::RESERVED_BYTES));
    assert_eq!(actual.requests[&id].result, final_commit.result);
}

#[tokio::test]
async fn retention_collection_is_incremental_and_does_not_collect_transaction_history() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, commit("a legacy receipt", 0, 1))])
        .await
        .unwrap();
    let initial = initialize(&mut store, 2, None).await;
    let ids: Vec<_> = (0..3)
        .map(|i| policy::scope_request_id(&initial, &i.to_string()))
        .collect();
    for (i, id) in ids.iter().enumerate() {
        let mut command = commit(id, i as u64 + 2, i as u64);
        command.puts.insert(
            format!("transaction:completed:{i}"),
            json!({"phase":"commit"}),
        );
        store.apply([entry(i as u64 + 3, command)]).await.unwrap();
    }
    store
        .apply([raft_entry(6, advance(5, &initial, 1))])
        .await
        .unwrap();
    let mut previous_count = 4;
    for i in 0..4 {
        store
            .apply([raft_entry(7 + i, collect(6 + i, &initial, 1))])
            .await
            .unwrap();
        let state = store.snapshot().await;
        assert!(state.requests.len() >= previous_count - 1);
        previous_count = state.requests.len();
        policy::validate_snapshot(&state).unwrap();
        for i in 0..3 {
            assert!(
                state
                    .data
                    .contains_key(&format!("transaction:completed:{i}"))
            );
        }
    }
    let final_state = store.snapshot().await;
    // Receipts from before initialization count as its first epoch, so the
    // floor retires the legacy one too, and its ID is new again.
    assert!(final_state.requests.is_empty());
    assert!(policy::status(&final_state).unwrap().unwrap().gc_complete);
    policy::validate_request(&final_state, "a legacy receipt").unwrap();
}

#[tokio::test]
async fn a_callers_own_id_deduplicates_across_epochs_until_the_floor_passes_it() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let initial = initialize(&mut store, 1, None).await;
    let original = commit("slack:T1:C1:1700000000.000100", 1, 10);
    let widen = control(
        2,
        Action::Advance {
            incarnation: initial.incarnation.clone(),
            current_epoch: 1,
            min_epoch: 0,
        },
    );
    let results = store
        .apply([
            entry(2, original.clone()),
            raft_entry(3, widen),
            entry(4, original.clone()),
        ])
        .await
        .unwrap();
    assert!(
        matches!(&results[2], ApplyResult::Committed(result) if result.duplicate && result.revision == 2),
        "{results:?}"
    );
    let state = store.snapshot().await;
    assert_eq!(state.requests[&original.request_id].epoch, Some(0));
    policy::validate_snapshot(&state).unwrap();
    store
        .apply([raft_entry(5, collect(3, &initial, 10))])
        .await
        .unwrap();
    assert!(
        store
            .snapshot()
            .await
            .requests
            .contains_key(&original.request_id)
    );
    store
        .apply([
            raft_entry(6, advance(4, &initial, 2)),
            raft_entry(7, collect(5, &initial, 10)),
        ])
        .await
        .unwrap();
    let state = store.snapshot().await;
    assert!(state.requests.is_empty());
    policy::validate_snapshot(&state).unwrap();
    // Once collected, the ID is new again.
    let again = store
        .apply([entry(8, commit(&original.request_id, 6, 11))])
        .await
        .unwrap();
    assert!(
        matches!(&again[0], ApplyResult::Committed(result) if !result.duplicate),
        "{again:?}"
    );
    assert_eq!(
        store.snapshot().await.requests[&original.request_id].epoch,
        Some(2)
    );
}

#[tokio::test]
async fn retention_history_and_metadata_cannot_be_replaced_or_forged_by_application_patches() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let initial = initialize(&mut store, 1, None).await;
    let before = store.snapshot().await;
    for (i, replacement) in [None, Some(json!({}))].into_iter().enumerate() {
        let mut command = commit("internal", 1, 0);
        command.internal = true;
        if let Some(value) = replacement {
            command.puts.insert(policy::KEY.into(), value);
        } else {
            command.deletes.push(policy::KEY.into());
        }
        let result = store.apply([entry(2 + i as u64, command)]).await.unwrap();
        rejected(&result[0], "RETENTION_METADATA_PROTECTED");
    }
    assert_eq!(store.snapshot().await, before);
    let mut other = initial.clone();
    other.incarnation = "f".repeat(32);
    let history = policy::scope_request_id(&other, "old history");
    let result = store
        .apply([entry(4, commit(&history, 1, 99))])
        .await
        .unwrap();
    rejected(&result[0], "HISTORY_MISMATCH");
    other = initial.clone();
    other.current_epoch = 1;
    let result = store
        .apply([entry(
            5,
            commit(&policy::scope_request_id(&other, "future"), 1, 99),
        )])
        .await
        .unwrap();
    rejected(&result[0], "RETRY_EPOCH_NOT_ADMITTED");
    let mut corrupt = before.clone();
    let mut corrupt_policy = corrupt.data.get(policy::KEY).unwrap().clone();
    corrupt_policy["receiptBytes"] = json!(99);
    corrupt.data.insert(policy::KEY.into(), corrupt_policy);
    assert!(policy::validate_snapshot(&corrupt).is_err());
    assert_eq!(store.snapshot().await, before);
}

#[tokio::test]
async fn retention_partition_migration_preserves_expired_identity_after_physical_collection() {
    let source_dir = tempfile::tempdir().unwrap();
    let destination_dir = tempfile::tempdir().unwrap();
    let mut source = Store::open(1, source_dir.path().into()).await.unwrap();
    let binding = PartitionBinding {
        partition: "shop🌸".into(),
        epoch: 1,
    };
    let scoped = |command| RaftCommand::Scoped {
        partition: binding.partition.clone(),
        epoch: 1,
        command: Box::new(command),
    };
    let partition_control = |command| RaftCommand::PartitionControl {
        partition_control: command,
        leader_id: None,
    };
    source
        .apply([
            raft_entry(
                1,
                partition_control(PartitionCommand::Create {
                    partition: binding.partition.clone(),
                    epoch: 1,
                    operation: "create".into(),
                }),
            ),
            raft_entry(
                2,
                partition_control(PartitionCommand::Activate {
                    partition: binding.partition.clone(),
                    epoch: 1,
                    operation: "create".into(),
                }),
            ),
            raft_entry(
                3,
                scoped(RaftCommand::Retention {
                    retention: policy::initialize(0, None).unwrap(),
                }),
            ),
        ])
        .await
        .unwrap();
    let initialized = source.partition_snapshot(&binding, None, true).unwrap();
    let initial = policy::status(&initialized).unwrap().unwrap();
    let id = policy::scope_request_id(&initial, "original");
    source
        .apply([
            raft_entry(4, scoped(commit(&id, 1, 42).into())),
            raft_entry(5, scoped(advance(2, &initial, 1))),
            raft_entry(6, scoped(collect(3, &initial, 10))),
        ])
        .await
        .unwrap();
    let expected = source.partition_snapshot(&binding, None, true).unwrap();
    assert!(expected.requests.is_empty());
    let owner = policy::owner_for("tenant-worker");
    let open = policy::open_session(&expected, &owner).unwrap();
    let result = source
        .apply([raft_entry(
            7,
            scoped(RaftCommand::Retention { retention: open }),
        )])
        .await
        .unwrap();
    let ApplyResult::Committed(opened) = &result[0] else {
        panic!("{result:?}")
    };
    let session: policy::Session =
        serde_json::from_value(opened.result["session"].clone()).unwrap();
    let latest = source.partition_snapshot(&binding, None, true).unwrap();
    let active_id =
        policy::session_request_id(&policy::status(&latest).unwrap().unwrap(), &session, 1)
            .unwrap();
    let active_commit = commit(&active_id, latest.revision, 77);
    source
        .apply([raft_entry(8, scoped(active_commit.clone().into()))])
        .await
        .unwrap();
    let expected = source.partition_snapshot(&binding, None, true).unwrap();
    let transfer = PartitionTransfer {
        partition: binding.partition.clone(),
        operation: "move".into(),
        source: "a".into(),
        destination: "b".into(),
        source_epoch: 1,
        epoch: 2,
    };
    source
        .apply([raft_entry(
            9,
            partition_control(PartitionCommand::Freeze {
                transfer: transfer.clone(),
                expected_revision: expected.revision,
            }),
        )])
        .await
        .unwrap();
    let image = serde_json::to_string(&expected).unwrap();
    let mut destination = Store::open(2, destination_dir.path().into()).await.unwrap();
    let controls = [
        PartitionCommand::BeginImport {
            transfer: transfer.clone(),
            revision: expected.revision,
            digest: partition_image_digest(image.as_bytes()),
            bytes: image.len() as u64,
        },
        PartitionCommand::ImportChunk {
            partition: binding.partition.clone(),
            epoch: 2,
            operation: "move".into(),
            index: 0,
            data: image,
        },
        PartitionCommand::SealImport {
            partition: binding.partition.clone(),
            epoch: 2,
            operation: "move".into(),
        },
        PartitionCommand::Activate {
            partition: binding.partition.clone(),
            epoch: 2,
            operation: "move".into(),
        },
    ];
    for (i, command) in controls.into_iter().enumerate() {
        let result = destination
            .apply([raft_entry(1 + i as u64, partition_control(command))])
            .await
            .unwrap();
        assert!(matches!(result[0], ApplyResult::Partition(_)), "{result:?}");
    }
    destination.close().await.unwrap();
    drop(destination);
    let mut destination = Store::open(2, destination_dir.path().into()).await.unwrap();
    let moved = PartitionBinding {
        epoch: 2,
        ..binding.clone()
    };
    assert_eq!(
        destination.partition_snapshot(&moved, None, true).unwrap(),
        expected
    );
    let result = destination
        .apply([raft_entry(
            5,
            RaftCommand::Scoped {
                partition: moved.partition.clone(),
                epoch: 2,
                command: Box::new(commit(&id, expected.revision, 99).into()),
            },
        )])
        .await
        .unwrap();
    rejected(&result[0], "RETRY_WINDOW_EXPIRED");
    let migrated = destination.partition_snapshot(&moved, None, true).unwrap();
    policy::validate_request_owner(&migrated, &active_id, &owner).unwrap();
    let scoped_here = |command| RaftCommand::Scoped {
        partition: moved.partition.clone(),
        epoch: 2,
        command: Box::new(command),
    };
    let duplicate = destination
        .apply([raft_entry(6, scoped_here(active_commit.clone().into()))])
        .await
        .unwrap();
    assert!(
        matches!(&duplicate[0], ApplyResult::Committed(result) if result.duplicate && result.result == json!({"written":77}))
    );
    let acknowledgement = Action::Acknowledge {
        incarnation: session.incarnation,
        session: session.id,
        owner,
        through: 1,
        limit: 1,
        abandon: false,
    };
    destination
        .apply([raft_entry(
            7,
            scoped_here(control(expected.revision, acknowledgement)),
        )])
        .await
        .unwrap();
    destination.close().await.unwrap();
    drop(destination);
    let mut destination = Store::open(2, destination_dir.path().into()).await.unwrap();
    let rejected_replay = destination
        .apply([raft_entry(8, scoped_here(active_commit.into()))])
        .await
        .unwrap();
    rejected(&rejected_replay[0], "ALREADY_ACKNOWLEDGED");
    policy::validate_snapshot(&destination.partition_snapshot(&moved, None, true).unwrap())
        .unwrap();
    assert!(
        policy::status(&destination.snapshot().await)
            .unwrap()
            .is_none()
    );
}

/// Reopening a store that retains many receipts under a retention policy:
/// `cargo test --release --lib -- --ignored --nocapture reopen_cost_with_retained_receipts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement"]
async fn reopen_cost_with_retained_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let state = initialize(&mut store, 1, None).await;
    let mut revision = 1;
    for index in 2..=201u64 {
        let batch = (0..1000)
            .map(|_| {
                revision += 1;
                let id = policy::scope_request_id(&state, &format!("intent-{revision}"));
                commit(&id, revision - 1, revision)
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
        "reopened with {} retained receipts in {:.1} ms",
        reopened.snapshot().await.requests.len(),
        started.elapsed().as_secs_f64() * 1e3
    );
}
