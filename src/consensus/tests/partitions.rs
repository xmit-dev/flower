use super::*;
use crate::consensus::{
    PartitionBinding, PartitionCommand, PartitionPhase, PartitionTransfer, Snapshot,
    partition_image_digest,
};

async fn apply(store: &mut Store, index: &mut u64, command: RaftCommand) -> ApplyResult {
    *index += 1;
    store
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), *index),
            payload: EntryPayload::Normal(command),
        }])
        .await
        .unwrap()
        .remove(0)
}
fn control(command: PartitionCommand) -> RaftCommand {
    RaftCommand::PartitionControl {
        partition_control: command,
        leader_id: None,
    }
}
fn scoped(id: &str, epoch: u64, command: Commit) -> RaftCommand {
    RaftCommand::Scoped {
        partition: id.into(),
        epoch,
        command: Box::new(command.into()),
    }
}
async fn create(store: &mut Store, index: &mut u64, id: &str) {
    assert!(matches!(
        apply(
            store,
            index,
            control(PartitionCommand::Create {
                partition: id.into(),
                epoch: 1,
                operation: format!("create-{id}")
            })
        )
        .await,
        ApplyResult::Partition(_)
    ));
    assert!(
        store
            .partition_snapshot(&binding(id, 1), None, true)
            .is_err()
    );
    assert!(matches!(
        apply(
            store,
            index,
            control(PartitionCommand::Activate {
                partition: id.into(),
                epoch: 1,
                operation: format!("create-{id}")
            })
        )
        .await,
        ApplyResult::Partition(_)
    ));
}
fn binding(id: &str, epoch: u64) -> PartitionBinding {
    PartitionBinding {
        partition: id.into(),
        epoch,
    }
}
fn transfer(id: &str) -> PartitionTransfer {
    PartitionTransfer {
        partition: id.into(),
        operation: "move-1".into(),
        source: "garden-a".into(),
        destination: "garden-b".into(),
        source_epoch: 1,
        epoch: 2,
    }
}

#[tokio::test]
async fn partition_graph_cleanup_remains_deleted_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    create(&mut store, &mut index, "graph").await;
    let generation = "a".repeat(64);
    let cell = r#"cell:["leaf",null]"#;
    let root = r#"root:["leaf",null]"#;
    let value = |number| json!({"name":"leaf","args":null,
        "outcome":{"ok":true,"value":number},"deps":[]});
    let mut seed = commit("graph-seed", 0, 1);
    seed.puts = BTreeMap::from([
        (cell.into(), value(1)),
        (root.into(), json!({"name":"leaf","args":null})),
        (format!("graph:{generation}:{cell}"), value(2)),
        (format!("graph:{generation}:{root}"), json!({"name":"leaf","args":null})),
        ("reactive:active".into(), json!(generation)),
    ]);
    assert!(matches!(apply(&mut store, &mut index, scoped("graph", 1, seed)).await,
        ApplyResult::Committed(_)));
    let mut cleanup = commit("graph-cleanup", 1, 2);
    cleanup.puts.clear();
    cleanup.deletes = vec![cell.into(), root.into()];
    assert!(matches!(apply(&mut store, &mut index, scoped("graph", 1, cleanup)).await,
        ApplyResult::Committed(_)));
    store.close().await.unwrap();
    drop(store);
    let store = Store::open(1, directory.path().into()).await.unwrap();
    let snapshot = store.partition_snapshot(&binding("graph", 1), None, true).unwrap();
    assert!(snapshot.data.get_raw_shared(cell).is_none());
    assert!(snapshot.data.get_raw_shared(root).is_none());
    assert_eq!(snapshot.data.get(cell), Some(&value(2)));
    assert_eq!(snapshot.revision, 2);
}

#[tokio::test]
async fn partitions_isolate_revision_receipts_keys_and_ownership_before_replay() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    create(&mut store, &mut index, "shop").await;
    create(&mut store, &mut index, "shop\0next").await;
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            scoped("shop", 1, commit("same", 0, 10))
        )
        .await,
        ApplyResult::Committed(_)
    ));
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            scoped("shop\0next", 1, commit("same", 0, 20))
        )
        .await,
        ApplyResult::Committed(_)
    ));
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            RaftCommand::Single(commit("same", 0, 30))
        )
        .await,
        ApplyResult::Committed(_)
    ));
    let first = store
        .partition_snapshot(&binding("shop", 1), None, true)
        .unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(first.requests["same"].result, json!({"written":10}));
    assert_eq!(
        store.snapshot().await.requests["same"].result,
        json!({"written":30})
    );
    assert!(
        matches!(
            apply(
                &mut store,
                &mut index,
                scoped("shop", 2, commit("same", 0, 10))
            )
            .await,
            ApplyResult::Rejected(_)
        ),
        "stale epoch cannot even return a matching receipt"
    );
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            control(PartitionCommand::Freeze {
                transfer: transfer("shop"),
                expected_revision: 0
            })
        )
        .await,
        ApplyResult::Rejected(_)
    ));
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            control(PartitionCommand::Freeze {
                transfer: transfer("shop"),
                expected_revision: 1
            })
        )
        .await,
        ApplyResult::Partition(_)
    ));
    assert!(
        store
            .partition_snapshot(&binding("shop", 1), None, true)
            .is_err()
    );
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            scoped("shop", 1, commit("same", 0, 10))
        )
        .await,
        ApplyResult::Rejected(_)
    ));
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            scoped("shop\0next", 1, commit("still-running", 1, 21))
        )
        .await,
        ApplyResult::Committed(_)
    ));
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            control(PartitionCommand::Retire {
                transfer: transfer("shop")
            })
        )
        .await,
        ApplyResult::Partition(_)
    ));
    store.close().await.unwrap();
    drop(store);
    let store = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(
        store.partition_info("shop").unwrap().phase,
        PartitionPhase::Retired
    );
    assert!(
        store
            .partition_state("shop")
            .unwrap()
            .snapshot
            .data
            .is_empty()
    );
    assert_eq!(
        store
            .partition_snapshot(&binding("shop\0next", 1), None, true)
            .unwrap()
            .revision,
        2
    );
    assert_eq!(store.snapshot().await.revision, 1);
}

#[tokio::test]
async fn partition_import_resumes_after_restart_preserves_entire_state_and_survives_raft_snapshot()
{
    let source_dir = tempfile::tempdir().unwrap();
    let destination_dir = tempfile::tempdir().unwrap();
    let restored_dir = tempfile::tempdir().unwrap();
    let mut source = Store::open(1, source_dir.path().into()).await.unwrap();
    let mut source_index = 0;
    create(&mut source, &mut source_index, "東京🌸").await;
    let mut original = commit("already-acknowledged", 0, 11);
    original.puts.insert(
        "source:[\"leases\",\"work\"]".into(),
        json!({"owner":"worker","fence":19,"expiresAt":100000}),
    );
    original.puts.insert(
        "source:[\"timers\",\"later\"]".into(),
        json!({"runAt":120000,"handler":"expire"}),
    );
    original.puts.insert(
        "bundle".into(),
        json!({"hash":"original","javascript":"const flower = '🌸';"}),
    );
    assert!(matches!(
        apply(
            &mut source,
            &mut source_index,
            scoped("東京🌸", 1, original.clone())
        )
        .await,
        ApplyResult::Committed(_)
    ));
    let expected = source
        .partition_snapshot(&binding("東京🌸", 1), None, true)
        .unwrap();
    let transfer = transfer("東京🌸");
    apply(
        &mut source,
        &mut source_index,
        control(PartitionCommand::Freeze {
            transfer: transfer.clone(),
            expected_revision: 1,
        }),
    )
    .await;
    let image = serde_json::to_string(&expected).unwrap();
    let chunks = image
        .chars()
        .collect::<Vec<_>>()
        .chunks(37)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect::<Vec<_>>();
    let mut destination = Store::open(1, destination_dir.path().into()).await.unwrap();
    let mut index = 0;
    let begin = PartitionCommand::BeginImport {
        transfer: transfer.clone(),
        revision: 1,
        digest: partition_image_digest(image.as_bytes()),
        bytes: image.len() as u64,
    };
    assert!(matches!(
        apply(&mut destination, &mut index, control(begin.clone())).await,
        ApplyResult::Partition(_)
    ));
    let chunk = |index, data| PartitionCommand::ImportChunk {
        partition: transfer.partition.clone(),
        epoch: 2,
        operation: transfer.operation.clone(),
        index,
        data,
    };
    apply(
        &mut destination,
        &mut index,
        control(chunk(0, chunks[0].clone())),
    )
    .await;
    destination.close().await.unwrap();
    drop(destination);
    let mut destination = Store::open(1, destination_dir.path().into()).await.unwrap();
    assert_eq!(
        destination
            .partition_info(&transfer.partition)
            .unwrap()
            .next_chunk,
        1
    );
    assert!(matches!(
        apply(
            &mut destination,
            &mut index,
            control(chunk(0, chunks[0].clone()))
        )
        .await,
        ApplyResult::Partition(_)
    ));
    assert!(matches!(
        apply(
            &mut destination,
            &mut index,
            control(chunk(0, "different".into()))
        )
        .await,
        ApplyResult::Rejected(_)
    ));
    assert!(matches!(
        apply(
            &mut destination,
            &mut index,
            control(chunk(2, chunks[2].clone()))
        )
        .await,
        ApplyResult::Rejected(_)
    ));
    let seal = PartitionCommand::SealImport {
        partition: transfer.partition.clone(),
        epoch: 2,
        operation: transfer.operation.clone(),
    };
    assert!(matches!(
        apply(&mut destination, &mut index, control(seal.clone())).await,
        ApplyResult::Rejected(_)
    ));
    for (offset, data) in chunks.into_iter().enumerate().skip(1) {
        assert!(matches!(
            apply(
                &mut destination,
                &mut index,
                control(chunk(offset as u64, data))
            )
            .await,
            ApplyResult::Partition(_)
        ));
    }
    assert!(matches!(
        apply(&mut destination, &mut index, control(seal)).await,
        ApplyResult::Partition(_)
    ));
    assert!(
        destination
            .partition_snapshot(&binding(&transfer.partition, 2), None, true)
            .is_err()
    );
    let activate = PartitionCommand::Activate {
        partition: transfer.partition.clone(),
        epoch: 2,
        operation: transfer.operation.clone(),
    };
    assert!(matches!(
        apply(&mut destination, &mut index, control(activate)).await,
        ApplyResult::Partition(_)
    ));
    assert_eq!(
        destination
            .partition_snapshot(&binding(&transfer.partition, 2), None, true)
            .unwrap(),
        expected
    );
    assert!(
        matches!(apply(&mut destination,&mut index,scoped(&transfer.partition,2,original)).await,ApplyResult::Committed(result) if result.duplicate&&result.revision==1)
    );
    let built = destination.build_snapshot().await.unwrap();
    let mut restored = Store::open(1, restored_dir.path().into()).await.unwrap();
    restored
        .install_snapshot(&built.meta, built.snapshot)
        .await
        .unwrap();
    assert_eq!(
        restored
            .partition_snapshot(&binding(&transfer.partition, 2), None, true)
            .unwrap(),
        expected
    );
    restored.close().await.unwrap();
    drop(restored);
    let restored = Store::open(1, restored_dir.path().into()).await.unwrap();
    assert_eq!(
        restored
            .partition_snapshot(&binding(&transfer.partition, 2), None, true)
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn partition_freeze_rejects_in_doubt_transactions_and_control_checks_full_leader_identity() {
    for record in [
        "transaction:participant",
        "transaction:coordinator:unfinished",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        let mut index = 0;
        create(&mut store, &mut index, "shop").await;
        let mut command = commit("pending", 0, 1);
        command
            .puts
            .insert(record.into(), json!({"phase":"preparing","complete":false}));
        apply(&mut store, &mut index, scoped("shop", 1, command)).await;
        let freeze = PartitionCommand::Freeze {
            transfer: transfer("shop"),
            expected_revision: 1,
        };
        assert!(matches!(
            apply(&mut store, &mut index, control(freeze)).await,
            ApplyResult::Rejected(_)
        ));
        assert_eq!(
            store.partition_info("shop").unwrap().phase,
            PartitionPhase::Active
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    let create = PartitionCommand::Create {
        partition: "shop".into(),
        epoch: 1,
        operation: "op".into(),
    };
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            RaftCommand::PartitionControl {
                partition_control: create.clone(),
                leader_id: Some(CommittedLeaderId::new(1, 2))
            }
        )
        .await,
        ApplyResult::Rejected(_)
    ));
    assert!(store.partition_info("shop").is_err());
    assert!(matches!(
        apply(
            &mut store,
            &mut index,
            RaftCommand::PartitionControl {
                partition_control: create,
                leader_id: Some(CommittedLeaderId::new(1, 1))
            }
        )
        .await,
        ApplyResult::Partition(_)
    ));
}

#[test]
fn partition_wire_rejects_nested_scopes_before_recursive_decode() {
    let command = scoped("shop", 1, commit("request", 0, 1));
    let encoded = serde_json::to_string(&command).unwrap();
    assert_eq!(
        serde_json::from_str::<RaftCommand>(&encoded).unwrap(),
        command
    );
    let nested = json!({"partition":"other","epoch":1,"command":command});
    assert!(serde_json::from_value::<RaftCommand>(nested).is_err());
    let malformed = Snapshot {
        revision: 0,
        requests: crate::consensus::Receipts::from([(
            "bad".into(),
            crate::consensus::Receipt {
                fingerprint: "x".into(),
                revision: 1,
                result: json!(null),
                epoch: None,
            },
        )]),
        ..Snapshot::default()
    };
    let state = crate::consensus::partitions::PartitionState {
        info: crate::consensus::PartitionInfo {
            partition: "x".into(),
            epoch: 1,
            phase: PartitionPhase::Active,
            operation: "op".into(),
            transfer: None,
            revision: 0,
            digest: None,
            bytes: 0,
            received_bytes: 0,
            next_chunk: 0,
            base_bytes: 0,
        },
        snapshot: malformed,
        base: None,
        last_import: None,
        chunks: crate::consensus::Records::default(),
    };
    assert!(crate::consensus::partitions::validate_state(&state).is_err());
}

#[tokio::test]
async fn partition_images_reject_hash_revision_receipt_transaction_and_graph_corruption() {
    for invalid in ["digest", "revision", "receipt", "transaction", "graph"] {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        let mut index = 0;
        let mut snapshot = Snapshot {
            revision: 1,
            ..Snapshot::default()
        };
        snapshot
            .data
            .insert("source:[\"records\",\"flower\"]".into(), json!("🌸"));
        if invalid == "receipt" {
            snapshot.requests.insert(
                "invalid".into(),
                crate::consensus::Receipt {
                    fingerprint: "fingerprint".into(),
                    revision: 2,
                    result: json!(null),
                    epoch: None,
                },
            );
        }
        if invalid == "transaction" {
            snapshot
                .data
                .insert("transaction:participant".into(), json!({"prepared":true}));
        }
        if invalid == "graph" {
            snapshot.data.insert("reactive:active".into(), json!("invalid-generation"));
        }
        let bytes = serde_json::to_string(&snapshot).unwrap();
        let transfer = transfer("invalid-import");
        let digest = if invalid == "digest" {
            "0".repeat(64)
        } else {
            partition_image_digest(bytes.as_bytes())
        };
        apply(
            &mut store,
            &mut index,
            control(PartitionCommand::BeginImport {
                transfer: transfer.clone(),
                revision: if invalid == "revision" { 2 } else { 1 },
                digest,
                bytes: bytes.len() as u64,
            }),
        )
        .await;
        apply(
            &mut store,
            &mut index,
            control(PartitionCommand::ImportChunk {
                partition: transfer.partition.clone(),
                epoch: 2,
                operation: transfer.operation.clone(),
                index: 0,
                data: bytes,
            }),
        )
        .await;
        assert!(
            matches!(
                apply(
                    &mut store,
                    &mut index,
                    control(PartitionCommand::SealImport {
                        partition: transfer.partition.clone(),
                        epoch: 2,
                        operation: transfer.operation,
                    })
                )
                .await,
                ApplyResult::Rejected(_)
            ),
            "invalid {invalid} was accepted"
        );
        assert_eq!(
            store.partition_info(&transfer.partition).unwrap().phase,
            PartitionPhase::Importing
        );
        assert!(
            store
                .partition_snapshot(&binding(&transfer.partition, 2), None, true)
                .is_err()
        );
    }
}

#[tokio::test]
async fn scoped_atomic_batches_preserve_replay_rollback_and_independent_partitions() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    create(&mut store, &mut index, "shop").await;
    create(&mut store, &mut index, "other").await;
    let commands = vec![commit("a", 0, 1), commit("b", 1, 2)];
    let batch = |commands| RaftCommand::Scoped {
        partition: "shop".into(),
        epoch: 1,
        command: Box::new(RaftCommand::Batch {
            batch: CompactBatch::new(commands).unwrap(),
        }),
    };
    assert!(
        matches!(apply(&mut store, &mut index, batch(commands.clone())).await,
        ApplyResult::Batch(results) if results.len() == 2 && results.iter().all(|r|matches!(r,ApplyResult::Committed(_))))
    );
    let original = store
        .partition_snapshot(&binding("shop", 1), None, true)
        .unwrap();
    assert!(
        matches!(apply(&mut store, &mut index, batch(commands)).await,
        ApplyResult::Batch(results) if results.iter().all(|r|matches!(r,ApplyResult::Committed(value) if value.duplicate)))
    );
    assert!(
        matches!(apply(&mut store, &mut index, batch(vec![commit("fresh", 2, 3), commit("a", 3, 4)])).await,
        ApplyResult::Batch(results) if results.iter().all(|r|matches!(r,ApplyResult::Rejected(_))))
    );
    assert_eq!(
        store
            .partition_snapshot(&binding("shop", 1), None, true)
            .unwrap(),
        original
    );
    assert_eq!(
        store
            .partition_snapshot(&binding("other", 1), None, true)
            .unwrap()
            .revision,
        0
    );
    let fenced = RaftCommand::Scoped {
        partition: "shop".into(),
        epoch: 1,
        command: Box::new(RaftCommand::Fenced {
            leader_id: CommittedLeaderId::new(1, 2),
            commit: commit("wrong-leader", 2, 3),
        }),
    };
    assert!(matches!(
        apply(&mut store, &mut index, fenced).await,
        ApplyResult::Rejected(_)
    ));
    assert_eq!(
        store
            .partition_snapshot(&binding("shop", 1), None, true)
            .unwrap(),
        original
    );
}

mod copy;

/// Applies move only the partitions holding records in memory onto newer
/// snapshots, and every settled one within a bounded number of rounds, so
/// that none pins an old snapshot for long.
#[tokio::test]
async fn applies_rebase_written_partitions_and_every_settled_one_eventually() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    for id in ["p0", "p1", "p2"] {
        create(&mut store, &mut index, id).await;
    }
    let backing = |store: &Store, id: &str| {
        let snapshot = store.partition_snapshot(&binding(id, 1), None, true).unwrap();
        snapshot.data.backing().cloned().unwrap()
    };
    store.persisted().await.unwrap();
    apply(&mut store, &mut index, commit("root-0", 0, 1).into()).await;
    let before: Vec<_> = ["p0", "p2"].iter().map(|id| backing(&store, id)).collect();
    let result = apply(&mut store, &mut index, scoped("p1", 1, commit("write", 0, 7))).await;
    assert!(matches!(result, ApplyResult::Committed(_)), "{result:?}");
    store.persisted().await.unwrap();
    apply(&mut store, &mut index, commit("root-1", 1, 2).into()).await;
    let written = store.partition_snapshot(&binding("p1", 1), None, true).unwrap();
    assert!(written.data.is_settled(), "the written partition moves on the next rebase");
    assert_eq!(written.data["source:[\"counter\",\"one\"]"], 7);
    for revision in 2..(2 + 64) {
        store.persisted().await.unwrap();
        let command = commit(&format!("root-{revision}"), revision, revision + 1);
        apply(&mut store, &mut index, command.into()).await;
    }
    for (id, before) in ["p0", "p2"].iter().zip(before) {
        assert!(!Arc::ptr_eq(&before, &backing(&store, id)), "{id} still pins its old snapshot");
    }
}

/// Root applies whose persistence advanced, with few and with many named
/// partitions: `cargo test --release --lib -- --ignored --nocapture
/// root_apply_cost_with_many_partitions`.
#[tokio::test]
#[ignore = "measurement"]
async fn root_apply_cost_with_many_partitions() {
    for partitions in [1, 2000] {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        let mut index = 0;
        for n in 0..partitions {
            create(&mut store, &mut index, &format!("p{n:05}")).await;
        }
        store.persisted().await.unwrap();
        let rounds = 300u64;
        let mut spent = Duration::ZERO;
        for revision in 0..rounds {
            // Let persistence advance, so that the apply rebases.
            store.persisted().await.unwrap();
            let command = commit(&format!("root-{revision}"), revision, revision);
            let started = Instant::now();
            apply(&mut store, &mut index, command.into()).await;
            spent += started.elapsed();
        }
        eprintln!(
            "{partitions} partitions: {:.1} µs per root apply",
            spent.as_secs_f64() * 1e6 / rounds as f64
        );
    }
}
