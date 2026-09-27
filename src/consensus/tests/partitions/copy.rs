use super::*;
use crate::consensus::partitions::copy::export_payload;
use crate::consensus::{ExportKind, retention as policy};
use serde_json::Value;

async fn invoke(
    store: &mut Store,
    index: &mut u64,
    command: PartitionCommand,
) -> crate::consensus::PartitionInfo {
    match apply(store, index, control(command)).await {
        ApplyResult::Partition(info) => info,
        other => panic!("{other:?}"),
    }
}
async fn payload(store: &mut Store, index: &mut u64, bytes: &str, start: usize) {
    let mut offset = start;
    let mut chunk = store.partition_info("shop").unwrap().next_chunk;
    while offset < bytes.len() {
        let mut end = (offset + 97).min(bytes.len());
        while !bytes.is_char_boundary(end) {
            end -= 1;
        }
        invoke(
            store,
            index,
            PartitionCommand::ImportChunk {
                partition: "shop".into(),
                epoch: 2,
                operation: "move-1".into(),
                index: chunk,
                data: bytes[offset..end].into(),
            },
        )
        .await;
        offset = end;
        chunk += 1;
    }
}
fn seal() -> PartitionCommand {
    PartitionCommand::SealImport {
        partition: "shop".into(),
        epoch: 2,
        operation: "move-1".into(),
    }
}
fn activate() -> PartitionCommand {
    PartitionCommand::Activate {
        partition: "shop".into(),
        epoch: 2,
        operation: "move-1".into(),
    }
}
async fn retained(store: &mut Store, index: &mut u64, action: policy::Action) {
    let revision = store.partition_state("shop").unwrap().snapshot.revision;
    let result = apply(
        store,
        index,
        RaftCommand::Scoped {
            partition: "shop".into(),
            epoch: 1,
            command: Box::new(RaftCommand::Retention {
                retention: policy::Command {
                    expected_revision: revision,
                    action,
                },
            }),
        },
    )
    .await;
    assert!(matches!(result, ApplyResult::Committed(_)), "{result:?}");
}

#[tokio::test]
async fn precopy_preserves_mutations_receipt_gc_and_durable_base_through_restart_and_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    create(&mut source, &mut index, "shop").await;
    let init = policy::initialize(0, None).unwrap();
    retained(&mut source, &mut index, init.action).await;
    let history = policy::status(&source.partition_state("shop").unwrap().snapshot)
        .unwrap()
        .unwrap();
    let first = policy::scope_request_id(&history, "first");
    let mut write = commit(&first, 1, 1);
    write
        .puts
        .insert("obsolete".into(), json!("delete after capture"));
    assert!(matches!(
        apply(&mut source, &mut index, scoped("shop", 1, write)).await,
        ApplyResult::Committed(_)
    ));
    let capture = PartitionCommand::Capture {
        transfer: transfer("shop"),
        expected_revision: 2,
        max_bytes: Some(1),
    };
    assert!(matches!(
        apply(&mut source, &mut index, control(capture)).await,
        ApplyResult::Rejected(_)
    ));
    let capture = PartitionCommand::Capture {
        transfer: transfer("shop"),
        expected_revision: 2,
        max_bytes: None,
    };
    invoke(&mut source, &mut index, capture.clone()).await;
    let base = source.partition_state("shop").unwrap();
    assert!(base.base.as_ref().unwrap().data.ptr_eq(&base.snapshot.data));
    assert!(base.info.base_bytes > 0);
    retained(
        &mut source,
        &mut index,
        policy::Action::Advance {
            incarnation: history.incarnation.clone(),
            current_epoch: 1,
            min_epoch: 1,
        },
    )
    .await;
    retained(
        &mut source,
        &mut index,
        policy::Action::Collect {
            incarnation: history.incarnation.clone(),
            limit: 100,
        },
    )
    .await;
    let history = policy::status(&source.partition_state("shop").unwrap().snapshot)
        .unwrap()
        .unwrap();
    let second = policy::scope_request_id(&history, "second");
    let mut write = commit(&second, 4, 2);
    write.deletes.push("obsolete".into());
    assert!(matches!(
        apply(&mut source, &mut index, scoped("shop", 1, write)).await,
        ApplyResult::Committed(_)
    ));
    // A retry never moves the base forward to include later writes.
    invoke(&mut source, &mut index, capture).await;
    source.close().await.unwrap();
    drop(source);
    let mut source = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(source.partition_state("shop").unwrap().base, base.base);
    let (_, _, encoded_base) =
        export_payload(source.partition_state("shop").unwrap(), ExportKind::Base).unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut destination = Store::open(2, target_dir.path().into()).await.unwrap();
    let mut target_index = 0;
    invoke(
        &mut destination,
        &mut target_index,
        PartitionCommand::BeginCopy {
            transfer: transfer("shop"),
            revision: 2,
            digest: partition_image_digest(encoded_base.as_bytes()),
            bytes: encoded_base.len() as u64,
        },
    )
    .await;
    payload(&mut destination, &mut target_index, &encoded_base, 0).await;
    assert_eq!(
        invoke(&mut destination, &mut target_index, seal())
            .await
            .phase,
        PartitionPhase::Copied
    );
    assert!(matches!(
        apply(&mut destination, &mut target_index, control(activate())).await,
        ApplyResult::Rejected(_)
    ));
    invoke(
        &mut source,
        &mut index,
        PartitionCommand::Freeze {
            transfer: transfer("shop"),
            expected_revision: 5,
        },
    )
    .await;
    let expected = source.partition_state("shop").unwrap().snapshot;
    assert!(!expected.requests.contains_key(&first));
    assert!(!expected.data.contains_key("obsolete"));
    let (revision, base_revision, difference) =
        export_payload(source.partition_state("shop").unwrap(), ExportKind::Delta).unwrap();
    let begin = PartitionCommand::BeginDelta {
        transfer: transfer("shop"),
        base_revision: base_revision.unwrap(),
        revision,
        digest: partition_image_digest(difference.as_bytes()),
        bytes: difference.len() as u64,
    };
    invoke(&mut destination, &mut target_index, begin.clone()).await;
    invoke(
        &mut destination,
        &mut target_index,
        PartitionCommand::ImportChunk {
            partition: "shop".into(),
            epoch: 2,
            operation: "move-1".into(),
            index: 0,
            data: difference[..1].into(),
        },
    )
    .await;
    let raft_image = destination.build_snapshot().await.unwrap();
    destination.close().await.unwrap();
    drop(destination);
    let mut destination = Store::open(2, target_dir.path().into()).await.unwrap();
    assert_eq!(
        destination
            .partition_state("shop")
            .unwrap()
            .snapshot
            .revision,
        2
    );
    // Raft transfer also retains the incomplete difference and captured base.
    let replica_dir = tempfile::tempdir().unwrap();
    let mut replica = Store::open(3, replica_dir.path().into()).await.unwrap();
    replica
        .install_snapshot(&raft_image.meta, raft_image.snapshot)
        .await
        .unwrap();
    assert_eq!(
        replica.partition_state("shop").unwrap(),
        destination.partition_state("shop").unwrap()
    );
    invoke(&mut destination, &mut target_index, begin.clone()).await;
    payload(&mut destination, &mut target_index, &difference, 1).await;
    assert_eq!(
        invoke(&mut destination, &mut target_index, seal())
            .await
            .phase,
        PartitionPhase::Staged
    );
    invoke(&mut destination, &mut target_index, begin).await;
    invoke(&mut destination, &mut target_index, activate()).await;
    assert_eq!(
        destination
            .partition_snapshot(&binding("shop", 2), None, true)
            .unwrap(),
        expected
    );
    policy::validate_snapshot(&expected).unwrap();
    assert!(policy::validate_request(&expected, &first).is_err());
    assert_eq!(expected.requests[&second].result, json!({"written":2}));
    invoke(
        &mut source,
        &mut index,
        PartitionCommand::Retire {
            transfer: transfer("shop"),
        },
    )
    .await;
    source.close().await.unwrap();
    drop(source);
    let source = Store::open(1, directory.path().into()).await.unwrap();
    assert!(source.partition_state("shop").unwrap().base.is_none());
    assert_eq!(source.partition_info("shop").unwrap().base_bytes, 0);
}

#[tokio::test]
async fn precopy_inflight_base_cannot_activate_and_final_delta_checks_final_digest() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = Store::open(1, directory.path().into()).await.unwrap();
    let mut index = 0;
    create(&mut source, &mut index, "shop").await;
    let mut prepare = commit("internal", 0, 1);
    prepare.internal = true;
    prepare
        .puts
        .insert("transaction:participant".into(), json!({"id":"pending"}));
    apply(&mut source, &mut index, scoped("shop", 1, prepare)).await;
    invoke(
        &mut source,
        &mut index,
        PartitionCommand::Capture {
            transfer: transfer("shop"),
            expected_revision: 1,
            max_bytes: None,
        },
    )
    .await;
    let (_, _, base) =
        export_payload(source.partition_state("shop").unwrap(), ExportKind::Base).unwrap();
    let digest = partition_image_digest(base.as_bytes());
    let target_dir = tempfile::tempdir().unwrap();
    let mut target = Store::open(2, target_dir.path().into()).await.unwrap();
    let mut ti = 0;
    invoke(
        &mut target,
        &mut ti,
        PartitionCommand::BeginCopy {
            transfer: transfer("shop"),
            revision: 1,
            digest: digest.clone(),
            bytes: base.len() as u64,
        },
    )
    .await;
    payload(&mut target, &mut ti, &base, 0).await;
    assert_eq!(
        invoke(&mut target, &mut ti, seal()).await.phase,
        PartitionPhase::Copied
    );
    for command in [
        activate(),
        PartitionCommand::FinalizeCopy {
            transfer: transfer("shop"),
            revision: 1,
            digest,
            bytes: base.len() as u64,
        },
    ] {
        assert!(matches!(
            apply(&mut target, &mut ti, control(command)).await,
            ApplyResult::Rejected(_)
        ));
    }
    assert!(matches!(
        apply(
            &mut source,
            &mut index,
            control(PartitionCommand::Freeze {
                transfer: transfer("shop"),
                expected_revision: 1
            })
        )
        .await,
        ApplyResult::Rejected(_)
    ));
    let mut finish = commit("internal", 1, 2);
    finish.internal = true;
    finish.deletes.push("transaction:participant".into());
    apply(&mut source, &mut index, scoped("shop", 1, finish)).await;
    invoke(
        &mut source,
        &mut index,
        PartitionCommand::Freeze {
            transfer: transfer("shop"),
            expected_revision: 2,
        },
    )
    .await;
    let (_, _, difference) =
        export_payload(source.partition_state("shop").unwrap(), ExportKind::Delta).unwrap();
    let mut corrupt: Value = serde_json::from_str(&difference).unwrap();
    corrupt["final_digest"] = json!("0".repeat(64));
    let corrupt = serde_json::to_string(&corrupt).unwrap();
    invoke(
        &mut target,
        &mut ti,
        PartitionCommand::BeginDelta {
            transfer: transfer("shop"),
            base_revision: 1,
            revision: 2,
            digest: partition_image_digest(corrupt.as_bytes()),
            bytes: corrupt.len() as u64,
        },
    )
    .await;
    payload(&mut target, &mut ti, &corrupt, 0).await;
    assert!(
        matches!(apply(&mut target,&mut ti,control(seal())).await,ApplyResult::Rejected(reason) if reason.contains("final digest"))
    );
    assert_eq!(
        target.partition_info("shop").unwrap().phase,
        PartitionPhase::Importing
    );
}

#[tokio::test]
async fn precopy_finalizes_unchanged_base_or_replaces_it_with_full_frozen_image() {
    for changed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = Store::open(1, directory.path().into()).await.unwrap();
        let mut index = 0;
        create(&mut source, &mut index, "shop").await;
        apply(
            &mut source,
            &mut index,
            scoped("shop", 1, commit("first", 0, 1)),
        )
        .await;
        invoke(
            &mut source,
            &mut index,
            PartitionCommand::Capture {
                transfer: transfer("shop"),
                expected_revision: 1,
                max_bytes: None,
            },
        )
        .await;
        let (_, _, base) =
            export_payload(source.partition_state("shop").unwrap(), ExportKind::Base).unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut target = Store::open(2, target_dir.path().into()).await.unwrap();
        let mut ti = 0;
        let begin = PartitionCommand::BeginCopy {
            transfer: transfer("shop"),
            revision: 1,
            digest: partition_image_digest(base.as_bytes()),
            bytes: base.len() as u64,
        };
        invoke(&mut target, &mut ti, begin.clone()).await;
        payload(&mut target, &mut ti, &base, 0).await;
        invoke(&mut target, &mut ti, seal()).await;
        if changed {
            apply(
                &mut source,
                &mut index,
                scoped("shop", 1, commit("second", 1, 999)),
            )
            .await;
        }
        let revision = if changed { 2 } else { 1 };
        invoke(
            &mut source,
            &mut index,
            PartitionCommand::Freeze {
                transfer: transfer("shop"),
                expected_revision: revision,
            },
        )
        .await;
        let (_, _, final_image) = export_payload(
            source.partition_state("shop").unwrap(),
            ExportKind::Snapshot,
        )
        .unwrap();
        let digest = partition_image_digest(final_image.as_bytes());
        if changed {
            invoke(
                &mut target,
                &mut ti,
                PartitionCommand::BeginImport {
                    transfer: transfer("shop"),
                    revision,
                    digest,
                    bytes: final_image.len() as u64,
                },
            )
            .await;
            payload(&mut target, &mut ti, &final_image, 0).await;
            invoke(&mut target, &mut ti, seal()).await;
            assert!(matches!(
                apply(&mut target, &mut ti, control(begin)).await,
                ApplyResult::Rejected(_)
            ));
        } else {
            let finalize = PartitionCommand::FinalizeCopy {
                transfer: transfer("shop"),
                revision,
                digest,
                bytes: final_image.len() as u64,
            };
            invoke(&mut target, &mut ti, finalize.clone()).await;
            invoke(&mut target, &mut ti, finalize).await;
        }
        invoke(&mut target, &mut ti, activate()).await;
        assert_eq!(
            target
                .partition_snapshot(&binding("shop", 2), None, true)
                .unwrap(),
            source.partition_state("shop").unwrap().snapshot
        );
    }
}
