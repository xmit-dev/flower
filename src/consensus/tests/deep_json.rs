use super::*;
use openraft::raft::AppendEntriesRequest;
use serde_json::Value;

fn nested(levels: usize) -> Value {
    (0..levels).fold(json!(7), |value, _| json!({"n": value}))
}

fn deep_commit(request: &str, revision: u64) -> Commit {
    let value = nested(123);
    let mut command = commit(request, revision, 1);
    command.puts = BTreeMap::from([
        ("source:[\"deep\",\"value\"]".into(), value.clone()),
        (
            "cell:[\"deep\",null]".into(),
            json!({
                "name": "deep", "args": null,
                "outcome": {"ok": true, "value": value}, "deps": []
            }),
        ),
    ]);
    command.result = nested(123);
    command
}

#[test]
fn payload_depth_is_independent_of_raft_and_snapshot_wrappers() {
    let command = deep_commit("deep", 0);
    let entry = group_entry(1, vec![command.clone()]);
    let bytes = serde_json::to_vec(&entry).unwrap();
    let decoded = serde_json::from_slice::<Entry<TypeConfig>>(&bytes)
        .expect("a valid deeply nested cell must survive a grouped Raft log entry");
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let request = AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: vec![entry],
        leader_commit: None,
    };
    let bytes = serde_json::to_vec(&request).unwrap();
    let decoded = serde_json::from_slice::<AppendEntriesRequest<TypeConfig>>(&bytes)
        .expect("peer RPC wrappers must not consume business-value depth");
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let snapshot = super::super::Snapshot {
        revision: 1,
        data: command.puts.into(),
        requests: super::super::Receipts::from([(
            "deep".into(),
            super::super::Receipt {
                fingerprint: command.fingerprint,
                revision: 1,
                result: command.result,
                epoch: None,
            },
        )]),
    };
    let wrapped = json!({"application": snapshot});
    #[derive(serde::Deserialize)]
    struct Wrapped {
        application: super::super::Snapshot,
    }
    let decoded: Wrapped = serde_json::from_slice(&serde_json::to_vec(&wrapped).unwrap()).unwrap();
    assert_eq!(decoded.application, snapshot);
}

#[test]
fn individual_payloads_keep_the_standard_json_recursion_limit() {
    let too_deep = nested(130);
    assert!(serde_json::from_slice::<Value>(&serde_json::to_vec(&too_deep).unwrap()).is_err());
    for in_result in [false, true] {
        let mut command = deep_commit("too-deep", 0);
        if in_result {
            command.result = too_deep.clone();
        } else {
            command.puts.insert("excessive".into(), too_deep.clone());
        }
        assert!(serde_json::from_slice::<Commit>(&serde_json::to_vec(&command).unwrap()).is_err());
        assert!(
            serde_json::from_slice::<Entry<TypeConfig>>(
                &serde_json::to_vec(&group_entry(1, vec![command])).unwrap()
            )
            .is_err()
        );
    }
    let receipt = super::super::Receipt {
        fingerprint: "deep".into(),
        revision: 1,
        result: too_deep.clone(),
        epoch: None,
    };
    assert!(
        serde_json::from_slice::<super::super::Receipt>(&serde_json::to_vec(&receipt).unwrap())
            .is_err()
    );
    let snapshot = super::super::Snapshot {
        revision: 1,
        data: super::super::Records::from([("deep".into(), too_deep)]),
        requests: Default::default(),
    };
    assert!(
        serde_json::from_slice::<super::super::Snapshot>(&serde_json::to_vec(&snapshot).unwrap())
            .is_err()
    );
}

#[test]
fn raw_command_decode_preserves_single_fallback_and_result_defaults() {
    let mut wire = serde_json::to_value(commit("old", 0, 1)).unwrap();
    wire["batch"] = json!("not a batch");
    wire.as_object_mut().unwrap().remove("result");
    let decoded: RaftCommand = serde_json::from_value(wire).unwrap();
    assert!(
        matches!(decoded, RaftCommand::Single(commit) if commit.request_id == "old" && commit.result.is_null())
    );
    let result = super::super::ApplyResult::Batch(vec![super::super::ApplyResult::Committed(
        super::super::CommitResult {
            revision: 1,
            duplicate: true,
            result: nested(123),
        },
    )]);
    let bytes = serde_json::to_vec(&result).unwrap();
    let decoded: ApplyResult = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

#[test]
fn recovery_barrier_requires_an_unscoped_exact_control_object() {
    let barrier = RaftCommand::RecoveryBarrier {
        recovery_barrier: (),
    };
    let wire = serde_json::to_string(&barrier).unwrap();
    assert_eq!(wire, r#"{"recovery_barrier":null}"#);
    assert_eq!(serde_json::from_str::<RaftCommand>(&wire).unwrap(), barrier);
    for malformed in [
        r#"[null]"#,
        r#"{"recovery_barrier":false}"#,
        r#"{"recovery_barrier":null,"unknown":null}"#,
        r#"{"recovery_barrier":null,"recovery_barrier":null}"#,
        r#"{"partition":"garden","epoch":1,"command":{"recovery_barrier":null}}"#,
    ] {
        assert!(
            serde_json::from_str::<RaftCommand>(malformed).is_err(),
            "{malformed}"
        );
    }
    // The new marker must not swallow an existing application command.
    let single = commit("application", 0, 7);
    let mut mixed = serde_json::to_value(&single).unwrap();
    mixed["recovery_barrier"] = Value::Null;
    assert_eq!(
        serde_json::from_value::<RaftCommand>(mixed).unwrap(),
        RaftCommand::Single(single)
    );
}

#[test]
fn command_field_prefilter_preserves_mixed_wrapper_precedence_and_fallbacks() {
    let single = commit("single", 0, 1);
    let scoped = RaftCommand::Scoped {
        partition: "garden".into(),
        epoch: 3,
        command: Box::new(RaftCommand::Single(commit("scoped", 0, 2))),
    };
    let control = RaftCommand::PartitionControl {
        partition_control: super::super::PartitionCommand::Create {
            partition: "garden".into(),
            epoch: 3,
            operation: "create".into(),
        },
        leader_id: Some(CommittedLeaderId::new(2, 1)),
    };
    let retention = RaftCommand::Retention {
        retention: super::super::retention::Command {
            expected_revision: 0,
            action: super::super::retention::Action::Initialize {
                database: "db".into(),
                incarnation: "incarnation".into(),
                max_receipt_bytes: None,
            },
        },
    };
    let batch = RaftCommand::Batch {
        batch: CompactBatch::new(vec![commit("batch", 0, 3)]).unwrap(),
    };
    let fenced = RaftCommand::Fenced {
        leader_id: CommittedLeaderId::new(2, 1),
        commit: commit("fenced", 0, 4),
    };
    let candidates = [scoped, control, retention, batch, fenced];
    let mut wire = serde_json::to_value(&single).unwrap();
    for candidate in &candidates {
        wire.as_object_mut().unwrap().extend(
            serde_json::to_value(candidate)
                .unwrap()
                .as_object()
                .unwrap()
                .clone(),
        );
    }
    // Invalid higher-priority wrappers must fall through exactly like absent
    // ones. Presence alone is never sufficient to accept a variant.
    for remove in [false, true] {
        let mut wire = wire.clone();
        for (candidate, field) in
            candidates
                .iter()
                .zip(["epoch", "partition_control", "retention", "batch", "commit"])
        {
            assert_eq!(
                serde_json::from_value::<RaftCommand>(wire.clone()).unwrap(),
                *candidate
            );
            if remove {
                wire.as_object_mut().unwrap().remove(field);
            } else {
                wire[field] = json!("invalid wrapper");
            }
        }
        assert_eq!(
            serde_json::from_value::<RaftCommand>(wire).unwrap(),
            RaftCommand::Single(single.clone())
        );
    }
    // Control's optional leader field was never required by its decoder.
    let mut wire = serde_json::to_value(&candidates[1]).unwrap();
    wire.as_object_mut().unwrap().remove("leader_id");
    assert!(matches!(
        serde_json::from_value::<RaftCommand>(wire).unwrap(),
        RaftCommand::PartitionControl {
            leader_id: None,
            ..
        }
    ));
}

#[test]
fn command_field_prefilter_handles_escaped_duplicate_and_nested_keys() {
    let single = commit("single", 0, 1);
    let batch = CompactBatch::new(vec![commit("batch", 0, 2)]).unwrap();
    let single_wire = serde_json::to_string(&single).unwrap();
    let single_fields = &single_wire[1..single_wire.len() - 1];
    let batch_wire = serde_json::to_string(&batch).unwrap();
    let escaped = format!(r#"{{"ba\u0074ch":{batch_wire},{single_fields}}}"#);
    assert_eq!(
        serde_json::from_str::<RaftCommand>(&escaped).unwrap(),
        RaftCommand::Batch { batch }
    );
    for duplicate in [
        format!(r#"{{"batch":{batch_wire},"ba\u0074ch":null,{single_fields}}}"#),
        format!(r#"{{"batch":null,"batch":{batch_wire},{single_fields}}}"#),
    ] {
        assert_eq!(
            serde_json::from_str::<RaftCommand>(&duplicate).unwrap(),
            RaftCommand::Single(single.clone())
        );
    }
    // A nested marker is forbidden even if its value is malformed, hidden
    // behind an escape, or followed by a duplicate that changes its value.
    for marker in [
        r#""part\u0069tion":null"#,
        r#""partition_control":null"#,
        r#""partition":{},"partition":false"#,
    ] {
        let wire =
            format!(r#"{{"partition":"garden","epoch":3,"command":{{{marker},{single_fields}}}}}"#);
        let error = serde_json::from_str::<RaftCommand>(&wire).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("nested partition scopes and controls")
        );
    }
    // Values and nested keys cannot masquerade as outer command fields.
    let mut wire = serde_json::to_value(&single).unwrap();
    wire["unknown"] = json!({"batch": {"partition_control": "retention"}});
    wire["result"] = json!(["batch", "partition", "leader_id", "commit"]);
    let expected = serde_json::from_value::<Commit>(wire.clone()).unwrap();
    assert_eq!(
        serde_json::from_value::<RaftCommand>(wire).unwrap(),
        RaftCommand::Single(expected)
    );
}

#[test]
fn command_field_prefilter_preserves_positional_encodings_and_malformed_rejection() {
    let single = commit("single", 0, 1);
    let positional = json!([
        single.internal,
        single.request_id,
        single.fingerprint,
        single.expected_revision,
        single.puts,
        single.deletes,
        single.result
    ]);
    assert_eq!(
        serde_json::from_value::<RaftCommand>(positional.clone()).unwrap(),
        RaftCommand::Single(single.clone())
    );
    let batch = CompactBatch::new(vec![single.clone()]).unwrap();
    assert_eq!(
        serde_json::from_value::<RaftCommand>(json!([batch])).unwrap(),
        RaftCommand::Batch { batch }
    );
    assert_eq!(
        serde_json::from_value::<RaftCommand>(json!(["garden", 3, single])).unwrap(),
        RaftCommand::Scoped {
            partition: "garden".into(),
            epoch: 3,
            command: Box::new(RaftCommand::Single(single)),
        }
    );
    // Inner commands have always required an object for the scope audit.
    assert!(
        serde_json::from_value::<RaftCommand>(json!({
            "partition": "garden", "epoch": 3, "command": positional
        }))
        .is_err()
    );
    for malformed in [
        "null",
        "1",
        "[]",
        "{}",
        r#"{"batch":[]}"#,
        r#"{"batch":{},}"#,
    ] {
        assert!(serde_json::from_str::<RaftCommand>(malformed).is_err());
    }
}

#[test]
fn command_field_prefilter_keeps_scoped_payload_and_unknown_field_depth_independent() {
    let command = RaftCommand::Scoped {
        partition: "garden".into(),
        epoch: 3,
        command: Box::new(RaftCommand::Batch {
            batch: CompactBatch::new(vec![deep_commit("deep", 0)]).unwrap(),
        }),
    };
    let wire = serde_json::to_string(&command).unwrap();
    assert_eq!(serde_json::from_str::<RaftCommand>(&wire).unwrap(), command);
    // Unknown values are skipped iteratively. Their depth must not consume the
    // independently validated business record depth or recurse on our stack.
    let ignored = format!("{}0{}", "[".repeat(4096), "]".repeat(4096));
    let wire = format!(r#"{{"unknown":{ignored},{}"#, &wire[1..]);
    assert_eq!(serde_json::from_str::<RaftCommand>(&wire).unwrap(), command);
    let mut excessive = deep_commit("excessive", 0);
    excessive.result = nested(130);
    let wire = serde_json::to_string(&RaftCommand::Scoped {
        partition: "garden".into(),
        epoch: 3,
        command: Box::new(RaftCommand::Single(excessive)),
    })
    .unwrap();
    assert!(serde_json::from_str::<RaftCommand>(&wire).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_deep_values_survive_grouped_replication_snapshot_and_restart() {
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
    let initial_leader = leader(&nodes).await;
    let lagging = (initial_leader + 1) % nodes.len();
    nodes[lagging].stop().await;
    let group = vec![deep_commit("deep-a", 0), deep_commit("deep-b", 1)];
    let results = nodes[initial_leader]
        .raft()
        .commit_many(group.clone())
        .await
        .expect("valid nested data and receipt results must replicate over HTTP/2");
    assert!(
        results
            .iter()
            .all(|result| matches!(result, ApplyResult::Committed(result)
        if !result.duplicate && result.result == nested(123)))
    );
    let expected = nodes[initial_leader].raft().read().await.unwrap();
    assert_eq!(expected.data["cell:[\"deep\",null]"]["outcome"]["ok"], true);
    assert_eq!(
        expected.data["cell:[\"deep\",null]"]["outcome"]["value"],
        nested(123)
    );
    for (index, node) in nodes.iter().enumerate() {
        if index != lagging {
            wait_revision(node, 2).await;
        }
    }

    // Either survivor may become leader under a busy test runner. Purge both
    // replicas so catch-up must transfer the deep values in a snapshot rather
    // than legally replaying the other replica's retained log.
    let mut required_snapshot_index = u64::MAX;
    for (index, node) in nodes.iter().enumerate() {
        if index == lagging {
            continue;
        }
        // Durable application state is published before OpenRaft's metrics.
        // Capture the storage index that already contains revision 2 instead
        // of comparing two potentially older metrics fields with each other.
        let applied_index = node
            .raft()
            .store
            .clone()
            .applied_state()
            .await
            .unwrap()
            .0
            .unwrap()
            .index;
        node.raft().raft.trigger().snapshot().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let snapshot_index = loop {
            let metrics = node.raft().metrics();
            if let Some(snapshot) = metrics.snapshot
                && snapshot.index >= applied_index
            {
                break snapshot.index;
            }
            assert!(Instant::now() < deadline, "snapshot not produced");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        required_snapshot_index = required_snapshot_index.min(snapshot_index);
        node.raft()
            .raft
            .trigger()
            .purge_log(snapshot_index)
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while node.raft().metrics().purged.map(|id| id.index).unwrap_or(0) < snapshot_index {
            assert!(Instant::now() < deadline, "logs not purged");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    nodes[lagging].restart().await;
    wait_revision(&nodes[lagging], 2).await;
    // The installed application can become visible before its snapshot metric.
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes[lagging]
        .raft()
        .metrics()
        .snapshot
        .is_none_or(|snapshot| snapshot.index < required_snapshot_index)
    {
        assert!(
            Instant::now() < deadline,
            "catch-up must install a snapshot after log purging"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(nodes[lagging].raft().local_snapshot().await, expected);
    for node in &mut nodes {
        node.stop().await;
    }
    for node in &mut nodes {
        node.restart().await;
    }
    let restored = leader(&nodes).await;
    assert_eq!(nodes[restored].raft().read().await.unwrap(), expected);
    let results = nodes[restored].raft().commit_many(group).await.unwrap();
    for (index, result) in results.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if result.duplicate && result.revision == index as u64 + 1 && result.result == nested(123)));
    }
    assert_eq!(nodes[restored].raft().read().await.unwrap(), expected);
    for node in &mut nodes {
        node.stop().await;
    }
}
