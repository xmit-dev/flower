use super::*;

#[test]
fn compact_batches_keep_only_the_final_overlay_and_ordered_metadata() {
    let mut first = commit("a", 10, 1);
    first.puts = BTreeMap::from([
        ("replaced".into(), json!(1)),
        ("removed".into(), json!(2)),
        ("restored".into(), json!(3)),
    ]);
    first.deletes = vec!["absent".into()];
    let mut second = commit("b", 11, 2);
    second.puts = BTreeMap::from([("replaced".into(), json!(4))]);
    second.deletes = vec!["removed".into(), "restored".into(), "absent".into()];
    let mut third = commit("c", 12, 3);
    third.puts = BTreeMap::from([("restored".into(), json!(5))]);
    third.deletes = vec!["restored".into()]; // Within one invocation, puts win.
    let batch = CompactBatch::new(vec![first, second, third]).unwrap();
    assert_eq!(batch.expected_revision, 10);
    assert_eq!(
        batch.puts,
        BTreeMap::from([("replaced".into(), json!(4)), ("restored".into(), json!(5)),])
    );
    assert_eq!(batch.deletes, ["absent", "removed"]);
    assert_eq!(
        batch
            .items
            .iter()
            .map(|item| item.request_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert_eq!(
        batch
            .items
            .iter()
            .map(|item| item.result.clone())
            .collect::<Vec<_>>(),
        [
            json!({"written":1}),
            json!({"written":2}),
            json!({"written":3})
        ]
    );
    let command = RaftCommand::Batch { batch };
    let encoded = serde_json::to_vec(&command).unwrap();
    assert_eq!(
        serde_json::from_slice::<RaftCommand>(&encoded).unwrap(),
        command
    );
}

#[test]
fn compact_batches_reject_noncontiguous_duplicate_or_exhausted_proposals() {
    assert!(CompactBatch::new(vec![]).is_err());
    assert!(CompactBatch::new(vec![commit("a", 0, 1), commit("b", 2, 2)]).is_err());
    assert!(CompactBatch::new(vec![commit("a", 0, 1), commit("a", 1, 2)]).is_err());
    assert!(
        CompactBatch::new(vec![commit("a", u64::MAX - 1, 1), commit("b", u64::MAX, 2)]).is_err()
    );
    let mut internal = commit("a", 1, 2);
    internal.internal = true;
    assert!(CompactBatch::new(vec![commit("a", 0, 1), internal]).is_ok());
}

#[tokio::test]
async fn precompacted_writer_batches_validate_before_append_and_count_scope_bytes() {
    let mut node = Node::new(1).await;
    let good = CompactBatch::new(vec![commit("a", 0, 1), commit("b", 1, 2)]).unwrap();
    let mut duplicate = good.clone();
    duplicate.items[1].request_id = duplicate.items[0].request_id.clone();
    let mut exhausted = good.clone();
    exhausted.expected_revision = u64::MAX - 1;
    let mut overlap = good.clone();
    overlap
        .deletes
        .push(overlap.puts.keys().next().unwrap().clone());
    let mut empty = good.clone();
    empty.items.clear();
    for (batch, expected) in [
        (duplicate, "repeats a request ID"),
        (exhausted, "revision exhausted"),
        (overlap, "both puts and deletes"),
        (empty, "must not be empty"),
    ] {
        let error = node.raft().commit_compact(batch).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
    let unscoped_size = super::super::encoded_json_len(&RaftCommand::Batch {
        batch: good.clone(),
    })
    .unwrap();
    let mut scoped = node.raft().partition("garden", 1).unwrap();
    std::sync::Arc::make_mut(&mut scoped.limits).transaction_max_bytes = unscoped_size;
    let error = scoped.commit_compact(good).await.unwrap_err();
    assert!(error.to_string().contains("FLOWER_TRANSACTION_MAX_BYTES"));
    assert_eq!(node.raft().local_snapshot().await.revision, 0);
    assert_eq!(node.raft().metrics().last_applied, None);
    assert_eq!(
        node.raft()
            .store
            .clone()
            .get_log_state()
            .await
            .unwrap()
            .last_log_id,
        None
    );
    drop(scoped);
    node.stop().await;
}

#[tokio::test]
async fn malformed_or_stale_compact_batches_leave_no_data_or_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([entry(1, commit("original", 0, 7))])
        .await
        .unwrap();
    let original = store.snapshot().await;
    let good = CompactBatch::new(vec![commit("a", 1, 1), commit("b", 2, 2)]).unwrap();
    let mut duplicate = good.clone();
    duplicate.items[1].request_id = "a".into();
    let mut overlap = good.clone();
    overlap
        .deletes
        .push(overlap.puts.keys().next().unwrap().clone());
    let mut exhausted = good.clone();
    exhausted.expected_revision = u64::MAX - 1;
    let mut stale = good.clone();
    stale.expected_revision = 0;
    let mut empty = good;
    empty.items.clear();
    for (index, batch) in [duplicate, overlap, exhausted, stale, empty]
        .into_iter()
        .enumerate()
    {
        let result = store
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), index as u64 + 2),
                payload: EntryPayload::Normal(RaftCommand::Batch { batch }),
            }])
            .await
            .unwrap();
        match &result[0] {
            ApplyResult::Batch(results) => {
                assert_eq!(results.len(), 2);
                assert!(
                    results
                        .iter()
                        .all(|result| matches!(result, ApplyResult::Rejected(_)))
                );
            }
            ApplyResult::Rejected(_) => {}
            other => panic!("invalid batch was not rejected: {other:?}"),
        }
        assert_eq!(store.snapshot().await, original);
    }
    store.close().await.unwrap();
    drop(store);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.snapshot().await, original);
}

#[tokio::test]
async fn atomic_rejection_preserves_preceding_entries_in_the_storage_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let results = store
        .apply([
            entry(1, commit("original", 0, 7)),
            group_entry(2, vec![commit("new", 1, 8), commit("original", 2, 999)]),
            entry(3, commit("after", 1, 9)),
        ])
        .await
        .unwrap();
    assert!(matches!(&results[1], ApplyResult::Batch(results)
        if results.iter().all(|result| matches!(result, ApplyResult::Rejected(_)))));
    assert!(matches!(&results[2], ApplyResult::Committed(result) if result.revision == 2));
    let state = store.snapshot().await;
    assert_eq!(state.revision, 2);
    assert_eq!(state.requests.len(), 2);
    assert!(!state.requests.contains_key("new"));
    assert_eq!(state.data["source:[\"counter\",\"one\"]"], 9);
}

#[tokio::test]
async fn complete_replay_returns_original_results_without_publishing_retry_overlay() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store
        .apply([group_entry(1, vec![commit("a", 0, 1), commit("b", 1, 2)])])
        .await
        .unwrap();
    store.apply([entry(2, commit("c", 2, 3))]).await.unwrap();
    let original = store.snapshot().await;
    let result = store
        .apply([group_entry(
            3,
            vec![commit("a", 0, 998), commit("b", 1, 999)],
        )])
        .await
        .unwrap();
    let ApplyResult::Batch(results) = &result[0] else {
        panic!("expected batch")
    };
    for (index, result) in results.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if result.duplicate && result.revision == index as u64 + 1
                && result.result == json!({"written": index + 1})));
    }
    assert_eq!(store.snapshot().await, original);
    let mut internal = commit("maintenance", 4, 5);
    internal.internal = true;
    let results = store
        .apply([group_entry(4, vec![commit("a", 3, 4), internal])])
        .await
        .unwrap();
    assert!(matches!(&results[0], ApplyResult::Batch(results)
        if results.iter().all(|result| matches!(result, ApplyResult::Rejected(_)))));
    assert_eq!(store.snapshot().await, original);
}

#[test]
fn compact_counter_payload_and_admission_bound() {
    for internal in [false, true] {
        for count in [1, 2, 3, 96] {
            for payload_bytes in [0, 2_048] {
                let commands: Vec<_> = (0..count)
                    .map(|index| {
                        let mut command = commit(&format!("counter-{index}"), index, index + 1);
                        command.internal = internal;
                        command.puts = BTreeMap::from([(
                            "counter".into(),
                            json!({
                                "count":index+1, "description":"x".repeat(payload_bytes),
                            }),
                        )]);
                        command
                    })
                    .collect();
                let old = serde_json::to_vec(&json!({"batch":&commands}))
                    .unwrap()
                    .len();
                let compact = RaftCommand::Batch {
                    batch: CompactBatch::new(commands.clone()).unwrap(),
                };
                let compact_bytes = serde_json::to_vec(&compact).unwrap().len();
                // The writer uses Single for a lone command; batches of two or
                // more must fit its existing conservative byte admission.
                if count > 1 {
                    assert!(
                        compact_bytes <= old,
                        "{count} {internal} {payload_bytes}: {compact_bytes} > {old}"
                    );
                }
                if count == 96 && !internal && payload_bytes == 2_048 {
                    eprintln!(
                        "96 counter updates / 2048-byte hot value: full patches {old} bytes; compact {compact_bytes} bytes; {:.2}% retained",
                        compact_bytes as f64 / old as f64 * 100.0
                    );
                    assert!(compact_bytes * 10 < old);
                }
            }
        }
    }
}
