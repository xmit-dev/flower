use super::*;

#[tokio::test]
async fn fenced_commands_check_the_full_log_leader_and_preserve_rejection_across_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let request = commit("prepare", 0, 19);
    let make_entry = |index, observed_term, observed_node| Entry {
        log_id: LogId::new(CommittedLeaderId::new(7, 1), index),
        payload: EntryPayload::Normal(RaftCommand::Fenced {
            leader_id: CommittedLeaderId::new(observed_term, observed_node),
            commit: request.clone(),
        }),
    };
    let stale = make_entry(1, 6, 1);
    let encoded = serde_json::to_vec(&stale).unwrap();
    let decoded: Entry<TypeConfig> = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, stale);
    let result = store.apply([decoded]).await.unwrap();
    assert!(
        matches!(&result[0], ApplyResult::Rejected(reason) if reason.contains("preparation was authorized"))
    );
    assert_eq!(store.snapshot().await.revision, 0);
    assert!(store.snapshot().await.data.is_empty());
    assert!(store.snapshot().await.requests.is_empty());
    assert_eq!(store.applied_state().await.unwrap().0, Some(stale.log_id));
    store.close().await.unwrap();
    drop(store);
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(store.snapshot().await.revision, 0);
    assert_eq!(store.applied_state().await.unwrap().0, Some(stale.log_id));
    // OpenRaft's advanced leader identity includes node ID even when term is
    // equal. Comparing only the numeric term would accept this foreign leader.
    let same_term_wrong_node = make_entry(2, 7, 2);
    let result = store.apply([same_term_wrong_node.clone()]).await.unwrap();
    assert!(matches!(&result[0], ApplyResult::Rejected(_)));
    assert_eq!(store.snapshot().await.revision, 0);
    assert!(store.snapshot().await.data.is_empty());
    assert!(store.snapshot().await.requests.is_empty());
    store.close().await.unwrap();
    drop(store);
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(store.snapshot().await.revision, 0);
    assert_eq!(
        store.applied_state().await.unwrap().0,
        Some(same_term_wrong_node.log_id)
    );
    let result = store.apply([make_entry(3, 7, 1)]).await.unwrap();
    assert!(matches!(&result[0], ApplyResult::Committed(result) if result.revision == 1));
    assert_eq!(
        store.snapshot().await.data["source:[\"counter\",\"one\"]"],
        19
    );
}

#[tokio::test]
async fn fenced_preparation_cannot_resume_after_a_leader_change_without_app_writes() {
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
    let (state, observed_term) = nodes[first]
        .raft()
        .read_for_writer_with_term()
        .await
        .unwrap();
    nodes[first].stop().await;
    let next = leader(&nodes).await;
    let (current, current_term) = nodes[next]
        .raft()
        .read_for_writer_with_term()
        .await
        .unwrap();
    assert!(current_term > observed_term);
    assert_eq!(
        current.revision, state.revision,
        "an election is not an application write"
    );
    let request = commit("resume-old-prepare", state.revision, 31);
    let error = nodes[next]
        .raft()
        .commit_in_term(request.clone(), observed_term)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("preparation was authorized"),
        "{error:#}"
    );
    assert_eq!(
        nodes[next].raft().read_for_writer().await.unwrap().revision,
        state.revision
    );
    nodes[next]
        .raft()
        .commit_in_term(request, current_term)
        .await
        .unwrap();
    assert_eq!(
        nodes[next].raft().read_for_writer().await.unwrap().revision,
        state.revision + 1
    );
    for node in &mut nodes {
        node.stop().await;
    }
}
