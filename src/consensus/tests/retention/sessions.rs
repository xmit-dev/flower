use super::*;

async fn next(store: &mut Store, index: &mut u64, command: RaftCommand) -> ApplyResult {
    *index += 1;
    store
        .apply([raft_entry(*index, command)])
        .await
        .unwrap()
        .remove(0)
}
async fn action(store: &mut Store, index: &mut u64, action: Action) -> ApplyResult {
    let revision = store.snapshot().await.revision;
    next(store, index, control(revision, action)).await
}
async fn open(store: &mut Store, index: &mut u64, owner: &str) -> policy::Session {
    let command = policy::open_session(&store.snapshot().await, owner).unwrap();
    let result = next(store, index, RaftCommand::Retention { retention: command }).await;
    let ApplyResult::Committed(result) = result else {
        panic!("{result:?}")
    };
    serde_json::from_value(result.result["session"].clone()).unwrap()
}
fn ack(session: &policy::Session, through: u64, limit: usize, abandon: bool) -> Action {
    Action::Acknowledge {
        incarnation: session.incarnation.clone(),
        session: session.id.clone(),
        owner: session.owner.clone(),
        through,
        limit,
        abandon,
    }
}
async fn mutation(store: &mut Store, index: &mut u64, id: &str, value: u64) -> ApplyResult {
    let revision = store.snapshot().await.revision;
    next(store, index, commit(id, revision, value).into()).await
}

#[tokio::test]
async fn retention_owner_derivation_is_lazy_without_weakening_session_admission() {
    use crate::consensus::Snapshot;
    use std::cell::Cell;

    policy::validate_request_owner_with(&Snapshot::default(), "legacy-intent", || -> &'static str {
        panic!("Legacy requests have no session owner")
    })
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let history = initialize(&mut store, 1, None).await;
    let mut index = 1;
    let alice = policy::owner_for("alice");
    let session = open(&mut store, &mut index, &alice).await;
    let snapshot = store.snapshot().await;
    let epoch_id = policy::scope_request_id(&history, "epoch-intent");
    policy::validate_request_owner_with(&snapshot, &epoch_id, || -> &'static str {
        panic!("Epoch requests have no session owner")
    })
    .unwrap();
    let session_id = policy::session_request_id(&history, &session, 1).unwrap();
    let derivations = Cell::new(0);
    policy::validate_request_owner_with(&snapshot, &session_id, || {
        derivations.set(derivations.get() + 1);
        policy::owner_for("alice")
    })
    .unwrap();
    assert_eq!(derivations.get(), 1);
    assert!(
        policy::validate_request_owner_with(&snapshot, &session_id, || policy::owner_for("bob"))
            .unwrap_err()
            .to_string()
            .contains("RETRY_SESSION_FORBIDDEN")
    );
    assert!(
        policy::validate_request_owner_with(&snapshot, "f2:malformed", || -> &'static str {
            panic!("Malformed session IDs must fail before owner derivation")
        })
        .unwrap_err()
        .to_string()
        .contains("REQUEST_ID_INVALID")
    );
    // Identity hashes are persisted protocol values; their bytes cannot change
    // when the encoder stops allocating one temporary String per digest byte.
    assert_eq!(
        policy::owner_for("abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[tokio::test]
async fn retention_session_contiguous_ack_rejects_gaps_and_fences_queued_replays_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let history = initialize(&mut store, 1, None).await;
    let mut index = 1;
    let alice = policy::owner_for("alice");
    let bob = policy::owner_for("bob");
    let session = open(&mut store, &mut index, &alice).await;
    let ids: Vec<_> = (1..=3)
        .map(|sequence| policy::session_request_id(&history, &session, sequence).unwrap())
        .collect();
    policy::validate_request_owner(&store.snapshot().await, &ids[0], &alice).unwrap();
    assert!(
        policy::validate_request_owner(&store.snapshot().await, &ids[0], &bob)
            .unwrap_err()
            .to_string()
            .contains("FORBIDDEN")
    );
    assert!(matches!(
        mutation(&mut store, &mut index, &ids[0], 1).await,
        ApplyResult::Committed(_)
    ));
    assert!(matches!(
        mutation(&mut store, &mut index, &ids[2], 3).await,
        ApplyResult::Committed(_)
    ));
    let before = store.snapshot().await;
    rejected(
        &action(&mut store, &mut index, ack(&session, 3, 3, false)).await,
        "RETRY_ACK_GAP",
    );
    assert_eq!(store.snapshot().await, before);
    let mut foreign = session.clone();
    foreign.owner = bob;
    rejected(
        &action(&mut store, &mut index, ack(&foreign, 1, 1, false)).await,
        "RETRY_SESSION_FORBIDDEN",
    );
    let queued = commit(&ids[0], before.revision, 999);
    assert!(matches!(
        action(&mut store, &mut index, ack(&session, 1, 1, false)).await,
        ApplyResult::Committed(_)
    ));
    rejected(
        &next(&mut store, &mut index, queued.into()).await,
        "ALREADY_ACKNOWLEDGED",
    );
    assert!(matches!(
        mutation(&mut store, &mut index, &ids[1], 2).await,
        ApplyResult::Committed(_)
    ));
    rejected(
        &action(&mut store, &mut index, ack(&session, 3, 1, false)).await,
        "RETRY_ACK_BUDGET",
    );
    assert!(matches!(
        action(&mut store, &mut index, ack(&session, 3, 2, false)).await,
        ApplyResult::Committed(_)
    ));
    let expected = store.snapshot().await;
    assert!(expected.requests.is_empty());
    policy::validate_snapshot(&expected).unwrap();
    assert_eq!(
        policy::session(&expected, &session.id)
            .unwrap()
            .unwrap()
            .acknowledged_through,
        3
    );
    let snapshot = store.build_snapshot().await.unwrap();
    store.close().await.unwrap();
    drop(store);
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(store.snapshot().await, expected);
    rejected(
        &mutation(&mut store, &mut index, &ids[1], 999).await,
        "ALREADY_ACKNOWLEDGED",
    );
    let follower_dir = tempfile::tempdir().unwrap();
    let mut follower = Store::open(2, follower_dir.path().into()).await.unwrap();
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    rejected(
        &mutation(&mut follower, &mut index, &ids[2], 999).await,
        "ALREADY_ACKNOWLEDGED",
    );
    assert_eq!(follower.snapshot().await, expected);
}

#[tokio::test]
async fn retention_session_abandonment_closure_and_epoch_gc_never_reopen_old_sequences() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let history = initialize(&mut store, 1, None).await;
    let mut index = 1;
    let session = open(&mut store, &mut index, &policy::owner_for("alice")).await;
    let id = |sequence| policy::session_request_id(&history, &session, sequence).unwrap();
    mutation(&mut store, &mut index, &id(2), 2).await;
    mutation(&mut store, &mut index, &id(100), 100).await;
    rejected(
        &action(&mut store, &mut index, ack(&session, 2, 2, false)).await,
        "RETRY_ACK_GAP",
    );
    // Only one lexicographically earlier result is inspected. Sequence 2 may
    // physically remain, but its watermark must already reject all replays.
    assert!(matches!(
        action(&mut store, &mut index, ack(&session, 2, 1, true)).await,
        ApplyResult::Committed(_)
    ));
    rejected(
        &mutation(&mut store, &mut index, &id(1), 999).await,
        "ALREADY_ACKNOWLEDGED",
    );
    rejected(
        &mutation(&mut store, &mut index, &id(2), 999).await,
        "ALREADY_ACKNOWLEDGED",
    );
    assert!(
        matches!(mutation(&mut store, &mut index, &id(100), 100).await, ApplyResult::Committed(result) if result.duplicate)
    );
    for _ in 0..4 {
        action(
            &mut store,
            &mut index,
            Action::Collect {
                incarnation: history.incarnation.clone(),
                limit: 1,
            },
        )
        .await;
        policy::validate_snapshot(&store.snapshot().await).unwrap();
    }
    assert_eq!(store.snapshot().await.requests.len(), 1);
    action(
        &mut store,
        &mut index,
        Action::CloseSession {
            incarnation: session.incarnation.clone(),
            session: session.id.clone(),
            owner: session.owner.clone(),
            limit: 1,
        },
    )
    .await;
    rejected(
        &mutation(&mut store, &mut index, &id(101), 999).await,
        "RETRY_SESSION_CLOSED",
    );
    action(
        &mut store,
        &mut index,
        Action::Collect {
            incarnation: history.incarnation.clone(),
            limit: 10,
        },
    )
    .await;
    assert!(
        policy::session(&store.snapshot().await, &session.id)
            .unwrap()
            .unwrap()
            .closed
    );
    rejected(
        &action(
            &mut store,
            &mut index,
            Action::OpenSession {
                incarnation: session.incarnation.clone(),
                session: session.id.clone(),
                owner: session.owner.clone(),
                epoch: 0,
            },
        )
        .await,
        "RETRY_SESSION_EXISTS",
    );
    action(
        &mut store,
        &mut index,
        Action::Advance {
            incarnation: history.incarnation.clone(),
            current_epoch: 1,
            min_epoch: 1,
        },
    )
    .await;
    action(
        &mut store,
        &mut index,
        Action::Collect {
            incarnation: history.incarnation.clone(),
            limit: 10,
        },
    )
    .await;
    let final_state = store.snapshot().await;
    assert!(
        policy::session(&final_state, &session.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        policy::status(&final_state).unwrap().unwrap().session_count,
        0
    );
    policy::validate_snapshot(&final_state).unwrap();
    rejected(
        &mutation(&mut store, &mut index, &id(101), 999).await,
        "RETRY_WINDOW_EXPIRED",
    );
    rejected(
        &action(
            &mut store,
            &mut index,
            Action::OpenSession {
                incarnation: session.incarnation.clone(),
                session: session.id.clone(),
                owner: session.owner,
                epoch: 0,
            },
        )
        .await,
        "RETRY_EPOCH_NOT_ADMITTED",
    );
}

#[tokio::test]
async fn retention_session_unknown_owners_and_empty_session_pressure_fail_before_effects() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let history = initialize(&mut store, 1, Some(1)).await;
    let mut index = 1;
    let command =
        policy::open_session(&store.snapshot().await, &policy::owner_for("alice")).unwrap();
    let before = store.snapshot().await;
    rejected(
        &next(
            &mut store,
            &mut index,
            RaftCommand::Retention { retention: command },
        )
        .await,
        "RECEIPT_BUDGET_EXCEEDED",
    );
    assert_eq!(store.snapshot().await, before);
    let fake = policy::Session {
        id: "a".repeat(32),
        incarnation: history.incarnation.clone(),
        owner: policy::owner_for("alice"),
        epoch: 0,
        acknowledged_through: 0,
        closed: false,
    };
    let id = policy::session_request_id(&history, &fake, 1).unwrap();
    rejected(
        &mutation(&mut store, &mut index, &id, 999).await,
        "RETRY_SESSION_UNKNOWN",
    );
    assert_eq!(store.snapshot().await, before);
    action(
        &mut store,
        &mut index,
        Action::SetBudget {
            incarnation: history.incarnation.clone(),
            max_receipt_bytes: None,
        },
    )
    .await;
    let session = open(&mut store, &mut index, &fake.owner).await;
    let bytes = policy::status(&store.snapshot().await)
        .unwrap()
        .unwrap()
        .session_bytes;
    action(
        &mut store,
        &mut index,
        Action::SetBudget {
            incarnation: history.incarnation.clone(),
            max_receipt_bytes: Some(bytes),
        },
    )
    .await;
    let before = store.snapshot().await;
    rejected(
        &action(&mut store, &mut index, ack(&session, 1_000, 1, true)).await,
        "RECEIPT_BUDGET_EXCEEDED",
    );
    assert_eq!(store.snapshot().await, before);
    // Closing releases metadata bytes, so a lowered budget cannot prevent it.
    action(
        &mut store,
        &mut index,
        Action::SetBudget {
            incarnation: history.incarnation.clone(),
            max_receipt_bytes: Some(1),
        },
    )
    .await;
    assert!(matches!(
        action(
            &mut store,
            &mut index,
            Action::CloseSession {
                incarnation: history.incarnation,
                session: session.id,
                owner: session.owner,
                limit: 1,
            }
        )
        .await,
        ApplyResult::Committed(_)
    ));
    policy::validate_snapshot(&store.snapshot().await).unwrap();
}

#[tokio::test]
async fn retention_reincarnation_rejects_old_capabilities_and_can_never_reuse_a_retired_history() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let history = initialize(&mut store, 1, None).await;
    let mut index = 1;
    let session = open(&mut store, &mut index, &policy::owner_for("alice")).await;
    let old_id = policy::scope_request_id(&history, "old");
    let session_id = policy::session_request_id(&history, &session, 1).unwrap();
    mutation(&mut store, &mut index, &old_id, 1).await;
    mutation(&mut store, &mut index, &session_id, 2).await;
    let before = store.snapshot().await;
    let missing_fence = policy::reincarnate(&before, "").unwrap();
    rejected(
        &next(
            &mut store,
            &mut index,
            RaftCommand::Retention {
                retention: missing_fence,
            },
        )
        .await,
        "RESTORE_FENCE_REQUIRED",
    );
    let command = policy::reincarnate(
        &before,
        "Original deployment isolated; external sinks admitted the new generation",
    )
    .unwrap();
    assert!(matches!(
        next(
            &mut store,
            &mut index,
            RaftCommand::Retention { retention: command }
        )
        .await,
        ApplyResult::Committed(_)
    ));
    let after = store.snapshot().await;
    let current = policy::status(&after).unwrap().unwrap();
    assert_eq!(current.database, history.database);
    assert_ne!(current.incarnation, history.incarnation);
    rejected(
        &mutation(&mut store, &mut index, &old_id, 999).await,
        "HISTORY_MISMATCH",
    );
    rejected(
        &mutation(&mut store, &mut index, &session_id, 999).await,
        "HISTORY_MISMATCH",
    );
    action(
        &mut store,
        &mut index,
        Action::Collect {
            incarnation: current.incarnation.clone(),
            limit: 10,
        },
    )
    .await;
    let cleaned = store.snapshot().await;
    policy::validate_snapshot(&cleaned).unwrap();
    assert!(cleaned.requests.is_empty());
    assert!(policy::session(&cleaned, &session.id).unwrap().is_none());
    rejected(
        &action(
            &mut store,
            &mut index,
            Action::Reincarnate {
                incarnation: current.incarnation.clone(),
                new_incarnation: history.incarnation,
                fence_attestation: "attempt to revive original".into(),
            },
        )
        .await,
        "HISTORY_REUSED",
    );
    let new_id = policy::scope_request_id(&current, "explicit new intent");
    assert!(matches!(
        mutation(&mut store, &mut index, &new_id, 3).await,
        ApplyResult::Committed(_)
    ));
    let mut coordinator = commit("internal", store.snapshot().await.revision, 0);
    coordinator.internal = true;
    coordinator.puts = BTreeMap::from([(
        "transaction:coordinator:old".into(),
        json!({"complete":true}),
    )]);
    next(&mut store, &mut index, coordinator.into()).await;
    let command = policy::reincarnate(
        &store.snapshot().await,
        "Cannot substitute for a distributed restore cut",
    )
    .unwrap();
    rejected(
        &next(
            &mut store,
            &mut index,
            RaftCommand::Retention { retention: command },
        )
        .await,
        "CROSS_GROUP_RESTORE_UNSUPPORTED",
    );
}
