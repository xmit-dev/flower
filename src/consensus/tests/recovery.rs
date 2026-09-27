//! Abrupt child exits deliberately bypass Database::drop, which otherwise
//! flushes pending non-durable projections and hides crash-recovery mistakes.
use super::*;
use crate::consensus::{PartitionCommand, PartitionPhase};
use openraft::storage::RaftLogStorageExt;
use openraft::{BasicNode, Membership, RaftLogReader};
use std::sync::atomic::Ordering;

const CHILD: &str = "consensus::tests::recovery::projection_crash_child";
const EXIT: i32 = 73;

fn tail(confirmed: bool) -> Vec<Entry<TypeConfig>> {
    let control = |partition_control| RaftCommand::PartitionControl {
        partition_control,
        leader_id: None,
    };
    let (first, next, scoped, value) = if confirmed {
        ("confirmed", "confirmed-next", "confirmed-scoped", 99)
    } else {
        ("root", "root-next", "scoped", 17)
    };
    let commands = [
        RaftCommand::Batch {
            batch: CompactBatch::new(vec![commit(first, 0, value - 1), commit(next, 1, value)])
                .unwrap(),
        },
        control(PartitionCommand::Create {
            partition: "garden".into(),
            epoch: 1,
            operation: "create".into(),
        }),
        control(PartitionCommand::Activate {
            partition: "garden".into(),
            epoch: 1,
            operation: "create".into(),
        }),
        RaftCommand::Scoped {
            partition: "garden".into(),
            epoch: 1,
            command: Box::new(commit(scoped, 0, value + 6).into()),
        },
    ];
    commands
        .into_iter()
        .enumerate()
        .map(|(index, command)| Entry {
            log_id: LogId::new(
                CommittedLeaderId::new(
                    if confirmed { 2 } else { 1 },
                    if confirmed { 2 } else { 1 },
                ),
                index as u64 + 1,
            ),
            payload: EntryPayload::Normal(command),
        })
        .collect()
}

#[test]
fn projection_crash_child() {
    let Some(directory) = std::env::var_os("FLOWER_TEST_CRASH_DIRECTORY") else {
        return;
    };
    let mode = std::env::var("FLOWER_TEST_CRASH_MODE").unwrap();
    let id: u64 = std::env::var("FLOWER_TEST_CRASH_ID")
        .unwrap()
        .parse()
        .unwrap();
    let members: BTreeMap<u64, String> =
        serde_json::from_str(&std::env::var("FLOWER_TEST_CRASH_MEMBERS").unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
        .block_on(async {
            if mode == "acknowledged" {
                let consensus = Consensus::open(id, members[&id].clone(), std::path::PathBuf::from(directory).into(), TEST_TOKEN.into()).await.unwrap();
                consensus.initialize(members).await.unwrap();
                let deadline = Instant::now() + Duration::from_secs(10);
                while consensus.read().await.is_err() {
                    assert!(Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let results = consensus.commit_many(vec![commit("acknowledged", 0, 41), commit("acknowledged-next", 1, 42)]).await.unwrap();
                assert!(matches!(&results[0], ApplyResult::Committed(result) if result.result == json!({"written":41}) && result.revision == 1));
                assert!(matches!(&results[1], ApplyResult::Committed(result) if result.result == json!({"written":42}) && result.revision == 2));
                assert_eq!(consensus.local_snapshot().await.requests.len(), 2);
                // No shutdown, drop, later append, vote, or snapshot can flush apply.
                std::process::exit(EXIT);
            }
            let mut store = Store::open(id, std::path::PathBuf::from(directory).into()).await.unwrap();
            let membership = Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 0),
                payload: EntryPayload::Membership(Membership::new(
                    vec![members.keys().copied().collect()],
                    members.into_iter().map(|(id,address)| (id,BasicNode::new(address))).collect::<BTreeMap<_,_>>(),
                )),
            };
            store.blocking_append([membership.clone()]).await.unwrap();
            store.apply([membership]).await.unwrap();
            let confirmed = mode == "confirmed";
            store.save_vote(&Vote::new_committed(if confirmed {2} else {1}, if confirmed {2} else {1})).await.unwrap();
            let entries = tail(confirmed);
            store.blocking_append(entries.clone()).await.unwrap();
            if mode == "abandoned" {
                // This isolated leader never obtained a quorum for its suffix.
                // A restart must not mistake durable append for commitment.
                std::process::exit(EXIT);
            }
            let results = store.apply(entries).await.unwrap();
            assert!(matches!(&results[0], ApplyResult::Batch(results) if results.len() == 2 && results.iter().all(|result| matches!(result,ApplyResult::Committed(_)))));
            assert!(results[1..].iter().all(|result| matches!(result, ApplyResult::Committed(_) | ApplyResult::Partition(_))));
            assert_eq!(store.snapshot().await.requests[if confirmed {"confirmed"} else {"root"}].result, json!({"written":if confirmed {98} else {16}}));
            if matches!(mode.as_str(), "append" | "vote") {
                // Write the queued no-sync projection; the Immediate commit
                // below must then make it durable.
                store.persisted().await.unwrap();
            }
            match mode.as_str() {
                "deferred" | "confirmed" => {},
                "append" => store.blocking_append([Entry { log_id: LogId::new(CommittedLeaderId::new(1,1), 5), payload: EntryPayload::Blank }]).await.unwrap(),
                "vote" => store.save_vote(&Vote::new(2, id)).await.unwrap(),
                "snapshot" | "purge" => {
                    let snapshot = store.build_snapshot().await.unwrap();
                    assert_eq!(snapshot.meta.last_log_id.unwrap().index, 4);
                    if mode == "purge" { store.purge(snapshot.meta.last_log_id.unwrap()).await.unwrap(); }
                },
                _ => panic!("unknown crash fixture {mode}"),
            }
            std::process::exit(EXIT);
        });
}

async fn crash(directory: &std::path::Path, id: u64, members: &BTreeMap<u64, String>, mode: &str) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", CHILD, "--nocapture"])
        .env("FLOWER_TEST_CRASH_DIRECTORY", directory)
        .env("FLOWER_TEST_CRASH_ID", id.to_string())
        .env(
            "FLOWER_TEST_CRASH_MEMBERS",
            serde_json::to_string(members).unwrap(),
        )
        .env("FLOWER_TEST_CRASH_MODE", mode);
    let output = tokio::task::spawn_blocking(move || child.output().unwrap())
        .await
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(EXIT),
        "child failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn recovered_query(consensus: &Consensus) -> crate::consensus::Snapshot {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(snapshot) = consensus.snapshot_for(None).await {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "recovery did not complete: {:?}",
            consensus.metrics()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn recovery_barrier_advances_only_the_applied_raft_position() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    store.apply(tail(false)).await.unwrap();
    let root = store.snapshot().await;
    let binding = crate::consensus::PartitionBinding {
        partition: "garden".into(),
        epoch: 1,
    };
    let partition = store.partition_snapshot(&binding, None, true).unwrap();
    let barrier = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
        payload: EntryPayload::Normal(RaftCommand::RecoveryBarrier {
            recovery_barrier: (),
        }),
    };
    assert!(matches!(
        store.apply([barrier.clone()]).await.unwrap().as_slice(),
        [ApplyResult::Internal]
    ));
    assert_eq!(store.applied_state().await.unwrap().0, Some(barrier.log_id));
    assert_eq!(store.snapshot().await, root);
    assert_eq!(
        store.partition_snapshot(&binding, None, true).unwrap(),
        partition
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_leader_serves_no_fence_before_tail_replay_in_a_new_term() {
    use axum::{
        body::{Body, to_bytes},
        extract::Request,
        middleware::{Next, from_fn},
    };
    use std::sync::Arc;
    use tokio::sync::{Notify, watch};

    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let members: BTreeMap<_, _> = listeners
        .iter()
        .enumerate()
        .map(|(index, listener)| (index as u64 + 1, listener.local_addr().unwrap().to_string()))
        .collect();
    let mut directories = Vec::new();
    for id in 1..=3 {
        let directory = tempfile::tempdir().unwrap();
        crash(directory.path(), id, &members, "deferred").await;
        directories.push(directory);
    }
    let (release, released) = watch::channel(false);
    let held = Arc::new(Notify::new());
    let mut nodes = Vec::new();
    for (index, (listener, directory)) in listeners.into_iter().zip(directories).enumerate() {
        let id = index as u64 + 1;
        let consensus = Consensus::open(
            id,
            members[&id].clone(),
            directory.path().into(),
            TEST_TOKEN.into(),
        )
        .await
        .unwrap();
        consensus.raft.runtime_config().elect(false);
        assert!(consensus.recovering.load(Ordering::Acquire));
        let released = released.clone();
        let held = held.clone();
        let router = consensus
            .router()
            .layer(from_fn(move |request: Request, next: Next| {
                let mut released = released.clone();
                let held = held.clone();
                async move {
                    let request = if request.uri().path() == "/raft/append" {
                        let (parts, body) = request.into_parts();
                        let body = to_bytes(body, usize::MAX).await.unwrap();
                        let append: openraft::raft::AppendEntriesRequest<TypeConfig> =
                            serde_json::from_slice(&body).unwrap();
                        // A startup replication probe can confirm a durable suffix
                        // without resending it. Hold it too, while letting independent
                        // read-index heartbeats prove the restored checkpoint quorum.
                        if !append.entries.is_empty()
                            || append.prev_log_id.is_some_and(|log| log.index > 0)
                            || append.leader_commit.is_some_and(|log| log.index > 0)
                        {
                            held.notify_one();
                            while !*released.borrow_and_update() {
                                released.changed().await.unwrap();
                            }
                        }
                        Request::from_parts(parts, Body::from(body))
                    } else {
                        request
                    };
                    next.run(request).await
                }
            }));
        let (stop_server, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        nodes.push(Node {
            host: None,
            id,
            address: members[&id].clone(),
            directory,
            consensus: Some(consensus),
            server: Some(server),
            stop_server: Some(stop_server),
        });
    }
    // Every replica holds the term-1 leader's committed vote, but a leader
    // flushes its own appends lazily: it must win a new term rather than
    // resume, since its log could be missing entries its followers hold.
    // Replicas refuse other candidates until the old leader's lease lapses.
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes[0].raft().metrics().current_leader != Some(1) {
        assert!(Instant::now() < deadline, "node 1 did not win a new term");
        nodes[0].raft().raft.trigger().elect().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    tokio::time::timeout(Duration::from_secs(5), held.notified())
        .await
        .unwrap();
    let leader = nodes[0].raft();
    assert_eq!(leader.metrics().current_leader, Some(1));
    assert!(leader.metrics().current_term > 1);
    // Its term's first entry, and with it any fence, waits for the held
    // replication while acknowledged application state is still missing.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), leader.raft.ensure_linearizable())
            .await
            .is_err()
    );
    assert_eq!(leader.local_snapshot().await.revision, 0);

    let mut reads = Vec::new();
    let c = leader.clone();
    reads.push(tokio::spawn(async move { c.read_query().await }));
    let c = leader.clone();
    reads.push(tokio::spawn(async move { c.read_for_writer().await }));
    let c = nodes[1].raft().clone();
    reads.push(tokio::spawn(async move { c.read_query().await }));
    let c = nodes[2].raft().clone();
    reads.push(tokio::spawn(async move { c.snapshot_for(None).await }));
    let c = leader.partition("garden", 1).unwrap();
    let partition = tokio::spawn(async move { c.snapshot_for(None).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(reads.iter().all(|read| !read.is_finished()));
    assert!(!partition.is_finished());
    assert!(leader.recovering.load(Ordering::Acquire));
    assert_eq!(leader.local_snapshot().await.revision, 0);

    release.send(true).unwrap();
    for read in reads {
        let snapshot = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.revision, 2);
        assert_eq!(snapshot.data["source:[\"counter\",\"one\"]"], 17);
    }
    let snapshot = tokio::time::timeout(Duration::from_secs(5), partition)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.data["source:[\"counter\",\"one\"]"], 23);
    assert!(!leader.recovering.load(Ordering::Acquire));
    let entries = leader.store.clone().try_get_log_entries(5..).await.unwrap();
    let barriers = entries
        .iter()
        .filter(|entry| {
            matches!(
                &entry.payload,
                EntryPayload::Normal(RaftCommand::RecoveryBarrier { .. })
            )
        })
        .count();
    assert!(barriers <= 1, "concurrent readers share one recovery barrier");
    assert_eq!(leader.local_snapshot().await.requests.len(), 2);
    for node in &mut nodes {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_projection_recovers_data_receipts_and_partitions_from_durable_log_after_crash() {
    let directory = tempfile::tempdir().unwrap();
    let members = BTreeMap::from([(1, "127.0.0.1:1".to_owned())]);
    crash(directory.path(), 1, &members, "deferred").await;
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(store.applied_state().await.unwrap().0.unwrap().index, 0);
    assert_eq!(store.snapshot().await.revision, 0);
    assert!(store.snapshot().await.requests.is_empty());
    assert!(store.partition_info("garden").is_err());
    assert_eq!(
        store
            .get_log_state()
            .await
            .unwrap()
            .last_log_id
            .unwrap()
            .index,
        4
    );
    store.close().await.unwrap();
    drop(store);
    let consensus = Consensus::open(
        1,
        members[&1].clone(),
        directory.path().into(),
        TEST_TOKEN.into(),
    )
    .await
    .unwrap();
    assert!(consensus.recovering.load(Ordering::Acquire));
    let snapshot = recovered_query(&consensus).await;
    assert_eq!(snapshot.data["source:[\"counter\",\"one\"]"], 17);
    assert!(!consensus.recovering.load(Ordering::Acquire));
    assert_eq!(
        consensus.read().await.unwrap().requests["root"].result,
        json!({"written":16})
    );
    assert_eq!(
        consensus.read().await.unwrap().requests["root-next"].result,
        json!({"written":17})
    );
    let scoped = consensus.partition("garden", 1).unwrap();
    assert_eq!(
        scoped.snapshot_for(None).await.unwrap().data["source:[\"counter\",\"one\"]"],
        23
    );
    assert_eq!(
        consensus.partition_info("garden").await.unwrap().phase,
        PartitionPhase::Active
    );
    let replay = scoped.commit(commit("scoped", 0, 23)).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.result, json!({"written":23}));
    assert!(
        consensus
            .commit(commit("root", 0, 16))
            .await
            .unwrap()
            .duplicate
    );
    consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_raft_write_survives_crash_before_projection_flush() {
    let directory = tempfile::tempdir().unwrap();
    let members = BTreeMap::from([(1, "127.0.0.1:1".to_owned())]);
    crash(directory.path(), 1, &members, "acknowledged").await;
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    assert!(!store.snapshot().await.requests.contains_key("acknowledged"));
    assert!(
        store.get_log_state().await.unwrap().last_log_id > store.applied_state().await.unwrap().0
    );
    store.close().await.unwrap();
    drop(store);
    let consensus = Consensus::open(
        1,
        members[&1].clone(),
        directory.path().into(),
        TEST_TOKEN.into(),
    )
    .await
    .unwrap();
    assert_eq!(
        recovered_query(&consensus).await.data["source:[\"counter\",\"one\"]"],
        42
    );
    let replay = consensus
        .commit_many(vec![
            commit("acknowledged", 0, 41),
            commit("acknowledged-next", 1, 42),
        ])
        .await
        .unwrap();
    for (index, result) in replay.into_iter().enumerate() {
        assert!(
            matches!(result, ApplyResult::Committed(result) if result.duplicate && result.revision == index as u64 + 1 && result.result == json!({"written":index+41}))
        );
    }
    consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_transactions_flush_projection_and_snapshot_protects_purged_logs_after_crash() {
    for mode in ["append", "vote", "snapshot", "purge"] {
        let directory = tempfile::tempdir().unwrap();
        crash(
            directory.path(),
            1,
            &BTreeMap::from([(1, "127.0.0.1:1".into())]),
            mode,
        )
        .await;
        let mut store = Store::open(1, directory.path().into()).await.unwrap();
        assert_eq!(
            store.applied_state().await.unwrap().0.unwrap().index,
            4,
            "{mode}"
        );
        assert_eq!(
            store.snapshot().await.requests["root"].result,
            json!({"written":16})
        );
        assert_eq!(
            store
                .partition_snapshot(
                    &crate::consensus::PartitionBinding {
                        partition: "garden".into(),
                        epoch: 1
                    },
                    Some("scoped"),
                    false
                )
                .unwrap()
                .requests["scoped"]
                .result,
            json!({"written":23})
        );
        if matches!(mode, "snapshot" | "purge") {
            assert_eq!(
                store
                    .get_current_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .meta
                    .last_log_id
                    .unwrap()
                    .index,
                4
            );
        }
        if mode == "purge" {
            assert_eq!(
                store
                    .get_log_state()
                    .await
                    .unwrap()
                    .last_purged_log_id
                    .unwrap()
                    .index,
                4
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_gate_rejects_local_queries_without_quorum_then_allows_replica_local_reads() {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let members: BTreeMap<_, _> = listeners
        .iter()
        .enumerate()
        .map(|(index, listener)| (index as u64 + 1, listener.local_addr().unwrap().to_string()))
        .collect();
    let mut directories = Vec::new();
    for id in 1..=3 {
        let directory = tempfile::tempdir().unwrap();
        crash(directory.path(), id, &members, "deferred").await;
        directories.push(directory);
    }
    let mut nodes = Vec::new();
    for (index, (listener, directory)) in listeners.into_iter().zip(directories).enumerate() {
        let id = index as u64 + 1;
        let mut node = Node {
            host: None,
            id,
            address: members[&id].clone(),
            directory,
            consensus: None,
            server: None,
            stop_server: None,
        };
        node.start_with_listener(listener).await;
        assert!(node.raft().recovering.load(Ordering::Acquire));
        if index == 0 {
            assert!(
                node.raft().snapshot_for(None).await.is_err(),
                "reverted local state must not be served without quorum"
            );
            assert!(
                node.raft()
                    .partition("garden", 1)
                    .unwrap()
                    .snapshot_for(None)
                    .await
                    .is_err()
            );
            // Diagnostics remain accessible without opening application serving.
            assert_eq!(node.raft().local_snapshot().await.revision, 0);
            assert_eq!(node.raft().metrics().id, 1);
        }
        nodes.push(node);
    }
    for node in &nodes {
        assert_eq!(recovered_query(node.raft()).await.revision, 2);
        assert!(!node.raft().recovering.load(Ordering::Acquire));
    }
    nodes[1].stop().await;
    nodes[2].stop().await;
    assert_eq!(
        nodes[0].raft().snapshot_for(None).await.unwrap().revision,
        2,
        "the recovery gate is one-time, not a perpetual quorum requirement for local reads"
    );
    nodes[0].stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_replaces_uncommitted_conflicting_suffix_before_opening_local_reads() {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let members: BTreeMap<_, _> = listeners
        .iter()
        .enumerate()
        .map(|(index, listener)| (index as u64 + 1, listener.local_addr().unwrap().to_string()))
        .collect();
    let mut directories = Vec::new();
    for id in 1..=3 {
        let directory = tempfile::tempdir().unwrap();
        crash(
            directory.path(),
            id,
            &members,
            if id == 1 { "abandoned" } else { "confirmed" },
        )
        .await;
        directories.push(directory);
    }
    let mut nodes = Vec::new();
    for (index, (listener, directory)) in listeners.into_iter().zip(directories).enumerate() {
        let id = index as u64 + 1;
        let mut node = Node {
            host: None,
            id,
            address: members[&id].clone(),
            directory,
            consensus: None,
            server: None,
            stop_server: None,
        };
        node.start_with_listener(listener).await;
        assert!(node.raft().recovering.load(Ordering::Acquire));
        if id == 1 {
            assert!(
                node.raft().snapshot_for(Some("root")).await.is_err(),
                "an isolated old suffix is not proof of committed application state"
            );
            let checkpoint = node.raft().local_snapshot().await;
            assert_eq!(checkpoint.revision, 0);
            assert!(checkpoint.data.is_empty() && checkpoint.requests.is_empty());
        }
        nodes.push(node);
    }
    // The first successful local query must already see the newer majority
    // prefix, never the old suffix or the pre-recovery empty checkpoint.
    for node in &nodes {
        assert_eq!(
            recovered_query(node.raft()).await.data["source:[\"counter\",\"one\"]"],
            99
        );
        let state = node.raft().local_snapshot().await;
        assert_eq!(state.revision, 2);
        assert!(!state.requests.contains_key("root"));
        assert!(!state.requests.contains_key("root-next"));
        assert_eq!(state.requests["confirmed"].result, json!({"written":98}));
        assert_eq!(
            state.requests["confirmed-next"].result,
            json!({"written":99})
        );
        let scoped = node
            .raft()
            .partition("garden", 1)
            .unwrap()
            .local_snapshot()
            .await;
        assert_eq!(scoped.data["source:[\"counter\",\"one\"]"], 105);
        assert!(!scoped.requests.contains_key("scoped"));
        assert_eq!(
            scoped.requests["confirmed-scoped"].result,
            json!({"written":105})
        );
    }
    let entries = nodes[0]
        .raft()
        .store
        .clone()
        .try_get_log_entries(1..5)
        .await
        .unwrap();
    assert_eq!(entries.len(), 4);
    assert!(
        entries
            .iter()
            .all(|entry| entry.log_id.leader_id == CommittedLeaderId::new(2, 2)),
        "the old suffix must be durably replaced, not merely hidden by a read filter"
    );
    for node in &mut nodes {
        node.stop().await;
    }
}
