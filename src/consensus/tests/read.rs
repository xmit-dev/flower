use super::*;
use crate::consensus::read::ReadFence;

#[tokio::test]
async fn read_fences_bind_publication_to_log_identity_and_revision() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(1, directory.path().into()).await.unwrap();
    let first = entry(1, commit("first", 0, 7));
    store.apply([first.clone()]).await.unwrap();
    let fence = store.read_fence().unwrap();
    assert_eq!(fence.applied, first.log_id);
    assert_eq!(fence.revision, 1);
    let retained = store.snapshot_after_fence(&fence).unwrap().unwrap();
    assert!(retained.requests.is_empty());

    // An unchanged application revision is not evidence of applying a later
    // blank or membership log. The published Raft position must catch up too.
    let second = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
        payload: EntryPayload::Blank,
    };
    let next = ReadFence {
        applied: second.log_id,
        revision: 1,
    };
    assert!(store.snapshot_after_fence(&next).unwrap().is_none());
    store.apply([second]).await.unwrap();
    assert!(
        store
            .snapshot_after_fence(&next)
            .unwrap()
            .unwrap()
            .data
            .ptr_eq(&retained.data)
    );
    let different_log = ReadFence {
        applied: LogId::new(CommittedLeaderId::new(2, 1), 2),
        revision: 1,
    };
    assert!(store.snapshot_after_fence(&different_log).is_err());
    for revision in [0, 2] {
        assert!(
            store
                .snapshot_after_fence(&ReadFence {
                    revision,
                    ..next.clone()
                })
                .is_err()
        );
    }

    store
        .apply([entry(3, commit("later", 1, 9))])
        .await
        .unwrap();
    let successor = store.snapshot_after_fence(&fence).unwrap().unwrap();
    assert_eq!(successor.revision, 2);
    assert_eq!(successor.data["source:[\"counter\",\"one\"]"], 9);
    assert_eq!(retained.revision, 1);
    assert_eq!(retained.data["source:[\"counter\",\"one\"]"], 7);
    // A numerically greater index in an older leader's log is not a substitute
    // for the fence's log identity.
    assert!(store.snapshot_after_fence(&different_log).is_err());
    let final_fence = store.read_fence().unwrap();
    store.close().await.unwrap();
    drop(store);
    let reopened = Store::open(1, directory.path().into()).await.unwrap();
    assert_eq!(reopened.read_fence().unwrap().applied, final_fence.applied);
    assert_eq!(
        reopened
            .snapshot_after_fence(&final_fence)
            .unwrap()
            .unwrap(),
        successor
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_queries_execute_on_followers_and_still_require_a_live_quorum() {
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
    let current = leader(&nodes).await;
    nodes[current]
        .raft()
        .commit(commit("first", 0, 10))
        .await
        .unwrap();
    // No pre-wait for follower application: read_query itself must fence/wait.
    for node in &nodes {
        let read = node.raft().read_query().await.unwrap();
        assert_eq!(read.revision, 1);
        assert_eq!(read.data["source:[\"counter\",\"one\"]"], 10);
        assert!(read.requests.is_empty());
    }
    let readers = nodes
        .iter()
        .flat_map(|node| (0..16).map(move |_| node.raft().read_query()));
    for read in futures_util::future::join_all(readers).await {
        let read = read.unwrap();
        assert_eq!(read.revision, 1);
        assert_eq!(read.data["source:[\"counter\",\"one\"]"], 10);
    }
    let follower = (current + 1) % nodes.len();
    assert!(nodes[follower].raft().read_for(None).await.is_err());
    assert!(nodes[follower].raft().read_for_many(&[]).await.is_err());
    assert!(nodes[follower].raft().read_for_writer().await.is_err());
    // The serving-replica dispatcher can forward a query, but the peer RPC
    // itself stays strictly leader-only so stale addressing cannot create an
    // internal forwarding cycle or validate a fence without its own quorum.
    let network = super::super::network::Network::new(TEST_TOKEN).unwrap();
    assert!(
        network
            .read_fence(
                nodes[follower].id,
                openraft::BasicNode::new(nodes[follower].address.clone())
            )
            .await
            .is_err()
    );

    // A completed prior read must never become a reusable freshness lease.
    // Keep only the follower alive; its explicit local view remains available.
    for (index, node) in nodes.iter_mut().enumerate() {
        if index != follower {
            node.stop().await;
        }
    }
    assert_eq!(
        nodes[follower]
            .raft()
            .snapshot_for(None)
            .await
            .unwrap()
            .revision,
        1
    );
    assert!(nodes[follower].raft().read_query().await.is_err());
    nodes[follower].stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_query_fence_survives_leader_change_and_replica_restart() {
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
    let previous = leader(&nodes).await;
    nodes[previous]
        .raft()
        .commit(commit("before-election", 0, 1))
        .await
        .unwrap();
    for node in &nodes {
        node.raft().read_query().await.unwrap();
    }
    nodes[previous].stop().await;
    let current = leader(&nodes).await;
    nodes[current]
        .raft()
        .commit(commit("after-election", 1, 2))
        .await
        .unwrap();
    let follower = (0..nodes.len())
        .find(|index| *index != previous && *index != current)
        .unwrap();
    let read = nodes[follower].raft().read_query().await.unwrap();
    assert_eq!(read.revision, 2);
    assert_eq!(read.data["source:[\"counter\",\"one\"]"], 2);

    nodes[previous].restart().await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(read) = nodes[previous].raft().read_query().await {
            assert_eq!(read.revision, 2);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted replica could not obtain a fresh fence"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for node in &mut nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn read_fence_rpc_requires_peer_authorization_and_target_identity() {
    let mut node = Node::new(1).await;
    node.raft()
        .initialize(BTreeMap::from([(node.id, node.address.clone())]))
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let url = format!("http://{}/raft/read-fence", node.address);
    assert_eq!(
        client.post(&url).send().await.unwrap().status(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(TEST_TOKEN)
            .header(super::super::RPC_TARGET_HEADER, "2")
            .send()
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::CONFLICT
    );
    let response = client
        .post(&url)
        .bearer_auth(TEST_TOKEN)
        .header(super::super::RPC_TARGET_HEADER, "1")
        .header(
            super::super::membership::COMPATIBILITY_HEADER,
            super::super::membership::contract(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()[super::super::RPC_NODE_HEADER], "1");
    let result: Result<ReadFence, super::super::read::ReadFenceError> =
        response.json().await.unwrap();
    assert!(result.is_ok());
    node.stop().await;
}
