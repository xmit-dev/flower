use super::*;
use crate::consensus::membership::{COMPATIBILITY_HEADER, contract};
use crate::consensus::{MembershipChange, MembershipView, compatibility};
use std::collections::BTreeSet;

fn members(nodes: &[Node]) -> BTreeMap<u64, String> {
    nodes
        .iter()
        .map(|node| (node.id, node.address.clone()))
        .collect()
}

fn change(members: BTreeMap<u64, String>) -> MembershipChange {
    MembershipChange {
        members,
        expected_log_id: None,
    }
}

async fn fresh_revision(node: &Node, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if node
            .raft()
            .read_query()
            .await
            .is_ok_and(|state| state.revision == expected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node did not serve expected fresh revision"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn compatibility_allows_build_changes_but_rejects_every_semantic_contract_change() {
    let current = compatibility();
    let mut patch = current.clone();
    patch.build = "9999.0.1-compatible-patch".into();
    assert!(current.compatible_with(&patch));
    assert_eq!(current.contract(), patch.contract());
    let mut variants = vec![current.clone(); 5];
    variants[0].raft_wire += 1;
    variants[1].state_machine += 1;
    variants[2].snapshot_format += 1;
    variants[3].value_format += 1;
    variants[4].quickjs_sha256 = "other-guest".into();
    for incompatible in variants {
        assert!(!current.compatible_with(&incompatible));
        assert_ne!(current.contract(), incompatible.contract());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_add_replace_restart_and_remove_leader_preserves_durable_data() {
    let mut nodes = vec![Node::new(1).await, Node::new(2).await, Node::new(3).await];
    nodes[0].raft().initialize(members(&nodes)).await.unwrap();
    let first_leader = leader(&nodes).await;
    nodes[first_leader]
        .raft()
        .commit(commit("before-membership", 0, 17))
        .await
        .unwrap();
    nodes.push(Node::new(4).await);
    let added = nodes[first_leader]
        .raft()
        .reconfigure(change(members(&nodes)))
        .await
        .unwrap();
    assert_eq!(added.voter_configs, vec![BTreeSet::from([1, 2, 3, 4])]);
    assert_eq!(added.nodes, members(&nodes));
    fresh_revision(&nodes[3], 1).await;
    let removed = (first_leader + 1) % 3;
    let mut target = members(&nodes);
    target.remove(&nodes[removed].id);
    let replaced = nodes[first_leader]
        .raft()
        .reconfigure(change(target.clone()))
        .await
        .unwrap();
    assert_eq!(
        replaced.voter_configs,
        vec![target.keys().copied().collect()]
    );
    assert!(!replaced.nodes.contains_key(&nodes[removed].id));
    nodes[removed].stop().await;

    nodes[3].stop().await;
    nodes[3].restart().await;
    fresh_revision(&nodes[3], 1).await;
    assert_eq!(nodes[3].raft().membership().nodes, target);

    let current = leader(&nodes).await;
    target.remove(&nodes[current].id);
    // The removed leader may observe its own demotion before its client reply.
    // A successful reply is exact; an uncertain reply requires inspection.
    let outcome = nodes[current]
        .raft()
        .reconfigure(change(target.clone()))
        .await;
    if let Ok(committed) = outcome {
        assert_eq!(
            committed.voter_configs,
            vec![target.keys().copied().collect()]
        );
    }
    nodes[current].stop().await;
    let next = leader(&nodes).await;
    assert_ne!(nodes[next].id, nodes[current].id);
    assert_eq!(
        nodes[next].raft().membership().voter_configs,
        vec![target.keys().copied().collect()]
    );
    nodes[next]
        .raft()
        .commit(commit("after-removing-leader", 1, 23))
        .await
        .unwrap();
    assert_eq!(nodes[next].raft().read().await.unwrap().revision, 2);

    // A retired server can restart its retained directory, but that does not
    // put its ID/address back in the cluster's authoritative membership.
    nodes[current].restart().await;
    assert!(
        !nodes[next]
            .raft()
            .membership()
            .nodes
            .contains_key(&nodes[current].id)
    );
    nodes[next]
        .raft()
        .commit(commit("retired-node-rejoined-network", 2, 29))
        .await
        .unwrap();
    for node in &mut nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn membership_rejects_aliases_address_changes_stale_cas_and_initialized_new_nodes() {
    let mut node = Node::new(1).await;
    node.raft()
        .initialize(members(std::slice::from_ref(&node)))
        .await
        .unwrap();
    leader(std::slice::from_ref(&node)).await;
    let initial = node.raft().membership();
    for invalid in [
        BTreeMap::new(),
        BTreeMap::from([(0, "127.0.0.1:8000".into())]),
        BTreeMap::from([(1, node.address.clone()), (2, node.address.clone())]),
        BTreeMap::from([(1, "127.0.0.1:8000".into())]),
    ] {
        assert!(node.raft().reconfigure(change(invalid)).await.is_err());
    }
    let mut precondition = change(members(std::slice::from_ref(&node)));
    precondition.expected_log_id = Some(LogId::new(CommittedLeaderId::new(99, 1), 999));
    assert!(
        node.raft()
            .reconfigure(precondition)
            .await
            .unwrap_err()
            .to_string()
            .contains("precondition")
    );
    let mut other_cluster = Node::new(2).await;
    other_cluster
        .raft()
        .initialize(members(std::slice::from_ref(&other_cluster)))
        .await
        .unwrap();
    leader(std::slice::from_ref(&other_cluster)).await;
    let error = node
        .raft()
        .reconfigure(change(BTreeMap::from([
            (1, node.address.clone()),
            (2, other_cluster.address.clone()),
        ])))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("already has Raft state"),
        "{error:#}"
    );
    assert_eq!(node.raft().membership().log_id, initial.log_id);
    other_cluster.stop().await;
    node.stop().await;
}

#[tokio::test]
async fn every_peer_route_rejects_missing_or_incompatible_contract_before_handling_rpc() {
    let mut node = Node::new(1).await;
    let client = reqwest::Client::new();
    let mut incompatible = compatibility().clone();
    incompatible.state_machine += 1;
    let incompatible_contract = incompatible.contract();
    for path in ["append", "vote", "snapshot", "read-fence", "version"] {
        for sent in [None, Some(incompatible_contract.as_str())] {
            let method = if path == "version" {
                reqwest::Method::GET
            } else {
                reqwest::Method::POST
            };
            let mut request = client
                .request(method, format!("http://{}/raft/{path}", node.address))
                .bearer_auth(TEST_TOKEN)
                .header(super::super::RPC_TARGET_HEADER, "1");
            if let Some(sent) = sent {
                request = request.header(COMPATIBILITY_HEADER, sent);
            }
            let response = request
                .json(&json!({"malformed":"must never reach the RPC decoder"}))
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                reqwest::StatusCode::UPGRADE_REQUIRED,
                "{path}"
            );
            assert_eq!(response.headers()[COMPATIBILITY_HEADER], contract());
        }
    }
    assert!(!node.raft().raft.is_initialized().await.unwrap());
    // Operators can inspect compatibility without knowing it ahead of time.
    let inspected: MembershipView = client
        .get(format!("http://{}/raft/membership", node.address))
        .bearer_auth(TEST_TOKEN)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(inspected.compatibility, *compatibility());
    let info = super::super::network::Network::new(TEST_TOKEN)
        .unwrap()
        .peer_info(1, &node.address)
        .await
        .unwrap();
    assert_eq!(info.id, 1);
    assert_eq!(info.compatibility, *compatibility());
    node.stop().await;
}

#[tokio::test]
async fn peer_transport_rejects_incompatible_successful_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let router = axum::Router::new().route(
        "/raft/vote",
        axum::routing::post(|| async {
            (
                [
                    (super::super::RPC_NODE_HEADER, "2"),
                    (COMPATIBILITY_HEADER, "incompatible"),
                ],
                axum::Json(Result::<_, openraft::error::RaftError<u64>>::Ok(
                    openraft::raft::VoteResponse::new(Vote::new(1, 1), None, true),
                )),
            )
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut connection = super::super::network::Network::new(TEST_TOKEN)
        .unwrap()
        .new_client(2, &openraft::BasicNode::new(address))
        .await;
    let error = connection
        .vote(
            VoteRequest {
                vote: Vote::new(1, 1),
                last_log_id: None,
            },
            RPCOption::new(Duration::from_secs(2)),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("compatibility mismatch"),
        "{error}"
    );
    server.abort();
}

#[tokio::test]
async fn peer_probe_accepts_a_different_build_with_the_same_semantic_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let advertised = address.clone();
    let router = axum::Router::new().route(
        "/raft/version",
        axum::routing::get(move || {
            let address = advertised.clone();
            async move {
                let mut compatible = compatibility().clone();
                compatible.build = "9999.0.1-compatible-patch".into();
                (
                    [
                        (super::super::RPC_NODE_HEADER, "2"),
                        (COMPATIBILITY_HEADER, contract()),
                    ],
                    axum::Json(crate::consensus::PeerInfo {
                        id: 2,
                        address,
                        initialized: false,
                        compatibility: compatible,
                    }),
                )
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let peer = super::super::network::Network::new(TEST_TOKEN)
        .unwrap()
        .peer_info(2, &address)
        .await
        .unwrap();
    assert_ne!(peer.compatibility.build, compatibility().build);
    assert!(peer.compatibility.compatible_with(compatibility()));
    server.abort();
}
