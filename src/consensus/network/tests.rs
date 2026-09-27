use super::*;
use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

#[test]
fn outgoing_rpc_budget_is_exact_and_preserves_json_encoding() {
    let request = request(1);
    let expected = serde_json::to_vec(&request).unwrap();
    let mut body = RpcBody::new(expected.len());
    serde_json::to_writer(&mut body, &request).unwrap();
    assert_eq!(body.bytes, expected);
    assert!(!body.exceeded);
    let mut body = RpcBody::new(expected.len() - 1);
    assert!(serde_json::to_writer(&mut body, &request).is_err());
    assert!(body.exceeded);
    assert!(body.bytes.len() < expected.len());
    let before = body.bytes.len();
    assert!(body.write_all(&vec![0; expected.len()]).is_err());
    assert_eq!(body.bytes.len(), before);
}

#[tokio::test]
async fn oversized_append_is_split_before_any_network_transfer() {
    let (peer, mut connection, server) = peer().await;
    let request = request(1);
    Arc::make_mut(&mut connection.limits).rpc_max_bytes =
        serde_json::to_vec(&request).unwrap().len() - 1;
    let result = connection
        .append_entries(request, RPCOption::new(Duration::from_millis(50)))
        .await;
    assert!(matches!(result, Err(RPCError::PayloadTooLarge(_))));
    assert!(peer.requests.lock().unwrap().is_empty());
    assert!(connection.pending_append.is_none());
    server.abort();
}

#[derive(Clone, Default)]
struct Peer {
    requests: Arc<Mutex<Vec<AppendEntriesRequest<TypeConfig>>>>,
    observed: Arc<Notify>,
    release: Arc<Notify>,
    in_flight: Arc<AtomicUsize>,
}

async fn append(
    State(peer): State<Peer>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> (
    HeaderMap,
    Json<Result<AppendEntriesResponse<u64>, openraft::error::RaftError<u64>>>,
) {
    let empty = request.entries.is_empty();
    let vote = request.vote;
    peer.requests.lock().unwrap().push(request);
    peer.observed.notify_one();
    if !empty {
        peer.in_flight.fetch_add(1, Ordering::SeqCst);
        peer.release.notified().await;
        peer.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
    let mut headers = HeaderMap::new();
    headers.insert(RPC_NODE_HEADER, "2".parse().unwrap());
    headers.insert(
        membership::COMPATIBILITY_HEADER,
        membership::contract().parse().unwrap(),
    );
    // Distinct responses expose accidental acknowledgements of another request.
    (
        headers,
        Json(Ok(if vote.leader_id.term == 1 {
            AppendEntriesResponse::Success
        } else {
            AppendEntriesResponse::HigherVote(Vote::new(vote.leader_id.term + 10, 2))
        })),
    )
}

async fn peer() -> (Peer, Connection, JoinHandle<()>) {
    let state = Peer::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let router = Router::new()
        .route("/raft/append", post(append))
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let connection = Network::new("network-tests")
        .unwrap()
        .new_client(2, &BasicNode::new(address))
        .await;
    (state, connection, task)
}

fn request(term: u64) -> AppendEntriesRequest<TypeConfig> {
    let log_id = LogId::new(CommittedLeaderId::new(term, 1), 1);
    AppendEntriesRequest {
        vote: Vote::new_committed(term, 1),
        prev_log_id: None,
        entries: vec![Entry {
            log_id,
            payload: EntryPayload::Blank,
        }],
        leader_commit: None,
    }
}

async fn poll_expired(connection: &mut Connection, request: AppendEntriesRequest<TypeConfig>) {
    let deadline = Duration::from_millis(50);
    assert!(
        tokio::time::timeout(
            deadline,
            connection.append_entries(request, RPCOption::new(deadline))
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn durable_append_survives_repeated_heartbeat_deadline_cancellation() {
    let (peer, mut connection, server) = peer().await;
    let request = request(1);
    for _ in 0..3 {
        poll_expired(&mut connection, request.clone()).await;
    }
    assert_eq!(
        peer.requests.lock().unwrap().len(),
        1,
        "canceled awaits must reuse the exact in-flight request"
    );
    assert_eq!(peer.in_flight.load(Ordering::SeqCst), 1);
    peer.release.notify_one();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        connection.append_entries(request, RPCOption::new(Duration::from_millis(50))),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, AppendEntriesResponse::Success);
    assert!(connection.pending_append.is_none());
    server.abort();
}

#[tokio::test]
async fn changed_vote_prev_entry_or_regressed_commit_never_reuses_a_completed_response() {
    let (peer, mut connection, server) = peer().await;
    let mut original = request(1);
    original.leader_commit = Some(original.entries[0].log_id);
    for changed in [
        request(2),
        AppendEntriesRequest {
            leader_commit: None,
            ..original.clone()
        },
        AppendEntriesRequest {
            prev_log_id: Some(original.entries[0].log_id),
            ..original.clone()
        },
        AppendEntriesRequest {
            entries: vec![Entry {
                log_id: original.entries[0].log_id,
                payload: EntryPayload::Membership(Default::default()),
            }],
            ..original.clone()
        },
    ] {
        poll_expired(&mut connection, original.clone()).await;
        peer.release.notify_one();
        // Let the old task finish but deliberately leave its response unconsumed.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !connection
                .pending_append
                .as_ref()
                .unwrap()
                .task
                .is_finished()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let before = peer.requests.lock().unwrap().len();
        poll_expired(&mut connection, changed.clone()).await;
        assert_eq!(peer.requests.lock().unwrap().len(), before + 1);
        peer.release.notify_one();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            connection.append_entries(changed.clone(), RPCOption::new(Duration::from_millis(50))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            result,
            if changed.vote.leader_id.term == 1 {
                AppendEntriesResponse::Success
            } else {
                AppendEntriesResponse::HigherVote(Vote::new(changed.vote.leader_id.term + 10, 2))
            }
        );
    }
    server.abort();
}

#[tokio::test]
async fn advancing_commit_and_fresh_heartbeats_preserve_one_pending_log_transfer() {
    let (peer, mut connection, server) = peer().await;
    let original = request(1);
    let mut advanced = original.clone();
    let deadline = Duration::from_millis(50);
    poll_expired(&mut connection, original.clone()).await;

    for index in 1..=3 {
        // The other follower can commit later logs while this follower remains
        // stuck on the same data range. That must not restart its durable RPC.
        advanced.leader_commit = Some(LogId::new(CommittedLeaderId::new(1, 1), index));
        poll_expired(&mut connection, advanced.clone()).await;
        let probe = AppendEntriesRequest {
            entries: vec![],
            ..advanced.clone()
        };
        let response = tokio::time::timeout(
            deadline,
            connection.append_entries(probe, RPCOption::new(deadline)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response, AppendEntriesResponse::Success);
        assert_eq!(peer.in_flight.load(Ordering::SeqCst), 1);
        let requests = peer.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|req| !req.entries.is_empty())
                .count(),
            1
        );
        assert_eq!(
            requests.last().unwrap().leader_commit,
            advanced.leader_commit
        );
        assert!(requests.last().unwrap().entries.is_empty());
    }

    // Consume the original data response under the latest commit. The retained
    // wire request still carries None: Success proves only the identical logs,
    // not delivery or application of the caller's now-newer commit value.
    assert_eq!(
        connection
            .pending_append
            .as_ref()
            .unwrap()
            .request
            .leader_commit,
        None
    );
    peer.release.notify_one();
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        connection.append_entries(advanced.clone(), RPCOption::new(deadline)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response, AppendEntriesResponse::Success);
    assert!(connection.pending_append.is_none());
    assert_eq!(peer.requests.lock().unwrap().len(), 4);

    // A subsequent real heartbeat independently sends the current commit. It
    // cannot consume the retained data acknowledgment or omit that propagation.
    let latest_commit = advanced.leader_commit;
    let probe = AppendEntriesRequest {
        prev_log_id: Some(original.entries[0].log_id),
        entries: vec![],
        ..advanced
    };
    connection
        .append_entries(probe, RPCOption::new(deadline))
        .await
        .unwrap();
    let requests = peer.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests.last().unwrap().leader_commit, latest_commit);
    assert!(requests.last().unwrap().entries.is_empty());
    server.abort();
}

#[tokio::test]
async fn commit_regression_after_an_advance_restarts_the_pending_transfer() {
    let (peer, mut connection, server) = peer().await;
    let original = request(1);
    poll_expired(&mut connection, original.clone()).await;
    let advanced = AppendEntriesRequest {
        leader_commit: Some(original.entries[0].log_id),
        ..original.clone()
    };
    poll_expired(&mut connection, advanced).await;
    assert_eq!(peer.requests.lock().unwrap().len(), 1);
    assert_eq!(
        connection
            .pending_append
            .as_ref()
            .unwrap()
            .request
            .leader_commit,
        None
    );
    // Although None equals the original wire request's commit, it regresses
    // relative to the latest caller and cannot share the retained response.
    poll_expired(&mut connection, original).await;
    assert_eq!(peer.requests.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn empty_probe_keeps_its_short_deadline_and_does_not_reuse_data_ack() {
    let (peer, mut connection, server) = peer().await;
    poll_expired(&mut connection, request(1)).await;
    let probe = AppendEntriesRequest {
        entries: vec![],
        ..request(3)
    };
    let deadline = Duration::from_millis(50);
    let result = tokio::time::timeout(
        deadline,
        connection.append_entries(probe, RPCOption::new(deadline)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, AppendEntriesResponse::HigherVote(Vote::new(13, 2)));
    assert!(connection.pending_append.is_none());
    assert_eq!(peer.requests.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn interleaved_same_vote_probe_does_not_cancel_pending_data() {
    let (peer, mut connection, server) = peer().await;
    let data = request(1);
    poll_expired(&mut connection, data.clone()).await;
    let probe = AppendEntriesRequest {
        entries: vec![],
        ..data.clone()
    };
    let deadline = Duration::from_millis(50);
    let response = tokio::time::timeout(
        deadline,
        connection.append_entries(probe, RPCOption::new(deadline)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response, AppendEntriesResponse::Success);
    assert!(connection.pending_append.is_some());
    assert_eq!(peer.requests.lock().unwrap().len(), 2);
    assert_eq!(peer.in_flight.load(Ordering::SeqCst), 1);
    peer.release.notify_one();
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        connection.append_entries(data, RPCOption::new(deadline)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response, AppendEntriesResponse::Success);
    assert_eq!(
        peer.requests.lock().unwrap().len(),
        2,
        "data retry must consume its retained response"
    );
    server.abort();
}

#[tokio::test]
async fn connection_drop_aborts_its_retained_transport_task() {
    let (_peer, mut connection, server) = peer().await;
    poll_expired(&mut connection, request(1)).await;
    let abort = connection
        .pending_append
        .as_ref()
        .unwrap()
        .task
        .abort_handle();
    drop(connection);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[test]
fn snapshot_segments_carry_their_bytes_as_they_are() {
    let request = InstallSnapshotRequest::<TypeConfig> {
        vote: openraft::Vote::new_committed(3, 1),
        meta: openraft::SnapshotMeta::default(),
        offset: 1 << 20,
        data: (0..=255).collect(),
        done: true,
    };
    let body = encode_segment(request.clone()).unwrap();
    // The bytes follow the header unchanged.
    assert_eq!(body[body.len() - 256..], request.data[..]);
    assert_eq!(decode_segment(&body).unwrap(), request);
    for truncated in [&body[..3], &body[..10]] {
        assert!(decode_segment(truncated).is_err());
    }
}
