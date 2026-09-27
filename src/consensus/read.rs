//! Fresh query reads may execute on any replica. A follower obtains a new
//! quorum-confirmed fence from the leader, then reads its own applied prefix.
//! Fences are per-request proof, never a reusable lease or a cached term check.

mod batch;
pub(super) use batch::Settings;

use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
#[cfg(test)]
use std::time::Duration;

use anyhow::Context;
use axum::{Json, extract::State};
use openraft::LogId;
use serde::{Deserialize, Serialize};

use super::{
    ApplyResult, Consensus, FlowerRaft, RaftCommand, Snapshot, network::Network, store::Store,
};

#[cfg(test)]
pub(super) const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// This log has actually been applied after a fresh leader quorum barrier.
/// Unlike a bare revision, its identity binds the fence to a committed Raft
/// prefix even when rejected commands, membership changes, or blank entries
/// leave the application revision unchanged.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ReadFence {
    pub applied: LogId<u64>,
    pub revision: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ReadFenceError {
    message: String,
}

impl fmt::Display for ReadFenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ReadFenceError {}

#[derive(Clone)]
pub(super) struct Dispatchers {
    leader: batch::Batcher,
    replica: batch::Batcher,
}

impl Dispatchers {
    pub(super) fn new(
        id: u64,
        raft: FlowerRaft,
        store: Store,
        network: Network,
        settings: Settings,
        recovering: Arc<AtomicBool>,
    ) -> Self {
        let leader_raft = raft.clone();
        let leader_store = store.clone();
        let leader = batch::Batcher::new(
            move || {
                let raft = leader_raft.clone();
                let store = leader_store.clone();
                let recovering = recovering.clone();
                async move {
                    if recovering.load(Ordering::Acquire) {
                        // OpenRaft can resume the same persisted leader/term.
                        // A heartbeat read then proves only the restored applied
                        // floor, before its durable tail has been reconfirmed.
                        // Committing this fresh entry proves and applies the
                        // preceding selected prefix without trusting a log tail
                        // that may instead be an abandoned uncommitted suffix.
                        // The leader dispatcher serializes every recovery attempt,
                        // including writer baselines and remote replica fences.
                        let response = raft
                            .client_write(RaftCommand::RecoveryBarrier {
                                recovery_barrier: (),
                            })
                            .await
                            .context("unavailable: startup recovery commit failed")?;
                        anyhow::ensure!(
                            matches!(response.data, ApplyResult::Internal),
                            "unavailable: unexpected startup recovery response"
                        );
                        recovering.store(false, Ordering::Release);
                    }
                    // Strictly leader-only: an RPC sent to a stale leader address
                    // fails here. It must never forward recursively to another node.
                    raft.ensure_linearizable()
                        .await
                        .context("unavailable: quorum read failed")?;
                    store.read_fence()
                }
            },
            settings,
        );
        let leader_dispatch = leader.clone();
        let replica = batch::Batcher::new(
            move || {
                let raft = raft.clone();
                let store = store.clone();
                let network = network.clone();
                let leader = leader_dispatch.clone();
                async move { replica_fence(id, &raft, &store, &network, &leader).await }
            },
            settings,
        );
        Self { leader, replica }
    }

    pub(super) async fn leader_fence(&self) -> anyhow::Result<ReadFence> {
        self.leader.request().await
    }

    pub(super) async fn shutdown(&self) {
        self.replica.shutdown().await;
        self.leader.shutdown().await;
    }
}

async fn replica_fence(
    id: u64,
    raft: &FlowerRaft,
    store: &Store,
    network: &Network,
    local_leader: &batch::Batcher,
) -> anyhow::Result<ReadFence> {
    // Subscribe before the remote proof and publication check, avoiding a
    // missed apply/installation wakeup. This wait is shared by the whole cohort.
    let mut progress = raft.metrics();
    let metrics = progress.borrow().clone();
    let leader = metrics
        .current_leader
        .context("unavailable: no known Raft leader")?;
    let fence = if leader == id {
        local_leader.request().await?
    } else {
        let node = metrics
            .membership_config
            .nodes()
            .find_map(|(member, node)| (*member == leader).then(|| node.clone()))
            .context("unavailable: leader is absent from the Raft membership")?;
        network
            .read_fence(leader, node)
            .await
            .context("unavailable: leader read fence failed")?
    };
    loop {
        if store.snapshot_after_fence(&fence)?.is_some() {
            return Ok(fence);
        }
        progress
            .changed()
            .await
            .context("unavailable: Raft stopped while waiting for read fence")?;
    }
}

impl Consensus {
    /// Fresh, linearizable query data on a leader or follower, without receipts.
    /// Concurrent callers share a sealed cohort, whose authenticated leader
    /// proof begins after every member enrolled. A later caller cannot reuse it.
    pub async fn read_query(&self) -> anyhow::Result<Snapshot> {
        tokio::time::timeout(self.limits.read_timeout, self.read_query_inner())
            .await
            .context("unavailable: fresh replica read timed out")?
    }

    async fn read_query_inner(&self) -> anyhow::Result<Snapshot> {
        let fence = self.reads.replica.request().await?;
        let snapshot = self
            .store
            .snapshot_after_fence_scoped(&fence, self.partition.as_ref())?
            .context("unavailable: local applied state regressed behind the read fence")?;
        self.recovering.store(false, Ordering::Release);
        Ok(snapshot)
    }

    /// Cheap replica-local publication. Callers must explicitly opt into stale
    /// query semantics; writers retain the leader-only read_for APIs.
    pub async fn snapshot_for(&self, request_id: Option<&str>) -> anyhow::Result<Snapshot> {
        if self.recovering.load(Ordering::Acquire) {
            // A previous process may have acknowledged entries whose durable
            // logs survived while the materialized state rolled back. Even an
            // opt-in stale query must not expose a reverted method/auth policy
            // until this process has recovered a quorum-confirmed prefix.
            // These calls share the normal read cohort. Failure/cancellation
            // leaves the gate closed; peer RPCs and initialization bypass it.
            tokio::time::timeout(self.limits.read_timeout, self.reads.replica.request())
                .await
                .context("unavailable: startup recovery read timed out")?
                .context("unavailable: startup recovery requires a quorum")?;
            self.recovering.store(false, Ordering::Release);
        }
        self.selected_snapshot(request_id, false).await
    }
}

/// This route is behind both peer-token and target-node identity middleware.
/// Leadership metrics alone are insufficient: even a restarted or partitioned
/// former leader must obtain a new quorum proof for this exact request.
pub(super) async fn read_fence(
    State(consensus): State<Consensus>,
) -> Json<Result<ReadFence, ReadFenceError>> {
    let result = consensus.reads.leader.request().await;
    Json(result.map_err(|error| ReadFenceError {
        message: format!("{error:#}"),
    }))
}
