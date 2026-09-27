//! Operator-controlled topology changes and the rolling-upgrade compatibility contract.
//!
//! Membership and addresses live in OpenRaft's durable log/snapshots. Never replace
//! the address of an existing node ID: introduce a fresh learner and retire the old ID.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
};

use anyhow::{Context, Result, ensure};
use axum::{Json, extract::State, http::StatusCode};
use openraft::{BasicNode, LogId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{Consensus, validate_address};

pub(super) const COMPATIBILITY_HEADER: &str = "x-flower-compatibility";

/// Bump the relevant field before publishing an incompatible protocol, command,
/// snapshot, or evaluator semantics change. A package/build version is diagnostic;
/// it deliberately does not exclude a compatible bugfix release.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Compatibility {
    pub raft_wire: u32,
    pub state_machine: u32,
    pub snapshot_format: u32,
    pub value_format: u32,
    pub quickjs_sha256: String,
    pub build: String,
}

impl Compatibility {
    pub fn contract(&self) -> String {
        format!(
            "raft{}-state{}-snapshot{}-value{}-qjs{}",
            self.raft_wire,
            self.state_machine,
            self.snapshot_format,
            self.value_format,
            self.quickjs_sha256
        )
    }

    pub fn compatible_with(&self, other: &Self) -> bool {
        self.raft_wire == other.raft_wire
            && self.state_machine == other.state_machine
            && self.snapshot_format == other.snapshot_format
            && self.value_format == other.value_format
            && self.quickjs_sha256 == other.quickjs_sha256
    }
}

pub fn compatibility() -> &'static Compatibility {
    static CURRENT: OnceLock<Compatibility> = OnceLock::new();
    CURRENT.get_or_init(|| Compatibility {
        // Snapshot segments are a JSON header followed by their raw bytes.
        raft_wire: 10,
        // Mutations may be declared without receipts, which older binaries
        // can neither parse in the method registry nor honor.
        state_machine: 16,
        snapshot_format: 4,
        value_format: 1,
        quickjs_sha256: crate::evaluator::hash(include_bytes!(
            "../../vendor/quickjs-ng/quickjs.wasm"
        )),
        build: env!("CARGO_PKG_VERSION").into(),
    })
}

pub(super) fn contract() -> &'static str {
    static CONTRACT: OnceLock<String> = OnceLock::new();
    CONTRACT.get_or_init(|| compatibility().contract())
}

pub(super) fn contract_header() -> &'static axum::http::HeaderValue {
    static HEADER: OnceLock<axum::http::HeaderValue> = OnceLock::new();
    HEADER.get_or_init(|| {
        contract()
            .parse()
            .expect("compatibility contract is a valid header")
    })
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PeerInfo {
    pub id: u64,
    pub address: String,
    pub initialized: bool,
    pub compatibility: Compatibility,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MembershipChange {
    /// The complete desired voter set. Removed voters are not retained as learners.
    pub members: BTreeMap<u64, String>,
    /// Optional optimistic precondition from GET /raft/membership. A timeout may
    /// have added learners or committed a joint config; inspect before retrying.
    #[serde(default)]
    pub expected_log_id: Option<LogId<u64>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MembershipView {
    pub leader: Option<u64>,
    pub log_id: Option<LogId<u64>>,
    pub last_applied: Option<LogId<u64>>,
    pub voter_configs: Vec<BTreeSet<u64>>,
    pub nodes: BTreeMap<u64, String>,
    pub compatibility: Compatibility,
}

impl Consensus {
    pub fn membership(&self) -> MembershipView {
        let metrics = self.metrics();
        MembershipView {
            leader: metrics.current_leader,
            log_id: *metrics.membership_config.log_id(),
            last_applied: metrics.last_applied,
            voter_configs: metrics
                .membership_config
                .membership()
                .get_joint_config()
                .clone(),
            nodes: metrics
                .membership_config
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
            compatibility: compatibility().clone(),
        }
    }

    pub async fn reconfigure(&self, change: MembershipChange) -> Result<MembershipView> {
        validate_members(&change.members)?;
        tokio::time::timeout(self.limits.commit_timeout, self.reconfigure_inner(change))
            .await
            .context("membership operation timed out; it may have partially or fully committed; inspect membership before retrying")?
    }

    async fn reconfigure_inner(&self, change: MembershipChange) -> Result<MembershipView> {
        let _operation = self.membership_changes.lock().await;
        self.read_barrier()
            .await
            .context("membership changes must be sent to the current leader with a live quorum")?;
        let initial = self.membership();
        if let Some(expected) = change.expected_log_id {
            ensure!(
                initial.log_id == Some(expected),
                "membership changed since the operator's precondition"
            );
        }
        let desired: BTreeSet<_> = change.members.keys().copied().collect();
        if initial.voter_configs.len() > 1 {
            ensure!(
                initial.voter_configs.last() == Some(&desired),
                "a joint membership change is already in progress; finish its target voter set first"
            );
        }
        for (id, address) in &change.members {
            if let Some(previous) = initial.nodes.get(id) {
                ensure!(
                    previous == address,
                    "cannot change the address of existing node {id}; replace it using a new node ID"
                );
            }
            ensure!(
                !initial
                    .nodes
                    .iter()
                    .any(|(other, known)| other != id && known == address),
                "address {address} already belongs to another member; start a replacement at a distinct address"
            );
            let peer = if *id == self.id {
                self.peer_info().await?
            } else {
                self.network.peer_info(*id, address).await?
            };
            ensure!(
                peer.id == *id && peer.address == *address,
                "peer {id} does not advertise the requested identity/address"
            );
            ensure!(
                compatibility().compatible_with(&peer.compatibility),
                "peer {id} has an incompatible protocol, state machine, snapshot, value format or QuickJS guest"
            );
            ensure!(
                initial.nodes.contains_key(id) || !peer.initialized,
                "new node {id} already has Raft state; use a fresh node ID and empty data directory"
            );
        }
        // Add only missing nodes. The committed membership record publishes their
        // addresses and starts replication without changing the old voter quorum.
        for (id, address) in &change.members {
            if !initial.nodes.contains_key(id) {
                self.raft
                    .add_learner(*id, BasicNode::new(address.clone()), false)
                    .await
                    .with_context(|| format!("add learner {id}"))?;
            }
        }
        // Capture a committed leader prefix after learner registration, then wait
        // for every proposed voter to have it. Do not rely on add_learner's
        // configurable lag threshold as proof of catching up to this operation.
        self.read_barrier()
            .await
            .context("quorum lost while adding learners")?;
        let fence = self.store.read_fence()?.applied;
        let mut progress = self.raft.metrics();
        let term = progress.borrow().current_term;
        loop {
            {
                let metrics = progress.borrow();
                ensure!(
                    metrics.current_leader == Some(self.id) && metrics.current_term == term,
                    "leadership changed during learner catch-up; inspect membership and retry on the current leader"
                );
                let caught_up = desired.iter().all(|id| {
                    *id == self.id
                        || metrics
                            .replication
                            .as_ref()
                            .and_then(|replication| replication.get(id))
                            .and_then(|position| *position)
                            .is_some_and(|position| {
                                position.index > fence.index || position == fence
                            })
                });
                if caught_up {
                    break;
                }
            }
            progress
                .changed()
                .await
                .context("Raft stopped during learner catch-up")?;
        }
        let response = self.raft.change_membership(desired, false).await
            .context("commit joint/uniform membership; inspect membership before retrying an uncertain result")?;
        let committed = response
            .membership
            .context("membership response lacks committed configuration")?;
        Ok(MembershipView {
            leader: self.metrics().current_leader,
            log_id: Some(response.log_id),
            last_applied: Some(response.log_id),
            voter_configs: committed.get_joint_config().clone(),
            nodes: committed
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
            compatibility: compatibility().clone(),
        })
    }

    async fn peer_info(&self) -> Result<PeerInfo> {
        Ok(PeerInfo {
            id: self.id,
            address: self.address.clone(),
            initialized: self.raft.is_initialized().await?,
            compatibility: compatibility().clone(),
        })
    }
}

fn validate_members(members: &BTreeMap<u64, String>) -> Result<()> {
    ensure!(!members.is_empty(), "voter set must not be empty");
    let mut addresses = BTreeSet::new();
    for (id, address) in members {
        ensure!(*id > 0, "node IDs must be positive");
        validate_address(address)?;
        ensure!(
            addresses.insert(address),
            "each node must have a distinct advertised address"
        );
    }
    Ok(())
}

pub(super) async fn inspect(State(consensus): State<Consensus>) -> Json<MembershipView> {
    Json(consensus.membership())
}

pub(super) async fn change(
    State(consensus): State<Consensus>,
    Json(change): Json<MembershipChange>,
) -> Result<Json<MembershipView>, (StatusCode, Json<Value>)> {
    consensus.reconfigure(change).await.map(Json).map_err(|error| (
        StatusCode::CONFLICT,
        Json(json!({"error":format!("{error:#}"), "membership":consensus.membership(),
            "note":"Inspect the membership before retrying: learner additions or the voter change may have committed."})),
    ))
}

pub(super) async fn version(
    State(consensus): State<Consensus>,
) -> Result<Json<PeerInfo>, (StatusCode, Json<Value>)> {
    consensus.peer_info().await.map(Json).map_err(|error| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":error.to_string()})),
        )
    })
}
