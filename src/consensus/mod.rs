//! Durable consensus for Flower. Only validated data patches enter the Raft log;
//! application code is never executed by a replica's state machine.

mod backing;
mod cache;
pub mod changes;
mod command_fields;
mod limits;
mod membership;
mod network;
mod packed;
mod partitions;
mod progress;
mod read;
mod receipts;
mod records;
pub mod retention;
mod snapshot_policy;
mod store;
mod timing;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, bail, ensure};
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::error::RaftError;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::{BasicNode, CommittedLeaderId, Config, RaftMetrics, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use limits::Limits;
pub(crate) use limits::encoded_json_len;
pub use membership::{Compatibility, MembershipChange, MembershipView, PeerInfo, compatibility};
pub use partitions::{
    ExportKind, PartitionBinding, PartitionCommand, PartitionImage, PartitionInfo, PartitionPhase,
    PartitionTransfer, partition_image_digest,
};
pub use progress::Progress;
pub use receipts::Receipts;
pub(crate) use records::HISTORY_MARKER;
pub use records::Records;
pub(crate) use records::{next_version, versions_after, versions_high_water};
pub use store::{SharedDatabase, SnapshotData, Storage};

/// Application state at one committed, atomically published revision.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub revision: u64,
    pub data: Records,
    pub requests: Receipts,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub fingerprint: String,
    pub revision: u64,
    #[serde(default, deserialize_with = "json_payload::value")]
    pub result: Value,
    /// The retry epoch a caller's own request ID ran in, which retention
    /// collects it after. Scoped IDs carry theirs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Commit {
    /// No retry receipt: host-generated maintenance, and mutations that wrote
    /// nothing. This flag is carried only by trusted consensus commands,
    /// never public method input. Their log entries omit the request ID and
    /// fingerprint, which only a receipt uses.
    #[serde(default)]
    pub internal: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fingerprint: String,
    pub expected_revision: u64,
    #[serde(deserialize_with = "json_payload::records")]
    pub puts: BTreeMap<String, Value>,
    pub deletes: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Value::is_null",
        deserialize_with = "json_payload::value"
    )]
    pub result: Value,
}

impl Commit {
    /// As logged: without the identity a receipt-less commit never uses.
    fn logged(mut self) -> Self {
        if self.internal {
            self.request_id.clear();
            self.fingerprint.clear();
        }
        self
    }
}

/// The per-invocation part of an atomic batch. Intermediate patches are omitted:
/// only the final overlay is observable when the Raft entry is published.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BatchItem {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub internal: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fingerprint: String,
    #[serde(
        default,
        skip_serializing_if = "Value::is_null",
        deserialize_with = "json_payload::value"
    )]
    pub result: Value,
}

/// One atomic state transition with an ordered revision and retry receipt for
/// every invocation. Preconditions are checked before applying any part of it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompactBatch {
    pub expected_revision: u64,
    #[serde(deserialize_with = "json_payload::records")]
    pub puts: BTreeMap<String, Value>,
    pub deletes: Vec<String>,
    pub items: Vec<BatchItem>,
}

impl CompactBatch {
    pub fn new(commits: Vec<Commit>) -> anyhow::Result<Self> {
        let expected_revision = commits
            .first()
            .context("a consensus group requires at least one application commit")?
            .expected_revision;
        let mut batch = Self {
            expected_revision,
            puts: BTreeMap::new(),
            deletes: Vec::new(),
            items: Vec::with_capacity(commits.len()),
        };
        let mut deletes = BTreeSet::new();
        for (index, commit) in commits.into_iter().enumerate() {
            if expected_revision.checked_add(index as u64) != Some(commit.expected_revision) {
                bail!("group commits require contiguous expected revisions");
            }
            let commit = commit.logged();
            // A Commit applies deletes before puts. Later invocations replace
            // earlier changes, including put/delete/put of the same key.
            for key in commit.deletes {
                batch.puts.remove(&key);
                deletes.insert(key);
            }
            for (key, value) in commit.puts {
                deletes.remove(&key);
                batch.puts.insert(key, value);
            }
            batch.items.push(BatchItem {
                internal: commit.internal,
                request_id: commit.request_id,
                fingerprint: commit.fingerprint,
                result: commit.result,
            });
        }
        batch.deletes = deletes.into_iter().collect();
        batch.validate()?;
        Ok(batch)
    }

    /// Validate decoded commands too: a Raft proposal is not permission to
    /// bypass atomicity through malformed metadata or revision overflow.
    fn validate(&self) -> anyhow::Result<u64> {
        if self.items.is_empty() {
            bail!("conflict: an atomic batch must not be empty");
        }
        let count = u64::try_from(self.items.len())?;
        let revision = self
            .expected_revision
            .checked_add(count)
            .context("conflict: application revision exhausted")?;
        let mut requests = BTreeSet::new();
        for item in &self.items {
            if !item.internal && !requests.insert(&item.request_id) {
                bail!("conflict: an atomic batch repeats a request ID");
            }
        }
        if self.deletes.iter().any(|key| self.puts.contains_key(key)) {
            bail!("conflict: an atomic batch both puts and deletes the same key");
        }
        Ok(revision)
    }
}

/// One durable Raft entry carries a single commit, a leadership-fenced commit,
/// or a compact atomic batch. Application code never runs during log replay.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum RaftCommand {
    Scoped {
        partition: String,
        epoch: u64,
        command: Box<RaftCommand>,
    },
    PartitionControl {
        partition_control: PartitionCommand,
        leader_id: Option<CommittedLeaderId<u64>>,
    },
    Retention {
        retention: retention::Command,
    },
    Batch {
        batch: CompactBatch,
    },
    /// A preparation authorized by remote state must not outlive the local
    /// leadership term in which that remote decision was observed.
    Fenced {
        leader_id: CommittedLeaderId<u64>,
        commit: Commit,
    },
    /// Commit a fresh physical-group prefix after recovering a durable log
    /// whose last acknowledged entries may be ahead of its saved projection.
    /// This advances only the applied Raft position, never application state.
    RecoveryBarrier {
        recovery_barrier: (),
    },
    Single(Commit),
}

impl<'de> Deserialize<'de> for RaftCommand {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Serde's derived untagged decoder first buffers the whole command as
        // recursive Content. That would spend the outer Raft/RPC depth before
        // our per-record payload decoder can reset it. Preserve the same
        // batch-first/single fallback and wire shape without that buffer.
        let raw = <Box<serde_json::value::RawValue>>::deserialize(deserializer)?;
        Self::decode_raw(&raw, command_fields::Fields::inspect(&raw))
            .map_err(serde::de::Error::custom)
    }
}

impl RaftCommand {
    fn decode_raw(
        raw: &serde_json::value::RawValue,
        fields: command_fields::Fields,
    ) -> serde_json::Result<Self> {
        use command_fields::Fields;
        // Skip impossible variants in one outer-key scan without buffering payloads. A
        // multi-megabyte batch otherwise gets scanned once for every preceding
        // untagged variant. Non-object encodings retain the old derived-struct
        // fallback (including positional sequences), rather than gaining a new
        // acceptance rule from this optimization.
        #[derive(Deserialize)]
        struct Scoped<'a> {
            partition: String,
            epoch: u64,
            #[serde(borrow)]
            command: &'a serde_json::value::RawValue,
        }
        if fields.has(Fields::SCOPED)
            && let Ok(Scoped {
                partition,
                epoch,
                command,
            }) = serde_json::from_str(raw.get())
        {
            let fields: Fields = serde_json::from_str(command.get())?;
            if fields.has(Fields::PARTITION)
                || fields.has(Fields::CONTROL)
                || fields.has(Fields::RECOVERY)
            {
                return Err(serde::de::Error::custom(
                    "nested partition scopes and controls, including recovery barriers, are invalid",
                ));
            }
            let command = Self::decode_raw(command, fields)?;
            return Ok(Self::Scoped {
                partition,
                epoch,
                command: Box::new(command),
            });
        }
        #[derive(Deserialize)]
        struct Control {
            partition_control: PartitionCommand,
            leader_id: Option<CommittedLeaderId<u64>>,
        }
        if fields.has(Fields::CONTROL)
            && let Ok(Control {
                partition_control,
                leader_id,
            }) = serde_json::from_str(raw.get())
        {
            return Ok(Self::PartitionControl {
                partition_control,
                leader_id,
            });
        }
        #[derive(Deserialize)]
        struct Retention {
            retention: retention::Command,
        }
        if fields.has(Fields::RETENTION)
            && let Ok(Retention { retention }) = serde_json::from_str(raw.get())
        {
            return Ok(Self::Retention { retention });
        }
        #[derive(Deserialize)]
        struct Batch {
            batch: CompactBatch,
        }
        if fields.has(Fields::BATCH)
            && let Ok(Batch { batch }) = serde_json::from_str(raw.get())
        {
            return Ok(Self::Batch { batch });
        }
        #[derive(Deserialize)]
        struct Fenced {
            leader_id: CommittedLeaderId<u64>,
            commit: Commit,
        }
        if fields.has(Fields::FENCED)
            && let Ok(Fenced { leader_id, commit }) = serde_json::from_str(raw.get())
        {
            return Ok(Self::Fenced { leader_id, commit });
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Recovery {
            recovery_barrier: (),
        }
        // A new control marker must not capture mixed legacy application
        // commands or previously invalid positional encodings such as [null].
        if fields.has(Fields::RECOVERY)
            && raw.get().trim_start().starts_with('{')
            && let Ok(Recovery { recovery_barrier }) = serde_json::from_str(raw.get())
        {
            return Ok(Self::RecoveryBarrier { recovery_barrier });
        }
        serde_json::from_str(raw.get()).map(Self::Single)
    }
}

/// Trusted transport/storage wrappers do not consume a user's JSON depth
/// budget. Each record/result is still parsed by serde_json with its ordinary
/// bounded recursion limit; no parser disables that limit.
mod json_payload {
    use super::*;
    use serde_json::value::RawValue;

    pub(super) fn value<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Value, D::Error> {
        let raw = <Box<RawValue>>::deserialize(deserializer)?;
        serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)
    }

    pub(super) fn records<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, Value>, D::Error> {
        let records = BTreeMap::<String, Box<RawValue>>::deserialize(deserializer)?;
        records
            .into_iter()
            .map(|(key, raw)| {
                serde_json::from_str(raw.get())
                    .map(|value| (key, value))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

impl From<Commit> for RaftCommand {
    fn from(commit: Commit) -> Self {
        Self::Single(commit.logged())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitResult {
    pub revision: u64,
    pub duplicate: bool,
    #[serde(default, deserialize_with = "json_payload::value")]
    pub result: Value,
}

/// A rejected application command is still an applied Raft entry. It must not
/// advance the application's revision or change its request receipts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ApplyResult {
    Committed(CommitResult),
    Rejected(String),
    Internal,
    Batch(Vec<ApplyResult>),
    Partition(PartitionInfo),
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftCommand,
        R = ApplyResult,
        SnapshotData = SnapshotData,
);

type FlowerRaft = openraft::Raft<TypeConfig>;

const RPC_TARGET_HEADER: &str = "x-flower-target-node-id";
const RPC_NODE_HEADER: &str = "x-flower-node-id";

#[derive(Clone)]
pub struct Consensus {
    partition: Option<PartitionBinding>,
    partition_overhead: usize,
    id: u64,
    address: String,
    raft: FlowerRaft,
    store: store::Store,
    storage_drained: tokio::sync::watch::Receiver<()>,
    reads: read::Dispatchers,
    // Shared by root and partition handles. A crash can leave durable log
    // entries ahead of the last materialized state; local application reads
    // must establish one fresh recovery fence before exposing that state.
    recovering: Arc<AtomicBool>,
    network: network::Network,
    membership_changes: Arc<tokio::sync::Mutex<()>>,
    limits: Arc<Limits>,
    token: Arc<str>,
    peer_token: Arc<str>,
    _snapshot_scheduler: Arc<tokio::sync::watch::Sender<()>>,
    progress: tokio::sync::watch::Receiver<Progress>,
    _progress: Arc<tokio::sync::watch::Sender<()>>,
    // Set once the process stops accepting connections, so that responses
    // that never end, like watches, end rather than hold up the drain.
    draining: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Consensus {
    /// Open a replica: a data directory of its own, or a `Storage::Shared`
    /// database that it shares with the other replicas of this process.
    pub async fn open(
        id: u64,
        address: String,
        storage: Storage,
        token: String,
    ) -> anyhow::Result<Self> {
        let peer_token = crate::transport::peer_token(&token)?;
        Self::open_with_tokens(id, address, storage, token, peer_token).await
    }

    pub async fn open_with_tokens(
        id: u64,
        address: String,
        storage: Storage,
        token: String,
        peer_token: String,
    ) -> anyhow::Result<Self> {
        crate::transport::validate_configuration()?;
        validate_address(&address)?;
        ensure!(
            !peer_token.trim().is_empty(),
            "a nonempty peer bearer token is required"
        );
        if token.trim().is_empty() {
            bail!("a nonempty operator bearer token is required");
        }
        // Validate admission before opening or creating durable state.
        let limits = Arc::new(Limits::from_env()?);
        let read_settings = read::Settings::from_env(limits.read_timeout)?;
        let config = timing::configure(Config {
            cluster_name: "flower".into(),
            // OpenRaft adds election_timeout_max as a leader lease before the
            // randomized election delay: these settings yield 450–600ms before
            // tick/network/storage overhead, rather than the previous 2.4–3.2s.
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            install_snapshot_timeout: limits.snapshot_timeout.as_millis() as u64,
            max_payload_entries: limits.raft_payload_entries,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(limits.snapshot_after_logs),
            replication_lag_threshold: limits.snapshot_lag_logs,
            snapshot_max_chunk_size: limits.snapshot_chunk_bytes as u64,
            max_in_snapshot_log_to_keep: limits.snapshot_keep_logs,
            purge_batch_size: limits.snapshot_purge_batch_logs,
            ..Config::default()
        })?;
        let network = network::Network::with_limits(&peer_token, limits.clone())?;
        let store = store::Store::open(id, storage).await?;
        let log_state =
            openraft::storage::RaftLogStorage::get_log_state(&mut store.clone()).await?;
        let applied = store.read_fence().ok().map(|fence| fence.applied);
        let recovering = Arc::new(AtomicBool::new(
            log_state.last_log_id.map(|id| id.index) > applied.map(|id| id.index),
        ));
        let last_index = [log_state.last_log_id, applied]
            .into_iter()
            .flatten()
            .map(|log_id| log_id.index)
            .max();
        limits.validate_log_index(last_index)?;
        let (raft_store, storage_drained) = store.raft_storage();
        // OpenRaft's leader lease is election_timeout_max: past it without a
        // quorum acknowledgement, followers may already follow someone else.
        let lease = std::time::Duration::from_millis(config.election_timeout_max);
        let raft = FlowerRaft::new(
            id,
            Arc::new(config),
            network.clone(),
            raft_store.clone(),
            raft_store,
        )
        .await
        .context("start Raft")?;
        let snapshot_scheduler =
            snapshot_policy::spawn(raft.clone(), store.snapshot_accounting(), limits.clone());
        let (progress_lifetime, progress) = progress::spawn(raft.clone(), lease);
        let reads = read::Dispatchers::new(
            id,
            raft.clone(),
            store.clone(),
            network.clone(),
            read_settings,
            recovering.clone(),
        );
        Ok(Self {
            partition: None,
            partition_overhead: 0,
            id,
            address,
            raft,
            store,
            storage_drained,
            reads,
            recovering,
            network,
            membership_changes: Arc::new(tokio::sync::Mutex::new(())),
            limits,
            token: token.into(),
            peer_token: peer_token.into(),
            _snapshot_scheduler: snapshot_scheduler,
            progress,
            _progress: progress_lifetime,
            draining: Arc::new(tokio::sync::watch::Sender::new(false)),
        })
    }

    pub(crate) fn peer_token(&self) -> &str {
        &self.peer_token
    }

    /// Peer protocol routes and operator control routes use separate tokens.
    /// No public endpoint accepts arbitrary precomputed application commits.
    pub fn router(&self) -> Router {
        let peers = Router::new()
            .route("/raft/append", post(append))
            .route("/raft/vote", post(vote))
            .route("/raft/snapshot", post(install_snapshot))
            .route("/raft/read-fence", post(read::read_fence))
            .route("/raft/version", get(membership::version))
            .route_layer(middleware::from_fn_with_state(
                self.id,
                require_rpc_identity,
            ))
            .route_layer(middleware::from_fn_with_state(
                self.peer_token.clone(),
                authorize,
            ));
        let operators = Router::new()
            .route("/raft/metrics", get(metrics))
            .route("/raft/initialize", post(initialize))
            .route(
                "/raft/membership",
                get(membership::inspect).post(membership::change),
            )
            .route_layer(middleware::from_fn_with_state(
                self.token.clone(),
                authorize,
            ));
        peers
            .merge(operators)
            .layer(DefaultBodyLimit::max(self.limits.rpc_max_bytes))
            .with_state(self.clone())
    }

    pub async fn read(&self) -> anyhow::Result<Snapshot> {
        self.read_barrier().await?;
        self.selected_snapshot(None, true).await
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Exact space available to an unscoped command before the logical
    /// partition binding is added for replication.
    pub(crate) fn command_payload_limit(&self) -> usize {
        self.limits
            .transaction_max_bytes
            .saturating_sub(self.partition_overhead)
    }

    /// Local scheduling telemetry, independent of consensus correctness.
    pub fn snapshot_policy_metrics(&self) -> Value {
        self.store.snapshot_accounting().metrics(&self.limits)
    }

    /// Batched storage commits of this process's database: every replica it
    /// hosts reports the same counters.
    pub fn storage_metrics(&self) -> Value {
        self.store.storage_metrics()
    }

    /// Service admission includes the binding envelope before collecting a
    /// group; the final compact proposal is checked again before replication.
    pub fn commit_group_fits(&self, encoded_command_bytes: usize, count: usize) -> bool {
        encoded_command_bytes
            .checked_add(self.partition_overhead)
            .is_some_and(|bytes| self.limits.commit_group_fits(bytes, count))
    }

    /// Linearizable application read with complete data and at most the named
    /// retry receipt. Pass None for queries and internal maintenance. The full
    /// receipt history remains durable and available through read/local_snapshot.
    pub async fn read_for(&self, request_id: Option<&str>) -> anyhow::Result<Snapshot> {
        self.read_barrier().await?;
        self.selected_snapshot(request_id, false).await
    }

    /// One quorum barrier and data clone for a proposed application commit group.
    pub async fn read_for_many(&self, request_ids: &[String]) -> anyhow::Result<Snapshot> {
        self.read_barrier().await?;
        if self.partition.is_none() {
            return Ok(self.store.snapshot_for_many(request_ids).await);
        }
        let mut snapshot = self.selected_snapshot(None, true).await?;
        snapshot.requests = request_ids
            .iter()
            .filter_map(|id| {
                snapshot
                    .requests
                    .get(id)
                    .map(|receipt| (id.clone(), receipt.clone()))
            })
            .collect();
        Ok(snapshot)
    }

    /// Establish the writer's linearizable baseline, including all historical
    /// receipts. Both trees share their immutable roots; successors can stage
    /// updates and resolve retries without another barrier or a storage lock.
    pub async fn read_for_writer(&self) -> anyhow::Result<Snapshot> {
        self.read_barrier().await?;
        self.selected_snapshot(None, true).await
    }

    /// Capture a fresh writer baseline and the stable leadership term that
    /// authorized it. Call this before observing remote state, then submit
    /// dependent preparation with commit_in_term; a process-local lock cannot
    /// protect that observation across leader changes and a later reelection.
    pub async fn read_for_writer_with_term(&self) -> anyhow::Result<(Snapshot, u64)> {
        let term = self.metrics().current_term;
        let state = self.read_for_writer().await?;
        let after = self.metrics();
        anyhow::ensure!(
            after.current_term == term && after.current_leader == Some(self.id),
            "unavailable: leadership changed while capturing the writer term"
        );
        Ok((state, term))
    }

    pub(crate) async fn read_barrier(&self) -> anyhow::Result<()> {
        tokio::time::timeout(self.limits.read_timeout, self.reads.leader_fence())
            .await
            .context("unavailable: quorum read timed out")?
            .context("unavailable: quorum read failed")?;
        Ok(())
    }

    /// A local snapshot is for diagnostics and replication-aware callers. It
    /// does not establish leadership and can be stale on a follower.
    pub async fn local_snapshot(&self) -> Snapshot {
        match &self.partition {
            Some(binding) => self
                .store
                .partition_state(&binding.partition)
                .map(|state| state.snapshot)
                .unwrap_or_default(),
            None => self.store.snapshot().await,
        }
    }

    async fn selected_snapshot(
        &self,
        request_id: Option<&str>,
        all_receipts: bool,
    ) -> anyhow::Result<Snapshot> {
        match &self.partition {
            Some(binding) => self
                .store
                .partition_snapshot(binding, request_id, all_receipts),
            None if all_receipts => Ok(self.store.snapshot_for_writer().await),
            None => Ok(self.store.snapshot_for(request_id).await),
        }
    }

    pub async fn commit(&self, commit: Commit) -> anyhow::Result<CommitResult> {
        self.commit_command(commit.into()).await
    }

    /// The replicated state machine rejects this command if Raft appends it
    /// under a different leader identity (term and node ID), including a process reelected
    /// after another leader completed a remote abort without preparing here.
    pub async fn commit_in_term(
        &self,
        commit: Commit,
        expected_term: u64,
    ) -> anyhow::Result<CommitResult> {
        self.commit_command(RaftCommand::Fenced {
            leader_id: CommittedLeaderId::new(expected_term, self.id),
            commit,
        })
        .await
    }

    async fn commit_command(&self, command: RaftCommand) -> anyhow::Result<CommitResult> {
        let command = self.scope_command(command);
        // A single command must fit the transport even when replication falls
        // back to one entry per RPC after receiving a payload-size rejection.
        if encoded_json_len(&command)? > self.limits.transaction_max_bytes {
            bail!(
                "transaction exceeds FLOWER_TRANSACTION_MAX_BYTES ({})",
                self.limits.transaction_max_bytes
            );
        }
        let response =
            tokio::time::timeout(self.limits.commit_timeout, self.raft.client_write(command))
                .await
                .context(
                    "unavailable: commit timed out; outcome unknown, retry the same request ID",
                )?
                .context("unavailable: Raft commit failed")?;
        match response.data {
            ApplyResult::Committed(result) => Ok(result),
            ApplyResult::Rejected(reason) => bail!(reason),
            ApplyResult::Internal => {
                bail!("unexpected internal Raft response to application commit")
            }
            ApplyResult::Partition(_) | ApplyResult::Batch(_) => {
                bail!("unexpected batch Raft response to a single application commit")
            }
        }
    }

    /// Replicate one atomic final overlay and ordered per-invocation receipts.
    /// Any conflict rejects the entire group; a complete matching receipt set
    /// replays the original outcomes without applying the overlay again.
    pub async fn commit_many(&self, commits: Vec<Commit>) -> anyhow::Result<Vec<ApplyResult>> {
        self.submit_compact(CompactBatch::new(commits)?).await
    }

    /// Writer-local shared patches are compacted before becoming owned JSON.
    /// Validate this alternate construction path before any Raft submission.
    pub(crate) async fn commit_compact(
        &self,
        batch: CompactBatch,
    ) -> anyhow::Result<Vec<ApplyResult>> {
        batch.validate()?;
        self.submit_compact(batch).await
    }

    async fn submit_compact(&self, batch: CompactBatch) -> anyhow::Result<Vec<ApplyResult>> {
        let count = batch.items.len();
        let command = self.scope_command(RaftCommand::Batch { batch });
        if encoded_json_len(&command)? > self.limits.transaction_max_bytes {
            bail!(
                "transaction group exceeds FLOWER_TRANSACTION_MAX_BYTES ({})",
                self.limits.transaction_max_bytes
            );
        }
        let response = tokio::time::timeout(
            self.limits.commit_timeout,
            self.raft.client_write(command),
        )
        .await
        .context(
            "unavailable: group commit timed out; outcomes unknown, retry the same request IDs",
        )?
        .context("unavailable: Raft group commit failed")?;
        match response.data {
            ApplyResult::Batch(results) if results.len() == count => Ok(results),
            _ => bail!("unexpected Raft response to application commit group"),
        }
    }

    /// Bootstrap once, on one node, with the complete initial membership.
    pub async fn initialize(&self, members: BTreeMap<u64, String>) -> anyhow::Result<()> {
        if members.is_empty() {
            bail!("initial membership must not be empty");
        }
        if members.get(&self.id) != Some(&self.address) {
            bail!("initial membership must contain this node's ID and advertised address");
        }
        let mut unique_addresses = BTreeSet::new();
        for address in members.values() {
            validate_address(address)?;
            if !unique_addresses.insert(address) {
                bail!("each Raft member must have a distinct advertised address");
            }
        }
        let members = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect::<BTreeMap<_, _>>();
        self.raft
            .initialize(members)
            .await
            .context("initialize Raft membership")?;
        Ok(())
    }

    pub fn metrics(&self) -> RaftMetrics<u64, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Subscribe before a watch's initial read so an intervening commit cannot
    /// be lost. Consumers filter/coalesce metrics; this allocates no state copy.
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<RaftMetrics<u64, BasicNode>> {
        self.raft.metrics()
    }

    /// Subscribe before a watch's initial read so an intervening commit cannot
    /// be lost. Changes only when applied state moves, freshness becomes
    /// suspect, or the replica stops.
    pub(crate) fn progress(&self) -> tokio::sync::watch::Receiver<Progress> {
        self.progress.clone()
    }

    /// End responses that never end on their own, such as watches: the
    /// process is shutting down and drains its connections.
    pub fn drain(&self) {
        self.draining.send_replace(true);
    }

    pub(crate) fn draining(&self) -> tokio::sync::watch::Receiver<bool> {
        self.draining.subscribe()
    }

    /// The keys of every publication from now on, of every application state;
    /// `changes::Changes::concern` picks this handle's.
    pub(crate) fn changes(&self) -> tokio::sync::broadcast::Receiver<Arc<changes::Changes>> {
        self.store.changes()
    }

    /// The revision this handle's application state is published at now.
    /// Subscribe to `changes` first: what it misses is at most this.
    pub(crate) fn published_revision(&self) -> Option<u64> {
        match &self.partition {
            Some(binding) => self
                .store
                .partition_snapshot(binding, None, false)
                .ok()
                .map(|snapshot| snapshot.revision),
            None => self.store.read_fence().ok().map(|fence| fence.revision),
        }
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.reads.shutdown().await;
        self.raft.shutdown().await.context("shut down Raft")?;
        // OpenRaft 0.9 joins its core/tick, while replication/state-machine
        // workers can finish later. Their storage handles (and uncancellable
        // blocking I/O) keep the database lock alive. Join that precise lifetime
        // before returning; router/Consensus clones carry no sender, so this
        // wait cannot depend on the caller dropping this Consensus first.
        let _ = self.storage_drained.clone().changed().await;
        Ok(())
    }
}

/// Transport endpoints must not impersonate another configured voter. Address
/// aliases can otherwise cause one process to be counted repeatedly in a quorum.
async fn require_rpc_identity(State(id): State<u64>, request: Request, next: Next) -> Response {
    let target = request
        .headers()
        .get(RPC_TARGET_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let peer_contract = request
        .headers()
        .get(membership::COMPATIBILITY_HEADER)
        .and_then(|value| value.to_str().ok());
    let mut response = if target != Some(id) {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "Raft RPC target does not match this node" })),
        )
            .into_response()
    } else if peer_contract != Some(membership::contract()) {
        (
            StatusCode::UPGRADE_REQUIRED,
            Json(serde_json::json!({"error":"incompatible Raft peer; wire, state, snapshot, value and QuickJS contracts must match", "compatibility":compatibility()})),
        ).into_response()
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        RPC_NODE_HEADER,
        id.to_string()
            .parse()
            .expect("numeric node ID is a valid header"),
    );
    response.headers_mut().insert(
        membership::COMPATIBILITY_HEADER,
        membership::contract_header().clone(),
    );
    response
}

async fn authorize(State(token): State<Arc<str>>, request: Request, next: Next) -> Response {
    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided != Some(token.as_ref()) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }
    next.run(request).await
}

fn validate_address(address: &str) -> anyhow::Result<()> {
    let authority: axum::http::uri::Authority = address
        .parse()
        .context("invalid node address; expected host:port")?;
    if authority.port_u16().is_none() || authority.host().is_empty() || address.contains('@') {
        bail!("invalid node address; expected host:port");
    }
    Ok(())
}

async fn append(
    State(consensus): State<Consensus>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<u64>, RaftError<u64>>> {
    Json(consensus.raft.append_entries(request).await)
}

async fn vote(
    State(consensus): State<Consensus>,
    Json(request): Json<VoteRequest<u64>>,
) -> Json<Result<VoteResponse<u64>, RaftError<u64>>> {
    Json(consensus.raft.vote(request).await)
}

async fn install_snapshot(State(consensus): State<Consensus>, body: axum::body::Bytes) -> Response {
    // Segments travel as bytes rather than JSON; see network::encode_segment.
    match network::decode_segment(&body) {
        Ok(request) => Json(consensus.raft.install_snapshot(request).await).into_response(),
        Err(error) => (axum::http::StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn metrics(State(consensus): State<Consensus>) -> Json<RaftMetrics<u64, BasicNode>> {
    Json(consensus.metrics())
}

async fn initialize(
    State(consensus): State<Consensus>,
    Json(members): Json<BTreeMap<u64, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    consensus.initialize(members).await.map_err(|error| {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": format!("{error:#}") })),
        )
    })?;
    Ok(Json(serde_json::json!({ "initialized": true })))
}
