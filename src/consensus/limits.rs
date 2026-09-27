//! Operator resource policy, separate from Raft's safety/numeric invariants.
//! All cluster members should use the same transport and snapshot settings.

use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use serde::Serialize;

use super::{RaftCommand, TypeConfig};

/// Count the exact wire encoding without allocating a throwaway JSON buffer.
/// Use the same serializer as transport/storage so escaping, floats and custom
/// serializers cannot make admission undercount the real encoded command.
pub(crate) fn encoded_json_len<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<usize> {
    let mut counter = JsonByteCount(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

struct JsonByteCount(usize);

impl Write for JsonByteCount {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_all(bytes)?;
        Ok(bytes.len())
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("encoded JSON size exceeds usize"))?;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Consensus admission and transport budgets, fixed for a node's lifetime.
#[derive(Clone, Debug)]
pub struct Limits {
    pub read_timeout: Duration,
    pub commit_timeout: Duration,
    pub transaction_max_bytes: usize,
    pub rpc_max_bytes: usize,
    pub append_timeout: Duration,
    pub peer_connect_timeout: Duration,
    pub peer_idle_timeout: Duration,
    pub snapshot_timeout: Duration,
    pub snapshot_chunk_bytes: usize,
    pub snapshot_after_logs: u64,
    pub snapshot_after_bytes: u64,
    pub snapshot_max_age: Duration,
    pub snapshot_check_interval: Duration,
    pub snapshot_duty_percent: u8,
    pub snapshot_lag_logs: u64,
    pub snapshot_keep_logs: u64,
    pub snapshot_purge_batch_logs: u64,
    pub raft_payload_entries: u64,
    /// How long applied states wait to be written to redb together.
    pub persist_interval: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(10),
            commit_timeout: Duration::from_secs(15),
            transaction_max_bytes: 32 * 1024 * 1024,
            rpc_max_bytes: 64 * 1024 * 1024,
            append_timeout: Duration::from_secs(5),
            peer_connect_timeout: Duration::from_millis(500),
            peer_idle_timeout: Duration::from_secs(30),
            snapshot_timeout: Duration::from_secs(10),
            snapshot_chunk_bytes: 1024 * 1024,
            snapshot_after_logs: 256,
            // Bounds the retained log and its replay. Checkpoints keep no
            // read transaction, so they cost no file space between them.
            snapshot_after_bytes: 16 * 1024 * 1024,
            snapshot_max_age: Duration::from_secs(300),
            snapshot_check_interval: Duration::from_secs(1),
            snapshot_duty_percent: 10,
            snapshot_lag_logs: 512,
            snapshot_keep_logs: 64,
            snapshot_purge_batch_logs: 1,
            raft_payload_entries: 64,
            persist_interval: Duration::from_millis(500),
        }
    }
}

impl Limits {
    pub fn from_env() -> Result<Self> {
        Self::parse(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read {name}")),
        })
    }

    fn parse(read: impl Fn(&str) -> Result<Option<String>>) -> Result<Self> {
        let mut limits = Self::default();
        for (name, value) in [
            ("FLOWER_READ_TIMEOUT_MS", &mut limits.read_timeout),
            ("FLOWER_COMMIT_TIMEOUT_MS", &mut limits.commit_timeout),
            ("FLOWER_APPEND_TIMEOUT_MS", &mut limits.append_timeout),
            (
                "FLOWER_PEER_CONNECT_TIMEOUT_MS",
                &mut limits.peer_connect_timeout,
            ),
            ("FLOWER_PEER_IDLE_TIMEOUT_MS", &mut limits.peer_idle_timeout),
            ("FLOWER_SNAPSHOT_TIMEOUT_MS", &mut limits.snapshot_timeout),
            (
                "FLOWER_SNAPSHOT_CHECK_MS",
                &mut limits.snapshot_check_interval,
            ),
        ] {
            if let Some(raw) = read(name)? {
                let millis: u64 = raw.parse().with_context(|| {
                    format!("{name} must be a positive integer number of milliseconds")
                })?;
                ensure!(millis > 0, "{name} must be positive");
                *value = Duration::from_millis(millis);
            }
            ensure!(
                Instant::now().checked_add(*value).is_some(),
                "{name} exceeds this platform's deadline range"
            );
        }
        if let Some(raw) = read("FLOWER_PERSIST_INTERVAL_MS")? {
            let millis: u64 = raw.parse().context(
                "FLOWER_PERSIST_INTERVAL_MS must be a nonnegative integer number of milliseconds",
            )?;
            ensure!(
                millis <= 60_000,
                "FLOWER_PERSIST_INTERVAL_MS must be at most 60000: unwritten states stay in memory"
            );
            limits.persist_interval = Duration::from_millis(millis);
        }
        if let Some(raw) = read("FLOWER_SNAPSHOT_AFTER_BYTES")? {
            limits.snapshot_after_bytes = raw
                .parse()
                .context("FLOWER_SNAPSHOT_AFTER_BYTES must be a nonnegative u64 byte count")?;
        }
        if let Some(raw) = read("FLOWER_SNAPSHOT_MAX_AGE_MS")? {
            limits.snapshot_max_age = Duration::from_millis(
                raw.parse()
                    .context("FLOWER_SNAPSHOT_MAX_AGE_MS must be nonnegative milliseconds")?,
            );
        }
        ensure!(
            Instant::now()
                .checked_add(limits.snapshot_max_age)
                .is_some(),
            "FLOWER_SNAPSHOT_MAX_AGE_MS exceeds this platform's deadline range"
        );
        if let Some(raw) = read("FLOWER_SNAPSHOT_DUTY_PERCENT")? {
            limits.snapshot_duty_percent = raw
                .parse()
                .context("FLOWER_SNAPSHOT_DUTY_PERCENT must be an integer in 1..=100")?;
        }
        ensure!(
            (1..=100).contains(&limits.snapshot_duty_percent),
            "FLOWER_SNAPSHOT_DUTY_PERCENT must be in 1..=100"
        );
        for (name, value) in [
            (
                "FLOWER_TRANSACTION_MAX_BYTES",
                &mut limits.transaction_max_bytes,
            ),
            ("FLOWER_RPC_MAX_BYTES", &mut limits.rpc_max_bytes),
            (
                "FLOWER_SNAPSHOT_CHUNK_BYTES",
                &mut limits.snapshot_chunk_bytes,
            ),
        ] {
            if let Some(raw) = read(name)? {
                *value = raw
                    .parse()
                    .with_context(|| format!("{name} must be a positive integer byte count"))?;
            }
            ensure!(
                *value > 0 && *value <= isize::MAX as usize,
                "{name} must fit a nonempty platform byte buffer"
            );
        }
        for (name, value, allow_zero) in [
            (
                "FLOWER_SNAPSHOT_AFTER_LOGS",
                &mut limits.snapshot_after_logs,
                false,
            ),
            (
                "FLOWER_SNAPSHOT_LAG_LOGS",
                &mut limits.snapshot_lag_logs,
                false,
            ),
            (
                "FLOWER_SNAPSHOT_KEEP_LOGS",
                &mut limits.snapshot_keep_logs,
                true,
            ),
            (
                "FLOWER_SNAPSHOT_PURGE_BATCH_LOGS",
                &mut limits.snapshot_purge_batch_logs,
                false,
            ),
            (
                "FLOWER_RAFT_PAYLOAD_ENTRIES",
                &mut limits.raft_payload_entries,
                false,
            ),
        ] {
            if let Some(raw) = read(name)? {
                *value = raw
                    .parse()
                    .with_context(|| format!("{name} must be an integer log count"))?;
            }
            ensure!(allow_zero || *value > 0, "{name} must be positive");
        }
        let headroom = append_envelope_bytes()?;
        ensure!(
            limits
                .transaction_max_bytes
                .checked_add(headroom)
                .is_some_and(|bytes| bytes <= limits.rpc_max_bytes),
            "FLOWER_RPC_MAX_BYTES must exceed FLOWER_TRANSACTION_MAX_BYTES by at least {headroom} bytes for the Raft envelope"
        );
        // A segment's bytes travel as they are, after a JSON header whose
        // size is checked dynamically before transmission.
        ensure!(
            limits
                .snapshot_chunk_bytes
                .checked_add(headroom)
                .is_some_and(|bytes| bytes <= limits.rpc_max_bytes),
            "FLOWER_RPC_MAX_BYTES must include FLOWER_SNAPSHOT_CHUNK_BYTES plus at least {headroom} metadata bytes"
        );
        ensure!(
            limits.snapshot_lag_logs > limits.snapshot_after_logs,
            "FLOWER_SNAPSHOT_LAG_LOGS must exceed FLOWER_SNAPSHOT_AFTER_LOGS so a new snapshot can close the lag"
        );
        ensure!(
            limits.raft_payload_entries <= limits.rpc_max_bytes as u64,
            "FLOWER_RAFT_PAYLOAD_ENTRIES cannot exceed FLOWER_RPC_MAX_BYTES (each encoded entry occupies bytes)"
        );
        Ok(limits)
    }

    /// Conservative admission from individual Commit sizes. With at least two
    /// commands, omitted patches/CAS fields exceed the compact wrapper overhead;
    /// a single commit is smaller still. Consensus checks the exact final bytes.
    pub fn commit_group_fits(&self, encoded_command_bytes: usize, count: usize) -> bool {
        count > 0
            && encoded_command_bytes
                .checked_add(count - 1)
                .and_then(|bytes| bytes.checked_add(b"{\"batch\":[]}".len()))
                .is_some_and(|bytes| bytes <= self.transaction_max_bytes)
    }

    pub(super) fn validate_log_index(&self, last_index: Option<u64>) -> Result<()> {
        let next_index = match last_index {
            Some(index) => index
                .checked_add(1)
                .context("persisted Raft log index exhausts u64")?,
            None => 0,
        };
        // OpenRaft 0.9.25 adds these distances to log positions in
        // SnapshotPolicy::should_snapshot, LogHandler::calc_purge_upto, and
        // ProgressEntry::next_send. Reject settings that would already overflow
        // this node. This is state-dependent validation, after storage is read
        // but before Raft starts; it does not impose an arbitrary count ceiling.
        for (name, distance) in [
            ("FLOWER_SNAPSHOT_AFTER_LOGS", self.snapshot_after_logs),
            (
                "FLOWER_SNAPSHOT_PURGE_BATCH_LOGS",
                self.snapshot_purge_batch_logs,
            ),
            ("FLOWER_RAFT_PAYLOAD_ENTRIES", self.raft_payload_entries),
        ] {
            ensure!(
                next_index.checked_add(distance).is_some(),
                "{name} overflows Raft's u64 log index at the persisted position {next_index}"
            );
        }
        Ok(())
    }
}

/// Exact worst-case fixed JSON overhead for carrying one application command.
/// Entry splitting can then always fit a validated transaction into one RPC.
fn append_envelope_bytes() -> Result<usize> {
    let id = LogId::new(CommittedLeaderId::new(u64::MAX, u64::MAX), u64::MAX);
    let command = RaftCommand::Batch {
        batch: super::CompactBatch {
            expected_revision: 0,
            puts: Default::default(),
            deletes: vec![],
            items: vec![],
        },
    };
    let command_bytes = serde_json::to_vec(&command)?.len();
    let request = openraft::raft::AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new(u64::MAX, u64::MAX),
        prev_log_id: Some(id),
        entries: vec![Entry {
            log_id: id,
            payload: EntryPayload::Normal(command),
        }],
        leader_commit: Some(id),
    };
    Ok(serde_json::to_vec(&request)?.len() - command_bytes)
}

#[cfg(test)]
mod tests;
