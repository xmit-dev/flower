//! Replicated retry admission. An expired identity remains inadmissible even
//! after its receipt has been collected. Epochs are explicit logical windows;
//! local wall clocks never decide whether a replicated request may execute. A
//! rotation lets the leader advance them after its own monotonic time elapses.
//! A caller's own unscoped ID is admitted too: its receipt records the epoch
//! it ran in and is collected like a scoped one, after which the ID is new.

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{CommitResult, Consensus, RaftCommand, Receipt, Snapshot};

pub(crate) const KEY: &str = "$flower.retention";
pub(crate) const SESSION_PREFIX: &str = "$flower.session:";
pub const RESERVED_BYTES: &str = "transaction:reserved-receipt-bytes";
pub(crate) const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct State {
    pub database: String,
    pub incarnation: String,
    pub current_epoch: u64,
    pub min_epoch: u64,
    pub receipt_bytes: u64,
    pub receipt_count: u64,
    #[serde(default)]
    pub session_bytes: u64,
    #[serde(default)]
    pub session_count: u64,
    pub max_receipt_bytes: Option<u64>,
    pub gc_cursor: Option<String>,
    pub gc_complete: bool,
    #[serde(default)]
    pub gc_receipts_complete: bool,
    #[serde(default)]
    pub gc_session_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation: Option<Rotation>,
}

/// Advance the epoch after every `epoch_ms` of the leader's monotonic time,
/// keeping requests of the last `keep_epochs` epochs, the current one
/// included, admissible. A request minted or run at an epoch's end is
/// retryable for at least `(keep_epochs - 1) * epoch_ms`; a leader change
/// only delays.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Rotation {
    pub epoch_ms: u64,
    pub keep_epochs: u64,
}

impl Rotation {
    /// The floor once `current` is the current epoch.
    pub fn min_epoch(&self, current: u64) -> u64 {
        current.saturating_sub(self.keep_epochs.saturating_sub(1))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Session {
    pub id: String,
    pub incarnation: String,
    pub owner: String,
    pub epoch: u64,
    pub acknowledged_through: u64,
    pub closed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub expected_revision: u64,
    pub action: Action,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Initialize {
        database: String,
        incarnation: String,
        max_receipt_bytes: Option<u64>,
    },
    Advance {
        incarnation: String,
        current_epoch: u64,
        min_epoch: u64,
    },
    Collect {
        incarnation: String,
        /// Maximum receipts inspected, including ones that remain retained.
        limit: usize,
    },
    SetBudget {
        incarnation: String,
        max_receipt_bytes: Option<u64>,
    },
    /// Rotate epochs automatically, or stop with `epoch_ms: null`.
    Rotate {
        incarnation: String,
        epoch_ms: Option<u64>,
        keep_epochs: u64,
    },
    OpenSession {
        incarnation: String,
        session: String,
        owner: String,
        epoch: u64,
    },
    Acknowledge {
        incarnation: String,
        session: String,
        owner: String,
        through: u64,
        limit: usize,
        /// Explicitly abandon missing/uncertain intents in this prefix. Never
        /// set automatically merely because a transport call failed.
        #[serde(default)]
        abandon: bool,
    },
    CloseSession {
        incarnation: String,
        session: String,
        owner: String,
        limit: usize,
    },
    Reincarnate {
        incarnation: String,
        new_incarnation: String,
        /// Operator evidence/attestation; this is not an automatic fencing
        /// authority and does not authorize unrelated cross-group restoration.
        fence_attestation: String,
    },
}

fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) struct Identity<'a> {
    pub database: &'a str,
    pub incarnation: &'a str,
    pub epoch: u64,
    pub session: Option<(&'a str, u64)>,
}

pub(crate) fn identity(id: &str) -> anyhow::Result<Identity<'_>> {
    let mut parts = id.split(':');
    let version = parts.next();
    ensure!(
        matches!(version, Some("f1" | "f2")),
        "REQUEST_ID_SCOPE_REQUIRED: initialize an immutable retry identity before submission"
    );
    let database = parts
        .next()
        .context("REQUEST_ID_INVALID: missing database")?;
    let incarnation = parts
        .next()
        .context("REQUEST_ID_INVALID: missing incarnation")?;
    let epoch = parts.next().context("REQUEST_ID_INVALID: missing epoch")?;
    let intent = parts
        .next()
        .context("REQUEST_ID_INVALID: missing intent or session")?;
    let session = if version == Some("f2") {
        let sequence = parts
            .next()
            .context("REQUEST_ID_INVALID: missing sequence")?;
        let parsed: u64 = sequence
            .parse()
            .context("REQUEST_ID_INVALID: invalid sequence")?;
        ensure!(
            lowercase_hex(intent, 32)
                && parsed > 0
                && parsed <= MAX_SAFE_INTEGER
                && sequence == parsed.to_string(),
            "REQUEST_ID_INVALID: session and sequence must be canonical"
        );
        Some((intent, parsed))
    } else {
        ensure!(
            lowercase_hex(intent, 64),
            "REQUEST_ID_INVALID: expected sha256 intent"
        );
        None
    };
    ensure!(
        lowercase_hex(database, 32) && lowercase_hex(incarnation, 32) && parts.next().is_none(),
        "REQUEST_ID_INVALID: expected f1:database:incarnation:epoch:sha256-intent"
    );
    let number: u64 = epoch.parse().context("REQUEST_ID_INVALID: invalid epoch")?;
    ensure!(
        number <= MAX_SAFE_INTEGER && epoch == number.to_string(),
        "REQUEST_ID_INVALID: epoch must be a canonical safe unsigned integer"
    );
    Ok(Identity {
        database,
        incarnation,
        epoch: number,
        session,
    })
}

impl State {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            lowercase_hex(&self.database, 32) && lowercase_hex(&self.incarnation, 32),
            "invalid retention history identity"
        );
        ensure!(
            self.min_epoch <= self.current_epoch && self.current_epoch <= MAX_SAFE_INTEGER,
            "invalid retention epoch interval"
        );
        Ok(())
    }

    pub fn validate_request(&self, id: &str) -> anyhow::Result<()> {
        self.validate()?;
        if !scoped(id) {
            return Ok(());
        }
        let identity = identity(id)?;
        ensure!(
            identity.database == self.database,
            "REQUEST_DATABASE_MISMATCH: request belongs to a different logical database"
        );
        ensure!(
            identity.incarnation == self.incarnation,
            "HISTORY_MISMATCH: request belongs to a different history; its outcome may be unknown"
        );
        ensure!(
            identity.epoch >= self.min_epoch,
            "RETRY_WINDOW_EXPIRED: the original outcome is no longer available; this does not prove failure"
        );
        ensure!(
            identity.epoch <= self.current_epoch,
            "RETRY_EPOCH_NOT_ADMITTED: request epoch has not been admitted"
        );
        Ok(())
    }

    pub(crate) fn charge(&mut self, bytes: u64, count: u64, reserved: u64) -> anyhow::Result<()> {
        let next = self
            .receipt_bytes
            .checked_add(bytes)
            .context("RECEIPT_BUDGET_EXCEEDED: receipt byte counter exhausted")?;
        self.capacity(next, reserved)?;
        self.receipt_count = self
            .receipt_count
            .checked_add(count)
            .context("receipt count exhausted")?;
        self.receipt_bytes = next;
        Ok(())
    }

    pub(crate) fn capacity(&self, receipt_bytes: u64, reserved: u64) -> anyhow::Result<()> {
        let promised = receipt_bytes
            .checked_add(self.session_bytes)
            .context("RECEIPT_BUDGET_EXCEEDED: session metadata counter exhausted")?
            .checked_add(reserved)
            .context("RECEIPT_BUDGET_EXCEEDED: receipt reservation counter exhausted")?;
        ensure!(
            self.max_receipt_bytes
                .is_none_or(|budget| promised <= budget),
            "RECEIPT_BUDGET_EXCEEDED: promised retry results exhaust the configured logical byte budget"
        );
        Ok(())
    }

    pub(crate) fn collectable(&self, id: &str, receipt: &Receipt) -> bool {
        // Receipts from before initialization count as its first epoch.
        if !scoped(id) {
            return receipt.epoch.unwrap_or(0) < self.min_epoch;
        }
        identity(id).is_ok_and(|id| {
            id.database == self.database
                && (id.incarnation != self.incarnation || id.epoch < self.min_epoch)
        })
    }

    pub(crate) fn restart_collection(&mut self) {
        self.gc_cursor = None;
        self.gc_session_cursor = None;
        self.gc_complete = false;
        self.gc_receipts_complete = false;
    }
}

/// Whether an ID carries its own retry identity, rather than being a caller's key.
pub(crate) fn scoped(id: &str) -> bool {
    id.starts_with("f1:") || id.starts_with("f2:")
}

/// The epoch a new receipt for `id` records: a caller's own ID keeps its
/// receipt from the current epoch, and a scoped one names its epoch itself.
pub(crate) fn receipt_epoch(state: &State, id: &str) -> Option<u64> {
    (!scoped(id)).then_some(state.current_epoch)
}

pub(crate) fn protected_key(key: &str) -> bool {
    key == KEY
        || key == "$flower.history"
        || key.starts_with("$flower.history:")
        || key.starts_with(SESSION_PREFIX)
}
pub(crate) fn session_key(id: &str) -> String {
    format!("{SESSION_PREFIX}{id}")
}
pub(crate) fn decode_session(value: &serde_json::Value) -> anyhow::Result<Session> {
    let session: Session = Session::deserialize(value).context("invalid retry session metadata")?;
    ensure!(
        lowercase_hex(&session.id, 32)
            && lowercase_hex(&session.incarnation, 32)
            && lowercase_hex(&session.owner, 64)
            && session.epoch <= MAX_SAFE_INTEGER
            && session.acknowledged_through <= MAX_SAFE_INTEGER,
        "invalid retry session metadata"
    );
    Ok(session)
}

pub fn session(snapshot: &Snapshot, id: &str) -> anyhow::Result<Option<Session>> {
    snapshot
        .data
        .get(&session_key(id))
        .map(decode_session)
        .transpose()
}

pub(crate) fn validate_with<'a>(
    state: Option<&State>,
    id: &str,
    get: impl Fn(&str) -> Option<&'a serde_json::Value>,
) -> anyhow::Result<()> {
    let Some(state) = state else {
        ensure!(
            !scoped(id),
            "RETENTION_NOT_INITIALIZED: logical database has no retry history identity"
        );
        return Ok(());
    };
    state.validate_request(id)?;
    if !scoped(id) {
        return Ok(());
    }
    let identity = identity(id)?;
    if let Some((id, sequence)) = identity.session {
        let session = get(&session_key(id))
            .map(decode_session)
            .transpose()?
            .context(
                "RETRY_SESSION_UNKNOWN: session does not exist and cannot be reopened by a request",
            )?;
        ensure!(
            session.incarnation == state.incarnation
                && session.epoch == identity.epoch
                && session.id == id,
            "RETRY_SESSION_MISMATCH: request does not match its session"
        );
        ensure!(
            !session.closed,
            "RETRY_SESSION_CLOSED: this session is terminal"
        );
        ensure!(
            sequence > session.acknowledged_through,
            "ALREADY_ACKNOWLEDGED: this intent has been consumed or permanently abandoned"
        );
    }
    Ok(())
}

pub fn status(snapshot: &Snapshot) -> anyhow::Result<Option<State>> {
    decode(snapshot.data.get(KEY))
}

pub(crate) fn decode(value: Option<&serde_json::Value>) -> anyhow::Result<Option<State>> {
    let Some(value) = value else { return Ok(None) };
    let state: State = State::deserialize(value).context("invalid retention metadata")?;
    state.validate()?;
    Ok(Some(state))
}

pub fn validate_request(snapshot: &Snapshot, id: &str) -> anyhow::Result<()> {
    validate_with(status(snapshot)?.as_ref(), id, |key| snapshot.data.get(key))
}

/// Transactions and staged deployments keep records under their request ID
/// past its receipt, until retiring the ID fences them. A caller's own ID is
/// never retired, so with retention these need scoped IDs.
pub fn require_scoped(snapshot: &Snapshot, id: &str) -> anyhow::Result<()> {
    ensure!(
        scoped(id) || status(snapshot)?.is_none(),
        "REQUEST_ID_SCOPE_REQUIRED: transactions and staged deployments need a scoped request ID once retention is initialized"
    );
    Ok(())
}

/// The owner must come from runtime-authenticated identity, never caller JSON.
pub fn validate_request_owner(snapshot: &Snapshot, id: &str, owner: &str) -> anyhow::Result<()> {
    validate_request_owner_with(snapshot, id, || owner)
}

/// Derive the authenticated owner only for session identities. Legacy and
/// epoch identities still pass the same admission checks without hashing a
/// principal that their protocol does not use.
pub(crate) fn validate_request_owner_with<S: AsRef<str>>(
    snapshot: &Snapshot,
    id: &str,
    owner: impl FnOnce() -> S,
) -> anyhow::Result<()> {
    if id.starts_with("f2:") {
        let (id, _) = identity(id)?.session.expect("parsed session identity");
        let owner = owner();
        ensure!(
            session(snapshot, id)?.is_some_and(|session| session.owner == owner.as_ref()),
            "RETRY_SESSION_FORBIDDEN: session belongs to another authenticated principal"
        );
    }
    validate_request(snapshot, id)
}

pub fn owner_for(subject: &str) -> String {
    digest(subject.as_bytes())
}

pub fn session_request_id(
    state: &State,
    session: &Session,
    sequence: u64,
) -> anyhow::Result<String> {
    let id = format!(
        "f2:{}:{}:{}:{}:{sequence}",
        state.database, session.incarnation, session.epoch, session.id
    );
    identity(&id)?;
    Ok(id)
}

pub fn open_session(snapshot: &Snapshot, owner: &str) -> anyhow::Result<Command> {
    let state =
        status(snapshot)?.context("RETENTION_NOT_INITIALIZED: initialize retry admission first")?;
    Ok(Command {
        expected_revision: snapshot.revision,
        action: Action::OpenSession {
            incarnation: state.incarnation,
            session: random_identity()?,
            owner: owner.into(),
            epoch: state.current_epoch,
        },
    })
}

pub fn reincarnate(snapshot: &Snapshot, fence_attestation: &str) -> anyhow::Result<Command> {
    let state =
        status(snapshot)?.context("RETENTION_NOT_INITIALIZED: initialize retry admission first")?;
    Ok(Command {
        expected_revision: snapshot.revision,
        action: Action::Reincarnate {
            incarnation: state.incarnation,
            new_incarnation: random_identity()?,
            fence_attestation: fence_attestation.into(),
        },
    })
}

fn random_identity() -> anyhow::Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("generate retry identity: {error}"))?;
    Ok(hex(&bytes))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 15) as usize] as char);
    }
    encoded
}
fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub(crate) fn reserved_bytes(value: Option<&serde_json::Value>) -> anyhow::Result<u64> {
    value.map_or(Ok(0), |value| {
        value
            .as_u64()
            .context("invalid transaction receipt reservation")
    })
}

/// Before the irrevocable distributed commit decision, reserve capacity in the
/// same replicated state. A final result may consume its own reservation only
/// when the final command atomically releases that reservation.
pub fn validate_capacity_for(
    snapshot: &Snapshot,
    id: &str,
    fingerprint: &str,
    result: &serde_json::Value,
    credit_bytes: u64,
) -> anyhow::Result<()> {
    let state = status(snapshot)?;
    validate_with(state.as_ref(), id, |key| snapshot.data.get(key))?;
    let Some(mut state) = state else {
        return Ok(());
    };
    if let Some(receipt) = snapshot.requests.get(id) {
        ensure!(
            receipt.fingerprint == fingerprint,
            "REQUEST_ID_REUSED: request identity was already used for different content"
        );
        return Ok(());
    }
    let reserved = reserved_bytes(snapshot.data.get(RESERVED_BYTES))?
        .checked_sub(credit_bytes)
        .context("receipt credit exceeds the durable reservation")?;
    let receipt = BorrowedReceipt {
        fingerprint,
        revision: snapshot
            .revision
            .checked_add(1)
            .context("revision exhausted")?,
        result,
        epoch: receipt_epoch(&state, id),
    };
    state.charge(receipt.encoded_bytes(id)?, 1, reserved)
}

/// Plan receipt accounting against a staged command's post-patch record view.
/// All fallible work completes before the writer changes its owned snapshot;
/// the replicated state machine independently repeats admission and accounting.
pub(crate) fn plan_receipt_accounting<'a>(
    id: &str,
    receipt: &mut Receipt,
    receipts: &super::Receipts,
    get: impl Fn(&str) -> Option<&'a serde_json::Value>,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(mut state) = decode(get(KEY))? else {
        return Ok(None);
    };
    ensure!(
        !receipts.contains_key(id),
        "cannot account an existing receipt twice"
    );
    validate_with(Some(&state), id, &get)?;
    receipt.epoch = receipt_epoch(&state, id);
    state.charge(
        receipt_bytes(id, receipt)?,
        1,
        reserved_bytes(get(RESERVED_BYTES))?,
    )?;
    Ok(Some(serde_json::to_value(state)?))
}

/// Call once per business intent, and persist the returned identity before
/// sending it. Re-scoping an uncertain intent after retirement is unsafe.
pub fn scope_request_id(state: &State, intent: &str) -> String {
    let digest = digest(intent.as_bytes());
    format!(
        "f1:{}:{}:{}:{digest}",
        state.database, state.incarnation, state.current_epoch
    )
}

/// Generate identities on the proposing node, never during deterministic apply.
pub fn initialize(
    expected_revision: u64,
    max_receipt_bytes: Option<u64>,
) -> anyhow::Result<Command> {
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow::anyhow!("generate retention identities: {error}"))?;
    Ok(Command {
        expected_revision,
        action: Action::Initialize {
            database: hex(&random[..16]),
            incarnation: hex(&random[16..]),
            max_receipt_bytes,
        },
    })
}

/// Logical receipt payload bytes, excluding allocator/redb/index overhead.
pub(crate) fn receipt_bytes(id: &str, receipt: &Receipt) -> anyhow::Result<u64> {
    BorrowedReceipt {
        fingerprint: &receipt.fingerprint,
        revision: receipt.revision,
        result: &receipt.result,
        epoch: receipt.epoch,
    }
    .encoded_bytes(id)
}

// Match Receipt's field names and declaration order exactly. Capacity checks
// borrow an evaluation result instead of cloning its JSON tree just to size it.
#[derive(Serialize)]
struct BorrowedReceipt<'a> {
    fingerprint: &'a str,
    revision: u64,
    result: &'a serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch: Option<u64>,
}

impl BorrowedReceipt<'_> {
    fn encoded_bytes(&self, id: &str) -> anyhow::Result<u64> {
        let bytes = id
            .len()
            .checked_add(super::encoded_json_len(self)?)
            .context("receipt size exhausted")?;
        u64::try_from(bytes).context("receipt size exceeds counter")
    }
}

pub(crate) fn validate_snapshot(snapshot: &Snapshot) -> anyhow::Result<()> {
    if let Some(state) = status(snapshot)? {
        let bytes = snapshot
            .requests
            .iter()
            .try_fold(0_u64, |total, (id, receipt)| {
                total
                    .checked_add(receipt_bytes(id, receipt)?)
                    .context("receipt size exhausted")
            })?;
        ensure!(
            state.receipt_bytes == bytes && state.receipt_count == snapshot.requests.len() as u64,
            "retention receipt accounting does not match snapshot"
        );
        let mut count = 0_u64;
        let mut bytes = 0_u64;
        for (key, value) in snapshot.data.range::<_>((
            std::ops::Bound::Included(SESSION_PREFIX),
            std::ops::Bound::Excluded("$flower.session;"),
        )) {
            let session = decode_session(value)?;
            ensure!(
                key == &session_key(&session.id),
                "session metadata key mismatch"
            );
            ensure!(
                session.incarnation != state.incarnation || session.epoch <= state.current_epoch,
                "session epoch has never been admitted"
            );
            count = count.checked_add(1).context("session counter exhausted")?;
            bytes = bytes
                .checked_add(session_bytes(&session)?)
                .context("session byte counter exhausted")?;
        }
        ensure!(
            state.session_bytes == bytes && state.session_count == count,
            "retention session accounting does not match snapshot"
        );
    } else {
        ensure!(
            snapshot
                .data
                .range::<_>((
                    std::ops::Bound::Included(SESSION_PREFIX),
                    std::ops::Bound::Excluded("$flower.session;")
                ))
                .next()
                .is_none(),
            "session metadata has no retention history"
        );
    }
    Ok(())
}

pub(crate) fn session_bytes(session: &Session) -> anyhow::Result<u64> {
    let bytes = session_key(&session.id)
        .len()
        .checked_add(super::encoded_json_len(session)?)
        .context("session size exhausted")?;
    u64::try_from(bytes).context("session size exceeds counter")
}

impl Consensus {
    pub async fn control_retention(&self, command: Command) -> anyhow::Result<CommitResult> {
        self.commit_command(RaftCommand::Retention { retention: command })
            .await
    }
}

#[cfg(test)]
mod tests;
