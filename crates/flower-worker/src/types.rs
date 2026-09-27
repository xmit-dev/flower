//! Claims, leases and events: the twins of `Claim`, `ExternalWork`, `QueueWorkerEvent` and
//! `ReconcileEvent`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A leased job, as a queue's claim returns it (`temporal.ts` `Claim`). Fields are declared in
/// Flower's canonical (sorted) order, the order a claim arrives in and a TS worker keeps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim<P = Value> {
    /// The attempt this lease is, from 1.
    pub attempt: u64,
    /// When the lease ends, in server epoch milliseconds.
    pub expires_at: i64,
    /// The logical database and incarnation, when retry retention is initialized.
    #[serde(default, skip_serializing_if = "is_absent")]
    pub history: Option<Value>,
    pub id: String,
    pub owner: String,
    pub payload: P,
    #[serde(default)]
    pub scope: String,
    /// The fencing token.
    pub token: u64,
}

fn is_absent(value: &Option<Value>) -> bool {
    matches!(value, None | Some(Value::Null))
}

impl<P> Claim<P> {
    /// The same claim with another payload.
    pub fn map<Q>(self, payload: impl FnOnce(P) -> Q) -> Claim<Q> {
        Claim {
            attempt: self.attempt,
            expires_at: self.expires_at,
            history: self.history,
            id: self.id,
            owner: self.owner,
            payload: payload(self.payload),
            scope: self.scope,
            token: self.token,
        }
    }
}

/// `{ id, owner, token, history? }`: what renew, complete, fail and release identify a lease by.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LeaseIdentity {
    pub id: String,
    pub owner: String,
    pub token: u64,
    #[serde(skip_serializing_if = "is_absent")]
    pub history: Option<Value>,
}

impl<P> From<&Claim<P>> for LeaseIdentity {
    fn from(claim: &Claim<P>) -> Self {
        LeaseIdentity {
            id: claim.id.clone(),
            owner: claim.owner.clone(),
            token: claim.token,
            history: claim.history.clone().filter(|history| !history.is_null()),
        }
    }
}

/// What `runQueueWorker` reports (`QueueWorkerEvent`). Serialized like the TS object:
/// `{"type":"failed","id":…,"error":…}`.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum QueueWorkerEvent {
    Claimed { job: Claim<Value> },
    Completed { id: String },
    Failed { id: String, error: String },
    Lost { id: String },
    Released { id: String },
    Unreported { id: String, error: String },
    Limit { limit: u64, reason: String },
    Waiting { error: String },
}

impl QueueWorkerEvent {
    /// The event's `type`.
    pub fn kind(&self) -> &'static str {
        match self {
            QueueWorkerEvent::Claimed { .. } => "claimed",
            QueueWorkerEvent::Completed { .. } => "completed",
            QueueWorkerEvent::Failed { .. } => "failed",
            QueueWorkerEvent::Lost { .. } => "lost",
            QueueWorkerEvent::Released { .. } => "released",
            QueueWorkerEvent::Unreported { .. } => "unreported",
            QueueWorkerEvent::Limit { .. } => "limit",
            QueueWorkerEvent::Waiting { .. } => "waiting",
        }
    }
}

/// One input to compute (`ExternalWork`): the arguments, the key naming their current input, and
/// the input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalWork<A = Value, I = Value> {
    pub args: A,
    pub key: String,
    pub input: I,
}

/// A leased key (`ExternalClaim`), fields in canonical order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalClaim<A = Value, I = Value> {
    pub args: A,
    pub attempt: u64,
    pub expires_at: i64,
    pub input: I,
    pub key: String,
    pub owner: String,
}

/// What `reconcile` reports (`ReconcileEvent`).
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ReconcileEvent {
    Claimed { key: String, attempt: u64 },
    Published { key: String, accepted: bool },
    Failed { key: String, error: String },
    Lost { key: String },
    Limit { limit: u64, reason: String },
    Waiting { error: String },
}

impl ReconcileEvent {
    /// The event's `type`.
    pub fn kind(&self) -> &'static str {
        match self {
            ReconcileEvent::Claimed { .. } => "claimed",
            ReconcileEvent::Published { .. } => "published",
            ReconcileEvent::Failed { .. } => "failed",
            ReconcileEvent::Lost { .. } => "lost",
            ReconcileEvent::Limit { .. } => "limit",
            ReconcileEvent::Waiting { .. } => "waiting",
        }
    }
}
