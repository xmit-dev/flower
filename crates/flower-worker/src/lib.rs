//! Flower queue workers and reconcilers: the Rust twin of `@flower-js/sdk/worker`
//! (`sdk/worker.ts`, `sdk/capacity.ts`).
//!
//! - [`run_queue_worker`] runs jobs from a `queue.http()` queue: adaptive concurrency, claimers,
//!   batches, lines (`wait`), chained claims (`chain`), lease renewal, throttling, idle jobs, and a
//!   drain with release or abandon when stopping.
//! - [`reconcile`] keeps `external()` values current: one key, a pool, or a leased pool.
//! - [`Limiter`] and [`process_health`] size pools.
//!
//! The runtime talks to Flower through [`QueueClient`]; `flower_client::FlowerClient` implements it
//! with the `flower-client` feature, and the `testing` feature provides an in-memory Flower.

mod capacity;
mod client;
mod clock;
mod json;
mod queue;
mod reconcile;
mod stop;
mod types;

#[cfg(feature = "testing")]
pub mod testing;

use std::error::Error as StdError;
use std::fmt;

pub use capacity::{
    Adaptive, Concurrency, Health, HealthLimits, LimitChange, Limiter, Load, ProcessHealth, default_process_health,
    idle_health, process_health,
};
pub use client::{ClientError, Predicate, QueueClient, RetryPolicy, backoff, default_backoff, non_empty_array, truthy};
pub use clock::{Clock, system_now_ms};
pub use json::to_js_raw;
pub use queue::{EventHandler, QueueWorkerOptions, run_queue_worker};
pub use reconcile::{ReconcileOptions, reconcile};
pub use stop::{JobControl, JobStop, WorkError};
pub use types::{Claim, ExternalClaim, ExternalWork, LeaseIdentity, QueueWorkerEvent, ReconcileEvent};

/// Why [`run_queue_worker`] or [`reconcile`] stopped with an error.
#[derive(Debug)]
pub enum WorkerError<E> {
    /// The options were invalid (TS: the `TypeError` thrown before starting), or reconcile computed
    /// a value Flower cannot hold.
    Invalid(String),
    /// A client call failed permanently: a claim, the readiness watch, or a publication.
    Client(E),
    /// Flower answered something the worker cannot read.
    Protocol(String),
}

impl<E> WorkerError<E> {
    /// The client error, when there is one.
    pub fn client(&self) -> Option<&E> {
        match self {
            WorkerError::Client(error) => Some(error),
            _ => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for WorkerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerError::Invalid(message) | WorkerError::Protocol(message) => f.write_str(message),
            WorkerError::Client(error) => error.fmt(f),
        }
    }
}

impl<E: StdError + 'static> StdError for WorkerError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            WorkerError::Client(error) => Some(error),
            _ => None,
        }
    }
}

/// `message(error)` for an error a job or computation returned: its display text.
pub(crate) fn describe_error(error: &(dyn StdError + 'static)) -> String {
    error.to_string()
}

/// `crypto.randomUUID()`: a random version 4 UUID.
pub(crate) fn new_uuid() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        bytes = fastrand::u128(..).to_le_bytes();
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

/// The message of a panic, as a thrown error's.
pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "The job panicked".to_owned()
    }
}
