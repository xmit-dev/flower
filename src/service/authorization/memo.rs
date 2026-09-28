//! Reused authorization decisions. A hook that reports what it read of the
//! call's arguments (the manifest's `authorize.result` is `"decision"`)
//! decided for every call with the same credentials, method, partition and
//! delegation whose arguments agree on the top-level fields it read: none, or
//! a few (`session`, say). Such a call would read the same values, so take
//! the same path to the same decision. It holds while nothing else it read
//! changes (its certificate) and its time has not run out (its validity),
//! exactly as a watch keeps an access decision between revisions, so the hook
//! runs about once per credential, method and fields instead of per request.
use super::{Access, App, Snapshot, Validity};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// What a decision is for: a digest, so entries hold neither credentials nor
/// arbitrarily long keys.
pub(super) type Key = [u8; 32];

/// The call without its arguments: credentials, method, partition, delegation.
pub(super) fn key(input: &Value, partition: Option<&str>, delegation: &Value) -> Key {
    #[derive(Serialize)]
    struct For<'a> {
        credentials: &'a Value,
        method: &'a Value,
        partition: Option<&'a str>,
        delegation: &'a Value,
    }
    let mut digest = Sha256::new();
    serde_json::to_writer(
        DigestWriter(&mut digest),
        &For {
            credentials: input.get("credentials").unwrap_or(&Value::Null),
            method: &input["name"],
            partition,
            delegation,
        },
    )
    .expect("authorization key is JSON");
    digest.finalize().into()
}

/// The call, with the values of the top-level argument `fields` a decision
/// read (absent ones marked so): the base key itself when it read none, and
/// nothing when arguments that are not an object meet fields.
fn projected(base: &Key, args: Option<&Value>, fields: &[String]) -> Option<Key> {
    if fields.is_empty() {
        return Some(*base);
    }
    let object = args?.as_object()?;
    let mut digest = Sha256::new();
    digest.update(b"fields\0");
    digest.update(base);
    for field in fields {
        let written = match object.get(field) {
            Some(value) => serde_json::to_writer(DigestWriter(&mut digest), &(field, value)),
            None => serde_json::to_writer(DigestWriter(&mut digest), &[field]),
        };
        written.expect("projected arguments are JSON");
    }
    Some(digest.finalize().into())
}

struct DigestWriter<'a>(&'a mut Sha256);

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(in crate::service) struct Memo {
    entries: Mutex<Entries>,
    max_bytes: usize,
    reused: AtomicU64,
    evaluated: AtomicU64,
    stored: AtomicU64,
}

#[derive(Default)]
struct Entries {
    values: HashMap<Key, Entry>,
    // Insertion order for eviction; a replaced entry leaves a stale item
    // behind, recognized by its sequence number.
    order: VecDeque<(Key, u64)>,
    bytes: usize,
    sequence: u64,
    // The fields the latest reusable decision for each call read.
    shapes: HashMap<Key, Arc<[String]>>,
}

struct Entry {
    revision: u64,
    sequence: u64,
    access: Access,
    bytes: usize,
}

impl Default for Memo {
    fn default() -> Self {
        let settings = super::super::tuning::settings().expect("validated authorization budget");
        Self::new(settings.authorization_cache_bytes)
    }
}

impl Memo {
    pub(super) fn new(max_bytes: usize) -> Self {
        Self {
            entries: Mutex::default(),
            max_bytes,
            reused: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            stored: AtomicU64::new(0),
        }
    }

    /// The decision for the call `base` with `args`, when it still holds on `state`.
    pub(super) fn get(
        &self,
        base: &Key,
        args: Option<&Value>,
        app: &App,
        state: &Snapshot,
    ) -> Option<Access> {
        if self.max_bytes == 0 {
            return None;
        }
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let fields = entries.shapes.get(base).cloned();
        let key = projected(base, args, fields.as_deref().unwrap_or_default());
        let access = key
            .and_then(|key| entries.values.get(&key))
            .map(|entry| entry.access.clone());
        drop(entries);
        // Certificate checks happen outside the lock.
        let access = access.filter(|access| access.holds(app, state))?;
        self.reused.fetch_add(1, Ordering::Relaxed);
        Some(access)
    }

    /// Count an evaluation of the hook, reusable or not.
    pub(super) fn evaluated(&self) {
        self.evaluated.fetch_add(1, Ordering::Relaxed);
    }

    /// Keep a decision that read only `fields` of the arguments (sorted,
    /// distinct; none at all when empty), when it can be rechecked: every
    /// read tracked and no clock polled.
    pub(super) fn insert(
        &self,
        base: Key,
        args: Option<&Value>,
        fields: Vec<String>,
        revision: u64,
        access: &Access,
    ) {
        let Some(certificate) = &access.observed else {
            return;
        };
        if self.max_bytes == 0 || access.validity == Validity::Polled {
            return;
        }
        let Some(key) = projected(&base, args, &fields) else {
            return;
        };
        let size = serde_json::to_vec(&access.principal)
            .map_or(usize::MAX, |encoded| encoded.len())
            .saturating_add(certificate.allocation_cost())
            .saturating_add(2 * size_of::<Key>() + 256);
        if size > self.max_bytes {
            return;
        }
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Shapes are small; forgetting them all only costs one evaluation each.
        if entries.shapes.len() >= 4096.max(2 * entries.values.len()) {
            entries.shapes.clear();
        }
        if entries
            .shapes
            .get(&base)
            .is_none_or(|known| **known != *fields)
        {
            entries.shapes.insert(base, fields.into());
        }
        // A decision made on an older snapshot never replaces a newer one.
        if entries
            .values
            .get(&key)
            .is_some_and(|entry| entry.revision > revision)
        {
            return;
        }
        if let Some(previous) = entries.values.remove(&key) {
            entries.bytes -= previous.bytes;
        }
        while entries.bytes.saturating_add(size) > self.max_bytes {
            let Some((oldest, sequence)) = entries.order.pop_front() else {
                break;
            };
            if entries
                .values
                .get(&oldest)
                .is_some_and(|entry| entry.sequence == sequence)
            {
                let removed = entries.values.remove(&oldest).expect("evicted entry");
                entries.bytes -= removed.bytes;
            }
        }
        entries.sequence += 1;
        let sequence = entries.sequence;
        entries.bytes += size;
        entries.values.insert(
            key,
            Entry {
                revision,
                sequence,
                access: access.clone(),
                bytes: size,
            },
        );
        entries.order.push_back((key, sequence));
        // Replacements leave stale order items; drop them before they outgrow
        // the entries they once named.
        if entries.order.len() > 2 * entries.values.len() + 64 {
            let Entries { values, order, .. } = &mut *entries;
            order.retain(|(key, sequence)| {
                values
                    .get(key)
                    .is_some_and(|entry| entry.sequence == *sequence)
            });
        }
        self.stored.fetch_add(1, Ordering::Relaxed);
    }

    pub(in crate::service) fn metrics(&self) -> Value {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        json!({
            "entries": entries.values.len(),
            "retainedBytes": entries.bytes,
            "limitBytes": self.max_bytes,
            "reused": self.reused.load(Ordering::Relaxed),
            "evaluated": self.evaluated.load(Ordering::Relaxed),
            "stored": self.stored.load(Ordering::Relaxed),
        })
    }
}
