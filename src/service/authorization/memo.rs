//! Reused authorization decisions. A hook that reports what it read of the
//! call's arguments (the manifest's `authorize.result` is `"decision"`)
//! decided for every call with the same credentials, method, partition and
//! delegation whose arguments agree on the top-level fields it read: none, or
//! a few (`session`, say). Such a call would read the same values, so take
//! the same path to the same decision. A decision that read more of them (all
//! of it, or arguments that are not an object, such as none at all), and one
//! from a hook that reports nothing, holds for calls whose whole input is the
//! same, as a query's result does for the same arguments. It holds while
//! nothing else it read changes (its certificate) and its time has not run
//! out (its validity), exactly as a watch keeps an access decision between
//! revisions, so the hook runs about once per credential, method and fields
//! (or arguments) instead of per request.
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

/// What a decision read of the call's arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Read {
    /// These top-level fields (sorted, distinct), present or absent; none at
    /// all when empty.
    Fields(Vec<String>),
    /// Anything: it holds only for the same arguments.
    Whole,
}

/// Arguments whose JSON is longer are not worth a digest on every call: a
/// decision that read all of them is not kept, and the hook runs as before.
pub(super) const WHOLE_ARGS_MAX_BYTES: usize = 16 * 1024;

/// The call with its whole arguments as the hook receives them: absent ones
/// are null, as for the hook, while `{}` is an object, which the SDK's hook
/// reads through a proxy. Nothing when they are too long to digest.
fn whole(base: &Key, args: Option<&Value>) -> Option<Key> {
    let mut digest = Sha256::new();
    digest.update(b"whole\0");
    digest.update(base);
    let mut writer = Bounded {
        digest: &mut digest,
        left: WHOLE_ARGS_MAX_BYTES,
    };
    serde_json::to_writer(&mut writer, args.unwrap_or(&Value::Null)).ok()?;
    Some(digest.finalize().into())
}

/// Digests at most `left` bytes, then fails the serialization.
struct Bounded<'a> {
    digest: &'a mut Sha256,
    left: usize,
}

impl std::io::Write for Bounded<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.left = self
            .left
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("arguments too long to digest"))?;
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
    // What the reusable decisions for each call read.
    shapes: HashMap<Key, Shapes>,
}

/// The fields the latest decision for a call read that named them, and
/// whether one read its whole arguments: a method whose calls sometimes
/// carry an object and sometimes none keeps both.
#[derive(Clone, Default)]
struct Shapes {
    fields: Option<Arc<[String]>>,
    whole: bool,
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
        let shapes = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .shapes
            .get(base)
            .cloned()
            .unwrap_or_default();
        // Digests happen outside the lock; a decision that read nothing is
        // kept under the base key itself.
        let keys = [
            projected(base, args, shapes.fields.as_deref().unwrap_or_default()),
            shapes.whole.then(|| whole(base, args)).flatten(),
        ];
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let found = keys.map(|key| {
            key.and_then(|key| entries.values.get(&key))
                .map(|entry| entry.access.clone())
        });
        drop(entries);
        // Certificate checks happen outside the lock.
        let access = found
            .into_iter()
            .flatten()
            .find(|access| access.holds(app, state))?;
        self.reused.fetch_add(1, Ordering::Relaxed);
        Some(access)
    }

    /// Count an evaluation of the hook, reusable or not.
    pub(super) fn evaluated(&self) {
        self.evaluated.fetch_add(1, Ordering::Relaxed);
    }

    /// Keep a decision that read `read` of the arguments, when it can be
    /// rechecked: every read tracked and no clock polled.
    pub(super) fn insert(
        &self,
        base: Key,
        args: Option<&Value>,
        read: Read,
        revision: u64,
        access: &Access,
    ) {
        let Some(certificate) = &access.observed else {
            return;
        };
        if self.max_bytes == 0 || access.validity == Validity::Polled {
            return;
        }
        let key = match &read {
            Read::Fields(fields) => projected(&base, args, fields),
            Read::Whole => whole(&base, args),
        };
        let Some(key) = key else {
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
        let shapes = entries.shapes.entry(base).or_default();
        match read {
            Read::Fields(fields) => {
                if shapes.fields.as_deref() != Some(&fields[..]) {
                    shapes.fields = Some(fields.into());
                }
            }
            Read::Whole => shapes.whole = true,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn access() -> Access {
        Access {
            principal: json!({"subject":"alice"}),
            validity: Validity::Stable,
            observed: Some(Arc::default()),
        }
    }

    #[test]
    fn decisions_on_whole_arguments_stay_within_the_byte_budget() {
        let memo = Memo::new(4096);
        let base = [7; 32];
        // Unique arguments (ids, search terms) each make an entry, which the
        // budget evicts oldest first, as it does decisions on fields.
        for n in 0..1000 {
            memo.insert(base, Some(&json!({"q":n})), Read::Whole, n, &access());
            let metrics = memo.metrics();
            assert!(metrics["retainedBytes"].as_u64().unwrap() <= 4096);
        }
        let metrics = memo.metrics();
        assert_eq!(metrics["stored"], 1000);
        let kept = metrics["entries"].as_u64().unwrap();
        assert!((1..20).contains(&kept), "{kept} entries");
        let entries = memo.entries.lock().unwrap();
        let has = |n: u64| {
            entries
                .values
                .contains_key(&whole(&base, Some(&json!({"q":n}))).unwrap())
        };
        assert!(has(999) && !has(0), "the newest stay, the oldest go");
        assert!(
            entries.order.len() <= 2 * entries.values.len() + 64,
            "eviction order stays bounded"
        );
    }

    #[test]
    fn arguments_too_long_to_digest_keep_no_decision() {
        let memo = Memo::new(1 << 20);
        let base = [9; 32];
        // The JSON string's quotes count too.
        let longest = json!("x".repeat(WHOLE_ARGS_MAX_BYTES - 2));
        let longer = json!("x".repeat(WHOLE_ARGS_MAX_BYTES - 1));
        assert!(whole(&base, Some(&longest)).is_some());
        assert!(whole(&base, Some(&longer)).is_none());
        assert_ne!(whole(&base, None), whole(&base, Some(&json!({}))));
        assert_eq!(whole(&base, None), whole(&base, Some(&Value::Null)));
        memo.insert(base, Some(&longer), Read::Whole, 1, &access());
        assert_eq!(memo.metrics()["entries"], 0);
        memo.insert(base, Some(&longest), Read::Whole, 1, &access());
        assert_eq!(memo.metrics()["entries"], 1);
    }
}
