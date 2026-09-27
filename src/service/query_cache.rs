//! Memoize deterministic queries across revisions using observed dependencies.
//! Every lookup still follows the query's consistency policy and HTTP registry
//! resolution. Independent queries and committed snapshots execute in parallel.

use crate::{consensus::Records, evaluator::DependencyCertificate};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Mutex as AsyncMutex;

/// Match the canonical object-key order without cloning caller arguments or
/// principals into temporary JSON trees.
pub(super) fn key(name: &str, args: &Value, principal: &Value) -> String {
    #[derive(serde::Serialize)]
    struct Invocation<'a> {
        args: &'a Value,
        name: &'a str,
    }
    #[derive(serde::Serialize)]
    struct Key<'a> {
        invocation: Invocation<'a>,
        principal: &'a Value,
    }
    serde_json::to_string(&Key {
        invocation: Invocation { args, name },
        principal,
    })
    .expect("query cache key is JSON")
}

pub(super) struct QueryCache {
    entries: Mutex<Entries>,
    flights: Mutex<Flights>,
    max_bytes: usize,
    max_flight_bytes: usize,
}
impl Default for QueryCache {
    fn default() -> Self {
        let settings = super::tuning::settings().expect("validated query cache budgets");
        Self::new(settings.query_cache_bytes, settings.query_flight_bytes)
    }
}

#[derive(Default)]
struct Flights {
    pending: BTreeMap<(u64, String), Weak<AsyncMutex<bool>>>,
    bytes: usize,
}

#[derive(Default)]
struct Entries {
    values: BTreeMap<String, Entry>,
    order: VecDeque<String>,
    bytes: usize,
}

struct Entry {
    revision: u64,
    value: Arc<str>,
    certificate: Arc<DependencyCertificate>,
    bytes: usize,
}

impl QueryCache {
    fn new(max_bytes: usize, max_flight_bytes: usize) -> Self {
        Self {
            entries: Mutex::default(),
            flights: Mutex::default(),
            max_bytes,
            max_flight_bytes,
        }
    }
    /// A weak, bounded registry only retains keys while evaluations are active.
    /// The boolean allows non-cacheable queries to release all queued callers
    /// after their first result rather than serializing clock-sensitive work.
    pub(super) fn flight(&self, revision: u64, key: &str) -> Option<Arc<AsyncMutex<bool>>> {
        let cost = key.len().saturating_add(256);
        if cost > self.max_flight_bytes {
            return None;
        }
        let mut flights = self
            .flights
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let key = (revision, key.to_owned());
        if let Some(active) = flights.pending.get(&key).and_then(Weak::upgrade) {
            return Some(active);
        }
        let mut removed = 0;
        flights.pending.retain(|(_, key), value| {
            if value.strong_count() == 0 {
                removed += key.len().saturating_add(256);
                false
            } else {
                true
            }
        });
        flights.bytes -= removed;
        if flights.bytes.saturating_add(cost) > self.max_flight_bytes {
            return None;
        }
        let active = Arc::new(AsyncMutex::new(true));
        flights.bytes += cost;
        flights.pending.insert(key, Arc::downgrade(&active));
        Some(active)
    }

    #[cfg(test)]
    pub(super) fn get(&self, _revision: u64, key: &str, snapshot: &Records) -> Option<Value> {
        self.get_certified(key, snapshot).map(|(value, _)| value)
    }

    /// A reused value, with the certificate that still holds for it.
    pub(super) fn get_certified(
        &self,
        key: &str,
        snapshot: &Records,
    ) -> Option<(Value, Arc<DependencyCertificate>)> {
        let (encoded, certificate) = self.get_valid(key, snapshot)?;
        Some((serde_json::from_str(&encoded).ok()?, certificate))
    }

    /// HTTP callers can reuse validated JSON without decoding it only to encode
    /// it again. The caller must account for retention after cache eviction.
    pub(super) fn get_encoded(&self, key: &str, snapshot: &Records) -> Option<Arc<str>> {
        self.get_valid(key, snapshot).map(|(encoded, _)| encoded)
    }

    fn get_valid(
        &self,
        key: &str,
        snapshot: &Records,
    ) -> Option<(Arc<str>, Arc<DependencyCertificate>)> {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = entries
            .values
            .get(key)
            .map(|entry| (entry.value.clone(), entry.certificate.clone()));
        drop(entries);
        // Dependency checks happen outside the global mutex.
        let (encoded, certificate) = entry?;
        certificate
            .valid(snapshot)
            .then_some((encoded, certificate))
    }

    pub(super) fn insert(
        &self,
        revision: u64,
        key: String,
        value: Value,
        certificate: Arc<DependencyCertificate>,
    ) {
        if self.max_bytes == 0 {
            return;
        }
        let Ok(encoded) = serde_json::to_string(&value) else {
            return;
        };
        let size = 2usize
            .saturating_mul(key.len())
            .saturating_add(encoded.len())
            .saturating_add(certificate.allocation_cost())
            .saturating_add(256);
        if size > self.max_bytes {
            return;
        }
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Delayed work must not evict a newer certificate for the same query.
        if entries
            .values
            .get(&key)
            .is_some_and(|entry| entry.revision >= revision)
        {
            return;
        }
        if let Some(previous) = entries.values.remove(&key) {
            entries.bytes -= previous.bytes;
            entries.order.retain(|existing| existing != &key);
        }
        while entries.bytes.saturating_add(size) > self.max_bytes {
            let Some(oldest) = entries.order.pop_front() else {
                break;
            };
            if let Some(removed) = entries.values.remove(&oldest) {
                entries.bytes -= removed.bytes;
            }
        }
        entries.bytes += size;
        entries.order.push_back(key.clone());
        entries.values.insert(
            key,
            Entry {
                revision,
                value: encoded.into(),
                certificate,
                bytes: size,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn borrowed_cache_keys_preserve_canonical_arguments_and_principal_scopes() {
        let args = json!({"escaped":"\u{0000}\n🌸", "nested":[null,{"x":1}]});
        for principal in [
            Value::Null,
            json!({"subject":"alice","claims":{"role":"reader"}}),
        ] {
            assert_eq!(
                key("quoted\"method", &args, &principal),
                serde_json::to_string(&json!({
                    "invocation":{"name":"quoted\"method","args":args},
                    "principal":principal,
                }))
                .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn flights_join_only_identical_snapshots_and_cancel_without_poisoning() {
        let cache = QueryCache::default();
        let first = cache.flight(1, "read").unwrap();
        let second = cache.flight(1, "read").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let held = first.lock().await;
        assert!(second.try_lock().is_err());
        assert!(cache.flight(2, "read").unwrap().try_lock().is_ok());
        assert!(cache.flight(1, "other").unwrap().try_lock().is_ok());
        drop(held);
        assert!(*second.lock().await);
        *second.lock().await = false;
        assert!(!*first.lock().await);
        drop((first, second));
        assert!(*cache.flight(1, "read").unwrap().lock().await);
    }

    #[test]
    fn flight_registry_is_byte_bounded_and_reclaims_finished_entries() {
        let cache = QueryCache::new(4096, 1024);
        let active: Vec<_> = (0..3)
            .map(|index| cache.flight(1, &index.to_string()).unwrap())
            .collect();
        assert!(cache.flight(1, "full").is_none());
        drop(active);
        let large = cache.flight(1, &"x".repeat(768)).unwrap();
        assert!(cache.flight(1, "full").is_none());
        drop(large);
        assert!(cache.flight(1, "new").is_some());
        assert!(cache.flight(1, &"x".repeat(769)).is_none());
        assert!(QueryCache::new(0, 0).flight(0, "disabled").is_none());
    }

    #[test]
    fn unrelated_revisions_reuse_and_late_evaluations_cannot_replace_newer_values() {
        let cache = QueryCache::default();
        let snapshot = Records::default();
        cache.insert(
            1,
            "constant".into(),
            json!(1),
            Arc::<DependencyCertificate>::default(),
        );
        assert_eq!(cache.get(2, "constant", &snapshot), Some(json!(1)));
        cache.insert(
            2,
            "constant".into(),
            json!(2),
            Arc::<DependencyCertificate>::default(),
        );
        cache.insert(
            1,
            "constant".into(),
            json!(0),
            Arc::<DependencyCertificate>::default(),
        );
        assert_eq!(cache.get(3, "constant", &snapshot), Some(json!(2)));
    }

    #[test]
    fn byte_budget_replaces_count_cap_and_can_disable_reuse() {
        let snapshot = Records::default();
        let cache = QueryCache::new(512 * 1024, 0);
        for index in 0..1000 {
            cache.insert(
                1,
                index.to_string(),
                json!(index),
                Arc::<DependencyCertificate>::default(),
            );
        }
        assert_eq!(
            cache.get(1, "0", &snapshot),
            Some(json!(0)),
            "no fixed 256-entry cap"
        );
        let small = QueryCache::new(1024, 0);
        let value = json!("x".repeat(512));
        small.insert(
            1,
            "a".into(),
            value.clone(),
            Arc::<DependencyCertificate>::default(),
        );
        small.insert(
            1,
            "b".into(),
            value.clone(),
            Arc::<DependencyCertificate>::default(),
        );
        assert!(small.get(1, "a", &snapshot).is_none());
        assert_eq!(small.get(1, "b", &snapshot), Some(value));
        small.insert(
            1,
            "oversized".into(),
            json!("x".repeat(1024)),
            Arc::<DependencyCertificate>::default(),
        );
        assert!(small.get(1, "oversized", &snapshot).is_none());
        let disabled = QueryCache::new(0, 0);
        disabled.insert(
            1,
            "x".into(),
            Value::Null,
            Arc::<DependencyCertificate>::default(),
        );
        assert!(disabled.get(1, "x", &snapshot).is_none());
    }
}
