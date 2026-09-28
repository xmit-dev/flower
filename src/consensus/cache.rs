//! Parsed values of stored records, shared by every snapshot in the process.
//! Versions identify writes uniquely, so a version alone keys a value: a
//! snapshot that reads the same write finds the same allocation.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use serde_json::Value;

const SHARDS: usize = 64;
const DEFAULT_BYTES: usize = 256 << 20;
/// Values too big for a shard's slice share this fraction of the budget.
const LARGE_SHARE: usize = 4;
/// Uncacheable values warned about, so that each warns once.
const WARNED: usize = 64;

/// Values evict in insertion order once a shard exceeds its slice of three
/// quarters of the budget. A value too big for a slice goes to the last
/// quarter, shared by large values, which evict least recently read: each
/// costs much to parse again, and one read on every evaluation (a deployed
/// bundle, say) must stay. A value too big for that too is never cached, and
/// says so once. A value still referenced elsewhere stays alive, only
/// uncached.
pub(crate) struct ValueCache {
    shards: Box<[Mutex<Shard>]>,
    shard_bytes: usize,
    large: Mutex<Shard>,
    large_bytes: usize,
    warned: Mutex<VecDeque<u64>>,
}

#[derive(Default)]
struct Shard {
    values: HashMap<u64, (Arc<Value>, usize)>,
    order: VecDeque<u64>,
    bytes: usize,
}

impl Shard {
    fn get(&self, version: u64) -> Option<Arc<Value>> {
        self.values.get(&version).map(|(value, _)| value.clone())
    }

    /// The value of `version`, now the most recently read.
    fn touch(&mut self, version: u64) -> Option<Arc<Value>> {
        let value = self.get(version)?;
        if let Some(position) = self.order.iter().position(|&kept| kept == version) {
            self.order.remove(position);
            self.order.push_back(version);
        }
        Some(value)
    }

    /// Keep `value` at `cost`, evicting from the front to stay within
    /// `budget`; a value another reader kept meanwhile wins.
    fn keep(&mut self, version: u64, value: Arc<Value>, cost: usize, budget: usize) -> Arc<Value> {
        if let Some(kept) = self.get(version) {
            return kept;
        }
        while self.bytes + cost > budget {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some((_, bytes)) = self.values.remove(&oldest) {
                self.bytes -= bytes;
            }
        }
        self.values.insert(version, (value.clone(), cost));
        self.order.push_back(version);
        self.bytes += cost;
        value
    }
}

pub(crate) static VALUES: LazyLock<ValueCache> = LazyLock::new(|| {
    let bytes = std::env::var("FLOWER_VALUE_CACHE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_BYTES);
    ValueCache::new(bytes)
});

impl ValueCache {
    pub(crate) fn new(bytes: usize) -> Self {
        let large_bytes = bytes / LARGE_SHARE;
        Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            shard_bytes: (bytes - large_bytes) / SHARDS,
            large: Mutex::default(),
            large_bytes,
            warned: Mutex::default(),
        }
    }

    /// The value of the write `version` of record `key`, parsed from `json`
    /// if not cached.
    pub(crate) fn get_or_parse<'a>(
        &self,
        version: u64,
        key: &str,
        json: impl FnOnce() -> std::borrow::Cow<'a, [u8]>,
    ) -> Arc<Value> {
        let shard = &self.shards[(version as usize) % SHARDS];
        if let Some(value) = shard.lock().expect("value cache lock").get(version) {
            return value;
        }
        if let Some(value) = self.large.lock().expect("value cache lock").touch(version) {
            return value;
        }
        let json = json();
        let value: Arc<Value> =
            Arc::new(serde_json::from_slice(&json).expect("stored JSON record"));
        #[cfg(test)]
        PARSES.with(|parses| *parses.borrow_mut().entry(version).or_default() += 1);
        // Parsed JSON takes several times its text; count that, not the text.
        let cost = 64 + json.len().saturating_mul(6);
        if cost <= self.shard_bytes {
            let mut shard = shard.lock().expect("value cache lock");
            return shard.keep(version, value, cost, self.shard_bytes);
        }
        if cost <= self.large_bytes {
            let mut large = self.large.lock().expect("value cache lock");
            return large.keep(version, value, cost, self.large_bytes);
        }
        self.refuse(version, key, json.len(), cost);
        value
    }

    /// Say, once per version, that a value is too big to cache: every read
    /// parses it again.
    fn refuse(&self, version: u64, key: &str, json_bytes: usize, cost: usize) {
        let mut warned = self.warned.lock().expect("value cache lock");
        if warned.contains(&version) {
            return;
        }
        if warned.len() >= WARNED {
            warned.pop_front();
        }
        warned.push_back(version);
        drop(warned);
        tracing::warn!(
            key,
            version,
            json_bytes,
            cost,
            limit = self.large_bytes,
            "stored value too big for the value cache, which keeps large values in a quarter of FLOWER_VALUE_CACHE_BYTES: every read of it parses its JSON again",
        );
    }
}

#[cfg(test)]
thread_local! {
    static PARSES: std::cell::RefCell<HashMap<u64, usize>> = Default::default();
}

/// How many times this thread parsed the stored value of write `version`.
#[cfg(test)]
pub(crate) fn parses(version: u64) -> usize {
    PARSES.with(|parses| parses.borrow().get(&version).copied().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &'static [u8]) -> impl FnOnce() -> std::borrow::Cow<'static, [u8]> {
        move || std::borrow::Cow::Borrowed(text)
    }

    #[test]
    fn versions_share_one_parse_until_evicted() {
        let cache = ValueCache::new(SHARDS * 1024);
        let first = cache.get_or_parse(1, "a", json(br#"{"a":1}"#));
        assert!(Arc::ptr_eq(
            &first,
            &cache.get_or_parse(1, "a", || unreachable!("cached"))
        ));
        for version in (1..200u64).map(|n| n * SHARDS as u64 + 1) {
            cache.get_or_parse(version, "a", json(br#"{"a":2}"#));
        }
        let reparsed = cache.get_or_parse(1, "a", json(br#"{"a":1}"#));
        assert!(!Arc::ptr_eq(&first, &reparsed));
        assert_eq!(first, reparsed);
    }

    #[test]
    fn values_past_a_shards_slice_stay_cached() {
        // Ultimator's bundle record (701,678 bytes of JSON) outgrew a shard's
        // slice of the default budget, and from then on every evaluation
        // parsed it again.
        let cache = ValueCache::new(SHARDS * 1024);
        let text = text(2048);
        let version = 1 << 40;
        let first = cache.get_or_parse(version, "bundle", json(text));
        for _ in 0..3 {
            assert!(Arc::ptr_eq(
                &first,
                &cache.get_or_parse(version, "bundle", json(text))
            ));
        }
        assert_eq!(parses(version), 1);
    }

    fn text(length: usize) -> &'static [u8] {
        let text = format!(r#"{{"javascript":"{}"}}"#, "x".repeat(length));
        Box::leak(text.into_bytes().into_boxed_slice())
    }

    #[test]
    fn large_values_evict_the_least_recently_read() {
        // A quarter of 64 KiB for large values: two of these fit, not three.
        let cache = ValueCache::new(SHARDS * 1024);
        let (hot, cold, new) = (2 << 40, 3 << 40, 4 << 40);
        cache.get_or_parse(hot, "hot", json(text(1024)));
        cache.get_or_parse(cold, "cold", json(text(1024)));
        cache.get_or_parse(hot, "hot", json(text(1024)));
        cache.get_or_parse(new, "new", json(text(1024)));
        for version in [hot, new] {
            cache.get_or_parse(version, "kept", || unreachable!("cached"));
        }
        cache.get_or_parse(cold, "cold", json(text(1024)));
        assert_eq!(parses(cold), 2, "the least recently read value left");
        assert_eq!(parses(hot), 1);
    }

    #[test]
    fn values_too_big_for_the_large_share_are_parsed_on_every_read_and_warn_once() {
        let cache = ValueCache::new(SHARDS * 1024);
        let version = 5 << 40;
        for _ in 0..3 {
            cache.get_or_parse(version, "huge", json(text(4096)));
        }
        assert_eq!(parses(version), 3);
        assert_eq!(
            cache.warned.lock().unwrap().iter().collect::<Vec<_>>(),
            [&version]
        );
    }
}
