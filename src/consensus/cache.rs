//! Parsed values of stored records, shared by every snapshot in the process.
//! Versions identify writes uniquely, so a version alone keys a value: a
//! snapshot that reads the same write finds the same allocation.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use serde_json::Value;

const SHARDS: usize = 64;
const DEFAULT_BYTES: usize = 256 << 20;

/// Values evict in insertion order once a shard exceeds its share of the
/// budget; a value still referenced elsewhere stays alive, only uncached.
pub(crate) struct ValueCache {
    shards: Box<[Mutex<Shard>]>,
    shard_bytes: usize,
}

#[derive(Default)]
struct Shard {
    values: HashMap<u64, (Arc<Value>, usize)>,
    order: VecDeque<u64>,
    bytes: usize,
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
        Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            shard_bytes: bytes / SHARDS,
        }
    }

    /// The value of the write `version`, parsed from `json` if not cached.
    pub(crate) fn get_or_parse<'a>(
        &self,
        version: u64,
        json: impl FnOnce() -> std::borrow::Cow<'a, [u8]>,
    ) -> Arc<Value> {
        let shard = &self.shards[(version as usize) % SHARDS];
        if let Some((value, _)) = shard.lock().expect("value cache lock").values.get(&version) {
            return value.clone();
        }
        let json = json();
        let value: Arc<Value> =
            Arc::new(serde_json::from_slice(&json).expect("stored JSON record"));
        // Parsed JSON takes several times its text; count that, not the text.
        let cost = 64 + json.len().saturating_mul(6);
        if cost > self.shard_bytes {
            return value;
        }
        let mut shard = shard.lock().expect("value cache lock");
        if let Some((cached, _)) = shard.values.get(&version) {
            return cached.clone();
        }
        while shard.bytes + cost > self.shard_bytes {
            let Some(oldest) = shard.order.pop_front() else {
                break;
            };
            if let Some((_, bytes)) = shard.values.remove(&oldest) {
                shard.bytes -= bytes;
            }
        }
        shard.values.insert(version, (value.clone(), cost));
        shard.order.push_back(version);
        shard.bytes += cost;
        value
    }
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
        let first = cache.get_or_parse(1, json(br#"{"a":1}"#));
        assert!(Arc::ptr_eq(
            &first,
            &cache.get_or_parse(1, || unreachable!("cached"))
        ));
        for version in (1..200u64).map(|n| n * SHARDS as u64 + 1) {
            cache.get_or_parse(version, json(br#"{"a":2}"#));
        }
        let reparsed = cache.get_or_parse(1, json(br#"{"a":1}"#));
        assert!(!Arc::ptr_eq(&first, &reparsed));
        assert_eq!(first, reparsed);
    }
}
