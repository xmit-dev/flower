//! Bounded, process-local Cranelift function-code cache. Wizer changes initialized
//! data between bundle images while virtually all native function code is equal.
use std::{borrow::Cow, collections::HashMap, sync::Mutex};
use wasmtime::CacheStore;

#[derive(Default)]
pub(super) struct CodeCache(Mutex<Entries>);
#[derive(Debug, Default)]
struct Entries {
    bytes: usize,
    values: HashMap<Vec<u8>, Vec<u8>>,
}
impl std::fmt::Debug for CodeCache {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self.0.lock().map_err(|_| std::fmt::Error)?;
        output
            .debug_struct("CodeCache")
            .field("entries", &entries.values.len())
            .field("bytes", &entries.bytes)
            .finish()
    }
}
impl CacheStore for CodeCache {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Owned(self.0.lock().ok()?.values.get(key)?.clone()))
    }
    fn insert(&self, key: &[u8], value: Vec<u8>) -> bool {
        let Ok(mut entries) = self.0.lock() else {
            return false;
        };
        if entries.values.contains_key(key) {
            return true;
        }
        if entries.bytes + key.len() + value.len() > 32 * 1024 * 1024 {
            return false;
        }
        entries.bytes += key.len() + value.len();
        entries.values.insert(key.to_vec(), value);
        true
    }
}
