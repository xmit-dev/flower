//! Persistent retry history. Writer snapshots retain one immutable tree root,
//! and each new receipt shares the prior history without copying its results.
//! Like records, receipts can lie over a stored snapshot, with the tree
//! holding only the writes it lacks.

use std::collections::{BTreeMap, HashMap};
use std::ops::{Bound, Index};
use std::sync::{Arc, Mutex};

use im::OrdMap;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Receipt;
use super::backing::{Backing, Encoded, Found, Merge, Slot, Spool, Stored, UNSEQUENCED};
use super::records::next_version;

#[derive(Clone, Debug)]
struct Entry {
    receipt: Option<Arc<Receipt>>,
    version: u64,
    // As in `Records`: the persistence batch that stores this write.
    sequence: u64,
}

impl Slot for Entry {
    fn live(&self) -> bool {
        self.receipt.is_some()
    }

    fn version(&self) -> u64 {
        self.version
    }
}

/// Receipts read from the backing, kept like `Records` keeps its values.
#[derive(Default)]
struct Memo(Mutex<HashMap<String, Box<(String, Arc<Receipt>)>>>);

impl Memo {
    fn keep(&self, key: String, load: impl FnOnce() -> Arc<Receipt>) -> (&String, &Arc<Receipt>) {
        let mut receipts = self.0.lock().expect("receipts memo lock");
        let kept = receipts
            .entry(key.clone())
            .or_insert_with(|| Box::new((key, load())));
        let kept: *const (String, Arc<Receipt>) = &**kept;
        // SAFETY: the box is neither moved nor dropped while `self` is borrowed.
        let kept = unsafe { &*kept };
        (&kept.0, &kept.1)
    }
}

#[derive(Default)]
pub struct Receipts {
    backing: Option<Arc<Backing>>,
    map: OrdMap<String, Entry>,
    memo: Memo,
}

impl Clone for Receipts {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
            map: self.map.clone(),
            memo: Memo::default(),
        }
    }
}

impl std::fmt::Debug for Receipts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl PartialEq for Receipts {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(left, right)| left == right)
    }
}

impl Eq for Receipts {}

fn decode(stored: &Stored) -> Arc<Receipt> {
    Arc::new(serde_json::from_slice(&stored.json()).expect("stored JSON receipt"))
}

impl Receipts {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn backed(backing: Arc<Backing>) -> Self {
        Self {
            backing: Some(backing),
            ..Self::default()
        }
    }

    /// Move onto a newer snapshot holding every persistence batch up to
    /// `persisted`, dropping what it now holds, as `Records::rebase` does.
    pub(crate) fn rebase(&mut self, backing: Arc<Backing>, persisted: u64) {
        let persisted: Vec<String> = self
            .map
            .iter()
            .filter(|(key, entry)| {
                entry.sequence <= persisted
                    || entry.receipt.is_some() && backing.version(key) == Some(entry.version)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in persisted {
            self.map.remove(&key);
        }
        self.backing = Some(backing);
        self.memo = Memo::default();
    }

    pub(crate) fn backing(&self) -> Option<&Arc<Backing>> {
        self.backing.as_ref()
    }

    /// As `Records::is_settled`.
    pub(crate) fn is_settled(&self) -> bool {
        self.backing.is_some() && self.map.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn overlay_len(&self) -> usize {
        self.map.len()
    }

    /// The version of the write that stored this receipt.
    pub(crate) fn version(&self, key: &str) -> Option<u64> {
        match self.map.get(key) {
            Some(entry) => entry.receipt.as_ref().map(|_| entry.version),
            None => self.backing.as_ref()?.version(key),
        }
    }

    pub fn len(&self) -> usize {
        let Some(backing) = &self.backing else {
            return self.map.len();
        };
        let mut len = backing.len() as isize;
        for (key, entry) in &self.map {
            match (entry.receipt.is_some(), backing.contains(key)) {
                (true, false) => len += 1,
                (false, true) => len -= 1,
                _ => {}
            }
        }
        len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        let same_backing = match (&self.backing, &other.backing) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        };
        same_backing && self.map.ptr_eq(&other.map)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        match self.map.get(key) {
            Some(entry) => entry.receipt.is_some(),
            None => self.backing.as_ref().is_some_and(|backing| backing.contains(key)),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Receipt> {
        self.get_shared(key).map(Arc::as_ref)
    }

    pub fn get_shared(&self, key: &str) -> Option<&Arc<Receipt>> {
        self.get_key_shared(key).map(|(_, receipt)| receipt)
    }

    fn get_key_shared(&self, key: &str) -> Option<(&String, &Arc<Receipt>)> {
        if let Some((key, entry)) = self.map.get_key_value(key) {
            return entry.receipt.as_ref().map(|receipt| (key, receipt));
        }
        let stored = self
            .backing
            .as_ref()?
            .range(Bound::Included(key), Bound::Included(key))
            .next()?;
        Some(self.memo.keep(stored.key.clone(), || decode(&stored)))
    }

    pub fn get_key_value(&self, key: &str) -> Option<(&String, &Receipt)> {
        self.get_key_shared(key)
            .map(|(key, receipt)| (key, receipt.as_ref()))
    }

    pub fn insert(&mut self, key: String, receipt: Receipt) -> Option<Arc<Receipt>> {
        self.insert_shared(key, Arc::new(receipt))
    }

    pub fn insert_shared(&mut self, key: String, receipt: Arc<Receipt>) -> Option<Arc<Receipt>> {
        self.insert_versioned(key, receipt, next_version())
    }

    pub(crate) fn insert_versioned(
        &mut self,
        key: String,
        receipt: Arc<Receipt>,
        version: u64,
    ) -> Option<Arc<Receipt>> {
        let previous = self.get_shared(&key).cloned();
        self.memo.0.get_mut().expect("receipts memo lock").remove(&key);
        self.map.insert(
            key,
            Entry {
                receipt: Some(receipt),
                version,
                sequence: UNSEQUENCED,
            },
        );
        previous
    }

    pub fn remove(&mut self, key: &str) -> Option<Arc<Receipt>> {
        let previous = self.get_shared(key).cloned();
        self.memo.0.get_mut().expect("receipts memo lock").remove(key);
        if self.backing.is_none() {
            self.map.remove(key);
        } else if previous.is_some() {
            self.map.insert(
                key.to_owned(),
                Entry {
                    receipt: None,
                    version: next_version(),
                    sequence: UNSEQUENCED,
                },
            );
        }
        previous
    }

    /// Assign the tree's write of `key` to persistence batch `sequence`.
    pub(crate) fn stamp(&mut self, key: &str, sequence: u64) {
        if let Some(entry) = self.map.get_mut(key) {
            entry.sequence = sequence;
        }
    }

    /// As `Records::keep_deletions`.
    pub(crate) fn keep_deletions(&mut self) {
        if self.backing.is_none() {
            self.backing = Some(Arc::new(Backing::empty()));
        }
    }

    fn merged(
        &self,
        lower: Bound<String>,
    ) -> impl DoubleEndedIterator<Item = Found<'_, Entry>> + Send {
        let stored: Box<dyn DoubleEndedIterator<Item = Stored> + Send + '_> = match &self.backing {
            Some(backing) => backing.range(lower.as_ref().map(String::as_str), Bound::Unbounded),
            None => Box::new(std::iter::empty()),
        };
        Merge::new(
            self.map.range::<_, String>((lower, Bound::Unbounded)),
            stored,
        )
    }

    /// Start strictly after an incremental collection cursor without rescanning
    /// the retained prefix. Old immutable roots continue to own removed values.
    pub fn after(&self, cursor: Option<&str>) -> Iter<'_> {
        let lower = cursor.map_or(Bound::Unbounded, |cursor| Bound::Excluded(cursor.to_owned()));
        Iter(Box::new(self.merged(lower).map(|found| match found {
            Found::Tree(key, entry) => (key, entry.receipt.as_deref().expect("live receipt")),
            Found::Stored(stored) => {
                let (key, receipt) = self.memo.keep(stored.key.clone(), || decode(&stored));
                (key, receipt.as_ref())
            }
        })))
    }

    pub fn iter(&self) -> Iter<'_> {
        self.after(None)
    }

    /// Every receipt with its version as stored JSON.
    /// As `Records::encoded`.
    pub(crate) fn encoded(&self) -> impl Iterator<Item = Encoded> + '_ {
        self.merged(Bound::Unbounded).map(|found| match found {
            Found::Tree(key, entry) => {
                let json = serde_json::to_vec(entry.receipt.as_deref().expect("live receipt"))
                    .expect("receipt JSON");
                Encoded::Tree(key.clone(), super::backing::encode(entry.version, &json))
            }
            Found::Stored(stored) => Encoded::Stored(stored),
        })
    }

    /// The same receipts served from an in-memory stored snapshot.
    #[cfg(test)]
    pub(crate) fn backed_copy(&self) -> Self {
        let stored: Vec<(String, u64, Vec<u8>)> = self
            .merged(Bound::Unbounded)
            .map(|found| {
                let key = found.key().to_owned();
                let version = found.version();
                let json = match found {
                    Found::Tree(_, entry) => {
                        serde_json::to_vec(entry.receipt.as_deref().expect("live receipt"))
                            .expect("receipt JSON")
                    }
                    Found::Stored(stored) => stored.json().to_vec(),
                };
                (key, version, json)
            })
            .collect();
        Self::backed(Arc::new(Backing::in_memory(
            stored
                .iter()
                .map(|(key, version, json)| (key.as_str(), *version, json.clone())),
        )))
    }
}

pub struct Iter<'a>(Box<dyn DoubleEndedIterator<Item = (&'a String, &'a Receipt)> + Send + 'a>);

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a String, &'a Receipt);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl Index<&str> for Receipts {
    type Output = Receipt;

    fn index(&self, key: &str) -> &Self::Output {
        self.get(key).expect("missing request receipt")
    }
}

impl Index<&String> for Receipts {
    type Output = Receipt;

    fn index(&self, key: &String) -> &Self::Output {
        &self[key.as_str()]
    }
}

impl Extend<(String, Receipt)> for Receipts {
    fn extend<T: IntoIterator<Item = (String, Receipt)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl FromIterator<(String, Receipt)> for Receipts {
    fn from_iter<T: IntoIterator<Item = (String, Receipt)>>(iter: T) -> Self {
        let mut result = Self::new();
        result.extend(iter);
        result
    }
}

impl FromIterator<(String, Arc<Receipt>)> for Receipts {
    fn from_iter<T: IntoIterator<Item = (String, Arc<Receipt>)>>(iter: T) -> Self {
        let mut result = Self::new();
        for (key, receipt) in iter {
            result.insert_shared(key, receipt);
        }
        result
    }
}

impl From<BTreeMap<String, Receipt>> for Receipts {
    fn from(receipts: BTreeMap<String, Receipt>) -> Self {
        receipts.into_iter().collect()
    }
}

impl<const N: usize> From<[(String, Receipt); N]> for Receipts {
    fn from(receipts: [(String, Receipt); N]) -> Self {
        receipts.into_iter().collect()
    }
}

impl<'a> IntoIterator for &'a Receipts {
    type Item = (&'a String, &'a Receipt);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Serialize for Receipts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for found in self.merged(Bound::Unbounded) {
            match found {
                Found::Tree(key, entry) => {
                    map.serialize_entry(key, entry.receipt.as_deref().expect("live receipt"))?
                }
                Found::Stored(stored) => {
                    let json = stored.json();
                    let json = std::str::from_utf8(&json).map_err(serde::ser::Error::custom)?;
                    let raw: &serde_json::value::RawValue =
                        serde_json::from_str(json).map_err(serde::ser::Error::custom)?;
                    map.serialize_entry(&stored.key, raw)?
                }
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Receipts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ReceiptsVisitor;

        impl<'de> Visitor<'de> for ReceiptsVisitor {
            type Value = Receipts;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object of request receipts")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let Some(spool) = Spool::current() else {
                    let mut receipts = Receipts::new();
                    while let Some((key, receipt)) = map.next_entry::<String, Receipt>()? {
                        receipts.insert(key, receipt);
                    }
                    return Ok(receipts);
                };
                // As spooled records: checked, then kept on disk only.
                let mut writer = spool.writer();
                while let Some((key, raw)) = map.next_entry::<String, Box<serde_json::value::RawValue>>()? {
                    serde_json::from_str::<Receipt>(raw.get()).map_err(serde::de::Error::custom)?;
                    writer
                        .insert(key, next_version(), raw.get().as_bytes())
                        .map_err(serde::de::Error::custom)?;
                }
                let backing = writer.finish().map_err(serde::de::Error::custom)?;
                Ok(Receipts::backed(Arc::new(backing)))
            }
        }

        deserializer.deserialize_map(ReceiptsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn receipt(revision: u64) -> Receipt {
        Receipt {
            fingerprint: format!("fingerprint-{revision}"),
            revision,
            result: json!({"revision": revision, "large": "x".repeat(1024)}),
            epoch: None,
        }
    }

    #[test]
    fn writer_snapshots_share_history_and_keep_speculative_receipts_private() {
        let durable: Receipts = (0..1024)
            .map(|revision| (format!("request-{revision:04}"), receipt(revision)))
            .collect();
        let mut staged = durable.clone();
        assert!(durable.ptr_eq(&staged));
        staged.insert("successor".into(), receipt(1024));
        staged.insert("request-0000".into(), receipt(1025));
        assert!(!durable.ptr_eq(&staged));
        assert!(!durable.contains_key("successor"));
        assert_eq!(durable["request-0000"].revision, 0);
        assert_eq!(staged["request-0000"].revision, 1025);
        for revision in [1, 31, 512, 1023] {
            let key = format!("request-{revision:04}");
            assert!(Arc::ptr_eq(
                durable.get_shared(&key).unwrap(),
                staged.get_shared(&key).unwrap()
            ));
        }
    }

    #[test]
    fn ordered_receipts_preserve_old_snapshot_wire_format_and_result_defaults() {
        let old = BTreeMap::from([
            ("z".into(), receipt(1)),
            ("a".into(), receipt(2)),
            ("東京/🌸".into(), receipt(3)),
        ]);
        let receipts = Receipts::from(old.clone());
        let bytes = serde_json::to_vec(&receipts).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&old).unwrap());
        assert_eq!(
            serde_json::from_slice::<Receipts>(&bytes).unwrap(),
            receipts
        );
        assert_eq!(
            serde_json::from_slice::<BTreeMap<String, Receipt>>(&bytes).unwrap(),
            old
        );
        let missing_result: Receipts =
            serde_json::from_str(r#"{"old":{"fingerprint":"original","revision":1}}"#).unwrap();
        assert!(missing_result["old"].result.is_null());
    }

    #[test]
    fn backed_receipts_match_memory_through_writes_cursors_and_rebases() {
        let mut state = 0xacce55_u64;
        let mut next = move |bound: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        let keys: Vec<String> = (0..32).map(|n| format!("r{n:02}")).collect();
        let mut memory = Receipts::new();
        for (n, key) in keys.iter().enumerate() {
            if next(2) == 0 {
                memory.insert(key.clone(), receipt(n as u64));
            }
        }
        let mut backed = memory.backed_copy();
        // As for records: states after each batch, a prefix of them persisted.
        let mut batches = vec![backed.clone()];
        let mut persisted = 0;
        for step in 0..400 {
            let key = keys[next(keys.len() as u64) as usize].clone();
            match next(8) {
                0..=3 => {
                    memory.insert(key.clone(), receipt(step));
                    backed.insert(key.clone(), receipt(step));
                    backed.stamp(&key, batches.len() as u64);
                    batches.push(backed.clone());
                }
                4 | 5 => {
                    assert_eq!(memory.remove(&key), backed.remove(&key));
                    backed.stamp(&key, batches.len() as u64);
                    batches.push(backed.clone());
                }
                _ => {
                    persisted += next((batches.len() - persisted) as u64) as usize;
                    let snapshot = batches[persisted].backed_copy();
                    backed.rebase(snapshot.backing.clone().expect("backing"), persisted as u64);
                    if persisted + 1 == batches.len() {
                        assert_eq!(backed.overlay_len(), 0);
                    }
                }
            }
            assert_eq!(memory, backed, "step {step}");
            assert_eq!(memory.len(), backed.len(), "step {step}");
            for key in &keys {
                assert_eq!(memory.get(key), backed.get(key), "step {step}: {key}");
                assert_eq!(memory.contains_key(key), backed.contains_key(key));
            }
            let cursor = keys[next(keys.len() as u64) as usize].clone();
            assert!(memory.after(Some(&cursor)).eq(backed.after(Some(&cursor))));
            assert!(memory.iter().rev().eq(backed.iter().rev()));
            assert_eq!(
                serde_json::to_string(&memory).unwrap(),
                serde_json::to_string(&backed).unwrap()
            );
        }
    }

    #[test]
    fn spooled_receipts_decode_like_memory() {
        let memory: Receipts = (0..300)
            .map(|revision| (format!("request-{revision:04}"), receipt(revision)))
            .collect();
        let text = serde_json::to_string(&memory).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let spooled: Receipts = {
            let _spooling = Spool::activate(directory.path()).unwrap();
            serde_json::from_str(&text).unwrap()
        };
        assert!(spooled.backing().is_some());
        assert_eq!(spooled.overlay_len(), 0);
        assert_eq!(spooled, memory);
        assert_eq!(spooled.len(), 300);
        assert_eq!(serde_json::to_string(&spooled).unwrap(), text);
    }
}
