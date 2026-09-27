//! Persistent application records. A snapshot clone only retains a tree root;
//! changing a key copies its tree path and shares every unchanged JSON value.
//! Every write gets a version, unique in this process, which certificates
//! compare instead of allocation identities.
//!
//! Records can lie over a backing, a pinned redb snapshot: the tree then holds
//! only writes the snapshot lacks, deletions included, and reads fall through
//! to the backing. A value read from the backing is kept by this instance, so
//! references to it live as long as the Records; clones start without them.

use std::borrow::Cow;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Bound, Index, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use im::{OrdMap, OrdSet};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use serde_json::value::RawValue;

use super::backing::{Backing, Encoded, Found, Merge, Slot, Spool, UNSEQUENCED};

/// A value with the version its write was given, or, over a backing, the
/// deletion of the backing's value.
#[derive(Clone, Debug)]
struct Entry {
    value: Option<Arc<Value>>,
    version: u64,
    // The persistence batch that stores this write, once the store assigns
    // it one.
    sequence: u64,
}

impl Slot for Entry {
    fn live(&self) -> bool {
        self.value.is_some()
    }

    fn version(&self) -> u64 {
        self.version
    }
}

// Records compare by content: two replicas' versions of one state differ.
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        match (&self.value, &other.value) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right) || left == right,
            (None, None) => true,
            _ => false,
        }
    }
}

static NEXT_VERSION: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static VERSIONS: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

/// A version no earlier write in this process has: versions are compared for
/// equality, never ordered. Threads take them in blocks to avoid contention.
pub(crate) fn next_version() -> u64 {
    const BLOCK: u64 = 1024;
    VERSIONS.with(|block| {
        let (next, end) = block.get();
        if next < end {
            block.set((next + 1, end));
            return next;
        }
        let start = NEXT_VERSION.fetch_add(BLOCK, Ordering::Relaxed);
        block.set((start + 1, start + BLOCK));
        start
    })
}

/// A bound on every version handed out so far, to store alongside them.
pub(crate) fn versions_high_water() -> u64 {
    NEXT_VERSION.load(Ordering::Relaxed)
}

/// Make later versions exceed every version stored so far, which a
/// restarted process reads back from disk.
pub(crate) fn versions_after(version: u64) {
    NEXT_VERSION.fetch_max(version.saturating_add(1), Ordering::Relaxed);
}

/// Values this instance read from its backing, so that references to them
/// stay valid for as long as the instance. Entries are boxed and only removed
/// through `&mut`, while no reference can be outstanding.
#[derive(Default)]
struct Memo(Mutex<HashMap<String, Box<(String, Arc<Value>, u64)>>>);

impl std::fmt::Debug for Memo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Memo")
    }
}

impl Memo {
    fn get(&self, key: &str) -> Option<(&String, &Arc<Value>, u64)> {
        let values = self.0.lock().expect("records memo lock");
        let kept = values.get(key)?;
        let kept: *const (String, Arc<Value>, u64) = &**kept;
        // SAFETY: the box is neither moved nor dropped while `self` is borrowed.
        let kept = unsafe { &*kept };
        Some((&kept.0, &kept.1, kept.2))
    }

    fn keep(&self, key: String, value: Arc<Value>, version: u64) -> (&String, &Arc<Value>, u64) {
        let mut values = self.0.lock().expect("records memo lock");
        let kept = values
            .entry(key.clone())
            .or_insert_with(|| Box::new((key, value, version)));
        let kept: *const (String, Arc<Value>, u64) = &**kept;
        // SAFETY: as in `get`; an existing entry is never replaced.
        let kept = unsafe { &*kept };
        (&kept.0, &kept.1, kept.2)
    }

    fn forget(&mut self, key: &str) {
        self.0.get_mut().expect("records memo lock").remove(key);
    }

    fn clear(&mut self) {
        self.0.get_mut().expect("records memo lock").clear();
    }
}

#[derive(Debug, Default)]
pub struct Records {
    // A pinned snapshot beneath the tree, if any.
    backing: Option<Arc<Backing>>,
    // Every record without a backing, only the backing's changes with one.
    map: OrdMap<String, Entry>,
    // Keys of records inserted since loading whose nesting is too deep.
    invalid_depth: OrdSet<String>,
    // Source keys inserted since loading whose syntax is invalid.
    invalid_sources: OrdSet<String>,
    // The original unprefixed graph.
    graph: crate::evaluator::ReactiveIndex,
    // Graph generations, by their physical record prefixes.
    graphs: OrdMap<String, crate::evaluator::ReactiveIndex>,
    // None follows the durable pointer; Some(None) forces the original graph.
    view: Option<Option<String>>,
    // Membership tokens of physical sources and indexes, shared by every
    // graph generation. A token changes with every write to a member, and
    // stays once created: there is one per collection or index.
    memberships: im::HashMap<String, u64>,
    memo: Memo,
}

impl Clone for Records {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
            map: self.map.clone(),
            invalid_depth: self.invalid_depth.clone(),
            invalid_sources: self.invalid_sources.clone(),
            graph: self.graph.clone(),
            graphs: self.graphs.clone(),
            view: self.view.clone(),
            memberships: self.memberships.clone(),
            memo: Memo::default(),
        }
    }
}

// Metadata is derived from records. Its shared validation proof is deliberately
// absent from equality and serialization, just like a process-local cache.
impl PartialEq for Records {
    fn eq(&self, other: &Self) -> bool {
        if self.backing.is_none() && other.backing.is_none() {
            return self.map == other.map;
        }
        self.entries(..)
            .zip(other.entries(..))
            .all(|((left_key, left), (right_key, right))| left_key == right_key && left == right)
            && self.len() == other.len()
    }
}

impl Eq for Records {}

type Bounds = (Bound<String>, Bound<String>);

fn owned<R: RangeBounds<str> + ?Sized>(range: &R) -> Bounds {
    (
        range.start_bound().map(str::to_owned),
        range.end_bound().map(str::to_owned),
    )
}

impl Records {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records served from a stored snapshot, with derived metadata rebuilt
    /// from its reader records rather than from every record.
    pub(crate) fn backed(backing: Arc<Backing>) -> Self {
        let mut records = Self {
            backing: Some(backing),
            ..Self::default()
        };
        records.rebuild_metadata();
        records
    }

    /// Move onto a newer snapshot of the same records, which holds every
    /// persistence batch up to `persisted`. Tree entries it holds at the same
    /// version, and the writes of those batches, are dropped. The records and
    /// their metadata are unchanged.
    pub(crate) fn rebase(&mut self, backing: Arc<Backing>, persisted: u64) {
        // A snapshot without a key cannot tell a persisted deletion from an
        // insertion still queued, so only a deletion's batch retires it.
        let persisted: Vec<String> = self
            .map
            .iter()
            .filter(|(key, entry)| {
                entry.sequence <= persisted
                    || entry.value.is_some() && backing.version(key) == Some(entry.version)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in persisted {
            self.map.remove(&key);
        }
        self.backing = Some(backing);
        self.memo.clear();
    }

    /// The same records and metadata served from a backing: every record,
    /// with its version, stored in an in-memory database beneath an empty
    /// tree.
    #[cfg(test)]
    pub(crate) fn backed_copy(&self) -> Self {
        let stored: Vec<(String, u64, Vec<u8>)> = self
            .merged(owned(&..))
            .map(|found| {
                let key = found.key().to_owned();
                let version = found.version();
                let json = match found {
                    Found::Tree(_, entry) => {
                        serde_json::to_vec(entry.value.as_deref().expect("live record"))
                            .expect("stored JSON")
                    }
                    Found::Stored(stored) => stored.json().to_vec(),
                };
                (key, version, json)
            })
            .collect();
        let backing = Backing::in_memory(
            stored
                .iter()
                .map(|(key, version, json)| (key.as_str(), *version, json.clone())),
        );
        Self {
            backing: Some(Arc::new(backing)),
            map: OrdMap::new(),
            memo: Memo::default(),
            ..self.clone()
        }
    }

    #[cfg(test)]
    pub(crate) fn backing(&self) -> Option<&Arc<Backing>> {
        self.backing.as_ref()
    }

    /// Whether it serves everything from a backing, holding nothing more.
    pub(crate) fn is_settled(&self) -> bool {
        self.backing.is_some() && self.map.is_empty()
    }

    /// Records the tree holds beyond the backing.
    #[cfg(test)]
    pub(crate) fn overlay_len(&self) -> usize {
        self.map.len()
    }

    pub fn len(&self) -> usize {
        let Some(backing) = &self.backing else {
            return self.map.len();
        };
        let mut len = backing.len() as isize;
        for (key, entry) in &self.map {
            match (entry.value.is_some(), backing.contains(key)) {
                (true, false) => len += 1,
                (false, true) => len -= 1,
                _ => {}
            }
        }
        len as usize
    }

    pub fn is_empty(&self) -> bool {
        match &self.backing {
            None => self.map.is_empty(),
            Some(_) => self.merged(owned(&..)).next().is_none(),
        }
    }

    fn merged(&self, (lower, upper): Bounds) -> impl DoubleEndedIterator<Item = Found<'_, Entry>> + Send {
        let stored: Box<dyn DoubleEndedIterator<Item = super::backing::Stored> + Send + '_> = match &self.backing {
            Some(backing) => backing.range(
                lower.as_ref().map(String::as_str),
                upper.as_ref().map(String::as_str),
            ),
            None => Box::new(std::iter::empty()),
        };
        Merge::new(self.map.range::<_, String>((lower, upper)), stored)
    }

    /// A stored record's value, kept for the lifetime of `self`.
    fn keep<'a>(&'a self, found: Found<'a, Entry>) -> (&'a String, &'a Arc<Value>, u64) {
        match found {
            Found::Tree(key, entry) => (
                key,
                entry.value.as_ref().expect("merged records are live"),
                entry.version,
            ),
            Found::Stored(stored) => {
                if let Some(kept) = self.memo.get(&stored.key) {
                    return kept;
                }
                let value = stored.value();
                let version = stored.version();
                self.memo.keep(stored.key, value, version)
            }
        }
    }

    /// Snapshot input validation is O(1). Only newly inserted records are
    /// traversed; the persistent set remembers invalid transient wrapper keys.
    /// Such wrappers can occur during an ephemeral graph preview, so insertion
    /// records validity without rejecting them prematurely.
    pub fn has_valid_depth(&self) -> bool {
        self.invalid_depth.is_empty()
    }

    /// Source-key syntax depends only on the key, so each insertion validates it
    /// once. Values and snapshot clones retain this result without a global lock.
    pub(crate) fn has_valid_source_ids(&self) -> bool {
        self.invalid_sources.is_empty()
    }

    pub(crate) fn reactive(&self) -> &crate::evaluator::ReactiveIndex {
        static EMPTY: std::sync::LazyLock<crate::evaluator::ReactiveIndex> =
            std::sync::LazyLock::new(Default::default);
        match self.graph_generation() {
            None => &self.graph,
            Some(generation) => self.graphs.get(generation).unwrap_or(&EMPTY),
        }
    }

    /// The membership token of a physical collection, index bucket or index
    /// range: it changes with every write to one of its members.
    pub(crate) fn generation(&self, id: &str) -> Option<u64> {
        self.memberships.get(id).copied()
    }

    /// Rebuild all derived metadata from the stored records without reading
    /// them all: each graph's index comes from its reader records, and graph
    /// generations from their key prefixes. Loading a stored state needs
    /// nothing else, because the engine validated every record it wrote.
    pub(crate) fn rebuild_metadata(&mut self) {
        self.memberships = im::HashMap::new();
        self.graph = crate::evaluator::ReactiveIndex::scanned(self, "");
        let mut graphs = OrdMap::new();
        let mut cursor = String::from("graph:");
        while let Some(key) = self.keys_from(&cursor).next()
            && key.starts_with("graph:")
        {
            let Some((generation, _)) = key["graph:".len()..].split_once(':') else {
                cursor = format!("{key}\0");
                continue;
            };
            if valid_graph_generation(generation) {
                let prefix = format!("graph:{generation}:");
                graphs.insert(
                    generation.to_owned(),
                    crate::evaluator::ReactiveIndex::scanned(self, &prefix),
                );
            }
            cursor = format!("graph:{generation};");
        }
        self.graphs = graphs;
    }

    /// Store a physical record without deriving metadata, which
    /// `rebuild_metadata` then rebuilds. Only its own validity is checked.
    fn load(&mut self, key: String, value: Value) {
        let value = if value.is_null() && is_reader_record(&key) {
            READER_VALUE.clone()
        } else {
            Arc::new(value)
        };
        if !valid_depth(&value) {
            self.invalid_depth.insert(key.clone());
        }
        if !valid_source_id(&key) {
            self.invalid_sources.insert(key.clone());
        }
        self.map.insert(
            key,
            Entry {
                value: Some(value),
                version: next_version(),
                sequence: UNSEQUENCED,
            },
        );
    }

    /// Physical keys from `start` on, in order.
    pub(crate) fn keys_from(&self, start: &str) -> impl Iterator<Item = String> + '_ {
        self.merged((Bound::Included(start.to_owned()), Bound::Unbounded))
            .map(|found| found.key().to_owned())
    }

    /// The committed graph pointer. Absence identifies the original unprefixed
    /// graph; explicit views do not change this durable pointer.
    pub fn active_graph(&self) -> Option<&str> {
        self.get_raw_shared("reactive:active")
            .and_then(|value| value.as_str())
            .filter(|generation| valid_graph_generation(generation))
    }

    pub(crate) fn has_valid_graph_pointer(&self) -> bool {
        self.get_raw_shared("reactive:active")
            .is_none_or(|value| value.as_str().is_some_and(valid_graph_generation))
    }

    pub(crate) fn valid_graph_generation(generation: &str) -> bool {
        valid_graph_generation(generation)
    }

    /// All retained graph clocks, including the original graph. New writes
    /// must use a common time no earlier than any graph's last evaluation.
    pub(crate) fn graph_clocks(&self) -> impl Iterator<Item = &Value> {
        self.get_raw_shared("clock")
            .into_iter()
            .chain(self.graphs.keys().filter_map(|generation| {
                self.get_raw_shared(&format!("graph:{generation}:clock"))
            }))
            .map(Arc::as_ref)
    }

    /// The graph selected for logical reads and evaluator metadata.
    pub fn graph_generation(&self) -> Option<&str> {
        self.view
            .as_ref()
            .map_or_else(|| self.active_graph(), |view| view.as_deref())
    }

    /// Select a graph for evaluation, including its writes. Storage snapshots
    /// themselves always mutate raw keys, so cleanup can remove an old graph
    /// without accidentally deleting the currently active graph.
    pub fn graph_view(&self, generation: Option<&str>) -> Self {
        assert!(generation.is_none_or(valid_graph_generation));
        let mut view = self.clone();
        view.view = Some(generation.map(str::to_owned));
        view
    }

    /// Translate a logical evaluator patch key to its durable storage key.
    pub fn graph_key(&self, logical: &str) -> String {
        self.read_key(logical).into_owned()
    }

    fn read_key<'a>(&self, key: &'a str) -> Cow<'a, str> {
        if is_graph_record(key)
            && let Some(generation) = self.graph_generation()
        {
            return Cow::Owned(format!("graph:{generation}:{key}"));
        }
        Cow::Borrowed(key)
    }

    /// Enumerate selected roots by logical identity, without scanning sources
    /// or any of the other graph generations retained in this snapshot.
    pub fn graph_roots(&self) -> impl DoubleEndedIterator<Item = (&str, &Value)> {
        self.logical_range("root:", "root;")
    }

    /// Stored cells of the selected graph by logical identity.
    pub fn graph_cells(&self) -> impl DoubleEndedIterator<Item = (&str, &Value)> {
        self.logical_range("cell:", "cell;")
    }

    fn logical_range(
        &self,
        lower: &str,
        upper: &str,
    ) -> impl DoubleEndedIterator<Item = (&str, &Value)> {
        let prefix = self.graph_prefix();
        let offset = prefix.len();
        self.merged((
            Bound::Included(format!("{prefix}{lower}")),
            Bound::Excluded(format!("{prefix}{upper}")),
        ))
            .map(move |found| {
                let (key, value, _) = self.keep(found);
                (&key[offset..], value.as_ref())
            })
    }

    /// The durable key recording that `reader` depends on `dependency`. Keys
    /// are grouped by dependency, so propagation seeks one dependency's
    /// readers instead of holding a reverse index in memory. Dependency IDs are
    /// generated from escaped JSON and never contain NUL.
    pub fn reader_key(dependency: &str, reader: &str) -> String {
        format!("reader:{dependency}\0{reader}")
    }

    /// The durable key holding a cell's height: the number of cells on its
    /// longest derived path, itself included. Leaves, of height 1, have none.
    pub fn height_key(cell: &str) -> String {
        format!("height:{cell}")
    }

    /// Cells of the selected graph that read `dependency`, in key order.
    pub fn readers(&self, dependency: &str) -> impl Iterator<Item = String> + '_ {
        let prefix = format!("{}reader:{dependency}\0", self.graph_prefix());
        let offset = prefix.len();
        self.merged((Bound::Included(prefix.clone()), Bound::Unbounded))
            .map(|found| found.key().to_owned())
            .take_while(move |key| key.starts_with(prefix.as_str()))
            .map(move |key| key[offset..].to_owned())
    }

    /// Reader records of the dependencies that start with `prefix`, as
    /// dependency and reader.
    pub fn readers_under(&self, prefix: &str) -> impl Iterator<Item = (String, String)> + '_ {
        let start = format!("{}reader:{prefix}", self.graph_prefix());
        let offset = start.len() - prefix.len();
        self.merged((Bound::Included(start.clone()), Bound::Unbounded))
            .map(|found| found.key().to_owned())
            .take_while(move |key| key.starts_with(start.as_str()))
            .filter_map(move |key| {
                let (dependency, reader) = key[offset..].split_once('\0')?;
                Some((dependency.to_owned(), reader.to_owned()))
            })
    }

    pub fn has_readers(&self, dependency: &str) -> bool {
        self.readers(dependency).next().is_some()
    }

    fn graph_prefix(&self) -> String {
        self.graph_generation()
            .map_or_else(String::new, |generation| format!("graph:{generation}:"))
    }

    /// O(1) identity check useful for revision/cache comparisons.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        let same_backing = match (&self.backing, &other.backing) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        };
        same_backing
            && self.map.ptr_eq(&other.map)
            && (self.view == other.view || self.graph_generation() == other.graph_generation())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        let key = self.read_key(key);
        match self.map.get(key.as_ref()) {
            Some(entry) => entry.value.is_some(),
            None => self
                .backing
                .as_ref()
                .is_some_and(|backing| backing.contains(&key)),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.get_shared(key).map(Arc::as_ref)
    }

    pub fn get_shared(&self, key: &str) -> Option<&Arc<Value>> {
        self.get_raw_shared(self.read_key(key).as_ref())
    }

    /// Bypass active-graph resolution when applying or inspecting storage keys.
    pub fn get_raw_shared(&self, key: &str) -> Option<&Arc<Value>> {
        self.lookup(key).map(|(_, value, _)| value)
    }

    fn lookup(&self, key: &str) -> Option<(&String, &Arc<Value>, u64)> {
        if let Some((key, entry)) = self.map.get_key_value(key) {
            return entry.value.as_ref().map(|value| (key, value, entry.version));
        }
        if let Some(kept) = self.memo.get(key) {
            return Some(kept);
        }
        let (version, value) = self.backing.as_ref()?.get(key)?;
        Some(self.memo.keep(key.to_owned(), value, version))
    }

    /// The version of the write that stored this logical key's value.
    pub fn version(&self, key: &str) -> Option<u64> {
        self.raw_version(&self.read_key(key))
    }

    /// The version of the write that stored this physical key's value.
    pub(crate) fn raw_version(&self, key: &str) -> Option<u64> {
        match self.map.get(key) {
            Some(entry) => entry.value.as_ref().map(|_| entry.version),
            None => self.backing.as_ref()?.version(key),
        }
    }

    /// The versions of the stored records in `range`, in key order.
    pub fn range_versions<R: RangeBounds<str>>(&self, range: R) -> impl Iterator<Item = u64> + '_ {
        self.merged(owned(&range)).map(|found| found.version())
    }

    pub fn get_key_value(&self, key: &str) -> Option<(&String, &Value)> {
        self.lookup(self.read_key(key).as_ref())
            .map(|(key, value, _)| (key, value.as_ref()))
    }

    /// Return the old allocation without deep-copying it even when another
    /// application revision still owns that record.
    pub fn insert(&mut self, key: String, value: Value) -> Option<Arc<Value>> {
        // Reader records carry no payload; they share one allocation.
        if value.is_null() && is_reader_record(&key) {
            return self.insert_shared(key, READER_VALUE.clone());
        }
        self.insert_shared(key, Arc::new(value))
    }

    pub fn insert_shared(&mut self, key: String, value: Arc<Value>) -> Option<Arc<Value>> {
        self.insert_versioned(key, value, next_version())
    }

    /// Insert with a given version: a stored write keeps its version when it
    /// moves from the tree to the backing.
    pub(crate) fn insert_versioned(
        &mut self,
        key: String,
        value: Arc<Value>,
        version: u64,
    ) -> Option<Arc<Value>> {
        let key = if self.view.is_some() {
            self.graph_key(&key)
        } else {
            key
        };
        let previous = self.get_raw_shared(&key).cloned();
        if previous
            .as_ref()
            .is_some_and(|previous| Arc::ptr_eq(previous, &value))
        {
            return Some(value);
        }
        let depth_valid = valid_depth(&value);
        self.update_reactive(&key, previous.as_ref(), Some(&value), depth_valid);
        if previous.is_none() && !valid_source_id(&key) {
            self.invalid_sources.insert(key.clone());
        }
        if depth_valid {
            self.invalid_depth.remove(&key);
        } else {
            self.invalid_depth.insert(key.clone());
        }
        self.memo.forget(&key);
        self.map.insert(
            key,
            Entry {
                value: Some(value),
                version,
                sequence: UNSEQUENCED,
            },
        );
        previous
    }

    pub fn remove(&mut self, key: &str) -> Option<Arc<Value>> {
        self.remove_shared(key)
    }

    pub fn remove_shared(&mut self, key: &str) -> Option<Arc<Value>> {
        let key = if self.view.is_some() {
            self.read_key(key).into_owned()
        } else {
            key.to_owned()
        };
        let previous = self.get_raw_shared(&key).cloned();
        self.update_reactive(&key, previous.as_ref(), None, true);
        self.invalid_depth.remove(&key);
        self.invalid_sources.remove(&key);
        self.memo.forget(&key);
        if self.backing.is_none() {
            self.map.remove(&key);
        } else if previous.is_some() {
            // Even without the key, a later snapshot may hold an earlier
            // write of it until this deletion is persisted.
            self.map.insert(
                key,
                Entry {
                    value: None,
                    version: next_version(),
                    sequence: UNSEQUENCED,
                },
            );
        }
        previous
    }

    /// Assign the tree's write of the physical `key` to persistence batch
    /// `sequence`, so that a rebase knows when the backing holds it.
    pub(crate) fn stamp(&mut self, key: &str, sequence: u64) {
        if let Some(entry) = self.map.get_mut(key) {
            entry.sequence = sequence;
        }
    }

    /// Serve from an empty snapshot until the first rebase, so that
    /// deletions leave tombstones like they do over a stored snapshot.
    pub(crate) fn keep_deletions(&mut self) {
        if self.backing.is_none() {
            self.backing = Some(Arc::new(Backing::empty()));
        }
    }

    fn update_reactive(
        &mut self,
        key: &str,
        previous: Option<&Arc<Value>>,
        next: Option<&Arc<Value>>,
        depth_valid: bool,
    ) {
        if previous.is_none() && next.is_none() {
            return;
        }
        if key == super::retention::KEY {
            // Most commits rewrite retention's accounting; what ctx.history()
            // returns changes only with its database or incarnation.
            let identity = |state: Option<&Arc<Value>>| {
                state.map(|state| (state.get("database").cloned(), state.get("incarnation").cloned()))
            };
            if identity(previous) != identity(next) {
                self.memberships
                    .insert(HISTORY_MARKER.into(), crate::consensus::next_version());
            }
            return;
        }
        if let Some((generation, logical)) = split_graph_key(key) {
            let generation = generation.to_owned();
            let index = self.graphs.entry(generation.clone()).or_default();
            index.update(logical, previous, next, depth_valid);
            if next.is_none() && !self.has_prefix(&format!("graph:{generation}:")) {
                self.graphs.remove(&generation);
            }
        } else if key.starts_with("source:")
            || key.starts_with("index-entry:")
            || key.starts_with("ordered-entry:")
        {
            crate::evaluator::update_memberships(
                &mut self.memberships,
                key,
                previous.is_some(),
                next.is_some(),
            );
        } else {
            self.graph.update(key, previous, next, depth_valid);
        }
    }

    /// Whether any stored key other than one being removed starts with
    /// `prefix`.
    fn has_prefix(&self, prefix: &str) -> bool {
        self.keys_from(prefix)
            .take(2)
            .filter(|key| key.starts_with(prefix))
            .count()
            > 1
    }

    pub fn iter(&self) -> Iter<'_> {
        self.iter_within(owned(&..))
    }

    pub fn iter_shared(&self) -> impl DoubleEndedIterator<Item = (&String, &Arc<Value>)> + Send {
        self.shared_within(owned(&..))
    }

    pub fn range<R: RangeBounds<str>>(&self, range: R) -> Iter<'_> {
        self.iter_within(owned(&range))
    }

    fn iter_within(&self, bounds: Bounds) -> Iter<'_> {
        Iter(Box::new(
            self.shared_within(bounds)
                .map(|(key, value)| (key, value.as_ref())),
        ))
    }

    pub fn range_shared<R: RangeBounds<str>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = (&String, &Arc<Value>)> + Send + use<'_, R> {
        self.shared_within(owned(&range))
    }

    fn shared_within(
        &self,
        bounds: Bounds,
    ) -> impl DoubleEndedIterator<Item = (&String, &Arc<Value>)> + Send + '_ {
        self.merged(bounds).map(|found| {
            let (key, value, _) = self.keep(found);
            (key, value)
        })
    }

    /// Every record with its version as stored JSON, for writing a whole
    /// table: stored records are copied, not parsed.
    pub(crate) fn encoded(&self) -> impl Iterator<Item = Encoded> + '_ {
        self.merged(owned(&..)).map(|found| match found {
            Found::Tree(key, entry) => {
                let json = serde_json::to_vec(entry.value.as_deref().expect("live record"))
                    .expect("record JSON");
                Encoded::Tree(key.clone(), super::backing::encode(entry.version, &json))
            }
            Found::Stored(stored) => Encoded::Stored(stored),
        })
    }

    /// Owned records in `range`, read without keeping them in this instance:
    /// for scans too long to hold in memory.
    pub fn entries<R: RangeBounds<str>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = (String, Arc<Value>)> + '_ {
        self.merged(owned(&range)).map(|found| match found {
            Found::Tree(key, entry) => (
                key.clone(),
                entry.value.clone().expect("merged records are live"),
            ),
            Found::Stored(stored) => {
                let value = stored.value();
                (stored.key, value)
            }
        })
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = String> + '_ {
        self.merged(owned(&..)).map(|found| found.key().to_owned())
    }

    pub fn values(&self) -> impl DoubleEndedIterator<Item = &Value> {
        self.shared_within(owned(&..)).map(|(_, value)| value.as_ref())
    }
}

pub struct Iter<'a>(Box<dyn DoubleEndedIterator<Item = (&'a String, &'a Value)> + Send + 'a>);

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a String, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl Index<&str> for Records {
    type Output = Value;

    fn index(&self, key: &str) -> &Self::Output {
        self.get(key).expect("missing application record")
    }
}

impl Index<&String> for Records {
    type Output = Value;

    fn index(&self, key: &String) -> &Self::Output {
        &self[key.as_str()]
    }
}

impl Extend<(String, Value)> for Records {
    fn extend<T: IntoIterator<Item = (String, Value)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

fn valid_graph_generation(generation: &str) -> bool {
    generation.len() == 64
        && generation
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The marker a read of `ctx.history()` observes: it moves only when the
/// retention state's database or incarnation does.
pub(crate) const HISTORY_MARKER: &str = "history:retention";

fn is_graph_record(key: &str) -> bool {
    key == "clock"
        || key.starts_with("cell:")
        || key.starts_with("root:")
        || key.starts_with("reader:")
        || key.starts_with("height:")
}

static READER_VALUE: std::sync::LazyLock<Arc<Value>> =
    std::sync::LazyLock::new(|| Arc::new(Value::Null));

fn is_reader_record(key: &str) -> bool {
    key.starts_with("reader:")
        || split_graph_key(key).is_some_and(|(_, logical)| logical.starts_with("reader:"))
}

fn split_graph_key(key: &str) -> Option<(&str, &str)> {
    let (generation, logical) = key.strip_prefix("graph:")?.split_once(':')?;
    (valid_graph_generation(generation) && is_graph_record(logical))
        .then_some((generation, logical))
}

fn valid_source_id(id: &str) -> bool {
    let Some(encoded) = id.strip_prefix("source:") else {
        return true;
    };
    let Ok(pair) = serde_json::from_str::<(String, String)>(encoded) else {
        return false;
    };
    serde_json::to_string(&pair).is_ok_and(|canonical| canonical == encoded)
}

fn valid_depth(value: &Value) -> bool {
    // Each frame holds only a borrowed value, remaining depth and collection
    // iterator. The 128-level guard bounds recursion to 129 frames including
    // rejection, without allocating a traversal stack per record.
    // A record starts at depth one inside its snapshot.
    fn visit(value: &Value, remaining: u8) -> bool {
        if remaining == 0 {
            return false;
        }
        match value {
            Value::Array(values) => values.iter().all(|value| visit(value, remaining - 1)),
            Value::Object(values) => values.values().all(|value| visit(value, remaining - 1)),
            _ => true,
        }
    }
    visit(value, 128)
}

impl FromIterator<(String, Value)> for Records {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        let mut result = Self::new();
        result.extend(iter);
        result
    }
}

impl From<BTreeMap<String, Value>> for Records {
    fn from(data: BTreeMap<String, Value>) -> Self {
        data.into_iter().collect()
    }
}

impl<const N: usize> From<[(String, Value); N]> for Records {
    fn from(data: [(String, Value); N]) -> Self {
        data.into_iter().collect()
    }
}

impl<'a> IntoIterator for &'a Records {
    type Item = (&'a String, &'a Value);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Owned iteration explicitly requests owned JSON trees. Query and graph
/// paths use borrowed iteration/shared handles instead.
pub struct IntoIter(std::vec::IntoIter<(String, Arc<Value>)>);

impl Iterator for IntoIter {
    type Item = (String, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, value)| (key, Arc::unwrap_or_clone(value)))
    }
}

impl IntoIterator for Records {
    type Item = (String, Value);
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let entries: Vec<_> = self.entries(..).collect();
        IntoIter(entries.into_iter())
    }
}

impl Serialize for Records {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Stored JSON is already canonical: copy it rather than parse it.
        let mut map = serializer.serialize_map(None)?;
        for found in self.merged(owned(&..)) {
            match found {
                Found::Tree(key, entry) => {
                    map.serialize_entry(key, entry.value.as_deref().expect("live record"))?
                }
                Found::Stored(stored) => {
                    let json = stored.json();
                    let json = std::str::from_utf8(&json).map_err(serde::ser::Error::custom)?;
                    let raw: &RawValue =
                        serde_json::from_str(json).map_err(serde::ser::Error::custom)?;
                    map.serialize_entry(&stored.key, raw)?
                }
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Records {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RecordsVisitor;

        impl<'de> Visitor<'de> for RecordsVisitor {
            type Value = Records;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object of application records")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                // A decoded state loads like a stored one: records first, then
                // metadata from their reader records, not record by record.
                let mut records = Records::new();
                let mut spooled = Spool::current().map(|spool| spool.writer());
                while let Some((key, raw)) = map.next_entry::<String, Box<RawValue>>()? {
                    // Reset parser depth for each user value, just as the old
                    // BTreeMap decoder did. Raft wrappers spend no user budget.
                    let value: Value =
                        serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
                    match &mut spooled {
                        // Checked like loaded records, then kept on disk only.
                        Some(writer) => {
                            if !valid_depth(&value) {
                                records.invalid_depth.insert(key.clone());
                            }
                            if !valid_source_id(&key) {
                                records.invalid_sources.insert(key.clone());
                            }
                            writer
                                .insert(key, next_version(), raw.get().as_bytes())
                                .map_err(serde::de::Error::custom)?;
                        }
                        None => records.load(key, value),
                    }
                }
                if let Some(writer) = spooled {
                    let backing = writer.finish().map_err(serde::de::Error::custom)?;
                    records.backing = Some(Arc::new(backing));
                }
                records.rebuild_metadata();
                Ok(records)
            }
        }

        deserializer.deserialize_map(RecordsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GRAPH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const GRAPH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const CELL: &str = r#"cell:["leaf",null]"#;
    const ROOT: &str = r#"root:["leaf",null]"#;

    fn graph_cell(value: u64) -> Value {
        json!({
            "name": "leaf", "args": null,
            "outcome": {"ok": true, "value": value},
            "deps": [r#"source:["items","first"]"#]
        })
    }

    fn graph_records() -> Records {
        let mut records = Records::from([
            (CELL.into(), graph_cell(1)),
            (ROOT.into(), json!({"name": "leaf", "args": null})),
            ("clock".into(), json!(10)),
            (r#"source:["items","first"]"#.into(), json!(1)),
        ]);
        for (generation, value, clock) in [(GRAPH_A, 2, 20), (GRAPH_B, 3, 30)] {
            records.insert(format!("graph:{generation}:{CELL}"), graph_cell(value));
            records.insert(
                format!("graph:{generation}:{ROOT}"),
                json!({"name": "leaf", "args": null}),
            );
            records.insert(format!("graph:{generation}:clock"), json!(clock));
        }
        records
    }

    #[test]
    fn graph_pointer_switches_logical_reads_and_metadata_in_one_snapshot() {
        let mut records = graph_records();
        let legacy = records.clone();
        let legacy_outcome = legacy.get_shared(CELL).unwrap();
        let candidate = records.graph_view(Some(GRAPH_A));
        let candidate_outcome = candidate.get_shared(CELL).unwrap();
        assert!(!Arc::ptr_eq(legacy_outcome, candidate_outcome));
        assert_eq!(records[CELL]["outcome"]["value"], 1);
        assert_eq!(records["clock"], 10);
        records.insert("reactive:active".into(), json!(GRAPH_A));
        assert_eq!(records.active_graph(), Some(GRAPH_A));
        assert_eq!(records.graph_generation(), Some(GRAPH_A));
        assert_eq!(records[CELL]["outcome"]["value"], 2);
        assert_eq!(records["clock"], 20);
        assert!(records.contains_key(CELL));
        assert_eq!(
            records.get_key_value(CELL).unwrap().0,
            &format!("graph:{GRAPH_A}:{CELL}")
        );
        assert!(Arc::ptr_eq(
            records.get_shared(CELL).unwrap(),
            candidate.get_shared(CELL).unwrap()
        ));
        assert!(Arc::ptr_eq(
            records.get_shared(CELL).unwrap(),
            candidate_outcome
        ));
        assert_eq!(legacy[CELL]["outcome"]["value"], 1);
        assert_eq!(legacy["clock"], 10);
        assert_eq!(legacy.active_graph(), None);

        let active = records.clone();
        records.insert("reactive:active".into(), json!(GRAPH_B));
        assert_eq!(records[CELL]["outcome"]["value"], 3);
        assert_eq!(records["clock"], 30);
        assert_eq!(active[CELL]["outcome"]["value"], 2);
        assert_eq!(candidate[CELL]["outcome"]["value"], 2);
        assert_eq!(
            records.graph_roots().collect::<Vec<_>>(),
            [(ROOT, &json!({"name":"leaf","args":null}))]
        );
    }

    #[test]
    fn graph_views_route_writes_but_storage_cleanup_keeps_raw_key_semantics() {
        let mut records = graph_records();
        records.insert("reactive:active".into(), json!(GRAPH_A));
        let mut candidate = records.graph_view(Some(GRAPH_B));
        assert_eq!(candidate.active_graph(), Some(GRAPH_A));
        assert_eq!(candidate.graph_generation(), Some(GRAPH_B));
        assert!(!records.ptr_eq(&candidate));
        assert!(records.ptr_eq(&records.graph_view(Some(GRAPH_A))));
        assert_eq!(candidate, records);
        assert_eq!(
            serde_json::to_string(&candidate).unwrap(),
            serde_json::to_string(&records).unwrap()
        );
        let loaded: Records =
            serde_json::from_str(&serde_json::to_string(&candidate).unwrap()).unwrap();
        assert_eq!(loaded.graph_generation(), Some(GRAPH_A));
        candidate.insert(CELL.into(), graph_cell(42));
        candidate.insert("clock".into(), json!(40));
        candidate.insert(r#"source:["items","second"]"#.into(), json!(2));
        candidate.remove(ROOT);
        assert_eq!(candidate[CELL]["outcome"]["value"], 42);
        assert_eq!(candidate["clock"], 40);
        assert_eq!(records[CELL]["outcome"]["value"], 2);
        assert_eq!(
            records.graph_view(Some(GRAPH_B))[CELL]["outcome"]["value"],
            3
        );
        assert!(!candidate.contains_key(ROOT));
        assert!(candidate.graph_roots().next().is_none());
        assert!(candidate.get_raw_shared(ROOT).is_some());
        assert_eq!(
            candidate
                .keys()
                .filter(|key| key.starts_with("source:"))
                .count(),
            2
        );
        assert_eq!(candidate.graph_key(CELL), format!("graph:{GRAPH_B}:{CELL}"));
        assert_eq!(
            candidate.graph_key("clock"),
            format!("graph:{GRAPH_B}:clock")
        );
        assert_eq!(candidate.graph_key("bundle"), "bundle");
        assert_eq!(candidate.graph_view(None).graph_key(CELL), CELL);
        assert_eq!(candidate.graph_view(None)[CELL]["outcome"]["value"], 1);

        records.remove(CELL);
        records.remove(ROOT);
        records.remove("clock");
        assert!(records.get_raw_shared(CELL).is_none());
        assert_eq!(records[CELL]["outcome"]["value"], 2);
        assert_eq!(records["clock"], 20);
        assert_eq!(records.graph_view(None).graph_roots().count(), 0);
        assert_eq!(records.graph_roots().count(), 1);
        for logical in [CELL, ROOT, "clock"] {
            records.remove(&format!("graph:{GRAPH_B}:{logical}"));
        }
        assert!(
            !records.graphs.contains_key(GRAPH_B),
            "cleanup also releases derived metadata"
        );
        assert_eq!(records.graph_view(Some(GRAPH_B)).graph_roots().count(), 0);
    }

    #[test]
    fn graph_membership_indexes_cover_source_and_index_updates_in_every_generation() {
        let first = r#"source:["items","first"]"#;
        let second = r#"source:["items","second"]"#;
        let bucket = r#"index-entries:["items",["group"]]"#;
        let range = r#"index-range:["items",["group"]]"#;
        let index_entry = r#"index-entry:["items",["group"]]:"a":"first""#;
        let ordered_entry = r#"ordered-entry:["items",["group"]]:"a":"first""#;
        let mut records = Records::from([(first.into(), json!(1))]);
        let empty = records.graph_view(Some(GRAPH_A));
        let empty_membership = empty
            .generation(r#"collection:"items""#)
            .unwrap();
        records.insert(format!("graph:{GRAPH_A}:{CELL}"), graph_cell(1));
        let candidate = records.graph_view(Some(GRAPH_A));
        assert!((empty_membership == candidate
                .generation(r#"collection:"items""#)
                .unwrap()));
        records.insert(second.into(), json!(2));
        records.insert(index_entry.into(), json!(true));
        records.insert(ordered_entry.into(), json!(true));
        // B is still empty: its view must already see current source metadata.
        for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
            let view = records.graph_view(generation);
            assert!(view.generation(bucket).is_some());
            assert!(view.generation(range).is_some());
            assert!((empty_membership != view.generation(r#"collection:"items""#).unwrap()));
        }
        records.insert(format!("graph:{GRAPH_B}:{CELL}"), graph_cell(2));
        records.remove(first);
        for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
            assert!(
                records
                    .graph_view(generation)
                    .generation(r#"collection:"items""#)
                    .is_some()
            );
        }
        let markers = [r#"collection:"items""#, bucket, range];
        let tokens = markers.map(|marker| records.generation(marker));
        records.remove(second);
        records.remove(index_entry);
        records.remove(ordered_entry);
        // Emptying a collection or index changes its token; the token stays.
        for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
            let view = records.graph_view(generation);
            for (marker, token) in markers.iter().zip(&tokens) {
                assert!(view.generation(marker).is_some(), "{generation:?}: {marker}");
                assert_ne!(view.generation(marker), *token, "{generation:?}: {marker}");
            }
        }
        assert!(
            empty
                .generation(r#"collection:"items""#)
                .is_some()
        );
    }

    #[test]
    fn graph_metadata_reconstructs_independently_of_physical_record_order() {
        let mut records = graph_records();
        records.insert("reactive:active".into(), json!(GRAPH_B));
        let loaded: Records =
            serde_json::from_str(&serde_json::to_string(&records).unwrap()).unwrap();
        let reverse: Records = records
            .iter()
            .rev()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        for restored in [loaded, reverse] {
            assert_eq!(restored, records);
            assert_eq!(restored[CELL]["outcome"]["value"], 3);
            for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
                let view = restored.graph_view(generation);
                assert_eq!(view.graph_roots().count(), 1);
                assert!(view.get_shared(CELL).is_some());
                // Tokens are process-local: a loaded state has none until a
                // member is written.
                let loaded = view.generation(r#"collection:"items""#);
                let mut removed = view.clone();
                removed.remove(r#"source:["items","first"]"#);
                assert!(removed.generation(r#"collection:"items""#).is_some());
                assert_ne!(removed.generation(r#"collection:"items""#), loaded);
            }
        }
        for malformed in ["", "abc", "A".repeat(64).as_str(), "g".repeat(64).as_str()] {
            assert!(!valid_graph_generation(malformed));
        }
        assert_eq!(
            split_graph_key(&format!("graph:{GRAPH_A}:{CELL}")),
            Some((GRAPH_A, CELL))
        );
        assert!(split_graph_key(&format!("graph:{GRAPH_A}:source:[]")).is_none());
    }

    #[test]
    fn graph_pointer_validation_and_retained_clocks_ignore_the_selected_view() {
        let mut records = graph_records();
        let legacy = records.clone();
        assert!(records.has_valid_graph_pointer());
        records.insert("reactive:active".into(), json!(GRAPH_A));
        assert!(records.has_valid_graph_pointer());
        for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
            assert_eq!(
                records
                    .graph_view(generation)
                    .graph_clocks()
                    .collect::<Vec<_>>(),
                [&json!(10), &json!(20), &json!(30)]
            );
        }
        records.remove(&format!("graph:{GRAPH_B}:clock"));
        assert_eq!(
            records.graph_clocks().collect::<Vec<_>>(),
            [&json!(10), &json!(20)]
        );
        for invalid in [
            Value::Null,
            json!(42),
            json!(""),
            json!("A".repeat(64)),
            json!("g".repeat(64)),
        ] {
            records.insert("reactive:active".into(), invalid);
            assert!(!records.has_valid_graph_pointer());
            assert!(!records.graph_view(Some(GRAPH_A)).has_valid_graph_pointer());
            let restored: Records =
                serde_json::from_str(&serde_json::to_string(&records).unwrap()).unwrap();
            assert!(!restored.has_valid_graph_pointer());
        }
        assert!(legacy.has_valid_graph_pointer());
        records.remove("reactive:active");
        assert!(records.has_valid_graph_pointer());
    }

    #[test]
    fn graph_generations_share_source_markers_but_keep_separate_outcomes() {
        let mut records = graph_records();
        let before = records.clone();
        let collection = r#"collection:"items""#;
        let old_marker = before.generation(collection).unwrap();
        for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
            assert!((old_marker == before
                    .graph_view(generation)
                    .generation(collection)
                    .unwrap()));
        }
        records.insert(r#"source:["items","second"]"#.into(), json!(2));
        let new_marker = records.generation(collection).unwrap();
        assert!((old_marker != new_marker));
        for generation in [None, Some(GRAPH_A), Some(GRAPH_B)] {
            let previous = before.graph_view(generation);
            let current = records.graph_view(generation);
            assert!((new_marker == current.generation(collection).unwrap()));
            assert!(Arc::ptr_eq(
                previous.get_shared(CELL).unwrap(),
                current.get_shared(CELL).unwrap()
            ));
        }
        let a = records.graph_view(Some(GRAPH_A));
        let b = records.graph_view(Some(GRAPH_B));
        assert!(!Arc::ptr_eq(
            a.get_shared(CELL).unwrap(),
            b.get_shared(CELL).unwrap()
        ));
        records.insert(format!("graph:{GRAPH_A}:{CELL}"), graph_cell(42));
        assert!((old_marker == before.generation(collection).unwrap()));
        assert!(Arc::ptr_eq(
            b.get_shared(CELL).unwrap(),
            records.graph_view(Some(GRAPH_B)).get_shared(CELL).unwrap()
        ));
    }

    /// Records written to a stored snapshot beneath an overlay read exactly
    /// like the same records in memory, from either end of any range, and
    /// keep doing so when rebased onto any persisted batch while later ones
    /// are still queued.
    #[test]
    fn backed_records_match_memory_through_writes_ranges_and_rebases() {
        let mut state = 0x5eed_u64;
        let mut next = move |bound: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        let keys: Vec<String> = (0..48).map(|n| format!("k{n:02}")).collect();
        let mut memory = Records::new();
        for (n, key) in keys.iter().enumerate() {
            if next(2) == 0 {
                memory.insert(key.clone(), json!({ "n": n }));
            }
        }
        let mut backed = memory.backed_copy();
        // The storage pipeline: the state after each batch, of which a
        // prefix is persisted. Batch 0 is the initial snapshot.
        let mut batches = vec![backed.clone()];
        let mut persisted = 0;
        let bound = |pick: u64, keys: &[String]| -> Bound<String> {
            match pick % 4 {
                0 => Bound::Unbounded,
                1 => Bound::Included(keys[(pick / 4) as usize % keys.len()].clone()),
                2 => Bound::Excluded(keys[(pick / 4) as usize % keys.len()].clone()),
                _ => Bound::Included(format!("{}~", keys[(pick / 4) as usize % keys.len()])),
            }
        };
        for step in 0..600 {
            let key = keys[next(keys.len() as u64) as usize].clone();
            match next(10) {
                0..=4 => {
                    let value = json!({ "step": step });
                    memory.insert(key.clone(), value.clone());
                    backed.insert(key.clone(), value);
                    backed.stamp(&key, batches.len() as u64);
                    batches.push(backed.clone());
                }
                5..=7 => {
                    assert_eq!(memory.remove(&key), backed.remove(&key), "step {step}");
                    backed.stamp(&key, batches.len() as u64);
                    batches.push(backed.clone());
                }
                _ => {
                    // Persistence catches up to some queued batch, often
                    // leaving later writes of the same keys queued behind it.
                    persisted += next((batches.len() - persisted) as u64) as usize;
                    let snapshot = batches[persisted].backed_copy();
                    backed.rebase(snapshot.backing.clone().expect("backing"), persisted as u64);
                    if persisted + 1 == batches.len() {
                        assert_eq!(backed.overlay_len(), 0, "step {step}");
                    }
                }
            }
            assert_eq!(memory.len(), backed.len(), "step {step}");
            assert_eq!(memory, backed, "step {step}");
            for key in &keys {
                assert_eq!(memory.get(key), backed.get(key), "step {step}: {key}");
                assert_eq!(memory.contains_key(key), backed.contains_key(key));
                assert_eq!(memory.version(key).is_some(), backed.version(key).is_some());
            }
            let range = (bound(next(200), &keys), bound(next(200), &keys));
            let valid = match (&range.0, &range.1) {
                (Bound::Included(lower) | Bound::Excluded(lower), Bound::Included(upper) | Bound::Excluded(upper)) => {
                    lower < upper
                }
                _ => true,
            };
            if valid {
                let range = (
                    range.0.as_ref().map(String::as_str),
                    range.1.as_ref().map(String::as_str),
                );
                let pattern: Vec<bool> = (0..keys.len()).map(|_| next(2) == 0).collect();
                let drain = |records: &Records| -> Vec<(String, Value)> {
                    let mut items = records.range_shared(range);
                    let mut drained = Vec::new();
                    for &front in &pattern {
                        let item = if front { items.next() } else { items.next_back() };
                        if let Some((key, value)) = item {
                            drained.push((key.clone(), (**value).clone()));
                        }
                    }
                    drained.extend(items.map(|(key, value)| (key.clone(), (**value).clone())));
                    drained
                };
                assert_eq!(drain(&memory), drain(&backed), "step {step}: {range:?}");
                let versions = backed.range_versions(range).count();
                assert_eq!(versions, memory.range(range).count(), "step {step}");
            }
            assert_eq!(
                memory.keys().collect::<Vec<_>>(),
                backed.keys().collect::<Vec<_>>()
            );
            assert_eq!(
                serde_json::to_string(&memory).unwrap(),
                serde_json::to_string(&backed).unwrap(),
                "step {step}"
            );
        }
    }

    fn iterative_depth_reference(value: &Value) -> bool {
        let mut pending = vec![(value, 1)];
        while let Some((value, depth)) = pending.pop() {
            if depth > 128 {
                return false;
            }
            match value {
                Value::Array(values) => {
                    pending.extend(values.iter().map(|value| (value, depth + 1)))
                }
                Value::Object(values) => {
                    pending.extend(values.values().map(|value| (value, depth + 1)))
                }
                _ => {}
            }
        }
        true
    }

    #[test]
    fn allocation_free_depth_matches_iterative_validation_at_every_boundary() {
        for leaf in [
            Value::Null,
            json!([]),
            json!({}),
            json!({"first": [], "last": [1, true]}),
        ] {
            for levels in 0..=132 {
                let value = (0..levels).fold(leaf.clone(), |value, level| {
                    if level % 2 == 0 {
                        json!([null, value, []])
                    } else {
                        json!({"first": {}, "last": value})
                    }
                });
                assert_eq!(
                    valid_depth(&value),
                    iterative_depth_reference(&value),
                    "{levels} levels"
                );
            }
        }
        for leaf in [Value::Null, json!([]), json!({})] {
            let edge = (0..127).fold(leaf, |value, _| json!([value]));
            assert!(
                valid_depth(&edge),
                "snapshot wrapper plus127 containers is permitted"
            );
            assert!(
                !valid_depth(&json!([edge])),
                "snapshot wrapper plus128 containers is rejected"
            );
        }
        let wide = Value::Object(
            (0..2048)
                .map(|index| (index.to_string(), json!([null, {"a": true}])))
                .collect(),
        );
        assert_eq!(valid_depth(&wide), iterative_depth_reference(&wide));
    }

    #[test]
    fn depth_guard_fits_a_small_stack_even_for_excessively_nested_values() {
        let edge = (0..127).fold(Value::Null, |value, _| json!({"next": value}));
        let excessive = (0..512).fold(Value::Null, |value, _| json!([value]));
        // Ordinary workers have much larger stacks. Return the values so their
        // recursive JSON destructors run on the test thread, outside this check.
        let values = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                assert!(valid_depth(&edge));
                assert!(!valid_depth(&excessive));
                (edge, excessive)
            })
            .unwrap()
            .join()
            .unwrap();
        drop(values);
    }

    #[test]
    fn source_identity_validity_is_persistent_and_rebuilt_from_disk() {
        let key = r#"source:["memo/🌺","quoted\"\n"]"#;
        let mut records = Records::from([(key.into(), json!(1))]);
        assert!(records.has_valid_source_ids());
        let pristine = records.clone();
        for malformed in [
            r#"source: ["memo/🌺","quoted\"\n"]"#,
            r#"source:["memo/🌺", "quoted\"\n"]"#,
            r#"source:["memo/🌺","quoted\"\u000a"]"#,
            r#"source:["memo/🌺",null]"#,
            "source:[",
            "source:[]",
        ] {
            records.insert(malformed.into(), json!(2));
            assert!(!records.has_valid_source_ids());
            records.insert(malformed.into(), json!(3));
            assert!(!records.has_valid_source_ids());
            assert!(pristine.has_valid_source_ids());
            let loaded: Records =
                serde_json::from_str(&serde_json::to_string(&records).unwrap()).unwrap();
            assert_eq!(loaded, records);
            assert!(!loaded.has_valid_source_ids());
            records.remove(malformed);
            assert!(records.has_valid_source_ids());
        }
        records.insert(key.into(), json!(42));
        assert!(records.has_valid_source_ids());
        assert_eq!(pristine[key], 1);
        let long = format!(
            "source:{}",
            serde_json::to_string(&["long", &"🌺".repeat(10_000)]).unwrap()
        );
        records.insert(long, json!(null));
        assert!(records.has_valid_source_ids());
    }

    #[test]
    fn snapshots_share_roots_and_unchanged_values_without_mutating_prior_revisions() {
        let records: Records = (0..1024)
            .map(|id| {
                (
                    format!("key-{id:04}"),
                    json!({"large": "x".repeat(1024), "id": id}),
                )
            })
            .collect();
        let mut next = records.clone();
        assert!(records.ptr_eq(&next));
        next.insert("key-0000".into(), json!("changed"));
        next.remove("key-0512");
        assert!(!records.ptr_eq(&next));
        assert_eq!(records["key-0000"]["id"], 0);
        assert_eq!(records["key-0512"]["id"], 512);
        assert_eq!(next["key-0000"], "changed");
        assert!(!next.contains_key("key-0512"));
        for id in [1, 2, 511, 513, 1023] {
            let key = format!("key-{id:04}");
            assert!(Arc::ptr_eq(
                records.get_shared(&key).unwrap(),
                next.get_shared(&key).unwrap()
            ));
        }
    }

    #[test]
    fn records_preserve_ordered_wire_shape_and_payload_depth_limits() {
        let old = BTreeMap::from([
            ("z".into(), json!({"a": [1, null, true]})),
            ("東京/🌸".into(), json!("value")),
            ("a".into(), json!(42)),
        ]);
        let records = Records::from(old.clone());
        let encoded = serde_json::to_string(&records).unwrap();
        assert_eq!(encoded, serde_json::to_string(&old).unwrap());
        assert_eq!(serde_json::from_str::<Records>(&encoded).unwrap(), records);
        assert_eq!(records.into_iter().collect::<BTreeMap<_, _>>(), old);

        let acceptable = format!(r#"{{"deep": {}0{}}}"#, "[".repeat(127), "]".repeat(127));
        assert!(serde_json::from_str::<Records>(&acceptable).is_ok());
        let excessive = format!(r#"{{"deep": {}0{}}}"#, "[".repeat(128), "]".repeat(128));
        assert!(serde_json::from_str::<Records>(&excessive).is_err());
    }

    #[test]
    fn cached_depth_validity_tracks_replacements_removals_and_shared_versions() {
        let deep = |levels| (0..levels).fold(Value::Null, |value, _| json!([value]));
        let mut records = Records::from([("edge".into(), deep(127))]);
        assert!(records.has_valid_depth());
        let pristine = records.clone();
        records.insert("first".into(), deep(128));
        records.insert_shared("second".into(), Arc::new(deep(129)));
        assert!(!records.has_valid_depth());
        assert!(pristine.has_valid_depth());
        let retained = records.clone();
        records.insert("first".into(), json!(42));
        assert!(!records.has_valid_depth());
        records.remove("second");
        assert!(records.has_valid_depth());
        assert!(!retained.has_valid_depth());
        records.extend([("first".into(), deep(129)), ("first".into(), deep(127))]);
        assert!(records.has_valid_depth());
        let reconstructed: Records =
            serde_json::from_str(&serde_json::to_string(&records).unwrap()).unwrap();
        assert!(reconstructed.has_valid_depth());
        assert_eq!(reconstructed, records);
    }

    /// A state decoded while spooling reads like one decoded into memory,
    /// its records served from the spool file across several commits.
    #[test]
    fn spooled_records_decode_like_memory() {
        let mut memory = Records::new();
        for n in 0..500 {
            memory.insert(
                format!(r#"source:["rows","{n:04}"]"#),
                json!({"n": n, "text": "x".repeat(n % 40)}),
            );
        }
        memory.insert(Records::reader_key("clock", "cell:a"), Value::Null);
        memory.insert("source:not-a-pair".into(), json!(1));
        let text = serde_json::to_string(&memory).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let spooled: Records = {
            let _spooling = Spool::activate(directory.path()).unwrap();
            serde_json::from_str(&text).unwrap()
        };
        assert!(spooled.backing.is_some());
        assert_eq!(spooled.overlay_len(), 0);
        assert_eq!(spooled, memory);
        assert!(!spooled.has_valid_source_ids());
        assert_eq!(spooled.get(r#"source:["rows","0042"]"#), memory.get(r#"source:["rows","0042"]"#));
        assert_eq!(serde_json::to_string(&spooled).unwrap(), text);
        let decoded: Records = serde_json::from_str(&text).unwrap();
        assert!(decoded.backing.is_none());
    }

    /// Spooled runs read like memory from either end of any range, across
    /// their indexed blocks.
    #[test]
    fn spooled_runs_match_memory_through_ranges_from_either_end() {
        let mut state = 0x5b001_u64;
        let mut next = move |bound: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        let keys: Vec<String> = (0..700).map(|n| format!("k{n:04}")).collect();
        let mut memory = Records::new();
        for key in &keys {
            if next(3) != 0 {
                memory.insert(key.clone(), json!({ "key": key, "n": next(1000) }));
            }
        }
        let text = serde_json::to_string(&memory).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let spooled: Records = {
            let _spooling = Spool::activate(directory.path()).unwrap();
            serde_json::from_str(&text).unwrap()
        };
        assert_eq!(spooled.len(), memory.len());
        for key in &keys {
            assert_eq!(spooled.get(key), memory.get(key), "{key}");
        }
        let bound = |pick: u64| -> Bound<String> {
            let key = &keys[(pick / 3) as usize % keys.len()];
            match pick % 3 {
                0 => Bound::Unbounded,
                1 => Bound::Included(key.clone()),
                _ => Bound::Excluded(key.clone()),
            }
        };
        for _ in 0..400 {
            let range = (bound(next(3000)), bound(next(3000)));
            if let (Bound::Included(lower) | Bound::Excluded(lower), Bound::Included(upper) | Bound::Excluded(upper)) =
                &range
                && lower >= upper
            {
                continue;
            }
            let range = (range.0.as_ref().map(String::as_str), range.1.as_ref().map(String::as_str));
            let pattern: Vec<bool> = (0..keys.len()).map(|_| next(2) == 0).collect();
            let drain = |records: &Records| -> Vec<String> {
                let mut items = records.range_shared(range);
                let mut drained = Vec::new();
                for &front in &pattern {
                    let item = if front { items.next() } else { items.next_back() };
                    drained.extend(item.map(|(key, _)| key.clone()));
                }
                drained
            };
            assert_eq!(drain(&spooled), drain(&memory), "{range:?}");
        }
    }

    #[test]
    fn spooling_rejects_records_out_of_key_order() {
        let directory = tempfile::tempdir().unwrap();
        let _spooling = Spool::activate(directory.path()).unwrap();
        let error = serde_json::from_str::<Records>(r#"{"b":1,"a":2}"#).unwrap_err();
        assert!(error.to_string().contains("key order"), "{error}");
    }
}
