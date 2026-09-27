//! Structurally shared indexes derived from immutable application records.
//! Updating source values does no graph work. The graph's structure itself is
//! durable: `reader:` records hold reverse edges and `height:` records the
//! longest derived path below each cell, both maintained by the engine.

use super::*;

/// Heights found by a full traversal, the reference for the stored ones.
pub(super) type Depths = im::HashMap<String, u8>;

/// Stored cells of the selected graph, including invalid ones.
#[cfg(test)]
pub(super) fn cell_count(data: &Records) -> usize {
    data.graph_cells().count()
}

/// The selected graph's whole accounted size: what building it from nothing
/// costs.
#[cfg(test)]
pub(super) fn graph_bytes(data: &Records) -> usize {
    let empty = Records::new();
    data.graph_cells()
        .map(|(id, _)| id)
        .chain(data.graph_roots().map(|(id, _)| id))
        .map(|id| ReactiveIndex::unshared_bytes(data, &empty, id))
        .sum()
}

/// What the index derives from one stored cell record. It is parsed again
/// from the record whenever the record is replaced, not retained per cell.
struct Cell {
    // Parsed `scan:` dependencies, which have no reader records.
    scans: Vec<windows::Window>,
    // The collection and fields of each `index-bucket:` dependency.
    buckets: Vec<(String, Vec<String>)>,
    bytes: usize,
}

impl Cell {
    fn parse(id: &str, value: &Value) -> Self {
        let deps: Vec<&str> = value
            .get("deps")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        // validate_cell rejected any scan dependency that does not parse.
        let scans = deps
            .iter()
            .filter(|dep| windows::Window::is_dependency(dep))
            .filter_map(|dep| windows::Window::parse(dep))
            .collect();
        let buckets = deps
            .iter()
            .filter_map(|dep| dependencies::bucket_spec(dep))
            .collect();
        // Includes persistent depth certificates and pending append IDs, as
        // well as traversal reservations and the cell's reader records.
        let bytes = 384_usize
            .saturating_add(id.len().saturating_mul(6))
            .saturating_add(
                deps.iter()
                    .map(|dep| 160 + id.len() + dep.len().saturating_mul(2))
                    .sum::<usize>(),
            );
        Self {
            scans,
            buckets,
            bytes,
        }
    }
}

/// Scan windows by collection, then by index fields (`None` orders by key).
type ScanWindows = im::HashMap<String, im::HashMap<Option<Vec<String>>, windows::Windows>>;

#[derive(Clone, Debug)]
pub(super) struct Root {
    pub value: Arc<Value>,
}

impl Root {
    pub fn name(&self) -> &str {
        self.value["name"].as_str().expect("validated root name")
    }

    pub fn args(&self) -> &Value {
        self.value.get("args").unwrap_or(&Value::Null)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReactiveIndex {
    // Scan windows that span buckets of their index's first field. Those
    // within one bucket are found among the reader records under it.
    scans: ScanWindows,
    // Field sets that some window within one bucket has scanned, by
    // collection. Like `buckets`, a set stays once added.
    narrow: im::HashMap<String, im::HashSet<Vec<String>>>,
    // Field sets that some cell has queried by equality, by collection. A set
    // stays once added: rebuilding it needs only the stored bucket specs, and
    // a stale one costs a lookup that finds no readers.
    buckets: im::HashMap<String, im::HashSet<Vec<String>>>,
    invalid_cells: im::OrdMap<String, EngineError>,
    invalid_roots: im::OrdMap<String, EngineError>,
    // Reader records of the clock, counted as records are written so that a
    // rebuild from them finds the same count.
    clock_readers: usize,
    // Unlike the cycle proof, this also changes for source dependency edges.
    // Optimistic patches must discover the same set of reactive readers.
    pub(super) shape: u64,
}

impl Default for ReactiveIndex {
    fn default() -> Self {
        Self {
            scans: im::HashMap::new(),
            narrow: im::HashMap::new(),
            buckets: im::HashMap::new(),
            invalid_cells: im::OrdMap::new(),
            invalid_roots: im::OrdMap::new(),
            clock_readers: 0,
            shape: crate::consensus::next_version(),
        }
    }
}

/// Record a write to a physical source or index entry `id`: its collection,
/// bucket or range token changes, and the collection's key token too when the
/// key appears or disappears.
pub(crate) fn update_memberships(
    memberships: &mut im::HashMap<String, u64>,
    id: &str,
    previous: bool,
    next: bool,
) {
    let Some(marker) = dependencies::membership_marker(id) else {
        return;
    };
    if previous != next
        && let Some(keys) = dependencies::keys_marker(&marker)
    {
        memberships.insert(keys, crate::consensus::next_version());
    }
    memberships.insert(marker, crate::consensus::next_version());
}

/// Cells whose scan result may change when a row of `collection` changes
/// from `previous` to `next`, as `ReactiveIndex::scan_readers` finds among
/// windows that span buckets. A window within one bucket can only hold
/// positions of that bucket, so the reader records under the row's bucket
/// hold every such window that could.
pub(super) fn scan_readers(
    records: &Records,
    collection: &str,
    key: &str,
    previous: Option<&Value>,
    next: Option<&Value>,
    readers: &mut Vec<String>,
) {
    let index = records.reactive();
    index.scan_readers(collection, key, previous, next, readers);
    for fields in index.narrow_fields(collection) {
        let position = |value: Option<&Value>| {
            value.and_then(|value| ranges::position(Some(fields), key, value))
        };
        let (before, after) = (position(previous), position(next));
        // As in the index: a value alone changes only windows that returned it.
        let values = before.is_some() && before == after;
        let positions = if values {
            vec![before]
        } else {
            vec![before, after]
        };
        for position in positions.into_iter().flatten() {
            let Some(bucket) = windows::bucket(&position) else {
                continue;
            };
            let prefix = windows::Window::bucket_dependency(collection, fields, bucket);
            for (dependency, reader) in records.readers_under(&prefix) {
                let Some(window) = windows::Window::parse(&dependency) else {
                    continue;
                };
                let bounds = if values {
                    window
                        .values
                        .as_ref()
                        .map(|(lower, upper)| (lower.as_str(), upper.as_str()))
                } else {
                    Some((window.lower.as_str(), window.upper.as_str()))
                };
                if let Some((lower, upper)) = bounds
                    && lower <= position.as_str()
                    && position.as_str() < upper
                {
                    readers.push(reader);
                }
            }
        }
    }
}

/// The dependency whose readers make evaluations uncacheable. Managed keys
/// change only through catalog writes, which every certificate observes.
const CLOCK: &str = "clock";

fn is_clock_reader(id: &str) -> bool {
    id.strip_prefix("reader:")
        .and_then(|edge| edge.split_once('\0'))
        .is_some_and(|(dependency, _)| dependency == CLOCK)
}

impl ReactiveIndex {
    /// The index of the graph whose records start with `prefix`, rebuilt from
    /// its reader records alone: scan windows, the field sets of equality
    /// buckets, and the cells that read the clock.
    pub(crate) fn scanned(records: &Records, prefix: &str) -> Self {
        let mut index = Self::default();
        let readers = format!("{prefix}reader:");
        fn edge(key: &str, offset: usize) -> Option<(&str, &str)> {
            key[offset..].split_once('\0')
        }
        let scans = format!("{readers}{}", windows::PREFIX);
        for key in records
            .keys_from(&scans)
            .take_while(|key| key.starts_with(&scans))
        {
            if let Some((dependency, reader)) = edge(&key, readers.len())
                && let Some(window) = windows::Window::parse(dependency)
            {
                index.change_scans(&[window], reader, true);
            }
        }
        // One seek per queried field set: skip each set's buckets at once.
        let buckets = format!("{readers}index-bucket:");
        let mut cursor = buckets.clone();
        while let Some(key) = records.keys_from(&cursor).next()
            && key.starts_with(&buckets)
        {
            cursor = match edge(&key, readers.len()).and_then(|(dependency, _)| {
                Some((
                    dependencies::bucket_spec(dependency)?,
                    dependencies::bucket_spec_text(dependency)?,
                ))
            }) {
                Some((spec, text)) => {
                    index.add_buckets(&[spec]);
                    format!("{buckets}{text};")
                }
                None => format!("{key}\0"),
            };
        }
        let clock = format!("{readers}{CLOCK}\0");
        index.clock_readers = records
            .keys_from(&clock)
            .take_while(|key| key.starts_with(&clock))
            .count();
        index
    }

    pub(crate) fn update(
        &mut self,
        id: &str,
        previous: Option<&Arc<Value>>,
        next: Option<&Arc<Value>>,
        record_depth_valid: bool,
    ) {
        if previous.is_none() && next.is_none() {
            return;
        }
        if id.starts_with("cell:") {
            self.update_cell(id, previous, next, record_depth_valid);
        } else if id.starts_with("root:") {
            self.update_root(id, previous, next, record_depth_valid);
        } else if previous.is_some() != next.is_some() && is_clock_reader(id) {
            if next.is_some() {
                self.clock_readers += 1;
            } else {
                self.clock_readers -= 1;
            }
        }
    }

    /// Cells whose scan result may change when a row of `collection` changes
    /// from `previous` to `next`: a row entering, leaving or moving touches
    /// membership windows at either position; a value alone touches the
    /// windows of rows returned at its position.
    pub(super) fn scan_readers(
        &self,
        collection: &str,
        key: &str,
        previous: Option<&Value>,
        next: Option<&Value>,
        readers: &mut Vec<String>,
    ) {
        let Some(indexes) = self.scans.get(collection) else {
            return;
        };
        for (fields, windows) in indexes {
            let position = |value: Option<&Value>| {
                value.and_then(|value| ranges::position(fields.as_deref(), key, value))
            };
            match (position(previous), position(next)) {
                (Some(before), Some(after)) if before == after => {
                    windows.readers(&before, true, readers);
                }
                (before, after) => {
                    for position in [before, after].into_iter().flatten() {
                        windows.readers(&position, false, readers);
                    }
                }
            }
        }
    }

    /// Field sets of `collection` with windows within one bucket.
    pub(super) fn narrow_fields(&self, collection: &str) -> impl Iterator<Item = &Vec<String>> {
        self.narrow
            .get(collection)
            .into_iter()
            .flat_map(|fields| fields.iter())
    }

    /// Field sets that derivations query by equality in `collection`.
    pub(super) fn bucket_fields(&self, collection: &str) -> impl Iterator<Item = &Vec<String>> {
        self.buckets
            .get(collection)
            .into_iter()
            .flat_map(|fields| fields.iter())
    }

    fn add_buckets(&mut self, buckets: &[(String, Vec<String>)]) {
        for (collection, fields) in buckets {
            let specs = self.buckets.entry(collection.clone()).or_default();
            if !specs.contains(fields) {
                specs.insert(fields.clone());
            }
        }
    }

    fn change_scans(&mut self, scans: &[windows::Window], reader: &str, insert: bool) {
        if scans.is_empty() {
            return;
        }
        let reader: Arc<str> = reader.into();
        for window in scans {
            if window.bucket().is_some() {
                let fields = window.fields.clone().expect("a bucket belongs to an index");
                let sets = self.narrow.entry(window.collection.clone()).or_default();
                if insert && !sets.contains(&fields) {
                    sets.insert(fields);
                }
                continue;
            }
            let indexes = self.scans.entry(window.collection.clone()).or_default();
            let windows = indexes.entry(window.fields.clone()).or_default();
            if insert {
                windows.insert(window, &reader);
            } else {
                windows.remove(window, &reader);
                if windows.is_empty() {
                    indexes.remove(&window.fields);
                    if indexes.is_empty() {
                        self.scans.remove(&window.collection);
                    }
                }
            }
        }
    }

    /// Accounted bytes of the graph entry for cell or root `id` that `base`
    /// does not share, i.e. what a transaction adds or replaces. Stored
    /// records are the graph's entries: one kept from the snapshot, a cell
    /// whose outcome alone changed, and a removed entry cost nothing.
    pub(super) fn unshared_bytes(staged: &Records, base: &Records, id: &str) -> usize {
        let Some(record) = staged.get_shared(id) else {
            return 0;
        };
        let old = base.get_shared(id);
        if old.is_some_and(|old| Arc::ptr_eq(old, record)) {
            return 0;
        }
        if id.starts_with("cell:") {
            let same_edges = old.is_some_and(|old| old.get("deps") == record.get("deps"))
                && staged.reactive().invalid_cells.get(id) == base.reactive().invalid_cells.get(id);
            if same_edges {
                0
            } else {
                Cell::parse(id, record).bytes
            }
        } else if id.starts_with("root:") {
            // Root lookup/traversal storage plus a pending append-frontier entry.
            320 + id.len().saturating_mul(4)
        } else {
            0
        }
    }

    pub(super) fn cacheable(&self) -> bool {
        self.clock_readers == 0
    }

    pub(super) fn validate(&self) -> EngineResult<()> {
        if let Some((_, error)) = self.invalid_cells.iter().next() {
            return Err(error.clone());
        }
        if let Some((_, error)) = self.invalid_roots.iter().next() {
            return Err(error.clone());
        }
        Ok(())
    }

    fn update_cell(
        &mut self,
        id: &str,
        previous: Option<&Arc<Value>>,
        next: Option<&Arc<Value>>,
        record_depth_valid: bool,
    ) {
        let old_error = self.invalid_cells.get(id).cloned();
        let new_error = next.map(|value| {
            if record_depth_valid {
                Ok(())
            } else {
                // Ephemeral cell outcomes may exceed the snapshot wrapper's
                // depth. Protect recursive identity handling using only args.
                depth(&value["args"], 0, "INPUT_INVALID")
            }
            .and_then(|()| {
                validate_cell(
                    id,
                    value,
                    previous,
                    previous.is_some() && old_error.is_none(),
                )
            })
            .err()
        });
        // An outcome change alone leaves every derived structure unchanged,
        // so compare dependencies before parsing either record.
        if let (Some(previous), Some(next), Some(error)) = (previous, next, &new_error)
            && old_error == *error
            && previous.get("deps") == next.get("deps")
        {
            return;
        }
        let old = previous.map(|value| Cell::parse(id, value));
        let new = next
            .zip(new_error)
            .map(|(value, error)| (Cell::parse(id, value), error));
        self.shape = crate::consensus::next_version();
        if let Some(old) = old {
            self.change_scans(&old.scans, id, false);
        }
        self.invalid_cells.remove(id);
        if let Some((new, error)) = new {
            self.change_scans(&new.scans, id, true);
            self.add_buckets(&new.buckets);
            if let Some(error) = error {
                self.invalid_cells.insert(id.into(), error);
            }
        }
    }

    fn update_root(
        &mut self,
        id: &str,
        previous: Option<&Arc<Value>>,
        next: Option<&Arc<Value>>,
        record_depth_valid: bool,
    ) {
        if (previous.is_none() && next.is_none())
            || previous
                .zip(next)
                .is_some_and(|(old, new)| Arc::ptr_eq(old, new))
        {
            return;
        }
        self.shape = crate::consensus::next_version();
        self.invalid_roots.remove(id);
        if let Some(value) = next {
            let depth = if record_depth_valid {
                Ok(())
            } else {
                depth(&value["args"], 0, "INPUT_INVALID")
            };
            let reference = depth
                .and_then(|()| Reference::parse(value, "stored root"))
                .and_then(|reference| {
                    if reference.root_id() != id {
                        return Err(EngineError::new(
                            "INPUT_INVALID",
                            "Malformed stored root identity",
                        ));
                    }
                    Ok(reference)
                });
            if let Err(error) = reference {
                self.invalid_roots.insert(id.into(), error);
            }
        }
    }
}

fn validate_cell(
    id: &str,
    value: &Value,
    previous: Option<&Arc<Value>>,
    previous_valid: bool,
) -> EngineResult<()> {
    record(value, "stored cell", "INPUT_INVALID")?;
    let name = string(&value["name"], "stored cell name", "INPUT_INVALID")?;
    let same_identity = previous_valid
        && previous.is_some_and(|old| old["name"] == value["name"] && old["args"] == value["args"]);
    if value.get("args").is_none() || (!same_identity && cell_id(name, &value["args"]) != id) {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Malformed stored cell identity",
        ));
    }
    let outcome = &value["outcome"];
    record(outcome, "stored cell outcome", "INPUT_INVALID")?;
    if outcome["ok"] == true {
        if outcome.get("value").is_none() {
            return Err(EngineError::new(
                "INPUT_INVALID",
                "Missing stored cell value",
            ));
        }
    } else if outcome["ok"] == false {
        record(&outcome["error"], "stored cell error", "INPUT_INVALID")?;
        string(
            &outcome["error"]["code"],
            "stored error code",
            "INPUT_INVALID",
        )?;
        string(
            &outcome["error"]["message"],
            "stored error message",
            "INPUT_INVALID",
        )?;
    } else {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Malformed stored cell outcome",
        ));
    }
    if value["deps"].as_array().is_none_or(|deps| {
        deps.iter().any(|dep| {
            dep.as_str().is_none_or(|dep| {
                windows::Window::is_dependency(dep) && windows::Window::parse(dep).is_none()
            })
        })
    }) {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Malformed stored cell dependencies",
        ));
    }
    Ok(())
}
