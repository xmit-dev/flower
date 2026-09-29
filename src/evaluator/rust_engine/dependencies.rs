//! Query certificates refer to write versions, never old payloads or
//! allocations, so values need not stay in memory to be checked.
use super::*;
use std::ops::Bound;
use std::sync::Mutex;

#[derive(Debug)]
enum Stamp {
    Record(Option<u64>),
    Marker(Option<u64>),
    /// A declared index window: unchanged while the index marker is, or while
    /// its entries keep the same versions. A walk that finds them unchanged
    /// adopts the newer marker, so later checks skip the walk.
    Window {
        index: String,
        marker: Mutex<Option<u64>>,
        lower: String,
        upper: String,
        entries: Vec<u64>,
    },
}

impl Clone for Stamp {
    fn clone(&self) -> Self {
        match self {
            Self::Record(record) => Self::Record(record.clone()),
            Self::Marker(marker) => Self::Marker(marker.clone()),
            Self::Window {
                index,
                marker,
                lower,
                upper,
                entries,
            } => Self::Window {
                index: index.clone(),
                marker: Mutex::new(marker.lock().expect("window marker lock").clone()),
                lower: lower.clone(),
                upper: upper.clone(),
                entries: entries.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DependencyCertificate {
    checks: BTreeMap<String, Stamp>,
    bytes: usize,
}
impl DependencyCertificate {
    /// Validate only observed identities against this request's chosen snapshot.
    pub fn valid(&self, data: &Records) -> bool {
        data.reactive().cacheable() && self.valid_records(data)
    }
    fn valid_records(&self, data: &Records) -> bool {
        data.has_valid_depth()
            && data.has_valid_source_ids()
            && data.has_valid_graph_pointer()
            && data.reactive().validate().is_ok()
            && data
                .get("clock")
                .is_none_or(|value| safe_time(value, "Stored clock").is_ok())
            && self.checks.iter().all(|(id, stamp)| match stamp {
                Stamp::Record(expected) => *expected == data.version(id),
                Stamp::Marker(expected) => *expected == data.generation(id),
                Stamp::Window {
                    index,
                    marker,
                    lower,
                    upper,
                    entries,
                } => {
                    let current = data.generation(index);
                    let mut marker = marker.lock().expect("window marker lock");
                    if *marker == current {
                        return true;
                    }
                    let mut found = data.range_versions::<(Bound<&str>, Bound<&str>)>((
                        Bound::Included(lower),
                        Bound::Excluded(upper),
                    ));
                    let unchanged = entries
                        .iter()
                        .all(|expected| found.next().is_some_and(|actual| *expected == actual))
                        && found.next().is_none();
                    if unchanged {
                        *marker = current;
                    }
                    unchanged
                }
            })
    }
    pub fn allocation_cost(&self) -> usize {
        self.bytes
    }
    /// The records and graph markers this certificate observed.
    #[cfg(test)]
    pub(crate) fn observed(&self) -> Vec<&str> {
        self.checks.keys().map(String::as_str).collect()
    }
    /// A certificate that observed these records, for tests of who watches them.
    #[cfg(test)]
    pub(crate) fn of_records(keys: &[&str]) -> Self {
        Self {
            checks: keys
                .iter()
                .map(|key| (key.to_string(), Stamp::Record(None)))
                .collect(),
            bytes: 0,
        }
    }
    /// What only a write can invalidate, as `touched` names writes. The
    /// checks of the whole state (depths, the graph pointer, clock readers)
    /// change only through keys a reader must treat as touching everything.
    pub fn observations(&self) -> impl Iterator<Item = Observation<'_>> {
        self.checks.iter().map(|(id, stamp)| match stamp {
            Stamp::Record(_) | Stamp::Marker(_) => Observation::Key(id),
            Stamp::Window {
                index,
                lower,
                upper,
                ..
            } => Observation::Range {
                marker: index,
                lower,
                upper,
            },
        })
    }
}

/// One thing a certificate stays valid for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observation<'a> {
    /// A record by its logical key, or a membership marker.
    Key(&'a str),
    /// The stored keys in `[lower, upper)`, all of which move `marker`.
    Range {
        marker: &'a str,
        lower: &'a str,
        upper: &'a str,
    },
}

/// Whether a write of the stored `key` can invalidate any certificate:
/// code, policy and configuration, and what the checks of the whole state
/// read. Records, index entries, the graph, and bookkeeping read as records
/// are not; neither is the clock, which a certified result never read.
pub fn touches_everything(key: &str) -> bool {
    let key = key
        .strip_prefix("graph:")
        .and_then(|rest| rest.split_once(':'))
        .filter(|(generation, _)| Records::valid_graph_generation(generation))
        .map_or(key, |(_, logical)| logical);
    if let Some(edge) = key.strip_prefix("reader:") {
        // A new clock reader makes every certified result time-dependent.
        return edge.starts_with("clock\0");
    }
    const DATA: [&str; 8] = [
        "source:",
        "index-entry:",
        "ordered-entry:",
        "cell:",
        "root:",
        "height:",
        "transaction:",
        "$flower.",
    ];
    key != "clock" && !DATA.iter().any(|prefix| key.starts_with(prefix))
}

/// The dependency keys a write of the stored `key` can invalidate: the
/// record by its stored and logical keys, and the membership markers it
/// moves, as `update_memberships` moves them. A range depends on the write
/// when its marker is among these and it holds `key`.
pub fn touched(key: &str, mut touch: impl FnMut(&str)) {
    touch(key);
    // The key alone cannot say whether the history identity moved.
    if key == crate::consensus::retention::KEY {
        touch(crate::consensus::HISTORY_MARKER);
    }
    if let Some((generation, logical)) = key
        .strip_prefix("graph:")
        .and_then(|rest| rest.split_once(':'))
        && Records::valid_graph_generation(generation)
    {
        touch(logical);
    }
    if let Some(marker) = membership_marker(key) {
        if let Some(keys) = keys_marker(&marker) {
            touch(&keys);
        }
        touch(&marker);
    }
}

/// An optimistic mutation is reusable only against the same observed values,
/// write bases, graph dependency shape and code/policy, at its fixed clock.
#[derive(Clone, Debug)]
pub struct MutationCertificate {
    reads: DependencyCertificate,
    graph: u64,
    now: u64,
}
impl MutationCertificate {
    pub(super) fn new(reads: DependencyCertificate, data: &Records, now: u64) -> Self {
        Self {
            reads,
            graph: data.reactive().shape,
            now,
        }
    }
    pub fn valid(&self, data: &Records) -> bool {
        self.reads.valid_records(data)
            && self.graph == data.reactive().shape
            && data
                .get("clock")
                .map_or(Ok(0), |clock| safe_time(clock, "Stored clock"))
                .is_ok_and(|clock| clock <= self.now)
    }
    pub fn allocation_cost(&self) -> usize {
        self.reads.allocation_cost().saturating_add(64)
    }
    /// What the mutation read, as `touched` names writes.
    pub fn observations(&self) -> impl Iterator<Item = Observation<'_>> {
        self.reads.observations()
    }
    #[cfg(test)]
    pub(crate) fn observed(&self) -> Vec<&str> {
        self.reads.observed()
    }
}
impl Engine<'_> {
    fn certificate_stamp(&mut self, id: &str, marker: bool) {
        if !self.certifying && !self.speculative {
            return;
        }
        let Some(certificate) = self.certificate.as_ref() else {
            return;
        };
        if certificate.checks.contains_key(id) {
            return;
        }
        let cost = 160usize.saturating_add(id.len());
        if self.total_retained_bytes().saturating_add(cost) > self.retained_limit {
            // Stop tracking when the certificate itself would exceed budget.
            self.disable_certificate();
            return;
        }
        let stamp = if marker {
            Stamp::Marker(self.base.generation(id))
        } else {
            Stamp::Record(self.base.version(id))
        };
        self.retained_bytes = self.retained_bytes.saturating_add(cost);
        let certificate = self.certificate.as_mut().expect("checked certificate");
        certificate.bytes = certificate.bytes.saturating_add(cost);
        certificate.checks.insert(id.to_owned(), stamp);
    }
    /// Stamp a declared index window by the entries currently in it. Too
    /// many entries fall back to the index marker alone.
    pub(super) fn window_read(&mut self, marker: String, lower: &str, upper: &str) {
        if !self.certifying && !self.speculative {
            return;
        }
        let Some(certificate) = self.certificate.as_ref() else {
            return;
        };
        let id = format!("{WINDOW}{}", canonical_json(&json!([marker, lower, upper])));
        if certificate.checks.contains_key(&id) {
            return;
        }
        let cost = 192usize
            .saturating_add(id.len())
            .saturating_add(marker.len());
        // Each entry costs 8 bytes more: walk no further than the budget
        // leaves room for, and one more to know whether it overflows.
        let room = self
            .retained_limit
            .saturating_sub(self.total_retained_bytes().saturating_add(cost))
            / 8;
        let entries: Vec<u64> = self
            .base
            .range_versions::<(Bound<&str>, Bound<&str>)>((
                Bound::Included(lower),
                Bound::Excluded(upper),
            ))
            .take(room.saturating_add(1))
            .collect();
        if entries.len() > room {
            self.marker_read(marker);
            return;
        }
        let cost = cost.saturating_add(entries.len().saturating_mul(8));
        let stamp = Stamp::Window {
            marker: Mutex::new(self.base.generation(&marker)),
            index: marker,
            lower: lower.into(),
            upper: upper.into(),
            entries,
        };
        self.retained_bytes = self.retained_bytes.saturating_add(cost);
        let certificate = self.certificate.as_mut().expect("checked certificate");
        certificate.bytes = certificate.bytes.saturating_add(cost);
        certificate.checks.insert(id, stamp);
    }

    /// Neither reusable nor fully observed.
    fn uncertified(&mut self) {
        self.query_cacheable = false;
        self.certifying = false;
    }

    fn disable_certificate(&mut self) {
        if let Some(previous) = self.certificate.take() {
            self.retained_bytes = self.retained_bytes.saturating_sub(previous.bytes);
        }
    }
    pub(super) fn record_read(&mut self, id: impl AsRef<str>) {
        self.certificate_stamp(id.as_ref(), false);
    }
    pub(super) fn marker_read(&mut self, id: impl AsRef<str>) {
        let id = id.as_ref();
        if (!self.certifying && !self.speculative) || self.certificate.is_none() {
            return;
        }
        // Equality buckets have no markers of their own; stamp their entries.
        if let Some((marker, lower, upper)) = bucket_window(id) {
            self.window_read(marker, &lower, &upper);
            return;
        }
        self.certificate_stamp(id, true);
    }
    pub(super) fn derived_read(&mut self, id: String) {
        // A stored cell may read the clock without this query noticing: only
        // a graph without clock readers proves the read is time-independent.
        if !self.base.reactive().cacheable() {
            self.clock_polled = true;
        }
        if self.speculative {
            self.record_read(id);
            return;
        }
        if !self.certifying || self.certificate.is_none() {
            return;
        }
        let mut walk_bytes = 160usize.saturating_add(id.len());
        let mut pending = vec![id];
        let mut seen = HashSet::new();
        while let Some(id) = pending.pop() {
            if self.certificate.is_none() {
                return;
            }
            if self.total_retained_bytes().saturating_add(walk_bytes) > self.retained_limit {
                self.disable_certificate();
                return;
            }
            if seen.len().is_multiple_of(64) && self.check_fatal().is_err() {
                self.disable_certificate();
                return;
            }
            if !seen.insert(id.clone()) {
                continue;
            }
            if id == "clock" {
                self.uncertified();
                self.clock_polled = true;
                return;
            }
            if id == "managedKeys" {
                // As resolving a key directly: uncached, but already observed.
                self.query_cacheable = false;
            } else if id.starts_with("source:") {
                self.record_read(id);
            } else if let Some(window) = windows::Window::parse(&id) {
                // Snapshot validation cannot see which rows moved.
                self.marker_read(window.marker(&self.schema));
            } else if let Some((collection, fields)) = bucket_spec(&id)
                && !self.maintained_index(&collection, &fields)
            {
                // Only maintained indexes have bucket markers.
                self.marker_read(collection_id(&collection));
            } else if id.starts_with("collection:")
                || id.starts_with("index-bucket:")
                || id.starts_with("index-range:")
            {
                self.marker_read(id);
            } else if id.starts_with("cell:") {
                if self.base.get(&id).is_some() {
                    // The patch never rewrites an unchanged record, so the
                    // allocation survives until the outcome or its deps change.
                    self.record_read(id);
                } else if let Some(cell) = self.staged.get(&id) {
                    let Some(deps) = cell["deps"].as_array() else {
                        self.uncertified();
                        return;
                    };
                    for dep in deps {
                        let Some(dep) = dep.as_str() else {
                            self.uncertified();
                            return;
                        };
                        walk_bytes = walk_bytes.saturating_add(160).saturating_add(dep.len());
                        if self.total_retained_bytes().saturating_add(walk_bytes)
                            > self.retained_limit
                        {
                            self.disable_certificate();
                            return;
                        }
                        pending.push(dep.to_owned());
                    }
                } else {
                    self.uncertified();
                    return;
                }
            } else {
                self.uncertified();
                return;
            }
        }
    }

    pub(super) fn speculative_read(&mut self, id: &str) {
        // range_rows already stamped a scan's marker and returned rows, and
        // query_rows the collection of an undeclared bucket.
        if !self.speculative
            || id == "clock"
            || windows::Window::is_dependency(id)
            || bucket_spec(id)
                .is_some_and(|(collection, fields)| !self.maintained_index(&collection, &fields))
        {
            return;
        }
        if id.starts_with("collection:")
            || id.starts_with("index-bucket:")
            || id.starts_with("index-range:")
        {
            self.marker_read(id);
        } else {
            self.record_read(id);
        }
    }
}

const WINDOW: &str = "window:";

/// Split the first complete JSON component without interpreting user strings.
/// IDs are trusted generated keys; malformed IDs conservatively have no marker.
fn component(value: &str) -> Option<(&str, &str)> {
    let mut quoted = false;
    let mut escaped = false;
    let mut nesting = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b'[' | b'{' => nesting = nesting.checked_add(1)?,
            b']' | b'}' => nesting = nesting.checked_sub(1)?,
            b':' if nesting == 0 => return Some((&value[..index], &value[index + 1..])),
            _ => {}
        }
    }
    None
}
/// An equality bucket's entries and the marker of its whole index, which
/// changes whenever any bucket of that index gains or loses an entry. One
/// marker per distinct indexed value would grow with the data.
pub(super) fn bucket_window(id: &str) -> Option<(String, String, String)> {
    let (spec, bucket) = component(id.strip_prefix("index-bucket:")?)?;
    // Scalar values have ordered entries only, under the range's marker.
    if let Ok((collection, fields)) = serde_json::from_str::<(String, Vec<String>)>(spec)
        && let Ok(value) = serde_json::from_str::<Value>(bucket)
        && let Some(components) = ranges::equality_position(fields.len(), &value)
    {
        let prefix = ranges::prefix(&collection, &fields);
        let lower = format!("{prefix}{components}");
        let upper = format!("{};", &lower[..lower.len() - 1]);
        return Some((ranges::dependency(&collection, &fields), lower, upper));
    }
    Some((
        format!("index-entries:{spec}"),
        format!("index-entry:{spec}:{bucket}:"),
        format!("index-entry:{spec}:{bucket};"),
    ))
}

/// The JSON `[collection, fields]` text of an `index-bucket:` dependency.
pub(super) fn bucket_spec_text(id: &str) -> Option<&str> {
    Some(component(id.strip_prefix("index-bucket:")?)?.0)
}

/// The collection and fields of an `index-bucket:` dependency.
pub(super) fn bucket_spec(id: &str) -> Option<(String, Vec<String>)> {
    let (spec, _) = component(id.strip_prefix("index-bucket:")?)?;
    serde_json::from_str(spec).ok()
}

/// Changes only when a collection gains or loses a key: key-ordered scans
/// check the values of the rows they returned separately.
pub(super) fn keys_marker(collection_marker: &str) -> Option<String> {
    collection_marker
        .strip_prefix("collection:")
        .map(|collection| format!("keys:{collection}"))
}

pub(super) fn membership_marker(id: &str) -> Option<String> {
    if let Some(source) = id.strip_prefix("source:[") {
        // The first member is always a JSON string; use the same escape-safe
        // component scanner after replacing its delimiting comma virtually.
        let mut escaped = false;
        let bytes = source.as_bytes();
        if bytes.first() != Some(&b'"') {
            return None;
        }
        for index in 1..bytes.len() {
            if escaped {
                escaped = false;
            } else if bytes[index] == b'\\' {
                escaped = true;
            } else if bytes[index] == b'"' {
                return (bytes.get(index + 1) == Some(&b','))
                    .then(|| format!("collection:{}", &source[..=index]));
            }
        }
        None
    } else if let Some(encoded) = id.strip_prefix("index-entry:") {
        let (spec, tail) = component(encoded)?;
        component(tail)?;
        Some(format!("index-entries:{spec}"))
    } else if let Some(encoded) = id.strip_prefix("ordered-entry:") {
        let (spec, _) = component(encoded)?;
        Some(format!("index-range:{spec}"))
    } else {
        None
    }
}
