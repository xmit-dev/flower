//! Ordered scalar index ranges. Cursors continue against the current snapshot;
//! they do not pin historical data. Index-wide dependencies cover phantoms.
use super::*;
use indexes::IndexSpec;
use std::ops::Bound;
use windows::Window;

pub(super) fn prefix(collection: &str, fields: &[String]) -> String {
    format!(
        "ordered-entry:{}:",
        canonical_json(&json!([collection, fields]))
    )
}
pub(super) fn dependency(collection: &str, fields: &[String]) -> String {
    format!(
        "index-range:{}",
        canonical_json(&json!([collection, fields]))
    )
}
// Ordered positions are ASCII, so bytes and UTF-16 units compare alike, in
// the order of the values. Each component delimits itself: null is "0",
// false "10", true "11", a number "2" and its ordered bits in base-64
// `DIGITS` without trailing zero digits, then a space, and a string "3" and
// its text, then a space. Text keeps printable ASCII from '"' to '}' as it
// is; a UTF-16 unit up to '!' becomes "!" and one of "@" to "a", and one from
// '~' on becomes "~" and four hex digits. Nothing that follows a whole
// component reaches "~", which range bounds rely on. The source key follows a
// ":" as text without the space.
const TERMINATOR: char = ' ';
const DIGITS: &[u8; 64] = b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

fn text_units(out: &mut String, text: &str) {
    for unit in text.encode_utf16() {
        match unit {
            0..=0x21 => {
                out.push('!');
                out.push(char::from(b'@' + unit as u8));
            }
            0x22..=0x7d => out.push(char::from(unit as u8)),
            _ => {
                use std::fmt::Write;
                write!(out, "~{unit:04x}").expect("String writes");
            }
        }
    }
}
pub(super) fn text_restored(encoded: &str) -> Option<String> {
    let mut units = Vec::with_capacity(encoded.len());
    let mut bytes = encoded.as_bytes().iter();
    while let Some(byte) = bytes.next() {
        units.push(match byte {
            b'!' => u16::from(
                bytes
                    .next()?
                    .checked_sub(b'@')
                    .filter(|unit| *unit <= 0x21)?,
            ),
            b'~' => {
                let hex = [
                    *bytes.next()?,
                    *bytes.next()?,
                    *bytes.next()?,
                    *bytes.next()?,
                ];
                u16::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?
            }
            0x22..=0x7d => u16::from(*byte),
            _ => return None,
        });
    }
    String::from_utf16(&units).ok()
}
fn text(out: &mut String, text: &str) {
    out.push('3');
    text_units(out, text);
    out.push(TERMINATOR);
}
fn number(out: &mut String, number: f64) {
    let bits = (if number == 0.0 { 0.0 } else { number }).to_bits();
    let ordered = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    // Eleven digits hold the 64 bits and two zero bits.
    let wide = u128::from(ordered) << 2;
    let digits: Vec<u8> = (0..11)
        .map(|digit| DIGITS[((wide >> (60 - 6 * digit)) & 63) as usize])
        .collect();
    let length = digits
        .iter()
        .rposition(|digit| *digit != DIGITS[0])
        .map_or(0, |last| last + 1);
    out.push('2');
    out.push_str(std::str::from_utf8(&digits[..length]).expect("ASCII digits"));
    out.push(TERMINATOR);
}
fn scalar_into(out: &mut String, value: &Value) -> EngineResult<()> {
    match value {
        Value::Null => out.push('0'),
        Value::Bool(false) => out.push_str("10"),
        Value::Bool(true) => out.push_str("11"),
        Value::Number(value) => {
            let value = value.as_f64().filter(|n| n.is_finite()).ok_or_else(|| {
                EngineError::new("INVALID_REFERENCE", "Range numbers must be finite")
            })?;
            number(out, value);
        }
        Value::String(value) => text(out, value),
        _ => {
            return Err(EngineError::new(
                "INVALID_REFERENCE",
                "Ordered index components must be null, boolean, finite number or string",
            ));
        }
    }
    Ok(())
}
fn scalar(value: &Value) -> EngineResult<String> {
    let mut out = String::new();
    scalar_into(&mut out, value)?;
    Ok(out)
}
/// The length of the component that starts `position`.
pub(super) fn component_len(position: &str) -> Option<usize> {
    match position.as_bytes() {
        [b'0', ..] => Some(1),
        [b'1', b'0' | b'1', ..] => Some(2),
        [b'2' | b'3', ..] => Some(position.find(TERMINATOR)? + 1),
        _ => None,
    }
}
/// A row's ordered-entry ID after its index prefix: its scalar fields, or its
/// key for a source-key scan (`fields` is `None`), then its key.
pub(super) fn position(fields: Option<&[String]>, key: &str, value: &Value) -> Option<String> {
    let mut encoded = String::with_capacity(2 * key.len() + 16);
    match fields {
        None => text(&mut encoded, key),
        Some(fields) => {
            let object = value.as_object()?;
            for field in fields {
                scalar_into(&mut encoded, object.get(field)?).ok()?;
            }
        }
    }
    encoded.push(':');
    text_units(&mut encoded, key);
    Some(encoded)
}
/// What the positions of rows whose `fields` fields equal `value` start
/// with, when every component is a scalar: equality lookups of such values
/// read ordered entries, and no equality entries hold them.
pub(super) fn equality_position(fields: usize, value: &Value) -> Option<String> {
    let mut encoded = String::new();
    if fields == 1 {
        scalar_into(&mut encoded, value).ok()?;
    } else {
        for value in value.as_array().filter(|values| values.len() == fields)? {
            scalar_into(&mut encoded, value).ok()?;
        }
    }
    encoded.push(':');
    Some(encoded)
}
/// The source key of a position of `components` components.
pub(super) fn position_key(position: &str, components: usize) -> Option<String> {
    let mut rest = position;
    for _ in 0..components {
        rest = &rest[component_len(rest)?..];
    }
    text_restored(rest.strip_prefix(':')?)
}
pub(super) fn entry(spec: &IndexSpec, key: &str, value: &Value) -> Option<String> {
    let mut encoded = prefix(&spec.collection, &spec.fields);
    encoded.push_str(&position(Some(&spec.fields), key, value)?);
    Some(encoded)
}

/// Rows and what determines them.
pub(super) struct Scanned {
    pub rows: Value,
    pub dependency: Dependency,
}

pub(super) enum Dependency {
    /// No data can change the rows.
    None,
    Window(Window),
    /// The whole index or collection, when no window applies.
    Marker(String),
}

impl Dependency {
    pub(super) fn id(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::Window(window) => Some(window.dependency()),
            Self::Marker(marker) => Some(marker.clone()),
        }
    }
}

pub(super) struct RangeQuery {
    collection: String,
    fields: Vec<String>,
    lower: String,
    upper: String,
    after: Option<String>,
    reverse: bool,
    limit: usize,
    scope: String,
    offset: usize,
    scan: bool,
    source_keys: bool,
}
impl RangeQuery {
    pub(super) fn parse_scan(reference: &Value, options: &Value) -> EngineResult<Self> {
        let collection = reference_name(reference, "collection")?;
        let options = record(options, "scan options", "INVALID_REFERENCE")?;
        if options.keys().any(|key| {
            ![
                "index", "prefix", "gt", "gte", "lt", "lte", "limit", "offset", "reverse",
            ]
            .contains(&key.as_str())
        }) {
            return Err(EngineError::new("INVALID_REFERENCE", "Unknown scan option"));
        }
        let integer = |name: &str, default: usize| -> EngineResult<usize> {
            match options.get(name) {
                None => Ok(default),
                Some(value) => value
                    .as_f64()
                    .filter(|n| {
                        n.is_finite()
                            && *n >= 0.0
                            && *n <= 9_007_199_254_740_991.0
                            && n.fract() == 0.0
                    })
                    .and_then(|n| usize::try_from(n as u64).ok())
                    .ok_or_else(|| {
                        EngineError::new(
                            "INVALID_REFERENCE",
                            format!("Scan {name} must be a nonnegative safe integer"),
                        )
                    }),
            }
        };
        let limit = integer("limit", usize::MAX)?;
        let offset = integer("offset", 0)?;
        let source_keys = !options.contains_key("index");
        let fields = match options.get("index") {
            Some(index) => {
                let index = string(index, "scan index", "INVALID_REFERENCE")?;
                reference
                    .get("indexes")
                    .and_then(Value::as_object)
                    .and_then(|indexes| indexes.get(index))
                    .cloned()
                    .ok_or_else(|| EngineError::new("INVALID_REFERENCE", "Unknown scan index"))?
            }
            // A key scan uses the same scalar encoder as an implicit single-field
            // index, but never selects a persisted application index.
            None => json!(["$key"]),
        };
        if source_keys {
            let prefix = options.get("prefix").and_then(Value::as_array);
            if prefix.is_some_and(|parts| parts.iter().any(|part| !part.is_string()))
                || ["gt", "gte", "lt", "lte"]
                    .iter()
                    .any(|key| options.get(*key).is_some_and(|value| !value.is_string()))
            {
                return Err(EngineError::new(
                    "INVALID_REFERENCE",
                    "Source key constraints must be strings",
                ));
            }
        }
        let mut bounds = options.clone();
        bounds.remove("index");
        bounds.remove("offset");
        // Reuse the range validator, including its compound prefix rules. Scan's
        // optional/zero limit is restored after validating the range constraints.
        bounds.insert("limit".into(), json!(1));
        let mut query = Self::parse(&json!({
            "kind":"range", "collection":collection, "fields":fields, "options":bounds,
        }))?;
        query.limit = limit;
        query.offset = offset;
        query.scan = true;
        query.source_keys = source_keys;
        Ok(query)
    }

    pub(super) fn parse(value: &Value) -> EngineResult<Self> {
        record(value, "range reference", "INVALID_REFERENCE")?;
        if value["kind"] != "range"
            || value
                .as_object()
                .unwrap()
                .keys()
                .any(|k| !["kind", "collection", "fields", "options"].contains(&k.as_str()))
        {
            return Err(EngineError::new(
                "INVALID_REFERENCE",
                "Invalid range reference",
            ));
        }
        let collection = string(
            &value["collection"],
            "range collection",
            "INVALID_REFERENCE",
        )?
        .to_owned();
        let fields = value["fields"]
            .as_array()
            .and_then(|v| {
                v.iter()
                    .map(|v| v.as_str().filter(|v| !v.is_empty()).map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
            })
            .filter(|v| !v.is_empty() && v.iter().collect::<HashSet<_>>().len() == v.len())
            .ok_or_else(|| {
                EngineError::new(
                    "INVALID_REFERENCE",
                    "Range fields must be distinct nonempty strings",
                )
            })?;
        let options = record(&value["options"], "range options", "INVALID_REFERENCE")?;
        if options.keys().any(|k| {
            ![
                "prefix", "gt", "gte", "lt", "lte", "limit", "after", "reverse",
            ]
            .contains(&k.as_str())
        }) {
            return Err(EngineError::new(
                "INVALID_REFERENCE",
                "Unknown range option",
            ));
        }
        let limit = options
            .get("limit")
            .and_then(Value::as_u64)
            .filter(|n| *n > 0 && *n <= 9_007_199_254_740_991)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                EngineError::new(
                    "INVALID_REFERENCE",
                    "Range limit must be a positive safe integer",
                )
            })?;
        let parts = match options.get("prefix") {
            None => &[][..],
            Some(Value::Array(v)) => v.as_slice(),
            _ => {
                return Err(EngineError::new(
                    "INVALID_REFERENCE",
                    "Range prefix must be an array",
                ));
            }
        };
        if parts.len() > fields.len()
            || (parts.len() == fields.len()
                && ["gt", "gte", "lt", "lte"]
                    .iter()
                    .any(|key| options.contains_key(*key)))
            || (options.contains_key("gt") && options.contains_key("gte"))
            || (options.contains_key("lt") && options.contains_key("lte"))
        {
            return Err(EngineError::new(
                "INVALID_REFERENCE",
                "Invalid range prefix or overlapping bounds",
            ));
        }
        let mut base = prefix(&collection, &fields);
        for part in parts {
            base.push_str(&scalar(part)?);
        }
        let mut lower = base.clone();
        let mut upper = format!("{base}~");
        if let Some(value) = options.get("gte") {
            lower.push_str(&scalar(value)?);
        }
        if let Some(value) = options.get("gt") {
            lower.push_str(&scalar(value)?);
            lower.push('~');
        }
        if let Some(value) = options.get("lt") {
            upper = format!("{base}{}", scalar(value)?);
        }
        if let Some(value) = options.get("lte") {
            upper = format!("{base}{}~", scalar(value)?);
        }
        let reverse = match options.get("reverse") {
            None => false,
            Some(Value::Bool(v)) => *v,
            _ => {
                return Err(EngineError::new(
                    "INVALID_REFERENCE",
                    "reverse must be boolean",
                ));
            }
        };
        let scope = canonical_json(&json!([collection, fields, lower, upper, reverse]));
        let after = match options.get("after") {
            None => None,
            Some(Value::String(encoded)) => {
                let cursor: Value = serde_json::from_str(encoded)
                    .map_err(|_| EngineError::new("INVALID_REFERENCE", "Invalid range cursor"))?;
                if cursor["version"] != 1
                    || cursor["scope"] != scope
                    || cursor.as_object().is_none_or(|v| v.len() != 3)
                {
                    return Err(EngineError::new(
                        "INVALID_REFERENCE",
                        "Cursor belongs to another range",
                    ));
                }
                let last =
                    string(&cursor["last"], "cursor position", "INVALID_REFERENCE")?.to_owned();
                if last < lower || last >= upper {
                    return Err(EngineError::new(
                        "INVALID_REFERENCE",
                        "Cursor is outside the range",
                    ));
                }
                Some(last)
            }
            _ => {
                return Err(EngineError::new(
                    "INVALID_REFERENCE",
                    "Range cursor must be a string",
                ));
            }
        };
        Ok(Self {
            collection,
            fields,
            lower,
            upper,
            after,
            reverse,
            limit,
            scope,
            offset: 0,
            scan: false,
            source_keys: false,
        })
    }
    fn entry(&self, spec: &IndexSpec, key: &str, value: &Value) -> Option<String> {
        if self.source_keys {
            let mut encoded = prefix(&self.collection, &self.fields);
            encoded.push_str(&position(None, key, value)?);
            Some(encoded)
        } else {
            entry(spec, key, value)
        }
    }

    /// The part of the index this result depends on. A result that examined
    /// its whole offset, limit and lookahead cannot change beyond the last row
    /// examined; rows skipped by the offset contribute their positions only.
    fn window(
        &self,
        examined: Option<&str>,
        full: bool,
        returned: Option<(&str, &str)>,
    ) -> Option<Option<Window>> {
        let prefix = prefix(&self.collection, &self.fields);
        let suffix = |id: &str| id.strip_prefix(prefix.as_str()).map(str::to_owned);
        let successor = |id: &str| suffix(id).map(|id| id + "\0");
        let (mut lower, mut upper) = (suffix(&self.lower)?, suffix(&self.upper)?);
        if let Some(after) = &self.after {
            if self.reverse {
                upper = suffix(after)?;
            } else {
                lower = successor(after)?;
            }
        }
        if full && let Some(examined) = examined {
            if self.reverse {
                lower = suffix(examined)?;
            } else {
                upper = successor(examined)?;
            }
        }
        if lower >= upper {
            // An empty range always yields the same empty result.
            return Some(None);
        }
        let values = match returned {
            Some((first, last)) => Some((suffix(first)?, successor(last)?)),
            None => None,
        };
        Some(Some(Window {
            collection: self.collection.clone(),
            fields: (!self.source_keys).then(|| self.fields.clone()),
            lower,
            upper,
            values,
        }))
    }
    fn includes(&self, id: &str) -> bool {
        id >= self.lower.as_str()
            && id < self.upper.as_str()
            && self.after.as_ref().is_none_or(|after| {
                if self.reverse {
                    id < after.as_str()
                } else {
                    id > after.as_str()
                }
            })
    }
    pub(super) fn dependency(&self, engine: &Engine<'_>) -> String {
        if self.indexed(engine) {
            dependency(&self.collection, &self.fields)
        } else {
            collection_id(&self.collection)
        }
    }
    fn indexed(&self, engine: &Engine<'_>) -> bool {
        !self.source_keys
            && engine
                .schema
                .indexes
                .iter()
                .any(|i| i.collection == self.collection && i.fields == self.fields)
    }
}

impl Engine<'_> {
    /// Certificates validate against a snapshot, not a write. Key order
    /// changes only with the collection's keys, and undeclared field order
    /// with any row; a declared index stamps just its window's entries.
    /// Returned rows are stamped individually either way.
    /// A range page or scan. With `caller`, a method's, rows the caller may
    /// not read are skipped before limit, offset and cursor apply, so pages
    /// stay full and a cursor never points at a hidden row.
    pub(super) fn range_rows(&mut self, query: &RangeQuery, caller: bool) -> EngineResult<Scanned> {
        let access = if caller {
            self.read_access(&query.collection)
        } else {
            None
        };
        let indexed = query.indexed(self);
        if query.source_keys {
            let collection = collection_id(&query.collection);
            self.marker_read(dependencies::keys_marker(&collection).expect("collection marker"));
        } else if !indexed {
            self.marker_read(collection_id(&query.collection));
        }
        let scanned = self.scan_rows(query, access.as_deref());
        if indexed {
            match scanned.as_ref().map(|scanned| &scanned.dependency) {
                Ok(Dependency::None) => {}
                Ok(Dependency::Window(window)) => {
                    let prefix = prefix(&query.collection, &query.fields);
                    self.window_read(
                        query.dependency(self),
                        &format!("{prefix}{}", window.lower),
                        &format!("{prefix}{}", window.upper),
                    );
                }
                Ok(Dependency::Marker(_)) | Err(_) => self.marker_read(query.dependency(self)),
            }
        }
        scanned
    }

    fn scan_rows(
        &mut self,
        query: &RangeQuery,
        access: Option<&access::Access>,
    ) -> EngineResult<Scanned> {
        // A row found through an index field the caller can't read stays hidden.
        let selecting: &[String] = if query.source_keys {
            &[]
        } else {
            &query.fields
        };
        // Rows the caller's `readable` rules look up, recorded as read below.
        let lookup = access::RowLookup::new(self);
        let visible = |key: &str, value: &Value| {
            access.is_none_or(|access| access.visible(key, value, selecting, &lookup))
        };
        if query.lower >= query.upper || query.limit == 0 {
            return Ok(Scanned {
                rows: if query.scan {
                    json!([])
                } else {
                    json!({"rows":[],"cursor":null})
                },
                dependency: Dependency::None,
            });
        }
        let spec = IndexSpec {
            collection: query.collection.clone(),
            fields: query.fields.clone(),
        };
        // A page reads one row past its limit to decide whether it continues.
        let selected_limit = query.limit.saturating_add(usize::from(!query.scan));
        let keep = query.offset.saturating_add(selected_limit);
        let mut found = BTreeMap::<String, (String, Arc<Value>)>::new();
        let mut bytes = 0usize;
        let retain = |found: &mut BTreeMap<String, (String, Arc<Value>)>,
                      bytes: &mut usize,
                      id: String,
                      key: String,
                      value: Arc<Value>| {
            if let Some((old_key, _)) = found.insert(id.clone(), (key.clone(), value)) {
                *bytes = bytes.saturating_sub(192 + id.len() + old_key.len());
            }
            *bytes = bytes.saturating_add(192 + id.len() + key.len());
            if found.len() > keep {
                let removed = if query.reverse {
                    found.pop_first()
                } else {
                    found.pop_last()
                };
                if let Some((id, (key, _))) = removed {
                    *bytes = bytes.saturating_sub(192 + id.len() + key.len());
                }
            }
        };
        // Overlay mutations before walking the durable index. Skipped old entries
        // and inserted positions preserve read-your-writes without a full preview.
        let source_prefix = format!(
            "source:[{},",
            serde_json::to_string(&query.collection).unwrap()
        );
        for (position, (id, value)) in self.writes.range(source_prefix.clone()..).enumerate() {
            if !id.starts_with(&source_prefix) {
                break;
            }
            if position % 64 == 0 {
                self.check_fatal()?;
            }
            if let Some(value) = value {
                let (_, key) = source_pair(id)?;
                if let Some(id) = query
                    .entry(&spec, &key, value)
                    .filter(|id| query.includes(id) && visible(&key, value))
                {
                    retain(&mut found, &mut bytes, id, key, value.clone());
                }
            }
            if self.total_retained_bytes().saturating_add(bytes) > self.retained_limit {
                return self.abort(
                    "EVALUATION_BUDGET",
                    "Range result exceeds Rust memory budget",
                );
            }
        }
        let indexed = query.indexed(self);
        if indexed {
            let snapshot = self.staged.clone();
            let lower = if !query.reverse {
                query
                    .after
                    .as_deref()
                    .map_or(Bound::Included(query.lower.as_str()), Bound::Excluded)
            } else {
                Bound::Included(query.lower.as_str())
            };
            let upper = if query.reverse {
                Bound::Excluded(query.after.as_deref().unwrap_or(&query.upper))
            } else {
                Bound::Excluded(query.upper.as_str())
            };
            let prefix = prefix(&query.collection, &query.fields);
            let range = snapshot.range_shared((lower, upper));
            let iterator: Box<dyn Iterator<Item = (&String, &Arc<Value>)>> = if query.reverse {
                Box::new(range.rev())
            } else {
                Box::new(range)
            };
            // Merge the sorted overlay with the durable walk before applying
            // offset. Skipped durable rows never occupy the result buffer.
            let pending = std::mem::take(&mut found);
            let pending: Box<dyn Iterator<Item = (String, (String, Arc<Value>))>> = if query.reverse
            {
                Box::new(pending.into_iter().rev())
            } else {
                Box::new(pending.into_iter())
            };
            let mut pending = pending.peekable();
            let mut stored = iterator.peekable();
            let mut skip = query.offset;
            let mut position = 0usize;
            loop {
                if position % 64 == 0 {
                    self.check_fatal()?;
                }
                position += 1;
                let take_pending = match (pending.peek(), stored.peek()) {
                    (Some((pending, _)), Some((stored, _))) => {
                        if query.reverse {
                            pending.as_str() >= stored.as_str()
                        } else {
                            pending.as_str() <= stored.as_str()
                        }
                    }
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => break,
                };
                let (id, key, value) = if take_pending {
                    let (id, (key, value)) = pending.next().expect("pending entry");
                    bytes = bytes.saturating_sub(192 + id.len() + key.len());
                    (id, key, value)
                } else {
                    let (id, _) = stored.next().expect("stored entry");
                    let key = id
                        .strip_prefix(prefix.as_str())
                        .and_then(|position| position_key(position, query.fields.len()))
                        .ok_or_else(|| {
                            EngineError::new("INPUT_INVALID", "Malformed ordered index entry")
                        })?;
                    let key = key.as_str();
                    let source = source_id(&query.collection, key);
                    if self.writes.contains_key(&source) {
                        continue;
                    }
                    let value = snapshot
                        .get_shared(&source)
                        .filter(|value| entry(&spec, key, value).as_ref() == Some(id))
                        .ok_or_else(|| {
                            EngineError::new(
                                "INPUT_INVALID",
                                "Ordered index entry does not match source",
                            )
                        })?;
                    (id.clone(), key.to_owned(), value.clone())
                };
                if !visible(&key, &value) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                bytes = bytes.saturating_add(192 + id.len() + key.len());
                found.insert(id, (key, value));
                if self.total_retained_bytes().saturating_add(bytes) > self.retained_limit {
                    return self.abort(
                        "EVALUATION_BUDGET",
                        "Range result exceeds Rust memory budget",
                    );
                }
                if found.len() == selected_limit {
                    break;
                }
            }
        } else {
            // Undeclared/dynamic helper collections stay correct. This fallback
            // scans native records, retaining only offset+limit (plus a cursor
            // lookahead for range pages); declare the index for seeks.
            let snapshot = self.staged.clone();
            for (position, (id, value)) in snapshot
                .range_shared((Bound::Included(source_prefix.as_str()), Bound::Unbounded))
                .enumerate()
            {
                if !id.starts_with(&source_prefix) {
                    break;
                }
                if position % 64 == 0 {
                    self.check_fatal()?;
                }
                if self.writes.contains_key(id) {
                    continue;
                }
                let (_, key) = source_pair(id)?;
                if let Some(id) = query
                    .entry(&spec, &key, value)
                    .filter(|id| query.includes(id) && visible(&key, value))
                {
                    retain(&mut found, &mut bytes, id, key, value.clone());
                }
                if self.total_retained_bytes().saturating_add(bytes) > self.retained_limit {
                    return self.abort(
                        "EVALUATION_BUDGET",
                        "Range result exceeds Rust memory budget",
                    );
                }
            }
        }
        // The indexed walk retained only rows after the offset.
        let full = found.len() == if indexed { selected_limit } else { keep };
        let examined = if query.reverse {
            found.first_key_value()
        } else {
            found.last_key_value()
        }
        .map(|(id, _)| id.clone());
        if !indexed {
            for _ in 0..query.offset.min(found.len()) {
                if query.reverse {
                    found.pop_last();
                } else {
                    found.pop_first();
                }
            }
        }
        let more = !query.scan && found.len() > query.limit;
        if more {
            if query.reverse {
                found.pop_first();
            } else {
                found.pop_last();
            }
        }
        let last = if query.reverse {
            found.first_key_value()
        } else {
            found.last_key_value()
        }
        .map(|(id, _)| id.clone());
        let cursor = if more {
            last.map(|last| canonical_json(&json!({"version":1,"scope":query.scope,"last":last})))
        } else {
            None
        };
        let looked_up = lookup.into_reads();
        self.record_lookups(looked_up)?;
        for (key, _) in found.values() {
            self.record_read(source_id(&query.collection, key));
        }
        let returned = found
            .first_key_value()
            .zip(found.last_key_value())
            .map(|((first, _), (last, _))| (first.as_str(), last.as_str()));
        let dependency = match query.window(examined.as_deref(), full, returned) {
            Some(Some(window)) => Dependency::Window(window),
            Some(None) => Dependency::None,
            None => Dependency::Marker(query.dependency(self)),
        };
        let rows: Vec<(String, Arc<Value>)> = if query.reverse {
            found.into_values().rev().collect()
        } else {
            found.into_values().collect()
        };
        let rows = match access {
            Some(access) => {
                let rows = self.looking_up(|lookup| {
                    rows.into_iter()
                        .map(|(key, value)| {
                            let shown = access.redact(&key, &value, lookup);
                            (key, shown)
                        })
                        .collect()
                })?;
                self.settle_access(access)?;
                rows
            }
            None => rows,
        };
        let rows = self.rows_for_host(rows)?;
        if query.scan {
            return Ok(Scanned { rows, dependency });
        }
        let page = json!({"rows":rows,"cursor":cursor});
        Ok(Scanned {
            rows: self.copy_for_host(&page)?,
            dependency,
        })
    }
}

#[cfg(test)]
mod encoding_tests {
    use super::*;
    use std::cmp::Ordering;

    fn values() -> Vec<Value> {
        let mut values = vec![Value::Null, json!(false), json!(true)];
        for number in [
            f64::MIN,
            -1e300,
            -2.5,
            -1.0,
            -f64::MIN_POSITIVE,
            -5e-324,
            -0.0,
            0.0,
            5e-324,
            f64::MIN_POSITIVE,
            0.1,
            1.0,
            2.0,
            3.0,
            1790403992895.0,
            9007199254740993.0,
            1e300,
            f64::MAX,
        ] {
            values.push(json!(number));
        }
        for text in [
            "",
            " ",
            "!",
            "\"",
            "#",
            "a",
            "a ",
            "a!",
            "ab",
            "b",
            "}",
            "~",
            "\u{7f}",
            "\u{0}",
            "\u{1}",
            "\n",
            "é",
            "\u{7ff}",
            "\u{d7ff}",
            "\u{e000}",
            "\u{ffff}",
            "\u{10000}",
            "😀",
            "\u{10ffff}",
            "a\u{0}",
            "a\u{0}b",
            "fcc3f0c1-32d7-4fca-9a38-347b607357b4",
        ] {
            values.push(json!(text));
        }
        values
    }

    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(false) => 1,
            Value::Bool(true) => 2,
            Value::Number(_) => 3,
            _ => 4,
        }
    }

    fn expected(a: &Value, b: &Value) -> Ordering {
        rank(a).cmp(&rank(b)).then_with(|| match (a, b) {
            (Value::Number(a), Value::Number(b)) => a
                .as_f64()
                .unwrap()
                .partial_cmp(&b.as_f64().unwrap())
                .unwrap(),
            (Value::String(a), Value::String(b)) => a.encode_utf16().cmp(b.encode_utf16()),
            _ => Ordering::Equal,
        })
    }

    #[test]
    fn positions_compare_as_their_values_and_delimit_themselves() {
        let values = values();
        for a in &values {
            let encoded = scalar(a).unwrap();
            assert!(encoded.is_ascii(), "{a} encodes as {encoded:?}");
            assert_eq!(component_len(&format!("{encoded}:k")), Some(encoded.len()));
            // A bound just past a whole component exceeds what can follow it.
            for next in ["0", "10", "11", "2V ", "3a ", ":"] {
                assert!(format!("{encoded}{next}") < format!("{encoded}~"));
            }
            for b in &values {
                let order = expected(a, b);
                assert_eq!(encoded.cmp(&scalar(b).unwrap()), order, "{a} vs {b}");
                // Tuples compare component by component, then by key.
                for (c, d) in [(a, b), (b, a)] {
                    let left = format!("{encoded}{}", scalar(c).unwrap());
                    let right = format!("{}{}", scalar(b).unwrap(), scalar(d).unwrap());
                    assert_eq!(left.cmp(&right), order.then_with(|| expected(c, d)));
                }
            }
        }
    }

    #[test]
    fn positions_name_their_keys() {
        let spec = IndexSpec {
            collection: "items".into(),
            fields: vec!["group".into(), "at".into()],
        };
        let keys: Vec<String> = values()
            .iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .chain(["[\"fcc3f0c1\",26]".into()])
            .collect();
        for key in &keys {
            let row = json!({"group": key, "at": 3});
            let position = super::position(Some(&spec.fields), key, &row).unwrap();
            assert!(position.is_ascii());
            assert_eq!(position_key(&position, 2).as_deref(), Some(key.as_str()));
            let scan = super::position(None, key, &row).unwrap();
            assert_eq!(position_key(&scan, 1).as_deref(), Some(key.as_str()));
            for other in &keys {
                let other_position = super::position(Some(&spec.fields), other, &row).unwrap();
                assert_eq!(
                    position.cmp(&other_position),
                    expected(&json!(key), &json!(other)),
                    "{key:?} vs {other:?}"
                );
            }
        }
        assert_eq!(position_key("3a :~d83", 1), None);
        assert_eq!(position_key("3a :!z", 1), None);
    }
}
