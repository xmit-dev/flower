//! Durable equality indexes live in the same replicated state as their sources.
use super::*;
use std::sync::{Mutex, OnceLock};

/// A schema record's validated typed form, with the memory an evaluation using it is charged.
struct CachedSchema {
    record: Arc<Value>,
    schema: Arc<Schema>,
    bytes: usize,
    /// `SchemaCache::clock` when last found, to evict the least recently used.
    used: u64,
}

/// The validated schemas of the schema records evaluations use, shared by
/// every worker thread and keyed by the record's address. Holding the
/// immutable record makes address reuse impossible; replacing or mutating a
/// record creates a different `Arc`, found again only if its contents equal a
/// kept record's, since parsing and validation depend on nothing else.
/// Evaluations of every database a server hosts (a directory and partitions
/// deploying one bundle, each with a staged deployment's shadow schema at
/// times) share the blocking pool's threads, which come and go, and the value
/// cache hands out a new `Arc` for a stored record each time it reads it
/// again, so one entry per thread parsed a schema again on most calls.
struct SchemaCache {
    entries: HashMap<usize, CachedSchema>,
    clock: u64,
}

/// Records nothing else holds any more go at the next lookup; this bounds
/// the ones still held, which a server hosting many databases may all need.
const SCHEMA_CACHE_ENTRIES: usize = 256;

static SCHEMA_CACHE: Mutex<Option<SchemaCache>> = Mutex::new(None);

impl SchemaCache {
    fn with<T>(run: impl FnOnce(&mut Self) -> T) -> T {
        let mut cache = SCHEMA_CACHE
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        run(cache.get_or_insert_with(|| Self {
            entries: HashMap::new(),
            clock: 0,
        }))
    }

    /// Let go of the records only this cache still holds: nothing can ask
    /// for them again, since their addresses stay taken until they go.
    fn prune(&mut self) {
        self.entries
            .retain(|_, entry| Arc::strong_count(&entry.record) > 1);
    }

    fn find(&mut self, record: &Arc<Value>) -> Option<(Arc<Schema>, usize)> {
        self.prune();
        self.clock += 1;
        let entry = self
            .entries
            .get_mut(&(Arc::as_ptr(record) as usize))
            .filter(|entry| Arc::ptr_eq(&entry.record, record))?;
        entry.used = self.clock;
        Some((entry.schema.clone(), entry.bytes))
    }

    /// The kept records charged `bytes`, the only ones that may equal a record charged `bytes`.
    fn alike(&self, bytes: usize) -> Vec<(Arc<Value>, Arc<Schema>)> {
        let mut alike = Vec::<(Arc<Value>, Arc<Schema>)>::new();
        for entry in self.entries.values().filter(|entry| entry.bytes == bytes) {
            // Records sharing a parse are equal: comparing with one is enough.
            if !alike
                .iter()
                .any(|(_, schema)| Arc::ptr_eq(schema, &entry.schema))
            {
                alike.push((entry.record.clone(), entry.schema.clone()));
            }
        }
        alike
    }

    /// Keep `schema` for `record`, or return what another thread kept for it meanwhile.
    fn keep(&mut self, record: &Arc<Value>, schema: Arc<Schema>, bytes: usize) -> Arc<Schema> {
        if let Some((kept, _)) = self.find(record) {
            return kept;
        }
        while self.entries.len() >= SCHEMA_CACHE_ENTRIES {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| *key)
                .expect("the cache is full");
            self.entries.remove(&oldest);
        }
        self.entries.insert(
            Arc::as_ptr(record) as usize,
            CachedSchema {
                record: record.clone(),
                schema: schema.clone(),
                bytes,
                used: self.clock,
            },
        );
        schema
    }
}

#[cfg(test)]
thread_local! {
    static PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many schema records this thread parsed and validated for `Schema::load_shared`.
#[cfg(test)]
fn schema_parses() -> usize {
    PARSES.get()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    #[serde(default)]
    pub aggregates: BTreeMap<String, IndexSpec>,
    /// The aggregates whose groups keep their accumulators across deployments while their version
    /// stays the same (`aggregate(…, { version })`), by definition name. The others rebuild every
    /// group a deployment reaches from all of its rows.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub aggregate_versions: BTreeMap<String, String>,
    /// Access policies by collection name, enforced on methods' reads and writes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub policies: BTreeMap<String, super::Policy>,
    /// Who may read each derived value from a method, by definition name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub derived_access: BTreeMap<String, super::Rule>,
    /// Foreign keys, checked at the end of every mutation: rows refer only to
    /// rows that exist. Each one held by fields has its index among `indexes`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<super::ReferenceSpec>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct IndexSpec {
    pub collection: String,
    pub fields: Vec<String>,
}

impl Schema {
    pub(super) fn load_shared(
        record: Option<&Arc<Value>>,
        available_bytes: usize,
    ) -> EngineResult<(Arc<Self>, usize)> {
        let Some(record) = record else {
            SchemaCache::with(SchemaCache::prune);
            static EMPTY: OnceLock<Arc<Schema>> = OnceLock::new();
            return Ok((EMPTY.get_or_init(|| Arc::new(Self::default())).clone(), 0));
        };
        let check_budget = |bytes: usize| {
            if bytes > available_bytes {
                Err(EngineError::new(
                    "EVALUATION_BUDGET",
                    "Index schema exceeds the Rust memory budget",
                ))
            } else {
                Ok(())
            }
        };
        if let Some((schema, bytes)) = SchemaCache::with(|cache| cache.find(record)) {
            check_budget(bytes)?;
            return Ok((schema, bytes));
        }
        // Preserve the existing conservative JSON + typed-schema charge,
        // including admission before cloning/deserializing on a miss.
        let bytes = allocation_cost(record).saturating_mul(2);
        check_budget(bytes)?;
        // Compare and parse outside the lock, so that other threads go on finding theirs.
        let alike = SchemaCache::with(|cache| cache.alike(bytes));
        if let Some((_, schema)) = alike.into_iter().find(|(kept, _)| kept == record) {
            return Ok((
                SchemaCache::with(|cache| cache.keep(record, schema, bytes)),
                bytes,
            ));
        }
        let schema = Arc::new(Self::load(Some(record))?);
        #[cfg(test)]
        PARSES.set(PARSES.get() + 1);
        let schema = SchemaCache::with(|cache| cache.keep(record, schema, bytes));
        Ok((schema, bytes))
    }

    pub(super) fn allocation_cost(&self) -> usize {
        let policies = self.policies.iter().fold(0usize, |bytes, (name, policy)| {
            bytes.saturating_add(128 + name.len() + policy.allocation_cost())
        });
        let policies = self
            .derived_access
            .iter()
            .fold(policies, |bytes, (name, rule)| {
                bytes.saturating_add(128 + name.len() + rule.allocation_cost())
            });
        let policies = self
            .aggregate_versions
            .iter()
            .fold(policies, |bytes, (name, version)| {
                bytes.saturating_add(128 + name.len() + version.len())
            });
        let policies = self.references.iter().fold(policies, |bytes, reference| {
            bytes.saturating_add(reference.allocation_cost())
        });
        policies.saturating_add(
            self.indexes.iter().chain(self.aggregates.values()).fold(
                self.aggregates
                    .keys()
                    .fold(0usize, |bytes, name| bytes.saturating_add(128 + name.len())),
                |bytes, index| {
                    index.fields.iter().fold(
                        bytes.saturating_add(192 + index.collection.len()),
                        |bytes, field| bytes.saturating_add(32 + field.len()),
                    )
                },
            ),
        )
    }

    pub(super) fn validate(mut self) -> EngineResult<Self> {
        for index in self.indexes.iter().chain(self.aggregates.values()) {
            if index.collection.is_empty()
                || index.fields.is_empty()
                || index.fields.iter().any(String::is_empty)
                || index.fields.iter().collect::<HashSet<_>>().len() != index.fields.len()
            {
                return Err(EngineError::new("INPUT_INVALID", "Malformed index schema"));
            }
        }
        self.indexes.sort();
        self.indexes.dedup();
        for (name, index) in &self.aggregates {
            if name.is_empty() || !self.indexes.contains(index) {
                return Err(EngineError::new(
                    "INPUT_INVALID",
                    "Aggregate index is not declared",
                ));
            }
        }
        for (name, version) in &self.aggregate_versions {
            if !self.aggregates.contains_key(name) || !valid_version(version) {
                return Err(EngineError::new(
                    "INPUT_INVALID",
                    "Malformed aggregate version",
                ));
            }
        }
        for (name, policy) in &self.policies {
            if name.is_empty() {
                return Err(EngineError::new("INPUT_INVALID", "Malformed access policy"));
            }
            policy.validate().map_err(|error| {
                EngineError::new("INPUT_INVALID", format!("Malformed access policy: {error}"))
            })?;
        }
        super::validate_targets(&self.policies).map_err(|error| {
            EngineError::new("INPUT_INVALID", format!("Malformed access policy: {error}"))
        })?;
        for (name, rule) in &self.derived_access {
            if name.is_empty() {
                return Err(EngineError::new("INPUT_INVALID", "Malformed access rule"));
            }
            rule.validate_derived().map_err(|error| {
                EngineError::new("INPUT_INVALID", format!("Malformed access rule: {error}"))
            })?;
        }
        super::validate_derived_targets(&self.derived_access, &self.policies).map_err(|error| {
            EngineError::new("INPUT_INVALID", format!("Malformed access rule: {error}"))
        })?;
        for reference in &self.references {
            reference.validate().map_err(|error| {
                EngineError::new("INPUT_INVALID", format!("Malformed reference: {error}"))
            })?;
            if reference
                .index()
                .is_some_and(|index| !self.indexes.contains(&index))
            {
                return Err(EngineError::new(
                    "INPUT_INVALID",
                    "A reference's fields need their index declared",
                ));
            }
        }
        self.references.sort();
        self.references.dedup();
        Ok(self)
    }
    pub(super) fn load(value: Option<&Value>) -> EngineResult<Self> {
        value
            .map(|value| {
                serde_json::from_value::<Self>(value.clone()).map_err(|error| {
                    EngineError::new("INPUT_INVALID", format!("Invalid stored schema: {error}"))
                })
            })
            .transpose()?
            .unwrap_or_default()
            .validate()
    }
    pub(super) fn empty(&self) -> bool {
        self.indexes.is_empty()
            && self.aggregates.is_empty()
            && self.aggregate_versions.is_empty()
            && self.policies.is_empty()
            && self.derived_access.is_empty()
            && self.references.is_empty()
    }
}

/// An aggregate's version: 1 to 128 bytes, no control characters.
pub fn valid_version(version: &str) -> bool {
    (1..=128).contains(&version.len()) && !version.chars().any(char::is_control)
}

#[derive(Clone)]
pub(super) struct SourceDelta {
    pub key: String,
    pub previous: Option<Arc<Value>>,
    pub value: Option<Arc<Value>>,
}

fn index_prefix(collection: &str, fields: &[String]) -> String {
    format!(
        "index-entry:{}:",
        canonical_json(&json!([collection, fields]))
    )
}
pub(super) fn bucket_prefix(collection: &str, fields: &[String], encoded: &str) -> String {
    format!("{}{encoded}:", index_prefix(collection, fields))
}
pub(super) fn bucket_id(collection: &str, fields: &[String], value: &Value) -> String {
    bucket_id_encoded(collection, fields, &canonical_json(value))
}
fn bucket_id_encoded(collection: &str, fields: &[String], encoded: &str) -> String {
    format!(
        "index-bucket:{}:{encoded}",
        canonical_json(&json!([collection, fields]))
    )
}
fn entry_id(spec: &IndexSpec, encoded: &str, key: &str) -> String {
    format!(
        "{}{}",
        bucket_prefix(&spec.collection, &spec.fields, encoded),
        serde_json::to_string(key).expect("source key encodes")
    )
}
/// A row's equality entry, for a value `encoded` that has no ordered entry:
/// equality lookups of scalar values read those instead.
fn equality_entry(
    spec: &IndexSpec,
    ordered: Option<&String>,
    encoded: Option<&String>,
    key: &str,
) -> Option<String> {
    match (ordered, encoded) {
        (None, Some(encoded)) => Some(entry_id(spec, encoded, key)),
        _ => None,
    }
}
/// What an index entry holds: nothing, since its ID names its source.
pub(super) fn entry_value() -> Value {
    Value::from(0)
}
impl IndexSpec {
    fn key(&self, value: &Value) -> Option<String> {
        let object = value.as_object()?;
        if self.fields.len() == 1 {
            return Some(canonical_json(object.get(&self.fields[0])?));
        }
        let values = self
            .fields
            .iter()
            .map(|field| object.get(field).cloned())
            .collect::<Option<Vec<_>>>()?;
        Some(canonical_json(&Value::Array(values)))
    }
}

pub(crate) fn staged_schema_bytes(schema: &Schema) -> usize {
    schema.allocation_cost()
}
pub(crate) fn staged_schema(value: Option<&Value>) -> EngineResult<Schema> {
    Schema::load(value)
}
pub(crate) fn staged_entries(
    spec: &IndexSpec,
    id: &str,
    value: &Value,
) -> EngineResult<BTreeMap<String, Value>> {
    let (collection, key) = source_pair(id)?;
    if collection != spec.collection {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Backfill source belongs to another collection",
        ));
    }
    let mut entries = BTreeMap::new();
    let ordered = ranges::entry(spec, &key, value);
    if let Some(entry) = equality_entry(spec, ordered.as_ref(), spec.key(value).as_ref(), &key) {
        entries.insert(entry, entry_value());
    }
    if let Some(ordered) = ordered {
        entries.insert(ordered, entry_value());
    }
    Ok(entries)
}
pub(crate) fn staged_prefixes(spec: &IndexSpec) -> [String; 2] {
    [
        index_prefix(&spec.collection, &spec.fields),
        ranges::prefix(&spec.collection, &spec.fields),
    ]
}

impl Engine<'_> {
    /// Whether writes maintain durable entries, and so bucket markers, for
    /// this field set: the current schema and, during a staged deployment,
    /// the other one.
    pub(super) fn maintained_index(&self, collection: &str, fields: &[String]) -> bool {
        self.schema
            .indexes
            .iter()
            .chain(
                self.shadow_schema
                    .iter()
                    .flat_map(|schema| schema.indexes.iter()),
            )
            .any(|index| index.collection == collection && index.fields == fields)
    }

    /// Derived equality queries depend on their bucket whether or not the
    /// index is declared. update_durable_indexes reports maintained buckets;
    /// report the others that some derivation reads, where the row leaves,
    /// enters or changes within them.
    pub(super) fn undeclared_bucket_changes(
        &self,
        collection: &str,
        previous: Option<&Value>,
        value: Option<&Value>,
        dependencies: &mut Vec<String>,
    ) {
        for fields in self.staged.reactive().bucket_fields(collection) {
            if self.maintained_index(collection, fields) {
                continue;
            }
            let spec = IndexSpec {
                collection: collection.into(),
                fields: fields.clone(),
            };
            let before = previous.and_then(|value| spec.key(value));
            let after = value.and_then(|value| spec.key(value));
            let buckets = if before == after {
                [before, None]
            } else {
                [before, after]
            };
            for encoded in buckets.into_iter().flatten() {
                dependencies.push(bucket_id_encoded(collection, fields, &encoded));
            }
        }
    }

    pub(super) fn has_index(&self, query: &Query) -> bool {
        self.schema
            .indexes
            .iter()
            .any(|index| index.collection == query.collection && index.fields == query.fields)
    }

    pub(super) fn install_schema(&mut self, staged: bool) -> EngineResult<bool> {
        let Some(schema) = self.deployment_schema.take() else {
            return Ok(false);
        };
        if self.schema.as_ref() == &schema {
            return Ok(false);
        }
        let snapshot = self.staged.clone();
        if !staged {
            for index in self.schema.indexes.clone() {
                if schema.indexes.contains(&index) {
                    continue;
                }
                for prefix in [
                    index_prefix(&index.collection, &index.fields),
                    ranges::prefix(&index.collection, &index.fields),
                ] {
                    for (id, _) in snapshot.range_shared((
                        std::ops::Bound::Included(prefix.as_str()),
                        std::ops::Bound::Unbounded,
                    )) {
                        if !id.starts_with(&prefix) {
                            break;
                        }
                        self.check_fatal()?;
                        self.remove_index_entry(id)?;
                    }
                }
            }
            for index in &schema.indexes {
                if self.schema.indexes.contains(index) {
                    continue;
                }
                let prefix = format!(
                    "source:[{},",
                    serde_json::to_string(&index.collection).expect("collection encodes")
                );
                for (id, value) in snapshot.range_shared((
                    std::ops::Bound::Included(prefix.as_str()),
                    std::ops::Bound::Unbounded,
                )) {
                    if !id.starts_with(&prefix) {
                        break;
                    }
                    self.check_fatal()?;
                    let (_, key) = source_pair(id)?;
                    let ordered = ranges::entry(index, &key, value);
                    let equality =
                        equality_entry(index, ordered.as_ref(), index.key(value).as_ref(), &key);
                    for entry in [ordered, equality].into_iter().flatten() {
                        self.put(entry, entry_value())?;
                    }
                }
            }
        }
        self.schema = Arc::new(schema);
        if self.schema.empty() {
            self.remove("schema");
        } else {
            self.put(
                "schema".into(),
                serde_json::to_value(self.schema.as_ref()).expect("schema encodes"),
            )?;
        }
        Ok(true)
    }

    fn remove_index_entry(&mut self, id: &str) -> EngineResult<()> {
        if !self.changed.contains(id) {
            self.trace_bytes = self
                .trace_bytes
                .saturating_add(256 + id.len().saturating_mul(3));
            if self.total_retained_bytes() > self.retained_limit {
                return self.abort(
                    "EVALUATION_BUDGET",
                    "Index deletion identities exceed the Rust memory budget",
                );
            }
        }
        self.remove(id);
        Ok(())
    }

    pub(super) fn update_durable_indexes(
        &mut self,
        collection: &str,
        key: &str,
        previous: Option<Arc<Value>>,
        value: Option<Arc<Value>>,
        dependencies: &mut Vec<String>,
    ) -> EngineResult<()> {
        let indexes: Vec<_> = self
            .schema
            .indexes
            .iter()
            .chain(
                self.shadow_schema
                    .iter()
                    .flat_map(|schema| schema.indexes.iter())
                    .filter(|index| !self.schema.indexes.contains(index)),
            )
            .filter(|index| index.collection == collection)
            .cloned()
            .collect();
        for index in indexes {
            self.check_fatal()?;
            let ordered_before = previous
                .as_deref()
                .and_then(|value| ranges::entry(&index, key, value));
            let ordered_after = value
                .as_deref()
                .and_then(|value| ranges::entry(&index, key, value));
            if ordered_before != ordered_after {
                if let Some(id) = &ordered_before {
                    self.remove_index_entry(id)?;
                }
                if let Some(id) = &ordered_after {
                    self.put(id.clone(), entry_value())?;
                }
            }
            if ordered_before.is_some() || ordered_after.is_some() {
                dependencies.push(ranges::dependency(collection, &index.fields));
            }
            let before = previous.as_deref().and_then(|value| index.key(value));
            let after = value.as_deref().and_then(|value| index.key(value));
            let entry_before =
                equality_entry(&index, ordered_before.as_ref(), before.as_ref(), key);
            let entry_after = equality_entry(&index, ordered_after.as_ref(), after.as_ref(), key);
            if entry_before != entry_after {
                if let Some(id) = &entry_before {
                    self.remove_index_entry(id)?;
                }
                if let Some(id) = entry_after {
                    self.put(id, entry_value())?;
                }
            }
            let needs_delta = self.schema.aggregates.values().any(|spec| spec == &index);
            let mut record =
                |encoded: &str, previous: Option<Arc<Value>>, value: Option<Arc<Value>>| {
                    let bucket = bucket_id_encoded(collection, &index.fields, encoded);
                    dependencies.push(bucket.clone());
                    self.trace_bytes = self
                        .trace_bytes
                        .saturating_add(256 + bucket.len() * 2 + key.len());
                    if self.total_retained_bytes() > self.retained_limit {
                        return self.abort(
                            "EVALUATION_BUDGET",
                            "Index deltas exceed the Rust memory budget",
                        );
                    }
                    if needs_delta {
                        self.preview
                            .index_changes
                            .entry(bucket)
                            .or_default()
                            .push(SourceDelta {
                                key: key.into(),
                                previous,
                                value,
                            });
                    }
                    Ok(())
                };
            if before == after {
                if let Some(encoded) = &before {
                    record(encoded, previous.clone(), value.clone())?;
                }
            } else {
                if let Some(encoded) = &before {
                    record(encoded, previous.clone(), None)?;
                }
                if let Some(encoded) = &after {
                    record(encoded, None, value.clone())?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn indexed_rows(
        &mut self,
        query: &Query,
    ) -> EngineResult<Vec<(String, Arc<Value>)>> {
        // Scalar values have ordered entries only, whose positions end in
        // their keys' text; the others have equality entries ending in JSON.
        let scalar = ranges::equality_position(query.fields.len(), &query.expected);
        let prefix = match &scalar {
            Some(components) => format!(
                "{}{components}",
                ranges::prefix(&query.collection, &query.fields)
            ),
            None => bucket_prefix(
                &query.collection,
                &query.fields,
                &canonical_json(&query.expected),
            ),
        };
        let key_of = |suffix: &str| match scalar {
            Some(_) => ranges::text_restored(suffix),
            None => serde_json::from_str::<String>(suffix).ok(),
        };
        let mut rows = BTreeMap::<Key, Arc<Value>>::new();
        let mut bytes = 0usize;
        for (position, (id, _)) in self
            .staged
            .range_shared((
                std::ops::Bound::Included(prefix.as_str()),
                std::ops::Bound::Unbounded,
            ))
            .enumerate()
        {
            if !id.starts_with(&prefix) {
                break;
            }
            if position % 64 == 0 {
                self.check_fatal()?;
            }
            let key = id.strip_prefix(&prefix).and_then(key_of).ok_or_else(|| {
                EngineError::new("INPUT_INVALID", "Malformed durable index identity")
            })?;
            let key = key.as_str();
            let source = source_id(&query.collection, key);
            if self.writes.contains_key(&source) {
                continue;
            }
            let value = self
                .staged
                .get_shared(&source)
                .filter(|value| query.matches(value))
                .ok_or_else(|| {
                    EngineError::new(
                        "INPUT_INVALID",
                        "Durable index entry does not match its source",
                    )
                })?;
            bytes = bytes.saturating_add(128 + key.len());
            if self.total_retained_bytes().saturating_add(bytes) > self.retained_limit {
                return Err(self
                    .fatal
                    .get_or_insert_with(|| {
                        EngineError::new(
                            "EVALUATION_BUDGET",
                            "Index result exceeds the Rust memory budget",
                        )
                    })
                    .clone());
            }
            rows.insert(Key(key.into()), value.clone());
        }
        // Methods can query before a preview commits staged source writes into
        // the index. Overlay just those writes, preserving read-your-writes.
        let source_prefix = format!(
            "source:[{},",
            serde_json::to_string(&query.collection).expect("collection encodes")
        );
        for (position, (id, value)) in self.writes.range(source_prefix.clone()..).enumerate() {
            if !id.starts_with(&source_prefix) {
                break;
            }
            if position % 64 == 0 {
                self.check_fatal()?;
            }
            if let Some(value) = value
                && query.matches(value)
            {
                let (_, key) = source_pair(id)?;
                bytes = bytes.saturating_add(128 + key.len());
                if self.total_retained_bytes().saturating_add(bytes) > self.retained_limit {
                    return Err(self
                        .fatal
                        .get_or_insert_with(|| {
                            EngineError::new(
                                "EVALUATION_BUDGET",
                                "Index overlay exceeds the Rust memory budget",
                            )
                        })
                        .clone());
                }
                rows.insert(Key(key), value.clone());
            }
        }
        Ok(rows
            .into_iter()
            .map(|(key, value)| (key.0, value))
            .collect())
    }
}

#[cfg(test)]
mod schema_cache_tests {
    use super::*;

    fn record(collection: &str) -> Arc<Value> {
        Arc::new(json!({"indexes":[{"collection":collection,"fields":["shop"]}]}))
    }

    #[test]
    fn immutable_schema_identity_reuses_validation_and_changed_records_revalidate() {
        // Names no other test uses, since equal records share a parse.
        let source = record("identity-orders");
        let before = schema_parses();
        let (first, bytes) = Schema::load_shared(Some(&source), usize::MAX).unwrap();
        let (reused, reused_bytes) = Schema::load_shared(Some(&source), usize::MAX).unwrap();
        assert!(Arc::ptr_eq(&first, &reused));
        assert_eq!(bytes, allocation_cost(&source) * 2);
        assert_eq!(reused_bytes, bytes);
        assert_eq!(schema_parses() - before, 1);

        // Another Arc with the same contents, as the value cache hands out
        // when it reads a stored record again, needs no parse.
        let equal_replacement = Arc::new((*source).clone());
        let (equal, equal_bytes) =
            Schema::load_shared(Some(&equal_replacement), usize::MAX).unwrap();
        assert!(Arc::ptr_eq(&first, &equal));
        assert_eq!(equal_bytes, bytes);
        assert_eq!(schema_parses() - before, 1);
        let changed = record("identity-ledger");
        let (changed_schema, changed_bytes) =
            Schema::load_shared(Some(&changed), usize::MAX).unwrap();
        assert_eq!(changed_bytes, bytes, "same size, different contents");
        assert_eq!(changed_schema.indexes[0].collection, "identity-ledger");
        assert_eq!(first.indexes[0].collection, "identity-orders");
        assert_eq!(schema_parses() - before, 2);
    }

    #[test]
    fn schema_cache_keeps_budget_admission_and_rejects_malformed_replacements() {
        let valid = record("orders");
        let (_, bytes) = Schema::load_shared(Some(&valid), usize::MAX).unwrap();
        assert_eq!(
            Schema::load_shared(Some(&valid), bytes - 1)
                .unwrap_err()
                .code,
            "EVALUATION_BUDGET"
        );
        Schema::load_shared(Some(&valid), bytes).unwrap();
        for malformed in [
            json!({"indexes":"wrong"}),
            json!({"indexes":[{"collection":"orders","fields":["shop","shop"]}]}),
            json!({"aggregates":{"count":{"collection":"orders","fields":["shop"]}}}),
        ] {
            let invalid = Arc::new(malformed);
            assert_eq!(
                Schema::load_shared(Some(&invalid), usize::MAX)
                    .unwrap_err()
                    .code,
                "INPUT_INVALID"
            );
        }
        // Copy-on-write mutation cannot reuse validation for a changed record.
        let mut changed = valid.clone();
        Arc::make_mut(&mut changed)["indexes"] = Value::Null;
        assert!(!Arc::ptr_eq(&valid, &changed));
        assert_eq!(
            Schema::load_shared(Some(&changed), usize::MAX)
                .unwrap_err()
                .code,
            "INPUT_INVALID"
        );
    }

    #[test]
    fn schemas_of_several_databases_stay_parsed_on_a_thread_and_across_threads() {
        // A server's directory, its partitions and a staged deployment's
        // shadow schema take turns on the same worker threads.
        let records = [
            record("several-orders"),
            record("several-customers"),
            record("several-shops"),
        ];
        let before = schema_parses();
        let first = records
            .iter()
            .map(|source| Schema::load_shared(Some(source), usize::MAX).unwrap())
            .collect::<Vec<_>>();
        for _ in 0..10 {
            for (source, (schema, bytes)) in records.iter().zip(&first) {
                let (again, again_bytes) = Schema::load_shared(Some(source), usize::MAX).unwrap();
                assert!(Arc::ptr_eq(schema, &again));
                assert_eq!(again_bytes, *bytes);
                assert_eq!(again_bytes, allocation_cost(source) * 2);
            }
        }
        assert_eq!(schema_parses() - before, records.len());
        // A worker the pool starts later finds them parsed too, even through
        // another Arc with the same contents.
        let shared = records.clone();
        let parsed = first[0].0.clone();
        std::thread::spawn(move || {
            for source in &shared {
                Schema::load_shared(Some(source), usize::MAX).unwrap();
            }
            let copy = Arc::new((*shared[0]).clone());
            let (equal, _) = Schema::load_shared(Some(&copy), usize::MAX).unwrap();
            assert!(Arc::ptr_eq(&equal, &parsed));
            assert_eq!(schema_parses(), 0);
        })
        .join()
        .unwrap();
        // Budget admission still applies to every call.
        assert_eq!(
            Schema::load_shared(Some(&records[1]), first[1].1 - 1)
                .unwrap_err()
                .code,
            "EVALUATION_BUDGET"
        );
        assert_eq!(schema_parses() - before, records.len());
    }

    #[test]
    fn the_schema_cache_keeps_the_most_recently_used_records_it_may() {
        // A cache of its own, since the shared one serves the other tests.
        let mut cache = SchemaCache {
            entries: HashMap::new(),
            clock: 0,
        };
        let schema = Arc::new(Schema::default());
        let kept = (0..SCHEMA_CACHE_ENTRIES + 8)
            .map(|n| record(&format!("c{n}")))
            .collect::<Vec<_>>();
        for (n, source) in kept.iter().enumerate() {
            cache.keep(source, schema.clone(), 1);
            if n == 200 {
                // The first record, found again, is more recent than the next ones.
                assert!(cache.find(&kept[0]).is_some());
            }
        }
        assert_eq!(cache.entries.len(), SCHEMA_CACHE_ENTRIES);
        assert!(cache.find(&kept[0]).is_some());
        for evicted in &kept[1..9] {
            assert!(cache.find(evicted).is_none());
        }
        assert!(kept[9..].iter().all(|source| cache.find(source).is_some()));
        // Records nothing else holds go at the next lookup.
        drop(kept);
        cache.prune();
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn empty_schema_reuses_one_default_and_lets_go_of_records_nothing_else_holds() {
        let source = record("empty-orders");
        Schema::load_shared(Some(&source), usize::MAX).unwrap();
        let retained = Arc::downgrade(&source);
        drop(source);
        let (first, bytes) = Schema::load_shared(None, 0).unwrap();
        let (second, _) = Schema::load_shared(None, 0).unwrap();
        assert!(retained.upgrade().is_none());
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.empty());
        assert_eq!(bytes, 0);
    }
}
