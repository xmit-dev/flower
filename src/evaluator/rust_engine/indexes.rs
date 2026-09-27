//! Durable equality indexes live in the same replicated state as their sources.
use super::*;
use std::{cell::RefCell, sync::OnceLock};

struct CachedSchema {
    record: Arc<Value>,
    schema: Arc<Schema>,
    bytes: usize,
}

thread_local! {
    // Keep only the last schema used by this worker. Holding the immutable
    // record makes pointer reuse impossible; replacing/mutating a record
    // creates a different Arc and must pass validation again.
    static SCHEMA_CACHE: RefCell<Option<CachedSchema>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    #[serde(default)]
    pub aggregates: BTreeMap<String, IndexSpec>,
    /// Access policies by collection name, enforced on methods' reads and writes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub policies: BTreeMap<String, super::Policy>,
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
            SCHEMA_CACHE.with_borrow_mut(|cached| *cached = None);
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
        SCHEMA_CACHE.with_borrow_mut(|cached| {
            if let Some(previous) = cached.as_ref()
                && Arc::ptr_eq(record, &previous.record)
            {
                check_budget(previous.bytes)?;
                return Ok((previous.schema.clone(), previous.bytes));
            }
            // Preserve the existing conservative JSON + typed-schema charge,
            // including admission before cloning/deserializing on a miss.
            let bytes = allocation_cost(record).saturating_mul(2);
            check_budget(bytes)?;
            let schema = Arc::new(Self::load(Some(record))?);
            *cached = Some(CachedSchema {
                record: record.clone(),
                schema: schema.clone(),
                bytes,
            });
            Ok((schema, bytes))
        })
    }

    pub(super) fn allocation_cost(&self) -> usize {
        let policies = self.policies.iter().fold(0usize, |bytes, (name, policy)| {
            bytes.saturating_add(128 + name.len() + policy.allocation_cost())
        });
        policies.saturating_add(self.indexes.iter().chain(self.aggregates.values()).fold(
            self.aggregates
                .keys()
                .fold(0usize, |bytes, name| bytes.saturating_add(128 + name.len())),
            |bytes, index| {
                index.fields.iter().fold(
                    bytes.saturating_add(192 + index.collection.len()),
                    |bytes, field| bytes.saturating_add(32 + field.len()),
                )
            },
        ))
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
        for (name, policy) in &self.policies {
            if name.is_empty() {
                return Err(EngineError::new("INPUT_INVALID", "Malformed access policy"));
            }
            policy.validate().map_err(|error| {
                EngineError::new("INPUT_INVALID", format!("Malformed access policy: {error}"))
            })?;
        }
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
        self.indexes.is_empty() && self.aggregates.is_empty() && self.policies.is_empty()
    }
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
fn bucket_prefix(collection: &str, fields: &[String], encoded: &str) -> String {
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
    fn immutable_schema_identity_reuses_validation_and_replacement_revalidates() {
        let source = record("orders");
        let (first, bytes) = Schema::load_shared(Some(&source), usize::MAX).unwrap();
        let (reused, reused_bytes) = Schema::load_shared(Some(&source), usize::MAX).unwrap();
        assert!(Arc::ptr_eq(&first, &reused));
        assert_eq!(bytes, allocation_cost(&source) * 2);
        assert_eq!(reused_bytes, bytes);

        let equal_replacement = Arc::new((*source).clone());
        let (revalidated, _) = Schema::load_shared(Some(&equal_replacement), usize::MAX).unwrap();
        assert!(!Arc::ptr_eq(&first, &revalidated));
        assert_eq!(*first, *revalidated);
        let changed = record("customers");
        let (changed_schema, _) = Schema::load_shared(Some(&changed), usize::MAX).unwrap();
        assert_eq!(changed_schema.indexes[0].collection, "customers");
        assert_eq!(first.indexes[0].collection, "orders");
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
    fn empty_schema_reuses_one_default_and_evicts_the_worker_entry() {
        let source = record("orders");
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
