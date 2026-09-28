//! Opt-in reducers receive only source-row deltas; arbitrary derives still rerun.
use super::*;
use indexes::IndexSpec;

/// A group built from its rows goes to its reducer this many rows at a time at most (the first call
/// initializing it, each next one carrying the accumulator on), so neither the guest nor the Rust
/// side ever holds a whole big group's rows as one payload.
pub(super) const REDUCER_CHUNK_ROWS: usize = 1024;
/// Nor more than about this many bytes of rows at a time.
pub(super) const REDUCER_CHUNK_BYTES: usize = 1 << 20;

impl Engine<'_> {
    /// What an aggregate's stored cell records to keep its accumulator across deployments: its
    /// version, and the index it folds (a different index is a different group). None when the
    /// aggregate declares no version: a new bundle rebuilds its groups.
    pub(super) fn aggregate_fingerprint(&self, name: &str, index: &IndexSpec) -> Option<Value> {
        self.schema.aggregate_versions.get(name).map(|version| {
            json!({"version": version, "collection": index.collection, "fields": index.fields})
        })
    }

    pub(super) fn aggregate_value(
        &mut self,
        name: &str,
        args: &Value,
        index: &IndexSpec,
        previous: Option<&Value>,
        observed: &mut BTreeSet<Key>,
    ) -> EngineResult<Value> {
        let query = Query::parse(
            &json!({"kind":"query","collection":index.collection,"fields":index.fields,"value":args}),
        )?;
        let bucket = indexes::bucket_id(&query.collection, &query.fields, &query.expected);
        self.observe(observed, bucket.clone())?;
        // A new bundle rebuilds a group from its rows, unless the aggregate kept the version (and
        // index) its accumulator was built with: then it goes on from the accumulator.
        let kept = !self.preview.rebuild_aggregates
            || self
                .aggregate_fingerprint(name, index)
                .is_some_and(|fingerprint| {
                    previous.and_then(|cell| cell.get("reducer")) == Some(&fingerprint)
                });
        let initialized = kept && previous.is_some_and(|cell| cell["outcome"]["ok"] == true);
        let mut accumulator = if initialized {
            previous.unwrap()["outcome"]["value"].clone()
        } else {
            Value::Null
        };
        let deltas: Vec<_> = if initialized {
            self.preview
                .index_changes
                .get(&bucket)
                .cloned()
                .unwrap_or_default()
        } else {
            let rows = self.indexed_rows(&query)?;
            // The cell depends on its bucket, which graph maintenance feeds
            // every row change within, but a certificate checks the bucket by
            // its index entries, which a row changing in place leaves alone.
            // Certify the rows themselves, as a query over the bucket would.
            for (key, _) in &rows {
                self.record_read(source_id(&query.collection, key));
            }
            rows.into_iter()
                .map(|(key, value)| indexes::SourceDelta {
                    key,
                    previous: None,
                    value: Some(value),
                })
                .collect()
        };
        if initialized && deltas.is_empty() {
            return Ok(accumulator);
        }
        self.count_reads(1 + deltas.len())?;
        let mut deltas = deltas.into_iter().peekable();
        let mut initialize = !initialized;
        // At least one call, even for an empty group (its initial value); then one per chunk.
        loop {
            let mut allocated = 256usize
                .saturating_add(allocation_cost(args))
                .saturating_add(allocation_cost(&accumulator));
            let floor = allocated;
            let mut changes = Vec::new();
            while changes.len() < REDUCER_CHUNK_ROWS
                && (changes.is_empty() || allocated - floor < REDUCER_CHUNK_BYTES)
            {
                let Some(delta) = deltas.next() else {
                    break;
                };
                if changes.len() % 64 == 0 {
                    self.check_fatal()?;
                }
                allocated = allocated
                    .saturating_add(256 + delta.key.len())
                    .saturating_add(delta.previous.as_deref().map_or(0, allocation_cost))
                    .saturating_add(delta.value.as_deref().map_or(0, allocation_cost));
                if self.total_retained_bytes().saturating_add(allocated) > self.retained_limit {
                    return self.abort(
                        "EVALUATION_BUDGET",
                        "Aggregate delta arguments exceed the Rust memory budget",
                    );
                }
                let mut change =
                    serde_json::Map::from_iter([("key".into(), Value::String(delta.key))]);
                if let Some(value) = delta.previous {
                    change.insert("old".into(), (*value).clone());
                }
                if let Some(value) = delta.value {
                    change.insert("new".into(), (*value).clone());
                }
                changes.push(Value::Object(change));
            }
            let payload = json!({"initialize":initialize,"group":args,"previous":accumulator,"changes":changes});
            let mut attempted_context = false;
            let result = self
                .execute
                .execute("derived", name, &payload, &mut |_, _| {
                    attempted_context = true;
                    Err(EngineError::new(
                        "AGGREGATE_CONTEXT_FORBIDDEN",
                        "Aggregate reducers cannot access database context",
                    ))
                });
            if attempted_context {
                return self.abort(
                    "AGGREGATE_CONTEXT_FORBIDDEN",
                    "Aggregate reducers cannot access database context",
                );
            }
            accumulator = result.and_then(|value| normalize(value, "INVALID_VALUE"))?;
            initialize = false;
            if deltas.peek().is_none() {
                return Ok(accumulator);
            }
            self.check_fatal()?;
        }
    }
}
