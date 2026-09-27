//! Opt-in reducers receive only source-row deltas; arbitrary derives still rerun.
use super::*;
use indexes::IndexSpec;

impl Engine<'_> {
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
        let initialized = !self.preview.rebuild_aggregates
            && previous.is_some_and(|cell| cell["outcome"]["ok"] == true);
        let accumulator = if initialized {
            previous.unwrap()["outcome"]["value"].clone()
        } else {
            Value::Null
        };
        let mut allocated = 256usize
            .saturating_add(allocation_cost(args))
            .saturating_add(allocation_cost(&accumulator));
        let mut changes = Vec::new();
        let deltas: Vec<_> = if initialized {
            self.preview
                .index_changes
                .get(&bucket)
                .cloned()
                .unwrap_or_default()
        } else {
            self.indexed_rows(&query)?
                .into_iter()
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
        for (position, delta) in deltas.into_iter().enumerate() {
            if position % 64 == 0 {
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
            let mut change = serde_json::Map::from_iter([("key".into(), Value::String(delta.key))]);
            if let Some(value) = delta.previous {
                change.insert("old".into(), (*value).clone());
            }
            if let Some(value) = delta.value {
                change.insert("new".into(), (*value).clone());
            }
            changes.push(Value::Object(change));
        }
        let payload = json!({"initialize":!initialized,"group":args,"previous":accumulator,"changes":changes});
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
        result.and_then(|value| normalize(value, "INVALID_VALUE"))
    }
}
