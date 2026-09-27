use super::*;

#[cfg(test)]
thread_local! {
    static FORCE_PROPAGATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FORCE_FULL_GRAPH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static GRAPH_PASSES: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    static GRAPH_VISITS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

fn force_propagation() -> bool {
    #[cfg(test)]
    {
        FORCE_PROPAGATION.get()
    }
    #[cfg(not(test))]
    {
        false
    }
}

#[cfg(test)]
pub(super) fn with_full_propagation<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            FORCE_PROPAGATION.set(self.0);
        }
    }
    let _reset = Reset(FORCE_PROPAGATION.replace(true));
    run()
}

fn force_full_graph() -> bool {
    #[cfg(test)]
    {
        FORCE_FULL_GRAPH.get()
    }
    #[cfg(not(test))]
    {
        false
    }
}

#[cfg(test)]
pub(super) fn take_graph_passes() -> (usize, usize) {
    GRAPH_PASSES.replace((0, 0))
}

#[cfg(test)]
pub(super) fn take_graph_visits() -> (usize, usize) {
    GRAPH_VISITS.replace((0, 0))
}

fn graph_visit(_append: bool) {
    #[cfg(test)]
    GRAPH_VISITS.set({
        let (append, full) = GRAPH_VISITS.get();
        (append + usize::from(_append), full + usize::from(!_append))
    });
}

#[cfg(test)]
pub(super) fn with_full_graph<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            FORCE_FULL_GRAPH.set(self.0);
        }
    }
    let _reset = Reset(FORCE_FULL_GRAPH.replace(true));
    run()
}

fn graph_pass(_fast: bool) {
    #[cfg(test)]
    GRAPH_PASSES.set({
        let (fast, full) = GRAPH_PASSES.get();
        (fast + usize::from(_fast), full + usize::from(!_fast))
    });
}

/// Preview validation needs only changed identities and their encoded size.
/// Keep values borrowed here; finish builds the owned commit patch once.
fn preview_changes(
    before: &Records,
    staged: &Records,
    changed: &HashSet<String>,
    evaluated: &[String],
) -> (Vec<String>, usize) {
    let mut put_ids = Vec::new();
    let mut puts_bytes = 2;
    let mut deletes_bytes = 2;
    let mut deletes = 0;
    for id in changed {
        match (before.get(id), staged.get(id)) {
            (previous, Some(value)) if previous.is_none_or(|previous| !equal(previous, value)) => {
                puts_bytes +=
                    usize::from(!put_ids.is_empty()) + string_len(id) + 1 + encoded_len(value);
                put_ids.push(id.clone());
            }
            (Some(_), None) => {
                deletes_bytes += usize::from(deletes != 0) + string_len(id);
                deletes += 1;
            }
            _ => {}
        }
    }
    let evaluated_bytes = 2
        + evaluated.len().saturating_sub(1)
        + evaluated.iter().map(|id| string_len(id)).sum::<usize>();
    // {"puts":...,"deletes":...,"evaluated":...}, identical to patch_bytes.
    (put_ids, 33 + puts_bytes + deletes_bytes + evaluated_bytes)
}

impl Engine<'_> {
    pub(super) fn init_graph(&mut self) -> EngineResult<()> {
        if self.graph_ready {
            return Ok(());
        }
        self.check_fatal()?;
        self.staged.reactive().validate()?;
        if !self.staged.has_valid_source_ids() {
            return Err(EngineError::new(
                "INPUT_INVALID",
                "Malformed stored source key",
            ));
        }
        // Stored reader and height records already describe the graph, so an
        // invocation after a restart has nothing to rebuild or revalidate.
        self.graph_ready = true;
        Ok(())
    }

    pub(super) fn run_preview(
        &mut self,
        final_preview: bool,
        bundle_changed: bool,
    ) -> EngineResult<()> {
        // A final preview is the invocation's last operation. Any error escapes
        // the engine and discards its private overlay, so no callback can catch
        // it and continue from a rollback snapshot. Keep the full test path as
        // the old rollback implementation for differential coverage.
        if final_preview && !force_full_graph() {
            return self.run_preview_inner(final_preview, bundle_changed);
        }
        // Failed previews are recoverable unless their error is fatal. Keep the
        // pending writes and last successful preview exactly as they were, so a
        // method which catches a validation error cannot observe a partial graph.
        // These record snapshots share their tree and JSON allocations.
        let staged = self.staged.clone();
        let writes = self.writes.clone();
        let changed = self.changed.clone();
        let unchecked = self.unchecked.clone();
        let preview_dirty = self.preview_dirty;
        let retained_costs = self.retained_costs.clone();
        let retained_bytes = self.retained_bytes;
        let graph_costs = self.graph_costs.clone();
        let graph_index_bytes = self.graph_index_bytes;
        let graph_changes = self.graph_changes.clone();
        let root_changes = self.root_changes.clone();
        let staged_temporary = self.staged_temporary.clone();
        let result = self.run_preview_inner(final_preview, bundle_changed);
        if result.is_err() {
            self.staged = staged;
            self.writes = writes;
            self.changed = changed;
            self.unchecked = unchecked;
            self.preview_dirty = preview_dirty;
            self.retained_costs = retained_costs;
            self.retained_bytes = retained_bytes;
            self.graph_costs = graph_costs;
            self.graph_index_bytes = graph_index_bytes;
            self.graph_changes = graph_changes;
            self.root_changes = root_changes;
            self.staged_temporary = staged_temporary;
            self.graph_ready = false;
            self.rows = None;
            self.source_index_bytes = 0;
            self.query_indexes.clear();
            self.preview = Preview::default();
        }
        result
    }

    fn run_preview_inner(&mut self, final_preview: bool, bundle_changed: bool) -> EngineResult<()> {
        self.check_fatal()?;
        // Source-only queries need no graph validation. Only roots this
        // invocation changed, or stored to read one value, are compared.
        let (removed_roots, added_roots, temporary) = self.root_changes(final_preview);
        let same_roots = removed_roots.is_empty() && added_roots.is_empty();
        if !self.preview_dirty && same_roots && !bundle_changed {
            return Ok(());
        }
        self.init_graph()?;
        // A preceding ephemeral read may have produced a value that cannot fit
        // into the next snapshot's wrapper depth. It is legal for one read, but
        // must be checked before another graph evaluation.
        for id in &self.unchecked {
            if let Some(value) = self.staged.get(id) {
                depth(value, 1, "INPUT_INVALID")?;
            }
        }
        self.unchecked.clear();
        for value in self.writes.values().flatten() {
            depth(value, 3, "INPUT_INVALID")?;
        }
        for (_, value) in &added_roots {
            depth(value, 2, "INPUT_INVALID")?;
        }
        let before = self.staged.clone();
        self.preview = Preview::default();
        self.preview.rebuild_aggregates = bundle_changed;
        let writes = std::mem::take(&mut self.writes);
        let mut changed_dependencies = Vec::new();
        if self.keys_changed {
            changed_dependencies.push("managedKeys".into());
        }
        let mut windowed = Vec::new();
        for (id, value) in writes {
            let changed = match (self.staged.get(&id), &value) {
                (Some(previous), Some(value)) => !equal(previous, value),
                (None, None) => false,
                _ => true,
            };
            if changed {
                let (collection, key) = source_pair(&id)?;
                let previous = self.staged.get_shared(&id).cloned();
                metadata::scan_readers(
                    &self.staged,
                    &collection,
                    &key,
                    previous.as_deref(),
                    value.as_deref(),
                    &mut windowed,
                );
                self.undeclared_bucket_changes(
                    &collection,
                    previous.as_deref(),
                    value.as_deref(),
                    &mut changed_dependencies,
                );
                self.update_durable_indexes(
                    &collection,
                    &key,
                    previous,
                    value.clone(),
                    &mut changed_dependencies,
                )?;
                changed_dependencies.push(id.clone());
                changed_dependencies.push(collection_id(&collection));
            }
            self.changed.insert(id.clone());
            self.preview.changed.insert(id.clone());
            if let Some(value) = value {
                self.staged.insert_shared(id, value);
            } else {
                self.staged.remove_shared(&id);
            }
        }
        if self.has_time && self.staged.get("clock").and_then(Value::as_u64) != Some(self.now) {
            self.put("clock".into(), json!(self.now))?;
            changed_dependencies.push("clock".into());
        }
        if bundle_changed {
            self.preview
                .dirty
                .extend(self.staged.graph_cells().map(|(id, _)| id.to_owned()));
        }
        if bundle_changed {
            self.preview
                .direct
                .extend(self.preview.dirty.iter().cloned());
        }
        let direct_dependencies = changed_dependencies.len();
        // Scans whose window a write touched read it directly; their own
        // readers are only transitively dirty.
        for reader in windowed {
            self.preview.direct.insert(reader.clone());
            if self.preview.dirty.insert(reader.clone()) {
                changed_dependencies.push(reader);
            }
        }
        let mut cursor = 0;
        while cursor < changed_dependencies.len() {
            if cursor % 64 == 0 {
                self.check_fatal()?;
            }
            let readers: Vec<String> = self
                .staged
                .readers(&changed_dependencies[cursor])
                .collect();
            for reader in readers {
                if cursor < direct_dependencies {
                    self.preview.direct.insert(reader.clone());
                }
                if self.preview.dirty.insert(reader.clone()) {
                    changed_dependencies.push(reader);
                }
            }
            cursor += 1;
        }
        for id in removed_roots {
            self.remove(&id);
        }
        for (id, value) in added_roots {
            self.put(id, (*value).clone())?;
        }
        self.root_changes.clear();
        self.staged_temporary = temporary;
        self.staged.reactive().validate()?;
        if bundle_changed || force_full_graph() {
            self.traverse_graph()?;
            graph_pass(false);
        } else {
            // The same UTF-16 cell ordering as the full root traversal; clean
            // roots have no callback side effects and need not be visited.
            let added = std::mem::take(&mut self.graph_changes.added_roots);
            let mut roots = BTreeMap::new();
            for id in self
                .preview
                .dirty
                .iter()
                .cloned()
                .chain(added.iter().map(|id| format!("cell:{}", &id[5..])))
            {
                if let Some(root) = self.stored_root(&id) {
                    roots.insert(Key(id), root);
                }
            }
            for root in roots.values() {
                self.ensure(root.name(), root.args())?;
            }
            self.settle_graph()?;
            graph_pass(true);
        }
        self.check_fatal()?;
        let (put_ids, bytes) = preview_changes(
            &before,
            &self.staged,
            &self.preview.changed,
            &self.preview.evaluated,
        );
        self.unchecked.extend(put_ids);
        if bytes > self.output_limit {
            return self.abort(
                "EVALUATION_BUDGET",
                "Output exceeds FLOWER_RESULT_MAX_BYTES",
            );
        }
        self.evaluated.append(&mut self.preview.evaluated);
        self.preview_dirty = false;
        self.keys_changed = false;
        Ok(())
    }

    pub(super) fn count_reads(&mut self, count: usize) -> EngineResult<()> {
        self.preview.reads = self.preview.reads.saturating_add(count);
        self.check_fatal()
    }

    pub(super) fn observe(
        &mut self,
        observed: &mut BTreeSet<Key>,
        dependency: String,
    ) -> EngineResult<()> {
        self.speculative_read(&dependency);
        let dependency = Key(dependency);
        if observed.contains(&dependency) {
            return Ok(());
        }
        let bytes = 128 + dependency.0.len();
        let total = self
            .retained_bytes
            .saturating_add(self.graph_index_bytes)
            .saturating_add(self.source_index_bytes)
            .saturating_add(self.trace_bytes)
            .saturating_add(self.observed_bytes)
            .saturating_add(bytes);
        if total > self.retained_limit {
            return self.abort(
                "EVALUATION_BUDGET",
                "Observed dependencies exceed FLOWER_RUST_MEMORY_BYTES",
            );
        }
        self.observed_bytes += bytes;
        observed.insert(dependency);
        Ok(())
    }

    fn ensure(&mut self, name: &str, args: &Value) -> EngineResult<()> {
        self.check_fatal()?;
        let id = cell_id(name, args);
        self.speculative_read(&id);
        if self.preview.active.contains(&id) {
            return self.abort("CYCLE", format!("Reactive cycle at {id}"));
        }
        let previous = self.staged.get_shared(&id).cloned();
        if previous.is_some()
            && (self.preview.complete.contains(&id) || !self.preview.dirty.contains(&id))
        {
            return Ok(());
        }
        if self.preview.depth >= 128 {
            return self.abort("EVALUATION_BUDGET", "Reactive evaluation depth exceeds 128");
        }
        // A transitive invalidation is only a possibility. Refresh the old
        // child dependencies first; unchanged outcomes do not require another
        // application callback. Branch dependency changes still live in the
        // child record and participate in full topology validation/collection.
        if !force_propagation()
            && !self.preview.direct.contains(&id)
            && let Some(previous) = &previous
        {
            self.preview.active.insert(id.clone());
            self.preview.depth += 1;
            let result = (|| -> EngineResult<bool> {
                let children = || {
                    previous["deps"]
                        .as_array()
                        .expect("validated dependencies")
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|dep| dep.starts_with("cell:"))
                };
                // Do not evaluate multiple old branches speculatively: a changed
                // control dependency can make another formerly used branch
                // unreachable (including a branch that now contains a cycle).
                // Let application read order choose its dependencies in that case.
                if children().any(|dep| !self.staged.contains_key(dep)) {
                    return Ok(true);
                }
                let mut dirty = children().filter(|dep| self.preview.dirty.contains(*dep));
                let Some(dep) = dirty.next() else {
                    return Ok(false);
                };
                if dirty.next().is_some() {
                    return Ok(true);
                }
                let child = self.staged.get_shared(dep).cloned().expect("checked child");
                self.ensure(
                    child["name"].as_str().expect("validated name"),
                    &child["args"],
                )?;
                Ok(self.preview.outcomes_changed.contains(dep))
            })();
            self.preview.depth -= 1;
            self.preview.active.remove(&id);
            if !result? {
                self.preview.complete.insert(id);
                return Ok(());
            }
        }
        // Keep the evaluation history and traversal sets bounded by actual
        // estimated allocation, including repeated previews of the same cell.
        let trace_bytes = 128usize.saturating_add(id.len().saturating_mul(4));
        if self.total_retained_bytes().saturating_add(trace_bytes) > self.retained_limit {
            return self.abort(
                "EVALUATION_BUDGET",
                "Evaluation history exceeds FLOWER_RUST_MEMORY_BYTES",
            );
        }
        self.trace_bytes = self.trace_bytes.saturating_add(trace_bytes);
        self.preview.evaluated.push(id.clone());
        self.preview.active.insert(id.clone());
        self.preview.depth += 1;
        let mut observed = BTreeSet::new();
        let result = if let Some(index) = self.schema.aggregates.get(name).cloned() {
            self.aggregate_value(name, args, &index, previous.as_deref(), &mut observed)
        } else {
            let execute = self.execute;
            let mut host = |operation: &str, arguments: Value| {
                self.derived_host(operation, arguments, &mut observed)
            };
            execute
                .execute("derived", name, args, &mut host)
                .and_then(|value| normalize(value, "INVALID_VALUE"))
        };
        self.preview.depth -= 1;
        self.preview.active.remove(&id);
        self.check_fatal()?;
        let outcome = match result {
            Ok(value) => json!({"ok":true,"value":value}),
            Err(error) => {
                if error.code == "CYCLE" || error.code == "EVALUATION_BUDGET" {
                    return self.abort(&error.code, error.message);
                }
                if let Some(previous) = &previous {
                    for dep in previous["deps"].as_array().expect("validated dependencies") {
                        self.observe(
                            &mut observed,
                            dep.as_str().expect("validated dependency").into(),
                        )?;
                    }
                }
                json!({"ok":false,"error":error})
            }
        };
        if previous
            .as_ref()
            .is_none_or(|previous| !equal(&previous["outcome"], &outcome))
        {
            self.preview.outcomes_changed.insert(id.clone());
        }
        let observed_bytes = observed.iter().map(|key| 128 + key.0.len()).sum::<usize>();
        let deps: Vec<_> = observed.into_iter().map(|key| key.0).collect();
        // Materialization callers need the stored cell, not an owned copy of
        // its outcome. Move the newly computed tree into that cell once.
        let cell = Value::Object(serde_json::Map::from_iter([
            ("name".into(), Value::String(name.into())),
            ("args".into(), args.clone()),
            ("outcome".into(), outcome),
            (
                "deps".into(),
                Value::Array(deps.into_iter().map(Value::String).collect()),
            ),
        ]));
        self.put(id.clone(), cell)?;
        self.observed_bytes = self.observed_bytes.saturating_sub(observed_bytes);
        self.preview.complete.insert(id);
        Ok(())
    }

    fn derived_host(
        &mut self,
        operation: &str,
        arguments: Value,
        observed: &mut BTreeSet<Key>,
    ) -> EngineResult<Value> {
        self.check_fatal()?;
        if self.mode != "deployment" {
            self.count_operations(1)?;
        }
        let arguments = arguments
            .as_array()
            .ok_or_else(|| EngineError::new("INVALID_VALUE", "Host arguments must be an array"))?;
        let argument = |index: usize| arguments.get(index).unwrap_or(&Value::Null);
        match operation {
            "managedKey" => {
                // Record unsuccessful resolutions too: binding a previously
                // missing key must repair the stored error on the next commit.
                self.count_reads(1)?;
                self.observe(observed, "managedKeys".into())?;
                self.managed_key(argument(0))
            }
            "now" | "clock" => {
                self.count_reads(1)?;
                self.query_cacheable = false;
                // Deployments build cells; only queries and mutations report time use.
                self.clock_polled |= operation == "now" && self.mode != "deployment";
                self.observe(observed, "clock".into())?;
                Ok(json!(self.now))
            }
            "changesAt" => {
                self.count_reads(1)?;
                self.declare_change(argument(0))?;
                Ok(Value::Null)
            }
            "get" => {
                self.count_reads(1)?;
                let reference = argument(0);
                record(reference, "reference", "INVALID_REFERENCE")?;
                let name = string(&reference["name"], "reference.name", "INVALID_REFERENCE")?;
                match reference["kind"].as_str() {
                    Some("collection") => {
                        let key = string(argument(1), "source key", "INVALID_REFERENCE")?;
                        let id = source_id(name, key);
                        self.observe(observed, id.clone())?;
                        let value = self.staged.get_shared(&id).cloned();
                        match value {
                            Some(value) => self.copy_for_host(&value),
                            None => Ok(Value::Null),
                        }
                    }
                    Some("derived") => {
                        let args = normalize(argument(1).clone(), "INVALID_VALUE")?;
                        let id = cell_id(name, &args);
                        self.observe(observed, id.clone())?;
                        self.ensure(name, &args)?;
                        self.check_materialized_keys(&id)?;
                        let cell = self.staged.get(&id).expect("evaluated cell exists");
                        outcome_value(&cell["outcome"])
                    }
                    _ => Err(EngineError::new(
                        "INVALID_REFERENCE",
                        "Unknown reference kind",
                    )),
                }
            }
            "scan" => {
                if let Some(options) = arguments.get(1) {
                    let query = ranges::RangeQuery::parse_scan(argument(0), options)?;
                    let rows = self.observed_range(observed, &query)?;
                    let count = rows.as_array().expect("scan rows").len();
                    self.count_reads(1 + count)?;
                    if self.mode != "deployment" {
                        self.count_operations(count)?;
                    }
                    return Ok(rows);
                }
                let collection = reference_name(argument(0), "collection")?;
                self.observe(observed, collection_id(collection))?;
                let rows = self.collection_rows(collection)?;
                self.count_reads(1 + rows.len())?;
                if self.mode != "deployment" {
                    self.count_operations(rows.len())?;
                }
                self.rows_for_host(rows)
            }
            "range" => {
                let query = ranges::RangeQuery::parse(argument(0))?;
                self.count_reads(1)?;
                self.observed_range(observed, &query)
            }
            "query" => {
                let query = Query::parse(argument(0))?;
                // Declared or not, only rows entering, leaving or changing in
                // the matching bucket can change the result.
                self.observe(
                    observed,
                    indexes::bucket_id(&query.collection, &query.fields, &query.expected),
                )?;
                if self.has_index(&query) {
                    let rows = self.indexed_rows(&query)?;
                    self.count_reads(1 + rows.len())?;
                    for (key, _) in &rows {
                        self.observe(observed, source_id(&query.collection, key))?;
                    }
                    self.query_values(rows.into_iter().map(|(_, value)| value).collect())
                } else {
                    self.query_rows(&query, true)
                }
            }
            _ => Err(EngineError::new(
                "INVALID_REFERENCE",
                format!("Unknown context operation: {operation}"),
            )),
        }
    }

    /// Depend on exactly the window a range or scan result came from. A failed
    /// read keeps the whole index, since a later write can repair it.
    fn observed_range(
        &mut self,
        observed: &mut BTreeSet<Key>,
        query: &ranges::RangeQuery,
    ) -> EngineResult<Value> {
        match self.range_rows(query, false) {
            Ok(scanned) => {
                if let Some(dependency) = scanned.dependency.id() {
                    self.observe(observed, dependency)?;
                }
                Ok(scanned.rows)
            }
            Err(error) => {
                self.observe(observed, query.dependency(self))?;
                Err(error)
            }
        }
    }

    /// Evaluate every root and derive the graph from scratch: its stored
    /// cells, reader records and heights must match what incremental
    /// maintenance keeps. Deployments rebuild every cell this way, tests use
    /// it as the reference, and a failed incremental check uses it to report
    /// the same error as the reference.
    fn traverse_graph(&mut self) -> EngineResult<()> {
        let all_roots: BTreeMap<Key, metadata::Root> = self
            .staged
            .graph_roots()
            .map(|(id, _)| format!("cell:{}", &id[5..]))
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|cell| Some((Key(cell.clone()), self.stored_root(&cell)?)))
            .collect();
        let all_roots: Vec<_> = all_roots.into_values().collect();
        for root in &all_roots {
            self.ensure(root.name(), root.args())?;
        }
        let mut depths = metadata::Depths::new();
        let mut visiting = HashSet::new();
        for root in &all_roots {
            self.visit(
                &cell_id(root.name(), root.args()),
                0,
                &mut depths,
                &mut visiting,
            )?;
        }
        let obsolete: Vec<_> = self
            .staged
            .graph_cells()
            .map(|(id, _)| id)
            .filter(|id| !depths.contains_key(*id))
            .map(str::to_owned)
            .collect();
        for id in obsolete {
            self.remove(&id);
        }
        for (id, height) in &depths {
            self.store_height(id, usize::from(*height));
        }
        self.graph_changes = GraphChanges::default();
        Ok(())
    }

    /// Collect released cells nothing holds, then bring heights up to date.
    /// Both run once the preview's evaluation is done, since callbacks may
    /// move an edge away and back, or through a deeper cell, while it runs.
    fn settle_graph(&mut self) -> EngineResult<()> {
        let mut collected = 0usize;
        while let Some(id) = self.graph_changes.released.pop_first() {
            if collected % 64 == 0 {
                self.check_fatal()?;
            }
            collected += 1;
            if self.staged.contains_key(&id) && !self.held(&id) {
                // Its reader records go too, releasing the cells it read.
                self.remove(&id);
            }
        }
        // A callback error keeps its old edges without evaluating them, so a
        // dirty child can still be held. Evaluating it where the reference
        // traversal would keeps callbacks in the same order.
        if self
            .preview
            .dirty
            .iter()
            .any(|id| !self.preview.complete.contains(id) && self.staged.contains_key(id))
        {
            graph_visit(false);
            return self.traverse_graph();
        }
        let mut pending: std::collections::VecDeque<String> =
            std::mem::take(&mut self.graph_changes.reshaped).into();
        let mut queued: HashSet<String> = pending.iter().cloned().collect();
        while let Some(id) = pending.pop_front() {
            queued.remove(&id);
            self.check_fatal()?;
            let Some(cell) = self.staged.get_shared(&id).cloned() else {
                continue;
            };
            graph_visit(true);
            let mut height = 1;
            for dep in cell_edges(&cell) {
                if !self.staged.contains_key(dep) {
                    return self.graph_error(EngineError::new(
                        "INPUT_INVALID",
                        format!("Missing derived dependency {dep}"),
                    ));
                }
                height = height.max(self.height(dep) + 1);
            }
            // A cycle raises the heights around it without bound.
            if height > 128 {
                return self.graph_error(EngineError::new(
                    "EVALUATION_BUDGET",
                    "Reactive graph depth exceeds 128",
                ));
            }
            if height != self.height(&id) {
                self.store_height(&id, height);
                for reader in self.staged.readers(&id) {
                    if queued.insert(reader.to_owned()) {
                        pending.push_back(reader.to_owned());
                    }
                }
            }
        }
        Ok(())
    }

    /// Report what the reference traversal reports for this graph, which
    /// names the cycle it finds first.
    fn graph_error(&mut self, fallback: EngineError) -> EngineResult<()> {
        graph_visit(false);
        self.traverse_graph()?;
        Err(fallback)
    }

    /// Roots to remove and add, in key order, and the root stored only to read
    /// `temporary_root`. The desired set is the stored roots, with the
    /// invocation's changes applied, plus the temporary root.
    fn root_changes(
        &self,
        final_preview: bool,
    ) -> (Vec<String>, Vec<(String, Arc<Value>)>, Option<String>) {
        let temporary = self
            .temporary_root
            .as_ref()
            .filter(|_| !final_preview)
            .map(|reference| (reference.root_id(), reference.value()));
        let durable = |id: &str| match self.root_changes.get(id) {
            Some(change) => change.is_some(),
            None => self.staged.contains_key(id) && self.staged_temporary.as_deref() != Some(id),
        };
        let candidates: BTreeSet<&str> = self
            .root_changes
            .keys()
            .map(String::as_str)
            .chain(self.staged_temporary.as_deref())
            .chain(temporary.as_ref().map(|(id, _)| id.as_str()))
            .collect();
        let (mut removed, mut added) = (Vec::new(), Vec::new());
        for id in candidates {
            let wanted_temporary = temporary
                .as_ref()
                .is_some_and(|(temporary, _)| temporary == id);
            let wanted = wanted_temporary || durable(id);
            let stored = self.staged.contains_key(id);
            if wanted && !stored {
                let value = match self.root_changes.get(id) {
                    Some(Some(value)) => value.clone(),
                    _ => Arc::new(temporary.as_ref().expect("temporary root").1.clone()),
                };
                added.push((id.to_owned(), value));
            } else if !wanted && stored {
                removed.push(id.to_owned());
            }
        }
        let staged_temporary = temporary.map(|(id, _)| id).filter(|id| !durable(id));
        (removed, added, staged_temporary)
    }

    fn stored_root(&self, cell: &str) -> Option<metadata::Root> {
        self.staged
            .get_shared(&format!("root:{}", &cell[5..]))
            .map(|value| metadata::Root {
                value: value.clone(),
            })
    }

    fn held(&self, cell: &str) -> bool {
        self.staged.has_readers(cell) || self.staged.contains_key(&format!("root:{}", &cell[5..]))
    }

    fn height(&self, cell: &str) -> usize {
        self.staged
            .get(&Records::height_key(cell))
            .and_then(Value::as_u64)
            .map_or(1, |height| height as usize)
    }

    fn store_height(&mut self, cell: &str, height: usize) {
        if height == self.height(cell) {
            return;
        }
        let id = Records::height_key(cell);
        if height == 1 {
            self.delete(&id);
        } else {
            self.write_structure(id, json!(height));
        }
    }

    fn visit(
        &mut self,
        id: &str,
        level: usize,
        depths: &mut metadata::Depths,
        visiting: &mut HashSet<String>,
    ) -> EngineResult<u8> {
        self.check_fatal()?;
        if visiting.contains(id) {
            return self.abort("CYCLE", format!("Reactive cycle at {id}"));
        }
        if let Some(&height) = depths.get(id) {
            if level + usize::from(height) > 128 {
                return self.abort("EVALUATION_BUDGET", "Reactive graph depth exceeds 128");
            }
            return Ok(height);
        }
        if level >= 128 {
            return self.abort("EVALUATION_BUDGET", "Reactive graph depth exceeds 128");
        }
        graph_visit(false);
        // Each cell's accounted bytes, charged to the transaction that added
        // it, reserve its traversal and proof entries; graph size has no
        // independent count ceiling.
        let mut cell = self.staged.get_shared(id).cloned().ok_or_else(|| {
            EngineError::new("INPUT_INVALID", format!("Missing derived dependency {id}"))
        })?;
        if self.preview.dirty.contains(id) && !self.preview.complete.contains(id) {
            self.ensure(
                cell["name"].as_str().expect("validated name"),
                &cell["args"],
            )?;
            cell = self
                .staged
                .get_shared(id)
                .expect("evaluated cell exists")
                .clone();
        }
        visiting.insert(id.into());
        let mut height = 1;
        for dep in cell["deps"].as_array().expect("validated dependencies") {
            let dep = dep.as_str().expect("validated dependency");
            if dep.starts_with("cell:") {
                height = height.max(usize::from(self.visit(dep, level + 1, depths, visiting)?) + 1);
            }
        }
        if level + height > 128 {
            return self.abort("EVALUATION_BUDGET", "Reactive graph depth exceeds 128");
        }
        visiting.remove(id);
        depths.insert(id.into(), height as u8);
        Ok(height as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_preview_accounting_matches_the_owned_patch() {
        let mut before = Records::default();
        before.insert("deleted\"\\\n\0🌸".into(), json!({"old": [1, 2, 3]}));
        before.insert("changed".into(), json!({"value": 1}));
        before.insert("numeric".into(), json!({"value": 1.0, "zero": -0.0}));
        before.insert("same".into(), json!({"text": "retained"}));
        let mut staged = before.clone();
        staged.remove("deleted\"\\\n\0🌸");
        staged.insert("changed".into(), json!({"value": 2}));
        staged.insert("numeric".into(), json!({"value": 1, "zero": 0}));
        staged.insert("😀".into(), json!({"\\\"\n": [true, null, "é🌸", 1e-7]}));
        staged.insert("\u{e000}".into(), json!([1e20, 1e21, -3.5]));

        for ids in [
            vec![],
            vec!["deleted\"\\\n\0🌸"],
            vec!["😀", "\u{e000}"],
            vec!["numeric", "same", "absent"],
            vec![
                "deleted\"\\\n\0🌸",
                "changed",
                "numeric",
                "same",
                "absent",
                "😀",
                "\u{e000}",
            ],
        ] {
            let changed = ids.into_iter().map(str::to_owned).collect();
            for evaluated in [
                vec![],
                vec!["cell:\"🌸".into(), "cell:\"🌸".into(), "\n".into()],
            ] {
                let (puts, deletes) = changes(&before, &staged, &changed);
                let (put_ids, bytes) = preview_changes(&before, &staged, &changed, &evaluated);
                assert_eq!(
                    put_ids.into_iter().collect::<HashSet<_>>(),
                    puts.keys().cloned().collect::<HashSet<_>>()
                );
                assert_eq!(bytes, patch_bytes(&puts, &deletes, &evaluated));
                assert_eq!(
                    bytes,
                    encoded_len(&json!({
                        "puts": puts, "deletes": deletes, "evaluated": evaluated
                    }))
                );
            }
        }
    }
}
