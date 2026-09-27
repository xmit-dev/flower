//! Trusted transaction coordinator. Application callbacks are the only code
//! executed in QuickJS; graph, overlays, dependency tracking, queries and commit
//! patches stay in Rust. A preview reuses both the persistent snapshot and its
//! lazily built indexes instead of serializing the database into another VM.

mod dependencies;
mod graph;
pub use dependencies::{DependencyCertificate, MutationCertificate, Observation, touched, touches_everything};
mod metadata;
pub(crate) use metadata::{ReactiveIndex, update_memberships};
mod access;
mod indexes;
mod ranges;
mod reducers;
mod windows;
pub use access::Policy;
pub use indexes::{IndexSpec, Schema};
pub(crate) use indexes::{staged_entries, staged_prefixes, staged_schema, staged_schema_bytes};
mod json;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::Evaluation;
use crate::consensus::Records;
use json::canonical_json;
use json::{
    Key, allocation_cost, cell_id, collection_id, compare, depth, encoded_len, equal, normalize,
    record, root_id, source_id, source_pair, string, string_len,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EngineError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl EngineError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    /// The caller-visible failure: code, message and optional JSON details.
    pub fn failure(&self) -> Value {
        let mut failure = serde_json::json!({"code": self.code, "message": self.message});
        if let Some(details) = &self.details {
            failure["details"] = details.clone();
        }
        failure
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EngineError {}

pub type EngineResult<T> = Result<T, EngineError>;

/// Synchronous application execution. A host call may recursively execute a
/// child derived function in another pristine Wasm instance. No JS heap or
/// context handle crosses this interface.
pub trait Executor {
    fn check_budget(&self) -> EngineResult<()> {
        Ok(())
    }
    fn execute(
        &self,
        kind: &str,
        name: &str,
        args: &Value,
        host: &mut dyn FnMut(&str, Value) -> EngineResult<Value>,
    ) -> EngineResult<Value>;
}

#[derive(Clone)]
struct Reference {
    name: String,
    args: Value,
}

impl Reference {
    fn parse(value: &Value, label: &str) -> EngineResult<Self> {
        record(value, label, "INPUT_INVALID")?;
        Ok(Self {
            name: string(&value["name"], &format!("{label}.name"), "INPUT_INVALID")?.into(),
            args: value.get("args").cloned().unwrap_or(Value::Null),
        })
    }
    fn value(&self) -> Value {
        json!({"name":self.name,"args":self.args})
    }
    fn root_id(&self) -> String {
        root_id(&self.name, &self.args)
    }
}

#[derive(Default)]
struct Preview {
    dirty: HashSet<String>,
    direct: HashSet<String>,
    outcomes_changed: HashSet<String>,
    complete: HashSet<String>,
    active: HashSet<String>,
    evaluated: Vec<String>,
    changed: HashSet<String>,
    reads: usize,
    depth: usize,
    index_changes: HashMap<String, Vec<indexes::SourceDelta>>,
    rebuild_aggregates: bool,
}

type Rows = HashMap<String, BTreeMap<Key, Arc<Value>>>;

/// Graph maintenance a preview owes for the writes since the last one.
#[derive(Clone, Default)]
struct GraphChanges {
    /// Roots stored since, whose cells must be evaluated.
    added_roots: BTreeSet<String>,
    /// Cells that lost a reader or their root, or are new: collected unless
    /// something still holds them.
    released: BTreeSet<String>,
    /// Cells whose derived dependencies changed, so their heights may too, in
    /// write order: evaluation stores children first, so each height is
    /// computed once.
    reshaped: Vec<String>,
}

struct Engine<'a> {
    execute: &'a dyn Executor,
    base: Records,
    staged: Records,
    mode: &'a str,
    principal: Value,
    /// No caller: maintenance, the authorization hook, deployments. Access
    /// policies apply only to invocations with one, even an anonymous one.
    system: bool,
    /// Each collection's policy specialized for this invocation's caller.
    access: HashMap<String, Option<Arc<access::Access>>>,
    now: u64,
    has_time: bool,
    graph_ready: bool,
    rows: Option<Rows>,
    source_index_bytes: usize,
    /// Graph entries this transaction added or replaced, by cell or root ID.
    graph_costs: HashMap<String, usize>,
    graph_index_bytes: usize,
    query_indexes: Vec<QueryIndex>,
    /// Roots the invocation materialized (Some) or unmaterialized (None)
    /// since the last preview applied them.
    root_changes: BTreeMap<String, Option<Arc<Value>>>,
    temporary_root: Option<Reference>,
    /// A root a preview stored only to read `temporary_root`, removed by the
    /// next preview that no longer needs it.
    staged_temporary: Option<String>,
    graph_changes: GraphChanges,
    writes: BTreeMap<String, Option<Arc<Value>>>,
    changed: HashSet<String>,
    unchecked: HashSet<String>,
    preview: Preview,
    schema: Arc<Schema>,
    shadow_schema: Option<Arc<Schema>>,
    deployment_schema: Option<Schema>,
    preview_dirty: bool,
    keys_changed: bool,
    keys_ready: bool,
    evaluated: Vec<String>,
    fatal: Option<EngineError>,
    query_cacheable: bool,
    /// Every read so far is in the certificate. Reading time through
    /// ctx.clock() keeps it: the result declares when time changes it, and
    /// only a write of a read can change it before then.
    certifying: bool,
    /// Read time through ctx.now(), which promises no change time.
    clock_polled: bool,
    /// The earliest declared future instant at which the result may change.
    changes_at: Option<u64>,
    speculative: bool,
    certificate: Option<DependencyCertificate>,
    retained_costs: HashMap<String, usize>,
    retained_bytes: usize,
    retained_limit: usize,
    output_limit: usize,
    index_limit: usize,
    trace_bytes: usize,
    observed_bytes: usize,
}

/// `data` is a structurally shared immutable snapshot, so creating transaction
/// overlays is O(1) and does not copy any stored JSON records.
#[cfg(test)]
pub fn run(
    data: Records,
    invocation: Value,
    mode: &str,
    now: Option<u64>,
    execute: &dyn Executor,
) -> EngineResult<Evaluation> {
    let settings = super::config::settings()
        .map_err(|error| EngineError::new("EVALUATION_BUDGET", error.to_string()))?;
    run_with_limit(
        data,
        invocation,
        mode,
        now,
        execute,
        settings.rust_memory_bytes,
    )
}

/// Deployment supplies a validated schema; ordinary invocations use durable metadata.
pub fn run_with_schema(
    data: Records,
    invocation: Value,
    mode: &str,
    now: Option<u64>,
    execute: &dyn Executor,
    schema: Option<Schema>,
) -> EngineResult<Evaluation> {
    let settings = super::config::settings()
        .map_err(|error| EngineError::new("EVALUATION_BUDGET", error.to_string()))?;
    run_with_limit_and_schema(
        data,
        invocation,
        mode,
        now,
        execute,
        settings.rust_memory_bytes,
        schema,
    )
}

#[cfg(test)]
fn run_with_limit(
    data: Records,
    invocation: Value,
    mode: &str,
    now: Option<u64>,
    execute: &dyn Executor,
    retained_limit: usize,
) -> EngineResult<Evaluation> {
    run_with_limit_and_schema(data, invocation, mode, now, execute, retained_limit, None)
}

fn run_with_limit_and_schema(
    data: Records,
    invocation: Value,
    mode: &str,
    now: Option<u64>,
    execute: &dyn Executor,
    retained_limit: usize,
    deployment_schema: Option<Schema>,
) -> EngineResult<Evaluation> {
    let settings = super::config::settings()
        .map_err(|error| EngineError::new("EVALUATION_BUDGET", error.to_string()))?;
    // FLOWER_TEST_BACKED=1 runs every engine test over records served from
    // a stored snapshot instead of memory.
    #[cfg(test)]
    let data = if std::env::var_os("FLOWER_TEST_BACKED").is_some() {
        data.backed_copy()
    } else {
        data
    };
    let invocation = normalize(invocation, "INPUT_INVALID")?;
    record(&invocation, "invocation", "INPUT_INVALID")?;
    if !data.has_valid_graph_pointer() {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Malformed active reactive graph pointer",
        ));
    }
    let stored_now = match data.get("clock") {
        Some(value) => safe_time(value, "Stored clock")?,
        None => 0,
    };
    if now.is_some_and(|value| value > 9_007_199_254_740_991) {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Time must be a nonnegative safe integer",
        ));
    }
    let graph_clock = data.graph_clocks().try_fold(0, |floor, value| {
        safe_time(value, "Stored graph clock").map(|clock| floor.max(clock))
    })?;
    // A paused build or restarted process may have a newer shadow clock. Both
    // code versions must consume the same monotonic time during dual replay.
    let fixed_now = now.unwrap_or(stored_now).max(stored_now).max(graph_clock);
    let has_time = now.is_some() || fixed_now != stored_now;
    let query_cacheable = mode == "query" && data.reactive().cacheable();
    // A reusable candidate would need certificates for both code versions.
    // Serial preparation keeps the shadow graph and active result indivisible.
    let speculative = mode == "mutation"
        && invocation["$speculate"] == true
        && !super::staging::maintaining_graph(&data);
    // The snapshot's graph is shared, not copied, so it costs this invocation
    // nothing: it pays for the graph entries it adds or replaces.
    let deployment_schema_bytes = deployment_schema
        .as_ref()
        .map_or(0, Schema::allocation_cost);
    if deployment_schema_bytes > retained_limit {
        return Err(EngineError::new(
            "EVALUATION_BUDGET",
            "Index schema exceeds the Rust memory budget",
        ));
    }
    // Immutable metadata keeps its validated typed form across invocations on
    // this worker. Its conservative memory charge still applies to every call.
    let (schema, stored_schema_bytes) = Schema::load_shared(
        data.get_shared("schema"),
        retained_limit - deployment_schema_bytes,
    )?;
    let (shadow_schema, shadow_bytes) = if mode == "mutation" {
        match data.get_shared(super::staging::INDEXES) {
            Some(record) => {
                let (schema, bytes) = Schema::load_shared(
                    Some(record),
                    retained_limit
                        .saturating_sub(deployment_schema_bytes)
                        .saturating_sub(stored_schema_bytes),
                )?;
                (Some(schema), bytes)
            }
            None => (None, 0),
        }
    } else {
        (None, 0)
    };
    let schema_bytes = stored_schema_bytes
        .saturating_add(deployment_schema_bytes)
        .saturating_add(shadow_bytes);
    let deployment_schema = deployment_schema.map(Schema::validate).transpose()?;
    let mut engine = Engine {
        execute,
        base: data.clone(),
        staged: data,
        mode,
        principal: invocation.get("$principal").cloned().unwrap_or(Value::Null),
        system: invocation.get("$principal").is_none(),
        access: HashMap::new(),
        now: fixed_now,
        has_time,
        graph_ready: false,
        rows: None,
        source_index_bytes: 0,
        graph_costs: HashMap::new(),
        graph_index_bytes: 0,
        query_indexes: Vec::new(),
        root_changes: BTreeMap::new(),
        temporary_root: None,
        staged_temporary: None,
        graph_changes: GraphChanges::default(),
        writes: BTreeMap::new(),
        changed: HashSet::new(),
        unchecked: HashSet::new(),
        preview: Preview::default(),
        schema,
        shadow_schema,
        deployment_schema,
        preview_dirty: now.is_some() && (stored_now != fixed_now || stored_now == 0),
        keys_changed: false,
        keys_ready: false,
        evaluated: Vec::new(),
        fatal: None,
        query_cacheable,
        certifying: query_cacheable,
        clock_polled: false,
        changes_at: None,
        speculative,
        certificate: (query_cacheable || speculative).then(DependencyCertificate::default),
        retained_costs: HashMap::new(),
        retained_bytes: schema_bytes,
        retained_limit,
        output_limit: settings.result_max_bytes,
        index_limit: settings.index_memory_bytes,
        trace_bytes: 0,
        observed_bytes: 0,
    };
    for id in [
        "bundle",
        "schema",
        "reactive:active",
        "keyDeclarations",
        "managedKeys",
        "authorizationMethod",
    ] {
        engine.record_read(id);
    }
    // Queries observe only the active graph and schema. Background progress
    // does not change their results; optimistic writes also depend on the
    // deployment lifecycle and the indexes they must maintain.
    if engine.speculative {
        engine.record_read(super::staging::INDEXES);
        engine.record_read(super::staging::JOB);
    }
    engine.preview_dirty =
        engine.has_time && engine.staged.get("clock").and_then(Value::as_u64) != Some(fixed_now);
    if mode == "deployment" || mode == "graph" {
        engine.deploy(&invocation)?;
        engine.finish(Value::Null)
    } else if mode == "keyUpdate" {
        record(
            &invocation["catalog"],
            "managed key catalog",
            "INPUT_INVALID",
        )?;
        engine.put("managedKeys".into(), invocation["catalog"].clone())?;
        engine.keys_changed = true;
        engine.preview_dirty = true;
        engine.run_preview(true, false)?;
        engine.finish(Value::Null)
    } else {
        if mode != "query" && mode != "mutation" && mode != "transaction" {
            return Err(EngineError::new(
                "INPUT_INVALID",
                "Invocation kind must be query, mutation, or transaction",
            ));
        }
        let name = string(&invocation["name"], "invocation.name", "INPUT_INVALID")?;
        let args = invocation.get("args").cloned().unwrap_or(Value::Null);
        let mut host = |operation: &str, arguments: Value| engine.method_host(operation, arguments);
        let value = execute.execute(mode, name, &args, &mut host);
        engine.check_fatal()?;
        let value = value?;
        if allocation_cost(&value) > retained_limit {
            return engine.abort(
                "EVALUATION_BUDGET",
                "Method result exceeds the Rust memory budget",
            );
        }
        let value = normalize(value, "INVALID_VALUE")?;
        if mode == "mutation" {
            engine.run_preview(true, false)?;
        }
        depth(&value, 2, "INPUT_INVALID")?;
        engine.finish(value)
    }
}

/// The reference engine appends these fields only when set:
/// `,"query_clock_polled":true` and `,"query_changes_at":<ms>`.
fn time_fields_len(polled: bool, changes_at: Option<u64>) -> usize {
    usize::from(polled) * 26 + changes_at.map_or(0, |time| 20 + time.to_string().len())
}

fn safe_time(value: &Value, label: &str) -> EngineResult<u64> {
    value
        .as_f64()
        .filter(|number| number.fract() == 0.0 && (0.0..=9_007_199_254_740_991.0).contains(number))
        .map(|number| number as u64)
        .ok_or_else(|| {
            EngineError::new(
                "INPUT_INVALID",
                format!("{label} must be a nonnegative safe integer"),
            )
        })
}

fn reference_name<'a>(reference: &'a Value, kind: &str) -> EngineResult<&'a str> {
    record(reference, "reference", "INVALID_REFERENCE")?;
    if reference["kind"] != kind {
        return Err(EngineError::new(
            "INVALID_REFERENCE",
            format!("Expected a {kind} reference"),
        ));
    }
    string(&reference["name"], "reference.name", "INVALID_REFERENCE")
}

fn list<'a>(value: &'a Value, field: &str) -> EngineResult<&'a [Value]> {
    match value.get(field) {
        None => Ok(&[]),
        Some(Value::Array(values)) => Ok(values),
        _ => Err(EngineError::new(
            "INPUT_INVALID",
            format!("{field} must be an array"),
        )),
    }
}

impl Engine<'_> {
    fn check_fatal(&self) -> EngineResult<()> {
        match &self.fatal {
            Some(error) => Err(error.clone()),
            None => self.execute.check_budget(),
        }
    }

    fn abort<T>(&mut self, code: &str, message: impl Into<String>) -> EngineResult<T> {
        Err(self
            .fatal
            .get_or_insert_with(|| EngineError::new(code, message))
            .clone())
    }

    fn count_operations(&mut self, _count: usize) -> EngineResult<()> {
        // CPU work is bounded by the shared deadline, not an arbitrary number
        // of reads. Retained structures are charged separately as they grow.
        self.check_fatal()
    }

    fn total_retained_bytes(&self) -> usize {
        self.retained_bytes
            .saturating_add(self.source_index_bytes)
            .saturating_add(self.graph_index_bytes)
            .saturating_add(self.observed_bytes)
            .saturating_add(self.trace_bytes)
    }

    fn writable(&mut self) -> EngineResult<()> {
        if self.mode != "mutation" {
            self.abort(
                "QUERY_WRITE_FORBIDDEN",
                "Query methods cannot write or change materialization",
            )
        } else {
            Ok(())
        }
    }

    fn retain(&mut self, id: &str, value: Option<&Value>) -> EngineResult<()> {
        // IDs also occur in changed/dependency/index structures. Reserve space
        // for those copies, as well as the retained JSON tree itself.
        let bytes = 256 + id.len().saturating_mul(8) + value.map_or(0, allocation_cost);
        let previous = self.retained_costs.get(id).copied().unwrap_or(0);
        let retained = self
            .retained_bytes
            .saturating_sub(previous)
            .saturating_add(bytes);
        if retained
            .saturating_add(self.source_index_bytes)
            .saturating_add(self.graph_index_bytes)
            .saturating_add(self.observed_bytes)
            .saturating_add(self.trace_bytes)
            > self.retained_limit
        {
            return self.abort(
                "EVALUATION_BUDGET",
                "Transaction Rust overlay exceeds FLOWER_RUST_MEMORY_BYTES",
            );
        }
        self.retained_bytes = retained;
        self.retained_costs.insert(id.into(), bytes);
        Ok(())
    }

    fn put(&mut self, id: String, value: Value) -> EngineResult<()> {
        if id.starts_with("cell:") {
            let previous = self.staged.get_shared(&id).cloned();
            // Most reevaluations keep their dependencies, and with them the
            // cell's reader records and height.
            let same_deps = previous
                .as_ref()
                .is_some_and(|previous| previous.get("deps") == value.get("deps"));
            if !same_deps {
                if previous
                    .as_ref()
                    .is_none_or(|previous| !cell_edges(previous).eq(cell_edges(&value)))
                {
                    self.graph_changes.reshaped.push(id.clone());
                }
                if previous.is_none() {
                    self.graph_changes.released.insert(id.clone());
                }
                self.update_readers(&id, Some(&value));
            }
        } else if id.starts_with("root:") && !self.staged.contains_key(&id) {
            self.graph_changes.added_roots.insert(id.clone());
        }
        self.write(id, value)
    }

    fn remove(&mut self, id: &str) {
        if id.starts_with("cell:") {
            self.update_readers(id, None);
            let height = Records::height_key(id);
            if self.staged.contains_key(&height) {
                self.delete(&height);
            }
        } else if id.starts_with("root:") {
            self.graph_changes.added_roots.remove(id);
            self.graph_changes
                .released
                .insert(format!("cell:{}", &id[5..]));
        }
        self.delete(id);
    }

    /// A stored cell has one `reader:` record per dependency, written in the
    /// same patch, so propagation can seek a dependency's readers and loading
    /// can rebuild scan windows without reading cells.
    fn update_readers(&mut self, id: &str, next: Option<&Value>) {
        fn edges(cell: &Value) -> impl Iterator<Item = &str> {
            cell["deps"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
        }
        let old: BTreeSet<String> = self
            .staged
            .get(id)
            .map(|cell| edges(cell).map(str::to_owned).collect())
            .unwrap_or_default();
        let new: BTreeSet<&str> = next.map(|cell| edges(cell).collect()).unwrap_or_default();
        for dep in &old {
            if !new.contains(dep.as_str()) {
                self.delete(&Records::reader_key(dep, id));
                if dep.starts_with("cell:") {
                    self.graph_changes.released.insert(dep.clone());
                }
            }
        }
        for dep in new {
            if !old.contains(dep) {
                self.write_structure(Records::reader_key(dep, id), Value::Null);
            }
        }
    }

    /// Reader and height records are a cell's reverse edges and depth, reserved
    /// in its accounted graph bytes rather than charged as overlay writes.
    fn write_structure(&mut self, id: String, value: Value) {
        self.speculative_read(&id);
        self.changed.insert(id.clone());
        self.preview.changed.insert(id.clone());
        self.staged.insert(id, value);
    }

    fn write(&mut self, id: String, value: Value) -> EngineResult<()> {
        self.speculative_read(&id);
        self.retain(&id, Some(&value))?;
        self.changed.insert(id.clone());
        self.preview.changed.insert(id.clone());
        self.staged.insert(id.clone(), value);
        self.charge_graph(&id);
        if self.total_retained_bytes() > self.retained_limit {
            return self.abort(
                "EVALUATION_BUDGET",
                "Graph index changes exceed FLOWER_RUST_MEMORY_BYTES",
            );
        }
        Ok(())
    }

    fn delete(&mut self, id: &str) {
        self.speculative_read(id);
        if let Some(bytes) = self.retained_costs.remove(id) {
            self.retained_bytes = self.retained_bytes.saturating_sub(bytes);
        }
        self.changed.insert(id.into());
        self.preview.changed.insert(id.into());
        self.staged.remove_shared(id);
        self.charge_graph(id);
    }

    /// Charge the graph metadata this transaction allocates for a cell or
    /// root: its entry if the snapshot does not share it, or nothing once it
    /// is removed. The graph inherited from the snapshot is not charged.
    fn charge_graph(&mut self, id: &str) {
        if !id.starts_with("cell:") && !id.starts_with("root:") {
            return;
        }
        let bytes = metadata::ReactiveIndex::unshared_bytes(&self.staged, &self.base, id);
        let previous = if bytes == 0 {
            self.graph_costs.remove(id)
        } else {
            self.graph_costs.insert(id.into(), bytes)
        };
        self.graph_index_bytes = self
            .graph_index_bytes
            .saturating_sub(previous.unwrap_or(0))
            .saturating_add(bytes);
    }

    fn source(&self, id: &str) -> Option<&Arc<Value>> {
        match self.writes.get(id) {
            Some(Some(value)) => Some(value),
            Some(None) => None,
            None => self.staged.get_shared(id),
        }
    }

    fn copy_for_host(&mut self, value: &Value) -> EngineResult<Value> {
        if encoded_len(value) > self.output_limit || allocation_cost(value) > self.retained_limit {
            return self.abort(
                "EVALUATION_BUDGET",
                "Context result exceeds the JSON or Rust memory budget",
            );
        }
        Ok(value.clone())
    }

    fn rows_for_host(&mut self, rows: Vec<(String, Arc<Value>)>) -> EngineResult<Value> {
        let mut bytes = 2usize;
        let mut allocation = 64usize;
        for (index, (key, value)) in rows.iter().enumerate() {
            bytes = bytes.saturating_add(
                17 + usize::from(index != 0) + string_len(key) + encoded_len(value),
            );
            allocation = allocation.saturating_add(256 + key.len() + allocation_cost(value));
            if bytes > self.output_limit || allocation > self.retained_limit {
                return self.abort(
                    "EVALUATION_BUDGET",
                    "Context result exceeds the JSON or Rust memory budget",
                );
            }
        }
        Ok(rows_value(rows))
    }

    fn write_source(
        &mut self,
        collection: &str,
        key: &str,
        value: Option<Value>,
    ) -> EngineResult<()> {
        let id = source_id(collection, key);
        self.speculative_read(&id);
        self.retain(&id, value.as_ref())?;
        let value = value.map(Arc::new);
        let previous = match self.writes.get(&id) {
            Some(value) => value.clone(),
            None => self.staged.get_shared(&id).cloned(),
        };
        let used = self
            .query_indexes
            .iter()
            .map(|index| index.bytes)
            .sum::<usize>();
        let mut available = self.index_limit.saturating_sub(used);
        self.query_indexes.retain_mut(|index| {
            let before = index.bytes;
            index.limit = before.saturating_add(available);
            let keep = index.collection != collection
                || index.update(key, previous.as_deref(), value.as_ref());
            available =
                available
                    .saturating_add(before)
                    .saturating_sub(if keep { index.bytes } else { 0 });
            keep
        });
        if let Some(collection_rows) = self.rows.as_mut().and_then(|rows| rows.get_mut(collection))
        {
            if let Some(value) = &value {
                if !collection_rows.contains_key(&Key(key.into())) {
                    self.source_index_bytes += key.len() + 128;
                }
                collection_rows.insert(Key(key.into()), value.clone());
            } else {
                if collection_rows.remove(&Key(key.into())).is_some() {
                    self.source_index_bytes -= key.len() + 128;
                }
            }
        }
        if self
            .retained_bytes
            .saturating_add(self.source_index_bytes)
            .saturating_add(self.graph_index_bytes)
            .saturating_add(self.observed_bytes)
            .saturating_add(self.trace_bytes)
            > self.retained_limit
        {
            return self.abort(
                "EVALUATION_BUDGET",
                "Transaction Rust overlay exceeds FLOWER_RUST_MEMORY_BYTES",
            );
        }
        self.writes.insert(id, value);
        self.preview_dirty = true;
        Ok(())
    }

    fn collection_rows(&mut self, collection: &str) -> EngineResult<Vec<(String, Arc<Value>)>> {
        self.init_rows(collection)?;
        Ok(self
            .rows
            .as_ref()
            .and_then(|rows| rows.get(collection))
            .map(|rows| {
                rows.iter()
                    .map(|(key, value)| (key.0.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn init_rows(&mut self, collection: &str) -> EngineResult<()> {
        if self
            .rows
            .as_ref()
            .is_some_and(|rows| rows.contains_key(collection))
        {
            return Ok(());
        }
        let prefix = format!(
            "source:[{},",
            serde_json::to_string(collection).expect("collection encodes")
        );
        let mut rows = BTreeMap::new();
        let mut bytes = 0usize;
        // The persistent map's range lookup avoids traversing unrelated source
        // records. UTF-16 ordering is applied only to this collection's row keys.
        for (index, (id, stored)) in self
            .staged
            .range_shared((
                std::ops::Bound::Included(prefix.as_str()),
                std::ops::Bound::Unbounded,
            ))
            .enumerate()
        {
            if index % 64 == 0 {
                self.check_fatal()?;
            }
            if !id.starts_with(&prefix) {
                break;
            }
            let value = match self.writes.get(id) {
                Some(None) => continue,
                Some(Some(value)) => value,
                None => stored,
            };
            let (_, key) = source_pair(id)?;
            bytes += key.len() + 128;
            if self
                .retained_bytes
                .saturating_add(self.source_index_bytes)
                .saturating_add(self.graph_index_bytes)
                .saturating_add(self.observed_bytes)
                .saturating_add(self.trace_bytes)
                .saturating_add(bytes)
                > self.retained_limit
            {
                return Err(self
                    .fatal
                    .get_or_insert_with(|| {
                        EngineError::new(
                            "EVALUATION_BUDGET",
                            "Collection index exceeds the Rust memory budget",
                        )
                    })
                    .clone());
            }
            rows.insert(Key(key), value.clone());
        }
        for (index, (id, value)) in self.writes.range(prefix.clone()..).enumerate() {
            if index % 64 == 0 {
                self.check_fatal()?;
            }
            if !id.starts_with(&prefix) {
                break;
            }
            if self.staged.get(id).is_some() {
                continue;
            }
            if let Some(value) = value {
                let (_, key) = source_pair(id)?;
                bytes += key.len() + 128;
                if self
                    .retained_bytes
                    .saturating_add(self.source_index_bytes)
                    .saturating_add(self.graph_index_bytes)
                    .saturating_add(self.observed_bytes)
                    .saturating_add(self.trace_bytes)
                    .saturating_add(bytes)
                    > self.retained_limit
                {
                    return Err(self
                        .fatal
                        .get_or_insert_with(|| {
                            EngineError::new(
                                "EVALUATION_BUDGET",
                                "Collection index exceeds the Rust memory budget",
                            )
                        })
                        .clone());
                }
                rows.insert(Key(key), value.clone());
            }
        }
        self.source_index_bytes += bytes;
        self.rows
            .get_or_insert_with(HashMap::new)
            .insert(collection.into(), rows);
        Ok(())
    }

    fn query_rows(&mut self, query: &Query, derived: bool) -> EngineResult<Value> {
        // Derived values have no caller; methods see what their caller may.
        let access = if derived {
            None
        } else {
            self.read_access(&query.collection)
        };
        if self.has_index(query) {
            self.marker_read(indexes::bucket_id(
                &query.collection,
                &query.fields,
                &query.expected,
            ));
            let rows = self.indexed_rows(query)?;
            for (key, _) in &rows {
                self.record_read(source_id(&query.collection, key));
            }
            if derived {
                self.count_reads(1 + rows.len())?;
            }
            self.count_operations(rows.len())?;
            let rows = match &access {
                Some(access) => {
                    let rows = access.filter(rows, &query.fields);
                    self.settle_access(access)?;
                    rows
                }
                None => rows,
            };
            let values: Vec<_> = rows.into_iter().map(|(_, value)| value).collect();
            return self.query_values(values);
        }
        self.marker_read(collection_id(&query.collection));
        self.init_rows(&query.collection)?;
        let count = self
            .rows
            .as_ref()
            .and_then(|rows| rows.get(&query.collection))
            .map_or(0, BTreeMap::len);
        // The read budget remains a collection read, even when the physical
        // lookup touches only an equality bucket. Certificates stamp the whole
        // collection: undeclared buckets have no markers.
        if derived {
            self.count_reads(1 + count)?;
        }
        if self.mode != "deployment" {
            self.count_operations(count)?;
        }
        let position = self
            .query_indexes
            .iter()
            .position(|index| index.collection == query.collection && index.fields == query.fields);
        let position = if let Some(position) = position {
            Some(position)
        } else {
            let available = self.index_limit.saturating_sub(
                self.query_indexes
                    .iter()
                    .map(|index| index.bytes)
                    .sum::<usize>(),
            );
            let mut index = QueryIndex {
                collection: query.collection.clone(),
                fields: query.fields.clone(),
                buckets: HashMap::new(),
                limit: available,
                bytes: 256
                    + query.collection.len()
                    + query
                        .fields
                        .iter()
                        .map(|field| 64 + field.len())
                        .sum::<usize>(),
            };
            let built = index.bytes <= available
                && self
                    .rows
                    .as_ref()
                    .and_then(|rows| rows.get(&query.collection))
                    .is_none_or(|rows| {
                        rows.iter()
                            .all(|(key, value)| index.update(&key.0, None, Some(value)))
                    });
            if built {
                self.query_indexes.push(index);
                Some(self.query_indexes.len() - 1)
            } else {
                None
            }
        };
        let rows: Vec<(String, Arc<Value>)> = if let Some(position) = position {
            self.query_indexes[position]
                .buckets
                .get(&canonical_json(&query.expected))
                .map(|rows| {
                    rows.iter()
                        .map(|(key, value)| (key.0.clone(), value.clone()))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            self.rows
                .as_ref()
                .and_then(|rows| rows.get(&query.collection))
                .map(|rows| {
                    rows.iter()
                        .filter(|(_, value)| query.matches(value))
                        .map(|(key, value)| (key.0.clone(), value.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let rows = match &access {
            Some(access) => {
                let rows = access.filter(rows, &query.fields);
                self.settle_access(access)?;
                rows
            }
            None => rows,
        };
        self.query_values(rows.into_iter().map(|(_, value)| value).collect())
    }

    fn query_values(&mut self, values: Vec<Arc<Value>>) -> EngineResult<Value> {
        let mut bytes = 2usize;
        let mut allocation = 64usize;
        for (index, value) in values.iter().enumerate() {
            bytes = bytes.saturating_add(usize::from(index != 0) + encoded_len(value));
            allocation = allocation.saturating_add(allocation_cost(value));
            if bytes > self.output_limit || allocation > self.retained_limit {
                return self.abort(
                    "EVALUATION_BUDGET",
                    "Context result exceeds the JSON or Rust memory budget",
                );
            }
        }
        Ok(Value::Array(
            values.into_iter().map(|value| (*value).clone()).collect(),
        ))
    }

    fn method_host(&mut self, operation: &str, arguments: Value) -> EngineResult<Value> {
        self.check_fatal()?;
        if self.mode == "transaction" {
            return self.abort(
                "TRANSACTION_PLAN_ONLY",
                "Transaction planners cannot access database context",
            );
        }
        self.count_operations(1)?;
        let Value::Array(mut arguments) = arguments else {
            return Err(EngineError::new(
                "INVALID_VALUE",
                "Host arguments must be an array",
            ));
        };
        let argument = |index: usize| arguments.get(index).unwrap_or(&Value::Null);
        match operation {
            "principal" => Ok(self.principal.clone()),
            "history" => {
                self.marker_read(crate::consensus::HISTORY_MARKER);
                Ok(self.base.get("$flower.retention").map_or(Value::Null, |state|json!({"database":state["database"],"incarnation":state["incarnation"]})))
            }
            "managedKey" => self.managed_key(argument(0)),
            "now" => {
                self.query_cacheable = false;
                self.certifying = false;
                self.clock_polled = true;
                Ok(json!(self.now))
            }
            "clock" => {
                self.query_cacheable = false;
                Ok(json!(self.now))
            }
            "changesAt" => {
                self.declare_change(argument(0))?;
                Ok(Value::Null)
            }
            "get" => {
                let reference = argument(0);
                record(reference, "reference", "INVALID_REFERENCE")?;
                if reference["kind"] == "collection" {
                    let collection = reference_name(reference, "collection")?;
                    let key = string(argument(1), "source key", "INVALID_REFERENCE")?;
                    let id = source_id(collection, key);
                    self.record_read(&id);
                    let value = self.source(&id).cloned();
                    match (value, self.read_access(collection)) {
                        (Some(value), None) => self.copy_for_host(&value),
                        (Some(value), Some(access)) => {
                            let shown = access
                                .visible(key, &value, &[])
                                .then(|| access.redact(key, &value));
                            self.settle_access(&access)?;
                            match shown {
                                Some(shown) => self.copy_for_host(&shown),
                                // A hidden row reads as absent.
                                None => Ok(Value::Null),
                            }
                        }
                        (None, _) => Ok(Value::Null),
                    }
                } else {
                    let name = reference_name(reference, "derived")?.to_owned();
                    let args = normalize(argument(1).clone(), "INVALID_VALUE")?;
                    self.temporary_root = Some(Reference {
                        name: name.clone(),
                        args: args.clone(),
                    });
                    self.run_preview(false, false)?;
                    let id = cell_id(&name, &args);
                    self.check_materialized_keys(&id)?;
                    self.derived_read(id.clone());
                    let cell = self.staged.get(&id).ok_or_else(|| {
                        EngineError::new("INPUT_INVALID", "Missing evaluated cell")
                    })?;
                    outcome_value(&cell["outcome"])
                }
            }
            "scan" => {
                if let Some(options) = arguments.get(1) {
                    let query = ranges::RangeQuery::parse_scan(argument(0), options)?;
                    let rows = self.range_rows(&query, true)?.rows;
                    self.count_operations(rows.as_array().expect("scan rows").len())?;
                    return Ok(rows);
                }
                let collection = reference_name(argument(0), "collection")?;
                self.marker_read(collection_id(collection));
                let rows = self.collection_rows(collection)?;
                self.count_operations(rows.len())?;
                let rows = match self.read_access(collection) {
                    Some(access) => {
                        let rows = access.filter(rows, &[]);
                        self.settle_access(&access)?;
                        rows
                    }
                    None => rows,
                };
                self.rows_for_host(rows)
            }
            "range" => {
                let query = ranges::RangeQuery::parse(argument(0))?;
                Ok(self.range_rows(&query, true)?.rows)
            }
            "query" => {
                let query = Query::parse(argument(0))?;
                self.query_rows(&query, false)
            }
            "set" | "delete" => {
                self.writable()?;
                // References and the written value occupy disjoint arguments.
                // Move the value out of the decoded host payload rather than
                // cloning its entire JSON tree before normalization.
                let prefix_len = arguments.len().min(2);
                let (reference, values) = arguments.split_at_mut(prefix_len);
                let collection =
                    reference_name(reference.first().unwrap_or(&Value::Null), "collection")?;
                let key = string(
                    reference.get(1).unwrap_or(&Value::Null),
                    "source key",
                    "INVALID_REFERENCE",
                )?;
                let value = if operation == "set" {
                    let value = values.first_mut().ok_or_else(|| {
                        EngineError::new("INVALID_VALUE", "Value is not valid JSON")
                    })?;
                    Some(normalize(value.take(), "INVALID_VALUE")?)
                } else {
                    None
                };
                let value = match self.write_access(collection) {
                    None => value,
                    Some(access) => {
                        // The decision depends on the stored row: an optimistic
                        // mutation must conflict with a concurrent change to it.
                        let id = source_id(collection, key);
                        self.record_read(&id);
                        let previous = self.source(&id).cloned();
                        let admitted = access.admit(key, previous.as_deref(), value);
                        self.settle_access(&access)?;
                        admitted?
                    }
                };
                self.write_source(collection, key, value)?;
                Ok(Value::Null)
            }
            "materialize" | "unmaterialize" => {
                self.writable()?;
                let name = reference_name(argument(0), "derived")?.into();
                let args = normalize(argument(1).clone(), "INVALID_VALUE")?;
                let reference = Reference { name, args };
                let id = reference.root_id();
                if operation == "materialize" {
                    let value = reference.value();
                    self.retain(&id, Some(&value))?;
                    self.root_changes.insert(id, Some(Arc::new(value)));
                } else {
                    self.root_changes.insert(id, None);
                }
                self.preview_dirty = true;
                Ok(Value::Null)
            }
            _ => Err(EngineError::new(
                "INVALID_REFERENCE",
                format!("Unknown context operation: {operation}"),
            )),
        }
    }

    /// Persisted outcomes can avoid invoking crypto. Follow the reused cell's
    /// dependencies before admitting it; unrelated source reads need no keys.
    fn check_materialized_keys(&mut self, root: &str) -> EngineResult<()> {
        if self.keys_ready
            || matches!(self.mode, "deployment" | "keyUpdate")
            || !self.staged.has_readers("managedKeys")
        {
            return Ok(());
        }
        let mut pending = vec![root.to_owned()];
        let mut seen = HashSet::new();
        let mut bytes = root.len() + 128;
        while let Some(id) = pending.pop() {
            self.check_fatal()?;
            if !seen.insert(id.clone()) {
                continue;
            }
            if let Some(cell) = self.staged.get(&id)
                && let Some(deps) = cell["deps"].as_array()
            {
                if deps.iter().any(|dep| dep == "managedKeys") {
                    let catalog = self.staged.get("managedKeys").ok_or_else(|| {
                        EngineError::new("KEY_UNAVAILABLE", "No managed key catalog")
                    })?;
                    crate::crypto::managed::validate_ready(catalog)
                        .map_err(|error| EngineError::new("KEY_UNAVAILABLE", error.to_string()))?;
                    // Found once per invocation, and uncached from then on, as
                    // resolving the key itself would be.
                    self.query_cacheable = false;
                    self.keys_ready = true;
                    return Ok(());
                }
                for dep in deps
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|dep| dep.starts_with("cell:"))
                {
                    bytes = bytes.saturating_add(dep.len() + 128);
                    if bytes > self.retained_limit {
                        return self.abort(
                            "EVALUATION_BUDGET",
                            "Key dependency traversal exceeds the Rust memory budget",
                        );
                    }
                    pending.push(dep.into());
                }
            }
        }
        Ok(())
    }

    /// ctx.changesAt(time): the result may change when the clock reaches time.
    /// Past instants and null declare nothing; the earliest future one wins.
    pub(super) fn declare_change(&mut self, time: &Value) -> EngineResult<()> {
        if time.is_null() {
            return Ok(());
        }
        let Some(time) = time.as_f64().filter(|time| time.is_finite()) else {
            return Err(EngineError::new(
                "INVALID_VALUE",
                "changesAt takes a finite number of milliseconds or null",
            ));
        };
        if time > self.now as f64 && self.mode != "deployment" {
            let time = time.ceil().min(9_007_199_254_740_991.0) as u64;
            self.changes_at = Some(self.changes_at.map_or(time, |earlier| earlier.min(time)));
        }
        Ok(())
    }

    fn managed_key(&mut self, request: &Value) -> EngineResult<Value> {
        // Secret-dependent results are never cached. Every certificate already
        // observes the declarations and catalog, so only a write changes them.
        self.query_cacheable = false;
        record(request, "managed key request", "KEY_INVALID")?;
        let declaration = &request["key"];
        let declared = self
            .staged
            .get("keyDeclarations")
            .and_then(Value::as_array)
            .is_some_and(|keys| keys.iter().any(|key| key == declaration));
        if !declared {
            return Err(EngineError::new(
                "KEY_FORBIDDEN",
                "Key must match a declaration in define({keys})",
            ));
        }
        let operation = string(&request["operation"], "key operation", "KEY_INVALID")?;
        let kid = match request.get("kid") {
            None | Some(Value::Null) => None,
            Some(value) => Some(string(value, "key version", "KEY_INVALID")?),
        };
        let catalog = self.staged.get_shared("managedKeys").ok_or_else(|| {
            EngineError::new("KEY_UNAVAILABLE", "No managed keys have been provisioned")
        })?;
        let resolved = crate::crypto::managed::resolve_shared(catalog, declaration, operation, kid)
            .map_err(|error| EngineError::new("KEY_FORBIDDEN", error.to_string()))?;
        if allocation_cost(&resolved) > self.retained_limit {
            return self.abort(
                "EVALUATION_BUDGET",
                "Key metadata exceeds the Rust memory budget",
            );
        }
        Ok(resolved)
    }

    fn deploy(&mut self, command: &Value) -> EngineResult<()> {
        string(&command["requestId"], "requestId", "INPUT_INVALID")?;
        // The deployment command has the same wrapper-depth constraints as the
        // previous trusted engine's normalizer.
        depth(command, 0, "INPUT_INVALID")?;
        self.init_graph()?;
        let graph = self.mode == "graph";
        let staged = command["$stagedDeployment"] == true;
        if !graph && let Some(job) = self.staged.get(super::staging::JOB) {
            if staged {
                if job["phase"] != "ready" || job["requestId"] != command["requestId"] {
                    return Err(EngineError::new(
                        "DEPLOYMENT_CONFLICT",
                        "Staged deployment is not ready for this request",
                    ));
                }
            } else if job["phase"] != "collected" {
                return Err(EngineError::new(
                    "DEPLOYMENT_CONFLICT",
                    "Resolve and collect the staged deployment before deploying different code",
                ));
            }
        } else if !graph && staged {
            return Err(EngineError::new(
                "DEPLOYMENT_CONFLICT",
                "Staged deployment metadata is missing",
            ));
        }
        let schema_changed = self.install_schema(staged || graph)?;
        if let Some(keys) = command.get("keyDeclarations")
            && (keys.as_array().is_none_or(|keys| !keys.is_empty())
                || self.staged.contains_key("keyDeclarations"))
        {
            self.put("keyDeclarations".into(), keys.clone())?;
        }
        for write in list(command, "writes")? {
            record(write, "write", "INPUT_INVALID")?;
            let collection = string(&write["collection"], "write.collection", "INPUT_INVALID")?;
            let key = string(&write["key"], "write.key", "INPUT_INVALID")?;
            let value = if let Some(delete) = write.get("delete") {
                if delete != true {
                    return Err(EngineError::new(
                        "INPUT_INVALID",
                        "write.delete must be true",
                    ));
                }
                if write.get("value").is_some() {
                    return Err(EngineError::new(
                        "INPUT_INVALID",
                        "A write cannot both set and delete a value",
                    ));
                }
                None
            } else {
                Some(write.get("value").cloned().ok_or_else(|| {
                    EngineError::new(
                        "INPUT_INVALID",
                        "A write must provide a value or delete:true",
                    )
                })?)
            };
            self.write_source(collection, key, value)?;
        }
        let mut bundle_changed = schema_changed;
        if let Some(bundle) = command.get("bundle") {
            record(bundle, "bundle", "INPUT_INVALID")?;
            string(&bundle["hash"], "bundle.hash", "INPUT_INVALID")?;
            match (bundle.get("javascript"), bundle.get("wasm")) {
                (Some(code), None) => string(code, "bundle.javascript", "INPUT_INVALID")?,
                (None, Some(code)) => string(code, "bundle.wasm", "INPUT_INVALID")?,
                _ => {
                    return Err(EngineError::new(
                        "INPUT_INVALID",
                        "bundle must carry either javascript or wasm",
                    ));
                }
            };
            if self
                .staged
                .get("bundle")
                .is_none_or(|previous| !equal(previous, bundle))
            {
                self.put("bundle".into(), bundle.clone())?;
                bundle_changed = true;
            }
        }
        for value in list(command, "materialize")? {
            let reference = Reference::parse(value, "materialize")?;
            let id = reference.root_id();
            let value = reference.value();
            self.put(id.clone(), value)?;
        }
        for value in list(command, "unmaterialize")? {
            let reference = Reference::parse(value, "unmaterialize")?;
            self.root_changes.insert(reference.root_id(), None);
        }
        if graph {
            // These roots already belong to the target program. Updating the
            // virtual manifest must not invalidate every completed root again.
            bundle_changed = false;
            self.keys_changed = command["$keysChanged"] == true;
            if self.staged.has_readers("managedKeys") {
                let catalog = self
                    .staged
                    .get("managedKeys")
                    .ok_or_else(|| EngineError::new("KEY_UNAVAILABLE", "No managed key catalog"))?;
                crate::crypto::managed::validate_ready(catalog)
                    .map_err(|error| EngineError::new("KEY_UNAVAILABLE", error.to_string()))?;
            }
        }
        self.preview_dirty = true;
        self.run_preview(true, bundle_changed)
    }

    fn finish(mut self, value: Value) -> EngineResult<Evaluation> {
        self.check_fatal()?;
        let (puts, deletes) = if self.mode == "query" {
            (Vec::new(), Vec::new())
        } else {
            shared_changes(&self.base, &self.staged, &self.changed)
        };
        if self.mode == "mutation" {
            for (_, value) in &puts {
                depth(value, 3, "INPUT_INVALID")?;
            }
        }
        let (certificate, mutation_certificate) = if self.speculative {
            (
                None,
                self.certificate
                    .take()
                    .map(|reads| MutationCertificate::new(reads, &self.base, self.now)),
            )
        } else {
            (
                self.certificate
                    .take()
                    .filter(|certificate| self.certifying && certificate.valid(&self.base)),
                None,
            )
        };
        let evaluated = std::mem::take(&mut self.evaluated);
        let query_cacheable = self.query_cacheable && certificate.is_some();
        let (query_clock_polled, query_changes_at) = (self.clock_polled, self.changes_at);
        let mode = self.mode;
        let output_limit = self.output_limit;
        // Newly staged values normally belong only to this private engine.
        // Release all overlay/preview references before forming the owned
        // patch, moving their JSON trees rather than cloning them a final time.
        // Values still shared with a prior snapshot retain the copy fallback.
        drop(self);
        let result = Evaluation {
            puts: puts
                .into_iter()
                .map(|(id, value)| (id, Arc::unwrap_or_clone(value)))
                .collect(),
            deletes,
            evaluated,
            value,
            query_cacheable,
            query_clock_polled,
            query_changes_at,
            query_certificate: certificate,
            mutation_certificate,
        };
        let bytes = patch_bytes(&result.puts, &result.deletes, &result.evaluated)
            + if mode == "deployment" {
                0
            } else {
                // Both properties include their leading comma, key and colon.
                9 + encoded_len(&result.value) + 19 + if result.query_cacheable { 4 } else { 5 }
                    + time_fields_len(result.query_clock_polled, result.query_changes_at)
            };
        if bytes > output_limit {
            return Err(EngineError::new(
                "EVALUATION_BUDGET",
                "Output exceeds FLOWER_RESULT_MAX_BYTES",
            ));
        }
        Ok(result)
    }
}

fn patch_bytes(puts: &BTreeMap<String, Value>, deletes: &[String], evaluated: &[String]) -> usize {
    let object = 2
        + puts.len().saturating_sub(1)
        + puts
            .iter()
            .map(|(key, value)| string_len(key) + 1 + encoded_len(value))
            .sum::<usize>();
    let strings = |values: &[String]| {
        2 + values.len().saturating_sub(1)
            + values.iter().map(|value| string_len(value)).sum::<usize>()
    };
    // {"puts":...,"deletes":...,"evaluated":...}
    33 + object + strings(deletes) + strings(evaluated)
}

/// A stored cell's dependencies on other cells, in stored order.
fn cell_edges(cell: &Value) -> impl Iterator<Item = &str> {
    cell["deps"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|dep| dep.starts_with("cell:"))
}

fn shared_changes(
    base: &Records,
    staged: &Records,
    changed: &HashSet<String>,
) -> (Vec<(String, Arc<Value>)>, Vec<String>) {
    // Ordering belongs to the final owned patch. A temporary map here would
    // build and discard a second tree just to hold these short-lived handles.
    let mut puts = Vec::with_capacity(changed.len());
    let mut deletes = Vec::new();
    for id in changed {
        match (base.get(id), staged.get_shared(id)) {
            (previous, Some(value)) if previous.is_none_or(|previous| !equal(previous, value)) => {
                puts.push((staged.graph_key(id), value.clone()));
            }
            (Some(_), None) => deletes.push(staged.graph_key(id)),
            _ => {}
        }
    }
    deletes.sort_unstable_by(|left, right| compare(left, right));
    (puts, deletes)
}

#[cfg(test)]
fn changes(
    base: &Records,
    staged: &Records,
    changed: &HashSet<String>,
) -> (BTreeMap<String, Value>, Vec<String>) {
    let (puts, deletes) = shared_changes(base, staged, changed);
    (
        puts.into_iter()
            .map(|(id, value)| (id, (*value).clone()))
            .collect(),
        deletes,
    )
}

fn outcome_value(outcome: &Value) -> EngineResult<Value> {
    if outcome["ok"] == true {
        Ok(outcome["value"].clone())
    } else {
        Err(EngineError::new(
            outcome["error"]["code"].as_str().unwrap_or("COMPUTE_ERROR"),
            outcome["error"]["message"]
                .as_str()
                .unwrap_or("evaluation failed"),
        ))
    }
}

fn rows_value(rows: Vec<(String, Arc<Value>)>) -> Value {
    Value::Array(
        rows.into_iter()
            .map(|(key, value)| json!({"key":key,"value":*value}))
            .collect(),
    )
}

struct Query {
    collection: String,
    fields: Vec<String>,
    expected: Value,
}

struct QueryIndex {
    collection: String,
    fields: Vec<String>,
    buckets: HashMap<String, BTreeMap<Key, Arc<Value>>>,
    bytes: usize,
    limit: usize,
}

impl QueryIndex {
    fn key(&self, value: &Value) -> Option<String> {
        let record = value.as_object()?;
        if self.fields.len() == 1 {
            Some(canonical_json(record.get(&self.fields[0])?))
        } else {
            let values = self
                .fields
                .iter()
                .map(|field| record.get(field).cloned())
                .collect::<Option<Vec<_>>>()?;
            Some(canonical_json(&Value::Array(values)))
        }
    }
    fn update(&mut self, key: &str, previous: Option<&Value>, value: Option<&Arc<Value>>) -> bool {
        if let Some(bucket) = previous.and_then(|value| self.key(value))
            && let Some(rows) = self.buckets.get_mut(&bucket)
        {
            if rows.remove(&Key(key.into())).is_some() {
                self.bytes = self.bytes.saturating_sub(bucket.len() + key.len() + 128);
            }
            if rows.is_empty() {
                self.buckets.remove(&bucket);
            }
        }
        if let Some(value) = value
            && let Some(bucket) = self.key(value)
        {
            self.bytes = self.bytes.saturating_add(bucket.len() + key.len() + 128);
            // All indexes share the operator's separate memory budget. An
            // oversized index falls back to scanning borrowed Rust values.
            if self.bytes > self.limit {
                return false;
            }
            self.buckets
                .entry(bucket)
                .or_default()
                .insert(Key(key.into()), value.clone());
        }
        true
    }
}

impl Query {
    fn matches(&self, value: &Value) -> bool {
        value.as_object().is_some_and(|record| {
            self.fields.iter().enumerate().all(|(index, field)| {
                record.get(field).is_some_and(|value| {
                    equal(
                        value,
                        if self.fields.len() == 1 {
                            &self.expected
                        } else {
                            &self.expected[index]
                        },
                    )
                })
            })
        })
    }
    fn parse(reference: &Value) -> EngineResult<Self> {
        record(reference, "query reference", "INVALID_REFERENCE")?;
        if reference["kind"] != "query" {
            return Err(EngineError::new(
                "INVALID_REFERENCE",
                "Expected a query reference",
            ));
        }
        let collection = match &reference["collection"] {
            Value::String(name) => name.as_str(),
            value => reference_name(value, "collection")?,
        }
        .into();
        let fields = reference["fields"]
            .as_array()
            .filter(|fields| !fields.is_empty())
            .and_then(|fields| {
                fields
                    .iter()
                    .map(|field| field.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
            })
            .ok_or_else(|| {
                EngineError::new(
                    "INVALID_REFERENCE",
                    "Query fields must be a nonempty array of strings",
                )
            })?;
        let expected = normalize(
            reference
                .get("value")
                .cloned()
                .ok_or_else(|| EngineError::new("INVALID_VALUE", "Value is not valid JSON"))?,
            "INVALID_VALUE",
        )?;
        if fields.len() > 1
            && expected
                .as_array()
                .is_none_or(|values| values.len() != fields.len())
        {
            return Err(EngineError::new(
                "INVALID_REFERENCE",
                "Composite query value must match the index field count",
            ));
        }
        Ok(Self {
            collection,
            fields,
            expected,
        })
    }
}
