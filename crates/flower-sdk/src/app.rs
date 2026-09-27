//! `define()` for Rust guests: the application's definitions, public HTTP
//! allowlist and components, the manifest they produce, and the maintenance
//! and materialization callbacks the SDK generates for them.
use crate::{
    Failure, Map, Result, Value,
    context::{Change, Collection, Ctx, Kind},
    fail, fail_with, json, object,
    schema::{Schema, is_safe_integer, pow2},
    type_error,
};
use alloc::{format, string::String, vec, vec::Vec};

pub type Callback = fn(&mut Ctx, Value) -> Result<Value>;

/// A query or mutation callback with an optional argument schema.
pub struct Method {
    pub name: &'static str,
    pub args: Option<&'static Schema>,
    pub run: Callback,
}

impl Method {
    pub const fn new(name: &'static str, run: Callback) -> Self {
        Method {
            name,
            args: None,
            run,
        }
    }
    pub const fn args(self, schema: &'static Schema) -> Self {
        Method {
            args: Some(schema),
            ..self
        }
    }
    /// Validate the arguments, then run.
    pub fn compute(&self, ctx: &mut Ctx, args: Value) -> Result<Value> {
        if let Some(schema) = self.args
            && let Err(error) = schema.parse(&args)
        {
            return fail_with(
                "INVALID_ARGUMENT",
                error.message(),
                object! {"path" => error.path_value()},
            );
        }
        (self.run)(ctx, args)
    }
}

/// Maintain one accumulator per equality group of an index from row deltas.
/// `add` and `remove` must be deterministic, order-independent inverses.
pub struct Aggregate {
    pub source: &'static Collection,
    pub index: &'static str,
    pub initial: fn(&Value) -> Result<Value>,
    /// (accumulator, row, key, group)
    pub add: fn(Value, &Value, &Value, &Value) -> Result<Value>,
    pub remove: fn(Value, &Value, &Value, &Value) -> Result<Value>,
}

impl Aggregate {
    fn fields(&self) -> &'static [&'static str] {
        self.source.by(self.index).fields
    }
    fn compute(&self, update: Value) -> Result<Value> {
        let group = update.get("group");
        let mut value = if update.get("initialize") == &Value::Bool(true) {
            (self.initial)(group)?
        } else {
            update.get("previous").clone()
        };
        for change in update.get("changes").as_array().into_iter().flatten() {
            let key = self
                .source
                .decode_key(change.text("key").unwrap_or_default());
            if change.has("old") {
                value = (self.remove)(value, change.get("old"), &key, group)?;
            }
            if change.has("new") {
                value = (self.add)(value, change.get("new"), &key, group)?;
            }
        }
        Ok(value)
    }
}

#[derive(Clone, Copy)]
pub enum Compute {
    /// A pure function of database state and its arguments.
    Function(Callback),
    Aggregate(&'static Aggregate),
}

#[derive(Clone, Copy)]
pub enum Materialize {
    Never,
    /// One argless instance.
    Always,
    /// One instance per row of the collection, keyed like it.
    Each(&'static Collection),
}

/// A reactive derived value.
pub struct Derived {
    pub name: &'static str,
    pub compute: Compute,
    pub materialize: Materialize,
}

impl Derived {
    pub const fn new(name: &'static str, compute: Callback) -> Self {
        Derived {
            name,
            compute: Compute::Function(compute),
            materialize: Materialize::Never,
        }
    }
    pub const fn aggregate(name: &'static str, aggregate: &'static Aggregate) -> Self {
        Derived {
            name,
            compute: Compute::Aggregate(aggregate),
            materialize: Materialize::Never,
        }
    }
    pub const fn materialize(self, policy: Materialize) -> Self {
        Derived {
            materialize: policy,
            ..self
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    Linearizable,
    /// Replica-local reads may lag without bound.
    ReplicaLocal,
}

#[derive(Clone, Copy)]
pub enum Definition {
    Derived(&'static Derived),
    Query(&'static Method, Consistency),
    Mutation(&'static Method),
}

impl Definition {
    pub fn name(&self) -> &'static str {
        match self {
            Definition::Derived(derived) => derived.name,
            Definition::Query(method, _) | Definition::Mutation(method) => method.name,
        }
    }
    fn kind(&self) -> Kind {
        match self {
            Definition::Derived(_) => Kind::Derived,
            Definition::Query(..) => Kind::Query,
            Definition::Mutation(_) => Kind::Mutation,
        }
    }
}

/// Runs inside every mutation that changes a row of `source`, once per changed key.
pub struct Trigger {
    pub name: &'static str,
    pub source: &'static Collection,
    pub run: fn(&mut Ctx, &Change) -> Result<()>,
}

pub struct TaskFailure {
    /// `{code, message, details?}`
    pub error: Value,
    pub failed_at: f64,
}

/// Maintenance work: the host runs it between writes whenever it is due.
pub trait Task: Sync {
    fn name(&self) -> String;
    /// The earliest time this task has work, or None when idle. Must depend only on data and time.
    fn due(&self, ctx: &mut Ctx) -> Result<Option<f64>>;
    /// Perform one bounded unit of work. It stays eligible while due() remains in the past.
    fn run(&self, ctx: &mut Ctx) -> Result<Value>;
    /// Runs against the failed invocation's snapshot and time. Without it, the task backs off.
    fn on_error(&self, _ctx: &mut Ctx, _failure: &TaskFailure) -> Option<Result<Value>> {
        None
    }
}

/// Collections, triggers and tasks packaged for `App::uses`.
pub trait Component: Sync {
    fn collections(&'static self) -> Vec<&'static Collection>;
    fn tasks(&'static self) -> Vec<&'static dyn Task>;
}

/// The application: its components, internals and complete public HTTP allowlist.
pub struct App {
    pub uses: &'static [&'static dyn Component],
    pub collections: &'static [&'static Collection],
    /// Definitions other code reads; exposed methods register through `http`.
    pub definitions: &'static [Definition],
    pub triggers: &'static [&'static Trigger],
    pub http: &'static [(&'static str, Definition)],
}

pub(crate) enum TriggerAction {
    Materialize(&'static Derived),
    Run(&'static Trigger),
}

const MAINTENANCE: &str = "$flower.maintenance";
const MAINTENANCE_ERROR: &str = "$flower.maintenance.error";
static TASK_STATES: Collection = Collection::new("$flower.tasks");
static MARKERS: Collection = Collection::new("$flower.materialized");

/// What define() computes from the application, rebuilt for each callback.
pub struct Runtime {
    app: &'static App,
    /// Registered definitions in registration order: definitions, then HTTP methods.
    definitions: Vec<Definition>,
    tasks: Vec<&'static dyn Task>,
    materialization: Option<Materialization>,
    pub(crate) triggers: Vec<(&'static Collection, Vec<TriggerAction>)>,
    /// Collections whose triggers only observe rows appearing or disappearing.
    pub(crate) existence: Vec<&'static str>,
}

impl Runtime {
    pub fn new(app: &'static App) -> Self {
        let mut definitions: Vec<Definition> = Vec::new();
        for definition in app
            .definitions
            .iter()
            .chain(app.http.iter().map(|(_, definition)| definition))
        {
            if !definitions
                .iter()
                .any(|known| known.name() == definition.name())
            {
                definitions.push(*definition);
            }
        }
        let mut triggers: Vec<(&'static Collection, Vec<TriggerAction>)> = Vec::new();
        let mut add = |source: &'static Collection, action| match triggers
            .iter_mut()
            .find(|(known, _)| known.name == source.name)
        {
            Some((_, actions)) => actions.push(action),
            None => triggers.push((source, vec![action])),
        };
        for trigger in app.triggers {
            add(trigger.source, TriggerAction::Run(trigger));
        }
        let mut jobs = Vec::new();
        for definition in &definitions {
            let Definition::Derived(derived) = definition else {
                continue;
            };
            match derived.materialize {
                Materialize::Never => {}
                Materialize::Always => {
                    jobs.push((format!("always:{}", derived.name), *derived, None))
                }
                Materialize::Each(source) => {
                    jobs.push((
                        format!("each:{}:{}", derived.name, source.name),
                        *derived,
                        Some(source),
                    ));
                    add(source, TriggerAction::Materialize(derived));
                }
            }
        }
        let existence = triggers
            .iter()
            .filter(|(_, actions)| {
                actions
                    .iter()
                    .all(|action| matches!(action, TriggerAction::Materialize(_)))
            })
            .map(|(source, _)| source.name)
            .collect();
        let mut tasks = Vec::new();
        for component in app.uses {
            tasks.extend(component.tasks());
        }
        Runtime {
            app,
            definitions,
            tasks,
            materialization: (!jobs.is_empty()).then_some(Materialization { jobs }),
            triggers,
            existence,
        }
    }

    fn tasks(&self) -> Vec<&dyn Task> {
        let mut tasks: Vec<&dyn Task> = self.tasks.iter().map(|task| *task as &dyn Task).collect();
        if let Some(materialization) = &self.materialization {
            tasks.push(materialization);
        }
        tasks
    }

    fn maintained(&self) -> bool {
        !self.tasks.is_empty() || self.materialization.is_some()
    }

    /// The raw manifest of GUEST_ABI.md.
    pub fn manifest(&self) -> Value {
        let mut definitions = Map::new();
        for definition in &self.definitions {
            let mut entry = Map::new();
            match definition {
                Definition::Derived(derived) => {
                    entry.insert("kind", "derived");
                    if let Compute::Aggregate(aggregate) = derived.compute {
                        let fields: Vec<Value> = aggregate
                            .fields()
                            .iter()
                            .map(|field| Value::from(*field))
                            .collect();
                        entry.insert(
                            "aggregate",
                            object! {"collection" => aggregate.source.name, "fields" => fields},
                        );
                    }
                }
                Definition::Query(_, consistency) => {
                    entry.insert("kind", "query");
                    if *consistency == Consistency::ReplicaLocal {
                        entry.insert("consistency", "replica-local");
                    }
                }
                Definition::Mutation(_) => entry.insert("kind", "mutation"),
            }
            definitions.insert(definition.name(), entry);
        }
        if self.maintained() {
            definitions.insert(MAINTENANCE, object! {"kind" => "mutation"});
            definitions.insert(MAINTENANCE_ERROR, object! {"kind" => "mutation"});
        }
        let mut http = Map::new();
        for (alias, definition) in self.app.http {
            let mut entry = object! {"name" => definition.name()};
            match definition {
                Definition::Query(_, consistency) => {
                    entry.set("kind", "query");
                    if *consistency == Consistency::ReplicaLocal {
                        entry.set("consistency", "replica-local");
                    }
                }
                Definition::Mutation(_) => entry.set("kind", "mutation"),
                Definition::Derived(_) => {
                    panic!("Only query, mutation, and transaction methods may be exposed over HTTP")
                }
            }
            http.insert(*alias, entry);
        }
        let mut manifest = object! {"definitions" => definitions, "http" => http};
        if self.maintained() {
            manifest.set(
                "maintenance",
                object! {"name" => MAINTENANCE, "kind" => "mutation", "onError" => object! {"name" => MAINTENANCE_ERROR, "kind" => "mutation"}},
            );
        }
        let collections = self.collections();
        if !collections.is_empty() {
            manifest.set("collections", collections);
        }
        manifest
    }

    /// Components' collections, the application's, trigger sources and
    /// aggregate sources: merged by name and sorted.
    fn collections(&self) -> Vec<Value> {
        let mut all: Vec<&'static Collection> = Vec::new();
        for component in self.app.uses {
            all.extend(component.collections());
        }
        all.extend(self.app.collections);
        all.extend(self.triggers.iter().map(|(source, _)| *source));
        for definition in &self.definitions {
            if let Definition::Derived(Derived {
                compute: Compute::Aggregate(aggregate),
                ..
            }) = definition
            {
                all.push(aggregate.source);
            }
        }
        let mut merged: Vec<&'static Collection> = Vec::new();
        for collection in all {
            if !merged.iter().any(|known| known.name == collection.name) {
                merged.push(collection);
            }
        }
        merged.sort_by(|a, b| json::compare(a.name, b.name));
        merged
            .into_iter()
            .map(|collection| {
                let indexes: Map = collection
                    .indexes
                    .iter()
                    .map(|index| {
                        (
                            index.name,
                            Value::Array(
                                index
                                    .fields
                                    .iter()
                                    .map(|field| Value::from(*field))
                                    .collect(),
                            ),
                        )
                    })
                    .collect();
                object! {"name" => collection.name, "indexes" => indexes}
            })
            .collect::<Vec<Value>>()
    }

    /// Run one callback. Failures of derived callbacks carry no details.
    pub fn invoke(&self, kind: i32, name: &str, args: Value) -> Result<Value> {
        let maintenance = self.maintained() && (name == MAINTENANCE || name == MAINTENANCE_ERROR);
        let definition = self
            .definitions
            .iter()
            .find(|definition| definition.name() == name);
        let expected = match (definition, maintenance) {
            (Some(definition), _) => definition.kind(),
            (None, true) => Kind::Mutation,
            (None, false) => {
                return fail(
                    "DEFINITION_MISSING",
                    format!("Unknown derived definition: {name}"),
                );
            }
        };
        if kind != expected as i32 {
            let label = ["query", "mutation", "transaction", "derived"]
                .get(kind as usize)
                .copied()
                .unwrap_or("undefined");
            return fail(
                "METHOD_KIND_MISMATCH",
                format!("Definition {name} is not a {label} method"),
            );
        }
        match definition {
            None => {
                let mut ctx = Ctx::new(Kind::Mutation, self);
                let value = if name == MAINTENANCE {
                    self.maintain(&mut ctx)?
                } else {
                    self.recover(&mut ctx, args)?
                };
                ctx.settle()?;
                Ok(value)
            }
            Some(Definition::Derived(derived)) => match derived.compute {
                Compute::Aggregate(aggregate) => aggregate.compute(args),
                Compute::Function(compute) => compute(&mut Ctx::new(Kind::Derived, self), args),
            },
            Some(Definition::Query(method, _)) => {
                method.compute(&mut Ctx::new(Kind::Query, self), args)
            }
            Some(Definition::Mutation(method)) => {
                let mut ctx = Ctx::new(Kind::Mutation, self);
                let value = method.compute(&mut ctx, args)?;
                ctx.settle()?;
                Ok(value)
            }
        }
    }

    /// The task due earliest by now, and when any task is next due, retry delays included.
    fn plan<'a>(
        &'a self,
        ctx: &mut Ctx,
        tasks: &[&'a dyn Task],
        now: f64,
    ) -> Result<(Option<&'a dyn Task>, Option<f64>)> {
        let states = ctx.get(&TASK_STATES, &Value::from("state"))?;
        let (mut chosen, mut earliest, mut next) = (None, f64::INFINITY, f64::INFINITY);
        for candidate in tasks {
            let Some(due) = candidate.due(ctx)? else {
                continue;
            };
            if !due.is_finite() {
                return type_error(format!(
                    "Task {} returned an invalid due time",
                    candidate.name()
                ));
            }
            let name = candidate.name();
            let at = match states.as_object().and_then(|states| states.get(&name)) {
                Some(state) => due.max(state.number("retryAt").unwrap_or(f64::NAN)),
                None => due,
            };
            if at <= now && at < earliest {
                chosen = Some(*candidate);
                earliest = at;
            }
            next = next.min(at);
        }
        Ok((chosen, next.is_finite().then_some(next)))
    }

    /// The host sleeps until next (null: until a write) instead of polling.
    fn hint(&self, ctx: &mut Ctx, tasks: &[&dyn Task], now: f64) -> Result<Value> {
        let (chosen, next) = self.plan(ctx, tasks, now)?;
        Ok(object! {"continue" => chosen.is_some(), "next" => next})
    }

    fn maintain(&self, ctx: &mut Ctx) -> Result<Value> {
        let tasks = self.tasks();
        let now = ctx.now()?;
        let (selected, next) = self.plan(ctx, &tasks, now)?;
        let Some(selected) = selected else {
            return Ok(object! {"$flower" => object! {"continue" => false, "next" => next}});
        };
        let result = selected.run(ctx)?;
        ctx.settle()?;
        let name = selected.name();
        let states = ctx.get(&TASK_STATES, &Value::from("state"))?;
        if let Some(states) = states.as_object()
            && states.contains_key(&name)
        {
            let mut rest = states.clone();
            rest.remove(&name);
            if rest.is_empty() {
                ctx.delete(&TASK_STATES, &Value::from("state"))?;
            } else {
                ctx.set(&TASK_STATES, &Value::from("state"), Value::Object(rest))?;
            }
        }
        let now = ctx.now()?;
        Ok(object! {"task" => name, "result" => result, "$flower" => self.hint(ctx, &tasks, now)?})
    }

    fn recover(&self, ctx: &mut Ctx, failure: Value) -> Result<Value> {
        let info = plain(&failure, "Maintenance failure", &["error", "failedAt"])?;
        let error = plain(
            info.get("error").unwrap_or(&Value::Null),
            "Maintenance error",
            &["code", "message", "details"],
        )?;
        let failed_at = info
            .get("failedAt")
            .and_then(Value::as_f64)
            .filter(|at| is_safe_integer(*at));
        let (Some(_), Some(_), Some(failed_at)) = (
            error.get("code").and_then(Value::as_str),
            error.get("message").and_then(Value::as_str),
            failed_at,
        ) else {
            return type_error("Invalid maintenance failure");
        };
        let tasks = self.tasks();
        let now = ctx.now()?;
        let (selected, next) = self.plan(ctx, &tasks, now)?;
        let Some(selected) = selected else {
            return Ok(object! {"$flower" => object! {"continue" => false, "next" => next}});
        };
        let failure = TaskFailure {
            error: Value::Object(error.clone()),
            failed_at: failed_at.max(now),
        };
        if let Some(result) = selected.on_error(ctx, &failure) {
            let result = result?;
            ctx.settle()?;
            return Ok(
                object! {"task" => selected.name(), "result" => result, "$flower" => self.hint(ctx, &tasks, now)?},
            );
        }
        let name = selected.name();
        let mut states = ctx
            .get(&TASK_STATES, &Value::from("state"))?
            .into_object()
            .unwrap_or_default();
        let failures = states
            .get(&name)
            .and_then(|state| state.number("failures"))
            .unwrap_or(0.0)
            + 1.0;
        let retry_at = failure.failed_at + 60_000f64.min(1_000.0 * pow2((failures - 1.0).min(6.0)));
        states.insert(
            name.as_str(),
            object! {"failures" => failures, "retryAt" => retry_at, "error" => failure.error},
        );
        ctx.set(&TASK_STATES, &Value::from("state"), Value::Object(states))?;
        Ok(
            object! {"task" => name, "failures" => failures, "retryAt" => retry_at, "$flower" => self.hint(ctx, &tasks, now)?},
        )
    }
}

/// `plainObject(value, label, allowed)`.
pub fn plain<'v>(value: &'v Value, label: &str, allowed: &[&str]) -> Result<&'v Map> {
    let Some(map) = value.as_object() else {
        return type_error(format!("{label} must be a plain object"));
    };
    if let Some(key) = map.keys().find(|key| !allowed.contains(key)) {
        return type_error(format!("{label} does not accept {}", json::string(key)));
    }
    Ok(map)
}

/// Declarative materialization: maintain every instance a policy names.
struct Materialization {
    jobs: Vec<(String, &'static Derived, Option<&'static Collection>)>,
}

impl Materialization {
    fn pending(
        &self,
        ctx: &mut Ctx,
    ) -> Result<Option<&(String, &'static Derived, Option<&'static Collection>)>> {
        for job in &self.jobs {
            if ctx.get(&MARKERS, &Value::from(&job.0))?.get("done") != &Value::Bool(true) {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }
}

impl Task for Materialization {
    fn name(&self) -> String {
        "materialize".into()
    }
    fn due(&self, ctx: &mut Ctx) -> Result<Option<f64>> {
        Ok(if self.pending(ctx)?.is_some() {
            Some(ctx.now()?)
        } else {
            None
        })
    }
    fn run(&self, ctx: &mut Ctx) -> Result<Value> {
        let Some((marker, derived, source)) = self.pending(ctx)? else {
            return Ok(Value::Null);
        };
        let marker = Value::from(marker);
        let Some(source) = source else {
            ctx.materialize(derived, &Value::Null)?;
            ctx.set(
                &MARKERS,
                &marker,
                object! {"cursor" => Value::Null, "done" => true},
            )?;
            return Ok(object! {"materialized" => derived.name, "rows" => 1});
        };
        let cursor = ctx.get(&MARKERS, &marker)?.get("cursor").clone();
        let options = match &cursor {
            Value::Null => object! {"limit" => 64},
            cursor => object! {"gt" => cursor, "limit" => 64},
        };
        let rows = ctx.raw_scan(source, Some(&options))?;
        for (key, _) in &rows {
            ctx.materialize(derived, &source.decode_key(key))?;
        }
        let last = rows.last().map_or(cursor, |(key, _)| Value::from(key));
        ctx.set(
            &MARKERS,
            &marker,
            object! {"cursor" => last, "done" => rows.len() < 64},
        )?;
        Ok(object! {"materialized" => derived.name, "rows" => rows.len()})
    }
}

impl From<Failure> for Value {
    fn from(failure: Failure) -> Value {
        let mut value = object! {"code" => failure.code, "message" => failure.message};
        if let Some(details) = failure.details {
            value.set("details", details);
        }
        value
    }
}
