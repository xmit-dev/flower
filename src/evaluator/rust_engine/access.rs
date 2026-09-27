//! Collection access policies.
//!
//! A collection may declare who can read, insert, update and delete its rows,
//! and who can read or write single fields. The host enforces those rules on
//! every collection operation a query or mutation performs for a caller:
//! hidden rows read as absent and drop out of scans, queries and range pages
//! (before limits, offsets and cursors), unreadable fields are redacted, and a
//! denied write fails with `ACCESS_DENIED`.
//!
//! Only public calls have a caller. Derived values, maintenance tasks and the
//! authorization hook run without one and see every row, like SQL's
//! `SECURITY DEFINER` views: expose them deliberately. Triggers act with the
//! same rights: the SDK brackets them with the `definer` host operation.
//!
//! Rules are data, not guest code, so the host evaluates them natively. Each
//! invocation folds its principal into a collection's rules once: a rule that
//! only looks at the caller (`principal.claims.role == "admin"`) becomes a
//! constant and costs nothing per row, and a collection without a policy
//! costs one map lookup per invocation.
//!
//! `readable(collection, key)` looks up one row of another guarded collection
//! and holds when the caller may read it, like Firestore's `get()`: a log's
//! entries can follow their session's privacy without copying it into every
//! entry. The target's read rule may not use `readable` itself, so one lookup
//! never leads to another; each check looks a row up once and records it as
//! read, so results depending on it follow its changes.
use super::*;
use std::cell::{OnceCell, RefCell};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};

const RELAXED: std::sync::atomic::Ordering = std::sync::atomic::Ordering::Relaxed;

/// Largest rule a collection may declare for one operation, in nodes.
const MAX_RULE_NODES: usize = 256;
const MAX_RULE_DEPTH: usize = 32;

/// A collection's declared policy, as stored in the schema record. Row
/// operations without a rule are denied.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default)]
    pub read: Rule,
    #[serde(default)]
    pub insert: Rule,
    #[serde(default)]
    pub update: Rule,
    #[serde(default)]
    pub delete: Rule,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, FieldPolicy>,
}

/// Rules for one top-level field, on top of the row's. Without `write`, a
/// caller may change the field only where it may read it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FieldPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<Rule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<Rule>,
}

/// `{"const":true}`, `{"all":[…]}`, `{"any":[…]}`, `{"not":…}`,
/// `{"eq":[a,b]}`, `{"ne":[a,b]}`, `{"in":[item,list]}`, `{"exists":a}`,
/// `{"lt":[a,b]}`, `{"lte":…}`, `{"gt":…}`, `{"gte":…}`,
/// `{"startsWith":[text,prefix]}` and `{"readable":[collection,key]}`.
/// Comparisons with a missing or null side are false, so a row without an
/// `owner` never matches an anonymous caller's missing subject. Only numbers
/// order against numbers and strings against strings (by code point); any
/// other pair compares false. `readable` holds when the collection has a row
/// at the key (a string, or an array or object as canonical JSON) that the
/// caller may read by that collection's read rule.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Rule {
    Const(bool),
    All(Vec<Rule>),
    Any(Vec<Rule>),
    Not(Box<Rule>),
    Eq(Operand, Operand),
    Ne(Operand, Operand),
    In(Operand, Operand),
    Exists(Operand),
    Lt(Operand, Operand),
    Lte(Operand, Operand),
    Gt(Operand, Operand),
    Gte(Operand, Operand),
    #[serde(rename = "startsWith")]
    StartsWith(Operand, Operand),
    Readable(String, Operand),
}

impl Default for Rule {
    fn default() -> Self {
        Self::Const(false)
    }
}

/// `{"value": json}` or `{"ref": path}`, where a path starts with
/// `principal` (`subject`, `tenant`, `claims`…), `row` (the stored row) or
/// `next` (the row being written), is `["key"]` (the stored key) or a path
/// into a JSON key (`["key","0"]`), or is exactly `["now"]`: the
/// invocation's time in milliseconds.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Operand {
    Ref(Vec<String>),
    Value(Value),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Read,
    Insert,
    Update,
    Delete,
    FieldRead,
    FieldWrite,
    /// Reading a derived value: the caller, the value's arguments and the time.
    Derived,
}

impl Scope {
    fn allows(self, root: &str) -> bool {
        match root {
            "principal" | "now" => true,
            "key" => self != Scope::Derived,
            "row" => !matches!(self, Scope::Insert | Scope::Derived),
            "next" => matches!(self, Scope::Insert | Scope::Update | Scope::FieldWrite),
            "args" => self == Scope::Derived,
            _ => false,
        }
    }
}

impl Policy {
    /// Structural checks the manifest and stored schema both apply.
    pub fn validate(&self) -> Result<(), String> {
        for (scope, label, rule) in [
            (Scope::Read, "read", &self.read),
            (Scope::Insert, "insert", &self.insert),
            (Scope::Update, "update", &self.update),
            (Scope::Delete, "delete", &self.delete),
        ] {
            rule.validate(scope, label)?;
        }
        for (field, policy) in &self.fields {
            if field.is_empty() {
                return Err("field names must be nonempty".into());
            }
            if policy.read.is_none() && policy.write.is_none() {
                return Err(format!("field {field:?} declares no rule"));
            }
            if let Some(rule) = &policy.read {
                rule.validate(Scope::FieldRead, &format!("fields.{field}.read"))?;
            }
            if let Some(rule) = &policy.write {
                rule.validate(Scope::FieldWrite, &format!("fields.{field}.write"))?;
            }
        }
        Ok(())
    }

    /// The rules of every operation and field.
    fn rules(&self) -> impl Iterator<Item = &Rule> {
        [&self.read, &self.insert, &self.update, &self.delete]
            .into_iter()
            .chain(
                self.fields
                    .values()
                    .flat_map(|field| field.read.iter().chain(field.write.iter())),
            )
    }

    pub(super) fn allocation_cost(&self) -> usize {
        let rules = [&self.read, &self.insert, &self.update, &self.delete]
            .into_iter()
            .chain(
                self.fields
                    .values()
                    .flat_map(|field| field.read.iter().chain(field.write.iter())),
            );
        self.fields.keys().fold(
            rules.fold(256usize, |bytes, rule| bytes.saturating_add(rule.cost())),
            |bytes, field| bytes.saturating_add(128 + field.len()),
        )
    }
}

/// Checks across policies: each `readable` names a collection with a policy
/// whose read rule doesn't use `readable` itself.
pub fn validate_targets(policies: &BTreeMap<String, Policy>) -> Result<(), String> {
    for (name, policy) in policies {
        let mut targets = Vec::new();
        for rule in policy.rules() {
            rule.targets(&mut targets);
        }
        for target in targets {
            match policies.get(target) {
                None => {
                    return Err(format!(
                        "{name}: readable({target:?}) needs a collection with an access policy"
                    ));
                }
                Some(policy) if policy.read.reads_rows() => {
                    return Err(format!(
                        "{name}: readable({target:?}) names a collection whose read rule uses readable itself"
                    ));
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

impl Rule {
    /// Checks for a derived value's rule: `principal`, `args` and `now` only.
    pub fn validate_derived(&self) -> Result<(), String> {
        self.validate(Scope::Derived, "access")
    }

    pub(super) fn allocation_cost(&self) -> usize {
        self.cost()
    }

    fn validate(&self, scope: Scope, label: &str) -> Result<(), String> {
        let mut nodes = 0usize;
        self.walk(scope, 1, &mut nodes)
            .map_err(|error| format!("{label}: {error}"))
    }

    fn walk(&self, scope: Scope, depth: usize, nodes: &mut usize) -> Result<(), String> {
        *nodes += 1;
        if *nodes > MAX_RULE_NODES {
            return Err(format!("a rule may have at most {MAX_RULE_NODES} nodes"));
        }
        if depth > MAX_RULE_DEPTH {
            return Err(format!("a rule may nest at most {MAX_RULE_DEPTH} deep"));
        }
        match self {
            Rule::Const(_) => Ok(()),
            Rule::All(rules) | Rule::Any(rules) => rules
                .iter()
                .try_for_each(|rule| rule.walk(scope, depth + 1, nodes)),
            Rule::Not(rule) => rule.walk(scope, depth + 1, nodes),
            Rule::Eq(left, right)
            | Rule::Ne(left, right)
            | Rule::In(left, right)
            | Rule::Lt(left, right)
            | Rule::Lte(left, right)
            | Rule::Gt(left, right)
            | Rule::Gte(left, right)
            | Rule::StartsWith(left, right) => {
                left.validate(scope)?;
                right.validate(scope)
            }
            Rule::Exists(operand) => operand.validate(scope),
            Rule::Readable(_, _) if scope == Scope::Derived => {
                Err("readable cannot be used here".into())
            }
            Rule::Readable(collection, _) if collection.is_empty() => {
                Err("readable needs a collection name".into())
            }
            Rule::Readable(_, key) => key.validate(scope),
        }
    }

    /// Whether the rule looks up rows with `readable`.
    fn reads_rows(&self) -> bool {
        let mut targets = Vec::new();
        self.targets(&mut targets);
        !targets.is_empty()
    }

    /// The collections the rule's `readable`s name.
    fn targets<'a>(&'a self, targets: &mut Vec<&'a str>) {
        match self {
            Rule::All(rules) | Rule::Any(rules) => {
                rules.iter().for_each(|rule| rule.targets(targets))
            }
            Rule::Not(rule) => rule.targets(targets),
            Rule::Readable(collection, _) => targets.push(collection),
            _ => {}
        }
    }

    fn cost(&self) -> usize {
        96 + match self {
            Rule::Const(_) => 0,
            Rule::All(rules) | Rule::Any(rules) => rules.iter().map(Rule::cost).sum(),
            Rule::Not(rule) => rule.cost(),
            Rule::Eq(left, right)
            | Rule::Ne(left, right)
            | Rule::In(left, right)
            | Rule::Lt(left, right)
            | Rule::Lte(left, right)
            | Rule::Gt(left, right)
            | Rule::Gte(left, right)
            | Rule::StartsWith(left, right) => left.cost() + right.cost(),
            Rule::Exists(operand) => operand.cost(),
            Rule::Readable(collection, key) => 32 + collection.len() + key.cost(),
        }
    }
}

impl Operand {
    fn validate(&self, scope: Scope) -> Result<(), String> {
        let Operand::Ref(path) = self else {
            return Ok(());
        };
        let valid = match path.split_first() {
            Some((root, rest)) if scope.allows(root) => match root.as_str() {
                "principal" => match rest {
                    [field] => field == "subject" || field == "tenant" || field == "claims",
                    [field, claims @ ..] => {
                        field == "claims" && claims.iter().all(|segment| !segment.is_empty())
                    }
                    [] => false,
                },
                "key" | "args" => rest.iter().all(|segment| !segment.is_empty()),
                "now" => rest.is_empty(),
                _ => !rest.is_empty() && rest.iter().all(|segment| !segment.is_empty()),
            },
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(format!("{} cannot be used here", path.join(".")))
        }
    }

    fn cost(&self) -> usize {
        match self {
            Operand::Ref(path) => path.iter().map(|segment| 32 + segment.len()).sum(),
            Operand::Value(value) => allocation_cost(value),
        }
    }
}

/// A rule with the caller folded in. What remains depends only on the row
/// and, for rules about time, the clock.
#[derive(Debug)]
enum Check {
    Const(bool),
    All(Vec<Check>),
    Any(Vec<Check>),
    Not(Box<Check>),
    Compare(Op, Term, Term),
    Exists(Term),
    /// A row of `collection` at `key` exists and `read`, the collection's
    /// read rule for this caller, holds on it.
    Readable {
        collection: String,
        read: Box<Check>,
        key: Term,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Eq,
    Ne,
    In,
    Lt,
    Lte,
    Gt,
    Gte,
    StartsWith,
}

#[derive(Debug)]
enum Term {
    Row(Vec<String>),
    Next(Vec<String>),
    Key,
    /// A path into a JSON key: canonical JSON of an array or an object.
    KeyPart(Vec<String>),
    /// The invocation's time, in milliseconds.
    Now(Value),
    Value(Value),
}

/// What checks learned about the clock since the engine last asked: whether
/// an outcome depended on the time, and the earliest instant one changes.
/// Atomics keep `Access` shareable; each invocation still owns its own.
#[derive(Debug)]
struct Clock {
    read: AtomicBool,
    flips_at: AtomicU64,
}

impl Default for Clock {
    fn default() -> Self {
        Self {
            read: AtomicBool::new(false),
            flips_at: AtomicU64::new(u64::MAX),
        }
    }
}

impl Clock {
    fn note(&self, flip: Option<u64>) {
        self.read.store(true, RELAXED);
        if let Some(flip) = flip {
            self.flips_at.fetch_min(flip, RELAXED);
        }
    }

    fn take(&self) -> (bool, Option<u64>) {
        let read = self.read.swap(false, RELAXED);
        let flip = self.flips_at.swap(u64::MAX, RELAXED);
        (read, (flip != u64::MAX).then_some(flip))
    }
}

/// What one check sees: the row's key, the stored row and the row being
/// written, when there are any.
#[derive(Clone, Copy)]
struct Subject<'a> {
    key: &'a str,
    /// The key parsed as JSON, on the first key-part term of this row.
    parts: &'a OnceCell<Option<Value>>,
    row: Option<&'a Value>,
    next: Option<&'a Value>,
    clock: &'a Clock,
    rows: &'a dyn Rows,
}

/// Where `readable` finds rows: the engine's view of the invocation, its own
/// writes included.
pub(super) trait Rows {
    /// Whether `collection` has a row at `key` on which `visible` holds.
    fn readable(&self, collection: &str, key: &str, visible: &dyn Fn(&Value) -> bool) -> bool;
}

/// No rows: for checks that can't look any up.
pub(super) struct NoRows;

impl Rows for NoRows {
    fn readable(&self, _: &str, _: &str, _: &dyn Fn(&Value) -> bool) -> bool {
        false
    }
}

/// The rows one access check looks up, each once, remembered so the engine
/// can record them as reads afterwards.
pub(super) struct RowLookup<'e, 'a> {
    engine: &'e Engine<'a>,
    seen: RefCell<BTreeMap<String, bool>>,
}

impl<'e, 'a> RowLookup<'e, 'a> {
    pub(super) fn new(engine: &'e Engine<'a>) -> Self {
        Self {
            engine,
            seen: RefCell::new(BTreeMap::new()),
        }
    }

    /// The source IDs of the rows looked up.
    pub(super) fn into_reads(self) -> Vec<String> {
        self.seen.into_inner().into_keys().collect()
    }
}

impl Rows for RowLookup<'_, '_> {
    fn readable(&self, collection: &str, key: &str, visible: &dyn Fn(&Value) -> bool) -> bool {
        let id = source_id(collection, key);
        if let Some(&seen) = self.seen.borrow().get(&id) {
            return seen;
        }
        let shown = self.engine.source(&id).is_some_and(|row| visible(row));
        self.seen.borrow_mut().insert(id, shown);
        shown
    }
}

/// No policies: what a `readable` target's read rule may look into.
static NO_POLICIES: BTreeMap<String, Policy> = BTreeMap::new();

#[derive(Clone, Copy)]
enum Resolved<'a> {
    Missing,
    Json(&'a Value),
    Key(&'a str),
}

fn lookup<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, segment| match value {
        Value::Object(fields) => fields.get(segment),
        Value::Array(items) => segment
            .parse::<usize>()
            .ok()
            .and_then(|index| items.get(index)),
        _ => None,
    })
}

fn present(value: Resolved<'_>) -> bool {
    !matches!(value, Resolved::Missing | Resolved::Json(Value::Null))
}

/// Equality of two present values; `None` when either side is missing or null.
fn same(left: Resolved<'_>, right: Resolved<'_>) -> Option<bool> {
    if !present(left) || !present(right) {
        return None;
    }
    Some(match (left, right) {
        (Resolved::Key(left), Resolved::Key(right)) => left == right,
        (Resolved::Key(key), Resolved::Json(value))
        | (Resolved::Json(value), Resolved::Key(key)) => value.as_str() == Some(key),
        (Resolved::Json(left), Resolved::Json(right)) => equal(left, right),
        _ => false,
    })
}

fn contains(item: Resolved<'_>, list: Resolved<'_>) -> bool {
    match list {
        Resolved::Json(Value::Array(items)) => {
            present(item)
                && items
                    .iter()
                    .any(|each| same(item, Resolved::Json(each)) == Some(true))
        }
        _ => false,
    }
}

fn text(value: Resolved<'_>) -> Option<&str> {
    match value {
        Resolved::Key(key) => Some(key),
        Resolved::Json(Value::String(text)) => Some(text),
        _ => None,
    }
}

/// Two numbers, or two strings by code point (UTF-8 byte order); nothing
/// else is ordered.
fn order(left: Resolved<'_>, right: Resolved<'_>) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (text(left), text(right)) {
        return Some(left.cmp(right));
    }
    let (Resolved::Json(Value::Number(left)), Resolved::Json(Value::Number(right))) = (left, right)
    else {
        return None;
    };
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return Some(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return Some(left.cmp(&right));
    }
    left.as_f64()?.partial_cmp(&right.as_f64()?)
}

impl Op {
    fn holds(self, left: Resolved<'_>, right: Resolved<'_>) -> bool {
        match self {
            Op::Eq => same(left, right) == Some(true),
            Op::Ne => same(left, right) == Some(false),
            Op::In => contains(left, right),
            Op::Lt => order(left, right) == Some(Ordering::Less),
            Op::Lte => matches!(order(left, right), Some(Ordering::Less | Ordering::Equal)),
            Op::Gt => order(left, right) == Some(Ordering::Greater),
            Op::Gte => matches!(
                order(left, right),
                Some(Ordering::Greater | Ordering::Equal)
            ),
            Op::StartsWith => text(left)
                .zip(text(right))
                .is_some_and(|(text, prefix)| text.starts_with(prefix)),
        }
    }

    /// The same comparison with its sides swapped: `a < b` is `b > a`.
    fn swapped(self) -> Self {
        match self {
            Op::Lt => Op::Gt,
            Op::Lte => Op::Gte,
            Op::Gt => Op::Lt,
            Op::Gte => Op::Lte,
            op => op,
        }
    }
}

/// The first instant after `now` when `now <op> value` changes outcome:
/// `now < v` and `now >= v` at v, `now <= v` and `now > v` just after v.
fn flip(op: Op, now: u64, value: &Value) -> Option<u64> {
    let value = value.as_f64().filter(|value| value.is_finite())?;
    let reached = value.ceil();
    let passed = value.floor() + 1.0;
    let at = match op {
        Op::Lt | Op::Gte => reached,
        Op::Lte | Op::Gt => passed,
        Op::Eq | Op::Ne if reached > now as f64 => reached,
        Op::Eq | Op::Ne => passed,
        Op::In | Op::StartsWith => return None,
    };
    (at > now as f64 && at < 9_007_199_254_740_992.0).then_some(at as u64)
}

/// When a comparison of the clock (on the left) with `other` next flips.
fn clock_flip(op: Op, now: &Value, other: Resolved<'_>) -> Option<u64> {
    let (Some(now), Resolved::Json(other)) = (now.as_u64(), other) else {
        return None;
    };
    match (op, other) {
        (Op::In, Value::Array(items)) => items
            .iter()
            .filter_map(|item| flip(Op::Eq, now, item))
            .min(),
        _ => flip(op, now, other),
    }
}

impl Term {
    fn resolve<'a>(&'a self, subject: Subject<'a>) -> Resolved<'a> {
        match self {
            Term::Value(value) | Term::Now(value) => Resolved::Json(value),
            Term::Key => Resolved::Key(subject.key),
            Term::KeyPart(path) => subject
                .parts
                .get_or_init(|| {
                    serde_json::from_str::<Value>(subject.key)
                        .ok()
                        .filter(|parts| parts.is_array() || parts.is_object())
                })
                .as_ref()
                .and_then(|parts| lookup(parts, path))
                .map_or(Resolved::Missing, Resolved::Json),
            Term::Row(path) => subject
                .row
                .and_then(|row| lookup(row, path))
                .map_or(Resolved::Missing, Resolved::Json),
            Term::Next(path) => subject
                .next
                .and_then(|next| lookup(next, path))
                .map_or(Resolved::Missing, Resolved::Json),
        }
    }

    fn constant(&self) -> bool {
        matches!(self, Term::Value(_) | Term::Now(_))
    }
}

impl Check {
    /// `policies` are the collections `readable` may look into.
    fn new(
        rule: &Rule,
        caller: &Value,
        now: u64,
        clock: &Clock,
        policies: &BTreeMap<String, Policy>,
    ) -> Self {
        let term = |operand: &Operand| match operand {
            Operand::Value(value) => Term::Value(value.clone()),
            Operand::Ref(path) => match path[0].as_str() {
                "principal" => {
                    Term::Value(lookup(caller, &path[1..]).cloned().unwrap_or(Value::Null))
                }
                // A derived value's rule sees its arguments where rows would be.
                "row" | "args" => Term::Row(path[1..].to_vec()),
                "next" => Term::Next(path[1..].to_vec()),
                "now" => Term::Now(json!(now)),
                _ if path.len() > 1 => Term::KeyPart(path[1..].to_vec()),
                _ => Term::Key,
            },
        };
        // Comparisons of two constants, such as a claim and a literal, fold now.
        let compare = |op: Op, left: &Operand, right: &Operand| {
            let check = Check::Compare(op, term(left), term(right));
            match &check {
                Check::Compare(_, left, right) if left.constant() && right.constant() => {
                    let parts = OnceCell::new();
                    Check::Const(check.holds(Subject {
                        key: "",
                        parts: &parts,
                        row: None,
                        next: None,
                        clock,
                        rows: &NoRows,
                    }))
                }
                _ => check,
            }
        };
        match rule {
            Rule::Const(value) => Check::Const(*value),
            Rule::All(rules) => Self::join(rules, caller, now, clock, policies, true),
            Rule::Any(rules) => Self::join(rules, caller, now, clock, policies, false),
            Rule::Not(rule) => match Check::new(rule, caller, now, clock, policies) {
                Check::Const(value) => Check::Const(!value),
                check => Check::Not(Box::new(check)),
            },
            Rule::Eq(left, right) => compare(Op::Eq, left, right),
            Rule::Ne(left, right) => compare(Op::Ne, left, right),
            Rule::In(left, right) => compare(Op::In, left, right),
            Rule::Lt(left, right) => compare(Op::Lt, left, right),
            Rule::Lte(left, right) => compare(Op::Lte, left, right),
            Rule::Gt(left, right) => compare(Op::Gt, left, right),
            Rule::Gte(left, right) => compare(Op::Gte, left, right),
            Rule::StartsWith(left, right) => compare(Op::StartsWith, left, right),
            Rule::Exists(operand) => match term(operand) {
                Term::Value(value) => Check::Const(present(Resolved::Json(&value))),
                Term::Now(_) => Check::Const(true),
                term => Check::Exists(term),
            },
            // The target's read rule folds for this caller too, without
            // policies of its own: one it can never satisfy needs no lookup.
            Rule::Readable(collection, key) => match policies
                .get(collection)
                .map(|policy| Check::new(&policy.read, caller, now, clock, &NO_POLICIES))
            {
                None | Some(Check::Const(false)) => Check::Const(false),
                Some(read) => Check::Readable {
                    collection: collection.clone(),
                    read: Box::new(read),
                    key: term(key),
                },
            },
        }
    }

    /// `all` (a conjunction) or `any`, dropping neutral constants and
    /// short-circuiting on absorbing ones.
    fn join(
        rules: &[Rule],
        caller: &Value,
        now: u64,
        clock: &Clock,
        policies: &BTreeMap<String, Policy>,
        conjunction: bool,
    ) -> Self {
        let mut checks = Vec::new();
        for rule in rules {
            match Check::new(rule, caller, now, clock, policies) {
                Check::Const(value) if value == conjunction => {}
                Check::Const(value) => return Check::Const(value),
                check => checks.push(check),
            }
        }
        match checks.len() {
            0 => Check::Const(conjunction),
            1 => checks.pop().expect("one check"),
            _ if conjunction => Check::All(checks),
            _ => Check::Any(checks),
        }
    }

    fn holds(&self, subject: Subject<'_>) -> bool {
        match self {
            Check::Const(value) => *value,
            Check::All(checks) => checks.iter().all(|check| check.holds(subject)),
            Check::Any(checks) => checks.iter().any(|check| check.holds(subject)),
            Check::Not(check) => !check.holds(subject),
            Check::Compare(op, left, right) => {
                let (resolved_left, resolved_right) =
                    (left.resolve(subject), right.resolve(subject));
                // An outcome that read the clock makes the result time
                // dependent, and says when it will next change.
                match (left, right) {
                    (Term::Now(_), Term::Now(_)) => {}
                    (Term::Now(now), _) => subject.clock.note(clock_flip(*op, now, resolved_right)),
                    (_, Term::Now(_)) if *op == Op::In => subject.clock.note(None),
                    (_, Term::Now(now)) => {
                        subject
                            .clock
                            .note(clock_flip(op.swapped(), now, resolved_left))
                    }
                    _ => {}
                }
                op.holds(resolved_left, resolved_right)
            }
            Check::Exists(term) => present(term.resolve(subject)),
            Check::Readable {
                collection,
                read,
                key,
            } => {
                let text;
                let key = match key.resolve(subject) {
                    Resolved::Key(key) => key,
                    Resolved::Json(Value::String(key)) => key.as_str(),
                    Resolved::Json(value @ (Value::Array(_) | Value::Object(_))) => {
                        text = canonical_json(value);
                        text.as_str()
                    }
                    _ => return false,
                };
                subject.rows.readable(collection, key, &|row| {
                    let parts = OnceCell::new();
                    read.holds(Subject {
                        key,
                        parts: &parts,
                        row: Some(row),
                        next: None,
                        clock: subject.clock,
                        rows: &NoRows,
                    })
                })
            }
        }
    }

    fn always(&self) -> bool {
        matches!(self, Check::Const(true))
    }
}

#[derive(Debug)]
struct FieldCheck {
    name: String,
    read: Option<Check>,
    write: Option<Check>,
}

/// The principal as rules see it. Methods see an anonymous caller as null,
/// and so do rules: only the tenant, a named partition's name, remains.
fn caller(principal: &Value) -> Value {
    match principal {
        Value::Object(fields)
            if fields.get("subject").and_then(Value::as_str) == Some("$anonymous") =>
        {
            json!({"tenant": fields.get("tenant").cloned().unwrap_or(Value::Null)})
        }
        Value::Object(_) => principal.clone(),
        _ => json!({}),
    }
}

/// One caller's view of a collection's policy, built once per invocation.
#[derive(Debug)]
pub(super) struct Access {
    collection: String,
    read: Check,
    insert: Check,
    update: Check,
    delete: Check,
    fields: Vec<FieldCheck>,
    /// Some field's read rule can fail for this caller.
    redacts: bool,
    clock: Clock,
}

impl Access {
    fn new(
        collection: &str,
        policy: &Policy,
        policies: &BTreeMap<String, Policy>,
        principal: &Value,
        now: u64,
    ) -> Self {
        let caller = caller(principal);
        let clock = Clock::default();
        let check = |rule: &Rule| Check::new(rule, &caller, now, &clock, policies);
        let fields: Vec<FieldCheck> = policy
            .fields
            .iter()
            .map(|(name, field)| FieldCheck {
                name: name.clone(),
                read: field.read.as_ref().map(check),
                write: field.write.as_ref().map(check),
            })
            .collect();
        let (read, insert, update, delete) = (
            check(&policy.read),
            check(&policy.insert),
            check(&policy.update),
            check(&policy.delete),
        );
        Self {
            collection: collection.to_owned(),
            read,
            insert,
            update,
            delete,
            redacts: fields
                .iter()
                .any(|field| field.read.as_ref().is_some_and(|check| !check.always())),
            fields,
            clock,
        }
    }

    fn subject<'a>(
        &'a self,
        key: &'a str,
        parts: &'a OnceCell<Option<Value>>,
        row: Option<&'a Value>,
        next: Option<&'a Value>,
        rows: &'a dyn Rows,
    ) -> Subject<'a> {
        Subject {
            key,
            parts,
            row,
            next,
            clock: &self.clock,
            rows,
        }
    }

    /// Every row and field is readable: reads pass through untouched.
    pub(super) fn open_reads(&self) -> bool {
        self.read.always() && !self.redacts
    }

    /// Every write is allowed, and no hidden field needs carrying over.
    pub(super) fn open_writes(&self) -> bool {
        self.insert.always()
            && self.update.always()
            && self.delete.always()
            && self.fields.iter().all(|field| field.write.is_none())
            && !self.redacts
    }

    /// Whether the caller may read `field` of the stored row in `subject`.
    fn field_readable(&self, field: &FieldCheck, subject: Subject<'_>) -> bool {
        field.read.as_ref().is_none_or(|check| check.holds(subject))
    }

    /// Whether the caller sees this row at all. `fields` are the index fields
    /// that selected it: finding a row through a field the caller can't read
    /// would reveal that field's value, so such rows stay hidden too.
    pub(super) fn visible(
        &self,
        key: &str,
        row: &Value,
        fields: &[String],
        rows: &dyn Rows,
    ) -> bool {
        let parts = OnceCell::new();
        let subject = self.subject(key, &parts, Some(row), None, rows);
        self.read.holds(subject)
            && fields.iter().all(|name| {
                self.fields
                    .iter()
                    .find(|field| &field.name == name)
                    .is_none_or(|field| self.field_readable(field, subject))
            })
    }

    /// The row without the fields the caller can't read. Shares the stored
    /// value when nothing is hidden.
    pub(super) fn redact(&self, key: &str, row: &Arc<Value>, rows: &dyn Rows) -> Arc<Value> {
        if !self.redacts {
            return row.clone();
        }
        let Value::Object(entries) = row.as_ref() else {
            return row.clone();
        };
        let parts = OnceCell::new();
        let subject = self.subject(key, &parts, Some(row), None, rows);
        let hidden: Vec<&str> = self
            .fields
            .iter()
            .filter(|field| {
                entries.contains_key(&field.name) && !self.field_readable(field, subject)
            })
            .map(|field| field.name.as_str())
            .collect();
        if hidden.is_empty() {
            return row.clone();
        }
        let mut shown = entries.clone();
        for name in hidden {
            shown.remove(name);
        }
        Arc::new(Value::Object(shown))
    }

    /// When every row the caller may read holds one string in one field (the
    /// read rule is, or has a conjunct, `row.field == "…"` once the caller is
    /// folded in, like `row.owner == principal.subject`), that field and value.
    fn bucket(&self) -> Option<(&str, &Value)> {
        fn equality(check: &Check) -> Option<(&str, &Value)> {
            match check {
                Check::Compare(Op::Eq, Term::Row(path), Term::Value(value @ Value::String(_)))
                | Check::Compare(Op::Eq, Term::Value(value @ Value::String(_)), Term::Row(path))
                    if path.len() == 1 =>
                {
                    Some((path[0].as_str(), value))
                }
                Check::All(checks) => checks.iter().find_map(equality),
                _ => None,
            }
        }
        equality(&self.read)
    }

    /// The visible rows, redacted.
    pub(super) fn filter(
        &self,
        found: Vec<(String, Arc<Value>)>,
        fields: &[String],
        rows: &dyn Rows,
    ) -> Vec<(String, Arc<Value>)> {
        found
            .into_iter()
            .filter(|(key, row)| self.visible(key, row, fields, rows))
            .map(|(key, row)| {
                let shown = self.redact(&key, &row, rows);
                (key, shown)
            })
            .collect()
    }

    /// Admit a write of `next` (`None` deletes) over `previous`, returning the
    /// value to store, or `Skip` to leave the row alone: a denied delete of a
    /// row the caller can't see acts like deleting a missing key, so it
    /// doesn't reveal that the key exists. Fields the caller can't read and didn't write keep
    /// their stored values, so a read-modify-write of a redacted row preserves
    /// them, except those in `clear` (`ctx.set(…, {clear: ["token"]})`): the
    /// write removes them, which their write rules must allow. Denied inserts
    /// and updates fail identically.
    pub(super) fn admit(
        &self,
        key: &str,
        previous: Option<&Value>,
        next: Option<Value>,
        clear: &[String],
        rows: &dyn Rows,
    ) -> EngineResult<Admitted> {
        let denied = || {
            EngineError::new(
                "ACCESS_DENIED",
                format!("Access policy denies this write to {}", self.collection),
            )
        };
        let parts = OnceCell::new();
        let (previous, mut next) = match (previous, next) {
            (None, None) => return Ok(Admitted::Write(None)),
            (Some(previous), None) => {
                let subject = self.subject(key, &parts, Some(previous), None, rows);
                return if self.delete.holds(subject) {
                    Ok(Admitted::Write(None))
                } else if !self.read.holds(subject) {
                    Ok(Admitted::Skip)
                } else {
                    Err(denied())
                };
            }
            (previous, Some(next)) => (previous, next),
        };
        if let (Some(stored @ Value::Object(entries)), Value::Object(written)) =
            (previous, &mut next)
        {
            for field in &self.fields {
                if !written.contains_key(&field.name)
                    && !clear.contains(&field.name)
                    && let Some(value) = entries.get(&field.name)
                    && !self.field_readable(
                        field,
                        self.subject(key, &parts, Some(stored), None, rows),
                    )
                {
                    written.insert(field.name.clone(), value.clone());
                }
            }
        }
        let subject = self.subject(key, &parts, previous, Some(&next), rows);
        let row = if previous.is_some() {
            &self.update
        } else {
            &self.insert
        };
        let fields = self.fields.iter().all(|field| {
            let before = previous.and_then(|row| row.get(&field.name));
            let after = next.get(&field.name);
            let changed = match (before, after) {
                (None, None) => false,
                (Some(before), Some(after)) => !equal(before, after),
                _ => true,
            };
            !changed
                || match (&field.write, &field.read) {
                    (Some(write), _) => write.holds(subject),
                    // Change only what you can read: in the stored row, or in
                    // the new one for an insert.
                    (None, Some(read)) => {
                        read.holds(self.subject(
                            key,
                            &parts,
                            Some(previous.unwrap_or(&next)),
                            None,
                            rows,
                        ))
                    }
                    (None, None) => true,
                }
        });
        if row.holds(subject) && fields {
            Ok(Admitted::Write(Some(next)))
        } else {
            Err(denied())
        }
    }
}

/// What a guarded write does: store a value (`None` deletes), or nothing.
pub(super) enum Admitted {
    Write(Option<Value>),
    Skip,
}

impl Engine<'_> {
    /// The caller's view of a collection's policy, or `None` when none
    /// applies: invocations without a caller, code running with definer
    /// rights (triggers), and collections without one.
    fn access(&mut self, collection: &str) -> Option<Arc<Access>> {
        if self.system || self.definer > 0 || self.schema.policies.is_empty() {
            return None;
        }
        if let Some(access) = self.access.get(collection) {
            return access.clone();
        }
        let access = self
            .schema
            .policies
            .get(collection)
            .map(|policy| {
                Arc::new(Access::new(
                    collection,
                    policy,
                    &self.schema.policies,
                    &self.principal,
                    self.now,
                ))
            });
        self.access.insert(collection.to_owned(), access.clone());
        access
    }

    /// The caller's read restrictions on a collection, if any.
    pub(super) fn read_access(&mut self, collection: &str) -> Option<Arc<Access>> {
        self.access(collection)
            .filter(|access| !access.open_reads())
    }

    /// The caller's write restrictions on a collection, if any.
    pub(super) fn write_access(&mut self, collection: &str) -> Option<Arc<Access>> {
        self.access(collection)
            .filter(|access| !access.open_writes())
    }

    /// A caller's plain scan of a collection whose read rule pins a field to
    /// one string reads that equality bucket through the field's declared
    /// index instead of the whole collection, and depends on the bucket and
    /// its rows alone: other callers' writes don't disturb it. `None` when
    /// no such rule and index apply.
    pub(super) fn bucket_scan(
        &mut self,
        collection: &str,
    ) -> EngineResult<Option<Vec<(String, Arc<Value>)>>> {
        let Some(access) = self.read_access(collection) else {
            return Ok(None);
        };
        let Some((field, value)) = access.bucket() else {
            return Ok(None);
        };
        let query = Query {
            collection: collection.to_owned(),
            fields: vec![field.to_owned()],
            expected: value.clone(),
        };
        if !self.has_index(&query) {
            return Ok(None);
        }
        self.marker_read(indexes::bucket_id(
            &query.collection,
            &query.fields,
            &query.expected,
        ));
        let rows = self.indexed_rows(&query)?;
        for (key, _) in &rows {
            self.record_read(source_id(collection, key));
        }
        self.count_operations(rows.len())?;
        let rows = self.looking_up(|lookup| access.filter(rows, &[], lookup))?;
        self.settle_access(&access)?;
        Ok(Some(rows))
    }

    /// A caller's method may read this derived value only where its access
    /// rule allows. Values without a rule, and reads by code without a
    /// caller or with definer rights, are open.
    pub(super) fn admit_derived_read(&mut self, name: &str, args: &Value) -> EngineResult<()> {
        if self.system || self.definer > 0 {
            return Ok(());
        }
        let Some(rule) = self.schema.derived_access.get(name) else {
            return Ok(());
        };
        let clock = Clock::default();
        let check = Check::new(
            rule,
            &caller(&self.principal),
            self.now,
            &clock,
            &NO_POLICIES,
        );
        let parts = OnceCell::new();
        let allowed = check.holds(Subject {
            key: "",
            parts: &parts,
            row: Some(args),
            next: None,
            clock: &clock,
            rows: &NoRows,
        });
        self.settle_clock(&clock)?;
        if allowed {
            Ok(())
        } else {
            Err(EngineError::new(
                "ACCESS_DENIED",
                format!("Access policy denies reading {name}"),
            ))
        }
    }

    /// Run an access check with the rows its `readable` rules look up, then
    /// record those as read: a result, or an optimistic mutation's decision,
    /// depends on them like on rows the method read itself.
    pub(super) fn looking_up<R>(&mut self, check: impl FnOnce(&dyn Rows) -> R) -> EngineResult<R> {
        let lookup = RowLookup::new(self);
        let result = check(&lookup);
        let reads = lookup.into_reads();
        self.record_lookups(reads)?;
        Ok(result)
    }

    /// Record the rows `readable` rules looked up as read, one operation each.
    pub(super) fn record_lookups(&mut self, reads: Vec<String>) -> EngineResult<()> {
        self.count_operations(reads.len())?;
        for id in reads {
            self.record_read(id);
        }
        Ok(())
    }

    /// Report what the caller's rules made of the clock, as `ctx.now()` and
    /// `ctx.changesAt()` would have: an outcome that read the time can't be
    /// cached or certified, and the result changes at the earliest flip.
    pub(super) fn settle_access(&mut self, access: &Access) -> EngineResult<()> {
        self.settle_clock(&access.clock)
    }

    fn settle_clock(&mut self, clock: &Clock) -> EngineResult<()> {
        let (read, flip) = clock.take();
        if read {
            self.query_cacheable = false;
            self.certifying = false;
            self.clock_polled = true;
        }
        match flip {
            Some(flip) => self.declare_change(&json!(flip)),
            None => Ok(()),
        }
    }
}

/// The fields `ctx.set(…, {clear})` removes instead of carrying over.
pub(super) fn set_clear(options: Option<&Value>) -> EngineResult<Vec<String>> {
    let invalid = || {
        EngineError::new(
            "INVALID_VALUE",
            "set options must be {clear: [field, …]}: at most 256 nonempty field names",
        )
    };
    match options {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(options)) if options.keys().all(|option| option == "clear") => {
            match options.get("clear") {
                None => Ok(Vec::new()),
                Some(Value::Array(fields)) if fields.len() <= 256 => fields
                    .iter()
                    .map(|field| {
                        field
                            .as_str()
                            .filter(|field| !field.is_empty())
                            .map(str::to_owned)
                    })
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(invalid),
                Some(_) => Err(invalid()),
            }
        }
        Some(_) => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(principal: Value) -> Access {
        let policy: Policy = serde_json::from_value(json!({
            "read": {"any":[
                {"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
                {"in":[{"value":"admin"},{"ref":["principal","claims","roles"]}]},
            ]},
            "insert": {"exists":{"ref":["principal","subject"]}},
            "update": {"not":{"eq":[{"ref":["principal","tenant"]},{"value":"frozen"}]}},
            "fields": {"secret": {"read": {"in":[{"value":"admin"},{"ref":["principal","claims","roles"]}]}}},
        }))
        .unwrap();
        Access::new("notes", &policy, &BTreeMap::new(), &principal, 1_000)
    }

    #[test]
    fn rules_about_the_caller_fold_into_constants_once() {
        // For an admin, reads need no per-row work at all.
        let admin = access(json!({"subject":"root","claims":{"roles":["admin"]}}));
        assert!(admin.open_reads());
        assert!(matches!(admin.insert, Check::Const(true)));
        assert!(matches!(admin.update, Check::Const(true)));
        // Anyone else keeps only the row comparison, with the subject inlined.
        let alice = access(json!({"subject":"alice","tenant":"frozen"}));
        assert!(!alice.open_reads());
        assert!(
            matches!(&alice.read, Check::Compare(Op::Eq, Term::Row(path), Term::Value(subject))
            if path == &["owner"] && subject == "alice")
        );
        assert!(matches!(alice.update, Check::Const(false)));
        // Anonymous callers have no subject, so they can't insert.
        let anonymous = access(json!({"subject":"$anonymous","tenant":"t"}));
        assert!(matches!(anonymous.insert, Check::Const(false)));
        assert!(matches!(anonymous.update, Check::Const(true)));
        assert!(!access(Value::Null).visible("k", &json!({"owner":null}), &[], &NoRows));
    }

    #[test]
    fn readable_folds_the_named_collections_read_rule_for_the_caller() {
        let policies: BTreeMap<String, Policy> = serde_json::from_value(json!({
            "sessions": {"read": {"any":[
                {"eq":[{"ref":["principal","claims","role"]},{"value":"staff"}]},
                {"all":[
                    {"exists":{"ref":["principal","subject"]}},
                    {"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
                ]},
            ]}},
            "events": {"read": {"any":[
                {"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]},
                {"readable":["sessions",{"ref":["key","0"]}]},
            ]}},
            // Only the engine's validation keeps readable out of a target's
            // read rule; if one got through, it would look nothing up.
            "nested": {"read": {"readable":["deeper",{"ref":["key"]}]}},
            "deeper": {"read": {"const":true}},
            "via": {"read": {"readable":["nested",{"ref":["key"]}]}},
        }))
        .unwrap();
        let access = |collection: &str, principal: Value| {
            Access::new(collection, &policies[collection], &policies, &principal, 1_000)
        };
        // Admins read every entry without looking a session up.
        assert!(access("events", json!({"subject":"root","claims":{"role":"admin"}})).open_reads());
        // So do staff, whose sessions rule folds to true: they still need the
        // session to exist.
        let staff = access("events", json!({"subject":"s","claims":{"role":"staff"}}));
        assert!(
            matches!(&staff.read, Check::Readable { collection, read, key: Term::KeyPart(path) }
            if collection == "sessions" && matches!(**read, Check::Const(true)) && path == &["0"])
        );
        // Others keep the owner comparison, with their subject inlined.
        let alice = access("events", json!({"subject":"alice"}));
        assert!(
            matches!(&alice.read, Check::Readable { read, .. }
            if matches!(&**read, Check::Compare(Op::Eq, Term::Row(path), Term::Value(subject))
                if path == &["owner"] && subject == "alice"))
        );
        // A caller no session rule can match needs no lookup either.
        assert!(matches!(access("events", Value::Null).read, Check::Const(false)));
        assert!(matches!(access("via", json!({"subject":"alice"})).read, Check::Const(false)));
        // Checks without rows to look into see none.
        assert!(!alice.visible(r#"["s1",1]"#, &json!({}), &[], &NoRows));
        // With rows, the entry shows when its session does.
        struct Sessions;
        impl Rows for Sessions {
            fn readable(&self, collection: &str, key: &str, visible: &dyn Fn(&Value) -> bool) -> bool {
                assert_eq!(collection, "sessions");
                match key {
                    "s1" => visible(&json!({"owner":"alice"})),
                    "s2" => visible(&json!({"owner":"bob"})),
                    _ => false,
                }
            }
        }
        assert!(alice.visible(r#"["s1",1]"#, &json!({}), &[], &Sessions));
        assert!(!alice.visible(r#"["s2",1]"#, &json!({}), &[], &Sessions));
        assert!(!alice.visible(r#"["s3",1]"#, &json!({}), &[], &Sessions));
        assert!(!alice.visible("s1", &json!({}), &[], &Sessions));
        // Validation names what's wrong.
        let error = validate_targets(&policies).unwrap_err();
        assert!(error.contains("via: readable(\"nested\")"), "{error}");
        let mut valid = policies.clone();
        valid.remove("via");
        validate_targets(&valid).unwrap();
        valid.remove("sessions");
        assert!(validate_targets(&valid).unwrap_err().contains("needs a collection with an access policy"));
    }

    /// `cargo test --release --lib specialization_cost -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn specialization_cost() {
        let policy: Policy = serde_json::from_value(json!({
            "read": {"any":[
                {"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
                {"in":[{"value":"admin"},{"ref":["principal","claims","roles"]}]},
            ]},
            "insert": {"eq":[{"ref":["next","owner"]},{"ref":["principal","subject"]}]},
            "update": {"all":[
                {"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
                {"eq":[{"ref":["next","owner"]},{"ref":["row","owner"]}]},
            ]},
            "delete": {"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},
            "fields": {"secret": {"read": {"in":[{"value":"admin"},{"ref":["principal","claims","roles"]}]}}},
        }))
        .unwrap();
        let principal = json!({"subject":"alice","tenant":"t","claims":{"roles":["user"],"email":"a@example.com"}});
        let rounds = 200_000u32;
        let started = std::time::Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(Access::new("notes", &policy, &BTreeMap::new(), &principal, 1_000));
        }
        println!(
            "Access::new for a 5-rule policy: {:?} each",
            started.elapsed() / rounds
        );
    }

    #[test]
    fn comparisons_with_the_clock_flip_at_the_first_instant_their_outcome_changes() {
        let now = 1_000;
        // now < v and now >= v change when now reaches v.
        assert_eq!(flip(Op::Lt, now, &json!(1_500)), Some(1_500));
        assert_eq!(flip(Op::Gte, now, &json!(1_500)), Some(1_500));
        // now <= v and now > v change once now passes v.
        assert_eq!(flip(Op::Lte, now, &json!(1_500)), Some(1_501));
        assert_eq!(flip(Op::Gt, now, &json!(1_500)), Some(1_501));
        // Fractional instants: the first whole millisecond on the other side.
        assert_eq!(flip(Op::Lt, now, &json!(1_500.5)), Some(1_501));
        assert_eq!(flip(Op::Gt, now, &json!(1_500.5)), Some(1_501));
        // Equality holds at v, then stops just after.
        assert_eq!(flip(Op::Eq, now, &json!(1_500)), Some(1_500));
        assert_eq!(flip(Op::Eq, now, &json!(1_000)), Some(1_001));
        // Past instants, non-numbers and prefixes never flip.
        assert_eq!(flip(Op::Lt, now, &json!(900)), None);
        assert_eq!(flip(Op::Gt, now, &json!(1_000)), Some(1_001));
        assert_eq!(flip(Op::Lt, now, &json!("1500")), None);
        assert_eq!(flip(Op::StartsWith, now, &json!(1_500)), None);
        // `v > now` is `now < v`.
        assert_eq!(Op::Gt.swapped(), Op::Lt);
        assert_eq!(Op::Lte.swapped(), Op::Gte);
    }
}
