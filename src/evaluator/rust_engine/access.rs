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
//! `SECURITY DEFINER` views: expose them deliberately.
//!
//! Rules are data, not guest code, so the host evaluates them natively. Each
//! invocation folds its principal into a collection's rules once: a rule that
//! only looks at the caller (`principal.claims.role == "admin"`) becomes a
//! constant and costs nothing per row, and a collection without a policy
//! costs one map lookup per invocation.
use super::*;

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
/// `{"eq":[a,b]}`, `{"ne":[a,b]}`, `{"in":[item,list]}`, `{"exists":a}`.
/// Comparisons with a missing or null side are false, so a row without an
/// `owner` never matches an anonymous caller's missing subject.
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
}

impl Default for Rule {
    fn default() -> Self {
        Self::Const(false)
    }
}

/// `{"value": json}` or `{"ref": path}`, where a path starts with
/// `principal` (`subject`, `tenant`, `claims`…), `row` (the stored row),
/// `next` (the row being written) or is exactly `["key"]`.
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
}

impl Scope {
    fn allows(self, root: &str) -> bool {
        match root {
            "principal" | "key" => true,
            "row" => self != Scope::Insert,
            "next" => matches!(self, Scope::Insert | Scope::Update | Scope::FieldWrite),
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

impl Rule {
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
            Rule::Eq(left, right) | Rule::Ne(left, right) | Rule::In(left, right) => {
                left.validate(scope)?;
                right.validate(scope)
            }
            Rule::Exists(operand) => operand.validate(scope),
        }
    }

    fn cost(&self) -> usize {
        96 + match self {
            Rule::Const(_) => 0,
            Rule::All(rules) | Rule::Any(rules) => rules.iter().map(Rule::cost).sum(),
            Rule::Not(rule) => rule.cost(),
            Rule::Eq(left, right) | Rule::Ne(left, right) | Rule::In(left, right) => {
                left.cost() + right.cost()
            }
            Rule::Exists(operand) => operand.cost(),
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
                "key" => rest.is_empty(),
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

/// A rule with the caller folded in. What remains depends only on the row.
#[derive(Debug)]
enum Check {
    Const(bool),
    All(Vec<Check>),
    Any(Vec<Check>),
    Not(Box<Check>),
    Eq(Term, Term),
    Ne(Term, Term),
    In(Term, Term),
    Exists(Term),
}

#[derive(Debug)]
enum Term {
    Row(Vec<String>),
    Next(Vec<String>),
    Key,
    Value(Value),
}

/// What one check sees: the row's key, the stored row and the row being
/// written, when there are any.
#[derive(Clone, Copy)]
struct Subject<'a> {
    key: &'a str,
    row: Option<&'a Value>,
    next: Option<&'a Value>,
}

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

impl Term {
    fn resolve<'a>(&'a self, subject: Subject<'a>) -> Resolved<'a> {
        match self {
            Term::Value(value) => Resolved::Json(value),
            Term::Key => Resolved::Key(subject.key),
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
}

impl Check {
    fn new(rule: &Rule, caller: &Value) -> Self {
        let term = |operand: &Operand| match operand {
            Operand::Value(value) => Term::Value(value.clone()),
            Operand::Ref(path) => match path[0].as_str() {
                "principal" => {
                    Term::Value(lookup(caller, &path[1..]).cloned().unwrap_or(Value::Null))
                }
                "row" => Term::Row(path[1..].to_vec()),
                "next" => Term::Next(path[1..].to_vec()),
                _ => Term::Key,
            },
        };
        // Comparisons of two constants, such as a claim and a literal, fold now.
        let binary = |left: &Operand, right: &Operand, build: fn(Term, Term) -> Check| {
            let check = build(term(left), term(right));
            match &check {
                Check::Eq(Term::Value(_), Term::Value(_))
                | Check::Ne(Term::Value(_), Term::Value(_))
                | Check::In(Term::Value(_), Term::Value(_)) => Check::Const(check.holds(Subject {
                    key: "",
                    row: None,
                    next: None,
                })),
                _ => check,
            }
        };
        match rule {
            Rule::Const(value) => Check::Const(*value),
            Rule::All(rules) => Self::join(rules, caller, true),
            Rule::Any(rules) => Self::join(rules, caller, false),
            Rule::Not(rule) => match Check::new(rule, caller) {
                Check::Const(value) => Check::Const(!value),
                check => Check::Not(Box::new(check)),
            },
            Rule::Eq(left, right) => binary(left, right, Check::Eq),
            Rule::Ne(left, right) => binary(left, right, Check::Ne),
            Rule::In(left, right) => binary(left, right, Check::In),
            Rule::Exists(operand) => match term(operand) {
                Term::Value(value) => Check::Const(present(Resolved::Json(&value))),
                term => Check::Exists(term),
            },
        }
    }

    /// `all` (a conjunction) or `any`, dropping neutral constants and
    /// short-circuiting on absorbing ones.
    fn join(rules: &[Rule], caller: &Value, conjunction: bool) -> Self {
        let mut checks = Vec::new();
        for rule in rules {
            match Check::new(rule, caller) {
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
            Check::Eq(left, right) => {
                same(left.resolve(subject), right.resolve(subject)) == Some(true)
            }
            Check::Ne(left, right) => {
                same(left.resolve(subject), right.resolve(subject)) == Some(false)
            }
            Check::In(item, list) => contains(item.resolve(subject), list.resolve(subject)),
            Check::Exists(term) => present(term.resolve(subject)),
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
}

impl Access {
    fn new(collection: &str, policy: &Policy, principal: &Value) -> Self {
        // Methods see an anonymous caller as null, and so do rules. Only the
        // tenant, a named partition's name, remains.
        let caller = match principal {
            Value::Object(fields)
                if fields.get("subject").and_then(Value::as_str) == Some("$anonymous") =>
            {
                json!({"tenant": fields.get("tenant").cloned().unwrap_or(Value::Null)})
            }
            Value::Object(_) => principal.clone(),
            _ => json!({}),
        };
        let fields: Vec<FieldCheck> = policy
            .fields
            .iter()
            .map(|(name, field)| FieldCheck {
                name: name.clone(),
                read: field.read.as_ref().map(|rule| Check::new(rule, &caller)),
                write: field.write.as_ref().map(|rule| Check::new(rule, &caller)),
            })
            .collect();
        Self {
            collection: collection.to_owned(),
            read: Check::new(&policy.read, &caller),
            insert: Check::new(&policy.insert, &caller),
            update: Check::new(&policy.update, &caller),
            delete: Check::new(&policy.delete, &caller),
            redacts: fields
                .iter()
                .any(|field| field.read.as_ref().is_some_and(|check| !check.always())),
            fields,
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

    fn field_readable(&self, field: &FieldCheck, key: &str, row: &Value) -> bool {
        field.read.as_ref().is_none_or(|check| {
            check.holds(Subject {
                key,
                row: Some(row),
                next: None,
            })
        })
    }

    /// Whether the caller sees this row at all. `fields` are the index fields
    /// that selected it: finding a row through a field the caller can't read
    /// would reveal that field's value, so such rows stay hidden too.
    pub(super) fn visible(&self, key: &str, row: &Value, fields: &[String]) -> bool {
        self.read.holds(Subject {
            key,
            row: Some(row),
            next: None,
        }) && fields.iter().all(|name| {
            self.fields
                .iter()
                .find(|field| &field.name == name)
                .is_none_or(|field| self.field_readable(field, key, row))
        })
    }

    /// The row without the fields the caller can't read. Shares the stored
    /// value when nothing is hidden.
    pub(super) fn redact(&self, key: &str, row: &Arc<Value>) -> Arc<Value> {
        if !self.redacts {
            return row.clone();
        }
        let Value::Object(entries) = row.as_ref() else {
            return row.clone();
        };
        let hidden: Vec<&str> = self
            .fields
            .iter()
            .filter(|field| {
                entries.contains_key(&field.name) && !self.field_readable(field, key, row)
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

    /// The visible rows, redacted.
    pub(super) fn filter(
        &self,
        rows: Vec<(String, Arc<Value>)>,
        fields: &[String],
    ) -> Vec<(String, Arc<Value>)> {
        rows.into_iter()
            .filter(|(key, row)| self.visible(key, row, fields))
            .map(|(key, row)| {
                let shown = self.redact(&key, &row);
                (key, shown)
            })
            .collect()
    }

    /// Admit a write of `next` (`None` deletes) over `previous`, returning the
    /// value to store. Fields the caller can't read and didn't write keep
    /// their stored values, so a read-modify-write of a redacted row
    /// preserves them. Denied inserts and updates fail identically.
    pub(super) fn admit(
        &self,
        key: &str,
        previous: Option<&Value>,
        next: Option<Value>,
    ) -> EngineResult<Option<Value>> {
        let denied = || {
            EngineError::new(
                "ACCESS_DENIED",
                format!("Access policy denies this write to {}", self.collection),
            )
        };
        let (previous, mut next) = match (previous, next) {
            (None, None) => return Ok(None),
            (Some(previous), None) => {
                let subject = Subject {
                    key,
                    row: Some(previous),
                    next: None,
                };
                return if self.delete.holds(subject) {
                    Ok(None)
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
                    && let Some(value) = entries.get(&field.name)
                    && !self.field_readable(field, key, stored)
                {
                    written.insert(field.name.clone(), value.clone());
                }
            }
        }
        let subject = Subject {
            key,
            row: previous,
            next: Some(&next),
        };
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
                    (None, Some(read)) => read.holds(Subject {
                        key,
                        row: Some(previous.unwrap_or(&next)),
                        next: None,
                    }),
                    (None, None) => true,
                }
        });
        if row.holds(subject) && fields {
            Ok(Some(next))
        } else {
            Err(denied())
        }
    }
}

impl Engine<'_> {
    /// The caller's view of a collection's policy, or `None` when none
    /// applies: invocations without a caller, and collections without one.
    fn access(&mut self, collection: &str) -> Option<Arc<Access>> {
        if self.system || self.schema.policies.is_empty() {
            return None;
        }
        if let Some(access) = self.access.get(collection) {
            return access.clone();
        }
        let access = self
            .schema
            .policies
            .get(collection)
            .map(|policy| Arc::new(Access::new(collection, policy, &self.principal)));
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
        Access::new("notes", &policy, &principal)
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
            matches!(&alice.read, Check::Eq(Term::Row(path), Term::Value(subject))
            if path == &["owner"] && subject == "alice")
        );
        assert!(matches!(alice.update, Check::Const(false)));
        // Anonymous callers have no subject, so they can't insert.
        let anonymous = access(json!({"subject":"$anonymous","tenant":"t"}));
        assert!(matches!(anonymous.insert, Check::Const(false)));
        assert!(matches!(anonymous.update, Check::Const(true)));
        assert!(!access(Value::Null).visible("k", &json!({"owner":null}), &[]));
    }
}
