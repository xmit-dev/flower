//! The database context a callback receives, with the TypeScript SDK's
//! semantics: keyed collections store canonical JSON keys, record schemas
//! validate writes, and mutations run triggers on the rows they change.
use crate::{
    Result, Value,
    app::{Derived, Runtime, TriggerAction},
    fail_with, json,
    schema::Schema,
    type_error,
    wire::{self, Encoder},
};
use alloc::{collections::BTreeMap, format, string::String, vec::Vec};

/// Principal subject that the authorization hook returns for anonymous callers.
pub const ANONYMOUS_SUBJECT: &str = "$anonymous";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Query = 0,
    Mutation = 1,
    Transaction = 2,
    Derived = 3,
}

pub struct IndexDef {
    pub name: &'static str,
    pub fields: &'static [&'static str],
}

/// A collection of JSON records.
#[derive(Clone, Copy)]
pub struct Collection {
    pub name: &'static str,
    /// Keys are JSON values matching this schema, stored as canonical JSON.
    /// Without one, keys are strings.
    pub key: Option<&'static Schema>,
    /// Validates every record written through the context.
    pub value: Option<&'static Schema>,
    pub indexes: &'static [IndexDef],
}

impl Collection {
    pub const fn new(name: &'static str) -> Self {
        Collection {
            name,
            key: None,
            value: None,
            indexes: &[],
        }
    }
    pub const fn key(self, schema: &'static Schema) -> Self {
        Collection {
            key: Some(schema),
            ..self
        }
    }
    pub const fn value(self, schema: &'static Schema) -> Self {
        Collection {
            value: Some(schema),
            ..self
        }
    }
    pub const fn indexes(self, indexes: &'static [IndexDef]) -> Self {
        Collection { indexes, ..self }
    }

    /// A declared index. Panics on an unknown name, a programming error.
    pub fn by(&self, name: &str) -> Index {
        let index = self.indexes.iter().find(|index| index.name == name);
        let index = index.unwrap_or_else(|| panic!("Unknown index {name} on {}", self.name));
        Index {
            collection: *self,
            fields: index.fields,
        }
    }

    /// The stored key: canonical JSON for keyed collections, else the string itself.
    pub fn encode_key(&self, key: &Value) -> Result<String> {
        let Some(schema) = self.key else {
            return match key {
                Value::String(key) => Ok(key.clone()),
                _ => crate::fail("INVALID_KEY", format!("{} keys must be strings", self.name)),
            };
        };
        if let Err(error) = schema.parse(key) {
            return fail_with(
                "INVALID_KEY",
                format!("{} key {}", self.name, error.message()),
                crate::object! {"collection" => self.name},
            );
        }
        Ok(json::canonical(key))
    }

    /// The key a callback sees for a stored key.
    pub fn decode_key(&self, raw: &str) -> Value {
        if self.key.is_some() {
            json::parse(raw).unwrap_or_else(|| Value::from(raw))
        } else {
            Value::from(raw)
        }
    }

    fn encode_reference(&self, encoder: &mut Encoder, indexes: bool) {
        encoder.map(2 + indexes as usize);
        encoder.key("kind");
        encoder.str("collection");
        encoder.key("name");
        encoder.str(self.name);
        if indexes {
            encoder.key("indexes");
            encoder.map(self.indexes.len());
            for index in self.indexes {
                encoder.key(index.name);
                encoder.array(index.fields.len());
                for field in index.fields {
                    encoder.str(field);
                }
            }
        }
    }
}

/// A declared index of a collection.
#[derive(Clone, Copy)]
pub struct Index {
    pub collection: Collection,
    pub fields: &'static [&'static str],
}

/// Bounds for an ordered index walk. Equality on leading fields (`prefix`),
/// then optional bounds on the next field.
#[derive(Clone, Debug, Default)]
pub struct Range {
    pub prefix: Vec<Value>,
    pub gt: Option<Value>,
    pub gte: Option<Value>,
    pub lt: Option<Value>,
    pub lte: Option<Value>,
    pub limit: usize,
    /// A cursor from the previous page.
    pub after: Option<String>,
    pub reverse: Option<bool>,
}

pub struct RangeQuery {
    pub index: Index,
    pub options: Range,
}

pub struct Query {
    pub index: Index,
    pub value: Value,
}

impl Index {
    pub fn range(&self, options: Range) -> RangeQuery {
        RangeQuery {
            index: *self,
            options,
        }
    }
    /// Rows whose index fields equal `value` (a tuple for several fields).
    pub fn eq(&self, value: Value) -> Query {
        Query {
            index: *self,
            value,
        }
    }
    fn encode_head(&self, encoder: &mut Encoder, kind: &str) {
        encoder.map(4);
        encoder.key("kind");
        encoder.str(kind);
        encoder.key("collection");
        encoder.str(self.collection.name);
        encoder.key("fields");
        encoder.array(self.fields.len());
        for field in self.fields {
            encoder.str(field);
        }
    }
}

/// Ordered index components: -0 is 0.
fn scalar(value: &Value) -> Value {
    match value {
        Value::Number(number) if *number == 0.0 => Value::Number(0.0),
        other => other.clone(),
    }
}

impl RangeQuery {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        let options = &self.options;
        let bounds = [
            ("gt", &options.gt),
            ("gte", &options.gte),
            ("lt", &options.lt),
            ("lte", &options.lte),
        ];
        if options.limit < 1
            || options.prefix.len() > self.index.fields.len()
            || (options.prefix.len() == self.index.fields.len()
                && bounds.iter().any(|(_, bound)| bound.is_some()))
            || (options.gt.is_some() && options.gte.is_some())
            || (options.lt.is_some() && options.lte.is_some())
        {
            return type_error("Invalid range bounds");
        }
        self.index.encode_head(encoder, "range");
        encoder.key("options");
        let present = bounds.iter().filter(|(_, bound)| bound.is_some()).count();
        encoder.map(
            2 + present + options.after.is_some() as usize + options.reverse.is_some() as usize,
        );
        encoder.key("prefix");
        encoder.array(options.prefix.len());
        for part in &options.prefix {
            encoder.value(&scalar(part));
        }
        encoder.key("limit");
        encoder.number(options.limit as f64);
        for (name, bound) in bounds {
            if let Some(bound) = bound {
                encoder.key(name);
                encoder.value(&scalar(bound));
            }
        }
        if let Some(after) = &options.after {
            encoder.key("after");
            encoder.str(after);
        }
        if let Some(reverse) = options.reverse {
            encoder.key("reverse");
            encoder.bool(reverse);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub key: Value,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Page {
    pub rows: Vec<Row>,
    pub cursor: Option<String>,
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "flower")]
unsafe extern "C" {
    fn host_call(op: i32, payload: *const u8, length: u32) -> u64;
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn host_call(_op: i32, _payload: *const u8, _length: u32) -> u64 {
    panic!("the Flower host is only available inside a Wasm guest")
}

/// Call a numbered host operation (GUEST_ABI.md) with arguments written by `write`.
pub fn host(op: i32, write: impl FnOnce(&mut Encoder)) -> Result<Value> {
    let mut encoder = Encoder::new();
    write(&mut encoder);
    // SAFETY: the host reads the payload during the call and returns an outcome
    // it allocated in this module's memory with flower_alloc.
    let packed = unsafe { host_call(op, encoder.out.as_ptr(), encoder.out.len() as u32) };
    let bytes = unsafe {
        core::slice::from_raw_parts(packed as u32 as usize as *const u8, (packed >> 32) as usize)
    };
    match wire::outcome(bytes) {
        Ok(result) => result,
        Err(_) => crate::abi::trap(),
    }
}

mod op {
    pub const NOW: i32 = 1;
    pub const PRINCIPAL: i32 = 2;
    pub const HISTORY: i32 = 3;
    pub const GET: i32 = 4;
    pub const SCAN: i32 = 5;
    pub const RANGE: i32 = 6;
    pub const QUERY: i32 = 7;
    pub const SET: i32 = 8;
    pub const DELETE: i32 = 9;
    pub const MATERIALIZE: i32 = 10;
    pub const UNMATERIALIZE: i32 = 11;
    pub const CLOCK: i32 = 12;
    pub const CHANGES_AT: i32 = 13;
}

fn derived_reference(encoder: &mut Encoder, name: &str) {
    encoder.map(2);
    encoder.key("kind");
    encoder.str("derived");
    encoder.key("name");
    encoder.str(name);
}

fn number(value: Value, label: &str) -> Result<f64> {
    match value {
        Value::Number(number) => Ok(number),
        _ => type_error(format!("{label} must be a number")),
    }
}

/// Rows with raw string keys, as the host returns them.
fn raw_rows(value: Value) -> Result<Vec<(String, Value)>> {
    let Value::Array(rows) = value else {
        return type_error("The host returned invalid rows");
    };
    rows.into_iter()
        .map(|row| match row.into_object() {
            Some(mut row) => match (row.remove("key"), row.remove("value")) {
                (Some(Value::String(key)), Some(value)) => Ok((key, value)),
                _ => type_error("The host returned an invalid row"),
            },
            None => type_error("The host returned an invalid row"),
        })
        .collect()
}

struct Touch {
    collection: Collection,
    raw: String,
    /// The row before this round, or, for existence-only collections, whether it existed.
    before: Before,
}

enum Before {
    Exists(bool),
    Row(Value),
}

/// A change to one row, as general triggers see it.
pub struct Change {
    pub key: Value,
    pub before: Value,
    pub after: Value,
}

/// What a callback may read and, in mutations, write.
pub struct Ctx<'r> {
    kind: Kind,
    runtime: &'r Runtime,
    touched: Vec<Touch>,
    /// Row existence this mutation has observed or written, for existence-only collections.
    exists: BTreeMap<(&'static str, String), bool>,
}

impl<'r> Ctx<'r> {
    pub(crate) fn new(kind: Kind, runtime: &'r Runtime) -> Self {
        Ctx {
            kind,
            runtime,
            touched: Vec::new(),
            exists: BTreeMap::new(),
        }
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Trusted server milliseconds, fixed for the invocation. Watches of a
    /// result that reads it poll; prefer `clock` with `changes_at`.
    pub fn now(&mut self) -> Result<f64> {
        number(host(op::NOW, |_| {})?, "Server time")
    }
    /// The same milliseconds as `now`, for code that reports when its result changes.
    pub fn clock(&mut self) -> Result<f64> {
        number(host(op::CLOCK, |_| {})?, "Server time")
    }
    /// The result may change when the clock reaches `time`, even without a write.
    pub fn changes_at(&mut self, time: Option<f64>) -> Result<()> {
        host(op::CHANGES_AT, |encoder| match time {
            Some(time) => encoder.number(time),
            None => encoder.null(),
        })?;
        Ok(())
    }
    /// The authenticated caller, or null.
    pub fn principal(&mut self) -> Result<Value> {
        let principal = host(op::PRINCIPAL, |_| {})?;
        Ok(if principal.text("subject") == Some(ANONYMOUS_SUBJECT) {
            Value::Null
        } else {
            principal
        })
    }
    /// `{database, incarnation}`, or null before retention initialization.
    pub fn history(&mut self) -> Result<Value> {
        host(op::HISTORY, |_| {})
    }

    fn existence(&self, collection: &Collection) -> bool {
        self.runtime.existence.contains(&collection.name)
    }

    fn observe(&mut self, collection: &Collection, raw: &str, exists: bool) {
        if self.existence(collection) {
            self.exists.insert((collection.name, raw.into()), exists);
        }
    }

    /// Read a stored row by its raw key, bypassing key encoding and triggers.
    pub fn raw_get(&mut self, collection: &Collection, raw: &str) -> Result<Value> {
        host(op::GET, |encoder| {
            collection.encode_reference(encoder, false);
            encoder.str(raw);
        })
    }

    /// One record, or null.
    pub fn get(&mut self, collection: &Collection, key: &Value) -> Result<Value> {
        let raw = collection.encode_key(key)?;
        let value = self.raw_get(collection, &raw)?;
        self.observe(collection, &raw, !value.is_null());
        Ok(value)
    }

    /// A derived value; `args` is null for argless definitions.
    pub fn get_derived(&mut self, derived: &Derived, args: &Value) -> Result<Value> {
        host(op::GET, |encoder| {
            derived_reference(encoder, derived.name);
            encoder.value(args);
        })
    }

    /// Scan by raw key order, with the host's scan options.
    pub fn raw_scan(
        &mut self,
        collection: &Collection,
        options: Option<&Value>,
    ) -> Result<Vec<(String, Value)>> {
        raw_rows(host(op::SCAN, |encoder| {
            collection.encode_reference(encoder, options.is_some());
            if let Some(options) = options {
                encoder.value(options);
            }
        })?)
    }

    /// Every row in key order.
    pub fn scan(&mut self, collection: &Collection) -> Result<Vec<Row>> {
        Ok(self
            .raw_scan(collection, None)?
            .into_iter()
            .map(|(key, value)| Row {
                key: collection.decode_key(&key),
                value,
            })
            .collect())
    }

    /// One page of an ordered index walk.
    pub fn range(&mut self, query: &RangeQuery) -> Result<Page> {
        let mut encoded = Encoder::new();
        query.encode(&mut encoded)?;
        let mut page = host(op::RANGE, |encoder| *encoder = encoded)?
            .into_object()
            .unwrap_or_default();
        let rows = raw_rows(page.remove("rows").unwrap_or_default())?;
        let owner = query.index.collection;
        Ok(Page {
            rows: rows
                .into_iter()
                .map(|(key, value)| Row {
                    key: owner.decode_key(&key),
                    value,
                })
                .collect(),
            cursor: match page.remove("cursor") {
                Some(Value::String(cursor)) => Some(cursor),
                _ => None,
            },
        })
    }

    /// Every row of an equality index lookup, as values.
    pub fn query(&mut self, query: &Query) -> Result<Vec<Value>> {
        let result = host(op::QUERY, |encoder| {
            query.index.encode_head(encoder, "query");
            encoder.key("value");
            encoder.value(&query.value);
        })?;
        match result {
            Value::Array(values) => Ok(values),
            _ => type_error("The host returned invalid query results"),
        }
    }

    fn writable(&self, operation: &str) -> Result<()> {
        if self.kind == Kind::Derived {
            return type_error(format!("host.{operation} is not a function"));
        }
        Ok(())
    }

    fn track(&mut self, collection: &Collection, raw: &str) -> Result<()> {
        if !self
            .runtime
            .triggers
            .iter()
            .any(|(source, _)| source.name == collection.name)
            || self
                .touched
                .iter()
                .any(|touch| touch.collection.name == collection.name && touch.raw == raw)
        {
            return Ok(());
        }
        let before = if self.existence(collection) {
            match self.exists.get(&(collection.name, raw.into())) {
                Some(exists) => Before::Exists(*exists),
                None => Before::Exists(!self.raw_get(collection, raw)?.is_null()),
            }
        } else {
            Before::Row(self.raw_get(collection, raw)?)
        };
        self.touched.push(Touch {
            collection: *collection,
            raw: raw.into(),
            before,
        });
        Ok(())
    }

    /// Write a record; record schemas validate it first.
    pub fn set(&mut self, collection: &Collection, key: &Value, value: Value) -> Result<()> {
        self.writable("set")?;
        let raw = collection.encode_key(key)?;
        if let Some(schema) = collection.value
            && let Err(error) = schema.parse(&value)
        {
            return fail_with(
                "INVALID_RECORD",
                format!("{} {}", collection.name, error.message()),
                crate::object! {"collection" => collection.name, "key" => raw.as_str(), "path" => error.path_value()},
            );
        }
        self.track(collection, &raw)?;
        host(op::SET, |encoder| {
            collection.encode_reference(encoder, false);
            encoder.str(&raw);
            encoder.value(&value);
        })?;
        self.observe(collection, &raw, true);
        Ok(())
    }

    pub fn delete(&mut self, collection: &Collection, key: &Value) -> Result<()> {
        self.writable("delete")?;
        let raw = collection.encode_key(key)?;
        self.track(collection, &raw)?;
        host(op::DELETE, |encoder| {
            collection.encode_reference(encoder, false);
            encoder.str(&raw);
        })?;
        self.observe(collection, &raw, false);
        Ok(())
    }

    /// Keep one instance of a derived value maintained.
    pub fn materialize(&mut self, derived: &Derived, args: &Value) -> Result<()> {
        self.writable("materialize")?;
        host(op::MATERIALIZE, |encoder| {
            derived_reference(encoder, derived.name);
            encoder.value(args);
        })?;
        Ok(())
    }

    pub fn unmaterialize(&mut self, derived: &Derived, args: &Value) -> Result<()> {
        self.writable("unmaterialize")?;
        host(op::UNMATERIALIZE, |encoder| {
            derived_reference(encoder, derived.name);
            encoder.value(args);
        })?;
        Ok(())
    }

    /// Run pending triggers now instead of at the end of the mutation.
    pub fn settle(&mut self) -> Result<()> {
        let mut round = 0;
        while !self.touched.is_empty() {
            if round == 32 {
                return crate::fail(
                    "TRIGGER_LOOP",
                    "Triggers kept changing records for 32 rounds",
                );
            }
            round += 1;
            // Read every after value before any trigger of this round runs: a
            // trigger's own writes are tracked for the next round.
            let batch = core::mem::take(&mut self.touched);
            let mut changes = Vec::with_capacity(batch.len());
            for touch in batch {
                let after = if self.existence(&touch.collection) {
                    Before::Exists(
                        self.exists.get(&(touch.collection.name, touch.raw.clone())) == Some(&true),
                    )
                } else {
                    Before::Row(self.raw_get(&touch.collection, &touch.raw)?)
                };
                changes.push((touch, after));
            }
            for (touch, after) in changes {
                let runtime = self.runtime;
                let Some((_, actions)) = runtime
                    .triggers
                    .iter()
                    .find(|(source, _)| source.name == touch.collection.name)
                else {
                    continue;
                };
                match (touch.before, after) {
                    (Before::Exists(before), Before::Exists(after)) => {
                        if before == after {
                            continue;
                        }
                        let key = touch.collection.decode_key(&touch.raw);
                        for action in actions {
                            if let TriggerAction::Materialize(derived) = action {
                                if after {
                                    self.materialize(derived, &key)?;
                                } else {
                                    self.unmaterialize(derived, &key)?;
                                }
                            }
                        }
                    }
                    (Before::Row(before), Before::Row(after)) => {
                        if before == after {
                            continue;
                        }
                        let change = Change {
                            key: touch.collection.decode_key(&touch.raw),
                            before,
                            after,
                        };
                        for action in actions {
                            match action {
                                TriggerAction::Materialize(derived) => {
                                    if change.before.is_null() != change.after.is_null() {
                                        if change.after.is_null() {
                                            self.unmaterialize(derived, &change.key)?;
                                        } else {
                                            self.materialize(derived, &change.key)?;
                                        }
                                    }
                                }
                                TriggerAction::Run(trigger) => (trigger.run)(self, &change)?,
                            }
                        }
                    }
                    _ => unreachable!("a collection's trigger mode is fixed"),
                }
            }
        }
        Ok(())
    }
}
