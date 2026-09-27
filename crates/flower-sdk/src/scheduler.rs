//! Durable callbacks (the TypeScript SDK's `scheduler`): mutation handlers run
//! after a deadline in their own transaction. Deadlines mean "not before"; a
//! handler's writes and the timer's removal commit together.
use crate::{
    Collection, Ctx, IndexDef, Range, Result, Value,
    app::{Component, Method, Task, TaskFailure},
    fail, fail_with, json, object,
    schema::is_safe_integer,
    type_error,
};
use alloc::{format, string::String, vec, vec::Vec};

const INDEXES: &[IndexDef] = &[IndexDef {
    name: "due",
    fields: &["state", "dueAt"],
}];
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

pub struct Scheduler {
    pub name: &'static str,
    /// Timers by ID: `{state, handler, args, dueAt, attempts, error, createdAt, updatedAt}`.
    pub records: Collection,
    pub handlers: &'static [(&'static str, &'static Method)],
    /// Failed attempts before a timer stays failed.
    pub max_attempts: f64,
    /// First retry delay in milliseconds, doubling per failure.
    pub retry_delay_ms: f64,
    pub max_retry_delay_ms: f64,
}

pub(crate) fn integer(value: f64, label: &str, minimum: f64) -> Result<()> {
    if !is_safe_integer(value) || value < minimum {
        return type_error(format!(
            "{label} must be a safe integer at least {}",
            json::number(minimum)
        ));
    }
    Ok(())
}

pub(crate) fn require_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        return type_error(format!("{label} must be a nonempty string"));
    }
    Ok(())
}

impl Scheduler {
    pub const fn new(
        name: &'static str,
        handlers: &'static [(&'static str, &'static Method)],
    ) -> Self {
        Scheduler {
            name,
            records: Collection::new(name).indexes(INDEXES),
            handlers,
            max_attempts: 3.0,
            retry_delay_ms: 1_000.0,
            max_retry_delay_ms: 60_000.0,
        }
    }
    pub const fn retries(
        self,
        max_attempts: f64,
        retry_delay_ms: f64,
        max_retry_delay_ms: f64,
    ) -> Self {
        Scheduler {
            max_attempts,
            retry_delay_ms,
            max_retry_delay_ms,
            ..self
        }
    }

    fn clock(&self, ctx: &mut Ctx) -> Result<f64> {
        let now = ctx.now()?;
        integer(now, "Server time", 0.0)?;
        Ok(now)
    }

    fn handler(&self, alias: &str) -> Result<&'static Method> {
        match self.handlers.iter().find(|(name, _)| *name == alias) {
            Some((_, method)) => Ok(method),
            None => fail(
                "SCHEDULER_HANDLER_MISSING",
                format!("Unknown scheduler handler {}", json::string(alias)),
            ),
        }
    }

    fn timer(id: &Value, stored: &Value) -> Value {
        object! {"id" => id}.with(stored)
    }

    pub fn get(&self, ctx: &mut Ctx, id: &str) -> Result<Value> {
        require_name(id, "Timer ID")?;
        let timer = ctx.get(&self.records, &Value::from(id))?;
        Ok(if timer.is_null() {
            Value::Null
        } else {
            Self::timer(&Value::from(id), &timer)
        })
    }

    fn first(&self, ctx: &mut Ctx, bound: Option<f64>) -> Result<Value> {
        let range = Range {
            prefix: vec![Value::from("pending")],
            lte: bound.map(Value::from),
            limit: 1,
            ..Range::default()
        };
        let page = ctx.range(&self.records.by("due").range(range))?;
        Ok(page
            .rows
            .into_iter()
            .next()
            .map_or(Value::Null, |row| Self::timer(&row.key, &row.value)))
    }

    fn schedule(
        &self,
        ctx: &mut Ctx,
        id: &str,
        due_at: f64,
        handler: &str,
        args: Value,
    ) -> Result<Value> {
        require_name(id, "Timer ID")?;
        integer(due_at, "Deadline", 0.0)?;
        if let Some(schema) = self.handler(handler)?.args
            && let Err(error) = schema.parse(&args)
        {
            return fail_with(
                "INVALID_ARGUMENT",
                format!("{handler} arguments: {}", error.message()),
                object! {"path" => error.path_value()},
            );
        }
        let time = self.clock(ctx)?;
        let key = Value::from(id);
        let previous = ctx.get(&self.records, &key)?;
        let created_at = previous.number("createdAt").unwrap_or(time);
        let timer = object! {
            "state" => "pending", "handler" => handler, "args" => args, "dueAt" => due_at, "attempts" => 0,
            "error" => Value::Null, "createdAt" => created_at, "updatedAt" => time,
        };
        ctx.set(&self.records, &key, timer.clone())?;
        Ok(Self::timer(&key, &timer))
    }

    /// Run `handler(args)` after `delay_ms`. Replacing an ID debounces earlier
    /// work and starts a new retry budget.
    pub fn after(
        &self,
        ctx: &mut Ctx,
        id: &str,
        delay_ms: f64,
        handler: &str,
        args: Value,
    ) -> Result<Value> {
        integer(delay_ms, "Delay", 0.0)?;
        let now = self.clock(ctx)?;
        self.schedule(ctx, id, now + delay_ms, handler, args)
    }

    pub fn at(
        &self,
        ctx: &mut Ctx,
        id: &str,
        due_at: f64,
        handler: &str,
        args: Value,
    ) -> Result<Value> {
        self.schedule(ctx, id, due_at, handler, args)
    }

    pub fn cancel(&self, ctx: &mut Ctx, id: &str) -> Result<bool> {
        if self.get(ctx, id)?.is_null() {
            return Ok(false);
        }
        ctx.delete(&self.records, &Value::from(id))?;
        Ok(true)
    }

    /// Timers in deadline order, optionally of one state ("pending" or "failed").
    pub fn scan(&self, ctx: &mut Ctx, state: Option<&str>) -> Result<Vec<Value>> {
        let states = match state {
            None => vec!["pending", "failed"],
            Some(state @ ("pending" | "failed")) => vec![state],
            Some(_) => return type_error("Unknown timer state filter"),
        };
        let mut timers = Vec::new();
        for current in states {
            let mut after: Option<String> = None;
            loop {
                let range = Range {
                    prefix: vec![Value::from(current)],
                    limit: 64,
                    after: after.take(),
                    ..Range::default()
                };
                let page = ctx.range(&self.records.by("due").range(range))?;
                timers.extend(
                    page.rows
                        .iter()
                        .map(|row| Self::timer(&row.key, &row.value)),
                );
                match page.cursor {
                    Some(cursor) => after = Some(cursor),
                    None => break,
                }
            }
        }
        timers.sort_by(|a, b| {
            let (a_due, b_due) = (
                a.number("dueAt").unwrap_or(0.0),
                b.number("dueAt").unwrap_or(0.0),
            );
            a_due
                .partial_cmp(&b_due)
                .unwrap_or(core::cmp::Ordering::Equal)
                .then_with(|| {
                    json::compare(
                        a.text("id").unwrap_or_default(),
                        b.text("id").unwrap_or_default(),
                    )
                })
        });
        Ok(timers)
    }

    /// Requeue a failed timer with the deployed handler and a fresh retry budget.
    pub fn retry(&self, ctx: &mut Ctx, id: &str, delay_ms: f64) -> Result<Value> {
        integer(delay_ms, "Delay", 0.0)?;
        let timer = self.get(ctx, id)?;
        if timer.text("state") != Some("failed") {
            return fail("TIMER_NOT_FAILED", "Only failed timers can be retried");
        }
        self.handler(timer.text("handler").unwrap_or_default())?;
        let time = self.clock(ctx)?;
        let mut stored = timer.into_object().unwrap_or_default();
        stored.remove("id");
        let mut retried = Value::Object(stored);
        retried.set("state", "pending");
        retried.set("attempts", 0);
        retried.set("error", Value::Null);
        retried.set("dueAt", time + delay_ms);
        retried.set("updatedAt", time);
        ctx.set(&self.records, &Value::from(id), retried.clone())?;
        Ok(Self::timer(&Value::from(id), &retried))
    }
}

impl Task for Scheduler {
    fn name(&self) -> String {
        format!("scheduler:{}", self.name)
    }
    fn due(&self, ctx: &mut Ctx) -> Result<Option<f64>> {
        let this = self;
        Ok(this.first(ctx, None)?.number("dueAt"))
    }
    fn run(&self, ctx: &mut Ctx) -> Result<Value> {
        let this = self;
        let now = this.clock(ctx)?;
        let due = this.first(ctx, Some(now))?;
        if due.is_null() {
            return Ok(Value::Null);
        }
        // Removal is staged before dispatch; a handler may replace its own ID.
        ctx.delete(&this.records, due.get("id"))?;
        this.handler(due.text("handler").unwrap_or_default())?
            .compute(ctx, due.get("args").clone())?;
        Ok(object! {"timer" => due.get("id")})
    }
    fn on_error(&self, ctx: &mut Ctx, failure: &TaskFailure) -> Option<Result<Value>> {
        let this = self;
        Some((|| {
            let now = this.clock(ctx)?;
            let due = this.first(ctx, Some(now))?;
            if due.is_null() {
                return Ok(Value::Null);
            }
            let attempts = due.number("attempts").unwrap_or(0.0) + 1.0;
            let mut delay = this.retry_delay_ms;
            let mut attempt = 1.0;
            while attempt < attempts && delay < this.max_retry_delay_ms {
                delay = this.max_retry_delay_ms.min(delay * 2.0);
                attempt += 1.0;
            }
            let exhausted =
                attempts >= this.max_attempts || failure.failed_at > MAX_SAFE_INTEGER - delay;
            let id = due.get("id").clone();
            let mut previous = due.clone().into_object().unwrap_or_default();
            previous.remove("id");
            let error = &failure.error;
            let mut recorded =
                object! {"code" => error.get("code"), "message" => error.get("message")};
            if error.has("details") {
                recorded.set("details", error.get("details").clone());
            }
            let state = if exhausted { "failed" } else { "pending" };
            let mut timer = Value::Object(previous);
            timer.set("state", state);
            timer.set("attempts", attempts);
            timer.set(
                "dueAt",
                if exhausted {
                    due.get("dueAt").clone()
                } else {
                    Value::from(failure.failed_at + delay)
                },
            );
            timer.set("updatedAt", failure.failed_at);
            timer.set("error", recorded);
            ctx.set(&this.records, &id, timer)?;
            Ok(object! {"timer" => id, "state" => state, "attempts" => attempts})
        })())
    }
}

impl Component for Scheduler {
    fn collections(&'static self) -> Vec<&'static Collection> {
        vec![&self.records]
    }
    fn tasks(&'static self) -> Vec<&'static dyn Task> {
        vec![self]
    }
}
