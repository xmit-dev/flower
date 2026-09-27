//! Leased durable work with fencing tokens, retries, delays and renewal, in
//! ordinary records (the TypeScript SDK's `queue` from `temporal.ts`).
use crate::{
    Collection, Ctx, IndexDef, Range, Result, Row, Value,
    app::{Component, Task},
    fail, json, object,
    scheduler::{integer, require_name},
    schema::{Schema, pow2, v},
    type_error,
};
use alloc::{format, string::String, vec, vec::Vec};
use core::cmp::Ordering;

const KEY: Schema = v::tuple(&[v::string(), v::string().min(1.0)]);
const INDEXES: &[IndexDef] = &[
    IndexDef {
        name: "ready",
        fields: &["scope", "state", "availableAt"],
    },
    IndexDef {
        name: "leases",
        fields: &["scope", "state", "leaseExpiresAt"],
    },
    IndexDef {
        name: "expiry",
        fields: &["state", "leaseExpiresAt"],
    },
];
/// Per queue and scope, the last fencing token issued.
pub static FENCING: Collection = Collection::new("$flower.fencing");

#[derive(Clone, Copy)]
pub struct Retry {
    pub max_attempts: f64,
    pub initial_delay_ms: f64,
    pub max_delay_ms: f64,
}

pub struct Queue {
    pub name: &'static str,
    /// Jobs keyed by `[scope, id]`.
    pub records: Collection,
    pub default_lease_ms: f64,
    pub max_lease_ms: f64,
    /// Automatic retries after fail() or an expired lease. None makes fail() final.
    pub retry: Option<Retry>,
    pub payload: Option<&'static Schema>,
    pub result: Option<&'static Schema>,
}

impl Queue {
    pub const fn new(name: &'static str) -> Self {
        Queue {
            name,
            records: Collection::new(name).key(&KEY).indexes(INDEXES),
            default_lease_ms: 30_000.0,
            max_lease_ms: 300_000.0,
            retry: Some(Retry {
                max_attempts: 5.0,
                initial_delay_ms: 1_000.0,
                max_delay_ms: 60_000.0,
            }),
            payload: None,
            result: None,
        }
    }
    pub const fn lease(self, default_ms: f64, max_ms: f64) -> Self {
        Queue {
            default_lease_ms: default_ms,
            max_lease_ms: max_ms,
            ..self
        }
    }
    pub const fn retry(self, retry: Option<Retry>) -> Self {
        Queue { retry, ..self }
    }
    pub const fn payload(self, schema: &'static Schema) -> Self {
        Queue {
            payload: Some(schema),
            ..self
        }
    }
    pub const fn result(self, schema: &'static Schema) -> Self {
        Queue {
            result: Some(schema),
            ..self
        }
    }

    /// The same queue restricted to one namespace of the shared collection.
    pub fn scope(&self, scope: &str) -> View<'_> {
        View {
            queue: self,
            scope: scope.into(),
        }
    }

    fn backoff(&self, attempts: f64) -> f64 {
        let policy = self.retry.expect("backoff requires a retry policy");
        policy
            .max_delay_ms
            .min(policy.initial_delay_ms * pow2((attempts - 1.0).min(30.0)))
    }

    /// A lease that has expired by `now` returns its job to the queue, or fails it.
    fn effective(&self, job: Value, now: f64) -> Value {
        let expires_at = job.get("lease").number("expiresAt").unwrap_or(f64::NAN);
        if job.text("state") != Some("leased") || now < expires_at {
            return job;
        }
        let exhausted = self
            .retry
            .is_some_and(|policy| job.number("attempts").unwrap_or(0.0) >= policy.max_attempts);
        let mut job = job;
        job.set("state", if exhausted { "failed" } else { "pending" });
        job.set("lease", Value::Null);
        job.set("leaseExpiresAt", Value::Null);
        job.set(
            "availableAt",
            if exhausted {
                Value::Null
            } else {
                Value::from(expires_at)
            },
        );
        job.set("updatedAt", expires_at);
        job.set("error", object! {"code" => "LEASE_EXPIRED", "message" => "Worker lease expired", "at" => expires_at});
        job
    }

    fn validated(schema: Option<&Schema>, value: &Value, label: &str) -> Result<()> {
        if let Some(schema) = schema
            && let Err(error) = schema.parse(value)
        {
            return crate::fail_with(
                "INVALID_ARGUMENT",
                format!("{label} {}", error.message()),
                object! {"path" => error.path_value()},
            );
        }
        Ok(())
    }
}

fn clock(ctx: &mut Ctx) -> Result<f64> {
    let now = ctx.clock()?;
    integer(now, "Server time", 0.0)?;
    Ok(now)
}

fn lease_error<T>() -> Result<T> {
    fail(
        "LEASE_LOST",
        "Job lease is missing, expired, or held by another claim",
    )
}

/// Walk every page of an index range; `visit` returns false to stop.
fn pages(
    ctx: &mut Ctx,
    query: impl Fn(Option<String>) -> crate::RangeQuery,
    mut visit: impl FnMut(&mut Ctx, Row) -> Result<bool>,
) -> Result<()> {
    let mut after = None;
    loop {
        let page = ctx.range(&query(after))?;
        for row in page.rows {
            if !visit(ctx, row)? {
                return Ok(());
            }
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => return Ok(()),
        }
    }
}

/// Options for `View::enqueue`.
#[derive(Clone, Copy, Default)]
pub struct Enqueue {
    pub delay_ms: Option<f64>,
    pub at: Option<f64>,
    pub replace: bool,
}

/// A queue restricted to one scope.
pub struct View<'q> {
    queue: &'q Queue,
    scope: String,
}

impl View<'_> {
    fn key(&self, id: &str) -> Result<Value> {
        require_name(id, "Job ID")?;
        Ok(Value::Array(vec![
            Value::from(&self.scope),
            Value::from(id),
        ]))
    }

    fn range(&self, index: &str, prefix: &[&str], bounds: Range) -> crate::RangeQuery {
        let mut prefix: Vec<Value> = prefix.iter().map(|part| Value::from(*part)).collect();
        prefix.insert(0, Value::from(&self.scope));
        self.queue
            .records
            .by(index)
            .range(Range { prefix, ..bounds })
    }

    fn held(&self, ctx: &mut Ctx, identity: &Value, now: f64) -> Result<Value> {
        if identity.as_object().is_none() {
            return type_error("Lease identity must be a plain object");
        }
        integer(
            identity.number("token").unwrap_or(f64::NAN),
            "Lease token",
            1.0,
        )?;
        let job = ctx.get(
            &self.queue.records,
            &self.key(identity.text("id").unwrap_or_default())?,
        )?;
        let history = ctx.history()?;
        let lease = job.get("lease");
        if job.is_null()
            || job.text("state") != Some("leased")
            || lease.get("owner") != identity.get("owner")
            || lease.get("token") != identity.get("token")
            || now >= lease.number("expiresAt").unwrap_or(f64::NAN)
            || json::canonical(identity.get("history")) != json::canonical(&history)
            || json::canonical(lease.get("history")) != json::canonical(&history)
        {
            return lease_error();
        }
        Ok(job)
    }

    /// A running lease changes the job at its expiry, whether or not anyone claims it again.
    fn current(&self, ctx: &mut Ctx, job: Value, now: f64) -> Result<Value> {
        let expires_at = job.get("lease").number("expiresAt");
        if job.text("state") == Some("leased")
            && let Some(expires_at) = expires_at
            && now < expires_at
        {
            ctx.changes_at(Some(expires_at))?;
        }
        Ok(self.queue.effective(job, now))
    }

    /// When a delayed job or a running lease next makes work available.
    fn upcoming(&self, ctx: &mut Ctx, now: f64) -> Result<Option<f64>> {
        let bounds = Range {
            gt: Some(Value::from(now)),
            limit: 1,
            ..Range::default()
        };
        let delayed = ctx
            .range(&self.range("ready", &["pending"], bounds.clone()))?
            .rows
            .first()
            .and_then(|row| row.value.number("availableAt"));
        let running = ctx
            .range(&self.range("leases", &["leased"], bounds))?
            .rows
            .first()
            .and_then(|row| row.value.number("leaseExpiresAt"));
        let next = match (delayed, running) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (time, None) | (None, time) => time,
        };
        ctx.changes_at(next)?;
        Ok(next)
    }

    fn claim_of(&self, job: &Value) -> Value {
        let mut claim = object! {"scope" => &self.scope, "id" => job.get("id"), "payload" => job.get("payload")}.with(job.get("lease"));
        claim.set("attempt", job.get("attempts").clone());
        claim
    }

    fn expired_pending(&self, ctx: &mut Ctx, now: f64) -> Result<Option<Value>> {
        let mut found = None;
        let queue = self.queue;
        pages(
            ctx,
            |after| {
                self.range(
                    "leases",
                    &["leased"],
                    Range {
                        lte: Some(Value::from(now)),
                        limit: 64,
                        after,
                        ..Range::default()
                    },
                )
            },
            |_, row| {
                let job = queue.effective(row.value, now);
                if job.text("state") == Some("pending") {
                    found = Some(job);
                    return Ok(false);
                }
                Ok(true)
            },
        )?;
        Ok(found)
    }

    fn oldest(&self, ctx: &mut Ctx, now: f64) -> Result<Option<Value>> {
        let ready = Range {
            lte: Some(Value::from(now)),
            limit: 1,
            ..Range::default()
        };
        let mut selected = ctx
            .range(&self.range("ready", &["pending"], ready))?
            .rows
            .into_iter()
            .next()
            .map(|row| row.value);
        let queue = self.queue;
        pages(
            ctx,
            |after| {
                self.range(
                    "leases",
                    &["leased"],
                    Range {
                        lte: Some(Value::from(now)),
                        limit: 64,
                        after,
                        ..Range::default()
                    },
                )
            },
            |_, row| {
                let job = queue.effective(row.value, now);
                if job.text("state") != Some("pending") {
                    return Ok(true);
                }
                let earlier = match &selected {
                    None => true,
                    Some(chosen) => {
                        let (at, chosen_at) = (
                            job.number("availableAt").unwrap_or(0.0),
                            chosen.number("availableAt").unwrap_or(0.0),
                        );
                        at < chosen_at
                            || (at == chosen_at
                                && json::compare(
                                    &json::canonical(
                                        &self.key(job.text("id").unwrap_or_default())?,
                                    ),
                                    &json::canonical(
                                        &self.key(chosen.text("id").unwrap_or_default())?,
                                    ),
                                ) == Ordering::Less)
                    }
                };
                if earlier {
                    selected = Some(job);
                }
                Ok(true)
            },
        )?;
        Ok(selected)
    }

    pub fn enqueue(
        &self,
        ctx: &mut Ctx,
        id: &str,
        payload: Value,
        options: Enqueue,
    ) -> Result<Value> {
        Queue::validated(self.queue.payload, &payload, "Payload")?;
        let now = clock(ctx)?;
        if options.delay_ms.is_some() && options.at.is_some() {
            return type_error("Use delayMs or at, not both");
        }
        if let Some(delay) = options.delay_ms {
            integer(delay, "delayMs", 0.0)?;
        }
        if let Some(at) = options.at {
            integer(at, "at", 0.0)?;
        }
        let available_at = options.at.unwrap_or(now + options.delay_ms.unwrap_or(0.0));
        let key = self.key(id)?;
        let stored = ctx.get(&self.queue.records, &key)?;
        if !stored.is_null() {
            let previous = self.queue.effective(stored, now);
            if !options.replace || matches!(previous.text("state"), Some("pending" | "leased")) {
                return fail("JOB_EXISTS", format!("Job {id} already exists"));
            }
        }
        let job = object! {
            "scope" => &self.scope, "id" => id, "payload" => payload, "state" => "pending", "availableAt" => available_at,
            "leaseExpiresAt" => Value::Null, "lease" => Value::Null, "attempts" => 0, "createdAt" => now, "updatedAt" => now,
            "result" => Value::Null, "error" => Value::Null,
        };
        ctx.set(&self.queue.records, &key, job.clone())?;
        Ok(job)
    }

    /// Lease the job that has waited longest, or return null.
    pub fn claim(&self, ctx: &mut Ctx, owner: &str, lease_ms: Option<f64>) -> Result<Value> {
        require_name(owner, "Lease owner")?;
        let lease_ms = lease_ms.unwrap_or(self.queue.default_lease_ms);
        integer(lease_ms, "Lease duration", 1.0)?;
        if lease_ms > self.queue.max_lease_ms {
            return fail(
                "LEASE_TOO_LONG",
                format!(
                    "Leases last at most {} ms",
                    json::number(self.queue.max_lease_ms)
                ),
            );
        }
        let now = clock(ctx)?;
        let Some(selected) = self.oldest(ctx, now)? else {
            return Ok(Value::Null);
        };
        let counter = Value::from(json::canonical(&crate::array![
            self.queue.name,
            self.scope.as_str()
        ]));
        let token = ctx.get(&FENCING, &counter)?.number("last").unwrap_or(0.0) + 1.0;
        integer(token, "Fencing token", 1.0)?;
        ctx.set(&FENCING, &counter, object! {"last" => token})?;
        let history = ctx.history()?;
        let mut lease = object! {"owner" => owner, "token" => token, "expiresAt" => now + lease_ms};
        if !history.is_null() {
            lease.set("history", history);
        }
        let id = String::from(selected.text("id").unwrap_or_default());
        let attempts = selected.number("attempts").unwrap_or(0.0) + 1.0;
        let mut job = selected;
        job.set("state", "leased");
        job.set("availableAt", Value::Null);
        job.set("leaseExpiresAt", now + lease_ms);
        job.set("lease", lease);
        job.set("attempts", attempts);
        job.set("updatedAt", now);
        ctx.set(&self.queue.records, &self.key(&id)?, job.clone())?;
        Ok(self.claim_of(&job))
    }

    /// Extend a current lease; the fencing token stays the same.
    pub fn renew(&self, ctx: &mut Ctx, identity: &Value, lease_ms: Option<f64>) -> Result<Value> {
        let lease_ms = lease_ms.unwrap_or(self.queue.default_lease_ms);
        integer(lease_ms, "Lease duration", 1.0)?;
        if lease_ms > self.queue.max_lease_ms {
            return fail(
                "LEASE_TOO_LONG",
                format!(
                    "Leases last at most {} ms",
                    json::number(self.queue.max_lease_ms)
                ),
            );
        }
        let now = clock(ctx)?;
        let mut job = self.held(ctx, identity, now)?;
        let mut lease = job.get("lease").clone();
        lease.set("expiresAt", now + lease_ms);
        job.set("lease", lease);
        job.set("leaseExpiresAt", now + lease_ms);
        job.set("updatedAt", now);
        let id = String::from(job.text("id").unwrap_or_default());
        ctx.set(&self.queue.records, &self.key(&id)?, job.clone())?;
        Ok(self.claim_of(&job))
    }

    pub fn complete(&self, ctx: &mut Ctx, identity: &Value, result: Value) -> Result<Value> {
        Queue::validated(self.queue.result, &result, "Result")?;
        let now = clock(ctx)?;
        let mut job = self.held(ctx, identity, now)?;
        job.set("state", "completed");
        job.set("availableAt", Value::Null);
        job.set("lease", Value::Null);
        job.set("leaseExpiresAt", Value::Null);
        job.set("updatedAt", now);
        job.set("result", result);
        job.set("error", Value::Null);
        let id = String::from(job.text("id").unwrap_or_default());
        ctx.set(&self.queue.records, &self.key(&id)?, job.clone())?;
        Ok(job)
    }

    /// Retries with backoff unless retry is disabled or attempts are exhausted.
    pub fn fail(
        &self,
        ctx: &mut Ctx,
        identity: &Value,
        error: Value,
        retry: Option<bool>,
        delay_ms: Option<f64>,
    ) -> Result<Value> {
        if let Some(delay) = delay_ms {
            integer(delay, "delayMs", 0.0)?;
        }
        let now = clock(ctx)?;
        let mut job = self.held(ctx, identity, now)?;
        let attempts = job.number("attempts").unwrap_or(0.0);
        let last = match self.queue.retry {
            None => true,
            Some(policy) => retry == Some(false) || attempts >= policy.max_attempts,
        };
        job.set("state", if last { "failed" } else { "pending" });
        job.set("lease", Value::Null);
        job.set("leaseExpiresAt", Value::Null);
        job.set("updatedAt", now);
        job.set("error", error);
        job.set(
            "availableAt",
            if last {
                Value::Null
            } else {
                Value::from(now + delay_ms.unwrap_or_else(|| self.queue.backoff(attempts)))
            },
        );
        let id = String::from(job.text("id").unwrap_or_default());
        ctx.set(&self.queue.records, &self.key(&id)?, job.clone())?;
        Ok(job)
    }

    /// Requeue a failed job with a fresh attempt budget.
    pub fn retry(&self, ctx: &mut Ctx, id: &str, delay_ms: Option<f64>) -> Result<Value> {
        let delay_ms = delay_ms.unwrap_or(0.0);
        integer(delay_ms, "delayMs", 0.0)?;
        let now = clock(ctx)?;
        let key = self.key(id)?;
        let stored = ctx.get(&self.queue.records, &key)?;
        let job = if stored.is_null() {
            Value::Null
        } else {
            self.queue.effective(stored, now)
        };
        if job.text("state") != Some("failed") {
            return fail("JOB_NOT_FAILED", "Only failed jobs can be retried");
        }
        let mut job = job;
        job.set("state", "pending");
        job.set("availableAt", now + delay_ms);
        job.set("attempts", 0);
        job.set("updatedAt", now);
        job.set("result", Value::Null);
        ctx.set(&self.queue.records, &key, job.clone())?;
        Ok(job)
    }

    pub fn cancel(&self, ctx: &mut Ctx, id: &str) -> Result<bool> {
        let key = self.key(id)?;
        if ctx.get(&self.queue.records, &key)?.is_null() {
            return Ok(false);
        }
        ctx.delete(&self.queue.records, &key)?;
        Ok(true)
    }

    pub fn get(&self, ctx: &mut Ctx, id: &str) -> Result<Value> {
        let job = ctx.get(&self.queue.records, &self.key(id)?)?;
        if job.is_null() {
            return Ok(job);
        }
        let now = clock(ctx)?;
        self.current(ctx, job, now)
    }

    /// Every job of this scope, by ID.
    pub fn scan(&self, ctx: &mut Ctx) -> Result<Vec<Value>> {
        let now = clock(ctx)?;
        let mut jobs = Vec::new();
        pages(
            ctx,
            |after| {
                self.range(
                    "ready",
                    &[],
                    Range {
                        limit: 64,
                        after,
                        ..Range::default()
                    },
                )
            },
            |ctx, row| {
                jobs.push(self.current(ctx, row.value, now)?);
                Ok(true)
            },
        )?;
        jobs.sort_by(|a, b| {
            json::compare(
                a.text("id").unwrap_or_default(),
                b.text("id").unwrap_or_default(),
            )
        });
        Ok(jobs)
    }

    /// A claim would succeed now.
    pub fn ready(&self, ctx: &mut Ctx) -> Result<bool> {
        let now = clock(ctx)?;
        // Time only adds ready jobs, so a true answer holds until the next write.
        let bounds = Range {
            lte: Some(Value::from(now)),
            limit: 1,
            ..Range::default()
        };
        if !ctx
            .range(&self.range("ready", &["pending"], bounds))?
            .rows
            .is_empty()
            || self.expired_pending(ctx, now)?.is_some()
        {
            return Ok(true);
        }
        self.upcoming(ctx, now)?;
        Ok(false)
    }

    /// `{ready, oldestReadyAt, nextAvailableAt}`.
    pub fn stats(&self, ctx: &mut Ctx) -> Result<Value> {
        let now = clock(ctx)?;
        let found = self.oldest(ctx, now)?;
        let oldest = found
            .as_ref()
            .map_or(Value::Null, |job| job.get("availableAt").clone());
        Ok(
            object! {"ready" => found.is_some(), "oldestReadyAt" => oldest, "nextAvailableAt" => self.upcoming(ctx, now)?},
        )
    }
}

impl Task for Queue {
    fn name(&self) -> String {
        format!("queue:{}", self.name)
    }
    fn due(&self, ctx: &mut Ctx) -> Result<Option<f64>> {
        let range = Range {
            prefix: vec![Value::from("leased")],
            limit: 1,
            ..Range::default()
        };
        let page = ctx.range(&self.records.by("expiry").range(range))?;
        Ok(page
            .rows
            .first()
            .and_then(|row| row.value.number("leaseExpiresAt")))
    }
    fn run(&self, ctx: &mut Ctx) -> Result<Value> {
        let now = clock(ctx)?;
        let range = Range {
            prefix: vec![Value::from("leased")],
            lte: Some(Value::from(now)),
            limit: 64,
            ..Range::default()
        };
        let rows = ctx.range(&self.records.by("expiry").range(range))?.rows;
        let reclaimed = rows.len();
        for row in rows {
            let job = self.effective(row.value, now);
            ctx.set(&self.records, &row.key, job)?;
        }
        Ok(object! {"reclaimed" => reclaimed})
    }
}

impl Component for Queue {
    fn collections(&'static self) -> Vec<&'static Collection> {
        vec![&self.records, &FENCING]
    }
    fn tasks(&'static self) -> Vec<&'static dyn Task> {
        vec![self]
    }
}
