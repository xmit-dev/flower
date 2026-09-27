//! A queue and its `queue.http()` methods, ported from `sdk/temporal.ts` (`queue`, `view`, `http`).

use std::collections::{BTreeMap, HashMap};

use serde_json::{Value, json};

use super::{FakeError, Obj, canonical_json, int, nullable, object, optional_bool, optional_int, string};

/// Automatic retries after fail() or an expired lease (`QueueRetry`).
#[derive(Clone, Copy, Debug)]
pub struct QueueRetry {
    pub max_attempts: u64,
    pub initial_delay_ms: i64,
    pub max_delay_ms: i64,
}

/// `QueueOptions` plus how `http()` takes the scope.
#[derive(Clone, Debug)]
pub struct QueueConfig {
    pub lease_default_ms: i64,
    pub lease_max_ms: i64,
    /// `None`: `retry: false`, fail() is final.
    pub retry: Option<QueueRetry>,
    pub turn_ms: i64,
    /// `http(prefix, { scope: "argument" })`.
    pub scope_argument: bool,
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig {
            lease_default_ms: 30_000,
            lease_max_ms: 300_000,
            retry: Some(QueueRetry {
                max_attempts: 5,
                initial_delay_ms: 1_000,
                max_delay_ms: 60_000,
            }),
            turn_ms: 1_000,
            scope_argument: false,
        }
    }
}

impl QueueConfig {
    /// `examples/workers.ts`'s `jobs`: leases of 10 s by default, 30 s at most.
    pub fn workers() -> Self {
        QueueConfig {
            lease_default_ms: 10_000,
            lease_max_ms: 30_000,
            ..QueueConfig::default()
        }
    }
}

#[derive(Clone, Debug)]
struct Lease {
    owner: String,
    token: u64,
    expires_at: i64,
    history: Option<Value>,
}

#[derive(Clone, Debug)]
struct Job {
    scope: String,
    id: String,
    payload: Value,
    state: &'static str,
    available_at: Option<i64>,
    lease: Option<Lease>,
    attempts: u64,
    created_at: i64,
    updated_at: i64,
    result: Value,
    error: Value,
    priority: i64,
    group: Option<String>,
    turn: i64,
    queued: Option<&'static str>,
}

impl Job {
    fn to_json(&self) -> Value {
        json!({
            "scope": self.scope,
            "id": self.id,
            "payload": self.payload,
            "state": self.state,
            "availableAt": self.available_at,
            "leaseExpiresAt": self.lease.as_ref().map(|lease| lease.expires_at),
            "lease": self.lease.as_ref().map(lease_json),
            "attempts": self.attempts,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "result": self.result,
            "error": self.error,
            "priority": self.priority,
            "group": self.group,
            "turn": self.turn,
            "queued": self.queued,
        })
    }

    /// `claimOf(job)`.
    fn claim(&self) -> Value {
        let mut claim = lease_json(self.lease.as_ref().expect("a claimed job is leased"));
        let object = claim.as_object_mut().expect("a lease is an object");
        object.insert("scope".into(), json!(self.scope));
        object.insert("id".into(), json!(self.id));
        object.insert("payload".into(), self.payload.clone());
        object.insert("attempt".into(), json!(self.attempts));
        claim
    }
}

fn lease_json(lease: &Lease) -> Value {
    let mut value = json!({ "owner": lease.owner, "token": lease.token, "expiresAt": lease.expires_at });
    if let Some(history) = &lease.history {
        value["history"] = history.clone();
    }
    value
}

#[derive(Clone, Debug)]
struct Waiter {
    owner: String,
    since: i64,
    expires_at: i64,
}

fn queued_at(available_at: i64, now: i64) -> &'static str {
    if available_at <= now { "now" } else { "later" }
}

/// One queue's records: jobs, the line, fencing counters and turns.
pub struct FakeQueue {
    config: QueueConfig,
    jobs: BTreeMap<(String, String), Job>,
    line: BTreeMap<(String, String), Waiter>,
    fencing: HashMap<String, u64>,
    turns: HashMap<(String, i64), i64>,
    group_turns: HashMap<(String, i64, String), i64>,
}

fn lease_lost() -> FakeError {
    FakeError::failure("LEASE_LOST", "Job lease is missing, expired, or held by another claim")
}

impl FakeQueue {
    pub fn new(config: QueueConfig) -> Self {
        FakeQueue {
            config,
            jobs: BTreeMap::new(),
            line: BTreeMap::new(),
            fencing: HashMap::new(),
            turns: HashMap::new(),
            group_turns: HashMap::new(),
        }
    }

    fn backoff(&self, attempts: u64) -> i64 {
        let retry = self.config.retry.expect("backoff needs a retry policy");
        let exponent = (attempts as i64 - 1).clamp(0, 30) as u32;
        retry.max_delay_ms.min(retry.initial_delay_ms.saturating_mul(1 << exponent))
    }

    /// `effective(job, now)`: a lease that ran out leaves its job pending, or failed on its last attempt.
    fn effective(&self, job: &Job, now: i64) -> Job {
        let Some(lease) = job.lease.as_ref().filter(|lease| job.state == "leased" && now >= lease.expires_at) else {
            return job.clone();
        };
        let expired_at = lease.expires_at;
        let exhausted = self.config.retry.is_some_and(|retry| job.attempts >= retry.max_attempts);
        Job {
            state: if exhausted { "failed" } else { "pending" },
            lease: None,
            available_at: if exhausted { None } else { Some(expired_at) },
            updated_at: expired_at,
            queued: if exhausted { None } else { Some("now") },
            error: json!({ "code": "LEASE_EXPIRED", "message": "Worker lease expired", "at": expired_at }),
            ..job.clone()
        }
    }

    fn lease_length(&self, value: Option<i64>) -> Result<i64, FakeError> {
        let lease_ms = value.unwrap_or(self.config.lease_default_ms);
        if lease_ms < 1 {
            return Err(FakeError::invalid("Lease duration must be a safe integer at least 1"));
        }
        if lease_ms > self.config.lease_max_ms {
            return Err(FakeError::failure(
                "LEASE_TOO_LONG",
                &format!("Leases last at most {} ms", self.config.lease_max_ms),
            ));
        }
        Ok(lease_ms)
    }

    fn scoped(&self, scope: &str) -> impl Iterator<Item = &Job> {
        self.jobs
            .range((scope.to_owned(), String::new())..)
            .take_while(move |((job_scope, _), _)| job_scope == scope)
            .map(|(_, job)| job)
    }

    /// Jobs whose lease ran out and that are pending again.
    fn expired_pending(&self, scope: &str, now: i64) -> Vec<Job> {
        let mut jobs: Vec<Job> = self
            .scoped(scope)
            .filter(|job| job.state == "leased" && job.lease.as_ref().is_some_and(|lease| lease.expires_at <= now))
            .map(|job| self.effective(job, now))
            .filter(|job| job.state == "pending")
            .collect();
        jobs.sort_by_key(|job| job.lease_expiry_before());
        jobs
    }

    /// When the longest-waiting claimable job became available, or None when none is.
    fn oldest_ready_at(&self, scope: &str, now: i64) -> Option<i64> {
        let ready = self
            .scoped(scope)
            .filter(|job| job.state == "pending")
            .filter_map(|job| job.available_at)
            .filter(|at| *at <= now)
            .min();
        let expired = self.expired_pending(scope, now).iter().filter_map(|job| job.available_at).min();
        match (ready, expired) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// When a delayed job or a running lease next makes work available.
    fn upcoming(&self, scope: &str, now: i64) -> Option<i64> {
        let delayed = self
            .scoped(scope)
            .filter(|job| job.state == "pending")
            .filter_map(|job| job.available_at)
            .filter(|at| *at > now)
            .min();
        let running = self
            .scoped(scope)
            .filter(|job| job.state == "leased")
            .filter_map(|job| job.lease.as_ref().map(|lease| lease.expires_at))
            .filter(|at| *at > now)
            .min();
        match (delayed, running) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn order(job: &Job) -> (i64, i64, i64, String) {
        (
            job.priority,
            job.turn,
            job.available_at.unwrap_or(i64::MAX),
            canonical_json(&json!([job.scope, job.id])),
        )
    }

    /// `next(ctx, now)`: the job a claim takes next, by priority, then turn, then how long it waited.
    fn next(&mut self, scope: &str, now: i64) -> Option<Job> {
        // Delayed jobs whose time came join the turn order.
        let mut due: Vec<(i64, (String, String))> = self
            .scoped(scope)
            .filter(|job| job.queued == Some("later") && job.available_at.is_some_and(|at| at <= now))
            .map(|job| (job.available_at.unwrap_or_default(), (job.scope.clone(), job.id.clone())))
            .collect();
        due.sort();
        for (_, key) in due.into_iter().take(64) {
            if let Some(job) = self.jobs.get_mut(&key) {
                job.queued = Some("now");
            }
        }
        let mut selected = self
            .scoped(scope)
            .filter(|job| job.queued == Some("now"))
            .min_by_key(|job| Self::order(job))
            .cloned();
        for job in self.expired_pending(scope, now) {
            if selected.as_ref().is_none_or(|current| Self::order(&job) < Self::order(current)) {
                selected = Some(job);
            }
        }
        selected
    }

    fn lease(&mut self, scope: &str, owner: &str, lease_ms: i64, now: i64, history: Option<&Value>) -> Option<Value> {
        let selected = self.next(scope, now)?;
        let token = self.fencing.get(scope).copied().unwrap_or(0) + 1;
        self.fencing.insert(scope.to_owned(), token);
        let clock = (scope.to_owned(), selected.priority);
        if self.turns.get(&clock).is_none_or(|at| *at < selected.turn) {
            self.turns.insert(clock, selected.turn);
        }
        if let Some(group) = &selected.group {
            let key = (scope.to_owned(), selected.priority, group.clone());
            if self.group_turns.get(&key) == Some(&(selected.turn + 1)) {
                self.group_turns.remove(&key);
            }
        }
        let job = Job {
            state: "leased",
            available_at: None,
            lease: Some(Lease {
                owner: owner.to_owned(),
                token,
                expires_at: now + lease_ms,
                history: history.cloned(),
            }),
            attempts: selected.attempts + 1,
            updated_at: now,
            queued: None,
            ..selected
        };
        let claim = job.claim();
        self.jobs.insert((scope.to_owned(), job.id.clone()), job);
        Some(claim)
    }

    /// `place(ctx, priority, group)`: the next turn for a job of this priority and group.
    fn place(&mut self, scope: &str, priority: i64, group: Option<&str>) -> i64 {
        let at = self.turns.get(&(scope.to_owned(), priority)).copied().unwrap_or(0);
        let Some(group) = group else { return at };
        let key = (scope.to_owned(), priority, group.to_owned());
        let turn = at.max(self.group_turns.get(&key).copied().unwrap_or(0));
        self.group_turns.insert(key, turn + 1);
        turn
    }

    /// `head(ctx, now)`: the first owner in line whose place has not run out.
    fn head(&self, scope: &str, now: i64) -> Option<Waiter> {
        let mut waiting: Vec<&Waiter> = self
            .line
            .range((scope.to_owned(), String::new())..)
            .take_while(|((waiter_scope, _), _)| waiter_scope == scope)
            .map(|(_, waiter)| waiter)
            .collect();
        waiting.sort_by(|a, b| (a.since, &a.owner).cmp(&(b.since, &b.owner)));
        waiting.into_iter().find(|waiter| waiter.expires_at > now).cloned()
    }

    fn claim_many(
        &mut self,
        scope: &str,
        owner: &str,
        max: Option<i64>,
        lease_ms: Option<i64>,
        wait_ms: Option<i64>,
        now: i64,
        history: Option<&Value>,
    ) -> Result<Vec<Value>, FakeError> {
        let max = max.unwrap_or(1);
        let lease_ms = self.lease_length(lease_ms)?;
        // First in line, yet it let work wait a whole turn: it is gone or stuck, and loses its place.
        if max > 0
            && let Some(first) = self.head(scope, now)
            && first.owner != owner
            && self
                .oldest_ready_at(scope, now)
                .is_some_and(|since| now >= since + self.config.turn_ms)
        {
            self.line.remove(&(scope.to_owned(), first.owner));
        }
        let mut claims = Vec::new();
        while (claims.len() as i64) < max {
            match self.lease(scope, owner, lease_ms, now, history) {
                Some(claim) => claims.push(claim),
                None => break,
            }
        }
        if let Some(wait_ms) = wait_ms {
            let spot = (scope.to_owned(), owner.to_owned());
            if (claims.len() as i64) < max && wait_ms > 0 {
                let since = match self.line.get(&spot) {
                    Some(waiting) if waiting.expires_at > now => waiting.since,
                    _ => now,
                };
                self.line.insert(
                    spot,
                    Waiter {
                        owner: owner.to_owned(),
                        since,
                        expires_at: now + wait_ms,
                    },
                );
            } else {
                self.line.remove(&spot);
            }
        }
        Ok(claims)
    }

    /// `holds(ctx, identity, now)`.
    fn holds(&self, scope: &str, identity: &Obj, now: i64, history: Option<&Value>) -> Result<Option<Job>, FakeError> {
        let id = string(identity, "id", 1)?;
        let owner = string(identity, "owner", 1)?;
        let token = int(identity, "token", 1, i64::MAX)? as u64;
        let current = canonical_json(history.unwrap_or(&Value::Null));
        let presented = canonical_json(identity.get("history").unwrap_or(&Value::Null));
        let Some(job) = self.jobs.get(&(scope.to_owned(), id.to_owned())) else { return Ok(None) };
        let Some(lease) = &job.lease else { return Ok(None) };
        let held = job.state == "leased"
            && lease.owner == owner
            && lease.token == token
            && now < lease.expires_at
            && presented == current
            && canonical_json(lease.history.as_ref().unwrap_or(&Value::Null)) == current;
        Ok(held.then(|| job.clone()))
    }

    fn held(&self, scope: &str, identity: &Obj, now: i64, history: Option<&Value>) -> Result<Job, FakeError> {
        self.holds(scope, identity, now, history)?.ok_or_else(lease_lost)
    }

    fn store(&mut self, job: Job) -> Value {
        let value = job.to_json();
        self.jobs.insert((job.scope.clone(), job.id.clone()), job);
        value
    }

    /// Several claims come back as the first, carrying the others in `more`.
    fn claimed(claims: Vec<Value>) -> Value {
        let mut claims = claims.into_iter();
        let Some(mut first) = claims.next() else { return Value::Null };
        let more: Vec<Value> = claims.collect();
        if !more.is_empty() {
            first["more"] = Value::Array(more);
        }
        first
    }

    fn next_claim(
        &mut self,
        scope: &str,
        args: &Obj,
        owner: &str,
        report: Value,
        now: i64,
        history: Option<&Value>,
    ) -> Result<Value, FakeError> {
        let Some(next) = args.get("next") else { return Ok(report) };
        let next = object(next, &["leaseMs", "max", "waitMs"], "next")?;
        let claims = self.claim_many(
            scope,
            owner,
            optional_int(next, "max", 0, 64)?,
            optional_int(next, "leaseMs", 1, i64::MAX)?,
            optional_int(next, "waitMs", 0, i64::MAX)?,
            now,
            history,
        )?;
        let mut report = report;
        report["next"] = Self::claimed(claims);
        Ok(report)
    }

    /// Run one generated method.
    pub(crate) fn call(&mut self, method: &str, args: &Value, now: i64, history: Option<&Value>) -> Result<Value, FakeError> {
        let scoped = self.config.scope_argument;
        let with_scope = |keys: &[&'static str]| -> Vec<&'static str> {
            let mut keys = keys.to_vec();
            if scoped {
                keys.push("scope");
            }
            keys
        };
        let scope_of = |object: Option<&Obj>| -> Result<String, FakeError> {
            if !scoped {
                return Ok(String::new());
            }
            match object.and_then(|object| object.get("scope")) {
                Some(Value::String(scope)) => Ok(scope.clone()),
                _ => Err(FakeError::invalid("scope must be a string")),
            }
        };
        const LEASE: [&str; 4] = ["id", "owner", "token", "history"];
        match method {
            "enqueue" => {
                let args = object(args, &with_scope(&["id", "payload", "delayMs", "at", "replace", "priority", "group"]), "args")?;
                let scope = scope_of(Some(args))?;
                let id = string(args, "id", 1)?.to_owned();
                let payload = args.get("payload").cloned().ok_or_else(|| FakeError::invalid("is missing \"payload\""))?;
                let delay_ms = optional_int(args, "delayMs", 0, i64::MAX)?;
                let at = optional_int(args, "at", 0, i64::MAX)?;
                if delay_ms.is_some() && at.is_some() {
                    return Err(FakeError::invalid("Use delayMs or at, not both"));
                }
                let replace = optional_bool(args, "replace")?.unwrap_or(false);
                let priority = optional_int(args, "priority", i64::MIN, i64::MAX)?.unwrap_or(0);
                let group = match args.get("group") {
                    Some(Value::String(group)) if !group.is_empty() => Some(group.clone()),
                    Some(_) => return Err(FakeError::invalid("group must be a nonempty string")),
                    None => None,
                };
                let available_at = at.unwrap_or(now + delay_ms.unwrap_or(0));
                if let Some(stored) = self.jobs.get(&(scope.clone(), id.clone())) {
                    let previous = self.effective(stored, now);
                    if !replace || previous.state == "pending" || previous.state == "leased" {
                        return Err(FakeError::failure("JOB_EXISTS", &format!("Job {id} already exists")));
                    }
                }
                let turn = self.place(&scope, priority, group.as_deref());
                Ok(self.store(Job {
                    scope,
                    id,
                    payload,
                    state: "pending",
                    available_at: Some(available_at),
                    lease: None,
                    attempts: 0,
                    created_at: now,
                    updated_at: now,
                    result: Value::Null,
                    error: Value::Null,
                    priority,
                    group,
                    turn,
                    queued: Some(queued_at(available_at, now)),
                }))
            }
            "claim" => {
                let args = object(args, &with_scope(&["owner", "leaseMs", "max", "waitMs"]), "args")?;
                let scope = scope_of(Some(args))?;
                let owner = string(args, "owner", 1)?.to_owned();
                let claims = self.claim_many(
                    &scope,
                    &owner,
                    optional_int(args, "max", 0, 64)?,
                    optional_int(args, "leaseMs", 1, i64::MAX)?,
                    optional_int(args, "waitMs", 0, i64::MAX)?,
                    now,
                    history,
                )?;
                Ok(Self::claimed(claims))
            }
            "renew" => {
                let args = object(args, &with_scope(&["leases", "leaseMs"]), "args")?;
                let scope = scope_of(Some(args))?;
                let leases = match args.get("leases") {
                    Some(Value::Array(leases)) if leases.len() <= 1_024 => leases,
                    _ => return Err(FakeError::invalid("leases must be an array of at most 1024")),
                };
                let lease_ms = self.lease_length(optional_int(args, "leaseMs", 1, i64::MAX)?)?;
                let identities = leases
                    .iter()
                    .map(|lease| object(lease, &LEASE, "lease"))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut expiries = Vec::new();
                for identity in identities {
                    match self.holds(&scope, identity, now, history)? {
                        None => expiries.push(Value::Null),
                        Some(mut job) => {
                            let lease = job.lease.as_mut().expect("held jobs are leased");
                            lease.expires_at = now + lease_ms;
                            job.updated_at = now;
                            expiries.push(json!(now + lease_ms));
                            self.store(job);
                        }
                    }
                }
                Ok(Value::Array(expiries))
            }
            "complete" => {
                let args = object(args, &with_scope(&["id", "owner", "token", "history", "result", "next"]), "args")?;
                let scope = scope_of(Some(args))?;
                let result = args.get("result").cloned().ok_or_else(|| FakeError::invalid("is missing \"result\""))?;
                let job = self.held(&scope, args, now, history)?;
                let report = self.store(Job {
                    state: "completed",
                    available_at: None,
                    lease: None,
                    updated_at: now,
                    result,
                    error: Value::Null,
                    queued: None,
                    ..job
                });
                let owner = string(args, "owner", 1)?.to_owned();
                self.next_claim(&scope, args, &owner, report, now, history)
            }
            "fail" => {
                let args = object(args, &with_scope(&["id", "owner", "token", "history", "error", "retry", "delayMs", "next"]), "args")?;
                let scope = scope_of(Some(args))?;
                let error = args.get("error").cloned().ok_or_else(|| FakeError::invalid("is missing \"error\""))?;
                let retry = optional_bool(args, "retry")?;
                let delay_ms = optional_int(args, "delayMs", 0, i64::MAX)?;
                let job = self.held(&scope, args, now, history)?;
                let last = self.config.retry.is_none_or(|policy| job.attempts >= policy.max_attempts);
                let last = last || retry == Some(false);
                let available_at = (!last).then(|| now + delay_ms.unwrap_or_else(|| self.backoff(job.attempts)));
                let report = self.store(Job {
                    state: if last { "failed" } else { "pending" },
                    lease: None,
                    updated_at: now,
                    error,
                    available_at,
                    queued: available_at.map(|at| queued_at(at, now)),
                    ..job
                });
                let owner = string(args, "owner", 1)?.to_owned();
                self.next_claim(&scope, args, &owner, report, now, history)
            }
            "release" => {
                let args = object(args, &with_scope(&["id", "owner", "token", "history", "delayMs"]), "args")?;
                let scope = scope_of(Some(args))?;
                let delay_ms = optional_int(args, "delayMs", 0, i64::MAX)?.unwrap_or(0);
                let job = self.held(&scope, args, now, history)?;
                Ok(self.store(Job {
                    state: "pending",
                    lease: None,
                    updated_at: now,
                    available_at: Some(now + delay_ms),
                    queued: Some(queued_at(now + delay_ms, now)),
                    ..job
                }))
            }
            "retry" => {
                let args = object(args, &with_scope(&["id", "delayMs"]), "args")?;
                let scope = scope_of(Some(args))?;
                let id = string(args, "id", 1)?.to_owned();
                let delay_ms = optional_int(args, "delayMs", 0, i64::MAX)?.unwrap_or(0);
                let job = match self.jobs.get(&(scope.clone(), id)) {
                    Some(stored) => self.effective(stored, now),
                    None => return Err(FakeError::failure("JOB_NOT_FAILED", "Only failed jobs can be retried")),
                };
                if job.state != "failed" {
                    return Err(FakeError::failure("JOB_NOT_FAILED", "Only failed jobs can be retried"));
                }
                let turn = self.place(&scope, job.priority, job.group.clone().as_deref());
                Ok(self.store(Job {
                    state: "pending",
                    available_at: Some(now + delay_ms),
                    attempts: 0,
                    updated_at: now,
                    result: Value::Null,
                    turn,
                    queued: Some(queued_at(now + delay_ms, now)),
                    ..job
                }))
            }
            "cancel" => {
                let args = object(args, &with_scope(&["id"]), "args")?;
                let scope = scope_of(Some(args))?;
                let id = string(args, "id", 1)?.to_owned();
                Ok(json!(self.jobs.remove(&(scope, id)).is_some()))
            }
            "get" => {
                let args = object(args, &with_scope(&["id"]), "args")?;
                let scope = scope_of(Some(args))?;
                let id = string(args, "id", 1)?;
                Ok(self
                    .jobs
                    .get(&(scope, id.to_owned()))
                    .map_or(Value::Null, |job| self.effective(job, now).to_json()))
            }
            "ready" => {
                let args = if scoped {
                    Some(object(args, &["scope", "owner"], "args")?)
                } else {
                    nullable(args, &["owner"], "args")?
                };
                let scope = scope_of(args)?;
                let owner = match args.and_then(|args| args.get("owner")) {
                    Some(_) => Some(string(args.expect("owner came from args"), "owner", 1)?.to_owned()),
                    None => None,
                };
                let Some(since) = self.oldest_ready_at(&scope, now) else { return Ok(json!(false)) };
                let Some(owner) = owner else { return Ok(json!(true)) };
                // Whoever waits first in line has new work to itself for turnMs.
                let first = self.head(&scope, now);
                Ok(json!(first.is_none_or(|first| first.owner == owner) || now >= since + self.config.turn_ms))
            }
            "stats" => {
                let args = if scoped {
                    Some(object(args, &["scope", "countUpTo"], "args")?)
                } else {
                    nullable(args, &["countUpTo"], "args")?
                };
                let scope = scope_of(args)?;
                let count_up_to = match args {
                    Some(args) => optional_int(args, "countUpTo", 1, 10_000)?.unwrap_or(100),
                    None => 100,
                } as usize;
                let oldest = self.oldest_ready_at(&scope, now);
                let jobs: Vec<&Job> = self.scoped(&scope).collect();
                let pending = |ready: bool| {
                    jobs.iter()
                        .filter(|job| job.state == "pending" && job.available_at.is_some_and(|at| (at <= now) == ready))
                        .count()
                };
                let expired = self.expired_pending(&scope, now).len().min(count_up_to);
                let leased = jobs
                    .iter()
                    .filter(|job| job.state == "leased" && job.lease.as_ref().is_some_and(|lease| lease.expires_at > now))
                    .count();
                Ok(json!({
                    "ready": oldest.is_some(),
                    "oldestReadyAt": oldest,
                    "nextAvailableAt": self.upcoming(&scope, now),
                    "readyCount": (pending(true).min(count_up_to) + expired).min(count_up_to),
                    "leasedCount": leased.min(count_up_to),
                    "delayedCount": pending(false).min(count_up_to),
                }))
            }
            _ => Err(FakeError::http(404, "METHOD_NOT_FOUND", &format!("No queue method {method}"))),
        }
    }
}

impl Job {
    fn lease_expiry_before(&self) -> (i64, String) {
        (self.available_at.unwrap_or_default(), self.id.clone())
    }
}
