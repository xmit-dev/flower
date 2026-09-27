//! `docs/reactive-worker.ts`: a `documents` collection and the `digest` external value tracking
//! it, with `external().http("digest")`'s methods, ported from `sdk/external.ts`.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::{FakeError, canonical_json, int, nullable, object, optional_int, string};

#[derive(Clone, Debug)]
struct Stale {
    args: Value,
    since: i64,
    key: Option<String>,
    attempt: Option<u64>,
    owner: Option<String>,
}

/// The documents and the digest's results and stale markers.
pub struct Digest {
    documents: BTreeMap<String, String>,
    results: BTreeMap<String, (String, Value)>,
    stale: BTreeMap<String, Stale>,
    lease_default_ms: i64,
    lease_max_ms: i64,
}

impl Default for Digest {
    fn default() -> Self {
        Digest {
            documents: BTreeMap::new(),
            results: BTreeMap::new(),
            stale: BTreeMap::new(),
            lease_default_ms: 30_000,
            lease_max_ms: 300_000,
        }
    }
}

/// `shardOf(key, count)`: FNV-1a over UTF-16 code units.
pub fn shard_of(key: &str, count: u32) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for unit in key.encode_utf16() {
        hash = (hash ^ unit as u32).wrapping_mul(0x0100_0193);
    }
    hash % count
}

fn row_key(args: &Value) -> String {
    canonical_json(args)
}

impl Digest {
    /// `input(ctx, id)`: `{ recipe: "sha256-v1", text }` for a stored document.
    fn desired(&self, args: &Value) -> Option<(String, Value)> {
        let text = self.documents.get(args.as_str()?)?;
        let input = json!({ "recipe": "sha256-v1", "text": text });
        Some((canonical_json(&json!([args, input])), input))
    }

    fn state(&self, args: &Value) -> Value {
        let Some((key, _)) = self.desired(args) else { return Value::Null };
        match self.results.get(&row_key(args)) {
            Some((stored, value)) if *stored == key => json!({ "status": "ready", "value": value }),
            _ => json!({ "status": "pending" }),
        }
    }

    fn pending(&self, args: &Value) -> Option<Value> {
        let (key, input) = self.desired(args)?;
        if self.results.get(&row_key(args)).is_some_and(|(stored, _)| *stored == key) {
            return None;
        }
        Some(json!({ "args": args, "key": key, "input": input }))
    }

    /// The trigger on `documents`: mark or unmark the row for pools.
    fn changed(&mut self, id: &str, now: i64) {
        let args = json!(id);
        let key = row_key(&args);
        if self.desired(&args).is_none() {
            self.results.remove(&key);
        }
        let found = self.pending(&args);
        match found {
            Some(found) => {
                let found_key = found["key"].as_str().map(str::to_owned);
                let marker = self.stale.get(&key);
                if marker.is_none_or(|marker| marker.key.is_some() && marker.key != found_key) {
                    self.stale.insert(
                        key,
                        Stale {
                            args,
                            since: now,
                            key: None,
                            attempt: None,
                            owner: None,
                        },
                    );
                }
            }
            None => {
                self.stale.remove(&key);
            }
        }
    }

    /// Stale rows by `since`, then key: the `since` index.
    fn by_since(&self) -> Vec<(String, Stale)> {
        let mut rows: Vec<(String, Stale)> = self.stale.iter().map(|(key, row)| (key.clone(), row.clone())).collect();
        rows.sort_by(|a, b| (a.1.since, &a.0).cmp(&(b.1.since, &b.0)));
        rows
    }

    fn lease_length(&self, value: Option<i64>) -> Result<i64, FakeError> {
        let lease_ms = value.unwrap_or(self.lease_default_ms);
        if lease_ms > self.lease_max_ms {
            return Err(FakeError::failure("LEASE_TOO_LONG", &format!("Leases last at most {} ms", self.lease_max_ms)));
        }
        Ok(lease_ms)
    }

    /// `held(ctx, lease, now)`: the row a lease still holds.
    fn held(&self, lease: &super::Obj, now: i64) -> Result<Option<String>, FakeError> {
        let args = lease.get("args").cloned().unwrap_or(Value::Null);
        let owner = string(lease, "owner", 1)?;
        let key = string(lease, "key", 0)?;
        let attempt = int(lease, "attempt", 1, i64::MAX)? as u64;
        let id = row_key(&args);
        let held = self.stale.get(&id).is_some_and(|row| {
            row.owner.as_deref() == Some(owner) && row.key.as_deref() == Some(key) && row.attempt == Some(attempt) && now < row.since
        });
        Ok(held.then_some(id))
    }

    /// Run a method by name; `None` when this app has no such method.
    pub(crate) fn call(&mut self, name: &str, args: &Value, now: i64) -> Option<Result<Value, FakeError>> {
        Some(match name {
            "document.put" => (|| {
                let args = object(args, &["id", "text"], "args")?;
                let id = string(args, "id", 1)?.to_owned();
                let text = string(args, "text", 0)?.to_owned();
                self.documents.insert(id.clone(), text);
                self.changed(&id, now);
                Ok(Value::Null)
            })(),
            "document.delete" => match args.as_str() {
                Some(id) => {
                    self.documents.remove(id);
                    self.changed(id, now);
                    Ok(Value::Null)
                }
                None => Err(FakeError::invalid("args must be a string")),
            },
            "document.get" => match args.as_str() {
                Some(id) => Ok(match self.documents.get(id) {
                    Some(text) => json!({ "text": text, "digest": self.state(args) }),
                    None => Value::Null,
                }),
                None => Err(FakeError::invalid("args must be a string")),
            },
            "digest.pending" => Ok(self.pending(args).unwrap_or(Value::Null)),
            "digest.publish" => (|| {
                let work = object(args, &["args", "key", "value"], "args")?;
                let args = work.get("args").cloned().unwrap_or(Value::Null);
                let key = string(work, "key", 0)?;
                let value = work.get("value").cloned().unwrap_or(Value::Null);
                let valid = value
                    .as_str()
                    .is_some_and(|text| text.len() == 64 && text.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')));
                if !valid {
                    return Err(FakeError::invalid("value must match /^[0-9a-f]{64}$/"));
                }
                let Some((wanted, _)) = self.desired(&args) else { return Ok(json!({ "accepted": false })) };
                if wanted != key {
                    return Ok(json!({ "accepted": false }));
                }
                let id = row_key(&args);
                if self.results.get(&id).is_none_or(|(stored, _)| stored != key) {
                    self.results.insert(id.clone(), (key.to_owned(), value));
                }
                self.stale.remove(&id);
                Ok(json!({ "accepted": true }))
            })(),
            "digest.next" => (|| {
                let options = nullable(args, &["limit", "shard"], "args")?;
                let limit = match options {
                    Some(options) => optional_int(options, "limit", 1, 1_024)?.unwrap_or(16),
                    None => 16,
                } as usize;
                let shard = match options.and_then(|options| options.get("shard")) {
                    None => None,
                    Some(shard) => match shard.as_array().map(|pair| (pair.len(), pair)) {
                        Some((2, pair)) => match (pair[0].as_u64(), pair[1].as_u64()) {
                            (Some(index), Some(count)) if count >= 1 && index < count => Some((index as u32, count as u32)),
                            _ => return Err(FakeError::invalid("shard must be [index, count] with index < count")),
                        },
                        _ => return Err(FakeError::invalid("shard must be [index, count]")),
                    },
                };
                let mut work = Vec::new();
                for (key, row) in self.by_since() {
                    if shard.is_some_and(|(index, count)| shard_of(&key, count) != index) {
                        continue;
                    }
                    if let Some(found) = self.pending(&row.args) {
                        work.push(found);
                    }
                    if work.len() == limit {
                        break;
                    }
                }
                Ok(Value::Array(work))
            })(),
            "digest.claim" => (|| {
                let options = object(args, &["owner", "limit", "leaseMs"], "args")?;
                let owner = string(options, "owner", 1)?.to_owned();
                let limit = optional_int(options, "limit", 1, 1_024)?.unwrap_or(16) as usize;
                let lease_ms = self.lease_length(optional_int(options, "leaseMs", 1, i64::MAX)?)?;
                let mut claims = Vec::new();
                // Every row read is claimed or dropped, so each claim makes progress.
                let rows: Vec<(String, Stale)> = self.by_since().into_iter().filter(|(_, row)| row.since <= now).take(limit).collect();
                for (id, row) in rows {
                    let Some(found) = self.pending(&row.args) else {
                        self.stale.remove(&id);
                        continue;
                    };
                    let found_key = found["key"].as_str().unwrap_or_default().to_owned();
                    let attempt = if row.key.as_deref() == Some(&found_key) { row.attempt.unwrap_or(0) } else { 0 } + 1;
                    let expires_at = now + lease_ms;
                    self.stale.insert(
                        id,
                        Stale {
                            args: row.args.clone(),
                            since: expires_at,
                            key: Some(found_key),
                            attempt: Some(attempt),
                            owner: Some(owner.clone()),
                        },
                    );
                    let mut claim = found;
                    claim["owner"] = json!(owner);
                    claim["attempt"] = json!(attempt);
                    claim["expiresAt"] = json!(expires_at);
                    claims.push(claim);
                }
                Ok(Value::Array(claims))
            })(),
            "digest.renew" => (|| {
                let options = object(args, &["leases", "leaseMs"], "args")?;
                let leases = match options.get("leases") {
                    Some(Value::Array(leases)) if leases.len() <= 1_024 => leases.clone(),
                    _ => return Err(FakeError::invalid("leases must be an array of at most 1024")),
                };
                let lease_ms = self.lease_length(optional_int(options, "leaseMs", 1, i64::MAX)?)?;
                let mut expiries = Vec::new();
                for lease in &leases {
                    let lease = object(lease, &["args", "key", "owner", "attempt"], "lease")?;
                    match self.held(lease, now)? {
                        None => expiries.push(Value::Null),
                        Some(id) => {
                            if let Some(row) = self.stale.get_mut(&id) {
                                row.since = now + lease_ms;
                            }
                            expiries.push(json!(now + lease_ms));
                        }
                    }
                }
                Ok(Value::Array(expiries))
            })(),
            "digest.release" => (|| {
                let lease = object(args, &["args", "key", "owner", "attempt", "delayMs"], "args")?;
                let delay_ms = optional_int(lease, "delayMs", 0, i64::MAX)?.unwrap_or(0);
                let Some(id) = self.held(lease, now)? else { return Ok(json!(false)) };
                if let Some(row) = self.stale.get_mut(&id) {
                    row.owner = None;
                    row.since = now + delay_ms;
                }
                Ok(json!(true))
            })(),
            "digest.ready" => (|| {
                nullable(args, &[], "args")?;
                Ok(json!(self.stale.values().any(|row| row.since <= now)))
            })(),
            "digest.stats" => (|| {
                nullable(args, &[], "args")?;
                let rows = self.by_since();
                let oldest = rows.iter().find(|(_, row)| row.since <= now).map(|(_, row)| row.since);
                let next = rows.iter().find(|(_, row)| row.since > now).map(|(_, row)| row.since);
                Ok(json!({ "ready": oldest.is_some(), "oldestReadyAt": oldest, "nextAvailableAt": next }))
            })(),
            _ => return None,
        })
    }
}
