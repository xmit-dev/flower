//! Code-owned admission runs independently of business execution and receipt
//! replay. Credentials never become part of the durable business fingerprint.
use super::*;
use serde::Serialize;
use serde::ser::SerializeMap;
use sha2::{Digest, Sha256};

pub(super) mod memo;
#[cfg(test)]
mod tests;

pub(super) fn required(state: &Snapshot) -> bool {
    state
        .data
        .get("authorizationMethod")
        .is_some_and(|value| !value.is_null())
}

fn denied(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::FORBIDDEN, "FORBIDDEN", message.into())
}

pub(super) async fn authorize_admitted(
    app: &App,
    state: &Snapshot,
    input: &Value,
    permit: &admission::Permit,
) -> Result<Value, ApiError> {
    Ok(authorize_inner(app, state, input, Value::Null, permit)
        .await?
        .principal)
}

/// An access decision, and what can change it without new credentials.
#[derive(Clone)]
pub(super) struct Access {
    pub principal: Value,
    /// How long it holds without a new revision, so an idle watch rechecks
    /// access exactly when a token expires rather than on a timer.
    pub validity: Validity,
    /// The records it read, so a write to them rechecks it; `None` when not
    /// every read was tracked.
    pub observed: Option<Arc<evaluator::DependencyCertificate>>,
}

/// The whole decision, for callers that can reuse it while it holds.
pub(super) async fn authorize_admitted_access(
    app: &App,
    state: &Snapshot,
    input: &Value,
    permit: &admission::Permit,
) -> Result<Access, ApiError> {
    authorize_inner(app, state, input, Value::Null, permit).await
}

impl Access {
    /// Whether this decision, made for the same invocation, still holds on
    /// `state`: nothing it read has changed, and its time has not run out.
    pub(super) fn holds(&self, app: &App, state: &Snapshot) -> bool {
        let Some(observed) = &self.observed else {
            return false;
        };
        let current = match self.validity {
            Validity::Stable => true,
            Validity::Until(time) => app.clock.sample(state).is_ok_and(|now| now < time),
            Validity::Polled => false,
        };
        current && observed.valid(&state.data)
    }
}

pub(super) async fn authorize_watch(
    app: &App,
    state: &Snapshot,
    input: &Value,
    permit: &admission::Permit,
) -> Result<Access, ApiError> {
    authorize_inner(app, state, input, Value::Null, permit).await
}

pub(super) async fn authorize_delegated_admitted(
    app: &App,
    state: &Snapshot,
    input: &Value,
    delegation: Value,
    permit: &admission::Permit,
) -> Result<Value, ApiError> {
    Ok(authorize_inner(app, state, input, delegation, permit)
        .await?
        .principal)
}

async fn authorize_inner(
    app: &App,
    state: &Snapshot,
    input: &Value,
    delegation: Value,
    admission: &admission::Permit,
) -> Result<Access, ApiError> {
    let Some(method) = state
        .data
        .get("authorizationMethod")
        .filter(|value| !value.is_null())
    else {
        return Ok(Access {
            principal: delegation.get("principal").cloned().unwrap_or(Value::Null),
            validity: Validity::Stable,
            observed: Some(Arc::default()),
        });
    };
    let name = method["name"]
        .as_str()
        .ok_or_else(|| denied("Invalid authorization method"))?;
    let partition = app
        .consensus
        .partition_binding()
        .map(|binding| binding.partition.as_str());
    // A hook that reports what it read of the arguments returns a decision,
    // which holds for arguments that agree on that; any other holds for the
    // same arguments. Reuse it while it holds.
    let reported = method.get("result").and_then(Value::as_str) == Some("decision");
    let key = memo::key(input, partition, &delegation);
    if let Some(access) = app.authorizations.get(&key, input.get("args"), app, state) {
        return Ok(access);
    }
    app.authorizations.evaluated();
    let invocation = json!({"name":name,"args":{
        "credentials": input.get("credentials").unwrap_or(&Value::Null),
        "method": input["name"],
        "args": input.get("args").unwrap_or(&Value::Null),
        "partition": partition,
        "delegation": delegation,
    }});
    let data = state.data.clone();
    let now = app.clock.sample(state).map_err(unavailable)?;
    let permit = app
        .query_evaluations
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| unavailable(error.into()))?;
    let admission = admission.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _admission = admission;
        let _permit = permit;
        evaluator::invoke_at(data, invocation, "query", now)
    })
    .await
    .map_err(|error| unavailable(error.into()))?
    .map_err(|error| match engine_failure(&error) {
        Some(failure) => denied("Authorization denied").with_failure(failure),
        None => denied("Authorization denied"),
    })?;
    let validity = Validity::of(&result);
    let observed = result.query_certificate.map(Arc::new);
    // What it read of the arguments: none (`false`), fields by name, or more
    // (`true`), which only calls with the same arguments are known to share,
    // as for a hook that does not say.
    let (principal, read) = if reported {
        let decision = result.value;
        let fields = decision.get("readArgs").and_then(|read| match read {
            Value::Bool(all) => Some((!all).then(Vec::new)),
            Value::Array(fields) => fields
                .iter()
                .map(|field| field.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .map(Some),
            _ => None,
        });
        match (decision.as_object(), fields) {
            (Some(object), Some(fields))
                if object.len() == 2 && object.contains_key("principal") =>
            {
                let read = match fields {
                    Some(mut fields) => {
                        fields.sort_unstable();
                        fields.dedup();
                        memo::Read::Fields(fields)
                    }
                    None => memo::Read::Whole,
                };
                (decision["principal"].clone(), read)
            }
            _ => return Err(denied("Authorization returned an invalid decision")),
        }
    } else {
        (result.value, memo::Read::Whole)
    };
    let Some(object) = principal.as_object() else {
        return Err(denied("Authorization denied"));
    };
    if object
        .keys()
        .any(|key| !["subject", "tenant", "claims"].contains(&key.as_str()))
        || !principal["subject"]
            .as_str()
            .is_some_and(|subject| !subject.is_empty())
        || object
            .get("tenant")
            .is_some_and(|tenant| !tenant.as_str().is_some_and(|value| !value.is_empty()))
    {
        return Err(denied("Authorization returned an invalid principal"));
    }
    if let Some(partition) = partition
        && principal["tenant"].as_str() != Some(partition)
    {
        return Err(denied("Principal is not authorized for this partition"));
    }
    let access = Access {
        principal,
        validity,
        observed,
    };
    app.authorizations
        .insert(key, input.get("args"), read, state.revision, &access);
    Ok(access)
}

pub(super) fn business_input(input: &Value) -> Value {
    let mut value = input.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("credentials");
        // Deployment preparation strategy is scheduling, not logical intent.
        object.remove("preparation");
    }
    value
}

pub(super) fn owner(principal: &Value, operator: bool) -> String {
    crate::consensus::retention::owner_for(
        &serde_json::to_string(&if operator {
            json!(["operator"])
        } else {
            json!(["application", principal["subject"], principal["tenant"]])
        })
        .expect("owner JSON"),
    )
}

pub(super) fn fingerprint(input: &Value, deployment: bool, principal: &Value) -> String {
    let value = Intent {
        deployment,
        // Claims may change as tokens refresh; only the stable subject and
        // tenant own the historical outcome. Null still omits owner entirely.
        owner: (!principal.is_null()).then(|| IntentOwner {
            subject: &principal["subject"],
            tenant: &principal["tenant"],
        }),
        request: BusinessInput(input),
    };
    let mut digest = FingerprintWriter(Sha256::new());
    serde_json::to_writer(&mut digest, &value).expect("JSON fingerprint");
    // Every Raft entry and receipt carries one. 128 bits keep an accidental
    // collision out of reach; only the retries of one request ID compare.
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest.0.finalize()[..16])
}

// Field order matches the old serde_json::Value object's sorted keys exactly.
// Borrowing and streaming avoid cloning the request/claims and allocating a
// throwaway JSON buffer for every admitted mutation or retry.
#[derive(Serialize)]
struct Intent<'a> {
    deployment: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<IntentOwner<'a>>,
    request: BusinessInput<'a>,
}

#[derive(Serialize)]
struct IntentOwner<'a> {
    subject: &'a Value,
    tenant: &'a Value,
}

struct BusinessInput<'a>(&'a Value);

impl Serialize for BusinessInput<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(object) = self.0.as_object() else {
            return self.0.serialize(serializer);
        };
        let count = object.len()
            - usize::from(object.contains_key("credentials"))
            - usize::from(object.contains_key("preparation"));
        let mut map = serializer.serialize_map(Some(count))?;
        for (key, value) in object {
            if key != "credentials" && key != "preparation" {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

struct FingerprintWriter(Sha256);

impl std::io::Write for FingerprintWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0.update(bytes);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
