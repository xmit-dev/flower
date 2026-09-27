//! Operator-only native key catalog. Public requests never contain raw secrets:
//! imports arrive in a wrapping-key encrypted envelope produced off-line.
use super::*;
use crate::crypto::managed;

pub(super) fn validate(input: &Value) -> Result<(), ApiError> {
    let object = input
        .as_object()
        .ok_or_else(|| invalid(anyhow::anyhow!("key operation must be an object")))?;
    let operation = input["operation"]
        .as_str()
        .ok_or_else(|| invalid(anyhow::anyhow!("operation must be a string")))?;
    let fields: &[&str] = match operation {
        "list" | "cache" => &["operation"],
        "generate" => &["requestId", "operation", "name", "algorithm", "bits"],
        "import" => &["requestId", "operation", "name", "algorithm", "sealed"],
        "bind" => &["requestId", "operation", "name", "key", "usages"],
        "unbind" => &["requestId", "operation", "name"],
        "rotate" => &["requestId", "operation", "name", "bits"],
        "revoke" | "retire" | "destroy" | "rewrap" => {
            &["requestId", "operation", "name", "version"]
        }
        _ => return Err(invalid(anyhow::anyhow!("unknown key operation"))),
    };
    if object.keys().any(|field| !fields.contains(&field.as_str())) {
        return Err(invalid(anyhow::anyhow!(
            "unsupported key operation field; raw key uploads are not accepted"
        )));
    }
    if !["list", "cache"].contains(&operation) {
        evaluator::validate_mutation(&json!({"requestId": input.get("requestId")}))
            .map_err(invalid)?;
        if !input["name"].as_str().is_some_and(|name| !name.is_empty()) {
            return Err(invalid(anyhow::anyhow!("name must be a nonempty string")));
        }
    }
    if let Some(bits) = input.get("bits")
        && !bits
            .as_u64()
            .is_some_and(|bits| bits > 0 && bits <= 9_007_199_254_740_991)
    {
        return Err(invalid(anyhow::anyhow!(
            "bits must be a positive safe integer"
        )));
    }
    if operation == "import" && !input["sealed"].is_object() {
        return Err(invalid(anyhow::anyhow!(
            "import requires a sealed envelope from flower key seal"
        )));
    }
    Ok(())
}

pub(super) fn requires_fresh_policy(state: &Snapshot) -> bool {
    state
        .data
        .get("keyDeclarations")
        .is_some_and(|value| match value {
            Value::Array(keys) => !keys.is_empty(),
            Value::Object(keys) => !keys.is_empty(),
            Value::Null => false,
            _ => true,
        })
}

pub(super) async fn handle(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some(format!("Bearer {}", app.admin_token).as_str())
    {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "operator bearer token required".into(),
        ));
    }
    validate(&input)?;
    if input["operation"] == "cache" {
        return cache(&app.consensus)
            .await
            .map(|value| Json(value).into_response());
    }
    forwarding::commit_keys(app, input).await
}

pub(super) async fn cache(consensus: &Consensus) -> Result<Value, ApiError> {
    let state = consensus.snapshot_for(None).await.map_err(unavailable)?;
    Ok(json!({"revision":state.revision,"value":managed::metrics(),"duplicate":false}))
}

pub(super) async fn commit(app: Arc<App>, input: Value) -> Result<Value, ApiError> {
    validate(&input)?;
    if input["operation"] == "cache" {
        return cache(&app.consensus).await;
    }
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let retained = app
        .admission
        .retain(admission::Class::Control, admission::input_bytes(&input))?;
    // The same mutex fences the writer pipeline, timer maintenance and
    // distributed transactions. A policy update cannot overtake staged writes.
    let _guard = app.writer.lock().await;
    let admitted = admission::acquire_retained(&app, admission::Class::Control).await?;
    let request_id = input["requestId"].as_str();
    let state = app
        .consensus
        .read_for(request_id)
        .await
        .map_err(unavailable)?;
    transactions::ensure_unlocked(&state)?;
    if input["operation"] == "list" {
        return Ok(
            json!({"revision":state.revision,"value":managed::public_catalog(state.data.get("managedKeys")).map_err(evaluation_error)?,"duplicate":false}),
        );
    }
    let request_id = request_id.expect("validated request ID").to_owned();
    crate::consensus::retention::validate_request_owner(
        &state,
        &request_id,
        &authorization::owner(&Value::Null, true),
    )
    .map_err(retention::error)?;
    transactions::ensure_request_id_available(&state, &request_id)?;
    let fingerprint = evaluator::hash(&serde_json::to_vec(&json!({"keys":input})).unwrap());
    if let Some(receipt) = state.requests.get(&request_id) {
        if receipt.fingerprint != fingerprint {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "REQUEST_ID_REUSED",
                "requestId was already used for different content".into(),
            ));
        }
        return Ok(json!({"revision":receipt.revision,"value":receipt.result,"duplicate":true}));
    }
    transactions::ensure_write_capacity(&state)?;
    let permit = app
        .evaluations
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| unavailable(error.into()))?;
    let now = app.clock.sample(&state).map_err(unavailable)?;
    let data = state.data.clone();
    let operation = input["operation"].as_str().unwrap().to_owned();
    let name = input["name"].as_str().unwrap().to_owned();
    let (evaluation, value) = tokio::task::spawn_blocking(move || {
        let _retained = retained;
        let _admitted = admitted;
        let _permit = permit;
        let (catalog, value) = managed::prepare(data.get("managedKeys"), &input)?;
        // Native generation/import has completed before the only persisted
        // command is built. Its catalog contains encrypted envelopes only.
        let evaluation = evaluator::update_keys_at(data, catalog, now)?;
        Ok::<_, anyhow::Error>((evaluation, value))
    })
    .await
    .map_err(|error| unavailable(error.into()))?
    .map_err(evaluation_error)?;
    crate::consensus::retention::validate_capacity_for(
        &state,
        &request_id,
        &fingerprint,
        &value,
        0,
    )
    .map_err(retention::error)?;
    let committed = app
        .consensus
        .commit(Commit {
            internal: false,
            request_id,
            fingerprint,
            expected_revision: state.revision,
            puts: evaluation.puts,
            deletes: evaluation.deletes,
            result: value,
        })
        .await
        .map_err(unavailable)?;
    tracing::info!(%operation, %name, revision = committed.revision,
        duplicate = committed.duplicate, "managed key policy committed");
    Ok(
        json!({"revision":committed.revision,"value":committed.result,"duplicate":committed.duplicate}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_requests_reject_plaintext_and_unknown_fields() {
        assert!(validate(&json!({"operation":"list"})).is_ok());
        assert!(validate(&json!({"operation":"generate","requestId":"one","name":"session","algorithm":"Ed25519"})).is_ok());
        for input in [
            json!({"operation":"import","requestId":"one","name":"secret","algorithm":"HS256","key":"plaintext"}),
            json!({"operation":"import","requestId":"one","name":"secret","algorithm":"HS256","sealed":"plaintext"}),
            json!({"operation":"generate","name":"session","algorithm":"Ed25519"}),
            json!({"operation":"list","requestId":"unused"}),
            json!({"operation":"delete","requestId":"one","name":"session"}),
        ] {
            assert!(validate(&input).is_err(), "{input}");
        }
    }

    #[test]
    fn declared_keys_require_fresh_policy_even_for_local_queries() {
        let mut state = Snapshot::default();
        assert!(!requires_fresh_policy(&state));
        state.data.insert("keyDeclarations".into(), json!({}));
        assert!(!requires_fresh_policy(&state));
        state.data.insert(
            "keyDeclarations".into(),
            json!({"sessions":{"algorithm":"Ed25519"}}),
        );
        assert!(requires_fresh_policy(&state));
    }
}
