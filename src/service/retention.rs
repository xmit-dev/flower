//! Retry identity metadata and operator-controlled replicated retirement.
use super::*;
use crate::consensus::retention as protocol;

pub(super) fn error(error: anyhow::Error) -> ApiError {
    let message = error.to_string();
    let code = message.split(':').next().unwrap_or_default();
    let code = match code {
        "RETRY_WINDOW_EXPIRED" => "RETRY_WINDOW_EXPIRED",
        "HISTORY_MISMATCH" => "HISTORY_MISMATCH",
        "REQUEST_DATABASE_MISMATCH" => "REQUEST_DATABASE_MISMATCH",
        "REQUEST_ID_SCOPE_REQUIRED" => "REQUEST_ID_SCOPE_REQUIRED",
        "REQUEST_ID_INVALID" => "REQUEST_ID_INVALID",
        "RETRY_EPOCH_NOT_ADMITTED" => "RETRY_EPOCH_NOT_ADMITTED",
        "RETENTION_NOT_INITIALIZED" => "RETENTION_NOT_INITIALIZED",
        "RECEIPT_BUDGET_EXCEEDED" => "RECEIPT_BUDGET_EXCEEDED",
        "ALREADY_ACKNOWLEDGED" => "ALREADY_ACKNOWLEDGED",
        "RETRY_SESSION_CLOSED" => "RETRY_SESSION_CLOSED",
        "RETRY_SESSION_UNKNOWN" => "RETRY_SESSION_UNKNOWN",
        "RETRY_SESSION_MISMATCH" => "RETRY_SESSION_MISMATCH",
        "RETRY_SESSION_FORBIDDEN" => "RETRY_SESSION_FORBIDDEN",
        "RETRY_SESSION_EXISTS" => "RETRY_SESSION_EXISTS",
        "RETRY_ACK_GAP" => "RETRY_ACK_GAP",
        "RETRY_ACK_BUDGET" => "RETRY_ACK_BUDGET",
        "RETENTION_TRANSACTION_ACTIVE" => "RETENTION_TRANSACTION_ACTIVE",
        "RETENTION_ROTATION_INVALID" => "RETENTION_ROTATION_INVALID",
        _ => "RETENTION_CONFLICT",
    };
    ApiError::new(StatusCode::CONFLICT, code, message)
}

pub(super) fn validate_session(input: &Value) -> Result<(), ApiError> {
    let object = input
        .as_object()
        .ok_or_else(|| invalid(anyhow::anyhow!("session request must be an object")))?;
    if object.keys().any(|key| {
        ![
            "operation",
            "session",
            "incarnation",
            "epoch",
            "through",
            "limit",
            "abandon",
            "credentials",
        ]
        .contains(&key.as_str())
    }) || !matches!(
        input["operation"].as_str(),
        Some("open" | "ack" | "close" | "status")
    ) {
        return Err(invalid(anyhow::anyhow!(
            "invalid session operation or field"
        )));
    }
    if !input["session"].as_str().is_some_and(|id| {
        id.len() == 32
            && id
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    }) || !input["incarnation"].is_string()
    {
        return Err(invalid(anyhow::anyhow!(
            "session requires a 32-character lowercase hex ID and incarnation"
        )));
    }
    Ok(())
}

pub(super) async fn session_handle(
    State(app): State<Arc<App>>,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    validate_session(&input)?;
    forwarding::commit_session(app, input).await
}

pub(super) async fn session_commit(app: Arc<App>, input: Value) -> Result<Value, ApiError> {
    validate_session(&input)?;
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let _input = app
        .admission
        .retain(admission::Class::User, admission::input_bytes(&input))?;
    let _writer = app.writer.lock().await;
    let admission = admission::acquire_retained(&app, admission::Class::User).await?;
    let snapshot = app.consensus.read_for_writer().await.map_err(unavailable)?;
    if !authorization::required(&snapshot) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            "retry sessions require a deployed authorization hook".into(),
        ));
    }
    let authorization_input = json!({"name":format!("$flower.session.{}",input["operation"].as_str().unwrap()),
        "args":authorization::business_input(&input),"credentials":input["credentials"]});
    let principal =
        authorization::authorize_admitted(&app, &snapshot, &authorization_input, &admission)
            .await?;
    let owner = authorization::owner(&principal, false);
    let state = protocol::status(&snapshot).map_err(error)?.ok_or_else(|| {
        error(anyhow::anyhow!(
            "RETENTION_NOT_INITIALIZED: initialize retention first"
        ))
    })?;
    let incarnation = input["incarnation"].as_str().unwrap().to_owned();
    if incarnation != state.incarnation {
        return Err(error(anyhow::anyhow!(
            "HISTORY_MISMATCH: session belongs to another history"
        )));
    }
    let id = input["session"].as_str().unwrap().to_owned();
    let existing = protocol::session(&snapshot, &id).map_err(error)?;
    if let Some(session) = &existing {
        if session.incarnation != state.incarnation {
            return Err(error(anyhow::anyhow!(
                "HISTORY_MISMATCH: session belongs to another history"
            )));
        }
        if session.owner != owner {
            return Err(error(anyhow::anyhow!(
                "RETRY_SESSION_FORBIDDEN: session belongs to another principal"
            )));
        }
        if session.epoch < state.min_epoch {
            return Err(error(anyhow::anyhow!(
                "RETRY_WINDOW_EXPIRED: session epoch was retired"
            )));
        }
    }
    let response = |session: protocol::Session, revision| {
        json!({"revision":revision,"value":{
            "database":state.database,"incarnation":session.incarnation,"id":session.id,"epoch":session.epoch,
            "acknowledgedThrough":session.acknowledged_through,"closed":session.closed,
        }})
    };
    let operation = input["operation"].as_str().unwrap();
    if operation == "status" {
        return existing
            .map(|session| response(session, snapshot.revision))
            .ok_or_else(|| {
                error(anyhow::anyhow!(
                    "RETRY_SESSION_UNKNOWN: session does not exist"
                ))
            });
    }
    let safe = |name: &str| {
        input[name]
            .as_u64()
            .filter(|n| *n <= 9_007_199_254_740_991)
            .ok_or_else(|| invalid(anyhow::anyhow!("{name} must be a nonnegative safe integer")))
    };
    let action = match operation {
        "open" => {
            let epoch = safe("epoch")?;
            if let Some(session) = existing {
                if session.epoch != epoch || session.closed {
                    return Err(error(anyhow::anyhow!(
                        "RETRY_SESSION_CLOSED: session identity cannot be reopened"
                    )));
                }
                return Ok(response(session, snapshot.revision));
            }
            protocol::Action::OpenSession {
                incarnation,
                session: id,
                owner,
                epoch,
            }
        }
        "ack" => {
            let limit = usize::try_from(safe("limit")?).map_err(|e| invalid(e.into()))?;
            let abandon = match input.get("abandon") {
                None => false,
                Some(Value::Bool(value)) => *value,
                _ => return Err(invalid(anyhow::anyhow!("abandon must be boolean"))),
            };
            protocol::Action::Acknowledge {
                incarnation,
                session: id,
                owner,
                through: safe("through")?,
                limit,
                abandon,
            }
        }
        "close" => protocol::Action::CloseSession {
            incarnation,
            session: id,
            owner,
            limit: usize::try_from(safe("limit")?).map_err(|e| invalid(e.into()))?,
        },
        _ => unreachable!(),
    };
    let committed = app
        .consensus
        .control_retention(protocol::Command {
            expected_revision: snapshot.revision,
            action,
        })
        .await
        .map_err(error)?;
    let session = serde_json::from_value(committed.result["session"].clone())
        .map_err(|e| unavailable(e.into()))?;
    Ok(response(session, committed.revision))
}

pub(super) async fn identity(State(app): State<Arc<App>>) -> Result<Json<Value>, ApiError> {
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let _admission = admission::acquire(&app, admission::Class::User, &Value::Null).await?;
    let state = app.consensus.read_query().await.map_err(unavailable)?;
    let status = protocol::status(&state).map_err(error)?;
    Ok(Json(
        json!({"revision":state.revision,"value":status.map(|s|json!({
            "database":s.database,"incarnation":s.incarnation,
            "currentEpoch":s.current_epoch,"minEpoch":s.min_epoch,
            "epochMs":s.rotation.map(|rotation| rotation.epoch_ms),
        }))}),
    ))
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
    forwarding::commit_retention(app, input).await
}

pub(super) fn validate(input: &Value) -> Result<(), ApiError> {
    if input == &json!({"operation":"status"}) {
        return Ok(());
    }
    serde_json::from_value::<protocol::Command>(input.clone()).map_err(|e| invalid(e.into()))?;
    Ok(())
}

pub(super) async fn commit(app: Arc<App>, input: Value) -> Result<Value, ApiError> {
    validate(&input)?;
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let _input = app
        .admission
        .retain(admission::Class::Control, admission::input_bytes(&input))?;
    let _writer = app.writer.lock().await;
    let _admission = admission::acquire_retained(&app, admission::Class::Control).await?;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    if input["operation"] == "status" {
        return Ok(
            json!({"revision":state.revision,"value":protocol::status(&state).map_err(error)?}),
        );
    }
    let command = serde_json::from_value(input).map_err(|e| invalid(e.into()))?;
    let committed = app
        .consensus
        .control_retention(command)
        .await
        .map_err(error)?;
    Ok(
        json!({"revision":committed.revision,"value":committed.result,"duplicate":committed.duplicate}),
    )
}

/// Receipts inspected by each collection command the driver commits.
const COLLECT_LIMIT: usize = 1024;

/// Drive retention on the leader: collect whatever fell below the floor, and
/// advance a rotating epoch once this leader has watched it for `epoch_ms`
/// of its monotonic time. Losing leadership forgets that time, so a new
/// leader waits a whole epoch again: failover and restarts only lengthen a
/// retry window, and no wall-clock jump can shorten one.
pub(super) async fn run(weak: Weak<App>) {
    let mut watched: Option<(String, u64, Instant)> = None;
    loop {
        let Some(app) = weak.upgrade() else {
            return;
        };
        let wait = match step(&app, &mut watched).await {
            Ok(wait) => wait,
            Err(error) => {
                tracing::debug!(message = %error.message, "retention rotation will retry");
                Duration::from_secs(1)
            }
        };
        drop(app);
        tokio::time::sleep(wait).await;
    }
}

async fn step(app: &App, watched: &mut Option<(String, u64, Instant)>) -> Result<Duration, ApiError> {
    const IDLE: Duration = Duration::from_secs(1);
    if app.consensus.metrics().state != openraft::ServerState::Leader {
        *watched = None;
        return Ok(IDLE);
    }
    let state = app.consensus.local_snapshot().await;
    let Some(status) = protocol::status(&state).map_err(error)? else {
        *watched = None;
        return Ok(IDLE);
    };
    if !status.gc_complete {
        control(app, protocol::Action::Collect {
            incarnation: status.incarnation,
            limit: COLLECT_LIMIT,
        })
        .await?;
        return Ok(Duration::ZERO);
    }
    let Some(rotation) = status.rotation else {
        *watched = None;
        return Ok(IDLE);
    };
    let epoch_ms = Duration::from_millis(rotation.epoch_ms);
    match watched {
        Some((incarnation, epoch, since))
            if *incarnation == status.incarnation && *epoch == status.current_epoch =>
        {
            let waited = since.elapsed();
            if waited < epoch_ms {
                return Ok((epoch_ms - waited).min(IDLE));
            }
            let current_epoch = status.current_epoch + 1;
            control(app, protocol::Action::Advance {
                incarnation: status.incarnation,
                current_epoch,
                min_epoch: rotation.min_epoch(current_epoch).max(status.min_epoch),
            })
            .await?;
            *watched = None;
            Ok(Duration::ZERO)
        }
        _ => {
            *watched = Some((status.incarnation, status.current_epoch, Instant::now()));
            Ok(epoch_ms.min(IDLE))
        }
    }
}

/// Commit one retention transition as the operator would, at the current revision.
async fn control(app: &App, action: protocol::Action) -> Result<(), ApiError> {
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let _writer = app.writer.lock().await;
    let _admission = admission::acquire(app, admission::Class::Control, &Value::Null).await?;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    app.consensus
        .control_retention(protocol::Command {
            expected_revision: state.revision,
            action,
        })
        .await
        .map_err(error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_leader_rotates_epochs_and_collects_what_they_retire() {
        let (_directory, app) = super::super::tests::application(
            r#"
            const records={kind:'collection',name:'records'};
            var __flowerBundle={default:{definitions:{
              write:{kind:'mutationMethod',name:'write',compute:(ctx,value)=>{ctx.set(records,'value',value);return value;}}
            },http:{write:{name:'write',kind:'mutation'}}}};
        "#
            .into(),
        )
        .await;
        // The fixture starts only the writer; production apps start this too.
        tokio::spawn(run(Arc::downgrade(&app)));
        let write = |request_id: String, value: u64| {
            let app = app.clone();
            async move {
                super::super::commit_method(app, json!({"name":"write","args":value,"requestId":request_id}), false)
                    .await
                    .unwrap()
            }
        };
        let _ = write("before retention".into(), 1).await;
        let state = app.consensus.read().await.unwrap();
        let initialize = serde_json::to_value(protocol::initialize(state.revision, None).unwrap()).unwrap();
        commit(app.clone(), initialize).await.unwrap();
        let status = protocol::status(&app.consensus.read().await.unwrap()).unwrap().unwrap();
        let settled = |check: fn(&Snapshot, &protocol::State) -> bool| {
            let app = app.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let state = app.consensus.read().await.unwrap();
                        let status = protocol::status(&state).unwrap().unwrap();
                        if check(&state, &status) {
                            return (state, status);
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap()
            }
        };
        let revision = app.consensus.read().await.unwrap().revision;
        commit(
            app.clone(),
            json!({"expected_revision":revision,"action":{"operation":"rotate","incarnation":status.incarnation,"epoch_ms":1000,"keep_epochs":2}}),
        )
        .await
        .unwrap();
        let first = protocol::scope_request_id(&status, "first");
        let _ = write(first.clone(), 2).await;
        let _ = write("caller key".into(), 3).await;
        let started = Instant::now();
        // Epoch 1 keeps epoch 0 admissible, and a caller's key deduplicates
        // across the boundary; epoch 2 retires and collects all of them.
        let (state, status) = settled(|_, status| status.current_epoch >= 1).await;
        assert_eq!(status.min_epoch, 0);
        for id in ["before retention", "caller key", first.as_str()] {
            assert!(state.requests.contains_key(id), "{id}");
        }
        assert_eq!(write("caller key".into(), 3).await["duplicate"], true);
        let (state, status) = settled(|state, status| {
            status.current_epoch >= 2 && status.gc_complete && state.requests.is_empty()
        })
        .await;
        assert!(started.elapsed() >= Duration::from_millis(1900));
        assert_eq!(status.min_epoch, status.current_epoch - 1);
        assert!(protocol::validate_request(&state, &first).is_err());
        protocol::validate_request(&state, "caller key").unwrap();
        let Json(identity) = identity(State(app.clone())).await.unwrap();
        assert_eq!(identity["value"]["epochMs"], 1000);
        app.consensus.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn writer_waiters_retain_bytes_without_occupying_worker_slots() {
        let (_directory,app)=super::super::tests::application(r#"
            const records={kind:'collection',name:'records'};
            var __flowerBundle={default:{definitions:{stable:{kind:'derived',name:'stable',compute:ctx=>ctx.get(records,'value')}},http:{}}};
        "#.into()).await;
        let writer = app.writer.lock().await;
        let mut session = Box::pin(session_commit(
            app.clone(),
            json!({
                "operation":"status","session":"0".repeat(32),"incarnation":"history"
            }),
        ));
        let mut control = Box::pin(commit(app.clone(), json!({"operation":"status"})));
        assert!(futures_util::poll!(&mut session).is_pending());
        assert!(futures_util::poll!(&mut control).is_pending());
        let metrics = app.admission.metrics();
        for class in metrics["classes"].as_array().unwrap() {
            assert_eq!(
                class["active"], 0,
                "writer waiters must not prevent the owner from admitting its next callback"
            );
            assert!(class["retainedInputBytes"].as_u64().unwrap() > 0);
        }
        drop(session);
        drop(control);
        for class in app.admission.metrics()["classes"].as_array().unwrap() {
            assert_eq!(class["retainedInputBytes"], 0);
        }
        drop(writer);
        app.consensus.shutdown().await.unwrap();
    }
}
