//! A client can address any member: only this internal hop selects the leader.
//! The receiver never forwards again, and retries retain the original invocation
//! and request ID so an uncertain response cannot apply a mutation twice.
use std::sync::OnceLock;

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderValue, header},
    middleware::{self, Next},
};
use openraft::{BasicNode, RaftMetrics, ServerState};
use tokio::time::Instant as TokioInstant;

use super::*;

const COMPATIBILITY: &str = "x-flower-compatibility";
const TARGET_ID: &str = "x-flower-target-node-id";
const TARGET_ADDRESS: &str = "x-flower-target-address";
const SOURCE_ID: &str = "x-flower-source-node-id";
const SOURCE_ADDRESS: &str = "x-flower-source-address";
const NODE_ID: &str = "x-flower-node-id";
const NODE_ADDRESS: &str = "x-flower-node-address";
const KIND: &str = "x-flower-forward-kind";

pub(super) fn router(app: Arc<App>) -> Router<Arc<App>> {
    Router::new()
        .route("/raft/forward", post(receive))
        // Authentication and identity checks precede JSON body extraction.
        .route_layer(middleware::from_fn_with_state(app, authorize))
}

fn client() -> Result<&'static reqwest::Client, ApiError> {
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let limits = crate::consensus::Limits::from_env().map_err(|error| error.to_string())?;
            crate::transport::client_builder()
                .map_err(|error| error.to_string())?
                .connect_timeout(limits.peer_connect_timeout)
                .pool_idle_timeout(limits.peer_idle_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| unavailable(anyhow::anyhow!(error.clone())))
}

fn text_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn address(metrics: &RaftMetrics<u64, BasicNode>, id: u64) -> Option<&str> {
    metrics
        .membership_config
        .nodes()
        .find_map(|(node_id, node)| (*node_id == id).then_some(node.addr.as_str()))
}

pub(in crate::service) fn verify_identity(
    headers: &HeaderMap,
    metrics: &RaftMetrics<u64, BasicNode>,
) -> Result<(), ApiError> {
    let node_id = |name| text_header(headers, name).and_then(|value| value.parse::<u64>().ok());
    if node_id(TARGET_ID) != Some(metrics.id)
        || text_header(headers, TARGET_ADDRESS) != address(metrics, metrics.id)
        || address(metrics, metrics.id).is_none()
    {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "FORWARD_IDENTITY_MISMATCH",
            "forwarded request reached a different node".into(),
        ));
    }
    if text_header(headers, COMPATIBILITY)
        != Some(crate::consensus::compatibility().contract().as_str())
    {
        return Err(ApiError::new(
            StatusCode::UPGRADE_REQUIRED,
            "FORWARD_INCOMPATIBLE",
            "forwarded request uses a different runtime contract".into(),
        ));
    }
    if !node_id(SOURCE_ID).is_some_and(|id| {
        address(metrics, id)
            .is_some_and(|expected| text_header(headers, SOURCE_ADDRESS) == Some(expected))
    }) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "FORWARD_SOURCE_UNKNOWN",
            "forwarding source is not a member of this Raft group".into(),
        ));
    }
    Ok(())
}

async fn authorize(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    let metrics = app.consensus.metrics();
    let authorized = text_header(request.headers(), header::AUTHORIZATION.as_str())
        .and_then(|value| value.strip_prefix("Bearer "))
        == Some(app.consensus.peer_token());
    let mut response = if !authorized {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "peer bearer token required".into(),
        )
        .into_response()
    } else if let Err(error) = verify_identity(request.headers(), &metrics) {
        error.into_response()
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        NODE_ID,
        metrics.id.to_string().parse().expect("numeric node ID"),
    );
    if let Some(address) =
        address(&metrics, metrics.id).and_then(|value| HeaderValue::from_str(value).ok())
    {
        response.headers_mut().insert(NODE_ADDRESS, address);
    }
    response.headers_mut().insert(
        COMPATIBILITY,
        crate::consensus::compatibility()
            .contract()
            .parse()
            .expect("runtime contract header"),
    );
    response
}

async fn receive(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let kind = match text_header(&headers, KIND) {
        Some("deploy") => {
            validate_deployment(&input)?;
            "deploy"
        }
        Some("mutate") => {
            evaluator::validate_invocation(&input, "mutation").map_err(invalid)?;
            "mutate"
        }
        Some("keys") => {
            super::keys::validate(&input)?;
            "keys"
        }
        Some("retention") => {
            super::retention::validate(&input)?;
            "retention"
        }
        Some("session") => {
            super::retention::validate_session(&input)?;
            "session"
        }
        Some("transactions") => {
            super::transactions::validate_admin(&input)?;
            "transactions"
        }
        Some("deployments") => {
            super::staged_deployment::validate_admin(&input)?;
            "deployments"
        }
        _ => return Err(invalid(anyhow::anyhow!("unknown forwarded operation"))),
    };
    let metrics = app.consensus.metrics();
    if metrics.current_leader != Some(metrics.id) || metrics.state != ServerState::Leader {
        return Err(unavailable(anyhow::anyhow!(
            "forwarded request reached a former leader"
        )));
    }
    // Never call commit(): stale routes must fail, not start a forwarding loop.
    // The normal writer still rechecks the method table, quorum, CAS and receipt.
    execute(app, input, kind).await
}

pub(super) async fn commit(
    app: Arc<App>,
    input: Value,
    deployment: bool,
) -> Result<Response, ApiError> {
    commit_kind(app, input, if deployment { "deploy" } else { "mutate" }).await
}

pub(super) async fn commit_keys(app: Arc<App>, input: Value) -> Result<Response, ApiError> {
    commit_kind(app, input, "keys").await
}

pub(super) async fn commit_retention(app: Arc<App>, input: Value) -> Result<Response, ApiError> {
    commit_kind(app, input, "retention").await
}
pub(super) async fn commit_session(app: Arc<App>, input: Value) -> Result<Response, ApiError> {
    commit_kind(app, input, "session").await
}

pub(super) async fn commit_transactions(app: Arc<App>, input: Value) -> Result<Response, ApiError> {
    commit_kind(app, input, "transactions").await
}

pub(super) async fn commit_deployments(app: Arc<App>, input: Value) -> Result<Response, ApiError> {
    commit_kind(app, input, "deployments").await
}

async fn execute(app: Arc<App>, input: Value, kind: &str) -> Result<Json<Value>, ApiError> {
    if kind == "keys" {
        super::keys::commit(app, input).await.map(Json)
    } else if kind == "retention" {
        super::retention::commit(app, input).await.map(Json)
    } else if kind == "session" {
        super::retention::session_commit(app, input).await.map(Json)
    } else if kind == "transactions" {
        super::transactions::administer(&app, input).await.map(Json)
    } else if kind == "deployments" {
        super::staged_deployment::administer(&app, input)
            .await
            .map(Json)
    } else {
        commit_method(app, input, kind == "deploy").await
    }
}

#[tracing::instrument(target = "flower::otel", name = "flower.forward", skip_all, fields(operation = kind))]
async fn commit_kind(app: Arc<App>, input: Value, kind: &str) -> Result<Response, ApiError> {
    // Named partitions have a separate ownership gateway, including epoch fences.
    if app.consensus.partition_binding().is_some() {
        return execute(app, input, kind)
            .await
            .map(IntoResponse::into_response);
    }
    let limits = app.consensus.limits();
    let budget = limits
        .read_timeout
        .checked_add(limits.commit_timeout)
        .and_then(|value| value.checked_add(evaluator::config::settings().ok()?.evaluation_timeout))
        .and_then(|value| TokioInstant::now().checked_add(value))
        .ok_or_else(|| {
            unavailable(anyhow::anyhow!(
                "forwarding deadline exceeds platform limits"
            ))
        })?;
    let mut changes = app.consensus.subscribe();
    let mut encoded: Option<axum::body::Bytes> = None;
    let mut last = "no elected leader is available".to_string();
    loop {
        let metrics = changes.borrow_and_update().clone();
        let route = (metrics.current_term, metrics.current_leader);
        let attempt = async {
            if metrics.current_leader == Some(metrics.id) && metrics.state == ServerState::Leader {
                return execute(app.clone(), input.clone(), kind)
                    .await
                    .map(IntoResponse::into_response);
            }
            let leader = metrics
                .current_leader
                .ok_or_else(|| unavailable(anyhow::anyhow!("no elected leader is available")))?;
            let target = address(&metrics, leader).ok_or_else(|| {
                unavailable(anyhow::anyhow!("elected leader is absent from membership"))
            })?;
            let source = address(&metrics, metrics.id).ok_or_else(|| {
                unavailable(anyhow::anyhow!("this node is absent from membership"))
            })?;
            let bytes = encoded.get_or_insert_with(|| {
                axum::body::Bytes::from(
                    serde_json::to_vec(&input).expect("JSON invocation serializes"),
                )
            });
            let request = client()?
                .post(crate::transport::peer_url(target, "/raft/forward"))
                .bearer_auth(app.consensus.peer_token())
                .header(header::CONTENT_TYPE, "application/json")
                .header(COMPATIBILITY, crate::consensus::compatibility().contract())
                .header(TARGET_ID, leader.to_string())
                .header(TARGET_ADDRESS, target)
                .header(SOURCE_ID, metrics.id.to_string())
                .header(SOURCE_ADDRESS, source)
                .header(KIND, kind)
                .body(bytes.clone());
            let mut response = crate::telemetry::inject(request)
                .send()
                .await
                .map_err(|error| unavailable(error.into()))?;
            if text_header(response.headers(), NODE_ID).and_then(|value| value.parse::<u64>().ok())
                != Some(leader)
                || text_header(response.headers(), NODE_ADDRESS) != Some(target)
                || text_header(response.headers(), COMPATIBILITY)
                    != Some(crate::consensus::compatibility().contract().as_str())
            {
                return Err(ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "FORWARD_IDENTITY_MISMATCH",
                    "leader response identity or runtime contract mismatch".into(),
                ));
            }
            let status = response.status();
            let mut output = Vec::new();
            let max_bytes = limits
                .rpc_max_bytes
                .max(tuning::settings().map_err(unavailable)?.http_max_body_bytes);
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| unavailable(error.into()))?
            {
                if output.len().saturating_add(chunk.len()) > max_bytes {
                    return Err(ApiError::new(
                        StatusCode::BAD_GATEWAY,
                        "FORWARD_RESPONSE_TOO_LARGE",
                        "leader response exceeds configured transport budget".into(),
                    ));
                }
                output.extend_from_slice(&chunk);
            }
            // Remote application errors and receipts retain their exact JSON and
            // status. Only temporary unavailability triggers another same-ID hop.
            if status == StatusCode::SERVICE_UNAVAILABLE && retryable_unavailable(&output) {
                return Err(unavailable(anyhow::anyhow!(
                    "leader is temporarily unavailable"
                )));
            }
            Ok(Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(output))
                .expect("validated HTTP status"))
        };
        match tokio::time::timeout_at(budget, attempt).await {
            Ok(Ok(response)) => return Ok(response),
            Ok(Err(error))
                if error.status == StatusCode::SERVICE_UNAVAILABLE
                    && matches!(error.code, "UNAVAILABLE" | "PARTITION_MOVING") =>
            {
                last = error.message
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => break,
        }
        // A changed leader wakes immediately; temporary unavailability waits the
        // existing peer-connect budget instead of spinning on metrics.
        let retry_at = TokioInstant::now()
            .checked_add(limits.peer_connect_timeout)
            .unwrap_or(budget)
            .min(budget);
        loop {
            match tokio::time::timeout_at(retry_at, changes.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(unavailable(anyhow::anyhow!("Raft member stopped"))),
                Err(_) => break,
            }
            let next = changes.borrow_and_update();
            if (next.current_term, next.current_leader) != route {
                break;
            }
        }
        if TokioInstant::now() >= budget {
            break;
        }
    }
    Err(unavailable(anyhow::anyhow!(
        "leader forwarding timed out ({last}); outcome may be unknown, retry the same request ID"
    )))
}

pub(in crate::service) fn retryable_unavailable(bytes: &[u8]) -> bool {
    let error: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
    error["error"]["code"]
        .as_str()
        .is_none_or(|code| matches!(code, "UNAVAILABLE" | "PARTITION_MOVING"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_route_failures_are_retried() {
        for code in ["ADMISSION_OVERLOADED", "TRANSACTION_PREPARED", "KEY_LOCKED"] {
            assert!(!retryable_unavailable(
                &serde_json::to_vec(&json!({"error":{"code":code}})).unwrap()
            ));
        }
        for code in ["UNAVAILABLE", "PARTITION_MOVING"] {
            assert!(retryable_unavailable(
                &serde_json::to_vec(&json!({"error":{"code":code}})).unwrap()
            ));
        }
    }

    #[tokio::test]
    async fn local_admission_overload_returns_without_a_leader_retry() {
        let (_directory,app)=super::super::tests::application(
            r#"const records={kind:'collection',name:'records'};
            var __flowerBundle={default:{definitions:{stable:{kind:'derived',name:'stable',compute:ctx=>ctx.get(records,'value')}},http:{}}};"#.into()
        ).await;
        let mut owned = Arc::try_unwrap(app).ok().expect("one fixture owner");
        owned.admission = admission::Pool::new([1, 1], [1, 1], [1, 1], 1);
        let app = Arc::new(owned);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            commit_kind(
                app.clone(),
                json!({"name":"write","args":{},"requestId":"full"}),
                "mutate",
            ),
        )
        .await
        .expect("overload must not enter forwarding retry loop");
        let error = result.expect_err("configured input budget rejects request");
        assert_eq!(error.code, "ADMISSION_OVERLOADED");
        app.consensus.shutdown().await.unwrap();
    }
}
