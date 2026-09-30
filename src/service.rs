mod admission;
mod clock;
mod forwarding;
mod keys;
mod partitions;
mod process;
mod query_cache;
mod staged_deployment;
#[cfg(test)]
mod tests;
mod transactions;
mod tuning;
mod watch;
mod writer;

use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{
    consensus::{Commit, Consensus, Snapshot},
    evaluator::{self, HttpMethod, MaintenanceMethod, MethodKind, QueryConsistency},
    telemetry::RequestJson,
};

struct App {
    consensus: Consensus,
    writer: Mutex<()>,
    writer_queue: mpsc::Sender<writer::Pending>,
    evaluations: Arc<Semaphore>,
    query_evaluations: Arc<Semaphore>,
    admission: Arc<admission::Pool>,
    query_cache: query_cache::QueryCache,
    authorizations: authorization::memo::Memo,
    watch_hubs: watch::hubs::Registry,
    admin_token: String,
    clock: clock::Clock,
    cross_group: transactions::Runtime,
    partition_gate: Option<Arc<partitions::router::Gate>>,
}

mod authorization;
mod retention;

/// Reject invalid capacity settings before opening a node.
pub fn validate_configuration() -> anyhow::Result<()> {
    crate::crypto::managed::validate_configuration()?;
    tuning::settings()?;
    partitions::Config::load()?;
    partitions::router::validate_configuration()?;
    transactions::Runtime::validate_configuration()?;
    tuning::watch_timeout(
        crate::consensus::Limits::from_env()?.read_timeout,
        evaluator::config::settings()?.evaluation_timeout,
    )?;
    Ok(())
}

/// Named methods are the entire public data API. Control-plane routes require
/// the operator token; authenticated peer transport may use a separate token.
pub fn router(consensus: Consensus, admin_token: String) -> Router {
    process::started();
    let raft_routes = consensus.router();
    let query_evaluations = Arc::new(Semaphore::new(
        tuning::settings()
            .expect("validated query configuration")
            .query_workers,
    ));
    let admission = admission::Pool::configured().expect("validated admission configuration");
    let partition_routes = partitions::router::router(
        consensus.clone(),
        admin_token.clone(),
        query_evaluations.clone(),
        admission.clone(),
    );
    let app = make_app(consensus, admin_token, query_evaluations, admission, None);
    let ingress = admission::ingress::Budget::new(&app);
    Router::new()
        .route(
            "/health",
            get(|| async { Json(json!({"service":"flower","version":env!("CARGO_PKG_VERSION")})) }),
        )
        .route(
            "/v1/mutate",
            post(|state, RequestJson(input)| mutate(state, Json(input))),
        )
        .route(
            "/v1/query",
            post(|state, RequestJson(input)| query_http(state, Json(input))),
        )
        .route("/v1/watch", post(watch::watch))
        .route(
            "/v1/call",
            post(|state, RequestJson(input)| call(state, Json(input))),
        )
        .route("/v1/identity", post(retention::identity))
        .route("/v1/session", post(retention::session_handle))
        .route("/admin/backup", get(backup_status))
        .route("/admin/deploy", post(deploy))
        .route("/admin/deployments", post(deployment_control))
        .route("/admin/keys", post(keys::handle))
        .route("/admin/resources", get(resource_metrics))
        .route("/admin/retention", post(retention::handle))
        .route("/admin/transactions", post(transaction_control))
        .merge(transactions::router())
        .merge(forwarding::router(app.clone()))
        .layer(DefaultBodyLimit::max(
            tuning::settings()
                .expect("validated HTTP body budget")
                .http_max_body_bytes,
        ))
        .with_state(app)
        .merge(raft_routes)
        .merge(partition_routes)
        .layer(axum::middleware::from_fn_with_state(
            ingress,
            admission::ingress::handle,
        ))
        .layer(axum::middleware::from_fn(crate::telemetry::http))
}

fn make_app(
    consensus: Consensus,
    admin_token: String,
    query_evaluations: Arc<Semaphore>,
    admission: Arc<admission::Pool>,
    partition_gate: Option<Arc<partitions::router::Gate>>,
) -> Arc<App> {
    let (writer_queue, receiver) = mpsc::channel(
        tuning::settings()
            .expect("validated writer configuration")
            .queue_capacity,
    );
    let app = Arc::new(App {
        consensus,
        writer: Mutex::new(()),
        writer_queue,
        evaluations: Arc::new(Semaphore::new(1)),
        query_evaluations,
        admission,
        query_cache: query_cache::QueryCache::default(),
        authorizations: authorization::memo::Memo::default(),
        watch_hubs: watch::hubs::Registry::default(),
        admin_token,
        clock: clock::Clock::new(),
        cross_group: transactions::Runtime::new().expect("validated cross-group configuration"),
        partition_gate,
    });
    tokio::spawn(writer::run(Arc::downgrade(&app), receiver));
    tokio::spawn(transactions::run(Arc::downgrade(&app)));
    tokio::spawn(retention::run(Arc::downgrade(&app)));
    app
}

#[derive(Clone, Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    /// The method's own failure, delivered to callers separately from the transport code.
    failure: Option<Arc<Value>>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: String) -> Self {
        Self {
            status,
            code,
            message,
            failure: None,
        }
    }

    fn with_failure(mut self, failure: Value) -> Self {
        self.failure = Some(Arc::new(failure));
        self
    }

    fn body(&self) -> Value {
        let mut error = json!({"code": self.code, "message": self.message});
        if let Some(failure) = &self.failure {
            error["failure"] = (**failure).clone();
        }
        json!({ "error": error })
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}

fn unavailable(error: anyhow::Error) -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "UNAVAILABLE",
        error.to_string(),
    )
}

fn invalid(error: anyhow::Error) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, "INPUT_INVALID", error.to_string())
}

/// A method's structured failure, when an evaluation error came from application code or the engine.
fn engine_failure(error: &anyhow::Error) -> Option<Value> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<crate::evaluator::rust_engine::EngineError>())
        .map(|engine| engine.failure())
}

/// Rebuild a peer's structured failure from its HTTP error body.
fn remote_failure(body: &[u8]) -> Option<anyhow::Error> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let failure = value.get("error")?.get("failure")?;
    let mut error = crate::evaluator::rust_engine::EngineError::new(
        failure.get("code")?.as_str()?,
        failure.get("message")?.as_str()?,
    );
    error.details = failure.get("details").cloned();
    Some(anyhow::Error::new(error))
}

fn evaluation_error(error: anyhow::Error) -> ApiError {
    let failure = engine_failure(&error);
    let api = ApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "EVALUATION_FAILED",
        error.to_string(),
    );
    match failure {
        Some(failure) => api.with_failure(failure),
        None => api,
    }
}

fn http_method(
    state: &Snapshot,
    name: &str,
    expected: Option<MethodKind>,
) -> Result<HttpMethod, ApiError> {
    let entry = state
        .data
        .get("httpMethods")
        .and_then(|methods| methods.get(name))
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "METHOD_NOT_FOUND",
                format!("HTTP method {name:?} is not exposed"),
            )
        })?;
    let method = HttpMethod::deserialize(entry).map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INVALID_METHOD_REGISTRY",
            e.to_string(),
        )
    })?;
    if expected.is_some_and(|kind| kind != method.kind) {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "METHOD_KIND_MISMATCH",
            format!("HTTP method {name:?} is a {}", method.kind.as_str()),
        ));
    }
    Ok(method)
}

async fn mutate(
    State(app): State<Arc<App>>,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    evaluator::validate_invocation(&input, "mutation").map_err(invalid)?;
    forwarding::commit(app, input, false).await
}

async fn transaction_control(
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
    transactions::validate_admin(&input)?;
    forwarding::commit_transactions(app, input).await
}

async fn deployment_control(
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
    staged_deployment::validate_admin(&input)?;
    forwarding::commit_deployments(app, input).await
}

async fn deploy(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    let supplied = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok());
    if supplied != Some(format!("Bearer {}", app.admin_token).as_str()) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "operator bearer token required".into(),
        ));
    }
    validate_deployment(&input)?;
    forwarding::commit(app, input, true).await
}

fn validate_deployment(input: &Value) -> Result<(), ApiError> {
    let object = input
        .as_object()
        .ok_or_else(|| invalid(anyhow::anyhow!("deployment must be an object")))?;
    if object
        .keys()
        .any(|key| !["requestId", "bundle", "preparation"].contains(&key.as_str()))
        || !object.contains_key("bundle")
    {
        return Err(invalid(anyhow::anyhow!(
            "deployment requires requestId and bundle, with optional preparation"
        )));
    }
    evaluator::validate_mutation(input).map_err(invalid)
}

async fn commit_method(
    app: Arc<App>,
    input: Value,
    deployment: bool,
) -> Result<Json<Value>, ApiError> {
    if !deployment {
        // This local lookup is only dispatch. The coordinator re-resolves the
        // alias under a fresh quorum fence and writer lock before planning.
        let local = app
            .consensus
            .snapshot_for(None)
            .await
            .map_err(unavailable)?;
        if http_method(&local, input["name"].as_str().unwrap(), None)
            .is_ok_and(|method| method.kind == MethodKind::Transaction)
        {
            return transactions::execute(app, input).await.map(Json);
        }
    }
    writer::submit(&app, input, deployment).await.map(Json)
}

#[cfg(test)]
async fn query(
    State(app): State<Arc<App>>,
    Json(input): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    evaluator::validate_invocation(&input, "query").map_err(invalid)?;
    read_query(&app, input).await.map(QueryResult::response)
}

async fn query_http(
    State(app): State<Arc<App>>,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    evaluator::validate_invocation(&input, "query").map_err(invalid)?;
    if let Some(response) = cached_query_response(&app, &input, Some(MethodKind::Query)).await? {
        return Ok(response);
    }
    read_query(&app, input)
        .await
        .map(|result| result.response().into_response())
}

/// A cache lookup needs bounded input, snapshot and response retention, but no
/// QuickJS worker. Public hits therefore do not wait behind writer preparation.
/// Every miss drops its root and lookup slot before entering normal admission,
/// which captures a new root and rechecks the method, policy and read fence.
async fn cached_query_response(
    app: &App,
    input: &Value,
    expected: Option<MethodKind>,
) -> Result<Option<Response>, ApiError> {
    crate::telemetry::query_stage(
        "cache_probe",
        cached_query_response_inner(app, input, expected),
    )
    .await
}

async fn cached_query_response_inner(
    app: &App,
    input: &Value,
    expected: Option<MethodKind>,
) -> Result<Option<Response>, ApiError> {
    let Ok(_input) = app
        .admission
        // Include decoded input and the cache key's worst-case JSON escaping.
        .retain(
            admission::Class::User,
            admission::input_bytes(input).saturating_mul(7),
        )
    else {
        // Optional lookup scratch must not reject a large request that can
        // still fit the normal admitted evaluator's memory reservation.
        return Ok(None);
    };
    let _lookup = match app.query_evaluations.clone().try_acquire_owned() {
        Ok(permit) => permit,
        // No lookup queue: saturated or slow fresh fences fall back to normal
        // fair partition admission without retaining another snapshot.
        Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(None),
        Err(error) => return Err(unavailable(error.into())),
    };
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let name = input["name"].as_str().unwrap();
    let local = crate::telemetry::query_stage("local_snapshot", app.consensus.snapshot_for(None))
        .await
        .map_err(unavailable)?;
    // Authorization runs application code, and managed-key policy requires the
    // existing admitted path even if the query's ordinary dependencies match.
    if authorization::required(&local) || keys::requires_fresh_policy(&local) {
        return Ok(None);
    }
    let local_method = http_method(&local, name, expected);
    if !local_method
        .as_ref()
        .is_ok_and(|method| method.kind == MethodKind::Query)
    {
        // This probe cannot answer a mutation or an unresolved alias. Normal
        // dispatch performs its own fresh lookup, without a redundant fence.
        return Ok(None);
    }
    let (state, method) = if let Ok(method) = local_method
        && method.kind == MethodKind::Query
        && method.consistency == QueryConsistency::ReplicaLocal
    {
        (local, method)
    } else {
        drop(local);
        let state = crate::telemetry::query_stage("fresh_snapshot", app.consensus.read_query())
            .await
            .map_err(unavailable)?;
        if authorization::required(&state) || keys::requires_fresh_policy(&state) {
            return Ok(None);
        }
        let method = http_method(&state, name, expected)?;
        (state, method)
    };
    transactions::ensure_unlocked(&state)?;
    if method.kind != MethodKind::Query {
        return Ok(None);
    }
    if input.get("expectedRevision").is_some() {
        return Err(invalid(anyhow::anyhow!(
            "expectedRevision applies only to mutation methods"
        )));
    }
    app.clock.sample(&state).map_err(unavailable)?;
    let Ok(_method) = app.admission.retain(
        admission::Class::User,
        method.name.len().saturating_mul(7).saturating_add(128),
    ) else {
        return Ok(None);
    };
    let key = query_cache::key(
        &method.name,
        input.get("args").unwrap_or(&Value::Null),
        &Value::Null,
    );
    let Some(encoded) = app.query_cache.get_encoded(&key, &state.data) else {
        crate::telemetry::query_cache("encoded_miss");
        return Ok(None);
    };
    crate::telemetry::query_cache("encoded_hit");
    let revision = state.revision;
    drop(state);
    let capacity = encoded.len().saturating_add(96);
    // Reserve before allocating. Bytes owns this lease through transport, even
    // if its cache entry is replaced or the handler has already returned.
    let retained = app
        .admission
        .retain(admission::Class::User, capacity.saturating_add(512))?;
    let mut body = String::with_capacity(capacity);
    let extra = (body.capacity() > capacity)
        .then(|| {
            app.admission
                .retain(admission::Class::User, body.capacity() - capacity)
        })
        .transpose()?;
    std::fmt::Write::write_fmt(
        &mut body,
        format_args!("{{\"revision\":{revision},\"value\":{encoded},\"duplicate\":false}}"),
    )
    .expect("writing JSON into a String");
    struct CachedBody {
        encoded: String,
        _retained: admission::Input,
        _extra: Option<admission::Input>,
    }
    impl AsRef<[u8]> for CachedBody {
        fn as_ref(&self) -> &[u8] {
            self.encoded.as_bytes()
        }
    }
    let bytes = axum::body::Bytes::from_owner(CachedBody {
        encoded: body,
        _retained: retained,
        _extra: extra,
    });
    Ok(Some(
        (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
    ))
}

async fn read_query(app: &App, input: Value) -> Result<QueryResult, ApiError> {
    let (state, method, permit) = query_snapshot(app, &input, Some(MethodKind::Query)).await?;
    let now = app.clock.sample(&state).map_err(unavailable)?;
    evaluate_query(app, state, input, method, now, permit).await
}

/// Only the deployed code may opt into a local snapshot. Missing aliases and
/// default-fresh definitions are resolved again after the quorum fence. An
/// explicitly local definition uses this replica's registry too; its removal
/// or policy change takes effect here when that deployment is applied.
async fn query_snapshot(
    app: &App,
    input: &Value,
    expected: Option<MethodKind>,
) -> Result<(Snapshot, HttpMethod, admission::Permit), ApiError> {
    let permit = crate::telemetry::query_stage(
        "admission",
        admission::acquire(app, admission::Class::User, input),
    )
    .await?;
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let name = input["name"].as_str().unwrap();
    let local = crate::telemetry::query_stage("local_snapshot", app.consensus.snapshot_for(None))
        .await
        .map_err(unavailable)?;
    if let Ok(method) = http_method(&local, name, expected)
        && method.kind == MethodKind::Query
        && method.consistency == QueryConsistency::ReplicaLocal
        && !keys::requires_fresh_policy(&local)
        && !authorization::required(&local)
    {
        transactions::ensure_unlocked(&local)?;
        return Ok((local, method, permit));
    }
    drop(local);
    let state = crate::telemetry::query_stage("fresh_snapshot", app.consensus.read_query())
        .await
        .map_err(unavailable)?;
    transactions::ensure_unlocked(&state)?;
    let method = http_method(&state, name, expected)?;
    Ok((state, method, permit))
}

/// Generic RPC dispatch: the committed code-owned table chooses read/write mode.
async fn call(State(app): State<Arc<App>>, Json(input): Json<Value>) -> Result<Response, ApiError> {
    evaluator::validate_invocation(&input, "call").map_err(invalid)?;
    if public_mutation_hint(&app, &input).await?
        && let Some(response) = mutation_hint_response(
            &app,
            forwarding::commit(app.clone(), input.clone(), false).await,
        )
        .await?
    {
        return Ok(response);
    }
    if let Some(response) = cached_query_response(&app, &input, None).await? {
        return Ok(response);
    }
    let (state, method, permit) = query_snapshot(&app, &input, None).await?;
    match method.kind {
        MethodKind::Query => {
            let now = app.clock.sample(&state).map_err(unavailable)?;
            evaluate_query(&app, state, input, method, now, permit)
                .await
                .map(|result| result.response().into_response())
        }
        MethodKind::Mutation | MethodKind::Transaction => {
            drop(state);
            drop(permit);
            evaluator::validate_invocation(&input, "mutation").map_err(invalid)?;
            // Re-resolve under the writer lock; deployment may have changed the
            // alias since the first snapshot. Never execute a mismatched kind.
            forwarding::commit(app, input, false).await
        }
    }
}

/// Mutation dispatch needs no separate preparation lease or quorum fence: the
/// ordered writer takes both before resolving the alias and current policy.
/// This snapshot is only a hint, never execution authority. Calls without an ID
/// still use generic dispatch because a stale mutation alias may now be a query.
async fn public_mutation_hint(app: &App, input: &Value) -> Result<bool, ApiError> {
    if input.get("requestId").is_none() {
        return Ok(false);
    }
    if let Some(gate) = &app.partition_gate {
        gate.check().await?;
    }
    let local = app
        .consensus
        .snapshot_for(None)
        .await
        .map_err(unavailable)?;
    Ok(!authorization::required(&local)
        && http_method(&local, input["name"].as_str().unwrap(), None)
            .is_ok_and(|method| method.kind == MethodKind::Mutation))
}

/// A kind rejection happens before authorization, receipt replay, or execution.
/// Re-dispatch it against a current snapshot if a deployment changed the alias
/// after our hint. Forwarded errors keep their exact body/status unless they are
/// this specific rejection; successful responses never need inspection.
async fn mutation_hint_response(
    app: &App,
    result: Result<Response, ApiError>,
) -> Result<Option<Response>, ApiError> {
    let response = match result {
        Err(error) if error.code == "METHOD_KIND_MISMATCH" => return Ok(None),
        Err(error) => return Err(error),
        Ok(response) if response.status() != StatusCode::UNPROCESSABLE_ENTITY => {
            return Ok(Some(response));
        }
        Ok(response) => response,
    };
    let (parts, body) = response.into_parts();
    // The forwarding transport already enforces this same response budget.
    let max_bytes = app
        .consensus
        .limits()
        .rpc_max_bytes
        .max(tuning::settings().map_err(unavailable)?.http_max_body_bytes);
    let bytes = axum::body::to_bytes(body, max_bytes)
        .await
        .map_err(|error| unavailable(error.into()))?;
    if serde_json::from_slice::<Value>(&bytes)
        .is_ok_and(|value| value["error"]["code"] == "METHOD_KIND_MISMATCH")
    {
        return Ok(None);
    }
    Ok(Some(Response::from_parts(
        parts,
        axum::body::Body::from(bytes),
    )))
}

struct QueryResult {
    revision: u64,
    value: Value,
    validity: Validity,
    /// What the value depends on, when only writes can change it.
    certificate: Option<Arc<evaluator::DependencyCertificate>>,
}

/// How long a result stays exact at its revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Validity {
    /// Only a new revision can change it.
    Stable,
    /// Time can change it, first at this server millisecond.
    Until(u64),
    /// It read the clock through ctx.now(), which says nothing about when.
    Polled,
}

impl Validity {
    fn of(evaluation: &evaluator::Evaluation) -> Self {
        match (evaluation.query_clock_polled, evaluation.query_changes_at) {
            (true, _) => Self::Polled,
            (false, Some(time)) => Self::Until(time),
            (false, None) => Self::Stable,
        }
    }

    /// Whichever changes first.
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Polled, _) | (_, Self::Polled) => Self::Polled,
            (Self::Until(a), Self::Until(b)) => Self::Until(a.min(b)),
            (Self::Until(time), Self::Stable) | (Self::Stable, Self::Until(time)) => {
                Self::Until(time)
            }
            (Self::Stable, Self::Stable) => Self::Stable,
        }
    }
}

/// An evaluated query, its admission, and what evaluating it took (see
/// `Work`; nothing for a cached result).
enum QueryEvaluation {
    Ready(QueryResult, admission::Permit, Duration),
    Retry,
}

/// The CPU time this thread spends from `start` on, or the time that
/// passes where threads have no CPU clock.
pub(super) struct Work {
    cpu: Option<Duration>,
    wall: Instant,
}

impl Work {
    pub fn start() -> Self {
        Self {
            cpu: thread_cpu(),
            wall: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        match (self.cpu, thread_cpu()) {
            (Some(start), Some(now)) => now.saturating_sub(start),
            _ => self.wall.elapsed(),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn thread_cpu() -> Option<Duration> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes one timespec through a valid pointer.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
    (status == 0).then(|| {
        Duration::new(
            u64::try_from(time.tv_sec).unwrap_or(0),
            u32::try_from(time.tv_nsec).unwrap_or(0),
        )
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn thread_cpu() -> Option<Duration> {
    None
}

struct QueryAdmission {
    permit: admission::Permit,
    coalesce: bool,
}

impl QueryResult {
    fn response(self) -> Json<Value> {
        Json(json!({"revision":self.revision,"value":self.value,"duplicate":false}))
    }
}

async fn evaluate_query(
    app: &App,
    mut state: Snapshot,
    input: Value,
    mut method: HttpMethod,
    mut now: u64,
    mut permit: admission::Permit,
) -> Result<QueryResult, ApiError> {
    if input.get("expectedRevision").is_some() {
        return Err(invalid(anyhow::anyhow!(
            "expectedRevision applies only to mutation methods"
        )));
    }
    let mut coalesce = true;
    loop {
        let principal = crate::telemetry::query_stage(
            "authorization",
            authorization::authorize_admitted(app, &state, &input, &permit),
        )
        .await?;
        match evaluate_query_authorized(
            app,
            state,
            &input,
            method,
            now,
            QueryAdmission { permit, coalesce },
            principal,
        )
        .await?
        {
            QueryEvaluation::Ready(result, _permit, _) => return Ok(result),
            QueryEvaluation::Retry => {
                // Do not chase an indefinitely moving snapshot under writes.
                // After one wait, a cache miss executes the newly admitted read.
                coalesce = false;
                (state, method, permit) =
                    query_snapshot(app, &input, Some(MethodKind::Query)).await?;
                now = app.clock.sample(&state).map_err(unavailable)?;
            }
        }
    }
}

// Only callers that already ran authorization against this exact snapshot may
// reuse this entry point. Shared watch producers never retain credentials.
async fn evaluate_query_authorized(
    app: &App,
    state: Snapshot,
    input: &Value,
    method: HttpMethod,
    now: u64,
    admission: QueryAdmission,
    principal: Value,
) -> Result<QueryEvaluation, ApiError> {
    let QueryAdmission { permit, coalesce } = admission;
    let key = query_cache::key(
        &method.name,
        input.get("args").unwrap_or(&Value::Null),
        &principal,
    );
    if let Some((value, certificate)) = app.query_cache.get_certified(&key, &state.data) {
        crate::telemetry::query_cache("hit");
        return Ok(QueryEvaluation::Ready(
            QueryResult {
                revision: state.revision,
                value,
                validity: Validity::Stable,
                certificate: Some(certificate),
            },
            permit,
            Duration::ZERO,
        ));
    }
    // Identical concurrent requests share a first evaluation, then recheck the
    // cache. Only proven clock-independent results can be reused. Once a first
    // evaluation finishes without a reusable result, waiters run independently.
    let flight = coalesce
        .then(|| app.query_cache.flight(state.revision, &key))
        .flatten();
    let mut flight = match &flight {
        Some(flight) => match flight.try_lock() {
            Ok(guard) => Some(guard),
            Err(_) => {
                // A coalescing waiter is not an active evaluator. Holding a
                // worker slot here lets one hot query monopolize the pool.
                let _input = app
                    .admission
                    .retain(admission::Class::User, admission::input_bytes(input))?;
                drop(state);
                drop(principal);
                drop(permit);
                crate::telemetry::query_cache("coalesced");
                crate::telemetry::query_stage("coalesce_wait", async {
                    drop(flight.lock().await);
                    Ok::<_, std::convert::Infallible>(())
                })
                .await
                .expect("infallible query wait");
                // The caller owns credentials and must reacquire its chosen
                // read fence, method registry and authorization after waiting.
                return Ok(QueryEvaluation::Retry);
            }
        },
        None => None,
    };
    if let Some((value, certificate)) = app.query_cache.get_certified(&key, &state.data) {
        crate::telemetry::query_cache("hit_after_coalesce");
        return Ok(QueryEvaluation::Ready(
            QueryResult {
                revision: state.revision,
                value,
                validity: Validity::Stable,
                certificate: Some(certificate),
            },
            permit,
            Duration::ZERO,
        ));
    }
    if flight.as_ref().is_some_and(|pending| !**pending) {
        flight = None;
    }
    // Only a cache miss needs an owned invocation. Hits and coalescing waiters
    // can release the original request without cloning its argument tree.
    let invocation =
        json!({"name":method.name,"args":input.get("args").cloned().unwrap_or(Value::Null)});
    let held_permit = permit.clone();
    crate::telemetry::query_cache("evaluated");
    let span = tracing::info_span!(target: "flower::otel", "flower.query.evaluate");
    let worker_span = span.clone();
    let queued = crate::telemetry::enabled().then(Instant::now);
    let result = crate::telemetry::query_stage("blocking_evaluation", async {
        tokio::task::spawn_blocking(move || {
            let _entered = worker_span.enter();
            if let Some(queued) = queued {
                crate::telemetry::query_duration(
                    "blocking_queue",
                    queued.elapsed().as_secs_f64(),
                    "ok",
                );
            }
            let _permit = held_permit;
            let work = Work::start();
            let result = evaluator::invoke_as(state.data, invocation, "query", now, principal);
            result.map(|result| (result, work.elapsed()))
        })
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "WORKER_FAILED",
                e.to_string(),
            )
        })?
        .map_err(evaluation_error)
    })
    .await;
    if let Some(pending) = flight.as_mut() {
        **pending = false;
    }
    let (result, work) = result?;
    let validity = Validity::of(&result);
    // A result that read the clock keeps its certificate for watches, which
    // also wake when it declared time changes it; only others are reusable.
    let certificate = result.query_certificate.map(Arc::new);
    if let Some(certificate) = certificate.as_ref().filter(|_| result.query_cacheable) {
        app.query_cache.insert(
            state.revision,
            key,
            result.value.clone(),
            certificate.clone(),
        );
    }
    Ok(QueryEvaluation::Ready(
        QueryResult {
            revision: state.revision,
            value: result.value,
            validity,
            certificate,
        },
        permit,
        work,
    ))
}

/// The writer actor runs optional application maintenance between pipeline
/// windows. Scheduling/expiration policy remains in the deployed TypeScript.
/// When maintenance should run again, as its last evaluation reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NextRun {
    /// More work is due already.
    Now,
    /// Nothing is due before this server millisecond.
    At(u64),
    /// Nothing is scheduled; only a write can change that.
    Idle,
    /// The handler didn't say, as hand-written handlers may not: poll.
    Unknown,
}

impl NextRun {
    /// `{"$flower":{"continue":bool,"next":ms|null}}` from the SDK's handler.
    fn of(value: &Value, now: u64) -> Self {
        let hint = value.get("$flower");
        if hint.and_then(|hint| hint.get("continue")) == Some(&Value::Bool(true)) {
            return Self::Now;
        }
        match hint.and_then(|hint| hint.get("next")) {
            Some(Value::Null) => Self::Idle,
            Some(next) => match next.as_f64().filter(|next| next.is_finite()) {
                Some(next) if next <= now as f64 => Self::Now,
                Some(next) => Self::At(next.ceil().min(9_007_199_254_740_991.0) as u64),
                None => Self::Unknown,
            },
            None => Self::Unknown,
        }
    }
}

/// What a maintenance run found, whether it committed a group, and which
/// writes can make its hint stale.
#[derive(Debug)]
pub(super) struct Maintained {
    pub next: NextRun,
    pub committed: bool,
    pub interest: writer::Interest,
}

async fn maintain(app: &Arc<App>) -> anyhow::Result<Maintained> {
    let _guard = app.writer.lock().await;
    let admission = admission::acquire(app, admission::Class::Control, &Value::Null)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let started = Instant::now();
    let mut state = app.consensus.read_for(None).await?;
    let mut commands = Vec::new();
    let mut bytes = 0;
    let mut failure = None;
    let mut next = NextRun::Unknown;
    // What the evaluation that gave `next` read, when that is known.
    let mut reads = None;
    // Each callback has its own rollback boundary and logical revision. Flush
    // their successful patches together, keeping earlier work if a later
    // callback fails. Bound the burst so customer methods make progress too.
    loop {
        match prepare_maintenance(app, &state, &admission).await {
            Ok((Some((command, again)), hint, certificate)) => {
                let size = serde_json::to_vec(&command)?.len();
                if !app
                    .consensus
                    .commit_group_fits(bytes + size, commands.len() + 1)
                {
                    if commands.is_empty() {
                        failure = Some(anyhow::anyhow!(
                            "maintenance command exceeds FLOWER_TRANSACTION_MAX_BYTES ({})",
                            app.consensus.limits().transaction_max_bytes,
                        ));
                    }
                    // Its work is left undone, so still due.
                    next = NextRun::Now;
                    reads = None;
                    break;
                }
                next = hint;
                reads = certificate;
                bytes += size;
                writer::stage(&mut state, &command)
                    .map_err(|error| anyhow::anyhow!(error.message))?;
                commands.push(command);
                if !again || started.elapsed() >= tuning::settings()?.maintenance_burst {
                    break;
                }
            }
            Ok((None, hint, certificate)) => {
                next = hint;
                reads = certificate;
                break;
            }
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    let committed = !commands.is_empty();
    writer::commit_group(app, commands)
        .await
        .map_err(|error| anyhow::anyhow!("{}: {}", error.code, error.message))?;
    if let Some(error) = failure {
        return Err(error);
    }
    // The hint reflects every write up to the staged revision, its own
    // commit's included. A timer or idleness it declares then holds until a
    // write to what it read, unless a stored cell reads the clock now (the
    // run may have materialized one): time alone could change that cell in a
    // later run's preview. More work, or no hint, is run or polled anyway.
    let interest = match (next, &reads) {
        (NextRun::Idle | NextRun::At(_), Some(reads)) if state.data.reactive().cacheable() => {
            writer::Interest::reads(state.revision, reads)
        }
        _ => writer::Interest::any(state.revision),
    };
    tracing::debug!(
        revision = state.revision,
        committed,
        ?next,
        observed = ?interest.observed(),
        "maintenance ran"
    );
    Ok(Maintained {
        next,
        committed,
        interest,
    })
}

#[cfg(test)]
async fn maintain_one(app: &Arc<App>) -> anyhow::Result<bool> {
    let admission = admission::acquire(app, admission::Class::Control, &Value::Null)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let state = app.consensus.read_for(None).await?;
    let (Some((command, again)), _, _) = prepare_maintenance(app, &state, &admission).await? else {
        return Ok(false);
    };
    app.consensus.commit(command).await?;
    Ok(again)
}

async fn prepare_maintenance(
    app: &Arc<App>,
    state: &Snapshot,
    admission: &admission::Permit,
) -> anyhow::Result<(
    Option<(Commit, bool)>,
    NextRun,
    Option<evaluator::MutationCertificate>,
)> {
    if transactions::ensure_unlocked(state).is_err() {
        // The lock's release is a commit, which schedules maintenance again.
        return Ok((None, NextRun::Idle, None));
    }
    let Some(entry) = state
        .data
        .get("maintenanceMethod")
        .filter(|value| !value.is_null())
    else {
        // A deployment registers a handler; its registry key is a write
        // that concerns every reader.
        return Ok((None, NextRun::Idle, None));
    };
    let method: MaintenanceMethod = serde_json::from_value(entry.clone())?;
    anyhow::ensure!(
        method.kind == MethodKind::Mutation,
        "maintenance must be a mutation"
    );
    anyhow::ensure!(
        state.revision < 9_007_199_254_740_991,
        "maximum safe application revision reached"
    );
    let permit = app.evaluations.clone().acquire_owned().await?;
    let now = app.clock.sample(state)?;
    let request_id = "__flower.maintenance".to_owned();
    let mut evaluation = match evaluate_maintenance(
        state,
        &method.name,
        Value::Null,
        now,
        permit,
        admission.clone(),
        true,
    )
    .await
    {
        Ok(evaluation) => evaluation,
        Err(error) => {
            let Some(on_error) = method.on_error else {
                return Err(error);
            };
            anyhow::ensure!(
                on_error.kind == MethodKind::Mutation,
                "maintenance onError must be a mutation"
            );
            let permit = app.evaluations.clone().acquire_owned().await?;
            let failed_at = app.clock.sample(state)?;
            // This evaluation has its own runtime and budget, and starts from
            // the original snapshot. No writes, roots or clock from the failed
            // callback can escape, including when it timed out or panicked.
            let args = json!({
                "error": engine_failure(&error).unwrap_or_else(|| json!({
                    "code": "MAINTENANCE_FAILED",
                    "message": error.to_string(),
                })),
                "failedAt": failed_at,
            });
            evaluate_maintenance(
                state,
                &on_error.name,
                args,
                now,
                permit,
                admission.clone(),
                false,
            )
            .await?
        }
    };
    let next = NextRun::of(&evaluation.value, now);
    // What the run read, which only a write can change, unless a stored cell
    // reads the clock: a later run's preview could then change it by time
    // alone, as an idle run with nothing due may.
    let reads = evaluation
        .mutation_certificate
        .take()
        .filter(|_| state.data.reactive().cacheable());
    // Idle maintenance need not replicate a clock tick. Actual cleanup or a
    // changed reactive outcome commits the sampled clock with its changes.
    if evaluation.puts.keys().all(|key| key == "clock") && evaluation.deletes.is_empty() {
        return Ok((None, next, reads));
    }
    transactions::ensure_write_capacity(state)
        .map_err(|error| anyhow::anyhow!("{}: {}", error.code, error.message))?;
    let again = evaluation
        .value
        .get("$flower")
        .and_then(|hint| hint.get("continue"))
        == Some(&Value::Bool(true));
    Ok((
        Some((
            Commit {
                internal: true,
                request_id,
                fingerprint: String::new(),
                expected_revision: state.revision,
                puts: evaluation.puts,
                deletes: evaluation.deletes,
                result: Value::Null,
            },
            again,
        )),
        next,
        reads,
    ))
}

async fn evaluate_maintenance(
    state: &Snapshot,
    name: &str,
    args: Value,
    now: u64,
    permit: OwnedSemaphorePermit,
    admission: admission::Permit,
    record_reads: bool,
) -> anyhow::Result<evaluator::Evaluation> {
    let data = state.data.clone();
    let invocation = json!({"name":name,"args":args,"requestId":"__flower.maintenance"});
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _admission = admission;
        if record_reads {
            evaluator::invoke_maintenance_at(data, invocation, now)
        } else {
            evaluator::invoke_at(data, invocation, "mutation", now)
        }
    })
    .await?
}

/// Seal an import off-line before sending it to any Flower HTTP endpoint.
/// The wrapping key file must be provisioned separately on authorized nodes.
pub fn seal_key_import(
    wrapping_key_file: &std::path::Path,
    bytes: &[u8],
    format: &str,
) -> anyhow::Result<Value> {
    crate::crypto::managed::seal_import_file(wrapping_key_file, bytes, format)
}

/// This replica's backups: what the leader has shipped, its newest base,
/// retention, errors, and what the store holds for them.
async fn backup_status(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
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
    Ok(Json(match app.consensus.backup_status() {
        Some(status) => json!({"enabled": true, "status": status}),
        None => json!({"enabled": false}),
    }))
}

async fn resource_metrics(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
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
    let mut metrics = app.admission.metrics();
    metrics["watches"] = app.watch_hubs.metrics();
    metrics["authorizations"] = app.authorizations.metrics();
    metrics["snapshots"] = app.consensus.snapshot_policy_metrics();
    metrics["storage"] = app.consensus.storage_metrics();
    metrics["process"] = process::metrics();
    Ok(Json(metrics))
}
