//! Stable HTTP routing for movable logical partitions. Epoch checks in the
//! replicated state machine remain authoritative even when a route is cached.
use super::{
    COMPATIBILITY_HEADER, PeerRequest, PeerResponse, Runtime,
    catalog::{self, CatalogRequest, Placement},
    coordinator,
};
use crate::{
    consensus::{Consensus, PartitionPhase},
    service::{self, ApiError, App, invalid, unavailable},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

const INVOKE_PATH: &str = "/raft/partitions/invoke";
const GROUP_ID_HEADER: &str = "x-flower-group-sha256";
const LEADER_HOP_HEADER: &str = "x-flower-partition-leader-hop";

fn group_identity(group: &str) -> HeaderValue {
    // Group names are exact Unicode strings. HTTP header values must remain
    // ASCII-readable without imposing an unrelated restriction on those names.
    HeaderValue::from_str(&crate::evaluator::hash(group.as_bytes()))
        .expect("SHA-256 hexadecimal is an ASCII header value")
}
type Slot = Arc<Mutex<Option<(Instant, Placement)>>>;

fn ttl() -> anyhow::Result<Duration> {
    let ms = std::env::var("FLOWER_ROUTE_CACHE_MS")
        .ok()
        .map(|v| v.parse::<u64>())
        .transpose()?
        .unwrap_or(1000);
    anyhow::ensure!(
        ms > 0
            && Instant::now()
                .checked_add(Duration::from_millis(ms))
                .is_some(),
        "FLOWER_ROUTE_CACHE_MS must be a positive representable duration"
    );
    Ok(Duration::from_millis(ms))
}
pub(in crate::service) fn validate_configuration() -> anyhow::Result<()> {
    ttl()?;
    Ok(())
}

struct Routes {
    runtime: Arc<Runtime>,
    ttl: Duration,
    entries: Mutex<BTreeMap<String, Slot>>,
}
impl Routes {
    async fn get(&self, partition: &str) -> Result<Placement, ApiError> {
        super::validate_name(partition).map_err(invalid)?;
        let slot = self
            .entries
            .lock()
            .await
            .entry(partition.into())
            .or_default()
            .clone();
        let mut cached = slot.lock().await;
        if let Some((at, placement)) = &*cached
            && at.elapsed() < self.ttl
        {
            return Ok(placement.clone());
        }
        // Only successful active routes are cached. Failure never extends a
        // cached ownership observation or allows an old watch to live forever.
        match self.runtime.resolve(partition).await {
            Ok(placement) if catalog::serving(&placement) => {
                *cached = Some((Instant::now(), placement.clone()));
                Ok(placement)
            }
            result => {
                *cached = None;
                self.entries.lock().await.remove(partition);
                match result {
                    Ok(_) => Err(moving(
                        "partition is being created or moved; retry with the same request ID",
                    )),
                    Err(error) => Err(unavailable(error)),
                }
            }
        }
    }
    async fn invalidate(&self, partition: &str) {
        self.entries.lock().await.remove(partition);
    }
    /// When the cached ownership observation expires, if one is cached.
    async fn expiry(&self, partition: &str) -> Option<Instant> {
        let slot = self.entries.lock().await.get(partition)?.clone();
        let cached = slot.lock().await;
        cached.as_ref().map(|(at, _)| *at + self.ttl)
    }
}

pub(in crate::service) struct Gate {
    routes: Arc<Routes>,
    partition: String,
    epoch: u64,
    owner: String,
    /// Notifies watches when this owner's claim lapses; see lapses().
    monitor: std::sync::Mutex<Option<tokio::sync::watch::Receiver<()>>>,
}
impl Gate {
    /// Changes once this owner's claim lapses: the partition moved away, or its
    /// cached route expired and could not be renewed. Watches wait on it instead
    /// of rechecking ownership on a timer. One monitor serves every watch of the
    /// partition, runs only while some watch listens, and renews the route
    /// exactly when its cached observation expires.
    pub(in crate::service) fn lapses(self: &Arc<Self>) -> tokio::sync::watch::Receiver<()> {
        let mut monitor = self.monitor.lock().expect("partition gate monitor");
        if let Some(receiver) = monitor.as_ref().filter(|receiver| receiver.has_changed().is_ok()) {
            return receiver.clone();
        }
        let (sender, receiver) = tokio::sync::watch::channel(());
        *monitor = Some(receiver.clone());
        let gate = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(current) = gate.upgrade() else { return };
                let expires = current.routes.expiry(&current.partition).await.unwrap_or_else(Instant::now);
                drop(current);
                tokio::time::sleep_until(expires.into()).await;
                // The gate keeps one receiver of its own; nobody else listens.
                if sender.receiver_count() <= 1 {
                    return;
                }
                let Some(current) = gate.upgrade() else { return };
                if current.check().await.is_err() {
                    sender.send_replace(());
                    return;
                }
            }
        });
        receiver
    }

    pub(in crate::service) async fn check(&self) -> Result<(), ApiError> {
        let placement = self.routes.get(&self.partition).await?;
        if placement.epoch != self.epoch || placement.owner.id != self.owner {
            return Err(moving(
                "partition ownership changed; reconnect through its stable URL",
            ));
        }
        Ok(())
    }
}
fn moving(message: &str) -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "PARTITION_MOVING",
        message.into(),
    )
}

struct Registry {
    runtime: Arc<Runtime>,
    routes: Arc<Routes>,
    apps: Mutex<BTreeMap<String, (u64, Arc<App>)>>,
    queries: Arc<Semaphore>,
    admission: Arc<service::admission::Pool>,
    client: reqwest::Client,
    next_read: AtomicUsize,
}
impl Registry {
    async fn app(&self, partition: &str, epoch: u64) -> Result<Arc<App>, ApiError> {
        let bound = self
            .runtime
            .consensus
            .partition(partition, epoch)
            .map_err(unavailable)?;
        bound.snapshot_for(None).await.map_err(unavailable)?;
        let mut apps = self.apps.lock().await;
        if let Some((existing, app)) = apps.get(partition)
            && *existing == epoch
        {
            return Ok(app.clone());
        }
        let gate = Arc::new(Gate {
            routes: self.routes.clone(),
            partition: partition.into(),
            epoch,
            owner: self.runtime.config.local_group.clone(),
            monitor: std::sync::Mutex::new(None),
        });
        let app = service::make_app(
            bound,
            self.runtime.token.clone(),
            self.queries.clone(),
            self.admission.clone(),
            Some(gate),
        );
        apps.insert(partition.into(), (epoch, app.clone()));
        Ok(app)
    }
    async fn refresh(&self) -> anyhow::Result<()> {
        // Startup/activation restores timers without waiting for a user request.
        // The periodic scan is control-plane work, never on a request hot path.
        let partitions = self.runtime.consensus.list_partitions().await?;
        let active: BTreeMap<_, _> = partitions
            .into_iter()
            .filter(|p| p.phase == PartitionPhase::Active)
            .map(|p| (p.partition, p.epoch))
            .collect();
        self.apps
            .lock()
            .await
            .retain(|id, (epoch, _)| active.get(id) == Some(epoch));
        for (id, epoch) in active {
            self.app(&id, epoch)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e.message))?;
        }
        Ok(())
    }
}

pub(in crate::service) fn router(
    consensus: Consensus,
    token: String,
    queries: Arc<Semaphore>,
    admission: Arc<service::admission::Pool>,
) -> Router {
    let Some(runtime) = Runtime::new(consensus, token, admission.clone())
        .expect("validated partition configuration")
    else {
        return Router::new();
    };
    let routes = Arc::new(Routes {
        runtime: runtime.clone(),
        ttl: ttl().expect("validated route cache"),
        entries: Mutex::new(BTreeMap::new()),
    });
    let client = crate::transport::client_builder()
        .expect("validated transport configuration")
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(runtime.consensus.limits().peer_connect_timeout)
        .pool_idle_timeout(runtime.consensus.limits().peer_idle_timeout)
        .build()
        .expect("partition HTTP client");
    let registry = Arc::new(Registry {
        runtime: runtime.clone(),
        routes,
        apps: Mutex::new(BTreeMap::new()),
        queries,
        admission,
        client,
        next_read: AtomicUsize::new(0),
    });
    tokio::spawn(coordinator::run(Arc::downgrade(&runtime)));
    let weak = Arc::downgrade(&registry);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let Some(registry) = weak.upgrade() else {
                break;
            };
            if let Err(error) = registry.refresh().await {
                tracing::debug!(%error, "partition activation scan deferred");
            }
        }
    });
    let public = Router::new()
        .route("/partitions/{partition}/v1/{operation}", post(public_call))
        .route("/partitions/{partition}/admin/deploy", post(public_deploy))
        .route(
            "/partitions/{partition}/admin/deployments",
            post(public_deployments),
        )
        .route("/partitions/{partition}/admin/keys", post(public_keys))
        .route(
            "/partitions/{partition}/admin/transactions",
            post(public_transactions),
        )
        .route(
            "/partitions/{partition}/admin/retention",
            post(public_retention),
        )
        .route("/admin/partitions/catalog", post(admin_catalog))
        .layer(DefaultBodyLimit::max(
            service::tuning::settings()
                .expect("validated body budget")
                .http_max_body_bytes,
        ));
    let peers = Router::new()
        .route(super::CATALOG_PATH, post(peer_catalog))
        .route(super::CONTROL_PATH, post(peer_control))
        .route(INVOKE_PATH, post(peer_invoke))
        .layer(DefaultBodyLimit::max(
            runtime.consensus.limits().rpc_max_bytes,
        ));
    public.merge(peers).with_state(registry)
}

fn authorize(runtime: &Runtime, headers: &HeaderMap) -> Result<(), ApiError> {
    if headers.get("authorization").and_then(|v| v.to_str().ok())
        != Some(format!("Bearer {}", runtime.token).as_str())
    {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "operator bearer token required".into(),
        ));
    }
    Ok(())
}
fn authenticated(runtime: &Runtime, headers: &HeaderMap, group: &str) -> Result<(), ApiError> {
    if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some(format!("Bearer {}", runtime.consensus.peer_token()).as_str())
    {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "peer bearer token required".into(),
        ));
    }
    runtime.authenticate(headers, group).map_err(invalid)
}
fn peer_response(runtime: &Runtime, body: Value) -> Response {
    let mut response = Json(PeerResponse {
        group: runtime.config.local_group.clone(),
        body,
    })
    .into_response();
    response.headers_mut().insert(
        COMPATIBILITY_HEADER,
        HeaderValue::from_str(&crate::consensus::compatibility().contract())
            .expect("contract is a header"),
    );
    response
}
async fn admin_catalog(
    State(registry): State<Arc<Registry>>,
    headers: HeaderMap,
    Json(input): Json<CatalogRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&registry.runtime, &headers)?;
    // Internal phase advancement is restricted to the peer protocol, whose
    // handlers verify native source/destination state before catalog cutover.
    if matches!(
        input,
        CatalogRequest::Advance { .. }
            | CatalogRequest::Ready { .. }
            | CatalogRequest::StepRebalance { .. }
    ) {
        return Err(invalid(anyhow::anyhow!("internal catalog transition")));
    }
    registry
        .runtime
        .catalog(input)
        .await
        .map(Json)
        .map_err(unavailable)
}
async fn peer_catalog(
    State(registry): State<Arc<Registry>>,
    headers: HeaderMap,
    Json(input): Json<PeerRequest<CatalogRequest>>,
) -> Result<Response, ApiError> {
    authenticated(&registry.runtime, &headers, &input.group)?;
    let body = catalog::apply(&registry.runtime, input.body)
        .await
        .map_err(unavailable)?;
    Ok(peer_response(&registry.runtime, body))
}
async fn peer_control(
    State(registry): State<Arc<Registry>>,
    headers: HeaderMap,
    Json(input): Json<PeerRequest<coordinator::ControlRequest>>,
) -> Result<Response, ApiError> {
    authenticated(&registry.runtime, &headers, &input.group)?;
    let body = coordinator::handle(&registry.runtime, input.body)
        .await
        .map_err(unavailable)?;
    Ok(peer_response(&registry.runtime, body))
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Invocation {
    partition: String,
    epoch: u64,
    operation: String,
    input: Value,
}

fn is_leader(registry: &Registry) -> bool {
    let metrics = registry.runtime.consensus.metrics();
    metrics.current_leader == Some(metrics.id) && metrics.state == openraft::ServerState::Leader
}

async fn forward_owner(registry: &Registry, invocation: Invocation) -> Result<Response, ApiError> {
    let metrics = registry.runtime.consensus.metrics();
    let leader = metrics
        .current_leader
        .filter(|id| *id != metrics.id)
        .ok_or_else(|| {
            unavailable(anyhow::anyhow!(
                "partition leader changed; retry the same request ID"
            ))
        })?;
    let node = |id| {
        metrics
            .membership_config
            .nodes()
            .find_map(|(key, node)| (*key == id).then_some(node.addr.as_str()))
    };
    let target = node(leader).ok_or_else(|| {
        unavailable(anyhow::anyhow!(
            "partition leader is absent from membership"
        ))
    })?;
    let source = node(metrics.id).ok_or_else(|| {
        unavailable(anyhow::anyhow!(
            "partition forwarding source is absent from membership"
        ))
    })?;
    let limits = registry.runtime.consensus.limits();
    let timeout = limits
        .read_timeout
        .checked_add(limits.commit_timeout)
        .and_then(|duration| {
            duration.checked_add(
                crate::evaluator::config::settings()
                    .ok()?
                    .evaluation_timeout,
            )
        })
        .filter(|duration| Instant::now().checked_add(*duration).is_some())
        .ok_or_else(|| {
            unavailable(anyhow::anyhow!(
                "partition forwarding deadline exceeds platform limits"
            ))
        })?;
    tokio::time::timeout(timeout, async {
        let mut response = registry
            .client
            .post(crate::transport::peer_url(target, INVOKE_PATH))
            .bearer_auth(registry.runtime.consensus.peer_token())
            .header(
                COMPATIBILITY_HEADER,
                crate::consensus::compatibility().contract(),
            )
            .header(LEADER_HOP_HEADER, "1")
            .header("x-flower-target-node-id", leader.to_string())
            .header("x-flower-target-address", target)
            .header("x-flower-source-node-id", metrics.id.to_string())
            .header("x-flower-source-address", source)
            .json(&PeerRequest {
                group: registry.runtime.config.local_group.clone(),
                body: invocation,
            })
            .send()
            .await
            .map_err(|error| unavailable(error.into()))?;
        if response
            .headers()
            .get(COMPATIBILITY_HEADER)
            .and_then(|value| value.to_str().ok())
            != Some(crate::consensus::compatibility().contract().as_str())
            || response.headers().get(GROUP_ID_HEADER)
                != Some(&group_identity(&registry.runtime.config.local_group))
            || response
                .headers()
                .get("x-flower-node-id")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                != Some(leader)
            || response
                .headers()
                .get("x-flower-node-address")
                .and_then(|value| value.to_str().ok())
                != Some(target)
        {
            return Err(unavailable(anyhow::anyhow!(
                "partition leader response identity mismatch"
            )));
        }
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| unavailable(error.into()))?
        {
            if bytes.len().saturating_add(chunk.len()) > limits.rpc_max_bytes {
                return Err(unavailable(anyhow::anyhow!(
                    "partition leader response exceeds FLOWER_RPC_MAX_BYTES"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if status == StatusCode::SERVICE_UNAVAILABLE
            && service::forwarding::retryable_unavailable(&bytes)
        {
            return Err(unavailable(anyhow::anyhow!(
                "partition leader changed or is unavailable; retry the same request ID"
            )));
        }
        Ok((status, [("content-type", "application/json")], bytes).into_response())
    })
    .await
    .map_err(|error| unavailable(error.into()))?
}

async fn dispatch(
    registry: &Registry,
    invocation: Invocation,
    allow_forward: bool,
) -> Result<Response, ApiError> {
    match invocation.operation.as_str() {
        "mutate" => {
            crate::evaluator::validate_invocation(&invocation.input, "mutation").map_err(invalid)?
        }
        "deploy" => service::validate_deployment(&invocation.input)?,
        "keys" => service::keys::validate(&invocation.input)?,
        "retention" => service::retention::validate(&invocation.input)?,
        "session" => service::retention::validate_session(&invocation.input)?,
        "transactions" => service::transactions::validate_admin(&invocation.input)?,
        "deployments" => service::staged_deployment::validate_admin(&invocation.input)?,
        "call" => {
            crate::evaluator::validate_invocation(&invocation.input, "call").map_err(invalid)?
        }
        "query" | "watch" | "identity" | "tx-status" | "tx-prepare" | "tx-finish"
        | "tx-closure-status" | "tx-closure-ack" => {}
        _ => {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "METHOD_NOT_FOUND",
                "unknown partition endpoint".into(),
            ));
        }
    }
    if matches!(
        invocation.operation.as_str(),
        "mutate"
            | "deploy"
            | "keys"
            | "retention"
            | "session"
            | "transactions"
            | "deployments"
            | "tx-prepare"
            | "tx-finish"
            | "tx-closure-ack"
    ) && !is_leader(registry)
    {
        return if allow_forward {
            forward_owner(registry, invocation).await
        } else {
            Err(unavailable(anyhow::anyhow!(
                "forwarded partition request reached a former leader"
            )))
        };
    }
    let app = match registry.app(&invocation.partition, invocation.epoch).await {
        Ok(app) => app,
        // A newly activated partition may not yet be applied on this seed. A
        // generic call has no locally resolvable kind in that case; the leader
        // can safely resolve it without requiring clients to select a member.
        Err(_) if allow_forward && invocation.operation == "call" && !is_leader(registry) => {
            return forward_owner(registry, invocation).await;
        }
        Err(error) => return Err(error),
    };
    // Recovery must continue after the catalog starts a move. Native freeze
    // waits for prepared work and incomplete coordinators, and ownership epochs
    // still fence it.
    if !matches!(
        invocation.operation.as_str(),
        "tx-status" | "tx-finish" | "tx-closure-status" | "tx-closure-ack"
    ) && let Some(gate) = &app.partition_gate
    {
        gate.check().await?;
    }
    match invocation.operation.as_str() {
        "tx-status" | "tx-prepare" | "tx-finish" | "tx-closure-status" | "tx-closure-ack" => {
            service::transactions::partition_rpc(&app, &invocation.operation, invocation.input)
                .await
                .map(|value| peer_response(&registry.runtime, value))
        }
        "query" => service::query_http(State(app), Json(invocation.input))
            .await
            .map(IntoResponse::into_response),
        "watch" => service::watch::watch(State(app), Json(invocation.input)).await,
        "call" => {
            if let Some(response) =
                service::cached_query_response(&app, &invocation.input, None).await?
            {
                return Ok(response);
            }
            // Resolve once, using the same consistency selection as /v1/call.
            // Query calls keep execution on this replica; only writes hop.
            let (state, method, permit) =
                service::query_snapshot(&app, &invocation.input, None).await?;
            if method.kind == crate::evaluator::MethodKind::Query {
                let now = app.clock.sample(&state).map_err(unavailable)?;
                return service::evaluate_query(&app, state, invocation.input, method, now, permit)
                    .await
                    .map(|result| result.response().into_response());
            }
            drop(state);
            drop(permit);
            crate::evaluator::validate_invocation(&invocation.input, "mutation")
                .map_err(invalid)?;
            if !is_leader(registry) {
                return if allow_forward {
                    forward_owner(registry, invocation).await
                } else {
                    Err(unavailable(anyhow::anyhow!(
                        "forwarded partition request reached a former leader"
                    )))
                };
            }
            service::commit_method(app, invocation.input, false)
                .await
                .map(IntoResponse::into_response)
        }
        "keys" => service::keys::commit(app, invocation.input)
            .await
            .map(|value| Json(value).into_response()),
        "identity" => service::retention::identity(State(app))
            .await
            .map(IntoResponse::into_response),
        "retention" => service::retention::commit(app, invocation.input)
            .await
            .map(|value| Json(value).into_response()),
        "session" => service::retention::session_commit(app, invocation.input)
            .await
            .map(|value| Json(value).into_response()),
        "transactions" => service::transactions::administer(&app, invocation.input)
            .await
            .map(|value| Json(value).into_response()),
        "deployments" => service::staged_deployment::administer(&app, invocation.input)
            .await
            .map(|value| Json(value).into_response()),
        "mutate" | "deploy" => {
            service::commit_method(app, invocation.input, invocation.operation == "deploy")
                .await
                .map(IntoResponse::into_response)
        }
        _ => unreachable!("operation validated above"),
    }
}
async fn peer_invoke(
    State(registry): State<Arc<Registry>>,
    headers: HeaderMap,
    Json(request): Json<PeerRequest<Invocation>>,
) -> Response {
    let metrics = registry.runtime.consensus.metrics();
    let result = async {
        authenticated(&registry.runtime, &headers, &request.group)?;
        let hopped = match headers
            .get(LEADER_HOP_HEADER)
            .and_then(|value| value.to_str().ok())
        {
            None => false,
            Some("1") => true,
            Some(_) => return Err(invalid(anyhow::anyhow!("invalid partition forwarding hop"))),
        };
        if hopped {
            service::forwarding::verify_identity(&headers, &metrics)?;
            if metrics.current_leader != Some(metrics.id)
                || metrics.state != openraft::ServerState::Leader
            {
                return Err(unavailable(anyhow::anyhow!(
                    "forwarded partition request reached a former leader"
                )));
            }
        }
        // Even if leadership changes after the check, dispatch(false) cannot
        // forward again. The normal quorum writer decides whether it may commit.
        dispatch(&registry, request.body, !hopped).await
    }
    .await;
    let mut response = match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    response.headers_mut().insert(
        COMPATIBILITY_HEADER,
        HeaderValue::from_str(&crate::consensus::compatibility().contract())
            .expect("contract header"),
    );
    response.headers_mut().insert(
        GROUP_ID_HEADER,
        group_identity(&registry.runtime.config.local_group),
    );
    response.headers_mut().insert(
        "x-flower-node-id",
        metrics.id.to_string().parse().expect("numeric node ID"),
    );
    if let Some(address) = metrics
        .membership_config
        .nodes()
        .find_map(|(id, node)| (*id == metrics.id).then_some(&node.addr))
    {
        response.headers_mut().insert(
            "x-flower-node-address",
            HeaderValue::from_str(address).expect("validated node address"),
        );
    }
    response
}
async fn public_call(
    State(registry): State<Arc<Registry>>,
    Path((partition, operation)): Path<(String, String)>,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    if !["query", "mutate", "call", "watch", "identity", "session"].contains(&operation.as_str()) {
        return Err(invalid(anyhow::anyhow!("unknown partition endpoint")));
    }
    route(&registry, partition, operation, input).await
}
async fn public_deploy(
    State(registry): State<Arc<Registry>>,
    Path(partition): Path<String>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    authorize(&registry.runtime, &headers)?;
    route(&registry, partition, "deploy".into(), input).await
}
async fn public_deployments(
    State(registry): State<Arc<Registry>>,
    Path(partition): Path<String>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    authorize(&registry.runtime, &headers)?;
    service::staged_deployment::validate_admin(&input)?;
    route(&registry, partition, "deployments".into(), input).await
}
async fn public_retention(
    State(registry): State<Arc<Registry>>,
    Path(partition): Path<String>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    authorize(&registry.runtime, &headers)?;
    service::retention::validate(&input)?;
    route(&registry, partition, "retention".into(), input).await
}

async fn public_transactions(
    State(registry): State<Arc<Registry>>,
    Path(partition): Path<String>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    authorize(&registry.runtime, &headers)?;
    service::transactions::validate_admin(&input)?;
    route(&registry, partition, "transactions".into(), input).await
}
async fn public_keys(
    State(registry): State<Arc<Registry>>,
    Path(partition): Path<String>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    authorize(&registry.runtime, &headers)?;
    service::keys::validate(&input)?;
    if input["operation"] == "cache" {
        // Prepared contexts are process-wide; inspect this ingress node even
        // when the logical partition is currently owned by another group.
        return service::keys::cache(&registry.runtime.consensus)
            .await
            .map(|value| Json(value).into_response());
    }
    route(&registry, partition, "keys".into(), input).await
}

async fn route(
    registry: &Registry,
    partition: String,
    operation: String,
    input: Value,
) -> Result<Response, ApiError> {
    // At most one refresh/retry of routing; the caller controls retries after an
    // uncertain result and retains its original mutation request identity.
    let mut last = moving("partition owner is unavailable");
    for _ in 0..2 {
        let placement = registry.routes.get(&partition).await?;
        let invocation = Invocation {
            partition: partition.clone(),
            epoch: placement.epoch,
            operation: operation.clone(),
            input: input.clone(),
        };
        if placement.owner.id == registry.runtime.config.local_group {
            match dispatch(registry, invocation.clone(), true).await {
                Ok(response) => return Ok(response),
                Err(error)
                    if error.status == StatusCode::SERVICE_UNAVAILABLE
                        && matches!(error.code, "PARTITION_MOVING" | "UNAVAILABLE") =>
                {
                    last = error
                }
                Err(error) => return Err(error),
            }
        }
        // A local follower may not commit, so try every configured owner peer.
        // The immutable invocation carries exactly the same request ID/body.
        let count = placement.owner.addresses.len();
        let first = if matches!(operation.as_str(), "query" | "watch") {
            registry.next_read.fetch_add(1, Ordering::Relaxed) % count
        } else {
            0
        };
        for offset in 0..count {
            let address = &placement.owner.addresses[(first + offset) % count];
            let request = registry
                .client
                .post(crate::transport::peer_url(address, INVOKE_PATH))
                .bearer_auth(registry.runtime.consensus.peer_token())
                .header(
                    COMPATIBILITY_HEADER,
                    crate::consensus::compatibility().contract(),
                )
                .json(&PeerRequest {
                    group: placement.owner.id.clone(),
                    body: &invocation,
                });
            let deadline =
                tokio::time::Instant::now() + registry.runtime.consensus.limits().read_timeout;
            let response = tokio::time::timeout_at(deadline, request.send()).await;
            let mut response = match response {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    last = unavailable(error.into());
                    continue;
                }
                Err(error) => {
                    last = unavailable(error.into());
                    continue;
                }
            };
            let status = response.status();
            if status == StatusCode::SERVICE_UNAVAILABLE {
                // Preserve application backpressure and prepared barriers. A
                // retryable HTTP status alone is not evidence of stale routing.
                let limit = registry.runtime.consensus.limits().rpc_max_bytes;
                let bytes = tokio::time::timeout_at(deadline, async {
                    let mut bytes = Vec::new();
                    while let Some(chunk) =
                        response.chunk().await.map_err(|e| unavailable(e.into()))?
                    {
                        if bytes.len().saturating_add(chunk.len()) > limit {
                            return Err(unavailable(anyhow::anyhow!(
                                "partition error exceeds response budget"
                            )));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    Ok(bytes)
                })
                .await
                .map_err(|e| unavailable(e.into()))??;
                if !service::forwarding::retryable_unavailable(&bytes) {
                    return Ok(
                        (status, [("content-type", "application/json")], bytes).into_response()
                    );
                }
                last = moving("partition owner is unavailable or moving");
                continue;
            }
            if status.is_success()
                && (response
                    .headers()
                    .get(COMPATIBILITY_HEADER)
                    .and_then(|v| v.to_str().ok())
                    != Some(crate::consensus::compatibility().contract().as_str())
                    || response.headers().get(GROUP_ID_HEADER)
                        != Some(&group_identity(&placement.owner.id)))
            {
                return Err(unavailable(anyhow::anyhow!(
                    "partition response identity mismatch"
                )));
            }
            if operation == "watch" && status.is_success() {
                let stream = futures_util::stream::unfold(Some(response), |response| async move {
                    let mut response = response?;
                    match response.chunk().await {
                        Ok(Some(chunk)) => Some((Ok::<_, reqwest::Error>(chunk), Some(response))),
                        Ok(None) => None,
                        Err(error) => Some((Err(error), None)),
                    }
                });
                return Ok((
                    [
                        ("content-type", "text/event-stream"),
                        ("cache-control", "no-cache"),
                        ("x-accel-buffering", "no"),
                    ],
                    Body::from_stream(stream),
                )
                    .into_response());
            }
            let limit = registry.runtime.consensus.limits().rpc_max_bytes;
            let body = tokio::time::timeout_at(deadline, async {
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    anyhow::ensure!(
                        bytes.len().saturating_add(chunk.len()) <= limit,
                        "partition response exceeds FLOWER_RPC_MAX_BYTES"
                    );
                    bytes.extend_from_slice(&chunk);
                }
                Ok::<_, anyhow::Error>(bytes)
            })
            .await
            .map_err(|e| unavailable(e.into()))?
            .map_err(unavailable)?;
            return Ok((status, [("content-type", "application/json")], body).into_response());
        }
        registry.routes.invalidate(&partition).await;
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_group_identity_is_ascii_and_binds_exact_utf8_name() {
        for group in ["🌸-東京", "café", "cafe\u{301}"] {
            let header = group_identity(group);
            let encoded = header.to_str().unwrap();
            assert_eq!(encoded.len(), 64);
            assert!(encoded.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_eq!(encoded, crate::evaluator::hash(group.as_bytes()));
            assert_eq!(header, group_identity(group));
        }
        assert_ne!(group_identity("café"), group_identity("cafe\u{301}"));
        assert_ne!(group_identity("🌸"), group_identity("🌻"));
    }
}
