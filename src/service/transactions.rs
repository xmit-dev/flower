//! Blocking, durable two-phase commit between logical databases, whether they
//! share a Raft group or not.
//!
//! The participant lock is itself replicated. No application state is published
//! before a durable commit decision; ordinary reads and writes are barred while
//! prepared. A lost coordinator therefore sacrifices availability, never atomicity.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::*;
mod closure;
mod targets;
use targets::{Target, local_target};

const COORDINATOR: &str = "transaction:coordinator:";
const PARTICIPANT: &str = "transaction:participant";
const COMPLETED: &str = "transaction:completed:";
const COMPATIBILITY_HEADER: &str = "x-flower-compatibility";
const RESERVED_REVISIONS: &str = "transaction:reserved-revisions";
const MAX_REVISION: u64 = 9_007_199_254_740_991;
const ABORT_REASON: &str = "participant preparation failed";

pub(super) struct Runtime {
    group: Option<String>,
    groups: BTreeMap<String, Vec<String>>,
    client: reqwest::Client,
    coordinator: Mutex<BTreeMap<String, Weak<Mutex<()>>>>,
    // Built on first use so catalog and partition RPCs share pooled connections.
    partitions: std::sync::OnceLock<Arc<super::partitions::Runtime>>,
}

impl Runtime {
    pub(super) fn validate_configuration() -> anyhow::Result<()> {
        Self::new().map(|_| ())
    }

    pub(super) fn new() -> anyhow::Result<Self> {
        let group = std::env::var("FLOWER_GROUP").ok();
        let registry = std::env::var("FLOWER_GROUPS").ok();
        let groups = match (&group, registry) {
            (None, None) => BTreeMap::new(),
            (Some(group), Some(registry)) => parse_registry(group, &registry)?,
            _ => anyhow::bail!("FLOWER_GROUP and FLOWER_GROUPS must be configured together"),
        };
        let limits = crate::consensus::Limits::from_env()?;
        Ok(Self {
            group,
            groups,
            client: crate::transport::client_builder()?
                .connect_timeout(limits.peer_connect_timeout)
                .pool_idle_timeout(limits.peer_idle_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()?,
            coordinator: Mutex::new(BTreeMap::new()),
            partitions: std::sync::OnceLock::new(),
        })
    }

    async fn coordinator_for(&self, id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.coordinator.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(id.into(), Arc::downgrade(&lock));
        lock
    }

    fn name(&self) -> Result<&str, ApiError> {
        self.group.as_deref().ok_or_else(|| {
            unavailable(anyhow::anyhow!(
                "cross-group transactions require FLOWER_GROUP and FLOWER_GROUPS"
            ))
        })
    }
}

fn parse_registry(group: &str, registry: &str) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
    let groups: BTreeMap<String, Vec<String>> = serde_json::from_str(registry)
        .context("FLOWER_GROUPS must map group names to arrays of host:port addresses")?;
    ensure!(
        !group.is_empty() && groups.contains_key(group),
        "FLOWER_GROUP must name a configured group"
    );
    let mut addresses = BTreeSet::new();
    for (name, peers) in &groups {
        ensure!(
            !name.is_empty() && !peers.is_empty(),
            "each group requires a name and at least one peer"
        );
        for peer in peers {
            let port = peer
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok());
            let url = reqwest::Url::parse(&format!("http://{peer}"))?;
            ensure!(
                url.host_str().is_some()
                    && port.is_some_and(|port| port > 0)
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "group peers must be host:port addresses"
            );
            ensure!(
                addresses.insert(url.to_string()),
                "a peer address cannot belong to multiple groups or appear twice"
            );
        }
    }
    Ok(groups)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Phase {
    Preparing,
    Commit,
    Abort,
}

// JSON arguments, patches, and results are strings in protocol metadata so the
// metadata envelope does not subtract from the application's JSON depth budget.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Call {
    group: Target,
    method: String,
    args: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Coordinator {
    history: String,
    sequence: u64,
    #[serde(default)]
    receipt_reservation: u64,
    #[serde(default)]
    principal: Value,
    coordinator: Target,
    fingerprint: String,
    calls: Vec<Call>,
    value: Option<String>,
    phase: Phase,
    results: Option<String>,
    reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<Value>,
    complete: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {
    history: String,
    sequence: u64,
    coordinator: Target,
    request_id: String,
    fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    group: String,
    transaction: Reference,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Prepared {
    transaction: Reference,
    patch: String,
    results: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Done {
    transaction: Reference,
    phase: Phase,
    results: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Patch {
    #[serde(deserialize_with = "patch_records")]
    puts: BTreeMap<String, Value>,
    deletes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IndexedResult {
    index: usize,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    calls: Vec<PlanCall>,
    #[serde(default)]
    value: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanCall {
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    partition: Option<String>,
    method: String,
    #[serde(default)]
    args: Value,
}

fn conflict(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "TRANSACTION_CONFLICT", message.into())
}
fn reused() -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "REQUEST_ID_REUSED",
        "requestId was already used for different content".into(),
    )
}
fn aborted(record: &Coordinator) -> ApiError {
    let error = ApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "TRANSACTION_ABORTED",
        record
            .reason
            .clone()
            .unwrap_or_else(|| "transaction aborted".into()),
    );
    match &record.failure {
        Some(failure) => error.with_failure(failure.clone()),
        None => error,
    }
}
fn coordinator_key(id: &str) -> String {
    format!("{COORDINATOR}{id}")
}
fn done_key(reference: &Reference) -> String {
    format!(
        "{COMPLETED}{}:{:016x}",
        reference.history, reference.sequence
    )
}

fn decode<T: DeserializeOwned>(value: &Value) -> Result<T, ApiError> {
    serde_json::from_value(value.clone()).map_err(|error| unavailable(error.into()))
}
fn packed<T: Serialize>(value: &T) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(|error| unavailable(error.into()))
}
fn unpacked<T: DeserializeOwned>(value: &str) -> Result<T, ApiError> {
    serde_json::from_str(value).map_err(|error| unavailable(error.into()))
}

fn patch_records<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<BTreeMap<String, Value>, D::Error> {
    let records = BTreeMap::<String, Box<serde_json::value::RawValue>>::deserialize(decoder)?;
    records
        .into_iter()
        .map(|(key, raw)| {
            serde_json::from_str(raw.get())
                .map(|value| (key, value))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}

fn record(state: &Snapshot, reference: &Reference) -> Result<Coordinator, ApiError> {
    closure::coordinator_open(state, reference)?;
    let value = state
        .data
        .get(&coordinator_key(&reference.request_id))
        .ok_or_else(|| conflict("coordinator has no durable transaction record"))?;
    let record: Coordinator = decode(value)?;
    if record.fingerprint != reference.fingerprint
        || record.coordinator != reference.coordinator
        || record.history != reference.history
        || record.sequence != reference.sequence
    {
        return Err(reused());
    }
    Ok(record)
}

pub(super) fn ensure_unlocked(state: &Snapshot) -> Result<(), ApiError> {
    if state.data.contains_key(PARTICIPANT) {
        return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "TRANSACTION_PREPARED",
            "group is prepared for a cross-group transaction; retry after its durable decision is applied".into()));
    }
    Ok(())
}

pub(super) fn ensure_request_id_available(
    state: &Snapshot,
    request_id: &str,
) -> Result<(), ApiError> {
    if state.data.contains_key(&coordinator_key(request_id)) {
        return Err(reused());
    }
    super::staged_deployment::ensure_request_id_available(state, request_id)
}

pub(super) fn router() -> Router<Arc<App>> {
    Router::new()
        .merge(closure::router())
        .route("/raft/transactions/status", post(status_route))
        .route("/raft/transactions/prepare", post(prepare_route))
        .route("/raft/transactions/finish", post(finish_route))
        .layer(DefaultBodyLimit::max(
            crate::consensus::Limits::from_env()
                .expect("validated RPC budgets")
                .rpc_max_bytes,
        ))
}

fn authenticate_headers(app: &App, headers: &HeaderMap) -> Result<(), ApiError> {
    if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some(format!("Bearer {}", app.consensus.peer_token()).as_str())
    {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "operator bearer token required".into(),
        ));
    }
    if headers
        .get(COMPATIBILITY_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(crate::consensus::compatibility().contract().as_str())
    {
        return Err(conflict("cross-group compatibility contract mismatch"));
    }
    Ok(())
}
fn validate_target(app: &App, request: &Request) -> Result<(), ApiError> {
    if request.group != app.cross_group.name()? {
        return Err(conflict("transaction RPC reached the wrong Raft group"));
    }
    if request.transaction.coordinator.partition.is_none()
        && !app
            .cross_group
            .groups
            .contains_key(&request.transaction.coordinator.group)
    {
        return Err(conflict(
            "transaction coordinator is not a configured group",
        ));
    }
    Ok(())
}
pub(super) async fn partition_rpc(
    app: &App,
    action: &str,
    input: Value,
) -> Result<Value, ApiError> {
    if action.starts_with("tx-closure-") {
        return closure::partition_rpc(app, action, input).await;
    }
    let reference: Reference = decode(&input)?;
    match action {
        "tx-status" => Ok(json!(local_status(app, &reference).await?)),
        "tx-prepare" => Ok(json!(prepare(app, &reference).await?)),
        "tx-finish" => Ok(json!(finish(app, &reference).await?)),
        _ => Err(conflict("invalid partition transaction operation")),
    }
}

async fn status_route(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let request = authenticated_request(&app, request).await?;
    let data = local_status(&app, &request.transaction).await?;
    Ok(rpc_response(&request.group, data))
}
async fn prepare_route(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let request = authenticated_request(&app, request).await?;
    let data = prepare(&app, &request.transaction).await?;
    Ok(rpc_response(&request.group, data))
}
async fn finish_route(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let request = authenticated_request(&app, request).await?;
    let data = finish(&app, &request.transaction).await?;
    Ok(rpc_response(&request.group, data))
}

async fn authenticated_request(
    app: &Arc<App>,
    request: axum::extract::Request,
) -> Result<Request, ApiError> {
    use axum::extract::FromRequest;
    authenticate_headers(app, request.headers())?;
    let Json(request) = Json::<Request>::from_request(request, app)
        .await
        .map_err(|error| ApiError::new(error.status(), "INPUT_INVALID", error.body_text()))?;
    validate_target(app, &request)?;
    Ok(request)
}

fn rpc_response(group: &str, data: impl Serialize) -> Response {
    (
        [(
            COMPATIBILITY_HEADER,
            crate::consensus::compatibility().contract(),
        )],
        Json(json!({"group":group,"data":data})),
    )
        .into_response()
}

async fn local_status(app: &App, reference: &Reference) -> Result<Coordinator, ApiError> {
    if !targets::is_local(app, &reference.coordinator)? {
        return Err(conflict("status must be read from the coordinator group"));
    }
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    record(&state, reference)
}

async fn contact(
    app: &App,
    target: &Target,
    action: &str,
    reference: &Reference,
) -> Result<Value, ApiError> {
    let group = &target.group;
    let request = Request {
        group: group.clone(),
        transaction: reference.clone(),
    };
    if targets::is_local(app, target)? {
        validate_target(app, &request)?;
        return match action {
            "status" => Ok(json!(local_status(app, reference).await?)),
            "prepare" => Ok(json!(prepare(app, reference).await?)),
            "finish" => Ok(json!(finish(app, reference).await?)),
            _ => unreachable!(),
        };
    }
    if target.partition.is_some() {
        return targets::contact_partition(app, target, action, reference).await;
    }
    remote_contact(app, target, action, json!(request)).await
}

async fn remote_contact(
    app: &App,
    target: &Target,
    action: &str,
    request: Value,
) -> Result<Value, ApiError> {
    let group = &target.group;
    let peers = app
        .cross_group
        .groups
        .get(group)
        .ok_or_else(|| invalid(anyhow::anyhow!("unknown group {group:?}")))?;
    let limits = app.consensus.limits();
    let timeout = limits
        .read_timeout
        .saturating_add(limits.commit_timeout)
        .saturating_add(
            evaluator::config::settings()
                .map_err(unavailable)?
                .evaluation_timeout,
        );
    let mut last = String::from("no peer answered");
    for peer in peers {
        let response = app
            .cross_group
            .client
            .post(crate::transport::peer_url(
                peer,
                &format!("/raft/transactions/{action}"),
            ))
            .bearer_auth(app.consensus.peer_token())
            .header(
                COMPATIBILITY_HEADER,
                crate::consensus::compatibility().contract(),
            )
            .timeout(timeout)
            .json(&request)
            .send()
            .await;
        let mut response = match response {
            Ok(value) => value,
            Err(error) => {
                last = error.to_string();
                continue;
            }
        };
        let status = response.status();
        if status.is_success()
            && response
                .headers()
                .get(COMPATIBILITY_HEADER)
                .and_then(|value| value.to_str().ok())
                != Some(crate::consensus::compatibility().contract().as_str())
        {
            return Err(conflict(
                "cross-group response compatibility contract mismatch",
            ));
        }
        let mut body = Vec::new();
        let mut failed = false;
        loop {
            match response.chunk().await {
                Ok(Some(chunk))
                    if body.len().saturating_add(chunk.len()) <= limits.rpc_max_bytes =>
                {
                    body.extend_from_slice(&chunk)
                }
                Ok(Some(_)) => {
                    last = "transaction RPC response exceeds FLOWER_RPC_MAX_BYTES".into();
                    failed = true;
                    break;
                }
                Ok(None) => break,
                Err(error) => {
                    last = error.to_string();
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            continue;
        }
        if !status.is_success() {
            if matches!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY | StatusCode::FORBIDDEN
            ) && let Some(failure) = remote_failure(&body)
            {
                return Err(evaluation_error(failure));
            }
            last = String::from_utf8_lossy(&body).into_owned();
            continue;
        }
        let response: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(error) => {
                last = error.to_string();
                continue;
            }
        };
        if response.get("group").and_then(Value::as_str) != Some(group.as_str()) {
            return Err(conflict(
                "transaction RPC response has the wrong group identity",
            ));
        }
        return response
            .get("data")
            .cloned()
            .ok_or_else(|| unavailable(anyhow::anyhow!("invalid transaction RPC response")));
    }
    Err(unavailable(anyhow::anyhow!("group {group:?}: {last}")))
}

// Boxing this boundary breaks the local-dispatch recursion (prepare -> status ->
// contact). Status itself only performs a quorum read, never another RPC.
async fn status(app: &App, reference: &Reference) -> Result<Coordinator, ApiError> {
    let value = Box::pin(contact(app, &reference.coordinator, "status", reference)).await?;
    decode(&value)
}

async fn persist(
    app: &App,
    state: &Snapshot,
    puts: BTreeMap<String, Value>,
    deletes: Vec<String>,
) -> Result<(), ApiError> {
    persist_in_term(app, state, puts, deletes, None).await
}

async fn persist_in_term(
    app: &App,
    state: &Snapshot,
    puts: BTreeMap<String, Value>,
    deletes: Vec<String>,
    term: Option<u64>,
) -> Result<(), ApiError> {
    let mut command = Commit {
        internal: true,
        request_id: String::new(),
        fingerprint: String::new(),
        expected_revision: state.revision,
        puts,
        deletes,
        result: Value::Null,
    };
    ensure_commit_capacity(state, &mut command)?;
    check_command_budget(app, &command)?;
    match term {
        Some(term) => app.consensus.commit_in_term(command, term).await,
        None => app.consensus.commit(command).await,
    }
    .map_err(unavailable)?;
    Ok(())
}

fn exhausted() -> ApiError {
    ApiError::new(
        StatusCode::INSUFFICIENT_STORAGE,
        "REVISION_EXHAUSTED",
        "insufficient safe revisions remain to finish outstanding cross-group transactions".into(),
    )
}

fn reserved_revisions(state: &Snapshot) -> Result<u64, ApiError> {
    let prepared: Option<Prepared> = state.data.get(PARTICIPANT).map(decode).transpose()?;
    let mut pending = u64::from(prepared.is_some())
        .checked_add(closure::reserved_revisions(state)?)
        .ok_or_else(exhausted)?;
    for item in closure::active_records(state) {
        let (key, record) = item?;
        if record.complete {
            continue;
        }
        let reference = Reference {
            history: record.history.clone(),
            sequence: record.sequence,
            coordinator: record.coordinator.clone(),
            request_id: key[COORDINATOR.len()..].into(),
            fingerprint: record.fingerprint.clone(),
        };
        // Every coordinator needs its completion record/receipt. Undecided
        // coordinators additionally need their irrevocable decision record.
        pending = pending
            .checked_add(1 + u64::from(record.phase == Phase::Preparing))
            .ok_or_else(exhausted)?;
        if record.calls.iter().any(|call| {
            call.group.group == record.coordinator.group
                && call.group.partition == record.coordinator.partition
                && call.group.epoch == record.coordinator.epoch
        }) && !state.data.contains_key(&done_key(&reference))
        {
            let is_prepared = prepared
                .as_ref()
                .is_some_and(|lock| lock.transaction == reference);
            if !is_prepared && record.phase != Phase::Abort {
                // Prepared locks already reserve their finalization above.
                pending = pending
                    .checked_add(1 + u64::from(record.phase == Phase::Preparing))
                    .ok_or_else(exhausted)?;
            }
        }
    }
    Ok(pending)
}

pub(super) fn ensure_write_capacity(state: &Snapshot) -> Result<(), ApiError> {
    // Transaction metadata maintains this aggregate atomically. Ordinary writes
    // pay one lookup, not a scan of historical coordinator/receipt records.
    let reserved = match state.data.get(RESERVED_REVISIONS) {
        None => 0,
        Some(value) => value.as_u64().ok_or_else(|| {
            unavailable(anyhow::anyhow!("invalid transaction revision reservation"))
        })?,
    };
    if state
        .revision
        .checked_add(1)
        .and_then(|next| next.checked_add(reserved))
        .is_none_or(|next| next > MAX_REVISION)
    {
        return Err(exhausted());
    }
    Ok(())
}

fn ensure_commit_capacity(state: &Snapshot, command: &mut Commit) -> Result<(), ApiError> {
    closure::index_command(state, command)?;
    let mut next = state.clone();
    next.revision = next.revision.checked_add(1).ok_or_else(exhausted)?;
    for key in &command.deletes {
        next.data.remove(key);
    }
    for (key, value) in &command.puts {
        next.data.insert(key.clone(), value.clone());
    }
    let reserved = reserved_revisions(&next)?;
    if next
        .revision
        .checked_add(reserved)
        .is_none_or(|end| end > MAX_REVISION)
    {
        return Err(exhausted());
    }
    command
        .puts
        .insert(RESERVED_REVISIONS.into(), json!(reserved));
    let receipt_reservation = closure::active_records(&next).try_fold(0u64, |total, item| {
        let (_, record) = item?;
        total
            .checked_add(record.receipt_reservation)
            .ok_or_else(exhausted)
    })?;
    command.puts.insert(
        crate::consensus::retention::RESERVED_BYTES.into(),
        json!(receipt_reservation),
    );
    Ok(())
}

fn metadata_command(reference: &Reference, record: &Coordinator) -> Commit {
    Commit {
        internal: true,
        request_id: String::new(),
        fingerprint: String::new(),
        expected_revision: MAX_REVISION,
        puts: BTreeMap::from([(coordinator_key(&reference.request_id), json!(record))]),
        deletes: vec![],
        result: Value::Null,
    }
}

fn plan(
    coordinator: &str,
    value: Value,
    fingerprint: String,
    groups: &BTreeMap<String, Vec<String>>,
) -> Result<Coordinator, ApiError> {
    let has_value = value
        .as_object()
        .is_some_and(|object| object.contains_key("value"));
    let plan: Plan = serde_json::from_value(value).map_err(|error| invalid(error.into()))?;
    let calls = plan
        .calls
        .into_iter()
        .map(|call| {
            if (call.group.is_some()==call.partition.is_some()) || call.method.is_empty()
                || call.group.as_ref().is_some_and(|group|!groups.contains_key(group))
                || call.partition.as_ref().is_some_and(|partition|super::partitions::validate_name(partition).is_err()) {
                return Err(invalid(anyhow::anyhow!(
                    "transaction calls require exactly one configured group or logical partition, and a nonempty method"
                )));
            }
            Ok(Call {
                group: Target{group:call.group.unwrap_or_default(),partition:call.partition,epoch:0,addresses:Vec::new()},
                method: call.method,
                args: packed(&call.args)?,
            })
        })
        .collect::<Result<_, ApiError>>()?;
    Ok(Coordinator {
        history: String::new(),
        sequence: 0,
        receipt_reservation: 0,
        principal: Value::Null,
        coordinator: coordinator.into(),
        fingerprint,
        calls,
        value: if has_value {
            Some(packed(&plan.value.unwrap_or(Value::Null))?)
        } else {
            None
        },
        phase: Phase::Preparing,
        results: None,
        reason: None,
        failure: None,
        complete: false,
    })
}

fn participants(record: &Coordinator) -> BTreeSet<Target> {
    record.calls.iter().map(|call| call.group.clone()).collect()
}
fn participant_calls<'a>(
    record: &'a Coordinator,
    group: &Target,
) -> Result<Vec<(usize, &'a Call)>, ApiError> {
    let calls: Vec<_> = record
        .calls
        .iter()
        .enumerate()
        .filter(|(_, call)| {
            call.group.group == group.group
                && call.group.partition == group.partition
                && call.group.epoch == group.epoch
        })
        .collect();
    if calls.is_empty() {
        return Err(conflict("group is not a participant in this transaction"));
    }
    Ok(calls)
}

async fn prepare(app: &App, reference: &Reference) -> Result<Vec<IndexedResult>, ApiError> {
    let budget = evaluator::config::settings()
        .map_err(unavailable)?
        .evaluation_timeout;
    prepare_with_budget(app, reference, budget).await
}

fn evaluation_deadline() -> ApiError {
    evaluation_error(anyhow::anyhow!(
        "EVALUATION_BUDGET: participant calls exceeded FLOWER_EVALUATION_TIMEOUT_MS"
    ))
}

async fn prepare_with_budget(
    app: &App,
    reference: &Reference,
    budget: Duration,
) -> Result<Vec<IndexedResult>, ApiError> {
    // Capture the participant leadership term BEFORE observing the coordinator
    // decision, under the writer lock. Consensus checks that term when applying
    // the preparation, fencing a former leader's stalled evaluator even if it
    // returns to leadership without an application revision change.
    let _input = app.admission.retain(
        admission::Class::User,
        admission::input_bytes(&json!(reference)),
    )?;
    let _writer = app.writer.lock().await;
    let admission = admission::acquire_retained(app, admission::Class::User).await?;
    let (state, term) = app
        .consensus
        .read_for_writer_with_term()
        .await
        .map_err(unavailable)?;
    closure::participant_open(&state, reference)?;
    let decision = status(app, reference).await?;
    let group = local_target(app)?;
    let calls = participant_calls(&decision, &group)?;
    if let Some(value) = state.data.get(&done_key(reference)) {
        let done: Done = decode(value)?;
        if done.transaction != *reference {
            return Err(reused());
        }
        return if done.phase == Phase::Commit {
            unpacked(
                done.results
                    .as_deref()
                    .ok_or_else(|| conflict("committed participant has no results"))?,
            )
        } else {
            Err(aborted(&decision))
        };
    }
    if let Some(value) = state.data.get(PARTICIPANT) {
        let prepared: Prepared = decode(value)?;
        if prepared.transaction != *reference {
            return Err(conflict("another cross-group transaction is prepared"));
        }
        if decision.phase == Phase::Abort {
            return Err(aborted(&decision));
        }
        return unpacked(&prepared.results);
    }
    if decision.phase != Phase::Preparing {
        return Err(conflict(
            "participant cannot prepare after the coordinator decision",
        ));
    }
    let deadline = tokio::time::Instant::now() + budget;
    let now = app.clock.sample(&state).map_err(unavailable)?;
    let mut staged = state.data.clone();
    let mut puts = BTreeMap::new();
    let mut deletes = BTreeSet::new();
    let mut results = Vec::new();
    for (index, call) in calls {
        if tokio::time::Instant::now() >= deadline {
            return Err(evaluation_deadline());
        }
        let mut view = state.clone();
        view.data = staged.clone();
        let method = http_method(&view, &call.method, None)?;
        if !matches!(method.kind, MethodKind::Query | MethodKind::Mutation) {
            return Err(invalid(anyhow::anyhow!(
                "transaction participants must call exposed queries or mutations; nested transactions are forbidden"
            )));
        }
        let mut invocation = json!({"name":method.name,"args":unpacked::<Value>(&call.args)?});
        if method.kind == MethodKind::Mutation {
            invocation["requestId"] = json!(reference.request_id);
        }
        let kind = method.kind.as_str();
        let authorization_input = json!({"name":call.method,"args":invocation["args"]});
        let principal = tokio::time::timeout_at(
            deadline,
            authorization::authorize_delegated_admitted(
                app,
                &view,
                &authorization_input,
                json!({"coordinator":reference.coordinator.label(),"principal":decision.principal}),
                &admission,
            ),
        )
        .await
        .map_err(|_| evaluation_deadline())??;
        let data = staged.clone();
        let evaluation = tokio::time::timeout_at(deadline, async {
            let permit = app
                .evaluations
                .clone()
                .acquire_owned()
                .await
                .map_err(|error| unavailable(error.into()))?;
            let admitted = admission.clone();
            tokio::task::spawn_blocking(move || {
                let _admitted = admitted;
                let _permit = permit;
                evaluator::invoke_as(data, invocation, kind, now, principal)
            })
            .await
            .map_err(|error| unavailable(error.into()))?
            .map_err(evaluation_error)
        })
        .await
        .map_err(|_| evaluation_deadline())??;
        for key in evaluation.deletes {
            staged.remove(&key);
            puts.remove(&key);
            deletes.insert(key);
        }
        for (key, value) in evaluation.puts {
            staged.insert(key.clone(), value.clone());
            deletes.remove(&key);
            puts.insert(key, value);
        }
        results.push(IndexedResult {
            index,
            value: packed(&evaluation.value)?,
        });
    }
    // Before preparing, verify both the prepared envelope and eventual commit
    // fit the configured Raft command budget. An oversized final patch must not
    // leave an unfinishable durable commit decision.
    let patch = Patch {
        puts,
        deletes: deletes.into_iter().collect(),
    };
    let prepared = Prepared {
        transaction: reference.clone(),
        patch: packed(&patch)?,
        results: packed(&results)?,
    };
    let mut final_puts = patch.puts.clone();
    final_puts.insert(
        done_key(reference),
        json!(Done {
            transaction: reference.clone(),
            phase: Phase::Commit,
            results: Some(prepared.results.clone())
        }),
    );
    let mut final_deletes = patch.deletes;
    final_deletes.push(PARTICIPANT.into());
    check_command_budget(
        app,
        &Commit {
            internal: true,
            request_id: String::new(),
            fingerprint: String::new(),
            expected_revision: 9_007_199_254_740_991,
            puts: final_puts,
            deletes: final_deletes,
            result: Value::Null,
        },
    )?;
    if tokio::time::Instant::now() >= deadline {
        return Err(evaluation_deadline());
    }
    // Same-leader abort finalization takes this writer lock. Across leadership
    // changes the captured term prevents an obsolete preparation appearing
    // after a new leader already acknowledged an empty abort.
    persist_in_term(
        app,
        &state,
        BTreeMap::from([(PARTICIPANT.into(), json!(prepared))]),
        vec![],
        Some(term),
    )
    .await?;
    Ok(results)
}

fn check_command_budget(app: &App, command: &Commit) -> Result<(), ApiError> {
    check_command_limit(command, app.consensus.limits().transaction_max_bytes)
}

fn check_command_limit(command: &Commit, max_bytes: usize) -> Result<(), ApiError> {
    let mut bounded = command.clone();
    closure::index_envelope(&mut bounded)?;
    bounded.puts.insert(
        crate::consensus::retention::RESERVED_BYTES.into(),
        json!(u64::MAX),
    );
    bounded
        .puts
        .insert(RESERVED_REVISIONS.into(), json!(MAX_REVISION));
    let encoded = serde_json::to_vec(&bounded).map_err(|error| invalid(error.into()))?;
    // Exercise the exact bounded per-record decoder used by consensus before
    // making a decision whose eventual receipt must be durably representable.
    serde_json::from_slice::<Commit>(&encoded).map_err(|error| invalid(error.into()))?;
    let bytes = encoded.len();
    if bytes > max_bytes {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "TRANSACTION_TOO_LARGE",
            "cross-group transaction exceeds FLOWER_TRANSACTION_MAX_BYTES".into(),
        ));
    }
    Ok(())
}

async fn finish(app: &App, reference: &Reference) -> Result<Done, ApiError> {
    // Check the durable rejection floor before contacting a possibly collected
    // coordinator. Holding the writer lock also orders finalization and closure.
    let _writer = app.writer.lock().await;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    closure::participant_open(&state, reference)?;
    let decision = status(app, reference).await?;
    participant_calls(&decision, &local_target(app)?)?;
    if decision.phase == Phase::Preparing {
        return Err(conflict("coordinator has not decided yet"));
    }
    let key = done_key(reference);
    if let Some(value) = state.data.get(&key) {
        let done: Done = decode(value)?;
        if done.transaction != *reference || done.phase != decision.phase {
            return Err(conflict(
                "participant completion conflicts with durable coordinator decision",
            ));
        }
        return Ok(done);
    }
    let prepared: Option<Prepared> = state.data.get(PARTICIPANT).map(decode).transpose()?;
    let prepared = prepared.filter(|prepared| prepared.transaction == *reference);
    let mut puts = BTreeMap::new();
    let mut deletes = Vec::new();
    let mut done = Done {
        transaction: reference.clone(),
        phase: decision.phase.clone(),
        results: None,
    };
    if decision.phase == Phase::Commit {
        let prepared = prepared
            .as_ref()
            .ok_or_else(|| conflict("commit decision has no durable prepared participant"))?;
        let patch: Patch = unpacked(&prepared.patch)?;
        puts = patch.puts;
        deletes = patch.deletes;
        done.results = Some(prepared.results.clone());
    }
    if prepared.is_some() {
        deletes.push(PARTICIPANT.into());
    }
    if prepared.is_none() && decision.phase == Phase::Abort {
        // No state to undo, and prepare must reread the immutable Abort while
        // holding this same lock. This also works for exhausted or undersized
        // groups that could never have durably prepared in the first place.
        return Ok(done);
    }
    // Participants that actually prepared retain a completion fence with the
    // same atomic commit that applies/discards their private patch.
    puts.insert(key, json!(done));
    persist(app, &state, puts, deletes).await?;
    Ok(done)
}

async fn decide(
    app: &App,
    reference: &Reference,
    phase: Phase,
    results: Option<String>,
    reason: Option<String>,
    failure: Option<Value>,
) -> Result<Coordinator, ApiError> {
    let _writer = app.writer.lock().await;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    let mut record = record(&state, reference)?;
    if record.phase != Phase::Preparing {
        return Ok(record);
    }
    record.results = if phase == Phase::Commit {
        results
    } else {
        None
    };
    record.phase = phase;
    record.reason = reason;
    record.failure = failure;
    if record.phase == Phase::Commit {
        crate::consensus::retention::validate_capacity_for(
            &state,
            &reference.request_id,
            &reference.fingerprint,
            &final_value(&record)?,
            record.receipt_reservation,
        )
        .map_err(retention::error)?;
    }
    if record.phase == Phase::Abort
        && check_command_budget(app, &metadata_command(reference, &record)).is_err()
    {
        record.reason = Some(ABORT_REASON.into());
    }
    persist(
        app,
        &state,
        BTreeMap::from([(coordinator_key(&reference.request_id), json!(record))]),
        vec![],
    )
    .await?;
    Ok(record)
}

fn final_value(record: &Coordinator) -> Result<Value, ApiError> {
    let results: Value = unpacked(
        record
            .results
            .as_deref()
            .ok_or_else(|| conflict("commit decision has no results"))?,
    )?;
    let mut result = json!({"results":results});
    if let Some(value) = &record.value {
        result["value"] = unpacked(value)?;
    }
    Ok(result)
}

async fn finish_coordinator(
    app: &App,
    reference: &Reference,
    decision: &Coordinator,
) -> Result<Value, ApiError> {
    if decision.phase == Phase::Preparing {
        return Err(conflict("coordinator has not decided"));
    }
    if !decision.complete {
        // Participants apply the decision independently, so a slow or
        // unavailable one must not keep the others locked.
        let responses =
            futures_util::future::join_all(participants(decision).into_iter().map(
                |group| async move { Box::pin(contact(app, &group, "finish", reference)).await },
            ))
            .await;
        let mut failure = None;
        for response in responses {
            match response.and_then(|value| decode::<Done>(&value)) {
                Ok(done) if done.transaction == *reference && done.phase == decision.phase => {}
                Ok(_) => {
                    failure = Some(conflict(
                        "participant did not confirm the coordinator decision",
                    ));
                }
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
    }
    let _writer = app.writer.lock().await;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    let mut current = record(&state, reference)?;
    if current.phase != decision.phase {
        return Err(conflict("durable coordinator decision changed"));
    }
    if current.phase == Phase::Commit {
        if let Some(receipt) = state.requests.get(&reference.request_id) {
            if receipt.fingerprint != reference.fingerprint {
                return Err(reused());
            }
            return Ok(
                json!({"revision":receipt.revision,"value":receipt.result,"duplicate":true}),
            );
        }
        current.complete = true;
        let result = final_value(&current)?;
        let mut command = Commit {
            internal: false,
            request_id: reference.request_id.clone(),
            fingerprint: reference.fingerprint.clone(),
            expected_revision: state.revision,
            puts: BTreeMap::from([(coordinator_key(&reference.request_id), json!(current))]),
            deletes: vec![],
            result,
        };
        ensure_commit_capacity(&state, &mut command)?;
        let committed = app.consensus.commit(command).await.map_err(unavailable)?;
        Ok(
            json!({"revision":committed.revision,"value":committed.result,"duplicate":committed.duplicate}),
        )
    } else {
        if !current.complete {
            current.complete = true;
            persist(
                app,
                &state,
                BTreeMap::from([(coordinator_key(&reference.request_id), json!(current))]),
                vec![],
            )
            .await?;
        }
        Err(aborted(&current))
    }
}

pub(super) async fn execute(app: Arc<App>, input: Value) -> Result<Value, ApiError> {
    evaluator::validate_invocation(&input, "transaction").map_err(invalid)?;
    let coordinator = local_target(&app)?;
    let _input = app
        .admission
        .retain(admission::Class::User, admission::input_bytes(&input))?;
    let request_id = input["requestId"]
        .as_str()
        .ok_or_else(|| invalid(anyhow::anyhow!("transaction requires requestId")))?
        .to_owned();
    let coordinator_lock = app.cross_group.coordinator_for(&request_id).await;
    let _active = coordinator_lock.lock().await;
    let mut reference = Reference {
        history: String::new(),
        sequence: 0,
        coordinator,
        request_id,
        fingerprint: String::new(),
    };
    let initial = {
        let _writer = app.writer.lock().await;
        let admission = admission::acquire_retained(&app, admission::Class::User).await?;
        let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
        let method = http_method(
            &state,
            input["name"].as_str().unwrap(),
            Some(MethodKind::Transaction),
        )?;
        let principal = authorization::authorize_admitted(&app, &state, &input, &admission).await?;
        reference.fingerprint = authorization::fingerprint(&input, false, &principal);
        crate::consensus::retention::require_scoped(&state, &reference.request_id)
            .and_then(|()| {
                crate::consensus::retention::validate_request_owner(
                    &state,
                    &reference.request_id,
                    &authorization::owner(&principal, false),
                )
            })
            .map_err(retention::error)?;
        super::staged_deployment::ensure_request_id_available(&state, &reference.request_id)?;
        if let Some(receipt) = state.requests.get(&reference.request_id) {
            if receipt.fingerprint != reference.fingerprint {
                return Err(reused());
            }
            return Ok(
                json!({"revision":receipt.revision,"value":receipt.result,"duplicate":true}),
            );
        }
        if state
            .data
            .contains_key(&coordinator_key(&reference.request_id))
        {
            let existing: Coordinator = decode(
                state
                    .data
                    .get(&coordinator_key(&reference.request_id))
                    .unwrap(),
            )?;
            if !existing.complete && !targets::is_local(&app, &existing.coordinator)? {
                return Err(conflict("pending coordinator placement changed"));
            }
            reference.history = existing.history.clone();
            reference.sequence = existing.sequence;
            reference.coordinator = existing.coordinator;
            record(&state, &reference)?
        } else {
            ensure_unlocked(&state)?;
            if input
                .get("expectedRevision")
                .and_then(Value::as_u64)
                .is_some_and(|expected| expected != state.revision)
            {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "REVISION_CONFLICT",
                    format!("current revision is {}", state.revision),
                ));
            }
            let now = app.clock.sample(&state).map_err(unavailable)?;
            let mut invocation = authorization::business_input(&input);
            invocation["name"] = json!(method.name);
            let data = state.data.clone();
            let permit = app
                .evaluations
                .clone()
                .acquire_owned()
                .await
                .map_err(|error| unavailable(error.into()))?;
            let admitted = admission.clone();
            let evaluation = tokio::task::spawn_blocking(move || {
                let _admitted = admitted;
                let _permit = permit;
                evaluator::invoke_at(data, invocation, "transaction", now)
            })
            .await
            .map_err(|error| unavailable(error.into()))?
            .map_err(evaluation_error)?;
            if !evaluation.puts.is_empty() || !evaluation.deletes.is_empty() {
                return Err(evaluation_error(anyhow::anyhow!(
                    "transaction planning must not mutate application state"
                )));
            }
            let mut record = plan(
                &reference.coordinator.group,
                evaluation.value,
                reference.fingerprint.clone(),
                &app.cross_group.groups,
            )?;
            closure::allocate(&state, &mut reference, &mut record)?;
            record.coordinator = reference.coordinator.clone();
            targets::resolve(&app, &mut record).await?;
            reference.coordinator = record.coordinator.clone();
            record.principal = principal;
            if crate::consensus::retention::status(&state)
                .map_err(retention::error)?
                .is_some()
            {
                record.receipt_reservation = app.consensus.limits().transaction_max_bytes as u64;
            }
            let mut abort = record.clone();
            abort.phase = Phase::Abort;
            abort.reason = Some(ABORT_REASON.into());
            // Both false/true completion encodings fit this worst-case wrapper.
            check_command_budget(&app, &metadata_command(&reference, &abort))?;
            persist(
                &app,
                &state,
                BTreeMap::from([(coordinator_key(&reference.request_id), json!(record))]),
                vec![],
            )
            .await?;
            record
        }
    };
    let decision = if initial.phase == Phase::Preparing {
        let mut ordered: Vec<Option<Value>> = vec![None; initial.calls.len()];
        let mut failure = None;
        let mut detail = None;
        // Prepare one target at a time in target order. Conflicts abort rather
        // than wait, so two conflicting transactions first meet at their first
        // shared target, and its winner cannot then be aborted by the loser.
        for group in participants(&initial) {
            let response = contact(&app, &group, "prepare", &reference).await;
            match response.and_then(|value| decode::<Vec<IndexedResult>>(&value)) {
                Ok(results) => {
                    for result in results {
                        if result.index >= initial.calls.len()
                            || initial.calls[result.index].group != group
                            || ordered[result.index].is_some()
                        {
                            failure = Some("participant returned invalid result indexes".into());
                            break;
                        }
                        match unpacked(&result.value) {
                            Ok(value) => ordered[result.index] = Some(value),
                            Err(error) => {
                                failure = Some(error.message);
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    detail = error.failure.as_deref().cloned();
                    failure = Some(error.message);
                }
            }
            if failure.is_some() {
                break;
            }
        }
        if failure.is_none() && ordered.iter().any(Option::is_none) {
            failure = Some("participant results are incomplete".into());
        }
        let results = if failure.is_none() {
            Some(packed(&ordered)?)
        } else {
            None
        };
        if failure.is_none() {
            // Reserve enough room for the final receipt and completed coordinator
            // metadata before crossing the irrevocable commit point.
            let mut candidate = initial.clone();
            candidate.phase = Phase::Commit;
            candidate.results = results.clone();
            candidate.complete = true;
            let result = final_value(&candidate)?;
            let command = Commit {
                internal: false,
                request_id: reference.request_id.clone(),
                fingerprint: reference.fingerprint.clone(),
                expected_revision: 9_007_199_254_740_991,
                puts: BTreeMap::from([(coordinator_key(&reference.request_id), json!(candidate))]),
                deletes: vec![],
                result,
            };
            if let Err(error) = check_command_budget(&app, &command) {
                failure = Some(error.message);
            }
        }
        let phase = if failure.is_none() {
            Phase::Commit
        } else {
            Phase::Abort
        };
        decide(&app, &reference, phase, results, failure, detail).await?
    } else {
        initial
    };
    finish_coordinator(&app, &reference, &decision).await
}

pub(super) async fn run(weak: Weak<App>) {
    loop {
        let Some(app) = weak.upgrade() else {
            return;
        };
        if app.cross_group.group.is_none() {
            return;
        }
        if app.consensus.metrics().state == openraft::ServerState::Leader
            && let Err(error) = recover(&app).await
        {
            tracing::debug!(message = %error.message, "cross-group transaction recovery will retry");
        }
        let cadence = app
            .consensus
            .limits()
            .read_timeout
            .min(Duration::from_secs(1));
        drop(app);
        tokio::time::sleep(cadence).await;
    }
}

async fn recover(app: &App) -> Result<(), ApiError> {
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    // Finish participants first: this may release a lock belonging to a
    // coordinator elsewhere even when one of our own decisions is unavailable.
    if let Some(value) = state.data.get(PARTICIPANT) {
        let prepared: Prepared = decode(value)?;
        let _ = finish(app, &prepared.transaction).await;
    }
    for item in closure::active_records(&state) {
        let (key, current) = item?;
        let request_id = key
            .strip_prefix(COORDINATOR)
            .ok_or_else(|| conflict("invalid active transaction key"))?;
        if current.complete {
            continue;
        }
        let reference = Reference {
            history: current.history.clone(),
            sequence: current.sequence,
            coordinator: local_target(app)?,
            request_id: request_id.into(),
            fingerprint: current.fingerprint.clone(),
        };
        let coordinator_lock = app.cross_group.coordinator_for(request_id).await;
        let Ok(_active) = coordinator_lock.try_lock() else {
            continue;
        };
        // A request may have completed between the scan and lock acquisition.
        let fresh = app.consensus.read_for_writer().await.map_err(unavailable)?;
        let current = record(&fresh, &reference)?;
        if current.complete {
            continue;
        }
        let current = if current.phase == Phase::Preparing {
            decide(
                app,
                &reference,
                Phase::Abort,
                None,
                Some("coordinator recovered an unfinished preparation".into()),
                None,
            )
            .await?
        } else {
            current
        };
        let _ = finish_coordinator(app, &reference, &current).await;
    }
    closure::recover(app).await
}

#[cfg(test)]
mod tests;

/// Operator-only distributed transaction history maintenance.
pub(super) async fn administer(app: &App, input: Value) -> Result<Value, ApiError> {
    let _input = app
        .admission
        .retain(admission::Class::Control, admission::input_bytes(&input))?;
    closure::operate(app, decode(&input)?).await
}

pub(super) fn validate_admin(input: &Value) -> Result<(), ApiError> {
    decode::<closure::Operation>(input).map(|_| ())
}
