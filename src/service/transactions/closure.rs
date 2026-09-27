//! Explicit distributed closure. A coordinator forgets decisions only after
//! every participant durably rejects the closed history prefix. Time is never
//! evidence of closure, and public retry admission remains a separate contract.
use super::*;

pub(super) const HISTORY: &str = "transaction:history";
pub(super) const ACTIVE: &str = "transaction:active:";
const SEQUENCE: &str = "transaction:sequence:";
const FLOORS: &str = "transaction:closed:";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct History {
    id: String,
    next_sequence: u64,
    closed_through: u64,
    #[serde(default)]
    closing: Option<Intent>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Intent {
    through: u64,
    participants: Vec<Target>,
    acknowledged: Vec<Target>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Boundary {
    coordinator: Target,
    history: String,
    through: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Floor {
    coordinator: Target,
    history: String,
    through: u64,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Operation {
    Status,
    Close {
        through: Option<u64>,
        #[serde(rename = "maxBytes")]
        max_bytes: Option<usize>,
    },
    Collect {
        #[serde(rename = "maxBytes")]
        max_bytes: Option<usize>,
    },
}
fn history(state: &Snapshot) -> Result<Option<History>, ApiError> {
    let value: Option<History> = state.data.get(HISTORY).map(decode).transpose()?;
    if let Some(history) = &value
        && (history.id.len() != 32
            || !history
                .id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || history.next_sequence == 0
            || history.next_sequence > MAX_REVISION
            || history.closed_through >= history.next_sequence)
    {
        return Err(conflict("invalid transaction history metadata"));
    }
    Ok(value)
}
fn index(prefix: &str, sequence: u64) -> String {
    format!("{prefix}{sequence:016x}")
}
fn floor_key(history: &str) -> String {
    format!("{FLOORS}{history}")
}
fn same_logical(left: &Target, right: &Target) -> bool {
    match (&left.partition, &right.partition) {
        (Some(left), Some(right)) => left == right,
        (None, None) => left.group == right.group,
        _ => false,
    }
}
fn closed() -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "TRANSACTION_CLOSED",
        "transaction history prefix is permanently closed".into(),
    )
}

pub(super) fn allocate(
    state: &Snapshot,
    reference: &mut Reference,
    record: &mut Coordinator,
) -> Result<(), ApiError> {
    let current = match history(state)? {
        Some(history) => history,
        None => {
            let mut bytes = [0u8; 16];
            getrandom::fill(&mut bytes).map_err(|error| {
                unavailable(anyhow::anyhow!("transaction history entropy: {error}"))
            })?;
            History {
                id: bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
                next_sequence: 1,
                closed_through: 0,
                closing: None,
            }
        }
    };
    if current.next_sequence >= MAX_REVISION {
        return Err(exhausted());
    }
    reference.history = current.id.clone();
    reference.sequence = current.next_sequence;
    record.history = current.id;
    record.sequence = current.next_sequence;
    Ok(())
}

/// Also used by command-size preflight: indexes are part of the real envelope.
pub(super) fn index_envelope(command: &mut Commit) -> Result<(), ApiError> {
    let records: Vec<_> = command
        .puts
        .iter()
        .filter(|(key, _)| key.starts_with(COORDINATOR))
        .map(|(key, value)| decode::<Coordinator>(value).map(|record| (key.clone(), record)))
        .collect::<Result<_, _>>()?;
    for (key, record) in records {
        if record.sequence == 0 || record.history.is_empty() {
            return Err(conflict("transaction is missing its history sequence"));
        }
        let active = index(ACTIVE, record.sequence);
        command
            .puts
            .insert(index(SEQUENCE, record.sequence), json!(key));
        if record.complete {
            command.puts.remove(&active);
            if !command.deletes.contains(&active) {
                command.deletes.push(active);
            }
        } else {
            command.puts.insert(active, json!(key));
        }
    }
    Ok(())
}
pub(super) fn index_command(state: &Snapshot, command: &mut Commit) -> Result<(), ApiError> {
    let records: Vec<_> = command
        .puts
        .iter()
        .filter(|(key, _)| key.starts_with(COORDINATOR))
        .map(|(key, value)| decode::<Coordinator>(value).map(|record| (key.clone(), record)))
        .collect::<Result<_, _>>()?;
    let mut current = history(state)?;
    for (key, record) in records {
        if state.data.contains_key(&key) {
            continue;
        }
        let item = current.get_or_insert_with(|| History {
            id: record.history.clone(),
            next_sequence: 1,
            closed_through: 0,
            closing: None,
        });
        if record.history != item.id
            || record.sequence != item.next_sequence
            || record.sequence >= MAX_REVISION
        {
            return Err(conflict("transaction history allocation conflict"));
        }
        item.next_sequence += 1;
        command.puts.insert(HISTORY.into(), json!(item));
    }
    index_envelope(command)
}
pub(super) fn reserved_revisions(state: &Snapshot) -> Result<u64, ApiError> {
    let Some(history) = history(state)? else {
        return Ok(0);
    };
    let Some(intent) = history.closing else {
        return Ok(0);
    };
    if intent.through <= history.closed_through
        || intent.through >= history.next_sequence
        || intent.acknowledged.iter().any(|ack| {
            !intent
                .participants
                .iter()
                .any(|target| same_logical(target, ack))
        })
    {
        return Err(conflict("invalid transaction closure intent"));
    }
    Ok((intent
        .participants
        .len()
        .saturating_sub(intent.acknowledged.len()) as u64)
        .max(1))
}
pub(super) fn coordinator_open(state: &Snapshot, reference: &Reference) -> Result<(), ApiError> {
    let current =
        history(state)?.ok_or_else(|| conflict("coordinator has no transaction history"))?;
    if current.id != reference.history {
        return Err(conflict("transaction history identity mismatch"));
    }
    if reference.sequence == 0 || reference.sequence <= current.closed_through {
        return Err(closed());
    }
    Ok(())
}
pub(super) fn participant_open(state: &Snapshot, reference: &Reference) -> Result<(), ApiError> {
    if reference.sequence == 0 || reference.history.len() != 32 {
        return Err(conflict("invalid transaction history reference"));
    }
    if let Some(value) = state.data.get(&floor_key(&reference.history)) {
        let floor: Floor = decode(value)?;
        if floor.history != reference.history
            || !same_logical(&floor.coordinator, &reference.coordinator)
        {
            return Err(conflict("transaction history floor identity mismatch"));
        }
        if reference.sequence <= floor.through {
            return Err(closed());
        }
    }
    Ok(())
}

pub(super) fn active_records(
    state: &Snapshot,
) -> impl Iterator<Item = Result<(&String, Coordinator), ApiError>> {
    state
        .data
        .range::<(std::ops::Bound<&str>, std::ops::Bound<&str>)>((
            std::ops::Bound::Included(ACTIVE),
            std::ops::Bound::Unbounded,
        ))
        .take_while(|(key, _)| key.starts_with(ACTIVE))
        .map(|(_, value)| {
            let key = value
                .as_str()
                .ok_or_else(|| conflict("invalid active transaction index"))?;
            let (stored, value) = state.data.get_key_value(key).ok_or_else(|| {
                conflict("active transaction index references missing coordinator")
            })?;
            Ok((stored, decode(value)?))
        })
}

fn inadmissible(state: &Snapshot, id: &str) -> bool {
    crate::consensus::retention::require_scoped(state, id)
        .and_then(|()| crate::consensus::retention::validate_request(state, id))
        .err()
        .is_some_and(|error| {
            let code = error.to_string();
            [
                "HISTORY_MISMATCH:",
                "RETRY_WINDOW_EXPIRED:",
                "ALREADY_ACKNOWLEDGED:",
                "RETRY_SESSION_CLOSED:",
                "REQUEST_ID_SCOPE_REQUIRED:",
            ]
            .iter()
            .any(|prefix| code.starts_with(prefix))
        })
}
fn target_key(target: &Target) -> String {
    serde_json::to_string(&(
        &target.partition,
        if target.partition.is_none() {
            Some(&target.group)
        } else {
            None
        },
    ))
    .expect("target identity")
}
fn cost(key: &str, value: &Value) -> usize {
    key.len()
        .saturating_add(crate::consensus::encoded_json_len(value).unwrap_or(usize::MAX))
        .saturating_add(128)
}
fn summary(state: &Snapshot, blocked: Option<String>) -> Result<Value, ApiError> {
    Ok(match history(state)? {
        Some(current) => {
            json!({"history":current.id,"nextSequence":current.next_sequence,"closedThrough":current.closed_through,
            "pending":current.closing,"blockedReason":blocked})
        }
        None => {
            json!({"history":null,"nextSequence":1,"closedThrough":0,"pending":null,"blockedReason":blocked})
        }
    })
}
async fn current_target(app: &App, target: &Target) -> Result<Target, ApiError> {
    targets::current(app, target).await
}
async fn contact_closure(
    app: &App,
    target: &Target,
    action: &str,
    proof: &Boundary,
) -> Result<Value, ApiError> {
    let target = current_target(app, target).await?;
    if targets::is_local(app, &target)? {
        return match action {
            "closure-status" => Ok(json!(local_proof(app, proof).await?)),
            "closure-ack" => acknowledge(app, proof).await,
            _ => Err(conflict("unknown closure action")),
        };
    }
    if target.partition.is_some() {
        return targets::contact_partition_value(app, &target, action, json!(proof)).await;
    }
    super::remote_contact(
        app,
        &target,
        action,
        json!({"group":target.group,"closure":proof}),
    )
    .await
}

/// A proof is read at quorum; a caller cannot supply the coordinator decision.
async fn local_proof(app: &App, proof: &Boundary) -> Result<History, ApiError> {
    if !same_logical(&local_target(app)?, &proof.coordinator) {
        return Err(conflict("closure status reached another logical database"));
    }
    let state = app.consensus.read_query().await.map_err(unavailable)?;
    let current =
        history(&state)?.ok_or_else(|| conflict("coordinator has no transaction history"))?;
    if current.id != proof.history {
        return Err(conflict("closure history identity mismatch"));
    }
    if proof.through == 0
        || (proof.through > current.closed_through
            && current
                .closing
                .as_ref()
                .is_none_or(|intent| intent.through != proof.through))
    {
        return Err(conflict(
            "coordinator has no durable closure intent for this prefix",
        ));
    }
    Ok(current)
}
async fn acknowledge(app: &App, proof: &Boundary) -> Result<Value, ApiError> {
    let _writer = app.writer.lock().await;
    let (state, term) = app
        .consensus
        .read_for_writer_with_term()
        .await
        .map_err(unavailable)?;
    let current: History = decode(
        &Box::pin(contact_closure(
            app,
            &proof.coordinator,
            "closure-status",
            proof,
        ))
        .await?,
    )?;
    let local = local_target(app)?;
    if proof.through > current.closed_through
        && !current.closing.as_ref().is_some_and(|intent| {
            intent
                .participants
                .iter()
                .any(|target| same_logical(target, &local))
        })
    {
        return Err(conflict("local database is not part of the closure intent"));
    }
    if let Some(value) = state.data.get(PARTICIPANT) {
        let prepared: Prepared = decode(value)?;
        if prepared.transaction.history == proof.history
            && prepared.transaction.sequence <= proof.through
        {
            return Err(conflict(
                "a prepared participant prevents closing this prefix",
            ));
        }
    }
    let key = floor_key(&proof.history);
    if let Some(value) = state.data.get(&key) {
        let floor: Floor = decode(value)?;
        if !same_logical(&floor.coordinator, &proof.coordinator) || floor.history != proof.history {
            return Err(conflict("closure floor identity mismatch"));
        }
        if floor.through >= proof.through {
            return Ok(json!({"history":proof.history,"through":floor.through}));
        }
    }
    let floor = Floor {
        coordinator: proof.coordinator.clone(),
        history: proof.history.clone(),
        through: proof.through,
    };
    persist_in_term(
        app,
        &state,
        BTreeMap::from([(key, json!(floor))]),
        vec![],
        Some(term),
    )
    .await?;
    Ok(json!({"history":proof.history,"through":proof.through}))
}

async fn begin(
    app: &App,
    requested: Option<u64>,
    budget: usize,
) -> Result<Option<String>, ApiError> {
    let _writer = app.writer.lock().await;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    let Some(mut current) = history(&state)? else {
        return Ok(Some("no transaction history".into()));
    };
    if current.closing.is_some() {
        return Ok(None);
    }
    let requested = requested.unwrap_or(current.next_sequence - 1);
    if requested >= current.next_sequence {
        return Err(conflict("close through exceeds the allocated sequence"));
    }
    if requested <= current.closed_through {
        return Ok(None);
    }
    let mut through = current.closed_through;
    let mut used = 0usize;
    let mut participants = BTreeMap::new();
    let mut blocked = None;
    for sequence in current.closed_through + 1..=requested {
        let key = index(SEQUENCE, sequence);
        let id = state
            .data
            .get(&key)
            .and_then(Value::as_str)
            .ok_or_else(|| conflict("missing transaction sequence index"))?;
        let value = state
            .data
            .get(id)
            .ok_or_else(|| conflict("transaction sequence references missing decision"))?;
        let amount = cost(id, value).saturating_add(key.len());
        if used.saturating_add(amount) > budget {
            blocked = Some("work budget exhausted before the requested prefix".into());
            break;
        }
        used += amount;
        let record: Coordinator = decode(value)?;
        if record.history != current.id || record.sequence != sequence {
            return Err(conflict("transaction sequence identity mismatch"));
        }
        if !record.complete {
            blocked = Some(format!("sequence {sequence} is not complete"));
            break;
        }
        let request = id
            .strip_prefix(COORDINATOR)
            .ok_or_else(|| conflict("invalid sequence index key"))?;
        if record.phase == Phase::Abort && !inadmissible(&state, request) {
            blocked = Some(format!(
                "sequence {sequence} aborted but its public request ID remains admissible; retire or acknowledge it first"
            ));
            break;
        }
        if record.phase == Phase::Commit
            && !inadmissible(&state, request)
            && !state
                .requests
                .get(request)
                .is_some_and(|receipt| receipt.fingerprint == record.fingerprint)
        {
            return Err(conflict(
                "committed transaction has neither a retry receipt nor a retry admission fence",
            ));
        }
        for target in super::participants(&record) {
            participants.insert(target_key(&target), target);
        }
        through = sequence;
    }
    if through == current.closed_through {
        return Ok(blocked);
    }
    let targets: Vec<_> = participants.into_values().collect();
    current.closing = Some(Intent {
        through,
        participants: targets.clone(),
        acknowledged: targets,
    });
    // Every future ACK must fit before installing an intent. Address/name sizes
    // are governed by the actual command budget, never an independent cap.
    let command = Commit {
        internal: true,
        request_id: String::new(),
        fingerprint: String::new(),
        expected_revision: MAX_REVISION,
        puts: BTreeMap::from([(HISTORY.into(), json!(current))]),
        deletes: vec![],
        result: Value::Null,
    };
    check_command_budget(app, &command)?;
    current.closing.as_mut().unwrap().acknowledged.clear();
    persist(
        app,
        &state,
        BTreeMap::from([(HISTORY.into(), json!(current))]),
        vec![],
    )
    .await?;
    Ok(blocked)
}
async fn advance(app: &App, budget: usize) -> Result<Option<String>, ApiError> {
    let snapshot = app.consensus.read_for_writer().await.map_err(unavailable)?;
    let Some(current) = history(&snapshot)? else {
        return Ok(None);
    };
    let Some(intent) = current.closing.clone() else {
        return Ok(None);
    };
    if intent.participants.is_empty() {
        let _writer = app.writer.lock().await;
        let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
        let mut fresh = history(&state)?.ok_or_else(|| conflict("closure history disappeared"))?;
        if fresh.id != current.id {
            return Err(conflict("closure history changed"));
        }
        if fresh.closed_through >= intent.through {
            return Ok(None);
        }
        if !fresh.closing.as_ref().is_some_and(|active| {
            active.through == intent.through && active.participants.is_empty()
        }) {
            return Err(conflict("closure intent changed"));
        }
        fresh.closed_through = intent.through;
        fresh.closing = None;
        persist(
            app,
            &state,
            BTreeMap::from([(HISTORY.into(), json!(fresh))]),
            vec![],
        )
        .await?;
        return Ok(None);
    }
    let proof = Boundary {
        coordinator: local_target(app)?,
        history: current.id,
        through: intent.through,
    };
    let mut used = 0usize;
    for target in intent.participants {
        if intent
            .acknowledged
            .iter()
            .any(|done| same_logical(done, &target))
        {
            continue;
        }
        let amount = cost("participant", &json!(target));
        if used.saturating_add(amount) > budget {
            return Ok(Some(
                "work budget exhausted while confirming participant floors".into(),
            ));
        }
        used += amount;
        let ack = match Box::pin(contact_closure(app, &target, "closure-ack", &proof)).await {
            Ok(ack) => ack,
            Err(error) => {
                return Ok(Some(format!(
                    "participant {}: {}",
                    target.label(),
                    error.message
                )));
            }
        };
        if ack["history"] != proof.history
            || ack["through"]
                .as_u64()
                .is_none_or(|through| through < proof.through)
        {
            return Err(conflict(
                "participant did not confirm the requested closure floor",
            ));
        }
        let _writer = app.writer.lock().await;
        let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
        let mut fresh = history(&state)?.ok_or_else(|| conflict("closure history disappeared"))?;
        if fresh.id != proof.history {
            return Err(conflict("closure history changed"));
        }
        if fresh.closed_through >= proof.through {
            return Ok(None);
        }
        let active = fresh
            .closing
            .as_mut()
            .ok_or_else(|| conflict("closure intent disappeared"))?;
        if active.through != proof.through {
            return Err(conflict("closure intent changed"));
        }
        if !active
            .acknowledged
            .iter()
            .any(|done| same_logical(done, &target))
        {
            active.acknowledged.push(target);
        }
        if active.acknowledged.len() == active.participants.len() {
            fresh.closed_through = active.through;
            fresh.closing = None;
        }
        persist(
            app,
            &state,
            BTreeMap::from([(HISTORY.into(), json!(fresh))]),
            vec![],
        )
        .await?;
    }
    Ok(None)
}

const GC_CURSOR: &str = "transaction:collection";
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Cursor {
    #[serde(default)]
    completed: bool,
    after: Option<String>,
}
fn collection_fits(app: &App, cursor: &Cursor, deletes: &[String]) -> bool {
    check_command_budget(
        app,
        &Commit {
            internal: true,
            request_id: String::new(),
            fingerprint: String::new(),
            expected_revision: MAX_REVISION,
            puts: BTreeMap::from([(GC_CURSOR.into(), json!(cursor))]),
            deletes: deletes.to_vec(),
            result: Value::Null,
        },
    )
    .is_ok()
}
// Exact JSON envelope bytes with an empty delete array. Per-record admission
// adds only the newly serialized strings/commas; never rescan the growing batch.
fn collection_base_bytes(cursor: &Cursor) -> Result<usize, ApiError> {
    let command = Commit {
        internal: true,
        request_id: String::new(),
        fingerprint: String::new(),
        expected_revision: MAX_REVISION,
        puts: BTreeMap::from([
            (GC_CURSOR.into(), json!(cursor)),
            (RESERVED_REVISIONS.into(), json!(MAX_REVISION)),
            (
                crate::consensus::retention::RESERVED_BYTES.into(),
                json!(u64::MAX),
            ),
        ]),
        deletes: vec![],
        result: Value::Null,
    };
    serde_json::to_vec(&command)
        .map(|bytes| bytes.len())
        .map_err(|error| invalid(error.into()))
}
async fn collect(app: &App, budget: usize) -> Result<(usize, Option<String>), ApiError> {
    let _writer = app.writer.lock().await;
    let state = app.consensus.read_for_writer().await.map_err(unavailable)?;
    let current = history(&state)?;
    let mut cursor: Cursor = state
        .data
        .get(GC_CURSOR)
        .map(decode)
        .transpose()?
        .unwrap_or_default();
    let mut deletes = Vec::new();
    let mut deletion_bytes = 0usize;
    let mut used = 0usize;
    let mut blocked = None;
    // Persist the actual scan position, including live/unclosed records. Each
    // call visits at most one cycle; empty histories cannot starve later ones.
    for _ in 0..2 {
        let prefix = if cursor.completed {
            COMPLETED
        } else {
            SEQUENCE
        };
        if cursor
            .after
            .as_ref()
            .is_some_and(|key| !key.starts_with(prefix))
        {
            return Err(conflict("invalid transaction collection cursor"));
        }
        let bound = cursor
            .after
            .as_deref()
            .map_or(std::ops::Bound::Included(prefix), std::ops::Bound::Excluded);
        let mut exhausted_range = true;
        for (key, value) in state
            .data
            .range::<(std::ops::Bound<&str>, std::ops::Bound<&str>)>((
                bound,
                std::ops::Bound::Unbounded,
            ))
            .take_while(|(key, _)| key.starts_with(prefix))
        {
            let amount = cost(key, value);
            if used.saturating_add(amount) > budget {
                blocked = Some("collection work budget exhausted; resume with collect or increase maxBytes for a single large record".into());
                exhausted_range = false;
                break;
            }
            used += amount;
            let mut additions = Vec::new();
            if cursor.completed {
                let reference: Reference = decode(
                    value
                        .get("transaction")
                        .ok_or_else(|| conflict("invalid participant completion"))?,
                )?;
                if key != &done_key(&reference) {
                    return Err(conflict("participant completion identity mismatch"));
                }
                if let Some(value) = state.data.get(&floor_key(&reference.history)) {
                    let floor: Floor = decode(value)?;
                    if floor.history != reference.history
                        || !same_logical(&floor.coordinator, &reference.coordinator)
                    {
                        return Err(conflict("participant closure floor identity mismatch"));
                    }
                    if reference.sequence <= floor.through {
                        additions.push(key.clone());
                    }
                }
            } else {
                let sequence = u64::from_str_radix(&key[SEQUENCE.len()..], 16)
                    .map_err(|error| invalid(error.into()))?;
                if current
                    .as_ref()
                    .is_some_and(|history| sequence <= history.closed_through)
                {
                    let id = value
                        .as_str()
                        .ok_or_else(|| conflict("invalid transaction sequence index"))?;
                    let record = state
                        .data
                        .get(id)
                        .ok_or_else(|| conflict("closed sequence references missing record"))?;
                    if record["history"] != current.as_ref().unwrap().id
                        || record["sequence"] != sequence
                    {
                        return Err(conflict("closed transaction identity mismatch"));
                    }
                    additions.push(id.to_owned());
                    additions.push(key.clone());
                }
            }
            let next = Cursor {
                completed: cursor.completed,
                after: Some(key.clone()),
            };
            let mut candidate_bytes = deletion_bytes;
            for (index, key) in additions.iter().enumerate() {
                candidate_bytes = candidate_bytes
                    .saturating_add(
                        serde_json::to_string(key)
                            .map_err(|error| invalid(error.into()))?
                            .len(),
                    )
                    .saturating_add(usize::from(!deletes.is_empty() || index > 0));
            }
            if collection_base_bytes(&next)?.saturating_add(candidate_bytes)
                > app.consensus.limits().transaction_max_bytes
            {
                blocked = Some("collection command budget exhausted".into());
                exhausted_range = false;
                break;
            }
            deletes.extend(additions);
            deletion_bytes = candidate_bytes;
            cursor = next;
        }
        if !exhausted_range {
            break;
        }
        cursor = Cursor {
            completed: !cursor.completed,
            after: None,
        };
    }
    let count = deletes.len();
    // Persist progress even when inspected records were not yet closed.
    if !collection_fits(app, &cursor, &deletes) {
        return Err(conflict(
            "transaction collection cursor exceeds command budget",
        ));
    }
    let next = json!(cursor);
    if count > 0 || state.data.get(GC_CURSOR) != Some(&next) {
        persist(
            app,
            &state,
            BTreeMap::from([(GC_CURSOR.into(), next)]),
            deletes,
        )
        .await?;
    }
    Ok((count, blocked))
}
pub(super) async fn operate(app: &App, operation: Operation) -> Result<Value, ApiError> {
    let budget = match &operation {
        Operation::Status => None,
        Operation::Close { max_bytes, .. } | Operation::Collect { max_bytes } => *max_bytes,
    }
    .unwrap_or(app.consensus.limits().transaction_max_bytes);
    if budget == 0 {
        return Err(invalid(anyhow::anyhow!("maxBytes must be positive")));
    }
    let mut deleted = 0;
    let blocked = match operation {
        Operation::Status => None,
        Operation::Close { through, .. } => {
            let blocked = begin(app, through, budget).await?;
            advance(app, budget).await?.or(blocked)
        }
        Operation::Collect { .. } => {
            let (result, blocked) = collect(app, budget).await?;
            deleted = result;
            blocked
        }
    };
    let state = app.consensus.read_query().await.map_err(unavailable)?;
    let mut value = summary(&state, blocked)?;
    value["deletedRecords"] = json!(deleted);
    Ok(json!({"revision":state.revision,"value":value}))
}
pub(super) async fn recover(app: &App) -> Result<(), ApiError> {
    let _ = advance(app, app.consensus.limits().transaction_max_bytes).await?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosureRequest {
    group: String,
    closure: Boundary,
}
async fn authenticated(
    app: &Arc<App>,
    request: axum::extract::Request,
) -> Result<Boundary, ApiError> {
    use axum::extract::FromRequest;
    authenticate_headers(app, request.headers())?;
    let Json(request) = Json::<ClosureRequest>::from_request(request, app)
        .await
        .map_err(|error| ApiError::new(error.status(), "INPUT_INVALID", error.body_text()))?;
    if request.group != app.cross_group.name()? {
        return Err(conflict("closure RPC reached the wrong group"));
    }
    Ok(request.closure)
}
async fn status_route(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let proof = authenticated(&app, request).await?;
    Ok(rpc_response(
        app.cross_group.name()?,
        local_proof(&app, &proof).await?,
    ))
}
async fn ack_route(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let proof = authenticated(&app, request).await?;
    Ok(rpc_response(
        app.cross_group.name()?,
        acknowledge(&app, &proof).await?,
    ))
}
pub(super) fn router() -> Router<Arc<App>> {
    Router::new()
        .route("/raft/transactions/closure-status", post(status_route))
        .route("/raft/transactions/closure-ack", post(ack_route))
}
pub(super) async fn partition_rpc(
    app: &App,
    action: &str,
    input: Value,
) -> Result<Value, ApiError> {
    let proof: Boundary = decode(&input)?;
    match action {
        "tx-closure-status" => Ok(json!(local_proof(app, &proof).await?)),
        "tx-closure-ack" => acknowledge(app, &proof).await,
        _ => Err(conflict("unknown closure RPC")),
    }
}
