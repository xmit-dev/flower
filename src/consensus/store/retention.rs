//! Retention transitions share the exact durable transaction and publication
//! path used for evaluated patches. No clock or secret is consulted here.
use super::*;
use crate::consensus::retention::{self as policy, Action, Command, Session, State};
use anyhow::{Context, ensure};

fn value<'a>(state: &'a Snapshot, delta: &'a ApplicationDelta, key: &str) -> Option<&'a Value> {
    delta
        .data
        .get(key)
        .map_or_else(|| state.data.get(key), Option::as_ref)
}

pub(super) fn prepare_retention(
    state: &Snapshot,
    delta: &ApplicationDelta,
    puts: &BTreeMap<String, Value>,
    deletes: &[String],
) -> anyhow::Result<Option<State>> {
    ensure!(
        !puts.keys().any(|key| policy::protected_key(key))
            && !deletes.iter().any(|key| policy::protected_key(key)),
        "RETENTION_METADATA_PROTECTED: retention state can only change through its native protocol"
    );
    let policy = policy::decode(value(state, delta, policy::KEY))?;
    if let Some(policy) = &policy {
        // Internal transaction preparation must not over-reserve capacity.
        // Releasing a reservation and adding its final receipt is checked again
        // with the result's exact encoded size before modifying any state.
        let before = reserved_after(state, delta, &BTreeMap::new(), &[])?;
        let after = reserved_after(state, delta, puts, deletes)?;
        if after > before {
            policy.capacity(policy.receipt_bytes, after)?;
        }
    }
    Ok(policy)
}

pub(super) fn reserved_after(
    state: &Snapshot,
    delta: &ApplicationDelta,
    puts: &BTreeMap<String, Value>,
    deletes: &[String],
) -> anyhow::Result<u64> {
    let next = puts.get(policy::RESERVED_BYTES).or_else(|| {
        if deletes.iter().any(|key| key == policy::RESERVED_BYTES) {
            None
        } else {
            value(state, delta, policy::RESERVED_BYTES)
        }
    });
    policy::reserved_bytes(next)
}

pub(super) fn admit_request(
    snapshot: &Snapshot,
    delta: &ApplicationDelta,
    state: Option<&State>,
    id: &str,
) -> anyhow::Result<()> {
    policy::validate_with(state, id, |key| value(snapshot, delta, key))
}

pub(super) fn publish_retention(delta: &mut ApplicationDelta, state: Option<State>) {
    if let Some(state) = state {
        delta.data.insert(
            policy::KEY.into(),
            Some(serde_json::to_value(state).expect("retention metadata serializes")),
        );
    }
}

fn project(state: &Snapshot, delta: &ApplicationDelta) -> Snapshot {
    let mut current = state.clone();
    current.revision = delta.revision;
    for (key, value) in &delta.data {
        if let Some(value) = value {
            current.data.insert(key.clone(), value.clone());
        } else {
            current.data.remove(key);
        }
    }
    for key in &delta.deleted_requests {
        current.requests.remove(key);
    }
    current.requests.extend(
        delta
            .requests
            .iter()
            .map(|(key, receipt)| (key.clone(), receipt.clone())),
    );
    current
}

fn no_active_transactions(state: &Snapshot) -> anyhow::Result<()> {
    ensure!(
        !state.data.contains_key("transaction:participant")
            && !state
                .data
                .entries::<(std::ops::Bound<&str>, std::ops::Bound<&str>)>((
                    std::ops::Bound::Included("transaction:coordinator:"),
                    std::ops::Bound::Unbounded
                ))
                .take_while(|(key, _)| key.starts_with("transaction:coordinator:"))
                .any(|(_, value)| value.get("complete").and_then(Value::as_bool) != Some(true)),
        "RETENTION_TRANSACTION_ACTIVE: complete outstanding distributed transactions before changing retry admission"
    );
    Ok(())
}

struct Change {
    state: State,
    receipts: Vec<String>,
    metadata: BTreeMap<String, Option<Value>>,
    session: Option<Session>,
}

fn remove_receipt(
    current: &mut State,
    id: &str,
    receipt: &Receipt,
    removed: &mut Vec<String>,
) -> anyhow::Result<()> {
    current.receipt_bytes = current
        .receipt_bytes
        .checked_sub(policy::receipt_bytes(id, receipt)?)
        .context("retention receipt byte accounting underflow")?;
    current.receipt_count = current
        .receipt_count
        .checked_sub(1)
        .context("retention receipt accounting underflow")?;
    removed.push(id.into());
    Ok(())
}

fn store_session(
    current: &mut State,
    old: Option<&Session>,
    next: &Session,
    metadata: &mut BTreeMap<String, Option<Value>>,
) -> anyhow::Result<()> {
    if let Some(old) = old {
        current.session_bytes = current
            .session_bytes
            .checked_sub(policy::session_bytes(old)?)
            .context("session byte accounting underflow")?;
    } else {
        current.session_count = current
            .session_count
            .checked_add(1)
            .context("session counter exhausted")?;
    }
    current.session_bytes = current
        .session_bytes
        .checked_add(policy::session_bytes(next)?)
        .context("session byte counter exhausted")?;
    metadata.insert(
        policy::session_key(&next.id),
        Some(serde_json::to_value(next)?),
    );
    Ok(())
}

fn owned_session(
    state: &Snapshot,
    current: &State,
    id: &str,
    owner: &str,
) -> anyhow::Result<Session> {
    let session = policy::session(state, id)?
        .context("RETRY_SESSION_UNKNOWN: unknown sessions cannot be reopened by a request")?;
    ensure!(
        session.owner == owner,
        "RETRY_SESSION_FORBIDDEN: session belongs to another principal"
    );
    ensure!(
        session.incarnation == current.incarnation,
        "HISTORY_MISMATCH: session belongs to a different history"
    );
    ensure!(
        session.epoch >= current.min_epoch,
        "RETRY_WINDOW_EXPIRED: session has been retired"
    );
    Ok(session)
}

fn collectable(
    state: &Snapshot,
    current: &State,
    id: &str,
    receipt: &Receipt,
) -> anyhow::Result<bool> {
    if current.collectable(id, receipt) {
        return Ok(true);
    }
    let Ok(identity) = policy::identity(id) else {
        return Ok(false);
    };
    if identity.database != current.database || identity.incarnation != current.incarnation {
        return Ok(false);
    }
    let Some((session, sequence)) = identity.session else {
        return Ok(false);
    };
    Ok(policy::session(state, session)?
        .is_some_and(|session| session.closed || sequence <= session.acknowledged_through))
}

fn collect(
    state: &Snapshot,
    current: &mut State,
    limit: usize,
    removed: &mut Vec<String>,
    metadata: &mut BTreeMap<String, Option<Value>>,
) -> anyhow::Result<()> {
    use std::ops::Bound;
    ensure!(
        limit > 0,
        "RETENTION_GC_INVALID: collection inspection budget must be positive"
    );
    if current.gc_complete {
        return Ok(());
    }
    let mut remaining = limit;
    if !current.gc_receipts_complete {
        let mut entries = state.requests.after(current.gc_cursor.as_deref());
        for (id, receipt) in entries.by_ref().take(limit) {
            remaining -= 1;
            current.gc_cursor = Some(id.clone());
            if collectable(state, current, id, receipt)? {
                remove_receipt(current, id, receipt, removed)?;
            }
        }
        current.gc_receipts_complete = entries.next().is_none();
        if current.gc_receipts_complete {
            current.gc_cursor = None;
        }
    }
    if current.gc_receipts_complete {
        let mut sessions = state.data.range((
            Bound::Excluded(
                current
                    .gc_session_cursor
                    .as_deref()
                    .unwrap_or(policy::SESSION_PREFIX),
            ),
            Bound::Excluded("$flower.session;"),
        ));
        for (key, value) in sessions.by_ref().take(remaining) {
            current.gc_session_cursor = Some(key.clone());
            let session = policy::decode_session(value)?;
            // Current-epoch tombstones/watermarks prevent OpenSession from
            // recreating the ID. Only a surviving epoch/history floor permits GC.
            if session.incarnation != current.incarnation || session.epoch < current.min_epoch {
                current.session_bytes = current
                    .session_bytes
                    .checked_sub(policy::session_bytes(&session)?)
                    .context("session byte accounting underflow")?;
                current.session_count = current
                    .session_count
                    .checked_sub(1)
                    .context("session accounting underflow")?;
                metadata.insert(key.clone(), None);
            }
        }
        current.gc_complete = sessions.next().is_none();
        if current.gc_complete {
            current.gc_session_cursor = None;
        }
    }
    Ok(())
}

fn transition(state: &Snapshot, command: Command) -> anyhow::Result<Change> {
    let current = policy::status(state)?;
    let mut removed = Vec::new();
    let mut metadata = BTreeMap::new();
    let mut session_response = None;
    let next = match command.action {
        Action::Initialize {
            database,
            incarnation,
            max_receipt_bytes,
        } => {
            ensure!(
                current.is_none(),
                "RETENTION_ALREADY_INITIALIZED: a history identity cannot be replaced by initialization"
            );
            no_active_transactions(state)?;
            let receipt_bytes = state
                .requests
                .iter()
                .try_fold(0_u64, |bytes, (id, receipt)| {
                    bytes
                        .checked_add(policy::receipt_bytes(id, receipt)?)
                        .context("receipt counter exhausted")
                })?;
            State {
                database,
                incarnation,
                current_epoch: 0,
                min_epoch: 0,
                receipt_bytes,
                receipt_count: state.requests.len() as u64,
                session_bytes: 0,
                session_count: 0,
                max_receipt_bytes,
                gc_cursor: None,
                gc_complete: true,
                gc_receipts_complete: true,
                gc_session_cursor: None,
                rotation: None,
            }
        }
        action => {
            let mut current =
                current.context("RETENTION_NOT_INITIALIZED: initialize retry admission first")?;
            let incarnation = match &action {
                Action::Advance { incarnation, .. }
                | Action::Collect { incarnation, .. }
                | Action::SetBudget { incarnation, .. }
                | Action::Rotate { incarnation, .. }
                | Action::OpenSession { incarnation, .. }
                | Action::Acknowledge { incarnation, .. }
                | Action::CloseSession { incarnation, .. }
                | Action::Reincarnate { incarnation, .. } => incarnation,
                Action::Initialize { .. } => unreachable!(),
            };
            ensure!(
                incarnation == &current.incarnation,
                "HISTORY_MISMATCH: retention control targets a different history"
            );
            match action {
                Action::Advance {
                    current_epoch,
                    min_epoch,
                    ..
                } => {
                    no_active_transactions(state)?;
                    ensure!(
                        current_epoch >= current.current_epoch
                            && min_epoch >= current.min_epoch
                            && min_epoch <= current_epoch,
                        "RETENTION_EPOCH_INVALID: retry epochs may only advance and floor cannot exceed current epoch"
                    );
                    if min_epoch > current.min_epoch {
                        current.restart_collection();
                    }
                    current.current_epoch = current_epoch;
                    current.min_epoch = min_epoch;
                }
                Action::Collect { limit, .. } => {
                    collect(state, &mut current, limit, &mut removed, &mut metadata)?;
                }
                Action::Rotate {
                    epoch_ms,
                    keep_epochs,
                    ..
                } => {
                    current.rotation = match epoch_ms {
                        None => None,
                        Some(epoch_ms) => {
                            ensure!(
                                (1_000..=policy::MAX_SAFE_INTEGER).contains(&epoch_ms)
                                    && (2..=policy::MAX_SAFE_INTEGER).contains(&keep_epochs),
                                "RETENTION_ROTATION_INVALID: epochs last at least 1000 ms and at least two are kept"
                            );
                            Some(policy::Rotation {
                                epoch_ms,
                                keep_epochs,
                            })
                        }
                    };
                }
                Action::SetBudget {
                    max_receipt_bytes, ..
                } => {
                    // Existing promises remain intact if an operator lowers a
                    // budget below use. New work is blocked until pressure falls.
                    current.max_receipt_bytes = max_receipt_bytes;
                    let reserved = policy::reserved_bytes(state.data.get(policy::RESERVED_BYTES))?;
                    if reserved > 0 {
                        current.capacity(current.receipt_bytes, reserved)?;
                    }
                }
                Action::OpenSession {
                    session,
                    owner,
                    epoch,
                    ..
                } => {
                    ensure!(
                        epoch == current.current_epoch,
                        "RETRY_EPOCH_NOT_ADMITTED: new sessions must use the current epoch"
                    );
                    ensure!(
                        policy::session(state, &session)?.is_none(),
                        "RETRY_SESSION_EXISTS: session IDs cannot be reopened"
                    );
                    let session = Session {
                        id: session,
                        incarnation: current.incarnation.clone(),
                        owner,
                        epoch,
                        acknowledged_through: 0,
                        closed: false,
                    };
                    policy::decode_session(&serde_json::to_value(&session)?)?;
                    store_session(&mut current, None, &session, &mut metadata)?;
                    current.capacity(
                        current.receipt_bytes,
                        policy::reserved_bytes(state.data.get(policy::RESERVED_BYTES))?,
                    )?;
                    session_response = Some(session);
                }
                Action::Acknowledge {
                    session,
                    owner,
                    through,
                    limit,
                    abandon,
                    ..
                } => {
                    no_active_transactions(state)?;
                    let old = owned_session(state, &current, &session, &owner)?;
                    ensure!(!old.closed, "RETRY_SESSION_CLOSED: session is terminal");
                    ensure!(
                        through <= 9_007_199_254_740_991
                            && through >= old.acknowledged_through
                            && limit > 0,
                        "RETRY_ACK_INVALID: acknowledgement must advance a safe watermark with a positive work budget"
                    );
                    let before_bytes = current
                        .receipt_bytes
                        .checked_add(current.session_bytes)
                        .context("retention byte counter exhausted")?;
                    let mut next = old.clone();
                    next.acknowledged_through = through;
                    if abandon {
                        // Abandonment fences the whole prefix immediately, while
                        // deleting only bounded existing receipts (never walking
                        // billions of missing sequence numbers).
                        let prefix = format!(
                            "f2:{}:{}:{}:{}:",
                            current.database, current.incarnation, old.epoch, old.id
                        );
                        for (id, receipt) in state
                            .requests
                            .after(Some(&prefix))
                            .take_while(|(id, _)| id.starts_with(&prefix))
                            .take(limit)
                        {
                            if policy::identity(id)?
                                .session
                                .is_some_and(|(_, sequence)| sequence <= through)
                            {
                                remove_receipt(&mut current, id, receipt, &mut removed)?;
                            }
                        }
                        current.restart_collection();
                    } else {
                        let distance = through - old.acknowledged_through;
                        ensure!(
                            distance <= limit as u64,
                            "RETRY_ACK_BUDGET: split acknowledgement into bounded contiguous prefixes"
                        );
                        ensure!(
                            distance <= state.requests.len() as u64,
                            "RETRY_ACK_GAP: prefix includes unknown outcomes; consume them or explicitly abandon the prefix"
                        );
                        for sequence in old.acknowledged_through + 1..=through {
                            let id = policy::session_request_id(&current, &old, sequence)?;
                            let receipt = state.requests.get(&id).context("RETRY_ACK_GAP: prefix includes unknown outcomes; consume them or explicitly abandon the prefix")?;
                            remove_receipt(&mut current, &id, receipt, &mut removed)?;
                        }
                    }
                    store_session(&mut current, Some(&old), &next, &mut metadata)?;
                    let after_bytes = current
                        .receipt_bytes
                        .checked_add(current.session_bytes)
                        .context("retention byte counter exhausted")?;
                    // An abandonment may grow the watermark without releasing
                    // any result. Bound that extra metadata too, while allowing
                    // cleanup that reduces pressure even after budget lowering.
                    if after_bytes > before_bytes {
                        current.capacity(
                            current.receipt_bytes,
                            policy::reserved_bytes(state.data.get(policy::RESERVED_BYTES))?,
                        )?;
                    }
                    session_response = Some(next);
                }
                Action::CloseSession {
                    session,
                    owner,
                    limit,
                    ..
                } => {
                    no_active_transactions(state)?;
                    ensure!(
                        limit > 0,
                        "RETENTION_GC_INVALID: collection inspection budget must be positive"
                    );
                    let old = owned_session(state, &current, &session, &owner)?;
                    let mut next = old.clone();
                    next.closed = true;
                    let prefix = format!(
                        "f2:{}:{}:{}:{}:",
                        current.database, current.incarnation, old.epoch, old.id
                    );
                    for (id, receipt) in state
                        .requests
                        .after(Some(&prefix))
                        .take_while(|(id, _)| id.starts_with(&prefix))
                        .take(limit)
                    {
                        remove_receipt(&mut current, id, receipt, &mut removed)?;
                    }
                    current.restart_collection();
                    store_session(&mut current, Some(&old), &next, &mut metadata)?;
                    session_response = Some(next);
                }
                Action::Reincarnate {
                    new_incarnation,
                    fence_attestation,
                    ..
                } => {
                    ensure!(
                        !fence_attestation.trim().is_empty(),
                        "RESTORE_FENCE_REQUIRED: attest that the previous deployment is externally fenced"
                    );
                    ensure!(
                        !state
                            .data
                            .keys_from("transaction:")
                            .next()
                            .is_some_and(|key| key.starts_with("transaction:")),
                        "CROSS_GROUP_RESTORE_UNSUPPORTED: unrelated group history cannot be reincarnated without a coordinated restore protocol"
                    );
                    ensure!(
                        new_incarnation != current.incarnation,
                        "HISTORY_MISMATCH: recovery must create a new incarnation"
                    );
                    ensure!(
                        !state
                            .data
                            .contains_key(&format!("$flower.history:{new_incarnation}")),
                        "HISTORY_REUSED: a retired incarnation can never be revived"
                    );
                    metadata.insert(
                        format!("$flower.history:{}", current.incarnation),
                        Some(serde_json::json!({"retiredAtRevision":state.revision + 1})),
                    );
                    metadata.insert("$flower.history".into(), Some(serde_json::json!({"previousIncarnation":current.incarnation,"incarnation":new_incarnation,"fenceAttestation":fence_attestation,"revision":state.revision + 1})));
                    current.incarnation = new_incarnation;
                    current.current_epoch = 0;
                    current.min_epoch = 0;
                    current.restart_collection();
                }
                Action::Initialize { .. } => unreachable!(),
            }
            current
        }
    };
    next.validate()?;
    Ok(Change {
        state: next,
        receipts: removed,
        metadata,
        session: session_response,
    })
}

pub(super) fn apply_retention(
    state: &Snapshot,
    delta: &mut ApplicationDelta,
    command: Command,
) -> ApplyResult {
    if command.expected_revision != delta.revision {
        return ApplyResult::Rejected(format!(
            "conflict: expected revision {}, current revision {}",
            command.expected_revision, delta.revision
        ));
    }
    let Some(revision) = delta
        .revision
        .checked_add(1)
        .filter(|revision| *revision <= 9_007_199_254_740_991)
    else {
        return ApplyResult::Rejected("application revision exhausted".into());
    };
    let Change {
        state: next,
        receipts: removed,
        metadata,
        session,
    } = match transition(&project(state, delta), command) {
        Ok(change) => change,
        Err(error) => return ApplyResult::Rejected(error.to_string()),
    };
    for id in &removed {
        delta.requests.remove(id);
        delta.deleted_requests.insert(id.clone());
    }
    delta.data.extend(metadata);
    let mut result = serde_json::json!({"state": next, "collected": removed.len()});
    if let Some(session) = session {
        result["session"] = serde_json::to_value(session).expect("session metadata serializes");
    }
    publish_retention(delta, Some(next));
    delta.revision = revision;
    ApplyResult::Committed(CommitResult {
        revision,
        duplicate: false,
        result,
    })
}
