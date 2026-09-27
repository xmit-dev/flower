//! Placement records live in the designated catalog group's durable state.
//! A rebalance plan starts one move at a time; planning does not pause tenants.
use super::{Runtime, validate_addresses, validate_name};
use crate::consensus::{Commit, Snapshot};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const GROUP: &str = "partition-catalog:group:";
const PARTITION: &str = "partition-catalog:partition:";
const MOVE: &str = "partition-catalog:move:";
const PLAN: &str = "partition-catalog:plan:";
const ACTIVE_PLAN: &str = "partition-catalog:active-plan";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(in crate::service) struct Group {
    pub id: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(in crate::service) enum Status {
    Creating,
    Active,
    Moving,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(in crate::service) enum Phase {
    Copying,
    Freezing,
    Importing,
    Activating,
    Retiring,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::service) struct Move {
    pub operation: String,
    pub partition: String,
    pub source: Group,
    pub destination: Group,
    pub source_epoch: u64,
    pub epoch: u64,
    pub phase: Phase,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::service) struct Placement {
    pub partition: String,
    pub epoch: u64,
    pub owner: Group,
    pub status: Status,
    pub operation: String,
    pub movement: Option<Move>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct PlannedMove {
    pub partition: String,
    pub source: String,
    pub destination: String,
    pub operation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Rebalance {
    pub operation: String,
    pub groups: Vec<Group>,
    pub moves: Vec<PlannedMove>,
    pub next: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct View {
    pub groups: Vec<Group>,
    pub partitions: Vec<Placement>,
    pub moves: Vec<Move>,
    pub rebalance: Option<Rebalance>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum CatalogRequest {
    Resolve {
        partition: String,
    },
    List,
    RegisterGroup {
        group: Group,
    },
    RemoveGroup {
        group: String,
    },
    Create {
        partition: String,
        group: String,
        operation: String,
    },
    Ready {
        partition: String,
        operation: String,
    },
    BeginMove {
        partition: String,
        destination: String,
        operation: String,
    },
    Advance {
        partition: String,
        operation: String,
        phase: Phase,
    },
    Rebalance {
        groups: Vec<String>,
        operation: String,
    },
    StepRebalance {
        operation: String,
    },
}

pub(in crate::service) fn serving(placement: &Placement) -> bool {
    placement.status == Status::Active
        || (placement.status == Status::Moving
            && placement
                .movement
                .as_ref()
                .is_some_and(|movement| movement.phase == Phase::Copying))
}

fn key(prefix: &str, id: &str) -> String {
    format!(
        "{prefix}{}",
        serde_json::to_string(id).expect("string encodes")
    )
}
fn decode<T: serde::de::DeserializeOwned>(
    state: &Snapshot,
    prefix: &str,
    id: &str,
) -> anyhow::Result<Option<T>> {
    state
        .data
        .get(&key(prefix, id))
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}
fn entries<T: serde::de::DeserializeOwned>(
    state: &Snapshot,
    prefix: &str,
) -> anyhow::Result<Vec<T>> {
    state
        .data
        .range((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Unbounded,
        ))
        .take_while(|(key, _)| key.starts_with(prefix))
        .map(|(_, value)| serde_json::from_value(value.clone()).map_err(Into::into))
        .collect()
}
fn active_plan(state: &Snapshot) -> anyhow::Result<Option<Rebalance>> {
    let Some(id) = state.data.get(ACTIVE_PLAN) else {
        return Ok(None);
    };
    decode(
        state,
        PLAN,
        id.as_str().context("invalid active rebalance identity")?,
    )
}
fn view(state: &Snapshot) -> anyhow::Result<View> {
    Ok(View {
        groups: entries(state, GROUP)?,
        partitions: entries(state, PARTITION)?,
        moves: entries(state, MOVE)?,
        rebalance: active_plan(state)?,
    })
}
fn put<T: Serialize>(
    puts: &mut BTreeMap<String, Value>,
    prefix: &str,
    id: &str,
    value: &T,
) -> anyhow::Result<()> {
    puts.insert(key(prefix, id), serde_json::to_value(value)?);
    Ok(())
}
fn quiet_plan(state: &Snapshot) -> anyhow::Result<()> {
    ensure!(
        active_plan(state)?.is_none_or(|plan| plan.complete),
        "a durable rebalance is already in progress"
    );
    Ok(())
}
fn placement(state: &Snapshot, partition: &str) -> anyhow::Result<Placement> {
    decode(state, PARTITION, partition)?.context("partition is not registered")
}
fn group(state: &Snapshot, id: &str) -> anyhow::Result<Group> {
    decode(state, GROUP, id)?.context("group is not registered")
}
fn settled(value: &Placement) -> bool {
    value.status == Status::Active
        && value
            .movement
            .as_ref()
            .is_none_or(|movement| movement.phase == Phase::Complete)
}

fn begin(
    state: &Snapshot,
    puts: &mut BTreeMap<String, Value>,
    partition: &str,
    destination: &str,
    operation: &str,
) -> anyhow::Result<Move> {
    validate_name(operation)?;
    if let Some(previous) = decode::<Move>(state, MOVE, operation)? {
        ensure!(
            previous.partition == partition && previous.destination.id == destination,
            "move operation identity was reused"
        );
        return Ok(previous);
    }
    let mut owner = placement(state, partition)?;
    ensure!(
        settled(&owner),
        "partition has an unfinished placement operation"
    );
    let destination = group(state, destination)?;
    ensure!(
        owner.owner.id != destination.id,
        "partition already belongs to destination"
    );
    let movement = Move {
        operation: operation.into(),
        partition: partition.into(),
        source: owner.owner.clone(),
        destination,
        source_epoch: owner.epoch,
        epoch: owner
            .epoch
            .checked_add(1)
            .filter(|epoch| *epoch <= 9_007_199_254_740_991)
            .context("partition epoch exhausted")?,
        phase: Phase::Copying,
    };
    owner.status = Status::Moving;
    owner.operation = operation.into();
    owner.movement = Some(movement.clone());
    put(puts, MOVE, operation, &movement)?;
    put(puts, PARTITION, partition, &owner)?;
    Ok(movement)
}

/// Retain as many current owners as possible while assigning deterministic
/// floor/ceiling counts. Only surplus/excluded owners move, one plan item at a time.
fn plan(state: &Snapshot, groups: Vec<String>, operation: String) -> anyhow::Result<Rebalance> {
    validate_name(&operation)?;
    let unique: BTreeSet<_> = groups.iter().cloned().collect();
    ensure!(
        !groups.is_empty() && unique.len() == groups.len(),
        "rebalance groups must be nonempty and distinct"
    );
    let groups: Vec<_> = unique
        .iter()
        .map(|id| group(state, id))
        .collect::<anyhow::Result<_>>()?;
    if let Some(previous) = decode::<Rebalance>(state, PLAN, &operation)? {
        ensure!(
            previous.groups == groups,
            "rebalance operation identity was reused"
        );
        return Ok(previous);
    }
    quiet_plan(state)?;
    let mut owners = entries::<Placement>(state, PARTITION)?;
    ensure!(
        owners.iter().all(settled),
        "finish existing partition operations before rebalancing"
    );
    owners.sort_by(|left, right| left.partition.cmp(&right.partition));
    let mut desired: BTreeMap<_, _> = groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            (
                group.id.clone(),
                owners.len() / groups.len() + usize::from(index < owners.len() % groups.len()),
            )
        })
        .collect();
    let mut surplus = Vec::new();
    for owner in owners {
        match desired.get_mut(&owner.owner.id) {
            Some(left) if *left > 0 => *left -= 1,
            _ => surplus.push(owner),
        }
    }
    let destinations = desired
        .into_iter()
        .flat_map(|(group, count)| std::iter::repeat_n(group, count));
    let moves = surplus
        .into_iter()
        .zip(destinations)
        .map(|(owner, destination)| PlannedMove {
            operation: format!(
                "rebalance:{}",
                crate::evaluator::hash(
                    serde_json::to_string(&(&operation, &owner.partition, &destination))
                        .unwrap()
                        .as_bytes()
                )
            ),
            partition: owner.partition,
            source: owner.owner.id,
            destination,
        })
        .collect::<Vec<_>>();
    Ok(Rebalance {
        operation,
        groups,
        complete: moves.is_empty(),
        moves,
        next: 0,
    })
}

fn start_plan(
    state: &Snapshot,
    puts: &mut BTreeMap<String, Value>,
    groups: Vec<String>,
    operation: String,
) -> anyhow::Result<Rebalance> {
    let value = plan(state, groups, operation)?;
    // A replay of an old plan must not replace a newer active plan pointer.
    if decode::<Rebalance>(state, PLAN, &value.operation)?.is_none() {
        put(puts, PLAN, &value.operation, &value)?;
        puts.insert(ACTIVE_PLAN.into(), json!(value.operation));
    }
    Ok(value)
}

pub(super) async fn apply(runtime: &Runtime, request: CatalogRequest) -> anyhow::Result<Value> {
    ensure!(
        runtime.config.local_group == runtime.config.catalog_group,
        "this physical group is not the partition catalog"
    );
    if matches!(
        request,
        CatalogRequest::Resolve { .. } | CatalogRequest::List
    ) {
        let state = runtime.consensus.read_query().await?;
        return match request {
            CatalogRequest::Resolve { partition } => Ok(json!(placement(&state, &partition)?)),
            CatalogRequest::List => Ok(json!(view(&state)?)),
            _ => unreachable!(),
        };
    }
    let _guard = runtime.catalog_writer.lock().await;
    let (state, term) = runtime.consensus.read_for_writer_with_term().await?;
    super::super::transactions::ensure_unlocked(&state)
        .map_err(|error| anyhow::anyhow!(error.message))?;
    super::super::transactions::ensure_write_capacity(&state)
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let mut puts = BTreeMap::new();
    let mut deletes = Vec::new();
    let value = match request {
        CatalogRequest::RegisterGroup { group: value } => {
            validate_name(&value.id)?;
            validate_addresses(&value.addresses)?;
            if let Some(previous) = decode::<Group>(&state, GROUP, &value.id)? {
                ensure!(
                    previous == value,
                    "registered group identity/address set is immutable until removed"
                );
            }
            for known in entries::<Group>(&state, GROUP)? {
                ensure!(
                    known.id == value.id
                        || !known
                            .addresses
                            .iter()
                            .any(|address| value.addresses.contains(address)),
                    "an address is already registered in a different group"
                );
            }
            let _: Value = runtime
                .send(
                    &value,
                    super::CONTROL_PATH,
                    &super::coordinator::ControlRequest::Ping,
                )
                .await?;
            put(&mut puts, GROUP, &value.id, &value)?;
            json!(value)
        }
        CatalogRequest::RemoveGroup { group: id } => {
            ensure!(
                id != runtime.config.catalog_group,
                "the designated catalog group cannot be removed"
            );
            ensure!(
                !entries::<Placement>(&state, PARTITION)?
                    .iter()
                    .any(|owner| owner.owner.id == id
                        || owner
                            .movement
                            .as_ref()
                            .is_some_and(|m| m.phase != Phase::Complete
                                && (m.source.id == id || m.destination.id == id))),
                "group still owns or is moving a partition"
            );
            ensure!(
                !active_plan(&state)?
                    .is_some_and(|p| !p.complete && p.groups.iter().any(|g| g.id == id)),
                "group is referenced by an active rebalance"
            );
            deletes.push(key(GROUP, &id));
            json!({"removed":id})
        }
        CatalogRequest::Create {
            partition,
            group: id,
            operation,
        } => {
            validate_name(&partition)?;
            validate_name(&operation)?;
            quiet_plan(&state)?;
            let value = if let Some(previous) = decode::<Placement>(&state, PARTITION, &partition)?
            {
                ensure!(
                    previous.operation == operation
                        && previous.owner.id == id
                        && previous.epoch == 1,
                    "partition already exists with a different creation identity"
                );
                previous
            } else {
                Placement {
                    partition: partition.clone(),
                    epoch: 1,
                    owner: group(&state, &id)?,
                    status: Status::Creating,
                    operation,
                    movement: None,
                }
            };
            if !state.data.contains_key(&key(PARTITION, &partition)) {
                super::budgets::preflight_create(runtime, &value).await?;
            }
            put(&mut puts, PARTITION, &partition, &value)?;
            json!(value)
        }
        CatalogRequest::Ready {
            partition,
            operation,
        } => {
            let mut value = placement(&state, &partition)?;
            ensure!(
                value.operation == operation && value.movement.is_none(),
                "creation operation differs"
            );
            ensure!(
                value.status == Status::Creating || value.status == Status::Active,
                "partition is not being created"
            );
            // The coordinator only calls Ready after the native activation is durable.
            // Direct control callers must prove the same native state below.
            let info: Value = runtime
                .send(
                    &value.owner,
                    super::CONTROL_PATH,
                    &super::coordinator::ControlRequest::Info {
                        partition: partition.clone(),
                    },
                )
                .await?;
            ensure!(
                info["phase"] == "active"
                    && info["epoch"].as_u64() == Some(value.epoch)
                    && info["operation"] == operation,
                "partition activation is not durable"
            );
            value.status = Status::Active;
            put(&mut puts, PARTITION, &partition, &value)?;
            json!(value)
        }
        CatalogRequest::BeginMove {
            partition,
            destination,
            operation,
        } => {
            quiet_plan(&state)?;
            let movement = begin(&state, &mut puts, &partition, &destination, &operation)?;
            if !puts.is_empty() {
                super::budgets::preflight_move(runtime, &movement).await?;
            }
            json!(movement)
        }
        CatalogRequest::Advance {
            partition,
            operation,
            phase,
        } => {
            let mut owner = placement(&state, &partition)?;
            let mut movement = owner.movement.clone().context("partition has no move")?;
            ensure!(movement.operation == operation, "move operation differs");
            if movement.phase != phase {
                ensure!(
                    matches!(
                        (movement.phase, phase),
                        (Phase::Copying, Phase::Freezing)
                            | (Phase::Freezing, Phase::Importing)
                            | (Phase::Importing, Phase::Activating)
                            | (Phase::Activating, Phase::Retiring)
                            | (Phase::Retiring, Phase::Complete)
                    ),
                    "move phase must advance exactly once"
                );
                super::coordinator::verify_advance(runtime, &movement, phase).await?;
                movement.phase = phase;
                if phase == Phase::Activating {
                    owner.owner = movement.destination.clone();
                    owner.epoch = movement.epoch;
                }
                if matches!(phase, Phase::Retiring | Phase::Complete) {
                    owner.status = Status::Active;
                }
                owner.movement = Some(movement.clone());
                put(&mut puts, MOVE, &operation, &movement)?;
                put(&mut puts, PARTITION, &partition, &owner)?;
            }
            json!(movement)
        }
        CatalogRequest::Rebalance { groups, operation } => {
            let value = start_plan(&state, &mut puts, groups, operation)?;
            if !puts.is_empty() {
                let mut movements = Vec::with_capacity(value.moves.len());
                for item in &value.moves {
                    ensure!(
                        !state.data.contains_key(&key(MOVE, &item.operation)),
                        "rebalance move identity was already used; choose another operation ID"
                    );
                    movements.push(begin(
                        &state,
                        &mut BTreeMap::new(),
                        &item.partition,
                        &item.destination,
                        &item.operation,
                    )?);
                }
                super::budgets::preflight_moves(runtime, &movements).await?;
            }
            json!(value)
        }
        CatalogRequest::StepRebalance { operation } => {
            let mut value = active_plan(&state)?.context("no active rebalance")?;
            ensure!(value.operation == operation, "rebalance operation differs");
            if !value.complete {
                if let Some(item) = value.moves.get(value.next) {
                    match decode::<Move>(&state, MOVE, &item.operation)? {
                        Some(movement) if movement.phase == Phase::Complete => {
                            value.next += 1;
                        }
                        Some(_) => {}
                        None => {
                            ensure!(
                                placement(&state, &item.partition)?.owner.id == item.source,
                                "rebalance source changed"
                            );
                            let movement = begin(
                                &state,
                                &mut puts,
                                &item.partition,
                                &item.destination,
                                &item.operation,
                            )?;
                            super::budgets::preflight_move(runtime, &movement).await?;
                        }
                    }
                }
                value.complete = value.next == value.moves.len();
                put(&mut puts, PLAN, &operation, &value)?;
            }
            json!(value)
        }
        CatalogRequest::Resolve { .. } | CatalogRequest::List => unreachable!(),
    };
    puts.retain(|key, value| state.data.get(key) != Some(value));
    deletes.retain(|key| state.data.contains_key(key));
    if !puts.is_empty() || !deletes.is_empty() {
        runtime
            .consensus
            .commit_in_term(
                Commit {
                    internal: true,
                    request_id: String::new(),
                    fingerprint: String::new(),
                    expected_revision: state.revision,
                    puts,
                    deletes,
                    result: Value::Null,
                },
                term,
            )
            .await?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(owners: &[&str]) -> Snapshot {
        let mut state = Snapshot::default();
        for name in ["a", "b", "c"] {
            state.data.insert(
                key(GROUP, name),
                json!(Group {
                    id: name.into(),
                    addresses: vec![format!("{name}:7101")]
                }),
            );
        }
        for (index, name) in owners.iter().enumerate() {
            let partition = format!("tenant-{index}");
            state.data.insert(
                key(PARTITION, &partition),
                json!(Placement {
                    partition: partition.clone(),
                    epoch: 1,
                    owner: group(&state, name).unwrap(),
                    status: Status::Active,
                    operation: format!("create-{index}"),
                    movement: None
                }),
            );
        }
        state
    }

    #[test]
    fn rebalance_preserves_owners_and_moves_only_surplus_partitions() {
        let state = fixture(&["a", "a", "a", "a", "b", "b"]);
        let plan = plan(
            &state,
            vec!["c".into(), "b".into(), "a".into()],
            "expand".into(),
        )
        .unwrap();
        assert_eq!(
            plan.groups
                .iter()
                .map(|group| group.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(plan.moves.len(), 2);
        assert!(
            plan.moves
                .iter()
                .all(|item| item.source == "a" && item.destination == "c")
        );
        assert!(
            entries::<Placement>(&state, PARTITION)
                .unwrap()
                .iter()
                .all(settled)
        );
        let shrunk = super::plan(&state, vec!["b".into()], "shrink".into()).unwrap();
        assert_eq!(shrunk.moves.len(), 4);
        assert!(shrunk.moves.iter().all(|item| item.destination == "b"));
    }

    #[test]
    fn plans_validate_destinations_and_block_overlap_without_pausing_every_tenant() {
        let mut state = fixture(&["a", "a"]);
        for groups in [vec![], vec!["a".into(), "a".into()], vec!["missing".into()]] {
            assert!(plan(&state, groups, "invalid".into()).is_err());
        }
        let mut puts = BTreeMap::new();
        let plan = start_plan(&state, &mut puts, vec!["b".into()], "first".into()).unwrap();
        state.data.extend(puts);
        assert!(!plan.complete);
        assert!(quiet_plan(&state).is_err());
        assert!(super::plan(&state, vec!["c".into()], "overlap".into()).is_err());
        assert!(
            entries::<Placement>(&state, PARTITION)
                .unwrap()
                .iter()
                .all(settled)
        );
    }

    #[test]
    fn replaying_old_rebalance_cannot_replace_new_active_plan() {
        let mut state = fixture(&["a", "a"]);
        let mut old = plan(&state, vec!["b".into()], "old".into()).unwrap();
        old.complete = true;
        old.next = old.moves.len();
        state.data.insert(key(PLAN, "old"), json!(old));
        let current = plan(&state, vec!["c".into()], "current".into()).unwrap();
        state.data.insert(key(PLAN, "current"), json!(current));
        state.data.insert(ACTIVE_PLAN.into(), json!("current"));
        let mut puts = BTreeMap::new();
        assert_eq!(
            start_plan(&state, &mut puts, vec!["b".into()], "old".into()).unwrap(),
            old
        );
        assert!(puts.is_empty());
        assert_eq!(active_plan(&state).unwrap().unwrap().operation, "current");
    }

    #[test]
    fn exhausted_epoch_rejects_before_marking_partition_moving() {
        let mut state = fixture(&["a"]);
        let mut owner = placement(&state, "tenant-0").unwrap();
        owner.epoch = 9_007_199_254_740_991;
        state.data.insert(key(PARTITION, "tenant-0"), json!(owner));
        let mut puts = BTreeMap::new();
        assert!(begin(&state, &mut puts, "tenant-0", "b", "exhausted").is_err());
        assert!(puts.is_empty());
        assert!(settled(&placement(&state, "tenant-0").unwrap()));
    }

    #[test]
    fn move_identity_replay_is_immutable_and_begins_with_source_owner() {
        let mut state = fixture(&["a", "a"]);
        let mut puts = BTreeMap::new();
        let movement = begin(&state, &mut puts, "tenant-0", "b", "move-0").unwrap();
        state.data.extend(puts);
        assert_eq!(placement(&state, "tenant-0").unwrap().owner.id, "a");
        assert_eq!(
            placement(&state, "tenant-0").unwrap().status,
            Status::Moving
        );
        assert!(settled(&placement(&state, "tenant-1").unwrap()));
        let mut replay = BTreeMap::new();
        assert_eq!(
            begin(&state, &mut replay, "tenant-0", "b", "move-0").unwrap(),
            movement
        );
        assert!(replay.is_empty());
        assert!(begin(&state, &mut replay, "tenant-0", "c", "move-0").is_err());
        assert!(begin(&state, &mut replay, "tenant-1", "b", "move-0").is_err());
    }
}
