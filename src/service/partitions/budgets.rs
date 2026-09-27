//! Admit every later ownership transition before pausing a partition. Names
//! have no arbitrary length limit; their actual JSON envelopes consume budgets.
use super::{
    CONTROL_PATH, PeerRequest, PeerResponse, Runtime,
    catalog::{Group, Move, Phase, Placement, Status},
    coordinator::{ControlRequest, ExportChunk, transfer},
};
use crate::consensus::{
    Commit, ExportKind, PartitionCommand, PartitionInfo, PartitionPhase, RaftCommand,
    encoded_json_len,
};
use anyhow::{Context, ensure};
use openraft::CommittedLeaderId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize)]
struct Budgets {
    rpc_max_bytes: usize,
    transaction_max_bytes: usize,
}

async fn policy(runtime: &Runtime, group: &Group) -> anyhow::Result<Budgets> {
    let budgets: Budgets = runtime
        .send(group, CONTROL_PATH, &ControlRequest::Ping)
        .await?;
    ensure!(
        budgets.rpc_max_bytes > 0 && budgets.transaction_max_bytes > 0,
        "peer advertised invalid partition budgets"
    );
    Ok(budgets)
}

fn fits<T: Serialize>(value: &T, limit: usize, what: &str) -> anyhow::Result<()> {
    let bytes = encoded_json_len(value)?;
    ensure!(
        bytes <= limit,
        "{what} needs {bytes} bytes, exceeding configured budget {limit}"
    );
    Ok(())
}

fn command_size(command: &PartitionCommand) -> anyhow::Result<usize> {
    Ok(encoded_json_len(&RaftCommand::PartitionControl {
        partition_control: command.clone(),
        leader_id: Some(CommittedLeaderId::new(u64::MAX, u64::MAX)),
    })?)
}

fn request(group: &Group, command: PartitionCommand) -> PeerRequest<ControlRequest> {
    PeerRequest {
        group: group.id.clone(),
        body: ControlRequest::Control { command },
    }
}

fn check_control(group: &Group, command: PartitionCommand, budgets: Budgets) -> anyhow::Result<()> {
    let bytes = command_size(&command)?;
    ensure!(
        bytes <= budgets.transaction_max_bytes,
        "partition transition for group {} needs {bytes} log bytes, exceeding FLOWER_TRANSACTION_MAX_BYTES ({})",
        group.id,
        budgets.transaction_max_bytes
    );
    fits(
        &request(group, command),
        budgets.rpc_max_bytes,
        "partition transition RPC",
    )
}

fn info(partition: &str, epoch: u64, operation: &str, movement: Option<&Move>) -> PartitionInfo {
    PartitionInfo {
        partition: partition.into(),
        epoch,
        operation: operation.into(),
        phase: PartitionPhase::Importing,
        transfer: movement.map(transfer),
        revision: u64::MAX,
        digest: Some("f".repeat(64)),
        bytes: u64::MAX,
        received_bytes: u64::MAX,
        next_chunk: u64::MAX,
        base_bytes: u64::MAX,
    }
}

pub(super) async fn preflight_create(
    runtime: &Runtime,
    placement: &Placement,
) -> anyhow::Result<()> {
    let budgets = policy(runtime, &placement.owner).await?;
    for command in [
        PartitionCommand::Create {
            partition: placement.partition.clone(),
            epoch: placement.epoch,
            operation: placement.operation.clone(),
        },
        PartitionCommand::Activate {
            partition: placement.partition.clone(),
            epoch: placement.epoch,
            operation: placement.operation.clone(),
        },
    ] {
        check_control(&placement.owner, command, budgets)?;
    }
    let mut result = info(
        &placement.partition,
        placement.epoch,
        &placement.operation,
        None,
    );
    result.phase = PartitionPhase::Staged;
    result.revision = 0;
    result.digest = None;
    result.bytes = 0;
    result.received_bytes = 0;
    result.next_chunk = 0;
    fits(
        &PeerResponse {
            group: placement.owner.id.clone(),
            body: result,
        },
        runtime.consensus.limits().rpc_max_bytes,
        "partition creation acknowledgement",
    )?;
    fits(
        &PeerResponse {
            group: runtime.config.catalog_group.clone(),
            body: placement,
        },
        budgets.rpc_max_bytes,
        "catalog placement response",
    )?;
    check_catalog_transition(placement, runtime.consensus.limits().transaction_max_bytes)
}

pub(super) async fn preflight_move(runtime: &Runtime, movement: &Move) -> anyhow::Result<()> {
    chunk_capacity(runtime, movement).await?;
    check_catalog_move(runtime, movement)
}

// One readiness/budget observation per physical group at plan admission. Each
// actual move repeats admission checks because operators can change settings.
pub(super) async fn preflight_moves(runtime: &Runtime, movements: &[Move]) -> anyhow::Result<()> {
    let mut policies = std::collections::BTreeMap::new();
    for movement in movements {
        for group in [&movement.source, &movement.destination] {
            if !policies.contains_key(&group.id) {
                policies.insert(group.id.clone(), policy(runtime, group).await?);
            }
        }
        capacity(
            movement,
            &runtime.config.catalog_group,
            runtime.consensus.limits().rpc_max_bytes,
            policies[&movement.source.id],
            policies[&movement.destination.id],
        )?;
        check_catalog_move(runtime, movement)?;
    }
    Ok(())
}

fn check_catalog_move(runtime: &Runtime, movement: &Move) -> anyhow::Result<()> {
    for owner in [&movement.source, &movement.destination] {
        let mut future = movement.clone();
        future.phase = Phase::Activating;
        check_catalog_transition(
            &Placement {
                partition: movement.partition.clone(),
                epoch: movement.epoch,
                owner: owner.clone(),
                status: Status::Active,
                operation: movement.operation.clone(),
                movement: Some(future),
            },
            runtime.consensus.limits().transaction_max_bytes,
        )?;
    }
    Ok(())
}

fn check_catalog_transition(placement: &Placement, budget: usize) -> anyhow::Result<()> {
    let mut puts = std::collections::BTreeMap::new();
    puts.insert(
        format!(
            "partition-catalog:partition:{}",
            serde_json::to_string(&placement.partition)?
        ),
        serde_json::to_value(placement)?,
    );
    if let Some(movement) = &placement.movement {
        puts.insert(
            format!(
                "partition-catalog:move:{}",
                serde_json::to_string(&movement.operation)?
            ),
            serde_json::to_value(movement)?,
        );
    }
    fits(
        &RaftCommand::Fenced {
            leader_id: CommittedLeaderId::new(u64::MAX, u64::MAX),
            commit: Commit {
                internal: true,
                request_id: String::new(),
                fingerprint: String::new(),
                expected_revision: u64::MAX,
                puts,
                deletes: Vec::new(),
                result: serde_json::Value::Null,
            },
        },
        budget,
        "future catalog transition",
    )
}

pub(super) async fn chunk_capacity(runtime: &Runtime, movement: &Move) -> anyhow::Result<usize> {
    let source = policy(runtime, &movement.source).await?;
    let destination = policy(runtime, &movement.destination).await?;
    capacity(
        movement,
        &runtime.config.catalog_group,
        runtime.consensus.limits().rpc_max_bytes,
        source,
        destination,
    )
}

fn capacity(
    movement: &Move,
    catalog: &str,
    caller_rpc: usize,
    source: Budgets,
    destination: Budgets,
) -> anyhow::Result<usize> {
    let transfer = transfer(movement);
    for command in [
        PartitionCommand::Capture {
            transfer: transfer.clone(),
            expected_revision: u64::MAX,
            max_bytes: Some(u64::MAX),
        },
        PartitionCommand::Freeze {
            transfer: transfer.clone(),
            expected_revision: u64::MAX,
        },
        PartitionCommand::Retire {
            transfer: transfer.clone(),
        },
    ] {
        check_control(&movement.source, command, source)?;
    }
    for command in [
        PartitionCommand::BeginCopy {
            transfer: transfer.clone(),
            revision: u64::MAX,
            digest: "f".repeat(64),
            bytes: u64::MAX,
        },
        PartitionCommand::BeginDelta {
            transfer: transfer.clone(),
            base_revision: u64::MAX,
            revision: u64::MAX,
            digest: "f".repeat(64),
            bytes: u64::MAX,
        },
        PartitionCommand::FinalizeCopy {
            transfer: transfer.clone(),
            revision: u64::MAX,
            digest: "f".repeat(64),
            bytes: u64::MAX,
        },
        PartitionCommand::BeginImport {
            transfer: transfer.clone(),
            revision: u64::MAX,
            digest: "f".repeat(64),
            bytes: u64::MAX,
        },
        PartitionCommand::SealImport {
            partition: movement.partition.clone(),
            epoch: movement.epoch,
            operation: movement.operation.clone(),
        },
        PartitionCommand::Activate {
            partition: movement.partition.clone(),
            epoch: movement.epoch,
            operation: movement.operation.clone(),
        },
    ] {
        check_control(&movement.destination, command, destination)?;
    }
    for (group, epoch) in [
        (&movement.source, movement.source_epoch),
        (&movement.destination, movement.epoch),
    ] {
        fits(
            &PeerResponse {
                group: group.id.clone(),
                body: info(
                    &movement.partition,
                    epoch,
                    &movement.operation,
                    Some(movement),
                ),
            },
            caller_rpc,
            "partition transition acknowledgement",
        )?;
    }
    // Both groups consult catalog ownership during the transfer. The larger
    // source/destination route and longest phase name must remain readable.
    for owner in [&movement.source, &movement.destination] {
        let mut future = movement.clone();
        future.phase = Phase::Activating;
        let placement = Placement {
            partition: movement.partition.clone(),
            epoch: movement.epoch,
            owner: owner.clone(),
            status: Status::Moving,
            operation: movement.operation.clone(),
            movement: Some(future),
        };
        fits(
            &PeerResponse {
                group: catalog.to_owned(),
                body: placement,
            },
            source
                .rpc_max_bytes
                .min(destination.rpc_max_bytes)
                .min(caller_rpc),
            "catalog move response",
        )?;
    }
    let export_request = PeerRequest {
        group: movement.source.id.clone(),
        body: ControlRequest::Export {
            partition: movement.partition.clone(),
            operation: movement.operation.clone(),
            image: ExportKind::Snapshot,
            offset: Some(u64::MAX),
            max_bytes: usize::MAX,
        },
    };
    fits(
        &export_request,
        source.rpc_max_bytes,
        "partition export request",
    )?;
    let chunk = PartitionCommand::ImportChunk {
        partition: movement.partition.clone(),
        epoch: movement.epoch,
        operation: movement.operation.clone(),
        index: u64::MAX,
        data: String::new(),
    };
    let raft_overhead = command_size(&chunk)?;
    let request_overhead = encoded_json_len(&request(&movement.destination, chunk))?;
    let response_overhead = encoded_json_len(&PeerResponse {
        group: movement.source.id.clone(),
        body: ExportChunk {
            revision: u64::MAX,
            base_revision: Some(u64::MAX),
            digest: "f".repeat(64),
            bytes: u64::MAX,
            offset: u64::MAX,
            data: String::new(),
            next_offset: u64::MAX,
        },
    })?;
    // BeginImport's authorization fetches a manifest at the destination too.
    ensure!(
        response_overhead <= destination.rpc_max_bytes,
        "destination cannot receive source manifest within FLOWER_RPC_MAX_BYTES"
    );
    let remaining = destination
        .transaction_max_bytes
        .checked_sub(raft_overhead)
        .context("partition chunk metadata exceeds transaction budget")?
        .min(
            destination
                .rpc_max_bytes
                .checked_sub(request_overhead)
                .context("partition chunk metadata exceeds RPC budget")?,
        )
        .min(
            source
                .rpc_max_bytes
                .min(caller_rpc)
                .checked_sub(response_overhead)
                .context("partition export metadata exceeds response budget")?,
        );
    // A JSON string expands each raw byte to at most six bytes. Four raw bytes
    // ensure every valid UTF-8 scalar can make progress without splitting it.
    let capacity = remaining / 6;
    ensure!(
        capacity >= 4,
        "partition envelopes leave insufficient room for one UTF-8 character"
    );
    Ok(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn movement() -> Move {
        Move {
            operation: "move".into(),
            partition: "flower".into(),
            source: Group {
                id: "a".into(),
                addresses: vec!["a:7101".into()],
            },
            destination: Group {
                id: "b".into(),
                addresses: vec!["b:7101".into()],
            },
            source_epoch: 1,
            epoch: 2,
            phase: Phase::Freezing,
        }
    }
    #[test]
    fn chunk_capacity_accounts_for_exact_envelopes_and_worst_case_escaping() {
        let movement = movement();
        let budget = Budgets {
            rpc_max_bytes: 4096,
            transaction_max_bytes: 3072,
        };
        let size = capacity(&movement, "catalog", 4096, budget, budget).unwrap();
        let command = PartitionCommand::ImportChunk {
            partition: movement.partition.clone(),
            epoch: 2,
            operation: movement.operation.clone(),
            index: u64::MAX,
            data: "\0".repeat(size),
        };
        assert!(command_size(&command).unwrap() <= budget.transaction_max_bytes);
        assert!(
            encoded_json_len(&request(&movement.destination, command)).unwrap()
                <= budget.rpc_max_bytes
        );
        let mut long = movement;
        long.operation = "x".repeat(2800);
        assert!(capacity(&long, "catalog", 4096, budget, budget).is_err());
    }
    #[test]
    fn catalog_cutover_checks_destination_descriptor_and_record_duplication() {
        let mut movement = movement();
        movement.destination.addresses = vec![format!("{}:7101", "b".repeat(500))];
        movement.phase = Phase::Activating;
        let placement = Placement {
            partition: movement.partition.clone(),
            epoch: movement.epoch,
            owner: movement.destination.clone(),
            status: Status::Active,
            operation: movement.operation.clone(),
            movement: Some(movement),
        };
        // Native controls contain group identities, while catalog transitions
        // also duplicate peer descriptors in the placement and move history.
        assert!(check_catalog_transition(&placement, 1024).is_err());
        assert!(check_catalog_transition(&placement, 4096).is_ok());
    }

    #[test]
    fn asymmetric_peer_and_catalog_budgets_fail_before_source_freeze() {
        let movement = movement();
        let large = Budgets {
            rpc_max_bytes: 65536,
            transaction_max_bytes: 65536,
        };
        let small = Budgets {
            rpc_max_bytes: 128,
            transaction_max_bytes: 128,
        };
        assert!(capacity(&movement, "catalog", 65536, large, small).is_err());
        assert!(capacity(&movement, "catalog", 128, large, large).is_err());
        assert!(capacity(&movement, "catalog", 65536, large, large).unwrap() > 4);
    }
}
