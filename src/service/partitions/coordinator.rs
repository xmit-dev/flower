//! Recoverable ownership transfer. Every phase is monotonic; an interrupted
//! operator request leaves durable work for the catalog leader to resume.
use super::{
    CONTROL_PATH, Runtime,
    catalog::{self, CatalogRequest, Group, Move, Phase, Placement, Status, View},
};
use crate::consensus::{
    ExportKind, PartitionCommand, PartitionInfo, PartitionPhase, PartitionTransfer,
};
use anyhow::{Context, ensure};
use openraft::CommittedLeaderId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, Weak};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ControlRequest {
    Ping,
    Locate,
    Info {
        partition: String,
    },
    Control {
        command: PartitionCommand,
    },
    Export {
        partition: String,
        operation: String,
        #[serde(default)]
        image: ExportKind,
        offset: Option<u64>,
        max_bytes: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct LeaderLocation {
    pub address: String,
}

pub(super) struct ExportCache {
    partition: String,
    operation: String,
    epoch: u64,
    revision: u64,
    base_revision: Option<u64>,
    image: ExportKind,
    digest: String,
    bytes: Arc<ExportBytes>,
}

struct ExportBytes {
    text: String,
    _retained: crate::service::admission::Input,
}
impl std::ops::Deref for ExportBytes {
    type Target = str;
    fn deref(&self) -> &str {
        &self.text
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ExportChunk {
    pub revision: u64,
    pub base_revision: Option<u64>,
    pub digest: String,
    pub bytes: u64,
    pub offset: u64,
    pub data: String,
    pub next_offset: u64,
}

pub(super) fn transfer(movement: &Move) -> PartitionTransfer {
    PartitionTransfer {
        partition: movement.partition.clone(),
        operation: movement.operation.clone(),
        source: movement.source.id.clone(),
        destination: movement.destination.id.clone(),
        source_epoch: movement.source_epoch,
        epoch: movement.epoch,
    }
}

fn matches_transfer(info: &PartitionInfo, movement: &Move, source: bool) -> bool {
    info.partition == movement.partition
        && info.operation == movement.operation
        && info.epoch
            == if source {
                movement.source_epoch
            } else {
                movement.epoch
            }
        && info.transfer.as_ref() == Some(&transfer(movement))
}

pub(super) async fn info(
    runtime: &Runtime,
    group: &Group,
    partition: &str,
) -> anyhow::Result<PartitionInfo> {
    runtime
        .send(
            group,
            CONTROL_PATH,
            &ControlRequest::Info {
                partition: partition.into(),
            },
        )
        .await
}

async fn manifest(
    runtime: &Runtime,
    movement: &Move,
    image: ExportKind,
) -> anyhow::Result<ExportChunk> {
    runtime
        .send(
            &movement.source,
            CONTROL_PATH,
            &ControlRequest::Export {
                partition: movement.partition.clone(),
                operation: movement.operation.clone(),
                image,
                offset: None,
                max_bytes: 0,
            },
        )
        .await
}

/// Catalog transitions verify durable native state themselves; an operator
/// cannot advance ownership by claiming that a remote action succeeded.
pub(super) async fn verify_advance(
    runtime: &Runtime,
    movement: &Move,
    phase: Phase,
) -> anyhow::Result<()> {
    match phase {
        Phase::Freezing => {
            let source = manifest(runtime, movement, ExportKind::Base).await?;
            let destination = info(runtime, &movement.destination, &movement.partition).await?;
            ensure!(
                matches_transfer(&destination, movement, false)
                    && destination.phase == PartitionPhase::Copied
                    && destination.revision == source.revision
                    && destination.digest.as_ref() == Some(&source.digest)
                    && destination.bytes == source.bytes,
                "destination has no durable complete copy base"
            );
        }
        Phase::Importing => {
            let source = info(runtime, &movement.source, &movement.partition).await?;
            ensure!(
                matches_transfer(&source, movement, true) && source.phase == PartitionPhase::Frozen,
                "source freeze is not durable"
            );
        }
        Phase::Activating => {
            let source = manifest(runtime, movement, ExportKind::Snapshot).await?;
            let destination = info(runtime, &movement.destination, &movement.partition).await?;
            ensure!(
                matches_transfer(&destination, movement, false)
                    && destination.phase == PartitionPhase::Staged
                    && destination.revision == source.revision
                    && destination.digest.as_ref() == Some(&source.digest)
                    && destination.bytes == source.bytes,
                "destination does not hold the complete frozen source image"
            );
        }
        Phase::Retiring => {
            let destination = info(runtime, &movement.destination, &movement.partition).await?;
            ensure!(
                matches_transfer(&destination, movement, false)
                    && destination.phase == PartitionPhase::Active,
                "destination activation is not durable"
            );
        }
        Phase::Complete => {
            let source = info(runtime, &movement.source, &movement.partition).await?;
            ensure!(
                matches_transfer(&source, movement, true)
                    && source.phase == PartitionPhase::Retired,
                "source retirement is not durable"
            );
        }
        Phase::Copying => anyhow::bail!("an existing move cannot go backwards"),
    }
    Ok(())
}

fn authorize(command: &PartitionCommand, placement: &Placement, local: &str) -> anyhow::Result<()> {
    ensure!(
        command.partition() == placement.partition,
        "partition differs from catalog ownership"
    );
    if let PartitionCommand::Create {
        epoch, operation, ..
    } = command
    {
        ensure!(
            placement.status == Status::Creating
                && placement.owner.id == local
                && placement.epoch == *epoch
                && placement.operation == *operation
                && placement.movement.is_none(),
            "catalog does not authorize this creation"
        );
        return Ok(());
    }
    if let PartitionCommand::Activate {
        epoch, operation, ..
    } = command
        && placement.movement.is_none()
    {
        ensure!(
            placement.status == Status::Creating
                && placement.owner.id == local
                && placement.epoch == *epoch
                && placement.operation == *operation,
            "catalog does not authorize initial activation"
        );
        return Ok(());
    }
    let movement = placement
        .movement
        .as_ref()
        .context("catalog has no matching move")?;
    let expected = transfer(movement);
    let allowed = match command {
        PartitionCommand::Capture { transfer, .. } => {
            local == movement.source.id && *transfer == expected && movement.phase == Phase::Copying
        }
        PartitionCommand::BeginCopy { transfer, .. } => {
            local == movement.destination.id
                && *transfer == expected
                && movement.phase == Phase::Copying
        }
        PartitionCommand::BeginDelta { transfer, .. }
        | PartitionCommand::FinalizeCopy { transfer, .. } => {
            local == movement.destination.id
                && *transfer == expected
                && movement.phase == Phase::Importing
        }
        PartitionCommand::Freeze { transfer, .. } => {
            local == movement.source.id
                && *transfer == expected
                && movement.phase == Phase::Freezing
        }
        PartitionCommand::BeginImport { transfer, .. } => {
            local == movement.destination.id
                && *transfer == expected
                && movement.phase == Phase::Importing
        }
        PartitionCommand::ImportChunk {
            epoch, operation, ..
        }
        | PartitionCommand::SealImport {
            epoch, operation, ..
        } => {
            local == movement.destination.id
                && *epoch == movement.epoch
                && *operation == movement.operation
                && matches!(movement.phase, Phase::Copying | Phase::Importing)
        }
        PartitionCommand::Activate {
            epoch, operation, ..
        } => {
            local == movement.destination.id
                && *epoch == movement.epoch
                && *operation == movement.operation
                && movement.phase == Phase::Activating
                && placement.owner.id == local
        }
        PartitionCommand::Retire { transfer } => {
            local == movement.source.id
                && *transfer == expected
                && movement.phase == Phase::Retiring
        }
        PartitionCommand::Create { .. } => false,
    };
    ensure!(
        allowed,
        "catalog does not authorize this ownership transition"
    );
    Ok(())
}

async fn check_import_keys(runtime: &Runtime, partition: &str) -> anyhow::Result<()> {
    if let Some(catalog) = runtime.consensus.partition_key_catalog(partition).await? {
        tokio::task::spawn_blocking(move || crate::crypto::managed::validate_ready(&catalog))
            .await??;
    }
    Ok(())
}

pub(super) async fn handle(runtime: &Runtime, request: ControlRequest) -> anyhow::Result<Value> {
    match request {
        ControlRequest::Locate => {
            runtime.consensus.read_query().await?;
            let metrics = runtime.consensus.metrics();
            let leader = metrics
                .current_leader
                .context("group has no current leader")?;
            let address = metrics
                .membership_config
                .nodes()
                .find_map(|(id, node)| (*id == leader).then(|| node.addr.clone()))
                .context("group leader is absent from membership")?;
            Ok(json!(LeaderLocation { address }))
        }
        ControlRequest::Ping => {
            // Control proposals run on the leader, so advertise the same
            // process's budgets rather than an arbitrary follower's policy.
            runtime.consensus.read_for_writer_with_term().await?;
            Ok(json!({"group":runtime.config.local_group,
                "rpc_max_bytes":runtime.consensus.limits().rpc_max_bytes,
                "transaction_max_bytes":runtime.consensus.limits().transaction_max_bytes,
                "http_max_body_bytes":super::super::tuning::settings()?.http_max_body_bytes}))
        }
        ControlRequest::Info { partition } => {
            let info = runtime.consensus.partition_info(&partition).await?;
            // The catalog observes Staged as readiness before ownership
            // cutover. An encrypted image alone must never prove readiness.
            if info.phase == PartitionPhase::Staged {
                check_import_keys(runtime, &partition).await?;
            }
            Ok(json!(info))
        }
        ControlRequest::Control { command } => {
            // Remote authorization is observed only after capturing local
            // leadership. The replicated full LeaderId fence rejects delayed
            // proposals from that leader after an election.
            let (_, term) = runtime.consensus.read_for_writer_with_term().await?;
            let placement = runtime.resolve(command.partition()).await?;
            authorize(&command, &placement, &runtime.config.local_group)?;
            let import = match &command {
                PartitionCommand::BeginCopy {
                    revision,
                    digest,
                    bytes,
                    ..
                } => Some((ExportKind::Base, *revision, digest, *bytes)),
                PartitionCommand::BeginDelta {
                    revision,
                    digest,
                    bytes,
                    ..
                } => Some((ExportKind::Delta, *revision, digest, *bytes)),
                PartitionCommand::BeginImport {
                    revision,
                    digest,
                    bytes,
                    ..
                }
                | PartitionCommand::FinalizeCopy {
                    revision,
                    digest,
                    bytes,
                    ..
                } => Some((ExportKind::Snapshot, *revision, digest, *bytes)),
                _ => None,
            };
            if let Some((image, revision, digest, bytes)) = import {
                let source = manifest(
                    runtime,
                    placement.movement.as_ref().expect("authorized transfer"),
                    image,
                )
                .await?;
                ensure!(
                    revision == source.revision
                        && *digest == source.digest
                        && bytes == source.bytes,
                    "import manifest differs from source"
                );
                if let PartitionCommand::BeginDelta { base_revision, .. } = &command {
                    ensure!(
                        Some(*base_revision) == source.base_revision,
                        "difference base revision differs from captured source"
                    );
                }
            }
            if let PartitionCommand::Activate { partition, .. } = &command {
                check_import_keys(runtime, partition).await?;
            }
            let sealed = matches!(&command, PartitionCommand::SealImport { .. });
            let leader_id = CommittedLeaderId::new(term, runtime.consensus.metrics().id);
            let retired = matches!(&command, PartitionCommand::Retire { .. });
            let result = runtime
                .consensus
                .control_partition(command, Some(leader_id))
                .await?;
            if sealed && result.phase == PartitionPhase::Staged {
                // Applied bytes stay staged while a missing/wrong wrapping
                // key is repaired; retrying the control operation is safe.
                check_import_keys(runtime, &result.partition).await?;
            }
            if retired {
                let mut cached = runtime.export.lock().await;
                if cached.as_ref().is_some_and(|cache| {
                    cache.partition == result.partition && cache.operation == result.operation
                }) {
                    *cached = None;
                }
            }
            Ok(json!(result))
        }
        ControlRequest::Export {
            partition,
            operation,
            image,
            offset,
            max_bytes,
        } => {
            let placement = runtime.resolve(&partition).await?;
            let movement = placement
                .movement
                .as_ref()
                .context("partition has no current move")?;
            ensure!(
                movement.operation == operation
                    && movement.source.id == runtime.config.local_group
                    && matches!(
                        movement.phase,
                        Phase::Copying
                            | Phase::Freezing
                            | Phase::Importing
                            | Phase::Activating
                            | Phase::Retiring
                    ),
                "catalog does not authorize this export"
            );
            let current = runtime.consensus.partition_info(&partition).await?;
            ensure!(
                matches_transfer(&current, movement, true)
                    && (current.phase == PartitionPhase::Frozen
                        || (image == ExportKind::Base && current.phase == PartitionPhase::Active)),
                "source is not ready for this export"
            );
            let mut cached = runtime.export.lock().await;
            if cached.as_ref().is_none_or(|cache| {
                cache.partition != partition
                    || cache.operation != operation
                    || cache.epoch != current.epoch
                    || cache.image != image
            }) {
                drop(cached.take());
                let admitted = runtime
                    .admission
                    .acquire(
                        format!("migration:{partition}"),
                        crate::service::admission::Class::Control,
                        0,
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{}: {}", error.code, error.message))?;
                let pool = runtime.admission.clone();
                let exported = runtime
                    .consensus
                    .export_partition_payload(
                        &partition,
                        &operation,
                        image,
                        move |bytes| {
                            pool.retain(crate::service::admission::Class::Control, bytes)
                                .map_err(|error| {
                                    anyhow::anyhow!("{}: {}", error.code, error.message)
                                })
                        },
                        admitted,
                    )
                    .await?;
                *cached = Some(ExportCache {
                    partition,
                    operation,
                    epoch: current.epoch,
                    revision: exported.revision,
                    base_revision: exported.base_revision,
                    image,
                    digest: crate::consensus::partition_image_digest(exported.bytes.as_bytes()),
                    bytes: Arc::new(ExportBytes {
                        text: exported.bytes,
                        _retained: exported.retained,
                    }),
                });
            }
            let cached = cached.as_ref().expect("initialized frozen export");
            let start = usize::try_from(offset.unwrap_or(0))
                .context("export offset exceeds platform representation")?;
            ensure!(
                start <= cached.bytes.len() && cached.bytes.is_char_boundary(start),
                "export offset is invalid"
            );
            let mut end = start;
            if offset.is_some() {
                ensure!(
                    max_bytes > 0 && max_bytes <= runtime.consensus.limits().rpc_max_bytes,
                    "export chunk exceeds configured control budgets"
                );
                end = start.saturating_add(max_bytes).min(cached.bytes.len());
                while !cached.bytes.is_char_boundary(end) {
                    end -= 1;
                }
                ensure!(
                    end > start || end == cached.bytes.len(),
                    "export chunk cannot contain the next UTF-8 character"
                );
            }
            let chunk = ExportChunk {
                revision: cached.revision,
                base_revision: cached.base_revision,
                digest: cached.digest.clone(),
                bytes: cached.bytes.len() as u64,
                offset: start as u64,
                data: cached.bytes[start..end].into(),
                next_offset: end as u64,
            };
            ensure!(
                crate::consensus::encoded_json_len(&super::PeerResponse {
                    group: runtime.config.local_group.clone(),
                    body: &chunk,
                })? <= runtime.consensus.limits().rpc_max_bytes,
                "export response exceeds configured RPC budget"
            );
            Ok(json!(chunk))
        }
    }
}

async fn control(
    runtime: &Runtime,
    group: &Group,
    command: PartitionCommand,
) -> anyhow::Result<PartitionInfo> {
    runtime
        .send(group, CONTROL_PATH, &ControlRequest::Control { command })
        .await
}

async fn copy_payload(
    runtime: &Runtime,
    movement: &Move,
    kind: ExportKind,
    image: &ExportChunk,
    command: PartitionCommand,
) -> anyhow::Result<()> {
    let mut imported = control(runtime, &movement.destination, command).await?;
    if imported.phase == PartitionPhase::Importing {
        let max_bytes = super::budgets::chunk_capacity(runtime, movement).await?;
        while imported.received_bytes < image.bytes {
            let chunk: ExportChunk = runtime
                .send(
                    &movement.source,
                    CONTROL_PATH,
                    &ControlRequest::Export {
                        partition: movement.partition.clone(),
                        operation: movement.operation.clone(),
                        image: kind,
                        offset: Some(imported.received_bytes),
                        max_bytes,
                    },
                )
                .await?;
            ensure!(
                chunk.digest == image.digest
                    && chunk.revision == image.revision
                    && chunk.bytes == image.bytes
                    && chunk.offset == imported.received_bytes
                    && chunk.next_offset == chunk.offset + chunk.data.len() as u64
                    && !chunk.data.is_empty(),
                "export chunk differs from immutable manifest"
            );
            imported = control(
                runtime,
                &movement.destination,
                PartitionCommand::ImportChunk {
                    partition: movement.partition.clone(),
                    epoch: movement.epoch,
                    operation: movement.operation.clone(),
                    index: imported.next_chunk,
                    data: chunk.data,
                },
            )
            .await?;
        }
    }
    control(
        runtime,
        &movement.destination,
        PartitionCommand::SealImport {
            partition: movement.partition.clone(),
            epoch: movement.epoch,
            operation: movement.operation.clone(),
        },
    )
    .await?;
    Ok(())
}

async fn drive(runtime: &Runtime, owner: Placement) -> anyhow::Result<()> {
    if owner.status == Status::Creating {
        control(
            runtime,
            &owner.owner,
            PartitionCommand::Create {
                partition: owner.partition.clone(),
                epoch: owner.epoch,
                operation: owner.operation.clone(),
            },
        )
        .await?;
        control(
            runtime,
            &owner.owner,
            PartitionCommand::Activate {
                partition: owner.partition.clone(),
                epoch: owner.epoch,
                operation: owner.operation.clone(),
            },
        )
        .await?;
        runtime
            .catalog(CatalogRequest::Ready {
                partition: owner.partition,
                operation: owner.operation,
            })
            .await?;
        return Ok(());
    }
    let Some(movement) = owner.movement else {
        return Ok(());
    };
    let next = match movement.phase {
        Phase::Copying => {
            let current = info(runtime, &movement.source, &movement.partition).await?;
            control(
                runtime,
                &movement.source,
                PartitionCommand::Capture {
                    transfer: transfer(&movement),
                    expected_revision: current.revision,
                    max_bytes: runtime.config.base_max_bytes,
                },
            )
            .await?;
            let base = manifest(runtime, &movement, ExportKind::Base).await?;
            copy_payload(
                runtime,
                &movement,
                ExportKind::Base,
                &base,
                PartitionCommand::BeginCopy {
                    transfer: transfer(&movement),
                    revision: base.revision,
                    digest: base.digest.clone(),
                    bytes: base.bytes,
                },
            )
            .await?;
            Phase::Freezing
        }
        Phase::Freezing => {
            let current = info(runtime, &movement.source, &movement.partition).await?;
            control(
                runtime,
                &movement.source,
                PartitionCommand::Freeze {
                    transfer: transfer(&movement),
                    expected_revision: current.revision,
                },
            )
            .await?;
            Phase::Importing
        }
        Phase::Importing => {
            let image = manifest(runtime, &movement, ExportKind::Snapshot).await?;
            let current = info(runtime, &movement.destination, &movement.partition).await?;
            if current.phase == PartitionPhase::Staged && current.revision == image.revision {
                // Resumed after final seal but before catalog cutover.
            } else if current.phase == PartitionPhase::Copied && current.revision == image.revision
            {
                control(
                    runtime,
                    &movement.destination,
                    PartitionCommand::FinalizeCopy {
                        transfer: transfer(&movement),
                        revision: image.revision,
                        digest: image.digest.clone(),
                        bytes: image.bytes,
                    },
                )
                .await?;
            } else {
                let difference = manifest(runtime, &movement, ExportKind::Delta).await?;
                let use_delta = if current.phase == PartitionPhase::Importing {
                    current.digest.as_ref() == Some(&difference.digest)
                } else {
                    difference.bytes <= image.bytes
                        && runtime
                            .config
                            .tail_max_bytes
                            .is_none_or(|limit| difference.bytes <= limit)
                };
                let (kind, selected, command) = if use_delta {
                    let command = PartitionCommand::BeginDelta {
                        transfer: transfer(&movement),
                        base_revision: difference
                            .base_revision
                            .context("difference lost base revision")?,
                        revision: difference.revision,
                        digest: difference.digest.clone(),
                        bytes: difference.bytes,
                    };
                    (ExportKind::Delta, difference, command)
                } else {
                    let command = PartitionCommand::BeginImport {
                        transfer: transfer(&movement),
                        revision: image.revision,
                        digest: image.digest.clone(),
                        bytes: image.bytes,
                    };
                    (ExportKind::Snapshot, image, command)
                };
                copy_payload(runtime, &movement, kind, &selected, command).await?;
            }
            Phase::Activating
        }
        Phase::Activating => {
            control(
                runtime,
                &movement.destination,
                PartitionCommand::Activate {
                    partition: movement.partition.clone(),
                    epoch: movement.epoch,
                    operation: movement.operation.clone(),
                },
            )
            .await?;
            Phase::Retiring
        }
        Phase::Retiring => {
            control(
                runtime,
                &movement.source,
                PartitionCommand::Retire {
                    transfer: transfer(&movement),
                },
            )
            .await?;
            Phase::Complete
        }
        Phase::Complete => return Ok(()),
    };
    runtime
        .catalog(CatalogRequest::Advance {
            partition: movement.partition,
            operation: movement.operation,
            phase: next,
        })
        .await?;
    Ok(())
}

pub(super) async fn run(weak: Weak<Runtime>) {
    let cadence = super::super::tuning::settings()
        .expect("validated coordinator cadence")
        .maintenance_interval;
    let mut timer = tokio::time::interval(cadence);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        let Some(runtime) = weak.upgrade() else {
            return;
        };
        // A former leader may never receive the final Retire RPC itself. Its
        // replicated phase still invalidates an image and releases its quota.
        if let Ok(mut cached) = runtime.export.try_lock() {
            let obsolete = cached.as_ref().is_some_and(|cache| {
                runtime.consensus.metrics().state != openraft::ServerState::Leader
                    || !runtime
                        .consensus
                        .partition_info_local(&cache.partition)
                        .is_ok_and(|info| {
                            info.operation == cache.operation
                                && info.epoch == cache.epoch
                                && (info.phase == PartitionPhase::Frozen
                                    || (cache.image == ExportKind::Base
                                        && info.phase == PartitionPhase::Active
                                        && info.base_bytes > 0))
                        })
            });
            if obsolete {
                *cached = None;
            }
        }
        if runtime.config.local_group != runtime.config.catalog_group
            || runtime.consensus.metrics().state != openraft::ServerState::Leader
        {
            continue;
        }
        let cycle = async {
            let view: View =
                serde_json::from_value(catalog::apply(&runtime, CatalogRequest::List).await?)?;
            for owner in view.partitions {
                if (owner.status == Status::Creating
                    || owner
                        .movement
                        .as_ref()
                        .is_some_and(|m| m.phase != Phase::Complete))
                    && let Err(error) = drive(&runtime, owner).await
                {
                    tracing::warn!(%error, "partition move will resume");
                }
            }
            if let Some(plan) = view.rebalance
                && !plan.complete
            {
                runtime
                    .catalog(CatalogRequest::StepRebalance {
                        operation: plan.operation,
                    })
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = cycle {
            tracing::warn!(%error, "partition coordinator will retry");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controls_require_the_exact_catalog_phase_owner_epoch_and_operation() {
        let movement = Move {
            operation: "move".into(),
            partition: "tenant".into(),
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
        };
        let mut owner = Placement {
            partition: "tenant".into(),
            epoch: 1,
            owner: movement.source.clone(),
            status: Status::Moving,
            operation: movement.operation.clone(),
            movement: Some(movement.clone()),
        };
        let freeze = PartitionCommand::Freeze {
            transfer: transfer(&movement),
            expected_revision: 7,
        };
        assert!(authorize(&freeze, &owner, "a").is_ok());
        assert!(authorize(&freeze, &owner, "b").is_err());
        let activate = PartitionCommand::Activate {
            partition: "tenant".into(),
            epoch: 2,
            operation: "move".into(),
        };
        assert!(authorize(&activate, &owner, "b").is_err());
        owner.owner = movement.destination.clone();
        owner.epoch = 2;
        owner.movement.as_mut().unwrap().phase = Phase::Activating;
        assert!(authorize(&activate, &owner, "b").is_ok());
        assert!(authorize(&freeze, &owner, "a").is_err());
        assert!(
            authorize(
                &PartitionCommand::Activate {
                    partition: "tenant".into(),
                    epoch: 1,
                    operation: "move".into()
                },
                &owner,
                "b"
            )
            .is_err()
        );
        owner.movement.as_mut().unwrap().phase = Phase::Complete;
        assert!(authorize(&activate, &owner, "b").is_err());
    }
}

#[cfg(test)]
mod export_budget_tests {
    use super::*;
    use crate::service::admission::{Class, Pool};

    #[test]
    fn shared_cached_image_keeps_its_budget_until_last_reference_drops() {
        let pool = Pool::new([1, 1], [1, 1], [1024, 1024], 1);
        let image = Arc::new(ExportBytes {
            text: "image".into(),
            _retained: pool.retain(Class::Control, 600).unwrap(),
        });
        let in_flight = image.clone();
        drop(image);
        assert_eq!(pool.metrics()["classes"][1]["retainedInputBytes"], 600);
        assert!(pool.retain(Class::Control, 500).is_err());
        drop(in_flight);
        assert_eq!(pool.metrics()["classes"][1]["retainedInputBytes"], 0);
    }
}
