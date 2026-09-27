//! Durable transaction routing pins logical placement for the protocol lifetime.
use super::*;
use crate::service::partitions::{
    self,
    catalog::{self, Group},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub(super) struct Target {
    pub group: String,
    pub partition: Option<String>,
    pub epoch: u64,
    // Peers are durable bootstrap locations for groups registered after startup.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<String>,
}
impl From<&str> for Target {
    fn from(group: &str) -> Self {
        Self {
            group: group.into(),
            partition: None,
            epoch: 0,
            addresses: Vec::new(),
        }
    }
}
impl From<String> for Target {
    fn from(group: String) -> Self {
        Self::from(group.as_str())
    }
}
impl Target {
    pub fn label(&self) -> &str {
        self.partition.as_deref().unwrap_or(&self.group)
    }
    fn same_location(&self, other: &Self) -> bool {
        self.group == other.group && self.partition == other.partition && self.epoch == other.epoch
    }
}
pub(super) fn local_target(app: &App) -> Result<Target, ApiError> {
    let mut target = Target::from(app.cross_group.name()?);
    if let Some(binding) = app.consensus.partition_binding() {
        target.partition = Some(binding.partition.clone());
        target.epoch = binding.epoch;
    }
    Ok(target)
}
pub(super) fn is_local(app: &App, target: &Target) -> Result<bool, ApiError> {
    Ok(target.same_location(&local_target(app)?))
}
fn runtime(app: &App) -> Result<Arc<partitions::Runtime>, ApiError> {
    if let Some(runtime) = app.cross_group.partitions.get() {
        return Ok(runtime.clone());
    }
    let runtime = partitions::Runtime::new(
        app.consensus.physical(),
        app.admin_token.clone(),
        app.admission.clone(),
    )
    .map_err(unavailable)?
    .ok_or_else(|| {
        invalid(anyhow::anyhow!(
            "logical transaction targets require FLOWER_CATALOG_GROUP"
        ))
    })?;
    Ok(app.cross_group.partitions.get_or_init(|| runtime).clone())
}
fn placed(partition: &str, placement: catalog::Placement) -> Target {
    Target {
        group: placement.owner.id,
        partition: Some(partition.into()),
        epoch: placement.epoch,
        addresses: placement.owner.addresses,
    }
}
pub(super) async fn resolve(app: &App, record: &mut Coordinator) -> Result<(), ApiError> {
    let partitions: BTreeSet<&str> = record
        .calls
        .iter()
        .filter_map(|call| call.group.partition.as_deref())
        .collect();
    if record.coordinator.partition.is_none() && partitions.is_empty() {
        return Ok(());
    }
    let runtime = runtime(app)?;
    // The caller holds its writer lock, so look up every placement at once.
    let targets = futures_util::future::try_join_all(partitions.into_iter().map(|partition| {
        let runtime = &runtime;
        async move {
            let placement = runtime.resolve(partition).await.map_err(unavailable)?;
            if !catalog::serving(&placement) {
                return Err(conflict(
                    "transaction target is moving; retry after cutover",
                ));
            }
            Ok((partition.to_owned(), placed(partition, placement)))
        }
    }));
    let own = async {
        match record.coordinator.partition.as_deref() {
            Some(partition) => runtime
                .resolve(partition)
                .await
                .map(Some)
                .map_err(unavailable),
            None => Ok(None),
        }
    };
    let (targets, own) = tokio::try_join!(targets, own)?;
    let pinned: BTreeMap<String, Target> = targets.into_iter().collect();
    for call in &mut record.calls {
        if let Some(partition) = &call.group.partition {
            call.group = pinned[partition].clone();
        }
    }
    // Give the coordinator's own stable identity the same durable seed list.
    if let Some(placement) = own {
        if !catalog::serving(&placement)
            || placement.epoch != record.coordinator.epoch
            || placement.owner.id != record.coordinator.group
        {
            return Err(conflict("transaction coordinator placement changed"));
        }
        record.coordinator.addresses = placement.owner.addresses;
    }
    Ok(())
}
pub(super) async fn contact_partition(
    app: &App,
    target: &Target,
    action: &str,
    reference: &Reference,
) -> Result<Value, ApiError> {
    contact_partition_value(app, target, action, json!(reference)).await
}
pub(super) async fn contact_partition_value(
    app: &App,
    target: &Target,
    action: &str,
    input: Value,
) -> Result<Value, ApiError> {
    let addresses = if target.addresses.is_empty() {
        app.cross_group
            .groups
            .get(&target.group)
            .cloned()
            .ok_or_else(|| conflict("transaction target has no bootstrap peers"))?
    } else {
        target.addresses.clone()
    };
    let input = json!({"partition":target.partition,"epoch":target.epoch,"operation":format!("tx-{action}"),"input":input});
    runtime(app)?
        .send(
            &Group {
                id: target.group.clone(),
                addresses,
            },
            "/raft/partitions/invoke",
            &input,
        )
        .await
        .map_err(|error| {
            if engine_failure(&error).is_some() {
                evaluation_error(error)
            } else {
                unavailable(error)
            }
        })
}

pub(super) async fn current(app: &App, target: &Target) -> Result<Target, ApiError> {
    let Some(partition) = &target.partition else {
        return Ok(target.clone());
    };
    let placement = runtime(app)?
        .resolve(partition)
        .await
        .map_err(unavailable)?;
    if !catalog::serving(&placement) {
        return Err(conflict("closure target is moving; retry after cutover"));
    }
    Ok(placed(partition, placement))
}
