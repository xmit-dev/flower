//! A partition-local producer cache. Scope keys contain the complete admitted
//! principal, invocation and deployment; credentials never enter this module.
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::body::Bytes;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{broadcast, watch};

use super::{failure, json_patch, wakes};
use crate::{
    consensus::{
        Consensus, Progress, Snapshot,
        changes::{Changes, Scope},
    },
    evaluator::{DependencyCertificate, HttpMethod},
    service::{
        ApiError, App, QueryAdmission, QueryEvaluation, QueryResult, Validity, admission,
        evaluate_query_authorized, unavailable,
    },
};

#[derive(Default)]
pub(crate) struct Registry(Arc<RegistryInner>);

struct RegistryInner {
    hubs: Mutex<HashMap<String, Weak<Hub>>>,
    /// What open watches hold: their inputs, hubs and current values.
    budget: Arc<admission::Pool>,
    wakes: Mutex<wakes::Wakes>,
    /// Raised when every watch must refresh: this replica's freshness came
    /// into doubt or it stopped, or its view of writes has a gap.
    everything: watch::Sender<u64>,
    dispatching: OnceLock<()>,
    next_hub: AtomicU64,
}

impl Default for RegistryInner {
    fn default() -> Self {
        let bytes = crate::service::tuning::settings()
            .expect("validated watch budget")
            .watch_retained_bytes;
        Self {
            hubs: Mutex::default(),
            budget: admission::Pool::watches(bytes),
            wakes: Mutex::default(),
            everything: watch::Sender::new(0),
            dispatching: OnceLock::new(),
            next_hub: AtomicU64::new(0),
        }
    }
}

impl RegistryInner {
    fn wakes(&self) -> std::sync::MutexGuard<'_, wakes::Wakes> {
        self.wakes.lock().expect("watch wakes mutex")
    }

    fn refresh_everything(&self) {
        self.everything.send_modify(|generation| *generation += 1);
    }

    /// Everything published before now is unknown: wake every hub.
    fn restart(&self, consensus: &Consensus) {
        self.wakes()
            .restart(consensus.published_revision().unwrap_or(0));
        self.refresh_everything();
    }
}

/// Route each publication of this registry's state to the hubs it touches.
async fn dispatch(
    registry: Weak<RegistryInner>,
    consensus: Consensus,
    mut changes: broadcast::Receiver<Arc<Changes>>,
    mut progress: watch::Receiver<Progress>,
    mut alive: watch::Receiver<u64>,
) {
    let partition = consensus
        .partition_binding()
        .map(|binding| binding.partition.clone());
    let mut observed = progress.borrow_and_update().clone();
    loop {
        tokio::select! {
            received = changes.recv() => {
                let Some(registry) = registry.upgrade() else { return };
                match received {
                    Ok(changes) if changes.scope == Scope::All => registry.restart(&consensus),
                    Ok(changes) if changes.concern(partition.as_deref()) => {
                        registry.wakes().publish(changes);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => registry.restart(&consensus),
                    Err(broadcast::error::RecvError::Closed) => {
                        registry.refresh_everything();
                        return;
                    }
                }
            }
            changed = progress.changed() => {
                let Some(registry) = registry.upgrade() else { return };
                if changed.is_err() {
                    registry.refresh_everything();
                    return;
                }
                let latest = progress.borrow_and_update().clone();
                if latest.suspicion != observed.suspicion || !latest.running {
                    registry.refresh_everything();
                }
                observed = latest;
            }
            // Dropping the registry drops this sender.
            closed = alive.changed() => if closed.is_err() { return },
        }
    }
}

pub(super) struct Authorized {
    pub state: Snapshot,
    pub method: HttpMethod,
    pub input: Value,
    pub principal: Value,
    pub permit: admission::Permit,
}

impl Authorized {
    pub fn scope(&self) -> String {
        // The registry itself is owned by one resolved partition App. Include
        // alias as authorization can differ between aliases of one method.
        crate::evaluator::hash(
            &serde_json::to_vec(&json!({
                "invocation":self.input,"method":self.method,"principal":self.principal,
                "deployment":self.state.data.get("bundle").and_then(|bundle|bundle.get("hash")),
            }))
            .expect("watch scope is JSON"),
        )
    }
}

impl Registry {
    /// Subscribe before the first snapshot a watch reads: this changes when
    /// every watch must refresh.
    pub(super) fn everything(&self, app: &App) -> watch::Receiver<u64> {
        self.dispatch(app);
        self.0.everything.subscribe()
    }

    fn dispatch(&self, app: &App) {
        self.0.dispatching.get_or_init(|| {
            // Subscribe first: what the publication read next misses, the
            // restart treats as unknown.
            let changes = app.consensus.changes();
            self.0
                .wakes()
                .restart(app.consensus.published_revision().unwrap_or(0));
            tokio::spawn(dispatch(
                Arc::downgrade(&self.0),
                app.consensus.clone(),
                changes,
                app.consensus.progress(),
                self.0.everything.subscribe(),
            ));
        });
    }

    /// Hold `bytes` for as long as a watch keeps what they measure.
    pub(super) fn retain(&self, bytes: usize) -> Result<admission::Input, ApiError> {
        self.0.budget.retain(admission::Class::User, bytes)
    }

    /// Bytes open watches hold, and their budget.
    pub(crate) fn metrics(&self) -> Value {
        let (retained, budget) = self.0.budget.retained();
        json!({"retainedBytes":retained,"budgetBytes":budget})
    }

    /// One watch's own wakes: writes to what its access decision read.
    pub(super) fn subscriber(&self) -> Subscriber {
        Subscriber {
            id: self.0.next_hub.fetch_add(1, Ordering::Relaxed),
            signal: Arc::new(watch::Sender::new(0)),
            registry: Arc::downgrade(&self.0),
        }
    }

    pub(super) fn get(&self, app: &App, scope: &str) -> Result<Arc<Hub>, ApiError> {
        self.dispatch(app);
        let mut entries = self.0.hubs.lock().expect("watch registry mutex");
        if let Some(hub) = entries.get(scope).and_then(Weak::upgrade) {
            return Ok(hub);
        }
        let retained = self.retain(scope.len().saturating_add(1024))?;
        let hub = Arc::new(Hub {
            scope: scope.into(),
            id: self.0.next_hub.fetch_add(1, Ordering::Relaxed),
            signal: Arc::new(watch::Sender::new(0)),
            registry: Arc::downgrade(&self.0),
            state: tokio::sync::Mutex::new(State::default()),
            _retained: retained,
        });
        entries.insert(scope.into(), Arc::downgrade(&hub));
        Ok(hub)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.0.hubs.lock().unwrap().len()
    }
}

/// A watch whose access decision can change with the records its
/// authorization read, when those are not what its result read.
pub(super) struct Subscriber {
    id: u64,
    signal: wakes::Signal,
    registry: Weak<RegistryInner>,
}

impl Subscriber {
    /// The latest revision whose writes touched what access read.
    pub fn signal(&self) -> watch::Receiver<u64> {
        self.signal.subscribe()
    }

    /// Wake for writes after `revision` to what an access decision read,
    /// or to anything when it is not known.
    pub fn observe(&self, revision: u64, access: Option<&DependencyCertificate>) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut wakes = registry.wakes();
        match access {
            // Code and policy changes wake its hub.
            Some(access) if access.observations().next().is_none() => wakes.remove(self.id),
            access => wakes.register(self.id, &self.signal, revision, access),
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.wakes().remove(self.id);
        }
    }
}

pub(super) struct Hub {
    pub scope: String,
    id: u64,
    signal: wakes::Signal,
    registry: Weak<RegistryInner>,
    state: tokio::sync::Mutex<State>,
    _retained: admission::Input,
}

#[derive(Default)]
struct State {
    current: Option<Arc<Frame>>,
    started: Option<Instant>,
    #[cfg(test)]
    evaluations: usize,
}

impl Drop for Hub {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            {
                let mut entries = registry.hubs.lock().expect("watch registry mutex");
                // A replacement can have been installed between the final strong
                // reference disappearing and this destructor acquiring the lock.
                if entries
                    .get(&self.scope)
                    .is_some_and(|entry| entry.strong_count() == 0)
                {
                    entries.remove(&self.scope);
                }
            }
            registry.wakes().remove(self.id);
        }
    }
}

impl Hub {
    /// The latest revision whose writes touched this hub's result. A
    /// subscriber holding a result from before it refreshes.
    pub fn signal(&self) -> watch::Receiver<u64> {
        self.signal.subscribe()
    }

    // None means another admitted subscriber advanced past this authorization
    // snapshot. The caller must reacquire and reauthorize, never borrow a newer
    // result under an older policy decision.
    pub async fn refresh(
        &self,
        app: &App,
        authorized: Authorized,
        joined: Option<Instant>,
        coalesce: bool,
    ) -> Result<Option<Arc<Frame>>, ApiError> {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(_) => {
                // Subscriber input already has a byte lease. Release its root
                // and execution slot while another producer evaluates/encodes.
                drop(authorized);
                drop(self.state.lock().await);
                return Ok(None);
            }
        };
        if let Some(current) = &state.current {
            if current.query.revision > authorized.state.revision {
                return Ok(None);
            }
            let refreshed = state.started.expect("initialized watch producer");
            let refresh = super::super::tuning::settings()
                .map_err(unavailable)?
                .watch_refresh;
            let fresh_join = joined.is_none_or(|joined| refreshed >= joined);
            let reusable = current.query.revision == authorized.state.revision
                && match current.query.validity {
                    Validity::Stable => true,
                    // Exact until then, for newly joined subscribers too.
                    Validity::Until(time) => {
                        app.clock.sample(&authorized.state).map_err(unavailable)? < time
                    }
                    Validity::Polled => fresh_join && refreshed.elapsed() < refresh,
                };
            if reusable {
                return Ok(Some(current.clone()));
            }
        }
        let started = Instant::now();
        let now = app.clock.sample(&authorized.state).map_err(unavailable)?;
        // Keep one reservation through native execution, diffing and encoding;
        // cloning a Permit does not acquire a second worker or memory budget.
        let result = evaluate_query_authorized(
            app,
            authorized.state,
            &authorized.input,
            authorized.method,
            now,
            QueryAdmission {
                permit: authorized.permit,
                coalesce,
            },
            authorized.principal,
        )
        .await?;
        let QueryEvaluation::Ready(next, permit) = result else {
            return Ok(None);
        };
        let previous = state.current.clone();
        let mut frame = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            prepare_sync(previous.as_deref(), next)
        })
        .await
        .map_err(|error| failure("WORKER_FAILED", &error.to_string()))??;
        let bytes = FRAME_BYTES
            .saturating_add(frame.snapshot.len())
            .saturating_add(frame.patch.as_ref().map_or(0, Bytes::len));
        frame.retained = Some(app.watch_hubs.retain(bytes)?);
        let frame = Arc::new(frame);
        if let Some(registry) = self.registry.upgrade() {
            registry.wakes().register(
                self.id,
                &self.signal,
                frame.query.revision,
                frame.query.certificate.as_deref(),
            );
        }
        state.current = Some(frame.clone());
        state.started = Some(started);
        #[cfg(test)]
        {
            state.evaluations += 1;
        }
        Ok(Some(frame))
    }

    #[cfg(test)]
    pub async fn evaluations(&self) -> usize {
        self.state.lock().await.evaluations
    }
}

/// A frame's own allocations besides its events, and its result's.
const FRAME_BYTES: usize = 256;

/// A hub's current result, encoded once for every subscriber. It keeps its
/// value only inside the snapshot event, and parses it back to diff the next.
pub(super) struct Frame {
    pub retained: Option<admission::Input>,
    /// Its value is `Null`: see `encoded`.
    pub query: QueryResult,
    pub sequence: u64,
    pub snapshot: Bytes,
    /// Where the encoded value lies in `snapshot`.
    value: std::ops::Range<usize>,
    pub patch: Option<Bytes>,
}

impl Frame {
    /// The value, as JSON.
    fn encoded(&self) -> &[u8] {
        &self.snapshot[self.value.clone()]
    }

    pub fn bytes_after(&self, previous: Option<u64>) -> Option<Bytes> {
        if previous.is_some_and(|previous| previous >= self.sequence) {
            return None;
        }
        if previous.is_some_and(|previous| previous.checked_add(1) == Some(self.sequence))
            && let Some(patch) = &self.patch
        {
            return Some(patch.clone());
        }
        // Joining an existing producer or skipping updates resets the complete
        // value. Only consecutive subscribers can consume a delta.
        Some(self.snapshot.clone())
    }
}

pub(super) fn event(name: &str, sequence: u64, data: &str) -> Bytes {
    Bytes::from(format!("event: {name}\nid: {sequence}\ndata: {data}\n\n"))
}

pub(super) fn prepare_sync(
    previous: Option<&Frame>,
    mut next: QueryResult,
) -> Result<Frame, ApiError> {
    let encoded = serde_json::to_string(&next.value)
        .map_err(|error| failure("WATCH_ENCODING_FAILED", &error.to_string()))?;
    if previous.is_some_and(|previous| next.revision < previous.query.revision) {
        return Err(failure("UNAVAILABLE", "watch revision moved backwards"));
    }
    let changed = previous.is_none_or(|previous| previous.encoded() != encoded.as_bytes());
    let sequence = match previous {
        Some(previous) if !changed => previous.sequence,
        Some(previous) => previous
            .sequence
            .checked_add(1)
            .filter(|sequence| *sequence <= 9_007_199_254_740_991)
            .ok_or_else(|| {
                failure(
                    "WATCH_SEQUENCE_EXHAUSTED",
                    "watch sequence exhausted; reconnect for a fresh snapshot",
                )
            })?,
        None => 0,
    };
    let framing = format!("event: snapshot\nid: {sequence}\ndata: ");
    let head = format!(
        "{framing}{{\"sequence\":{sequence},\"revision\":{},\"value\":",
        next.revision
    );
    let value = head.len()..head.len() + encoded.len();
    let snapshot = format!("{head}{encoded}}}\n\n");
    // The payload alone, as `event` frames patches.
    let snapshot_payload = snapshot.len() - framing.len() - 2;
    let mut patch_event = None;
    if changed && let Some(previous) = previous {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Patch<'a> {
            sequence: u64,
            base_sequence: u64,
            revision: u64,
            patch: Vec<json_patch::Operation<'a>>,
        }
        let before: Value = serde_json::from_slice(previous.encoded())
            .map_err(|error| failure("WATCH_ENCODING_FAILED", &error.to_string()))?;
        if let Some(patch) = json_patch::diff(&before, &next.value) {
            let patch = serde_json::to_string(&Patch {
                sequence,
                base_sequence: previous.sequence,
                revision: next.revision,
                patch,
            })
            .map_err(|error| failure("WATCH_ENCODING_FAILED", &error.to_string()))?;
            if patch.len() < snapshot_payload {
                patch_event = Some(event("patch", sequence, &patch));
            }
        }
    }
    next.value = Value::Null;
    Ok(Frame {
        retained: None,
        query: next,
        sequence,
        snapshot: Bytes::from(snapshot),
        value,
        patch: patch_event,
    })
}
