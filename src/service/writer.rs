//! A bounded leader queue amortizes one durable Raft flush over several methods.
//! Each method sees preceding staged writes, has its own receipt and revision,
//! and is acknowledged only after durable quorum commitment and local application.

mod batching;
mod deployment;
mod observability;
mod serial;
mod shared;
mod speculation;
use super::*;
use crate::consensus::{ApplyResult, Receipt, encoded_json_len};
use batching::{Controller, Decision};
use shared::SharedCommit;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::oneshot;
use tracing::Instrument;

#[cfg(test)]
mod tests;

pub(super) struct Pending {
    input: PendingInput,
    deployment: bool,
    enqueued: Instant,
    reply: oneshot::Sender<Result<Value, ApiError>>,
}

struct Body {
    value: Value,
    _retained: Option<admission::Input>,
}
struct InputState {
    body: Option<Arc<Body>>,
    begun: bool,
    staged: Option<Prepared>,
    queued: Option<Instant>,
}
#[derive(Clone)]
struct PendingInput {
    state: Arc<std::sync::Mutex<InputState>>,
    canceled: Arc<tokio::sync::Notify>,
    trace: tracing::Span,
}
impl PendingInput {
    fn new(value: Value, retained: Option<admission::Input>) -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(InputState {
                body: Some(Arc::new(Body {
                    value,
                    _retained: retained,
                })),
                begun: false,
                staged: None,
                queued: None,
            })),
            canceled: Arc::new(tokio::sync::Notify::new()),
            trace: if crate::telemetry::enabled() {
                tracing::Span::current()
            } else {
                tracing::Span::none()
            },
        }
    }
    fn enqueued(&self) {
        if crate::telemetry::enabled() {
            self.state.lock().expect("pending input").queued = Some(Instant::now());
        }
    }
    fn dequeued(&self) {
        if crate::telemetry::enabled()
            && let Some(queued) = self.state.lock().expect("pending input").queued.take()
        {
            observability::stage("queue_wait", queued.elapsed(), "request");
            self.trace.in_scope(|| tracing::info!(target: "flower::otel", queue_wait_seconds = queued.elapsed().as_secs_f64(), "writer preparation started"));
        }
    }
    fn request_id(&self) -> Option<String> {
        self.state
            .lock()
            .expect("pending input")
            .body
            .as_ref()
            .map(|body| body.value["requestId"].as_str().unwrap().to_owned())
    }
    fn method(&self) -> String {
        self.state
            .lock()
            .expect("pending input")
            .body
            .as_ref()
            .and_then(|body| body.value["name"].as_str())
            .unwrap_or("__deployment")
            .to_owned()
    }
    fn begin(&self) -> Result<Arc<Body>, ApiError> {
        let mut state = self.state.lock().expect("pending input");
        let body = state
            .body
            .clone()
            .ok_or_else(|| unavailable(anyhow::anyhow!("request canceled before preparation")))?;
        state.begun = true;
        Ok(body)
    }
    fn cancel(&self) {
        let removed = {
            let mut state = self.state.lock().expect("pending input");
            if state.begun {
                None
            } else {
                Some((state.body.take(), state.staged.take()))
            }
        };
        drop(removed);
        self.canceled.notify_one();
    }
}
#[cfg(test)]
impl From<Value> for PendingInput {
    fn from(value: Value) -> Self {
        Self::new(value, None)
    }
}
struct Submitted(PendingInput);
impl Drop for Submitted {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn submit(app: &App, input: Value, deployment: bool) -> Result<Value, ApiError> {
    if !crate::telemetry::enabled() {
        return submit_inner(app, input, deployment).await;
    }
    let trace = tracing::info_span!(target: "flower::otel", "flower.writer.submit",
        kind = observability::kind(deployment), status = tracing::field::Empty);
    let mut observation = observability::Submission::new(deployment, trace.clone());
    let result = submit_inner(app, input, deployment)
        .instrument(trace.clone())
        .await;
    observation.finish(&result);
    result
}

async fn submit_inner(app: &App, input: Value, deployment: bool) -> Result<Value, ApiError> {
    let retained = app.admission.retain(
        if deployment {
            admission::Class::Control
        } else {
            admission::Class::User
        },
        admission::input_bytes(&input),
    )?;
    let input = PendingInput::new(input, Some(retained));
    let _submitted = Submitted(input.clone());
    if deployment
        && deployment::online(&input)?
        && let Some(receipt) = deployment::stage(app, &input).await?
    {
        return Ok(receipt);
    }
    let (reply, receive) = oneshot::channel();
    input.enqueued();
    app.writer_queue
        .try_send(Pending {
            input,
            deployment,
            enqueued: Instant::now(),
            reply,
        })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                observability::rejected("full");
                unavailable(anyhow::anyhow!(
                    "mutation queue full; retry the same request ID"
                ))
            }
            mpsc::error::TrySendError::Closed(_) => {
                observability::rejected("closed");
                unavailable(anyhow::anyhow!("mutation queue closed"))
            }
        })?;
    receive
        .await
        .map_err(|_| unavailable(anyhow::anyhow!("mutation worker stopped")))?
}

#[cfg(test)]
fn test_budget(normal: Duration, hooks: Option<&tests::Hooks>) -> Duration {
    // Deliberately held test gates must not consume production batching windows.
    // Ungated tests retain the same timing limits as the production actor.
    if hooks.is_some() {
        Duration::from_secs(60)
    } else {
        normal
    }
}

pub(super) async fn run(weak: Weak<App>, receiver: mpsc::Receiver<Pending>) {
    run_actor(
        weak,
        receiver,
        #[cfg(test)]
        true,
    )
    .await;
}

#[cfg(test)]
pub(super) async fn run_without_maintenance(weak: Weak<App>, receiver: mpsc::Receiver<Pending>) {
    // Maintenance unit tests drive callbacks explicitly. The same queue actor
    // must not also advance application revisions on its background timer.
    run_actor(weak, receiver, false).await;
}

async fn run_actor(
    weak: Weak<App>,
    mut receiver: mpsc::Receiver<Pending>,
    #[cfg(test)] automatic_maintenance: bool,
) {
    #[cfg(not(test))]
    let automatic_maintenance = true;
    let mut deferred = VecDeque::new();
    let mut controller = Controller::new(tuning::settings().expect("validated writer settings"));
    let mut schedule = Schedule::new(
        tuning::settings()
            .expect("validated maintenance cadence")
            .maintenance_interval,
    );
    // Applied writes and leadership changes can move a task's due time.
    let mut progress = weak.upgrade().map(|app| app.consensus.progress());
    let mut maintenance_due = false;
    loop {
        // There is only one mutation owner. Drain the pipeline before timer
        // callbacks, and prioritize overdue maintenance over another customer burst.
        let first = match next_work(
            &mut receiver,
            &mut deferred,
            &mut progress,
            schedule.next,
            maintenance_due,
            automatic_maintenance,
        )
        .await
        {
            Work::Maintenance => {
                let Some(app) = weak.upgrade() else { return };
                // What the run will read. A commit applied while it runs, by
                // another writer, may move a due time the run never saw.
                let before = progress
                    .as_mut()
                    .map(|progress| progress.borrow_and_update().applied);
                let mut committed = false;
                let outcome = if app.consensus.metrics().state == openraft::ServerState::Leader {
                    let outcome = maintain(&app).await;
                    if let Err(error) = &outcome {
                        tracing::warn!(%error, "application maintenance did not commit");
                    }
                    committed = outcome
                        .as_ref()
                        .is_ok_and(|maintained| maintained.committed);
                    Some(outcome.map(|maintained| maintained.next))
                } else {
                    None
                };
                schedule.ran(&app, outcome);
                // The run's hint covers its own commit, one Raft entry, but
                // anything else applied since calls for another run soon.
                if let (Some(progress), Some(before)) = (progress.as_mut(), before) {
                    let after = progress.borrow().applied;
                    let own = committed
                        && before
                            .zip(after)
                            .is_some_and(|(before, after)| after.index == before.index + 1);
                    if after == before || own {
                        progress.borrow_and_update();
                    }
                }
                maintenance_due = false;
                continue;
            }
            Work::Changed => {
                schedule.soon();
                continue;
            }
            Work::Request(first) => first,
            Work::Closed => return,
        };
        let Some(app) = weak.upgrade() else { return };
        // Start immediately: durable commits provide the only coalescing wait.
        // No strong App reference survives an idle queue wait.
        let _guard = app.writer.lock().await;
        deferred.push_front(first);
        maintenance_due = pipeline_with_controller(
            &app,
            &mut receiver,
            &mut deferred,
            &mut controller,
            #[cfg(test)]
            None,
        )
        .await;
    }
}

/// Maintenance has no timer of its own. It runs when a task is due, as the
/// last run reported, or soon after a write or leadership change that may have
/// moved a due time, but never twice within `spacing`.
struct Schedule {
    next: Option<Instant>,
    last: Option<Instant>,
    spacing: Duration,
}

impl Schedule {
    fn new(spacing: Duration) -> Self {
        Self {
            next: Some(Instant::now()),
            last: None,
            spacing,
        }
    }

    fn paced(&self, at: Instant) -> Instant {
        self.last.map_or(at, |last| at.max(last + self.spacing))
    }

    fn soon(&mut self) {
        let at = self.paced(Instant::now());
        self.next = Some(self.next.map_or(at, |next| next.min(at)));
    }

    fn ran(&mut self, app: &App, outcome: Option<anyhow::Result<NextRun>>) {
        let now = Instant::now();
        self.last = Some(now);
        self.next = match outcome {
            // A follower runs nothing until it leads.
            None | Some(Ok(NextRun::Idle)) => None,
            Some(Ok(NextRun::Now)) => Some(self.paced(now)),
            Some(Ok(NextRun::At(time))) => {
                let current = app.clock.sample_after(0).unwrap_or(time);
                // One more millisecond: the clock samples whole milliseconds.
                let delay = Duration::from_millis(time.saturating_sub(current).saturating_add(1));
                Some(self.paced(now.checked_add(delay).unwrap_or(now + self.spacing)))
            }
            // Failures retry, and handlers that give no hint are polled.
            Some(Ok(NextRun::Unknown)) | Some(Err(_)) => Some(now + self.spacing),
        };
    }
}

enum Work {
    Maintenance,
    Changed,
    Request(Pending),
    Closed,
}

async fn next_work(
    receiver: &mut mpsc::Receiver<Pending>,
    deferred: &mut VecDeque<Pending>,
    progress: &mut Option<tokio::sync::watch::Receiver<crate::consensus::Progress>>,
    next_maintenance: Option<Instant>,
    maintenance_due: bool,
    automatic_maintenance: bool,
) -> Work {
    tokio::select! {
        biased;
        _ = async {
            // An explicit drain must be immediately ready, so a busy customer
            // queue cannot starve due maintenance.
            if !maintenance_due && let Some(next) = next_maintenance {
                tokio::time::sleep_until(next.into()).await;
            }
        }, if automatic_maintenance && (maintenance_due || next_maintenance.is_some()) => Work::Maintenance,
        changed = async {
            match progress.as_mut() {
                Some(progress) => progress.changed().await,
                None => std::future::pending().await,
            }
        }, if automatic_maintenance => {
            match changed {
                Ok(()) => {
                    progress.as_mut().expect("subscribed").borrow_and_update();
                }
                Err(_) => *progress = None,
            }
            Work::Changed
        }
        first = async {
            if let Some(first) = deferred.pop_front() {
                Some(first)
            } else {
                receiver.recv().await
            }
        } => match first {
            Some(first) => Work::Request(first),
            None => Work::Closed,
        }
    }
}

fn collect(
    receiver: &mut mpsc::Receiver<Pending>,
    deferred: &mut VecDeque<Pending>,
    limit: usize,
) -> Vec<Pending> {
    // Grow with actual queued work, not the operator's scheduling ceiling.
    let mut pending = Vec::new();
    while pending.len() < limit {
        let next = deferred.pop_front().or_else(|| receiver.try_recv().ok());
        let Some(next) = next else { break };
        // Deployment is a barrier on both sides: bundle code and HTTP exposure
        // change only after all older methods have reached durable application.
        if next.deployment && !pending.is_empty() {
            deferred.push_front(next);
            break;
        }
        let deployment = next.deployment;
        pending.push(next);
        if deployment {
            break;
        }
    }
    pending
}

#[cfg(test)]
async fn pipeline(
    app: &Arc<App>,
    receiver: &mut mpsc::Receiver<Pending>,
    deferred: &mut VecDeque<Pending>,
    hooks: Option<&tests::Hooks>,
) {
    // Deterministic gates model fixed-size groups; separate controller tests
    // exercise adaptive sizing without conflating timing with safety assertions.
    let mut controller = Controller::new(tuning::settings().expect("validated writer settings"));
    if hooks.is_some() {
        controller.fixed_for_test(64);
    }
    pipeline_with_controller(app, receiver, deferred, &mut controller, hooks).await;
}

fn decide(
    app: &App,
    controller: &Controller,
    receiver: &mut mpsc::Receiver<Pending>,
    deferred: &mut VecDeque<Pending>,
    maintenance: Duration,
) -> Decision {
    if deferred.is_empty()
        && let Ok(first) = receiver.try_recv()
    {
        deferred.push_back(first);
    }
    let oldest = deferred
        .front()
        .map_or(Duration::ZERO, |pending| pending.enqueued.elapsed());
    let mut decision = controller.decide_aged(receiver.len() + deferred.len(), oldest, maintenance);
    decision.lag = batching::Lag::from_metrics(&app.consensus.metrics());
    decision
}

async fn pipeline_with_controller(
    app: &Arc<App>,
    receiver: &mut mpsc::Receiver<Pending>,
    deferred: &mut VecDeque<Pending>,
    controller: &mut Controller,
    #[cfg(test)] hooks: Option<&tests::Hooks>,
) -> bool {
    let window = tuning::settings()
        .expect("validated writer window")
        .writer_window;
    #[cfg(test)]
    let window = test_budget(window, hooks);
    let started = Instant::now();
    let decision = decide(
        app,
        controller,
        receiver,
        deferred,
        window.saturating_sub(started.elapsed()),
    );
    let pending = collect(receiver, deferred, decision.count);
    // Adaptive mode accepts useful preparation during every real commit wait.
    // Fixed mode retains its full-group threshold for controlled comparisons.
    let pipelined = decision.lag.allows_successor()
        && (controller.adaptive()
            || (pending.len() == decision.count && (!deferred.is_empty() || !receiver.is_empty())));
    #[cfg(test)]
    let pipelined = pipelined || hooks.is_some();
    // A queued partition must enter node admission before pinning a root. This
    // lease belongs to its first candidate, rather than to the batching window:
    // holding an extra window lease would deadlock a one-slot worker pool.
    let admission = match pending.first() {
        Some(first) => match admit(app, &first.input, first.deployment).await {
            Ok(permit) => Some(permit),
            Err(error) => {
                for entry in pending {
                    let _ = entry.reply.send(Err(error.clone()));
                }
                return false;
            }
        },
        None => return false,
    };
    let reading = Instant::now();
    let snapshot = if pipelined {
        app.consensus.read_for_writer().await
    } else {
        let ids = pending
            .iter()
            .filter_map(|entry| entry.input.request_id())
            .collect::<Vec<_>>();
        app.consensus.read_for_many(&ids).await
    };
    let mut state = match snapshot {
        Ok(state) => state,
        Err(error) => {
            let error = unavailable(error);
            for entry in pending {
                let _ = entry.reply.send(Err(error.clone()));
            }
            return false;
        }
    };
    let read_us = reading.elapsed().as_micros() as u64;
    // Candidates one group did not reach, prepared for its successor.
    let mut carried = None;
    let (mut group, rest) = prepare_group_carried(
        app,
        &mut state,
        pending,
        Preparation {
            read_us,
            decision,
            admission,
        },
        &mut controller.speculation,
        None,
        &mut carried,
        #[cfg(test)]
        hooks,
    )
    .await;
    for entry in rest.into_iter().rev() {
        deferred.push_front(entry);
    }
    loop {
        let remaining = window.saturating_sub(started.elapsed());
        group.early_drain = !remaining.is_zero()
            && pipelined
            && !group.deployment
            && controller.drain_before_deadline(remaining);
        if !pipelined || group.deployment || started.elapsed() >= window || group.early_drain {
            drain(&mut carried).await;
            let early_drain = group.early_drain;
            let outcome = group
                .commit_and_respond(
                    app,
                    #[cfg(test)]
                    hooks,
                )
                .await;
            outcome.observe(controller);
            return early_drain;
        }
        let (completed, completion) = oneshot::channel();
        let predecessor_done = Arc::new(AtomicBool::new(false));
        let committing = async {
            let result = group
                .commit_and_respond(
                    app,
                    #[cfg(test)]
                    hooks,
                )
                .await;
            // Both success and uncertainty close the successor's batching
            // window, after the predecessor's replies have been dispatched.
            predecessor_done.store(true, Ordering::Release);
            let _ = completed.send(());
            result
        };
        tokio::pin!(committing);
        let decision = decide(
            app,
            controller,
            receiver,
            deferred,
            window.saturating_sub(started.elapsed()),
        );
        // The carried candidates lead the deferred calls: take them all.
        let carrying = carried.as_ref().map_or(0, speculation::Wave::len);
        let mut pending = collect(receiver, deferred, decision.count.max(carrying));
        if pending.is_empty() {
            // Low-concurrency callers often arrive during fsync. Keep polling
            // the commit while accepting that next call, instead of serializing
            // its preparation behind a flush just because the queue was empty.
            let first = tokio::select! {
                biased;
                outcome = &mut committing => {
                    outcome.observe(controller);
                    drain(&mut carried).await;
                    return false;
                },
                first = receiver.recv() => match first {
                    Some(first) => first,
                    None => {
                        committing.await.observe(controller);
                        drain(&mut carried).await;
                        return false;
                    }
                },
            };
            deferred.push_front(first);
            pending = collect(receiver, deferred, decision.count);
        }
        if pending[0].deployment || started.elapsed() >= window || !decision.lag.allows_successor()
        {
            for entry in pending.into_iter().rev() {
                deferred.push_front(entry);
            }
            drain(&mut carried).await;
            committing.await.observe(controller);
            return false;
        }
        // At most one submitted group and one preparing group. Replies for N
        // happen inside its future, so a slow N+1 cannot delay durable acks.
        let mut arrivals = Arrivals {
            receiver,
            deferred,
            completion,
            predecessor_done: predecessor_done.clone(),
            deadline: started + window,
            stop_reason: "queue_empty",
        };
        let (committed, (next, rest)) = tokio::join!(
            committing,
            prepare_group_carried(
                app,
                &mut state,
                pending,
                Preparation {
                    read_us: 0,
                    decision,
                    admission: None,
                },
                &mut controller.speculation,
                Some(&mut arrivals),
                &mut carried,
                #[cfg(test)]
                hooks
            ),
        );
        // An unprepared suffix precedes anything left in the queue (including
        // deployments). Never append it behind an already-deferred boundary.
        for entry in rest.into_iter().rev() {
            deferred.push_front(entry);
        }
        committed.observe(controller);
        if let Err(error) = committed.result {
            // A failed/unknown commit may have applied a prefix. Never submit
            // its speculative successor or expose dependent validation/replay
            // answers. The next window starts with a new quorum barrier and
            // atomically published data plus the full durable receipt history.
            next.respond(Err(error), 0);
            drain(&mut carried).await;
            return false;
        }
        group = next;
    }
}

/// Release carried candidates: the calls they served were returned
/// unprepared and will be prepared again.
async fn drain(carried: &mut Option<speculation::Wave<'_>>) {
    if let Some(mut wave) = carried.take() {
        wave.drain().await;
    }
}

/// Continue filling the successor during a real durability wait. Existing
/// queued prefixes are prepared normally; late arrivals join only while the
/// predecessor is outstanding, without an additional coalescing delay. See
/// `Decision::work_conserving` for the adaptive count and time targets.
struct Arrivals<'a> {
    receiver: &'a mut mpsc::Receiver<Pending>,
    deferred: &'a mut VecDeque<Pending>,
    completion: oneshot::Receiver<()>,
    // Set with `completion`; also readable from the blocking serial worker.
    predecessor_done: Arc<AtomicBool>,
    deadline: Instant,
    stop_reason: &'static str,
}

impl Arrivals<'_> {
    fn overlapping(&self) -> bool {
        !self.predecessor_done.load(Ordering::Acquire)
    }

    async fn next(&mut self, preparation_deadline: Option<Instant>) -> Option<Pending> {
        let deadline = preparation_deadline.map_or(self.deadline, |at| at.min(self.deadline));
        if Instant::now() >= deadline {
            self.stop_reason = "preparation_time";
            return None;
        }
        if !matches!(
            self.completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ) {
            self.stop_reason = "predecessor_completed";
            return None;
        }
        let pending = match self.ready_input() {
            Some(pending) => pending,
            None => tokio::select! {
                biased;
                _ = &mut self.completion => { self.stop_reason = "predecessor_completed"; return None; },
                _ = tokio::time::sleep_until(deadline.into()) => { self.stop_reason = "preparation_time"; return None; },
                pending = self.receiver.recv() => pending?,
            },
        };
        self.admit(pending)
    }

    /// Another already queued call, without waiting. Callers answered by the
    /// same durable group tend to reply together; preparing them as one serial
    /// job avoids a separate blocking dispatch for each.
    fn ready(&mut self) -> Option<Pending> {
        if !self.overlapping() || Instant::now() >= self.deadline {
            return None;
        }
        let pending = self.ready_input()?;
        self.admit(pending)
    }

    fn ready_input(&mut self) -> Option<Pending> {
        self.deferred
            .pop_front()
            .or_else(|| self.receiver.try_recv().ok())
    }

    fn admit(&mut self, pending: Pending) -> Option<Pending> {
        if pending.deployment {
            self.stop_reason = "deployment_barrier";
            self.deferred.push_front(pending);
            return None;
        }
        Some(pending)
    }
}

struct Prepared {
    response: Value,
    command: Option<Commit>,
    permit_wait_us: u64,
    evaluation_us: u64,
    // Accepted output remains charged while it waits in its durable group.
    _retained: Option<admission::Input>,
}

#[cfg(test)]
async fn prepare(app: &App, state: &Snapshot, pending: &Pending) -> Result<Prepared, ApiError> {
    prepare_candidate(app, state, &pending.input, pending.deployment, None)
        .await
        .map(|candidate| candidate.prepared)
}

async fn prepare_serial(
    app: &App,
    state: &Snapshot,
    pending: &Pending,
    admission: &mut Option<admission::Permit>,
) -> Result<Prepared, ApiError> {
    prepare_serial_at(app, state, pending, admission, None).await
}

async fn prepare_serial_at(
    app: &App,
    state: &Snapshot,
    pending: &Pending,
    admission: &mut Option<admission::Permit>,
    speculative_now: Option<u64>,
) -> Result<Prepared, ApiError> {
    prepare_serial_input(
        app,
        state,
        &pending.input,
        pending.deployment,
        admission,
        speculative_now,
        serial::Execution::Dispatch,
    )
    .await
}

async fn prepare_serial_input(
    app: &App,
    state: &Snapshot,
    input: &PendingInput,
    deployment: bool,
    admission: &mut Option<admission::Permit>,
    speculative_now: Option<u64>,
    execution: serial::Execution,
) -> Result<Prepared, ApiError> {
    let candidate = prepare_candidate_with(
        app,
        state,
        input,
        deployment,
        speculative_now,
        None,
        admission.clone(),
        execution,
    )
    .await?;
    // Reuse one active reservation through this bounded preparation pass.
    // Cloning the permit shares its lease; it does not admit another worker.
    *admission = Some(candidate.admission);
    Ok(candidate.prepared)
}

#[cfg(test)]
async fn prepare_candidate(
    app: &App,
    state: &Snapshot,
    pending: &PendingInput,
    deployment: bool,
    speculative_now: Option<u64>,
) -> Result<speculation::Candidate, ApiError> {
    prepare_candidate_admitted(app, state, pending, deployment, speculative_now, None, None).await
}

async fn admit(
    app: &App,
    pending: &PendingInput,
    deployment: bool,
) -> Result<admission::Permit, ApiError> {
    // Preparing a bundle (online, or blocking in the lane) compiles and
    // evaluates for seconds, so it takes a deployment slot. An online
    // deployment's cutover evaluates nothing unless its base changed, and then
    // for at most FLOWER_DEPLOYMENT_CUTOVER_MS, so it takes a control slot:
    // that is never held for long, while another database's preparation may
    // hold the deployment slot for seconds, and this lane would wait for it.
    let class = if !deployment {
        admission::Class::User
    } else if pending
        .state
        .lock()
        .expect("pending input")
        .staged
        .is_some()
    {
        admission::Class::Control
    } else {
        admission::Class::Deployment
    };
    pending.dequeued();
    let enabled = crate::telemetry::enabled();
    let started = enabled.then(Instant::now);
    let trace = if enabled {
        tracing::info_span!(target: "flower::otel", parent: &pending.trace, "flower.writer.admission",
            kind = observability::kind(deployment), status = tracing::field::Empty)
    } else {
        tracing::Span::none()
    };
    let result = async { tokio::select! {
        biased;
        _=pending.canceled.notified()=>Err(unavailable(anyhow::anyhow!("request canceled before preparation"))),
        permit=admission::acquire_retained(app, class)=>permit,
    }}.instrument(trace.clone()).await;
    if let Some(started) = started {
        observability::status(&trace, observability::outcome(&result));
        observability::stage(
            "admission",
            started.elapsed(),
            observability::kind(deployment),
        );
    }
    result
}

async fn prepare_candidate_admitted(
    app: &App,
    state: &Snapshot,
    pending: &PendingInput,
    deployment: bool,
    speculative_now: Option<u64>,
    started: Option<Arc<std::sync::atomic::AtomicBool>>,
    admitted: Option<admission::Permit>,
) -> Result<speculation::Candidate, ApiError> {
    prepare_candidate_with(
        app,
        state,
        pending,
        deployment,
        speculative_now,
        started,
        admitted,
        serial::Execution::Dispatch,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_candidate_with(
    app: &App,
    state: &Snapshot,
    pending: &PendingInput,
    deployment: bool,
    speculative_now: Option<u64>,
    started: Option<Arc<std::sync::atomic::AtomicBool>>,
    admitted: Option<admission::Permit>,
    execution: serial::Execution,
) -> Result<speculation::Candidate, ApiError> {
    pending.dequeued();
    if !crate::telemetry::enabled() {
        return prepare_candidate_inner(
            app,
            state,
            pending,
            deployment,
            speculative_now,
            started,
            admitted,
            execution,
        )
        .await;
    }
    let mode = if deployment {
        "deployment"
    } else if speculative_now.is_some() {
        "speculative"
    } else {
        "serial"
    };
    let trace = tracing::info_span!(target: "flower::otel", parent: &pending.trace, "flower.writer.prepare",
        mode, method_name = tracing::field::Empty, status = tracing::field::Empty,
        duplicate = tracing::field::Empty);
    let preparing = Instant::now();
    let result = prepare_candidate_inner(
        app,
        state,
        pending,
        deployment,
        speculative_now,
        started,
        admitted,
        execution,
    )
    .instrument(trace.clone())
    .await;
    observability::status(&trace, observability::outcome(&result));
    if let Ok(candidate) = &result {
        trace.record(
            "duplicate",
            candidate.prepared.response["duplicate"]
                .as_bool()
                .unwrap_or(false),
        );
    }
    observability::stage("prepare", preparing.elapsed(), mode);
    result
}

#[allow(clippy::too_many_arguments)]
async fn prepare_candidate_inner(
    app: &App,
    state: &Snapshot,
    pending: &PendingInput,
    deployment: bool,
    speculative_now: Option<u64>,
    started: Option<Arc<std::sync::atomic::AtomicBool>>,
    admitted: Option<admission::Permit>,
    execution: serial::Execution,
) -> Result<speculation::Candidate, ApiError> {
    let admission = match admitted {
        Some(permit) => permit,
        None => admit(app, pending, deployment).await?,
    };
    if let Some(started) = started {
        started.store(true, std::sync::atomic::Ordering::Release);
    }
    let body = pending.begin()?;
    let input = &body.value;
    let request_id = input["requestId"].as_str().unwrap().to_owned();
    super::transactions::ensure_unlocked(state)?;
    super::transactions::ensure_request_id_available(state, &request_id)?;
    // Resolve before receipt lookup: revoked aliases also revoke old retries.
    let method = if deployment {
        None
    } else {
        Some(http_method(
            state,
            input["name"].as_str().unwrap(),
            Some(MethodKind::Mutation),
        )?)
    };
    if crate::telemetry::enabled()
        && let Some(method) = &method
    {
        tracing::Span::current().record("method_name", method.name.as_str());
    }
    let authorizing = crate::telemetry::enabled().then(Instant::now);
    let access = if deployment {
        None
    } else {
        Some(authorization::authorize_admitted_access(app, state, input, &admission).await?)
    };
    let principal = access
        .as_ref()
        .map_or(Value::Null, |access| access.principal.clone());
    if let Some(started) = authorizing {
        observability::stage(
            "authorization",
            started.elapsed(),
            observability::kind(deployment),
        );
    }
    let fingerprint = authorization::fingerprint(input, deployment, &principal);
    crate::consensus::retention::validate_request_owner_with(state, &request_id, || {
        authorization::owner(&principal, deployment)
    })
    .map_err(retention::error)?;
    if let Some(receipt) = state.requests.get(&request_id) {
        if receipt.fingerprint != fingerprint {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "REQUEST_ID_REUSED",
                "requestId was already used for different content".into(),
            ));
        }
        let mut prepared = Prepared {
            response: json!({"revision":receipt.revision,"value":receipt.result,"duplicate":true}),
            command: None,
            permit_wait_us: 0,
            evaluation_us: 0,
            _retained: None,
        };
        prepared._retained = Some(app.admission.retain(
            if deployment {
                admission::Class::Control
            } else {
                admission::Class::User
            },
            speculation::retained_bytes(&prepared, None),
        )?);
        return Ok(speculation::Candidate::plain(prepared, admission));
    }
    if let Some(expected) = input.get("expectedRevision").and_then(Value::as_u64)
        && expected != state.revision
    {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "REVISION_CONFLICT",
            format!(
                "expected revision {expected}, current revision {}",
                state.revision
            ),
        ));
    }
    super::transactions::ensure_write_capacity(state)?;
    // Set when a staged deployment's base changed and it prepares again here,
    // in the lane, within FLOWER_DEPLOYMENT_CUTOVER_MS.
    let mut again: Option<Instant> = None;
    let staged = if deployment {
        pending.state.lock().expect("pending input").staged.take()
    } else {
        None
    };
    if let Some(prepared) = staged {
        let command = prepared
            .command
            .as_ref()
            .expect("staged deployment command");
        if command.expected_revision == state.revision {
            crate::consensus::retention::validate_capacity_for(
                state,
                &request_id,
                &fingerprint,
                &command.result,
                0,
            )
            .map_err(retention::error)?;
            return Ok(speculation::Candidate::plain(prepared, admission));
        }
        // Writes keep landing on a busy database, so an exact base would never
        // hold. Evaluate again on this state, as blocking preparation does, if
        // the online evaluation took no longer than writes may wait for it and
        // its compiled image is still at hand (compiling here could take seconds).
        // It runs under the lane's own admission, which this pass may share
        // with the candidates around it: waiting for another slot here could
        // wait for this very lease.
        let limit = deployment_cutover()?;
        if limit.is_zero()
            || Duration::from_micros(prepared.evaluation_us) > limit
            || !evaluator::deployment_prepared(&input["bundle"])
        {
            return Err(deployment_conflict(false));
        }
        drop(prepared);
        again = Some(Instant::now() + limit);
    }
    let permit_started = Instant::now();
    let permit = if speculative_now.is_some() {
        None
    } else {
        let permit = app
            .evaluations
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| unavailable(error.into()))?;
        Some(permit)
    };
    let permit_wait_us = permit_started.elapsed().as_micros() as u64;
    observability::stage(
        "evaluation_gate",
        permit_started.elapsed(),
        if speculative_now.is_some() {
            "speculative"
        } else {
            "serial"
        },
    );
    let mut invocation = authorization::business_input(input);
    let method_name = method.as_ref().map(|method| method.name.clone());
    if let Some(method) = &method {
        invocation["name"] = json!(method.name);
    }
    let now = speculative_now.map_or_else(|| app.clock.sample(state).map_err(unavailable), Ok)?;
    let data = state.data.clone();
    let evaluating = Instant::now();
    let held_admission = admission.clone();
    let callback_principal = principal.clone();
    let evaluate = move || {
        let _body = body;
        let _admission = held_admission;
        let _permit = permit;
        if let Some(deadline) = again {
            evaluator::evaluate_deployment_within(
                data,
                invocation,
                now,
                deadline.saturating_duration_since(Instant::now()),
            )
        } else if deployment {
            evaluator::evaluate_at(data, invocation, now)
        } else if speculative_now.is_some() {
            evaluator::invoke_speculative_as(data, invocation, now, callback_principal)
        } else {
            evaluator::invoke_as(data, invocation, "mutation", now, callback_principal)
        }
    };
    let evaluation = match execution.evaluate(evaluate).await {
        // Out of time (the deadline is FLOWER_DEPLOYMENT_CUTOVER_MS away) is a
        // conflict to retry; any other failure is the deployment's own.
        Err(error)
            if again.is_some_and(|deadline| Instant::now() >= deadline)
                && error.message.contains("EVALUATION_BUDGET") =>
        {
            observability::stage("cutover_reevaluation", evaluating.elapsed(), "conflict");
            return Err(deployment_conflict(true));
        }
        result => result?,
    };
    let evaluation_us = evaluating.elapsed().as_micros() as u64;
    if again.is_some() {
        observability::stage("cutover_reevaluation", evaluating.elapsed(), "ok");
    }
    crate::consensus::retention::validate_capacity_for(
        state,
        &request_id,
        &fingerprint,
        &evaluation.value,
        0,
    )
    .map_err(retention::error)?;
    let response =
        json!({"revision":state.revision + 1,"value":evaluation.value,"duplicate":false});
    let certificate = evaluation.mutation_certificate;
    // A mutation that wrote nothing has no effect a retry could repeat: it
    // commits in order without a receipt, and its request ID may run again.
    // Most of a queue worker's mutations are empty claims. Methods declared
    // with `receipt: false` never keep one. Retry sessions acknowledge every
    // sequence number, so theirs keep receipts.
    let wrote = !evaluation.deletes.is_empty() || evaluation.puts.keys().any(|key| key != "clock");
    let declined = method.as_ref().is_some_and(|method| !method.receipt);
    let receiptless = (!wrote || declined) && !deployment && !request_id.starts_with("f2:");
    let mut prepared = Prepared {
        response,
        command: Some(Commit {
            internal: receiptless,
            request_id,
            fingerprint,
            expected_revision: state.revision,
            puts: evaluation.puts,
            deletes: evaluation.deletes,
            result: evaluation.value,
        }),
        permit_wait_us,
        evaluation_us,
        _retained: None,
    };
    prepared._retained = Some(app.admission.retain(
        if deployment {
            admission::Class::Control
        } else {
            admission::Class::User
        },
        speculation::retained_bytes(&prepared, certificate.as_ref()),
    )?);
    Ok(speculation::Candidate {
        prepared,
        principal,
        access,
        method: method_name,
        certificate,
        admission,
    })
}

/// FLOWER_DEPLOYMENT_CUTOVER_MS, at most the evaluation timeout.
fn deployment_cutover() -> Result<Duration, ApiError> {
    let limit = tuning::settings().map_err(unavailable)?.deployment_cutover;
    let timeout = evaluator::config::settings()
        .map_err(unavailable)?
        .evaluation_timeout;
    Ok(limit.min(timeout))
}

fn deployment_conflict(prepared_again: bool) -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "DEPLOYMENT_CONFLICT",
        if prepared_again {
            "Database changed during online preparation, and preparing again in the writer needed more than FLOWER_DEPLOYMENT_CUTOVER_MS; retry the same request ID, or use preparation: blocking"
        } else {
            "Database changed during online preparation; retry the same request ID, or use preparation: blocking"
        }
        .into(),
    )
}

struct StagePlan {
    revision: u64,
    receipt: Option<Receipt>,
    accounting: Option<Value>,
}

fn plan_stage(state: &Snapshot, command: &Commit) -> Result<StagePlan, ApiError> {
    let revision = state.revision + 1;
    let mut receipt = (!command.internal).then(|| Receipt {
        fingerprint: command.fingerprint.clone(),
        revision,
        result: command.result.clone(),
        // Accounting stamps it, having read the retention state.
        epoch: None,
    });
    // Validate receipt accounting against the complete post-command overlay
    // before changing anything. This preserves atomic failure without cloning
    // every persistent tree root and forcing COW paths for every batch item.
    let accounting = receipt
        .as_mut()
        .map(|receipt| {
            crate::consensus::retention::plan_receipt_accounting(
                &command.request_id,
                receipt,
                &state.requests,
                |key| {
                    command.puts.get(key).or_else(|| {
                        (!command.deletes.iter().any(|deleted| deleted == key))
                            .then(|| state.data.get(key))
                            .flatten()
                    })
                },
            )
            .map_err(retention::error)
        })
        .transpose()?
        .flatten();
    Ok(StagePlan {
        revision,
        receipt,
        accounting,
    })
}

fn publish_stage(state: &mut Snapshot, request_id: &str, plan: StagePlan) {
    state.revision = plan.revision;
    if let Some(accounting) = plan.accounting {
        state
            .data
            .insert(crate::consensus::retention::KEY.into(), accounting);
    }
    if let Some(receipt) = plan.receipt {
        state.requests.insert(request_id.to_owned(), receipt);
    }
}

pub(super) fn stage(state: &mut Snapshot, command: &Commit) -> Result<(), ApiError> {
    let plan = plan_stage(state, command)?;
    for key in &command.deletes {
        state.data.remove(key);
    }
    state.data.extend(command.puts.clone());
    publish_stage(state, &command.request_id, plan);
    Ok(())
}

// None means the current nonempty prefix must commit before retrying this input.
fn stage_prepared(
    app: &App,
    state: &mut Snapshot,
    prepared: Result<Prepared, ApiError>,
    commands: &mut Vec<SharedCommit>,
    bytes: &mut usize,
) -> Option<Result<Prepared, ApiError>> {
    Some(match prepared {
        Ok(mut prepared) => {
            if let Some(command) = prepared.command.take() {
                let size = encoded_json_len(&command).expect("validated commit encodes as JSON");
                if !app
                    .consensus
                    .commit_group_fits(*bytes + size, commands.len() + 1)
                {
                    if !commands.is_empty() {
                        return None;
                    }
                    Err(ApiError::new(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "RESULT_TOO_LARGE",
                        format!(
                            "mutation command exceeds FLOWER_TRANSACTION_MAX_BYTES ({})",
                            app.consensus.limits().transaction_max_bytes
                        ),
                    ))
                } else {
                    match SharedCommit::stage(state, command) {
                        Err(error) => Err(error),
                        Ok(command) => {
                            *bytes += size;
                            commands.push(command);
                            Ok(prepared)
                        }
                    }
                }
            } else {
                Ok(prepared)
            }
        }
        Err(error) => Err(error),
    })
}

struct Group {
    pending: Vec<Pending>,
    results: Vec<Result<Prepared, ApiError>>,
    commands: Vec<SharedCommit>,
    deployment: bool,
    revision: u64,
    successor: bool,
    started: Instant,
    read_us: u64,
    prepare_us: u64,
    fill_wait_us: u64,
    bytes: usize,
    deferred: usize,
    decision: Decision,
    stop_reason: &'static str,
    speculative_candidates: usize,
    speculative_reused: usize,
    serial_worker_jobs: usize,
    serial_worker_requests: usize,
    early_drain: bool,
    trace: tracing::Span,
}

struct Preparation {
    read_us: u64,
    decision: Decision,
    admission: Option<admission::Permit>,
}

#[cfg(test)]
async fn prepare_group(
    app: &Arc<App>,
    state: &mut Snapshot,
    pending: Vec<Pending>,
    preparation: Preparation,
    speculation: &mut speculation::Policy,
    arrivals: Option<&mut Arrivals<'_>>,
    #[cfg(test)] hooks: Option<&tests::Hooks>,
) -> (Group, Vec<Pending>) {
    let mut carried = None;
    let prepared = prepare_group_carried(
        app,
        state,
        pending,
        preparation,
        speculation,
        arrivals,
        &mut carried,
        #[cfg(test)]
        hooks,
    )
    .await;
    if let Some(wave) = &mut carried {
        wave.drain().await;
    }
    prepared
}

/// Prepare a group, continuing the speculation `carried` in from the group
/// before, and leaving in it the candidates this group did not reach. They
/// lead the calls returned unprepared, in order, and were prepared against
/// the state this group leaves, where its successor starts.
#[allow(clippy::too_many_arguments)]
async fn prepare_group_carried<'a>(
    app: &'a Arc<App>,
    state: &mut Snapshot,
    pending: Vec<Pending>,
    preparation: Preparation,
    speculation: &mut speculation::Policy,
    arrivals: Option<&mut Arrivals<'_>>,
    carried: &mut Option<speculation::Wave<'a>>,
    #[cfg(test)] hooks: Option<&tests::Hooks>,
) -> (Group, Vec<Pending>) {
    if !crate::telemetry::enabled() {
        return prepare_group_inner(
            app,
            state,
            pending,
            preparation,
            speculation,
            arrivals,
            carried,
            #[cfg(test)]
            hooks,
        )
        .await;
    }
    let trace = observability::batch(&pending);
    prepare_group_inner(
        app,
        state,
        pending,
        preparation,
        speculation,
        arrivals,
        carried,
        #[cfg(test)]
        hooks,
    )
    .instrument(trace)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_group_inner<'a>(
    app: &'a Arc<App>,
    state: &mut Snapshot,
    mut pending: Vec<Pending>,
    preparation: Preparation,
    speculation: &mut speculation::Policy,
    mut arrivals: Option<&mut Arrivals<'_>>,
    carried: &mut Option<speculation::Wave<'a>>,
    #[cfg(test)] hooks: Option<&tests::Hooks>,
) -> (Group, Vec<Pending>) {
    let Preparation {
        read_us,
        decision,
        mut admission,
    } = preparation;
    let preparation_budget = decision.budget;
    #[cfg(test)]
    let preparation_budget = test_budget(preparation_budget, hooks);
    let preparing = Instant::now();
    let deployment = pending.first().is_some_and(|entry| entry.deployment);
    let mut commands = Vec::new();
    let mut results = Vec::with_capacity(pending.len());
    let mut bytes = 0;
    let mut fill_wait_us = 0u64;
    #[cfg(test)]
    let initial_count = pending.len();
    #[cfg(test)]
    let mut prefix_observed = false;
    let mut stop_reason = "queue_empty";
    let mut wave: Option<speculation::Wave<'a>> = carried.take();
    // The index after the last call the wave prepares.
    let mut wave_end = wave.as_ref().map_or(0, speculation::Wave::len);
    if wave_end > pending.len() {
        if let Some(wave) = &mut wave {
            wave.drain().await;
        }
        wave = None;
        wave_end = 0;
    }
    // Whether the wave still lines up with the calls this group leaves.
    let mut aligned = true;
    let mut speculative_candidates = 0;
    let mut speculative_reused = 0;
    let mut serial_worker_jobs = 0;
    let mut serial_worker_requests = 0;
    loop {
        let overlapping = decision.work_conserving()
            && arrivals
                .as_ref()
                .is_some_and(|arrivals| arrivals.overlapping());
        let limit = if overlapping {
            decision.limit
        } else {
            decision.count
        };
        // Submit a work-conserving successor as soon as its predecessor
        // completes; an unprepared suffix leads the next group instead.
        if !overlapping && decision.work_conserving() && arrivals.is_some() && !results.is_empty() {
            stop_reason = "predecessor_completed";
            break;
        }
        if results.len() >= limit {
            stop_reason = "count_target";
            break;
        }
        if !overlapping && !results.is_empty() && preparing.elapsed() >= preparation_budget {
            stop_reason = "preparation_time";
            break;
        }
        if results.len() == pending.len() {
            let Some(arrivals) = &mut arrivals else { break };
            // An idle writer must not occupy a worker while waiting for a
            // client or for the predecessor's durability completion.
            drop(admission.take());
            let waiting = Instant::now();
            let next = arrivals
                .next((!decision.work_conserving()).then(|| preparing + preparation_budget))
                .await;
            fill_wait_us += waiting.elapsed().as_micros() as u64;
            let Some(next) = next else {
                stop_reason = arrivals.stop_reason;
                break;
            };
            pending.push(next);
            while overlapping
                && pending.len() < decision.limit
                && let Some(next) = arrivals.ready()
            {
                pending.push(next);
            }
            if crate::telemetry::enabled() {
                for next in &pending[results.len()..] {
                    observability::link(&tracing::Span::current(), &next.input.trace);
                }
            }
        }
        let maintaining_graph = evaluator::staging::maintaining_graph(&state.data);
        // Target graph replay does not issue reusable mutation certificates.
        // Skip probes entirely while it is live; otherwise every speculative
        // callback would need a second serial execution against this overlay.
        let serial_prefix = if maintaining_graph {
            usize::MAX
        } else {
            speculation.serial_prefix()
        };
        let serial_count = serial_prefix
            .min(pending.len() - results.len())
            .min(limit - results.len());
        let eligible = wave.is_none()
            && !deployment
            && admission.is_some()
            && !authorization::required(state)
            && serial_count > 1;
        #[cfg(test)]
        let eligible = eligible && hooks.is_none();
        if eligible {
            let inputs = pending[results.len()..results.len() + serial_count]
                .iter()
                .map(|entry| entry.input.clone())
                .collect();
            let batch = serial::Batch {
                state: std::mem::take(state),
                commands: std::mem::take(&mut commands),
                results: Vec::with_capacity(serial_count),
                bytes,
                admission: admission.take(),
                stop_reason: None,
            }
            .prepare(
                app.clone(),
                inputs,
                preparing,
                preparation_budget,
                results.len(),
                maintaining_graph,
                arrivals
                    .as_ref()
                    .filter(|_| decision.work_conserving())
                    .map(|arrivals| arrivals.predecessor_done.clone()),
            )
            .await;
            *state = batch.state;
            commands = batch.commands;
            bytes = batch.bytes;
            admission = batch.admission;
            serial_worker_jobs += 1;
            serial_worker_requests += batch.results.len();
            if !maintaining_graph {
                speculation.consume_serial(batch.results.len());
            }
            results.extend(batch.results);
            if let Some(reason) = batch.stop_reason {
                stop_reason = reason;
                break;
            }
            continue;
        }
        let entry = &pending[results.len()];
        let width = if wave.is_none() && !deployment && !maintaining_graph {
            speculation.width()
        } else {
            1
        };
        #[cfg(test)]
        let width = if hooks.is_some() { 1 } else { width };
        if width > 1 && pending.len() - results.len() > 1 {
            let end = pending
                .len()
                .min(results.len().saturating_add(width))
                .min(limit);
            if let Ok(now) = app.clock.sample(state) {
                speculative_candidates += end - results.len();
                observability::speculation("started", end - results.len());
                wave = Some(speculation::Wave::new_admitted(
                    app,
                    state,
                    &pending[results.len()..end],
                    now,
                    admission.take(),
                ));
                wave_end = end;
            }
        }
        let waiting = Instant::now();
        let speculative = match &mut wave {
            Some(wave) => wave.next().await,
            None => None,
        };
        if speculative.is_some() {
            observability::stage("wave_wait", waiting.elapsed(), "batch");
        }
        let prepared = if let Some(Ok(mut candidate)) = speculative {
            match candidate.validate(app, state, &entry.input).await {
                Ok(true) => {
                    observability::speculation("reused", 1);
                    speculative_reused += 1;
                    wave.as_mut().expect("candidate belongs to wave").reused += 1;
                    admission = Some(candidate.admission);
                    Ok(candidate.prepared)
                }
                _ => {
                    observability::speculation("conflict", 1);
                    let current = wave.as_mut().expect("candidate belongs to wave");
                    current.conflicts += 1;
                    let now = current.now;
                    // Only this candidate is invalid. Reuse its worker lease
                    // while rerunning every guard against the ordered overlay;
                    // acquiring another lease could deadlock behind siblings.
                    admission = Some(candidate.admission.clone());
                    drop(candidate);
                    // All methods in a wave share one logical clock. Advancing
                    // it here would invalidate otherwise independent siblings.
                    let rerun = Instant::now();
                    let prepared =
                        prepare_serial_at(app, state, entry, &mut admission, Some(now)).await;
                    observability::stage("conflict_rerun", rerun.elapsed(), "batch");
                    prepared
                }
            }
        } else {
            if speculative.is_some() {
                observability::speculation("failed", 1);
            }
            if let Some(wave) = &mut wave {
                wave.drain().await;
                speculation.conflicted();
            }
            wave = None;
            prepare_serial(app, state, entry, &mut admission).await
        };
        if let Some(current) = &wave
            && !current.is_empty()
        {
            // The remaining wave owns its reservations, including queued
            // admissions. Release this slot before polling the next one.
            drop(admission.take());
        }
        let staging = Instant::now();
        let Some(result) = stage_prepared(app, state, prepared, &mut commands, &mut bytes) else {
            stop_reason = "encoded_bytes";
            // This call leaves unprepared, ahead of the wave.
            aligned = false;
            break;
        };
        observability::stage("staging", staging.elapsed(), "batch");
        results.push(result);
        // While a wave's conflicts stay rare, keep it full: prepare the next
        // calls against the state this one leaves, instead of waiting for it
        // to end before starting another. Each conflict reruns alone.
        let available = pending.len().min(limit);
        if let Some(current) = &mut wave
            && current.conflicts.saturating_mul(4) <= current.reused
            && wave_end < available
            && current.len() < speculation.target()
        {
            speculation.accepted();
            let end = available.min(wave_end + speculation.target() - current.len());
            speculative_candidates += end - wave_end;
            observability::speculation("started", end - wave_end);
            current.extend(app, state, &pending[wave_end..end], admission.take());
            wave_end = end;
        }
        if let Some(current) = &wave
            && current.is_empty()
        {
            speculation.observe_wave(current.reused, current.conflicts);
            wave = None;
        }
        #[cfg(test)]
        if let Some(hooks) = hooks {
            if results.len() == initial_count {
                hooks.prepared(results.len()).await;
                prefix_observed = true;
            } else if results.len() > initial_count {
                hooks.extended(results.len());
            }
        }
    }
    match wave {
        Some(wave) if aligned && !wave.is_empty() => *carried = Some(wave),
        Some(mut wave) => wave.drain().await,
        None => {}
    }
    // Prepared outputs keep only their byte leases while Raft commits.
    drop(admission);
    let deferred = pending.split_off(results.len());
    let group = Group {
        pending,
        results,
        commands,
        deployment,
        revision: state.revision,
        successor: arrivals.is_some(),
        started: preparing,
        read_us,
        prepare_us: (preparing.elapsed().as_micros() as u64).saturating_sub(fill_wait_us),
        fill_wait_us,
        bytes,
        deferred: deferred.len(),
        decision,
        stop_reason,
        speculative_candidates,
        speculative_reused,
        serial_worker_jobs,
        serial_worker_requests,
        early_drain: false,
        trace: if crate::telemetry::enabled() {
            tracing::Span::current()
        } else {
            tracing::Span::none()
        },
    };
    #[cfg(test)]
    if let Some(hooks) = hooks
        && !prefix_observed
    {
        hooks.prepared(group.pending.len()).await;
    }
    (group, deferred)
}

struct CommitOutcome {
    result: Result<(), ApiError>,
    calls: usize,
    prepare: Duration,
    commit: Duration,
}

impl CommitOutcome {
    fn observe(&self, controller: &mut Controller) {
        controller.observe(self.calls, self.prepare, self.commit, self.result.is_ok());
    }
}

impl Group {
    async fn commit_and_respond(
        self,
        app: &App,
        #[cfg(test)] hooks: Option<&tests::Hooks>,
    ) -> CommitOutcome {
        if !crate::telemetry::enabled() {
            return self
                .commit_and_respond_inner(
                    app,
                    #[cfg(test)]
                    hooks,
                )
                .await;
        }
        let trace = if crate::telemetry::enabled() {
            tracing::info_span!(target: "flower::otel", parent: &self.trace, "flower.writer.commit",
                commands = observability::count(self.commands.len()), requests = observability::count(self.results.len()), bytes = observability::count(self.bytes),
                status = tracing::field::Empty)
        } else {
            tracing::Span::none()
        };
        let outcome = self
            .commit_and_respond_inner(
                app,
                #[cfg(test)]
                hooks,
            )
            .instrument(trace.clone())
            .await;
        observability::status(&trace, observability::outcome(&outcome.result));
        outcome
    }

    async fn commit_and_respond_inner(
        mut self,
        app: &App,
        #[cfg(test)] hooks: Option<&tests::Hooks>,
    ) -> CommitOutcome {
        let committing = Instant::now();
        let count = self.commands.len();
        let calls = if self.deployment {
            0
        } else {
            self.results.len()
        };
        let prepare = Duration::from_micros(self.prepare_us);
        #[cfg(test)]
        let decision = match hooks {
            Some(hooks) => hooks.committing(&self).await,
            None => tests::Decision::Apply,
        };
        #[cfg(test)]
        if matches!(decision, tests::Decision::RejectBefore) {
            let error = unavailable(anyhow::anyhow!("injected unknown commit before apply"));
            self.respond(Err(error.clone()), 0);
            return CommitOutcome {
                result: Err(error),
                calls,
                prepare,
                commit: Duration::ZERO,
            };
        }
        let committed = if self.commands.is_empty() {
            // No log entry supplies this group's linearization point. Its
            // requests may have arrived after the window's initial barrier.
            match app.consensus.read_for(None).await {
                Ok(state) if state.revision == self.revision => Ok(()),
                Ok(_) => Err(unavailable(anyhow::anyhow!(
                    "mutation snapshot changed; retry the same request IDs"
                ))),
                Err(error) => Err(unavailable(error)),
            }
        } else {
            commit_prepared_group(app, std::mem::take(&mut self.commands), &self.results).await
        };
        #[cfg(test)]
        let committed = if committed.is_ok() && matches!(decision, tests::Decision::RejectAfter) {
            Err(unavailable(anyhow::anyhow!(
                "injected unknown commit after apply"
            )))
        } else {
            committed
        };
        let commit_us = committing.elapsed().as_micros() as u64;
        self.respond_with_count(committed.clone(), commit_us, count);
        CommitOutcome {
            result: committed,
            calls,
            prepare,
            commit: Duration::from_micros(commit_us),
        }
    }

    fn respond(self, committed: Result<(), ApiError>, commit_us: u64) {
        let count = self.commands.len();
        self.respond_with_count(committed, commit_us, count);
    }

    fn respond_with_count(self, committed: Result<(), ApiError>, commit_us: u64, count: usize) {
        observability::batch_completed(&self, &committed, commit_us, count);
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                batch_mode = self.decision.mode,
                batch_reason = self.decision.reason,
                batch_stop = self.stop_reason,
                speculative_candidates = self.speculative_candidates,
                speculative_reused = self.speculative_reused,
                serial_worker_jobs = self.serial_worker_jobs,
                serial_worker_requests = self.serial_worker_requests,
                batch_target_count = self.decision.count,
                batch_target_us = self.decision.budget.as_micros() as u64,
                batch_queued = self.decision.queued,
                batch_local_unapplied_logs = self.decision.lag.local_unapplied,
                batch_quorum_unmatched_logs = self.decision.lag.quorum_unmatched,
                group_requests = self.results.len(),
                group_successor = u8::from(self.successor),
                group_early_drain = u8::from(self.early_drain),
                group_commands = count,
                group_duplicates = self
                    .results
                    .iter()
                    .filter(|entry| entry
                        .as_ref()
                        .is_ok_and(|p| p.response["duplicate"] == true))
                    .count(),
                group_errors = self.results.iter().filter(|entry| entry.is_err()).count(),
                group_deferred = self.deferred,
                group_bytes = self.bytes,
                read_us = self.read_us,
                prepare_us = self.prepare_us,
                fill_wait_us = self.fill_wait_us,
                commit_us,
                group_us = self.started.elapsed().as_micros() as u64 + self.read_us,
                group_committed = committed.is_ok(),
                "mutation group timing"
            );
        }
        for (entry, result) in self.pending.into_iter().zip(self.results) {
            let response = match &committed {
                // Errors and replays depend on speculative predecessors too.
                Err(error) => Err(error.clone()),
                Ok(()) => result.map(|prepared| {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        let method = entry.input.method();
                        tracing::debug!(%method, batch_commands = count,
                            writer_wait_us = self.started.saturating_duration_since(entry.enqueued).as_micros() as u64,
                            read_us = self.read_us, permit_wait_us = prepared.permit_wait_us,
                            evaluation_us = prepared.evaluation_us, commit_us,
                            duplicate = prepared.response["duplicate"].as_bool().unwrap_or(false),
                            "mutation timing");
                    }
                    prepared.response
                }),
            };
            if crate::telemetry::enabled() {
                let trace = tracing::info_span!(target: "flower::otel", parent: &entry.input.trace, "flower.writer.response",
                    status = observability::outcome(&response), batch_commands = observability::count(count),
                    delivered = tracing::field::Empty);
                observability::status(&trace, observability::outcome(&response));
                observability::link(&trace, &self.trace);
                trace.in_scope(|| {
                    let started = Instant::now();
                    trace.record("delivered", entry.reply.send(response).is_ok());
                    observability::stage("response", started.elapsed(), "request");
                });
            } else {
                let _ = entry.reply.send(response);
            }
        }
    }
}

// Maintenance and other callers without retained Prepared responses keep the
// generic verification path. The ordered request writer borrows its existing
// responses instead.
pub(super) async fn commit_group(app: &App, commands: Vec<Commit>) -> Result<(), ApiError> {
    let expected = commands
        .iter()
        .map(|command| (command.expected_revision + 1, command.result.clone()))
        .collect::<Vec<_>>();
    let applied = commit_commands(app, commands).await?;
    if applied.len() != expected.len() || applied.iter().zip(expected).any(|(actual, (revision, result))| {
        !matches!(actual, ApplyResult::Committed(value) if value.revision == revision && value.result == result && !value.duplicate)
    }) {
        return Err(commit_changed());
    }
    Ok(())
}

async fn commit_prepared_group(
    app: &App,
    commands: Vec<SharedCommit>,
    prepared: &[Result<Prepared, ApiError>],
) -> Result<(), ApiError> {
    let count = commands.len();
    let applied = match count {
        0 => Vec::new(),
        1 => vec![ApplyResult::Committed(
            app.consensus
                .commit(commands.into_iter().next().unwrap().into_commit())
                .await
                .map_err(unavailable)?,
        )],
        _ => app
            .consensus
            .commit_compact(SharedCommit::compact(commands).map_err(unavailable)?)
            .await
            .map_err(unavailable)?,
    };
    if !committed_matches(&applied, count, prepared) {
        return Err(commit_changed());
    }
    Ok(())
}

async fn commit_commands(app: &App, commands: Vec<Commit>) -> Result<Vec<ApplyResult>, ApiError> {
    if commands.is_empty() {
        Ok(Vec::new())
    } else if commands.len() == 1 {
        Ok(vec![ApplyResult::Committed(
            app.consensus
                .commit(commands.into_iter().next().unwrap())
                .await
                .map_err(unavailable)?,
        )])
    } else {
        app.consensus
            .commit_many(commands)
            .await
            .map_err(unavailable)
    }
}

fn commit_changed() -> ApiError {
    unavailable(anyhow::anyhow!(
        "mutation group changed while committing; retry the same request IDs"
    ))
}

fn committed_matches(
    applied: &[ApplyResult],
    count: usize,
    prepared: &[Result<Prepared, ApiError>],
) -> bool {
    // Prepared responses already retain the exact ordered result. Failed calls
    // and historical replays have no command, so omit those when matching Raft
    // acknowledgements without cloning every successful result again.
    let mut expected = prepared.iter().filter_map(|prepared| {
        let response = &prepared.as_ref().ok()?.response;
        (response["duplicate"] == false).then_some(response)
    });
    applied.len() == count
        && applied.iter().all(|actual| {
            let Some(response) = expected.next() else {
                return false;
            };
            matches!(actual, ApplyResult::Committed(value)
                if Some(value.revision) == response["revision"].as_u64()
                    && value.result == response["value"] && !value.duplicate)
        })
        && expected.next().is_none()
}
