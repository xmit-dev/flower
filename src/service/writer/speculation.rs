//! Concurrent preparation never owns the commit order. Every candidate keeps
//! its admission reservation until accepted or discarded by that ordered lane.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{future::Future, pin::Pin, task::Poll};

pub(super) struct Candidate {
    pub prepared: Prepared,
    pub principal: Value,
    /// The decision `principal` came from, when authorization ran.
    pub access: Option<authorization::Access>,
    pub method: Option<String>,
    pub certificate: Option<evaluator::MutationCertificate>,
    pub admission: admission::Permit,
}
impl Candidate {
    pub fn plain(prepared: Prepared, admission: admission::Permit) -> Self {
        Self {
            prepared,
            principal: Value::Null,
            access: None,
            method: None,
            certificate: None,
            admission,
        }
    }
    pub async fn validate(
        &mut self,
        app: &App,
        state: &Snapshot,
        pending: &PendingInput,
    ) -> Result<bool, ApiError> {
        if !crate::telemetry::enabled() {
            return self.validate_inner(app, state, pending).await;
        }
        let trace = tracing::info_span!(target: "flower::otel", parent: &pending.trace, "flower.writer.speculation.validate",
            status = tracing::field::Empty);
        let started = Instant::now();
        let result = self
            .validate_inner(app, state, pending)
            .instrument(trace.clone())
            .await;
        let status = match &result {
            Ok(true) => "reused",
            Ok(false) => "conflict",
            Err(_) => "error",
        };
        observability::status(&trace, status);
        observability::stage("speculation_validation", started.elapsed(), "speculative");
        result
    }

    async fn validate_inner(
        &mut self,
        app: &App,
        state: &Snapshot,
        pending: &PendingInput,
    ) -> Result<bool, ApiError> {
        let Some(certificate) = &self.certificate else {
            return Ok(false);
        };
        if !certificate.valid(&state.data) {
            return Ok(false);
        }
        let body = pending.begin()?;
        let input = &body.value;
        transactions::ensure_unlocked(state)?;
        let id = input["requestId"].as_str().expect("validated request ID");
        transactions::ensure_request_id_available(state, id)?;
        let method = http_method(
            state,
            input["name"].as_str().unwrap(),
            Some(MethodKind::Mutation),
        )?;
        if self.method.as_deref() != Some(method.name.as_str()) {
            return Ok(false);
        }
        // Owner equality is insufficient: changed claims may change the body.
        // A decision that still holds on this state would decide the same.
        if !self
            .access
            .as_ref()
            .is_some_and(|access| access.holds(app, state))
        {
            let principal =
                authorization::authorize_admitted(app, state, input, &self.admission).await?;
            if principal != self.principal {
                return Ok(false);
            }
        }
        let principal = &self.principal;
        let fingerprint = authorization::fingerprint(input, false, &principal);
        crate::consensus::retention::validate_request_owner_with(state, id, || {
            authorization::owner(&principal, false)
        })
        .map_err(retention::error)?;
        if state.requests.contains_key(id) {
            return Ok(false);
        }
        if input
            .get("expectedRevision")
            .and_then(Value::as_u64)
            .is_some_and(|expected| expected != state.revision)
        {
            return Ok(false);
        }
        transactions::ensure_write_capacity(state)?;
        let Some(command) = &mut self.prepared.command else {
            return Ok(false);
        };
        if command.fingerprint != fingerprint {
            return Ok(false);
        }
        if command
            .puts
            .get("clock")
            .is_some_and(|clock| state.data.get("clock") == Some(clock))
        {
            command.puts.remove("clock");
        }
        crate::consensus::retention::validate_capacity_for(
            state,
            id,
            &fingerprint,
            &command.result,
            0,
        )
        .map_err(retention::error)?;
        command.expected_revision = state.revision;
        self.prepared.response["revision"] = json!(state.revision + 1);
        Ok(true)
    }
}
pub(super) fn retained_bytes(
    prepared: &Prepared,
    certificate: Option<&evaluator::MutationCertificate>,
) -> usize {
    let mut size = admission::input_bytes(&prepared.response)
        .saturating_add(certificate.map_or(0, evaluator::MutationCertificate::allocation_cost));
    if let Some(command) = &prepared.command {
        size = size
            .saturating_add(command.request_id.len())
            .saturating_add(command.fingerprint.len())
            .saturating_add(admission::input_bytes(&command.result));
        for (key, value) in &command.puts {
            size = size
                .saturating_add(key.len() + 128)
                .saturating_add(admission::input_bytes(value));
        }
        for key in &command.deletes {
            size = size.saturating_add(key.len() + 64);
        }
    }
    size
}

pub(super) struct Policy {
    limit: usize,
    width: usize,
    serial: usize,
    cooldown: usize,
    max_cooldown: usize,
}
impl Policy {
    pub fn new(limit: usize, queue_capacity: usize) -> Self {
        Self {
            limit,
            // Learn independence with a pair before occupying every worker.
            width: limit.min(2),
            serial: 0,
            cooldown: limit,
            max_cooldown: queue_capacity.max(limit),
        }
    }
    // Batch only calls that the existing adaptive policy would run serially;
    // consume completed calls so a time/byte boundary cannot skip the next probe.
    pub fn serial_prefix(&self) -> usize {
        if self.limit <= 1 {
            usize::MAX
        } else {
            self.serial
        }
    }
    pub fn consume_serial(&mut self, count: usize) {
        self.serial = self.serial.saturating_sub(count);
    }
    /// The width a conflict-free wave aims for, without consuming a probe.
    pub fn target(&self) -> usize {
        self.width.max(2).min(self.limit)
    }
    pub fn width(&mut self) -> usize {
        if self.serial > 0 {
            self.serial -= 1;
            return 1;
        }
        self.width.max(2).min(self.limit)
    }
    pub fn accepted(&mut self) {
        self.width = self.width.saturating_mul(2).min(self.limit);
        self.cooldown = self.limit;
    }
    pub fn conflicted(&mut self) {
        self.width = (self.width / 2).max(1);
        if self.width == 1 {
            // Repeated hot-key conflicts make probes progressively rarer.
            // The operator's queue capacity bounds recovery distance; an
            // accepted probe resets it so newly independent work ramps up.
            self.serial = self.cooldown;
            self.cooldown = self.cooldown.saturating_mul(2).min(self.max_cooldown);
        }
    }
    pub fn observe_wave(&mut self, reused: usize, conflicts: usize) {
        if conflicts == 0 {
            self.accepted();
        } else if reused > conflicts {
            // A partially conflicting wave can still save most executions.
            // Keep its useful width without rewarding collisions with growth.
            self.serial = 0;
            self.cooldown = self.limit;
        } else {
            self.conflicted();
        }
    }
}

type PreparedFuture<'a> = Pin<Box<dyn Future<Output = Result<Candidate, ApiError>> + Send + 'a>>;
struct Slot<'a> {
    future: Option<PreparedFuture<'a>>,
    result: Option<Result<Candidate, ApiError>>,
    started: Arc<AtomicBool>,
}
pub(super) struct Wave<'a> {
    slots: VecDeque<Slot<'a>>,
    pub now: u64,
    pub reused: usize,
    pub conflicts: usize,
}
impl<'a> Wave<'a> {
    #[cfg(test)]
    pub fn new(app: &'a App, state: &Snapshot, pending: &[Pending], now: u64) -> Self {
        Self::new_admitted(app, state, pending, now, None)
    }
    pub fn new_admitted(
        app: &'a App,
        state: &Snapshot,
        pending: &[Pending],
        now: u64,
        admitted: Option<admission::Permit>,
    ) -> Self {
        let mut wave = Self {
            slots: VecDeque::new(),
            now,
            reused: 0,
            conflicts: 0,
        };
        wave.extend(app, state, pending, admitted);
        wave
    }
    pub fn len(&self) -> usize {
        self.slots.len()
    }
    /// Prepare more calls, after those already in the wave, against `state`
    /// as the earlier ones leave it. The first may use `admitted`.
    pub fn extend(
        &mut self,
        app: &'a App,
        state: &Snapshot,
        pending: &[Pending],
        mut admitted: Option<admission::Permit>,
    ) {
        let now = self.now;
        let slots = pending
            .iter()
            .map(|pending| {
                let admitted = admitted.take();
                let input = pending.input.clone();
                let state = state.clone();
                let started = Arc::new(AtomicBool::new(false));
                let signal = started.clone();
                Slot {
                    future: Some(Box::pin(async move {
                        prepare_candidate_admitted(
                            app,
                            &state,
                            &input,
                            false,
                            Some(now),
                            Some(signal),
                            admitted,
                        )
                        .await
                    })),
                    result: None,
                    started,
                }
            });
        self.slots.extend(slots);
    }
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
    pub async fn next(&mut self) -> Option<Result<Candidate, ApiError>> {
        if self.slots.is_empty() {
            return None;
        }
        std::future::poll_fn(|cx| {
            // Poll admission in queue order. A later completed candidate must
            // never hold every permit while an earlier sibling waits to start.
            for slot in &mut self.slots {
                if let Some(future) = &mut slot.future
                    && let Poll::Ready(result) = future.as_mut().poll(cx)
                {
                    slot.future = None;
                    slot.result = Some(result);
                }
            }
            if self.slots.front().is_some_and(|slot| slot.result.is_some()) {
                Poll::Ready(self.slots.pop_front().and_then(|slot| slot.result))
            } else {
                Poll::Pending
            }
        })
        .await
    }
    pub async fn drain(&mut self) {
        // Cancel admission waits without canceling the original queued request.
        // Do not start more callbacks after the batching window/conflict ends.
        self.slots
            .retain(|slot| slot.result.is_some() || slot.started.load(Ordering::Acquire));
        // Await and release in queue order, including running blocking jobs.
        // Keeping an entire completed wave would deadlock small permit pools.
        while let Some(candidate) = self.next().await {
            drop(candidate)
        }
    }
}

#[cfg(test)]
mod policy_tests {
    use super::Policy;

    #[test]
    fn useful_partial_waves_keep_width_but_hot_conflicts_back_off() {
        let mut policy = Policy::new(8, 32);
        policy.observe_wave(2, 0);
        assert_eq!(policy.width(), 4);
        policy.observe_wave(3, 1);
        assert_eq!(policy.width(), 4);
        policy.observe_wave(1, 3);
        assert_eq!(policy.width(), 2);
        policy.observe_wave(1, 1);
        for _ in 0..8 {
            assert_eq!(policy.width(), 1);
        }
        assert_eq!(policy.width(), 2);
    }

    #[test]
    fn hot_conflicts_back_off_and_successful_probe_restores_parallelism() {
        let mut policy = Policy::new(8, 32);
        assert_eq!(policy.width(), 2);
        for expected in [8, 16, 32, 32] {
            policy.conflicted();
            for _ in 0..expected {
                assert_eq!(policy.width(), 1);
            }
            assert_eq!(policy.width(), 2);
        }
        policy.accepted();
        assert_eq!(policy.width(), 2);
        policy.accepted();
        assert_eq!(policy.width(), 4);
        policy.accepted();
        assert_eq!(policy.width(), 8);
        policy.conflicted();
        policy.conflicted();
        policy.conflicted();
        for _ in 0..8 {
            assert_eq!(policy.width(), 1);
        }
        assert_eq!(policy.width(), 2, "Accepted work resets contention history");
    }

    #[test]
    fn serial_configuration_never_starts_a_wave_and_probe_budget_is_operator_bounded() {
        let mut serial = Policy::new(1, 64);
        for _ in 0..4 {
            serial.accepted();
            assert_eq!(serial.width(), 1);
        }
        let mut small = Policy::new(4, 1);
        for _ in 0..8 {
            small.conflicted();
            for _ in 0..4 {
                assert_eq!(small.width(), 1);
            }
            assert_eq!(small.width(), 2);
        }
    }

    #[test]
    fn serial_batches_consume_only_completed_cooldown_calls() {
        let mut policy = Policy::new(4, 16);
        assert_eq!(policy.serial_prefix(), 0);
        policy.conflicted();
        assert_eq!(policy.serial_prefix(), 4);
        policy.consume_serial(2);
        assert_eq!(policy.serial_prefix(), 2);
        assert_eq!(policy.width(), 1);
        policy.consume_serial(1);
        assert_eq!(policy.serial_prefix(), 0);
        assert_eq!(
            policy.width(),
            2,
            "the next independence probe is not skipped"
        );
        let mut serial = Policy::new(1, 8);
        serial.consume_serial(999);
        assert_eq!(serial.serial_prefix(), usize::MAX);
        assert_eq!(serial.width(), 1);
    }
}
