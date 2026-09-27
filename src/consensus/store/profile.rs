//! Storage phase timings. Encoding is included in write/prepare, and redb
//! commit includes fsync only with immediate durability. Do not add these
//! overlapping measurements together or interpret them as CPU time.

use super::{Entry, EntryPayload, RaftCommand, Serialize, TypeConfig};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram},
    KeyValue,
};
use std::{sync::OnceLock, time::Instant};

/// Disabled runs do no clock reads or counter updates. The trace moves with
/// disk work so cancellation of its caller cannot hide a completed write.
#[derive(Default)]
pub(super) struct StorageTrace(pub(super) Option<Box<StorageTiming>>);

pub(super) struct StorageTiming {
    node: u64,
    operation: &'static str,
    durability: &'static str,
    legacy: bool,
    telemetry: bool,
    span: tracing::Span,
    started: Instant,
    marked: Instant,
    worker_started: Option<Instant>,
    phases_ns: [u64; 8],
    pub(super) encode_ns: u64,
    pub(super) encoded_bytes: usize,
    entries: usize,
    commands: usize,
    pub(super) first_log_index: Option<u64>,
    pub(super) last_log_index: Option<u64>,
    base_revision: Option<u64>,
    revision: Option<u64>,
    changed_keys: usize,
    receipts: usize,
}

#[derive(Clone, Copy)]
pub(super) enum StoragePhase {
    StateLock,
    IoQueue,
    BlockingQueue,
    Prepare,
    Begin,
    Write,
    Flush,
    Publish,
}

impl StoragePhase {
    const ALL: [Self; 8] = [
        Self::StateLock,
        Self::IoQueue,
        Self::BlockingQueue,
        Self::Prepare,
        Self::Begin,
        Self::Write,
        Self::Flush,
        Self::Publish,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::StateLock => "state_lock",
            Self::IoQueue => "io_queue",
            Self::BlockingQueue => "blocking_queue",
            Self::Prepare => "prepare",
            Self::Begin => "begin",
            Self::Write => "write",
            Self::Flush => "commit",
            Self::Publish => "publish",
        }
    }
}

struct Metrics {
    operations: Counter<u64>,
    duration: Histogram<f64>,
    stage: Histogram<f64>,
    batch: Histogram<u64>,
    bytes: Histogram<u64>,
}

fn duration_boundaries() -> Vec<f64> {
    vec![
        0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500,
        0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0,
        60.0,
    ]
}

fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("flower");
        Metrics {
            operations: meter.u64_counter("flower.storage.operations")
                .with_description("Storage operations by completion outcome").build(),
            duration: meter.f64_histogram("flower.storage.operation.duration")
                .with_unit("s").with_boundaries(duration_boundaries())
                .with_description("Storage operation elapsed time including queueing").build(),
            stage: meter.f64_histogram("flower.storage.stage.duration")
                .with_unit("s").with_boundaries(duration_boundaries())
                .with_description("Storage stage elapsed time; encode overlaps write/prepare; immediate commit includes fsync").build(),
            batch: meter.u64_histogram("flower.storage.batch.size")
                .with_boundaries(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 1_024.0, 4_096.0, 16_384.0, 65_536.0, 1_048_576.0])
                .with_description("Entries, commands, changed keys and receipts per storage operation").build(),
            bytes: meter.u64_histogram("flower.storage.encoded.bytes")
                .with_unit("By")
                .with_boundaries(vec![0.0, 256.0, 1_024.0, 4_096.0, 16_384.0, 65_536.0, 262_144.0, 1_048_576.0, 4_194_304.0, 16_777_216.0, 67_108_864.0, 268_435_456.0, 1_073_741_824.0])
                .with_description("Serialized bytes per storage operation").build(),

        }
    })
}

impl StorageTrace {
    pub(super) fn new(node: u64, operation: &'static str) -> Self {
        static LEGACY: OnceLock<bool> = OnceLock::new();
        let legacy =
            *LEGACY.get_or_init(|| std::env::var("FLOWER_PROFILE_STORAGE").as_deref() == Ok("1"));
        let telemetry = crate::telemetry::enabled();
        if !legacy && !telemetry {
            return Self::default();
        }
        Self::enabled(node, operation, legacy, telemetry)
    }

    fn enabled(node: u64, operation: &'static str, legacy: bool, telemetry: bool) -> Self {
        let durability = if matches!(
            operation,
            "apply" | "persist" | "leader_append" | "snapshot_transfer"
        ) {
            "none"
        } else {
            "immediate"
        };
        let started = Instant::now();
        let span = if telemetry {
            tracing::info_span!(target: "flower::otel", "flower.storage.operation",
                operation, durability, outcome = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                state_lock_seconds = tracing::field::Empty,
                io_queue_seconds = tracing::field::Empty,
                blocking_queue_seconds = tracing::field::Empty,
                prepare_seconds = tracing::field::Empty,
                begin_seconds = tracing::field::Empty,
                write_seconds = tracing::field::Empty,
                encode_seconds = tracing::field::Empty,
                commit_seconds = tracing::field::Empty,
                publish_seconds = tracing::field::Empty,
                total_seconds = tracing::field::Empty,
                encoded_bytes = tracing::field::Empty,
                entries = tracing::field::Empty, commands = tracing::field::Empty,
                changed_keys = tracing::field::Empty, receipts = tracing::field::Empty)
        } else {
            tracing::Span::none()
        };
        Self(Some(Box::new(StorageTiming {
            node,
            operation,
            durability,
            legacy,
            telemetry,
            span,
            started,
            marked: started,
            worker_started: None,
            phases_ns: [0; 8],
            encode_ns: 0,
            encoded_bytes: 0,
            entries: 0,
            commands: 0,
            first_log_index: None,
            last_log_index: None,
            base_revision: None,
            revision: None,
            changed_keys: 0,
            receipts: 0,
        })))
    }

    pub(super) fn span(&self) -> tracing::Span {
        self.0
            .as_ref()
            .map_or_else(tracing::Span::none, |timing| timing.span.clone())
    }

    pub(super) fn phase(&mut self, phase: StoragePhase) {
        let Some(timing) = &mut self.0 else { return };
        let now = Instant::now();
        let elapsed = now
            .duration_since(timing.marked)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        timing.marked = now;
        if matches!(phase, StoragePhase::BlockingQueue) {
            timing.worker_started.get_or_insert(now);
        }
        timing.phases_ns[phase as usize] = timing.phases_ns[phase as usize].saturating_add(elapsed);
    }

    pub(super) fn entries(&mut self, entries: &[Entry<TypeConfig>]) {
        let Some(timing) = &mut self.0 else { return };
        timing.entries = entries.len();
        timing.first_log_index = entries.first().map(|entry| entry.log_id.index);
        timing.last_log_index = entries.last().map(|entry| entry.log_id.index);
        timing.commands = entries
            .iter()
            .map(|entry| {
                let EntryPayload::Normal(command) = &entry.payload else {
                    return 0;
                };
                let mut command = command;
                while let RaftCommand::Scoped { command: inner, .. } = command {
                    command = inner;
                }
                match command {
                    RaftCommand::Single(_) | RaftCommand::Fenced { .. } => 1,
                    RaftCommand::Batch { batch } => batch.items.len(),
                    _ => 0,
                }
            })
            .sum();
    }

    pub(super) fn application(
        &mut self,
        base_revision: u64,
        revision: u64,
        changed_keys: usize,
        receipts: usize,
    ) {
        let Some(timing) = &mut self.0 else { return };
        timing.base_revision = Some(base_revision);
        timing.revision = Some(revision);
        timing.changed_keys = changed_keys;
        timing.receipts = receipts;
    }

    pub(super) fn encode<T: Serialize>(&mut self, value: &T) -> serde_json::Result<Vec<u8>> {
        let Some(timing) = &mut self.0 else {
            return serde_json::to_vec(value);
        };
        let started = Instant::now();
        let result = serde_json::to_vec(value);
        // Preserve sub-microsecond receipt encoding work when accumulating it.
        timing.encode_ns = timing
            .encode_ns
            .saturating_add(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        if let Ok(bytes) = &result {
            timing.encoded_bytes = timing.encoded_bytes.saturating_add(bytes.len());
        }
        result
    }

    pub(super) fn report(mut self, ok: bool) {
        if let Some(timing) = self.0.take() {
            timing.report(if ok { "ok" } else { "error" });
        }
    }
}

impl Drop for StorageTrace {
    fn drop(&mut self) {
        if let Some(timing) = self.0.take() {
            // A task may be cancelled before reaching the blocking pool, or
            // fail during snapshot preparation. Do not claim success or guess.
            timing.report(if std::thread::panicking() {
                "panic"
            } else {
                "incomplete"
            });
        }
    }
}

impl StorageTiming {
    fn report(self, outcome: &'static str) {
        let finished = Instant::now();
        let seconds = |phase: StoragePhase| self.phases_ns[phase as usize] as f64 / 1_000_000_000.0;
        let total = finished.duration_since(self.started).as_secs_f64();
        if self.telemetry {
            let attributes = [
                KeyValue::new("operation", self.operation),
                KeyValue::new("durability", self.durability),
                KeyValue::new("outcome", outcome),
            ];
            let metrics = metrics();
            metrics.operations.add(1, &attributes);
            metrics.duration.record(total, &attributes);
            metrics.bytes.record(self.encoded_bytes as u64, &attributes);
            for phase in StoragePhase::ALL {
                let attrs = [
                    attributes[0].clone(),
                    attributes[1].clone(),
                    attributes[2].clone(),
                    KeyValue::new("stage", phase.name()),
                ];
                metrics.stage.record(seconds(phase), &attrs);
            }
            let attrs = [
                attributes[0].clone(),
                attributes[1].clone(),
                attributes[2].clone(),
                KeyValue::new("stage", "encode"),
            ];
            metrics
                .stage
                .record(self.encode_ns as f64 / 1_000_000_000.0, &attrs);
            for (kind, value) in [
                ("entries", self.entries),
                ("commands", self.commands),
                ("changed_keys", self.changed_keys),
                ("receipts", self.receipts),
            ] {
                let attrs = [
                    attributes[0].clone(),
                    attributes[1].clone(),
                    attributes[2].clone(),
                    KeyValue::new("kind", kind),
                ];
                metrics.batch.record(value as u64, &attrs);
            }
            self.span.record("outcome", outcome);
            if matches!(outcome, "error" | "panic") {
                self.span.record("otel.status_code", "ERROR");
            }
            for (field, value) in [
                ("state_lock_seconds", seconds(StoragePhase::StateLock)),
                ("io_queue_seconds", seconds(StoragePhase::IoQueue)),
                (
                    "blocking_queue_seconds",
                    seconds(StoragePhase::BlockingQueue),
                ),
                ("prepare_seconds", seconds(StoragePhase::Prepare)),
                ("begin_seconds", seconds(StoragePhase::Begin)),
                ("write_seconds", seconds(StoragePhase::Write)),
                ("commit_seconds", seconds(StoragePhase::Flush)),
                ("publish_seconds", seconds(StoragePhase::Publish)),
                ("encode_seconds", self.encode_ns as f64 / 1_000_000_000.0),
                ("total_seconds", total),
            ] {
                self.span.record(field, value);
            }
            for (field, value) in [
                ("encoded_bytes", self.encoded_bytes),
                ("entries", self.entries),
                ("commands", self.commands),
                ("changed_keys", self.changed_keys),
                ("receipts", self.receipts),
            ] {
                // OpenTelemetry attributes use signed integers. Recording a
                // u64 falls back to a string in the tracing OTEL visitor.
                self.span
                    .record(field, i64::try_from(value).unwrap_or(i64::MAX));
            }
        }
        if self.legacy {
            let us = |phase: StoragePhase| self.phases_ns[phase as usize] / 1_000;
            tracing::info!(target: "flower::storage_profile",
                node = self.node, operation = self.operation, ok = outcome == "ok",
                first_log_index = self.first_log_index, last_log_index = self.last_log_index,
                entries = self.entries, commands = self.commands,
                base_revision = self.base_revision, revision = self.revision,
                changed_keys = self.changed_keys, receipts = self.receipts,
                encoded_bytes = self.encoded_bytes,
                state_lock_us = us(StoragePhase::StateLock), io_queue_us = us(StoragePhase::IoQueue),
                blocking_queue_us = us(StoragePhase::BlockingQueue), prepare_us = us(StoragePhase::Prepare),
                begin_us = us(StoragePhase::Begin), write_us = us(StoragePhase::Write), encode_us = self.encode_ns / 1_000,
                flush_us = us(StoragePhase::Flush), publish_us = us(StoragePhase::Publish),
                work_us = finished.duration_since(self.worker_started.unwrap_or(self.started)).as_micros() as u64,
                total_us = finished.duration_since(self.started).as_micros() as u64,
                "storage write");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_profile_preserves_encoding_without_allocating_timers() {
        let mut profile = StorageTrace::default();
        let value = serde_json::json!({"value": [1, 2, 3]});
        assert_eq!(
            profile.encode(&value).unwrap(),
            serde_json::to_vec(&value).unwrap()
        );
        profile.phase(StoragePhase::Write);
        assert!(profile.0.is_none());
    }

    #[test]
    fn projection_commit_and_durable_log_commit_are_distinguishable() {
        let mut apply = StorageTrace::enabled(1, "apply", false, false);
        let append = StorageTrace::enabled(1, "append", false, false);
        assert_eq!(apply.0.as_ref().unwrap().durability, "none");
        assert_eq!(append.0.as_ref().unwrap().durability, "immediate");
        for deferred in ["persist", "leader_append"] {
            let trace = StorageTrace::enabled(1, deferred, false, false);
            assert_eq!(trace.0.as_ref().unwrap().durability, "none");
        }
        let flush = StorageTrace::enabled(1, "leader_flush", false, false);
        assert_eq!(flush.0.as_ref().unwrap().durability, "immediate");
        let value = serde_json::json!({"secret-value": "never a label"});
        let encoded = apply.encode(&value).unwrap();
        apply.phase(StoragePhase::Write);
        assert_eq!(apply.0.as_ref().unwrap().encoded_bytes, encoded.len());
        assert!(
            apply.0.as_ref().unwrap().phases_ns[StoragePhase::Write as usize]
                >= apply.0.as_ref().unwrap().encode_ns
        );
        assert_eq!(StoragePhase::Flush.name(), "commit");
    }

    #[test]
    fn partition_wrappers_do_not_hide_batched_commands() {
        let mut profile = StorageTrace::enabled(1, "append", false, false);
        let batch = crate::consensus::CompactBatch {
            expected_revision: 0,
            puts: Default::default(),
            deletes: Vec::new(),
            items: (0..3)
                .map(|index| crate::consensus::BatchItem {
                    internal: false,
                    request_id: index.to_string(),
                    fingerprint: "not exported".into(),
                    result: serde_json::Value::Null,
                })
                .collect(),
        };
        profile.entries(&[Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 3),
            payload: EntryPayload::Normal(RaftCommand::Scoped {
                partition: "not exported".into(),
                epoch: 1,
                command: Box::new(RaftCommand::Batch { batch }),
            }),
        }]);
        let timing = profile.0.as_ref().unwrap();
        assert_eq!(timing.entries, 1);
        assert_eq!(timing.commands, 3);
    }
}
