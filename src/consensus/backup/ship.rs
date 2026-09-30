//! The leader's side of backups: ship applied entries in segments, write
//! bases, and apply retention. Followers ship nothing; their stores hold
//! recent entries so that they can continue the generation as leaders.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use axum::body::Bytes;
use futures_util::StreamExt;
use openraft::ServerState;
use serde::Serialize;
use tokio::sync::{mpsc, watch};

use super::format::{
    self, BaseEncoder, BaseHeader, Contract, Generation, Record, SegmentHeader, Tip,
};
use super::target::{Target, Upload};
use super::{Config, format_time, now_ms};
use crate::consensus::FlowerRaft;
use crate::consensus::store::{BackupImage, EntryMark, Shippable, Store};

// A failed base is tried again after this.
const BASE_RETRY: Duration = Duration::from_secs(60);
// A generation without a base, older than this and older than the current
// one, can't be restored and is deleted.
const ORPHAN_AGE_MS: u64 = 3600 * 1000;
const DELETE_CONCURRENCY: usize = 16;
// The same failure is logged again after this.
const LOG_AGAIN: Duration = Duration::from_secs(60);

/// What `GET /admin/backup` reports.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Status {
    target: String,
    /// starting, leader (shipping), follower (holding) or stopped.
    role: &'static str,
    generation: Option<String>,
    applied: Option<u64>,
    shipped: Option<Shipped>,
    lag_entries: Option<u64>,
    newest_base: Option<BaseStatus>,
    base_in_progress: Option<BaseProgress>,
    segments_shipped: u64,
    /// Stored, then unpacked.
    segment_bytes_shipped: u64,
    segment_raw_bytes_shipped: u64,
    bases_written: u64,
    retention: Option<RetentionStatus>,
    failing: bool,
    errors: u64,
    last_error: Option<ErrorStatus>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Shipped {
    index: u64,
    at: u64,
    time: String,
    segment: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BaseStatus {
    index: u64,
    at: u64,
    time: String,
    bytes: u64,
    raw_bytes: Option<u64>,
    seconds: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BaseProgress {
    index: u64,
    at: u64,
    time: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RetentionStatus {
    time: String,
    horizon: String,
    deleted: u64,
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorStatus {
    message: String,
    time: String,
}

impl Status {
    pub(super) fn new(target: String) -> Self {
        Self {
            target,
            role: "starting",
            generation: None,
            applied: None,
            shipped: None,
            lag_entries: None,
            newest_base: None,
            base_in_progress: None,
            segments_shipped: 0,
            segment_bytes_shipped: 0,
            segment_raw_bytes_shipped: 0,
            bases_written: 0,
            retention: None,
            failing: false,
            errors: 0,
            last_error: None,
        }
    }
}

/// The generation this leader ships.
struct Current {
    generation: String,
    // The newest entry shipped, or the first base's.
    tip: u64,
    tip_at: u64,
    tip_mark: Option<EntryMark>,
    tip_segment: Option<String>,
    // tip.json is behind, and when it was last written.
    tip_dirty: bool,
    tip_written: Option<Instant>,
    // The newest base written: index, capture time, object bytes.
    newest_base: Option<(u64, u64, u64)>,
    shipped_since_base: u64,
}

struct BaseTask {
    generation: String,
    index: u64,
    at: u64,
    started: Instant,
    cancel: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<anyhow::Result<(u64, u64)>>,
}

pub(super) struct Shipper {
    config: Config,
    id: u64,
    target: Arc<Target>,
    raft: FlowerRaft,
    store: Store,
    status: Arc<Mutex<Status>>,
    current: Option<Current>,
    base: Option<BaseTask>,
    retry_base_after: Option<Instant>,
    retention: Option<tokio::task::JoinHandle<RetentionOutcome>>,
    last_retention: Option<Instant>,
    logged: Option<(String, Instant)>,
}

struct RetentionOutcome {
    horizon: u64,
    deleted: u64,
    error: Option<String>,
}

impl Shipper {
    pub(super) fn new(
        config: Config,
        id: u64,
        target: Arc<Target>,
        raft: FlowerRaft,
        store: Store,
        status: Arc<Mutex<Status>>,
    ) -> Self {
        Self {
            config,
            id,
            target,
            raft,
            store,
            status,
            current: None,
            base: None,
            retry_base_after: None,
            retention: None,
            last_retention: None,
            logged: None,
        }
    }

    fn status(&self) -> std::sync::MutexGuard<'_, Status> {
        self.status.lock().expect("backup status lock")
    }

    pub(super) async fn run(mut self, mut stop: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break;
                    }
                    continue;
                }
                _ = ticker.tick() => {}
            }
            let outcome = tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break;
                    }
                    continue;
                }
                outcome = self.tick() => outcome,
            };
            self.note(outcome);
        }
        // Ship what the leader applied last, if the store can still say.
        if tokio::time::timeout(Duration::from_secs(5), self.final_shipment())
            .await
            .is_err()
        {
            tracing::warn!(target: "flower::backup", "the last backup shipment did not finish in 5 s");
        }
        self.cancel_base().await;
        if let Some(retention) = self.retention.take() {
            retention.abort();
        }
        self.status().role = "stopped";
    }

    async fn final_shipment(&mut self) {
        if self.current.is_none() || !self.leading() {
            return;
        }
        let Some(applied) = self.store.read_fence().ok().map(|fence| fence.applied) else {
            return;
        };
        let result = async {
            self.ship(applied.index).await?;
            self.write_tip(true).await
        }
        .await;
        match result {
            Ok(()) => {
                if let Some(current) = &self.current {
                    tracing::info!(target: "flower::backup", generation = %current.generation,
                        index = current.tip, "backup shipped through the last applied entry");
                }
            }
            Err(error) => tracing::warn!(target: "flower::backup", error = %format!("{error:#}"),
                "the last backup shipment failed"),
        }
    }

    fn note(&mut self, outcome: anyhow::Result<()>) {
        let mut status = self.status();
        match outcome {
            Ok(()) => status.failing = false,
            Err(error) => {
                let message = format!("{error:#}");
                status.failing = true;
                status.errors += 1;
                status.last_error = Some(ErrorStatus {
                    message: message.clone(),
                    time: format_time(now_ms()),
                });
                drop(status);
                let again = self
                    .logged
                    .as_ref()
                    .is_none_or(|(logged, when)| *logged != message || when.elapsed() >= LOG_AGAIN);
                if again {
                    tracing::warn!(target: "flower::backup", error = %message, "backup failed; retrying");
                    self.logged = Some((message, Instant::now()));
                }
            }
        }
    }

    fn leading(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        metrics.state == ServerState::Leader && metrics.current_leader == Some(self.id)
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        self.poll_base().await;
        self.poll_retention();
        let leading = self.leading();
        let applied = self.store.read_fence().ok().map(|fence| fence.applied);
        {
            let mut status = self.status();
            status.role = if leading { "leader" } else { "follower" };
            status.applied = applied.map(|applied| applied.index);
        }
        if !leading {
            if let Some(current) = self.current.take() {
                tracing::info!(target: "flower::backup", generation = %current.generation,
                    index = current.tip, "no longer the leader: stopped shipping backups");
                self.cancel_base().await;
                let mut status = self.status();
                status.generation = None;
                status.lag_entries = None;
            }
            return Ok(());
        }
        let Some(applied) = applied else {
            return Ok(());
        };
        if self.current.is_none() {
            self.resolve().await?;
        }
        self.ship(applied.index).await?;
        self.maybe_base().await?;
        self.write_tip(false).await?;
        self.maybe_retention();
        if let Some(current) = &self.current {
            self.status().lag_entries = Some(applied.index.saturating_sub(current.tip));
        }
        Ok(())
    }

    /// Continue the newest generation if this log continues it, or start
    /// a new one.
    async fn resolve(&mut self) -> anyhow::Result<()> {
        let newest = self
            .target
            .children("generations/")
            .await?
            .into_iter()
            .filter_map(|prefix| {
                prefix
                    .strip_prefix("generations/")
                    .and_then(|rest| rest.strip_suffix('/'))
                    .filter(|id| format::generation_created(id).is_some())
                    .map(str::to_owned)
            })
            .next_back();
        let mut reason = "new";
        if let Some(generation) = &newest {
            reason = "continuity";
            if let Some(found) = self.find_tip(generation).await? {
                let ours = self.store.entry_mark(found.index).await?;
                if found.mark.is_some() && ours == found.mark {
                    tracing::info!(target: "flower::backup", generation = %generation,
                        index = found.index, "continuing the backup generation");
                    self.store
                        .release_backup_hold(found.index.saturating_sub(1));
                    self.store.forget_applied_at(found.index);
                    self.current = Some(Current {
                        generation: generation.clone(),
                        tip: found.index,
                        tip_at: found.at,
                        tip_mark: found.mark,
                        tip_segment: found.segment,
                        tip_dirty: false,
                        tip_written: None,
                        newest_base: Some(found.newest_base),
                        shipped_since_base: 0,
                    });
                    let mut status = self.status();
                    status.generation = Some(generation.clone());
                    status.newest_base = Some(BaseStatus {
                        index: found.newest_base.0,
                        at: found.newest_base.1,
                        time: format_time(found.newest_base.1),
                        bytes: found.newest_base.2,
                        raw_bytes: None,
                        seconds: None,
                    });
                    return Ok(());
                }
                tracing::info!(target: "flower::backup", generation = %generation,
                    index = found.index, "this log does not continue the newest backup generation");
            }
        }
        self.start_generation(reason, newest).await
    }

    /// The newest shipped entry of a generation that has a base.
    async fn find_tip(&self, generation: &str) -> anyhow::Result<Option<Found>> {
        let mut bases = Vec::new();
        self.target
            .list(&format::bases_prefix(generation), None, |key, size| {
                if let Some((index, at)) = format::parse_base_key(key) {
                    bases.push((index, at, size));
                }
                true
            })
            .await?;
        let Some(&newest_base) = bases.iter().max_by_key(|base| base.0) else {
            return Ok(None);
        };
        let tip: Option<Tip> = self
            .target
            .get(&format::tip_key(generation))
            .await?
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        let start_after = match &tip {
            Some(Tip {
                segment: Some(segment),
                ..
            }) => segment.clone(),
            Some(tip) => format::segments_from(generation, tip.index.max(newest_base.0) + 1),
            None => format::segments_from(generation, newest_base.0 + 1),
        };
        let mut latest = None;
        self.target
            .list(
                &format::segments_prefix(generation),
                Some(&start_after),
                |key, _| {
                    if format::parse_segment_key(key).is_some() {
                        latest = Some(key.to_owned());
                    }
                    true
                },
            )
            .await?;
        if let Some(key) = latest {
            let bytes = self
                .target
                .get(&key)
                .await?
                .with_context(|| format!("backup segment {key} vanished"))?;
            let (header, _) = format::decode_segment(&bytes)?;
            return Ok(Some(Found {
                index: header.last,
                at: header.last_at,
                mark: Some(EntryMark {
                    log_id: header.last_log_id,
                    sha256: header.last_sha256,
                }),
                segment: Some(key),
                newest_base,
            }));
        }
        Ok(tip.and_then(|tip| {
            (tip.index >= newest_base.0).then(|| Found {
                index: tip.index,
                at: tip.at,
                mark: tip
                    .log_id
                    .zip(tip.sha256)
                    .map(|(log_id, sha256)| EntryMark { log_id, sha256 }),
                segment: tip.segment,
                newest_base,
            })
        }))
    }

    async fn start_generation(
        &mut self,
        reason: &str,
        previous: Option<String>,
    ) -> anyhow::Result<()> {
        let restored = self.store.restored_from().await?;
        let reason = if restored.is_some() {
            "restored"
        } else {
            reason
        };
        let image = self.store.backup_image().await;
        let at = now_ms();
        let Some(last) = image.last_applied() else {
            return Ok(());
        };
        let mark = self.store.entry_mark(last.index).await?;
        let generation = format::generation_id(at);
        let record = Generation {
            id: generation.clone(),
            created: at,
            node: self.id,
            previous: previous.clone(),
            reason: reason.into(),
            restored_from: restored.clone(),
            contract: Contract::current(),
        };
        self.target
            .put(
                &format::generation_key(&generation),
                Bytes::from(serde_json::to_vec_pretty(&record)?),
            )
            .await?;
        if restored.is_some() {
            self.store.forget_restored_from().await?;
        }
        tracing::info!(target: "flower::backup", generation = %generation, reason,
            previous = previous.as_deref().unwrap_or(""), index = last.index,
            "started a backup generation");
        self.store.release_backup_hold(last.index.saturating_sub(1));
        self.store.forget_applied_at(last.index);
        self.current = Some(Current {
            generation: generation.clone(),
            tip: last.index,
            tip_at: at,
            tip_mark: mark.clone(),
            tip_segment: None,
            tip_dirty: true,
            tip_written: None,
            newest_base: None,
            shipped_since_base: 0,
        });
        {
            let mut status = self.status();
            status.generation = Some(generation);
            status.newest_base = None;
            status.shipped = None;
        }
        self.retry_base_after = None;
        self.spawn_base(image, at, mark);
        Ok(())
    }

    /// Ship every applied entry through `upto`.
    async fn ship(&mut self, upto: u64) -> anyhow::Result<()> {
        loop {
            let Some(current) = &self.current else {
                return Ok(());
            };
            if current.tip >= upto {
                return Ok(());
            }
            let first = current.tip + 1;
            match self
                .store
                .shippable(first, upto, self.config.segment_max_bytes)
                .await?
            {
                Shippable::Lost => {
                    let previous = current.generation.clone();
                    tracing::warn!(target: "flower::backup", generation = %previous, first,
                        "the log no longer holds the next entries to back up; starting a new generation");
                    self.current = None;
                    self.cancel_base().await;
                    return self.start_generation("gap", Some(previous)).await;
                }
                Shippable::Entries(entries) if entries.is_empty() => return Ok(()),
                Shippable::Entries(entries) => self.put_segment(entries).await?,
            }
        }
    }

    async fn put_segment(&mut self, entries: Vec<(u64, Vec<u8>)>) -> anyhow::Result<()> {
        let current = self.current.as_ref().context("no backup generation")?;
        let now = now_ms();
        let mut at = current.tip_at;
        let mut records = Vec::with_capacity(entries.len());
        for (index, stored) in entries {
            at = at.max(self.store.applied_at(index).unwrap_or(now));
            records.push(Record {
                index,
                at,
                bytes: stored,
            });
        }
        let (first, last) = match (records.first(), records.last()) {
            (Some(first), Some(last)) => (first, last),
            _ => bail!("empty backup segment"),
        };
        let mark = EntryMark::of(&last.bytes)?;
        let header = SegmentHeader {
            generation: current.generation.clone(),
            first: first.index,
            last: last.index,
            first_at: first.at,
            last_at: last.at,
            last_log_id: mark.log_id,
            last_sha256: mark.sha256.clone(),
            contract: Contract::current(),
            node: self.id,
        };
        let key = format::segment_key(
            &current.generation,
            header.first,
            header.last,
            header.last_at,
        );
        let (object, raw) = format::encode_segment(&header, &records)?;
        let size = object.len() as u64;
        self.target.put(&key, Bytes::from(object)).await?;
        self.store
            .release_backup_hold(header.last.saturating_sub(1));
        self.store.forget_applied_at(header.last);
        let current = self.current.as_mut().context("no backup generation")?;
        current.tip = header.last;
        current.tip_at = header.last_at;
        current.tip_mark = Some(mark);
        current.tip_segment = Some(key.clone());
        current.tip_dirty = true;
        // Compressed, as the newest base's size it is compared with.
        current.shipped_since_base += size;
        let mut status = self.status();
        status.shipped = Some(Shipped {
            index: header.last,
            at: header.last_at,
            time: format_time(header.last_at),
            segment: Some(key),
        });
        status.segments_shipped += 1;
        status.segment_bytes_shipped += size;
        status.segment_raw_bytes_shipped += raw as u64;
        Ok(())
    }

    /// Start a base if one is due.
    async fn maybe_base(&mut self) -> anyhow::Result<()> {
        if self.base.is_some() {
            return Ok(());
        }
        let Some(current) = &self.current else {
            return Ok(());
        };
        let due = match current.newest_base {
            None => self
                .retry_base_after
                .is_none_or(|after| Instant::now() >= after),
            Some((_, at, size)) => {
                now_ms().saturating_sub(at) >= self.config.base_interval.as_millis() as u64
                    || current.shipped_since_base >= self.config.base_after_bytes.max(size)
            }
        };
        if !due {
            return Ok(());
        }
        let image = self.store.backup_image().await;
        let at = now_ms();
        let Some(last) = image.last_applied() else {
            return Ok(());
        };
        // The base ends a segment: the next one starts after it.
        self.ship(last.index).await?;
        let Some(current) = &self.current else {
            return Ok(());
        };
        if current.tip != last.index || self.base.is_some() {
            // A new generation began, with its own base, or the entries
            // are not all written yet.
            return Ok(());
        }
        let mark = current.tip_mark.clone();
        self.spawn_base(image, at, mark);
        Ok(())
    }

    fn spawn_base(&mut self, image: BackupImage, at: u64, mark: Option<EntryMark>) {
        let Some(current) = self.current.as_mut() else {
            return;
        };
        let Some(last) = image.last_applied() else {
            return;
        };
        current.shipped_since_base = 0;
        let header = BaseHeader {
            generation: current.generation.clone(),
            last_log_id: Some(last),
            membership: image.membership(),
            at,
            last_sha256: mark.map(|mark| mark.sha256),
            contract: Contract::current(),
            node: self.id,
        };
        let key = format::base_key(&current.generation, last.index, at);
        let (cancel, cancelled) = watch::channel(false);
        let handle = tokio::spawn(upload_base(
            self.target.clone(),
            key,
            header,
            image,
            self.config.part_bytes,
            cancelled,
        ));
        self.base = Some(BaseTask {
            generation: current.generation.clone(),
            index: last.index,
            at,
            started: Instant::now(),
            cancel,
            handle,
        });
        self.status().base_in_progress = Some(BaseProgress {
            index: last.index,
            at,
            time: format_time(at),
        });
    }

    async fn poll_base(&mut self) {
        if !self
            .base
            .as_ref()
            .is_some_and(|task| task.handle.is_finished())
        {
            return;
        }
        let task = self.base.take().expect("checked above");
        let result = task
            .handle
            .await
            .map_err(anyhow::Error::new)
            .and_then(|result| result);
        self.status().base_in_progress = None;
        match result {
            Ok((bytes, raw)) => {
                let seconds = task.started.elapsed().as_secs_f64();
                tracing::info!(target: "flower::backup", generation = %task.generation,
                    index = task.index, bytes, raw_bytes = raw, seconds, "wrote a backup base");
                if let Some(current) = self.current.as_mut()
                    && current.generation == task.generation
                {
                    current.newest_base = Some((task.index, task.at, bytes));
                    current.tip_dirty = true;
                }
                self.retry_base_after = None;
                // Retention can now let older objects go.
                self.last_retention = None;
                let mut status = self.status();
                status.bases_written += 1;
                status.newest_base = Some(BaseStatus {
                    index: task.index,
                    at: task.at,
                    time: format_time(task.at),
                    bytes,
                    raw_bytes: Some(raw),
                    seconds: Some(seconds),
                });
            }
            Err(error) => {
                self.retry_base_after = Some(Instant::now() + BASE_RETRY);
                self.note(Err(error.context(format!("backup base at {}", task.index))));
            }
        }
    }

    async fn cancel_base(&mut self) {
        if let Some(task) = self.base.take() {
            let _ = task.cancel.send(true);
            let _ = tokio::time::timeout(Duration::from_secs(5), task.handle).await;
            self.status().base_in_progress = None;
        }
    }

    /// Record the newest shipped entry, every `tip_interval` at most unless
    /// `now`, once the generation has a base.
    async fn write_tip(&mut self, now: bool) -> anyhow::Result<()> {
        let Some(current) = &self.current else {
            return Ok(());
        };
        if current.newest_base.is_none()
            || !current.tip_dirty
            || (!now
                && current
                    .tip_written
                    .is_some_and(|written| written.elapsed() < self.config.tip_interval))
        {
            return Ok(());
        }
        let tip = Tip {
            generation: current.generation.clone(),
            index: current.tip,
            at: current.tip_at,
            log_id: current.tip_mark.as_ref().map(|mark| mark.log_id),
            sha256: current.tip_mark.as_ref().map(|mark| mark.sha256.clone()),
            segment: current.tip_segment.clone(),
            written: now_ms(),
        };
        self.target
            .put(
                &format::tip_key(&current.generation),
                Bytes::from(serde_json::to_vec_pretty(&tip)?),
            )
            .await?;
        if let Some(current) = self.current.as_mut() {
            current.tip_dirty = false;
            current.tip_written = Some(Instant::now());
        }
        Ok(())
    }

    fn poll_retention(&mut self) {
        if !self
            .retention
            .as_ref()
            .is_some_and(|task| task.is_finished())
        {
            return;
        }
        let task = self.retention.take().expect("checked above");
        let Some(outcome) = futures_util::FutureExt::now_or_never(task).and_then(Result::ok) else {
            return;
        };
        if let Some(error) = &outcome.error {
            tracing::warn!(target: "flower::backup", error = %error, "backup retention failed");
        } else if outcome.deleted > 0 {
            tracing::info!(target: "flower::backup", deleted = outcome.deleted,
                horizon = %format_time(outcome.horizon), "backup retention deleted objects");
        }
        self.status().retention = Some(RetentionStatus {
            time: format_time(now_ms()),
            horizon: format_time(outcome.horizon),
            deleted: outcome.deleted,
            error: outcome.error,
        });
    }

    fn maybe_retention(&mut self) {
        if self.retention.is_some()
            || self
                .last_retention
                .is_some_and(|last| last.elapsed() < self.config.retention_interval)
        {
            return;
        }
        let Some(current) = self
            .current
            .as_ref()
            .filter(|current| current.newest_base.is_some())
        else {
            return;
        };
        self.last_retention = Some(Instant::now());
        let target = self.target.clone();
        let generation = current.generation.clone();
        let retention = self.config.retention;
        self.retention = Some(tokio::spawn(async move {
            let horizon = now_ms().saturating_sub(retention.as_millis() as u64);
            match retain(&target, &generation, horizon).await {
                Ok(deleted) => RetentionOutcome {
                    horizon,
                    deleted,
                    error: None,
                },
                Err(error) => RetentionOutcome {
                    horizon,
                    deleted: 0,
                    error: Some(format!("{error:#}")),
                },
            }
        }));
    }
}

struct Found {
    index: u64,
    at: u64,
    mark: Option<EntryMark>,
    segment: Option<String>,
    newest_base: (u64, u64, u64),
}

/// Encode a base as its state is read, and upload it in parts as they fill.
async fn upload_base(
    target: Arc<Target>,
    key: String,
    header: BaseHeader,
    image: BackupImage,
    part_bytes: usize,
    mut cancelled: watch::Receiver<bool>,
) -> anyhow::Result<(u64, u64)> {
    let (sender, mut parts) = mpsc::channel::<Vec<u8>>(2);
    let encoder = BaseEncoder::new(&header)?;
    let mut encoding = Some(tokio::task::spawn_blocking(
        move || -> anyhow::Result<u64> {
            let mut writer = PartWriter {
                encoder: Some(encoder),
                sender,
                part_bytes,
            };
            image.encode(&mut writer)?;
            drop(image);
            let encoder = writer.encoder.take().expect("encoder until finished");
            let raw = encoder.raw();
            writer
                .sender
                .blocking_send(encoder.finish())
                .map_err(|_| anyhow::anyhow!("the base upload stopped"))?;
            Ok(raw)
        },
    ));
    let mut upload: Option<Upload> = None;
    let result = async {
        // One part is held back until the next arrives: the last may be
        // small, and a base of one part needs no multipart upload.
        let mut held: Option<Vec<u8>> = None;
        let mut bytes = 0u64;
        loop {
            let part = tokio::select! {
                part = parts.recv() => part,
                _ = cancelled.changed() => bail!("the base upload was cancelled"),
            };
            let Some(part) = part else { break };
            bytes += part.len() as u64;
            if let Some(previous) = held.replace(part) {
                if upload.is_none() {
                    upload = Some(target.begin_upload(&key).await?);
                }
                target
                    .upload_part(upload.as_mut().expect("begun above"), Bytes::from(previous))
                    .await?;
            }
        }
        let raw = encoding
            .take()
            .expect("encoding until awaited")
            .await
            .context("base encoder")??;
        let last = held.take().context("the base encoder produced nothing")?;
        match upload.take() {
            None => target.put(&key, Bytes::from(last)).await?,
            Some(mut begun) => {
                target.upload_part(&mut begun, Bytes::from(last)).await?;
                target.finish_upload(begun).await?;
            }
        }
        Ok((bytes, raw))
    }
    .await;
    if result.is_err() {
        // Stop the encoder, and let go of its state, before returning.
        drop(parts);
        if let Some(encoding) = encoding.take() {
            let _ = encoding.await;
        }
        if let Some(begun) = upload.take()
            && let Err(error) = target.abort_upload(begun).await
        {
            tracing::warn!(target: "flower::backup", key = %key, error = %format!("{error:#}"),
                "could not abort an unfinished base upload; a bucket lifecycle rule that aborts \
                 incomplete multipart uploads removes its parts");
        }
    }
    result
}

/// Hands encoded base bytes to the uploader a part at a time.
struct PartWriter {
    encoder: Option<BaseEncoder>,
    sender: mpsc::Sender<Vec<u8>>,
    part_bytes: usize,
}

impl std::io::Write for PartWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let encoder = self.encoder.as_mut().expect("encoder until finished");
        encoder.write_all(data)?;
        if encoder.pending() >= self.part_bytes {
            let part = encoder.take();
            self.sender
                .blocking_send(part)
                .map_err(|_| std::io::Error::other("the base upload stopped"))?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Apply retention at `horizon`: in each generation, keep the newest base
/// at or before it and everything after, and delete whole generations that
/// ended before it, and old generations that never got a base.
pub(super) async fn retain(target: &Target, current: &str, horizon: u64) -> anyhow::Result<u64> {
    let now = now_ms();
    let mut deleted = 0;
    for prefix in target.children("generations/").await? {
        let Some(generation) = prefix
            .strip_prefix("generations/")
            .and_then(|rest| rest.strip_suffix('/'))
            .filter(|id| format::generation_created(id).is_some())
        else {
            continue;
        };
        let mut bases = Vec::new();
        target
            .list(&format::bases_prefix(generation), None, |key, _| {
                if let Some((index, at)) = format::parse_base_key(key) {
                    bases.push((index, at, key.to_owned()));
                }
                true
            })
            .await?;
        bases.sort();
        if generation != current {
            let created = format::generation_created(generation).unwrap_or(0);
            let ended = match target.get(&format::tip_key(generation)).await? {
                Some(bytes) => serde_json::from_slice::<Tip>(&bytes)
                    .map(|tip| tip.at)
                    .unwrap_or(created),
                None => bases.last().map_or(created, |base| base.1),
            };
            let orphan = bases.is_empty()
                && generation < current
                && created.saturating_add(ORPHAN_AGE_MS) < now;
            if ended < horizon || orphan {
                deleted += delete_generation(target, &prefix).await?;
                continue;
            }
        }
        let Some(keep) = bases.iter().rfind(|base| base.1 <= horizon) else {
            continue;
        };
        let keep = keep.0;
        let mut old: Vec<String> = bases
            .iter()
            .filter(|base| base.0 < keep)
            .map(|base| base.2.clone())
            .collect();
        target
            .list(&format::segments_prefix(generation), None, |key, _| {
                match format::parse_segment_key(key) {
                    Some((_, last, _)) if last <= keep => {
                        old.push(key.to_owned());
                        true
                    }
                    Some((first, _, _)) if first > keep => false,
                    _ => true,
                }
            })
            .await?;
        deleted += delete_all(target, old).await?;
    }
    Ok(deleted)
}

async fn delete_generation(target: &Target, prefix: &str) -> anyhow::Result<u64> {
    let mut keys = Vec::new();
    target
        .list(prefix, None, |key, _| {
            keys.push(key.to_owned());
            true
        })
        .await?;
    // generation.json goes last: a generation is listed until it is gone.
    let record = format!("{prefix}generation.json");
    keys.retain(|key| *key != record);
    let deleted = delete_all(target, keys).await?;
    target.delete(&record).await?;
    Ok(deleted + 1)
}

async fn delete_all(target: &Target, keys: Vec<String>) -> anyhow::Result<u64> {
    let mut deleted = 0;
    let mut pending = futures_util::stream::FuturesUnordered::new();
    for key in keys {
        if pending.len() >= DELETE_CONCURRENCY
            && let Some(result) = pending.next().await
        {
            let () = result?;
            deleted += 1;
        }
        pending.push(async move { target.delete(&key).await });
    }
    while let Some(result) = pending.next().await {
        let () = result?;
        deleted += 1;
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600 * 1000;

    async fn keys(target: &Target) -> Vec<String> {
        let mut keys = Vec::new();
        target
            .list("", None, |key, _| {
                keys.push(key.to_owned());
                true
            })
            .await
            .unwrap();
        keys
    }

    async fn put(target: &Target, key: &str) {
        target.put(key, Bytes::from_static(b"x")).await.unwrap();
    }

    async fn tip(target: &Target, generation: &str, at: u64) {
        let tip = Tip {
            generation: generation.into(),
            index: 0,
            at,
            log_id: None,
            sha256: None,
            segment: None,
            written: at,
        };
        target
            .put(
                &format::tip_key(generation),
                Bytes::from(serde_json::to_vec(&tip).unwrap()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn retention_keeps_the_newest_base_before_the_horizon_and_what_follows() {
        let directory = tempfile::tempdir().unwrap();
        let target = Target::Directory(directory.path().to_owned());
        let now = now_ms();
        let horizon = now - 30 * 24 * HOUR;
        // An old generation that ended before the horizon: it goes whole.
        let ended = format!("{:013}-{:016x}", horizon - 10 * HOUR, 1);
        put(&target, &format::generation_key(&ended)).await;
        put(&target, &format::base_key(&ended, 10, horizon - 10 * HOUR)).await;
        put(
            &target,
            &format::segment_key(&ended, 11, 20, horizon - 2 * HOUR),
        )
        .await;
        tip(&target, &ended, horizon - 2 * HOUR).await;
        // A generation still inside the window: older objects go.
        let previous = format!("{:013}-{:016x}", horizon - 5 * HOUR, 2);
        put(&target, &format::generation_key(&previous)).await;
        put(
            &target,
            &format::base_key(&previous, 100, horizon - 5 * HOUR),
        )
        .await;
        put(
            &target,
            &format::segment_key(&previous, 101, 150, horizon - 4 * HOUR),
        )
        .await;
        put(
            &target,
            &format::base_key(&previous, 150, horizon - 4 * HOUR),
        )
        .await;
        put(
            &target,
            &format::segment_key(&previous, 151, 160, horizon - HOUR),
        )
        .await;
        put(
            &target,
            &format::segment_key(&previous, 161, 170, horizon + HOUR),
        )
        .await;
        put(&target, &format::base_key(&previous, 170, horizon + HOUR)).await;
        put(
            &target,
            &format::segment_key(&previous, 171, 180, horizon + 2 * HOUR),
        )
        .await;
        tip(&target, &previous, horizon + 2 * HOUR).await;
        // A generation that never got a base: gone once old enough.
        let orphan = format!("{:013}-{:016x}", now - 2 * HOUR, 3);
        put(&target, &format::generation_key(&orphan)).await;
        put(&target, &format::segment_key(&orphan, 1, 2, now - 2 * HOUR)).await;
        // One younger than that, which may yet get its base.
        let young = format!("{:013}-{:016x}", now - HOUR / 2, 4);
        put(&target, &format::generation_key(&young)).await;
        // The current generation, whose only base is before the horizon.
        let current = format!("{:013}-{:016x}", now - HOUR / 4, 5);
        put(&target, &format::generation_key(&current)).await;
        put(&target, &format::base_key(&current, 180, horizon - HOUR)).await;
        put(&target, &format::segment_key(&current, 181, 190, now)).await;

        let deleted = retain(&target, &current, horizon).await.unwrap();
        let mut expected = vec![
            format::generation_key(&previous),
            format::base_key(&previous, 150, horizon - 4 * HOUR),
            format::base_key(&previous, 170, horizon + HOUR),
            format::segment_key(&previous, 151, 160, horizon - HOUR),
            format::segment_key(&previous, 161, 170, horizon + HOUR),
            format::segment_key(&previous, 171, 180, horizon + 2 * HOUR),
            format::tip_key(&previous),
            format::generation_key(&young),
            format::generation_key(&current),
            format::base_key(&current, 180, horizon - HOUR),
            format::segment_key(&current, 181, 190, now),
        ];
        expected.sort();
        assert_eq!(keys(&target).await, expected);
        assert_eq!(deleted, 4 + 2 + 2);
        assert!(!directory.path().join("generations").join(&ended).exists());
        // Nothing more goes a second time.
        assert_eq!(retain(&target, &current, horizon).await.unwrap(), 0);
    }
}
