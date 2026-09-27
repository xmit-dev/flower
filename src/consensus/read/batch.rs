//! Bounded read cohorts. Unlike ordinary request single-flight, an arrival
//! cannot join work that has already started: its quorum proof must begin
//! after its own request, including at every hop of a replica read.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, bail};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

#[cfg(test)]
use super::READ_TIMEOUT;
use super::ReadFence;

#[derive(Clone, Copy, Debug)]
pub(in crate::consensus) struct Settings {
    capacity: usize,
    timeout: Duration,
}

impl Settings {
    pub(in crate::consensus) fn from_env(timeout: Duration) -> anyhow::Result<Self> {
        let configured = match std::env::var("FLOWER_READ_QUEUE_CAPACITY") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => {
                return Err(error).context("FLOWER_READ_QUEUE_CAPACITY must be a positive integer");
            }
        };
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        let mut settings = Self::parse(configured.as_deref(), cpus)?;
        settings.timeout = timeout;
        Ok(settings)
    }

    fn parse(configured: Option<&str>, cpus: usize) -> anyhow::Result<Self> {
        let capacity = match configured {
            Some(value) => value
                .parse::<usize>()
                .context("FLOWER_READ_QUEUE_CAPACITY must be a positive integer")?,
            None => cpus
                .max(1)
                .saturating_mul(64)
                .min(tokio::sync::Semaphore::MAX_PERMITS),
        };
        anyhow::ensure!(
            capacity > 0 && capacity <= tokio::sync::Semaphore::MAX_PERMITS,
            "FLOWER_READ_QUEUE_CAPACITY must be between 1 and {} (Tokio/platform limit)",
            tokio::sync::Semaphore::MAX_PERMITS
        );
        Ok(Self {
            capacity,
            timeout: crate::consensus::Limits::default().read_timeout,
        })
    }
}

type Outcome = Result<ReadFence, Arc<str>>;
type Pending = oneshot::Sender<Outcome>;

#[derive(Clone)]
pub(super) struct Batcher(Arc<Inner>);

struct Inner {
    sender: mpsc::Sender<Pending>,
    task: Mutex<Option<JoinHandle<()>>>,
    timeout: Duration,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            task.abort();
        }
    }
}

impl Batcher {
    pub(super) fn new<F, Fut>(mut operation: F, settings: Settings) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<ReadFence>> + Send + 'static,
    {
        let (sender, mut receiver) = mpsc::channel::<Pending>(settings.capacity);
        // The task owns only the receiver and operation, never its own sender
        // or Inner. Dropping the final dispatcher therefore aborts it without
        // an ownership cycle; explicit shutdown joins it before store teardown.
        let task = tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut group = Vec::new();
                if !first.is_closed() {
                    group.push(first);
                }
                // Let already-ready callers enroll without adding a timer or
                // a fixed latency floor to every query.
                tokio::task::yield_now().await;
                // Admission bounds retained memory; no separate arbitrary
                // cohort-size ceiling prevents sharing a larger queued burst.
                for _ in 1..settings.capacity {
                    let Ok(pending) = receiver.try_recv() else {
                        break;
                    };
                    if !pending.is_closed() {
                        group.push(pending);
                    }
                }
                group.retain(|pending| !pending.is_closed());
                if group.is_empty() {
                    continue;
                }
                // The cohort is now SEALED. Nothing arriving during this await
                // can receive this fence, even if the resulting log is unchanged.
                let result = tokio::time::timeout(settings.timeout, operation())
                    .await
                    .context("unavailable: read cohort timed out")
                    .and_then(|result| result)
                    .map_err(|error| Arc::<str>::from(format!("{error:#}")));
                for pending in group {
                    let _ = pending.send(result.clone());
                }
            }
        });
        Self(Arc::new(Inner {
            sender,
            task: Mutex::new(Some(task)),
            timeout: settings.timeout,
        }))
    }

    pub(super) async fn request(&self) -> anyhow::Result<ReadFence> {
        let (sender, receiver) = oneshot::channel();
        // No unbounded population of tasks waiting to send into a full queue.
        // Callers may retry ordinary UNAVAILABLE, exactly as after quorum loss.
        match self.0.sender.try_send(sender) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                bail!("unavailable: read cohort queue is full")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                bail!("unavailable: read dispatcher stopped")
            }
        }
        // Includes queue time. Timeout drops the receiver, letting the actor
        // skip this caller if its cohort has not begun yet.
        tokio::time::timeout(self.0.timeout, receiver)
            .await
            .context("unavailable: queued read timed out")?
            .context("unavailable: read dispatcher stopped")?
            .map_err(|message| anyhow::anyhow!("{message}"))
    }

    pub(super) async fn shutdown(&self) {
        let task = {
            self.0
                .task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        };
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}

#[cfg(test)]
mod tests;
