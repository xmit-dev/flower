//! Operator capacity settings. Defaults guide scheduling, not application size.
use anyhow::Result;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BatchMode {
    Adaptive,
    Fixed,
}

impl BatchMode {
    fn parse(value: Option<String>) -> Result<Self, String> {
        match value.as_deref() {
            None | Some("adaptive") => Ok(Self::Adaptive),
            Some("fixed") => Ok(Self::Fixed),
            _ => Err("FLOWER_WRITER_BATCH_MODE must be adaptive or fixed".into()),
        }
    }
}

pub(super) struct Settings {
    pub batch_mode: BatchMode,
    pub batch_size: Option<usize>,
    pub queue_capacity: usize,
    pub query_workers: usize,
    pub query_cache_bytes: usize,
    pub authorization_cache_bytes: usize,
    pub query_flight_bytes: usize,
    pub preparation_workers: usize,
    pub writer_preparation_workers: usize,
    pub control_workers: usize,
    pub deployment_workers: usize,
    pub preparation_memory_bytes: usize,
    pub control_memory_bytes: usize,
    pub deployment_memory_bytes: usize,
    pub queued_bytes: usize,
    pub control_queued_bytes: usize,
    pub watch_retained_bytes: usize,
    pub http_max_body_bytes: usize,
    pub writer_window: Duration,
    pub writer_batch_time: Duration,
    pub deployment_page_time: Duration,
    /// How long an online deployment whose base changed may prepare again
    /// in the writer lane at cutover; zero never does.
    pub deployment_cutover: Duration,
    pub maintenance_interval: Duration,
    pub maintenance_burst: Duration,
    pub watch_refresh: Duration,
    pub watch_keepalive: Duration,
    pub watch_send_timeout: Duration,
    pub watch_duty_percent: usize,
    /// Share of one core every write-woken watch refresh together may take;
    /// 0 means no budget.
    pub watch_budget_percent: usize,
}

fn nonnegative(name: &str, value: Option<String>, default: usize) -> Result<usize, String> {
    value.map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .map_err(|_| format!("{name} must be a nonnegative platform-sized integer"))
    })
}

fn positive(name: &str, value: Option<String>, default: usize) -> Result<usize, String> {
    match value {
        None if default > 0 => Ok(default),
        None => Err(format!("{name} default must be positive")),
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("{name} must be a positive platform-sized integer")),
    }
}

pub(super) fn watch_timeout(read: Duration, evaluation: Duration) -> anyhow::Result<Duration> {
    let timeout = read.checked_add(evaluation)
        .filter(|timeout| std::time::Instant::now().checked_add(*timeout).is_some())
        .ok_or_else(|| anyhow::anyhow!("combined FLOWER_READ_TIMEOUT_MS and FLOWER_EVALUATION_TIMEOUT_MS exceed the platform deadline representation"))?;
    Ok(timeout)
}

pub(super) fn settings() -> Result<&'static Settings> {
    static SETTINGS: OnceLock<Result<Settings, String>> = OnceLock::new();
    SETTINGS
        .get_or_init(|| {
            let cpus = std::thread::available_parallelism().map_or(1, usize::from);
            let option = |name: &str, default| {
                let value = match std::env::var(name) {
                    Ok(value) => Some(value),
                    Err(std::env::VarError::NotPresent) => None,
                    Err(error) => return Err(format!("invalid {name}: {error}")),
                };
                positive(name, value, default)
            };
            let bytes = |name: &str, default| {
                let value = match std::env::var(name) {
                    Ok(value) => Some(value),
                    Err(std::env::VarError::NotPresent) => None,
                    Err(error) => return Err(format!("invalid {name}: {error}")),
                };
                nonnegative(name, value, default)
            };
            let admission = |name: &str, default| {
                let value = option(name, default)?;
                if value > tokio::sync::Semaphore::MAX_PERMITS {
                    return Err(format!("{name} exceeds Tokio's admission counter"));
                }
                Ok(value)
            };
            let batch_mode = BatchMode::parse(
                std::env::var("FLOWER_WRITER_BATCH_MODE")
                    .map(Some)
                    .or_else(|error| match error {
                        std::env::VarError::NotPresent => Ok(None),
                        _ => Err(format!("invalid FLOWER_WRITER_BATCH_MODE: {error}")),
                    })?,
            )?;
            let batch_size = match std::env::var("FLOWER_WRITER_BATCH_SIZE") {
                Ok(value) => Some(positive("FLOWER_WRITER_BATCH_SIZE", Some(value), 64)?),
                Err(std::env::VarError::NotPresent) if batch_mode == BatchMode::Fixed => Some(64),
                Err(std::env::VarError::NotPresent) => None,
                Err(error) => return Err(format!("invalid FLOWER_WRITER_BATCH_SIZE: {error}")),
            };
            let duration = |name: &str, default| {
                let milliseconds = option(name, default)?;
                let value = Duration::from_millis(
                    u64::try_from(milliseconds)
                        .map_err(|_| format!("{name} exceeds the duration representation"))?,
                );
                if std::time::Instant::now().checked_add(value).is_none() {
                    return Err(format!(
                        "{name} exceeds the platform deadline representation"
                    ));
                }
                Ok(value)
            };
            let query_workers = admission("FLOWER_QUERY_WORKERS", cpus)?;
            let preparation_workers = admission("FLOWER_PREPARATION_WORKERS", query_workers)?;
            let control_workers = admission("FLOWER_CONTROL_WORKERS", 1)?;
            let deployment_workers = admission("FLOWER_DEPLOYMENT_WORKERS", 1)?;
            let evaluator = crate::evaluator::config::settings().map_err(|error| error.to_string())?;
            let per_job = evaluator.guest_memory_bytes.checked_add(evaluator.rust_memory_bytes)
                .ok_or("evaluation memory reservation overflow")?;
            let preparation_memory_bytes = option("FLOWER_PREPARATION_MEMORY_BYTES", per_job.saturating_mul(preparation_workers))?;
            let control_memory_bytes = option("FLOWER_CONTROL_MEMORY_BYTES", per_job.saturating_mul(control_workers))?;
            let deployment_memory_bytes = option("FLOWER_DEPLOYMENT_MEMORY_BYTES", per_job.saturating_mul(deployment_workers))?;
            if preparation_memory_bytes < per_job || control_memory_bytes < per_job || deployment_memory_bytes < per_job {
                return Err("Preparation/control/deployment memory budgets must fit at least one guest + Rust evaluation reservation".into());
            }
            let deployment_cutover = Duration::from_millis(
                u64::try_from(bytes("FLOWER_DEPLOYMENT_CUTOVER_MS", 1000)?)
                    .map_err(|_| "FLOWER_DEPLOYMENT_CUTOVER_MS exceeds the duration representation")?,
            );
            if std::time::Instant::now().checked_add(deployment_cutover).is_none() {
                return Err("FLOWER_DEPLOYMENT_CUTOVER_MS exceeds the platform deadline representation".into());
            }
            Ok(Settings {
                batch_mode,
                batch_size,
                queue_capacity: admission(
                    "FLOWER_WRITER_QUEUE_CAPACITY",
                    batch_size
                        .unwrap_or(cpus.saturating_mul(64))
                        .saturating_mul(2)
                        .min(tokio::sync::Semaphore::MAX_PERMITS),
                )?,
                query_workers,
                query_cache_bytes: bytes("FLOWER_QUERY_CACHE_BYTES", 16 * 1024 * 1024)?,
                authorization_cache_bytes: bytes("FLOWER_AUTHORIZATION_CACHE_BYTES", 16 * 1024 * 1024)?,
                query_flight_bytes: bytes("FLOWER_QUERY_FLIGHT_BYTES", 512 * 1024)?,
                preparation_workers,
                writer_preparation_workers: admission("FLOWER_WRITER_PREPARATION_WORKERS", preparation_workers)?,
                control_workers,
                deployment_workers,
                preparation_memory_bytes,
                control_memory_bytes,
                deployment_memory_bytes,
                queued_bytes: option("FLOWER_QUEUED_INPUT_BYTES", 64 * 1024 * 1024)?,
                control_queued_bytes: option("FLOWER_CONTROL_QUEUED_INPUT_BYTES", 16 * 1024 * 1024)?,
                watch_retained_bytes: option("FLOWER_WATCH_RETAINED_BYTES", 256 * 1024 * 1024)?,
                http_max_body_bytes: option("FLOWER_HTTP_MAX_BODY_BYTES", 8 * 1024 * 1024)?,
                writer_window: duration("FLOWER_WRITER_WINDOW_MS", 250)?,
                writer_batch_time: duration("FLOWER_WRITER_BATCH_MS", 50)?,
                deployment_page_time: duration("FLOWER_DEPLOYMENT_PAGE_MS", 200)?,
                deployment_cutover,
                maintenance_interval: duration("FLOWER_MAINTENANCE_INTERVAL_MS", 250)?,
                maintenance_burst: duration("FLOWER_MAINTENANCE_BURST_MS", 50)?,
                watch_refresh: duration("FLOWER_WATCH_REFRESH_MS", 250)?,
                watch_keepalive: duration("FLOWER_WATCH_KEEPALIVE_MS", 15_000)?,
                watch_send_timeout: duration("FLOWER_WATCH_SEND_TIMEOUT_MS", 5000)?,
                watch_duty_percent: option("FLOWER_WATCH_DUTY_PERCENT", 10)?,
                watch_budget_percent: bytes("FLOWER_WATCH_BUDGET_PERCENT", 50)?,
            })
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_watch_deadline_rejects_overflow() {
        assert_eq!(
            watch_timeout(Duration::from_secs(10), Duration::from_secs(5)).unwrap(),
            Duration::from_secs(15)
        );
        assert!(watch_timeout(Duration::MAX, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn capacity_settings_have_no_small_arbitrary_ceiling() {
        for size in [
            1,
            65,
            257,
            4097,
            65_536,
            tokio::sync::Semaphore::MAX_PERMITS,
            usize::MAX,
        ] {
            assert_eq!(
                positive("capacity", Some(size.to_string()), 1).unwrap(),
                size
            );
        }
        assert_eq!(positive("capacity", None, 17).unwrap(), 17);
        for value in ["0", "-1", "1.5", "", "18446744073709551616"] {
            assert!(positive("capacity", Some(value.into()), 1).is_err());
        }
    }
    #[test]
    fn cache_budgets_accept_zero_without_count_limits() {
        for size in [0, 1, 1_048_576, usize::MAX] {
            assert_eq!(
                nonnegative("cache", Some(size.to_string()), 1).unwrap(),
                size
            );
        }
        assert_eq!(nonnegative("cache", None, 0).unwrap(), 0);
        for value in ["-1", "1.5", "", "18446744073709551616"] {
            assert!(nonnegative("cache", Some(value.into()), 1).is_err());
        }
    }
    #[test]
    fn batching_mode_is_explicit_and_defaults_to_adaptive() {
        assert_eq!(BatchMode::parse(None).unwrap(), BatchMode::Adaptive);
        assert_eq!(
            BatchMode::parse(Some("adaptive".into())).unwrap(),
            BatchMode::Adaptive
        );
        assert_eq!(
            BatchMode::parse(Some("fixed".into())).unwrap(),
            BatchMode::Fixed
        );
        assert!(BatchMode::parse(Some("automatic".into())).is_err());
    }
}
