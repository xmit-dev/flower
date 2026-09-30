//! Continuous backups to S3 (or an S3-compatible store, or a directory),
//! restorable to any point in time within their retention.
//!
//! The leader ships every applied log entry, as stored, in segments of
//! consecutive entries, each entry stamped with when it was applied, about
//! once a second; now and then it also writes a base, the whole state as a
//! snapshot transfer encodes it. A generation is one unbroken history: a
//! base, then segments that continue it without a gap, and later bases. A
//! new leader continues the newest generation when its own log holds the
//! generation's last shipped entry (same log ID, same bytes), and otherwise
//! starts a new one with a base of its own. Retention keeps, per generation,
//! the newest base at or before the horizon and everything after it.
//!
//! A restore (`flower backup restore`, offline) picks a generation and its
//! newest base at or before the point asked for, replays the segments after
//! it up to that point through the state machine, and leaves a data
//! directory for a new single-node cluster.
//!
//! ```text
//! ROOT/generations/{created:013}-{random:016x}/generation.json
//! ROOT/generations/…/tip.json
//! ROOT/generations/…/bases/{index:020}-{at:013}.base
//! ROOT/generations/…/log/{first:020}-{last:020}-{last_at:013}.seg
//! ```
mod format;
mod restore;
mod s3;
mod ship;
mod target;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use serde_json::Value;

pub use restore::{Point, RestoreOptions, list, restore};
use s3::{Credentials, Location};
use target::Target;

/// How and where a replica backs up.
#[derive(Clone, Debug)]
pub struct Config {
    destination: Destination,
    /// How often the leader ships what it has applied.
    pub interval: Duration,
    /// Stored entry bytes per segment, at most (one entry may exceed it).
    pub segment_max_bytes: usize,
    /// A new base once the newest is this old…
    pub base_interval: Duration,
    /// …or once this many entry bytes have been shipped since, or as many as
    /// the newest base holds if that is more.
    pub base_after_bytes: u64,
    /// How far back restores reach.
    pub retention: Duration,
    /// Purged log entries a replica keeps for its backup, at most.
    pub hold_max_bytes: u64,
    /// Multipart upload part size of bases.
    pub part_bytes: usize,
    /// How often the newest shipped entry is recorded (`tip.json`).
    pub tip_interval: Duration,
    /// How often retention runs, besides after each base.
    pub retention_interval: Duration,
}

#[derive(Clone, Debug)]
enum Destination {
    S3 {
        location: Location,
        credentials: Credentials,
        root: String,
    },
    Directory(PathBuf),
}

const DEFAULT_INTERVAL: Duration = Duration::from_millis(1000);
const DEFAULT_SEGMENT_MAX_BYTES: usize = 16 << 20;
const DEFAULT_BASE_INTERVAL: Duration = Duration::from_secs(24 * 3600);
const DEFAULT_BASE_AFTER_BYTES: u64 = 64 << 20;
const DEFAULT_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);
const DEFAULT_HOLD_MAX_BYTES: u64 = 1 << 30;
const DEFAULT_PART_BYTES: usize = 16 << 20;

impl Config {
    /// The configuration in `FLOWER_BACKUP_*`, or `None` without
    /// `FLOWER_BACKUP_URL`.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        Self::from_lookup(None, |name| std::env::var(name).ok())
    }

    /// The configuration in `FLOWER_BACKUP_*`, for backups at `url` instead
    /// of `FLOWER_BACKUP_URL`'s when given.
    pub fn from_env_with_url(url: Option<&str>) -> anyhow::Result<Option<Self>> {
        Self::from_lookup(url, |name| std::env::var(name).ok())
    }

    fn from_lookup(
        url: Option<&str>,
        get: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let get = |name: &str| get(name).filter(|value| !value.trim().is_empty());
        let Some(url) = url.map(str::to_owned).or_else(|| get("FLOWER_BACKUP_URL")) else {
            return Ok(None);
        };
        let destination = if let Some(path) = url.strip_prefix("file://") {
            let path = PathBuf::from(path);
            ensure!(
                path.is_absolute(),
                "FLOWER_BACKUP_URL file:// needs an absolute path (file:///…)"
            );
            Destination::Directory(path)
        } else if let Some(rest) = url.strip_prefix("s3://") {
            let (bucket, root) = rest.split_once('/').unwrap_or((rest, ""));
            ensure!(
                !bucket.is_empty(),
                "FLOWER_BACKUP_URL s3:// needs a bucket (s3://BUCKET/PREFIX)"
            );
            let root = root.trim_matches('/');
            ensure!(
                !root.split('/').any(|part| part == "." || part == "..") && !root.contains("//"),
                "FLOWER_BACKUP_URL has an invalid prefix"
            );
            let root = if root.is_empty() {
                String::new()
            } else {
                format!("{root}/")
            };
            let region = get("FLOWER_BACKUP_S3_REGION")
                .or_else(|| get("AWS_REGION"))
                .or_else(|| get("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|| "us-east-1".into());
            let endpoint = get("FLOWER_BACKUP_S3_ENDPOINT");
            let addressing = get("FLOWER_BACKUP_S3_ADDRESSING");
            let virtual_host = match addressing.as_deref() {
                None => endpoint.is_none(),
                Some("path") => false,
                Some("virtual") => true,
                Some(other) => {
                    bail!("FLOWER_BACKUP_S3_ADDRESSING must be path or virtual, not {other}")
                }
            };
            let (scheme, authority, base_path) = match &endpoint {
                Some(endpoint) => {
                    let parsed = reqwest::Url::parse(endpoint)
                        .context("FLOWER_BACKUP_S3_ENDPOINT is not a URL")?;
                    ensure!(
                        matches!(parsed.scheme(), "http" | "https")
                            && parsed.query().is_none()
                            && parsed.username().is_empty(),
                        "FLOWER_BACKUP_S3_ENDPOINT must be an http:// or https:// URL"
                    );
                    let host = parsed
                        .host_str()
                        .context("FLOWER_BACKUP_S3_ENDPOINT has no host")?;
                    let authority = match parsed.port() {
                        Some(port) => format!("{host}:{port}"),
                        None => host.to_owned(),
                    };
                    (
                        parsed.scheme().to_owned(),
                        authority,
                        parsed.path().trim_end_matches('/').to_owned(),
                    )
                }
                None => (
                    "https".into(),
                    format!("s3.{region}.amazonaws.com"),
                    String::new(),
                ),
            };
            ensure!(
                !virtual_host || !bucket.contains('.') || scheme == "http",
                "a bucket name with dots needs FLOWER_BACKUP_S3_ADDRESSING=path over HTTPS"
            );
            let access_key_id = get("FLOWER_BACKUP_S3_ACCESS_KEY_ID")
                .or_else(|| get("AWS_ACCESS_KEY_ID"))
                .context("S3 backups need FLOWER_BACKUP_S3_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID)")?;
            let secret_access_key = get("FLOWER_BACKUP_S3_SECRET_ACCESS_KEY")
                .or_else(|| get("AWS_SECRET_ACCESS_KEY"))
                .context(
                    "S3 backups need FLOWER_BACKUP_S3_SECRET_ACCESS_KEY (or AWS_SECRET_ACCESS_KEY)",
                )?;
            let session_token =
                get("FLOWER_BACKUP_S3_SESSION_TOKEN").or_else(|| get("AWS_SESSION_TOKEN"));
            Destination::S3 {
                location: Location {
                    scheme,
                    authority,
                    base_path,
                    bucket: bucket.to_owned(),
                    virtual_host,
                    region,
                },
                credentials: Credentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                },
                root,
            }
        } else {
            bail!("FLOWER_BACKUP_URL must start with s3:// or file://");
        };
        let millis = |name: &str, default: Duration| -> anyhow::Result<Duration> {
            match get(name) {
                None => Ok(default),
                Some(value) => value
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value > 0)
                    .map(Duration::from_millis)
                    .with_context(|| format!("{name} must be a positive integer (milliseconds)")),
            }
        };
        let bytes = |name: &str, default: u64, min: u64| -> anyhow::Result<u64> {
            match get(name) {
                None => Ok(default),
                Some(value) => value
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value >= min)
                    .with_context(|| format!("{name} must be an integer of at least {min}")),
            }
        };
        let config = Self {
            destination,
            interval: millis("FLOWER_BACKUP_INTERVAL_MS", DEFAULT_INTERVAL)?,
            segment_max_bytes: bytes(
                "FLOWER_BACKUP_SEGMENT_MAX_BYTES",
                DEFAULT_SEGMENT_MAX_BYTES as u64,
                1,
            )? as usize,
            base_interval: millis("FLOWER_BACKUP_BASE_INTERVAL_MS", DEFAULT_BASE_INTERVAL)?,
            base_after_bytes: bytes(
                "FLOWER_BACKUP_BASE_AFTER_BYTES",
                DEFAULT_BASE_AFTER_BYTES,
                1,
            )?,
            retention: millis("FLOWER_BACKUP_RETENTION_MS", DEFAULT_RETENTION)?,
            hold_max_bytes: bytes("FLOWER_BACKUP_HOLD_MAX_BYTES", DEFAULT_HOLD_MAX_BYTES, 1)?,
            part_bytes: bytes(
                "FLOWER_BACKUP_PART_BYTES",
                DEFAULT_PART_BYTES as u64,
                target::MIN_PART_BYTES as u64,
            )? as usize,
            tip_interval: Duration::from_secs(10),
            retention_interval: Duration::from_secs(3600),
        };
        Ok(Some(config))
    }

    /// Backups to a directory, with the default settings.
    pub fn directory(path: impl Into<PathBuf>) -> Self {
        Self {
            destination: Destination::Directory(path.into()),
            interval: DEFAULT_INTERVAL,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            base_interval: DEFAULT_BASE_INTERVAL,
            base_after_bytes: DEFAULT_BASE_AFTER_BYTES,
            retention: DEFAULT_RETENTION,
            hold_max_bytes: DEFAULT_HOLD_MAX_BYTES,
            part_bytes: DEFAULT_PART_BYTES,
            tip_interval: Duration::from_secs(10),
            retention_interval: Duration::from_secs(3600),
        }
    }

    /// The backups of a hosted replica, under `NAME/` of the root.
    pub fn for_replica(&self, name: &str) -> Self {
        let mut config = self.clone();
        config.destination = match &self.destination {
            Destination::S3 {
                location,
                credentials,
                root,
            } => Destination::S3 {
                location: location.clone(),
                credentials: credentials.clone(),
                root: format!("{root}{name}/"),
            },
            Destination::Directory(path) => Destination::Directory(path.join(name)),
        };
        config
    }

    fn target(&self) -> anyhow::Result<Target> {
        match &self.destination {
            Destination::S3 {
                location,
                credentials,
                root,
            } => Target::s3(location.clone(), credentials.clone(), root.clone()),
            Destination::Directory(path) => Ok(Target::Directory(path.clone())),
        }
    }

    /// Where backups go, without credentials.
    pub fn describe(&self) -> String {
        match self.target() {
            Ok(target) => target.describe(),
            Err(error) => format!("unusable backup target: {error:#}"),
        }
    }
}

/// A replica's running backup, which Consensus handles share.
pub(super) struct Backup {
    status: Arc<std::sync::Mutex<ship::Status>>,
    store: super::store::Store,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Backup {
    /// Make the store keep what backups need. Call before Raft starts.
    pub(super) async fn prepare(
        config: &Config,
        store: &super::store::Store,
    ) -> anyhow::Result<()> {
        config.target()?;
        store.enable_backup(config.hold_max_bytes).await
    }

    pub(super) fn start(
        config: Config,
        id: u64,
        raft: super::FlowerRaft,
        store: super::store::Store,
    ) -> anyhow::Result<Arc<Self>> {
        let target = Arc::new(config.target()?);
        let status = Arc::new(std::sync::Mutex::new(ship::Status::new(target.describe())));
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let shipper = ship::Shipper::new(config, id, target, raft, store.clone(), status.clone());
        let task = tokio::spawn(shipper.run(stopped));
        Ok(Arc::new(Self {
            status,
            store,
            stop,
            task: tokio::sync::Mutex::new(Some(task)),
        }))
    }

    pub(super) fn status(&self) -> Value {
        let mut status = serde_json::to_value(&*self.status.lock().expect("backup status lock"))
            .unwrap_or_default();
        status["hold"] = serde_json::to_value(self.store.backup_hold()).unwrap_or_default();
        status
    }

    /// Ship what is left, briefly, and stop.
    pub(super) async fn stop(&self) {
        let _ = self.stop.send(true);
        let Some(mut task) = self.task.lock().await.take() else {
            return;
        };
        if tokio::time::timeout(Duration::from_secs(10), &mut task)
            .await
            .is_err()
        {
            tracing::warn!(target: "flower::backup", "backup did not stop in 10 s; abandoning it");
            task.abort();
            let _ = task.await;
        }
    }
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 15) as usize] as char);
    }
    encoded
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    format::sha256_hex(bytes)
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Year, month, day, hour, minute and second (UTC) of a Unix time in ms.
fn civil_time(ms: u64) -> (i64, u32, u32, u32, u32, u32) {
    let seconds = (ms / 1000) as i64;
    let days = seconds.div_euclid(86_400) + 719_468;
    let of_day = seconds.rem_euclid(86_400);
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * shifted + 2) / 5 + 1) as u32;
    let month = if shifted < 10 {
        shifted + 3
    } else {
        shifted - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (
        year,
        month,
        day,
        (of_day / 3600) as u32,
        (of_day % 3600 / 60) as u32,
        (of_day % 60) as u32,
    )
}

/// RFC 3339 in UTC with milliseconds.
pub fn format_time(ms: u64) -> String {
    let (year, month, day, hour, minute, second) = civil_time(ms);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}Z",
        ms % 1000
    )
}

/// A Unix time in milliseconds, or an RFC 3339 time such as
/// `2026-09-30T19:23:00Z` or `2026-09-30T12:23:00.250-07:00`.
pub fn parse_time(text: &str) -> anyhow::Result<u64> {
    let text = text.trim();
    if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        return text.parse().context("time out of range");
    }
    let invalid = || anyhow::anyhow!("{text:?} is neither Unix milliseconds nor an RFC 3339 time");
    let bytes = text.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(invalid());
    }
    let number = |range: std::ops::Range<usize>| -> anyhow::Result<u32> {
        let part = text.get(range).ok_or_else(invalid)?;
        if !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        part.parse().map_err(|_| invalid())
    };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    let mut rest = &text[19..];
    let mut millis = 0u64;
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(invalid());
        }
        let padded = format!("{:0<3}", &fraction[..digits.min(3)]);
        millis = padded.parse().map_err(|_| invalid())?;
        rest = &fraction[digits..];
    }
    let offset_minutes: i64 = match rest {
        "Z" | "z" => 0,
        _ if rest.len() == 6
            && matches!(rest.as_bytes()[0], b'+' | b'-')
            && rest.as_bytes()[3] == b':' =>
        {
            let hours: i64 = rest[1..3].parse().map_err(|_| invalid())?;
            let minutes: i64 = rest[4..6].parse().map_err(|_| invalid())?;
            let sign = if rest.starts_with('-') { -1 } else { 1 };
            sign * (hours * 60 + minutes)
        }
        _ => return Err(invalid()),
    };
    ensure!(
        (1..=12).contains(&month)
            && (1..=31).contains(&day)
            && hour < 24
            && minute < 60
            && second < 61,
        "{text:?} is not a valid time"
    );
    let days = days_from_civil(i64::from(year), month, day);
    let seconds =
        days * 86_400 + i64::from(hour * 3600 + minute * 60 + second.min(59)) - offset_minutes * 60;
    ensure!(seconds >= 0, "{text:?} is before 1970");
    Ok(seconds as u64 * 1000 + millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_format_and_parse_as_rfc_3339() {
        assert_eq!(format_time(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_time(1_369_353_600_000), "2013-05-24T00:00:00.000Z");
        assert_eq!(format_time(951_782_400_123), "2000-02-29T00:00:00.123Z");
        assert_eq!(
            parse_time("2013-05-24T00:00:00Z").unwrap(),
            1_369_353_600_000
        );
        assert_eq!(parse_time("1369353600000").unwrap(), 1_369_353_600_000);
        assert_eq!(
            parse_time("2026-09-30T12:23:00.25-07:00").unwrap(),
            parse_time("2026-09-30T19:23:00.250Z").unwrap()
        );
        assert_eq!(
            parse_time("2000-02-29t00:00:00.1234z").unwrap(),
            951_782_400_123
        );
        for ms in [
            0,
            86_399_999,
            951_782_400_123,
            4_102_444_800_000,
            1_790_000_000_001,
        ] {
            assert_eq!(parse_time(&format_time(ms)).unwrap(), ms);
        }
        for invalid in [
            "",
            "yesterday",
            "2026-09-30",
            "2026-09-30T19:23:00",
            "2026-13-01T00:00:00Z",
            "2026-09-30T19:23:00+0700",
            "1969-12-31T23:59:59Z",
        ] {
            assert!(parse_time(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn configuration_reads_s3_and_directory_targets() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).to_owned())
            }
        };
        assert!(Config::from_lookup(None, env(&[])).unwrap().is_none());
        let garage = Config::from_lookup(
            None,
            env(&[
                ("FLOWER_BACKUP_URL", "s3://flower-backup/live/main/"),
                ("FLOWER_BACKUP_S3_ENDPOINT", "http://127.0.0.1:3900"),
                ("FLOWER_BACKUP_S3_REGION", "garage"),
                ("FLOWER_BACKUP_S3_ACCESS_KEY_ID", "GK1"),
                ("FLOWER_BACKUP_S3_SECRET_ACCESS_KEY", "secret"),
                ("FLOWER_BACKUP_INTERVAL_MS", "500"),
            ]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(garage.interval, Duration::from_millis(500));
        assert_eq!(garage.retention, DEFAULT_RETENTION);
        assert_eq!(
            garage.describe(),
            "s3://flower-backup/live/main/ at http://127.0.0.1:3900 (garage, path-style)"
        );
        assert_eq!(
            garage.for_replica("a").describe(),
            "s3://flower-backup/live/main/a/ at http://127.0.0.1:3900 (garage, path-style)"
        );
        let aws = Config::from_lookup(
            Some("s3://bucket"),
            env(&[
                ("FLOWER_BACKUP_URL", "file:///ignored"),
                ("AWS_REGION", "eu-west-3"),
                ("AWS_ACCESS_KEY_ID", "AKIA"),
                ("AWS_SECRET_ACCESS_KEY", "secret"),
            ]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            aws.describe(),
            "s3://bucket/ at https://s3.eu-west-3.amazonaws.com (eu-west-3, virtual-hosted)"
        );
        assert!(!format!("{aws:?}").contains("secret"));
        let directory = Config::from_lookup(Some("file:///var/backups/flower"), env(&[]))
            .unwrap()
            .unwrap();
        assert_eq!(directory.describe(), "file:///var/backups/flower");
        for (url, pairs) in [
            ("s3://bucket/x", &[][..]),
            ("file://relative", &[][..]),
            ("gs://bucket", &[][..]),
            (
                "s3:///x",
                &[("AWS_ACCESS_KEY_ID", "a"), ("AWS_SECRET_ACCESS_KEY", "b")][..],
            ),
        ] {
            let pairs: &'static [(&str, &str)] = Box::leak(pairs.to_vec().into_boxed_slice());
            assert!(Config::from_lookup(Some(url), env(pairs)).is_err(), "{url}");
        }
        assert!(
            Config::from_lookup(
                Some("file:///x"),
                env(&[("FLOWER_BACKUP_PART_BYTES", "1024")])
            )
            .is_err()
        );
    }
}
