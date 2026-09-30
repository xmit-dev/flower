//! Restoring backups, offline: `flower backup restore` writes a data
//! directory (or a hosted replica's tables) for a new single-node cluster
//! holding the state as of a point in time or a log index, and `flower
//! backup list` says what can be restored.
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, bail, ensure};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};

use super::format::{self, BaseDecoder, Generation, Tip};
use super::target::Target;
use super::{Config, format_time};
use crate::consensus::store::{self, SharedDatabase, Storage, Store};

/// Where in the backed-up history to restore to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    /// Everything the newest generation holds.
    Latest,
    /// Every entry applied at or before this time (Unix ms).
    Time(u64),
    /// Every entry through this log index.
    Index(u64),
}

pub struct RestoreOptions {
    /// The backups (the replica's own, without `NAME/`: `replica` adds it).
    pub config: Config,
    pub data: PathBuf,
    /// The restored node's ID and the address other nodes will reach it at.
    pub id: u64,
    pub advertise: String,
    /// A hosted replica's name: its tables go into DATA/flower.redb, its
    /// backups are under NAME/.
    pub replica: Option<String>,
    pub point: Point,
    /// Restore from this generation rather than the one the point picks.
    pub generation: Option<String>,
    /// Report progress on stderr.
    pub progress: bool,
}

// Entries (and their JSON bytes) applied in one call.
const APPLY_ENTRIES: usize = 256;
const APPLY_BYTES: usize = 16 << 20;
// Segments downloaded ahead of the one being applied.
const PREFETCH: usize = 8;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BaseInfo {
    index: u64,
    at: u64,
    time: String,
    bytes: u64,
    #[serde(skip)]
    key: String,
}

struct GenerationInfo {
    id: String,
    record: Option<Generation>,
    bases: Vec<BaseInfo>,
    tip: Option<Tip>,
}

async fn generations(target: &Target) -> anyhow::Result<Vec<GenerationInfo>> {
    let mut found = Vec::new();
    for prefix in target.children("generations/").await? {
        let Some(id) = prefix
            .strip_prefix("generations/")
            .and_then(|rest| rest.strip_suffix('/'))
            .filter(|id| format::generation_created(id).is_some())
        else {
            continue;
        };
        let record = target
            .get(&format::generation_key(id))
            .await?
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        let tip = target
            .get(&format::tip_key(id))
            .await?
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        let mut bases = Vec::new();
        target
            .list(&format::bases_prefix(id), None, |key, bytes| {
                if let Some((index, at)) = format::parse_base_key(key) {
                    bases.push(BaseInfo {
                        index,
                        at,
                        time: format_time(at),
                        bytes,
                        key: key.to_owned(),
                    });
                }
                true
            })
            .await?;
        bases.sort_by_key(|base| base.index);
        found.push(GenerationInfo {
            id: id.to_owned(),
            record,
            bases,
            tip,
        });
    }
    Ok(found)
}

fn backups(config: &Config, replica: Option<&str>) -> anyhow::Result<Target> {
    match replica {
        Some(name) => config.for_replica(name).target(),
        None => config.target(),
    }
}

/// The newest entry a generation holds, from its tip and the segments after.
async fn newest_entry(
    target: &Target,
    generation: &GenerationInfo,
) -> anyhow::Result<Option<(u64, u64)>> {
    let start_after = match &generation.tip {
        Some(Tip {
            segment: Some(segment),
            ..
        }) => segment.clone(),
        Some(tip) => format::segments_from(&generation.id, tip.index + 1),
        None => match generation.bases.last() {
            Some(base) => format::segments_from(&generation.id, base.index + 1),
            None => return Ok(None),
        },
    };
    let mut newest = generation.tip.as_ref().map(|tip| (tip.index, tip.at));
    target
        .list(
            &format::segments_prefix(&generation.id),
            Some(&start_after),
            |key, _| {
                if let Some((_, last, at)) = format::parse_segment_key(key) {
                    newest = Some((last, at));
                }
                true
            },
        )
        .await?;
    Ok(newest.or_else(|| generation.bases.last().map(|base| (base.index, base.at))))
}

/// What the backups at `config` can restore, generation by generation.
pub async fn list(config: &Config, replica: Option<&str>) -> anyhow::Result<Value> {
    let target = backups(config, replica)?;
    let mut listed = Vec::new();
    for generation in generations(&target).await? {
        let newest = newest_entry(&target, &generation).await?;
        let restorable = match (generation.bases.first(), newest) {
            (Some(base), Some((index, at))) => json!({
                "from": {"index": base.index, "at": base.at, "time": base.time},
                "to": {"index": index, "at": at, "time": format_time(at)},
            }),
            _ => Value::Null,
        };
        listed.push(json!({
            "generation": generation.id,
            "created": generation.record.as_ref().map(|record| format_time(record.created)),
            "node": generation.record.as_ref().map(|record| record.node),
            "reason": generation.record.as_ref().map(|record| record.reason.clone()),
            "previous": generation.record.as_ref().and_then(|record| record.previous.clone()),
            "restoredFrom": generation.record.as_ref().and_then(|record| record.restored_from.clone()),
            "bases": generation.bases,
            "restorable": restorable,
        }));
    }
    Ok(json!({"target": target.describe(), "generations": listed}))
}

/// Restore the backups at `options.config` into `options.data`.
pub async fn restore(options: RestoreOptions) -> anyhow::Result<Value> {
    ensure!(options.id > 0, "node ID must be positive");
    crate::consensus::validate_address(&options.advertise)?;
    let target = backups(&options.config, options.replica.as_deref())?;
    let progress = |message: String| {
        if options.progress {
            eprintln!("flower backup restore: {message}");
        }
    };
    let all = generations(&target).await?;
    ensure!(!all.is_empty(), "no backups at {}", target.describe());
    let candidates: Vec<&GenerationInfo> = match &options.generation {
        Some(id) => vec![
            all.iter()
                .find(|generation| generation.id == *id)
                .with_context(|| format!("no generation {id} at {}", target.describe()))?,
        ],
        None => all.iter().collect(),
    };
    let qualifies = |base: &BaseInfo| match options.point {
        Point::Latest => true,
        Point::Time(time) => base.at <= time,
        Point::Index(index) => base.index <= index,
    };
    let Some(generation) = candidates
        .iter()
        .rev()
        .find(|generation| generation.bases.iter().any(qualifies))
    else {
        let oldest = all
            .iter()
            .flat_map(|generation| generation.bases.first())
            .min_by_key(|base| base.at);
        bail!(
            "no base at or before that point{}",
            oldest.map_or(String::new(), |base| format!(
                "; the oldest is at index {} ({})",
                base.index, base.time
            ))
        );
    };
    let base = generation
        .bases
        .iter()
        .rev()
        .find(|base| qualifies(base))
        .expect("the generation has one");
    let storage = prepare_storage(&options)?;
    let directory = match &storage {
        Storage::Directory(directory) => directory.clone(),
        Storage::Shared { directory, .. } => directory.clone(),
    };
    let mut store = Store::open(options.id, storage).await?;

    // The base.
    progress(format!(
        "generation {}, base at index {} ({}), {} bytes",
        generation.id, base.index, base.time, base.bytes
    ));
    let mut download = target
        .open(&base.key)
        .await?
        .with_context(|| format!("backup base {} vanished", base.key))?;
    std::fs::create_dir_all(&directory)?;
    let mut image = std::io::BufWriter::with_capacity(
        1 << 20,
        tempfile::tempfile_in(&directory).context("create the restore's temporary image")?,
    );
    let mut decoder = BaseDecoder::default();
    while let Some(chunk) = download.chunk().await? {
        decoder.feed(&chunk, &mut image)?;
        if let Some(header) = decoder.header() {
            ensure!(
                header.generation == generation.id,
                "backup base {} belongs to generation {}",
                base.key,
                header.generation
            );
            header.contract.check_readable("the backup base")?;
        }
    }
    let (header, raw) = decoder.finish()?;
    ensure!(
        header.last_log_id.map(|id| id.index) == Some(base.index),
        "backup base {} holds the state at another index",
        base.key
    );
    image.flush()?;
    let image = image.into_inner().map_err(|error| error.into_error())?;
    store
        .restore_base(header.last_log_id, header.membership.clone(), image)
        .await?;
    progress(format!("installed the base ({raw} bytes of state)"));
    let mut last_applied = header.last_log_id.expect("checked above");
    let mut last_at = header.at;
    let mut max_term = last_applied.leader_id.term;

    // The segments after it.
    let mut keys = Vec::new();
    let point = options.point;
    target
        .list(
            &format::segments_prefix(&generation.id),
            Some(&format::segments_from(&generation.id, base.index + 1)),
            |key, _| {
                let Some((first, last, at)) = format::parse_segment_key(key) else {
                    return true;
                };
                keys.push((key.to_owned(), first, last));
                match point {
                    Point::Latest => true,
                    Point::Time(time) => at <= time,
                    Point::Index(index) => last < index,
                }
            },
        )
        .await?;
    let mut segments = futures_util::stream::iter(keys)
        .map(|(key, first, last)| {
            let target = &target;
            async move {
                let bytes = target
                    .get(&key)
                    .await?
                    .with_context(|| format!("backup segment {key} vanished"))?;
                anyhow::Ok((key, first, last, bytes))
            }
        })
        .buffered(PREFETCH);
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    let mut replayed = 0u64;
    let mut segments_read = 0u64;
    let mut reached = false;
    'segments: while let Some(segment) = segments.next().await {
        let (key, first, last, bytes) = segment?;
        if last <= last_applied.index {
            continue;
        }
        ensure!(
            first <= last_applied.index + 1,
            "the backup has no entries {} through {} (a gap before {key})",
            last_applied.index + 1,
            first - 1
        );
        let (header, records) = format::decode_segment(&bytes)?;
        ensure!(
            header.generation == generation.id && header.first == first && header.last == last,
            "backup segment {key} does not match its name"
        );
        header.contract.check_readable("a backup segment")?;
        segments_read += 1;
        for record in records {
            if record.index <= last_applied.index {
                continue;
            }
            let beyond = match point {
                Point::Latest => false,
                Point::Time(time) => record.at > time,
                Point::Index(index) => record.index > index,
            };
            if beyond {
                reached = true;
                break 'segments;
            }
            let entry = store::decode_stored_entry(&record.bytes)?;
            ensure!(
                entry.log_id.index == record.index,
                "backup segment {key} holds entry {} as {}",
                entry.log_id.index,
                record.index
            );
            last_applied = entry.log_id;
            last_at = record.at;
            max_term = max_term.max(entry.log_id.leader_id.term);
            batch_bytes += record.bytes.len();
            batch.push(entry);
            replayed += 1;
            if batch.len() >= APPLY_ENTRIES || batch_bytes >= APPLY_BYTES {
                store.restore_apply(std::mem::take(&mut batch)).await?;
                batch_bytes = 0;
            }
        }
        if segments_read.is_multiple_of(1000) {
            progress(format!(
                "replayed {replayed} entries, through index {}",
                last_applied.index
            ));
        }
    }
    drop(segments);
    if !batch.is_empty() {
        store.restore_apply(batch).await?;
    }
    if let Point::Index(index) = point {
        ensure!(
            last_applied.index == index,
            "the backup holds entries through index {} only",
            last_applied.index
        );
    }
    let _ = reached;
    // Terms of the new cluster start far above the backed-up history's.
    let mut random = [0u8; 4];
    getrandom::fill(&mut random).map_err(|error| anyhow::anyhow!("random term: {error}"))?;
    let term = max_term + (1 << 20) + u64::from(u32::from_le_bytes(random) >> 12);
    let origin = json!({
        "target": target.describe(),
        "generation": generation.id,
        "index": last_applied.index,
        "at": last_at,
        "time": format_time(last_at),
        "base": base.index,
    });
    let finished = store
        .restore_finish(options.id, options.advertise.clone(), term, origin)
        .await?;
    let (_, revision) = store.restored().await;
    drop(store);
    progress(format!(
        "restored through index {} ({}), {replayed} entries replayed",
        finished.index,
        format_time(last_at)
    ));
    Ok(json!({
        "restored": {
            "target": target.describe(),
            "generation": generation.id,
            "base": {"index": base.index, "at": base.at, "time": base.time},
            "index": finished.index,
            "term": finished.leader_id.term,
            "at": last_at,
            "time": format_time(last_at),
            "entriesReplayed": replayed,
            "segmentsRead": segments_read,
            "revision": revision,
        },
        "node": {"id": options.id, "advertise": options.advertise, "term": term},
        "data": options.data,
        "replica": options.replica,
    }))
}

fn prepare_storage(options: &RestoreOptions) -> anyhow::Result<Storage> {
    std::fs::create_dir_all(&options.data)
        .with_context(|| format!("create {}", options.data.display()))?;
    let file = options.data.join("flower.redb");
    Ok(match &options.replica {
        None => {
            ensure!(
                !file.exists(),
                "{} already holds a database; restore into an empty data directory",
                options.data.display()
            );
            Storage::Directory(options.data.clone())
        }
        Some(name) => {
            let database = SharedDatabase::open(&file)?;
            let prefix = format!("{name}/");
            ensure!(
                !store::holds_replica(&database, &prefix)?,
                "{} already holds replica {name}; restore it into a database without it",
                file.display()
            );
            Storage::Shared {
                database,
                prefix,
                directory: options.data.join(name),
            }
        }
    })
}
