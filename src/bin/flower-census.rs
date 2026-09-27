//! Census of a data directory's redb file: bytes per table, and per key class
//! for application records and Raft log entries.
//!
//!   flower-census FILE [CLASS]   print the census, and up to 12 records of CLASS
//!   flower-census FILE log       also print the median Raft log entry
//!   flower-census FILE compact   count allocated pages, then compact FILE in place
//!
//! Point it at a copy of a live file: opening a file repairs it.
use anyhow::{Context, Result};
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
};
use std::collections::BTreeMap;

#[derive(Default)]
struct Tally {
    count: u64,
    keys: u64,
    values: u64,
}

impl Tally {
    fn add(&mut self, key: usize, value: usize) {
        self.count += 1;
        self.keys += key as u64;
        self.values += value as u64;
    }
}

fn print(title: &str, tallies: &BTreeMap<String, Tally>) {
    println!("{title}");
    let mut rows: Vec<_> = tallies.iter().collect();
    rows.sort_by_key(|(_, tally)| std::cmp::Reverse(tally.keys + tally.values));
    for (class, tally) in rows {
        println!(
            "  {:>8} rows {:>10} key B {:>10} value B  {class}",
            tally.count, tally.keys, tally.values
        );
    }
}

fn class(key: &str) -> String {
    let prefix = key.split(':').next().unwrap_or(key);
    match prefix {
        "index-entry" | "ordered-entry" | "index-bucket" => {
            // Group by collection.
            let rest = &key[prefix.len() + 1..];
            let collection = rest
                .strip_prefix("[\"")
                .and_then(|rest| rest.split('"').next())
                .unwrap_or("?");
            format!("{prefix} {collection}")
        }
        "source" => {
            let rest = &key[prefix.len() + 1..];
            let collection = serde_json::from_str::<(String, String)>(rest)
                .map(|(collection, _)| collection)
                .unwrap_or_else(|_| "?".into());
            format!("source {collection}")
        }
        _ => prefix.to_owned(),
    }
}

/// Stored JSON, unpacked (see src/consensus/packed.rs).
fn unpack(bytes: &[u8]) -> Vec<u8> {
    match bytes {
        [0, a, b, c, d, block @ ..] => {
            let length = u32::from_le_bytes([*a, *b, *c, *d]) as usize;
            lz4_flex::block::decompress(block, length).expect("packed JSON")
        }
        _ => bytes.to_vec(),
    }
}

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: flower-census FILE [SAMPLE-CLASS]")?;
    let sample = std::env::args().nth(2);
    if sample.as_deref() == Some("compact") {
        let before = std::fs::metadata(&path)?.len();
        let mut database = Database::open(&path)?;
        // A commit frees the pages a file closed mid-use still held.
        database.begin_write()?.commit()?;
        let stats = database.begin_write()?.stats()?;
        println!(
            "allocated {} pages x {} B = {} B, leaf {} branch {}, stored leaf {} B, fragmented {} B",
            stats.allocated_pages(),
            stats.page_size(),
            stats.allocated_pages() * stats.page_size() as u64,
            stats.leaf_pages(),
            stats.branch_pages(),
            stats.stored_bytes(),
            stats.fragmented_bytes()
        );
        let started = std::time::Instant::now();
        while database.compact()? {}
        drop(database);
        println!(
            "compacted {before} B -> {} B in {:?}",
            std::fs::metadata(&path)?.len(),
            started.elapsed()
        );
        return Ok(());
    }
    let database = Database::open(&path)?;
    let read = database.begin_read()?;
    for table in read.list_tables()? {
        println!("table {}", table.name());
    }
    let stats = |name: &str| -> Result<()> {
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name);
        if let Ok(table) = read.open_table(definition) {
            let stats = table.stats()?;
            println!(
                "{name}: {} rows, stored {} B, metadata {} B, fragmented {} B",
                table.len()?,
                stats.stored_bytes(),
                stats.metadata_bytes(),
                stats.fragmented_bytes()
            );
        }
        Ok(())
    };
    stats("application_data_v4")?;
    stats("application_requests_v4")?;
    let logs: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs_v1");
    let logs = read.open_table(logs)?;
    let log_stats = logs.stats()?;
    println!(
        "raft_logs_v1: {} rows, stored {} B, metadata {} B, fragmented {} B",
        logs.len()?,
        log_stats.stored_bytes(),
        log_stats.metadata_bytes(),
        log_stats.fragmented_bytes()
    );
    let data: TableDefinition<&[u8], &[u8]> = TableDefinition::new("application_data_v4");
    let data = read.open_table(data)?;
    let mut tallies = BTreeMap::<String, Tally>::new();
    let mut packed = BTreeMap::<String, Tally>::new();
    let mut samples = 0;
    for entry in data.iter()? {
        let (key, value) = entry?;
        let key = std::str::from_utf8(key.value())?.to_owned();
        let class = class(&key);
        if let Some(sample) = &sample
            && class.starts_with(sample.as_str())
            && samples < 12
        {
            samples += 1;
            let json = unpack(&value.value()[8..]);
            let json = String::from_utf8_lossy(&json);
            println!("{key}\n    = {}", &json[..json.len().min(300)]);
        }
        let json = unpack(&value.value()[8..]);
        packed
            .entry(class.clone())
            .or_default()
            .add(0, json.len() + 8);
        tallies
            .entry(class)
            .or_default()
            .add(key.len(), value.value().len());
    }
    print("application_data_v4", &tallies);
    print("application_data_v4 values unpacked", &packed);
    let requests: TableDefinition<&[u8], &[u8]> = TableDefinition::new("application_requests_v4");
    let requests = read.open_table(requests)?;
    let mut tally = Tally::default();
    let mut shapes = BTreeMap::<String, Tally>::new();
    for entry in requests.iter()? {
        let (key, value) = entry?;
        tally.add(key.value().len(), value.value().len());
        // Receipts by the shape of their result: its type, or its keys.
        let receipt: serde_json::Value = serde_json::from_slice(&unpack(&value.value()[8..]))?;
        let shape = match &receipt["result"] {
            serde_json::Value::Object(object) => {
                let keys: Vec<_> = object.keys().map(String::as_str).collect();
                format!("{{{}}}", keys.join(","))
            }
            serde_json::Value::Array(_) => "array".into(),
            serde_json::Value::String(_) => "string".into(),
            other => other.to_string().chars().take(20).collect(),
        };
        shapes
            .entry(shape)
            .or_default()
            .add(key.value().len(), value.value().len());
    }
    print(
        "application_requests_v4",
        &BTreeMap::from([("receipts".to_owned(), tally)]),
    );
    print("receipts by result shape", &shapes);
    let mut first = None;
    let mut last = 0;
    let mut kinds = BTreeMap::<String, Tally>::new();
    let mut parts = BTreeMap::<String, Tally>::new();
    // Receipted items by whether their entry wrote anything (key column: items).
    let mut commits = BTreeMap::<String, Tally>::new();
    // Rewrites of a key put earlier in the log: full bytes, and delta bytes.
    let mut deltas = BTreeMap::<String, Tally>::new();
    let mut previous_puts = std::collections::HashMap::<String, serde_json::Value>::new();
    let mut largest = Vec::new();
    for entry in logs.iter()? {
        let (index, value) = entry?;
        let index = index.value();
        first.get_or_insert(index);
        last = index;
        let stored = value.value();
        let bytes = unpack(stored);
        let bytes = bytes.as_slice();
        let entry: serde_json::Value = serde_json::from_slice(bytes)?;
        let payload = &entry["payload"];
        let normal = &payload["Normal"];
        let command = if normal["batch"].is_object() {
            "batch"
        } else if normal["request_id"].is_string() {
            "single"
        } else if normal.is_object() {
            "other normal"
        } else if payload["Membership"].is_object() {
            "membership"
        } else {
            "blank"
        };
        kinds
            .entry(command.into())
            .or_default()
            .add(8, stored.len());
        kinds
            .entry(format!("{command} unpacked"))
            .or_default()
            .add(8, bytes.len());
        let body = if command == "batch" {
            &normal["batch"]
        } else {
            normal
        };
        if let Some(puts) = body["puts"].as_object() {
            for (key, value) in puts {
                let size = serde_json::to_string(value)?.len();
                parts
                    .entry(format!("put {}", class(key)))
                    .or_default()
                    .add(key.len() + 3, size + 1);
                // What a top-level field delta against the previous put would take.
                if let (Some(previous), Some(object)) = (
                    previous_puts
                        .get(key)
                        .and_then(|v: &serde_json::Value| v.as_object().cloned()),
                    value.as_object(),
                ) {
                    let mut set = serde_json::Map::new();
                    for (field, field_value) in object {
                        if previous.get(field) != Some(field_value) {
                            set.insert(field.clone(), field_value.clone());
                        }
                    }
                    let unset: Vec<_> = previous
                        .keys()
                        .filter(|field| !object.contains_key(*field))
                        .collect();
                    let delta =
                        serde_json::to_string(&serde_json::json!({"set": set, "unset": unset}))?
                            .len();
                    let tally = deltas.entry(class(key)).or_default();
                    tally.add(size, delta);
                }
                previous_puts.insert(key.clone(), value.clone());
            }
        }
        if let Some(deletes) = body["deletes"].as_array() {
            for key in deletes {
                let key = key.as_str().unwrap_or("");
                parts
                    .entry(format!("delete {}", class(key)))
                    .or_default()
                    .add(key.len() + 3, 0);
            }
        }
        let items: Vec<&serde_json::Value> = match body["items"].as_array() {
            Some(items) => items.iter().collect(),
            None if command == "single" => vec![body],
            None => vec![],
        };
        // Entries whose only write is the clock changed nothing else.
        let unchanged = body["puts"]
            .as_object()
            .is_some_and(|puts| puts.keys().all(|key| key == "clock"))
            && body["deletes"].as_array().is_some_and(Vec::is_empty);
        let receipted = items
            .iter()
            .filter(|item| item["internal"] != serde_json::Value::Bool(true))
            .count();
        let tally = commits
            .entry(if unchanged { "items of entries writing nothing" } else { "items of entries writing" }.into())
            .or_default();
        tally.add(receipted, 0);
        for item in items {
            let result = serde_json::to_string(&item["result"])?.len();
            let identity = item["request_id"].as_str().map_or(0, str::len)
                + item["fingerprint"].as_str().map_or(0, str::len)
                + 40;
            parts
                .entry("item identity".into())
                .or_default()
                .add(identity, 0);
            parts
                .entry("item result".into())
                .or_default()
                .add(0, result);
        }
        largest.push((bytes.len(), index));
    }
    println!("raft log indices {first:?}..={last}");
    print("raft_logs_v1 by kind", &kinds);
    print("raft_logs_v1 by payload part", &parts);
    print("raft_logs_v1 receipted items (rows: entries, key column: items)", &commits);
    print(
        "rewrites: full value bytes (key column) vs top-level delta bytes (value column)",
        &deltas,
    );
    largest.sort();
    largest.reverse();
    println!(
        "largest log entries: {:?}",
        &largest[..largest.len().min(10)]
    );
    if sample.as_deref() == Some("log") {
        let (_, index) = largest[largest.len() / 2];
        let entry = logs.get(index)?.context("entry")?;
        let json = unpack(entry.value());
        let text = String::from_utf8_lossy(&json);
        println!("median entry {index}: {}", &text[..text.len().min(4000)]);
    }
    Ok(())
}
