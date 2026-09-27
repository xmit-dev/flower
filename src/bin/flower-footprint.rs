//! Memory and load-cost census of application state, for sizing disk-backed
//! serving. Seeds bench/footprint.ts through the production evaluator, then
//! measures the same records in each candidate representation and in a redb
//! table with the production layout. See bench/FOOTPRINT.md.
use anyhow::{Context, Result, ensure};
use clap::Parser;
use flower::consensus::{Receipt, Receipts, Records};
use flower::evaluator::{evaluate, hash, invoke};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Live requested heap bytes and allocations, over the production allocator.
struct Counting;
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            LIVE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            LIVE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        LIVE_ALLOCATIONS.fetch_sub(1, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, size) };
        if !moved.is_null() {
            LIVE_BYTES.fetch_add(size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("application_data_v2");

#[derive(Parser)]
struct Args {
    /// JavaScript bundle built from bench/footprint.ts.
    #[arg(long)]
    bundle: PathBuf,
    /// Orders to seed; each also has one customer and `--lines` order lines.
    #[arg(long)]
    orders: usize,
    #[arg(long, default_value_t = 3)]
    lines: usize,
    /// Orders per seeding mutation.
    #[arg(long, default_value_t = 500)]
    batch: usize,
    /// Directory for the temporary redb file.
    #[arg(long)]
    dir: PathBuf,
    /// Random point reads per read measurement.
    #[arg(long, default_value_t = 200_000)]
    reads: usize,
    /// Cache for the small-cache redb read measurement.
    #[arg(long, default_value_t = 16 << 20)]
    small_cache: usize,
    /// Print one key and value of each class, then stop after the census.
    #[arg(long)]
    sample: bool,
    /// Attribute derived metadata to key classes instead of timing storage.
    #[arg(long)]
    breakdown: bool,
    /// After seeding, sample this process with macOS `sample` for this many
    /// seconds per mutation kind, writing .footprint/sample-<kind>.txt.
    #[arg(long)]
    profile: Option<u64>,
}

#[derive(Clone, Copy)]
struct Heap {
    bytes: usize,
    allocations: usize,
}

fn heap() -> Heap {
    Heap {
        bytes: LIVE_BYTES.load(Ordering::Relaxed),
        allocations: LIVE_ALLOCATIONS.load(Ordering::Relaxed),
    }
}

/// Resident and (on macOS) physical-footprint bytes of this process.
fn process_memory() -> Value {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut info: libc::rusage_info_v4 = std::mem::zeroed();
        let status = libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            (&mut info as *mut libc::rusage_info_v4).cast(),
        );
        if status == 0 {
            return json!({
                "residentBytes": info.ri_resident_size,
                "physFootprintBytes": info.ri_phys_footprint,
                "lifetimeMaxPhysFootprintBytes": info.ri_lifetime_max_phys_footprint,
            });
        }
    }
    #[cfg(target_os = "linux")]
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        let pages: Vec<u64> = statm
            .split_whitespace()
            .filter_map(|n| n.parse().ok())
            .collect();
        if let Some(resident) = pages.get(1) {
            return json!({ "residentBytes": resident * 4096 });
        }
    }
    Value::Null
}

/// Heap retained by what `build` returns, and how long building it took.
fn retained<T>(build: impl FnOnce() -> T) -> (T, Value) {
    let before = heap();
    let started = Instant::now();
    let value = build();
    let elapsed = started.elapsed();
    let after = heap();
    let stats = json!({
        "bytes": after.bytes as i64 - before.bytes as i64,
        "allocations": after.allocations as i64 - before.allocations as i64,
        "ms": millis(elapsed),
    });
    (value, stats)
}

fn millis(elapsed: Duration) -> f64 {
    (elapsed.as_secs_f64() * 1e6).round() / 1e3
}

fn per_op(elapsed: Duration, operations: usize) -> f64 {
    (elapsed.as_secs_f64() * 1e10 / operations as f64).round() / 10.0
}

/// A key's storage class: its prefix, plus the first JSON string component
/// (a collection or derivation name) when one follows.
fn class(key: &str) -> String {
    let Some((prefix, rest)) = key.split_once(':') else {
        return key.to_owned();
    };
    let mut label = prefix.to_owned();
    let rest = rest.strip_prefix('[').unwrap_or(rest);
    if let Some(quoted) = rest.strip_prefix('"')
        && let Some(end) = quoted.find('"')
    {
        label.push(':');
        label.push_str(&quoted[..end]);
    } else if let Some((name, _)) = rest.split_once(':')
        && name.len() <= 32
    {
        label.push(':');
        label.push_str(name);
    }
    label
}

fn seed(args: &Args) -> Result<(Records, Value)> {
    let javascript = std::fs::read_to_string(&args.bundle).context("read bundle")?;
    let bundle = json!({"hash": hash(javascript.as_bytes()), "javascript": javascript});
    let deployed = evaluate(
        BTreeMap::new(),
        json!({"requestId": "deploy", "bundle": bundle}),
    )?;
    let mut records: Records = deployed.puts.into();
    let started = Instant::now();
    let mut calls = 0;
    // Mean batch time per tenth of the seeding, to expose state-size costs.
    let tenth = args.orders.div_ceil(args.batch).div_ceil(10).max(1);
    let mut tenths = Vec::new();
    let mut tenth_started = Instant::now();
    for start in (0..args.orders).step_by(args.batch) {
        let count = args.batch.min(args.orders - start);
        let evaluation = invoke(
            records.clone(),
            json!({"name": "internal.footprint.seed", "requestId": format!("seed-{start}"),
                "args": {"start": start, "count": count, "lines": args.lines}}),
            "mutation",
        )
        .with_context(|| format!("seed batch at {start}"))?;
        for (key, value) in evaluation.puts {
            records.insert(key, value);
        }
        for key in evaluation.deletes {
            records.remove(&key);
        }
        calls += 1;
        if calls % tenth == 0 {
            tenths.push(millis(tenth_started.elapsed()) / tenth as f64);
            tenth_started = Instant::now();
        }
    }
    let elapsed = started.elapsed();
    Ok((
        records,
        json!({"calls": calls, "ms": millis(elapsed), "batchMsByTenth": tenths}),
    ))
}

fn create(records: &Records, n: usize, lines: usize) -> Result<()> {
    invoke(
        records.clone(),
        json!({"name": "internal.footprint.seed", "requestId": format!("create-{n}"),
            "args": {"start": n, "count": 1, "lines": lines}}),
        "mutation",
    )?;
    Ok(())
}

fn touch(records: &Records, n: usize, orders: usize) -> Result<()> {
    invoke(
        records.clone(),
        json!({"name": "internal.footprint.touch", "requestId": format!("touch-{n}"),
            "args": {"order": n % orders, "quantity": 1 + n % 7}}),
        "mutation",
    )?;
    Ok(())
}

fn remove(records: &Records, n: usize, args: &Args) -> Result<()> {
    invoke(
        records.clone(),
        json!({"name": "internal.footprint.remove", "requestId": format!("remove-{n}"),
            "args": {"order": n % args.orders, "lines": args.lines}}),
        "mutation",
    )?;
    Ok(())
}

/// Mean latency of independent single-row mutations against one state.
fn mutation_latency(records: &Records, args: &Args) -> Result<Value> {
    create(records, 1 << 40, args.lines)?;
    let calls = 20;
    let started = Instant::now();
    for n in 0..calls {
        create(records, args.orders + n, args.lines)?;
    }
    let create_ms = millis(started.elapsed()) / calls as f64;
    let started = Instant::now();
    for n in 0..calls {
        touch(records, n * 7919, args.orders)?;
    }
    let touch_ms = millis(started.elapsed()) / calls as f64;
    let started = Instant::now();
    for n in 0..calls {
        remove(records, n * 7919, args)?;
    }
    let remove_ms = millis(started.elapsed()) / calls as f64;
    Ok(json!({"createOrderMs": create_ms, "updateLineMs": touch_ms, "deleteOrderMs": remove_ms}))
}

fn profile(records: &Records, args: &Args, seconds: u64) -> Result<()> {
    for kind in ["create", "touch"] {
        let output = args.dir.join(format!("sample-{kind}.txt"));
        let mut sampler = std::process::Command::new("sample")
            .args([
                &std::process::id().to_string(),
                &seconds.to_string(),
                "-file",
            ])
            .arg(&output)
            .stdout(std::process::Stdio::null())
            .spawn()
            .context("start sample")?;
        let deadline = Instant::now() + Duration::from_secs(seconds + 1);
        let mut n = 0;
        while Instant::now() < deadline {
            if kind == "create" {
                create(records, args.orders + n, args.lines)?;
            } else {
                touch(records, n * 7919, args.orders)?;
            }
            n += 1;
        }
        sampler.wait()?;
        eprintln!("{kind}: {n} calls in {seconds} s; {}", output.display());
    }
    Ok(())
}

fn census(records: &Records, sample: bool) -> Result<(Vec<(String, Vec<u8>)>, Value)> {
    #[derive(Default)]
    struct Class {
        keys: usize,
        key_bytes: usize,
        value_bytes: usize,
    }
    let mut classes: BTreeMap<String, Class> = BTreeMap::new();
    let mut encoded = Vec::with_capacity(records.len());
    for (key, value) in records {
        let bytes = serde_json::to_vec(value)?;
        let label = class(key);
        if sample && !classes.contains_key(&label) {
            let shown = String::from_utf8_lossy(&bytes[..bytes.len().min(300)]).into_owned();
            eprintln!("{label}\n  key   {key}\n  value {shown}");
        }
        let entry = classes.entry(label).or_default();
        entry.keys += 1;
        entry.key_bytes += key.len();
        entry.value_bytes += bytes.len();
        encoded.push((key.clone(), bytes));
    }
    let total_key_bytes: usize = classes.values().map(|class| class.key_bytes).sum();
    let total_value_bytes: usize = classes.values().map(|class| class.value_bytes).sum();
    let report = json!({
        "keys": records.len(),
        "keyBytes": total_key_bytes,
        "valueBytes": total_value_bytes,
        "classes": classes.iter().map(|(label, class)| (label.clone(), json!({
            "keys": class.keys,
            "keyBytes": class.key_bytes,
            "valueBytes": class.value_bytes,
        }))).collect::<serde_json::Map<_, _>>(),
    });
    Ok((encoded, report))
}

fn parse(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("stored JSON")
}

/// Heap retained by each in-memory representation of the same records.
fn representations(encoded: &[(String, Vec<u8>)]) -> Value {
    let (keys, keys_only) = retained(|| {
        encoded
            .iter()
            .map(|(key, _)| (key.clone(), ()))
            .collect::<im::OrdMap<String, ()>>()
    });
    drop(keys);
    let (compact, compact_stats) = retained(|| {
        encoded
            .iter()
            .map(|(key, bytes)| (key.clone(), Arc::<[u8]>::from(bytes.as_slice())))
            .collect::<im::OrdMap<String, Arc<[u8]>>>()
    });
    drop(compact);
    let (parsed, parsed_stats) = retained(|| {
        encoded
            .iter()
            .map(|(key, bytes)| (key.clone(), Arc::new(parse(bytes))))
            .collect::<im::OrdMap<String, Arc<Value>>>()
    });
    drop(parsed);
    let (records, records_stats) = retained(|| {
        encoded
            .iter()
            .map(|(key, bytes)| (key.clone(), parse(bytes)))
            .collect::<Records>()
    });
    drop(records);
    json!({
        "keysOnly": keys_only,
        "compactJson": compact_stats,
        "parsedJson": parsed_stats,
        "records": records_stats,
    })
}

/// Derived metadata (production Records minus a plain map of the same parsed
/// values) as key classes are added, plus the parsed-value cost of each class.
fn breakdown(encoded: &[(String, Vec<u8>)]) -> Value {
    let stages: [(&str, &[&str]); 5] = [
        ("sources", &["source:"]),
        ("indexEntries", &["index-entry:", "ordered-entry:"]),
        ("roots", &["root:"]),
        ("cells", &["cell:"]),
        ("everything", &[""]),
    ];
    let mut included: Vec<&str> = Vec::new();
    let mut report = serde_json::Map::new();
    for (stage, prefixes) in stages {
        included.extend(prefixes);
        let subset: Vec<&(String, Vec<u8>)> = encoded
            .iter()
            .filter(|(key, _)| included.iter().any(|prefix| key.starts_with(prefix)))
            .collect();
        let (parsed, parsed_stats) = retained(|| {
            subset
                .iter()
                .map(|(key, bytes)| (key.clone(), Arc::new(parse(bytes))))
                .collect::<im::OrdMap<String, Arc<Value>>>()
        });
        drop(parsed);
        let (records, records_stats) = retained(|| {
            subset
                .iter()
                .map(|(key, bytes)| (key.clone(), parse(bytes)))
                .collect::<Records>()
        });
        drop(records);
        let text: usize = subset
            .iter()
            .map(|(key, bytes)| key.len() + bytes.len())
            .sum();
        report.insert(
            stage.into(),
            json!({"keys": subset.len(), "textBytes": text, "parsed": parsed_stats, "records": records_stats}),
        );
    }
    // Persistent collections have large minimum nodes; measure a lone member.
    let count = 100_000;
    let (sets, hash_set) = retained(|| {
        (0..count)
            .map(|n| im::HashSet::unit(format!("source:[\"orderLines\",\"line-{n}-0\"]")))
            .collect::<Vec<_>>()
    });
    drop(sets);
    let (sets, ord_set) = retained(|| {
        (0..count)
            .map(|n| im::OrdSet::unit(format!("source:[\"orderLines\",\"line-{n}-0\"]")))
            .collect::<Vec<_>>()
    });
    drop(sets);
    let (sets, boxed) = retained(|| {
        (0..count)
            .map(|n| vec![format!("source:[\"orderLines\",\"line-{n}-0\"]")].into_boxed_slice())
            .collect::<Vec<_>>()
    });
    drop(sets);
    let per = |stats: &Value| stats["bytes"].as_i64().unwrap_or(0) as f64 / count as f64;
    report.insert(
        "singletonCollectionBytes".into(),
        json!({"imHashSet": per(&hash_set), "imOrdSet": per(&ord_set), "boxedSlice": per(&boxed)}),
    );
    Value::Object(report)
}

fn write_redb(path: &std::path::Path, encoded: &[(String, Vec<u8>)]) -> Result<Value> {
    let started = Instant::now();
    // Written around the OS page cache, so the first reads below miss it.
    let database = redb::Builder::new().create_file(uncached_file(path, true)?)?;
    for chunk in encoded.chunks(200_000) {
        let mut transaction = database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            let mut table = transaction.open_table(DATA)?;
            for (key, bytes) in chunk {
                table.insert(key.as_str(), bytes.as_slice())?;
            }
        }
        transaction.commit()?;
    }
    drop(database);
    Ok(json!({
        "ms": millis(started.elapsed()),
        "fileBytes": std::fs::metadata(path)?.len(),
    }))
}

/// The application-data part of startup (store.rs load_or_migrate_application),
/// split into scanning, parsing and building. Each pass opens the database
/// afresh: redb's cache starts cold, the OS page cache does not.
fn load_redb(path: &std::path::Path, args: &Args) -> Result<Value> {
    let scan = {
        let database = Database::create(path)?;
        let started = Instant::now();
        let transaction = database.begin_read()?;
        let mut bytes = 0usize;
        for entry in transaction.open_table(DATA)?.iter()? {
            let (key, value) = entry?;
            bytes += key.value().len() + value.value().len();
        }
        ensure!(bytes > 0, "empty table");
        millis(started.elapsed())
    };
    let parse_only = {
        let database = Database::create(path)?;
        let started = Instant::now();
        let transaction = database.begin_read()?;
        let mut objects = 0usize;
        for entry in transaction.open_table(DATA)?.iter()? {
            let (_, value) = entry?;
            objects += usize::from(parse(value.value()).is_object());
        }
        ensure!(objects > 0, "no objects");
        millis(started.elapsed())
    };
    let database = Database::create(path)?;
    let transaction = database.begin_read()?;
    let table = transaction.open_table(DATA)?;
    let (records, load) = retained(|| {
        table
            .iter()
            .expect("iterate")
            .map(|entry| {
                let (key, value) = entry.expect("entry");
                (key.value().to_owned(), parse(value.value()))
            })
            .collect::<Records>()
    });
    ensure!(!records.is_empty(), "loaded nothing");
    // The first mutation after startup validates the whole graph and caches
    // its reachability proof in the loaded state; the second reuses it.
    let mutate = |start: usize| -> Result<Value> {
        let before = heap();
        let started = Instant::now();
        let evaluation = invoke(
            records.clone(),
            json!({"name": "internal.footprint.seed", "requestId": format!("after-load-{start}"),
                "args": {"start": start, "count": 1, "lines": args.lines}}),
            "mutation",
        )?;
        let elapsed = started.elapsed();
        drop(evaluation);
        let after = heap();
        Ok(
            json!({"ms": millis(elapsed), "retainedBytes": after.bytes as i64 - before.bytes as i64}),
        )
    };
    let first = mutate(args.orders)?;
    let second = mutate(args.orders + 1)?;
    drop(records);
    Ok(
        json!({"scanMs": scan, "scanParseMs": parse_only, "load": load,
        "firstMutation": first, "secondMutation": second}),
    )
}

struct Random(u64);
impl Random {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

/// Uniform random point reads and short range reads against each store.
fn picks(encoded: &[(String, Vec<u8>)], args: &Args) -> Vec<usize> {
    let mut random = Random(0x9e37_79b9_7f4a_7c15);
    (0..args.reads)
        .map(|_| random.below(encoded.len()))
        .collect()
}

fn reads(path: &std::path::Path, encoded: &[(String, Vec<u8>)], args: &Args) -> Result<Value> {
    let picks = picks(encoded, args);
    let records: Records = encoded
        .iter()
        .map(|(key, bytes)| (key.clone(), parse(bytes)))
        .collect();

    // One untimed pass over the same picks warms every cache that can hold them.
    let time = |mut read: Box<dyn FnMut(&str) -> usize + '_>| {
        for &pick in &picks {
            read(&encoded[pick].0);
        }
        let started = Instant::now();
        let mut found = 0;
        for &pick in &picks {
            found += read(&encoded[pick].0);
        }
        assert_eq!(found, picks.len());
        per_op(started.elapsed(), picks.len())
    };

    let records_get = time(Box::new(|key| usize::from(records.get(key).is_some())));
    let parse_ns = time(Box::new(|key| {
        let index = encoded
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .unwrap();
        std::hint::black_box(parse(&encoded[index].1));
        1
    }));
    let binary_search = time(Box::new(|key| {
        usize::from(
            encoded
                .binary_search_by(|(k, _)| k.as_str().cmp(key))
                .is_ok(),
        )
    }));

    let database = Database::create(path)?;
    let transaction = database.begin_read()?;
    let table = transaction.open_table(DATA)?;
    let redb_get = time(Box::new(|key| {
        usize::from(
            table
                .get(key)
                .unwrap()
                .is_some_and(|value| !value.value().is_empty()),
        )
    }));
    let redb_get_parse = time(Box::new(|key| {
        let value = table.get(key).unwrap().unwrap();
        std::hint::black_box(parse(value.value()));
        1
    }));
    let records_range = time(Box::new(|key| {
        usize::from(
            records
                .range::<(std::ops::Bound<&str>, std::ops::Bound<&str>)>((
                    std::ops::Bound::Included(key),
                    std::ops::Bound::Unbounded,
                ))
                .take(10)
                .count()
                > 0,
        )
    }));
    let redb_range = time(Box::new(|key| {
        usize::from(
            table
                .range(key..)
                .unwrap()
                .take(10)
                .map(|entry| entry.unwrap().1.value().len())
                .sum::<usize>()
                > 0,
        )
    }));
    drop(table);
    drop(transaction);
    drop(database);

    let mut builder = redb::Builder::new();
    builder.set_cache_size(args.small_cache);
    let database = builder.create(path)?;
    let transaction = database.begin_read()?;
    let table = transaction.open_table(DATA)?;
    let small_cache_get = time(Box::new(|key| {
        usize::from(
            table
                .get(key)
                .unwrap()
                .is_some_and(|value| !value.value().is_empty()),
        )
    }));
    drop(table);
    drop(transaction);
    drop(database);

    Ok(json!({
        "pointReadNs": {
            "records": records_get,
            "redb": redb_get,
            "redbParse": redb_get_parse,
            "redbSmallCache": small_cache_get,
            "parseOnlyWithBinarySearch": parse_ns,
            "binarySearchOnly": binary_search,
        },
        "range10Ns": {"records": records_range, "redb": redb_range},
        "smallCacheBytes": args.small_cache,
    }))
}

/// Untimed warm-up is skipped: the point is to miss. Each read goes through a
/// small redb cache over a file descriptor with F_NOCACHE.
fn uncached_reads(
    path: &std::path::Path,
    picks: &[usize],
    encoded: &[(String, Vec<u8>)],
    args: &Args,
) -> Result<Value> {
    {
        let file = uncached_file(path, false)?;
        let mut builder = redb::Builder::new();
        builder.set_cache_size(args.small_cache);
        let database = builder.create_file(file)?;
        let transaction = database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let count = picks.len().min(20_000);
        let started = Instant::now();
        for &pick in &picks[..count] {
            ensure!(
                table.get(encoded[pick].0.as_str())?.is_some(),
                "missing key"
            );
        }
        Ok(json!(per_op(started.elapsed(), count)))
    }
}

fn uncached_file(path: &std::path::Path, create: bool) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(create)
        .open(path)?;
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == 0,
            "F_NOCACHE"
        );
    }
    Ok(file)
}

/// One receipt per order, as if each came from its own request.
fn receipts(orders: usize) -> Value {
    let (receipts, stats) = retained(|| {
        let mut receipts = Receipts::new();
        for n in 0..orders {
            receipts.insert(
                format!("{:08x}-{:04x}-4000-8000-{:012x}", n, n % 65_536, n),
                Receipt {
                    fingerprint: format!("{:064x}", n as u128 * 0x9e37_79b9_7f4a_7c15),
                    revision: n as u64,
                    result: json!({"order": {"customerId": format!("customer-{n}"), "shippingCents": 250},
                        "subtotal": 4200, "total": 4450}),
                    epoch: None,
                },
            );
        }
        receipts
    });
    let encoded: usize = receipts
        .iter()
        .map(|(key, receipt)| {
            key.len() + serde_json::to_vec(receipt).map_or(0, |bytes| bytes.len())
        })
        .sum();
    json!({"count": receipts.len(), "encodedBytes": encoded, "heap": stats})
}

fn main() -> Result<()> {
    let args = Args::parse();
    let path = args
        .dir
        .join(format!("footprint-{}.redb", std::process::id()));
    let (records, seeding) = seed(&args)?;
    if let Some(seconds) = args.profile {
        create(&records, 1 << 40, args.lines)?;
        return profile(&records, &args, seconds);
    }
    let latency = mutation_latency(&records, &args)?;
    let (encoded, census) = census(&records, args.sample)?;
    let seeded_memory = process_memory();
    drop(records);
    if args.sample {
        println!("{}", serde_json::to_string_pretty(&census)?);
        return Ok(());
    }
    if args.breakdown {
        let report = json!({"orders": args.orders, "mutationLatency": latency, "census": census, "breakdown": breakdown(&encoded)});
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }
    let representations = representations(&encoded);
    let redb_write = write_redb(&path, &encoded)?;
    // First, while no page is in the OS cache: SSD point reads, then a cold scan.
    let ssd_point_read_ns = uncached_reads(&path, &picks(&encoded, &args), &encoded, &args)?;
    let redb_load = load_redb(&path, &args)?;
    let reads = reads(&path, &encoded, &args)?;
    std::fs::remove_file(&path)?;
    let receipts = receipts(args.orders);
    let report = json!({
        "orders": args.orders,
        "linesPerOrder": args.lines,
        "seeding": seeding,
        "mutationLatency": latency,
        "census": census,
        "heap": representations,
        "redbWrite": redb_write,
        "redbLoad": redb_load,
        "ssdPointReadNs": ssd_point_read_ns,
        "reads": reads,
        "receipts": receipts,
        "processAfterSeeding": seeded_memory,
        "processAtEnd": process_memory(),
    });
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
