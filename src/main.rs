use std::{
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
};
use zeroize::Zeroizing;

use clap::Parser;
use flower::{
    consensus::{Consensus, SharedDatabase, Storage},
    service,
};

mod server;

// Evaluations, HTTP handling and commits allocate many small, short-lived
// values across worker threads; mimalloc serves them from per-thread pages.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The mimalloc settings Flower starts with, as the `MIMALLOC_*` environment
/// variables mimalloc reads once when the process starts; a variable already
/// in the environment keeps its value. bench/LIMITS.md, "Allocator", has the
/// measurements.
///
/// `MIMALLOC_ALLOW_THP=0`: no transparent huge pages. mimalloc otherwise
/// advises its arenas MADV_HUGEPAGE, and under the kernel's default THP
/// defrag setting (`madvise`) a fault on an advised region that finds no
/// free 2 MiB page compacts memory first, in the faulting thread: whenever
/// mimalloc touched 2 MiB it had purged, that thread could stall there (on
/// the live Ultimator Flower at 02:35 UTC on 2026-09-29, 10.7% of its CPU
/// samples, compaction that mostly failed). With 0, mimalloc does not advise
/// and disables huge pages for the process (PR_SET_THP_DISABLE), so the heap
/// faults in 4 KiB pages on any host.
///
/// `MIMALLOC_PURGE_DELAY=10000`: memory freed stays with the process for 10 s
/// instead of 1 s before it goes back to the kernel (MADV_DONTNEED), so pages
/// a busy server frees and takes again within seconds do not fault and get
/// zeroed each time.
#[cfg(target_os = "linux")]
const ALLOCATOR_DEFAULTS: [(&std::ffi::CStr, &std::ffi::CStr); 2] = [
    (c"MIMALLOC_ALLOW_THP", c"0"),
    (c"MIMALLOC_PURGE_DELAY", c"10000"),
];

/// Runs after Rust's argument setup (priority 99) and before mimalloc's own
/// initialization (a constructor of default priority, 101 when built with
/// clang), so before anything allocates. setenv copies with libc's malloc.
#[cfg(target_os = "linux")]
#[used]
#[unsafe(link_section = ".init_array.00100")]
static ALLOCATOR_SETTINGS: extern "C" fn() = allocator_settings;

#[cfg(target_os = "linux")]
extern "C" fn allocator_settings() {
    for (name, value) in ALLOCATOR_DEFAULTS {
        // SAFETY: the process has one thread before main, and both strings
        // are NUL-terminated statics; overwrite 0 keeps the operator's value.
        unsafe { libc::setenv(name.as_ptr(), value.as_ptr(), 0) };
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "A Raft-backed database of reactive TypeScript values",
    after_help = "Offline provisioning: flower key seal --wrapping-key-file PATH [--format raw|pem|der] < private-key\nBackups (FLOWER_BACKUP_URL and FLOWER_BACKUP_*): flower backup list | flower backup restore --help"
)]
struct Args {
    /// Unique positive ID of this Raft node.
    #[arg(long, required_unless_present = "replica", conflicts_with = "replica")]
    id: Option<u64>,
    /// HTTP/1.1 and HTTP/2 listen address; optional TLS through FLOWER_TLS_* files.
    #[arg(long, conflicts_with = "replica")]
    listen: Option<SocketAddr>,
    /// Address reachable by other nodes, without an http:// prefix.
    #[arg(long, conflicts_with = "replica")]
    advertise: Option<String>,
    /// Host replicas of several Raft groups in this process instead, as
    /// NAME,ID,LISTEN[,ADVERTISE]. They share one database in --data, so
    /// their log appends share fsyncs.
    #[arg(long, value_name = "NAME,ID,LISTEN[,ADVERTISE]", value_parser = parse_replica)]
    replica: Vec<Replica>,
    /// Exclusive data directory for this node, or for every hosted replica.
    #[arg(long)]
    data: PathBuf,
    /// Operator secret; use FLOWER_PEER_TOKEN for a separate peer credential.
    #[arg(long, env = "FLOWER_ADMIN_TOKEN", hide_env_values = true)]
    admin_token: String,
}

#[derive(Clone)]
struct Replica {
    name: String,
    id: u64,
    listen: SocketAddr,
    advertise: Option<String>,
}

fn parse_replica(value: &str) -> Result<Replica, String> {
    let parts: Vec<&str> = value.split(',').collect();
    let (name, id, listen, advertise) = match parts[..] {
        [name, id, listen] => (name, id, listen, None),
        [name, id, listen, advertise] => (name, id, listen, Some(advertise.to_owned())),
        _ => return Err("expected NAME,ID,LISTEN[,ADVERTISE]".into()),
    };
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err("replica names use 1-64 ASCII letters, digits, '-' or '_'".into());
    }
    let id = id
        .parse::<u64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or("replica IDs are positive integers")?;
    let listen = listen
        .parse()
        .map_err(|error| format!("listen address: {error}"))?;
    Ok(Replica {
        name: name.to_owned(),
        id,
        listen,
        advertise,
    })
}

#[derive(Parser)]
#[command(about = "Seal a private-key import before it reaches Flower HTTP")]
struct SealArgs {
    /// Mounted wrapping key, supplied separately to authorized database nodes.
    #[arg(long)]
    wrapping_key_file: PathBuf,
    /// Encoding of bytes read from stdin.
    #[arg(long, default_value = "raw", value_parser = ["raw", "pem", "der"])]
    format: String,
}

#[derive(Parser)]
#[command(
    name = "flower backup",
    about = "List or restore Flower backups; FLOWER_BACKUP_URL and FLOWER_BACKUP_S3_* say where they are"
)]
struct BackupCli {
    #[command(subcommand)]
    command: BackupCommand,
}

#[derive(clap::Subcommand)]
enum BackupCommand {
    /// The generations of backups, and the points each can restore.
    List {
        /// The backups, instead of FLOWER_BACKUP_URL: s3://BUCKET/PREFIX or file:///PATH.
        #[arg(long)]
        from: Option<String>,
        /// A hosted replica's backups (under NAME/).
        #[arg(long)]
        replica: Option<String>,
    },
    /// Restore into an empty data directory as a new single-node cluster.
    /// Start it with the same --id (and --replica) and it elects itself;
    /// add other nodes with /raft/membership.
    Restore {
        /// The backups, instead of FLOWER_BACKUP_URL: s3://BUCKET/PREFIX or file:///PATH.
        #[arg(long)]
        from: Option<String>,
        /// The data directory to create (with --replica, the hosted replicas' directory).
        #[arg(long)]
        data: PathBuf,
        /// The restored node's ID.
        #[arg(long)]
        id: u64,
        /// The address other nodes reach the restored node at, host:port.
        #[arg(long)]
        advertise: String,
        /// Restore a hosted replica's backups (under NAME/) into DATA/flower.redb.
        #[arg(long)]
        replica: Option<String>,
        /// Restore the state as of this time: RFC 3339 (2026-09-30T19:23:00Z) or Unix ms.
        #[arg(long, conflicts_with = "index")]
        at: Option<String>,
        /// Restore through this log index.
        #[arg(long)]
        index: Option<u64>,
        /// Restore from this generation instead of the one the point picks.
        #[arg(long)]
        generation: Option<String>,
    },
}

async fn backup_command(command: BackupCommand) -> anyhow::Result<serde_json::Value> {
    use flower::consensus::{BackupConfig, RestoreOptions, RestorePoint};
    let config = |from: Option<String>| -> anyhow::Result<BackupConfig> {
        BackupConfig::from_env_with_url(from.as_deref())?.ok_or_else(|| {
            anyhow::anyhow!("say where the backups are: --from URL or FLOWER_BACKUP_URL")
        })
    };
    match command {
        BackupCommand::List { from, replica } => {
            flower::consensus::list_backups(&config(from)?, replica.as_deref()).await
        }
        BackupCommand::Restore {
            from,
            data,
            id,
            advertise,
            replica,
            at,
            index,
            generation,
        } => {
            let point = match (at, index) {
                (Some(at), _) => RestorePoint::Time(flower::consensus::parse_time(&at)?),
                (None, Some(index)) => RestorePoint::Index(index),
                (None, None) => RestorePoint::Latest,
            };
            flower::consensus::restore_backup(RestoreOptions {
                config: config(from)?,
                data,
                id,
                advertise,
                replica,
                point,
                generation,
                progress: true,
            })
            .await
        }
    }
}

fn main() -> anyhow::Result<()> {
    let arguments: Vec<_> = std::env::args_os().collect();
    if arguments.get(1).is_some_and(|value| value == "backup") {
        let cli = BackupCli::parse_from(
            std::iter::once(arguments[0].clone()).chain(arguments.into_iter().skip(2)),
        );
        let result = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(backup_command(cli.command))?;
        let mut output = std::io::stdout().lock();
        serde_json::to_writer_pretty(&mut output, &result)?;
        writeln!(output)?;
        return Ok(());
    }
    if arguments.get(1).is_some_and(|value| value == "key") {
        anyhow::ensure!(
            arguments.get(2).is_some_and(|value| value == "seal"),
            "native key command is: flower key seal --wrapping-key-file PATH [--format raw|pem|der] < private-key"
        );
        let arguments = SealArgs::parse_from(
            std::iter::once(arguments[0].clone()).chain(arguments.into_iter().skip(3)),
        );
        let mut bytes = Zeroizing::new(Vec::new());
        std::io::stdin().read_to_end(&mut bytes)?;
        let sealed =
            service::seal_key_import(&arguments.wrapping_key_file, &bytes, &arguments.format)?;
        let mut output = std::io::stdout().lock();
        serde_json::to_writer(&mut output, &sealed)?;
        writeln!(output)?;
        return Ok(());
    }

    // The evaluator's shared stack allowance reserves headroom within this
    // explicit worker size, including nested isolated QuickJS runtimes.
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(2 * 1024 * 1024)
        // Poll sockets/timers regularly during bursts of ready HTTP and Raft
        // work. Evaluation and durable storage use the blocking worker pool.
        .event_interval(7)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let (node_id, listen) = match (args.id, args.replica.first()) {
        (Some(id), _) => (id, args.listen.unwrap_or(DEFAULT_LISTEN).to_string()),
        (None, Some(replica)) => (replica.id, replica.listen.to_string()),
        (None, None) => anyhow::bail!("--id or --replica is required"),
    };
    anyhow::ensure!(node_id > 0, "node ID must be positive");
    let telemetry =
        tokio::task::spawn_blocking(move || flower::telemetry::init(node_id, &listen)).await??;
    let result = run_server(args).await;
    let exported = telemetry.shutdown().await;
    result?;
    exported?;
    Ok(())
}

const DEFAULT_LISTEN: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::LOCALHOST,
    7101,
));

async fn run_server(args: Args) -> anyhow::Result<()> {
    let server_config = server::Config::from_env()?;
    flower::transport::validate_configuration()?;
    flower::evaluator::warmup()?;
    flower::service::validate_configuration()?;
    if !args.replica.is_empty() {
        return host_replicas(args, server_config).await;
    }
    let id = args.id.expect("--id is required without --replica");
    let listen = args.listen.unwrap_or(DEFAULT_LISTEN);
    let address = args.advertise.unwrap_or_else(|| listen.to_string());
    let backup = flower::consensus::BackupConfig::from_env()?;
    if let Some(backup) = &backup {
        tracing::info!(target: "flower::backup", target_url = %backup.describe(), "backing up");
    }
    let consensus = Consensus::open_backed_up(
        id,
        address,
        args.data.into(),
        args.admin_token.clone(),
        backup,
    )
    .await?;
    let app = service::router(consensus.clone(), args.admin_token);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(id, listen = %listen,
        http2_max_streams = server_config.http2_max_streams, "Flower is listening");
    let draining = consensus.clone();
    let result = server::serve(listener, app, server_config, async move {
        shutdown_signal().await;
        draining.drain();
    })
    .await;
    consensus.shutdown().await?;
    result?;
    Ok(())
}

/// Serve replicas of several Raft groups from one process and one database.
/// Each keeps its own listener, node ID and membership; only storage, and so
/// the disk's flushes, are shared.
async fn host_replicas(args: Args, server_config: server::Config) -> anyhow::Result<()> {
    let mut names = std::collections::BTreeSet::new();
    for replica in &args.replica {
        anyhow::ensure!(
            names.insert(&replica.name),
            "duplicate replica name {}",
            replica.name
        );
    }
    let data = args.data.clone();
    let database = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        std::fs::create_dir_all(&data)?;
        SharedDatabase::open(&data.join("flower.redb"))
    })
    .await??;
    let backup = flower::consensus::BackupConfig::from_env()?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let mut servers = Vec::new();
    let mut hosted = Vec::new();
    let opened: anyhow::Result<()> = async {
        for replica in &args.replica {
            let address = replica
                .advertise
                .clone()
                .unwrap_or_else(|| replica.listen.to_string());
            let storage = Storage::Shared {
                database: database.clone(),
                prefix: format!("{}/", replica.name),
                directory: args.data.join(&replica.name),
            };
            let backup = backup
                .as_ref()
                .map(|backup| backup.for_replica(&replica.name));
            if let Some(backup) = &backup {
                tracing::info!(target: "flower::backup", replica = %replica.name,
                    target_url = %backup.describe(), "backing up");
            }
            let consensus = Consensus::open_backed_up(
                replica.id,
                address,
                storage,
                args.admin_token.clone(),
                backup,
            )
            .await?;
            hosted.push(consensus.clone());
            let app = service::router(consensus, args.admin_token.clone());
            let listener = tokio::net::TcpListener::bind(replica.listen).await?;
            tracing::info!(replica = %replica.name, id = replica.id, listen = %replica.listen,
                http2_max_streams = server_config.http2_max_streams, "Flower is listening");
            let mut stopped = stopped.clone();
            servers.push(tokio::spawn(server::serve(
                listener,
                app,
                server_config,
                async move {
                    let _ = stopped.wait_for(|stop| *stop).await;
                },
            )));
        }
        Ok(())
    }
    .await;
    if opened.is_ok() {
        // Any server's failure stops them all, like a signal.
        tokio::select! {
            _ = shutdown_signal() => {}
            _ = futures_util::future::select_all(servers.iter_mut()) => {}
        }
    }
    for consensus in &hosted {
        consensus.drain();
    }
    let _ = stop.send(true);
    let mut result = opened;
    for server in servers {
        let served = server
            .await
            .map_err(anyhow::Error::new)
            .and_then(|served| served.map_err(anyhow::Error::new));
        result = result.and(served);
    }
    for consensus in hosted {
        result = result.and(consensus.shutdown().await);
    }
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
                return;
            }
            Err(error) => {
                tracing::error!(%error, "failed to register SIGTERM handler; Ctrl+C remains available")
            }
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    /// This test binary links the constructor and the allocator as the server
    /// does, so mimalloc started with the settings it gave.
    #[test]
    fn the_allocator_starts_without_transparent_huge_pages() {
        for (name, value) in super::ALLOCATOR_DEFAULTS {
            let name = name.to_str().unwrap();
            assert!(std::env::var_os(name).is_some(), "{name} is set");
            if std::env::var(name).as_deref() != Ok(value.to_str().unwrap()) {
                eprintln!("{name} comes from the environment; skipping");
                return;
            }
        }
        // Touched memory in mimalloc's first arena, reserved before main.
        let blocks: Vec<Vec<u8>> = (0..16).map(|_| vec![1u8; 1 << 20]).collect();
        // SAFETY: PR_GET_THP_DISABLE takes no pointers.
        assert_eq!(
            unsafe { libc::prctl(libc::PR_GET_THP_DISABLE, 0, 0, 0, 0) },
            1
        );
        let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
        let advised: Vec<&str> = smaps
            .lines()
            .filter(|line| {
                line.starts_with("VmFlags:") && line.split_whitespace().any(|flag| flag == "hg")
            })
            .collect();
        assert!(advised.is_empty(), "MADV_HUGEPAGE mappings: {advised:?}");
        let huge: u64 = smaps
            .lines()
            .filter_map(|line| line.strip_prefix("AnonHugePages:"))
            .map(|kib| kib.trim().trim_end_matches(" kB").parse::<u64>().unwrap())
            .sum();
        assert_eq!(huge, 0, "{huge} KiB of anonymous huge pages");
        drop(blocks);
    }
}
