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

#[derive(Parser)]
#[command(
    version,
    about = "A Raft-backed database of reactive TypeScript values",
    after_help = "Offline provisioning: flower key seal --wrapping-key-file PATH [--format raw|pem|der] < private-key"
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

fn main() -> anyhow::Result<()> {
    let arguments: Vec<_> = std::env::args_os().collect();
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
    let consensus =
        Consensus::open(id, address, args.data.into(), args.admin_token.clone()).await?;
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
            let consensus =
                Consensus::open(replica.id, address, storage, args.admin_token.clone()).await?;
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
