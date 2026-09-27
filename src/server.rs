//! HTTP/1.1 and HTTP/2, with optional native TLS and ALPN.
//!
//! Keep Axum's connection behavior without advertising a fixed HTTP/2 stream
//! ceiling. Operators may set `FLOWER_HTTP2_MAX_STREAMS` to a positive u32 to
//! apply a per-connection admission limit in any build.

use std::{ffi::OsString, future::Future, io, time::Duration};

use axum::{
    Router,
    serve::{Listener, ListenerExt},
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::{conn::auto::Builder, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use tokio::net::TcpListener;

const STREAMS_OPTION: &str = "FLOWER_HTTP2_MAX_STREAMS";
const H1_HEADERS_OPTION: &str = "FLOWER_HTTP1_MAX_HEADERS";
const H1_BUFFER_OPTION: &str = "FLOWER_HTTP1_MAX_BUFFER_BYTES";
const H2_HEADERS_OPTION: &str = "FLOWER_HTTP2_MAX_HEADER_LIST_BYTES";
const SHUTDOWN_OPTION: &str = "FLOWER_SHUTDOWN_TIMEOUT_MS";
// Hyper reserves the entire parsed count at once. http 1.5's 32768-slot
// HeaderMap allows a 3/4 usable reservation before its index representation ends.
const H1_HEADER_CAPACITY: usize = (1 << 15) / 4 * 3;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Config {
    pub http2_max_streams: Option<u32>,
    pub http1_max_headers: Option<usize>,
    pub http1_max_buffer_bytes: usize,
    pub http2_max_header_list_bytes: u32,
    pub shutdown_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http2_max_streams: None,
            // Preserve Hyper's default 100-header stack allocation fast path.
            http1_max_headers: None,
            http1_max_buffer_bytes: 8192 + 4096 * 100,
            http2_max_header_list_bytes: 16 * 1024,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl Config {
    /// Validate once during startup, before opening the node's data directory.
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        Self::parse_options(|name| std::env::var_os(name))
    }

    fn parse_options(read: impl Fn(&str) -> Option<OsString>) -> anyhow::Result<Self> {
        let mut config = Self::default();
        if let Some(value) = read(SHUTDOWN_OPTION) {
            let millis = value
                .to_str()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|millis| *millis > 0)
                .ok_or_else(|| {
                    anyhow::anyhow!("{SHUTDOWN_OPTION} must be a positive integer of milliseconds")
                })?;
            config.shutdown_timeout = Duration::from_millis(millis);
            anyhow::ensure!(
                std::time::Instant::now()
                    .checked_add(config.shutdown_timeout)
                    .is_some(),
                "{SHUTDOWN_OPTION} exceeds this platform's timer range"
            );
        }
        if let Some(value) = read(STREAMS_OPTION) {
            config.http2_max_streams = Some(parse_u32(STREAMS_OPTION, &value)?);
        }
        if let Some(value) = read(H2_HEADERS_OPTION) {
            config.http2_max_header_list_bytes = parse_u32(H2_HEADERS_OPTION, &value)?;
        }
        if let Some(value) = read(H1_HEADERS_OPTION) {
            let count = parse_usize(H1_HEADERS_OPTION, &value)?;
            // Hyper calls HeaderMap::reserve(count); its usable reservation
            // capacity is smaller than its 32768-slot raw representation.
            anyhow::ensure!(
                count <= H1_HEADER_CAPACITY,
                "{H1_HEADERS_OPTION} exceeds Hyper/HeaderMap's {H1_HEADER_CAPACITY}-header reservation representation"
            );
            config.http1_max_headers = Some(count);
        }
        if let Some(value) = read(H1_BUFFER_OPTION) {
            config.http1_max_buffer_bytes = parse_usize(H1_BUFFER_OPTION, &value)?;
        }
        anyhow::ensure!(
            (8192..=isize::MAX as usize).contains(&config.http1_max_buffer_bytes),
            "{H1_BUFFER_OPTION} must be at least 8192 bytes (Hyper minimum) and fit a platform byte buffer"
        );
        Ok(config)
    }

    #[cfg(test)]
    fn parse(value: Option<&std::ffi::OsStr>) -> anyhow::Result<Self> {
        Self::parse_options(|name| {
            (name == STREAMS_OPTION)
                .then(|| value.map(OsString::from))
                .flatten()
        })
    }
}

fn parse_usize(name: &str, value: &std::ffi::OsStr) -> anyhow::Result<usize> {
    value
        .to_str()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow::anyhow!("{name} must be a positive platform-sized integer"))
}

fn parse_u32(name: &str, value: &std::ffi::OsStr) -> anyhow::Result<u32> {
    value
        .to_str()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow::anyhow!("{name} must be a positive 32-bit integer"))
}

pub(crate) async fn serve(
    listener: TcpListener,
    app: Router,
    config: Config,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let tls = flower::transport::tls().map_err(io::Error::other)?.cloned();
    serve_with_tls(listener, app, config, tls, shutdown).await
}

async fn serve_with_tls(
    listener: TcpListener,
    app: Router,
    config: Config,
    tls: Option<flower::transport::Tls>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    // Listener::accept preserves Axum's retry/backoff behavior for accept errors.
    let mut listener = listener.tap_io(|stream| {
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(%error, "could not disable TCP coalescing");
        }
    });
    // Match Router's make-service conversion: finish route state binding before
    // serving requests rather than making each request build those routes.
    let app: Router = app.with_state(());
    let mut builder = Builder::new(TokioExecutor::new());
    if let Some(count) = config.http1_max_headers {
        builder.http1().max_headers(count);
    }
    builder.http1().max_buf_size(config.http1_max_buffer_bytes);
    // Hyper defaults to Some(200). Explicit None removes that advertised
    // setting and leaves h2's receive-stream count unrestricted by configuration.
    builder
        .http2()
        .max_concurrent_streams(config.http2_max_streams)
        .max_header_list_size(config.http2_max_header_list_bytes)
        .enable_connect_protocol();
    let graceful = GracefulShutdown::new();
    let mut connections = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            (stream, address) = listener.accept() => {
                let watcher = graceful.watcher();
                if let Some(tls) = tls.clone() {
                    let builder = builder.clone();
                    let app = app.clone();
                    connections.spawn(async move {
                        let acceptor = tokio_rustls::TlsAcceptor::from(tls.server);
                        let stream = match tokio::time::timeout(tls.handshake_timeout, acceptor.accept(stream)).await {
                            Ok(Ok(stream)) => stream,
                            Ok(Err(error)) => { tracing::debug!(%error,%address,"TLS handshake rejected"); return; },
                            Err(_) => { tracing::debug!(%address,"TLS handshake deadline exceeded"); return; },
                        };
                        let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream),TowerToHyperService::new(app)).into_owned();
                        if let Err(error) = watcher.watch(connection).await {
                            tracing::trace!(%error,%address,"failed to serve TLS connection");
                        }
                    });
                } else {
                    let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream),TowerToHyperService::new(app.clone())).into_owned();
                    let connection = watcher.watch(connection);
                    connections.spawn(async move {
                        if let Err(error) = connection.await {
                            tracing::trace!(%error, %address, "failed to serve connection");
                        }
                    });
                }
            }
        }
    }

    // Stop accepting first and give active requests a bounded chance to finish.
    // Long-lived SSE streams must not hold an operational restart open forever.
    // Dropping a connection never undoes a mutation that reached Raft; callers
    // retain their request ID when retrying an interrupted response.
    drop(listener);
    if tokio::time::timeout(config.shutdown_timeout, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::info!(
            timeout_ms = config.shutdown_timeout.as_millis(),
            "HTTP drain deadline reached; closing remaining connections"
        );
        connections.abort_all();
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests;
