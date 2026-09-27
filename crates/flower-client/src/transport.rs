//! HTTP transport: the `Transport` trait (the twin of `FlowerFetch`) and `HttpTransport`, a reqwest
//! 0.13 + rustls implementation with the limits of `sdk/http2.ts` and `sdk/http2-stream.ts`.
//!
//! * `http://` URLs speak h2c with prior knowledge (Flower serves HTTP/1.1 and h2c on one port);
//!   [`Protocol::Http1`] switches plain HTTP to HTTP/1.1 (for HTTP/1.1-only servers).
//! * `https://` URLs negotiate h2 or HTTP/1.1 by ALPN, verifying with the platform verifier plus
//!   extra PEM roots (explicit files, `NODE_EXTRA_CA_CERTS`, `TRINITY_CA_FILE`).
//! * Watches ([`Lane::Watch`]) use their own connection(s), so unread watches can never eat the
//!   flow-control window unary calls need.
//! * Unary replies are buffered with a byte limit and one whole-request deadline; watch replies
//!   return at headers, and an event-stream body is never timed out (the subscriber's stall timer
//!   polices it).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream, StreamExt};
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, Method, StatusCode};
use tokio::time::Instant;

use crate::error::FlowerError;

/// Which connection pool a request uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Lane {
    /// Queries, mutations and admin calls: the reply is buffered before `send` resolves.
    Unary,
    /// Watches: `send` resolves at response headers and the body streams.
    Watch,
}

/// One HTTP request. Bodies are already-serialized JSON, reused verbatim across retries.
#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub lane: Lane,
    /// Unary: the whole-request deadline (headers and body). Watch: the deadline until response
    /// headers, and for a reply that is not a 2xx event stream, until its body ends too.
    /// `None` uses the transport's default ([`TransportOptions::request_timeout`]).
    pub timeout: Option<Duration>,
}

/// A reply. For [`Lane::Unary`] the body is always [`ResponseBody::Full`] (or empty).
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: ResponseBody,
}

impl std::fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

impl HttpResponse {
    /// `statusText` as undici reports it for HTTP/1.1 (h2 has none): the canonical reason.
    pub fn status_text(&self) -> &'static str {
        self.status.canonical_reason().unwrap_or("")
    }

    /// The media type of `content-type`, lowercased, without parameters.
    pub fn media_type(&self) -> Option<String> {
        media_type(&self.headers)
    }
}

pub(crate) fn media_type(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(CONTENT_TYPE)?.to_str().ok()?;
    Some(
        value
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase(),
    )
}

/// A reply body: buffered, or a stream of chunks. Dropping a streaming body cancels its request.
pub enum ResponseBody {
    Empty,
    Full(Bytes),
    Stream(BoxStream<'static, Result<Bytes, FlowerError>>),
}

impl ResponseBody {
    /// The next chunk, `None` at a clean end.
    pub async fn chunk(&mut self) -> Option<Result<Bytes, FlowerError>> {
        match self {
            ResponseBody::Empty => None,
            ResponseBody::Full(bytes) => {
                let bytes = std::mem::take(bytes);
                *self = ResponseBody::Empty;
                (!bytes.is_empty()).then_some(Ok(bytes))
            }
            ResponseBody::Stream(stream) => stream.next().await,
        }
    }

    /// Read to the end, failing with `too_large()` once more than `limit` bytes arrive.
    pub async fn collect(
        mut self,
        limit: usize,
        too_large: impl Fn() -> FlowerError,
    ) -> Result<Bytes, FlowerError> {
        if let ResponseBody::Full(bytes) = &self {
            return if bytes.len() > limit {
                Err(too_large())
            } else {
                Ok(bytes.clone())
            };
        }
        let mut buffer = BytesMut::new();
        while let Some(chunk) = self.chunk().await {
            let chunk = chunk?;
            if buffer.len() + chunk.len() > limit {
                return Err(too_large());
            }
            buffer.extend_from_slice(&chunk);
        }
        Ok(buffer.freeze())
    }
}

/// How the client talks HTTP. Custom transports implement this (tests use
/// [`crate::testing::MockTransport`]). Cancellation is by dropping the future or the body.
pub trait Transport: Send + Sync + 'static {
    fn send(&self, request: HttpRequest) -> BoxFuture<'static, Result<HttpResponse, FlowerError>>;
}

/// Plain-HTTP protocol choice. HTTPS always negotiates by ALPN.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Protocol {
    /// h2c with prior knowledge, as Flower serves it (default).
    #[default]
    H2c,
    /// HTTP/1.1, for servers that don't speak h2c.
    Http1,
}

/// HTTP/2 flow-control windows for one lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Windows {
    pub stream: u32,
    pub connection: u32,
}

/// Options of [`HttpTransport`]. Every timeout is explicit; see each field.
#[derive(Clone, Debug)]
pub struct TransportOptions {
    pub protocol: Protocol,
    /// Unary connections per origin, used round-robin (`http2Connections(n)`). Default 1.
    pub connections: usize,
    /// Watch connections per origin, used round-robin, separate from unary ones. Default 1.
    pub watch_connections: usize,
    /// Default whole-request deadline when a request sets none: unary calls without retry, and
    /// watch headers. Default 300 s, like undici's headersTimeout/bodyTimeout that bound the TS
    /// client's un-retried calls. Retried calls use their per-attempt timeout (20 s by default).
    pub request_timeout: Duration,
    /// TCP (and TLS) connect deadline. Default 10 s, like undici.
    pub connect_timeout: Duration,
    /// Close connections idle this long. Default 30 s, like `sdk/http2.ts`.
    pub pool_idle_timeout: Duration,
    /// Refuse request bodies above this (`H2_REQUEST_TOO_LARGE`). Default 8 MiB.
    pub max_request_bytes: usize,
    /// Refuse buffered reply bodies above this (`H2_RESPONSE_TOO_LARGE`). Default 64 MiB. Event
    /// streams are bounded per event by the watch budgets instead.
    pub max_response_bytes: usize,
    /// Extra PEM root files for HTTPS, merged with the platform roots. Unreadable files fail
    /// [`HttpTransport::new`].
    pub ca_files: Vec<PathBuf>,
    /// Extra PEM roots, as bytes.
    pub ca_pem: Vec<Vec<u8>>,
    /// Also read `NODE_EXTRA_CA_CERTS` and `TRINITY_CA_FILE` when set (default true). Like Node,
    /// an unreadable file there is skipped with a warning on stderr.
    pub ca_from_env: bool,
    /// Trust only the extra roots, not the platform's. Default false.
    pub roots_only: bool,
    /// Unary lane windows. Default hyper's: 2 MiB per stream, 5 MiB per connection.
    pub unary_windows: Windows,
    /// Watch lane windows. Default 256 KiB per stream and 32 MiB per connection: up to 128
    /// unread watches before one watch connection stalls.
    pub watch_windows: Windows,
    /// HTTP/2 PING interval (and while idle). Default none, like Node.
    pub keep_alive: Option<Duration>,
}

impl Default for TransportOptions {
    fn default() -> Self {
        TransportOptions {
            protocol: Protocol::H2c,
            connections: 1,
            watch_connections: 1,
            request_timeout: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            pool_idle_timeout: Duration::from_secs(30),
            max_request_bytes: 8 * 1024 * 1024,
            max_response_bytes: 64 * 1024 * 1024,
            ca_files: Vec::new(),
            ca_pem: Vec::new(),
            ca_from_env: true,
            roots_only: false,
            unary_windows: Windows {
                stream: 2 * 1024 * 1024,
                connection: 5 * 1024 * 1024,
            },
            watch_windows: Windows {
                stream: 256 * 1024,
                connection: 32 * 1024 * 1024,
            },
            keep_alive: None,
        }
    }
}

struct Lanes {
    unary: Vec<reqwest::Client>,
    watch: Vec<reqwest::Client>,
}

struct Inner {
    plain: Lanes,
    tls: Lanes,
    next_unary: AtomicUsize,
    next_watch: AtomicUsize,
    request_timeout: Duration,
    max_request_bytes: usize,
    max_response_bytes: usize,
}

/// The default [`Transport`]. Cheap to clone; clones share connections.
#[derive(Clone)]
pub struct HttpTransport {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransport")
            .field("unary", &self.inner.plain.unary.len())
            .field("watch", &self.inner.plain.watch.len())
            .finish()
    }
}

impl HttpTransport {
    pub fn new(options: TransportOptions) -> Result<Self, FlowerError> {
        if options.connections == 0 || options.watch_connections == 0 {
            return Err(FlowerError::invalid("connections must be at least 1"));
        }
        let roots = roots(&options)?;
        let build = |tls: bool, lane: Lane| -> Result<reqwest::Client, FlowerError> {
            let windows = match lane {
                Lane::Unary => options.unary_windows,
                Lane::Watch => options.watch_windows,
            };
            let mut builder = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(options.connect_timeout)
                .pool_idle_timeout(options.pool_idle_timeout)
                .http2_initial_stream_window_size(windows.stream)
                .http2_initial_connection_window_size(windows.connection);
            if let Some(interval) = options.keep_alive {
                builder = builder
                    .http2_keep_alive_interval(interval)
                    .http2_keep_alive_while_idle(true);
            }
            if tls {
                builder = if options.roots_only {
                    builder.tls_certs_only(roots.clone())
                } else {
                    builder.tls_certs_merge(roots.clone())
                };
            } else {
                builder = match options.protocol {
                    Protocol::H2c => builder.http2_prior_knowledge(),
                    Protocol::Http1 => builder.http1_only(),
                };
            }
            builder.build().map_err(FlowerError::from_reqwest)
        };
        let lanes = |tls: bool| -> Result<Lanes, FlowerError> {
            Ok(Lanes {
                unary: (0..options.connections)
                    .map(|_| build(tls, Lane::Unary))
                    .collect::<Result<_, _>>()?,
                watch: (0..options.watch_connections)
                    .map(|_| build(tls, Lane::Watch))
                    .collect::<Result<_, _>>()?,
            })
        };
        Ok(HttpTransport {
            inner: Arc::new(Inner {
                plain: lanes(false)?,
                tls: lanes(true)?,
                next_unary: AtomicUsize::new(0),
                next_watch: AtomicUsize::new(0),
                request_timeout: options.request_timeout,
                max_request_bytes: options.max_request_bytes,
                max_response_bytes: options.max_response_bytes,
            }),
        })
    }

    /// The reqwest client (and so the connection) a request to `url` on `lane` would use next, for
    /// raw streaming requests such as a proxy's pass-through.
    pub fn client(&self, url: &str, lane: Lane) -> reqwest::Client {
        self.inner.pick(url.starts_with("https:"), lane).clone()
    }
}

impl Inner {
    fn pick(&self, tls: bool, lane: Lane) -> &reqwest::Client {
        let lanes = if tls { &self.tls } else { &self.plain };
        let (clients, next) = match lane {
            Lane::Unary => (&lanes.unary, &self.next_unary),
            Lane::Watch => (&lanes.watch, &self.next_watch),
        };
        &clients[next.fetch_add(1, Ordering::Relaxed) % clients.len()]
    }
}

fn roots(options: &TransportOptions) -> Result<Vec<reqwest::Certificate>, FlowerError> {
    let mut roots = Vec::new();
    let parse = |pem: &[u8], origin: &str| {
        reqwest::Certificate::from_pem_bundle(pem).map_err(|error| {
            FlowerError::invalid(format!("Invalid PEM roots in {origin}: {error}"))
        })
    };
    for pem in &options.ca_pem {
        roots.extend(parse(pem, "ca_pem")?);
    }
    for path in &options.ca_files {
        let pem = std::fs::read(path).map_err(|error| {
            FlowerError::invalid(format!("Cannot read CA file {}: {error}", path.display()))
        })?;
        roots.extend(parse(&pem, &path.display().to_string())?);
    }
    if options.ca_from_env {
        for name in ["NODE_EXTRA_CA_CERTS", "TRINITY_CA_FILE"] {
            let Some(path) = std::env::var_os(name).filter(|path| !path.is_empty()) else {
                continue;
            };
            let path = PathBuf::from(path);
            match std::fs::read(&path)
                .map_err(|error| error.to_string())
                .and_then(|pem| parse(&pem, name).map_err(|error| error.message))
            {
                Ok(certificates) => roots.extend(certificates),
                Err(error) => eprintln!(
                    "Warning: Ignoring extra certs from `{}`, load failed: {error}",
                    path.display()
                ),
            }
        }
    }
    Ok(roots)
}

fn validate_url(url: &str) -> Result<bool, FlowerError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|error| FlowerError::invalid(format!("Invalid URL {url}: {error}")))?;
    let tls = match parsed.scheme() {
        "http" => false,
        "https" => true,
        _ => {
            return Err(FlowerError::invalid(
                "Flower HTTP/2 transport requires an http:// or https:// URL without credentials",
            ));
        }
    };
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(FlowerError::invalid(
            "Flower HTTP/2 transport requires an http:// or https:// URL without credentials",
        ));
    }
    Ok(tls)
}

fn check_encoding(headers: &HeaderMap) -> Result<(), FlowerError> {
    match headers.get(CONTENT_ENCODING) {
        Some(encoding) if encoding.as_bytes() != b"identity" => Err(FlowerError::transport(
            "H2_UNSUPPORTED_ENCODING",
            "Flower HTTP/2 transport does not decode compressed responses",
        )),
        _ => Ok(()),
    }
}

fn declared_length(headers: &HeaderMap) -> Result<Option<u64>, FlowerError> {
    let Some(value) = headers.get(CONTENT_LENGTH) else {
        return Ok(None);
    };
    let invalid = || {
        FlowerError::transport(
            "H2_INVALID_RESPONSE",
            "HTTP/2 response has an invalid content-length",
        )
    };
    let text = value.to_str().map_err(|_| invalid())?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let length: u64 = text.parse().map_err(|_| invalid())?;
    if length > (1 << 53) - 1 {
        return Err(invalid());
    }
    Ok(Some(length))
}

fn too_large(limit: usize) -> FlowerError {
    FlowerError::transport(
        "H2_RESPONSE_TOO_LARGE",
        format!("HTTP/2 response body exceeds {limit} bytes"),
    )
}

fn body_stream(response: reqwest::Response) -> BoxStream<'static, Result<Bytes, FlowerError>> {
    stream::unfold(Some(response), |response| async move {
        let mut response = response?;
        match response.chunk().await {
            Ok(Some(chunk)) => Some((Ok(chunk), Some(response))),
            Ok(None) => None,
            Err(error) => Some((Err(FlowerError::from_reqwest(error)), None)),
        }
    })
    .boxed()
}

/// A non-event-stream watch reply keeps the whole-body deadline and the byte limit.
fn bounded_stream(
    inner: BoxStream<'static, Result<Bytes, FlowerError>>,
    deadline: Instant,
    limit: usize,
) -> BoxStream<'static, Result<Bytes, FlowerError>> {
    stream::unfold(Some((inner, 0usize)), move |state| async move {
        let (mut inner, received) = state?;
        match tokio::time::timeout_at(deadline, inner.next()).await {
            Err(_) => Some((Err(FlowerError::timeout()), None)),
            Ok(None) => None,
            Ok(Some(Err(error))) => Some((Err(error), None)),
            Ok(Some(Ok(chunk))) => {
                let received = received + chunk.len();
                if received > limit {
                    Some((Err(too_large(limit)), None))
                } else {
                    Some((Ok(chunk), Some((inner, received))))
                }
            }
        }
    })
    .boxed()
}

impl Transport for HttpTransport {
    fn send(&self, request: HttpRequest) -> BoxFuture<'static, Result<HttpResponse, FlowerError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let tls = validate_url(&request.url)?;
            if request.body.len() > inner.max_request_bytes {
                return Err(FlowerError::transport(
                    "H2_REQUEST_TOO_LARGE",
                    format!(
                        "HTTP/2 request body exceeds {} bytes",
                        inner.max_request_bytes
                    ),
                ));
            }
            let deadline = Instant::now() + request.timeout.unwrap_or(inner.request_timeout);
            let limit = inner.max_response_bytes;
            let builder = inner
                .pick(tls, request.lane)
                .request(request.method, &request.url)
                .headers(request.headers)
                .body(request.body);
            match request.lane {
                Lane::Unary => tokio::time::timeout_at(deadline, async move {
                    let mut response = builder.send().await.map_err(FlowerError::from_reqwest)?;
                    check_encoding(response.headers())?;
                    let expected = declared_length(response.headers())?;
                    let status = response.status();
                    let headers = response.headers().clone();
                    let mut buffer = BytesMut::new();
                    while let Some(chunk) =
                        response.chunk().await.map_err(FlowerError::from_reqwest)?
                    {
                        if buffer.len() + chunk.len() > limit {
                            return Err(too_large(limit));
                        }
                        buffer.extend_from_slice(&chunk);
                    }
                    if let Some(expected) = expected
                        && buffer.len() as u64 != expected
                        && status != StatusCode::NOT_MODIFIED
                    {
                        return Err(FlowerError::transport(
                            "H2_RESPONSE_TRUNCATED",
                            format!(
                                "HTTP/2 response ended after {} of {expected} declared bytes",
                                buffer.len()
                            ),
                        ));
                    }
                    let body = if matches!(status.as_u16(), 204 | 205 | 304) || buffer.is_empty() {
                        ResponseBody::Empty
                    } else {
                        ResponseBody::Full(buffer.freeze())
                    };
                    Ok(HttpResponse {
                        status,
                        headers,
                        body,
                    })
                })
                .await
                .unwrap_or_else(|_| Err(FlowerError::timeout())),
                Lane::Watch => {
                    let response = tokio::time::timeout_at(deadline, builder.send())
                        .await
                        .map_err(|_| FlowerError::timeout())?
                        .map_err(FlowerError::from_reqwest)?;
                    check_encoding(response.headers())?;
                    let status = response.status();
                    let headers = response.headers().clone();
                    let events = status.is_success()
                        && media_type(&headers).as_deref() == Some("text/event-stream");
                    let body = if matches!(status.as_u16(), 204 | 205 | 304) {
                        ResponseBody::Empty
                    } else if events {
                        ResponseBody::Stream(body_stream(response))
                    } else {
                        ResponseBody::Stream(bounded_stream(body_stream(response), deadline, limit))
                    };
                    Ok(HttpResponse {
                        status,
                        headers,
                        body,
                    })
                }
            }
        })
    }
}
