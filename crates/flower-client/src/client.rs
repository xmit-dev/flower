//! `FlowerClient`: queries, mutations, calls and partitions, the twin of `FlowerClient` in
//! `sdk/client.ts` (without retry sessions, bounded retry IDs or `queryUrls`, which Trinity does
//! not use).

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::FlowerError;
use crate::json;
use crate::retry::{self, Retry};
use crate::transport::{HttpRequest, HttpTransport, Lane, Transport, TransportOptions};

/// The URL `new FlowerClient()` uses by default.
pub const DEFAULT_URL: &str = "http://127.0.0.1:7101";

/// `{revision, value}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult<T = Value> {
    pub revision: u64,
    pub value: T,
}

/// `{revision, value, duplicate}`: `duplicate` is true when a retry found the original result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MutationResult<T = Value> {
    pub revision: u64,
    pub value: T,
    #[serde(default)]
    pub duplicate: bool,
}

/// Evaluated on every attempt and every watch connection, so a renewed token is picked up by the
/// next retry. The value is sent as the body's `credentials`, e.g. `{"token": "..."}`.
pub trait CredentialProvider: Send + Sync {
    fn credentials(&self) -> BoxFuture<'_, Result<Value, FlowerError>>;
}

struct FnProvider<F>(F);

impl<F, Fut> CredentialProvider for FnProvider<F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<Value, FlowerError>> + Send + 'static,
{
    fn credentials(&self) -> BoxFuture<'_, Result<Value, FlowerError>> {
        Box::pin((self.0)())
    }
}

/// `credentials?: Json | (() => Json | Promise<Json>)`.
#[derive(Clone, Default)]
pub enum Credentials {
    /// Send none.
    #[default]
    None,
    /// The same JSON on every request.
    Static(Value),
    /// Asked on every attempt.
    Provider(Arc<dyn CredentialProvider>),
}

impl Credentials {
    /// `{token}`: a principal's credential.
    pub fn token(token: impl Into<String>) -> Self {
        Credentials::Static(serde_json::json!({ "token": token.into() }))
    }

    /// `{operator}`: the operator's secret, for apps that admit it.
    pub fn operator(secret: impl Into<String>) -> Self {
        Credentials::Static(serde_json::json!({ "operator": secret.into() }))
    }

    pub fn provider(provider: impl CredentialProvider + 'static) -> Self {
        Credentials::Provider(Arc::new(provider))
    }

    /// A provider from an async closure: `Credentials::from_fn(move || async move { ... })`.
    pub fn from_fn<F, Fut>(provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, FlowerError>> + Send + 'static,
    {
        Credentials::Provider(Arc::new(FnProvider(provider)))
    }

    /// The JSON to send now, or `None` for no `credentials` field.
    pub async fn resolve(&self) -> Result<Option<Value>, FlowerError> {
        match self {
            Credentials::None => Ok(None),
            Credentials::Static(value) => Ok(Some(value.clone())),
            Credentials::Provider(provider) => provider.credentials().await.map(Some),
        }
    }
}

impl From<Value> for Credentials {
    fn from(value: Value) -> Self {
        Credentials::Static(value)
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print secrets.
        f.write_str(match self {
            Credentials::None => "Credentials::None",
            Credentials::Static(_) => "Credentials::Static(..)",
            Credentials::Provider(_) => "Credentials::Provider(..)",
        })
    }
}

/// Options of `query` (and the base of the others). `retry: None` uses the client's default.
#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    /// Cancels the call, including between retries (TS: `signal`).
    pub cancel: Option<CancellationToken>,
    pub retry: Option<Retry>,
    /// Replaces the client's credentials for this call.
    pub credentials: Option<Value>,
}

impl RequestOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// `{retry: true}`.
    pub fn retrying() -> Self {
        Self::new().retry(Retry::Default)
    }

    pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
        self.retry = Some(retry.into());
        self
    }

    pub fn cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    pub fn credentials(mut self, credentials: Value) -> Self {
        self.credentials = Some(credentials);
        self
    }
}

/// Options of `mutate` and `call`.
#[derive(Clone, Debug, Default)]
pub struct MutationOptions {
    pub cancel: Option<CancellationToken>,
    pub retry: Option<Retry>,
    pub credentials: Option<Value>,
    /// Fixed before the first attempt; a random UUID when absent. Retries reuse it, so a lost
    /// reply cannot apply twice.
    pub request_id: Option<String>,
    pub expected_revision: Option<u64>,
}

impl MutationOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// `{retry: true}`.
    pub fn retrying() -> Self {
        Self::new().retry(Retry::Default)
    }

    pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
        self.retry = Some(retry.into());
        self
    }

    pub fn cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    pub fn credentials(mut self, credentials: Value) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn expected_revision(mut self, revision: u64) -> Self {
        self.expected_revision = Some(revision);
        self
    }
}

impl From<RequestOptions> for MutationOptions {
    fn from(options: RequestOptions) -> Self {
        MutationOptions {
            cancel: options.cancel,
            retry: options.retry,
            credentials: options.credentials,
            request_id: None,
            expected_revision: None,
        }
    }
}

/// Builds a [`FlowerClient`].
#[must_use]
pub struct ClientBuilder {
    url: String,
    credentials: Credentials,
    partition: Option<String>,
    retry: Retry,
    transport: Option<Arc<dyn Transport>>,
    options: TransportOptions,
}

impl ClientBuilder {
    pub fn credentials(mut self, credentials: impl Into<Credentials>) -> Self {
        self.credentials = credentials.into();
        self
    }

    /// Scope the client to a named logical database.
    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    /// The default for every call; per-call `retry` overrides it (TS: `options.retry`).
    pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
        self.retry = retry.into();
        self
    }

    /// Share a transport (and its connections) between clients, or plug in a custom one.
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Options of the [`HttpTransport`] built when no transport is given.
    pub fn transport_options(mut self, options: TransportOptions) -> Self {
        self.options = options;
        self
    }

    /// Unary connections, used round-robin (`http2Connections(n)`).
    pub fn connections(mut self, connections: usize) -> Self {
        self.options.connections = connections;
        self
    }

    /// An extra PEM root file for HTTPS.
    pub fn ca_file(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.options.ca_files.push(path.into());
        self
    }

    /// Plain HTTP as HTTP/1.1 instead of h2c.
    pub fn http1(mut self) -> Self {
        self.options.protocol = crate::transport::Protocol::Http1;
        self
    }

    /// The deadline of calls that set none (un-retried unary calls, watch headers).
    pub fn request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.options.request_timeout = timeout;
        self
    }

    pub fn build(self) -> Result<FlowerClient, FlowerError> {
        let mut url = http_url(&self.url, "Flower URL")?;
        if let Some(partition) = &self.partition {
            url.push_str(&partition_path(partition)?);
        }
        let transport = match self.transport {
            Some(transport) => transport,
            None => Arc::new(HttpTransport::new(self.options)?),
        };
        Ok(FlowerClient {
            inner: Arc::new(Inner {
                url,
                transport,
                credentials: self.credentials,
                retry: self.retry,
            }),
        })
    }
}

struct Inner {
    url: String,
    transport: Arc<dyn Transport>,
    credentials: Credentials,
    retry: Retry,
}

/// Invokes an application's public methods. Cheap to clone; clones and partitions share the
/// transport.
#[derive(Clone)]
pub struct FlowerClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for FlowerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowerClient")
            .field("url", &self.inner.url)
            .field("credentials", &self.inner.credentials)
            .field("retry", &self.inner.retry)
            .finish()
    }
}

impl FlowerClient {
    pub fn builder(url: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            url: url.into(),
            credentials: Credentials::None,
            partition: None,
            retry: Retry::Off,
            transport: None,
            options: TransportOptions::default(),
        }
    }

    /// A client with default options.
    pub fn new(url: impl Into<String>) -> Result<Self, FlowerError> {
        Self::builder(url).build()
    }

    /// The base URL, without trailing slashes, including any partition path.
    pub fn url(&self) -> &str {
        &self.inner.url
    }

    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.inner.transport
    }

    pub fn credentials(&self) -> &Credentials {
        &self.inner.credentials
    }

    /// The default retry setting.
    pub fn default_retry(&self) -> &Retry {
        &self.inner.retry
    }

    /// The same application in a named logical database: `/partitions/{encodeURIComponent(name)}`.
    /// Keeps the transport, credentials and retry default.
    ///
    /// # Panics
    /// When the name is blank or contains control characters (the TS throws a `TypeError`); use
    /// [`FlowerClient::try_partition`] for names that aren't known to be valid.
    pub fn partition(&self, name: &str) -> FlowerClient {
        self.try_partition(name)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn try_partition(&self, name: &str) -> Result<FlowerClient, FlowerError> {
        Ok(self.with_url(format!("{}{}", self.inner.url, partition_path(name)?)))
    }

    /// The same client with other credentials (and the same transport).
    pub fn with_credentials(&self, credentials: impl Into<Credentials>) -> FlowerClient {
        FlowerClient {
            inner: Arc::new(Inner {
                url: self.inner.url.clone(),
                transport: self.inner.transport.clone(),
                credentials: credentials.into(),
                retry: self.inner.retry.clone(),
            }),
        }
    }

    fn with_url(&self, url: String) -> FlowerClient {
        FlowerClient {
            inner: Arc::new(Inner {
                url,
                transport: self.inner.transport.clone(),
                credentials: self.inner.credentials.clone(),
                retry: self.inner.retry.clone(),
            }),
        }
    }

    /// Invoke a read-only method. `args` of `&()` sends `null`.
    pub async fn query<A, T>(
        &self,
        name: &str,
        args: &A,
        options: RequestOptions,
    ) -> Result<QueryResult<T>, FlowerError>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let args = encode_args(args)?;
        let retry = options.retry.as_ref().unwrap_or(&self.inner.retry);
        retry::run(retry, options.cancel.as_ref(), || async {
            let credentials = self.authorization(options.credentials.as_ref()).await?;
            let body = body(name, &args, None, credentials.as_ref(), None);
            self.post(Lane::Unary, "/v1/query", body, None).await
        })
        .await
    }

    /// [`FlowerClient::query`] returning JSON.
    pub async fn query_value<A: Serialize + ?Sized>(
        &self,
        name: &str,
        args: &A,
        options: RequestOptions,
    ) -> Result<QueryResult<Value>, FlowerError> {
        self.query(name, args, options).await
    }

    /// Invoke an atomic method. Retries keep one request ID and return the original result.
    pub async fn mutate<A, T>(
        &self,
        name: &str,
        args: &A,
        options: MutationOptions,
    ) -> Result<MutationResult<T>, FlowerError>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        self.invoke("/v1/mutate", name, args, options).await
    }

    /// [`FlowerClient::mutate`] returning JSON.
    pub async fn mutate_value<A: Serialize + ?Sized>(
        &self,
        name: &str,
        args: &A,
        options: MutationOptions,
    ) -> Result<MutationResult<Value>, FlowerError> {
        self.mutate(name, args, options).await
    }

    /// Invoke any public alias; deployed code decides whether it is a query, mutation or
    /// transaction.
    pub async fn call<A, T>(
        &self,
        name: &str,
        args: &A,
        options: MutationOptions,
    ) -> Result<MutationResult<T>, FlowerError>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        self.invoke("/v1/call", name, args, options).await
    }

    async fn invoke<A, T>(
        &self,
        path: &str,
        name: &str,
        args: &A,
        options: MutationOptions,
    ) -> Result<T, FlowerError>
    where
        A: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let args = encode_args(args)?;
        let request_id = options.request_id.clone().unwrap_or_else(new_request_id);
        let retry = options.retry.as_ref().unwrap_or(&self.inner.retry);
        retry::run(retry, options.cancel.as_ref(), || async {
            let credentials = self.authorization(options.credentials.as_ref()).await?;
            let body = body(
                name,
                &args,
                Some(&request_id),
                credentials.as_ref(),
                options.expected_revision,
            );
            self.post(Lane::Unary, path, body, None).await
        })
        .await
    }

    /// The credentials to send on this attempt.
    pub(crate) async fn authorization(
        &self,
        explicit: Option<&Value>,
    ) -> Result<Option<Value>, FlowerError> {
        match explicit {
            Some(value) => Ok(Some(value.clone())),
            None => self.inner.credentials.resolve().await,
        }
    }

    pub(crate) async fn post<T: DeserializeOwned>(
        &self,
        lane: Lane,
        path: &str,
        body: Bytes,
        token: Option<&str>,
    ) -> Result<T, FlowerError> {
        let request = request(
            Method::POST,
            format!("{}{path}", self.inner.url),
            body,
            lane,
            token,
        )?;
        let response = self.inner.transport.send(request).await?;
        decode_reply(response).await
    }
}

/// POST/GET with Flower's headers.
pub(crate) fn request(
    method: Method,
    url: String,
    body: Bytes,
    lane: Lane,
    token: Option<&str>,
) -> Result<HttpRequest, FlowerError> {
    let mut headers = HeaderMap::new();
    if method == Method::POST {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    if lane == Lane::Watch {
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    }
    if let Some(token) = token {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| FlowerError::invalid("The admin token is not a valid header value"))?;
        headers.insert(AUTHORIZATION, value);
    }
    Ok(HttpRequest {
        method,
        url,
        headers,
        body,
        lane,
        timeout: None,
    })
}

/// `Connection.request`'s reply handling: errors from non-2xx, JSON from the rest.
pub(crate) async fn decode_reply<T: DeserializeOwned>(
    response: crate::transport::HttpResponse,
) -> Result<T, FlowerError> {
    let status = response.status;
    let status_text = response.status_text();
    let bytes = response.body.collect(usize::MAX, || unreachable!()).await?;
    if !status.is_success() {
        return Err(FlowerError::from_response(
            status.as_u16(),
            status_text,
            &bytes,
        ));
    }
    let text: &[u8] = if bytes.is_empty() { b"null" } else { &bytes };
    json::from_slice_deep(text).map_err(|error| {
        FlowerError::decode(format!("Invalid Flower reply: {error}"), status.as_u16())
    })
}

/// A fresh idempotency key, like `crypto.randomUUID()`.
pub fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Serialize arguments once (JS number spelling), checking `canonicalJson({name, args})`'s nesting.
pub(crate) fn encode_args<A: Serialize + ?Sized>(args: &A) -> Result<Bytes, FlowerError> {
    let bytes = json::to_js_vec(args)?;
    if json::nesting(&bytes) >= json::MAX_DEPTH - 1 {
        let value: Value = json::from_slice_deep(&bytes)
            .map_err(|error| FlowerError::invalid(error.to_string()))?;
        // In `{name, args}`, args sit at depth 1.
        json::check_depth(
            &Value::Array(vec![value]),
            "Flower JSON nesting exceeds 128",
        )?;
    }
    Ok(Bytes::from(bytes))
}

/// `JSON.stringify({name, args, requestId?, credentials?, expectedRevision?})` in that key order.
pub(crate) fn body(
    name: &str,
    args: &[u8],
    request_id: Option<&str>,
    credentials: Option<&Value>,
    expected_revision: Option<u64>,
) -> Bytes {
    let mut output = Vec::with_capacity(args.len() + name.len() + 96);
    output.extend_from_slice(b"{\"name\":");
    json::write_js(&mut output, name).expect("strings encode");
    output.extend_from_slice(b",\"args\":");
    output.extend_from_slice(args);
    if let Some(request_id) = request_id {
        output.extend_from_slice(b",\"requestId\":");
        json::write_js(&mut output, request_id).expect("strings encode");
    }
    if let Some(credentials) = credentials {
        output.extend_from_slice(b",\"credentials\":");
        json::write_js(&mut output, credentials).expect("JSON values encode");
    }
    if let Some(revision) = expected_revision {
        output.extend_from_slice(b",\"expectedRevision\":");
        json::write_js(&mut output, &revision).expect("numbers encode");
    }
    output.push(b'}');
    Bytes::from(output)
}

/// Validate an HTTP(S) URL and strip trailing slashes.
pub(crate) fn http_url(address: &str, label: &str) -> Result<String, FlowerError> {
    let parsed = reqwest::Url::parse(address)
        .map_err(|_| FlowerError::invalid(format!("{label} must be an HTTP or HTTPS URL")))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(FlowerError::invalid(format!(
            "{label} must use HTTP or HTTPS"
        )));
    }
    Ok(address.trim_end_matches('/').to_owned())
}

/// JavaScript's `String.prototype.trim` whitespace (WhiteSpace and LineTerminator).
fn js_space(character: char) -> bool {
    matches!(
        character,
        '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

/// `partitionPath`: `/partitions/{encodeURIComponent(name)}`.
pub fn partition_path(name: &str) -> Result<String, FlowerError> {
    if name.chars().all(js_space) || name.chars().any(|c| c <= '\u{1f}' || c == '\u{7f}') {
        return Err(FlowerError::invalid(
            "Partition name must be nonempty and contain no control characters",
        ));
    }
    Ok(format!("/partitions/{}", encode_uri_component(name)))
}

/// JavaScript's `encodeURIComponent`.
pub fn encode_uri_component(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}
