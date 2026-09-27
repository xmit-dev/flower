# flower-client API

The Rust twin of `@flower-js/sdk`'s client side (`sdk/client.ts`, `watch.ts`, `http2.ts`,
`json.ts`, `bundle.ts`). The queue worker, capacity and reconcile live in `flower-worker`.
Everything is re-exported at the crate root unless noted. Deviations from the TS: `DEVIATIONS.md`.

```toml
flower-client = { path = "../flower/crates/flower-client" }
```

## Client

```rust
let client = FlowerClient::builder("http://127.0.0.1:7101") // DEFAULT_URL; trailing slashes stripped
    .credentials(Credentials::token(t))   // or ::operator(s), ::Static(json), ::from_fn(|| async {..}), ::provider(p)
    .partition("acme")                    // optional: /partitions/{encodeURIComponent}
    .retry(Retry::Off)                    // client-wide default; per-call `retry` overrides
    .transport(arc_transport)             // optional: share an Arc<dyn Transport> between clients
    .transport_options(TransportOptions { .. })   // or .connections(n) .ca_file(p) .http1() .request_timeout(d)
    .build()?;                            // FlowerClient: Clone (Arc), Send + Sync
FlowerClient::new(url)?;
client.partition("west")        // -> FlowerClient (panics on blank/control-char names); try_partition -> Result
client.with_credentials(c)      // same transport, other credentials
client.url(); client.transport(); client.credentials(); client.default_retry();
```

- `trait CredentialProvider: Send + Sync { fn credentials(&self) -> BoxFuture<'_, Result<Value, FlowerError>>; }`
  is asked on **every attempt and every watch (re)connection**; the JSON goes in the body's
  `credentials` (e.g. `{"token": ..}`), never a header. `Credentials::resolve()` evaluates it.
- `query<A: Serialize + ?Sized, T: DeserializeOwned>(name, &args, RequestOptions) -> Result<QueryResult<T>>`
  (`POST {base}/v1/query {name, args, credentials?}`); `query_value` = `T = Value`.
- `mutate<A, T>(name, &args, MutationOptions) -> Result<MutationResult<T>>`
  (`/v1/mutate {name, args, requestId, credentials?, expectedRevision?}`); `mutate_value`; `call<A, T>` (`/v1/call`).
- Args: `&()` sends `null`. Bodies are `JSON.stringify`-identical (key order name, args, requestId,
  credentials, expectedRevision; JS number spelling via `json::JsFormatter`); args are serialized once.
- `QueryResult<T = Value> { revision: u64, value: T }`, `MutationResult<T = Value> { revision, value, duplicate: bool }`.
- `RequestOptions { cancel: Option<CancellationToken>, retry: Option<Retry>, credentials: Option<Value> }`
  with `::new()`, `::retrying()` (= `{retry: true}`), `.retry(r)`, `.cancel(t)`, `.credentials(v)`.
- `MutationOptions { cancel, retry, credentials, request_id: Option<String>, expected_revision: Option<u64> }`
  with the same builders plus `.request_id(id)`, `.expected_revision(n)`. The request id is fixed
  once before the first attempt (random UUID v4 if absent: `new_request_id()`).
- `CancellationToken` is re-exported (tokio-util). Dropping a future also cancels.

## Retry

- `enum Retry { Off (default: one attempt, no per-attempt timeout), Default (= RetryPolicy::default()), Policy(RetryPolicy) }`,
  `From<bool>`, `From<RetryPolicy>`.
- `RetryPolicy { attempts: u32 = 8, until: Option<SystemTime>, initial_delay = 250ms, max_delay = 30s, timeout = 20s /* per attempt */, retryable: Option<Arc<dyn Fn(&FlowerError) -> bool + Send + Sync>> }`
  with builders `.attempts(n) .until(t) .initial_delay(d) .max_delay(d) .timeout(d) .retryable(f)`.
  Trinity's shapes: `RetryPolicy::default().attempts(3).timeout(Duration::from_secs(5))`, `.attempts(3)`, `.until(t)`.
- Semantics exactly as `attempt()`: never retries a caller cancel; sleeps `backoff(attempt-1)`;
  gives up when `now + delay >= until`.
- `backoff(attempt, initial, max) -> Duration` and `retry::backoff_ms(attempt, initial_ms, max_ms) -> u64`
  (equal jitter, `Math.round`). `retry::run(&retry, cancel, || async {..})` runs any closure under a setting.

## Errors

```rust
pub struct FlowerError { pub status: u16, pub code: String, pub message: String,
                         pub failure: Option<Box<Failure>>, pub kind: ErrorKind, /* source */ }
pub struct Failure { pub code: String, pub message: String, pub details: Option<Value> }
pub enum ErrorKind { Flower, Timeout, Aborted, Transport, Decode, Invalid }   // #[non_exhaustive]
```

- `is_transient()`: `Flower` kind like the TS `FlowerError` rules (no failure; status 0 only
  `WATCH_STALLED`/`WATCH_ENDED`; 408/425/429/≥500); `Timeout` true; `Aborted`/`Decode`/`Invalid` false;
  `Transport` per the Node code regex (`H2_*`, `UND_ERR_*`, `ERR_HTTP2_*`, `ECONNRESET`, `ECONNREFUSED`, …)
  minus `H2_REQUEST_TOO_LARGE`/`H2_UNSUPPORTED_ENCODING`/`H2_RESPONSE_TOO_LARGE` (`error::transient_code`).
- reqwest/hyper/h2/io errors map to Node codes: GOAWAY → `ERR_HTTP2_GOAWAY_SESSION`, stream resets
  (`REFUSED_STREAM`, …) → `ERR_HTTP2_STREAM_ERROR`, refused → `ECONNREFUSED`, DNS → `ENOTFOUND`/`EAI_AGAIN`,
  TLS certificate → `ERR_TLS_CERT_INVALID` (permanent), others → `UND_ERR_SOCKET`/`UND_ERR_CONNECT`.
- Constructors: `new(message, status, code)`, `.with_failure(..)`, `timeout()`, `aborted()`,
  `transport(code, msg)`, `decode(msg, status)`, `invalid(msg)` (TS `TypeError`), `from_response(status, status_text, body)`
  (the unary mapping), `from_watch_response`, `from_reqwest`, `from_io`. Helpers `is_timeout()`, `is_aborted()`, `failure_code()`.

## Watches

- `subscribe<A, T>(name, &args, SubscribeOptions) -> Subscription<T>`: `Stream<Item = Result<Update<T>, FlowerError>> + Send + Unpin`.
  `Update<T = Value> { revision: u64, value: T, reset: bool }`.
  Reconnects with `backoff(failures)` after ends, stalls and transient errors; `reset` on each
  connection's first value; `monotonic` drops older revisions; answers/protocol errors end it with
  one `Err`; cancel or drop ends it (drop closes the HTTP/2 stream). Option/argument errors are the first item.
- `SubscribeOptions { cancel, credentials, budgets: WatchBudgets, reconnect: Reconnect, stall: Duration = 45s, monotonic: bool }`,
  builders `.cancel .monotonic .stall .reconnect .credentials .budgets`;
  `enum Reconnect { Off, On { initial_delay = 250ms, max_delay = 30s } }`.
- `wait_until<A, T, P: FnMut(&T) -> bool>(name, &args, predicate, SubscribeOptions) -> Result<Update<T>>`
  (`WATCH_ENDED` if the subscription ends, `ABORTED` when cancelled); `wait_until_truthy(name, &args, opts) -> Result<Update<Value>>`
  (the TS default predicate `Boolean`, `json::truthy`).
- `watch<A, T>(name, &args, WatchOptions) -> watch::Watch<T>` (`Result<QueryResult<T>>` items, one connection, no reconnect);
  `watch_deltas(name, &args, WatchOptions) -> watch::Deltas` (raw `WatchDelta::{Snapshot{sequence, revision, value}, Patch{sequence, base_sequence, revision, patch: Vec<PatchOperation>}}`).
- `WatchOptions { cancel, credentials, budgets }`; `WatchBudgets { max_event_bytes = 17 MiB, max_value_bytes = 16 MiB, max_patch_operations = 256 }`.
- `watch::{decode_watch_event, apply_patch, apply_patch_in_place, WatchProtocolError, WatchEvent}`, `sse::{SseParser, SseFrame}`
  are public for tools and tests.

## JSON (`flower_client::json`)

- `canonical_json(&Value) -> String` (byte-identical to `sdk/json.ts`; keys sorted by UTF-16 code
  units explicitly, so it is correct even with serde_json `preserve_order` enabled downstream);
  `canonical_json_checked` (rejects nesting > 128), `canonical_json_of<T: Serialize>`.
- `format_number(f64)` (`JSON.stringify` numbers via ryu-js, `-0` → `0`), `compare_utf16`, `push_string`,
  `stringify_len(&Value)` (UTF-8 length of `JSON.stringify`), `truthy`, `nesting(bytes)`,
  `from_slice_deep<T>` (parses up to 512 levels, like `JSON.parse` for Flower's 128-deep values).
- `JsFormatter` (serde_json formatter with JS number spelling), `to_js_vec`, `to_js_string`.
- This crate never enables serde_json `preserve_order`/`arbitrary_precision` (guard test in `tests/guard.rs`),
  nor `float_roundtrip` (a Flower-owner decision); enable those in your own crate if you need them.

## Transport

- `trait Transport: Send + Sync + 'static { fn send(&self, HttpRequest) -> BoxFuture<'static, Result<HttpResponse, FlowerError>>; }`
- `HttpRequest { method, url, headers, body: Bytes, lane: Lane, timeout: Option<Duration> }`, `enum Lane { Unary, Watch }`.
- `HttpResponse { status, headers, body: ResponseBody }`, `enum ResponseBody { Empty, Full(Bytes), Stream(BoxStream<..>) }`
  with `chunk().await`, `collect(limit, too_large).await`. Unary replies are buffered; watch replies return at headers.
- `HttpTransport::new(TransportOptions) -> Result<HttpTransport>` (Clone; `impl Transport`), `.client(url, lane) -> reqwest::Client`
  for raw streaming (proxies) on the same pools.
- `TransportOptions` (all explicit, documented on the fields): `protocol: Protocol::{H2c (default: h2c prior knowledge for http://), Http1}`
  (https:// always ALPN), `connections = 1`, `watch_connections = 1` (watches never share a connection with unary calls),
  `request_timeout = 300s` (un-retried unary calls and watch headers; never an event-stream body),
  `connect_timeout = 10s`, `pool_idle_timeout = 30s`, `max_request_bytes = 8 MiB`, `max_response_bytes = 64 MiB`,
  `ca_files`, `ca_pem`, `ca_from_env = true` (`NODE_EXTRA_CA_CERTS`, `TRINITY_CA_FILE`), `roots_only = false`,
  `unary_windows`, `watch_windows`, `keep_alive`.

## Admin

```rust
let admin = FlowerAdmin::builder(url).admin_token(token).build()?;   // or FlowerAdmin::new(url, Some(token)), ::local(token)
admin.partition("acme")                                   // or try_partition
admin.initialize(&BTreeMap::from([("1".into(), "127.0.0.1:7101".into())])).await?;   // POST /raft/initialize
admin.deploy(&Bundle::from(js_bundle), DeployOptions { request_id: Some(id), preparation: None }).await?; // MutationResult<Value>
admin.key_list().await?;                                  // MutationResult<ManagedKeyCatalog { domain, revision, keys, bindings }>
admin.key_generate("trinity-tokens", "Ed25519", None, Some("key:p:trinity-tokens")).await?;
admin.key_bind("tokens", "trinity-tokens", &["sign", "verify"], Some("bind:p:tokens")).await?;
admin.retention_status().await?;                          // QueryResult<Option<RetentionState>> (rotation, incarnation, extra)
admin.control_retention(rev, &RetentionAction::Rotate { incarnation, epoch_ms: Some(3_600_000), keep_epochs: 24 }).await?;
admin.raft_metrics().await?;                              // GET /raft/metrics (root URL) -> Value
admin.wait_for_leader(Duration::from_secs(30), Duration::from_millis(200)).await?;  // -> metrics
admin.admin::<_, T>("/admin/transactions", &body).await?; // generic POST (partition-scoped); admin.get::<T>(path) generic GET
admin::uninitialized(&metrics)                            // no membership yet (scripts/init.ts's check)
```

Admin calls send `authorization: Bearer <token>`, never retry, and use the same error mapping.

## Bundles

- `build_bundle(&entry, BundleOptions { initialization, node, sdk_dir }) -> Result<JavaScriptBundle>` runs
  `node` on the SDK's own `buildBundle` (`sdk/bundle.ts`), so the bytes and hash equal the TS deployer's.
  SDK dir: option, else `$FLOWER_SDK_DIR`, else this crate's `../../sdk`; `esbuild` must resolve from there
  (a Flower checkout with `node_modules`).
- `JavaScriptBundle { hash, javascript }` (`::new(js)` hashes, `.bytes()`), `enum Bundle { JavaScript(..), Wasm { hash, wasm } }`,
  `Initialization::{Static (default), PerInvocation}`, `bundle::sha256_hex`.

## Testing (`flower_client::testing`)

`MockTransport::new([Step::..])` (scripted replies in order, then hangs), `Step::{Reply(MockReply), Fail(FlowerError), Hang, Handle(..)}`,
`MockReply::{ok, result, json, failure, text, events, event_stream, stream}`, `StreamHandle::{push, close, error, cancelled}`,
`mock.requests() -> Vec<Recorded { method, url, headers, body, json, lane }>` with `.aborted()`.
