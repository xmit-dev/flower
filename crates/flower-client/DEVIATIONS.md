# flower-client: deviations from the TypeScript SDK

Where this crate knowingly does something other than `sdk/client.ts`, `watch.ts`, `http2.ts`,
`json.ts`, `bundle.ts`, and why. Anything not listed is meant to behave the same way, and the tests
check it (ports of `client.test.ts`, `watch.test.ts`, `http2.test.ts`, Node-generated JSON goldens,
and a run against the real server).

## JSON

- **`float_roundtrip` stays off.** Without serde_json's `float_roundtrip` feature, parsing a long
  decimal can come out one ulp away from V8's `JSON.parse` (137 of the 470 golden inputs;
  `tests/json.rs` prints the count). This crate formats numbers exactly like V8 (ryu-js, the JS
  exponent rules), so the error is only in *parsing*. The feature is left off because Cargo would
  also turn it on in the Flower server in `--workspace` builds, which is for the Flower owner to
  decide. A crate that re-hashes or re-canonicalizes values it read from Flower should turn it on
  itself (`serde_json = { features = ["float_roundtrip"] }`; Trinity's crates do). The golden test
  formats the exact doubles JS chose, so it passes either way.
- **Non-finite numbers become `null`.** serde_json turns `NaN`/`±Infinity` into `null` before any
  formatter sees them, so `canonical_json_of` and request bodies carry `null` where
  `canonicalJson` throws a `TypeError` (`JSON.stringify` in request bodies also writes `null`, so
  bodies agree).
- **Object key order in request bodies.** `JSON.stringify` puts integer-like keys first
  (`{"2":..,"10":..,"a":..}`); serde_json maps write keys in their own order. Only the bytes of
  non-canonical bodies differ; the server parses them. `canonical_json` sorts by UTF-16 code units
  explicitly and is byte-identical (goldens).
- **Parse depth.** Responses parse up to 512 levels (`json::from_slice_deep`, serde_json's recursion
  limit disabled behind our own counter); V8 has no fixed limit. Flower values are at most 128 deep.
- The TS cycle/accessor/prototype checks have no Rust equivalent (a `Value` cannot hold them).

## Transport

- **Watches use their own connection(s)** (`watch_connections`, default 1), never the unary
  connection: a stalled or flow-controlled event stream cannot slow calls. The TS transport put
  everything on one session per origin (its test asserts one connection); ours opens two.
- **Default request timeout 300 s**, like undici's headers/body timeouts that apply to Trinity's
  `fetch` today; `createHttp2Transport`'s own default is 30 s. Set
  `TransportOptions::request_timeout` (or retry with a per-attempt `timeout`). Event-stream bodies
  never time out (only their headers do); non-event-stream answers on the watch lane keep the
  deadline and size limit.
- **Connections per origin, not `maxSessions`.** `connections` (unary) and `watch_connections` are
  per lane, round-robin; there is no global session cap across origins.
- **Error messages** are reqwest/hyper's, as `fetch failed: <cause chain>`, not Node's text; the
  **codes** follow Node's (`ECONNREFUSED`, `ERR_HTTP2_GOAWAY_SESSION`, `ERR_HTTP2_STREAM_ERROR` with
  "Stream closed with error code NGHTTP2_…", `H2_*`, `UND_ERR_SOCKET`/`UND_ERR_CONNECT` as the
  fallback), so `is_transient` matches. `status_text` for error bodies without a message is the
  canonical reason phrase (h2 carries none).
- TLS: rustls with the platform verifier plus extra PEM (`ca_files` and `ca_pem` fail
  `HttpTransport::new` when unreadable; `NODE_EXTRA_CA_CERTS`/`TRINITY_CA_FILE` are warned about
  and skipped, like Node). A bad certificate is `ERR_TLS_CERT_INVALID` and permanent.

## Client

- **Cancellation.** A cancelled call returns `FlowerError { kind: Aborted, code: "ABORTED",
  message: "This operation was aborted" }` rather than rethrowing the signal's reason (a
  `CancellationToken` has none). Timeouts are `kind: Timeout`, code `TIMEOUT`, message "The
  operation was aborted due to timeout" (the `DOMException` text), transient.
- **Not ported:** `queryUrls` (query fan-out), retry sessions and bounded retries
  (`boundedRetries`, `openRetrySession`, `sessionRequestId`, `refreshRetryIdentity`,
  `retrySessionStatus`, `acknowledgeRetrySession`, `closeRetrySession`), `watchPoll`, and a custom
  `fetch` (use the `Transport` trait instead; `testing::MockTransport` for tests). Request ids are
  UUID v4 (`new_request_id()`), as without `boundedRetries`.
- `partition(name)` panics on names the TS constructor rejects (blank, control characters);
  `try_partition` returns the error.
- `Credentials::Static` takes a JSON `Value`; `Credentials::token(t)` / `::operator(s)` build the
  usual shapes. Providers are asked on every attempt and every watch (re)connection, as in TS.

## Watches

- **Lazy stall detection.** `subscribe` is a pull-driven `Stream`: the stall timer runs while the
  stream is polled, so a consumer that stops reading notices a stall (and reconnects) on its next
  poll rather than having the connection torn down in the background. The values and resets it
  then yields are the same.
- `stall` must be whole milliseconds (≥ 1 ms, ≤ 2^53−1), mirroring `stallMs` validation; option
  and argument errors are yielded as the stream's first item (the TS generator throws on first
  `next()`).

## Admin

- Typed: `initialize`, `deploy`, `key_list`, `key_generate`, `key_bind`, `retention_status`,
  `control_retention`, plus `raft_metrics`, `wait_for_leader` and `uninitialized` (from
  `scripts/init.ts`). Everything else in `FlowerAdmin` (staged deployments, transaction closure,
  key import/unbind/rotate/revoke/retire/destroy/rewrap/cache stats, partition layout/groups/
  create/move/resize/status/wait) goes through the generic `admin.admin(path, &body)` /
  `admin.get(path)`.
- `wait_for_leader` retries transient errors (the node may not be listening yet); `init.ts` polls
  only after the server is up.

## Bundles

- `build_bundle` shells out to `node` running the SDK's own `buildBundle`, so bytes and hash are the
  TS deployer's by construction; it needs Node and an SDK whose `esbuild` resolves. `loadBundle`,
  `writeBundle` and building Wasm bundles are not ported (`Bundle::Wasm` can still be deployed).

## Queue worker

`runQueueWorker`, capacity and reconcile are not here: they live in the `flower-worker` crate,
built on this one.
