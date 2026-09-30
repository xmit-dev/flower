# Flower

The [web handbook](https://flower.xmit.dev/) walks through application code, HTTP methods, scheduling, leases, and cluster operation. [Build Awesome (Eleventy)](https://build.awesome.me/) builds the sources in `docs/` into the Git-ignored `_site/`, with navigation from `scripts/docs/pages.mjs`. Run `npm run docs:build` to generate the site and check its links, `bin/web-preview` for live preview, or `bin/web-deploy` to build and publish it with xmit. See [publishing the handbook](PUBLISHING.md).

Flower is a working prototype of a **Raft-backed database of reactive TypeScript values**. TypeScript defines both the dependency graph and the database's public methods. Every external data read or write goes through a named method.

The [external worker guide](https://flower.xmit.dev/guide/workers.html) designs reactive result reconciliation (`external()` values kept current by `reconcile`) and durable job processing (`queue()` drained by `runQueueWorker`), including worker pools, sharding, and high availability. Its runnable [application](docs/reactive-worker.ts) and [client](docs/reactive-worker-client.ts) demonstrate conditional result publication.

The [architecture review](DESIGN.md) records the implemented improvements and larger experiments still under consideration. The [retention contract](RETENTION.md) covers bounded retries, acknowledgements, transaction closure, key lifecycle and recovery fencing.

```ts
import { collection, define, derive, mutation, query, v } from "@flower-js/sdk";

const counters = collection("counters", v.int({ min: 0 }));
const id = v.string({ min: 1, max: 64 });

const doubled = derive("counter.doubled", (ctx, id: string) =>
  (ctx.get(counters, id) ?? 0) * 2,
  { materialize: { each: counters } },
);

const increment = mutation("internal.counter.increment", { args: id }, (ctx, id) => {
  ctx.set(counters, id, (ctx.get(counters, id) ?? 0) + 1);
  return ctx.get(doubled, id); // sees this mutation's writes
});

const get = query("internal.counter.get", { args: id }, (ctx, id) => ({
  count: ctx.get(counters, id) ?? 0,
  doubled: ctx.get(doubled, id),
}));

export default define({
  definitions: [doubled],
  http: {
    "counter.increment": increment,
    "counter.get": get,
  },
});
```

Schemas (`v`) validate the method argument and every record written through `counters`; their types flow into the callbacks and into typed clients. `materialize: { each: counters }` keeps one maintained `counter.doubled` instance per counter row. Derived values that other code reads are listed in `definitions`; exposed methods register themselves through `http`.

The complete [order example](examples/orders.ts) uses record and argument schemas, an equality index, derived subtotals, per-order materialization, coded failures, mutation methods, and a public query. Bundles run in QuickJS inside Wasmtime; applications can also be written in Rust with the [`flower-sdk`](crates/flower-sdk) crate and deployed as WebAssembly guest modules, as [the benchmark's Rust port](examples/goblin-pizza-rs) is. Each callback starts from an isolated pristine Wasm image; module globals do not persist between calls. Rust owns database state, transaction overlays, queries, and dependency-graph maintenance.

`import { nacl, jwt } from "@flower-js/sdk/crypto"` exposes native cryptography inside those callbacks: TweetNaCl's high-level byte API, HS256/RS256/ES256/EdDSA JWT signing and verification, and authenticated `dir`/`A256GCM` JWT encryption/decryption. Binary inputs go straight from Wasm memory to native code. Fresh randomness is mutation-only; JWT validation uses the invocation clock and disables stale query caching. See the [ticket example](examples/crypto.ts), [API and tradeoffs](https://flower.xmit.dev/reference/crypto.html), and `node tests/e2e-crypto.mjs` after building the server.

The same module supports passkeys: `webauthn` builds WebAuthn ceremony options with fresh challenges and natively verifies the browser's registration and sign-in responses (EdDSA, ES256, ES384, ES512, PS256 and RS256 keys; challenge, origin, RP ID, flags and counters). `sha256` and `base64url` cover the surrounding encodings. The [passkey accounts example](docs/passkeys.ts) stores single-use challenges, passkeys and hashed session tokens, and `node tests/e2e-passkeys.mjs` drives it with a software authenticator.

[Managed keys and prepared-key caching](SECRETS.md) provision encrypted key catalogs separately from code, keep private material out of QuickJS, and reuse native prepared contexts under current authorization. Rotation, historical verification/decryption, retire/revoke/destroy and wrapping-key rewrap are supported.

## Install

Download the server archive for your platform from [GitHub Releases](https://github.com/xmit-dev/flower/releases): Linux (x86-64 or Arm64, glibc 2.35+) or macOS 15+ (Intel or Apple silicon). Each archive contains the standalone `flower` server, its MIT license and third-party notices, with a SHA-256 checksum alongside. The server embeds QuickJS/Wasm and needs no Node installation. The handbook's [Get started](https://flower.xmit.dev/guide/) downloads it, deploys an app and calls it in five minutes.

For TypeScript applications and clients:

```sh
npm install @flower-js/sdk
npx flower --help
```

```ts
import { collection, define, fail, mutation, query, v, FlowerClient } from "@flower-js/sdk";
import { scheduler } from "@flower-js/sdk/scheduler";
import { expiringCollection, queue } from "@flower-js/sdk/temporal";
import { reconcile, runQueueWorker } from "@flower-js/sdk/worker";
import { testDatabase } from "@flower-js/sdk/testing";
```

The npm `flower` command builds modules, deploys and calls methods; the server executable is distributed separately. Use `./flower` to select a downloaded server if both commands are on your path. `@flower-js/sdk/client` exports the clients alone: `FlowerClient`, `FlowerAdmin`, `FlowerError`, `isTransient` and `backoff`. `@flower-js/sdk/worker` uses no Node APIs, so workers can run in Node or a browser. The in-process test database (`@flower-js/sdk/testing`), the H2 transport (`@flower-js/sdk/http2`) and bundling (`@flower-js/sdk/bundle`) are Node-only. Releases include compiled ESM and TypeScript declarations. The SDK was redesigned on 2026-09-24 without backward compatibility; code written for `workQueue`, `define({ maintenance, authorize })` or operator methods on `FlowerClient` must be ported.

## Run it

Requirements: Rust 1.96 or newer, a C compiler for native dependencies, and Node.js 22.18 or newer for SDK/build tools. The server runs independently of Node.

```sh
npm ci
cargo build
```

On Nix, `nix develop -c cargo build` uses the pinned Rust 1.98.1 toolchain; the development shell also includes Node 26.10.0. `nix build` builds the standalone server without Node.

`nix build .#sdk` builds `@flower-js/sdk` as npm installs it, under `lib/node_modules/@flower-js/sdk`, with the SDK's own `flower` command in `bin/` (`nix run .#cli -- --help`). Other flakes get both packages from `overlays.default` (`pkgs.flower`, `pkgs.flower-sdk`). A NixOS host runs any number of nodes with `nixosModules.default`:

```nix
{
  imports = [ flower.nixosModules.default ];
  services.flower.instances = {
    main = {
      listen = "127.0.0.1:7101";
      initialize = true; # bootstrap a one-member cluster on first start
    };
    staging.listen = "127.0.0.1:7102";
  };
}
```

Each instance is a `flower-<name>` service with its data in `/var/lib/private/flower-<name>`, and generates its operator token and keyring there unless `adminTokenFile` and `keyringFile` name files kept out of the Nix store. The `flower-<name>` command is the SDK's, aimed at the instance, with its operator token for root: `sudo flower-main deploy app.ts`. Modules that deploy read `services.flower.instances.<name>.url` and `.adminTokenPath` (for systemd's LoadCredential). `nix flake check` builds both packages and, on Linux, boots a VM that runs two instances, deploys `examples/orders.ts` to each with the packaged SDK, and checks that they keep their data apart and across restarts.

[OpenTelemetry reporting](TELEMETRY.md) adds opt-in request traces and detailed query, writer, evaluator, storage, and Raft metrics. The [benchmark profiler](bench/README.md#opentelemetry-reporting) captures a local report without a separate collector service.

Choose an operator token and use the same value in both terminals. It protects deployment, cluster administration, and peer RPCs; ordinary method callers do not receive it.

Start a single-node cluster:

```sh
export FLOWER_ADMIN_TOKEN='local-development-secret'
./target/debug/flower --id 1 --listen 127.0.0.1:7101 --data .flower/node1
```

In another terminal, initialize it **once**, deploy the example, and call its methods:

```sh
export FLOWER_ADMIN_TOKEN='local-development-secret'
node sdk/cli.ts init --members 1=127.0.0.1:7101
node sdk/cli.ts deploy examples/orders.ts

node sdk/cli.ts call order.create @examples/orders.create.json --request-id create-order-42
node sdk/cli.ts call order.get '"order-42"'
# value: { order: { shippingCents: 500 }, subtotal: 3200, total: 3700 }

node sdk/cli.ts mutate order.updateLine @examples/orders.update.json --request-id update-line-2
node sdk/cli.ts query order.get '"order-42"'
# value: { order: { shippingCents: 500 }, subtotal: 4600, total: 5100 }

node sdk/cli.ts watch order.get '"order-42"'

node sdk/cli.ts call order.create @examples/orders.create.json --request-id create-order-43
# flower: ORDER_EXISTS: Order already exists
```

A method's own failure keeps its code: the CLI prints `CODE: message` and any JSON details, then exits with status 1. `--credentials JSON` (or `FLOWER_CREDENTIALS`; `@FILE` reads a file) sends application credentials with `call`, `mutate`, `query` and `watch`.

The cluster needs a brief election after initialization. If a request reports `UNAVAILABLE` immediately after bootstrap, retry when authenticated `/raft/metrics` reports `state: "Leader"`. Do not initialize again when restarting an existing data directory.

For three nodes, start three server processes with distinct IDs, listen addresses, and data directories, all sharing the operator token. Initialize once with the full membership:

```sh
node sdk/cli.ts init --members 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103
```

Point `--url http://127.0.0.1:7101` or `FLOWER_URL` at any reachable member. Servers forward mutations and deployments to the current leader, preserving request IDs across retries. Queries and watches execute on the addressed replica and remain fresh by default. Authenticated `/raft/metrics` exposes leadership for operators; SDK callers do not need it. `--advertise HOST:PORT` sets the peer address when it differs from the listen address. Three nodes tolerate one failed node; a minority cannot acknowledge mutations or serve strong reads. Application code can explicitly permit replica-local reads during quorum loss.

Raft defaults target a low-latency local network: 50 ms heartbeats and a randomized 150–300 ms election delay. OpenRaft also waits a 300 ms leader lease after the last leader contact, so failure detection normally starts an election after roughly 450–600 ms, plus timer ticks, network, storage, and scheduling delay. This is a timing policy, not a recovery-time guarantee. Strong reads and mutation acknowledgements still require a quorum.

For slower networks, storage, or heavily scheduled hosts, set the same overrides on every member before starting it:

```sh
export FLOWER_RAFT_HEARTBEAT_MS=200
export FLOWER_RAFT_ELECTION_MIN_MS=800
export FLOWER_RAFT_ELECTION_MAX_MS=1600
```

These environment-only settings are integer milliseconds in `1..3600000`, with `heartbeat < election minimum < election maximum`. The example restores the earlier conservative timing: a committed leader's failure detector waits about 2.4–3.2 seconds before election overhead. Lower values shorten recovery but can cause unnecessary elections when ordinary communication, durable writes, or runtime scheduling exceed the timeout. Keep the same values across the cluster; these settings do not alter durability or quorum requirements.

Run `npm run bench:stress` to measure serving-quorum recovery under the mixed workload, with a leader crash and an independent audit in every group; see the [benchmark guide](bench/README.md) for accounting and workload controls.

To run an automated temporary three-process cluster, including leader crashes and recovery:

```sh
cargo build
node tests/e2e.mjs
```

## Methods are the public interface

| Definition | Capabilities | External invocation |
| --- | --- | --- |
| `derive(name, fn, { materialize? })` | Read sources and derived values; return JSON | Internal only |
| `aggregate(name, options)` | Maintain indexed groups with add/remove deltas | Internal only |
| `external(name, options)` | A value computed by workers outside the database | Internal; `.http(prefix)` generates worker methods |
| `query(name, spec?, fn)` | Read one consistent snapshot; fresh by default | Only when listed in `http` |
| `mutation(name, spec?, fn)` | Read, stage writes, change materialization, return JSON | Only when listed in `http` |
| `transaction(name, spec?, planner)` | Coordinate exposed methods across logical databases | Only when listed in `http` |

A method `spec` is `{ args?, access? }`, plus `consistency` for queries. `define({ uses, collections, definitions, tasks, triggers, keys, http, auth })` declares the application and owns the complete HTTP allowlist. Definitions are private unless explicitly exposed in `http`; exposed methods are registered automatically. Public aliases can differ from internal names, and several aliases can share a method. Derived values cannot be exposed directly. `uses` takes components: bundles of collections, definitions, maintenance tasks, triggers and keys, made with `component()` or returned by `scheduler()`, `queue()`, `expiringCollection()` and `external()`. Components never add HTTP aliases; helpers that generate public methods return them from `.http(prefix)` for you to spread into `http`. Names beginning with `$flower.` are reserved.

`POST /v1/call` invokes a public alias; Flower determines whether it is a query, mutation, or cross-group transaction from the deployed code. The SDK's `call` and CLI's `call` command use this endpoint. `/v1/query` and `/v1/mutate` use the same allowlist and additionally assert the method's kind. Unknown or removed aliases return `404 METHOD_NOT_FOUND`.

There are no public record, snapshot, transaction-patch, or raw changefeed endpoints. Callers cannot invoke a derived definition directly, invoke a mutation through the query route, or attach raw writes to a method call.

Query and mutation methods can contain arbitrary synchronous application logic, including reads across collections. Methods decide what data to expose. An `args` schema runs before the method body; a violation fails with `INVALID_ARGUMENT` and `details.path`. A collection declared with a record schema, `collection("orders", schema)`, validates every `ctx.set` through that reference (`INVALID_RECORD`). `.key(schema)` makes keys typed JSON values, stored as canonical JSON (`INVALID_KEY`). Schemas are `v.string`, `v.number`, `v.int`, `v.boolean`, `v.null`, `v.literal`, `v.enum`, `v.array`, `v.tuple`, `v.object` (closed: unknown properties are rejected; `v.optional` marks optional ones), `v.nullable`, `v.union`, `v.record`, `v.json`, `v.refine` and `v.lazy`; any synchronous Standard Schema, such as zod, is accepted in their place. Validation is SDK code inside the callback: reads do not revalidate stored records, and a reference declared without the schema writes unchecked.

Inside functions, `ctx.get(collection, key)` returns a record or `null`. `ctx.scan(collection, options?)` returns `{key, value}` rows, decoding typed keys. Options select an ordered `index`, constrain its `prefix` and `gt`/`gte`/`lt`/`lte` bounds, and apply `reverse`, `offset`, and `limit`. Without an index, scans order and constrain source keys; on a tuple-keyed collection, `prefix` selects leading key components and bounds require an index. `ctx.query(collection.by(index).eq(value))` returns matching record values; index names, fields and equality values are typed from the collection. Plain scans and equality queries use lexical primary-key order, which for typed keys is the order of their canonical JSON text, not of component values. Multi-field equality indexes accept a tuple.

Indexed collections must reach `define`, through `collections`, a component in `uses`, an aggregate's `source` or a trigger's collection, to persist equality and ordered scalar indexes and track dependencies. `aggregate` maintains retained group accumulators from changed rows; see the [complete indexed aggregate example](INDEXES.md). Use `ctx.range(ref.by(index).range({prefix, gte, lte, limit, after, reverse}))` for bounded `{rows,cursor}` pages; bounds select the field after the prefix. Cursors continue against the next invocation’s snapshot. Undeclared collections retain a Rust scan fallback. Derived scans and ranges depend only on the index entries they examined and the values they returned; equality queries depend on matching buckets.

Mutation methods additionally have `ctx.set`, `ctx.delete`, `ctx.materialize`, and `ctx.unmaterialize`. All reads see prior writes from the same invocation, including fresh derived results. A thrown method error aborts the entire mutation. Returning an error object is a successful result unless the method throws. A `trigger(name, collection, (ctx, { key, before, after }) => …)` runs inside every mutation method that changed a row of its collection, once per changed key, after the method body and before commit. Its writes can trigger further rounds, up to 32 (`TRIGGER_LOOP`).

### Failures keep their codes

`fail(code, message, details?)` aborts with a structured failure: an UPPER_SNAKE_CASE code, a message and optional JSON details. Any thrown error with a string `code` property is reported the same way; other exceptions become `COMPUTE_ERROR`, and exhausted execution or memory budgets `EVALUATION_BUDGET`. The caller receives HTTP 422:

```json
{"error":{"code":"EVALUATION_FAILED","failure":{"code":"OUT_OF_STOCK","details":{"available":1,"item":"dough"},"message":"Not enough stock"},"message":"OUT_OF_STOCK: Not enough stock"}}
```

Match on `failure.code`; the outer `code` names the transport outcome. Details survive only for methods: a derived value's stored error keeps `{code, message}`, so a method reading it fails with that code and no details. Authorization hook failures arrive as `403 FORBIDDEN` with `failure`, and a participant's failure in a cross-group transaction as `422 TRANSACTION_ABORTED` with `failure`. [PROTOCOL.md](PROTOCOL.md#errors) specifies every shape, including SSE error events.

```ts
import { FlowerClient, FlowerError } from "@flower-js/sdk";
import type counter from "./counter.ts"; // the module above

const client = new FlowerClient<typeof counter>("http://127.0.0.1:7101");
const receipt = await client.mutate("counter.increment", "visits", {
  requestId: "visit-123", retry: true,
});
// { revision, value: 2, duplicate: false }

const { value } = await client.query("counter.get", "visits");
// { count: 1, doubled: 2 }

try {
  await client.mutate("counter.increment", "");
} catch (error) {
  if (!(error instanceof FlowerError)) throw error;
  // error.status 422, error.code "EVALUATION_FAILED", error.failure
  // { code: "INVALID_ARGUMENT", message: "must not be empty", details: { path: [] } }
}
```

`FlowerClient<typeof app>` types aliases, arguments and results from the module; without the type argument, aliases are strings and values are JSON. `query` accepts query aliases, `mutate` mutation aliases and `call` any alias, including transactions. `client.partition(name)` addresses a named logical database. Operator endpoints (deployment, initialization, keys, retention, partitions and transaction closure) belong to `FlowerAdmin`, which holds the operator token; `FlowerClient` never sends it.

### Authenticate callers

```ts
import { collection, define, fail, jwtBearer, key, mutation, query, v } from "@flower-js/sdk";

const sessions = key("sessions", { algorithm: "Ed25519", usages: ["verify"] });
const shops = collection("shops", v.object({ name: v.string({ min: 1, max: 80 }) }));

const catalog = query("catalog.list", { access: "public" }, (ctx) =>
  ctx.scan(shops).map((row) => row.value.name));
const me = query("me", (ctx) => ctx.principal());
const rename = mutation("shop.rename", {
  args: v.object({ shop: v.string({ min: 1 }), name: v.string({ min: 1, max: 80 }) }),
  access: (_ctx, principal, args) => principal?.tenant === args.shop,
}, (ctx, { shop, name }) => {
  if (ctx.get(shops, shop) === null) fail("SHOP_NOT_FOUND", `No shop ${shop}`);
  ctx.set(shops, shop, { name });
  return null;
});

export default define({
  auth: {
    authenticate: jwtBearer({ key: sessions, issuer: "shop", audience: ["shop-api"] }),
  },
  http: { "catalog.list": catalog, me, "shop.rename": rename },
});
```

`define` compiles `auth` and each exposed method's `access` into the application's single authorization hook, a pure query that decides every request before receipt replay, query-cache reuse and watch delivery. The hook notices what a decision read of the call's arguments (through `request.args` in `authenticate`, or a predicate's `args`): nothing, top-level fields by name (`args.session`), or more (enumerating or serializing them). The server reuses a decision for the same credentials, method and partition with arguments that agree on the fields it read (the same arguments, when it read more or there were none), while the records it read are unchanged and its declared time (a JWT's expiry) has not come, so authentication and per-session policies run about once per credential, method and session instead of once per request. `FLOWER_AUTHORIZATION_CACHE_BYTES` (default 16 MiB per logical database, 0 disables) bounds the decisions kept. Clients send credentials beside the arguments, never inside retry fingerprints: `new FlowerClient(url, { credentials: async () => ({ token: await currentToken() }) })` evaluates the function for each call and connection.

- `authenticate(ctx, credentials, request)` returns a principal `{subject, tenant?, claims?}`, returns `null` for an anonymous caller, or `fail()`s to reject. `jwtBearer({ key, algorithms?, issuer?, audience?, clockToleranceSeconds?, principal? })` verifies `"Bearer …"`, a bare token or `{ token }` natively. Missing credentials are anonymous and an invalid token fails `UNAUTHENTICATED`. By default `sub` becomes the subject and a string `tenant` claim the tenant. A managed key joins `define({ keys })` automatically; raw keys require `algorithms`.
- `access` is `"public"`, `"authenticated"` or a predicate `(ctx, principal, args) => boolean` that sees arguments already validated by the method's `args` schema. Methods without `access` use `auth.default`: `"authenticated"` when `authenticate` is set, otherwise `"public"`.
- Anonymous callers are admitted as subject `$anonymous`, which `authenticate` cannot return; methods see `ctx.principal() === null`. All anonymous callers share one receipt-owner scope.
- `auth.sessions` sets access to retry sessions (default `auth.default`). `auth.delegation(ctx, coordinator, principal)` decides whether to accept a principal delegated by a transaction coordinator; by default cluster peers are trusted.

Denials return `403 FORBIDDEN` with the hook's failure: `UNAUTHENTICATED` for missing or invalid credentials, `FORBIDDEN` when a predicate returns false, or `INVALID_ARGUMENT` when a predicate's arguments fail the schema. On a named partition the server admits only principals whose tenant equals the partition name: `authenticate` must return that tenant (for `jwtBearer`, a matching `tenant` claim), while anonymous callers and principals delegated by a transaction coordinator are admitted as that partition's tenant.

`define` compiles a hook only when a call could be refused or a delegation judged: when `auth.authenticate` or `auth.delegation` is set, or an exposed method's `access` is a predicate. `access: "public"` alone adds none. A hook has costs: replica-local queries and watches then obtain a quorum-backed snapshot, public cache hits take the fully admitted path, and mutations no longer use the serial batched-preparation path. Without a hook, calls are anonymous and `ctx.principal()` is null. The hook reads policy but must not perform once-only effects; consume one-time challenges or quotas inside the mutation.

### Spread reads across replicas

Queries and SSE watches evaluate on the node receiving the request. By default they are linearizable: the serving node obtains a quorum-backed read fence from the leader and waits until its own committed state has applied that fence before capturing a snapshot. This distributes query CPU across replicas while retaining fresh reads. Quorum loss can make fresh reads unavailable.

Configure read endpoints separately from the primary write URL:

```ts
const client = new FlowerClient("http://127.0.0.1:7101", {
  queryUrls: [
    "http://127.0.0.1:7101",
    "http://127.0.0.1:7102",
    "http://127.0.0.1:7103",
  ],
});
await client.query("counter.get", "visits");
// Concurrent queries rotate through these endpoints.
const updates = client.subscribe("counter.get", "visits");
```

`query()` attempts, new `watch()`/`watchDeltas()`/`subscribe()` connections, and new `watchPoll()` subscriptions share a round-robin endpoint list. Each connection stays on its selected endpoint for its lifetime; `subscribe()` takes the next endpoint when it reconnects. Mutations and generic `call()` use the primary URL, as do `FlowerAdmin` deployments and initialization. That URL may name any reachable member: servers forward writes and deployments to the current leader with the original request identity. Initialization still addresses the uninitialized group directly. A query through `/v1/call` can run on a follower, but use `client.query()` to distribute SDK reads. Omitting `queryUrls` retains one-endpoint behavior. Endpoint selection does not change consistency or discover members automatically; a query retried under `retry` takes the next endpoint for each attempt. The HTTP/2 transport pools a session per configured origin.

For a read that may tolerate lag, opt in within the deployed TypeScript:

```ts
const getLocal = query("internal.counter.local", {
  args: id, consistency: "replica-local",
}, (ctx, id) => ctx.get(counters, id) ?? 0);
// Expose getLocal through define({ http: { "counter.local": getLocal } }).
```

Replica-local queries and watches use one coherent, locally applied committed snapshot without a per-read quorum fence. After startup recovery completes, they can run during a partition, but there is **no bounded-staleness guarantee**. A restarted node whose durable log extends beyond its recovered application checkpoint first needs a quorum-confirmed read fence and local replay before serving these reads. State, deployed code, and the HTTP allowlist can all lag; removing a local-read alias takes effect on a disconnected replica only when it applies that deployment. Separate calls on different replicas may return decreasing revisions. An authorization hook (from `auth` or any method's `access`) or a managed-key declaration requires fresh policy, so those queries and watches obtain a quorum-backed snapshot even when declared replica-local. Neither an HTTP caller nor a client option can weaken a method's declared policy. Omitting the option, or explicitly selecting `"linearizable"`, keeps fresh-read semantics; mutations and maintenance reject consistency metadata.

### HTTP/2 from Node.js

The same server port accepts HTTP/1.1 and HTTP/2. By default it serves cleartext h2c with prior knowledge; configuring the three `FLOWER_TLS_*_FILE` settings enables native TLS with h2/HTTP1 ALPN. HTTP/1.1 Upgrade is not supported. Internal traffic uses pooled HTTP/2 with verified HTTPS when TLS is enabled. See [TLS.md](TLS.md) for certificates, CA trust, separate peer/operator credentials and rotation.

To multiplex Node.js application calls over HTTP/2, use the optional transport:

```ts
import { FlowerClient } from "@flower-js/sdk";
import { createHttp2Transport } from "@flower-js/sdk/http2";
import type counter from "./counter.ts";

const transport = createHttp2Transport({ requestTimeoutMs: 10_000 });
const client = new FlowerClient<typeof counter>("http://127.0.0.1:7101", {
  fetch: transport.fetch,
});
try {
  const result = await client.mutate("counter.increment", "visits", {
    requestId: "visit-over-h2-001",
    signal: AbortSignal.timeout(5_000),
  });
  console.log(result);
} finally {
  await transport.close();
}
```

The package subpath is `@flower-js/sdk/http2`. It is Node-only and stays out of application bundles and browser clients. It accepts `http://` and `https://` origins, reuses a multiplexed session per origin and buffers bounded ordinary JSON responses with a 30-second default whole-request deadline. SSE watches stream their response body immediately; that deadline covers response headers only, and the caller’s signal or `close()` controls their lifetime. `close()` cancels active streams and closes owned sessions. The transport itself does not retry, redirect, or downgrade to HTTP/1.1; the client's `retry` option retries above it with the same request ID, and callers must otherwise preserve request IDs after uncertain responses. `FlowerAdmin` accepts the same `fetch` option. Aborting a request stops waiting, not a mutation that may already have committed. HTTPS validates certificates and hostnames; optional `ca` accepts PEM strings, Uint8Arrays or arrays of them to replace the transport’s trusted roots. Otherwise Node’s configured trust defaults apply. There is no verification bypass.

An HTTP/2-enabled curl can check the cleartext listener:

```sh
curl --http2-prior-knowledge -i http://127.0.0.1:7101/health
```

### Retry-safe methods

An optional `expectedRevision` on mutations rejects stale writes. Keep the **same request ID and arguments** after an uncertain response. Request receipts persist the original method result, so retries after failover or code deployment return that result without executing the method again, provided the alias remains exposed as a mutation. Exposure is checked before receipt lookup: removing an alias also blocks retries through it. Reusing an ID with different request content is a conflict. A mutation declared with `receipt: false` never keeps one, so a retry runs it again: use it for methods that are idempotent by construction. A mutation that wrote nothing keeps no receipt either, since running it again cannot repeat an effect: a retry of an empty claim runs again, and can claim a job that has become ready since. Requests of retry sessions keep their receipts, which acknowledgements count. Separate CLI invocations generate different IDs unless `--request-id` is supplied.

`retry: true`, per call or as a `FlowerClient` default, retries transient failures (network errors, per-attempt timeouts, 408, 425, 429 and 5xx) with jittered exponential backoff, reusing the request ID chosen for the call. A method's own failure is never retried, nor are 409 conflicts such as `REQUEST_ID_REUSED` or `RETRY_WINDOW_EXPIRED`. A `RetryPolicy` object sets `attempts` (default 8, including the first), `until` (no retry starts after this epoch-millisecond deadline; the first attempt always runs), `initialDelayMs` (250), `maxDelayMs` (30,000), `timeoutMs` per attempt (20,000) and `retryable` (default `isTransient`). Aborting the call's signal stops retrying; it does not undo a mutation that may already have committed.

### Watch a query over SSE

`watch(query, args, {signal})` opens `POST /v1/watch` and yields reconstructed `{revision, value}` results. The server sends an initial snapshot, then JSON Patch deltas when the returned value changes; small replacements can use another snapshot. Unrelated revisions with identical values produce no event. Watch one query that returns all related values to keep a page consistent.

```ts
const stop = new AbortController();
for await (const { revision, value, reset } of client.subscribe("counter.get", "visits", {
  signal: stop.signal,
})) {
  console.log(revision, value, reset);
  // Break the loop or call stop.abort() to close the stream.
}
```

`subscribe()` turns watches into a live value. It reconnects with jittered backoff after disconnects and transient errors, treats `stallMs` without any bytes (heartbeats included; default 45,000) as a disconnect, and marks the first snapshot of every connection `reset: true`, because intermediate values may have been skipped. `monotonic: true` drops values older than the newest revision already delivered, as a lagging replica can produce after a reconnect. `reconnect` accepts `false` or `{ initialDelayMs, maxDelayMs }`. Non-transient errors, including the query's own failure and authorization denial, end the subscription with `FlowerError`. `waitUntil(alias, args, predicate, options)` resolves with the first update whose value satisfies the predicate; queries that take `null` may omit `args`.

Identical invocations share evaluation, diffing, and immutable encoded updates within a node and logical database, scoped by the full admitted principal, deployment, and consistency. Each subscriber independently runs authorization and its read fence, including idle ticks; credentials are never shared. A consumer that misses a producer update receives a full reset snapshot at an increasing sequence. Patches always name the exact preceding sequence. Deployment or principal-scope changes end the stream with `503 WATCH_SCOPE_CHANGED`; `subscribe()` treats that as transient and reconnects for a fresh snapshot, while `watch()` needs a new call.

Use `watchDeltas(...)` to receive `{type: "snapshot", sequence, revision, value}` and `{type: "patch", sequence, baseSequence, revision, patch}` directly. Patches use RFC 6902 `add`, `remove`, and `replace`, with escaped JSON Pointer paths. SDK reconstruction isolates yielded values, so editing a received object does not corrupt the next update. Explicit `watchPoll(query, args, {intervalMs, signal})` retains polling compatibility; `intervalMs` does not apply to SSE.

Every reevaluation rechecks the exposed-query allowlist and its declared consistency. Fresh watches establish a quorum-backed read fence for each evaluation. Idle watches have no timer: commits wake them, and Raft role, term and leader changes or a lapsed leader lease make every fresh watch prove a new fence. A clock-dependent query wakes at the instant it declares with `ctx.changesAt()`, and so does an authorization; one that reads `ctx.now()` without declaring is re-evaluated every `FLOWER_WATCH_REFRESH_MS` (250 ms by default). Replica-local watches use the local applied state and registry, including their replication lag. Wake-ups can be delayed by load, and a write refreshes a result no sooner than its last refresh's cost allows: `FLOWER_WATCH_DUTY_PERCENT` (10% by default) of the time, so a result that took 20 ms refreshes at most every 200 ms while what it read keeps changing, and the writes in between join one refresh. Together, the refreshes writes start on one server take at most `FLOWER_WATCH_BUDGET_PERCENT` of one core's CPU time (50% by default; 0 turns it off), shared equally by the results refreshing for writes at the time: with N of them, one whose refresh took T of CPU refreshes about every N × T × 100 / that percent when that is longer than its own pace, so a costly result refreshes less often and a cheap one never waits for a costly one. New subscribers, and access, code, clock, Raft and lapse wakes, are never held, also while a write waits. Streams send updates immediately while their one-item output queue has room. When a changed update finds the queue full, they wait for space and batch intermediate changes into one refresh of the latest value, rechecking admission, the read fence and authorization before sending. This can skip intermediate commits. There is no fixed watch-count ceiling. Each stream has bounded buffering and a configurable timeout for waiting for room to send a changed update (five seconds by default); the shared node preparation pool bounds evaluation concurrency. Refresh and heartbeat intervals are configurable too.

A terminal server error arrives as an SSE `error` event and becomes `FlowerError`, with `failure` when the query itself failed. `watch()` and `watchDeltas()` then end and never reconnect; `subscribe()` reconnects after transient errors, taking the next endpoint in `queryUrls`. No connection moves once established. Every new connection begins with a full snapshot at the shared producer’s current sequence (which may be nonzero); `Last-Event-ID` does not resume history. This is a live view, not a durable event log. Both HTTP/1.1 and the Node HTTP/2 transport support watches.

### A live pizza dashboard

The [Goblin Pizza dashboard](examples/pizza-dashboard) watches **one `pizza.dashboard({tenant})` value** for the selected tenant's stores, orders, timers, delivery leases, and leaderboard. New streams rotate across running replicas; switching tenants closes the previous stream. **This observational view is replica-local: data, code, and aliases may lag without a bound, and reconnecting can show an older revision even while connected.** The page labels this policy and marks disconnected data. Countdown labels use approximate local time; mutations determine actual deadlines.

```sh
bin/demo
# Builds the server, then opens the dashboard. Other paces, a fixed port,
# initially paused arrivals, and automatic shutdown:
bin/demo --rate 80 --port 3030 --paused --duration 120
```

The launcher creates a fresh three-node Rust/QuickJS cluster with three tenants and two stores each. Customers arrive as a Poisson process at 40 orders a second by default (`--rate`, 1 to 150, or the pace buttons), with each kitchen's popularity drifting over time, and a drone fleet sized to that pace claims deliveries from each tenant's queue scope. Idle drones wait for an oven bell rather than polling, now and then a drone vanishes so another reclaims its expired lease, and customers tip. Buttons place an order, tip a kitchen, pause arrivals, or kill the leader; the other replicas elect a replacement and the old one restarts from its data directory. Pausing arrivals leaves the drones running.

Every two seconds the launcher calls `pizza.archive`, which folds deliveries older than ten seconds into per-kitchen tallies and deletes their order rows and queue jobs, so the dashboard's work stays the size of the orders in flight. Retry receipts and other per-call state still accumulate, about 1 MB/s across the three replicas at the default pace, so arrivals stop at 150,000 orders (`--max-orders`). The board shows the 120 orders with the latest activity plus every delivery and oven timer in flight; totals include archived orders. A pulse panel charts orders placed and delivered per second across all tenants and shows each replica's role, applied index, and dashboard streams, with write latency measured by the launcher. Ctrl+C stops owned processes and removes their temporary data. It never attaches to an existing database.

Store identity is the tuple `[tenant, store]`. Shops are keyed by that tuple and orders by `[tenant, store, orderId]`, both declared with `.key(v.tuple(...))`. Different tenants and stores can reuse the same local order ID. Each tenant claims deliveries from its own scope of one queue and has its own derived leaderboard. Order statistics are an incremental aggregate, and one store summary per shop row stays materialized through `materialize: { each: shops }`; rankings compute from those summaries when read, so tips do not sort or replicate whole rankings. The scheduler uses composite timer IDs; order and dashboard queries use the durable store index.

```ts
import type pizza from "./examples/goblin-pizza-ts/goblin-pizza.ts";

const client = new FlowerClient<typeof pizza>("http://127.0.0.1:7101");
await client.mutate("pizza.setup", {
  tenants: ["goblins", "elves"], storesPerTenant: 2,
  stockPerShop: 1_000, bakeMs: 1_800, leaseMs: 4_000,
});
await client.mutate("pizza.order", { shop: ["goblins", "store-0"], id: "lunch", quantity: 2 });
await client.query("pizza.shop.local", ["goblins", "store-0"]); // may lag
await client.query("pizza.shop", ["goblins", "store-0"]);       // fresh
const { value: job } = await client.mutate("pizza.claim", { tenant: "goblins", owner: "drone-1" });
if (job) await client.mutate("pizza.deliver", { tenant: "goblins", id: job.id, owner: job.owner, token: job.token, ...(job.history ? { history: job.history } : {}) });
```

Customer previews use `pizza.shop.local` by default in the benchmark; pass `--read-consistency fresh` to compare fresh reads. `pizza.world(null)` stays a fresh, group-wide audit method. Tenant keys provide logical separation in this unauthenticated demo; they are **not authorization**. A production application must bind allowed tenants to authenticated callers. Independent Raft groups can host disjoint tenant sets; the benchmark explicitly routes each complete tenant to its configured group, without automatic partition discovery.

## Durable scheduled business logic

Use the [TypeScript scheduler](sdk/scheduler.ts) to run a private mutation after a deadline. It stores named callbacks and JSON arguments as ordinary records. Scheduling a callback inside a mutation commits the timer and the business update together.

```ts
import { collection, define, mutation, query, v } from "@flower-js/sdk";
import { scheduler } from "@flower-js/sdk/scheduler";

const invoices = collection<{ total: number; status: "draft" | "ready" }>("invoices");
const id = v.string({ min: 1 });

const finalize = mutation("internal.invoice.finalize", { args: id }, (ctx, id) => {
  const invoice = ctx.get(invoices, id);
  if (invoice) ctx.set(invoices, id, { ...invoice, status: "ready" });
  return null;
});
const timers = scheduler("invoiceTimers", { finalize });

const update = mutation("internal.invoice.update", {
  args: v.object({ id, total: v.int({ min: 0 }) }),
}, (ctx, { id, total }) => {
  ctx.set(invoices, id, { total, status: "draft" });
  return timers.after(ctx, `finalize:${id}`, 5_000, "finalize", id);
});
const get = query("internal.invoice.get", { args: id }, (ctx, id) => ctx.get(invoices, id));

export default define({
  uses: [timers],
  http: { "invoice.update": update, "invoice.get": get },
});
```

Every update replaces the pending timer with the same ID. The example finalizes an invoice after five seconds without another update. Use a distinct ID for each update when every update needs its own later action. This is explicit application code in the mutation method, so derived functions remain pure.

A scheduler is a component: `uses: [timers]` declares its timer collection, indexed by `(state, dueAt)`, and contributes one maintenance task. `timers.after(ctx, id, delayMs, handler, args)` and `timers.at(ctx, id, epochMilliseconds, handler, args)` return the stored timer. Handler names and argument types are checked against the registry when compiling, and arguments against the handler's `args` schema when scheduling (`INVALID_ARGUMENT`). `cancel` removes a pending or failed timer, `get(ctx, id)` and `scan(ctx, { state? })` inspect timers in deadline order, and `retry(ctx, id, delayMs?)` requeues a failed timer (`TIMER_NOT_FAILED` otherwise). A timer records `state`, `handler`, `args`, `dueAt`, `attempts`, its last `error` as `{code, message, details?}`, and creation and update times. Handler aliases are separate from HTTP aliases, and handlers need no exposure. Captured closures are not serialized; arguments are JSON, and pending timers use the currently deployed handler code. Renaming a handler requires migrating pending records or keeping its old alias available; an unknown alias fails the timer with `SCHEDULER_HANDLER_MISSING`.

One due callback runs per maintenance transaction, selected through the `(state, dueAt)` index and ordered by deadline and ID. Its database changes and timer removal commit atomically through Raft. A callback may reschedule its own ID, including to implement recurring work. If it throws or exhausts its execution budget, the host discards all its writes and runs the private error handler against the original snapshot and time, which reselects the same timer. The scheduler records the failure and applies bounded retries with backoff, eventually retaining a failed timer for inspection. Other due timers and tasks remain eligible between retries.

The scheduler's third argument configures `{maxAttempts, retryDelayMs, maxRetryDelayMs}`. Defaults are three attempts, an initial 1,000 ms retry delay, and a 60,000 ms cap. Delays double after each failure and start when the failed attempt finishes. Replacing a timer or explicitly retrying a failed timer starts a new attempt budget. If the private error handler itself fails, neither patch commits; the error is logged and replacement code can be deployed.

Deadlines mean **not before**, using Flower's server time. Execution can be late during load, elections, or quorum loss. Maintenance wakes when the earliest timer is due, never starting two runs within 250 ms by default, and runs a bounded catch-up burst when callbacks remain due. Each callback has its own transaction; a burst stops at its serialized-batch budget or after an invocation takes it past the configured time window, 50 ms by default. Timer records survive restart and failover. Callback code can execute again before a successful commit, while the timer removal and database effects commit together once for that timer. For network calls or other external work, have the callback enqueue a job for the leased worker queue.

The complete [scheduling example](examples/scheduling.ts) includes delayed business updates, cancellation, retries, and timer inspection. TTL deletion can be just another scheduled callback; the expiration helper below additionally hides expired values before physical cleanup runs.

## Worker leases and expiring keys

These policies live in [TypeScript helpers](sdk/temporal.ts), built from ordinary records, methods and maintenance tasks. The [worker example](examples/workers.ts) exposes a queue's generated methods and an expiring cache, and [docs/job-worker.ts](docs/job-worker.ts) is a worker for it. Deploy it to a fresh cluster with `node sdk/cli.ts deploy examples/workers.ts`.

```ts
import { define, mutation, v } from "@flower-js/sdk";
import { expiringCollection, queue } from "@flower-js/sdk/temporal";

const jobs = queue<{ url: string }, { status: number }>("jobs", {
  lease: { defaultMs: 10_000, maxMs: 30_000 },
  retry: { maxAttempts: 5, initialDelayMs: 1_000, maxDelayMs: 60_000 },
  payload: v.object({ url: v.string({ pattern: /^https:\/\// }) }),
});
const sessions = expiringCollection("sessions", {
  expiration: { afterUpdateMs: 60_000 },
});

const fetchLater = mutation("internal.fetch", { args: v.object({ id: v.string({ min: 1 }), url: v.string() }) },
  (ctx, { id, url }) => jobs.enqueue(ctx, id, { url }, { delayMs: 1_000 }));

export default define({
  uses: [jobs, sessions],
  http: { "fetch.later": fetchLater, ...jobs.http("jobs") },
});
```

`queue()` returns a component holding its jobs collection (keys `[scope, id]`, five indexes), its line of waiting owners, the shared fencing counters and turn clocks, a lease-reclaim task and a task that clears lapsed places in line. Inside mutation methods:

- `jobs.enqueue(ctx, id, payload, { delayMs?, at?, replace?, priority?, group? })` creates a pending job, available now or later. An existing ID fails with `JOB_EXISTS` unless `replace: true` and that job is completed or failed. Lower `priority` goes first (default 0, strictly); within a priority, `group`s take turns, so each ready group's next job goes before any group's second.
- `jobs.claim(ctx, owner, { leaseMs? })` leases the next job (by priority, then turn, then how long it has waited) and returns `{scope, id, payload, owner, token, expiresAt, attempt, history?}`, or `null`. Jobs whose lease expired are claimable at once, before cleanup runs. Leases default to 30,000 ms and cannot exceed `lease.maxMs` (default 300,000; `LEASE_TOO_LONG`).
- `jobs.claimMany(ctx, owner, { max?, leaseMs?, waitMs?, admit? })` leases up to `max` jobs; a job `admit` refuses is deleted and the next taken. With `waitMs`, a claim that comes back short puts `owner` in the scope's line, and `jobs.ready(ctx, owner)` tells the first in line of new work `turnMs` (queue option, default 1,000) before anyone else, so one process wakes per job.
- `jobs.renew(ctx, lease, { leaseMs? })` extends the current lease and keeps its fencing token; `jobs.renewMany(ctx, leases, { leaseMs? })` extends several and returns each new expiry, or `null` for a lost lease.
- `jobs.complete(ctx, lease, result)`, `jobs.fail(ctx, lease, error, { retry?, delayMs? })` and `jobs.release(ctx, lease, { delayMs? })` require the current, unexpired lease (`LEASE_LOST` otherwise). `release` hands an unfinished job back, keeping its turn and attempt count and recording no error.
- `jobs.retry(ctx, id, { delayMs? })` requeues a failed job with a fresh attempt budget (`JOB_NOT_FAILED` otherwise). `cancel` deletes a job. `get`, `scan`, `ready` and `stats` report effective state; `stats(ctx, { countUpTo? })` returns `{ready, oldestReadyAt, nextAvailableAt, readyCount, leasedCount, delayedCount}`, each count stopping at `countUpTo` (default 100, at most 10,000).

Every claim counts an attempt, so an expired lease counts as a failed one. Under the default retry policy (five attempts, a 1,000 ms delay doubling to a 60,000 ms cap), `fail` returns the job to pending after that backoff, or after `delayMs`, until attempts run out or it passes `{ retry: false }`; a lease that expires on the last attempt fails the job with `LEASE_EXPIRED`. With `retry: false` in the queue options, `fail` is final, while expired leases still return jobs to pending. `payload` and `result` schemas reject invalid values with `INVALID_ARGUMENT`.

Pass a claim's `{id, owner, token, history?}` fields back unchanged; after retention initialization, `history` binds database and incarnation and rejects restored-history leases. Fencing tokens increase strictly per queue and scope and are retained in `$flower.fencing` independently of job deletion or replacement, so an old worker cannot finish a newer worker's claim. `jobs.scope(name)` returns the same API over one namespace of the shared collection; `scan` inspects only that scope. Completed jobs and valid leases never block claims. Scopes provide logical separation, not authentication.

`jobs.http(prefix, { methods?, scope?, access? })` generates public methods named `${prefix}.claim`, `.renew`, `.complete`, `.fail`, `.release`, `.get`, `.ready` and `.stats` by default; `enqueue` (with `delayMs`, `at`, `replace`, `priority` and `group`), `retry` and `cancel` exist only when listed in `methods`. `claim` takes `{ owner, leaseMs?, max?, waitMs? }` and returns the first job carrying the rest in `more`; `complete` and `fail` also take `next: { max?, leaseMs?, waitMs? }`, which claims the lease owner's next jobs in the same commit and returns them as the job's `next`; `renew` takes `{ leases, leaseMs? }`; `ready` takes `null` or `{ owner }`; `stats` takes `null` or `{ countUpTo }`. `scope: "argument"` adds a required `scope` argument to each method, and `scope: (ctx) => string` derives the scope from the caller, for example from `ctx.principal()?.tenant`; otherwise methods use the default scope. `access` applies to every generated method.

`runQueueWorker` from `@flower-js/sdk/worker` drives those methods:

```ts
import { FlowerClient } from "@flower-js/sdk";
import { runQueueWorker } from "@flower-js/sdk/worker";

const stop = new AbortController();
process.once("SIGTERM", () => stop.abort());
await runQueueWorker<{ url: string }, { status: number }>(new FlowerClient("http://127.0.0.1:7101"), {
  queue: "jobs",
  signal: stop.signal,
  concurrency: { max: 32 },
  wait: true,
  leaseMs: 10_000,
  work: async (job, signal) => {
    const response = await fetch(job.payload.url, { signal, headers: { "idempotency-key": job.id } });
    return { status: response.status };
  },
});
```

The worker is a pool. One subscription to `${queue}.ready` serves the whole process; once it fires, claims take jobs while there is room (`batch` per claim, one claim at a time until claims come back full), then go back to waiting when one comes back short. How many jobs run at once follows `concurrency`: a number fixes it, and `{ min, max, initial }` (default `{ min: 1, max: 16 }`) lets it grow while the queue holds more than the process runs and shrink once its event loop or memory falls behind (`processHealth`). With `wait: true` it waits in the queue's line, so a new job wakes one process rather than every idle one. With `chain: true` each report also claims the next jobs, as many as there is room for, in the same commit (it needs reports that take `next`, as `queue.http()` generates them), so a worker that stays busy spends one mutation per job. `work(job, signal, control)` gets a signal that aborts `marginMs` (default a fifth of `leaseMs`) before the lease ends, or when a renewal reports the lease lost; `control.throttle(ms, reason)` pauses claims and halves the limit when a service pushes back, and `control.idle()` stops a job that mostly waits (on a child process, say) from counting toward it. Every held lease renews in one call, each about every `(leaseMs − marginMs) / 3`. The worker then completes the job, or fails it with `{ message }` when `work` throws, leaving the retry decision to the queue. Its mutations retry transient errors with one request ID, and no attempt starts after the lease would end. Aborting `signal` stops claiming, lets held jobs finish (or, with `drainMs`, aborts and fails those still running after that long, and gives up on work that hasn't stopped 5 s later; with `release: true` too, hands them back to run again at once), and resolves; a non-transient claim error stops every claimer the same way and then rejects. `onEvent` reports `claimed`, `completed`, `failed`, `released`, `lost`, `unreported`, `limit` and `waiting`. Calling the methods directly works too:

```ts
import { FlowerClient } from "@flower-js/sdk";
import type workers from "./examples/workers.ts";

const client = new FlowerClient<typeof workers>("http://127.0.0.1:7101");
const { value: lease } = await client.mutate("jobs.claim", { owner: "worker-1", leaseMs: 10_000 });
if (lease) {
  // After doing the work, send the claim's identity back unchanged:
  await client.mutate("jobs.complete", {
    id: lease.id, owner: lease.owner, token: lease.token,
    ...(lease.history ? { history: lease.history } : {}),
    result: { processed: true },
  });
}
```

Check for a `null` claim before starting work. Use stable request IDs for retries of one invocation. A retried claim that took a job returns its original receipt and can already be expired; use a new request ID to acquire fresh work. Lease expiry cannot stop a worker process or undo external effects. Downstream services should enforce fencing tokens or accept idempotency keys when duplicate work would matter.

Expiring collections are components too. `expiringCollection(name, { expiration?, value? })` provides `set`, `get`, `entry`, `scan`, and `delete`, and contributes a task that deletes expired records in pages of 64. Reads hide a record at `ctx.now() >= expiresAt`, including inside reactive functions, before the task reclaims it. `entry` returns its value and `createdAt`, `updatedAt`, and `expiresAt` metadata. Updates preserve a live record's creation time; replacing an expired record starts a new lifetime. A `value` schema rejects invalid values with `INVALID_ARGUMENT`.

Each `set(ctx, key, value, expiration?)` uses its supplied policy or the collection default. Supported policies are `{afterCreationMs: n}`, `{afterUpdateMs: n}`, `{at: epochMilliseconds}`, and `null` for no expiry. The override applies to that write. Absolute deadlines let TypeScript implement other rules, such as the earlier of an idle timeout and a maximum lifetime. Internal code can access `.records` for raw envelopes; application methods should use the helpers when they want expiration filtering.

Expiry after access can be an ordinary mutation method that reads the live value and writes it back with an updated deadline. Policies remain application code; a read-only query never silently refreshes a lifetime. The helpers are also available through the package's `@flower-js/sdk/temporal` export.

## Maintenance tasks

Background work is a set of tasks. `task(name, { due, run, onError? })` declares one: `due(ctx)` returns the earliest time it has work, or `null`, and must depend only on data and time; `run(ctx)` performs one bounded unit in an ordinary mutation context and stays eligible while `due` remains in the past. Pass tasks to `define({ tasks })` or `component({ tasks })`. Schedulers, queues, expiring collections and `materialize` policies contribute their own.

```ts
import { collection, define, task } from "@flower-js/sdk";

const sessions = collection<{ user: string; expiresAt: number }>("sessions")
  .index("expiry", ["expiresAt"]);

const purge = task("sessions.purge", {
  due: (ctx) => ctx.range(sessions.by("expiry").range({ limit: 1 })).rows[0]?.value.expiresAt ?? null,
  run(ctx) {
    const { rows } = ctx.range(sessions.by("expiry").range({ lte: ctx.now(), limit: 64 }));
    for (const row of rows) ctx.delete(sessions, row.key);
    return { purged: rows.length };
  },
});

export default define({ collections: [sessions], tasks: [purge] });
```

`define` compiles every task into one private handler pair and registers it as the application's maintenance. On the leader, the handler evaluates each task's `due`, runs the task due earliest, and reports when the next task is due; the leader sleeps until then or until a write or leadership change, never starting two runs within 250 ms by default. A task that fails without `onError` backs off for 1 s, doubling to 60 s; its failure count, retry time and last failure live in the reserved collection `$flower.tasks` until its next success, and other tasks stay eligible meanwhile. A task with `onError(ctx, { error, failedAt })` handles its own failures instead, as the scheduler does per timer. `error` is the real failure `{code, message, details?}`: the task's `fail()` code, `COMPUTE_ERROR` for an exception without a code, `EVALUATION_BUDGET` for an exhausted budget, and `MAINTENANCE_FAILED` only for errors outside evaluation, such as a panic.

Maintenance handlers are not callable over HTTP. Their code and registration replicate with the bundle. A failed run rolls back; public methods and deployment remain usable. Maintenance can be delayed by load, election, or quorum loss, and must be safe to repeat. Idle runs do not advance revisions; actual changes can invalidate `expectedRevision`. Background commits do not accumulate client retry receipts.

While another task is already due, the handler's result carries `{ $flower: { continue: true } }`, requesting another invocation against its successful staged patch. The host stops after the first invocation that takes the configured burst past its time budget (50 ms by default), when the serialized Raft batch would exceed its byte budget, or on an idle patch. There is no independent callback-count cap. Each invocation gets a fresh time, resource budget, rollback boundary, and application revision. Successful patches share a durable group commit; a later callback failure still allows the preceding successful patches to commit.

`ctx.now()` is the small host primitive behind these helpers: a fixed epoch-millisecond timestamp for the entire evaluation, including all derived previews. Mutations commit it with their changes, and future mutations never move behind committed time. Queries use a fresh timestamp and temporarily refresh time-dependent derived values without writing. Reactive clock dependencies update on mutations and maintenance; source-only applications need no background handler. `ctx.clock()` reads the same time for code that also calls `ctx.changesAt(time)` for every later instant at which its result can change, so watches wake exactly then; a query that reads `ctx.now()` is rechecked every 250 ms instead. The SDK's helpers use `ctx.clock()`.

Time comes from the serving node, bounded below by committed time and the process's monotonic clock floor. Keep node clocks synchronized for accurate real-time durations: skew can expire a lease early or delay expiry, and query timestamps can move back between serving nodes when an earlier query's time was never committed. Fresh-read guarantees concern database state, not synchronized clocks. Deadlines are checked at invocation time, so slow evaluation or commitment may consume the remaining lease before the response arrives. Claims return an absolute deadline; successful acquisition is not a guarantee of a full duration remaining at receipt.

## Values computed outside the database

```ts
import { collection, define, external, mutation, query, v } from "@flower-js/sdk";

const documents = collection("documents", v.object({ text: v.string({ max: 100_000 }) }));
const digest = external("digest", {
  input: (ctx, id: string) => {
    const document = ctx.get(documents, id);
    return document && { recipe: "sha256-v1", text: document.text };
  },
  result: v.string({ pattern: /^[0-9a-f]{64}$/ }),
  each: documents,
});
const get = query("document.get", { args: v.string() }, (ctx, id) => ({
  document: ctx.get(documents, id),
  digest: ctx.get(digest, id), // null, { status: "pending" } or { status: "ready", value }
}));
const put = mutation("document.put", { args: v.object({ id: v.string({ min: 1 }), text: v.string() }) }, (ctx, { id, text }) => {
  ctx.set(documents, id, { text });
  return null;
});

export default define({
  uses: [digest],
  http: { "document.get": get, "document.put": put, ...digest.http("digest") },
});
```

`external(name, { input, result?, each?, lease? })` declares a derived value whose result comes from workers. `input(ctx, args)` returns everything the result depends on, or `null` when there is nothing to compute. `ctx.get(digest, id)` returns `null`, `{ status: "pending" }`, or `{ status: "ready", value }` once a result computed from the current input is stored; a changed input makes the old result pending at once. `publish(ctx, { args, key, value })` stores a result only while `key` still names the current input and returns `{ accepted }`; racing publications for one input keep the first value. `pending(ctx, args)` returns the work for one key. With `each: collection`, writes to that collection's rows mark their keys, and `next(ctx, { limit, shard })` lists pending work oldest first, optionally for one hash shard `[index, count]`. An input that reads other collections still updates `ctx.get` and `pending`, but worker pools notice the change only when the row itself is written. Pools can instead lease keys: `claim(ctx, owner, { limit, leaseMs })` leases the longest-waiting keys so other claims skip them until the lease ends, `renew(ctx, leases)` extends several at once, and `release(ctx, lease, { delayMs })` hands one back, claimable again after the delay. A lease is `{ args, key, owner, attempt }`; `attempt` counts claims of the current input. Leases spread work but don't guard `publish`, and an edit that changes a leased key's input voids its lease. `lease: { defaultMs, maxMs }` bounds lease lengths (30 s and 5 minutes by default). `digest.http("digest")` generates `digest.pending`, `digest.publish` and, with `each`, `digest.next`, `digest.claim`, `digest.renew`, `digest.release`, `digest.ready` and `digest.stats`; its `access` option applies to all of them.

`reconcile` runs the worker side:

```ts
import { FlowerClient } from "@flower-js/sdk";
import { reconcile } from "@flower-js/sdk/worker";
import { createHash } from "node:crypto";

const stop = new AbortController();
await reconcile<string, { recipe: string; text: string }, string>(new FlowerClient("http://127.0.0.1:7101"), {
  external: "digest",
  signal: stop.signal,
  concurrency: 4,
  compute: async (input) => createHash("sha256").update(input.text).digest("hex"),
});
```

With `args` it keeps one key current; otherwise it drains `next` in batches (`batch`, default 16) with `concurrency` parallel computations, optionally for one `shard`. With `lease: true` it claims keys instead, at most one per free computation, renews all of its leases in one call, and hands them back when stopped, so any number of processes split the work and take over from one that dies; there `concurrency` may also be bounds `{ min, max, initial }` that follow the backlog and the process's load, like `runQueueWorker`'s (`health`, `adjustEveryMs`, `limit` events). It waits through subscriptions and publishes with retries. After a failed computation, or a publication the database rejects (for example by the `result` schema), it backs off and leaves the key pending for the next round. `compute` may run more than once for the same input. The [worker guide](https://flower.xmit.dev/guide/workers.html) covers pools, sharding and high availability.

## Test applications in process

```ts
import assert from "node:assert/strict";
import { test } from "node:test";
import { FlowerError } from "@flower-js/sdk";
import { testDatabase } from "@flower-js/sdk/testing";
import app from "./examples/scheduling.ts";

test("an edit restarts the publication delay", async () => {
  const db = await testDatabase(app);
  db.mutate("documents.update", { id: "a", text: "draft", publishAfterMs: 1_000 });
  db.advance(600);
  db.mutate("documents.update", { id: "a", text: "final", publishAfterMs: 1_000 });
  db.advance(600);
  assert.equal(db.query("documents.get", "a")?.status, "draft");
  db.advance(400);
  assert.equal(db.query("documents.get", "a")?.status, "published");
  assert.throws(() => db.mutate("documents.retryPublication", { id: "a" }),
    (error) => error instanceof FlowerError && error.failure?.code === "TIMER_NOT_FAILED");
});
```

`testDatabase(app, { now?, credentials?, partitions? })` runs an application on Flower's reference engine inside Node. `query`, `mutate` and `call` are synchronous, return values, and throw `FlowerError` shaped like the HTTP errors, including authorization hook denials and request-ID receipts. Time moves only when the test says so: `db.now` starts at 1,000,000, `advance(ms)` moves it and then runs due maintenance like the leader, and `maintain()` runs due maintenance alone. `db.data` and `db.revision` expose raw state. `db.client` is a typed `FlowerClient` over an in-process `fetch`, with `watch` and `subscribe`. `db.partition(name)` addresses a partition listed in `partitions`; transactions across them commit or abort together. Passing a module path instead of the module bundles it and runs it in a separate `node:vm` context. Index reads on an index the application does not declare fail with `UNDECLARED_INDEX`, since the server would answer them by scanning the collection.

This is not the server: there is no Raft, no QuickJS, Wasm or server budget, and no native crypto, so `nacl`, `jwt`, `sha256`, `webauthn`, managed keys and `jwtBearer` fail. Module globals persist between calls, unlike the server's pristine per-callback images. Retry retention and sessions are not simulated, and `ctx.history()` is null. Use the end-to-end tests below for server behavior.

## Reactive semantics

A materialized root maintains its transitive derived dependencies. Instance identity is `(definition name, canonical JSON argument)`; omitted arguments mean `null`. Removing a root collects derived instances that are no longer reachable. Querying an unmaterialized derived instance evaluates it temporarily without persisting it.

`derive(name, fn, { materialize })` declares which instances stay materialized. `"always"` materializes the argless instance through the `materialize` maintenance task, so it appears at the first maintenance run after deployment. `{ each: collection }` keeps one instance per row, with the row key as its argument: a trigger materializes the instance in the mutation that creates the row and unmaterializes it in the one that deletes it, and the same task backfills rows that already existed, 64 per run. Its progress markers live in the reserved collection `$flower.materialized`. `ctx.materialize` and `ctx.unmaterialize` remain available for other policies.

Dependencies follow actual reads, including missing records. Scans without options depend on their entire collection. Equality queries depend on the matching bucket, declared or not: only a row entering, leaving or changing within it re-runs the derivation. A scan or range with bounds, `offset` or `limit` depends only on the entries in its range up to the last row it examined, and on the values of the rows it returned: a write past that row, outside the range, or to a row the offset skipped re-runs nothing. Queries on declared durable indexes depend on the matching equality bucket and rows, including inserts into a previously empty bucket. Successful evaluations replace old dependencies. Failed evaluations retain old and newly observed dependencies to allow recovery.

Ordinary derived exceptions become stored error outcomes and propagate to dependents. Stored outcomes keep the failure's code and message, not its details. A method may catch such an error intentionally. Cycles and shared transaction-budget failures abort the proposal even if application code tries to catch them. Failed computed values are not served as their previous successful result.

Unchanged source values do not invalidate. For a single changed dependency, unchanged derived outcomes stop downstream propagation; multiple dirty branches conservatively retain the full dependency order. Explicit aggregates update retained accumulators from changed rows; see [indexes and reducers](INDEXES.md) for their callback contract and rebuild behavior.

Functions return finite JSON values. There is no network, filesystem, process, timer or ambient date access. Mutation-only crypto entropy comes from the native OS CSPRNG; it is separate from deterministic business PRNG state. Queries and derived values cannot request fresh crypto randomness. Time is supplied through `ctx.now()`; other external facts enter through mutation methods. Promises, nonfinite numbers, sparse arrays, cyclic objects, and other unsupported values are rejected instead of silently serialized.

Deployment uses a SHA-256-addressed JavaScript bundle. Module initialization, the HTTP allowlist, and the maintenance and authorization registrations are checked before activation; changed bundles recompute live instances. Code, registrations, input changes, dependencies, and derived outcomes publish atomically. These are included in durable state and snapshots. A failed deployment leaves the previous code and registrations active. Deployment does not run the old maintenance handler, so a broken handler can be replaced.

Applications register components, definitions, indexed collections, tasks, triggers, keys, authentication and public aliases through `define({ uses, collections, definitions, tasks, triggers, keys, http, auth })`. No methods are exposed without an explicit HTTP allowlist. Flower is unreleased; APIs and stored/wire formats may change without backward compatibility or a migration path.

## Replication

The leader evaluates mutations outside the async consensus runtime, then proposes a concrete batch containing the expected revision, changes, and method result. OpenRaft orders the batches; followers apply replicated changes without reexecuting those mutations. Every replica can execute query methods against its applied committed state. Compare-and-set validation prevents obsolete computations from replacing newer state.

Redb stores votes, logs, applied indexes, application state, receipts, and snapshot checkpoint metadata. Mutation acknowledgements wait for a quorum to durably retain the Raft log entry and for local atomic application. Votes and log appends use immediate durability. Application checkpoints can lag after a crash; committed log entries reconstruct the missing records and receipts without reexecuting TypeScript. Snapshots include code, its HTTP allowlist, and dependency metadata, allowing recovered nodes to become leaders and evaluate future methods.

Application records and request receipts use separate redb tables. Each apply writes changed records, receipts, revision, membership, and applied-index metadata in one atomic redb transaction with deferred durability (`Durability::None`), then publishes the committed snapshot. It does not perform another fsync for every apply. A subsequent immediate log, vote, snapshot, or purge transaction also makes prior apply checkpoints durable. Raft snapshot checkpoints retain immediate durability but write only metadata: the existing application tables are the recovery source. Full transfer images are encoded lazily into temporary files when a peer reads them, preserving a consistent captured state without repeatedly storing another copy. Snapshot installation and log purging retain immediate durability. This reuses the existing redb log and transaction engine; it does not introduce a separate WAL or background checkpoint scheduler.

One process can also host replicas of several Raft groups, each with its own listener, node ID and membership:

```sh
./target/release/flower --data .flower/host1 \
  --replica orders,1,127.0.0.1:7101 --replica billing,1,127.0.0.1:7201
```

Initialize and address each replica like a separate node. They share one redb database in `--data`, under per-replica table prefixes, with snapshot transfers in `DIR/NAME/`. Their log appends and applied-state writes commit together in batches: one transaction, flushed once when anything in it must be durable. Groups colocated on a disk therefore share its fsyncs instead of contending for them, and any durable batch also persists the others' deferred writes. A replica queues each append without waiting for its earlier ones to commit, since batches commit in order, so a leader keeps publishing entries for replication while a batch flushes. Votes, truncation, purges and snapshots keep their own transactions, which first wait for that replica's queued writes. Stopping the process stops all of its replicas, like losing that machine. A single-replica directory cannot be reopened as a host database, or the reverse.

At startup, a durable log beyond the recovered applied position arms a serving barrier shared by all logical partitions on that node. A recovering leader first quorum-commits and applies a fresh internal recovery entry; followers obtain its read fence and wait for local application. This confirms the chosen log prefix even when the same leader and term resume after a crash, without assuming every durable tail entry was committed. The recovery entry changes no application records, revisions or receipts. Until recovery succeeds, even replica-local application reads can be unavailable; peer traffic and operator diagnostics remain available. Once released, replica-local reads again need no per-read quorum.

Opening supported data migrates checkpoint metadata forward and marks the prior format unreadable by older binaries. Downgrading that directory is unsupported. The state-machine compatibility contract rejects mixed pre-graph-generation and current nodes; use a coordinated upgrade rather than treating this change as a compatible rolling release. Unsupported older redb formats remain rejected. Other compatible builds can follow the [rolling-upgrade procedure](MEMBERSHIP.md).

Concurrent mutations enter a bounded queue and share durable Raft commits. By default, the writer adapts group size to measured preparation cost, durable commit time, and queued work. Idle requests start immediately. Preparation has a 50 ms maximum window, and serialized commands including the batch envelope have a 32 MiB allowance. Adaptive mode has no fixed command-count cap; admission capacity bounds retained requests, and an explicit batch-size setting can impose a cap. These are [operator settings](bench/LIMITS.md); fixed mode is available for comparisons. Each method sees preceding staged writes and retains its own revision, retry receipt, and rollback boundary. Replies wait for durable quorum commitment and local application of the group. A full queue returns `503`; clients can retry the same request ID. Under sustained load, the writer overlaps preparation of the next group with durable quorum commitment and local application of the current group. It coalesces later arrivals while an existing predecessor commits, without adding an idle batching delay. In adaptive mode, the learned count and time targets do not close a successor while its predecessor is still committing; it is submitted as soon as that predecessor completes. Its private snapshot shares immutable data and retry history; queries and SSE continue to see only committed state. A failed or uncertain commit discards the prepared successor, returns retryable errors, and starts the next window from a fresh quorum read. Within a group, bounded preparation waves can run concurrently. The ordered lane validates read/negative/index-range dependencies, write bases, code/policy and reactive dependency shape against preceding staged changes; it reauthorizes the complete principal and rechecks retry/CAS/transaction-lock rules. Invalid candidates rerun serially. Every wave shares one fixed time, and discarded entropy/results are never published. Hot conflicts reduce the speculative width and add serial cooldown. `FLOWER_WRITER_PREPARATION_WORKERS` defaults to the shared preparation-worker count; set it to 1 for serial preparation. Shared admission reserves active memory, and candidate output stays byte-accounted through its durable group. Deployments drain the pipeline, and maintenance gets a turn between bounded 250 ms writer windows (checked between methods, so a slow callback can exceed the window).

Initialization rejects duplicate peer addresses. Raft RPCs check the target and responding node IDs, so two addresses pointing at the same process cannot count as two voters.

Public queries are linearizable by default: a replica obtains the leader's quorum-backed applied-index fence, waits for local application, then captures a snapshot. After the startup recovery barrier, explicit replica-local queries skip per-read fences and can return older state and code. Query execution has no durable effects and never sees the writer's speculative successor batch. Every acknowledged mutation publishes its final source records and materialized results at one revision.

### Upgrading existing storage

Current builds use redb 4.3. Legacy redb v2 data files are unsupported.

## Control plane

`POST /admin/deploy` accepts a bundle and request ID. `/raft/initialize` and `/raft/metrics` require `Authorization: Bearer <operator-token>`; internal peer RPCs use the separately configurable peer credential. Deployment and cluster administration are privileged operations separate from application methods; in the SDK they belong to `FlowerAdmin`, not `FlowerClient`. `/health` checks process availability without reading database data or establishing quorum.

Native TLS protects HTTP/1.1 and HTTP/2 when configured; cleartext h2c remains the default for local experiments. Set a distinct `FLOWER_PEER_TOKEN` to separate internal traffic from `FLOWER_ADMIN_TOKEN` operator endpoints (omission falls back to the operator token). Deployed `define({ auth })` code establishes end-user principals; Flower does not supply an identity provider. Trusted peers can replicate state and forward privileged work, so route separation is not a sandbox for compromised peers. See [TLS.md](TLS.md) for the exact trust and rotation contract. The embedded runtime has not undergone a security audit.

## Tests and boundaries

Goblin Pizza Express is a complete TypeScript example: durable oven timers bake
pizzas, delivery drones claim leased work, and reactive kitchen summaries feed a
leaderboard. Its benchmark runs a fresh local Raft cluster, drives concurrent
customers and drones, replays requests, abandons leases, and independently audits
inventory, money, jobs, timers, and derived results.

```sh
cargo build --release --bin flower --bin flower-bench-driver
npm run bench
npm run bench:stress     # More concurrency plus a leader crash during load
npm run bench -- --help
```

See [the example](examples/goblin-pizza-ts/goblin-pizza.ts) and [benchmark guide](bench/README.md)
for workload controls and measurement limits. Reports default to
`bench/results/latest.json` and a self-contained HTML report with charts at
`bench/results/latest.html`. Use `--baseline earlier.json` for a before/after
comparison. Any invariant violation, unexpected request failure,
or incomplete drain makes the command fail.

```sh
npm run check          # Types, SDK, dependency engine and randomized differential checks
cargo test            # QuickJS, allocation budgets, storage conformance and real Raft recovery
cargo build
node tests/e2e.mjs     # Methods → HTTP → QuickJS → three processes, including SIGKILL failover
node tests/e2e-http2.mjs # Multiplexed HTTP/2 methods, HTTP/1 compatibility, and failover
node tests/e2e-watch.mjs # SSE deltas, clocks, revocation, cancellation, HTTP/2 and reconnect
```

One leader per Raft group orders mutation commits and eagerly maintains affected values; safely validated preparation can run concurrently. Evaluations share immutable Rust record trees and copy only changed paths; normal storage applies persist only changed records. Quorum reads use a separately published committed snapshot, so a later storage transaction does not hold their snapshot lock. Records and receipts are served from redb read snapshots; memory holds writes still queued for disk, a bounded cache of parsed values and redb's page cache. Retry retention is opt-in: replicated epoch floors, which the leader can advance on a schedule, and explicit session acknowledgements collect results without reviving scoped intents, while a caller's own request ID deduplicates until the floor passes the epoch it ran in; uninitialized databases retain receipts indefinitely. Distributed transaction detail can be collected after durable participant closure floors; admissible aborted retry IDs and incomplete transactions block closure. See [RETENTION.md](RETENTION.md) for the exact contract and restore tradeoffs. Cluster nodes must share the compatibility contract, and restarted binaries must support their local data format.

Evaluation uses configurable deadlines and memory/byte budgets. Defaults are ten seconds, 128 MiB of aggregate live guest linear memory, 128 MiB of estimated Rust transaction allocation, 2 MiB of source, 16 MiB of evaluation input/output, and an 8 MiB HTTP body. For undeclared indexes, an optional temporary equality-index cache retains up to 16 MiB and falls back to scanning when full; declared indexes are durable records. These are not process RSS caps. Genuine recursive-stack and wasm32 representation bounds remain; see the complete [limits audit and configuration reference](bench/LIMITS.md).

Each node defaults to one query worker per available CPU, independently of its ordered writer lane. Fresh reads on every replica batch concurrent quorum proofs, sealing each cohort before the proof begins; a later arrival always needs a later proof. SSE uses the same declared consistency and can be distributed across replicas. HTTP/2 advertises no concurrent-stream ceiling by default; operators can configure one.

The server embeds a [vendored QuickJS-NG guest](vendor/quickjs-ng) built from pinned upstream sources. Its only Wasm imports are Flower's database and crypto bridges: it has no WASI runtime, filesystem, networking or ambient clock. Mutation-only crypto entropy is supplied through the native capability bridge. A small C bridge binds arguments, loads optional bytecode, and invokes the callback in one Wasm entry. Normal Rust builds embed the checked-in guest and need no guest cross-compiler or WASI SDK.

The server shares a Wasmtime engine, compiled modules, linked imports, pristine memory images, and pooled instance allocations. Evaluations find the deployed bundle's image by the version of the record that stores it, so the record is read, parsed and hashed once per version, not once per evaluation. Every callback receives the same isolated logical state, either in a new Store or in a completely restored resident instance. A 4 KiB input allocation reserved in each image avoids an extra guest allocator call for small invocations; larger inputs allocate normally. Input bytes reset between callbacks and count toward the usual memory budget. Bundles default to static initialization: module code, including `define()` and component construction, runs once per prepared bundle image on each node rather than per callback, without invocation bindings, database access or entropy. Every callback then starts from that initialized snapshot, which Wasmtime maps copy-on-write from a file where the host supports it. The bundle compiles in an instance of its own, so the snapshot holds the initialized heap without the parser's garbage. Initialized heaps above 32 MiB, or images the 96 MiB image cache can't hold, instead restore the shared base QuickJS sandbox and load cached bytecode, rerunning module code in each callback; the server logs a warning when a static bundle falls back. Per-invocation initialization is an explicit opt-out:

```sh
node sdk/cli.ts build examples/orders.ts orders.flower.json --initialization per-invocation
node sdk/cli.ts deploy orders.flower.json
# Or build and deploy in one step:
node sdk/cli.ts deploy examples/orders.ts --initialization per-invocation
```

The equivalent SDK option is `buildBundle(path, { initialization: "per-invocation" })`; it restores the base sandbox and reruns module initialization for every callback. In both modes methods receive all request information through their context and arguments. The mode is part of the hashed bundle. Closures, globals, prototypes, and guest memory reset to the pristine image for every callback. Before capturing a base or initialized application image, the host collects unreachable QuickJS cycles and resets the collection threshold to live allocated bytes plus 50%, its normal post-collection policy. Automatic collection and execution/memory limits remain enabled. Frozen method and derived context objects are constructed during trusted setup, before application initialization; overriding `Object.freeze` in application code does not intercept that construction. Cached application images are bounded; heap mutations never carry into another callback. The original JavaScript coordinator remains a test-only differential oracle, executed through the same vendored Wasm engine. No native JavaScript engine is linked.

Idle instances return to one process-wide pool per image, and any thread can check one out. Before reuse, linear memory and every mutable numeric Wasm global are restored, host callbacks and authorization/key caches are replaced, and the next invocation’s memory budget is charged anew. Linux and macOS copy only pages an instance may have written: protected pages are untouched, and hot pages are copied from the pristine image after each callback. Linux 6.7+ lets the kernel record writes through userfaultfd where permitted, so each reset copies exactly what the callback wrote plus frequently written pages; elsewhere a first write faults once. New instances start with their image's learned hot pages. Other platforms, or `FLOWER_WASM_DIRTY_PAGES=0`, use full-memory copies. Traps, growth and failed evaluations discard the instance; idle instances are released after 30 seconds, or earlier when the budget needs room. `FLOWER_WASM_RECYCLE=0` disables resident reuse. `FLOWER_WASM_RECYCLE_BYTES` bounds idle linear memory per process (96 MiB by default); zero or an image larger than the allowance bypasses retention without rejecting the callback. This trades retained memory and pool slots for fewer allocations and page faults; see the [resource limits](bench/LIMITS.md).

Successful clock-independent query results can be reused for identical method arguments and authenticated principal across unrelated committed revisions. Every request still checks the HTTP allowlist and its declared consistency; fresh reads establish a quorum fence even on a cache hit. Identical concurrent cacheable reads share their first evaluation. Calls to `ctx.now()`, including through computed dependencies, disable this optimization; so does any stored clock dependency. Each hit validates a dependency certificate against its selected snapshot: present/missing records, collection and index membership, derived outcomes, code, schema, and managed policy. Certificates retain weak allocation identities rather than old JSON payloads. Matching index rows track value changes while unrelated equality buckets can remain cached. Scans of a declared index check only the entries in the window their result came from, and key-ordered scans only whether their collection gained or lost a key. Each logical database retains up to `FLOWER_QUERY_CACHE_BYTES` (default 16 MiB) of estimated result/key/certificate data; `FLOWER_QUERY_FLIGHT_BYTES` (default 512 KiB) bounds the active shared-evaluation registry. Zero disables either tier, and there is no entry-count ceiling. Certificates are process-local optimization metadata, rebuilt from immutable applied state after restart or snapshot installation; they neither persist nor replace Raft durability or the selected read fence.

Public HTTP cache hits use a bounded probe without waiting for heavy preparation. `FLOWER_QUERY_WORKERS` bounds concurrent probes and authorization callbacks; heavy query evaluation instead shares `FLOWER_PREPARATION_WORKERS` with writers and watches. Probes try a slot immediately, reserve retained input/key bytes, and keep all current registry, policy, transaction and certificate checks. A miss or busy probe releases its temporary snapshot before entering normal fair admission and capturing a new one. Applications with authorization hooks or managed keys keep the full admitted path; SSE behavior is unchanged. The encoded response reserves shared user bytes through transport, even if its cache entry is evicted, and retains no snapshot or evaluation slot. See the [resource budget reference](bench/LIMITS.md) for the memory and fairness tradeoffs.

Reactive invalidation discovers potentially affected cells, then skips a parent callback when its only changed derived dependency produces the same outcome. Dependency changes still update graph edges, cycle checks, and collection. Parents with several potentially changed branches rerun in application read order, so an obsolete branch cannot introduce a spurious cycle. This reduces callback work; it does not eliminate the initial invalidation walk.

Durable equality indexes and incremental aggregates, membership changes and compatible rolling upgrades, and cross-group transactions are implemented:

- [Indexes and reducers](INDEXES.md): declare collection indexes; update retained totals using row deltas.
- [Membership and rolling operation](MEMBERSHIP.md): catch up learners, change voters through joint consensus, and restart compatible builds one member at a time.
- [Cross-group transactions](TRANSACTIONS.md): code-owned plans, durable two-phase commit, and recovery after failure.

Distributed preparation blocks fresh reads and writes in each participating logical partition (or default group namespace) until its durable decision is applied; unrelated named partitions continue serving, and opt-in replica-local reads may observe older state. Direct deployment supports optimistic preparation and an explicit blocking fallback. The [staged deployment API](INDEXES.md#resumable-staged-deployment) durably backfills indexes and rebuilds materialized roots in separate pages while source mutations maintain both graph generations, then atomically activates code/policy/indexes and the prepared graph. Each root dependency closure and clock/key refresh still obeys evaluation budgets. Ordinary derived functions reevaluate when invalidated, with unchanged intermediate outcomes stopping eligible downstream work. External actions remain outside database callbacks.

The [architecture review](bench/ARCHITECTURE.md) explains the remaining boundaries: independent groups, shared tenant costs, read-cache invalidation, cross-group coordination, and durable-history growth.

Publish the first npm package manually, then configure its trusted publisher; subsequent version tags publish through OIDC automatically, with no enable flag or token secret. See [RELEASING.md](RELEASING.md) for the exact setup.

## License

Flower is [MIT licensed](LICENSE-MIT). Bundled third-party code retains its own licenses and notices.

## Movable partitions and group resizing

The [Resize Raft groups guide](https://flower.xmit.dev/operate/groups.html) walks through growing from one group to two, waiting for rebalancing, and draining and removing a group.

A named partition is a complete logical database: application bundle, source and derived state, indexes, timers, lease counters, revision, and retry receipts. Several partitions can share a physical Raft group and execute independent writer preparation. Moving a partition briefly pauses that database; unrelated partitions continue, subject to shared CPU, disk and Raft contention. Existing tenant keys in the default database are not automatically extracted into partitions.

Configure participating servers with `FLOWER_GROUP`, `FLOWER_CATALOG_GROUP`, and `FLOWER_GROUPS` (bootstrap host:port peers for the local group and catalog). Provision and initialize physical groups separately, then register them through any configured gateway:

```ts
import { FlowerAdmin, FlowerClient } from "@flower-js/sdk/client";
import { buildBundle } from "@flower-js/sdk/bundle";
import type app from "./app.ts";

const cluster = new FlowerAdmin("http://catalog-1:7101", {
  adminToken: process.env.FLOWER_ADMIN_TOKEN,
});
await cluster.registerGroup({ id: "west", addresses: ["west-1:7101", "west-2:7101", "west-3:7101"] });
await cluster.createPartition("tenant-a", "west", { requestId: "create-a" });
await cluster.waitForPartition("tenant-a");
await cluster.partition("tenant-a").deploy(await buildBundle("app.ts"), { requestId: "deploy-a" });
const tenant = new FlowerClient<typeof app>("http://catalog-1:7101").partition("tenant-a");
// After registering another initialized group named east:
await cluster.resize(["west", "east"], { requestId: "grow-to-two" });
console.log(await cluster.layout());
```

`FlowerAdmin.partition(name)` scopes operator calls, and `FlowerClient.partition(name)` scopes method calls, to the stable `/partitions/{name}` URL. With an authorization hook, a partition admits only principals whose tenant equals its name.

`resize` durably balances partition counts, moving one partition at a time. It neither starts servers nor balances bytes or measured CPU. Live pre-copy, source freeze, final difference transfer, catalog cutover, destination activation and source retirement are recoverable and idempotent. The source keeps serving while its durable base is copied. After a brief freeze it sends changed records and receipts, including deletions; a large difference falls back to a full frozen image. Migration rolls forward after it starts; unavailable required groups can prolong the frozen phase. An interrupted client does not cancel the work. Logical-partition transaction methods are supported; freeze waits for unresolved participants and coordinators while completion remains available.

Active routes are cached for `FLOWER_ROUTE_CACHE_MS` (default 1,000 ms). Expired routes require the catalog; a catalog outage then affects named calls and watches. Native epoch/status checks independently fence writes and fresh reads. Streams terminate on ownership changes; reconnect at the stable partition URL for a fresh snapshot. Migration retains a durable base until retirement, and export caches the complete encoded base, difference, or fallback image in memory despite chunked transport. Optional `FLOWER_PARTITION_BASE_MAX_BYTES` bounds base admission and `FLOWER_PARTITION_TAIL_MAX_BYTES` chooses when to fall back to a full frozen transfer; neither is set by default. Partition names are logical boundaries, not authentication. The [complete SDK and operations reference](https://flower.xmit.dev/reference/partitions.html#partitions) documents every method, phase, default and tradeoff.
