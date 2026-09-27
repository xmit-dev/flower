# Indexes that grow, totals that keep up

Declare indexes in TypeScript and include their collections in the application. Flower stores the index entries in Raft alongside the rows. Queries then visit the matching entries instead of scanning the collection.

```ts
import { aggregate, collection, define, mutation, query, v } from "@flower-js/sdk";

const orders = collection("orders", v.object({ shop: v.string({ min: 1 }), cents: v.int() }))
  .index("byShop", ["shop"]);

const total = aggregate("shop.total", {
  source: orders,
  index: "byShop",
  initial: () => 0,
  add: (sum, row) => sum + row.cents,
  remove: (sum, row) => sum - row.cents,
});

const save = mutation("orders.save", {
  args: v.object({ id: v.string({ min: 1 }), shop: v.string({ min: 1 }), cents: v.int() }),
}, (ctx, { id, shop, cents }) => {
  // Validate business rules here; use define({ auth }) and access for caller admission.
  ctx.set(orders, id, { shop, cents });
  ctx.materialize(total, shop);
  return ctx.get(total, shop);
});

const read = query("shop.read", { args: v.string() }, (ctx, shop) => ({
  total: ctx.get(total, shop),
  orders: ctx.query(orders.by("byShop").eq(shop)),
}));

export default define({
  collections: [orders],
  definitions: [total],
  http: { save, read },
});
```

Only those HTTP methods are public. The same `read` method can be watched over SSE. Materializing `total` retains its accumulator; a materialized derived value that reads it also retains it. An unmaterialized aggregate computed only for a query can be rebuilt from matching rows for that query; that query's cached result and watches then depend on those rows, so a row changing within the group invalidates it, not only a row joining or leaving.

`.index(name, fields)` returns a new collection reference; keep and pass the one that carries the index. Index names and fields are checked against the record type, and `orders.by("byShop").eq(shop)` takes a value of the `shop` field's type.

## Equality and dependencies

A single-field index takes a JSON equality value. A composite index such as `.index("shopState", ["shop", "state"])` takes `.eq([shop, state])`. Values use Flower's canonical JSON equality; object property order does not affect a match. A missing field does not match `null`. Query results retain the existing source-key ordering, including Unicode keys.

Methods see their pending writes when querying an index. Derived queries depend on the equality bucket and the matching rows. An insertion into a previously empty bucket invalidates its readers; changes in unrelated buckets do not. Ordinary `derive` callbacks still rerun when their dependencies change.

The declaration matters: a collection reaches the application's durable index schema through `define({ collections })`, a component in `uses` (schedulers, queues and expiring collections declare theirs), an aggregate's `source`, or a trigger's or `materialize: { each }` collection. A collection that reaches none of these keeps the existing scan-based query behavior: each query still reads the whole collection, but its derivations still depend only on the matching bucket. One collection name must always carry the same indexes; conflicting declarations fail `define`. Indexes support JSON equality and ordered scalar ranges. They do not enforce uniqueness constraints or arbitrary projections.

## Ordered scans, ranges, and continuations

`ctx.scan(collection, options)` returns `{key,value}` rows in source-key order by default. Select a declared index name with `index` to walk its ordered tuples instead:

```ts
const orders = collection<{ shop: string; createdAt: number }>("orders")
  .index("created", ["shop", "createdAt"]);
const recent = query("orders.recent", { args: v.string() }, (ctx, shop) =>
  ctx.scan(orders, {
    index: "created", prefix: [shop], gte: 0,
    reverse: true, offset: 20, limit: 10,
  }));
// Include orders in define({ collections: [orders], http: { recent } }).
```

Scan constraints use the same `prefix`, `gt`/`gte`, and `lt`/`lte` rules as ranges below. Matching rows are ordered, optionally reversed, then skipped by `offset` and capped by `limit`. Both counts must be nonnegative safe integers; `offset` defaults to zero, an omitted `limit` returns all remaining matches, and `limit: 0` returns no rows. Without `index`, bounds must be strings and apply to source keys; `prefix` is either empty or a single exact source key. Unknown options and index names are rejected. An index name refers to the collection reference's `.index()` declarations; declaring the collection in the application enables persistent index seeks.

Collections with typed keys, `.key(v.tuple([v.string(), v.string()]))` for example, store each key as canonical JSON and return decoded keys from `scan` and `range`. Without an index, their `prefix` lists leading tuple components, so `ctx.scan(lines, { prefix: [orderId] })` returns that order's rows; source-key bounds fail with `INVALID_SCAN`, because canonical JSON text does not order tuple components by value. Declare an index for ordered access to key components.

Scans see pending mutation writes. Ordered scans share the scalar ordering and conservative index dependencies described below; source-key scans depend on the collection. Missing or nonscalar indexed fields are excluded from ordered scans, while source-key scans include every row. Offsets count matching rows in the selected direction and require walking past those rows; large offsets still cost work. Declared indexes skip those rows without retaining them, keeping selected results and pending-write candidates. Source-key scans and undeclared indexes retain up to `offset + limit` candidates while ordering native source records. Use `ctx.range` when a continuation cursor is more appropriate than an offset.

```ts
const timers = collection<{ tenant: string; state: string; dueAt: number }>("timers")
  .index("due", ["tenant", "state", "dueAt"]);
const due = query("timers.due", { args: v.string() }, (ctx, tenant) =>
  ctx.range(timers.by("due").range({
    prefix: [tenant, "pending"], lte: ctx.now(), limit: 20,
  })));
// Include timers in define({ collections: [timers], http: { due } }).
```

`ctx.range` returns `{ rows: [{key,value}], cursor: string | null }`, with typed keys decoded. Pass a cursor as `after` to continue. `prefix` fixes initial fields, typed from the index fields; `gt`/`gte` and `lt`/`lte` bound the next field. `reverse: true` reverses the entire order. `limit` is a positive safe integer, subject to normal memory and output budgets. A full prefix cannot also have bounds; duplicate exclusive/inclusive alternatives are rejected.

Ordered components are finite numbers, strings, booleans, or null. Their order is null, false, true, numbers ascending, strings in UTF-16 order. Composite tuples compare component by component; source keys break ties in UTF-16 order. Missing or nonscalar indexed fields have no ordered entry; equality queries still accept every JSON value. A row whose indexed fields are all scalars stores only its ordered entry, which equality queries of scalar values also read; a row with a nonscalar indexed field stores an equality entry instead.

Declared ranges seek into persistent ordered storage and retain at most `limit + 1` selected rows plus bounded pending-write candidates, merged for read-your-writes. Undeclared indexes scan native Rust source records while retaining at most `limit + 1` candidates; declare helper collections to avoid scans. Cursors bind the index fields, prefix, bounds, and direction, but not the limit. They continue against each invocation’s current snapshot, not a retained historical snapshot: concurrent changes can skip or repeat moved rows. They are traversal positions, not authorization tokens.

Derived scans and ranges depend on a window of their index, not the whole index. A result that filled its offset, limit and, for a page, its lookahead row depends only on the entries in its range up to the last row it examined; a shorter result depends on its entire range, which also covers inserts into an empty one. Rows skipped by `offset` and a page's lookahead row count only through their positions; returned rows count through their values too. A write re-runs a derivation only when its row's old or new position falls in the window, or when the value of a returned row changes, with or without a declared index. Scans without options still depend on their whole collection. Cached query results and watches that scan a declared index stay valid while its window's entries are the same: an unchanged index needs no check, and otherwise only the window's entries are compared. Key-ordered scans stay valid until their collection gains or loses a key, and scans of undeclared indexes until any row of their collection changes; the values of returned rows are checked individually in every case.

The TypeScript scheduler declares a `(state, dueAt)` index. Expiring collections seek expired deadlines. Queues index `(scope, state, availableAt)`, `(scope, state, leaseExpiresAt)` and `(state, leaseExpiresAt)`: selecting a ready job is bounded, while expired leases are inspected in bounded pages so a claim still takes the job that has been available longest. A large expired-lease herd still costs work. Explicit helper inspection APIs can return full datasets. Policies, retry behavior, lease fencing, and callbacks remain TypeScript.

## Delta reducers

`aggregate` is an opt-in derived value. Rust tracks changed rows, updates durable index membership, and sends one isolated QuickJS callback the relevant row deltas for each affected accumulator. An ordinary update removes the old row's contribution and adds the new row's contribution. Moving a row between groups removes it from the old group and adds it to the new group; deletion only removes it.

The callback signatures are:

```ts
initial(group)
add(accumulator, row, sourceKey, group)
remove(accumulator, row, sourceKey, group)
```

`index` must name one of the source reference's declared indexes. `group` is that index's equality value, typed like `.eq()`, and `sourceKey` is decoded for collections with typed keys. Use deterministic, order-independent operations with `remove` as the inverse of `add`. Counts, integer sums, and objects containing these are straightforward. Use integer units for exact money or quantity totals; floating-point addition can depend on update history. Arbitrary sorting, minima, or joins need additional accumulator state or a regular derived query.

Reducers receive no database context. They may use pure helper functions and constants. Flower rejects attempts to call database context even if the callback catches the error. Return the usual finite JSON values.

The first evaluation initializes from matching rows. Subsequent successful evaluations of a retained accumulator process changed rows, without reloading all matching rows. A reducer business error is stored as the derived value's error; a later relevant change rebuilds the group so recovery cannot accidentally reuse a partial accumulator. A code deployment rebuilds materialized aggregates using the new callbacks.

## Durable operation

Rows, index entries, accumulator cells, and their dependencies are committed atomically in the same Raft state transition. Replicas apply the committed state; snapshots and ordinary restarts preserve index membership and accumulators. No process-local JavaScript heap is required for recovery. Failed mutations or failed deployment validation publish none of their staged changes.

A direct deployment builds added indexes and removes dropped index entries in its atomic candidate. For larger existing collections, use the resumable staged path below. Remove or replace any aggregate using that index in the same bundle: an aggregate whose index is undeclared is rejected. Existing applications without declarations need no data conversion.

Deployment defaults to optimistic online preparation: a bounded native worker builds the complete candidate against one fresh immutable snapshot while the previous code, authorization policy and indexes keep serving. Cutover enters the ordered writer lane and checks the exact base revision before publishing code, keys declarations, policy, indexes and recomputed values atomically. Any intervening committed application change produces `DEPLOYMENT_CONFLICT`; no candidate changes are published. Retry with the same request ID, or call `admin.deploy(bundle, {requestId, preparation: "blocking"})` on a `FlowerAdmin` / `flower deploy FILE --preparation blocking` to hold the writer lane for preparation on a continuously busy database. The strategy is excluded from receipt identity, so changing it on a retry is safe. Already committed receipts return before rebuilding.

Direct online/blocking deployment remains a whole-candidate operation. Its backfill, initial materialization and reducer rebuilds consume the normal evaluation time, Rust/QuickJS memory, output and transaction budgets. Exceeding a budget aborts instead of installing half an index. Candidate output stays byte-accounted until durable cutover; its execution slot and snapshot are released before waiting in the writer queue. Blocking preparation pauses writes and maintenance in that logical database; committed snapshots remain readable. Size budgets for the largest full deployment or group rebuild you expect.

## Resumable staged deployment

Use a `FlowerAdmin`, including `admin.partition(id)` for a named logical database:

```ts
import { FlowerAdmin } from "@flower-js/sdk/client";
import { buildBundle } from "@flower-js/sdk/bundle";

const admin = new FlowerAdmin("http://127.0.0.1:7101", { adminToken: process.env.FLOWER_ADMIN_TOKEN });
const bundle = await buildBundle("app.ts");
const id = "orders-by-shop-v2"; // Preserve this request ID across retries.
let build = (await admin.stageDeployment(bundle, { requestId: id })).value;
while (build.phase === "backfill" || build.phase === "rebuilding") {
  build = (await admin.controlStagedDeployment({
    operation: "advance", requestId: id, maxBytes: 256 * 1024,
  })).value;
}
if (build.phase === "failed") throw new Error(build.error ?? "Staged graph failed");
if (build.phase === "ready") {
  build = (await admin.controlStagedDeployment({ operation: "activate", requestId: id })).value;
}
while (build.phase === "active" || build.phase === "canceled") {
  build = (await admin.controlStagedDeployment({
    operation: "collect", requestId: id, maxBytes: 256 * 1024,
  })).value;
}
```

`stage` durably records the target bundle/schema and a base revision without activating its code. Index definitions have canonical identities `(collection, fields)`; identical definitions share completed entries. The old schema remains authoritative for queries. New definitions are hidden from index selection, and old methods continue using the existing index or native scan path.

From that admission revision, each ordinary source mutation maintains both the active indexes and added definitions atomically. `advance` reads a bounded native page of current source rows under the logical writer lock, then durably records entries and its cursor. Updating/deleting a visited row, or inserting behind the cursor, is handled by the same dual maintenance. This closes the catch-up gap without retaining a full historical source image. Old code and policy remain active; writes pause for each page's preparation/commit, while reads can use committed snapshots. The temporary extra indexes add storage and write cost. Once index backfill completes, the job enters `rebuilding`; code-only deployments begin in this phase.

During `rebuilding`, each `advance` evaluates a bounded batch of retained roots and their required dependencies using the target bundle, storing results in a separate graph generation. Existing target outcomes and aggregate accumulators are reused. `graphCursor` and `rebuiltRoots` record durable traversal progress; roots added or removed during preparation are included by ordinary mutations, even behind the cursor. Source rows are stored once. Each mutation and managed-key update also maintains the already-built target graph, and both graphs publish in the same commit. Only the active code, policy, and graph serve public requests. This temporarily adds graph storage, derived callback work, and index writes. Speculative mutation preparation is bypassed while a target graph is being maintained; serial batching and grouped commits remain available. Rebuild progress alone does not invalidate query caches; active data, code, policy and graph changes still do.

Progress survives leader changes, complete restarts, snapshots, and logical-partition moves. Resume with `admin.stagedDeploymentStatus()` and the same request ID; there is no background worker choosing page sizes or activating code. `maxBytes` bounds inspected row/entry work during index backfill and the graph patch during rebuilding; it defaults to the configured transaction byte budget. Exact encoded command size, control admission, and evaluation limits also apply. Graph pages start with one root and adapt the next root count to observed execution time and output size, growing by at most twice per page. Batches target `FLOWER_DEPLOYMENT_PAGE_MS` (default 200 ms, capped by the evaluation timeout). This setting is independent of ordinary writer batching. If a multi-root candidate fails or exceeds its output/transaction allowance, it retries only its first root using the normal evaluation timeout; no partial candidate is published. This fallback can take the page target plus one full evaluation allowance. A single root that cannot fit makes no durable progress, preserving earlier successful pages. `scannedRows` and `builtEntries` count index backfill visits/emitted entries; `rebuiltRoots` counts roots visited by graph pages, not roots created by concurrent writes.

A single root and its newly reached dependency subgraph must still fit one evaluation. Callbacks and initial aggregate-group scans are not resumable. A page's memory budget covers the graph entries it adds, not the target graph earlier pages built. A page computes the heights of the cells it adds from their children's stored heights, and collects the cells its removals release; neither traverses earlier roots, before or after a restart. Activation refreshes clock/key-dependent work within normal budgets. These limits matter for a giant connected computation even though independent roots no longer need one combined rebuild. Arbitrary source-record transformations remain explicit application mutations.

Lower `FLOWER_DEPLOYMENT_PAGE_MS` values target smaller, more numerous graph pages and shorter competing write stalls; larger values allow fewer pages and can improve rebuild throughput at the cost of longer stalls. For latency-sensitive rebuilds, try 20 ms and measure both rebuild time and write latency on the application workload. The setting accepts positive integer milliseconds and is read at server startup; restart nodes to apply changes. It leaves `FLOWER_WRITER_BATCH_MS` and ordinary writer batching unchanged. The page target is soft, and the full single-root fallback means it is not an end-to-end latency guarantee. See the [staged deployment measurements and tuning comparison](bench/STAGED_DEPLOYMENT.md), which identifies the settings used by the historical measurements.

`activate` requires `ready` progress, unchanged base code/schema/graph generation, and no prepared participant lock. It validates current key readiness, refreshes time/key-dependent outcomes, and commits the target bundle, HTTP/maintenance/authorization registrations, key declarations, schema, active-graph pointer, and deployment receipt in one Raft transition. It reuses prepared clock-independent outcomes rather than evaluating or copying the entire graph. Readers never select a partial index or combine old code with the new graph. Activation validation or budget failure leaves the old deployment and ready job intact.

Ordinary derived exceptions retain their stored-error semantics in both graphs. A fatal target error, cycle, or budget failure during dual maintenance marks the job `failed` with an `error`, while the active application mutation still commits if its result and failure status fit the normal budgets. A failed graph cannot activate: cancel and collect it, then stage corrected code under a new request ID. A failing explicit graph page instead returns an error without changing prior progress, so page budgets can be increased and retried.

Before activation, `cancel` preserves the old application, stops dual maintenance, and records a terminal canceled receipt. It cannot undo an activated deployment; stage another bundle to roll forward. `collect` incrementally removes the canceled graph generation and its added indexes, or the previous graph generation and obsolete indexes after activation, protecting the active graph and shared index definitions. Collect before starting another stage or a different direct deployment. The immutable target plan/bundle is removed once collection completes; the small latest-job summary remains. Failed/uncertain operations can be repeated with the same ID. Direct `deploy()` with a canceled ID replays cancellation rather than activating that bundle.

A preparing, ready, or failed build reserves its request ID across ordinary mutations, key administration, and distributed transactions. Staging likewise rejects an ID already reserved by a transaction coordinator. This prevents an unrelated receipt from stranding a build before activation or cancellation.

Retry admission still governs activation and final receipts. If the request's epoch/session retires during a build, activation is rejected; cancellation and cleanup remain possible because the permanent retry fence already forbids reusing that ID. A full receipt budget can otherwise prevent activation/cancellation until space is reclaimed. Physical restore cannot roll back retry or deployment history safely without the documented external fencing procedure. The operator endpoints are `/admin/deployments` and `/partitions/{id}/admin/deployments`; see the [SDK reference](https://flower.xmit.dev/reference/deployments.html) for every request and response field.

Indexes add one persisted entry per indexed row, ordered when every component is scalar and equality otherwise, and write work when index keys change. Aggregates add retained accumulator cells and callback work when their groups change. Use them when selective queries or repeatedly maintained totals repay those costs. The [pizza example](examples/goblin-pizza-ts/goblin-pizza.ts) uses both to keep per-kitchen order statistics current with one remove/add pair per changed order.

Validation includes a three-node staged-deployment test with 64 retained roots, shared dependencies and aggregates, concurrent source/root changes, restart during index and graph preparation, coherent authorization cutover, cancellation, successive graph generations and snapshot recovery ([`tests/e2e-staged-deployment.mjs`](tests/e2e-staged-deployment.mjs)). Unit tests cover bounded retries, changes after readiness, target failures, clocks, managed keys and physical graph cleanup. The recovery regression holds back log replay while allowing quorum heartbeats and checks that application reads wait for the committed recovery barrier ([`src/consensus/tests/recovery.rs`](src/consensus/tests/recovery.rs)). Existing index tests also cover staged reads, empty-bucket invalidation, composite/null/Unicode equality, rollback, reducer errors and redeployment, plus a three-node snapshot/crash/restart test that continues updating totals: [`tests/e2e-indexes.mjs`](tests/e2e-indexes.mjs).
