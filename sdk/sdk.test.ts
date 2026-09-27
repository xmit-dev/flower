import assert from "node:assert/strict";
import { test } from "node:test";
import {
  collection, component, define, derive, fail, FlowerError, jwtBearer, key, mutation, participant, query, task, transaction, trigger, v,
} from "./index.ts";
import type { AuthorizationRequest, Context, Failure, Json, MutationContext, Principal, QueryContext, QueryMethod } from "./index.ts";
import { ANONYMOUS_SUBJECT, hostContext } from "./define.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";

const plain = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const pair = v.tuple([v.string(), v.int()]);

function rejected(run: () => unknown): FlowerError {
  try { run(); } catch (error) { if (error instanceof FlowerError) return error; throw error; }
  assert.fail("expected a FlowerError");
}
const roots = (db: TestDatabase<any>) => Object.keys(db.data).filter((id) => id.startsWith("root:")).sort();

// ---- Collections, typed keys and indexes

test("collections are frozen references; index() and key() return new references with typed indexes", () => {
  const base = collection<{ owner: string; active: boolean }>("records");
  const owned = base.index("owner", ["owner"]).index("ownerActive", ["owner", "active"]);
  assert.deepEqual(plain(owned), { kind: "collection", name: "records", indexes: { owner: ["owner"], ownerActive: ["owner", "active"] } });
  assert.deepEqual(plain(base.indexes), {});
  assert.ok(Object.isFrozen(owned) && Object.isFrozen(owned.indexes) && Object.isFrozen(owned.indexes.owner));
  const keyed = owned.key(pair);
  assert.notEqual(keyed, owned);
  assert.deepEqual(plain(keyed), plain(owned));
  assert.deepEqual(plain(owned.by("owner").eq("alice")), { kind: "query", collection: "records", fields: ["owner"], value: "alice" });
  assert.deepEqual(plain(owned.by("ownerActive").eq(["alice", true])), { kind: "query", collection: "records", fields: ["owner", "active"], value: ["alice", true] });
  assert.ok(Object.isFrozen(owned.by("owner").eq("alice")));
  assert.throws(() => owned.index("owner", ["active"]), /Index "owner" is already declared/);
  assert.throws(() => base.index("x", [] as never), /one or more field names/);
  assert.throws(() => base.index("x", ["owner", "owner"]), /distinct/);
  assert.throws(() => base.index("", ["owner"]), /nonempty/);
  assert.throws(() => collection(""), /nonempty/);
  assert.throws(() => owned.by("ownerActive").eq("alice" as never), /2-element tuple/);
  assert.throws(() => owned.by("owner").eq(Number.NaN as never), /finite/);
  // @ts-expect-error unknown index names do not type-check
  assert.throws(() => owned.by("missing"), /Unknown index "missing" on records/);
  // @ts-expect-error index fields name record properties
  base.index("bad", ["missing"]);
  // @ts-expect-error equality values follow the field types
  owned.by("ownerActive").eq(["alice", "yes"]);
  const odd = collection<{ key: string }>("constructor").index("__proto__", ["key"]);
  assert.equal(odd.by("__proto__").eq("value").collection, "constructor");
});

test("writes validate records (INVALID_RECORD) and keys (INVALID_KEY) and discard the mutation", async () => {
  const shops = collection("shops", v.object({ name: v.string({ min: 1 }), rating: v.optional(v.int({ min: 1, max: 5 })) }));
  const pairs = collection("pairs", v.object({ n: v.int() })).key(pair);
  const save = mutation("save", { args: v.object({ id: v.json(), shop: v.json() }) }, (ctx, input) => {
    ctx.set(shops, input.id as string, input.shop as never);
    return ctx.get(shops, input.id as string);
  });
  const savePair = mutation("savePair", { args: v.object({ key: v.json(), n: v.json() }) }, (ctx, input) => {
    ctx.set(pairs, input.key as never, { n: input.n } as never);
    return ctx.get(pairs, input.key as never);
  });
  const db = await testDatabase(define({ http: { save, savePair } }));
  assert.deepEqual(db.mutate("save", { id: "s1", shop: { name: "A", rating: 5 } }), { name: "A", rating: 5 });
  const revision = db.revision;
  const record = rejected(() => db.mutate("save", { id: "s1", shop: { name: "" } }));
  assert.equal(record.status, 422);
  assert.equal(record.code, "EVALUATION_FAILED");
  assert.deepEqual(record.failure, { code: "INVALID_RECORD", message: "shops name: must not be empty", details: { collection: "shops", key: "s1", path: ["name"] } });
  assert.deepEqual(rejected(() => db.mutate("save", { id: "s2", shop: { name: "B", extra: 1 } })).failure,
    { code: "INVALID_RECORD", message: 'shops has unexpected property "extra"', details: { collection: "shops", key: "s2", path: [] } });
  assert.deepEqual(rejected(() => db.mutate("save", { id: 1, shop: { name: "B" } })).failure, { code: "INVALID_KEY", message: "shops keys must be strings" });
  assert.deepEqual(rejected(() => db.mutate("savePair", { key: ["a", -1.5], n: 1 })).failure,
    { code: "INVALID_KEY", message: "pairs key [1]: must be a safe integer", details: { collection: "pairs" } });
  assert.deepEqual(rejected(() => db.mutate("savePair", { key: "a", n: 1 })).failure,
    { code: "INVALID_KEY", message: "pairs key must be an array of 2 items", details: { collection: "pairs" } });
  assert.deepEqual(rejected(() => db.mutate("savePair", { key: ["a", 1], n: "x" })).failure,
    { code: "INVALID_RECORD", message: "pairs n: must be a finite number", details: { collection: "pairs", key: '["a",1]', path: ["n"] } });
  assert.equal(db.revision, revision);
  assert.deepEqual(db.mutate("savePair", { key: ["a", 1], n: 2 }), { n: 2 });
  assert.deepEqual(db.data['source:["pairs","[\\"a\\",1]"]'], { n: 2 }, "tuple keys are stored as canonical JSON");
});

test("tuple keys round-trip through get, delete, prefix scans, index scans and equality queries", async () => {
  const lines = collection<{ sku: string; qty: number }>("lines").key(pair).index("bySku", ["sku"]);
  const put = mutation("put", { args: v.object({ key: pair, sku: v.string(), qty: v.int() }) }, (ctx, line) => {
    ctx.set(lines, line.key, { sku: line.sku, qty: line.qty });
    return null;
  });
  const remove = mutation("remove", { args: pair }, (ctx, id) => { ctx.delete(lines, id); return ctx.get(lines, id); });
  const get = query("get", { args: pair }, (ctx, id) => ctx.get(lines, id));
  const prefixed = query("prefixed", { args: v.array(v.union(v.string(), v.int())) }, (ctx, prefix) => ctx.scan(lines, { prefix }).map((row) => row.key));
  const sku = query("sku", { args: v.string() }, (ctx, value) => ({
    values: ctx.query(lines.by("bySku").eq(value)),
    rows: ctx.scan(lines, { index: "bySku", prefix: [value] }),
  }));
  const bounded = query("bounded", (ctx) => ctx.scan(lines, { gte: "a" }));
  const loose = query("loose", (ctx) => ctx.scan(lines, { prefix: "o1" as never }));
  const db = await testDatabase(define({ collections: [lines], http: { put, remove, get, prefixed, sku, bounded, loose } }));
  for (const [order, n, item] of [["o1", 1, "x"], ["o1", 2, "y"], ["o1", 10, "x"], ["o10", 1, "x"], ["o2", 1, "y"]] as const) {
    db.mutate("put", { key: [order, n], sku: item, qty: n });
  }
  assert.deepEqual(db.query("get", ["o1", 2]), { sku: "y", qty: 2 });
  assert.deepEqual(db.query("prefixed", ["o1"]), [["o1", 10], ["o1", 1], ["o1", 2]], "rows follow encoded key order");
  assert.deepEqual(db.query("prefixed", []), [["o1", 10], ["o1", 1], ["o1", 2], ["o10", 1], ["o2", 1]]);
  assert.deepEqual(db.query("prefixed", ["o3"]), []);
  assert.deepEqual(db.query("sku", "y"), {
    values: [{ sku: "y", qty: 2 }, { sku: "y", qty: 1 }],
    rows: [{ key: ["o1", 2], value: { sku: "y", qty: 2 } }, { key: ["o2", 1], value: { sku: "y", qty: 1 } }],
  });
  assert.equal(db.mutate("remove", ["o1", 2]), null);
  assert.equal(db.query("get", ["o1", 2]), null);
  assert.deepEqual(rejected(() => db.query("bounded")).failure, { code: "INVALID_SCAN", message: "lines has JSON keys; declare an index for ordered bounds" });
  assert.deepEqual(rejected(() => db.query("loose")).failure, { code: "INVALID_SCAN", message: "Scan prefix must be an array" });
  const typed = (ctx: Context) => {
    const row: { sku: string; qty: number } | null = ctx.get(lines, ["o1", 1]);
    // @ts-expect-error keys are typed by the key schema
    ctx.get(lines, "o1");
    // @ts-expect-error scans accept declared index names only
    ctx.scan(lines, { index: "byQty" });
    return row;
  };
  void typed;
});

test("string-keyed collections scan by source-key bounds without an index", async () => {
  const items = collection<number>("items");
  const put = mutation("put", { args: v.string() }, (ctx, id) => { ctx.set(items, id, id.length); return null; });
  const between = query("between", (ctx) => ctx.scan(items, { gte: "b", lt: "d" }).map((row) => row.key));
  const newest = query("newest", (ctx) => ctx.scan(items, { reverse: true, offset: 1, limit: 2 }).map((row) => row.key));
  const db = await testDatabase(define({ http: { put, between, newest } }));
  for (const id of ["a", "b", "bb", "c", "d"]) db.mutate("put", id);
  assert.deepEqual(db.query("between"), ["b", "bb", "c"]);
  assert.deepEqual(db.query("newest"), ["c", "bb"]);
});

// ---- derive

test("derived values compute from state, compose, and must be registered to be read", async () => {
  const prices = collection("prices", v.object({ cents: v.int() }));
  const price = derive("price", (ctx, sku: string) => ctx.get(prices, sku)?.cents ?? fail("UNKNOWN_SKU", `No price for ${sku}`));
  const doubled = derive("doubled", (ctx, sku: string) => ctx.get(price, sku) * 2);
  const clock = derive("clock", (ctx) => ctx.now());
  const setPrice = mutation("setPrice", { args: v.object({ sku: v.string(), cents: v.int() }) }, (ctx, input) => { ctx.set(prices, input.sku, { cents: input.cents }); return null; });
  const read = query("read", { args: v.string() }, (ctx, sku) => ({ doubled: ctx.get(doubled, sku), now: ctx.get(clock) }));
  const db = await testDatabase(define({ definitions: [price, doubled, clock], http: { setPrice, read } }));
  db.mutate("setPrice", { sku: "a", cents: 5 });
  assert.deepEqual(db.query("read", "a"), { doubled: 10, now: 1_000_000 });
  db.advance(5);
  db.mutate("setPrice", { sku: "a", cents: 7 });
  assert.deepEqual(db.query("read", "a"), { doubled: 14, now: 1_000_005 });
  const missing = rejected(() => db.query("read", "b")).failure!;
  assert.deepEqual([missing.code, missing.message], ["UNKNOWN_SKU", "No price for b"]);
  const unregistered = await testDatabase(define({ http: { read } }));
  assert.equal(rejected(() => unregistered.query("read", "a")).failure!.code, "DEFINITION_MISSING");
  const typed = (ctx: Context) => {
    const cents: number = ctx.get(price, "a");
    // @ts-expect-error derived arguments are typed
    ctx.get(price, 1);
    return cents;
  };
  void typed;
});

test("derive validates its name, compute function and options", () => {
  const compute = () => 1;
  const derived = derive("d", compute);
  assert.deepEqual(Object.keys(derived), ["kind", "name", "compute"]);
  assert.ok(Object.isFrozen(derived));
  assert.throws(() => derive("", compute), /nonempty/);
  assert.throws(() => derive("d", 1 as never), /compute function/);
  assert.throws(() => derive("d", compute, { extra: 1 } as never), /does not accept "extra"/);
  assert.throws(() => derive("d", compute, { materialize: "sometimes" as never }), /materialize must be/);
  assert.throws(() => derive("d", compute, { materialize: { each: {} as never } }), /materialize must be/);
  assert.throws(() => derive("d", compute, null as never), /plain object/);
});

// ---- Query and mutation specs

test("method args schemas reject invalid input with INVALID_ARGUMENT and a path", async () => {
  const line = v.object({ sku: v.string({ min: 1 }), quantity: v.int({ min: 1, max: 4 }) });
  const order = mutation("order", { args: v.object({ lines: v.array(line, { min: 1 }) }) }, (_ctx, input) => input.lines.reduce((sum, each) => sum + each.quantity, 0));
  const echo = query("echo", { args: v.nullable(v.string()) }, (_ctx, value) => value);
  const raw = query("raw", (_ctx, args: Json) => args);
  const db = await testDatabase(define({ http: { order, echo, raw } }));
  assert.equal(db.mutate("order", { lines: [{ sku: "a", quantity: 2 }, { sku: "b", quantity: 1 }] }), 3);
  const error = rejected(() => db.mutate("order", { lines: [{ sku: "a", quantity: 5 }] }));
  assert.equal(error.status, 422);
  assert.equal(error.code, "EVALUATION_FAILED");
  assert.equal(error.message, "INVALID_ARGUMENT: lines[0].quantity: must be at most 4");
  assert.deepEqual(error.failure, { code: "INVALID_ARGUMENT", message: "lines[0].quantity: must be at most 4", details: { path: ["lines", 0, "quantity"] } });
  assert.deepEqual(rejected(() => db.mutate("order", {} as never)).failure, { code: "INVALID_ARGUMENT", message: 'is missing "lines"', details: { path: [] } });
  assert.equal(rejected(() => db.query("echo", 1 as never)).failure!.code, "INVALID_ARGUMENT");
  assert.equal(db.query("echo"), null);
  assert.equal(db.query("echo", "x"), "x");
  assert.deepEqual(db.query("raw", { any: [1] }), { any: [1] });
  // @ts-expect-error argument types come from the schema
  rejected(() => db.mutate("order", { lines: [{ sku: "a", quantity: "2" }] }));
  // @ts-expect-error schema-typed arguments are required
  rejected(() => db.mutate("order"));
  // @ts-expect-error queries are not mutations
  assert.throws(() => db.mutate("echo", "x"), /is not a mutation/);
});

test("method specs reject unknown options and invalid values", () => {
  const compute = () => 1;
  assert.throws(() => query("q", { consistency: "stale" } as never, compute), /consistency must be/);
  assert.throws(() => query("q", { extra: 1 } as never, compute), /does not accept "extra"/);
  assert.throws(() => mutation("m", { consistency: "replica-local" } as never, compute), /does not accept "consistency"/);
  assert.throws(() => query("q", { access: "admins" } as never, compute), /access must be/);
  assert.throws(() => query("q", { args: 1 } as never, compute), /Expected a Flower or Standard Schema/);
  assert.throws(() => query("q", {} as never), /requires a compute function/);
  assert.throws(() => mutation("m", null as never, compute), /plain object/);
  assert.throws(() => query("", compute), /nonempty/);
  assert.ok(Object.isFrozen(query("q", compute)) && Object.isFrozen(mutation("m", { args: v.null() }, () => null)));
});

test("query consistency is code-owned and propagates to every alias", () => {
  const fresh = query("fresh", () => 1);
  const explicit = query("explicit", { consistency: "linearizable" }, () => 2);
  const local = query("local", { consistency: "replica-local" }, () => 3);
  const module = define({ http: { fresh, explicit, local, another: local } });
  assert.equal(Object.hasOwn(fresh, "consistency"), false);
  assert.equal(Object.hasOwn(explicit, "consistency"), false);
  assert.equal(local.consistency, "replica-local");
  assert.deepEqual(plain(module.http), {
    fresh: { name: "fresh", kind: "query" }, explicit: { name: "explicit", kind: "query" },
    local: { name: "local", kind: "query", consistency: "replica-local" }, another: { name: "local", kind: "query", consistency: "replica-local" },
  });
  assert.equal((module.definitions.local as typeof local).consistency, "replica-local");
  assert.equal(Object.hasOwn(module.definitions.explicit, "consistency"), false);
});

// ---- fail()

test("fail() delivers code, message and details to callers and discards the mutation's writes", async () => {
  const notes = collection<string>("notes");
  const reject = mutation("reject", { args: v.object({ code: v.string(), details: v.optional(v.json()) }) }, (ctx, input) => {
    ctx.set(notes, "n", "written");
    return fail(input.code, "rejected", input.details);
  });
  const crash = mutation("crash", (ctx) => { ctx.set(notes, "n", "written"); throw new RangeError("exploded"); });
  const db = await testDatabase(define({ http: { reject, crash } }));
  const error = rejected(() => db.mutate("reject", { code: "OUT_OF_STOCK", details: { sku: "a", left: [0] } }));
  assert.equal(error.status, 422);
  assert.equal(error.code, "EVALUATION_FAILED");
  assert.equal(error.message, "OUT_OF_STOCK: rejected");
  assert.deepEqual(error.failure, { code: "OUT_OF_STOCK", message: "rejected", details: { sku: "a", left: [0] } });
  assert.deepEqual(rejected(() => db.mutate("reject", { code: "PLAIN" })).failure, { code: "PLAIN", message: "rejected" });
  assert.deepEqual(rejected(() => db.mutate("reject", { code: "lower_case" })).failure, { code: "COMPUTE_ERROR", message: "Failure codes use UPPER_SNAKE_CASE" });
  assert.deepEqual(rejected(() => db.mutate("crash")).failure, { code: "COMPUTE_ERROR", message: "exploded" });
  assert.equal(db.revision, 0);
  assert.deepEqual(db.data, {});
});

test("fail() validates codes and JSON details", () => {
  assert.throws(() => fail("A_1", "message", { ok: true }), (error: unknown) => {
    const thrown = error as Error & Failure;
    return thrown instanceof Error && thrown.code === "A_1" && thrown.message === "message" && (thrown.details as { ok: boolean }).ok;
  });
  for (const code of ["", "lower", "9LIVES", "HAS-DASH", 1]) assert.throws(() => fail(code as string, "m"), /UPPER_SNAKE_CASE/);
  assert.throws(() => fail("BAD", "m", { n: Number.NaN } as never), TypeError);
  const lookup = (value: string | null): string => value ?? fail("MISSING", "absent");
  assert.equal(lookup("x"), "x");
});

// ---- define: components, manifest and names

test("define flattens components and nested uses once and merges their parts", () => {
  const logs = collection<string>("logs");
  const shared = collection<{ x: string }>("shared").index("byX", ["x"]);
  const signer = key("signer", { algorithm: "Ed25519", usages: ["sign"] });
  const inner = component({ collections: [shared], definitions: [derive("inner", () => 1)], tasks: [task("tick", { due: () => null, run: () => null })], keys: [signer] });
  const outer = component({ uses: [inner, { component: inner }], triggers: [trigger("log", logs, () => {})], keys: [signer] });
  const app = define({ uses: [outer, inner], collections: [shared] });
  assert.deepEqual(Object.keys(app.definitions).sort(), ["$flower.maintenance", "$flower.maintenance.error", "inner"]);
  assert.deepEqual(plain(app.collections), [{ name: "logs", indexes: {} }, { name: "shared", indexes: { byX: ["x"] } }]);
  assert.deepEqual(plain(app.keys), [{ kind: "key", name: "signer", algorithm: "Ed25519", usages: ["sign"] }]);
  const empty = component();
  assert.deepEqual({ ...empty }, { kind: "component", uses: [], collections: [], definitions: [], tasks: [], triggers: [], keys: [] });
  assert.ok(Object.isFrozen(empty) && Object.isFrozen(empty.tasks));
  assert.throws(() => define({ uses: [{} as never] }), /uses requires components/);
  assert.throws(() => define({ uses: "x" as never }), /uses must be an array/);
  assert.throws(() => component({ tasks: {} as never }), /Component tasks must be an array/);
  assert.throws(() => component({ extra: [] } as never), /does not accept "extra"/);
  const twin = () => task("tick", { due: () => null, run: () => null });
  assert.throws(() => define({ tasks: [twin()], uses: [component({ tasks: [twin()] })] }), /Duplicate task "tick"/);
});

test("module manifests have a fixed, frozen shape", () => {
  const read = query("read", () => 1);
  const write = mutation("write", () => 2);
  const plan = transaction("plan", () => ({ calls: [] }));
  const app = define({ http: { read, write, plan } });
  assert.deepEqual(Object.keys(app).sort(), ["definitions", "http", "maintenance"]);
  assert.equal(app.maintenance, null);
  assert.deepEqual(plain(app.http), { read: { name: "read", kind: "query" }, write: { name: "write", kind: "mutation" }, plan: { name: "plan", kind: "transaction" } });
  assert.equal(Object.getPrototypeOf(app.http), null);
  assert.equal(Object.getPrototypeOf(app.definitions), null);
  assert.ok([app, app.http, app.http.read, app.definitions, app.definitions.read].every(Object.isFrozen));
  const full = define({
    collections: [collection("c")], tasks: [task("t", { due: () => null, run: () => null })],
    keys: [key("k", { algorithm: "HS256", usages: ["sign"] })], auth: { authenticate: () => null, default: "public" }, http: { read },
  });
  assert.deepEqual(Object.keys(full).sort(), ["authorize", "collections", "definitions", "http", "keys", "maintenance"]);
  assert.deepEqual(plain(full.maintenance), { name: "$flower.maintenance", kind: "mutation", onError: { name: "$flower.maintenance.error", kind: "mutation" } });
  assert.deepEqual(full.authorize, { name: "$flower.authorize" });
  assert.ok(Object.isFrozen(full.maintenance) && Object.isFrozen(full.maintenance!.onError) && Object.isFrozen(full.authorize));
  assert.equal(full.definitions["$flower.authorize"].kind, "queryMethod");
  assert.equal(full.definitions["$flower.maintenance"].kind, "mutationMethod");
});

test("HTTP exposure is an explicit alias allowlist, separate from private definitions", async () => {
  const read = query("internal.read", () => 1);
  const write = mutation("internal.write", () => 2);
  const hidden = mutation("internal.hidden", () => 3);
  const module = define({ definitions: [read, hidden], http: { get: read, again: read, save: write } });
  assert.deepEqual(Object.keys(module.definitions).sort(), ["internal.hidden", "internal.read", "internal.write"]);
  assert.deepEqual(plain(module.http), {
    get: { name: "internal.read", kind: "query" }, again: { name: "internal.read", kind: "query" }, save: { name: "internal.write", kind: "mutation" },
  });
  assert.deepEqual(Object.keys(define({ definitions: [read, write] }).http), []);
  const db = await testDatabase(module);
  assert.equal(db.query("again"), 1);
  assert.equal(db.mutate("save"), 2);
  const unknown = rejected(() => (db as TestDatabase).call("internal.hidden"));
  assert.deepEqual([unknown.status, unknown.code], [404, "METHOD_NOT_FOUND"]);
  const odd = define({ definitions: [derive("__proto__", () => 42)], http: { ["__proto__"]: read, constructor: write } });
  assert.equal(Object.getPrototypeOf(odd.definitions), null);
  assert.deepEqual(plain(odd.http), { ["__proto__"]: { name: "internal.read", kind: "query" }, constructor: { name: "internal.write", kind: "mutation" } });
  const oddDb = await testDatabase(odd);
  assert.equal(oddDb.query("__proto__"), 1);
  assert.equal(oddDb.mutate("constructor"), 2);
});

test("define rejects reserved names, removed keys, conflicts and malformed definitions", () => {
  const read = query("read", () => 1);
  assert.throws(() => define({ definitions: [derive("$flower.mine", () => 1)] }), /reserved/);
  assert.throws(() => define({ http: { ok: query("$flower.mine", () => 1) } }), /reserved/);
  assert.throws(() => define({ http: { "$flower.alias": read } }), /HTTP aliases beginning with \$flower\. are reserved/);
  for (const removed of ["maintenance", "authorize", "unknown"]) {
    assert.throws(() => define({ [removed]: mutation("m", () => null) } as never), new RegExp(`does not accept "${removed}"`));
  }
  assert.throws(() => define({ definitions: [read], http: { get: query("read", () => 2) } }), /Conflicting definition "read"/);
  assert.throws(() => define({ http: { d: derive("d", () => 1) as never } }), /Only query, mutation, and transaction methods/);
  assert.throws(() => define({ http: { "": read } }), /nonempty/);
  assert.throws(() => define({ http: [read] as never }), /plain object/);
  for (const definition of [null, { kind: "queryMethod", name: "bad", compute: "x" }, { kind: "view", name: "v", compute: () => 1 }, { kind: "queryMethod", name: "", compute: () => 1 }]) {
    assert.throws(() => define({ definitions: [definition as never] }), TypeError);
  }
  assert.throws(() => define(null as never), /plain object/);
});

// ---- Tasks

const inbox = collection<{ due: number }>("inbox");
const log = collection<string[]>("log");
const append = (ctx: MutationContext, entry: string) => ctx.set(log, "entries", [...(ctx.get(log, "entries") ?? []), entry]);
const entries = query("entries", (ctx) => ctx.get(log, "entries") ?? []);
const enqueue = mutation("enqueue", { args: v.object({ id: v.string(), due: v.int() }) }, (ctx, item) => { ctx.set(inbox, item.id, { due: item.due }); return null; });
function drain(name: string, prefix: string) {
  const next = (ctx: Context) => ctx.scan(inbox).filter((row) => row.key.startsWith(prefix)).sort((a, b) => a.value.due - b.value.due)[0] ?? null;
  return task(name, {
    due: (ctx) => next(ctx)?.value.due ?? null,
    run: (ctx) => { const row = next(ctx)!; ctx.delete(inbox, row.key); append(ctx, row.key); return row.key; },
  });
}

test("tasks run earliest-due first, one per maintenance commit, while work stays due", async () => {
  const db = await testDatabase(define({ tasks: [drain("a", "a:"), drain("b", "b:")], http: { enqueue, entries } }));
  for (const [id, due] of [["a:x", 1_000_005], ["b:y", 1_000_002], ["a:z", 1_000_010], ["b:w", 1_002_000]] as const) db.mutate("enqueue", { id, due });
  assert.equal(db.maintain(), 0);
  assert.equal(db.advance(10), 3);
  assert.deepEqual(db.query("entries"), ["b:y", "a:x", "a:z"]);
  assert.equal(db.advance(1_989), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("entries"), ["b:y", "a:x", "a:z", "b:w"]);
  assert.equal(db.maintain(), 0);
});

test("failing tasks back off per task in $flower.tasks while healthy tasks proceed", async () => {
  const broken = task("broken", {
    due: (ctx) => ctx.get(inbox, "broken")?.due ?? null,
    run: (ctx) => {
      if (ctx.now() < 1_003_000) { append(ctx, "discarded"); fail("NOT_YET", "too early", { now: ctx.now() }); }
      ctx.delete(inbox, "broken");
      append(ctx, "broken");
      return null;
    },
  });
  const db = await testDatabase(define({ tasks: [broken, drain("healthy", "h:")], http: { enqueue, entries } }));
  db.mutate("enqueue", { id: "broken", due: 1_000_000 });
  db.mutate("enqueue", { id: "h:1", due: 1_000_000 });
  const state = () => db.data['source:["$flower.tasks","state"]'];
  assert.equal(db.maintain(), 2);
  assert.deepEqual(db.query("entries"), ["h:1"]);
  assert.deepEqual(state(), { broken: { failures: 1, retryAt: 1_001_000, error: { code: "NOT_YET", message: "too early", details: { now: 1_000_000 } } } });
  assert.equal(db.advance(999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(state(), { broken: { failures: 2, retryAt: 1_003_000, error: { code: "NOT_YET", message: "too early", details: { now: 1_001_000 } } } });
  assert.equal(db.advance(2_000), 1);
  assert.equal(state(), undefined);
  assert.deepEqual(db.query("entries"), ["h:1", "broken"]);
});

test("a task's onError receives the real failure and writes against the failed snapshot", async () => {
  const failures = collection<Failure & { failedAt: number }>("failures");
  const guarded = (name: string, run: (ctx: MutationContext) => Json) => task(name, {
    due: (ctx) => ctx.get(inbox, name)?.due ?? null,
    run,
    onError: (ctx, { error, failedAt }) => { ctx.set(failures, name, { ...error, failedAt }); ctx.delete(inbox, name); return error.code; },
  });
  const app = define({
    tasks: [
      guarded("crash", (ctx) => { append(ctx, "discarded"); throw new RangeError("exploded"); }),
      guarded("refuse", () => fail("REFUSED", "no", { why: "policy" })),
    ],
    http: { enqueue, entries, failures: query("failures", (ctx) => ctx.scan(failures)) },
  });
  const db = await testDatabase(app);
  db.mutate("enqueue", { id: "crash", due: 1_000_000 });
  db.mutate("enqueue", { id: "refuse", due: 1_000_000 });
  assert.equal(db.maintain(), 2);
  assert.deepEqual(db.query("failures"), [
    { key: "crash", value: { code: "COMPUTE_ERROR", message: "exploded", failedAt: 1_000_000 } },
    { key: "refuse", value: { code: "REFUSED", message: "no", details: { why: "policy" }, failedAt: 1_000_000 } },
  ]);
  assert.deepEqual(db.query("entries"), []);
  assert.equal(db.data['source:["$flower.tasks","state"]'], undefined);
});

test("task() and define() validate tasks", async () => {
  const run = () => null;
  assert.throws(() => task("", { due: () => null, run }), /nonempty/);
  assert.throws(() => task("t", { due: 1, run } as never), /requires due and run functions/);
  assert.throws(() => task("t", { due: () => null, run, onError: 1 } as never), /requires due and run functions/);
  assert.throws(() => task("t", { due: () => null, run, every: 1 } as never), /does not accept "every"/);
  assert.throws(() => define({ tasks: [{} as never] }), /tasks requires task definitions/);
  assert.ok(Object.isFrozen(task("t", { due: () => null, run })));
  const db = await testDatabase(define({ tasks: [task("nan", { due: () => Number.NaN, run })] }));
  assert.throws(() => db.maintain(), /Task nan returned an invalid due time/);
});

// ---- Triggers

test("triggers run once per changed key with decoded keys and before/after values", async () => {
  const stock = collection<{ qty: number }>("stock").key(v.tuple([v.string(), v.string()]));
  const changes = collection<Json[]>("changes");
  const record = trigger("record", stock, (ctx, change) => {
    ctx.set(changes, "all", [...(ctx.get(changes, "all") ?? []), { key: change.key, before: change.before, after: change.after }]);
  });
  const apply = mutation("apply", { args: v.array(v.object({ key: v.tuple([v.string(), v.string()]), qty: v.nullable(v.int()) })) }, (ctx, ops) => {
    for (const op of ops) op.qty === null ? ctx.delete(stock, op.key) : ctx.set(stock, op.key, { qty: op.qty });
    return null;
  });
  const seen = query("seen", (ctx) => ctx.get(changes, "all") ?? []);
  const db = await testDatabase(define({ triggers: [record], http: { apply, seen } }));
  db.mutate("apply", [{ key: ["a", "1"], qty: 1 }, { key: ["a", "1"], qty: 2 }, { key: ["b", "1"], qty: 5 }]);
  assert.deepEqual(db.query("seen"), [{ key: ["a", "1"], before: null, after: { qty: 2 } }, { key: ["b", "1"], before: null, after: { qty: 5 } }]);
  db.mutate("apply", [{ key: ["a", "1"], qty: 2 }, { key: ["c", "1"], qty: 1 }, { key: ["c", "1"], qty: null }]);
  assert.equal(db.query("seen").length, 2, "unchanged values and rows created then deleted fire nothing");
  db.mutate("apply", [{ key: ["b", "1"], qty: 6 }, { key: ["b", "1"], qty: null }]);
  assert.deepEqual(db.query("seen").at(-1), { key: ["b", "1"], before: { qty: 5 }, after: null });
});

test("trigger writes cascade within the mutation, failures abort it, and loops are bounded", async () => {
  const a = collection<number>("a"), b = collection<number>("b"), c = collection<number>("c"), loop = collection<number>("loop");
  const setA = mutation("setA", { args: v.int() }, (ctx, n) => { ctx.set(a, "k", n); return null; });
  const readC = query("readC", (ctx) => ctx.get(c, "k"));
  const spin = mutation("spin", (ctx) => { ctx.set(loop, "k", 0); return null; });
  const db = await testDatabase(define({
    triggers: [
      trigger("ab", a, (ctx, change) => {
        if (change.after !== null && change.after < 0) fail("NEGATIVE", "a must be nonnegative");
        ctx.set(b, change.key, (change.after ?? 0) * 10);
      }),
      trigger("bc", b, (ctx, change) => ctx.set(c, change.key, (change.after ?? 0) + 1)),
      trigger("forever", loop, (ctx, change) => ctx.set(loop, change.key, (change.after ?? 0) + 1)),
    ],
    http: { setA, readC, spin },
  }));
  db.mutate("setA", 2);
  assert.equal(db.query("readC"), 21);
  const revision = db.revision;
  assert.deepEqual(rejected(() => db.mutate("setA", -1)).failure, { code: "NEGATIVE", message: "a must be nonnegative" });
  assert.deepEqual(rejected(() => db.mutate("spin")).failure, { code: "TRIGGER_LOOP", message: "Triggers kept changing records for 32 rounds" });
  assert.equal(db.revision, revision);
  assert.equal(db.query("readC"), 21);
});

test("cascading triggers see one consistent chain of changes whatever the write order", async () => {
  const a = collection<number>("a"), b = collection<number>("b"), sums = collection<number>("sums"), chains = collection<string[]>("chains");
  const triggers = [
    trigger("ab", a, (ctx, change) => ctx.set(b, change.key, 20)),
    trigger("sum", b, (ctx, change) => {
      ctx.set(sums, change.key, (ctx.get(sums, change.key) ?? 0) + (change.after ?? 0) - (change.before ?? 0));
      ctx.set(chains, change.key, [...(ctx.get(chains, change.key) ?? []), `${change.before}->${change.after}`]);
    }),
  ];
  const write = mutation("write", { args: v.boolean() }, (ctx, aFirst) => {
    if (aFirst) ctx.set(a, "k", 1);
    ctx.set(b, "k", 10);
    if (!aFirst) ctx.set(a, "k", 1);
    return null;
  });
  const read = query("read", (ctx) => [ctx.get(b, "k"), ctx.get(sums, "k"), ctx.get(chains, "k")]);
  for (const aFirst of [true, false]) {
    const db = await testDatabase(define({ triggers, http: { write, read } }));
    db.mutate("write", aFirst);
    assert.deepEqual(db.query("read"), [20, 20, ["null->10", "10->20"]], `a written first: ${aFirst}`);
  }
});

test("maintenance task writes fire triggers", async () => {
  const counts = collection<number>("counts");
  const counted = trigger("count", log, (ctx) => { ctx.set(counts, "log", (ctx.get(counts, "log") ?? 0) + 1); });
  const db = await testDatabase(define({ tasks: [drain("d", "")], triggers: [counted], http: { enqueue, count: query("count", (ctx) => ctx.get(counts, "log")) } }));
  db.mutate("enqueue", { id: "x", due: 0 });
  db.mutate("enqueue", { id: "y", due: 0 });
  assert.equal(db.maintain(), 2);
  assert.equal(db.query("count"), 2);
});

test("triggers are validated, unique per source and auto-declare their sources", () => {
  const a = collection<number>("a"), b = collection<number>("b");
  assert.throws(() => trigger("", a, () => {}), /nonempty/);
  assert.throws(() => trigger("t", {} as never, () => {}), /requires a collection/);
  assert.throws(() => trigger("t", a, 1 as never), /requires a function/);
  assert.ok(Object.isFrozen(trigger("t", a, () => {})));
  const indexed = collection<{ x: string }>("indexed").index("byX", ["x"]);
  assert.deepEqual(plain(define({ triggers: [trigger("t", indexed, () => {})] }).collections), [{ name: "indexed", indexes: { byX: ["x"] } }]);
  assert.throws(() => define({ triggers: [trigger("t", a, () => {}), trigger("t", a, () => {})] }), /Duplicate trigger "t"/);
  assert.doesNotThrow(() => define({ triggers: [trigger("t", a, () => {}), trigger("t", b, () => {})] }));
  assert.throws(() => define({ triggers: [{} as never] }), /triggers requires trigger definitions/);
});

// ---- Auth

const whoami = query("whoami", (ctx) => ctx.principal());
const anyone = query("anyone", { access: "public" }, (ctx) => ctx.principal());
const users: Record<string, Principal> = { alice: { subject: "alice", tenant: "t1" }, bob: { subject: "bob", claims: { role: "admin" } } };
function authenticate(_ctx: QueryContext, credentials: Json): Principal | null {
  if (credentials === null) return null;
  if (credentials === "expired") fail("TOKEN_EXPIRED", "Token expired", { at: 1 });
  return users[credentials as string] ?? fail("UNKNOWN_USER", "Unknown user");
}
const host = { now: () => 0, clock: () => 0, changesAt: () => null, history: () => null, principal: () => null, get: () => null, scan: () => [], query: () => [], range: () => ({ rows: [], cursor: null }) };
function hookOf(app: { definitions: Readonly<Record<string, unknown>> }) {
  const hook = app.definitions["$flower.authorize"] as QueryMethod<AuthorizationRequest, Principal>;
  return (method: string, request: Partial<AuthorizationRequest> = {}) =>
    hook.compute(host as never, { credentials: null, method, args: null, partition: null, delegation: null, ...request });
}

test("authorization compiles only when a call could be refused", () => {
  assert.equal(Object.hasOwn(define({ http: { whoami } }), "authorize"), false);
  const open = query("open", { access: "public" }, () => 1);
  for (const auth of [undefined, {}, { default: "public" as const }]) assert.equal(Object.hasOwn(define({ auth, http: { whoami, open } }), "authorize"), false);
  const predicate = query("predicate", { access: (_ctx, principal) => principal === null }, () => 1);
  assert.deepEqual(define({ http: { whoami, predicate } }).authorize, { name: "$flower.authorize" });
  assert.deepEqual(define({ auth: { authenticate }, http: { whoami } }).authorize, { name: "$flower.authorize" });
  assert.deepEqual(define({ auth: { delegation: () => true }, http: { whoami } }).authorize, { name: "$flower.authorize" });
  assert.throws(() => define({ auth: { sessions: "authenticated" }, http: { whoami } }), /no authenticate/);
  assert.throws(() => define({ http: { x: query("x", { access: "authenticated" }, () => 1) } }), /no authenticate/);
  assert.throws(() => define({ auth: { default: "authenticated" }, http: { whoami } }), /no authenticate/);
  assert.throws(() => define({ auth: { extra: 1 } as never }), /does not accept "extra"/);
  for (const [auth, message] of [
    [{ authenticate: "yes" }, /auth.authenticate must be a function or an authenticator/],
    [{ authenticate: {} }, /auth.authenticate must be a function or an authenticator/],
    [{ authenticate, default: "authenticate" }, /auth.default must be "public" or "authenticated"/],
    [{ authenticate, sessions: "private" }, /auth.sessions must be "public" or "authenticated"/],
    [{ authenticate, delegation: true }, /auth.delegation must be a function/],
  ] as const) assert.throws(() => define({ auth: auth as never, http: { whoami } }), message);
});

test("authenticate resolves principals; default access requires one and anonymous callers see null", async () => {
  const notes = collection("notes", v.object({ owner: v.string(), text: v.string() }));
  const writeNote = mutation("writeNote", {
    args: v.object({ owner: v.string(), text: v.string({ max: 10 }) }),
    access: (_ctx, principal, note) => principal?.subject === note.owner,
  }, (ctx, note) => { ctx.set(notes, note.owner, note); return ctx.principal()!.subject; });
  const readNote = query("readNote", {
    args: v.string(),
    access: (ctx, principal, owner) => principal !== null && ctx.get(notes, owner)?.owner === principal.subject,
  }, (ctx, owner) => ctx.get(notes, owner));
  const app = define({ auth: { authenticate }, http: { whoami, anyone, writeNote, readNote } });
  const db = await testDatabase(app);
  assert.deepEqual(db.query("whoami", null, { credentials: "alice" }), { subject: "alice", tenant: "t1" });
  const anonymous = rejected(() => db.query("whoami"));
  assert.deepEqual([anonymous.status, anonymous.code], [403, "FORBIDDEN"]);
  assert.deepEqual(anonymous.failure, { code: "UNAUTHENTICATED", message: "Authentication required" });
  assert.equal(db.query("anyone"), null);
  assert.deepEqual(db.query("anyone", null, { credentials: "bob" }), { subject: "bob", claims: { role: "admin" } });
  assert.deepEqual(rejected(() => db.query("anyone", null, { credentials: "expired" })).failure, { code: "TOKEN_EXPIRED", message: "Token expired", details: { at: 1 } });
  assert.equal(rejected(() => db.query("whoami", null, { credentials: "mallory" })).failure!.code, "UNKNOWN_USER");
  assert.equal(db.mutate("writeNote", { owner: "alice", text: "hi" }, { credentials: "alice" }), "alice");
  assert.deepEqual(rejected(() => db.mutate("writeNote", { owner: "alice", text: "hijack" }, { credentials: "bob" })).failure, { code: "FORBIDDEN", message: "Access denied" });
  const invalid = rejected(() => db.mutate("writeNote", { owner: "alice", text: "far too long" }, { credentials: "alice" }));
  assert.equal(invalid.status, 403);
  assert.deepEqual(invalid.failure, { code: "INVALID_ARGUMENT", message: "text: must contain at most 10 characters", details: { path: ["text"] } });
  assert.deepEqual(db.query("readNote", "alice", { credentials: "alice" }), { owner: "alice", text: "hi" });
  assert.equal(rejected(() => db.query("readNote", "alice", { credentials: "bob" })).failure!.code, "FORBIDDEN");
  const preset = await testDatabase(app, { credentials: "bob" });
  assert.equal(preset.query("whoami")!.subject, "bob");
});

test("the compiled hook returns the anonymous subject, applies sessions and defaults, and rejects unknown methods", () => {
  const strict = hookOf(define({ auth: { authenticate }, http: { whoami, anyone } }));
  assert.equal(ANONYMOUS_SUBJECT, "$anonymous");
  assert.deepEqual(strict("anyone"), { subject: "$anonymous" });
  assert.deepEqual(strict("anyone", { partition: "west" }), { subject: "$anonymous", tenant: "west" });
  assert.deepEqual(strict("whoami", { credentials: "alice" }), { subject: "alice", tenant: "t1" });
  assert.throws(() => strict("$flower.session.open"), { code: "UNAUTHENTICATED" });
  assert.deepEqual(strict("$flower.session.open", { credentials: "bob" }).subject, "bob");
  assert.throws(() => strict("missing"), { code: "FORBIDDEN", message: "Unknown method missing" });
  const relaxed = hookOf(define({ auth: { authenticate, default: "public", sessions: "authenticated" }, http: { whoami } }));
  assert.deepEqual(relaxed("whoami"), { subject: "$anonymous" });
  assert.throws(() => relaxed("$flower.session.open"), { code: "UNAUTHENTICATED" });
  const guarded = hookOf(define({ auth: { default: "public", delegation: () => true }, http: { whoami } }));
  assert.deepEqual(guarded("whoami", { credentials: "alice" }), { subject: "$anonymous" });
});

test("delegated principals skip authentication and can be refused by a delegation policy", () => {
  const trusting = hookOf(define({ auth: { authenticate }, http: { whoami } }));
  const delegation = (principal: Principal | null, coordinator = "node-1") => ({ delegation: { coordinator, principal }, credentials: "mallory" });
  assert.deepEqual(trusting("whoami", delegation({ subject: "alice" })), { subject: "alice" });
  assert.throws(() => trusting("whoami", delegation(null)), { code: "UNAUTHENTICATED" });
  assert.throws(() => trusting("whoami", delegation({ subject: ANONYMOUS_SUBJECT })), { code: "UNAUTHENTICATED" });
  const coordinators: string[] = [];
  const picky = hookOf(define({
    auth: { authenticate, delegation: (_ctx, coordinator, principal) => { coordinators.push(`${coordinator}:${principal?.subject}`); return coordinator === "trusted"; } },
    http: { whoami },
  }));
  assert.deepEqual(picky("whoami", delegation({ subject: "alice" }, "trusted")), { subject: "alice" });
  assert.throws(() => picky("whoami", delegation({ subject: "alice" }, "rogue")), { code: "FORBIDDEN", message: "This database does not trust the transaction coordinator" });
  assert.deepEqual(coordinators, ["trusted:alice", "rogue:alice"]);
});

test("authenticate must return a valid principal", async () => {
  for (const bad of [{ subject: "" }, { subject: ANONYMOUS_SUBJECT }, { subject: "a", tenant: "" }, { subject: "a", extra: 1 }, { subject: 1 }, "alice", []]) {
    const db = await testDatabase(define({ auth: { authenticate: () => bad as never }, http: { whoami } }));
    const error = rejected(() => db.query("whoami", null, { credentials: "x" }));
    assert.equal(error.status, 403);
    assert.equal(error.failure!.code, "COMPUTE_ERROR", JSON.stringify(bad));
  }
});

test("jwtBearer maps verified bearer tokens to principals and declares managed keys", async () => {
  assert.throws(() => jwtBearer({ key: new Uint8Array(32) }), /Raw JWT keys require algorithms/);
  assert.throws(() => jwtBearer({ key: new Uint8Array(32), algorithms: ["HS256"], extra: 1 } as never), /does not accept "extra"/);
  const managed = key("sessions", { algorithm: "HS256", usages: ["sign", "verify"] });
  assert.deepEqual(plain(define({ auth: { authenticate: jwtBearer({ key: managed }) }, http: { whoami } }).keys), [plain(managed)]);
  const mapped = await testDatabase(define({
    auth: { authenticate: jwtBearer({ key: new Uint8Array(32), algorithms: ["HS256"], principal: (claims) => ({ subject: `user:${claims.sub}` }) }) },
    http: { whoami, anyone },
  }));
  const defaults = await testDatabase(define({ auth: { authenticate: jwtBearer({ key: new Uint8Array(32), algorithms: ["HS256"] }) }, http: { whoami } }));
  assert.equal(mapped.query("anyone"), null);
  assert.deepEqual(rejected(() => mapped.query("whoami", null, { credentials: 42 })).failure,
    { code: "UNAUTHENTICATED", message: "Credentials must be a bearer token or { token }" });
  const bridge = globalThis as { __flowerCrypto?: unknown };
  bridge.__flowerCrypto = (operation: number, _parameter: number, token: string, _key: Uint8Array, options: string) => {
    assert.equal(operation, 101);
    assert.deepEqual(JSON.parse(options).algorithms, ["HS256"]);
    if (token === "no-subject") return JSON.stringify({ claims: { tenant: "t1" }, protectedHeader: { alg: "HS256" } });
    if (token === "orphan") throw new Error("CRYPTO_ERROR: KEY_FORBIDDEN: Key must match a declaration in define({keys})");
    if (token !== "good") throw new Error("CRYPTO_ERROR: bad signature");
    return JSON.stringify({ claims: { sub: "alice", tenant: "t1" }, protectedHeader: { alg: "HS256" } });
  };
  try {
    assert.deepEqual(mapped.query("whoami", null, { credentials: "Bearer good" }), { subject: "user:alice" });
    assert.deepEqual(mapped.query("whoami", null, { credentials: { token: "good" } }), { subject: "user:alice" });
    assert.deepEqual(rejected(() => mapped.query("whoami", null, { credentials: "Bearer forged" })).failure,
      { code: "UNAUTHENTICATED", message: "Invalid bearer token: bad signature" });
    // Key trouble is the server's, so it keeps its code instead of blaming the token.
    assert.deepEqual(rejected(() => mapped.query("whoami", null, { credentials: "orphan" })).failure,
      { code: "KEY_FORBIDDEN", message: "Key must match a declaration in define({keys})" });
    assert.deepEqual(defaults.query("whoami", null, { credentials: "good" }), { subject: "alice", tenant: "t1", claims: { sub: "alice", tenant: "t1" } });
    assert.deepEqual(rejected(() => defaults.query("whoami", null, { credentials: "no-subject" })).failure, { code: "UNAUTHENTICATED", message: "The token has no subject" });
  } finally { delete bridge.__flowerCrypto; }
});

// ---- Declarative materialization

test('materialize: "always" keeps the argless instance through the maintenance task', async () => {
  const counter = collection<number>("counter");
  const total = derive("total", (ctx) => ctx.get(counter, "n") ?? 0, { materialize: "always" });
  const bump = mutation("bump", (ctx) => { ctx.set(counter, "n", (ctx.get(counter, "n") ?? 0) + 1); return null; });
  const app = define({ definitions: [total], http: { bump } });
  assert.ok(app.maintenance);
  const db = await testDatabase(app);
  assert.deepEqual(roots(db), []);
  assert.equal(db.maintain(), 1);
  assert.deepEqual(roots(db), ['root:["total",null]']);
  assert.deepEqual(db.data['source:["$flower.materialized","always:total"]'], { cursor: null, done: true });
  db.mutate("bump");
  db.mutate("bump");
  assert.equal((db.data['cell:["total",null]'] as { outcome: { value: Json } }).outcome.value, 2);
  assert.equal(db.maintain(), 0);
});

test("materialize: { each } follows rows through a trigger and backfills existing rows in pages", async () => {
  const shops = collection("shops", v.object({ name: v.string() }));
  const lines = collection<{ qty: number }>("lines").key(v.tuple([v.string(), v.string()]));
  const label = derive("label", (ctx, id: string) => ctx.get(shops, id)?.name.toUpperCase() ?? null, { materialize: { each: shops } });
  const quantity = derive("quantity", (ctx, id: [string, string]) => ctx.get(lines, id)?.qty ?? 0, { materialize: { each: lines } });
  const add = mutation("add", { args: v.object({ id: v.string(), name: v.string() }) }, (ctx, shop) => { ctx.set(shops, shop.id, { name: shop.name }); return null; });
  const remove = mutation("remove", { args: v.string() }, (ctx, id) => { ctx.delete(shops, id); return null; });
  const line = mutation("line", { args: v.tuple([v.string(), v.string()]) }, (ctx, id) => { ctx.set(lines, id, { qty: 3 }); return null; });
  const seed = mutation("seed", { args: v.int() }, (ctx, count) => {
    const raw = hostContext(ctx);
    for (let index = 0; index < count; index++) raw.set(shops, `s${String(index).padStart(3, "0")}`, { name: `shop ${index}` });
    return null;
  });
  const app = define({ definitions: [label, quantity], http: { add, remove, line, seed } });
  const db = await testDatabase(app);
  db.mutate("add", { id: "a", name: "x" });
  assert.deepEqual(roots(db), ['root:["label","a"]']);
  assert.equal((db.data['cell:["label","a"]'] as { outcome: { value: Json } }).outcome.value, "X");
  db.mutate("line", ["o1", "l1"]);
  assert.ok(roots(db).includes('root:["quantity",["o1","l1"]]'), "keys materialize as decoded arguments");
  db.mutate("remove", "a");
  assert.deepEqual(roots(db), ['root:["quantity",["o1","l1"]]']);
  db.mutate("seed", 70);
  assert.equal(roots(db).length, 1, "rows written around triggers wait for the backfill");
  assert.equal(db.maintain(), 3, "one page of 64 shops, the final page of shops, then the lines backfill");
  assert.equal(roots(db).filter((id) => id.startsWith('root:["label"')).length, 70);
  assert.equal(db.maintain(), 0);
  // @ts-expect-error each requires a collection keyed like the derived arguments
  derive("mismatch", (_ctx, id: number) => id, { materialize: { each: shops } });
});

// ---- Transactions

const accounts = collection<{ balance: number }>("accounts");
const movement = v.object({ id: v.string(), amount: v.int({ min: 1 }) });
const debit = mutation("debit", { args: movement }, (ctx, input) => {
  const balance = ctx.get(accounts, input.id)?.balance ?? 0;
  if (balance < input.amount) fail("INSUFFICIENT_FUNDS", `${input.id} has ${balance}`, { balance });
  ctx.set(accounts, input.id, { balance: balance - input.amount });
  return balance - input.amount;
});
const credit = mutation("credit", { args: movement }, (ctx, input) => {
  if (input.amount > 100) fail("LIMIT_EXCEEDED", "Credits are limited to 100");
  const balance = (ctx.get(accounts, input.id)?.balance ?? 0) + input.amount;
  ctx.set(accounts, input.id, { balance });
  return balance;
});
const balance = query("balance", { args: v.string() }, (ctx, id) => ctx.get(accounts, id)?.balance ?? 0);
const ledger = { debit, credit, balance };
const transfer = transaction("transfer", { args: v.object({ from: v.string(), to: v.string(), amount: v.int({ min: 1 }) }) }, (move) => ({
  calls: [
    participant<typeof ledger>({ partition: "west" }).call("debit", { id: move.from, amount: move.amount }),
    participant<typeof ledger>({ partition: "east" }).call("credit", { id: move.to, amount: move.amount }),
  ],
  value: { moved: move.amount },
}));
const bank = define({ http: { ...ledger, transfer } });

test("participant() builds typed, frozen calls to exactly one partition or group", () => {
  const call = participant<typeof ledger>({ group: "accounts" }).call("balance", "alice");
  assert.deepEqual(call, { group: "accounts", method: "balance", args: "alice" });
  assert.ok(Object.isFrozen(call));
  assert.deepEqual(participant({ partition: "p" }).call("anything"), { partition: "p", method: "anything", args: null });
  for (const target of [{}, { partition: "a", group: "b" }, { shard: "x" }, null]) assert.throws(() => participant(target as never), TypeError);
  assert.throws(() => participant({ partition: "" }), /nonempty/);
  assert.throws(() => participant({ partition: "p" }).call(""), /nonempty/);
  // @ts-expect-error aliases come from the target's HTTP map
  participant<typeof ledger>({ partition: "p" }).call("steal", {});
  // @ts-expect-error arguments follow the target's schemas
  participant<typeof ledger>({ partition: "p" }).call("debit", { id: "a" });
  // @ts-expect-error transactions cannot be participants
  participant<typeof bank>({ partition: "p" }).call("transfer", { from: "a", to: "b", amount: 1 });
  assert.deepEqual(bank.definitions.transfer.compute({} as never, { from: "a", to: "b", amount: 2 }), {
    calls: [{ partition: "west", method: "debit", args: { id: "a", amount: 2 } }, { partition: "east", method: "credit", args: { id: "b", amount: 2 } }],
    value: { moved: 2 },
  });
  assert.throws(() => transaction("", () => ({ calls: [] })), /nonempty/);
  assert.throws(() => transaction("t", null as never), /plan function/);
  assert.throws(() => transaction("t", { consistency: "replica-local" } as never, () => ({ calls: [] })), /does not accept "consistency"/);
});

test("transactions apply participant calls atomically across partitions", async () => {
  const db = await testDatabase(bank, { partitions: ["west", "east"] });
  const west = db.partition("west"), east = db.partition("east");
  west.mutate("credit", { id: "alice", amount: 100 });
  west.mutate("credit", { id: "alice", amount: 100 });
  const result = db.call("transfer", { from: "alice", to: "bob", amount: 40 });
  assert.deepEqual(result, { results: [160, 40], value: { moved: 40 } });
  const moved: number = result.value.moved;
  void moved;
  assert.deepEqual([west.query("balance", "alice"), east.query("balance", "bob")], [160, 40]);
  const insufficient = rejected(() => db.call("transfer", { from: "bob", to: "alice", amount: 1 }));
  assert.deepEqual([insufficient.status, insufficient.code], [422, "TRANSACTION_ABORTED"]);
  assert.deepEqual(insufficient.failure, { code: "INSUFFICIENT_FUNDS", message: "bob has 0", details: { balance: 0 } });
  const limited = rejected(() => db.call("transfer", { from: "alice", to: "bob", amount: 150 }));
  assert.deepEqual([limited.code, limited.failure!.code], ["TRANSACTION_ABORTED", "LIMIT_EXCEEDED"]);
  assert.deepEqual([west.query("balance", "alice"), east.query("balance", "bob")], [160, 40], "the committed debit rolled back");
  assert.deepEqual(rejected(() => db.call("transfer", { from: "alice", to: "bob", amount: 0 })).failure,
    { code: "INVALID_ARGUMENT", message: "amount: must be at least 1", details: { path: ["amount"] } });
  assert.deepEqual(db.mutate("transfer", { from: "alice", to: "bob", amount: 1 }).results.length, 2, "/v1/mutate runs transactions too");
});

test("maintenance continues when a task's writes make another task due through a trigger", async () => {
  const source = collection<number>("chain.source");
  const work = collection<number>("chain.work");
  const seeded = collection<boolean>("chain.seeded");
  const producer = task("chain.producer", {
    due: (ctx) => ctx.get(seeded, "done") ? null : ctx.now(),
    run: (ctx) => { ctx.set(source, "a", 1); ctx.set(seeded, "done", true); return null; },
  });
  const consumer = task("chain.consumer", {
    due: (ctx) => ctx.scan(work, { limit: 1 }).length ? ctx.now() : null,
    run: (ctx) => { for (const row of ctx.scan(work)) ctx.delete(work, row.key); return null; },
  });
  const forward = trigger("chain.forward", source, (ctx, change) => { if (change.after !== null) ctx.set(work, change.key, change.after); });
  const db = await testDatabase(define({ tasks: [producer, consumer], triggers: [forward] }));
  assert.equal(db.maintain(), 2);
  assert.equal(Object.keys(db.data).some((key) => key.startsWith('source:["chain.work"')), false);
});
