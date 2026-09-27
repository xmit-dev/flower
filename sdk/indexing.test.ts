import assert from "node:assert/strict";
import { test } from "node:test";
import { aggregate, canonicalJson, collection, component, define, FlowerError, mutation, query, v } from "./index.ts";
import type { Context, Json } from "./index.ts";
import { collectionManifest, normalizeAggregateMetadata } from "./indexing.ts";
import { testDatabase } from "./testing.ts";

const plain = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const key = v.tuple([v.string(), v.string()]);

interface Order { shop: string; cents: number; paid: boolean }
const orders = collection<Order>("orders").key(key).index("byShop", ["shop"]).index("byShopPaid", ["shop", "paid"]);
const revenue = aggregate("revenue", {
  source: orders, index: "byShop", initial: () => 0,
  add: (total, row) => total + row.cents,
  remove: (total, row) => total - row.cents,
});
const paidKeys = aggregate("paidKeys", {
  source: orders, index: "byShopPaid",
  initial: (group) => ({ group, keys: [] as [string, string][] }),
  add: (value, _row, rowKey) => ({ ...value, keys: [...value.keys, rowKey] }),
  remove: (value, _row, rowKey) => ({ ...value, keys: value.keys.filter((each) => canonicalJson(each) !== canonicalJson(rowKey)) }),
});

const put = mutation("put", { args: v.object({ key, shop: v.string(), cents: v.int(), paid: v.boolean() }) }, (ctx, order) => {
  ctx.set(orders, order.key, { shop: order.shop, cents: order.cents, paid: order.paid });
  return null;
});
const drop = mutation("drop", { args: key }, (ctx, id) => { ctx.delete(orders, id); return null; });
const shopRevenue = query("shopRevenue", { args: v.string() }, (ctx, shop) => ctx.get(revenue, shop));
const paid = query("paid", { args: v.tuple([v.string(), v.boolean()]) }, (ctx, group) => ctx.get(paidKeys, group));

test("aggregates fold rows per equality group with decoded tuple keys", async () => {
  const app = define({ definitions: [revenue, paidKeys], http: { put, drop, shopRevenue, paid } });
  const db = await testDatabase(app);
  assert.equal(db.query("shopRevenue", "a"), 0);
  assert.deepEqual(db.query("paid", ["a", true]), { group: ["a", true], keys: [] });
  db.mutate("put", { key: ["t", "1"], shop: "a", cents: 10, paid: true });
  db.mutate("put", { key: ["t", "2"], shop: "a", cents: 20, paid: false });
  db.mutate("put", { key: ["u", "1"], shop: "b", cents: 5, paid: true });
  assert.equal(db.query("shopRevenue", "a"), 30);
  assert.equal(db.query("shopRevenue", "b"), 5);
  assert.deepEqual(db.query("paid", ["a", true]), { group: ["a", true], keys: [["t", "1"]] });
  db.mutate("put", { key: ["t", "2"], shop: "a", cents: 20, paid: true });
  assert.deepEqual(db.query("paid", ["a", true]).keys, [["t", "1"], ["t", "2"]]);
  db.mutate("drop", ["t", "1"]);
  assert.equal(db.query("shopRevenue", "a"), 20);
  assert.deepEqual(db.query("paid", ["a", true]).keys, [["t", "2"]]);
});

test("aggregate callbacks see decoded keys and groups, and remove undoes add", () => {
  const calls: Json[] = [];
  const options = {
    source: orders, index: "byShop" as const,
    initial: (group: string) => { calls.push(["initial", group]); return 0; },
    add: (value: number, row: Order, rowKey: [string, string], group: string) => { calls.push(["add", rowKey, group]); return value + row.cents; },
    remove: (value: number, row: Order, rowKey: [string, string], group: string) => { calls.push(["remove", rowKey, group]); return value - row.cents; },
  };
  const traced = aggregate("traced", options);
  options.initial = () => 100;
  const ctx = new Proxy({}, { get() { throw new Error("aggregates must not read the context"); } }) as Context;
  const row = (cents: number): Order => ({ shop: "a", cents, paid: false });
  assert.equal(traced.compute(ctx, { initialize: true, group: "a", previous: null, changes: [] } as never), 0);
  assert.equal(traced.compute(ctx, { initialize: false, group: "a", previous: 30, changes: [
    { key: canonicalJson(["t", "1"]), old: row(10), new: row(15) },
    { key: canonicalJson(["t", "2"]), old: row(20) },
  ] } as never), 15);
  assert.deepEqual(calls, [["initial", "a"], ["remove", ["t", "1"], "a"], ["add", ["t", "1"], "a"], ["remove", ["t", "2"], "a"]]);
  const stringKeys = collection<{ shop: string }>("plain").index("byShop", ["shop"]);
  const seen: unknown[] = [];
  const count = aggregate("count", { source: stringKeys, index: "byShop", initial: () => 0, add: (n, _row, rowKey) => { seen.push(rowKey); return n + 1; }, remove: (n) => n - 1 });
  assert.equal(count.compute(ctx, { initialize: true, group: "a", previous: null, changes: [{ key: '["raw"]', new: { shop: "a" } }] } as never), 1);
  assert.deepEqual(seen, ['["raw"]'], "string-keyed sources pass raw keys");
});

test("aggregate validates its source, index and callbacks and freezes its metadata", () => {
  const callbacks = { initial: () => 0, add: (n: number) => n, remove: (n: number) => n };
  assert.throws(() => aggregate("bad", { source: orders, index: "missing" as never, ...callbacks }), /Unknown aggregate index "missing"/);
  // @ts-expect-error index names come from the source's declared indexes
  assert.throws(() => aggregate("bad", { source: orders, index: "byOwner", ...callbacks }));
  assert.throws(() => aggregate("bad", { source: orders, index: "byShop", ...callbacks, add: 1 as never }), /Aggregate add must be a function/);
  assert.throws(() => aggregate("bad", { source: orders, index: "byShop", ...callbacks, extra: true } as never), /does not accept "extra"/);
  assert.throws(() => aggregate("", { source: orders, index: "byShop", ...callbacks }), /nonempty/);
  assert.throws(() => aggregate("bad", { source: {} as never, index: "byShop", ...callbacks }), /collection references/);
  assert.deepEqual(plain(revenue.aggregate), { collection: "orders", fields: ["shop"] });
  assert.deepEqual(plain(paidKeys.aggregate), { collection: "orders", fields: ["shop", "paid"] });
  assert.ok(Object.isFrozen(revenue) && Object.isFrozen(revenue.aggregate) && Object.isFrozen(revenue.aggregate.fields));
  const typed = () => query("typed", (ctx) => {
    const total: number = ctx.get(revenue, "a");
    // @ts-expect-error groups are typed by the index fields
    ctx.get(paidKeys, ["a", "yes"]);
    return total;
  });
  void typed;
});

test("aggregate sources are declared automatically and definitions keep normalized metadata", () => {
  const app = define({ definitions: [revenue] });
  assert.deepEqual(plain(app.collections), [{ name: "orders", indexes: { byShop: ["shop"], byShopPaid: ["shop", "paid"] } }]);
  const stored = app.definitions.revenue as typeof revenue;
  assert.deepEqual(plain(stored.aggregate), { collection: "orders", fields: ["shop"] });
  assert.ok(Object.isFrozen(app.collections) && Object.isFrozen(stored.aggregate.fields));
  assert.equal(Object.hasOwn(define(), "collections"), false);
});

test("collection manifests merge identical declarations by name and reject conflicts", () => {
  const first = collection("a").index("i", ["x"]).index("j", ["y", "z"]);
  const reordered = collection<{ x: string; y: string; z: string }>("a").index("j", ["y", "z"]).index("i", ["x"]);
  const manifest = collectionManifest([collection("c"), first, reordered, collection("b")]);
  assert.deepEqual(plain(manifest), [
    { name: "a", indexes: { i: ["x"], j: ["y", "z"] } }, { name: "b", indexes: {} }, { name: "c", indexes: {} },
  ]);
  assert.ok(Object.isFrozen(manifest[0]) && Object.isFrozen(manifest[0].indexes));
  for (const other of [collection("a").index("i", ["x"]), collection("a").index("i", ["x"]).index("j", ["z", "y"]), collection("a")]) {
    assert.throws(() => collectionManifest([first, other]), /Collection "a" is declared with different indexes/);
  }
  assert.throws(() => define({ collections: [first], uses: [component({ collections: [collection("a").index("k", ["x"])] })] }), /different indexes/);
  assert.deepEqual(plain(define({ collections: [first], uses: [component({ collections: [reordered] })] }).collections), plain(manifest.slice(0, 1)));
  for (const bad of [{ kind: "collection", name: "x" }, { kind: "table", name: "x", indexes: {} }, { kind: "collection", name: "", indexes: {} },
    { kind: "collection", name: "x", indexes: { i: [] } }, { kind: "collection", name: "x", indexes: { i: ["f", "f"] } }, null]) {
    assert.throws(() => collectionManifest([bad as never]), TypeError);
  }
  let invoked = false;
  assert.throws(() => collectionManifest([{ get name() { invoked = true; return "x"; }, kind: "collection", indexes: {} } as never]), /collection references/);
  assert.equal(invoked, false);
});

test("aggregate metadata normalization rejects malformed input", () => {
  assert.deepEqual(plain(normalizeAggregateMetadata({ collection: "orders", fields: ["shop"] })), { collection: "orders", fields: ["shop"] });
  for (const value of [null, [], { collection: "orders" }, { collection: "orders", fields: [] }, { collection: "orders", fields: ["shop", "shop"] },
    { collection: "orders", fields: ["shop"], extra: true }, { collection: "", fields: ["shop"] }, { collection: "orders", fields: [""] }]) {
    assert.throws(() => normalizeAggregateMetadata(value), TypeError);
  }
});

test("range options are validated, normalized and snapshotted", () => {
  const timers = collection<{ tenant: string; due: number }>("timers").index("due", ["tenant", "due"]);
  const prefix: [string] = ["a"];
  const range = timers.by("due").range({ prefix, gte: -0, lt: 30, limit: 4 });
  prefix[0] = "other";
  assert.deepEqual(plain(range), { kind: "range", collection: "timers", fields: ["tenant", "due"], options: { prefix: ["a"], gte: 0, lt: 30, limit: 4 } });
  assert.ok(Object.is(range.options.gte, 0));
  assert.ok(Object.isFrozen(range) && Object.isFrozen(range.options) && Object.isFrozen(range.options.prefix));
  assert.deepEqual(plain(timers.by("due").range({ limit: 1, after: "cursor", reverse: true }).options), { prefix: [], limit: 1, after: "cursor", reverse: true });
  for (const options of [null, [], {}, { limit: 0 }, { limit: 1.5 }, { limit: Infinity }, { limit: "1" }, { limit: 1, extra: true },
    { limit: 1, prefix: ["a", 1, 2] }, { limit: 1, prefix: "a" }, { limit: 1, prefix: ["a", 1], gte: 0 }, { limit: 1, gt: 1, gte: 2 },
    { limit: 1, lt: 2, lte: 2 }, { limit: 1, prefix: [Number.NaN] }, { limit: 1, prefix: [{}] }, { limit: 1, gte: [1] },
    { limit: 1, after: null }, { limit: 1, reverse: 1 }]) {
    assert.throws(() => timers.by("due").range(options as never), TypeError, JSON.stringify(options));
  }
  // @ts-expect-error prefixes follow the index field types
  assert.throws(() => timers.by("due").range({ prefix: [1, 2, 3], limit: 1 }));
  // @ts-expect-error limit is required
  assert.throws(() => timers.by("due").range({}));
});

test("ranges page through declared indexes with cursors and decoded keys", async () => {
  const events = collection<{ tenant: string; at: number }>("events").key(v.tuple([v.string(), v.int()])).index("byTime", ["tenant", "at"]);
  const add = mutation("add", { args: v.object({ tenant: v.string(), seq: v.int(), at: v.int() }) }, (ctx, event) => {
    ctx.set(events, [event.tenant, event.seq], { tenant: event.tenant, at: event.at });
    return null;
  });
  const page = query("page", {
    args: v.object({ tenant: v.string(), after: v.optional(v.string()), reverse: v.optional(v.boolean()), gte: v.optional(v.int()) }),
  }, (ctx, { tenant, ...rest }) => ctx.range(events.by("byTime").range({ prefix: [tenant], limit: 2, ...rest })));
  const scan = query("scan", (ctx) => ctx.scan(events, { index: "byTime", prefix: ["t"], gte: 20, offset: 1, limit: 2, reverse: true }));
  const db = await testDatabase(define({ collections: [events], http: { add, page, scan } }));
  for (const [seq, at] of [[1, 50], [2, 10], [3, 40], [4, 20], [5, 30]]) db.mutate("add", { tenant: "t", seq, at });
  db.mutate("add", { tenant: "u", seq: 1, at: 0 });
  const first = db.query("page", { tenant: "t" });
  assert.deepEqual(first.rows, [{ key: ["t", 2], value: { tenant: "t", at: 10 } }, { key: ["t", 4], value: { tenant: "t", at: 20 } }]);
  assert.equal(typeof first.cursor, "string");
  const second = db.query("page", { tenant: "t", after: first.cursor! });
  assert.deepEqual(second.rows.map((row) => row.value.at), [30, 40]);
  const third = db.query("page", { tenant: "t", after: second.cursor! });
  assert.deepEqual(third.rows.map((row) => row.key), [["t", 1]]);
  assert.equal(third.cursor, null);
  assert.deepEqual(db.query("page", { tenant: "t", reverse: true }).rows.map((row) => row.value.at), [50, 40]);
  assert.deepEqual(db.query("page", { tenant: "t", gte: 25 }).rows.map((row) => row.value.at), [30, 40]);
  assert.throws(() => db.query("page", { tenant: "u", after: first.cursor! }), (error: unknown) =>
    error instanceof FlowerError && error.failure?.code === "INVALID_REFERENCE");
  assert.deepEqual(db.query("scan").map((row) => row.key), [["t", 3], ["t", 5]]);
  const undeclared = await testDatabase(define({ http: { add, page } }));
  undeclared.mutate("add", { tenant: "t", seq: 1, at: 1 });
  assert.throws(() => undeclared.query("page", { tenant: "t" }), (error: unknown) =>
    error instanceof FlowerError && error.failure?.code === "UNDECLARED_INDEX");
});
