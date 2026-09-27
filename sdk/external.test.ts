import assert from "node:assert/strict";
import { test } from "node:test";
import { canonicalJson, collection, define, external, FlowerError, mutation, query, v } from "./index.ts";
import type { ExternalState } from "./index.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";

const docs = collection("docs", v.object({ text: v.string() }));
const lines = collection("lines", v.object({ qty: v.int() })).key(v.tuple([v.string(), v.string()]));

const words = external("words", {
  input: (ctx, id: string) => { const text = ctx.get(docs, id)?.text; return text ? { text } : null; },
  result: v.object({ count: v.int({ min: 0 }) }),
});
const tracked = external("tracked", { input: (ctx, id: string) => ctx.get(docs, id)?.text || null, each: docs });
const doubled = external("doubled", {
  input: (ctx, key: [string, string]) => ctx.get(lines, key)?.qty ?? null,
  result: v.int(),
  each: lines,
});

const put = mutation("put", { args: v.object({ id: v.string(), text: v.string() }) }, (ctx, doc) => { ctx.set(docs, doc.id, { text: doc.text }); return null; });
const remove = mutation("remove", { args: v.string() }, (ctx, id) => { ctx.delete(docs, id); return null; });
const setLine = mutation("setLine", { args: v.object({ key: v.tuple([v.string(), v.string()]), qty: v.int() }) }, (ctx, line) => {
  ctx.set(lines, line.key, { qty: line.qty });
  return null;
});
const readWords = query("readWords", { args: v.string() }, (ctx, id) => ctx.get(words, id));
const readTracked = query("readTracked", { args: v.string() }, (ctx, id) => ctx.get(tracked, id));

const app = define({
  uses: [words, tracked, doubled],
  http: { put, remove, setLine, readWords, readTracked, ...words.http("words"), ...tracked.http("tracked"), ...doubled.http("doubled") },
});

function failure(run: () => unknown) {
  try { run(); } catch (error) { assert.ok(error instanceof FlowerError, String(error)); return error; }
  assert.fail("expected a FlowerError");
}

/** The identity HTTP lease methods take: a claim without its input and expiry. */
const lease = ({ args, key, owner, attempt }: { args: any; key: string; owner: string; attempt: number }) => ({ args, key, owner, attempt });

function keys(db: TestDatabase<any>, name: string): string[] {
  return Object.keys(db.data).filter((id) => id.startsWith("source:")).map((id) => JSON.parse(id.slice(7)) as [string, string])
    .filter(([collection]) => collection === name).map(([, key]) => key);
}

test("external values stay pending until a result for the current input is published", async () => {
  const db = await testDatabase(app);
  assert.equal(db.query("readWords", "a"), null);
  assert.equal(db.query("words.pending", "a"), null);
  db.mutate("put", { id: "a", text: "one two" });
  assert.deepEqual(db.query("readWords", "a"), { status: "pending" });
  const work = db.query("words.pending", "a")!;
  assert.deepEqual(work, { args: "a", key: canonicalJson(["a", { text: "one two" }]), input: { text: "one two" } });
  assert.deepEqual(db.mutate("words.publish", { args: "a", key: "stale", value: { count: 9 } }), { accepted: false });
  assert.deepEqual(db.mutate("words.publish", { args: "b", key: work.key, value: { count: 9 } }), { accepted: false });
  assert.deepEqual(db.query("readWords", "a"), { status: "pending" });
  assert.deepEqual(db.mutate("words.publish", { args: "a", key: work.key, value: { count: 2 } }), { accepted: true });
  const state: ExternalState<{ count: number }> | null = db.query("readWords", "a");
  assert.deepEqual(state, { status: "ready", value: { count: 2 } });
  assert.equal(db.query("words.pending", "a"), null);
  assert.deepEqual(db.mutate("words.publish", { args: "a", key: work.key, value: { count: 3 } }), { accepted: true });
  assert.deepEqual(db.query("readWords", "a"), { status: "ready", value: { count: 2 } }, "the first result for an input wins");

  db.mutate("put", { id: "a", text: "one two three" });
  assert.deepEqual(db.query("readWords", "a"), { status: "pending" });
  assert.deepEqual(db.mutate("words.publish", { args: "a", key: work.key, value: { count: 2 } }), { accepted: false });
  const next = db.query("words.pending", "a")!;
  assert.notEqual(next.key, work.key);
  db.mutate("words.publish", { args: "a", key: next.key, value: { count: 3 } });
  assert.deepEqual(db.query("readWords", "a"), { status: "ready", value: { count: 3 } });
  db.mutate("put", { id: "a", text: "" });
  assert.equal(db.query("readWords", "a"), null, "a null input means no value");
  assert.equal(db.query("words.pending", "a"), null);
  // @ts-expect-error pending takes the external's argument type
  assert.throws(() => db.query("words.pending", 1));
});

test("publish validates results and its arguments", async () => {
  const db = await testDatabase(app);
  db.mutate("put", { id: "a", text: "x" });
  const { key } = db.query("words.pending", "a")!;
  const invalid = failure(() => db.mutate("words.publish", { args: "a", key, value: { count: -1 } }));
  assert.equal(invalid.status, 422);
  assert.deepEqual(invalid.failure, { code: "INVALID_ARGUMENT", message: "value count: must be at least 0", details: { path: ["count"] } });
  assert.deepEqual(failure(() => db.mutate("words.publish", { args: "a", value: { count: 1 } } as never)).failure,
    { code: "INVALID_ARGUMENT", message: 'is missing "key"', details: { path: [] } });
  // @ts-expect-error results are typed by the result schema
  assert.throws(() => db.mutate("words.publish", { args: "a", key, value: { count: "1" } }));
  assert.deepEqual(db.query("readWords", "a"), { status: "pending" });
});

test("each marks changed rows stale for worker pools, oldest first, with limits and shards", async () => {
  const db = await testDatabase(app);
  for (const id of ["c", "a", "e", "b", "d"]) { db.mutate("put", { id, text: id }); db.now++; }
  const all = db.query("tracked.next", null);
  assert.deepEqual(all.map((work) => work.args), ["c", "a", "e", "b", "d"]);
  assert.deepEqual(all[0], { args: "c", key: canonicalJson(["c", "c"]), input: "c" });
  assert.deepEqual(db.query("tracked.next", { limit: 2 }).map((work) => work.args), ["c", "a"]);
  const shards = [0, 1, 2].map((index) => db.query("tracked.next", { shard: [index, 3] }).map((work) => work.args));
  assert.deepEqual(shards.flat().sort(), ["a", "b", "c", "d", "e"], "shards partition the pending rows");
  assert.deepEqual(db.query("tracked.next", { limit: 1, shard: [0, 1] }).map((work) => work.args), ["c"]);

  assert.deepEqual(db.mutate("tracked.publish", { args: "c", key: all[0].key, value: "C" }), { accepted: true });
  assert.deepEqual(db.query("readTracked", "c"), { status: "ready", value: "C" });
  assert.deepEqual(db.query("tracked.next", null).map((work) => work.args), ["a", "e", "b", "d"]);
  assert.deepEqual(keys(db, "tracked.stale").sort(), ['"a"', '"b"', '"d"', '"e"']);
  db.mutate("put", { id: "c", text: "changed" });
  assert.deepEqual(db.query("tracked.next", null).map((work) => work.args), ["a", "e", "b", "d", "c"]);
  db.mutate("put", { id: "a", text: "a2" });
  assert.equal(db.query("tracked.next", null)[0].input, "a2", "rewriting a stale row keeps its place and refreshes its input");

  assert.equal(failure(() => db.query("tracked.next", { limit: 0 })).failure!.code, "INVALID_ARGUMENT");
  assert.equal(failure(() => db.query("tracked.next", { shard: [0, 0] })).failure!.code, "INVALID_ARGUMENT");
  assert.equal(failure(() => db.query("tracked.next", { limit: 1025 })).failure!.code, "INVALID_ARGUMENT");
});

test("clearing or deleting a tracked row removes its result and stale marker", async () => {
  const db = await testDatabase(app);
  db.mutate("put", { id: "a", text: "x" });
  const [work] = db.query("tracked.next", null);
  db.mutate("tracked.publish", { args: "a", key: work.key, value: "X" });
  assert.deepEqual(keys(db, "tracked.results"), ['"a"']);
  db.mutate("put", { id: "a", text: "" });
  assert.equal(db.query("readTracked", "a"), null);
  assert.deepEqual(keys(db, "tracked.results"), []);
  assert.deepEqual(keys(db, "tracked.stale"), []);
  db.mutate("put", { id: "a", text: "y" });
  assert.deepEqual(db.query("tracked.next", null).map((each) => each.input), ["y"]);
  db.mutate("remove", "a");
  assert.deepEqual(db.query("tracked.next", null), []);
  assert.deepEqual(keys(db, "tracked.stale"), []);
  assert.equal(db.query("readTracked", "a"), null);
});

test("each follows typed collection keys", async () => {
  const db = await testDatabase(app);
  db.mutate("setLine", { key: ["o1", "l1"], qty: 2 });
  const [work] = db.query("doubled.next", null);
  assert.deepEqual(work, { args: ["o1", "l1"], key: canonicalJson([["o1", "l1"], 2]), input: 2 });
  assert.deepEqual(db.query("doubled.pending", ["o1", "l1"]), work);
  assert.deepEqual(db.mutate("doubled.publish", { args: ["o1", "l1"], key: work.key, value: 4 }), { accepted: true });
  assert.deepEqual(keys(db, "doubled.results"), [canonicalJson(["o1", "l1"])]);
  assert.deepEqual(db.query("doubled.next", null), []);
  assert.equal(failure(() => db.mutate("doubled.publish", { args: ["o1", "l1"], key: work.key, value: 4.5 })).failure!.code, "INVALID_ARGUMENT");
});

test("http() exposes pending and publish, next only with each, and forwards access", async () => {
  assert.deepEqual(Object.keys(words.http("w")), ["w.pending", "w.publish"]);
  assert.equal(words.http("w"), words.http("w"));
  assert.throws(() => words.http("w", { access: "public" }), /External methods for "w" were already generated differently/);
  assert.deepEqual(Object.keys(tracked.http("t")).sort(), ["t.claim", "t.next", "t.pending", "t.publish", "t.ready", "t.release", "t.renew", "t.stats"]);
  assert.deepEqual(app.http["tracked.next"], { name: "tracked.next", kind: "query" });
  assert.deepEqual(app.http["words.publish"], { name: "words.publish", kind: "mutation" });
  assert.equal(Object.hasOwn(app.http, "words.next"), false);
  const guarded = define({
    uses: [words],
    auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials } : null },
    http: { put, ...words.http("open", { access: "public" }) },
  });
  const db = await testDatabase(guarded);
  assert.equal(failure(() => db.mutate("put", { id: "a", text: "x" })).failure!.code, "UNAUTHENTICATED");
  db.mutate("put", { id: "a", text: "x" }, { credentials: "alice" });
  assert.equal(db.query("open.pending", "a")!.args, "a");
});

test("claims lease the longest-waiting keys to one owner until the lease ends", async () => {
  const db = await testDatabase(app);
  for (const id of ["c", "a", "b"]) { db.mutate("put", { id, text: id }); db.now++; }
  assert.equal(db.query("tracked.ready", null), true);
  const start = db.now;
  const first = db.mutate("tracked.claim", { owner: "w1", limit: 2, leaseMs: 1_000 });
  assert.deepEqual(first, [
    { args: "c", key: canonicalJson(["c", "c"]), input: "c", owner: "w1", attempt: 1, expiresAt: start + 1_000 },
    { args: "a", key: canonicalJson(["a", "a"]), input: "a", owner: "w1", attempt: 1, expiresAt: start + 1_000 },
  ]);
  assert.deepEqual(db.mutate("tracked.claim", { owner: "w2" }).map((work) => work.args), ["b"], "leased keys are skipped");
  assert.deepEqual(db.mutate("tracked.claim", { owner: "w3" }), []);
  assert.equal(db.query("tracked.ready", null), false);
  assert.deepEqual(db.query("tracked.stats", null), { ready: false, oldestReadyAt: null, nextAvailableAt: start + 1_000 });
  assert.equal(db.query("tracked.next", null).length, 3, "next still lists leased keys");

  db.advance(1_000);
  assert.deepEqual(db.query("tracked.stats", null), { ready: true, oldestReadyAt: start + 1_000, nextAvailableAt: start + 30_000 });
  const again = db.mutate("tracked.claim", { owner: "w3" });
  assert.deepEqual(again.map((work) => [work.args, work.owner, work.attempt]), [["a", "w3", 2], ["c", "w3", 2]],
    "expired leases go back in line; equal expiries follow key order");
  assert.deepEqual(db.mutate("tracked.renew", { leases: [lease(first[0]), lease(again[1])] }), [null, db.now + 30_000], "renew skips lost leases");
  assert.equal(failure(() => db.mutate("tracked.renew", { leases: [again[1]] as never })).failure!.code, "INVALID_ARGUMENT",
    "HTTP methods take the lease identity, not the whole claim");

  // publish needs no lease: the input key decides. It also retires the lease.
  assert.deepEqual(db.mutate("tracked.publish", { args: "a", key: first[1].key, value: "A" }), { accepted: true });
  assert.deepEqual(db.mutate("tracked.renew", { leases: [lease(again[0])] }), [null]);
  assert.equal(db.mutate("tracked.release", lease(again[0])), false);
  assert.deepEqual(db.query("readTracked", "a"), { status: "ready", value: "A" });
});

test("release hands a key back at once or after a delay, and a changed input voids its lease", async () => {
  const db = await testDatabase(app);
  db.mutate("put", { id: "a", text: "one" });
  const [claim] = db.mutate("tracked.claim", { owner: "w1" });
  assert.equal(db.mutate("tracked.release", { ...lease(claim), owner: "w2" }), false, "only the owner can release");
  assert.equal(db.mutate("tracked.release", { ...lease(claim), delayMs: 500 }), true);
  assert.equal(db.mutate("tracked.release", lease(claim)), false, "a released lease is gone");
  assert.deepEqual(db.mutate("tracked.claim", { owner: "w2" }), []);
  assert.deepEqual(db.query("tracked.stats", null), { ready: false, oldestReadyAt: null, nextAvailableAt: db.now + 500 });
  db.advance(500);
  const [retry] = db.mutate("tracked.claim", { owner: "w2" });
  assert.deepEqual([retry.owner, retry.attempt], ["w2", 2], "attempts count claims of one input");
  assert.equal(db.mutate("tracked.release", lease(retry)), true);

  const [held] = db.mutate("tracked.claim", { owner: "w1" });
  assert.equal(held.attempt, 3);
  db.mutate("put", { id: "a", text: "one" });
  assert.deepEqual(db.mutate("tracked.renew", { leases: [lease(held)] }), [db.now + 30_000], "rewriting the same input keeps the lease");
  db.mutate("put", { id: "a", text: "two" });
  assert.deepEqual(db.mutate("tracked.renew", { leases: [lease(held)] }), [null], "a new input voids the lease");
  const [fresh] = db.mutate("tracked.claim", { owner: "w2" });
  assert.deepEqual([fresh.input, fresh.attempt], ["two", 1], "and is claimable at once, with a fresh attempt count");
  assert.equal(db.mutate("tracked.release", { ...lease(fresh), delayMs: 60_000 }), true);
  db.mutate("put", { id: "a", text: "three" });
  assert.equal(db.mutate("tracked.claim", { owner: "w3" })[0].input, "three", "a new input also skips a retry delay");
});

test("claims drop keys that are no longer pending and respect lease limits", async () => {
  const flags = collection("flags", v.boolean());
  const gated = external("gated", {
    input: (ctx, id: string) => ctx.get(flags, "on") ? ctx.get(docs, id)?.text || null : null,
    each: docs,
    lease: { defaultMs: 100, maxMs: 1_000 },
  });
  const flag = mutation("flag", { args: v.boolean() }, (ctx, on) => { ctx.set(flags, "on", on); return null; });
  const db = await testDatabase(define({ uses: [gated], http: { put, flag, ...gated.http("g") } }));
  db.mutate("flag", true);
  db.mutate("put", { id: "a", text: "x" });
  const [claim] = db.mutate("g.claim", { owner: "w1" });
  assert.equal(claim.expiresAt, db.now + 100, "lease.defaultMs applies");
  assert.equal(failure(() => db.mutate("g.claim", { owner: "w1", leaseMs: 1_001 })).failure!.code, "LEASE_TOO_LONG");
  assert.equal(failure(() => db.mutate("g.renew", { leases: [lease(claim)], leaseMs: 1_001 })).failure!.code, "LEASE_TOO_LONG");
  assert.equal(failure(() => db.mutate("g.claim", { owner: "w1", limit: 1_025 })).failure!.code, "INVALID_ARGUMENT");
  assert.equal(failure(() => db.mutate("g.release", { ...lease(claim), delayMs: -1 })).failure!.code, "INVALID_ARGUMENT");
  assert.equal(failure(() => db.mutate("g.claim", { owner: "" })).failure!.code, "INVALID_ARGUMENT");

  // Turning the flag off ends the work without writing the tracked row, so its marker lingers until a claim reads it.
  db.mutate("put", { id: "b", text: "y" });
  db.mutate("flag", false);
  assert.equal(db.query("g.ready", null), true);
  assert.deepEqual(db.mutate("g.claim", { owner: "w2" }), []);
  assert.equal(db.query("g.ready", null), false);
  assert.deepEqual(keys(db, "gated.stale"), ['"a"'], "the leased marker waits for its lease");
  db.advance(100);
  assert.deepEqual(db.mutate("g.claim", { owner: "w2" }), []);
  assert.deepEqual(keys(db, "gated.stale"), []);
});

test("external() validates its configuration and next() options", () => {
  assert.throws(() => external("", { input: () => null }), /nonempty/);
  assert.throws(() => external("x", {} as never), /input function/);
  assert.throws(() => external("x", { input: () => null, extra: true } as never), /does not accept "extra"/);
  assert.throws(() => external("x", { input: () => null, lease: {} } as never), /Leases need an external value that tracks a collection/);
  assert.throws(() => external("x", { input: () => null, each: docs, lease: { defaultMs: 10, maxMs: 5 } }), /default lease exceeds/);
  assert.throws(() => external("x", { input: () => null, each: docs, lease: { maxMs: 0 } }), /lease.maxMs must be a positive safe integer/);
  assert.throws(() => external("x", { input: () => null, each: docs, lease: { other: 1 } as never }), /does not accept "other"/);
  assert.throws(() => words.claim({} as never, "w"), /does not track a collection/);
  assert.throws(() => words.next({} as never), /does not track a collection/);
  for (const options of [{ limit: 0 }, { limit: 1.5 }, { shard: [1, 1] }, { shard: [0] }, { shard: [-1, 2] }]) {
    assert.throws(() => tracked.next({} as never, options as never), (error: any) => error.code === "INVALID_ARGUMENT");
  }
  assert.throws(() => tracked.next({} as never, { other: 1 } as never), TypeError);
  assert.equal(words.kind, "derived");
  assert.equal(words.name, "words");
  assert.ok(Object.isFrozen(words));
  const module = define({ uses: [tracked] });
  assert.deepEqual(Object.keys(module.definitions).sort(), ["tracked", "tracked.input"]);
  assert.deepEqual(module.collections!.map((each) => each.name), ["docs", "tracked.results", "tracked.stale"]);
});
