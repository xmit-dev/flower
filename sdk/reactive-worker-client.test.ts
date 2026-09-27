import assert from "node:assert/strict";
import { createHash, webcrypto } from "node:crypto";
import { registerHooks } from "node:module";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import type app from "../docs/reactive-worker.ts";
import { FlowerClient, FlowerError } from "./client.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";
import { reconcile, type ReconcileEvent, type ReconcileOptions } from "./worker.ts";

// Node ignores tsconfig paths, so resolve the download's @flower-js/sdk imports to
// current source; the worker then shares FlowerError and friends with this test.
registerHooks({
  resolve(specifier, context, nextResolve) {
    const match = /^@flower-js\/sdk(?:\/([a-z0-9-]+))?$/.exec(specifier);
    return nextResolve(match ? new URL(`./${match[1] ?? "index"}.ts`, import.meta.url).href : specifier, context);
  },
});
const { runWorker } = await import("../docs/reactive-worker-client.ts");

const entry = fileURLToPath(new URL("../docs/reactive-worker.ts", import.meta.url));
const application = () => testDatabase<typeof app>(entry);
const sha = (text: string) => createHash("sha256").update(text).digest("hex");
const originalDigest = webcrypto.subtle.digest.bind(webcrypto.subtle);
type DigestArgs = Parameters<typeof originalDigest>;
type Db = TestDatabase<typeof app>;

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

async function until(condition: () => boolean) {
  for (const started = Date.now(); !condition(); await sleep(2)) {
    if (Date.now() - started > 3_000) throw new Error("Timed out waiting for the worker");
  }
}

const digestOf = (db: Db, id: string, text: string) => db.client.waitUntil("document.get", id,
  (document) => document?.digest?.status === "ready" && document.digest.value === sha(text), { signal: AbortSignal.timeout(3_000) });

/** A client over the test database that records each publication and its reply. */
function recording(db: Db, intercept: (reply: Response, count: number) => Response | Promise<Response> = (reply) => reply) {
  const publications: { requestId: string; value: string; reply: { value: { accepted: boolean }; duplicate: boolean } }[] = [];
  const client = new FlowerClient<typeof app>("http://flower.test", {
    async fetch(url, init) {
      const response = await db.fetch(url, init);
      const body = JSON.parse(init.body);
      if (body.name !== "digest.publish") return response;
      publications.push({ requestId: body.requestId, value: body.args.value, reply: await response.clone().json() });
      return intercept(response, publications.length);
    },
  });
  return { client, publications };
}

test("a single-key worker keeps one document's digest current as it changes", async (t) => {
  const log = t.mock.method(console, "log", () => {});
  const db = await application();
  const stop = new AbortController();
  const worker = runWorker(db.client, stop.signal, "one");
  try {
    db.mutate("document.put", { id: "one", text: "A" });
    db.mutate("document.put", { id: "two", text: "not mine" });
    await digestOf(db, "one", "A");
    db.mutate("document.put", { id: "one", text: "B" });
    await digestOf(db, "one", "B");
  } finally { stop.abort(); await worker; }
  assert.deepEqual(db.query("document.get", "two"), { text: "not mine", digest: { status: "pending" } });
  assert.deepEqual(log.mock.calls.map((call) => call.arguments[0]), ["published", "published"]);
});

test("a pool worker drains every pending document and follows edits, additions and deletions", async (t) => {
  t.mock.method(console, "log", () => {});
  const db = await application();
  for (const id of ["one", "two", "three"]) db.mutate("document.put", { id, text: id });
  const stop = new AbortController();
  const worker = runWorker(db.client, stop.signal);
  try {
    for (const id of ["one", "two", "three"]) await digestOf(db, id, id);
    db.mutate("document.put", { id: "two", text: "second draft" });
    db.mutate("document.delete", "three");
    db.mutate("document.put", { id: "four", text: "four" });
    await digestOf(db, "two", "second draft");
    await digestOf(db, "four", "four");
  } finally { stop.abort(); await worker; }
  assert.deepEqual(db.query("digest.next"), []);
  assert.equal(db.query("document.get", "three"), null);
});

test("a result computed for a superseded input is rejected, and the worker computes the current one", async (t) => {
  t.mock.method(console, "log", () => {});
  const db = await application();
  const { client, publications } = recording(db);
  const started = deferred(), release = deferred();
  let computations = 0;
  t.mock.method(webcrypto.subtle, "digest", async (...args: DigestArgs) => {
    if (++computations === 1) { started.resolve(); await release.promise; }
    return originalDigest(...args);
  });
  db.mutate("document.put", { id: "one", text: "A" });
  const stop = new AbortController();
  const worker = runWorker(client, stop.signal, "one");
  try {
    await started.promise;
    db.mutate("document.put", { id: "one", text: "B" });
    release.resolve();
    await digestOf(db, "one", "B");
  } finally { release.resolve(); stop.abort(); await worker; }
  assert.deepEqual(publications.map(({ value, reply }) => [value, reply.value.accepted]), [[sha("A"), false], [sha("B"), true]]);
  assert.equal(computations, 2);
});

test("a publication whose reply is lost is retried with the same request ID and applied once", async (t) => {
  const log = t.mock.method(console, "log", () => {});
  const db = await application();
  const { client, publications } = recording(db, (reply, count) => {
    if (count === 1) throw new TypeError("fetch failed");
    return reply;
  });
  db.mutate("document.put", { id: "one", text: "A" });
  const stop = new AbortController();
  const worker = runWorker(client, stop.signal, "one");
  try { await until(() => log.mock.callCount() === 1); } finally { stop.abort(); await worker; }
  assert.equal(publications.length, 2);
  assert.equal(publications[0].requestId, publications[1].requestId);
  assert.deepEqual(publications.map(({ reply }) => [reply.value.accepted, reply.duplicate]), [[true, false], [true, true]]);
  assert.deepEqual(db.query("document.get", "one"), { text: "A", digest: { status: "ready", value: sha("A") } });
});

test("a failed computation is reported and retried until it succeeds", async (t) => {
  const log = t.mock.method(console, "log", () => {});
  let computations = 0;
  t.mock.method(webcrypto.subtle, "digest", async (...args: DigestArgs) => {
    if (++computations === 1) throw new Error("hardware hiccup");
    return originalDigest(...args);
  });
  const db = await application();
  db.mutate("document.put", { id: "one", text: "A" });
  const stop = new AbortController();
  const worker = runWorker(db.client, stop.signal, "one");
  try { await digestOf(db, "one", "A"); } finally { stop.abort(); await worker; }
  assert.deepEqual(log.mock.calls.map((call) => call.arguments[0]), ["failed", "published"]);
  assert.equal(computations, 2);
});

test("the worker rides out transient watch failures and stops on a permanent one", async (t) => {
  t.mock.method(console, "log", () => {});
  const db = await application();
  const reply = (status: number, error: object) => new Response(JSON.stringify({ error }), { status, headers: { "content-type": "application/json" } });
  let watches = 0;
  const client = new FlowerClient<typeof app>("http://flower.test", {
    async fetch(url, init) {
      if (!url.endsWith("/v1/watch")) return db.fetch(url, init);
      watches++;
      if (watches === 1) return reply(503, { code: "UNAVAILABLE", message: "No leader yet" });
      return reply(403, { code: "FORBIDDEN", message: "Authorization denied", failure: { code: "FORBIDDEN", message: "Access denied" } });
    },
  });
  await assert.rejects(runWorker(client, new AbortController().signal, "one"),
    (error) => error instanceof FlowerError && error.status === 403 && error.failure?.code === "FORBIDDEN");
  assert.equal(watches, 2);
});

test("sharded reconcile pools split the keys between them and compute concurrently", async () => {
  const db = await application();
  const ids = Array.from({ length: 8 }, (_, index) => `doc-${index}`);
  for (const id of ids) db.mutate("document.put", { id, text: id });
  const seen = [new Set<string>(), new Set<string>()];
  let running = 0, peak = 0;
  const stop = new AbortController();
  const pools = [0, 1].map((index) => reconcile<string, { recipe: string; text: string }, string>(db.client, {
    external: "digest", shard: [index, 2], concurrency: 3, signal: stop.signal,
    async compute(input, work) {
      seen[index].add(work.args);
      peak = Math.max(peak, ++running);
      await sleep(5);
      running--;
      return sha(input.text);
    },
  }));
  try { for (const id of ids) await digestOf(db, id, id); } finally { stop.abort(); await Promise.all(pools); }
  assert.deepEqual([...seen[0], ...seen[1]].sort(), ids);
  assert.equal([...seen[0]].filter((id) => seen[1].has(id)).length, 0, "shards are disjoint");
  assert.ok(peak > 1, `peak concurrency ${peak}`);
});

test("a pool keeps publishing other keys when one computed value fails the result schema", async () => {
  const db = await application();
  for (const id of ["good-1", "bad", "good-2"]) db.mutate("document.put", { id, text: id });
  const events: string[] = [];
  const stop = new AbortController();
  const pool = reconcile<string, { recipe: string; text: string }, string>(db.client, {
    external: "digest", signal: stop.signal,
    compute: async (input, work) => work.args === "bad" ? "not a digest" : sha(input.text),
    onEvent: (event) => events.push(`${event.type}:${"key" in event ? JSON.parse(event.key)[0] : ""}`),
  });
  try {
    await digestOf(db, "good-1", "good-1");
    await digestOf(db, "good-2", "good-2");
    await until(() => events.includes("failed:bad"));
  } finally { stop.abort(); await pool; }
  assert.deepEqual(db.query("document.get", "bad")?.digest, { status: "pending" });
});

type Input = { recipe: string; text: string };
/** Start a lease-mode pool over the digest external value, recording its events. */
function leased(client: FlowerClient<typeof app>, options: Partial<ReconcileOptions<string, Input, string>> & Pick<ReconcileOptions<string, Input, string>, "compute">) {
  const stop = new AbortController();
  const events: ReconcileEvent[] = [];
  const done = reconcile<string, Input, string>(client, { external: "digest", lease: true, signal: stop.signal, onEvent: (event) => events.push(event), ...options });
  const claims = () => events.filter((event) => event.type === "claimed");
  return { events, claims, done, stop: () => { stop.abort(); return done; } };
}
/** A computation that runs until its signal aborts, recording why. */
const endless = (reasons: string[] = []) => (_input: Input, _work: unknown, signal: AbortSignal) => new Promise<string>((_, reject) =>
  signal.addEventListener("abort", () => { reasons.push((signal.reason as Error).message); reject(signal.reason); }, { once: true }));

test("leased pools compute each key once between them, concurrently", async () => {
  const db = await application();
  const ids = Array.from({ length: 12 }, (_, index) => `doc-${index}`);
  for (const id of ids) db.mutate("document.put", { id, text: id });
  const seen: string[][] = [[], []];
  let running = 0, peak = 0;
  const pools = [0, 1].map((index) => leased(db.client, {
    owner: `pool-${index}`, concurrency: 3,
    async compute(input, work) {
      seen[index].push(work.args);
      peak = Math.max(peak, ++running);
      await sleep(5);
      running--;
      return sha(input.text);
    },
  }));
  try { for (const id of ids) await digestOf(db, id, id); } finally { await Promise.all(pools.map((pool) => pool.stop())); }
  assert.deepEqual([...seen[0], ...seen[1]].sort(), [...ids].sort(), "every key was computed exactly once");
  assert.ok(seen[0].length > 0 && seen[1].length > 0, `both pools took work: ${seen[0].length} and ${seen[1].length}`);
  assert.ok(peak > 3, `peak concurrency ${peak}`);
  assert.deepEqual(db.query("digest.stats", null), { ready: false, oldestReadyAt: null, nextAvailableAt: null });
});

test("when a pool goes silent, another takes its keys once their leases end", async () => {
  const db = await application();
  for (const id of ["a", "b"]) db.mutate("document.put", { id, text: id });
  let dead = false;
  const silent = new FlowerClient<typeof app>("http://flower.test", {
    fetch: (url, init) => dead ? Promise.reject(new TypeError("fetch failed")) : db.fetch(url, init),
  });
  const first = leased(silent, { owner: "silent", concurrency: 2, leaseMs: 60_000, compute: endless() });
  await until(() => first.claims().length === 2);
  dead = true;
  const second = leased(db.client, { owner: "rescuer", concurrency: 2, compute: async (input) => sha(input.text) });
  await sleep(20);
  assert.deepEqual(second.events, [], "leased keys are left alone");
  db.advance(60_000);
  try { await digestOf(db, "a", "a"); await digestOf(db, "b", "b"); } finally { await Promise.all([first.stop(), second.stop()]); }
  assert.deepEqual(second.claims().map((event) => event.type === "claimed" && event.attempt), [2, 2]);
});

test("a stopping pool hands its keys to another at once", async () => {
  const db = await application();
  for (const id of ["a", "b"]) db.mutate("document.put", { id, text: id });
  const first = leased(db.client, { owner: "leaving", concurrency: 2, compute: endless() });
  await until(() => first.claims().length === 2);
  const second = leased(db.client, { owner: "staying", concurrency: 2, compute: async (input) => sha(input.text) });
  await sleep(10);
  assert.deepEqual(second.events, []);
  const now = db.now;
  await first.stop();
  try { await digestOf(db, "a", "a"); await digestOf(db, "b", "b"); } finally { await second.stop(); }
  assert.equal(db.now, now, "no lease had to run out");
  assert.deepEqual(first.events.map((event) => event.type), ["claimed", "claimed"], "shutdown is not a failure");
});

test("an edit voids the lease on the old input and aborts its computation", async () => {
  const db = await application();
  db.mutate("document.put", { id: "a", text: "old" });
  const reasons: string[] = [];
  const stale = endless(reasons);
  const pool = leased(db.client, {
    owner: "solo", concurrency: 2, leaseMs: 300,
    compute: (input, work, signal) => input.text === "old" ? stale(input, work, signal) : Promise.resolve(sha(input.text)),
  });
  await until(() => pool.claims().length === 1);
  db.mutate("document.put", { id: "a", text: "new" });
  try {
    await digestOf(db, "a", "new");
    await until(() => pool.events.some((event) => event.type === "lost"));
  } finally { await pool.stop(); }
  assert.deepEqual(reasons, ["The lease was lost"]);
  assert.deepEqual(pool.events.map((event) => event.type), ["claimed", "claimed", "published", "lost"]);
});

test("a failed key waits out its backoff for every pool, then succeeds", async () => {
  const db = await testDatabase<typeof app>(entry, { now: Date.now() });
  const clock = setInterval(() => db.advance(Math.max(0, Date.now() - db.now)), 5);
  db.mutate("document.put", { id: "a", text: "a" });
  const times: number[] = [];
  let computations = 0;
  const compute = async (input: Input) => {
    times.push(Date.now());
    if (++computations === 1) throw new Error("hiccup");
    return sha(input.text);
  };
  const pools = ["p1", "p2"].map((owner) => leased(db.client, { owner, compute }));
  try { await digestOf(db, "a", "a"); } finally { await Promise.all(pools.map((pool) => pool.stop())); clearInterval(clock); }
  const events = pools.flatMap((pool) => pool.events);
  assert.deepEqual(events.filter((event) => event.type === "claimed").map((event) => event.type === "claimed" && event.attempt).sort(), [1, 2]);
  assert.equal(events.filter((event) => event.type === "failed").length, 1);
  assert.equal(computations, 2);
  assert.ok(times[1] - times[0] >= 100, `retried after ${times[1] - times[0]} ms`);
});

test("an adaptive leased pool computes more keys at once while keys wait, and fewer once the process falls behind", async () => {
  const db = await application();
  const ids = Array.from({ length: 16 }, (_, index) => `doc-${index}`);
  for (const id of ids) db.mutate("document.put", { id, text: id });
  const hold = deferred();
  let load = 0;
  const pool = leased(db.client, {
    owner: "adaptive", concurrency: { min: 1, max: 8 }, adjustEveryMs: 5,
    health: () => ({ load, reason: load < 1 ? "idle" : "event loop busy" }),
    async compute(input) { await hold.promise; return sha(input.text); },
  });
  const limits = () => pool.events.flatMap((event) => event.type === "limit" ? [`${event.limit} ${event.reason}`] : []);
  try {
    await until(() => pool.claims().length === 8);
    load = 1.5;
    await until(() => limits().includes("6 event loop busy"));
    load = 0;
    // Full, it claims nothing: its readiness watch shows keys still wait, and it grows back gently.
    await until(() => limits().length === 6);
    assert.equal(pool.claims().length, 8);
    hold.resolve();
    for (const id of ids) await digestOf(db, id, id);
  } finally { await pool.stop(); }
  assert.deepEqual(limits(), ["2 more work is waiting", "4 more work is waiting", "8 more work is waiting", "6 event loop busy", "7 more work is waiting", "8 more work is waiting"]);
  assert.equal(pool.claims().length, 16);
});

test("lease mode checks its options and stops on a permanent claim failure", async () => {
  const db = await application();
  const base = { external: "digest", signal: new AbortController().signal, compute: async () => "" };
  await assert.rejects(reconcile(db.client, { ...base, lease: true, args: "a" }), /omit args and shard/);
  await assert.rejects(reconcile(db.client, { ...base, lease: true, shard: [0, 2] }), /omit args and shard/);
  await assert.rejects(reconcile(db.client, { ...base, owner: "x" }), /need lease: true/);
  await assert.rejects(reconcile(db.client, { ...base, lease: true, leaseMs: 100, marginMs: 100 }), /leaseMs must exceed marginMs/);
  await assert.rejects(reconcile(db.client, { ...base, lease: true, concurrency: 1_025 }), /at most 1024/);
  await assert.rejects(reconcile(db.client, { ...base, lease: true, concurrency: { max: 1_025 } }), /at most 1024/);
  await assert.rejects(reconcile(db.client, { ...base, lease: true, concurrency: { min: 4, max: 2 } }), /1 <= min <= initial <= max/);
  await assert.rejects(reconcile(db.client, { ...base, concurrency: { max: 4 } }), /Adaptive concurrency needs lease: true/);
  await assert.rejects(reconcile(db.client, { ...base, health: () => ({ load: 0, reason: "idle" }) }), /need lease: true/);
  await assert.rejects(reconcile(db.client, { ...base, concurrency: 0 }), /positive safe integer/);
  const denied = new FlowerClient<typeof app>("http://flower.test", {
    async fetch(url, init) {
      if (JSON.parse(init.body).name !== "digest.claim") return db.fetch(url, init);
      const error = { code: "FORBIDDEN", message: "Authorization denied", failure: { code: "FORBIDDEN", message: "Access denied" } };
      return new Response(JSON.stringify({ error }), { status: 403, headers: { "content-type": "application/json" } });
    },
  });
  await assert.rejects(reconcile(denied, { ...base, lease: true }), (error) => error instanceof FlowerError && error.failure?.code === "FORBIDDEN");
});
