// Run the TS SDK's runQueueWorker and reconcile against the reference test database with a
// recording fetch, and write the arguments of every call they make to tests/golden/wire.json.
// tests/wire.rs runs the same scenarios on the Rust worker and the in-memory Flower and compares
// the bytes. Regenerate with: node crates/flower-worker/parity/wire.ts
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { registerHooks } from "node:module";
import { setTimeout as sleep } from "node:timers/promises";

const sdk = (name: string) => new URL(`../../../sdk/${name}.ts`, import.meta.url).href;
// docs/ imports @flower-js/sdk; sdk/testing.ts imports esbuild, which only bundling needs.
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === "esbuild") return { url: "data:text/javascript,export function build() { throw new Error('no esbuild here') }", shortCircuit: true };
    const match = /^@flower-js\/sdk(?:\/([a-z0-9-]+))?$/.exec(specifier);
    return nextResolve(match ? sdk(match[1] ?? "index") : specifier, context);
  },
});

const { FlowerClient } = await import(sdk("client"));
const { testDatabase } = await import(sdk("testing"));
const { runQueueWorker, reconcile } = await import(sdk("worker"));
const { define } = await import(sdk("index"));
const { queue } = await import(sdk("temporal"));
const workersApp = (await import(new URL("../../../examples/workers.ts", import.meta.url).href)).default;
const reactiveApp = (await import(new URL("../../../docs/reactive-worker.ts", import.meta.url).href)).default;

const methods = ["enqueue", "claim", "renew", "complete", "fail", "release", "retry", "get", "ready", "stats"];
const scopedJobs = queue("scopedJobs", { lease: { defaultMs: 10_000, maxMs: 30_000 }, retry: { maxAttempts: 5 } });
const scopedApp = define({ uses: [scopedJobs], http: { ...scopedJobs.http("sjobs", { scope: "argument", methods }) } });

// Field order, a number JSON.stringify spells differently from Rust's defaults, and a line
// separator, which JSON.stringify leaves unescaped.
const TRICKY = { zeta: 1, alpha: { b: 2, a: [1.5, "é\u2028", 1e21] } };
const idle = () => ({ load: 0, reason: "idle" });
const sha = (text: string) => createHash("sha256").update(text).digest("hex");

type Call = { kind: string; name: string; args: string };
function recorder(db: any) {
  const calls: Call[] = [];
  const client = new FlowerClient("http://flower.test", {
    async fetch(url: string, init: { body: string }) {
      const body = JSON.parse(init.body);
      const kind = url.slice(url.lastIndexOf("/") + 1);
      // JSON.stringify(JSON.parse(x)) === x for what JSON.stringify wrote.
      calls.push({ kind, name: body.name, args: JSON.stringify(body.args) });
      return db.fetch(url, init);
    },
  });
  return { client, calls };
}

async function until(condition: () => boolean) {
  for (const started = Date.now(); !condition(); await sleep(2)) {
    if (Date.now() - started > 5_000) throw new Error("Timed out");
  }
}

/** Per `${kind} ${name}`, the distinct arguments in the order first sent. */
function summary(calls: Call[]) {
  const out: Record<string, string[]> = {};
  for (const { kind, name, args } of calls) {
    const list = (out[`${kind} ${name}`] ??= []);
    if (!list.includes(args)) list.push(args);
  }
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
}

const golden: Record<string, unknown> = {};

// 1. A scoped worker waiting in line, chaining claims into reports, one job at a time.
{
  const db = await testDatabase(scopedApp);
  for (const id of ["a", "b", "c"]) db.mutate("sjobs.enqueue", { scope: "s1", id, payload: { id } });
  const { client, calls } = recorder(db);
  const stop = new AbortController();
  const events: any[] = [];
  const done = runQueueWorker(client, {
    queue: "sjobs", scope: "s1", owner: "w1", wait: true, waitMs: 20_000, chain: true, batch: 4, concurrency: 1, claimers: 1,
    leaseMs: 10_000, health: idle, signal: stop.signal, onEvent: (event: any) => events.push(event),
    work: async (job: any) => {
      if (job.id === "b") throw new Error("boom");
      return job.id === "c" ? TRICKY : { done: job.id };
    },
  });
  await until(() => events.some((event) => event.type === "completed" && event.id === "c") && calls.some((call) => call.kind === "watch"));
  stop.abort();
  await done;
  golden["scoped line chain"] = summary(calls);
}

// 2. Short leases renewed, a drain that releases the job still running.
{
  const db = await testDatabase(workersApp);
  for (const id of ["quick", "long"]) db.mutate("jobs.enqueue", { id, payload: { id } });
  const { client, calls } = recorder(db);
  const stop = new AbortController();
  const done = runQueueWorker(client, {
    queue: "jobs", owner: "w2", chain: true, concurrency: 1, claimers: 1, leaseMs: 300, drainMs: 50, release: true,
    health: idle, signal: stop.signal,
    work: (job: any, signal: AbortSignal) => job.id === "quick" ? TRICKY
      : new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true })),
  });
  await until(() => calls.some((call) => call.name === "jobs.renew"));
  stop.abort();
  await done;
  golden["renew drain release"] = summary(calls);
}

// 3. A scoped worker outside any line, claiming one job per call.
{
  const db = await testDatabase(scopedApp);
  db.mutate("sjobs.enqueue", { scope: "s3", id: "x", payload: { id: "x" } });
  const { client, calls } = recorder(db);
  const stop = new AbortController();
  const events: any[] = [];
  const done = runQueueWorker(client, {
    queue: "sjobs", scope: "s3", owner: "w3", concurrency: 1, claimers: 1, health: idle, signal: stop.signal,
    onEvent: (event: any) => events.push(event), work: async () => null,
  });
  await until(() => events.some((event) => event.type === "completed") && calls.filter((call) => call.kind === "watch").length >= 2);
  stop.abort();
  await done;
  golden["scoped claims"] = summary(calls);
}

// 4. reconcile: one key, a sharded pool, and a leased pool that renews and hands a key back.
const digest = async (input: { text: string }) => sha(input.text);
{
  const db = await testDatabase(reactiveApp);
  db.mutate("document.put", { id: "one", text: "A" });
  const { client, calls } = recorder(db);
  const stop = new AbortController();
  const events: any[] = [];
  const done = reconcile(client, { external: "digest", args: "one", signal: stop.signal, onEvent: (event: any) => events.push(event), compute: digest });
  await until(() => events.some((event) => event.type === "published"));
  stop.abort();
  await done;
  golden["reconcile one key"] = summary(calls);
}
{
  const db = await testDatabase(reactiveApp);
  db.mutate("document.put", { id: "p", text: "P" });
  const { client, calls } = recorder(db);
  const stop = new AbortController();
  const events: any[] = [];
  const done = reconcile(client, { external: "digest", shard: [0, 1], batch: 2, signal: stop.signal, onEvent: (event: any) => events.push(event), compute: digest });
  await until(() => events.some((event) => event.type === "published"));
  stop.abort();
  await done;
  golden["reconcile pool"] = summary(calls);
}
{
  const db = await testDatabase(reactiveApp);
  for (const id of ["fast", "slow"]) db.mutate("document.put", { id, text: id });
  const { client, calls } = recorder(db);
  const stop = new AbortController();
  const done = reconcile(client, {
    external: "digest", lease: true, owner: "r1", concurrency: 1, leaseMs: 300, signal: stop.signal,
    compute: (input: { text: string }, _work: unknown, signal: AbortSignal) => input.text === "fast" ? digest(input)
      : new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true })),
  });
  await until(() => calls.some((call) => call.name === "digest.renew"));
  stop.abort();
  await done;
  golden["reconcile leased"] = summary(calls);
}

const path = new URL("../tests/golden/wire.json", import.meta.url);
writeFileSync(path, JSON.stringify(golden, null, 2) + "\n");
console.log(`wrote ${path.pathname}`);
