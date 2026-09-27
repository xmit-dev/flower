import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import app from "../examples/workers.ts";
import { FlowerClient, FlowerError, type FlowerFetch } from "./client.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";
import { runQueueWorker, type QueueWorkerEvent, type QueueWorkerOptions } from "./worker.ts";

const idle = () => ({ load: 0, reason: "idle" });

type Db = TestDatabase<typeof app>;
type Call = { name: string; args: Record<string, unknown>; requestId?: string };

// Workers keep local deadlines with Date.now(), so the server clock starts there.
async function queue(ids: string[]): Promise<Db> {
  const db = await testDatabase(app, { now: Date.now() });
  for (const id of ids) db.mutate("jobs.enqueue", { id, payload: { id } });
  return db;
}

/** Keep server time in step with the wall clock, running due maintenance like a leader. */
function wallClock(db: Db): () => void {
  const timer = setInterval(() => db.advance(Math.max(0, Date.now() - db.now)), 5);
  return () => clearInterval(timer);
}

/** A client over the database that records every call; intercept may drop or replace replies. */
function recording(db: Db, intercept?: (call: Call, reply: Response) => Response | Promise<Response>) {
  const calls: Call[] = [];
  const fetch: FlowerFetch = async (url, init) => {
    const reply = await db.fetch(url, init);
    if (url.endsWith("/v1/watch")) return reply;
    const call: Call = JSON.parse(init.body);
    calls.push({ name: call.name, args: call.args, requestId: call.requestId });
    return intercept ? intercept(call, reply) : reply;
  };
  return { client: new FlowerClient<typeof app>("http://flower.test", { fetch }), calls };
}

// The process's real load would make limits depend on the machine running the tests.
function start(client: FlowerClient<typeof app>, options: Partial<QueueWorkerOptions> & Pick<QueueWorkerOptions, "work">) {
  const stop = new AbortController();
  const events: QueueWorkerEvent[] = [];
  const done = runQueueWorker(client, { queue: "jobs", signal: stop.signal, health: idle, onEvent: (event) => events.push(event), ...options });
  return {
    events, done, stop: () => { stop.abort(); return done; },
    types: () => events.filter((event) => event.type !== "limit").map((event) => event.type),
    count: (type: QueueWorkerEvent["type"]) => events.filter((event) => event.type === type).length,
  };
}

async function until(condition: () => boolean) {
  for (const started = Date.now(); !condition(); await sleep(2)) {
    if (Date.now() - started > 3_000) throw new Error("Timed out waiting for the worker");
  }
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

const job = (db: Db, id: string) => {
  const found = db.query("jobs.get", { id });
  assert.ok(found, `no job ${id}`);
  return found;
};
const leaseEnd = (signal: AbortSignal) => new Promise<never>((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }));

test("a worker drains the queue concurrently and completes every job exactly once", async () => {
  const ids = ["a", "b", "c", "d", "e"];
  const db = await queue(ids);
  const both = deferred();
  let running = 0;
  const worker = start(db.client, {
    owner: "test-worker", concurrency: 2,
    async work(claim) {
      if (++running === 2) both.resolve();
      await both.promise;
      await sleep(1);
      running--;
      return { done: claim.id, by: claim.owner };
    },
  });
  await until(() => worker.types().filter((type) => type === "completed").length === ids.length);
  await worker.stop();
  assert.deepEqual(ids.map((id) => [job(db, id).state, job(db, id).attempts, job(db, id).result]), ids.map((id) => ["completed", 1, { done: id, by: "test-worker" }]));
  assert.equal(db.query("jobs.ready"), false);
});

test("a failed attempt is reported, requeued with backoff, and retried once it becomes ready", async () => {
  const db = await queue(["flaky"]);
  const worker = start(db.client, {
    work(claim) {
      if (claim.attempt === 1) throw new Error("upstream said no");
      return { attempt: claim.attempt };
    },
  });
  await until(() => worker.types().includes("failed"));
  const failed = job(db, "flaky");
  assert.deepEqual([failed.state, failed.attempts, failed.error, failed.availableAt], ["pending", 1, { message: "upstream said no" }, db.now + 1_000]);
  db.advance(999);
  await sleep(20);
  assert.deepEqual(worker.types(), ["claimed", "failed"], "the worker waits out the backoff");
  db.advance(1);
  await until(() => worker.types().includes("completed"));
  await worker.stop();
  assert.deepEqual(worker.types(), ["claimed", "failed", "claimed", "completed"]);
  assert.deepEqual(worker.events[1], { type: "failed", id: "flaky", error: "upstream said no" });
  const done = job(db, "flaky");
  assert.deepEqual([done.state, done.attempts, done.result], ["completed", 2, { attempt: 2 }]);
});

test("a result Flower cannot store fails the attempt with the reason instead of going unreported", async () => {
  const db = await queue(["odd"]);
  const worker = start(db.client, {
    work: () => Object.defineProperty({ ok: true }, "buffer", { value: "hidden", enumerable: false }),
  });
  try {
    await until(() => worker.types().includes("failed"));
  } finally {
    await worker.stop();
  }
  const reason = "The result cannot be stored: Flower values cannot contain symbols, hidden properties, or accessors";
  assert.deepEqual(worker.events[1], { type: "failed", id: "odd", error: reason });
  const failed = job(db, "odd");
  assert.deepEqual([failed.state, failed.attempts, failed.error], ["pending", 1, { message: reason }]);
});

test("renewal keeps a job alive through many short leases", async () => {
  const db = await queue(["long"]);
  const { client, calls } = recording(db);
  const stopClock = wallClock(db);
  try {
    const worker = start(client, {
      leaseMs: 300,
      async work(_claim, signal) {
        await sleep(900, undefined, { signal });
        return "survived";
      },
    });
    await until(() => worker.events.length === 2);
    await worker.stop();
    assert.deepEqual(worker.types(), ["claimed", "completed"]);
  } finally { stopClock(); }
  const done = job(db, "long");
  assert.deepEqual([done.state, done.attempts, done.result], ["completed", 1, "survived"]);
  const renewals = calls.filter((call) => call.name === "jobs.renew");
  assert.ok(renewals.length >= 5, "renewed well past the first lease");
  assert.deepEqual(renewals[0].args, { leases: [{ id: "long", owner: (renewals[0].args.leases as { owner: string }[])[0].owner, token: 1 }], leaseMs: 300 });
});

test("work still running at the end of its lease is aborted and failed with the reason", async () => {
  const db = await queue(["slow"]);
  const worker = start(db.client, { leaseMs: 100, renew: false, work: (_claim, signal) => sleep(5_000, null, { signal }) });
  await until(() => worker.types().includes("failed"));
  await worker.stop();
  const failed = job(db, "slow");
  assert.deepEqual([failed.state, failed.attempts, failed.error], ["pending", 1, { message: "The lease ran out" }]);
});

test("a completion after another worker took over the expired lease is reported as lost", async () => {
  const db = await queue(["contested"]);
  const started = deferred(), release = deferred();
  const worker = start(db.client, {
    renew: false, leaseMs: 10_000,
    async work() { started.resolve(); await release.promise; return "too late"; },
  });
  await started.promise;
  db.advance(10_000);
  const thief = db.mutate("jobs.claim", { owner: "thief" });
  assert.ok(thief);
  assert.equal(thief.attempt, 2);
  release.resolve();
  await until(() => worker.types().includes("lost"));
  await worker.stop();
  assert.deepEqual(worker.types(), ["claimed", "lost"]);
  assert.equal(job(db, "contested").lease?.owner, "thief", "the lost completion changed nothing");
  db.mutate("jobs.complete", { id: thief.id, owner: thief.owner, token: thief.token, result: "rescued" });
  assert.equal(job(db, "contested").result, "rescued");
});

test("renewal notices a lease taken over after expiry and stops the work early", async () => {
  const db = await queue(["contested"]);
  const started = deferred();
  let reason: unknown;
  const worker = start(db.client, {
    leaseMs: 300,
    async work(_claim, signal) { started.resolve(); try { return await leaseEnd(signal); } catch (error) { reason = error; throw error; } },
  });
  await started.promise;
  db.advance(300);
  assert.ok(db.mutate("jobs.claim", { owner: "thief" }));
  await until(() => worker.types().includes("lost"));
  await worker.stop();
  assert.deepEqual(worker.types(), ["claimed", "lost"]);
  assert.equal((reason as Error).message, "The lease was lost");
  assert.equal(job(db, "contested").lease?.owner, "thief");
});

test("a completion whose reply is lost is retried with the same request ID and applied once", async () => {
  const db = await queue(["a"]);
  let dropped = 0;
  const { client, calls } = recording(db, (call, reply) => {
    if (call.name === "jobs.complete" && dropped++ === 0) throw new TypeError("fetch failed");
    return reply;
  });
  const worker = start(client, { retry: { initialDelayMs: 1 }, work: () => "ok" });
  await until(() => worker.events.length === 2);
  await worker.stop();
  assert.deepEqual(worker.types(), ["claimed", "completed"]);
  const completions = calls.filter((call) => call.name === "jobs.complete");
  assert.equal(completions.length, 2);
  assert.equal(completions[0].requestId, completions[1].requestId);
  const done = job(db, "a");
  assert.deepEqual([done.state, done.attempts, done.result], ["completed", 1, "ok"]);
});

test("stopping lets the held job finish and claims nothing new", async () => {
  const db = await queue(["a", "b"]);
  const { client, calls } = recording(db);
  const started = deferred(), release = deferred();
  const worker = start(client, {
    concurrency: 1,
    async work(_claim, signal) { started.resolve(); await release.promise; return { interrupted: signal.aborted }; },
  });
  await started.promise;
  const stopped = worker.stop();
  release.resolve();
  await stopped;
  assert.deepEqual(["a", "b"].map((id) => job(db, id).state), ["completed", "pending"]);
  assert.deepEqual(job(db, "a").result, { interrupted: false }, "stopping does not abort work in progress");
  assert.deepEqual(calls.map((call) => call.name), ["jobs.claim", "jobs.complete"]);
});

test("a permanent claim error rejects the worker instead of spinning", async () => {
  const db = await queue(["a"]);
  const { client, calls } = recording(db);
  await assert.rejects(runQueueWorker(client, { queue: "jobs", signal: new AbortController().signal, leaseMs: 60_000, work: () => null }),
    (error) => error instanceof FlowerError && error.failure?.code === "LEASE_TOO_LONG");
  assert.deepEqual(calls.map((call) => call.name), ["jobs.claim"]);
  assert.equal(job(db, "a").state, "pending");
});

test("a permanent claim error stops every claimer once held jobs finish", async () => {
  const db = await queue(["a", "b", "c"]);
  let claims = 0;
  const denied = () => new Response(JSON.stringify({ error: { code: "FORBIDDEN", message: "Authorization denied", failure: { code: "FORBIDDEN", message: "Access revoked" } } }),
    { status: 403, headers: { "content-type": "application/json" } });
  const client = new FlowerClient<typeof app>("http://flower.test", {
    fetch: async (url, init) => JSON.parse(init.body).name === "jobs.claim" && ++claims === 2 ? denied() : db.fetch(url, init),
  });
  const worker = start(client, { concurrency: 2, claimers: 2, async work(claim) { await sleep(20); return claim.id; } });
  await assert.rejects(worker.done, (error) => error instanceof FlowerError && error.status === 403 && error.failure?.code === "FORBIDDEN");
  assert.deepEqual(worker.types(), ["claimed", "completed"]);
  assert.deepEqual(["a", "b", "c"].map((id) => job(db, id).state), ["completed", "pending", "pending"]);
});

test("the worker pools backlog check subscribes to a method examples/workers.ts exposes, reading fields it returns", async () => {
  const page = readFileSync(new URL("../docs/guide/worker-pools.html", import.meta.url), "utf8");
  const block = page.match(/<span>Backlog check<\/span>[\s\S]*?<code class="language-ts">([\s\S]*?)<\/code>/)![1];
  const code = block.replace(/<\/?span[^>]*>/g, "").replaceAll("&gt;", ">").replaceAll("&lt;", "<").replaceAll("&amp;", "&");
  const [, alias, args] = code.match(/client\.subscribe\("([^"]+)", ([^)]+)\)/)!;
  const db = await queue(["a"]);
  const stats = db.query(alias as "jobs.stats", JSON.parse(args));
  for (const [, field] of code.matchAll(/value\.(\w+)/g)) assert.ok(field in stats, field);
  assert.equal(typeof stats.oldestReadyAt, "number");
  assert.ok(page.includes("<code>nextAvailableAt</code>") && "nextAvailableAt" in stats);
});

test("a job arriving at an idle worker costs one claim, not one per claimer", async () => {
  const db = await queue([]);
  const { client, calls } = recording(db);
  const worker = start(client, { concurrency: 4, claimers: 4, batch: 4, work: (claim) => claim.id });
  for (const [index, id] of ["a", "b", "c"].entries()) {
    await sleep(10);
    db.mutate("jobs.enqueue", { id, payload: { id } });
    await until(() => worker.count("completed") === index + 1);
  }
  await worker.stop();
  assert.deepEqual(calls.filter((call) => call.name === "jobs.claim").map((call) => call.args.max), [4, 4, 4]);
});

test("a worker takes several jobs per claim and renews every lease it holds in one call", async () => {
  const db = await queue(["a", "b", "c", "d", "e"]);
  const { client, calls } = recording(db);
  const stopClock = wallClock(db);
  try {
    const worker = start(client, {
      concurrency: 8, claimers: 1, batch: 4, leaseMs: 300,
      // The first two outlive their first lease, which only renewals keep.
      work: async (claim, signal) => { await sleep(claim.id === "a" || claim.id === "b" ? 400 : 10, undefined, { signal }); return claim.id; },
    });
    await until(() => worker.count("completed") === 5);
    await worker.stop();
    assert.equal(worker.count("claimed"), 5);
  } finally { stopClock(); }
  assert.equal(calls.find((call) => call.name === "jobs.claim")?.args.max, 4);
  const renewed = calls.filter((call) => call.name === "jobs.renew").map((call) => (call.args.leases as { id: string }[]).map((lease) => lease.id));
  assert.ok(renewed.length >= 2 && renewed.every((ids) => ids.join() === "a,b"), `renewals ${JSON.stringify(renewed)}`);
  assert.deepEqual(["a", "b", "c", "d", "e"].map((id) => [job(db, id).state, job(db, id).attempts]), Array(5).fill(["completed", 1]));
});

test("a worker takes more jobs at once while its queue holds more than it runs", async () => {
  const ids = Array.from({ length: 120 }, (_, index) => `job-${String(index).padStart(3, "0")}`);
  const db = await queue(ids);
  let running = 0, peak = 0;
  const worker = start(db.client, {
    concurrency: { initial: 2, max: 24 }, batch: 8, adjustEveryMs: 10,
    async work(claim) {
      peak = Math.max(peak, ++running);
      await sleep(20);
      running--;
      return claim.id;
    },
  });
  await until(() => worker.count("completed") === ids.length);
  await worker.stop();
  const limits = worker.events.flatMap((event) => event.type === "limit" ? [event.limit] : []);
  assert.deepEqual([limits[0], limits.at(-1)], [4, 24]);
  assert.ok(peak > 2 && peak <= 24, `peak ${peak}`);
});

test("a job whose provider pushes back stops the worker claiming for a while", async () => {
  const ids = ["a", "b", "c", "d"];
  const db = await queue(ids);
  const claimedAt: number[] = [];
  let throttledAt = 0;
  const worker = start(db.client, {
    concurrency: 1,
    onEvent: (event) => { if (event.type === "claimed") claimedAt.push(Date.now()); },
    work(claim, _signal, control) {
      if (claim.id === "b") {
        throttledAt = Date.now();
        control.throttle(150, "RATE_LIMITED");
      }
      return claim.id;
    },
  });
  await until(() => claimedAt.length === ids.length);
  await worker.stop();
  const next = claimedAt.find((at) => at > throttledAt)!;
  assert.ok(next - throttledAt >= 140, `claimed again ${next - throttledAt} ms after the provider pushed back`);
});

test("a worker cut below its long-running jobs takes more again once work waits and the process keeps up", async () => {
  const db = await queue(["a", "b", "c", "d", "e", "f"]);
  const release = deferred();
  const done: string[] = [];
  let load = 2;
  setTimeout(() => { load = 0; }, 100);
  const worker = start(db.client, {
    concurrency: { min: 1, initial: 2, max: 8 }, claimers: 1, adjustEveryMs: 20,
    health: () => ({ load, reason: load >= 1 ? "event loop 100% busy" : "idle" }),
    async work(claim) {
      // The first two run until the others are done, like watchers or background commands.
      if (claim.id === "a" || claim.id === "b") await release.promise;
      done.push(claim.id);
      if (done.length === 4) release.resolve();
      return claim.id;
    },
  });
  await until(() => done.length === 6);
  await worker.stop();
  assert.deepEqual(done.slice(0, 4).toSorted(), ["c", "d", "e", "f"]);
  assert.ok(worker.events.some((event) => event.type === "limit" && event.limit === 1), "the busy process was cut to one job");
});

test("a claim Flower could not answer is waited out without holding room", async () => {
  const db = await queue(["a"]);
  let claims = 0;
  const unavailable = () => new Response(JSON.stringify({ error: { code: "UNAVAILABLE", message: "No leader" } }), { status: 503, headers: { "content-type": "application/json" } });
  const client = new FlowerClient<typeof app>("http://flower.test", {
    fetch: async (url, init) => !url.endsWith("/v1/watch") && JSON.parse(init.body).name === "jobs.claim" && ++claims === 1 ? unavailable() : db.fetch(url, init),
  });
  const worker = start(client, { concurrency: 1, retry: { attempts: 1 }, work: (claim) => claim.id });
  await until(() => worker.count("completed") === 1);
  await worker.stop();
  assert.deepEqual(worker.types(), ["waiting", "claimed", "completed"]);
});

test("waiting workers line up: a new job wakes the first in line only, and a worker leaves the line when it stops", async () => {
  const db = await queue([]);
  const alpha = recording(db), beta = recording(db);
  const claims = (calls: Call[]) => calls.filter((call) => call.name === "jobs.claim");
  const first = start(alpha.client, { owner: "alpha", wait: true, work: (claim) => claim.id });
  await until(() => claims(alpha.calls).length === 1);
  const second = start(beta.client, { owner: "beta", wait: true, work: (claim) => claim.id });
  await until(() => claims(beta.calls).length === 1);
  assert.deepEqual(claims(beta.calls)[0].args, { owner: "beta", leaseMs: 30_000, max: 1, waitMs: 60_000 });
  db.mutate("jobs.enqueue", { id: "x", payload: {} });
  await until(() => first.count("completed") === 1);
  await sleep(20);
  assert.equal(claims(beta.calls).length, 1, "the job woke alpha alone");
  assert.equal(second.count("claimed"), 0);
  await first.stop();
  assert.deepEqual(claims(alpha.calls).at(-1)?.args, { owner: "alpha", max: 0, waitMs: 0 }, "alpha gave up its place");
  db.mutate("jobs.enqueue", { id: "y", payload: {} });
  await until(() => second.count("completed") === 1);
  await second.stop();
  assert.deepEqual([job(db, "x").lease, job(db, "y").state], [null, "completed"]);
});

test("a stopping worker gives held jobs drainMs, then aborts and fails them", async () => {
  const db = await queue(["server"]);
  let reason: unknown;
  const worker = start(db.client, {
    drainMs: 50,
    async work(_claim, signal) { try { return await leaseEnd(signal); } catch (error) { reason = error; throw error; } },
  });
  await until(() => worker.count("claimed") === 1);
  const stoppedAt = Date.now();
  await worker.stop();
  assert.ok(Date.now() - stoppedAt >= 45, "held jobs had their time");
  assert.equal((reason as Error).message, "The worker stopped before the job finished");
  assert.deepEqual(worker.types(), ["claimed", "failed"]);
  const failed = job(db, "server");
  assert.deepEqual([failed.state, failed.error], ["pending", { message: "The worker stopped before the job finished" }]);
});

test("with release, jobs still running when drainMs ends go back to the queue instead of failing", async () => {
  const db = await queue(["server", "short"]);
  const finish = deferred();
  const worker = start(db.client, {
    concurrency: 2, drainMs: 50, release: true,
    async work(claim, signal) {
      if (claim.id !== "short") return leaseEnd(signal);
      await finish.promise;
      return claim.id;
    },
  });
  await until(() => worker.count("claimed") === 2);
  const stopped = worker.stop();
  finish.resolve();
  await stopped;
  assert.deepEqual(worker.types().sort(), ["claimed", "claimed", "completed", "released"]);
  const released = job(db, "server");
  assert.deepEqual([released.state, released.error, released.attempts, released.lease], ["pending", null, 1, null]);
  assert.equal(job(db, "short").state, "completed");
  await assert.rejects(runQueueWorker(db.client, { queue: "jobs", signal: AbortSignal.abort(), release: true, work: () => null }), /release needs drainMs/);
});

test("a stopping worker gives up on work that ignores its abort, so it stops anyway", async () => {
  const db = await queue(["stuck"]);
  const worker = start(db.client, { drainMs: 20, work: () => new Promise<never>(() => {}) });
  await until(() => worker.count("claimed") === 1);
  const stoppedAt = Date.now();
  await worker.stop();
  const took = Date.now() - stoppedAt;
  assert.ok(took >= 4_900 && took < 7_000, `stopped after ${took} ms`);
  assert.deepEqual(worker.events.filter((event) => event.type === "unreported"), [{ type: "unreported", id: "stuck", error: "The job did not stop when its worker did" }]);
  assert.equal(job(db, "stuck").state, "leased", "its lease runs out on its own");
});

test("a job that goes idle stops counting toward concurrency, so long jobs can't keep out short ones", async () => {
  const db = await queue(["long", "a", "b"]);
  const release = deferred();
  const worker = start(db.client, {
    concurrency: 1,
    async work(claim, _signal, control) {
      if (claim.id !== "long") return claim.id;
      control.idle();
      await release.promise;
      return claim.id;
    },
  });
  await until(() => worker.count("completed") === 2);
  assert.deepEqual(["long", "a", "b"].map((id) => job(db, id).state), ["leased", "completed", "completed"]);
  release.resolve();
  await until(() => worker.count("completed") === 3);
  await worker.stop();
});
