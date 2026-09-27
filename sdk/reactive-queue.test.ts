import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep, setImmediate as tick } from "node:timers/promises";
import app from "../examples/workers.ts";
import { FlowerError, type Update } from "./client.ts";
import { canonicalJson, type Json } from "./json.ts";
import type { Claim, Job } from "./temporal.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";

const T0 = 1_000_000;
const workers = () => testDatabase(app, { now: T0 });
type Db = TestDatabase<typeof app>;
const lease = (job: Claim) => ({ id: job.id, owner: job.owner, token: job.token });
const stored = (db: Db, id: string) => db.data[`source:${canonicalJson(["workerJobs", canonicalJson(["", id])])}`] as unknown as Job;

function claim(db: Db, owner: string, leaseMs = 100) {
  const job = db.mutate("jobs.claim", { owner, leaseMs });
  assert.ok(job, `${owner} found nothing to claim`);
  return job;
}

function fails(action: () => unknown, code: string): FlowerError {
  let caught: unknown;
  assert.throws(action, (error) => { caught = error; return true; });
  assert.ok(caught instanceof FlowerError, String(caught));
  assert.equal(caught.failure?.code, code, caught.message);
  return caught;
}

async function until(condition: () => boolean) {
  for (const started = Date.now(); !condition(); await sleep(2)) {
    if (Date.now() - started > 3_000) throw new Error("Timed out waiting for the workers");
  }
}

async function next<T>(updates: AsyncGenerator<Update<T>>): Promise<T> {
  const result = await updates.next();
  assert.equal(result.done, false);
  return (result.value as Update<T>).value;
}

test("readiness follows pending work and lease expiry without waiting for a sweep", async () => {
  const db = await workers();
  assert.equal(db.query("jobs.ready"), false);
  db.mutate("jobs.enqueue", { id: "one", payload: { work: true } });
  assert.equal(db.query("jobs.ready"), true);

  const first = claim(db, "worker-a");
  assert.equal(db.query("jobs.ready"), false);
  db.now = first.expiresAt - 1;
  assert.equal(db.query("jobs.ready"), false);
  db.now = first.expiresAt;
  assert.equal(db.query("jobs.ready"), true, "expiry alone makes the job claimable");
  assert.equal(stored(db, "one").state, "leased", "no reclaim task has run");

  const second = claim(db, "worker-b");
  assert.deepEqual([second.id, second.attempt], ["one", 2]);
  assert.ok(second.token > first.token);
  assert.equal(db.query("jobs.ready"), false);
  fails(() => db.mutate("jobs.complete", { ...lease(first), result: "stale" }), "LEASE_LOST");
  db.mutate("jobs.complete", { ...lease(second), result: "finished" });
  db.now = second.expiresAt;
  assert.equal(db.query("jobs.ready"), false, "finished work never becomes ready");
});

test("a readiness subscription wakes for new work, delayed work reaching its time and expired leases", async () => {
  const db = await workers();
  const ready = db.client.subscribe("jobs.ready", null);
  try {
    assert.equal(await next(ready), false);
    db.mutate("jobs.enqueue", { id: "later", payload: null, delayMs: 500 });
    assert.deepEqual(db.query("jobs.stats"), { ready: false, oldestReadyAt: null, nextAvailableAt: T0 + 500, readyCount: 0, leasedCount: 0, delayedCount: 1 });
    db.advance(499);
    db.advance(1);
    assert.equal(await next(ready), true, "the delayed job became ready as time advanced");

    const first = claim(db, "worker-a");
    assert.equal(await next(ready), false);
    assert.deepEqual(db.query("jobs.stats"), { ready: false, oldestReadyAt: null, nextAvailableAt: first.expiresAt, readyCount: 0, leasedCount: 1, delayedCount: 0 });
    db.advance(100);
    assert.equal(await next(ready), true, "the expired lease woke the subscription");
    assert.equal(stored(db, "later").state, "pending", "maintenance reclaimed the lease");
    const second = claim(db, "worker-b");
    assert.equal(await next(ready), false);
    db.mutate("jobs.complete", { ...lease(second), result: null });
    db.mutate("jobs.enqueue", { id: "now", payload: null });
    assert.equal(await next(ready), true, "equal values are suppressed, so the next change is the new job");
  } finally { await ready.return(undefined); }
});

test("workers driven by a readiness subscription claim each job exactly once as work becomes ready", async () => {
  const db = await workers();
  const stop = new AbortController();
  const claimed: string[][] = [[], []];
  const loops = [0, 1].map(async (index) => {
    for await (const { value: ready } of db.client.subscribe("jobs.ready", null, { signal: stop.signal })) {
      if (!ready) continue;
      for (let job; (job = (await db.client.mutate("jobs.claim", { owner: `worker-${index}` })).value);) {
        claimed[index].push(job.id);
        await db.client.mutate("jobs.complete", { ...lease(job), result: index });
      }
    }
  });
  const completed = (ids: string[]) => ids.every((id) => db.query("jobs.get", { id })?.state === "completed");
  for (const id of ["a", "b", "c"]) db.mutate("jobs.enqueue", { id, payload: null });
  db.mutate("jobs.enqueue", { id: "later", payload: null, delayMs: 1_000 });
  await until(() => completed(["a", "b", "c"]));
  assert.equal(db.query("jobs.get", { id: "later" })?.state, "pending");
  db.advance(1_000);
  await until(() => completed(["later"]));
  stop.abort();
  await Promise.all(loops);
  assert.deepEqual(claimed.flat().sort(), ["a", "b", "c", "later"]);
  assert.equal(db.query("jobs.ready"), false);
});

test("a worker blocked on readiness wakes when a lease expires and fences out the old owner", async () => {
  const db = await workers();
  db.mutate("jobs.enqueue", { id: "one", payload: null });
  const first = claim(db, "worker-a");
  const woke = db.client.waitUntil("jobs.ready", null, Boolean, { signal: AbortSignal.timeout(3_000) });
  let state = "waiting";
  void woke.then(() => { state = "woke"; });
  db.advance(99);
  for (let index = 0; index < 10; index++) await tick();
  assert.equal(state, "waiting");
  db.advance(1);
  assert.equal((await woke).value, true);

  const second = claim(db, "worker-b");
  assert.deepEqual([second.id, second.attempt], [first.id, 2]);
  assert.ok(second.token > first.token);
  assert.deepEqual(db.mutate("jobs.renew", { leases: [lease(first)] }), [null]);
  fails(() => db.mutate("jobs.fail", { ...lease(first), error: "stale" }), "LEASE_LOST");
  db.mutate("jobs.complete", { ...lease(second), result: "done" });
  assert.deepEqual([db.query("jobs.get", { id: "one" })?.state, db.query("jobs.get", { id: "one" })?.result], ["completed", "done"]);
});

test("readiness stays true until drained, failed work returns after its backoff, and final failures need a retry", async () => {
  const db = await workers();
  for (const id of ["one", "two"]) db.mutate("jobs.enqueue", { id, payload: id });
  const first = claim(db, "worker-a");
  assert.equal(db.query("jobs.ready"), true, "another pending job keeps the queue ready");
  const second = claim(db, "worker-b");
  assert.equal(db.query("jobs.ready"), false);
  db.mutate("jobs.complete", { ...lease(first), result: null });

  const failed = db.mutate("jobs.fail", { ...lease(second), error: { code: "EXTERNAL_FAILURE" } });
  assert.deepEqual([failed.state, failed.attempts, failed.availableAt], ["pending", 1, T0 + 1_000]);
  assert.deepEqual(db.query("jobs.stats"), { ready: false, oldestReadyAt: null, nextAvailableAt: T0 + 1_000, readyCount: 0, leasedCount: 0, delayedCount: 1 });
  db.now += 999;
  assert.equal(db.query("jobs.ready"), false);
  db.now += 1;
  assert.equal(db.query("jobs.ready"), true);
  const third = claim(db, "worker-c");
  assert.deepEqual([third.id, third.attempt], ["two", 2]);

  const final = db.mutate("jobs.fail", { ...lease(third), error: { code: "PERMANENT" }, retry: false });
  assert.deepEqual([final.state, final.availableAt], ["failed", null]);
  db.now += 3_600_000;
  assert.equal(db.query("jobs.ready"), false, "failed work does not come back on its own");
  fails(() => db.mutate("jobs.retry", { id: "one" }), "JOB_NOT_FAILED");
  db.mutate("jobs.retry", { id: "two" });
  assert.equal(db.query("jobs.ready"), true);
  const retried = claim(db, "worker-d");
  assert.deepEqual([retried.id, retried.attempt], ["two", 1], "a retry starts a fresh attempt budget");
  assert.equal(db.query("jobs.ready"), false);
});

test("a lease that keeps expiring spends the attempt budget and then fails for good", async () => {
  const db = await workers();
  db.mutate("jobs.enqueue", { id: "doomed", payload: null });
  for (let attempt = 1; attempt <= 5; attempt++) {
    assert.equal(claim(db, `worker-${attempt}`).attempt, attempt);
    db.advance(100);
  }
  const job = db.query("jobs.get", { id: "doomed" }) as Job<Json, Json>;
  assert.deepEqual([job.state, job.attempts, (job.error as { code: string }).code], ["failed", 5, "LEASE_EXPIRED"]);
  assert.equal(db.query("jobs.ready"), false);
  assert.equal(db.mutate("jobs.claim", { owner: "worker-6" }), null);
});
