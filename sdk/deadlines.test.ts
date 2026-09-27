import assert from "node:assert/strict";
import { test } from "node:test";
import { collection, define, external, mutation, query, v } from "./index.ts";
import type { Context } from "./index.ts";
import { expiringCollection, queue } from "./temporal.ts";
import { testDatabase } from "./testing.ts";

// Watches of these helpers sleep until the instants they report through
// ctx.changesAt, so each read must report exactly when its answer changes.
const jobs = queue<{ n: number }>("jobs", { lease: { defaultMs: 1_000 } });
const cache = expiringCollection<string>("cache");
const docs = collection("docs", v.object({ text: v.string() }));
const digest = external("digest", { input: (ctx, id: string) => ctx.get(docs, id)?.text ?? null, each: docs });

/** Runs read against a context that records every declared change time. */
function recorded<T>(name: string, read: (ctx: Context) => T) {
  return query(name, (ctx) => {
    const times: number[] = [];
    const recording = { ...ctx, changesAt: (time: number | null) => { if (time !== null) times.push(time); ctx.changesAt(time); } };
    return { value: read(recording as Context), times } as { value: any; times: number[] };
  });
}

const app = define({
  uses: [jobs, cache, digest],
  http: {
    ...jobs.http("jobs", { methods: ["enqueue", "claim"] }),
    ...digest.http("digest"),
    "cache.set": mutation("cache.set", { args: v.object({ key: v.string(), ttl: v.int() }) }, (ctx, input) => {
      cache.set(ctx, input.key, input.key, { afterCreationMs: input.ttl });
      return null;
    }),
    "doc.put": mutation("doc.put", { args: v.string() }, (ctx, id) => { ctx.set(docs, id, { text: id }); return null; }),
    "jobs.ready.times": recorded("jobs.ready.times", (ctx) => jobs.ready(ctx)),
    "jobs.stats.times": recorded("jobs.stats.times", (ctx) => jobs.stats(ctx).nextAvailableAt),
    "jobs.get.times": recorded("jobs.get.times", (ctx) => jobs.get(ctx, "a")?.state ?? null),
    "cache.get.times": recorded("cache.get.times", (ctx) => cache.get(ctx, "k")),
    "cache.scan.times": recorded("cache.scan.times", (ctx) => cache.scan(ctx).length),
    "digest.ready.times": recorded("digest.ready.times", (ctx) => digest.ready(ctx)),
  },
});

test("queues report when delayed jobs and running leases make work available", async () => {
  const db = await testDatabase(app);
  const start = db.now;
  assert.deepEqual(db.query("jobs.ready.times", null), { value: false, times: [] }, "an empty queue changes only on a write");
  db.mutate("jobs.enqueue", { id: "a", payload: { n: 1 }, delayMs: 500 });
  assert.deepEqual(db.query("jobs.ready.times", null), { value: false, times: [start + 500] });
  db.advance(500);
  assert.deepEqual(db.query("jobs.ready.times", null), { value: true, times: [] }, "time only adds ready jobs");
  db.mutate("jobs.claim", { owner: "w" });
  assert.deepEqual(db.query("jobs.ready.times", null), { value: false, times: [db.now + 1_000] }, "a lease ends");
  assert.deepEqual(db.query("jobs.get.times", null), { value: "leased", times: [db.now + 1_000] });
  assert.deepEqual(db.query("jobs.stats.times", null), { value: db.now + 1_000, times: [db.now + 1_000] });
  db.advance(1_000);
  assert.deepEqual(db.query("jobs.get.times", null), { value: "pending", times: [] });
});

test("expiring collections report each live record's expiry", async () => {
  const db = await testDatabase(app);
  db.mutate("cache.set", { key: "k", ttl: 300 });
  db.mutate("cache.set", { key: "other", ttl: 100 });
  assert.deepEqual(db.query("cache.get.times", null), { value: "k", times: [db.now + 300] });
  assert.deepEqual(db.query("cache.scan.times", null).value, 2);
  assert.deepEqual(db.query("cache.scan.times", null).times.sort(), [db.now + 100, db.now + 300]);
  db.advance(300);
  assert.deepEqual(db.query("cache.get.times", null), { value: null, times: [] }, "an expired record stays expired");
});

test("external values report when a lease or retry delay makes a key claimable", async () => {
  const db = await testDatabase(app);
  db.mutate("doc.put", "a");
  assert.deepEqual(db.query("digest.ready.times", null), { value: true, times: [] });
  const [claim] = db.mutate("digest.claim", { owner: "w", leaseMs: 2_000 });
  assert.deepEqual(db.query("digest.ready.times", null), { value: false, times: [db.now + 2_000] });
  const { args, key, owner, attempt } = claim;
  db.mutate("digest.release", { args, key, owner, attempt, delayMs: 700 });
  assert.deepEqual(db.query("digest.ready.times", null), { value: false, times: [db.now + 700] });
});
