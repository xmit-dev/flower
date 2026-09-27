// queue(name, { access }): callers' own reads and writes of job records obey the rule, while
// the queue's bookkeeping acts with the application's rights.
import assert from "node:assert/strict";
import { test } from "node:test";
import { collection, define, FlowerError, mutation, query, v } from "./index.ts";
import { queue } from "./temporal.ts";
import { testDatabase } from "./testing.ts";

const sessions = collection("sessions", v.object({ owner: v.string() }))
  .access(({ principal, row, next, any }) => ({
    read: any(principal.claim("role").eq("worker"), row("owner").eq(principal.subject)),
    insert: next("owner").eq(principal.subject),
  }));
type Payload = { session: string; text: string };
// Whoever may read a job's session may read, enqueue, retry and cancel its jobs; workers any.
const work = queue<Payload, { words: number }>("work", {
  lease: { defaultMs: 100 },
  retry: { maxAttempts: 2, initialDelayMs: 10, maxDelayMs: 10 },
  access: ({ principal, row, next, any, readable }) => {
    const worker = principal.claim("role").eq("worker");
    return {
      read: any(worker, readable(sessions, row("payload", "session"))),
      insert: any(worker, readable(sessions, next("payload", "session"))),
      update: any(worker, readable(sessions, next("payload", "session"))),
      delete: any(worker, readable(sessions, row("payload", "session"))),
    };
  },
});
const id = v.string({ min: 1 });
const methods = {
  start: mutation("start", { args: id }, (ctx, session) => { ctx.set(sessions, session, { owner: ctx.principal()!.subject }); return null; }),
  ask: mutation("ask", { args: v.object({ session: id, id, text: v.string(), delayMs: v.optional(v.int({ min: 0 })) }) },
    (ctx, { session, id, text, delayMs }) => work.enqueue(ctx, id, { session, text }, delayMs === undefined ? {} : { delayMs }).id),
  mine: query("mine", (ctx) => work.scan(ctx).map((job) => [job.id, job.state])),
  job: query("job", { args: id }, (ctx, key) => work.get(ctx, key)?.state ?? null),
  row: query("row", { args: id }, (ctx, key) => ctx.get(work.records, ["", key])?.state ?? null),
  cancelMine: mutation("cancelMine", { args: id }, (ctx, key) => work.cancel(ctx, key)),
  retryMine: mutation("retryMine", { args: id }, (ctx, key) => work.retry(ctx, key).state),
  // An app method that works a job inline for whoever calls it: bookkeeping, not the caller's rights.
  claimInline: mutation("claimInline", { args: v.object({ owner: id, waitMs: v.optional(v.int({ min: 0 })) }) }, (ctx, { owner, waitMs }) =>
    work.claimMany(ctx, owner, { max: 1, ...(waitMs === undefined ? {} : { waitMs }) }).map((claim) => ({ id: claim.id, token: claim.token }))),
  finishInline: mutation("finishInline", { args: v.object({ id, owner: id, token: v.int() }) }, (ctx, lease) => work.complete(ctx, lease, { words: 1 }).state),
  forge: mutation("forge", { args: v.object({ id, session: id }) }, (ctx, { id, session }) => {
    ctx.set(work.records, ["", id], { ...work.get(ctx, id)!, payload: { session, text: "forged" } });
    return null;
  }),
};
const auth = {
  authenticate: (_ctx: unknown, credentials: unknown) => typeof credentials !== "string" ? null
    : credentials === "worker" ? { subject: "w", claims: { role: "worker" } } : { subject: credentials },
  default: "public" as const,
};
const app = define({
  uses: [work],
  collections: [sessions],
  auth,
  http: { ...methods, ...work.http("work", { access: (_ctx, principal) => (principal?.claims as { role?: string } | undefined)?.role === "worker" }) },
});
const alice = { credentials: "alice" }, bob = { credentials: "bob" }, worker = { credentials: "worker" };
const failure = (run: () => unknown) => {
  try { run(); } catch (error) { assert.ok(error instanceof FlowerError, String(error)); return error.failure?.code; }
  assert.fail("expected a failure");
};

test("a queue's access rule reaches the manifest on its records only, with nested payload fields", () => {
  const entries = Object.fromEntries((app.collections ?? []).map((entry) => [entry.name, entry]));
  assert.deepEqual(Object.keys(entries).sort(), ["$flower.fencing", "$flower.turns", "sessions", "work", "work.line"]);
  assert.deepEqual((entries.work.access as any).insert, {
    any: [{ eq: [{ ref: ["principal", "claims", "role"] }, { value: "worker" }] }, { readable: ["sessions", { ref: ["next", "payload", "session"] }] }],
  });
  for (const name of ["work.line", "$flower.turns", "$flower.fencing"]) assert.equal(entries[name].access, undefined, name);
  // Without access, a queue is exactly as before.
  const plain = queue("plain");
  const [records] = define({ uses: [plain] }).collections!.filter((entry) => entry.name === "plain");
  assert.equal(records.access, undefined);
  // Its collections list it as well as uses does.
  const listed = define({ collections: [...work.collections, sessions], http: { ask: methods.ask } });
  assert.ok(listed.collections!.some((entry) => entry.name === "work" && entry.access));
  assert.throws(() => queue("bad", { access: { read: 1 as never } }), /must be a rule, true or false/);
});

test("a guarded queue must be listed, so the server enforces its rule", async () => {
  const unlisted = define({ collections: [sessions], auth, http: { ask: methods.ask, start: methods.start } });
  const db = await testDatabase(unlisted);
  db.mutate("start", "s1", alice);
  assert.throws(() => db.mutate("ask", { session: "s1", id: "j", text: "x" }, alice), /list it in define\(\{ collections \}\)/);
});

test("callers enqueue, read, retry and cancel only the jobs their rule allows", async () => {
  const db = await testDatabase(app);
  db.mutate("start", "s1", alice);
  db.mutate("start", "s2", bob);
  // Enqueue: checked against next, the job written.
  assert.equal(db.mutate("ask", { session: "s1", id: "a1", text: "one two" }, alice), "a1");
  assert.equal(failure(() => db.mutate("ask", { session: "s2", id: "a2", text: "sneak" }, alice)), "ACCESS_DENIED");
  assert.equal(failure(() => db.mutate("ask", { session: "nowhere", id: "a3", text: "lost" }, alice)), "ACCESS_DENIED");
  assert.equal(db.mutate("ask", { session: "s2", id: "b1", text: "three" }, bob), "b1");
  // Whether an ID is taken is the queue's business, whoever asks: a hidden job still exists.
  assert.equal(failure(() => db.mutate("ask", { session: "s1", id: "b1", text: "mine now" }, alice)), "JOB_EXISTS");
  // Reads show the caller's jobs; workers see all.
  assert.deepEqual(db.query("mine", null, alice), [["a1", "pending"]]);
  assert.deepEqual(db.query("mine", null, bob), [["b1", "pending"]]);
  assert.deepEqual(db.query("mine", null, worker), [["a1", "pending"], ["b1", "pending"]]);
  assert.equal(db.query("job", "b1", alice), null);
  assert.equal(db.query("row", "b1", alice), null);
  assert.equal(db.query("work.ready", null, worker), true);
  assert.equal(db.query("work.stats", null, worker).readyCount, 2);
  // A direct write that moves a job to someone else's session is denied.
  assert.equal(failure(() => db.mutate("forge", { id: "a1", session: "s2" }, alice)), "ACCESS_DENIED");
  // Cancel: found whoever asks, deleted as the caller. A hidden job alice may not delete stays.
  assert.equal(db.mutate("cancelMine", "b1", alice), false);
  assert.equal(db.query("job", "b1", bob), "pending");
  assert.equal(db.mutate("cancelMine", "missing", alice), false);
  assert.equal(db.mutate("cancelMine", "b1", bob), true);
  assert.equal(db.query("job", "b1", bob), null);
  // Retry: only failed jobs the caller can read; a hidden one looks missing.
  db.mutate("ask", { session: "s2", id: "b2", text: "fails" }, bob);
  db.mutate("ask", { session: "s1", id: "a4", text: "fails" }, alice);
  for (let round = 0; round < 2; round++) {
    let claimed;
    while ((claimed = db.mutate("work.claim", { owner: "w" }, worker)) !== null) {
      db.mutate("work.fail", { id: claimed.id, owner: "w", token: claimed.token, error: { round } }, worker);
    }
    db.now += 10;
  }
  assert.equal(db.query("job", "b2", bob), "failed");
  assert.equal(failure(() => db.mutate("retryMine", "b2", alice)), "JOB_NOT_FAILED");
  assert.equal(db.mutate("retryMine", "b2", bob), "pending");
  assert.equal(db.query("job", "a1", alice), "failed");
});

test("workers claim, renew, report and reclaim whatever the rule, and the line and turns keep working", async () => {
  const db = await testDatabase(app);
  db.mutate("start", "s1", alice);
  db.mutate("start", "s2", bob);
  db.mutate("ask", { session: "s1", id: "a1", text: "one" }, alice);
  db.mutate("ask", { session: "s2", id: "b1", text: "two" }, bob);
  db.mutate("ask", { session: "s2", id: "b2", text: "later", delayMs: 50 }, bob);
  const first = db.mutate("work.claim", { owner: "w", max: 2, waitMs: 1_000 }, worker)!;
  assert.deepEqual([first.id, first.more?.map((claim) => claim.id)], ["a1", ["b1"]]);
  assert.deepEqual(db.mutate("work.renew", { leases: [{ id: "a1", owner: "w", token: first.token }], leaseMs: 200 }, worker), [1_000_200]);
  const done = db.mutate("work.complete", { id: "a1", owner: "w", token: first.token, result: { words: 1 } }, worker);
  assert.equal(done.state, "completed");
  assert.deepEqual(db.query("mine", null, alice), [["a1", "completed"]]);
  // The delayed job joins the turn order when its time comes; an expired lease is reclaimed.
  db.now += 150;
  const next = db.mutate("work.claim", { owner: "w", max: 2 }, worker)!;
  assert.deepEqual([next.id, ...next.more!.map((claim) => claim.id)].sort(), ["b1", "b2"]);
  db.now += 1_000;
  db.maintain();
  assert.deepEqual(db.query("mine", null, bob), [["b1", "failed"], ["b2", "pending"]]);
});

test("the queue's bookkeeping acts for the application even when the caller's rule wouldn't allow it", async () => {
  const db = await testDatabase(app);
  db.mutate("start", "s1", alice);
  db.mutate("start", "s2", bob);
  db.mutate("ask", { session: "s2", id: "b1", text: "bob's" }, bob);
  db.mutate("ask", { session: "s2", id: "b2", text: "later", delayMs: 10 }, bob);
  db.now += 10;
  // alice can't see bob's jobs, yet an app method working jobs inline for her promotes the
  // delayed one, leases the next and bumps the fencing token and turn.
  const [claim] = db.mutate("claimInline", { owner: "alice-inline", waitMs: 100 }, alice);
  assert.deepEqual(claim, { id: "b1", token: 1 });
  assert.equal(db.query("job", "b1", bob), "leased");
  assert.equal(db.query("job", "b1", alice), null);
  assert.equal(db.mutate("finishInline", { id: "b1", owner: "alice-inline", token: 1 }, alice), "completed");
  assert.equal(db.query("job", "b1", bob), "completed");
  // Her own view of bob's jobs is still empty.
  assert.deepEqual(db.query("mine", null, alice), []);
});
