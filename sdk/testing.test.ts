import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import pizza from "../examples/goblin-pizza-ts/goblin-pizza.ts";
import { FlowerError, type FlowerClient, type Update } from "./client.ts";
import { collection, define, derive, fail, mutation, participant, query, task, transaction, v } from "./index.ts";
import type { Context } from "./index.ts";
import { canonicalJson, type Json } from "./json.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";

function caught(action: () => unknown): FlowerError {
  let error: unknown;
  assert.throws(action, (thrown) => { error = thrown; return true; });
  assert.ok(error instanceof FlowerError, String(error));
  return error;
}

function fails(action: () => unknown, code: string): FlowerError {
  const error = caught(action);
  assert.equal(error.failure?.code, code, error.message);
  return error;
}

async function next<T>(updates: AsyncGenerator<Update<T>>): Promise<Update<T>> {
  const result = await updates.next();
  assert.equal(result.done, false);
  return result.value as Update<T>;
}

const accounts = collection("accounts", v.object({ balance: v.int({ min: 0 }) }));
const movement = v.object({ id: v.string({ min: 1 }), amount: v.int({ min: 1 }) });
const account = (ctx: Context, id: string) => ctx.get(accounts, id) ?? fail("NO_ACCOUNT", `No account ${id}`, { id });
const ledger = define({
  http: {
    "account.open": mutation("account.open", { args: v.object({ id: v.string({ min: 1 }), balance: v.int({ min: 0 }) }) }, (ctx, { id, balance }) => {
      ctx.set(accounts, id, { balance });
      return balance;
    }),
    "account.close": mutation("account.close", { args: v.string() }, (ctx, id) => { ctx.delete(accounts, id); return null; }),
    "account.debit": mutation("account.debit", { args: movement }, (ctx, { id, amount }) => {
      const { balance } = account(ctx, id);
      if (balance < amount) fail("INSUFFICIENT_FUNDS", `${id} holds only ${balance}`, { balance });
      ctx.set(accounts, id, { balance: balance - amount });
      return balance - amount;
    }),
    "account.credit": mutation("account.credit", { args: movement }, (ctx, { id, amount }) => {
      const { balance } = account(ctx, id);
      ctx.set(accounts, id, { balance: balance + amount });
      return balance + amount;
    }),
    "account.balance": query("account.balance", { args: v.string() }, (ctx, id) => account(ctx, id).balance),
    "account.transfer": transaction("account.transfer", { args: v.object({ from: v.string(), to: v.string(), amount: v.int({ min: 1 }) }) },
      ({ from, to, amount }) => ({
        calls: [participant({ partition: "west" }).call("account.debit", { id: from, amount }), participant({ partition: "east" }).call("account.credit", { id: to, amount })],
        value: { moved: amount },
      })),
  },
});

const notes = collection("notes", v.object({ owner: v.string(), text: v.string() }));
const guarded = define({
  auth: {
    authenticate(_ctx, credentials) {
      if (credentials === null) return null;
      const { user, tenant } = credentials as Record<string, Json>;
      if (typeof user !== "string") fail("BAD_CREDENTIALS", "Credentials need a user");
      return { subject: user, ...(typeof tenant === "string" ? { tenant } : {}) };
    },
  },
  http: {
    "whoami": query("whoami", { access: "public" }, (ctx) => ctx.principal()),
    "note.put": mutation("note.put", { args: v.object({ id: v.string(), text: v.string() }) }, (ctx, note) => {
      ctx.set(notes, note.id, { owner: ctx.principal()!.subject, text: note.text });
      return null;
    }),
    "note.get": query("note.get", { args: v.string(), access: (ctx, principal, id) => ctx.get(notes, id)?.owner === principal?.subject },
      (ctx, id) => ctx.get(notes, id)),
  },
});

const jobs = collection<{ at: number; broken?: boolean }>("timed.jobs").index("due", ["at"]);
const log = collection<{ at: number }>("timed.log");
const timed = define({
  collections: [jobs],
  tasks: [task("work", {
    due: (ctx) => ctx.range(jobs.by("due").range({ limit: 1 })).rows[0]?.value.at ?? null,
    run(ctx) {
      const [row] = ctx.range(jobs.by("due").range({ lte: ctx.now(), limit: 1 })).rows;
      if (row.value.broken) fail("BROKEN_JOB", `Job ${row.key} is broken`, { job: row.key });
      ctx.delete(jobs, row.key);
      ctx.set(log, row.key, { at: ctx.now() });
      return row.key;
    },
  })],
  http: {
    "job.add": mutation("job.add", { args: v.object({ id: v.string(), at: v.int(), broken: v.optional(v.boolean()) }) }, (ctx, { id, ...job }) => {
      ctx.set(jobs, id, job);
      return null;
    }),
    "job.log": query("job.log", (ctx) => Object.fromEntries(ctx.scan(log).map((row) => [row.key, row.value.at]))),
  },
});

test("path mode passes credentials into the isolated module", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-testing-"));
  const entry = join(directory, "guarded.ts");
  const sdk = fileURLToPath(new URL("./index.ts", import.meta.url));
  await writeFile(entry, `import { define, query } from ${JSON.stringify(sdk)};
export default define({
  auth: { authenticate: (_ctx, credentials) => credentials === null ? null : { subject: String(credentials.user) } },
  http: { whoami: query("whoami", (ctx) => ctx.principal()?.subject ?? null) },
});`);
  try {
    const db = await testDatabase(entry, { credentials: { user: "ann" } });
    assert.equal(db.query("whoami"), "ann");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("module and path modes run an application to the same results, errors and state", async () => {
  const entry = fileURLToPath(new URL("../examples/goblin-pizza-ts/goblin-pizza.ts", import.meta.url));
  const scenario = (db: TestDatabase<typeof pizza>) => {
    const results: unknown[] = [];
    const attempt = (action: () => unknown) => {
      try { results.push(action()); } catch (error) {
        const { status, code, message, failure } = error as FlowerError;
        results.push({ status, code, message, failure });
      }
    };
    attempt(() => db.mutate("pizza.setup", { tenants: ["t"], storesPerTenant: 2, stockPerShop: 10, bakeMs: 50, leaseMs: 100 }));
    attempt(() => db.mutate("pizza.order", { id: "o", shop: ["t", "store-0"], quantity: 2 }));
    attempt(() => db.mutate("pizza.order", { id: "o", shop: ["t", "store-0"], quantity: 2 }));
    attempt(() => db.mutate("pizza.order", { id: "p", shop: ["t", "store-1"], quantity: 5 }));
    attempt(() => db.advance(50));
    const job = db.mutate("pizza.claim", { tenant: "t", owner: "drone" })!;
    attempt(() => db.mutate("pizza.deliver", { tenant: "t", id: job.id, owner: job.owner, token: job.token }));
    attempt(() => db.mutate("pizza.deliver", { tenant: "t", id: job.id, owner: job.owner, token: job.token }));
    attempt(() => db.query("pizza.world"));
    return { results, data: db.data, revision: db.revision, now: db.now };
  };
  const module = scenario(await testDatabase(pizza));
  assert.deepEqual(scenario(await testDatabase<typeof pizza>(entry)), module);
  assert.equal((module.results[2] as FlowerError).failure?.code, "ORDER_EXISTS");
  assert.deepEqual((module.results[3] as FlowerError).failure?.details, { path: ["quantity"] });
  assert.equal((module.results[6] as FlowerError).failure?.code, "LEASE_LOST");
});

test("receipts return the original result for a repeated request ID and reject its reuse for other content", async () => {
  const db = await testDatabase(ledger, { partitions: ["west"] });
  db.mutate("account.open", { id: "a", balance: 5 });
  const first = await db.client.mutate("account.credit", { id: "a", amount: 1 }, { requestId: "r1" });
  assert.deepEqual(first, { revision: 2, value: 6, duplicate: false });
  assert.deepEqual(await db.client.mutate("account.credit", { id: "a", amount: 1 }, { requestId: "r1" }), { ...first, duplicate: true });
  assert.equal(db.mutate("account.credit", { id: "a", amount: 1 }, { requestId: "r1" }), 6, "direct calls share the receipts");
  assert.deepEqual([db.revision, db.query("account.balance", "a")], [2, 6]);
  for (const [alias, args] of [["account.credit", { id: "a", amount: 2 }], ["account.debit", { id: "a", amount: 1 }]] as const) {
    await assert.rejects(db.client.mutate(alias, args, { requestId: "r1" }), { status: 409, code: "REQUEST_ID_REUSED" });
  }
  const west = db.partition("west");
  assert.equal(west.mutate("account.open", { id: "a", balance: 0 }, { requestId: "r1" }), 0, "each partition keeps its own receipts");
  db.mutate("account.credit", { id: "a", amount: 1 });
  db.mutate("account.credit", { id: "a", amount: 1 });
  assert.equal(db.query("account.balance", "a"), 8, "calls without a request ID are always new");
});

test("credentials run through the compiled auth hook, and denials are 403 FORBIDDEN with the hook's failure", async () => {
  const db = await testDatabase(guarded, { partitions: ["west"] });
  const denied = (action: () => unknown, code: string) => {
    const error = caught(action);
    assert.deepEqual([error.status, error.code, error.failure?.code], [403, "FORBIDDEN", code]);
  };
  assert.equal(db.query("whoami"), null, "missing credentials are anonymous");
  assert.deepEqual(db.query("whoami", null, { credentials: { user: "ann" } }), { subject: "ann" });
  denied(() => db.mutate("note.put", { id: "n", text: "hi" }), "UNAUTHENTICATED");
  denied(() => db.query("whoami", null, { credentials: { name: "ann" } }), "BAD_CREDENTIALS");
  db.mutate("note.put", { id: "n", text: "hi" }, { credentials: { user: "ann" } });
  assert.deepEqual(db.query("note.get", "n", { credentials: { user: "ann" } }), { owner: "ann", text: "hi" });
  denied(() => db.query("note.get", "n", { credentials: { user: "bob" } }), "FORBIDDEN");
  denied(() => db.query("note.get", 42 as never, { credentials: { user: "ann" } }), "INVALID_ARGUMENT");
  await assert.rejects(db.client.mutate("note.put", { id: "m", text: "x" }),
    (error) => error instanceof FlowerError && error.status === 403 && error.failure?.code === "UNAUTHENTICATED");
  assert.deepEqual((await db.client.query("whoami", null, { credentials: { user: "cy" } })).value, { subject: "cy" });

  const west = db.partition("west");
  assert.deepEqual(west.query("whoami", null, { credentials: { user: "ann", tenant: "west" } }), { subject: "ann", tenant: "west" });
  assert.throws(() => west.query("whoami", null, { credentials: { user: "ann", tenant: "east" } }), { status: 403, code: "FORBIDDEN" });
  assert.equal(west.query("whoami"), null, "anonymous callers act in the partition they call");
});

test("default credentials apply to direct calls and the client, and receipts belong to their principal", async () => {
  const db = await testDatabase(guarded, { credentials: { user: "ann" } });
  assert.deepEqual(db.query("whoami"), { subject: "ann" });
  assert.deepEqual((await db.client.query("whoami")).value, { subject: "ann" });
  assert.deepEqual(db.query("whoami", null, { credentials: { user: "bob" } }), { subject: "bob" });
  assert.equal(db.query("whoami", null, { credentials: null }), null);
  db.mutate("note.put", { id: "n", text: "hi" }, { requestId: "r" });
  assert.throws(() => db.mutate("note.put", { id: "n", text: "hi" }, { requestId: "r", credentials: { user: "bob" } }), { status: 409, code: "REQUEST_ID_REUSED" });
});

test("maintain runs due tasks one commit at a time, and advance moves the clock and maintains every partition", async () => {
  const db = await testDatabase(timed, { now: 0, partitions: ["west"] });
  assert.equal(db.maintain(), 0);
  db.mutate("job.add", { id: "a", at: 100 });
  db.mutate("job.add", { id: "b", at: 200 });
  assert.equal(db.advance(99), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("job.log"), { a: 100 });
  assert.equal(db.advance(1_000), 1);
  assert.deepEqual(db.query("job.log"), { a: 100, b: 1_100 }, "overdue work runs at the current time");
  for (const id of ["c", "d", "e"]) db.mutate("job.add", { id, at: 0 });
  assert.equal(db.maintain(1), 1, "a limit bounds the commits");
  assert.equal(db.maintain(), 2, "continuation hints drain the rest");
  const revision = db.revision;
  db.now += 5;
  assert.equal(db.maintain(), 0);
  assert.equal(db.revision, revision, "idle maintenance commits nothing, not even the clock");

  const west = db.partition("west");
  west.mutate("job.add", { id: "w", at: 1_200 });
  assert.equal(west.maintain(), 0);
  assert.equal(db.advance(95), 1);
  assert.deepEqual(west.query("job.log"), { w: 1_200 });
  assert.deepEqual(db.query("job.log"), { a: 100, b: 1_100, c: 1_100, d: 1_100, e: 1_100 });
  for (const ms of [-1, 0.5, Number.NaN]) assert.throws(() => db.advance(ms), TypeError);
});

test("a failing task hands its real failure code to maintenance error handling, which backs off", async () => {
  const db = await testDatabase(timed, { now: 0 });
  const state = () => (db.data[`source:${canonicalJson(["$flower.tasks", "state"])}`] as { work: { failures: number; retryAt: number; error: Json } } | undefined)?.work;
  db.mutate("job.add", { id: "x", at: 0, broken: true });
  assert.equal(db.maintain(), 1, "the error handler commits the backoff");
  assert.deepEqual(state(), { failures: 1, retryAt: 1_000, error: { code: "BROKEN_JOB", message: "Job x is broken", details: { job: "x" } } });
  assert.equal(db.advance(999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual([state()?.failures, state()?.retryAt], [2, 3_000]);
  db.mutate("job.add", { id: "x", at: 0 });
  assert.equal(db.advance(1_999), 0);
  assert.equal(db.advance(1), 1);
  assert.equal(state(), undefined, "success clears the backoff");
  assert.deepEqual(db.query("job.log"), { x: 3_000 });
});

test("transactions commit across partitions atomically, and a participant failure aborts all of them", async () => {
  const db = await testDatabase(ledger, { partitions: ["west", "east"] });
  const west = db.partition("west"), east = db.partition("east");
  west.mutate("account.open", { id: "alice", balance: 10 });
  east.mutate("account.open", { id: "bob", balance: 0 });
  fails(() => db.query("account.balance", "alice"), "NO_ACCOUNT");
  const balances = () => [west.query("account.balance", "alice"), east.query("account.balance", "bob")];
  const revision = db.revision;
  assert.deepEqual(db.call("account.transfer", { from: "alice", to: "bob", amount: 4 }), { results: [6, 4], value: { moved: 4 } });
  assert.deepEqual(balances(), [6, 4]);
  assert.equal(db.revision, revision + 1);

  const missing = caught(() => db.call("account.transfer", { from: "alice", to: "carol", amount: 1 }));
  assert.deepEqual([missing.status, missing.code, missing.failure], [422, "TRANSACTION_ABORTED", { code: "NO_ACCOUNT", message: "No account carol", details: { id: "carol" } }]);
  assert.deepEqual(balances(), [6, 4], "the debit that already applied in west rolled back");
  const poor = caught(() => db.call("account.transfer", { from: "alice", to: "bob", amount: 100 }));
  assert.deepEqual([poor.code, poor.failure?.code, poor.failure?.details], ["TRANSACTION_ABORTED", "INSUFFICIENT_FUNDS", { balance: 6 }]);
  await assert.rejects(db.client.call("account.transfer", { from: "alice", to: "carol", amount: 1 }),
    (error) => error instanceof FlowerError && error.status === 422 && error.code === "TRANSACTION_ABORTED" && error.failure?.code === "NO_ACCOUNT");
  fails(() => db.call("account.transfer", { from: "alice", to: "bob" } as never), "INVALID_ARGUMENT");
  assert.deepEqual(balances(), [6, 4]);
  assert.equal(db.revision, revision + 1);
  const args = { from: "alice", to: "bob", amount: 1 };
  const receipt = await db.client.call("account.transfer", args, { requestId: "t1" });
  assert.deepEqual(await db.client.call("account.transfer", args, { requestId: "t1" }), { ...receipt, duplicate: true }, "a retried transaction applies once");
  await assert.rejects(db.client.call("account.transfer", { ...args, amount: 2 }, { requestId: "t1" }), { status: 409, code: "REQUEST_ID_REUSED" });
  assert.deepEqual(balances(), [5, 5]);
  assert.equal((await west.client.query("account.balance", "alice")).value, 5);
  assert.throws(() => db.partition("north"), { status: 404, code: "PARTITION_NOT_FOUND" });
});

test("the client queries, mutates and subscribes over the in-process transport, including SSE error events", async () => {
  const db = await testDatabase(ledger, { partitions: ["west"] });
  const untyped = db.client as unknown as FlowerClient;
  assert.deepEqual(await db.client.mutate("account.open", { id: "a", balance: 1 }), { revision: 1, value: 1, duplicate: false });
  const read = await db.client.query("account.balance", "a");
  assert.deepEqual([read.revision, read.value], [1, 1]);
  await assert.rejects(db.client.query("account.balance", "ghost"),
    (error) => error instanceof FlowerError && error.status === 422 && error.code === "EVALUATION_FAILED" && error.failure?.code === "NO_ACCOUNT");
  await assert.rejects(untyped.query("account.nope"), { status: 404, code: "METHOD_NOT_FOUND" });
  await assert.rejects(untyped.mutate("account.balance", "a"), { status: 422, code: "METHOD_KIND_MISMATCH" });
  await assert.rejects(untyped.query("account.balance", 7), (error) => error instanceof FlowerError && error.failure?.code === "INVALID_ARGUMENT");
  await assert.rejects(db.client.newRequestId(), { status: 409, code: "RETENTION_NOT_INITIALIZED" });

  const balance = db.client.subscribe("account.balance", "a");
  try {
    assert.deepEqual(await next(balance), { revision: 1, value: 1, reset: true });
    db.mutate("account.open", { id: "b", balance: 0 });
    db.mutate("account.credit", { id: "a", amount: 2 });
    assert.deepEqual(await next(balance), { revision: 3, value: 3, reset: false }, "unchanged values are not resent");
    db.mutate("account.close", "a");
    await assert.rejects(next(balance), (error) => error instanceof FlowerError && error.status === 422 && error.code === "EVALUATION_FAILED" &&
      error.failure?.code === "NO_ACCOUNT" && error.failure.message === "No account a");
  } finally { await balance.return(undefined); }

  const watch = db.client.watch("account.balance", "ghost");
  await assert.rejects(watch.next(), (error) => error instanceof FlowerError && error.failure?.code === "NO_ACCOUNT", "the first snapshot can fail too");
  const west = db.partition("west");
  west.mutate("account.open", { id: "w", balance: 0 });
  const waiting = west.client.waitUntil("account.balance", "w", (value) => value >= 2, { signal: AbortSignal.timeout(3_000) });
  west.mutate("account.credit", { id: "w", amount: 1 });
  west.mutate("account.credit", { id: "w", amount: 1 });
  assert.deepEqual([(await waiting).revision, (await waiting).value], [3, 2]);
  assert.deepEqual((await (await db.fetch("http://flower.test/v2/query", { body: "{}" })).json()).error.code, "NOT_FOUND");
});

test("an index missing from the manifest fails with UNDECLARED_INDEX until its collection is declared", async () => {
  const tags = collection<{ tag: string }>("tags").index("byTag", ["tag"]);
  const counted = derive("tags.counted", (ctx, tag: string) => ctx.range(tags.by("byTag").range({ prefix: [tag], limit: 10 })).rows.length);
  const http = {
    "tag.put": mutation("tag.put", { args: v.object({ id: v.string(), tag: v.string() }) }, (ctx, { id, tag }) => { ctx.set(tags, id, { tag }); return null; }),
    "tag.query": query("tag.query", { args: v.string() }, (ctx, tag) => ctx.query(tags.by("byTag").eq(tag)).length),
    "tag.range": query("tag.range", { args: v.string() }, (ctx, tag) => ctx.get(counted, tag)),
    "tag.scan": query("tag.scan", { args: v.string() }, (ctx, tag) => ctx.scan(tags, { index: "byTag", prefix: [tag] }).length),
  };
  const aliases = ["tag.query", "tag.range", "tag.scan"] as const;
  const forgetful = await testDatabase(define({ definitions: [counted], http }));
  forgetful.mutate("tag.put", { id: "a", tag: "x" });
  for (const alias of aliases) {
    const error = fails(() => forgetful.query(alias, "x"), "UNDECLARED_INDEX");
    assert.match(error.failure!.message, /Index \[tag\] on tags is not declared/);
  }
  const declared = await testDatabase(define({ collections: [tags], definitions: [counted], http }));
  declared.mutate("tag.put", { id: "a", tag: "x" });
  declared.mutate("tag.put", { id: "b", tag: "y" });
  assert.deepEqual(aliases.map((alias) => declared.query(alias, "x")), [1, 1, 1]);
});

test("receipts cover calls that wrote something, unless the method declines them", async () => {
  const counters = collection<number>("counters");
  const bump = mutation("counter.bump", (ctx) => {
    const next = (ctx.get(counters, "n") ?? 0) + 1;
    ctx.set(counters, "n", next);
    return next;
  });
  const stream = mutation("counter.stream", { receipt: false }, (ctx) => {
    const next = (ctx.get(counters, "n") ?? 0) + 1;
    ctx.set(counters, "n", next);
    return next;
  });
  const peek = mutation("counter.peek", (ctx) => ctx.get(counters, "n"));
  const app = define({ collections: [counters], http: { bump, stream, peek } });
  assert.deepEqual(app.http.stream, { name: "counter.stream", kind: "mutation", receipt: false });
  assert.equal(app.definitions["counter.stream"]!.kind, "mutationMethod");
  assert.equal((app.definitions["counter.stream"] as { receipt?: boolean }).receipt, false);
  assert.throws(() => mutation("counter.bad", { receipt: "no" as never }, () => null), /receipt must be a boolean/);
  const db = await testDatabase(app);
  assert.equal(db.mutate("bump", null, { requestId: "b" }), 1);
  assert.equal(db.mutate("bump", null, { requestId: "b" }), 1, "a write replays its receipt");
  assert.equal(db.mutate("stream", null, { requestId: "s" }), 2);
  assert.equal(db.mutate("stream", null, { requestId: "s" }), 3, "declined receipts run again");
  assert.equal(db.mutate("peek", null, { requestId: "p" }), 3);
  db.mutate("bump", null, { requestId: "later" });
  assert.equal(db.mutate("peek", null, { requestId: "p" }), 4, "a call that wrote nothing runs again");
});
