import assert from "node:assert/strict";
import { test } from "node:test";
import { collection, define, derive, fail, FlowerError, mutation, query, v } from "./index.ts";
import type { Json, MutationContext } from "./index.ts";
import { scheduler } from "./scheduler.ts";
import { testDatabase } from "./testing.ts";

const plain = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const output = collection<Json[]>("output");
const record = (ctx: MutationContext, entry: Json) => ctx.set(output, "log", [...(ctx.get(output, "log") ?? []), entry]);
const log = query("log", (ctx) => ctx.get(output, "log") ?? []);

const publish = mutation("private.publish", { args: v.object({ value: v.int() }) }, (ctx, args) => { record(ctx, args.value); return null; });
const flaky = mutation("private.flaky", { args: v.object({ until: v.int() }) }, (ctx, args) => {
  record(ctx, "discarded");
  if (ctx.now() < args.until) fail("FLAKY", "not yet", { now: ctx.now() });
  record(ctx, "flaky");
  return null;
});
const noop = mutation("private.noop", () => null);
const timers = scheduler("timers", { publish, flaky, noop }, { maxAttempts: 3, retryDelayMs: 10, maxRetryDelayMs: 15 });

const after = mutation("after", { args: v.object({ id: v.string(), delayMs: v.int(), value: v.int() }) }, (ctx, input) =>
  timers.after(ctx, input.id, input.delayMs, "publish", { value: input.value }));
const at = mutation("at", { args: v.object({ id: v.string(), dueAt: v.int(), value: v.int() }) }, (ctx, input) =>
  timers.at(ctx, input.id, input.dueAt, "publish", { value: input.value }));
const flakyAfter = mutation("flakyAfter", { args: v.object({ id: v.string(), until: v.int() }) }, (ctx, input) =>
  timers.after(ctx, input.id, 0, "flaky", { until: input.until }));
const unchecked = mutation("unchecked", { args: v.object({ id: v.string(), args: v.json() }) }, (ctx, input) =>
  timers.at(ctx, input.id, 0, "publish", input.args as never));
const cancel = mutation("cancel", { args: v.string() }, (ctx, id) => timers.cancel(ctx, id));
const retry = mutation("retry", { args: v.object({ id: v.string(), delayMs: v.optional(v.int()) }) }, (ctx, input) => timers.retry(ctx, input.id, input.delayMs));
const get = query("get", { args: v.string() }, (ctx, id) => timers.get(ctx, id));
const scan = query("scan", { args: v.nullable(v.enum(["pending", "failed"])) }, (ctx, state) => timers.scan(ctx, state === null ? {} : { state }));
const app = define({ uses: [timers], http: { after, at, flakyAfter, unchecked, cancel, retry, get, scan, log } });

function rejected(run: () => unknown): FlowerError {
  try { run(); } catch (error) { if (error instanceof FlowerError) return error; throw error; }
  assert.fail("expected a FlowerError");
}

test("a scheduler is a component: timers fire at or after their deadline through composite maintenance", async () => {
  assert.equal(timers.kind, "component");
  assert.deepEqual(plain(app.maintenance), { name: "$flower.maintenance", kind: "mutation", onError: { name: "$flower.maintenance.error", kind: "mutation" } });
  assert.deepEqual(plain(app.collections), [{ name: "timers", indexes: { due: ["state", "dueAt"] } }]);
  assert.equal(Object.values(app.http).some((method) => method.name.startsWith("private.") || method.name.startsWith("$flower.")), false);
  const db = await testDatabase(app);
  const timer = db.mutate("after", { id: "one", delayMs: 100, value: 42 });
  assert.deepEqual(timer, {
    id: "one", state: "pending", handler: "publish", args: { value: 42 }, dueAt: 1_000_100, attempts: 0, error: null, createdAt: 1_000_000, updatedAt: 1_000_000,
  });
  assert.deepEqual(db.query("get", "one"), timer);
  assert.equal(db.advance(99), 0);
  assert.deepEqual(db.query("log"), []);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("log"), [42]);
  assert.equal(db.query("get", "one"), null);
  assert.deepEqual(db.query("scan", null), []);
  assert.equal(db.maintain(), 0);
});

test("at() takes absolute deadlines and timers run one per commit in deadline, then ID, order", async () => {
  const db = await testDatabase(app);
  for (const [id, dueAt, value] of [["b", 1_000_010, 2], ["a", 1_000_010, 1], ["early", 1_000_005, 0], ["late", 1_000_050, 3]] as const) {
    db.mutate("at", { id, dueAt, value });
  }
  assert.deepEqual(db.query("scan", null).map((each) => each.id), ["early", "a", "b", "late"]);
  assert.equal(db.advance(10), 3);
  assert.deepEqual(db.query("log"), [0, 1, 2]);
  assert.deepEqual(db.query("scan", "pending").map((each) => each.id), ["late"]);
  db.mutate("at", { id: "past", dueAt: 0, value: 9 });
  assert.equal(db.maintain(), 1);
  assert.deepEqual(db.query("log"), [0, 1, 2, 9]);
});

test("reusing an ID debounces the earlier timer and cancel() removes pending work", async () => {
  const db = await testDatabase(app);
  db.mutate("after", { id: "debounce", delayMs: 50, value: 1 });
  db.now += 25;
  const replaced = db.mutate("after", { id: "debounce", delayMs: 50, value: 2 });
  assert.deepEqual([replaced.createdAt, replaced.updatedAt, replaced.dueAt, replaced.args], [1_000_000, 1_000_025, 1_000_075, { value: 2 }]);
  assert.equal(db.advance(25), 0);
  assert.equal(db.advance(25), 1);
  assert.deepEqual(db.query("log"), [2]);
  db.mutate("after", { id: "cancelled", delayMs: 10, value: 3 });
  assert.equal(db.mutate("cancel", "cancelled"), true);
  assert.equal(db.mutate("cancel", "cancelled"), false);
  assert.equal(db.query("get", "cancelled"), null);
  assert.equal(db.advance(10), 0);
});

test("handlers may reschedule their own ID", async () => {
  let again: (ctx: MutationContext, remaining: number) => void = () => {};
  const repeat = mutation("private.repeat", { args: v.object({ remaining: v.int() }) }, (ctx, args) => {
    record(ctx, args.remaining);
    if (args.remaining > 0) again(ctx, args.remaining - 1);
    return null;
  });
  const loops = scheduler("loops", { repeat });
  again = (ctx, remaining) => { loops.after(ctx, "repeat", 1, "repeat", { remaining }); };
  const start = mutation("start", (ctx) => loops.after(ctx, "repeat", 0, "repeat", { remaining: 2 }));
  const peek = query("peek", (ctx) => loops.get(ctx, "repeat"));
  const db = await testDatabase(define({ uses: [loops], http: { start, peek, log } }));
  db.mutate("start");
  assert.equal(db.maintain(), 1);
  assert.deepEqual([db.query("peek")!.args, db.query("peek")!.dueAt], [{ remaining: 1 }, 1_000_001]);
  assert.equal(db.advance(1), 1);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("log"), [2, 1, 0]);
  assert.equal(db.query("peek"), null);
});

test("failed handlers retry with capped exponential backoff through the scheduler's onError, then stay failed", async () => {
  const db = await testDatabase(app);
  db.mutate("flakyAfter", { id: "f", until: 2_000_000 });
  db.mutate("after", { id: "healthy", delayMs: 0, value: 7 });
  assert.equal(db.maintain(), 2);
  const state = () => { const { state, attempts, dueAt, updatedAt, error } = db.query("get", "f")!; return { state, attempts, dueAt, updatedAt, error }; };
  assert.deepEqual(state(), { state: "pending", attempts: 1, dueAt: 1_000_010, updatedAt: 1_000_000, error: { code: "FLAKY", message: "not yet", details: { now: 1_000_000 } } });
  assert.deepEqual(db.query("log"), [7], "a failed handler's writes are discarded while healthy timers proceed");
  assert.equal(db.data['source:["$flower.tasks","state"]'], undefined, "the scheduler owns its retry state");
  assert.equal(db.advance(9), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual([state().attempts, state().dueAt], [2, 1_000_025], "the doubled delay is capped by maxRetryDelayMs");
  assert.equal(db.advance(15), 1);
  assert.deepEqual(state(), { state: "failed", attempts: 3, dueAt: 1_000_025, updatedAt: 1_000_025, error: { code: "FLAKY", message: "not yet", details: { now: 1_000_025 } } });
  assert.equal(db.advance(1_000), 0);
  assert.deepEqual(db.query("scan", "failed").map((each) => each.id), ["f"]);
  assert.deepEqual(db.query("scan", "pending"), []);
  const retried = db.mutate("retry", { id: "f", delayMs: 5 });
  assert.deepEqual([retried.state, retried.attempts, retried.error, retried.dueAt, retried.createdAt], ["pending", 0, null, db.now + 5, 1_000_000]);
  assert.deepEqual(rejected(() => db.mutate("retry", { id: "f" })).failure, { code: "TIMER_NOT_FAILED", message: "Only failed timers can be retried" });
  assert.equal(rejected(() => db.mutate("retry", { id: "missing" })).failure!.code, "TIMER_NOT_FAILED");
  db.now = 2_000_000;
  assert.equal(db.maintain(), 1);
  assert.deepEqual(db.query("log"), [7, "discarded", "flaky"]);
});

test("handler argument schemas type scheduling and reject bad arguments when scheduled", async () => {
  const db = await testDatabase(app);
  assert.throws(() => db.mutate("unchecked", { id: "bad", args: { value: "x" } }),
    (error: any) => error.failure.code === "INVALID_ARGUMENT" && /publish arguments: value: must be a finite number/.test(error.failure.message));
  assert.equal(db.query("get", "bad"), null);
  const typed = (ctx: MutationContext) => {
    timers.after(ctx, "id", 0, "noop");
    timers.at(ctx, "id", 0, "publish", { value: 1 });
    // @ts-expect-error unknown handler aliases are rejected
    timers.after(ctx, "id", 0, "missing", { value: 1 });
    // @ts-expect-error arguments follow the handler's schema
    timers.after(ctx, "id", 0, "publish", { value: "1" });
    // @ts-expect-error arguments are required when the handler takes them
    timers.at(ctx, "id", 0, "publish");
    const until: number = timers.after(ctx, "id", 0, "flaky", { until: 1 }).args.until;
    return until;
  };
  void typed;
});

test("several schedulers in one module keep separate records, tasks and retry policies", async () => {
  const fast = scheduler("fast", { flaky }, { maxAttempts: 1 });
  const slow = scheduler("slow", { publish, flaky }, { maxAttempts: 2, retryDelayMs: 100 });
  const both = mutation("both", (ctx) => {
    fast.after(ctx, "x", 0, "flaky", { until: 2_000_000 });
    slow.after(ctx, "x", 0, "flaky", { until: 2_000_000 });
    slow.after(ctx, "y", 5, "publish", { value: 1 });
    return null;
  });
  const view = query("view", (ctx) => ({
    fast: fast.scan(ctx).map(({ id, state, attempts }) => ({ id, state, attempts })),
    slow: slow.scan(ctx).map(({ id, state, attempts, dueAt }) => ({ id, state, attempts, dueAt })),
  }));
  const multi = define({ uses: [fast, slow], http: { both, view, log } });
  assert.deepEqual(multi.collections!.map((each) => each.name), ["fast", "slow"]);
  assert.deepEqual(Object.keys(multi.definitions).filter((name) => name.startsWith("$flower.")).sort(), ["$flower.maintenance", "$flower.maintenance.error"]);
  const db = await testDatabase(multi);
  db.mutate("both");
  assert.equal(db.maintain(), 2);
  assert.deepEqual(db.query("view"), {
    fast: [{ id: "x", state: "failed", attempts: 1 }],
    slow: [{ id: "y", state: "pending", attempts: 0, dueAt: 1_000_005 }, { id: "x", state: "pending", attempts: 1, dueAt: 1_000_100 }],
  });
  assert.equal(db.advance(5), 1);
  assert.deepEqual(db.query("log"), [1]);
  assert.equal(db.advance(95), 1);
  assert.deepEqual(db.query("view").slow, [{ id: "x", state: "failed", attempts: 2, dueAt: 1_000_100 }]);
  assert.throws(() => define({ uses: [scheduler("same", { noop }), scheduler("same", { noop })] }), /Duplicate task "scheduler:same"/);
});

test("timers whose handler was removed fail finitely and cannot be retried", async () => {
  const orphans = scheduler("orphans", { publish }, { maxAttempts: 1 });
  const plant = mutation("plant", (ctx) => {
    const now = ctx.now();
    ctx.set(orphans.records, "orphan", { state: "pending", handler: "gone", args: null, dueAt: now, attempts: 0, error: null, createdAt: now, updatedAt: now });
    return null;
  });
  const revive = mutation("revive", (ctx) => orphans.retry(ctx, "orphan"));
  const schedule = mutation("schedule", (ctx) => orphans.after(ctx, "x", 0, "gone" as never, null as never));
  const peek = query("peek", (ctx) => orphans.get(ctx, "orphan"));
  const db = await testDatabase(define({ uses: [orphans], http: { plant, revive, schedule, peek } }));
  db.mutate("plant");
  assert.equal(db.maintain(), 1);
  assert.deepEqual([db.query("peek")!.state, db.query("peek")!.error!.code], ["failed", "SCHEDULER_HANDLER_MISSING"]);
  const missing = { code: "SCHEDULER_HANDLER_MISSING", message: 'Unknown scheduler handler "gone"' };
  assert.deepEqual(rejected(() => db.mutate("revive")).failure, missing);
  assert.deepEqual(rejected(() => db.mutate("schedule")).failure, missing);
});

test("async and non-JSON handler results are failed attempts whose writes are discarded", async () => {
  const promised = mutation("private.promised", (ctx) => { record(ctx, "discarded"); return Promise.resolve(null) as never; });
  const empty = mutation("private.empty", (ctx) => { record(ctx, "discarded"); return undefined as never; });
  const strict = scheduler("strict", { promised, empty }, { maxAttempts: 1 });
  const start = mutation("start", (ctx) => { strict.after(ctx, "p", 0, "promised"); strict.after(ctx, "u", 0, "empty"); return null; });
  const states = query("states", (ctx) => strict.scan(ctx).map((each) => [each.id, each.state, each.attempts]));
  const db = await testDatabase(define({ uses: [strict], http: { start, states, log } }));
  db.mutate("start");
  assert.equal(db.maintain(), 2);
  assert.deepEqual(db.query("states"), [["p", "failed", 1], ["u", "failed", 1]]);
  assert.deepEqual(db.query("log"), []);
});

test("retry deadline overflow makes the timer fail instead of jamming maintenance", async () => {
  const db = await testDatabase(app, { now: Number.MAX_SAFE_INTEGER - 5 });
  db.mutate("flakyAfter", { id: "f", until: Number.MAX_SAFE_INTEGER });
  assert.equal(db.maintain(), 1);
  const timer = db.query("get", "f")!;
  assert.deepEqual([timer.state, timer.attempts, timer.updatedAt], ["failed", 1, Number.MAX_SAFE_INTEGER - 5]);
  assert.equal(db.maintain(), 0);
});

test("registries snapshot handlers and options, and prototype-like aliases and IDs are safe", async () => {
  const handlers = { ["__proto__"]: publish, constructor: publish, flaky };
  const options = { maxAttempts: 1 };
  const proto = scheduler("proto", handlers, options);
  Reflect.set(handlers, "constructor", mutation("private.other", () => fail("REPLACED", "handlers were snapshotted")));
  options.maxAttempts = 10;
  const start = mutation("start", (ctx) => {
    proto.after(ctx, "__proto__", 0, "__proto__", { value: 1 });
    proto.after(ctx, "constructor", 0, "constructor", { value: 2 });
    proto.after(ctx, "flaky", 0, "flaky", { until: 2_000_000 });
    return null;
  });
  const flakyState = query("flakyState", (ctx) => proto.get(ctx, "flaky")?.state ?? null);
  const db = await testDatabase(define({ uses: [proto], http: { start, log, flakyState } }));
  db.mutate("start");
  assert.equal(db.maintain(), 3);
  assert.deepEqual(db.query("log"), [1, 2]);
  assert.equal(db.query("flakyState"), "failed", "maxAttempts was read at construction");
});

test("scheduler() and its methods validate names, handlers, options and deadlines", () => {
  const run = mutation("run", () => null);
  for (const name of ["", "$flower.timers"]) assert.throws(() => scheduler(name, { run }), TypeError);
  for (const handlers of [null, [], { run: query("q", () => null) }, { run: derive("d", () => null) }, { run: { kind: "mutationMethod", name: "x" } }]) {
    assert.throws(() => scheduler("t", handlers as never), TypeError);
  }
  assert.throws(() => scheduler("t", { "": run }), /nonempty/);
  for (const options of [null, [], { extra: 1 }, { maxAttempts: 0 }, { retryDelayMs: 0 }, { maxAttempts: 1.5 }, { retryDelayMs: Number.NaN },
    { retryDelayMs: Infinity }, { maxAttempts: "3" }]) {
    assert.throws(() => scheduler("t", { run }, options as never), TypeError, JSON.stringify(options));
  }
  assert.throws(() => scheduler("t", { run }, { maxRetryDelayMs: 999 }), RangeError);
  const simple = scheduler("simple", { run });
  const clock = (now: number) => ({ now: () => now }) as unknown as MutationContext;
  for (const delay of [-1, Infinity, Number.NaN, 0.5, "10"]) {
    assert.throws(() => simple.after(clock(0), "one", delay as number, "run"), TypeError);
    assert.throws(() => simple.at(clock(0), "one", delay as number, "run"), TypeError);
  }
  assert.throws(() => simple.after(clock(0), "", 0, "run"), /nonempty/);
  assert.throws(() => simple.after(clock(Number.MAX_SAFE_INTEGER), "overflow", 1, "run"), /Deadline must be a safe integer/);
  assert.throws(() => simple.after(clock(0), "json", 0, "run", { bad: Number.NaN } as never), TypeError);
  assert.throws(() => simple.scan(clock(0), { state: "running" as never }), /Unknown timer state filter/);
  assert.throws(() => simple.retry(clock(0), "x", -1), TypeError);
  assert.deepEqual(plain(simple.records.indexes), { due: ["state", "dueAt"] });
  assert.ok(Object.isFrozen(simple));
});
