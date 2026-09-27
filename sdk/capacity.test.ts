import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { Limiter, processHealth } from "./capacity.ts";

const idle = () => ({ load: 0, reason: "idle" });

test("a limit doubles while work waits and the process keeps up, then grows by a sixteenth after a cut; it shrinks when the process falls behind", () => {
  let now = 0;
  let load = { load: 0.1, reason: "event loop 9% busy" };
  const limiter = new Limiter({ min: 2, max: 40, initial: 4 }, () => load, () => now);
  assert.equal(limiter.adjust(), null, "no demand, no growth");
  limiter.want();
  assert.deepEqual(limiter.adjust(), { limit: 8, reason: "more work is waiting" });
  for (const expected of [16, 32, 40, 40]) {
    limiter.want();
    limiter.adjust();
    assert.equal(limiter.limit, expected);
  }
  load = { load: 1.08, reason: "event loop 97% busy" };
  assert.deepEqual(limiter.adjust(), { limit: 30, reason: "event loop 97% busy" });
  assert.equal(limiter.adjust(), null, "one cut per second");
  now += 1_000;
  limiter.adjust();
  assert.equal(limiter.limit, 22);
  load = { load: 0.1, reason: "event loop 9% busy" };
  now += 1_000;
  limiter.want();
  limiter.adjust();
  assert.equal(limiter.limit, 23, "right after a cut the limit grows by a sixteenth, at least one");
  now += 5_000;
  limiter.want();
  limiter.adjust();
  assert.equal(limiter.limit, 40, "then doubles again, up to max");
  limiter.throttle(0, "RATE_LIMITED");
  assert.equal(limiter.limit, 20);
  now += 29_000;
  limiter.want();
  limiter.adjust();
  assert.equal(limiter.limit, 21, "under a provider's limit it settles for longer");
  now += 1_000;
  limiter.want();
  limiter.adjust();
  assert.equal(limiter.limit, 40);
  const busy = new Limiter({ initial: 32, max: 64 }, () => ({ load: 0.7, reason: "event loop 63% busy" }), () => now);
  busy.want();
  assert.deepEqual(busy.adjust(), { limit: 34, reason: "more work is waiting" }, "a process past half its load grows by a sixteenth");
});

test("a provider pushing back halves the limit and pauses claims, even a fixed one's", () => {
  let now = 0;
  const limiter = new Limiter({ min: 1, max: 64, initial: 32 }, idle, () => now);
  assert.deepEqual(limiter.throttle(5_000, "RATE_LIMITED"), { limit: 16, reason: "RATE_LIMITED" });
  assert.equal(limiter.pause, 5_000);
  assert.equal(limiter.throttle(1_000, "RATE_LIMITED"), null, "a burst of refusals cuts once");
  limiter.want();
  assert.equal(limiter.adjust(), null, "no growth while paused");
  now += 5_000;
  assert.equal(limiter.pause, 0);
  const fixed = new Limiter(8, () => ({ load: 3, reason: "heap 99% full" }), () => now);
  assert.equal(fixed.throttle(2_000, "OVERLOADED"), null);
  assert.equal(fixed.pause, 2_000);
  fixed.want();
  assert.equal(fixed.adjust(), null);
  assert.equal(fixed.limit, 8);
});

test("bounds default to 1..16 starting at min, and must be whole and ordered", () => {
  const limiter = new Limiter({}, idle);
  assert.deepEqual([limiter.min, limiter.limit, limiter.max], [1, 1, 16]);
  assert.equal(new Limiter({ min: 32 }, idle).max, 32);
  for (const bad of [0, { min: 0 }, { min: 4, max: 2 }, { initial: 20 }, { max: 1.5 }]) assert.throws(() => new Limiter(bad, idle), TypeError);
});

test("the process's health reads its event loop and memory since the last read, and each read names what loads it most", async () => {
  // The event loop reports how busy it is once it runs.
  await sleep(1);
  const health = processHealth();
  const start = Date.now();
  while (Date.now() - start < 30);
  const busy = health();
  assert.ok(busy.load > 0.5, `a spinning loop reads as busy: ${JSON.stringify(busy)}`);
  assert.match(busy.reason, /^event loop \d+% busy$/);
  assert.match(processHealth({ busy: 1e9, memory: 1e-9 })().reason, /^(?:heap \d+% full|memory \d+% used)$/);
  assert.throws(() => processHealth({ busy: 0 }), TypeError);
});
