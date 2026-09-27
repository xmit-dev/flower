import assert from "node:assert/strict";
import test from "node:test";
import { createRandom, Histogram, HISTOGRAM_PROPERTIES, Stats, summarizeApplication } from "./metrics.mjs";

test("empty histograms and zero-duration runs have finite, unambiguous results", () => {
  const histogram = new Histogram();
  assert.deepEqual(histogram.snapshot(), {
    buckets: Array(HISTOGRAM_PROPERTIES.bucketCount).fill(0),
    samples: 0, invalidSamples: 0, overflowSamples: 0, approximate: true,
    min: null, p50: null, p95: null, p99: null, max: null,
  });
  const stats = new Stats();
  stats.record("read", { latencyMs: 0, ok: true });
  const result = stats.snapshot(0);
  assert.equal(result.attemptsPerSecond, 0);
  assert.equal(result.throughputPerSecond, 0);
  assert.equal(result.latencyMs.p99, 0);
  assert.doesNotThrow(() => JSON.stringify(result));
});

test("histogram percentiles bound exact nearest ranks within the documented precision", () => {
  const histogram = new Histogram();
  for (let value = 1; value <= 1_000; value++) histogram.record(value);
  for (const percentile of [1, 10, 50, 95, 99]) {
    const exact = Math.ceil(percentile / 100 * 1_000);
    assert.ok(histogram.percentile(percentile) >= exact);
    assert.ok(histogram.percentile(percentile) <= exact * 1.01);
  }
  assert.equal(histogram.percentile(0), 1);
  assert.equal(histogram.percentile(100), 1_000);
  assert.equal(histogram.snapshot().max, 1_000);
});

test("zero, sub-microsecond, repeated, and overflow timings stay ordered", () => {
  const histogram = new Histogram();
  for (const value of [0, 0, 0.0001, 0.0002, 0.001, 0.001, 1, 1, 1e9, 2e9]) histogram.record(value);
  assert.equal(histogram.percentile(20), 0);
  assert.equal(histogram.percentile(30), 0.001);
  assert.equal(histogram.percentile(50), 0.001);
  assert.equal(histogram.percentile(90), 2e9);
  assert.equal(histogram.snapshot().overflowSamples, 2);
  assert.equal(histogram.snapshot().samples, 10);
  const singleton = new Histogram();
  singleton.record(0.0001);
  assert.equal(singleton.percentile(99), 0.0001);
});

test("invalid samples cannot poison latency statistics", () => {
  const histogram = new Histogram();
  for (const value of [NaN, Infinity, -Infinity, -1, undefined, null, "12"]) {
    assert.equal(histogram.record(value), false);
  }
  assert.equal(histogram.record(12), true);
  assert.equal(histogram.snapshot().samples, 1);
  assert.equal(histogram.snapshot().invalidSamples, 7);
  assert.equal(histogram.percentile(50), 12);
  for (const percentile of [NaN, Infinity, -1, 101]) {
    assert.throws(() => histogram.percentile(percentile), RangeError);
  }
});

test("histogram allocation is independent of the number of requests", () => {
  const histogram = new Histogram();
  for (let i = 0; i < 100_000; i++) histogram.record(i % 250);
  assert.equal(histogram.snapshot().samples, 100_000);
  assert.ok(HISTOGRAM_PROPERTIES.bucketCount < 3_000);
  assert.ok(JSON.stringify(histogram.snapshot()).length < 20_000);
});

test("seeded workloads are repeatable, bounded, and differ with the seed", () => {
  const first = createRandom("goblin-pizza");
  const second = createRandom("goblin-pizza");
  const different = createRandom("dragon-pizza");
  const values = Array.from({ length: 1_000 }, first);
  assert.deepEqual(values, Array.from({ length: 1_000 }, second));
  assert.notDeepEqual(values, Array.from({ length: 1_000 }, different));
  assert.ok(values.every((value) => value >= 0 && value < 1));
  assert.equal(createRandom(0)(), createRandom(0)());
  for (const seed of [undefined, {}, NaN, Infinity]) assert.throws(() => createRandom(seed), TypeError);
});

test("attempts, retries, duplicates, logical completions, and lateness are separate", () => {
  const stats = new Stats({ methods: ["order", "read"], operations: ["order"] });
  stats.record("order", { latencyMs: 10, ok: false, status: 503 });
  stats.record("order", { latencyMs: 20, ok: true, retry: true, duplicate: true });
  stats.record("read", { latencyMs: 5, ok: true });
  stats.recordOperation("order", { latencyMs: 45, ok: true, duplicate: true });
  stats.recordOperation("order", { latencyMs: 30, ok: false });
  stats.recordLateness(7);
  const result = stats.snapshot(2_000);
  assert.equal(result.attempts, 3);
  assert.equal(result.successes, 2);
  assert.equal(result.failures, 1);
  assert.equal(result.retries, 1);
  assert.equal(result.duplicates, 1);
  assert.equal(result.attemptsPerSecond, 1.5);
  assert.equal(result.throughputPerSecond, 1);
  assert.deepEqual(result.errors, { HTTP_503: 1 });
  assert.equal(result.perMethod.order.attempts, 2);
  assert.equal(result.perMethod.read.successes, 1);
  assert.equal(result.operations.count, 2);
  assert.equal(result.operations.completed, 1);
  assert.equal(result.operations.failed, 1);
  assert.equal(result.operations.duplicates, 1);
  assert.equal(result.operations.throughputPerSecond, 0.5);
  assert.equal(result.operations.latencyMs.max, 45);
  assert.equal(result.timerLatenessMs.max, 7);
});

test("unknown methods and arbitrary failure labels have bounded cardinality", () => {
  const stats = new Stats({ methods: ["read"], maxMethods: 1, maxOperations: 2 });
  stats.record("read", { latencyMs: 1, ok: true });
  for (let i = 0; i < 1_000; i++) {
    stats.record(`untrusted-${i}`, { latencyMs: 2, ok: false, status: `error-${i}` });
    stats.recordOperation(`operation-${i}`, { latencyMs: 3, ok: true });
  }
  stats.record("unknown", { latencyMs: 2, ok: false });
  stats.record("unknown", { latencyMs: 2, ok: false, status: "timeout" });
  stats.record("unknown", { latencyMs: 2, ok: false, status: "aborted" });
  assert.deepEqual(Object.keys(stats.snapshot(1_000).perMethod), ["(other)", "read"]);
  assert.deepEqual(stats.snapshot(1_000).errors, { aborted: 1, network: 1, other: 1_000, timeout: 1 });
  assert.equal(Object.keys(stats.snapshot(1_000).operations.perMethod).length, 3);
});

test("special property names are safe and invalid measurements do not partially count", () => {
  const stats = new Stats({ methods: ["__proto__", "constructor"] });
  stats.record("__proto__", { latencyMs: 1, ok: true });
  stats.record("constructor", { latencyMs: NaN, ok: false, status: 500 });
  assert.throws(() => stats.record("__proto__", { latencyMs: 3, ok: "yes" }), TypeError);
  const result = stats.snapshot(100);
  assert.equal(result.perMethod.__proto__.successes, 1);
  assert.equal(result.perMethod.constructor.failures, 1);
  assert.equal(result.attempts, 2);
  assert.equal(result.latencyMs.invalidSamples, 1);
  assert.equal({}.successes, undefined);
  for (const duration of [-1, NaN, Infinity]) assert.throws(() => stats.snapshot(duration), RangeError);
  assert.throws(() => new Stats({ maxMethods: 0 }), RangeError);
  assert.throws(() => new Stats({ methods: ["one", "two"], maxMethods: 1 }), RangeError);
});

test("application goodput excludes worker polls, explicit replays, and failed calls", () => {
  const report = {
    passed: true,
    phases: { load: {
      durationMs: 2_000,
      attempts: 40_000, successes: 30_000, throughputPerSecond: 15_000,
      operations: { completed: 29_000, perMethod: {
        "pizza.shop": { count: 14_100, completed: 14_000, failed: 100 },
        "pizza.order": { count: 101, completed: 100, failed: 1 },
        "pizza.tip": { count: 5_902, completed: 5_900, failed: 2, duplicates: 50 },
        "pizza.tip.replay": { count: 2_000, completed: 2_000, failed: 0 },
        "pizza.claim": { count: 6_010, completed: 6_000, failed: 10 },
        "pizza.deliver": { count: 1_001, completed: 1_000, failed: 1 },
      } },
    } },
  };
  const summary = summarizeApplication(report);
  assert.equal(summary.completed, 20_000);
  assert.equal(summary.count, 20_103);
  assert.equal(summary.failed, 103);
  assert.equal(summary.goodputRps, 10_000);
  assert.equal(summary.reads.fraction, 0.7);
  assert.equal(summary.mutations.fraction, 0.3);
  assert.equal(summary.excludedLogicalCompletions, 9_000);
  assert.equal(summary.excludedReplayCompletions, 2_000);
  assert.equal(summary.excludedWorkerCompletions, 7_000);
  assert.equal(summary.status, "valid");
  assert.equal(Object.hasOwn(summary, "targetRps"), false);
  report.passed = false;
  assert.equal(summarizeApplication(report).status, "invalid");
});

test("missing application measurements are unavailable and worker-only traffic is zero goodput", () => {
  const empty = summarizeApplication({});
  assert.equal(empty.status, "unmeasured");
  assert.equal(empty.goodputRps, null);
  assert.equal(empty.completed, null);
  assert.equal(empty.reads.fraction, null);
  const workers = summarizeApplication({ passed: true, phases: { load: {
    durationMs: 1_000, operations: { completed: 20_000, perMethod: { "pizza.claim": { completed: 20_000 } } },
  } } });
  assert.equal(workers.goodputRps, 0);
  assert.equal(workers.status, "valid");
  assert.equal(workers.mutations.fraction, null);
  assert.equal(workers.excludedWorkerCompletions, 20_000);
  assert.equal(summarizeApplication({ phases: { load: { durationMs: 0, operations: { perMethod: {} } } } }).goodputRps, null);
});

test("histogram merge rejects invalid counters and incoherent bounds without mutating the destination", () => {
  const source = new Histogram(); source.record(10); source.record(20);
  const destination = new Histogram(); destination.record(5);
  const before = destination.snapshot();
  for (const alter of [
    (r) => { delete r.invalidSamples; },
    (r) => { r.invalidSamples = -1; },
    (r) => { r.invalidSamples = 0.5; },
    (r) => { r.samples = Number.MAX_SAFE_INTEGER + 1; },
    (r) => { r.min = -1; },
    (r) => { r.max = Infinity; },
    (r) => { r.max = 0; },
    (r) => { r.min = 1; }, // Valid number, wrong first populated bucket.
    (r) => { r.max = 1000; }, // Wrong last populated bucket.
    (r) => { r.overflowSamples = 1; },
  ]) {
    const invalid = source.snapshot(); alter(invalid);
    assert.throws(() => destination.merge(invalid), /incompatible/);
    assert.deepEqual(destination.snapshot(), before);
  }
  const empty = new Histogram().snapshot(); empty.min = 0;
  assert.throws(() => destination.merge(empty), /incompatible/);
  assert.deepEqual(destination.snapshot(), before);
  const invalidOnly = new Histogram(); invalidOnly.record(NaN);
  destination.merge(invalidOnly.snapshot());
  assert.equal(destination.snapshot().invalidSamples, 1);
});

test("disjoint native intervals merge exactly like recording the combined stream", () => {
  const destination = new Stats(), expected = new Stats();
  const random = createRandom("native merge");
  for (let interval = 0; interval < 3; interval++) {
    const source = new Stats();
    for (let i = 0; i < 100; i++) {
      const name = ["pizza.shop", "pizza.tip", "pizza.tip.replay"][i % 3];
      const measurement = { latencyMs: random() * 1000, ok: i % 7 !== 0, status: i % 14 ? 503 : "timeout", retry: i % 5 === 0, duplicate: i % 7 !== 0 && i % 9 === 0 };
      for (const stats of [source, expected]) { stats.record(name, measurement); stats.recordOperation(name, measurement); }
    }
    source.recordLateness(interval); expected.recordLateness(interval);
    destination.merge(source.snapshot(1000));
  }
  assert.deepEqual(destination.snapshot(3000), expected.snapshot(3000));
});

test("native interval validation is atomic across attempt and logical series", () => {
  const destination = new Stats();
  const source = new Stats();
  const sample = { latencyMs: 10, ok: false, status: 503, retry: false, duplicate: false };
  destination.record("old", { ...sample, ok: true });
  source.record("pizza.tip", sample); source.recordOperation("pizza.tip", sample);
  const before = destination.snapshot(1000);
  for (const corrupt of [
    (s) => { s.operations.completed++; },
    (s) => { s.perMethod["pizza.tip"].errors = { timeout: 1 }; },
    (s) => { s.histogram.bucketCount--; },
    (s) => { s.perMethod["pizza.tip"].latencyMs.buckets.fill(0); },
  ]) {
    const snapshot = structuredClone(source.snapshot(1000)); corrupt(snapshot);
    assert.throws(() => destination.merge(snapshot));
    assert.deepEqual(destination.snapshot(1000), before);
  }
});

test("merged unknown series obey the same overflow policy as direct records", () => {
  const source = new Stats(), destination = new Stats({ methods: ["known"], maxMethods: 1 });
  const sample = { latencyMs: 1, ok: true };
  source.record("unknown", sample); destination.merge(source.snapshot(100));
  source.record("known", sample);
  destination.merge(source.snapshot(100));
  assert.deepEqual(Object.keys(destination.snapshot(100).perMethod), ["(other)", "known"]);
});
