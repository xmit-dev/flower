import assert from "node:assert/strict";
import test from "node:test";
import { Histogram, Stats } from "./metrics.mjs";
import { summarizeGroups } from "./multi-group.mjs";

function group(tenant, start, durations) {
  const stats = new Stats();
  for (const [name, ms, ok = true] of durations) stats.recordOperation(name, { latencyMs: ms, ok });
  return { options: { tenantIds: [tenant], readConsistency: "fresh" },
    loadStartedAt: new Date(start).toISOString(), loadEndedAt: new Date(start + 1000).toISOString(),
    bundleHash: "bundle", binary: { sha256: "binary" }, correctnessPassed: true,
    phases: { load: stats.snapshot(1000) }, audit: { passed: true, orders: 1, pizzas: 2, revenue: 14, tips: 3 }, counters: { replayChecks: 1, staleLeaseChecks: 1 },
  };
}
const options = { groups: 2, readConsistency: "fresh" };

test("multi-group goodput uses union wall time, excludes extra traffic, and merges customer buckets", () => {
  const first = group("a", 1000, [["pizza.shop", 1], ["pizza.tip", 2], ["pizza.tip.replay", 900], ["pizza.claim", 1000]]);
  const second = group("b", 1100, [["pizza.shop", 100], ["pizza.tip", 200], ["pizza.tip", 300, false]]);
  const report = summarizeGroups([first, second], options);
  assert.equal(report.durationMs, 1100);
  assert.equal(report.synchronizedOverlapMs, 900);
  assert.equal(report.startSkewMs, 100);
  assert.equal(report.goodputRps, 4 / 1.1);
  assert.equal(report.totals.failed, 1);
  assert.equal(report.latencyMs.all.samples, 5);
  assert.ok(report.latencyMs.all.p50 >= 100 && report.latencyMs.all.p50 <= 101);
  assert.equal(report.latencyMs.all.p99, 300);
  assert.equal(report.totals.replays, 2);
  assert.equal(report.latencyMs.mutation.samples, 3);
  assert.equal(report.latencyMs.read.samples, 2);
  assert.equal(report.groups[0].customerReadLatencyMs.samples, 1);
  assert.equal(report.groups[0].customerReadLatencyMs.p99, 1);
  assert.equal(report.groups[0].customerMutationLatencyMs.samples, 1);
  assert.equal(report.groups[0].customerMutationLatencyMs.p99, 2);
  assert.equal(report.groups[1].customerReadLatencyMs.p99, 100);
  assert.equal(report.groups[1].customerMutationLatencyMs.samples, 2);
  assert.equal(report.groups[1].customerMutationLatencyMs.p99, 300);
  assert.equal(report.passed, true); // Synthetic correctness verdict is explicit.
});

test("aggregate correctness rejects mixed builds, repeated tenants and nonoverlapping runs", () => {
  for (const mutate of [
    (r) => { r[1].binary.sha256 = "different"; },
    (r) => { r[1].bundleHash = "different"; },
    (r) => { r[1].options.tenantIds = ["a"]; },
    (r) => { r[1].options.readConsistency = "replica-local"; },
    (r) => { r[1].loadStartedAt = new Date(5000).toISOString(); r[1].loadEndedAt = new Date(6000).toISOString(); },
    (r) => { r[1].correctnessPassed = false; },
    (r) => { r[1].loadStartedAt = null; },
    (r) => { r.pop(); },
  ]) {
    const reports = [group("a", 1000, [["pizza.shop", 1]]), group("b", 1000, [["pizza.shop", 2]])];
    mutate(reports);
    const report = summarizeGroups(reports, options);
    assert.equal(report.passed, false);
    assert.ok(report.violations.length > 0);
  }
});

test("histogram merges exactly match a combined stream and reject summaries without buckets", () => {
  const a = new Histogram(), b = new Histogram(), all = new Histogram();
  for (let i = 0; i < 1000; i++) { (i % 2 ? a : b).record(i * i / 100); all.record(i * i / 100); }
  assert.deepEqual(new Histogram().merge(a.snapshot()).merge(b.snapshot()).snapshot(), all.snapshot());
  assert.throws(() => new Histogram().merge({ samples: 5, p99: 4 }), /incompatible/);
  const broken = a.snapshot(); broken.buckets[0] = -1;
  assert.throws(() => new Histogram().merge(broken), /incompatible/);
});

test("aggregate environment counts every colocated server and driver", () => {
  const reports = [group("a", 1000, [["pizza.shop", 1]]), group("b", 1000, [["pizza.shop", 2]])];
  for (const report of reports) report.environment = { cpu: "test", colocatedNodes: 3 };
  assert.deepEqual(summarizeGroups(reports, { ...options, nodes: 3 }).environment,
    { cpu: "test", colocatedNodes: 6, replicasPerGroup: 3, colocatedDrivers: 2, colocatedControllers: 2, colocatedNativeDrivers: 0 });
});

test("aggregate verifies native driver identity and accounts for controllers separately", () => {
  const reports = [group("a", 1000, [["pizza.shop", 1]]), group("b", 1000, [["pizza.shop", 2]])];
  for (const report of reports) {
    report.driver = { kind: "rust", binary: { sha256: "driver" } };
    report.environment = { colocatedNodes: 3 };
  }
  const aggregate = summarizeGroups(reports, { ...options, nodes: 3 });
  assert.equal(aggregate.passed, true);
  assert.deepEqual(aggregate.driver, reports[0].driver);
  assert.equal(aggregate.environment.colocatedDrivers, 4);
  assert.equal(aggregate.environment.colocatedControllers, 2);
  assert.equal(aggregate.environment.colocatedNativeDrivers, 2);
  for (const change of [
    (r) => { r.driver.kind = "node"; },
    (r) => { delete r.driver.binary; },
    (r) => { r.driver.binary.sha256 = "different"; },
  ]) {
    const mixed = structuredClone(reports);
    change(mixed[1]);
    assert.equal(summarizeGroups(mixed, options).passed, false);
  }
});

test("missing group measurements and identities produce invalid aggregate reports", () => {
  const empty = summarizeGroups([], options);
  assert.equal(empty.passed, false);
  assert.equal(empty.durationMs, null);
  assert.equal(empty.goodputRps, null);
  assert.equal(empty.loadStartedAt, null);
  assert.equal(empty.loadEndedAt, null);
  for (const alter of [
    (r) => { for (const item of r) delete item.bundleHash; },
    (r) => { for (const item of r) delete item.binary; },
    (r) => { delete r[1].phases.load; },
    (r) => { delete r[1].phases.load.operations.perMethod; },
    (r) => { delete r[1].options; },
    (r) => { r[1].options.tenantIds = []; },
    (r) => { r[1].options.tenantIds = [""]; },
    (r) => { r[1].phases.load.operations.perMethod["pizza.shop"].count = -1; },
    (r) => { delete r[1].phases.load.operations.perMethod["pizza.shop"].latencyMs; },
    (r) => { r[1].phases.load.operations.perMethod["pizza.shop"].latencyMs.invalidSamples = -1; },
    (r) => { r[1].phases.load.operations.perMethod["pizza.shop"].count++; },
  ]) {
    const reports = [group("a", 1000, [["pizza.shop", 1]]), group("b", 1000, [["pizza.shop", 2]])];
    alter(reports);
    const report = summarizeGroups(reports, options);
    assert.equal(report.passed, false);
    assert.ok(report.violations.length > 0);
  }
  const reports = [group("a", 1000, [["pizza.shop", 1]]), group("b", 1000, [["pizza.shop", 2]])];
  assert.equal(summarizeGroups(reports, { ...options, tenants: 2 }).passed, false);
});
