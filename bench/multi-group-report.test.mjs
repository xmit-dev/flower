import assert from "node:assert/strict";
import test from "node:test";
import { Histogram } from "./metrics.mjs";
import { renderGroupsReport } from "./multi-group-report.mjs";

// Real buckets, with p99 overridden so each population's displayed tail is distinct.
function histogram(p99, values = Array.from({ length: 300 }, (_, index) => 1 + index % 50)) {
  const recorder = new Histogram();
  for (const value of values) recorder.record(value);
  return { ...recorder.snapshot(), p99 };
}

function fixture() {
  return {
    passed: true, correctnessPassed: true, schemaVersion: 1,
    options: { groups: 2, nodes: 3, tenants: 2, shops: 4, hotShops: 1, hotProbability: 0.8,
      readConsistency: "replica-local", queryRouting: "replicas", concurrency: 64, workers: 2,
      duration: 10, warmup: 1, drain: 60, maxOrders: 32, http2: true, initialization: "static" },
    environment: { cpu: "Test flower CPU", os: "Test OS", arch: "arm64", node: "v26", logicalCpus: 8 },
    binary: { sha256: "binary-identity" }, bundleHash: "bundle-identity", runtime: { engine: "quickjs", settings: { queryWorkers: 4 } },
    loadStartedAt: "2026-09-24T20:00:00.000Z", loadEndedAt: "2026-09-24T20:00:10.000Z",
    durationMs: 10_000, synchronizedOverlapMs: 9_990, startSkewMs: 10, goodputRps: 12_345,
    totals: { completed: 123_450, failed: 0, reads: 86_415, mutations: 37_035, orders: 64, pizzas: 128, replays: 10 },
    latencyMs: { all: histogram(44), read: histogram(23), mutation: histogram(75) },
    definition: "Successful customer calls divided by the union measurement window.",
    partitioning: "Static tenant-to-group assignment; no global atomic snapshot.", violations: [],
    groups: [0, 1].map((index) => ({ name: `group-${index}`, tenants: [`tenant-${index * 2}`, `tenant-${index * 2 + 1}`],
      html: `latest-groups/group-${index}.html`, json: `latest-groups/group-${index}.json`,
      application: { goodputRps: 6_000 + 345 * index, completed: 60_000 + 3_450 * index, failed: 0 },
      customerLatencyMs: histogram(12 + index * 60),
      customerReadLatencyMs: histogram(4 + index * 5),
      customerMutationLatencyMs: histogram(16 + index * 60),
      audit: { passed: true, delivered: 32, orders: 32 }, chaos: index ? [{ quorumRecoveryMs: 321 }] : [],
      resources: { serverPeakRssMiB: { 1: 100, 2: 105, 3: 110 }, driverPeakRssMiB: 85,
        serverCpuSampledLoad: { 1: { meanCores: 1.5 }, 2: { meanCores: 0.8 }, 3: { meanCores: 0.7 } },
        driverCpuMsWholeRun: { user: 30, system: 10 }, driverEventLoopP99Ms: 8 }, clusterDiskMiB: 22,
    })),
  };
}

test("multi-group report records global goodput, merged customer tails, policy, and offline charts", () => {
  const html = renderGroupsReport(fixture());
  assert.match(html, /^<!doctype html>/);
  for (const expected of ["12,345.0 / s", "p99 44 ms", "Replica-local customer reads", "lag without a bound",
    "Static tenant-to-group assignment", "union window", "Test flower CPU", "binary-identity", "bundle-identity",
    "queryWorkers", "321.0 ms", "315.0", "Server peak sums", "not a globally atomic transaction", "Customer reads", "Customer writes"]) {
    assert.ok(html.includes(expected), expected);
  }
  assert.match(html, /href="latest-groups\/group-0.html"/);
  assert.doesNotMatch(html, /<script\b|<link\b|<iframe\b|https?:\/\//i);
  // Goodput bars, two headline sparklines, four group cells, and three full histograms.
  assert.equal((html.match(/<svg /g) ?? []).length, 10);
  assert.match(html, /bucket counts are merged before binning/);
  const headline = html.split('<div class="metrics">')[1].split('<div class="notice')[0];
  assert.match(headline, /Customer reads<\/span><svg [^]*?<small>p50 25\.1 ms · p99 23 ms · merged across groups<\/small>/);
  assert.match(headline, /Customer writes<\/span><svg [^]*?<small>p50 25\.1 ms · p99 75 ms · merged across groups<\/small>/);
  assert.doesNotMatch(headline, /p99 44 ms|Customer p99/);
  const groups = html.split('<section id="groups">')[1].split('<section id="latency">')[0];
  assert.match(groups, /<small>p99 4 ms<\/small><\/span><\/td><td><span class="latency-cell">[^]*?<small>p99 16 ms<\/small>/);
  assert.match(groups, /<small>p99 9 ms<\/small><\/span><\/td><td><span class="latency-cell">[^]*?<small>p99 76 ms<\/small>/);
  // The global p99 is supplied by the merged histogram, never computed as the
  // 42 ms arithmetic mean of the two group p99s (12 and 72 ms).
  assert.doesNotMatch(html, /p99 42 ms/);
});

test("fresh policy and failures remain explicit without inventing absent observations", () => {
  const report = fixture();
  report.options.readConsistency = "fresh";
  report.passed = report.correctnessPassed = false;
  report.violations.push("Group 1 failed its audit");
  assert.match(renderGroupsReport(report), /Fresh customer reads · linearizable/);
  assert.match(renderGroupsReport(report), /Group 1 failed its audit/);
  const empty = renderGroupsReport(null);
  assert.match(empty, /Incomplete/);
  assert.match(empty, /policy was not recorded/);
  assert.match(empty, /No recorded groups/);
  assert.doesNotMatch(empty, /NaN|Infinity|undefined/);
});

test("the global verdict describes correctness independently of measured throughput", () => {
  const report = fixture();
  report.goodputRps = 1;
  let html = renderGroupsReport(report);
  assert.match(html, /<span class="status">Passed<\/span>/);
  assert.match(html, /Global customer goodput<\/span><strong>1\.0 \/ s/);
  assert.doesNotMatch(html, /RPS target|targetRps|targetMet|attainment|target met|target not|<progress/i);
  report.goodputRps = 90_000;
  report.passed = report.correctnessPassed = false;
  report.violations = ["A receipt replay failed"];
  html = renderGroupsReport(report);
  assert.match(html, /<span class="status">Failed<\/span>/);
  assert.match(html, /This run failed its correctness checks/);
  assert.match(html, /A receipt replay failed/);
});

test("report escapes HTML and blocks active artifact URLs", () => {
  const report = fixture();
  const attack = '</script><script>alert("x")</script><img src=x onerror=alert(1)>&\'"';
  report.environment.cpu = report.binary.sha256 = report.definition = report.partitioning = attack;
  report.groups[0].name = attack;
  report.groups[0].tenants = [attack];
  report.groups[0].html = "javascript:alert(1)";
  report.groups[0].json = "//example.invalid/external";
  report.violations = [attack];
  const html = renderGroupsReport(report);
  assert.doesNotMatch(html, /<script\b|<img\b|javascript:|href="\/\//i);
  assert.match(html, /&lt;script&gt;alert\(&quot;x&quot;\)&lt;\/script&gt;/);
});

test("all SVG accessibility labels have unique targets", () => {
  const html = renderGroupsReport(fixture());
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map((match) => match[1]);
  assert.equal(new Set(ids).size, ids.length);
  for (const [, labels] of html.matchAll(/aria-labelledby="([^"]+)"/g)) {
    for (const label of labels.split(" ")) assert.ok(ids.includes(label));
  }
});

test("recovery and lifecycle details retain their own measurement scopes", () => {
  const report = fixture();
  report.groups[0].chaos = [
    { oldLeader: 1, newLeader: 2, elapsedMs: 5_000, quorumRecoveryMs: 412, restartCatchUpMs: 2_700 },
    { oldLeader: 2, newLeader: 3, elapsedMs: 8_000, quorumRecoveryMs: 623 },
  ];
  report.groups[0].lifecycle = {
    orderToDeliveryMs: histogram(6_200, Array.from({ length: 32 }, (_, index) => 500 + index * 180)),
    timerLatenessMs: histogram(990, Array.from({ length: 31 }, (_, index) => 10 + index * 32)),
  };
  report.groups[0].counters = { replayChecks: 0, staleLeaseChecks: 7 };
  const html = renderGroupsReport(report);
  const recovery = html.split('<section id="recovery">')[1].split('<section id="resources">')[0];
  assert.match(recovery, /<td>2<\/td><td>8\.00 s<\/td><td>2 → 3<\/td><td>623\.0 ms<\/td><td>—<\/td>/);
  assert.match(recovery, /2,700\.0 ms/);
  assert.match(recovery, /<td>32<\/td><td><span class="latency-cell"><svg [^]*?aria-label="group-0 order to delivery: 32 samples; [^]*?<small>p99 6\.2 s<\/small><\/span><\/td><td>31<\/td><td><span class="latency-cell"><svg [^]*?aria-label="group-0 timer lateness: 31 samples; [^]*?<small>p99 990 ms<\/small><\/span><\/td><td>0<\/td><td>7<\/td>/);
  assert.match(recovery, /both start at the crash/);
  assert.match(recovery, /include load and drain/);
  assert.match(recovery, /Expected stale-lease rejections/);
  assert.match(html, /p99 23 ms · merged across groups/);
  assert.match(html, /p99 75 ms · merged across groups/);
});

test("missing read or write observations never fall back to an aggregate p99", () => {
  const report = fixture();
  report.latencyMs.read = { ...histogram(23), buckets: [], samples: 0 };
  delete report.latencyMs.mutation;
  for (const group of report.groups) {
    delete group.customerReadLatencyMs;
    group.customerMutationLatencyMs = { samples: 0, p99: 75 };
  }
  const html = renderGroupsReport(report);
  const headline = html.split('<div class="metrics">')[1].split('<div class="notice')[0];
  assert.match(headline, /Customer reads<\/span><strong>—<\/strong>/);
  assert.match(headline, /Customer writes<\/span><strong>—<\/strong>/);
  assert.doesNotMatch(headline, /<svg |44 ms|23 ms|75 ms/);
  const groups = html.split('<section id="groups">')[1].split('<section id="latency">')[0];
  assert.equal((groups.match(/<td>—<\/td><td>—<\/td>/g) ?? []).length, 2);
  assert.doesNotMatch(groups, /<svg |12 ms|72 ms|75 ms/);
});

test("unrecorded crashes and lifecycle measurements remain unavailable", () => {
  const report = fixture();
  for (const group of report.groups) delete group.chaos;
  const html = renderGroupsReport(report);
  const recovery = html.split('<section id="recovery">')[1].split('<section id="resources">')[0];
  assert.match(recovery, /No leader crashes recorded/);
  assert.doesNotMatch(recovery, /0\.0 ms|NaN|Infinity|undefined/);
  assert.match(recovery, /<td>—<\/td>/);
});
