import assert from "node:assert/strict";
import test from "node:test";
import { Histogram } from "./metrics.mjs";
import { renderReport } from "./report.mjs";

function recorded(values) {
  const histogram = new Histogram();
  for (const value of values) histogram.record(value);
  return histogram.snapshot();
}

function fixture() {
  const histogram = recorded([1, 2, 3, 4, 5, 5, 5, 8, 12, 20, 30, 40]);
  const counters = { attempts: 13, successes: 12, failures: 1, retries: 1, duplicates: 2, throughputPerSecond: 6, latencyMs: histogram, errors: { HTTP_503: 1 } };
  const logical = { count: 12, completed: 12, failed: 0, duplicates: 2, throughputPerSecond: 6, latencyMs: histogram };
  return {
    schemaVersion: 1, startedAt: "2026-09-23T18:00:00.000Z", finishedAt: "2026-09-23T18:00:05.000Z", passed: true,
    options: { duration: 2, warmup: 0, drain: 10, concurrency: 8, workers: 2, shops: 4, hotShops: 1, hotProbability: 0.8, maxOrders: 32, bakeMs: 250, leaseMs: 2_000, duplicateRate: 0.1, abandonRate: 0.1, pollMs: 150, requestTimeoutMs: 2_000, retryBudgetMs: 15_000, nodes: 3, seed: "42", chaos: true },
    environment: { cpu: "Example CPU", logicalCpus: 8, os: "Test OS", arch: "arm64", node: "v26", colocatedNodes: 3 },
    audit: { passed: true, violations: [], orders: 8, delivered: 8, pizzas: 20, revenue: 140, tips: 7 },
    phases: { load: { ...counters, durationMs: 2_000, perMethod: { "pizza.order": counters }, operations: { ...logical, perMethod: { "pizza.order": logical } } } },
    lifecycle: { timerLatenessMs: histogram, orderToDeliveryMs: histogram, deliveredOrdersPerSecondIncludingDrain: 2 },
    leaderboard: [{ id: "shop-0", name: "The Crispy Cauldron", orders: 8, deliveredQuantity: 20, stock: 40, revenue: 140, tips: 7 }],
    counters: { issuedOrders: 8, replayChecks: 2, claims: 10, emptyClaims: 2, reclaimed: 2, abandoned: 2, lostLeases: 0, staleLeaseChecks: 2 },
    chaos: [{ oldLeader: 1, newLeader: 2, crashedAt: "2026-09-23T18:00:01.000Z", quorumRecoveryMs: 1_200, restartCatchUpMs: 1_500 }],
    replication: [{ node: 1, state: "Follower", leader: 2, lastApplied: 100 }],
    resources: { serverPeakRssMiB: { 1: 33 }, driverPeakRssMiB: 120, driverCpuMsWholeRun: { user: 500, system: 100 }, driverEventLoopP99Ms: 21 },
    clusterDiskMiB: 12, failureCount: 0, failures: [], bundleHash: "abc123",
  };
}

test("renders recorded metrics, charts, audit, topology, and measurement caveats", () => {
  const html = renderReport(fixture());
  assert.match(html, /^<!doctype html>/);
  assert.match(html, /<html lang="en">/);
  assert.match(html, /<span class="status passed">Passed<\/span>/);
  assert.match(html, /The Crispy Cauldron/);
  assert.match(html, /8 of 8 acknowledged orders delivered/);
  assert.match(html, /pizza\.order/);
  assert.match(html, /HTTP_503/);
  assert.match(html, /1,200\.0 ms/);
  assert.match(html, /Example CPU/);
  assert.match(html, /abc123/);
  assert.match(html, /one Raft group, not independent shards/);
  assert.match(html, /nearest-rank bucket upper bounds/);
  assert.match(html, /within 1%/);
  assert.match(html, /not estimate open-loop latency/);
  assert.match(html, /aria-label="All-call latency: 12 calls; p50 5 ms, p99 40 ms, max 40 ms\./);
  assert.match(html, /aria-label="Oven lateness: 12 samples; p50 /);
  assert.match(html, /aria-label="pizza\.order HTTP attempts: 12 calls; /);
  assert.match(html, /<small>p99 40 ms<\/small>/);
  assert.doesNotMatch(html, /<th scope="col">p95<\/th>/);
  assert.ok((html.match(/<svg /g) ?? []).length >= 5);
  assert.ok((html.match(/role="img"/g) ?? []).length >= 5);
  assert.ok((html.match(/<caption>/g) ?? []).length >= 5);
  assert.doesNotMatch(html, /<script\b|<link\b|<iframe\b|https?:\/\//i);
});

test("reports fresh replica query routing and flags comparisons with leader-only runs", () => {
  const report = fixture();
  const baseline = fixture();
  report.options.queryRouting = "replicas";
  baseline.options.queryRouting = "leader";
  report.queryRouting = { mode: "replicas", consistency: "linearizable", nodes: [{ id: 2, attempts: 8, completed: 7, failures: 1 }] };
  const html = renderReport(report, { baseline });
  assert.match(html, /Round-robin across live replicas/);
  assert.match(html, /does not opt the application into stale reads/);
  assert.match(html, /queryRouting/);
  assert.match(html, /&quot;attempts&quot;: 8/);
  assert.match(renderReport(baseline), /Leader only/);
});

test("replica-local customer previews are explicit and cannot be compared silently with fresh reads", () => {
  const report = fixture(), baseline = fixture();
  report.options.readConsistency = "replica-local";
  report.options.tenants = 3;
  baseline.options.readConsistency = "fresh";
  baseline.options.tenants = 2;
  report.leaderboard[0].id = ["tenant-0", "store-0"];
  const html = renderReport(report, { baseline });
  for (const text of ["Replica-local customer previews", "lag without a bound", "pizza.shop.local",
    "--read-consistency fresh", "final pizza.world audit retain fresh semantics", "readConsistency", "tenants",
    "tenant-0 / The Crispy Cauldron", "not a global ranking"]) assert.ok(html.includes(text), text);
  assert.doesNotMatch(html, /Fresh customer reads: linearizable within this Raft group/);
});

test("escapes script and HTML injection across report text, SVG, attributes, and logs", () => {
  const attack = '</script><script>alert("owned")</script><img src=x onerror=alert(1)>&\'"';
  const report = fixture();
  report.startedAt = attack;
  report.bundleHash = attack;
  report.binary = { sha256: attack, path: attack };
  report.options.seed = attack;
  report.environment.cpu = attack;
  report.leaderboard[0].name = attack;
  report.chaos[0].oldLeader = attack;
  report.replication[0].state = attack;
  report.audit.violations = [attack];
  report.failures = [{ context: attack, message: attack, code: attack }];
  report.clusterLogs = [{ node: attack, generation: attack, pid: attack, exit: attack, error: attack, tail: attack }];
  report.phases[attack] = report.phases.load;
  report.phases.load.perMethod[attack] = report.phases.load.perMethod["pizza.order"];
  report.resources.serverPeakRssMiB[attack] = 2;
  const html = renderReport(report);
  assert.doesNotMatch(html, /<script\b|<img\b|<\/script>|<iframe\b/i);
  assert.ok(html.includes("&lt;/script&gt;&lt;script&gt;alert(&quot;owned&quot;)&lt;/script&gt;"));
  assert.ok(html.includes("&amp;&#39;&quot;"));
});

test("an early failed or empty report is readable without fabricated zero measurements", () => {
  const html = renderReport({ passed: false, failureCount: 1, failures: [{ context: "startup", message: "Binary missing" }] });
  assert.match(html, /<span class="status failed">Failed<\/span>/);
  assert.match(html, /Binary missing/);
  assert.match(html, /No phase measurements were recorded/);
  assert.match(html, /before a final independent audit was available/);
  assert.match(html, /No leader crash recorded/);
  assert.doesNotMatch(html, /NaN|Infinity|undefined/);
  assert.match(renderReport(null), /<span class="status incomplete">Incomplete<\/span>/);
  assert.match(renderReport({ phases: { load: { latencyMs: { p99: NaN }, durationMs: Infinity } } }), /No measurements yet/);
});

test("recovery milestones distinguish observed election states from a successful quorum probe", () => {
  const report = fixture();
  Object.assign(report.chaos[0], { candidateObservedMs: 450, leaderObservedMs: 475,
    quorumProbeMs: 490, clientRecoveryMs: 510 });
  let html = renderReport(report);
  for (const text of ["Candidate observed in metrics", "Leader observed in metrics", "Quorum read probe completed",
    "Leader discovery returned", "Client query returned", "450.0 ms", "475.0 ms", "490.0 ms", "510.0 ms"]) {
    assert.ok(html.includes(text), text);
  }
  assert.match(html, /not exact internal transition times/);
  assert.match(html, /brief candidate state can be missed/);
  report.chaos[0].candidateObservedMs = Infinity;
  report.chaos[0].leaderObservedMs = '<script>bad</script>';
  report.chaos[0].quorumProbeMs = -1;
  html = renderReport(report);
  assert.doesNotMatch(html, /NaN|Infinity|<script>/);
  assert.match(html, /Candidate observed in metrics<\/th><td>—<\/td>/);
  assert.match(renderReport(fixture()), /Quorum read probe completed<\/th><td>—<\/td>/);
});

test("all generated SVG labels reference unique, existing IDs", () => {
  const html = renderReport(fixture());
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map((match) => match[1]);
  assert.equal(new Set(ids).size, ids.length);
  for (const [, references] of html.matchAll(/aria-labelledby="([^"]+)"/g)) {
    for (const id of references.split(" ")) assert.ok(ids.includes(id), `Missing accessibility label ${id}`);
  }
});

test("baseline comparisons report changes and disclose mismatched conditions", () => {
  const baseline = fixture();
  const current = fixture();
  current.phases.load.operations.throughputPerSecond = 12;
  current.phases.load.operations.latencyMs = { ...current.phases.load.operations.latencyMs, p99: 20 };
  let html = renderReport(current, { baseline });
  assert.match(html, /\+100\.0%/);
  assert.match(html, /-50\.0%/);
  assert.match(html, /Recorded workload settings and basic host descriptions match/);
  current.options.concurrency = 16;
  current.environment.cpu = "A different CPU";
  html = renderReport(current, { baseline });
  assert.match(html, /These runs have different conditions/);
  assert.match(html, /Workload: concurrency/);
  assert.match(html, /Environment: cpu/);
  assert.match(html, /cannot be attributed to implementation alone/);
  baseline.phases.load.throughputPerSecond = 0;
  assert.doesNotMatch(renderReport(current, { baseline }), /NaN|Infinity/);
  current.options.http2 = true;
  html = renderReport(current, { baseline });
  assert.match(html, /HTTP\/2 · h2c prior knowledge/);
  assert.match(html, /application HTTP protocol/);
});

test("timeline charts distinguish interval rates from cumulative percentiles and mark crashes", () => {
  const report = fixture();
  report.timeline = [
    { elapsedMs: 1_000, phase: "load", attemptsPerSecond: 10, successesPerSecond: 9, logicalPerSecond: 8, retries: 1, failures: 1, p95Ms: 12, ordersAcknowledged: 3, deliveriesAcknowledged: 1, driverRssMiB: 100 },
    { elapsedMs: 2_000, phase: "drain", attemptsPerSecond: 5, successesPerSecond: 5, logicalPerSecond: 5, retries: 0, failures: 0, p95Ms: 20, ordersAcknowledged: 3, deliveriesAcknowledged: 3, driverRssMiB: 110 },
  ];
  report.chaos[0].elapsedMs = 1_200;
  const html = renderReport(report);
  assert.match(html, /How the run unfolded/);
  assert.match(html, /p95 of attempts since load began; not an interval percentile/);
  assert.match(html, /Leader crash at 1\.20 s after load start/);
  assert.match(html, /Retries and HTTP failures are counts for each interval/);
  assert.match(html, /Driver RSS: first 100\.0, last 110\.0 MiB/);
  assert.ok((html.match(/<svg /g) ?? []).length >= 9);
  report.timeline[0].phase = '<script>alert("timeline")</script>';
  report.timeline[0].p95Ms = Infinity;
  assert.doesNotMatch(renderReport(report), /<script\b|NaN|Infinity/);
  assert.doesNotMatch(renderReport(fixture()), /How the run unfolded/);
});

test("native profile reports waiting and active denominators without unsafe links or markup", () => {
  const report = fixture();
  const attack = '<script>alert("cpu")</script>';
  report.options.cpuProfile = "profile.sample.txt";
  report.cpuProfile = {
    status: "complete", tool: "/usr/bin/sample", pid: 42, node: 1,
    outputPath: "profile.sample.txt", relativePath: "javascript:alert(1)",
    requestedDurationSeconds: 10, intervalMs: 1, elapsedMs: 12_000, rawBytes: 99,
    scope: attack, stderr: attack,
    summary: { available: true, totalThreadSamples: 15, activeThreadSamples: 4, waitingThreadSamples: 11,
      activeInclusive: [{ frame: attack, samples: 4, fraction: 1 }],
      activeSelf: [{ frame: "JS_CallInternal", samples: 3, fraction: 0.75 }],
      waitingFrames: [{ frame: "kevent", samples: 11, fraction: 1 }],
      activeCategories: [{ category: attack, samples: 4, fraction: 1 }],
      threadGroups: [{ thread: attack, physicalThreads: 1, totalThreadSamples: 15, activeThreadSamples: 4, waitingThreadSamples: 11, activeFraction: 4 / 15 }],
      threads: [{ thread: attack, totalThreadSamples: 15, activeThreadSamples: 4, waitingThreadSamples: 11, activeFraction: 4 / 15 }],
    },
  };
  const html = renderReport(report);
  assert.match(html, /Native stack sampling/);
  assert.match(html, /Profiling perturbs performance/);
  assert.match(html, /Thread observations are not CPU utilization/);
  assert.match(html, /Share of active observations/);
  assert.match(html, /Active share within this thread/);
  assert.match(html, /75%/);
  assert.match(html, /href="\.\/javascript%3Aalert\(1\)"/);
  assert.doesNotMatch(html, /<script\b|href="javascript:/);
  assert.match(renderReport(report, { baseline: fixture() }), /CPU profiling enabled/);
  report.options.cpuProfile = null;
  report.cpuProfile.perturbsPerformance = true;
  assert.match(renderReport(report, { baseline: fixture() }), /CPU profiling enabled/, "externally attached profiling is disclosed too");
  report.cpuProfile = { status: "failed", error: "sample unavailable" };
  assert.match(renderReport(report), /sample unavailable/);
  assert.doesNotMatch(renderReport(report), /NaN|Infinity|undefined/);
});

test("customer goodput remains distinct from inflated raw traffic and the correctness verdict", () => {
  const report = fixture();
  report.binary = { sha256: "d".repeat(64), bytes: 123_456, path: "/tmp/flower" };
  report.phases.load.successes = 50_000;
  report.phases.load.throughputPerSecond = 25_000;
  report.phases.load.operations.completed = 50_000;
  report.phases.load.operations.throughputPerSecond = 25_000;
  report.phases.load.operations.perMethod["pizza.claim"] = { count: 49_988, completed: 49_988, failed: 0 };
  const html = renderReport(report);
  assert.match(html, /<span class="status passed">Passed<\/span>/);
  assert.match(html, /Customer goodput<\/p><p class="metric-value">6\.0 <small>\/ s<\/small>/);
  assert.match(html, /All primary customer calls/);
  assert.match(html, /every failed call contribute zero customer goodput/);
  assert.match(html, /correctness and completion of any requested CPU profile/);
  assert.doesNotMatch(html, /target|attainment|<progress/i);
  assert.match(html, /Binary SHA-256/);
  assert.match(html, /123,456 bytes/);
  assert.ok(html.includes("d".repeat(64)));
  report.phases.load.operations.perMethod["pizza.order"].completed = 30_000;
  assert.match(renderReport(report), /Customer goodput<\/p><p class="metric-value">15,000\.0/);
  assert.match(renderReport(report), /<span class="status passed">Passed<\/span>/);
  report.passed = false;
  report.cpuProfile = { status: "failed", error: "profile did not complete" };
  assert.match(renderReport(report), /<span class="status failed">Failed<\/span>/);
  assert.match(renderReport(report), /profile did not complete/);
});
