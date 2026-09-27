import assert from "node:assert/strict";
import test from "node:test";
import { compareCpu, renderCpuComparison, summarizeCpu } from "./compare-cpu.mjs";

const measurement = (cpuMs, sampledWallMs = 900, intervals = 9) => ({ cpuMs, sampledWallMs, intervals });
function fixture() {
  return {
    kind: "multi-group", passed: true, durationMs: 1000, bundleHash: "bundle",
    options: { groups: 2, nodes: 1, duration: 1, offeredRate: 100, driver: "rust" },
    environment: { cpu: "CPU", logicalCpus: 8, os: "darwin", arch: "arm64", node: "v26" },
    runtime: { engine: "quickjs", settings: { TOKIO_WORKER_THREADS: "2" } },
    driver: { kind: "rust", binary: { sha256: "driver" } }, binary: { sha256: "server" },
    totals: { completed: 200, failed: 0 },
    offeredLoad: { offered: 200, dispatched: 200, completed: 200, failed: 0, driverDropped: 0 },
    groups: [0, 1].map(index => ({ name: `group-${index}`, options: { nodes: 1 },
      application: { available: true, durationMs: 1000, completed: 100, failed: 0, reads: { completed: 70 }, mutations: { completed: 30 } },
      resources: { serverCpuSampledLoad: { 1: measurement(900) },
        nodeDriverCpuSampledLoad: measurement(90), nativeDriverCpuSampledLoad: measurement(180) },
    })),
  };
}

test("extrapolates each process's covered CPU without averaging unequal coverage", () => {
  const report = fixture();
  report.groups[1].resources.serverCpuSampledLoad[1] = measurement(50, 100, 1);
  const summary = summarizeCpu(report);
  assert.equal(summary.servers.observedCpuMs, 950);
  assert.equal(summary.servers.estimatedLoadCpuMs, 1500);
  assert.equal(summary.servers.estimatedCpuUsPerSuccessfulCall, 7500);
  assert.equal(summary.servers.coverageMin, 0.1);
  assert.equal(summary.servers.coverageMax, 0.9);
  assert.equal(summary.drivers.estimatedCpuUsPerSuccessfulCall, 3000);
  assert.equal(summary.processes.length, 6);
});

test("matched fixed offered work compares server binary changes with exact successful work", () => {
  const before = fixture(), after = fixture();
  after.binary.sha256 = "optimized";
  for (const group of after.groups) group.resources.serverCpuSampledLoad[1].cpuMs *= 0.8;
  const comparison = compareCpu(before, after);
  assert.equal(comparison.matchedFixedWork, true);
  assert.deepEqual(comparison.reasons, []);
  assert.ok(Math.abs(comparison.changesPercent.estimatedServerCpuPerCall + 20) < 1e-10);
  assert.match(renderCpuComparison(comparison), /Estimated server CPU per call change: -20.00%/);
});

test("missing process CPU remains unavailable and never creates an apparent reduction", () => {
  const before = fixture(), after = fixture();
  delete after.groups[0].resources.serverCpuSampledLoad[1];
  const comparison = compareCpu(before, after);
  assert.equal(comparison.candidate.servers.available, false);
  assert.equal(comparison.candidate.servers.measuredProcesses, 1);
  assert.equal(comparison.candidate.servers.estimatedCpuUsPerSuccessfulCall, null);
  assert.equal(comparison.changesPercent.estimatedServerCpuPerCall, null);
  assert.equal(comparison.matchedFixedWork, false);
});

test("rejects impossible coverage and preserves zero CPU as a real measurement", () => {
  for (const invalid of [measurement(-1), measurement(1, 0), measurement(1, 1002), measurement(1, 100, 0), measurement(NaN)]) {
    const report = fixture(); report.groups[0].resources.serverCpuSampledLoad[1] = invalid;
    assert.equal(summarizeCpu(report).servers.available, false);
  }
  const report = fixture();
  for (const group of report.groups) group.resources.serverCpuSampledLoad[1] = measurement(0);
  assert.equal(summarizeCpu(report).servers.estimatedCpuUsPerSuccessfulCall, 0);
});

test("closed-loop and changed workload, telemetry, host, or driver are explicit caveats", () => {
  for (const [change, reason] of [
    [report => { report.options.offeredRate = 0; }, /Closed-loop/],
    [report => { report.options.duration = 2; }, /workload.duration/],
    [report => { report.runtime.settings.FLOWER_OTEL_ENABLED = "1"; }, /runtime/],
    [report => { report.environment.cpu = "Another CPU"; }, /environment.cpu/],
    [report => { report.driver.binary.sha256 = "other"; }, /customer driver binary/],
    [report => { report.options.cpuProfile = "profile.txt"; }, /CPU profiling/],
    [report => { report.passed = false; }, /failed correctness/],
  ]) {
    const after = fixture(); change(after);
    const result = compareCpu(fixture(), after);
    assert.equal(result.matchedFixedWork, false);
    assert.ok(result.reasons.some(item => reason.test(item)), result.reasons.join("\n"));
  }
});

test("drops, failures, missing arrivals, and unequal completed work cannot pass fixed-work criteria", () => {
  for (const change of [
    report => { report.offeredLoad.driverDropped = 1; },
    report => { report.offeredLoad.failed = 1; },
    report => { delete report.offeredLoad; },
    report => { report.offeredLoad.completed = 199; },
    report => {
      report.groups[0].application.completed = 99; report.groups[0].application.reads.completed = 69; report.totals.completed = 199;
      for (const key of ["offered", "dispatched", "completed"]) report.offeredLoad[key] = 199;
    },
  ]) {
    const after = fixture(); change(after);
    assert.equal(compareCpu(fixture(), after).matchedFixedWork, false);
  }
});

test("inconsistent aggregate completion totals fail rather than inventing a denominator", () => {
  const report = fixture(); report.totals.completed++;
  assert.throws(() => summarizeCpu(report), /disagree/);
});

test("single-group primary accounting excludes replays and worker successes", () => {
  const source = fixture(), group = source.groups[0];
  const report = { ...source, kind: undefined, options: { ...source.options, groups: 1 }, resources: group.resources,
    phases: { load: { durationMs: 1000, operations: { completed: 1000, perMethod: {
      "pizza.shop": { count: 70, completed: 70, failed: 0 }, "pizza.tip": { count: 30, completed: 30, failed: 0 },
      "pizza.tip.replay": { count: 600, completed: 600, failed: 0 }, "pizza.claim": { count: 300, completed: 300, failed: 0 },
    } } } },
  };
  const summary = summarizeCpu(report);
  assert.equal(summary.completed, 100);
  assert.equal(summary.servers.estimatedCpuUsPerSuccessfulCall, 10000);
});

test("runtime property order does not create a mismatch", () => {
  const after = fixture(); after.runtime = { settings: { TOKIO_WORKER_THREADS: "2" }, engine: "quickjs" };
  assert.equal(compareCpu(fixture(), after).matchedFixedWork, true);
});


test("matching diagnostic settings still exclude a run from uninstrumented fixed-work comparison", () => {
  for (const key of ["FLOWER_OTEL_ENABLED", "FLOWER_PROFILE_EVALUATOR"]) {
    const before = fixture(), after = fixture();
    before.runtime.settings[key] = after.runtime.settings[key] = "1";
    const comparison = compareCpu(before, after);
    assert.equal(comparison.matchedFixedWork, false);
    assert.ok(comparison.reasons.every(reason => /diagnostic instrumentation enabled/.test(reason)));
  }
});

test("explicit failed correctness takes precedence over a passed legacy field", () => {
  const report = fixture(); report.correctnessPassed = false;
  assert.equal(summarizeCpu(report).passed, false);
});
