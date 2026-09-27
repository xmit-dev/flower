import test from "node:test";
import assert from "node:assert/strict";
import { cpuMilliseconds, ServerCpuSamples } from "./resources.mjs";

test("ps cumulative CPU formats retain fractional seconds and reject malformed values", () => {
  assert.equal(cpuMilliseconds("0:03.15"), 3150);
  assert.equal(cpuMilliseconds("01:02:03"), 3723000);
  assert.equal(cpuMilliseconds("2-01:02:03.50"), 176523500);
  assert.equal(cpuMilliseconds("100:00.00"), 6000000);
  for (const text of ["", "-1", "00:60", "1:60:00", "NaN"]) assert.equal(cpuMilliseconds(text), null);
});

test("CPU coverage excludes phase boundaries, restart gaps, and reset counters", () => {
  const samples = new ServerCpuSamples();
  samples.record(1, 100, 500, 0, "setup");
  samples.record(1, 100, 1000, 1000, "load");
  samples.record(1, 100, 3000, 2000, "load");
  samples.record(1, 200, 500, 3000, "load");
  samples.record(1, 200, 2000, 4000, "load");
  samples.record(1, 200, 0, 5000, "load");
  samples.record(1, 200, 100, 6000, "drain");
  assert.deepEqual(samples.snapshot(), { 1: { cpuMs: 3500, sampledWallMs: 2000, intervals: 2, meanCores: 1.75 } });
});
