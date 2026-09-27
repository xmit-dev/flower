import assert from "node:assert/strict";
import test from "node:test";
import { latency, parseOptions } from "./staged-deployment.mjs";

test("deployment latency percentiles use nearest ranks and preserve the maximum", () => {
  const samples = [9, 1, 2, 3, 4, 5, 6, 7, 8, 100];
  assert.deepEqual(latency(samples), { count: 10, p50Ms: 5, p95Ms: 100, p99Ms: 100, maxMs: 100 });
  assert.equal(samples[0], 9);
  assert.deepEqual(latency([]), { count: 0, p50Ms: null, p95Ms: null, p99Ms: null, maxMs: null });
});

test("deployment benchmark accepts disabled writes and explicit test shapes", () => {
  const options = parseOptions(["--nodes", "1", "--sizes", "25,100", "--modes", "staged", "--write-rate", "0", "--warmup-ms", "0", "--warm-target-bundle"]);
  assert.deepEqual(options.sizes, [25, 100]);
  assert.deepEqual(options.modes, ["staged"]);
  assert.equal(options.nodes, 1);
  assert.equal(options.writeRate, 0);
  assert.equal(options.warmupMs, 0);
  assert.equal(options.warmTargetBundle, true);
});

test("deployment benchmark rejects ambiguous or unbounded invalid configurations", () => {
  for (const args of [
    ["--sizes", "1,1"], ["--sizes", "0"], ["--sizes", "1.5"],
    ["--nodes", "2"], ["--modes", "online"], ["--modes", "direct,direct"],
    ["--write-rate", "-1"], ["--seed-batch", "0"], ["--sizes"],
    ["--nodes", "1", "--nodes", "3"], ["--case-timeout-ms", "2147483648"],
    ["--warmup-ms", "2147483648"],
    ["--binary", "/tmp/same", "--output", "/tmp/same"],
  ]) assert.throws(() => parseOptions(args), args.join(" "));
});
