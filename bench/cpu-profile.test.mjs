import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";
import { demangleSummary, startCpuProfile, summarizeSample } from "./cpu-profile.mjs";

const RAW = `Sampling process 42 for 1 seconds with 1 millisecond of run time between samples
Call graph:
    10 Thread_1: worker
    + 10 rust_worker (in flower) + 10 [0x123]
    +   6 __psynch_cvwait (in libsystem_kernel.dylib) + 8 [0x124]
    +   4 wasm[0]::function[123] (in flower) + 20 [0x125]
    +   ! 3 malloc (in libsystem_malloc.dylib) + 4 [0x126]
    +   ! 1 wasm[0]::function[123] (in flower) + 12 [0x127]
    5 Thread_2: reactor
    + 5 kevent (in libsystem_kernel.dylib) + 8 [0x128]

Total number in stack (recursive counted multiple, when >=5):
        100 imaginary_flat_total
Sort by top of stack, same collapsed:
Binary Images:
`;

test("sample tree separates waiting observations and avoids double-counting recursive frames", () => {
  const summary = summarizeSample(RAW);
  assert.equal(summary.available, true);
  assert.equal(summary.totalThreadSamples, 15);
  assert.equal(summary.activeThreadSamples, 4);
  assert.equal(summary.waitingThreadSamples, 11);
  assert.equal(summary.activeFraction, 4 / 15);
  assert.deepEqual(summary.threads.map(({ activeFraction }) => activeFraction), [0.4, 0]);
  assert.deepEqual(summary.activeSelf, [
    { frame: "malloc", samples: 3, fraction: 0.75 },
    { frame: "wasm[0]::function[123]", samples: 1, fraction: 0.25 },
  ]);
  assert.deepEqual(summary.activeInclusive.find(({ frame }) => frame === "wasm[0]::function[123]"), { frame: "wasm[0]::function[123]", samples: 4, fraction: 1 });
  assert.equal(summary.activeCategories.find(({ category }) => category === "Wasmtime / QuickJS guest").samples, 4);
  assert.equal(summary.waitingFrames[0].frame, "__psynch_cvwait");
  assert.equal(summary.waitingFrames[1].samples, 5);
  assert.ok(summary.limitations.some((text) => text.includes("not a measurement of CPU utilization")));
});

test("summary has bounded frames and rejects oversized or inconsistent call trees", () => {
  assert.equal(summarizeSample("No process could be sampled").available, false);
  assert.equal(summarizeSample("Call graph:\n    10 Thread_1\n    + 10 read (in libsystem_kernel.dylib)\n").waitingThreadSamples, 0, "read can block, but cannot be inferred from the symbol alone");
  assert.throws(() => summarizeSample("Call graph:\n    2 Thread_1\n    + 3 work\n"), /Inconsistent/);
  assert.throws(() => summarizeSample("x".repeat(16 * 1_048_576 + 1)), /limit/);
  const many = "Call graph:\n    50 Thread_1\n" + Array.from({ length: 50 }, (_, index) => `    + 1 frame_${index}`).join("\n");
  const summary = summarizeSample(many);
  assert.equal(summary.activeThreadSamples, 50);
  assert.equal(summary.activeInclusive.length, 25);
});

test("durable fcntl stacks are separated from active work, with bounded thread rows and exclusive categories", () => {
  const raw = `Call graph:
    5 Thread_1: worker
    + 5 redb::FileBackend::sync_data (in flower)
    +   5 __fcntl (in libsystem_kernel.dylib)
    2 Thread_2: worker
    + 2 __fcntl (in libsystem_kernel.dylib)
    3 Thread_3: flower-cell
    + 3 wasm[0]::function[123] (in flower)
    +   1 serde_json::serialize (in flower)
    +   2 malloc (in libsystem_malloc.dylib)
`;
  const summary = summarizeSample(raw);
  assert.equal(summary.storageSyncThreadSamples, 5);
  assert.equal(summary.activeThreadSamples, 5, "generic fcntl is not automatically treated as durable sync");
  assert.equal(summary.waitingThreadSamples, 5);
  assert.equal(summary.activeCategories.reduce((total, entry) => total + entry.samples, 0), 5);
  assert.equal(summary.activeCategories.find(({ category }) => category === "JSON serialization / parsing").samples, 1);
  assert.equal(summary.threadGroups.find(({ thread }) => thread === "worker").physicalThreads, 2);
  const manyThreads = summarizeSample("Call graph:\n" + Array.from({ length: 100 }, (_, index) => `    1 Thread_${index}: flower-cell\n    + 1 wasm[0]::function[123]\n`).join(""));
  assert.equal(manyThreads.threads.length, 50);
  assert.equal(manyThreads.omittedThreadRows, 50);
  assert.equal(manyThreads.threadGroups.length, 1);
  assert.equal(manyThreads.threadGroups[0].physicalThreads, 100);
});

test("optional demangling preserves original symbols and degrades to native names on failure", async () => {
  const summary = { activeSelf: [{ frame: "_RustSymbol", samples: 1 }] };
  await demangleSummary(summary, { execute: async (command, args) => {
    assert.equal(command, "/usr/bin/xcrun");
    assert.deepEqual(args, ["llvm-cxxfilt", "_RustSymbol"]);
    return { stdout: "flower::evaluator::invoke_at\n" };
  } });
  assert.equal(summary.activeSelf[0].symbol, "_RustSymbol");
  assert.equal(summary.activeSelf[0].frame, "flower::evaluator::invoke_at");
  const unavailable = { activeSelf: [{ frame: "_RustSymbol" }] };
  await demangleSummary(unavailable, { execute: async () => { throw new Error("no Xcode tools"); } });
  assert.equal(unavailable.activeSelf[0].frame, "_RustSymbol");
  assert.match(unavailable.demangling, /Unavailable/);
});

function mockDependencies() {
  const child = new EventEmitter();
  child.pid = 77;
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.unref = () => {};
  child.kill = () => { setImmediate(() => child.emit("close", null, "SIGTERM")); return true; };
  const calls = [];
  return { child, calls, deps: {
    platform: "darwin", mkdir: async () => {}, writeFile: async () => {},
    stat: async () => ({ size: RAW.length }), readFile: async () => RAW,
    spawn: (...args) => { calls.push(args); return child; },
    timeoutGraceMs: 100, terminateGraceMs: 5, killGraceMs: 5,
  } };
}

const options = { pid: 42, node: 1, outputPath: "/tmp/profile with spaces.txt", durationSeconds: 1 };

test("profiler uses an explicit owned PID and bounded output, retaining raw and summary metadata", async () => {
  const { child, calls, deps } = mockDependencies();
  const handle = await startCpuProfile(options, deps);
  child.stderr.write("x".repeat(20_000));
  child.emit("close", 0, null);
  const report = await handle.done;
  assert.deepEqual(calls, [["/usr/bin/sample", ["42", "1", "1", "-mayDie", "-fullPaths", "-file", options.outputPath], { stdio: ["ignore", "pipe", "pipe"] }]]);
  assert.equal(report.status, "complete");
  assert.equal(report.profilerPid, 77);
  assert.equal(report.stderr.length, 16_384);
  assert.equal(report.rawBytes, RAW.length);
  assert.equal(report.summary.activeThreadSamples, 4);
  assert.equal(report.perturbsPerformance, true);
  assert.ok(report.elapsedMs >= 0);
  assert.equal(await handle.stop(), report, "late cleanup preserves completed result");
});

// Own the clock and acknowledge child exit explicitly. A setImmediate close
// racing a 5ms real timer makes a cooperative fake child appear unresponsive on
// loaded CI runners; these tests instead exercise exact cancellation boundaries.
test("abort terminates only the profiler child and retains a cancelled result", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { child, deps } = mockDependencies();
  const controller = new AbortController();
  const kills = [];
  child.kill = (signal) => { kills.push(signal); return true; };
  const handle = await startCpuProfile({ ...options, signal: controller.signal }, deps);
  controller.abort();
  assert.deepEqual(kills, ["SIGTERM"]);
  t.mock.timers.tick(deps.terminateGraceMs - 1);
  assert.deepEqual(kills, ["SIGTERM"], "the child has its whole termination grace period");
  child.emit("close", null, "SIGTERM");
  t.mock.timers.tick(10_000);
  const report = await handle.done;
  assert.equal(report.status, "cancelled");
  assert.deepEqual(kills, ["SIGTERM"], "acknowledged exit cancels every escalation/deadline timer");
  assert.equal(report.summary.totalThreadSamples, 15, "partial raw profiles can still be summarized");
});

test("unresponsive profiler gets a bounded kill escalation without hanging cleanup", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { child, deps } = mockDependencies();
  const kills = [];
  let unref = false;
  child.kill = (signal) => { kills.push(signal); return true; };
  child.unref = () => { unref = true; };
  const handle = await startCpuProfile(options, deps);
  const done = handle.stop("Test cleanup");
  assert.deepEqual(kills, ["SIGTERM"]);
  t.mock.timers.tick(deps.terminateGraceMs - 1);
  assert.deepEqual(kills, ["SIGTERM"]);
  t.mock.timers.tick(1);
  assert.deepEqual(kills, ["SIGTERM", "SIGKILL"]);
  t.mock.timers.tick(deps.killGraceMs - 1);
  assert.equal(unref, false, "cleanup waits for the SIGKILL acknowledgement grace");
  assert.equal(child.stdout.destroyed, false);
  t.mock.timers.tick(1);
  const report = await done;
  assert.equal(report.cleanupIncomplete, true);
  assert.equal(report.status, "cancelled");
  assert.equal(unref, true);
  assert.equal(child.stdout.destroyed, true);
});

test("acknowledging SIGKILL completes cancellation without abandoning the child", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { child, deps } = mockDependencies();
  const kills = [];
  let unref = false;
  child.kill = (signal) => { kills.push(signal); return true; };
  child.unref = () => { unref = true; };
  const handle = await startCpuProfile(options, deps);
  const done = handle.stop("Test cleanup");
  t.mock.timers.tick(deps.terminateGraceMs);
  assert.deepEqual(kills, ["SIGTERM", "SIGKILL"]);
  child.emit("close", null, "SIGKILL");
  t.mock.timers.tick(10_000);
  const report = await done;
  assert.equal(report.status, "cancelled");
  assert.equal(report.exitSignal, "SIGKILL");
  assert.equal(report.cleanupIncomplete, undefined);
  assert.equal(unref, false);
  assert.equal(child.stdout.destroyed, false);
  assert.deepEqual(kills, ["SIGTERM", "SIGKILL"]);
});

test("a collection timeout terminates the owned profiler and remains a timeout", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { child, deps } = mockDependencies();
  const kills = [];
  child.kill = (signal) => { kills.push(signal); return true; };
  const handle = await startCpuProfile(options, deps);
  t.mock.timers.tick(options.durationSeconds * 1_000 + deps.timeoutGraceMs - 1);
  assert.deepEqual(kills, []);
  t.mock.timers.tick(1);
  assert.deepEqual(kills, ["SIGTERM"]);
  child.emit("close", null, "SIGTERM");
  t.mock.timers.tick(10_000);
  const report = await handle.done;
  assert.equal(report.status, "timed_out");
  assert.deepEqual(kills, ["SIGTERM"]);
});

test("unsupported hosts, missing executable, and absent call trees fail explicitly", async () => {
  const { child, calls, deps } = mockDependencies();
  const unsupported = await startCpuProfile(options, { ...deps, platform: "linux" });
  assert.equal((await unsupported.done).status, "failed");
  assert.equal(calls.length, 0);
  const invalid = await startCpuProfile({ ...options, pid: -1 }, deps);
  assert.equal((await invalid.done).status, "failed");
  const missing = await startCpuProfile(options, { ...deps, spawn: () => { throw new Error("ENOENT"); } });
  assert.equal((await missing.done).error, "ENOENT");
  const empty = await startCpuProfile(options, { ...deps, readFile: async () => "" });
  child.emit("close", 0, null);
  assert.match((await empty.done).summaryError, /No usable thread/);
  assert.equal((await empty.done).status, "failed");
});
