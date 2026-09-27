import { execFile, spawn } from "node:child_process";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { promisify } from "node:util";

const MAX_BYTES = 16 * 1_048_576;
const MAX_NODES = 100_000;
const MAX_DEPTH = 256;
const OUTPUT_TAIL = 16_384;
const WAIT_FRAME = /^(?:__psynch_(?:cvwait|mutexwait|rw_rdlock|rw_wrlock)|__ulock_wait\d*|semaphore_(?:timed)?wait_trap|__semwait_signal|mach_msg(?:2)?_trap|kevent(?:64|_qos)?|_?poll|_?select|nanosleep|__nanosleep|pthread_cond_(?:timed)?wait|pthread_join)(?:$|\b)/;

function categoryFor(frames) {
  // The nearest recognized subsystem owns the observation, so buckets do not overlap.
  for (const frame of frames) {
    if (/serde_json|json_parse|json_stringify|JS_(?:ParseJSON|JSONStringify)/.test(frame)) return "JSON serialization / parsing";
    if (/sha2|sha256|compress256|blake3/.test(frame)) return "Hashing";
    if (/redb|flower.*consensus.*store/.test(frame)) return "Storage / Raft persistence";
    if (/^(?:_?JS_|__JS_|js_|lre_|json_|free_gc_object|mark_children|gc_|add_shape|add_property|string_cmp)/.test(frame)) return "QuickJS engine";
    if (/wasmtime|cranelift|wasm\[/.test(frame)) return "Wasmtime / QuickJS guest";
    if (/flower.*evaluator/.test(frame)) return "Flower evaluator host code";
  }
  return "Other native work";
}

function frameName(value) {
  return value.replace(/\s+\(in [\s\S]*$/, "").replace(/\s+\+\s+\d+[\s\S]*$/, "").replace(/\s+\[0x[\s\S]*$/, "").trim();
}

/**
 * Summarize macOS sample's all-thread call tree, not its overlapping flat totals.
 * Counts are thread observations, never CPU milliseconds or CPU utilization.
 * A stack containing a recognized blocking primitive is classified as waiting;
 * everything else is a candidate active stack (including unrecognized blocking).
 */
export function summarizeSample(text) {
  if (typeof text !== "string") throw new TypeError("sample output must be text");
  if (Buffer.byteLength(text) > MAX_BYTES) throw new RangeError("sample output exceeds the 16 MiB summary limit");
  const nodes = [];
  const stack = [];
  let inGraph = false;
  for (const line of text.split(/\r?\n/)) {
    if (/^Call graph:/.test(line)) { inGraph = true; continue; }
    if (!inGraph) continue;
    if (/^\s*(?:Total number in stack|Sort by top of stack|Binary Images:)/.test(line)) break;
    const match = line.match(/^([\s+!:|]*)(\d+)\s+(.+)$/);
    if (!match) continue;
    const depth = match[1].replaceAll("\t", "    ").length;
    const samples = Number(match[2]);
    if (!Number.isSafeInteger(samples) || samples < 0) throw new Error("Invalid sample count");
    const name = frameName(match[3]);
    const thread = /^Thread_/.test(name);
    while (stack.length && (thread || nodes[stack.at(-1)].depth >= depth)) stack.pop();
    // Ignore flat totals or malformed non-thread roots outside the call tree.
    if (!thread && !stack.length) continue;
    if (nodes.length >= MAX_NODES || stack.length >= MAX_DEPTH) throw new RangeError("sample call tree exceeds summary bounds");
    const parent = stack.at(-1);
    if (parent !== undefined) nodes[parent].children += samples;
    nodes.push({ name, depth, samples, children: 0, parent, thread });
    stack.push(nodes.length - 1);
  }
  const activeInclusive = new Map();
  const activeSelf = new Map();
  const waiting = new Map();
  const categories = new Map();
  const threads = new Map();
  let totalThreadSamples = 0;
  let waitingThreadSamples = 0;
  let storageSyncThreadSamples = 0;
  const add = (map, name, samples) => map.set(name, (map.get(name) ?? 0) + samples);
  for (let index = 0; index < nodes.length; index++) {
    const node = nodes[index];
    if (node.children > node.samples) throw new Error("Inconsistent sample call-tree counts");
    const self = node.samples - node.children;
    if (!self) continue;
    totalThreadSamples += self;
    const frames = [];
    let threadName;
    for (let cursor = index; cursor !== undefined; cursor = nodes[cursor].parent) {
      if (!nodes[cursor].thread) frames.push(nodes[cursor].name);
      else threadName = nodes[cursor].name;
    }
    if (!threads.has(threadName)) threads.set(threadName, { thread: threadName, totalThreadSamples: 0, activeThreadSamples: 0, waitingThreadSamples: 0 });
    const thread = threads.get(threadName);
    thread.totalThreadSamples += self;
    // Choose the deepest recognized blocker, counting each stack exactly once.
    const durableSync = frames.some((name) => /^(?:__)?fcntl$/.test(name)) && frames.some((name) => /redb/.test(name) && /sync_data|durable_commit|file_backend/.test(name));
    const blocker = frames.find((name) => WAIT_FRAME.test(name)) ?? (durableSync ? "fcntl during durable storage sync (blocking-capable)" : null);
    if (blocker) {
      waitingThreadSamples += self;
      if (durableSync) storageSyncThreadSamples += self;
      thread.waitingThreadSamples += self;
      add(waiting, blocker, self);
    } else {
      thread.activeThreadSamples += self;
      add(categories, categoryFor(frames), self);
      add(activeSelf, frames[0] ?? "[unresolved thread stack]", self);
      for (const name of new Set(frames)) add(activeInclusive, name, self);
    }
  }
  const activeThreadSamples = totalThreadSamples - waitingThreadSamples;
  const ranked = (map, denominator) => [...map.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 25).map(([frame, samples]) => ({ frame, samples, fraction: denominator ? samples / denominator : null }));
  const threadGroups = new Map();
  for (const thread of threads.values()) {
    const name = thread.thread.replace(/^Thread_[^\s:]+(?::\s*)?/, "").trim() || "Unnamed thread";
    if (!threadGroups.has(name)) threadGroups.set(name, { thread: name, physicalThreads: 0, totalThreadSamples: 0, activeThreadSamples: 0, waitingThreadSamples: 0 });
    const group = threadGroups.get(name);
    group.physicalThreads++;
    for (const field of ["totalThreadSamples", "activeThreadSamples", "waitingThreadSamples"]) group[field] += thread[field];
  }
  const activity = (thread) => ({ ...thread, activeFraction: thread.totalThreadSamples ? thread.activeThreadSamples / thread.totalThreadSamples : null });
  return {
    available: totalThreadSamples > 0,
    unit: "thread stack observations",
    totalThreadSamples, activeThreadSamples, waitingThreadSamples, storageSyncThreadSamples,
    activeFraction: totalThreadSamples ? activeThreadSamples / totalThreadSamples : null,
    waitingFraction: totalThreadSamples ? waitingThreadSamples / totalThreadSamples : null,
    parsedTreeNodes: nodes.length,
    threadCount: threads.size,
    omittedThreadRows: Math.max(0, threads.size - 50),
    threads: [...threads.values()].sort((a, b) => b.activeThreadSamples - a.activeThreadSamples || b.totalThreadSamples - a.totalThreadSamples).slice(0, 50).map(activity),
    threadGroups: [...threadGroups.values()].sort((a, b) => b.activeThreadSamples - a.activeThreadSamples).slice(0, 50).map(activity),
    activeCategories: ranked(categories, activeThreadSamples).map(({ frame, ...rest }) => ({ category: frame, ...rest })),
    activeInclusive: ranked(activeInclusive, activeThreadSamples),
    activeSelf: ranked(activeSelf, activeThreadSamples),
    waitingFrames: ranked(waiting, waitingThreadSamples),
    limitations: [
      "All-thread wall sampling is not a measurement of CPU utilization or on-CPU time.",
      "Active means no recognized waiting primitive in the stack; unrecognized blocking can remain.",
      "fcntl beneath redb durable sync is classified as blocking-capable storage sync, not CPU work; samples cannot separate its kernel execution from I/O waiting.",
      "Active categories are mutually exclusive, assigned to the nearest recognized subsystem frame; they are heuristic native-stack attribution, not TypeScript source profiling.",
      "Inclusive frame counts overlap. A recursive frame is counted once per stack observation.",
      "Only the initially selected leader process is sampled; followers and a replacement leader are excluded.",
    ],
  };
}

/** Optional native demangling only affects display names; raw symbols remain available. */
export async function demangleSummary(summary, { execute = promisify(execFile) } = {}) {
  const entries = [...(summary.activeInclusive ?? []), ...(summary.activeSelf ?? []), ...(summary.waitingFrames ?? [])];
  const symbols = [...new Set(entries.map(({ frame }) => frame).filter((frame) => /^_R|^_ZN/.test(frame)))];
  if (!symbols.length) return summary;
  try {
    const { stdout } = await execute("/usr/bin/xcrun", ["llvm-cxxfilt", ...symbols], { timeout: 5_000, maxBuffer: 1_048_576 });
    const names = stdout.trimEnd().split("\n");
    if (names.length !== symbols.length) throw new Error("Unexpected llvm-cxxfilt output count");
    const mapping = new Map(symbols.map((symbol, index) => [symbol, names[index]]));
    for (const entry of entries) {
      const name = mapping.get(entry.frame);
      if (name && name !== entry.frame) { entry.symbol = entry.frame; entry.frame = name; }
    }
    summary.demangling = "xcrun llvm-cxxfilt; original symbols retained";
  } catch (error) { summary.demangling = `Unavailable: ${String(error.message ?? error).slice(0, 500)}`; }
  return summary;
}

/** Launch only an explicitly owned PID. The returned promise never rejects. */
export async function startCpuProfile({ pid, node, outputPath, durationSeconds, intervalMs = 1, signal }, dependencies = {}) {
  const deps = {
    platform: process.platform, spawn, mkdir, readFile, stat, writeFile,
    timeoutGraceMs: 15_000, terminateGraceMs: 1_000, killGraceMs: 1_000,
    ...dependencies,
  };
  const report = {
    status: "starting", tool: "/usr/bin/sample", pid, node, outputPath,
    requestedDurationSeconds: durationSeconds, intervalMs,
    startedAt: new Date().toISOString(),
    scope: "Initial leader only; launched at load start, including initial order growth. Attaching and symbolication can extend the profiler process beyond its sampling interval.",
    perturbsPerformance: true,
  };
  const started = performance.now();
  const complete = () => {
    report.finishedAt = new Date().toISOString();
    report.elapsedMs = performance.now() - started;
    return report;
  };
  try {
    if (deps.platform !== "darwin") throw new Error("--cpu-profile requires macOS /usr/bin/sample");
    if (!Number.isSafeInteger(pid) || pid < 1) throw new Error("Profiler requires an owned positive PID");
    if (!Number.isSafeInteger(durationSeconds) || durationSeconds < 1 || durationSeconds > 60) throw new Error("Profile duration must be 1–60 whole seconds");
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1 || intervalMs > 1_000) throw new Error("Profile interval must be 1–1000 whole milliseconds");
    if (typeof outputPath !== "string" || !outputPath) throw new Error("Profiler requires an output path");
    if (signal?.aborted) throw new Error("Profile cancelled before launch");
    await deps.mkdir(dirname(outputPath), { recursive: true });
    // Prevent a failed collection from being mistaken for a previous raw profile.
    await deps.writeFile(outputPath, "");
    if (signal?.aborted) throw new Error("Profile cancelled before launch");
  } catch (error) {
    report.status = "failed";
    report.error = String(error.message ?? error);
    const done = Promise.resolve(complete());
    return { done, stop: () => done };
  }

  report.args = [String(pid), String(durationSeconds), String(intervalMs), "-mayDie", "-fullPaths", "-file", outputPath];
  let child;
  try { child = deps.spawn(report.tool, report.args, { stdio: ["ignore", "pipe", "pipe"] }); }
  catch (error) {
    report.status = "failed";
    report.error = String(error.message ?? error);
    const done = Promise.resolve(complete());
    return { done, stop: () => done };
  }
  report.profilerPid = child.pid ?? null;
  report.status = "running";
  let ended = false;
  let stopping = false;
  let timeout;
  let terminateTimer;
  let killTimer;
  let resolveClosed;
  const closed = new Promise((resolve) => { resolveClosed = resolve; });
  const onStdout = (chunk) => { report.stdout = ((report.stdout ?? "") + chunk.toString()).slice(-OUTPUT_TAIL); };
  const onStderr = (chunk) => { report.stderr = ((report.stderr ?? "") + chunk.toString()).slice(-OUTPUT_TAIL); };
  child.stdout?.on("data", onStdout);
  child.stderr?.on("data", onStderr);
  const finish = () => {
    if (ended) return;
    ended = true;
    clearTimeout(timeout);
    clearTimeout(terminateTimer);
    clearTimeout(killTimer);
    signal?.removeEventListener("abort", onAbort);
    child.stdout?.removeListener("data", onStdout);
    child.stderr?.removeListener("data", onStderr);
    resolveClosed();
  };
  const stopChild = (reason, status = "cancelled") => {
    if (ended || stopping) return;
    stopping = true;
    report.status = status;
    report.error = reason;
    try { child.kill("SIGTERM"); } catch { /* The process may already have exited. */ }
    if (ended) return;
    terminateTimer = setTimeout(() => {
      try { child.kill("SIGKILL"); } catch { /* The process may already have exited. */ }
      if (ended) return;
      killTimer = setTimeout(() => {
        // A wedged OS child must not keep the benchmark cleanup open indefinitely.
        report.error += "; profiler did not acknowledge SIGKILL before cleanup deadline";
        report.cleanupIncomplete = true;
        child.stdout?.destroy();
        child.stderr?.destroy();
        child.unref();
        finish();
      }, deps.killGraceMs);
    }, deps.terminateGraceMs);
  };
  const onAbort = () => stopChild("Profile interrupted with benchmark");
  child.on("error", (error) => {
    report.status = "failed";
    report.error = String(error.message ?? error);
    // Spawn errors have no process to reap. A kill error can still have a child.
    if (!child.pid) finish();
    else stopChild(report.error, "failed");
  });
  child.once("close", (code, exitSignal) => {
    report.exitCode = code;
    report.exitSignal = exitSignal;
    if (!stopping) {
      report.status = code === 0 ? "collected" : "failed";
      if (code !== 0) report.error = `sample exited with ${exitSignal ?? `code ${code}`}`;
    }
    finish();
  });
  timeout = setTimeout(() => stopChild("Profiler exceeded its collection and symbolication deadline", "timed_out"), durationSeconds * 1_000 + deps.timeoutGraceMs);
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted) onAbort();
  const done = closed.then(async () => {
    try {
      const info = await deps.stat(outputPath);
      report.rawBytes = info.size;
      if (info.size > MAX_BYTES) throw new Error("Raw profile retained; exceeds the 16 MiB summary limit");
      report.summary = await demangleSummary(summarizeSample(await deps.readFile(outputPath, "utf8")));
      if (!report.summary.available) throw new Error("No usable thread call tree found in sample output");
      if (report.status === "collected") report.status = "complete";
    } catch (error) {
      report.summaryError = String(error.message ?? error);
      if (report.status === "collected") report.status = "failed";
    }
    return complete();
  });
  return { done, stop: (reason = "Profile stopped during benchmark cleanup") => { stopChild(reason); return done; } };
}
