import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

/** Owned subprocess; only fixed setup/start messages cross stdin. The private
 * admin token is never placed in argv, stdout, report configuration, or logs. */
export async function startRustDriver(cluster, options, { signal, onInterval, spawnProcess = spawn } = {}) {
  signal?.throwIfAborted();
  const child = spawnProcess(options.driverBinary, [], { stdio: ["pipe", "pipe", "pipe"] });
  let readyResolve, readyReject, doneResolve, doneReject, exitResolve;
  const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  const done = new Promise((resolve, reject) => { doneResolve = resolve; doneReject = reject; });
  const exited = new Promise((resolve) => { exitResolve = resolve; });
  // Readiness can fail before the caller receives the measured-run promise.
  done.catch(() => {});
  let stopping, killTimer, startupTimer, failure, completed, warmupRequests = 0, started = false, isReady = false;
  let stderr = "";
  const stop = () => {
    if (stopping) return stopping;
    stopping = exited;
    child.stdin.destroy();
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM");
      killTimer = setTimeout(() => child.kill("SIGKILL"), 1000);
      killTimer.unref();
    }
    return stopping;
  };
  const fail = (error) => {
    failure ??= error instanceof Error ? error : new Error(String(error));
    readyReject(failure);
    doneReject(failure);
    void stop();
  };
  const abort = () => fail(signal.reason ?? new Error("Rust driver interrupted"));
  const lines = createInterface({ input: child.stdout });
  lines.on("line", (line) => {
    try {
      const message = JSON.parse(line);
      if (message.type === "ready") {
        if (isReady || message.schemaVersion !== 1 || !Number.isSafeInteger(message.warmupRequests)) throw new Error("Invalid Rust driver readiness");
        isReady = true;
        warmupRequests = message.warmupRequests;
        clearTimeout(startupTimer);
        readyResolve();
      } else if (["interval", "done"].includes(message.type)) {
        if (!started || completed || !Number.isFinite(message.elapsedMs) || message.elapsedMs < 0) throw new Error("Invalid Rust driver interval ordering");
        onInterval?.(message);
        if (message.type === "done") completed = message;
      } else throw new Error("Unknown Rust driver message");
    } catch (error) { fail(error); }
  });
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (text) => { stderr = (stderr + text).slice(-16_000); });
  child.once("error", fail);
  child.stdin.on("error", fail);
  child.once("close", (code, exitSignal) => {
    clearTimeout(killTimer);
    clearTimeout(startupTimer);
    signal?.removeEventListener("abort", abort);
    lines.close();
    try { if (Number.isSafeInteger(child.pid)) options.onProcess?.({ type: "exit", pid: child.pid }); }
    catch (error) { failure ??= error; }
    const error = failure ?? (code !== 0 || !completed ? new Error(`Rust driver exited without a complete result (${code ?? exitSignal}): ${stderr}`) : null);
    if (error) { readyReject(error); doneReject(error); } else doneResolve(completed);
    exitResolve();
  });
  try {
    if (Number.isSafeInteger(child.pid)) options.onProcess?.({ type: "spawn", pid: child.pid });
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    startupTimer = setTimeout(() => fail(new Error("Rust driver startup timed out")), options.startupTimeoutMs ?? 30_000);
    startupTimer.unref();
    child.stdin.write(JSON.stringify({ schemaVersion: 1,
      members: cluster.members.map(({ id, url }) => ({ id, url })), leaderId: cluster.leader.id,
      adminToken: cluster.adminToken, concurrency: options.concurrency, offeredRate:options.offeredRate??0,
      tenantIds: options.tenantIds ?? Array.from({ length: options.tenants }, (_, index) => `tenant-${index}`),
      shops: options.shops, hotShops: options.hotShops, hotProbability: options.hotProbability,
      maxOrders: options.maxOrders, duplicateRate: options.duplicateRate, pollMs: options.pollMs,
      requestTimeoutMs: options.requestTimeoutMs, retryBudgetMs: options.retryBudgetMs,
      http2: options.http2, queryRouting: options.queryRouting, readConsistency: options.readConsistency, seed: options.seed,
    }) + "\n");
    await ready;
  } catch (error) { fail(error); await exited; throw error; }
  return {
    pid: child.pid,
    warmupRequests,
    get completed() { return completed; },
    start(deadlineUnixMs) {
      if (started) throw new Error("Rust driver was already started");
      if (failure) return Promise.reject(failure);
      if (!Number.isSafeInteger(deadlineUnixMs) || deadlineUnixMs <= Date.now()) throw new TypeError("Rust driver deadline must be a future integer timestamp");
      started = true;
      child.stdin.end(JSON.stringify({ type: "start", deadlineUnixMs }) + "\n");
      return done;
    },
    close: stop,
  };
}

export function mergeRouting(left, right) {
  if (!right) return left;
  if (left.mode !== right.mode || left.consistency !== right.consistency) throw new Error("Driver routing policy mismatch");
  const nodes = new Map(left.nodes.map((node) => [node.url, { ...node }]));
  for (const node of right.nodes) {
    const entry = nodes.get(node.url) ?? { id: node.id, url: node.url, attempts: 0, completed: 0, failures: 0 };
    if (entry.id !== node.id || ![node.attempts, node.completed, node.failures].every((count) => Number.isSafeInteger(count) && count >= 0) ||
        node.attempts !== node.completed + node.failures) throw new Error("Invalid native routing counts");
    for (const key of ["attempts", "completed", "failures"]) entry[key] += node[key];
    nodes.set(node.url, entry);
  }
  return { ...left, nodes: [...nodes.values()] };
}
