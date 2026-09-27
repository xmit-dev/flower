// ps CPU time is cumulative per PID. Keep only intervals wholly within load,
// without treating a restarted node's counter as a continuation of its old PID.
export function cpuMilliseconds(text) {
  const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$/.exec(text);
  if (!match) return null;
  const [, days = "0", hours = "0", minutes, seconds] = match;
  if (+seconds >= 60 || (match[2] !== undefined && +minutes >= 60)) return null;
  const value = ((+days * 24 + +hours) * 3600 + +minutes * 60 + +seconds) * 1000;
  return Number.isFinite(value) ? value : null;
}

/** Storage commit rates between two `/admin/resources` samples per node. A
 * node that restarted in between (another PID, or counters that went back)
 * is reported as unmeasured rather than partially counted. */
export function storageRates(start, end, seconds) {
  const nodes = {};
  // Older servers report no busy time; count only what every node reports.
  const reported = ["batches", "durableBatches", "stagedWrites", "busyMicros", "commitMicros"]
    .filter((key) => Object.values(start ?? {}).every((node) => Number.isSafeInteger(node[key])));
  const totals = Object.fromEntries(reported.map((key) => [key, 0]));
  let complete = seconds > 0;
  for (const [id, before] of Object.entries(start ?? {})) {
    const after = end?.[id];
    if (!after || after.pid !== before.pid || Object.keys(totals).some((key) =>
      !Number.isSafeInteger(after[key]) || !Number.isSafeInteger(before[key]) || after[key] < before[key])) {
      nodes[id] = null;
      complete = false;
      continue;
    }
    nodes[id] = {};
    for (const key of Object.keys(totals)) {
      totals[key] += after[key] - before[key];
      nodes[id][`${key}PerSecond`] = (after[key] - before[key]) / seconds;
    }
    // The committer's share of wall time: near 1, batches queue behind it.
    if ("busyMicros" in totals) nodes[id].busyFraction = nodes[id].busyMicrosPerSecond / 1e6;
  }
  return { seconds, complete, nodes, ...Object.fromEntries(Object.entries(totals).map(([key, value]) => [`${key}PerSecond`, value / seconds])) };
}

export class ServerCpuSamples {
  previous = new Map();
  totals = new Map();
  record(id, pid, cpuMs, elapsedMs, phase) {
    if (!Number.isFinite(cpuMs) || cpuMs < 0) return;
    const previous = this.previous.get(id);
    this.previous.set(id, { pid, cpuMs, elapsedMs, phase });
    if (phase !== "load" || previous?.phase !== "load" || previous.pid !== pid
      || elapsedMs <= previous.elapsedMs || cpuMs < previous.cpuMs) return;
    const total = this.totals.get(id) ?? { cpuMs: 0, sampledWallMs: 0, intervals: 0 };
    total.cpuMs += cpuMs - previous.cpuMs;
    total.sampledWallMs += elapsedMs - previous.elapsedMs;
    total.intervals++;
    this.totals.set(id, total);
  }
  snapshot() {
    return Object.fromEntries([...this.totals].map(([id, total]) => [id, {
      ...total, meanCores: total.cpuMs / total.sampledWallMs,
    }]));
  }
}
