import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { summarizeApplication } from "./metrics.mjs";

const positive = value => Number.isFinite(value) && value > 0;
const count = value => Number.isSafeInteger(value) && value >= 0;
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => [key, canonical(item)]));
  return value;
}
const stable = value => JSON.stringify(canonical(value));
const workloadKeys = ["groups", "nodes", "duration", "warmup", "concurrency", "offeredRate", "workers", "tenants", "shops", "hotShops", "hotProbability", "maxOrders", "bakeMs", "leaseMs", "duplicateRate", "abandonRate", "pollMs", "requestTimeoutMs", "retryBudgetMs", "seed", "initialization", "queryRouting", "readConsistency", "chaos", "http2"];

/** CPU counters are observed; per-call CPU is an estimate because CPU intervals
 * and customer completion windows do not exactly coincide. Never silently turn
 * missing processes into zero CPU or extrapolate across a zero-length window. */
export function summarizeCpu(report) {
  const aggregate = report.kind === "multi-group";
  const groups = aggregate ? report.groups : [{ name: "group-0", options: report.options, application: summarizeApplication(report), resources: report.resources, offeredLoad: report.offeredLoad }];
  if (!Array.isArray(groups) || !groups.length) throw new Error("Missing benchmark groups");
  const processes = [], issues = [];
  let completed = 0, failed = 0, reads = 0, mutations = 0;
  const addProcess = (group, kind, id, measurement, loadDurationMs) => {
    if (!measurement || !Number.isFinite(measurement.cpuMs) || measurement.cpuMs < 0 || !positive(measurement.sampledWallMs)
      || measurement.sampledWallMs > loadDurationMs + 1 || !count(measurement.intervals) || measurement.intervals === 0) {
      issues.push(`${group}/${kind}/${id}: missing or invalid CPU coverage`);
      processes.push({ group, kind, id, available: false });
      return;
    }
    const meanCores = measurement.cpuMs / measurement.sampledWallMs;
    processes.push({ group, kind, id, available: true, observedCpuMs: measurement.cpuMs,
      sampledWallMs: measurement.sampledWallMs, loadDurationMs,
      intervals: measurement.intervals, coverage: measurement.sampledWallMs / loadDurationMs,
      meanCores, estimatedLoadCpuMs: meanCores * loadDurationMs });
  };
  for (const [index, group] of groups.entries()) {
    const application = group.application;
    if (!application?.available || !positive(application.durationMs) || !count(application.completed) || !count(application.failed)) throw new Error(`Group ${index} has no valid primary customer accounting`);
    if (!count(application.reads?.completed) || !count(application.mutations?.completed)
      || application.reads.completed + application.mutations.completed !== application.completed) throw new Error(`Group ${index} primary call kinds disagree`);
    completed += application.completed;
    failed += application.failed;
    reads += application.reads?.completed ?? 0;
    mutations += application.mutations?.completed ?? 0;
    const resources = group.resources ?? {}, groupName = group.name ?? `group-${index}`;
    const add = (kind, id, measurement) => addProcess(groupName, kind, id, measurement, application.durationMs);
    const nodes = group.options?.nodes ?? report.options?.nodes;
    if (!count(nodes) || nodes < 1) throw new Error(`Group ${index} has no replica count`);
    // Shared hosts serve every group; the coordinator samples them below.
    if (!report.hosts) for (let node = 1; node <= nodes; node++) add("server", String(node), resources.serverCpuSampledLoad?.[node]);
    add("node-controller", "node-driver", resources.nodeDriverCpuSampledLoad);
    if ((group.driver?.kind ?? report.driver?.kind ?? group.options?.driver ?? report.options?.driver) === "rust") add("rust-driver", "rust-driver", resources.nativeDriverCpuSampledLoad);
  }
  if (aggregate && (report.totals?.completed !== completed || report.totals?.failed !== failed || report.options?.groups !== groups.length)) throw new Error("Group counts disagree with aggregate customer accounting");
  const durationMs = aggregate ? report.durationMs : groups[0].application.durationMs;
  if (!positive(durationMs)) throw new Error("Invalid benchmark load duration");
  if (report.hosts) {
    for (let host = 1; host <= report.hosts.count; host++) addProcess("hosts", "server", String(host), report.hosts.serverCpuSampledLoad?.[host], durationMs);
  }
  const summarize = kind => {
    const selected = processes.filter(process => kind === "all" || (kind === "drivers" ? process.kind !== "server" : process.kind === kind));
    const available = selected.length > 0 && selected.every(process => process.available);
    const known = selected.filter(process => process.available);
    const observedCpuMs = known.reduce((sum, process) => sum + process.observedCpuMs, 0);
    const estimatedLoadCpuMs = available ? known.reduce((sum, process) => sum + process.estimatedLoadCpuMs, 0) : null;
    return { available, expectedProcesses: selected.length, measuredProcesses: known.length, observedCpuMs,
      coverageMin: known.length ? Math.min(...known.map(process => process.coverage)) : null,
      coverageMax: known.length ? Math.max(...known.map(process => process.coverage)) : null,
      estimatedLoadCpuMs, estimatedMeanCores: available ? estimatedLoadCpuMs / durationMs : null,
      estimatedCpuUsPerSuccessfulCall: available && completed > 0 ? estimatedLoadCpuMs * 1000 / completed : null };
  };
  return { passed: (report.correctnessPassed ?? report.passed) === true, completed, failed, reads, mutations, durationMs,
    goodputRps: completed * 1000 / durationMs, servers: summarize("server"), drivers: summarize("drivers"), all: summarize("all"),
    processes, issues, offeredLoad: report.offeredLoad ?? null,
    note: "CPU counters include replication, retries, delivery workers and background server work. The denominator counts successful primary customer calls only. Estimated load CPU extrapolates each process's sampled CPU/wall ratio across its group's full load duration; it assumes omitted intervals have similar CPU demand. Edge and restart gaps can bias this estimate. It is not exact per-request CPU attribution. Driver CPU excludes the top-level parent, profiler and OTEL collector." };
}

export function compareCpu(before, after) {
  const baseline = summarizeCpu(before), candidate = summarizeCpu(after), differences = [];
  for (const key of workloadKeys) if (stable(before.options?.[key]) !== stable(after.options?.[key])) differences.push(`workload.${key}`);
  for (const key of ["cpu", "logicalCpus", "os", "arch", "node"]) {
    if (before.environment?.[key] === undefined || after.environment?.[key] === undefined) differences.push(`environment.${key} unavailable`);
    else if (stable(before.environment[key]) !== stable(after.environment[key])) differences.push(`environment.${key}`);
  }
  for (const key of ["bundleHash", "runtime"]) if (before[key] === undefined || after[key] === undefined || stable(before[key]) !== stable(after[key])) differences.push(key);
  if (!before.driver?.binary?.sha256 || !after.driver?.binary?.sha256 || before.driver.binary.sha256 !== after.driver.binary.sha256) differences.push("customer driver binary");
  if (Boolean(before.options?.cpuProfile || before.cpuProfile?.perturbsPerformance) !== Boolean(after.options?.cpuProfile || after.cpuProfile?.perturbsPerformance)) differences.push("CPU profiling");
  const reasons = [...differences.map(value => `Changed or unavailable ${value}`)];
  if (!(before.options?.offeredRate > 0 && after.options?.offeredRate > 0)) reasons.push("Closed-loop load: arrivals change with server speed; use matched --offered-rate for fixed-work CPU comparisons");
  for (const [label, summary, report] of [["Baseline", baseline, before], ["Candidate", candidate, after]]) {
    if (!summary.passed || summary.failed !== 0) reasons.push(`${label} failed correctness or customer calls`);
    if (!summary.servers.available) reasons.push(`${label} has incomplete server CPU measurements`);
    const settings = report.runtime?.settings ?? {};
    const enabled = value => value === "1" || value === "true";
    if (report.options?.cpuProfile || report.cpuProfile?.perturbsPerformance
      || enabled(settings.FLOWER_OTEL_ENABLED) && settings.OTEL_SDK_DISABLED !== "true"
      || Object.entries(settings).some(([key, value]) => key.startsWith("FLOWER_PROFILE_") && enabled(value))) reasons.push(`${label} has diagnostic instrumentation enabled`);
    if (report.options?.offeredRate > 0) {
      const load = summary.offeredLoad;
      if (!load || ![load.offered, load.dispatched, load.completed, load.failed, load.driverDropped].every(count)
        || load.failed !== 0 || load.driverDropped !== 0 || load.offered !== load.dispatched || load.dispatched !== load.completed || load.completed !== summary.completed) reasons.push(`${label} did not successfully serve every offered primary call`);
    }
  }
  if (before.options?.offeredRate > 0 && after.options?.offeredRate > 0 && baseline.completed !== candidate.completed) reasons.push("Successful primary call counts differ under fixed offered load");
  const change = (a, b) => positive(a) && Number.isFinite(b) ? (b / a - 1) * 100 : null;
  return { baseline, candidate, matchedFixedWork: reasons.length === 0, reasons,
    changesPercent: { goodput: change(baseline.goodputRps, candidate.goodputRps),
      estimatedServerCpuPerCall: change(baseline.servers.estimatedCpuUsPerSuccessfulCall, candidate.servers.estimatedCpuUsPerSuccessfulCall),
      estimatedDriverCpuPerCall: change(baseline.drivers.estimatedCpuUsPerSuccessfulCall, candidate.drivers.estimatedCpuUsPerSuccessfulCall) },
    note: "Matching fixed offered work improves interpretability, but does not remove sampling gaps, thermal effects or scheduling noise. Compare coverage and tails, repeat matched runs, and keep builds/profilers out of measurement windows. Different server binaries are intentional and their effects are the subject of the comparison." };
}

export function renderCpuComparison(comparison) {
  const f = (value, places = 2) => Number.isFinite(value) ? value.toFixed(places) : "unavailable";
  const coverage = value => Number.isFinite(value.coverageMin) && Number.isFinite(value.coverageMax) ? `${f(value.coverageMin * 100)}–${f(value.coverageMax * 100)}%` : "unavailable";
  const { baseline: a, candidate: b } = comparison;
  const rows = [
    ["Successful primary calls", a.completed, b.completed], ["Failed primary calls", a.failed, b.failed],
    ["Goodput, calls/sec", f(a.goodputRps), f(b.goodputRps)],
    ["Observed server CPU seconds", f(a.servers.observedCpuMs / 1000), f(b.servers.observedCpuMs / 1000)],
    ["Server coverage per process", coverage(a.servers), coverage(b.servers)],
    ["Estimated server CPU µs / successful call", f(a.servers.estimatedCpuUsPerSuccessfulCall), f(b.servers.estimatedCpuUsPerSuccessfulCall)],
    ["Observed driver CPU seconds", f(a.drivers.observedCpuMs / 1000), f(b.drivers.observedCpuMs / 1000)],
    ["Driver coverage per process", coverage(a.drivers), coverage(b.drivers)],
    ["Estimated driver CPU µs / successful call", f(a.drivers.estimatedCpuUsPerSuccessfulCall), f(b.drivers.estimatedCpuUsPerSuccessfulCall)],
  ];
  return `${comparison.matchedFixedWork ? "Matched fixed offered work" : "Exploratory comparison; fixed-work criteria not met"}\n\n| Measurement | Baseline | Candidate |\n| --- | ---: | ---: |\n${rows.map(row => `| ${row.join(" | ")} |`).join("\n")}\n\nEstimated server CPU per call change: ${f(comparison.changesPercent.estimatedServerCpuPerCall)}%.\n\n${[...comparison.reasons, ...a.issues.map(value => `Baseline: ${value}`), ...b.issues.map(value => `Candidate: ${value}`)].map(reason => `- ${reason}`).join("\n")}\n\n${a.note}\n\n${comparison.note}\n`;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2), json = args[0] === "--json";
  if (json) args.shift();
  if (args.length !== 2) { console.error("Usage: node bench/compare-cpu.mjs [--json] BASELINE.json CANDIDATE.json"); process.exitCode = 1; }
  else {
    try {
      const [before, after] = await Promise.all(args.map(async path => JSON.parse(await readFile(path, "utf8"))));
      const comparison = compareCpu(before, after);
      console.log(json ? JSON.stringify(comparison, null, 2) : renderCpuComparison(comparison));
    } catch (error) { console.error(error.message); process.exitCode = 1; }
  }
}
