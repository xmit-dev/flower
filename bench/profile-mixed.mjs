#!/usr/bin/env node
// Trace batch timings for the actual mixed workload across one or more groups.
// Debug tracing perturbs the result; use ordinary benchmark runs for headlines.
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { HELP, parseOptions } from "./config.mjs";
import { evaluatorMetric, summary } from "./profile-metrics.mjs";
import { installMixedObserver, readMixedCaptures } from "./profile-mixed-observer.mjs";
export { boundedLogLines, parseStorageLine } from "./profile-mixed-observer.mjs";

const protectedPaths = (options) => [
  ["json", options.json], ["html", options.html], ["baseline", options.baseline],
  ["binary", options.binary], ["driver-binary", options.driverBinary], ["cpu-profile", options.cpuProfile],
].filter(([, path]) => path);

export function diagnosticPath(options) {
  const output = options.json.replace(/\.json$/i, "") + "-groups.json";
  for (const [name, path] of protectedPaths(options)) {
    if (output === path) throw new Error(`Diagnostic output must not overwrite --${name}`);
  }
  return output;
}

export async function assertNoOutputAlias(output, options) {
  const info = async (path) => {
    try { return await stat(path, { bigint: true }); }
    catch (error) { if (error.code === "ENOENT") return null; throw error; }
  };
  const paths = protectedPaths(options);
  const [destination, ...protectedFiles] = await Promise.all([output, ...paths.map(([, path]) => path)].map(info));
  if (!destination) return;
  for (const [index, file] of protectedFiles.entries()) {
    if (file && file.dev === destination.dev && file.ino === destination.ino) {
      throw new Error(`Diagnostic output must not alias --${paths[index][0]}`);
    }
  }
}

export function assertFreshReport(report, startedAt) {
  const aggregate = Array.isArray(report.groups) && report.groups.length > 0;
  const timestamp = Date.parse(aggregate ? report.loadStartedAt : report.startedAt);
  if (!Number.isFinite(timestamp) || timestamp < startedAt) {
    throw new Error("Benchmark did not produce a fresh report; refusing to reuse an earlier run");
  }
  if (!Number.isFinite(Date.parse(report.loadStartedAt))
    || !Number.isFinite(aggregate ? report.durationMs : report.application?.durationMs)
    || (aggregate ? report.durationMs : report.application.durationMs) <= 0) {
    throw new Error("Benchmark produced no usable measured load interval; no batch diagnostic written");
  }
}

async function main() {
  const options = parseOptions(process.argv.slice(2));
  if (options.help) {
    console.log("Mixed-workload batch diagnostic. Uses the ordinary benchmark flags and writes an additional <json>-groups.json file.");
    console.log(HELP);
    return;
  }
  const output = diagnosticPath(options);
  await assertNoOutputAlias(output, options);
  const startedAt = Date.now();
  const directory = await mkdtemp(join(tmpdir(), "flower-mixed-profile-"));
  const diagnostic = { directory, token: randomUUID(), startedAt,
    maxRecords: Math.max(1, Math.floor(200_000 / options.groups)),
    maxBytes: Math.max(1, Math.floor(64 * 1024 * 1024 / options.groups)),
    startOffsetMs: Math.min(10_000, options.duration * 1000 / 3),
  };
  const priorLog = process.env.FLOWER_BENCH_LOG;
  process.env.FLOWER_BENCH_LOG = "flower::service=debug,flower::storage_profile=debug,flower::evaluator_profile=debug,openraft=warn";
  let report, captures;
  try {
    if (options.groups > 1) {
      const { runGroups } = await import("./multi-group.mjs");
      if (!(await runGroups({ ...options, mixedProfile: diagnostic })).passed) process.exitCode = 1;
    } else {
      const observer = installMixedObserver({ ...diagnostic, group: 0 });
      try {
        const { run } = await import("./goblin-pizza.mjs");
        const result = await run(options, { ready: async () => {
          const start = Date.now(); observer.begin(start); return start;
        } });
        if (!result.passed) process.exitCode = 1;
        await observer.finish(result.runId);
      } finally { observer.restore(); }
    }
    report = JSON.parse(await readFile(options.json, "utf8"));
    assertFreshReport(report, startedAt);
    captures = await readMixedCaptures(diagnostic, report, options.groups);
  } finally {
    if (priorLog === undefined) delete process.env.FLOWER_BENCH_LOG;
    else process.env.FLOWER_BENCH_LOG = priorLog;
    await rm(directory, { recursive: true, force: true });
  }
  const { groups, responses, storage, evaluator, dropped, oversizedLogLines, incompleteLogLines } = captures;
  const application = report.application ?? {
    durationMs: report.durationMs, goodputRps: report.goodputRps, ...report.totals,
  };
  const duration = application.durationMs;
  const start = Math.min(10_000, duration / 3);
  const end = duration - Math.min(2_000, duration / 10);
  const origin = Date.parse(report.loadStartedAt);
  const inside = (record) => record.receivedAt >= origin + start && record.receivedAt < origin + end;
  const windowGroups = groups.filter(inside);
  if (!windowGroups.length) {
    throw new Error("No mutation-group records were captured in the measured window; refusing to write an empty diagnostic");
  }
  const selected = windowGroups.filter((group) => group.group_committed === true);
  const timing = responses.filter(inside);
  const storageRecords = storage.filter(inside);
  const evaluatorRecords = evaluator.filter(inside);
  const storageStages = ["state_lock_us", "io_queue_us", "blocking_queue_us", "prepare_us", "begin_us", "write_us", "encode_us", "flush_us", "publish_us", "work_us", "total_us"];
  const storageKey = (record) => `${options.groups > 1 ? `${record.cluster}:` : ""}${record.node}:${record.operation}`;
  const storageSummary = Object.fromEntries([...new Set(storageRecords.map(storageKey))]
    .map((key) => {
      const records = storageRecords.filter((record) => storageKey(record) === key);
      return [key, { count: records.length, failed: records.filter((record) => !record.ok).length,
        stages: Object.fromEntries(storageStages.map((stage) => [stage, summary(records.map((record) => record[stage]))])) }];
    }));
  const metrics = Object.fromEntries([
    "batch_target_count", "batch_target_us", "batch_queued", "batch_local_unapplied_logs", "batch_quorum_unmatched_logs",
    "group_requests", "group_commands", "group_successor", "group_duplicates", "group_errors", "group_deferred",
    "speculative_candidates", "speculative_reused", "serial_worker_jobs", "serial_worker_requests",
    "read_us", "prepare_us", "stage_us", "fill_wait_us", "commit_us", "group_us",
  ].map((key) => [key, summary(selected.map((group) => group[key]))]));
  const methods = Object.fromEntries([...new Set(timing.map((record) => record.method))].map((method) => [method,
    Object.fromEntries(["writer_wait_us", "evaluation_us", "commit_us"].map((key) => [key,
      summary(timing.filter((record) => record.method === method).map((record) => record[key])),
    ])),
  ]));
  const result = {
    note: "Diagnostic debug tracing of the actual mixed workload. Timings select successful groups and responses; command counts include worker mutations. Window selection uses host log-receipt time; a group can cross a boundary. Group lifetime includes preparation, fill/predecessor waiting, and its own commit; overlapping group lifetimes must not be summed as elapsed wall time. prepare_us excludes fill_wait_us; filling can overlap predecessor commit. writer_wait_us ends when group preparation starts and excludes earlier methods within that group. Speculative candidates count scheduled wave slots, including canceled admission waits; reused counts candidates accepted by the ordered writer. Dropped counts include per-type record/encoded-byte limits divided across groups, and oversized/incomplete log lines. Collection begins at the configured diagnostic window offset; truncated streams may bias the retained sample toward its beginning. This is not an uninstrumented throughput result.",
    binary: report.binary, bundleHash: report.bundleHash, environment: report.environment,
    options: report.options, runtime: report.runtime, application, dropped, oversizedLogLines, incompleteLogLines,
    capture: captures.sources,
    loadStartedAt: report.loadStartedAt, windowMs: { start, end, duration: end - start },
    observedGroups: windowGroups.length, selectedGroups: selected.length,
    observedRaftGroups: [...new Set(windowGroups.map(group => group.group))].sort((a, b) => a - b),
    failedGroups: windowGroups.filter((group) => group.group_committed === false).length,
    unknownGroups: windowGroups.filter((group) => typeof group.group_committed !== "boolean").length,
    commands: selected.every((group) => Number.isSafeInteger(group.group_commands) && group.group_commands >= 0)
      ? selected.reduce((sum, group) => sum + group.group_commands, 0) : null,
    metrics, methods, groups: selected,
    evaluator: { enabled: process.env.FLOWER_PROFILE_EVALUATOR === "1", records: evaluatorRecords,
      summary: Object.fromEntries([...new Set(evaluatorRecords.map(record => `${record.mode}:${record.name}`))].map(name => {
        const records = evaluatorRecords.filter(record => `${record.mode}:${record.name}` === name);
        const fields = [...new Set(records.flatMap(record => Object.keys(record)))].filter(evaluatorMetric);
        return [name, Object.fromEntries(fields.map(key => [key, summary(records.map(record => record[key]))]))];
      })),
      note: "Evaluator wall-clock stages include profiler overhead; nested stages overlap and are not CPU utilization. Records retain group/node identities.",
    },
    storage: { enabled: process.env.FLOWER_PROFILE_STORAGE === "1", summary: storageSummary, records: storageRecords,
      note: "Per-node storage transactions selected by host log-receipt time. flush_us times the redb transaction commit; application checkpoints defer their own fsync, while logs, votes, snapshots, and purge retain immediate durability. Stages overlap with Raft and evaluation; encode_us overlaps prepare_us and/or write_us. Do not sum stages across nodes as transaction wall time." },
  };
  // Reports created during this run may make a previously absent alias visible.
  await assertNoOutputAlias(output, options);
  await writeFile(output, JSON.stringify(result, null, 2) + "\n");
  console.log(`Batch timings: ${output}`);
  console.log(JSON.stringify({ selectedGroups: result.selectedGroups, commands: result.commands, dropped,
    means: Object.fromEntries(Object.entries(metrics).map(([key, value]) => [key, value.mean])),
  }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
