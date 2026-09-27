#!/usr/bin/env node
// Run the actual benchmark with a bounded local OpenTelemetry JSON receiver.
import { randomUUID } from "node:crypto";
import { lstat, mkdir, open, readFile, realpath, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { HELP, parseOptions } from "./config.mjs";
import { createCapture, otelResourceAttributes, startCollector } from "./otel-capture.mjs";
import { buildOtelReport, renderOtelReport } from "./otel-report.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const protectedDirectories = [join(root, "bench/results"), join(root, "docs")];
const exists = async path => { try { await lstat(path); return true; } catch (error) { if (error.code === "ENOENT") return false; throw error; } };
async function canonicalDestination(path) {
  try { return await realpath(path); }
  catch (error) { if (error.code !== "ENOENT") throw error; return join(await canonicalDestination(dirname(path)), basename(path)); }
}

export async function prepareOutput(options) {
  const directory = options.json.replace(/\.json$/i, "") + "-otel";
  const outputs = [options.json, options.html, directory];
  if (new Set(outputs).size !== outputs.length) throw new Error("Diagnostic outputs must have distinct paths");
  for (const output of [options.json, options.html]) {
    const suffix = relative(directory, output);
    if (suffix === "" || !suffix.startsWith("..") && !isAbsolute(suffix)) throw new Error("Benchmark reports must be outside the diagnostic artifact directory");
  }
  const inputs = [options.binary, options.driverBinary, options.baseline, options.cpuProfile].filter(Boolean);
  for (const output of outputs) {
    if (inputs.includes(output)) throw new Error("Diagnostic output must not overwrite an input, binary, or CPU profile");
    const canonical = await canonicalDestination(output);
    for (const retained of protectedDirectories) {
      const path = relative(retained, canonical);
      if (path === "" || !path.startsWith("..") && !isAbsolute(path)) throw new Error("OpenTelemetry diagnostics must be outside retained bench/results and docs directories");
    }
    if (await exists(output)) throw new Error(`Diagnostic output already exists: ${output}; choose a fresh --json/--html path`);
  }
  // Reserve the artifact directory before running any benchmark or touching reports.
  await mkdir(dirname(directory), { recursive: true });
  await mkdir(directory);
  return directory;
}

export function captureLimits(environment = process.env) {
  const settings = { maxRecords: ["FLOWER_BENCH_OTEL_MAX_RECORDS", 100_000, 1_000_000], maxBytes: ["FLOWER_BENCH_OTEL_MAX_BYTES", 64 * 1024 * 1024, 512 * 1024 * 1024] };
  return Object.fromEntries(Object.entries(settings).map(([key, [name, fallback, maximum]]) => {
    const raw = environment[name];
    if (raw === undefined) return [key, fallback];
    const value = /^\d+$/.test(raw) ? Number(raw) : NaN;
    if (!Number.isSafeInteger(value) || value < 1 || value > maximum) throw new Error(`${name} must be a positive integer no greater than ${maximum}`);
    return [key, value];
  }));
}

export async function withOtelEnvironment(endpoint, runId, operation, environment = process.env) {
  const overrides = {
    FLOWER_OTEL_ENABLED: "1", OTEL_SDK_DISABLED: "false", OTEL_SERVICE_NAME: "flower",
    OTEL_EXPORTER_OTLP_ENDPOINT: endpoint, OTEL_EXPORTER_OTLP_PROTOCOL: "http/json",
    OTEL_TRACES_SAMPLER: environment.OTEL_TRACES_SAMPLER ?? "parentbased_traceidratio",
    OTEL_TRACES_SAMPLER_ARG: environment.OTEL_TRACES_SAMPLER_ARG ?? "0.01",
    OTEL_METRIC_EXPORT_INTERVAL: environment.OTEL_METRIC_EXPORT_INTERVAL ?? "1000",
    OTEL_BSP_SCHEDULE_DELAY: environment.OTEL_BSP_SCHEDULE_DELAY ?? "1000",
    OTEL_RESOURCE_ATTRIBUTES: otelResourceAttributes(runId),
  };
  // Signal-specific endpoints override the base endpoint. Remove every supplied
  // exporter setting, including headers, so local diagnostics never transmit it.
  const names = new Set([...Object.keys(environment).filter(name => name.startsWith("OTEL_EXPORTER_OTLP")), ...Object.keys(overrides)]);
  const previous = new Map([...names].map(name => [name, environment[name]]));
  try {
    for (const name of names) delete environment[name];
    Object.assign(environment, overrides);
    return await operation();
  } finally {
    for (const [name, value] of previous) { if (value === undefined) delete environment[name]; else environment[name] = value; }
  }
}

async function writeCapture(path, records) {
  const file = await open(path, "wx", 0o600);
  try {
    let chunk = "";
    for (const record of records) {
      chunk += JSON.stringify(record) + "\n";
      if (Buffer.byteLength(chunk) >= 65536) { await file.writeFile(chunk); chunk = ""; }
    }
    if (chunk) await file.writeFile(chunk);
  } finally { await file.close(); }
}

export async function runProfile(argv, { runBenchmark } = {}) {
  const hasJson = argv.some(arg => arg === "--json" || arg.startsWith("--json="));
  const options = parseOptions(argv.includes("--help") || hasJson ? argv : [...argv, "--json", join(tmpdir(), `flower-otel-${randomUUID()}`, "benchmark.json")]);
  if (options.help) {
    console.log("OpenTelemetry diagnostic of the actual workload. Same benchmark flags; defaults to fresh /tmp output. Adds <json>-otel/{capture.ndjson,summary.json,report.html}. Exports sampled traces (default 1%) and unsampled metrics to a bounded local collector. Instrumentation perturbs throughput. FLOWER_BENCH_OTEL_MAX_RECORDS and FLOWER_BENCH_OTEL_MAX_BYTES override the finite retention limits.");
    console.log(HELP); return null;
  }
  const limits = captureLimits();
  const directory = await prepareOutput(options);
  const capture = createCapture({ runId: randomUUID(), ...limits });
  options.otelCapture = { runId: capture.runId, group: 0 };
  const collector = await startCollector({ capture });
  capture.limits = { ...capture.limits, ...collector.limits };
  let benchmark, failure;
  try {
    await withOtelEnvironment(collector.endpoint, capture.runId, async () => {
      const run = runBenchmark ?? (options.groups > 1 ? (await import("./multi-group.mjs")).runGroups : (await import("./goblin-pizza.mjs")).run);
      await run(options);
      benchmark = JSON.parse(await readFile(options.json, "utf8"));
    });
  } catch (error) { failure = error; }
  finally { await collector.close(); }
  // Keep bounded sanitized evidence and counters even when startup/audit fails.
  await writeCapture(join(directory, "capture.ndjson"), capture.records);
  let report;
  if (!failure) {
    try { report = buildOtelReport(capture, benchmark); }
    catch (error) { failure = error; }
  }
  if (failure) {
    // Exceptions can include server log bodies; retain only an explicit status.
    await writeFile(join(directory, "summary.json"), JSON.stringify({ schemaVersion: 1, kind: "flower-otel-diagnostic", passed: false,
      failure: "Benchmark failed or produced no fresh measured load interval", capture: { ...capture.stats, limits: capture.limits } }, null, 2) + "\n", { flag: "wx", mode: 0o600 });
    throw failure;
  }
  await writeFile(join(directory, "summary.json"), JSON.stringify(report, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  await writeFile(join(directory, "report.html"), renderOtelReport(report), { flag: "wx", mode: 0o600 });
  console.log(`OpenTelemetry diagnostic: ${join(directory, "report.html")}`);
  console.log(`Sampled load spans: ${report.sampledSpansInLoad}; metric series: ${report.metricSeries.length}; resources: ${report.resources.length}; capture ${report.passed ? "validated" : "incomplete"}.`);
  for (const reason of report.failures) console.error(reason);
  return { report, directory, passed: report.passed && benchmark.passed };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { const result = await runProfile(process.argv.slice(2)); if (result && !result.passed) process.exitCode = 1; }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
