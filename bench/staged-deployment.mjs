#!/usr/bin/env node
// Isolated deployment scaling experiment; never connects to an existing cluster.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { copyFile, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { cpus, platform, release, tmpdir, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";
import { createHttp2Transport } from "../sdk/http2.ts";
import { LocalCluster } from "./cluster.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const help = `Compare direct blocking deployment with staged materialized-root rebuilding.

  node bench/staged-deployment.mjs [options]

  --binary PATH             Server executable (default target/release/flower).
  --output PATH             JSON report (default /tmp/flower-staged-TIMESTAMP.json).
  --label TEXT              Build/run label recorded in the report (default empty).
  --sizes LIST              Independent root counts (default 100,500,1000).
  --modes LIST              direct,staged (default both, in this order).
  --nodes N                 Local replicas: 1 or 3 (default 3).
  --repeats N               Fresh-cluster repetitions per size/mode (default 1).
  --max-bytes N             Staged advance/collect page budget (default 262144).
  --seed-batch N            Roots created per setup mutation (default 100).
  --read-batch N            Roots checked per final query (default 100).
  --write-rate N            Target paced write starts/second, zero disables (default 0).
  --hot-roots N             Fixed write subset, capped at root count (default 8).
  --warmup-ms N             Write warmup before deployment (default 250).
  --request-timeout-ms N    Each HTTP request deadline (default 30000).
  --case-timeout-ms N       Whole case including setup/audit (default 180000).
  --warm-target-bundle      Prepare target code on the empty database during setup.
  --keep-data               Preserve isolated cluster directories and pinned binary.
  --help                    Show this help.

Writes use one paced worker with at most one outstanding mutation. Missed start
slots are counted rather than queued. All nodes and the load generator share one
host. Direct mode uses preparation:blocking, avoiding optimistic conflict retries.
`;

export function parseOptions(args) {
  const options = {
    binary: resolve(root, "target/release/flower"),
    output: join(tmpdir(), `flower-staged-${Date.now()}.json`),
    sizes: [100, 500, 1000], modes: ["direct", "staged"], nodes: 3, repeats: 1,
    maxBytes: 256 * 1024, seedBatch: 100, readBatch: 100, writeRate: 0,
    hotRoots: 8, warmupMs: 250, requestTimeoutMs: 30_000, caseTimeoutMs: 180_000,
    keepData: false, help: false, label: "", warmTargetBundle: false,
  };
  const numbers = new Map([
    ["nodes", "nodes"], ["repeats", "repeats"], ["max-bytes", "maxBytes"],
    ["seed-batch", "seedBatch"], ["read-batch", "readBatch"], ["write-rate", "writeRate"],
    ["hot-roots", "hotRoots"], ["warmup-ms", "warmupMs"],
    ["request-timeout-ms", "requestTimeoutMs"], ["case-timeout-ms", "caseTimeoutMs"],
  ]);
  const seen = new Set();
  for (let n = 0; n < args.length; n++) {
    const flag = args[n];
    if (flag === "--help") { options.help = true; continue; }
    if (!flag.startsWith("--")) throw new Error(`Expected --option, got ${flag}`);
    const name = flag.slice(2);
    if (seen.has(name)) throw new Error(`Duplicate option ${flag}`);
    seen.add(name);
    if (name === "keep-data") { options.keepData = true; continue; }
    if (name === "warm-target-bundle") { options.warmTargetBundle = true; continue; }
    if (!["binary", "output", "label", "sizes", "modes"].includes(name) && !numbers.has(name)) throw new Error(`Unknown option ${flag}`);
    const value = args[++n];
    if (!value || value.startsWith("--")) throw new Error(`${flag} needs a value`);
    if (name === "binary" || name === "output") options[name] = resolve(value);
    else if (name === "label") options.label = value;
    else if (name === "modes") {
      options.modes = value.split(",");
      if (options.modes.some(mode => !["direct", "staged"].includes(mode))) throw new Error("--modes accepts direct,staged");
    } else if (name === "sizes") {
      options.sizes = value.split(",").map(item => integer(item, flag, 1));
    } else options[numbers.get(name)] = integer(value, flag, ["write-rate", "warmup-ms"].includes(name) ? 0 : 1);
  }
  if (![1, 3].includes(options.nodes)) throw new Error("--nodes must be 1 or 3");
  if ([options.requestTimeoutMs, options.caseTimeoutMs, options.warmupMs].some(value => value > 2_147_483_647)) throw new Error("Timeout exceeds the timer range");
  if (new Set(options.modes).size !== options.modes.length || new Set(options.sizes).size !== options.sizes.length) throw new Error("Duplicate modes/sizes are not allowed");
  if (options.binary === options.output) throw new Error("Output must not overwrite the server executable");
  return options;
}

function integer(value, label, minimum) {
  const number = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(number) || number < minimum) throw new Error(`${label} must be an integer >= ${minimum}`);
  return number;
}
const key = id => String(id).padStart(10, "0");
async function hashFile(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}
export function latency(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const percentile = value => sorted.length ? sorted[Math.max(0, Math.ceil(sorted.length * value) - 1)] : null;
  return { count: sorted.length, p50Ms: percentile(.5), p95Ms: percentile(.95), p99Ms: percentile(.99), maxMs: sorted.at(-1) ?? null };
}
function source(version) {
  return `import {collection,define,derive,mutation,query} from ${JSON.stringify(join(root, "sdk/index.ts"))};
const rows=collection("rows"),settings=collection("settings");
const shared=derive("shared",ctx=>ctx.get(settings,"base").offset+${version});
const result=derive("result",(ctx,id)=>{const row=ctx.get(rows,id);return {key:id,value:row.value*ctx.get(shared)};});
export default define({definitions:[shared,result],http:{
 seed:mutation("seed",(ctx,values)=>{if(ctx.get(settings,"base")===null)ctx.set(settings,"base",{offset:10});for(const row of values){ctx.set(rows,row.key,{value:row.value});ctx.materialize(result,row.key);}return values.length;}),
 change:mutation("change",(ctx,input)=>{ctx.set(rows,input.key,{value:input.value});return input.value;}),
 read:query("read",(ctx,ids)=>ids.map(id=>({key:id,source:ctx.get(rows,id),result:ctx.get(result,id)}))),
}});`;
}

async function audit(client, ledger, version, batchSize, requestOptions) {
  const entries = [...ledger];
  for (let start = 0; start < entries.length; start += batchSize) {
    const batch = entries.slice(start, start + batchSize);
    const observed = (await client.query("read", batch.map(([id]) => id), requestOptions())).value;
    assert.deepEqual(observed, batch.map(([id, value]) => ({ key: id, source: { value }, result: { key: id, value: value * (10 + version) } })));
  }
  return { passed: true, checkedRoots: entries.length, independentLedger: true, version };
}

function errorInfo(error) {
  return { name: error.name, code: error.code ?? null, status: error.status ?? null, message: error.message, failure: error.failure ?? null };
}

async function runCase(options, binary, bundles, size, mode, repetition) {
  const cluster = new LocalCluster({ nodes: options.nodes, binary, keepData: options.keepData, requestTimeoutMs: options.requestTimeoutMs });
  const transport = createHttp2Transport({ requestTimeoutMs: options.requestTimeoutMs });
  const caseSignal = AbortSignal.timeout(options.caseTimeoutMs);
  const requestOptions = () => ({ signal: AbortSignal.any([caseSignal, AbortSignal.timeout(options.requestTimeoutMs)]) });
  const result = { roots: size, mode, repetition, nodes: options.nodes, passed: false,
    timings: {}, pages: { advance: 0, backfill: 0, rebuilding: 0, collect: 0 }, writes: null, correctness: null };
  const ledger = new Map(Array.from({ length: size }, (_, id) => [key(id), id + 1]));
  let stop = false, measured = false, sequence = 0, worker = Promise.resolve(), workerError = null;
  let deploymentStart = null, drainStart = null;
  const stopWaiting = new AbortController();
  let measuredStarts = 0, measuredFailures = 0, missedSlots = 0;
  const writeLatencies = [], writeErrors = [], recoveries = [];
  const caseStart = performance.now();
  try {
    await cluster.start();
    const client = new FlowerClient(cluster.url, { fetch: transport.fetch });
    const admin = new FlowerAdmin(cluster.url, { adminToken: cluster.adminToken, fetch: transport.fetch });
    const setupStart = performance.now();
    if (options.warmTargetBundle) {
      await admin.deploy(bundles[1], { ...requestOptions(), requestId: "warm-target", preparation: "blocking" });
    }
    await admin.deploy(bundles[0], { ...requestOptions(), requestId: "initial", preparation: "blocking" });
    const entries = [...ledger];
    for (let start = 0; start < size; start += options.seedBatch) {
      await client.mutate("seed", entries.slice(start, start + options.seedBatch).map(([id, value]) => ({ key: id, value })), {
        ...requestOptions(), requestId: `seed-${start}`,
      });
    }
    await audit(client, ledger, 1, options.readBatch, requestOptions);
    result.timings.setupMs = performance.now() - setupStart;
    const interval = options.writeRate > 0 ? 1000 / options.writeRate : Infinity;
    if (options.writeRate > 0) {
      worker = (async () => {
        let next = performance.now();
        while (!stop) {
          const now = performance.now();
          if (next > now) {
            try { await delay(next - now, undefined, { signal: AbortSignal.any([caseSignal, stopWaiting.signal]) }); }
            catch (error) { if (stop) break; throw error; }
          }
          if (stop) break;
          const started = performance.now(), count = measured;
          const id = key(sequence % Math.min(size, options.hotRoots));
          const input = { key: id, value: size + (++sequence) };
          const requestId = `load-${sequence}`;
          if (count) measuredStarts++;
          let receipt;
          try {
            receipt = await client.mutate("change", input, { ...requestOptions(), requestId });
          } catch (error) {
            if (count) measuredFailures++;
            writeErrors.push({ measured: count, requestId, ...errorInfo(error) });
            // Resolve uncertain outcomes with the identical intent before the
            // worker moves on. Never silently omit a possibly committed write.
            for (let attempt = 1; attempt <= 3 && !receipt; attempt++) {
              try {
                receipt = await client.mutate("change", input, { ...requestOptions(), requestId });
                recoveries.push({ requestId, attempt, duplicate: receipt.duplicate });
              } catch (retryError) {
                if (attempt === 3) throw new Error(`Cannot reconcile ${requestId}: ${retryError.message}`);
              }
            }
          }
          assert.equal(receipt.value, input.value);
          ledger.set(id, input.value);
          if (count) writeLatencies.push(performance.now() - started);
          next += interval;
          const behind = performance.now() - next;
          if (behind >= interval) {
            const skipped = Math.floor(behind / interval);
            if (count) missedSlots += skipped;
            next += skipped * interval;
          }
        }
      })().catch(error => { workerError = error; stop = true; });
      await delay(options.warmupMs, undefined, { signal: caseSignal });
      if (workerError) throw workerError;
    }
    measured = true;
    deploymentStart = performance.now();
    if (mode === "direct") {
      await admin.deploy(bundles[1], { ...requestOptions(), requestId: "upgrade", preparation: "blocking" });
      result.timings.directDeployMs = performance.now() - deploymentStart;
      result.timings.activationMs = null;
    } else {
      const stageStart = performance.now();
      let state = (await admin.stageDeployment(bundles[1], { ...requestOptions(), requestId: "upgrade" })).value;
      result.timings.stageMs = performance.now() - stageStart;
      const preparationStart = performance.now();
      while (state.phase !== "ready") {
        caseSignal.throwIfAborted();
        if (!["backfill", "rebuilding"].includes(state.phase)) throw new Error(`Staged deployment entered ${state.phase}: ${state.error ?? ""}`);
        result.pages[state.phase]++;
        state = (await admin.controlStagedDeployment({ operation: "advance", requestId: "upgrade", maxBytes: options.maxBytes }, requestOptions())).value;
        result.pages.advance++;
      }
      result.timings.prepareMs = performance.now() - preparationStart;
      result.ready = state;
      const activationStart = performance.now();
      await admin.controlStagedDeployment({ operation: "activate", requestId: "upgrade" }, requestOptions());
      result.timings.activationMs = performance.now() - activationStart;
    }
    result.timings.deploymentMs = performance.now() - deploymentStart;
    measured = false;
    stop = true;
    stopWaiting.abort();
    drainStart = performance.now();
    await worker;
    result.timings.writeDrainMs = performance.now() - drainStart;
    if (workerError) throw workerError;
    result.correctness = await audit(client, ledger, 2, options.readBatch, requestOptions);
    if (mode === "staged") {
      const cleanupStart = performance.now();
      let state;
      do {
        state = (await admin.controlStagedDeployment({ operation: "collect", requestId: "upgrade", maxBytes: options.maxBytes }, requestOptions())).value;
        result.pages.collect++;
      } while (state.phase !== "collected");
      result.timings.cleanupMs = performance.now() - cleanupStart;
      await audit(client, ledger, 2, options.readBatch, requestOptions);
    }
    result.passed = true;
  } catch (error) {
    if (deploymentStart !== null && result.timings.deploymentMs === undefined) result.timings.deploymentMs = performance.now() - deploymentStart;
    result.error = errorInfo(error);
    result.logs = cluster.logTails();
    cluster.keepData = true;
  } finally {
    measured = false;
    stop = true;
    stopWaiting.abort();
    drainStart ??= performance.now();
    await worker;
    result.timings.writeDrainMs ??= performance.now() - drainStart;
    const measurementMs = (result.timings.deploymentMs ?? 0) + result.timings.writeDrainMs;
    result.writes = {
      targetStartsPerSecond: options.writeRate, concurrency: 1, hotRoots: Math.min(size, options.hotRoots),
      ...latency(writeLatencies), started: measuredStarts, failedInitialAttempts: measuredFailures,
      unresolved: workerError ? errorInfo(workerError) : null,
      completedPerSecond: measurementMs > 0 ? writeLatencies.length * 1000 / measurementMs : null,
      measurementMs, missedStartSlots: missedSlots, errors: writeErrors, recoveredRequests: recoveries,
      note: "Includes only logical writes started during deployment; latency includes retries and final drain. Failed initial attempts may later succeed with the same ID. The achieved rate denominator includes deployment and outstanding-write drain. No backlog accumulates after missed paced starts.",
    };
    result.timings.caseMs = performance.now() - caseStart;
    result.directory = cluster.keepData ? cluster.directory : null;
    await transport.close();
    await cluster.close();
  }
  return result;
}

export async function run(options) {
  const sourceInfo = await stat(options.binary);
  try {
    const destination = await stat(options.output);
    if (sourceInfo.dev === destination.dev && sourceInfo.ino === destination.ino) throw new Error("Output aliases the server executable");
  } catch (error) { if (error.code !== "ENOENT") throw error; }
  const directory = await mkdtemp(join(tmpdir(), "flower-staged-benchmark-"));
  const binary = join(directory, "flower");
  await copyFile(options.binary, binary);
  const binaryHash = await hashFile(binary);
  const binaryInfo = await stat(binary);
  const report = {
    format: "flower-staged-deployment-benchmark-v1", startedAt: new Date().toISOString(),
    options, binary: { source: options.binary, pinned: binary, sha256: binaryHash, bytes: binaryInfo.size },
    environment: { platform: platform(), release: release(), cpu: cpus()[0]?.model, logicalCpus: cpus().length, memoryBytes: totalmem(), node: process.version,
      runtime: Object.fromEntries(Object.entries(process.env).filter(([name]) => name.startsWith("FLOWER_") && !/TOKEN|SECRET|KEYRING|KEY_FILE|WRAPPING/.test(name))) },
    workload: { independentMaterializedRoots: true, sharedDerivedDependencies: 1, sourceRowsPerRoot: 1,
      oldMultiplier: 11, newMultiplier: 12, indexBackfill: false, rootCollectionScan: false,
      targetCodeWarmedDuringSetup: options.warmTargetBundle,
      note: "Fresh isolated cluster per case, one physical Raft group, all replicas/load generation on one host. Setup/audit/cleanup are excluded from deployment duration. Direct preparation is blocking; staged advances are sequential client requests. No measurement here establishes a production capacity limit." },
    cases: [], passed: true,
  };
  const save = async () => {
    await mkdir(dirname(options.output), { recursive: true });
    await writeFile(options.output, JSON.stringify(report, null, 2) + "\n");
  };
  try {
    const bundles = [];
    for (const version of [1, 2]) {
      const path = join(directory, `version-${version}.ts`);
      await writeFile(path, source(version));
      bundles.push(await buildBundle(path, { initialization: "static" }));
    }
    report.bundleHashes = bundles.map(bundle => bundle.hash);
    for (const size of options.sizes) for (let repetition = 1; repetition <= options.repeats; repetition++) for (const mode of options.modes) {
      console.log(`Running ${mode}, ${size} roots, ${options.nodes} node(s), repetition ${repetition}…`);
      const result = await runCase(options, binary, bundles, size, mode, repetition);
      report.cases.push(result);
      report.passed &&= result.passed;
      await save();
      console.log(JSON.stringify({ roots: size, mode, passed: result.passed, ...result.timings, pages: result.pages.advance, writes: result.writes, error: result.error }));
    }
    assert.equal(await hashFile(binary), binaryHash, "pinned binary changed during the experiment");
    report.completedAt = new Date().toISOString();
    await save();
    console.log(`Report: ${options.output}`);
    return report;
  } finally {
    if (!options.keepData && report.passed) await rm(directory, { recursive: true, force: true });
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const options = parseOptions(process.argv.slice(2));
    if (options.help) console.log(help);
    else if (!(await run(options)).passed) process.exitCode = 1;
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}
