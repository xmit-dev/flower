#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { arch, cpus, platform, release } from "node:os";
import { dirname, relative, resolve } from "node:path";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { auditWorld } from "./audit.mjs";
import { LocalCluster } from "./cluster.mjs";
import { HELP, parseOptions } from "./config.mjs";
import { startCpuProfile } from "./cpu-profile.mjs";
import { ENGINES, goblinBundle } from "./guests.mjs";
import { createRandom, Histogram, Stats, summarizeApplication } from "./metrics.mjs";
import { BenchmarkClient, RpcError } from "./rpc.mjs";
import { renderReport } from "./report.mjs";
import { cpuMilliseconds, ServerCpuSamples, storageRates } from "./resources.mjs";
import { runtimeSettings } from "./runtime-settings.mjs";
import { mergeRouting, startRustDriver } from "./rust-driver.mjs";

const execute = promisify(execFile);
const methods = ["deploy", "pizza.setup", "pizza.order", "pizza.tip", "pizza.shop", "pizza.shop.local", "pizza.claim", "pizza.deliver", "pizza.world"];
const lostLease = (error) => error instanceof RpcError && error.status === 422 && error.failure?.code === "LEASE_LOST";
const format = (value) => value === null || value === undefined ? "n/a" : value.toFixed(1);

function newPhase() {
  const now = performance.now();
  return { started: now, ended: now, stats: new Stats({ methods }) };
}

async function fingerprintBinary(path, signal) {
  const info = await stat(path);
  const digest = createHash("sha256");
  for await (const chunk of createReadStream(path, { signal })) digest.update(chunk);
  return { path, sha256: digest.digest("hex"), bytes: info.size, modifiedAt: info.mtime.toISOString() };
}

export async function run(options, { ready, hostCrash } = {}) {
  // Read comparisons before creating anything, including report output files.
  const baseline = options.baseline ? JSON.parse(await readFile(options.baseline, "utf8")) : undefined;
  const controller = new AbortController();
  const signal = controller.signal;
  const workerController = new AbortController();
  const workerSignal = AbortSignal.any([signal, workerController.signal]);
  const cluster = new LocalCluster(options);
  const phases = { setup: newPhase() };
  let phase = phases.setup;
  const client = new BenchmarkClient(cluster, options, signal, () => phase);
  const tasks = [];
  const orders = new Map();
  const tenantIds = options.tenantIds ?? Array.from({ length: options.tenants }, (_, i) => `tenant-${i}`);
  const tips = {};
  const readMethod = options.readConsistency === "replica-local" ? "pizza.shop.local" : "pizza.shop";
  const abandoned = new Map();
  const counters = { issuedOrders: 0, delivered: 0, replayChecks: 0, claims: 0, emptyClaims: 0, reclaimed: 0, abandoned: 0, lostLeases: 0, staleLeaseChecks: 0 };
  const failures = [];
  let failureCount = 0;
  let stopWorkers = false;
  let settings;
  let world;
  let loadStarted;
  let loadDeadline;
  let deliveryFinished;
  let resourceTimer;
  let cpuProfile;
  let rustDriver;
  let nativeDriverRssPeak = 0;
  const nativeDriverCpu = new ServerCpuSamples();
  const nodeDriverCpu = new ServerCpuSamples();
  let resourceTask = Promise.resolve();
  let samplingResources = false;
  let previousSample;
  const rssPeaks = {};
  const serverCpu = new ServerCpuSamples();
  let driverRssPeak = process.memoryUsage().rss;
  const eventLoop = monitorEventLoopDelay({ resolution: 20 });
  const cpuStart = process.cpuUsage();
  const report = {
    schemaVersion: 1, runId: options.runId ?? randomUUID(), startedAt: new Date().toISOString(), options,
    runtime: { engine: ENGINES[options.guest ?? "js"],
      settings: runtimeSettings() },
    environment: { node: process.version, os: `${platform()} ${release()}`, arch: arch(), cpu: cpus()[0]?.model, logicalCpus: cpus().length, colocatedNodes: options.nodes },
    passed: false, timeline: [],
  };
  const failed = (error, context) => {
    failureCount++;
    if (failures.length < 50) failures.push({ context, message: String(error.message ?? error).slice(0, 2_000), code: error.code ?? null });
  };
  const sleep = (ms) => delay(ms, undefined, { signal });
  const interrupt = () => {
    controller.abort(new Error("Benchmark interrupted"));
    void cluster.close().catch((error) => failed(error, "interrupt cleanup"));
  };
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);

  async function replay(name, args, requestId, original, random, callSignal = signal) {
    if (random() >= options.duplicateRate) return;
    const repeated = await client.call(name, args, { requestId, replay: true, signal: callSignal });
    assert.equal(repeated.duplicate, true, `${name}: repeated request must be deduplicated`);
    assert.equal(repeated.revision, original.revision, `${name}: replay must preserve revision`);
    assert.deepEqual(repeated.value, original.value, `${name}: replay must preserve original result`);
    counters.replayChecks++;
  }

  async function worker(index) {
    const random = createRandom(`${options.seed}:drone:${index}`);
    let cursor = index;
    while (!workerSignal.aborted && !stopWorkers) {
      try {
        const tenant = tenantIds[cursor++ % tenantIds.length];
        const { value: claim } = await client.call("pizza.claim", { tenant, owner: `drone-${index}` }, { signal: workerSignal });
        if (!claim) {
          counters.emptyClaims++;
          await delay(options.pollMs, undefined, { signal: workerSignal });
          continue;
        }
        counters.claims++;
        if (claim.attempt > 1) counters.reclaimed++;
        const identity = { tenant, id: claim.id, owner: claim.owner, token: claim.token };
        // Always exercise one abandoned lease when enabled; later choices are seeded.
        if (options.abandonRate > 0 && claim.attempt === 1 &&
            (abandoned.size === 0 || random() < options.abandonRate)) {
          abandoned.set(claim.id, identity);
          counters.abandoned++;
          continue;
        }
        await delay(5 + Math.floor(random() * 25), undefined, { signal: workerSignal });
        const requestId = client.id();
        let result;
        try { result = await client.call("pizza.deliver", identity, { requestId, signal: workerSignal }); }
        catch (error) {
          if (!lostLease(error)) throw error;
          counters.lostLeases++;
          continue;
        }
        counters.delivered++;
        await replay("pizza.deliver", identity, requestId, result, random, workerSignal);
      } catch (error) {
        if (workerSignal.aborted) return;
        failed(error, `drone ${index}`);
        await delay(options.pollMs, undefined, { signal: workerSignal });
      }
    }
  }

  async function sampleResources() {
    const cpu = process.cpuUsage();
    nodeDriverCpu.record("node-driver", process.pid, (cpu.user + cpu.system) / 1000, performance.now(),
      Object.entries(phases).find(([, value]) => value === phase)?.[0]);
    driverRssPeak = Math.max(driverRssPeak, process.memoryUsage().rss);
    if (client.aggregate) {
      const elapsedMs = performance.now() - loadStarted;
      const current = client.aggregate.snapshot(elapsedMs);
      if (previousSample) {
        const seconds = (elapsedMs - previousSample.elapsedMs) / 1_000;
        report.timeline.push({
          elapsedMs, phase: Object.entries(phases).find(([, value]) => value === phase)?.[0],
          attemptsPerSecond: (current.attempts - previousSample.attempts) / seconds,
          successesPerSecond: (current.successes - previousSample.successes) / seconds,
          logicalPerSecond: (current.operations.completed - previousSample.completed) / seconds,
          retries: current.retries - previousSample.retries, failures: current.failures - previousSample.failures,
          p95Ms: current.latencyMs.p95,
          ordersAcknowledged: orders.size, deliveriesAcknowledged: counters.delivered,
          driverRssMiB: process.memoryUsage().rss / 1_048_576,
        });
      }
      previousSample = { ...current, elapsedMs, completed: current.operations.completed };
    }
    const pids = [...cluster.pids, ...(rustDriver && !rustDriver.completed ? [{ id: "rust-driver", pid: rustDriver.pid }] : [])];
    if (!pids.length) return;
    try {
      const samplePhase = Object.entries(phases).find(([, value]) => value === phase)?.[0];
      const sampledAt = performance.now();
      const { stdout } = await execute("ps", ["-o", "pid=,rss=,time=", "-p", pids.map(({ pid }) => pid).join(",")], { timeout: 2_000 });
      const completedPhase = Object.entries(phases).find(([, value]) => value === phase)?.[0];
      for (const line of stdout.trim().split("\n")) {
        const [pidText, rssText, cpuText] = line.trim().split(/\s+/);
        const pid = Number(pidText), kib = Number(rssText);
        const id = pids.find((node) => node.pid === pid)?.id;
        if (id && Number.isFinite(kib)) {
          if (id === "rust-driver") nativeDriverRssPeak = Math.max(nativeDriverRssPeak, kib / 1_024);
          else rssPeaks[id] = Math.max(rssPeaks[id] ?? 0, kib / 1_024);
        }
        const cpuMs = cpuMilliseconds(cpuText ?? "");
        if (id && cpuMs !== null) (id === "rust-driver" ? nativeDriverCpu : serverCpu).record(id, pid, cpuMs, sampledAt,
          samplePhase === completedPhase ? samplePhase : "boundary");
      }
    } catch { /* RSS is optional on hosts without a compatible ps. */ }
  }

  try {
    console.log(`Goblin Pizza Express (${options.guest === "wasm" ? "Rust Wasm" : "TypeScript"} guest): ${options.nodes} local nodes, ${options.concurrency} customer loops, ${options.workers} drones, ${options.http2 ? "HTTP/2 h2c" : "HTTP/1.1"} methods${options.chaos ? ", one leader crash" : ""}.`);
    report.binary = await fingerprintBinary(options.binary, signal);
    report.driver = { kind: "rust", orchestration: "node",
      binary: await fingerprintBinary(options.driverBinary, signal) };
    await cluster.start();
    const bundle = await goblinBundle(options);
    report.bundleHash = bundle.hash;
    report.guest = { kind: options.guest ?? "js", source: options.guest === "wasm" ? "examples/goblin-pizza-rs" : "examples/goblin-pizza-ts/goblin-pizza.ts",
      bytes: Buffer.byteLength(bundle.javascript ?? Buffer.from(bundle.wasm, "base64")) };
    await client.deploy(bundle);
    settings = (await client.call("pizza.setup", {
      tenants: tenantIds, storesPerTenant: options.shops, stockPerShop: options.maxOrders * 4 + 10,
      bakeMs: options.bakeMs, leaseMs: options.leaseMs,
    })).value;
    // Local reads may legitimately precede this acknowledged setup on followers.
    // Establish causal visibility once on every replica before serving previews.
    await Promise.all(cluster.members.map((queryNode) => client.call("pizza.shop", settings.shopIds[0], { query: true, queryNode })));
    for (const shop of settings.shopIds) tips[JSON.stringify(shop)] = 0;
    phases.warmup = phase = newPhase();
    const warmupDeadline = performance.now() + options.warmup * 1_000;
    await Promise.all(Array.from({ length: options.concurrency }, async (_, index) => {
      while (performance.now() < warmupDeadline) {
        await client.call(readMethod, settings.shopIds[index % settings.shopIds.length], { query: true });
      }
    }));
    const offered={offered:0,dispatched:0,driverDropped:0,completed:0,failed:0};
    const schedulingLag=new Histogram();
    rustDriver = await startRustDriver(cluster, options, { signal, onInterval: (message) => {
      // Intervals are disjoint and retain the same histogram buckets as Node.
      // Customer calls always belong to load, including their final retry tail.
      const count = (value) => Number.isSafeInteger(value) && value >= 0;
      if (!Array.isArray(message.orders) || !message.tips || typeof message.tips !== "object" ||
          !count(message.counters?.issuedOrders) || !count(message.counters?.replayChecks) ||
          !count(message.failureCount) || !Array.isArray(message.failures)) throw new Error("Invalid native customer accounting");
      const acknowledged = message.orders.map((order) => {
        if (!Array.isArray(order.shop) || order.shop.length !== 2 || !tenantIds.includes(order.shop[0]) ||
            !settings.shopIds.some((shop) => JSON.stringify(shop) === JSON.stringify(order.shop)) ||
            typeof order.id !== "string" || !/^pizza-\d+$/.test(order.id) || !Number.isInteger(order.quantity) || order.quantity < 1 || order.quantity > 4) throw new Error("Invalid native acknowledged order");
        const key = JSON.stringify([...order.shop, order.id]);
        if (orders.has(key)) throw new Error("Repeated native acknowledged order");
        return [key, order];
      });
      for (const [shop, amount] of Object.entries(message.tips)) {
        if (!Object.hasOwn(tips, shop) || !count(amount) || !count(tips[shop] + amount)) throw new Error("Invalid native acknowledged tips");
      }
      if(message.offered){
        for(const key of Object.keys(offered)){
          if(!count(message.offered[key]))throw new Error("Invalid native arrival accounting");
          offered[key]+=message.offered[key];
        }
        if(offered.offered!==offered.dispatched+offered.driverDropped || message.type==="done"&&offered.dispatched!==offered.completed+offered.failed)throw new Error("Unaccounted native arrivals");
        schedulingLag.merge(message.offered.schedulingLagMs);
        report.offeredLoad={mode:options.offeredRate>0?"open-loop":"closed-loop",ratePerGroup:options.offeredRate??0,...offered,schedulingLagMs:schedulingLag.snapshot()};
      }
      phases.load.stats.merge(message.stats);
      client.aggregate?.merge(message.stats);
      for (const [key, order] of acknowledged) orders.set(key, order);
      for (const [shop, amount] of Object.entries(message.tips)) tips[shop] += amount;
      counters.issuedOrders += message.counters.issuedOrders;
      counters.replayChecks += message.counters.replayChecks;
      failureCount += message.failureCount;
      failures.push(...message.failures.slice(0, Math.max(0, 50 - failures.length)));
      phases.load.ended = Math.max(phases.load.ended, performance.now());
    } });
    report.driver.warmupRequests = rustDriver.warmupRequests;
    if (ready) {
      const startAt = await ready(signal);
      await sleep(Math.max(0, startAt - Date.now()));
    }
    // Shared hosts' counters belong to every group; the coordinator reads them.
    const storageStart = options.attach ? null : await cluster.storageCounters();
    const storageStarted = performance.now();
    phases.load = phase = newPhase();
    loadStarted = phase.started;
    loadDeadline = loadStarted + options.duration * 1_000;
    report.loadStartedAt = new Date(Date.now() - (performance.now() - loadStarted)).toISOString();
    if (options.cpuProfile) {
      cpuProfile = await startCpuProfile({
        pid: cluster.leader.process.child.pid, node: cluster.leader.id,
        outputPath: options.cpuProfile,
        durationSeconds: Math.min(10, Math.floor(options.duration)), signal,
      });
      console.log(`Sampling initial leader for up to ${Math.min(10, Math.floor(options.duration))}s at 1ms; profiling perturbs performance.`);
    }
    client.aggregate = new Stats({ methods });
    eventLoop.enable();
    await sampleResources();
    resourceTimer = setInterval(() => {
      if (samplingResources) return;
      samplingResources = true;
      resourceTask = sampleResources().finally(() => { samplingResources = false; });
    }, 1_000);
    const workers = Array.from({ length: options.workers }, (_, index) => worker(index).catch((error) => {
      if (!workerSignal.aborted) failed(error, `drone ${index}`);
    }));
    tasks.push(...workers);
    const customers = rustDriver.start(Math.ceil(Date.now() + loadDeadline - performance.now()));
    tasks.push(customers);
    const chaos = options.chaos ? (async () => {
      let event;
      // Shared hosts crash once for every group, when the coordinator says.
      if (hostCrash) event = await hostCrash(cluster);
      else {
        await sleep(Math.max(0, loadDeadline - performance.now() - options.duration * 500));
        console.log("A dragon ate the leader. Electing another goblin…");
        event = await cluster.crashLeaderAndRecover();
      }
      event.elapsedMs = Date.parse(event.crashedAt) - Date.parse(report.loadStartedAt);
      console.log(`Quorum serving again after ${format(event.quorumRecoveryMs)} ms; old node restarted.`);
    })().catch((error) => { if (!signal.aborted) failed(error, "leader crash"); }) : Promise.resolve();
    tasks.push(chaos);
    const loadMode = options.offeredRate > 0 ? `${options.offeredRate}/s independently scheduled load` : "closed-loop load";
    console.log(`Rush hour: ${options.duration}s of ${loadMode}; up to ${options.maxOrders} retained orders.`);
    await Promise.all([customers, chaos]);
    phase.ended = Math.max(phase.ended, performance.now());
    if (storageStart) report.storage = storageRates(storageStart, await cluster.storageCounters(), (performance.now() - storageStarted) / 1_000);
    phases.drain = phase = newPhase();
    const drainDeadline = performance.now() + options.drain * 1_000;
    const drainSignal = AbortSignal.any([signal, AbortSignal.timeout(Math.ceil(options.drain * 1_000))]);
    const cancelWorkers = () => workerController.abort(new Error("Drain deadline reached"));
    drainSignal.addEventListener("abort", cancelWorkers, { once: true });
    let drained = false;
    console.log(`Kitchen closing: draining ${orders.size} acknowledged orders and checking the books.`);
    while (performance.now() < drainDeadline && !signal.aborted) {
      try { world = (await client.call("pizza.world", null, { query: true, signal: drainSignal })).value; }
      catch (error) { if (drainSignal.aborted) break; throw error; }
      if (world.orders.length === orders.size && world.orders.every((order) => order.status === "delivered") &&
          world.timers.length === 0 && world.jobs.every((job) => job.state === "completed")) {
        drained = true;
        break;
      }
      if (world.timers.some((timer) => timer.state === "failed")) break;
      try { await delay(500, undefined, { signal: drainSignal }); }
      catch (error) { if (drainSignal.aborted) break; throw error; }
    }
    stopWorkers = true;
    await Promise.all(workers);
    drainSignal.removeEventListener("abort", cancelWorkers);
    deliveryFinished = performance.now();
    phase.ended = Math.max(phase.ended, deliveryFinished);
    clearInterval(resourceTimer);
    await resourceTask;
    await sampleResources();
    client.aggregate = null;
    if (!drained) failed(new Error(`Work did not drain within ${options.drain}s`), "drain");
    phases.audit = phase = newPhase();
    for (const identity of abandoned.values()) {
      try {
        await client.call("pizza.deliver", identity);
        failed(new Error(`Stale drone delivered ${identity.id}`), "fencing");
      } catch (error) {
        if (!lostLease(error)) throw error;
        counters.staleLeaseChecks++;
      }
    }
    world = (await client.call("pizza.world", null, { query: true })).value;
    report.audit = auditWorld(world, { orders: [...orders.values()], tips });
    if (!orders.size) failed(new Error("No order was acknowledged; workload did not exercise the pizza lifecycle"), "coverage");
    const lifecycle = new Histogram();
    const lateness = new Histogram();
    for (const order of world.orders) {
      if (order.readyAt !== null) lateness.record(order.readyAt - order.dueAt);
      if (order.deliveredAt !== null) lifecycle.record(order.deliveredAt - order.createdAt);
    }
    report.lifecycle = {
      orderToDeliveryMs: lifecycle.snapshot(), timerLatenessMs: lateness.snapshot(),
      deliveredOrdersPerSecondIncludingDrain: report.audit.delivered / ((deliveryFinished - loadStarted) / 1_000),
    };
    report.leaderboards = world.leaderboards;
    report.leaderboard = Object.values(world.leaderboards).flat();
    report.replication = await Promise.all(cluster.members.map(async (node) => {
      const metrics = await cluster.metrics(node);
      return { node: node.id, state: metrics.state, leader: metrics.current_leader, lastApplied: metrics.last_applied?.index };
    }));
    try {
      const { stdout } = await execute("du", ["-sk", cluster.directory], { timeout: 5_000 });
      report.clusterDiskMiB = Number(stdout.trim().split(/\s+/)[0]) / 1_024;
    } catch { report.clusterDiskMiB = null; }
  } catch (error) {
    failed(error, signal.aborted ? "interrupted" : "benchmark");
  } finally {
    clearInterval(resourceTimer);
    await resourceTask;
    eventLoop.disable();
    stopWorkers = true;
    if (cpuProfile) {
      report.cpuProfile = signal.aborted
        ? await cpuProfile.stop("Benchmark interrupted")
        : await cpuProfile.done;
      report.cpuProfile.relativePath = relative(dirname(options.html), options.cpuProfile);
      report.cpuProfile.launchedAfterLoadStartMs = Date.parse(report.cpuProfile.startedAt) - Date.parse(report.loadStartedAt);
    } else if (options.cpuProfile) report.cpuProfile = { status: "not_started", outputPath: options.cpuProfile, error: "Benchmark ended before load began", perturbsPerformance: false };
    controller.abort(new Error("Benchmark finished"));
    try { await rustDriver?.close(); } catch (error) { failed(error, "native driver cleanup"); }
    report.queryRouting = mergeRouting(client.routingSummary(), rustDriver?.completed?.queryRouting);
    try { await client.close(); } catch (error) { failed(error, "transport cleanup"); }
    const logs = cluster.logTails();
    try { await cluster.close(); } catch (error) { failed(error, "cleanup"); }
    await Promise.allSettled(tasks);
    if (client.aggregate) await sampleResources();
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", interrupt);
    report.finishedAt = new Date().toISOString();
    report.counters = counters;
    report.phases = Object.fromEntries(Object.entries(phases).map(([name, item]) => [name, item.stats.snapshot(Math.max(0, item.ended - item.started))]));
    if (report.loadStartedAt) report.loadEndedAt = new Date(Date.parse(report.loadStartedAt) + report.phases.load.durationMs).toISOString();
    report.chaos = cluster.events;
    report.resources = {
      serverPeakRssMiB: rssPeaks, driverPeakRssMiB: driverRssPeak / 1_048_576,
      serverCpuSampledLoad: serverCpu.snapshot(),
      nodeDriverCpuSampledLoad: nodeDriverCpu.snapshot()["node-driver"] ?? null,
      ...(rustDriver ? { nativeDriverPeakRssMiB: nativeDriverRssPeak, nativeDriverCpuSampledLoad: nativeDriverCpu.snapshot()["rust-driver"] ?? null } : {}),
      driverCpuMsWholeRun: Object.fromEntries(Object.entries(process.cpuUsage(cpuStart)).map(([key, micros]) => [key, micros / 1_000])),
      driverEventLoopP99Ms: eventLoop.count ? eventLoop.percentile(99) / 1e6 : null,
    };
    report.failureCount = failureCount;
    report.failures = failures;
    report.correctnessPassed = report.audit?.passed === true && failureCount === 0;
    report.passed = report.correctnessPassed;
    report.application = summarizeApplication(report);
    report.profilePassed = options.cpuProfile ? report.cpuProfile?.status === "complete" : null;
    report.passed &&= report.profilePassed !== false;
    report.dataDirectory = options.keepData ? cluster.directories.join(", ") : null;
    if (!report.passed) report.clusterLogs = logs;
    await mkdir(dirname(options.json), { recursive: true });
    await writeFile(options.json, JSON.stringify(report, null, 2) + "\n");
    await mkdir(dirname(options.html), { recursive: true });
    await writeFile(options.html, renderReport(report, { baseline }));
  }
  const load = report.phases.load;
  const application = report.application;
  console.log(`Application: ${format(application.goodputRps)} successful customer requests/s.`);
  console.log(`Successful customer mix: ${format(application.reads.fraction === null ? null : application.reads.fraction * 100)}% reads, ${format(application.mutations.fraction === null ? null : application.mutations.fraction * 100)}% mutations; excludes worker polls, explicit replays, and errors.`);
  if (load) {
    console.log(`HTTP: ${format(load.throughputPerSecond)} successful responses/s; p50/p95/p99 ${format(load.latencyMs.p50)}/${format(load.latencyMs.p95)}/${format(load.latencyMs.p99)} ms; ${load.retries} retries, ${load.failures} failed attempts.`);
    console.log(`Logical calls: ${format(load.operations.throughputPerSecond)}/s; p99 ${format(load.operations.latencyMs.p99)} ms (includes retry and leader discovery time).`);
  }
  if (report.audit) {
    console.log(`${report.passed ? "PASS" : "FAIL"}: ${report.audit.delivered}/${report.audit.orders} orders delivered, ${report.audit.pizzas} pizzas, ${report.audit.revenue} copper; ${counters.replayChecks} receipt replays, ${counters.staleLeaseChecks} stale drones rejected.`);
    console.log(`Oven lateness p95 ${format(report.lifecycle.timerLatenessMs.p95)} ms; order-to-door p95 ${format(report.lifecycle.orderToDeliveryMs.p95)} ms.`);
    for (const issue of report.audit.violations) console.error(`Audit: ${issue}`);
  }
  for (const error of failures.slice(0, 10)) console.error(`${error.context}: ${error.message}`);
  console.log(`Report: ${options.json}`);
  console.log(`HTML: ${options.html}`);
  if (report.cpuProfile) console.log(`CPU profile: ${report.cpuProfile.status} — ${options.cpuProfile}${report.cpuProfile.error || report.cpuProfile.summaryError ? ` (${report.cpuProfile.error ?? report.cpuProfile.summaryError})` : ""}`);
  if (report.dataDirectory) console.log(`Retained data: ${report.dataDirectory}`);
  return report;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const options = parseOptions(process.argv.slice(2));
    if (options.help) console.log(HELP);
    else {
      const report = options.groups > 1 || options.hosted
        ? await (await import("./multi-group.mjs")).runGroups(options)
        : await run(options);
      if (!report.passed) process.exitCode = 1;
    }
  } catch (error) {
    console.error(error.stack ?? error.message);
    process.exitCode = 1;
  }
}
