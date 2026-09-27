import { randomUUID } from "node:crypto";
import { execFile, fork } from "node:child_process";
import { mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import { basename, dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import { HostCluster } from "./cluster.mjs";
import { Histogram, summarizeApplication } from "./metrics.mjs";
import { cpuMilliseconds, ServerCpuSamples, storageRates } from "./resources.mjs";
import { renderGroupsReport } from "./multi-group-report.mjs";

/** Sum completed work over the union of simultaneous load intervals, never sum
 * per-group rates or average percentiles. Includes final in-flight request tails. */
export function summarizeGroups(reports, options) {
  const violations = [];
  if (reports.length !== options.groups) violations.push("Missing group results");
  const starts = reports.map((r) => Date.parse(r.loadStartedAt));
  const ends = reports.map((r) => Date.parse(r.loadEndedAt));
  const validWindows = reports.length > 0 && starts.every(Number.isFinite) && ends.every((end, i) => Number.isFinite(end) && end > starts[i]);
  if (!validWindows) violations.push("Missing or invalid load measurement interval");
  const start = validWindows ? Math.min(...starts) : null;
  const end = validWindows ? Math.max(...ends) : null;
  const durationMs = validWindows ? end - start : null;
  const synchronizedOverlapMs = validWindows ? Math.max(0, Math.min(...ends) - Math.max(...starts)) : 0;
  if (!synchronizedOverlapMs) violations.push("Group load intervals did not overlap");
  const validHash = (value) => typeof value === "string" && value.trim().length > 0;
  if (reports.some((r) => !validHash(r.bundleHash))) violations.push("Missing group bundle identity");
  if (reports.some((r) => !validHash(r.binary?.sha256))) violations.push("Missing group binary identity");
  if (new Set(reports.map((r) => r.bundleHash)).size > 1) violations.push("Groups disagree on bundleHash");
  if (new Set(reports.map((r) => JSON.stringify(r.runtime))).size > 1) violations.push("Groups used different runtime settings");
  if (new Set(reports.map((r) => r.binary?.sha256)).size > 1) violations.push("Groups used different binaries");
  if (new Set(reports.map((r) => r.driver?.kind ?? r.options?.driver ?? "node")).size > 1) violations.push("Groups used different customer drivers");
  if (reports.some((r) => r.driver?.kind === "rust" && !validHash(r.driver.binary?.sha256))) violations.push("Missing Rust driver binary identity");
  if (new Set(reports.map((r) => r.driver?.binary?.sha256)).size > 1) violations.push("Groups used different driver binaries");
  const tenants = new Set();
  const offeredTotals={offered:0,dispatched:0,driverDropped:0,completed:0,failed:0};
  const schedulingLag=new Histogram();
  const latency = { all: new Histogram(), read: new Histogram(), mutation: new Histogram() };
  const totals = { completed: 0, failed: 0, reads: 0, mutations: 0, orders: 0, pizzas: 0, revenue: 0, tips: 0, replays: 0, staleLeases: 0 };
  const groups = reports.map((r, index) => {
    const app = summarizeApplication(r);
    if(r.offeredLoad){
      for(const key of Object.keys(offeredTotals))offeredTotals[key]+=r.offeredLoad[key]??0;
      schedulingLag.merge(r.offeredLoad.schedulingLagMs);
    } else if(options.offeredRate>0)violations.push(`Group ${index} has no offered arrival accounting`);
    if (!app.available) violations.push(`Group ${index} has no application load measurements`);
    const tenantIds = Array.isArray(r.options?.tenantIds) ? r.options.tenantIds : [];
    if (!tenantIds.length || tenantIds.some((tenant) => typeof tenant !== "string" || !tenant.trim())) {
      violations.push(`Group ${index} has missing or invalid tenant assignments`);
    }
    if (options.tenants !== undefined && tenantIds.length !== options.tenants) {
      violations.push(`Group ${index} tenant count does not match its configuration`);
    }
    for (const tenant of tenantIds) {
      if (tenants.has(tenant)) violations.push(`Tenant ${tenant} was assigned to multiple groups`);
      tenants.add(tenant);
    }
    if (r.options?.readConsistency !== options.readConsistency) violations.push(`Group ${index} read policy mismatch`);
    totals.completed += app.completed ?? 0;
    totals.failed += app.failed ?? 0;
    totals.reads += app.reads.completed;
    totals.mutations += app.mutations.completed;
    for (const field of ["orders", "pizzas", "revenue", "tips"]) totals[field] += r.audit?.[field] ?? 0;
    totals.replays += r.counters?.replayChecks ?? 0;
    totals.staleLeases += r.counters?.staleLeaseChecks ?? 0;
    const customerLatency = new Histogram();
    const customerLatencyByKind = { read: new Histogram(), mutation: new Histogram() };
    for (const [name, kind] of Object.entries(app.methods)) {
      const method = r.phases?.load?.operations?.perMethod?.[name];
      if (!method) continue;
      if (![method.count, method.completed, method.failed].every((n) => Number.isSafeInteger(n) && n >= 0)
        || method.count !== method.completed + method.failed) {
        violations.push(`Group ${index} has invalid ${name} operation counts`);
      }
      const histogram = method.latencyMs;
      try {
        // Save a failed diagnostic report even when one group's measurements
        // cannot be merged, instead of throwing before HTML/JSON is written.
        new Histogram().merge(histogram);
        if (histogram.samples + histogram.invalidSamples !== method.count) {
          throw new TypeError("latency sample count differs from operation count");
        }
        latency.all.merge(histogram); latency[kind].merge(histogram); customerLatency.merge(histogram);
        customerLatencyByKind[kind].merge(histogram);
      } catch (error) { violations.push(`Group ${index} has invalid ${name} latency measurements: ${error.message}`); }
    }
    if (!r.correctnessPassed) violations.push(`Group ${index} failed its independent audit or workload`);
    return {
      name: `group-${index}`, tenants: tenantIds, options: r.options,
      application: app, customerLatencyMs: customerLatency.snapshot(), latencyMs: r.phases?.load?.operations?.latencyMs,
      customerReadLatencyMs: customerLatencyByKind.read.snapshot(),
      customerMutationLatencyMs: customerLatencyByKind.mutation.snapshot(),
      offeredLoad:r.offeredLoad, audit: r.audit, failures: r.failures, counters: r.counters, chaos: r.chaos,
      resources: r.resources, driver: r.driver, clusterDiskMiB: r.clusterDiskMiB, queryRouting: r.queryRouting,
      loadStartedAt: r.loadStartedAt, loadEndedAt: r.loadEndedAt,
      timeline: r.timeline, lifecycle: r.lifecycle,
    };
  });
  const goodputRps = durationMs && groups.every((group) => group.application.available)
    ? totals.completed / (durationMs / 1_000) : null;
  const correctnessPassed = violations.length === 0;
  return {
    schemaVersion: 1, kind: "multi-group", options,
    definition: "Successful primary customer logical calls across all groups divided by the union of their synchronized load intervals, including in-flight tails. Excludes retries, explicit replays, workers, errors, warmup and drain. Percentiles merge bucket counts; they are not averages of group percentiles.",
    partitioning: "Static tenant-to-group assignment. Each tenant's stores, queues and rankings stay within one independent Raft group. No cross-group transaction or global atomic snapshot in this workload.",
    environment: reports[0]?.environment ? {
      ...reports[0].environment,
      colocatedNodes: reports.reduce((total, report) => total + (report.environment?.colocatedNodes ?? report.options?.nodes ?? 0), 0),
      replicasPerGroup: options.nodes,
      colocatedDrivers: reports.length + reports.filter((report) => report.driver?.kind === "rust").length,
      colocatedControllers: reports.length,
      colocatedNativeDrivers: reports.filter((report) => report.driver?.kind === "rust").length,
    } : undefined,
    runtime: reports[0]?.runtime, binary: reports[0]?.binary, driver: reports[0]?.driver, bundleHash: reports[0]?.bundleHash,
    // Per-process storage commits summed over groups; shared hosts report theirs under `hosts`.
    storage: reports.every((r) => r.storage) ? {
      complete: reports.every((r) => r.storage.complete),
      ...Object.fromEntries(["batchesPerSecond", "durableBatchesPerSecond", "stagedWritesPerSecond"].map((key) =>
        [key, reports.reduce((total, r) => total + r.storage[key], 0)])),
    } : undefined,
    durationMs, synchronizedOverlapMs, startSkewMs: validWindows ? Math.max(...starts) - Math.min(...starts) : null,
    loadStartedAt: start === null ? null : new Date(start).toISOString(), loadEndedAt: end === null ? null : new Date(end).toISOString(),
    offeredLoad:{mode:options.offeredRate>0?"open-loop":"closed-loop",ratePerGroup:options.offeredRate??0,groups:options.groups,...offeredTotals,schedulingLagMs:schedulingLag.snapshot()},
    goodputRps, totals, latencyMs: Object.fromEntries(Object.entries(latency).map(([kind, histogram]) => [kind, histogram.snapshot()])),
    correctnessPassed, passed: correctnessPassed, violations, groups,
  };
}

/** Track only workers we forked and server PIDs those workers report. Process
 * groups are an optimization: restricted hosts can deny them while permitting
 * individual owned-process signals. A failure never skips another target. */
export function createProcessCleanup({ kill = process.kill.bind(process), platform = process.platform,
  graceMs = 10_000, pollMs = 25 } = {}) {
  const workers = [];
  const errors = [];
  let stopping = false;
  let escalated = false;
  let timer;
  let stoppedAt;
  const alive = (child) => child.exitCode === null && child.signalCode === null;
  const direct = (record, pid, signal) => {
    if (record.sent.get(pid) === signal) return;
    record.sent.set(pid, signal);
    try { kill(pid, signal); }
    catch (error) {
      if (error.code === "ESRCH") record.servers.delete(pid);
      else errors.push({ pid, signal, error: error.message, code: error.code });
    }
  };
  const terminate = (record, signal) => {
    const childAlive = alive(record.child);
    if (childAlive && platform !== "win32") {
      try {
        kill(-record.child.pid, signal);
        record.sent.set(record.child.pid, signal);
        for (const pid of record.servers) record.sent.set(pid, signal);
        return;
      } catch { /* A process-group denial must fall back to every owned PID. */ }
    }
    for (const pid of record.servers) direct(record, pid, signal);
    if (childAlive && Number.isSafeInteger(record.child.pid)) direct(record, record.child.pid, signal);
  };
  const stop = () => {
    if (stopping) return;
    stopping = true;
    stoppedAt = performance.now();
    // Arm escalation before the first signal, including on partial failures.
    timer = setTimeout(() => {
      escalated = true;
      for (const record of workers) terminate(record, "SIGKILL");
    }, graceMs);
    timer.unref();
    for (const record of workers) terminate(record, "SIGTERM");
  };
  const track = (child) => {
    const record = { child, servers: new Set(), sent: new Map() };
    workers.push(record);
    if (stopping) terminate(record, escalated ? "SIGKILL" : "SIGTERM");
    return (event) => {
      const pid = event?.pid;
      if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid || pid === child.pid) return;
      if (event.type === "spawn") {
        record.servers.add(pid);
        if (stopping) direct(record, pid, escalated ? "SIGKILL" : "SIGTERM");
      } else if (event.type === "exit") {
        record.servers.delete(pid);
        record.sent.delete(pid);
      }
    };
  };
  const remaining = () => {
    let count = 0;
    for (const record of workers) {
      count += Number(alive(record.child));
      for (const pid of record.servers) {
        try { kill(pid, 0); count++; }
        catch (error) {
          if (error.code === "ESRCH") record.servers.delete(pid);
          else count++; // Inability to inspect is not proof of process exit.
        }
      }
    }
    return count;
  };
  const finish = async () => {
    stop();
    // Do not cancel escalation merely because Node exited: reported Rust
    // children may still be alive. Bound cleanup even if the OS denies KILL.
    while (remaining() && performance.now() - stoppedAt < graceMs + 1_000) {
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
    clearTimeout(timer);
    const count = remaining();
    if (count) errors.push({ error: `${count} owned processes did not exit after cleanup escalation` });
    return errors;
  };
  return { track, stop, finish, errors };
}

/** Sample shared hosts' CPU once a second while `inLoad()`. */
function sampleHosts(hosts, inLoad) {
  const samples = new ServerCpuSamples();
  let stopped = false;
  const task = (async () => {
    while (!stopped) {
      const pids = hosts.pids;
      if (pids.length) {
        try {
          const sampledAt = performance.now();
          const phase = inLoad() ? "load" : "outside";
          const { stdout } = await promisify(execFile)("ps", ["-o", "pid=,time=", "-p", pids.map(({ pid }) => pid).join(",")], { timeout: 2_000 });
          for (const line of stdout.trim().split("\n")) {
            const [pid, time] = line.trim().split(/\s+/);
            const host = pids.find((entry) => entry.pid === Number(pid));
            const cpuMs = cpuMilliseconds(time ?? "");
            if (host && cpuMs !== null) samples.record(host.id, host.pid, cpuMs, sampledAt, phase);
          }
        } catch { /* A host exiting between listing and sampling. */ }
      }
      await delay(1_000);
    }
  })();
  return { samples, stop: async () => { stopped = true; await task; } };
}

export async function runGroups(options) {
  const directory = resolve(dirname(options.json), basename(options.json).replace(/\.json$/i, "") + "-groups");
  await mkdir(directory, { recursive: true });
  const children = [];
  let hosts = null;
  let hostSampler = null;
  let hostChaos = Promise.resolve();
  const runId = randomUUID();
  let stopping = false;
  const cleanup = createProcessCleanup();
  let rejectStopped;
  const interrupted = new Promise((_, reject) => { rejectStopped = reject; });
  interrupted.catch(() => {});
  const stop = () => {
    if (stopping) return;
    stopping = true;
    cleanup.stop();
    rejectStopped(new Error("Multi-group benchmark interrupted"));
  };
  const save = async (report) => {
    report.groups.forEach((group, i) => {
      group.html = relative(dirname(options.html), children[i].options.html);
      group.json = relative(dirname(options.json), children[i].options.json);
    });
    await mkdir(dirname(options.json), { recursive: true });
    await mkdir(dirname(options.html), { recursive: true });
    await writeFile(options.json, JSON.stringify(report, null, 2) + "\n");
    await writeFile(options.html, renderGroupsReport(report));
    // A later run can use fewer groups. Prune only our numbered outputs after
    // the replacement aggregate is saved; preserve other files and directories.
    const retained = new Set(children.flatMap(({ options }) => [options.json, options.html]));
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = resolve(directory, entry.name);
      if ((entry.isFile() || entry.isSymbolicLink()) && /^group-\d+\.(?:html|json)$/.test(entry.name)
        && !retained.has(path)) await unlink(path);
    }
    return report;
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    if (options.hosted) {
      hosts = new HostCluster(options);
      await hosts.start();
      console.log(`${options.nodes} host processes serve ${options.groups} groups over one shared database each.`);
    }
    for (let group = 0; group < options.groups; group++) {
      const childOptions = { ...options, groups: 1, runId, ...(hosts ? { attach: hosts.attachment(group) } : {}),
        ...(options.otelCapture ? { otelCapture: { ...options.otelCapture, group } } : {}),
        ...(options.mixedProfile ? { mixedProfile: { ...options.mixedProfile, group } } : {}),
        seed: `${options.seed}:group-${group}`, tenantIds: Array.from({ length: options.tenants }, (_, i) => `tenant-${group * options.tenants + i}`),
        json: resolve(directory, `group-${group}.json`), html: resolve(directory, `group-${group}.html`),
      };
      const child = fork(fileURLToPath(new URL("./multi-group-worker.mjs", import.meta.url)), [JSON.stringify(childOptions)], { detached: process.platform !== "win32", stdio: ["ignore", "inherit", "inherit", "ipc"] });
      let readyResolve, readyReject, recoveredResolve;
      const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
      const recovered = new Promise((resolve) => { recoveredResolve = resolve; });
      const trackServer = cleanup.track(child);
      child.on("message", (message) => {
        if (message?.type === "ready") readyResolve();
        else if (message?.type === "server-process") trackServer(message.event);
        else if (message?.type === "host-recovered") recoveredResolve();
      });
      const done = new Promise((resolve, reject) => {
        child.once("error", (error) => { readyReject(error); reject(error); });
        child.once("exit", (code, signal) => {
          readyReject(new Error(`Group ${group} exited before the start barrier (${code ?? signal})`));
          resolve({ code, signal });
        });
      });
      // Install rejection handlers immediately while later children start.
      ready.catch(() => {}); done.catch(() => {});
      children.push({ child, ready, done, recovered, options: childOptions });
    }
    await Promise.race([Promise.all(children.map((child) => child.ready)), interrupted]);
    if (stopping) throw new Error("Benchmark interrupted before synchronized start");
    const startAt = Date.now() + 250;
    console.log(`Starting ${options.groups} independent ${options.nodes}-node Raft groups together; ${options.groups * options.concurrency} parallel customer loops, ${options.readConsistency} reads.`);
    for (const { child } of children) child.send({ type: "start", startAt });
    let hostStorage = Promise.resolve(null);
    if (hosts) {
      hostStorage = (async () => {
        await delay(Math.max(0, startAt - Date.now()));
        const start = await hosts.storageCounters(), started = performance.now();
        await delay(options.duration * 1_000);
        return storageRates(start, await hosts.storageCounters(), (performance.now() - started) / 1_000);
      })().catch(() => null);
      hostSampler = sampleHosts(hosts, () => Date.now() >= startAt && Date.now() <= startAt + options.duration * 1_000);
      if (options.chaos) hostChaos = (async () => {
        await delay(Math.max(0, startAt + options.duration * 500 - Date.now()));
        const [host, leaders] = await hosts.busiestHost();
        const crashedAt = new Date().toISOString();
        await hosts.crash(host);
        console.log(`A dragon ate host ${host}, which led ${leaders} of ${options.groups} groups…`);
        for (const { child } of children) if (child.connected) child.send({ type: "host-crash", host, crashedAt });
        // Restart once every group serves again, as a restarted machine would.
        await Promise.all(children.map(({ recovered, done }) => Promise.race([recovered, done])));
        await delay(Math.max(0, 250 - (Date.now() - Date.parse(crashedAt))));
        await hosts.restart(host);
        for (const { child } of children) if (child.connected) child.send({ type: "host-restarted", host });
      })();
    }
    const exits = await Promise.race([Promise.all(children.map((child) => child.done)), interrupted]);
    await hostChaos;
    const reports = await Promise.all(children.map(({ options }) => readFile(options.json, "utf8").then(JSON.parse)));
    if (reports.some((report) => report.runId !== runId)) throw new Error("A group did not write a report for this run");
    const report = summarizeGroups(reports, options);
    report.runId = runId;
    if (hosts) {
      await hostSampler.stop();
      report.hosts = { count: hosts.hosts.length, replicasPerHost: options.groups, serverCpuSampledLoad: hostSampler.samples.snapshot(),
        storage: await hostStorage };
    }
    exits.forEach(({ code, signal }, i) => {
      if (code !== 0) { report.violations.push(`Group ${i} exited with ${code ?? signal}`); report.passed = report.correctnessPassed = false; }
    });
    await save(report);
    console.log(`${report.passed ? "PASS" : "FAIL"}: ${report.goodputRps?.toFixed(1)} global customer requests/s across ${options.groups} Raft groups; ${report.latencyMs.all.p99?.toFixed(1)} ms customer p99; ${report.totals.orders} audited orders.\nHTML: ${options.html}`);
    return report;
  } catch (error) {
    stop();
    await cleanup.finish();
    const reports = await Promise.all(children.map(async ({ options: childOptions }) => {
      try {
        const report = JSON.parse(await readFile(childOptions.json, "utf8"));
        if (report.runId === runId) return report;
      } catch { /* A worker may have died before writing its report. */ }
      return { options: childOptions, correctnessPassed: false, failures: [{ context: "coordinator", message: "No report from this run" }] };
    }));
    const report = summarizeGroups(reports, options);
    report.runId = runId;
    report.violations.unshift(String(error.message ?? error));
    report.passed = report.correctnessPassed = false;
    await save(report);
    console.error(`FAIL: ${error.message}\nHTML: ${options.html}`);
    return report;
  } finally {
    stop();
    await hostSampler?.stop();
    await hostChaos.catch((error) => console.error("Host crash:", error));
    try { await hosts?.close(); } catch (error) { console.error("Benchmark host cleanup:", error); }
    const errors = await cleanup.finish();
    for (const error of errors) console.error("Benchmark process cleanup:", error);
    // Failed permission checks must not leave this coordinator waiting forever
    // on an IPC handle; disconnect also activates the worker's own cleanup.
    for (const { child } of children) {
      if (child.connected) child.disconnect();
      child.unref();
    }
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
  }
}
