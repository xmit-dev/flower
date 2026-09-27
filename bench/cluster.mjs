import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { access, mkdtemp, rm } from "node:fs/promises";
import { constants } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function positiveInteger(value, name) {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

async function reservePort() {
  const server = createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return { server, port: server.address().port };
}

async function releasePort(reservation) {
  if (!reservation.server.listening) return;
  await new Promise((resolve, reject) => reservation.server.close((error) => error ? reject(error) : resolve()));
}

async function waitForExit(runtime, timeoutMs) {
  let timer;
  try {
    return await Promise.race([
      runtime.exited.then(() => true),
      new Promise((resolve) => { timer = setTimeout(() => resolve(false), timeoutMs); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function discoveryTimeout(timeoutMs) {
  const error = new Error(`elect a leader with a serving quorum timed out after ${timeoutMs} ms`);
  error.code = "DISCOVERY_TIMEOUT";
  return error;
}

async function waitForDiscovery(promise, timeoutMs) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => { timer = setTimeout(() => reject(discoveryTimeout(timeoutMs)), timeoutMs); }),
    ]);
  } finally { clearTimeout(timer); }
}

/**
 * A fresh directory under each root, by default the system temporary
 * directory. Servers alternate between them, e.g. to put replicas on
 * separate disks.
 */
async function dataDirectories(roots, prefix) {
  const created = await Promise.allSettled((roots?.length ? roots : [tmpdir()]).map((parent) => mkdtemp(join(parent, prefix))));
  const failure = created.find(({ status }) => status === "rejected");
  if (!failure) return created.map(({ value }) => value);
  await Promise.all(created.filter(({ status }) => status === "fulfilled").map(({ value }) => rm(value, { recursive: true, force: true })));
  throw failure.reason;
}

/** An isolated, disposable cluster. Never connects to or deletes an existing database. */
export class LocalCluster {
  constructor(options = {}) {
    this.nodeCount = options.nodes ?? 3;
    if (![1, 3].includes(this.nodeCount)) throw new Error("nodes must be 1 or 3");
    this.binary = resolve(options.binary ?? join(root, "target/release/flower"));
    this.startupTimeoutMs = positiveInteger(options.startupTimeoutMs ?? 30_000, "startupTimeoutMs");
    this.requestTimeoutMs = positiveInteger(options.requestTimeoutMs ?? 2_000, "requestTimeoutMs");
    this.keepData = options.keepData ?? false;
    this.dataRoots = options.data ?? null;
    this.onProcess = options.onProcess;
    // Replicas served by shared host processes (HostCluster): never spawned,
    // restarted or deleted here.
    this.attached = options.attach ?? null;
    this.adminToken = this.attached?.adminToken ?? randomUUID();
    this.members = [];
    this.leader = null;
    this.directory = null;
    this.directories = [];
    this.events = [];
    this._generations = [];
    this._reservations = [];
    this._controller = new AbortController();
    this._discovering = null;
    this._recovering = null;
    this._recoveryObservation = null;
    this._closing = null;
    this._starting = null;
    this._started = false;
    this._probeName = `__flower_benchmark_probe_${randomUUID()}`;
  }

  get url() {
    if (!this.leader) throw new Error("Cluster has no known serving leader");
    return this.leader.url;
  }

  get pids() {
    // Shared hosts' CPU belongs to every group; the coordinator samples it.
    if (this.attached) return [];
    return this.members.filter((node) => node.process && !node.process.ended)
      .map((node) => ({ id: node.id, pid: node.process.child.pid }));
  }

  _assertOpen() {
    this._controller.signal.throwIfAborted();
  }

  _assertHealthy() {
    this._assertOpen();
    for (const node of this.members) {
      const runtime = node.process;
      if (runtime?.ended && !runtime.intentional) {
        throw new Error(`Flower node ${node.id} exited unexpectedly: ${runtime.error?.message ?? JSON.stringify(runtime.exit)}`);
      }
    }
  }

  _eligible(node, runtime = node?.process) {
    return Boolean(runtime && node.process === runtime && !runtime.ended && !runtime.intentional);
  }

  _startNode(node) {
    this._assertOpen();
    const child = spawn(this.binary, ["--id", String(node.id), "--listen", node.address, "--data", node.directory], {
      cwd: root,
      env: {
        ...process.env, FLOWER_ADMIN_TOKEN: this.adminToken,
        RUST_LOG: process.env.FLOWER_BENCH_LOG ?? "flower=info,openraft=warn",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const runtime = { child, id: node.id, logs: "", error: null, ended: false, intentional: false };
    const collect = (chunk) => { runtime.logs = (runtime.logs + chunk.toString()).slice(-64_000); };
    child.stdout.on("data", collect);
    child.stderr.on("data", collect);
    child.on("error", (error) => { runtime.error = error; runtime.ended = true; });
    runtime.exited = new Promise((resolve) => child.once("close", (code, signal) => {
      runtime.ended = true;
      runtime.exit = { code, signal };
      try { if (Number.isSafeInteger(child.pid)) this.onProcess?.({ type: "exit", pid: child.pid }); }
      catch (error) { this._controller.abort(error); }
      resolve();
    }));
    node.process = runtime;
    this._generations.push(runtime);
    // Register ownership before invoking an observer so reporting failures can
    // never strand a process outside the cluster's normal cleanup path.
    try { if (Number.isSafeInteger(child.pid)) this.onProcess?.({ type: "spawn", pid: child.pid }); }
    catch (error) { this._controller.abort(error); }
  }

  async _fetch(node, path, { method = "GET", body, timeoutMs = this.requestTimeoutMs, signal } = {}) {
    this._assertOpen();
    if (!node.process || node.process.ended) throw new Error(`Node ${node.id} is not running`);
    const headers = {};
    if (path.startsWith("/raft/") || path.startsWith("/admin/")) headers.authorization = `Bearer ${this.adminToken}`;
    if (body !== undefined) headers["content-type"] = "application/json";
    const response = await fetch(node.url + path, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.any([this._controller.signal, AbortSignal.timeout(Math.max(1, timeoutMs)), ...(signal ? [signal] : [])]),
    });
    // Consume the body under the same abort deadline as the response headers.
    const text = await response.text();
    let value;
    try { value = JSON.parse(text); } catch { value = text; }
    return { status: response.status, ok: response.ok, value };
  }

  async metrics(node = this.leader) {
    if (!node) throw new Error("No node selected for metrics");
    const response = await this._fetch(node, "/raft/metrics");
    if (!response.ok) throw new Error(`Node ${node.id} metrics returned HTTP ${response.status}`);
    return response.value;
  }

  async _until(label, operation, timeoutMs = this.startupTimeoutMs, { pollMs = 75 } = {}) {
    const deadline = performance.now() + timeoutMs;
    let lastError;
    while (performance.now() < deadline) {
      this._assertHealthy();
      try {
        const result = await operation(Math.max(1, Math.min(this.requestTimeoutMs, Math.ceil(deadline - performance.now()))));
        if (result) return result;
      } catch (error) { lastError = error; }
      this._assertHealthy();
      await delay(Math.min(pollMs, Math.max(1, deadline - performance.now())), undefined, { signal: this._controller.signal });
    }
    const error = new Error(`${label} timed out after ${timeoutMs} ms${lastError ? `: ${lastError.message}` : ""}`, { cause: lastError });
    error.code = "CLUSTER_WAIT_TIMEOUT";
    throw error;
  }

  async start() {
    if (this._started) throw new Error("Cluster can only be started once");
    this._started = true;
    this._assertOpen();
    if (this.attached) {
      for (const { id, address } of this.attached.members) {
        this.members.push({ id, address, url: `http://${address}`, process: { attached: true, ended: false, intentional: false } });
      }
      await this.discoverLeader();
      return this;
    }
    this._starting = (async () => {
      await access(this.binary, constants.X_OK);
      this.directories = await dataDirectories(this.dataRoots, "flower-bench-");
      [this.directory] = this.directories;
      for (let index = 0; index < this.nodeCount; index++) {
        const reservation = await reservePort();
        this._reservations.push(reservation);
        const id = index + 1;
        const address = `127.0.0.1:${reservation.port}`;
        this.members.push({ id, address, url: `http://${address}`, directory: join(this.directories[index % this.directories.length], `node-${id}`) });
      }
      for (const [index, node] of this.members.entries()) {
        await releasePort(this._reservations[index]);
        this._startNode(node);
      }
      await this._until("start all Flower nodes", async (timeoutMs) => {
        const results = await Promise.all(this.members.map(async (node) => {
          try { return (await this._fetch(node, "/raft/metrics", { timeoutMs })).ok; } catch { return false; }
        }));
        return results.every(Boolean);
      });
      const response = await this._fetch(this.members[0], "/raft/initialize", {
        method: "POST", body: Object.fromEntries(this.members.map((node) => [node.id, node.address])),
        timeoutMs: this.startupTimeoutMs,
      });
      if (!response.ok) throw new Error(`Cluster initialization returned HTTP ${response.status}: ${JSON.stringify(response.value)}`);
      await this.discoverLeader();
      return this;
    })();
    try { return await this._starting; }
    catch (error) {
      error.clusterLogs = this.logTails();
      await this.close();
      throw error;
    }
  }

  _observeRecovery(field, node) {
    const observation = this._recoveryObservation;
    if (!observation || node.id === observation.event.oldLeader) return;
    observation.event[field] ??= performance.now() - observation.started;
  }

  async _probeLeader(timeoutMs) {
    const round = new AbortController();
    const deadline = performance.now() + timeoutMs;
    try {
      // A slow follower must not hold up a leader that has already established
      // a quorum. Reject nonleaders and take the first successful quorum probe.
      return await Promise.any(this.members.filter((node) => this._eligible(node)).map(async (node) => {
        const runtime = node.process;
        const response = await this._fetch(node, "/raft/metrics", { timeoutMs, signal: round.signal });
        const metrics = response.value;
        if (!this._eligible(node, runtime) || !response.ok) throw new Error("Stale metrics response");
        if (metrics.state === "Candidate") this._observeRecovery("candidateObservedMs", node);
        if (metrics.state !== "Leader" || Number(metrics.current_leader) !== node.id) throw new Error("Not leader");
        this._observeRecovery("leaderObservedMs", node);
        const probe = await this._fetch(node, "/v1/query", {
          method: "POST", body: { name: this._probeName, args: null },
          timeoutMs: Math.max(1, Math.ceil(deadline - performance.now())), signal: round.signal,
        });
        // Check process generation again: responses can complete after SIGKILL,
        // before the process close event, or after that node has restarted.
        if (!this._eligible(node, runtime) || probe.status !== 404
          || probe.value?.error?.code !== "METHOD_NOT_FOUND") throw new Error("No serving quorum");
        this._observeRecovery("quorumProbeMs", node);
        return { node, runtime };
      }));
    } catch { return null; }
    finally { round.abort(new Error("Leader discovery round finished")); }
  }

  /** Concurrent callers share one election/linearizability probe loop. */
  async discoverLeader({ timeoutMs = this.startupTimeoutMs } = {}) {
    positiveInteger(timeoutMs, "timeoutMs");
    const deadline = performance.now() + timeoutMs;
    try {
      while (performance.now() < deadline) {
        this._assertHealthy();
        if (!this._discovering) {
          const discovering = this._until("elect a leader with a serving quorum",
            (requestTimeoutMs) => this._probeLeader(requestTimeoutMs),
            Math.max(1, Math.ceil(deadline - performance.now())), { pollMs: 25 });
          // Clear once on settlement, before all waiting callers resume. They
          // recheck this slot, so only one can launch the next polling loop.
          const shared = discovering.finally(() => {
            if (this._discovering === shared) this._discovering = null;
          });
          this._discovering = shared;
        }
        const pending = this._discovering;
        const remaining = Math.max(1, Math.ceil(deadline - performance.now()));
        let found;
        try { found = await waitForDiscovery(pending, remaining); }
        catch (error) {
          // A short-lived RPC retry may have created this shared polling loop.
          // Its budget must not shorten a recovery caller's longer deadline.
          if (error.code === "CLUSTER_WAIT_TIMEOUT" && performance.now() < deadline) continue;
          throw error;
        }
        this._assertHealthy();
        if (this._eligible(found.node, found.runtime)) {
          this.leader = found.node;
          return found.node;
        }
      }
      throw discoveryTimeout(timeoutMs);
    } catch (error) { error.clusterLogs = this.logTails(); throw error; }
  }

  /** Kill a leader, restore quorum service, then restart its durable data directory. */
  async crashLeaderAndRecover({ restartAfterMs = 250, timeoutMs = this.startupTimeoutMs } = {}) {
    if (this.nodeCount < 3) throw new Error("Leader failover requires a three-node cluster");
    if (!Number.isSafeInteger(restartAfterMs) || restartAfterMs < 0) throw new Error("restartAfterMs must be a non-negative integer");
    positiveInteger(timeoutMs, "timeoutMs");
    if (this._recovering) throw new Error("A leader crash is already in progress");
    const recovery = this._crashAndRecover(restartAfterMs, timeoutMs);
    this._recovering = recovery;
    try { return await recovery; }
    catch (error) { error.clusterLogs = this.logTails(); throw error; }
    finally {
      if (this._recovering === recovery) this._recovering = null;
      this._recoveryObservation = null;
    }
  }

  async _crashAndRecover(restartAfterMs, timeoutMs) {
    const previous = await this.discoverLeader({ timeoutMs });
    this._assertHealthy();
    const started = performance.now();
    const event = { oldLeader: previous.id, crashedAt: new Date().toISOString() };
    this.events.push(event);
    this._recoveryObservation = { event, started };
    previous.process.intentional = true;
    previous.process.child.kill("SIGKILL");
    this.leader = null;
    if (!await waitForExit(previous.process, 5_000)) throw new Error(`Killed node ${previous.id} did not exit`);
    // The stopped process is immediately ineligible, so RPC retries and this
    // recovery path can share exactly the same discovery loop.
    const next = await this.discoverLeader({ timeoutMs });
    event.newLeader = next.id;
    event.quorumRecoveryMs = performance.now() - started;
    const restartDelay = Math.max(0, restartAfterMs - (performance.now() - started));
    if (restartDelay) await delay(restartDelay, undefined, { signal: this._controller.signal });
    const target = (await this.metrics(next)).last_applied?.index ?? 0;
    this._startNode(previous);
    await this._until(`restarted node ${previous.id} catches up`, async (requestTimeoutMs) => {
      const response = await this._fetch(previous, "/raft/metrics", { timeoutMs: requestTimeoutMs });
      return response.ok && response.value.last_applied?.index >= target
        && Number(response.value.current_leader) === next.id;
    }, timeoutMs);
    event.restartCatchUpMs = performance.now() - started;
    return event;
  }

  /** Each running node's batched storage commits, from /admin/resources. */
  async storageCounters() {
    const counters = {};
    await Promise.all(this.members.filter((node) => this._eligible(node)).map(async (node) => {
      try {
        const response = await this._fetch(node, "/admin/resources");
        if (response.ok && response.value?.storage) counters[node.id] = { pid: node.process.child?.pid ?? null, ...response.value.storage };
      } catch { /* Unmeasured, like a restarted node. */ }
    }));
    return counters;
  }

  /** A shared host stopped: this group loses a replica, perhaps its leader. */
  async observeHostCrash(hostId, crashedAt, { timeoutMs = this.startupTimeoutMs } = {}) {
    const node = this.members.find((member) => member.id === hostId);
    const started = performance.now() - (Date.now() - Date.parse(crashedAt));
    const event = { oldLeader: this.leader?.id ?? null, crashedHost: hostId, crashedAt };
    this.events.push(event);
    node.process = { attached: true, ended: true, intentional: true };
    if (this.leader?.id === hostId) this.leader = null;
    this._recoveryObservation = { event, started };
    try {
      const next = await this.discoverLeader({ timeoutMs });
      event.newLeader = next.id;
      event.quorumRecoveryMs = performance.now() - started;
    } finally { this._recoveryObservation = null; }
    return { event, started };
  }

  /** The host is back: wait until this group's replica caught up with its leader. */
  async observeHostRestart(hostId, { event, started }, { timeoutMs = this.startupTimeoutMs } = {}) {
    const node = this.members.find((member) => member.id === hostId);
    node.process = { attached: true, ended: false, intentional: false };
    const leader = await this.discoverLeader({ timeoutMs });
    const target = (await this.metrics(leader)).last_applied?.index ?? 0;
    await this._until(`restarted replica ${hostId} catches up`, async (requestTimeoutMs) => {
      const response = await this._fetch(node, "/raft/metrics", { timeoutMs: requestTimeoutMs });
      return response.ok && response.value.last_applied?.index >= target
        && Number(response.value.current_leader) === leader.id;
    }, timeoutMs);
    event.restartCatchUpMs = performance.now() - started;
    return event;
  }

  logTails() {
    return this._generations.map((runtime, index) => ({
      node: runtime.id, generation: index + 1, pid: runtime.child.pid,
      exit: runtime.exit ?? null, error: runtime.error?.message ?? null, tail: runtime.logs.slice(-12_000),
    }));
  }

  async close() {
    if (this._closing) return this._closing;
    this._controller.abort(new Error("Benchmark cluster is closing"));
    this._closing = (async () => {
      // Let any in-flight port/directory allocation settle before collecting it.
      await this._starting?.catch(() => {});
      await Promise.all(this._reservations.map(releasePort));
      await Promise.all(this._generations.map(async (runtime) => {
        runtime.intentional = true;
        if (!runtime.ended) runtime.child.kill("SIGTERM");
        if (!await waitForExit(runtime, 1_000)) {
          runtime.child.kill("SIGKILL");
          if (!await waitForExit(runtime, 5_000)) throw new Error(`Could not stop Flower process ${runtime.child.pid}`);
        }
      }));
      await Promise.allSettled([this._recovering, this._discovering]);
      if (!this.keepData) await Promise.all(this.directories.map((directory) => rm(directory, { recursive: true, force: true })));
    })();
    return this._closing;
  }
}

/**
 * Server processes shared by several groups: host k serves replica k of every
 * group from one database, so their log appends share its fsyncs. Groups
 * attach to their replicas' addresses (LocalCluster `attach`).
 */
export class HostCluster {
  constructor(options = {}) {
    this.nodeCount = options.nodes ?? 3;
    this.groups = positiveInteger(options.groups ?? 1, "groups");
    this.binary = resolve(options.binary ?? join(root, "target/release/flower"));
    this.startupTimeoutMs = positiveInteger(options.startupTimeoutMs ?? 30_000, "startupTimeoutMs");
    this.keepData = options.keepData ?? false;
    this.dataRoots = options.data ?? null;
    this.onProcess = options.onProcess;
    this.adminToken = randomUUID();
    this.hosts = [];
    this.directory = null;
    this.directories = [];
    this._reservations = [];
    this._generations = [];
    this._controller = new AbortController();
  }

  get pids() {
    return this.hosts.filter((host) => host.process && !host.process.ended).map((host) => ({ id: host.id, pid: host.process.child.pid }));
  }

  /** One group's replicas, for LocalCluster's `attach`. */
  attachment(group) {
    return { adminToken: this.adminToken, members: this.hosts.map((host) => ({ id: host.id, address: host.addresses[group] })) };
  }

  _startHost(host) {
    const replicas = host.addresses.flatMap((address, group) => ["--replica", `group-${group},${host.id},${address}`]);
    const child = spawn(this.binary, ["--data", host.directory, ...replicas], {
      cwd: root,
      env: { ...process.env, FLOWER_ADMIN_TOKEN: this.adminToken, RUST_LOG: process.env.FLOWER_BENCH_LOG ?? "flower=info,openraft=warn" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const runtime = { child, id: host.id, logs: "", ended: false, intentional: false };
    const collect = (chunk) => { runtime.logs = (runtime.logs + chunk.toString()).slice(-64_000); };
    child.stdout.on("data", collect);
    child.stderr.on("data", collect);
    child.on("error", (error) => { runtime.error = error; runtime.ended = true; });
    runtime.exited = new Promise((resolve) => child.once("close", (code, signal) => {
      runtime.ended = true;
      runtime.exit = { code, signal };
      if (Number.isSafeInteger(child.pid)) this.onProcess?.({ type: "exit", pid: child.pid });
      resolve();
    }));
    host.process = runtime;
    this._generations.push(runtime);
    if (Number.isSafeInteger(child.pid)) this.onProcess?.({ type: "spawn", pid: child.pid });
  }

  async _fetch(address, path, { method = "GET", body, timeoutMs = 2_000 } = {}) {
    const response = await fetch(`http://${address}${path}`, {
      method, headers: { authorization: `Bearer ${this.adminToken}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.any([this._controller.signal, AbortSignal.timeout(timeoutMs)]),
    });
    const text = await response.text();
    let value;
    try { value = JSON.parse(text); } catch { value = text; }
    return { ok: response.ok, status: response.status, value };
  }

  async _until(label, operation) {
    const deadline = performance.now() + this.startupTimeoutMs;
    let last;
    while (performance.now() < deadline) {
      for (const host of this.hosts) {
        if (host.process?.ended && !host.process.intentional) throw new Error(`Host ${host.id} exited: ${host.process.logs.slice(-2_000)}`);
      }
      try { if (await operation()) return; } catch (error) { last = error; }
      await delay(75, undefined, { signal: this._controller.signal });
    }
    throw new Error(`${label} timed out${last ? `: ${last.message}` : ""}`);
  }

  async _serving(host) {
    const results = await Promise.all(host.addresses.map(async (address) => {
      try { return (await this._fetch(address, "/raft/metrics")).ok; } catch { return false; }
    }));
    return results.every(Boolean);
  }

  async start() {
    await access(this.binary, constants.X_OK);
    this.directories = await dataDirectories(this.dataRoots, "flower-bench-hosts-");
    [this.directory] = this.directories;
    for (let index = 0; index < this.nodeCount; index++) {
      const addresses = [];
      for (let group = 0; group < this.groups; group++) {
        const reservation = await reservePort();
        this._reservations.push(reservation);
        addresses.push(`127.0.0.1:${reservation.port}`);
      }
      this.hosts.push({ id: index + 1, addresses, directory: join(this.directories[index % this.directories.length], `host-${index + 1}`) });
    }
    await Promise.all(this._reservations.map(releasePort));
    for (const host of this.hosts) this._startHost(host);
    await this._until("start all Flower hosts", async () => (await Promise.all(this.hosts.map((host) => this._serving(host)))).every(Boolean));
    // The initializing replica wins the first election, so rotate it: each
    // host starts out leading an equal share of groups.
    for (let group = 0; group < this.groups; group++) {
      const members = Object.fromEntries(this.hosts.map((host) => [host.id, host.addresses[group]]));
      const initializer = this.hosts[group % this.hosts.length];
      const response = await this._fetch(initializer.addresses[group], "/raft/initialize", { method: "POST", body: members, timeoutMs: this.startupTimeoutMs });
      if (!response.ok) throw new Error(`Group ${group} initialization returned HTTP ${response.status}: ${JSON.stringify(response.value)}`);
    }
    return this;
  }

  /** Each host's batched storage commits, shared by all of its replicas. */
  async storageCounters() {
    const counters = {};
    await Promise.all(this.hosts.filter((host) => host.process && !host.process.ended).map(async (host) => {
      try {
        const response = await this._fetch(host.addresses[0], "/admin/resources");
        if (response.ok && response.value?.storage) counters[host.id] = { pid: host.process.child.pid, ...response.value.storage };
      } catch { /* Unmeasured, like a restarted host. */ }
    }));
    return counters;
  }

  /** The host that currently leads the most groups. */
  async busiestHost() {
    const leaders = new Map(this.hosts.map((host) => [host.id, 0]));
    for (let group = 0; group < this.groups; group++) {
      for (const host of this.hosts) {
        try {
          const metrics = (await this._fetch(host.addresses[group], "/raft/metrics")).value;
          if (metrics.state === "Leader") { leaders.set(host.id, leaders.get(host.id) + 1); break; }
        } catch { /* An unreachable replica leads nothing. */ }
      }
    }
    return [...leaders].sort((a, b) => b[1] - a[1] || a[0] - b[0])[0];
  }

  async crash(hostId) {
    const host = this.hosts.find((candidate) => candidate.id === hostId);
    host.process.intentional = true;
    host.process.child.kill("SIGKILL");
    if (!await waitForExit(host.process, 5_000)) throw new Error(`Killed host ${hostId} did not exit`);
  }

  async restart(hostId) {
    const host = this.hosts.find((candidate) => candidate.id === hostId);
    this._startHost(host);
    await this._until(`restart host ${hostId}`, () => this._serving(host));
  }

  logTails() {
    return this._generations.map((runtime, index) => ({ node: `host-${runtime.id}`, generation: index + 1, pid: runtime.child.pid,
      exit: runtime.exit ?? null, error: runtime.error?.message ?? null, tail: runtime.logs.slice(-12_000) }));
  }

  async close() {
    this._controller.abort(new Error("Benchmark hosts are closing"));
    await Promise.all(this._reservations.map(releasePort));
    await Promise.all(this._generations.map(async (runtime) => {
      runtime.intentional = true;
      if (!runtime.ended) runtime.child.kill("SIGTERM");
      if (!await waitForExit(runtime, 5_000)) {
        runtime.child.kill("SIGKILL");
        await waitForExit(runtime, 5_000);
      }
    }));
    if (!this.keepData) await Promise.all(this.directories.map((directory) => rm(directory, { recursive: true, force: true })));
  }
}
