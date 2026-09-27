import assert from "node:assert/strict";
import { access, mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { LocalCluster } from "./cluster.mjs";

async function assertPortReleased(address) {
  const port = Number(address.split(":")[1]);
  const server = createServer();
  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, "127.0.0.1", resolve);
    });
  } finally {
    if (server.listening) await new Promise((resolve) => server.close(resolve));
  }
}

test("cluster rejects configurations which cannot exercise supported quorum sizes", () => {
  assert.throws(() => new LocalCluster({ nodes: 2 }), /1 or 3/);
  assert.throws(() => new LocalCluster({ startupTimeoutMs: 0 }), /positive integer/);
  assert.throws(() => new LocalCluster({ requestTimeoutMs: NaN }), /positive integer/);
});

test("missing binary fails before creating any data or child processes", async () => {
  const cluster = new LocalCluster({ binary: join(tmpdir(), `missing-flower-${randomUUID()}`) });
  await assert.rejects(cluster.start(), { code: "ENOENT" });
  assert.equal(cluster.directory, null);
  assert.deepEqual(cluster.pids, []);
  await cluster.close();
});

test("unexpected process exits fail startup and release every port and data directory", async () => {
  // Node deliberately rejects Flower's --id argument and exits immediately.
  const cluster = new LocalCluster({ binary: process.execPath, startupTimeoutMs: 10_000 });
  const started = Date.now();
  await assert.rejects(cluster.start(), (error) => {
    assert.match(error.message, /exited unexpectedly/);
    assert.ok(error.clusterLogs.length > 0);
    return true;
  });
  assert.ok(Date.now() - started < 5_000, "process failure must not wait for the startup timeout");
  assert.deepEqual(cluster.pids, []);
  await assert.rejects(access(cluster.directory), { code: "ENOENT" });
  for (const node of cluster.members) await assertPortReleased(node.address);
  await cluster.close();
});

test("closing during startup cancels work and cleans allocations that were in flight", async () => {
  const cluster = new LocalCluster({ binary: process.execPath });
  const starting = cluster.start();
  const rejected = assert.rejects(starting, /closing/);
  await Promise.all([cluster.close(), rejected]);
  assert.deepEqual(cluster.pids, []);
  if (cluster.directory) await assert.rejects(access(cluster.directory), { code: "ENOENT" });
  for (const node of cluster.members) await assertPortReleased(node.address);
});

test("keepData retains only the harness-created directory for inspection", async () => {
  const cluster = new LocalCluster({ binary: process.execPath, nodes: 1, keepData: true });
  try {
    await assert.rejects(cluster.start(), /exited unexpectedly/);
    await access(cluster.directory);
    assert.deepEqual(cluster.pids, []);
    for (const node of cluster.members) await assertPortReleased(node.address);
  } finally {
    if (cluster.directory) await rm(cluster.directory, { recursive: true, force: true });
  }
});

test("data roots receive servers in turn and are all removed", async () => {
  const roots = await Promise.all(["a", "b"].map((name) => mkdtemp(join(tmpdir(), `flower-root-${name}-`))));
  try {
    const cluster = new LocalCluster({ binary: process.execPath, data: roots, startupTimeoutMs: 10_000 });
    await assert.rejects(cluster.start(), /exited unexpectedly/);
    assert.deepEqual(cluster.directories.map((directory) => dirname(directory)), roots);
    assert.equal(cluster.directory, cluster.directories[0]);
    assert.deepEqual(cluster.members.map((node) => dirname(node.directory)), [0, 1, 0].map((index) => cluster.directories[index]));
    for (const directory of cluster.directories) await assert.rejects(access(directory), { code: "ENOENT" });
    await cluster.close();
  } finally {
    await Promise.all(roots.map((root) => rm(root, { recursive: true, force: true })));
  }
});

test("closed clusters cannot restart and single-node clusters reject failover", async () => {
  const cluster = new LocalCluster({ nodes: 1 });
  await assert.rejects(cluster.crashLeaderAndRecover(), /three-node/);
  await cluster.close();
  await assert.rejects(cluster.start(), /closing/);
  await assert.rejects(cluster.discoverLeader(), /closing/);
});

function simulatedCluster() {
  const cluster = new LocalCluster({ startupTimeoutMs: 1_000 });
  cluster.members = [1, 2, 3].map((id) => ({ id, process: { ended: false, intentional: false } }));
  return cluster;
}

test("concurrent retry lookups discard a completed response from a dying leader and share the next probe", async () => {
  const cluster = simulatedCluster();
  const reachedProbe = Promise.withResolvers();
  const releaseProbe = Promise.withResolvers();
  let leaderId = 1;
  const probes = [];
  cluster._fetch = async (node, path) => {
    if (path === "/raft/metrics") return { ok: true, value: {
      state: node.id === leaderId ? "Leader" : "Follower", current_leader: leaderId,
    } };
    probes.push(node.id);
    if (node.id === 1) {
      reachedProbe.resolve();
      await releaseProbe.promise;
    }
    return { status: 404, value: { error: { code: "METHOD_NOT_FOUND" } } };
  };
  try {
    const lookups = Array.from({ length: 32 }, () => cluster.discoverLeader());
    await reachedProbe.promise;
    // SIGKILL has been sent, but the operating system's close event is pending.
    cluster.members[0].process.intentional = true;
    leaderId = 2;
    releaseProbe.resolve();
    const leaders = await Promise.all(lookups);
    assert.ok(leaders.every((node) => node.id === 2));
    assert.equal(cluster.leader.id, 2);
    assert.deepEqual(probes, [1, 2], "all callers must share each election probe");
  } finally { releaseProbe.resolve(); await cluster.close(); }
});

test("a restarted process must get a fresh quorum probe before becoming the discovered leader", async () => {
  const cluster = simulatedCluster();
  const reachedProbe = Promise.withResolvers();
  const releaseProbe = Promise.withResolvers();
  let probes = 0;
  cluster._fetch = async (node, path) => {
    if (path === "/raft/metrics") return { ok: true, value: {
      state: node.id === 1 ? "Leader" : "Follower", current_leader: 1,
    } };
    probes++;
    if (probes === 1) { reachedProbe.resolve(); await releaseProbe.promise; }
    return { status: 404, value: { error: { code: "METHOD_NOT_FOUND" } } };
  };
  try {
    const lookup = cluster.discoverLeader();
    await reachedProbe.promise;
    cluster.members[0].process = { ended: false, intentional: false };
    releaseProbe.resolve();
    assert.equal((await lookup).id, 1);
    assert.equal(probes, 2);
  } finally { releaseProbe.resolve(); await cluster.close(); }
});

test("a caller joining discovery keeps its own timeout, and close cancels the shared loop", async () => {
  const cluster = simulatedCluster();
  cluster._fetch = async () => ({ ok: true, value: { state: "Follower", current_leader: null } });
  const pending = cluster.discoverLeader({ timeoutMs: 1_000 });
  const cancelled = assert.rejects(pending, /closing|aborted/);
  await assert.rejects(cluster.discoverLeader({ timeoutMs: 25 }), /timed out/);
  const closed = cluster.close();
  await Promise.race([
    Promise.all([closed, cancelled]),
    delay(500).then(() => { throw new Error("close did not promptly cancel leader discovery"); }),
  ]);
});

test("a recovery caller outlives the shorter RPC retry which started shared discovery", async () => {
  const cluster = simulatedCluster();
  const availableAt = performance.now() + 80;
  cluster._fetch = async (node, path) => {
    if (path === "/raft/metrics") return { ok: true, value: {
      state: node.id === 2 && performance.now() >= availableAt ? "Leader" : "Follower", current_leader: 2,
    } };
    return { status: 404, value: { error: { code: "METHOD_NOT_FOUND" } } };
  };
  try {
    const retry = assert.rejects(cluster.discoverLeader({ timeoutMs: 25 }), /timed out/);
    const recovery = cluster.discoverLeader({ timeoutMs: 500 });
    assert.equal((await recovery).id, 2);
    await retry;
  } finally { await cluster.close(); }
});

test("a serving leader wins discovery without waiting for a stalled peer or obsolete leader", async () => {
  const cluster = simulatedCluster();
  const stalled = [];
  cluster._fetch = async (node, path, { signal }) => {
    if ((node.id === 1 && path === "/v1/query") || node.id === 3) {
      stalled.push(signal);
      return new Promise((_, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
      });
    }
    if (path === "/raft/metrics") return { ok: true, value: { state: "Leader", current_leader: node.id } };
    return { status: 404, value: { error: { code: "METHOD_NOT_FOUND" } } };
  };
  try {
    // Each stalled request could take the whole 1s discovery budget. Success
    // must instead follow node 2's completed quorum probe, cancelling losers.
    const leader = await Promise.race([
      cluster.discoverLeader({ timeoutMs: 1_000 }),
      delay(300).then(() => { throw new Error("serving leader waited for unrelated peers"); }),
    ]);
    assert.equal(leader.id, 2);
    assert.equal(stalled.length, 2);
    assert.ok(stalled.every((signal) => signal.aborted));
  } finally { await cluster.close(); }
});

test("recovery observations distinguish candidate, leader claim, and completed quorum probe", async () => {
  const cluster = simulatedCluster();
  const event = { oldLeader: 1 };
  cluster._recoveryObservation = { event, started: performance.now() };
  let available = false;
  cluster._fetch = async (node, path) => {
    if (path === "/raft/metrics") return { ok: true, value: {
      state: node.id === 2 ? (available ? "Leader" : "Candidate") : "Follower", current_leader: available ? 2 : null,
    } };
    await delay(5);
    return { status: 404, value: { error: { code: "METHOD_NOT_FOUND" } } };
  };
  try {
    assert.equal(await cluster._probeLeader(100), null);
    assert.ok(Number.isFinite(event.candidateObservedMs));
    assert.equal(event.leaderObservedMs, undefined);
    available = true;
    assert.equal((await cluster.discoverLeader()).id, 2);
    assert.ok(event.leaderObservedMs >= event.candidateObservedMs);
    assert.ok(event.quorumProbeMs >= event.leaderObservedMs);
  } finally { await cluster.close(); }
});

test("process observers report startup exits and cannot break owned-process cleanup", async () => {
  const events = [];
  const cluster = new LocalCluster({ binary: process.execPath, onProcess(event) {
    events.push(event);
    throw new Error("observer unavailable");
  } });
  await assert.rejects(cluster.start(), /observer unavailable/);
  await cluster.close();
  const spawned = events.filter((event) => event.type === "spawn").map((event) => event.pid);
  const exited = events.filter((event) => event.type === "exit").map((event) => event.pid);
  assert.ok(spawned.length > 0);
  assert.deepEqual(exited.sort(), spawned.sort());
  for (const pid of spawned) assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
});
