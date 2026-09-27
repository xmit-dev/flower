#!/usr/bin/env node
// Resident memory of Flower servers as their application state grows: seeds
// bench/footprint.ts over HTTP, sampling each node's resident set and redb
// file, then restarts every node and samples again. With --catch-up, one
// follower stops halfway and restarts at the end, when it must install a
// snapshot; its peak resident set during catch-up is reported. With
// --attach and --container, it drives one node running as process 1 of a
// Docker container, such as one with a memory limit, and restarts that.
//
//   node bench/footprint-server.mjs --binary target/release/flower --orders 300000
import { execFile } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, stat, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";
import { LocalCluster } from "./cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/index.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const execute = promisify(execFile);
const { values } = parseArgs({
  options: {
    binary: { type: "string", default: join(root, "target/release/flower") },
    nodes: { type: "string", default: "1" },
    orders: { type: "string", default: "100000" },
    batch: { type: "string", default: "500" },
    lines: { type: "string", default: "3" },
    samples: { type: "string", default: "10" },
    out: { type: "string" },
    "catch-up": { type: "boolean", default: false },
    logs: { type: "string" },
    "keep-data": { type: "boolean", default: false },
    attach: { type: "string" },
    "admin-token": { type: "string" },
    container: { type: "string" },
    "container-redb": { type: "string", default: "/data/node-1/flower.redb" },
  },
});
const [nodes, orders, batch, lines, samples] = ["nodes", "orders", "batch", "lines", "samples"].map((name) => {
  const value = Number(values[name]);
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`--${name} must be a positive integer`);
  return value;
});

if (values["catch-up"]) {
  if (nodes < 3) throw new Error("--catch-up needs at least three nodes");
  // Purge logs promptly, so that the stopped follower needs a snapshot.
  process.env.FLOWER_SNAPSHOT_AFTER_LOGS ??= "16";
  process.env.FLOWER_SNAPSHOT_KEEP_LOGS ??= "0";
}
const attach = values.attach && { members: [{ id: 1, address: values.attach }], adminToken: values["admin-token"] };
if (attach && (nodes !== 1 || values["catch-up"] || !values.container)) {
  throw new Error("--attach drives one node, in the Docker container named by --container");
}
const cluster = new LocalCluster({ nodes, binary: values.binary, startupTimeoutMs: 600_000, requestTimeoutMs: 60_000, attach, keepData: values["keep-data"] });
// Copy every process's output to its own file, when asked.
let generation = 0;
async function record() {
  if (!values.logs) return;
  await mkdir(values.logs, { recursive: true });
  for (const member of cluster.members) {
    const child = member.process?.child;
    if (!child || child.recorded) continue;
    child.recorded = true;
    const file = createWriteStream(join(values.logs, `node-${member.id}-${++generation}.log`));
    child.stdout.pipe(file);
    child.stderr.pipe(file);
  }
}
const transport = createHttp2Transport({ requestTimeoutMs: 120_000 });
const client = () => new FlowerClient(cluster.leader.url, { fetch: transport.fetch });
const admin = () => new FlowerAdmin(cluster.leader.url, { adminToken: cluster.adminToken, fetch: transport.fetch });
const started = performance.now();
const report = { binary: values.binary, nodes, orders, batch, lines, env: environment(), samples: [] };

function environment() {
  return Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("FLOWER_")));
}

async function residentBytes(pid) {
  const { stdout } = await execute("ps", ["-o", "rss=", "-p", String(pid)]);
  return Number(stdout.trim()) * 1024;
}

// Latencies of the mutations since the previous sample.
let latencies = [];
async function timed(operation) {
  const begun = performance.now();
  await operation;
  latencies.push(performance.now() - begun);
}

async function attachedStats() {
  const status = (await execute("docker", ["exec", values.container, "cat", "/proc/1/status"])).stdout;
  const size = await execute("docker", ["exec", values.container, "stat", "-c", "%s", values["container-redb"]]).catch(() => null);
  return { id: 1, residentBytes: Number(/VmRSS:\s+(\d+) kB/.exec(status)[1]) * 1024, redbBytes: size ? Number(size.stdout.trim()) : null };
}

async function sample(label, seeded) {
  const members = attach ? [await attachedStats()] : [];
  for (const member of attach ? [] : cluster.members) {
    const pid = member.process && !member.process.ended ? member.process.child.pid : null;
    const file = await stat(join(member.directory, "flower.redb")).catch(() => null);
    members.push({ id: member.id, residentBytes: pid ? await residentBytes(pid) : null, redbBytes: file?.size ?? null });
  }
  const sorted = latencies.sort((a, b) => a - b);
  const mutationMs = sorted.length ? { count: sorted.length, mean: sorted.reduce((a, b) => a + b) / sorted.length, max: sorted.at(-1) } : null;
  latencies = [];
  const entry = { label, orders: seeded, seconds: (performance.now() - started) / 1000, mutationMs, members };
  report.samples.push(entry);
  const mib = (bytes) => bytes === null ? "n/a" : `${(bytes / 2 ** 20).toFixed(0)} MiB`;
  const timing = mutationMs ? `  writes mean ${mutationMs.mean.toFixed(1)} ms, max ${mutationMs.max.toFixed(0)} ms` : "";
  console.log(`${label.padEnd(10)} ${String(seeded).padStart(9)} orders${timing}  ` +
    members.map((member) => `node ${member.id}: RSS ${mib(member.residentBytes)}, redb ${mib(member.redbBytes)}`).join("; "));
}

try {
  if (attach) {
    const response = await fetch(`http://${values.attach}/raft/initialize`, {
      method: "POST", body: JSON.stringify({ 1: values.attach }),
      headers: { "content-type": "application/json", authorization: `Bearer ${cluster.adminToken}` },
    });
    if (!response.ok) throw new Error(`initialize returned HTTP ${response.status}: ${await response.text()}`);
  }
  await cluster.start();
  await record();
  await admin().deploy(await buildBundle(join(root, "bench/footprint.ts")), { requestId: "footprint-deploy" });
  await sample("deployed", 0);
  const every = Math.max(batch, Math.ceil(orders / samples / batch) * batch);
  let stopped = null;
  for (let start = 0; start < orders; start += batch) {
    const count = Math.min(batch, orders - start);
    if (values["catch-up"] && !stopped && start >= orders / 2) {
      stopped = cluster.members.find((member) => member.id !== cluster.leader.id);
      stopped.process.intentional = true;
      stopped.process.child.kill("SIGTERM");
      await stopped.process.exited;
      console.log(`stopped node ${stopped.id} at ${start} orders`);
    }
    await timed(client().mutate("seed", { start, count, lines }, { requestId: `seed-${start}`, signal: AbortSignal.timeout(600_000) }));
    if ((start + count) % every === 0 || start + count === orders) await sample("seeded", start + count);
  }
  for (let n = 0; n < 200; n++) {
    await timed(client().mutate("touch", { order: (n * 7919) % orders, quantity: 1 + n % 7 }, { requestId: `touch-${n}`, signal: AbortSignal.timeout(60_000) }));
  }
  await sample("touched", orders);
  if (stopped) {
    const target = (await cluster.metrics()).last_applied.index;
    const catching = performance.now();
    cluster._startNode(stopped);
    await record();
    let peak = 0;
    for (;;) {
      if (performance.now() - catching > 600_000) {
        console.log(stopped.process.logs.slice(-20_000));
        throw new Error(`node ${stopped.id} did not catch up within ten minutes`);
      }
      peak = Math.max(peak, await residentBytes(stopped.process.child.pid).catch(() => 0));
      const response = await cluster._fetch(stopped, "/raft/metrics", { timeoutMs: 5_000 }).catch(() => null);
      if (response?.ok && response.value.last_applied?.index >= target) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    report.catchUp = { node: stopped.id, seconds: (performance.now() - catching) / 1000, peakResidentBytes: peak, snapshot: (await cluster.metrics(stopped)).snapshot };
    console.log(`node ${stopped.id} caught up in ${report.catchUp.seconds.toFixed(1)} s, peak RSS ${(peak / 2 ** 20).toFixed(0)} MiB, snapshot ${JSON.stringify(report.catchUp.snapshot)}`);
    await sample("caught-up", orders);
  }
  const restarting = performance.now();
  if (attach) {
    await execute("docker", ["restart", "-t", "30", values.container]);
  } else {
    for (const member of cluster.members) {
      member.process.intentional = true;
      member.process.child.kill("SIGTERM");
      await member.process.exited;
    }
    for (const member of cluster.members) cluster._startNode(member);
    await record();
  }
  await cluster.discoverLeader({ timeoutMs: 600_000 });
  report.restartSeconds = (performance.now() - restarting) / 1000;
  await sample("restarted", orders);
  for (let n = 0; n < 200; n++) {
    await timed(client().mutate("touch", { order: (n * 104729) % orders, quantity: 1 + n % 5 }, { requestId: `retouch-${n}`, signal: AbortSignal.timeout(60_000) }));
  }
  await sample("retouched", orders);
  console.log(`restart to leader: ${report.restartSeconds.toFixed(2)} s`);
} finally {
  await cluster.close();
  if (values["keep-data"]) console.log(`kept data in ${cluster.members.map((member) => member.directory).join(", ")}`);
  await transport.close();
  if (values.out) {
    await mkdir(dirname(resolve(values.out)), { recursive: true });
    await writeFile(values.out, `${JSON.stringify(report, null, 2)}\n`);
  }
}
