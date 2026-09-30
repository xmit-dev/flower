// Run after cargo build: node tests/e2e-backup.mjs
// Backs up to a temporary directory, or with E2E_BACKUP_URL (s3://BUCKET/PREFIX,
// and FLOWER_BACKUP_S3_* for the store) under a fresh prefix of that URL.
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/index.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = process.env.E2E_FLOWER_BIN ?? join(root, "target/debug/flower");
const scratch = await mkdtemp(join(tmpdir(), "flower-backup-e2e-"));
const url = process.env.E2E_BACKUP_URL
  ? `${process.env.E2E_BACKUP_URL.replace(/\/$/, "")}/e2e-${randomUUID()}`
  : `file://${join(scratch, "backups")}`;
process.env.FLOWER_BACKUP_URL = url;
process.env.FLOWER_BACKUP_INTERVAL_MS = "100";
process.env.FLOWER_BACKUP_BASE_AFTER_BYTES = "4000";
// Raft compacts its log often, so shipping relies on the entries held for it.
process.env.FLOWER_SNAPSHOT_AFTER_LOGS = "8";
process.env.FLOWER_SNAPSHOT_LAG_LOGS = "16";
process.env.FLOWER_SNAPSHOT_KEEP_LOGS = "0";

const cluster = new LocalCluster({ nodes: 1, binary });
const client = () => new FlowerClient(cluster.leader.url);
let sequence = 0;
const add = (amount) => client().mutate("add", amount, { requestId: `backup-${++sequence}` });
const run = async (...args) => JSON.parse((await promisify(execFile)(binary, args, {
  env: { ...process.env, RUST_LOG: "warn" }, maxBuffer: 16 << 20,
})).stdout);

async function backupStatus() {
  const response = await cluster._fetch(cluster.leader, "/admin/backup");
  assert.equal(response.status, 200);
  return response.value;
}

async function shipped() {
  return cluster._until("the backup ships every applied entry", async () => {
    const applied = (await cluster.metrics(cluster.leader)).last_applied?.index;
    const { status } = await backupStatus();
    return status.role === "leader" && status.applied === applied && status.lagEntries === 0
      && (status.shipped?.index ?? status.newestBase?.index) === applied
      && status.newestBase && !status.baseInProgress && !status.failing && status;
  }, 20_000, { pollMs: 50 });
}

async function freePort() {
  const server = createServer();
  await new Promise((done) => server.listen(0, "127.0.0.1", done));
  const { port } = server.address();
  await new Promise((done) => server.close(done));
  return port;
}

/** Restore to `point` (arguments for `flower backup restore`), serve it, and read the counter. */
async function restoredValue(point) {
  const data = join(scratch, `restored-${randomUUID()}`);
  const address = `127.0.0.1:${await freePort()}`;
  const summary = await run("backup", "restore", "--from", url, "--data", data, "--id", "1", "--advertise", address, ...point);
  const token = randomUUID();
  const child = spawn(binary, ["--id", "1", "--listen", address, "--data", data], {
    env: { ...process.env, FLOWER_BACKUP_URL: "", FLOWER_ADMIN_TOKEN: token, RUST_LOG: "warn" },
    stdio: ["ignore", "ignore", "pipe"],
  });
  let logs = "";
  child.stderr.on("data", (chunk) => { logs += chunk; });
  const exited = new Promise((done) => child.once("close", done));
  try {
    const restored = new FlowerClient(`http://${address}`);
    for (let attempt = 0; ; attempt++) {
      try {
        return { value: (await restored.query("get")).value, summary };
      } catch (error) {
        // Starting, electing itself and loading the bundle: slow on a busy host.
        if (attempt > 1200) throw new Error(`restored server did not serve: ${error.message}\n${logs}`);
        await delay(50);
      }
    }
  } finally {
    child.kill("SIGTERM");
    await exited;
    await rm(data, { recursive: true, force: true });
  }
}

try {
  await cluster.start();
  const fixture = join(cluster.directory, "counter.ts");
  await writeFile(fixture, `import { collection, define, mutation, query } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const counters = collection<number>("counters");
const add = mutation("add", (ctx, amount: number) => { const value = (ctx.get(counters, "c") ?? 0) + amount; ctx.set(counters, "c", value); return value; });
const get = query("get", ctx => ctx.get(counters, "c") ?? 0);
export default define({ collections: [counters], http: { add, get } });`);
  await new FlowerAdmin(cluster.leader.url, { adminToken: cluster.adminToken })
    .deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "backup-deploy" });
  const points = [];
  let value = 0;
  for (let round = 1; round <= 4; round++) {
    for (let n = 1; n <= 15; n++) value = (await add(round * 100 + n)).value;
    const status = await shipped();
    await delay(20);
    points.push({ time: Date.now(), index: status.applied, value });
    await delay(20);
  }
  const { status } = await backupStatus();
  assert.ok(status.basesWritten >= 2, JSON.stringify(status));
  assert.ok(status.hold.maxBytes > 0);
  // Written just before a graceful stop: the stop ships it.
  value = (await add(1_000_000)).value;
  const node = cluster.leader;
  node.process.intentional = true;
  node.process.child.kill("SIGTERM");
  await node.process.exited;

  const listed = await run("backup", "list", "--from", url);
  assert.equal(listed.generations.length, 1, JSON.stringify(listed));
  assert.equal(listed.generations[0].reason, "new");
  assert.ok(listed.generations[0].bases.length >= 2);
  for (const point of points) {
    const byTime = await restoredValue(["--at", new Date(point.time).toISOString()]);
    assert.equal(byTime.value, point.value, JSON.stringify(byTime.summary));
    // Internal entries (maintenance) may follow the point's last mutation.
    assert.ok(byTime.summary.restored.index >= point.index, JSON.stringify(byTime.summary));
    assert.equal((await restoredValue(["--index", String(point.index)])).value, point.value);
  }
  const latest = await restoredValue([]);
  assert.equal(latest.value, value);
  assert.ok(latest.summary.node.term > 2 ** 20);
  console.log(`PASS: continuous backups to ${url.startsWith("s3://") ? "S3" : "a directory"} restore the state at any point in time or index, including the write shipped at shutdown, and the restored node serves it`);
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await cluster.close();
  await rm(scratch, { recursive: true, force: true });
}
