// E2E_FLOWER_BIN=target/release/flower node tests/e2e-staged-deployment.mjs
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { copyFile, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

process.env.FLOWER_SNAPSHOT_AFTER_LOGS = "8";
process.env.FLOWER_SNAPSHOT_LAG_LOGS = "16";
process.env.FLOWER_SNAPSHOT_KEEP_LOGS = "0";
const sourceBinary = resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower");
const binaryDirectory = await mkdtemp(join(tmpdir(), "flower-staged-binary-"));
const binary = join(binaryDirectory, "flower");
await copyFile(sourceBinary, binary);
async function binaryHash() {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(binary)) hash.update(chunk);
  return hash.digest("hex");
}
const initialBinaryHash = await binaryHash();
const cluster = new LocalCluster({ nodes: 3, binary });
const transport = createHttp2Transport({ requestTimeoutMs: 20_000 });
const follower = () => cluster.members.find(node => node.id !== cluster.leader.id).url;
const client = (credentials = { subject: "alice", allowNew: true }) => new FlowerClient(follower(), { credentials, fetch: transport.fetch });
const admin = () => new FlowerAdmin(follower(), { adminToken: cluster.adminToken, fetch: transport.fetch });
let sequence = 0;
let lastChange = null;
const ledger = new Map();
function source(version, indexVersion = version) {
  return `import {aggregate,collection,define,derive,mutation,query} from ${JSON.stringify(resolve("sdk/index.ts"))};
const rows=collection("rows").index("id",["id"])${indexVersion >= 2 ? '.index("status",["status"])' : ''}${indexVersion >= 3 ? '.index("payload",["payload"])' : ''};
const multiplier=derive("multiplier",()=>${version});
const total=derive("total",ctx=>ctx.scan(rows).length*ctx.get(multiplier));
const weighted=aggregate("weighted",{source:rows,index:"id",initial:()=>0,add:(sum,row)=>sum+row.payload.length*${version},remove:(sum,row)=>sum-row.payload.length*${version}});
const detail=derive("detail",(ctx,id)=>{const row=ctx.get(rows,id);return row ? {id,status:row.status,score:ctx.get(weighted,id),factor:ctx.get(multiplier)}:null;});
export default define({collections:[rows],definitions:[total,detail,multiplier,weighted],auth:{authenticate:(_ctx,c:any)=>
  c?.subject==="alice" && (${version}<2 || c?.allowNew) ? {subject:"alice"}:null},http:{
  change:mutation("change",(ctx,a)=>{if(a.value){ctx.set(rows,a.key,a.value);ctx.materialize(detail,a.key);}else{ctx.delete(rows,a.key);ctx.unmaterialize(detail,a.key);}ctx.materialize(total);return ctx.get(total);}),
  read:query("read",(ctx,status)=>{const selected=${indexVersion >= 2 ? 'ctx.query(rows.by("status").eq(status))' : 'ctx.scan(rows).map(row=>row.value).filter(row=>row.status===status)'}.sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0);return {version:${version},total:ctx.get(total),rows:selected,details:selected.map(row=>ctx.get(detail,row.id))};}),
}});`;
}
async function change(key, value) {
  const requestId = `staged-write-${++sequence}`;
  const result = await client().mutate("change", { key, value: value ?? null }, { requestId });
  lastChange = { requestId, key, value: value ?? null, result };
  if (value) ledger.set(key, value); else ledger.delete(key);
}
async function verify(version, who = client()) {
  for (const status of ["ready", "waiting"]) {
    const value = (await who.query("read", status)).value;
    assert.equal(value.version, version);
    assert.equal(value.total, ledger.size * version);
    const expected = [...ledger.values()].filter(row => row.status === status).sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0);
    assert.deepEqual(value.rows, expected);
    assert.deepEqual(value.details, expected.map(row => ({ id: row.id, status: row.status, score: row.payload.length * version, factor: version })));
  }
}
async function progress(id, operation, stop, maxBytes = 4096) {
  let state;
  for (let n = 0; n < 1000; n++) {
    state = (await admin().controlStagedDeployment({ operation, requestId: id, maxBytes })).value;
    assert.notEqual(state.phase, "failed", state.error ?? "staged preparation failed");
    if (state.phase === stop) return state;
  }
  throw new Error(`${operation} did not reach ${stop}: ${JSON.stringify(state)}`);
}
async function restartAll() {
  await Promise.all(cluster.members.map(async node => {
    node.process.intentional = true; node.process.child.kill("SIGKILL"); await node.process.exited;
  }));
  cluster.leader = null;
  for (const node of cluster.members) cluster._startNode(node);
  // A quorum can elect a leader before the third replica starts listening.
  // The test deliberately sends requests through a follower, including that one.
  await cluster._until("restart all Flower nodes", async timeoutMs => {
    const ready = await Promise.all(cluster.members.map(async node => {
      try { return (await cluster._fetch(node, "/raft/metrics", { timeoutMs })).ok; }
      catch { return false; }
    }));
    return ready.every(Boolean);
  });
  await cluster.discoverLeader();
}
try {
  await cluster.start();
  const fixture = join(cluster.directory, "staged.ts");
  const bundle = async (version, indexVersion = version) => { await writeFile(fixture, source(version, indexVersion)); return buildBundle(fixture); };
  await admin().deploy(await bundle(1), { requestId: "initial", preparation: "blocking" });
  for (let i = 0; i < 64; i++) {
    const id = String(i).padStart(3, "0");
    await change(id, { id, status: i % 2 ? "ready" : "waiting", payload: "x".repeat(96) });
  }
  const old = client({ subject: "alice" });
  await verify(1, old);
  const next = await bundle(2);
  const staged = (await admin().stageDeployment(next, { requestId: "index-v2" })).value;
  assert.equal(staged.phase, "backfill");
  const firstResult = await admin().controlStagedDeployment({ operation: "advance", requestId: "index-v2", maxBytes: 4096 });
  const first = firstResult.value;
  assert.equal(first.phase, "backfill", "bounded page must leave durable work to resume");
  assert.ok(first.scannedRows > 0 && first.scannedRows < 64);
  await assert.rejects(client().mutate("change", {
    key: "collision", value: { id: "collision", status: "ready", payload: "must not commit" },
  }, { requestId: "index-v2" }), "a live staged build reserves its deployment intent ID");
  await verify(1, old);
  await restartAll();
  const restoredResult = await admin().stagedDeploymentStatus();
  const restored = restoredResult.value;
  try {
    assert.deepEqual(restored, first, "index progress must survive a full restart");
  } catch (error) {
    // Collect diagnostics only after the failed observation; a pre-crash read
    // would add an extra quorum fence to the durability scenario being tested.
    const recovery = await Promise.allSettled(cluster.members.map(async node => ({
      node: node.id,
      metrics: await cluster.metrics(node),
      status: await new FlowerAdmin(node.url, { adminToken: cluster.adminToken, fetch: transport.fetch }).stagedDeploymentStatus(),
    })));
    await writeFile(join(cluster.directory, "recovery-failure.json"), JSON.stringify({ firstResult, restoredResult, recovery }, null, 2));
    console.error({ firstResult, restoredResult, recovery });
    throw error;
  }
  assert.deepEqual((await admin().stageDeployment(next, { requestId: "index-v2" })).value, restored);

  // Writes crossing both sides of the backfill cursor race bounded advances.
  await Promise.all([
    progress("index-v2", "advance", "rebuilding"),
    (async () => {
      await change("000", { id: "000", status: "ready", payload: "updated behind cursor" });
      await change("063", { id: "063", status: "waiting", payload: "updated ahead" });
      await change("001");
      await change("-new", { id: "-new", status: "ready", payload: "insert before cursor" });
      await change("zzz", { id: "zzz", status: "waiting", payload: "insert after cursor" });
    })(),
  ]);
  let graphPage;
  for (let n = 0; n < 4; n++) {
    graphPage = (await admin().controlStagedDeployment({ operation: "advance", requestId: "index-v2", maxBytes: 64 * 1024 })).value;
  }
  assert.equal(graphPage.phase, "rebuilding", "many materialized roots must leave resumable graph work");
  assert.ok(graphPage.rebuiltRoots >= 4 && graphPage.rebuiltRoots <= 15,
    "adaptive graph pages grow by at most two and preserve bounded progress");
  assert.ok(graphPage.graphCursor && graphPage.generation);
  await assert.rejects(admin().controlStagedDeployment({ operation: "activate", requestId: "index-v2" }), { code: "DEPLOYMENT_CONFLICT" });
  await verify(1, client({ subject: "alice" }));
  await restartAll();
  const resumedGraph = (await admin().stagedDeploymentStatus()).value;
  assert.equal(resumedGraph.graphCursor, graphPage.graphCursor);
  assert.equal(resumedGraph.rebuiltRoots, graphPage.rebuiltRoots);
  assert.equal(resumedGraph.generation, graphPage.generation);

  // Completed roots stay current; roots created behind the durable cursor must
  // join the target graph, and roots removed during preparation must disappear.
  await Promise.all([
    progress("index-v2", "advance", "ready", 64 * 1024),
    (async () => {
      await change("000", { id: "000", status: "waiting", payload: "changed completed root" });
      await change("002");
      await change("!graph-new", { id: "!graph-new", status: "ready", payload: "new root behind cursor" });
      await change("zzz", { id: "zzz", status: "ready", payload: "changed future root" });
    })(),
  ]);
  await verify(1, client({ subject: "alice" }));
  const activated = (await admin().controlStagedDeployment({ operation: "activate", requestId: "index-v2" })).value;
  assert.equal(activated.phase, "active");
  // Code, index and authorization activate atomically: the new hook rejects the old credentials.
  await assert.rejects(client({ subject: "alice" }).query("read", "ready"),
    error => error.status === 403 && error.code === "FORBIDDEN" && error.failure?.code === "UNAUTHENTICATED");
  await verify(2);
  assert.equal((await admin().controlStagedDeployment({ operation: "activate", requestId: "index-v2" })).value.phase, "active");
  await progress("index-v2", "collect", "collected");

  const third = await bundle(3);
  await admin().stageDeployment(third, { requestId: "canceled-v3" });
  await progress("canceled-v3", "advance", "rebuilding");
  const abandonedGraph = (await admin().controlStagedDeployment({ operation: "advance", requestId: "canceled-v3", maxBytes: 64 * 1024 })).value;
  assert.equal(abandonedGraph.rebuiltRoots, 1);
  assert.equal((await admin().controlStagedDeployment({ operation: "cancel", requestId: "canceled-v3" })).value.phase, "canceled");
  await progress("canceled-v3", "collect", "collected");
  await assert.rejects(admin().controlStagedDeployment({ operation: "activate", requestId: "canceled-v3" }));
  await admin().deploy(third, { requestId: "canceled-v3", preparation: "blocking" });
  await verify(2); // The canceled intent replays its terminal receipt, never deploys.

  // A code-only successor rebuilds from the selected graph generation without
  // requiring any index backfill, then survives snapshots and a full restart.
  const successor = (await admin().stageDeployment(await bundle(4, 2), { requestId: "graph-v4" })).value;
  assert.equal(successor.phase, "rebuilding");
  await progress("graph-v4", "advance", "ready", 64 * 1024);
  await admin().controlStagedDeployment({ operation: "activate", requestId: "graph-v4" });
  await verify(4);
  await progress("graph-v4", "collect", "collected");
  const activatedIndex = (await cluster.metrics(cluster.leader)).last_applied.index;
  for (let n = 0; n < 16; n++) {
    await change("000", { id: "000", status: "waiting", payload: `post-activation-${n}` });
  }
  await cluster._until("all replicas snapshot the active graph generation", async () => {
    const metrics = await Promise.all(cluster.members.map(node => cluster.metrics(node)));
    return metrics.every(metric => (metric.snapshot?.index ?? 0) >= activatedIndex);
  });
  await restartAll();
  await verify(4);
  await change("!graph-new", { id: "!graph-new", status: "waiting", payload: "updated after graph recovery" });
  await verify(4);
  assert.equal(await binaryHash(), initialBinaryHash, "every restart must use the original server binary");
  console.log(`PASS: staged deployment over follower h2, bounded index/graph preparation, restart mid-graph, concurrent root changes, atomic code/auth/index cutover, generation cleanup, snapshot recovery (server sha256 ${initialBinaryHash})`);
} catch (error) {
  cluster.keepData = true;
  if (cluster.directory) {
    const recovery = await Promise.allSettled(cluster.members.map(async node => {
      const who = new FlowerClient(node.url, { credentials: { subject: "alice", allowNew: true }, fetch: transport.fetch });
      const operator = new FlowerAdmin(node.url, { adminToken: cluster.adminToken, fetch: transport.fetch });
      return { node: node.id, metrics: await cluster.metrics(node),
        status: await operator.stagedDeploymentStatus({ signal: AbortSignal.timeout(3000) }),
        waiting: await who.query("read", "waiting", { signal: AbortSignal.timeout(3000) }) };
    }));
    await writeFile(join(cluster.directory, "failure-context.json"), JSON.stringify({
      binary: { source: sourceBinary, path: binary, initialHash: initialBinaryHash, finalHash: await binaryHash() },
      lastChange, ledger: [...ledger], recovery,
    }, null, 2));
    await writeFile(join(cluster.directory, "failure-logs.json"), JSON.stringify(cluster.logTails(), null, 2));
  }
  console.error(error); console.error(cluster.logTails());
  console.error(`Failed cluster retained at ${cluster.directory}`); process.exitCode = 1;
} finally {
  await transport.close();
  await cluster.close();
  if (!cluster.keepData) await rm(binaryDirectory, { recursive: true, force: true });
}
