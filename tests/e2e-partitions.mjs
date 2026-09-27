// Three physical one-node groups exercise routing and durable tenant movement.
// Native unit tests cover ownership fences; this harness uses the HTTP surface.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";
import { buildBundle } from "../sdk/bundle.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = resolve(process.env.E2E_FLOWER_BIN ?? join(root, "target/debug/flower"));
const timeout = Number(process.env.E2E_TIMEOUT_MS ?? 45_000);
const token = randomUUID();
const directory = await mkdtemp(join(tmpdir(), "flower-partitions-"));
const nodes = new Map();
const children = [];
const watchController = new AbortController();
const watches = new Set();
let registry;
let sequence = 0;

async function until(label, operation) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try { const value = await operation(); if (value) return value; } catch (error) { last = error; }
    await delay(75);
  }
  throw new Error(`${label} timed out: ${last?.message ?? "condition false"}`, { cause: last });
}
async function reserve(id) {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = `127.0.0.1:${server.address().port}`;
  nodes.set(id, { id, address, url: `http://${address}`, directory: join(directory, id), reservation: server });
}
function start(node) {
  const child = spawn(binary, ["--id", "1", "--listen", node.address, "--data", node.directory], {
    cwd: root, env: { ...process.env, FLOWER_ADMIN_TOKEN: token, FLOWER_GROUP: node.id,
      FLOWER_CATALOG_GROUP: "catalog", FLOWER_GROUPS: JSON.stringify(registry),
      // Group budgets admit the SDK bundle, while the frozen tenant image (five
      // 32 KiB blobs plus that bundle) still exceeds one RPC and 🌻's commands.
      FLOWER_RPC_MAX_BYTES: "196608", FLOWER_TRANSACTION_MAX_BYTES: node.id === "🌻" ? "8192" : "131072", FLOWER_SNAPSHOT_CHUNK_BYTES: "8192",
      FLOWER_SNAPSHOT_AFTER_LOGS: "8", FLOWER_SNAPSHOT_LAG_LOGS: "16", FLOWER_SNAPSHOT_KEEP_LOGS: "0",
      RUST_LOG: "flower=info,openraft=warn" }, stdio: ["ignore", "pipe", "pipe"],
  });
  const runtime = { child, ended: false, logs: "" };
  const capture = (chunk) => { runtime.logs = (runtime.logs + chunk).slice(-60_000); };
  child.stdout.on("data", capture); child.stderr.on("data", capture);
  runtime.exited = new Promise(resolve => child.once("close", () => { runtime.ended = true; resolve(); }));
  node.runtime = runtime; children.push(runtime);
}
async function stop(node) {
  if (!node.runtime?.ended) { node.runtime.child.kill("SIGCONT"); node.runtime.child.kill("SIGKILL"); await node.runtime.exited; }
}
async function post(node, path, body, authenticated = true) {
  const response = await fetch(node.url + path, { method: "POST", headers: { "content-type": "application/json",
    ...(authenticated ? { authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(body), signal: AbortSignal.timeout(10_000) });
  const text = await response.text();
  let value; try { value = JSON.parse(text); } catch { value = text; }
  return { response, value };
}
async function catalog(body) {
  const result = await post(nodes.get("catalog"), "/admin/partitions/catalog", body);
  assert.equal(result.response.status, 200, JSON.stringify(result.value));
  return result.value;
}
const cluster = (gateway = "catalog") => new FlowerAdmin(nodes.get(gateway).url, { adminToken: token });
async function placement(partition) { return cluster().partitionStatus(partition); }
const client = (partition, gateway = "catalog") => new FlowerClient(nodes.get(gateway).url).partition(partition);
async function mutate(partition, method, args, requestId = `request-${++sequence}`) {
  return client(partition).mutate(method, args, { requestId, signal: AbortSignal.timeout(15_000) });
}
async function active(partition, owner) {
  return until(`${partition} active on ${owner}`, async () => {
    const state = await placement(partition);
    return state.status === "active" && state.owner.id === owner && (!state.movement || state.movement.phase === "complete") && state;
  });
}

try {
  for (const id of ["catalog", "a", "🌻"]) await reserve(id);
  registry = Object.fromEntries([...nodes.values()].map(node => [node.id, [node.address]]));
  for (const node of nodes.values()) { await new Promise(resolve => node.reservation.close(resolve)); start(node); }
  for (const node of nodes.values()) {
    await until(`${node.id} health`, async () => (await fetch(node.url + "/health", { signal: AbortSignal.timeout(1000) })).ok);
    const initialized = await post(node, "/raft/initialize", { 1: node.address });
    assert.equal(initialized.response.status, 200, JSON.stringify(initialized.value));
  }
  await until("catalog leader", async () => { try { await catalog({ action: "list" }); return true; } catch { return false; } });
  assert.equal((await post(nodes.get("catalog"), "/admin/partitions/catalog", { action: "list" }, false)).response.status, 401);
  // A freshly initialized group may still be electing; registration retries until its leader serves.
  for (const id of ["a", "🌻"]) await until(`${id} registers after election`, async () => { await cluster().registerGroup({ id, addresses: registry[id] }); return true; });
  await assert.rejects(cluster().createPartition("x".repeat(10_000), "🌻", { requestId: "too-large-for-owner" }));
  assert.equal((await cluster().layout()).partitions.length, 0);
  for (const partition of ["tenant-a", "tenant-b"]) {
    await cluster().createPartition(partition, "a", { requestId: `create-${partition}` });
    await cluster().waitForPartition(partition, { timeoutMs: timeout });
    await active(partition, "a");
  }
  const entry = join(directory, "partitions.ts");
  await writeFile(entry, `
import { aggregate, collection, define, derive, mutation, query, transaction } from ${JSON.stringify(join(root, "sdk/index.ts"))};
import { scheduler } from ${JSON.stringify(join(root, "sdk/scheduler.ts"))};
import { queue } from ${JSON.stringify(join(root, "sdk/temporal.ts"))};
const rows=collection<{bucket:string,value:number}>("rows").index("bucket",["bucket"]);
const blobs=collection<string>("blobs");
const jobs=queue("jobs",{lease:{maxMs:60000}});
const total=aggregate("total",{source:rows,index:"bucket",initial:()=>0,add:(sum,row)=>sum+row.value,remove:(sum,row)=>sum-row.value});
const summary=derive("summary",ctx=>({total:ctx.get(total,"g"),count:ctx.query(rows.by("bucket").eq("g")).length}));
const fire=mutation("fire",ctx=>{ctx.set(rows,"timer",{bucket:"g",value:5});return null;});
const timers=scheduler("timers",{fire});
const seed=mutation("seed",(ctx,value:number)=>{ctx.set(rows,"shared",{bucket:"g",value});ctx.materialize(summary);jobs.enqueue(ctx,"same-job",{value});return ctx.get(summary);});
const blob=mutation("blob",(ctx,args:any)=>{ctx.set(blobs,args.key,args.value);return args.value.length;});
const schedule=mutation("schedule",(ctx,ms:number)=>timers.after(ctx,"same-timer",ms,"fire",null));
const add=mutation("add",(ctx,value:number)=>{const row=ctx.get(rows,"shared")!;ctx.set(rows,"shared",{...row,value:row.value+value});return ctx.get(summary);});
const claim=mutation("claim",(ctx,owner:string)=>jobs.claim(ctx,owner,{leaseMs:1000}));
const complete=mutation("complete",(ctx,args:any)=>jobs.complete(ctx,args,"done"));
const read=query("read",ctx=>({summary:ctx.get(summary),job:jobs.get(ctx,"same-job"),timers:timers.scan(ctx),blobBytes:ctx.scan(blobs).reduce((n,row)=>n+row.value.length,0)}));
const local=query("local",{consistency:"replica-local"},read.compute);
const tx=transaction("tx",()=>({calls:[]}));
export default define({uses:[jobs,timers],collections:[rows],definitions:[total,summary],http:{seed,blob,schedule,add,claim,complete,read,local,tx}});
`);
  const bundle = await buildBundle(entry, { initialization: "static" });
  for (const partition of ["tenant-a", "tenant-b"]) await cluster().partition(partition).deploy(bundle, { requestId: "same-deploy" });
  const seeded = await mutate("tenant-a", "seed", 10, "same-request");
  const other = await mutate("tenant-b", "seed", 100, "same-request");
  assert.deepEqual(seeded.value, { total: 10, count: 1 });
  assert.deepEqual(other.value, { total: 100, count: 1 });
  // The frozen image exceeds the configured per-RPC/command budget. Transfer
  // must resume through durable chunks instead of one oversized command.
  for (let index = 0; index < 5; index++) await mutate("tenant-a", "blob", { key: `blob-${index}`, value: "🌸".repeat(8192) });
  const oldClaim = (await mutate("tenant-a", "claim", "old-worker")).value;
  await mutate("tenant-a", "schedule", 10_000);
  const oldWatch = client("tenant-a", "a").watch("local", null, {
    signal: AbortSignal.any([watchController.signal, AbortSignal.timeout(timeout)]),
  });
  watches.add(oldWatch);
  assert.equal((await oldWatch.next()).value.value.summary.total, 10);
  // An impossible destination envelope is rejected before the source pauses.
  await assert.rejects(cluster().movePartition("tenant-a", "🌻", { requestId: "x".repeat(10_000) }));
  assert.equal((await placement("tenant-a")).status, "active");
  await cluster().movePartition("tenant-a", "🌻", { requestId: "move-a-to-b" });
  // Admission verifies both groups' budgets before the durable move decision.
  // The move first streams a base image to the destination in chunks while the
  // source keeps serving; pause the destination during that copy.
  nodes.get("🌻").runtime.child.kill("SIGSTOP");
  await delay(1_500); // Longer than an unpaused base copy of this image takes.
  assert.equal((await placement("tenant-a")).movement?.phase, "copying", "the base copy waits for the paused destination");
  assert.deepEqual((await client("tenant-a").query("read", null, { signal: AbortSignal.timeout(3000) })).value.summary,
    { total: 10, count: 1 }, "the source keeps serving until its brief final freeze");
  assert.deepEqual((await mutate("tenant-b", "add", 1)).value, { total: 101, count: 1 });
  // Losing the catalog coordinator must retain the move decision. Unrelated
  // tenant data remains independent; no source unfreeze/abort is permitted.
  await stop(nodes.get("catalog")); start(nodes.get("catalog"));
  await until("catalog restarted", async () => { try { return (await placement("tenant-a")).status === "moving"; } catch { return false; } });
  nodes.get("🌻").runtime.child.kill("SIGCONT");
  await assert.rejects((async () => {
    for await (const _ of oldWatch) { /* A buffered local value may precede the terminal ownership error. */ }
  })(), error => ["PARTITION_MOVING", "UNAVAILABLE"].includes(error.code),
  "an old owner watch must terminate after its ownership cache refresh");
  await oldWatch.return(); watches.delete(oldWatch);
  const moved = await active("tenant-a", "🌻");
  assert.equal(moved.epoch, 2);
  // Do not invoke this tenant before its timer is due: destination maintenance
  // must start after activation even without a query instantiating the App.
  await delay(11_000);
  const afterMove = (await client("tenant-a", "a").query("read")).value;
  assert.deepEqual(afterMove.summary, { total: 15, count: 2 });
  assert.equal(afterMove.blobBytes, 5 * 8192 * 2);
  assert.equal(afterMove.timers.length, 0);
  const reconnected = client("tenant-a", "a").watch("local", null, {
    signal: AbortSignal.any([watchController.signal, AbortSignal.timeout(timeout)]),
  });
  watches.add(reconnected);
  assert.deepEqual((await reconnected.next()).value.value.summary, { total: 15, count: 2 });
  await reconnected.return(); watches.delete(reconnected);
  const replay = await mutate("tenant-a", "seed", 10, "same-request");
  assert.equal(replay.duplicate, true); assert.equal(replay.revision, seeded.revision); assert.deepEqual(replay.value, seeded.value);
  const claim = (await mutate("tenant-a", "claim", "new-worker")).value;
  assert.ok(claim.token > oldClaim.token);
  await assert.rejects(mutate("tenant-a", "complete", { id: oldClaim.id, owner: oldClaim.owner, token: oldClaim.token }),
    error => error.status === 422 && error.failure?.code === "LEASE_LOST", "the moved fencing token rejects the stale worker");
  await mutate("tenant-a", "complete", { id: claim.id, owner: claim.owner, token: claim.token });
  // Logical partitions coordinate transactions too (see e2e-partition-transactions); an empty plan commits.
  const planned = await client("tenant-a").call("tx", null, { requestId: "empty-partition-tx" });
  assert.deepEqual(planned.value.results, []);
  const outageWatch = client("tenant-b", "a").watch("local", null, {
    signal: AbortSignal.any([watchController.signal, AbortSignal.timeout(timeout)]),
  });
  watches.add(outageWatch);
  assert.equal((await outageWatch.next()).value.value.summary.total, 101);
  await stop(nodes.get("catalog"));
  await assert.rejects((async () => { for await (const _ of outageWatch) {} })(),
    error => error.code === "UNAVAILABLE",
    "expired ownership cache must terminate a watch when the catalog is unavailable");
  await outageWatch.return(); watches.delete(outageWatch);
  start(nodes.get("catalog"));
  await until("catalog recovers after ownership TTL outage", async () => (await cluster().layout()).partitions.length === 2);
  await cluster().resize(["a"], { requestId: "shrink-to-a" });
  await active("tenant-a", "a");
  await until("shrink plan complete", async () => (await catalog({ action: "list" })).rebalance?.complete);
  assert.deepEqual((await client("tenant-a").query("read")).value.summary, { total: 15, count: 2 });
  assert.deepEqual((await client("tenant-b").query("read")).value.summary, { total: 101, count: 1 });
  await cluster().resize(["a", "🌻"], { requestId: "expand-again" });
  await until("expand plan complete", async () => (await catalog({ action: "list" })).rebalance?.complete);
  const placed = (await catalog({ action: "list" })).partitions;
  assert.deepEqual(placed.map(value => value.owner.id).sort(), ["a", "🌻"]);
  await Promise.all([...nodes.values()].map(stop));
  for (const node of nodes.values()) start(node);
  await until("all data survives full restart", async () => {
    const a = (await client("tenant-a").query("read")).value;
    const b = (await client("tenant-b").query("read")).value;
    return a.summary.total === 15 && b.summary.total === 101 && a.job.state === "completed";
  });
  const restartedReplay = await mutate("tenant-a", "seed", 10, "same-request");
  assert.equal(restartedReplay.duplicate, true); assert.equal(restartedReplay.revision, seeded.revision);
  console.log("PASS: Unicode physical group identity, native isolated partitions, durable chunked move, paused-tenant isolation, coordinator restart recovery, ownership-watch termination/reconnect/outage, exact-budget admission, receipts, timers without first read, lease fencing, derived/index state, shrink/expand and full-cluster restart");
} catch (error) {
  for (const node of nodes.values()) console.error(`--- ${node.id} ---\n${node.runtime?.logs ?? "not started"}`);
  throw error;
} finally {
  watchController.abort(new Error("Partition E2E finished"));
  await Promise.allSettled([...watches].map(watch => watch.return()));
  for (const node of nodes.values()) {
    await stop(node);
    if (node.reservation.listening) await new Promise(resolve => node.reservation.close(resolve));
  }
  await Promise.allSettled(children.map(runtime => runtime.exited));
  await rm(directory, { recursive: true, force: true });
}
