// Run after cargo build: node tests/e2e-indexes.mjs
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/index.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
process.env.FLOWER_SNAPSHOT_AFTER_LOGS = "8";
process.env.FLOWER_SNAPSHOT_LAG_LOGS = "16";
process.env.FLOWER_SNAPSHOT_KEEP_LOGS = "0";
const cluster = new LocalCluster({ nodes: 3, binary: process.env.E2E_FLOWER_BIN ?? join(root, "target/debug/flower") });
const transport = createHttp2Transport({ requestTimeoutMs: 15_000 });
const client = (node = cluster.leader) => new FlowerClient(node.url, { fetch: transport.fetch });
const admin = (node = cluster.leader) => new FlowerAdmin(node.url, { adminToken: cluster.adminToken, fetch: transport.fetch });
let sequence = 0;
const mutate = (name, args) => client().mutate(name, args, { requestId: `index-${++sequence}`, signal: AbortSignal.timeout(20_000) });

function source(multiplier = 1) {
  return `import { aggregate, collection, define, derive, mutation, query } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const orders = collection<{shop:string,cents:number}>("orders").index("shop", ["shop"]);
const total = aggregate("total", { source: orders, index: "shop", initial: () => 0,
  add: (sum, row) => sum + row.cents * ${multiplier}, remove: (sum, row) => sum - row.cents * ${multiplier} });
const summary = derive("summary", (ctx, shop: string) => ({ total: ctx.get(total, shop), count: ctx.query(orders.by("shop").eq(shop)).length }));
const seed = mutation("seed", ctx => { for(let i=0;i<100;i++)ctx.set(orders,String(i),{shop:"a",cents:10}); ctx.materialize(summary,"a"); ctx.materialize(summary,"b"); return ctx.get(summary,"a"); });
const change = mutation("change", (ctx, args: any) => { if(args.value)ctx.set(orders,args.key,args.value); else ctx.delete(orders,args.key); return {a:ctx.get(summary,"a"),b:ctx.get(summary,"b")}; });
const read = query("read", (ctx, shop:string) => ctx.get(summary,shop));
export default define({collections:[orders],definitions:[total,summary],http:{seed,change,read}});`;
}
async function verify(a, b) {
  for (const node of cluster.members) {
    assert.deepEqual((await client(node).query("read", "a")).value, a);
    assert.deepEqual((await client(node).query("read", "b")).value, b);
  }
}
async function kill(node) {
  node.process.intentional = true;
  node.process.child.kill("SIGKILL");
  await node.process.exited;
}
try {
  await cluster.start();
  const fixture = join(cluster.directory, "indexes.ts");
  await writeFile(fixture, source());
  await admin().deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "index-deploy" });
  assert.deepEqual((await mutate("seed")).value, { total: 1000, count: 100 });
  for (let i = 1; i <= 16; i++) await mutate("change", { key: "0", value: { shop: "a", cents: i } });
  await verify({total:1006,count:100},{total:0,count:0});
  // Wait until each node has durably checkpointed the index entries and
  // accumulator in redb before stopping every process simultaneously.
  await cluster._until("all replicas durably checkpoint their index state", async () => {
    const metrics = await Promise.all(cluster.members.map(node => cluster.metrics(node)));
    return metrics.every(metric => (metric.snapshot?.index ?? 0) >= 8);
  });
  await Promise.all(cluster.members.map(kill));
  cluster.leader = null;
  for (const node of cluster.members) cluster._startNode(node);
  await cluster.discoverLeader();
  await verify({total:1006,count:100},{total:0,count:0});
  assert.deepEqual((await mutate("change", {key:"0",value:{shop:"b",cents:16}})).value,
    {a:{total:990,count:99},b:{total:16,count:1}});
  await mutate("change", {key:"1"});
  await verify({total:980,count:98},{total:16,count:1});
  await cluster.crashLeaderAndRecover();
  await mutate("change", {key:"new",value:{shop:"b",cents:4}});
  await verify({total:980,count:98},{total:20,count:2});
  // A code deployment rebuilds the accumulator, including previously persisted
  // materializations, instead of combining a new reducer with old arithmetic.
  await writeFile(fixture, source(2));
  await admin().deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "index-redeploy" });
  await verify({total:1960,count:98},{total:40,count:2});
  console.log("PASS: durable equality indexes, delta aggregates, fresh replica reads, full-cluster redb checkpoint restart, leader recovery, row moves/deletes, and reducer redeployment");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await transport.close();
  await cluster.close();
}
