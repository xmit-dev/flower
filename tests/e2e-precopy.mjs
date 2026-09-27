// Live partition copy: writes during transfer, source restart, and final receipt/deletion catch-up.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";
import { buildBundle } from "../sdk/bundle.ts";

const directory = await mkdtemp(join(tmpdir(), "flower-precopy-"));
const binary = resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower");
const token = randomUUID(), nodes = new Map(), runtimes = [];
let registry;
async function until(label, operation) {
  const end = Date.now() + 45_000;
  let failure;
  while (Date.now() < end) {
    try { const result = await operation(); if (result) return result; } catch (error) { failure = error; }
    await delay(75);
  }
  throw new Error(`${label}: ${failure?.message ?? "timed out"}`, { cause: failure });
}
async function reserve(id) {
  const reservation = createServer();
  await new Promise((done, fail) => { reservation.once("error", fail); reservation.listen(0, "127.0.0.1", done); });
  const address = `127.0.0.1:${reservation.address().port}`;
  nodes.set(id, { id, address, url:`http://${address}`, reservation });
}
function start(node) {
  const child = spawn(binary, ["--id","1","--listen",node.address,"--data",join(directory,node.id)], {
    env:{...process.env,FLOWER_ADMIN_TOKEN:token,FLOWER_PEER_TOKEN:token,FLOWER_GROUP:node.id,FLOWER_CATALOG_GROUP:"catalog",FLOWER_GROUPS:JSON.stringify(registry),
      ...(node.id==="b"?{FLOWER_TRANSACTION_MAX_BYTES:"65536",FLOWER_RPC_MAX_BYTES:"262144",FLOWER_SNAPSHOT_CHUNK_BYTES:"16384"}:{}),RUST_LOG:"flower=info,openraft=warn"},stdio:["ignore","pipe","pipe"],
  });
  const runtime = {child,logs:"",ended:false};
  child.stdout.on("data", bytes => { runtime.logs=(runtime.logs+bytes).slice(-60_000); });
  child.stderr.on("data", bytes => { runtime.logs=(runtime.logs+bytes).slice(-60_000); });
  runtime.exited=new Promise(done=>child.once("close",()=>{runtime.ended=true;done();}));
  node.runtime=runtime;runtimes.push(runtime);
}
async function stop(node) { if(node.runtime&&!node.runtime.ended){node.runtime.child.kill("SIGTERM");const timer=setTimeout(()=>node.runtime.child.kill("SIGKILL"),3000);try{await node.runtime.exited;}finally{clearTimeout(timer);}} }
async function post(node,path,body) {
  const response=await fetch(node.url+path,{method:"POST",headers:{"content-type":"application/json",authorization:`Bearer ${token}`},body:JSON.stringify(body),signal:AbortSignal.timeout(20_000)});
  const value=await response.json();assert.equal(response.status,200,JSON.stringify(value));return value;
}
let contract;
async function nativeInfo(node) {
  const response=await fetch(node.url+"/raft/partitions/control",{method:"POST",headers:{"content-type":"application/json",authorization:`Bearer ${token}`,"x-flower-compatibility":contract},body:JSON.stringify({group:node.id,body:{action:"info",partition:"shop"}}),signal:AbortSignal.timeout(10_000)});
  const value=await response.json();assert.equal(response.status,200,JSON.stringify(value));return value.body;
}
try {
  for(const id of ["catalog","a","b"])await reserve(id);
  registry=Object.fromEntries([...nodes].map(([id,node])=>[id,[node.address]]));
  for(const node of nodes.values()){await new Promise(done=>node.reservation.close(done));start(node);}
  for(const node of nodes.values()){
    await until(`${node.id} starts`,async()=> (await fetch(node.url+"/health")).ok);
    await post(node,"/raft/initialize",{1:node.address});
  }
  const sdk=new FlowerAdmin(nodes.get("catalog").url,{adminToken:token});
  await until("catalog leader",async()=>{await sdk.layout();return true;});
  const version=await fetch(nodes.get("a").url+"/raft/version",{headers:{authorization:`Bearer ${token}`,"x-flower-target-node-id":"1"}}).then(response=>response.json());
  const c=version.compatibility;contract=`raft${c.raftWire}-state${c.stateMachine}-snapshot${c.snapshotFormat}-value${c.valueFormat}-qjs${c.quickjsSha256}`;
  for(const id of ["catalog","a","b"])await until(`${id} registers`,async()=>{await sdk.registerGroup({id,addresses:registry[id]});return true;});
  const entry=join(directory,"app.ts");
  await writeFile(entry,`
import {collection,define,query,mutation} from ${JSON.stringify(resolve("sdk/index.ts"))};
const rows=collection("rows");
export default define({auth:{authenticate:(_ctx,credentials:any,request)=>credentials?.subject==="tester"?{subject:"tester",...(request.partition?{tenant:request.partition}:{})}:null},http:{
  fill:mutation("fill",ctx=>{const value="🌸".repeat(2048);for(let i=0;i<256;i++)ctx.set(rows,String(i),value);ctx.set(rows,"obsolete","remove during copy");return 256;}),
  bump:mutation("bump",ctx=>{const n=(ctx.get(rows,"n")??0)+1;ctx.set(rows,"n",n);return n;}),
  erase:mutation("erase",ctx=>{ctx.delete(rows,"obsolete");return true;}),
  summary:query("summary",ctx=>({n:ctx.get(rows,"n")??0,first:ctx.get(rows,"0")?.length,last:ctx.get(rows,"255")?.length,obsolete:ctx.get(rows,"obsolete")})),
}});
`);
  await sdk.createPartition("shop","a",{requestId:"create-shop"});await sdk.waitForPartition("shop",{timeoutMs:45_000});
  const admin=sdk.partition("shop");await admin.deploy(await buildBundle(entry),{requestId:"deploy"});
  const tester=new FlowerClient(nodes.get("catalog").url,{credentials:{subject:"tester"}}).partition("shop");
  assert.equal((await tester.mutate("fill",null,{requestId:"fill"})).value,256);
  const incarnation=randomBytes(16).toString("hex");
  let status=await admin.retentionStatus();
  await admin.controlRetention(status.revision,{operation:"initialize",database:randomBytes(16).toString("hex"),incarnation,max_receipt_bytes:null});
  const client=new FlowerClient(nodes.get("catalog").url,{credentials:{subject:"tester"},boundedRetries:true}).partition("shop");
  const expired=await client.newRequestId("before-copy");assert.equal((await client.mutate("bump",null,{requestId:expired})).value,1);
  await sdk.movePartition("shop","b",{requestId:"precopy-shop"});
  nodes.get("b").runtime.child.kill("SIGSTOP");
  const captured=await until("durable base captured while destination paused",async()=>{const info=await nativeInfo(nodes.get("a"));return info.base_bytes>0&&info.phase==="active"?info:false;});
  assert.ok(captured.base_bytes>2*1024*1024);
  assert.equal((await sdk.partitionStatus("shop")).movement.phase,"copying");
  await until("cached base is charged to shared control budget",async()=>{
    const metrics=await fetch(nodes.get("a").url+"/admin/resources",{headers:{authorization:`Bearer ${token}`}}).then(response=>response.json());
    return metrics.classes.find(item=>item.class==="control").retainedInputBytes>=captured.base_bytes;
  });
  console.log("pre-copy: retained base",captured.base_bytes,"bytes; writing with destination paused");
  status=await admin.retentionStatus();await admin.controlRetention(status.revision,{operation:"advance",incarnation,current_epoch:1,min_epoch:1});
  do {status=await admin.retentionStatus();const gc=await admin.controlRetention(status.revision,{operation:"collect",incarnation,limit:1000});if(gc.value.state.gcComplete)break;}while(true);
  await client.refreshRetryIdentity();
  const calls=await Promise.all(Array.from({length:48},async(_,index)=>{const id=await client.newRequestId(`during-copy-${index}`);const result=await client.mutate("bump",null,{requestId:id});return{id,result};}));
  assert.equal(Math.max(...calls.map(call=>call.result.value)),49);
  await client.mutate("erase");assert.equal((await client.query("summary")).value.n,49);
  assert.equal((await sdk.partitionStatus("shop")).movement.phase,"copying");
  console.log("pre-copy: source restart with retained base and 48 committed concurrent writes");
  await stop(nodes.get("a"));start(nodes.get("a"));
  await until("source recovers durable live and base state",async()=>{const info=await nativeInfo(nodes.get("a"));return info.base_bytes===captured.base_bytes&&(await client.query("summary")).value.n===49;});
  const last=calls.at(-1);assert.deepEqual(await client.mutate("bump",null,{requestId:last.id}),{...last.result,duplicate:true});
  nodes.get("b").runtime.child.kill("SIGCONT");
  await sdk.waitForPartition("shop",{timeoutMs:60_000});
  await until("source retired and base released",async()=>{const info=await nativeInfo(nodes.get("a"));return info.phase==="retired"&&info.base_bytes===0;});
  await until("retirement releases cached export quota",async()=>{
    const metrics=await fetch(nodes.get("a").url+"/admin/resources",{headers:{authorization:`Bearer ${token}`}}).then(response=>response.json());
    return metrics.classes.find(item=>item.class==="control").retainedInputBytes===0;
  });
  assert.equal((await sdk.partitionStatus("shop")).owner.id,"b");
  const summary=(await client.query("summary")).value;
  assert.equal(summary.n,49);assert.equal(summary.first,4096);assert.equal(summary.last,4096);assert.equal(summary.obsolete,null);
  assert.deepEqual(await client.mutate("bump",null,{requestId:last.id}),{...last.result,duplicate:true});
  await assert.rejects(client.mutate("bump",null,{requestId:expired}),{code:"RETRY_WINDOW_EXPIRED"});
  for(const node of nodes.values())await stop(node);for(const node of nodes.values())start(node);
  await until("all-group restart retains final state",async()=> (await client.query("summary")).value.n===49);
  assert.deepEqual(await client.mutate("bump",null,{requestId:last.id}),{...last.result,duplicate:true});
  console.log("pre-copy E2E: >2MiB live base, concurrent writes/receipt GC/deletion, source restart before freeze, final delta, retirement and full restart passed");
} catch(error){for(const [id,node]of nodes)console.error(`${id}: ${node.runtime?.logs.slice(-6_000)??"not started"}`);throw error;}
finally {for(const node of nodes.values())node.runtime?.child.kill("SIGCONT");for(const node of nodes.values()){await stop(node);if(node.reservation.listening)await new Promise(done=>node.reservation.close(done));}await Promise.allSettled(runtimes.map(runtime=>runtime.exited));await rm(directory,{recursive:true,force:true});}
