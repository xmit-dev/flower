// Logical-partition 2PC: placement pins, delegation, narrow barriers and movement.
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

const directory = await mkdtemp(join(tmpdir(), "flower-partition-tx-"));
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
    env:{...process.env,FLOWER_ADMIN_TOKEN:token,FLOWER_GROUP:node.id,FLOWER_CATALOG_GROUP:"catalog",FLOWER_GROUPS:JSON.stringify(registry),
      RUST_LOG:"flower=info,openraft=warn"},stdio:["ignore","pipe","pipe"],
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
try {
  for(const id of ["catalog","a","b"])await reserve(id);
  registry=Object.fromEntries([...nodes].map(([id,node])=>[id,[node.address]]));
  for(const node of nodes.values()){await new Promise(done=>node.reservation.close(done));start(node);}
  for(const node of nodes.values()){
    await until(`${node.id} starts`,async()=> (await fetch(node.url+"/health")).ok);
    await post(node,"/raft/initialize",{1:node.address});
  }
  const admin=new FlowerAdmin(nodes.get("catalog").url,{adminToken:token});
  const sdk=new FlowerClient(nodes.get("catalog").url,{credentials:{subject:"banker"}});
  await until("catalog leader",async()=>{await admin.layout();return true;});
  for(const id of ["catalog","a","b"])await until(`${id} registers after election`,async()=>{await admin.registerGroup({id,addresses:registry[id]});return true;});
  const entry=join(directory,"app.ts");
  await writeFile(entry,`
import {collection,define,query,mutation,transaction} from ${JSON.stringify(resolve("sdk/index.ts"))};
const rows=collection<number>("rows");
const add=mutation("add",(ctx,n:number)=>{const next=(ctx.get(rows,"n")??0)+n;ctx.set(rows,"n",next);return next;});
const read=query("read",ctx=>ctx.get(rows,"n")??0);
const failing=mutation("fail",()=>{throw Error("planned failure");});
const run=transaction("run",(plan:any)=>plan);
export default define({
  auth:{
    authenticate:(_ctx,credentials:any,request)=>credentials?.subject==="banker"?{subject:"banker",...(request.partition?{tenant:request.partition}:{})}:null,
    // Participants accept only the banker's work delegated by a coordinator.
    delegation:(_ctx,_coordinator,principal)=>principal?.subject==="banker",
  },
  http:{add,read,fail:failing,run},
});
`);
  const bundle=await buildBundle(entry);
  const clients={},admins={};
  for(const id of ["coordinator","alice","bob","bystander"]){
    await admin.createPartition(id,"a",{requestId:`create-${id}`});
    await admin.waitForPartition(id,{timeoutMs:45_000});
    clients[id]=sdk.partition(id);
    admins[id]=admin.partition(id);
    await admins[id].deploy(bundle,{requestId:`deploy-${id}`});
  }
  const abortPlan={calls:[{partition:"alice",method:"add",args:100},{partition:"bob",method:"fail"}]};
  // An uncoded exception in a participant reaches the caller as its failure.
  const aborted=(error)=>error.status===422&&error.code==="TRANSACTION_ABORTED"&&
    JSON.stringify(error.failure)===JSON.stringify({code:"COMPUTE_ERROR",message:"planned failure"});
  const plan={calls:[{partition:"alice",method:"add",args:1},{partition:"bob",method:"add",args:10},{partition:"alice",method:"read"}],value:"same physical group"};
  console.log("logical TX: same-group commit");
  const receipt=await clients.coordinator.call("run",plan,{requestId:"same-group"});
  assert.deepEqual(receipt.value,{results:[1,10,1],value:"same physical group"});
  await assert.rejects(clients.coordinator.call("run",abortPlan,{requestId:"abort"}),aborted);
  assert.equal((await clients.alice.query("read")).value,1);
  console.log("logical TX: move bob");
  await admin.movePartition("bob","b",{requestId:"move-bob"});
  await admin.waitForPartition("bob",{timeoutMs:45_000});
  assert.equal((await clients.bob.query("read")).value,10);
  const next=await clients.coordinator.call("run",plan,{requestId:"cross-group"});
  assert.deepEqual(next.value,{results:[2,20,2],value:"same physical group"});
  assert.equal((await clients.coordinator.call("run",plan,{requestId:"same-group"})).duplicate,true);
  // The authorization hook's failure arrives as 403 FORBIDDEN with its own failure.
  await assert.rejects(new FlowerClient(nodes.get("catalog").url).partition("coordinator").call("run",plan,{requestId:"same-group"}),
    error=>error.status===403&&error.code==="FORBIDDEN"&&JSON.stringify(error.failure)===JSON.stringify({code:"UNAUTHENTICATED",message:"Authentication required"}));
  // A prepared Alice must not block an unrelated tenant on its physical group.
  console.log("logical TX: pause b, prepare alice");
  nodes.get("b").runtime.child.kill("SIGSTOP");
  const pending=clients.coordinator.call("run",plan,{requestId:"held-participant"}).then(value=>({value}),error=>({error}));
  await until("Alice is prepared",async()=>{
    try{await clients.alice.query("read");return false;}catch(error){return error.code==="TRANSACTION_PREPARED";}
  });
  console.log("logical TX: bystander and prepared move");
  assert.equal((await clients.bystander.mutate("add",7)).value,7);
  await admin.movePartition("alice","catalog",{requestId:"move-prepared-alice"});
  assert.equal((await admin.partitionStatus("alice")).owner.id,"a");
  nodes.get("b").runtime.child.kill("SIGCONT");
  console.log("logical TX: resume b");
  console.log("logical TX: pending outcome",await pending);
  await admin.waitForPartition("alice",{timeoutMs:60_000});
  assert.equal((await admin.partitionStatus("alice")).owner.id,"catalog");
  // Completed coordinator state can move, including abort identity and receipts.
  console.log("logical TX: move coordinator");
  await admin.movePartition("coordinator","b",{requestId:"move-coordinator"});
  await admin.waitForPartition("coordinator",{timeoutMs:60_000});
  assert.deepEqual(await clients.coordinator.call("run",plan,{requestId:"same-group"}),{...receipt,duplicate:true});
  await assert.rejects(clients.coordinator.call("run",abortPlan,{requestId:"abort"}),aborted,"the moved abort decision keeps its participant failure");
  for(const node of nodes.values())await stop(node);
  for(const node of nodes.values())start(node);
  await until("coordinator replay after full restart",async()=> (await clients.coordinator.call("run",plan,{requestId:"same-group"})).duplicate);
  assert.equal((await clients.bystander.query("read")).value,7);
  console.log("logical TX: close migrated history and collect detail");
  const firstClosure=await admins.coordinator.controlTransactionClosure({operation:"close",through:1});
  assert.equal(firstClosure.value.closedThrough,1);
  assert.equal(firstClosure.value.pending,null);
  assert.ok((await admins.coordinator.controlTransactionClosure({operation:"collect"})).value.deletedRecords>=2);
  assert.deepEqual(await clients.coordinator.call("run",plan,{requestId:"same-group"}),{...receipt,duplicate:true},"closure preserves committed client receipts");
  const blocked=await admins.coordinator.controlTransactionClosure({operation:"close"});
  assert.equal(blocked.value.closedThrough,1);
  assert.match(blocked.value.blockedReason,/aborted.*admissible/);
  const retention=await admins.coordinator.retentionStatus();
  const database=randomBytes(16).toString("hex"),incarnation=randomBytes(16).toString("hex");
  await admins.coordinator.controlRetention(retention.revision,{operation:"initialize",database,incarnation,max_receipt_bytes:null});
  const closed=await until("entire transaction prefix closes after retry retirement",async()=>{
    const result=await admins.coordinator.controlTransactionClosure({operation:"close"});
    return result.value.closedThrough===result.value.nextSequence-1&&result.value.pending===null?result:false;
  });
  assert.ok((await admins.coordinator.controlTransactionClosure({operation:"collect"})).value.deletedRecords>=2);
  for(const participant of [admins.alice,admins.bob]) {
    const collected=await participant.controlTransactionClosure({operation:"collect"});
    assert.ok(collected.value.deletedRecords>0,"participant tombstones collect only behind the closure floor");
  }
  await assert.rejects(clients.coordinator.call("run",plan,{requestId:"same-group"}),{code:"REQUEST_ID_SCOPE_REQUIRED"});
  const requestId=await clients.coordinator.newRequestId("post-closure");
  await clients.coordinator.call("run",{calls:[],value:"new history work"},{requestId});
  const empty=await admins.coordinator.controlTransactionClosure({operation:"close"});
  assert.equal(empty.value.closedThrough,closed.value.closedThrough+1,"empty participant plans close too");
  for(const node of nodes.values())await stop(node);
  for(const node of nodes.values())start(node);
  const restored=await until("closure floor after full restart",async()=>{
    const result=await admins.coordinator.transactionClosureStatus();
    return result.value.closedThrough===empty.value.closedThrough?result:false;
  });
  assert.equal(restored.value.history,closed.value.history);
  assert.equal((await clients.coordinator.call("run",{calls:[],value:"new history work"},{requestId})).duplicate,true);
  console.log("logical transactions E2E: commit/rollback, delegation, isolated barriers, prepared/coordinator moves, durable closure/GC, retained receipts, aborted retry fencing and restart passed");
} catch(error){for(const [id,node]of nodes)console.error(`${id}: ${node.runtime?.logs.slice(-6_000)??"not started"}`);throw error;}
finally {for(const node of nodes.values())node.runtime?.child.kill("SIGCONT");for(const node of nodes.values()){await stop(node);if(node.reservation.listening)await new Promise(done=>node.reservation.close(done));}await Promise.allSettled(runtimes.map(runtime=>runtime.exited));await rm(directory,{recursive:true,force:true});}
