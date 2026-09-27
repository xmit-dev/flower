// Encrypted tenant catalogs move only after the destination proves it can unlock.
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

const directory = await mkdtemp(join(tmpdir(), "flower-key-migration-"));
const binary = resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower");
const token = randomUUID(), nodes = new Map(), runtimes = [];
const good = join(directory, "good.key"), wrong = join(directory, "wrong.key");
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
  nodes.set(id, { id, address, url:`http://${address}`, reservation, keyFile:id === "b" ? wrong : good });
}
function start(node) {
  const child = spawn(binary, ["--id","1","--listen",node.address,"--data",join(directory,node.id)], {
    env:{...process.env,FLOWER_ADMIN_TOKEN:token,FLOWER_GROUP:node.id,FLOWER_CATALOG_GROUP:"catalog",FLOWER_GROUPS:JSON.stringify(registry),
      FLOWER_KEYRING_FILE:node.keyFile,RUST_LOG:"flower=info,openraft=warn"},stdio:["ignore","pipe","pipe"],
  });
  const runtime = {child,logs:"",ended:false};
  child.stdout.on("data", bytes => { runtime.logs=(runtime.logs+bytes).slice(-60_000); });
  child.stderr.on("data", bytes => { runtime.logs=(runtime.logs+bytes).slice(-60_000); });
  runtime.exited=new Promise(done=>child.once("close",()=>{runtime.ended=true;done();}));
  node.runtime=runtime;runtimes.push(runtime);
}
async function stop(node) { if(node.runtime&&!node.runtime.ended){node.runtime.child.kill("SIGTERM");await node.runtime.exited;} }
async function post(node,path,body) {
  const response=await fetch(node.url+path,{method:"POST",headers:{"content-type":"application/json",authorization:`Bearer ${token}`},body:JSON.stringify(body),signal:AbortSignal.timeout(20_000)});
  const value=await response.json();assert.equal(response.status,200,JSON.stringify(value));return value;
}
try {
  await writeFile(good,randomBytes(32),{mode:0o600});await writeFile(wrong,randomBytes(32),{mode:0o600});
  for(const id of ["catalog","a","b"])await reserve(id);
  registry=Object.fromEntries([...nodes].map(([id,node])=>[id,[node.address]]));
  for(const node of nodes.values()){await new Promise(done=>node.reservation.close(done));start(node);}
  for(const node of nodes.values()){
    await until(`${node.id} starts`,async()=> (await fetch(node.url+"/health")).ok);
    await post(node,"/raft/initialize",{1:node.address});
  }
  const sdk=new FlowerAdmin(nodes.get("catalog").url,{adminToken:token});
  const app=new FlowerClient(nodes.get("catalog").url);
  await until("catalog leader",async()=>{await sdk.layout();return true;});
  for(const id of ["a","b"])await until(`${id} registers after election`,async()=>{await sdk.registerGroup({id,addresses:registry[id]});return true;});
  const entry=join(directory,"keys.ts");
  await writeFile(entry,`
import {define,key,derive,query,mutation,jwt,publicKey} from ${JSON.stringify(resolve("sdk/index.ts"))};
const sessions=key("sessions",{algorithm:"Ed25519",usages:["sign","verify","publicKey"]});
const fingerprint=derive("fingerprint",()=>Array.from(publicKey(sessions)));
const issue=mutation("issue",ctx=>{ctx.materialize(fingerprint,null);return{token:jwt.sign({sub:"tenant",exp:ctx.now()/1000+120},sessions),publicKey:ctx.get(fingerprint,null)};});
const check=query("check",{consistency:"replica-local"},(ctx,token:string)=>({claims:jwt.verify(token,sessions).claims,publicKey:ctx.get(fingerprint,null)}));
export default define({keys:[sessions],definitions:[fingerprint],http:{issue,check}});
`);
  const bundle=await buildBundle(entry);
  const tenant=sdk.partition("moving-tenant"),tenantApp=app.partition("moving-tenant");
  await sdk.createPartition("moving-tenant","a",{requestId:"create"});
  await sdk.waitForPartition("moving-tenant",{timeoutMs:45_000});
  const generated=await tenant.keyGenerate("signer","Ed25519",{requestId:"generate"});
  await tenant.keyBind("sessions","signer",["sign","verify","publicKey"],{requestId:"bind"});
  await tenant.deploy(bundle,{requestId:"deploy"});
  const issued=await tenantApp.mutate("issue",null,{requestId:"issue"});
  const before=await tenantApp.query("check",issued.value.token);
  await sdk.movePartition("moving-tenant","b",{requestId:"move"});
  await until("wrong KEK holds cutover",async()=>{
    const status=await sdk.partitionStatus("moving-tenant");
    return status.movement?.phase==="importing"&&/wrapping key/i.test(nodes.get("catalog").runtime.logs)&&status;
  });
  assert.equal((await sdk.partitionStatus("moving-tenant")).owner.id,"a","ownership must not cut over to a locked destination");
  await stop(nodes.get("b"));nodes.get("b").keyFile=good;start(nodes.get("b"));
  await until("unlocked destination activates",async()=>{
    const status=await sdk.partitionStatus("moving-tenant");return status.status==="active"&&status.owner.id==="b"&&status.movement?.phase==="complete";
  });
  const after=await tenantApp.query("check",issued.value.token);
  assert.deepEqual(after.value,before.value,"key identity and materialized values survive encrypted transfer");
  assert.deepEqual(await tenant.keyGenerate("signer","Ed25519",{requestId:"generate"}),{...generated,duplicate:true},"key provisioning receipts move with tenant");
  assert.deepEqual(await tenantApp.mutate("issue",null,{requestId:"issue"}),{...issued,duplicate:true});
  await sdk.createPartition("other-tenant","a",{requestId:"create-other"});await sdk.waitForPartition("other-tenant",{timeoutMs:45_000});
  const other=sdk.partition("other-tenant");
  await other.keyGenerate("signer","Ed25519",{requestId:"generate"});await other.keyBind("sessions","signer",["sign","verify","publicKey"],{requestId:"bind"});await other.deploy(bundle,{requestId:"deploy"});
  await assert.rejects(app.partition("other-tenant").query("check",issued.value.token),
    error=>error.status===422&&/key|version|bound|signature/i.test(error.failure?.message??""),"same alias in another tenant must not authorize the first tenant's key");
  console.log("managed migration E2E passed: wrong KEK blocks cutover, restart unlocks staged transfer, encrypted catalogs/materialized values/retry receipts survive, tenant scopes remain isolated");
} catch(error){for(const [id,node]of nodes)console.error(`${id}: ${node.runtime?.logs.slice(-6_000)??"not started"}`);throw error;}
finally {for(const node of nodes.values()){await stop(node);if(node.reservation.listening)await new Promise(done=>node.reservation.close(done));}await Promise.allSettled(runtimes.map(runtime=>runtime.exited));await rm(directory,{recursive:true,force:true});}
