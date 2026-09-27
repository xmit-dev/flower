// Disposable real three-node TLS cluster with independent operator/peer tokens.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { request as httpsRequest } from "node:https";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

const directory=await mkdtemp(join(tmpdir(),"flower-tls-e2e-"));
const fixture=resolve("tests/fixtures/tls");
const ca=await readFile(join(fixture,"ca.crt"));
const binary=resolve(process.env.E2E_FLOWER_BIN??"target/debug/flower");
const operator=randomUUID(), peer=randomUUID();
const children=[],nodes=[];
const transport=createHttp2Transport({ca,requestTimeoutMs:20_000});
async function reserve(){const server=createServer();await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));return {server,port:server.address().port};}
async function http(node,path,token=operator,body){return new Promise((resolve,reject)=>{
  const req=httpsRequest(node.url+path,{ca,method:body===undefined?"GET":"POST",headers:{authorization:`Bearer ${token}`,"content-type":"application/json"},timeout:2000},response=>{
    let text="";response.on("data",chunk=>text+=chunk);response.on("end",()=>{try{resolve({status:response.statusCode,value:JSON.parse(text)});}catch(error){reject(error);}});
  });req.on("error",reject);req.on("timeout",()=>req.destroy(new Error("request timeout")));req.end(body===undefined?undefined:JSON.stringify(body));
});}
async function until(run,timeout=30_000){const end=Date.now()+timeout;let error;while(Date.now()<end){try{const result=await run();if(result)return result;}catch(failure){error=failure;}await delay(50);}throw error??new Error("cluster condition timed out");}
try {
  for(let id=1;id<=3;id++){
    const held=await reserve();const port=held.port;await new Promise(resolve=>held.server.close(resolve));
    const node={id,address:`localhost:${port}`,url:`https://localhost:${port}`,logs:""};nodes.push(node);
    const child=spawn(binary,["--id",String(id),"--listen",`127.0.0.1:${port}`,"--advertise",node.address,"--data",join(directory,String(id))],{
      env:{...process.env,FLOWER_ADMIN_TOKEN:operator,FLOWER_PEER_TOKEN:peer,FLOWER_TLS_CERT_FILE:join(fixture,"localhost.crt"),FLOWER_TLS_KEY_FILE:join(fixture,"localhost.key"),FLOWER_TLS_CA_FILE:join(fixture,"ca.crt"),FLOWER_SHUTDOWN_TIMEOUT_MS:"500",RUST_LOG:"flower=warn,openraft=error"},stdio:["ignore","pipe","pipe"],
    });children.push(child);node.child=child;for(const source of [child.stdout,child.stderr])source.on("data",chunk=>node.logs=(node.logs+chunk).slice(-12000));
  }
  await until(async()=>{const responses=await Promise.all(nodes.map(node=>http(node,"/raft/metrics")));return responses.every(response=>response.status===200);});
  assert.equal((await http(nodes[0],"/raft/initialize",operator,Object.fromEntries(nodes.map(node=>[node.id,node.address])))).status,200);
  const leader=await until(async()=>{for(const node of nodes){if((await http(node,"/raft/metrics")).value.state==="Leader")return node;}});
  for(const node of nodes){assert.equal((await http(node,"/raft/membership",peer)).status,401);}
  const follower=nodes.find(node=>node!==leader);
  const client=new FlowerClient(follower.url,{fetch:transport.fetch,queryUrls:nodes.map(node=>node.url)});
  const admin=new FlowerAdmin(follower.url,{adminToken:operator,fetch:transport.fetch});
  const javascript=`const records={kind:'collection',name:'records'};var __flowerBundle={default:{definitions:{
    read:{kind:'queryMethod',name:'read',compute:ctx=>({count:ctx.get(records,'count')||0,padding:'x'.repeat(400)})},
    write:{kind:'mutationMethod',name:'write',compute:(ctx,args)=>{const n=(ctx.get(records,'count')||0)+args;ctx.set(records,'count',n);return n;}}
  },http:{read:{kind:'query',name:'read'},write:{kind:'mutation',name:'write'}}}};`;
  await admin.deploy({hash:createHash("sha256").update(javascript).digest("hex"),javascript},{requestId:randomUUID()});
  const watches=[client.watch("read"),client.watch("read")];
  for(const watch of watches)assert.equal((await watch.next()).value.value.count,0);
  const requestId=randomUUID();
  const result=await client.call("write",7,{requestId});assert.equal(result.value,7);
  for(const watch of watches){assert.equal((await watch.next()).value.value.count,7);await watch.return();}
  const retry=await client.call("write",7,{requestId});assert.equal(retry.duplicate,true);assert.equal(retry.value,7);
  for(const node of nodes){const other=new FlowerClient(node.url,{fetch:transport.fetch});assert.equal((await other.query("read")).value.count,7);}
  // Revoke one peer credential on a restarted follower would require a cluster
  // rollout; this test instead proves privilege separation at exposed routes.
  const forbidden=new FlowerAdmin(follower.url,{adminToken:peer,fetch:transport.fetch});
  await assert.rejects(forbidden.deploy({hash:createHash("sha256").update(javascript).digest("hex"),javascript},{requestId:randomUUID()}),error=>error.status===401);
  console.log("PASS: three-node native TLS/h2 quorum, follower forwarding, independent peer/operator tokens, SDK pooling, shared SSE and retry receipts");
} catch(error){for(const node of nodes)console.error(`node ${node.id}: ${node.logs}`);throw error;}
finally {
  await transport.close();
  await Promise.all(children.map(child=>new Promise(resolve=>{if(child.exitCode!==null||child.signalCode!==null)return resolve();child.once("exit",resolve);child.kill("SIGTERM");const timer=setTimeout(()=>child.kill("SIGKILL"),2000);timer.unref();})));
  await rm(directory,{recursive:true,force:true});
}
