import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { mkdtemp,writeFile,rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join,resolve } from "node:path";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";

const directory=await mkdtemp(join(tmpdir(),"flower-retention-e2e-"));
const cluster=new LocalCluster({nodes:3,binary:resolve(process.env.E2E_FLOWER_BIN??"target/debug/flower")});
try {
  const entry=join(directory,"app.ts");
  await writeFile(entry,`
import {collection,define,fail,mutation,query} from ${JSON.stringify(resolve("sdk/index.ts"))};
const records=collection<number>("records");
export default define({auth:{authenticate:(_ctx,credentials:any)=>{
  if(credentials?.subject==="suspended")fail("ACCOUNT_SUSPENDED","This account is suspended",{subject:"suspended",until:42});
  return credentials?.subject ? {subject:credentials.subject} : null;
}},http:{
  add:mutation("add",ctx=>{const n=(ctx.get(records,"n")??0)+1;ctx.set(records,"n",n);return n;}),
  read:query("read",ctx=>ctx.get(records,"n")),
  history:query("history",ctx=>ctx.history()),
}});
`);
  await cluster.start();
  const follower=cluster.members.find(member=>member.id!==cluster.leader.id).url;
  const admin=new FlowerAdmin(follower,{adminToken:cluster.adminToken});
  await admin.deploy(await buildBundle(entry));
  const initial=await admin.retentionStatus();
  const database=randomBytes(16).toString("hex"),incarnation=randomBytes(16).toString("hex");
  await admin.controlRetention(initial.revision,{operation:"initialize",database,incarnation,max_receipt_bytes:1024*1024});
  const client=new FlowerClient(follower,{boundedRetries:true,credentials:{subject:"alice"}});
  assert.deepEqual((await client.query("history")).value,{database,incarnation});
  // The authorization hook's own failures arrive as 403 FORBIDDEN with their structured failure.
  const hookFailure=(failure)=>(error)=>error.status===403&&error.code==="FORBIDDEN"&&JSON.stringify(error.failure)===JSON.stringify(failure);
  await assert.rejects(new FlowerClient(follower,{credentials:{subject:"suspended"}}).query("read"),
    hookFailure({code:"ACCOUNT_SUSPENDED",message:"This account is suspended",details:{subject:"suspended",until:42}}));
  await assert.rejects(new FlowerClient(follower,{credentials:{subject:"suspended"}}).mutate("add",null,{requestId:"suspended-add"}),
    hookFailure({code:"ACCOUNT_SUSPENDED",message:"This account is suspended",details:{subject:"suspended",until:42}}));
  await assert.rejects(new FlowerClient(follower).query("read"),hookFailure({code:"UNAUTHENTICATED",message:"Authentication required"}));
  const id=await client.newRequestId("business-one");
  const receipt=await client.mutate("add",null,{requestId:id});
  assert.equal(receipt.value,1);
  assert.deepEqual(await client.mutate("add",null,{requestId:id}),{...receipt,duplicate:true});
  // A caller's own ID is admitted too, and deduplicated until the floor passes the epoch it ran in.
  const own=await client.mutate("add",null,{requestId:"own-key"});
  assert.equal(own.value,2);
  assert.deepEqual(await client.mutate("add",null,{requestId:"own-key"}),{...own,duplicate:true});
  let status=await admin.retentionStatus();
  await admin.controlRetention(status.revision,{operation:"advance",incarnation,current_epoch:1,min_epoch:1});
  await assert.rejects(client.mutate("add",null,{requestId:id}),{code:"RETRY_WINDOW_EXPIRED"});
  let collected=0;
  do {
    status=await admin.retentionStatus();
    const gc=await admin.controlRetention(status.revision,{operation:"collect",incarnation,limit:1});
    collected+=gc.value.collected;
    if(gc.value.state.gcComplete)break;
  } while(true);
  assert.equal(collected,3,"the scoped ID, the caller's own, and the deployment from before initialization");
  await assert.rejects(client.mutate("add",null,{requestId:id}),{code:"RETRY_WINDOW_EXPIRED"});
  await client.refreshRetryIdentity();
  const next=await client.mutate("add");
  assert.equal(next.value,3);
  for(const member of cluster.members) {
    const c=new FlowerClient(member.url,{credentials:{subject:"alice"}});
    assert.equal((await c.query("read")).value,3);
    await assert.rejects(c.mutate("add",null,{requestId:id}),{code:"RETRY_WINDOW_EXPIRED"});
  }
  const sessionId=randomBytes(16).toString("hex");
  const opened=await client.openRetrySession(sessionId);
  const session=opened.value;
  assert.deepEqual((await client.openRetrySession(sessionId)).value,session);
  const one=client.sessionRequestId(session,1);
  const two=client.sessionRequestId(session,2);
  assert.equal((await client.mutate("add",null,{requestId:one})).value,4);
  assert.equal((await client.mutate("add",null,{requestId:two})).value,5);
  assert.equal((await client.mutate("add",null,{requestId:one})).duplicate,true);
  const intruder=new FlowerClient(follower,{credentials:{subject:"mallory"}});
  await assert.rejects(intruder.retrySessionStatus(session),{code:"RETRY_SESSION_FORBIDDEN"});
  await assert.rejects(intruder.mutate("add",null,{requestId:one}),{code:"RETRY_SESSION_FORBIDDEN"});
  const acknowledged=await client.acknowledgeRetrySession(session,2,{limit:2});
  assert.equal(acknowledged.value.acknowledgedThrough,2);
  await assert.rejects(client.mutate("add",null,{requestId:one}),{code:"ALREADY_ACKNOWLEDGED"});
  await assert.rejects(client.acknowledgeRetrySession(session,3),{code:"RETRY_ACK_GAP"});
  const abandoned=await client.acknowledgeRetrySession(session,3,{abandon:true,limit:1});
  assert.equal(abandoned.value.acknowledgedThrough,3);
  await assert.rejects(client.mutate("add",null,{requestId:client.sessionRequestId(session,3)}),{code:"ALREADY_ACKNOWLEDGED"});
  await cluster.crashLeaderAndRecover();
  const afterFailover=new FlowerClient(cluster.leader.url,{credentials:{subject:"alice"}});
  assert.equal((await afterFailover.retrySessionStatus(session)).value.acknowledgedThrough,3);
  assert.equal((await afterFailover.query("read")).value,5);
  await afterFailover.closeRetrySession(session);
  await assert.rejects(afterFailover.mutate("add",null,{requestId:client.sessionRequestId(session,4)}),{code:"RETRY_SESSION_CLOSED"});
  await afterFailover.refreshRetryIdentity();
  await assert.rejects(afterFailover.openRetrySession(sessionId),{code:"RETRY_SESSION_CLOSED"});
  const restoredIncarnation=randomBytes(16).toString("hex");
  const latest=await admin.retentionStatus();
  await admin.controlRetention(latest.revision,{operation:"reincarnate",incarnation,new_incarnation:restoredIncarnation,fence_attestation:"test cluster is exclusively owned; simulated fenced restore"});
  assert.deepEqual((await afterFailover.query("history")).value,{database,incarnation:restoredIncarnation});
  await assert.rejects(afterFailover.mutate("add",null,{requestId:id}),{code:"HISTORY_MISMATCH"});
  console.log("retention E2E: structured authorization failures, scoped and caller retries, retirement/GC, authenticated sessions, strict ACK/abandon/close, and leader-failure fences passed");
} finally {
  await cluster.close();
  await rm(directory,{recursive:true,force:true});
}
