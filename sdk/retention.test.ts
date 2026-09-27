import assert from "node:assert/strict";
import test from "node:test";
import { FlowerAdmin, FlowerClient } from "./client.ts";

test("bounded retry IDs pin their original epoch until explicitly refreshed", async()=>{
  let epoch=0;const sent:any[]=[];
  const identity={database:"a".repeat(32),incarnation:"b".repeat(32),currentEpoch:epoch,minEpoch:0};
  const client=new FlowerClient("http://localhost:7101",{boundedRetries:true,fetch:async(url,init)=>{
    if(url.endsWith("/identity"))return new Response(JSON.stringify({revision:1,value:{...identity,currentEpoch:epoch}}));
    sent.push(JSON.parse(init.body));
    return new Response(JSON.stringify({revision:2,value:null,duplicate:false}));
  }});
  const id=await client.newRequestId("order-1");
  assert.match(id,new RegExp(`^f1:${identity.database}:${identity.incarnation}:0:[a-f0-9]{64}$`));
  await client.mutate("order",null,{requestId:id});
  epoch=1;
  await client.mutate("order",null,{requestId:id});
  assert.equal(sent[0].requestId,sent[1].requestId);
  assert.equal(await client.newRequestId("order-1"),id);
  await client.refreshRetryIdentity();
  assert.notEqual(await client.newRequestId("order-1"),id);
  await client.mutate("new-order");
  assert.match(sent[2].requestId,/:1:[a-f0-9]{64}$/);
});

test("with rotating epochs, new IDs follow the leader's epoch and partitions keep their own history",async()=>{
  let epoch=0;const fetched:string[]=[];
  const client=new FlowerClient("http://localhost:7101",{boundedRetries:true,fetch:async(url)=>{
    fetched.push(url);
    const database=url.includes("/partitions/")?"c".repeat(32):"a".repeat(32);
    return new Response(JSON.stringify({revision:1,value:{database,incarnation:"b".repeat(32),currentEpoch:epoch,minEpoch:0,epochMs:40}}));
  }});
  const first=await client.newRequestId("order-1");
  assert.match(first,/:0:[a-f0-9]{64}$/);
  epoch=1;
  // Within a quarter of an epoch, the cached identity still names the current one.
  assert.equal(await client.newRequestId("order-1"),first);
  await new Promise((resolve)=>setTimeout(resolve,15));
  assert.match(await client.newRequestId("order-1"),/:1:[a-f0-9]{64}$/);
  assert.equal(fetched.length,2);
  assert.match(await client.partition("shop").newRequestId("order-1"),new RegExp(`^f1:${"c".repeat(32)}:`));
});

test("bounded retries fail before mutation when the server has no history identity",async()=>{
  const urls:string[]=[];
  const client=new FlowerClient("http://localhost:7101",{boundedRetries:true,fetch:async(url)=>{
    urls.push(url);return new Response(JSON.stringify({revision:0,value:null}));
  }});
  await assert.rejects(client.mutate("order"),{code:"RETENTION_NOT_INITIALIZED"});
  assert.deepEqual(urls,["http://localhost:7101/v1/identity"]);
});

test("transaction closure uses operator routing and preserves explicit bounded progress",async()=>{
  const sent:any[]=[];
  const value={history:"a".repeat(32),nextSequence:10,closedThrough:4,pending:null,blockedReason:null,deletedRecords:0};
  const client=new FlowerAdmin("https://db.example/partitions/shop",{adminToken:"operator",fetch:async(url,init)=>{
    sent.push({url,headers:init.headers,body:JSON.parse(init.body)});
    return new Response(JSON.stringify({revision:20,value}));
  }});
  assert.deepEqual((await client.transactionClosureStatus()).value,value);
  await client.controlTransactionClosure({operation:"close",through:7,maxBytes:4096});
  await client.controlTransactionClosure({operation:"collect",maxBytes:2048});
  assert.ok(sent.every(item=>item.url==="https://db.example/partitions/shop/admin/transactions"&&item.headers.authorization==="Bearer operator"));
  assert.deepEqual(sent.map(item=>item.body),[{operation:"status"},{operation:"close",through:7,maxBytes:4096},{operation:"collect",maxBytes:2048}]);
});

test("retention control is revision-conditional operator routing",async()=>{
  const sent:any[]=[];
  const admin=new FlowerAdmin("https://db.example/",{adminToken:"operator",fetch:async(url,init)=>{
    sent.push({url,headers:init.headers,body:JSON.parse(init.body)});
    return new Response(JSON.stringify({revision:3,value:null,duplicate:false}));
  }});
  assert.equal((await admin.retentionStatus()).revision,3);
  const action={operation:"advance" as const,incarnation:"b".repeat(32),current_epoch:2,min_epoch:1};
  await admin.controlRetention(3,action);
  assert.ok(sent.every(item=>item.url==="https://db.example/admin/retention"&&item.headers.authorization==="Bearer operator"));
  assert.deepEqual(sent.map(item=>item.body),[{operation:"status"},{expected_revision:3,action}]);
});

test("staged deployment controls preserve durable job identity and explicit progress",async()=>{
  const sent:any[]=[];
  const value={requestId:"deploy-1",phase:"backfill",baseRevision:10,baseBundleHash:null,
    bundleHash:"a".repeat(64),cursor:null,scannedRows:0,builtEntries:0,cleanupCursor:null};
  const client=new FlowerAdmin("https://db.example/partitions/shop",{adminToken:"operator",fetch:async(url,init)=>{
    sent.push({url,headers:init.headers,body:JSON.parse(init.body)});
    return new Response(JSON.stringify({revision:20,value}));
  }});
  const bundle={hash:value.bundleHash,javascript:"source"};
  assert.deepEqual((await client.stageDeployment(bundle,{requestId:"deploy-1"})).value,value);
  await client.stagedDeploymentStatus();
  await client.controlStagedDeployment({operation:"advance",requestId:"deploy-1",maxBytes:4096});
  await client.controlStagedDeployment({operation:"activate",requestId:"deploy-1"});
  await client.controlStagedDeployment({operation:"collect",requestId:"deploy-1",maxBytes:2048});
  await client.controlStagedDeployment({operation:"cancel",requestId:"deploy-1"});
  assert.ok(sent.every(item=>item.url==="https://db.example/partitions/shop/admin/deployments"&&item.headers.authorization==="Bearer operator"));
  assert.deepEqual(sent.map(item=>item.body),[
    {operation:"stage",requestId:"deploy-1",bundle},{operation:"status"},
    {operation:"advance",requestId:"deploy-1",maxBytes:4096},
    {operation:"activate",requestId:"deploy-1"},
    {operation:"collect",requestId:"deploy-1",maxBytes:2048},
    {operation:"cancel",requestId:"deploy-1"},
  ]);
});


test("retry sessions keep explicit sequence and acknowledgements bound to authenticated ownership",async()=>{
  const database="a".repeat(32),incarnation="b".repeat(32),id="c".repeat(32);
  const session={database,incarnation,id,epoch:7,acknowledgedThrough:0,closed:false};
  const sent:any[]=[];
  const client=new FlowerClient("http://localhost:7101",{credentials:()=>({subject:"alice"}),fetch:async(url,init)=>{
    if(url.endsWith("/identity"))return new Response(JSON.stringify({revision:1,value:{database,incarnation,currentEpoch:7,minEpoch:5}}));
    const body=JSON.parse(init.body);sent.push(body);
    return new Response(JSON.stringify({revision:2,value:{...session,
      acknowledgedThrough:body.operation==="ack"?body.through:0,closed:body.operation==="close"}}));
  }});
  assert.deepEqual((await client.openRetrySession(id)).value,session);
  assert.equal(client.sessionRequestId(session,3),`f2:${database}:${incarnation}:7:${id}:3`);
  assert.equal(client.sessionRequestId(session,3),client.sessionRequestId(session,3));
  assert.throws(()=>client.sessionRequestId(session,0),TypeError);
  const ack=await client.acknowledgeRetrySession(session,3,{limit:8});
  assert.throws(()=>client.sessionRequestId(ack.value,3),TypeError);
  assert.equal(session.acknowledgedThrough,0,"the caller controls durable local session state");
  await client.retrySessionStatus(session,{credentials:{subject:"refreshed"}});
  await client.acknowledgeRetrySession(session,4,{abandon:true});
  const closed=await client.closeRetrySession(session);
  assert.throws(()=>client.sessionRequestId(closed.value,8),TypeError);
  assert.deepEqual(sent.map(x=>x.operation),["open","ack","status","ack","close"]);
  assert.deepEqual(sent[0],{operation:"open",session:id,incarnation,epoch:7,credentials:{subject:"alice"}});
  assert.equal(sent[1].abandon,false);
  assert.equal(sent[3].abandon,true);
  assert.deepEqual(sent[2].credentials,{subject:"refreshed"});
  assert.ok(sent.every(x=>!("owner" in x)),"the server derives owner from authorization");
});
