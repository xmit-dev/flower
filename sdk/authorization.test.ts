import assert from "node:assert/strict";
import test from "node:test";
import { define, query, FlowerClient, FlowerError } from "./index.ts";
import { testDatabase } from "./testing.ts";

test("compiled authorization is a private, fresh query hook", async () => {
  const read=query("read",ctx=>ctx.principal());
  const app=define({auth:{authenticate:(_ctx,credentials)=>typeof credentials==="string"?{subject:credentials}:null},http:{read}});
  assert.deepEqual(app.authorize,{name:"$flower.authorize"});
  assert.deepEqual(Object.keys(app.http),["read"]);
  const hook=app.definitions["$flower.authorize"];
  assert.equal(hook.kind,"queryMethod");
  assert.equal((hook as {consistency?:string}).consistency,undefined);
  assert.equal(define({http:{read}}).authorize,undefined);
  assert.throws(()=>define({authorize:query("policy",()=>({subject:"alice"}))} as never),/does not accept "authorize"/);
  assert.throws(()=>define({http:{"$flower.authorize":read}}),/reserved/);
  assert.throws(()=>define({http:{read:query("read",{access:"authenticated"},ctx=>ctx.principal())}}),/no authenticate/);
  const db=await testDatabase(app);
  assert.deepEqual(db.query("read",null,{credentials:"alice"}),{subject:"alice"});
  assert.throws(()=>db.query("read"),(error:any)=>error instanceof FlowerError&&error.status===403&&error.failure?.code==="UNAUTHENTICATED");
});

test("SDK refreshes credentials independently of stable mutation identity", async () => {
  const bodies:any[]=[];
  let version=0;
  const client=new FlowerClient("http://localhost:7101",{
    credentials:()=>({token:++version}),
    fetch:async(_url,init)=>{
      bodies.push(JSON.parse(init.body));
      return new Response(JSON.stringify({revision:1,value:null,duplicate:false}));
    },
  });
  await client.mutate("update",{x:1},{requestId:"intent"});
  await client.mutate("update",{x:1},{requestId:"intent"});
  assert.equal(bodies[0].requestId,bodies[1].requestId);
  assert.deepEqual(bodies[0].args,bodies[1].args);
  assert.deepEqual(bodies.map(x=>x.credentials),[{token:1},{token:2}]);
  await client.partition("store").query("read",null,{credentials:"override"});
  assert.equal(bodies[2].credentials,"override");
});
