import assert from "node:assert/strict";
import { test } from "node:test";
import { collection, define, query, sqlCatalog, sqlTable, v } from "./index.ts";
import { sqlDeclaration, type SqlTableSpec } from "./sql.ts";
import { testDatabase } from "./testing.ts";

const source = collection<{name:string}>("private.rows").index("name", ["name"]);
const project: SqlTableSpec["project"] = (_ctx, batch) => ({ rows: batch.rows.map((_row, sourceRow) => ({ sourceRow, cells: [null, null] })) });
const table = () => sqlTable({ name:"rows", source, columns:[{name:"id",type:"text",sourceKey:true},{name:"name",type:"text",sourceField:"name"}], indexes:[{name:"name",columns:["name"]}], project });
const catalog = () => sqlCatalog({ name:"app",version:"v1",tables:[table()],authorize:(_ctx,request)=>request.invoker ? {principal:request.invoker,context:null,tables:["rows"]}:null });

test("SQL is explicit opt-in; separate pure callbacks and one-shot flags enter the manifest", () => {
  const relation=table(), sql=sqlCatalog({name:"app",version:"v1",tables:[relation],authorize:()=>null});
  const run=query("sql.query",{cache:false,watch:false},ctx=>ctx.sql.query(sql,{sql:"SELECT id FROM rows"}).result);
  const app=define({sql:[sql],http:{"sql.query":run}});
  assert.deepEqual(app.http["sql.query"],{name:"sql.query",kind:"query",cache:false,watch:false});
  assert.deepEqual(app.collections?.map(c=>c.name),["private.rows"]);
  assert.equal(app.definitions["app.$sql.authority"].kind,"sqlAuthority");
  assert.equal(app.definitions["app.$sql.provider.rows"].kind,"sqlProvider");
  assert.equal(app.sql?.length,1);
  assert.equal(define({collections:[source]}).sql,undefined);
  assert.throws(()=>define({sql:[sql,sql]}),/distinct/);
  assert.throws(()=>define({http:{"escape":app.definitions["app.$sql.provider.rows"] as never}}),/Only query/);
});

test("SQL descriptor validation is default-deny and mapping-aware", () => {
  const spec={name:"rows",source,columns:[{name:"name",type:"text" as const,sourceField:"name"}],project};
  assert.throws(()=>sqlTable({...spec,name:"sqlite_schema"}),/unreserved/);
  assert.throws(()=>sqlTable({...spec,name:"SQL"}),/lowercase/);
  assert.throws(()=>sqlTable({...spec,source:undefined,sources:[]}),/1 to 8/);
  assert.throws(()=>sqlTable({...spec,sources:[source]}),/exactly one/);
  assert.throws(()=>sqlTable({...spec,columns:[...spec.columns,...spec.columns]}),/distinct/);
  assert.throws(()=>sqlTable({...spec,columns:[{name:"name",type:"text",sourceKey:true,sourceField:"name"}]}),/together/);
  assert.throws(()=>sqlTable({...spec,indexes:[{name:"name",columns:["missing"]}]}),/matching native/);
  assert.throws(()=>sqlCatalog({name:"app",version:"v1",tables:[table()],authorize:null as never}),/authority callback/);
  assert.throws(()=>sqlCatalog({name:"app",version:"v1",tables:[table()],authorize:()=>null,relationships:[{name:"broken",fromTable:"rows",fromColumns:["id"],toTable:"secret",toColumns:["id"]}]}),/declared/);
});

test("bounded union sources and typed source-key projectors preserve multiplicity", () => {
  const jsonSource=collection("json.source").key(v.tuple([v.string(),v.int()]));
  const relation=sqlTable({name:"expanded",sources:[jsonSource,source],columns:[{name:"value",type:"text"}],project:(_ctx,batch)=>({rows:batch.rows.flatMap((row,sourceRow)=>[{sourceRow,cells:[{text:JSON.stringify(row.key)}]},{sourceRow,cells:[{text:batch.source.name}]}])})});
  const sql=sqlCatalog({name:"app",version:"v1",tables:[relation],authorize:()=>null});
  const declaration=sqlDeclaration(sql);
  assert.deepEqual(declaration.manifest.tables[0].sources,[{name:"json.source",jsonKey:true},{name:"private.rows",jsonKey:false}]);
  const provider=declaration.definitions.find(entry=>entry.kind==="sqlProvider")!;
  const batch=provider.compute({} as never,{source:{name:"json.source"},rows:[{key:'["x",2]',value:{}}],purpose:null} as never);
  assert.deepEqual(batch,{rows:[{sourceRow:0,cells:[{text:'["x",2]'}]},{sourceRow:0,cells:[{text:"json.source"}]}]});
});

test("Node testDatabase refuses native SQL rather than emulate a second engine", async () => {
  const sql=catalog();
  const db=await testDatabase(define({sql:[sql],http:{run:query("run",{cache:false,watch:false},ctx=>ctx.sql.query(sql,{sql:"SELECT COUNT(*) FROM rows"}).result)}}));
  assert.throws(()=>db.query("run"),error=> typeof error==="object" && error!==null && String(error).includes("native SQL"));
});
