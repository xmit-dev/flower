// Run after cargo build --bin flower: node tests/e2e-otel.mjs
// Inspect actual OTLP JSON before any benchmark collector sanitization.
import assert from "node:assert/strict";
import { createServer, request as httpRequest } from "node:http";
import { writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/index.ts";

const binary = resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower");
const payloads = [];
const protobufExports = [];
const collector = createServer(async (request, response) => {
  try {
    assert.ok(["/v1/traces", "/v1/metrics"].includes(request.url));
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const bytes = Buffer.concat(chunks);
    if (request.headers["content-type"] === "application/x-protobuf") {
      assert.ok(bytes.length > 0, "protobuf export is not empty");
      protobufExports.push(request.url);
      response.writeHead(200, { "content-type": "application/x-protobuf" }); response.end();
    } else {
      assert.match(request.headers["content-type"], /application\/json/);
      payloads.push({ path: request.url, data: JSON.parse(bytes) });
      response.writeHead(200, { "content-type": "application/json" }); response.end("{}");
    }
  } catch (error) { response.writeHead(400); response.end(); throw error; }
});
await new Promise(resolve => collector.listen(0, "127.0.0.1", resolve));
const overrides = {
  FLOWER_OTEL_ENABLED: "1", OTEL_SDK_DISABLED: "false",
  OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${collector.address().port}`,
  OTEL_EXPORTER_OTLP_PROTOCOL: "http/json", OTEL_TRACES_SAMPLER: "always_on",
  OTEL_BSP_SCHEDULE_DELAY: "100", OTEL_METRIC_EXPORT_INTERVAL: "200",
  OTEL_RESOURCE_ATTRIBUTES: "flower.bench.run_id=e2e-otel",
};
// Do not allow a caller's signal-specific exporters/headers to leak this test.
const names = new Set([...Object.keys(overrides), ...Object.keys(process.env).filter(key => key.startsWith("OTEL_"))]);
const previous = Object.fromEntries([...names].map(key => [key, process.env[key]]));
for (const key of names) delete process.env[key];
Object.assign(process.env, overrides);
let cluster;
try {
  cluster = new LocalCluster({ nodes: 3, binary });
  await cluster.start();
  const secret = "otel-must-never-export-this-payload";
  const fixture = join(cluster.directory, "otel.ts");
  await writeFile(fixture, `import { collection, define, mutation, query } from ${JSON.stringify(resolve("sdk/index.ts"))};
const rows=collection<any>("otel.records");
export default define({http:{
  "test.write": mutation("write", (ctx,args:any)=>{ctx.set(rows,"one",args);return args;}),
  "test.read": query("read",{consistency:"replica-local"},ctx=>ctx.get(rows,"one")),
  "test.fail": mutation("fail",(_ctx,args:any)=>{throw new Error(args.secret);}),
  "test.queryfail": query("queryfail",(_ctx,args:any)=>{throw new Error(args.secret);})
}});`);
  const client = new FlowerClient(cluster.url);
  await new FlowerAdmin(cluster.url, { adminToken: cluster.adminToken })
    .deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "otel-deploy" });
  const traceId = "4bf92f3577b34da6a3ce929d0e0e4736", parentId = "00f067aa0ba902b7";
  const follower = cluster.members.find(node => node !== cluster.leader);
  const response = await fetch(follower.url + "/v1/mutate", {
    method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${secret}`,
      traceparent: `00-${traceId}-${parentId}-01`, baggage: `private=${secret}` },
    body: JSON.stringify({ name: "test.write", args: { secret }, requestId: secret }),
    signal: AbortSignal.timeout(15_000),
  });
  assert.equal(response.status, 200, await response.text());
  for (let i=0;i<6;i++) assert.deepEqual((await client.query("test.read")).value, { secret });
  const bodyTrace = "6bf92f3577b34da6a3ce929d0e0e4736";
  await new Promise((resolve, reject) => {
    const request = httpRequest(cluster.url + "/v1/query", { method: "POST", headers: {
      "content-type": "application/json", traceparent: `00-${bodyTrace}-${parentId}-01`,
    } }, response => {
      response.resume();
      response.on("end", () => response.statusCode === 200 ? resolve() : reject(new Error(`delayed body: ${response.statusCode}`)));
    });
    request.on("error", reject);
    request.flushHeaders();
    setTimeout(() => request.end(JSON.stringify({ name: "test.read", args: {} })), 80);
  });
  const malformed = await fetch(cluster.url + "/v1/query", {
    method: "POST", headers: { "content-type": "application/json" }, body: "{",
  });
  assert.equal(malformed.status, 400);
  await malformed.arrayBuffer();
  // Callers receive the method's failure text; the privacy check below proves OTLP never does.
  await assert.rejects(client.mutate("test.fail", { secret }, { requestId: "otel-fail" }),
    error => error.code === "EVALUATION_FAILED" && error.failure?.code === "COMPUTE_ERROR" && error.failure.message === secret);
  await assert.rejects(client.query("test.queryfail", { secret }),
    error => error.code === "EVALUATION_FAILED" && error.failure?.code === "COMPUTE_ERROR" && error.failure.message === secret);
  await fetch(cluster.url + `/unknown-${secret}?q=${secret}`, { signal: AbortSignal.timeout(2000) });
  await cluster.close(); cluster = null;

  const spans = payloads.flatMap(p => (p.data.resourceSpans ?? []).flatMap(r => r.scopeSpans.flatMap(s => s.spans)));
  const metrics = payloads.flatMap(p => (p.data.resourceMetrics ?? []).flatMap(r => r.scopeMetrics.flatMap(s => s.metrics)));
  const names = new Set(spans.map(s => s.name));
  for (const name of ["flower.http.request", "flower.writer.submit", "flower.writer.batch", "flower.raft.rpc"]) assert.ok(names.has(name), `missing span ${name}: ${[...names]}`);
  assert.ok([...names].some(n => n.startsWith("flower.storage.")), "storage spans exported");
  assert.ok([...names].some(n => n.startsWith("flower.evaluator.")), "evaluator spans exported");
  const attrs = span => Object.fromEntries((span.attributes ?? []).map(a => [a.key, a.value.stringValue ?? a.value.intValue ?? a.value.doubleValue]));
  const ingress = spans.find(s => s.traceId === bodyTrace && s.name === "flower.http.stage" && attrs(s).stage === "body_receive");
  assert.ok(ingress && Number(BigInt(ingress.endTimeUnixNano) - BigInt(ingress.startTimeUnixNano)) >= 30_000_000,
    "body reception reports transport waiting inside the request trace");
  assert.ok(spans.some(s => s.traceId === bodyTrace && s.name === "flower.http.stage" && attrs(s).stage === "json_extract" && attrs(s).outcome === "ok"), "JSON extraction has a separate stage");
  assert.ok(spans.some(s => s.name === "flower.http.stage" && attrs(s).stage === "json_extract" && attrs(s).outcome === "error"), "invalid JSON records extraction failure");
  assert.ok(metrics.some(m => m.name === "flower.http.server.stage.duration"), "HTTP stage histograms exported");
  const forwarded = spans.filter(s => s.traceId === traceId && s.name === "flower.http.request");
  assert.ok(forwarded.some(s => s.parentSpanId === parentId && attrs(s)["http.route"] === "/v1/mutate"), "incoming W3C parent preserved");
  assert.ok(forwarded.some(s => attrs(s)["http.route"] === "/raft/forward"), "W3C context reaches leader");
  assert.ok(spans.some(s => (s.links ?? []).some(link => link.traceId === traceId)), "writer batch links contributing request trace");
  assert.ok(spans.some(s => s.name === "flower.query.stage" && attrs(s).stage === "blocking_evaluation" && attrs(s).outcome === "error" && [2,"STATUS_CODE_ERROR"].includes(s.status?.code)), "query evaluation failures have error outcomes and status");
  for (const prefix of ["flower.http.", "flower.query.", "flower.writer.", "flower.storage.", "flower.evaluator.", "flower.raft."]) assert.ok(metrics.some(m => m.name.startsWith(prefix)), `missing metric ${prefix}`);
  const resources = payloads.flatMap(p => p.data.resourceSpans ?? p.data.resourceMetrics ?? []).map(r => attrs({ attributes:r.resource.attributes }));
  assert.equal(new Set(resources.map(r => r["service.instance.id"])).size, 3, "replicas retain distinct resources");
  const wire = JSON.stringify(payloads);
  assert.ok(!wire.includes(secret), "payloads, credentials, request IDs, raw URLs, baggage and error text never reach OTLP");
  process.env.OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf";
  cluster = new LocalCluster({ nodes:1, binary }); await cluster.start(); await cluster.close(); cluster=null;
  assert.ok(protobufExports.includes("/v1/traces") && protobufExports.includes("/v1/metrics"), "both signals also export protobuf");
  const before = payloads.length;
  const protobufBefore = protobufExports.length;
  process.env.FLOWER_OTEL_ENABLED = "0";
  cluster = new LocalCluster({ nodes:1, binary }); await cluster.start(); await cluster.close(); cluster=null;
  assert.equal(payloads.length, before, "disabled telemetry exports nothing even with endpoint configured");
  assert.equal(protobufExports.length, protobufBefore, "disabled telemetry starts no protobuf exporters");
  console.log(`OTEL E2E PASS: ${spans.length} spans, ${metrics.length} metric exports; JSON/protobuf, forwarding, batch links, all replicas, privacy, shutdown flush and disabled mode.`);
} finally {
  await cluster?.close();
  await new Promise(resolve => collector.close(resolve));
  for (const [key,value] of Object.entries(previous)) { if (value===undefined) delete process.env[key]; else process.env[key]=value; }
}
