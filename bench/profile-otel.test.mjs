import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createCapture, otelResourceAttributes, safeAttributes, startCollector } from "./otel-capture.mjs";
import { buildOtelReport, renderOtelReport, summarizeMetricSeries } from "./otel-report.mjs";
import { captureLimits, prepareOutput, runProfile, withOtelEnvironment } from "./profile-otel.mjs";

const ns = ms => String(BigInt(ms) * 1_000_000n);
const attr = (key, value) => ({ key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } });
const resource = id => ({ attributes: [attr("flower.bench.run_id", id), attr("flower.node.id", "1"), attr("process.pid", 123),
  attr("service.instance.id", "127.0.0.1:9000/1/123"), attr("server.address", "127.0.0.1:9000"), attr("password", "secret")] });
function tracePayload(runId, start = 1002, end = 1003) {
  return { resourceSpans: [{ resource: resource(runId), scopeSpans: [{ spans: [{ name: "flower.writer.request", traceId: "a".repeat(32), spanId: "b".repeat(16),
    startTimeUnixNano: ns(start), endTimeUnixNano: ns(end), attributes: [attr("stage", "writer"), attr("password", "secret"), attr("evaluation_us", 450)],
    links: [{ traceId: "c".repeat(32), spanId: "d".repeat(16), flags: 1 }], events: [{ name: "secret exception", attributes: [attr("password", "secret")] }],
    droppedLinksCount: 2, droppedEventsCount: 3, droppedAttributesCount: 4 }] }] }] };
}
function metricPayload(runId, time = 1002, count = 3) {
  return { resourceMetrics: [{ resource: resource(runId), scopeMetrics: [{ metrics: [{ name: "flower.writer.stage.duration", unit: "s",
    histogram: { aggregationTemporality: 2, dataPoints: [{ startTimeUnixNano: ns(1000), timeUnixNano: ns(time), count: String(count), sum: count * 0.2,
      explicitBounds: [0.1, 0.5], bucketCounts: ["0", String(count), "0"], attributes: [attr("stage", "commit"), attr("execution", "serial")] }] } }] }] }] };
}
const post = (endpoint, signal, data) => fetch(`${endpoint}/v1/${signal}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(data) });
const benchmark = (start = 1001, end = 1010) => ({ runId: "benchmark-run", passed: true, loadStartedAt: new Date(start).toISOString(), loadEndedAt: new Date(end).toISOString() });

test("OTLP keeps node identity, timing, links and numeric stages without sensitive payloads", () => {
  const capture = createCapture({ runId: "run", startedAt: 1000 });
  capture.ingest("traces", tracePayload("run"), 1004);
  assert.equal(capture.records[0].resource["flower.node.id"], "1");
  assert.equal(capture.records[0].durationMs, 1);
  assert.equal(capture.records[0].attributes.evaluation_us, 450);
  assert.equal(capture.records[0].links[0].traceId, "c".repeat(32));
  assert.equal(capture.records[0].droppedLinksCount, 2);
  assert.equal(capture.stats.omittedSpanEvents, 1);
  assert.equal(JSON.stringify(capture.records).includes("secret"), false);
  assert.deepEqual(safeAttributes([attr("http.route", "/v1/query"), attr("user.token", "secret")]), { "http.route": "/v1/query" });
});

test("capture bounds record and encoded byte retention, rejects stale and foreign records", () => {
  const capture = createCapture({ runId: "run", startedAt: 1000, maxRecords: 1 });
  capture.ingest("traces", tracePayload("other"), 1004);
  capture.ingest("traces", tracePayload("run", 990, 991), 1004);
  capture.ingest("traces", tracePayload("run"), 1004);
  capture.ingest("metrics", metricPayload("run"), 1004);
  assert.equal(capture.stats.foreignRecords, 1);
  assert.equal(capture.stats.invalidRecords, 1);
  assert.equal(capture.stats.droppedMetricPoints, 1);
  assert.equal(capture.records.length, 1);
  const tiny = createCapture({ runId: "run", startedAt: 1000, maxBytes: 1 });
  tiny.ingest("traces", tracePayload("run"), 1004);
  assert.equal(tiny.stats.droppedSpans, 1);
  assert.equal(tiny.stats.retainedBytes, 0);
});

test("loopback HTTP receiver validates input, bounds requests, acknowledges capture drops and closes", async () => {
  const capture = createCapture({ runId: "run", startedAt: Date.now(), maxRecords: 1 });
  const collector = await startCollector({ capture, maxRequestBytes: 2048 });
  const t = Date.now();
  try {
    assert.equal((await post(collector.endpoint, "traces", tracePayload("run", t, t + 1))).status, 200);
    const limited = await post(collector.endpoint, "traces", tracePayload("run", t, t + 1));
    assert.equal((await limited.json()).partialSuccess.rejectedSpans, "1");
    assert.equal((await fetch(`${collector.endpoint}/v1/traces`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}bad" })).status, 400);
    assert.equal((await post(collector.endpoint, "traces", { padding: "x".repeat(3000) })).status, 413);
    assert.equal((await post(collector.endpoint + "bad", "traces", tracePayload("run", t, t + 1))).status, 400);
    assert.equal(capture.stats.malformedRequests, 1);
    assert.equal(capture.stats.oversizedRequests, 1);
    assert.equal(capture.stats.rejectedRequests, 1);
  } finally { await collector.close(); await collector.close(); }
  await assert.rejects(post(collector.endpoint, "traces", {}));
});

test("collector shutdown terminates an unfinished request", async () => {
  const capture = createCapture({ runId: "run" });
  const collector = await startCollector({ capture });
  const pending = request(`${collector.endpoint}/v1/traces`, { method: "POST", headers: { "content-type": "application/json" } });
  pending.on("error", () => {});
  pending.write('{"resourceSpans":');
  await new Promise(resolve => pending.once("socket", socket => socket.once("connect", resolve)));
  await collector.close();
  pending.destroy();
});

test("histogram summaries difference cumulative exports and exclude incomplete edge intervals", () => {
  const capture = createCapture({ runId: "run", startedAt: 1000 });
  for (const [time, count] of [[1002, 10], [1004, 30], [1008, 40], [1012, 100]]) capture.ingest("metrics", metricPayload("run", time, count), 1015);
  const result = summarizeMetricSeries(capture.records, { start: 1001, end: 1010 });
  assert.equal(result.count, 30, "cumulative snapshots are never summed");
  assert.equal(result.coverage.durationMs, 6);
  assert.equal(result.p99UpperBound, .5);
  assert.equal(result.attributes.execution, "serial");
  const reset = structuredClone(capture.records); reset[2].startTimeMs = 1006;
  assert.equal(summarizeMetricSeries(reset, { start: 1001, end: 1010 }).count, 20);
});

test("report validates measurement freshness and required signals, escapes HTML", () => {
  const capture = createCapture({ runId: "run", startedAt: 1000 });
  assert.equal(buildOtelReport(capture, benchmark()).passed, false);
  assert.throws(() => buildOtelReport(capture, benchmark(900, 990)), /fresh/);
  capture.ingest("traces", tracePayload("run"), 1005);
  capture.ingest("metrics", metricPayload("run", 1002, 3), 1005);
  capture.ingest("metrics", metricPayload("run", 1004, 6), 1005);
  const report = buildOtelReport(capture, benchmark());
  assert.equal(report.passed, true);
  assert.equal(report.sampledSpansInLoad, 1);
  assert.equal(report.metricSeries[0].count, 3);
  report.failures.push("<script>secret</script>");
  assert.equal(renderOtelReport(report).includes("<script>secret"), false);
  assert.equal(renderOtelReport(report).includes("&lt;script&gt;"), true);
});

test("local export configuration clears remote headers/endpoints and restores exact state on failure", async () => {
  const environment = { OTEL_EXPORTER_OTLP_HEADERS: "token=secret", OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "https://secret@remote",
    OTEL_RESOURCE_ATTRIBUTES: "password=secret", FLOWER_OTEL_ENABLED: "0", OTEL_TRACES_SAMPLER_ARG: "0.2" };
  const prior = { ...environment };
  await assert.rejects(withOtelEnvironment("http://127.0.0.1:1/run", "run", async () => {
    assert.equal(environment.OTEL_EXPORTER_OTLP_HEADERS, undefined);
    assert.equal(environment.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, undefined);
    assert.equal(environment.OTEL_TRACES_SAMPLER_ARG, "0.2");
    assert.equal(environment.OTEL_RESOURCE_ATTRIBUTES, "flower.bench.run_id=run,flower.bench.group_index=0");
    throw new Error("intentional");
  }, environment), /intentional/);
  assert.deepEqual(environment, prior);
});

test("diagnostic outputs reject collisions, symlinks into retained reports and input aliases", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-otel-output-test-"));
  try {
    const json = join(directory, "report.json"), html = join(directory, "report.html");
    await assert.rejects(prepareOutput({ json, html, binary: json }), /input/);
    await writeFile(json, "existing");
    await assert.rejects(prepareOutput({ json, html }), /exists/);
    await symlink(resolve("docs"), join(directory, "published"));
    await assert.rejects(prepareOutput({ json: join(directory, "published/never-write.json"), html }), /retained/);
    assert.equal(await readFile(json, "utf8"), "existing");
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("diagnostic runner uses current reports, writes sanitized artifacts and restores environment", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-otel-run-test-"));
  const before = { ...process.env };
  try {
    const result = await runProfile(["--json", join(directory, "bench.json")], { runBenchmark: async options => {
      const runId = process.env.OTEL_RESOURCE_ATTRIBUTES.split(",")[0].split("=")[1], endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
      const t = Date.now() + 2;
      await post(endpoint, "traces", tracePayload(runId, t + 1, t + 2));
      await post(endpoint, "metrics", metricPayload(runId, t + 1, 3));
      await post(endpoint, "metrics", metricPayload(runId, t + 3, 6));
      await writeFile(options.json, JSON.stringify(benchmark(t, t + 5)));
    } });
    assert.equal(result.passed, true);
    assert.equal(JSON.stringify(process.env), JSON.stringify(before));
    assert.equal((await readFile(join(result.directory, "capture.ndjson"), "utf8")).includes("secret"), false);
    assert.match(await readFile(join(result.directory, "report.html"), "utf8"), /Capture validated/);
    await assert.rejects(runProfile(["--json", join(directory, "bench.json")]), /exists/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});


test("diagnostic resource tags distinguish groups without accepting arbitrary resource configuration", () => {
  assert.equal(otelResourceAttributes("run", 7), "flower.bench.run_id=run,flower.bench.group_index=7");
  assert.throws(() => otelResourceAttributes("run,password=secret", 0), /identity/);
  assert.throws(() => otelResourceAttributes("run", -1), /identity/);
  assert.deepEqual(safeAttributes([attr("flower.bench.group_index", "7")], true), { "flower.bench.group_index": "7" });
});


test("writer labels preserve distinct cumulative series and active request counters retain levels", () => {
  const labels = safeAttributes([attr("component", "commands"), attr("reason", "full"), attr("stop_reason", "ready"),
    { key: "successor", value: { boolValue: true } }, { key: "early_drain", value: { boolValue: false } }, attr("backend", "quickjs-wasm")]);
  assert.deepEqual(labels, { component: "commands", reason: "full", stop_reason: "ready", successor: true, early_drain: false, backend: "quickjs-wasm" });
  const points = [3, 8].map((value, index) => ({ kind: "sum", monotonic: false, temporality: 2, name: "flower.http.server.active_requests", resource: {},
    attributes: {}, value, timeMs: 1002 + index * 2, startTimeMs: 1000 }));
  const report = summarizeMetricSeries(points, { start: 1001, end: 1010 });
  assert.equal(report.lastValue, 8);
  assert.equal(report.values.max, 8);
  assert.equal(report.value, undefined);
});


test("capture limit overrides remain explicit finite bounds and never copy unrelated environment", () => {
  assert.deepEqual(captureLimits({}), { maxRecords: 100_000, maxBytes: 67_108_864 });
  assert.deepEqual(captureLimits({ FLOWER_BENCH_OTEL_MAX_RECORDS: "250000", FLOWER_BENCH_OTEL_MAX_BYTES: "268435456",
    OTEL_EXPORTER_OTLP_HEADERS: "authorization=secret", FLOWER_TOKEN: "secret" }), { maxRecords: 250_000, maxBytes: 268_435_456 });
  assert.throws(() => captureLimits({ FLOWER_BENCH_OTEL_MAX_RECORDS: "1000001" }), /1000000/);
  assert.throws(() => captureLimits({ FLOWER_BENCH_OTEL_MAX_BYTES: "536870913" }), /536870912/);
  for (const value of ["", "0", "-1", "1.5", "NaN", "Infinity", "9007199254740992", "secret", " 5", "5e5"]) {
    for (const name of ["FLOWER_BENCH_OTEL_MAX_RECORDS", "FLOWER_BENCH_OTEL_MAX_BYTES"]) {
      assert.throws(() => captureLimits({ [name]: value }), error => error.message.startsWith(`${name} must be a positive integer no greater than `));
    }
  }
});


test("reordered OTLP attributes stay in the same metric series and sampled span group", () => {
  const capture = createCapture({ runId: "run", startedAt: 1000 });
  const first = metricPayload("run", 1002, 3), second = metricPayload("run", 1004, 6);
  second.resourceMetrics[0].scopeMetrics[0].metrics[0].histogram.dataPoints[0].attributes.reverse();
  capture.ingest("metrics", first, 1005); capture.ingest("metrics", second, 1005);
  const span1 = tracePayload("run"), span2 = tracePayload("run", 1003, 1004);
  span1.resourceSpans[0].scopeSpans[0].spans[0].attributes.push(attr("outcome", "ok"));
  span2.resourceSpans[0].scopeSpans[0].spans[0].attributes.push(attr("outcome", "ok"));
  span2.resourceSpans[0].scopeSpans[0].spans[0].attributes.reverse();
  capture.ingest("traces", span1, 1005); capture.ingest("traces", span2, 1005);
  const report = buildOtelReport(capture, benchmark());
  assert.equal(report.metricSeries.length, 1);
  assert.equal(report.metricSeries[0].count, 3);
  assert.equal(report.stages.length, 1);
  assert.equal(report.stages[0].durationMs.count, 2);
});

test("offline report filters hide unmatched rows and update accessible counts", async () => {
  const { runInNewContext } = await import("node:vm");
  const capture = createCapture({ runId: "run", startedAt: 1000 });
  const html = renderOtelReport(buildOtelReport(capture, benchmark()));
  for (const id of ["capture", "nodes", "metrics", "spans", "slowest"]) {
    assert.ok(html.includes(`href="#${id}"`) && html.includes(`id="${id}"`));
  }
  assert.match(html, /<label for="metrics-search">/);
  assert.match(html, /<label for="spans-search">/);
  assert.match(html, /aria-live="polite"/);
  const rows = [{ textContent: "node1 writer commit", hidden: false }, { textContent: "node2 storage append", hidden: false }];
  const output = {}; let onInput;
  const input = { dataset: { filter: "metrics-table" }, value: "", getAttribute: () => "metrics-count",
    addEventListener: (event, listener) => { assert.equal(event, "input"); onInput = listener; } };
  const document = { querySelectorAll: () => [input], getElementById: id => id === "metrics-table" ? { tBodies: [{ rows }] } : output };
  runInNewContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], { document });
  input.value = " COMMIT "; onInput();
  assert.deepEqual(rows.map(row => row.hidden), [false, true]);
  assert.equal(output.textContent, "1 of 2 rows");
  input.value = ""; onInput();
  assert.deepEqual(rows.map(row => row.hidden), [false, false]);
  assert.equal(output.textContent, "2 of 2 rows");
});
