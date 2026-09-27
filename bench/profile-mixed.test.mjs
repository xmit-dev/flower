import test from "node:test";
import assert from "node:assert/strict";
import { assertFreshReport, assertNoOutputAlias, diagnosticPath, parseStorageLine } from "./profile-mixed.mjs";
import { installMixedObserver, readMixedCaptures } from "./profile-mixed-observer.mjs";
import { mkdtemp, rm, readFile, writeFile, link } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { PassThrough } from "node:stream";

test("multi-group profiling requires a fresh measured aggregate window", () => {
  const started = Date.parse("2026-09-24T12:00:00Z");
  const report = { groups: [{}, {}], loadStartedAt: "2026-09-24T12:00:01Z", durationMs: 20000 };
  assert.doesNotThrow(() => assertFreshReport(report, started));
  assert.throws(() => assertFreshReport(report, started + 2000), /fresh/);
  assert.throws(() => assertFreshReport({ ...report, durationMs: 0 }, started), /usable/);
  assert.throws(() => assertFreshReport({ ...report, loadStartedAt: "invalid" }, started), /fresh/);
});

test("storage tracing keeps per-node timing units and failed commits distinct", () => {
  assert.equal(parseStorageLine("unrelated storage message"), null);
  assert.deepEqual(parseStorageLine('INFO durable storage write node=2 operation="build_snapshot" ok=true encode_us=1200 flush_us=4567 encoded_bytes=1000'), {
    node: 2, operation: "build_snapshot", ok: true, encode_us: 1200, flush_us: 4567, encoded_bytes: 1000,
  });
  assert.equal(parseStorageLine('durable storage write node=1 operation="apply" ok=false total_us=500').ok, false);
  assert.deepEqual(parseStorageLine('INFO flower::storage_profile: storage write node=1 operation="apply" ok=true flush_us=12'), {
    node: 1, operation: "apply", ok: true, flush_us: 12,
  });
});

test("worker observer captures bounded node/group diagnostics and evaluator stages", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-observer-test-"));
  let now = 1000;
  class Cluster {
    constructor() { this.directory = "/tmp/test-cluster"; }
    _startNode(node) { node.process = { child: { stdout: new PassThrough(), pid: 123 } }; }
  }
  const original = Cluster.prototype._startNode;
  const config = { directory, token: "this-run", startedAt: 1000, group: 0, startOffsetMs: 5, maxRecords: 2, maxBytes: 4096 };
  const observer = installMixedObserver(config, { Cluster, now: () => now });
  try {
    const cluster = new Cluster(), node = { id: 2 };
    cluster._startNode(node);
    const log = node.process.child.stdout;
    const group = "mutation group timing group_commands=4 group_committed=true prepare_us=23\n";
    log.write(group); // Startup and warmup never consume the retained sample.
    observer.begin(1001);
    now = 1007;
    log.write(group + group + group);
    log.write('mutation timing method="pizza.tip" duplicate=false evaluation_us=45\n');
    log.write('durable storage write node=999 operation="apply" ok=true flush_us=67\n');
    log.write('evaluator wall-clock stages name="internal.pizza.tip" mode=mutation total_ms=1.5 cell_count=2\n');
    log.end();
    now = 1010;
    await observer.finish("benchmark-run");
    assert.equal(Cluster.prototype._startNode, original);
    const result = await readMixedCaptures(config, { runId: "benchmark-run" }, 1);
    assert.equal(result.groups.length, 2);
    assert.equal(result.dropped, 1);
    assert.equal(result.responses.length, 1);
    assert.equal(result.storage[0].node, 2, "Observed process identity overrides any log field");
    assert.equal(result.evaluator[0].total_ms, 1.5);
    for (const records of [result.groups, result.responses, result.storage, result.evaluator]) {
      for (const record of records) {
        assert.equal(record.group, 0);
        assert.equal(record.node, 2);
        assert.equal(record.cluster, "test-cluster");
        assert.equal(record.receivedAt, 1007);
      }
    }
    await assert.rejects(observer.finish("benchmark-run"), /EEXIST/);
    await assert.rejects(readMixedCaptures(config, { runId: "old-run" }, 1), /stale/);
    const path = join(directory, "group-0.json");
    const saved = JSON.parse(await readFile(path, "utf8"));
    saved.token = "old-token";
    await writeFile(path, JSON.stringify(saved));
    await assert.rejects(readMixedCaptures(config, { runId: "benchmark-run" }, 1), /stale/);
  } finally { observer.restore(); await rm(directory, { recursive: true, force: true }); }
});

test("observer byte budget bounds each record stream independently", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-observer-bytes-"));
  class Cluster {
    constructor() { this.directory = directory; }
    _startNode(node) { node.process = { child: { stdout: new PassThrough(), pid: 123 } }; }
  }
  const config = { directory, token: "run", startedAt: 1000, group: 0, startOffsetMs: 0, maxRecords: 100, maxBytes: 1 };
  const observer = installMixedObserver(config, { Cluster, now: () => 1001 });
  try {
    observer.begin(1001);
    const node = { id: 1 }; new Cluster()._startNode(node);
    node.process.child.stdout.end("mutation group timing group_commands=4 group_committed=true\n");
    await observer.finish("benchmark");
    const result = await readMixedCaptures(config, { runId: "benchmark" }, 1);
    assert.equal(result.groups.length, 0);
    assert.equal(result.dropped, 1);
  } finally { observer.restore(); await rm(directory, { recursive: true, force: true }); }
});

test("diagnostic output protects native driver and existing input-file aliases", async () => {
  assert.throws(() => diagnosticPath({ json: "/tmp/report.json", driverBinary: "/tmp/report-groups.json" }), /driver-binary/);
  const directory = await mkdtemp(join(tmpdir(), "flower-profile-alias-"));
  try {
    const driverBinary = join(directory, "driver"), output = join(directory, "report-groups.json");
    await writeFile(driverBinary, "do not replace the native driver");
    await link(driverBinary, output);
    await assert.rejects(assertNoOutputAlias(output, { driverBinary }), /driver-binary/);
    assert.equal(await readFile(driverBinary, "utf8"), "do not replace the native driver");
  } finally { await rm(directory, { recursive: true, force: true }); }
});
