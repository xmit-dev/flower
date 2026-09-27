// Diagnostic only: isolated cold-follower first-use fan-out, not a capacity benchmark.
// Usage: node bench/profile-cold-burst.mjs BINARY NEW_OUTPUT_DIRECTORY [CONCURRENCY=16]
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { connect } from "node:http2";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { LocalCluster } from "./cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin } from "../sdk/index.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [binaryArg, directoryArg, concurrencyArg = "16"] = process.argv.slice(2);
assert.ok(binaryArg && directoryArg, "Usage: profile-cold-burst.mjs BINARY NEW_OUTPUT_DIRECTORY [CONCURRENCY=16]");
const binary = resolve(binaryArg), directory = resolve(directoryArg), concurrency = Number(concurrencyArg);
assert.ok(Number.isSafeInteger(concurrency) && concurrency > 0 && concurrency <= 64);
await mkdir(directory); // Refuse to reuse or replace previous diagnostic artifacts.
const mapPrefix = join(directory, "native-map");
const settings = {
  FLOWER_OTEL_ENABLED: "0", OTEL_SDK_DISABLED: "true", FLOWER_BENCH_LOG: "flower=info,openraft=warn",
  FLOWER_PROFILE_WASM_MAP: mapPrefix, FLOWER_EVALUATION_TIMEOUT_MS: "30000",
  TOKIO_WORKER_THREADS: "2", FLOWER_QUERY_WORKERS: String(concurrency),
  FLOWER_PREPARATION_WORKERS: String(concurrency), FLOWER_WRITER_PREPARATION_WORKERS: "1",
  FLOWER_WRITER_BATCH_MODE: "adaptive", FLOWER_WRITER_BATCH_MS: "200",
  FLOWER_WRITER_QUEUE_CAPACITY: "1024",
};
for (const key of Object.keys(process.env)) {
  if (key.startsWith("FLOWER_PROFILE_") || key.startsWith("OTEL_")) delete process.env[key];
}
Object.assign(process.env, settings);

function cpuSeconds(pid) {
  const raw = execFileSync("ps", ["-p", String(pid), "-o", "time="], { encoding: "utf8" }).trim();
  const [daysPrefix, clock] = raw.includes("-") ? raw.split("-") : ["0", raw];
  const parts = clock.split(":").map(Number);
  assert.ok(parts.length >= 2 && parts.length <= 3 && parts.every(Number.isFinite), `Unexpected ps CPU time: ${raw}`);
  let seconds = 0;
  for (const part of parts) seconds = seconds * 60 + part;
  return { raw, seconds: seconds + Number(daysPrefix) * 86400 };
}

async function nativeMaps(pid) {
  const path = `${mapPrefix}.${pid}.jsonl`;
  const lines = (await readFile(path, "utf8")).trim().split("\n").filter(Boolean);
  const records = lines.map(line => JSON.parse(line));
  assert.ok(records.every(record => record.pid === pid), "native mapping PID mismatch");
  return { path, records: records.length, nativeTextBytes: records.map(record => record.text_length) };
}

function query(session, id) {
  const started = performance.now();
  return new Promise((resolve, reject) => {
    const stream = session.request({ ":method": "POST", ":path": "/v1/query", "content-type": "application/json" });
    let status, body = "";
    stream.setEncoding("utf8");
    stream.setTimeout(60_000, () => { stream.close(); reject(new Error(`query ${id} timeout`)); });
    stream.on("response", headers => { status = headers[":status"]; });
    stream.on("data", chunk => { body += chunk; });
    stream.on("error", reject);
    stream.on("end", () => {
      try { resolve({ id, status, elapsedMs: performance.now() - started, response: JSON.parse(body) }); }
      catch (error) { reject(error); }
    });
    // Unique args prevent the separate per-query result/coalescing cache from
    // reducing this burst to one evaluation before bundle preparation.
    stream.end(JSON.stringify({ name: "cold.echo", args: { id } }));
  });
}

const cluster = new LocalCluster({ nodes: 3, binary, startupTimeoutMs: 60_000, requestTimeoutMs: 10_000 });
let session;
let report;
try {
  const fixture = join(directory, "fixture.ts");
  await writeFile(fixture, `import { define, query } from ${JSON.stringify(join(root, "sdk/index.ts"))};
let calls = 0;
export default define({http:{"cold.echo":query("internal.cold.echo",{consistency:"replica-local"},(_ctx,args:{id:number})=>({id:args.id,calls:++calls}))}});
`);
  const bundle = await buildBundle(fixture, { initialization: "static" });
  await writeFile(join(directory, "bundle.json"), JSON.stringify(bundle) + "\n");
  const binarySha256 = createHash("sha256").update(await readFile(binary)).digest("hex");
  await cluster.start();
  const deployed = await new FlowerAdmin(cluster.url, { adminToken: cluster.adminToken }).deploy(bundle, {
    requestId: "cold-burst-deploy", signal: AbortSignal.timeout(60_000),
  });
  const leader = cluster.leader;
  const follower = cluster.members.find(node => node.id !== leader.id);
  const pid = follower.process.child.pid;
  const targetIndex = (await cluster.metrics(leader)).last_applied?.index;
  assert.ok(Number.isSafeInteger(targetIndex), "deployment has a committed applied index");
  const deadline = performance.now() + 30_000;
  let followerMetrics;
  do {
    followerMetrics = await cluster.metrics(follower); // Metadata only; no guest/query execution.
    if ((followerMetrics.last_applied?.index ?? -1) >= targetIndex) break;
    assert.ok(performance.now() < deadline, "follower did not apply deployment");
    await delay(10);
  } while (true);
  assert.equal(followerMetrics.state, "Follower");
  session = connect(follower.url, { settings: { enablePush: false } });
  session.on("error", () => {});
  await once(session, "connect"); // Establish HTTP/2 without making a query.
  const mapsBefore = await nativeMaps(pid);
  assert.equal(mapsBefore.records, 2, "cold follower has only instrumented/runtime-base maps; bundle must not be prepared");
  const cpuBefore = cpuSeconds(pid);
  const startedAt = new Date().toISOString(), started = performance.now();
  const results = await Promise.all(Array.from({ length: concurrency }, (_, id) =>
    query(session, id).catch(error => ({ id, elapsedMs: performance.now() - started, error: String(error) }))));
  const elapsedMs = performance.now() - started;
  const cpuAfter = cpuSeconds(pid);
  const mapsAfter = await nativeMaps(pid);
  const correct = results.filter(result => result.status === 200
    && result.response?.value?.id === result.id && result.response?.value?.calls === 1).length;
  const times = results.map(result => result.elapsedMs).sort((a, b) => a - b);
  report = {
    schemaVersion: 1, kind: "cold-follower-distinct-query-burst", binary, binarySha256,
    bundleHash: bundle.hash, bundleBytes: Buffer.byteLength(bundle.javascript), startedAt,
    settings, concurrency, transport: "HTTP/2 one preconnected session; distinct args; no retries",
    target: { id: follower.id, pid, url: follower.url, stateBefore: followerMetrics.state,
      lastAppliedBefore: followerMetrics.last_applied, targetIndex, deploymentRevision: deployed.revision },
    elapsedMs, latencyMs: { min: times[0], p50: times[Math.ceil(times.length * .5) - 1], max: times.at(-1) },
    cpu: { before: cpuBefore, after: cpuAfter, processSeconds: cpuAfter.seconds - cpuBefore.seconds,
      scope: "Follower cumulative user+system CPU from ps across burst, including ordinary Raft background work; ps resolution 0.01s" },
    maps: { before: mapsBefore, after: mapsAfter, newPreparedImages: mapsAfter.records - mapsBefore.records,
      scope: "One synchronous native-profile map per successfully native-compiled bundle image; excludes compilation attempts failing before map export" },
    correct, failed: concurrency - correct, results,
  };
  await writeFile(join(directory, "result.json"), JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify({ directory, binary, bundleHash: bundle.hash, correct, failed: report.failed,
    elapsedMs, followerCpuSeconds: report.cpu.processSeconds, newPreparedImages: report.maps.newPreparedImages,
    latencyMs: report.latencyMs }, null, 2));
  assert.equal(correct, concurrency, "every unique query must return its own args and pristine guest state");
  assert.ok(report.maps.newPreparedImages >= 1, "burst compiled at least one new native bundle image");
} catch (error) {
  await writeFile(join(directory, "failure.txt"), `${error.stack ?? error}\n${JSON.stringify(cluster.logTails(), null, 2)}\n`);
  throw error;
} finally {
  session?.destroy();
  await cluster.close();
}
