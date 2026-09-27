// Real QuickJS/Wasm + two independent three-node Raft groups.
// Run after cargo build: node tests/e2e-transactions.mjs
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { FlowerAdmin, FlowerClient, FlowerError } from "../sdk/client.ts";
import { buildBundle } from "../sdk/bundle.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = resolve(process.env.E2E_FLOWER_BIN ?? join(root, "target/debug/flower"));
const timeout = Number(process.env.E2E_TIMEOUT_MS ?? 45_000);
const token = randomUUID();
const directory = await mkdtemp(join(tmpdir(), "flower-transactions-"));
const groups = { a: [], b: [] };
const nodes = [];
const processes = [];
const reservations = [];
let registry;

async function until(label, operation) {
  let last;
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try { const value = await operation(); if (value) return value; } catch (error) { last = error; }
    await delay(75);
  }
  throw new Error(`${label} timed out: ${last?.message ?? "condition false"}`, { cause: last });
}
async function reserve(group, id) {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  reservations.push(server);
  const address = `127.0.0.1:${server.address().port}`;
  const node = { group, id, address, url: `http://${address}`, directory: join(directory, `${group}-${id}`), server };
  groups[group].push(node); nodes.push(node);
}
function start(node) {
  const child = spawn(binary, ["--id", String(node.id), "--listen", node.address, "--data", node.directory], {
    cwd: root, env: { ...process.env, FLOWER_ADMIN_TOKEN: token, FLOWER_GROUP: node.group,
      FLOWER_GROUPS: JSON.stringify(registry), RUST_LOG: "flower=info,openraft=warn" }, stdio: ["ignore", "pipe", "pipe"],
  });
  const runtime = { child, ended: false, logs: "" };
  const capture = chunk => { runtime.logs = (runtime.logs + chunk).slice(-100_000); };
  child.stdout.on("data", capture); child.stderr.on("data", capture);
  runtime.exited = new Promise(resolve => { child.once("close", () => { runtime.ended = true; resolve(); }); });
  node.runtime = runtime; processes.push(runtime);
}
async function stop(node) {
  if (!node.runtime.ended) { node.runtime.child.kill("SIGKILL"); await node.runtime.exited; }
}
async function post(node, path, input, authenticated = true) {
  return fetch(node.url + path, { method: "POST", headers: { "content-type": "application/json",
    ...(authenticated ? { authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(input), signal: AbortSignal.timeout(5000) });
}
async function leader(group) {
  return until(`${group} leader`, async () => {
    for (const node of groups[group]) {
      if (node.runtime.ended) continue;
      try {
        const response = await fetch(node.url + "/raft/metrics", { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1500) });
        const metrics = await response.json();
        if (metrics.state === "Leader" && Number(metrics.current_leader) === node.id) return node;
      } catch {}
    }
  });
}
async function value(node) { return (await new FlowerClient(node.url).call("read")).value; }
const participantFailure = { code: "DELIBERATE_FAILURE", message: "deliberate participant failure", details: { staged: 999, tags: ["🌻", null] } };
async function expectAborted(operation, label) {
  await assert.rejects(operation(), error => {
    assert.ok(error instanceof FlowerError, `${label}: ${error}`);
    assert.equal(error.status, 422, label);
    assert.equal(error.code, "TRANSACTION_ABORTED", label);
    assert.deepEqual(error.failure, participantFailure, label);
    return true;
  });
}

try {
  for (const group of Object.keys(groups)) for (let id = 1; id <= 3; id++) await reserve(group, id);
  registry = Object.fromEntries(Object.entries(groups).map(([group, nodes]) => [group, nodes.map(node => node.address)]));
  for (const node of nodes) {
    await new Promise(resolve => node.server.close(resolve));
    start(node);
  }
  for (const node of nodes) await until(`${node.group}/${node.id} health`, async () => (await fetch(node.url + "/health", { signal: AbortSignal.timeout(1000) })).ok);
  for (const group of Object.keys(groups)) {
    const response = await post(groups[group][0], "/raft/initialize", Object.fromEntries(groups[group].map(node => [node.id, node.address])));
    assert.equal(response.status, 200, await response.text());
  }
  const entry = join(directory, "transactions.ts");
  await writeFile(entry, `
import { collection, define, fail, mutation, query, transaction } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const rows=collection<number>("rows");
const add=mutation("internal.add",(ctx,amount:number)=>{const next=(ctx.get(rows,"count")??0)+amount;ctx.set(rows,"count",next);return next;});
const read=query("internal.read",ctx=>ctx.get(rows,"count")??0);
const failing=mutation("internal.fail",ctx=>{ctx.set(rows,"count",999);return fail("DELIBERATE_FAILURE","deliberate participant failure",{staged:999,tags:["🌻",null]});});
const run=transaction("internal.transaction",(plan:any)=>plan);
export default define({http:{add,read,fail:failing,run}});
`);
  const bundle = await buildBundle(entry);
  for (const group of Object.keys(groups)) {
    const node = await leader(group);
    await new FlowerAdmin(node.url, { adminToken: token }).deploy(bundle, { requestId: `deploy-${group}` });
  }
  let coordinator = await leader("a");
  const client = new FlowerClient(coordinator.url);
  const input = { calls: [
    { group: "b", method: "add", args: 10 },
    { group: "a", method: "add", args: 1 },
    { group: "a", method: "read" },
    { group: "b", method: "read" },
  ], value: { label: "two gardens" } };
  const first = await client.call("run", input, { requestId: "cross-group-success" });
  assert.deepEqual(first.value, { results: [10, 1, 1, 10], value: { label: "two gardens" } });
  assert.equal(first.duplicate, false);
  const retry = await client.call("run", input, { requestId: "cross-group-success" });
  assert.equal(retry.duplicate, true); assert.equal(retry.revision, first.revision);
  await assert.rejects(client.call("run", { ...input, value: "different" }, { requestId: "cross-group-success" }),
    error => error instanceof FlowerError && error.code === "REQUEST_ID_REUSED");
  // A participant's own failure aborts every group and reaches the caller intact,
  // whether that participant is remote or the coordinator's own group.
  const remoteAbort = { calls: [{ group: "a", method: "add", args: 100 }, { group: "b", method: "fail" }] };
  const localAbort = { calls: [{ group: "b", method: "add", args: 100 }, { group: "a", method: "fail" }] };
  await expectAborted(() => client.call("run", remoteAbort, { requestId: "cross-group-abort" }), "remote participant failure");
  await expectAborted(() => client.call("run", localAbort, { requestId: "local-group-abort" }), "local participant failure");
  await expectAborted(() => client.call("run", remoteAbort, { requestId: "cross-group-abort" }), "replayed abort keeps its failure");
  for (const node of nodes) assert.equal(await value(node), node.group === "a" ? 1 : 10, "fresh reads across replicas observe only committed work");
  await assert.rejects(client.call("run", { calls: [{ group: "b", method: "internal.add", args: 100 }] }, { requestId: "private-method" }),
    error => error instanceof FlowerError && error.code === "TRANSACTION_ABORTED");
  await assert.rejects(client.call("run", { calls: [{ group: "b", method: "run", args: { calls: [] } }] }, { requestId: "nested" }),
    error => error instanceof FlowerError && error.code === "TRANSACTION_ABORTED");
  const unauthorized = await post(coordinator, "/raft/transactions/prepare", { group: "a", transaction: {
    coordinator: "a", request_id: "forged", fingerprint: "forged",
  } }, false);
  assert.equal(unauthorized.status, 401);
  // Freeze the second participant so the coordinator is killed with a durable
  // first-group preparation, before any commit decision exists.
  for (const node of groups.b) node.runtime.child.kill("SIGSTOP");
  const interruptedPlan = { calls: [
    { group: "a", method: "add", args: 1000 },
    { group: "b", method: "add", args: 1000 },
  ] };
  const interrupted = client.call("run", interruptedPlan, { requestId: "coordinator-dies-preparing" })
    .then(value => ({ value }), error => ({ error }));
  await until("first participant durably prepared", async () => {
    const response = await post(coordinator, "/v1/query", { name: "read", args: null }, false);
    const body = await response.json();
    return response.status === 503 && body.error?.code === "TRANSACTION_PREPARED";
  });
  await stop(coordinator);
  assert.ok((await interrupted).error, "lost coordinator cannot acknowledge an unfinished commit");
  const replacement = await leader("a");
  await until("replacement aborts its prepared first participant", async () => await value(replacement) === 1);
  for (const node of groups.b) node.runtime.child.kill("SIGCONT");
  await assert.rejects(new FlowerClient(replacement.url).call("run", interruptedPlan, { requestId: "coordinator-dies-preparing" }),
    error => error instanceof FlowerError && error.code === "TRANSACTION_ABORTED");
  assert.equal(await value(await leader("b")), 10, "late preparation cannot apply after the durable abort");
  const afterFailover = await new FlowerClient(replacement.url).call("run", input, { requestId: "cross-group-success" });
  assert.equal(afterFailover.duplicate, true); assert.deepEqual(afterFailover.value, first.value);
  await expectAborted(() => new FlowerClient(replacement.url).call("run", remoteAbort, { requestId: "cross-group-abort" }),
    "the durable abort decision keeps the participant failure across coordinator failover");
  start(coordinator);
  await until("restarted coordinator catches up", async () => await value(coordinator) === 1);
  for (const node of nodes) await stop(node);
  for (const node of nodes) start(node);
  coordinator = await leader("a"); await leader("b");
  const afterRestart = await new FlowerClient(coordinator.url).call("run", input, { requestId: "cross-group-success" });
  assert.equal(afterRestart.duplicate, true);
  for (const node of nodes) await until(`${node.group}/${node.id} durable balance`, async () => await value(node) === (node.group === "a" ? 1 : 10));
  console.log("PASS: two three-node Raft groups; ordered sequential calls; atomic commit/abort with durable participant failures; method exposure; nested/auth rejection; fresh replica reads; abort recovery after coordinator death during preparation; retry across leader failure and full restart");
} catch (error) {
  for (const node of nodes) console.error(`\n${node.group}/${node.id}\n${node.runtime?.logs ?? "not started"}`);
  throw error;
} finally {
  for (const runtime of processes) if (!runtime.ended) runtime.child.kill("SIGKILL");
  await Promise.all(processes.map(runtime => runtime.exited));
  for (const server of reservations) if (server.listening) await new Promise(resolve => server.close(resolve));
  await rm(directory, { recursive: true, force: true });
}
