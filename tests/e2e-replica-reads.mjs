// Run after cargo build: node tests/e2e-replica-reads.mjs
// Uses an isolated three-node cluster and keeps default reads quorum-confirmed.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient, FlowerError } from "../sdk/index.ts";
import { createHttp2Transport } from "../sdk/http2.ts";
import { applyWatchPatch } from "../sdk/watch.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = process.env.E2E_FLOWER_BIN ? resolve(process.env.E2E_FLOWER_BIN) : join(root, "target/debug/flower");
const cluster = new LocalCluster({ nodes: 3, binary });
const transport = createHttp2Transport({ requestTimeoutMs: 15_000 });
const iterators = new Set();
let sequence = 0;

async function within(promise, label, timeoutMs = 15_000) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs} ms`)), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}

function client(node = cluster.leader, options = {}) {
  return new FlowerClient(node.url, { fetch: transport.fetch, ...options });
}
function admin(node = cluster.leader) {
  return new FlowerAdmin(node.url, { adminToken: cluster.adminToken, fetch: transport.fetch });
}
function watched(node, name) {
  const iterator = client(node).watchDeltas(name);
  iterators.add(iterator);
  return iterator;
}
async function next(iterator) {
  const event = await within(iterator.next(), "replica watch event");
  assert.equal(event.done, false, "watch ended before its next event");
  return event.value;
}
async function stop(iterator) {
  await within(iterator.return(), "cancel replica watch");
  iterators.delete(iterator);
}
async function expectWatchError(iterator, code, status) {
  // A local time query can have one coalesced event buffered before revocation.
  await assert.rejects(within((async () => {
    for (let remaining = 32; remaining > 0; remaining--) {
      const event = await iterator.next();
      assert.equal(event.done, false, "expected a terminal error event");
    }
    throw new Error("watch kept emitting after its expected terminal error");
  })(), `watch ${code}`), (error) => error instanceof FlowerError && error.code === code && error.status === status);
  await stop(iterator);
}
async function change(counter) {
  return client().mutate("counter.change", counter, { requestId: `replica-change-${sequence++}`, signal: AbortSignal.timeout(15_000) });
}
async function assertFreshEverywhere(receipt) {
  const values = await Promise.all(cluster.members.map((node) => client(node).query("counter.read", null, { signal: AbortSignal.timeout(15_000) })));
  for (const value of values) {
    assert.ok(value.revision >= receipt.revision, "read after acknowledgement must include that committed revision");
    assert.deepEqual(value.value, receipt.value, "every replica must evaluate the acknowledged state");
  }
  return values;
}

function source(exposed = true) {
  return `import { collection, define, mutation, query } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const rows = collection<any>("replica.records");
const change = mutation("internal.replica.change", (ctx, counter: number) => {
  const row = ctx.get(rows, "value") ?? { counter: 0, padding: "unchanged ".repeat(1024) };
  row.counter = counter;
  ctx.set(rows, "value", row);
  return row;
});
const read = query("internal.replica.read", ctx => ctx.get(rows, "value"));
const local = query("internal.replica.local", { consistency: "replica-local" }, ctx => ctx.get(rows, "value"));
const clock = query("internal.replica.clock", { consistency: "replica-local" }, ctx => ({ now: ctx.now(), counter: ctx.get(rows, "value").counter }));
export default define({ http: {
  "counter.change": change, "counter.clock": clock,
  ${exposed ? '"counter.read": read, "counter.local": local,' : ""}
} });`;
}

async function kill(node) {
  node.process.intentional = true;
  node.process.child.kill("SIGKILL");
  await within(node.process.exited, `stop replica ${node.id}`, 5_000);
}

const interrupt = () => { void transport.close(); void cluster.close(); };
process.once("SIGINT", interrupt);
process.once("SIGTERM", interrupt);

try {
  await cluster.start();
  const fixture = join(cluster.directory, "replica-reads.ts");
  await writeFile(fixture, source());
  const bundle = await buildBundle(fixture, { initialization: "static" });
  await admin().deploy(bundle, { requestId: "replica-deploy" });
  let receipt = await change(1);
  await assertFreshEverywhere(receipt);

  // SDK routing uses every replica without adding consistency to HTTP bodies.
  const urls = [];
  const distributed = client(cluster.leader, { queryUrls: cluster.members.map((node) => node.url),
    fetch: (url, init) => { urls.push(url); return transport.fetch(url, init); } });
  await Promise.all(Array.from({ length: 6 }, () => distributed.query("counter.read")));
  for (const node of cluster.members) assert.equal(urls.filter((url) => url === node.url + "/v1/query").length, 2);
  await distributed.mutate("counter.change", 2, { requestId: "replica-sdk-mutation" });
  assert.equal(urls.at(-1), cluster.url + "/v1/mutate");
  receipt = await change(3);
  await assertFreshEverywhere(receipt);

  const followers = cluster.members.filter((node) => node !== cluster.leader);
  const watches = followers.map((node) => ({ node, iterator: watched(node, "counter.read"), revision: 0, sequence: 0 }));
  for (const watch of watches) {
    const initial = await next(watch.iterator);
    assert.equal(initial.type, "snapshot");
    assert.equal(initial.sequence, 0);
    assert.deepEqual(initial.value, receipt.value);
    watch.revision = initial.revision;
  }
  for (const counter of [4, 5, 6]) {
    receipt = await change(counter);
    await assertFreshEverywhere(receipt);
    const deltas = await Promise.all(watches.map(async (watch) => {
      const delta = await next(watch.iterator);
      assert.equal(delta.type, "patch");
      assert.equal(delta.baseSequence, watch.sequence);
      assert.equal(delta.sequence, ++watch.sequence);
      assert.ok(delta.revision > watch.revision && delta.revision >= receipt.revision);
      assert.deepEqual(delta.patch, [{ op: "replace", path: "/counter", value: counter }]);
      watch.revision = delta.revision;
      return delta;
    }));
    assert.deepEqual(deltas[0], deltas[1], "followers emit the same committed deltas");
  }

  // A client cannot lower consistency for either reads or SSE.
  for (const path of ["/v1/query", "/v1/watch"]) {
    const response = await transport.fetch(followers[0].url + path, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "counter.read", args: null, consistency: "replica-local" }), signal: AbortSignal.timeout(15_000),
    });
    assert.equal(response.status, 400);
    assert.equal((await response.json()).error.code, "INPUT_INVALID");
  }

  const locals = followers.map((node) => watched(node, "counter.local"));
  await Promise.all(locals.map(next));
  await writeFile(fixture, source(false));
  await admin().deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "replica-revoke" });
  await Promise.all([...watches.map(({ iterator }) => iterator), ...locals].map((iterator) => expectWatchError(iterator, "METHOD_NOT_FOUND", 404)));
  await admin().deploy(bundle, { requestId: "replica-restore" });
  receipt = await change(7);
  await assertFreshEverywhere(receipt);

  const survivor = followers[0];
  const stopped = cluster.members.filter((node) => node !== survivor);
  const localClock = watched(survivor, "counter.clock");
  const clockInitial = await next(localClock);
  const fresh = watched(survivor, "counter.read");
  await next(fresh);
  const interrupted = watched(stopped[0], "counter.read");
  await next(interrupted);
  const disconnected = interrupted.next().then((value) => ({ value }), (error) => ({ error }));
  await Promise.all(stopped.map(kill));
  const quorumLostAt = Date.now();
  cluster.leader = null;
  const lost = await within(disconnected, "watch disconnect on killed replica");
  assert.ok(lost.error || lost.value?.done, "a lost TCP connection cannot manufacture watch continuity");
  await stop(interrupted);

  const localRead = await client(survivor).query("counter.local", null, { signal: AbortSignal.timeout(3_000) });
  assert.deepEqual(localRead.value, receipt.value);
  assert.equal(localRead.revision, receipt.revision);
  // The clock-driven local watch proves the stream still evaluates with no quorum.
  let clockValue = clockInitial.value;
  let clockSequence = clockInitial.sequence;
  for (let sample = 0; sample < 16 && clockValue.now <= quorumLostAt; sample++) {
    const event = await next(localClock);
    assert.ok(event.sequence > clockSequence);
    assert.equal(event.revision, localRead.revision);
    clockValue = event.type === "snapshot" ? event.value : applyWatchPatch(clockValue, event.patch);
    clockSequence = event.sequence;
  }
  assert.ok(clockValue.now > quorumLostAt, "local watch must evaluate after quorum was lost, not just drain old buffered events");
  assert.equal(clockValue.counter, receipt.value.counter);
  await Promise.all([
    assert.rejects(client(survivor).query("counter.read", null, { signal: AbortSignal.timeout(15_000) }),
      (error) => error instanceof FlowerError && error.status === 503 && error.code === "UNAVAILABLE"),
    expectWatchError(fresh, "UNAVAILABLE", 503),
  ]);

  // Restore the exact durable directories, then reconnect with a fresh sequence.
  for (const node of stopped) cluster._startNode(node);
  await cluster.discoverLeader();
  await cluster._until("all restarted replicas answer metrics", async () => {
    await Promise.all(cluster.members.map((node) => cluster.metrics(node)));
    return true;
  });
  receipt = await change(8);
  await assertFreshEverywhere(receipt);
  const reconnect = watched(stopped[0], "counter.read");
  const initial = await next(reconnect);
  assert.equal(initial.type, "snapshot");
  assert.equal(initial.sequence, 0);
  assert.deepEqual(initial.value, receipt.value);
  await stop(reconnect);
  await stop(localClock);
  console.log("PASS: fresh reads on every replica, SDK distribution, follower SSE deltas and revocation, caller downgrade rejection, replica-local reads/watch during lost quorum, and fresh reconnect after restart");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await Promise.allSettled([...iterators].map(stop));
  await transport.close();
  await cluster.close();
  process.removeListener("SIGINT", interrupt);
  process.removeListener("SIGTERM", interrupt);
}
