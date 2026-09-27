// Run after cargo build: node tests/e2e-http2.mjs
// Raw Node HTTP/2 proves the server contract independently of the SDK adapter.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { connect, constants } from "node:http2";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerClient, FlowerError } from "../sdk/client.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = process.env.E2E_FLOWER_BIN ? resolve(process.env.E2E_FLOWER_BIN) : join(root, "target/debug/flower");
const cluster = new LocalCluster({ nodes: 3, binary });
const transport = createHttp2Transport({ requestTimeoutMs: 15_000 });
const sessions = new Set();
const streamIds = new Set();
let active = 0;
let peakActive = 0;

function session(url) {
  const client = connect(url);
  // Connection errors also reach the affected streams. Keep the session event
  // handled when the failover test kills a process with an idle connection.
  client.on("error", () => {});
  sessions.add(client);
  return client;
}

function request(client, path, body, token, peerHeaders = {}) {
  return new Promise((resolve, reject) => {
    const stream = client.request({
      ":method": body === undefined ? "GET" : "POST", ":path": path,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
      ...(token === undefined ? {} : { authorization: `Bearer ${token}` }),
      ...peerHeaders,
    });
    active++;
    peakActive = Math.max(peakActive, active);
    let status;
    let encoded = "";
    let settled = false;
    const timer = setTimeout(() => {
      finish(new Error(`HTTP/2 ${path} timed out`));
      stream.close(constants.NGHTTP2_CANCEL);
    }, 15_000);
    function finish(error, value) {
      if (settled) return;
      settled = true;
      active--;
      clearTimeout(timer);
      if (error) reject(error); else resolve(value);
    }
    stream.setEncoding("utf8");
    stream.on("response", (headers) => {
      streamIds.add(stream.id);
      status = headers[":status"];
    });
    stream.on("data", (chunk) => { encoded += chunk; });
    stream.on("error", (error) => finish(error));
    stream.on("aborted", () => finish(new Error(`HTTP/2 ${path} stream aborted`)));
    stream.on("close", () => { if (!settled) finish(new Error(`HTTP/2 ${path} closed before its response ended`)); });
    stream.on("end", () => {
      try { finish(null, { status, value: JSON.parse(encoded) }); }
      catch (error) { finish(error); }
    });
    stream.end(body === undefined ? undefined : JSON.stringify(body));
  });
}

async function success(client, path, body, token) {
  const response = await request(client, path, body, token);
  assert.equal(response.status, 200, JSON.stringify(response.value));
  return response.value;
}

// The orders example plus one method whose structured failure carries JSON details.
function source() {
  const sdk = JSON.stringify(join(root, "sdk/index.ts"));
  const orders = JSON.stringify(join(root, "examples/orders.ts"));
  return `import { define, fail, mutation, v } from ${sdk};
import { orders, lines, subtotal, total, privateReset, createOrder, updateLine, updateShipping, getOrder } from ${orders};
const reject = mutation("internal.order.reject", { args: v.object({ orderId: v.string({ min: 1 }), reason: v.string() }) }, (ctx, args) => {
  ctx.set(orders, args.orderId, { shippingCents: 1 }); // Staged, then rolled back by the failure.
  return fail("ORDER_REJECTED", "Order " + args.orderId + " rejected: " + args.reason, {
    orderId: args.orderId, reasons: [args.reason, "🌻"], retryable: false, limits: { lines: 3, cents: 12.5 }, note: null,
  });
});
export default define({
  collections: [lines],
  definitions: [subtotal, total, privateReset],
  http: { "order.create": createOrder, "order.updateLine": updateLine, "order.updateShipping": updateShipping, "order.get": getOrder, "order.reject": reject },
});`;
}
const rejection = { orderId: "h2-rejected", reason: "burnt crust" };
const rejectionFailure = {
  code: "ORDER_REJECTED", message: "Order h2-rejected rejected: burnt crust",
  details: { orderId: "h2-rejected", reasons: ["burnt crust", "🌻"], retryable: false, limits: { lines: 3, cents: 12.5 }, note: null },
};
async function expectFailure(label, operation, failure) {
  await assert.rejects(operation(), (error) => {
    assert.ok(error instanceof FlowerError, `${label}: ${error}`);
    assert.equal(error.status, 422, label);
    assert.equal(error.code, "EVALUATION_FAILED", label);
    assert.equal(error.message, `${failure.code}: ${failure.message}`, label);
    assert.deepEqual(error.failure, failure, label);
    return true;
  });
}
async function identityRevision(url) {
  const response = await fetch(url + "/v1/identity", {
    method: "POST", headers: { "content-type": "application/json" }, body: "{}", signal: AbortSignal.timeout(15_000),
  });
  assert.equal(response.status, 200);
  return (await response.json()).revision;
}

const interrupt = () => {
  for (const client of sessions) client.destroy();
  void transport.close();
  void cluster.close();
};
process.once("SIGINT", interrupt);
process.once("SIGTERM", interrupt);

try {
  await cluster.start();
  const seed = cluster.members.find((node) => node.id !== cluster.leader.id);
  const client = session(seed.url);
  await success(client, "/health");
  assert.equal(client.socket.remotePort, Number(new URL(seed.url).port));
  assert.equal((await request(client, "/raft/metrics")).status, 401);
  const metrics = await success(client, "/raft/metrics", undefined, cluster.adminToken);
  assert.equal(metrics.id, seed.id);
  assert.notEqual(metrics.id, cluster.leader.id, "all public writes initially enter a follower");
  const membership = await success(client, "/raft/membership", undefined, cluster.adminToken);
  const c = membership.compatibility;
  const contract = `raft${c.raftWire}-state${c.stateMachine}-snapshot${c.snapshotFormat}-value${c.valueFormat}-qjs${c.quickjsSha256}`;
  const peerHeaders = {
    "x-flower-target-node-id": String(seed.id), "x-flower-target-address": seed.address,
    "x-flower-source-node-id": String(cluster.leader.id), "x-flower-source-address": cluster.leader.address,
    "x-flower-compatibility": contract, "x-flower-forward-kind": "mutate",
  };
  const probe = { name: "not.exposed", args: null, requestId: "h2-internal-probe" };
  assert.equal((await request(client, "/raft/forward", probe)).status, 401);
  assert.equal((await request(client, "/raft/forward", probe, cluster.adminToken)).status, 409);
  assert.equal((await request(client, "/raft/forward", probe, cluster.adminToken,
    { ...peerHeaders, "x-flower-compatibility": "incompatible" })).status, 426);
  assert.equal((await request(client, "/raft/forward", probe, cluster.adminToken,
    { ...peerHeaders, "x-flower-source-address": "127.0.0.1:1" })).status, 403);
  assert.equal((await request(client, "/raft/forward", probe, cluster.adminToken, peerHeaders)).status, 503,
    "internal forwarding never forwards recursively from a follower");

  const fixture = join(cluster.directory, "orders-with-rejection.ts");
  await writeFile(fixture, source());
  const bundle = await buildBundle(fixture);
  const deployment = { requestId: "h2-deploy", bundle };
  assert.equal((await request(client, "/admin/deploy", deployment)).status, 401);
  const deployed = await success(client, "/admin/deploy", deployment, cluster.adminToken);
  // order.total is materialized for each order; its private backfill marker is
  // one maintenance commit. Settle it so later revision equalities stay exact.
  for (const deadline = Date.now() + 15_000; await identityRevision(seed.url) === deployed.revision;) {
    assert.ok(Date.now() < deadline, "maintenance did not commit the materialization marker");
    await delay(50);
  }
  assert.equal(await identityRevision(seed.url), deployed.revision + 1);
  await success(client, "/v1/call", {
    name: "order.create", requestId: "h2-create", args: {
      orderId: "h2-order", shippingCents: 5,
      lines: [{ id: "h2-line", quantity: 1, unitCents: 7 }],
    },
  });
  const calls = Array.from({ length: 16 }, (_, index) => ({
    name: "order.updateLine", args: { lineId: "h2-line", quantity: index + 2 },
    requestId: `h2-update-${index}`,
  }));
  const results = await Promise.all(calls.map(async (call) => {
    // These streams all share one session/socket and can complete independently.
    const [receipt, query] = await Promise.all([
      success(client, "/v1/mutate", call),
      success(client, "/v1/query", { name: "order.get", args: "h2-order" }),
    ]);
    assert.equal(receipt.value.subtotal, call.args.quantity * 7);
    assert.equal(receipt.value.total, call.args.quantity * 7 + 5);
    assert.equal(query.duplicate, false);
    assert.equal(query.value.total, query.value.subtotal + 5);
    return receipt;
  }));
  assert.equal(new Set(results.map(({ revision }) => revision)).size, calls.length);
  assert.ok(peakActive >= 32, "queries and mutations must overlap on the HTTP/2 session");
  assert.ok(streamIds.size >= 32, "the session must use separate HTTP/2 streams");
  const last = results.reduce((a, b) => a.revision > b.revision ? a : b);
  const current = await success(client, "/v1/query", { name: "order.get", args: "h2-order" });
  assert.deepEqual(current.value, last.value);
  assert.equal(current.revision, last.revision);
  const staleCas = await request(client, "/v1/mutate", {
    ...calls[0], requestId: "h2-stale-cas", expectedRevision: current.revision - 1,
  });
  assert.equal(staleCas.status, 409);
  assert.equal(staleCas.value.error.code, "REVISION_CONFLICT");
  for (const [index, call] of calls.entries()) {
    const retry = await success(client, "/v1/call", call);
    assert.deepEqual(retry, { ...results[index], duplicate: true });
  }
  assert.equal((await request(client, "/v1/call", { ...calls[0], args: { lineId: "h2-line", quantity: 999 } })).status, 409);
  assert.equal((await request(client, "/v1/query", { name: "internal.order.read", args: "h2-order" })).status, 404);

  // fail(code, message, details) reaches every caller intact: raw HTTP/2 streams,
  // the SDK's HTTP/2 transport and HTTP/1.1, through the follower's forwarding
  // to the leader as well as directly on the leader.
  const rawRejection = await request(client, "/v1/mutate", { name: "order.reject", args: rejection, requestId: "h2-reject-raw" });
  assert.equal(rawRejection.status, 422);
  assert.deepEqual(rawRejection.value, { error: {
    code: "EVALUATION_FAILED", message: `${rejectionFailure.code}: ${rejectionFailure.message}`, failure: rejectionFailure,
  } });
  const h2Client = new FlowerClient(seed.url, { fetch: transport.fetch });
  const h1Client = new FlowerClient(seed.url);
  const leaderClient = new FlowerClient(cluster.leader.url);
  for (const [label, sdk] of [["HTTP/2 follower", h2Client], ["HTTP/1.1 follower", h1Client], ["HTTP/1.1 leader", leaderClient]]) {
    await expectFailure(`${label} mutate`, () => sdk.mutate("order.reject", rejection, { requestId: `h2-reject-${label}` }), rejectionFailure);
    await expectFailure(`${label} generic call`, () => sdk.call("order.reject", rejection), rejectionFailure);
    await expectFailure(`${label} query`, () => sdk.query("order.get", rejection.orderId),
      { code: "ORDER_NOT_FOUND", message: "Order h2-rejected does not exist" });
    await assert.rejects(sdk.mutate("order.updateLine", { lineId: "h2-line", quantity: -1 }), (error) => {
      assert.equal(error.status, 422);
      assert.deepEqual(error.failure, { code: "INVALID_ARGUMENT", message: "quantity: must be at least 0", details: { path: ["quantity"] } });
      return true;
    });
  }
  const h2Failure = await request(client, "/v1/query", { name: "order.get", args: rejection.orderId });
  assert.equal(h2Failure.status, 422);
  assert.deepEqual(h2Failure.value.error.failure, { code: "ORDER_NOT_FOUND", message: "Order h2-rejected does not exist" },
    "the staged write was rolled back with the failure");

  // The same application and listener continue to serve existing HTTP/1 clients.
  const h1 = await fetch(seed.url + "/v1/query", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "order.get", args: "h2-order" }),
    signal: AbortSignal.timeout(15_000),
  });
  assert.equal(h1.status, 200);
  assert.deepEqual(await h1.json(), current);

  const oldLeaderStopped = cluster.leader.process.exited;
  const recovery = cluster.crashLeaderAndRecover();
  await oldLeaderStopped;
  // Keep the exact same seed and socket, issuing a mutation while an election
  // is in progress. The server owns discovery and repeats the original ID.
  const duringElection = success(client, "/v1/mutate", calls[0]);
  const [event, uncertainRetry] = await Promise.all([recovery, duringElection]);
  assert.notEqual(event.newLeader, event.oldLeader);
  assert.deepEqual(uncertainRetry, { ...results[0], duplicate: true });
  assert.equal(client.socket.remotePort, Number(new URL(seed.url).port));
  const recovered = await success(client, "/v1/query", { name: "order.get", args: "h2-order" });
  assert.deepEqual(recovered, current);
  const retry = await success(client, "/v1/mutate", calls[0]);
  assert.deepEqual(retry, { ...results[0], duplicate: true });
  const after = await success(client, "/v1/call", {
    name: "order.updateLine", args: { lineId: "h2-line", quantity: 20 }, requestId: "h2-after-election",
    expectedRevision: current.revision,
  });
  assert.equal(after.revision, current.revision + 1);
  assert.equal(after.value.total, 145);
  console.log("PASS: HTTP/2 multiplexed methods through a follower, forwarding auth/identity/no loops, allowlist, receipts, structured failures over h2/HTTP/1 and forwarding, HTTP/1, same-seed leader failover and durable replay");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  for (const client of sessions) client.destroy();
  await transport.close();
  await cluster.close();
  process.removeListener("SIGINT", interrupt);
  process.removeListener("SIGTERM", interrupt);
}
