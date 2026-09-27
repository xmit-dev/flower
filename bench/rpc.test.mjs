import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createServer as createHttp2Server } from "node:http2";
import test from "node:test";
import { BenchmarkClient, RpcError } from "./rpc.mjs";
import { Stats } from "./metrics.mjs";

async function fixture(t, handler, options = {}) {
  const server = options.http2 ? createHttp2Server(handler) : createServer(handler);
  const sessions = new Set();
  server.on("session", (session) => {
    sessions.add(session);
    session.on("error", () => {});
    session.on("close", () => sessions.delete(session));
  });
  server.on("stream", (stream) => stream.on("error", () => {}));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const cluster = { url: `http://127.0.0.1:${server.address().port}`, leader: {}, discoverLeader: async () => {} };
  const phase = { stats: new Stats(), ended: 0 };
  const controller = new AbortController();
  const client = new BenchmarkClient(cluster, { requestTimeoutMs: 100, retryBudgetMs: 1_000, ...options }, controller.signal, () => phase);
  t.after(async () => {
    await client.close();
    for (const session of sessions) session.destroy();
    await new Promise((resolve) => { server.closeAllConnections?.(); server.close(resolve); });
  });
  return { client, cluster, phase, controller };
}

async function body(request) {
  let text = "";
  for await (const chunk of request) text += chunk;
  return text;
}

function send(response, status, data) {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(data));
}

test("uncertain response retries the exact request and records one logical invocation", async (t) => {
  const bodies = [];
  const { client, phase } = await fixture(t, async (request, response) => {
    bodies.push(await body(request));
    if (bodies.length === 1) request.socket.destroy(); // Server may already have committed.
    else send(response, 200, { revision: 7, value: { pizzas: 2 }, duplicate: true });
  });
  const result = await client.call("pizza.order", { id: "one" });
  assert.equal(result.duplicate, true);
  assert.equal(bodies.length, 2);
  assert.equal(bodies[0], bodies[1]);
  assert.match(JSON.parse(bodies[0]).requestId, /^bench-/);
  const stats = phase.stats.snapshot(1_000);
  assert.equal(stats.attempts, 2);
  assert.equal(stats.retries, 1);
  assert.equal(stats.operations.count, 1);
  assert.equal(stats.operations.completed, 1);
});

test("application errors are not retried and explicit replays have a separate logical series", async (t) => {
  let count = 0;
  const failure = { code: "LEASE_LOST", message: "Job lease is missing, expired, or held by another claim", details: { token: 3 } };
  const { client, phase } = await fixture(t, async (request, response) => {
    await body(request);
    count++;
    send(response, 422, { error: { code: "EVALUATION_FAILED", message: `LEASE_LOST: ${failure.message}`, failure } });
  });
  await assert.rejects(client.call("pizza.deliver", {}, { replay: true }), (error) => error instanceof RpcError && error.status === 422 &&
    error.code === "EVALUATION_FAILED" && JSON.stringify(error.failure) === JSON.stringify(failure));
  assert.equal(count, 1);
  assert.equal(phase.stats.snapshot(1_000).operations.perMethod["pizza.deliver.replay"].failed, 1);
});

test("response-body stalls are aborted and the total retry budget is bounded", async (t) => {
  const { client, phase } = await fixture(t, async (request, response) => {
    await body(request);
    response.writeHead(200, { "content-type": "application/json" });
    response.write('{"revision":'); // Headers alone must not satisfy the deadline.
  }, { requestTimeoutMs: 30, retryBudgetMs: 120 });
  const started = performance.now();
  await assert.rejects(client.call("pizza.tip", {}), (error) => error.code === "RETRY_EXHAUSTED");
  assert.ok(performance.now() - started < 1_000);
  const stats = phase.stats.snapshot(1_000);
  assert.ok(stats.failures >= 1);
  assert.equal(stats.operations.failed, 1);
});

test("a shared leader lookup cannot extend the caller's retry budget", async (t) => {
  const { client, cluster, phase } = await fixture(t, () => {}, { retryBudgetMs: 50 });
  cluster.leader = null;
  cluster.discoverLeader = () => new Promise(() => {});
  await assert.rejects(client.call("pizza.shop", null, { query: true }), (error) => error.code === "RETRY_EXHAUSTED");
  const stats = phase.stats.snapshot(1_000);
  assert.equal(stats.attempts, 0);
  assert.equal(stats.operations.failed, 1);
});

test("a drain cancellation aborts an in-flight body despite a much longer retry budget", async (t) => {
  const { client, phase } = await fixture(t, async (request, response) => {
    await body(request);
    response.writeHead(200, { "content-type": "application/json" });
    response.write('{"revision":');
  }, { requestTimeoutMs: 5_000, retryBudgetMs: 300_000 });
  const deadline = AbortSignal.timeout(40);
  const started = performance.now();
  await assert.rejects(client.call("pizza.world", null, { query: true, signal: deadline }), (error) => error === deadline.reason);
  assert.ok(performance.now() - started < 1_000);
  assert.equal(phase.stats.snapshot(1_000).errors.aborted, 1);
});

test("HTTP/2 retries an uncertain response with the same body and ID on a replacement session", async (t) => {
  const bodies = [];
  const { client, phase } = await fixture(t, async (request, response) => {
    assert.equal(request.httpVersionMajor, 2);
    bodies.push(await body(request));
    if (bodies.length === 1) request.stream.session.destroy();
    else send(response, 200, { revision: 7, value: { pizzas: 2 }, duplicate: true });
  }, { http2: true });
  assert.equal((await client.call("pizza.order", { id: "one" })).duplicate, true);
  assert.equal(bodies.length, 2);
  assert.equal(bodies[0], bodies[1]);
  const stats = phase.stats.snapshot(1_000);
  assert.equal(stats.attempts, 2);
  assert.equal(stats.operations.completed, 1);
  assert.equal(stats.retries, 1);
});

test("HTTP/2 response-body stalls obey per-request and total retry deadlines", async (t) => {
  const { client, phase } = await fixture(t, async (request, response) => {
    await body(request);
    response.writeHead(200, { "content-type": "application/json" });
    response.write('{"revision":');
  }, { http2: true, requestTimeoutMs: 30, retryBudgetMs: 120 });
  const started = performance.now();
  await assert.rejects(client.call("pizza.tip", {}), (error) => error.code === "RETRY_EXHAUSTED");
  assert.ok(performance.now() - started < 1_000);
  assert.ok(phase.stats.snapshot(1_000).errors.timeout >= 1);
});

test("HTTP/2 in-flight calls respect drain cancellation and application errors are not retried", async (t) => {
  let failures = 0;
  const { client, phase } = await fixture(t, async (request, response) => {
    const input = JSON.parse(await body(request));
    if (input.name === "pizza.deliver") {
      failures++;
      send(response, 422, { error: { message: "LEASE_LOST: lost", code: "EVALUATION_FAILED", failure: { code: "LEASE_LOST", message: "lost" } } });
    } else { response.writeHead(200); response.write('{"revision":'); }
  }, { http2: true, requestTimeoutMs: 5_000, retryBudgetMs: 300_000 });
  await assert.rejects(client.call("pizza.deliver", {}), (error) => error.status === 422 && error.failure?.code === "LEASE_LOST");
  assert.equal(failures, 1);
  const deadline = AbortSignal.timeout(40);
  await assert.rejects(client.call("pizza.world", null, { query: true, signal: deadline }), (error) => error === deadline.reason);
  assert.equal(phase.stats.snapshot(1_000).errors.aborted, 1);
});

test("discovery of a different serving leader retries immediately with the same request", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (_url, options) => {
    requests.push(options.body);
    return new Response(JSON.stringify(requests.length <= 10
      ? { error: { code: "NOT_LEADER", message: "leader changed" } }
      : { revision: 11, value: "recovered" }), { status: requests.length <= 10 ? 503 : 200 });
  });
  const cluster = {
    leader: { id: 1 }, url: "http://unused.invalid",
    async discoverLeader() { this.leader = { id: this.leader.id + 1 }; return this.leader; },
  };
  const phase = { stats: new Stats(), ended: 0 };
  const client = new BenchmarkClient(cluster, { requestTimeoutMs: 100, retryBudgetMs: 1_000 },
    new AbortController().signal, () => phase);
  // Ten unnecessary linear backoffs would exhaust this budget (1.375s).
  assert.equal((await client.call("pizza.tip", {}, { requestId: "stable-id" })).value, "recovered");
  assert.equal(requests.length, 11);
  assert.equal(new Set(requests).size, 1);
  assert.equal(phase.stats.snapshot(1_000).operations.completed, 1);
});

test("retryable overload at the same leader retains bounded backoff", async (t) => {
  let attempts = 0;
  t.mock.method(globalThis, "fetch", async () => {
    attempts++;
    return new Response(JSON.stringify({ error: { code: "OVERLOADED", message: "busy" } }), { status: 503 });
  });
  const cluster = { leader: { id: 1 }, url: "http://unused.invalid", async discoverLeader() { return this.leader; } };
  const phase = { stats: new Stats(), ended: 0 };
  const client = new BenchmarkClient(cluster, { requestTimeoutMs: 100, retryBudgetMs: 90 },
    new AbortController().signal, () => phase);
  await assert.rejects(client.call("pizza.tip", {}), (error) => error.code === "RETRY_EXHAUSTED");
  assert.ok(attempts >= 1 && attempts <= 5, `same-leader overload spun through ${attempts} attempts`);
});

function replicaFixture(options = {}) {
  const members = [1, 2, 3].map((id) => ({ id, url: `http://node-${id}.invalid`, process: { ended: false, intentional: false } }));
  const cluster = { members, leader: members[1], adminToken: "routing-test", discoveries: 0,
    get url() { return this.leader.url; },
    async discoverLeader() { this.discoveries++; return this.leader; },
  };
  const phase = { stats: new Stats(), ended: 0 };
  const client = new BenchmarkClient(cluster,
    { queryRouting: "replicas", requestTimeoutMs: 100, retryBudgetMs: 1_000, ...options },
    new AbortController().signal, () => phase);
  return { client, cluster, phase };
}

test("fresh queries distribute fairly while mutations and deployment stay on the leader", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url, options) => {
    requests.push({ url, ...options });
    return Response.json({ revision: 9, value: "ok", duplicate: false });
  });
  const { client, cluster, phase } = replicaFixture();
  await Promise.all(Array.from({ length: 9 }, () => client.call("pizza.shop", "shop-0", { query: true })));
  assert.deepEqual(requests.map(({ url }) => url), [1, 2, 3, 1, 2, 3, 1, 2, 3].map((id) => `http://node-${id}.invalid/v1/query`));
  for (const request of requests) assert.deepEqual(JSON.parse(request.body), { name: "pizza.shop", args: "shop-0" });
  await client.call("pizza.tip", { amount: 1 }, { requestId: "fixed-id" });
  await client.deploy({ hash: "bundle", javascript: "fixture" });
  assert.equal(requests[9].url, cluster.url + "/v1/call");
  assert.equal(JSON.parse(requests[9].body).requestId, "fixed-id");
  assert.equal(requests[10].url, cluster.url + "/admin/deploy");
  assert.equal(requests[10].headers.authorization, "Bearer routing-test");
  assert.equal(cluster.discoveries, 0);
  assert.deepEqual(client.routingSummary(), { mode: "replicas", consistency: "fresh", auditConsistency: "fresh",
    nodes: cluster.members.map(({ id, url }) => ({ id, url, attempts: 3, completed: 3, failures: 0 })) });
  assert.equal(phase.stats.snapshot(1_000).operations.completed, 11);
});

test("replica queries skip stopped or intentionally killed members and do not require a known leader", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url) => { requests.push(url); return Response.json({ revision: 1, value: null }); });
  const { client, cluster } = replicaFixture();
  cluster.members[0].process.ended = true;
  cluster.members[1].process.intentional = true;
  cluster.leader = null;
  await client.call("pizza.shop", null, { query: true });
  assert.deepEqual(requests, [cluster.members[2].url + "/v1/query"]);
  assert.equal(cluster.discoveries, 0);
  cluster.members[0].process = { ended: false, intentional: false };
  await Promise.all(Array.from({ length: 4 }, () => client.call("pizza.shop", null, { query: true })));
  assert.deepEqual(requests.slice(1), [1, 3, 1, 3].map((id) => `http://node-${id}.invalid/v1/query`));
});

test("query retries rotate through live replicas with identical bodies and one logical statistic", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url, options) => {
    requests.push({ url, body: options.body });
    if (requests.length === 1) throw new TypeError("connection reset");
    if (requests.length === 2) return Response.json({ error: { code: "UNAVAILABLE", message: "waiting for quorum" } }, { status: 503 });
    return Response.json({ revision: 42, value: "fresh" });
  });
  const { client, cluster, phase } = replicaFixture();
  assert.equal((await client.call("pizza.shop", { shop: "shop-0" }, { query: true })).value, "fresh");
  assert.deepEqual(requests.map(({ url }) => url), cluster.members.map(({ url }) => url + "/v1/query"));
  assert.equal(new Set(requests.map(({ body }) => body)).size, 1);
  assert.equal(cluster.discoveries, 0);
  const stats = phase.stats.snapshot(1_000);
  assert.equal(stats.attempts, 3);
  assert.equal(stats.retries, 2);
  assert.equal(stats.operations.count, 1);
  assert.equal(stats.operations.completed, 1);
});

test("an unavailable replica set backs off after one full cycle", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url) => {
    requests.push(url);
    return Response.json({ error: { code: "UNAVAILABLE", message: "no quorum" } }, { status: 503 });
  });
  const { client, phase } = replicaFixture({ retryBudgetMs: 60 });
  await assert.rejects(client.call("pizza.shop", null, { query: true }), (error) => error.code === "RETRY_EXHAUSTED");
  assert.deepEqual(requests, [1, 2, 3].map((id) => `http://node-${id}.invalid/v1/query`));
  assert.equal(phase.stats.snapshot(1_000).operations.failed, 1);
});

test("leader query mode preserves leader routing and replica mode never retries invalid successful responses", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url) => { requests.push(url); return Response.json({ revision: 1, value: null }); });
  const { client, cluster } = replicaFixture({ queryRouting: "leader" });
  await Promise.all(Array.from({ length: 3 }, () => client.call("pizza.shop", null, { query: true })));
  assert.deepEqual(requests, Array(3).fill(cluster.url + "/v1/query"));
  t.mock.method(globalThis, "fetch", async (url) => { requests.push(url); return Response.json({ value: "missing revision" }); });
  const distributed = replicaFixture().client;
  await assert.rejects(distributed.call("pizza.shop", null, { query: true }), (error) => error.code === "INVALID_RESPONSE" && error.status === 200);
  assert.equal(requests.length, 4, "intact malformed responses must remain fatal correctness failures");
});

test("replica backoff is shared by new callers until the replica can be retried", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url) => {
    requests.push(url);
    return requests.length === 1
      ? Response.json({ error: { code: "UNAVAILABLE", message: "still catching up" } }, { status: 503 })
      : Response.json({ revision: 1, value: null });
  });
  const { client, cluster } = replicaFixture();
  await client.call("pizza.shop", null, { query: true });
  const recovering = cluster.members[0].url;
  assert.ok(client.replicaCooldowns.has(recovering));
  // Pin the deadline for this assertion, independent of test-machine speed.
  client.replicaCooldowns.set(recovering, Infinity);
  await Promise.all(Array.from({ length: 12 }, () => client.call("pizza.shop", null, { query: true })));
  assert.equal(requests.filter((url) => url.startsWith(recovering + "/")).length, 1);
  client.replicaCooldowns.delete(recovering);
  await Promise.all(Array.from({ length: 3 }, () => client.call("pizza.shop", null, { query: true })));
  assert.equal(requests.filter((url) => url.startsWith(recovering + "/")).length, 2);
});

test("startup freshness barriers stay pinned to each replica through retry", async (t) => {
  const requests = [];
  let first = true;
  t.mock.method(globalThis, "fetch", async (url) => {
    requests.push(url);
    if (first) { first = false; return Response.json({ error: { message: "catching up" } }, { status: 503 }); }
    return Response.json({ revision: 20, value: { initialized: true } });
  });
  const { client, cluster } = replicaFixture();
  const result = await client.call("pizza.shop", ["tenant-a", "store-0"], { query: true, queryNode: cluster.members[0] });
  assert.equal(result.revision, 20);
  assert.deepEqual(requests, ["http://node-1.invalid/v1/query", "http://node-1.invalid/v1/query"]);
  assert.equal(cluster.discoveries, 0);
  assert.throws(() => client.call("pizza.tip", {}, { queryNode: cluster.members[0] }), /only valid for queries/);
});
