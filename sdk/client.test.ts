import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test from "node:test";
import type { TestContext } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { collection, define, mutation, query, transaction, v } from "./index.ts";
import type { TransactionResult } from "./index.ts";
import { backoff, FlowerAdmin, FlowerClient, FlowerError, isTransient } from "./client.ts";
import type { FlowerFetch, FlowerRequestInit, MutationResult, QueryResult } from "./client.ts";

const encoder = new TextEncoder();
const sse = (event: string, value: unknown) => `event: ${event}\ndata: ${JSON.stringify(value)}\n\n`;
const snapshot = (value: unknown, revision = 1, sequence = 0) => sse("snapshot", { sequence, revision, value });
const patch = (value: unknown, revision: number, sequence: number, path = "") =>
  sse("patch", { sequence, baseSequence: sequence - 1, revision, patch: [{ op: "replace", path, value }] });
const ok = (value: unknown = null, revision = 1) => Response.json({ revision, value, duplicate: false });
const failure = (status: number, code: string, message = code, extra: object = {}) => Response.json({ error: { code, message, ...extra } }, { status });

function stream(text: string, open = false) {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  let cancelled = 0;
  const body = new ReadableStream<Uint8Array>({
    start(control) { controller = control; if (text) control.enqueue(encoder.encode(text)); if (!open) control.close(); },
    cancel() { cancelled++; },
  });
  return {
    response: new Response(body, { headers: { "content-type": "text/event-stream" } }),
    push: (more: string) => controller.enqueue(encoder.encode(more)),
    cancelled: () => cancelled,
  };
}
const events = (text: string) => stream(text).response;

function hang(init: FlowerRequestInit): Promise<Response> {
  return new Promise((_, reject) => init.signal?.addEventListener("abort", () => reject(init.signal!.reason), { once: true }));
}

type Step = Response | Error | ((init: FlowerRequestInit) => Promise<Response>);
/** Replies in order; once exhausted, requests hang until aborted like an unresponsive server. */
function scripted(steps: Step[]) {
  const requests: { url: string; init: FlowerRequestInit; body: any }[] = [];
  const fetch: FlowerFetch = async (url, init) => {
    requests.push({ url, init, body: JSON.parse(init.body) });
    init.signal?.throwIfAborted();
    const step = steps.shift() ?? hang;
    if (step instanceof Error) throw step;
    return typeof step === "function" ? step(init) : step;
  };
  return { fetch, requests };
}

/** Aborted after the test, so a failing assertion never leaves connections, timers or retries behind. */
function scope(t: TestContext): AbortSignal {
  const controller = new AbortController();
  t.after(() => controller.abort());
  return controller.signal;
}

async function bounded<T>(promise: Promise<T>, ms = 2_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("timed out")), ms); })]); }
  finally { clearTimeout(timer); }
}

async function take<T>(iterator: AsyncIterator<T>, count: number): Promise<T[]> {
  const values: T[] = [];
  while (values.length < count) {
    const next = await bounded(iterator.next());
    if (next.done) break;
    values.push(next.value);
  }
  return values;
}

test("FlowerError carries the method's structured failure from JSON error bodies", async () => {
  const { fetch } = scripted([
    failure(422, "EVALUATION_FAILED", "OUT_OF_STOCK: no dough", { failure: { code: "OUT_OF_STOCK", message: "no dough", details: { left: 0 } } }),
    failure(403, "FORBIDDEN", "Authorization denied", { failure: { code: "UNAUTHENTICATED", message: "Authentication required" } }),
    failure(422, "TRANSACTION_ABORTED", "participant failed", { failure: { code: "INSUFFICIENT_FUNDS", message: "balance too low" } }),
    failure(503, "UNAVAILABLE", "no quorum"),
    failure(422, "EVALUATION_FAILED", "malformed", { failure: { code: 7, message: "not a failure" } }),
    new Response("gateway exploded", { status: 502 }),
  ]);
  const client = new FlowerClient("http://db", { fetch });
  const errors = [];
  for (let index = 0; index < 6; index++) errors.push(await client.mutate("order").then(() => assert.fail("expected an error"), (error: unknown) => error));
  assert.ok(errors.every((error) => error instanceof FlowerError));
  const [evaluation, forbidden, aborted, unavailable, malformed, gateway] = errors as FlowerError[];
  assert.deepEqual([evaluation.status, evaluation.code, evaluation.message], [422, "EVALUATION_FAILED", "OUT_OF_STOCK: no dough"]);
  assert.deepEqual(evaluation.failure, { code: "OUT_OF_STOCK", message: "no dough", details: { left: 0 } });
  assert.ok(Object.isFrozen(evaluation.failure));
  assert.deepEqual([forbidden.status, forbidden.code, forbidden.failure], [403, "FORBIDDEN", { code: "UNAUTHENTICATED", message: "Authentication required" }]);
  assert.deepEqual([aborted.code, aborted.failure?.code], ["TRANSACTION_ABORTED", "INSUFFICIENT_FUNDS"]);
  assert.deepEqual([unavailable.status, unavailable.code, unavailable.message, unavailable.failure], [503, "UNAVAILABLE", "no quorum", undefined]);
  assert.equal(malformed.failure, undefined);
  assert.deepEqual([gateway.status, gateway.code, gateway.message], [502, "HTTP_ERROR", "gateway exploded"]);
});

test("FlowerError carries failures from SSE error events and watch HTTP errors", async (t) => {
  const limit = { code: "LIMIT", message: "too many toppings", details: { max: 3 } };
  const { fetch } = scripted([
    events(snapshot(1) + sse("error", { error: { code: "EVALUATION_FAILED", message: "LIMIT: too many toppings", status: 422, failure: limit } })),
    failure(403, "FORBIDDEN", "Authorization denied", { failure: { code: "FORBIDDEN", message: "Access denied" } }),
    events(sse("error", { error: { code: "UNAVAILABLE", message: "leader lost", status: 503 } })),
  ]);
  const client = new FlowerClient("http://db", { fetch });
  const signal = scope(t);
  const watcher = client.watch("pizza", null, { signal });
  assert.deepEqual((await bounded(watcher.next())).value, { revision: 1, value: 1 });
  await assert.rejects(bounded(watcher.next()), (error: unknown) => error instanceof FlowerError && error.status === 422 &&
    error.code === "EVALUATION_FAILED" && error.message === "LIMIT: too many toppings" && assert.deepEqual(error.failure, limit) === undefined);
  await assert.rejects(bounded(client.watchDeltas("pizza", null, { signal }).next()), (error: unknown) => error instanceof FlowerError &&
    error.status === 403 && error.failure?.code === "FORBIDDEN" && error.failure.message === "Access denied");
  await assert.rejects(bounded(client.watch("pizza", null, { signal }).next()), (error: unknown) => error instanceof FlowerError &&
    error.status === 503 && error.failure === undefined);
});

test("isTransient retries transport trouble, never answers or the caller's own abort", () => {
  const withFailure = (status: number, code: string) => new FlowerError(code, status, code, { code: "APP_CODE", message: "the method said no" });
  const cases: [unknown, boolean][] = [
    [new TypeError("fetch failed"), true],
    [Object.assign(new Error("HTTP/2 stream aborted"), { code: "H2_STREAM_ABORTED" }), true],
    [new DOMException("The operation timed out", "TimeoutError"), true],
    [new DOMException("The operation was aborted", "AbortError"), false],
    [new FlowerError("Watch stalled", 0, "WATCH_STALLED"), true],
    [new FlowerError("The subscription ended", 0, "WATCH_ENDED"), true],
    [new FlowerError("bad frame", 0, "WATCH_PROTOCOL_ERROR"), false],
    [new FlowerError("gave up", 0, "PARTITION_WAIT_TIMEOUT"), false],
    ...[408, 425, 429, 500, 502, 503, 504].map((status): [unknown, boolean] => [new FlowerError(`HTTP ${status}`, status, "HTTP_ERROR"), true]),
    ...[400, 401, 403, 404, 409, 413, 422].map((status): [unknown, boolean] => [new FlowerError(`HTTP ${status}`, status, "HTTP_ERROR"), false]),
    [withFailure(422, "EVALUATION_FAILED"), false],
    [withFailure(422, "TRANSACTION_ABORTED"), false],
    [withFailure(403, "FORBIDDEN"), false],
    [withFailure(503, "UNAVAILABLE"), false],
  ];
  for (const [error, expected] of cases) assert.equal(isTransient(error), expected, `${(error as Error).name}: ${(error as Error).message}`);
});

test("backoff is jittered exponential growth between half and all of its capped ceiling", () => {
  for (let attempt = 0; attempt < 40; attempt++) {
    const ceiling = Math.min(30_000, 250 * 2 ** attempt);
    for (let sample = 0; sample < 20; sample++) {
      const delay = backoff(attempt);
      assert.ok(Number.isInteger(delay) && delay >= Math.round(ceiling / 2) && delay <= ceiling, `${attempt}: ${delay}`);
    }
  }
  const samples = new Set(Array.from({ length: 50 }, () => backoff(3, 10, 40)));
  assert.ok([...samples].every((delay) => delay >= 20 && delay <= 40) && samples.size > 1, "jittered within the cap");
  assert.equal(backoff(1_000, 1, 1), 1);
});

test("retries keep one request ID and refresh credentials on every attempt", async () => {
  let token = 0;
  const { fetch, requests } = scripted([new TypeError("fetch failed"), failure(502, "BAD_GATEWAY"), ok(7)]);
  const client = new FlowerClient("http://db", { fetch, credentials: () => ({ token: ++token }) });
  assert.deepEqual(await client.mutate("add", { by: 1 }, { retry: { initialDelayMs: 1 } }), { revision: 1, value: 7, duplicate: false });
  assert.equal(requests.length, 3);
  assert.ok(requests.every(({ url }) => url === "http://db/v1/mutate"));
  assert.equal(new Set(requests.map(({ body }) => body.requestId)).size, 1);
  assert.match(requests[0].body.requestId, /^[0-9a-f]{8}-[0-9a-f]{4}-/);
  assert.deepEqual(requests.map(({ body }) => body.credentials), [{ token: 1 }, { token: 2 }, { token: 3 }]);
  assert.deepEqual(requests.map(({ body }) => body.args), [{ by: 1 }, { by: 1 }, { by: 1 }]);
  const explicit = scripted([failure(503, "UNAVAILABLE"), ok()]);
  await new FlowerClient("http://db", { fetch: explicit.fetch, retry: { initialDelayMs: 1 } }).call("add", { by: 1 }, { requestId: "intent-1", expectedRevision: 4 });
  assert.deepEqual(explicit.requests.map(({ url, body }) => [url, body.requestId, body.expectedRevision]),
    [["http://db/v1/call", "intent-1", 4], ["http://db/v1/call", "intent-1", 4]]);
});

test("retries stop at answers: failures, non-transient statuses and custom predicates", async () => {
  const answers: [Response, string][] = [
    [failure(422, "EVALUATION_FAILED", "SOLD_OUT: none left", { failure: { code: "SOLD_OUT", message: "none left" } }), "EVALUATION_FAILED"],
    [failure(403, "FORBIDDEN", "denied", { failure: { code: "UNAUTHENTICATED", message: "Authentication required" } }), "FORBIDDEN"],
    [failure(503, "UNAVAILABLE", "participant said no", { failure: { code: "DOWNSTREAM", message: "no" } }), "UNAVAILABLE"],
    [failure(409, "REQUEST_ID_REUSED", "requestId was already used for different content"), "REQUEST_ID_REUSED"],
    [failure(400, "INVALID_REQUEST"), "INVALID_REQUEST"],
  ];
  for (const [answer, code] of answers) {
    const { fetch, requests } = scripted([answer, ok()]);
    await assert.rejects(new FlowerClient("http://db", { fetch, retry: true }).mutate("order"), { code });
    assert.equal(requests.length, 1, code);
  }
  const busy = scripted([failure(409, "BUSY"), failure(409, "BUSY"), ok("done")]);
  const retryable = (error: unknown) => error instanceof FlowerError && error.code === "BUSY";
  assert.equal((await new FlowerClient("http://db", { fetch: busy.fetch }).mutate("order", null, { retry: { initialDelayMs: 1, retryable } })).value, "done");
  assert.equal(busy.requests.length, 3);
  const unavailable = scripted([failure(503, "UNAVAILABLE")]);
  await assert.rejects(new FlowerClient("http://db", { fetch: unavailable.fetch }).query("read", null, { retry: { initialDelayMs: 1, retryable: () => false } }), { status: 503 });
  assert.equal(unavailable.requests.length, 1);
});

test("retry attempts, until and per-attempt timeouts bound the work", async (t) => {
  const signal = scope(t);
  const down = () => scripted(Array.from({ length: 20 }, () => failure(503, "UNAVAILABLE")));
  let server = down();
  await assert.rejects(new FlowerClient("http://db", { fetch: server.fetch }).query("read", null, { retry: { attempts: 3, initialDelayMs: 1 } }), { status: 503 });
  assert.equal(server.requests.length, 3);
  server = down();
  await assert.rejects(new FlowerClient("http://db", { fetch: server.fetch }).query("read", null, { retry: { initialDelayMs: 1, maxDelayMs: 1 } }), { status: 503 });
  assert.equal(server.requests.length, 8, "eight attempts by default");
  server = down();
  const started = Date.now();
  await assert.rejects(bounded(new FlowerClient("http://db", { fetch: server.fetch }).query("read", null,
    { signal, retry: { initialDelayMs: 1_000, until: Date.now() + 100 } })), { status: 503 });
  assert.equal(server.requests.length, 1, "no attempt may start after until");
  assert.ok(Date.now() - started < 500, "until does not sleep toward a retry it cannot make");
  const lost = scripted([hang, Response.json({ revision: 3, value: "applied", duplicate: true })]);
  const result = await bounded(new FlowerClient("http://db", { fetch: lost.fetch }).mutate("pay", { cents: 5 }, { signal, retry: { timeoutMs: 30, initialDelayMs: 1 } }));
  assert.deepEqual(result, { revision: 3, value: "applied", duplicate: true });
  assert.equal(lost.requests[0].init.signal?.reason?.name, "TimeoutError");
  assert.equal(lost.requests[0].body.requestId, lost.requests[1].body.requestId);
});

test("retries honor the caller's signal while waiting between attempts", async () => {
  const controller = new AbortController();
  const { fetch, requests } = scripted([failure(503, "UNAVAILABLE")]);
  const pending = new FlowerClient("http://db", { fetch }).query("read", null, { signal: controller.signal, retry: { initialDelayMs: 60_000 } });
  await delay(10);
  const reason = new Error("caller gave up");
  controller.abort(reason);
  await assert.rejects(bounded(pending), (error) => error === reason);
  assert.equal(requests.length, 1);
  await assert.rejects(new FlowerClient("http://db", { fetch }).query("read", null, { signal: AbortSignal.abort(reason), retry: true }), (error) => error === reason);
  assert.equal(requests.length, 1);
});

test("the client retry default applies to partitions and rotates query endpoints, and calls can opt out", async () => {
  const { fetch, requests } = scripted([failure(503, "UNAVAILABLE"), ok(1), failure(429, "RATE_LIMITED"), ok(2), failure(503, "UNAVAILABLE")]);
  const client = new FlowerClient("http://primary", { fetch, retry: { initialDelayMs: 1 }, queryUrls: ["http://a", "http://b"] });
  assert.equal((await client.query("read")).value, 1);
  assert.equal((await client.partition("west").mutate("write")).value, 2);
  await assert.rejects(client.query("read", null, { retry: false }), { status: 503 });
  assert.deepEqual(requests.map(({ url }) => url), [
    "http://a/v1/query", "http://b/v1/query",
    "http://primary/partitions/west/v1/mutate", "http://primary/partitions/west/v1/mutate",
    "http://a/v1/query",
  ]);
  assert.equal(requests[2].body.requestId, requests[3].body.requestId);
});

test("subscribe marks the first value of every connection as a reset", async (t) => {
  const { fetch, requests } = scripted([
    events(snapshot({ n: 1 }, 1) + patch(2, 2, 1, "/n")),
    events(snapshot({ n: 5 }, 5) + patch(6, 6, 1, "/n")),
  ]);
  const updates = new FlowerClient("http://db", { fetch }).subscribe("counter", { shop: "north" }, { signal: scope(t), reconnect: { initialDelayMs: 1 } });
  assert.deepEqual(await take(updates, 4), [
    { revision: 1, value: { n: 1 }, reset: true },
    { revision: 2, value: { n: 2 }, reset: false },
    { revision: 5, value: { n: 5 }, reset: true },
    { revision: 6, value: { n: 6 }, reset: false },
  ]);
  await bounded(updates.return(undefined));
  assert.equal(requests.length, 2);
  for (const { url, init, body } of requests) {
    assert.equal(url, "http://db/v1/watch");
    assert.equal(init.headers.accept, "text/event-stream");
    assert.deepEqual(body, { name: "counter", args: { shop: "north" } });
    assert.equal(init.signal?.aborted, true);
  }
});

test("subscribe reconnects after end of stream, network errors, transient statuses and transient error events", async (t) => {
  const { fetch, requests } = scripted([
    events(snapshot("first", 1)),
    new TypeError("fetch failed"),
    failure(503, "UNAVAILABLE", "leader lost"),
    failure(429, "RATE_LIMITED", "slow down"),
    events(sse("error", { error: { code: "UNAVAILABLE", message: "shutting down", status: 503 } })),
    events(snapshot("second", 2)),
  ]);
  const updates = new FlowerClient("http://db", { fetch }).subscribe("value", null, { signal: scope(t), reconnect: { initialDelayMs: 1, maxDelayMs: 2 } });
  assert.deepEqual(await take(updates, 2), [{ revision: 1, value: "first", reset: true }, { revision: 2, value: "second", reset: true }]);
  assert.equal(requests.length, 6);
});

test("subscribe surfaces answers and protocol violations instead of reconnecting", async (t) => {
  const cases: [Step[], (error: any) => boolean][] = [
    [[failure(404, "METHOD_NOT_FOUND", "no such alias")], (error) => error.status === 404 && error.code === "METHOD_NOT_FOUND"],
    [[failure(403, "FORBIDDEN", "denied", { failure: { code: "UNAUTHENTICATED", message: "Authentication required" } })], (error) => error.failure?.code === "UNAUTHENTICATED"],
    [[events(snapshot(1) + sse("error", { error: { code: "EVALUATION_FAILED", message: "BAD: no", status: 422, failure: { code: "BAD", message: "no" } } }))],
      (error) => error.status === 422 && error.failure?.code === "BAD"],
    [[events(snapshot({}) + patch(1, 2, 1, "/missing"))], (error) => error.code === "WATCH_PROTOCOL_ERROR"],
    [[events(snapshot(1, 5) + snapshot(2, 4, 1))], (error) => error.code === "WATCH_PROTOCOL_ERROR"],
  ];
  const signal = scope(t);
  for (const [steps, expected] of cases) {
    const { fetch, requests } = scripted([...steps, events(snapshot("reconnected"))]);
    const updates = new FlowerClient("http://db", { fetch }).subscribe("value", null, { signal, reconnect: { initialDelayMs: 1 } });
    await assert.rejects(take(updates, 3), (error: unknown) => error instanceof FlowerError && expected(error));
    assert.equal(requests.length, 1);
    assert.equal((await bounded(updates.next())).done, true);
  }
});

test("subscribe without reconnect ends at end of stream and throws transient errors", async (t) => {
  const signal = scope(t);
  const ended = scripted([events(snapshot(1)), events(snapshot(2))]);
  const updates = new FlowerClient("http://db", { fetch: ended.fetch }).subscribe("value", null, { signal, reconnect: false });
  assert.deepEqual(await take(updates, 5), [{ revision: 1, value: 1, reset: true }]);
  assert.equal(ended.requests.length, 1);
  const down = scripted([failure(503, "UNAVAILABLE"), events(snapshot(2))]);
  await assert.rejects(take(new FlowerClient("http://db", { fetch: down.fetch }).subscribe("value", null, { signal, reconnect: false }), 1), { status: 503 });
  assert.equal(down.requests.length, 1);
});

test("subscribe reconnects silent streams and stalled error bodies after stallMs, while heartbeats keep it alive", async (t) => {
  const signal = scope(t);
  const silent = stream(snapshot("stale", 1), true);
  const stalled = scripted([silent.response, events(snapshot("fresh", 2))]);
  const updates = new FlowerClient("http://db", { fetch: stalled.fetch }).subscribe("value", null, { signal, stallMs: 50, reconnect: { initialDelayMs: 1 } });
  assert.deepEqual(await take(updates, 2), [{ revision: 1, value: "stale", reset: true }, { revision: 2, value: "fresh", reset: true }]);
  assert.equal(stalled.requests[0].init.signal?.reason?.code, "WATCH_STALLED");
  assert.equal(silent.cancelled(), 1);

  let errorBodyCancelled = 0;
  const stuck = new Response(new ReadableStream({ cancel() { errorBodyCancelled++; } }), { status: 503 });
  const erroring = scripted([stuck, events(snapshot("recovered", 3))]);
  const recovering = new FlowerClient("http://db", { fetch: erroring.fetch }).subscribe("value", null, { signal, stallMs: 50, reconnect: { initialDelayMs: 1 } });
  assert.deepEqual(await take(recovering, 1), [{ revision: 3, value: "recovered", reset: true }]);
  assert.equal(errorBodyCancelled, 1);

  const live = stream(snapshot(1, 1), true);
  const beating = scripted([live.response]);
  const watcher = new FlowerClient("http://db", { fetch: beating.fetch }).subscribe("value", null, { signal, stallMs: 150 });
  assert.deepEqual((await bounded(watcher.next())).value, { revision: 1, value: 1, reset: true });
  const next = watcher.next();
  for (let beat = 0; beat < 12; beat++) { await delay(25); live.push(": heartbeat\n\n"); }
  live.push(patch(2, 2, 1));
  assert.deepEqual((await bounded(next)).value, { revision: 2, value: 2, reset: false });
  assert.equal(beating.requests.length, 1);
  assert.throws(() => new FlowerClient("http://db", { fetch: beating.fetch }).subscribe("value", null, { stallMs: 0 }), /stallMs/);
  assert.throws(() => new FlowerClient("http://db", { fetch: beating.fetch }).subscribe("value", null, { stallMs: 1.5 }), /stallMs/);
  assert.equal(beating.requests.length, 1);
});

test("monotonic subscriptions skip values older than any already delivered", async (t) => {
  const signal = scope(t);
  const lagging = () => [
    events(snapshot("fresh", 5)),
    events(snapshot("old", 3) + patch("older", 4, 1) + patch("newest", 6, 2)),
  ];
  const monotonic = scripted(lagging());
  const client = new FlowerClient("http://primary", { fetch: monotonic.fetch, queryUrls: ["http://a", "http://b"] });
  const updates = client.subscribe("value", null, { signal, monotonic: true, reconnect: { initialDelayMs: 1 } });
  assert.deepEqual(await take(updates, 2), [{ revision: 5, value: "fresh", reset: true }, { revision: 6, value: "newest", reset: true }]);
  const plain = scripted(lagging());
  const all = new FlowerClient("http://primary", { fetch: plain.fetch, queryUrls: ["http://a", "http://b"] }).subscribe("value", null, { signal, reconnect: { initialDelayMs: 1 } });
  assert.deepEqual((await take(all, 4)).map(({ revision, reset }) => [revision, reset]), [[5, true], [3, true], [4, false], [6, false]]);
});

test("subscribe rotates query endpoints and refreshes credentials on every reconnect", async (t) => {
  let token = 0;
  const { fetch, requests } = scripted([events(snapshot(1, 1)), failure(503, "UNAVAILABLE"), events(snapshot(2, 2))]);
  const client = new FlowerClient("http://primary", { fetch, queryUrls: ["http://a/", "http://b"], credentials: async () => ({ token: ++token }) });
  const updates = client.subscribe("value", null, { signal: scope(t), reconnect: { initialDelayMs: 1 } });
  assert.deepEqual((await take(updates, 2)).map(({ value }) => value), [1, 2]);
  await bounded(updates.return(undefined));
  assert.deepEqual(requests.map(({ url }) => url), ["http://a/v1/watch", "http://b/v1/watch", "http://a/v1/watch"]);
  assert.deepEqual(requests.map(({ body }) => body.credentials), [{ token: 1 }, { token: 2 }, { token: 3 }]);
});

test("aborting a subscription stops it cleanly while connected, while backing off and before starting", async (t) => {
  const cleanup = scope(t);
  const live = stream(snapshot(1), true);
  const connected = scripted([live.response]);
  const controller = new AbortController();
  const updates = new FlowerClient("http://db", { fetch: connected.fetch }).subscribe("value", null, { signal: AbortSignal.any([controller.signal, cleanup]) });
  await bounded(updates.next());
  const pending = updates.next();
  controller.abort(new Error("done watching"));
  assert.equal((await bounded(pending)).done, true);
  assert.equal(connected.requests[0].init.signal?.aborted, true);
  assert.equal(live.cancelled(), 1);

  const down = scripted([failure(503, "UNAVAILABLE")]);
  const stop = new AbortController();
  const waiting = new FlowerClient("http://db", { fetch: down.fetch }).subscribe("value", null,
    { signal: AbortSignal.any([stop.signal, cleanup]), reconnect: { initialDelayMs: 60_000 } });
  const next = waiting.next();
  await delay(10);
  stop.abort();
  assert.equal((await bounded(next)).done, true);
  const returned = new FlowerClient("http://db", { fetch: down.fetch }).subscribe("value", null, { signal: cleanup, reconnect: { initialDelayMs: 60_000 } });
  const blocked = returned.next();
  await delay(10);
  assert.equal((await bounded(returned.return(undefined))).done, true);
  assert.equal((await bounded(blocked)).done, true);
  await delay(20);
  assert.equal(connected.requests.length, 1);
  assert.equal(down.requests.length, 2);

  const idle = scripted([]);
  assert.equal((await new FlowerClient("http://db", { fetch: idle.fetch }).subscribe("value", null, { signal: AbortSignal.abort() }).next()).done, true);
  assert.equal(idle.requests.length, 0);
});

test("an aborted subscription releases its stall timer even if iteration stops", async () => {
  const script = `
    import { FlowerClient } from ${JSON.stringify(new URL("./client.ts", import.meta.url).href)};
    const body = new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(${JSON.stringify(snapshot(1))})); } });
    const client = new FlowerClient("http://db", { fetch: async () => new Response(body, { headers: { "content-type": "text/event-stream" } }) });
    const controller = new AbortController();
    const updates = client.subscribe("value", null, { signal: controller.signal, stallMs: 60000 });
    await updates.next();
    controller.abort();`;
  const started = Date.now();
  await promisify(execFile)(process.execPath, ["--input-type=module", "--eval", script], { timeout: 20_000 });
  assert.ok(Date.now() - started < 15_000, "the process exits without waiting for stallMs");
});

test("waitUntil resolves with the first matching update and closes its subscription", async (t) => {
  const signal = scope(t);
  const live = stream(snapshot({ n: 1 }, 1) + patch(2, 2, 1, "/n") + patch(3, 3, 2, "/n"), true);
  const counting = scripted([live.response]);
  const client = new FlowerClient("http://db", { fetch: counting.fetch });
  assert.deepEqual(await bounded(client.waitUntil("counter", null, (value: any) => value.n >= 2, { signal })), { revision: 2, value: { n: 2 }, reset: false });
  assert.equal(counting.requests[0].init.signal?.aborted, true);
  assert.equal(live.cancelled(), 1);

  const truthy = scripted([events(snapshot(0, 1) + patch(7, 2, 1))]);
  assert.equal((await bounded(new FlowerClient("http://db", { fetch: truthy.fetch }).waitUntil("count", null, undefined, { signal }))).value, 7);
  // Null arguments can be omitted before the predicate, or before the options alone.
  const omitted = scripted([events(snapshot(1, 1) + patch(3, 2, 1)), events(snapshot(0, 1) + patch(5, 2, 1))]);
  const bare = new FlowerClient("http://db", { fetch: omitted.fetch });
  assert.equal((await bounded(bare.waitUntil("count", (value: any) => value > 2, { signal }))).value, 3);
  assert.equal((await bounded(bare.waitUntil("count", undefined, { signal }))).value, 5);
  assert.deepEqual(omitted.requests.map(({ init }) => JSON.parse(String(init.body)).args), [null, null]);
  const ended = scripted([events(snapshot(false))]);
  await assert.rejects(bounded(new FlowerClient("http://db", { fetch: ended.fetch }).waitUntil("flag", null, Boolean, { signal, reconnect: false })),
    (error: unknown) => error instanceof FlowerError && error.code === "WATCH_ENDED" && isTransient(error));
  const gone = scripted([failure(404, "METHOD_NOT_FOUND")]);
  await assert.rejects(bounded(new FlowerClient("http://db", { fetch: gone.fetch }).waitUntil("flag", null, Boolean, { signal })), { code: "METHOD_NOT_FOUND" });
  const broken = new Error("predicate failed");
  const throwing = stream(snapshot(1), true);
  await assert.rejects(bounded(new FlowerClient("http://db", { fetch: scripted([throwing.response]).fetch }).waitUntil("flag", null, () => { throw broken; }, { signal })),
    (error) => error === broken);
  assert.equal(throwing.cancelled(), 1);

  const controller = new AbortController();
  const never = scripted([events(snapshot(false)), stream(snapshot(false), true).response]);
  const waiting = new FlowerClient("http://db", { fetch: never.fetch }).waitUntil("flag", null, Boolean,
    { signal: AbortSignal.any([controller.signal, signal]), reconnect: { initialDelayMs: 1 } });
  await delay(20);
  const reason = new Error("stop waiting");
  controller.abort(reason);
  await assert.rejects(bounded(waiting), (error) => error === reason);
  assert.equal(never.requests.length, 2);
});

test("FlowerAdmin scopes operator calls to partitions and alone sends the operator token", async () => {
  const { fetch, requests } = scripted(Array.from({ length: 8 }, () => ok()));
  const admin = new FlowerAdmin("http://seed:7101/", { adminToken: "operator", fetch });
  const tenant = admin.partition("tenant/🌻");
  const path = "/partitions/tenant%2F%F0%9F%8C%BB";
  assert.equal(admin.url, "http://seed:7101");
  assert.equal(tenant.url, "http://seed:7101" + path);
  assert.ok(tenant instanceof FlowerAdmin);
  await tenant.deploy({ hash: "h", javascript: "code" }, { requestId: "deploy-1", preparation: "blocking" });
  await tenant.keyList();
  await tenant.retentionStatus();
  await tenant.transactionClosureStatus();
  await admin.layout();
  assert.deepEqual(requests.map(({ url }) => url), [
    `http://seed:7101${path}/admin/deploy`, `http://seed:7101${path}/admin/keys`, `http://seed:7101${path}/admin/retention`,
    `http://seed:7101${path}/admin/transactions`, "http://seed:7101/admin/partitions/catalog",
  ]);
  assert.ok(requests.every(({ init }) => init.headers.authorization === "Bearer operator"));
  assert.deepEqual(requests[0].body, { requestId: "deploy-1", bundle: { hash: "h", javascript: "code" }, preparation: "blocking" });
  await assert.rejects(tenant.deploy({ hash: "h", javascript: "code" }, { preparation: "eager" as never }), TypeError);
  for (const name of ["", " ", "a\u0000", "a\n", 7 as never]) assert.throws(() => admin.partition(name), TypeError);

  await new FlowerAdmin("http://seed:7101", { fetch }).partition("a").keyList();
  assert.equal(requests.at(-1)!.init.headers.authorization, undefined);
  const client = new FlowerClient("http://seed:7101", { fetch, ...({ adminToken: "operator" } as {}) });
  await client.partition("tenant/🌻").mutate("set", 1, { requestId: "r" });
  assert.equal(requests.at(-1)!.url, `http://seed:7101${path}/v1/mutate`);
  assert.equal(requests.at(-1)!.init.headers.authorization, undefined);
});

const counters = collection("counters", v.object({ n: v.int() }));
const count = query("count", (ctx) => ctx.get(counters, "main")?.n ?? 0);
const find = query("find", { args: v.string() }, (ctx, id) => ctx.get(counters, id));
const page = query("page", (_ctx, args: { limit?: number } | null) => args?.limit ?? 10);
const add = mutation("add", { args: v.object({ by: v.int({ min: 1 }) }) }, (ctx, { by }) => {
  const n = (ctx.get(counters, "main")?.n ?? 0) + by;
  ctx.set(counters, "main", { n });
  return n;
});
const move = transaction("move", (args: { to: string }) => ({ calls: [{ partition: args.to, method: "add", args: { by: 1 } }], value: args.to }));
const app = define({ http: { count, find, page, add, move } });

test("typed clients check aliases, method kinds, arguments and results at compile time", async () => {
  const { fetch, requests } = scripted(Array.from({ length: 9 }, () => ok(3)));
  const client = new FlowerClient<typeof app>("http://db", { fetch });
  const counted: QueryResult<number> = await client.query("count");
  const found: QueryResult<{ n: number } | null> = await client.query("find", "main", { retry: false });
  const added: MutationResult<number> = await client.mutate("add", { by: 2 }, { requestId: "r1" });
  const moved: MutationResult<TransactionResult<string>> = await client.call("move", { to: "west" });
  await client.query("count", null, { signal: new AbortController().signal });
  await client.query("page");
  await client.query("page", null);
  await client.query("page", { limit: 5 });
  await client.call("count");
  const tenant: FlowerClient<typeof app> = client.partition("west");
  void [counted, found, added, moved, tenant];
  assert.deepEqual(requests.map(({ url, body }) => [url.slice("http://db".length), body.name, body.args]), [
    ["/v1/query", "count", null], ["/v1/query", "find", "main"], ["/v1/mutate", "add", { by: 2 }], ["/v1/call", "move", { to: "west" }],
    ["/v1/query", "count", null], ["/v1/query", "page", null], ["/v1/query", "page", null], ["/v1/query", "page", { limit: 5 }], ["/v1/call", "count", null],
  ]);
  assert.equal(requests[2].body.requestId, "r1");

  const typed = async () => {
    for await (const update of client.subscribe("count")) { const value: number = update.value, reset: boolean = update.reset; void value; void reset; }
    for await (const { value } of client.watch("find", "main")) { const row: { n: number } | null = value; void row; }
    const ready: number = (await client.waitUntil("count", null, (n) => n > 2)).value;
    const soon: number = (await client.waitUntil("count", (n) => n > 2, { signal: AbortSignal.timeout(1) })).value;
    void soon;
    const moved = (await client.mutate("move", { to: "west" })).value.results;
    void ready; void moved;
  };
  const rejected = async () => {
    // @ts-expect-error unknown alias
    await client.query("missing");
    // @ts-expect-error add is a mutation
    await client.query("add", { by: 1 });
    // @ts-expect-error count is a query
    await client.mutate("count");
    // @ts-expect-error move takes a destination object
    await client.mutate("move", "west");
    // @ts-expect-error by must be a number
    await client.mutate("add", { by: "2" });
    // @ts-expect-error add requires arguments
    await client.mutate("add");
    // @ts-expect-error find requires its string argument
    await client.query("find");
    // @ts-expect-error options follow the omitted null argument
    await client.query("count", { retry: true });
    // @ts-expect-error only queries can be watched
    client.watch("add", { by: 1 });
    // @ts-expect-error only queries can be subscribed to
    client.subscribe("add", { by: 1 });
    // @ts-expect-error predicates receive the typed value
    await client.waitUntil("count", null, (n: string) => n === "3");
    // @ts-expect-error find requires its argument before the predicate
    await client.waitUntil("find", (row) => row !== null);
    // @ts-expect-error results are typed
    const wrong: QueryResult<string> = await client.query("count");
    // @ts-expect-error explicit value generics are gone
    await client.mutate<"add", number>("add", { by: 1 });
    // @ts-expect-error control-plane methods live on FlowerAdmin
    await client.deploy({ hash: "h", javascript: "code" });
    // @ts-expect-error the operator token belongs to FlowerAdmin
    new FlowerClient<typeof app>("http://db", { adminToken: "operator" });
    void wrong;
  };
  void typed; void rejected;

  const untyped = new FlowerClient("http://db", { fetch: scripted([ok({ any: "shape" })]).fetch });
  const loose: string = (await untyped.mutate("anything", { at: "all" })).value.any;
  assert.equal(loose, "shape");
});
