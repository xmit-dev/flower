import assert from "node:assert/strict";
import test from "node:test";
import { FlowerClient, FlowerError } from "./client.ts";
import { applyWatchPatch, decodeWatchEvent, readSse, WatchProtocolError } from "./watch.ts";
import type { FlowerRequestInit } from "./client.ts";
import type { Json } from "./index.ts";

const encode = (event: string, value: unknown, ending = "\n") => `event: ${event}${ending}data: ${JSON.stringify(value)}${ending}${ending}`;
const snapshot = (value: unknown, sequence = 0, revision = 1) => encode("snapshot", { sequence, revision, value });
const patch = (operations: unknown[], sequence = 1, baseSequence = 0, revision = 1) => encode("patch", { sequence, baseSequence, revision, patch: operations });

function fixture(text: string, options: { close?: boolean; status?: number; contentType?: string; chunks?: number } = {}) {
  let cancelled = 0;
  let request: FlowerRequestInit | undefined;
  let url: string | undefined;
  let bodyController!: ReadableStreamDefaultController<Uint8Array>;
  const client = new FlowerClient("http://localhost:7101", { fetch: async (address, init) => {
    request = init; url = address;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        bodyController = controller;
        const bytes = new TextEncoder().encode(text);
        const width = options.chunks ?? (bytes.length || 1);
        for (let offset = 0; offset < bytes.length; offset += width) controller.enqueue(bytes.slice(offset, offset + width));
        if (options.close) controller.close();
      }, cancel() { cancelled++; },
    });
    return new Response(body, { status: options.status ?? 200, headers: { "content-type": options.contentType ?? "text/event-stream; charset=utf-8" } });
  } });
  return { client, cancelled: () => cancelled, request: () => request!, url: () => url, send: (text: string) => bodyController.enqueue(new TextEncoder().encode(text)) };
}

async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try { return await Promise.race([promise, new Promise<T>((_, reject) => { timer = setTimeout(() => reject(new Error("watch cancellation stalled")), 1000); })]); }
  finally { clearTimeout(timer!); }
}

test("watch reconstructs deltas, same-revision clocks, and snapshot fallback without sharing mutable state", async () => {
  const stream = fixture(snapshot({ list: [1], label: "first" }) + patch([{ op: "add", path: "/list/-", value: 2 }]) + snapshot({ fresh: true }, 2, 2));
  const watcher = stream.client.watch("clock", { key: "x" });
  const first = (await watcher.next()).value!;
  (first.value as any).list.push(999);
  (first.value as any).label = "caller changed";
  assert.deepEqual((await watcher.next()).value, { revision: 1, value: { list: [1, 2], label: "first" } });
  assert.deepEqual((await watcher.next()).value, { revision: 2, value: { fresh: true } });
  await watcher.return(undefined);
  assert.equal(stream.cancelled(), 1);
  assert.equal(stream.request().signal?.aborted, true);
  assert.equal(stream.url(), "http://localhost:7101/v1/watch");
  assert.equal(stream.request().headers.accept, "text/event-stream");
  assert.deepEqual(JSON.parse(stream.request().body), { name: "clock", args: { key: "x" } });
});

test("raw events survive one-byte UTF-8 chunks, CRLF, comments, and multiline data", async () => {
  const text = ': heartbeat\r\n\r\nid: 0\r\nevent: snapshot\r\ndata: {"sequence":0,\r\ndata: "revision":3,"value":"😀"}\r\n\r\n';
  const stream = fixture(text, { chunks: 1, close: true });
  const events = [];
  for await (const event of stream.client.watchDeltas("value")) events.push(event);
  assert.deepEqual(events, [{ type: "snapshot", sequence: 0, revision: 3, value: "😀" }]);
});

test("return interrupts pending next and cancels both reader and HTTP request", async () => {
  const stream = fixture(snapshot(null));
  const watcher = stream.client.watch("idle");
  await watcher.next();
  const next = watcher.next();
  const finish = watcher.return(undefined);
  assert.equal((await bounded(next)).done, true);
  assert.equal((await bounded(finish)).done, true);
  assert.equal(stream.cancelled(), 1);
  assert.equal(stream.request().signal?.aborted, true);
});

test("abort ends cleanly before startup, during SSE, and during a stalled HTTP error", async () => {
  const unused = fixture("");
  const early = unused.client.watch("early", null, { signal: AbortSignal.abort() });
  assert.equal((await early.next()).done, true);
  assert.equal(unused.url(), undefined);
  for (const status of [200, 503]) {
    const stream = fixture("", { status });
    const controller = new AbortController();
    const watcher = stream.client.watchDeltas("idle", null, { signal: controller.signal });
    const next = watcher.next();
    await new Promise((resolve) => setImmediate(resolve));
    controller.abort();
    assert.equal((await bounded(next)).done, true);
    assert.equal(stream.cancelled(), 1);
  }
});

test("terminal server errors retain code/status and do not reconnect", async () => {
  const stream = fixture(snapshot(1) + encode("error", { error: { code: "METHOD_NOT_FOUND", message: "alias revoked", status: 404 } }));
  const watcher = stream.client.watch("gone");
  await watcher.next();
  await assert.rejects(watcher.next(), (error: any) => error instanceof FlowerError && error.status === 404 && error.code === "METHOD_NOT_FOUND");
  assert.equal(stream.cancelled(), 1);
  assert.equal((await watcher.next()).done, true);
});

test("invalid sequence, revision, pointer, event ID, incomplete events, and unsupported content fail closed", async () => {
  const cases = [
    snapshot(1, -1), snapshot(1, 2) + snapshot(2, 2), snapshot(1, 3) + snapshot(2, 2), patch([], 0, -1), snapshot(1) + patch([], 2, 0), snapshot(1) + patch([], 1, 3),
    snapshot(1, 0, 3) + patch([], 1, 0, 2), snapshot({}) + patch([{ op: "replace", path: "/missing", value: 1 }]),
    'id: 2\n' + snapshot(1), 'event: snapshot\ndata: {}', 'event: snapshot\ndata: {bad}\n\n',
    encode("unknown", {}), encode("snapshot", { sequence: 0, revision: 1, value: 1e300 }).replace("1e+300", "1e999"),
  ];
  for (const text of cases) {
    const stream = fixture(text, { close: true });
    await assert.rejects(async () => { for await (const _ of stream.client.watch("broken")) {} }, (error: any) => error.code === "WATCH_PROTOCOL_ERROR", text);
    assert.equal(stream.request().signal?.aborted, true);
  }
  const wrong = fixture("x", { contentType: "application/json" });
  await assert.rejects(wrong.client.watch("wrong").next(), /text\/event-stream/);
  assert.equal(wrong.cancelled(), 1);
});

test("SSE limits cover comments/lines and malformed UTF-8 without buffering indefinitely", async () => {
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new TextEncoder().encode(':' + 'x'.repeat(100))); }, cancel() { cancelled = true; } });
  await assert.rejects(async () => { for await (const _ of readSse(body, new AbortController().signal, 64)) {} }, /byte limit/);
  assert.ok(cancelled);
  const bad = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new Uint8Array([0xc3, 0x28])); controller.close(); } });
  await assert.rejects(async () => { for await (const _ of readSse(bad, new AbortController().signal)) {} }, /UTF-8/);
  const interrupted = new TypeError("network terminated");
  const failed = new ReadableStream<Uint8Array>({ start(controller) { controller.error(interrupted); } });
  await assert.rejects(async () => { for await (const _ of readSse(failed, new AbortController().signal)) {} }, (error) => error === interrupted);
});

test("snapshot size and value depth budgets exclude only the trusted SSE envelope", () => {
  const data = JSON.stringify({ sequence: 0, revision: 1, value: "x".repeat(16 * 1024 * 1024) });
  assert.throws(() => decodeWatchEvent({ event: "snapshot", data }, -1, -1), /maxValueBytes/);
  let value: Json = 1;
  for (let depth = 0; depth < 128; depth++) value = [value];
  assert.equal(decodeWatchEvent({ event: "snapshot", data: JSON.stringify({ sequence: 0, revision: 1, value }) }, -1, -1).type, "snapshot");
  assert.throws(() => decodeWatchEvent({ event: "snapshot", data: JSON.stringify({ sequence: 0, revision: 1, value: [value] }) }, -1, -1), /128/);
});

test("JSON patches handle arrays, escaped pointers, root replacement and prototype-looking data keys safely", () => {
  const initial: Json = JSON.parse('{"__proto__":{"safe":1},"constructor":{"prototype":{"safe":2}},"a/b":{"~":[1,3]}}');
  const result = applyWatchPatch(initial, [
    { op: "replace", path: "/__proto__/safe", value: 9 },
    { op: "add", path: "/constructor/prototype/owned", value: true },
    { op: "add", path: "/a~1b/~0/1", value: 2 },
    { op: "remove", path: "/a~1b/~0/0" },
  ]);
  assert.equal((Object.prototype as any).owned, undefined);
  assert.equal((result as any).__proto__.safe, 9);
  assert.deepEqual((result as any)["a/b"]["~"], [2, 3]);
  assert.equal((initial as any).__proto__.safe, 1);
  assert.deepEqual(applyWatchPatch({}, [{ op: "add", path: "/__proto__", value: { x: 1 } }]), JSON.parse('{"__proto__":{"x":1}}'));
  assert.deepEqual(applyWatchPatch(result, [{ op: "replace", path: "", value: [null] }]), [null]);
  for (const path of ["/constructor/prototype/polluted", "/__proto__/polluted"]) {
    assert.throws(() => applyWatchPatch({}, [{ op: "add", path, value: true }]), WatchProtocolError);
  }
  for (const path of ["/01", "/-", "/3", "/length", "/~2", "/999999999999999999"]) {
    assert.throws(() => applyWatchPatch([0], [{ op: "replace", path, value: true }]), WatchProtocolError);
  }
  assert.throws(() => decodeWatchEvent({ event: "patch", data: JSON.stringify({ sequence: 1, baseSequence: 0, revision: 1, patch: Array.from({ length: 257 }, () => ({ op: "remove", path: "/x" })) }) }, 0, 1), /oversized/);
});

test("polling remains explicit and intervalMs cannot silently enable polling in SSE", async () => {
  const stream = fixture(snapshot(1));
  // @ts-expect-error intervalMs belongs to watchPoll
  await assert.rejects(stream.client.watch("value", null, { intervalMs: 10 }).next(), /intervalMs applies only to watchPoll/);
  const sse = stream.client.watch("value", null);
  assert.deepEqual((await sse.next()).value, { revision: 1, value: 1 });
  assert.equal(stream.url(), "http://localhost:7101/v1/watch");
  await sse.return(undefined);
  const controller = new AbortController();
  const client = new FlowerClient("http://localhost", { fetch: async () => new Response('{"revision":1,"value":2}', { headers: { "content-type": "application/json" } }) });
  const poll = client.watchPoll("value", null, { intervalMs: 10000, signal: controller.signal });
  assert.deepEqual((await poll.next()).value, { revision: 1, value: 2 });
  const next = poll.next();
  await bounded(poll.return(undefined));
  assert.equal((await next).done, true);
});


test("watch byte budgets cover UTF-8 snapshots and reconstructed values without wire options", async () => {
  const small = fixture(snapshot("🌼"), { close: true });
  await assert.rejects(small.client.watch("small", null, { maxValueBytes: 5 }).next(), /maxValueBytes/);
  const stream = fixture(snapshot({ text: "" }) + patch([{ op: "replace", path: "/text", value: "🌼🌼" }]), { close: true });
  const watcher = stream.client.watch("small", null, { maxValueBytes: 15, maxEventBytes: 1024 });
  assert.deepEqual((await watcher.next()).value?.value, { text: "" });
  await assert.rejects(watcher.next(), /maxValueBytes/);
  assert.deepEqual(JSON.parse(stream.request().body), { name: "small", args: null });
  const event = fixture(snapshot(1), { close: true });
  await assert.rejects(event.client.watchDeltas("small", null, { maxEventBytes: 20 }).next(), /byte limit/);
});

test("watch limits can grow beyond the safe defaults for larger configured servers", async () => {
  const value = "x".repeat(17 * 1024 * 1024);
  const stream = fixture(snapshot(value), { close: true });
  const watch = stream.client.watch("large", null, { maxValueBytes: 18 * 1024 * 1024, maxEventBytes: 19 * 1024 * 1024 });
  assert.equal((await watch.next()).value?.value, value);
  assert.equal((await watch.next()).done, true);
  const operations = Array.from({ length: 300 }, (_, index) => ({ op: "add", path: `/${index}`, value: index }));
  const patches = fixture(snapshot({}) + patch(operations), { close: true });
  const reconstructed = [];
  for await (const value of patches.client.watch("many", null, { maxPatchOperations: 300 })) reconstructed.push(value.value);
  assert.equal(Object.keys(reconstructed[1] as object).length, 300);
  const constrained = fixture(snapshot({}) + patch(operations), { close: true });
  await assert.rejects(async () => { for await (const _ of constrained.client.watch("many", null, { maxPatchOperations: 299 })) {} }, /oversized/);
});

test("watch budgets reject invalid values before opening HTTP and bound HTTP error bodies", async () => {
  for (const key of ["maxEventBytes", "maxValueBytes", "maxPatchOperations"] as const) {
    for (const invalid of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "10", null]) {
      const stream = fixture(snapshot(1));
      // Null is intentionally invalid too: only absence selects a default.
      assert.throws(() => stream.client.watch("invalid", null, { [key]: invalid } as any), new RegExp(key));
      assert.throws(() => stream.client.watchDeltas("invalid", null, { [key]: invalid } as any), new RegExp(key));
      assert.equal(stream.url(), undefined);
    }
  }
  const body = JSON.stringify({ error: { message: "x".repeat(100), code: "BIG" } });
  const small = fixture(body, { status: 503, close: true });
  await assert.rejects(small.client.watch("error", null, { maxEventBytes: 32 }).next(), /maxEventBytes/);
  const allowed = fixture(body, { status: 503, close: true });
  await assert.rejects(allowed.client.watch("error", null, { maxEventBytes: 1024 }).next(), (error: any) => error.code === "BIG");
});


test("polling rejects intervals that overflow the runtime timer instead of becoming busy loops", async () => {
  let requests = 0;
  const client = new FlowerClient("http://localhost", { fetch: async () => {
    requests++;
    return new Response('{"revision":1,"value":2}', { headers: { "content-type": "application/json" } });
  } });
  for (const intervalMs of [0, -1, 1.5, NaN, Infinity, 2_147_483_648, Number.MAX_SAFE_INTEGER]) {
    assert.throws(() => client.watchPoll("value", null, { intervalMs }), /intervalMs.*2147483647/);
  }
  assert.equal(requests, 0);
  const poll = client.watchPoll("value", null, { intervalMs: 2_147_483_647 });
  assert.deepEqual((await poll.next()).value, { revision: 1, value: 2 });
  const next = poll.next();
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(requests, 1);
  await bounded(poll.return(undefined));
  assert.equal((await next).done, true);
});


test("shared producers allow nonzero joins and reset gaps while patches require an exact base", async () => {
  const stream = fixture(snapshot({ n: 7 }, 7, 3) + patch([{ op: "replace", path: "/n", value: 8 }], 8, 7, 4) + snapshot({ n: 20 }, 20, 8) + patch([{ op: "replace", path: "/n", value: 21 }], 21, 20, 8), { close: true });
  const values = [];
  for await (const event of stream.client.watch("shared")) values.push(event);
  assert.deepEqual(values, [
    { revision: 3, value: { n: 7 } }, { revision: 4, value: { n: 8 } },
    { revision: 8, value: { n: 20 } }, { revision: 8, value: { n: 21 } },
  ]);
  assert.throws(() => decodeWatchEvent({ event: "patch", data: JSON.stringify({ sequence: 20, baseSequence: 8, revision: 8, patch: [] }) }, 8, 4), /consecutive/);
});
