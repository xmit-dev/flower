// Run after cargo build: node tests/e2e-watch.mjs
// Wire-level SSE assertions are independent of the SDK patch reconstructor.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient, FlowerError } from "../sdk/index.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = process.env.E2E_FLOWER_BIN ? resolve(process.env.E2E_FLOWER_BIN) : join(root, "target/debug/flower");
const cluster = new LocalCluster({ nodes: 3, binary });
const probes = new Set();
const iterators = new Set();
const transports = new Set();
let request = 0;

async function within(promise, label, timeoutMs = 10_000) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs} ms`)), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}

class EventTimeout extends Error {}
class Probe {
  constructor(response, controller) {
    this.controller = controller;
    this.reader = response.body.getReader();
    this.queue = [];
    this.waiters = [];
    this.closed = false;
    this.done = this.pump();
    probes.add(this);
  }
  emit(value) {
    const waiter = this.waiters.shift();
    if (waiter) { clearTimeout(waiter.timer); waiter.resolve(value); }
    else this.queue.push(value);
  }
  async pump() {
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      while (true) {
        const { value, done } = await this.reader.read();
        if (done) break;
        buffer = (buffer + decoder.decode(value, { stream: true })).replaceAll("\r\n", "\n");
        assert.ok(buffer.length < 4 * 1024 * 1024, "fixture SSE event unexpectedly large");
        let end;
        while ((end = buffer.indexOf("\n\n")) !== -1) {
          const block = buffer.slice(0, end);
          buffer = buffer.slice(end + 2);
          let event = "message", id;
          const data = [];
          for (const line of block.split("\n")) {
            if (line.startsWith(":")) continue;
            const colon = line.indexOf(":");
            const key = colon === -1 ? line : line.slice(0, colon);
            const raw = colon === -1 ? "" : line.slice(colon + 1);
            const field = raw.startsWith(" ") ? raw.slice(1) : raw;
            if (key === "event") event = field;
            if (key === "id") id = field;
            if (key === "data") data.push(field);
          }
          if (data.length) this.emit({ event, id, data: JSON.parse(data.join("\n")) });
        }
      }
    } catch (error) {
      if (!this.controller.signal.aborted) this.error = error;
    } finally {
      this.closed = true;
      for (const waiter of this.waiters.splice(0)) {
        clearTimeout(waiter.timer);
        waiter.reject(this.error ?? new Error("SSE stream closed"));
      }
      this.reader.releaseLock();
    }
  }
  next(timeoutMs = 10_000) {
    if (this.queue.length) return Promise.resolve(this.queue.shift());
    if (this.closed) return Promise.reject(this.error ?? new Error("SSE stream closed"));
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject };
      waiter.timer = setTimeout(() => {
        this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
        reject(new EventTimeout("No SSE data event"));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }
  async quiet(ms = 650) {
    await assert.rejects(this.next(ms), EventTimeout, "unchanged values must not emit data events");
  }
  async close() {
    this.controller.abort();
    await within(this.done, "cancel raw SSE stream");
    probes.delete(this);
  }
}

async function watchRaw(name, { url = cluster.url, args = null, headers = {}, fetcher = fetch } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("SSE response headers timed out")), 10_000);
  let response;
  try {
    response = await fetcher(url + "/v1/watch", {
      method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream", ...headers },
      body: JSON.stringify({ name, args }), signal: controller.signal,
    });
    assert.equal(response.status, 200, response.status === 200 ? "" : await response.text());
    assert.match(response.headers.get("content-type") ?? "", /^text\/event-stream\b/);
    return new Probe(response, controller);
  } catch (error) { controller.abort(); throw error; }
  finally { clearTimeout(timer); }
}

function iterator(value) { iterators.add(value); return value; }
async function stop(value) {
  await within(value.return(), "cancel SDK watch");
  iterators.delete(value);
}
async function next(value) {
  const result = await within(value.next(), "receive SDK watch event");
  assert.equal(result.done, false, "watch ended before its event");
  return result.value;
}
async function mutate(client, args) {
  return client.mutate("public.change", args, { requestId: `watch-change-${request++}`, signal: AbortSignal.timeout(10_000) });
}
function assertSnapshot(event, sequence = 0) {
  assert.equal(event.event, "snapshot");
  assert.equal(event.id, String(sequence));
  assert.equal(event.data.sequence, sequence);
  assert.ok(Number.isSafeInteger(event.data.revision));
}
function assertPatch(event, sequence, path, value, op = "replace") {
  assert.equal(event.event, "patch");
  assert.equal(event.id, String(sequence));
  assert.equal(event.data.sequence, sequence);
  assert.equal(event.data.baseSequence, sequence - 1);
  assert.deepEqual(event.data.patch, [{ op, path, value }]);
}

function source(expose = true) {
  return `import { collection, define, fail, mutation, query } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const records = collection<any>("watch.records");
const setup = mutation("internal.watch.setup", ctx => {
  const fields: Record<string,string> = Object.create(null);
  for (let index = 0; index < 80; index++) fields["key-" + String(index).padStart(3, "0")] = "unchanged payload " + "x".repeat(64);
  fields["a/b"] = "slash"; fields["a~b"] = "tilde"; fields["__proto__"] = "ordinary own field";
  ctx.set(records, "watched", { fields, items: [] });
  ctx.set(records, "small", 0);
  return null;
});
const change = mutation("internal.watch.change", (ctx, args: any) => {
  if (args.kind === "unrelated") { ctx.set(records, "unrelated", args.value); return null; }
  if (args.kind === "small") { ctx.set(records, "small", args.value); return null; }
  if (args.kind === "guard") { ctx.set(records, "guard", args.value); return null; }
  const record = ctx.get(records, "watched");
  if (args.kind === "append") record.items.push(args.value);
  else record.fields[args.key] = args.value;
  ctx.set(records, "watched", record);
  return null;
});
const read = query("internal.watch.read", ctx => ctx.get(records, "watched"));
const small = query("internal.watch.small", ctx => ctx.get(records, "small"));
const time = query("internal.watch.time", ctx => ctx.now());
const guard = query("internal.watch.guard", ctx => {
  const value = ctx.get(records, "guard");
  if (value?.locked) fail("WATCH_GUARD", "The guarded record is locked", { reason: value.reason, since: value.since });
  return value;
});
const secret = query("internal.watch.secret", () => "must stay private");
export default define({ definitions: [secret], http: {
  "public.setup": setup, "public.change": change, "public.small": small, "public.time": time, "public.guard": guard,
  ${expose ? '"public.watch": read,' : ""}
}});`;
}

const interrupt = () => {
  for (const probe of probes) probe.controller.abort();
  for (const transport of transports) void transport.close();
  void cluster.close();
};
process.once("SIGINT", interrupt);
process.once("SIGTERM", interrupt);

try {
  await cluster.start();
  const fixture = join(cluster.directory, "watch-app.ts");
  await writeFile(fixture, source());
  const bundle = await buildBundle(fixture);
  let client = new FlowerClient(cluster.url);
  const admin = new FlowerAdmin(cluster.url, { adminToken: cluster.adminToken });
  await admin.deploy(bundle, { requestId: "watch-deploy" });
  await client.mutate("public.setup", null, { requestId: "watch-setup" });

  for (const [name, status, code] of [
    ["internal.watch.secret", 404, "METHOD_NOT_FOUND"],
    ["public.change", 422, "METHOD_KIND_MISMATCH"],
  ]) {
    const response = await fetch(cluster.url + "/v1/watch", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name }),
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, status);
    assert.equal((await response.json()).error.code, code);
  }

  const raw = await watchRaw("public.watch");
  const initial = await raw.next();
  assertSnapshot(initial);
  assert.deepEqual(initial.data.value, (await client.query("public.watch")).value);
  const unrelated = await mutate(client, { kind: "unrelated", value: 1 });
  assert.ok(unrelated.revision > initial.data.revision);
  await raw.quiet();
  let sequence = 0;
  for (const [key, path] of [["key-010", "/fields/key-010"], ["a/b", "/fields/a~1b"], ["a~b", "/fields/a~0b"], ["__proto__", "/fields/__proto__"]]) {
    const value = `updated ${key}`;
    const receipt = await mutate(client, { kind: "field", key, value });
    const event = await raw.next();
    assertPatch(event, ++sequence, path, value);
    assert.equal(event.data.revision, receipt.revision);
    assert.ok(JSON.stringify(event.data).length < JSON.stringify(initial.data).length / 10, "sparse changes must not retransmit the large object");
  }
  await mutate(client, { kind: "append", value: "first parcel" });
  const appended = await raw.next();
  assert.equal(appended.data.patch.length, 1, "append should need one patch operation");
  const appendPath = appended.data.patch[0].path;
  assert.ok(appendPath === "/items/0" || appendPath === "/items/-");
  assertPatch(appended, ++sequence, appendPath, "first parcel", "add");
  await raw.close();

  const small = await watchRaw("public.small");
  assertSnapshot(await small.next());
  await mutate(client, { kind: "small", value: 1 });
  const fallback = await small.next();
  assertSnapshot(fallback, 1);
  assert.equal(fallback.data.value, 1, "small changes should use a cheaper replacement snapshot");
  await small.close();

  // A watched query that starts failing ends its stream with the method's own
  // structured failure, on the wire and through every SDK stream API.
  const guardFailure = { code: "WATCH_GUARD", message: "The guarded record is locked", details: { reason: "maintenance window", since: 7 } };
  const isGuardFailure = (error) => error instanceof FlowerError && error.status === 422 && error.code === "EVALUATION_FAILED" &&
    error.message === "WATCH_GUARD: The guarded record is locked" && JSON.stringify(error.failure) === JSON.stringify(guardFailure);
  const guarded = await watchRaw("public.guard");
  assertSnapshot(await guarded.next());
  const guardedSdk = iterator(client.watch("public.guard"));
  assert.equal((await next(guardedSdk)).value, null);
  const guardedSubscription = iterator(client.subscribe("public.guard", null, { reconnect: { initialDelayMs: 10, maxDelayMs: 20 } }));
  assert.equal((await next(guardedSubscription)).reset, true);
  await mutate(client, { kind: "guard", value: { locked: true, reason: "maintenance window", since: 7 } });
  const guardError = await guarded.next();
  assert.equal(guardError.event, "error");
  assert.deepEqual(guardError.data, { error: {
    code: "EVALUATION_FAILED", message: "WATCH_GUARD: The guarded record is locked", status: 422, failure: guardFailure,
  } });
  await within(guarded.done, "server closes the failed watch stream");
  await guarded.close();
  await assert.rejects(within(guardedSdk.next(), "SDK watch receives the failure"), isGuardFailure);
  await stop(guardedSdk);
  // A method's failure is not transient: subscribe surfaces it instead of reconnecting.
  await assert.rejects(within(guardedSubscription.next(), "subscription surfaces the failure"), isGuardFailure);
  await stop(guardedSubscription);
  // Opening a watch on a failing query fails before streaming, with the same failure.
  const refused = await fetch(cluster.url + "/v1/watch", {
    method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream" },
    body: JSON.stringify({ name: "public.guard", args: null }), signal: AbortSignal.timeout(10_000),
  });
  assert.equal(refused.status, 422);
  assert.deepEqual((await refused.json()).error.failure, guardFailure);
  const refusedSdk = iterator(client.watchDeltas("public.guard"));
  await assert.rejects(within(refusedSdk.next(), "SDK watch refused before streaming"), isGuardFailure);
  await stop(refusedSdk);
  await mutate(client, { kind: "guard", value: { locked: false } });
  assert.deepEqual((await client.query("public.guard")).value, { locked: false });

  const timed = await watchRaw("public.time");
  const beforeTime = await timed.next();
  assertSnapshot(beforeTime);
  const afterTime = await timed.next();
  assert.equal(afterTime.data.revision, beforeTime.data.revision, "query time changes without a durable revision");
  assert.equal(afterTime.data.sequence, 1);
  assertSnapshot(afterTime, 1);
  assert.ok(afterTime.data.value > beforeTime.data.value);
  await timed.close();

  const reconstructed = iterator(client.watch("public.watch"));
  const firstValue = await next(reconstructed);
  firstValue.value.fields["key-030"] = "caller mutation must not poison reconstruction";
  firstValue.value.items.push("caller-only parcel");
  await mutate(client, { kind: "field", key: "key-031", value: "SDK patch" });
  const updated = await next(reconstructed);
  const current = await client.query("public.watch");
  assert.deepEqual(updated, { revision: current.revision, value: current.value });
  const pendingReturn = reconstructed.next();
  const returned = reconstructed.return();
  assert.equal((await within(pendingReturn, "pending next after iterator return")).done, true);
  assert.equal((await within(returned, "iterator return while awaiting data")).done, true);
  iterators.delete(reconstructed);

  const abort = new AbortController();
  const cancellable = iterator(client.watchDeltas("public.watch", null, { signal: abort.signal }));
  assert.equal((await next(cancellable)).type, "snapshot");
  const pendingAbort = cancellable.next();
  abort.abort();
  assert.equal((await within(pendingAbort, "signal cancellation")).done, true);
  await stop(cancellable);

  const reconnect = await watchRaw("public.watch", { headers: { "Last-Event-ID": "9000" } });
  const reset = await reconnect.next();
  assertSnapshot(reset);
  assert.deepEqual(reset.data.value, (await client.query("public.watch")).value);
  await reconnect.close();

  const transport = createHttp2Transport({ requestTimeoutMs: 10_000 });
  transports.add(transport);
  const h2 = new FlowerClient(cluster.url, { fetch: transport.fetch });
  const multiplexed = iterator(h2.watchDeltas("public.watch"));
  const h2Initial = await next(multiplexed);
  assert.equal(h2Initial.type, "snapshot");
  assert.equal(h2Initial.sequence, 0, "HTTP/2 response body must stream before the request ends");
  const [h2Receipt, h2Read] = await Promise.all([
    mutate(h2, { kind: "field", key: "key-020", value: "HTTP/2 delta" }),
    h2.query("public.watch"),
  ]);
  assert.ok(Number.isSafeInteger(h2Read.revision));
  const h2Patch = await next(multiplexed);
  assert.equal(h2Patch.type, "patch");
  assert.equal(h2Patch.revision, h2Receipt.revision);
  assert.deepEqual(h2Patch.patch, [{ op: "replace", path: "/fields/key-020", value: "HTTP/2 delta" }]);
  await stop(multiplexed);
  await transport.close();
  transports.delete(transport);

  const revokedRaw = await watchRaw("public.watch");
  await revokedRaw.next();
  const revokedSdk = iterator(client.watchDeltas("public.watch"));
  await next(revokedSdk);
  await writeFile(fixture, source(false));
  await admin.deploy(await buildBundle(fixture), { requestId: "watch-revoke" });
  const removed = await revokedRaw.next();
  assert.equal(removed.event, "error");
  assert.equal(removed.data.error.code, "METHOD_NOT_FOUND");
  assert.equal(removed.data.error.status, 404);
  await within(revokedRaw.done, "server closes revoked alias stream");
  await revokedRaw.close();
  await assert.rejects(within(revokedSdk.next(), "SDK receives revoked alias"), (error) => error instanceof FlowerError && error.code === "METHOD_NOT_FOUND" && error.status === 404);
  await stop(revokedSdk);
  await admin.deploy(bundle, { requestId: "watch-restore" });

  const oldLeader = iterator(client.watchDeltas("public.watch"));
  await next(oldLeader);
  const loss = oldLeader.next().then((value) => ({ value }), (error) => ({ error }));
  const election = await cluster.crashLeaderAndRecover();
  assert.notEqual(election.newLeader, election.oldLeader);
  const ended = await within(loss, "old leader stream terminates");
  assert.ok(ended.error || ended.value?.done, "connection loss must end the old watch instead of silently inventing continuity");
  await stop(oldLeader);
  client = new FlowerClient(cluster.url);
  const afterElection = iterator(client.watchDeltas("public.watch"));
  const fresh = await next(afterElection);
  assert.equal(fresh.type, "snapshot");
  assert.equal(fresh.sequence, 0);
  assert.deepEqual(fresh.value, (await client.query("public.watch")).value);
  await mutate(client, { kind: "field", key: "key-040", value: "after election" });
  const electedPatch = await next(afterElection);
  assert.equal(electedPatch.type, "patch");
  assert.equal(electedPatch.baseSequence, 0);
  assert.equal(electedPatch.sequence, 1);
  assert.deepEqual(electedPatch.patch, [{ op: "replace", path: "/fields/key-040", value: "after election" }]);
  await stop(afterElection);
  console.log("PASS: SSE snapshots, sparse/escaped JSON patches, array append, unchanged-value suppression, structured watch failures, clock refresh, SDK reconstruction/cancellation, h2 multiplexing, allowlist revocation and fresh reconnect after leader loss");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await Promise.allSettled([...probes].map((probe) => probe.close()));
  await Promise.allSettled([...iterators].map((value) => within(value.return(), "cleanup SDK watch")));
  await Promise.allSettled([...transports].map((transport) => transport.close()));
  await cluster.close();
  process.removeListener("SIGINT", interrupt);
  process.removeListener("SIGTERM", interrupt);
}
