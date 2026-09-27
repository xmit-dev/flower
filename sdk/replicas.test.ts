import assert from "node:assert/strict";
import test from "node:test";
import { FlowerAdmin, FlowerClient, FlowerError } from "./client.ts";
import type { FlowerRequestInit } from "./client.ts";

test("parallel queries rotate replicas while writes, call, and operator calls stay primary", async () => {
  const calls: { url: string; init: FlowerRequestInit }[] = [];
  const origins = ["http://replica-1:7101/", "http://replica-2:7101/"];
  const fetch = async (url: string, init: FlowerRequestInit) => {
    calls.push({ url, init });
    return Response.json({ revision: 1, value: null, duplicate: false });
  };
  const client = new FlowerClient("http://primary:7101", { queryUrls: origins, fetch });
  const admin = new FlowerAdmin("http://primary:7101", { adminToken: "test-secret", fetch });
  origins[0] = "http://changed:7101";
  const signal = new AbortController().signal;
  await Promise.all(Array.from({ length: 6 }, (_, args) => client.query("read", args, { signal })));
  assert.deepEqual(calls.map(({ url }) => url), Array.from({ length: 6 }, (_, index) =>
    `http://replica-${index % 2 + 1}:7101/v1/query`));
  for (const [index, { init }] of calls.entries()) {
    assert.deepEqual(JSON.parse(init.body), { name: "read", args: index });
    assert.equal(init.signal, signal);
    assert.equal(init.headers.authorization, undefined);
  }
  await client.mutate("write", null, { requestId: "stable" });
  await client.call("readOrWrite");
  await admin.deploy({ hash: "h", javascript: "bundle" });
  await admin.initialize({ "1": "primary:7101" });
  assert.deepEqual(calls.slice(6).map(({ url }) => url), [
    "http://primary:7101/v1/mutate", "http://primary:7101/v1/call",
    "http://primary:7101/admin/deploy", "http://primary:7101/raft/initialize",
  ]);
  assert.equal(calls[6].init.headers.authorization, undefined);
  assert.equal(calls[7].init.headers.authorization, undefined);
  assert.equal(calls[8].init.headers.authorization, "Bearer test-secret");
  assert.equal(calls[9].init.headers.authorization, "Bearer test-secret");
});

test("new SSE watches share query rotation and remain on their selected endpoint", async () => {
  const calls: { url: string; init: FlowerRequestInit }[] = [];
  const client = new FlowerClient("http://primary:7101", {
    queryUrls: ["http://one:7101", "http://two:7101"],
    fetch: async (url, init) => {
      calls.push({ url, init });
      if (url.endsWith("/v1/query")) return Response.json({ revision: 1, value: 1 });
      return new Response(
        'event: snapshot\ndata: {"sequence":0,"revision":1,"value":1}\n\n' +
        'event: snapshot\ndata: {"sequence":1,"revision":2,"value":2}\n\n',
        { headers: { "content-type": "text/event-stream" } },
      );
    },
  });
  const first = client.watch("read");
  assert.equal(calls.length, 0, "constructing a lazy watcher must not consume a replica slot");
  assert.deepEqual((await first.next()).value, { revision: 1, value: 1 });
  await client.query("read");
  const second = client.watchDeltas("read");
  await second.next();
  assert.deepEqual((await first.next()).value, { revision: 2, value: 2 });
  await first.return(undefined);
  await second.return(undefined);
  assert.deepEqual(calls.map(({ url }) => url), [
    "http://one:7101/v1/watch", "http://two:7101/v1/query", "http://one:7101/v1/watch",
  ]);
  assert.ok(calls[0].init.signal?.aborted);
  assert.ok(calls[2].init.signal?.aborted);
  assert.deepEqual(JSON.parse(calls[0].init.body), { name: "read", args: null });
});

test("polling watches choose once so replicas cannot move their revisions backwards", async () => {
  const calls: string[] = [];
  const client = new FlowerClient("http://primary:7101", {
    queryUrls: ["http://one:7101", "http://two:7101"],
    fetch: async (url) => {
      calls.push(url);
      return Response.json({ revision: calls.length, value: null });
    },
  });
  const watcher = client.watchPoll("read", null, { intervalMs: 1 });
  await watcher.next();
  await client.query("read");
  await watcher.next();
  await watcher.return(undefined);
  assert.deepEqual(calls, [
    "http://one:7101/v1/query", "http://two:7101/v1/query", "http://one:7101/v1/query",
  ]);
});

test("replica errors surface without retry or changing the consistency request", async () => {
  const calls: string[] = [];
  const client = new FlowerClient("http://primary:7101", {
    queryUrls: ["http://one:7101", "http://two:7101"],
    fetch: async (url) => {
      calls.push(url);
      return Response.json({ error: { code: "UNAVAILABLE", message: "no quorum" } }, { status: 503 });
    },
  });
  await assert.rejects(client.query("fresh"), (error: unknown) =>
    error instanceof FlowerError && error.status === 503 && error.code === "UNAVAILABLE");
  assert.deepEqual(calls, ["http://one:7101/v1/query"]);
});

test("replica endpoint configuration rejects empty, sparse and non-HTTP inputs", () => {
  for (const queryUrls of [[], "http://one", [null], ["file:///data"], ["ftp://one"], new Array(1)]) {
    assert.throws(() => new FlowerClient("http://primary:7101", { queryUrls: queryUrls as any }), TypeError);
  }
});
