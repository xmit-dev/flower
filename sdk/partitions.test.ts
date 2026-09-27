import assert from "node:assert/strict";
import test from "node:test";
import { FlowerAdmin, FlowerClient, FlowerError } from "./client.ts";
import type { FlowerRequestInit } from "./client.ts";

function fixture(reply: (body: any, init: FlowerRequestInit) => unknown = () => ({})) {
  const calls: { url: string; init: FlowerRequestInit; body: any }[] = [];
  const fetch = async (url: string, init: FlowerRequestInit) => {
    const body = JSON.parse(init.body); calls.push({ url, init, body });
    return Response.json(await reply(body, init));
  };
  const client = new FlowerClient("http://gateway:7101", { queryUrls: ["http://a:7101", "http://b:7101", "http://c:7101"], fetch });
  const admin = new FlowerAdmin("http://gateway:7101", { adminToken: "operator", fetch });
  return { client, admin, calls };
}

test("partition clients preserve query rotation, pooled transport and mutation identities", async () => {
  const { client, admin, calls } = fixture();
  const partition = client.partition("tenant/a 🍕");
  await partition.query("view"); await partition.query("view"); await partition.query("view");
  await partition.mutate("set", { key: "x" }, { requestId: "once", expectedRevision: 3 });
  const operator = admin.partition("tenant/a 🍕");
  await operator.deploy({ hash: "hash", javascript: "code" }, { requestId: "deploy-once" });
  const path = "/partitions/" + encodeURIComponent("tenant/a 🍕");
  assert.deepEqual(calls.slice(0, 3).map(c => c.url), ["a", "b", "c"].map(n => `http://${n}:7101${path}/v1/query`));
  assert.equal(calls[3].url, `http://gateway:7101${path}/v1/mutate`);
  assert.deepEqual(calls[3].body, { name: "set", args: { key: "x" }, requestId: "once", expectedRevision: 3 });
  assert.equal(calls[3].init.headers.authorization, undefined);
  assert.equal(calls[4].init.headers.authorization, "Bearer operator");
  assert.equal(calls[4].url, `http://gateway:7101${path}/admin/deploy`);
  assert.deepEqual(calls[4].body, { requestId: "deploy-once", bundle: { hash: "hash", javascript: "code" } });
  assert.equal(operator.url, `http://gateway:7101${path}`);
  assert.equal(partition.url, `http://gateway:7101${path}`);
  for (const name of ["", " ", "a\n", "a\0"]) {
    assert.throws(() => client.partition(name), TypeError);
    assert.throws(() => admin.partition(name), TypeError);
  }
});

test("operator actions preserve caller operation IDs and do not add raw data access", async () => {
  const { admin: client, calls } = fixture(); const options = { requestId: "stable", signal: new AbortController().signal };
  await client.registerGroup({ id: "west", addresses: ["west:7101"] });
  await client.createPartition("a", "west", options);
  await client.movePartition("a", "east", options);
  await client.resize(["west", "east"], options);
  await client.partitionStatus("a"); await client.layout(); await client.removeGroup("west");
  assert.deepEqual(calls.map(c => c.body.action), ["register_group", "create", "begin_move", "rebalance", "resolve", "list", "remove_group"]);
  assert.ok(calls.every(c => c.url === "http://gateway:7101/admin/partitions/catalog" && c.init.headers.authorization === "Bearer operator"));
  assert.ok(calls.slice(1, 4).every(c => c.body.operation === "stable" && c.init.signal === options.signal));
});

test("activation waits are cancellable and have a configurable deadline", async () => {
  let attempts = 0;
  const { admin: client } = fixture(() => ({ status: ++attempts < 3 ? "moving" : "active", epoch: 2 }));
  assert.equal((await client.waitForPartition("a", { timeoutMs: 500, intervalMs: 1 })).epoch, 2);
  const waiting = fixture(() => ({ status: "moving" }));
  await assert.rejects(waiting.admin.waitForPartition("a", { timeoutMs: 10, intervalMs: 2 }), (e: any) => e instanceof FlowerError && e.code === "PARTITION_WAIT_TIMEOUT");
  const stop = new AbortController(); const work = waiting.admin.waitForPartition("a", { signal: stop.signal }); stop.abort(new Error("stopped"));
  await assert.rejects(work, /stopped/);
  for (const options of [{ timeoutMs: 0 }, { intervalMs: Infinity }, { timeoutMs: 2 ** 31 }]) await assert.rejects(client.waitForPartition("a", options), TypeError);
});
