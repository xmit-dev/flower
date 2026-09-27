import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { Server } from "node:http";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import test from "node:test";
import { LocalCluster } from "../../bench/cluster.mjs";
import { FlowerAdmin, FlowerClient } from "../../sdk/client.ts";
import { startPizzaDemo } from "./demo.mjs";

const run = promisify(execFile);
const clusterModule = new URL("../../bench/cluster.mjs", import.meta.url).href;
const clientModule = new URL("../../sdk/client.ts", import.meta.url).href;
const demoModule = new URL("./demo.mjs", import.meta.url).href;

test("abort during listener startup closes the eventual listener and rejects startup", async (t) => {
  const controller = new AbortController();
  const original = { start: LocalCluster.prototype.start, close: LocalCluster.prototype.close,
    deploy: FlowerAdmin.prototype.deploy, mutate: FlowerClient.prototype.mutate, listen: Server.prototype.listen };
  let listener, closeCalls = 0;
  LocalCluster.prototype.start = async function () { this.leader = { url: "http://127.0.0.1:1" }; return this; };
  LocalCluster.prototype.close = async () => { closeCalls++; };
  FlowerAdmin.prototype.deploy = async () => ({ revision: 1, value: null, duplicate: false });
  FlowerClient.prototype.mutate = async () => ({ revision: 1, value: null, duplicate: false });
  Server.prototype.listen = function (...args) {
    listener = this;
    const result = original.listen.apply(this, args);
    controller.abort(new Error("interrupt during pending listen"));
    return result;
  };
  t.after(async () => {
    LocalCluster.prototype.start = original.start; LocalCluster.prototype.close = original.close;
    FlowerAdmin.prototype.deploy = original.deploy; FlowerClient.prototype.mutate = original.mutate;
    Server.prototype.listen = original.listen;
    if (listener?.listening) await new Promise((resolve) => listener.close(resolve));
  });
  await assert.rejects(startPizzaDemo({ auto: false, signal: controller.signal }), /closed/);
  assert.ok(listener, "the test interrupts after listener acquisition begins");
  assert.equal(listener.listening, false);
  assert.equal(closeCalls, 1);
});

test("dashboard streams rotate across running replicas without leader discovery", async (t) => {
  const original = { start: LocalCluster.prototype.start, close: LocalCluster.prototype.close,
    discover: LocalCluster.prototype.discoverLeader, deploy: FlowerAdmin.prototype.deploy,
    mutate: FlowerClient.prototype.mutate, fetch: globalThis.fetch };
  const members = [1, 2, 3].map((id) => ({ id, url: `http://127.0.0.1:${12340 + id}`, process: { ended: false, intentional: false } }));
  const watched = [];
  LocalCluster.prototype.start = async function () { this.members = members; this.leader = members[0]; return this; };
  LocalCluster.prototype.close = async () => {};
  LocalCluster.prototype.discoverLeader = async () => { throw new Error("Dashboard reads must not discover a leader"); };
  FlowerAdmin.prototype.deploy = async () => ({ revision: 1, value: null, duplicate: false });
  FlowerClient.prototype.mutate = async () => ({ revision: 1, value: null, duplicate: false });
  globalThis.fetch = async (url, options) => {
    if (members.some((member) => String(url).startsWith(member.url + "/"))) {
      watched.push({ url: String(url), body: JSON.parse(options.body) });
      return new Response("event: snapshot\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } });
    }
    return original.fetch(url, options);
  };
  let demo;
  t.after(async () => {
    await demo?.close();
    LocalCluster.prototype.start = original.start; LocalCluster.prototype.close = original.close;
    LocalCluster.prototype.discoverLeader = original.discover;
    FlowerAdmin.prototype.deploy = original.deploy; FlowerClient.prototype.mutate = original.mutate;
    globalThis.fetch = original.fetch;
  });
  demo = await startPizzaDemo({ auto: false });
  for (let index = 0; index < 4; index++) {
    const response = await original.fetch(demo.url + "/v1/watch", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "pizza.dashboard", args: { tenant: `tenant-${index % 3}` } }),
    });
    assert.equal(response.status, 200);
    await response.text();
  }
  assert.deepEqual(watched.map(({ url }) => url), [1, 2, 3, 1].map((id) => `http://127.0.0.1:${12340 + id}/v1/watch`));
  assert.deepEqual(watched.map(({ body }) => body.args.tenant), ["tenant-0", "tenant-1", "tenant-2", "tenant-0"]);
  members[1].process.ended = true;
  const response = await original.fetch(demo.url + "/v1/watch", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "pizza.dashboard", args: { tenant: "tenant-0" } }),
  });
  await response.text();
  assert.notEqual(watched.at(-1).url, members[1].url + "/v1/watch");
});

const stubbedCli = `
  import { Server } from "node:http";
  import { LocalCluster } from ${JSON.stringify(clusterModule)};
  import { FlowerAdmin, FlowerClient } from ${JSON.stringify(clientModule)};
  LocalCluster.prototype.start = async function () { this.leader = { url: "http://127.0.0.1:1" }; return this; };
  LocalCluster.prototype.close = async function () {};
  FlowerAdmin.prototype.deploy = async () => ({ revision: 1, value: null, duplicate: false });
  FlowerClient.prototype.mutate = async () => ({ revision: 1, value: null, duplicate: false });
  process.argv = [process.execPath, ${JSON.stringify(fileURLToPath(demoModule))}, "--duration", "3600", "--paused"];
`;

test("CLI SIGINT after readiness exits despite a long duration timer", async () => {
  const { stdout, stderr } = await run(process.execPath, ["--input-type=module", "--eval", stubbedCli + `
    const log = console.log;
    console.log = (...args) => { log(...args); setImmediate(() => process.emit("SIGINT")); };
    await import(${JSON.stringify(demoModule)});
  `], { timeout: 3_000, maxBuffer: 16_384 });
  assert.match(stdout, /Goblin Pizza live:/);
  assert.equal(stderr, "");
});

test("CLI SIGINT during startup exits cleanly without announcing a ready server", async () => {
  const { stdout, stderr } = await run(process.execPath, ["--input-type=module", "--eval", stubbedCli + `
    const listen = Server.prototype.listen;
    Server.prototype.listen = function (...args) {
      const result = listen.apply(this, args);
      process.emit("SIGINT");
      return result;
    };
    await import(${JSON.stringify(demoModule)});
  `], { timeout: 3_000, maxBuffer: 16_384 });
  assert.equal(stdout, "");
  assert.equal(stderr, "");
});
