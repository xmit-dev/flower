import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import test from "node:test";
import { parseOptions } from "./config.mjs";
import { mergeRouting, startRustDriver } from "./rust-driver.mjs";

const cluster = { members: [1, 2, 3].map((id) => ({ id, url: `http://localhost:${9000 + id}` })), leader: { id: 1 }, adminToken: "private-token" };
const fixture = (action, inspect) => (binary, args, options) => {
  inspect?.(binary, args, options);
  return spawn(process.execPath, ["--input-type=module", "-e", `
    import {createInterface} from 'node:readline';
    const send = value => process.stdout.write(JSON.stringify(value)+'\\n');
    let config;
    for await (const line of createInterface({input:process.stdin})) {
      const value=JSON.parse(line);
      if (!config) { config=value; ${action === "bad-ready" ? "send({type:'ready',schemaVersion:2});" : "send({type:'ready',schemaVersion:1,warmupRequests:config.members.length});"} }
      else {
        if(value.type!=='start'||config.adminToken!=='private-token') process.exit(2);
        ${action === "success" ? "send({type:'interval',elapsedMs:1});send({type:'done',elapsedMs:2,queryRouting:{}});" : action === "bad-json" ? "process.stdout.write('not-json\\n');" : action === "early-exit" ? "process.exit(7);" : "await new Promise(()=>{});"}
      }
    }
  `], options);
};

test("native bridge keeps setup private and delivers disjoint intervals before completion", async () => {
  const messages = [], lifecycle = [];
  const options = { ...parseOptions([]), onProcess: (event) => lifecycle.push(event) };
  const driver = await startRustDriver(cluster, options, { onInterval: (message) => messages.push(message),
    spawnProcess: fixture("success", (binary, args) => { assert.equal(binary, options.driverBinary); assert.deepEqual(args, []); }) });
  assert.equal(driver.warmupRequests, 3);
  const result = await driver.start(Date.now() + 1000);
  assert.equal(result.type, "done");
  assert.deepEqual(messages.map((message) => message.type), ["interval", "done"]);
  assert.equal(driver.completed, result);
  await driver.close();
  assert.deepEqual(lifecycle.map((event) => event.type), ["spawn", "exit"]);
});

test("native bridge rejects malformed and incomplete child output and reaps the process", async () => {
  await assert.rejects(startRustDriver(cluster, parseOptions([]), { spawnProcess: fixture("bad-ready") }), /readiness/);
  for (const action of ["bad-json", "early-exit"]) {
    const driver = await startRustDriver(cluster, parseOptions([]), { spawnProcess: fixture(action) });
    await assert.rejects(driver.start(Date.now() + 1000));
    await driver.close();
    assert.throws(() => process.kill(driver.pid, 0), /ESRCH/);
  }
});

test("native bridge cancellation waits for owned child exit", async () => {
  const controller = new AbortController();
  const driver = await startRustDriver(cluster, parseOptions([]), { signal: controller.signal, spawnProcess: fixture("wait") });
  const pending = driver.start(Date.now() + 10_000);
  controller.abort(new Error("cancel test"));
  await assert.rejects(pending, /cancel test/);
  await driver.close();
  assert.throws(() => process.kill(driver.pid, 0), /ESRCH/);
});

test("routing merge preserves each replica and rejects inconsistent policy or counts", () => {
  const left = { mode: "replicas", consistency: "fresh", auditConsistency: "fresh", nodes: [{ id: 1, url: "one", attempts: 4, completed: 3, failures: 1 }] };
  const right = { ...left, nodes: [{ id: 1, url: "one", attempts: 2, completed: 2, failures: 0 }, { id: 2, url: "two", attempts: 3, completed: 3, failures: 0 }] };
  assert.deepEqual(mergeRouting(left, right).nodes, [{ id: 1, url: "one", attempts: 6, completed: 5, failures: 1 }, right.nodes[1]]);
  assert.equal(left.nodes[0].attempts, 4);
  assert.throws(() => mergeRouting(left, { ...right, consistency: "replica-local" }), /policy/);
  assert.throws(() => mergeRouting(left, { ...right, nodes: [{ ...right.nodes[0], failures: 1 }] }), /counts/);
});
