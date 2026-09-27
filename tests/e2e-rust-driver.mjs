#!/usr/bin/env node
// Native-driver contract test against real pooled HTTP/2 connections. It does
// not measure database throughput; the independent pizza audit remains in run().
import assert from "node:assert/strict";
import { createServer } from "node:http2";
import { once } from "node:events";
import { parseOptions } from "../bench/config.mjs";
import { Stats } from "../bench/metrics.mjs";
import { startRustDriver } from "../bench/rust-driver.mjs";

const servers = [], sessions = new Set(), receipts = new Map(), bodies = new Map();
const attempts = [0, 0, 0], connections = [0, 0, 0];
const acknowledgedOrders = [], acknowledgedTips = {};
let revision = 0, lostAcknowledgement = false, failedRead = false;
const stats = new Stats();
const intervals = [];
let driver;
try {
  for (let index = 0; index < 3; index++) {
    const server = createServer();
    server.on("session", (session) => {
      sessions.add(session); connections[index]++;
      session.on("error", () => {});
      session.on("close", () => sessions.delete(session));
    });
    server.on("stream", (stream, headers) => {
      stream.on("error", () => {});
      let text = "";
      stream.setEncoding("utf8"); stream.on("data", (chunk) => { text += chunk; });
      stream.on("end", () => {
        const request = JSON.parse(text);
        let status = 200, result;
        if (headers[":path"] === "/v1/query") {
          attempts[index]++;
          result = { revision, value: { preview: true } };
          // Warmup completes on all three first; then a read must retry elsewhere.
          if (!failedRead && attempts[index] > 1) { failedRead = true; status = 503; }
        } else {
          assert.equal(index, 0, "writes retain the discovered leader address");
          const previous = bodies.get(request.requestId);
          if (previous) assert.equal(text, previous, "uncertain retry and explicit replay preserve exact request bytes");
          bodies.set(request.requestId, text);
          const receipt = receipts.get(request.requestId);
          if (receipt) result = { ...receipt, duplicate: true };
          else {
            result = { revision: ++revision, value: request.args, duplicate: false };
            receipts.set(request.requestId, result);
            if (!lostAcknowledgement) { lostAcknowledgement = true; status = 503; }
          }
        }
        stream.respond({ ":status": status, "content-type": "application/json" });
        stream.end(JSON.stringify(status === 200 ? result : { error: { code: "RETRY", message: "retry test" } }));
      });
    });
    server.listen(0, "127.0.0.1"); await once(server, "listening"); servers.push(server);
  }
  const members = servers.map((server, index) => ({ id: index + 1, url: `http://127.0.0.1:${server.address().port}` }));
  const options = { ...parseOptions([]), driver: "rust", driverBinary:process.env.E2E_DRIVER_BIN??parseOptions([]).driverBinary, offeredRate:Number(process.env.E2E_OFFERED_RATE??0), http2: true, concurrency: 4, maxOrders: 3,
    duplicateRate: 1, requestTimeoutMs: 100, retryBudgetMs: 2000, tenantIds: ["tenant-0", "tenant-1"] };
  driver = await startRustDriver({ members, leader: members[0], adminToken: "test-token" }, options, { onInterval: (message) => {
    intervals.push(message);
    stats.merge(message.stats); assert.equal(message.failureCount, 0, JSON.stringify(message.failures));
    acknowledgedOrders.push(...message.orders);
    for (const [shop, amount] of Object.entries(message.tips)) acknowledgedTips[shop] = (acknowledgedTips[shop] ?? 0) + amount;
  } });
  const final = await driver.start(Date.now() + 1250);
  const snapshot = stats.snapshot(final.elapsedMs);
  assert.ok(intervals.some((message) => message.type === "interval"), "driver reports disjoint live intervals");
  assert.ok(Math.abs(intervals.reduce((sum, message) => sum + message.stats.durationMs, 0) - final.elapsedMs) < 0.001, "interval durations cover the full measured run exactly once");
  assert.ok(snapshot.operations.completed > 10, "driver exercised customer loops");
  assert.equal(acknowledgedOrders.length, 3);
  if(options.offeredRate>0){
    const counts=intervals.reduce((sum,x)=>Object.fromEntries(Object.entries(sum).map(([key,n])=>[key,n+x.offered[key]])),{offered:0,dispatched:0,driverDropped:0,completed:0,failed:0});
    assert.equal(counts.offered,counts.dispatched+counts.driverDropped);
    assert.equal(counts.dispatched,counts.completed+counts.failed);
    assert.ok(counts.driverDropped>0,"saturated driver counts independent arrivals rather than self-throttling");
    assert.ok(counts.offered>options.offeredRate,"offered arrival clock spans the requested interval");
  }

  assert.ok(snapshot.retries >= 2, "both a failed read and an uncertain mutation retried");
  assert.ok(snapshot.operations.perMethod["pizza.tip.replay"].completed > 0);
  assert.ok(final.queryRouting.nodes.every((node) => node.completed > 1), "all three replicas received read traffic");
  assert.ok(connections.every((count) => count < 12), "connections are reused across hundreds of requests");
  const receivedOrders = [], receivedTips = {};
  for (const receipt of receipts.values()) {
    if (receipt.value.id) receivedOrders.push(receipt.value);
    else {
      const shop = JSON.stringify(receipt.value.shop);
      receivedTips[shop] = (receivedTips[shop] ?? 0) + receipt.value.amount;
    }
  }
  assert.deepEqual(acknowledgedOrders.sort((a, b) => a.id.localeCompare(b.id)), receivedOrders.sort((a, b) => a.id.localeCompare(b.id)));
  assert.deepEqual(acknowledgedTips, receivedTips);
  console.log(`Native driver contract PASS: ${snapshot.operations.completed} calls; all three HTTP/2 replicas, pooled sessions, uncertain retry, replay, histograms and independent ledger agree.`);
} finally {
  await driver?.close();
  for (const session of sessions) session.destroy();
  await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))));
}
