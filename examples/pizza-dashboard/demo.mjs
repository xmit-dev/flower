#!/usr/bin/env node
// Development launcher: Rust/QuickJS runs the database; Node builds browser
// assets, owns a temporary cluster, and generates example customers/workers.
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { readFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { setTimeout as delay } from "node:timers/promises";
import { parseArgs } from "node:util";
import { build } from "esbuild";
import { LocalCluster } from "../../bench/cluster.mjs";
import { buildBundle } from "../../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient, FlowerError } from "../../sdk/client.ts";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../..");
const tenants = ["tenant-0", "tenant-1", "tenant-2"];
const storesPerTenant = 2;
const shops = tenants.flatMap((tenant) => Array.from({ length: storesPerTenant }, (_, index) => [tenant, `store-${index}`]));
const bakeMs = 1_800;
const leaseMs = 4_000;
// Orders arrive as a Poisson process at this mean rate across all kitchens.
// Fast enough to keep every stage busy; slow enough that each tenant's board
// still shows an order travel from the oven to the doorstep.
export const DEFAULT_RATE = 40;
export const MAX_RATE = 150;
// Delivered orders stay on the board this long, then pizza.archive folds them
// into their kitchen's tally. Every call still leaves state behind, such as
// its retry receipt: about 1 MB/s across the replicas at the default pace. So
// arrivals stop after about an hour, before memory gets silly.
const archiveAfterMs = 10_000;
export const DEFAULT_MAX_ORDERS = 150_000;
// Customers walk away rather than queue behind this many unanswered orders,
// e.g. while the cluster elects a new leader.
const maxPendingOrders = 256;
const flightMs = [500, 1_500];
const lostDroneRate = 0.004;
const latencyWindowMs = 10_000;
const failure = (error) => error instanceof FlowerError ? error.failure?.code ?? error.code : undefined;

// Enough drones per tenant to fly its share of arrivals, with headroom for bursts.
export function fleetSize(rate) {
  const perTenant = rate / tenants.length;
  const cycleSeconds = (flightMs[0] + flightMs[1]) / 2_000 + 0.1;
  return Math.max(2, Math.ceil(perTenant * cycleSeconds * 1.6) + 1);
}

function checkRate(rate) {
  if (!Number.isFinite(rate) || rate < 1 || rate > MAX_RATE) throw new Error(`rate must be 1..${MAX_RATE} orders per second`);
  return rate;
}

export async function startPizzaDemo({ port = 0, binary, auto = true, signal, rate = DEFAULT_RATE, maxOrders = DEFAULT_MAX_ORDERS } = {}) {
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535) throw new Error("port must be 0..65535");
  if (!Number.isSafeInteger(maxOrders) || maxOrders < 1) throw new Error("maxOrders must be a positive integer");
  checkRate(rate);
  const shutdown = new AbortController();
  const cluster = new LocalCluster({ nodes: 3, binary });
  const connections = new Set();
  const tasks = [];
  const clients = new Map();
  const streams = new Map();
  const latencies = [];
  let oldestLatency = 0;
  const roosts = new Map(tenants.map((tenant) => [tenant, { resting: new Set(), bells: [] }]));
  const counters = { placed: 0, delivered: 0, pizzas: 0, tips: 0, archived: 0, claims: 0, emptyClaims: 0, walkedAway: 0, lostDrones: 0, reclaimed: 0, errors: 0 };
  let server;
  let listening;
  let paused = !auto;
  let sequence = 0;
  let pendingOrders = 0;
  let watchSequence = 0;
  let fleet = 0;
  const flying = new Set();
  let crash;
  let origin;
  let closing;
  let nodeView;
  const sleep = (ms) => delay(ms, undefined, { signal: shutdown.signal });
  const client = () => {
    const url = cluster.url;
    if (!clients.has(url)) clients.set(url, new FlowerClient(url));
    return clients.get(url);
  };
  async function mutate(name, args) {
    const requestId = randomUUID();
    const deadline = Date.now() + 12_000;
    let last;
    while (!shutdown.signal.aborted && Date.now() < deadline) {
      const started = performance.now();
      try {
        const result = await client().mutate(name, args, {
          requestId, signal: AbortSignal.any([shutdown.signal, AbortSignal.timeout(2_000)]),
        });
        record(started);
        return result;
      } catch (error) {
        last = error;
        if (error instanceof FlowerError && error.status < 500) throw error;
        await cluster.discoverLeader({ timeoutMs: 2_000 }).catch(() => {});
        await sleep(50);
      }
    }
    throw last ?? shutdown.signal.reason ?? new Error("Mutation retry deadline exceeded");
  }
  // Keep the last few seconds of acknowledged call latencies.
  function record(started) {
    const now = performance.now();
    latencies.push([now, now - started]);
    while (latencies[oldestLatency][0] < now - latencyWindowMs) oldestLatency++;
    if (oldestLatency > 4_096) { latencies.splice(0, oldestLatency); oldestLatency = 0; }
  }
  async function order(shop, quantity) {
    if (sequence >= maxOrders) throw new Error(`This demo stops at ${maxOrders.toLocaleString("en")} orders. Restart it for a fresh kitchen.`);
    const index = sequence++;
    const result = await mutate("pizza.order", { id: `pizza-${String(index + 1).padStart(6, "0")}`, shop, quantity });
    counters.placed++;
    later(() => ring(shop[0], 0), bakeMs + 50);
    return result;
  }
  const later = (callback, ms) => setTimeout(() => { if (!shutdown.signal.aborted) callback(); }, ms).unref();

  // Every claim keeps a retry receipt, so idle drones wait at their tenant's
  // roost instead of polling. Each ready pizza rings once: the bell sends the
  // longest-resting drone, or waits for the next one back from a delivery.
  function ring(tenant, attempt) {
    const { resting, bells } = roosts.get(tenant);
    const [drone] = resting;
    if (drone) drone(attempt);
    else bells.push(attempt);
  }
  // Resolves with the bell's attempt number, or null when the drone gives up
  // waiting and looks for itself, e.g. for a missed bell after a failover.
  // Together a tenant's resting drones look about once a second.
  const patience = () => fleetSize(rate) * (500 + Math.random() * 1_000);
  function roost(tenant, ms = patience()) {
    const { resting, bells } = roosts.get(tenant);
    if (bells.length) return Promise.resolve(bells.shift());
    return new Promise((resolve) => {
      const done = (attempt) => { clearTimeout(timer); resting.delete(done); resolve(attempt); };
      const timer = setTimeout(() => done(null), ms);
      resting.add(done);
    });
  }

  // Each kitchen's popularity drifts on its own slow cycle, so the leaderboard
  // and every tenant's load keep changing.
  const drift = shops.map((_, index) => ({ periodMs: 35_000 + 11_000 * index, phase: index * 2.1 }));
  function randomShop() {
    const now = Date.now();
    const weights = drift.map(({ periodMs, phase }) => 1 + 0.8 * Math.sin(2 * Math.PI * now / periodMs + phase));
    let pick = Math.random() * weights.reduce((sum, weight) => sum + weight, 0);
    for (let index = 0; index < shops.length; index++) if ((pick -= weights[index]) <= 0) return shops[index];
    return shops.at(-1);
  }
  function arrive() {
    if (pendingOrders >= maxPendingOrders) { counters.walkedAway++; return; }
    pendingOrders++;
    const quantity = [1, 1, 1, 2, 2, 3, 4][Math.floor(Math.random() * 7)];
    void order(randomShop(), quantity).catch((error) => {
      if (shutdown.signal.aborted) return;
      if (failure(error) === "OUT_OF_STOCK") counters.walkedAway++;
      else { counters.errors++; console.error("Arrival:", error.message); }
    }).finally(() => { pendingOrders--; });
  }

  async function drone(slot) {
    const tenant = tenants[slot % tenants.length];
    const name = `drone-${slot + 1}`;
    let bell = null;
    while (!shutdown.signal.aborted) {
      if (slot >= fleet) {
        // A landing drone passes its bell on.
        if (bell !== null) ring(tenant, bell);
        return;
      }
      try {
        const { value: claim } = await mutate("pizza.claim", { tenant, owner: name });
        counters.claims++;
        if (!claim) {
          counters.emptyClaims++;
          // The bell can beat the oven timer's commit; ring again shortly.
          if (bell !== null && bell < 3) {
            const next = bell + 1;
            later(() => ring(tenant, next), 100 << bell);
          }
          bell = await roost(tenant);
          continue;
        }
        if (claim.attempt > 1) counters.reclaimed++;
        if (Math.random() < lostDroneRate) {
          // This drone vanishes with the pizza. Its lease expires, then another
          // drone reclaims the delivery with a newer fencing token.
          counters.lostDrones++;
          later(() => ring(tenant, 0), leaseMs + 100);
          await sleep(leaseMs + 500);
        } else {
          await sleep(flightMs[0] + Math.random() * (flightMs[1] - flightMs[0]));
          const { value: delivered } = await mutate("pizza.deliver", { tenant, id: claim.id, owner: claim.owner, token: claim.token });
          counters.delivered++;
          counters.pizzas += delivered.quantity;
        }
      } catch (error) {
        if (shutdown.signal.aborted) return;
        if (failure(error) !== "LEASE_LOST") { counters.errors++; console.error("Drone:", error.message); }
      }
      bell = await roost(tenant);
    }
  }
  // Resize the fleet in place: surplus drones finish their delivery and land.
  function staff() {
    fleet = fleetSize(rate) * tenants.length;
    for (let slot = 0; slot < fleet; slot++) {
      if (flying.has(slot)) continue;
      flying.add(slot);
      tasks.push(drone(slot).catch(() => {}).finally(() => flying.delete(slot)));
    }
  }

  // Every open dashboard shares one recent sample of the replicas' metrics.
  async function inspectNodes() {
    if (!nodeView || performance.now() - nodeView.at > 500) {
      nodeView = { at: performance.now(), members: Promise.all(cluster.members.map(async (node) => {
        const running = Boolean(node.process && !node.process.ended);
        const view = { id: node.id, running, state: running ? "Starting" : "Stopped", applied: null, term: null };
        if (!running) return view;
        try {
          const metrics = await cluster.metrics(node);
          return { ...view, state: metrics.state, applied: metrics.last_applied?.index ?? null, term: metrics.current_term ?? null };
        } catch { return view; }
      })) };
    }
    return (await nodeView.members).map((node) => ({ ...node, streams: streams.get(node.id) ?? 0 }));
  }
  function percentile(sorted, fraction) {
    return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(fraction * sorted.length))] : null;
  }
  async function stats() {
    const since = performance.now() - latencyWindowMs;
    const sorted = latencies.slice(oldestLatency).filter(([at]) => at >= since).map(([, ms]) => ms).sort((a, b) => a - b);
    return {
      rate, maxRate: MAX_RATE, paused, maxOrders, sequence, pendingOrders, drones: fleet, bakeMs, leaseMs,
      counters: { ...counters },
      latency: { windowMs: latencyWindowMs, calls: sorted.length, p50: percentile(sorted, 0.5), p99: percentile(sorted, 0.99) },
      nodes: await inspectNodes(),
      recovering: Boolean(crash),
    };
  }

  async function json(request) {
    if (request.headers["content-type"]?.split(";", 1)[0] !== "application/json") throw new Error("Use application/json");
    let size = 0;
    const chunks = [];
    for await (const chunk of request) {
      size += chunk.length;
      if (size > 4_096) throw new Error("Demo request exceeds 4 KiB");
      chunks.push(chunk);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  }
  const reply = (response, status, value) => {
    if (!response.headersSent) response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify(value));
  };
  async function close() {
    if (closing) return closing;
    closing = (async () => {
      shutdown.abort(new Error("Pizza dashboard closed"));
      signal?.removeEventListener("abort", interrupted);
      for (const { resting } of roosts.values()) for (const done of resting) done(null);
      // A listener can still be opening when shutdown arrives. Settle that
      // acquisition before collecting sockets and closing the owned server.
      await listening?.catch(() => {});
      for (const socket of connections) socket.destroy();
      if (server?.listening) await new Promise((done) => server.close(done));
      await cluster.close();
      await Promise.allSettled(tasks);
    })();
    return closing;
  }
  const interrupted = () => { void close(); };
  signal?.addEventListener("abort", interrupted, { once: true });
  try {
    if (signal?.aborted) { await close(); throw signal.reason; }
    const output = resolve(root, ".flower/pizza-dashboard");
    await mkdir(output, { recursive: true });
    shutdown.signal.throwIfAborted();
    await build({ entryPoints: [resolve(here, "app.ts")], bundle: true, format: "esm",
      platform: "browser", target: "es2022", outfile: resolve(output, "app.js"), sourcemap: true });
    shutdown.signal.throwIfAborted();
    const assets = new Map(await Promise.all([
      ["/", resolve(here, "index.html"), "text/html; charset=utf-8"],
      ["/style.css", resolve(here, "style.css"), "text/css; charset=utf-8"],
      ["/app.js", resolve(output, "app.js"), "text/javascript; charset=utf-8"],
    ].map(async ([path, file, type]) => [path, { type, body: await readFile(file) }])));
    shutdown.signal.throwIfAborted();
    await cluster.start();
    shutdown.signal.throwIfAborted();
    const bundle = await buildBundle(resolve(root, "examples/goblin-pizza-ts/goblin-pizza.ts"), { initialization: "static" });
    shutdown.signal.throwIfAborted();
    await new FlowerAdmin(cluster.url, { adminToken: cluster.adminToken }).deploy(
      bundle, { signal: shutdown.signal });
    shutdown.signal.throwIfAborted();
    await mutate("pizza.setup", { tenants, storesPerTenant, stockPerShop: 1_000_000, bakeMs, leaseMs });
    shutdown.signal.throwIfAborted();

    server = createServer((request, response) => {
      void (async () => {
        if (request.method === "GET" && assets.has(request.url)) {
          const asset = assets.get(request.url);
          response.writeHead(200, { "content-type": asset.type, "cache-control": "no-store",
            "content-security-policy": "default-src 'self'; connect-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'" });
          response.end(request.url === "/" ? asset.body.toString("utf8").replace("</head>", `<meta name="pizza-demo-paused" content="${paused}"></head>`) : asset.body);
          return;
        }
        const route = `${request.method} ${request.url}`;
        if (!["POST /v1/watch", "POST /demo/action", "GET /demo/stats"].includes(route)) {
          reply(response, 404, { error: "Unknown demo route" }); return;
        }
        if (request.headers.host !== new URL(origin).host || (request.headers.origin && request.headers.origin !== origin)) {
          reply(response, 403, { error: "Use the dashboard's local origin" }); return;
        }
        if (route === "GET /demo/stats") { reply(response, 200, await stats()); return; }
        const input = await json(request);
        if (request.url === "/v1/watch") {
          if (input?.name !== "pizza.dashboard" || !input.args || Array.isArray(input.args) ||
              Object.keys(input.args).length !== 1 || !tenants.includes(input.args.tenant) ||
              Object.keys(input).some((key) => !["name", "args"].includes(key))) {
            reply(response, 400, { error: "This demo watches only pizza.dashboard({tenant}) for its configured tenants" }); return;
          }
          const controller = new AbortController();
          const disconnect = () => controller.abort(new Error("Dashboard disconnected"));
          response.once("close", disconnect);
          let replica;
          try {
            // Spread independent streams across replicas. A stream stays on its
            // chosen node; reconnecting selects another local applied snapshot.
            const replicas = cluster.members.filter((node) => node.process && !node.process.ended && !node.process.intentional);
            if (!replicas.length) throw new Error("No running pizza replicas");
            replica = replicas[watchSequence++ % replicas.length];
            streams.set(replica.id, (streams.get(replica.id) ?? 0) + 1);
            const upstream = await fetch(replica.url + "/v1/watch", {
              method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream" },
              body: JSON.stringify(input), signal: AbortSignal.any([shutdown.signal, controller.signal]),
            });
            response.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json",
              "cache-control": "no-cache", "x-accel-buffering": "no" });
            if (upstream.body) await pipeline(Readable.fromWeb(upstream.body), response);
            else response.end();
          } finally {
            response.removeListener("close", disconnect);
            controller.abort();
            if (replica) streams.set(replica.id, streams.get(replica.id) - 1);
          }
          return;
        }
        if (!input || typeof input !== "object" || Array.isArray(input) || Object.keys(input).some((key) => !["action", "shop", "rate"].includes(key))) throw new Error("Invalid demo action");
        if (input.shop !== undefined && (!Array.isArray(input.shop) || input.shop.length !== 2 ||
            !tenants.includes(input.shop[0]) || !["store-0", "store-1"].includes(input.shop[1]))) throw new Error("Unknown kitchen");
        switch (input.action) {
          case "order": await order(input.shop ?? shops[0], 1 + sequence % 3); break;
          case "tip": await mutate("pizza.tip", { shop: input.shop ?? shops[0], amount: 3 }); counters.tips += 3; break;
          case "pause": paused = !paused; break;
          case "rate": rate = checkRate(input.rate); staff(); break;
          case "crash":
            if (crash) throw new Error("A leader recovery is already in progress");
            crash = cluster.crashLeaderAndRecover();
            try { await crash; } finally { crash = undefined; }
            break;
          default: throw new Error("Unknown demo action");
        }
        reply(response, 200, { ok: true, paused, rate });
      })().catch((error) => {
        if (response.destroyed) return;
        if (response.headersSent) response.destroy();
        else reply(response, 400, { error: error.message });
      });
    });
    server.on("connection", (socket) => { connections.add(socket); socket.on("close", () => connections.delete(socket)); });
    let ready, failed;
    listening = new Promise((resolve, reject) => { ready = resolve; failed = reject; });
    server.once("error", failed);
    try { server.listen(port, "127.0.0.1", ready); } catch (error) { failed(error); }
    await listening;
    shutdown.signal.throwIfAborted();
    origin = `http://127.0.0.1:${server.address().port}`;
    // Every write goes through an exposed method, including customers and drones.
    tasks.push((async () => {
      let next = performance.now();
      while (!shutdown.signal.aborted) {
        await sleep(20);
        const now = performance.now();
        // A stalled loop, e.g. after the laptop slept, starts afresh.
        if (paused || sequence >= maxOrders || now - next > 1_000) { next = now; continue; }
        // Exponential gaps make a Poisson process; catch up on the ones due.
        while (next <= now) {
          arrive();
          next += -Math.log(1 - Math.random()) * 1_000 / rate;
        }
      }
    })().catch(() => {}));
    // Grateful customers tip now and then, in proportion to the rush.
    tasks.push((async () => {
      while (!shutdown.signal.aborted) {
        await sleep(-Math.log(1 - Math.random()) * 20_000 / rate);
        if (paused) continue;
        const amount = 1 + Math.floor(Math.random() * 5);
        await mutate("pizza.tip", { shop: randomShop(), amount }).then(() => { counters.tips += amount; }, (error) => {
          if (!shutdown.signal.aborted) { counters.errors++; console.error("Tip:", error.message); }
        });
      }
    })().catch(() => {}));
    // The goblins sweep delivered orders off each tenant's board into tallies.
    tasks.push((async () => {
      while (!shutdown.signal.aborted) {
        await sleep(2_000);
        for (const tenant of tenants) {
          await mutate("pizza.archive", { tenant, olderThanMs: archiveAfterMs, limit: 1_000 })
            .then(({ value }) => { counters.archived += value?.archived ?? 0; }, (error) => {
              if (!shutdown.signal.aborted) { counters.errors++; console.error("Archive:", error.message); }
            });
        }
      }
    })().catch(() => {}));
    staff();
    return { url: origin, close, pids: () => cluster.pids, maxOrders, rate: () => rate, stats };
  } catch (error) { await close(); throw error; }
}

function openBrowser(url) {
  const command = process.platform === "darwin" ? "open" : process.platform === "win32" ? "explorer" : "xdg-open";
  const child = spawn(command, [url], { stdio: "ignore", detached: true });
  child.on("error", () => console.log(`Could not open a browser; visit ${url}`));
  child.unref();
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const { values } = parseArgs({ args: process.argv.slice(2), options: { port: { type: "string", default: "0" }, binary: { type: "string" },
    duration: { type: "string" }, paused: { type: "boolean", default: false }, rate: { type: "string", default: String(DEFAULT_RATE) },
    "max-orders": { type: "string", default: String(DEFAULT_MAX_ORDERS) }, open: { type: "boolean", default: false } } });
  const duration = values.duration === undefined ? undefined : Number(values.duration);
  if (duration !== undefined && (!Number.isFinite(duration) || duration < 1 || duration > 86_400)) throw new Error("duration must be 1..86400 seconds");
  const controller = new AbortController();
  let timer;
  const interrupt = () => { clearTimeout(timer); controller.abort(new Error("Demo interrupted")); };
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);
  try {
    const demo = await startPizzaDemo({ port: Number(values.port), binary: values.binary, auto: !values.paused, signal: controller.signal,
      rate: Number(values.rate), maxOrders: Number(values["max-orders"]) });
    console.log(`Goblin Pizza live: ${demo.url}\nOne watched value: pizza.dashboard({tenant}). Three tenants, two stores each; three Rust/QuickJS replicas.\n${demo.rate()} orders/s arrive until ${demo.maxOrders.toLocaleString("en")} orders; change the pace from the dashboard. Ctrl+C removes this temporary cluster.`);
    if (values.open) openBrowser(demo.url);
    if (duration !== undefined) {
      timer = setTimeout(() => { void demo.close().catch((error) => { console.error(error); process.exitCode = 1; }); }, duration * 1_000);
      timer.unref();
    }
  } catch (error) { if (!controller.signal.aborted) throw error; }
}
