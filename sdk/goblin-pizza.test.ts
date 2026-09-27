import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import app from "../examples/goblin-pizza-ts/goblin-pizza.ts";
import { FlowerError, type Update } from "./client.ts";
import { canonicalJson, type Json } from "./json.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";

// The bundled example runs in an isolated context on Flower's reference engine,
// as the server runs it; the imported module is only read for its manifest.
const entry = fileURLToPath(new URL("../examples/goblin-pizza-ts/goblin-pizza.ts", import.meta.url));
const T0 = 1_000_000;
const tenant = "tenant-0";
const shop = (index: number, owner = tenant): [string, string] => [owner, `store-${index}`];
const key = (id: string, index = 0, owner = tenant) => canonicalJson([owner, `store-${index}`, id]);
const cell = (name: string, args: Json) => `cell:${canonicalJson([name, args])}`;
const root = (name: string, args: Json) => `root:${canonicalJson([name, args])}`;
const row = (collection: string, raw: string) => `source:${canonicalJson([collection, raw])}`;
type Kitchen = TestDatabase<typeof app>;

async function kitchen(options: { tenants?: string[]; shops?: number; stockPerShop?: number; bakeMs?: number; leaseMs?: number } = {}): Promise<Kitchen> {
  const db = await testDatabase<typeof app>(entry, { now: T0 });
  db.mutate("pizza.setup", {
    tenants: options.tenants ?? [tenant], storesPerTenant: options.shops ?? 2, stockPerShop: options.stockPerShop ?? 100,
    bakeMs: options.bakeMs ?? 50, leaseMs: options.leaseMs ?? 100,
  });
  return db;
}

function fails(action: () => unknown, code: string): FlowerError {
  let caught: unknown;
  assert.throws(action, (error) => { caught = error; return true; });
  assert.ok(caught instanceof FlowerError, String(caught));
  assert.equal(caught.failure?.code, code, caught.message);
  return caught;
}

function claim(db: Kitchen, owner: string, forTenant = tenant) {
  const job = db.mutate("pizza.claim", { tenant: forTenant, owner });
  assert.ok(job, `${owner} found no delivery`);
  return job;
}

const deliver = (db: Kitchen, job: { id: string; owner: string; token: number }, forTenant = tenant) =>
  db.mutate("pizza.deliver", { tenant: forTenant, id: job.id, owner: job.owner, token: job.token });

const pick = <T extends object, K extends keyof T>(value: T, ...keys: K[]) => Object.fromEntries(keys.map((name) => [name, value[name]]));
const outcome = (db: Kitchen, id: string) => (db.data[id] as { outcome: { value: Json } }).outcome.value;
const changed = (before: Record<string, Json>, after: Record<string, Json>) =>
  [...new Set([...Object.keys(before), ...Object.keys(after)])].filter((id) => canonicalJson(before[id] ?? null) !== canonicalJson(after[id] ?? null)).sort();

async function next<T>(updates: AsyncGenerator<Update<T>>): Promise<T> {
  const result = await updates.next();
  assert.equal(result.done, false);
  return (result.value as Update<T>).value;
}

test("ovens fire at their deadline and a delivery updates stock, revenue and summaries atomically", async () => {
  const db = await kitchen();
  const order = db.mutate("pizza.order", { id: "pepperoni", shop: shop(0), quantity: 3 });
  assert.deepEqual([order.status, order.createdAt, order.dueAt], ["baking", T0, T0 + 50]);
  let world = db.query("pizza.world");
  assert.deepEqual(world.timers.map((timer) => [timer.id, timer.dueAt]), [[`bake:${key("pepperoni")}`, T0 + 50]]);
  assert.equal(world.jobs.length, 0);
  assert.deepEqual(pick(world.summaries[0], "stock", "baking", "orderedQuantity"), { stock: 97, baking: 1, orderedQuantity: 3 });

  db.advance(49);
  assert.equal(db.query("pizza.world").timers.length, 1);
  assert.equal(db.mutate("pizza.claim", { tenant, owner: "drone-1" }), null);
  assert.equal(db.advance(1), 1, "exactly the oven bell rings");
  const job = claim(db, "drone-1");
  assert.deepEqual(job.payload, { orderId: "pepperoni", shop: shop(0), quantity: 3 });
  const delivered = deliver(db, job);
  assert.deepEqual([delivered.status, delivered.readyAt, delivered.deliveredAt], ["delivered", T0 + 50, T0 + 50]);

  world = db.query("pizza.world");
  assert.equal(world.timers.length, 0);
  assert.deepEqual([world.jobs[0].state, world.jobs[0].result], ["completed", { deliveredAt: T0 + 50 }]);
  assert.deepEqual(pick(world.summaries[0], "stock", "revenue", "orders", "baking", "ready", "delivered", "deliveredQuantity"),
    { stock: 97, revenue: 21, orders: 1, baking: 0, ready: 0, delivered: 1, deliveredQuantity: 3 });
  assert.deepEqual(world.leaderboards[tenant][0].id, shop(0));
  assert.deepEqual(outcome(db, cell("pizza.shopSummary", shop(0))), world.summaries[0], "the materialized summary tracks the delivery");
});

test("shop summaries are materialized per shop row without ctx.materialize and stay current on writes", async () => {
  const db = await kitchen({ shops: 3 });
  const roots = [0, 1, 2].map((index) => root("pizza.shopSummary", shop(index))).sort();
  assert.deepEqual(Object.keys(db.data).filter((id) => id.startsWith("root:")).sort(), roots, "setup's shop rows created their summaries");
  db.mutate("pizza.tip", { shop: shop(1), amount: 5 });
  const stored = outcome(db, cell("pizza.shopSummary", shop(1))) as { tips: number };
  assert.equal(stored.tips, 5, "a write refreshes the durable summary without anyone reading it");
  assert.deepEqual(stored, db.query("pizza.shop", shop(1)));
  db.maintain();
  assert.deepEqual(Object.keys(db.data).filter((id) => id.startsWith("root:")).sort(), roots, "the backfill adds nothing more");
});

test("invalid orders, duplicate IDs and exhausted dough leave no stock, order or timer behind", async () => {
  const db = await kitchen({ stockPerShop: 3 });
  db.mutate("pizza.order", { id: "last-pizza", shop: shop(0), quantity: 3 });
  for (const [args, code] of [
    [{ id: "last-pizza", shop: shop(0), quantity: 1 }, "ORDER_EXISTS"],
    [{ id: "too-many", shop: shop(0), quantity: 1 }, "OUT_OF_STOCK"],
    [{ id: "bad-count", shop: shop(1), quantity: 0 }, "INVALID_ARGUMENT"],
    [{ id: "bad-count", shop: shop(1), quantity: 1.5 }, "INVALID_ARGUMENT"],
    [{ id: "bad-count", shop: shop(1), quantity: 5 }, "INVALID_ARGUMENT"],
    [{ id: "bad-shop", shop: shop(9), quantity: 1 }, "SHOP_NOT_FOUND"],
    [{ id: "has spaces", shop: shop(1), quantity: 1 }, "INVALID_ARGUMENT"],
    [{ id: "extra", shop: shop(1), quantity: 1, discount: 100 }, "INVALID_ARGUMENT"],
    [null, "INVALID_ARGUMENT"],
  ] as const) {
    const before = db.data;
    fails(() => db.mutate("pizza.order", args as never), code);
    assert.deepEqual(db.data, before);
  }
  const error = fails(() => db.mutate("pizza.order", { id: "bad-count", shop: shop(1), quantity: 5 }), "INVALID_ARGUMENT");
  assert.deepEqual([error.status, error.code, error.failure?.message, error.failure?.details], [422, "EVALUATION_FAILED", "quantity: must be at most 4", { path: ["quantity"] }]);
  const world = db.query("pizza.world");
  assert.equal(world.orders.length, 1);
  assert.equal(world.timers.length, 1);
  assert.deepEqual(world.shops.map((each) => each.stock), [0, 3]);
});

test("an abandoned drone's lease expires, and fencing rejects stale and repeated deliveries with LEASE_LOST", async () => {
  const db = await kitchen({ bakeMs: 0, leaseMs: 20 });
  db.mutate("pizza.order", { id: "mushroom", shop: shop(0), quantity: 2 });
  db.maintain();
  const first = claim(db, "sleepy-drone");
  assert.equal(first.expiresAt, T0 + 20);
  db.now = T0 + 19;
  assert.equal(db.mutate("pizza.claim", { tenant, owner: "rescue-drone" }), null);
  db.now = T0 + 20;
  const second = claim(db, "rescue-drone");
  assert.deepEqual([second.id, second.attempt], [first.id, 2]);
  assert.ok(second.token > first.token);

  let before = db.data;
  fails(() => deliver(db, first), "LEASE_LOST");
  assert.deepEqual(db.data, before);
  deliver(db, second);
  before = db.data;
  fails(() => deliver(db, second), "LEASE_LOST");
  assert.deepEqual(db.data, before);
  const world = db.query("pizza.world");
  assert.equal(world.shops[0].revenue, 14);
  assert.equal(world.summaries[0].delivered, 1);
  assert.equal(world.jobs[0].attempts, 2);
});

test("retries that reuse a request ID get the original receipt instead of applying twice", async () => {
  const db = await kitchen({ bakeMs: 0 });
  const args = { id: "retry", shop: shop(0), quantity: 2 };
  const placed = await db.client.mutate("pizza.order", args, { requestId: "order-1" });
  assert.equal(placed.duplicate, false);
  assert.deepEqual(await db.client.mutate("pizza.order", args, { requestId: "order-1" }), { ...placed, duplicate: true });
  assert.equal(db.query("pizza.shop", shop(0)).stock, 98);
  await assert.rejects(db.client.mutate("pizza.order", { ...args, quantity: 1 }, { requestId: "order-1" }), { status: 409, code: "REQUEST_ID_REUSED" });
  fails(() => db.mutate("pizza.order", args), "ORDER_EXISTS");

  db.maintain();
  const job = db.mutate("pizza.claim", { tenant, owner: "drone" }, { requestId: "claim-1" });
  assert.ok(job);
  assert.deepEqual(db.mutate("pizza.claim", { tenant, owner: "drone" }, { requestId: "claim-1" }), job, "a retried claim returns the same lease");
  assert.equal(db.mutate("pizza.claim", { tenant, owner: "drone" }), null);
  const identity = { tenant, id: job.id, owner: job.owner, token: job.token };
  const delivered = db.mutate("pizza.deliver", identity, { requestId: "deliver-1" });
  const revision = db.revision;
  assert.deepEqual(db.mutate("pizza.deliver", identity, { requestId: "deliver-1" }), delivered);
  assert.equal(db.revision, revision);
  fails(() => db.mutate("pizza.deliver", identity), "LEASE_LOST");
  assert.equal(db.query("pizza.shop", shop(0)).revenue, 14);
});

test("tips and deliveries propagate through per-shop summaries into the leaderboard", async () => {
  const db = await kitchen({ shops: 3, bakeMs: 0 });
  for (let index = 0; index < 9; index++) {
    db.mutate("pizza.order", { id: `pizza-${index}`, shop: shop(index % 3), quantity: index % 4 + 1 });
    db.maintain();
    deliver(db, claim(db, `drone-${index}`));
  }
  db.mutate("pizza.tip", { shop: shop(1), amount: 100 });
  db.mutate("pizza.tip", { shop: shop(1), amount: 13 });
  const world = db.query("pizza.world");
  const expected = world.shops.map((each) => {
    const orders = world.orders.filter((order) => canonicalJson(order.shop) === canonicalJson(each.id));
    const quantity = orders.reduce((sum, order) => sum + order.quantity, 0);
    assert.equal(each.initialStock - each.stock, quantity);
    assert.equal(each.revenue, quantity * world.config.unitPrice);
    return { ...each, orders: orders.length, baking: 0, ready: 0, delivered: orders.length, orderedQuantity: quantity, deliveredQuantity: quantity };
  });
  assert.deepEqual(world.summaries, expected);
  expected.sort((a, b) => (b.revenue + b.tips) - (a.revenue + a.tips) || (a.key < b.key ? -1 : 1));
  assert.deepEqual(world.leaderboards[tenant], expected);
  assert.deepEqual([world.leaderboards[tenant][0].id, world.leaderboards[tenant][0].tips], [shop(1), 113]);
  assert.deepEqual(db.query("pizza.shop", shop(2)), world.summaries[2]);
});

test("a tip recomputes only its shop summary while order transitions refresh order statistics", async () => {
  const db = await kitchen();
  const stats = cell("pizza.orderStats", shop(0));
  db.mutate("pizza.order", { id: "cached-mushroom", shop: shop(0), quantity: 2 });
  assert.deepEqual(outcome(db, stats), { orders: 1, baking: 1, ready: 0, delivered: 0, orderedQuantity: 2, deliveredQuantity: 0 });
  let before = db.data;
  db.mutate("pizza.tip", { shop: shop(0), amount: 11 });
  assert.deepEqual(changed(before, db.data), [cell("pizza.shopSummary", shop(0)), row("pizza.shops", canonicalJson(shop(0)))],
    "no order statistics, rankings or other shops are recomputed");
  assert.equal(db.query("pizza.shop", shop(0)).tips, 11);

  db.advance(50);
  assert.deepEqual(outcome(db, stats), { orders: 1, baking: 0, ready: 1, delivered: 0, orderedQuantity: 2, deliveredQuantity: 0 });
  before = db.data;
  const job = claim(db, "stats-drone");
  assert.deepEqual(db.data[stats], before[stats], "leasing work leaves order statistics alone");
  deliver(db, job);
  assert.deepEqual(outcome(db, stats), { orders: 1, baking: 0, ready: 0, delivered: 1, orderedQuantity: 2, deliveredQuantity: 2 });
  assert.deepEqual(pick(db.query("pizza.shop", shop(0)), "revenue", "tips"), { revenue: 14, tips: 11 });
});

test("rankings are computed on read and never enter durable state", async () => {
  const db = await kitchen({ shops: 3 });
  assert.deepEqual(db.query("pizza.world").leaderboards[tenant].map((each) => each.id), [shop(0), shop(1), shop(2)]);
  for (const [index, amount] of [[2, 9], [1, 15], [0, 21]] as const) {
    db.mutate("pizza.tip", { shop: shop(index), amount });
    const before = db.data;
    assert.deepEqual(db.query("pizza.world").leaderboards[tenant][0].id, shop(index));
    const board = db.query("pizza.dashboard", { tenant });
    assert.equal(board.leaderboard[0], canonicalJson(shop(index)));
    assert.equal(board.summaries[board.leaderboard[0]].tips, amount);
    assert.deepEqual(db.data, before);
    assert.ok(!Object.keys(db.data).some((id) => id.includes('"pizza.leaderboard"')));
  }
});

test("setup and lease policy are enforced, and only the business aliases are public", async () => {
  const db = await kitchen({ leaseMs: 10 });
  const before = db.data;
  fails(() => db.mutate("pizza.setup", { tenants: [tenant], storesPerTenant: 1, stockPerShop: 1, bakeMs: 0, leaseMs: 10 }), "ALREADY_INITIALIZED");
  assert.match(fails(() => db.mutate("pizza.claim", { tenant, owner: "drone", leaseMs: 11 }), "INVALID_ARGUMENT").message, /at most 10 ms/);
  assert.deepEqual(fails(() => db.mutate("pizza.claim", { tenant, owner: "drone", leaseMs: 0 }), "INVALID_ARGUMENT").failure?.details, { path: ["leaseMs"] });
  assert.deepEqual(fails(() => db.mutate("pizza.tip", { shop: shop(0), amount: -1 }), "INVALID_ARGUMENT").failure?.details, { path: ["amount"] });
  fails(() => db.query("pizza.world", {} as never), "INVALID_ARGUMENT");
  assert.deepEqual(db.data, before);

  assert.deepEqual(Object.keys(app.http).sort(), ["pizza.archive", "pizza.claim", "pizza.dashboard", "pizza.deliver", "pizza.order", "pizza.setup", "pizza.shop", "pizza.shop.local", "pizza.tip", "pizza.world"]);
  assert.equal(app.maintenance?.name, "$flower.maintenance");
  for (const hidden of ["internal.pizza.finishBaking", "internal.pizza.order", "pizza.shopSummary", "pizza.orderStats", "$flower.maintenance"]) {
    assert.throws(() => (db as unknown as TestDatabase).call(hidden), { status: 404, code: "METHOD_NOT_FOUND" });
  }
  for (const options of [{ shops: 0 }, { tenants: [] }, { tenants: [tenant, tenant] }, { stockPerShop: 0 }, { bakeMs: -1 }, { leaseMs: 60_001 }]) {
    await assert.rejects(kitchen(options), (error: FlowerError) => error.failure?.code === "INVALID_ARGUMENT");
  }
});

test("only observational methods opt into replica-local reads", async () => {
  const db = await kitchen();
  for (const alias of ["pizza.shop.local", "pizza.dashboard"] as const) {
    assert.equal(app.http[alias].consistency, "replica-local");
    assert.equal((app.definitions[app.http[alias].name] as { consistency?: string }).consistency, "replica-local");
  }
  for (const alias of ["pizza.shop", "pizza.world", "pizza.setup", "pizza.order", "pizza.claim", "pizza.deliver", "pizza.tip"] as const) {
    assert.equal(Object.hasOwn(app.http[alias], "consistency"), false);
  }
  db.mutate("pizza.tip", { shop: shop(0), amount: 17 });
  assert.deepEqual(db.query("pizza.shop.local", shop(0)), db.query("pizza.shop", shop(0)), "both compute the same value from the same snapshot");
  fails(() => db.query("pizza.shop.local", "bad shop" as never), "INVALID_ARGUMENT");
  fails(() => db.query("pizza.shop.local", shop(9)), "SHOP_NOT_FOUND");
});

test("the dashboard joins lifecycle records under stable keys with tenant totals, live over SSE", async () => {
  const db = await kitchen({ bakeMs: 0 });
  db.mutate("pizza.order", { id: "dashboard-pizza", shop: shop(0), quantity: 2 });
  let board = db.query("pizza.dashboard", { tenant });
  assert.equal(board.orders[key("dashboard-pizza")].status, "baking");
  assert.equal(board.timers[`bake:${key("dashboard-pizza")}`].handler, "bake");
  assert.deepEqual(board.totals, { orders: 1, baking: 1, ready: 0, delivered: 0, pizzas: 0, revenue: 0, tips: 0 });
  fails(() => db.query("pizza.dashboard", { tenant, raw: true } as never), "INVALID_ARGUMENT");

  db.maintain();
  const job = claim(db, "dashboard-drone");
  board = db.query("pizza.dashboard", { tenant });
  assert.deepEqual([board.jobs[job.id].lease?.owner, board.jobs[job.id].lease?.token], ["dashboard-drone", job.token]);
  assert.deepEqual(board.timers, {});

  const live = db.client.subscribe("pizza.dashboard", { tenant });
  try {
    assert.deepEqual(await next(live), board);
    deliver(db, job);
    board = await next(live);
    // The board keeps the delivered order but only deliveries still in flight.
    assert.deepEqual([board.orders[job.id].status, Object.hasOwn(board.jobs, job.id)], ["delivered", false]);
    db.mutate("pizza.tip", { shop: shop(0), amount: 3 });
    board = await next(live);
    assert.deepEqual(board.totals, { orders: 1, baking: 0, ready: 0, delivered: 1, pizzas: 2, revenue: 14, tips: 3 });
  } finally { await live.return(undefined); }
  assert.deepEqual(board.summaries[canonicalJson(shop(0))], db.query("pizza.shop", shop(0)));
  db.now += 1;
  assert.deepEqual(db.query("pizza.dashboard", { tenant }), board, "local countdowns do not force clock-only snapshots");
});

test("the dashboard keeps the latest 120 orders, every oven timer, and totals covering all of them", async () => {
  const db = await kitchen({ stockPerShop: 1000, bakeMs: 60_000 });
  for (let index = 0; index < 121; index++) {
    db.now++;
    db.mutate("pizza.order", { id: `recent-${index}`, shop: shop(0), quantity: 1 });
  }
  const board = db.query("pizza.dashboard", { tenant });
  assert.equal(Object.keys(board.orders).length, 120);
  assert.equal(Object.keys(board.timers).length, 121);
  assert.equal(Object.hasOwn(board.orders, key("recent-0")), false);
  assert.equal(Object.hasOwn(board.orders, key("recent-120")), true);
  assert.equal(board.totals.orders, 121);
  assert.equal(board.summaries[canonicalJson(shop(0))].orders, 121);
});

test("a delivery returns an old order to the board, and archiving keeps its counts", async () => {
  const db = await kitchen({ stockPerShop: 1000, bakeMs: 0, leaseMs: 60_000 });
  db.mutate("pizza.order", { id: "first", shop: shop(0), quantity: 2 });
  db.maintain();
  const job = claim(db, "drone");
  for (let index = 0; index < 120; index++) {
    db.now++;
    db.mutate("pizza.order", { id: `later-${index}`, shop: shop(1), quantity: 1 });
  }
  let board = db.query("pizza.dashboard", { tenant });
  assert.equal(Object.hasOwn(board.orders, key("first")), false, "the oldest order dropped off the board");
  db.now++;
  deliver(db, job);
  board = db.query("pizza.dashboard", { tenant });
  assert.equal(board.orders[key("first")].status, "delivered", "its delivery is the latest activity");
  assert.equal(Object.keys(board.orders).length, 120);
  const totals = board.totals;

  assert.deepEqual(db.mutate("pizza.archive", { tenant, olderThanMs: 1_000, limit: 10 }), { archived: 0 });
  db.now += 1_000;
  assert.deepEqual(db.mutate("pizza.archive", { tenant, olderThanMs: 1_000, limit: 10 }), { archived: 1 });
  board = db.query("pizza.dashboard", { tenant });
  assert.equal(Object.hasOwn(board.orders, key("first")), false);
  assert.deepEqual([board.totals, board.archived], [totals, 1]);
  assert.deepEqual(pick(board.summaries[canonicalJson(shop(0))], "orders", "delivered", "orderedQuantity", "deliveredQuantity", "revenue"),
    { orders: 1, delivered: 1, orderedQuantity: 2, deliveredQuantity: 2, revenue: 14 });
  const world = db.query("pizza.world");
  assert.equal(world.orders.some((order) => order.id === "first"), false);
  assert.equal(world.jobs.some(({ id }) => id === job.id), false);
  // Live summaries cover the orders still on record; revenue stays on the shop.
  assert.deepEqual(pick(db.query("pizza.shop", shop(0)), "orders", "delivered", "revenue"), { orders: 0, delivered: 0, revenue: 14 });
  fails(() => db.mutate("pizza.archive", { tenant: "nobody", olderThanMs: 0, limit: 1 }), "TENANT_NOT_FOUND");
});

test("tenants reuse store and order IDs while queues, timers, dashboards and accounting stay isolated", async () => {
  const other = "tenant-1";
  const db = await kitchen({ tenants: [tenant, other], shops: 2, bakeMs: 0 });
  const placed = [shop(0), shop(1), shop(0, other)].map((ref) => db.mutate("pizza.order", { id: "same-order", shop: ref, quantity: 2 }));
  assert.equal(new Set(placed.map((order) => order.key)).size, 3);
  let world = db.query("pizza.world");
  assert.equal(world.orders.length, 3);
  assert.equal(new Set(world.timers.map((timer) => timer.id)).size, 3);
  db.maintain();

  const first = claim(db, "same-worker");
  const isolated = claim(db, "same-worker", other);
  assert.deepEqual([first.payload.shop[0], isolated.payload.shop[0]], [tenant, other]);
  assert.notEqual(first.id, isolated.id);
  assert.deepEqual([first.token, isolated.token], [1, 1], "fencing tokens count per tenant scope");
  const beforeWrongTenant = db.data;
  fails(() => deliver(db, isolated, tenant), "LEASE_LOST");
  assert.deepEqual(db.data, beforeWrongTenant);
  deliver(db, isolated, other);
  assert.equal(db.mutate("pizza.claim", { tenant: other, owner: "same-worker" }), null, "a tenant without work cannot claim another tenant's order");
  const another = claim(db, "another-worker");
  assert.equal(another.payload.shop[0], tenant);
  assert.notEqual(another.id, first.id);

  const beforeTip = db.data;
  db.mutate("pizza.tip", { shop: shop(0, other), amount: 19 });
  assert.deepEqual(changed(beforeTip, db.data), [cell("pizza.shopSummary", shop(0, other)), row("pizza.shops", canonicalJson(shop(0, other)))],
    "a tip does not fan out to other tenants");
  const a = db.query("pizza.dashboard", { tenant });
  const b = db.query("pizza.dashboard", { tenant: other });
  assert.deepEqual(pick(a.totals, "orders", "revenue", "tips"), { orders: 2, revenue: 0, tips: 0 });
  assert.deepEqual(pick(b.totals, "orders", "revenue", "tips"), { orders: 1, revenue: 14, tips: 19 });
  for (const [owner, board] of [[tenant, a], [other, b]] as const) {
    assert.ok(Object.values(board.summaries).every((value) => value.id[0] === owner));
    assert.ok(Object.values(board.orders).every((value) => value.shop[0] === owner));
    assert.ok(Object.values(board.jobs).every((value) => value.payload.shop[0] === owner));
    assert.ok(board.config.shopIds.every((ref) => ref[0] === owner));
  }
  assert.deepEqual(db.query("pizza.shop.local", shop(0, other)), b.summaries[canonicalJson(shop(0, other))]);
  fails(() => db.query("pizza.dashboard", { tenant: "missing" }), "TENANT_NOT_FOUND");
  fails(() => db.mutate("pizza.claim", { tenant: "missing", owner: "worker" }), "TENANT_NOT_FOUND");
  world = db.query("pizza.world");
  assert.equal(world.shops.length, 4);
  assert.deepEqual(Object.keys(world.leaderboards), [tenant, other]);
});
