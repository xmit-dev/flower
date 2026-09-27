import assert from "node:assert/strict";
import { test } from "node:test";
import { auditWorld } from "./audit.mjs";

function fixture() {
  const config = { tenantIds: ["tenant-a"], storesPerTenant: 2, shopIds: [["tenant-a", "store-0"], ["tenant-a", "store-1"]], initialStock: 100, bakeMs: 50, leaseMs: 1_000, unitPrice: 7 };
  const orders = [
    { id: "pizza-a", shop: ["tenant-a", "store-0"], quantity: 2, status: "delivered", createdAt: 1_000, dueAt: 1_050, readyAt: 1_053, deliveredAt: 1_100 },
    { id: "pizza-b", shop: ["tenant-a", "store-1"], quantity: 3, status: "delivered", createdAt: 1_003, dueAt: 1_053, readyAt: 1_055, deliveredAt: 1_103 },
    { id: "pizza-c", shop: ["tenant-a", "store-1"], quantity: 1, status: "delivered", createdAt: 1_100, dueAt: 1_150, readyAt: 1_150, deliveredAt: 1_151 },
  ];
  const shops = [
    { id: ["tenant-a", "store-0"], name: "The Crispy Cauldron", initialStock: 100, stock: 98, revenue: 14, tips: 30 },
    { id: ["tenant-a", "store-1"], name: "Mushroom Mayhem", initialStock: 100, stock: 96, revenue: 28, tips: 0 },
  ];
  orders.forEach((order) => { order.key = JSON.stringify([...order.shop, order.id]); });
  shops.forEach((shop) => { shop.key = JSON.stringify(shop.id); });
  const jobs = orders.map((order) => ({
    id: order.key, payload: { orderId: order.id, shop: order.shop, quantity: order.quantity },
    state: "completed", createdAt: order.readyAt, updatedAt: order.deliveredAt, attempts: 1,
    lease: null, result: { deliveredAt: order.deliveredAt }, error: null,
  }));
  const summaries = shops.map((shop, index) => ({ ...shop, orders: index + 1, baking: 0, ready: 0, delivered: index + 1,
    orderedQuantity: index === 0 ? 2 : 4, deliveredQuantity: index === 0 ? 2 : 4 }));
  return {
    world: { config, shops, orders, jobs, timers: [], summaries, leaderboards: { "tenant-a": structuredClone(summaries) } },
    expected: { orders: orders.map(({ id, shop, quantity }) => ({ id, shop, quantity })), tips: { [JSON.stringify(["tenant-a", "store-0"])]: 30 } },
  };
}

test("pizza audit independently accounts for stock, deliveries, tips, and rankings", () => {
  const { world, expected } = fixture();
  assert.deepEqual(auditWorld(world, expected), {
    passed: true, violations: [], orders: 3, delivered: 3, pizzas: 6, revenue: 42, tips: 30,
  });
  // Source row order is immaterial; configured order defines the summaries.
  world.shops.reverse();
  world.orders.reverse();
  world.jobs.reverse();
  assert.equal(auditWorld(world, expected).passed, true);
});

test("pizza audit detects lost, duplicated, unexpected, or altered acknowledged orders", () => {
  const corruptions = [
    (w) => { w.orders.pop(); },
    (w) => { w.orders.push({ ...w.orders[0] }); },
    (w) => { w.orders[0].id = "unacknowledged"; },
    (w) => { w.orders[0].quantity++; },
    (w) => { w.orders[0].shop = ["tenant-a", "store-1"]; },
    (w) => { w.orders[0].status = "ready"; },
  ];
  for (const corrupt of corruptions) {
    const { world, expected } = fixture();
    corrupt(world);
    const report = auditWorld(world, expected);
    assert.equal(report.passed, false);
    assert.ok(report.violations.length > 0);
  }
});

test("pizza audit rejects money and stock corruption even if derived values agree", () => {
  for (const field of ["stock", "revenue", "tips", "initialStock"]) {
    const { world, expected } = fixture();
    world.shops[0][field]++;
    world.summaries[0][field]++;
    world.leaderboards["tenant-a"][0][field]++;
    assert.equal(auditWorld(world, expected).passed, false, field);
  }
});

test("pizza audit rejects early ovens, impossible delivery clocks, and residual timers", () => {
  const corruptions = [
    (w) => { w.orders[0].dueAt++; },
    (w) => { w.orders[0].readyAt = w.orders[0].dueAt - 1; },
    (w) => { w.orders[0].deliveredAt = w.orders[0].readyAt - 1; },
    (w) => { w.orders[0].createdAt = null; },
    (w) => { w.orders[0].readyAt = NaN; },
    (w) => { w.timers.push({ state: "failed" }); },
  ];
  for (const corrupt of corruptions) {
    const { world, expected } = fixture();
    corrupt(world);
    assert.equal(auditWorld(world, expected).passed, false);
  }
});

test("pizza audit checks job identities, final state, payloads, and completion records", () => {
  const corruptions = [
    (w) => { w.jobs.pop(); },
    (w) => { w.jobs.push({ ...w.jobs[0] }); },
    (w) => { w.jobs[0].state = "leased"; },
    (w) => { w.jobs[0].lease = { owner: "old-drone", token: 1, expiresAt: 1_100 }; },
    (w) => { w.jobs[0].error = { code: "LEASE_EXPIRED" }; },
    (w) => { w.jobs[0].payload.orderId = "pizza-b"; },
    (w) => { w.jobs[0].result.deliveredAt++; },
    (w) => { w.jobs[0].createdAt++; },
    (w) => { w.jobs[0].updatedAt++; },
    (w) => { w.jobs[0].attempts = 0; },
  ];
  for (const corrupt of corruptions) {
    const { world, expected } = fixture();
    corrupt(world);
    assert.equal(auditWorld(world, expected).passed, false);
  }
  const { world, expected } = fixture();
  world.jobs[0].attempts = 3;
  assert.equal(auditWorld(world, expected).passed, true, "reclaimed work may need multiple claims");
});

test("pizza audit reconstructs every summary field and deterministic leaderboard order", () => {
  for (const mutate of [
    (w) => { w.summaries[0].deliveredQuantity++; },
    (w) => { w.summaries[1].orders--; },
    (w) => { w.summaries.reverse(); },
    (w) => { w.leaderboards["tenant-a"].reverse(); },
    (w) => { w.leaderboards["tenant-a"][0].tips++; },
  ]) {
    const { world, expected } = fixture();
    mutate(world);
    assert.equal(auditWorld(world, expected).passed, false);
  }
  const { world, expected } = fixture();
  world.shops[0].tips = 14;
  expected.tips[JSON.stringify(["tenant-a", "store-0"])] = 14;
  world.summaries[0].tips = 14;
  world.leaderboards["tenant-a"][0].tips = 14;
  assert.equal(auditWorld(world, expected).passed, true, "equal scores break ties by shop ID");
});

test("pizza audit reports malformed data without throwing and bounds diagnostics", () => {
  for (const world of [null, undefined, [], {}, { config: null, shops: [null, 5], orders: "oops", jobs: false }]) {
    const report = auditWorld(world, { orders: [], tips: {} });
    assert.equal(report.passed, false);
    assert.ok(report.violations.length > 0 && report.violations.length <= 100);
  }
  const { world, expected } = fixture();
  assert.equal(auditWorld(world, null).passed, false);
  expected.orders.push({ ...expected.orders[0] });
  assert.equal(auditWorld(world, expected).passed, false);
  world.orders = Array.from({ length: 1_000 }, () => null);
  assert.equal(auditWorld(world, expected).violations.length, 100);
  const malformed = fixture();
  const nonnumeric = { valueOf: null, toString: null };
  malformed.world.orders[0].createdAt = nonnumeric;
  malformed.world.shops[0].revenue = nonnumeric;
  malformed.world.config.initialStock = nonnumeric;
  assert.equal(auditWorld(malformed.world, malformed.expected).passed, false);
});


test("audit distinguishes repeated local order/store IDs across tenants and detects crossed tenant keys", () => {
  const { world, expected } = fixture();
  const other = fixture();
  other.world.config.tenantIds = ["tenant-b"];
  for (const row of [...other.world.shops, ...other.world.summaries, ...other.world.leaderboards["tenant-a"]]) {
    row.id = ["tenant-b", row.id[1]]; row.key = JSON.stringify(row.id);
  }
  for (const row of other.world.orders) { row.shop = ["tenant-b", row.shop[1]]; row.key = JSON.stringify([...row.shop, row.id]); }
  for (const row of other.world.jobs) { row.payload.shop = ["tenant-b", row.payload.shop[1]]; row.id = JSON.stringify([...row.payload.shop, row.payload.orderId]); }
  world.config.tenantIds.push("tenant-b");
  world.config.shopIds.push(...other.world.shops.map((row) => row.id));
  for (const field of ["shops", "orders", "jobs", "summaries"]) world[field].push(...other.world[field]);
  world.leaderboards["tenant-b"] = other.world.leaderboards["tenant-a"];
  expected.orders.push(...other.world.orders.map(({ id, shop, quantity }) => ({ id, shop, quantity })));
  expected.tips[JSON.stringify(["tenant-b", "store-0"])] = 30;
  assert.equal(auditWorld(world, expected).passed, true);
  world.jobs.at(-1).id = world.jobs[0].id;
  assert.equal(auditWorld(world, expected).passed, false);
});
