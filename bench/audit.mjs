import { isDeepStrictEqual } from "node:util";

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const isInteger = (value, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) => Number.isSafeInteger(value) && value >= minimum && value <= maximum;
const isId = (value) => typeof value === "string" && /^[A-Za-z0-9_-]{1,96}$/.test(value);

const isShop = (value) => Array.isArray(value) && value.length === 2 && value.every(isId);
const shopKey = (value) => isShop(value) ? JSON.stringify(value) : null;
const orderKey = (row) => isObject(row) && isShop(row.shop) && isId(row.id) ? JSON.stringify([...row.shop, row.id]) : null;
const jobKey = (row) => {
  try {
    const parts = JSON.parse(row.id);
    return Array.isArray(parts) && parts.length === 3 && parts.every(isId) && JSON.stringify(parts) === row.id ? row.id : null;
  } catch { return null; }
};

/**
 * Check the settled pizza world against acknowledged workload inputs. Derived
 * values never serve as the accounting oracle: rebuild them from source orders,
 * shop records, and the client's successful order/tip ledger.
 */
export function auditWorld(world, expected = {}) {
  const violations = [];
  let failed = false;
  function check(condition, message) {
    if (condition) return true;
    failed = true;
    if (violations.length < 100) violations.push(message);
    return false;
  }
  function array(value, label) {
    return check(Array.isArray(value), `${label} must be an array`) ? value : [];
  }
  function records(value, label, identity, requireKey = false) {
    const rows = array(value, label);
    const result = new Map();
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      if (!check(isObject(row), `${label}[${index}] must be a record`)) continue;
      const key = identity(row);
      if (!check(key !== null, `${label}[${index}] has an invalid ID`)) continue;
      if (requireKey) check(row.key === key, `${label}[${index}] key does not match its identity`);
      if (check(!result.has(key), `${label} contains duplicate ID ${key}`)) result.set(key, row);
    }
    return { rows, byId: result };
  }
  function sameIds(actual, desired, label) {
    for (const id of desired.keys()) check(actual.has(id), `${label} is missing ${id}`);
    for (const id of actual.keys()) check(desired.has(id), `${label} contains unexpected ${id}`);
  }
  function equal(actual, desired, label) {
    check(isDeepStrictEqual(actual, desired), `${label} does not match independently reconstructed values`);
  }
  function add(left, right, label) {
    if (!isInteger(right)) return left;
    const sum = left + right;
    return check(isInteger(sum), `${label} exceeds the safe integer range`) ? sum : left;
  }

  if (!check(isObject(world), "World must be an object")) world = {};
  if (!check(isObject(expected), "Expected workload must be an object")) expected = {};
  const accepted = records(expected.orders, "Acknowledged orders", orderKey);
  const expectedTips = check(isObject(expected.tips), "Acknowledged tips must be an object") ? expected.tips : {};
  const config = check(isObject(world.config), "World config must be an object") ? world.config : {};
  const shopIds = array(config.shopIds, "config.shopIds");
  check(shopIds.length >= 1, "Config must name at least one shop");
  const tenants = array(config.tenantIds, "config.tenantIds");
  check(tenants.length > 0 && tenants.every(isId) && new Set(tenants).size === tenants.length, "Tenant IDs must be unique valid identifiers");
  const configuredShops = new Map();
  for (const id of shopIds) {
    const key = shopKey(id);
    if (!check(key !== null, "config.shopIds contains an invalid ID")) continue;
    check(tenants.includes(id[0]), `Store ${key} has an unknown tenant`);
    if (check(!configuredShops.has(key), `config.shopIds contains duplicate ID ${key}`)) configuredShops.set(key, id);
  }
  check(isInteger(config.initialStock, 1, 1_000_000), "config.initialStock is invalid");
  check(isInteger(config.bakeMs, 0, 60_000), "config.bakeMs is invalid");
  check(isInteger(config.leaseMs, 1, 60_000), "config.leaseMs is invalid");
  check(config.unitPrice === 7, "config.unitPrice must be seven copper coins");
  const shops = records(world.shops, "Shops", (row) => shopKey(row.id), true);
  const orders = records(world.orders, "Orders", orderKey, true);
  const jobs = records(world.jobs, "Jobs", jobKey);
  sameIds(shops.byId, configuredShops, "Shops");
  sameIds(orders.byId, accepted.byId, "Orders");
  sameIds(jobs.byId, orders.byId, "Jobs");
  check(array(world.timers, "Timers").length === 0, "Settled world still contains pending or failed timers");

  for (const [id, amount] of Object.entries(expectedTips)) {
    check(configuredShops.has(id), `Acknowledged tips refer to unknown shop ${id}`);
    check(isInteger(amount), `Acknowledged tips for ${id} are invalid`);
  }
  for (const [id, order] of accepted.byId) {
    check(configuredShops.has(shopKey(order.shop)), `Acknowledged order ${id} refers to an unknown shop`);
    check(isInteger(order.quantity, 1, 4), `Acknowledged order ${id} has an invalid quantity`);
  }

  let delivered = 0;
  let pizzas = 0;
  const counts = new Map(Array.from(configuredShops.keys(), (id) => [id, {
    orders: 0, baking: 0, ready: 0, delivered: 0, orderedQuantity: 0, deliveredQuantity: 0,
  }]));
  for (const [id, order] of orders.byId) {
    const original = accepted.byId.get(id);
    if (original) {
      check(isDeepStrictEqual(order.shop, original.shop), `Order ${id} changed its shop`);
      check(order.quantity === original.quantity, `Order ${id} changed its quantity`);
    }
    check(configuredShops.has(shopKey(order.shop)), `Order ${id} refers to an unknown shop`);
    const validQuantity = check(isInteger(order.quantity, 1, 4), `Order ${id} has an invalid quantity`);
    check(order.status === "delivered", `Order ${id} is not delivered`);
    if (order.status === "delivered") delivered++;
    if (validQuantity) pizzas = add(pizzas, order.quantity, "Total pizza count");
    const timestamps = ["createdAt", "dueAt", "readyAt", "deliveredAt"];
    for (const field of timestamps) check(isInteger(order[field]), `Order ${id} has an invalid ${field}`);
    check(isInteger(order.createdAt) && isInteger(config.bakeMs) && isInteger(order.createdAt + config.bakeMs) && order.dueAt === order.createdAt + config.bakeMs,
      `Order ${id} has the wrong baking deadline`);
    check(isInteger(order.readyAt) && isInteger(order.dueAt) && order.readyAt >= order.dueAt,
      `Order ${id} became ready before its deadline`);
    check(isInteger(order.deliveredAt) && isInteger(order.readyAt) && order.deliveredAt >= order.readyAt,
      `Order ${id} was delivered before it became ready`);
    const aggregate = counts.get(shopKey(order.shop));
    if (aggregate) {
      aggregate.orders++;
      if (["baking", "ready", "delivered"].includes(order.status)) aggregate[order.status]++;
      if (validQuantity) {
        aggregate.orderedQuantity = add(aggregate.orderedQuantity, order.quantity, `${order.shop} ordered quantity`);
        if (order.status === "delivered") aggregate.deliveredQuantity = add(aggregate.deliveredQuantity, order.quantity, `${order.shop} delivered quantity`);
      }
    }
  }

  for (const [id, job] of jobs.byId) {
    const order = orders.byId.get(id);
    check(job.state === "completed", `Job ${id} is not completed`);
    check(job.lease === null, `Completed job ${id} still has a lease`);
    check(job.error === null, `Completed job ${id} still has an error`);
    check(isInteger(job.attempts, 1), `Job ${id} has an invalid attempt count`);
    check(isInteger(job.createdAt), `Job ${id} has an invalid createdAt`);
    check(isInteger(job.updatedAt), `Job ${id} has an invalid updatedAt`);
    if (order) {
      equal(job.payload, { orderId: order.id, shop: order.shop, quantity: order.quantity }, `Job ${id} payload`);
      equal(job.result, { deliveredAt: order.deliveredAt }, `Job ${id} result`);
      check(job.createdAt === order.readyAt, `Job ${id} was not created when its pizza became ready`);
      check(job.updatedAt === order.deliveredAt, `Job ${id} was not completed when its pizza was delivered`);
    }
  }

  let revenue = 0;
  let tips = 0;
  const summaries = [];
  for (const id of configuredShops.keys()) {
    const shop = shops.byId.get(id);
    if (!shop) continue;
    const aggregate = counts.get(id);
    check(typeof shop.name === "string" && shop.name.length > 0, `Shop ${id} has an invalid name`);
    check(isInteger(shop.initialStock, 1) && shop.initialStock === config.initialStock, `Shop ${id} has the wrong initial stock`);
    check(isInteger(shop.stock) && isInteger(config.initialStock) && shop.stock === config.initialStock - aggregate.orderedQuantity, `Shop ${id} stock is not conserved`);
    check(isInteger(shop.revenue) && isInteger(config.unitPrice) && shop.revenue === aggregate.deliveredQuantity * config.unitPrice, `Shop ${id} revenue is not conserved`);
    const acknowledgedTips = Object.hasOwn(expectedTips, id) ? expectedTips[id] : 0;
    check(isInteger(shop.tips) && shop.tips === acknowledgedTips, `Shop ${id} tips do not match acknowledged mutations`);
    check(isInteger(shop.revenue) && isInteger(shop.tips) && isInteger(shop.revenue + shop.tips), `Shop ${id} score exceeds the safe integer range`);
    revenue = add(revenue, shop.revenue, "Total revenue");
    tips = add(tips, shop.tips, "Total tips");
    summaries.push({
      id: shop.id, key: shop.key, name: shop.name, initialStock: shop.initialStock, stock: shop.stock, revenue: shop.revenue, tips: shop.tips,
      ...aggregate,
    });
  }
  equal(world.summaries, summaries, "Shop summaries");
  const score = (shop) => isInteger(shop.revenue) && isInteger(shop.tips) ? shop.revenue + shop.tips : 0;
  const leaderboards = Object.fromEntries(tenants.map((tenant) => [tenant, summaries.filter((shop) => shop.id[0] === tenant).sort((a, b) =>
    score(b) - score(a) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0))]));
  equal(world.leaderboards, leaderboards, "Tenant leaderboards");
  return { passed: !failed, violations, orders: orders.rows.length, delivered, pizzas, revenue, tips };
}
