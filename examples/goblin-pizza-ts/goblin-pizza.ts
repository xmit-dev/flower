import { aggregate, canonicalJson, collection, define, derive, fail, mutation, query, v } from "../../sdk/index.ts";
import type { Context } from "../../sdk/index.ts";
import { scheduler } from "../../sdk/scheduler.ts";
import { queue } from "../../sdk/temporal.ts";

// Goblins run the kitchens; drones carry the pizzas. Everything here, including
// oven timers, lease policy, stock accounting, and the leaderboard, is TypeScript.
// Money is an integer number of copper coins. Successful records stay available
// for the benchmark's independent audit until pizza.archive folds delivered
// orders into per-kitchen tallies; start a fresh database for each run.
export const UNIT_PRICE = 7;
export const MAX_LEASE_MS = 60_000;
export type ShopRef = [tenant: string, store: string];
export const shopKey = (shop: ShopRef): string => canonicalJson(shop);
export const orderKey = (shop: ShopRef, id: string): string => canonicalJson([...shop, id]);

const identifier = v.string({ pattern: /^[A-Za-z0-9_-]{1,96}$/ });
const shopRef = v.tuple([identifier, identifier]);
const history = v.object({ database: v.string(), incarnation: v.string() });

export interface PizzaConfig {
  tenantIds: string[];
  shopIds: ShopRef[];
  storesPerTenant: number;
  initialStock: number;
  bakeMs: number;
  leaseMs: number;
  unitPrice: number;
}
export interface PizzaShop {
  id: ShopRef;
  key: string;
  name: string;
  initialStock: number;
  stock: number;
  revenue: number;
  tips: number;
}
export interface PizzaOrder {
  id: string;
  key: string;
  shop: ShopRef;
  quantity: number;
  status: "baking" | "ready" | "delivered";
  createdAt: number;
  dueAt: number;
  readyAt: number | null;
  deliveredAt: number | null;
}
export interface OrderStats {
  orders: number;
  baking: number;
  ready: number;
  delivered: number;
  orderedQuantity: number;
  deliveredQuantity: number;
}
export interface ShopSummary extends PizzaShop, OrderStats {}
export interface Delivery { orderId: string; shop: ShopRef; quantity: number }
export interface ArchivedOrders { orders: number; pizzas: number }

export const configuration = collection<PizzaConfig>("pizza.config");
export const tenants = collection<PizzaConfig>("pizza.tenants");
export const shops = collection<PizzaShop>("pizza.shops").key(shopRef);
export const archived = collection<ArchivedOrders>("pizza.archived").key(shopRef);
export const orders = collection<PizzaOrder>("pizza.orders")
  .key(v.tuple([identifier, identifier, identifier]))
  .index("byShop", ["shop"]);
// Each tenant's drones claim from their own scope of one indexed queue.
export const deliveries = queue<Delivery, { deliveredAt: number }>("pizza.deliveries", {
  lease: { defaultMs: MAX_LEASE_MS, maxMs: MAX_LEASE_MS }, retry: false,
});

function config(ctx: Context): PizzaConfig {
  return ctx.get(configuration, "world") ?? fail("NOT_INITIALIZED", "The goblin kitchens are not open yet");
}

function tenantConfig(ctx: Context, tenant: string): PizzaConfig {
  return ctx.get(tenants, tenant) ?? fail("TENANT_NOT_FOUND", `No tenant named ${tenant}`);
}

function shopRecord(ctx: Context, id: ShopRef): PizzaShop {
  return ctx.get(shops, id) ?? fail("SHOP_NOT_FOUND", `No goblin kitchen named ${shopKey(id)}`);
}

// Rust keeps a durable equality index and feeds only changed orders into these
// reversible reducers. A tip does not touch order totals; an order change costs
// one remove/add pair, regardless of how many pizzas the kitchen has sold.
function adjustOrders(total: OrderStats, order: PizzaOrder, direction: number): OrderStats {
  return {
    orders: total.orders + direction,
    baking: total.baking + direction * Number(order.status === "baking"),
    ready: total.ready + direction * Number(order.status === "ready"),
    delivered: total.delivered + direction * Number(order.status === "delivered"),
    orderedQuantity: total.orderedQuantity + direction * order.quantity,
    deliveredQuantity: total.deliveredQuantity + direction * (order.status === "delivered" ? order.quantity : 0),
  };
}
export const orderStats = aggregate("pizza.orderStats", {
  source: orders, index: "byShop",
  initial: (): OrderStats => ({ orders: 0, baking: 0, ready: 0, delivered: 0, orderedQuantity: 0, deliveredQuantity: 0 }),
  add: (total, order) => adjustOrders(total, order, 1),
  remove: (total, order) => adjustOrders(total, order, -1),
});

// One maintained summary per kitchen, created and removed with its shop row.
export const shopSummary = derive("pizza.shopSummary", (ctx, id: ShopRef): ShopSummary => ({
  ...shopRecord(ctx, id),
  ...ctx.get(orderStats, id),
}), { materialize: { each: shops } });

// Rankings are an observational projection of the snapshot's durable summaries.
// Computing them on read avoids sorting and replicating a whole tenant ranking
// for every tip. Watching the dashboard still sees a coherent ranking and totals.
export const leaderboard = derive("pizza.leaderboard", (ctx, tenant: string): ShopSummary[] =>
  tenantConfig(ctx, tenant).shopIds.map((id) => ctx.get(shopSummary, id)).sort((a, b) =>
    (b.revenue + b.tips) - (a.revenue + a.tips) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)),
);

// A private callback represents the oven bell. The status transition and queue
// insertion commit together; an interrupted attempt cannot publish half a pizza.
export const finishBaking = mutation("internal.pizza.finishBaking", { args: v.object({ id: identifier, shop: shopRef }) }, (ctx, { id, shop }) => {
  const order = ctx.get(orders, [...shop, id]) ?? fail("ORDER_NOT_FOUND", "The oven lost its order");
  if (order.status !== "baking") return null;
  ctx.set(orders, [...shop, id], { ...order, status: "ready", readyAt: ctx.now() });
  deliveries.scope(shop[0]).enqueue(ctx, order.key, { orderId: order.id, shop: order.shop, quantity: order.quantity });
  return null;
});

export const ovens = scheduler("pizza.ovens", { bake: finishBaking }, {
  maxAttempts: 3, retryDelayMs: 100, maxRetryDelayMs: 1_000,
});

export const setup = mutation("internal.pizza.setup", {
  args: v.object({
    tenants: v.array(identifier, { min: 1 }),
    storesPerTenant: v.int({ min: 1 }),
    stockPerShop: v.int({ min: 1, max: 1_000_000 }),
    bakeMs: v.int({ min: 0, max: 60_000 }),
    leaseMs: v.int({ min: 1, max: MAX_LEASE_MS }),
  }),
}, (ctx, input): PizzaConfig => {
  if (new Set(input.tenants).size !== input.tenants.length) fail("INVALID_ARGUMENT", "Tenant IDs must be distinct");
  if (!Number.isSafeInteger(input.tenants.length * input.storesPerTenant)) fail("INVALID_ARGUMENT", "Too many stores");
  if (ctx.get(configuration, "world") !== null) fail("ALREADY_INITIALIZED", "The kitchens are already open; use a fresh database for another run");
  const names = ["The Crispy Cauldron", "Mushroom Mayhem", "The Sizzling Slime", "Dough or Die", "The Goblin's Slice", "Dragon Breath Delivery"];
  const shopIds = input.tenants.flatMap((tenant) => Array.from({ length: input.storesPerTenant }, (_, index): ShopRef => [tenant, `store-${index}`]));
  const settings: PizzaConfig = {
    tenantIds: input.tenants, shopIds, storesPerTenant: input.storesPerTenant, initialStock: input.stockPerShop,
    bakeMs: input.bakeMs, leaseMs: input.leaseMs, unitPrice: UNIT_PRICE,
  };
  ctx.set(configuration, "world", settings);
  for (const tenant of input.tenants) {
    ctx.set(tenants, tenant, { ...settings, tenantIds: [tenant], shopIds: shopIds.filter(([owner]) => owner === tenant) });
  }
  shopIds.forEach((id, index) => ctx.set(shops, id, {
    id, key: shopKey(id), name: `${names[index % names.length]} · ${id[1]}`,
    initialStock: input.stockPerShop, stock: input.stockPerShop, revenue: 0, tips: 0,
  }));
  return settings;
});

export const placeOrder = mutation("internal.pizza.order", {
  args: v.object({ id: identifier, shop: shopRef, quantity: v.int({ min: 1, max: 4 }) }),
}, (ctx, input): PizzaOrder => {
  const shop = shopRecord(ctx, input.shop);
  const settings = tenantConfig(ctx, shop.id[0]);
  if (ctx.get(orders, [...shop.id, input.id]) !== null) fail("ORDER_EXISTS", "That pizza order already exists in this store");
  if (shop.stock < input.quantity) fail("OUT_OF_STOCK", "The goblins have run out of enchanted dough");
  const order: PizzaOrder = {
    id: input.id, key: orderKey(shop.id, input.id), shop: shop.id, quantity: input.quantity, status: "baking",
    createdAt: ctx.now(), dueAt: ctx.now() + settings.bakeMs, readyAt: null, deliveredAt: null,
  };
  ctx.set(shops, shop.id, { ...shop, stock: shop.stock - order.quantity });
  ctx.set(orders, [...shop.id, input.id], order);
  ovens.after(ctx, `bake:${order.key}`, settings.bakeMs, "bake", { id: order.id, shop: shop.id });
  return order;
});

export const claimDelivery = mutation("internal.pizza.claim", {
  args: v.object({ tenant: identifier, owner: identifier, leaseMs: v.optional(v.int({ min: 1 })) }),
}, (ctx, input) => {
  const settings = tenantConfig(ctx, input.tenant);
  const leaseMs = input.leaseMs ?? settings.leaseMs;
  if (leaseMs > settings.leaseMs) fail("INVALID_ARGUMENT", `Leases last at most ${settings.leaseMs} ms`);
  return deliveries.scope(input.tenant).claim(ctx, input.owner, { leaseMs });
});

export const deliverPizza = mutation("internal.pizza.deliver", {
  args: v.object({ tenant: identifier, id: v.string({ min: 1 }), owner: identifier, token: v.int({ min: 1 }), history: v.optional(history) }),
}, (ctx, { tenant, ...identity }): PizzaOrder => {
  tenantConfig(ctx, tenant);
  const deliveredAt = ctx.now();
  // Validate the lease before accounting. An expired or replaced drone cannot
  // collect coins; a failed transaction also discards this staged completion.
  const job = deliveries.scope(tenant).complete(ctx, identity, { deliveredAt });
  if (job.payload.shop[0] !== tenant || orderKey(job.payload.shop, job.payload.orderId) !== identity.id) fail("LEASE_LOST", "Delivery tenant and identity must match");
  const key: [string, string, string] = [...job.payload.shop, job.payload.orderId];
  const order = ctx.get(orders, key) ?? fail("ORDER_NOT_FOUND", "This drone has no pizza");
  if (order.status !== "ready") fail("ORDER_NOT_READY", "Only a ready pizza can be delivered");
  const shop = shopRecord(ctx, order.shop);
  const delivered: PizzaOrder = { ...order, status: "delivered", deliveredAt };
  ctx.set(orders, key, delivered);
  ctx.set(shops, shop.id, { ...shop, revenue: shop.revenue + order.quantity * UNIT_PRICE });
  return delivered;
});

export const tipKitchen = mutation("internal.pizza.tip", {
  args: v.object({ shop: shopRef, amount: v.int({ min: 1, max: 1_000_000 }) }),
}, (ctx, input) => {
  const shop = shopRecord(ctx, input.shop);
  const tips = shop.tips + input.amount;
  if (tips > Number.MAX_SAFE_INTEGER - shop.initialStock * UNIT_PRICE) fail("INVALID_ARGUMENT", "Total tips overflow");
  ctx.set(shops, shop.id, { ...shop, tips });
  return { shop: shop.id, tips };
});

// A long-running kitchen clears delivered orders off the board so its rows,
// queue and dashboard stay the size of the work in flight. Their counts move
// to one tally per kitchen in the same transaction; revenue stays on the shop.
// Summaries and pizza.world then cover live orders only; the dashboard adds
// the tallies back. The benchmark never archives, so it audits every order.
export const archiveDeliveries = mutation("internal.pizza.archive", {
  args: v.object({ tenant: identifier, olderThanMs: v.int({ min: 0, max: 86_400_000 }), limit: v.int({ min: 1, max: 1_000 }) }),
}, (ctx, { tenant, olderThanMs, limit }) => {
  tenantConfig(ctx, tenant);
  const cutoff = ctx.now() - olderThanMs;
  const queue = deliveries.scope(tenant);
  let count = 0;
  for (const job of queue.scan(ctx)) {
    if (count >= limit) break;
    if (job.state !== "completed" || job.updatedAt > cutoff) continue;
    const key: [string, string, string] = [...job.payload.shop, job.payload.orderId];
    const order = ctx.get(orders, key);
    if (order?.status !== "delivered") continue;
    const tally = ctx.get(archived, order.shop) ?? { orders: 0, pizzas: 0 };
    ctx.set(archived, order.shop, { orders: tally.orders + 1, pizzas: tally.pizzas + order.quantity });
    ctx.delete(orders, key);
    queue.cancel(ctx, job.id);
    count++;
  }
  return { archived: count };
});

const inspect = (ctx: Context, id: ShopRef) => ctx.get(shopSummary, id);
export const inspectShop = query("internal.pizza.shop", { args: shopRef }, inspect);
// Browsing can use a replica's coherent applied snapshot without contacting the
// leader. This preview may lag; stock checks and money updates stay in mutations.
export const inspectShopLocal = query("internal.pizza.shop.local", { args: shopRef, consistency: "replica-local" }, inspect);

// This deliberately public audit method returns raw business records as well as
// derived summaries, so a benchmark can independently verify every invariant.
export const inspectWorld = query("internal.pizza.world", { args: v.null() }, (ctx) => {
  const settings = config(ctx);
  return {
    config: settings,
    shops: ctx.scan(shops).map((row) => row.value),
    orders: ctx.scan(orders).map((row) => row.value),
    jobs: settings.tenantIds.flatMap((tenant) => deliveries.scope(tenant).scan(ctx)),
    timers: ovens.scan(ctx),
    summaries: settings.shopIds.map((id) => ctx.get(shopSummary, id)),
    leaderboards: Object.fromEntries(settings.tenantIds.map((tenant) => [tenant, ctx.get(leaderboard, tenant)])),
  };
});

// One public value drives the entire dashboard. Stable object keys let SSE
// patches address one order/job instead of shifting a table's array indexes.
// Keep the 120 orders with the latest activity on screen, so fresh orders,
// pizzas out of the oven and deliveries all show at any pace, with every
// delivery and oven timer still in flight. Summaries and totals, including
// archived orders, still cover the whole world. This observational view
// tolerates replication lag, including older code and aliases. Use pizza.world
// for a fresh audit; actions validate current state.
const activity = (order: PizzaOrder): number => order.deliveredAt ?? order.readyAt ?? order.createdAt;
function withArchived(shop: ShopSummary, tally: ArchivedOrders | null): ShopSummary {
  if (!tally) return shop;
  return {
    ...shop, orders: shop.orders + tally.orders, delivered: shop.delivered + tally.orders,
    orderedQuantity: shop.orderedQuantity + tally.pizzas, deliveredQuantity: shop.deliveredQuantity + tally.pizzas,
  };
}
export const inspectDashboard = query("internal.pizza.dashboard", {
  args: v.object({ tenant: identifier }), consistency: "replica-local",
}, (ctx, { tenant }) => {
  const settings = tenantConfig(ctx, tenant);
  const tallies = settings.shopIds.map((id) => ctx.get(archived, id));
  const summaries = settings.shopIds.map((id, index) => withArchived(ctx.get(shopSummary, id), tallies[index]));
  const recent = settings.shopIds.flatMap((id) => ctx.query(orders.by("byShop").eq(id)))
    .sort((a, b) => activity(b) - activity(a) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)).slice(0, 120);
  return {
    tenant, tenantIds: config(ctx).tenantIds,
    config: settings,
    summaries: Object.fromEntries(summaries.map((shop) => [shop.key, shop])),
    orders: Object.fromEntries(recent.map((order) => [order.key, order])),
    jobs: Object.fromEntries(deliveries.scope(tenant).scan(ctx).filter((job) => job.state !== "completed").map((job) => [job.id, job])),
    timers: Object.fromEntries(ovens.scan(ctx).filter((timer) => timer.handler === "bake" && (timer.args as { shop: ShopRef }).shop[0] === tenant)
      .map((timer) => [timer.id, timer])),
    leaderboard: ctx.get(leaderboard, tenant).map((shop) => shop.key),
    totals: summaries.reduce((total, shop) => ({
      orders: total.orders + shop.orders, baking: total.baking + shop.baking,
      ready: total.ready + shop.ready, delivered: total.delivered + shop.delivered,
      pizzas: total.pizzas + shop.deliveredQuantity, revenue: total.revenue + shop.revenue,
      tips: total.tips + shop.tips,
    }), { orders: 0, baking: 0, ready: 0, delivered: 0, pizzas: 0, revenue: 0, tips: 0 }),
    archived: tallies.reduce((total, tally) => total + (tally?.orders ?? 0), 0),
  };
});
export type PizzaDashboard = ReturnType<typeof inspectDashboard.compute>;

const app = define({
  uses: [ovens, deliveries],
  collections: [orders],
  definitions: [orderStats, shopSummary, leaderboard],
  http: {
    "pizza.setup": setup,
    "pizza.order": placeOrder,
    "pizza.claim": claimDelivery,
    "pizza.deliver": deliverPizza,
    "pizza.tip": tipKitchen,
    "pizza.archive": archiveDeliveries,
    "pizza.shop": inspectShop,
    "pizza.shop.local": inspectShopLocal,
    "pizza.world": inspectWorld,
    "pizza.dashboard": inspectDashboard,
  },
});
export default app;
