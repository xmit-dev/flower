// Memory-footprint workload: the orders example (an equality/ordered index,
// an index-reading derivation and a materialized total per order) plus a wider
// customer record per order. Seeded through src/bin/flower-footprint.rs.
import { collection, define, derive, mutation, v } from "../sdk/index.ts";

const id = v.string({ min: 1, max: 96 });
const cents = v.int({ min: 0 });

export const orders = collection("orders", v.object({ customerId: id, shippingCents: cents }));
export const lines = collection("orderLines", v.object({ orderId: id, quantity: v.int({ min: 0 }), unitCents: cents }))
  .index("byOrder", ["orderId"]);
export const customers = collection("customers", v.object({
  name: v.string(),
  email: v.string(),
  address: v.object({ street: v.string(), city: v.string(), postcode: v.string(), country: v.string() }),
  tags: v.array(v.string()),
  createdAt: v.int({ min: 0 }),
  marketing: v.boolean(),
}));

export const subtotal = derive("order.subtotal", (ctx, orderId: string) =>
  ctx.query(lines.by("byOrder").eq(orderId)).reduce((sum, line) => sum + line.quantity * line.unitCents, 0),
);

export const total = derive("order.total", (ctx, orderId: string) => {
  const order = ctx.get(orders, orderId);
  return order === null ? 0 : ctx.get(subtotal, orderId) + order.shippingCents;
}, { materialize: { each: orders } });

const cities = ["Lisbon", "Porto", "Paris", "Lyon", "Berlin", "Hamburg", "Madrid", "Valencia"];

export const seed = mutation("internal.footprint.seed", {
  args: v.object({ start: v.int({ min: 0 }), count: v.int({ min: 1 }), lines: v.int({ min: 0 }) }),
}, (ctx, args) => {
  for (let n = args.start; n < args.start + args.count; n++) {
    const orderId = `order-${n}`;
    const customerId = `customer-${n}`;
    ctx.set(customers, customerId, {
      name: `Customer ${n}`,
      email: `customer-${n}@example.com`,
      address: { street: `${n % 997} Main Street`, city: cities[n % cities.length], postcode: `${10000 + n % 89999}`, country: "PT" },
      tags: n % 3 === 0 ? ["wholesale", "priority"] : ["retail"],
      createdAt: 1_760_000_000_000 + n,
      marketing: n % 2 === 0,
    });
    ctx.set(orders, orderId, { customerId, shippingCents: 250 + n % 500 });
    for (let k = 0; k < args.lines; k++) {
      ctx.set(lines, `line-${n}-${k}`, { orderId, quantity: 1 + (n + k) % 5, unitCents: 99 + (n * 7 + k * 13) % 2000 });
    }
  }
  return args.count;
});

// Changes one existing line: graph work without creating a materialized root.
export const touch = mutation("internal.footprint.touch", {
  args: v.object({ order: v.int({ min: 0 }), quantity: v.int({ min: 0 }) }),
}, (ctx, args) => {
  const lineId = `line-${args.order}-0`;
  const line = ctx.get(lines, lineId);
  if (line !== null) ctx.set(lines, lineId, { ...line, quantity: args.quantity });
  return line !== null;
});

// Deletes one order with its lines and customer, dropping its materialized root.
export const remove = mutation("internal.footprint.remove", {
  args: v.object({ order: v.int({ min: 0 }), lines: v.int({ min: 0 }) }),
}, (ctx, args) => {
  ctx.delete(orders, `order-${args.order}`);
  ctx.delete(customers, `customer-${args.order}`);
  for (let k = 0; k < args.lines; k++) ctx.delete(lines, `line-${args.order}-${k}`);
  return null;
});

export default define({
  collections: [lines, customers],
  definitions: [subtotal, total],
  http: { seed, touch, remove },
});
