import { collection, define, derive, fail, mutation, query, v } from "../sdk/index.ts";
import type { Context } from "../sdk/index.ts";

const id = v.string({ min: 1, max: 96 });
const cents = v.int({ min: 0 });
const line = v.object({ id, quantity: v.int({ min: 0 }), unitCents: cents });

export const orders = collection("orders", v.object({ shippingCents: cents }));
export const lines = collection("orderLines", v.object({ orderId: id, quantity: v.int({ min: 0 }), unitCents: cents }))
  .index("byOrder", ["orderId"]);

export const subtotal = derive("order.subtotal", (ctx, orderId: string) =>
  ctx.query(lines.by("byOrder").eq(orderId)).reduce((sum, line) => sum + line.quantity * line.unitCents, 0),
);

export const total = derive("order.total", (ctx, orderId: string) => {
  const order = ctx.get(orders, orderId) ?? fail("ORDER_NOT_FOUND", `Order ${orderId} does not exist`);
  return ctx.get(subtotal, orderId) + order.shippingCents;
}, { materialize: { each: orders } });

function readOrder(ctx: Context, orderId: string) {
  const order = ctx.get(orders, orderId) ?? fail("ORDER_NOT_FOUND", `Order ${orderId} does not exist`);
  return { order, subtotal: ctx.get(subtotal, orderId), total: ctx.get(total, orderId) };
}

export const createOrder = mutation("internal.order.create", {
  args: v.object({ orderId: id, shippingCents: cents, lines: v.array(line) }),
}, (ctx, args) => {
  if (ctx.get(orders, args.orderId)) fail("ORDER_EXISTS", "Order already exists");
  ctx.set(orders, args.orderId, { shippingCents: args.shippingCents });
  for (const { id: lineId, quantity, unitCents } of args.lines) {
    if (ctx.get(lines, lineId)) fail("LINE_EXISTS", `Line ${lineId} already exists`);
    ctx.set(lines, lineId, { orderId: args.orderId, quantity, unitCents });
  }
  return readOrder(ctx, args.orderId);
});

export const updateLine = mutation("internal.order.updateLine", {
  args: v.object({ lineId: id, quantity: v.int({ min: 0 }) }),
}, (ctx, args) => {
  const line = ctx.get(lines, args.lineId) ?? fail("LINE_NOT_FOUND", "Line does not exist");
  ctx.set(lines, args.lineId, { ...line, quantity: args.quantity });
  return readOrder(ctx, line.orderId);
});

export const updateShipping = mutation("internal.order.updateShipping", {
  args: v.object({ orderId: id, shippingCents: cents }),
}, (ctx, args) => {
  if (ctx.get(orders, args.orderId) === null) fail("ORDER_NOT_FOUND", "Order does not exist");
  ctx.set(orders, args.orderId, { shippingCents: args.shippingCents });
  return readOrder(ctx, args.orderId);
});

export const getOrder = query("internal.order.read", { args: id }, readOrder);

// Registered for internal use, deliberately omitted from the HTTP allowlist.
export const privateReset = mutation("internal.order.reset", { args: id }, (ctx, orderId) => {
  ctx.delete(orders, orderId);
  for (const row of ctx.scan(lines, { index: "byOrder", prefix: [orderId] })) ctx.delete(lines, row.key);
  return null;
});

const app = define({
  collections: [lines],
  definitions: [subtotal, total, privateReset],
  http: {
    "order.create": createOrder,
    "order.updateLine": updateLine,
    "order.updateShipping": updateShipping,
    "order.get": getOrder,
  },
});
export default app;
