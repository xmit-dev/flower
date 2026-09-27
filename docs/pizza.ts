import { collection, define, derive, fail, mutation, query, v } from "@flower-js/sdk";

const orders = collection("orders", v.object({ topping: v.string({ min: 1 }) }));

const board = derive("board", (ctx) => {
  const pizzas = ctx.scan(orders).map(({ value }) => value);
  return {
    orders: pizzas.length,
    mushroom: pizzas.filter((pizza) => pizza.topping === "mushroom").length,
  };
}, { materialize: "always" });

const order = mutation("order", {
  args: v.object({ id: v.string({ min: 1 }), topping: v.string({ min: 1 }) }),
}, (ctx, { id, topping }) => {
  if (ctx.get(orders, id)) fail("ALREADY_ORDERED", "Already ordered!");
  ctx.set(orders, id, { topping });
  return { accepted: id };
});

const dashboard = query("dashboard", (ctx) => ctx.get(board));

const app = define({
  definitions: [board],
  http: { "pizza.order": order, "pizza.board": dashboard },
});
export default app;
