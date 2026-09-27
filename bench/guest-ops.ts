// Guest cost microbenchmark: each call loops one SDK operation inside the
// QuickJS guest. Run it through src/evaluator/perf_tests.rs.
import { canonicalJson, collection, define, query, v } from "../sdk/index.ts";
import { encodeKey } from "../sdk/define.ts";

const identifier = v.string({ pattern: /^[A-Za-z0-9_-]{1,96}$/ });
const shopRef = v.tuple([identifier, identifier]);
const tipArgs = v.object({ shop: shopRef, amount: v.int({ min: 1, max: 1_000_000 }) });
const shops = collection<unknown>("pizza.shops").key(shopRef);
const shop = { id: ["t0", "store-0"], key: '["t0","store-0"]', name: "The Crispy Cauldron · store-0", initialStock: 1000, stock: 900, revenue: 7, tips: 3 };

const ops: Record<string, (ctx: any) => unknown> = {
  noop: () => 0,
  canonicalTuple: () => canonicalJson(["t0", "store-0"]),
  canonicalRecord: () => canonicalJson(shop),
  regex: () => /^[A-Za-z0-9_-]{1,96}$/.test("store-0"),
  identifier: () => identifier.parse("store-0"),
  tuple: () => shopRef.parse(["t0", "store-0"]),
  object: () => tipArgs.parse({ shop: ["t0", "store-0"], amount: 1 }),
  encodeKey: () => encodeKey(shops as any, ["t0", "store-0"]),
  spread: () => ({ ...shop, tips: 4 }),
  get: (ctx) => ctx.get(shops, ["t0", "store-0"]),
  now: (ctx) => ctx.now(),
};

export default define({
  http: {
    micro: query("micro", (ctx, args: { op: string; n: number }) => {
      const op = ops[args.op];
      let last: unknown;
      for (let i = 0; i < args.n; i++) last = op(ctx);
      return typeof last === "object" ? null : last as any;
    }),
  },
});
