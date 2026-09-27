import { define, mutation, query, v } from "../sdk/index.ts";
import type { Json } from "../sdk/index.ts";
import { expiringCollection, queue } from "../sdk/temporal.ts";
import type { Expiration } from "../sdk/temporal.ts";

// Jobs retry with backoff up to five attempts; workers renew short leases while
// they run. Expired cache entries disappear from reads immediately and from
// storage when maintenance reaches them.
export const jobs = queue<Json, Json>("workerJobs", { lease: { defaultMs: 10_000, maxMs: 30_000 }, retry: { maxAttempts: 5 } });
export const cache = expiringCollection<Json>("workerCache", { expiration: { afterUpdateMs: 60_000 } });

const expiration = v.nullable(v.union(
  v.object({ at: v.int({ min: 0 }) }), v.object({ afterCreationMs: v.int({ min: 0 }) }), v.object({ afterUpdateMs: v.int({ min: 0 }) }),
));
export const setCache = mutation("internal.cache.set", {
  args: v.object({ key: v.string({ min: 1 }), value: v.json(), expiration: v.optional(expiration) }),
}, (ctx, args) => cache.set(ctx, args.key, args.value, args.expiration as Expiration | undefined));
export const getCache = query("internal.cache.get", { args: v.string({ min: 1 }) }, (ctx, key) => cache.get(ctx, key));
export const entryCache = query("internal.cache.entry", { args: v.string({ min: 1 }) }, (ctx, key) => cache.entry(ctx, key));

const app = define({
  uses: [jobs, cache],
  http: {
    ...jobs.http("jobs", { methods: ["enqueue", "claim", "renew", "complete", "fail", "release", "retry", "get", "ready", "stats"] }),
    "cache.set": setCache,
    "cache.get": getCache,
    "cache.entry": entryCache,
  },
});
export default app;
