import { canonicalJson, collection, define, derive, fail, mutation, query, v } from "@flower-js/sdk";
import { scheduler } from "@flower-js/sdk/scheduler";

const GARDEN_SIZE = 12;
const name = v.string({ min: 1, max: 64 });
const flowers = collection<{ garden: string; id: string; plantedAt: number; bloomed: boolean }>("flowers")
  .key(v.tuple([name, name]))
  .index("garden", ["garden"]);

const garden = derive("garden", (ctx, id: string) => {
  const rows = ctx.query(flowers.by("garden").eq(id));
  return {
    spacesLeft: GARDEN_SIZE - rows.length,
    blooming: rows.filter((flower) => flower.bloomed).length,
    flowers: Object.fromEntries(rows.map((flower) => [flower.id, flower.bloomed ? "🌼" : "🌱"])),
  };
});
const view = query("view", { args: name, consistency: "replica-local" }, (ctx, id) => ctx.get(garden, id));

const season = v.object({ key: v.tuple([name, name]), plantedAt: v.int() });
const bloom = mutation("internal.bloom", { args: season }, (ctx, event) => {
  const flower = ctx.get(flowers, event.key);
  if (flower?.plantedAt === event.plantedAt) ctx.set(flowers, event.key, { ...flower, bloomed: true });
  return null;
});
const perish = mutation("internal.perish", { args: season }, (ctx, event) => {
  if (ctx.get(flowers, event.key)?.plantedAt === event.plantedAt) ctx.delete(flowers, event.key);
  return null;
});
const seasons = scheduler("seasons", { bloom, perish });

const plant = mutation("plant", { args: v.object({ garden: name, id: name }) }, (ctx, seed) => {
  const key: [string, string] = [seed.garden, seed.id];
  if (ctx.get(flowers, key)) fail("SPOT_TAKEN", "That spot is already planted.");
  if (ctx.query(flowers.by("garden").eq(seed.garden)).length >= GARDEN_SIZE) {
    fail("GARDEN_FULL", "Garden full! Wait for a flower to make room.");
  }
  const plantedAt = ctx.now();
  ctx.set(flowers, key, { ...seed, plantedAt, bloomed: false });
  const event = { key, plantedAt }; // An old timer cannot affect a replacement.
  seasons.at(ctx, `bloom:${canonicalJson(key)}`, plantedAt + 5_000, "bloom", event);
  seasons.at(ctx, `perish:${canonicalJson(key)}`, plantedAt + 35_000, "perish", event);
  ctx.materialize(garden, seed.garden);
  return { planted: seed.id };
});

const app = define({
  uses: [seasons],
  collections: [flowers],
  definitions: [garden],
  http: { "garden.plant": plant, "garden.view": view },
});
export default app;
