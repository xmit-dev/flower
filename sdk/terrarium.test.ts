import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import type app from "../docs/terrarium.ts";
import { FlowerError, type Update } from "./client.ts";
import { testDatabase, type TestDatabase } from "./testing.ts";

// The downloadable app bundles against current SDK source (esbuild follows the
// tsconfig paths for @flower-js/sdk, never dist/) and runs isolated like the server.
const entry = fileURLToPath(new URL("../docs/terrarium.ts", import.meta.url));
const terrarium = () => testDatabase<typeof app>(entry, { now: 1_000 });
const seedlings = { spacesLeft: 11, blooming: 0, flowers: { luna: "🌱" } };
const blooming = { spacesLeft: 11, blooming: 1, flowers: { luna: "🌼" } };
const empty = { spacesLeft: 12, blooming: 0, flowers: {} };

function fails(action: () => unknown, code: string): FlowerError {
  let caught: unknown;
  assert.throws(action, (error) => { caught = error; return true; });
  assert.ok(caught instanceof FlowerError, String(caught));
  assert.equal(caught.failure?.code, code, caught.message);
  return caught;
}

async function next<T>(updates: AsyncGenerator<Update<T>>): Promise<T> {
  const result = await updates.next();
  assert.equal(result.done, false);
  return (result.value as Update<T>).value;
}

test("flowers bloom 5 s and perish 35 s after planting, each garden on its own schedule", async () => {
  const db = await terrarium();
  db.mutate("garden.plant", { garden: "moon", id: "luna" });
  assert.equal(db.advance(1_000), 0);
  db.mutate("garden.plant", { garden: "sun", id: "luna" });
  assert.deepEqual(db.query("garden.view", "moon"), seedlings);
  assert.deepEqual(db.query("garden.view", "sun"), seedlings);

  assert.equal(db.advance(3_999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("garden.view", "moon"), blooming);
  assert.deepEqual(db.query("garden.view", "sun"), seedlings);
  assert.equal(db.advance(999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("garden.view", "sun"), blooming);

  assert.equal(db.advance(28_999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("garden.view", "moon"), empty);
  assert.deepEqual(db.query("garden.view", "sun"), blooming);
  assert.equal(db.advance(1_000), 1);
  assert.deepEqual(db.query("garden.view", "sun"), empty);
  assert.equal(db.maintain(), 0, "each timer left with the change it made");
});

test("a garden view streams every season as time advances", async () => {
  const db = await terrarium();
  const view = db.client.subscribe("garden.view", "moon");
  try {
    assert.deepEqual(await next(view), empty);
    db.mutate("garden.plant", { garden: "moon", id: "luna" });
    assert.deepEqual(await next(view), seedlings);
    db.advance(5_000);
    assert.deepEqual(await next(view), blooming);
    db.advance(30_000);
    assert.deepEqual(await next(view), empty);
  } finally { await view.return(undefined); }
});

test("a full garden and a taken spot reject planting without side effects, and perishing frees space", async () => {
  const db = await terrarium();
  for (let index = 0; index < 12; index++) db.mutate("garden.plant", { garden: "moon", id: `seed-${index}` });
  assert.equal(db.query("garden.view", "moon").spacesLeft, 0);
  const full = db.data;
  assert.equal(fails(() => db.mutate("garden.plant", { garden: "moon", id: "one-too-many" }), "GARDEN_FULL").failure?.message,
    "Garden full! Wait for a flower to make room.");
  assert.equal(fails(() => db.mutate("garden.plant", { garden: "moon", id: "seed-3" }), "SPOT_TAKEN").failure?.message,
    "That spot is already planted.");
  assert.deepEqual(db.data, full, "rejected seeds create neither timers nor records");
  db.mutate("garden.plant", { garden: "sun", id: "luna" });
  assert.equal(db.query("garden.view", "sun").spacesLeft, 11);

  assert.equal(db.advance(34_999), 13, "every overdue bloom runs, no flower perishes yet");
  assert.deepEqual([db.query("garden.view", "moon").spacesLeft, db.query("garden.view", "moon").blooming], [0, 12]);
  db.now += 1;
  assert.equal(db.maintain(1), 1);
  assert.equal(db.query("garden.view", "moon").spacesLeft, 1);
  db.mutate("garden.plant", { garden: "moon", id: "next-generation" });
  assert.equal(db.query("garden.view", "moon").spacesLeft, 0);
  assert.equal(db.advance(0), 12);
  assert.deepEqual(db.query("garden.view", "moon"), { spacesLeft: 11, blooming: 0, flowers: { "next-generation": "🌱" } });
  assert.deepEqual(db.query("garden.view", "sun"), empty);
});

test("plant validates its arguments and composite keys never collide across gardens", async () => {
  const db = await terrarium();
  db.mutate("garden.plant", { garden: "moon/fern", id: "luna" });
  db.mutate("garden.plant", { garden: "moon", id: "fern/luna" });
  assert.deepEqual(db.query("garden.view", "moon/fern"), seedlings);
  assert.deepEqual(db.query("garden.view", "moon"), { spacesLeft: 11, blooming: 0, flowers: { "fern/luna": "🌱" } });
  const before = db.data;
  for (const args of [null, {}, { garden: "moon", id: "" }, { garden: 42, id: "luna" }, { garden: "moon", id: "x".repeat(65) }, { garden: "moon", id: "a", color: "red" }]) {
    fails(() => db.mutate("garden.plant", args as never), "INVALID_ARGUMENT");
  }
  fails(() => db.query("garden.view", ""), "INVALID_ARGUMENT");
  assert.deepEqual(db.data, before);
});

test("lifecycle callbacks stay private, and a replanted flower starts a fresh lifecycle", async () => {
  const db = await terrarium();
  for (const alias of ["internal.bloom", "internal.perish", "plant", "view"]) {
    assert.throws(() => (db as unknown as TestDatabase).call(alias), { status: 404, code: "METHOD_NOT_FOUND" });
  }
  assert.throws(() => (db as unknown as TestDatabase).mutate("garden.view", "moon"), { status: 422, code: "METHOD_KIND_MISMATCH" });
  db.mutate("garden.plant", { garden: "moon", id: "luna" });
  assert.equal(db.advance(35_000), 2);
  assert.deepEqual(db.query("garden.view", "moon"), empty);
  db.mutate("garden.plant", { garden: "moon", id: "luna" });
  assert.equal(db.advance(4_999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("garden.view", "moon"), blooming);
  assert.equal(db.advance(29_999), 0);
  assert.equal(db.advance(1), 1);
  assert.deepEqual(db.query("garden.view", "moon"), empty);
});

test("homepage snippets are the complete downloadable terrarium and client", () => {
  const html = readFileSync(new URL("../docs/index.html", import.meta.url), "utf8");
  const snippets = [...html.matchAll(/<code class="language-ts">([\s\S]*?)<\/code>/g)].map(([, code]) =>
    code.replace(/<\/?span\b[^>]*>/g, "").replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&quot;", '"').replaceAll("&amp;", "&"));
  assert.equal(snippets.length, 3);
  for (const [file, source] of [["terrarium.ts", snippets.slice(0, 2).join("\n\n")], ["terrarium-client.ts", snippets[2]]]) {
    assert.equal(source, readFileSync(new URL(`../docs/${file}`, import.meta.url), "utf8").trimEnd());
    // The two files download together, so the client may import the app's type.
    for (const match of source.matchAll(/\bfrom\s+["']([^"']+)["']/g)) assert.match(match[1], /^(?:@flower-js\/sdk(?:\/scheduler)?|\.\/terrarium\.ts)$/);
  }
});
