// Run after cargo build: node tests/e2e-access.mjs
// Collection access policies, enforced by the server on every method's reads and writes.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient, FlowerError } from "../sdk/index.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cluster = new LocalCluster({ nodes: 1, binary: process.env.E2E_FLOWER_BIN ?? join(root, "target/debug/flower") });
const admin = () => new FlowerAdmin(cluster.leader.url, { adminToken: cluster.adminToken });
const as = (credentials) => new FlowerClient(cluster.leader.url, credentials === undefined ? {} : { credentials });
let alice, bob, rootUser, anonymous;
let sequence = 0;
const mutate = (client, name, args) => client.mutate(name, args, { requestId: `access-${++sequence}` });
const value = async (promise) => (await promise).value;
async function denied(promise) {
  await assert.rejects(promise, (error) => error instanceof FlowerError && error.failure?.code === "ACCESS_DENIED");
}

// `read` is the knob the redeploy turns: owners and admins first, then any signed-in caller.
function source(read) {
  return `import { collection, define, derive, fail, mutation, query, v } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const notes = collection("notes", v.object({ owner: v.string(), rank: v.int(), text: v.string(), secret: v.optional(v.string()) }))
  .index("owner", ["owner"])
  .index("rank", ["rank"])
  .access(({ principal, row, next }) => {
    const admin = principal.claim("role").eq("admin");
    const mine = row("owner").eq(principal.subject);
    return {
      read: ${read},
      insert: next("owner").eq(principal.subject).or(admin),
      update: mine.and(next("owner").eq(row("owner"))).or(admin),
      delete: mine.or(admin),
      fields: { secret: { read: admin } },
    };
  });
const total = derive("total", (ctx) => ctx.scan(notes).length);
const id = v.string({ min: 1 });
const open = { access: "public" };
const get = query("get", { ...open, args: id }, (ctx, key) => ctx.get(notes, key));
const list = query("list", open, (ctx) => ctx.scan(notes).map((row) => row.key));
const mine = query("mine", open, (ctx) => ctx.query(notes.by("owner").eq(ctx.principal()?.subject ?? "")).map((note) => note.text));
const page = query("page", { ...open, args: v.nullable(v.string()) }, (ctx, after) =>
  ctx.range(notes.by("rank").range({ limit: 2, ...(after ? { after } : {}) })));
const count = query("count", open, (ctx) => ctx.get(total));
const put = mutation("put", { args: v.object({ id, note: v.json() }) }, (ctx, { id, note }) => { ctx.set(notes, id, note as any); return null; });
const edit = mutation("edit", { args: v.object({ id, text: v.string() }) }, (ctx, { id, text }) => {
  const note = ctx.get(notes, id);
  if (!note) fail("NOT_FOUND", "No such note");
  ctx.set(notes, id, { ...note, text });
  return null;
});
const remove = mutation("remove", { args: id }, (ctx, key) => { ctx.delete(notes, key); return null; });
export default define({
  collections: [notes],
  definitions: [total],
  auth: {
    authenticate: (_ctx, credentials) => typeof credentials === "string"
      ? { subject: credentials, claims: credentials === "root" ? { role: "admin" } : {} }
      : null,
  },
  http: { get, list, mine, page, count, put, edit, remove },
});`;
}

try {
  await cluster.start();
  [alice, bob, rootUser, anonymous] = [as("alice"), as("bob"), as("root"), as()];
  const fixture = join(cluster.directory, "notes.ts");
  await writeFile(fixture, source(`mine.or(admin)`));
  const bundle = await buildBundle(fixture, { initialization: "static" });
  await admin().deploy(bundle, { requestId: "access-deploy" });

  // Admins may write anything; everyone else only their own rows.
  const seed = [
    ["a1", { owner: "alice", rank: 1, text: "one", secret: "s1" }],
    ["b1", { owner: "bob", rank: 2, text: "two", secret: "s2" }],
    ["a2", { owner: "alice", rank: 3, text: "three" }],
    ["b2", { owner: "bob", rank: 4, text: "four" }],
  ];
  for (const [id, note] of seed) await mutate(rootUser, "put", { id, note });
  await mutate(alice, "put", { id: "a3", note: { owner: "alice", rank: 5, text: "five" } });

  // Reads: own rows only, the admin-only field left out; others' rows read as absent.
  assert.deepEqual(await value(alice.query("get", "a1")), { owner: "alice", rank: 1, text: "one" });
  assert.equal(await value(alice.query("get", "b1")), null);
  assert.deepEqual(await value(alice.query("list")), ["a1", "a2", "a3"]);
  assert.deepEqual(await value(bob.query("list")), ["b1", "b2"]);
  assert.deepEqual(await value(anonymous.query("list")), []);
  assert.deepEqual(await value(rootUser.query("list")), ["a1", "a2", "a3", "b1", "b2"]);
  assert.equal((await value(rootUser.query("get", "b1"))).secret, "s2");
  assert.deepEqual(await value(alice.query("mine")), ["one", "three", "five"]);
  // Pages fill with visible rows, and cursors continue past hidden ones.
  const first = await value(alice.query("page", null));
  assert.deepEqual(first.rows.map((row) => row.key), ["a1", "a2"]);
  const second = await value(alice.query("page", first.cursor));
  assert.deepEqual(second.rows.map((row) => row.key), ["a3"]);
  assert.equal(second.cursor, null);
  // Derived values run without a caller: they see every row.
  assert.equal(await value(alice.query("count")), 5);

  // Writes.
  await denied(mutate(alice, "put", { id: "b9", note: { owner: "bob", rank: 9, text: "forged" } }));
  await denied(mutate(alice, "put", { id: "b1", note: { owner: "alice", rank: 2, text: "taken" } }));
  await denied(mutate(alice, "put", { id: "a1", note: { owner: "alice", rank: 1, text: "one", secret: "mine now" } }));
  // A denied delete of a row alice can't see acts like deleting a missing key.
  await mutate(alice, "remove", "b1");
  assert.equal((await value(rootUser.query("get", "b1"))).owner, "bob");
  await assert.rejects(mutate(alice, "edit", { id: "b1", text: "hi" }), (error) => error.failure?.code === "NOT_FOUND");
  // Editing a redacted row keeps the field the caller couldn't see.
  await mutate(alice, "edit", { id: "a1", text: "edited" });
  assert.deepEqual(await value(rootUser.query("get", "a1")), { owner: "alice", rank: 1, text: "edited", secret: "s1" });
  await mutate(alice, "remove", "a2");
  assert.deepEqual(await value(rootUser.query("list")), ["a1", "a3", "b1", "b2"]);

  // A policy-only redeploy takes effect at once, cached results included.
  await writeFile(fixture, source(`principal.authenticated`));
  await admin().deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "access-redeploy" });
  assert.deepEqual(await value(alice.query("list")), ["a1", "a3", "b1", "b2"]);
  assert.deepEqual(await value(alice.query("get", "b1")), { owner: "bob", rank: 2, text: "two" });
  assert.deepEqual(await value(anonymous.query("list")), []);
  console.log("PASS: collection access policies hide rows and fields, fill pages, guard inserts, updates and deletes, keep redacted fields on edit, and follow policy redeploys");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await cluster.close();
}
