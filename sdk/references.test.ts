// Foreign keys: collection.references(target, on, { onDelete }). The server (and the testing module, as it)
// checks at the end of every mutation that the rows it wrote refer only to rows that exist and that no row
// it deleted is still referred to; cascade and setNull are the SDK's triggers on the target.
import assert from "node:assert/strict";
import test from "node:test";
import { collection, define, mutation, query, trigger, v } from "./index.ts";
import type { Collection, Json } from "./index.ts";
import { FlowerError } from "./client.ts";
import { testDatabase } from "./testing.ts";

type Op = ["set", string, Json, Json] | ["delete", string, Json];

/** An app over `collections` with one mutation applying writes in order and one query reading every row. */
function app(collections: Collection<any, any, any>[], parts: { triggers?: any[]; auth?: any } = {}) {
  const byName = new Map(collections.map((each) => [each.name, each]));
  const named = (name: string) => byName.get(name) ?? assert.fail(`no collection ${name}`);
  return define({
    collections,
    ...(parts.triggers ? { triggers: parts.triggers } : {}),
    ...(parts.auth ? { auth: parts.auth } : {}),
    http: {
      apply: mutation("apply", (ctx, ops: Op[]) => {
        for (const op of ops) {
          if (op[0] === "set") ctx.set(named(op[1]), op[2], op[3]);
          else ctx.delete(named(op[1]), op[2]);
        }
        return null;
      }),
      rows: query("rows", (ctx) => Object.fromEntries(collections.map((each) =>
        [each.name, ctx.scan(each).map((row: { key: Json; value: Json }) => [row.key, row.value])]))),
    },
  });
}

/** The failure a call ended with. */
function failure(run: () => unknown): { code: string; message: string; details?: any } {
  try { run(); }
  catch (error) {
    assert.ok(error instanceof FlowerError, String(error));
    assert.equal(error.code, "EVALUATION_FAILED");
    return error.failure as any;
  }
  return assert.fail("the call succeeded");
}

const orgs = collection<{ name: string }>("orgs");
const members = collection<{ org?: string | null; name: string }>("members").references(orgs, "org");

test("rows refer only to rows that exist, and rows still referred to can't go", async () => {
  const db = await testDatabase(app([orgs, members]));
  const missing = failure(() => db.mutate("apply", [["set", "members", "ann", { org: "acme", name: "Ann" }]]));
  assert.equal(missing.code, "FOREIGN_KEY_VIOLATION");
  assert.equal(missing.message, 'members row "ann" refers to orgs row "acme", which does not exist');
  assert.deepEqual(missing.details, { collection: "members", key: "ann", references: { fields: ["org"] }, target: "orgs", targetKey: "acme" });
  // Checked once the mutation is done: the order of its writes doesn't matter.
  db.mutate("apply", [["set", "members", "ann", { org: "acme", name: "Ann" }], ["set", "orgs", "acme", { name: "Acme" }]]);
  // A missing or null field refers to nothing.
  db.mutate("apply", [["set", "members", "bob", { name: "Bob" }], ["set", "members", "cy", { org: null, name: "Cy" }]]);
  const referred = failure(() => db.mutate("apply", [["delete", "orgs", "acme"]]));
  assert.equal(referred.code, "FOREIGN_KEY_VIOLATION");
  assert.equal(referred.message, 'orgs row "acme" is deleted while members row "ann" still refers to it');
  assert.deepEqual(referred.details, { collection: "members", key: "ann", references: { fields: ["org"] }, target: "orgs", targetKey: "acme", deleted: true });
  // Moving the row elsewhere is checked as writing it.
  assert.equal(failure(() => db.mutate("apply", [["set", "members", "ann", { org: "initech", name: "Ann" }]])).code, "FOREIGN_KEY_VIOLATION");
  // A row that refers as it did needs nothing, even if it changes otherwise.
  db.mutate("apply", [["set", "members", "ann", { org: "acme", name: "Ann B." }]]);
  // Deleted and written again in one mutation, the target is still there.
  db.mutate("apply", [["delete", "orgs", "acme"], ["set", "orgs", "acme", { name: "Acme 2" }]]);
  // Both at once go.
  db.mutate("apply", [["delete", "orgs", "acme"], ["delete", "members", "ann"]]);
  assert.deepEqual(db.query("rows"), { orgs: [], members: [["bob", { name: "Bob" }], ["cy", { org: null, name: "Cy" }]] });
});

test("a key refers by its leading components, and deleting cascades through triggers", async () => {
  // Sessions of a string-keyed collection, events keyed [session, n] refer to them by their first component.
  const sessions = collection<{ title: string }>("sessions");
  const events = collection<{ text: string }>("events").key(v.tuple([v.string(), v.int()])).references(sessions, { key: 1 }, { onDelete: "cascade" });
  const tally = collection<{ gone: number }>("tally");
  const count = trigger("count", events, (ctx, change) => {
    if (change.after === null) ctx.set(tally, "gone", { gone: (ctx.get(tally, "gone")?.gone ?? 0) + 1 });
  });
  const db = await testDatabase(app([sessions, events, tally], { triggers: [count] }));
  // Keys whose first component starts as another's does: `["a` against `["ab`, and one with a quote.
  db.mutate("apply", [
    ["set", "sessions", "a", { title: "A" }], ["set", "sessions", "ab", { title: "AB" }], ["set", "sessions", "a\"", { title: "Q" }],
    ["set", "events", ["a", 1], { text: "1" }], ["set", "events", ["a", 2], { text: "2" }],
    ["set", "events", ["ab", 1], { text: "x" }], ["set", "events", ["a\"", 1], { text: "q" }],
  ]);
  const missing = failure(() => db.mutate("apply", [["set", "events", ["zz", 1], { text: "?" }]]));
  assert.equal(missing.message, 'events row "[\\"zz\\",1]" refers to sessions row "zz", which does not exist');
  assert.deepEqual(missing.details.references, { key: 1 });
  db.mutate("apply", [["delete", "sessions", "a"]]);
  const rows = db.query("rows") as any;
  assert.deepEqual(rows.sessions.map(([key]: [string]) => key), ["a\"", "ab"]);
  assert.deepEqual(rows.events, [[["a\"", 1], { text: "q" }], [["ab", 1], { text: "x" }]]);
  // The events' own trigger saw both deletions.
  assert.deepEqual(rows.tally, [["gone", { gone: 2 }]]);
});

test("cascades go on through references of references, and setNull clears the fields", async () => {
  type Task = { title: string; team: string; assignee?: string | null };
  const teams = collection<{ org: string }>("teams").references(orgs, "org", { onDelete: "cascade" });
  const people = collection<{ name: string }>("people");
  const tasks = collection<Task>("tasks")
    .references(teams, "team", { onDelete: "cascade" })
    .references(people, "assignee", { onDelete: "setNull" });
  const db = await testDatabase(app([orgs, teams, people, tasks]));
  db.mutate("apply", [
    ["set", "orgs", "acme", { name: "Acme" }], ["set", "orgs", "initech", { name: "Initech" }],
    ["set", "teams", "red", { org: "acme" }], ["set", "teams", "blue", { org: "initech" }],
    ["set", "people", "ann", { name: "Ann" }], ["set", "people", "bob", { name: "Bob" }],
    ["set", "tasks", "t1", { title: "One", team: "red", assignee: "ann" }],
    ["set", "tasks", "t2", { title: "Two", team: "blue", assignee: "ann" }],
    ["set", "tasks", "t3", { title: "Three", team: "blue", assignee: "bob" }],
  ]);
  db.mutate("apply", [["delete", "people", "ann"]]);
  assert.deepEqual((db.query("rows") as any).tasks, [
    ["t1", { title: "One", team: "red", assignee: null }],
    ["t2", { title: "Two", team: "blue", assignee: null }],
    ["t3", { title: "Three", team: "blue", assignee: "bob" }],
  ]);
  db.mutate("apply", [["delete", "orgs", "initech"]]);
  const rows = db.query("rows") as any;
  assert.deepEqual(rows.teams, [["red", { org: "acme" }]]);
  assert.deepEqual(rows.tasks, [["t1", { title: "One", team: "red", assignee: null }]]);
});

test("a row can extend another by its whole key, and a collection can refer to itself", async () => {
  type Node = { parent: string | null };
  const users = collection<{ name: string }>("users");
  const profiles = collection<{ bio: string }>("profiles").references(users, { key: true }, { onDelete: "cascade" });
  // A reference to a collection declared later, or to itself, names it with a function.
  const nodes: Collection<Node> = collection<Node>("nodes").references(() => nodes, "parent", { onDelete: "cascade" });
  const db = await testDatabase(app([users, profiles, nodes]));
  assert.equal(failure(() => db.mutate("apply", [["set", "profiles", "ann", { bio: "?" }]])).message,
    'profiles row "ann" refers to users row "ann", which does not exist');
  db.mutate("apply", [["set", "users", "ann", { name: "Ann" }], ["set", "profiles", "ann", { bio: "hi" }], ["set", "users", "bob", { name: "Bob" }]]);
  db.mutate("apply", [["delete", "users", "ann"]]);
  assert.deepEqual((db.query("rows") as any).profiles, []);
  db.mutate("apply", [
    ["set", "nodes", "root", { parent: null }], ["set", "nodes", "a", { parent: "root" }], ["set", "nodes", "b", { parent: "a" }],
    ["set", "nodes", "c", { parent: "b" }], ["set", "nodes", "other", { parent: null }],
  ]);
  assert.equal(failure(() => db.mutate("apply", [["set", "nodes", "d", { parent: "nowhere" }]])).code, "FOREIGN_KEY_VIOLATION");
  db.mutate("apply", [["delete", "nodes", "root"]]);
  assert.deepEqual((db.query("rows") as any).nodes, [["other", { parent: null }]]);
});

test("several fields make a target's tuple key", async () => {
  const rooms = collection<{ seats: number }>("rooms").key(v.tuple([v.string(), v.int()]));
  const bookings = collection<{ building: string; room: number; who: string }>("bookings")
    .references(rooms, ["building", "room"], { onDelete: "cascade" });
  const desks = collection<{ at: [string, number] }>("desks").references(rooms, "at");
  const db = await testDatabase(app([rooms, bookings, desks]));
  assert.equal(failure(() => db.mutate("apply", [["set", "bookings", "b1", { building: "HQ", room: 2, who: "ann" }]])).message,
    'bookings row "b1" refers to rooms row "[\\"HQ\\",2]", which does not exist');
  db.mutate("apply", [
    ["set", "rooms", ["HQ", 2], { seats: 8 }], ["set", "rooms", ["HQ", 3], { seats: 4 }],
    ["set", "bookings", "b1", { building: "HQ", room: 2, who: "ann" }], ["set", "bookings", "b2", { building: "HQ", room: 3, who: "bob" }],
  ]);
  db.mutate("apply", [["delete", "rooms", ["HQ", 2]]]);
  assert.deepEqual((db.query("rows") as any).bookings, [["b2", { building: "HQ", room: 3, who: "bob" }]]);
  // One field may hold a whole tuple key: restricted, as found by the index's buckets.
  db.mutate("apply", [["set", "desks", "d1", { at: ["HQ", 3] }]]);
  assert.equal(failure(() => db.mutate("apply", [["set", "desks", "d2", { at: ["HQ", 9] }]])).code, "FOREIGN_KEY_VIOLATION");
  const referred = failure(() => db.mutate("apply", [["delete", "bookings", "b2"], ["delete", "rooms", ["HQ", 3]]]));
  assert.equal(referred.message, 'rooms row "[\\"HQ\\",3]" is deleted while desks row "d1" still refers to it');
});

test("cascades act with the application's rights on rows the caller can't see", async () => {
  const owned = collection<{ owner: string }>("owned").access(({ principal, row, next }) => ({
    read: row("owner").eq(principal.subject), insert: next("owner").eq(principal.subject), update: row("owner").eq(principal.subject),
    delete: row("owner").eq(principal.subject),
  }));
  const notes = collection<{ on: string }>("notes").references(owned, "on", { onDelete: "cascade" }).access({ read: false, insert: true });
  const auth = { authenticate: (_ctx: unknown, credentials: Json) => typeof credentials === "string" ? { subject: credentials } : null, default: "public" as const };
  const db = await testDatabase(app([owned, notes], { auth }));
  db.mutate("apply", [["set", "owned", "x", { owner: "ann" }], ["set", "notes", "n1", { on: "x" }]], { credentials: "ann" });
  // Ann can't see the note, so deleting it herself leaves it there.
  db.mutate("apply", [["delete", "notes", "n1"]], { credentials: "ann" });
  assert.ok(Object.hasOwn(db.data, 'source:["notes","n1"]'));
  db.mutate("apply", [["delete", "owned", "x"]], { credentials: "ann" });
  assert.deepEqual(Object.keys(db.data).filter((id) => id.startsWith("source:")), []);
});

test("the manifest carries references, with the index that finds them", async () => {
  const rooms = collection<{ seats: number }>("rooms").key(v.tuple([v.string(), v.int()]));
  const bookings = collection<{ building: string; room: number }>("bookings").index("byRoom", ["room"])
    .references(rooms, ["building", "room"], { onDelete: "cascade" });
  const events = collection<{ text: string }>("events").key(v.tuple([v.string(), v.int()])).references(orgs, { key: 1 });
  const module = app([orgs, members, rooms, bookings, events, members]);
  assert.deepEqual(JSON.parse(JSON.stringify(module.collections)), [
    { name: "bookings", indexes: { byRoom: ["room"] }, references: [{ target: "rooms", fields: ["building", "room"], json: true }] },
    { name: "events", indexes: {}, references: [{ target: "orgs", key: 1 }] },
    { name: "members", indexes: {}, references: [{ target: "orgs", fields: ["org"] }] },
    { name: "orgs", indexes: {} },
    { name: "rooms", indexes: {} },
  ]);
  // A declaration without references doesn't conflict with one that has them.
  assert.deepEqual(app([collection("members"), members]).collections?.find((each) => each.name === "members")?.references, [{ target: "orgs", fields: ["org"] }]);
  assert.throws(() => app([members, collection("members").references(orgs, "boss")]), /declared with different references/);
  assert.throws(() => app([members, collection("members").references(orgs, "org", { onDelete: "cascade" })]), /declared with different references/);
});

test("references are checked as they are declared and defined", async () => {
  const keyed = collection("keyed").key(v.tuple([v.string(), v.string()]));
  const plain = collection("plain");
  assert.throws(() => plain.references({} as never, "x"), /requires a collection/);
  assert.throws(() => plain.references(orgs, [] as never), /distinct nonempty strings/);
  assert.throws(() => plain.references(orgs, ["a", "a"] as never), /distinct nonempty strings/);
  assert.throws(() => plain.references(orgs, { key: 0 }), /1 to 64/);
  assert.throws(() => plain.references(orgs, { key: 65 }), /1 to 64/);
  assert.throws(() => keyed.references(orgs, { key: 1 }, { onDelete: "setNull" }), /can't be set null/);
  assert.throws(() => plain.references(orgs, "x", { onDelete: "nothing" as never }), /onDelete must be/);
  assert.throws(() => app([plain.references(orgs, { key: 1 })]), /need tuple keys/);
  assert.throws(() => app([keyed.references(orgs, { key: 2 })]), /several key components/);
  assert.throws(() => app([plain.references(orgs, ["a", "b"] as never)]), /several fields/);
  assert.throws(() => app([keyed.references(orgs, { key: true })]), /keyed as the target is/);
  assert.throws(() => app([plain.references(() => ({}) as never, "x")]), /must be a collection/);
  // A collection declaring references the server never received would enforce nothing.
  const db = await testDatabase(define({
    collections: [orgs],
    http: { add: mutation("add", (ctx) => { ctx.set(members, "ann", { org: "none", name: "Ann" }); return null; }) },
  }));
  assert.throws(() => db.mutate("add"), (error: any) => /declares references; list it in define\(\{ collections \}\)/.test(error.failure?.message ?? error.message));
});

test("finding rows by a field that holds a whole tuple key fails rather than miss them", async () => {
  const rooms = collection<{ seats: number }>("rooms").key(v.tuple([v.string(), v.int()]));
  const desks = collection<{ at: [string, number] }>("desks").references(rooms, "at", { onDelete: "cascade" });
  const db = await testDatabase(app([rooms, desks]));
  db.mutate("apply", [["set", "rooms", ["HQ", 3], { seats: 4 }], ["set", "desks", "d1", { at: ["HQ", 3] }]]);
  const failed = failure(() => db.mutate("apply", [["delete", "rooms", ["HQ", 3]]]));
  assert.equal(failed.code, "INVALID_REFERENCE");
  assert.match(failed.message, /only strings, numbers and booleans are found by their fields/);
});
