import assert from "node:assert/strict";
import test from "node:test";
import { collection, define, derive, fail, FlowerError, mutation, query, v } from "./index.ts";
import { testDatabase } from "./testing.ts";

const noteSchema = v.object({ owner: v.string(), rank: v.int(), text: v.string(), secret: v.optional(v.string()) });
const notes = collection("notes", noteSchema)
  .index("owner", ["owner"])
  .index("rank", ["rank"])
  .access(({ principal, row, next }) => {
    const admin = principal.claim("role").eq("admin");
    const mine = row("owner").eq(principal.subject);
    return {
      read: mine.or(admin),
      insert: next("owner").eq(principal.subject).or(admin),
      update: mine.and(next("owner").eq(row("owner"))).or(admin),
      delete: mine.or(admin),
      fields: { secret: { read: admin } },
    };
  });

function notesApp() {
  const total = derive("total", (ctx) => ctx.scan(notes).length);
  const id = v.string({ min: 1 });
  const open = { access: "public" } as const;
  return define({
    collections: [notes],
    definitions: [total],
    auth: {
      authenticate: (_ctx, credentials) => typeof credentials === "string"
        ? { subject: credentials, claims: { role: credentials === "root" ? "admin" : "user" } }
        : null,
    },
    http: {
      get: query("get", { ...open, args: id }, (ctx, key) => ctx.get(notes, key)),
      list: query("list", open, (ctx) => ctx.scan(notes).map((row) => row.key)),
      top: query("top", open, (ctx) => ctx.scan(notes, { index: "rank", offset: 1, limit: 1 }).map((row) => row.key)),
      mine: query("mine", open, (ctx) => ctx.query(notes.by("owner").eq(ctx.principal()?.subject ?? "")).map((note) => note.text)),
      page: query("page", { ...open, args: v.nullable(v.string()) }, (ctx, after) =>
        ctx.range(notes.by("rank").range({ limit: 2, ...(after ? { after } : {}) }))),
      count: query("count", open, (ctx) => ctx.get(total)),
      put: mutation("put", { args: v.object({ id, note: noteSchema }) }, (ctx, { id, note }) => { ctx.set(notes, id, note); return null; }),
      edit: mutation("edit", { args: v.object({ id, text: v.string() }) }, (ctx, { id, text }) => {
        const note = ctx.get(notes, id);
        if (!note) fail("NOT_FOUND", "No such note");
        ctx.set(notes, id, { ...note, text });
        return null;
      }),
      remove: mutation("remove", { args: id }, (ctx, key) => { ctx.delete(notes, key); return null; }),
    },
  });
}

test("access compiles into the collection's manifest entry", () => {
  const app = notesApp();
  const entry = app.collections!.find((each) => each.name === "notes")!;
  const owner = { eq: [{ ref: ["row", "owner"] }, { ref: ["principal", "subject"] }] };
  const admin = { eq: [{ ref: ["principal", "claims", "role"] }, { value: "admin" }] };
  assert.deepEqual(entry.access, {
    read: { any: [owner, admin] },
    insert: { any: [{ eq: [{ ref: ["next", "owner"] }, { ref: ["principal", "subject"] }] }, admin] },
    update: { any: [{ all: [owner, { eq: [{ ref: ["next", "owner"] }, { ref: ["row", "owner"] }] }] }, admin] },
    delete: { any: [owner, admin] },
    fields: { secret: { read: admin } },
  });
  // write is the default for insert, update and delete; anything unspecified is denied.
  const shared = collection<{ tags: string[] }>("shared").access(({ principal, row, key, not }) => ({
    read: row("tags").has(principal.subject).or(key.in(["public"])),
    write: not(principal.authenticated).not(),
  }));
  const [compiled] = define({ collections: [shared], http: {} }).collections!;
  assert.deepEqual(compiled.access, {
    read: { any: [{ in: [{ ref: ["principal", "subject"] }, { ref: ["row", "tags"] }] }, { in: [{ ref: ["key"] }, { value: ["public"] }] }] },
    insert: { not: { not: { exists: { ref: ["principal", "subject"] } } } },
    update: { not: { not: { exists: { ref: ["principal", "subject"] } } } },
    delete: { not: { not: { exists: { ref: ["principal", "subject"] } } } },
  });
  assert.deepEqual(define({ collections: [collection("closed").access({ read: true })], http: {} }).collections![0].access,
    { read: { const: true }, insert: { const: false }, update: { const: false }, delete: { const: false } });
});

test("access declarations fail at definition time when they can't work", () => {
  const plain = collection<{ owner: string }>("plain");
  assert.throws(() => plain.access(({ next }) => ({ read: next("owner").exists() })), /read can't use next.owner: next only exists while writing/);
  assert.throws(() => plain.access(({ row }) => ({ insert: row("owner").exists() })), /insert can't use row.owner/);
  assert.throws(() => plain.access(({ row }) => ({ write: row("owner").exists() })), /insert can't use row.owner/);
  assert.throws(() => plain.access({ reads: true } as never), /does not accept "reads"/);
  assert.throws(() => plain.access({ read: "yes" } as never), /must be a rule, true or false/);
  assert.throws(() => plain.access({ fields: { owner: {} } }), /needs read or write/);
  assert.throws(() => plain.access({ read: true }).access({ read: true }), /already declared/);
  assert.throws(() => plain.access(({ row }) => ({ read: row("owner").eq(row("owner").exists() as never) })), /Compare values, not rules/);
  // @ts-expect-error: fields are typed by the collection's records.
  plain.access(({ row }) => ({ read: row("missing").exists() }));
  // @ts-expect-error: comparisons are typed too.
  plain.access(({ row }) => ({ read: row("owner").eq(1) }));
  // Two different policies for one collection conflict; a reference without one doesn't.
  const a = plain.access({ read: true });
  const b = plain.access({ read: false });
  assert.throws(() => define({ collections: [a, b], http: {} }), /different access/);
  assert.equal(define({ collections: [plain, a], http: {} }).collections![0].access?.read !== undefined, true);
  assert.equal(define({ collections: [a, plain], http: {} }).collections![0].access?.read !== undefined, true);
});

test("declaring access keeps collections covariant, so spread writes still infer from the collection", () => {
  // A strict callback over row fields made Collection<T> invariant: ctx.set then inferred T from the
  // spread value (state widened to string) and rejected apps' plain collections. This must typecheck.
  type Machine = { id: string; kind: "docker" | "vm"; state: "running" | "stopping"; note: string | null };
  const machines = collection<Machine>("machines").index("byKind", ["kind", "state"]);
  const guarded = machines.access(({ row, principal }) => ({ read: row("id").eq(principal.subject), write: false }));
  const stop = mutation("stop", { args: v.string() }, (ctx, id) => {
    const machine = ctx.get(machines, id);
    if (machine === null || machine.kind !== "docker") return null;
    ctx.set(machines, id, { ...machine, state: "stopping" });
    ctx.set(guarded, id, { ...machine, state: "stopping", note: null });
    return null;
  });
  const takesMachines = (collection: typeof machines) => collection.name;
  assert.equal(takesMachines(guarded), "machines");
  assert.ok(stop);
});

test("the test database enforces access like the server", async () => {
  const db = await testDatabase(notesApp());
  const as = (credentials?: string) => credentials === undefined ? {} : { credentials };
  const seed: [string, { owner: string; rank: number; text: string; secret?: string }][] = [
    ["a1", { owner: "alice", rank: 1, text: "one", secret: "s1" }],
    ["b1", { owner: "bob", rank: 2, text: "two", secret: "s2" }],
    ["a2", { owner: "alice", rank: 3, text: "three" }],
    ["b2", { owner: "bob", rank: 4, text: "four" }],
  ];
  for (const [id, note] of seed) db.mutate("put", { id, note }, as("root"));
  db.mutate("put", { id: "a3", note: { owner: "alice", rank: 5, text: "five" } }, as("alice"));

  assert.deepEqual(db.query("get", "a1", as("alice")), { owner: "alice", rank: 1, text: "one" });
  assert.equal(db.query("get", "b1", as("alice")), null);
  assert.deepEqual(db.query("list", null, as("alice")), ["a1", "a2", "a3"]);
  assert.deepEqual(db.query("list", null, as("bob")), ["b1", "b2"]);
  assert.deepEqual(db.query("list", null, as()), []);
  assert.deepEqual(db.query("list", null, as("root")), ["a1", "a2", "a3", "b1", "b2"]);
  assert.deepEqual(db.query("top", null, as("alice")), ["a2"]);
  assert.deepEqual(db.query("mine", null, as("alice")), ["one", "three", "five"]);
  const first = db.query("page", null, as("alice")) as { rows: { key: string }[]; cursor: string | null };
  assert.deepEqual(first.rows.map((row) => row.key), ["a1", "a2"]);
  const second = db.query("page", first.cursor, as("alice")) as { rows: { key: string }[]; cursor: string | null };
  assert.deepEqual(second.rows.map((row) => row.key), ["a3"]);
  assert.equal(second.cursor, null);
  assert.equal(db.query("count", null, as("alice")), 5);

  const denied = (run: () => unknown) => assert.throws(run, (error: any) => error instanceof FlowerError && error.failure?.code === "ACCESS_DENIED");
  denied(() => db.mutate("put", { id: "b9", note: { owner: "bob", rank: 9, text: "forged" } }, as("alice")));
  denied(() => db.mutate("put", { id: "b1", note: { owner: "alice", rank: 2, text: "taken" } }, as("alice")));
  denied(() => db.mutate("put", { id: "a1", note: { owner: "alice", rank: 1, text: "one", secret: "mine now" } }, as("alice")));
  denied(() => db.mutate("remove", "b1", as("alice")));
  db.mutate("edit", { id: "a1", text: "edited" }, as("alice"));
  assert.deepEqual(db.query("get", "a1", as("root")), { owner: "alice", rank: 1, text: "edited", secret: "s1" });
  db.mutate("remove", "a2", as("alice"));
  assert.deepEqual(db.query("list", null, as("root")), ["a1", "a3", "b1", "b2"]);
});

test("a collection with access must be declared, and callers' views never reach the auth hook", async () => {
  const sessions = collection<{ subject: string }>("sessions").access({ read: false, insert: true });
  const orphan = collection<{ owner: string }>("orphan").access({ read: true });
  const app = define({
    collections: [sessions],
    auth: {
      // The hook runs without a caller, so it reads sessions nobody else can.
      authenticate: (ctx, credentials) => typeof credentials === "string" ? { subject: ctx.get(sessions, credentials)?.subject ?? fail("UNAUTHENTICATED", "Who?") } : null,
    },
    http: {
      login: mutation("login", { access: "public", args: v.string() }, (ctx, token) => { ctx.set(sessions, token, { subject: "alice" }); return null; }),
      whoami: query("whoami", (ctx) => ctx.principal()?.subject ?? null),
      peek: query("peek", (ctx) => ctx.scan(sessions).length),
      stray: query("stray", { access: "public" }, (ctx) => ctx.get(orphan, "x")),
    },
  });
  const db = await testDatabase(app);
  db.mutate("login", "t1");
  assert.equal(db.query("whoami", null, { credentials: "t1" }), "alice");
  assert.equal(db.query("peek", null, { credentials: "t1" }), 0);
  assert.throws(() => db.query("whoami", null, { credentials: "t2" }), (error: any) => error.status === 403);
  assert.throws(() => db.mutate("login", "t1"), (error: any) => error.failure?.code === "ACCESS_DENIED");
  assert.throws(() => db.query("stray"), /declares access; list it in define/);
});
