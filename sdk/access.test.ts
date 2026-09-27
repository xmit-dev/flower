import assert from "node:assert/strict";
import test from "node:test";
import { collection, define, derive, fail, FlowerError, mutation, query, trigger, v } from "./index.ts";
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
  // Deleting a row you can't see is a no-op, like deleting a missing key.
  db.mutate("remove", "b1", as("alice"));
  assert.deepEqual(db.query("get", "b1", as("root")), { owner: "bob", rank: 2, text: "two", secret: "s2" });
  db.mutate("edit", { id: "a1", text: "edited" }, as("alice"));
  assert.deepEqual(db.query("get", "a1", as("root")), { owner: "alice", rank: 1, text: "edited", secret: "s1" });
  db.mutate("remove", "a2", as("alice"));
  assert.deepEqual(db.query("list", null, as("root")), ["a1", "a3", "b1", "b2"]);
});

test("rules order numbers and strings by code point, match prefixes, read key parts and follow the clock", async () => {
  type Item = { rank: number; text: string; expiresAt?: number };
  const items = collection<Item>("items").key(v.tuple([v.string(), v.int()])).access(({ principal, row, next, key, now }) => ({
    read: key.at(0).eq(principal.subject).and(row("rank").gte(2), row("text").gt("\uFFFD").or(row("text").startsWith("b")), row("expiresAt").gt(now)),
    insert: key.at(0).eq(principal.subject).and(next("rank").lt(100).or(next("text").startsWith("b"))),
  }));
  const app = define({
    collections: [items],
    auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials } : null },
    http: {
      list: query("list", { access: "public" }, (ctx) => ctx.scan(items).map((row) => row.key)),
      put: mutation("put", { args: v.object({ key: v.tuple([v.string(), v.int()]), item: v.json() }) }, (ctx, { key, item }) => { ctx.set(items, key, item as Item); return null; }),
    },
  });
  const [entry] = app.collections!;
  assert.deepEqual(entry.access!.insert, {
    all: [
      { eq: [{ ref: ["key", "0"] }, { ref: ["principal", "subject"] }] },
      { any: [{ lt: [{ ref: ["next", "rank"] }, { value: 100 }] }, { startsWith: [{ ref: ["next", "text"] }, { value: "b" }] }] },
    ],
  });
  assert.deepEqual((entry.access!.read as any).all[3], { gt: [{ ref: ["row", "expiresAt"] }, { ref: ["now"] }] });
  const db = await testDatabase(app, { now: 1_000 });
  const seed: [[string, number], Item][] = [
    [["alice", 1], { rank: 1, text: "\u{1F600}", expiresAt: 5_000 }], // rank too low
    [["alice", 2], { rank: 2, text: "\u{1F600}", expiresAt: 5_000 }], // an emoji sorts after U+FFFD by code point
    [["alice", 3], { rank: 3, text: "\uFFFD", expiresAt: 5_000 }], // not greater than itself
    [["alice", 4], { rank: 4, text: "bee", expiresAt: 2_000 }], // a prefix match, until it expires
    [["alice", 5], { rank: "5" as never, text: "bee", expiresAt: 5_000 }], // a string never orders against a number
    [["bob", 1], { rank: 9, text: "bee", expiresAt: 5_000 }], // someone else's key
  ];
  for (const [key, item] of seed) db.mutate("put", { key, item }, { credentials: key[0] });
  assert.deepEqual(db.query("list", null, { credentials: "alice" }), [["alice", 2], ["alice", 4]]);
  db.advance(1_000);
  assert.deepEqual(db.query("list", null, { credentials: "alice" }), [["alice", 2]]);
  assert.throws(() => db.mutate("put", { key: ["bob", 2], item: { rank: 1, text: "x" } }, { credentials: "alice" }), (error: any) => error.failure?.code === "ACCESS_DENIED");
  assert.throws(() => db.mutate("put", { key: ["alice", 9], item: { rank: 100, text: "x" } }, { credentials: "alice" }), (error: any) => error.failure?.code === "ACCESS_DENIED");
  // Key parts need JSON keys: a plain string key that parses as JSON must not have parts.
  const plain = collection<Item>("plain").access(({ key, principal }) => ({ read: key.at(0).eq(principal.subject) }));
  assert.throws(() => define({ collections: [plain], http: {} }), /read key parts; declare its keys with collection.key/);
  assert.throws(() => collection<Item>("bad").access(({ key }) => ({ read: key.at("").exists() })), /nonempty/);
});

test("clear removes hidden fields where their write rules allow, and keeps the rest", async () => {
  type Account = { owner: string; name: string; token?: string; plan?: string };
  const accounts = collection<Account>("accounts").access(({ principal, row, next }) => {
    const admin = principal.claim("role").eq("admin");
    const mine = row("owner").eq(principal.subject);
    return {
      read: mine.or(admin),
      insert: next("owner").eq(principal.subject).or(admin),
      update: mine.or(admin),
      fields: {
        // Write-only for its owner: admins read it, owners set or clear it.
        token: { read: admin, write: mine.or(next("owner").eq(principal.subject)).or(admin) },
        // Admin-only both ways.
        plan: { read: admin },
      },
    };
  });
  const app = define({
    collections: [accounts],
    auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials, claims: { role: credentials === "root" ? "admin" : "user" } } : null },
    http: {
      get: query("get", { access: "public", args: v.string() }, (ctx, id) => ctx.get(accounts, id)),
      put: mutation("put", { args: v.object({ id: v.string(), account: v.json(), clear: v.optional(v.json()) }) }, (ctx, { id, account, clear }) => {
        ctx.set(accounts, id, account as Account, clear === undefined ? undefined : { clear } as never);
        return null;
      }),
    },
  });
  const db = await testDatabase(app);
  const as = (credentials: string) => ({ credentials });
  db.mutate("put", { id: "a", account: { owner: "alice", name: "A", token: "t1", plan: "pro" } }, as("root"));
  assert.deepEqual(db.query("get", "a", as("alice")), { owner: "alice", name: "A" });
  // Without clear, hidden fields survive a read-modify-write.
  db.mutate("put", { id: "a", account: { owner: "alice", name: "B" } }, as("alice"));
  assert.deepEqual(db.query("get", "a", as("root")), { owner: "alice", name: "B", token: "t1", plan: "pro" });
  // The owner clears the token; the plan it can't write stays.
  db.mutate("put", { id: "a", account: { owner: "alice", name: "B" }, clear: ["token"] }, as("alice"));
  assert.deepEqual(db.query("get", "a", as("root")), { owner: "alice", name: "B", plan: "pro" });
  assert.throws(() => db.mutate("put", { id: "a", account: { owner: "alice", name: "B" }, clear: ["plan"] }, as("alice")), (error: any) => error.failure?.code === "ACCESS_DENIED");
  assert.throws(() => db.mutate("put", { id: "a", account: { owner: "alice", name: "B" }, clear: "plan" }, as("alice")), /clear must list/);
  // clear is typed by the collection's fields.
  mutation("typed", (ctx) => {
    // @ts-expect-error: not a field of Account.
    ctx.set(accounts, "a", { owner: "alice", name: "A" }, { clear: ["tokn"] });
    return null;
  });
});

test("triggers act with the application's rights: full before and after values, and writes callers can't make", async () => {
  type Doc = { owner: string; text: string; secret?: string };
  const docs = collection<Doc>("docs").access(({ principal, row, next }) => {
    const admin = principal.claim("role").eq("admin");
    const mine = row("owner").eq(principal.subject);
    return { read: mine.or(admin), insert: next("owner").eq(principal.subject).or(admin), update: mine.or(admin), fields: { secret: { read: admin } } };
  });
  type Entry = { by: string | null; before: Doc | null; after: Doc | null; others: number };
  const audit = collection<Entry>("audit").access(({ principal }) => ({ read: principal.claim("role").eq("admin") }));
  const log = trigger("log", docs, (ctx, change) => {
    ctx.set(audit, String(ctx.scan(audit).length).padStart(3, "0"), { by: ctx.principal()?.subject ?? null, before: change.before, after: change.after, others: ctx.scan(docs).length });
  });
  // Rows nobody reads, which their owners still add and remove, each with a materialized value.
  const presence = collection<{ at: number }>("presence").access(({ principal, key }) => ({ read: false, write: key.eq(principal.subject) }));
  const seen = derive("seen", (ctx, id: string) => ctx.get(presence, id)?.at ?? null, { materialize: { each: presence } });
  const app = define({
    collections: [docs, audit, presence],
    definitions: [seen],
    triggers: [log],
    auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials, claims: { role: credentials === "root" ? "admin" : "user" } } : null },
    http: {
      put: mutation("put", { args: v.object({ id: v.string(), doc: v.json() }) }, (ctx, { id, doc }) => { ctx.set(docs, id, doc as Doc); return null; }),
      edit: mutation("edit", { args: v.object({ id: v.string(), text: v.string() }) }, (ctx, { id, text }) => {
        const doc = ctx.get(docs, id)!;
        ctx.set(docs, id, { ...doc, text });
        return null;
      }),
      audit: query("audit", { access: "public" }, (ctx) => ctx.scan(audit).map((row) => row.value)),
      arrive: mutation("arrive", { args: v.string() }, (ctx, id) => { ctx.set(presence, id, { at: ctx.now() }); return null; }),
      leave: mutation("leave", { args: v.string() }, (ctx, id) => {
        // A read first: the caller sees nothing, which must not fool the trigger.
        ctx.get(presence, id);
        ctx.delete(presence, id);
        return null;
      }),
    },
  });
  const db = await testDatabase(app);
  const as = (credentials: string) => ({ credentials });
  db.mutate("put", { id: "a", doc: { owner: "alice", text: "one", secret: "s1" } }, as("root"));
  db.mutate("put", { id: "b", doc: { owner: "bob", text: "two" } }, as("root"));
  db.mutate("edit", { id: "a", text: "edited" }, as("alice"));
  const entries = db.query("audit", null, as("root")) as Entry[];
  assert.equal(entries.length, 3);
  // The trigger saw the hidden field and every row, and wrote an audit alice can't.
  assert.deepEqual(entries[2], {
    by: "alice",
    before: { owner: "alice", text: "one", secret: "s1" },
    after: { owner: "alice", text: "edited", secret: "s1" },
    others: 2,
  });
  // Back in the caller's view afterwards.
  assert.deepEqual(db.query("audit", null, as("alice")), []);
  const roots = () => Object.keys(db.data).filter((id) => id.startsWith('root:["seen"')).sort();
  db.mutate("arrive", "alice", as("alice"));
  db.mutate("arrive", "bob", as("bob"));
  assert.deepEqual(roots(), ['root:["seen","alice"]', 'root:["seen","bob"]']);
  db.mutate("leave", "alice", as("alice"));
  assert.deepEqual(roots(), ['root:["seen","bob"]']);
  // Leaving for someone else is a no-op: their row and its value stay.
  db.mutate("leave", "bob", as("alice"));
  assert.deepEqual(roots(), ['root:["seen","bob"]']);
});

test("derived values say who may read them, by the caller and the arguments", async () => {
  const count = (ctx: any, owner: string) => ctx.scan(notes).filter((row: any) => row.value.owner === owner).length;
  // Each caller may read its own count; admins any.
  const countFor = derive("countFor", count, {
    access: ({ principal, args }) => args.eq(principal.subject).or(principal.claim("role").eq("admin")),
  });
  const countOf = derive("countOf", (ctx, args: { owner: string }) => count(ctx, args.owner), {
    access: ({ principal, args }) => args.at("owner").eq(principal.subject),
  });
  const everyone = derive("everyone", (ctx) => ctx.scan(notes).length);
  const app = define({
    collections: [notes],
    definitions: [countFor, countOf, everyone],
    auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials, claims: { role: credentials === "root" ? "admin" : "user" } } : null },
    http: {
      put: mutation("put", { args: v.object({ id: v.string(), note: noteSchema }) }, (ctx, { id, note }) => { ctx.set(notes, id, note); return null; }),
      readFor: query("readFor", { access: "public", args: v.string() }, (ctx, owner) => ctx.get(countFor, owner)),
      readOf: query("readOf", { access: "public", args: v.string() }, (ctx, owner) => ctx.get(countOf, { owner })),
      readAll: query("readAll", { access: "public" }, (ctx) => ctx.get(everyone)),
    },
  });
  assert.deepEqual((app.definitions as any).countFor.access, {
    any: [{ eq: [{ ref: ["args"] }, { ref: ["principal", "subject"] }] }, { eq: [{ ref: ["principal", "claims", "role"] }, { value: "admin" }] }],
  });
  assert.equal((app.definitions as any).everyone.access, undefined);
  const db = await testDatabase(app);
  const as = (credentials?: string) => credentials === undefined ? {} : { credentials };
  for (const [id, owner] of [["a1", "alice"], ["a2", "alice"], ["b1", "bob"]]) db.mutate("put", { id, note: { owner, rank: 1, text: id } }, as("root"));
  assert.equal(db.query("readFor", "alice", as("alice")), 2);
  assert.equal(db.query("readOf", "alice", as("alice")), 2);
  assert.equal(db.query("readFor", "bob", as("root")), 1);
  const denied = (run: () => unknown) => assert.throws(run, (error: any) => error.failure?.code === "ACCESS_DENIED" && /denies reading/.test(error.failure.message));
  denied(() => db.query("readFor", "bob", as("alice")));
  denied(() => db.query("readOf", "bob", as("alice")));
  denied(() => db.query("readFor", "alice", as()));
  // Without a rule, any method may read it: its author decides.
  assert.equal(db.query("readAll", null, as()), 3);
  assert.throws(() => derive("bad", count, { access: 1 as never }), /must be a rule, true or false/);
  // @ts-expect-error: the arguments are a string.
  derive("typed", count, { access: ({ args }) => args.eq(1) });
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
