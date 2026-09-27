// Definer rights: the application's own rights, which triggers and SDK bookkeeping act
// with. The server hands them only to the SDK, as a mutation compute's third argument.
import assert from "node:assert/strict";
import test from "node:test";
import * as sdk from "./index.ts";
import { collection, define, derive, mutation, query, trigger, v } from "./index.ts";
import { asDefiner, hostContext } from "./define.ts";
import { testDatabase } from "./testing.ts";

type Doc = { owner: string; text: string };
const docs = collection<Doc>("docs").access(({ principal, row, next }) => ({
  read: row("owner").eq(principal.subject),
  insert: next("owner").eq(principal.subject),
  update: row("owner").eq(principal.subject),
}));
const audit = collection<{ by: string | null; text: string }>("audit").access({ read: false });

/**
 * A stand-in for the server's host context: rows in a map, every call logged with how
 * deep in definer rights it ran. `definer` gives it the older servers' host.definer.
 */
function fakeHost(options: { definer?: boolean } = {}) {
  const rows = new Map<string, unknown>();
  const log: string[] = [];
  let depth = 0;
  const elevate = (on: boolean) => { depth += on ? 1 : -1; log.push(`elevate(${on})`); return null; };
  const name = (reference: any) => typeof reference === "string" ? reference : reference.name;
  const host: Record<string, (...args: any[]) => any> = {
    now: () => 1, clock: () => 1, changesAt: () => null, history: () => null,
    principal: () => ({ subject: "alice" }),
    get: (reference: any, key: string) => { log.push(`get ${name(reference)} ${depth}`); return rows.get(`${name(reference)}/${key}`) ?? null; },
    scan: (reference: any) => { log.push(`scan ${name(reference)} ${depth}`); return []; },
    query: () => [], range: () => ({ rows: [], cursor: null }),
    set: (reference: any, key: string, value: unknown) => { log.push(`set ${name(reference)} ${depth}`); rows.set(`${name(reference)}/${key}`, value); return null; },
    delete: (reference: any, key: string) => { rows.delete(`${name(reference)}/${key}`); return null; },
    materialize: () => null, unmaterialize: () => null,
  };
  if (options.definer) host.definer = elevate;
  return { host: Object.freeze(host), elevate, log, depth: () => depth };
}

function auditedApp(probe?: (ctx: sdk.MutationContext) => unknown) {
  const log = trigger("log", docs, (ctx, change) => {
    ctx.set(audit, String(ctx.scan(audit).length), { by: ctx.principal()?.subject ?? null, text: change.after?.text ?? "" });
  });
  return define({
    collections: [docs, audit],
    triggers: [log],
    auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials } : null, default: "public" },
    http: {
      put: mutation("put", { args: v.object({ id: v.string(), text: v.string() }) }, (ctx, { id, text }) => {
        ctx.set(docs, id, { owner: ctx.principal()!.subject, text });
        return probe ? probe(ctx) : null;
      }),
    },
  });
}

test("no context carries definer rights, not ctx nor the raw host behind it", async () => {
  const app = auditedApp((ctx) => [typeof (ctx as any).definer, typeof hostContext(ctx).definer]);
  const db = await testDatabase(app);
  assert.deepEqual(db.mutate("put", { id: "a", text: "one" }, { credentials: "alice" }), ["undefined", "undefined"]);
  // Older servers' host contexts carry definer: the raw host the SDK hands out still doesn't.
  const old = fakeHost({ definer: true });
  assert.deepEqual((app.definitions as any).put.compute(old.host, { id: "a", text: "one" }), ["undefined", "undefined"]);
  // Nor does the package export a way in.
  for (const name of ["asDefiner", "hostContext", "enforceAccess"]) assert.equal(name in sdk, false, name);
});

test("triggers act as the definer through the runner's third argument, and never touch what the method passed in meanwhile", () => {
  const app = auditedApp();
  const { host, elevate, log, depth } = fakeHost();
  // A reference whose every property read is recorded with the rights it ran with.
  const seen: number[] = [];
  const sneaky = new Proxy(docs, { get(target, property, receiver) { seen.push(depth()); return Reflect.get(target, property, receiver); } });
  const put = mutation("put", { args: v.string() }, (ctx, id) => { ctx.set(sneaky, id, { owner: "alice", text: id }); return null; });
  const logTrigger = trigger("log", docs, (ctx, change) => { ctx.set(audit, "0", { by: null, text: change.after?.text ?? "" }); });
  const proxied = define({ collections: [docs, audit], triggers: [logTrigger], http: { put } });
  (proxied.definitions as any).put.compute(host, "a", elevate);
  assert.deepEqual(log, ["elevate(true)", "get docs 1", "elevate(false)", "set docs 0", "elevate(true)", "get docs 1", "set audit 1", "elevate(false)"]);
  assert.ok(seen.length > 0 && seen.every((level) => level === 0), `method-supplied reference read while elevated: ${seen}`);
  assert.equal(depth(), 0);
  // The plain app too: its trigger writes the audit as the definer.
  const plain = fakeHost();
  (app.definitions as any).put.compute(plain.host, { id: "b", text: "two" }, plain.elevate);
  assert.ok(plain.log.includes("set audit 1"), plain.log.join(", "));
  assert.equal(plain.depth(), 0);
});

test("a bundle built with this SDK keeps working on servers that pass no third argument", () => {
  // Older servers offer host.definer on the context instead: the SDK falls back to it.
  const app = auditedApp();
  const old = fakeHost({ definer: true });
  (app.definitions as any).put.compute(old.host, { id: "a", text: "one" });
  assert.deepEqual(old.log, ["elevate(true)", "get docs 1", "elevate(false)", "set docs 0", "elevate(true)", "get docs 1", "scan audit 1", "set audit 1", "elevate(false)"]);
  assert.equal(old.depth(), 0);
  // A server without either enforces nothing, so there is nothing to lift.
  const none = fakeHost();
  (app.definitions as any).put.compute(none.host, { id: "a", text: "one" });
  assert.equal(none.log.some((line) => line.startsWith("elevate")), false);
  // Apps without rules never lift, even where they could.
  const open = collection<Doc>("open");
  const plain = define({
    triggers: [trigger("echo", open, (ctx, change) => ctx.set(collection("copy"), String(change.key), change.after))],
    http: { put: mutation("put", { args: v.string() }, (ctx, id) => { ctx.set(open, id, { owner: "x", text: id }); return null; }) },
  });
  const unguarded = fakeHost({ definer: true });
  (plain.definitions as any).put.compute(unguarded.host, "a", unguarded.elevate);
  assert.equal(unguarded.log.some((line) => line.startsWith("elevate")), false);
});

test("SDK internals act as the definer only in mutations: queries and derived values run as the caller", () => {
  const lifted: string[] = [];
  const peek = query("peek", (ctx) => asDefiner(ctx, () => { lifted.push("query"); return ctx.get(docs, "a"); }));
  const change = mutation("change", (ctx) => asDefiner(ctx, () => { lifted.push("mutation"); return ctx.get(docs, "a"); }));
  const app = define({ collections: [docs], http: { peek, change } });
  const { host, elevate, log } = fakeHost();
  // Even handed a capability, a query doesn't use it.
  (app.definitions as any).peek.compute(host, null, elevate);
  (app.definitions as any).change.compute(host, null, elevate);
  assert.deepEqual(lifted, ["query", "mutation"]);
  assert.deepEqual(log, ["get docs 0", "elevate(true)", "get docs 1", "elevate(false)"]);
  // On older servers too, where host.definer would fail a query.
  const old = fakeHost({ definer: true });
  (app.definitions as any).peek.compute(old.host, null);
  assert.deepEqual(old.log, ["get docs 0"]);
});

test("the test database hands elevate only to mutations, as their third argument, and fails a method returning elevated", async () => {
  // Raw definitions, as the runner sees them: the SDK would keep elevate to itself.
  const module = {
    definitions: {
      leak: { kind: "mutationMethod", name: "leak", compute: (_ctx: unknown, _args: unknown, elevate: (on: boolean) => void) => { elevate(true); return null; } },
      even: { kind: "mutationMethod", name: "even", compute: (ctx: any, _args: unknown, elevate: (on: boolean) => void) => {
        elevate(true); elevate(false);
        return [typeof ctx.definer, typeof elevate];
      } },
      caught: { kind: "mutationMethod", name: "caught", compute: (_ctx: unknown, _args: unknown, elevate: (on: boolean) => void) => {
        try { elevate(false); } catch (error: any) { return error.code; }
      } },
      look: { kind: "queryMethod", name: "look", compute: (ctx: any, _args: unknown, elevate: unknown) => [typeof ctx.definer, typeof elevate, ctx.get({ kind: "derived", name: "inner" }, null)] },
      inner: { kind: "derived", name: "inner", compute: (ctx: any, _args: unknown, elevate: unknown) => [typeof ctx.definer, typeof elevate] },
    },
    http: {
      leak: { name: "leak", kind: "mutation" }, even: { name: "even", kind: "mutation" },
      caught: { name: "caught", kind: "mutation" }, look: { name: "look", kind: "query" },
    },
    maintenance: null,
  } as unknown as sdk.FlowerModule;
  const db = await testDatabase(module);
  assert.throws(() => (db as any).mutate("leak"), (error: any) =>
    error.failure?.code === "DEFINER_UNBALANCED" && /leak returned acting as the definer/.test(error.failure.message));
  assert.deepEqual((db as any).mutate("even"), ["undefined", "function"]);
  assert.equal((db as any).mutate("caught"), "INVALID_VALUE");
  assert.deepEqual((db as any).query("look"), ["undefined", "undefined", ["undefined", "undefined"]]);
});

test("triggers read derived values whose rule the caller fails", async () => {
  // A rule on a derived value alone makes the app's triggers act as the definer.
  const notes = collection<{ text: string }>("notes");
  const secret = derive("secret", (ctx) => ctx.scan(notes).length, { access: false });
  const counts = collection<{ count: number }>("counts");
  const count = trigger("count", notes, (ctx) => ctx.set(counts, "all", { count: ctx.get(secret) }));
  const app = define({
    definitions: [secret],
    triggers: [count],
    http: {
      add: mutation("add", { args: v.string() }, (ctx, id) => { ctx.set(notes, id, { text: id }); return null; }),
      peek: query("peek", (ctx) => ctx.get(secret)),
      total: query("total", (ctx) => ctx.get(counts, "all")?.count ?? null),
    },
  });
  const db = await testDatabase(app);
  db.mutate("add", "a");
  db.mutate("add", "b");
  assert.equal(db.query("total"), 2);
  assert.throws(() => db.query("peek"), (error: any) => error.failure?.code === "ACCESS_DENIED");
});
