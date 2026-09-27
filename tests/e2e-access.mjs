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
  return `import { aggregate, collection, define, derive, external, fail, mutation, query, trigger, v } from ${JSON.stringify(join(root, "sdk/index.ts"))};
import { queue } from ${JSON.stringify(join(root, "sdk/temporal.ts"))};
const notes = collection("notes", v.object({ owner: v.string(), rank: v.int(), text: v.string(), secret: v.optional(v.string()), token: v.optional(v.string()) }))
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
      // token is write-only for its owner: admins read it, owners set or clear it.
      fields: { secret: { read: admin }, token: { read: admin, write: mine.or(next("owner").eq(principal.subject)).or(admin) } },
    };
  });
// Triggers act with the application's rights: they see the hidden secret and write an
// audit only admins read.
const audit = collection("audit").access(({ principal }) => ({ read: principal.claim("role").eq("admin") }));
const log = trigger("log", notes, (ctx, change) => {
  ctx.set(audit, String(ctx.scan(audit).length).padStart(3, "0"), { by: ctx.principal()?.subject ?? null, secret: (change.after ?? change.before).secret ?? null });
});
const total = derive("total", (ctx) => ctx.scan(notes).length);
// Counts every owner's rows, but tells each caller only its own count (admins any).
const countFor = derive("countFor", (ctx, owner) => ctx.scan(notes).filter((row) => row.value.owner === owner).length, {
  access: ({ principal, args }) => args.eq(principal.subject).or(principal.claim("role").eq("admin")),
});
// Aggregates and external values are derived values: the same kind of rule says who reads them.
const perOwner = aggregate("perOwner", {
  source: notes, index: "owner", initial: () => 0, add: (count) => count + 1, remove: (count) => count - 1,
  access: ({ principal, args }) => args.eq(principal.subject).or(principal.claim("role").eq("admin")),
});
const summary = external("summary", {
  input: (ctx, key) => ctx.get(notes, key)?.text ?? null,
  access: ({ principal }) => principal.claim("role").eq("admin"),
});
const id = v.string({ min: 1 });
const open = { access: "public" };
const owned = query("owned", { ...open, args: id }, (ctx, owner) => ctx.get(perOwner, owner));
const summarized = query("summarized", { ...open, args: id }, (ctx, key) => ctx.get(summary, key));
const get = query("get", { ...open, args: id }, (ctx, key) => ctx.get(notes, key));
const list = query("list", open, (ctx) => ctx.scan(notes).map((row) => row.key));
const mine = query("mine", open, (ctx) => ctx.query(notes.by("owner").eq(ctx.principal()?.subject ?? "")).map((note) => note.text));
const page = query("page", { ...open, args: v.nullable(v.string()) }, (ctx, after) =>
  ctx.range(notes.by("rank").range({ limit: 2, ...(after ? { after } : {}) })));
const count = query("count", open, (ctx) => ctx.get(total));
const tally = query("tally", { ...open, args: id }, (ctx, owner) => ctx.get(countFor, owner));
const put = mutation("put", { args: v.object({ id, note: v.json() }) }, (ctx, { id, note }) => { ctx.set(notes, id, note as any); return null; });
const edit = mutation("edit", { args: v.object({ id, text: v.string() }) }, (ctx, { id, text }) => {
  const note = ctx.get(notes, id);
  if (!note) fail("NOT_FOUND", "No such note");
  ctx.set(notes, id, { ...note, text });
  return null;
});
const remove = mutation("remove", { args: id }, (ctx, key) => { ctx.delete(notes, key); return null; });
const clearToken = mutation("clearToken", { args: id }, (ctx, key) => {
  const note = ctx.get(notes, key);
  if (!note) fail("NOT_FOUND", "No such note");
  ctx.set(notes, key, note, { clear: ["token"] });
  return null;
});
const entries = query("audit", open, (ctx) => ctx.scan(audit).map((row) => row.value));
// Sessions people see when they own them or they aren't private, and a log whose entries
// follow their session's rule through readable().
const sessions = collection("sessions", v.object({ owner: v.string(), private: v.boolean() }))
  .access(({ principal, row, next, any, not }) => ({
    read: any(principal.claim("role").eq("admin"), row("owner").eq(principal.subject), not(row("private").eq(true))),
    insert: next("owner").eq(principal.subject),
    update: row("owner").eq(principal.subject),
  }));
const events = collection("events", v.object({ session: v.string(), text: v.string() }))
  .key(v.tuple([v.string({ min: 1 }), v.int({ min: 1 })]))
  .access(({ principal, key, readable, any }) => {
    const entries = any(principal.claim("role").eq("admin"), readable(sessions, key.at(0)));
    return { read: entries, write: entries };
  });
const timeline = query("timeline", open, (ctx) => ctx.scan(events).map((row) => row.value.text));
const start = mutation("start", { args: v.object({ id, private: v.boolean() }) }, (ctx, { id, private: hidden }) => {
  ctx.set(sessions, id, { owner: ctx.principal().subject, private: hidden });
  ctx.set(events, [id, 1], { session: id, text: id + "/1" });
  return null;
});
const append = mutation("append", { args: v.object({ session: id, n: v.int({ min: 1 }) }) }, (ctx, { session, n }) => {
  ctx.set(events, [session, n], { session, text: session + "/" + n });
  return null;
});
const share = mutation("share", { args: v.object({ id, private: v.boolean() }) }, (ctx, { id, private: hidden }) => {
  const row = ctx.get(sessions, id);
  if (!row) fail("NOT_FOUND", "No such session");
  ctx.set(sessions, id, { ...row, private: hidden });
  return null;
});
// A job follows its session: whoever reads the session reads, enqueues, retries and cancels
// its jobs (admins any). Workers claim and report whatever the rule: the lease entitles them.
const jobs = queue("jobs", {
  retry: false,
  access: ({ principal, row, next, any, readable }) => {
    const admin = principal.claim("role").eq("admin");
    return {
      read: any(admin, readable(sessions, row("payload", "session"))),
      insert: any(admin, readable(sessions, next("payload", "session"))),
      update: any(admin, readable(sessions, next("payload", "session"))),
      delete: any(admin, readable(sessions, row("payload", "session"))),
    };
  },
});
const ask = mutation("ask", { args: v.object({ id, session: id }) }, (ctx, { id, session }) => { jobs.enqueue(ctx, id, { session }); return null; });
const asked = query("asked", open, (ctx) => jobs.scan(ctx).map((job) => job.id + ":" + job.state));
const work = mutation("work", { args: v.object({ failing: v.boolean() }) }, (ctx, { failing }) => {
  const claim = jobs.claim(ctx, ctx.principal().subject);
  if (claim === null) return null;
  if (failing) jobs.fail(ctx, claim, "no");
  else jobs.complete(ctx, claim, "done");
  return claim.id;
});
const again = mutation("again", { args: id }, (ctx, key) => jobs.retry(ctx, key).state);
const drop = mutation("drop", { args: id }, (ctx, key) => jobs.cancel(ctx, key));
// What a mutation can find of definer rights: nothing, on servers that hand them to the SDK alone.
const probe = mutation("probe", open, (ctx) => [typeof ctx.definer, typeof globalThis.__flowerContexts?.[0]?.definer, typeof globalThis.__flowerContexts?.[1]?.definer]);
export default define({
  collections: [notes, audit, sessions, events],
  uses: [summary, jobs],
  definitions: [total, countFor, perOwner],
  triggers: [log],
  auth: {
    authenticate: (_ctx, credentials) => typeof credentials === "string"
      ? { subject: credentials, claims: credentials === "root" ? { role: "admin" } : {} }
      : null,
  },
  http: { get, list, mine, page, count, tally, put, edit, remove, clearToken, audit: entries, timeline, start, append, share, probe, owned, summarized, ask, asked, work, again, drop },
});`;
}

try {
  await cluster.start();
  [alice, bob, rootUser, anonymous] = [as("alice"), as("bob"), as("root"), as()];
  const fixture = join(cluster.directory, "notes.ts");
  await writeFile(fixture, source(`mine.or(admin)`));
  const bundle = await buildBundle(fixture, { initialization: "static" });
  await admin().deploy(bundle, { requestId: "access-deploy" });

  // No context offers definer rights. E2E_OLD_RUNNER=1 runs this against a server from before
  // the runner handed them to the SDK alone, whose raw method context still carries them: the
  // bundle's triggers must act as the definer there too (the audit checks below).
  const exposed = process.env.E2E_OLD_RUNNER === "1" ? "function" : "undefined";
  assert.deepEqual(await value(mutate(alice, "probe", null)), ["undefined", "undefined", exposed]);

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
  // Derived values run without a caller: they see every row, so they say who may read them.
  assert.equal(await value(alice.query("count")), 5);
  assert.equal(await value(alice.query("tally", "alice")), 3);
  assert.equal(await value(rootUser.query("tally", "bob")), 2);
  await denied(alice.query("tally", "bob"));
  await denied(anonymous.query("tally", "alice"));
  assert.equal(await value(alice.query("owned", "alice")), 3);
  assert.equal(await value(rootUser.query("owned", "bob")), 2);
  await denied(alice.query("owned", "bob"));
  await denied(anonymous.query("owned", "alice"));
  assert.deepEqual(await value(rootUser.query("summarized", "a1")), { status: "pending" });
  await denied(alice.query("summarized", "a1"));

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
  // A write-only field: alice sets it without seeing it, then clears it by name, while
  // the hidden secret keeps its value.
  await mutate(alice, "put", { id: "a1", note: { owner: "alice", rank: 1, text: "edited", token: "t1" } });
  assert.deepEqual(await value(alice.query("get", "a1")), { owner: "alice", rank: 1, text: "edited" });
  assert.equal((await value(rootUser.query("get", "a1"))).token, "t1");
  await mutate(alice, "clearToken", "a1");
  assert.deepEqual(await value(rootUser.query("get", "a1")), { owner: "alice", rank: 1, text: "edited", secret: "s1" });
  // The trigger saw alice's hidden secret and wrote an audit she can't read.
  const trail = await value(rootUser.query("audit"));
  assert.deepEqual(trail.at(-1), { by: "alice", secret: "s1" });
  assert.deepEqual(await value(alice.query("audit")), []);

  // readable(): a log's entries follow their session's rule, a session created in the same
  // mutation included, and a live watch follows the session's privacy.
  await mutate(alice, "start", { id: "s1", private: true });
  await mutate(bob, "start", { id: "s2", private: true });
  await mutate(bob, "start", { id: "s3", private: false });
  assert.deepEqual(await value(alice.query("timeline")), ["s1/1", "s3/1"]);
  assert.deepEqual(await value(bob.query("timeline")), ["s2/1", "s3/1"]);
  assert.deepEqual(await value(rootUser.query("timeline")), ["s1/1", "s2/1", "s3/1"]);
  await denied(mutate(alice, "append", { session: "s2", n: 2 }));
  await denied(mutate(alice, "append", { session: "nowhere", n: 1 }));
  await mutate(alice, "append", { session: "s3", n: 2 });
  const watching = new AbortController();
  const watch = alice.watch("timeline", null, { signal: watching.signal });
  const next = async () => {
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error("watch did not update")), 10_000));
    return (await Promise.race([watch.next(), timeout])).value.value;
  };
  assert.deepEqual(await next(), ["s1/1", "s3/1", "s3/2"]);
  await mutate(bob, "share", { id: "s2", private: false });
  assert.deepEqual(await next(), ["s1/1", "s2/1", "s3/1", "s3/2"]);
  await mutate(bob, "share", { id: "s2", private: true });
  assert.deepEqual(await next(), ["s1/1", "s3/1", "s3/2"]);
  watching.abort();
  await watch.return?.().catch(() => {});

  // Queues: callers enqueue, read, retry and cancel the jobs of sessions they see; a worker
  // with no role claims and reports every job, and still reads only what the rule allows.
  const code = (expected) => (error) => error instanceof FlowerError && error.failure?.code === expected;
  await mutate(alice, "ask", { id: "j1", session: "s1" });
  await mutate(alice, "ask", { id: "j3", session: "s3" });
  await denied(mutate(alice, "ask", { id: "j2", session: "s2" }));
  await mutate(bob, "ask", { id: "j2", session: "s2" });
  await assert.rejects(mutate(alice, "ask", { id: "j2", session: "s1" }), code("JOB_EXISTS"));
  assert.deepEqual(await value(alice.query("asked")), ["j1:pending", "j3:pending"]);
  assert.deepEqual(await value(bob.query("asked")), ["j2:pending", "j3:pending"]);
  const worker = as("worker");
  assert.equal(await value(mutate(worker, "work", { failing: true })), "j1");
  assert.equal(await value(mutate(worker, "work", { failing: false })), "j3");
  assert.equal(await value(mutate(worker, "work", { failing: false })), "j2");
  assert.equal(await value(mutate(worker, "work", { failing: false })), null);
  assert.deepEqual(await value(worker.query("asked")), ["j3:completed"]);
  assert.deepEqual(await value(alice.query("asked")), ["j1:failed", "j3:completed"]);
  // For bob, alice's failed job isn't there to retry or cancel.
  await assert.rejects(mutate(bob, "again", "j1"), code("JOB_NOT_FAILED"));
  assert.equal(await value(mutate(bob, "drop", "j1")), false);
  assert.equal(await value(mutate(alice, "again", "j1")), "pending");
  assert.equal(await value(mutate(alice, "drop", "j1")), true);
  assert.deepEqual(await value(rootUser.query("asked")), ["j2:completed", "j3:completed"]);

  // A policy-only redeploy takes effect at once, cached results included.
  await writeFile(fixture, source(`principal.authenticated`));
  await admin().deploy(await buildBundle(fixture, { initialization: "static" }), { requestId: "access-redeploy" });
  assert.deepEqual(await value(alice.query("list")), ["a1", "a3", "b1", "b2"]);
  assert.deepEqual(await value(alice.query("get", "b1")), { owner: "bob", rank: 2, text: "two" });
  assert.deepEqual(await value(anonymous.query("list")), []);
  console.log("PASS: collection access policies hide rows and fields, fill pages, guard inserts, updates and deletes, keep redacted fields on edit, clear them by name, give triggers the application's rights, guard aggregates and external values like derived ones, guard queue jobs while workers claim them all, let rows follow another row's rule (live watches included), and follow policy redeploys");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await cluster.close();
}
