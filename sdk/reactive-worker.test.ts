import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import type app from "../docs/reactive-worker.ts";
import { FlowerError } from "./client.ts";
import { canonicalJson } from "./json.ts";
import { testDatabase } from "./testing.ts";

// The downloadable app, bundled against current SDK source and run isolated.
const entry = fileURLToPath(new URL("../docs/reactive-worker.ts", import.meta.url));
const application = () => testDatabase<typeof app>(entry);
const sha = (text: string) => createHash("sha256").update(text).digest("hex");
const input = (text: string) => ({ recipe: "sha256-v1", text });
const pending = { status: "pending" } as const;
const ready = (text: string) => ({ status: "ready", value: sha(text) }) as const;

function fails(action: () => unknown, code: string): FlowerError {
  let caught: unknown;
  assert.throws(action, (error) => { caught = error; return true; });
  assert.ok(caught instanceof FlowerError, String(caught));
  assert.equal(caught.failure?.code, code, caught.message);
  return caught;
}

test("publishing moves a digest from pending to ready, and an edit makes it pending again", async () => {
  const db = await application();
  assert.equal(db.query("document.get", "one"), null);
  assert.equal(db.query("digest.pending", "one"), null);
  db.mutate("document.put", { id: "one", text: "A" });
  const work = db.query("digest.pending", "one");
  assert.deepEqual(work, { args: "one", key: canonicalJson(["one", input("A")]), input: input("A") });
  assert.deepEqual(db.query("document.get", "one"), { text: "A", digest: pending });
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: work.key, value: sha("A") }), { accepted: true });
  assert.deepEqual(db.query("document.get", "one"), { text: "A", digest: ready("A") });
  assert.equal(db.query("digest.pending", "one"), null);

  db.mutate("document.put", { id: "one", text: "B" });
  assert.deepEqual(db.query("document.get", "one"), { text: "B", digest: pending });
  assert.deepEqual(db.query("digest.pending", "one")?.input, input("B"));
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: work.key, value: sha("A") }), { accepted: false }, "stale work is rejected");
  assert.deepEqual(db.query("document.get", "one"), { text: "B", digest: pending });
});

test("superseded and deleted work cannot publish, and deleting a document removes its result", async () => {
  const db = await application();
  const results = `source:${canonicalJson(["digest.results", canonicalJson("one")])}`;
  db.mutate("document.put", { id: "one", text: "A" });
  const workA = db.query("digest.pending", "one")!;
  db.mutate("digest.publish", { args: "one", key: workA.key, value: sha("A") });
  assert.ok(Object.hasOwn(db.data, results));
  db.mutate("document.put", { id: "one", text: "B" });
  const workB = db.query("digest.pending", "one")!;
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: workA.key, value: sha("A") }), { accepted: false });
  db.mutate("document.delete", "one");
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: workB.key, value: sha("B") }), { accepted: false });
  assert.equal(db.query("digest.pending", "one"), null);
  assert.equal(db.query("document.get", "one"), null);
  assert.deepEqual(db.query("digest.next"), []);
  assert.deepEqual(Object.keys(db.data), ["clock"], "the result and the stale marker are gone with the document");
  assert.deepEqual(db.mutate("digest.publish", { args: "never", key: workA.key, value: sha("A") }), { accepted: false });
});

test("racing workers keep the first accepted result", async () => {
  const db = await application();
  db.mutate("document.put", { id: "one", text: "A" });
  const work = db.query("digest.pending", "one")!;
  for (const value of [sha("A"), sha("a duplicate must not overwrite")]) {
    assert.deepEqual(db.mutate("digest.publish", { args: "one", key: work.key, value }), { accepted: true });
  }
  assert.deepEqual(db.query("document.get", "one"), { text: "A", digest: ready("A") });
  assert.equal(db.query("digest.pending", "one"), null);
});

test("A → B → A accepts equivalent old work and reuses a stored matching result", async () => {
  const db = await application();
  db.mutate("document.put", { id: "one", text: "A" });
  const workA = db.query("digest.pending", "one")!;
  db.mutate("document.put", { id: "one", text: "B" });
  const workB = db.query("digest.pending", "one")!;
  db.mutate("document.put", { id: "one", text: "A" });
  assert.deepEqual(db.query("digest.pending", "one"), workA);
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: workA.key, value: sha("A") }), { accepted: true });
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: workB.key, value: sha("B") }), { accepted: false });

  db.mutate("document.put", { id: "one", text: "B" });
  assert.deepEqual(db.query("document.get", "one"), { text: "B", digest: pending });
  db.mutate("document.put", { id: "one", text: "A" });
  assert.equal(db.query("digest.pending", "one"), null);
  assert.deepEqual(db.query("document.get", "one"), { text: "A", digest: ready("A") });
});

test("edits to other documents neither invalidate work nor share identities", async () => {
  const db = await application();
  db.mutate("document.put", { id: "one", text: "A" });
  const work = db.query("digest.pending", "one")!;
  db.mutate("document.put", { id: "two", text: "A" });
  assert.notEqual(db.query("digest.pending", "two")!.key, work.key);
  assert.deepEqual(db.mutate("digest.publish", { args: "two", key: work.key, value: sha("A") }), { accepted: false });
  db.mutate("document.put", { id: "two", text: "B" });
  assert.deepEqual(db.mutate("digest.publish", { args: "one", key: work.key, value: sha("A") }), { accepted: true });
  assert.deepEqual(db.query("document.get", "one"), { text: "A", digest: ready("A") });
  assert.deepEqual(db.query("document.get", "two"), { text: "B", digest: pending });
});

test("next lists pending documents oldest first, in limits and disjoint shards, and drains as results arrive", async () => {
  const db = await application();
  for (const [id, text] of [["one", "A"], ["two", "B"], ["three", "C"]]) {
    db.mutate("document.put", { id, text });
    db.now += 1;
  }
  const ids = (options: Parameters<typeof db.query<"digest.next">>[1] = null) => db.query("digest.next", options).map((work) => work.args);
  assert.deepEqual(ids(), ["one", "two", "three"]);
  assert.deepEqual(db.query("digest.next")[1], db.query("digest.pending", "two"));
  assert.deepEqual(ids({ limit: 2 }), ["one", "two"]);
  const shards = [ids({ shard: [0, 2] }), ids({ shard: [1, 2] })];
  assert.deepEqual(shards.flat().sort(), ["one", "three", "two"]);
  assert.equal(new Set(shards.flat()).size, 3);

  const two = db.query("digest.pending", "two")!;
  db.mutate("digest.publish", { args: "two", key: two.key, value: sha("B") });
  db.mutate("document.put", { id: "one", text: "A, edited" });
  assert.deepEqual(ids(), ["one", "three"], "an edit keeps its place in line");
  db.mutate("document.put", { id: "two", text: "B, edited" });
  assert.deepEqual(ids(), ["one", "three", "two"], "a fresh edit of a finished document joins the end");
  for (const options of [{ limit: 0 }, { limit: 1025 }, { shard: [2, 2] }, { shard: [0, 0] }, { batch: 1 }]) {
    assert.throws(() => db.query("digest.next", options as never), FlowerError);
  }
});

test("invalid documents and malformed results leave state unchanged", async () => {
  const db = await application();
  db.mutate("document.put", { id: "one", text: "" });
  const work = db.query("digest.pending", "one")!;
  const before = db.data;
  for (const args of [null, {}, { id: "", text: "A" }, { id: "one", text: 42 }, { id: "one", text: "A".repeat(100_001) }, { id: "one", text: "A", extra: true }]) {
    fails(() => db.mutate("document.put", args as never), "INVALID_ARGUMENT");
  }
  for (const value of [null, 42, "a".repeat(63), "g".repeat(64), "A".repeat(64)]) {
    const error = fails(() => db.mutate("digest.publish", { args: "one", key: work.key, value } as never), "INVALID_ARGUMENT");
    assert.match(error.failure!.message, /^value must (be a string|match)/);
  }
  fails(() => db.mutate("digest.publish", { args: "one", key: work.key } as never), "INVALID_ARGUMENT");
  assert.deepEqual(db.data, before);
});
