// E2E_FLOWER_BIN=target/release/flower node tests/e2e-references.mjs
// Foreign keys on a three-node cluster: a deployment that declares them checks the rows already there
// (blocking, and a staged one at activation), mutations keep them whole on every replica, cascades
// delete through the bundle's triggers, concurrent writers can't slip an orphan past a deletion, and
// the stored schema keeps enforcing them after every node restarts.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient } from "../sdk/index.ts";
import { createHttp2Transport } from "../sdk/http2.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cluster = new LocalCluster({ nodes: 3, binary: process.env.E2E_FLOWER_BIN ?? join(root, "target/debug/flower") });
const transport = createHttp2Transport({ requestTimeoutMs: 20_000 });
const client = (node = cluster.leader) => new FlowerClient(node.url, { fetch: transport.fetch });
const admin = () => new FlowerAdmin(cluster.leader.url, { adminToken: cluster.adminToken, fetch: transport.fetch });
let sequence = 0;
const apply = (ops) => client().mutate("apply", ops, { requestId: `references-${++sequence}`, signal: AbortSignal.timeout(20_000) });

function source(references) {
  return `import { collection, define, mutation, query, v } from ${JSON.stringify(join(root, "sdk/index.ts"))};
const orgs = collection("orgs");
const sessions = collection<{ org: string | null }>("sessions")${references ? '.references(orgs, "org")' : ""};
const events = collection<{ text: string }>("events").key(v.tuple([v.string(), v.int()]))${references ? '.references(sessions, { key: 1 }, { onDelete: "cascade" })' : ""};
const all = { orgs, sessions, events };
export default define({ collections: [orgs, sessions, events], http: {
  apply: mutation("apply", (ctx, ops: any[]) => {
    for (const [op, name, key, value] of ops) op === "set" ? ctx.set(all[name], key, value) : ctx.delete(all[name], key);
    return null;
  }),
  rows: query("rows", (ctx) => Object.fromEntries(Object.entries(all).map(([name, each]) => [name, ctx.scan(each).map((row) => [row.key, row.value])]))),
} });`;
}

const violation = (pattern) => (error) => {
  assert.equal(error.failure?.code, "FOREIGN_KEY_VIOLATION", `${error.code}: ${error.message}`);
  assert.match(error.failure.message, pattern);
  return true;
};
/** Every replica serves the same rows. */
async function rows() {
  const views = await Promise.all(cluster.members.map(async (node) => (await client(node).query("rows")).value));
  for (const view of views.slice(1)) assert.deepEqual(view, views[0]);
  return views[0];
}
async function restartAll() {
  await Promise.all(cluster.members.map(async (node) => {
    node.process.intentional = true; node.process.child.kill("SIGKILL"); await node.process.exited;
  }));
  cluster.leader = null;
  for (const node of cluster.members) cluster._startNode(node);
  // A quorum can elect a leader before the third replica listens: wait for every one.
  await cluster._until("restart all Flower nodes", async (timeoutMs) => (await Promise.all(cluster.members.map(async (node) => {
    try { return (await cluster._fetch(node, "/raft/metrics", { timeoutMs })).ok; }
    catch { return false; }
  }))).every(Boolean));
  await cluster.discoverLeader();
}

try {
  await cluster.start();
  const fixture = join(cluster.directory, "references.ts");
  const bundle = async (references) => { await writeFile(fixture, source(references)); return buildBundle(fixture); };
  const plain = await bundle(false);
  await admin().deploy(plain, { requestId: "plain", preparation: "blocking" });
  // Rows from before any reference, two of them orphans.
  await apply([
    ["set", "orgs", "o1", {}], ["set", "sessions", "s1", { org: "o1" }], ["set", "sessions", "s2", { org: "gone" }],
    ["set", "events", ["s1", 1], { text: "a" }], ["set", "events", ["s1", 2], { text: "b" }], ["set", "events", ["s3", 1], { text: "orphan" }],
  ]);
  const referring = await bundle(true);
  // References are checked in order, by collection: the first row that breaks one fails the deployment.
  await assert.rejects(admin().deploy(referring, { requestId: "blocking", preparation: "blocking" }),
    violation(/^events row "\[\\"s3\\",1\]" refers to sessions row "s3", which does not exist$/));
  // The old code still serves: nothing is checked yet.
  await apply([["set", "sessions", "s4", { org: "nowhere" }], ["delete", "sessions", "s4"]]);

  // A staged deployment prepares, then checks the rows when it activates.
  await admin().stageDeployment(referring, { requestId: "staged" });
  let state;
  for (let n = 0; n < 100 && state?.phase !== "ready"; n++) {
    state = (await admin().controlStagedDeployment({ operation: "advance", requestId: "staged", maxBytes: 64 * 1024 })).value;
    assert.notEqual(state.phase, "failed", state.error);
  }
  assert.equal(state.phase, "ready");
  const activate = () => admin().controlStagedDeployment({ operation: "activate", requestId: "staged" });
  await assert.rejects(activate(), violation(/^events row "\[\\"s3\\",1\]" refers to sessions row "s3", which does not exist$/));
  await apply([["delete", "events", ["s3", 1]]]);
  await assert.rejects(activate(), violation(/^sessions row "s2" refers to orgs row "gone", which does not exist$/));
  await apply([["delete", "sessions", "s2"]]);
  assert.equal((await activate()).value.phase, "active");

  // Written rows refer only to rows that exist; rows still referred to stay.
  await assert.rejects(apply([["set", "sessions", "s5", { org: "nowhere" }]]), violation(/refers to orgs row "nowhere"/));
  await assert.rejects(apply([["delete", "orgs", "o1"]]), violation(/^orgs row "o1" is deleted while sessions row "s1" still refers to it$/));
  await apply([["set", "sessions", "s5", { org: null }], ["set", "events", ["s5", 1], { text: "c" }]]);
  // Deleting a session deletes its events, through the bundle's trigger.
  await apply([["delete", "sessions", "s1"]]);
  assert.deepEqual(await rows(), { orgs: [["o1", {}]], sessions: [["s5", { org: null }]], events: [[["s5", 1], { text: "c" }]] });

  // Writers racing a deletion: whichever commits first, no session is left referring to a missing org.
  for (let round = 0; round < 6; round++) {
    const org = `race${round}`;
    await apply([["set", "orgs", org, {}]]);
    const outcomes = await Promise.allSettled([
      ...Array.from({ length: 12 }, (_, n) => apply([["set", "sessions", `${org}-${n}`, { org }]])),
      apply([["delete", "orgs", org]]),
    ]);
    for (const outcome of outcomes) if (outcome.status === "rejected") violation(/./)(outcome.reason);
    const view = await rows();
    const orgNames = new Set(view.orgs.map(([key]) => key));
    for (const [key, value] of view.sessions) assert.ok(value.org === null || orgNames.has(value.org), `${key} refers to ${value.org}`);
    const deleted = outcomes.at(-1).status === "fulfilled";
    assert.equal(orgNames.has(org), !deleted);
    if (deleted) assert.ok(view.sessions.every(([, value]) => value.org !== org));
  }

  // The stored schema keeps them after every node restarts.
  const before = await rows();
  await restartAll();
  assert.deepEqual(await rows(), before);
  await assert.rejects(apply([["set", "sessions", "s6", { org: "nowhere" }]]), violation(/refers to orgs row "nowhere"/));
  await apply([["set", "events", ["s5", 2], { text: "d" }], ["delete", "sessions", "s5"]]);
  assert.deepEqual((await rows()).events, []);
  // Deploying code without them drops them: nothing is checked again.
  for (let n = 0; n < 100 && state.phase !== "collected"; n++) {
    state = (await admin().controlStagedDeployment({ operation: "collect", requestId: "staged", maxBytes: 64 * 1024 })).value;
  }
  assert.equal(state.phase, "collected");
  await admin().deploy(plain, { requestId: "plain-again", preparation: "blocking" });
  await apply([["set", "sessions", "s7", { org: "nowhere" }]]);
  console.log("PASS: references checked at deployment (blocking and staged activation), kept whole by mutations on every replica, cascades through triggers, racing writers, full restart, and dropped with the code");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  await transport.close();
  await cluster.close();
}
