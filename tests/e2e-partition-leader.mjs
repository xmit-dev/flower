// A partition's registered peer addresses are seeds, not a list of its leaders.
import assert from "node:assert/strict";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { LocalCluster } from "../bench/cluster.mjs";
import { FlowerAdmin, FlowerClient } from "../sdk/client.ts";
import { buildBundle } from "../sdk/bundle.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const group = "🌸-owners";
class PartitionCluster extends LocalCluster {
  _startNode(node) {
    // LocalCluster chooses all isolated ports before spawning its first node.
    // Scope these environment additions to this synchronous spawn only.
    const values = {
      FLOWER_GROUP: group, FLOWER_CATALOG_GROUP: group,
      FLOWER_GROUPS: JSON.stringify({ [group]: this.members.map(member => member.address) }),
    };
    const previous = Object.fromEntries(Object.keys(values).map(key => [key, process.env[key]]));
    Object.assign(process.env, values);
    try { super._startNode(node); }
    finally {
      for (const [key, value] of Object.entries(previous)) {
        if (value === undefined) delete process.env[key]; else process.env[key] = value;
      }
    }
  }
}
const cluster = new PartitionCluster({ nodes: 3,
  binary: process.env.E2E_FLOWER_BIN ? resolve(process.env.E2E_FLOWER_BIN) : join(root, "target/debug/flower"),
});
const signal = () => AbortSignal.timeout(20_000);
let watch;
const watching = new AbortController();
const interrupted = () => { watching.abort(); void cluster.close(); };
process.once("SIGINT", interrupted);
process.once("SIGTERM", interrupted);
try {
  await cluster.start();
  const seed = cluster.members.find(node => node.id !== cluster.leader.id);
  const sdk = new FlowerAdmin(seed.url, { adminToken: cluster.adminToken });
  // This deliberately omits the serving leader. Ping/create/activate must use
  // the seed's authenticated membership hint, as must application writes.
  await sdk.registerGroup({ id: group, addresses: [seed.address] }, { signal: signal() });
  await sdk.createPartition("tenant🌷", group, { requestId: "create", signal: signal() });
  const placement = await sdk.waitForPartition("tenant🌷", { timeoutMs: 30_000 });
  assert.equal(placement.status, "active");
  assert.deepEqual(placement.owner.addresses, [seed.address]);
  assert.ok(!placement.owner.addresses.includes(cluster.leader.address));
  const tenant = new FlowerClient(seed.url).partition("tenant🌷");
  const deployed = await sdk.partition("tenant🌷").deploy(await buildBundle(join(root, "examples/orders.ts")), { requestId: "deploy", signal: signal() });
  // order.total is materialized for each order: private maintenance commits its
  // one-time backfill marker. Settle it so the CAS revisions below are exact.
  const identity = async () => {
    const response = await fetch(`${seed.url}/partitions/${encodeURIComponent("tenant🌷")}/v1/identity`, {
      method: "POST", headers: { "content-type": "application/json" }, body: "{}", signal: signal(),
    });
    assert.equal(response.status, 200);
    return (await response.json()).revision;
  };
  for (const deadline = Date.now() + 20_000; await identity() === deployed.revision;) {
    assert.ok(Date.now() < deadline, "maintenance did not commit the materialization marker");
    await delay(50);
  }
  const settled = await identity();
  assert.equal(settled, deployed.revision + 1);
  const create = { orderId: "order", shippingCents: 5, lines: [{ id: "line", quantity: 1, unitCents: 7 }] };
  const creation = { requestId: "create-order", expectedRevision: settled };
  const created = await tenant.mutate("order.create", create, { ...creation, signal: signal() });
  assert.equal(created.revision, settled + 1);
  const updated = await tenant.call("order.updateLine", { lineId: "line", quantity: 2 }, { requestId: "update", expectedRevision: created.revision, signal: signal() });
  assert.equal(updated.revision, created.revision + 1);
  assert.equal(updated.value.total, 19);
  assert.deepEqual((await tenant.call("order.get", "order", { signal: signal() })).value, updated.value);
  await assert.rejects(tenant.mutate("order.updateLine", { lineId: "line", quantity: 3 }, {
    requestId: "stale", expectedRevision: 1, signal: signal(),
  }), error => error.status === 409 && error.code === "REVISION_CONFLICT");
  watch = tenant.watch("order.get", "order", { signal: watching.signal });
  assert.deepEqual((await watch.next()).value.value, updated.value);

  const membershipResponse = await fetch(seed.url + "/raft/membership", { headers: { authorization: `Bearer ${cluster.adminToken}` }, signal: signal() });
  const membership = await membershipResponse.json();
  const c = membership.compatibility;
  const headers = {
    authorization: `Bearer ${cluster.adminToken}`, "content-type": "application/json",
    "x-flower-compatibility": `raft${c.raftWire}-state${c.stateMachine}-snapshot${c.snapshotFormat}-value${c.valueFormat}-qjs${c.quickjsSha256}`,
    "x-flower-partition-leader-hop": "1", "x-flower-target-node-id": String(seed.id), "x-flower-target-address": seed.address,
    "x-flower-source-node-id": String(cluster.leader.id), "x-flower-source-address": cluster.leader.address,
  };
  const response = await fetch(seed.url + "/raft/partitions/invoke", { method: "POST", headers,
    body: JSON.stringify({ group, body: { partition: "tenant🌷", epoch: placement.epoch, operation: "mutate",
      input: { name: "order.updateLine", args: { lineId: "line", quantity: 999 }, requestId: "must-not-forward-twice" } } }), signal: signal() });
  assert.equal(response.status, 503, "a forwarded request reaching a follower never hops again");
  await response.text();
  assert.deepEqual((await tenant.query("order.get", "order", { signal: signal() })).value, updated.value);

  const watched = await tenant.mutate("order.updateLine", { lineId: "line", quantity: 3 }, {
    requestId: "watch-update", expectedRevision: updated.revision, signal: signal(),
  });
  assert.deepEqual((await watch.next()).value.value, watched.value);
  await watch.return(); watch = undefined;
  await cluster.crashLeaderAndRecover();
  // The SDK retains the same URL and catalog retains the same seed descriptor.
  const replay = await tenant.mutate("order.create", create, { ...creation, signal: signal() });
  assert.deepEqual(replay, { ...created, duplicate: true });
  const after = await tenant.mutate("order.updateLine", { lineId: "line", quantity: 4 }, {
    requestId: "after-election", expectedRevision: watched.revision, signal: signal(),
  });
  assert.equal(after.value.total, 33);
  watch = tenant.watch("order.get", "order", { signal: watching.signal });
  assert.deepEqual((await watch.next()).value.value, after.value);
  console.log("PASS: follower-only partition seeds create/deploy/mutate/call, exact CAS/replay, bounded one-hop routing, Unicode group identity, follower watch and same-seed failover");
} catch (error) {
  console.error(error);
  console.error(cluster.logTails());
  process.exitCode = 1;
} finally {
  watching.abort();
  await watch?.return().catch(() => {});
  await cluster.close();
  process.removeListener("SIGINT", interrupted);
  process.removeListener("SIGTERM", interrupted);
}
