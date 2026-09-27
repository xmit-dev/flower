// Real SDK → QuickJS/Wasm → native crypto → replicated receipt integration.
import assert from "node:assert/strict";
import { createPublicKey, verify } from "node:crypto";
import { resolve } from "node:path";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";

const cluster = new LocalCluster({ nodes: 3, binary: resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower") });
async function call(url, path, body, admin = false) {
  const response = await fetch(url + path, {
    method: "POST", headers: { "content-type": "application/json", ...(admin ? { authorization: `Bearer ${cluster.adminToken}` } : {}) },
    body: JSON.stringify(body), signal: AbortSignal.timeout(20_000),
  });
  const value = await response.json();
  assert.equal(response.status, 200, JSON.stringify(value));
  return value;
}
try {
  await cluster.start();
  const follower = cluster.members.find(node => node.id !== cluster.leader.id).url;
  await call(follower, "/admin/deploy", { requestId: "crypto-deploy", bundle: await buildBundle(resolve("examples/crypto.ts")) }, true);
  const request = { name: "ticket.issue", args: { guest: "🌻", lifetimeSeconds: 60 }, requestId: "crypto-ticket" };
  const receipt = await call(follower, "/v1/mutate", request);
  assert.equal(receipt.value.signed.split(".").length, 3);
  assert.equal(receipt.value.encrypted.split(".").length, 5);
  const publicKey = createPublicKey({ format: "der", type: "spki",
    key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), Buffer.from(receipt.value.publicKey)]) });
  assert.ok(verify(null, Buffer.from(receipt.value.signed), publicKey, Buffer.from(receipt.value.signature)),
    "Node independently verifies the native NaCl signature returned through Raft");
  const next = await call(follower, "/v1/mutate", { ...request, requestId: "crypto-ticket-2" });
  assert.notEqual(next.value.encrypted, receipt.value.encrypted, "fresh invocations must not reuse COW snapshot entropy");
  for (const member of cluster.members) {
    const retry = await call(member.url, "/v1/mutate", request);
    assert.deepEqual(retry, { ...receipt, duplicate: true }, "retry returns the original random result");
    const checked = await call(member.url, "/v1/query", { name: "ticket.check", args: { signed: receipt.value.signed, encrypted: receipt.value.encrypted } });
    assert.deepEqual(checked.value.signed, checked.value.encrypted);
    assert.equal(checked.value.signed.sub, "🌻");
  }
  assert.deepEqual((await call(follower, "/v1/query", { name: "crypto.primitives", args: null })).value,
    { opened: [7, 8, 9], shared: true, precomputed: true, openAfter: [7, 8, 9], signed: [7, 8, 9], hashLength: 64, scalar: true });
  const denied = await fetch(follower + "/v1/query", { method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "crypto.randomQuery", args: null }), signal: AbortSignal.timeout(20_000) });
  // Native crypto refusals reach callers as the method's structured failure.
  const deniedError = (await denied.json()).error;
  assert.equal(denied.status, 422);
  assert.equal(deniedError.code, "EVALUATION_FAILED");
  assert.deepEqual(deniedError.failure, { code: "CRYPTO_RANDOM_FORBIDDEN", message: "system randomness is available only in mutations" });
  const short = await call(follower, "/v1/mutate", { ...request, args: { guest: "short", lifetimeSeconds: 1 }, requestId: "crypto-short" });
  const checkShort = () => fetch(follower + "/v1/query", { method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "ticket.check", args: { signed: short.value.signed, encrypted: short.value.encrypted } }), signal: AbortSignal.timeout(20_000) });
  assert.equal((await checkShort()).status, 200);
  await new Promise(resolve => setTimeout(resolve, 1100));
  const expired = await checkShort();
  assert.notEqual(expired.status, 200, "query cache cannot preserve expired token validation");
  const expiredError = (await expired.json()).error;
  assert.equal(expiredError.code, "EVALUATION_FAILED");
  assert.equal(expiredError.failure?.code, "CRYPTO_ERROR");
  assert.match(expiredError.failure?.message ?? "", /^JWT expired/);
  console.log("crypto E2E passed: native SDK surface, replicated random receipts, follower reads, time-aware validation");
} finally {
  await cluster.close();
}
