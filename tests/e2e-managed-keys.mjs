// Three real Raft members, encrypted provisioning and disposable QuickJS cells.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHmac, randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { setTimeout as delay } from "node:timers/promises";
import { join, resolve } from "node:path";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";

const binary = resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower");
const directory = await mkdtemp(join(tmpdir(), "flower-managed-e2e-"));
const wrappingFile = join(directory, "wrapping.key");
const previousWrapping = process.env.FLOWER_KEYRING_FILE;
const previousWrappingFiles = process.env.FLOWER_KEYRING_PREVIOUS_FILES;
delete process.env.FLOWER_KEYRING_PREVIOUS_FILES;
await writeFile(wrappingFile, randomBytes(32), { mode: 0o600 });
process.env.FLOWER_KEYRING_FILE = wrappingFile;
const cluster = new LocalCluster({ nodes: 3, binary });
let requestNumber = 0;
async function request(url, path, body, { admin = false, status = 200 } = {}) {
  const response = await fetch(url + path, {
    method: "POST", headers: { "content-type": "application/json", ...(admin ? { authorization: `Bearer ${cluster.adminToken}` } : {}) },
    body: JSON.stringify(body), signal: AbortSignal.timeout(25_000),
  });
  const value = await response.json();
  assert.equal(response.status, status, JSON.stringify(value));
  return value;
}
const keys = (url, operation, fields = {}) => request(url, "/admin/keys", {
  operation, ...(["list", "cache"].includes(operation) ? {} : { requestId: `key-${++requestNumber}` }), ...fields,
}, { admin: true });
const query = (url, name, args = null, status = 200) => request(url, "/v1/query", { name, args }, { status });
// The data events one watch sends within ms: its snapshot, then one per new result.
async function watchEvents(url, name, args, ms) {
  const controller = new AbortController();
  const response = await fetch(url + "/v1/watch", {
    method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream" },
    body: JSON.stringify({ name, args }), signal: controller.signal,
  });
  if (response.status !== 200) assert.fail(`watch ${name}: ${response.status} ${await response.text()}`);
  const timer = setTimeout(() => controller.abort(), ms);
  let text = "";
  try {
    for await (const chunk of response.body) text += Buffer.from(chunk).toString();
  } catch (error) {
    if (!controller.signal.aborted) throw error;
  } finally { clearTimeout(timer); }
  return text.split("\n").filter(line => line.startsWith("data:")).length;
}
try {
  const entry = join(directory, "managed.ts");
  await writeFile(entry, `
import { define, key, mutation, query, derive, collection } from ${JSON.stringify(resolve("sdk/index.ts"))};
import { jwt, publicKey, keyVersion, nacl } from ${JSON.stringify(resolve("sdk/crypto.ts"))};
const sessions = key("sessions", {algorithm:"Ed25519",usages:["sign","verify","publicKey"]});
const encryption = key("encryption", {algorithm:"A256GCM",usages:["encrypt","decrypt"]});
const imported = key("imported", {algorithm:"HS256",usages:["sign","verify"]});
const mailbox = key("mailbox", {algorithm:"X25519",usages:["derive","encrypt","decrypt","publicKey"]});
const visible = collection("visible");
const status = query("status",{consistency:"replica-local"},ctx=>ctx.get(visible,"status"));
const fingerprint = derive("fingerprint", () => Array.from(publicKey(sessions)));
const issue = mutation("issue", (ctx) => {
  ctx.set(visible,"status","sunny");
  const claims = {sub:"🌻",exp:ctx.now()/1000+600};
  return {signed:jwt.sign(claims,sessions),encrypted:jwt.encrypt(claims,encryption),mac:jwt.sign(claims,imported)};
});
const check = query("check", {consistency:"replica-local"}, (_ctx,args:any) => ({signed:jwt.verify(args.signed,sessions).claims,encrypted:jwt.decrypt(args.encrypted,encryption).claims}));
const stamp = query("stamp", (ctx,token:string) => {jwt.verify(token,sessions);return ctx.clock();});
const stampSealed = query("stampSealed", (ctx,token:string) => {jwt.decrypt(token,encryption);return ctx.clock();});
const stampDerived = query("stampDerived", (ctx) => {ctx.get(fingerprint,null);return ctx.clock();});
const stampStatus = query("stampStatus", (ctx) => {ctx.get(visible,"status");return ctx.clock();});
const touch = mutation("touch", (ctx) => {ctx.set(visible,"other",ctx.now());return null;});
const materialize = mutation("materialize", (ctx) => {ctx.materialize(fingerprint,null);return ctx.get(fingerprint,null);});
const current = query("current", {consistency:"replica-local"}, (ctx) => ctx.get(fingerprint,null));
const forge = query("forge", () => jwt.sign({sub:"bad",exp:9e9},{kind:"key",name:"undeclared",algorithm:"Ed25519",usages:["sign"]}));
const primitive = query("primitive", () => {
 const msg = new Uint8Array([7,8,9]);
 for(let i=0;i<64;i++) {
   const signature = nacl.sign.detached(msg,sessions);
   if(!nacl.sign.detached.verify(msg,signature,sessions))throw new Error("signature mismatch");
 }
 return true;
});
const verifyThenSign = query("verifyThenSign", (_ctx,token) => {
 jwt.verify(token,sessions);
 return jwt.sign({sub:"must-not-sign",exp:9e9},sessions);
});
const shared = query("shared", () => {
 const peer=nacl.box.keyPair.fromSecretKey(new Uint8Array(32).fill(7));
 const handle=nacl.box.before(peer.publicKey,mailbox);
 const nonce=new Uint8Array(24), message=new Uint8Array([1,2,3]);
 const cipher=nacl.box.after(message,nonce,handle);
 if(nacl.box.open.after(cipher,nonce,handle)?.[0]!==1)throw new Error("shared key mismatch");
 let blocked=false;try{JSON.stringify(handle)}catch{blocked=true}
 if(!blocked||Reflect.ownKeys(handle).length||!Object.isFrozen(handle)||handle.token!==undefined)throw new Error("observable shared key");
 return {cipher:Array.from(cipher)};
});
const replayShared = query("replayShared", (_ctx,args) => globalThis.__flowerCrypto(201,0,args,new Uint8Array([1]),new Uint8Array(24)));
const pack = query("pack", () => {
 const version=keyVersion(mailbox), peer=nacl.box.keyPair.fromSecretKey(new Uint8Array(32).fill(7));
 return {version:version.version,cipher:Array.from(nacl.box(new Uint8Array([6,7,8]),new Uint8Array(24),peer.publicKey,version))};
});
const unpack = query("unpack", (_ctx,args) => {
 const version=keyVersion(mailbox,args.version), peer=nacl.box.keyPair.fromSecretKey(new Uint8Array(32).fill(7));
 return Array.from(nacl.box.open(new Uint8Array(args.cipher),new Uint8Array(24),peer.publicKey,version));
});
const historicalWrite = query("historicalWrite", (_ctx,args) => {
 const version=keyVersion(mailbox,args.version), peer=nacl.box.keyPair.fromSecretKey(new Uint8Array(32).fill(7));
 return Array.from(nacl.box(new Uint8Array([6,7,8]),new Uint8Array(24),peer.publicKey,version));
});
export default define({keys:[sessions,encryption,imported,mailbox],definitions:[fingerprint],http:{issue,check,stamp,stampSealed,stampDerived,stampStatus,touch,materialize,current,forge,primitive,status,verifyThenSign,shared,replayShared,pack,unpack,historicalWrite}});
`);
  await cluster.start();
  const follower = cluster.members.find(node => node.id !== cluster.leader.id).url;
  await request(follower, "/admin/keys", { operation: "list" }, { status: 401 });
  await request(follower, "/admin/keys", { operation: "import", name: "bad", requestId: "raw", algorithm: "HS256", key: "plaintext" }, { admin: true, status: 400 });
  const generation = { operation: "generate", name: "session-signing", algorithm: "Ed25519", requestId: "generate-once" };
  const generated = await request(follower, "/admin/keys", generation, { admin: true });
  for (const member of cluster.members) {
    assert.deepEqual(await request(member.url, "/admin/keys", generation, { admin: true }), { ...generated, duplicate: true });
  }
  await request(follower, "/admin/keys", { ...generation, name: "different" }, { admin: true, status: 409 });
  await keys(follower, "generate", { name: "invoice", algorithm: "A256GCM" });
  await keys(follower, "generate", { name: "mailbox", algorithm: "X25519" });
  await keys(follower, "bind", { name: "mailbox", key: "mailbox", usages: ["derive", "encrypt", "decrypt", "publicKey"] });
  const secret = Buffer.from("flower-e2e-import-secret".padEnd(32, "!"));
  assert.equal(secret.length, 32);
  const sealing = spawnSync(binary, ["key", "seal", "--wrapping-key-file", wrappingFile, "--format", "raw"], { input: secret, encoding: "utf8" });
  assert.equal(sealing.status, 0, sealing.stderr);
  const sealed = JSON.parse(sealing.stdout);
  assert.ok(!sealing.stdout.includes(secret.toString()));
  await keys(follower, "import", { name: "external", algorithm: "HS256", sealed });
  await keys(follower, "bind", { name: "sessions", key: "session-signing", usages: ["sign", "verify", "publicKey"] });
  await keys(follower, "bind", { name: "encryption", key: "invoice", usages: ["encrypt", "decrypt"] });
  await keys(follower, "bind", { name: "imported", key: "external", usages: ["sign", "verify"] });
  await request(follower, "/admin/deploy", { requestId: "managed-deploy", bundle: await buildBundle(entry) }, { admin: true });
  const issueRequest = { name: "issue", args: null, requestId: "issue-once" };
  const receipt = await request(follower, "/v1/mutate", issueRequest);
  const [header, payload, mac] = receipt.value.mac.split(".");
  assert.equal(createHmac("sha256", secret).update(`${header}.${payload}`).digest("base64url"), mac);
  for (const member of cluster.members) {
    assert.deepEqual(await request(member.url, "/v1/mutate", issueRequest), { ...receipt, duplicate: true });
    const result = await query(member.url, "check", receipt.value);
    assert.deepEqual(result.value.signed, result.value.encrypted);
    assert.equal(result.value.signed.sub, "🌻");
    assert.equal((await query(member.url, "primitive")).value, true);
  }
  // Verifying with a managed key wakes a watch at the token's expiry, like a raw
  // key; the key catalog wakes it when it changes. Neither polls the clock.
  assert.equal(await watchEvents(follower, "stamp", receipt.value.signed, 1_500), 1, "managed-key watches must not poll");
  await query(follower, "forge", null, 422);
  const materialized = await request(follower, "/v1/mutate", { name: "materialize", args: null, requestId: "materialize" });
  // A stored value that resolved a key is no clock: a watch reading it doesn't
  // poll, one reading something else sleeps through unrelated writes, and
  // decryption declares its token's expiry.
  const [derivedEvents, statusEvents, sealedEvents] = await Promise.all([
    watchEvents(follower, "stampDerived", null, 1_500),
    watchEvents(follower, "stampStatus", null, 1_500),
    watchEvents(follower, "stampSealed", receipt.value.encrypted, 1_500),
    delay(500).then(() => request(follower, "/v1/mutate", { name: "touch", args: null, requestId: "touch" })),
  ]);
  assert.deepEqual({ derivedEvents, statusEvents, sealedEvents }, { derivedEvents: 1, statusEvents: 1, sealedEvents: 1 });
  await keys(follower, "rotate", { name: "session-signing" });
  for (const member of cluster.members) {
    assert.notDeepEqual((await query(member.url, "current")).value, materialized.value, "rotation invalidates materialized key dependencies");
    await query(member.url, "check", receipt.value); // Retained old version verifies.
  }
  const next = await request(follower, "/v1/mutate", { ...issueRequest, requestId: "issue-next" });
  assert.notEqual(JSON.parse(Buffer.from(next.value.signed.split(".")[0], "base64url")).kid,
    JSON.parse(Buffer.from(receipt.value.signed.split(".")[0], "base64url")).kid);
  await keys(follower, "bind", { name: "sessions", key: "session-signing", usages: ["verify"] });
  await query(follower, "verifyThenSign", next.value.signed, 422); // A verification cache hit must not grant signing.
  await keys(follower, "bind", { name: "sessions", key: "session-signing", usages: ["sign", "verify", "publicKey"] });
  const shared = await query(follower, "shared");
  await query(follower, "replayShared", shared.value, 422);
  await query(follower, "replayShared", {kind:"sharedKey",token:"1"}, 422);
  const packed = (await query(follower, "pack")).value;
  await keys(follower, "rotate", {name:"mailbox"});
  for(const member of cluster.members) assert.deepEqual((await query(member.url, "unpack", packed)).value, [6,7,8]);
  await query(follower, "historicalWrite", packed, 422); // Prepared shared contexts never escape their callback.
  await keys(follower, "bind", { name: "mailbox", key: "mailbox", usages: ["derive"] });
  await query(follower, "shared", null, 422); // Derivation permission does not grant encrypt/decrypt.
  await keys(follower, "bind", { name: "mailbox", key: "mailbox", usages: ["derive", "encrypt", "decrypt", "publicKey"] });
  await keys(follower, "revoke", { name: "session-signing", version: 1 });
  for (const member of cluster.members) {
    await query(member.url, "check", receipt.value, 422);
    await query(member.url, "check", next.value);
  }
  for (const member of cluster.members) {
    const stats = await keys(member.url, "cache");
    assert.ok(stats.value.loads > 0, "cache metrics inspect each reader node");
    assert.ok(stats.value.hits > 0, "native contexts are reused across evaluations");
  }
  const wrongFile = join(directory, "wrong-wrapping.key");
  await writeFile(wrongFile, randomBytes(32), { mode: 0o600 });
  const locked = cluster.members.find(node => node.id !== cluster.leader.id);
  async function restart(node, keyFile) {
    node.process.intentional = true;
    node.process.child.kill("SIGTERM");
    await node.process.exited;
    process.env.FLOWER_KEYRING_FILE = keyFile;
    cluster._startNode(node);
    await cluster._until("restart managed-key node", async () => {
      try { return (await fetch(node.url + "/health")).ok; } catch { return false; }
    });
  }
  await restart(locked, wrongFile);
  assert.equal((await query(locked.url, "status")).value,"sunny","unrelated source queries stay available on a locked replica");
  const lockedResponse = await fetch(locked.url + "/v1/query", {
    method:"POST", headers:{"content-type":"application/json"},
    body:JSON.stringify({name:"current",args:null}), signal:AbortSignal.timeout(25_000),
  });
  assert.notEqual(lockedResponse.status, 200, "locked replica cannot serve materialized secret-dependent values");
  assert.match(JSON.stringify(await lockedResponse.json()), /wrapping|unlock|key/i);
  await restart(locked, wrappingFile);
  await query(locked.url, "current");
  await keys(follower, "retire", {name:"mailbox",version:2});
  await query(follower, "pack", null, 422);
  assert.deepEqual((await query(follower, "unpack", packed)).value, [6,7,8]);
  await keys(follower, "destroy", {name:"mailbox",version:1});
  await query(follower, "unpack", packed, 422);
  const destroyed = await keys(follower, "list");
  assert.equal(destroyed.value.keys.mailbox.versions[0].destroyed, true);
  assert.equal(destroyed.value.keys.mailbox.versions[0].wrappingId, null);
  await keys(follower, "rotate", {name:"mailbox"});
  await query(follower, "pack");
  // Rotate the external wrapping key on every node, retaining the old key only
  // for the rolling transition and DEK rewrap. Encrypted material stays native.
  const nextWrappingFile = join(directory, "next-wrapping.key");
  await writeFile(nextWrappingFile, randomBytes(32), {mode:0o600});
  process.env.FLOWER_KEYRING_PREVIOUS_FILES = JSON.stringify([wrappingFile]);
  for(const member of cluster.members) { await restart(member, nextWrappingFile); await cluster.discoverLeader(); }
  const beforeRewrap = await keys(cluster.url, "list");
  for(const name of Object.keys(beforeRewrap.value.keys)) await keys(cluster.url, "rewrap", {name});
  const afterRewrap = await keys(cluster.url, "list");
  assert.notEqual(afterRewrap.value.keys.invoice.versions[0].wrappingId, beforeRewrap.value.keys.invoice.versions[0].wrappingId);
  delete process.env.FLOWER_KEYRING_PREVIOUS_FILES;
  for(const member of cluster.members) { await restart(member, nextWrappingFile); await cluster.discoverLeader(); }
  for(const member of cluster.members) await query(member.url, "check", next.value);
  await keys(follower, "unbind", { name: "sessions" });
  for (const member of cluster.members) await query(member.url, "current", null, 422);
  const listing = await keys(follower, "list");
  assert.ok(!JSON.stringify(listing).includes("ciphertext"), "operator metadata does not export encrypted/private material");
  await cluster.crashLeaderAndRecover();
  assert.deepEqual(await request(cluster.url, "/admin/keys", generation, { admin: true }), { ...generated, duplicate: true });
  for (const member of cluster.members) {
    const bytes = await readFile(join(member.directory, "flower.redb"));
    for (const pattern of [secret, Buffer.from(secret.toString("base64")), Buffer.from(JSON.stringify([...secret]))]) {
      assert.equal(bytes.indexOf(pattern), -1, "database/log/receipt persistence contains no imported private bytes");
    }
  }
  await keys(cluster.url, "bind", { name: "sessions", key: "session-signing", usages: ["sign", "verify", "publicKey"] });
  const isolated = cluster.members.find(node => node.id !== cluster.leader.id);
  await query(isolated.url, "current");
  for (const member of cluster.members) {
    if (member === isolated) continue;
    member.process.intentional = true;
    member.process.child.kill("SIGTERM");
    await member.process.exited;
  }
  await query(isolated.url, "current", null, 503); // Declared replica-local cannot bypass fresh key policy.
  console.log("managed keys E2E passed: encrypted import, follower admin routing, replication/retry/restart, managed JWT/NaCl, rotation, revocation, locked-node refusal, fresh policy under quorum loss and materialized dependency invalidation");
} finally {
  await cluster.close();
  if (previousWrapping === undefined) delete process.env.FLOWER_KEYRING_FILE;
  else process.env.FLOWER_KEYRING_FILE = previousWrapping;
  if (previousWrappingFiles === undefined) delete process.env.FLOWER_KEYRING_PREVIOUS_FILES;
  else process.env.FLOWER_KEYRING_PREVIOUS_FILES = previousWrappingFiles;
  await rm(directory, { recursive: true, force: true });
}
