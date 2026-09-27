import assert from "node:assert/strict";
import { test } from "node:test";
import { define, key, canonicalJson, publicKey, nacl, jwt } from "./index.ts";
import { keyVersion } from "./crypto.ts";
import { FlowerAdmin } from "./client.ts";
import { main } from "./cli.ts";

test("key declarations are deterministic public metadata and require explicit manifest admission", () => {
  const sessions = key("sessions", { algorithm: "Ed25519", usages: ["verify", "sign"] });
  assert.deepEqual(sessions, { kind: "key", name: "sessions", algorithm: "Ed25519", usages: ["sign", "verify"] });
  assert.ok(Object.isFrozen(sessions)); assert.ok(Object.isFrozen(sessions.usages));
  assert.equal(define().keys, undefined);
  assert.deepEqual(define({ keys: [sessions] }).keys, [sessions]);
  assert.deepEqual(define({ keys: [sessions, key("sessions", { algorithm: "Ed25519", usages: ["sign", "verify"] })] }).keys, [sessions]);
  assert.throws(() => define({ keys: [sessions, key("sessions", { algorithm: "Ed25519", usages: ["sign"] })] }), /Conflicting key/);
  assert.throws(() => key("", { algorithm: "Ed25519", usages: ["sign"] }), /nonempty/);
  assert.throws(() => key("a", { algorithm: "unknown", usages: ["sign"] } as any), /supported algorithm/);
  assert.throws(() => key("a", { algorithm: "Ed25519", usages: [] }), /nonempty usages/);
  assert.throws(() => key("a", { algorithm: "Ed25519", usages: ["sign", "sign"] }), /distinct/);
  assert.throws(() => define({ keys: [{ ...sessions, privateBytes: "forbidden" } as any] }), /declaration/);
  assert.throws(() => key("a", { get algorithm() { throw new Error("must not execute"); }, usages: ["sign"] } as any), /accessors/);
});

test("managed crypto sends descriptors and borrowed binary views without exporting private keys", (t) => {
  const sessions = key("sessions", { algorithm: "Ed25519", usages: ["sign", "verify", "publicKey"] });
  const boxes = key("boxes", { algorithm: "X25519", usages: ["derive", "encrypt", "decrypt"] });
  const aes = key("sealed", { algorithm: "A256GCM", usages: ["encrypt", "decrypt"] });
  const calls: { request: any; inputs: (string | Uint8Array)[] }[] = [];
  class NativeHandle { readonly kind = "sharedKey"; toJSON() { throw new TypeError("invocation-local"); } }
  const handle = Object.freeze(new NativeHandle());
  Object.defineProperty(globalThis, "__flowerCrypto", { configurable: true, value: (op: number, parameter: number, request: string, ...inputs: (string | Uint8Array)[]) => {
    assert.equal(parameter, 0);
    if (op === 201 || op === 202) {
      assert.equal(request as unknown, handle);
      calls.push({ request: { operation: op }, inputs });
      return new Uint8Array([1]);
    }
    assert.equal(op, 200); assert.equal(inputs.length, 3);
    const parsed = JSON.parse(request); calls.push({ request: parsed, inputs });
    if (parsed.operation === "nacl.box.before") return handle;
    if (parsed.operation === "key.version") return "flower.key-id.2";
    if (parsed.operation === "jwt.verify" || parsed.operation === "jwt.decrypt") return '{"claims":{"sub":"alice"},"protectedHeader":{"alg":"EdDSA","kid":"native-version"}}';
    if (parsed.operation.startsWith("jwt.")) return "native.token";
    return new Uint8Array([1, 2, 3]);
  } });
  t.after(() => Reflect.deleteProperty(globalThis, "__flowerCrypto"));
  const message = new Uint8Array([7, 8, 9]).subarray(1), nonce = new Uint8Array(24), peer = new Uint8Array(32);
  publicKey(sessions); assert.equal(calls.at(-1)!.request.operation, "key.publicKey");
  nacl.sign.detached(message, sessions);
  assert.equal(calls.at(-1)!.request.operation, "nacl.sign.detached");
  assert.equal(calls.at(-1)!.inputs[0], message);
  const shared = nacl.box.before(peer, boxes);
  assert.equal(shared.kind, "sharedKey"); assert.ok(Object.isFrozen(shared));
  assert.throws(() => JSON.stringify(shared), /invocation-local/);
  assert.throws(() => canonicalJson(shared), /plain objects/);
  nacl.box.after(message, nonce, shared);
  assert.equal(calls.at(-1)!.request.operation, 201);
  assert.equal("token" in shared, false);
  const version = keyVersion(sessions);
  assert.deepEqual(version, { kind: "keyVersion", key: sessions, version: "flower.key-id.2" });
  assert.ok(Object.isFrozen(version));
  publicKey(version); assert.equal(calls.at(-1)!.request.version, "flower.key-id.2");
  assert.deepEqual(calls.at(-1)!.request.key, sessions);
  assert.throws(() => keyVersion(sessions, "flower.key-id.01"), /Invalid managed key version/);
  nacl.box.after(message, nonce, shared);
  assert.equal(calls.at(-1)!.inputs[1], nonce);
  nacl.box(message, nonce, peer, boxes); assert.equal(calls.at(-1)!.inputs[2], peer);
  assert.throws(() => nacl.scalarMult(boxes as any, peer), /Uint8Array/);
  assert.throws(() => nacl.sign.keyPair.fromSecretKey(sessions as any), /Uint8Array/);
  assert.equal(jwt.sign({ sub: "alice" }, sessions), "native.token");
  assert.deepEqual(calls.at(-1)!.request.options, {});
  assert.equal(jwt.verify("a.b.c", sessions).claims.sub, "alice");
  jwt.encrypt({ sub: "alice" }, aes, { nonce: new Uint8Array(12) });
  assert.equal(calls.at(-1)!.request.operation, "jwt.encrypt");
  assert.equal(jwt.decrypt("a..b.c.d", aes).claims.sub, "alice");
  assert.throws(() => jwt.sign({}, sessions, { kid: "caller-chosen" } as any), /determined by the binding/);
  assert.throws(() => jwt.verify("a.b.c", sessions, { keyFormat: "pem" } as any), /determined by the binding/);
});

test("operator key methods preserve identity, scope and sealed import boundaries", async () => {
  const requests: { url: string; headers: Record<string, string>; body: any }[] = [];
  const catalog = { domain: "domain", revision: 1, keys: {}, bindings: {} };
  const client = new FlowerAdmin("http://seed:7101", { adminToken: "admin", fetch: async (url, init) => {
    requests.push({ url, headers: init.headers, body: JSON.parse(init.body) });
    return new Response(JSON.stringify({ revision: 2, value: catalog, duplicate: false }));
  } }).partition("tenant/🌻");
  await client.keyGenerate("sessions", "Ed25519", { requestId: "generate-once" });
  await client.keyBind("signer", "sessions", ["sign", "verify"], { requestId: "bind-once" });
  await client.keyRotate("sessions", { requestId: "rotate-once" });
  await client.keyRevoke("sessions", { version: 1, requestId: "revoke-once" });
  await client.keyUnbind("signer", { requestId: "unbind-once" });
  await client.keyList(); await client.keyCacheStats();
  const sealed = { version: 1 as const, wrappingId: "a".repeat(43), nonce: "a".repeat(16), ciphertext: "a".repeat(24) };
  await client.keyImport("external", "Ed25519", sealed, { requestId: "import-once" });
  assert.ok(requests.every(request => request.url.endsWith("/partitions/tenant%2F%F0%9F%8C%BB/admin/keys") && request.headers.authorization === "Bearer admin"));
  assert.deepEqual(requests.map(request => request.body.operation), ["generate", "bind", "rotate", "revoke", "unbind", "list", "cache", "import"]);
  assert.equal(requests[0].body.requestId, "generate-once");
  assert.deepEqual(requests[1].body, { operation: "bind", name: "signer", key: "sessions", usages: ["sign", "verify"], requestId: "bind-once" });
  assert.equal(requests[3].body.version, 1);
  assert.deepEqual(requests[7].body.sealed, sealed);
  const count = requests.length;
  for (const invalid of ["private bytes", { key: "secret" }, { ...sealed, privateKey: "secret" }, { ...sealed, nonce: "wrong" }]) {
    await assert.rejects(client.keyImport("external", "Ed25519", invalid as any), /encrypted envelope/);
  }
  assert.equal(requests.length, count, "plaintext-shaped imports must fail before network I/O");
});

test("key CLI maps explicit commands and rejects misplaced flags", async (t) => {
  const calls: unknown[][] = [];
  t.mock.method(process.stdout, "write", (() => true) as any);
  t.mock.method(FlowerAdmin.prototype, "keyGenerate", async function (this: FlowerAdmin, ...args: unknown[]) { calls.push([this.url, ...args]); return {} as any; });
  t.mock.method(FlowerAdmin.prototype, "keyBind", async (...args: unknown[]) => { calls.push(args); return {} as any; });
  await main(["key", "generate", "sessions", "--algorithm", "Ed25519", "--partition", "north", "--request-id", "one"]);
  assert.deepEqual(calls[0], ["http://127.0.0.1:7101/partitions/north", "sessions", "Ed25519", { requestId: "one" }]);
  await main(["key", "bind", "alias", "sessions", "--usages", "verify,sign", "--request-id", "two"]);
  assert.deepEqual(calls[1], ["alias", "sessions", ["sign", "verify"], { requestId: "two" }]);
  await assert.rejects(main(["key", "generate", "x"]), /requires --algorithm/);
  await assert.rejects(main(["key", "list", "--bits", "2048"]), /does not apply/);
  await assert.rejects(main(["key", "revoke", "x", "--version", "1.5"]), /positive safe integer/);
  await assert.rejects(main(["key", "seal"]), /native Flower executable/);
  await assert.rejects(main(["query", "x", "--algorithm", "RSA"]), /only to key/);
  await assert.rejects(main(["init", "--partition", "north"]), /methods, deploy and key/);
});
