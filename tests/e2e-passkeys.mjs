// Browser-shaped passkey ceremonies → SDK → QuickJS/Wasm → native WebAuthn
// verification. The software authenticator signs with Node's OpenSSL, which is
// independent of the server's AWS-LC.
import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, randomBytes, sign } from "node:crypto";
import { resolve } from "node:path";
import { LocalCluster } from "../bench/cluster.mjs";
import { buildBundle } from "../sdk/bundle.ts";
import { FlowerAdmin, FlowerClient, FlowerError } from "../sdk/index.ts";

const RP_ID = "garden.example", ORIGIN = "https://garden.example";
const b64 = (bytes) => Buffer.from(bytes).toString("base64url");
const sha256 = (bytes) => createHash("sha256").update(bytes).digest();

// Authenticators emit canonical CBOR; these fixtures need maps, integers, text and bytes.
function cbor(value) {
  const head = (major, n) => Buffer.from(n < 24 ? [(major << 5) | n] : n < 256 ? [(major << 5) | 24, n] : [(major << 5) | 25, n >> 8, n & 255]);
  if (typeof value === "number") return value >= 0 ? head(0, value) : head(1, -1 - value);
  if (typeof value === "string") return Buffer.concat([head(3, Buffer.byteLength(value)), Buffer.from(value)]);
  if (value instanceof Uint8Array) return Buffer.concat([head(2, value.length), value]);
  return Buffer.concat([head(5, value.size), ...[...value].flatMap(([key, entry]) => [cbor(key), cbor(entry)])]);
}

class Authenticator {
  credentials = new Map();
  constructor(algorithm, { counter = false } = {}) {
    this.algorithm = algorithm;
    this.counter = counter ? 0 : null;
  }

  create(options, { origin = ORIGIN } = {}) {
    assert.equal(options.rp.id, RP_ID);
    assert.equal(options.attestation, "none");
    assert.ok(options.pubKeyCredParams.some(({ alg }) => alg === this.algorithm));
    const { publicKey, privateKey } = this.algorithm === -7 ? generateKeyPairSync("ec", { namedCurve: "P-256" })
      : this.algorithm === -8 ? generateKeyPairSync("ed25519") : generateKeyPairSync("rsa", { modulusLength: 2048 });
    const jwk = publicKey.export({ format: "jwk" }), part = (name) => Buffer.from(jwk[name], "base64url");
    const cose = cbor(new Map(this.algorithm === -7 ? [[1, 2], [3, -7], [-1, 1], [-2, part("x")], [-3, part("y")]]
      : this.algorithm === -8 ? [[1, 1], [3, -8], [-1, 6], [-2, part("x")]] : [[1, 3], [3, -257], [-1, part("n")], [-2, part("e")]]));
    const id = randomBytes(16);
    this.credentials.set(b64(id), { privateKey, handle: options.user.id });
    const length = Buffer.alloc(2);
    length.writeUInt16BE(id.length);
    // UP | UV | BE | BS | AT, a zero AAGUID, then the credential.
    const authData = this.#data(0x5d, Buffer.alloc(16), length, id, cose);
    const clientDataJSON = JSON.stringify({ type: "webauthn.create", challenge: options.challenge, origin, crossOrigin: false });
    return {
      id: b64(id), rawId: b64(id), type: "public-key", authenticatorAttachment: "platform", clientExtensionResults: {},
      response: {
        clientDataJSON: b64(clientDataJSON), transports: ["internal", "hybrid"],
        attestationObject: b64(cbor(new Map([["fmt", "none"], ["attStmt", new Map()], ["authData", authData]]))),
      },
    };
  }

  get(options, { origin = ORIGIN, counter } = {}) {
    const [id, { privateKey, handle }] = [...this.credentials].at(-1);
    if (this.counter !== null) this.counter++;
    const authData = this.#data(0x1d, counter ?? this.counter ?? 0);
    const clientDataJSON = JSON.stringify({ type: "webauthn.get", challenge: options.challenge, origin, crossOrigin: false });
    const signed = Buffer.concat([authData, sha256(clientDataJSON)]);
    return {
      id, rawId: id, type: "public-key", authenticatorAttachment: "platform", clientExtensionResults: {},
      response: {
        clientDataJSON: b64(clientDataJSON), authenticatorData: b64(authData), userHandle: handle,
        // Node signs ECDSA in DER and RSA with PKCS#1 v1.5, as authenticators do.
        signature: b64(sign(this.algorithm === -8 ? null : "sha256", signed, privateKey)),
      },
    };
  }

  #data(flags, ...rest) {
    const counter = Buffer.alloc(4);
    if (typeof rest[0] === "number") counter.writeUInt32BE(rest.shift());
    else counter.writeUInt32BE(this.counter ?? 0);
    return Buffer.concat([sha256(RP_ID), Buffer.from([flags]), counter, ...rest]);
  }
}

const cluster = new LocalCluster({ nodes: 3, binary: resolve(process.env.E2E_FLOWER_BIN ?? "target/debug/flower") });
let client;
const call = async (name, args, credentials) => (await client.mutate(name, args, credentials === undefined ? {} : { credentials })).value;
const failsWith = (promise, code, message) => assert.rejects(promise,
  (error) => error instanceof FlowerError && (error.failure?.code ?? error.code) === code && (!message || message.test(error.failure?.message ?? error.message)));

async function register(authenticator, args, credentials) {
  const { ceremony, options } = await call("passkey.register.begin", args, credentials);
  return { options, registered: await call("passkey.register.finish", { ceremony, response: authenticator.create(options) }, credentials) };
}

async function signIn(authenticator, respond = (options) => authenticator.get(options)) {
  const { ceremony, options } = await call("passkey.signIn.begin", null);
  assert.deepEqual(options.allowCredentials, []);
  return call("passkey.signIn.finish", { ceremony, response: respond(options) });
}

try {
  await cluster.start();
  client = new FlowerClient(cluster.url);
  const admin = new FlowerAdmin(cluster.url, { adminToken: cluster.adminToken });
  await admin.deploy(await buildBundle(resolve("docs/passkeys.ts")), { requestId: "passkeys-deploy" });

  // Every default algorithm opens an account, signs in, and reaches authenticated methods.
  for (const [algorithm, name] of [[-7, "ada"], [-8, "grace"], [-257, "hopper"]]) {
    const phone = new Authenticator(algorithm);
    const { options, registered } = await register(phone, { name });
    assert.deepEqual(options.pubKeyCredParams.map(({ alg }) => alg), [-8, -7, -257]);
    assert.deepEqual(options.authenticatorSelection, { residentKey: "required", requireResidentKey: true, userVerification: "required" });
    assert.deepEqual([registered.account, registered.synced], [name, true]);
    const session = await signIn(phone);
    assert.equal(session.account, name);
    const me = (await client.query("account.me", null, { credentials: session.token })).value;
    assert.deepEqual(me.passkeys.map(({ id, synced }) => [id, synced]), [[registered.passkey, true]]);
  }
  await failsWith(client.query("account.me"), "UNAUTHENTICATED");
  await failsWith(client.query("account.me", null, { credentials: b64(randomBytes(32)) }), "UNAUTHENTICATED", /Sign in again/);

  // A challenge is accepted once, and only for its own ceremony and origin.
  const phone = new Authenticator(-7);
  await register(phone, { name: "katherine" });
  const { ceremony, options } = await call("passkey.signIn.begin", null);
  const response = phone.get(options);
  await failsWith(call("passkey.signIn.finish", { ceremony, response: phone.get(options, { origin: "https://evil.example" }) }),
    "CRYPTO_ERROR", /origin is not allowed/);
  const other = await call("passkey.signIn.begin", null);
  await failsWith(call("passkey.signIn.finish", { ceremony: other.ceremony, response }), "CRYPTO_ERROR", /challenge does not match/);
  // Failed attempts roll back, so the genuine response still completes its ceremony, once.
  const session = await call("passkey.signIn.finish", { ceremony, response });
  assert.equal(session.account, "katherine");
  await failsWith(call("passkey.signIn.finish", { ceremony, response }), "CEREMONY_EXPIRED");

  // Adding a passkey needs the account's session; its existing passkeys are excluded.
  await failsWith(call("passkey.register.begin", { name: "katherine" }), "NAME_TAKEN");
  const laptop = new Authenticator(-8);
  const added = await register(laptop, {}, session.token);
  assert.equal(added.options.excludeCredentials.length, 1);
  assert.equal((await signIn(laptop)).account, "katherine");
  const pending = await call("passkey.register.begin", {}, session.token);
  await failsWith(call("passkey.register.finish", { ceremony: pending.ceremony, response: new Authenticator(-7).create(pending.options) }),
    "UNAUTHENTICATED", /Sign in to add/);
  const me = (await client.query("account.me", null, { credentials: session.token })).value;
  assert.equal(me.passkeys.length, 2);

  // A counting security key that repeats a counter may be cloned.
  const key = new Authenticator(-7, { counter: true });
  await register(key, { name: "dorothy" });
  assert.equal((await signIn(key)).account, "dorothy");
  await failsWith(signIn(key, (options) => key.get(options, { counter: 1 })), "WEBAUTHN_COUNTER", /may be cloned/);
  assert.equal((await signIn(key)).account, "dorothy");
  console.log("passkeys E2E passed: ES256/EdDSA/RS256 ceremonies, single-use challenges, origins, sessions, counters");
} finally {
  await cluster.close();
}
