import assert from "node:assert/strict";
import { test } from "node:test";
import type { TestContext } from "node:test";
import { base64url, jwt, nacl, sha256, webauthn } from "./crypto.ts";
import { nacl as rootNaCl, jwt as rootJWT, webauthn as rootWebAuthn } from "./index.ts";

type Input = Uint8Array | string;
type Result = Uint8Array | string | boolean | null;
function bridge(t: TestContext, callback: (operation: number, parameter: number, ...inputs: Input[]) => Result): void {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "__flowerCrypto");
  Object.defineProperty(globalThis, "__flowerCrypto", { value: callback, configurable: true });
  t.after(() => {
    if (previous) Object.defineProperty(globalThis, "__flowerCrypto", previous);
    else Reflect.deleteProperty(globalThis, "__flowerCrypto");
    nacl.setPRNG(null);
  });
}

test("crypto imports are lazy and package root shares the same API", () => {
  assert.equal(rootNaCl, nacl);
  assert.equal(rootJWT, jwt);
  assert.equal(rootWebAuthn, webauthn);
  assert.throws(() => nacl.hash(new Uint8Array()), /only inside a Flower method/);
  assert.throws(() => nacl.hash("no implicit UTF-8" as any), TypeError);
  assert.throws(() => nacl.randomBytes(-1), TypeError);
  assert.throws(() => nacl.randomBytes(0x1_0000_0000), TypeError);
  assert.throws(() => nacl.randomBytes(1.5), TypeError);
  assert.throws(() => nacl.setPRNG(undefined as any), TypeError);
});

test("NaCl binary wrappers preserve view identity, argument order and result kinds", (t) => {
  const backing = new Uint8Array(128);
  const message = backing.subarray(7, 11), nonce = backing.subarray(16, 40);
  const key = backing.subarray(48, 80), signature = backing.subarray(48, 112);
  let observed: [number, number, ...Input[]] = [0, 0];
  let result: Result = new Uint8Array([9]);
  bridge(t, (op, parameter, ...inputs) => { observed = [op, parameter, ...inputs]; return result; });
  const calls: [number, Input[], () => unknown][] = [
    [1, [message, nonce, key], () => nacl.secretbox(message, nonce, key)],
    [2, [message, nonce, key], () => nacl.secretbox.open(message, nonce, key)],
    [3, [key, key], () => nacl.scalarMult(key, key)],
    [4, [key], () => nacl.scalarMult.base(key)],
    [5, [key, key], () => nacl.box.before(key, key)],
    [6, [message, nonce, key, key], () => nacl.box(message, nonce, key, key)],
    [7, [message, nonce, key, key], () => nacl.box.open(message, nonce, key, key)],
    [8, [message, signature], () => nacl.sign(message, signature)],
    [9, [signature, key], () => nacl.sign.open(signature, key)],
    [10, [message, signature], () => nacl.sign.detached(message, signature)],
    [11, [message, signature, key], () => nacl.sign.detached.verify(message, signature, key)],
    [15, [message], () => nacl.hash(message)],
    [16, [message, message], () => nacl.verify(message, message)],
  ];
  for (const [operation, inputs, call] of calls) {
    assert.equal(call(), result);
    assert.equal(observed[0], operation);
    assert.equal(observed[1], 0);
    assert.equal(observed.length, inputs.length + 2);
    inputs.forEach((input, index) => assert.equal(observed[index + 2], input));
  }
  result = null;
  assert.equal(nacl.secretbox.open(message, nonce, key), null);
  assert.equal(nacl.sign.open(signature, key), null);
  result = false;
  assert.equal(nacl.sign.detached.verify(message, signature, key), false);
  assert.equal(nacl.verify(message, message), false);
});

test("NaCl aliases and constants match the high-level API", () => {
  assert.equal(nacl.box.after, nacl.secretbox);
  assert.equal(nacl.box.open.after, nacl.secretbox.open);
  assert.deepEqual([nacl.secretbox.keyLength, nacl.secretbox.nonceLength, nacl.secretbox.overheadLength], [32, 24, 16]);
  assert.deepEqual([nacl.box.publicKeyLength, nacl.box.secretKeyLength, nacl.box.sharedKeyLength, nacl.box.nonceLength, nacl.box.overheadLength], [32, 32, 32, 24, 16]);
  assert.deepEqual([nacl.sign.publicKeyLength, nacl.sign.secretKeyLength, nacl.sign.seedLength, nacl.sign.signatureLength], [32, 64, 32, 64]);
  assert.deepEqual([nacl.scalarMult.scalarLength, nacl.scalarMult.groupElementLength, nacl.hash.hashLength], [32, 32, 64]);
});

test("key-pair wrappers isolate public buffers and erase temporary secret copies", (t) => {
  let operation = 0;
  let packed = new Uint8Array();
  let input: Uint8Array | undefined;
  bridge(t, (op, _parameter, ...inputs) => {
    operation = op;
    input = inputs[0] as Uint8Array;
    packed = new Uint8Array(op === 14 ? 64 : 96);
    packed.fill(1, 0, 32);
    packed.fill(2, 32);
    return packed;
  });
  const seed = new Uint8Array(32).fill(9);
  for (const [expected, call] of [
    [12, () => nacl.sign.keyPair.fromSeed(seed)],
    [13, () => nacl.sign.keyPair.fromSecretKey(seed)],
    [14, () => nacl.box.keyPair.fromSecretKey(seed)],
  ] as const) {
    const pair = call();
    assert.equal(operation, expected);
    assert.equal(input, seed);
    assert.equal(pair.publicKey.buffer.byteLength, 32);
    assert.equal(pair.secretKey.length, expected === 14 ? 32 : 64);
    assert.notEqual(pair.publicKey.buffer, pair.secretKey.buffer);
    assert.ok(pair.publicKey.every(value => value === 1));
    assert.ok(pair.secretKey.every(value => value === 2));
    assert.ok(packed.every(value => value === 0));
    assert.ok(seed.every(value => value === 9));
  }
  nacl.setPRNG((bytes, length) => { assert.equal(length, 32); bytes.fill(5); });
  nacl.sign.keyPair();
  assert.equal(operation, 12);
  assert.ok(input!.every(value => value === 0));
  nacl.box.keyPair();
  assert.equal(operation, 14);
});

test("native entropy and explicit PRNG overrides have separate paths", (t) => {
  let calls = 0;
  bridge(t, (op, parameter, ...inputs) => {
    assert.equal(op, 0); assert.equal(inputs.length, 0); calls += 1;
    return new Uint8Array(parameter).fill(7);
  });
  assert.deepEqual(nacl.randomBytes(3), new Uint8Array([7, 7, 7]));
  nacl.setPRNG((bytes, length) => { assert.equal(length, 2); bytes.fill(8); });
  assert.deepEqual(nacl.randomBytes(2), new Uint8Array([8, 8]));
  assert.equal(calls, 1);
  let failedOutput: Uint8Array | undefined;
  nacl.setPRNG(bytes => { failedOutput = bytes; bytes.fill(3); throw new Error("failed PRNG"); });
  assert.throws(() => nacl.randomBytes(2), /failed PRNG/);
  assert.deepEqual(failedOutput, new Uint8Array(2));
  nacl.setPRNG(null);
  assert.equal(nacl.randomBytes(0).length, 0);
  assert.equal(calls, 2);
});

test("host refusals become structured failures that keep a host code", (t) => {
  let refusal = "";
  bridge(t, () => { throw refusal ? new Error(refusal) : new TypeError("crypto inputs must be Uint8Array or primitive string"); });
  const key = new Uint8Array(32);
  const refused = (thrown: string, call: () => unknown, code: string, message: string) => {
    refusal = thrown;
    assert.throws(call, (error: Error & { code?: string; details?: unknown }) =>
      error.code === code && error.message === message && !("details" in error));
  };
  refused("CRYPTO_ERROR: JWT expired", () => jwt.verify("a.b.c", key, { algorithms: ["HS256"] }), "CRYPTO_ERROR", "JWT expired");
  refused("CRYPTO_ERROR: invalid JWT: bad header", () => jwt.verify("a.b.c", key, { algorithms: ["HS256"] }), "CRYPTO_ERROR", "invalid JWT: bad header");
  refused("CRYPTO_ERROR: CRYPTO_RANDOM_FORBIDDEN: system randomness is available only in mutations", () => nacl.randomBytes(8),
    "CRYPTO_RANDOM_FORBIDDEN", "system randomness is available only in mutations");
  refused("CRYPTO_ERROR: KEY_FORBIDDEN: Key must match a declaration in define({keys})", () => nacl.hash(key),
    "KEY_FORBIDDEN", "Key must match a declaration in define({keys})");
  // The bridge's own argument checks are programming errors, not refusals.
  refusal = "";
  assert.throws(() => nacl.hash(key), (error: Error & { code?: string }) => error instanceof TypeError && error.code === undefined);
});

test("JWT sends binary keys directly and serializes only finite plain JSON", (t) => {
  let observed: [number, number, ...Input[]] = [0, 0];
  bridge(t, (op, parameter, ...inputs) => {
    observed = [op, parameter, ...inputs];
    return op === 100 ? "signed.token.value" : '{"claims":{"sub":"alice"},"protectedHeader":{"alg":"HS256"}}';
  });
  const key = new Uint8Array(32), claims = { sub: "alice", exp: 2000 };
  assert.equal(jwt.sign(claims, key, { algorithm: "HS256" }), "signed.token.value");
  assert.deepEqual(observed, [100, 0, '{"exp":2000,"sub":"alice"}', key, '{"algorithm":"HS256","keyFormat":"raw"}']);
  assert.equal(observed[3], key);
  assert.equal(jwt.verify<{ sub: string }>("a.b.c", key, { algorithms: ["HS256"] }).claims.sub, "alice");
  assert.deepEqual(observed, [101, 0, "a.b.c", key, '{"algorithms":["HS256"],"keyFormat":"raw"}']);
  jwt.sign(claims, "PEM text", { algorithm: "RS256" });
  assert.match(observed[4] as string, /"keyFormat":"pem"/);
  jwt.sign(claims, key, { algorithm: "EdDSA", keyFormat: "der" });
  assert.match(observed[4] as string, /"keyFormat":"der"/);
  assert.throws(() => jwt.sign(claims, "PEM", { algorithm: "HS256", keyFormat: "raw" }), TypeError);
  assert.throws(() => jwt.sign({ exp: NaN }, key, { algorithm: "HS256" }), /finite/);
  assert.throws(() => jwt.sign([] as any, key, { algorithm: "HS256" }), /object/);
  assert.throws(() => jwt.verify("a.b.c", key, { get algorithms() { throw new Error("must not run"); } } as any), /accessors/);
});

test("JWT encryption uses native entropy unless a nonce is explicitly provided", (t) => {
  const key = new Uint8Array(32), explicit = new Uint8Array(12).fill(1);
  let calls: number[] = [], generated: Uint8Array | undefined;
  bridge(t, (op, parameter, ...inputs) => {
    calls.push(op);
    if (op === 0) { assert.equal(parameter, 12); generated = new Uint8Array(12).fill(2); return generated; }
    if (op === 102) {
      assert.equal(inputs[1], key);
      assert.equal((inputs[2] as Uint8Array).length, 12);
      assert.deepEqual(JSON.parse(inputs[3] as string), { kid: "key-1" });
      return "protected..nonce.ciphertext.tag";
    }
    assert.equal(op, 103);
    return '{"claims":{"exp":2000},"protectedHeader":{"alg":"dir","enc":"A256GCM"}}';
  });
  nacl.setPRNG(() => { throw new Error("JWT must not use NaCl PRNG override"); });
  assert.equal(jwt.encrypt({ exp: 2000 }, key, { nonce: explicit, kid: "key-1" }), "protected..nonce.ciphertext.tag");
  assert.deepEqual(calls, [102]);
  assert.ok(explicit.every(byte => byte === 1));
  calls = [];
  jwt.encrypt({ exp: 2000 }, key, { kid: "key-1" });
  assert.deepEqual(calls, [0, 102]);
  assert.ok(generated!.every(byte => byte === 0));
  assert.equal(jwt.decrypt("a..b.c.d", key).protectedHeader.enc, "A256GCM");
  assert.throws(() => jwt.encrypt({}, key, { nonce: new Uint8Array(11) }), /12 bytes/);
  assert.throws(() => jwt.encrypt({}, new Uint8Array(31)), /32 bytes/);
  assert.throws(() => jwt.encrypt({}, key, { get nonce() { throw new Error("must not run"); } } as any), /accessors/);
});

test("base64url matches RFC 4648's url alphabet and accepts one encoding per byte string", () => {
  const text = (value: string) => new Uint8Array(Array.from(value, (character) => character.charCodeAt(0)));
  for (const [plain, encoded] of [["", ""], ["f", "Zg"], ["fo", "Zm8"], ["foo", "Zm9v"], ["foob", "Zm9vYg"], ["fooba", "Zm9vYmE"], ["foobar", "Zm9vYmFy"]]) {
    assert.equal(base64url.encode(text(plain)), encoded);
    assert.deepEqual(base64url.decode(encoded), text(plain));
  }
  assert.equal(base64url.encode(new Uint8Array([0xfb, 0xff])), "-_8");
  assert.deepEqual(base64url.decode("Zg=="), text("f"));
  assert.deepEqual(base64url.decode("Zm8="), text("fo"));
  for (let length = 0; length < 70; length++) {
    const bytes = Uint8Array.from({ length }, (_, index) => (index * 151 + length * 7) & 0xff);
    assert.deepEqual(base64url.decode(base64url.encode(bytes)), bytes);
  }
  for (const invalid of ["Z", "Zh", "Zm9=", "Zg=", "a+b/", "Zm 9v", "Zg==="]) {
    assert.throws(() => base64url.decode(invalid), TypeError, invalid);
  }
  assert.throws(() => base64url.decode(7 as any), TypeError);
  assert.throws(() => base64url.encode("text" as any), TypeError);
});

test("sha256 hands bytes to the native digest", (t) => {
  const digest = new Uint8Array(32).fill(9), message = new Uint8Array([1, 2, 3]);
  bridge(t, (op, parameter, ...inputs) => {
    assert.deepEqual([op, parameter, inputs.length], [17, 0, 1]);
    assert.equal(inputs[0], message);
    return digest;
  });
  assert.equal(sha256(message), digest);
  assert.equal(sha256.hashLength, 32);
  assert.throws(() => sha256("text" as any), TypeError);
});

test("passkey ceremony options carry a fresh challenge and safe defaults", (t) => {
  let draws = 0;
  bridge(t, (op, parameter) => {
    assert.deepEqual([op, parameter], [0, 32]);
    return new Uint8Array(32).fill(++draws);
  });
  const user = { id: base64url.encode(new Uint8Array(16).fill(4)), name: "ada@example.com" };
  const created = webauthn.registrationOptions({ rp: { id: "example.com", name: "Example" }, user, exclude: [{ id: "AQID", transports: ["internal"] }] });
  assert.deepEqual(created, {
    challenge: base64url.encode(new Uint8Array(32).fill(1)),
    rp: { id: "example.com", name: "Example" },
    user: { ...user, displayName: "ada@example.com" },
    pubKeyCredParams: [{ type: "public-key", alg: -8 }, { type: "public-key", alg: -7 }, { type: "public-key", alg: -257 }],
    timeout: 300_000,
    excludeCredentials: [{ type: "public-key", id: "AQID", transports: ["internal"] }],
    authenticatorSelection: { residentKey: "required", requireResidentKey: true, userVerification: "required" },
    attestation: "none",
  });
  const custom = webauthn.registrationOptions({ rp: { id: "example.com", name: "Example" }, user: { ...user, displayName: "Ada" },
    residentKey: "preferred", userVerification: "preferred", algorithms: [-7], timeoutMs: 60_000 });
  assert.equal(custom.challenge, base64url.encode(new Uint8Array(32).fill(2)));
  assert.deepEqual([custom.user.displayName, custom.pubKeyCredParams, custom.timeout, custom.authenticatorSelection],
    ["Ada", [{ type: "public-key", alg: -7 }], 60_000, { residentKey: "preferred", requireResidentKey: false, userVerification: "preferred" }]);
  assert.deepEqual(webauthn.authenticationOptions({ rpId: "example.com" }), {
    challenge: base64url.encode(new Uint8Array(32).fill(3)), rpId: "example.com", timeout: 300_000, userVerification: "required", allowCredentials: [],
  });
  assert.deepEqual(webauthn.authenticationOptions({ rpId: "example.com", allow: [{ id: "AQID" }] }).allowCredentials, [{ type: "public-key", id: "AQID" }]);

  const init = { rp: { id: "example.com", name: "Example" }, user };
  for (const [change, message] of [
    [{ user: { ...user, id: base64url.encode(new Uint8Array(65)) } }, /1 to 64 bytes/],
    [{ user: { ...user, id: "" } }, /user.id must be a nonempty string/],
    [{ algorithms: [-65535] }, /supported COSE identifiers/],
    [{ residentKey: "sometimes" }, /residentKey/],
    [{ userVerification: "maybe" }, /userVerification/],
    [{ timeoutMs: 0 }, /timeoutMs/],
    [{ exclude: [{ id: "AQID", extra: true }] }, /does not accept "extra"/],
    [{ attestation: "direct" }, /does not accept "attestation"/],
  ] as const) {
    assert.throws(() => webauthn.registrationOptions({ ...init, ...change } as any), message);
  }
});

test("passkey verification sends the response and exact expectations to native code", (t) => {
  const calls: [number, unknown, unknown][] = [];
  let refusal: string | null = null;
  bridge(t, (op, _parameter, response, expected) => {
    calls.push([op, JSON.parse(response as string), JSON.parse(expected as string)]);
    if (refusal) throw new Error(refusal);
    return op === 110 ? '{"credential":{"id":"AQID","signCount":0},"userVerified":true,"attestation":{"format":"none"}}'
      : '{"credentialId":"AQID","signCount":7,"userVerified":true,"backupEligible":true,"backupState":true,"userHandle":null}';
  });
  const response = { id: "AQID", rawId: "AQID", type: "public-key", response: { clientDataJSON: "e30" } };
  const challenge = base64url.encode(new Uint8Array(32).fill(5));
  const registered = webauthn.verifyRegistration(response, { challenge, origin: "https://example.com", rpId: "example.com" });
  assert.equal(registered.attestation.format, "none");
  const stored = { id: "AQID", publicKey: "pQECAyYgASFY", algorithm: -7, signCount: 6, transports: ["internal"], backupEligible: true, backupState: true, aaguid: "" };
  const signedIn = webauthn.verifyAuthentication(response, { challenge, origin: ["https://example.com", "android:apk-key-hash:x"], rpId: "example.com",
    userVerification: "preferred", credential: stored, userHandle: "dXNlcg" });
  assert.equal(signedIn.signCount, 7);
  assert.deepEqual(calls, [
    [110, response, { challenge, origins: ["https://example.com"], rpId: "example.com", userVerification: "required", algorithms: [-8, -7, -257] }],
    [111, response, { challenge, origins: ["https://example.com", "android:apk-key-hash:x"], rpId: "example.com", userVerification: "preferred",
      credential: { id: "AQID", publicKey: "pQECAyYgASFY", signCount: 6, userHandle: "dXNlcg" } }],
  ]);

  refusal = "CRYPTO_ERROR: WEBAUTHN_COUNTER: the signature counter did not increase, so the authenticator may be cloned";
  assert.throws(() => webauthn.verifyAuthentication(response, { challenge, origin: "https://example.com", rpId: "example.com", credential: stored }),
    { code: "WEBAUTHN_COUNTER", message: "the signature counter did not increase, so the authenticator may be cloned" });
  refusal = "CRYPTO_ERROR: WebAuthn challenge does not match";
  assert.throws(() => webauthn.verifyRegistration(response, { challenge, origin: "https://example.com", rpId: "example.com" }),
    { code: "CRYPTO_ERROR", message: "WebAuthn challenge does not match" });
  refusal = null;

  const valid = { challenge, origin: "https://example.com", rpId: "example.com", credential: stored };
  for (const [change, message] of [
    [{ origin: [] }, /origin must be/],
    [{ challenge: "" }, /challenge must be a nonempty string/],
    [{ credential: { ...stored, signCount: -1 } }, /uint32/],
    [{ credential: { ...stored, signCount: 2 ** 32 } }, /uint32/],
    [{ userHandle: "!" }, /Invalid base64url/],
    [{ expected: true }, /does not accept "expected"/],
  ] as const) {
    assert.throws(() => webauthn.verifyAuthentication(response, { ...valid, ...change } as any), message);
  }
  assert.throws(() => webauthn.verifyRegistration(response, { challenge, origin: "https://example.com", rpId: "example.com", algorithms: [] as any }), /COSE/);
  assert.equal(calls.length, 4);
});

