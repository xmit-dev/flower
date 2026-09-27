// A small ticket booth demonstrating native crypto. Put your application's
// authorization policy in front of ticket.issue before using it as an issuer.
// This exercises raw keys: its JSON records are not an encrypted key store.
// SECRETS.md proposes operator-provisioned, non-exportable managed keys.
import { collection, define, mutation, query } from "../sdk/index.ts";
import { jwt, nacl } from "../sdk/crypto.ts";

const keys = collection<{ token: number[]; encryption: number[]; signing: number[] }>("private.ticket.keys");

const issue = mutation("internal.ticket.issue", (ctx, args: { guest: string; lifetimeSeconds: number }) => {
  if (!args || typeof args.guest !== "string" || !args.guest
    || !Number.isFinite(args.lifetimeSeconds) || args.lifetimeSeconds <= 0) {
    throw new Error("A guest and positive lifetimeSeconds are required");
  }
  let key = ctx.get(keys, "current");
  if (key === null) {
    key = { token: Array.from(nacl.randomBytes(32)), encryption: Array.from(nacl.randomBytes(32)), signing: Array.from(nacl.sign.keyPair().secretKey) };
    ctx.set(keys, "current", key);
  }
  const secret = new Uint8Array(key.token);
  const now = ctx.now() / 1000;
  const claims = { sub: args.guest, iss: "flower-garden", aud: "greenhouse", iat: now, exp: now + args.lifetimeSeconds };
  const signed = jwt.sign(claims, secret, { algorithm: "HS256" });
  const encrypted = jwt.encrypt(claims, new Uint8Array(key.encryption));
  // Compact JWTs contain only ASCII, so their bytes need no Unicode encoder.
  const message = new Uint8Array(Array.from(signed, character => character.charCodeAt(0)));
  const pair = nacl.sign.keyPair.fromSecretKey(new Uint8Array(key.signing));
  const signature = nacl.sign.detached(message, pair.secretKey);
  return { signed, encrypted, publicKey: Array.from(pair.publicKey), signature: Array.from(signature) };
});

const check = query("internal.ticket.check", (ctx, args: { signed: string; encrypted: string }) => {
  const key = ctx.get(keys, "current");
  if (key === null) throw new Error("No tickets issued yet");
  const secret = new Uint8Array(key.token);
  const validation = { issuer: "flower-garden", audience: ["greenhouse"] };
  const signed = jwt.verify(args.signed, secret, { ...validation, algorithms: ["HS256"] });
  const encrypted = jwt.decrypt(args.encrypted, new Uint8Array(key.encryption), validation);
  return { signed: signed.claims, encrypted: encrypted.claims };
});

const primitives = query("internal.crypto.primitives", () => {
  const alice = nacl.box.keyPair.fromSecretKey(new Uint8Array(32).fill(1));
  const bob = nacl.box.keyPair.fromSecretKey(new Uint8Array(32).fill(2));
  const nonce = new Uint8Array(24), message = new Uint8Array([7, 8, 9]);
  const shared = nacl.box.before(bob.publicKey, alice.secretKey);
  const cipher = nacl.box(message, nonce, bob.publicKey, alice.secretKey);
  const signer = nacl.sign.keyPair.fromSeed(new Uint8Array(32).fill(3));
  const signed = nacl.sign(message, signer.secretKey);
  return {
    opened: Array.from(nacl.box.open(cipher, nonce, alice.publicKey, bob.secretKey)!),
    shared: nacl.verify(shared, nacl.box.before(alice.publicKey, bob.secretKey)),
    precomputed: nacl.verify(cipher, nacl.box.after(message, nonce, shared)),
    openAfter: Array.from(nacl.box.open.after(cipher, nonce, shared)!),
    signed: Array.from(nacl.sign.open(signed, signer.publicKey)!),
    hashLength: nacl.hash(message).length,
    scalar: nacl.verify(nacl.scalarMult.base(alice.secretKey), alice.publicKey),
  };
});

const randomQuery = query("internal.crypto.randomQuery", () => Array.from(nacl.randomBytes(32)));

export default define({ http: {
  "ticket.issue": issue, "ticket.check": check,
  "crypto.primitives": primitives, "crypto.randomQuery": randomQuery,
} });
