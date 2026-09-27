// Passwordless accounts with passkeys. Each ceremony is a begin/finish pair of
// mutations: begin returns options for navigator.credentials.create() or get()
// and stores the challenge; finish verifies the browser's credential.toJSON()
// and consumes it, so every challenge is accepted at most once.
import { collection, define, fail, mutation, query, v } from "@flower-js/sdk";
import type { MutationContext } from "@flower-js/sdk";
import { base64url, nacl, sha256, webauthn } from "@flower-js/sdk/crypto";
import type { WebAuthnCredential } from "@flower-js/sdk/crypto";
import { scheduler } from "@flower-js/sdk/scheduler";

// Your site: the RP ID is its domain, and origins are exactly what browsers report.
const RP = { id: "garden.example", name: "Flower garden" };
const ORIGINS = ["https://garden.example"];
const CEREMONY_MS = 5 * 60_000;
const SESSION_MS = 7 * 24 * 60 * 60_000;

interface Account { handle: string; name: string; createdAt: number }
interface Passkey { account: string; credential: WebAuthnCredential; createdAt: number; lastUsedAt: number | null }
interface Ceremony { kind: "register" | "signIn"; challenge: string; account: { handle: string; name: string } | null; expiresAt: number }

const accounts = collection<Account>("accounts");
const passkeys = collection<Passkey>("passkeys").index("byAccount", ["account"]);
const ceremonies = collection<Ceremony>("passkeyCeremonies");
// Keyed by the token's SHA-256, so a leaked record cannot sign anyone in.
const sessions = collection<{ account: string; expiresAt: number }>("sessions");

const expire = mutation("internal.passkey.expire", { args: v.string() }, (ctx, id) => {
  ctx.delete(ceremonies, id);
  return null;
});
const cleanup = scheduler("passkeyCeremonyTimers", { expire });

function begin(ctx: MutationContext, kind: Ceremony["kind"], challenge: string, account: Ceremony["account"]): string {
  const id = base64url.encode(nacl.randomBytes(16));
  ctx.set(ceremonies, id, { kind, challenge, account, expiresAt: ctx.now() + CEREMONY_MS });
  cleanup.after(ctx, id, CEREMONY_MS, "expire", id);
  return id;
}

// A failed verification rolls this back too, so the user may retry until it expires.
function finish(ctx: MutationContext, id: string, kind: Ceremony["kind"]): Ceremony {
  const ceremony = ctx.get(ceremonies, id);
  if (!ceremony || ceremony.kind !== kind || ceremony.expiresAt <= ctx.now()) fail("CEREMONY_EXPIRED", "Start again");
  ctx.delete(ceremonies, id);
  cleanup.cancel(ctx, id);
  return ceremony;
}

const sessionKey = (token: string) => base64url.encode(sha256(new Uint8Array(Array.from(token, (character) => character.charCodeAt(0)))));
const ceremonyArgs = v.object({ ceremony: v.string({ min: 1, max: 64 }), response: v.json() });

// Signed in, this adds a passkey to your account; otherwise it opens a new one.
const registerBegin = mutation("passkey.register.begin", {
  access: "public", args: v.object({ name: v.optional(v.string({ min: 1, max: 64 })) }),
}, (ctx, args) => {
  const signedIn = ctx.principal();
  let account: { handle: string; name: string };
  if (signedIn) {
    account = ctx.get(accounts, signedIn.subject) ?? fail("ACCOUNT_NOT_FOUND", "Your account no longer exists");
  } else {
    if (!args.name) fail("NAME_REQUIRED", "Choose a name for the new account");
    if (ctx.get(accounts, args.name)) fail("NAME_TAKEN", "That name is taken; sign in to add a passkey to it");
    account = { handle: base64url.encode(nacl.randomBytes(16)), name: args.name };
  }
  const options = webauthn.registrationOptions({
    rp: RP,
    user: { id: account.handle, name: account.name },
    exclude: ctx.query(passkeys.by("byAccount").eq(account.name)).map(({ credential }) => ({ id: credential.id, transports: credential.transports })),
  });
  return { ceremony: begin(ctx, "register", options.challenge, { handle: account.handle, name: account.name }), options };
});

const registerFinish = mutation("passkey.register.finish", { access: "public", args: ceremonyArgs }, (ctx, args) => {
  const { challenge, account } = finish(ctx, args.ceremony, "register");
  const { credential } = webauthn.verifyRegistration(args.response, { challenge, origin: ORIGINS, rpId: RP.id });
  if (ctx.get(passkeys, credential.id)) fail("PASSKEY_EXISTS", "This passkey is already registered");
  const existing = ctx.get(accounts, account!.name);
  if (existing && existing.handle !== account!.handle) fail("NAME_TAKEN", "That name was taken meanwhile");
  if (existing && ctx.principal()?.subject !== existing.name) fail("UNAUTHENTICATED", "Sign in to add a passkey to this account");
  if (!existing) ctx.set(accounts, account!.name, { ...account!, createdAt: ctx.now() });
  ctx.set(passkeys, credential.id, { account: account!.name, credential, createdAt: ctx.now(), lastUsedAt: null });
  return { account: account!.name, passkey: credential.id, synced: credential.backupState };
});

// No allow list: the browser offers every passkey it holds for this site.
const signInBegin = mutation("passkey.signIn.begin", { access: "public" }, (ctx) => {
  const options = webauthn.authenticationOptions({ rpId: RP.id });
  return { ceremony: begin(ctx, "signIn", options.challenge, null), options };
});

const signInFinish = mutation("passkey.signIn.finish", { access: "public", args: ceremonyArgs }, (ctx, args) => {
  const { challenge } = finish(ctx, args.ceremony, "signIn");
  const id = (args.response as { id?: unknown } | null)?.id;
  const passkey = (typeof id === "string" ? ctx.get(passkeys, id) : null) ?? fail("PASSKEY_UNKNOWN", "This passkey isn't registered here");
  const account = ctx.get(accounts, passkey.account) ?? fail("ACCOUNT_NOT_FOUND", "The passkey's account no longer exists");
  const verified = webauthn.verifyAuthentication(args.response, {
    challenge, origin: ORIGINS, rpId: RP.id, credential: passkey.credential, userHandle: account.handle,
  });
  ctx.set(passkeys, passkey.credential.id, {
    ...passkey, credential: { ...passkey.credential, signCount: verified.signCount, backupState: verified.backupState }, lastUsedAt: ctx.now(),
  });
  const token = base64url.encode(nacl.randomBytes(32));
  ctx.set(sessions, sessionKey(token), { account: account.name, expiresAt: ctx.now() + SESSION_MS });
  return { account: account.name, token };
});

const me = query("account.me", { access: "authenticated" }, (ctx) => {
  const name = ctx.principal()!.subject;
  return {
    name,
    passkeys: ctx.query(passkeys.by("byAccount").eq(name)).map(({ credential, createdAt, lastUsedAt }) => ({
      id: credential.id, synced: credential.backupState, createdAt, lastUsedAt,
    })),
  };
});

const app = define({
  uses: [cleanup],
  collections: [accounts, passkeys, ceremonies, sessions],
  auth: {
    authenticate: (ctx, credentials) => {
      if (credentials === null) return null;
      const valid = typeof credentials === "string" && /^[A-Za-z0-9_-]{43}$/.test(credentials);
      const session = valid ? ctx.get(sessions, sessionKey(credentials)) : null;
      if (!session || session.expiresAt <= ctx.now()) fail("UNAUTHENTICATED", "Sign in again");
      return { subject: session.account };
    },
  },
  http: {
    "passkey.register.begin": registerBegin,
    "passkey.register.finish": registerFinish,
    "passkey.signIn.begin": signInBegin,
    "passkey.signIn.finish": signInFinish,
    "account.me": me,
  },
});
export default app;
