# Managed keys and native crypto reuse

Flower keeps managed private keys below the QuickJS/Wasm boundary. TypeScript
holds public capability declarations; Rust resolves operator bindings, checks
policy, selects immutable versions and reuses native cryptographic contexts.
This is implemented with a mounted wrapping-key provider. Wrapping-key rewrap and mounted previous providers support rotation. KMS/HSM providers, private-key export and general exportable secrets are not implemented.

The [Field Guide](https://flower.xmit.dev/guide/crypto.html) gives a complete usage
walkthrough. The [SDK reference](https://flower.xmit.dev/reference/keys.html)
covers every declaration, overload, operator method and option.

## Declare; then grant

```ts
import { define, key, mutation, query, v } from "@flower-js/sdk";
import { jwt } from "@flower-js/sdk/crypto";

const sessions = key("sessions", {
  algorithm: "Ed25519", usages: ["sign", "verify"],
});
const issue = mutation("session.issue", { args: v.string({ min: 1 }) }, (ctx, user) =>
  // Gate issuance with define({ auth }) and an access policy before exposing it.
  jwt.sign({ sub: user, iss: "shop", aud: "shop-api",
    exp: ctx.now() / 1000 + 900 }, sessions));
const check = query("session.check", { args: v.string() }, (_ctx, token) =>
  jwt.verify(token, sessions, { issuer: "shop", audience: ["shop-api"] }).claims);

export default define({ keys: [sessions], http: { issue, check } });
```

`key` returns a frozen `{kind:"key", name, algorithm, usages}` descriptor. It
contains no material and grants nothing by itself. Include every descriptor in
`define({keys})` or a component's `keys`; the operator must separately bind
each alias to a stored key and approved usages. Native code checks the
declaration, algorithm and binding on every use, even if JavaScript fabricates
a descriptor. Identical repeated declarations merge; conflicting declarations
of one name and unsupported algorithm/usage combinations fail validation.

A managed key can also authenticate callers. `jwtBearer` verifies bearer
credentials natively in the application's authorization hook and adds its key to
`define({keys})` automatically:

```ts
import { define, jwtBearer, key, query } from "@flower-js/sdk";

const sessions = key("sessions", { algorithm: "Ed25519", usages: ["sign", "verify"] });
const me = query("me", (ctx) => ctx.principal());

export default define({
  auth: { authenticate: jwtBearer({ key: sessions, issuer: "shop", audience: ["shop-api"] }) },
  http: { me },
});
```

The binding decides the verification algorithm; an optional `algorithms` list
must agree with it. Missing credentials are anonymous, and an invalid token
fails authorization with `UNAUTHENTICATED`. See the README's access section for
per-method policy. Like any application with an authorization hook or managed
keys, this one serves even replica-local queries and watches from fresh
quorum-backed state. Its watches wake on writes, key policy changes and token
expiry, not on a timer.

A key catalog belongs to one logical database, with a generated immutable
security domain. Named partitions have separate catalogs; no application or
tenant string supplied by guest code can select a different catalog. Domain and
key identities survive movement between physical Raft groups. Handles remain
unresolved in both static and per-invocation bundle initialization.

## Algorithms and operations

| Managed algorithm | Supported operations and permissions |
| --- | --- |
| Ed25519 | JWT EdDSA and NaCl sign/verify; `sign`, `verify`, `publicKey`. NaCl retains strict Ed25519 verification. |
| P256 | JWT ES256; deterministic RFC 6979 signing; `sign`, `verify`, `publicKey`. |
| RSA | JWT RS256; `sign`, `verify`, `publicKey`. Generation defaults to 2048 bits and accepts 2048, 3072, 4096 or 8192. Rotation preserves the current size unless overridden. |
| HS256 | JWT HS256; `sign`, `verify`. |
| A256GCM | Compact dir/A256GCM JWE; `encrypt`, `decrypt`. |
| XSalsa20Poly1305 | NaCl secretbox/open and aliases; `encrypt`, `decrypt`. |
| X25519 | NaCl box/open (`encrypt`, `decrypt`); box.before (`derive`); publicKey/scalarMult.base (`publicKey`). |

`publicKey(handle)` returns raw 32-byte Ed25519/X25519 public keys, or SPKI DER
for P256/RSA. Symmetric keys have no public component. Managed private keys have
no export operation. Managed scalarMult and key-pair helpers are unsupported
because those byte APIs return secret material.

Managed `nacl.box.before(peerPublic, handle)` returns an opaque `SharedKey`, not
bytes. It works only in the current callback, with box.after/open.after (the
secretbox/open aliases). Each use rechecks the original declaration and binding
for encrypt/decrypt, in addition to the derive permission used to create it.
The QuickJS native handle has no observable token or private bytes and rejects serialization. A hidden invocation-local slot authenticates every bridge use; plain objects and handles from another callback cannot forge that authority.

Raw-key overloads remain supported. Managed JWT sign/verify infer the trusted
algorithm from the binding. Optional algorithm restrictions must agree exactly;
callers cannot supply keyFormat or override the version kid. Standard claim
checks, expiration required by default, issuer/audience options and custom-claim
validation responsibilities remain unchanged.

NaCl requires explicit unique 24-byte nonces. JWT encryption accepts an explicit
unique 12-byte nonce or generates one through mutation-only OS randomness. The
NaCl guest `setPRNG` hook does not change operator generation or automatic JWT
nonces. Flower uses host OS entropy, not a seeded consensus RNG or a PRNG whose
state persists through reusable QuickJS snapshots. The leader commits the
resulting data changes; followers need no matching random stream. Explicit-input
HMAC/Ed25519 signatures are deterministic and P256 uses RFC 6979; provider-local
randomness such as RSA blinding remains native. Opaque shared handles expose no OS-random identity to pure callbacks. Explicit inputs make pure cryptographic
operations usable in queries. No managed-key
operation may resolve a key while initializing a bundle.

## Provisioning and operator API

Configure a protected 32-byte wrapping key outside the Raft data directory:

```sh
umask 077
openssl rand 32 > /secure/flower-wrapping.key
# Set on each authorized server before starting it:
export FLOWER_KEYRING_FILE=/secure/flower-wrapping.key
```

Unix files must have no group/other permission bits (for example mode 0600).
Back up this file separately: the ciphertext in a Raft backup is insufficient
to recover lost wrapping material. The provider is read once at startup;
changing the file requires restarting the process. Invalid configured files
fail startup. Omitted configuration allows the server to run with keys locked.

The SDK CLI uses FLOWER_URL and FLOWER_ADMIN_TOKEN (or the corresponding flags):

```sh
flower key generate session-signing --algorithm Ed25519 --request-id key-create
flower key bind sessions session-signing --usages sign,verify --request-id key-bind
flower deploy sessions.ts --request-id app-deploy
flower key rotate session-signing --request-id rotate-2
flower key revoke session-signing --version 1 --request-id revoke-1
flower key list
flower key cache
```

Add `--partition north` to select a named database. `key unbind ALIAS` removes
an operator binding without deleting the key. `key revoke NAME` without a
version revokes every existing version. RSA generation/rotation accepts
`--bits N`. Unsupported flags and raw secret fields are rejected. RSA generation can be
slow: native preparation holds serialized operator admission and blocks that
logical database writer until it finishes. No deadline interrupts the native
provider during generation. An HTTP timeout leaves the result uncertain; retry
with the same request ID.

To import existing private material, use the **native runtime executable**
locally; the npm SDK CLI is also called flower but does not implement seal:

```sh
/path/to/native/flower key seal --wrapping-key-file /secure/flower-wrapping.key \
  --format pem < private.pem > sealed.json
flower key import imported-signing sealed.json --algorithm Ed25519 \
  --request-id import-signing
```

The native seal command consumes plaintext stdin and emits only an authenticated
encrypted envelope. Its format is raw, pem or der. SDK import accepts this JSON
file, or `-` for encrypted JSON stdin. Private bytes are never sent as ordinary
HTTP fields or forwarded in plaintext. The server authenticates the envelope,
validates/canonicalizes the imported key, and reseals it under the catalog
domain. Sealed imports protect key material, not the entire connection. Enable
[native TLS](TLS.md) to protect operator credentials and metadata; the default
h2c mode still requires a trusted network or external transport protection.

| Import algorithm | Accepted material |
| --- | --- |
| HS256 | Raw secret, at least 32 bytes. |
| A256GCM / XSalsa20Poly1305 / X25519 | Exactly 32 raw bytes. |
| Ed25519 | Raw seed32 or validated NaCl secret64; private PKCS#8 PEM/DER. |
| P256 | Raw valid scalar32; private PKCS#8 PEM/DER. |
| RSA | Private PKCS#8 or PKCS#1 PEM/DER, 2048–8192 bits. |

Public-only imports, certificates, SEC1 EC PRIVATE KEY wrappers and encrypted
PEM are not supported. Raw public-key JWT/NaCl APIs remain available. Standard
canonical DER/parser-stack guards apply before parsing untrusted key material.

`FlowerAdmin` exposes keyList, keyCacheStats, keyGenerate, keyImport, keyBind,
keyUnbind, keyRotate, keyRetire, keyRevoke, keyDestroy and keyRewrap. Use `admin.partition(id)` for a named
catalog. HTTP equivalents are POST `/admin/keys` and
`/partitions/{id}/admin/keys`, with an operator bearer token. Requests use
`operation: list|cache|generate|import|bind|unbind|rotate|retire|revoke|destroy|rewrap`; mutations
require requestId. The SDK generates one if omitted. Preserve the original ID
and exact contents after an uncertain response to retrieve its committed
receipt instead of creating another version.

Responses use `{revision,value,duplicate}`. Public catalog value contains its
own policy revision, stable domain, key IDs/algorithms/active versions, version
revocation/kid metadata and bindings; no envelopes or material. Catalog revision
is distinct from the outer database revision. Cache is exceptional: it reports
the ingress physical node's global statistics, even under a partition prefix,
without forwarding or requiring quorum. Its outer revision is physical local
snapshot metadata. Cache fields are entries, bytes, budgetBytes, hits, misses,
loads and evictions.

## Storage and admission

Envelope encryption uses a fresh per-version AES-256 data-encryption key and
nonce to protect material, and the mounted AES-256 wrapping key plus a separate
nonce to protect that DEK. Associated metadata authenticates domain, key UUID,
immutable version, algorithm and purpose. Copying encrypted versions between
security domains fails authentication. Ordinary collections/scans/bundles cannot
read the reserved catalog.

Generation/import and encryption happen before Raft proposal. The committed log
contains encrypted catalog changes, derived results and retry receipts.
Followers apply changes without rerunning JavaScript or generating key material.
Discarded/uncertain preparations can have generated unused keys; a committed
request receipt identifies the only successful result.

When the application declares any managed keys, replica-local aliases also use
fresh quorum-backed state, exactly as an authorization hook makes them.
Linearizable queries and watches, the default, already do: each obtains a read
fence, one leader heartbeat round shared by every concurrent read on its node,
not a read of the whole cluster. Public cache probes also defer to the fully
admitted path. This trades stale-read availability for current revocation
policy. Each invocation
pins one admitted snapshot of declarations and policy. Concurrent revocation
cannot cancel every already-admitted callback or retract already returned
bytes; it affects operations admitted against subsequent committed policy.

Managed resolution observes the catalog as a reactive dependency. Binding,
rotation and revocation atomically recompute dependent materialized values.
Secret-dependent query results bypass result caching; writer revision checks
reject speculative preparation against conflicting policy updates. A failed
crypto dependency becomes an error rather than an obsolete materialized answer.
Readiness follows the derived values actually read, so a source-only query can
still serve on a locked node. Reusing a managed materialized value conservatively
loads all unrevoked catalog versions; cold admission can cost more than loading
only the directly used key.

A query or authorization that resolves a managed key keeps its dependency
certificate: every certificate observes the declarations and catalog, and a
catalog write wakes every watch. Its watch therefore wakes on writes to what it
read, on key policy changes and, after JWT verification or decryption, when the
token expires or activates, exactly as with a raw key. Only the key opens JWE
claims, so a failed decryption can't say when it would succeed and reads the
clock as `ctx.now()` does. A materialized value that resolves a managed key is
an ordinary dependency: queries that read it are certified but uncached, and
other queries stay cacheable.

Managed JWT/JWE emits an authenticated immutable version kid. Verification and
decryption select only an allowed, unrevoked version of the explicitly bound
key. Missing, foreign or malformed kids fail; there is no arbitrary key/URL
lookup. NaCl carries no kid. Save `keyVersion(key).version` with the ciphertext, then use `keyVersion(key, savedVersion)` for historical verification/decryption. A selected historical version remains restricted to the original binding and read operations; it cannot sign, encrypt or derive a new shared encryption handle.

Revoking the active version blocks its use until rotation selects a new one.
Revocation does not erase history or invalidate tokens verified elsewhere.
Rotation retains encrypted historical versions. `keyRetire` permits verification/decryption while refusing new signing/encryption. `keyDestroy` removes the selected encrypted envelope from current state and leaves revoked/retired metadata; it does not erase older logs, backups, plaintext results or already-admitted native references. RSA rotation after destruction needs an explicit size. No automatic version GC is inferred from JWT expiry or receipt retirement. Retry receipts can be bounded using [epochs and sessions](RETENTION.md); that lifecycle does not establish when external ciphertext may be destroyed. Always-run application authorization precedes callbacks, receipt replay and cached reads.

## Prepared native cache

`FLOWER_KEY_CACHE_BYTES` defaults to 16 MiB. Zero disables retention without
disabling crypto. The cache retains actual AWS-LC Ed25519/RSA signers and JWT
verifiers, separate strict Dalek NaCl verification state, deterministic P256
signers, HMAC/AES contexts, and zeroizing XSalsa/X25519 buffers. Small NaCl cipher
contexts are still initialized per operation. Cache identity includes domain,
key/version, algorithm and the complete authenticated envelope; tampered
replacement ciphertext cannot reuse an older legitimate context.

The first resolution of an operation/key/version checks the invocation’s pinned
policy and records its dependency. Repeated uses can reuse the authorized
context while enforcing pinned permissions; they do not perform a fresh Raft
barrier or parse policy again for every crypto call. The node cache is consulted
only when invocation-local reuse misses, so its hit/miss counters exclude that
fast path. Cold local unwrap/parse loads coalesce per immutable key identity. Provider unwrap and key parsing run outside the global cache mutex, so different cold keys and warm hits proceed independently. Flight metadata shares the byte budget with cached contexts; zero or exhausted flight budget bypasses coalescing without rejecting valid work. Failed/aborted preparation releases its flight and wakes waiters. Signing/decryption also run outside the mutex and concurrently across workers and fresh Wasm instances. Conservative admission weights include
provider allocations; oversized contexts still work without retention. This is
a retention budget, not an exact RSS cap or a cap on in-flight references.

`FLOWER_KEY_CACHE_TTL_MS=0` means no expiry. Positive values bound actual prepared-context age from creation, including
invocation-local reuse, and reload expired contexts on next use. Expiry/eviction is
lazy. Reload uses the already-loaded mounted wrapping key: **TTL is not a file
refresh or external revocation lease**. There is no background secure-erasure
deadline. Cache metrics are node-wide and reset on restart. Raw caller-supplied
keys do not have a persistent prepared-context cache.

No nonces, plaintext, JWT validity results or claims results are cached here.
Each verify/decrypt validates claims against the invocation clock. Guest input,
output and native workspace admission use existing configurable execution
budgets. Native work checks deadlines before and after calls, rather than
supporting mid-call interruption. Zeroizing owned buffers does not protect
against host compromise, process dumps or swapping; an authorized host can
access locally unwrapped material.

## Migration and boundaries

Moving a partition transfers its encrypted key catalog together with data,
code, timers and receipts. Before catalog ownership cutover, the destination
validates every unrevoked key version can be unlocked/prepared. Give all
receiving nodes the same wrapping key through an independent secure mechanism.
A locked/wrong-key destination leaves the durable move staged and the tenant
paused; correct configuration, restart affected nodes, and allow recovery to
roll forward. There is no unsafe fallback activation.

For wrapping-key rotation, mount the new `FLOWER_KEYRING_FILE` and protected old files through `FLOWER_KEYRING_PREVIOUS_FILES` (JSON array of paths), restart, and invoke `keyRewrap()` for every catalog. It preserves logical key/version identity while wrapping DEKs with the active KEK. Once every required catalog and backup policy is reconciled, remove old providers and restart. A destination can accept envelopes from any mounted matching provider; there is no hidden cross-provider secret transfer. Not implemented: KMS unlock, externally enforced revocation leases,
non-exportable HSM signing, general exportable secrets or private-key exports.
Those require additional provider protocols and failure semantics. The current
boundary protects private keys from application JS and standalone encrypted
backups; it does not promise that an authorized server host cannot access them.

## Observe reuse

Use `admin.keyCacheStats()` or `flower key cache` against each node to inspect
retained bytes, loads, evictions and coalesced preparation. These counters are
node-local, reset on restart, and exclude invocation-local hits. Compare deltas
during your actual workload alongside request latency and CPU use. To measure
retention's effect, repeat with `FLOWER_KEY_CACHE_BYTES=0` and otherwise identical
settings; this disables node-wide retention but preserves invocation-local reuse.
Cryptographic operations per callback and database requests per second are
different measurements, so record both when comparing results.
