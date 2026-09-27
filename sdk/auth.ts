import { fail, plainObject, type Principal } from "./core.ts";
import { jwt, type JWTAlgorithm, type JWTClaims, type JWTKey, type JWTVerifyOptions } from "./crypto.ts";
import type { Authenticator } from "./define.ts";
import type { Json } from "./json.ts";
import type { ManagedKey, ManagedKeyVersion } from "./keys.ts";

export interface JwtBearerOptions {
  /** A managed key declaration, raw HMAC bytes, or a PEM public key. Managed keys join define({ keys }) automatically. */
  readonly key: JWTKey;
  /** Required for raw keys; an optional restriction for managed keys. */
  readonly algorithms?: readonly JWTAlgorithm[];
  readonly issuer?: string;
  readonly audience?: readonly string[];
  readonly clockToleranceSeconds?: number;
  /** Map verified claims to a principal. Defaults to sub, an optional tenant claim, and all claims. */
  readonly principal?: (claims: JWTClaims) => Principal | null;
}

function token(credentials: Json): string | null {
  if (credentials === null) return null;
  if (typeof credentials === "string") return credentials.startsWith("Bearer ") ? credentials.slice(7) : credentials;
  if (typeof credentials === "object" && !Array.isArray(credentials) && typeof credentials.token === "string") return credentials.token;
  fail("UNAUTHENTICATED", "Credentials must be a bearer token or { token }");
}

function claimsPrincipal(claims: JWTClaims): Principal {
  if (typeof claims.sub !== "string" || !claims.sub) fail("UNAUTHENTICATED", "The token has no subject");
  return { subject: claims.sub, ...(typeof claims.tenant === "string" && claims.tenant ? { tenant: claims.tenant } : {}), claims: claims as Json };
}

/** Authenticate bearer JWTs with native verification. Missing credentials are anonymous. */
export function jwtBearer(options: JwtBearerOptions): Authenticator {
  const settings = plainObject(options, "JWT bearer options", ["key", "algorithms", "issuer", "audience", "clockToleranceSeconds", "principal"]);
  const { key, principal = claimsPrincipal, ...validation } = settings as unknown as JwtBearerOptions;
  const managed = key !== null && typeof key === "object" && !(key instanceof Uint8Array);
  if (!managed && !validation.algorithms?.length) throw new TypeError("Raw JWT keys require algorithms");
  const declaration = managed ? ((key as ManagedKey | ManagedKeyVersion).kind === "key" ? key as ManagedKey : (key as ManagedKeyVersion).key) : null;
  return Object.freeze({
    kind: "authenticator" as const,
    keys: Object.freeze(declaration ? [declaration] : []),
    authenticate(_ctx: unknown, credentials: Json): Principal | null {
      const bearer = token(credentials);
      if (bearer === null) return null;
      let claims: JWTClaims;
      try {
        claims = managed
          ? jwt.verify(bearer, key as ManagedKey | ManagedKeyVersion, validation).claims
          : jwt.verify(bearer, key as Uint8Array | string, validation as JWTVerifyOptions).claims;
      } catch (error) {
        // Only a rejected token is the caller's problem; key trouble keeps its own code.
        if ((error as { code?: unknown } | null)?.code !== "CRYPTO_ERROR") throw error;
        return fail("UNAUTHENTICATED", `Invalid bearer token: ${(error as Error).message}`);
      }
      return principal(claims);
    },
  });
}
