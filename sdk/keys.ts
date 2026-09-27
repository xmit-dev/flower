import { canonicalJson } from "./json.ts";

export type ManagedKeyAlgorithm = "Ed25519" | "P256" | "RSA" | "HS256" | "A256GCM" | "XSalsa20Poly1305" | "X25519";
export type KeyUsage = "sign" | "verify" | "encrypt" | "decrypt" | "derive" | "publicKey";
export interface KeyOptions {
  readonly algorithm: ManagedKeyAlgorithm;
  readonly usages: readonly KeyUsage[];
}
/** A public declaration, not authority: native policy must separately bind it. */
export interface ManagedKey extends KeyOptions {
  readonly kind: "key";
  readonly name: string;
}
/** Public version metadata, constrained to the original bound declaration. */
export interface ManagedKeyVersion {
  readonly kind: "keyVersion";
  readonly key: ManagedKey;
  readonly version: string;
}
/** Opaque native capability. No bytes, identifier, or serializable token exists. */
declare const sharedKeyBrand: unique symbol;
export interface SharedKey { readonly kind: "sharedKey"; readonly [sharedKeyBrand]: true; }
const sharedHandles = new WeakSet<object>();
export function sharedKey(value: unknown): SharedKey {
  if (value === null || typeof value !== "object" || (value as SharedKey).kind !== "sharedKey") {
    throw new TypeError("Native crypto returned an invalid shared-key handle");
  }
  sharedHandles.add(value);
  return value as SharedKey;
}
export function isSharedKey(value: unknown): value is SharedKey {
  return value !== null && typeof value === "object" && sharedHandles.has(value);
}

const algorithms: readonly ManagedKeyAlgorithm[] = ["Ed25519", "P256", "RSA", "HS256", "A256GCM", "XSalsa20Poly1305", "X25519"];
const usages: readonly KeyUsage[] = ["sign", "verify", "encrypt", "decrypt", "derive", "publicKey"];

/** Declare a key capability and include it in define({keys}). No key is loaded. */
export function key(name: string, options: KeyOptions): ManagedKey {
  if (typeof name !== "string" || name.length === 0) throw new TypeError("Key name must be nonempty");
  canonicalJson(options);
  if (options === null || typeof options !== "object" || Array.isArray(options) ||
      Object.keys(options).some(name => !["algorithm", "usages"].includes(name)) ||
      !algorithms.includes(options.algorithm) || !Array.isArray(options.usages) || options.usages.length === 0 ||
      options.usages.some(usage => !usages.includes(usage)) || new Set(options.usages).size !== options.usages.length) {
    throw new TypeError("Keys require a supported algorithm and distinct nonempty usages");
  }
  return Object.freeze({ kind: "key", name, algorithm: options.algorithm, usages: Object.freeze([...options.usages].sort()) });
}

export function keyManifest(input: unknown): readonly ManagedKey[] {
  if (input === undefined) return [];
  if (!Array.isArray(input)) throw new TypeError("keys must be an array");
  const declarations = new Map<string, ManagedKey>();
  for (const value of input) {
    canonicalJson(value);
    if (value === null || typeof value !== "object" || Array.isArray(value) || value.kind !== "key" ||
        Object.keys(value).length !== 4 || Object.keys(value).some(name => !["kind", "name", "algorithm", "usages"].includes(name))) {
      throw new TypeError("Invalid managed key declaration");
    }
    const declaration = key(value.name, { algorithm: value.algorithm, usages: value.usages });
    const previous = declarations.get(declaration.name);
    if (previous && canonicalJson(previous) !== canonicalJson(declaration)) {
      throw new TypeError(`Conflicting key declaration ${JSON.stringify(declaration.name)}`);
    }
    declarations.set(declaration.name, previous ?? declaration);
  }
  return Object.freeze([...declarations.values()].sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

export function isManaged(value: unknown): value is ManagedKey | ManagedKeyVersion | SharedKey {
  if (isSharedKey(value)) return true;
  if (value === null || typeof value !== "object" || value instanceof Uint8Array) return false;
  // Validation precedes property access so this dispatch cannot execute getters.
  canonicalJson(value);
  return (value as ManagedKey).kind === "key" || (value as ManagedKeyVersion).kind === "keyVersion";
}
