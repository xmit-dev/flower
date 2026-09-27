/** JSON is Flower's durable value format. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

// Flower installs this immutable capability before evaluating the bundle. The
// bare binding avoids trusting an application replacement of globalThis.
// Other JavaScript hosts use the exact same validated TypeScript fallback.
declare const __flowerCanonicalJson: ((value: unknown) => string | undefined) | undefined;
const nativeCanonicalJson = typeof __flowerCanonicalJson === "function" ? __flowerCanonicalJson : undefined;

/** Stable serialization for instance identities, including object arguments. */
export function canonicalJson(value: unknown): string {
  const native = nativeCanonicalJson?.(value);
  if (native !== undefined) return native;
  return canonicalJsonFallback(value);
}

function canonicalJsonFallback(value: unknown): string {
  // Primitive keys and arrays of primitive key components cannot contain
  // cycles. Allocate ancestry tracking only when traversing compound values.
  let ancestors: Set<object> | undefined;
  function encode(current: unknown, depth: number): string {
    if (depth > 128) throw new TypeError("Flower JSON nesting exceeds 128");
    if (current === null || typeof current === "boolean" || typeof current === "string") {
      return JSON.stringify(current);
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current)) throw new TypeError("Flower values require finite numbers");
      return JSON.stringify(current);
    }
    if (typeof current !== "object") throw new TypeError("Flower values must be JSON values");
    if (ancestors?.has(current)) throw new TypeError("Flower values cannot contain cycles");
    const prototype = Object.getPrototypeOf(current);
    if (!Array.isArray(current) && prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("Flower values must contain plain objects");
    }
    const array = Array.isArray(current);
    const fields = array ? undefined : new Map<string, unknown>();
    // A null-prototype table also ignores inherited numeric getters/setters;
    // assigning into an ordinary scratch Array would invoke those hooks.
    const items = array ? Object.create(null) as Record<string, unknown> : undefined;
    let count = 0;
    let primitive = array;
    for (const key of Reflect.ownKeys(current)) {
      if (array && key === "length") continue;
      const property = Object.getOwnPropertyDescriptor(current, key)!;
      if (typeof key !== "string" || !property.enumerable || !("value" in property)) {
        throw new TypeError("Flower values cannot contain symbols, hidden properties, or accessors");
      }
      if (array) {
        if (!/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= current.length) {
          throw new TypeError("Flower arrays cannot contain named properties");
        }
        const item = property.value;
        items![key] = item;
        ++count;
        primitive &&= item === null || typeof item === "boolean" || typeof item === "string" || typeof item === "number";
      } else {
        fields!.set(key, property.value);
      }
    }
    if (!primitive) (ancestors ??= new Set<object>()).add(current);
    try {
      if (array) {
        if (count !== current.length) throw new TypeError("Flower arrays cannot contain holes");
        // Retain the separate length read used by Array.from: a Proxy may
        // observe it or return a different value after descriptor validation.
        const length = current.length;
        if (primitive && length === count) {
          let output = "[";
          for (let index = 0; index < length; ++index) {
            if (index) output += ",";
            output += encode(items![index], depth + 1);
          }
          return output + "]";
        }
        return "[" + Array.from({ length }, (_, index) => encode(items![index], depth + 1)).join(",") + "]";
      }
      return "{" + Array.from(fields!.keys()).sort().map((key) =>
        JSON.stringify(key) + ":" + encode(fields!.get(key), depth + 1)
      ).join(",") + "}";
    } finally {
      if (!primitive) ancestors!.delete(current);
    }
  }
  return encode(value, 0);
}
