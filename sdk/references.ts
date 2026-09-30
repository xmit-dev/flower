import { canonicalJson, type Json } from "./json.ts";
import type { CollectionManifest, ReferenceManifest } from "./core.ts";

// Foreign keys, as the server checks them at the end of a mutation (src/evaluator/rust_engine/references.rs),
// over a state as the in-process reference engine holds it: record IDs to values. The testing module runs it.

type Parts = Json[] | null;

const sourceId = (collection: string, key: string) => "source:" + JSON.stringify([collection, key]);

function sourcePair(id: string): [string, string] | null {
  if (!id.startsWith("source:")) return null;
  const pair = JSON.parse(id.slice(7)) as unknown;
  return Array.isArray(pair) && pair.length === 2 && typeof pair[0] === "string" && typeof pair[1] === "string" ? [pair[0], pair[1]] : null;
}

/** The parts of a row's reference, or null when one is missing or null. */
function parts(reference: ReferenceManifest, key: string, value: Json): Parts {
  let found: Json[];
  if (reference.key === true) found = [key];
  else if (reference.key !== undefined) {
    let components: unknown;
    try { components = JSON.parse(key); } catch { return null; }
    if (!Array.isArray(components) || components.length < reference.key) return null;
    found = components.slice(0, reference.key) as Json[];
  } else {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
    found = [];
    for (const field of reference.fields!) {
      if (!Object.hasOwn(value, field)) return null;
      found.push(value[field]!);
    }
  }
  // Rows may come from another realm (a bundle run in its own context): copy the parts alone.
  return found.some((part) => part === null) ? null : JSON.parse(JSON.stringify(found)) as Json[];
}

type Named = { kind: "nothing" } | { kind: "key"; key: string } | { kind: "impossible" };

/** The target's key a row refers to. */
function named(reference: ReferenceManifest, key: string, value: Json): Named {
  if (reference.key === true) return { kind: "key", key };
  const found = parts(reference, key, value);
  if (found === null) return { kind: "nothing" };
  if (reference.json) return { kind: "key", key: canonicalJson(found.length === 1 ? found[0]! : found) };
  return found.length === 1 && typeof found[0] === "string" ? { kind: "key", key: found[0] } : { kind: "impossible" };
}

/** A key as a message shows it: quoted, and cut short past 200 characters. */
function shown(key: string): string {
  const characters = [...key];
  const quoted = JSON.stringify(characters.slice(0, 200).join(""));
  return characters.length > 200 ? `${quoted.slice(0, -1)}…` : quoted;
}

function place(reference: ReferenceManifest): Json {
  return reference.key === undefined ? { fields: [...reference.fields!] } : { key: reference.key };
}

function violation(message: string, details: Record<string, Json>): Error {
  return Object.assign(new Error(message), { code: "FOREIGN_KEY_VIOLATION", details });
}

function missing(collection: string, reference: ReferenceManifest, key: string, target: string | null): Error {
  return violation(target === null
    ? `${collection} row ${shown(key)} refers to a key no ${reference.target} row can have`
    : `${collection} row ${shown(key)} refers to ${reference.target} row ${shown(target)}, which does not exist`,
  { collection, key, references: place(reference), target: reference.target, targetKey: target });
}

function referred(collection: string, reference: ReferenceManifest, target: string, key: string): Error {
  return violation(`${reference.target} row ${shown(target)} is deleted while ${collection} row ${shown(key)} still refers to it`,
    { collection, key, references: place(reference), target: reference.target, targetKey: target, deleted: true });
}

/**
 * Throw FOREIGN_KEY_VIOLATION, as the server fails such a mutation, when the writes `puts` and `deletes` make
 * over `before` leave a row they wrote referring to a row that does not exist, or a row they deleted still
 * referred to.
 */
export function checkReferences(collections: readonly CollectionManifest[], before: Readonly<Record<string, Json>>,
  puts: Readonly<Record<string, Json>>, deletes: readonly string[]): void {
  const references = collections.flatMap((entry) => (entry.references ?? []).map((reference) => ({ collection: entry.name, reference })));
  if (!references.length) return;
  const gone = new Set(deletes);
  const after = (id: string): Json | undefined => Object.hasOwn(puts, id) ? puts[id] : gone.has(id) ? undefined : before[id];
  const changed = [...new Set([...Object.keys(puts), ...deletes])].filter((id) => id.startsWith("source:")).sort();
  // Deleted rows by collection, checked once all rows are known.
  const deleted = new Map<string, Set<string>>();
  for (const id of changed) {
    const pair = sourcePair(id);
    if (!pair) continue;
    const [collection, key] = pair;
    const previous = Object.hasOwn(before, id) ? before[id]! : undefined;
    const next = after(id);
    if (next !== undefined) {
      for (const { reference } of references.filter((each) => each.collection === collection)) {
        if (previous !== undefined && (reference.key !== undefined || canonicalJson(parts(reference, key, previous) as Json) === canonicalJson(parts(reference, key, next) as Json))) continue;
        const target = named(reference, key, next);
        if (target.kind === "impossible") throw missing(collection, reference, key, null);
        if (target.kind === "key" && after(sourceId(reference.target, target.key)) === undefined) throw missing(collection, reference, key, target.key);
      }
    } else if (previous !== undefined) {
      const keys = deleted.get(collection) ?? new Set<string>();
      keys.add(key);
      deleted.set(collection, keys);
    }
  }
  if (!deleted.size) return;
  const rows = new Map<string, [string, Json][]>();
  const rowsOf = (collection: string) => {
    let found = rows.get(collection);
    if (!found) {
      const prefix = "source:" + JSON.stringify([collection]).slice(0, -1) + ",";
      found = [...new Set([...Object.keys(before), ...Object.keys(puts)])].filter((id) => id.startsWith(prefix))
        .flatMap((id): [string, Json][] => {
          const value = after(id);
          const pair = sourcePair(id);
          return value === undefined || !pair ? [] : [[pair[1], value]];
        })
        .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
      rows.set(collection, found);
    }
    return found;
  };
  for (const [target, keys] of [...deleted].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) {
    for (const { collection, reference } of references.filter((each) => each.reference.target === target)) {
      for (const [key, value] of rowsOf(collection)) {
        const found = named(reference, key, value);
        if (found.kind === "key" && keys.has(found.key)) throw referred(collection, reference, found.key, key);
      }
    }
  }
}
