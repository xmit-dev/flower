import { canonicalJson, type Json } from "./json.ts";
import { collectionInfo, plainObject, requireName } from "./core.ts";
import type { AggregateMetadata, Collection, CollectionManifest, Derived, EqualityValue, IndexMap } from "./core.ts";

export interface Aggregate<G = Json, V = Json> extends Derived<G, V> { readonly aggregate: AggregateMetadata }
export interface AggregateOptions<T, K, G, V> {
  readonly source: Collection<T, any, any>;
  readonly index: string;
  readonly initial: (group: G) => V;
  readonly add: (value: V, row: T, key: K, group: G) => V;
  readonly remove: (value: V, row: T, key: K, group: G) => V;
}

const aggregateSources = new WeakMap<object, Collection<any, any, any>>();
export function aggregateSource(definition: object): Collection<any, any, any> | undefined { return aggregateSources.get(definition); }

function fields(value: unknown): readonly string[] {
  if (!Array.isArray(value) || value.length === 0 || value.some((field) => typeof field !== "string" || !field) ||
      new Set(value).size !== value.length) {
    throw new TypeError("Index fields must be distinct nonempty strings");
  }
  return Object.freeze([...value]) as readonly string[];
}

export function normalizeAggregateMetadata(value: unknown): AggregateMetadata {
  const metadata = plainObject(value, "Aggregate metadata", ["collection", "fields"]);
  requireName(metadata.collection, "Aggregate collection");
  return Object.freeze({ collection: metadata.collection, fields: fields(metadata.fields) });
}

/** Merge collection declarations by name; one name must always carry the same indexes. */
export function collectionManifest(references: Iterable<Collection<any, any, any>>): CollectionManifest[] {
  const byName = new Map<string, CollectionManifest>();
  for (const reference of references) {
    for (const field of ["kind", "name", "indexes"]) {
      const property = Object.getOwnPropertyDescriptor(reference, field);
      if (!property || !("value" in property)) throw new TypeError("collections requires collection references");
    }
    if (reference.kind !== "collection") throw new TypeError("collections requires collection references");
    requireName(reference.name, "Collection name");
    const declared = plainObject(reference.indexes, "Collection indexes");
    const indexes: Record<string, readonly string[]> = Object.create(null);
    for (const [index, columns] of Object.entries(declared)) {
      requireName(index, "Index name");
      indexes[index] = fields(columns);
    }
    const entry = Object.freeze({ name: reference.name, indexes: Object.freeze(indexes) });
    const previous = byName.get(reference.name);
    if (previous && canonicalJson(previous.indexes) !== canonicalJson(entry.indexes)) {
      throw new TypeError(`Collection ${JSON.stringify(reference.name)} is declared with different indexes`);
    }
    byName.set(reference.name, previous ?? entry);
  }
  return [...byName.values()].sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
}

/**
 * Maintain one accumulator per equality group of an index from row deltas.
 * add/remove must be deterministic, order-independent inverses. Prefer integer
 * units: floating-point sums depend on update order. Redeploying rebuilds.
 */
export function aggregate<T, K extends Json, I extends IndexMap, N extends Extract<keyof I, string>, V>(
  name: string,
  options: { readonly source: Collection<T, K, I>; readonly index: N } & Omit<AggregateOptions<T, K, EqualityValue<T, I[N]>, V>, "source" | "index">,
): Aggregate<EqualityValue<T, I[N]>, V> {
  requireName(name, "Aggregate name");
  const settings = plainObject(options, "Aggregate options", ["source", "index", "initial", "add", "remove"]);
  const [declaration] = collectionManifest([options.source]);
  requireName(settings.index, "Aggregate index");
  if (!Object.hasOwn(declaration.indexes, options.index)) throw new TypeError(`Unknown aggregate index ${JSON.stringify(options.index)}`);
  for (const callback of ["initial", "add", "remove"] as const) {
    if (typeof settings[callback] !== "function") throw new TypeError(`Aggregate ${callback} must be a function`);
  }
  const metadata = normalizeAggregateMetadata({ collection: declaration.name, fields: declaration.indexes[options.index] });
  const { initial, add, remove } = options;
  const decode = collectionInfo(options.source)?.key ? (key: string) => JSON.parse(key) as K : (key: string) => key as K;
  type Group = EqualityValue<T, I[N]>;
  const compute = (_ctx: unknown, raw: Group): V => {
    const update = raw as unknown as { initialize: boolean; group: Group; previous: V; changes: { key: string; old?: T; new?: T }[] };
    let value = update.initialize ? initial(update.group) : update.previous;
    for (const change of update.changes) {
      const key = decode(change.key);
      if (Object.hasOwn(change, "old")) value = remove(value, change.old!, key, update.group);
      if (Object.hasOwn(change, "new")) value = add(value, change.new!, key, update.group);
    }
    return value;
  };
  const definition = Object.freeze({ kind: "derived" as const, name, compute, aggregate: metadata });
  aggregateSources.set(definition, options.source);
  return definition;
}
