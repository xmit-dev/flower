import { canonicalJson, type Json } from "./json.ts";
import { collectionInfo, declareDerivedAccess, plainObject, requireName } from "./core.ts";
import type { AggregateMetadata, Collection, CollectionManifest, Derived, EqualityValue, IndexMap } from "./core.ts";
import type { DerivedAccess } from "./access.ts";

export interface Aggregate<G = Json, V = Json> extends Derived<G, V> { readonly aggregate: AggregateMetadata }
export interface AggregateOptions<T, K, G, V> {
  readonly source: Collection<T, any, any>;
  readonly index: string;
  readonly initial: (group: G) => V;
  readonly add: (value: V, row: T, key: K, group: G) => V;
  readonly remove: (value: V, row: T, key: K, group: G) => V;
  /**
   * Keep each group's accumulator across deployments while this stays the same (a string of 1 to
   * 128 characters, or a nonnegative integer). Without it, every deployment rebuilds each group it
   * reaches from all of its rows, in one evaluation: fine for small groups, slow and eventually over
   * budget for big ones. Change it whenever initial, add or remove (or anything they call) would
   * compute differently, so the next deployment rebuilds the groups with the new callbacks.
   */
  readonly version?: string | number;
  /**
   * Who may read a group's value from a method, as derive(…, { access }) says it: a rule
   * over `principal`, `args` (the group, as ctx.get(aggregate, group) passes it) and `now`.
   * Aggregates fold every row of the source, whoever may read them; without a rule, any
   * method may read any group.
   */
  readonly access?: DerivedAccess<NoInfer<G>>;
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
  const metadata = plainObject(value, "Aggregate metadata", ["collection", "fields", "version"]);
  requireName(metadata.collection, "Aggregate collection");
  const version = metadata.version === undefined ? undefined : aggregateVersion(metadata.version);
  return Object.freeze({ collection: metadata.collection, fields: fields(metadata.fields), ...(version === undefined ? {} : { version }) });
}

/** UTF-8 bytes, without TextEncoder (guests lack it). */
function utf8Length(text: string): number {
  let bytes = 0;
  for (const char of text) {
    const code = char.codePointAt(0)!;
    bytes += code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
  }
  return bytes;
}

function aggregateVersion(value: unknown): string {
  const version = typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? String(value) : value;
  // The server's rule (valid_version): 1 to 128 bytes, no control characters.
  if (typeof version !== "string" || version.length === 0 || utf8Length(version) > 128 || /[\u0000-\u001f\u007f-\u009f]/.test(version)) {
    throw new TypeError("Aggregate version must be a nonnegative integer or a string of 1 to 128 bytes without control characters");
  }
  return version;
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
    const access = collectionInfo(reference)?.access;
    // Only JSON keys have parts: a plain string key that happens to parse as JSON must not.
    if (access && !collectionInfo(reference)?.key && usesKeyParts(access)) {
      throw new TypeError(`Access rules of ${JSON.stringify(reference.name)} read key parts; declare its keys with collection.key(schema)`);
    }
    const entry: CollectionManifest = Object.freeze({ name: reference.name, indexes: Object.freeze(indexes), ...(access ? { access } : {}) });
    const previous = byName.get(reference.name);
    if (previous && canonicalJson(previous.indexes) !== canonicalJson(entry.indexes)) {
      throw new TypeError(`Collection ${JSON.stringify(reference.name)} is declared with different indexes`);
    }
    // Access belongs to the collection: references without it (in a trigger, say) don't
    // conflict, but two different policies do.
    if (previous?.access && entry.access && canonicalJson(previous.access as unknown as Json) !== canonicalJson(entry.access as unknown as Json)) {
      throw new TypeError(`Collection ${JSON.stringify(reference.name)} is declared with different access`);
    }
    byName.set(reference.name, previous?.access || !entry.access ? previous ?? entry : entry);
  }
  return [...byName.values()].sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
}

/**
 * Check a whole application's collections, and its derived values' rules (`derived`, by
 * name): each readable(collection, key) names a collection declared with access whose
 * read rule doesn't use readable itself.
 */
export function checkReadable(manifest: readonly CollectionManifest[], derived: Readonly<Record<string, unknown>> = {}): void {
  const byName = new Map(manifest.map((entry) => [entry.name, entry]));
  const check = (label: string, rules: unknown) => {
    for (const target of readableTargets(rules)) {
      const policy = byName.get(target)?.access;
      if (!policy) {
        throw new TypeError(`Access rules of ${label} use readable(${JSON.stringify(target)}); declare that collection with access in define({ collections })`);
      }
      if (readableTargets(policy.read).length) {
        throw new TypeError(`Access rules of ${label} use readable(${JSON.stringify(target)}), whose read rule uses readable itself`);
      }
    }
  };
  for (const entry of byName.values()) check(JSON.stringify(entry.name), entry.access);
  for (const [name, rule] of Object.entries(derived)) check(`derived ${JSON.stringify(name)}`, rule);
}

/** The collections a compiled rule (or policy) names in readable(). */
function readableTargets(rule: unknown): string[] {
  if (Array.isArray(rule)) return rule.flatMap(readableTargets);
  if (rule === null || typeof rule !== "object" || Object.hasOwn(rule, "value") || Object.hasOwn(rule, "ref")) return [];
  const own = Object.hasOwn(rule, "readable") ? [(rule as { readable: [string, unknown] }).readable[0]] : [];
  return [...own, ...Object.values(rule).flatMap(readableTargets)];
}

function usesKeyParts(rule: unknown): boolean {
  if (Array.isArray(rule)) return rule.some(usesKeyParts);
  if (rule === null || typeof rule !== "object" || Object.hasOwn(rule, "value")) return false;
  const ref = (rule as { ref?: unknown }).ref;
  if (Array.isArray(ref)) return ref[0] === "key" && ref.length > 1;
  return Object.values(rule).some(usesKeyParts);
}

/**
 * Maintain one accumulator per equality group of an index from row deltas.
 * add/remove must be deterministic, order-independent inverses. Prefer integer
 * units: floating-point sums depend on update order. Redeploying rebuilds every group from its
 * rows, unless `version` is given and unchanged: then groups keep their accumulators.
 */
export function aggregate<T, K extends Json, I extends IndexMap, N extends Extract<keyof I, string>, V>(
  name: string,
  options: { readonly source: Collection<T, K, I>; readonly index: N } & Omit<AggregateOptions<T, K, EqualityValue<T, I[N]>, V>, "source" | "index">,
): Aggregate<EqualityValue<T, I[N]>, V> {
  requireName(name, "Aggregate name");
  const settings = plainObject(options, "Aggregate options", ["source", "index", "initial", "add", "remove", "access", "version"]);
  const [declaration] = collectionManifest([options.source]);
  requireName(settings.index, "Aggregate index");
  if (!Object.hasOwn(declaration.indexes, options.index)) throw new TypeError(`Unknown aggregate index ${JSON.stringify(options.index)}`);
  for (const callback of ["initial", "add", "remove"] as const) {
    if (typeof settings[callback] !== "function") throw new TypeError(`Aggregate ${callback} must be a function`);
  }
  const metadata = normalizeAggregateMetadata({
    collection: declaration.name, fields: declaration.indexes[options.index], ...(settings.version === undefined ? {} : { version: settings.version }),
  });
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
  declareDerivedAccess(settings.access, name, definition);
  return definition;
}
