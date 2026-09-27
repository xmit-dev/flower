import { canonicalJson, type Json } from "./json.ts";
import { schema as adopt, ValidationError, type Infer, type Schema, type SchemaLike } from "./schema.ts";
import type { ManagedKey } from "./keys.ts";

export type { Json };

declare const phantom: unique symbol;

export interface Failure { readonly code: string; readonly message: string; readonly details?: Json }

/** Abort with a structured failure. Callers receive its code, message and details intact. */
export function fail(code: string, message: string, details?: Json): never {
  if (typeof code !== "string" || !/^[A-Z][A-Z0-9_]*$/.test(code)) throw new TypeError("Failure codes use UPPER_SNAKE_CASE");
  const error = Object.assign(new Error(String(message)), { code }) as Error & { code: string; details?: Json };
  if (details !== undefined) {
    canonicalJson(details);
    error.details = details;
  }
  throw error;
}

export function requireName(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${label} must be a nonempty string`);
}

export function plainObject(value: unknown, label: string, allowed?: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} must be a plain object`);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError(`${label} must be a plain object`);
  if (allowed) for (const key in value) {
    if (!allowed.includes(key)) throw new TypeError(`${label} does not accept ${JSON.stringify(key)}`);
  }
  return value as Record<string, unknown>;
}

// ---- Collections and indexes

/** Ordered index components sort null, false, true, numbers, then UTF-16 strings. */
export type IndexScalar = null | boolean | number | string;
export type IndexMap = { readonly [index: string]: readonly string[] };
export type FieldOf<T> = [T] extends [object] ? Extract<keyof T, string> : string;
type FieldValue<T, F> = [T] extends [object] ? (F extends keyof T ? T[F] : never) : Json;
export type IndexValues<T, F extends readonly string[]> = { -readonly [P in keyof F]: FieldValue<T, F[P]> };
export type EqualityValue<T, F extends readonly string[]> = F extends readonly [infer A] ? FieldValue<T, A> : IndexValues<T, F>;
type Prefix<V extends readonly unknown[]> = number extends V["length"] ? readonly IndexScalar[] :
  V extends readonly [...infer Head, unknown] ? Prefix<Head> | V : [];
type Simplify<T> = { [K in keyof T]: T[K] } & {};

export interface RangeOptions<T = any, F extends readonly string[] = readonly string[]> {
  /** Equality on leading fields; bounds then select the next field. */
  readonly prefix?: Prefix<IndexValues<T, F>>;
  readonly gt?: IndexScalar;
  readonly gte?: IndexScalar;
  readonly lt?: IndexScalar;
  readonly lte?: IndexScalar;
  readonly limit: number;
  /** A cursor from the previous page; it continues against the next snapshot. */
  readonly after?: string;
  readonly reverse?: boolean;
}

export interface ScanOptions<I extends IndexMap = IndexMap> {
  /** Walk a declared index. Without it, rows follow encoded key order. */
  readonly index?: Extract<keyof I, string>;
  /** Index field values, or leading components of a tuple key (matching longer keys; use get for one key). */
  readonly prefix?: readonly Json[];
  readonly gt?: IndexScalar;
  readonly gte?: IndexScalar;
  readonly lt?: IndexScalar;
  readonly lte?: IndexScalar;
  readonly limit?: number;
  readonly offset?: number;
  readonly reverse?: boolean;
}

export interface Row<T = Json, K = string> { key: K; value: T }
export interface RangePage<T = Json, K = string> { readonly rows: Row<T, K>[]; readonly cursor: string | null }
export interface Query<T = Json, K = string> {
  readonly kind: "query";
  readonly collection: string;
  readonly fields: readonly string[];
  readonly value: Json;
  readonly [phantom]?: [T, K];
}
export interface RangeQuery<T = Json, K = string> {
  readonly kind: "range";
  readonly collection: string;
  readonly fields: readonly string[];
  readonly options: RangeOptions;
  readonly [phantom]?: [T, K];
}
export interface Index<T = Json, K = string, F extends readonly string[] = readonly string[]> {
  eq(value: EqualityValue<T, F>): Query<T, K>;
  range(options: RangeOptions<T, F>): RangeQuery<T, K>;
}

export interface Collection<T = Json, K extends Json = string, I extends IndexMap = {}> {
  readonly kind: "collection";
  readonly name: string;
  readonly indexes: I;
  /** A new reference with one more durable index. */
  index<const N extends string, const F extends readonly [FieldOf<T>, ...FieldOf<T>[]]>(name: N, fields: F): Collection<T, K, Simplify<I & { readonly [P in N]: F }>>;
  /** A new reference whose keys are validated JSON values, stored as canonical JSON. */
  key<K2 extends Json>(schema: SchemaLike<K2>): Collection<T, K2, I>;
  by<N extends Extract<keyof I, string>>(name: N): Index<T, K, I[N]>;
}

export interface CollectionInfo { readonly key: Schema<Json> | null; readonly value: Schema<unknown> | null }
const collectionInfos = new WeakMap<object, CollectionInfo>();
const rangeOwners = new WeakMap<object, Collection<any, any, any>>();

export function collectionInfo(reference: unknown): CollectionInfo | undefined {
  return typeof reference === "object" && reference !== null ? collectionInfos.get(reference) : undefined;
}
export function rangeOwner(query: unknown): Collection<any, any, any> | undefined {
  return typeof query === "object" && query !== null ? rangeOwners.get(query) : undefined;
}

function scalar(part: unknown): IndexScalar {
  if (part === null || typeof part === "boolean" || typeof part === "string") return part;
  if (typeof part === "number" && Number.isFinite(part)) return part === 0 ? 0 : part;
  throw new TypeError("Ordered index components must be null, boolean, finite number or string");
}

function rangeQuery(collection: string, fields: readonly string[], value: RangeOptions): RangeQuery {
  const input = plainObject(value, "Range options", ["prefix", "gt", "gte", "lt", "lte", "limit", "after", "reverse"]);
  if (!Number.isSafeInteger(input.limit) || (input.limit as number) < 1) throw new TypeError("Range limit must be a positive safe integer");
  const prefix = Object.hasOwn(input, "prefix") ? input.prefix : [];
  if (!Array.isArray(prefix) || prefix.length > fields.length) throw new TypeError("Invalid range prefix");
  canonicalJson(prefix);
  const copied = Array.from(prefix, scalar);
  const bounds = ["gt", "gte", "lt", "lte"] as const;
  if ((copied.length === fields.length && bounds.some((key) => Object.hasOwn(input, key))) ||
      (Object.hasOwn(input, "gt") && Object.hasOwn(input, "gte")) ||
      (Object.hasOwn(input, "lt") && Object.hasOwn(input, "lte"))) throw new TypeError("Invalid range bounds");
  const options: Record<string, unknown> = { prefix: Object.freeze(copied), limit: input.limit };
  for (const bound of bounds) if (Object.hasOwn(input, bound)) options[bound] = scalar(input[bound]);
  if (Object.hasOwn(input, "after")) {
    if (typeof input.after !== "string") throw new TypeError("Range cursor must be a string");
    options.after = input.after;
  }
  if (Object.hasOwn(input, "reverse")) {
    if (typeof input.reverse !== "boolean") throw new TypeError("Range reverse must be boolean");
    options.reverse = input.reverse;
  }
  return Object.freeze({ kind: "range", collection, fields, options: Object.freeze(options) as unknown as RangeOptions });
}

class IndexReference {
  readonly owner: CollectionReference;
  readonly index: string;
  readonly fields: readonly string[];
  constructor(owner: CollectionReference, index: string, fields: readonly string[]) {
    this.owner = owner;
    this.index = index;
    this.fields = fields;
  }
  range(options: RangeOptions): RangeQuery {
    const query = rangeQuery(this.owner.name, this.fields, options);
    rangeOwners.set(query, this.owner as unknown as Collection<any, any, any>);
    return query;
  }
  eq(value: Json): Query {
    canonicalJson(value);
    if (this.fields.length > 1 && (!Array.isArray(value) || value.length !== this.fields.length)) {
      throw new TypeError(`Index ${JSON.stringify(this.index)} requires a ${this.fields.length}-element tuple`);
    }
    return Object.freeze({ kind: "query" as const, collection: this.owner.name, fields: this.fields, value });
  }
}

class CollectionReference {
  readonly kind = "collection" as const;
  readonly name: string;
  readonly indexes: Readonly<Record<string, readonly string[]>>;
  constructor(name: string, indexes: Record<string, readonly string[]>, info: CollectionInfo) {
    this.name = name;
    this.indexes = Object.freeze(indexes);
    Object.freeze(this);
    collectionInfos.set(this, info);
  }
  index(indexName: string, fields: readonly string[]): CollectionReference {
    requireName(indexName, "Index name");
    if (!Array.isArray(fields) || fields.length === 0 || fields.some((field) => typeof field !== "string" || !field)) {
      throw new TypeError("An index requires one or more field names");
    }
    if (new Set(fields).size !== fields.length) throw new TypeError("Index field names must be distinct");
    if (Object.hasOwn(this.indexes, indexName)) throw new TypeError(`Index ${JSON.stringify(indexName)} is already declared`);
    return new CollectionReference(this.name, Object.assign(Object.create(null), this.indexes, { [indexName]: Object.freeze([...fields]) }),
      collectionInfos.get(this)!);
  }
  key(keySchema: SchemaLike<Json>): CollectionReference {
    return new CollectionReference(this.name, Object.assign(Object.create(null), this.indexes), { ...collectionInfos.get(this)!, key: adopt(keySchema) });
  }
  by(indexName: string): IndexReference {
    if (!Object.hasOwn(this.indexes, indexName)) throw new TypeError(`Unknown index ${JSON.stringify(indexName)} on ${this.name}`);
    return new IndexReference(this, indexName, this.indexes[indexName]);
  }
}

function buildCollection(name: string, indexes: Record<string, readonly string[]>, info: CollectionInfo): Collection<any, any, any> {
  return new CollectionReference(name, indexes, info) as unknown as Collection<any, any, any>;
}

/** Declare a collection of JSON records. A record schema validates every write. */
export function collection<T = Json>(name: string): Collection<T>;
export function collection<S extends SchemaLike<any>>(name: string, value: S): Collection<Infer<S>>;
export function collection(name: string, value?: SchemaLike<unknown>): Collection<any, any, any> {
  requireName(name, "Collection name");
  return buildCollection(name, Object.create(null), { key: null, value: value === undefined ? null : adopt(value) });
}

// ---- Principals and contexts

export interface Principal {
  readonly subject: string;
  readonly tenant?: string;
  readonly claims?: Json;
}
export interface AuthorizationRequest {
  readonly credentials: Json;
  /** The public alias the caller invoked, or $flower.session.* for retry sessions. */
  readonly method: string;
  readonly args: Json;
  readonly partition: string | null;
  /** Present only on an authenticated participant RPC from a transaction coordinator. */
  readonly delegation: { readonly coordinator: string; readonly principal: Principal | null } | null;
}
export interface HistoryIdentity { readonly database: string; readonly incarnation: string }

export interface Context {
  /**
   * Trusted server milliseconds, fixed for the invocation; creates a time dependency.
   * Flower can't tell when a result that reads it changes, so watches poll it.
   */
  now(): number;
  /**
   * The same milliseconds as now(), for code that reports every future time at which
   * its result can change through changesAt(). Watches then wake exactly at those times.
   */
  clock(): number;
  /**
   * The result may change when the clock reaches time, even without a write. Pass every
   * such instant after reading clock(); the earliest counts. Past times and null declare nothing.
   */
  changesAt(time: number | null): void;
  get<T, K extends Json>(collection: Collection<T, K, any>, key: K): T | null;
  get<V>(derived: Derived<null, V>): V;
  get<A, V>(derived: Derived<A, V>, args: A): V;
  scan<T, K extends Json, I extends IndexMap>(collection: Collection<T, K, I>, options?: ScanOptions<I>): Row<T, K>[];
  query<T>(query: Query<T, any>): T[];
  range<T, K>(range: RangeQuery<T, K>): RangePage<T, K>;
}
export interface QueryContext extends Context {
  /** Stable across movement; changes on a fenced disaster restore. Null before retention initialization. */
  history(): HistoryIdentity | null;
  /** The authenticated caller, or null for anonymous and unauthenticated calls. */
  principal(): Principal | null;
}
export interface MutationContext extends QueryContext {
  set<T, K extends Json>(collection: Collection<T, K, any>, key: K, value: T): void;
  delete<T, K extends Json>(collection: Collection<T, K, any>, key: K): void;
  materialize<V>(derived: Derived<null, V>): void;
  materialize<A, V>(derived: Derived<A, V>, args: A): void;
  unmaterialize<V>(derived: Derived<null, V>): void;
  unmaterialize<A, V>(derived: Derived<A, V>, args: A): void;
}

// ---- Definitions

export interface AggregateMetadata { readonly collection: string; readonly fields: readonly string[] }
export interface Derived<A = Json, V = Json> {
  readonly kind: "derived";
  readonly name: string;
  readonly compute: (ctx: Context, args: A) => V;
  readonly aggregate?: AggregateMetadata;
}
export type Materialization<A> = "always" | { readonly each: Collection<any, A & Json, any> };
export interface DeriveOptions<A> {
  /** Keep instances maintained: one argless instance, or one per row keyed like the collection. */
  readonly materialize?: Materialization<A>;
}
const materializations = new WeakMap<object, Materialization<any>>();
export function materializationOf(definition: object): Materialization<any> | undefined { return materializations.get(definition); }

/** A pure reactive function of database state. */
export function derive<A = null, V = Json>(name: string, compute: (ctx: Context, args: A) => V, options: DeriveOptions<A> = {}): Derived<A, V> {
  requireName(name, "Definition name");
  if (typeof compute !== "function") throw new TypeError("A definition requires a compute function");
  const settings = plainObject(options, "Derive options", ["materialize"]);
  const definition = Object.freeze({ kind: "derived" as const, name, compute });
  if (settings.materialize !== undefined) {
    const policy = settings.materialize as Materialization<A>;
    if (policy !== "always" && (typeof policy !== "object" || policy === null || !collectionInfos.has((policy as { each: object }).each))) {
      throw new TypeError("materialize must be \"always\" or { each: collection }");
    }
    materializations.set(definition, policy);
  }
  return definition;
}

export type QueryConsistency = "linearizable" | "replica-local";
/** Who may call a public method: anyone, any authenticated principal, or a pure predicate. */
export type Access<A = any> = "public" | "authenticated" | ((ctx: QueryContext, principal: Principal | null, args: A) => boolean);
export interface MethodSpec<A> {
  readonly args?: SchemaLike<A>;
  readonly access?: Access<A>;
}
export interface QuerySpec<A> extends MethodSpec<A> {
  /** Fresh by default. Replica-local reads may lag without bound. */
  readonly consistency?: QueryConsistency;
}
export interface MutationSpec<A> extends MethodSpec<A> {
  /**
   * `false` keeps no retry receipt, so a retry with the same request ID runs
   * the method again. For methods that are idempotent by construction or
   * whose repeats are harmless. Calls that write nothing keep none anyway.
   */
  readonly receipt?: boolean;
}

export interface QueryMethod<A = Json, V = Json> {
  readonly kind: "queryMethod";
  readonly name: string;
  readonly compute: (ctx: QueryContext, args: A) => V;
  readonly consistency?: "replica-local";
}
export interface MutationMethod<A = Json, V = Json> {
  readonly kind: "mutationMethod";
  readonly name: string;
  readonly compute: (ctx: MutationContext, args: A) => V;
  readonly receipt?: false;
}
export type TransactionTarget = { readonly partition: string; readonly group?: never } | { readonly group: string; readonly partition?: never };
export type TransactionCall = TransactionTarget & { readonly method: string; readonly args?: Json };
export interface TransactionPlan<V = Json> { readonly calls: readonly TransactionCall[]; readonly value?: V }
export interface TransactionMethod<A = Json, V = Json> {
  readonly kind: "transactionMethod";
  readonly name: string;
  readonly compute: (ctx: Context, args: A) => TransactionPlan<V>;
}
export type Definition = Derived<any, any> | QueryMethod<any, any> | MutationMethod<any, any> | TransactionMethod<any, any>;
export type HttpMethod = QueryMethod<any, any> | MutationMethod<any, any> | TransactionMethod<any, any>;
export type HttpMap = { readonly [alias: string]: HttpMethod };

export interface MethodInfo { readonly args: Schema<unknown> | null; readonly access: Access | undefined }
const methodInfos = new WeakMap<object, MethodInfo>();
export function methodInfo(method: object): MethodInfo { return methodInfos.get(method) ?? { args: null, access: undefined }; }

function access(value: unknown): Access | undefined {
  if (value === undefined || value === "public" || value === "authenticated" || typeof value === "function") return value as Access | undefined;
  throw new TypeError("access must be \"public\", \"authenticated\" or a predicate");
}

function parseArgs<A>(args: Schema<unknown> | null, raw: unknown): A {
  if (!args) return raw as A;
  try { return args.parse(raw) as A; }
  catch (error) {
    if (error instanceof ValidationError) fail("INVALID_ARGUMENT", error.message, { path: [...error.path] });
    throw error;
  }
}

function method<M extends object>(label: string, name: string, specOrCompute: unknown, maybeCompute: unknown, extra: readonly string[],
  create: (compute: (ctx: any, args: any) => any, spec: Record<string, unknown>) => M): M {
  requireName(name, `${label} name`);
  const [spec, compute] = typeof specOrCompute === "function" ? [{}, specOrCompute] : [specOrCompute, maybeCompute];
  if (typeof compute !== "function") throw new TypeError(`A ${label.toLowerCase()} requires a compute function`);
  const settings = plainObject(spec, `${label} options`, ["args", "access", ...extra]);
  const args = settings.args === undefined ? null : adopt(settings.args as SchemaLike<unknown>);
  const run = compute as (ctx: unknown, args: unknown) => unknown;
  const definition = create(args ? (ctx, raw) => run(ctx, parseArgs(args, raw)) : run, settings);
  methodInfos.set(definition, { args, access: access(settings.access) });
  return definition;
}

/** A private read-only method. Expose it in define({ http }). */
export function query<A = null, V = Json>(name: string, compute: (ctx: QueryContext, args: A) => V): QueryMethod<A, V>;
export function query<S extends SchemaLike<any>, V>(name: string, spec: QuerySpec<Infer<S>> & { readonly args: S }, compute: (ctx: QueryContext, args: Infer<S>) => V): QueryMethod<Infer<S>, V>;
export function query<A = null, V = Json>(name: string, spec: QuerySpec<A>, compute: (ctx: QueryContext, args: A) => V): QueryMethod<A, V>;
export function query(name: string, specOrCompute: unknown, maybeCompute?: unknown): QueryMethod {
  return method("Query", name, specOrCompute, maybeCompute, ["consistency"], (compute, spec) => {
    if (spec.consistency !== undefined && spec.consistency !== "linearizable" && spec.consistency !== "replica-local") {
      throw new TypeError("Query consistency must be linearizable or replica-local");
    }
    return Object.freeze({ kind: "queryMethod" as const, name, compute,
      ...(spec.consistency === "replica-local" ? { consistency: "replica-local" as const } : {}) });
  });
}

/** A private atomic method. Expose it in define({ http }). */
export function mutation<A = null, V = Json>(name: string, compute: (ctx: MutationContext, args: A) => V): MutationMethod<A, V>;
export function mutation<S extends SchemaLike<any>, V>(name: string, spec: MutationSpec<Infer<S>> & { readonly args: S }, compute: (ctx: MutationContext, args: Infer<S>) => V): MutationMethod<Infer<S>, V>;
export function mutation<A = null, V = Json>(name: string, spec: MutationSpec<A>, compute: (ctx: MutationContext, args: A) => V): MutationMethod<A, V>;
export function mutation(name: string, specOrCompute: unknown, maybeCompute?: unknown): MutationMethod {
  return method("Mutation", name, specOrCompute, maybeCompute, ["receipt"], (compute, spec) => {
    if (spec.receipt !== undefined && typeof spec.receipt !== "boolean") throw new TypeError("Mutation receipt must be a boolean");
    return Object.freeze({ kind: "mutationMethod" as const, name, compute, ...(spec.receipt === false ? { receipt: false as const } : {}) });
  });
}

/** An atomic plan over methods exposed by other logical databases. Planning sees only its arguments. */
export function transaction<A = null, V = Json>(name: string, plan: (args: A) => TransactionPlan<V>): TransactionMethod<A, V>;
export function transaction<S extends SchemaLike<any>, V>(name: string, spec: MethodSpec<Infer<S>> & { readonly args: S }, plan: (args: Infer<S>) => TransactionPlan<V>): TransactionMethod<Infer<S>, V>;
export function transaction<A = null, V = Json>(name: string, spec: MethodSpec<A>, plan: (args: A) => TransactionPlan<V>): TransactionMethod<A, V>;
export function transaction(name: string, specOrPlan: unknown, maybePlan?: unknown): TransactionMethod {
  const [spec, plan] = typeof specOrPlan === "function" ? [{}, specOrPlan] : [specOrPlan, maybePlan];
  if (typeof plan !== "function") throw new TypeError("A transaction requires a plan function");
  return method("Transaction", name, spec, (_ctx: unknown, args: unknown) => plan(args), [],
    (compute) => Object.freeze({ kind: "transactionMethod" as const, name, compute }));
}

// ---- Typed method maps shared by define, participants and clients

/** A committed transaction: each call's result, and the plan's value when it has one. */
export interface TransactionResult<V = Json> { results: Json[]; value?: V }
export interface ManifestMethod { readonly name: string; readonly kind: "query" | "mutation" | "transaction"; readonly consistency?: "replica-local"; readonly receipt?: false }
export interface CollectionManifest { readonly name: string; readonly indexes: Readonly<Record<string, readonly string[]>> }
export interface FlowerModule<H extends HttpMap = HttpMap> {
  readonly definitions: Readonly<Record<string, Definition>>;
  readonly http: { readonly [K in keyof H]: ManifestMethod };
  readonly maintenance: { readonly name: string; readonly kind: "mutation"; readonly onError: { readonly name: string; readonly kind: "mutation" } } | null;
  readonly authorize?: { readonly name: string };
  readonly collections?: readonly CollectionManifest[];
  readonly keys?: readonly ManagedKey[];
  readonly [phantom]?: H;
}

export type ApiOf<App> = App extends FlowerModule<infer H> ? H : App extends HttpMap ? App : HttpMap;
export type ArgsOf<M> = M extends { readonly compute: (ctx: any, args: infer A) => any } ? A : Json;
export type ResultOf<M> = M extends TransactionMethod<any, infer V> ? TransactionResult<V> :
  M extends { readonly compute: (ctx: any, args: any) => infer V } ? V : Json;
export type AliasOf<App> = Extract<keyof ApiOf<App>, string>;
export type QueryAliasOf<App> = string extends AliasOf<App> ? string :
  { [K in AliasOf<App>]: ApiOf<App>[K] extends QueryMethod<any, any> ? K : never }[AliasOf<App>];
export type MutationAliasOf<App> = string extends AliasOf<App> ? string :
  { [K in AliasOf<App>]: ApiOf<App>[K] extends MutationMethod<any, any> | TransactionMethod<any, any> ? K : never }[AliasOf<App>];
/** Arguments are optional when null is acceptable; untyped maps accept anything. */
export type ArgsParameter<A> = 0 extends (1 & A) ? [args?: any] : null extends A ? [args?: A] : [args: A];

/** Build calls to another database's exposed methods, typed by its HTTP map or module. */
export function participant<M = HttpMap>(target: TransactionTarget) {
  const settings = plainObject(target, "Transaction target", ["partition", "group"]);
  if (Object.keys(settings).length !== 1) throw new TypeError("A transaction target names exactly one partition or group");
  const [field, value] = Object.entries(settings)[0];
  requireName(value, "Transaction target");
  return Object.freeze({
    call<Alias extends Exclude<AliasOf<M>, { [K in AliasOf<M>]: ApiOf<M>[K] extends TransactionMethod<any, any> ? K : never }[AliasOf<M>]>>(
      alias: Alias, ...args: ArgsParameter<ArgsOf<ApiOf<M>[Alias]>>
    ): TransactionCall {
      requireName(alias, "Participant method");
      return Object.freeze({ [field]: value, method: alias, args: (args[0] ?? null) as Json }) as unknown as TransactionCall;
    },
  });
}

// ---- Composition

export interface TaskFailure { readonly error: Failure; readonly failedAt: number }
export interface Task {
  readonly kind: "task";
  readonly name: string;
  /** The earliest time this task has work, or null when idle. Must depend only on data and time. */
  readonly due: (ctx: QueryContext) => number | null;
  /** Perform one bounded unit of work. It stays eligible while due() remains in the past. */
  readonly run: (ctx: MutationContext) => Json;
  /** Runs against the failed invocation's snapshot and time. Without it, the task backs off. */
  readonly onError?: (ctx: MutationContext, failure: TaskFailure) => Json;
}
export function task(name: string, options: { due: Task["due"]; run: Task["run"]; onError?: Task["onError"] }): Task {
  requireName(name, "Task name");
  const settings = plainObject(options, "Task options", ["due", "run", "onError"]);
  if (typeof settings.due !== "function" || typeof settings.run !== "function" ||
      (settings.onError !== undefined && typeof settings.onError !== "function")) {
    throw new TypeError("A task requires due and run functions");
  }
  return Object.freeze({ kind: "task" as const, name, due: settings.due as Task["due"], run: settings.run as Task["run"],
    ...(settings.onError ? { onError: settings.onError as Task["onError"] } : {}) });
}

export interface Change<T = Json, K = string> { readonly key: K; readonly before: T | null; readonly after: T | null }
export interface Trigger<T = any, K = any> {
  readonly kind: "trigger";
  readonly name: string;
  readonly source: Collection<T, any, any>;
  readonly run: (ctx: MutationContext, change: Change<T, K>) => void;
}
/** Runs inside every mutation that changes a row of source, once per changed key, before commit. */
export function trigger<T, K extends Json>(name: string, source: Collection<T, K, any>, run: (ctx: MutationContext, change: Change<T, K>) => void): Trigger<T, K> {
  requireName(name, "Trigger name");
  if (!collectionInfos.has(source)) throw new TypeError("A trigger requires a collection");
  if (typeof run !== "function") throw new TypeError("A trigger requires a function");
  return Object.freeze({ kind: "trigger" as const, name, source, run });
}

export type Usable = Component | { readonly component: Component };
export interface ComponentParts {
  readonly uses?: readonly Usable[];
  readonly collections?: readonly Collection<any, any, any>[];
  readonly definitions?: readonly Definition[];
  readonly tasks?: readonly Task[];
  readonly triggers?: readonly Trigger[];
  readonly keys?: readonly ManagedKey[];
}
export interface Component {
  readonly kind: "component";
  readonly uses: readonly Usable[];
  readonly collections: readonly Collection<any, any, any>[];
  readonly definitions: readonly Definition[];
  readonly tasks: readonly Task[];
  readonly triggers: readonly Trigger[];
  readonly keys: readonly ManagedKey[];
}
const componentFields = ["uses", "collections", "definitions", "tasks", "triggers", "keys"] as const;

/** Package collections, definitions, maintenance tasks, triggers and keys for define({ uses }). */
export function component(parts: ComponentParts = {}): Component {
  const settings = plainObject(parts, "Component", componentFields);
  const result: Record<string, unknown> = { kind: "component" };
  for (const field of componentFields) {
    const value = settings[field] ?? [];
    if (!Array.isArray(value)) throw new TypeError(`Component ${field} must be an array`);
    result[field] = Object.freeze([...value]);
  }
  return Object.freeze(result) as unknown as Component;
}
