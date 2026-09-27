import { canonicalJson, type Json } from "./json.ts";
import {
  collection, collectionInfo, component, derivedAccessOf, fail, materializationOf, methodInfo, mutation, plainObject, query, rangeOwner, requireName, task, trigger,
} from "./core.ts";
import type {
  Access, AuthorizationRequest, Collection, Component, ComponentParts, Definition, Derived, Failure, FlowerModule, HttpMap,
  ManifestMethod, MutationContext, Principal, QueryContext, SetOptions, Task, TaskFailure, Trigger,
} from "./core.ts";
import { aggregateSource, checkReadable, collectionManifest, normalizeAggregateMetadata } from "./indexing.ts";
import { keyManifest, type ManagedKey } from "./keys.ts";
import { ValidationError, type Schema } from "./schema.ts";

/** Principal subject the authorization hook returns for anonymous callers; methods see null. */
export const ANONYMOUS_SUBJECT = "$anonymous";

export type Authenticate = (ctx: QueryContext, credentials: Json, request: AuthorizationRequest) => Principal | null;
export interface Authenticator {
  readonly kind: "authenticator";
  readonly keys: readonly ManagedKey[];
  readonly authenticate: Authenticate;
}
export interface AuthConfig {
  /** Resolve credentials to a principal. Return null for anonymous callers; fail() rejects the call. */
  readonly authenticate?: Authenticate | Authenticator;
  /** Access for exposed methods that declare none: "authenticated" when authenticate is set, else "public". */
  readonly default?: "public" | "authenticated";
  /** Access to retry sessions. Defaults to default. */
  readonly sessions?: "public" | "authenticated";
  /** Accept a principal delegated by a transaction coordinator. Defaults to trusting cluster peers. */
  readonly delegation?: (ctx: QueryContext, coordinator: string, principal: Principal | null) => boolean;
}
export interface ModuleConfig<H extends HttpMap = HttpMap> extends ComponentParts {
  readonly http?: H;
  readonly auth?: AuthConfig;
}

// ---- Context binding: typed keys, record schemas and triggers over the host context

type Host = Record<string, (...args: any[]) => any>;
/** The runner's capability to act with the application's rights: host operation 14. */
type Elevate = (on: boolean) => unknown;
interface Session {
  readonly ctx: MutationContext;
  readonly flush: () => void;
  /**
   * Run one callback with this invocation's definer capability: the runner's third
   * argument, or null where there is none (queries, derived values). Undefined leaves
   * older servers' host.definer in use.
   */
  readonly within: <T>(elevate: Elevate | null | undefined, run: () => T) => T;
}
interface Runtime {
  readonly triggers: ReadonlyMap<string, readonly Trigger[]>;
  /** Collections whose triggers only observe rows appearing or disappearing. */
  readonly existence: ReadonlySet<string>;
  readonly sessions: Map<object, Session>;
  /** Collections whose access policy the manifest carries, so the server enforces it. */
  guarded: ReadonlySet<string>;
  /** The server enforces rules (collection policies, derived access) this app declares. */
  enforced: boolean;
}

// Flower publishes the contexts it will pass to callbacks before initializing
// the application. Binding them in define() puts the bound contexts into the
// initialization snapshot: every callback starts from that pristine state, so
// its session is already built and its trigger bookkeeping already empty.
declare const __flowerContexts: readonly Host[] | undefined;
interface Touch {
  /** A frozen plain reference the SDK built: host calls as the definer take no app object. */
  readonly target: { readonly kind: "collection"; readonly name: string };
  readonly name: string;
  /** The collection has JSON keys (collection.key(schema)): triggers get them decoded. */
  readonly keyed: boolean;
  readonly raw: string;
  readonly triggers: readonly Trigger[];
  /** The row before this round, or, for existence-only collections, whether it existed. */
  readonly before: Json | boolean;
}
/** Internal triggers that need only a row's existence, run as (ctx, key, exists). */
const existenceTriggers = new WeakMap<Trigger, (ctx: MutationContext, key: Json, exists: boolean) => void>();
const hosts = new WeakMap<object, Host>();
/** Each bound context's raw host, without any definer capability. */
const raws = new WeakMap<object, Host>();
const flushes = new WeakMap<object, () => void>();
const definers = new WeakMap<object, <T>(run: () => T) => T>();

/** Run pending triggers now instead of at the end of the mutation. */
function settleTriggers(ctx: object): void {
  flushes.get(ctx)?.();
}

/**
 * The raw host context behind a bound context, for SDK internals that need encoded keys.
 * It never carries the definer capability, even on servers whose host context does.
 */
export function hostContext(ctx: object): Host {
  return raws.get(ctx) ?? (ctx as Host);
}

/**
 * SDK internals only (not exported to applications): run `run` with the application's
 * rights when `ctx` is a mutation's context in an app whose rules the server enforces,
 * else as the caller. Keep app-supplied objects and callbacks out of `run`: whatever runs
 * inside acts for the application.
 */
export function asDefiner<T>(ctx: object, run: () => T): T {
  const definer = definers.get(ctx);
  return definer ? definer(run) : run();
}

export function encodeKey(reference: Collection<any, any, any>, key: unknown): string {
  const codec = collectionInfo(reference)?.key;
  if (!codec) {
    if (typeof key !== "string") fail("INVALID_KEY", `${reference.name} keys must be strings`);
    return key;
  }
  try { codec.parse(key); }
  catch (error) {
    if (error instanceof ValidationError) fail("INVALID_KEY", `${reference.name} key ${error.message}`, { collection: reference.name });
    throw error;
  }
  return canonicalJson(key);
}

export function decodeKey(reference: Collection<any, any, any> | undefined, raw: string): Json {
  return reference && collectionInfo(reference)?.key ? JSON.parse(raw) as Json : raw;
}

function keyScan(reference: Collection<any, any, any>, options: Record<string, unknown> | undefined) {
  if (options === undefined || options.index !== undefined) return options;
  const { prefix, gt, gte, lt, lte, ...rest } = options;
  if (gt !== undefined || gte !== undefined || lt !== undefined || lte !== undefined) {
    fail("INVALID_SCAN", `${reference.name} has JSON keys; declare an index for ordered bounds`);
  }
  if (prefix === undefined) return rest;
  if (!Array.isArray(prefix)) fail("INVALID_SCAN", "Scan prefix must be an array");
  if (prefix.length === 0) return rest;
  const head = canonicalJson(prefix).slice(0, -1);
  return { ...rest, gte: head + ",", lt: head + "-" };
}

/** Equality of host values, which are plain JSON data: canonical JSON equality without encoding. */
function sameValue(a: Json, b: Json): boolean {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    for (let index = 0; index < a.length; ++index) if (!sameValue(a[index], b[index])) return false;
    return true;
  }
  if (Array.isArray(b)) return false;
  const keys = Object.keys(a);
  if (keys.length !== Object.keys(b).length) return false;
  for (const key of keys) if (!Object.hasOwn(b, key) || !sameValue(a[key], b[key])) return false;
  return true;
}

function setClear(options: unknown): string[] {
  if (options === undefined) return [];
  const { clear } = plainObject(options, "set options", ["clear"]);
  if (clear === undefined) return [];
  if (!Array.isArray(clear) || clear.length > 256 || clear.some((field) => typeof field !== "string" || !field)) {
    throw new TypeError("set option clear must list at most 256 nonempty field names");
  }
  return [...clear] as string[];
}

function bind(host: Host, runtime: Runtime): Session {
  const touched = new Map<string, Touch>();
  // Row existence this mutation has observed or written, for existence-only collections.
  const exists = new Map<string, boolean>();
  const identity = (reference: Collection<any, any, any>, raw: string) => reference.name + "\u0000" + raw;
  // A guarded collection's reads show the caller's view, where hidden rows look absent and
  // skipped deletes leave rows in place, so existence is read afresh there, as the definer.
  const cached = (reference: Collection<any, any, any>) =>
    runtime.existence.has(reference.name) && !runtime.guarded.has(reference.name);
  function observe(reference: Collection<any, any, any>, raw: string, value: Json) {
    if (cached(reference)) exists.set(identity(reference, raw), value !== null);
    return value;
  }
  // A policy the server never received protects nothing: refuse to touch a collection
  // declared with access but missing from define({ collections }).
  function guard(reference: unknown) {
    const owner = (reference as { kind?: unknown })?.kind === "collection" ? reference as Collection<any, any, any> : rangeOwner(reference);
    if (owner && collectionInfo(owner)?.access && !runtime.guarded.has(owner.name)) {
      throw new TypeError(`Collection ${JSON.stringify(owner.name)} declares access; list it in define({ collections }) so the server enforces it`);
    }
  }
  // Triggers act with the application's rights, like derived values: they see rows and
  // fields the caller can't, and their writes aren't checked against the caller's policy.
  // Only apps with rules need it, and only servers that enforce them offer it: as the
  // third argument of this invocation's compute (`within` sets it), or, on older
  // servers, as host.definer. No context an application holds carries it.
  let elevate: Elevate | null | undefined;
  function definer<T>(run: () => T): T {
    const lift = elevate !== undefined ? elevate
      : typeof host.definer === "function" ? (on: boolean) => host.definer(on) : null;
    if (!runtime.enforced || lift === null) return run();
    lift(true);
    try { return run(); }
    finally { lift(false); }
  }
  function within<T>(capability: Elevate | null | undefined, run: () => T): T {
    const previous = elevate;
    elevate = capability;
    try { return run(); }
    finally { elevate = previous; }
  }
  function track(reference: Collection<any, any, any>, raw: string) {
    // Read what the method passed in once, as the caller: nothing it supplied (a getter,
    // a proxy) runs with the application's rights below or when triggers settle.
    const name = reference.name;
    const triggers = runtime.triggers.get(name);
    if (!triggers) return;
    const id = name + "\u0000" + raw;
    if (touched.has(id)) return;
    const target = Object.freeze({ kind: "collection" as const, name });
    const keyed = Boolean(collectionInfo(reference)?.key);
    const existence = runtime.existence.has(name);
    const known = existence && !runtime.guarded.has(name) ? exists.get(id) : undefined;
    const before = known ?? definer(() => existence ? host.get(target, raw) !== null : host.get(target, raw));
    touched.set(id, { target, name, keyed, raw, before, triggers });
  }
  const ctx: MutationContext = Object.freeze({
    now: () => host.now(),
    clock: () => host.clock(),
    changesAt: (time: number | null) => { host.changesAt(time); },
    history: () => host.history(),
    principal() {
      const principal = host.principal();
      return principal !== null && typeof principal === "object" && principal.subject === ANONYMOUS_SUBJECT ? null : principal;
    },
    get(reference: any, key?: unknown) {
      if (reference?.kind === "collection") {
        guard(reference);
        const raw = encodeKey(reference, key);
        return observe(reference, raw, host.get(reference, raw));
      }
      return host.get(reference, key === undefined ? null : key);
    },
    scan(reference: any, options?: Record<string, unknown>) {
      guard(reference);
      if (!collectionInfo(reference)?.key) return options === undefined ? host.scan(reference) : host.scan(reference, options);
      const translated = keyScan(reference, options);
      const rows = translated === undefined ? host.scan(reference) : host.scan(reference, translated);
      return rows.map((row: { key: string; value: Json }) => ({ key: JSON.parse(row.key), value: row.value }));
    },
    query: (reference: any) => { guard(reference); return host.query(reference); },
    range(reference: any) {
      guard(reference);
      const page = host.range(reference);
      const owner = rangeOwner(reference);
      if (!owner || !collectionInfo(owner)?.key) return page;
      return { rows: page.rows.map((row: { key: string; value: Json }) => ({ key: JSON.parse(row.key), value: row.value })), cursor: page.cursor };
    },
    set(reference: any, key: unknown, value: unknown, options?: SetOptions) {
      guard(reference);
      const clear = setClear(options);
      const raw = encodeKey(reference, key);
      const schema = collectionInfo(reference)?.value;
      if (schema) {
        try { schema.parse(value); }
        catch (error) {
          if (error instanceof ValidationError) {
            fail("INVALID_RECORD", `${reference.name} ${error.message}`, { collection: reference.name, key: raw, path: [...error.path] });
          }
          throw error;
        }
      }
      track(reference, raw);
      // Older servers read three arguments; send the options only when they matter.
      if (clear.length) host.set(reference, raw, value, { clear });
      else host.set(reference, raw, value);
      observe(reference, raw, true);
    },
    delete(reference: any, key: unknown) {
      guard(reference);
      const raw = encodeKey(reference, key);
      track(reference, raw);
      host.delete(reference, raw);
      observe(reference, raw, null);
    },
    materialize: (definition: any, args?: unknown) => { host.materialize(definition, args === undefined ? null : args); },
    unmaterialize: (definition: any, args?: unknown) => { host.unmaterialize(definition, args === undefined ? null : args); },
  }) as MutationContext;
  hosts.set(ctx, host);
  const raw: Host = { ...host };
  delete raw.definer;
  raws.set(ctx, Object.freeze(raw));
  definers.set(ctx, definer);
  function flush() {
    if (touched.size) definer(settle);
  }
  function settle() {
    for (let round = 0; touched.size; round++) {
      if (round === 32) fail("TRIGGER_LOOP", "Triggers kept changing records for 32 rounds");
      // Read every after value before any trigger of this round runs: a trigger's own writes are
      // tracked for the next round, so reading them here too would report overlapping changes.
      const batch = [...touched.values()].map((entry) => ({
        ...entry,
        after: runtime.existence.has(entry.name)
          ? !runtime.guarded.has(entry.name)
            ? exists.get(entry.name + "\u0000" + entry.raw) === true
            : host.get(entry.target, entry.raw) !== null
          : host.get(entry.target, entry.raw) as Json,
      }));
      touched.clear();
      for (const entry of batch) {
        const key = () => entry.keyed ? JSON.parse(entry.raw) as Json : entry.raw;
        if (runtime.existence.has(entry.name)) {
          if (entry.before === entry.after) continue;
          for (const each of entry.triggers) existenceTriggers.get(each)!(ctx, key(), entry.after as boolean);
          continue;
        }
        const after = entry.after as Json;
        if (sameValue(entry.before as Json, after)) continue;
        const change = Object.freeze({ key: key(), before: entry.before as Json, after });
        for (const each of entry.triggers) each.run(ctx, change);
      }
    }
  }
  flushes.set(ctx, flush);
  return { ctx, flush, within };
}

function bound(definition: Definition, runtime: Runtime): (ctx: any, args: any, elevate?: Elevate) => any {
  const compute = definition.compute as (ctx: unknown, args: unknown) => unknown;
  if (definition.kind === "derived" && definition.aggregate) return compute;
  const session = (host: Host) => runtime.sessions.get(host) ?? bind(host, runtime);
  if (definition.kind === "mutationMethod") {
    // Servers that enforce access rules pass the definer capability as a third argument,
    // for this invocation only; the application's compute never sees it. Without one
    // (older servers), host.definer stays in use.
    return (host: Host, args: unknown, elevate?: Elevate) => {
      if (hosts.has(host)) return compute(host, args);
      const { ctx, flush, within } = session(host);
      return within(typeof elevate === "function" ? elevate : undefined, () => {
        const value = compute(ctx, args);
        flush();
        return value;
      });
    };
  }
  // Queries, transaction plans and derived values never act as the definer.
  return (host: Host, args: unknown) => {
    if (!host || hosts.has(host)) return compute(host, args);
    const { ctx, within } = session(host);
    return within(null, () => compute(ctx, args));
  };
}

// ---- Maintenance: one host handler pair selecting among every task, earliest due first

interface TaskState { failures: number; retryAt: number; error: Failure }
const taskStates = collection<Record<string, TaskState>>("$flower.tasks");

function maintenance(tasks: readonly Task[]) {
  /** The task due earliest by now, and when any task is next due, retry delays included. */
  function plan(ctx: QueryContext, now: number): { chosen: Task | null; next: number | null } {
    const states = ctx.get(taskStates, "state") ?? {};
    let chosen: Task | null = null, earliest = Infinity, next = Infinity;
    for (const candidate of tasks) {
      const due = candidate.due(ctx);
      if (due === null) continue;
      if (typeof due !== "number" || !Number.isFinite(due)) throw new TypeError(`Task ${candidate.name} returned an invalid due time`);
      const at = Object.hasOwn(states, candidate.name) ? Math.max(due, states[candidate.name].retryAt) : due;
      if (at <= now && at < earliest) { chosen = candidate; earliest = at; }
      next = Math.min(next, at);
    }
    return { chosen, next: next === Infinity ? null : next };
  }
  // The host sleeps until next (null: until a write) instead of polling.
  const hint = (ctx: QueryContext, now: number) => {
    const { chosen, next } = plan(ctx, now);
    return { continue: chosen !== null, next };
  };
  const run = mutation("$flower.maintenance", (ctx) => {
    const { chosen: selected, next } = plan(ctx, ctx.now());
    if (!selected) return { $flower: { continue: false, next } };
    const result = selected.run(ctx);
    settleTriggers(ctx);
    const states = ctx.get(taskStates, "state");
    if (states && Object.hasOwn(states, selected.name)) {
      const { [selected.name]: _cleared, ...rest } = states;
      if (Object.keys(rest).length) ctx.set(taskStates, "state", rest);
      else ctx.delete(taskStates, "state");
    }
    return { task: selected.name, result, $flower: hint(ctx, ctx.now()) };
  });
  const onError = mutation("$flower.maintenance.error", (ctx, failure: TaskFailure) => {
    const info = plainObject(failure, "Maintenance failure", ["error", "failedAt"]);
    const error = plainObject(info.error, "Maintenance error", ["code", "message", "details"]) as unknown as Failure;
    if (typeof error.code !== "string" || typeof error.message !== "string" || !Number.isSafeInteger(info.failedAt)) {
      throw new TypeError("Invalid maintenance failure");
    }
    const now = ctx.now();
    const { chosen: selected, next } = plan(ctx, now);
    if (!selected) return { $flower: { continue: false, next } };
    const failedAt = Math.max(info.failedAt as number, now);
    if (selected.onError) {
      const result = selected.onError(ctx, Object.freeze({ error, failedAt }));
      settleTriggers(ctx);
      return { task: selected.name, result, $flower: hint(ctx, now) };
    }
    const states = ctx.get(taskStates, "state") ?? {};
    const failures = (states[selected.name]?.failures ?? 0) + 1;
    const retryAt = failedAt + Math.min(60_000, 1_000 * 2 ** Math.min(failures - 1, 6));
    ctx.set(taskStates, "state", { ...states, [selected.name]: { failures, retryAt, error } });
    return { task: selected.name, failures, retryAt, $flower: hint(ctx, now) };
  });
  return { run, onError };
}

// ---- Declarative materialization

interface Marker { cursor: string | null; done: boolean }
const markers = collection<Marker>("$flower.materialized");

function materialization(definitions: readonly Definition[]): { tasks: Task[]; triggers: Trigger[] } {
  const jobs: { marker: string; derived: Derived<any, any>; source: Collection<any, any, any> | null }[] = [];
  const triggers: Trigger[] = [];
  for (const definition of definitions) {
    if (definition.kind !== "derived") continue;
    const policy = materializationOf(definition);
    if (policy === undefined) continue;
    if (policy === "always") {
      jobs.push({ marker: `always:${definition.name}`, derived: definition, source: null });
      continue;
    }
    jobs.push({ marker: `each:${definition.name}:${policy.each.name}`, derived: definition, source: policy.each });
    const maintain = (ctx: MutationContext, key: Json, exists: boolean) =>
      exists ? ctx.materialize(definition, key) : ctx.unmaterialize(definition, key);
    const each = trigger(`materialize:${definition.name}`, policy.each, (ctx, change) => {
      if ((change.before === null) !== (change.after === null)) maintain(ctx, change.key, change.after !== null);
    });
    existenceTriggers.set(each, maintain);
    triggers.push(each);
  }
  if (!jobs.length) return { tasks: [], triggers };
  const pending = (ctx: QueryContext) => jobs.find((job) => ctx.get(markers, job.marker)?.done !== true);
  return {
    triggers,
    tasks: [task("materialize", {
      due: (ctx) => pending(ctx) ? ctx.now() : null,
      run(ctx) {
        const job = pending(ctx);
        if (!job) return null;
        if (!job.source) {
          ctx.materialize(job.derived, null);
          ctx.set(markers, job.marker, { cursor: null, done: true });
          return { materialized: job.derived.name, rows: 1 };
        }
        const host = hostContext(ctx);
        const cursor = ctx.get(markers, job.marker)?.cursor ?? null;
        const rows: { key: string }[] = host.scan(job.source, cursor === null ? { limit: 64 } : { gt: cursor, limit: 64 });
        for (const row of rows) ctx.materialize(job.derived, decodeKey(job.source, row.key));
        ctx.set(markers, job.marker, { cursor: rows.at(-1)?.key ?? cursor, done: rows.length < 64 });
        return { materialized: job.derived.name, rows: rows.length };
      },
    })],
  };
}

// ---- Authorization: per-method access compiled into the single host hook

function checkedPrincipal(value: unknown): Principal {
  const principal = plainObject(value, "Principal", ["subject", "tenant", "claims"]);
  if (typeof principal.subject !== "string" || !principal.subject || principal.subject === ANONYMOUS_SUBJECT ||
      (principal.tenant !== undefined && (typeof principal.tenant !== "string" || !principal.tenant))) {
    throw new TypeError("A principal needs a nonempty subject and an optional nonempty tenant");
  }
  return principal as unknown as Principal;
}

function anonymousAware(principal: Principal | null): Principal | null {
  return principal !== null && principal.subject === ANONYMOUS_SUBJECT ? null : principal;
}

function authorization(config: AuthConfig | undefined, http: Readonly<Record<string, Definition>>) {
  const settings = config === undefined ? undefined : plainObject(config, "Auth configuration", ["authenticate", "default", "sessions", "delegation"]) as AuthConfig;
  const source = settings?.authenticate;
  const authenticate: Authenticate | null = typeof source === "function" ? source : source ? source.authenticate : null;
  // Reject malformed settings up front: unknown values would otherwise leave methods public.
  if (source !== undefined && typeof authenticate !== "function") throw new TypeError("auth.authenticate must be a function or an authenticator");
  for (const field of ["default", "sessions"] as const) {
    if (settings?.[field] !== undefined && settings[field] !== "public" && settings[field] !== "authenticated") {
      throw new TypeError(`auth.${field} must be "public" or "authenticated"`);
    }
  }
  if (settings?.delegation !== undefined && typeof settings.delegation !== "function") throw new TypeError("auth.delegation must be a function");
  const fallback = settings?.default ?? (authenticate ? "authenticated" : "public");
  const policies = new Map<string, { access: Access; args: Schema<unknown> | null }>();
  for (const [alias, method] of Object.entries(http)) {
    const info = methodInfo(method);
    policies.set(alias, { access: info.access ?? fallback, args: info.args });
  }
  const sessions = { access: settings?.sessions ?? fallback, args: null };
  if (!authenticate && [...policies.values(), sessions].some((policy) => policy.access === "authenticated")) {
    throw new TypeError("Methods require authentication, but define({ auth }) has no authenticate");
  }
  // A hook forces fresh policy reads and full admission, so compile one only when a call could be refused.
  if (!authenticate && !settings?.delegation && ![...policies.values()].some((policy) => typeof policy.access === "function")) return null;
  return query("$flower.authorize", (ctx, request: AuthorizationRequest): Principal => {
    let principal: Principal | null;
    if (request.delegation) {
      principal = anonymousAware(request.delegation.principal);
      if (settings?.delegation && !settings.delegation(ctx, request.delegation.coordinator, principal)) {
        fail("FORBIDDEN", "This database does not trust the transaction coordinator");
      }
      // A trusted coordinator's principal acts in this partition; the host requires tenant === partition.
      if (principal !== null && request.partition) principal = { ...principal, tenant: request.partition };
    } else {
      const resolved = authenticate ? authenticate(ctx, request.credentials, request) : null;
      principal = resolved === null ? null : checkedPrincipal(resolved);
    }
    const policy = request.method.startsWith("$flower.session.") ? sessions : policies.get(request.method);
    if (!policy) fail("FORBIDDEN", `Unknown method ${request.method}`);
    if (policy.access === "authenticated" && principal === null) fail("UNAUTHENTICATED", "Authentication required");
    if (typeof policy.access === "function") {
      let args: unknown = request.args;
      if (policy.args) {
        try { args = policy.args.parse(request.args); }
        catch (error) {
          if (error instanceof ValidationError) fail("INVALID_ARGUMENT", error.message, { path: [...error.path] });
          throw error;
        }
      }
      if (!policy.access(ctx, principal, args)) fail("FORBIDDEN", "Access denied");
    }
    return principal ?? { subject: ANONYMOUS_SUBJECT, ...(request.partition ? { tenant: request.partition } : {}) };
  });
}

// ---- define

function flatten(root: Component) {
  const parts = { collections: [] as Collection<any, any, any>[], definitions: [] as Definition[], tasks: [] as Task[], triggers: [] as Trigger[], keys: [] as ManagedKey[] };
  const seen = new Set<object>();
  (function visit(value: Component | { readonly component: Component }) {
    const part = value !== null && typeof value === "object" && (value as Component).kind !== "component" && "component" in value ? value.component : value as Component;
    if (seen.has(part)) return;
    if (part === null || typeof part !== "object" || part.kind !== "component") throw new TypeError("uses requires components");
    seen.add(part);
    for (const nested of part.uses) visit(nested);
    parts.collections.push(...part.collections);
    parts.definitions.push(...part.definitions);
    parts.tasks.push(...part.tasks);
    parts.triggers.push(...part.triggers);
    parts.keys.push(...part.keys);
  })(root);
  return parts;
}

const definitionKinds = ["derived", "queryMethod", "mutationMethod", "transactionMethod"];

/** Declare the application: its components, internals and complete public HTTP allowlist. */
export function define<const H extends HttpMap = {}>(config: ModuleConfig<H> = {}): FlowerModule<H> {
  const settings = plainObject(config, "Module configuration", ["uses", "collections", "definitions", "tasks", "triggers", "keys", "http", "auth"]);
  const { http: httpSetting, auth, ...parts } = settings as ModuleConfig<H>;
  const flat = flatten(component(parts));
  const originals = new Map<string, Definition>();
  function register(value: unknown, internal = false): Definition {
    const definition = plainObject(value, "Definition") as unknown as Definition;
    if (!definitionKinds.includes(definition.kind) || typeof definition.compute !== "function") {
      throw new TypeError("A definition requires kind, name, and compute fields");
    }
    requireName(definition.name, "Definition name");
    if (!internal && definition.name.startsWith("$flower.")) throw new TypeError("Definition names beginning with $flower. are reserved");
    const previous = originals.get(definition.name);
    if (previous && previous !== definition) throw new TypeError(`Conflicting definition ${JSON.stringify(definition.name)}`);
    originals.set(definition.name, definition);
    return definition;
  }
  for (const definition of flat.definitions) register(definition);

  const exposed: Record<string, Definition> = Object.create(null);
  const http: Record<string, ManifestMethod> = Object.create(null);
  const allowlist = httpSetting === undefined ? {} : plainObject(httpSetting, "HTTP allowlist");
  for (const alias of Object.keys(allowlist)) {
    requireName(alias, "HTTP alias");
    if (alias.startsWith("$flower.")) throw new TypeError("HTTP aliases beginning with $flower. are reserved");
    const method = register(allowlist[alias]);
    if (method.kind === "derived") throw new TypeError("Only query, mutation, and transaction methods may be exposed over HTTP");
    exposed[alias] = method;
    http[alias] = Object.freeze({
      name: method.name,
      kind: method.kind === "queryMethod" ? "query" as const : method.kind === "transactionMethod" ? "transaction" as const : "mutation" as const,
      ...(method.kind === "queryMethod" && method.consistency === "replica-local" ? { consistency: "replica-local" as const } : {}),
      ...(method.kind === "mutationMethod" && method.receipt === false ? { receipt: false as const } : {}),
    });
  }

  const materialized = materialization([...originals.values()]);
  const tasks = [...flat.tasks, ...materialized.tasks];
  const names = new Set<string>();
  for (const each of tasks) {
    if (each?.kind !== "task") throw new TypeError("tasks requires task definitions");
    if (names.has(each.name)) throw new TypeError(`Duplicate task ${JSON.stringify(each.name)}`);
    names.add(each.name);
  }
  let maintenanceManifest: FlowerModule["maintenance"] = null;
  if (tasks.length) {
    const handlers = maintenance(tasks);
    register(handlers.run, true);
    register(handlers.onError, true);
    maintenanceManifest = Object.freeze({ name: handlers.run.name, kind: "mutation" as const,
      onError: Object.freeze({ name: handlers.onError.name, kind: "mutation" as const }) });
  }

  const authorize = authorization(auth, exposed);
  if (authorize) register(authorize, true);

  const triggers = new Map<string, Trigger[]>();
  for (const each of [...flat.triggers, ...materialized.triggers]) {
    if (each?.kind !== "trigger") throw new TypeError("triggers requires trigger definitions");
    const list = triggers.get(each.source.name) ?? [];
    if (list.some((existing) => existing.name === each.name)) throw new TypeError(`Duplicate trigger ${JSON.stringify(each.name)}`);
    list.push(each);
    triggers.set(each.source.name, list);
  }
  const existence = new Set([...triggers].filter(([, list]) => list.every((each) => existenceTriggers.has(each))).map(([name]) => name));
  const runtime: Runtime = { triggers, existence, sessions: new Map(), guarded: new Set(), enforced: false };
  if (typeof __flowerContexts !== "undefined") {
    for (const host of __flowerContexts) runtime.sessions.set(host, bind(host, runtime));
  }

  const definitions: Record<string, Definition> = Object.create(null);
  const collections = [...flat.collections, ...[...triggers.values()].flat().map((each) => each.source)];
  for (const [name, definition] of originals) {
    const aggregate = definition.kind === "derived" && definition.aggregate !== undefined ? normalizeAggregateMetadata(definition.aggregate) : undefined;
    if (aggregate) {
      const source = aggregateSource(definition);
      if (source) collections.push(source);
    }
    definitions[name] = Object.freeze({
      kind: definition.kind, name, compute: bound(definition, runtime),
      ...(aggregate ? { aggregate } : {}),
      ...(definition.kind === "derived" && derivedAccessOf(definition) !== undefined ? { access: derivedAccessOf(definition) } : {}),
      ...(definition.kind === "queryMethod" && definition.consistency === "replica-local" ? { consistency: "replica-local" as const } : {}),
      ...(definition.kind === "mutationMethod" && definition.receipt === false ? { receipt: false as const } : {}),
    }) as Definition;
  }

  const authenticator = auth?.authenticate;
  const keys = keyManifest([...flat.keys, ...(authenticator && typeof authenticator === "object" ? authenticator.keys : [])]);
  const manifest = collectionManifest(collections);
  checkReadable(manifest, Object.fromEntries(Object.entries(definitions).flatMap(([name, definition]) =>
    Object.hasOwn(definition, "access") ? [[name, (definition as { access?: unknown }).access]] : [])));
  runtime.guarded = new Set(manifest.filter((entry) => entry.access).map((entry) => entry.name));
  // Triggers and SDK bookkeeping must also read derived values whose rule the caller fails.
  runtime.enforced = runtime.guarded.size > 0 || Object.values(definitions).some((definition) => Object.hasOwn(definition, "access"));
  return Object.freeze({
    definitions: Object.freeze(definitions),
    http: Object.freeze(http),
    maintenance: maintenanceManifest,
    ...(authorize ? { authorize: Object.freeze({ name: authorize.name }) } : {}),
    ...(manifest.length ? { collections: Object.freeze(manifest) } : {}),
    ...(keys.length ? { keys } : {}),
  }) as unknown as FlowerModule<H>;
}
