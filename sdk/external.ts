import { canonicalJson, type Json } from "./json.ts";
import { collection, component, derive, fail, mutation, plainObject, query, requireName, trigger } from "./core.ts";
import type { Access, Collection, Component, Context, Derived, MutationContext, MutationMethod, QueryMethod } from "./core.ts";
import { schema as adopt, v, ValidationError, type SchemaLike } from "./schema.ts";

export type ExternalState<R> = { readonly status: "pending" } | { readonly status: "ready"; readonly value: R };
export interface ExternalWork<A, I> { readonly args: A; readonly key: string; readonly input: I }
export interface ExternalNextOptions { readonly limit?: number; readonly shard?: readonly [index: number, count: number] }
/** Identifies one claim of one input. Leases spread work; they don't guard publish. */
export interface ExternalLease<A> { readonly args: A; readonly key: string; readonly owner: string; readonly attempt: number }
export interface ExternalClaim<A, I> extends ExternalWork<A, I>, ExternalLease<A> { readonly expiresAt: number }
export interface ExternalClaimOptions { readonly limit?: number; readonly leaseMs?: number }
export interface ExternalStats {
  /** A claim would succeed now. */
  readonly ready: boolean;
  /** When the longest-waiting claimable key became available. */
  readonly oldestReadyAt: number | null;
  /** When a lease or a delayed key next makes work available. */
  readonly nextAvailableAt: number | null;
}

export type ExternalHttp<Prefix extends string, A, I, R, Pool extends boolean> = {
  readonly [K in `${Prefix}.pending`]: QueryMethod<A, ExternalWork<A, I> | null>;
} & {
  readonly [K in `${Prefix}.publish`]: MutationMethod<{ args: A; key: string; value: R }, { accepted: boolean }>;
} & (Pool extends true ? {
  readonly [K in `${Prefix}.next`]: QueryMethod<ExternalNextOptions | null, ExternalWork<A, I>[]>;
} & {
  readonly [K in `${Prefix}.claim`]: MutationMethod<ExternalClaimOptions & { owner: string }, ExternalClaim<A, I>[]>;
} & {
  readonly [K in `${Prefix}.renew`]: MutationMethod<{ leases: ExternalLease<A>[]; leaseMs?: number }, (number | null)[]>;
} & {
  readonly [K in `${Prefix}.release`]: MutationMethod<ExternalLease<A> & { delayMs?: number }, boolean>;
} & {
  readonly [K in `${Prefix}.ready`]: QueryMethod<null, boolean>;
} & {
  readonly [K in `${Prefix}.stats`]: QueryMethod<null, ExternalStats>;
} : {});

/** A reactive value produced outside the database. ctx.get(value, args) reads its current state. */
export interface External<A, I, R, Pool extends boolean = false> extends Derived<A, ExternalState<R> | null> {
  readonly component: Component;
  readonly results: Collection<{ key: string; value: R }>;
  /** The work a worker should do for args, or null when the stored result is current. */
  pending(ctx: Context, args: A): ExternalWork<A, I> | null;
  /** Store a result only if key still names the current input. Racing workers keep the first result. */
  publish(ctx: MutationContext, work: { readonly args: A; readonly key: string; readonly value: R }): { accepted: boolean };
  /** Pending work across the tracked collection, oldest first; optionally one hash shard of it. */
  next(ctx: Context, options?: ExternalNextOptions): ExternalWork<A, I>[];
  /** Lease the keys that have waited longest, so other workers skip them until the lease ends. */
  claim(ctx: MutationContext, owner: string, options?: ExternalClaimOptions): ExternalClaim<A, I>[];
  /** Extend current leases. Returns each lease's new expiry, or null when it was lost. */
  renew(ctx: MutationContext, leases: readonly ExternalLease<A>[], options?: { readonly leaseMs?: number }): (number | null)[];
  /** Give a key back, claimable again after delayMs. False when the lease was already lost. */
  release(ctx: MutationContext, lease: ExternalLease<A>, options?: { readonly delayMs?: number }): boolean;
  ready(ctx: Context): boolean;
  stats(ctx: Context): ExternalStats;
  http<const Prefix extends string>(prefix: Prefix, options?: { readonly access?: Access }): ExternalHttp<Prefix, A, I, R, Pool>;
}

type StaleRow = { args: Json; since: number; key?: string; attempt?: number; owner?: string };
const leaseShape = { args: v.json(), key: v.string(), owner: v.string({ min: 1 }), attempt: v.int({ min: 1 }) };

function shardOf(key: string, count: number): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < key.length; index++) hash = Math.imul(hash ^ key.charCodeAt(index), 0x01000193) >>> 0;
  return hash % count;
}

/**
 * Keep a result current for each input, computed by external workers.
 * input() derives everything that affects the result; returning null means no work.
 * With each, writes to that collection's rows mark their keys for worker pools.
 */
export function external<A extends Json, I extends Json = Json, R extends Json = Json>(name: string, options: {
  readonly input: (ctx: Context, args: A) => I | null;
  readonly result?: SchemaLike<R>;
  readonly each: Collection<any, A, any>;
  readonly lease?: { readonly defaultMs?: number; readonly maxMs?: number };
}): External<A, I, R, true>;
export function external<A = string, I extends Json = Json, R extends Json = Json>(name: string, options: {
  readonly input: (ctx: Context, args: A) => I | null;
  readonly result?: SchemaLike<R>;
}): External<A, I, R, false>;
export function external<A, I extends Json, R extends Json>(name: string, options: {
  readonly input: (ctx: Context, args: A) => I | null;
  readonly result?: SchemaLike<R>;
  readonly each?: Collection<any, any, any>;
  readonly lease?: { readonly defaultMs?: number; readonly maxMs?: number };
}): External<A, I, R, boolean> {
  requireName(name, "External value name");
  const settings = plainObject(options, "External options", ["input", "result", "each", "lease"]);
  if (typeof settings.input !== "function") throw new TypeError("An external value requires an input function");
  const describe = settings.input as (ctx: Context, args: A) => I | null;
  const resultSchema = settings.result === undefined ? null : adopt(settings.result as SchemaLike<R>);
  const each = settings.each as Collection<any, A & Json, any> | undefined;
  if (settings.lease !== undefined && !each) throw new TypeError("Leases need an external value that tracks a collection");
  const leaseSettings = plainObject(settings.lease ?? {}, "Lease options", ["defaultMs", "maxMs"]);
  const maxLeaseMs = (leaseSettings.maxMs ?? 300_000) as number;
  const defaultLeaseMs = (leaseSettings.defaultMs ?? Math.min(30_000, maxLeaseMs)) as number;
  for (const [label, value] of [["lease.maxMs", maxLeaseMs], ["lease.defaultMs", defaultLeaseMs]] as const) {
    if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be a positive safe integer`);
  }
  if (defaultLeaseMs > maxLeaseMs) throw new RangeError("The default lease exceeds the maximum");
  const results = collection<{ key: string; value: R }>(`${name}.results`);
  // One row per stale key. since orders claims: when the key became stale, or when its
  // lease or retry delay ends. key and attempt count claims of one input; owner marks a
  // live lease, which expires at since.
  const stale = collection<StaleRow>(`${name}.stale`).index("since", ["since"]);
  const rowKey = (args: A) => canonicalJson(args as Json);
  const desired = derive(`${name}.input`, (ctx, args: A) => {
    const input = describe(ctx, args);
    return input === null ? null : { key: canonicalJson([args as Json, input]), input };
  });
  const state = (ctx: Context, args: A): ExternalState<R> | null => {
    const wanted = ctx.get(desired, args);
    if (!wanted) return null;
    const stored = ctx.get(results, rowKey(args));
    return stored?.key === wanted.key ? { status: "ready", value: stored.value } : { status: "pending" };
  };
  function pending(ctx: Context, args: A): ExternalWork<A, I> | null {
    const wanted = ctx.get(desired, args);
    if (!wanted || ctx.get(results, rowKey(args))?.key === wanted.key) return null;
    return { args, key: wanted.key, input: wanted.input };
  }
  function publish(ctx: MutationContext, work: { readonly args: A; readonly key: string; readonly value: R }) {
    const { args, key, value } = plainObject(work, "External result", ["args", "key", "value"]) as { args: A; key: string; value: R };
    if (resultSchema) {
      try { resultSchema.parse(value); }
      catch (error) {
        if (error instanceof ValidationError) fail("INVALID_ARGUMENT", `value ${error.message}`, { path: [...error.path] });
        throw error;
      }
    } else canonicalJson(value);
    const wanted = ctx.get(desired, args);
    if (!wanted || wanted.key !== key) return { accepted: false };
    if (ctx.get(results, rowKey(args))?.key !== key) ctx.set(results, rowKey(args), { key, value });
    if (each) ctx.delete(stale, rowKey(args));
    return { accepted: true };
  }
  function next(ctx: Context, nextOptions: ExternalNextOptions = {}): ExternalWork<A, I>[] {
    if (!each) throw new TypeError(`External value ${name} does not track a collection`);
    const choice = plainObject(nextOptions, "Next options", ["limit", "shard"]);
    const limit = choice.limit ?? 16;
    if (!Number.isSafeInteger(limit) || (limit as number) < 1) fail("INVALID_ARGUMENT", "limit must be a positive safe integer");
    const shard = choice.shard as readonly [number, number] | undefined;
    if (shard !== undefined && (!Array.isArray(shard) || shard.length !== 2 || !Number.isSafeInteger(shard[1]) || shard[1] < 1 ||
        !Number.isSafeInteger(shard[0]) || shard[0] < 0 || shard[0] >= shard[1])) fail("INVALID_ARGUMENT", "shard must be [index, count] with index < count");
    const work: ExternalWork<A, I>[] = [];
    let after: string | undefined;
    do {
      const page = ctx.range(stale.by("since").range({ limit: 64, ...(after === undefined ? {} : { after }) }));
      for (const row of page.rows) {
        if (shard && shardOf(row.key, shard[1]) !== shard[0]) continue;
        const found = pending(ctx, row.value.args as A);
        if (found) work.push(found);
        if (work.length === limit) return work;
      }
      after = page.cursor ?? undefined;
    } while (after !== undefined);
    return work;
  }
  function pool(): void {
    if (!each) throw new TypeError(`External value ${name} does not track a collection`);
  }
  function duration(value: unknown, label: string, minimum: number): number {
    if (!Number.isSafeInteger(value) || (value as number) < minimum) fail("INVALID_ARGUMENT", `${label} must be a safe integer at least ${minimum}`);
    return value as number;
  }
  function leaseLength(value: unknown): number {
    const leaseMs = duration(value ?? defaultLeaseMs, "leaseMs", 1);
    if (leaseMs > maxLeaseMs) fail("LEASE_TOO_LONG", `Leases last at most ${maxLeaseMs} ms`);
    return leaseMs;
  }
  function held(ctx: Context, lease: ExternalLease<A>, now: number): { id: string; row: StaleRow } | null {
    const { args, key, owner, attempt } = plainObject(lease, "Lease") as unknown as ExternalLease<A>;
    requireName(owner, "Lease owner");
    if (typeof key !== "string") fail("INVALID_ARGUMENT", "key must be a string");
    duration(attempt, "attempt", 1);
    const id = rowKey(args);
    const row = ctx.get(stale, id);
    if (!row || row.owner !== owner || row.key !== key || row.attempt !== attempt || now >= row.since) return null;
    return { id, row };
  }
  function claim(ctx: MutationContext, owner: string, claimOptions: ExternalClaimOptions = {}): ExternalClaim<A, I>[] {
    pool();
    requireName(owner, "Lease owner");
    const choice = plainObject(claimOptions, "Claim options", ["limit", "leaseMs"]);
    const limit = duration(choice.limit ?? 16, "limit", 1);
    const leaseMs = leaseLength(choice.leaseMs);
    const now = ctx.clock();
    const claims: ExternalClaim<A, I>[] = [];
    // Every row read is claimed or dropped, so each claim makes progress.
    for (const row of ctx.range(stale.by("since").range({ lte: now, limit })).rows) {
      const found = pending(ctx, row.value.args as A);
      if (!found) { ctx.delete(stale, row.key); continue; }
      const attempt = (row.value.key === found.key ? row.value.attempt ?? 0 : 0) + 1;
      const expiresAt = now + leaseMs;
      ctx.set(stale, row.key, { args: row.value.args, since: expiresAt, key: found.key, attempt, owner });
      claims.push({ ...found, owner, attempt, expiresAt });
    }
    return claims;
  }
  function renew(ctx: MutationContext, leases: readonly ExternalLease<A>[], renewOptions: { readonly leaseMs?: number } = {}): (number | null)[] {
    pool();
    if (!Array.isArray(leases)) throw new TypeError("renew takes an array of leases");
    const leaseMs = leaseLength(plainObject(renewOptions, "Renew options", ["leaseMs"]).leaseMs);
    const now = ctx.clock();
    return leases.map((lease) => {
      const current = held(ctx, lease, now);
      if (!current) return null;
      ctx.set(stale, current.id, { ...current.row, since: now + leaseMs });
      return now + leaseMs;
    });
  }
  function release(ctx: MutationContext, lease: ExternalLease<A>, releaseOptions: { readonly delayMs?: number } = {}): boolean {
    pool();
    const delayMs = duration(plainObject(releaseOptions, "Release options", ["delayMs"]).delayMs ?? 0, "delayMs", 0);
    const now = ctx.clock();
    const current = held(ctx, lease, now);
    if (!current) return false;
    const { owner: _, ...rest } = current.row;
    ctx.set(stale, current.id, { ...rest, since: now + delayMs });
    return true;
  }
  // When a lease or retry delay next ends. Time only adds claimable keys.
  function upcoming(ctx: Context, now: number): number | null {
    const next = ctx.range(stale.by("since").range({ gt: now, limit: 1 })).rows[0]?.value.since ?? null;
    ctx.changesAt(next);
    return next;
  }
  function ready(ctx: Context): boolean {
    pool();
    const now = ctx.clock();
    if (ctx.range(stale.by("since").range({ lte: now, limit: 1 })).rows.length > 0) return true;
    upcoming(ctx, now);
    return false;
  }
  function stats(ctx: Context): ExternalStats {
    pool();
    const now = ctx.clock();
    const oldest = ctx.range(stale.by("since").range({ lte: now, limit: 1 })).rows[0]?.value.since ?? null;
    return { ready: oldest !== null, oldestReadyAt: oldest, nextAvailableAt: upcoming(ctx, now) };
  }
  const value = derive(name, state);
  const tracking = each ? [trigger(`external:${name}`, each, (ctx, change) => {
    const args = change.key as A;
    const key = rowKey(args);
    if (!ctx.get(desired, args) && ctx.get(results, key) !== null) ctx.delete(results, key);
    const found = pending(ctx, args);
    const marker = ctx.get(stale, key);
    if (found) {
      // A claim or retry delay for an older input no longer applies: the new input is claimable at once.
      if (marker === null || (marker.key !== undefined && marker.key !== found.key)) ctx.set(stale, key, { args: args as Json, since: ctx.clock() });
    } else if (marker !== null) ctx.delete(stale, key);
  })] : [];
  const parts = component({ collections: [results, ...(each ? [stale] : [])], definitions: [desired, value], triggers: tracking });
  const generated = new Map<string, { access: Access | undefined; methods: Record<string, QueryMethod<any, any> | MutationMethod<any, any>> }>();
  return Object.freeze({
    ...value,
    component: parts,
    results,
    pending,
    publish,
    next,
    claim,
    renew,
    release,
    ready,
    stats,
    http(prefix: string, httpOptions: { readonly access?: Access } = {}) {
      requireName(prefix, "HTTP prefix");
      const { access } = plainObject(httpOptions, "External HTTP options", ["access"]) as { access?: Access };
      const cached = generated.get(prefix);
      if (cached) {
        if (cached.access !== access) throw new TypeError(`External methods for ${JSON.stringify(prefix)} were already generated differently`);
        return cached.methods;
      }
      const spec = <S>(args: SchemaLike<S>) => ({ args, ...(access === undefined ? {} : { access }) });
      const methods: Record<string, QueryMethod<any, any> | MutationMethod<any, any>> = {
        [`${prefix}.pending`]: query(`${prefix}.pending`, access === undefined ? {} : { access }, (ctx, args: A) => pending(ctx, args)),
        [`${prefix}.publish`]: mutation(`${prefix}.publish`, spec(v.object({ args: v.json(), key: v.string(), value: v.json() })),
          (ctx, work) => publish(ctx, work as { args: A; key: string; value: R })),
        ...(each ? {
          [`${prefix}.next`]: query(`${prefix}.next`,
            spec(v.nullable(v.object({ limit: v.optional(v.int({ min: 1, max: 1024 })), shard: v.optional(v.tuple([v.int({ min: 0 }), v.int({ min: 1 })])) }))),
            (ctx, args) => next(ctx, args ?? {})),
          [`${prefix}.claim`]: mutation(`${prefix}.claim`,
            spec(v.object({ owner: v.string({ min: 1 }), limit: v.optional(v.int({ min: 1, max: 1024 })), leaseMs: v.optional(v.int({ min: 1 })) })),
            (ctx, { owner, ...rest }) => claim(ctx, owner, rest)),
          [`${prefix}.renew`]: mutation(`${prefix}.renew`,
            spec(v.object({ leases: v.array(v.object(leaseShape), { max: 1024 }), leaseMs: v.optional(v.int({ min: 1 })) })),
            (ctx, { leases, ...rest }) => renew(ctx, leases as ExternalLease<A>[], rest)),
          [`${prefix}.release`]: mutation(`${prefix}.release`,
            spec(v.object({ ...leaseShape, delayMs: v.optional(v.int({ min: 0 })) })),
            (ctx, { delayMs, ...lease }) => release(ctx, lease as ExternalLease<A>, delayMs === undefined ? {} : { delayMs })),
          [`${prefix}.ready`]: query(`${prefix}.ready`, spec(v.nullable(v.object({}))), (ctx) => ready(ctx)),
          [`${prefix}.stats`]: query(`${prefix}.stats`, spec(v.nullable(v.object({}))), (ctx) => stats(ctx)),
        } : {}),
      };
      generated.set(prefix, { access, methods: Object.freeze(methods) });
      return methods;
    },
  }) as unknown as External<A, I, R, boolean>;
}
