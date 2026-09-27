import { canonicalJson, type Json } from "./json.ts";
import { collection, component, fail, mutation, plainObject, query, requireName, task } from "./core.ts";
import type {
  Access, Collection, Component, Context, HistoryIdentity, MutationContext, MutationMethod, QueryContext, QueryMethod, RangeOptions, RangeQuery, Row,
} from "./core.ts";
import { schema as adopt, v, ValidationError, type Optional, type Schema, type SchemaLike } from "./schema.ts";

function* pages<T, K>(ctx: Context, index: { range(options: RangeOptions): RangeQuery<T, K> }, bounds: Omit<RangeOptions, "limit" | "after">): Generator<Row<T, K>> {
  let after: string | undefined;
  do {
    const page = ctx.range(index.range({ ...bounds, limit: 64, ...(after === undefined ? {} : { after }) }));
    yield* page.rows;
    after = page.cursor ?? undefined;
  } while (after !== undefined);
}

function integer(value: unknown, label: string, minimum = 0): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    throw new TypeError(`${label} must be a safe integer at least ${minimum}`);
  }
}

// Every read below reports when its result next changes through ctx.changesAt,
// so watches of these helpers wake exactly then instead of polling.
function clock(ctx: Context): number {
  const now = ctx.clock();
  integer(now, "Server time");
  return now;
}

function reserved(name: unknown, label: string): asserts name is string {
  requireName(name, label);
  if (name.startsWith("$flower.")) throw new TypeError(`${label}s beginning with $flower. are reserved`);
}

function validated<T>(schema: Schema<T> | null, value: unknown, label: string): T {
  if (schema) {
    try { schema.parse(value); }
    catch (error) {
      if (error instanceof ValidationError) fail("INVALID_ARGUMENT", `${label} ${error.message}`, { path: [...error.path] });
      throw error;
    }
  } else canonicalJson(value);
  return value as T;
}

// ---- Expiring collections

/** Millisecond deadlines. null disables expiration. */
export type Expiration = null | { at: number } | { afterCreationMs: number } | { afterUpdateMs: number };
export interface ExpiringEntry<T> { value: T; createdAt: number; updatedAt: number; expiresAt: number | null }

function deadline(policy: Expiration, createdAt: number, updatedAt: number): number | null {
  if (policy === null) return null;
  const rule = plainObject(policy, "Expiration policy", ["at", "afterCreationMs", "afterUpdateMs"]);
  const keys = Object.keys(rule);
  if (keys.length !== 1) throw new TypeError("Expiration requires exactly one deadline rule");
  const value = rule[keys[0]];
  integer(value, "Expiration time or duration");
  const result = keys[0] === "at" ? value : (keys[0] === "afterCreationMs" ? createdAt : updatedAt) + value;
  integer(result, "Deadline");
  return result;
}

export interface ExpiringCollection<T> extends Component {
  readonly records: Collection<ExpiringEntry<T>, string, { readonly expiry: readonly ["expiresAt"] }>;
  entry(ctx: Context, key: string): ExpiringEntry<T> | null;
  get(ctx: Context, key: string): T | null;
  scan(ctx: Context): Row<T>[];
  set(ctx: MutationContext, key: string, value: T, expiration?: Expiration): ExpiringEntry<T>;
  delete(ctx: MutationContext, key: string): void;
}

/** Reads hide records at ctx.clock() >= expiresAt; a maintenance task reclaims their storage. */
export function expiringCollection<T = Json>(name: string, options: { readonly expiration?: Expiration; readonly value?: SchemaLike<T> } = {}): ExpiringCollection<T> {
  reserved(name, "Collection name");
  const settings = plainObject(options, "Expiration configuration", ["expiration", "value"]);
  const configured = (settings.expiration ?? null) as Expiration;
  deadline(configured, 0, 0);
  // Snapshot the validated default so later changes to the caller's object have no effect.
  const fallback = configured === null ? null : Object.freeze({ ...configured }) as Expiration;
  const valueSchema = settings.value === undefined ? null : adopt(settings.value as SchemaLike<T>);
  const records = collection<ExpiringEntry<T>>(name).index("expiry", ["expiresAt"]);
  const live = (entry: ExpiringEntry<T> | null, now: number): entry is ExpiringEntry<T> =>
    entry !== null && (entry.expiresAt === null || now < entry.expiresAt);
  // A live entry disappears from reads at its expiry.
  function visible(ctx: Context, entry: ExpiringEntry<T> | null, now: number): entry is ExpiringEntry<T> {
    if (!live(entry, now)) return false;
    ctx.changesAt(entry.expiresAt);
    return true;
  }
  function entry(ctx: Context, key: string): ExpiringEntry<T> | null {
    requireName(key, "Key");
    const stored = ctx.get(records, key);
    return visible(ctx, stored, clock(ctx)) ? stored : null;
  }
  const sweep = task(`expiring:${name}`, {
    due: (ctx) => ctx.range(records.by("expiry").range({ gte: 0, limit: 1 })).rows[0]?.value.expiresAt ?? null,
    run(ctx) {
      const now = clock(ctx);
      const rows = ctx.range(records.by("expiry").range({ gte: 0, lte: now, limit: 64 })).rows;
      for (const row of rows) ctx.delete(records, row.key);
      return { expired: rows.length };
    },
  });
  return Object.freeze({
    ...component({ collections: [records], tasks: [sweep] }),
    records,
    entry,
    get(ctx: Context, key: string): T | null { return entry(ctx, key)?.value ?? null; },
    scan(ctx: Context): Row<T>[] {
      const now = clock(ctx);
      return ctx.scan(records).filter((row) => visible(ctx, row.value, now)).map((row) => ({ key: row.key, value: row.value.value }));
    },
    set(ctx: MutationContext, key: string, value: T, expiration: Expiration = fallback): ExpiringEntry<T> {
      requireName(key, "Key");
      validated(valueSchema, value, "Value");
      const now = clock(ctx);
      const previous = ctx.get(records, key);
      const createdAt = live(previous, now) ? previous.createdAt : now;
      const next: ExpiringEntry<T> = { value, createdAt, updatedAt: now, expiresAt: deadline(expiration, createdAt, now) };
      ctx.set(records, key, next);
      return next;
    },
    delete(ctx: MutationContext, key: string): void {
      requireName(key, "Key");
      ctx.delete(records, key);
    },
  }) as unknown as ExpiringCollection<T>;
}

// ---- Work queues

export interface Lease { owner: string; token: number; expiresAt: number; history?: HistoryIdentity }
export interface Job<P = Json, R = Json> {
  scope: string;
  id: string;
  payload: P;
  state: "pending" | "leased" | "completed" | "failed";
  /** When a pending job may be claimed; null otherwise. */
  availableAt: number | null;
  /** Indexed mirror of lease.expiresAt; null unless leased. */
  leaseExpiresAt: number | null;
  lease: Lease | null;
  attempts: number;
  createdAt: number;
  updatedAt: number;
  result: R | null;
  error: Json;
  /** Lower goes first. */
  priority: number;
  /** Jobs of one group take turns with other groups'; null: the job takes the next turn on its own. */
  group: string | null;
  /** The job's place in its priority's order, given when it was enqueued. */
  turn: number;
  /** Indexed: "now" for a pending job available when it was written, "later" for one delayed then, else null. */
  queued: "now" | "later" | null;
}
export interface Claim<P = Json> extends Lease { scope: string; id: string; payload: P; attempt: number }
export interface LeaseIdentity { id: string; owner: string; token: number; history?: HistoryIdentity }
export interface QueueRetry { readonly maxAttempts?: number; readonly initialDelayMs?: number; readonly maxDelayMs?: number }
export interface QueueOptions<P, R> {
  readonly lease?: { readonly defaultMs?: number; readonly maxMs?: number };
  /** Automatic retries after fail() or an expired lease. false makes fail() final. */
  readonly retry?: QueueRetry | false;
  readonly payload?: SchemaLike<P>;
  readonly result?: SchemaLike<R>;
  /** How long the process first in line has new work to itself before any process may claim it. Default 1,000. */
  readonly turnMs?: number;
}
export interface QueueStats {
  /** A claim would succeed now. */
  readonly ready: boolean;
  /** When the longest-waiting claimable job became available. */
  readonly oldestReadyAt: number | null;
  /** When a delayed job or running lease next makes work available. */
  readonly nextAvailableAt: number | null;
  /** Jobs a claim could take now, counted up to `countUpTo`: that number means at least as many. */
  readonly readyCount: number;
  /** Jobs held under a lease that hasn't run out, counted the same way. */
  readonly leasedCount: number;
  /** Pending jobs that become available later, counted the same way. */
  readonly delayedCount: number;
}
export interface QueueStatsOptions {
  /** How far stats counts each kind of job: every counted job is read, so counting costs. Default 100, at most 10,000. */
  readonly countUpTo?: number;
}
export interface EnqueueOptions {
  readonly delayMs?: number;
  readonly at?: number;
  readonly replace?: boolean;
  /** Lower goes first: a job waits while any of higher priority is ready. Default 0. */
  readonly priority?: number;
  /** Groups take turns: each ready group's next job goes before any group's second. */
  readonly group?: string;
}
export interface ClaimOptions<P = Json> {
  /** Jobs to take at most. Default 1; 0 takes none, to leave the line with waitMs 0. */
  readonly max?: number;
  readonly leaseMs?: number;
  /**
   * Wait in the scope's line for this long when the claim takes fewer than `max`: while it waits,
   * `ready(ctx, owner)` tells this owner first when work comes. A claim that takes `max`, or passes 0,
   * leaves the line. Claim again before it ends to keep waiting, and keep your place.
   */
  readonly waitMs?: number;
  /** Checks each job before handing it out; a job it refuses is cancelled, and the claim takes the next. */
  readonly admit?: (claim: Claim<P>) => boolean;
}

export interface QueueView<P = Json, R = Json> {
  enqueue(ctx: MutationContext, id: string, payload: P, options?: EnqueueOptions): Job<P, R>;
  /** Lease the next job, or return null. */
  claim(ctx: MutationContext, owner: string, options?: { readonly leaseMs?: number }): Claim<P> | null;
  /** Lease up to max jobs, in order, and join or leave the line. */
  claimMany(ctx: MutationContext, owner: string, options?: ClaimOptions<P>): Claim<P>[];
  /** Extend a current lease; the fencing token stays the same. */
  renew(ctx: MutationContext, lease: LeaseIdentity, options?: { readonly leaseMs?: number }): Claim<P>;
  /** Extend each current lease: its new expiry, or null when it was lost. */
  renewMany(ctx: MutationContext, leases: readonly LeaseIdentity[], options?: { readonly leaseMs?: number }): (number | null)[];
  complete(ctx: MutationContext, lease: LeaseIdentity, result: R): Job<P, R>;
  /** Retries with backoff unless retry is false or attempts are exhausted. */
  fail(ctx: MutationContext, lease: LeaseIdentity, error: Json, options?: { readonly retry?: boolean; readonly delayMs?: number }): Job<P, R>;
  /** Hand a leased job back, claimable again after delayMs (0), keeping its turn and its attempt count, with no error. */
  release(ctx: MutationContext, lease: LeaseIdentity, options?: { readonly delayMs?: number }): Job<P, R>;
  /** Requeue a failed job with a fresh attempt budget. */
  retry(ctx: MutationContext, id: string, options?: { readonly delayMs?: number }): Job<P, R>;
  cancel(ctx: MutationContext, id: string): boolean;
  get(ctx: Context, id: string): Job<P, R> | null;
  scan(ctx: Context): Job<P, R>[];
  /** A claim would find work now; with an owner, only once it is that owner's turn in the line. */
  ready(ctx: Context, owner?: string): boolean;
  stats(ctx: Context, options?: QueueStatsOptions): QueueStats;
}

type QueueIndexes = {
  readonly ready: readonly ["scope", "state", "availableAt"];
  readonly turns: readonly ["scope", "queued", "priority", "turn", "availableAt"];
  readonly later: readonly ["scope", "queued", "availableAt"];
  readonly leases: readonly ["scope", "state", "leaseExpiresAt"];
  readonly expiry: readonly ["state", "leaseExpiresAt"];
};
/** One owner waiting in a scope's line. */
export interface QueueWaiter { scope: string; owner: string; room: number; since: number; expiresAt: number }
// With scope: "argument" every method takes the scope; otherwise none accepts one.
type ScopeArgs<A extends boolean> = A extends true ? { scope: string } : unknown;
type LeaseArgs<A extends boolean> = LeaseIdentity & ScopeArgs<A>;
export type QueueMethodName = "enqueue" | "claim" | "renew" | "complete" | "fail" | "release" | "retry" | "cancel" | "get" | "ready" | "stats";
export interface QueueMethods<P, R, A extends boolean = false> {
  enqueue: MutationMethod<{ id: string; payload: P; delayMs?: number; at?: number; replace?: boolean; priority?: number; group?: string } & ScopeArgs<A>, Job<P, R>>;
  claim: MutationMethod<{ owner: string; leaseMs?: number; max?: number; waitMs?: number } & ScopeArgs<A>, (Claim<P> & { more?: Claim<P>[] }) | null>;
  renew: MutationMethod<{ leases: LeaseIdentity[]; leaseMs?: number } & ScopeArgs<A>, (number | null)[]>;
  complete: MutationMethod<LeaseArgs<A> & { result: R }, Job<P, R>>;
  fail: MutationMethod<LeaseArgs<A> & { error: Json; retry?: boolean; delayMs?: number }, Job<P, R>>;
  release: MutationMethod<LeaseArgs<A> & { delayMs?: number }, Job<P, R>>;
  retry: MutationMethod<{ id: string; delayMs?: number } & ScopeArgs<A>, Job<P, R>>;
  cancel: MutationMethod<{ id: string } & ScopeArgs<A>, boolean>;
  get: QueryMethod<{ id: string } & ScopeArgs<A>, Job<P, R> | null>;
  ready: QueryMethod<(A extends true ? { scope: string; owner?: string } : { owner?: string }) | null, boolean>;
  stats: QueryMethod<(A extends true ? { scope: string; countUpTo?: number } : { countUpTo?: number }) | null, QueueStats>;
}
const workerMethods = ["claim", "renew", "complete", "fail", "release", "get", "ready", "stats"] as const;
export type QueueHttp<Prefix extends string, P, R, M extends QueueMethodName, A extends boolean = false> = { readonly [K in M as `${Prefix}.${K}`]: QueueMethods<P, R, A>[K] };
export interface QueueHttpOptions<M extends QueueMethodName> {
  /** Which methods to expose. Defaults to the worker set: claim, renew, complete, fail, release, get, ready, stats. */
  readonly methods?: readonly M[];
  /** Scope source: a caller argument, a function of the authenticated context, or the default scope. */
  readonly scope?: "argument" | ((ctx: QueryContext) => string);
  readonly access?: Access;
}

export interface Queue<P = Json, R = Json> extends QueueView<P, R>, Component {
  readonly name: string;
  readonly records: Collection<Job<P, R>, [scope: string, id: string], QueueIndexes>;
  /** Owners waiting for work, by scope. */
  readonly line: Collection<QueueWaiter, [scope: string, owner: string], { readonly order: readonly ["scope", "since", "owner"]; readonly expiry: readonly ["expiresAt"] }>;
  /** The same queue restricted to one namespace of the shared collection. */
  scope(name: string): QueueView<P, R>;
  /** Public methods for workers; spread into define({ http }). */
  http<const Prefix extends string, const M extends QueueMethodName = typeof workerMethods[number]>(
    prefix: Prefix, options: QueueHttpOptions<M> & { readonly scope: "argument" }): QueueHttp<Prefix, P, R, M, true>;
  http<const Prefix extends string, const M extends QueueMethodName = typeof workerMethods[number]>(
    prefix: Prefix, options?: QueueHttpOptions<M> & { readonly scope?: (ctx: QueryContext) => string }): QueueHttp<Prefix, P, R, M>;
}

function leaseError(): never {
  return fail("LEASE_LOST", "Job lease is missing, expired, or held by another claim");
}

const fencing = collection<{ last: number }>("$flower.fencing");
/** Per queue, scope and priority: the turn last claimed (`at`), and each group's next turn. */
const turns = collection<{ at: number }>("$flower.turns");
const leaseSchema = {
  id: v.string({ min: 1 }), owner: v.string({ min: 1 }), token: v.int({ min: 1 }),
  history: v.optional(v.object({ database: v.string(), incarnation: v.string() })),
};
/** Claims one call may take, and renewals one call may send. */
const MAX_CLAIMS = 64;
const MAX_RENEWALS = 1_024;
/** Due delayed jobs one claim moves into turn order. */
const PROMOTE = 64;
/** How far stats counts each kind of job, by default and at most. */
const COUNT_UP_TO = 100;
const MAX_COUNT_UP_TO = 10_000;

/** Leased durable work with fencing tokens, retries, delays and renewal, in ordinary records. */
export function queue<P = Json, R = Json>(name: string, options: QueueOptions<P, R> = {}): Queue<P, R> {
  reserved(name, "Queue name");
  const settings = plainObject(options, "Queue options", ["lease", "retry", "payload", "result", "turnMs"]);
  const leaseSettings = plainObject(settings.lease ?? {}, "Lease options", ["defaultMs", "maxMs"]);
  const maxLeaseMs = (leaseSettings.maxMs ?? 300_000) as number;
  integer(maxLeaseMs, "lease.maxMs", 1);
  const defaultLeaseMs = (leaseSettings.defaultMs ?? Math.min(30_000, maxLeaseMs)) as number;
  integer(defaultLeaseMs, "lease.defaultMs", 1);
  if (defaultLeaseMs > maxLeaseMs) throw new RangeError("The default lease exceeds the maximum");
  const turnMs = (settings.turnMs ?? 1_000) as number;
  integer(turnMs, "turnMs");
  let policy: Required<QueueRetry> | null = null;
  if (settings.retry !== false) {
    const retry = plainObject(settings.retry ?? {}, "Retry options", ["maxAttempts", "initialDelayMs", "maxDelayMs"]);
    policy = { maxAttempts: (retry.maxAttempts ?? 5) as number, initialDelayMs: (retry.initialDelayMs ?? 1_000) as number, maxDelayMs: (retry.maxDelayMs ?? 60_000) as number };
    integer(policy.maxAttempts, "retry.maxAttempts", 1);
    integer(policy.initialDelayMs, "retry.initialDelayMs", 0);
    integer(policy.maxDelayMs, "retry.maxDelayMs", policy.initialDelayMs);
  }
  const payloadSchema = settings.payload === undefined ? null : adopt(settings.payload as SchemaLike<P>);
  const resultSchema = settings.result === undefined ? null : adopt(settings.result as SchemaLike<R>);
  const records = collection<Job<P, R>>(name)
    .key(v.tuple([v.string(), v.string({ min: 1 })]))
    .index("ready", ["scope", "state", "availableAt"])
    .index("turns", ["scope", "queued", "priority", "turn", "availableAt"])
    .index("later", ["scope", "queued", "availableAt"])
    .index("leases", ["scope", "state", "leaseExpiresAt"])
    .index("expiry", ["state", "leaseExpiresAt"]);
  const line = collection<QueueWaiter>(`${name}.line`)
    .key(v.tuple([v.string(), v.string({ min: 1 })]))
    .index("order", ["scope", "since", "owner"])
    .index("expiry", ["expiresAt"]);
  const backoff = (attempts: number) => Math.min(policy!.maxDelayMs, policy!.initialDelayMs * 2 ** Math.min(attempts - 1, 30));
  // Jobs stored before turns existed have none: they go first.
  const priorityOf = (job: Job<P, R>) => job.priority ?? 0;
  const turnOf = (job: Job<P, R>) => job.turn ?? -1;
  const queuedAt = (availableAt: number, now: number): "now" | "later" => availableAt <= now ? "now" : "later";

  function effective(job: Job<P, R>, now: number): Job<P, R> {
    if (job.state !== "leased" || now < job.lease!.expiresAt) return job;
    const expiredAt = job.lease!.expiresAt;
    const exhausted = policy !== null && job.attempts >= policy.maxAttempts;
    return {
      ...job, state: exhausted ? "failed" : "pending", lease: null, leaseExpiresAt: null,
      availableAt: exhausted ? null : expiredAt, updatedAt: expiredAt, queued: exhausted ? null : "now",
      error: { code: "LEASE_EXPIRED", message: "Worker lease expired", at: expiredAt },
    };
  }

  function view(scope: string): QueueView<P, R> {
    if (typeof scope !== "string") throw new TypeError("Queue scope must be a string");
    const key = (id: string): [string, string] => { requireName(id, "Job ID"); return [scope, id]; };
    const clockKey = (priority: number) => canonicalJson([name, scope, priority]);
    const groupKey = (priority: number, group: string) => canonicalJson([name, scope, priority, group]);
    function holds(ctx: MutationContext, identity: LeaseIdentity, now: number): Job<P, R> | null {
      plainObject(identity, "Lease identity");
      integer(identity.token, "Lease token", 1);
      const job = ctx.get(records, key(identity.id));
      const history = ctx.history();
      if (!job || job.state !== "leased" || job.lease!.owner !== identity.owner || job.lease!.token !== identity.token ||
          now >= job.lease!.expiresAt || canonicalJson(identity.history ?? null) !== canonicalJson(history) ||
          canonicalJson(job.lease!.history ?? null) !== canonicalJson(history)) return null;
      return job;
    }
    const held = (ctx: MutationContext, identity: LeaseIdentity, now: number): Job<P, R> => holds(ctx, identity, now) ?? leaseError();
    // A running lease changes the job at its expiry, whether or not anyone claims it again.
    function current(ctx: Context, job: Job<P, R>, now: number): Job<P, R> {
      if (job.state === "leased" && now < job.lease!.expiresAt) ctx.changesAt(job.lease!.expiresAt);
      return effective(job, now);
    }
    // When a delayed job or a running lease next makes work available.
    function upcoming(ctx: Context, now: number): number | null {
      const delayed = ctx.range(records.by("ready").range({ prefix: [scope, "pending"], gt: now, limit: 1 })).rows[0]?.value.availableAt ?? null;
      const running = ctx.range(records.by("leases").range({ prefix: [scope, "leased"], gt: now, limit: 1 })).rows[0]?.value.leaseExpiresAt ?? null;
      const times = [delayed, running].filter((time): time is number => time !== null);
      const next = times.length ? Math.min(...times) : null;
      ctx.changesAt(next);
      return next;
    }
    function claimOf(job: Job<P, R>): Claim<P> {
      return { scope, id: job.id, payload: job.payload, ...job.lease!, attempt: job.attempts };
    }
    function* expiredPending(ctx: Context, now: number): Generator<Job<P, R>> {
      for (const row of pages(ctx, records.by("leases"), { prefix: [scope, "leased"], lte: now })) {
        const job = effective(row.value, now);
        if (job.state === "pending") yield job;
      }
    }
    /** When the longest-waiting claimable job became available, or null when none is. */
    function oldestReadyAt(ctx: Context, now: number): number | null {
      let oldest = ctx.range(records.by("ready").range({ prefix: [scope, "pending"], lte: now, limit: 1 })).rows[0]?.value.availableAt ?? null;
      for (const job of expiredPending(ctx, now)) if (oldest === null || job.availableAt! < oldest) oldest = job.availableAt;
      return oldest;
    }
    const before = (a: Job<P, R>, b: Job<P, R>) => {
      const order = [priorityOf(a) - priorityOf(b), turnOf(a) - turnOf(b), a.availableAt! - b.availableAt!].find((difference) => difference !== 0);
      return order !== undefined ? order < 0 : canonicalJson(key(a.id)) < canonicalJson(key(b.id));
    };
    /** The job a claim takes next: by priority, then turn, then how long it has waited. */
    function next(ctx: MutationContext, now: number): Job<P, R> | null {
      // Delayed jobs whose time came join the turn order.
      for (const row of ctx.range(records.by("later").range({ prefix: [scope, "later"], lte: now, limit: PROMOTE })).rows) {
        ctx.set(records, row.key, { ...row.value, queued: "now" });
      }
      let selected = ctx.range(records.by("turns").range({ prefix: [scope, "now"], limit: 1 })).rows[0]?.value ?? null;
      for (const job of expiredPending(ctx, now)) if (selected === null || before(job, selected)) selected = job;
      // Jobs stored before turns existed appear only in the ready index.
      const legacy = ctx.range(records.by("ready").range({ prefix: [scope, "pending"], lte: now, limit: 1 })).rows[0]?.value;
      if (legacy !== undefined && legacy.queued === undefined && (selected === null || before(legacy, selected))) selected = legacy;
      return selected;
    }
    function lease(ctx: MutationContext, owner: string, leaseMs: number, now: number): Claim<P> | null {
      const selected = next(ctx, now);
      if (!selected) return null;
      const counter = canonicalJson([name, scope]);
      const token = (ctx.get(fencing, counter)?.last ?? 0) + 1;
      integer(token, "Fencing token", 1);
      ctx.set(fencing, counter, { last: token });
      const priority = priorityOf(selected);
      const turn = turnOf(selected);
      const last = ctx.get(turns, clockKey(priority));
      if (last === null || last.at < turn) ctx.set(turns, clockKey(priority), { at: turn });
      // A group's record matters only while it has later turns queued.
      if (selected.group !== null && selected.group !== undefined && ctx.get(turns, groupKey(priority, selected.group))?.at === turn + 1) {
        ctx.delete(turns, groupKey(priority, selected.group));
      }
      const history = ctx.history();
      const granted: Lease = { owner, token, expiresAt: now + leaseMs, ...(history === null ? {} : { history }) };
      const job: Job<P, R> = {
        ...selected, state: "leased", availableAt: null, lease: granted, leaseExpiresAt: granted.expiresAt, attempts: selected.attempts + 1,
        updatedAt: now, priority, group: selected.group ?? null, turn, queued: null,
      };
      ctx.set(records, key(selected.id), job);
      return claimOf(job);
    }
    /** The next turn for a job of this priority and group. */
    function place(ctx: MutationContext, priority: number, group: string | null): number {
      const at = ctx.get(turns, clockKey(priority))?.at ?? 0;
      if (group === null) return at;
      const turn = Math.max(at, ctx.get(turns, groupKey(priority, group))?.at ?? 0);
      ctx.set(turns, groupKey(priority, group), { at: turn + 1 });
      return turn;
    }
    function leaseLength(value: unknown): number {
      const leaseMs = (value ?? defaultLeaseMs) as number;
      integer(leaseMs, "Lease duration", 1);
      if (leaseMs > maxLeaseMs) fail("LEASE_TOO_LONG", `Leases last at most ${maxLeaseMs} ms`);
      return leaseMs;
    }
    /** The first owner in line whose place has not run out. */
    function head(ctx: Context, now: number): QueueWaiter | null {
      for (const row of pages(ctx, line.by("order"), { prefix: [scope] })) {
        if (row.value.expiresAt <= now) continue;
        ctx.changesAt(row.value.expiresAt);
        return row.value;
      }
      return null;
    }
    function claimMany(ctx: MutationContext, owner: string, claimOptions: ClaimOptions<P> = {}): Claim<P>[] {
      requireName(owner, "Lease owner");
      const choice = plainObject(claimOptions, "Claim options", ["max", "leaseMs", "waitMs", "admit"]);
      const max = (choice.max ?? 1) as number;
      integer(max, "max");
      const leaseMs = leaseLength(choice.leaseMs);
      const admit = choice.admit as ClaimOptions<P>["admit"];
      if (admit !== undefined && typeof admit !== "function") throw new TypeError("admit must be a function");
      const now = clock(ctx);
      // First in line, yet it let work wait a whole turn: it is gone or stuck, and loses its place to whoever claims instead.
      const first = max > 0 ? head(ctx, now) : null;
      if (first !== null && first.owner !== owner) {
        const since = oldestReadyAt(ctx, now);
        if (since !== null && now >= since + turnMs) ctx.delete(line, [scope, first.owner]);
      }
      const claims: Claim<P>[] = [];
      while (claims.length < max) {
        const claim = lease(ctx, owner, leaseMs, now);
        if (claim === null) break;
        if (admit === undefined || admit(claim)) claims.push(claim);
        else ctx.delete(records, key(claim.id));
      }
      if (choice.waitMs !== undefined) {
        integer(choice.waitMs, "waitMs");
        const spot = [scope, owner] as [string, string];
        const waiting = ctx.get(line, spot);
        if (claims.length < max && choice.waitMs > 0) {
          const since = waiting !== null && waiting.expiresAt > now ? waiting.since : now;
          ctx.set(line, spot, { scope, owner, room: max - claims.length, since, expiresAt: now + choice.waitMs });
        } else if (waiting !== null) ctx.delete(line, spot);
      }
      return claims;
    }
    return Object.freeze({
      enqueue(ctx: MutationContext, id: string, payload: P, enqueueOptions: EnqueueOptions = {}): Job<P, R> {
        const choice = plainObject(enqueueOptions, "Enqueue options", ["delayMs", "at", "replace", "priority", "group"]);
        validated(payloadSchema, payload, "Payload");
        const now = clock(ctx);
        if (choice.delayMs !== undefined && choice.at !== undefined) throw new TypeError("Use delayMs or at, not both");
        if (choice.delayMs !== undefined) integer(choice.delayMs, "delayMs");
        if (choice.at !== undefined) integer(choice.at, "at");
        const priority = (choice.priority ?? 0) as number;
        if (!Number.isSafeInteger(priority)) throw new TypeError("priority must be a safe integer");
        const group = (choice.group ?? null) as string | null;
        if (group !== null) requireName(group, "Group");
        const availableAt = choice.at !== undefined ? choice.at as number : now + ((choice.delayMs as number | undefined) ?? 0);
        const stored = ctx.get(records, key(id));
        const previous = stored && effective(stored, now);
        if (previous !== null && (choice.replace !== true || previous.state === "pending" || previous.state === "leased")) {
          fail("JOB_EXISTS", `Job ${id} already exists`);
        }
        const job: Job<P, R> = {
          scope, id, payload, state: "pending", availableAt, leaseExpiresAt: null, lease: null,
          attempts: 0, createdAt: now, updatedAt: now, result: null, error: null,
          priority, group, turn: place(ctx, priority, group), queued: queuedAt(availableAt, now),
        };
        ctx.set(records, key(id), job);
        return job;
      },
      claim(ctx: MutationContext, owner: string, claimOptions: { readonly leaseMs?: number } = {}): Claim<P> | null {
        const choice = plainObject(claimOptions, "Claim options", ["leaseMs"]);
        return claimMany(ctx, owner, choice.leaseMs === undefined ? {} : { leaseMs: choice.leaseMs as number })[0] ?? null;
      },
      claimMany,
      renew(ctx: MutationContext, identity: LeaseIdentity, renewOptions: { readonly leaseMs?: number } = {}): Claim<P> {
        const leaseMs = leaseLength(plainObject(renewOptions, "Renew options", ["leaseMs"]).leaseMs);
        const now = clock(ctx);
        const job = held(ctx, identity, now);
        const extended = { ...job.lease!, expiresAt: now + leaseMs };
        const renewed: Job<P, R> = { ...job, lease: extended, leaseExpiresAt: extended.expiresAt, updatedAt: now };
        ctx.set(records, key(job.id), renewed);
        return claimOf(renewed);
      },
      renewMany(ctx: MutationContext, leases: readonly LeaseIdentity[], renewOptions: { readonly leaseMs?: number } = {}): (number | null)[] {
        if (!Array.isArray(leases)) throw new TypeError("renewMany takes an array of leases");
        const leaseMs = leaseLength(plainObject(renewOptions, "Renew options", ["leaseMs"]).leaseMs);
        const now = clock(ctx);
        return leases.map((identity) => {
          const job = holds(ctx, identity, now);
          if (job === null) return null;
          const extended = { ...job.lease!, expiresAt: now + leaseMs };
          ctx.set(records, key(job.id), { ...job, lease: extended, leaseExpiresAt: extended.expiresAt, updatedAt: now });
          return extended.expiresAt;
        });
      },
      complete(ctx: MutationContext, identity: LeaseIdentity, result: R): Job<P, R> {
        validated(resultSchema, result, "Result");
        const now = clock(ctx);
        const job = held(ctx, identity, now);
        const completed: Job<P, R> = { ...job, state: "completed", availableAt: null, lease: null, leaseExpiresAt: null, updatedAt: now, result, error: null, queued: null };
        ctx.set(records, key(job.id), completed);
        return completed;
      },
      fail(ctx: MutationContext, identity: LeaseIdentity, error: Json, failOptions: { readonly retry?: boolean; readonly delayMs?: number } = {}): Job<P, R> {
        const choice = plainObject(failOptions, "Fail options", ["retry", "delayMs"]);
        canonicalJson(error);
        if (choice.delayMs !== undefined) integer(choice.delayMs, "delayMs");
        const now = clock(ctx);
        const job = held(ctx, identity, now);
        const final = policy === null || choice.retry === false || job.attempts >= policy.maxAttempts;
        const availableAt = final ? null : now + ((choice.delayMs as number | undefined) ?? backoff(job.attempts));
        // A retried job keeps its turn: it goes before work that came after it.
        const failed: Job<P, R> = {
          ...job, state: final ? "failed" : "pending", lease: null, leaseExpiresAt: null, updatedAt: now, error,
          availableAt, queued: availableAt === null ? null : queuedAt(availableAt, now),
        };
        ctx.set(records, key(job.id), failed);
        return failed;
      },
      release(ctx: MutationContext, identity: LeaseIdentity, releaseOptions: { readonly delayMs?: number } = {}): Job<P, R> {
        const delayMs = plainObject(releaseOptions, "Release options", ["delayMs"]).delayMs ?? 0;
        integer(delayMs, "delayMs");
        const now = clock(ctx);
        const job = held(ctx, identity, now);
        // Like a failure that retries, it keeps its turn and attempt count, but records no error.
        const released: Job<P, R> = {
          ...job, state: "pending", lease: null, leaseExpiresAt: null, updatedAt: now,
          availableAt: now + (delayMs as number), queued: queuedAt(now + (delayMs as number), now),
        };
        ctx.set(records, key(job.id), released);
        return released;
      },
      retry(ctx: MutationContext, id: string, retryOptions: { readonly delayMs?: number } = {}): Job<P, R> {
        const delayMs = plainObject(retryOptions, "Retry options", ["delayMs"]).delayMs ?? 0;
        integer(delayMs, "delayMs");
        const now = clock(ctx);
        const stored = ctx.get(records, key(id));
        const job = stored && effective(stored, now);
        if (!job || job.state !== "failed") fail("JOB_NOT_FAILED", "Only failed jobs can be retried");
        const priority = priorityOf(job);
        const group = job.group ?? null;
        const pending: Job<P, R> = {
          ...job, state: "pending", availableAt: now + delayMs, attempts: 0, updatedAt: now, result: null,
          priority, group, turn: place(ctx, priority, group), queued: queuedAt(now + delayMs, now),
        };
        ctx.set(records, key(id), pending);
        return pending;
      },
      cancel(ctx: MutationContext, id: string): boolean {
        if (ctx.get(records, key(id)) === null) return false;
        ctx.delete(records, key(id));
        return true;
      },
      get(ctx: Context, id: string): Job<P, R> | null {
        const job = ctx.get(records, key(id));
        return job && current(ctx, job, clock(ctx));
      },
      scan(ctx: Context): Job<P, R>[] {
        const now = clock(ctx);
        return Array.from(pages(ctx, records.by("ready"), { prefix: [scope] }), (row) => current(ctx, row.value, now))
          .sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
      },
      ready(ctx: Context, owner?: string): boolean {
        if (owner !== undefined) requireName(owner, "Owner");
        const now = clock(ctx);
        // Time only adds ready jobs, so a true answer holds until the next write.
        const since = oldestReadyAt(ctx, now);
        if (since === null) {
          upcoming(ctx, now);
          return false;
        }
        if (owner === undefined) return true;
        // Whoever waits first in line has new work to itself for turnMs, so one process wakes per job.
        const first = head(ctx, now);
        if (first === null || first.owner === owner || now >= since + turnMs) return true;
        ctx.changesAt(since + turnMs);
        return false;
      },
      stats(ctx: Context, statsOptions: QueueStatsOptions = {}): QueueStats {
        const countUpTo = (plainObject(statsOptions, "Stats options", ["countUpTo"]).countUpTo ?? COUNT_UP_TO) as number;
        integer(countUpTo, "countUpTo", 1);
        if (countUpTo > MAX_COUNT_UP_TO) throw new RangeError(`countUpTo is at most ${MAX_COUNT_UP_TO}`);
        const now = clock(ctx);
        const oldest = oldestReadyAt(ctx, now);
        const count = (index: "ready" | "leases", state: "pending" | "leased", bounds: { lte: number } | { gt: number }) =>
          ctx.range(records.by(index).range({ prefix: [scope, state], ...bounds, limit: countUpTo })).rows;
        // A lease that ran out leaves its job ready, or failed on its last attempt.
        const expired = count("leases", "leased", { lte: now }).filter((row) => effective(row.value, now).state === "pending").length;
        return {
          ready: oldest !== null, oldestReadyAt: oldest, nextAvailableAt: upcoming(ctx, now),
          readyCount: Math.min(countUpTo, count("ready", "pending", { lte: now }).length + expired),
          leasedCount: count("leases", "leased", { gt: now }).length,
          delayedCount: count("ready", "pending", { gt: now }).length,
        };
      },
    });
  }

  const reclaim = task(`queue:${name}`, {
    due: (ctx) => ctx.range(records.by("expiry").range({ prefix: ["leased"], limit: 1 })).rows[0]?.value.leaseExpiresAt ?? null,
    run(ctx) {
      const now = clock(ctx);
      const rows = ctx.range(records.by("expiry").range({ prefix: ["leased"], lte: now, limit: 64 })).rows;
      for (const row of rows) ctx.set(records, row.key, effective(row.value, now));
      return { reclaimed: rows.length };
    },
  });
  const sweep = task(`queue:${name}.line`, {
    due: (ctx) => ctx.range(line.by("expiry").range({ gte: 0, limit: 1 })).rows[0]?.value.expiresAt ?? null,
    run(ctx) {
      const now = clock(ctx);
      const rows = ctx.range(line.by("expiry").range({ gte: 0, lte: now, limit: 64 })).rows;
      for (const row of rows) ctx.delete(line, row.key);
      return { left: rows.length };
    },
  });

  const defaultView = view("");
  const generated = new Map<string, { signature: string; access: unknown; scope: unknown; methods: Readonly<Record<string, MutationMethod<any, any> | QueryMethod<any, any>>> }>();
  function http(prefix: string, httpOptions: QueueHttpOptions<QueueMethodName> = {}) {
    requireName(prefix, "HTTP prefix");
    const choice = plainObject(httpOptions, "Queue HTTP options", ["methods", "scope", "access"]);
    const selected = (choice.methods ?? workerMethods) as readonly QueueMethodName[];
    const scopeSource = choice.scope as QueueHttpOptions<QueueMethodName>["scope"];
    if (scopeSource !== undefined && scopeSource !== "argument" && typeof scopeSource !== "function") throw new TypeError("scope must be \"argument\" or a function");
    const signature = canonicalJson([[...selected].sort(), scopeSource === undefined ? null : typeof scopeSource]);
    const cached = generated.get(prefix);
    if (cached) {
      if (cached.signature !== signature || cached.access !== choice.access || cached.scope !== scopeSource) {
        throw new TypeError(`Queue methods for ${JSON.stringify(prefix)} were already generated differently`);
      }
      return cached.methods;
    }
    const access = choice.access as Access | undefined;
    const spec = (shape: Record<string, SchemaLike<any> | Optional<any>>): { args: Schema<any>; access?: Access } => ({
      args: v.object(scopeSource === "argument" ? { ...shape, scope: v.string() } : shape) as Schema<any>,
      ...(access === undefined ? {} : { access }),
    });
    const countUpTo = v.optional(v.int({ min: 1, max: MAX_COUNT_UP_TO }));
    const counting = {
      args: (scopeSource === "argument" ? v.object({ scope: v.string(), countUpTo }) : v.nullable(v.object({ countUpTo }))) as Schema<any>,
      ...(access === undefined ? {} : { access }),
    };
    const owned = {
      args: (scopeSource === "argument" ? v.object({ scope: v.string(), owner: v.optional(v.string({ min: 1 })) }) : v.nullable(v.object({ owner: v.optional(v.string({ min: 1 })) }))) as Schema<any>,
      ...(access === undefined ? {} : { access }),
    };
    const target = (ctx: QueryContext, args: any) =>
      view(scopeSource === "argument" ? args.scope : typeof scopeSource === "function" ? scopeSource(ctx) : "");
    const pick = (args: any, ...keys: string[]) => Object.fromEntries(keys.filter((key) => args?.[key] !== undefined).map((key) => [key, args[key]]));
    const identity = (args: any): LeaseIdentity => pick(args, "id", "owner", "token", "history") as unknown as LeaseIdentity;
    const factories: Record<QueueMethodName, () => MutationMethod<any, any> | QueryMethod<any, any>> = {
      enqueue: () => mutation(`${prefix}.enqueue`, spec({
        id: v.string({ min: 1 }), payload: v.json(), delayMs: v.optional(v.int({ min: 0 })), at: v.optional(v.int({ min: 0 })), replace: v.optional(v.boolean()),
        priority: v.optional(v.int()), group: v.optional(v.string({ min: 1 })),
      }), (ctx, args: any) => target(ctx, args).enqueue(ctx, args.id, args.payload, pick(args, "delayMs", "at", "replace", "priority", "group"))),
      // Several claims come back as the first, carrying the others in `more`.
      claim: () => mutation(`${prefix}.claim`, spec({
        owner: v.string({ min: 1 }), leaseMs: v.optional(v.int({ min: 1 })), max: v.optional(v.int({ min: 0, max: MAX_CLAIMS })), waitMs: v.optional(v.int({ min: 0 })),
      }), (ctx, args: any) => {
        const [first, ...more] = target(ctx, args).claimMany(ctx, args.owner, pick(args, "leaseMs", "max", "waitMs"));
        return first === undefined ? null : more.length === 0 ? first : { ...first, more };
      }),
      renew: () => mutation(`${prefix}.renew`, spec({ leases: v.array(v.object(leaseSchema), { max: MAX_RENEWALS }), leaseMs: v.optional(v.int({ min: 1 })) }),
        (ctx, args: any) => target(ctx, args).renewMany(ctx, args.leases.map(identity), pick(args, "leaseMs"))),
      complete: () => mutation(`${prefix}.complete`, spec({ ...leaseSchema, result: v.json() }),
        (ctx, args: any) => target(ctx, args).complete(ctx, identity(args), args.result)),
      fail: () => mutation(`${prefix}.fail`, spec({ ...leaseSchema, error: v.json(), retry: v.optional(v.boolean()), delayMs: v.optional(v.int({ min: 0 })) }),
        (ctx, args: any) => target(ctx, args).fail(ctx, identity(args), args.error, pick(args, "retry", "delayMs"))),
      release: () => mutation(`${prefix}.release`, spec({ ...leaseSchema, delayMs: v.optional(v.int({ min: 0 })) }),
        (ctx, args: any) => target(ctx, args).release(ctx, identity(args), pick(args, "delayMs"))),
      retry: () => mutation(`${prefix}.retry`, spec({ id: v.string({ min: 1 }), delayMs: v.optional(v.int({ min: 0 })) }),
        (ctx, args: any) => target(ctx, args).retry(ctx, args.id, pick(args, "delayMs"))),
      cancel: () => mutation(`${prefix}.cancel`, spec({ id: v.string({ min: 1 }) }),
        (ctx, args: any) => target(ctx, args).cancel(ctx, args.id)),
      get: () => query(`${prefix}.get`, spec({ id: v.string({ min: 1 }) }),
        (ctx, args: any) => target(ctx, args).get(ctx, args.id)),
      ready: () => query(`${prefix}.ready`, owned, (ctx, args: any) => target(ctx, args).ready(ctx, args?.owner)),
      stats: () => query(`${prefix}.stats`, counting, (ctx, args: any) => target(ctx, args).stats(ctx, pick(args, "countUpTo"))),
    };
    const methods: Record<string, MutationMethod<any, any> | QueryMethod<any, any>> = {};
    for (const method of selected) {
      if (!Object.hasOwn(factories, method)) throw new TypeError(`Unknown queue method ${JSON.stringify(method)}`);
      methods[`${prefix}.${method}`] = factories[method]();
    }
    generated.set(prefix, { signature, access, scope: scopeSource, methods: Object.freeze(methods) });
    return methods;
  }

  return Object.freeze({
    ...component({ collections: [records, line, fencing, turns], tasks: [reclaim, sweep] }),
    ...defaultView,
    name,
    records,
    line,
    scope: view,
    http,
  }) as unknown as Queue<P, R>;
}
