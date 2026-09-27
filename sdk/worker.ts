import { backoff, FlowerClient, FlowerError, isTransient } from "./client.ts";
import type { RetryPolicy } from "./client.ts";
import { Limiter, processHealth, type Concurrency, type Health } from "./capacity.ts";
import type { ExternalClaim, ExternalWork } from "./external.ts";
import { canonicalJson, type Json } from "./json.ts";
import type { Claim, Claimed } from "./temporal.ts";

export { processHealth } from "./capacity.ts";
export type { Adaptive, Concurrency, Health, HealthLimits, Load } from "./capacity.ts";

export type QueueWorkerEvent =
  | { readonly type: "claimed"; readonly job: Claim<Json> }
  | { readonly type: "completed"; readonly id: string }
  | { readonly type: "failed"; readonly id: string; readonly error: string }
  | { readonly type: "lost"; readonly id: string }
  | { readonly type: "released"; readonly id: string }
  | { readonly type: "unreported"; readonly id: string; readonly error: string }
  | { readonly type: "limit"; readonly limit: number; readonly reason: string }
  | { readonly type: "waiting"; readonly error: string };

/** What a job can tell the worker running it. */
export interface QueueWorkerControl {
  /** The service behind this job refused it for being asked too much: claim nothing for `ms`, and hold fewer jobs. */
  throttle(ms: number, reason: string): void;
  /**
   * From now on this job mostly waits, on a child process or a server say, and loads the process
   * little: it stops counting toward concurrency, so long jobs can't keep the worker from taking short ones.
   */
  idle(): void;
}

export interface QueueWorkerOptions<P = Json, R = Json> {
  /** The queue.http() prefix; the worker uses its claim, renew, complete, fail and ready methods. */
  readonly queue: string;
  /** Do the job. It can run again after a crash, so give external services job.id as an idempotency key. */
  readonly work: (job: Claim<P>, signal: AbortSignal, control: QueueWorkerControl) => R | Promise<R>;
  /** Stop claiming, finish held jobs, then resolve. */
  readonly signal: AbortSignal;
  /**
   * Once stopping, how long held jobs may keep running before their signal aborts and they fail. Work
   * that still runs 5 s after its abort is given up on (an `unreported` event): the worker stops anyway,
   * and the job runs again once its lease ends. Default: until they finish.
   */
  readonly drainMs?: number;
  /** When drainMs ends, hand the jobs still running back with the queue's release method instead of failing them: they run again at once, with no error recorded and their attempt counted. Default false. */
  readonly release?: boolean;
  /** Unique per process. Defaults to a random identifier. */
  readonly owner?: string;
  /** Jobs this process runs at once: a number fixes it; bounds let it follow demand and the process's load. Default { min: 1, max: 16 }. */
  readonly concurrency?: Concurrency;
  /** Jobs one claim may take (max; queue.http() claims take up to 64). Default 1, sent without max. */
  readonly batch?: number;
  /** Claims in flight at once, once claims come back full. Default 4. */
  readonly claimers?: number;
  /**
   * Report each outcome with a claim for this process's next jobs, as many as it has room for, in
   * the same commit: while work waits, a job then costs its report alone rather than a claim too.
   * Needs complete and fail methods that take `next` and answer with it, as queue.http() generates. Default false.
   */
  readonly chain?: boolean;
  /** Wait in the queue's line, so a new job wakes one process instead of all: needs a claim that takes max and waitMs, and a ready that takes owner, as queue.http() generates. Default false. */
  readonly wait?: boolean;
  /** How long a place in line lasts; an idle worker claims again at half of it to keep it. Default 60,000. */
  readonly waitMs?: number;
  /** Lease length requested per claim and renewal. Default 30000. */
  readonly leaseMs?: number;
  /** Extend leases while work runs, all of them in one call, so leases can stay short. Needs the renew method. Default true. */
  readonly renew?: boolean;
  /** Stop working this long before a lease ends. Default a fifth of leaseMs. */
  readonly marginMs?: number;
  /** For queue.http(prefix, { scope: "argument" }). */
  readonly scope?: string;
  readonly retry?: RetryPolicy;
  /** The process's load, read at each adjustment. Default processHealth(). */
  readonly health?: Health;
  /** How often the limit follows demand and load. Default 250. */
  readonly adjustEveryMs?: number;
  readonly onEvent?: (event: QueueWorkerEvent) => void;
}

function message(error: unknown): string {
  if (error instanceof FlowerError) return error.failure ? `${error.failure.code}: ${error.failure.message}` : `${error.code}: ${error.message}`;
  return String((error as { cause?: { code?: string } })?.cause?.code ?? (error as Error)?.message ?? error);
}

/** The result as Flower will store it. One it cannot store fails the attempt with the reason, rather than going unreported until the lease runs out. */
function storable(result: unknown): Json {
  try {
    canonicalJson(result);
  } catch (error) {
    throw new TypeError(`The result cannot be stored: ${(error as Error).message}`);
  }
  return result as Json;
}

function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const done = () => { clearTimeout(timer); signal.removeEventListener("abort", done); resolve(); };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });
}

const leaseLost = (error: unknown) => error instanceof FlowerError && error.failure?.code === "LEASE_LOST";

/** How long work may take to stop once a drain aborted it, before a stopping worker gives up on it. */
const ABANDON_AFTER_MS = 5_000;

/**
 * Run jobs from a queue until stopped. One readiness watch serves the whole process: after it fires,
 * one claim at a time asks until claims come back full, several jobs per claim with `batch`. How
 * many jobs run at once follows demand and the process's load (see `concurrency`). Leases renew
 * together in one call; outcomes are reported with retries that keep one request ID.
 */
export async function runQueueWorker<P = Json, R = Json>(client: FlowerClient<any>, options: QueueWorkerOptions<P, R>): Promise<void> {
  const untyped = client as FlowerClient;
  const { queue, work, leaseMs = 30_000, renew = true, onEvent = () => {} } = options;
  // A permanent claim error stops every claimer; held jobs finish first.
  const halt = new AbortController();
  const signal = AbortSignal.any([options.signal, halt.signal]);
  const owner = options.owner ?? `worker-${crypto.randomUUID()}`;
  const marginMs = options.marginMs ?? Math.floor(leaseMs / 5);
  const batch = options.batch ?? 1;
  const wait = options.wait ?? false;
  const waitMs = options.waitMs ?? 60_000;
  const chain = options.chain ?? false;
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= marginMs || marginMs < 0) throw new TypeError("leaseMs must exceed marginMs");
  if (!Number.isSafeInteger(batch) || batch < 1) throw new TypeError("batch must be a positive safe integer");
  if (!Number.isSafeInteger(waitMs) || waitMs < 2) throw new TypeError("waitMs must be a safe integer of at least 2");
  if (options.drainMs !== undefined && (!Number.isSafeInteger(options.drainMs) || options.drainMs < 0)) throw new TypeError("drainMs must be a nonnegative safe integer");
  if (options.release && options.drainMs === undefined) throw new TypeError("release needs drainMs");
  const limiter = new Limiter(options.concurrency ?? { min: 1, max: 16 }, options.health ?? processHealth());
  const claimers = Math.max(1, Math.min(options.claimers ?? 4, limiter.max));
  if (!Number.isSafeInteger(claimers)) throw new TypeError("claimers must be a positive safe integer");
  const scope: Record<string, Json> = options.scope === undefined ? {} : { scope: options.scope };
  const readyArgs: Json = wait ? { ...scope, owner } : options.scope === undefined ? null : scope;
  const send = async <T>(method: string, args: Json, until: number): Promise<T> =>
    (await untyped.mutate(`${queue}.${method}`, args, { retry: { ...options.retry, until } })).value as T;

  type Held = { readonly job: Claim<P>; readonly identity: Record<string, Json>; readonly stop: AbortController; deadline: number; renewedAt: number; timer?: ReturnType<typeof setTimeout>; drained?: boolean };
  const held = new Set<Held>();
  const running = new Set<Promise<void>>();
  const roomWaiters: Array<() => void> = [];
  let claiming = 0;
  // Held jobs that said they mostly wait: they don't count toward the limit.
  let idle = 0;
  const busy = () => running.size - idle + claiming;
  // Waiting in line starts with a claim, which takes a place when it comes back short.
  let ready: Promise<void> | null = wait ? Promise.resolve() : null;
  // The queue showed work since the last short claim.
  let shown = false;
  // A readiness watch is open.
  let watching = false;
  let failures = 0;
  // After the queue shows work, one claimer asks at a time until claims come back full: when a
  // single job arrived, the others would only come back empty. `full` counts full claims in a row.
  let probing: Promise<void> | null = null;
  let full = 0;

  const wake = (count = roomWaiters.length) => { for (let index = 0; index < count; index++) roomWaiters.shift()?.(); };
  const changed = (change: { limit: number; reason: string } | null) => {
    if (change === null) return;
    onEvent({ type: "limit", ...change });
    wake();
  };
  const throttle = (ms: number, reason: string) => changed(limiter.throttle(ms, reason));
  const room = async () => {
    for (;;) {
      if (signal.aborted) return;
      if (limiter.pause > 0) await pause(limiter.pause, signal);
      else if (busy() >= limiter.limit) await new Promise<void>((done) => roomWaiters.push(done));
      else return;
    }
  };
  signal.addEventListener("abort", () => wake(), { once: true });

  /**
   * Resolves once the queue shows work (for this owner, when waiting in line) since the last short
   * claim, or, in line, when it is time to claim again to keep the place. One watch serves every claimer.
   */
  const whenReady = () => ready ??= (async () => {
    watching = true;
    try { await watch(); } finally { watching = false; }
  })();
  const watch = async () => {
    for (;;) {
      const refresh = wait ? AbortSignal.timeout(Math.floor(waitMs / 2)) : null;
      try {
        await untyped.waitUntil(`${queue}.ready`, readyArgs, Boolean, { signal: refresh ? AbortSignal.any([signal, refresh]) : signal });
        shown = true;
        return;
      } catch (error) {
        if (signal.aborted) return;
        if (refresh?.aborted) {
          // Claim again to keep the place, and watch anew after.
          ready = null;
          return;
        }
        if (!isTransient(error)) throw error;
        onEvent({ type: "waiting", error: message(error) });
        await pause(backoff(failures++), signal);
      }
    }
  };

  const adjusting = setInterval(() => {
    // A full pool claims nothing, so claims cannot show that work waits: its readiness watch does.
    // Without it, a pool cut below the jobs it holds (long ones, say) would never grow again.
    if (busy() >= limiter.limit) {
      whenReady().catch(() => {});
      if (shown) limiter.want();
    }
    changed(limiter.adjust());
  }, options.adjustEveryMs ?? 250);

  // One call renews every lease held since at least a renewal interval, every half interval.
  const renewEveryMs = Math.max(1, Math.floor((leaseMs - marginMs) / 3));
  const arm = (entry: Held) => {
    clearTimeout(entry.timer);
    entry.timer = setTimeout(() => entry.stop.abort(new Error("The lease ran out")), Math.max(0, entry.deadline - Date.now()));
  };
  const renewal = renew ? setInterval(async () => {
    const renewedAt = Date.now();
    const due = [...held].filter((entry) => renewedAt - entry.renewedAt >= renewEveryMs && !entry.stop.signal.aborted);
    if (due.length === 0) return;
    try {
      const expiries = await send<(number | null)[]>("renew", { leases: due.map((entry) => entry.identity), leaseMs, ...scope } as unknown as Json,
        Math.min(...due.map((entry) => entry.deadline)));
      due.forEach((entry, index) => {
        if (!held.has(entry)) return;
        if (expiries[index] === null) return entry.stop.abort(new Error("The lease was lost"));
        entry.renewedAt = renewedAt;
        entry.deadline = renewedAt + leaseMs - marginMs;
        arm(entry);
      });
    } catch {
      // Each job's deadline stops its work if renewals stop landing.
    }
  }, Math.max(1, Math.floor(renewEveryMs / 2))) : undefined;

  /** A claim that came back short means the queue ran dry: watch for work again, keeping a watch still open. */
  function claimed(got: number, asked: number): void {
    if (got < asked) {
      full = 0;
      if (!watching) ready = null;
      shown = false;
    } else full++;
  }

  function start(job: Claim<P>, sentAt: number): void {
    const identity = { id: job.id, owner: job.owner, token: job.token, ...(job.history ? { history: job.history } : {}) } as unknown as Record<string, Json>;
    const entry: Held = { job, identity, stop: new AbortController(), deadline: sentAt + leaseMs - marginMs, renewedAt: sentAt };
    if (entry.deadline <= Date.now()) return onEvent({ type: "lost", id: job.id });
    arm(entry);
    held.add(entry);
    onEvent({ type: "claimed", job: job as Claim<Json> });
    let idled = false;
    const control: QueueWorkerControl = {
      throttle,
      idle: () => {
        if (idled || !held.has(entry)) return;
        idled = true;
        idle++;
        wake(1);
      },
    };
    const task = (async () => {
      let outcome: [string, Record<string, Json>];
      try {
        const result = await work(job, entry.stop.signal, control);
        if (entry.stop.signal.aborted) throw entry.stop.signal.reason;
        outcome = ["complete", { ...identity, ...scope, result: storable(result) }];
      } catch (error) {
        // Abortable APIs reject with a generic AbortError; the lease's reason says why.
        outcome = entry.drained && options.release ? ["release", { ...identity, ...scope }]
          : ["fail", { ...identity, ...scope, error: { message: message(entry.stop.signal.aborted ? entry.stop.signal.reason : error) } }];
      } finally {
        held.delete(entry);
        clearTimeout(entry.timer);
        if (idled) idle--;
      }
      // This job's room passes to the jobs its report claims; the claimers keep out of the rest.
      const take = chain && outcome[0] !== "release" && !signal.aborted && limiter.pause === 0 ? Math.min(batch, limiter.limit - busy() + 1) : 0;
      if (take > 0) {
        outcome[1].next = { max: take, leaseMs, ...(wait ? { waitMs } : {}) };
        claiming += take - 1;
      }
      const sentAt = Date.now();
      try {
        const reply = await send<{ next?: Claimed<P> } | null>(outcome[0], outcome[1], entry.deadline + marginMs);
        onEvent(outcome[0] === "complete" ? { type: "completed", id: job.id } : outcome[0] === "release" ? { type: "released", id: job.id }
          : { type: "failed", id: job.id, error: String((outcome[1] as { error: { message: string } }).error.message) });
        if (take > 0) {
          const jobs: Claim<P>[] = [];
          if (reply?.next) {
            const { more = [], ...first } = reply.next;
            jobs.push(first as Claim<P>, ...more);
          }
          claimed(jobs.length, take);
          for (const next of jobs) start(next, sentAt);
          if (jobs.length === take && busy() >= limiter.limit) limiter.want();
        }
      } catch (error) {
        // Refused (the lease moved on) or never answered: either way the queue hands the job out again once the lease ends.
        if (leaseLost(error)) onEvent({ type: "lost", id: job.id });
        else onEvent({ type: "unreported", id: job.id, error: message(error) });
      } finally {
        if (take > 0) {
          claiming -= take - 1;
          wake(take - 1);
        }
      }
    })().finally(() => {
      running.delete(task);
      wake(1);
    });
    running.add(task);
  }

  async function claimer(): Promise<void> {
    while (!signal.aborted) {
      await room();
      await whenReady();
      if (signal.aborted) return;
      const confirmed = full >= 2 || (full >= 1 && batch > 1);
      if (!confirmed && probing !== null) {
        await probing;
        continue;
      }
      // Room was checked before waiting: check again now the queue has work.
      if (limiter.pause > 0 || busy() >= limiter.limit) continue;
      let probed = () => {};
      if (!confirmed) probing = new Promise<void>((done) => { probed = done; });
      // Room for this many, held until the claim answers.
      const take = Math.max(1, Math.min(batch, limiter.limit - busy()));
      claiming += take;
      const sentAt = Date.now();
      let jobs: Claim<P>[] = [];
      let retryIn: number | null = null;
      try {
        const args = { owner, leaseMs, ...scope, ...(batch > 1 || wait ? { max: take } : {}), ...(wait ? { waitMs } : {}) };
        const value = await send<(Claim<P> & { more?: Claim<P>[] }) | null>("claim", args, sentAt + leaseMs);
        if (value !== null) {
          const { more = [], ...first } = value;
          jobs = [first as Claim<P>, ...more];
        }
        failures = 0;
      } catch (error) {
        if (signal.aborted) return;
        if (!isTransient(error)) throw error;
        onEvent({ type: "waiting", error: message(error) });
        retryIn = backoff(failures++);
      } finally {
        claiming -= take;
        wake(take);
        if (!confirmed) {
          probing = null;
          probed();
        }
      }
      // Wait out a failed claim without holding its room.
      if (retryIn !== null) {
        await pause(retryIn, signal);
        continue;
      }
      claimed(jobs.length, take);
      for (const job of jobs) start(job, sentAt);
      // The claim took all it asked for and filled the room: the queue may hold more than the pool takes.
      if (jobs.length === take && busy() >= limiter.limit) limiter.want();
    }
  }

  let failed: { error: unknown } | undefined;
  try {
    await Promise.all(Array.from({ length: claimers }, () => claimer().catch((error) => {
      failed ??= { error };
      halt.abort();
    })));
  } finally {
    clearInterval(adjusting);
    // Work that ignores its signal, such as a command whose child still holds its output, must not keep the worker from stopping.
    let giveUp: ReturnType<typeof setTimeout> | undefined;
    const abandoned = new Promise<void>((resolve) => {
      if (options.drainMs === undefined) return;
      giveUp = setTimeout(() => {
        for (const entry of held) {
          entry.drained = true;
          entry.stop.abort(new Error("The worker stopped before the job finished"));
        }
        giveUp = setTimeout(() => {
          for (const entry of held) onEvent({ type: "unreported", id: entry.job.id, error: "The job did not stop when its worker did" });
          resolve();
        }, ABANDON_AFTER_MS);
      }, options.drainMs);
    });
    await Promise.race([Promise.allSettled(running), abandoned]);
    clearTimeout(giveUp);
    clearInterval(renewal);
    // Give up the place in line at once, rather than when it runs out.
    if (wait && !failed) await send("claim", { owner, max: 0, waitMs: 0, ...scope }, Date.now() + 2_000).catch(() => {});
  }
  if (failed) throw failed.error;
}

export type ReconcileEvent =
  | { readonly type: "claimed"; readonly key: string; readonly attempt: number }
  | { readonly type: "published"; readonly key: string; readonly accepted: boolean }
  | { readonly type: "failed"; readonly key: string; readonly error: string }
  | { readonly type: "lost"; readonly key: string }
  | { readonly type: "limit"; readonly limit: number; readonly reason: string }
  | { readonly type: "waiting"; readonly error: string };

export interface ReconcileOptions<A = Json, I = Json, R = Json> {
  /** The external.http() prefix; uses pending and publish, or next for pools. */
  readonly external: string;
  /** Compute the result for an input. It may run more than once for the same input. */
  readonly compute: (input: I, work: ExternalWork<A, I>, signal: AbortSignal) => R | Promise<R>;
  readonly signal: AbortSignal;
  /** Keep one key current. Omit to drain every tracked key through next. */
  readonly args?: A;
  /** Pool mode: work only on keys in this hash shard. */
  readonly shard?: readonly [index: number, count: number];
  /** Pool mode: parallel computations. Default 1. In lease mode, bounds let the number follow the backlog and the process's load, as runQueueWorker's does. */
  readonly concurrency?: Concurrency;
  /** Pool mode: keys fetched per round. Default 16. */
  readonly batch?: number;
  /** Pool mode: lease keys, so each is computed by one process at a time and taken over when that process stops. */
  readonly lease?: boolean;
  /** Lease mode: unique per process. Defaults to a random identifier. */
  readonly owner?: string;
  /** Lease mode: lease length requested per claim and renewal. Default 30000. */
  readonly leaseMs?: number;
  /** Lease mode: abort compute this long before a lease ends. Default a fifth of leaseMs. */
  readonly marginMs?: number;
  /** Lease mode with adaptive concurrency: the process's load, read at each adjustment. Default processHealth(). */
  readonly health?: Health;
  /** Lease mode with adaptive concurrency: how often the limit follows the backlog and load. Default 250. */
  readonly adjustEveryMs?: number;
  readonly retry?: RetryPolicy;
  readonly onEvent?: (event: ReconcileEvent) => void;
}

/**
 * Keep external values current: wait for pending work, compute it, and publish
 * it guarded by its input key. Stale results are rejected, never stored.
 */
export async function reconcile<A = Json, I = Json, R = Json>(client: FlowerClient<any>, options: ReconcileOptions<A, I, R>): Promise<void> {
  const untyped = client as FlowerClient;
  const { external, compute, signal, onEvent = () => {} } = options;
  const concurrency = options.concurrency ?? 1;
  if (typeof concurrency === "number" && (!Number.isSafeInteger(concurrency) || concurrency < 1)) throw new TypeError("concurrency must be a positive safe integer");
  if (options.lease) {
    if (options.args !== undefined || options.shard !== undefined) throw new TypeError("Lease mode spreads the whole pool: omit args and shard");
    return leasedPool(untyped, options, concurrency);
  }
  if (typeof concurrency !== "number") throw new TypeError("Adaptive concurrency needs lease: true");
  if (options.owner !== undefined || options.leaseMs !== undefined || options.marginMs !== undefined || options.health !== undefined || options.adjustEveryMs !== undefined) {
    throw new TypeError("owner, leaseMs, marginMs, health and adjustEveryMs need lease: true");
  }
  const failures = new Map<string, number>();

  async function failed(key: string, error: unknown, stop: AbortSignal): Promise<void> {
    const count = (failures.get(key) ?? 0) + 1;
    failures.set(key, count);
    onEvent({ type: "failed", key, error: message(error) });
    await pause(backoff(count - 1), stop);
  }

  async function settle(work: ExternalWork<A, I>, stop: AbortSignal): Promise<void> {
    let value: R;
    try {
      value = await compute(work.input, work, stop);
    } catch (error) {
      if (!stop.aborted) await failed(work.key, error, stop);
      return;
    }
    let receipt: Json;
    try {
      ({ value: receipt } = await untyped.mutate(`${external}.publish`, { args: work.args, key: work.key, value } as unknown as Json,
        { retry: options.retry ?? true, signal: stop }));
    } catch (error) {
      // The publish method rejected this result (e.g. its schema); other keys can still progress.
      if (error instanceof FlowerError && error.code === "EVALUATION_FAILED" && !stop.aborted) return failed(work.key, error, stop);
      throw error;
    }
    failures.delete(work.key);
    onEvent({ type: "published", key: work.key, accepted: (receipt as { accepted: boolean }).accepted });
  }

  for (let attempt = 0; !signal.aborted;) {
    try {
      if (options.args !== undefined) {
        const { value } = await untyped.waitUntil(`${external}.pending`, options.args as Json, Boolean, { signal });
        await settle(value as unknown as ExternalWork<A, I>, signal);
      } else {
        const request = { limit: options.batch ?? 16, ...(options.shard ? { shard: options.shard } : {}) } as unknown as Json;
        const { value } = await untyped.waitUntil(`${external}.next`, request, (items) => Array.isArray(items) && items.length > 0, { signal });
        const queue = [...(value as unknown as ExternalWork<A, I>[])];
        const batch = new AbortController();
        const stop = AbortSignal.any([signal, batch.signal]);
        await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
          try {
            for (let work = queue.shift(); work && !stop.aborted; work = queue.shift()) await settle(work, stop);
          } catch (error) {
            batch.abort(error);
            throw error;
          }
        }));
      }
      attempt = 0;
    } catch (error) {
      if (signal.aborted) return;
      if (!isTransient(error)) throw error;
      onEvent({ type: "waiting", error: message(error) });
      await pause(backoff(attempt++), signal);
    }
  }
}

// Claim only as many keys as there are free computations, so no process hoards work;
// one renewal covers every held lease, and held keys are handed back on the way out.
async function leasedPool<A, I, R>(client: FlowerClient, options: ReconcileOptions<A, I, R>, concurrency: Concurrency): Promise<void> {
  const { external, compute, onEvent = () => {} } = options;
  const owner = options.owner ?? `reconciler-${crypto.randomUUID()}`;
  const leaseMs = options.leaseMs ?? 30_000;
  const marginMs = options.marginMs ?? Math.floor(leaseMs / 5);
  const batch = options.batch ?? 16;
  if (!Number.isSafeInteger(leaseMs) || !Number.isSafeInteger(marginMs) || leaseMs <= marginMs || marginMs < 0) throw new TypeError("leaseMs must exceed marginMs");
  if (!Number.isSafeInteger(batch) || batch < 1) throw new TypeError("batch must be a positive safe integer");
  const limiter = new Limiter(concurrency, options.health ?? (typeof concurrency === "number" ? () => ({ load: 0, reason: "idle" }) : processHealth()));
  if (limiter.max > 1024) throw new TypeError("Lease mode runs at most 1024 computations at once");
  // Shutdown or a permanent error stops claiming and aborts held computations.
  const halt = new AbortController();
  const signal = AbortSignal.any([options.signal, halt.signal]);
  let failed: { error: unknown } | undefined;
  const send = async <T>(method: string, args: unknown, until: number, abort?: AbortSignal): Promise<T> =>
    (await client.mutate(`${external}.${method}`, args as Json, { retry: { ...options.retry, until }, ...(abort ? { signal: abort } : {}) })).value as T;
  type Held = { readonly claim: ExternalClaim<A, I>; readonly lost: AbortController; deadline: number; timer?: ReturnType<typeof setTimeout> };
  const held = new Set<Held>();
  const identity = ({ claim: { args, key, owner, attempt } }: Held) => ({ args, key, owner, attempt });
  const arm = (entry: Held, deadline: number) => {
    entry.deadline = deadline;
    clearTimeout(entry.timer);
    entry.timer = setTimeout(() => entry.lost.abort(new Error("The lease ran out")), Math.max(0, deadline - Date.now()));
  };
  const renewal = setInterval(async () => {
    const entries = [...held];
    if (!entries.length) return;
    const renewedAt = Date.now();
    try {
      const expiries = await send<(number | null)[]>("renew", { leases: entries.map(identity), leaseMs }, Math.min(...entries.map((entry) => entry.deadline)), signal);
      entries.forEach((entry, index) => {
        if (!held.has(entry) || entry.lost.signal.aborted) return;
        if (expiries[index] === null) entry.lost.abort(new Error("The lease was lost"));
        else arm(entry, renewedAt + leaseMs - marginMs);
      });
    } catch {
      // Each lease's deadline aborts its computation if renewals stop landing.
    }
  }, Math.max(1, Math.floor((leaseMs - marginMs) / 3)));

  async function release(entry: Held, delayMs: number): Promise<void> {
    // While running, retry until the lease would end anyway; on the way out, try once.
    try { await send("release", { ...identity(entry), delayMs }, signal.aborted ? Date.now() : entry.deadline + marginMs); }
    catch { /* The lease runs out on its own. */ }
  }

  async function run(entry: Held): Promise<void> {
    const { claim } = entry;
    const stop = AbortSignal.any([signal, entry.lost.signal]);
    let value: R;
    try {
      value = await compute(claim.input, claim, stop);
      if (stop.aborted) throw stop.reason;
    } catch (error) {
      if (entry.lost.signal.aborted) return onEvent({ type: "lost", key: claim.key });
      if (signal.aborted) return release(entry, 0);
      onEvent({ type: "failed", key: claim.key, error: message(error) });
      // The delay applies to every process, and attempt counts across them.
      return release(entry, backoff(claim.attempt - 1));
    }
    let receipt: Json;
    try {
      ({ value: receipt } = await client.mutate(`${external}.publish`, { args: claim.args, key: claim.key, value } as unknown as Json,
        { retry: options.retry ?? true, signal }));
    } catch (error) {
      if (signal.aborted) return release(entry, 0);
      if (error instanceof FlowerError && error.code === "EVALUATION_FAILED") {
        onEvent({ type: "failed", key: claim.key, error: message(error) });
        return release(entry, backoff(claim.attempt - 1));
      }
      // Once the lease runs out, another process computes the key again.
      if (isTransient(error)) return onEvent({ type: "waiting", error: message(error) });
      throw error;
    }
    const { accepted } = receipt as { accepted: boolean };
    onEvent({ type: "published", key: claim.key, accepted });
    // A rejected result was computed for an older input: hand the key back for the current one.
    if (!accepted) await release(entry, 0);
  }

  const running = new Set<Promise<void>>();
  // A full pool claims nothing, so claims cannot show that keys wait: the readiness watch does,
  // shared with the wait after a short claim. `shown` holds from the watch firing to the next short claim.
  let ready: Promise<void> | null = null;
  let shown = false;
  const whenReady = () => ready ??= client.waitUntil(`${external}.ready`, null, Boolean, { signal }).then(
    () => { shown = true; },
    (error) => { ready = null; throw error; },
  );
  let roomChanged = () => {};
  const adjusting = limiter.fixed ? undefined : setInterval(() => {
    if (running.size >= limiter.limit) {
      if (shown) limiter.want();
      else whenReady().catch(() => {});
    }
    const change = limiter.adjust();
    if (change === null) return;
    onEvent({ type: "limit", ...change });
    roomChanged();
  }, options.adjustEveryMs ?? 250);
  const start = (claim: ExternalClaim<A, I>, sentAt: number) => {
    const entry: Held = { claim, lost: new AbortController(), deadline: 0 };
    arm(entry, sentAt + leaseMs - marginMs);
    held.add(entry);
    onEvent({ type: "claimed", key: claim.key, attempt: claim.attempt });
    const task: Promise<void> = run(entry)
      .catch((error) => { failed ??= { error }; halt.abort(); })
      .finally(() => { held.delete(entry); clearTimeout(entry.timer); running.delete(task); });
    running.add(task);
  };
  try {
    for (let attempt = 0; !signal.aborted;) {
      if (running.size >= limiter.limit) {
        // Wait for a computation to end, or for the limit to grow.
        await Promise.race([...running, new Promise<void>((done) => { roomChanged = done; })]);
        continue;
      }
      try {
        const sentAt = Date.now();
        const limit = Math.min(batch, limiter.limit - running.size);
        const claims = await send<ExternalClaim<A, I>[]>("claim", { owner, leaseMs, limit }, sentAt + leaseMs);
        attempt = 0;
        for (const claim of claims) start(claim, sentAt);
        if (claims.length < limit) {
          // A short batch means the backlog is drained; claim again once more work is ready.
          ready = null;
          shown = false;
          await whenReady();
        } else if (running.size >= limiter.limit) limiter.want();
      } catch (error) {
        if (signal.aborted) break;
        if (!isTransient(error)) throw error;
        onEvent({ type: "waiting", error: message(error) });
        await pause(backoff(attempt++), signal);
      }
    }
  } catch (error) {
    failed ??= { error };
    halt.abort();
  } finally {
    clearInterval(adjusting);
    await Promise.all(running);
    clearInterval(renewal);
  }
  if (failed) throw failed.error;
}
