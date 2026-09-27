import { canonicalJson, type Json } from "./json.ts";
import { collection, component, fail, methodInfo, plainObject, requireName, task } from "./core.ts";
import { ValidationError } from "./schema.ts";
import type { ArgsOf, ArgsParameter, Collection, Component, Context, Failure, MutationContext, MutationMethod, TaskFailure } from "./core.ts";

export interface ScheduledTimer<A = Json> {
  state: "pending" | "failed";
  handler: string;
  args: A;
  dueAt: number;
  attempts: number;
  error: Failure | null;
  createdAt: number;
  updatedAt: number;
}
export interface Timer<A = Json> extends ScheduledTimer<A> { id: string }

export interface SchedulerOptions {
  /** Failed attempts before a timer stays failed. Default 3. */
  readonly maxAttempts?: number;
  /** First retry delay in milliseconds, doubling per failure. Default 1000. */
  readonly retryDelayMs?: number;
  /** Upper bound on retry delays. Default 60000. */
  readonly maxRetryDelayMs?: number;
}

type Handlers = Readonly<Record<string, MutationMethod<any, any>>>;

export interface Scheduler<R extends Handlers> extends Component {
  readonly records: Collection<ScheduledTimer, string, { readonly due: readonly ["state", "dueAt"] }>;
  /** Replacing an ID debounces earlier work and starts a new retry budget. */
  after<H extends Extract<keyof R, string>>(ctx: MutationContext, id: string, delayMs: number, handler: H, ...args: ArgsParameter<ArgsOf<R[H]>>): Timer<ArgsOf<R[H]>>;
  at<H extends Extract<keyof R, string>>(ctx: MutationContext, id: string, dueAt: number, handler: H, ...args: ArgsParameter<ArgsOf<R[H]>>): Timer<ArgsOf<R[H]>>;
  cancel(ctx: MutationContext, id: string): boolean;
  get(ctx: Context, id: string): Timer | null;
  /** Timers in deadline order, optionally one state. */
  scan(ctx: Context, options?: { readonly state?: "pending" | "failed" }): Timer[];
  /** Requeue a failed timer with the deployed handler and a fresh retry budget. */
  retry(ctx: MutationContext, id: string, delayMs?: number): Timer;
}

function integer(value: unknown, label: string, minimum = 0): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    throw new TypeError(`${label} must be a safe integer at least ${minimum}`);
  }
}

function clock(ctx: Context): number {
  const now = ctx.now();
  integer(now, "Server time");
  return now;
}

/**
 * Durable callbacks: mutation handlers run after a deadline in their own
 * transaction. Add the scheduler to define({ uses }). Deadlines mean "not
 * before"; a handler's writes and the timer's removal commit together.
 */
export function scheduler<const R extends Handlers>(name: string, handlers: R, options: SchedulerOptions = {}): Scheduler<R> {
  requireName(name, "Scheduler name");
  if (name.startsWith("$flower.")) throw new TypeError("Scheduler names beginning with $flower. are reserved");
  const settings = plainObject(options, "Scheduler options", ["maxAttempts", "retryDelayMs", "maxRetryDelayMs"]);
  const maxAttempts = settings.maxAttempts ?? 3, retryDelayMs = settings.retryDelayMs ?? 1_000, maxRetryDelayMs = settings.maxRetryDelayMs ?? 60_000;
  integer(maxAttempts, "maxAttempts", 1);
  integer(retryDelayMs, "retryDelayMs", 1);
  integer(maxRetryDelayMs, "maxRetryDelayMs", 1);
  if (maxRetryDelayMs < retryDelayMs) throw new RangeError("Maximum retry delay must be at least the first retry delay");
  const registry: Record<string, MutationMethod<any, any>> = Object.create(null);
  for (const [alias, handler] of Object.entries(plainObject(handlers, "Scheduler handlers"))) {
    requireName(alias, "Handler alias");
    if ((handler as MutationMethod)?.kind !== "mutationMethod" || typeof (handler as MutationMethod).compute !== "function") {
      throw new TypeError("Scheduler handlers must be mutation methods");
    }
    registry[alias] = handler as MutationMethod;
  }
  const records = collection<ScheduledTimer>(name).index("due", ["state", "dueAt"]);

  function handlerFor(alias: string): MutationMethod {
    if (!Object.hasOwn(registry, alias)) fail("SCHEDULER_HANDLER_MISSING", `Unknown scheduler handler ${JSON.stringify(alias)}`);
    return registry[alias];
  }
  function get(ctx: Context, id: string): Timer | null {
    requireName(id, "Timer ID");
    const timer = ctx.get(records, id);
    return timer === null ? null : { id, ...timer };
  }
  function first(ctx: Context, bound: number | null): Timer | null {
    const row = ctx.range(records.by("due").range({ prefix: ["pending"], ...(bound === null ? {} : { lte: bound }), limit: 1 })).rows[0];
    return row ? { id: row.key, ...row.value } : null;
  }
  function schedule(ctx: MutationContext, id: string, dueAt: number, handler: string, args: unknown): Timer {
    requireName(id, "Timer ID");
    integer(dueAt, "Deadline");
    const schema = methodInfo(handlerFor(handler)).args;
    if (schema) {
      try { schema.parse(args ?? null); }
      catch (error) {
        if (error instanceof ValidationError) fail("INVALID_ARGUMENT", `${handler} arguments: ${error.message}`, { path: [...error.path] });
        throw error;
      }
    }
    const time = clock(ctx);
    const previous = ctx.get(records, id);
    const timer: ScheduledTimer = {
      state: "pending", handler, args: JSON.parse(canonicalJson(args ?? null)) as Json, dueAt, attempts: 0, error: null,
      createdAt: previous?.createdAt ?? time, updatedAt: time,
    };
    ctx.set(records, id, timer);
    return { id, ...timer };
  }

  const timers = task(`scheduler:${name}`, {
    due: (ctx) => first(ctx, null)?.dueAt ?? null,
    run(ctx) {
      const due = first(ctx, clock(ctx));
      if (!due) return null;
      // Removal is staged before dispatch; a handler may replace its own ID.
      ctx.delete(records, due.id);
      const result: unknown = handlerFor(due.handler).compute(ctx, due.args);
      canonicalJson(result);
      return { timer: due.id };
    },
    onError(ctx, failure: TaskFailure) {
      const due = first(ctx, clock(ctx));
      if (!due) return null;
      const attempts = due.attempts + 1;
      let delay = retryDelayMs;
      for (let attempt = 1; attempt < attempts && delay < maxRetryDelayMs; attempt++) delay = Math.min(maxRetryDelayMs, delay * 2);
      const exhausted = attempts >= maxAttempts || failure.failedAt > Number.MAX_SAFE_INTEGER - delay;
      const { id, ...previous } = due;
      ctx.set(records, id, {
        ...previous, state: exhausted ? "failed" : "pending", attempts,
        dueAt: exhausted ? due.dueAt : failure.failedAt + delay, updatedAt: failure.failedAt,
        error: { code: failure.error.code, message: failure.error.message, ...(failure.error.details === undefined ? {} : { details: failure.error.details }) },
      });
      return { timer: id, state: exhausted ? "failed" : "pending", attempts };
    },
  });

  return Object.freeze({
    ...component({ collections: [records], tasks: [timers] }),
    records,
    get,
    scan(ctx: Context, filter: { readonly state?: "pending" | "failed" } = {}): Timer[] {
      const { state } = plainObject(filter, "Timer filter", ["state"]);
      if (state !== undefined && state !== "pending" && state !== "failed") throw new TypeError("Unknown timer state filter");
      const timers: Timer[] = [];
      for (const current of (state === undefined ? ["pending", "failed"] : [state]) as ("pending" | "failed")[]) {
        let after: string | undefined;
        do {
          const page = ctx.range(records.by("due").range({ prefix: [current], limit: 64, ...(after === undefined ? {} : { after }) }));
          for (const row of page.rows) timers.push({ id: row.key, ...row.value });
          after = page.cursor ?? undefined;
        } while (after !== undefined);
      }
      return timers.sort((a, b) => a.dueAt - b.dueAt || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    },
    after(ctx: MutationContext, id: string, delayMs: number, handler: string, args?: unknown) {
      integer(delayMs, "Delay");
      return schedule(ctx, id, clock(ctx) + delayMs, handler, args);
    },
    at(ctx: MutationContext, id: string, dueAt: number, handler: string, args?: unknown) {
      return schedule(ctx, id, dueAt, handler, args);
    },
    cancel(ctx: MutationContext, id: string): boolean {
      if (get(ctx, id) === null) return false;
      ctx.delete(records, id);
      return true;
    },
    retry(ctx: MutationContext, id: string, delayMs = 0): Timer {
      integer(delayMs, "Delay");
      const timer = get(ctx, id);
      if (timer === null || timer.state !== "failed") fail("TIMER_NOT_FAILED", "Only failed timers can be retried");
      handlerFor(timer.handler);
      const time = clock(ctx);
      const { id: _id, ...stored } = timer;
      const retried: ScheduledTimer = { ...stored, state: "pending", attempts: 0, error: null, dueAt: time + delayMs, updatedAt: time };
      ctx.set(records, id, retried);
      return { id, ...retried };
    },
  }) as unknown as Scheduler<R>;
}
