// How many jobs a worker process takes on at once. Only globals are used, so worker loops still
// run in a browser, where the process reports no load.

/** How loaded the process is: 1 where it falls behind, with what loads it most. */
export interface Load {
  readonly load: number;
  readonly reason: string;
}

/** Read at every adjustment: the load since the previous read. */
export type Health = () => Load;

/** Bounds for a limit that adapts. */
export interface Adaptive {
  /** Default 1. */
  readonly min?: number;
  /** Default 16. */
  readonly max?: number;
  /** Where the limit starts, within min and max. Default min. */
  readonly initial?: number;
}

/** A fixed number of jobs at once, or a limit that adapts within bounds. */
export type Concurrency = number | Adaptive;

export interface HealthLimits {
  /** The share of the time the event loop spends busy at which the process falls behind. Default 0.9. */
  readonly busy?: number;
  /** The share of the heap limit, or of the memory the process may use, at which it is full. Default 0.85. */
  readonly memory?: number;
}

interface NodeProcess {
  memoryUsage(): { heapUsed: number; rss: number };
  availableMemory?(): number;
  getBuiltinModule?(id: string): unknown;
}
interface LoopUtilization { idle: number; active: number; utilization: number }
type Utilization = (current?: LoopUtilization, previous?: LoopUtilization) => LoopUtilization;

/**
 * The load of this process: how busy its event loop was since the last read, or how full its
 * memory is, whichever is higher. The event loop's busy share climbs only when the process has more
 * to do than one core can, where its delay also spikes on one long parse or collection. Work done
 * by child processes or other machines doesn't count.
 */
export function processHealth({ busy = 0.9, memory = 0.85 }: HealthLimits = {}): Health {
  if (!(busy > 0) || !(memory > 0)) throw new TypeError("Health limits must be positive");
  const host = (globalThis as { process?: NodeProcess }).process;
  const measure = (globalThis as { performance?: { eventLoopUtilization?: Utilization } }).performance;
  const utilization = typeof measure?.eventLoopUtilization === "function" ? measure.eventLoopUtilization.bind(measure) : null;
  const v8 = host?.getBuiltinModule?.("node:v8") as { getHeapStatistics(): { heap_size_limit: number } } | undefined;
  const heapLimit = v8?.getHeapStatistics().heap_size_limit ?? null;
  let last = utilization?.() ?? null;
  return () => {
    const loads: Load[] = [{ load: 0, reason: "idle" }];
    if (utilization !== null && last !== null) {
      const now = utilization();
      const share = utilization(now, last).utilization;
      last = now;
      loads.push({ load: share / busy, reason: `event loop ${Math.round(share * 100)}% busy` });
    }
    if (host !== undefined && typeof host.memoryUsage === "function") {
      const usage = host.memoryUsage();
      if (heapLimit !== null) loads.push({ load: usage.heapUsed / heapLimit / memory, reason: `heap ${Math.round(usage.heapUsed / heapLimit * 100)}% full` });
      // What the process may still use, within a container's limit or the machine's free memory.
      const available = host.availableMemory?.();
      if (available !== undefined && available > 0) {
        const used = usage.rss / (usage.rss + available);
        loads.push({ load: used / memory, reason: `memory ${Math.round(used * 100)}% used` });
      }
    }
    return loads.reduce((most, next) => next.load > most.load ? next : most);
  };
}

/** Below this load, a limit the queue's work presses on doubles; up to 1, it grows by a sixteenth. */
const HEADROOM = 0.5;
/**
 * How long after a cut the limit grows by a sixteenth at most, so it settles under what the process
 * can take, or for longer under what its provider does: a rate limit refills by the minute.
 */
const SETTLE_MS = 5_000;
const SETTLE_THROTTLED_MS = 30_000;
/** Cuts at least this far apart, so a burst of failures or a busy second cuts once rather than to the floor. */
const CUT_EVERY_MS = 1_000;

export type LimitChange = { readonly limit: number; readonly reason: string };

/**
 * How many jobs a process runs at once, like a TCP congestion window. While the queue holds more
 * work than the process takes, the limit doubles at each adjustment if the process is under half
 * its load, and grows by a sixteenth (at least one) if it is busier or was recently cut. Once the
 * process falls behind the limit shrinks by a quarter; when a provider pushes back it halves, and
 * claims pause. A number fixes it.
 */
export class Limiter {
  readonly min: number;
  readonly max: number;
  private current: number;
  private wanted = false;
  private pausedUntil = 0;
  private lastCut = -Infinity;
  private settleUntil = -Infinity;
  private readonly health: Health;
  private readonly now: () => number;

  constructor(concurrency: Concurrency, health: Health, now: () => number = Date.now) {
    const bounds = typeof concurrency === "number" ? { min: concurrency, max: concurrency, initial: concurrency } : concurrency;
    if (bounds === null || typeof bounds !== "object") throw new TypeError("concurrency must be a number or { min?, max?, initial? }");
    this.min = bounds.min ?? 1;
    this.max = bounds.max ?? Math.max(16, this.min);
    const initial = bounds.initial ?? this.min;
    if (![this.min, this.max, initial].every(Number.isSafeInteger) || this.min < 1 || this.max < this.min || initial < this.min || initial > this.max) {
      throw new TypeError("concurrency needs whole numbers with 1 <= min <= initial <= max");
    }
    this.current = initial;
    this.health = health;
    this.now = now;
  }

  get fixed(): boolean { return this.min === this.max; }

  /** Jobs the process may hold now. */
  get limit(): number { return Math.floor(this.current); }

  /** Milliseconds until claims may resume after a provider pushed back; 0 when they may now. */
  get pause(): number { return Math.max(0, this.pausedUntil - this.now()); }

  /** The queue had more work than the process had room for. */
  want(): void { this.wanted = true; }

  /** A provider refused work for being asked too much: claim nothing for `ms`, and hold fewer jobs. */
  throttle(ms: number, reason: string): LimitChange | null {
    this.pausedUntil = Math.max(this.pausedUntil, this.now() + Math.max(0, ms));
    return this.cut(0.5, reason, SETTLE_THROTTLED_MS);
  }

  /** Once per adjustment interval: follow the process's load and the demand seen since the last call. */
  adjust(): LimitChange | null {
    const wanted = this.wanted;
    this.wanted = false;
    if (this.fixed) return null;
    const { load, reason } = this.health();
    if (load >= 1) return this.cut(0.75, reason, SETTLE_MS);
    if (!wanted || this.pause > 0 || this.current >= this.max) return null;
    const before = this.limit;
    const gentle = load >= HEADROOM || this.now() < this.settleUntil;
    this.current = Math.min(this.max, gentle ? this.current + Math.max(1, this.current / 16) : this.current * 2);
    return this.limit === before ? null : { limit: this.limit, reason: "more work is waiting" };
  }

  private cut(factor: number, reason: string, settleMs: number): LimitChange | null {
    const now = this.now();
    if (this.fixed || now - this.lastCut < CUT_EVERY_MS) return null;
    this.lastCut = now;
    this.settleUntil = Math.max(this.settleUntil, now + settleMs);
    const before = this.limit;
    this.current = Math.max(this.min, this.current * factor);
    return this.limit === before ? null : { limit: this.limit, reason };
  }
}
