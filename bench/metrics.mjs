/**
 * Bounded-memory measurements for the closed-loop Flower stress benchmark.
 * Durations and histogram values are milliseconds; rates are events per second.
 * Percentiles use nearest ranks and logarithmic bucket upper bounds, not samples.
 */

const MIN_POSITIVE_MS = 0.001;
const MAX_BOUNDED_MS = 24 * 60 * 60 * 1_000;
const BUCKET_RATIO = 1.01;
const OTHER = "(other)";

const bounds = [0, MIN_POSITIVE_MS];
while (bounds.at(-1) < MAX_BOUNDED_MS) {
  bounds.push(Math.min(MAX_BOUNDED_MS, bounds.at(-1) * BUCKET_RATIO));
}
bounds.push(Infinity);
const BUCKET_BOUNDS = Float64Array.from(bounds);
/** Upper bound of each histogram bucket, in milliseconds; the last is Infinity. */
export const HISTOGRAM_BOUNDS = BUCKET_BOUNDS;

export const HISTOGRAM_PROPERTIES = Object.freeze({
  unit: "milliseconds",
  percentiles: "nearest-rank logarithmic bucket upper bounds",
  relativeBucketWidth: BUCKET_RATIO - 1,
  minPositiveMs: MIN_POSITIVE_MS,
  maxBoundedMs: MAX_BOUNDED_MS,
  bucketCount: BUCKET_BOUNDS.length,
});

export const APPLICATION_METHOD_KINDS = Object.freeze({
  "pizza.shop": "read",
  "pizza.shop.local": "read",
  "pizza.order": "mutation",
  "pizza.tip": "mutation",
});

/**
 * Goodput is successful primary customer calls per measured load second.
 * It excludes transport retries, explicit receipt replays, all worker activity,
 * setup/maintenance/audit traffic, and every failed call. A transport retry that
 * recovers the original successful receipt completes that customer call once.
 * Percentiles cannot be merged from summaries, so none are synthesized here.
 */
export function summarizeApplication(report) {
  const load = report?.phases?.load;
  const logical = load?.operations;
  const perMethod = logical?.perMethod;
  const validCount = (value) => Number.isSafeInteger(value) && value >= 0;
  const numericCount = (value) => validCount(value) ? value : 0;
  const validDuration = Number.isFinite(load?.durationMs) && load.durationMs > 0;
  const available = validDuration && perMethod !== null && typeof perMethod === "object" && !Array.isArray(perMethod);
  const values = { read: { count: 0, completed: 0, failed: 0 }, mutation: { count: 0, completed: 0, failed: 0 } };
  let excludedReplayCompletions = 0;
  let excludedWorkerCompletions = 0;
  for (const [name, value] of Object.entries(available ? perMethod : {})) {
    const kind = Object.hasOwn(APPLICATION_METHOD_KINDS, name) ? APPLICATION_METHOD_KINDS[name] : null;
    if (kind) {
      for (const field of ["count", "completed", "failed"]) values[kind][field] += numericCount(value?.[field]);
    } else if (name.endsWith(".replay")) excludedReplayCompletions += numericCount(value?.completed);
    else if (name === "pizza.claim" || name === "pizza.deliver") excludedWorkerCompletions += numericCount(value?.completed);
  }
  const completed = values.read.completed + values.mutation.completed;
  const goodputRps = available ? completed / (load.durationMs / 1_000) : null;
  const correctnessPassed = (report?.correctnessPassed ?? report?.passed) === true;
  return {
    definition: "Successful primary customer logical calls per measured load second; excludes worker traffic, explicit replays, and errors.",
    methods: APPLICATION_METHOD_KINDS,
    available,
    durationMs: validDuration ? load.durationMs : null,
    count: available ? values.read.count + values.mutation.count : null,
    completed: available ? completed : null,
    failed: available ? values.read.failed + values.mutation.failed : null,
    reads: { ...values.read, fraction: completed ? values.read.completed / completed : null },
    mutations: { ...values.mutation, fraction: completed ? values.mutation.completed / completed : null },
    goodputRps,
    correctnessPassed,
    status: !available ? "unmeasured" : correctnessPassed ? "valid" : "invalid",
    excludedLogicalCompletions: available && validCount(logical.completed)
      ? Math.max(0, logical.completed - completed) : null,
    excludedReplayCompletions,
    excludedWorkerCompletions,
  };
}

export class Histogram {
  #buckets = new Float64Array(BUCKET_BOUNDS.length);
  #samples = 0;
  #invalidSamples = 0;
  #min = Infinity;
  #max = -Infinity;

  /** Invalid timings are counted separately and excluded from percentiles. */
  record(milliseconds) {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) {
      this.#invalidSamples++;
      return false;
    }
    let low = 0;
    let high = BUCKET_BOUNDS.length - 1;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (BUCKET_BOUNDS[middle] >= milliseconds) high = middle;
      else low = middle + 1;
    }
    this.#buckets[low]++;
    this.#samples++;
    this.#min = Math.min(this.#min, milliseconds);
    this.#max = Math.max(this.#max, milliseconds);
    return true;
  }

  /**
   * p is a percentage in [0, 100]. Empty histograms return null.
   * Between 0.001 ms and 24 h, the upper bound is at most 1% above the
   * corresponding exact rank. Values below that range use a 0.001 ms bucket;
   * values above it share an overflow bucket bounded by the observed maximum.
   */
  percentile(p) {
    if (!Number.isFinite(p) || p < 0 || p > 100) {
      throw new RangeError("percentile must be a finite number from 0 to 100");
    }
    if (!this.#samples) return null;
    if (p === 0) return this.#min;
    if (p === 100) return this.#max;
    const rank = Math.max(1, Math.ceil((p / 100) * this.#samples));
    let cumulative = 0;
    for (let i = 0; i < this.#buckets.length; i++) {
      cumulative += this.#buckets[i];
      if (cumulative >= rank) return Math.min(BUCKET_BOUNDS[i], this.#max);
    }
    return this.#max;
  }

  merge(snapshot) {
    const validCount = (n) => Number.isSafeInteger(n) && n >= 0;
    const inBucket = (value, index) => index === 0 ? value === 0
      : value > BUCKET_BOUNDS[index - 1] && value <= BUCKET_BOUNDS[index];
    if (!snapshot || !Array.isArray(snapshot.buckets) || snapshot.buckets.length !== BUCKET_BOUNDS.length
      || !snapshot.buckets.every(validCount) || !validCount(snapshot.samples) || !validCount(snapshot.invalidSamples)
      || snapshot.buckets.reduce((a, b) => a + b, 0) !== snapshot.samples
      || snapshot.overflowSamples !== snapshot.buckets.at(-1)
      || !Number.isSafeInteger(this.#samples + snapshot.samples)
      || !Number.isSafeInteger(this.#invalidSamples + snapshot.invalidSamples)
      || (snapshot.samples === 0 ? snapshot.min !== null || snapshot.max !== null
        : !Number.isFinite(snapshot.min) || !Number.isFinite(snapshot.max)
          || snapshot.min < 0 || snapshot.max < snapshot.min
          || !inBucket(snapshot.min, snapshot.buckets.findIndex((count) => count > 0))
          || !inBucket(snapshot.max, snapshot.buckets.findLastIndex((count) => count > 0)))) {
      throw new TypeError("Cannot merge missing or incompatible histogram buckets");
    }
    // Validate the complete snapshot before mutating any destination counter.
    snapshot.buckets.forEach((count, i) => { this.#buckets[i] += count; });
    this.#samples += snapshot.samples;
    this.#invalidSamples += snapshot.invalidSamples;
    if (snapshot.samples) { this.#min = Math.min(this.#min, snapshot.min); this.#max = Math.max(this.#max, snapshot.max); }
    return this;
  }

  snapshot() {
    return {
      buckets: Array.from(this.#buckets),
      samples: this.#samples,
      invalidSamples: this.#invalidSamples,
      overflowSamples: this.#buckets.at(-1),
      approximate: true,
      min: this.#samples ? this.#min : null,
      p50: this.percentile(50),
      p95: this.percentile(95),
      p99: this.percentile(99),
      max: this.#samples ? this.#max : null,
    };
  }
}

/** A repeatable [0, 1) PRNG; this is for workload generation, not secrets. */
export function createRandom(seed) {
  if (typeof seed !== "string" && !(typeof seed === "number" && Number.isFinite(seed))) {
    throw new TypeError("seed must be a string or finite number");
  }
  let state = 0x811c9dc5;
  for (const character of String(seed)) {
    state = Math.imul(state ^ character.codePointAt(0), 0x01000193) >>> 0;
  }
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 0x1_0000_0000;
  };
}

function errorCategory(status) {
  if (Number.isInteger(status) && status >= 100 && status <= 599) return `HTTP_${status}`;
  if (status === undefined || status === null || status === "network") return "network";
  if (status === "timeout" || status === "aborted") return status;
  return "other";
}

class Counters {
  attempts = 0;
  successes = 0;
  failures = 0;
  retries = 0;
  duplicates = 0;
  latency = new Histogram();
  errors = new Map();

  merge(value) {
    const fields = ["attempts", "successes", "failures", "retries", "duplicates"];
    for (const field of fields) {
      if (!Number.isSafeInteger(value?.[field]) || value[field] < 0 || !Number.isSafeInteger(this[field] + value[field])) throw new TypeError("Invalid merged counter");
    }
    if (value.successes + value.failures !== value.attempts || value.retries > value.attempts || value.duplicates > value.successes ||
        value.latencyMs?.samples + value.latencyMs?.invalidSamples !== value.attempts ||
        !value.errors || typeof value.errors !== "object" || Array.isArray(value.errors)) throw new TypeError("Inconsistent merged counters");
    const errors = new Map(this.errors);
    let failures = 0;
    for (const [name, count] of Object.entries(value.errors)) {
      if (!/^(?:HTTP_[1-5][0-9]{2}|network|timeout|aborted|other)$/.test(name) || !Number.isSafeInteger(count) || count < 0 ||
          !Number.isSafeInteger((errors.get(name) ?? 0) + count)) throw new TypeError("Invalid merged error counters");
      failures += count;
      errors.set(name, (errors.get(name) ?? 0) + count);
    }
    if (failures !== value.failures) throw new TypeError("Merged error counts disagree with failures");
    const latency = new Histogram().merge(this.latency.snapshot()).merge(value.latencyMs);
    for (const field of fields) this[field] += value[field];
    this.errors = errors;
    this.latency = latency;
    return this;
  }

  record({ latencyMs, ok, status, retry, duplicate }) {
    this.attempts++;
    if (ok) this.successes++;
    else {
      this.failures++;
      const category = errorCategory(status);
      this.errors.set(category, (this.errors.get(category) ?? 0) + 1);
    }
    if (retry) this.retries++;
    if (duplicate) this.duplicates++;
    this.latency.record(latencyMs);
  }

  snapshot(durationMs) {
    const seconds = durationMs / 1_000;
    return {
      attempts: this.attempts,
      successes: this.successes,
      failures: this.failures,
      retries: this.retries,
      duplicates: this.duplicates,
      attemptsPerSecond: seconds ? this.attempts / seconds : 0,
      throughputPerSecond: seconds ? this.successes / seconds : 0,
      latencyMs: this.latency.snapshot(),
      errors: Object.fromEntries([...this.errors.entries()].sort(([a], [b]) => a.localeCompare(b))),
    };
  }
}

class Series {
  all = new Counters();
  #byName = new Map();
  #allowed;
  #limit;

  constructor(names, limit) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1_024) {
      throw new RangeError("series limit must be an integer from 1 to 1024");
    }
    if (names !== undefined && (!Array.isArray(names) || names.some((name) => !validName(name)))) {
      throw new TypeError("series names must be strings of 1 to 128 characters");
    }
    this.#allowed = names === undefined ? null : new Set(names);
    if (this.#allowed && this.#allowed.size > limit) {
      throw new RangeError("series allowlist exceeds its limit");
    }
    this.#limit = limit;
  }

  record(name, measurement) {
    if (typeof measurement?.ok !== "boolean") throw new TypeError("measurement.ok must be boolean");
    const canAdd = this.#byName.size - Number(this.#byName.has(OTHER)) < this.#limit;
    if (!validName(name)
      || (this.#allowed && !this.#allowed.has(name))
      || (!this.#byName.has(name) && !canAdd)) name = OTHER;
    let counters = this.#byName.get(name);
    if (!counters) {
      counters = new Counters();
      this.#byName.set(name, counters);
    }
    this.all.record(measurement);
    counters.record(measurement);
  }

  clone() {
    const cloned = new Series(this.#allowed ? [...this.#allowed] : undefined, this.#limit);
    cloned.merge(this.snapshot(0));
    return cloned;
  }

  merge(snapshot, logical = false) {
    if (!snapshot?.perMethod || typeof snapshot.perMethod !== "object" || Array.isArray(snapshot.perMethod)) throw new TypeError("Missing merged method counters");
    const convert = (value) => logical ? { attempts: value.count, successes: value.completed, failures: value.failed,
      retries: 0, duplicates: value.duplicates, latencyMs: value.latencyMs, errors: value.failed ? { network: value.failed } : {} } : value;
    const incoming = new Counters().merge(convert(snapshot));
    const sum = new Counters();
    const pending = [];
    for (let [name, value] of Object.entries(snapshot.perMethod)) {
      const counters = new Counters().merge(convert(value));
      sum.merge(counters.snapshot(0));
      const known = this.#byName.has(name) || pending.some(([key]) => key === name);
      const keys = new Set([...this.#byName.keys(), ...pending.map(([key]) => key)]);
      const size = keys.size - Number(keys.has(OTHER));
      if (!validName(name) || (this.#allowed && !this.#allowed.has(name)) || (!known && size >= this.#limit)) name = OTHER;
      pending.push([name, counters]);
    }
    for (const field of ["attempts", "successes", "failures", "retries", "duplicates"]) {
      if (sum[field] !== incoming[field]) throw new TypeError("Merged totals differ from methods");
    }
    if (JSON.stringify(sum.snapshot(0).errors) !== JSON.stringify(incoming.snapshot(0).errors)) throw new TypeError("Merged errors differ from methods");
    if (JSON.stringify(sum.latency.snapshot()) !== JSON.stringify(incoming.latency.snapshot())) throw new TypeError("Merged histogram differs from methods");
    this.all.merge(incoming.snapshot(0));
    for (const [name, counters] of pending) {
      const existing = this.#byName.get(name) ?? new Counters();
      existing.merge(counters.snapshot(0));
      this.#byName.set(name, existing);
    }
    return this;
  }

  snapshot(durationMs) {
    return {
      ...this.all.snapshot(durationMs),
      perMethod: Object.fromEntries([...this.#byName.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([name, counters]) => [name, counters.snapshot(durationMs)])),
    };
  }
}

function validName(name) {
  return typeof name === "string" && name.length > 0 && name.length <= 128;
}

function logicalSnapshot(series) {
  const logical = ({ attempts, successes, failures, duplicates, throughputPerSecond, latencyMs }) => ({
    count: attempts,
    completed: successes,
    failed: failures,
    duplicates,
    throughputPerSecond,
    latencyMs,
  });
  return {
    ...logical(series),
    perMethod: Object.fromEntries(Object.entries(series.perMethod).map(([name, value]) => [name, logical(value)])),
  };
}

/**
 * HTTP attempts and logical operations are recorded independently. Retrying a
 * request contributes another attempt, but only one final logical operation.
 * Unknown names collapse into one series; arbitrary error messages are never
 * retained. Memory is bounded by the method/operation limits and fixed buckets.
 */
export class Stats {
  #attempts;
  #operations;
  #lateness = new Histogram();

  constructor({ methods, operations, maxMethods = 64, maxOperations = 64 } = {}) {
    this.#attempts = new Series(methods, maxMethods);
    this.#operations = new Series(operations, maxOperations);
  }

  record(method, measurement) {
    this.#attempts.record(method, measurement);
  }

  recordOperation(method, measurement) {
    this.#operations.record(method, measurement);
  }

  /** Lateness is max(0, callback execution timestamp - scheduled timestamp). */
  recordLateness(milliseconds) {
    this.#lateness.record(milliseconds);
  }

  /** Merge disjoint native-driver intervals, preserving every bucket/count.
   * Validate into detached copies so a malformed interval changes no counters. */
  merge(snapshot) {
    if (!snapshot?.histogram || Object.keys(snapshot.histogram).length !== Object.keys(HISTOGRAM_PROPERTIES).length ||
        Object.entries(HISTOGRAM_PROPERTIES).some(([key, value]) => snapshot.histogram[key] !== value)) throw new TypeError("Incompatible merged histogram definition");
    const attempts = this.#attempts.clone().merge(snapshot);
    const operations = this.#operations.clone().merge(snapshot.operations, true);
    const lateness = new Histogram().merge(this.#lateness.snapshot()).merge(snapshot.timerLatenessMs);
    this.#attempts = attempts;
    this.#operations = operations;
    this.#lateness = lateness;
    return this;
  }

  snapshot(durationMs) {
    if (!Number.isFinite(durationMs) || durationMs < 0) {
      throw new RangeError("durationMs must be finite and nonnegative");
    }
    return {
      durationMs,
      histogram: HISTOGRAM_PROPERTIES,
      ...this.#attempts.snapshot(durationMs),
      operations: logicalSnapshot(this.#operations.snapshot(durationMs)),
      timerLatenessMs: this.#lateness.snapshot(),
    };
  }
}
