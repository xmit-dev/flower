import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const PROJECT_ROOT = fileURLToPath(new URL("../", import.meta.url));

export const HELP = `Usage: node bench/goblin-pizza.mjs [options]

Goblin Pizza stress benchmark: Rust customer loops wait for replies. With
--offered-rate N, arrivals follow an independent clock; saturated
driver slots drop and count arrivals rather than delaying them. At most --max-orders are admitted;
after the measured interval, a bounded drain checks their final state. This
reports customer goodput separately from offered and driver-dropped arrivals.

  --duration SECONDS          Measured load interval (default 10; 1–3600)
  --warmup SECONDS            Warmup before measurement (default 1; 0–60)
  --drain SECONDS             Maximum final drain (default 60; 1–3600)
  --concurrency N             Concurrent client loops or open-loop slots (default 8/group)
  --offered-rate N            Independent arrivals/sec/group (default 0: closed-loop)
  --workers N                 Delivery worker loops (default 2 per group)
  --groups N                  Independent Raft groups and client processes (default 1)
  --tenants N                 Tenants per group (default 2)
  --shops N                   Stores per tenant (default 4)
  --hot-shops N               Hot shops, at most shops (default 1)
  --hot-probability FRACTION  Hot-set mixture weight; rest uniform (default 0.8)
  --max-orders N              Total admitted order cap (default 32 per group)
  --bake-ms MS                Scheduled baking delay (default 250; 0–60000)
  --lease-ms MS               Delivery lease duration (default 2000; 100–60000)
  --duplicate-rate FRACTION   Deliberate mutation retry rate (default 0.1; 0–1)
  --abandon-rate FRACTION     Deliberately abandoned lease rate (default 0.1; 0–1)
  --poll-ms MS                Worker polling interval (default 150; 10–10000)
  --request-timeout-ms MS     Per-request timeout (default 2000; 100–60000)
  --retry-budget-ms MS        Total operation retry budget (default 15000;
                             at least request timeout and at most 300000)
  --seed VALUE               Reproducible workload seed (default 42)
  --nodes N                  Raft cluster size: 1 or 3 (default 3)
  --binary PATH              Flower binary (default target/release/flower)
  --guest KIND               js (default): examples/goblin-pizza-ts/goblin-pizza.ts
                             on QuickJS, or wasm: its Rust port, examples/goblin-pizza-rs
  --guest-wasm PATH          Wasm guest module (default
                             target/wasm32-unknown-unknown/release/goblin_pizza.wasm)
  --initialization MODE      static (default) or per-invocation bundle initialization
                             (JavaScript guest only)
  --json PATH                JSON report (default bench/results/latest.json;
                             latest-wasm.json for the Wasm guest)
  --html PATH                HTML report (default JSON path with .html extension)
  --baseline PATH            Earlier JSON report for HTML before/after comparison
  --cpu-profile PATH         macOS sample text output; initial leader, up to 10s
                             from load start at 1ms; perturbs performance
  --chaos                    Kill a leader during load (requires 3 nodes); with
                             --hosted, kill the host leading the most groups
  --hosted                   One server process per replica slot, hosting that
                             replica of every group over one shared database,
                             so all groups share its fsyncs
  --http2                    Use pooled h2c for application methods (default HTTP/1)
  --query-routing MODE       replicas (default) or leader; queries use the chosen consistency
  --read-consistency MODE     replica-local (default) or fresh for customer previews;
                             audits always use fresh reads
  --driver-binary PATH       Rust customer driver (default target/release/flower-bench-driver)
  --keep-data                Preserve temporary cluster data after completion
  --data DIR[,DIR...]        Parent directories for server data (default: the system
                             temporary directory); servers alternate between them,
                             e.g. to give replicas separate disks
  --help                     Show this help; use alone

Seconds can be fractional; counts and milliseconds must be integers.
Custom paths resolve relative to the current directory. Default paths resolve
relative to this checkout. Flags also accept --name=value; duplicates are errors.
`;

const NUMERIC = {
  "offered-rate": {field:"offeredRate",initial:0,min:0,max:Number.MAX_SAFE_INTEGER},
  groups: { field: "groups", initial: 1, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  tenants: { field: "tenants", initial: 2, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  duration: { field: "duration", initial: 10, min: 1, max: 3_600 },
  warmup: { field: "warmup", initial: 1, min: 0, max: 60 },
  drain: { field: "drain", initial: 60, min: 1, max: 3_600 },
  concurrency: { field: "concurrency", initial: 8, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  workers: { field: "workers", initial: 2, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  shops: { field: "shops", initial: 4, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  "hot-shops": { field: "hotShops", initial: 1, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  "hot-probability": { field: "hotProbability", initial: 0.8, min: 0, max: 1 },
  "max-orders": { field: "maxOrders", initial: 32, min: 1, max: Number.MAX_SAFE_INTEGER, integer: true },
  "bake-ms": { field: "bakeMs", initial: 250, min: 0, max: 60_000, integer: true },
  "lease-ms": { field: "leaseMs", initial: 2_000, min: 100, max: 60_000, integer: true },
  "duplicate-rate": { field: "duplicateRate", initial: 0.1, min: 0, max: 1 },
  "abandon-rate": { field: "abandonRate", initial: 0.1, min: 0, max: 1 },
  "poll-ms": { field: "pollMs", initial: 150, min: 10, max: 10_000, integer: true },
  "request-timeout-ms": { field: "requestTimeoutMs", initial: 2_000, min: 100, max: 60_000, integer: true },
  "retry-budget-ms": { field: "retryBudgetMs", initial: 15_000, min: 100, max: 300_000, integer: true },
  nodes: { field: "nodes", initial: 3, min: 1, max: 3, integer: true },
};

const BOOLEAN = { chaos: "chaos", hosted: "hosted", http2: "http2", "keep-data": "keepData", help: "help" };
const TEXT = { seed: "seed", guest: "guest", "guest-wasm": "guestWasm", initialization: "initialization", "query-routing": "queryRouting", "read-consistency": "readConsistency", "driver-binary": "driverBinary", binary: "binary", json: "json", html: "html", baseline: "baseline", "cpu-profile": "cpuProfile", data: "data" };
const NUMBER_PATTERN = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;

function defaults() {
  return {
    ...Object.fromEntries(Object.values(NUMERIC).map(({ field, initial }) => [field, initial])),
    seed: "42",
    guest: "js",
    guestWasm: resolve(PROJECT_ROOT, "target/wasm32-unknown-unknown/release/goblin_pizza.wasm"),
    initialization: "static",
    queryRouting: "replicas",
    readConsistency: "replica-local",
    driver: "rust",
    driverBinary: resolve(PROJECT_ROOT, "target/release/flower-bench-driver"),
    binary: resolve(PROJECT_ROOT, "target/release/flower"),
    json: null,
    html: null,
    baseline: null,
    cpuProfile: null,
    data: null,
    chaos: false,
    hosted: false,
    http2: false,
    keepData: false,
    help: false,
  };
}

function parseNumber(name, raw, rule) {
  const value = Number(raw);
  if (!NUMBER_PATTERN.test(raw) || !Number.isFinite(value)) {
    throw new Error(`--${name} requires a finite number`);
  }
  if (rule.integer && !Number.isSafeInteger(value)) {
    throw new Error(`--${name} requires an integer`);
  }
  if (value < rule.min || value > rule.max) {
    throw new Error(`--${name} must be between ${rule.min} and ${rule.max}`);
  }
  return value;
}

/** Parse only the flags after the executable and script name. */
export function parseOptions(argv) {
  if (!Array.isArray(argv) || argv.some((value) => typeof value !== "string")) {
    throw new TypeError("argv must be an array of strings");
  }
  const options = defaults();
  const seen = new Set();
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (!argument.startsWith("--") || argument === "--") {
      throw new Error(`Unexpected argument: ${argument}`);
    }
    const equals = argument.indexOf("=");
    const name = argument.slice(2, equals === -1 ? undefined : equals);
    if (!Object.hasOwn(NUMERIC, name) && !Object.hasOwn(BOOLEAN, name) && !Object.hasOwn(TEXT, name)) {
      throw new Error(`Unknown option: --${name}`);
    }
    if (seen.has(name)) throw new Error(`Duplicate option: --${name}`);
    seen.add(name);
    if (Object.hasOwn(BOOLEAN, name)) {
      if (equals !== -1) throw new Error(`--${name} does not accept a value`);
      options[BOOLEAN[name]] = true;
      continue;
    }
    let raw;
    if (equals !== -1) raw = argument.slice(equals + 1);
    else {
      raw = argv[++index];
      if (raw === undefined || raw.startsWith("--")) throw new Error(`--${name} requires a value`);
    }
    if (raw.trim() === "") throw new Error(`--${name} requires a nonempty value`);
    if (Object.hasOwn(NUMERIC, name)) {
      const rule = NUMERIC[name];
      options[rule.field] = parseNumber(name, raw, rule);
    } else if (name === "data") {
      const roots = raw.split(",");
      if (roots.some((root) => root.trim() === "")) throw new Error("--data requires nonempty directories");
      options.data = roots.map((root) => resolve(root));
    } else options[TEXT[name]] = ["seed", "guest", "initialization", "query-routing", "read-consistency"].includes(name) ? raw : resolve(raw);
  }
  if (options.help && argv.length !== 1) throw new Error("--help must be used alone");
  if (!["js", "wasm"].includes(options.guest)) throw new Error("--guest must be js or wasm");
  if (!["static", "per-invocation"].includes(options.initialization)) throw new Error("--initialization must be static or per-invocation");
  if (options.guest === "wasm" && options.initialization !== "static") throw new Error("--initialization applies only to the JavaScript guest");
  options.json ??= resolve(PROJECT_ROOT, options.guest === "wasm" ? "bench/results/latest-wasm.json" : "bench/results/latest.json");
  if (!["leader", "replicas"].includes(options.queryRouting)) throw new Error("--query-routing must be leader or replicas");
  if (!["fresh", "replica-local"].includes(options.readConsistency)) throw new Error("--read-consistency must be fresh or replica-local");
  if (!Number.isSafeInteger(Math.ceil(options.offeredRate*options.duration))) throw new Error("Offered arrival count exceeds safe integer range");
  if (!Number.isSafeInteger(options.groups * options.tenants * options.shops)) throw new Error("Total store count exceeds safe integer range");
  if (options.groups > 1 && (options.baseline || options.cpuProfile)) throw new Error("--baseline and --cpu-profile require --groups 1");
  if (options.nodes !== 1 && options.nodes !== 3) throw new Error("--nodes must be 1 or 3");
  if (options.chaos && options.nodes !== 3) throw new Error("--chaos requires --nodes 3");
  if (options.hotShops > options.shops) throw new Error("--hot-shops must not exceed --shops");
  if (options.retryBudgetMs < options.requestTimeoutMs) {
    throw new Error("--retry-budget-ms must be at least --request-timeout-ms");
  }
  options.html ??= options.json.replace(/\.json$/i, "") + ".html";
  if (options.html === options.json) throw new Error("--html and --json must use different files");
  if (options.baseline === options.json || options.baseline === options.html) throw new Error("Reports must not overwrite --baseline");
  if (options.cpuProfile && [options.json, options.html, options.baseline, options.binary, options.guestWasm].includes(options.cpuProfile)) {
    throw new Error("--cpu-profile must not overwrite a report, baseline, or binary");
  }
  return options;
}
