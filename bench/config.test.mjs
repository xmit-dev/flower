import assert from "node:assert/strict";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { HELP, parseOptions } from "./config.mjs";

test("defaults are bounded and resolve paths from the checkout", () => {
  const result = parseOptions([]);
  assert.deepEqual(result, {
    offeredRate:0, groups: 1, tenants: 2, readConsistency: "replica-local", duration: 10, warmup: 1, drain: 60, concurrency: 8, workers: 2,
    shops: 4, hotShops: 1, hotProbability: 0.8, maxOrders: 32,
    bakeMs: 250, leaseMs: 2_000, duplicateRate: 0.1, abandonRate: 0.1,
    pollMs: 150, requestTimeoutMs: 2_000, retryBudgetMs: 15_000, seed: "42", nodes: 3, initialization: "static", queryRouting: "replicas",
    guest: "js", guestWasm: fileURLToPath(new URL("../target/wasm32-unknown-unknown/release/goblin_pizza.wasm", import.meta.url)),
    driver: "rust", driverBinary: fileURLToPath(new URL("../target/release/flower-bench-driver", import.meta.url)),
    binary: fileURLToPath(new URL("../target/release/flower", import.meta.url)),
    json: fileURLToPath(new URL("../bench/results/latest.json", import.meta.url)),
    html: fileURLToPath(new URL("../bench/results/latest.html", import.meta.url)), baseline: null, cpuProfile: null, data: null,
    chaos: false, hosted: false, http2: false, keepData: false, help: false,
  });
  result.duration = 123;
  assert.equal(parseOptions([]).duration, 10);
});

test("all numeric, text, and boolean options parse into driver fields", () => {
  assert.deepEqual(parseOptions([
    "--duration=1.5", "--warmup", "0", "--drain", "2.5",
    "--concurrency", "256", "--workers", "64", "--shops", "32", "--hot-shops", "32",
    "--hot-probability", "0", "--max-orders", "10000", "--bake-ms", "0",
    "--lease-ms", "100", "--duplicate-rate", "1", "--abandon-rate", "0",
    "--poll-ms", "10", "--request-timeout-ms", "60000", "--retry-budget-ms", "300000",
    "--seed", "goblin army", "--nodes", "3", "--chaos", "--http2", "--keep-data", "--query-routing=leader",
    "--binary", "./a flower", "--json=./output report.json",
  ]), {
    offeredRate:0, groups: 1, tenants: 2, readConsistency: "replica-local", duration: 1.5, warmup: 0, drain: 2.5, concurrency: 256, workers: 64,
    shops: 32, hotShops: 32, hotProbability: 0, maxOrders: 10_000,
    bakeMs: 0, leaseMs: 100, duplicateRate: 1, abandonRate: 0,
    pollMs: 10, requestTimeoutMs: 60_000, retryBudgetMs: 300_000, seed: "goblin army", nodes: 3, initialization: "static", queryRouting: "leader",
    guest: "js", guestWasm: fileURLToPath(new URL("../target/wasm32-unknown-unknown/release/goblin_pizza.wasm", import.meta.url)),
    driver: "rust", driverBinary: fileURLToPath(new URL("../target/release/flower-bench-driver", import.meta.url)),
    binary: resolve("a flower"), json: resolve("output report.json"),
    html: resolve("output report.html"), baseline: null, cpuProfile: null, data: null,
    chaos: true, hosted: false, http2: true, keepData: true, help: false,
  });
  assert.equal(parseOptions(["--seed=0"]).seed, "0");
  assert.equal(parseOptions(["--initialization", "per-invocation"]).initialization, "per-invocation");
  assert.throws(() => parseOptions(["--initialization", "unsafe"]), /initialization/);
  const wasm = parseOptions(["--guest", "wasm", "--guest-wasm", "guest module.wasm"]);
  assert.equal(wasm.guestWasm, resolve("guest module.wasm"));
  assert.equal(wasm.json, fileURLToPath(new URL("../bench/results/latest-wasm.json", import.meta.url)));
  assert.equal(wasm.html, fileURLToPath(new URL("../bench/results/latest-wasm.html", import.meta.url)));
  assert.equal(parseOptions(["--guest", "wasm", "--json", "w.json"]).json, resolve("w.json"));
  assert.throws(() => parseOptions(["--guest", "lua"]), /--guest must be js or wasm/);
  assert.throws(() => parseOptions(["--guest", "wasm", "--initialization", "per-invocation"]), /only to the JavaScript guest/);
  assert.equal(parseOptions(["--query-routing", "replicas"]).queryRouting, "replicas");
  assert.throws(() => parseOptions(["--query-routing", "stale"]), /query-routing/);
  assert.equal(parseOptions(["--duration", "1e1"]).duration, 10);
  assert.equal(parseOptions(["--html", "custom.html"]).html, resolve("custom.html"));
  assert.equal(parseOptions(["--baseline", "before.json"]).baseline, resolve("before.json"));
  assert.equal(parseOptions(["--cpu-profile", "cpu sample.txt"]).cpuProfile, resolve("cpu sample.txt"));
  assert.deepEqual(parseOptions(["--data", "/Volumes/Data,internal disk"]).data, ["/Volumes/Data", resolve("internal disk")]);
  assert.throws(() => parseOptions(["--data", "a,,b"]), /nonempty directories/);
  assert.throws(() => parseOptions(["--html", "same", "--json", "same"]), /different/);
  assert.throws(() => parseOptions(["--baseline", "same", "--json", "same"]), /overwrite/);
  for (const flag of ["--json", "--html", "--baseline", "--binary"]) {
    assert.throws(() => parseOptions(["--cpu-profile", "same", flag, "same"]), /overwrite/);
  }
});

test("help is standalone and describes the bounded closed-loop methodology", () => {
  assert.equal(parseOptions(["--help"]).help, true);
  assert.match(HELP, /closed-loop/);
  assert.match(HELP, /bounded drain/);
  for (const flag of ["--duration", "--warmup", "--drain", "--hot-shops", "--chaos", "--retry-budget-ms", "--cpu-profile", "--query-routing"]) {
    assert.ok(HELP.includes(flag));
  }
  assert.throws(() => parseOptions(["--help", "--nodes", "1"]), /used alone/);
});

test("unknown, duplicate, positional, missing, and boolean-value arguments are rejected", () => {
  for (const args of [
    ["--typo", "1"], ["--__proto__", "1"], ["--constructor", "1"], ["filename"], ["--"],
    ["--nodes", "1", "extra"], ["--duration"], ["--duration", "--nodes", "1"],
    ["--duration", "1", "--duration=2"], ["--chaos", "--chaos"],
    ["--chaos=false"], ["--keep-data", "true"], ["--help=true"],
    ["--json="], ["--seed", " "], ["--binary", ""],
  ]) assert.throws(() => parseOptions(args), Error, args.join(" "));
  assert.throws(() => parseOptions("--help"), TypeError);
  assert.throws(() => parseOptions([1]), TypeError);
});

test("nonfinite numbers, fractional counts, and every numeric range are validated", () => {
  for (const value of ["NaN", "Infinity", "-Infinity", "1e999", "0x10", "true", "", " "]) {
    assert.throws(() => parseOptions(["--duration", value]), Error);
  }
  const ranges = [
    ["duration", "0", "3601"], ["warmup", "-1", "61"], ["drain", "0", "3601"],
    ["concurrency", "0", "9007199254740992"], ["workers", "0", "9007199254740992"], ["shops", "0", "9007199254740992"],
    ["hot-shops", "0", "9007199254740992"], ["hot-probability", "-0.1", "1.1"],
    ["max-orders", "0", "9007199254740992"], ["bake-ms", "-1", "60001"], ["lease-ms", "99", "60001"],
    ["duplicate-rate", "-0.1", "1.1"], ["abandon-rate", "-0.1", "1.1"],
    ["poll-ms", "9", "10001"], ["request-timeout-ms", "99", "60001"],
    ["retry-budget-ms", "99", "300001"], ["nodes", "0", "4"],
  ];
  for (const [flag, low, high] of ranges) {
    for (const value of [low, high]) assert.throws(() => parseOptions([`--${flag}`, value]), Error, `${flag}=${value}`);
  }
  for (const flag of ["concurrency", "workers", "shops", "hot-shops", "max-orders", "bake-ms", "lease-ms", "poll-ms", "request-timeout-ms", "retry-budget-ms", "nodes"]) {
    assert.throws(() => parseOptions([`--${flag}`, "100.5"]), /integer/);
  }
});

test("cluster size, hot-shop count, and timeout relationships are validated", () => {
  assert.equal(parseOptions(["--nodes", "1"]).nodes, 1);
  assert.throws(() => parseOptions(["--nodes", "2"]), /1 or 3/);
  assert.throws(() => parseOptions(["--nodes", "1", "--chaos"]), /requires --nodes 3/);
  assert.throws(() => parseOptions(["--shops", "2", "--hot-shops", "3"]), /must not exceed/);
  assert.throws(() => parseOptions(["--request-timeout-ms", "2000", "--retry-budget-ms", "1999"]), /at least/);
  assert.equal(parseOptions(["--request-timeout-ms", "2000", "--retry-budget-ms", "2000"]).retryBudgetMs, 2_000);
});


test("multitenant groups accept scale and explicit read semantics without old count caps", () => {
  const options = parseOptions(["--groups", "8", "--tenants", "100", "--shops", "128", "--hot-shops", "64", "--concurrency", "4096", "--read-consistency", "replica-local"]);
  assert.equal(options.groups, 8);
  assert.equal(options.tenants, 100);
  assert.equal(options.readConsistency, "replica-local");
  assert.throws(() => parseOptions(["--read-consistency", "snapshot-ish"]), /read-consistency/);
  assert.throws(() => parseOptions(["--groups", "3", "--cpu-profile", "sample.txt"]), /require --groups 1/);
  assert.throws(() => parseOptions(["--groups", "9007199254740991"]), /Total store count/);
});

test("Rust is the only customer driver and the binary path is resolved", () => {
  assert.equal(parseOptions([]).driver, "rust");
  assert.equal(parseOptions(["--driver-binary", "native driver"]).driverBinary, resolve("native driver"));
  for (const name of ["node", "rust"]) {
    assert.throws(() => parseOptions(["--driver", name]), /Unknown option: --driver/);
  }
});


test("independent arrival rate preserves its per-group rate",()=>{
  assert.equal(parseOptions(["--offered-rate","2500.5"]).offeredRate,2500.5);
  assert.throws(()=>parseOptions(["--offered-rate","-1"]));
  assert.throws(()=>parseOptions(["--offered-rate","9007199254740991","--duration","2"]),/arrival count/);
});
