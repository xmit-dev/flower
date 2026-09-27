import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const engineCode = readFileSync(new URL("./engine.js", import.meta.url), "utf8");
const engineContext = vm.createContext(Object.create(null));
vm.runInContext(engineCode, engineContext, { filename: "engine.js" });
const evaluate = engineContext.flowerEvaluate;
const invoke = engineContext.flowerInvoke;
const plain = (value) => JSON.parse(JSON.stringify(value));
const collection = (name) => ({ kind: "collection", name });
const derived = (name) => ({ kind: "derived", name });
const cellKey = (name, args = null) => `cell:${JSON.stringify([name, args])}`;
const sourceKey = (name, key) => `source:${JSON.stringify([name, key])}`;

test("native identity encoding preserves escaped and Unicode source/cell keys", () => {
  for (const text of ["", "10", "__proto__", 'quote"slash\\', "\n\u0000", "\u2028", "\ue000", "🌺", "\ud800"]) {
    const name = `value:${text}`;
    const rows = collection(`rows:${text}`);
    const db = harness({ [name]: (ctx, args) => ctx.get(rows, args) });
    db.mutate({ writes: [{ collection: rows.name, key: text, value: 7 }], materialize: [{ name, args: text }] });
    assert.equal(db.data[sourceKey(rows.name, text)], 7);
    assert.deepEqual(db.data[cellKey(name, text)].deps, [sourceKey(rows.name, text)]);
    assert.equal(db.value(name, text), 7);
    db.mutate({ writes: [{ collection: rows.name, key: text, value: 9 }] });
    assert.equal(db.value(name, text), 9);
  }
});

test("output limits count exact encoded UTF-8 bytes independent of object key ordering", () => {
  const value = { "10": "🌺".repeat(20), "2": "\ud800", "\ue000": "é", "🌺": [1, null] };
  const result = plain(invoke({}, { kind: "query", name: "read" }, () => value, () => null));
  const bytes = Buffer.byteLength(JSON.stringify(result));
  for (const limit of [bytes, bytes - 1]) {
    const context = vm.createContext(Object.create(null));
    vm.runInContext(engineCode.replace("var MAX_OUTPUT_BYTES = 16 * 1024 * 1024;", `var MAX_OUTPUT_BYTES = ${limit};`), context);
    const call = () => context.flowerInvoke({}, { kind: "query", name: "read" }, () => value, () => null);
    if (limit === bytes) assert.deepEqual(plain(call()), result);
    else assert.throws(call, (error) => error.code === "EVALUATION_BUDGET");
  }
});

function harness(definitions, initial = {}) {
  let state = Object.assign(Object.create(null), plain(initial));
  let nextRequest = 0;
  const calls = [];
  const compute = (name, args, context) => {
    calls.push([name, args]);
    if (!Object.hasOwn(definitions, name)) throw Object.assign(new Error(`Unknown definition: ${name}`), { code: "UNKNOWN_DEFINITION" });
    return definitions[name](context, args);
  };
  return {
    get data() { return state; },
    calls,
    mutate(command = {}) {
      const before = JSON.stringify(state);
      let result;
      try {
        result = plain(evaluate(state, { requestId: `r${nextRequest++}`, ...command }, compute));
      } finally {
        assert.equal(JSON.stringify(state), before, "evaluation never mutates its input");
      }
      state = Object.assign(Object.create(null), state, result.puts);
      for (const key of result.deletes) delete state[key];
      return result;
    },
    outcome(name, args = null) { return state[cellKey(name, args)]?.outcome; },
    value(name, args = null) {
      const outcome = this.outcome(name, args);
      assert.equal(outcome?.ok, true, JSON.stringify(outcome));
      return outcome.value;
    },
  };
}

test("a staged transaction evaluates a diamond consistently and only once per cell", () => {
  const values = collection("values");
  const definitions = {
    left: (ctx) => ctx.get(values, "x") * 2,
    right: (ctx) => ctx.get(values, "x") + ctx.get(values, "y"),
    total: (ctx) => ctx.get(derived("left")) + ctx.get(derived("right")),
  };
  const db = harness(definitions);
  db.mutate({ writes: [{ collection: "values", key: "x", value: 1 }, { collection: "values", key: "y", value: 10 }], materialize: [{ name: "total" }] });
  assert.equal(db.value("total"), 13);
  const result = db.mutate({ writes: [{ collection: "values", key: "x", value: 3 }, { collection: "values", key: "y", value: 20 }] });
  assert.equal(db.value("total"), 29);
  assert.equal(result.evaluated.length, 3);
  assert.equal(new Set(result.evaluated).size, 3);
  assert.deepEqual(db.data[cellKey("total")].deps, [cellKey("left"), cellKey("right")]);
});

test("missing point reads invalidate on creation and deletion", () => {
  const db = harness({ value: (ctx) => ctx.get(collection("records"), "missing") });
  db.mutate({ materialize: [{ name: "value" }] });
  assert.equal(db.value("value"), null);
  db.mutate({ writes: [{ collection: "records", key: "missing", value: 7 }] });
  assert.equal(db.value("value"), 7);
  db.mutate({ writes: [{ collection: "records", key: "missing", delete: true }] });
  assert.equal(db.value("value"), null);
});

test("unchanged and net-unchanged source writes do not invalidate", () => {
  const db = harness({ value: (ctx) => ctx.get(collection("records"), "x") });
  db.mutate({ materialize: [{ name: "value" }], writes: [{ collection: "records", key: "x", value: { b: 2, a: 1 } }] });
  assert.deepEqual(db.mutate({ writes: [{ collection: "records", key: "x", value: { a: 1, b: 2 } }] }), { puts: {}, deletes: [], evaluated: [] });
  const result = db.mutate({ writes: [{ collection: "records", key: "x", value: 4 }, { collection: "records", key: "x", value: { a: 1, b: 2 } }] });
  assert.deepEqual(result.evaluated, []);
  assert.deepEqual(db.mutate({ writes: [{ collection: "records", key: "absent", delete: true }] }).evaluated, []);
});

test("conditional dependencies are replaced after successful evaluation", () => {
  const db = harness({ selected: (ctx) => ctx.get(collection("values"), ctx.get(collection("values"), "selector")) });
  db.mutate({ writes: [
    { collection: "values", key: "selector", value: "a" },
    { collection: "values", key: "a", value: 1 },
    { collection: "values", key: "b", value: 2 },
  ], materialize: [{ name: "selected" }] });
  db.mutate({ writes: [{ collection: "values", key: "selector", value: "b" }, { collection: "values", key: "b", value: 3 }] });
  assert.equal(db.value("selected"), 3);
  assert.deepEqual(db.mutate({ writes: [{ collection: "values", key: "a", value: 9 }] }).evaluated, []);
  assert.deepEqual(db.data[cellKey("selected")].deps, [sourceKey("values", "b"), sourceKey("values", "selector")]);
});

test("scan and equality queries catch inserts, deletes and records entering predicates", () => {
  const db = harness({
    scan: (ctx) => ctx.scan(collection("items")),
    open: (ctx) => ctx.query({ kind: "query", collection: "items", fields: ["status"], value: "open" }),
    owner: (ctx) => ctx.query({ kind: "query", collection: collection("items"), fields: ["status", "owner"], value: ["open", "alice"] }),
  });
  db.mutate({ materialize: [{ name: "scan" }, { name: "open" }, { name: "owner" }] });
  assert.deepEqual(db.value("open"), []);
  db.mutate({ writes: [
    { collection: "items", key: "z", value: { status: "open", owner: "alice" } },
    { collection: "items", key: "a", value: { status: "closed", owner: "bob" } },
    { collection: "items", key: "b", value: { status: "open", owner: "bob" } },
  ] });
  assert.deepEqual(db.value("scan").map((row) => row.key), ["a", "b", "z"]);
  assert.deepEqual(db.value("open").map((row) => row.owner), ["bob", "alice"]);
  assert.equal(db.value("owner").length, 1);
  db.mutate({ writes: [{ collection: "items", key: "z", delete: true }, { collection: "items", key: "a", value: { status: "open", owner: "alice" } }] });
  assert.deepEqual(db.value("open").map((row) => row.owner), ["alice", "bob"]);
  assert.deepEqual(db.mutate({ writes: [{ collection: "unrelated", key: "x", value: 1 }] }).evaluated, []);
});

test("query equality is canonical, distinguishes absent fields, and supports array-valued fields", () => {
  const db = harness({
    object: (ctx) => ctx.query({ kind: "query", collection: "items", fields: ["object"], value: { a: 1, b: 2 } }),
    array: (ctx) => ctx.query({ kind: "query", collection: "items", fields: ["array"], value: [1, 2] }),
    null: (ctx) => ctx.query({ kind: "query", collection: "items", fields: ["nullable"], value: null }),
  });
  db.mutate({ writes: [
    { collection: "items", key: "a", value: { object: { b: 2, a: 1 }, array: [1, 2], nullable: null } },
    { collection: "items", key: "b", value: {} },
  ], materialize: [{ name: "object" }, { name: "array" }, { name: "null" }] });
  for (const name of ["object", "array", "null"]) assert.equal(db.value(name).length, 1);
});

test("error outcomes preserve previous plus newly observed dependencies and recover", () => {
  const db = harness({
    value: (ctx) => {
      const mode = ctx.get(collection("data"), "mode");
      if (mode === "fail") { ctx.get(collection("data"), "b"); throw new Error("broken"); }
      return ctx.get(collection("data"), mode);
    },
    consumer: (ctx) => ctx.get(derived("value")) + 1,
  });
  db.mutate({ writes: [
    { collection: "data", key: "mode", value: "a" },
    { collection: "data", key: "a", value: 10 },
    { collection: "data", key: "b", value: 20 },
  ], materialize: [{ name: "consumer" }] });
  db.mutate({ writes: [{ collection: "data", key: "mode", value: "fail" }] });
  assert.deepEqual(db.outcome("value"), { ok: false, error: { code: "COMPUTE_ERROR", message: "broken" } });
  assert.equal(db.outcome("consumer").ok, false);
  assert.deepEqual(db.data[cellKey("value")].deps, [sourceKey("data", "a"), sourceKey("data", "b"), sourceKey("data", "mode")]);
  assert.equal(db.mutate({ writes: [{ collection: "data", key: "a", value: 11 }] }).evaluated.length, 2);
  db.mutate({ writes: [{ collection: "data", key: "mode", value: "b" }] });
  assert.equal(db.value("consumer"), 21);
  assert.equal(db.mutate({ writes: [{ collection: "data", key: "a", value: 12 }] }).evaluated.length, 0);
});

test("user code may catch a recoverable dependency error", () => {
  const db = harness({
    broken: () => { throw Object.assign(new Error("missing external input"), { code: "NOT_READY" }); },
    fallback: (ctx) => { try { return ctx.get(derived("broken")); } catch { return "waiting"; } },
  });
  db.mutate({ materialize: [{ name: "fallback" }] });
  assert.equal(db.value("fallback"), "waiting");
  assert.equal(db.outcome("broken").error.code, "NOT_READY");
});

test("unmaterialization and changing dynamic derived dependencies collect unreachable cells", () => {
  const db = harness({
    leaf: (_ctx, arg) => arg * 2,
    a: (ctx) => ctx.get(derived("leaf"), ctx.get(collection("config"), "selected")),
    b: (ctx) => ctx.get(derived("leaf"), 1),
  });
  db.mutate({ writes: [{ collection: "config", key: "selected", value: 1 }], materialize: [{ name: "a" }, { name: "b" }] });
  db.mutate({ writes: [{ collection: "config", key: "selected", value: 2 }] });
  assert.equal(db.value("leaf", 1), 2);
  assert.equal(db.value("leaf", 2), 4);
  const result = db.mutate({ unmaterialize: [{ name: "b" }] });
  assert.ok(result.deletes.includes(cellKey("leaf", 1)));
  assert.ok(result.deletes.includes(cellKey("b")));
  db.mutate({ unmaterialize: [{ name: "a" }] });
  assert.equal(Object.keys(db.data).some((key) => key.startsWith("cell:") || key.startsWith("root:")), false);
});

test("bundle activation invalidates live cells without false cycles from old edges", () => {
  let version = 1;
  const db = harness({
    a: (ctx) => version === 1 ? ctx.get(derived("b")) + 1 : 20,
    b: (ctx) => version === 1 ? 1 : ctx.get(derived("a")) + 1,
  });
  db.mutate({ bundle: { hash: "v1", javascript: "first" }, materialize: [{ name: "a" }, { name: "b" }] });
  assert.equal(db.value("a"), 2);
  version = 2;
  const result = db.mutate({ bundle: { hash: "v2", javascript: "second" } });
  assert.equal(result.evaluated.length, 2);
  assert.equal(db.value("a"), 20);
  assert.equal(db.value("b"), 21);
  assert.deepEqual(db.mutate({ bundle: { hash: "v2", javascript: "second" } }).evaluated, []);
});

test("direct and swallowed cycles abort all staged changes", () => {
  for (const swallow of [false, true]) {
    const db = harness({
      a: (ctx) => {
        if (!swallow) return ctx.get(derived("b"));
        try { return ctx.get(derived("b")); } catch { return 1; }
      },
      b: (ctx) => ctx.get(derived("a")),
    });
    assert.throws(() => db.mutate({ writes: [{ collection: "x", key: "y", value: 1 }], materialize: [{ name: "a" }] }), (error) => error.code === "CYCLE");
    assert.deepEqual(Object.keys(db.data), []);
  }
});

test("retained error dependencies cannot introduce a hidden graph cycle", () => {
  let version = 1;
  const db = harness({
    a: (ctx) => { if (version === 1) return ctx.get(derived("b")); throw new Error("a failed"); },
    b: (ctx) => { if (version === 1) return 1; try { return ctx.get(derived("a")); } catch { return 2; } },
  });
  db.mutate({ materialize: [{ name: "a" }, { name: "b" }] });
  const before = plain(db.data);
  version = 2;
  assert.throws(() => db.mutate({ bundle: { hash: "v2", javascript: "v2" } }), (error) => error.code === "CYCLE");
  assert.deepEqual(plain(db.data), before);
});

test("reads, derived arguments, and returned values cannot mutate persisted inputs", () => {
  let escapedContext;
  const db = harness({
    child: (_ctx, arg) => { arg.n = 100; return arg; },
    root: (ctx) => {
      escapedContext = ctx;
      const record = ctx.get(collection("data"), "x");
      record.n = 2;
      const scan = ctx.scan(collection("data"));
      scan[0].value.n = 3;
      const arg = { n: 1 };
      const result = ctx.get(derived("child"), arg);
      result.n = 200;
      return [ctx.get(collection("data"), "x").n, arg.n, ctx.get(derived("child"), arg).n];
    },
  });
  db.mutate({ writes: [{ collection: "data", key: "x", value: { n: 1 } }], materialize: [{ name: "root" }] });
  assert.deepEqual(db.value("root"), [1, 1, 100]);
  assert.equal(db.data[sourceKey("data", "x")].n, 1);
  assert.throws(() => escapedContext.get(collection("data"), "x"), (error) => error.code === "INVALID_CONTEXT");
});

test("prototype-like keys and canonical numeric-looking object keys are safe", () => {
  const special = JSON.parse('{"__proto__":{"polluted":true},"constructor":2,"10":"ten","2":"two"}');
  const definitions = Object.create(null);
  definitions.__proto__ = (ctx) => ctx.get(collection("__proto__"), "constructor");
  const db = harness(definitions);
  db.mutate({ writes: [{ collection: "__proto__", key: "constructor", value: special }], materialize: [{ name: "__proto__", args: special }] });
  const id = 'cell:["__proto__",{"10":"ten","2":"two","__proto__":{"polluted":true},"constructor":2}]';
  assert.equal(db.data[id].outcome.value.__proto__.polluted, true);
  assert.equal(Object.prototype.polluted, undefined);
  assert.deepEqual(db.mutate({ materialize: [{ name: "__proto__", args: { "2": "two", constructor: 2, "10": "ten", ["__proto__"]: { polluted: true } } }] }).evaluated, []);
});

test("invalid mutation values reject while invalid compute returns become recoverable errors", () => {
  const cycle = {}; cycle.self = cycle;
  const sparse = []; sparse.length = 1;
  for (const bad of [undefined, NaN, Infinity, 1n, () => 1, Symbol("x"), cycle, sparse, new Date(), Promise.resolve(1)]) {
    const db = harness({ bad: () => bad });
    assert.throws(() => db.mutate({ writes: [{ collection: "data", key: "x", value: bad }] }), (error) => error.code === "INPUT_INVALID");
    db.mutate({ materialize: [{ name: "bad" }] });
    assert.equal(db.outcome("bad").error.code, "INVALID_VALUE");
  }
});

test("malformed mutations and references fail with explicit errors", () => {
  const db = harness({ bad: (ctx) => ctx.get({ kind: "derived" }) });
  for (const command of [
    { requestId: 1 }, { expectedRevision: -1 }, { writes: {} },
    { writes: [{ collection: "a", key: "b" }] },
    { writes: [{ collection: "a", key: "b", delete: false }] },
    { writes: [{ collection: "a", key: "b", delete: true, value: 1 }] },
    { materialize: [{ name: 1 }] }, { bundle: { hash: "x" } },
  ]) assert.throws(() => db.mutate(command), (error) => error.code === "INPUT_INVALID");
  db.mutate({ materialize: [{ name: "bad" }] });
  assert.equal(db.outcome("bad").error.code, "INVALID_REFERENCE");
});

test("read, evaluation, recursion, output and host budgets abort even when caught", () => {
  const scenarios = [
    { name: "reads", compute: (ctx) => { try { for (let i = 0; i < 100001; i++) ctx.get(collection("x"), "y"); } catch { return 1; } } },
    { name: "cells", compute: (ctx) => { try { for (let i = 0; i < 10001; i++) ctx.get(derived("leaf"), i); } catch { return 1; } } },
    { name: "depth", compute: (ctx, n = 0) => ctx.get(derived("depth"), (n ?? 0) + 1) },
    { name: "output", compute: () => "x".repeat(16 * 1024 * 1024) },
    { name: "host", compute: () => { throw Object.assign(new Error("host interrupted execution"), { code: "EVALUATION_BUDGET" }); } },
  ];
  for (const scenario of scenarios) {
    const db = harness({ [scenario.name]: scenario.compute, leaf: (_ctx, n) => n });
    assert.throws(() => db.mutate({ materialize: [{ name: scenario.name }] }), (error) => error.code === "EVALUATION_BUDGET", scenario.name);
    assert.deepEqual(Object.keys(db.data), []);
  }
});

test("incremental results match a from-scratch graph across randomized mutations", () => {
  let seed = 0x193abeef;
  const random = (max) => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % max; };
  let version = 0;
  const definitions = {
    selected: (ctx, name) => {
      const choice = ctx.get(collection("selectors"), name) ?? "k0";
      return ctx.get(collection("values"), choice) ?? 0;
    },
    subtotal: (ctx, owner) => ctx.query({ kind: "query", collection: "items", fields: ["owner"], value: owner }).reduce((sum, row) => sum + row.amount, 0),
    report: (ctx, name) => ({
      selected: ctx.get(derived("selected"), name) * (version + 1),
      subtotal: ctx.get(derived("subtotal"), name),
      count: ctx.scan(collection("values")).length,
    }),
  };
  const db = harness(definitions);
  db.mutate({ materialize: [{ name: "report", args: "alice" }, { name: "report", args: "bob" }] });
  for (let step = 0; step < 250; step++) {
    const writes = [];
    const command = { writes };
    for (let count = 1 + random(3); count > 0; count--) {
      const kind = random(4);
      const key = `k${random(8)}`;
      if (kind === 0) writes.push({ collection: "values", key, value: random(100) });
      if (kind === 1) writes.push({ collection: "values", key, delete: true });
      if (kind === 2) writes.push({ collection: "selectors", key: random(2) ? "alice" : "bob", value: key });
      if (kind === 3) writes.push(random(4) ? { collection: "items", key, value: { owner: random(2) ? "alice" : "bob", amount: random(20) } } : { collection: "items", key, delete: true });
    }
    if (step % 23 === 0) {
      version++;
      command.bundle = { hash: `v${version}`, javascript: `version=${version}` };
    }
    if (step % 11 === 0) command.materialize = [{ name: "subtotal", args: "alice" }];
    if (step % 17 === 0) command.unmaterialize = [{ name: "subtotal", args: "alice" }];
    db.mutate(command);
    const sourcesAndRoots = Object.fromEntries(Object.entries(db.data).filter(([key]) => !key.startsWith("cell:")));
    const scratch = harness(definitions, sourcesAndRoots);
    scratch.mutate();
    assert.deepEqual(plain(db.data), plain(scratch.data), `step ${step}, seed ${seed}`);
  }
});

function methodHarness(methods, definitions = {}, initial = {}, trusted = false) {
  let data = Object.assign(Object.create(null), plain(initial));
  let sequence = 0;
  return {
    get data() { return data; },
    call(kind, name, args = null, now) {
      const before = JSON.stringify(data);
      let result;
      try {
        const execute = trusted === "ordered" ? engineContext.flowerInvokeOrdered : trusted ? engineContext.flowerInvokeTrusted : invoke;
        const ordered = (value) => value === null || typeof value !== "object" ? value : Array.isArray(value)
          ? value.map(ordered) : Object.fromEntries(Object.keys(value).sort().map((key) => [key, ordered(value[key])]));
        result = plain(execute(trusted === "ordered" ? ordered(data) : data, { kind, name, args, requestId: `m${sequence++}` },
          (method, methodArgs, ctx) => {
            if (!Object.hasOwn(methods, method)) throw Object.assign(new Error("Unknown method"), { code: "METHOD_MISSING" });
            return methods[method](ctx, methodArgs);
          },
          (definition, cellArgs, ctx) => {
            if (!Object.hasOwn(definitions, definition)) throw Object.assign(new Error("Unknown derived value"), { code: "DEFINITION_MISSING" });
            return definitions[definition](ctx, cellArgs);
          }, now));
      } finally {
        assert.equal(JSON.stringify(data), before, "invocation never mutates its input");
      }
      data = Object.assign(Object.create(null), data, result.puts);
      for (const key of result.deletes) delete data[key];
      return result;
    },
  };
}

test("scan orders and constrains source keys before reverse, offset, and limit", () => {
  const rows = collection("rows");
  const keys = ["a", "aa", "b", "c", "🌺", "\ue000"];
  const db = methodHarness({ scan: (ctx, options) => ctx.scan(rows, options).map((row) => row.key) }, {},
    Object.fromEntries(keys.map((key) => [sourceKey("rows", key), key])));
  assert.deepEqual(db.call("query", "scan", {}).value, keys);
  assert.deepEqual(db.call("query", "scan", { gte: "aa", lte: "🌺", reverse: true, offset: 1, limit: 2 }).value, ["c", "b"]);
  assert.deepEqual(db.call("query", "scan", { gt: "aa", lt: "c" }).value, ["b"]);
  assert.deepEqual(db.call("query", "scan", { prefix: ["a"] }).value, ["a"]);
  assert.deepEqual(db.call("query", "scan", { prefix: [], reverse: true, offset: 4 }).value, ["aa", "a"]);
  assert.deepEqual(db.call("query", "scan", { limit: 0 }).value, []);
  assert.deepEqual(db.call("query", "scan", { offset: Number.MAX_SAFE_INTEGER }).value, []);
  assert.deepEqual(db.call("query", "scan", { gt: "c", lt: "a" }).value, []);
});

test("scan shares scalar tuple ordering and key tie breaks with ordered ranges", () => {
  const rows = { ...collection("rows"), indexes: { byValue: ["value"], byGroup: ["group", "value"] } };
  const values = [null, false, true, -7, 0, 1.5, "", "🌺", "\ue000"];
  const data = Object.fromEntries(values.map((value, i) => [sourceKey("rows", `value-${i}`), { group: "open", value }]));
  Object.assign(data, {
    [sourceKey("rows", "🌺")]: { group: "open", value: 1.5 },
    [sourceKey("rows", "\ue000")]: { group: "open", value: 1.5 },
    [sourceKey("rows", "closed")]: { group: "closed", value: 0 },
    [sourceKey("rows", "missing")]: { group: "open" },
    [sourceKey("rows", "object")]: { group: "open", value: {} },
    [sourceKey("rows", "array")]: { group: "open", value: [] },
    [sourceKey("rows", "scalar")]: 1,
  });
  const db = methodHarness({ scan: (ctx, options) => ctx.scan(rows, options).map((row) => row.key) }, {}, data);
  assert.deepEqual(db.call("query", "scan", { index: "byGroup", prefix: ["open"] }).value,
    ["value-0", "value-1", "value-2", "value-3", "value-4", "value-5", "🌺", "\ue000", "value-6", "value-7", "value-8"]);
  assert.deepEqual(db.call("query", "scan", { index: "byGroup", prefix: ["open"], gte: 0, lte: 1.5, reverse: true, offset: 1, limit: 2 }).value,
    ["🌺", "value-5"]);
  assert.deepEqual(db.call("query", "scan", { index: "byGroup", prefix: ["open", 1.5] }).value, ["value-5", "🌺", "\ue000"]);
  assert.deepEqual(db.call("query", "scan", { index: "byValue", gt: false, lt: 0 }).value, ["value-2", "value-3"]);
  assert.deepEqual(db.call("query", "scan", { index: "byValue", lte: null }).value, ["value-0"]);
});

test("scan options survive derived method previews and react to staged index changes", () => {
  const rows = { ...collection("rows"), indexes: { byScore: ["score"] } };
  const options = { index: "byScore", gte: 0, reverse: true, offset: 1, limit: 1 };
  const definitions = { page: (ctx) => ctx.scan(rows, options) };
  const initial = harness(definitions);
  initial.mutate({
    writes: [
      { collection: "rows", key: "a", value: { score: 1 } },
      { collection: "rows", key: "b", value: { score: 2 } },
      { collection: "rows", key: "c", value: { score: 3 } },
    ],
    materialize: [{ name: "page" }],
  });
  assert.deepEqual(initial.value("page"), [{ key: "b", value: { score: 2 } }]);
  const db = methodHarness({ update: (ctx) => {
    ctx.set(rows, "a", { score: 4 });
    const first = ctx.get(derived("page"));
    first[0].value.score = 100;
    ctx.delete(rows, "c");
    return { after: ctx.get(derived("page")), direct: ctx.scan(rows, options) };
  } }, definitions, initial.data);
  const result = db.call("mutation", "update");
  assert.deepEqual(result.value, { after: [{ key: "b", value: { score: 2 } }], direct: [{ key: "b", value: { score: 2 } }] });
  assert.deepEqual(db.data[cellKey("page")].outcome.value, result.value.after);
  assert.equal(db.data[sourceKey("rows", "b")].score, 2);
});

test("scan rejects invalid options and index descriptors without invoking accessors", () => {
  const rows = { ...collection("rows"), indexes: { byScore: ["score"] } };
  const invalid = [null, [], 1, { unknown: true }, { limit: -1 }, { limit: 0.5 }, { limit: Number.MAX_SAFE_INTEGER + 1 },
    { offset: -1 }, { offset: 0.5 }, { reverse: 1 }, { index: "missing" }, { index: "" }, { index: null },
    { prefix: null }, { prefix: ["a", "b"] }, { prefix: ["a"], gt: "a" }, { prefix: [0] }, { gt: 0 },
    { gt: "a", gte: "b" }, { lt: "a", lte: "b" }, { index: "byScore", gt: {} },
    { index: "byScore", prefix: [1], lt: 2 }];
  for (const options of invalid) {
    const db = methodHarness({ scan: (ctx) => ctx.scan(rows, options) });
    assert.throws(() => db.call("query", "scan"), (error) => error.code === "INVALID_REFERENCE", JSON.stringify(options));
  }
  let accessed = false;
  const accessor = { get limit() { accessed = true; return 1; } };
  const indexAccessor = { ...collection("rows"), get indexes() { accessed = true; return { byScore: ["score"] }; } };
  const prefixAccessor = [];
  Object.defineProperty(prefixAccessor, "0", { get() { accessed = true; return 0; }, enumerable: true });
  for (const [ref, options] of [[rows, accessor], [indexAccessor, { index: "byScore" }], [rows, { index: "byScore", prefix: prefixAccessor }]]) {
    const db = methodHarness({ scan: (ctx) => ctx.scan(ref, options) });
    assert.throws(() => db.call("query", "scan"), (error) => error.code === "INVALID_REFERENCE");
  }
  assert.equal(accessed, false);
  const db = methodHarness({ scan: (ctx) => ctx.scan(indexAccessor, undefined) });
  assert.deepEqual(db.call("query", "scan").value, []);
  assert.equal(accessed, false);
});

test("query methods read source and ephemeral derived values without durable changes", () => {
  const db = methodHarness({
    read: (ctx, key) => ({ source: ctx.get(collection("numbers"), key), double: ctx.get(derived("double"), key) }),
  }, { double: (ctx, key) => (ctx.get(collection("numbers"), key) ?? 0) * 2 }, {
    [sourceKey("numbers", "x")]: 4,
  });
  const result = db.call("query", "read", "x");
  assert.deepEqual(result.value, { source: 4, double: 8 });
  assert.deepEqual(result.puts, {});
  assert.deepEqual(result.deletes, []);
  assert.deepEqual(result.evaluated, [cellKey("double", "x")]);
  assert.deepEqual(Object.keys(db.data), [sourceKey("numbers", "x")]);
});

test("mutation methods read their source and derived writes and publish one final transition", () => {
  const db = methodHarness({
    create: (ctx) => {
      ctx.set(collection("numbers"), "x", 1);
      ctx.materialize(derived("double"), "x");
      return ctx.get(derived("double"), "x");
    },
    update: (ctx) => {
      ctx.set(collection("numbers"), "x", 3);
      const first = ctx.get(derived("double"), "x");
      ctx.set(collection("numbers"), "x", 7);
      const second = ctx.get(derived("double"), "x");
      return [first, second, ctx.get(collection("numbers"), "x")];
    },
  }, { double: (ctx, key) => ctx.get(collection("numbers"), key) * 2 });
  assert.equal(db.call("mutation", "create").value, 2);
  const result = db.call("mutation", "update");
  assert.deepEqual(result.value, [6, 14, 7]);
  assert.equal(db.data[sourceKey("numbers", "x")], 7);
  assert.equal(db.data[cellKey("double", "x")].outcome.value, 14);
  assert.equal(result.evaluated.length, 2);
});

test("writes after the last derived read are recomputed before committing", () => {
  const db = methodHarness({
    update: (ctx) => {
      ctx.materialize(derived("double"));
      ctx.set(collection("numbers"), "x", 3);
      const before = ctx.get(derived("double"));
      ctx.set(collection("numbers"), "x", 9);
      return before;
    },
  }, { double: (ctx) => ctx.get(collection("numbers"), "x") * 2 });
  const result = db.call("mutation", "update");
  assert.equal(result.value, 6, "method returns the value it explicitly read");
  assert.equal(db.data[cellKey("double")].outcome.value, 18, "durable derived state reflects the last write");
  assert.equal(result.evaluated.length, 2);
});

test("source-only methods lazily avoid derived work and scan their staged writes", () => {
  let calls = 0;
  const db = methodHarness({
    write: (ctx) => {
      ctx.set(collection("items"), "z", { status: "open" });
      ctx.set(collection("items"), "a", { status: "open" });
      ctx.delete(collection("items"), "z");
      return {
        missing: ctx.get(collection("items"), "z"),
        scan: ctx.scan(collection("items")),
        query: ctx.query({ kind: "query", collection: "items", fields: ["status"], value: "open" }),
      };
    },
  }, { unused: () => { calls++; return 1; } });
  const result = db.call("mutation", "write");
  assert.deepEqual(result.value, { missing: null, scan: [{ key: "a", value: { status: "open" } }], query: [{ status: "open" }] });
  assert.deepEqual(result.evaluated, []);
  assert.equal(calls, 0);
});

test("temporary derived read roots and dependencies are collected from final mutations", () => {
  const db = methodHarness({
    temporary: (ctx) => { ctx.set(collection("n"), "x", 5); return ctx.get(derived("parent")); },
    keep: (ctx) => { ctx.materialize(derived("parent")); return ctx.get(derived("parent")); },
    remove: (ctx) => { ctx.unmaterialize(derived("parent")); return ctx.get(derived("parent")); },
  }, {
    child: (ctx) => ctx.get(collection("n"), "x"),
    parent: (ctx) => ctx.get(derived("child")) + 1,
  });
  assert.equal(db.call("mutation", "temporary").value, 6);
  assert.deepEqual(Object.keys(db.data), [sourceKey("n", "x")]);
  db.call("mutation", "keep");
  assert.ok(db.data[cellKey("parent")]);
  assert.ok(db.data[cellKey("child")]);
  assert.equal(db.call("mutation", "remove").value, 6);
  assert.deepEqual(Object.keys(db.data), [sourceKey("n", "x")]);
});

test("an old ephemeral read is not reevaluated after a write when another value is read", () => {
  const db = methodHarness({
    update: (ctx) => {
      const before = ctx.get(derived("conditional"));
      ctx.set(collection("config"), "cycle", true);
      return [before, ctx.get(derived("other"))];
    },
  }, {
    conditional: (ctx) => ctx.get(collection("config"), "cycle") ? ctx.get(derived("conditional")) : 1,
    other: () => 2,
  });
  assert.deepEqual(db.call("mutation", "update").value, [1, 2]);
  assert.deepEqual(Object.keys(db.data), [sourceKey("config", "cycle")]);
});

test("query write attempts are fatal even when the method catches them", () => {
  for (const operation of [
    (ctx) => ctx.set(collection("x"), "y", 1),
    (ctx) => ctx.delete(collection("x"), "y"),
    (ctx) => ctx.materialize(derived("value")),
    (ctx) => ctx.unmaterialize(derived("value")),
  ]) {
    const db = methodHarness({ bad: (ctx) => { try { operation(ctx); } catch { return "caught"; } } });
    assert.throws(() => db.call("query", "bad"), (error) => error.code === "QUERY_WRITE_FORBIDDEN");
    assert.deepEqual(Object.keys(db.data), []);
  }
});

test("failed or invalid method results abort every staged operation", () => {
  for (const bad of ["throw", "undefined", "nan"]) {
    const db = methodHarness({ bad: (ctx) => {
      ctx.set(collection("x"), "y", 1);
      ctx.materialize(derived("value"));
      ctx.get(derived("value"));
      if (bad === "throw") throw new Error("method failed");
      return bad === "nan" ? NaN : undefined;
    } }, { value: () => 9 });
    assert.throws(() => db.call("mutation", "bad"));
    assert.deepEqual(Object.keys(db.data), []);
  }
  const db = methodHarness({});
  assert.throws(() => db.call("mutation", "absent"), (error) => error.code === "METHOD_MISSING");
});

test("methods may recover from ordinary derived errors but cannot swallow cycles", () => {
  const db = methodHarness({
    recover: (ctx) => { try { return ctx.get(derived("broken")); } catch { return "fallback"; } },
    cycle: (ctx) => { try { return ctx.get(derived("cycle")); } catch { return "caught"; } },
  }, {
    broken: () => { throw new Error("not ready"); },
    cycle: (ctx) => ctx.get(derived("cycle")),
  });
  assert.equal(db.call("query", "recover").value, "fallback");
  assert.throws(() => db.call("mutation", "cycle"), (error) => error.code === "CYCLE");
  assert.deepEqual(Object.keys(db.data), []);
});

test("method operation and total cell-evaluation budgets span all previews", () => {
  const db = methodHarness({
    operations: (ctx) => { try { for (let i = 0; i < 100001; i++) ctx.get(collection("n"), "x"); } catch { return 1; } },
    previews: (ctx) => {
      try {
        for (let i = 0; i < 10001; i++) {
          ctx.set(collection("n"), "x", i);
          ctx.get(derived("value"));
        }
      } catch { return 1; }
      return 2;
    },
  }, { value: (ctx) => ctx.get(collection("n"), "x") });
  assert.throws(() => db.call("query", "operations"), (error) => error.code === "EVALUATION_BUDGET");
  assert.throws(() => db.call("mutation", "previews"), (error) => error.code === "EVALUATION_BUDGET");
  assert.deepEqual(Object.keys(db.data), []);
});

test("method source values are copied and contexts expire", () => {
  let escaped;
  const db = methodHarness({
    write: (ctx) => {
      escaped = ctx;
      const value = { n: 1 };
      ctx.set(collection("data"), "x", value);
      value.n = 5;
      const read = ctx.get(collection("data"), "x");
      read.n = 6;
      return ctx.get(collection("data"), "x");
    },
  });
  assert.deepEqual(db.call("mutation", "write").value, { n: 1 });
  assert.deepEqual(db.data[sourceKey("data", "x")], { n: 1 });
  assert.throws(() => escaped.set(collection("data"), "x", 2), (error) => error.code === "INVALID_CONTEXT");
});

test("method previews preserve canonical key order and isolate nested JSON copies", () => {
  const record = JSON.parse('{"z":1,"toJSON":"ordinary data","10":"ten","2":"two","__proto__":{"safe":true},"a":{"nested":[1,2]}}');
  const db = methodHarness({
    update: (ctx) => {
      ctx.set(collection("data"), "x", record);
      const first = ctx.get(derived("record"));
      const keys = Object.keys(first);
      first.a.nested.push(3);
      first.__proto__.safe = false;
      record.a.nested.push(4);
      ctx.set(collection("data"), "unrelated", 1);
      const second = ctx.get(derived("record"));
      return { keys, second };
    },
  }, { record: (ctx) => ctx.get(collection("data"), "x") });
  const result = db.call("mutation", "update");
  assert.deepEqual(result.value.keys, ["2", "10", "__proto__", "a", "toJSON", "z"]);
  assert.deepEqual(result.value.second.a.nested, [1, 2]);
  assert.equal(result.value.second.__proto__.safe, true);
  assert.equal(result.value.second.toJSON, "ordinary data");
  assert.deepEqual(db.data[sourceKey("data", "x")], result.value.second);
  assert.equal(Object.hasOwn(db.data, cellKey("record")), false, "temporary preview is still collected");
});

test("public snapshots and staged writes still reject accessors before invoking them", () => {
  let getters = 0;
  let computes = 0;
  const invalid = Object.defineProperty({}, "n", { enumerable: true, get() { getters++; return 1; } });
  const callback = () => { computes++; return null; };
  const malformed = { [sourceKey("data", "x")]: invalid };
  assert.throws(() => evaluate(malformed, { requestId: "invalid" }, callback), (error) => error.code === "INPUT_INVALID");
  assert.throws(() => invoke(malformed, { kind: "query", name: "read" }, callback, callback), (error) => error.code === "INPUT_INVALID");
  const db = methodHarness({ write: (ctx) => { ctx.set(collection("data"), "x", invalid); return null; } });
  assert.throws(() => db.call("mutation", "write"), (error) => error.code === "INVALID_VALUE");
  assert.deepEqual(Object.keys(db.data), []);
  assert.equal(getters, 0);
  assert.equal(computes, 0);
});

test("staged values must satisfy the nesting limit including their transaction wrappers", () => {
  function nested(levels) {
    let value = 0;
    for (let level = 0; level < levels; level++) value = { n: value };
    return value;
  }
  const db = methodHarness({ write: (ctx, levels) => { ctx.set(collection("data"), "x", nested(levels)); return null; } });
  db.call("mutation", "write", 125);
  const before = plain(db.data);
  assert.throws(() => db.call("mutation", "write", 126), (error) => error.code === "INPUT_INVALID");
  assert.deepEqual(plain(db.data), before, "a deeply nested staged write must not poison the next snapshot");
});

test("trusted host snapshots preserve JavaScript canonical key order across all read paths", () => {
  // Rust's map order places U+E000 before U+1F600; JavaScript UTF-16 ordering
  // reverses them. Trusting JSON structure must not change observable key order.
  const record = JSON.parse('{"a":0,"match":true,"\ue000":1,"\ud83d\ude00":2}');
  const data = { [sourceKey("records", "x")]: record };
  const methods = { read: (ctx) => [
    Object.keys(ctx.get(collection("records"), "x")),
    Object.keys(ctx.scan(collection("records"))[0].value),
    Object.keys(ctx.query({ kind: "query", collection: "records", fields: ["match"], value: true })[0]),
    Object.keys(ctx.get(derived("record"))),
  ] };
  const definitions = { record: (ctx) => ctx.get(collection("records"), "x") };
  const expected = methodHarness(methods, definitions, data).call("query", "read", null, 100);
  const actual = methodHarness(methods, definitions, data, true).call("query", "read", null, 100);
  assert.deepEqual(actual, { ...expected, query_cacheable: true });
  assert.deepEqual(methodHarness(methods, definitions, data, "ordered").call("query", "read", null, 100), actual);
  assert.deepEqual(actual.value[0], ["a", "match", "\ud83d\ude00", "\ue000"]);
});

test("trusted graph previews retain staged-write isolation, rollback, and exact final patches", () => {
  const methods = { update: (ctx, args) => {
    const value = { amount: args };
    ctx.set(collection("rows"), "x", value);
    value.amount = 999;
    ctx.materialize(derived("double"));
    const before = ctx.get(derived("double"));
    ctx.set(collection("rows"), "x", { amount: args + 1 });
    return [before, ctx.get(derived("double"))];
  }, fail: (ctx) => {
    ctx.set(collection("rows"), "x", { amount: 123 });
    ctx.get(derived("double"));
    throw new Error("rollback");
  } };
  const definitions = { double: (ctx) => (ctx.get(collection("rows"), "x")?.amount ?? 0) * 2 };
  const strict = methodHarness(methods, definitions);
  const trusted = methodHarness(methods, definitions, {}, true);
  for (let index = 0; index < 30; index++) {
    assert.deepEqual(trusted.call("mutation", "update", index, 100 + index), { ...strict.call("mutation", "update", index, 100 + index), query_cacheable: false });
    assert.deepEqual(plain(trusted.data), plain(strict.data));
  }
  const before = plain(trusted.data);
  assert.throws(() => trusted.call("mutation", "fail", null, 200), /rollback/);
  assert.deepEqual(plain(trusted.data), before);
});

test("preview change tracking discards restored writes and roots after intermediate collection", () => {
  const rows = collection("rows");
  const definitions = {
    total: (ctx) => ctx.scan(rows).reduce((sum, row) => sum + row.value, 0),
    twice: (ctx) => 2 * ctx.get(derived("total")),
    temporary: (ctx) => ctx.get(rows, "temporary"),
  };
  const initial = harness(definitions);
  initial.mutate({ now: 100, writes: [{ collection: "rows", key: "a", value: 2 }], materialize: [{ name: "twice" }] });
  const methods = {
    restore: (ctx) => {
      const observations = [];
      ctx.set(rows, "a", 8);
      observations.push(ctx.get(derived("twice")));
      ctx.delete(rows, "a");
      observations.push(ctx.get(derived("twice")));
      ctx.set(rows, "a", 2);
      observations.push(ctx.get(derived("twice")));
      ctx.set(rows, "temporary", 9);
      observations.push(ctx.get(derived("temporary")));
      ctx.delete(rows, "temporary");
      ctx.unmaterialize(derived("twice"));
      observations.push(ctx.get(derived("total")));
      ctx.materialize(derived("twice"));
      observations.push(ctx.get(derived("twice")));
      return observations;
    },
  };
  for (const trusted of [false, true]) {
    const db = methodHarness(methods, definitions, initial.data, trusted);
    const before = plain(db.data);
    const result = db.call("mutation", "restore", null, 100);
    assert.deepEqual(result.value, [16, 0, 4, 9, 2, 4]);
    assert.deepEqual(result.puts, {}, "restoring the base value produces no final put");
    assert.deepEqual(result.deletes, [], "removed then restored roots/cells remain retained");
    assert.ok(result.evaluated.includes(cellKey("temporary")));
    assert.deepEqual(plain(db.data), before, "temporary source/root/cell changes leave no trace");
  }
});

test("deferred collection ordering preserves scans, query copies, and staged-write invalidation", () => {
  const rows = collection("rows");
  const keys = ["\ue000", "2", "😀", "__proto__", "10", "a"];
  const initial = Object.fromEntries(keys.map((key) => [sourceKey("rows", key), { key, included: true }]));
  initial[sourceKey("unread", "z")] = { unused: true };
  const definitions = {
    ordered: (ctx) => {
      const first = ctx.scan(rows);
      first[0].value.key = "changed returned copy";
      return ctx.query({ kind: "query", collection: "rows", fields: ["included"], value: true }).map((value) => value.key);
    },
  };
  const methods = {
    update: (ctx) => {
      ctx.materialize(derived("ordered"));
      const before = ctx.get(derived("ordered"));
      ctx.delete(rows, "2");
      ctx.set(rows, "b", { key: "b", included: true });
      const after = ctx.get(derived("ordered"));
      return { before, after, scanned: ctx.scan(rows).map((row) => row.key) };
    },
  };
  for (const trusted of [false, true]) {
    const db = methodHarness(methods, definitions, initial, trusted);
    const result = db.call("mutation", "update");
    assert.deepEqual(result.value.before, ["10", "2", "__proto__", "a", "😀", "\ue000"]);
    assert.deepEqual(result.value.after, ["10", "__proto__", "a", "b", "😀", "\ue000"]);
    assert.deepEqual(result.value.scanned, result.value.after);
    assert.deepEqual(db.data[cellKey("ordered")].deps, ['collection:"rows"', 'index-bucket:["rows",["included"]]:true']);
    assert.equal(db.data[sourceKey("rows", "10")].key, "10");
  }
});

test("point-only previews still reject malformed sources before evaluating derived code", () => {
  for (const trusted of [false, true]) {
    let computed = false;
    const db = methodHarness({ update: (ctx) => {
      ctx.set(collection("rows"), "a", 1);
      ctx.materialize(derived("point"));
      return null;
    } }, { point: (ctx) => { computed = true; return ctx.get(collection("rows"), "a"); } }, {
      'source:["malformed"]': 7,
    }, trusted);
    assert.throws(() => db.call("mutation", "update"), (error) => error.code === "INPUT_INVALID");
    assert.equal(computed, false);
    assert.deepEqual(plain(db.data), { 'source:["malformed"]': 7 });
  }
});

test("trusted previews enforce wrapper depth without rejecting transient query values", () => {
  const methods = {
    retain: (ctx, levels) => { ctx.materialize(derived("deep"), levels); return null; },
    once: (ctx, levels) => { ctx.get(derived("deep"), levels); return null; },
    twice: (ctx, levels) => { ctx.get(derived("deep"), levels); ctx.get(derived("other")); return null; },
    write: (ctx, levels) => { ctx.set(collection("records"), "x", nested(levels)); return null; },
  };
  function nested(levels) { let value = 0; for (let level = 0; level < levels; level++) value = { n: value }; return value; }
  const definitions = { deep: (_ctx, levels) => nested(levels), other: () => 0 };
  assert.equal(methodHarness(methods, definitions, {}, true).call("query", "once", 128).value, null);
  for (const trusted of [false, true]) {
    assert.throws(() => methodHarness(methods, definitions, {}, trusted).call("query", "twice", 128), (error) => error.code === "INPUT_INVALID");
    assert.throws(() => methodHarness(methods, definitions, {}, trusted).call("mutation", "write", 126), (error) => error.code === "INPUT_INVALID");
  }
  const host = methodHarness(methods, definitions, {}, true);
  host.call("mutation", "retain", 123);
  const before = plain(host.data);
  assert.throws(() => host.call("mutation", "retain", 124), (error) => error.code === "INPUT_INVALID");
  assert.deepEqual(plain(host.data), before);
});

test("trusted query memoization metadata excludes direct, computed, and cached clock reads", () => {
  const definitions = { timed: (ctx) => ctx.now(), fixed: () => 7 };
  const methods = {
    fixed: (ctx) => ctx.get(derived("fixed")),
    direct: (ctx) => ctx.now(),
    timed: (ctx) => ctx.get(derived("timed")),
  };
  const db = methodHarness(methods, definitions, {}, true);
  assert.equal(db.call("query", "fixed", null, 100).query_cacheable, true);
  assert.equal(db.call("query", "direct", null, 100).query_cacheable, false);
  assert.equal(db.call("query", "timed", null, 100).query_cacheable, false);
  const initial = harness(definitions);
  initial.mutate({ now: 100, materialize: [{ name: "timed" }] });
  const cached = methodHarness(methods, definitions, initial.data, true);
  assert.equal(cached.call("query", "timed", null, 100).query_cacheable, false);
  assert.equal(cached.call("query", "fixed", null, 200).query_cacheable, false, "any stored clock dependency conservatively disables caching");
  assert.equal(db.call("mutation", "fixed", null, 100).query_cacheable, false);
  assert.equal(Object.hasOwn(methodHarness(methods, definitions).call("query", "fixed"), "query_cacheable"), false);
});

test("randomized method writes and derived previews match recomputation from final sources", () => {
  let seed = 0x1456abe3;
  const random = (max) => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % max; };
  const definitions = {
    sum: (ctx) => ctx.scan(collection("numbers")).reduce((sum, row) => sum + row.value, 0),
    twice: (ctx) => ctx.get(derived("sum")) * 2,
  };
  const methods = {
    apply: (ctx, operations) => {
      ctx.materialize(derived("twice"));
      const seen = [];
      for (const operation of operations) {
        if (operation.remove) ctx.delete(collection("numbers"), operation.key);
        else ctx.set(collection("numbers"), operation.key, operation.value);
        if (operation.read) seen.push(ctx.get(derived("twice")));
      }
      return seen;
    },
  };
  const db = methodHarness(methods, definitions);
  const trusted = methodHarness(methods, definitions, {}, true);
  const ordered = methodHarness(methods, definitions, {}, "ordered");
  const expected = new Map();
  for (let step = 0; step < 80; step++) {
    const operations = [];
    const seen = [];
    for (let count = 1 + random(5); count > 0; count--) {
      const operation = { key: `k${random(5)}`, value: random(40), remove: random(4) === 0, read: random(2) === 0 };
      operations.push(operation);
      if (operation.remove) expected.delete(operation.key);
      else expected.set(operation.key, operation.value);
      if (operation.read) seen.push([...expected.values()].reduce((a, b) => a + b, 0) * 2);
    }
    const result = db.call("mutation", "apply", operations);
    assert.deepEqual(result.value, seen);
    assert.deepEqual(trusted.call("mutation", "apply", operations), { ...result, query_cacheable: false });
    assert.deepEqual(plain(trusted.data), plain(db.data));
    assert.deepEqual(ordered.call("mutation", "apply", operations), { ...result, query_cacheable: false });
    assert.deepEqual(plain(ordered.data), plain(db.data));
    const scratch = harness(definitions, Object.fromEntries(Object.entries(db.data).filter(([key]) => !key.startsWith("cell:"))));
    scratch.mutate();
    assert.deepEqual(plain(db.data), plain(scratch.data), `step ${step}`);
  }
});

test("clock changes invalidate time-dependent cells and their downstream graph", () => {
  const db = harness({
    remaining: (ctx) => ctx.get(collection("leases"), "expiresAt") - ctx.now(),
    active: (ctx) => ctx.get(derived("remaining")) > 0,
    stable: (ctx) => ctx.get(collection("leases"), "expiresAt"),
  });
  db.mutate({ now: 100, writes: [{ collection: "leases", key: "expiresAt", value: 150 }],
    materialize: [{ name: "active" }, { name: "stable" }] });
  assert.equal(db.data.clock, 100);
  assert.equal(db.value("remaining"), 50);
  assert.equal(db.value("active"), true);
  assert.deepEqual(db.data[cellKey("remaining")].deps, ["clock", sourceKey("leases", "expiresAt")]);
  const result = db.mutate({ now: 160 });
  assert.equal(db.data.clock, 160);
  assert.equal(db.value("remaining"), -10);
  assert.equal(db.value("active"), false);
  assert.deepEqual(result.evaluated.sort(), [cellKey("active"), cellKey("remaining")].sort());
  assert.deepEqual(db.mutate({ now: 160 }).evaluated, []);
});

test("query methods recompute materialized time-dependent values at a later temporary time", () => {
  const definitions = {
    remaining: (ctx) => ctx.get(collection("leases"), "expiresAt") - ctx.now(),
    active: (ctx) => ctx.get(derived("remaining")) > 0,
  };
  const initial = harness(definitions);
  initial.mutate({ now: 100, writes: [{ collection: "leases", key: "expiresAt", value: 150 }], materialize: [{ name: "active" }] });
  const db = methodHarness({
    inspect: (ctx) => ({ now: ctx.now(), remaining: ctx.get(derived("remaining")), active: ctx.get(derived("active")) }),
  }, definitions, initial.data);
  const before = plain(db.data);
  const result = db.call("query", "inspect", null, 160);
  assert.deepEqual(result.value, { now: 160, remaining: -10, active: false });
  assert.deepEqual(result.puts, {});
  assert.deepEqual(result.deletes, []);
  assert.equal(result.evaluated.length, 2);
  assert.deepEqual(plain(db.data), before);
  assert.equal(db.data.clock, 100);
  assert.equal(db.data[cellKey("active")].outcome.value, true);
});

test("one invocation shares a fixed clock across method reads and multiple derived previews", () => {
  const db = methodHarness({
    renew: (ctx) => {
      const start = ctx.now();
      ctx.materialize(derived("active"));
      ctx.set(collection("leases"), "expiresAt", ctx.now() + 20);
      const first = ctx.get(derived("active"));
      ctx.set(collection("leases"), "expiresAt", ctx.now() - 1);
      const second = ctx.get(derived("active"));
      return { times: [start, ctx.now()], first, second };
    },
  }, { active: (ctx) => ctx.get(collection("leases"), "expiresAt") > ctx.now() });
  const result = db.call("mutation", "renew", null, 250);
  assert.deepEqual(result.value, { times: [250, 250], first: true, second: false });
  assert.equal(db.data.clock, 250);
  assert.equal(db.data[sourceKey("leases", "expiresAt")], 249);
  assert.equal(db.data[cellKey("active")].outcome.value, false);
  assert.equal(result.evaluated.length, 2);
});

test("a time-only mutation advances the clock and recomputes durable expiration state", () => {
  const definitions = { expired: (ctx) => ctx.now() >= 200 };
  const initial = harness(definitions);
  initial.mutate({ now: 100, materialize: [{ name: "expired" }] });
  const db = methodHarness({ tick: (ctx) => ctx.now() }, definitions, initial.data);
  const result = db.call("mutation", "tick", null, 200);
  assert.equal(result.value, 200);
  assert.equal(result.puts.clock, 200);
  assert.equal(db.data[cellKey("expired")].outcome.value, true);
  assert.deepEqual(result.evaluated, [cellKey("expired")]);
});

test("time-independent graphs retain their values while clock-only mutations advance time", () => {
  const definitions = { value: (ctx) => ctx.get(collection("numbers"), "x") };
  const initial = harness(definitions);
  initial.mutate({ now: 100, writes: [{ collection: "numbers", key: "x", value: 7 }], materialize: [{ name: "value" }] });
  const db = methodHarness({ read: (ctx) => [ctx.now(), ctx.get(derived("value"))] }, definitions, initial.data);
  const before = plain(db.data);
  assert.deepEqual(db.call("query", "read", null, 200).value, [200, 7]);
  assert.deepEqual(plain(db.data), before, "a query's temporary clock is not committed");
  const result = db.call("mutation", "read", null, 200);
  assert.deepEqual(result.value, [200, 7]);
  assert.deepEqual(result.puts, { clock: 200 });
  assert.deepEqual(result.evaluated, []);
});

test("a previously time-independent graph can start tracking time after a write or new root", () => {
  const flags = collection("flags");
  const definitions = { conditional: (ctx) => ctx.get(flags, "timed") ? ctx.now() : 0, newClock: (ctx) => ctx.now() };
  const initial = harness(definitions);
  initial.mutate({ now: 100, materialize: [{ name: "conditional" }] });
  const db = methodHarness({
    activate: (ctx) => {
      assert.equal(ctx.get(derived("conditional")), 0);
      ctx.set(flags, "timed", true);
      ctx.materialize(derived("newClock"));
      return [ctx.get(derived("conditional")), ctx.get(derived("newClock"))];
    },
    read: (ctx) => [ctx.get(derived("conditional")), ctx.get(derived("newClock"))],
  }, definitions, initial.data);
  assert.deepEqual(db.call("mutation", "activate", null, 200).value, [200, 200]);
  assert.deepEqual(db.call("query", "read", null, 300).value, [300, 300]);
  assert.equal(db.data.clock, 200);
});

test("clock-only refresh still validates all stored cells, source identities, and roots", () => {
  const good = { name: "value", args: null, outcome: { ok: true, value: 7 }, deps: [] };
  const cases = [
    { [cellKey("value")]: { ...good, deps: "bad" } },
    { [cellKey("value")]: { ...good, name: "different" } },
    { [cellKey("value")]: { ...good, outcome: { ok: true } } },
    { [cellKey("value")]: { ...good, outcome: { ok: false, error: null } } },
    { 'source:["bad"]': 1 },
    { 'root:["value",null]': { name: "different", args: null } },
  ];
  for (const data of cases) {
    const db = methodHarness({ tick: () => null }, {}, { clock: 100, ...data });
    assert.throws(() => db.call("mutation", "tick", null, 200), (error) => error.code === "INPUT_INVALID");
    assert.deepEqual(plain(db.data), { clock: 100, ...data });
  }
});

test("clock-only refresh preserves missing-cell evaluation, orphan collection, and cycle errors", () => {
  const good = { name: "value", args: null, outcome: { ok: true, value: 7 }, deps: [] };
  const orphan = methodHarness({ tick: () => null }, {}, { clock: 100, [cellKey("value")]: good });
  assert.deepEqual(orphan.call("mutation", "tick", null, 200).deletes, [cellKey("value")]);
  const missing = methodHarness({ tick: () => null }, { value: (ctx) => ctx.now() }, {
    clock: 100, 'root:["value",null]': { name: "value", args: null },
  });
  assert.deepEqual(missing.call("mutation", "tick", null, 200).evaluated, [cellKey("value")]);
  assert.equal(missing.data[cellKey("value")].outcome.value, 200);
  const cycle = methodHarness({ tick: () => null }, {}, {
    clock: 100, 'root:["value",null]': { name: "value", args: null },
    [cellKey("value")]: { ...good, deps: [cellKey("value")] },
  });
  assert.throws(() => cycle.call("mutation", "tick", null, 200), (error) => error.code === "CYCLE");
});

test("clock-only refresh retains computed errors and cannot bypass graph depth limits", () => {
  const definitions = { broken: () => { throw Object.assign(new Error("still broken"), { code: "BROKEN" }); } };
  const initial = harness(definitions);
  initial.mutate({ now: 100, materialize: [{ name: "broken" }] });
  const failed = methodHarness({ tick: () => null, read: (ctx) => ctx.get(derived("broken")) }, definitions, initial.data);
  assert.deepEqual(failed.call("mutation", "tick", null, 200).puts, { clock: 200 });
  assert.throws(() => failed.call("query", "read", null, 300), (error) => error.code === "BROKEN");
  const data = { clock: 100, 'root:["level-0",null]': { name: "level-0", args: null } };
  for (let level = 0; level < 129; level++) {
    data[cellKey(`level-${level}`)] = { name: `level-${level}`, args: null, outcome: { ok: true, value: level },
      deps: level === 128 ? [] : [cellKey(`level-${level + 1}`)] };
  }
  const deep = methodHarness({ tick: () => null }, {}, data);
  assert.throws(() => deep.call("mutation", "tick", null, 200), (error) => error.code === "EVALUATION_BUDGET");
  assert.equal(deep.data.clock, 100);
});

test("clock defaults remain stable and explicit times cannot move durable time backwards", () => {
  const defaults = harness({ now: (ctx) => ctx.now() });
  defaults.mutate({ materialize: [{ name: "now" }] });
  assert.equal(defaults.value("now"), 0);
  assert.equal(Object.hasOwn(defaults.data, "clock"), false);
  defaults.mutate({ now: 0 });
  assert.equal(defaults.data.clock, 0);
  defaults.mutate({ now: 300 });
  assert.equal(defaults.value("now"), 300);
  assert.deepEqual(defaults.mutate({ now: 100 }).puts, {});
  assert.equal(defaults.data.clock, 300);
  assert.deepEqual(defaults.mutate().evaluated, []);

  const db = methodHarness({ now: (ctx) => [ctx.now(), ctx.get(derived("now"))] }, { now: (ctx) => ctx.now() }, defaults.data);
  const clamped = db.call("mutation", "now", null, 50);
  assert.deepEqual(clamped.value, [300, 300]);
  assert.deepEqual(clamped.puts, {});
  assert.deepEqual(clamped.evaluated, []);
  assert.deepEqual(db.call("query", "now").value, [300, 300]);
});

test("method failures roll back time and intermediate time-dependent previews", () => {
  const definitions = { now: (ctx) => ctx.now() };
  const initial = harness(definitions);
  initial.mutate({ now: 100, materialize: [{ name: "now" }] });
  const db = methodHarness({ fail: (ctx) => {
    assert.equal(ctx.now(), 500);
    assert.equal(ctx.get(derived("now")), 500);
    ctx.set(collection("leases"), "expiresAt", ctx.now() + 10);
    throw new Error("renewal failed");
  } }, definitions, initial.data);
  const before = plain(db.data);
  assert.throws(() => db.call("mutation", "fail", null, 500), /renewal failed/);
  assert.deepEqual(plain(db.data), before);
});

test("invalid supplied or persisted times fail before running user code", () => {
  let called = false;
  const callback = () => { called = true; return null; };
  for (const now of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, Infinity, NaN, null, "100", {}, 1n]) {
    assert.throws(() => invoke({}, { kind: "query", name: "x" }, callback, callback, now), (error) => error.code === "INPUT_INVALID");
    assert.throws(() => evaluate({}, { requestId: "invalid-time", now }, callback), (error) => error.code === "INPUT_INVALID");
  }
  for (const clock of [-1, null, "100", 1.5]) {
    assert.throws(() => invoke({ clock }, { kind: "query", name: "x" }, callback, callback, 100), (error) => error.code === "INPUT_INVALID");
    assert.throws(() => evaluate({ clock }, { requestId: "invalid-clock" }, callback), (error) => error.code === "INPUT_INVALID");
  }
  assert.equal(called, false);
});

test("clock reads count toward method and derived budgets", () => {
  const db = methodHarness({ repeat: (ctx) => {
    try { for (let i = 0; i < 100001; i++) ctx.now(); } catch { return "caught"; }
    return null;
  } });
  assert.throws(() => db.call("query", "repeat", null, 100), (error) => error.code === "EVALUATION_BUDGET");
  const derivedDb = harness({ repeat: (ctx) => {
    try { for (let i = 0; i < 100001; i++) ctx.now(); } catch { return "caught"; }
    return null;
  } });
  assert.throws(() => derivedDb.mutate({ now: 100, materialize: [{ name: "repeat" }] }), (error) => error.code === "EVALUATION_BUDGET");
  assert.deepEqual(Object.keys(derivedDb.data), []);
});
