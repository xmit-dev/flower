import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { transformSync } from "esbuild";
import { canonicalJson } from "./json.ts";

test("canonical JSON captures the native capability once and retains its full fallback", () => {
  const source = transformSync(readFileSync(new URL("./json.ts", import.meta.url), "utf8"), {
    loader: "ts", format: "cjs", target: "es2022",
  }).code;
  let probes = 0;
  const sandbox: Record<string, any> = {
    exports: {}, module: { exports: {} },
    globalThis: { __flowerCanonicalJson: () => "spoofed" },
    __flowerCanonicalJson(value: unknown) {
      ++probes;
      return value === null || typeof value === "string" ? JSON.stringify(value) : undefined;
    },
  };
  runInNewContext(source, sandbox);
  const encode = sandbox.module.exports.canonicalJson as typeof canonicalJson;
  sandbox.__flowerCanonicalJson = () => { throw Error("a later global assignment must not replace the captured helper"); };
  assert.equal(encode("flower"), '"flower"');
  assert.equal(encode(Object.assign(Object.create(null), { b: 2, a: 1 })), '{"a":1,"b":2}');
  assert.equal(encode(["tenant", "store"]), '["tenant","store"]');
  assert.throws(() => encode([undefined]), /must be JSON values/);
  assert.equal(probes, 4);
});

test("canonical primitive keys retain JSON escaping, number spelling, and depth", () => {
  for (const [value, expected] of [
    [null, "null"], [true, "true"], [-0, "0"], ["a\0b🌸", '"a\\u0000b🌸"'],
    [["tenant-a", "store-1"], '["tenant-a","store-1"]'],
    [[null, true, false, -0, 1e-7, 1e21, "\ud800", "\"\\"], '[null,true,false,0,1e-7,1e+21,"\\ud800","\\\"\\\\"]'],
    [Object.freeze(["a", "b"]), '["a","b"]'],
    [Object.seal(["a", "b"]), '["a","b"]'],
    [Object.setPrototypeOf(["a", "b"], null), '["a","b"]'],
  ] as const) {
    assert.equal(canonicalJson(value), expected);
  }
  const shared = { "\ue000": 1, "😀": 2, "10": 10, "2": 2 };
  assert.equal(canonicalJson([shared, shared]), '[{"10":10,"2":2,"😀":2,"":1},{"10":10,"2":2,"😀":2,"":1}]');
  let boundary: unknown = ["key"];
  for (let depth = 0; depth < 127; ++depth) boundary = [boundary];
  assert.doesNotThrow(() => canonicalJson(boundary));
  assert.throws(() => canonicalJson([boundary]), /nesting exceeds/);
});

test("array fast path rejects holes, exotic values, fields, and cycles without running getters", () => {
  let getters = 0;
  const accessor = Object.defineProperty(["a"], "0", { get() { ++getters; return "x"; } });
  const hidden = Object.defineProperty(["a"], "0", { enumerable: false });
  const cycle: unknown[] = [];
  cycle.push(cycle);
  for (const value of [
    [undefined], [NaN], [Infinity], [1n], [Symbol("x")], [() => 1],
    [, "b"], new Array(2), accessor, hidden,
    Object.assign(["a"], { other: "b" }), Object.assign(["a"], { [Symbol("x")]: "b" }),
    [new Map()], [new Uint8Array(2)], [Promise.resolve(1)], cycle,
  ]) {
    assert.throws(() => canonicalJson(value), TypeError);
  }
  assert.equal(getters, 0);
});

test("proxied arrays keep descriptor order, length observations, and index order", () => {
  for (const order of [["0", "1", "length"], ["1", "0", "length"]]) {
    const trace: string[] = [];
    const array = new Proxy(["a", "b"], {
      getPrototypeOf(target) { trace.push("prototype"); return Reflect.getPrototypeOf(target); },
      ownKeys() { trace.push("keys"); return order; },
      getOwnPropertyDescriptor(target, key) { trace.push(`descriptor:${String(key)}`); return Reflect.getOwnPropertyDescriptor(target, key); },
      get(target, key, receiver) { trace.push(`get:${String(key)}`); return Reflect.get(target, key, receiver); },
    });
    assert.equal(canonicalJson(array), '["a","b"]');
    assert.deepEqual(trace, ["prototype", "keys", `descriptor:${order[0]}`, "get:length", `descriptor:${order[1]}`, "get:length", "get:length", "get:length"]);
  }
  let reads = 0;
  const shrinking = new Proxy(["a", "b"], {
    get(target, key, receiver) { return key === "length" && ++reads === 4 ? 1 : Reflect.get(target, key, receiver); },
  });
  assert.equal(canonicalJson(shrinking), '["a"]');
  assert.equal(reads, 4);
});

test("array scratch storage never reads or invokes inherited numeric properties", () => {
  const previous = Object.getOwnPropertyDescriptor(Array.prototype, "0");
  let gets = 0;
  let sets = 0;
  let encoded: string;
  try {
    Object.defineProperty(Array.prototype, "0", {
      configurable: true,
      get() { ++gets; return "polluted"; },
      set() { ++sets; },
    });
    encoded = canonicalJson(["a", "b"]);
  } finally {
    if (previous) Object.defineProperty(Array.prototype, "0", previous);
    else delete (Array.prototype as unknown[])[0];
  }
  assert.equal(encoded!, '["a","b"]');
  assert.equal(gets, 0);
  assert.equal(sets, 0);
});
