// Maintainer ABI check; production executes the same QuickJS guest in Wasmtime.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const module = new WebAssembly.Module(readFileSync(new URL("./quickjs.wasm", import.meta.url)));
const { exports: api } = new WebAssembly.Instance(module, {
  flower: {
    host_call() { throw new Error("Image preparation must not call the database"); },
    crypto_call() { throw new Error("Image preparation must not use cryptography"); },
  },
});
assert.equal(api.flower_init(), 0);
const encoder = new TextEncoder(), decoder = new TextDecoder("utf8", { fatal: true });
function result(packed) {
  packed = BigInt.asUintN(64, packed);
  const pointer = Number(packed & 0xffffffffn), length = Number((packed >> 32n) & 0x7fffffffn);
  const text = decoder.decode(new Uint8Array(api.memory.buffer, pointer, length));
  api.flower_free(pointer);
  assert.equal(packed >> 63n, 0n, text);
  return text;
}
function evaluate(source) {
  const bytes = encoder.encode(source + "\0"), pointer = api.flower_alloc(bytes.length);
  new Uint8Array(api.memory.buffer, pointer, bytes.length).set(bytes);
  const packed = api.flower_eval(pointer, bytes.length - 1);
  api.flower_free(pointer);
  return result(packed);
}
evaluate(`
  globalThis.keep = (() => { let count = 40; return () => ++count; })();
  globalThis.discarded = Array.from({length:1000}, () => { const value = {}; value.self = value; return value; });
  discarded = null;
  "ready";
`);
const prepared = JSON.parse(result(api.flower_snapshot_prepare()));
assert(prepared.objectsBefore - prepared.objectsAfter >= 1000, JSON.stringify(prepared));
assert(prepared.bytesAfter < prepared.bytesBefore);
assert.equal(prepared.thresholdAfter, prepared.bytesAfter + Math.floor(prepared.bytesAfter / 2));
assert(prepared.thresholdAfter < 0xffffffff, "Automatic GC stays enabled");
assert.equal(evaluate("keep()"), "41");
const repeated = JSON.parse(result(api.flower_snapshot_prepare()));
assert.equal(repeated.thresholdAfter, repeated.bytesAfter + Math.floor(repeated.bytesAfter / 2));
assert.equal(evaluate("keep()"), "42", "Preparation preserves live closures");
console.log(JSON.stringify({ prepared, repeated }));
