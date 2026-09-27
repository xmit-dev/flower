// Drives flower_invoke and flower.host_call with an independent JavaScript
// implementation of the value encoding. Production runs the same guest in
// Wasmtime; each case uses a fresh instance, as production does.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as wire from "./wire.mjs";

const module = new WebAssembly.Module(readFileSync(new URL("./quickjs.wasm", import.meta.url)));
const encoder = new TextEncoder(), decoder = new TextDecoder("utf-8", { fatal: true });
const describe = "(kind,e)=>[e&&typeof e.code==='string'?e.code:'COMPUTE_ERROR',String(e&&e.message||e),e&&e.details]";

function guest(runner, host = () => wire.success(null)) {
  let api;
  const calls = [];
  ({ exports: api } = new WebAssembly.Instance(module, { flower: {
    host_call(op, pointer, length) {
      const bytes = new Uint8Array(api.memory.buffer, pointer, length).slice();
      const args = [];
      for (let offset = 0; offset < bytes.length;) {
        const [value, next] = wire.decode(bytes, offset);
        args.push(value);
        offset = next;
      }
      calls.push([op, args]);
      const reply = host(op, args);
      const out = api.flower_alloc(reply.length);
      new Uint8Array(api.memory.buffer, out, reply.length).set(reply);
      return BigInt(out) | (BigInt(reply.length) << 32n);
    },
    crypto_call() { throw Error("unexpected crypto call"); },
  } }));
  assert.equal(api.flower_init(), 0);
  const input = (bytes) => {
    const pointer = api.flower_alloc(bytes.length + 1);
    new Uint8Array(api.memory.buffer, pointer, bytes.length).set(bytes);
    new Uint8Array(api.memory.buffer)[pointer + bytes.length] = 0;
    return [pointer, bytes.length];
  };
  const source = input(encoder.encode(`const host=__flowerHost;__flowerSetRunner(${runner},${describe},()=>({definitions:{},http:{}}));`));
  const packed = BigInt.asUintN(64, api.flower_eval(...source));
  const pointer = Number(packed & 0xffffffffn), length = Number((packed >> 32n) & 0x7fffffffn);
  assert.equal(packed >> 63n, 0n, decoder.decode(new Uint8Array(api.memory.buffer, pointer, length)));
  api.flower_free(pointer);
  return (args = null, name = "test", kind = 0) => {
    const [namePointer, nameLength] = input(encoder.encode(name));
    const [argsPointer, argsLength] = input(wire.encode(args));
    const packed = BigInt.asUintN(64, api.flower_invoke(kind, namePointer, nameLength, argsPointer, argsLength));
    const pointer = Number(packed & 0xffffffffn), length = Number(packed >> 32n);
    const bytes = new Uint8Array(api.memory.buffer, pointer, length).slice();
    return { bytes, outcome: wire.outcome(bytes), calls };
  };
}

const ok = (value) => ({ ok: true, value });
const failed = (code, message) => ({ ok: false, error: { code, message } });

// Arguments and results round trip, including every string representation.
const values = [
  null, true, false, 0, -1, 2 ** 31 - 1, 2 ** 31, -(2 ** 53 - 1), 1.5, 1e21,
  "", "ascii", "é Latin-1", "wide 🌸\0tail", "字".repeat(3000),
  [1, [2, [3]], { a: { b: [] } }], { z: 1, a: 2, "é": null, "😀": { "": 1 } },
  Array.from({ length: 600 }, (_, i) => ({ [`k${i}`]: i, shared: i })),
];
for (const value of values) assert.deepEqual(guest("(kind,name,args)=>args")(value).outcome, ok(value));
assert.deepEqual(guest("(kind,name,args)=>[kind,name]")(null, "名前", 3).outcome, ok([3, "名前"]));

// Strings leave in QuickJS's representation; keys repeat as references.
for (const [compute, tag] of [
  ["'plain'", wire.LATIN1], ["'é'.repeat(2)", wire.LATIN1], ["'🌸'", wire.UTF16],
  ["'a'.repeat(700)+'é'.repeat(700)", wire.LATIN1], ["'a'.repeat(700)+'🌸'.repeat(300)", wire.UTF16],
  ["'0123456789'.repeat(500).slice(3)", wire.LATIN1],
]) {
  const seen = { tags: [], references: 0 };
  const { bytes, outcome } = guest(`()=>${compute}`)();
  wire.decode(bytes, 1, seen);
  assert.deepEqual(seen.tags, [tag], compute);
  assert.equal(outcome.value, new Function(`return ${compute}`)());
}
const seen = { tags: [], references: 0 };
wire.decode(guest("()=>[{id:1,name:'a'},{id:2,name:'b'},{name:'c',id:3}]")().bytes, 1, seen);
assert.equal(seen.references, 4);

// Values are plain data, read without running application code.
for (const [compute, expected] of [
  ["()=>undefined", failed("INVALID_VALUE", "values must be null, booleans, finite numbers, strings, arrays or plain objects")],
  ["()=>NaN", failed("INVALID_VALUE", "numbers must be finite")],
  ["()=>({get x(){host(1);return 1}})", failed("INVALID_VALUE", "values cannot contain symbols, hidden properties or accessors")],
  ["()=>{const x={};x.x=x;return x}", failed("INVALID_VALUE", "values cannot contain cycles")],
  ["()=>[1,,3]", failed("INVALID_VALUE", "arrays cannot contain holes")],
  ["()=>Object.assign([1],{extra:2})", failed("INVALID_VALUE", "arrays cannot contain named properties")],
  ["()=>{throw Object.assign(Error('boom 🌸\\0'),{code:'CUSTOM'})}", failed("CUSTOM", "boom 🌸\0")],
  ["()=>{let sum=0;outer:for(let i=0;i<5;i++){for(let j=0;j<5;j++){if(i===3)break outer;if(j===2)continue;sum+=i*10+j}}return sum}", ok(144)],
  ["()=>[...function*(){try{yield 1;throw Error('caught')}catch(error){yield 2}finally{yield 3}}()]", ok([1, 2, 3])],
]) {
  const result = guest(compute)();
  assert.deepEqual(result.outcome, expected, compute);
  assert.equal(result.calls.length, 0, compute);
}
// The guest sends UTF-16 as stored; the host is the one to reject it.
assert.throws(() => guest("()=>'\\ud800'")(), /lone surrogate/);

// Host calls: arguments as consecutive values; replies decoded or thrown.
{
  const run = guest("()=>{const v=host(4,'items',{k:[1,'é']});return [v,Object.keys(v)]}",
    () => wire.success({ b: 1, a: [true], "10": 0, "2": 0 }));
  const { outcome, calls } = run();
  assert.deepEqual(calls, [[4, ["items", { k: [1, "é"] }]]]);
  assert.deepEqual(outcome, ok([{ b: 1, a: [true], "10": 0, "2": 0 }, ["2", "10", "a", "b"]]));
}
assert.deepEqual(
  guest("()=>{try{host(1)}catch(e){return [e.code,e.message,e instanceof Error]}}", () => wire.failure("NOPE", "no 🌸"))().outcome,
  ok(["NOPE", "no 🌸", true]),
);
assert.deepEqual(
  guest("()=>{try{host(1)}catch(e){return [e.name,e.message]}}", () => Uint8Array.of(0, 99))().outcome,
  ok(["TypeError", "malformed host value"]),
);
assert.deepEqual(
  guest("()=>{try{host(4,'x',()=>1)}catch(e){return [e.code,e.message]}}")().outcome,
  ok(["INVALID_VALUE", "values must be null, booleans, finite numbers, strings, arrays or plain objects"]),
);
console.log("Guest invocation: value round trips, string representations, key interning, validation, failures and host calls passed");
