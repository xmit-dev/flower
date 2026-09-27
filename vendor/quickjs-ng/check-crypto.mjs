#!/usr/bin/env node
// Tests the C/Wasm ABI and ownership, independently of Rust's crypto algorithms.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as wire from "./wire.mjs";

const module = new WebAssembly.Module(readFileSync(new URL("./quickjs.wasm", import.meta.url)));
const encoder = new TextEncoder(), decoder = new TextDecoder();
let api, hostCalls = 0;
const response = (out, kind, bytes, integer = 0) => {
  let pointer = 0;
  if (bytes !== undefined) {
    pointer = api.flower_alloc(bytes.length);
    new Uint8Array(api.memory.buffer, pointer, bytes.length).set(bytes);
  }
  // flower_alloc may grow memory; never retain pre-growth ArrayBuffer views.
  const view = new DataView(api.memory.buffer);
  view.setUint32(out, kind, true);
  view.setUint32(out + 4, pointer, true);
  view.setUint32(out + 8, bytes?.length ?? integer, true);
  return 0;
};
({ exports: api } = new WebAssembly.Instance(module, { flower: {
  host_call() { throw new Error("crypto ABI tests must not access database state"); },
  crypto_call(op, parameter, spans, count, out) {
    hostCalls++;
    const view = new DataView(api.memory.buffer);
    const parts = Array.from({ length: count }, (_, i) => {
      const pointer = view.getUint32(spans + i * 8, true), length = view.getUint32(spans + i * 8 + 4, true);
      return new Uint8Array(api.memory.buffer, pointer, length).slice();
    });
    switch (op) {
      case 0: return response(out, 0, new Uint8Array(parameter).fill(7));
      case 1: return response(out, 0, Uint8Array.from(parts.flatMap((part) => [...part])));
      case 2: return response(out, 1);
      case 3: return response(out, 2);
      case 4: return response(out, 3);
      case 5: return response(out, 4, undefined, 0xfffffff9);
      case 6: return response(out, 5, encoder.encode("host error 🌸\0tail"));
      case 7: return response(out, 6, Uint8Array.from(parts.flatMap((part) => [...part])));
      case 8: return response(out, 0, new Uint8Array(parameter).fill(19));
      case 200: return response(out, 7, undefined, 37);
      case 201:
      case 202:
        assert.equal(parameter, 37); assert.equal(count, 2);
        return response(out, 0, parts[0]);
      case 9: throw new WebAssembly.RuntimeError("uncatchable host budget trap");
      default: throw new Error("unexpected crypto test operation");
    }
  },
} }));
assert.equal(api.flower_init(), 0);
function allocate(text) {
  const bytes = encoder.encode(text + "\0"), pointer = api.flower_alloc(bytes.length);
  new Uint8Array(api.memory.buffer, pointer, bytes.length).set(bytes);
  return [pointer, bytes.length - 1];
}
function unpack(raw, owned = true) {
  const packed = BigInt.asUintN(64, raw);
  const pointer = Number(packed & 0xffffffffn), length = Number((packed >> 32n) & 0x7fffffffn);
  const text = decoder.decode(new Uint8Array(api.memory.buffer, pointer, length));
  if (owned) api.flower_free(pointer);
  if (packed >> 63n) throw new Error(text);
  return text;
}
function evaluate(source) {
  const [pointer, length] = allocate(source);
  try { return unpack(api.flower_eval(pointer, length)); } finally { api.flower_free(pointer); }
}
function check(source) { assert.equal(evaluate(`JSON.stringify(${source})`), "true", source); }
check(`(() => {const a=new Uint8Array([91,0,255,17,92]);const out=__flowerCrypto(1,0,a.subarray(1,4));return out instanceof Uint8Array&&out.join(',')==='0,255,17'&&a.join(',')==='91,0,255,17,92'})()`);
check(`__flowerCrypto(7,0,'flower 🌸\\0','ok') === 'flower 🌸\\0ok'`);
check(`__flowerCrypto(1,0,new Uint8Array()).length===0`);
check(`__flowerCrypto(2,0)===false&&__flowerCrypto(3,0)===true&&__flowerCrypto(4,0)===null&&__flowerCrypto(5,0)===-7`);
check(`(() => {try {__flowerCrypto(6,0);return false}catch(e){return e instanceof Error&&e.message==='host error 🌸\\0tail'}})()`);
check(`(() => {const a=__flowerCrypto(1,0,new Uint8Array([1,2,3]));const b=a.buffer.transfer(10);return a.byteLength===0&&b.byteLength===10&&new Uint8Array(b).slice(0,3).join(',')==='1,2,3'})()`);
// JS prototypes/getters must not participate in native pointer extraction.
check(`(() => {const a=new Uint8Array([4,5,6]);Object.defineProperty(a,'buffer',{get(){throw Error('getter')}});Object.defineProperty(a,'byteOffset',{get(){throw Error('getter')}});return __flowerCrypto(1,0,a).join(',')==='4,5,6'})()`);
check(`(() => {const b=new ArrayBuffer(8,{maxByteLength:16});const a=new Uint8Array(b,2);a.set([1,2,3,4,5,6]);b.resize(12);const x=__flowerCrypto(1,0,a);b.resize(5);const y=__flowerCrypto(1,0,a);return x.join(',')==='1,2,3,4,5,6,0,0,0,0'&&y.join(',')==='1,2,3'})()`);
for (const source of [
  `__flowerCrypto()`, `__flowerCrypto('1',0)`, `__flowerCrypto(1,-1)`, `__flowerCrypto(1,1.5)`,
  `__flowerCrypto(1,NaN)`, `__flowerCrypto(1,Infinity)`, `__flowerCrypto(4294967296,0)`,
  `__flowerCrypto(1,0,{toString(){throw Error('must not coerce')}})`,
  `__flowerCrypto(1,0,new String('not primitive'))`, `__flowerCrypto(1,0,new Uint8ClampedArray(1))`,
  `__flowerCrypto(1,0,new Int8Array(1))`, `__flowerCrypto(1,0,new DataView(new ArrayBuffer(1)))`,
  `__flowerCrypto(1,0,new Proxy(new Uint8Array(1),{}))`,
  `(() => {const a=new Uint8Array(3);a.buffer.transfer();return __flowerCrypto(1,0,a)})()`,
  `(() => {const b=new ArrayBuffer(8,{maxByteLength:16});const a=new Uint8Array(b,4,4);b.resize(3);return __flowerCrypto(1,0,a)})()`,
  `__flowerCrypto(1,0,new Uint8Array(new SharedArrayBuffer(3)))`,
  `(() => {const a=new Uint8Array(3);return __flowerCrypto(1,0,a,(a.buffer.transfer(),new Uint8Array()))})()`,
  `__flowerCrypto(0,1)`, `__flowerCrypto(200,0,'{}','','','')`,
  `__flowerCrypto(201,0,{},new Uint8Array(),new Uint8Array())`,
]) {
  const before = hostCalls;
  assert.throws(() => evaluate(source), /TypeError|RangeError/, source);
  assert.equal(hostCalls, before, "invalid input must not reach the host");
}
// Inputs must remain alive when producing an output grows Wasm memory. The
// resulting external buffer supports transfer and GC without allocator mixing.
check(`(() => {const a=__flowerCrypto(8,24*1024*1024);return a.length===24*1024*1024&&a[0]===19&&a[a.length-1]===19})()`);
check(`(() => {for(let i=0;i<2000;i++){const a=__flowerCrypto(1,0,'test',new Uint8Array([0,255]));if(a.length!==6)return false}return true})()`);
// Only the private runner enables entropy, and it resets the guard on errors.
evaluate(`__flowerSetRunner((kind, name) => {
 if(name==='throw')throw Error('callback failed');
 if(name==='opaque') {
   const h=__flowerCrypto(200,0,'{}','','','');
   if(h.kind!=='sharedKey'||!Object.isFrozen(h)||Reflect.ownKeys(h).length||h.token!==undefined)throw Error('observable handle');
   try {JSON.stringify(h);throw Error('serializable')}catch(e){if(!e.message.includes('invocation-local'))throw e}
   for(const fake of [37,{kind:'sharedKey',token:'37'},Object.create(Object.getPrototypeOf(h)),new Proxy(h,{})]) {
     let rejected=false;try{__flowerCrypto(201,0,fake,new Uint8Array(),new Uint8Array())}catch(e){rejected=true}
     if(!rejected)throw Error('forged handle accepted');
   }
   let rejected=false;try{__flowerCrypto(201,37,h,new Uint8Array(),new Uint8Array())}catch(e){rejected=true}
   if(!rejected)throw Error('numeric slot accepted');
   return JSON.stringify(Array.from(__flowerCrypto(202,0,h,new Uint8Array([4,5]),new Uint8Array(24))));
 }
 return JSON.stringify(Array.from(__flowerCrypto(0,3)))
}, (kind, e) => ['COMPUTE_ERROR', String(e && e.message || e), undefined], () => null)`);
function invoke(name, bytecode) {
  // Bundles without an initialized snapshot load before the callback runs.
  if (bytecode) unpack(api.flower_load(...bytecode));
  const [namePointer, nameLength] = allocate(name);
  const args = wire.encode(null), argsPointer = api.flower_alloc(args.length);
  new Uint8Array(api.memory.buffer, argsPointer, args.length).set(args);
  // The outcome stays in guest memory until this test instance is discarded.
  const packed = BigInt.asUintN(64, api.flower_invoke(1, namePointer, nameLength, argsPointer, args.length));
  const pointer = Number(packed & 0xffffffffn), length = Number(packed >> 32n);
  const outcome = wire.outcome(new Uint8Array(api.memory.buffer, pointer, length).slice());
  if (!outcome.ok) throw new Error(outcome.error.message);
  return outcome.value;
}
assert.equal(invoke("allowed"), "[7,7,7]");
assert.equal(invoke("opaque"), "[4,5]");
assert.throws(() => invoke("throw"), /callback failed/);
assert.throws(() => evaluate(`__flowerCrypto(0,1)`), /bundle initialization/);
const [source, length] = allocate(`__flowerCrypto(0,1)`);
const packed = BigInt.asUintN(64, api.flower_compile(source, length)); api.flower_free(source);
assert.equal(packed >> 63n, 0n);
const code = [Number(packed & 0xffffffffn), Number(packed >> 32n)];
try { assert.throws(() => invoke("denied-bundle", code), /bundle initialization/); }
finally { api.flower_free(code[0]); }
// Host budget traps cross the Wasm boundary and cannot be caught by JS.
assert.throws(() => evaluate(`try{__flowerCrypto(9,0)}catch(e){'escaped'}`), /uncatchable host budget trap/);
console.log(`Binary crypto guest ABI PASS: ${hostCalls} host calls; typed views, UTF-8, ownership, memory growth, traps, and initialization entropy guard.`);
