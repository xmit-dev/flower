// Differential checks of the native key encoder against the SDK's JS fallback.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { transformSync } from 'esbuild';
import { performance } from 'node:perf_hooks';

const wasm = new WebAssembly.Module(fs.readFileSync(process.argv[2] ?? new URL('./quickjs.wasm', import.meta.url)));
const source = fs.readFileSync(new URL('../../sdk/json.ts', import.meta.url), 'utf8').replace('export function canonicalJson', 'function canonicalJson');
const sdk = transformSync(source, { loader: 'ts', target: 'es2022' }).code;
const factory = `(__flowerCanonicalJson)=>{${sdk};return canonicalJson}`;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });

function guest() {
  const { exports: api } = new WebAssembly.Instance(wasm, {
    flower: { host_call() { throw Error('unexpected host call'); }, crypto_call() { throw Error('unexpected crypto call'); } },
  });
  assert.equal(api.flower_init(), 0);
  const execute = (source) => {
    const bytes = encoder.encode(`${source}\0`);
    const pointer = api.flower_alloc(bytes.length);
    new Uint8Array(api.memory.buffer, pointer, bytes.length).set(bytes);
    const packed = BigInt.asUintN(64, api.flower_eval(pointer, bytes.length - 1));
    api.flower_free(pointer);
    const output = Number(packed & 0xffffffffn);
    const length = Number((packed >> 32n) & 0x7fffffffn);
    const value = decoder.decode(new Uint8Array(api.memory.buffer, output, length));
    api.flower_free(output);
    if (packed >> 63n) throw Error(value);
    return value;
  };
  execute(`const __stringify=JSON.stringify;const __create=Object.create;
    const __native=(${factory})(__flowerCanonicalJson);const __reference=(${factory})(undefined);
    __flowerSetRunner(()=>null,()=>['E','m',undefined],()=>null);`);
  return execute;
}

const cases = [
  ['null', '', 'null', true],
  ['boolean', '', 'false', true],
  ['negative zero', '', '-0', true],
  ['subnormal', '', 'Number.MIN_VALUE', true],
  ['large number', '', 'Number.MAX_VALUE', true],
  ['string escaping', '', '"🌸\\u0000\\b\\f\\n\\r\\t\\\"\\\\\\ud800\\udfff"', true],
  ['rope string', 'let rope="a";for(let i=0;i<24;i++)rope+="flower";', 'rope', true],
  ['composite key', '', '["tenant-a","store-1"]', true],
  ['scalar array', '', '[null,true,false,-0,1e-7,1e21,Number.MIN_VALUE,Number.MAX_VALUE,"\\ud800","🌸"]', true],
  ['empty array', '', '[]', true],
  ['null prototype array', 'const array=["a","b"];Object.setPrototypeOf(array,null);', 'array', true],
  ['custom prototype array', 'const array=["a","b"];Object.setPrototypeOf(array,{get toJSON(){trace+="toJSON;";throw Error("called")}});', 'array', true],
  ['proxy prototype array', 'const array=["a","b"];Object.setPrototypeOf(array,new Proxy({},{get(){trace+="prototype get;";throw Error("called")},getPrototypeOf(){trace+="prototype parent;";throw Error("called")}}));', 'array', true],
  ['deleted named slot', 'const array=["a","b"];array.extra=1;delete array.extra;', 'array', true],
  ['shortened array', 'const array=["a","b","c"];array.length=2;', 'array', true],
  ['inherited toJSON', 'Array.prototype.toJSON=function(){trace+="toJSON;";throw Error("called")};', '["a","b"]', true],
  ['inherited numeric accessors', 'Object.defineProperty(Array.prototype,"0",{get(){trace+="get;";return 9},set(){trace+="set;"}});', '["a","b"]', true],
  ['frozen array', '', 'Object.freeze(["a","b"])'],
  ['sealed array', '', 'Object.seal(["a","b"])'],
  ['undefined', '', 'undefined', false],
  ['NaN', '', 'NaN', false],
  ['infinity', '', 'Infinity', false],
  ['bigint', '', '1n', false],
  ['function', '', '()=>1', false],
  ['symbol', '', 'Symbol("x")', false],
  ['nested array', '', '[["a"],"b"]', true],
  ['object sorting', '', '({"b":2,"a":{"2":2,"10":10,"😀":3,"\\ue000":4}})', true],
  ['array object', '', '[{b:2,a:1}]', true],
  ['record', '', '({id:["t0","s0"],key:"k",name:"The Crispy Cauldron · s0",stock:-0,revenue:1e21,tips:0.5,note:null,open:true})', true],
  ['null prototype object', 'const object=Object.create(null);object.b=1;object.a=[{}];', 'object', true],
  ['deleted field', 'const object={a:1,b:2};delete object.a;', 'object', true],
  ['frozen object', '', 'Object.freeze({a:1,b:"x"})', true],
  ['frozen nested array', '', 'Object.freeze({a:Object.freeze([1])})'],
  ['object accessor', 'const object={};Object.defineProperty(object,"a",{enumerable:true,get(){trace+="getter;";return 1}});', 'object', false],
  ['hidden field', 'const object={};Object.defineProperty(object,"a",{value:1});', 'object', false],
  ['symbol key', '', '({[Symbol("x")]:1})', false],
  ['undefined field', '', '({a:undefined})', false],
  ['class instance', '', 'new (class{constructor(){this.a=1}})()', false],
  ['custom prototype object', '', 'Object.create({inherited:1})', false],
  ['object cycle', 'const object={};object.self=object;', 'object', false],
  ['shared object', 'const shared={a:1};', '[shared,{shared}]', true],
  ['object depth boundary', 'let object={a:"z"};for(let i=0;i<127;i++)object={a:object};', 'object', true],
  ['object depth rejected', 'let object={a:"z"};for(let i=0;i<128;i++)object={a:object};', 'object', false],
  ['patched map', 'const Original=Map;globalThis.Map=class extends Original{constructor(){trace+="map;";super()}};', '({a:1})', false],
  ['lexical map', 'let Map=globalThis.Map;', '({a:1})', false],
  ['patched map set', 'const original=Map.prototype.set;Map.prototype.set=function(k,v){trace+="set;";return original.call(this,k,v)};', '({a:1})', false],
  ['patched sort', 'const original=Array.prototype.sort;Array.prototype.sort=function(f){trace+="sort;";return original.call(this,f)};', '({a:1})', false],
  ['patched join', 'const original=Array.prototype.join;Array.prototype.join=function(s){trace+="join;";return original.call(this,s)};', '({a:1})', false],
  ['patched from', 'const original=Array.from;Array.from=function(...a){trace+="from;";return original.apply(this,a)};', '({a:1})', false],
  ['patched map iterator', 'const proto=Object.getPrototypeOf(new Map().keys());const original=proto.next;proto.next=function(){trace+="next;";return original.call(this)};', '({a:1})', false],
  ['polluted iterator', 'Object.prototype[Symbol.iterator]=function*(){trace+="iterator;"};', '[[1]]', false],
  ['holes', '', '[,"b"]', false],
  ['extended length', 'const array=["a"];array.length=2;', 'array', false],
  ['deleted element', 'const array=["a","b"];delete array[0];', 'array', false],
  ['undefined element', '', '[undefined]', false],
  ['nonfinite element', '', '[Infinity]', false],
  ['named field', 'const array=["a"];array.extra=1;', 'array', false],
  ['symbol field', 'const array=["a"];array[Symbol("x")]=1;', 'array', false],
  ['hidden element', 'const array=["a"];Object.defineProperty(array,"0",{enumerable:false});', 'array', false],
  ['accessor element', 'const array=["a"];Object.defineProperty(array,"0",{get(){trace+="getter;";return 1}});', 'array', false],
  ['own toJSON', 'const array=["a"];array.toJSON=()=>{trace+="toJSON;";return []};', 'array', false],
  ['proxy', 'const array=new Proxy(["a","b"],{getPrototypeOf(t){trace+="prototype;";return Reflect.getPrototypeOf(t)},ownKeys(){trace+="keys;";return ["1","0","length"]},getOwnPropertyDescriptor(t,k){trace+="descriptor:"+String(k)+";";return Reflect.getOwnPropertyDescriptor(t,k)},get(t,k,r){trace+="get:"+String(k)+";";return Reflect.get(t,k,r)}});', 'array', false],
  ['cycle', 'const array=[];array.push(array);', 'array', false],
  ['depth boundary', 'let array=["a"];for(let i=0;i<127;i++)array=[array];', 'array', true],
  ['depth rejected', 'let array=["a"];for(let i=0;i<128;i++)array=[array];', 'array', false],
  ['patched stringify', 'const original=JSON.stringify;JSON.stringify=(v)=>{trace+="stringify;";return original(v)};', '["a","b"]', false],
  ['stringify accessor', 'const original=JSON.stringify;Object.defineProperty(JSON,"stringify",{get(){trace+="stringify;";return original}});', '"a"', false],
  ['lexical JSON', 'let JSON={stringify(v){trace+="lexical;";return "changed"}};', '["a","b"]', false],
  ['lexical globalThis', 'let globalThis={__flowerCanonicalJson:()=>"wrong"};', '["a","b"]', true],
  ['patched finite', 'const original=Number.isFinite;Number.isFinite=(v)=>{trace+="finite;";return original(v)};', '[1,2]', false],
  ['patched create', 'const original=Object.create;Object.create=(v)=>{trace+="create;";return original(v)};', '["a","b"]', false],
  ['patched descriptors', 'const original=Object.getOwnPropertyDescriptor;Object.getOwnPropertyDescriptor=(v,k)=>{trace+="descriptor;";return original(v,k)};', '["a","b"]', false],
  ['patched ownkeys', 'const original=Reflect.ownKeys;Reflect.ownKeys=(v)=>{trace+="keys;";return original(v)};', '["a","b"]', false],
  ['patched regexp test', 'const original=RegExp.prototype.test;RegExp.prototype.test=function(v){trace+="test;";return original.call(this,v)};', '["a","b"]', false],
  ['patched regexp exec', 'const original=RegExp.prototype.exec;RegExp.prototype.exec=function(v){trace+="exec;";return original.call(this,v)};', '["a","b"]', false],
  ['patched iterator', 'const original=Array.prototype[Symbol.iterator];Array.prototype[Symbol.iterator]=function(){trace+="iterator;";return original.call(this)};', '["a","b"]', false],
];

for (const [name, prepare, expression, route] of cases) {
  const evaluate = (native) => {
    const execute = guest();
    return JSON.parse(execute(`let trace="";${prepare}const value=(${expression});
      const output=__create(null);output.route=typeof __flowerCanonicalJson(value)==="string";
      output.probeTrace=trace;trace="";
      try{output.text=${native ? '__native' : '__reference'}(value);output.ok=true}
      catch(error){output.ok=false;output.text=error.name+":"+error.message}
      output.trace=trace;__stringify(output);`));
  };
  const actual = evaluate(true);
  const expected = evaluate(false);
  assert.equal(actual.probeTrace, '', `${name}: native fallback invoked application code`);
  assert.deepEqual(actual, expected, name);
  if (route !== undefined) assert.equal(actual.route, route, `${name}: native route`);
}

const execute = guest();
assert.equal(execute(`(()=>{const d=Object.getOwnPropertyDescriptor(globalThis,"__flowerCanonicalJson");return !d.configurable&&!d.writable&&!d.enumerable})()`), 'true');
assert.throws(() => execute('let __flowerCanonicalJson=()=>"bypass";'), /redeclaration|redefinition|SyntaxError/i);
assert.throws(() => execute('__flowerCanonicalJson=()=>"bypass";'), /read.only|TypeError/i);
assert.equal(execute('__flowerCanonicalJson(["a","b"])'), '["a","b"]');
console.log(`Canonical JSON: ${cases.length} differential/route vectors and protected capability checks passed`);

if (process.env.FLOWER_CANONICAL_BENCH) {
  for (const implementation of ['__reference','__native']) {
    const iterations = 100000;
    const start = performance.now();
    execute(`(()=>{const value=["tenant-a","store-17","order-123"];let sum=0;for(let i=0;i<${iterations};++i)sum+=${implementation}(value).length;return sum})()`);
    console.log(`${implementation}: ${((performance.now()-start)*1000/iterations).toFixed(3)} µs/key`);
  }
}
