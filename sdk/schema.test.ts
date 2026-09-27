import assert from "node:assert/strict";
import { test } from "node:test";
import { collection, define, FlowerError, mutation, v, ValidationError } from "./index.ts";
import type { Infer, Schema, StandardSchema } from "./index.ts";
import { formatPath, schema } from "./schema.ts";
import { testDatabase } from "./testing.ts";

function invalid(run: () => unknown, message: string, path: readonly (string | number)[] = []): void {
  assert.throws(run, (error: unknown) => {
    assert.ok(error instanceof ValidationError);
    assert.equal(error.name, "ValidationError");
    assert.equal(error.message, message);
    assert.deepEqual(error.path, path);
    return true;
  });
}

test("strings check type, length and patterns without regex state", () => {
  assert.equal(v.string().parse(""), "");
  invalid(() => v.string().parse(1), "must be a string");
  invalid(() => v.string({ min: 1 }).parse(""), "must not be empty");
  invalid(() => v.string({ min: 3 }).parse("ab"), "must contain at least 3 characters");
  invalid(() => v.string({ max: 2 }).parse("abc"), "must contain at most 2 characters");
  const word = v.string({ pattern: /^a+$/gy });
  for (let round = 0; round < 3; round++) assert.equal(word.parse("aaa"), "aaa");
  invalid(() => word.parse("ab"), "must match /^a+$/");
  for (const options of [{ min: Number.NaN }, { max: Infinity }, { min: "1" }]) {
    assert.throws(() => v.string(options as never), /String (min|max) must be a finite number/);
  }
});

test("numbers are finite, integers are safe, and bounds are inclusive", () => {
  assert.equal(v.number().parse(-1.5), -1.5);
  for (const value of [Number.NaN, Infinity, "1", null]) invalid(() => v.number().parse(value), "must be a finite number");
  invalid(() => v.int().parse(1.5), "must be a safe integer");
  invalid(() => v.int().parse(2 ** 53), "must be a safe integer");
  invalid(() => v.number({ integer: true }).parse(0.1), "must be a safe integer");
  assert.equal(v.int({ min: 1, max: 4 }).parse(4), 4);
  invalid(() => v.int({ min: 1 }).parse(0), "must be at least 1");
  invalid(() => v.number({ max: 4 }).parse(4.5), "must be at most 4");
  assert.equal(v.int().description, "integer");
  assert.throws(() => v.int({ max: Number.NaN }), /Number max must be a finite number/);
});

test("booleans, null, literals and enums match exactly", () => {
  assert.equal(v.boolean().parse(false), false);
  invalid(() => v.boolean().parse(0), "must be a boolean");
  assert.equal(v.null().parse(null), null);
  invalid(() => v.null().parse(undefined), "must be null");
  assert.equal(v.literal("a").parse("a"), "a");
  invalid(() => v.literal("a").parse("b"), 'must be "a"');
  invalid(() => v.literal(1).parse("1"), "must be 1");
  invalid(() => v.literal(null).parse(false), "must be null");
  const status = v.enum(["open", 2]);
  assert.equal(status.parse(2), 2);
  invalid(() => status.parse("2"), 'must be one of "open", 2');
  const open: "open" | 2 = status.parse("open");
  // @ts-expect-error enum values narrow to their literals
  const other: "closed" = status.parse("open");
  void open, other;
});

test("arrays and tuples validate items with index paths", () => {
  const counts = v.array(v.int({ min: 0 }), { min: 1, max: 3 });
  assert.deepEqual(counts.parse([0, 1]), [0, 1]);
  invalid(() => counts.parse({ length: 0 }), "must be an array");
  invalid(() => counts.parse([]), "must contain at least 1 items");
  invalid(() => counts.parse([1, 2, 3, 4]), "must contain at most 3 items");
  invalid(() => counts.parse([1, -1]), "[1]: must be at least 0", [1]);
  const pair = v.tuple([v.string(), v.int()]);
  const typed: [string, number] = pair.parse(["a", 1]);
  void typed;
  invalid(() => pair.parse(["a"]), "must be an array of 2 items");
  invalid(() => pair.parse(["a", 1, 2]), "must be an array of 2 items");
  invalid(() => pair.parse([1, 1]), "[0]: must be a string", [0]);
  invalid(() => v.array(pair).parse([["a", 1], ["b", "x"]]), "[1][1]: must be a finite number", [1, 1]);
  assert.throws(() => v.array(v.int(), { min: Infinity }), /Array min must be a finite number/);
});

test("objects are closed, report missing and unexpected properties, and allow optional fields", () => {
  const item = v.object({ sku: v.string({ min: 1 }), note: v.optional(v.string()) });
  assert.deepEqual(item.parse({ sku: "a" }), { sku: "a" });
  assert.deepEqual(item.parse({ sku: "a", note: "n" }), { sku: "a", note: "n" });
  invalid(() => item.parse({}), 'is missing "sku"');
  invalid(() => item.parse({ sku: "a", extra: 1 }), 'has unexpected property "extra"');
  invalid(() => item.parse({ sku: "a", note: undefined }), "note: must be a string", ["note"]);
  for (const value of [null, [], new Date(0), new (class Point { x = 1 })(), "x"]) invalid(() => item.parse(value), "must be an object");
  const bare = Object.assign(Object.create(null), { sku: "b" });
  assert.equal(item.parse(bare), bare);
  const open = v.object({ id: v.int() }, { rest: v.string() });
  assert.deepEqual(open.parse({ id: 1, label: "x" }), { id: 1, label: "x" });
  invalid(() => open.parse({ id: 1, label: 2 }), "label: must be a string", ["label"]);
  type Item = Infer<typeof item>;
  const ok: Item = { sku: "a" };
  // @ts-expect-error required properties stay required
  const missing: Item = { note: "n" };
  // @ts-expect-error property types flow from their schemas
  const wrong: Item = { sku: 1 };
  void ok, missing, wrong;
});

test("messages locate nested failures and quote non-identifier keys", () => {
  const order = v.object({ order: v.object({ lines: v.array(v.object({ quantity: v.int({ max: 4 }) })) }) });
  invalid(() => order.parse({ order: { lines: [{ quantity: 1 }, { quantity: 5 }] } }),
    "order.lines[1].quantity: must be at most 4", ["order", "lines", 1, "quantity"]);
  const error = (() => { try { order.parse({ order: { lines: [{ quantity: 9 }] } }); } catch (caught) { return caught as ValidationError; } })()!;
  assert.equal(error.reason, "must be at most 4");
  invalid(() => v.object({ "a b": v.int() }).parse({ "a b": "x" }), '["a b"]: must be a finite number', ["a b"]);
  invalid(() => v.array(v.object({ $id: v.int() })).parse([{ $id: "x" }]), "[0].$id: must be a finite number", [0, "$id"]);
  assert.equal(formatPath(["x", 0, "1st", "_ok"]), 'x[0]["1st"]._ok');
  assert.equal(new ValidationError("bad").message, "bad");
});

test("optional marks object properties only", () => {
  assert.throws(() => v.array(v.optional(v.string()) as never), /Expected a Flower or Standard Schema/);
  // @ts-expect-error optional is not a schema outside objects
  const list = () => v.array(v.optional(v.string()));
  void list;
  const optional = v.optional(v.int());
  assert.equal(optional.kind, "optional");
  assert.ok(Object.isFrozen(optional));
});

test("records validate each value and an optional key schema", () => {
  const scores = v.record(v.int(), { key: v.string({ pattern: /^[a-z]+$/ }) });
  assert.deepEqual(scores.parse({ alice: 1, bob: 2 }), { alice: 1, bob: 2 });
  invalid(() => scores.parse({ alice: "1" }), "alice: must be a finite number", ["alice"]);
  invalid(() => scores.parse({ Alice: 1 }), "Alice: must match /^[a-z]+$/", ["Alice"]);
  invalid(() => scores.parse([1]), "must be an object");
  const typed: Record<string, number> = v.record(v.int()).parse({});
  void typed;
});

test("nullable accepts null and unions report the most specific alternative", () => {
  const maybe = v.nullable(v.int());
  assert.equal(maybe.parse(null), null);
  invalid(() => maybe.parse("x"), "must be a finite number");
  const scalar = v.union(v.string(), v.int());
  assert.equal(scalar.parse(3), 3);
  invalid(() => scalar.parse(true), "must be a string");
  const event = v.union(
    v.object({ kind: v.literal("a"), n: v.int() }),
    v.object({ kind: v.literal("b"), data: v.object({ text: v.string() }) }),
  );
  assert.deepEqual(event.parse({ kind: "b", data: { text: "t" } }), { kind: "b", data: { text: "t" } });
  invalid(() => event.parse({ kind: "b", data: { text: 1 } }), "data.text: must be a string", ["data", "text"]);
  invalid(() => v.object({ event }).parse({ event: { kind: "c", n: 1 } }), 'event.kind: must be "a"', ["event", "kind"]);
  const parsed = event.parse({ kind: "a", n: 1 });
  if (parsed.kind === "a") { const n: number = parsed.n; void n; }
  const exploding = v.union(v.refine(v.int(), () => { throw new RangeError("boom"); }, "never"), v.string());
  assert.throws(() => exploding.parse(1), RangeError);
});

test("json accepts JSON values, including shared subvalues, and rejects everything else", () => {
  const shared = { k: [1] };
  const value = { a: [1, "x", null, true, { b: -0 }], s: shared, t: [shared, shared] };
  assert.equal(v.json().parse(value), value);
  invalid(() => v.json().parse({ a: [Number.NaN] }), "must contain only finite numbers");
  for (const bad of [undefined, { a: undefined }, [, 1], new Date(0), () => 1, 1n, Symbol("s"), new Map()]) {
    invalid(() => v.json().parse(bad), "must be JSON");
  }
  const cyclic: Record<string, unknown> = {};
  cyclic.self = { back: cyclic };
  invalid(() => v.json().parse(cyclic), "must not contain cycles");
  const loop: unknown[] = [];
  loop.push([loop]);
  invalid(() => v.json().parse(loop), "must not contain cycles");
  let deep: unknown = null;
  for (let depth = 0; depth < 128; depth++) deep = [deep];
  assert.doesNotThrow(() => v.json().parse(deep));
  invalid(() => v.json().parse([deep]), "nests deeper than 128 levels");
});

test("refine adds predicates after the inner schema; lazy builds recursive schemas once", () => {
  const even = v.refine(v.int(), (n) => n % 2 === 0, "must be even");
  assert.equal(even.parse(2), 2);
  invalid(() => even.parse(3), "must be even");
  invalid(() => even.parse("2"), "must be a finite number");
  interface Tree { value: number; children: Tree[] }
  let resolved = 0;
  const tree: Schema<Tree> = v.lazy(() => { resolved++; return v.object({ value: v.int(), children: v.array(tree) }); });
  assert.equal(resolved, 0);
  assert.ok(tree.is({ value: 1, children: [{ value: 2, children: [] }] }));
  invalid(() => tree.parse({ value: 1, children: [{ value: "2", children: [] }] }), "children[0].value: must be a finite number", ["children", 0, "value"]);
  assert.equal(resolved, 1);
});

test("parse returns its input unchanged and is() narrows without throwing", () => {
  const shape = v.object({ id: v.int() });
  const input = { id: 1 };
  assert.equal(shape.parse(input), input);
  const unknownValue: unknown = { id: 2 };
  if (shape.is(unknownValue)) { const id: number = unknownValue.id; assert.equal(id, 2); }
  assert.equal(shape.is({ id: "x" }), false);
  assert.ok(Object.isFrozen(shape));
  assert.equal(shape.kind, "schema");
  assert.deepEqual([v.string(), v.literal("a"), v.enum(["a", 1]), v.tuple([v.null()]), v.json()].map((each) => each.description),
    ["string", '"a"', 'one of "a", 1', "tuple of 1", "JSON"]);
});

test("Flower schemas implement Standard Schema v1", () => {
  const quantity = v.object({ quantity: v.int({ max: 4 }) });
  const standard = quantity["~standard"];
  assert.equal(standard.version, 1);
  assert.equal(standard.vendor, "flower");
  assert.deepEqual(standard.validate({ quantity: 2 }), { value: { quantity: 2 } });
  assert.deepEqual(standard.validate({ quantity: 5 }), { issues: [{ message: "must be at most 4", path: ["quantity"] }] });
  assert.throws(() => v.refine(v.int(), () => { throw new RangeError("boom"); }, "x")["~standard"].validate(1), RangeError);
});

function foreign<T>(check: (value: unknown) => boolean, message: string, path?: readonly unknown[]): StandardSchema<T> {
  return { "~standard": { version: 1, vendor: "test", validate: (value) => check(value) ? { value: value as T } : { issues: [{ message, ...(path ? { path } : {}) }] } } };
}

test("foreign Standard Schemas compose inside Flower schemas with mapped paths", () => {
  const even = foreign<number>((value) => typeof value === "number" && value % 2 === 0, "must be even", [{ key: "inner" }, 0, Symbol("dropped")]);
  const holder = v.object({ n: even, list: v.array(foreign<string>((value) => value === "ok", "must be ok")) });
  const parsed: { n: number; list: string[] } = holder.parse({ n: 2, list: ["ok"] });
  void parsed;
  invalid(() => holder.parse({ n: 3, list: [] }), "n.inner[0]: must be even", ["n", "inner", 0]);
  invalid(() => holder.parse({ n: 2, list: ["ok", "no"] }), "list[1]: must be ok", ["list", 1]);
  const adopted = schema(even);
  assert.equal(adopted.description, "custom");
  assert.equal(adopted.is(4), true);
  assert.equal(schema(holder), holder);
  const later: StandardSchema<number> = { "~standard": { version: 1, vendor: "test", validate: async (value) => ({ value: value as number }) } };
  assert.throws(() => v.array(later).parse([1]), /Asynchronous schemas are not supported/);
  assert.throws(() => v.array({} as never), /Expected a Flower or Standard Schema/);
  assert.throws(() => v.object({ x: 1 as never }), /Expected a Flower or Standard Schema/);
});

test("foreign schemas validate method arguments and records end-to-end", async () => {
  const positive = foreign<number>((value) => typeof value === "number" && value > 0, "must be positive");
  const counters = collection("counters", foreign<{ n: number }>((value) => typeof (value as { n?: unknown })?.n === "number", "needs n", [{ key: "n" }]));
  const bump = mutation("bump", { args: positive }, (ctx, by) => {
    const next = (ctx.get(counters, "c")?.n ?? 0) + by;
    ctx.set(counters, "c", { n: next });
    return next;
  });
  const corrupt = mutation("corrupt", (ctx) => { ctx.set(counters, "c", { n: "x" } as never); return null; });
  const db = await testDatabase(define({ http: { bump, corrupt } }));
  assert.equal(db.mutate("bump", 2), 2);
  const failure = (run: () => unknown) => { try { run(); } catch (error) { assert.ok(error instanceof FlowerError); return error.failure; } assert.fail("expected a failure"); };
  assert.deepEqual(failure(() => db.mutate("bump", -1)), { code: "INVALID_ARGUMENT", message: "must be positive", details: { path: [] } });
  assert.deepEqual(failure(() => db.mutate("corrupt")), {
    code: "INVALID_RECORD", message: "counters n: needs n", details: { collection: "counters", key: "c", path: ["n"] },
  });
  // @ts-expect-error argument types come from the foreign schema
  assert.throws(() => db.mutate("bump", "2"));
});
