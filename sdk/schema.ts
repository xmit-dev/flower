import type { Json } from "./json.ts";

export type SchemaPath = readonly (string | number)[];

export class ValidationError extends Error {
  readonly reason: string;
  readonly path: SchemaPath;
  constructor(reason: string, path: SchemaPath = []) {
    const where = formatPath(path);
    super(where ? `${where}: ${reason}` : reason);
    this.name = "ValidationError";
    this.reason = reason;
    this.path = path;
  }
}

export function formatPath(path: SchemaPath): string {
  let text = "";
  for (const part of path) {
    if (typeof part === "number") text += `[${part}]`;
    else if (/^[A-Za-z_$][\w$]*$/.test(part)) text += text ? `.${part}` : part;
    else text += `[${JSON.stringify(part)}]`;
  }
  return text;
}

interface StandardResult<T> { readonly value?: T; readonly issues?: readonly { readonly message: string; readonly path?: readonly unknown[] }[] }
/** The Standard Schema v1 contract, accepted wherever Flower takes a schema. */
export interface StandardSchema<T = unknown> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (value: unknown) => StandardResult<T> | Promise<StandardResult<T>>;
    readonly types?: { readonly input: unknown; readonly output: T };
  };
}

type Check = (value: unknown, path: (string | number)[]) => void;
const checks = new WeakMap<object, Check>();

export interface Schema<T> extends StandardSchema<T> {
  readonly kind: "schema";
  readonly description: string;
  /** Returns the value unchanged when it matches; throws ValidationError otherwise. */
  parse(value: unknown): T;
  is(value: unknown): value is T;
}
export interface Optional<T> { readonly kind: "optional"; readonly schema: Schema<T> }
export type SchemaLike<T> = Schema<T> | StandardSchema<T>;
export type Infer<S> = S extends Schema<infer T> ? T : S extends Optional<infer T> ? T : S extends StandardSchema<infer T> ? T : never;

type Shape = Readonly<Record<string, SchemaLike<any> | Optional<any>>>;
type Simplify<T> = { [K in keyof T]: T[K] } & {};
export type ObjectOf<P extends Shape> = Simplify<
  { -readonly [K in keyof P as P[K] extends Optional<any> ? never : K]: Infer<P[K]> } &
  { -readonly [K in keyof P as P[K] extends Optional<any> ? K : never]?: Infer<P[K]> }
>;

function make<T>(description: string, check: Check): Schema<T> {
  const schema: Schema<T> = Object.freeze({
    kind: "schema" as const,
    description,
    parse(value: unknown): T {
      check(value, []);
      return value as T;
    },
    is(value: unknown): value is T {
      try { check(value, []); return true; } catch { return false; }
    },
    "~standard": Object.freeze({
      version: 1 as const,
      vendor: "flower",
      validate(value: unknown): StandardResult<T> {
        try { check(value, []); return { value: value as T }; }
        catch (error) {
          if (!(error instanceof ValidationError)) throw error;
          return { issues: [{ message: error.reason, path: error.path }] };
        }
      },
    }),
  });
  checks.set(schema, check);
  return schema;
}

function checkOf(schema: SchemaLike<any>): Check {
  const own = checks.get(schema);
  if (own) return own;
  const standard = schema?.["~standard"];
  if (!standard || typeof standard.validate !== "function") throw new TypeError("Expected a Flower or Standard Schema");
  return (value, path) => {
    const result = standard.validate(value);
    if (result && typeof (result as Promise<unknown>).then === "function") throw new TypeError("Asynchronous schemas are not supported");
    const issue = (result as StandardResult<unknown>).issues?.[0];
    if (issue) {
      const nested = (issue.path ?? []).map((part) =>
        part !== null && typeof part === "object" && "key" in part ? (part as { key: unknown }).key : part)
        .filter((part): part is string | number => typeof part === "string" || typeof part === "number");
      throw new ValidationError(issue.message, [...path, ...nested]);
    }
  };
}

/** Adopt a Flower or Standard Schema. */
export function schema<T>(value: SchemaLike<T>): Schema<T> {
  if (checks.has(value)) return value as Schema<T>;
  return make<T>("custom", checkOf(value));
}

function fail(path: (string | number)[], reason: string): never {
  throw new ValidationError(reason, path.slice());
}

function plain(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function bounds(label: string, options: { min?: number; max?: number }): void {
  for (const key of ["min", "max"] as const) {
    if (options[key] !== undefined && (typeof options[key] !== "number" || !Number.isFinite(options[key]))) {
      throw new TypeError(`${label} ${key} must be a finite number`);
    }
  }
}

function string(options: { min?: number; max?: number; pattern?: RegExp } = {}): Schema<string> {
  bounds("String", options);
  const { min, max } = options;
  const pattern = options.pattern && new RegExp(options.pattern.source, options.pattern.flags.replace(/[gy]/g, ""));
  return make("string", (value, path) => {
    if (typeof value !== "string") fail(path, "must be a string");
    if (min !== undefined && value.length < min) fail(path, min === 1 ? "must not be empty" : `must contain at least ${min} characters`);
    if (max !== undefined && value.length > max) fail(path, `must contain at most ${max} characters`);
    if (pattern && !pattern.test(value)) fail(path, `must match ${pattern}`);
  });
}

function number(options: { min?: number; max?: number; integer?: boolean } = {}): Schema<number> {
  bounds("Number", options);
  const { min, max, integer } = options;
  return make(integer ? "integer" : "number", (value, path) => {
    if (typeof value !== "number" || !Number.isFinite(value)) fail(path, "must be a finite number");
    if (integer && !Number.isSafeInteger(value)) fail(path, "must be a safe integer");
    if (min !== undefined && value < min) fail(path, `must be at least ${min}`);
    if (max !== undefined && value > max) fail(path, `must be at most ${max}`);
  });
}

function literal<const L extends string | number | boolean | null>(expected: L): Schema<L> {
  return make(JSON.stringify(expected), (value, path) => {
    if (value !== expected) fail(path, `must be ${JSON.stringify(expected)}`);
  });
}

function oneOf<const L extends readonly [string | number, ...(string | number)[]]>(values: L): Schema<L[number]> {
  const allowed = new Set<unknown>(values);
  const text = values.map((item) => JSON.stringify(item)).join(", ");
  return make(`one of ${text}`, (value, path) => {
    if (!allowed.has(value)) fail(path, `must be one of ${text}`);
  });
}

function array<S extends SchemaLike<any>>(item: S, options: { min?: number; max?: number } = {}): Schema<Infer<S>[]> {
  bounds("Array", options);
  const check = checkOf(item);
  const { min, max } = options;
  return make("array", (value, path) => {
    if (!Array.isArray(value)) fail(path, "must be an array");
    if (min !== undefined && value.length < min) fail(path, `must contain at least ${min} items`);
    if (max !== undefined && value.length > max) fail(path, `must contain at most ${max} items`);
    for (let index = 0; index < value.length; index++) {
      path.push(index);
      check(value[index], path);
      path.pop();
    }
  });
}

function tuple<const S extends readonly SchemaLike<any>[]>(items: S): Schema<{ -readonly [K in keyof S]: Infer<S[K]> }> {
  const itemChecks = items.map(checkOf);
  return make(`tuple of ${items.length}`, (value, path) => {
    if (!Array.isArray(value) || value.length !== itemChecks.length) fail(path, `must be an array of ${itemChecks.length} items`);
    for (let index = 0; index < itemChecks.length; index++) {
      path.push(index);
      itemChecks[index](value[index], path);
      path.pop();
    }
  });
}

function optional<S extends SchemaLike<any>>(inner: S): Optional<Infer<S>> {
  return Object.freeze({ kind: "optional" as const, schema: schema(inner) });
}

function object<const P extends Shape>(shape: P, options: { rest?: SchemaLike<any> } = {}): Schema<ObjectOf<P>> {
  const entries = Object.keys(shape).map((key) => {
    const field = shape[key] as SchemaLike<any> | Optional<any>;
    const isOptional = (field as Optional<any>).kind === "optional";
    return { key, optional: isOptional, check: checkOf(isOptional ? (field as Optional<any>).schema : field as SchemaLike<any>) };
  });
  const known = new Set(entries.map((entry) => entry.key));
  const rest = options.rest && checkOf(options.rest);
  return make("object", (value, path) => {
    if (!plain(value)) fail(path, "must be an object");
    for (const entry of entries) {
      if (!Object.hasOwn(value, entry.key)) {
        if (!entry.optional) fail(path, `is missing ${JSON.stringify(entry.key)}`);
        continue;
      }
      path.push(entry.key);
      entry.check(value[entry.key], path);
      path.pop();
    }
    for (const key of Object.keys(value)) {
      if (known.has(key)) continue;
      if (!rest) fail(path, `has unexpected property ${JSON.stringify(key)}`);
      path.push(key);
      rest(value[key], path);
      path.pop();
    }
  });
}

function record<S extends SchemaLike<any>>(item: S, options: { key?: SchemaLike<string> } = {}): Schema<Record<string, Infer<S>>> {
  const check = checkOf(item);
  const key = options.key && checkOf(options.key);
  return make("record", (value, path) => {
    if (!plain(value)) fail(path, "must be an object");
    for (const name of Object.keys(value)) {
      path.push(name);
      key?.(name, path);
      check(value[name], path);
      path.pop();
    }
  });
}

function nullable<S extends SchemaLike<any>>(inner: S): Schema<Infer<S> | null> {
  const check = checkOf(inner);
  return make("nullable", (value, path) => { if (value !== null) check(value, path); });
}

function union<const S extends readonly [SchemaLike<any>, ...SchemaLike<any>[]]>(...options: S): Schema<Infer<S[number]>> {
  const alternatives = options.map(checkOf);
  return make("union", (value, path) => {
    let best: ValidationError | undefined;
    for (const check of alternatives) {
      try { check(value, path.slice()); return; }
      catch (error) {
        if (!(error instanceof ValidationError)) throw error;
        if (!best || error.path.length > best.path.length) best = error;
      }
    }
    throw best!;
  });
}

function json(): Schema<Json> {
  return make("JSON", (value, path) => {
    const pending: [unknown, number][] = [[value, 0]];
    // Ancestors only: shared (acyclic) subvalues are valid JSON. depth -1 marks leaving a container.
    const active = new Set<object>();
    while (pending.length) {
      const [item, depth] = pending.pop()!;
      if (depth < 0) { active.delete(item as object); continue; }
      if (depth > 128) fail(path, "nests deeper than 128 levels");
      if (item === null || typeof item === "string" || typeof item === "boolean") continue;
      if (typeof item === "number") { if (!Number.isFinite(item)) fail(path, "must contain only finite numbers"); continue; }
      if (!Array.isArray(item) && !plain(item)) fail(path, "must be JSON");
      if (active.has(item as object)) fail(path, "must not contain cycles");
      active.add(item as object);
      pending.push([item, -1]);
      for (const child of Array.isArray(item) ? item : Object.values(item as object)) pending.push([child, depth + 1]);
    }
  });
}

function refine<S extends SchemaLike<any>>(inner: S, predicate: (value: Infer<S>) => boolean, reason: string): Schema<Infer<S>> {
  const check = checkOf(inner);
  return make("refined", (value, path) => {
    check(value, path);
    if (!predicate(value as Infer<S>)) fail(path, reason);
  });
}

function lazy<T>(resolve: () => SchemaLike<T>): Schema<T> {
  let check: Check | undefined;
  return make("lazy", (value, path) => (check ??= checkOf(resolve()))(value, path));
}

/** Runtime validators whose types flow into methods, collections and clients. */
export const v = Object.freeze({
  string,
  number,
  int: (options: { min?: number; max?: number } = {}) => number({ ...options, integer: true }),
  boolean: () => make<boolean>("boolean", (value, path) => { if (typeof value !== "boolean") fail(path, "must be a boolean"); }),
  null: () => make<null>("null", (value, path) => { if (value !== null) fail(path, "must be null"); }),
  literal,
  enum: oneOf,
  array,
  tuple,
  object,
  record,
  optional,
  nullable,
  union,
  json,
  refine,
  lazy,
});
