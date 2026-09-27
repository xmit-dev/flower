// Collection access policies: rules the server evaluates natively on every
// read and write a method makes for its caller. See docs/guide/access.html.
import { canonicalJson, type Json } from "./json.ts";
import type { FieldOf } from "./core.ts";

type FieldValue<T, F> = [T] extends [object] ? (F extends keyof T ? T[F] : never) : Json;
type Item<T> = T extends readonly (infer E)[] ? E : never;

const json = Symbol("flower.rule");
type RuleJson = Record<string, unknown>;
type OperandJson = { ref: string[] } | { value: Json };

/** A condition on the caller and the row. Combine with and/or/not. */
export interface Rule {
  readonly kind: "rule";
  and(...rules: RuleLike[]): Rule;
  or(...rules: RuleLike[]): Rule;
  not(): Rule;
}
/** A rule, or true/false for everyone/no one. */
export type RuleLike = Rule | boolean;

/**
 * A value a rule compares: a principal attribute, a field of the stored row (`row`) or
 * of the row being written (`next`), the row's key, or the time (`now`). Missing and
 * null values compare false with everything, so `row("owner").eq(principal.subject)`
 * never matches an anonymous caller. Only numbers order against numbers, and strings
 * against strings (by code point).
 */
export interface Operand<T = Json> {
  /** Both sides present, non-null and equal. */
  eq(value: OperandLike<T>): Rule;
  /** Both sides present, non-null and different. */
  ne(value: OperandLike<T>): Rule;
  /** Present and equal to an element of the list. */
  in(list: OperandLike<readonly T[]>): Rule;
  /** A list containing the item. */
  has(item: OperandLike<Item<T>>): Rule;
  /** Present and not null. */
  exists(): Rule;
  /** Less than: two numbers, or two strings. */
  lt(value: OperandLike<T>): Rule;
  /** Less than or equal. */
  lte(value: OperandLike<T>): Rule;
  /** Greater than. */
  gt(value: OperandLike<T>): Rule;
  /** Greater than or equal. */
  gte(value: OperandLike<T>): Rule;
  /** A string that starts with the prefix. */
  startsWith(prefix: OperandLike<string>): Rule;
}
export type OperandLike<T> = Operand<T> | T;

/** The row's key; `at` reads a part of a JSON key (collection.key(schema)). */
export interface KeyOperand extends Operand<string> {
  /** An element of an array key, or a field of an object key, then optionally deeper. */
  at(...path: [string | number, ...(string | number)[]]): Operand<Json>;
}

/** A derived value's arguments; `at` reads an element or field of them. */
export interface ArgsOperand<A> extends Operand<A> {
  at(...path: [string | number, ...(string | number)[]]): Operand<Json>;
}

/** What a derived value's access rule can refer to. */
export interface DerivedScope<A> {
  readonly principal: PrincipalFields;
  /** The arguments the method reads the value with. */
  readonly args: ArgsOperand<A>;
  readonly now: Operand<number>;
  all(...rules: RuleLike[]): Rule;
  any(...rules: RuleLike[]): Rule;
  not(rule: RuleLike): Rule;
}

/**
 * Who may read a derived value from a method: a rule, or a function of a DerivedScope
 * returning one. Derived values compute with the application's rights, over every row,
 * so a value built from guarded collections says who may see it.
 */
export type DerivedAccess<A> = RuleLike | ((scope: DerivedScope<A>) => RuleLike);

/** A field of a row, then optionally a path into it. */
export interface RowFields<T> {
  <F extends FieldOf<T>>(field: F): Operand<FieldValue<T, F>>;
  (field: FieldOf<T>, ...path: [string, ...string[]]): Operand<Json>;
}

export interface PrincipalFields {
  /** The caller's subject; absent for anonymous callers. */
  readonly subject: Operand<string>;
  /** The caller's tenant (in a named partition, its name). */
  readonly tenant: Operand<string>;
  /** A claim, or a path into one. */
  claim(name: string, ...path: string[]): Operand<Json>;
  /** Any signed-in caller. */
  readonly authenticated: Rule;
}

/** A collection a rule names: a collection reference, or its name. */
export type CollectionName = string | { readonly kind: "collection"; readonly name: string };

/** What access rules can refer to. `row` is absent on insert; `next` exists only while writing. */
export interface AccessScope<T> {
  readonly principal: PrincipalFields;
  /** The stored row: in read, update, delete and field rules. */
  readonly row: RowFields<T>;
  /** The row being written: in insert, update and field write rules. */
  readonly next: RowFields<T>;
  /** The row's stored key: the string, or canonical JSON for typed keys. */
  readonly key: KeyOperand;
  /**
   * The invocation's time in milliseconds. A result that depends on it isn't cached,
   * and changes by itself when a comparison with it flips (`row("expiresAt").gt(now)`).
   */
  readonly now: Operand<number>;
  all(...rules: RuleLike[]): Rule;
  any(...rules: RuleLike[]): Rule;
  not(rule: RuleLike): Rule;
  /**
   * The caller may read the row of `collection` at `key`: it exists (this invocation's
   * own writes included), and that collection's read rule allows it. Rows can so follow
   * another row's rule, like a log's entries their session's (`readable(sessions,
   * key.at(0))`), without copying what it depends on into each of them; a result that
   * used it changes when that row does. A string key is used as is; an array or object,
   * as canonical JSON, like typed keys. The collection needs an access policy whose read
   * rule doesn't use readable itself.
   */
  readable(collection: CollectionName, key: OperandLike<Json>): Rule;
}

export interface FieldAccess {
  /** Who sees the field; others get rows without it. Defaults to whoever sees the row. */
  readonly read?: RuleLike;
  /** Who may change it. Defaults to whoever may read it. */
  readonly write?: RuleLike;
}

/**
 * Row rules and field rules. An operation without a rule is denied; `write` is the
 * default for insert, update and delete.
 */
export interface AccessRules<T> {
  readonly read?: RuleLike;
  readonly write?: RuleLike;
  readonly insert?: RuleLike;
  readonly update?: RuleLike;
  readonly delete?: RuleLike;
  readonly fields?: { readonly [F in FieldOf<T>]?: FieldAccess };
}

export type CollectionAccess<T> = AccessRules<T> | ((scope: AccessScope<T>) => AccessRules<T>);

/** The compiled policy in a collection's manifest entry. */
export interface AccessManifest {
  readonly read: Json;
  readonly insert: Json;
  readonly update: Json;
  readonly delete: Json;
  readonly fields?: Readonly<Record<string, { readonly read?: Json; readonly write?: Json }>>;
}

class RuleNode implements Rule {
  readonly kind = "rule" as const;
  readonly [json]: RuleJson;
  constructor(body: RuleJson) {
    this[json] = body;
    Object.freeze(this);
  }
  and(...rules: RuleLike[]): Rule { return join("all", [this, ...rules]); }
  or(...rules: RuleLike[]): Rule { return join("any", [this, ...rules]); }
  not(): Rule { return new RuleNode({ not: this[json] }); }
}

function ruleJson(rule: unknown, label: string): RuleJson {
  if (typeof rule === "boolean") return { const: rule };
  if (rule instanceof RuleNode) return rule[json];
  throw new TypeError(`${label} must be a rule, true or false`);
}

function join(operator: "all" | "any", rules: RuleLike[]): Rule {
  const parts: RuleJson[] = [];
  for (const rule of rules) {
    const body = ruleJson(rule, operator === "all" ? "all()" : "any()");
    // Flatten nested all(all(...)) and any(any(...)).
    if (Object.hasOwn(body, operator)) parts.push(...(body[operator] as RuleJson[]));
    else parts.push(body);
  }
  return new RuleNode({ [operator]: parts });
}

class OperandNode implements Operand<any> {
  readonly [json]: OperandJson;
  constructor(body: OperandJson) {
    this[json] = body;
    Object.freeze(this);
  }
  eq(value: unknown): Rule { return new RuleNode({ eq: [this[json], operand(value)] }); }
  ne(value: unknown): Rule { return new RuleNode({ ne: [this[json], operand(value)] }); }
  in(list: unknown): Rule { return new RuleNode({ in: [this[json], operand(list)] }); }
  has(item: unknown): Rule { return new RuleNode({ in: [operand(item), this[json]] }); }
  exists(): Rule { return new RuleNode({ exists: this[json] }); }
  lt(value: unknown): Rule { return new RuleNode({ lt: [this[json], operand(value)] }); }
  lte(value: unknown): Rule { return new RuleNode({ lte: [this[json], operand(value)] }); }
  gt(value: unknown): Rule { return new RuleNode({ gt: [this[json], operand(value)] }); }
  gte(value: unknown): Rule { return new RuleNode({ gte: [this[json], operand(value)] }); }
  startsWith(prefix: unknown): Rule { return new RuleNode({ startsWith: [this[json], operand(prefix)] }); }
}

/** An operand whose parts `at` reads: a JSON key, or a derived value's arguments. */
class PartsNode extends OperandNode implements KeyOperand {
  at(...path: unknown[]): Operand<Json> {
    const base = (this[json] as { ref: string[] }).ref;
    const parts = path.map((part) => typeof part === "number" && Number.isSafeInteger(part) && part >= 0 ? String(part) : part);
    return new OperandNode({ ref: [...base, ...segments(parts, `${base[0]}.at`)] });
  }
}

function operand(value: unknown): OperandJson {
  if (value instanceof OperandNode) return value[json];
  if (value instanceof RuleNode) throw new TypeError("Compare values, not rules");
  canonicalJson(value as Json);
  return { value: value as Json };
}

function segments(parts: unknown[], label: string): string[] {
  if (parts.length === 0 || parts.some((part) => typeof part !== "string" || !part)) {
    throw new TypeError(`${label} needs nonempty field names`);
  }
  return parts as string[];
}

function fieldsOf(root: "row" | "next"): RowFields<any> {
  return ((...path: unknown[]) => new OperandNode({ ref: [root, ...segments(path, root)] })) as RowFields<any>;
}

const principalFields: PrincipalFields = Object.freeze({
  subject: new OperandNode({ ref: ["principal", "subject"] }),
  tenant: new OperandNode({ ref: ["principal", "tenant"] }),
  claim: (...path: unknown[]) => new OperandNode({ ref: ["principal", "claims", ...segments(path, "claim")] }),
  authenticated: new RuleNode({ exists: { ref: ["principal", "subject"] } }),
});
const combinators = {
  all: (...rules: RuleLike[]) => join("all", rules),
  any: (...rules: RuleLike[]) => join("any", rules),
  not: (rule: RuleLike) => new RuleNode({ not: ruleJson(rule, "not()") }),
};

function collectionName(collection: unknown): string {
  const name = typeof collection === "string" ? collection
    : collection !== null && typeof collection === "object" && (collection as { kind?: unknown }).kind === "collection" ? (collection as { name?: unknown }).name
    : undefined;
  if (typeof name !== "string" || !name) throw new TypeError("readable() needs a collection or its name");
  return name;
}

const scope: AccessScope<any> = Object.freeze({
  principal: principalFields,
  row: fieldsOf("row"),
  next: fieldsOf("next"),
  key: new PartsNode({ ref: ["key"] }),
  now: new OperandNode({ ref: ["now"] }),
  ...combinators,
  readable: (collection: CollectionName, key: unknown) => new RuleNode({ readable: [collectionName(collection), operand(key)] }),
});

const derivedScope: DerivedScope<any> = Object.freeze({
  principal: principalFields,
  args: new PartsNode({ ref: ["args"] }),
  now: new OperandNode({ ref: ["now"] }),
  ...combinators,
});

/** Roots each rule may use, matching the server's checks. */
const roots = {
  read: ["principal", "key", "now", "row"],
  insert: ["principal", "key", "now", "next"],
  update: ["principal", "key", "now", "row", "next"],
  delete: ["principal", "key", "now", "row"],
  fieldRead: ["principal", "key", "now", "row"],
  fieldWrite: ["principal", "key", "now", "row", "next"],
} as const;

function checkRoots(body: unknown, allowed: readonly string[], label: string): void {
  if (Array.isArray(body)) { for (const each of body) checkRoots(each, allowed, label); return; }
  if (body === null || typeof body !== "object") return;
  if (Object.hasOwn(body, "value")) return;
  const ref = (body as { ref?: string[] }).ref;
  if (ref) {
    if (!allowed.includes(ref[0])) {
      const hint = allowed.includes("args") ? "derived values' rules see principal, args and now"
        : ref[0] === "next" ? "next only exists while writing" : "row doesn't exist yet on insert";
      throw new TypeError(`Access rule ${label} can't use ${ref.join(".")}: ${hint}`);
    }
    return;
  }
  for (const each of Object.values(body)) checkRoots(each, allowed, label);
}

function compiled(rule: unknown, allowed: readonly string[], label: string): Json {
  const body = ruleJson(rule, `Access rule ${label}`);
  checkRoots(body, allowed, label);
  return body as Json;
}

/** Compile a derived value's access rule into the form its manifest entry carries. */
export function compileDerivedAccess<A>(declaration: DerivedAccess<A>, name: string): Json {
  const rule = typeof declaration === "function" ? declaration(derivedScope as DerivedScope<A>) : declaration;
  return deepFreeze(compiled(rule, ["principal", "args", "now"], `${name}.access`));
}

/** Compile an access declaration into the manifest form the server enforces. */
export function compileAccess<T>(declaration: CollectionAccess<T>, collection: string): AccessManifest {
  const rules = typeof declaration === "function" ? declaration(scope as AccessScope<T>) : declaration;
  const settings = plainRules(rules, collection);
  const write = settings.write ?? false;
  const manifest: Record<string, unknown> = {
    read: compiled(settings.read ?? false, roots.read, `${collection}.read`),
    insert: compiled(settings.insert ?? write, roots.insert, `${collection}.insert`),
    update: compiled(settings.update ?? write, roots.update, `${collection}.update`),
    delete: compiled(settings.delete ?? write, roots.delete, `${collection}.delete`),
  };
  if (settings.fields !== undefined) {
    if (settings.fields === null || typeof settings.fields !== "object" || Array.isArray(settings.fields)) {
      throw new TypeError(`Access fields of ${collection} must be an object`);
    }
    const fields: Record<string, Record<string, Json>> = {};
    for (const [field, access] of Object.entries(settings.fields as Record<string, FieldAccess>)) {
      if (!field) throw new TypeError(`Access fields of ${collection} need nonempty names`);
      const entry = plainRules(access, `${collection}.fields.${field}`, ["read", "write"]);
      if (entry.read === undefined && entry.write === undefined) {
        throw new TypeError(`Access for ${collection}.${field} needs read or write`);
      }
      fields[field] = {
        ...(entry.read !== undefined ? { read: compiled(entry.read, roots.fieldRead, `${collection}.fields.${field}.read`) } : {}),
        ...(entry.write !== undefined ? { write: compiled(entry.write, roots.fieldWrite, `${collection}.fields.${field}.write`) } : {}),
      };
    }
    if (Object.keys(fields).length) manifest.fields = fields;
  }
  return deepFreeze(manifest) as unknown as AccessManifest;
}

function plainRules(value: unknown, label: string, allowed: readonly string[] = ["read", "write", "insert", "update", "delete", "fields"]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`Access for ${label} must be an object`);
  for (const key of Object.keys(value)) {
    if (!allowed.includes(key)) throw new TypeError(`Access for ${label} does not accept ${JSON.stringify(key)}`);
  }
  return value as Record<string, unknown>;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    for (const each of Object.values(value)) deepFreeze(each);
    Object.freeze(value);
  }
  return value;
}

// ---- Enforcement for the in-process test database, matching the server's rules.

type Row = { key: string; value: Json };
/** What a check sees; `parts` caches the key parsed as JSON. */
type Subject = { key: string; row?: Json; next?: Json; parts?: Json | undefined | null };
/** The invocation's clock, as rules read it: the time, and a flip to report. */
interface Clock { now(): number; note(flip: number | undefined): void }
/** Whether the caller may read the row of `collection` at `key`, for `readable`. */
type Rows = (collection: string, key: string) => boolean;
const noRows: Rows = () => false;
type Host = Record<string, (...args: any[]) => any>;

function lookup(value: Json | undefined, path: readonly string[]): Json | undefined {
  let current = value;
  for (const segment of path) {
    if (Array.isArray(current)) current = /^\d+$/.test(segment) ? current[Number(segment)] : undefined;
    else if (current !== null && typeof current === "object") current = Object.hasOwn(current, segment) ? (current as Record<string, Json>)[segment] : undefined;
    else return undefined;
  }
  return current;
}

const present = (value: Json | undefined): value is Json => value !== undefined && value !== null;

function keyParts(subject: Subject): Json | undefined {
  if (subject.parts === undefined) {
    let parsed: Json | null = null;
    try { parsed = JSON.parse(subject.key) as Json; } catch { /* a plain string key */ }
    subject.parts = parsed !== null && typeof parsed === "object" ? parsed : null;
  }
  return subject.parts ?? undefined;
}

function resolve(operand: OperandJson, caller: Json, subject: Subject, clock: Clock): Json | undefined {
  if ("value" in operand) return operand.value;
  const [root, ...path] = operand.ref;
  if (root === "principal") return lookup(caller, path);
  if (root === "now") return clock.now();
  if (root === "key") return path.length ? lookup(keyParts(subject), path) : subject.key;
  // A derived value's rule sees its arguments where rows would be.
  return lookup(root === "row" || root === "args" ? subject.row : subject.next, path);
}

/** Code point order, which is how the server (UTF-8 bytes) orders strings. */
function compareText(left: string, right: string): number {
  let i = 0;
  let j = 0;
  while (i < left.length && j < right.length) {
    const a = left.codePointAt(i)!;
    const b = right.codePointAt(j)!;
    if (a !== b) return a < b ? -1 : 1;
    i += a > 0xffff ? 2 : 1;
    j += b > 0xffff ? 2 : 1;
  }
  return i < left.length ? 1 : j < right.length ? -1 : 0;
}

function order(left: Json | undefined, right: Json | undefined): number | undefined {
  if (typeof left === "string" && typeof right === "string") return compareText(left, right);
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  return undefined;
}

const swapped: Record<string, string> = { lt: "gt", lte: "gte", gt: "lt", gte: "lte" };

/** The first instant after `now` when `now <operator> value` changes outcome. */
function flip(operator: string, now: number, value: Json | undefined): number | undefined {
  if (operator === "in") {
    if (!Array.isArray(value)) return undefined;
    const flips = value.map((item) => flip("eq", now, item)).filter((at): at is number => at !== undefined);
    return flips.length ? Math.min(...flips) : undefined;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  const reached = Math.ceil(value);
  const passed = Math.floor(value) + 1;
  const at = operator === "lt" || operator === "gte" ? reached
    : operator === "lte" || operator === "gt" ? passed
    : operator === "eq" || operator === "ne" ? (reached > now ? reached : passed)
    : undefined;
  return at !== undefined && at > now && at <= Number.MAX_SAFE_INTEGER ? at : undefined;
}

function same(left: Json | undefined, right: Json | undefined): boolean | undefined {
  return present(left) && present(right) ? canonicalJson(left) === canonicalJson(right) : undefined;
}

const isNow = (operand: OperandJson) => "ref" in operand && operand.ref[0] === "now";

function holds(rule: RuleJson, caller: Json, subject: Subject, clock: Clock, rows: Rows): boolean {
  const [operator, body] = Object.entries(rule)[0] as [string, any];
  switch (operator) {
    case "const": return body === true;
    case "all": return (body as RuleJson[]).every((each) => holds(each, caller, subject, clock, rows));
    case "any": return (body as RuleJson[]).some((each) => holds(each, caller, subject, clock, rows));
    case "not": return !holds(body, caller, subject, clock, rows);
    case "exists": return isNow(body) || present(resolve(body, caller, subject, clock));
    case "readable": {
      const key = resolve(body[1], caller, subject, clock);
      const text = typeof key === "string" ? key : key !== null && typeof key === "object" ? canonicalJson(key) : undefined;
      return text !== undefined && rows(body[0], text);
    }
  }
  const left = resolve(body[0], caller, subject, clock);
  const right = resolve(body[1], caller, subject, clock);
  // Reading the clock makes the result time dependent until the comparison flips.
  if (isNow(body[0]) && !isNow(body[1])) clock.note(flip(operator, left as number, right));
  else if (isNow(body[1]) && !isNow(body[0])) clock.note(operator === "in" ? undefined : flip(swapped[operator] ?? operator, right as number, left));
  switch (operator) {
    case "eq": return same(left, right) === true;
    case "ne": return same(left, right) === false;
    case "in": return present(left) && Array.isArray(right) && right.some((each) => same(left, each) === true);
    case "lt": return order(left, right) === -1;
    case "lte": { const sign = order(left, right); return sign === -1 || sign === 0; }
    case "gt": return order(left, right) === 1;
    case "gte": { const sign = order(left, right); return sign === 1 || sign === 0; }
    case "startsWith": return typeof left === "string" && typeof right === "string" && left.startsWith(right);
    default: throw new TypeError(`Unknown access rule ${operator}`);
  }
}

const skip = Symbol("flower.skip");

class Guard {
  readonly collection: string;
  readonly policy: AccessManifest;
  readonly caller: Json;
  readonly clock: Clock;
  readonly readable: Rows;
  constructor(collection: string, policy: AccessManifest, caller: Json, clock: Clock, readable: Rows) {
    this.collection = collection;
    this.policy = policy;
    this.caller = caller;
    this.clock = clock;
    this.readable = readable;
  }
  private rule(body: Json): RuleJson { return body as RuleJson; }
  private check(rule: Json, subject: Subject): boolean { return holds(this.rule(rule), this.caller, subject, this.clock, this.readable); }
  private fieldReadable(field: string, key: string, row: Json): boolean {
    const read = this.policy.fields?.[field]?.read;
    return read === undefined || this.check(read, { key, row });
  }
  visible(key: string, row: Json, fields: readonly string[] = []): boolean {
    return this.check(this.policy.read, { key, row }) &&
      fields.every((field) => this.fieldReadable(field, key, row));
  }
  redact(key: string, row: Json): Json {
    if (row === null || typeof row !== "object" || Array.isArray(row) || !this.policy.fields) return row;
    const hidden = Object.keys(this.policy.fields).filter((field) => Object.hasOwn(row, field) && !this.fieldReadable(field, key, row));
    if (!hidden.length) return row;
    const shown: Record<string, Json> = { ...row };
    for (const field of hidden) delete shown[field];
    return shown;
  }
  rows(rows: Row[], fields: readonly string[] = []): Row[] {
    return rows.filter((row) => this.visible(row.key, row.value, fields)).map((row) => ({ key: row.key, value: this.redact(row.key, row.value) }));
  }
  /** The value to store (undefined deletes), or `skip` to leave the row alone. */
  admit(key: string, previous: Json, next: Json | undefined, clear: readonly string[] = []): Json | undefined | typeof skip {
    const denied = () => Object.assign(new Error(`Access policy denies this write to ${this.collection}`), { code: "ACCESS_DENIED" });
    if (previous === null && next === undefined) return undefined;
    if (next === undefined) {
      if (this.check(this.policy.delete, { key, row: previous })) return undefined;
      // A denied delete of a row the caller can't see acts like deleting a missing key.
      if (!this.check(this.policy.read, { key, row: previous })) return skip;
      throw denied();
    }
    const fields = this.policy.fields ?? {};
    let written = next;
    if (previous !== null && typeof previous === "object" && !Array.isArray(previous) &&
        written !== null && typeof written === "object" && !Array.isArray(written)) {
      const carried: Record<string, Json> = { ...written };
      for (const field of Object.keys(fields)) {
        if (!Object.hasOwn(carried, field) && !clear.includes(field) && Object.hasOwn(previous, field) && !this.fieldReadable(field, key, previous)) carried[field] = previous[field];
      }
      written = carried;
    }
    const stored = previous === null ? undefined : previous;
    const subject: Subject = { key, row: stored, next: written };
    const row = stored === undefined ? this.policy.insert : this.policy.update;
    const allowed = this.check(row, subject) && Object.entries(fields).every(([field, access]) => {
      const before = lookup(stored, [field]);
      const after = lookup(written, [field]);
      const changed = before === undefined || after === undefined ? before !== after : canonicalJson(before) !== canonicalJson(after);
      if (!changed) return true;
      if (access.write !== undefined) return this.check(access.write, subject);
      if (access.read !== undefined) return this.check(access.read, { key, row: stored ?? written });
      return true;
    });
    if (!allowed) throw denied();
    return written;
  }
}

function matches(value: Json, fields: readonly string[], expected: Json): boolean {
  const wanted = fields.length === 1 ? [expected] : expected as Json[];
  return fields.every((field, index) => {
    const actual = lookup(value, [field]);
    return actual !== undefined && canonicalJson(actual) === canonicalJson(wanted[index]);
  });
}

/** A method's context as the server enforces access for its caller, and its definer capability. */
export interface EnforcedAccess {
  /** The raw context (collection references and encoded keys) the method gets. */
  readonly host: Host;
  /**
   * What the server passes mutation computes as their third argument: between elevate(true)
   * and elevate(false), which nest, the method acts with the application's rights.
   */
  readonly elevate: (on: unknown) => null;
  /** How many elevate(true) calls are unmatched: a method must return with none. */
  readonly depth: () => number;
}

/**
 * A method's database context, as the server enforces collection access for its caller.
 * `host` is the raw context (collection references and encoded keys).
 */
export function enforceAccess(
  host: Host,
  collections: readonly { name: string; access?: AccessManifest }[],
  principal: Json,
  definitions: Readonly<Record<string, { readonly kind: string; readonly access?: Json }>> = {},
): EnforcedAccess {
  // Triggers run between elevate(true) and elevate(false), with the application's rights.
  let definer = 0;
  const elevate = (on: unknown) => {
    if (on === true) definer++;
    else if (on === false && definer > 0) definer--;
    else throw Object.assign(new Error(on === false ? "definer(false) without a matching definer(true)" : "definer takes true or false"), { code: "INVALID_VALUE" });
    return null;
  };
  const depth = () => definer;
  const policies = new Map(collections.filter((entry) => entry.access).map((entry) => [entry.name, entry.access!]));
  const derived = new Map(Object.entries(definitions).filter(([, entry]) => entry.kind === "derived" && entry.access !== undefined).map(([name, entry]) => [name, entry.access as RuleJson]));
  if (!policies.size && !derived.size) return { host, elevate, depth };
  const anonymous = principal === null || typeof principal !== "object" || Array.isArray(principal) ||
    (principal as Record<string, Json>).subject === "$anonymous";
  const caller: Json = anonymous
    ? (principal !== null && typeof principal === "object" && !Array.isArray(principal) && "tenant" in principal ? { tenant: principal.tenant } : {})
    : principal;
  // Rules read the clock like ctx.now() and report flips like ctx.changesAt().
  const clock: Clock = {
    now: () => host.now() as number,
    note: (flip) => { if (flip !== undefined) host.changesAt(flip); },
  };
  // `readable` reads the named row with the application's rights, this invocation's
  // writes included, and checks the collection's read rule on it, which can't look
  // further. The raw read makes a result depend on the row.
  const readable: Rows = (collection, key) => {
    const policy = policies.get(collection);
    if (!policy) return false;
    const row = host.get({ kind: "collection", name: collection }, key) as Json;
    return row !== null && row !== undefined && holds(policy.read as RuleJson, caller, { key, row }, clock, noRows);
  };
  const guard = (name: unknown) => {
    const policy = typeof name === "string" && definer === 0 ? policies.get(name) : undefined;
    return policy ? new Guard(name as string, policy, caller, clock, readable) : undefined;
  };
  const name = (reference: any): string | undefined =>
    typeof reference?.collection === "string" ? reference.collection : reference?.collection?.name ?? reference?.name;
  return { elevate, depth, host: Object.freeze({
    ...host,
    get(reference: any, key: any) {
      const rule = reference?.kind === "derived" && definer === 0 ? derived.get(reference.name) : undefined;
      if (rule && !holds(rule, caller, { key: "", row: key ?? null }, clock, noRows)) {
        throw Object.assign(new Error(`Access policy denies reading ${reference.name}`), { code: "ACCESS_DENIED" });
      }
      const value = host.get(reference, key);
      const access = reference?.kind === "collection" ? guard(reference.name) : undefined;
      if (!access || value === null) return value;
      return access.visible(key, value) ? access.redact(key, value) : null;
    },
    scan(reference: any, options?: Record<string, any>) {
      const access = guard(reference?.name);
      if (!access) return options === undefined ? host.scan(reference) : host.scan(reference, options);
      if (options === undefined) return access.rows(host.scan(reference));
      // Skip hidden rows before offset and limit, as the server does.
      const { offset = 0, limit, ...rest } = options;
      const fields = rest.index === undefined ? [] : reference.indexes?.[rest.index] ?? [];
      const rows = access.rows(host.scan(reference, rest), fields).slice(offset);
      return limit === undefined ? rows : rows.slice(0, limit);
    },
    query(reference: any) {
      const values = host.query(reference);
      const access = guard(name(reference));
      if (!access) return values;
      const rows = (host.scan({ kind: "collection", name: name(reference) }) as Row[])
        .filter((row) => matches(row.value, reference.fields, reference.value));
      return access.rows(rows, reference.fields).map((row) => row.value);
    },
    range(reference: any) {
      const access = guard(name(reference));
      if (!access) return host.range(reference);
      const { limit, ...options } = reference.options;
      const all = host.range({ ...reference, options: { ...options, limit: Number.MAX_SAFE_INTEGER } }).rows as Row[];
      const visible = all.map((row, index) => ({ row, index })).filter(({ row }) => access.visible(row.key, row.value, reference.fields));
      const page = visible.slice(0, limit);
      // Continue after the last row returned, found by paging the unfiltered range up to it.
      const cursor = visible.length > limit
        ? host.range({ ...reference, options: { ...options, limit: page[page.length - 1].index + 1 } }).cursor
        : null;
      return { rows: page.map(({ row }) => ({ key: row.key, value: access.redact(row.key, row.value) })), cursor };
    },
    set(reference: any, key: any, value: Json, options?: { clear?: readonly string[] }) {
      const access = guard(reference?.name);
      return host.set(reference, key, access ? access.admit(key, host.get(reference, key), value, options?.clear) as Json : value);
    },
    delete(reference: any, key: any) {
      const access = guard(reference?.name);
      if (access && access.admit(key, host.get(reference, key), undefined) === skip) return null;
      return host.delete(reference, key);
    },
  }) };
}
