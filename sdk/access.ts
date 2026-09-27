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
 * of the row being written (`next`), or the row's key. Missing and null values compare
 * false with everything, so `row("owner").eq(principal.subject)` never matches an
 * anonymous caller.
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
}
export type OperandLike<T> = Operand<T> | T;

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

/** What access rules can refer to. `row` is absent on insert; `next` exists only while writing. */
export interface AccessScope<T> {
  readonly principal: PrincipalFields;
  /** The stored row: in read, update, delete and field rules. */
  readonly row: RowFields<T>;
  /** The row being written: in insert, update and field write rules. */
  readonly next: RowFields<T>;
  /** The row's stored key: the string, or canonical JSON for typed keys. */
  readonly key: Operand<string>;
  all(...rules: RuleLike[]): Rule;
  any(...rules: RuleLike[]): Rule;
  not(rule: RuleLike): Rule;
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

const scope: AccessScope<any> = Object.freeze({
  principal: Object.freeze({
    subject: new OperandNode({ ref: ["principal", "subject"] }),
    tenant: new OperandNode({ ref: ["principal", "tenant"] }),
    claim: (...path: unknown[]) => new OperandNode({ ref: ["principal", "claims", ...segments(path, "claim")] }),
    authenticated: new RuleNode({ exists: { ref: ["principal", "subject"] } }),
  }),
  row: fieldsOf("row"),
  next: fieldsOf("next"),
  key: new OperandNode({ ref: ["key"] }),
  all: (...rules: RuleLike[]) => join("all", rules),
  any: (...rules: RuleLike[]) => join("any", rules),
  not: (rule: RuleLike) => new RuleNode({ not: ruleJson(rule, "not()") }),
});

/** Roots each rule may use, matching the server's checks. */
const roots = {
  read: ["principal", "key", "row"],
  insert: ["principal", "key", "next"],
  update: ["principal", "key", "row", "next"],
  delete: ["principal", "key", "row"],
  fieldRead: ["principal", "key", "row"],
  fieldWrite: ["principal", "key", "row", "next"],
} as const;

function checkRoots(body: unknown, allowed: readonly string[], label: string): void {
  if (Array.isArray(body)) { for (const each of body) checkRoots(each, allowed, label); return; }
  if (body === null || typeof body !== "object") return;
  if (Object.hasOwn(body, "value")) return;
  const ref = (body as { ref?: string[] }).ref;
  if (ref) {
    if (!allowed.includes(ref[0])) {
      const hint = ref[0] === "next" ? "next only exists while writing" : "row doesn't exist yet on insert";
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
type Subject = { key: string; row?: Json; next?: Json };
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

function resolve(operand: OperandJson, caller: Json, subject: Subject): Json | undefined {
  if ("value" in operand) return operand.value;
  const [root, ...path] = operand.ref;
  if (root === "principal") return lookup(caller, path);
  if (root === "key") return subject.key;
  return lookup(root === "row" ? subject.row : subject.next, path);
}

function same(left: Json | undefined, right: Json | undefined): boolean | undefined {
  return present(left) && present(right) ? canonicalJson(left) === canonicalJson(right) : undefined;
}

function holds(rule: RuleJson, caller: Json, subject: Subject): boolean {
  const [operator, body] = Object.entries(rule)[0] as [string, any];
  switch (operator) {
    case "const": return body === true;
    case "all": return (body as RuleJson[]).every((each) => holds(each, caller, subject));
    case "any": return (body as RuleJson[]).some((each) => holds(each, caller, subject));
    case "not": return !holds(body, caller, subject);
    case "eq": return same(resolve(body[0], caller, subject), resolve(body[1], caller, subject)) === true;
    case "ne": return same(resolve(body[0], caller, subject), resolve(body[1], caller, subject)) === false;
    case "in": {
      const item = resolve(body[0], caller, subject);
      const list = resolve(body[1], caller, subject);
      return present(item) && Array.isArray(list) && list.some((each) => same(item, each) === true);
    }
    case "exists": return present(resolve(body, caller, subject));
    default: throw new TypeError(`Unknown access rule ${operator}`);
  }
}

class Guard {
  readonly collection: string;
  readonly policy: AccessManifest;
  readonly caller: Json;
  constructor(collection: string, policy: AccessManifest, caller: Json) {
    this.collection = collection;
    this.policy = policy;
    this.caller = caller;
  }
  private rule(body: Json): RuleJson { return body as RuleJson; }
  private fieldReadable(field: string, key: string, row: Json): boolean {
    const read = this.policy.fields?.[field]?.read;
    return read === undefined || holds(this.rule(read), this.caller, { key, row });
  }
  visible(key: string, row: Json, fields: readonly string[] = []): boolean {
    return holds(this.rule(this.policy.read), this.caller, { key, row }) &&
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
  admit(key: string, previous: Json, next: Json | undefined): Json | undefined {
    const denied = () => Object.assign(new Error(`Access policy denies this write to ${this.collection}`), { code: "ACCESS_DENIED" });
    if (previous === null && next === undefined) return undefined;
    if (next === undefined) {
      if (holds(this.rule(this.policy.delete), this.caller, { key, row: previous })) return undefined;
      throw denied();
    }
    const fields = this.policy.fields ?? {};
    let written = next;
    if (previous !== null && typeof previous === "object" && !Array.isArray(previous) &&
        written !== null && typeof written === "object" && !Array.isArray(written)) {
      const carried: Record<string, Json> = { ...written };
      for (const field of Object.keys(fields)) {
        if (!Object.hasOwn(carried, field) && Object.hasOwn(previous, field) && !this.fieldReadable(field, key, previous)) carried[field] = previous[field];
      }
      written = carried;
    }
    const stored = previous === null ? undefined : previous;
    const subject: Subject = { key, row: stored, next: written };
    const row = this.rule(stored === undefined ? this.policy.insert : this.policy.update);
    const allowed = holds(row, this.caller, subject) && Object.entries(fields).every(([field, access]) => {
      const before = lookup(stored, [field]);
      const after = lookup(written, [field]);
      const changed = before === undefined || after === undefined ? before !== after : canonicalJson(before) !== canonicalJson(after);
      if (!changed) return true;
      if (access.write !== undefined) return holds(this.rule(access.write), this.caller, subject);
      if (access.read !== undefined) return holds(this.rule(access.read), this.caller, { key, row: stored ?? written });
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

/**
 * A method's database context, as the server enforces collection access for its caller.
 * `host` is the raw context (collection references and encoded keys).
 */
export function enforceAccess(host: Host, collections: readonly { name: string; access?: AccessManifest }[], principal: Json): Host {
  const policies = new Map(collections.filter((entry) => entry.access).map((entry) => [entry.name, entry.access!]));
  if (!policies.size) return host;
  const anonymous = principal === null || typeof principal !== "object" || Array.isArray(principal) ||
    (principal as Record<string, Json>).subject === "$anonymous";
  const caller: Json = anonymous
    ? (principal !== null && typeof principal === "object" && !Array.isArray(principal) && "tenant" in principal ? { tenant: principal.tenant } : {})
    : principal;
  const guard = (name: unknown) => {
    const policy = typeof name === "string" ? policies.get(name) : undefined;
    return policy ? new Guard(name as string, policy, caller) : undefined;
  };
  const name = (reference: any): string | undefined =>
    typeof reference?.collection === "string" ? reference.collection : reference?.collection?.name ?? reference?.name;
  return Object.freeze({
    ...host,
    get(reference: any, key: any) {
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
    set(reference: any, key: any, value: Json) {
      const access = guard(reference?.name);
      return host.set(reference, key, access ? access.admit(key, host.get(reference, key), value) : value);
    },
    delete(reference: any, key: any) {
      const access = guard(reference?.name);
      if (access) access.admit(key, host.get(reference, key), undefined);
      return host.delete(reference, key);
    },
  });
}
