import { existsSync, readFileSync } from "node:fs";
import { createContext, runInContext } from "node:vm";
import { buildBundle } from "./bundle.ts";
import { FlowerClient, FlowerError } from "./client.ts";
import type { AliasOf, ApiOf, ArgsOf, ArgsParameter, Failure, FlowerModule, MutationAliasOf, Principal, QueryAliasOf, ResultOf } from "./core.ts";
import { canonicalJson, type Json } from "./json.ts";
import { enforceAccess } from "./access.ts";
import { checkReferences } from "./references.ts";

type Engine = { flowerInvoke(data: Record<string, Json>, invocation: Json, method: Function, cell: Function, now?: number): { puts: Record<string, Json>; deletes: string[]; value: Json } };
type Manifest = FlowerModule<any>;

function engineSource(): string {
  for (const candidate of ["./engine.js", "../runtime/engine.js"]) {
    const url = new URL(candidate, import.meta.url);
    if (existsSync(url)) return readFileSync(url, "utf8");
  }
  throw new Error("Flower's reference engine is missing from this installation");
}

const plain = <T>(value: T): T => value === undefined ? value : JSON.parse(JSON.stringify(value));

function failureOf(error: unknown): Failure {
  const source = error as { code?: unknown; message?: unknown; details?: unknown };
  const code = typeof source?.code === "string" ? source.code : "COMPUTE_ERROR";
  const message = typeof source?.message === "string" ? source.message : String(error);
  return { code, message, ...(source?.details === undefined ? {} : { details: plain(source.details) as Json }) };
}

export interface TestCallOptions {
  credentials?: Json;
  requestId?: string;
}
export interface TestDatabaseOptions {
  /** Initial server time in epoch milliseconds. Default 1_000_000. */
  now?: number;
  /** Credentials sent by the client and by calls that pass none. */
  credentials?: Json;
  /** Named logical databases running the same application, for transactions and partition clients. */
  partitions?: readonly string[];
}

interface Store { data: Record<string, Json>; revision: number; receipts: Map<string, { fingerprint: string; result: { revision: number; value: Json } }> }
interface Watcher { refresh(): void }
type Invoke = (partition: string, alias: string, kind: "query" | "mutation" | null, args: unknown, options: TestCallOptions) => { revision: number; value: Json; duplicate: boolean };
const invokers = new WeakMap<object, Invoke>();

/**
 * An in-process database running an application on Flower's reference engine.
 * Calls behave like HTTP calls, including authorization, receipts and error
 * shapes; maintenance and time advance only when the test says so.
 */
export class TestDatabase<App = FlowerModule> {
  now: number;
  readonly client: FlowerClient<App>;
  private readonly module: Manifest;
  private readonly engine: Engine;
  private readonly stores = new Map<string, Store>();
  private readonly watchers = new Set<Watcher>();
  private readonly credentials: Json | undefined;
  private readonly local: <T>(value: T) => T;
  private sequence = 0;

  constructor(module: Manifest, engine: Engine, options: TestDatabaseOptions = {}, local: <T>(value: T) => T = plain) {
    this.module = module;
    this.engine = engine;
    this.local = local;
    this.now = options.now ?? 1_000_000;
    this.credentials = options.credentials;
    for (const name of ["", ...(options.partitions ?? [])]) this.stores.set(name, { data: {}, revision: 0, receipts: new Map() });
    invokers.set(this, (partition, alias, kind, args, callOptions) => this.invoke(partition, alias, kind, args, callOptions));
    this.client = new FlowerClient<App>("http://flower.test", {
      fetch: (url, init) => this.fetch(url, init),
      ...(options.credentials === undefined ? {} : { credentials: options.credentials }),
    });
  }

  /** Revision of the root database. */
  get revision(): number { return this.store("").revision; }
  /** A copy of the root database's raw state. */
  get data(): Record<string, Json> { return plain(this.store("").data); }

  query<A extends QueryAliasOf<App>>(alias: A, ...rest: [...ArgsParameter<ArgsOf<ApiOf<App>[A]>>, options?: TestCallOptions]): ResultOf<ApiOf<App>[A]> {
    return this.invoke("", alias, "query", rest[0] ?? null, (rest[1] ?? {}) as TestCallOptions).value as ResultOf<ApiOf<App>[A]>;
  }
  mutate<A extends MutationAliasOf<App>>(alias: A, ...rest: [...ArgsParameter<ArgsOf<ApiOf<App>[A]>>, options?: TestCallOptions]): ResultOf<ApiOf<App>[A]> {
    return this.invoke("", alias, "mutation", rest[0] ?? null, (rest[1] ?? {}) as TestCallOptions).value as ResultOf<ApiOf<App>[A]>;
  }
  call<A extends AliasOf<App>>(alias: A, ...rest: [...ArgsParameter<ArgsOf<ApiOf<App>[A]>>, options?: TestCallOptions]): ResultOf<ApiOf<App>[A]> {
    return this.invoke("", alias, null, rest[0] ?? null, (rest[1] ?? {}) as TestCallOptions).value as ResultOf<ApiOf<App>[A]>;
  }

  /** Run maintenance like the leader: one task per commit, while tasks stay due. Returns committed runs. */
  maintain(limit = 10_000, partition = ""): number {
    const maintenance = this.module.maintenance;
    if (!maintenance) return 0;
    let runs = 0;
    while (runs < limit) {
      const store = this.store(partition);
      let output;
      try {
        output = this.run(store.data, "mutation", maintenance.name, null, null, false);
      } catch (error) {
        output = this.run(store.data, "mutation", maintenance.onError.name, { error: failureOf(error) as unknown as Json, failedAt: this.now }, null, false);
      }
      if (Object.keys(output.puts).every((key) => key === "clock") && !output.deletes.length) break;
      this.commit(store, output);
      runs++;
      const hint = output.value as { $flower?: { continue?: boolean } } | null;
      if (hint?.$flower?.continue !== true) break;
    }
    if (runs) this.notify();
    return runs;
  }

  /** Move server time forward, then run due maintenance. */
  advance(ms: number): number {
    if (!Number.isSafeInteger(ms) || ms < 0) throw new TypeError("advance requires a nonnegative safe integer");
    this.now += ms;
    let runs = 0;
    for (const partition of this.stores.keys()) runs += this.maintain(10_000, partition);
    this.notify();
    return runs;
  }

  /** The same application in a named partition, sharing this database's clock. */
  partition(name: string): TestPartition<App> {
    this.store(name);
    return new TestPartition<App>(this, name);
  }

  private invoke(partition: string, alias: string, kind: "query" | "mutation" | null, args: unknown, options: TestCallOptions): { revision: number; value: Json; duplicate: boolean } {
    const store = this.store(partition);
    const entry = this.module.http[alias];
    if (!entry) throw new FlowerError(`Unknown method ${alias}`, 404, "METHOD_NOT_FOUND");
    if (kind !== null && entry.kind !== kind && !(kind === "mutation" && entry.kind === "transaction")) {
      throw new FlowerError(`${alias} is not a ${kind}`, 422, "METHOD_KIND_MISMATCH");
    }
    canonicalJson(args);
    const principal = this.authorize(store, alias, args as Json, options, partition, null);
    if (entry.kind === "query") return { revision: store.revision, value: plain(this.evaluate(store, "query", entry.name, args as Json, principal).value), duplicate: false };
    const requestId = options.requestId ?? `test-${++this.sequence}`;
    const fingerprint = canonicalJson([alias, args as Json, principal?.subject ?? null, principal?.tenant ?? null]);
    const receipt = store.receipts.get(requestId);
    if (receipt) {
      if (receipt.fingerprint !== fingerprint) throw new FlowerError("requestId was already used for different content", 409, "REQUEST_ID_REUSED");
      return { ...plain(receipt.result), duplicate: true };
    }
    if (entry.kind === "transaction") {
      const { revision, value } = this.transaction(store, entry.name, args as Json, principal);
      store.receipts.set(requestId, { fingerprint, result: { revision, value } });
      return { revision, value: plain(value), duplicate: false };
    }
    const output = this.evaluate(store, "mutation", entry.name, args as Json, principal);
    this.commit(store, output);
    const result = { revision: store.revision, value: plain(output.value) };
    // As on a server: no receipt for a call that wrote nothing, or for a
    // method declared with receipt: false, outside retry sessions.
    const wrote = output.deletes.length > 0 || Object.keys(output.puts).some((key) => key !== "clock");
    if ((wrote && entry.receipt !== false) || requestId.startsWith("f2:")) store.receipts.set(requestId, { fingerprint, result });
    this.notify();
    return { ...plain(result), duplicate: false };
  }

  private store(name: string): Store {
    const store = this.stores.get(name);
    if (!store) throw new FlowerError(`Unknown partition ${name}`, 404, "PARTITION_NOT_FOUND");
    return store;
  }

  private authorize(store: Store, alias: string, args: Json, options: TestCallOptions, partition: string, delegation: Json): Principal | null {
    const hook = this.module.authorize;
    if (!hook) return (delegation as { principal?: Principal } | null)?.principal ?? null;
    const credentials = options.credentials !== undefined ? options.credentials : this.credentials ?? null;
    let principal: Principal;
    try {
      const value = plain(this.run(store.data, "query", hook.name,
        { credentials, method: alias, args, partition: partition || null, delegation } as unknown as Json, null, false).value) as Record<string, unknown> | null;
      // A decision carries the principal beside whether it read the arguments (the server reuses those that did not).
      principal = (hook.result === "decision" ? value?.principal : value) as Principal;
    } catch (error) {
      throw new FlowerError("Authorization denied", 403, "FORBIDDEN", failureOf(error));
    }
    if (principal === null || typeof principal !== "object" || typeof principal.subject !== "string" || !principal.subject) {
      throw new FlowerError("Authorization returned an invalid principal", 403, "FORBIDDEN");
    }
    if (partition && principal.tenant !== partition) throw new FlowerError("Principal is not authorized for this partition", 403, "FORBIDDEN");
    return principal;
  }

  private evaluate(store: Store, kind: "query" | "mutation", name: string, args: Json, principal: Principal | null) {
    try { return this.run(store.data, kind, name, args, principal); }
    catch (error) {
      const failure = failureOf(error);
      throw new FlowerError(`${failure.code}: ${failure.message}`, 422, "EVALUATION_FAILED", failure);
    }
  }

  /** `caller` is false for maintenance and the authorization hook, which see every row. */
  private run(data: Record<string, Json>, kind: "query" | "mutation", name: string, args: Json, principal: Principal | null, caller = true) {
    const definitions = this.module.definitions as Record<string, any>;
    const identity = principal === null ? null : this.local(principal);
    // References held in fields have an index of them, as on the server.
    const declared = new Set((this.module.collections ?? []).flatMap((entry) =>
      [...Object.values(entry.indexes), ...(entry.references ?? []).flatMap((reference) => reference.fields ? [reference.fields] : [])]
        .map((fields) => canonicalJson([entry.name, [...fields]]))));
    const indexed = (collection: string, fields: readonly string[]) => {
      if (!declared.has(canonicalJson([collection, [...fields]]))) {
        throw Object.assign(new Error(`Index [${fields.join(", ")}] on ${collection} is not declared in define(); the server would scan the whole collection`),
          { code: "UNDECLARED_INDEX" });
      }
    };
    const withIdentity = (ctx: any) => Object.freeze({
      ...ctx,
      principal: () => identity,
      history: ctx.history ?? (() => null),
      range: (reference: any) => { indexed(reference.collection, reference.fields); return ctx.range(reference); },
      query: (reference: any) => { indexed(typeof reference.collection === "string" ? reference.collection : reference.collection.name, reference.fields); return ctx.query(reference); },
      scan: (reference: any, options?: any) => {
        if (options?.index !== undefined) indexed(reference.name, reference.indexes?.[options.index] ?? [options.index]);
        return options === undefined ? ctx.scan(reference) : ctx.scan(reference, options);
      },
    });
    const method = (method: string, input: Json, ctx: any) => {
      if (!Object.hasOwn(definitions, method)) throw Object.assign(new Error(`Unknown definition ${method}`), { code: "DEFINITION_MISSING" });
      // Methods see collections as their caller may, like on the server.
      const host = withIdentity(ctx);
      const enforced = caller ? enforceAccess(host, this.module.collections ?? [], identity as unknown as Json, definitions) : enforceAccess(host, [], null);
      // Like the server's runner: only mutations get the definer capability, as a third
      // argument no context carries, and must return with it balanced.
      if (kind !== "mutation") return definitions[method].compute(enforced.host, input);
      const value = definitions[method].compute(enforced.host, input, enforced.elevate);
      if (enforced.depth() !== 0) {
        throw Object.assign(new Error(`${method} returned acting as the definer: definer(true) outnumbers definer(false) by ${enforced.depth()}`),
          { code: "DEFINER_UNBALANCED" });
      }
      return value;
    };
    const cell = (cellName: string, input: Json, ctx: any) => {
      const definition = definitions[cellName];
      if (!definition) throw Object.assign(new Error(`Unknown derived definition: ${cellName}`), { code: "DEFINITION_MISSING" });
      if (!definition.aggregate) return definition.compute(withIdentity(ctx), input);
      const { collection, fields } = definition.aggregate as { collection: string; fields: string[] };
      const group = fields.length === 1 ? [input] : input as Json[];
      const changes = (ctx.scan({ kind: "collection", name: collection }) as { key: string; value: Record<string, Json> }[])
        .filter(({ value }) => value !== null && typeof value === "object" &&
          fields.every((field, index) => Object.hasOwn(value, field) && canonicalJson(value[field]) === canonicalJson(group[index])))
        .map(({ key, value }) => ({ key, new: value }));
      return definition.compute(undefined, { initialize: true, group: input, previous: null, changes });
    };
    const output = this.engine.flowerInvoke(data, { kind, name, args, requestId: `test-${++this.sequence}` } as unknown as Json, method, cell, this.now);
    // As the server does at the end of every mutation: rows refer only to rows that exist.
    if (kind === "mutation") checkReferences(this.module.collections ?? [], data, output.puts, output.deletes);
    return output;
  }

  private commit(store: Store, output: { puts: Record<string, Json>; deletes: string[] }) {
    const data = { ...store.data, ...plain(output.puts) };
    for (const key of output.deletes) delete data[key];
    store.data = data;
    store.revision++;
  }

  private transaction(store: Store, name: string, args: Json, principal: Principal | null) {
    const plan = plain(this.evaluate(store, "query", name, args, principal).value) as { calls: { partition?: string; group?: string; method: string; args?: Json }[]; value?: Json };
    const saved = new Map([...this.stores].map(([key, value]) => [key, { ...value, data: value.data }]));
    try {
      const results: Json[] = [];
      for (const call of plan.calls) {
        const target = call.partition ?? call.group ?? "";
        const participant = this.store(target);
        const entry = this.module.http[call.method];
        if (!entry || entry.kind === "transaction") throw new FlowerError(`Unknown participant method ${call.method}`, 422, "TRANSACTION_ABORTED");
        const delegated = this.authorize(participant, call.method, call.args ?? null, {}, target, { coordinator: "test", principal } as unknown as Json);
        const output = this.evaluate(participant, entry.kind, entry.name, call.args ?? null, delegated);
        if (entry.kind === "mutation") this.commit(participant, output);
        results.push(plain(output.value));
      }
      // Like the server, a plan without a value commits to { results } alone.
      const value = Object.hasOwn(plan, "value") ? { results, value: plan.value } : { results };
      store.revision++;
      this.notify();
      return { revision: store.revision, value: value as unknown as Json, duplicate: false };
    } catch (error) {
      for (const [key, value] of saved) this.stores.set(key, value);
      if (error instanceof FlowerError) throw new FlowerError(error.message, 422, "TRANSACTION_ABORTED", error.failure);
      throw error;
    }
  }

  private notify() { for (const watcher of this.watchers) watcher.refresh(); }

  /** A FlowerFetch transport serving /v1 calls and SSE watches from this database. */
  async fetch(url: string, init: { body: string; signal?: AbortSignal }): Promise<Response> {
    const path = new URL(url).pathname;
    const match = /^(?:\/partitions\/([^/]+))?\/v1\/(query|mutate|call|watch|identity|session)$/.exec(path);
    const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
    const failed = (error: unknown) => {
      if (!(error instanceof FlowerError)) throw error;
      return json({ error: { code: error.code, message: error.message, ...(error.failure ? { failure: error.failure } : {}) } }, error.status);
    };
    if (!match) return json({ error: { code: "NOT_FOUND", message: `No route ${path}` } }, 404);
    const partition = match[1] === undefined ? "" : decodeURIComponent(match[1]);
    const input = JSON.parse(init.body);
    const options: TestCallOptions = { ...(input.credentials === undefined ? {} : { credentials: input.credentials }), ...(input.requestId === undefined ? {} : { requestId: input.requestId }) };
    if (match[2] === "identity") return json({ revision: this.store(partition).revision, value: null });
    if (match[2] === "session") return json({ error: { code: "RETENTION_NOT_INITIALIZED", message: "Retry sessions are not simulated" } }, 409);
    if (match[2] !== "watch") {
      try {
        return json(this.invoke(partition, input.name, match[2] === "query" ? "query" : match[2] === "mutate" ? "mutation" : null, input.args ?? null, options));
      } catch (error) { return failed(error); }
    }
    return this.watch(partition, input.name, input.args ?? null, options, init.signal);
  }

  private watch(partition: string, alias: string, args: Json, options: TestCallOptions, signal?: AbortSignal): Response {
    const encoder = new TextEncoder();
    let sequence = -1, previous: string | undefined;
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const watcher: Watcher = {
      refresh: () => {
        try {
          const result = this.invoke(partition, alias, "query", args, options);
          const encoded = canonicalJson(result.value);
          if (encoded === previous) return;
          previous = encoded;
          sequence++;
          controller.enqueue(encoder.encode(`event: snapshot\nid: ${sequence}\ndata: ${JSON.stringify({ sequence, revision: result.revision, value: result.value })}\n\n`));
        } catch (error) {
          const status = error instanceof FlowerError ? error.status : 500;
          const failure = error instanceof FlowerError ? error.failure : undefined;
          controller.enqueue(encoder.encode(`event: error\ndata: ${JSON.stringify({ error: { code: (error as FlowerError).code ?? "INTERNAL", message: String((error as Error).message), status, ...(failure ? { failure } : {}) } })}\n\n`));
          close();
        }
      },
    };
    const close = () => {
      if (!this.watchers.delete(watcher)) return;
      try { controller.close(); } catch { }
    };
    const body = new ReadableStream<Uint8Array>({
      start: (control) => { controller = control; },
      cancel: () => { close(); },
    });
    this.watchers.add(watcher);
    signal?.addEventListener("abort", close, { once: true });
    // The caller may abort before the first snapshot.
    queueMicrotask(() => { if (this.watchers.has(watcher)) watcher.refresh(); });
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
  }
}

/** A named partition of a TestDatabase. */
export class TestPartition<App = FlowerModule> {
  readonly client: FlowerClient<App>;
  private readonly database: TestDatabase<App>;
  private readonly name: string;
  constructor(database: TestDatabase<App>, name: string) {
    this.database = database;
    this.name = name;
    this.client = database.client.partition(name);
  }
  query<A extends QueryAliasOf<App>>(alias: A, ...rest: [...ArgsParameter<ArgsOf<ApiOf<App>[A]>>, options?: TestCallOptions]): ResultOf<ApiOf<App>[A]> {
    return invokers.get(this.database)!(this.name, alias, "query", rest[0] ?? null, (rest[1] ?? {}) as TestCallOptions).value as ResultOf<ApiOf<App>[A]>;
  }
  mutate<A extends MutationAliasOf<App>>(alias: A, ...rest: [...ArgsParameter<ArgsOf<ApiOf<App>[A]>>, options?: TestCallOptions]): ResultOf<ApiOf<App>[A]> {
    return invokers.get(this.database)!(this.name, alias, "mutation", rest[0] ?? null, (rest[1] ?? {}) as TestCallOptions).value as ResultOf<ApiOf<App>[A]>;
  }
  call<A extends AliasOf<App>>(alias: A, ...rest: [...ArgsParameter<ArgsOf<ApiOf<App>[A]>>, options?: TestCallOptions]): ResultOf<ApiOf<App>[A]> {
    return invokers.get(this.database)!(this.name, alias, null, rest[0] ?? null, (rest[1] ?? {}) as TestCallOptions).value as ResultOf<ApiOf<App>[A]>;
  }
  maintain(limit?: number): number { return this.database.maintain(limit, this.name); }
}

/**
 * Start an in-process database. Pass an imported define(...) module, or a path
 * to bundle and run it in an isolated context like the server does.
 */
export async function testDatabase<App extends Manifest = Manifest>(app: App | string, options: TestDatabaseOptions = {}): Promise<TestDatabase<App>> {
  const source = engineSource();
  if (typeof app === "string") {
    const bundle = await buildBundle(app);
    const sandbox = createContext(Object.create(null));
    runInContext(bundle.javascript, sandbox, { timeout: 10_000 });
    runInContext(source, sandbox, { timeout: 10_000 });
    // A context's globals are not properties of its contextified object; read JSON from inside.
    const contextJSON = runInContext("JSON", sandbox) as typeof JSON;
    const local = <T>(value: T): T => contextJSON.parse(JSON.stringify(value));
    return new TestDatabase<App>(sandbox.__flowerBundle.default, sandbox as unknown as Engine, options, local);
  }
  const scope = Object.create(null) as Engine;
  new Function("globalThis", source)(scope);
  return new TestDatabase<App>(app, scope, options);
}

