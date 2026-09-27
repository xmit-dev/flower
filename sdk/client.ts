import { canonicalJson, type Json } from "./json.ts";
import type { AliasOf, ApiOf, ArgsOf, ArgsParameter, Failure, FlowerModule, MutationAliasOf, QueryAliasOf, ResultOf } from "./core.ts";
import type { ManagedKeyAlgorithm, KeyUsage } from "./keys.ts";
import { applyWatchPatch, cloneWatchValue, controlledWatch, decodeWatchEvent, readSse, watchBudgets, WatchProtocolError } from "./watch.ts";
import type { WatchBudgets, WatchDelta } from "./watch.ts";

export interface JavaScriptBundle { hash: string; javascript: string }
/** A guest module implementing GUEST_ABI.md, such as one built with the Rust flower-sdk crate. */
export interface WasmBundle { hash: string; wasm: string }
/** SHA-256 hex of the JavaScript text, or of the module bytes base64-encoded in `wasm`. */
export type Bundle = JavaScriptBundle | WasmBundle;
/** The JSON POST subset used by Flower; custom transports need not implement general fetch. */
export interface FlowerRequestInit { method: "POST"; headers: Record<string, string>; body: string; signal?: AbortSignal }
export type FlowerFetch = (url: string, init: FlowerRequestInit) => Promise<Response>;

export interface RetryPolicy {
  /** Attempts including the first. Default 8. */
  readonly attempts?: number;
  /** No retry starts after this epoch-millisecond deadline; the first attempt always runs. */
  readonly until?: number;
  /** Default 250 ms, doubling with jitter. */
  readonly initialDelayMs?: number;
  /** Default 30000 ms. */
  readonly maxDelayMs?: number;
  /** Abort each attempt after this long. Default 20000 ms, room for a fresh read fence and a full evaluation. */
  readonly timeoutMs?: number;
  /** Defaults to isTransient. */
  readonly retryable?: (error: unknown) => boolean;
}
export interface RequestOptions {
  signal?: AbortSignal;
  credentials?: Json;
  /** Retry transient failures. Mutations reuse one request ID, so a lost reply cannot apply twice. */
  retry?: RetryPolicy | boolean;
}
export interface MutationOptions extends RequestOptions { requestId?: string; expectedRevision?: number }
export interface QueryResult<Value = Json> { revision: number; value: Value }
export interface MutationResult<Value = Json> extends QueryResult<Value> { duplicate: boolean }
export interface WatchOptions extends WatchBudgets { signal?: AbortSignal; credentials?: Json }
export interface WatchPollOptions extends RequestOptions { /** Integer milliseconds in 1..2147483647. Default: 250. */ intervalMs?: number }
export interface SubscribeOptions extends WatchOptions {
  /** Reconnect after disconnects, stalls and transient errors. Default true. */
  reconnect?: boolean | { readonly initialDelayMs?: number; readonly maxDelayMs?: number };
  /** Reconnect when no bytes, including heartbeats, arrive for this long. Default 45000. */
  stallMs?: number;
  /** Skip values older than the newest revision already delivered, e.g. from a lagging replica. */
  monotonic?: boolean;
}
export interface Update<Value = Json> {
  revision: number;
  value: Value;
  /** A full snapshot after (re)connecting; intermediate values may have been skipped. */
  reset: boolean;
}
export interface FlowerClientOptions {
  /** Use epoch-scoped retry IDs. Enable server retention first; expired IDs are never silently renewed. */
  boundedRetries?: boolean;
  /** Evaluated for each call and connection; refreshed credentials keep the same business intent. */
  credentials?: Json | (() => Json | Promise<Json>);
  fetch?: FlowerFetch;
  /** Round-robin endpoints for queries and new subscriptions. Mutations use the primary URL. */
  queryUrls?: readonly string[];
  /** Default retry policy for every request; per-call retry overrides it. */
  retry?: RetryPolicy | boolean;
}

export class FlowerError extends Error {
  readonly status: number;
  readonly code: string;
  /** The method's own failure: fail(code, message, details) or a runtime evaluation error. */
  readonly failure?: Failure;
  constructor(message: string, status = 0, code = "FLOWER_ERROR", failure?: Failure) {
    super(message);
    this.name = "FlowerError";
    this.status = status;
    this.code = code;
    if (failure !== undefined) this.failure = failure;
  }
}

/** Network failures, timeouts, stalls, 408, 425, 429 and 5xx; never a method's own failure. */
export function isTransient(error: unknown): boolean {
  if (error instanceof FlowerError) {
    if (error.failure !== undefined) return false;
    if (error.status === 0) return error.code === "WATCH_STALLED" || error.code === "WATCH_ENDED";
    return [408, 425, 429].includes(error.status) || error.status >= 500;
  }
  if (error instanceof DOMException) return error.name === "TimeoutError" || error.name === "NetworkError";
  const coded = error as { code?: unknown; cause?: { code?: unknown } } | null;
  const code = typeof coded?.code === "string" ? coded.code : coded?.cause?.code;
  if (typeof code === "string") return transientCodes.test(code) && !permanentCodes.has(code);
  return error instanceof TypeError && /fetch|network|load failed/i.test(error.message);
}
const transientCodes = /^(?:H2_|UND_ERR_|ERR_HTTP2_|ECONNRESET$|ECONNREFUSED$|ECONNABORTED$|EPIPE$|ETIMEDOUT$|EHOSTUNREACH$|ENETUNREACH$|ENETDOWN$|EAI_AGAIN$)/;
const permanentCodes = new Set(["H2_REQUEST_TOO_LARGE", "H2_UNSUPPORTED_ENCODING", "H2_RESPONSE_TOO_LARGE"]);

function failureOf(value: unknown): Failure | undefined {
  if (value === null || typeof value !== "object") return undefined;
  const { code, message, details } = value as Record<string, unknown>;
  if (typeof code !== "string" || typeof message !== "string") return undefined;
  return Object.freeze({ code, message, ...(details === undefined ? {} : { details: details as Json }) });
}

/** The value after one watch event; invalid patches fail like other protocol violations. */
function advance(baseline: Json, event: WatchDelta, budgets: Required<WatchBudgets>): Json {
  if (event.type === "snapshot") return cloneWatchValue(event.value);
  try { return applyWatchPatch(baseline, event.patch, budgets); }
  catch (error) { throw error instanceof WatchProtocolError ? new FlowerError(error.message, 0, "WATCH_PROTOCOL_ERROR") : error; }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const done = () => { clearTimeout(timer); signal?.removeEventListener("abort", abort); };
    const abort = () => { done(); reject(signal!.reason); };
    const timer = setTimeout(() => { done(); resolve(); }, ms);
    signal?.addEventListener("abort", abort, { once: true });
  });
}

/** Jittered exponential backoff between half and all of initial·2^attempt, capped. */
export function backoff(attempt: number, initialDelayMs = 250, maxDelayMs = 30_000): number {
  const ceiling = Math.min(maxDelayMs, initialDelayMs * 2 ** Math.min(attempt, 30));
  return Math.round(ceiling / 2 + Math.random() * ceiling / 2);
}

function httpUrl(address: unknown, label: string): string {
  if (typeof address !== "string") throw new TypeError(`${label} must be an HTTP or HTTPS URL`);
  const parsed = new URL(address);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new TypeError(`${label} must use HTTP or HTTPS`);
  return address.replace(/\/+$/, "");
}

const monotonic = () => globalThis.performance?.now() ?? Date.now();

class Connection {
  readonly url: string;
  readonly fetch: FlowerFetch;
  readonly adminToken: string | undefined;
  readonly boundedRetries: boolean;
  readonly identity: { pending?: Promise<RetryIdentity>; fetchedAt?: number } = {};
  constructor(url: string, fetch: FlowerFetch, adminToken: string | undefined, boundedRetries: boolean) {
    this.url = url;
    this.fetch = fetch;
    this.adminToken = adminToken;
    this.boundedRetries = boundedRetries;
  }

  async request<T>(path: string, body: unknown, options: { admin?: boolean; signal?: AbortSignal; url?: string } = {}): Promise<T> {
    canonicalJson(body);
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (options.admin && this.adminToken) headers.authorization = `Bearer ${this.adminToken}`;
    const response = await this.fetch((options.url ?? this.url) + path, { method: "POST", headers, body: JSON.stringify(body), signal: options.signal });
    const text = await response.text();
    let data: any;
    try { data = text ? JSON.parse(text) : null; } catch { data = null; }
    if (!response.ok) {
      const message = typeof data?.error === "string" ? data.error : data?.error?.message ?? data?.message ?? (text || response.statusText);
      throw new FlowerError(message, response.status, data?.error?.code ?? data?.code ?? "HTTP_ERROR", failureOf(data?.error?.failure));
    }
    return data as T;
  }

  refreshRetryIdentity(options: { signal?: AbortSignal } = {}): Promise<RetryIdentity> {
    const pending = this.request<QueryResult<RetryIdentity | null>>("/v1/identity", {}, { signal: options.signal }).then(({ value }) => {
      if (!value) throw new FlowerError("Retry retention is not initialized", 409, "RETENTION_NOT_INITIALIZED");
      if (!/^[a-f0-9]{32}$/.test(value.database) || !/^[a-f0-9]{32}$/.test(value.incarnation) ||
          !Number.isSafeInteger(value.currentEpoch) || !Number.isSafeInteger(value.minEpoch) ||
          value.minEpoch < 0 || value.currentEpoch < value.minEpoch ||
          (value.epochMs != null && (!Number.isSafeInteger(value.epochMs) || value.epochMs < 1))) {
        throw new FlowerError("Invalid retry identity", 0, "PROTOCOL_ERROR");
      }
      return Object.freeze({ ...value });
    });
    this.identity.pending = pending;
    this.identity.fetchedAt = monotonic();
    pending.catch(() => { if (this.identity.pending === pending) this.identity.pending = undefined; });
    return pending;
  }

  /**
   * The identity new IDs use. When the leader rotates epochs, it is fetched
   * again a quarter of the way through one, so new IDs stay admissible for
   * most of the retry window.
   */
  async currentIdentity(options: { signal?: AbortSignal } = {}): Promise<RetryIdentity> {
    const pending = this.identity.pending;
    if (pending) {
      const identity = await pending;
      if (identity.epochMs == null || monotonic() - this.identity.fetchedAt! < identity.epochMs / 4) return identity;
      // Another call may have started the refresh meanwhile.
      if (this.identity.pending !== pending && this.identity.pending) return this.currentIdentity(options);
    }
    return this.refreshRetryIdentity(options);
  }

  async newRequestId(intent: string = crypto.randomUUID(), options: { signal?: AbortSignal } = {}): Promise<string> {
    const identity = await this.currentIdentity(options);
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(intent));
    const hash = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
    return `f1:${identity.database}:${identity.incarnation}:${identity.currentEpoch}:${hash}`;
  }

  async requestId(requested: string | undefined, options: { signal?: AbortSignal } = {}): Promise<string> {
    return requested ?? (this.boundedRetries ? this.newRequestId(undefined, options) : crypto.randomUUID());
  }

  scoped(path: string): Connection {
    // Each logical database has its own retry identity.
    return new Connection(this.url + path, this.fetch, this.adminToken, this.boundedRetries);
  }
}

function connection(url: string, options: { fetch?: FlowerFetch; adminToken?: string; boundedRetries?: boolean }): Connection {
  return new Connection(httpUrl(url, "Flower URL"), options.fetch ?? ((target, init) => globalThis.fetch(target, init)), options.adminToken, options.boundedRetries ?? false);
}

function partitionPath(name: string): string {
  if (typeof name !== "string" || !name.trim() || /[\u0000-\u001f\u007f]/u.test(name)) {
    throw new TypeError("Partition name must be nonempty and contain no control characters");
  }
  return "/partitions/" + encodeURIComponent(name);
}

type Args<App, A extends string> = ArgsParameter<ArgsOf<ApiOf<App>[A]>>;
type NullaryQueryAlias<App> = { [K in QueryAliasOf<App>]: null extends ArgsOf<ApiOf<App>[K]> ? K : never }[QueryAliasOf<App>];
type Value<App, A extends string> = ResultOf<ApiOf<App>[A]>;

/**
 * Invoke an application's public methods. Pass the module type for typed aliases,
 * arguments and results: new FlowerClient<typeof app>(url).
 */
export class FlowerClient<App = FlowerModule> {
  readonly url: string;
  private readonly connection: Connection;
  private readonly queryUrls: readonly string[];
  private nextQuery = 0;
  private readonly credentials: FlowerClientOptions["credentials"];
  private readonly retry: RetryPolicy | boolean;

  constructor(url = "http://127.0.0.1:7101", options: FlowerClientOptions = {}) {
    this.connection = connection(url, options);
    this.url = this.connection.url;
    if (options.queryUrls !== undefined && (!Array.isArray(options.queryUrls) || options.queryUrls.length === 0)) {
      throw new TypeError("queryUrls must be a nonempty array of HTTP or HTTPS URLs");
    }
    this.queryUrls = Object.freeze(Array.from(options.queryUrls ?? [this.url], (address) => httpUrl(address, "queryUrls entry")));
    this.credentials = options.credentials;
    this.retry = options.retry ?? false;
  }

  private queryUrl(): string {
    const address = this.queryUrls[this.nextQuery];
    this.nextQuery = (this.nextQuery + 1) % this.queryUrls.length;
    return address;
  }

  private async authorization(options: { credentials?: Json }): Promise<Record<string, Json>> {
    const credentials = options.credentials !== undefined ? options.credentials :
      typeof this.credentials === "function" ? await this.credentials() : this.credentials;
    if (credentials === undefined) return {};
    canonicalJson(credentials);
    return { credentials };
  }

  private async attempt<T>(options: RequestOptions, send: (signal: AbortSignal | undefined) => Promise<T>): Promise<T> {
    const setting = options.retry ?? this.retry;
    if (setting === false) return send(options.signal);
    const policy: RetryPolicy = setting === true ? {} : setting;
    const attempts = policy.attempts ?? 8, timeoutMs = policy.timeoutMs ?? 20_000;
    const retryable = policy.retryable ?? isTransient;
    for (let attempt = 0; ; attempt++) {
      options.signal?.throwIfAborted();
      const timeout = AbortSignal.timeout(timeoutMs);
      try { return await send(options.signal ? AbortSignal.any([options.signal, timeout]) : timeout); }
      catch (error) {
        if (options.signal?.aborted || !retryable(error) || attempt + 1 >= attempts) throw error;
        const delay = backoff(attempt, policy.initialDelayMs, policy.maxDelayMs);
        if (policy.until !== undefined && Date.now() + delay >= policy.until) throw error;
        await sleep(delay, options.signal);
      }
    }
  }

  private split(name: string, rest: unknown[]): [Json, RequestOptions & MutationOptions & SubscribeOptions & WatchPollOptions] {
    const args = (rest[0] ?? null) as Json;
    canonicalJson({ name, args });
    return [args, (rest[1] ?? {}) as RequestOptions & MutationOptions];
  }

  /** Invoke a read-only method on the next query endpoint. */
  async query<A extends QueryAliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: RequestOptions]): Promise<QueryResult<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    return this.attempt(options, async (signal) => this.connection.request("/v1/query",
      { name: alias, args, ...await this.authorization(options) }, { signal, url: this.queryUrl() }));
  }

  /** Invoke an atomic method. Retries keep one request ID and return the original result. */
  async mutate<A extends MutationAliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: MutationOptions]): Promise<MutationResult<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    return this.invoke("/v1/mutate", alias, args, options);
  }

  /** Invoke any public alias; deployed code decides whether it is a query, mutation or transaction. */
  async call<A extends AliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: MutationOptions]): Promise<MutationResult<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    return this.invoke("/v1/call", alias, args, options);
  }

  private async invoke<T>(path: string, name: string, args: Json, options: MutationOptions): Promise<T> {
    const requestId = await this.connection.requestId(options.requestId, options);
    return this.attempt(options, async (signal) => {
      const request: Record<string, unknown> = { name, args, requestId, ...await this.authorization(options) };
      if (options.expectedRevision !== undefined) request.expectedRevision = options.expectedRevision;
      return this.connection.request<T>(path, request, { signal });
    });
  }

  /** Stream a query's values over SSE. Equal values are suppressed; the stream ends on disconnect. */
  watch<A extends QueryAliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: WatchOptions]): AsyncGenerator<QueryResult<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    const client = this;
    const budgets = watchBudgets(options);
    return controlledWatch(options.signal, async function* (signal) {
      let baseline: Json = null;
      for await (const event of client.deltas(alias, args, { ...options, ...budgets, signal })) {
        baseline = advance(baseline, event, budgets);
        yield { revision: event.revision, value: cloneWatchValue(baseline) as Value<App, A> };
      }
    });
  }

  /** Raw SSE snapshot and patch events. Each connection starts with a full snapshot. */
  watchDeltas<A extends QueryAliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: WatchOptions]): AsyncGenerator<WatchDelta<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    return this.deltas(alias, args, options) as AsyncGenerator<WatchDelta<Value<App, A>>>;
  }

  private deltas(name: string, args: Json, options: WatchOptions & { onActivity?: () => void }): AsyncGenerator<WatchDelta> {
    const client = this;
    const budgets = watchBudgets(options);
    return controlledWatch(options.signal, async function* (signal) {
      if ((options as { intervalMs?: unknown }).intervalMs !== undefined) throw new TypeError("intervalMs applies only to watchPoll(); watches use SSE");
      const response = await client.connection.fetch(client.queryUrl() + "/v1/watch", {
        method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream" },
        body: JSON.stringify({ name, args, ...await client.authorization(options) }), signal,
      });
      try {
        if (!response.ok) {
          const reader = response.body?.getReader();
          const chunks: Uint8Array[] = [];
          let bytes = 0;
          // Abort and stall detection must also interrupt a stalled error body.
          const cancel = () => { void reader?.cancel(signal.reason).catch(() => {}); };
          if (reader) try {
            signal.addEventListener("abort", cancel, { once: true });
            if (signal.aborted) cancel();
            while (true) {
              const next = await reader.read();
              if (next.done) break;
              if ((bytes += next.value.byteLength) > budgets.maxEventBytes) throw new FlowerError("Watch HTTP error body exceeds maxEventBytes", response.status, "HTTP_ERROR");
              chunks.push(next.value);
            }
          } finally { signal.removeEventListener("abort", cancel); void reader.cancel().catch(() => {}); reader.releaseLock(); }
          const buffer = new Uint8Array(bytes);
          let offset = 0;
          for (const chunk of chunks) { buffer.set(chunk, offset); offset += chunk.length; }
          const text = new TextDecoder().decode(buffer);
          let value: any;
          try { value = JSON.parse(text); } catch { value = null; }
          throw new FlowerError(value?.error?.message ?? (text || response.statusText), response.status, value?.error?.code ?? "HTTP_ERROR", failureOf(value?.error?.failure));
        }
        if (response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() !== "text/event-stream" || !response.body) {
          throw new FlowerError("Expected a text/event-stream response body", response.status, "WATCH_PROTOCOL_ERROR");
        }
        let sequence = -1, revision = -1;
        for await (const frame of readSse(response.body, signal, budgets.maxEventBytes, options.onActivity)) {
          const event = decodeWatchEvent(frame, sequence, revision, budgets);
          if (event.type === "error") {
            throw new FlowerError(event.error.message, event.error.status, event.error.code, failureOf((event.error as { failure?: unknown }).failure));
          }
          sequence = event.sequence; revision = event.revision;
          yield event;
        }
        if (!signal.aborted && sequence < 0) throw new FlowerError("Watch ended before its initial snapshot", 0, "WATCH_ENDED");
      } catch (error) {
        if (error instanceof WatchProtocolError) throw new FlowerError(error.message, 0, "WATCH_PROTOCOL_ERROR");
        throw error;
      } finally { if (!response.body?.locked) void response.body?.cancel().catch(() => {}); }
    });
  }

  /**
   * A live value that survives disconnects: it reconnects with backoff, rotates
   * query endpoints, detects silent stalls and marks each fresh snapshot as a reset.
   */
  subscribe<A extends QueryAliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: SubscribeOptions]): AsyncGenerator<Update<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    const client = this;
    const budgets = watchBudgets(options);
    const reconnect = options.reconnect ?? true;
    const delays = typeof reconnect === "object" ? reconnect : {};
    const stallMs = options.stallMs ?? 45_000;
    if (!Number.isSafeInteger(stallMs) || stallMs < 1) throw new TypeError("stallMs must be a positive safe integer");
    return controlledWatch(options.signal, async function* (signal) {
      let failures = 0, newest = -1;
      while (!signal.aborted) {
        const connection = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        // Clear the stall timer too: an aborted subscription must not keep the process alive.
        const stop = () => { clearTimeout(timer); connection.abort(signal.reason); };
        signal.addEventListener("abort", stop, { once: true });
        const touch = () => {
          clearTimeout(timer);
          timer = setTimeout(() => connection.abort(new FlowerError("Watch stalled", 0, "WATCH_STALLED")), stallMs);
        };
        touch();
        let reset = true, baseline: Json = null;
        try {
          for await (const event of client.deltas(alias, args, { ...options, ...budgets, signal: connection.signal, onActivity: touch })) {
            baseline = advance(baseline, event, budgets);
            failures = 0;
            if (options.monotonic && event.revision < newest) continue;
            newest = Math.max(newest, event.revision);
            yield { revision: event.revision, value: cloneWatchValue(baseline) as Value<App, A>, reset };
            reset = false;
          }
          if (reconnect === false && !signal.aborted && connection.signal.reason instanceof FlowerError) throw connection.signal.reason;
        } catch (error) {
          if (signal.aborted) return;
          if (reconnect === false || !isTransient(error)) throw error;
        } finally {
          clearTimeout(timer);
          signal.removeEventListener("abort", stop);
          connection.abort();
        }
        if (reconnect === false || signal.aborted) return;
        await sleep(backoff(failures++, delays.initialDelayMs, delays.maxDelayMs), signal).catch(() => {});
      }
    });
  }

  /** Resolve with the first value satisfying predicate, reconnecting as needed. Queries that take null may omit args. */
  waitUntil<A extends NullaryQueryAlias<App>>(alias: A, predicate?: (value: Value<App, A>) => boolean, options?: SubscribeOptions): Promise<Update<Value<App, A>>>;
  waitUntil<A extends QueryAliasOf<App>>(alias: A, args: ArgsOf<ApiOf<App>[A]>, predicate?: (value: Value<App, A>) => boolean, options?: SubscribeOptions): Promise<Update<Value<App, A>>>;
  async waitUntil(alias: string, ...rest: unknown[]): Promise<Update<Json>> {
    // Arguments are JSON, so a function or nothing in their place means they were omitted.
    const [args, predicate, options = {}] = (typeof rest[0] === "function" || rest[0] === undefined ? [null, ...rest] : rest) as
      [Json, ((value: Json) => boolean) | undefined, SubscribeOptions | undefined];
    const subscribe = this.subscribe as unknown as (alias: string, args: unknown, options: SubscribeOptions) => AsyncGenerator<Update<Json>>;
    for await (const update of subscribe.call(this, alias, args, options)) if ((predicate ?? Boolean)(update.value)) return update;
    throw options.signal?.reason ?? new FlowerError("The subscription ended", 0, "WATCH_ENDED");
  }

  /** Poll a query at an interval; emits revision changes even for equal values. */
  watchPoll<A extends QueryAliasOf<App>>(alias: A, ...rest: [...Args<App, A>, options?: WatchPollOptions]): AsyncGenerator<QueryResult<Value<App, A>>> {
    const [args, options] = this.split(alias, rest);
    const client = this;
    const { intervalMs = 250 } = options;
    if (!Number.isInteger(intervalMs) || intervalMs < 1 || intervalMs > 2_147_483_647) throw new TypeError("Polling intervalMs must be an integer from 1 to 2147483647 milliseconds");
    return controlledWatch(options.signal, async function* (signal) {
      const url = client.queryUrl();
      let revision: number | undefined, value: string | undefined;
      while (!signal.aborted) {
        const result = await client.connection.request<QueryResult<Value<App, A>>>("/v1/query", { name: alias, args, ...await client.authorization(options) }, { signal, url });
        const encoded = canonicalJson(result.value);
        if (result.revision !== revision || encoded !== value) {
          revision = result.revision;
          value = encoded;
          yield result;
        }
        await sleep(intervalMs, signal).catch(() => {});
      }
    });
  }

  /** The same application in a named logical database. */
  partition(name: string): FlowerClient<App> {
    const path = partitionPath(name);
    return new FlowerClient<App>(this.url + path, { fetch: this.connection.fetch, credentials: this.credentials,
      boundedRetries: this.connection.boundedRetries, retry: this.retry, queryUrls: this.queryUrls.map((url) => url + path) });
  }

  /** Refresh only for new intent; keep every issued ID across uncertain retries. */
  refreshRetryIdentity(options: { signal?: AbortSignal } = {}): Promise<RetryIdentity> {
    return this.connection.refreshRetryIdentity(options);
  }

  /** Persist the returned ID before sending. Re-scoping an uncertain intent is unsafe. */
  newRequestId(intent?: string, options: { signal?: AbortSignal } = {}): Promise<string> {
    return this.connection.newRequestId(intent, options);
  }

  /** Save the ID and identity before opening if the response must be recoverable. */
  async openRetrySession(id: string = crypto.randomUUID().replaceAll("-", ""), options: RequestOptions = {}): Promise<QueryResult<RetrySession>> {
    const identity = await this.connection.currentIdentity(options);
    return this.session({ operation: "open", session: id, incarnation: identity.incarnation, epoch: identity.currentEpoch }, options);
  }

  /** Persist sequence allocation yourself; nothing acknowledges or advances implicitly. */
  sessionRequestId(session: RetrySession, sequence: number): string {
    if (!Number.isSafeInteger(sequence) || sequence < 1 || session.closed || sequence <= session.acknowledgedThrough ||
        !/^[a-f0-9]{32}$/.test(session.id) || !/^[a-f0-9]{32}$/.test(session.database) || !/^[a-f0-9]{32}$/.test(session.incarnation) ||
        !Number.isSafeInteger(session.epoch) || session.epoch < 0) throw new TypeError("Invalid or retired session sequence");
    return `f2:${session.database}:${session.incarnation}:${session.epoch}:${session.id}:${sequence}`;
  }

  async retrySessionStatus(session: Pick<RetrySession, "id" | "incarnation">, options: RequestOptions = {}): Promise<QueryResult<RetrySession>> {
    return this.session({ operation: "status", session: session.id, incarnation: session.incarnation }, options);
  }

  /** Acknowledge only a durably consumed contiguous prefix; abandon also fences unknown outcomes. */
  async acknowledgeRetrySession(session: RetrySession, through: number, options: SessionOptions = {}): Promise<QueryResult<RetrySession>> {
    return this.session({ operation: "ack", session: session.id, incarnation: session.incarnation, through,
      limit: options.limit ?? 256, abandon: options.abandon ?? false }, options);
  }

  /** Terminal: in-flight results may become unavailable; cleanup is bounded. */
  async closeRetrySession(session: RetrySession, options: SessionOptions = {}): Promise<QueryResult<RetrySession>> {
    return this.session({ operation: "close", session: session.id, incarnation: session.incarnation, limit: options.limit ?? 256 }, options);
  }

  private async session(body: Record<string, unknown>, options: RequestOptions): Promise<QueryResult<RetrySession>> {
    return this.attempt(options, async (signal) => this.connection.request("/v1/session", { ...body, ...await this.authorization(options) }, { signal }));
  }
}

// ---- Control plane

export interface DeploymentOptions {
  requestId?: string;
  signal?: AbortSignal;
  /** Online preparation preserves writes but may conflict; blocking guarantees an exclusive window. */
  preparation?: "online" | "blocking";
}
export interface DeploymentReceipt { revision: number; value: Json; duplicate: boolean }
export interface ControlOptions { signal?: AbortSignal; /** Retain across retries of create, move or resize. */ requestId?: string }
export interface FlowerAdminOptions { adminToken?: string; fetch?: FlowerFetch; boundedRetries?: boolean }
/** Physical Raft groups are provisioned separately; addresses use host:port. */
export interface ClusterGroup { id: string; addresses: string[] }
export type PartitionMovePhase = "copying" | "freezing" | "importing" | "activating" | "retiring" | "complete";
export interface PartitionMove {
  operation: string; partition: string; source: ClusterGroup; destination: ClusterGroup;
  source_epoch: number; epoch: number; phase: PartitionMovePhase;
}
export interface PartitionPlacement {
  partition: string; epoch: number; owner: ClusterGroup;
  status: "creating" | "active" | "moving"; operation: string; movement: PartitionMove | null;
}
export interface RebalanceMove { partition: string; source: string; destination: string; operation: string }
export interface RebalancePlan { operation: string; groups: ClusterGroup[]; moves: RebalanceMove[]; next: number; complete: boolean }
export interface ClusterLayout { groups: ClusterGroup[]; partitions: PartitionPlacement[]; moves: PartitionMove[]; rebalance: RebalancePlan | null }
export interface PartitionWaitOptions { signal?: AbortSignal; timeoutMs?: number; intervalMs?: number }
export interface KeyGenerateOptions extends ControlOptions { /** RSA modulus size; rejected for other algorithms. */ bits?: number }
export interface KeyRevokeOptions extends ControlOptions { /** Omit to affect every version. */ version?: number }
export interface SealedKeyImport { version: 1; wrappingId: string; nonce: string; ciphertext: string }
export interface ManagedKeyCatalog {
  domain: string | null;
  revision: number;
  keys: Record<string, { id: string; algorithm: ManagedKeyAlgorithm; activeVersion: number; versions: { version: number; revoked: boolean; retired: boolean; destroyed: boolean; wrappingId: string | null; kid: string }[] }>;
  bindings: Record<string, { key: string; usages: KeyUsage[] }>;
}
export interface KeyCacheStats { entries: number; bytes: number; budgetBytes: number; hits: number; misses: number; loads: number; evictions: number; flightEntries: number; flightBytes: number; coalesced: number }
export interface RetryIdentity {
  database: string; incarnation: string; currentEpoch: number; minEpoch: number;
  /** How often the leader advances the epoch, or null when an operator does. */
  epochMs?: number | null;
}
export interface RetentionState extends RetryIdentity {
  receiptBytes: number; receiptCount: number; maxReceiptBytes: number | null; gcCursor: string | null; gcComplete: boolean;
  sessionBytes: number; sessionCount: number; gcReceiptsComplete: boolean; gcSessionCursor: string | null;
  /** Set by the rotate action. */
  rotation?: { epochMs: number; keepEpochs: number };
}
export interface RetrySession { database: string; incarnation: string; id: string; epoch: number; acknowledgedThrough: number; closed: boolean }
export interface SessionOptions extends RequestOptions { limit?: number; /** Explicitly retire unknown and abandoned intents too. */ abandon?: boolean }
export type RetentionAction =
  | { operation: "initialize"; database: string; incarnation: string; max_receipt_bytes: number | null }
  | { operation: "advance"; incarnation: string; current_epoch: number; min_epoch: number }
  | { operation: "collect"; incarnation: string; limit: number }
  | { operation: "set_budget"; incarnation: string; max_receipt_bytes: number | null }
  | { operation: "rotate"; incarnation: string; epoch_ms: number | null; keep_epochs: number }
  | { operation: "reincarnate"; incarnation: string; new_incarnation: string; fence_attestation: string };
/** Pinned placement recorded in transaction protocol metadata, not a routing hint. */
export interface TransactionClosureTarget { group: string; partition: string | null; epoch: number; addresses?: string[] }
export interface TransactionClosureState {
  history: string | null; nextSequence: number; closedThrough: number;
  pending: { through: number; participants: TransactionClosureTarget[]; acknowledged: TransactionClosureTarget[] } | null;
  blockedReason: string | null; deletedRecords: number;
}
export type TransactionClosureAction = { operation: "close"; through?: number; maxBytes?: number } | { operation: "collect"; maxBytes?: number };
/** Durable resumable index and graph preparation. Active code remains visible until activation. */
export interface StagedDeploymentState {
  requestId: string;
  phase: "backfill" | "rebuilding" | "ready" | "failed" | "active" | "canceled" | "collected";
  baseRevision: number;
  baseBundleHash: string | null;
  bundleHash: string;
  cursor: string | null;
  scannedRows: number;
  builtEntries: number;
  graphCursor: string | null;
  rebuiltRoots: number;
  generation: string | null;
  error?: string | null;
  cleanupCursor: string | null;
}
export type StagedDeploymentAction =
  | { operation: "advance" | "collect"; requestId: string; maxBytes?: number }
  | { operation: "activate" | "cancel"; requestId: string };

/** Operator endpoints: deployment, cluster, partitions, keys and retention. Requires the operator token. */
export class FlowerAdmin {
  readonly url: string;
  private readonly connection: Connection;

  constructor(url = "http://127.0.0.1:7101", options: FlowerAdminOptions = {}) {
    this.connection = connection(url, options);
    this.url = this.connection.url;
  }

  private admin<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
    return this.connection.request<T>(path, body, { admin: true, signal });
  }

  /** The same operations against one named logical database. */
  partition(name: string): FlowerAdmin {
    const inner = this.connection.scoped(partitionPath(name));
    return Object.assign(Object.create(FlowerAdmin.prototype) as FlowerAdmin, { url: inner.url, connection: inner });
  }

  async initialize(members: Record<string, string>, options: { signal?: AbortSignal } = {}): Promise<void> {
    await this.admin("/raft/initialize", members, options.signal);
  }

  async deploy(bundle: Bundle, options: DeploymentOptions = {}): Promise<DeploymentReceipt> {
    const request: Record<string, unknown> = { requestId: await this.connection.requestId(options.requestId, options), bundle };
    if (options.preparation !== undefined) {
      if (options.preparation !== "online" && options.preparation !== "blocking") throw new TypeError("preparation must be online or blocking");
      request.preparation = options.preparation;
    }
    return this.admin("/admin/deploy", request, options.signal);
  }

  /** Start or recover a durable staged deployment. Preserve requestId after uncertain responses. */
  async stageDeployment(bundle: Bundle, options: ControlOptions = {}): Promise<QueryResult<StagedDeploymentState>> {
    return this.admin("/admin/deployments", { operation: "stage", requestId: await this.connection.requestId(options.requestId, options), bundle }, options.signal);
  }

  async stagedDeploymentStatus(options: { signal?: AbortSignal } = {}): Promise<QueryResult<StagedDeploymentState | null>> {
    return this.admin("/admin/deployments", { operation: "status" }, options.signal);
  }

  /** Advance preparation, activate a ready job, cancel, or collect obsolete state. */
  async controlStagedDeployment(action: StagedDeploymentAction, options: { signal?: AbortSignal } = {}): Promise<QueryResult<StagedDeploymentState>> {
    return this.admin("/admin/deployments", action, options.signal);
  }

  async retentionStatus(options: { signal?: AbortSignal } = {}): Promise<QueryResult<RetentionState | null>> {
    return this.admin("/admin/retention", { operation: "status" }, options.signal);
  }

  /** Revision-conditional control. After an uncertain response, inspect status first. */
  async controlRetention(expectedRevision: number, action: RetentionAction, options: { signal?: AbortSignal } = {}): Promise<MutationResult<{ state: RetentionState; collected: number }>> {
    return this.admin("/admin/retention", { expected_revision: expectedRevision, action }, options.signal);
  }

  /** Fresh operator view of distributed transaction closure. */
  async transactionClosureStatus(options: { signal?: AbortSignal } = {}): Promise<QueryResult<TransactionClosureState>> {
    return this.admin("/admin/transactions", { operation: "status" }, options.signal);
  }

  /** Monotonic, retryable closure and collection. Inspect blockedReason before assuming completion. */
  async controlTransactionClosure(action: TransactionClosureAction, options: { signal?: AbortSignal } = {}): Promise<QueryResult<TransactionClosureState>> {
    return this.admin("/admin/transactions", action, options.signal);
  }

  /** Operator metadata; private material is never returned. */
  async keyList(options: { signal?: AbortSignal } = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.admin("/admin/keys", { operation: "list" }, options.signal);
  }

  /** This node's prepared-key cache across partitions. */
  async keyCacheStats(options: { signal?: AbortSignal } = {}): Promise<MutationResult<KeyCacheStats>> {
    return this.admin("/admin/keys", { operation: "cache" }, options.signal);
  }

  async keyGenerate(name: string, algorithm: ManagedKeyAlgorithm, options: KeyGenerateOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "generate", name, algorithm, ...(options.bits === undefined ? {} : { bits: options.bits }) }, options);
  }

  /** Import only an encrypted envelope produced by the native key seal command. */
  async keyImport(name: string, algorithm: ManagedKeyAlgorithm, sealed: SealedKeyImport, options: ControlOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    canonicalJson(sealed);
    if (sealed === null || typeof sealed !== "object" || Array.isArray(sealed) || sealed.version !== 1 ||
        Object.keys(sealed).length !== 4 || Object.keys(sealed).some((field) => !["version", "wrappingId", "nonce", "ciphertext"].includes(field)) ||
        typeof sealed.wrappingId !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(sealed.wrappingId) ||
        typeof sealed.nonce !== "string" || !/^[A-Za-z0-9_-]{16}$/.test(sealed.nonce) ||
        typeof sealed.ciphertext !== "string" || !/^[A-Za-z0-9_-]{23,}$/.test(sealed.ciphertext) || sealed.ciphertext.length % 4 === 1) {
      throw new TypeError("Key import requires the encrypted envelope produced by native flower key seal");
    }
    return this.key({ operation: "import", name, algorithm, sealed }, options);
  }

  async keyBind(name: string, key: string, usages: readonly KeyUsage[], options: ControlOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "bind", name, key, usages }, options);
  }

  async keyUnbind(name: string, options: ControlOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "unbind", name }, options);
  }

  async keyRotate(name: string, options: KeyGenerateOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "rotate", name, ...(options.bits === undefined ? {} : { bits: options.bits }) }, options);
  }

  async keyRevoke(name: string, options: KeyRevokeOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "revoke", name, ...(options.version === undefined ? {} : { version: options.version }) }, options);
  }

  async keyRetire(name: string, options: KeyRevokeOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "retire", name, ...(options.version === undefined ? {} : { version: options.version }) }, options);
  }

  async keyDestroy(name: string, options: KeyRevokeOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "destroy", name, ...(options.version === undefined ? {} : { version: options.version }) }, options);
  }

  async keyRewrap(name: string, options: KeyRevokeOptions = {}): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.key({ operation: "rewrap", name, ...(options.version === undefined ? {} : { version: options.version }) }, options);
  }

  private async key(body: Record<string, unknown>, options: ControlOptions): Promise<MutationResult<ManagedKeyCatalog>> {
    return this.admin("/admin/keys", { ...body, requestId: await this.connection.requestId(options.requestId, options) }, options.signal);
  }

  /** Registered groups, placements and durable moves. */
  async layout(options: { signal?: AbortSignal } = {}): Promise<ClusterLayout> {
    return this.admin("/admin/partitions/catalog", { action: "list" }, options.signal);
  }

  async registerGroup(group: ClusterGroup, options: { signal?: AbortSignal } = {}): Promise<ClusterGroup> {
    return this.admin("/admin/partitions/catalog", { action: "register_group", group }, options.signal);
  }

  /** Unregister an empty, unreferenced group; its processes keep running. */
  async removeGroup(group: string, options: { signal?: AbortSignal } = {}): Promise<{ removed: string }> {
    return this.admin("/admin/partitions/catalog", { action: "remove_group", group }, options.signal);
  }

  /** Durably start creation; wait for active before deploying. */
  async createPartition(partition: string, group: string, options: ControlOptions = {}): Promise<PartitionPlacement> {
    return this.admin("/admin/partitions/catalog", { action: "create", partition, group, operation: options.requestId ?? crypto.randomUUID() }, options.signal);
  }

  /** Copy while serving, then briefly pause the partition for the final changes and cutover. */
  async movePartition(partition: string, destination: string, options: ControlOptions = {}): Promise<PartitionMove> {
    return this.admin("/admin/partitions/catalog", { action: "begin_move", partition, destination, operation: options.requestId ?? crypto.randomUUID() }, options.signal);
  }

  /** Balance partition counts over existing groups, one partition at a time. */
  async resize(groups: string[], options: ControlOptions = {}): Promise<RebalancePlan> {
    return this.admin("/admin/partitions/catalog", { action: "rebalance", groups, operation: options.requestId ?? crypto.randomUUID() }, options.signal);
  }

  async partitionStatus(partition: string, options: { signal?: AbortSignal } = {}): Promise<PartitionPlacement> {
    return this.admin("/admin/partitions/catalog", { action: "resolve", partition }, options.signal);
  }

  /** Wait for creation or movement to activate the owner. Cancelling does not undo the operation. */
  async waitForPartition(partition: string, options: PartitionWaitOptions = {}): Promise<PartitionPlacement> {
    const timeoutMs = options.timeoutMs ?? 30_000, intervalMs = options.intervalMs ?? 100;
    for (const [name, value] of Object.entries({ timeoutMs, intervalMs })) {
      if (!Number.isInteger(value) || value < 1 || value > 2_147_483_647) throw new TypeError(`${name} must be a positive integer within the timer range`);
    }
    const timeout = AbortSignal.timeout(timeoutMs);
    const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
    try {
      while (true) {
        const placement = await this.partitionStatus(partition, { signal });
        if (placement.status === "active") return placement;
        await sleep(intervalMs, signal);
      }
    } catch (error) {
      if (timeout.aborted && !options.signal?.aborted) throw new FlowerError("Timed out waiting for partition activation", 0, "PARTITION_WAIT_TIMEOUT");
      throw error;
    }
  }
}
