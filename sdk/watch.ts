import type { Json } from "./json.ts";

export type JsonPatchOperation = { op: "add" | "replace"; path: string; value: Json } | { op: "remove"; path: string };
export interface WatchSnapshot<Value = Json> { type: "snapshot"; sequence: number; revision: number; value: Value }
export interface WatchPatch { type: "patch"; sequence: number; baseSequence: number; revision: number; patch: JsonPatchOperation[] }
export type WatchDelta<Value = Json> = WatchSnapshot<Value> | WatchPatch;
export const MAX_WATCH_EVENT_BYTES = 17 * 1024 * 1024;
export const MAX_WATCH_VALUE_BYTES = 16 * 1024 * 1024;
export const MAX_PATCH_OPERATIONS = 256;
/** Local client allowances; these are never sent to the server. */
export interface WatchBudgets {
  /** Maximum wire bytes in one SSE event (also bounds an HTTP error body). Default: 17 MiB. */
  maxEventBytes?: number;
  /** Maximum UTF-8 bytes in a snapshot or a value reconstructed by watch(). Raw deltas do not reconstruct state. Default: 16 MiB. */
  maxValueBytes?: number;
  /** Maximum operations in one JSON Patch event. Default: 256. */
  maxPatchOperations?: number;
}
export function watchBudgets(options: WatchBudgets = {}): Required<WatchBudgets> {
  const limits = {
    maxEventBytes: options.maxEventBytes === undefined ? MAX_WATCH_EVENT_BYTES : options.maxEventBytes,
    maxValueBytes: options.maxValueBytes === undefined ? MAX_WATCH_VALUE_BYTES : options.maxValueBytes,
    maxPatchOperations: options.maxPatchOperations === undefined ? MAX_PATCH_OPERATIONS : options.maxPatchOperations,
  };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${name} must be a positive safe integer`);
  }
  return limits;
}
const own = (object: object, key: PropertyKey) => Object.hasOwn(object, key);

export class WatchProtocolError extends Error {
  constructor(message: string) { super(message); this.name = "WatchProtocolError"; }
}
function invalid(message: string): never { throw new WatchProtocolError(message); }

/** Return/abort must interrupt a pending next(), not wait behind an idle stream. */
export function controlledWatch<T>(signal: AbortSignal | undefined, run: (signal: AbortSignal) => AsyncGenerator<T>): AsyncGenerator<T> {
  const controller = new AbortController();
  const iterator = (async function* () {
    const abort = () => controller.abort(signal?.reason);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    try { if (!controller.signal.aborted) yield* run(controller.signal); }
    catch (error) { if (!controller.signal.aborted) throw error; }
    finally { signal?.removeEventListener("abort", abort); controller.abort(); }
  })();
  const finish = iterator.return.bind(iterator);
  iterator.return = (value) => { controller.abort(); return finish(value); };
  const fail = iterator.throw.bind(iterator);
  iterator.throw = (error) => { controller.abort(); return fail(error); };
  return iterator;
}

export function cloneWatchValue<T>(value: T): T { return JSON.parse(JSON.stringify(value)); }

function validateValueSize(value: Json, maxBytes: number): void {
  if (new TextEncoder().encode(JSON.stringify(value)).byteLength > maxBytes) invalid("Watch value exceeds maxValueBytes");
}

function validateJson(value: unknown): asserts value is Json {
  function* objectValues(value: object): Generator<unknown> {
    for (const key in value) if (own(value, key)) yield (value as Record<string, unknown>)[key];
  }
  // Keep one iterator per nesting level, rather than one tuple per array item.
  const pending: { values: Iterator<unknown>; depth: number }[] = [{ values: [value].values(), depth: 0 }];
  while (pending.length) {
    const frame = pending.at(-1)!;
    const next = frame.values.next();
    if (next.done) { pending.pop(); continue; }
    const item = next.value, depth = frame.depth;
    if (depth > 128) invalid("Watch JSON nesting exceeds 128");
    if (item === null || typeof item === "string" || typeof item === "boolean") continue;
    if (typeof item === "number") { if (!Number.isFinite(item)) invalid("Watch JSON numbers must be finite"); continue; }
    if (typeof item !== "object") invalid("Watch values must be JSON");
    pending.push({ values: Array.isArray(item) ? item.values() : objectValues(item), depth: depth + 1 });
  }
}

function pointer(path: unknown): string[] {
  if (typeof path !== "string") return invalid("Patch path must be a JSON pointer");
  if (path === "") return [];
  if (!path.startsWith("/")) return invalid("Patch path must begin with /");
  const parts = path.slice(1).split("/");
  if (parts.length > 128) invalid("Patch path exceeds 128 levels");
  return parts.map((part) => {
    if (/~(?:[^01]|$)/.test(part)) invalid("Invalid JSON pointer escape");
    return part.replace(/~1/g, "/").replace(/~0/g, "~");
  });
}

export function validatePatch(value: unknown, maxOperations = MAX_PATCH_OPERATIONS): asserts value is JsonPatchOperation[] {
  watchBudgets({ maxPatchOperations: maxOperations });
  if (!Array.isArray(value) || value.length > maxOperations) invalid("Invalid or oversized watch patch");
  for (const operation of value) {
    if (!operation || typeof operation !== "object" || Array.isArray(operation)) invalid("Invalid patch operation");
    if (!["add", "remove", "replace"].includes(operation.op)) invalid("Unsupported patch operation");
    pointer(operation.path);
    if (operation.op !== "remove") {
      if (!own(operation, "value")) invalid("Patch operation has no value");
      validateJson(operation.value);
    }
  }
}

/** Applies only own JSON properties. __proto__ and constructor remain ordinary data keys. */
export function applyWatchPatch(value: Json, operations: JsonPatchOperation[], options: WatchBudgets = {}): Json {
  const limits = watchBudgets(options);
  validatePatch(operations, limits.maxPatchOperations);
  let result = cloneWatchValue(value);
  for (const operation of operations) {
    const parts = pointer(operation.path);
    if (!parts.length) {
      if (operation.op === "remove") invalid("Cannot remove the entire watched value");
      result = cloneWatchValue(operation.value);
      continue;
    }
    let parent: any = result;
    const indexFor = (array: Json[], key: string, add: boolean): number => {
      if (add && key === "-") return array.length;
      if (!/^(0|[1-9][0-9]*)$/.test(key)) return invalid("Invalid patch array index");
      const index = Number(key);
      if (!Number.isSafeInteger(index) || index < 0 || index > array.length || !add && index === array.length) invalid("Patch array index out of bounds");
      return index;
    };
    for (const key of parts.slice(0, -1)) {
      if (parent === null || typeof parent !== "object") invalid("Patch parent is not a container");
      if (Array.isArray(parent)) indexFor(parent, key, false);
      if (!own(parent, key)) invalid("Patch parent does not exist");
      parent = parent[key];
    }
    if (parent === null || typeof parent !== "object") invalid("Patch target is not a container");
    const key = parts.at(-1)!;
    if (Array.isArray(parent)) {
      const index = indexFor(parent, key, operation.op === "add");
      if (operation.op === "remove") parent.splice(index, 1);
      else if (operation.op === "add") parent.splice(index, 0, cloneWatchValue(operation.value));
      else parent[index] = cloneWatchValue(operation.value);
    } else {
      if (operation.op !== "add" && !own(parent, key)) invalid("Patch target does not exist");
      if (operation.op === "remove") delete parent[key];
      else Object.defineProperty(parent, key, { value: cloneWatchValue(operation.value), enumerable: true, writable: true, configurable: true });
    }
  }
  validateJson(result);
  validateValueSize(result, limits.maxValueBytes);
  return result;
}

interface SseFrame { event: string; data: string; id?: string }

/** Bounded streaming SSE decoder, including split UTF-8 and split CRLF. */
export async function* readSse(body: ReadableStream<Uint8Array>, signal: AbortSignal, maxBytes = MAX_WATCH_EVENT_BYTES, onActivity?: () => void): AsyncGenerator<SseFrame> {
  watchBudgets({ maxEventBytes: maxBytes });
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const decode = (bytes?: Uint8Array, stream = false): string => {
    try { return decoder.decode(bytes, { stream }); }
    catch { return invalid("Watch stream contains invalid UTF-8"); }
  };
  const encoder = new TextEncoder();
  let line = "", event = "", id: string | undefined, data: string[] = [], bytes = 0, afterCr = false;
  const abort = () => { void reader.cancel(signal.reason).catch(() => {}); };
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  function fragment(text: string): void {
    bytes += encoder.encode(text).byteLength;
    if (bytes > maxBytes) invalid("Watch event exceeds its byte limit");
    line += text;
  }
  function endLine(carriageReturn: boolean): SseFrame | undefined {
    // Charge CR as CRLF even if a peer uses bare CR; never undercount wire bytes.
    bytes += carriageReturn ? 2 : 1;
    if (bytes > maxBytes) invalid("Watch event exceeds its byte limit");
    const current = line;
    line = "";
    if (!current) {
      const frame = data.length ? { event: event || "message", data: data.join("\n"), ...(id === undefined ? {} : { id }) } : undefined;
      event = ""; id = undefined; data = []; bytes = 0;
      return frame;
    }
    if (current.startsWith(":")) return;
    const colon = current.indexOf(":");
    const field = colon < 0 ? current : current.slice(0, colon);
    let value = colon < 0 ? "" : current.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") event = value;
    else if (field === "data") data.push(value);
    else if (field === "id" && !value.includes("\0")) id = value;
  }
  try {
    while (!signal.aborted) {
      const next = await reader.read();
      onActivity?.();
      if (next.done) {
        decode();
        if (line || data.length || event) invalid("Watch stream ended during an event");
        return;
      }
      for (let start = 0; start < next.value.length; start += 65536) {
        const text = decode(next.value.subarray(start, start + 65536), true);
        let offset = 0;
        for (let index = 0; index < text.length; index++) {
          const character = text[index];
          if (afterCr) {
            afterCr = false;
            if (character === "\n") { offset = index + 1; continue; }
          }
          if (character !== "\r" && character !== "\n") continue;
          fragment(text.slice(offset, index));
          offset = index + 1;
          afterCr = character === "\r";
          const frame = endLine(afterCr);
          if (frame) yield frame;
        }
        fragment(text.slice(offset));
      }
    }
  } finally {
    signal.removeEventListener("abort", abort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function decodeWatchEvent(frame: SseFrame, previousSequence: number, previousRevision: number, options: WatchBudgets = {}): WatchDelta | { type: "error"; error: { code: string; message: string; status: number; failure?: unknown } } {
  const limits = watchBudgets(options);
  let value: any;
  try { value = JSON.parse(frame.data); } catch { return invalid("Watch event contains invalid JSON"); }
  if (!value || typeof value !== "object" || Array.isArray(value)) invalid("Invalid watch event");
  if (frame.event === "error") {
    const error = value.error;
    if (!error || typeof error.code !== "string" || typeof error.message !== "string" || !Number.isInteger(error.status) || error.status < 100 || error.status > 599) invalid("Invalid terminal watch error");
    return { type: "error", error };
  }
  if (!["snapshot", "patch"].includes(frame.event)) invalid("Unknown watch event type");
  if (!Number.isSafeInteger(value.sequence) || value.sequence < 0 || value.sequence <= previousSequence) invalid("Watch event sequence must increase");
  if (frame.id !== undefined && frame.id !== String(value.sequence)) invalid("Watch event ID does not match its sequence");
  if (!Number.isSafeInteger(value.revision) || value.revision < previousRevision || value.revision < 0) invalid("Invalid watch revision");
  if (frame.event === "snapshot") {
    if (!own(value, "value")) invalid("Watch snapshot has no value");
    validateJson(value.value);
    validateValueSize(value.value, limits.maxValueBytes);
    return { type: "snapshot", sequence: value.sequence, revision: value.revision, value: value.value };
  }
  if (value.sequence !== previousSequence + 1) invalid("Watch patch sequence is not consecutive");
  if (previousSequence < 0 || value.baseSequence !== previousSequence) invalid("Watch patch has the wrong base sequence");
  validatePatch(value.patch, limits.maxPatchOperations);
  return { type: "patch", sequence: value.sequence, baseSequence: value.baseSequence, revision: value.revision, patch: value.patch };
}
