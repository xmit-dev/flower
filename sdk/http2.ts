import { connect, constants } from "node:http2";
import { constants as bufferConstants } from "node:buffer";
import type { ClientHttp2Session, ClientHttp2Stream, IncomingHttpHeaders, OutgoingHttpHeaders } from "node:http2";
import type { FlowerFetch, FlowerRequestInit } from "./client.ts";
import { fetchEventStream } from "./http2-stream.ts";

export interface Http2TransportOptions {
  /** PEM trust roots for HTTPS; omitted uses Node's configured defaults. No verification bypass. */
  ca?: string | Uint8Array | readonly (string | Uint8Array)[];
  /** Whole-request deadline; SSE uses it only until response headers. Default 30 seconds. */
  requestTimeoutMs?: number;
  /** Close unused sessions after this interval. Default 30 seconds. */
  idleTimeoutMs?: number;
  /** Maximum live sessions, including draining sessions. Default 16. */
  maxSessions?: number;
  /** Default 8 MiB, matching Flower's default application request budget. */
  maxRequestBytes?: number;
  /** Buffered JSON response limit; default 64 MiB. */
  maxResponseBytes?: number;
}

export interface Http2Transport {
  /** JSON POST requests; streams responses when Accept requests SSE. No retries or redirects. */
  fetch: FlowerFetch;
  /** Cancel active streams and close all owned sessions. Idempotent. */
  close(): Promise<void>;
}

interface Session {
  origin: string;
  connection: ClientHttp2Session;
  active: number;
  draining: boolean;
  idle?: ReturnType<typeof setTimeout>;
}

function limit(name: string, value: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) throw new TypeError(`${name} must be an integer from 1 to ${maximum}`);
  return value;
}

function transportError(message: string, code: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

/**
 * Node-only HTTP/2 transport: h2c for HTTP and verified TLS/ALPN for HTTPS.
 * One multiplexed session is reused per origin. No HTTP/1 fallback, compression decoding,
 * automatic replay. Accept: text/event-stream enables bounded, backpressured
 * streaming; use an AbortSignal to stop a subscription. Keep IDs when retrying.
 */
export function createHttp2Transport(options: Http2TransportOptions = {}): Http2Transport {
  const certificate = (value: unknown): string | Buffer => {
    if (typeof value === "string" && value.length) return value;
    if (value instanceof Uint8Array && value.length) return Buffer.from(value);
    throw new TypeError("ca must contain nonempty PEM strings or Uint8Arrays");
  };
  const ca = options.ca === undefined ? undefined : Array.isArray(options.ca)
    ? options.ca.map(certificate) : certificate(options.ca);
  if (Array.isArray(ca) && ca.length === 0) throw new TypeError("ca must contain at least one certificate");
  const requestTimeoutMs = limit("requestTimeoutMs", options.requestTimeoutMs ?? 30_000, 2_147_483_647);
  const idleTimeoutMs = limit("idleTimeoutMs", options.idleTimeoutMs ?? 30_000, 2_147_483_647);
  const maxSessions = limit("maxSessions", options.maxSessions ?? 16, Number.MAX_SAFE_INTEGER);
  const maxRequestBytes = limit("maxRequestBytes", options.maxRequestBytes ?? 8 * 1_048_576, Math.min(Number.MAX_SAFE_INTEGER, bufferConstants.MAX_LENGTH));
  const maxResponseBytes = limit("maxResponseBytes", options.maxResponseBytes ?? 64 * 1_048_576, Math.min(Number.MAX_SAFE_INTEGER, bufferConstants.MAX_LENGTH));
  const available = new Map<string, Session>();
  const sessions = new Set<Session>();
  const shutdown = new AbortController();
  let closing: Promise<void> | undefined;

  function forget(session: Session): void {
    if (available.get(session.origin) === session) available.delete(session.origin);
    clearTimeout(session.idle);
  }

  function idle(session: Session): void {
    clearTimeout(session.idle);
    if (session.active || session.connection.destroyed || session.connection.closed) return;
    if (session.draining) { session.connection.close(); return; }
    session.idle = setTimeout(() => {
      forget(session);
      session.connection.destroy();
    }, idleTimeoutMs);
    session.idle.unref();
  }

  function acquire(origin: string): Session {
    let session = available.get(origin);
    if (session && !session.draining && !session.connection.closed && !session.connection.destroyed) {
      clearTimeout(session.idle);
      return session;
    }
    if (session) forget(session);
    if (sessions.size >= maxSessions) {
      // Evict an idle connection, but retain its slot until the socket closes.
      const unused = [...sessions].find((candidate) => candidate.active === 0);
      if (unused) { forget(unused); unused.connection.destroy(); }
      throw transportError("HTTP/2 session limit reached; retry after an idle session closes", "H2_SESSION_LIMIT");
    }
    const connection = connect(origin, { ca, rejectUnauthorized: true, settings: { enablePush: false } });
    session = { origin, connection, active: 0, draining: false };
    const owned = session;
    sessions.add(owned);
    available.set(origin, owned);
    connection.on("error", () => {
      forget(owned);
      owned.draining = true;
      connection.destroy();
    });
    connection.on("goaway", () => {
      // Existing accepted streams can finish; later calls establish a new session.
      forget(owned);
      owned.draining = true;
      connection.close();
    });
    connection.on("close", () => { forget(owned); sessions.delete(owned); });
    connection.on("stream", (stream) => { stream.on("error", () => {}); stream.close(constants.NGHTTP2_CANCEL); });
    return owned;
  }

  const fetch = async (input: string, init: FlowerRequestInit): Promise<Response> => {
    if (shutdown.signal.aborted) throw shutdown.signal.reason;
    if (init.signal?.aborted) throw init.signal.reason;
    const url = new URL(input);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) throw new TypeError("Flower HTTP/2 transport requires an http:// or https:// URL without credentials");
    if (init.method !== "POST" || typeof init.body !== "string") throw new TypeError("Flower HTTP/2 transport accepts JSON POST requests with string bodies");
    const bytes = Buffer.byteLength(init.body);
    if (bytes > maxRequestBytes) throw transportError(`HTTP/2 request body exceeds ${maxRequestBytes} bytes`, "H2_REQUEST_TOO_LARGE");
    const headers: OutgoingHttpHeaders = {
      ":method": "POST", ":path": url.pathname + url.search, ":authority": url.host, ":scheme": url.protocol.slice(0, -1),
    };
    for (const [name, value] of new Headers(init.headers)) {
      if (name.startsWith(":") || /^(?:connection|keep-alive|proxy-connection|transfer-encoding|upgrade|host)$/.test(name)) {
        throw new TypeError(`Unsupported HTTP/2 request header: ${name}`);
      }
      if (name === "content-length" && value !== String(bytes)) throw new TypeError("HTTP/2 content-length does not match the request body");
      headers[name] = value;
    }
    headers["content-length"] = String(bytes);
    const session = acquire(url.origin);
    session.active++;
    if (new Headers(init.headers).get("accept")?.split(",").some((type) => type.split(";", 1)[0].trim().toLowerCase() === "text/event-stream")) {
      return fetchEventStream({
        connection: session.connection, headers, body: init.body,
        signal: AbortSignal.any([shutdown.signal, ...(init.signal ? [init.signal] : [])]),
        requestTimeoutMs, maxResponseBytes,
        complete() { session.active--; idle(session); },
      });
    }
    const signal = AbortSignal.any([shutdown.signal, AbortSignal.timeout(requestTimeoutMs), ...(init.signal ? [init.signal] : [])]);
    return new Promise<Response>((resolve, reject) => {
      let stream: ClientHttp2Stream;
      let settled = false;
      let ended = false;
      let responseHeaders: IncomingHttpHeaders | undefined;
      let expectedBytes: number | undefined;
      let chunks: Buffer[] = [];
      let receivedBytes = 0;
      const finish = (error?: unknown, response?: Response) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", abort);
        chunks = [];
        session.active--;
        idle(session);
        if (error !== undefined) reject(error); else resolve(response!);
      };
      const cancel = (error: unknown) => {
        finish(error);
        if (stream && !stream.destroyed) {
          try { stream.close(constants.NGHTTP2_CANCEL); } catch { stream.destroy(); }
        }
      };
      const abort = () => cancel(signal.reason);
      try {
        stream = session.connection.request(headers);
        stream.on("error", (error) => finish(error));
        stream.on("aborted", () => finish(transportError("HTTP/2 stream aborted before the response completed", "H2_STREAM_ABORTED")));
        stream.on("close", () => {
          if (settled) return;
          if (!ended || stream.aborted || stream.rstCode !== constants.NGHTTP2_NO_ERROR) {
            finish(transportError("HTTP/2 stream closed before the response completed", "H2_STREAM_CLOSED"));
            return;
          }
          try {
            const status = Number(responseHeaders?.[":status"]);
            if (!Number.isInteger(status) || status < 200 || status > 599) throw transportError("HTTP/2 response has no valid final status", "H2_INVALID_RESPONSE");
            if (expectedBytes !== undefined && receivedBytes !== expectedBytes && status !== 304) {
              throw transportError(`HTTP/2 response ended after ${receivedBytes} of ${expectedBytes} declared bytes`, "H2_RESPONSE_TRUNCATED");
            }
            const headers = new Headers();
            for (const [name, value] of Object.entries(responseHeaders ?? {})) {
              if (name.startsWith(":") || value === undefined) continue;
              for (const item of Array.isArray(value) ? value : [value]) headers.append(name, String(item));
            }
            const body = [204, 205, 304].includes(status) ? null : Buffer.concat(chunks, receivedBytes).toString("utf8");
            finish(undefined, new Response(body, { status, headers }));
          } catch (error) { finish(error); }
        });
        stream.on("response", (headers) => {
          responseHeaders = headers;
          const encoding = headers["content-encoding"];
          if (encoding && encoding !== "identity") cancel(transportError("Flower HTTP/2 transport does not decode compressed responses", "H2_UNSUPPORTED_ENCODING"));
          const length = headers["content-length"];
          if (length !== undefined) {
            if (typeof length !== "string" || !/^\d+$/.test(length) || !Number.isSafeInteger(Number(length))) {
              cancel(transportError("HTTP/2 response has an invalid content-length", "H2_INVALID_RESPONSE"));
              return;
            }
            expectedBytes = Number(length);
          }
        });
        stream.on("data", (chunk: Buffer) => {
          if (settled) return;
          receivedBytes += chunk.length;
          if (receivedBytes > maxResponseBytes) { cancel(transportError(`HTTP/2 response body exceeds ${maxResponseBytes} bytes`, "H2_RESPONSE_TOO_LARGE")); return; }
          chunks.push(chunk);
        });
        stream.on("end", () => {
          // A broken connection can emit readable `end` before the stream's
          // reset/abort status is final. Only a clean close can acknowledge a
          // buffered response; otherwise a truncated HTTP200 loses retryability.
          ended = true;
        });
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort(); else stream.end(init.body);
      } catch (error) { cancel(error); }
    });
  };

  return {
    fetch,
    close() {
      if (closing) return closing;
      const owned = [...sessions];
      closing = new Promise<void>((resolve) => {
        let remaining = owned.length;
        const timer = setTimeout(resolve, 1_000);
        const finished = () => { if (--remaining <= 0) { clearTimeout(timer); resolve(); } };
        for (const session of owned) session.connection.once("close", finished);
        shutdown.abort(new DOMException("Flower HTTP/2 transport closed", "AbortError"));
        for (const session of owned) { forget(session); session.connection.destroy(); }
        if (!remaining) { clearTimeout(timer); resolve(); }
      });
      return closing;
    },
  };
}
