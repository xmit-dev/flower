// Streaming response support for the Node HTTP/2 transport. Session ownership
// remains with http2.ts; one completion callback releases the active stream slot.
import { constants } from "node:http2";
import type { ClientHttp2Session, ClientHttp2Stream, OutgoingHttpHeaders } from "node:http2";

interface StreamRequest {
  connection: ClientHttp2Session;
  headers: OutgoingHttpHeaders;
  body: string;
  signal: AbortSignal;
  requestTimeoutMs: number;
  maxResponseBytes: number;
  complete(): void;
}

function failure(message: string, code: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

export function fetchEventStream(request: StreamRequest): Promise<Response> {
  return new Promise((resolve, reject) => {
    let stream: ClientHttp2Stream | undefined;
    let controller: ReadableStreamDefaultController<Uint8Array>;
    let delivered = false;
    let finished = false;
    let events = false;
    let received = 0;
    const timeout = setTimeout(() => cancel(new DOMException("HTTP/2 response deadline exceeded", "TimeoutError")), request.requestTimeoutMs);
    timeout.unref();

    function finish(error?: unknown): void {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);
      request.signal.removeEventListener("abort", abort);
      request.complete();
      if (error !== undefined) {
        controller.error(error);
        if (!delivered) reject(error);
      } else {
        controller.close();
        if (!delivered) reject(failure("HTTP/2 response ended without headers", "H2_INVALID_RESPONSE"));
      }
    }

    function cancel(error: unknown): void {
      finish(error);
      if (stream && !stream.destroyed) {
        try { stream.close(constants.NGHTTP2_CANCEL); } catch { stream.destroy(); }
      }
    }

    function abort(): void { cancel(request.signal.reason); }

    const body = new ReadableStream<Uint8Array>({
      start(value) { controller = value; },
      pull() { stream?.resume(); },
      cancel(reason) {
        // The reader has already closed its own controller. Release ownership
        // and reset only this HTTP/2 stream, leaving other requests usable.
        if (finished) return;
        finished = true;
        clearTimeout(timeout);
        request.signal.removeEventListener("abort", abort);
        request.complete();
        if (stream && !stream.destroyed) {
          try { stream.close(constants.NGHTTP2_CANCEL); } catch { stream.destroy(); }
        }
        void reason;
      },
    }, { highWaterMark: 64 * 1024, size: (chunk) => chunk.byteLength });

    try {
      stream = request.connection.request(request.headers);
      stream.on("error", (error) => finish(error));
      stream.on("aborted", () => finish(failure("HTTP/2 event stream aborted", "H2_STREAM_ABORTED")));
      stream.on("close", () => finish(failure("HTTP/2 event stream closed before completion", "H2_STREAM_CLOSED")));
      stream.on("response", (raw) => {
        if (finished) return;
        try {
          const status = Number(raw[":status"]);
          if (!Number.isInteger(status) || status < 200 || status > 599) throw failure("HTTP/2 response has no valid final status", "H2_INVALID_RESPONSE");
          const headers = new Headers();
          for (const [name, value] of Object.entries(raw)) {
            if (name.startsWith(":") || value === undefined) continue;
            for (const item of Array.isArray(value) ? value : [value]) headers.append(name, String(item));
          }
          const encoding = headers.get("content-encoding");
          if (encoding && encoding !== "identity") throw failure("Flower HTTP/2 transport does not decode compressed responses", "H2_UNSUPPORTED_ENCODING");
          events = status >= 200 && status < 300
            && headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() === "text/event-stream";
          // Long-lived SSE streams use the caller's cancellation signal after
          // headers. Error/ordinary responses retain the whole-body deadline.
          if (events) clearTimeout(timeout);
          const response = new Response([204, 205, 304].includes(status) ? null : body, { status, headers });
          delivered = true;
          resolve(response);
        } catch (error) { cancel(error); }
      });
      stream.on("data", (chunk: Buffer) => {
        if (finished) return;
        received += chunk.length;
        if (!events && received > request.maxResponseBytes) {
          cancel(failure(`HTTP/2 response body exceeds ${request.maxResponseBytes} bytes`, "H2_RESPONSE_TOO_LARGE"));
          return;
        }
        controller.enqueue(chunk);
        if ((controller.desiredSize ?? 0) <= 0) stream!.pause();
      });
      stream.on("end", () => finish());
      request.signal.addEventListener("abort", abort, { once: true });
      if (request.signal.aborted) abort(); else stream.end(request.body);
    } catch (error) { cancel(error); }
  });
}
