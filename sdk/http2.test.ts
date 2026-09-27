import assert from "node:assert/strict";
import { constants as bufferConstants } from "node:buffer";
import { createServer, createSecureServer, constants } from "node:http2";
import { readFileSync } from "node:fs";
import type { Http2ServerRequest, Http2ServerResponse, ServerHttp2Session } from "node:http2";
import type { Socket } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import type { TestContext } from "node:test";
import { FlowerAdmin, FlowerClient } from "./client.ts";
import { createHttp2Transport } from "./http2.ts";
import type { Http2TransportOptions } from "./http2.ts";

async function fixture(t: TestContext, handler: (request: Http2ServerRequest, response: Http2ServerResponse) => void, options: Http2TransportOptions = {}) {
  const sessions = new Set<ServerHttp2Session>();
  let connections = 0;
  const server = createServer(handler);
  server.on("session", (session) => {
    connections++;
    sessions.add(session);
    session.on("error", () => {});
    session.on("close", () => sessions.delete(session));
  });
  server.on("stream", (stream) => stream.on("error", () => {}));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const url = `http://127.0.0.1:${address.port}`;
  const transport = createHttp2Transport({ requestTimeoutMs: 1_000, ...options });
  const client = new FlowerClient(url, { fetch: transport.fetch });
  const admin = new FlowerAdmin(url, { fetch: transport.fetch, adminToken: "test-token" });
  t.after(async () => {
    await transport.close();
    for (const session of sessions) session.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  return { transport, client, admin, url, server, connections: () => connections };
}

async function body(request: Http2ServerRequest): Promise<any> {
  let text = "";
  for await (const chunk of request) text += chunk;
  return JSON.parse(text);
}

function reply(response: Http2ServerResponse, value: unknown = "ok"): void {
  response.setHeader("content-type", "application/json");
  response.end(JSON.stringify({ revision: 1, value, duplicate: false }));
}

test("SSE headers return immediately, outlive the header deadline, and multiplex with ordinary calls", async (t) => {
  let responseStream: Http2ServerResponse | undefined;
  let closed!: () => void;
  const closedPromise = new Promise<void>((resolve) => { closed = resolve; });
  const { transport, client, url, connections } = await fixture(t, (request, response) => {
    if (request.headers.accept === "text/event-stream") {
      responseStream = response;
      response.on("close", closed);
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.write(": connected\n\n");
    } else void body(request).then(() => reply(response));
  }, { requestTimeoutMs: 40 });
  const response = await transport.fetch(url + "/v1/watch", { method: "POST", headers: { accept: "text/event-stream" }, body: "{}" });
  const reader = response.body!.getReader();
  assert.match(new TextDecoder().decode((await reader.read()).value), /connected/);
  await delay(80);
  assert.equal((await client.query("still-usable")).value, "ok");
  responseStream!.write("event: snapshot\ndata: {}\n\n");
  assert.match(new TextDecoder().decode((await reader.read()).value), /event: snapshot/);
  await reader.cancel();
  await Promise.race([closedPromise, delay(1_000).then(() => { throw new Error("SSE cancel did not reset stream"); })]);
  assert.equal((await client.query("after-cancel")).value, "ok");
  assert.equal(connections(), 1);
});

test("SSE waiting readers fail on caller abort and transport shutdown", async (t) => {
  const { transport, url } = await fixture(t, (_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write(": connected\n\n");
  });
  const controller = new AbortController();
  const response = await transport.fetch(url, { method: "POST", headers: { accept: "text/event-stream" }, body: "{}", signal: controller.signal });
  const reader = response.body!.getReader();
  await reader.read();
  const pending = reader.read();
  controller.abort(new Error("stop subscription"));
  await assert.rejects(pending, /stop subscription/);
  const second = await transport.fetch(url, { method: "POST", headers: { accept: "text/event-stream" }, body: "{}" });
  const secondReader = second.body!.getReader();
  await secondReader.read();
  const waiting = secondReader.read();
  const rejection = assert.rejects(waiting, /transport closed/);
  await transport.close();
  await rejection;
});

test("SSE bounds non-streaming error bodies and times out missing headers", async (t) => {
  const { transport, url } = await fixture(t, (request, response) => {
    if (request.url === "/error") {
      response.writeHead(400, { "content-type": "application/json" });
      response.end("x".repeat(1_024));
    } else if (request.url === "/sse-error") {
      response.writeHead(503, { "content-type": "text/event-stream" });
      response.write(": unavailable\n\n");
    }
  }, { maxResponseBytes: 128, requestTimeoutMs: 40 });
  const init = { method: "POST" as const, headers: { accept: "text/event-stream" }, body: "{}" };
  const response = await transport.fetch(url + "/error", init);
  assert.equal(response.status, 400);
  await assert.rejects(response.text(), (error: any) => error.code === "H2_RESPONSE_TOO_LARGE");
  const errorStream = await transport.fetch(url + "/sse-error", init);
  await assert.rejects(errorStream.text(), (error: any) => error.name === "TimeoutError");
  await assert.rejects(transport.fetch(url + "/stall", init), (error: any) => error.name === "TimeoutError");
});

test("SSE transport backpressures an unread stream instead of buffering the whole response", async (t) => {
  let written = 0;
  const { transport, url } = await fixture(t, (_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    const write = () => {
      while (!response.destroyed && written < 16 * 1024 * 1024) {
        written += 16 * 1024;
        if (!response.write("x".repeat(16 * 1024))) { response.once("drain", write); return; }
      }
    };
    write();
  });
  const response = await transport.fetch(url, { method: "POST", headers: { accept: "text/event-stream" }, body: "{}" });
  await delay(50);
  assert.ok(written < 16 * 1024 * 1024, `unread SSE consumed the whole ${written}-byte response`);
  await response.body!.cancel();
});

test("SDK HTTP/2 calls multiplex on one session and preserve methods, headers, and request IDs", async (t) => {
  let concurrent = 0;
  let peak = 0;
  const received: any[] = [];
  const { client, admin, connections } = await fixture(t, (request, response) => {
    void (async () => {
      assert.equal(request.httpVersionMajor, 2);
      assert.equal(request.method, "POST");
      const data = await body(request);
      received.push({ path: request.url, data, token: request.headers.authorization });
      peak = Math.max(peak, ++concurrent);
      await delay(15);
      concurrent--;
      reply(response, data.args ?? null);
    })();
  });
  const values = await Promise.all(Array.from({ length: 12 }, (_, index) => client.call("test", { index }, { requestId: `same-${index}` })));
  assert.equal(values.length, 12);
  assert.ok(peak > 1, "requests overlap on the HTTP/2 connection");
  assert.equal(connections(), 1);
  assert.deepEqual(received.map(({ data }) => data.requestId).sort(), Array.from({ length: 12 }, (_, index) => `same-${index}`).sort());
  await client.query("read", null);
  await client.mutate("write", null, { requestId: "stable", expectedRevision: 1 });
  await admin.deploy({ hash: "hash", javascript: "source" }, { requestId: "deploy" });
  await admin.initialize({ "1": "127.0.0.1:1" });
  assert.equal(connections(), 1, "sequential requests reuse the session too");
  assert.deepEqual(received.slice(12).map(({ path }) => path), ["/v1/query", "/v1/mutate", "/admin/deploy", "/raft/initialize"]);
  assert.equal(received.at(-1).token, "Bearer test-token");
  assert.equal(received.at(-2).data.requestId, "deploy");
  assert.equal(received.at(-3).data.expectedRevision, 1);
  assert.ok(received.slice(0, -2).every(({ token }) => token === undefined), "method calls never carry the operator token");
});

test("a caller deadline cancels a stalled body without cancelling other streams", async (t) => {
  const { client, connections } = await fixture(t, (request, response) => {
    void body(request).then((data) => {
      if (data.name === "stall") { response.writeHead(200); response.write('{"revision":'); }
      else reply(response);
    });
  });
  const deadline = AbortSignal.timeout(40);
  const blocked = assert.rejects(client.query("stall", null, { signal: deadline }), (error) => error === deadline.reason);
  assert.equal((await client.query("other")).value, "ok");
  await blocked;
  assert.equal((await client.query("after-abort")).value, "ok");
  assert.equal(connections(), 1);
});

test("default deadline covers response-body completion and pre-aborted calls open no session", async (t) => {
  const { client, connections } = await fixture(t, (_request, response) => { response.writeHead(200); response.write("{"); }, { requestTimeoutMs: 40 });
  const aborted = AbortSignal.abort(new Error("already cancelled"));
  await assert.rejects(client.call("cancelled", null, { signal: aborted }), (error) => error === aborted.reason);
  assert.equal(connections(), 0);
  const started = performance.now();
  await assert.rejects(client.call("stall"), (error: any) => error.name === "TimeoutError");
  assert.ok(performance.now() - started < 1_000);
});

test("request and response byte limits fail without unbounded buffering", async (t) => {
  let requests = 0;
  const { client, connections } = await fixture(t, (request, response) => {
    requests++;
    void body(request).then((data) => data.name === "huge" ? response.end("x".repeat(1_024)) : reply(response));
  }, { maxRequestBytes: 256, maxResponseBytes: 128 });
  await assert.rejects(client.call("large-request", "x".repeat(300)), (error: any) => error.code === "H2_REQUEST_TOO_LARGE");
  assert.equal(requests, 0);
  assert.equal(connections(), 0);
  await assert.rejects(client.query("huge"), (error: any) => error.code === "H2_RESPONSE_TOO_LARGE");
  assert.equal((await client.query("small")).value, "ok");
});

test("GOAWAY drains accepted streams and sends subsequent calls on a new session", async (t) => {
  let first = true;
  const { client, connections } = await fixture(t, (request, response) => {
    void body(request).then(async () => {
      if (first) {
        first = false;
        response.stream.session!.goaway(constants.NGHTTP2_NO_ERROR, response.stream.id);
        await delay(10);
      }
      reply(response);
    });
  });
  assert.equal((await client.query("first")).value, "ok");
  assert.equal((await client.query("second")).value, "ok");
  assert.equal(connections(), 2);
});

test("lost sessions fail once without implicit replay and later calls reconnect", async (t) => {
  const received: any[] = [];
  const { client, connections } = await fixture(t, (request, response) => {
    void body(request).then((data) => {
      received.push(data);
      if (received.length === 1) response.stream.session!.destroy();
      else reply(response);
    });
  });
  await assert.rejects(client.call("write", { amount: 1 }, { requestId: "keep-me" }));
  assert.equal(received.length, 1, "transport does not replay an uncertain mutation");
  assert.equal((await client.call("write", { amount: 1 }, { requestId: "keep-me" })).value, "ok");
  assert.equal(connections(), 2);
  assert.deepEqual(received[0], received[1]);
});

test("a connection lost after response headers rejects incomplete bodies instead of returning HTTP 200", async (t) => {
  for (const declaredLength of [false, true]) {
    for (const partial of ["", '{"revision":1,"value":"🌸']) {
      await t.test(`${declaredLength ? "declared" : "unspecified"} length, ${partial ? "partial" : "header-only"} body`, async (t) => {
        let socket: Socket | undefined;
        let killed = false;
        const { transport, url, server, connections } = await fixture(t, (request, response) => {
          void body(request).then(() => {
            if (killed) { reply(response); return; }
            const headers: Record<string, string> = { "content-type": "application/json" };
            if (declaredLength) headers["content-length"] = "100";
            response.stream.respond({ ":status": 200, ...headers });
            if (partial) response.stream.write(partial);
            // The peer acknowledges this ping only after it has received the
            // preceding response headers/data. Destroy TCP without END_STREAM,
            // matching a leader killed after sending a successful status.
            response.stream.session!.ping((error) => {
              assert.ifError(error);
              killed = true;
              socket!.destroy();
            });
          });
        });
        server.on("connection", (connection) => { socket = connection; });
        const init = { method: "POST" as const, headers: {}, body: "{}" };
        await assert.rejects(transport.fetch(url, init), (error: any) =>
          /^(?:H2_|ERR_HTTP2_|ECONN)/.test(error.code));
        assert.ok(killed);
        const retry = await transport.fetch(url, init);
        assert.equal((await retry.json()).value, "ok");
        assert.equal(connections(), 2);
      });
    }
  }
});

test("response content-length counts bytes and cleanly completed malformed JSON remains an application error", async (t) => {
  const { transport, client, url } = await fixture(t, (request, response) => {
    void body(request).then((data) => {
      const text = data.name === "malformed" ? "{" : JSON.stringify({ revision: 1, value: "🌸 café", duplicate: false });
      response.writeHead(200, { "content-type": "application/json", "content-length": String(Buffer.byteLength(text)) });
      response.end(text);
    });
  });
  assert.equal((await client.query("unicode")).value, "🌸 café");
  const response = await transport.fetch(url, { method: "POST", headers: {}, body: '{"name":"malformed"}' });
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "{", "transport must not reinterpret an intact invalid application response");
});

test("closing cancels in-flight streams, prevents new calls, and is idempotent", async (t) => {
  let started!: () => void;
  const seen = new Promise<void>((resolve) => { started = resolve; });
  const { client, transport } = await fixture(t, (_request, response) => { response.writeHead(200); response.write("{"); started(); });
  const pending = assert.rejects(client.query("stall"), (error: any) => error.name === "AbortError");
  await seen;
  await transport.close();
  await pending;
  await transport.close();
  await assert.rejects(client.query("closed"), /transport closed/);
});

test("session count is bounded across origins and only HTTP(S) JSON POST is accepted", async (t) => {
  let started!: () => void;
  const seen = new Promise<void>((resolve) => { started = resolve; });
  const { client, transport } = await fixture(t, (_request, response) => { response.writeHead(200); response.write("{"); started(); }, { maxSessions: 1 });
  const pending = assert.rejects(client.query("stall"));
  await seen;
  const init = { method: "POST" as const, headers: {}, body: "{}" };
  await assert.rejects(transport.fetch("http://127.0.0.1:1/", init), (error: any) => error.code === "H2_SESSION_LIMIT");
  await assert.rejects(transport.fetch("ftp://localhost/", init), /http:\/\//);
  await assert.rejects(transport.fetch("http://localhost/", { ...init, headers: { connection: "close" } }), /Unsupported/);
  await transport.close();
  await pending;
  assert.throws(() => createHttp2Transport({ maxResponseBytes: Infinity }), /integer/);
});

test("idle sessions expire and unsupported compressed responses fail explicitly", async (t) => {
  const { client, connections } = await fixture(t, (request, response) => {
    void body(request).then((data) => {
      if (data.name === "compressed") { response.setHeader("content-encoding", "gzip"); response.end("compressed"); }
      else reply(response);
    });
  }, { idleTimeoutMs: 15 });
  assert.equal((await client.query("first")).value, "ok");
  await delay(40);
  assert.equal((await client.query("second")).value, "ok");
  assert.equal(connections(), 2);
  await assert.rejects(client.query("compressed"), (error: any) => error.code === "H2_UNSUPPORTED_ENCODING");
});


test("transport budgets have no arbitrary session or one-GiB ceiling", async () => {
  const transport = createHttp2Transport({ maxSessions: 2048, maxRequestBytes: Math.min(2 * 1024 ** 3, bufferConstants.MAX_LENGTH), maxResponseBytes: Math.min(2 * 1024 ** 3, bufferConstants.MAX_LENGTH) });
  await transport.close();
  assert.throws(() => createHttp2Transport({ maxSessions: Number.MAX_SAFE_INTEGER + 1 }), /maxSessions/);
  assert.throws(() => createHttp2Transport({ requestTimeoutMs: 2 ** 31 }), /requestTimeoutMs/);
  assert.throws(() => createHttp2Transport({ idleTimeoutMs: 2 ** 31 }), /idleTimeoutMs/);
});


test("HTTPS HTTP/2 verifies custom roots and hostname while pooling sessions", async (t) => {
  const fixtures = new URL("../tests/fixtures/tls/", import.meta.url);
  const server = createSecureServer({ key: readFileSync(new URL("localhost.key", fixtures)), cert: readFileSync(new URL("localhost.crt", fixtures)) }, (_request, response) => reply(response, "secure"));
  const sessions = new Set<ServerHttp2Session>();
  let connections = 0;
  server.on("session", (session) => { connections++; sessions.add(session); session.on("error", () => {}); session.on("close", () => sessions.delete(session)); });
  server.on("tlsClientError", () => {});
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address(); assert.ok(address && typeof address === "object");
  const url = `https://localhost:${address.port}`;
  const trusted = createHttp2Transport({ ca: readFileSync(new URL("ca.crt", fixtures)), requestTimeoutMs: 1000 });
  const untrusted = createHttp2Transport({ requestTimeoutMs: 1000 });
  t.after(async () => { await trusted.close(); await untrusted.close(); for (const session of sessions) session.destroy(); await new Promise<void>(resolve => server.close(() => resolve())); });
  const client = new FlowerClient(url, { fetch: trusted.fetch });
  assert.equal((await client.query("value")).value, "secure");
  assert.equal((await client.query("value")).value, "secure");
  assert.equal(connections, 1);
  await assert.rejects(new FlowerClient(url, { fetch: untrusted.fetch }).query("untrusted"));
  await assert.rejects(new FlowerClient(`https://127.0.0.1:${address.port}`, { fetch: trusted.fetch }).query("wrong-name"));
  for (const ca of ["", [], [""], 7, {}, new Uint8Array()]) assert.throws(() => createHttp2Transport({ ca } as any), /ca must/);
});
