// Bounded, loopback-only OTLP/HTTP JSON receiver for disposable benchmark runs.
// Keep only known numeric measurements and low-cardinality labels. Never retain
// request headers, raw bodies, exception messages, user arguments, or OTLP config.
import { createServer } from "node:http";

const RESOURCE_KEYS = new Set(["service.name", "service.version", "service.instance.id", "flower.node.id", "server.address", "process.pid", "flower.bench.run_id", "flower.bench.group_index"]);
const LABEL_KEYS = new Set(["http.request.method", "http.route", "http.response.status_code", "rpc.system", "rpc.method", "rpc.service", "error.type", "flower.method", "flower.operation", "flower.stage", "flower.outcome", "flower.mode", "flower.cache", "flower.node.id", "method", "operation", "stage", "outcome", "mode", "cache", "status", "node", "execution", "durability", "kind", "result", "consistency", "route", "reused", "unit", "flow", "flower.execution", "flower.durability", "backend", "reason", "stop_reason", "successor", "early_drain", "component", "method_name"]);
const NUMERIC_KEYS = new Set(["requests", "commands", "bytes", "duplicates", "errors", "prepared", "entries", "changed_keys", "receipts", "state_keys", "cells", "reads", "max_cell_nesting", "cell_created", "cell_reused", "cell_recycle_eligible", "cell_grew", "cell_reset", "cell_initial_bytes_sum", "cell_reset_bytes_sum"]);
const numericAttribute = key => NUMERIC_KEYS.has(key) || /^flower\.[\w.]{1,80}$/.test(key) || /^(?:[a-z][a-z_]{0,70}_(?:us|ms|seconds|bytes|count)|batch_(?:commands|requests|bytes|target_count|target_us|queued|local_unapplied_logs|quorum_unmatched_logs)|group_(?:requests|commands|successor|duplicates|errors|deferred)|speculative_(?:candidates|reused)|serial_worker_(?:jobs|requests))$/.test(key);
const nonnegativeInteger = value => Number.isSafeInteger(Number(value)) && Number(value) >= 0 ? Number(value) : 0;
const finite = value => typeof value === "number" && Number.isFinite(value);
const number = value => (typeof value === "number" || typeof value === "string" && /^-?\d+(?:\.\d+)?$/.test(value)) && Number.isFinite(Number(value)) ? Number(value) : null;
const safeLabel = value => typeof value === "string" && value.length <= 200 && /^[\w.\-:/\[\]{}@]+$/.test(value);
const identity = (value, length) => typeof value === "string" && new RegExp(`^[a-fA-F0-9]{${length}}$`).test(value) ? value.toLowerCase() : null;
const list = value => Array.isArray(value) ? value : [];
export const nanosecondsToMs = value => typeof value === "string" && /^\d{1,21}$/.test(value) || typeof value === "number" && Number.isFinite(value) && value >= 0 ? Number(value) / 1e6 : null;


export function otelResourceAttributes(runId, group = 0) {
  if (!safeLabel(runId) || !Number.isSafeInteger(group) || group < 0) throw new Error("Invalid diagnostic run/group identity");
  return `flower.bench.run_id=${runId},flower.bench.group_index=${group}`;
}

export function safeAttributes(attributes, resource = false) {
  const result = {};
  for (const { key, value } of list(attributes).slice(0, 128)) {
    if (typeof key !== "string" || !value || key.length > 100) continue;
    const numeric = number(value.intValue ?? value.doubleValue);
    if ((resource ? RESOURCE_KEYS.has(key) : LABEL_KEYS.has(key) || numericAttribute(key)) && numeric !== null) result[key] = numeric;
    else if ((resource ? RESOURCE_KEYS : LABEL_KEYS).has(key)) {
      if (typeof value.boolValue === "boolean") result[key] = value.boolValue;
      else if (safeLabel(value.stringValue)) result[key] = value.stringValue;
    }
  }
  return result;
}

export function createCapture({ runId, startedAt = Date.now(), maxRecords = 100_000, maxBytes = 64 * 1024 * 1024 } = {}) {
  if (!safeLabel(runId)) throw new Error("A bounded run identity is required");
  for (const [name, value] of Object.entries({ maxRecords, maxBytes })) {
    if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
  }
  const records = [];
  const stats = { requests: 0, receivedBytes: 0, acceptedSpans: 0, acceptedMetricPoints: 0, retainedBytes: 0,
    droppedSpans: 0, droppedMetricPoints: 0, malformedRequests: 0, oversizedRequests: 0,
    rejectedRequests: 0, abortedRequests: 0, invalidRecords: 0, foreignRecords: 0, unsupportedMetrics: 0, omittedSpanEvents: 0, locallyDroppedLinks: 0, sdkDroppedAttributes: 0, sdkDroppedLinks: 0, sdkDroppedEvents: 0 };
  function retain(record) {
    const size = Buffer.byteLength(JSON.stringify(record)) + 1;
    const suffix = record.type === "span" ? "Spans" : "MetricPoints";
    if (records.length >= maxRecords || stats.retainedBytes + size > maxBytes) { stats[`dropped${suffix}`]++; return; }
    records.push(record); stats.retainedBytes += size; stats[`accepted${suffix}`]++;
  }
  function ingest(signal, payload, receivedAt = Date.now()) {
    if (!payload || typeof payload !== "object" || !Array.isArray(payload[signal === "traces" ? "resourceSpans" : "resourceMetrics"])) throw new Error("Invalid OTLP JSON envelope");
    for (const envelope of payload[signal === "traces" ? "resourceSpans" : "resourceMetrics"]) {
      const resource = safeAttributes(envelope.resource?.attributes, true);
      const matching = resource["flower.bench.run_id"] === runId;
      for (const scope of list(envelope[signal === "traces" ? "scopeSpans" : "scopeMetrics"])) {
        for (const item of list(scope[signal === "traces" ? "spans" : "metrics"])) {
          if (!matching) { stats.foreignRecords++; continue; }
          if (typeof item?.name !== "string" || !/^flower\.[\w.]{1,100}$/.test(item.name)) { stats.invalidRecords++; continue; }
          const base = { resource, name: item.name, receivedAt };
          if (signal === "traces") {
            const startTimeMs = nanosecondsToMs(item.startTimeUnixNano), endTimeMs = nanosecondsToMs(item.endTimeUnixNano);
            if (startTimeMs === null || endTimeMs === null || endTimeMs < startTimeMs || startTimeMs < startedAt || endTimeMs > receivedAt + 1000) { stats.invalidRecords++; continue; }
            const traceId = identity(item.traceId, 32), spanId = identity(item.spanId, 16);
            if (!traceId || !spanId) { stats.invalidRecords++; continue; }
            const links = list(item.links).slice(0, 128).map(link => ({ traceId: identity(link.traceId, 32), spanId: identity(link.spanId, 16), flags: nonnegativeInteger(link.flags) })).filter(link => link.traceId && link.spanId);
            stats.omittedSpanEvents += list(item.events).length;
            stats.sdkDroppedAttributes += nonnegativeInteger(item.droppedAttributesCount);
            stats.sdkDroppedLinks += nonnegativeInteger(item.droppedLinksCount);
            stats.sdkDroppedEvents += nonnegativeInteger(item.droppedEventsCount);
            stats.locallyDroppedLinks += Math.max(0, list(item.links).length - 128);
            retain({ ...base, type: "span", traceId, spanId, parentSpanId: identity(item.parentSpanId, 16),
              flags: nonnegativeInteger(item.flags), links,
              droppedAttributesCount: nonnegativeInteger(item.droppedAttributesCount), droppedLinksCount: nonnegativeInteger(item.droppedLinksCount), droppedEventsCount: nonnegativeInteger(item.droppedEventsCount),
              startTimeMs, endTimeMs, durationMs: endTimeMs - startTimeMs,
              status: [0, 1, 2, "STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"].includes(item.status?.code) ? item.status.code : 0,
              attributes: safeAttributes(item.attributes) });
          } else {
            const kind = ["histogram", "sum", "gauge"].find(key => item[key]);
            if (!kind) { stats.unsupportedMetrics++; continue; }
            for (const point of list(item[kind].dataPoints)) {
              const timeMs = nanosecondsToMs(point.timeUnixNano), startTimeMs = nanosecondsToMs(point.startTimeUnixNano);
              if (timeMs === null || timeMs < startedAt || timeMs > receivedAt + 1000) { stats.invalidRecords++; continue; }
              const record = { ...base, type: "metric", kind, unit: safeLabel(item.unit) ? item.unit : "", timeMs, startTimeMs,
                temporality: [1, 2, "AGGREGATION_TEMPORALITY_DELTA", "AGGREGATION_TEMPORALITY_CUMULATIVE"].includes(item[kind].aggregationTemporality) ? item[kind].aggregationTemporality : null,
                monotonic: typeof item[kind].isMonotonic === "boolean" ? item[kind].isMonotonic : null, attributes: safeAttributes(point.attributes) };
              if (kind === "histogram") {
                const bounds = list(point.explicitBounds), counts = list(point.bucketCounts).map(number), count = number(point.count), sum = number(point.sum);
                if (bounds.length > 256 || counts.length !== bounds.length + 1 || !bounds.every((n, i) => finite(n) && (i === 0 || n > bounds[i - 1]))
                  || !counts.every(n => Number.isSafeInteger(n) && n >= 0) || !Number.isSafeInteger(count) || count < 0 || counts.reduce((a, b) => a + b, 0) !== count
                  || sum === null) { stats.invalidRecords++; continue; }
                Object.assign(record, { count, sum, bounds, bucketCounts: counts });
              } else {
                record.value = number(point.asDouble ?? point.asInt);
                if (record.value === null) { stats.invalidRecords++; continue; }
              }
              retain(record);
            }
          }
        }
      }
    }
  }
  return { runId, startedAt, limits: { maxRecords, maxBytes }, records, stats, ingest };
}

export async function startCollector({ capture, token = capture.runId, maxRequestBytes = 4 * 1024 * 1024, maxConcurrentRequests = 8 } = {}) {
  for (const [name, value] of Object.entries({ maxRequestBytes, maxConcurrentRequests })) {
    if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
  }
  let active = 0;
  const sockets = new Set();
  const server = createServer(async (request, response) => {
    const respond = (code, value = {}) => {
      if (!response.destroyed) { response.writeHead(code, { "content-type": "application/json", connection: "close" }); response.end(JSON.stringify(value)); }
    };
    capture.stats.requests++;
    const signal = request.url === `/${token}/v1/traces` ? "traces" : request.url === `/${token}/v1/metrics` ? "metrics" : null;
    if (request.method !== "POST" || !signal || !/^application\/json(?:;|$)/i.test(request.headers["content-type"] ?? "")
      || request.headers["content-encoding"] && request.headers["content-encoding"] !== "identity") {
      capture.stats.rejectedRequests++; request.resume(); respond(400); return;
    }
    if (active >= maxConcurrentRequests) { capture.stats.rejectedRequests++; request.resume(); respond(429); return; }
    active++;
    const chunks = []; let size = 0;
    try {
      for await (const chunk of request) {
        size += chunk.length; capture.stats.receivedBytes += chunk.length;
        if (size > maxRequestBytes) { capture.stats.oversizedRequests++; respond(413); return; }
        chunks.push(chunk);
      }
      let payload;
      try { payload = JSON.parse(Buffer.concat(chunks).toString("utf8")); }
      catch { capture.stats.malformedRequests++; respond(400); return; }
      const before = capture.stats.droppedSpans + capture.stats.droppedMetricPoints;
      try { capture.ingest(signal, payload); }
      catch { capture.stats.malformedRequests++; respond(400); return; }
      const rejected = capture.stats.droppedSpans + capture.stats.droppedMetricPoints - before;
      respond(200, rejected ? { partialSuccess: { [signal === "traces" ? "rejectedSpans" : "rejectedDataPoints"]: String(rejected), errorMessage: "Local diagnostic capture limit reached" } } : {});
    } catch { capture.stats.abortedRequests++; respond(400); }
    finally { active--; }
  });
  server.requestTimeout = 10_000;
  server.headersTimeout = 10_000;
  server.on("connection", socket => { sockets.add(socket); socket.once("close", () => sockets.delete(socket)); socket.setTimeout(10_000, () => socket.destroy()); });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  let closed;
  return { endpoint: `http://127.0.0.1:${server.address().port}/${token}`, limits: { maxRequestBytes, maxConcurrentRequests },
    close() { return closed ??= new Promise((resolve, reject) => { server.close(error => error ? reject(error) : resolve()); for (const socket of sockets) socket.destroy(); }); } };
}
