import { summary } from "./profile-metrics.mjs";

const identity = resource => resource["service.instance.id"] ?? `${resource["server.address"] ?? "unknown"}/${resource["flower.node.id"] ?? "?"}/${resource["process.pid"] ?? "?"}`;
const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === "object" ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, child]) => [key, canonical(child)])) : value;
const stable = value => JSON.stringify(canonical(value));
const cumulative = value => value === 2 || value === "AGGREGATION_TEMPORALITY_CUMULATIVE";
const delta = value => value === 1 || value === "AGGREGATION_TEMPORALITY_DELTA";

function bucketQuantile(counts, bounds, fraction) {
  const target = counts.reduce((a, b) => a + b, 0) * fraction;
  if (!target) return null;
  let count = 0;
  for (let i = 0; i < counts.length; i++) { count += counts[i]; if (count >= target) return bounds[i] ?? "overflow"; }
  return null;
}

export function summarizeMetricSeries(points, window) {
  const sorted = [...points].sort((a, b) => a.timeMs - b.timeMs);
  const inside = sorted.filter(p => p.timeMs > window.start && p.timeMs <= window.end);
  if (!inside.length) return null;
  const last = inside.at(-1);
  const result = { node: identity(last.resource), name: last.name, kind: last.kind, unit: last.unit, attributes: last.attributes,
    exportsInWindow: inside.length, firstExportMs: inside[0].timeMs, lastExportMs: last.timeMs };
  if (last.kind === "gauge" || last.kind === "sum" && last.monotonic === false && cumulative(last.temporality)) return { ...result, values: summary(inside.map(p => p.value)), lastValue: last.value };
  let intervals;
  if (cumulative(last.temporality)) {
    // Difference adjacent exports only within one process and instrument epoch.
    // Never count a cumulative snapshot more than once, even after a reset.
    intervals = sorted.flatMap((p, index) => {
      if (p.timeMs <= window.start || p.timeMs > window.end) return [];
      const previous = sorted[index - 1];
      if (!previous || previous.timeMs < window.start || p.startTimeMs !== previous.startTimeMs || !cumulative(previous.temporality)) return [];
      if (p.kind === "histogram") {
        if (JSON.stringify(p.bounds) !== JSON.stringify(previous.bounds) || p.count < previous.count) return [];
        const bucketCounts = p.bucketCounts.map((n, i) => n - previous.bucketCounts[i]);
        if (bucketCounts.some(n => n < 0)) return [];
        return [{ ...p, count: p.count - previous.count, sum: p.sum - previous.sum, bucketCounts, intervalStart: previous.timeMs }];
      }
      return [{ ...p, value: p.value - previous.value, intervalStart: previous.timeMs }];
    });
  } else if (delta(last.temporality)) {
    intervals = inside.filter(p => p.startTimeMs !== null && p.startTimeMs >= window.start && p.startTimeMs < p.timeMs)
      .map(p => ({ ...p, intervalStart: p.startTimeMs }));
  } else return { ...result, unavailable: "Unknown metric aggregation temporality" };
  if (!intervals.length) return { ...result, unavailable: "No complete export interval within measured load" };
  result.coverage = { startMs: Math.min(...intervals.map(p => p.intervalStart)), endMs: last.timeMs, intervals: intervals.length,
    durationMs: intervals.reduce((sum, p) => sum + p.timeMs - p.intervalStart, 0) };
  if (last.kind === "sum") return { ...result, value: intervals.reduce((sum, p) => sum + p.value, 0) };
  const sameBounds = intervals.every(p => JSON.stringify(p.bounds) === JSON.stringify(last.bounds));
  if (!sameBounds) return { ...result, unavailable: "Histogram boundaries changed" };
  const count = intervals.reduce((sum, p) => sum + p.count, 0), sum = intervals.reduce((sum, p) => sum + p.sum, 0);
  const bucketCounts = last.bucketCounts.map((_, i) => intervals.reduce((sum, p) => sum + p.bucketCounts[i], 0));
  return { ...result, count, sum, mean: count ? sum / count : null, bounds: last.bounds, bucketCounts,
    p50UpperBound: bucketQuantile(bucketCounts, last.bounds, .5), p95UpperBound: bucketQuantile(bucketCounts, last.bounds, .95), p99UpperBound: bucketQuantile(bucketCounts, last.bounds, .99) };
}

export function buildOtelReport(capture, benchmark) {
  const start = Date.parse(benchmark.loadStartedAt), end = Date.parse(benchmark.loadEndedAt);
  const failures = [];
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start || start < capture.startedAt) throw new Error("Benchmark did not produce a fresh measured load interval");
  const window = { start, end, durationMs: end - start };
  // Whole spans end within load; startup, warmup, drain, and boundary-crossing spans are excluded.
  const spans = capture.records.filter(r => r.type === "span" && r.startTimeMs >= start && r.endTimeMs <= end);
  const metrics = capture.records.filter(r => r.type === "metric");
  const spanGroups = new Map(), metricGroups = new Map(), resources = new Map();
  for (const record of capture.records) resources.set(identity(record.resource), record.resource);
  for (const span of spans) {
    const labels = Object.fromEntries(Object.entries(span.attributes).filter(([, value]) => typeof value !== "number"));
    const key = stable([identity(span.resource), span.name, labels]);
    if (!spanGroups.has(key)) spanGroups.set(key, []);
    spanGroups.get(key).push(span);
  }
  for (const point of metrics) {
    const key = stable([identity(point.resource), point.name, point.kind, point.unit, point.attributes]);
    if (!metricGroups.has(key)) metricGroups.set(key, []);
    metricGroups.get(key).push(point);
  }
  const stages = [...spanGroups.values()].map(records => {
    const first = records[0];
    const numericKeys = [...new Set(records.flatMap(r => Object.entries(r.attributes).filter(([, value]) => typeof value === "number").map(([key]) => key)))];
    return { node: identity(first.resource), name: first.name,
      labels: Object.fromEntries(Object.entries(first.attributes).filter(([, value]) => typeof value !== "number")),
      errors: records.filter(r => r.status === 2 || r.status === "STATUS_CODE_ERROR").length,
      durationMs: summary(records.map(r => r.durationMs)),
      numericAttributes: Object.fromEntries(numericKeys.map(key => [key, summary(records.map(r => r.attributes[key]))])) };
  }).sort((a, b) => b.durationMs.p99 - a.durationMs.p99);
  const metricSeries = [...metricGroups.values()].map(points => summarizeMetricSeries(points, window)).filter(Boolean);
  if (!spans.length) failures.push("No sampled Flower spans wholly inside measured load; use a longer run or a higher sample ratio");
  if (!metricSeries.length) failures.push("No Flower metric exports inside measured load");
  if (!metricSeries.some(series => series.coverage)) failures.push("No complete metric export intervals inside measured load; use a longer run");
  const incomplete = capture.stats.droppedSpans + capture.stats.droppedMetricPoints + capture.stats.oversizedRequests + capture.stats.malformedRequests + capture.stats.invalidRecords + capture.stats.abortedRequests + capture.stats.rejectedRequests + capture.stats.foreignRecords + capture.stats.unsupportedMetrics + capture.stats.locallyDroppedLinks;
  if (incomplete) failures.push("Local capture was incomplete; inspect capture counters and limits");
  return {
    schemaVersion: 1, kind: "flower-otel-diagnostic", runId: capture.runId, benchmarkRunId: benchmark.runId,
    note: "Instrumented diagnostic, not a capacity result. Trace sampling and bounded retention can bias distributions. Spans must fit entirely inside load; they include server and worker traffic, not only primary customer calls. Nested and concurrent spans overlap and must not be summed as elapsed time or CPU use. Metrics are unsampled but bounded by export/capture limits. Cumulative metrics are differenced between consecutive exports entirely inside load and within one process/instrument epoch; edge intervals and resets are excluded. Histogram percentiles are bucket upper bounds, not exact quantiles. Crashes can lose unexported spans and metrics, which the local collector cannot count.",
    passed: failures.length === 0, failures, benchmarkPassed: benchmark.passed,
    benchmark: { binary: benchmark.binary, bundleHash: benchmark.bundleHash, environment: benchmark.environment,
      runtime: benchmark.runtime, goodputRps: benchmark.goodputRps ?? benchmark.application?.goodputRps,
      loadStartedAt: benchmark.loadStartedAt, loadEndedAt: benchmark.loadEndedAt },
    window, capture: { startedAt: new Date(capture.startedAt).toISOString(), ...capture.stats, limits: capture.limits, exporterLoss: "Unavailable: SDK queue overflow, transport failures before receipt, and process crashes cannot be counted by this receiver" },
    resources: [...resources.values()], sampledSpansInLoad: spans.length, stages, metricSeries,
    slowestSpans: [...spans].sort((a, b) => b.durationMs - a.durationMs).slice(0, 100).map(({ resource, ...span }) => ({ ...span, node: identity(resource) })),
  };
}

export function renderOtelReport(report) {
  const escape = value => String(value ?? "—").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
  const n = value => typeof value === "number" ? value.toLocaleString("en-US", { maximumFractionDigits: 4 }) : escape(value);
  const table = (headings, rows, id) => `<div class="table"><table${id ? ` id="${escape(id)}"` : ""}><thead><tr>${headings.map(h => `<th>${escape(h)}</th>`).join("")}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(value => `<td>${value}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  const filter = (id, label, count) => `<div class="filter"><label for="${id}-search">${label}</label><input id="${id}-search" type="search" data-filter="${id}-table" aria-describedby="${id}-count" placeholder="Type to filter rows"><output id="${id}-count" for="${id}-search" aria-live="polite">${count.toLocaleString("en-US")} of ${count.toLocaleString("en-US")} rows</output></div>`;
  const filterScript = `<script>for(const input of document.querySelectorAll('[data-filter]')){const table=document.getElementById(input.dataset.filter);const rows=[...table.tBodies[0].rows];const text=rows.map(row=>row.textContent.toLocaleLowerCase());const output=document.getElementById(input.getAttribute('aria-describedby'));input.addEventListener('input',()=>{const query=input.value.trim().toLocaleLowerCase();let shown=0;rows.forEach((row,index)=>{row.hidden=!text[index].includes(query);if(!row.hidden)shown++});output.textContent=shown.toLocaleString('en-US')+' of '+rows.length.toLocaleString('en-US')+' rows'})}</script>`;
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Flower OpenTelemetry diagnostic</title><style>body{font:15px system-ui,sans-serif;line-height:1.5;color:#17202a;background:#f4f6f8;max-width:1400px;margin:32px auto;padding:0 24px}h1,h2{line-height:1.2}p{max-width:1000px}.table{overflow:auto;background:white;margin:20px 0;border:1px solid #d6dde3}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:9px 12px;border-bottom:1px solid #e2e7ed;vertical-align:top}th{background:#edf1f5;white-space:nowrap}code{font-size:12px}.fail{color:#b22}.ok{color:#176037}a{color:#155ca1}nav{display:flex;flex-wrap:wrap;gap:8px 20px;margin:20px 0}.filter{display:flex;flex-wrap:wrap;align-items:center;gap:8px 16px;margin:16px 0}.filter label{font-weight:600}.filter input{font:inherit;border:1px solid #9ba8b5;border-radius:4px;padding:8px 12px;min-width:240px}.filter output{font-size:13px;color:#536170}[hidden]{display:none!important}h2{scroll-margin-top:20px}</style><h1>Flower OpenTelemetry diagnostic</h1><nav aria-label="Report sections"><a href="#capture">Capture accounting</a><a href="#nodes">Node identities</a><a href="#metrics">Unsampled metrics</a><a href="#spans">Sampled spans</a><a href="#slowest">Slowest spans</a></nav><p class="${report.passed ? "ok" : "fail"}">${report.passed ? "Capture validated" : "Capture incomplete"} · benchmark ${report.benchmarkPassed ? "passed" : "failed"} · ${n(report.sampledSpansInLoad)} sampled spans inside ${n(report.window.durationMs / 1000)} seconds of load</p><p>${escape(report.note)}</p>${report.failures.map(f => `<p class="fail">${escape(f)}</p>`).join("")}<p><a href="summary.json">Structured summary</a> · <a href="capture.ndjson">Sanitized OTLP records</a></p><h2 id="capture">Capture accounting</h2>${table(["Counter", "Value"], Object.entries(report.capture).map(([key, value]) => [escape(key), escape(typeof value === "object" ? JSON.stringify(value) : value)]))}<h2 id="nodes">Node identities</h2>${table(["Instance", "Group", "Node", "PID", "Address"], report.resources.map(r => [escape(identity(r)), n(r["flower.bench.group_index"]), n(r["flower.node.id"]), n(r["process.pid"]), escape(r["server.address"])]))}<h2 id="metrics">Unsampled metrics</h2><p>Counts cover complete export intervals inside load. Percentiles show histogram bucket upper bounds in the stated unit. Each row belongs to one node and attribute set.</p>${filter("metrics", "Filter metrics by node, name, or label", report.metricSeries.length)}${table(["Node / metric", "Labels", "Coverage seconds", "Count / value", "Mean", "p50 ≤", "p95 ≤", "p99 ≤"], report.metricSeries.map(m => [escape(m.node) + "<br><b>" + escape(m.name) + "</b> (" + escape(m.unit) + ")", escape(JSON.stringify(m.attributes)), m.coverage ? n(m.coverage.durationMs / 1000) : escape(m.unavailable ?? "gauge snapshots"), n(m.count ?? m.value ?? m.lastValue), n(m.mean ?? m.values?.mean), n(m.p50UpperBound), n(m.p95UpperBound), n(m.p99UpperBound)]), "metrics-table")}<h2 id="spans">Sampled span durations</h2>${filter("spans", "Filter spans by node, name, or label", report.stages.length)}${table(["Node / span", "Labels", "Samples", "Errors", "Mean ms", "p50 ms", "p95 ms", "p99 ms", "Max ms"], report.stages.map(s => [escape(s.node) + "<br><b>" + escape(s.name) + "</b>", escape(JSON.stringify(s.labels)), n(s.durationMs.count), n(s.errors), ...["mean", "p50", "p95", "p99", "max"].map(k => n(s.durationMs[k]))]), "spans-table")}<h2 id="slowest">Slowest sampled spans</h2><p>Trace and parent IDs in the summary and capture link related stages. Batch spans can link multiple request traces; links are preserved in the summary and capture. A parent can be absent because of the capture window, sampling, export loss, or retention limits.</p>${table(["Node / span", "Duration ms", "Trace ID", "Span / parent ID"], report.slowestSpans.map(s => [escape(s.node) + "<br>" + escape(s.name), n(s.durationMs), `<code>${escape(s.traceId)}</code>`, `<code>${escape(s.spanId)}<br>${escape(s.parentSpanId)}</code>`]))}${filterScript}</html>`;
}
