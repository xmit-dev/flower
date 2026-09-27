import {offeredPanel} from "./offered-report.mjs";
import { GUEST_LABELS, guestOf } from "./guests.mjs";
/** Render a portable benchmark report. No remote assets or scripts are required. */
import { LATENCY_COLORS, duration, latencyCell, latencyDomain, latencyHistogram, latencyRange, latencySparkline, latencyTable, usableHistogram } from "./histogram.mjs";
import { summarizeApplication } from "./metrics.mjs";

const object = (value) => value && typeof value === "object" && !Array.isArray(value) ? value : {};
const array = (value) => Array.isArray(value) ? value : [];
const finite = (value) => typeof value === "number" && Number.isFinite(value);
const positive = (value) => finite(value) && value >= 0 ? value : null;
const escape = (value) => String(value ?? "—").replace(/[&<>"']/g, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[character]));
const number = (value, decimals = 1) => finite(value)
  ? value.toLocaleString("en-US", { maximumFractionDigits: decimals, minimumFractionDigits: decimals }) : "—";
const count = (value) => number(value, 0);
const ms = (value) => finite(value) ? `${number(value)} ms` : "—";
const seconds = (value) => finite(value) ? `${number(value / 1_000, 2)} s` : "—";
const percent = (value) => finite(value) ? `${number(value * 100, 0)}%` : "—";
const label = (value) => String(value).replaceAll("_", " ").replace(/^./, (first) => first.toUpperCase());
const time = (value) => {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) return escape(value);
  return escape(new Date(value).toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC"));
};

const COLORS = ["#177b5a", "#df7b38", "#6267a4", "#b4565c"];

function table(headers, rows, { caption, className = "" } = {}) {
  return `<div class="table-scroll"><table class="${className}">${caption ? `<caption>${escape(caption)}</caption>` : ""}<thead><tr>${headers.map((header) => `<th scope="col">${escape(header)}</th>`).join("")}</tr></thead><tbody>${rows.length
    ? rows.map((row) => `<tr>${row.map((cell, index) => index === 0 ? `<th scope="row">${cell}</th>` : `<td>${cell}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${headers.length}" class="empty">No measurements available.</td></tr>`}</tbody></table></div>`;
}

function metric(title, value, detail, accent = "") {
  return `<article class="metric ${accent}"><p class="eyebrow">${escape(title)}</p><p class="metric-value">${value}</p><p class="metric-note">${detail}</p></article>`;
}

function latencyMetric(title, histogram, detail) {
  if (!usableHistogram(histogram)) return metric(title, "—", detail);
  return `<article class="metric"><p class="eyebrow">${escape(title)}</p>${latencySparkline(histogram, { label: title, width: 240, height: 48 })}<p class="metric-note">p50 ${escape(duration(histogram.p50))} · p99 ${escape(duration(histogram.p99))}. ${detail}</p></article>`;
}

/** Rows of histograms sharing one log axis, so phases or methods compare at a glance. */
function latencyRows(rows, { caption, color = LATENCY_COLORS.other }) {
  const domain = latencyDomain(rows.map(([, histogram]) => histogram), { bins: 24 });
  return table(["Population", "Calls", "Latency"], rows.map(([name, histogram]) => [escape(name), count(object(histogram).samples), latencyCell(histogram, { domain, color, label: name })]),
    { caption: `${caption} One log axis from ${latencyRange(domain)}; each line marks p99.` });
}

function lifecycleCharts(lifecycle) {
  const charts = [["Oven lateness", lifecycle.timerLatenessMs], ["Order to door", lifecycle.orderToDeliveryMs]];
  return `<div class="charts-grid">${charts.map(([name, histogram]) => `<figure class="chart"><p class="latency-title">${escape(name)}<span>${count(object(histogram).samples)} samples</span></p>${latencyHistogram(histogram, { label: name, noun: "samples", width: 440, height: 140 })}</figure>`).join("")}</div><details><summary>Show as tables</summary><div class="two-up">${charts.map(([name, histogram]) => {
    const value = object(histogram);
    return latencyTable(histogram, `${name}: ${count(value.samples)} samples, ${count(value.invalidSamples)} invalid, ${count(value.overflowSamples)} beyond 24 hours`, { noun: "samples" });
  }).join("")}</div></details>`;
}

function definitionList(entries) {
  return `<dl class="facts">${entries.map(([key, value]) => `<div><dt>${escape(key)}</dt><dd>${value}</dd></div>`).join("")}</dl>`;
}

function customerPanel(report, application) {
  const load = object(report.phases?.load);
  const logical = object(load.operations);
  const completedRate = (value) => application.durationMs && finite(value) ? value / (application.durationMs / 1_000) : null;
  const actualMix = application.completed ? `${percent(application.reads.fraction)} reads / ${percent(application.mutations.fraction)} mutations` : "Unavailable";
  return `<section id="customers" class="panel"><div class="section-heading"><div><p class="eyebrow">The customer's view</p><h2>Successful work, precisely counted.</h2></div><span class="tag">One Raft group</span></div>${definitionList([["Successful customer mix", escape(actualMix)], ["Successful customer calls", count(application.completed)], ["Failed customer calls", count(application.failed)], ["Measured load duration", seconds(application.durationMs)], ["Load worker completions excluded", count(application.excludedWorkerCompletions)], ["Load replay completions excluded", count(application.excludedReplayCompletions)]])}${table(["Load measurement", "Successful completions", "Goodput / s", "Successful customer share"], [
    ["Primary customer reads", count(application.available ? application.reads.completed : null), number(application.available ? completedRate(application.reads.completed) : null), percent(application.reads.fraction)],
    ["Primary customer mutations", count(application.available ? application.mutations.completed : null), number(application.available ? completedRate(application.mutations.completed) : null), percent(application.mutations.fraction)],
    ["All primary customer calls", count(application.completed), number(application.goodputRps), application.completed ? "100%" : "—"],
    ["All logical calls, including workers and replays", count(logical.completed), number(logical.throughputPerSecond), "Includes non-customer traffic"],
    ["Raw successful HTTP responses", count(load.successes), number(load.throughputPerSecond), "Includes retries and non-customer traffic"],
  ], { caption: "Customer goodput uses actual successful primary customer calls and the measured load duration." })}<p class="note"><strong>Counting definition:</strong> successful <code>${report.options?.readConsistency === "replica-local" ? "pizza.shop.local" : "pizza.shop"}</code>, <code>pizza.order</code>, and <code>pizza.tip</code> logical calls per measured load second. Transport retries count once when their customer call succeeds. Explicit <code>.replay</code> probes, worker claims and deliveries, polling, setup, audit, and every failed call contribute zero customer goodput. Actual successful read/mutation shares appear above.</p><p class="small muted">The run verdict covers correctness and completion of any requested CPU profile. Customer goodput is an average over the recorded load phase; individual intervals can differ.</p></section>`;
}

/** Bar heights represent only recorded aggregates; never fabricate time samples. */
function groupedChart(id, title, description, groups, series, unit) {
  const numeric = groups.flatMap((group) => group.values.map(positive)).filter((value) => value !== null);
  if (!numeric.length) return `<div class="chart-empty"><p>No measurements yet.</p><span>${escape(description)}</span></div>`;
  const max = Math.max(...numeric, 0);
  const scale = max || 1;
  const width = 680;
  const height = 265;
  const left = 65;
  const right = 18;
  const top = 20;
  const bottom = 55;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const groupWidth = plotWidth / Math.max(groups.length, 1);
  const barWidth = Math.min(28, groupWidth * 0.7 / Math.max(series.length, 1));
  const ticks = Array.from({ length: 5 }, (_, index) => {
    const fraction = index / 4;
    const y = top + plotHeight * (1 - fraction);
    return `<line class="grid-line" x1="${left}" x2="${width - right}" y1="${y}" y2="${y}"/><text class="axis-label" x="${left - 10}" y="${y + 4}" text-anchor="end">${escape(number(scale * fraction, scale < 10 ? 1 : 0))}</text>`;
  }).join("");
  const bars = groups.map((group, groupIndex) => {
    const center = left + groupWidth * (groupIndex + 0.5);
    const totalWidth = series.length * barWidth + (series.length - 1) * 4;
    return group.values.map((value, seriesIndex) => {
      if (positive(value) === null) return "";
      const barHeight = value / scale * plotHeight;
      const x = center - totalWidth / 2 + seriesIndex * (barWidth + 4);
      return `<rect x="${x}" y="${top + plotHeight - barHeight}" width="${barWidth}" height="${barHeight}" rx="3" fill="${COLORS[seriesIndex % COLORS.length]}"><title>${escape(`${group.name} · ${series[seriesIndex]}: ${number(value)} ${unit}`)}</title></rect>`;
    }).join("") + `<text class="axis-label group-label" x="${center}" y="${height - 27}" text-anchor="middle">${escape(group.name)}</text>${group.detail ? `<text class="axis-label small-axis" x="${center}" y="${height - 11}" text-anchor="middle">${escape(group.detail)}</text>` : ""}`;
  }).join("");
  return `<figure class="chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="${id}-title ${id}-desc"><title id="${id}-title">${escape(title)}</title><desc id="${id}-desc">${escape(description)} ${escape(groups.map((group) => `${group.name}: ${group.values.map((value, index) => `${series[index]} ${number(value)} ${unit}`).join(", ")}`).join(". "))}</desc>${ticks}${bars}</svg><figcaption class="legend">${series.map((name, index) => `<span><i style="background:${COLORS[index % COLORS.length]}"></i>${escape(name)}</span>`).join("")}<span class="unit">${escape(unit)} · linear scale</span></figcaption></figure>`;
}

function lineChart(id, title, description, samples, series, unit, crashes) {
  const points = samples.filter((sample) => positive(sample.elapsedMs) !== null);
  const recorded = points.flatMap((point) => series.map(({ field }) => positive(point[field]))).filter((value) => value !== null);
  if (!points.length || !recorded.length) return `<div class="chart-empty"><p>No time samples available.</p><span>${escape(description)}</span></div>`;
  const width = 680;
  const height = 265;
  const left = 65;
  const right = 18;
  const top = 25;
  const bottom = 45;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const maxX = Math.max(...points.map((point) => point.elapsedMs), 1);
  const maxY = Math.max(...recorded, 1);
  const x = (value) => left + value / maxX * plotWidth;
  const y = (value) => top + plotHeight * (1 - value / maxY);
  const ticks = Array.from({ length: 5 }, (_, index) => {
    const fraction = index / 4;
    const tickY = y(maxY * fraction);
    const tickX = x(maxX * fraction);
    return `<line class="grid-line" x1="${left}" x2="${width - right}" y1="${tickY}" y2="${tickY}"/><text class="axis-label" x="${left - 10}" y="${tickY + 4}" text-anchor="end">${escape(number(maxY * fraction, maxY < 10 ? 1 : 0))}</text><text class="axis-label" x="${tickX}" y="${height - 22}" text-anchor="middle">${escape(number(maxX * fraction / 1_000, 0))}s</text>`;
  }).join("");
  const lines = series.map(({ name, field }, index) => {
    let path = "";
    let continued = false;
    for (const point of points) {
      const value = positive(point[field]);
      if (value === null) { continued = false; continue; }
      path += `${continued ? "L" : "M"}${x(point.elapsedMs).toFixed(2)},${y(value).toFixed(2)} `;
      continued = true;
    }
    return `<path d="${path}" fill="none" stroke="${COLORS[index % COLORS.length]}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"><title>${escape(name)}</title></path>`;
  }).join("");
  const markers = crashes.filter((event) => positive(event.elapsedMs) !== null && event.elapsedMs <= maxX).map((event) => {
    const markerX = x(event.elapsedMs);
    return `<line x1="${markerX}" x2="${markerX}" y1="${top - 5}" y2="${top + plotHeight}" stroke="${COLORS[3]}" stroke-width="1.4" stroke-dasharray="4 4"><title>${escape(`Leader crash at ${seconds(event.elapsedMs)} after load start`)}</title></line><text class="axis-label" x="${markerX}" y="${top - 11}" text-anchor="middle" style="fill:${COLORS[3]}">Crash</text>`;
  }).join("");
  const first = points[0];
  const last = points.at(-1);
  const summary = series.map(({ name, field }) => `${name}: first ${number(first[field])}, last ${number(last[field])} ${unit}`).join(". ");
  return `<figure class="chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="${id}-title ${id}-desc"><title id="${id}-title">${escape(title)}</title><desc id="${id}-desc">${escape(description)} ${points.length} samples over ${escape(seconds(maxX))} since load start. ${escape(summary)}.</desc>${ticks}${markers}${lines}</svg><figcaption class="legend">${series.map(({ name }, index) => `<span><i style="background:${COLORS[index % COLORS.length]}"></i>${escape(name)}</span>`).join("")}<span class="unit">${escape(unit)} · elapsed load time</span></figcaption></figure>`;
}

function timeSeriesPanel(report) {
  const samples = array(report.timeline).map(object);
  if (!samples.length) return "";
  const crashes = array(report.chaos).map(object);
  const summaries = samples.filter((sample) => positive(sample.elapsedMs) !== null);
  return `<section id="timeline" class="panel"><div class="section-heading"><div><p class="eyebrow">The rush, second by second</p><h2>How the run unfolded</h2></div><span class="tag">${count(samples.length)} recorded time samples</span></div><div class="charts-grid"><div><h3 class="chart-title">Observed completion rate</h3><p class="chart-subtitle">Each point summarizes the interval since the previous sample.</p>${lineChart("timeline-throughput", "Interval throughput over time", "Successful HTTP responses and completed logical calls per second in each sampling interval. Dashed vertical lines mark recorded leader crashes.", samples, [{ name: "HTTP responses", field: "successesPerSecond" }, { name: "Logical calls", field: "logicalPerSecond" }], "events / s", crashes)}</div><div><h3 class="chart-title">Cumulative HTTP tail latency</h3><p class="chart-subtitle">p95 of attempts since load began; not an interval percentile.</p>${lineChart("timeline-latency", "Cumulative HTTP p95 over time", "The latency p95 is cumulative from load start through each sample, including drain. It is not a moving-window or interval percentile.", samples, [{ name: "Cumulative p95", field: "p95Ms" }], "ms", crashes)}</div><div><h3 class="chart-title">Orders placed and delivered</h3><p class="chart-subtitle">Acknowledgements observed by the benchmark driver.</p>${lineChart("timeline-orders", "Acknowledged orders and deliveries over time", "Cumulative acknowledged order and delivery counts. Their gap shows business work still waiting for completion.", samples, [{ name: "Orders", field: "ordersAcknowledged" }, { name: "Deliveries", field: "deliveriesAcknowledged" }], "orders", crashes)}</div><div><h3 class="chart-title">Driver resident memory</h3><p class="chart-subtitle">Samples include the driver's runtime and retained workload records.</p>${lineChart("timeline-memory", "Driver resident memory over time", "Benchmark driver resident memory sampled over elapsed load time.", samples, [{ name: "Driver RSS", field: "driverRssMiB" }], "MiB", crashes)}</div></div><p class="note">Lines connect observed samples; no measurements are interpolated into the underlying data. Throughput is an interval rate, while p95 is cumulative. The driver and database share a machine. Any crash marker is positioned relative to the start of measured load.</p><details><summary>Recorded time samples</summary>${table(["Elapsed", "Phase", "HTTP attempts / s", "HTTP success / s", "Logical complete / s", "Retries", "HTTP failures", "Cumulative p95", "Orders", "Deliveries", "Driver MiB"], summaries.map((sample) => [seconds(sample.elapsedMs), escape(sample.phase), number(sample.attemptsPerSecond), number(sample.successesPerSecond), number(sample.logicalPerSecond), count(sample.retries), count(sample.failures), ms(sample.p95Ms), count(sample.ordersAcknowledged), count(sample.deliveriesAcknowledged), number(sample.driverRssMiB)]), { caption: "Retries and HTTP failures are counts for each interval. Cumulative p95 covers all attempts since load began." })}</details></section>`;
}

function errorList(errors) {
  const entries = Object.entries(object(errors));
  return entries.length ? entries.map(([name, value]) => `<code>${escape(name)}</code> ${count(value)}`).join(" · ") : "None recorded";
}

function phaseDetails(name, raw) {
  const phase = object(raw);
  const logical = object(phase.operations);
  const methods = Object.entries(object(phase.perMethod));
  const logicalMethods = Object.entries(object(logical.perMethod));
  const httpDomain = latencyDomain(methods.map(([, value]) => object(value).latencyMs), { bins: 24 });
  const logicalDomain = latencyDomain(logicalMethods.map(([, value]) => object(value).latencyMs), { bins: 24 });
  const httpRows = methods.map(([method, rawValue]) => {
    const value = object(rawValue);
    return [escape(method), count(value.attempts), count(value.successes), count(value.failures), count(value.retries), count(value.duplicates), number(value.throughputPerSecond), latencyCell(value.latencyMs, { domain: httpDomain, label: `${method} HTTP attempts` })];
  });
  const logicalRows = logicalMethods.map(([method, rawValue]) => {
    const value = object(rawValue);
    return [escape(method), count(value.count), count(value.completed), count(value.failed), count(value.duplicates), number(value.throughputPerSecond), latencyCell(value.latencyMs, { domain: logicalDomain, label: `${method} logical calls` })];
  });
  return `<details class="phase-detail"${name === "load" ? " open" : ""}><summary><span>${escape(label(name))}</span><span class="summary-meta">${seconds(phase.durationMs)} · ${count(phase.attempts)} HTTP attempts</span></summary><div class="detail-body"><div class="phase-counters">${definitionList([
    ["Successful HTTP / s", number(phase.throughputPerSecond)], ["Completed logical / s", number(logical.throughputPerSecond)],
    ["HTTP failures", count(phase.failures)], ["Logical failures", count(logical.failed)],
  ])}</div><p class="small"><strong>HTTP error categories:</strong> ${errorList(phase.errors)}</p>${table(["HTTP method", "Attempts", "Succeeded", "Failed", "Retries", "Duplicates", "Success / s", "Latency"], httpRows, { caption: `${label(name)} HTTP attempts. Latency histograms share one log axis from ${latencyRange(httpDomain)}; each line marks p99.` })}<details class="nested-detail"><summary>Logical calls, including explicit replays</summary>${table(["Logical method", "Calls", "Completed", "Failed", "Duplicates", "Complete / s", "Latency"], logicalRows, { caption: `One logical call includes discovery, retries, and backoff. Explicit replay checks are separate .replay calls. Latency histograms share one log axis from ${latencyRange(logicalDomain)}.` })}</details></div></details>`;
}

const COMPARE_OPTIONS = ["duration", "warmup", "drain", "concurrency", "workers", "shops", "hotShops", "hotProbability", "maxOrders", "bakeMs", "leaseMs", "duplicateRate", "abandonRate", "pollMs", "requestTimeoutMs", "retryBudgetMs", "nodes", "seed", "chaos", "initialization", "queryRouting", "readConsistency", "tenants", "groups"];

function comparison(report, baseline) {
  if (!baseline) return "";
  const current = object(report.phases?.load);
  const previous = object(baseline.phases?.load);
  const currentOptions = object(report.options);
  const previousOptions = object(baseline.options);
  const currentApplication = summarizeApplication(report);
  const baselineApplication = summarizeApplication(baseline);
  const differing = COMPARE_OPTIONS.filter((key) => JSON.stringify(currentOptions[key]) !== JSON.stringify(previousOptions[key]));
  if (Boolean(currentOptions.cpuProfile || report.cpuProfile?.perturbsPerformance) !== Boolean(previousOptions.cpuProfile || baseline.cpuProfile?.perturbsPerformance)) differing.push("CPU profiling enabled");
  if (Boolean(currentOptions.http2) !== Boolean(previousOptions.http2)) differing.push("application HTTP protocol");
  if (JSON.stringify(report.runtime ?? null) !== JSON.stringify(baseline.runtime ?? null)) differing.push("runtime engine/settings metadata");
  const environmentDifferences = ["cpu", "os", "arch", "node"].filter((key) => report.environment?.[key] !== baseline.environment?.[key]);
  const rows = [
    ["Application goodput / s", baselineApplication.goodputRps, currentApplication.goodputRps, false],
    ["All completed logical calls / s", previous.operations?.throughputPerSecond, current.operations?.throughputPerSecond, false],
    ["Successful HTTP responses / s", previous.throughputPerSecond, current.throughputPerSecond, false],
    ["Logical p99, ms", previous.operations?.latencyMs?.p99, current.operations?.latencyMs?.p99, true],
    ["HTTP p99, ms", previous.latencyMs?.p99, current.latencyMs?.p99, true],
    ["Oven lateness p95, ms", baseline.lifecycle?.timerLatenessMs?.p95, report.lifecycle?.timerLatenessMs?.p95, true],
    ["Order to delivery p95, ms", baseline.lifecycle?.orderToDeliveryMs?.p95, report.lifecycle?.orderToDeliveryMs?.p95, true],
  ].map(([name, before, after, lowerIsBetter]) => {
    let change = "—";
    if (finite(before) && finite(after) && before > 0) {
      const delta = (after - before) / before * 100;
      const better = lowerIsBetter ? delta < 0 : delta > 0;
      change = `<span class="${delta === 0 ? "" : better ? "good-text" : "bad-text"}">${delta > 0 ? "+" : ""}${number(delta)}%</span> <span class="muted">${lowerIsBetter ? "lower is better" : "higher is better"}</span>`;
    } else if (before === 0 && after === 0) change = "Unchanged";
    return [escape(name), number(before), number(after), change];
  });
  return `<section id="comparison" class="panel"><div class="section-heading"><div><p class="eyebrow">Before &amp; after</p><h2>Compare the runs</h2></div><span class="tag">Two observations, not a statistical study</span></div><p class="small">Baseline: ${time(baseline.startedAt)} · ${baseline.passed === true ? "passed" : "did not pass"}. Current: ${time(report.startedAt)}.</p>${differing.length || environmentDifferences.length
    ? `<div class="notice warning"><strong>These runs have different conditions.</strong> ${differing.length ? `Workload: ${escape(differing.join(", "))}. ` : ""}${environmentDifferences.length ? `Environment: ${escape(environmentDifferences.join(", "))}.` : ""} Changes cannot be attributed to implementation alone.</div>`
    : '<p class="small">Recorded workload settings and basic host descriptions match. Shared-host activity, election timing, and scheduling can still vary; repeat trials before drawing conclusions.</p>'}<p class="small">Successful customer mutation share: baseline <strong>${percent(baselineApplication.mutations.fraction)}</strong>, current <strong>${percent(currentApplication.mutations.fraction)}</strong>. Application goodput excludes workers, receipt replay probes, and errors in both runs.</p>${table(["Measurement", "Baseline", "Current", "Change"], rows, { caption: "Load-phase rates and percentiles; lifecycle spans load and drain." })}</section>`;
}

function cpuProfilePanel(rawProfile) {
  if (!rawProfile) return "";
  const profile = object(rawProfile);
  const summary = object(profile.summary);
  const frameTable = (frames, kind) => table(["Frame", "Thread observations", kind === "wait" ? "Share of known waiting observations" : "Share of active observations"], array(frames).map((raw) => {
    const frame = object(raw);
    return [`<code>${escape(frame.frame)}</code>`, count(frame.samples), percent(frame.fraction)];
  }), { caption: kind === "inclusive" ? "Inclusive counts overlap across frames. Recursive occurrences count once per stack observation; the denominator excludes recognized waiting stacks." : kind === "self" ? "Self counts use the deepest sampled frame; the denominator excludes recognized waiting stacks." : "A stack is assigned once to its deepest recognized blocking primitive." });
  const relativePath = typeof profile.relativePath === "string" && profile.relativePath.length ? profile.relativePath : null;
  const link = relativePath ? `<a href="${escape("./" + relativePath.split("/").map(encodeURIComponent).join("/"))}">Open raw sample text</a>` : escape(profile.outputPath);
  const categories = array(summary.activeCategories).length ? `<h3>Candidate active work by subsystem</h3>${table(["Nearest recognized subsystem", "Thread observations", "Share of active observations"], array(summary.activeCategories).map((entry) => [escape(entry.category), count(entry.samples), percent(entry.fraction)]), { caption: "Mutually exclusive heuristic attribution: each candidate active observation is assigned to its nearest recognized subsystem frame. These categories sum to active observations, unlike inclusive frames." })}` : "";
  const threadGroups = array(summary.threadGroups).length ? `${table(["Thread name", "Physical threads", "All observations", "Candidate active", "Known waiting", "Active share within group"], array(summary.threadGroups).map((entry) => [escape(entry.thread), count(entry.physicalThreads), count(entry.totalThreadSamples), count(entry.activeThreadSamples), count(entry.waitingThreadSamples), percent(entry.activeFraction)]), { caption: "Thread groups combine identical thread names, including short-lived evaluator threads. Raw sample text preserves individual threads." })}<p class="small muted">${count(summary.threadCount)} physical threads observed. Individual rows below retain the 50 threads with the most candidate active observations; ${count(summary.omittedThreadRows)} rows omitted.</p>` : "";
  return `<section id="cpu-profile" class="panel"><div class="section-heading"><div><p class="eyebrow">Inside the initial leader</p><h2>Native stack sampling</h2></div><span class="tag ${profile.status === "complete" ? "" : "danger-tag"}">${escape(profile.status)}</span></div><div class="notice warning"><strong>Profiling perturbs performance.</strong> Treat this run as a diagnostic observation, and repeat without profiling to establish throughput.</div>${definitionList([["Selected leader node / PID", `${count(profile.node)} / ${count(profile.pid)}`], ["Requested sample duration / interval", `${number(profile.requestedDurationSeconds)} s / ${ms(profile.intervalMs)}`], ["Profiler launched after load start", ms(profile.launchedAfterLoadStartMs)], ["Profiler process elapsed time", ms(profile.elapsedMs)], ["All-thread observations", count(summary.totalThreadSamples)], ["Candidate active / known waiting observations", `${count(summary.activeThreadSamples)} / ${count(summary.waitingThreadSamples)}`], ["Durable storage sync observations", count(summary.storageSyncThreadSamples)], ["Raw profile", link]])}<p class="note">${escape(profile.scope ?? "Only the initially selected leader is sampled.")} The requested sampling duration differs from profiler elapsed time because attaching and symbolication take additional time. Sampling scope is described above; this is not a separately stabilized phase.</p>${profile.error || profile.summaryError ? `<p class="notice bad">${escape(profile.error ?? profile.summaryError)}</p>` : ""}<p class="note"><strong>Thread observations are not CPU utilization.</strong> Known waiting stacks are separated using blocking primitive names. Candidate active stacks can still contain unrecognized blocking or off-CPU time. Blocking-capable <code>fcntl</code> under durable redb sync is also excluded from candidate active work; native samples cannot separate its kernel work from I/O waiting. Frame percentages below use only candidate active observations; they never divide by all idle threads.</p>${summary.available ? `${categories}<h3>Hottest active inclusive frames</h3>${frameTable(summary.activeInclusive, "inclusive")}<h3>Hottest active self frames</h3>${frameTable(summary.activeSelf, "self")}<details><summary>Per-thread observations and known waiting frames</summary>${threadGroups}${table(["Thread", "All observations", "Candidate active", "Known waiting", "Active share within this thread"], array(summary.threads).map((raw) => {
    const thread = object(raw);
    return [escape(thread.thread), count(thread.totalThreadSamples), count(thread.activeThreadSamples), count(thread.waitingThreadSamples), percent(thread.activeFraction)];
  }), { caption: "Per-thread activity heuristic, not OS CPU utilization. Sampling all threads can produce more observations than sampling intervals." })}${frameTable(summary.waitingFrames, "wait")}</details>` : '<p class="empty">No usable native call tree was summarized.</p>'}<details><summary>Profiler command, timing, and diagnostics</summary><pre>${escape(JSON.stringify({ tool: profile.tool, args: profile.args, startedAt: profile.startedAt, finishedAt: profile.finishedAt, exitCode: profile.exitCode, exitSignal: profile.exitSignal, rawBytes: profile.rawBytes, cleanupIncomplete: profile.cleanupIncomplete, stdout: profile.stdout, stderr: profile.stderr, summaryError: profile.summaryError }, null, 2))}</pre></details></section>`;
}

function chaosPanel(events) {
  if (!events.length) return '<div class="notice"><strong>No leader crash recorded.</strong> This run contains no measured failover event.</div>';
  return events.map((raw, index) => {
    const event = object(raw);
    const recovery = positive(event.quorumRecoveryMs);
    const caughtUp = positive(event.restartCatchUpMs);
    const end = Math.max(recovery ?? 0, caughtUp ?? 0, 1);
    const marks = [
      { position: 0, value: 0, name: `Leader ${event.oldLeader ?? "?"} killed`, color: COLORS[3] },
      { position: (recovery ?? 0) / end, value: recovery, name: `Quorum serving; leader ${event.newLeader ?? "?"}`, color: COLORS[0] },
      { position: (caughtUp ?? 0) / end, value: caughtUp, name: "Restarted node caught up", color: COLORS[1] },
    ];
    return `<article class="chaos-event"><div class="event-heading"><h3>Leader crash ${index + 1}</h3><span class="small">${time(event.crashedAt)}</span></div><svg class="timeline" viewBox="0 0 760 120" role="img" aria-labelledby="chaos-${index}-title"><title id="chaos-${index}-title">${escape(`Crash recovery: leader ${event.oldLeader ?? "unknown"} killed; serving quorum after ${ms(recovery)}; restarted node caught up after ${ms(caughtUp)}. Both durations are measured from the crash.`)}</title><line x1="25" x2="735" y1="30" y2="30" class="timeline-track"/>${marks.filter((mark) => mark.value !== null).map((mark, markIndex) => {
      const x = 25 + mark.position * 710;
      return `<circle cx="${x}" cy="30" r="7" fill="${mark.color}"/><text x="${x}" y="${markIndex === 1 ? 65 : 90}" class="axis-label" text-anchor="${markIndex === 0 ? "start" : "end"}">${escape(mark.name)}</text><text x="${x}" y="${markIndex === 1 ? 81 : 106}" class="axis-label small-axis" text-anchor="${markIndex === 0 ? "start" : "end"}">${ms(mark.value)}</text>`;
    }).join("")}</svg>${table(["Recovery milestone", "Time since crash"], [
      ["Candidate observed in metrics", ms(positive(event.candidateObservedMs))],
      ["Leader observed in metrics", ms(positive(event.leaderObservedMs))],
      ["Quorum read probe completed", ms(positive(event.quorumProbeMs))],
      ["Leader discovery returned", ms(recovery)],
      ...(positive(event.clientRecoveryMs) === null ? [] : [["Client query returned", ms(event.clientRecoveryMs)]]),
      ["Restarted node caught up", ms(caughtUp)],
    ], { caption: "Recovery observations, all measured from the leader crash" })}<p class="small muted">Metrics observations follow polling and are not exact internal transition times. A brief candidate state can be missed; observing a leader alone does not establish a serving quorum. Missing milestones were not recorded or did not finish. Recovery and catch-up durations share the crash as time zero.</p></article>`;
  }).join("");
}

function failureDetails(report) {
  const failures = array(report.failures);
  const logs = array(report.clusterLogs);
  return `<section id="diagnostics" class="panel"><div class="section-heading"><div><p class="eyebrow">When the oven smokes</p><h2>Diagnostics</h2></div><span class="tag ${report.failureCount > 0 ? "danger-tag" : ""}">${count(report.failureCount)} unexpected failures</span></div>${failures.length ? `<ol class="failure-list">${failures.map((raw) => {
    const failure = object(raw);
    return `<li><strong>${escape(failure.context)}</strong>${failure.code ? `<code>${escape(failure.code)}</code>` : ""}<pre>${escape(failure.message)}</pre></li>`;
  }).join("")}</ol>` : '<p class="small">No unexpected failure details were recorded. Intentional stale-lease rejections and retried HTTP failures are reported in their own counters.</p>'}${logs.length ? `<details><summary>Server log tails (${logs.length} process generations)</summary>${logs.map((raw) => {
    const log = object(raw);
    return `<h3>Node ${escape(log.node)} · generation ${escape(log.generation)} · process ${escape(log.pid)}</h3><p class="small">Exit: ${escape(JSON.stringify(log.exit ?? null))} · error: ${escape(log.error ?? "none")}</p><pre class="log">${escape(log.tail)}</pre>`;
  }).join("")}</details>` : ""}</section>`;
}

const CSS = `
:root{color-scheme:light;--ink:#213a30;--muted:#617267;--green:#177b5a;--orange:#df7b38;--paper:#f3f5ef;--card:#fff;--border:#dce4db;--soft:#f5f8f3;--bad:#a73c43;font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);line-height:1.5;font-size:15px}.skip{position:absolute;left:-10000px}.skip:focus{left:16px;top:12px;z-index:9;background:#fff;padding:12px}a{color:var(--green)}button,summary,a{touch-action:manipulation}:focus-visible{outline:3px solid var(--orange);outline-offset:5px}.masthead{background:#17392c;color:#eff8eb}.masthead-inner{max-width:1280px;margin:auto;padding:30px 36px 40px}.brand{display:flex;justify-content:space-between;align-items:center;font-size:12px;letter-spacing:.11em;text-transform:uppercase;color:#c0d7c4}.brand strong{color:white;letter-spacing:.17em}.brand-mark{display:inline-block;color:#dfad65;margin-right:9px;font-size:22px;vertical-align:middle}.hero{display:flex;justify-content:space-between;gap:40px;align-items:flex-end;margin-top:38px}.eyebrow{font-size:11px;letter-spacing:.12em;text-transform:uppercase;font-weight:750;color:var(--muted);margin:0 0 9px}.masthead .eyebrow{color:#b8d6bc}h1{font-size:clamp(34px,4.7vw,59px);line-height:1.07;letter-spacing:-.05em;margin:0 0 15px;font-weight:650}.subtitle{color:#c9decd;max-width:710px;margin:0;font-size:16px}.run-stamp{flex-shrink:0;text-align:right;color:#c9decd;font-size:12px}.run-stamp p{margin:11px 0 0}.status{display:inline-flex;align-items:center;gap:8px;background:#e5f4e6;color:#1f6246;border:1px solid #b2d8bc;border-radius:99px;font-weight:800;font-size:12px;letter-spacing:.08em;padding:9px 14px}.status::before{content:"";width:7px;height:7px;border-radius:50%;background:currentColor}.status.failed{color:#8f3037;background:#fbe9e9;border-color:#e6b8bb}.status.incomplete{color:#8b6530;background:#fff1d9;border-color:#e5cc9d}.nav{max-width:1280px;margin:auto;display:flex;gap:25px;overflow:auto;padding:18px 36px;border-bottom:1px solid var(--border)}.nav a{white-space:nowrap;text-decoration:none;color:var(--muted);font-size:13px;font-weight:650}.nav a:hover{color:var(--green)}main{max-width:1280px;margin:auto;padding:28px 36px 65px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:23px}.metric{padding:22px;border:1px solid var(--border);background:var(--card);border-radius:13px;min-width:0}.metric-value{font-size:clamp(25px,2.9vw,37px);line-height:1.1;font-weight:680;letter-spacing:-.04em;margin:13px 0 11px}.metric-value small{font-size:14px;font-weight:500;color:var(--muted);letter-spacing:0}.metric-note{font-size:12px;line-height:1.55;color:var(--muted);margin:0}.metric.feature{background:#e9f0e5;border-color:#cbdcc6}.panel{padding:27px;background:var(--card);border:1px solid var(--border);border-radius:14px;margin-bottom:23px;min-width:0}.section-heading{display:flex;justify-content:space-between;align-items:flex-start;gap:18px;margin-bottom:22px}.section-heading .eyebrow{margin-bottom:5px}h2{font-size:24px;letter-spacing:-.035em;line-height:1.2;margin:0;font-weight:650}h3{font-size:16px;line-height:1.35;margin:0 0 12px;font-weight:650}.tag{font-size:11px;padding:5px 9px;background:var(--soft);border:1px solid var(--border);border-radius:6px;color:var(--muted);white-space:nowrap}.danger-tag{background:#fbe9e9;color:var(--bad);border-color:#e6b8bb}.charts-grid,.two-up{display:grid;grid-template-columns:1fr 1fr;gap:24px}.chart{margin:0}.chart svg{width:100%;height:auto;display:block;overflow:visible}.chart-title{font-size:14px;margin:0 0 6px}.chart-subtitle{font-size:12px;color:var(--muted);margin:0 0 12px}.grid-line{stroke:var(--border);stroke-width:1}.axis-label{fill:var(--muted);font-family:inherit;font-size:12px}.group-label{font-weight:600}.small-axis{font-size:10px}.legend{display:flex;align-items:center;gap:15px;flex-wrap:wrap;font-size:11px;color:var(--muted);margin:8px 0 15px}.legend span{display:flex;align-items:center;gap:6px}.legend i{width:8px;height:8px;border-radius:2px;display:inline-block}.legend .unit{margin-left:auto}.chart-empty{border:1px dashed var(--border);padding:38px 18px;min-height:160px;color:var(--muted);border-radius:8px}.chart-empty p{font-size:16px;margin:0 0 10px}.chart-empty span{font-size:12px}.small{font-size:12px;line-height:1.65}.muted{color:var(--muted)}.note{background:var(--soft);padding:13px 16px;border-radius:8px;color:var(--muted);font-size:12px;margin:18px 0 0}.notice{padding:15px 17px;border:1px solid var(--border);background:var(--soft);border-radius:9px;font-size:13px;line-height:1.65;margin:16px 0}.notice.warning{background:#fff8ea;border-color:#eddcb7}.notice.good{background:#edf7ed;border-color:#c3dec7}.notice.bad{background:#fff1f1;border-color:#e7bbbb}.table-scroll{overflow:auto;max-width:100%;margin:15px 0}table{width:100%;border-collapse:collapse;font-size:12px;text-align:left;white-space:nowrap;font-variant-numeric:tabular-nums}caption{text-align:left;color:var(--muted);font-size:11px;padding-bottom:10px;white-space:normal}thead th{background:var(--soft);font-weight:650;color:var(--muted);border-bottom:1px solid var(--border)}td,th{padding:11px 12px;border-bottom:1px solid #edf0eb}tbody th{font-weight:550;text-align:left}td:not(:first-child),thead th:not(:first-child){text-align:right}tbody tr:last-child td,tbody tr:last-child th{border-bottom:0}tbody tr:hover{background:#f8faf6}.empty{color:var(--muted);padding:20px;text-align:left!important}.facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 24px;margin:0}.facts div{display:flex;justify-content:space-between;gap:18px;border-bottom:1px solid #edf0eb;padding:12px 0;min-width:0}.facts dt{color:var(--muted);font-size:12px;flex-shrink:0}.facts dd{font-size:12px;font-weight:600;margin:0;text-align:right;overflow-wrap:anywhere}.phase-detail{border-top:1px solid var(--border)}.phase-detail:last-child{border-bottom:1px solid var(--border)}summary{cursor:pointer;padding:15px 0;font-size:13px;font-weight:650;color:var(--ink)}summary::marker{color:var(--green)}.summary-meta{float:right;font-weight:450;color:var(--muted);font-size:12px}.detail-body{padding:0 0 15px}.nested-detail{margin-top:13px}.nested-detail summary{font-size:12px}.phase-counters .facts{grid-template-columns:repeat(4,minmax(0,1fr));gap:20px}.phase-counters .facts div{display:block}.phase-counters .facts dd{text-align:left;font-size:20px;letter-spacing:-.03em;margin:6px 0}.invariants{list-style:none;padding:0;margin:16px 0;display:grid;grid-template-columns:1fr 1fr;gap:9px 24px}.invariants li{font-size:12px;padding-left:22px;position:relative;color:var(--muted)}.invariants li::before{content:"✓";position:absolute;left:0;font-weight:bold;color:var(--green)}.invariants.pending li::before{content:"·";color:var(--muted)}.violations{font-size:13px;color:var(--bad);padding-left:20px}.event-heading{display:flex;justify-content:space-between;align-items:center;gap:15px}.timeline{width:100%;height:auto;display:block;margin:10px 0}.timeline-track{stroke:#d2e0d2;stroke-width:5;stroke-linecap:round}.chaos-event+.chaos-event{border-top:1px solid var(--border);padding-top:22px;margin-top:22px}.run-details .facts{grid-template-columns:1fr}.methodology{padding-left:19px;margin:0;font-size:13px;color:var(--muted)}.methodology li{margin:10px 0}.methodology strong{color:var(--ink)}code{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:11px;padding:2px 5px;background:#eef2eb;border-radius:4px;overflow-wrap:anywhere}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f3f6ef;border:1px solid var(--border);padding:16px;border-radius:8px;font-size:11px;line-height:1.65;max-height:470px;overflow:auto}.log{background:#17392c;color:#d6e8d5;border:0}.failure-list{padding-left:22px}.failure-list li{padding:8px 0}.failure-list code{margin-left:10px}.good-text{color:var(--green);font-weight:700}.bad-text{color:var(--bad);font-weight:700}footer{color:var(--muted);font-size:11px;display:flex;justify-content:space-between;gap:20px;margin:30px 0 0}.bundle{max-width:65%;overflow-wrap:anywhere}.inline-unit{font-size:12px;font-weight:500;color:var(--muted)}.latency-title{display:flex;align-items:baseline;gap:7px;margin:0 0 6px;font-size:14px;font-weight:650}.latency-title span{margin-left:auto;font-size:11px;font-weight:450;color:var(--muted)}.chart svg.latency-histogram{max-width:560px;overflow:visible}.latency-axis,.latency-marker{font:10px ui-monospace,SFMono-Regular,Consolas,monospace}.latency-bin:hover path{opacity:.72}.latency-sparkline{display:block;max-width:100%;height:auto}.metric .latency-sparkline{width:100%;margin:14px 0 10px}.latency-cell{display:inline-flex;flex-direction:column;align-items:flex-end;gap:3px}.latency-cell small{font-size:11px;color:var(--muted)}.latency-table td,.latency-table th{padding:4px 10px}
@media(max-width:900px){.masthead-inner{padding:25px}.hero{gap:20px}.run-stamp{max-width:200px}.nav{padding:16px 25px}main{padding:24px 25px}.metrics{grid-template-columns:repeat(2,1fr)}.charts-grid,.two-up{grid-template-columns:1fr}.phase-counters .facts{grid-template-columns:repeat(2,1fr)}.facts{gap:0 16px}.section-heading{flex-wrap:wrap}.panel{padding:23px}.tag{white-space:normal}}
@media(max-width:540px){.masthead-inner{padding:22px 18px 28px}.brand{font-size:9px}.hero{display:block;margin-top:27px}.run-stamp{text-align:left;max-width:none;margin-top:22px}.run-stamp p{display:inline;margin-left:9px;font-size:10px}.subtitle{font-size:14px}.nav{padding:14px 18px;gap:21px}main{padding:20px 14px}.metrics{gap:10px}.metric{padding:17px 13px}.metric-value{font-size:27px}.metric-note{font-size:11px}.panel{padding:20px 15px}.facts{grid-template-columns:1fr}.summary-meta{float:none;display:block;margin-left:16px;margin-top:4px}.invariants{grid-template-columns:1fr}.event-heading{display:block}.tag{font-size:10px}h2{font-size:22px}footer{display:block}.bundle{max-width:none;margin-top:9px}.legend{gap:10px}.axis-label{font-size:12px}}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}@media print{body{background:white}.masthead{-webkit-print-color-adjust:exact;print-color-adjust:exact}.nav,.skip{display:none}main{padding:20px 0;max-width:none}.panel,.metric{break-inside:avoid}details{display:block}.detail-body{display:block}.charts-grid{grid-template-columns:1fr 1fr}.table-scroll{overflow:visible}table{font-size:9px}pre{max-height:none}a{color:inherit}footer{margin-top:15px}}
`;

export function renderReport(rawReport, { baseline: rawBaseline } = {}) {
  const report = object(rawReport);
  const baseline = rawBaseline ? object(rawBaseline) : null;
  const options = object(report.options);
  const localReads = options.readConsistency === "replica-local";
  const environment = object(report.environment);
  const audit = object(report.audit);
  const counters = object(report.counters);
  const lifecycle = object(report.lifecycle);
  const resources = object(report.resources);
  const phases = Object.entries(object(report.phases));
  const load = object(report.phases?.load);
  const logical = object(load.operations);
  const application = summarizeApplication(report);
  const chaos = array(report.chaos);
  const leaderboard = array(report.leaderboard);
  const replication = array(report.replication);
  const status = report.passed === true ? "Passed" : report.passed === false ? "Failed" : "Incomplete";
  const auditPassed = audit.passed === true;
  const phaseGroups = phases.map(([name, raw]) => {
    const value = object(raw);
    return { name: label(name), detail: seconds(value.durationMs), values: [value.throughputPerSecond, value.operations?.throughputPerSecond] };
  });
  const rssGroups = Object.entries(object(resources.serverPeakRssMiB)).map(([node, value]) => ({ name: `Node ${node}`, values: [value] }));
  if (finite(resources.driverPeakRssMiB)) rssGroups.push({ name: "Driver", values: [resources.driverPeakRssMiB] });
  const runElapsed = typeof report.startedAt === "string" && typeof report.finishedAt === "string" ? Date.parse(report.finishedAt) - Date.parse(report.startedAt) : null;
  const totalUnexpected = finite(report.failureCount) ? report.failureCount : null;
  const recovery = chaos.length ? object(chaos[0]).quorumRecoveryMs : null;
  const auditMessage = auditPassed
    ? `${count(audit.delivered)} of ${count(audit.orders)} acknowledged orders delivered. The independent audit found no invariant violations.`
    : report.audit ? "The independent audit found invariant violations. See the details below." : "The run ended before a final independent audit was available.";

  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="color-scheme" content="light"><title>Goblin Pizza · ${status} · Flower benchmark</title><style>${CSS}</style></head>
<body><a class="skip" href="#results">Skip to benchmark results</a>
<header class="masthead"><div class="masthead-inner"><div class="brand"><strong><span class="brand-mark" aria-hidden="true">✳</span>Flower / Field notes</strong><span>Reactive database · Raft · TypeScript</span></div><div class="hero"><div><p class="eyebrow">Goblin Pizza Express / Benchmark report / ${escape(GUEST_LABELS[guestOf(report)])}</p><h1>The rush-hour report.</h1><p class="subtitle">Goblins bake. Drones deliver. Raft keeps the books.<br>Scheduled ovens, leased deliveries, and reactive shop totals under load.</p></div><div class="run-stamp"><span class="status ${status.toLowerCase()}">${status}</span><p>${time(report.startedAt)}</p><p>${seconds(runElapsed)} total wall time · ${count(totalUnexpected)} unexpected failures</p></div></div></div></header>
<nav class="nav" aria-label="Report sections"><a href="#results">Results</a><a href="#customers">Customer work</a>${array(report.timeline).length ? '<a href="#timeline">Timeline</a>' : ""}${baseline ? '<a href="#comparison">Before &amp; after</a>' : ""}<a href="#latency">Latency</a><a href="#accounting">Accounting</a><a href="#resilience">Resilience</a><a href="#resources">Resources</a><a href="#methodology">Run details</a><a href="#diagnostics">Diagnostics</a></nav>
<main id="results"><div class="metrics">${metric("Customer goodput", `${number(application.goodputRps)} <small>/ s</small>`, `${percent(application.reads.fraction)} reads · ${percent(application.mutations.fraction)} mutations among successful customer calls.`, "feature")}${latencyMetric("All-call latency", logical.latencyMs, "All load-phase logical calls, including worker traffic, replays, discovery, and retries.")}${metric("Orders delivered", `${count(audit.delivered)} <small>/ ${count(audit.orders)}</small>`, `${count(audit.pizzas)} pizzas · ${count(audit.revenue)} copper in delivery revenue.`)}${metric("Quorum recovery", chaos.length ? `${number(recovery)} <small>ms</small>` : "Not exercised", chaos.length ? "First recorded crash to a serving quorum; restart catch-up is tracked separately." : "Leader failure injection was not recorded in this run.")}</div>
<div class="notice ${localReads ? "warning" : "good"}"><strong>${localReads ? "Replica-local customer previews: replication lag is allowed." : "Fresh customer reads: linearizable within this Raft group."}</strong> ${localReads ? "The selected pizza.shop.local method uses coherent locally applied snapshots without a quorum fence. Data, code, and aliases may lag without a bound; requests across replicas can return decreasing revisions. Replica-local is the benchmark default; choose --read-consistency fresh to measure quorum-fenced previews." : "The selected pizza.shop method waits for a quorum-confirmed applied position. This run does not opt the application into stale reads."} Mutations and the final pizza.world audit retain fresh semantics.</div>
${customerPanel(report, application)}
${cpuProfilePanel(report.cpuProfile)}
${timeSeriesPanel(report)}
<section class="panel"><div class="section-heading"><div><p class="eyebrow">The shape of the rush</p><h2>Throughput &amp; response time</h2></div><span class="tag">Closed-loop workload</span></div><div class="charts-grid"><div><h3 class="chart-title">Successful work by phase</h3><p class="chart-subtitle">Actual measured phase durations appear below the bars.</p>${groupedChart("phase-throughput", "Successful throughput by phase", "Successful HTTP responses and completed logical calls per second for each phase.", phaseGroups, ["HTTP responses", "Logical calls"], "events / s")}</div><div><h3 class="chart-title">Logical latency by phase</h3><p class="chart-subtitle">Retry and discovery time remain visible in the tails.</p>${latencyRows(phases.map(([name, raw]) => [label(name), object(raw).operations?.latencyMs]), { caption: "Logical calls by the phase they started in." })}</div></div><p class="note">A successful HTTP response is not necessarily a new business mutation: receipt replays count as responses, and queries also count. Phase percentiles cover successful and failed attempts or calls. Warmup uses reads and is not comparable to the mixed load phase.</p></section>
${comparison(report, baseline)}
<section id="latency" class="panel"><div class="section-heading"><div><p class="eyebrow">Every call accounted for</p><h2>Requests, retries &amp; methods</h2></div><span class="tag">Load details expanded below</span></div>${definitionList([["Load HTTP attempts", count(load.attempts)], ["Load HTTP successes", count(load.successes)], ["Load HTTP failures", count(load.failures)], ["Load retry attempts", count(load.retries)], ["Load duplicate responses", count(load.duplicates)], ["Load logical failures", count(logical.failed)]])}<p class="small muted">Network retries belong to the same logical call. Deliberate deduplication probes appear separately as <code>.replay</code> calls. Expected stale-lease failures can appear in these measurements without failing the benchmark.</p>${phases.length ? phases.map(([name, phase]) => phaseDetails(name, phase)).join("") : '<p class="empty">No phase measurements were recorded.</p>'}</section>
<section class="panel"><div class="section-heading"><div><p class="eyebrow">From the oven to the doorstep</p><h2>Business completion latency</h2></div><span class="tag">Load and drain combined</span></div>${lifecycleCharts(lifecycle)}${definitionList([["Configured oven delay", ms(options.bakeMs)], ["Configured lease duration", ms(options.leaseMs)], ["Delivered orders / s, including drain", number(lifecycle.deliveredOrdersPerSecondIncludingDrain)], ["Retained order cap", count(options.maxOrders)]])}<p class="note">Oven lateness measures how far execution runs past the requested deadline. It does not include the configured baking delay. Order-to-door latency includes the entire lifecycle. Each log time axis spans its population's p0.1 to p99.9; the tables list every bin.</p></section>
<section id="accounting" class="panel"><div class="section-heading"><div><p class="eyebrow">The goblin ledger</p><h2>Business invariants &amp; tenant rankings</h2></div><span class="tag ${report.audit && !auditPassed ? "danger-tag" : ""}">${auditPassed ? "Audit passed" : report.audit ? "Audit failed" : "Audit unavailable"}</span></div><div class="notice ${auditPassed ? "good" : report.audit ? "bad" : ""}">${auditMessage}</div><ul class="invariants${auditPassed ? "" : " pending"}"><li>Orders match the client's acknowledged inputs.</li><li>Stock equals initial dough minus ordered pizzas.</li><li>Revenue equals delivered pizzas × seven copper.</li><li>Tips match the acknowledged mutation ledger.</li><li>Completed jobs match orders and delivery timestamps.</li><li>Derived summaries and each tenant’s ranking match source records.</li><li>Baking and delivery timestamps respect their deadlines.</li><li>No pending or failed oven timers remain after drain.</li></ul>${array(audit.violations).length ? `<ul class="violations">${audit.violations.map((violation) => `<li>${escape(violation)}</li>`).join("")}</ul>` : ""}${table(["Tenant / kitchen", "Orders", "Pizzas delivered", "Stock left", "Revenue", "Tips", "Score"], leaderboard.map((raw) => {
    const shop = object(raw);
    return [escape(Array.isArray(shop.id) ? `${shop.id[0]} / ${shop.name ?? shop.id[1]}` : shop.name ?? shop.id), count(shop.orders), count(shop.deliveredQuantity), count(shop.stock), count(shop.revenue), count(shop.tips), finite(shop.revenue) && finite(shop.tips) ? count(shop.revenue + shop.tips) : "—"];
  }), { caption: "Scores are revenue plus tips, in integer copper coins. Rows follow each tenant’s independently derived leaderboard; this is not a global ranking." })}</section>
<section id="resilience" class="panel"><div class="section-heading"><div><p class="eyebrow">A dragon ate the leader</p><h2>Failover, leases &amp; replay safety</h2></div><span class="tag">${count(options.nodes)} Raft replicas · one group</span></div>${chaosPanel(chaos)}${definitionList([["Receipt replays verified", count(counters.replayChecks)], ["Stale drones rejected by audit", count(counters.staleLeaseChecks)], ["Leases claimed", count(counters.claims)], ["Empty claims", count(counters.emptyClaims)], ["Deliberately abandoned leases", count(counters.abandoned)], ["Reclaimed jobs", count(counters.reclaimed)], ["Lease losses during workload", count(counters.lostLeases)], ["Orders issued", count(counters.issuedOrders)]])}${table(["Replica", "Observed role", "Observed leader", "Last applied index"], replication.map((raw) => {
    const node = object(raw);
    return [`Node ${escape(node.node)}`, escape(node.state), escape(node.leader), count(node.lastApplied)];
  }), { caption: "End-of-run replica observations, collected separately. These are replicas of one Raft group, not independent shards." })}</section>
<section id="resources" class="panel"><div class="section-heading"><div><p class="eyebrow">The cost of a pizza rush</p><h2>Resource footprint</h2></div><span class="tag">Co-located processes</span></div><div class="charts-grid"><div><h3 class="chart-title">Sampled peak resident memory</h3>${groupedChart("memory", "Sampled peak resident memory by process", "Peak observed resident memory per server and benchmark driver. Samples can miss brief peaks; per-process peaks need not occur together.", rssGroups, ["Peak sampled RSS"], "MiB")}</div><div>${definitionList([["Cluster disk footprint", finite(report.clusterDiskMiB) ? `${number(report.clusterDiskMiB, 2)} MiB` : "—"], ["Node controller sampled load cores", number(resources.nodeDriverCpuSampledLoad?.meanCores, 2)], ["Node controller CPU, user / entire run", ms(resources.driverCpuMsWholeRun?.user)], ["Node controller CPU, system / entire run", ms(resources.driverCpuMsWholeRun?.system)], ["Node controller event-loop delay p99", ms(resources.driverEventLoopP99Ms)], ["Node controller sampled peak RSS", finite(resources.driverPeakRssMiB) ? `${number(resources.driverPeakRssMiB)} MiB` : "—"], ["Rust customer driver peak RSS", finite(resources.nativeDriverPeakRssMiB) ? `${number(resources.nativeDriverPeakRssMiB)} MiB` : "—"], ["Rust customer driver sampled load cores", number(resources.nativeDriverCpuSampledLoad?.meanCores, 2)]])}<p class="note">RSS is sampled, not an allocation total. Server CPU, when available, uses cumulative process CPU deltas over sampled load intervals; one core means one CPU-second per elapsed second. Restart and phase-boundary gaps are excluded. Older reports did not measure it. The load generator and all nodes share the same host, so these numbers include local resource contention and do not establish multi-host network performance.</p></div></div>${table(["Server", "Mean CPU cores", "CPU time covered", "Wall time covered", "Intervals"], Object.entries(object(resources.serverCpuSampledLoad)).map(([id, raw]) => { const value = object(raw); return [escape(id), number(value.meanCores, 2), ms(value.cpuMs), ms(value.sampledWallMs), count(value.intervals)]; }), { caption: "CPU observations during load; node coverage can differ. Includes all process threads." })}</section>
${offeredPanel(report.offeredLoad)}
<section id="methodology" class="panel run-details"><div class="section-heading"><div><p class="eyebrow">Make the result reproducible</p><h2>Workload, host &amp; measurement limits</h2></div><span class="tag">Report schema ${escape(report.schemaVersion)}</span></div><div class="two-up"><div><h3>Workload configuration</h3>${definitionList([["Application transport", options.http2 ? "HTTP/2 · h2c prior knowledge" : "HTTP/1.1"], ["Admin and discovery transport", "HTTP/1.1"], ["Query routing", options.queryRouting === "replicas" ? "Round-robin across live replicas" : options.queryRouting === "leader" ? "Leader only" : "Leader only (historical default)"], ["Customer read policy", localReads ? "Replica-local · lag allowed" : "Fresh · linearizable"], ["Bundle initialization", escape(options.initialization ?? "Not recorded")], ["Customer loops / drone loops", `${count(options.concurrency)} / ${count(options.workers)}`], ["Requested load / warmup", `${number(options.duration)} s / ${number(options.warmup)} s`], ["Maximum drain", `${number(options.drain)} s`], ["Tenants / stores per tenant", `${count(options.tenants)} / ${count(options.shops)}`], ["Hot stores per tenant", count(options.hotShops)], ["Hot-set mixture weight", percent(options.hotProbability)], ["Duplicate / abandonment probability", `${percent(options.duplicateRate)} / ${percent(options.abandonRate)}`], ["Worker polling interval", ms(options.pollMs)], ["Request timeout / retry budget", `${ms(options.requestTimeoutMs)} / ${ms(options.retryBudgetMs)}`], ["Workload seed", escape(options.seed)], ["Crash injection requested", options.chaos === true ? "Yes" : options.chaos === false ? "No" : "—"]])}</div><div><h3>Execution environment</h3>${definitionList([["Guest engine", escape(report.runtime?.engine ?? "Not recorded")], ["Runtime settings", escape(JSON.stringify(report.runtime?.settings ?? {}))], ["CPU", escape(environment.cpu)], ["Logical CPUs", count(environment.logicalCpus)], ["Operating system", escape(environment.os)], ["Architecture", escape(environment.arch)], ["Node.js controller", escape(environment.node)], ["Customer driver", escape(report.driver?.kind ?? options.driver ?? "node")], ["Customer driver SHA-256", escape(report.driver?.binary?.sha256 ?? "—")], ["Co-located database nodes", count(environment.colocatedNodes)], ["Binary SHA-256", escape(report.binary?.sha256)], ["Binary size", finite(report.binary?.bytes) ? `${count(report.binary.bytes)} bytes` : "—"], ["Started", time(report.startedAt)], ["Finished", time(report.finishedAt)], ["Preserved database files", escape(report.dataDirectory ?? "Not retained")]])}</div></div><h3 style="margin-top:27px">How to read these numbers</h3><ul class="methodology"><li>${options.offeredRate>0?"<strong>Independent arrival rate.</strong> Offered work follows a fixed clock; driver saturation is counted as dropped arrivals. Logical primary latency includes intended-arrival delay.":"<strong>Closed-loop concurrency.</strong> Each client waits for a response before issuing its next operation. Slow service reduces offered load; these results do not estimate open-loop latency under an independent arrival rate."}</li><li><strong>HTTP/2 connection reuse.</strong> With <code>--http2</code>, each driver process reuses a multiplexed session per origin. Rust customers and Node workers use separate pools; this comparison changes the whole driver, including its connection topology. Concurrency controls outstanding operations; it does not create one connection per loop.</li><li><strong>Declared read policy.</strong> <code>--query-routing=replicas</code> spreads query evaluation across live replicas. ${localReads ? "Replica-local previews skip the quorum fence and may be arbitrarily stale; this is the default customer workload." : "Fresh previews wait for a quorum-confirmed applied position; this does not opt the application into stale reads."} Final audits always use fresh reads. Mutations and deployment use the leader. Failed query attempts rotate through replicas before bounded backoff.</li><li><strong>Tenant locality.</strong> Store keys are [tenant, store]; order IDs are scoped to that pair. Each tenant has its own queue and leaderboard. Logical separation in this unauthenticated demo is not authorization. A hot tenant still shares one ordered writer lane with the other tenants in this group.</li><li><strong>Timeline sampling.</strong> Rust customer statistics arrive in disjoint one-second batches; interval charts can show delivery-boundary artifacts. Whole-run counts and latency histograms merge the actual observations.</li><li><strong>Phase attribution.</strong> Calls belong to the phase in which they start. In-flight work can finish after the requested load interval, so measured phase durations may overlap at boundaries and should not be added as exact wall time.</li><li><strong>Bounded order population.</strong> After the order cap is reached, the workload continues with queries and tips. Receipt retention and the database's own state can still grow with mutations; order limits do not imply constant database size.</li><li><strong>Latency histograms.</strong> Fixed buckets 1% wide cover 0.001 ms to 24 hours; lower values share a floor bucket and larger ones an overflow bucket. Charts merge whole buckets into bins on log axes spanning p0.1 to p99.9. Percentiles are nearest-rank bucket upper bounds, within 1% of the exact rank.</li><li><strong>Fault and safety probes.</strong> Retried HTTP errors and intentional stale-lease rejections are recorded. The final verdict also requires successful lifecycle drainage and an independent business audit; a low HTTP error count alone is not sufficient.</li><li><strong>Replication topology.</strong> This benchmark uses one Raft group with ${count(options.nodes)} replicas. Replica observations do not measure sharding or horizontal partitioning.</li><li><strong>One run is an observation.</strong> Repeat matched configurations on a quiet host. A seeded workload stabilizes choices, but concurrent execution, scheduling, and elections remain nondeterministic.</li></ul><details><summary>Complete run options and bundle identity</summary><pre>${escape(JSON.stringify({ options: report.options ?? {}, runtime: report.runtime ?? null, queryRouting: report.queryRouting ?? null, binary: report.binary ?? null, driver: report.driver ?? null, bundleHash: report.bundleHash ?? null }, null, 2))}</pre></details></section>
${failureDetails(report)}
<footer><span>Flower · Goblin Pizza Express<br>Self-contained report. Charts remain available offline.</span><span class="bundle">Binary: ${escape(report.binary?.sha256)}<br>Bundle: ${escape(report.bundleHash)}<br>Generated from the recorded benchmark JSON; no external resources.</span></footer></main></body></html>`;
}
