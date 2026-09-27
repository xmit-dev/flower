// Retain validated measurements in docs/bench/; Eleventy renders reports at build time.
// --check verifies the retained JSON offline without rerunning the workload.
import { existsSync, readFileSync } from "node:fs";
import { access, mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { summarizeCpu } from "../bench/compare-cpu.mjs";
import { GUEST_LABELS, GUESTS, guestOf } from "../bench/guests.mjs";
import { summarizeGroups } from "../bench/multi-group.mjs";
import { renderGroupsReport } from "../bench/multi-group-report.mjs";
import { renderReport } from "../bench/report.mjs";
import { siteFooter, siteHeader } from "./docs/layout.mjs";
import { LATENCY_COLORS, latencyHistogram, latencyTable } from "../bench/histogram.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const markerStart = "<!-- latest-benchmark:start -->";
const markerEnd = "<!-- latest-benchmark:end -->";
const escape = (value) => String(value).replace(/[&<>"']/g, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[character]));
const finite = (value) => typeof value === "number" && Number.isFinite(value) && value >= 0;
const number = (value, digits = 0) => value.toLocaleString("en-US", { maximumFractionDigits: digits });
const json = (value) => `${JSON.stringify(value)}\n`;
const ms = (value) => finite(value) ? `${number(value, value < 10 ? 1 : 0)}&nbsp;ms` : "—";
const exists = (path) => access(path).then(() => true, () => false);

/** Each guest's retained run: bench/latest.* (TypeScript) and bench/latest-wasm.* (Rust). */
export const stemOf = (guest) => guest === "js" ? "latest" : `latest-${guest}`;

export function childPath(source, path) {
  if (typeof path !== "string" || !path || isAbsolute(path) || /[\\\x00-\x1f\x7f?#]/.test(path) || /^[a-z][a-z\d+.-]*:/i.test(path)) {
    throw new Error("A child report must be a relative JSON file inside the report directory");
  }
  const directory = dirname(resolve(source));
  const target = resolve(directory, path);
  const local = relative(directory, target);
  if (!local || local.startsWith(`..${sep}`) || local === ".." || !local.endsWith(".json")) {
    throw new Error("A child report must be a relative JSON file inside the report directory");
  }
  return target;
}

export function summarizePublishedRun(report) {
  if (report.kind !== "multi-group" || report.schemaVersion !== 1 ||
      !Number.isSafeInteger(report.options?.groups) || report.options.groups < 1 ||
      !Array.isArray(report.groups) || report.groups.length !== report.options.groups ||
      !Number.isSafeInteger(report.options.nodes) || report.options.nodes < 1 ||
      !finite(report.goodputRps) || !finite(report.latencyMs?.all?.p99) ||
      !finite(report.durationMs) || report.durationMs === 0 ||
      !finite(report.totals?.completed) || report.totals.completed === 0 ||
      !finite(report.totals?.reads) || !finite(report.totals?.mutations) ||
      report.totals.reads + report.totals.mutations !== report.totals.completed ||
      !["fresh", "replica-local"].includes(report.options.readConsistency) ||
      !Number.isFinite(Date.parse(report.loadStartedAt)) || !Number.isFinite(Date.parse(report.loadEndedAt)) ||
      typeof report.passed !== "boolean" || typeof report.correctnessPassed !== "boolean" ||
      (report.passed && !report.correctnessPassed) ||
      typeof report.binary?.sha256 !== "string" || !report.binary.sha256 ||
      typeof report.bundleHash !== "string" || !report.bundleHash || !GUESTS.includes(guestOf(report))) {
    throw new Error("Cannot publish an incomplete or invalid multi-group measurement");
  }
  if (report.durationMs !== Date.parse(report.loadEndedAt) - Date.parse(report.loadStartedAt) ||
      report.goodputRps !== report.totals.completed / (report.durationMs / 1000)) {
    throw new Error("Published throughput must match completed work and its measured interval");
  }
  const crashes = report.groups.flatMap((group) => group.chaos ?? []);
  const recoveries = crashes.map((event) => event.quorumRecoveryMs).filter(finite);
  return {
    goodputRps: report.goodputRps,
    groups: report.options.groups,
    replicas: report.options.nodes,
    durationSeconds: report.durationMs / 1000,
    readConsistency: report.options.readConsistency,
    readPercent: report.totals.reads / report.totals.completed * 100,
    mutationPercent: report.totals.mutations / report.totals.completed * 100,
    passed: report.passed,
    correctnessPassed: report.correctnessPassed,
    auditedGroups: report.groups.filter((group) => group.audit?.passed === true).length,
    crashes: crashes.length,
    recoveredCrashes: recoveries.length,
    recoveryMinMs: recoveries.length ? Math.min(...recoveries) : null,
    recoveryMaxMs: recoveries.length ? Math.max(...recoveries) : null,
    measuredAt: report.loadEndedAt,
    cpu: report.environment?.cpu ?? "CPU not recorded",
    hosts: report.hosts?.count ?? null,
    transport: report.options.http2 ? "HTTP/2" : "HTTP/1.1",
    guest: guestOf(report),
    guestLabel: GUEST_LABELS[guestOf(report)],
    stem: stemOf(guestOf(report)),
  };
}

/** Sampled server CPU per successful customer call, when the report covers every server. */
function serverCpuPerCall(report) {
  try { return summarizeCpu(report).servers.estimatedCpuUsPerSuccessfulCall; } catch { return null; }
}

// The same workload with the application in each guest, one column per run.
function guestComparison(reports, root) {
  const runs = reports.map((report) => ({ run: summarizePublishedRun(report), latency: report.latencyMs, cpu: serverCpuPerCall(report) }));
  const row = (label, cell) => `<tr><th scope="row">${label}</th>${runs.map((entry) => `<td>${cell(entry)}</td>`).join("")}</tr>`;
  const rows = [
    row("Calls&nbsp;/&nbsp;s", ({ run }) => number(run.goodputRps)),
    row("Server CPU&nbsp;/&nbsp;call", ({ cpu }) => finite(cpu) ? `${number(cpu)}&nbsp;µs` : "—"),
    row("Reads p50 · p99", ({ latency }) => `${ms(latency.read?.p50)} · ${ms(latency.read?.p99)}`),
    row("Writes p50 · p99", ({ latency }) => `${ms(latency.mutation?.p50)} · ${ms(latency.mutation?.p99)}`),
    row("Group audits", ({ run }) => `${run.auditedGroups}/${run.groups}${run.passed ? "" : " · run failed"}`),
  ].join("");
  const heads = runs.map(({ run }) => `<th scope="col"><a href="${root}bench/${run.stem}.html">${escape(run.guestLabel)}</a></th>`).join("");
  const binaries = new Set(reports.map((report) => report.binary.sha256));
  return `<div class="benchmark-guests"><table><caption>The same workload with the application in each guest</caption><thead><tr><td></td>${heads}</tr></thead><tbody>${rows}</tbody></table></div>
<p class="benchmark-context">The <a href="https://github.com/xmit-dev/flower/tree/main/examples/goblin-pizza-rs">Rust port</a> of the application makes the same host calls and writes the same records. Runs are measured one after the other${binaries.size === 1 ? " with the same server binary" : ", with different server binaries"}. Flushes to the one shared disk bound goodput here, and single runs vary by a fifth or more; server CPU per call, sampled across all replicas, is the steadier comparison.</p>`;
}

// `root` leads from the embedding page to the site root. The benchmark page
// itself omits the link back to its own workload description. `others` are
// the other guests' runs, compared below the headline run.
export function renderPublishedSummary(report, { root = "", workload = true, others = [] } = {}) {
  const run = summarizePublishedRun(report);
  const local = run.readConsistency === "replica-local";
  const recovery = run.crashes === 0 ? "No injected failure in this run."
    : run.recoveredCrashes !== run.crashes ? `${run.recoveredCrashes}/${run.crashes} injected failures have a recorded quorum recovery.`
    : `${run.crashes} injected leader failures; quorum recovery ${number(run.recoveryMinMs)}–${number(run.recoveryMaxMs)} ms.`;
  return `<section class="benchmark-latest" id="latest-benchmark" aria-labelledby="latest-benchmark-title">
<div class="benchmark-heading"><div><p class="eyebrow">LATEST MEASURED RUN · <time datetime="${escape(run.measuredAt)}">${escape(run.measuredAt.slice(0, 10))}</time></p><h2 id="latest-benchmark-title">A busy day in the garden.</h2></div><img src="${root}assets/flower.svg" width="36" height="36" alt="" aria-hidden="true"></div>
<dl class="benchmark-stats"><div><dt>Global customer calls&nbsp;/&nbsp;s</dt><dd>${number(run.goodputRps)}</dd></div><div><dt>Independent Raft groups</dt><dd>${run.groups} <small>× ${run.replicas} replicas</small></dd></div></dl>
${latencyFigure(report)}
<p class="benchmark-policy"><strong>${local ? "Replica-local reads: lag is allowed." : "Fresh reads: quorum-confirmed per group."}</strong> ${number(run.readPercent, 1)}% reads / ${number(run.mutationPercent, 1)}% mutations · ${run.transport} · ${number(run.durationSeconds, 1)} measured seconds.</p>
<p><strong>${run.passed ? "Run passed." : "Run failed."} ${run.auditedGroups}/${run.groups} group audits passed.</strong> ${escape(recovery)}</p>
<p class="benchmark-context">${escape(run.cpu)}; all replicas and load generators share one machine${run.hosts ? `, where ${run.hosts} host processes each serve one replica of every group over one shared database` : ""}. Completed customer calls use the union measurement window; retries, worker traffic, and explicit replays do not inflate throughput. ${local ? "Reads may be stale; mutations and audits retain fresh checks." : "Each group has its own fresh-read boundary."} Application code: ${escape(run.guestLabel)}.</p>
${others.length ? guestComparison([report, ...others], root) : ""}
<p class="benchmark-links"><a href="${root}bench/${run.stem}.html">Charts &amp; every group →</a><a href="${root}bench/${run.stem}.json">Raw measurements ↓</a>${others.map((other) => `<a href="${root}bench/${stemOf(guestOf(other))}.html">${escape(GUEST_LABELS[guestOf(other)])} report →</a>`).join("")}${workload ? `<a href="${root}operate/benchmarks.html">Workload &amp; reproduction →</a>` : ""}</p>
</section>`;
}

// Customer latency as distributions, each on its own log axis from its p0.1 to its p99.9.
export function latencyFigure(report) {
  const populations = [["Reads", report.latencyMs.read, LATENCY_COLORS.read], ["Writes", report.latencyMs.mutation, LATENCY_COLORS.mutation]];
  const charts = populations.map(([name, histogram, color]) => `<div class="latency-chart"><p class="latency-title"><i style="background:${color}" aria-hidden="true"></i>${name}<span>${number(histogram?.samples ?? 0)} calls</span></p>${latencyHistogram(histogram, { color, label: `Customer ${name.toLowerCase()}` })}</div>`).join("");
  const tables = populations.map(([name, histogram]) => latencyTable(histogram, `Customer ${name.toLowerCase()}, all groups`)).join("");
  return `<figure class="latency-figure"><div class="latency-charts">${charts}</div><figcaption>Customer call latency, merged across groups. Each log time axis spans p0.1 to p99.9 of its calls; bar height is a bin's share of calls, and lines mark p50 and p99. <details><summary>Show as a table</summary>${tables}</details></figcaption></figure>`;
}

export function replaceSummary(source, summary) {
  const start = source.indexOf(markerStart);
  const end = source.indexOf(markerEnd);
  if (start < 0 || end < start || source.indexOf(markerStart, start + markerStart.length) >= 0 || source.indexOf(markerEnd, end + markerEnd.length) >= 0) {
    throw new Error("Expected exactly one latest-benchmark marker pair");
  }
  return `${source.slice(0, start + markerStart.length)}\n${summary}\n${source.slice(end)}`;
}

// Give generated reports the site's header and footer, plus report-level links.
export function siteNavigation(html, child, stem = "latest") {
  const up = child ? "../../" : "../";
  const chrome = `<link rel="stylesheet" href="${up}chrome.css">`;
  const links = child ? `<a href="../${stem}.html">All groups</a>` : `<a href="${stem}.json">Raw JSON</a>`;
  const withHead = html.replace("</head>", `${chrome}</head>`);
  const withHeader = withHead.replace(/(<body[^>]*>(?:<a class="skip"[^>]*>[^<]*<\/a>)?)/, `$1${siteHeader(up, "operate")}`);
  const withLinks = withHeader.replace(/(<nav\b[^>]*>)/, `$1${links}`);
  const result = withLinks.replace("</body>", `${siteFooter(up)}</body>`);
  if (result === withLinks || withLinks === withHeader || withHeader === withHead || withHead === html) throw new Error("A benchmark report is missing its head, body or section navigation");
  return result;
}

export async function renderBenchResults(source = resolve(root, "docs/bench/latest.json")) {
  const report = JSON.parse(await readFile(source, "utf8"));
  const { stem } = summarizePublishedRun(report);
  const output = new Map();
  const published = structuredClone(report);
  const children = [];
  for (let i = 0; i < report.groups.length; i++) {
    const group = report.groups[i];
    const child = JSON.parse(await readFile(childPath(source, group.json), "utf8"));
    children.push(child);
    if (child.binary?.sha256 !== report.binary?.sha256 || child.bundleHash !== report.bundleHash ||
        child.loadStartedAt !== group.loadStartedAt || child.loadEndedAt !== group.loadEndedAt ||
        child.options?.readConsistency !== report.options.readConsistency || child.audit?.passed !== group.audit?.passed) {
      throw new Error(`Group ${i} does not match its aggregate report`);
    }
    const path = `${stem}-groups/group-${i}`;
    published.groups[i].html = `${path}.html`;
    published.groups[i].json = `${path}.json`;
    output.set(`bench/${path}.json`, json(child));
    const display = child.cpuProfile ? { ...child, cpuProfile: { ...child.cpuProfile,
      relativePath: null, outputPath: "Raw sample retained with the local benchmark artifacts" } } : child;
    output.set(`bench/${path}.html`, siteNavigation(renderReport(display), true, stem));
  }
  const reconstructed = summarizeGroups(children, report.options);
  for (const key of ["durationMs", "synchronizedOverlapMs", "startSkewMs", "loadStartedAt", "loadEndedAt",
    "goodputRps", "totals", "latencyMs", "runtime", "binary", "driver", "bundleHash"]) {
    if (!isDeepStrictEqual(report[key], reconstructed[key])) throw new Error(`Aggregate ${key} does not match child measurements`);
  }
  const derivedGroupFields = new Set(["customerReadLatencyMs", "customerMutationLatencyMs"]);
  for (let i = 0; i < reconstructed.groups.length; i++) {
    for (const [key, value] of Object.entries(reconstructed.groups[i])) {
      // Older reports retain the per-method buckets in their children. Derive
      // newly exposed splits from those measurements, never aggregate p99s.
      if (derivedGroupFields.has(key) && !Object.hasOwn(report.groups[i], key)) {
        published.groups[i][key] = value;
        continue;
      }
      if (!isDeepStrictEqual(report.groups[i][key], value)) throw new Error(`Group ${i} ${key} does not match child measurements`);
    }
  }
  if (commonGuest(children) !== guestOf(report)) throw new Error("Group guests do not match their aggregate report");
  if ((report.correctnessPassed && !reconstructed.correctnessPassed) ||
      (report.passed && children.some((child) => child.passed !== true))) {
    throw new Error("Aggregate passing verdict does not match child correctness or profiling");
  }
  output.set(`bench/${stem}.json`, json(published));
  output.set(`bench/${stem}.html`, siteNavigation(renderGroupsReport(published), false, stem));
  return output;
}

function commonGuest(children) {
  const guests = new Set(children.map(guestOf));
  return guests.size === 1 ? [...guests][0] : null;
}

/** Every guest's retained run in the site directory, rendered for the site build. */
export async function renderAllBenchResults(directory = resolve(root, "docs/bench")) {
  const output = new Map();
  for (const guest of GUESTS) {
    const source = resolve(directory, `${stemOf(guest)}.json`);
    if (guest !== "js" && !(await exists(source))) continue;
    for (const [file, content] of await renderBenchResults(source)) output.set(file, content);
  }
  return output;
}

/** The headline TypeScript run and every other retained guest run, for page summaries. */
export function publishedReports(directory = resolve(root, "docs/bench")) {
  const [report, ...others] = GUESTS.map((guest) => resolve(directory, `${stemOf(guest)}.json`))
    .filter((source, index) => index === 0 || existsSync(source))
    .map((source) => JSON.parse(readFileSync(source, "utf8")));
  return { report, others };
}

export async function publishBenchResults({ source = resolve(root, "bench/results/latest.json"), site = resolve(root, "docs"), check = false } = {}) {
  const output = new Map([...(await renderBenchResults(source))].filter(([file]) => file.endsWith(".json")));
  const stem = [...output.keys()].find((file) => /^bench\/[^/]+\.json$/.test(file)).slice("bench/".length, -".json".length);
  if (check) {
    for (const [file, content] of output) {
      if (await readFile(resolve(site, file), "utf8") !== content) throw new Error(`Published benchmark file is stale: ${file}`);
    }
  } else {
    for (const [file, content] of output) {
      await mkdir(dirname(resolve(site, file)), { recursive: true });
      await writeFile(resolve(site, file), content);
    }
  }
  // Remove obsolete measurements after retaining a run with fewer groups.
  const directory = resolve(site, `bench/${stem}-groups`);
  for (const file of await readdir(directory)) {
    if (/^group-\d+\.json$/.test(file) && !output.has(`bench/${stem}-groups/${file}`)) {
      if (check) throw new Error(`Obsolete benchmark measurement: ${file}`);
      await unlink(resolve(directory, file));
    }
  }
  return { files: output.size, bytes: [...output.values()].reduce((sum, content) => sum + Buffer.byteLength(content), 0) };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const check = args.includes("--check");
  const paths = args.filter((arg) => arg !== "--check");
  if (paths.some((arg) => arg.startsWith("-"))) throw new Error("Usage: node scripts/publish-bench-results.mjs [report.json ...] [--check]");
  // Without paths: every guest's run in bench/results/, or, for --check, in docs/bench/.
  const defaults = [];
  for (const guest of GUESTS) {
    const source = resolve(root, check ? "docs/bench" : "bench/results", `${stemOf(guest)}.json`);
    if (await exists(source)) defaults.push(source);
  }
  const sources = paths.length ? paths.map((path) => resolve(path)) : defaults;
  if (!sources.length) throw new Error("No benchmark report to publish; run bin/bench or name a report");
  for (const source of sources) {
    const result = await publishBenchResults({ source, check });
    console.log(`${check ? "Verified" : "Retained"} ${result.files} benchmark JSON files (${number(result.bytes / 1024)} KiB) from ${relative(root, source)}`);
  }
}
