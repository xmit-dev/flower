// Latency distributions from the benchmark's 1%-wide logarithmic buckets, as
// static SVG: full charts, table sparklines, and a table view of the same bins.
import { HISTOGRAM_BOUNDS } from "./metrics.mjs";

/** Validated for CVD separation and 3:1 contrast on the site and report surfaces. */
export const LATENCY_COLORS = Object.freeze({ read: "#20936b", mutation: "#c2641f", other: "#56685c" });
const FLOOR_MS = HISTOGRAM_BOUNDS[1];
const STEP = Math.log(HISTOGRAM_BOUNDS[2] / HISTOGRAM_BOUNDS[1]);
const DECADE = Math.log(10) / STEP;
// Axes span p0.1 to p99.9, so a handful of outliers cannot stretch them.
const TAIL = 0.001;
const INK = "#203f35";
const MUTED = "#56685c";
const RULE = "#cbd3c1";
const CHARACTER = 6;

const escape = (value) => String(value).replace(/[&<>"']/g, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[character]));
const finite = (value) => typeof value === "number" && Number.isFinite(value);
const count = (value) => value.toLocaleString("en-US");
const round = (value) => Math.round(value * 10) / 10;
const counted = (samples, noun) => `${count(samples)} ${samples === 1 ? noun.slice(0, -1) : noun}`;
const share = (fraction) => `${fraction >= 0.001 ? (fraction * 100).toFixed(1) : (fraction * 100).toPrecision(2)}%`;

export function usableHistogram(histogram) {
  return Array.isArray(histogram?.buckets) && histogram.buckets.length === HISTOGRAM_BOUNDS.length &&
    Number.isSafeInteger(histogram.samples) && histogram.samples > 0;
}

/** Three significant digits, switching to seconds at one second. */
export function duration(milliseconds) {
  if (!finite(milliseconds)) return "—";
  const [value, unit] = milliseconds >= 1_000 ? [milliseconds / 1_000, "s"] : [milliseconds, "ms"];
  const text = value >= 1_000 ? Math.round(value).toLocaleString("en-US") : String(Number(value.toPrecision(3)));
  return `${text} ${unit}`;
}

// Positions count buckets above FLOOR_MS: bucket i ≥ 2 spans (i - 2, i - 1], and a
// domain's bin j merges the buckets that end within (j * size, (j + 1) * size].
const position = (milliseconds) => Math.log(Math.max(milliseconds, FLOOR_MS) / FLOOR_MS) / STEP;
const valueAt = (position) => FLOOR_MS * Math.exp(position * STEP);
const bucketEnd = (histogram, index) => HISTOGRAM_BOUNDS[index] === Infinity
  ? position(finite(histogram.max) ? histogram.max : HISTOGRAM_BOUNDS.at(-2)) : Math.max(index - 1, 0);

function nearestRank(histogram, rank) {
  let seen = 0;
  for (let index = 0; index < histogram.buckets.length; index++) if ((seen += histogram.buckets[index]) >= rank) return index;
  return histogram.buckets.length - 1;
}

/**
 * One log-scaled axis for histograms that share a scale, such as table rows: from
 * their lowest p0.1 to their highest p99.9, at least a decade wide, cut into about
 * `bins` bins of whole buckets with a margin bin at each end.
 */
export function latencyDomain(histograms, { bins = 44 } = {}) {
  let from = Infinity, to = -Infinity;
  for (const histogram of histograms.filter(usableHistogram)) {
    from = Math.min(from, Math.max(nearestRank(histogram, Math.ceil(histogram.samples * TAIL)) - 2, -1));
    to = Math.max(to, bucketEnd(histogram, nearestRank(histogram, Math.ceil(histogram.samples * (1 - TAIL)))));
  }
  if (!finite(from)) return null;
  const widen = Math.max(0, DECADE - (to - from)) / 2;
  const size = Math.max(1, Math.round((to - from + 2 * widen) / bins));
  return { size, low: Math.floor((from - widen) / size) - 1, high: Math.ceil((to + widen) / size) };
}

/** The span of a domain's axis, for captions of unlabeled sparklines. */
export function latencyRange(domain) {
  return domain ? `${duration(valueAt(domain.low * domain.size))} to ${duration(valueAt((domain.high + 1) * domain.size))}` : "—";
}

function binCounts(histogram, size) {
  const bins = new Map();
  histogram.buckets.forEach((samples, index) => {
    if (!samples) return;
    const bin = Math.ceil(bucketEnd(histogram, index) / size) - 1;
    bins.set(bin, (bins.get(bin) ?? 0) + samples);
  });
  return [...bins].sort(([a], [b]) => a - b);
}

const binLabel = (bin, size) => bin < 0 ? `≤ ${duration(FLOOR_MS)}` : `${duration(valueAt(bin * size))}–${duration(valueAt((bin + 1) * size))}`;
const binTitle = (bar, size, total, noun) => `${binLabel(bar.bin, size)}: ${share(bar.samples / total)} of ${noun} (${count(bar.samples)})`;

function layout(histogram, domain, left, right, top, plot) {
  const slots = domain.high - domain.low + 1, slot = (right - left) / slots;
  const gap = slot >= 6 ? 2 : slot >= 3 ? 1 : 0;
  const visible = binCounts(histogram, domain.size).filter(([bin]) => bin >= domain.low && bin <= domain.high);
  const peak = Math.max(...visible.map(([, samples]) => samples));
  const bars = visible.map(([bin, samples]) => {
    const height = Math.max(1.5, samples / peak * plot), from = left + (bin - domain.low) * slot;
    return { bin, samples, from, x: from + gap / 2, width: Math.max(0.75, slot - gap), y: top + plot - height, height };
  });
  const x = (milliseconds) => left + Math.min(slots, Math.max(0, position(milliseconds) / domain.size - domain.low)) * slot;
  return { bars, x, slot, outside: histogram.samples - visible.reduce((sum, [, samples]) => sum + samples, 0) };
}

function barPath({ x, y, width, height }, base) {
  const radius = Math.min(2, width / 2, height);
  const r = round, right = x + width;
  if (radius < 0.5) return `M${r(x)} ${r(base)}V${r(y)}H${r(right)}V${r(base)}Z`;
  return `M${r(x)} ${r(base)}V${r(y + radius)}Q${r(x)} ${r(y)} ${r(x + radius)} ${r(y)}H${r(right - radius)}Q${r(right)} ${r(y)} ${r(right)} ${r(y + radius)}V${r(base)}Z`;
}

// Ticks at 1, 2 and 5 per decade; labels thin to decades, then every other decade, until none collide.
function axis(domain, left, right) {
  const slot = (right - left) / (domain.high - domain.low + 1);
  const x = (milliseconds) => left + (position(milliseconds) / domain.size - domain.low) * slot;
  const from = valueAt(domain.low * domain.size), to = valueAt((domain.high + 1) * domain.size);
  const ticks = [];
  for (let exponent = Math.floor(Math.log10(from)); exponent <= Math.ceil(Math.log10(to)); exponent++) {
    for (const mantissa of [1, 2, 5]) {
      const value = Number((mantissa * 10 ** exponent).toPrecision(12));
      if (value >= from && value <= to) ticks.push({ value, x: x(value), major: mantissa === 1 });
    }
  }
  const place = (chosen) => chosen.map((tick) => {
    const text = duration(tick.value), width = text.length * CHARACTER;
    const anchor = tick.x - width / 2 < left ? "start" : tick.x + width / 2 > right ? "end" : "middle";
    const start = anchor === "start" ? tick.x : anchor === "end" ? tick.x - width : tick.x - width / 2;
    return { ...tick, text, anchor, start, end: start + width };
  });
  const majors = ticks.filter((tick) => tick.major);
  const labels = [ticks, majors, majors.filter((_, index) => index % 2 === 0), majors.slice(0, 1)].map(place)
    .find((chosen) => chosen.every((tick, index) => !index || tick.start >= chosen[index - 1].end + 8)) ?? [];
  return { ticks, labels };
}

function summary(histogram, label, domain, outside, noun) {
  return `${label}: ${counted(histogram.samples, noun)}; p50 ${duration(histogram.p50)}, p99 ${duration(histogram.p99)}, max ${duration(histogram.max)}. ` +
    `Log axis from ${latencyRange(domain)}${outside ? `; ${share(outside / histogram.samples)} of ${noun} fall outside it` : ""}.`;
}

function markerLabels(markers, left, right) {
  const width = (text) => text.length * CHARACTER;
  const placed = markers.map((marker, index) => {
    const text = `${marker.name} ${duration(marker.value)}`;
    let anchor = index === 0 ? "end" : "start";
    let start = anchor === "end" ? marker.x - 4 - width(text) : marker.x + 4;
    if (start < left) { anchor = "start"; start = marker.x + 4; }
    if (start + width(text) > right) { anchor = "end"; start = marker.x - 4 - width(text); }
    return { ...marker, text, anchor, start, end: start + width(text), row: 0 };
  });
  for (let i = 1; i < placed.length; i++) {
    if (placed[i].start < placed[i - 1].end + 6 && placed[i].end > placed[i - 1].start - 6) placed[i].row = placed[i - 1].row + 1;
  }
  return placed;
}

/**
 * A latency histogram on a log time axis: bar height is each bin's share of calls,
 * lines mark p50 and p99, and every nonempty bin stays at least 1.5 px tall.
 */
export function latencyHistogram(histogram, { domain = latencyDomain([histogram]), color = LATENCY_COLORS.other, label = "Latency", noun = "calls", width = 300, height = 132 } = {}) {
  if (!usableHistogram(histogram) || !domain) return `<p class="latency-empty">No ${escape(label.toLowerCase())} measurements.</p>`;
  const left = 4, right = width - 4, top = 30, plot = height - top - 20, base = top + plot;
  const { bars, x, slot, outside } = layout(histogram, domain, left, right, top, plot);
  const { ticks, labels } = axis(domain, left, right);
  const markers = markerLabels(["p50", "p99"].filter((name) => finite(histogram[name]))
    .map((name) => ({ name, value: histogram[name], x: x(histogram[name]) })), left, right);
  const described = escape(summary(histogram, label, domain, outside, noun));
  return `<svg class="latency-histogram" viewBox="0 0 ${width} ${height}" role="img" aria-label="${described}"><title>${described}</title>` +
    ticks.map((tick) => `<line x1="${round(tick.x)}" x2="${round(tick.x)}" y1="${base}" y2="${base + (tick.major ? 4 : 2.5)}" stroke="${RULE}"/>`).join("") +
    labels.map((tick) => `<text x="${round(tick.x)}" y="${height - 5}" text-anchor="${tick.anchor}" class="latency-axis" fill="${MUTED}">${escape(tick.text)}</text>`).join("") +
    bars.map((bar) => `<g class="latency-bin"><title>${escape(binTitle(bar, domain.size, histogram.samples, noun))}</title>` +
      `<rect x="${round(bar.from)}" y="${top}" width="${round(slot)}" height="${plot}" fill="transparent"/><path d="${barPath(bar, base)}" fill="${color}"/></g>`).join("") +
    `<line x1="${left}" x2="${right}" y1="${base}" y2="${base}" stroke="${RULE}"/><g pointer-events="none">` +
    markers.map((marker) => {
      const y = 10 + marker.row * 11;
      return `<line x1="${round(marker.x)}" x2="${round(marker.x)}" y1="${y + 3}" y2="${base}" stroke="${INK}" stroke-opacity=".55"/>` +
        `<text x="${round(marker.anchor === "end" ? marker.x - 4 : marker.x + 4)}" y="${y + 3}" text-anchor="${marker.anchor}" class="latency-marker" fill="${INK}">${escape(marker.text)}</text>`;
    }).join("") + `</g></svg>`;
}

/** Bars only, for tiles and table cells; a line marks p99 and the title carries the numbers. */
export function latencySparkline(histogram, { domain = latencyDomain([histogram], { bins: 24 }), color = LATENCY_COLORS.other, label = "Latency", noun = "calls", width = 120, height = 26 } = {}) {
  if (!usableHistogram(histogram) || !domain) return "—";
  const base = height - 1;
  const { bars, x, outside } = layout(histogram, domain, 0, width, 2, height - 3);
  const described = escape(summary(histogram, label, domain, outside, noun));
  const p99 = finite(histogram.p99) ? `<line x1="${round(x(histogram.p99))}" x2="${round(x(histogram.p99))}" y1="0" y2="${base}" stroke="${INK}" stroke-opacity=".6"/>` : "";
  return `<svg class="latency-sparkline" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="${described}"><title>${described}</title>` +
    bars.map((bar) => `<path d="${barPath(bar, base)}" fill="${color}"/>`).join("") +
    `<line x1="0" x2="${width}" y1="${base}" y2="${base}" stroke="${RULE}"/>${p99}</svg>`;
}

/** A sparkline above its p99, for table cells; rows given one domain share a scale. */
export function latencyCell(histogram, options) {
  return usableHistogram(histogram) ? `<span class="latency-cell">${latencySparkline(histogram, options)}<small>p99 ${escape(duration(histogram.p99))}</small></span>` : "—";
}

/** The table view of a histogram: every nonempty bin, including any beyond the axis. */
export function latencyTable(histogram, caption, { domain = latencyDomain([histogram]), noun = "calls" } = {}) {
  if (!usableHistogram(histogram) || !domain) return "";
  const percent = (fraction) => fraction > 0 && fraction < 0.00005 ? "<0.01%" : `${(fraction * 100).toFixed(2)}%`;
  let running = 0;
  const rows = binCounts(histogram, domain.size).map(([bin, samples]) => {
    running += samples;
    const cumulative = running === histogram.samples ? "100%" : `${Math.min(99.99, running / histogram.samples * 100).toFixed(2)}%`;
    return `<tr><th scope="row">${escape(binLabel(bin, domain.size))}</th><td>${count(samples)}</td><td>${percent(samples / histogram.samples)}</td><td>${cumulative}</td></tr>`;
  }).join("");
  return `<table class="latency-table"><caption>${escape(caption)}</caption><thead><tr><th scope="col">Latency</th><th scope="col">${noun[0].toUpperCase()}${noun.slice(1)}</th><th scope="col">Share</th><th scope="col">Cumulative</th></tr></thead><tbody>${rows}</tbody></table>`;
}
