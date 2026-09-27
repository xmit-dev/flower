// Parse bounded diagnostics from the Rust-driven mixed workload.
export const evaluatorMetric = (key) => key.endsWith("_ms") || key.endsWith("_count")
  || key.endsWith("_bytes_sum") || key === "max_cell_nesting" || key === "state_keys";

export function summary(samples) {
  const ordered = samples.filter((value) => Number.isFinite(value) && value >= 0).sort((a, b) => a - b);
  const quantile = (fraction) => ordered.length ? ordered[Math.ceil(ordered.length * fraction) - 1] : null;
  return { count: ordered.length, invalid: samples.length - ordered.length,
    mean: ordered.length ? ordered.reduce((sum, value) => sum + value, 0) / ordered.length : null,
    p50: quantile(0.5), p95: quantile(0.95), p99: quantile(0.99), max: ordered.at(-1) ?? null };
}

export function parseProfileLine(raw) {
  const line = raw.replace(/\x1b\[[0-9;]*m/g, "");
  const unquote = (value) => {
    if (!value.startsWith('"')) return value;
    try { return JSON.parse(value); } catch { return value.slice(1, -1); }
  };
  const fields = Object.fromEntries([...line.matchAll(/\b([a-z_]+)=("(?:[^"\\]|\\.)*"|\S+)/g)]
    .map(([, key, value]) => [key, unquote(value)]));
  const numbers = (accept) => Object.fromEntries(Object.entries(fields)
    .filter(([key, value]) => accept(key) && Number.isFinite(Number(value)) && Number(value) >= 0)
    .map(([key, value]) => [key, Number(value)]));
  if (line.includes("evaluator wall-clock stages") && fields.name) {
    return { type: "evaluator", value: { mode: fields.mode, name: fields.name,
      ...numbers(evaluatorMetric) } };
  }
  if (line.includes("mutation group timing")) {
    return { type: "groups", value: {
      ...numbers((key) => key.endsWith("_us") || key.startsWith("batch_") || key.startsWith("group_") && key !== "group_committed"
        || key === "speculative_candidates" || key === "speculative_reused"
        || key === "serial_worker_jobs" || key === "serial_worker_requests"),
      ...Object.fromEntries(["batch_mode", "batch_reason", "batch_stop"].filter((key) => fields[key] !== undefined).map((key) => [key, fields[key]])),
      ...(fields.group_committed === "true" || fields.group_committed === "false" ? { group_committed: fields.group_committed === "true" } : {}),
    } };
  }
  if (line.includes("mutation timing") && fields.method) {
    return { type: "server", value: { method: fields.method + (fields.duplicate === "true" ? ".replay" : ""),
      ...numbers((key) => key.endsWith("_us") || key === "batch_commands") } };
  }
  return null;
}
