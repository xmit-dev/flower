// Installed only by the mixed diagnostic, inside each controller process.
import { writeFile, readFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { StringDecoder } from "node:string_decoder";
import { LocalCluster } from "./cluster.mjs";
import { parseProfileLine } from "./profile-metrics.mjs";

// Discard an oversized line through its next newline, without retaining a tail
// that could be mistaken for another record. Decode split UTF-8 characters once.
export function boundedLogLines(record, oversized, incomplete, maximum = 1024 * 1024) {
  const decoder = new StringDecoder("utf8");
  let partial = "", skipping = false;
  const consume = (text) => {
    let offset = 0;
    while (offset < text.length) {
      const newline = text.indexOf("\n", offset);
      const part = text.slice(offset, newline < 0 ? undefined : newline);
      if (!skipping) {
        if (partial.length + part.length > maximum) {
          partial = "";
          skipping = true;
          oversized();
        } else partial += part;
      }
      if (newline < 0) break;
      if (!skipping) record(partial);
      partial = "";
      skipping = false;
      offset = newline + 1;
    }
  };
  return {
    write: (chunk) => consume(decoder.write(chunk)),
    end: () => { consume(decoder.end()); if (partial) incomplete(); partial = ""; },
  };
}

export function parseStorageLine(raw) {
  const line = raw.replace(/\x1b\[[0-9;]*m/g, "");
  if (!line.includes("storage write")) return null;
  const unquote = value => {
    if (!value.startsWith('"')) return value;
    try { return JSON.parse(value); } catch { return value.slice(1, -1); }
  };
  const fields = Object.fromEntries([...line.matchAll(/\b([a-z_]+)=("(?:[^"\\]|\\.)*"|\S+)/g)]
    .map(([, key, value]) => [key, unquote(value)]));
  return { operation: fields.operation, ok: fields.ok === "true",
    ...Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== "" && Number.isFinite(Number(value)))
      .map(([key, value]) => [key, Number(value)])),
  };
}

export function installMixedObserver(config, { Cluster = LocalCluster, now = Date.now } = {}) {
  const data = { token: config.token, group: config.group, startedAt: now(),
    groups: [], responses: [], storage: [], evaluator: [], dropped: 0,
    oversizedLogLines: 0, incompleteLogLines: 0, bytes: {} };
  let loadStart = Infinity;
  const streams = [];
  const original = Cluster.prototype._startNode;
  const restored = () => { if (Cluster.prototype._startNode === observed) Cluster.prototype._startNode = original; };
  function observed(node) {
    original.call(this, node);
    const lines = boundedLogLines(line => {
      const receivedAt = now();
      if (receivedAt < loadStart + config.startOffsetMs) return;
      const disk = parseStorageLine(line);
      const record = disk ? { type: "storage", value: disk } : parseProfileLine(line);
      const type = record?.type === "server" ? "responses" : record?.type;
      if (!["groups", "responses", "storage", "evaluator"].includes(type)) return;
      const value = { ...record.value, receivedAt, group: config.group,
        cluster: basename(this.directory), node: node.id, pid: node.process.child.pid };
      const bytes = Buffer.byteLength(JSON.stringify(value));
      if (data[type].length >= config.maxRecords || (data.bytes[type] ?? 0) + bytes > config.maxBytes) {
        data.dropped++; return;
      }
      data.bytes[type] = (data.bytes[type] ?? 0) + bytes;
      data[type].push(value);
    }, () => { data.dropped++; data.oversizedLogLines++; }, () => { data.dropped++; data.incompleteLogLines++; });
    const stream = node.process.child.stdout;
    stream.on("data", lines.write);
    stream.once("end", lines.end);
    streams.push({ stream, lines });
  }
  Cluster.prototype._startNode = observed;
  return {
    begin(startAt) { loadStart = startAt; data.loadStartedAt = startAt; },
    restore() { restored(); },
    async finish(runId) {
      restored();
      for (const { stream, lines } of streams) {
        stream.off("data", lines.write); stream.off("end", lines.end);
        lines.end();
      }
      data.finishedAt = now(); data.runId = runId;
      await writeFile(join(config.directory, `group-${config.group}.json`), JSON.stringify(data), { flag: "wx" });
      return data;
    },
  };
}

export async function readMixedCaptures(config, report, count) {
  const merged = { groups: [], responses: [], storage: [], evaluator: [], dropped: 0,
    oversizedLogLines: 0, incompleteLogLines: 0, sources: [] };
  for (let group = 0; group < count; group++) {
    const data = JSON.parse(await readFile(join(config.directory, `group-${group}.json`), "utf8"));
    if (data.token !== config.token || data.group !== group || data.runId !== report.runId
      || !Number.isFinite(data.startedAt) || data.startedAt < config.startedAt
      || !Number.isFinite(data.finishedAt) || data.finishedAt < data.startedAt
      || !Number.isFinite(data.loadStartedAt) || data.loadStartedAt < config.startedAt) {
      throw new Error(`Group ${group} diagnostic is missing or stale for this run`);
    }
    for (const type of ["groups", "responses", "storage", "evaluator"]) {
      if (!Array.isArray(data[type]) || data[type].length > config.maxRecords
        || data[type].some(record => record.group !== group || !Number.isFinite(record.receivedAt)
          || record.receivedAt < data.startedAt || record.receivedAt > data.finishedAt)) {
        throw new Error(`Group ${group} diagnostic has invalid ${type} records`);
      }
      for (const record of data[type]) merged[type].push(record);
    }
    for (const key of ["dropped", "oversizedLogLines", "incompleteLogLines"]) {
      if (!Number.isSafeInteger(data[key]) || data[key] < 0) throw new Error(`Group ${group} has invalid diagnostic counters`);
      merged[key] += data[key];
    }
    merged.sources.push({ group, runId: data.runId, startedAt: data.startedAt,
      finishedAt: data.finishedAt, loadStartedAt: data.loadStartedAt, bytes: data.bytes,
      dropped: data.dropped, oversizedLogLines: data.oversizedLogLines, incompleteLogLines: data.incompleteLogLines });
  }
  return merged;
}
