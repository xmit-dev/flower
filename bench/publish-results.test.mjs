import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Histogram, Stats } from "./metrics.mjs";
import { summarizeGroups } from "./multi-group.mjs";
import { childPath, publishBenchResults, publishedReports, renderAllBenchResults, renderBenchResults, renderPublishedSummary, replaceSummary } from "../scripts/publish-bench-results.mjs";

function fixture(guest) {
  const stats = new Stats();
  for (let i = 0; i < 100; i++) stats.recordOperation(i < 70 ? "pizza.shop.local" : "pizza.tip", { latencyMs: i < 70 ? 5 : 12.34, ok: true });
  const options = { groups: 1, nodes: 3, readConsistency: "replica-local", http2: true, ...(guest ? { guest } : {}) };
  const child = {
    schemaVersion: 1, passed: true, correctnessPassed: true,
    loadStartedAt: "2026-09-24T01:00:00.000Z", loadEndedAt: "2026-09-24T01:00:00.100Z",
    options: { ...options, tenantIds: ["tenant-0"] },
    binary: { sha256: "measured-binary" }, bundleHash: "measured-bundle",
    environment: { cpu: "Test & CPU" },
    phases: { load: stats.snapshot(100) }, audit: { passed: true }, chaos: [{ quorumRecoveryMs: 456.7 }],
  };
  const report = summarizeGroups([child], options);
  report.groups[0].json = "input-groups/group-0.json";
  return { report, child };
}

test("public summary preserves measured scope, consistency, audits and failure recovery", () => {
  const { report: input } = fixture();
  // Make the aggregate visibly different so neither split can borrow it.
  input.latencyMs.all.p99 = 99.99;
  const html = renderPublishedSummary(input);
  assert.match(html, /1,000/);
  assert.match(html, /aria-label="Customer reads: 70 calls; p50 5 ms, p99 5 ms, max 5 ms\./);
  assert.match(html, /aria-label="Customer writes: 30 calls; p50 12\.3 ms, p99 12\.3 ms, max 12\.3 ms\./);
  assert.match(html, /<caption>Customer reads, all groups<\/caption>/);
  assert.doesNotMatch(html, /Customer p99|100 ms/);
  assert.match(html, /Replica-local reads: lag is allowed/);
  assert.match(html, /70% reads \/ 30% mutations/);
  assert.match(html, /1\/1 group audits passed/);
  assert.match(html, /quorum recovery 457–457 ms/);
  assert.match(html, /union measurement window/);
  assert.match(html, /Test &amp; CPU/);
  assert.doesNotMatch(html, /target|attainment/i);
  input.correctnessPassed = false; input.passed = false; input.groups[0].audit.passed = false;
  input.groups[0].chaos[0].quorumRecoveryMs = null;
  assert.match(renderPublishedSummary(input), /Run failed/);
  assert.match(renderPublishedSummary(input), /0\/1 injected failures have a recorded quorum recovery/);
  input.latencyMs.all.p99 = null;
  assert.throws(() => renderPublishedSummary(input), /incomplete or invalid/);
});

test("public summary leaves missing or empty split latencies unmeasured instead of using the aggregate", () => {
  const { report: input } = fixture();
  delete input.latencyMs.read;
  input.latencyMs.mutation = { samples: 0, p99: 0 };
  let html = renderPublishedSummary(input);
  assert.match(html, /No customer reads measurements/);
  assert.match(html, /No customer writes measurements/);
  assert.doesNotMatch(html, /Customer p99|<svg /);
  const instant = new Histogram();
  instant.record(0);
  input.latencyMs.read = instant.snapshot();
  input.latencyMs.mutation = { samples: 1, p99: null };
  html = renderPublishedSummary(input);
  assert.match(html, /aria-label="Customer reads: 1 call; p50 0 ms, p99 0 ms, max 0 ms\./);
  assert.match(html, /No customer writes measurements/);
});

test("summaries require explicit markers and child JSON stays within its source directory", () => {
  const source = join(tmpdir(), "flower-results", "latest.json");
  for (const path of ["../secret.json", "https://example.com/x.json", "/tmp/secret.json", "group.log", "groups\\secret.json"]) {
    assert.throws(() => childPath(source, path), /relative JSON/);
  }
  assert.equal(childPath(source, "groups/0.json"), join(tmpdir(), "flower-results", "groups/0.json"));
  const page = "before<!-- latest-benchmark:start -->old<!-- latest-benchmark:end -->after";
  assert.equal(replaceSummary(page, "new"), "before<!-- latest-benchmark:start -->\nnew\n<!-- latest-benchmark:end -->after");
  assert.throws(() => replaceSummary(page + page, "new"), /exactly one/);
  assert.throws(() => replaceSummary("missing", "new"), /exactly one/);
});

test("publication is deterministic, checks child identity, and excludes unrelated logs and binaries", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-publication-"));
  try {
    const site = join(directory, "docs"), input = join(directory, "results"), source = join(input, "latest.json");
    await mkdir(join(input, "input-groups"), { recursive: true });
    await mkdir(join(site, "operate"), { recursive: true });
    const { report: aggregate, child } = fixture();
    const splits = ["customerReadLatencyMs", "customerMutationLatencyMs"];
    const expectedSplits = Object.fromEntries(splits.map((key) => [key, aggregate.groups[0][key]]));
    // Older parent reports omit these splits but retain the original child buckets.
    for (const key of splits) delete aggregate.groups[0][key];
    await writeFile(source, JSON.stringify(aggregate));
    const childFile = join(input, "input-groups/group-0.json");
    await writeFile(childFile, JSON.stringify(child));
    await writeFile(join(input, "private.log"), "do not publish");
    await writeFile(join(input, "flower"), "do not publish");
    for (const page of ["index.html", "operate/benchmarks.html"]) {
      await writeFile(join(site, page), "<html><!-- latest-benchmark:start --><!-- latest-benchmark:end --></html>");
    }
    const result = await publishBenchResults({ source, site });
    assert.equal(result.files, 2);
    const publicSource = join(site, "bench/latest.json");
    const snapshot = await readFile(publicSource, "utf8");
    for (const key of splits) assert.deepEqual(JSON.parse(snapshot).groups[0][key], expectedSplits[key]);
    assert.deepEqual(await publishBenchResults({ source: publicSource, site, check: true }), result);
    await publishBenchResults({ source, site });
    assert.equal(await readFile(publicSource, "utf8"), snapshot);
    assert.deepEqual((await readdir(join(site, "bench"))).sort(), ["latest-groups", "latest.json"]);
    assert.deepEqual(await readdir(join(site, "bench/latest-groups")), ["group-0.json"]);
    for (const page of ["index.html", "operate/benchmarks.html"]) {
      assert.equal(await readFile(join(site, page), "utf8"), "<html><!-- latest-benchmark:start --><!-- latest-benchmark:end --></html>");
    }
    const rendered = await renderBenchResults(publicSource);
    assert.deepEqual(await renderBenchResults(publicSource), rendered);
    assert.equal(rendered.size, 4);
    assert.equal(rendered.get("bench/latest.json"), snapshot);
    assert.match(rendered.get("bench/latest-groups/group-0.html"), /href="\.\.\/latest.html">All groups/);
    assert.match(rendered.get("bench/latest.html"), /href="latest-groups\/group-0.html"/);
    assert.match(rendered.get("bench/latest.html"), /class="site-header"[\s\S]*href="\.\.\/reference\/"/);
    await writeFile(join(site, "bench/latest-groups/group-99.json"), "obsolete");
    await assert.rejects(publishBenchResults({ source: publicSource, site, check: true }), /Obsolete benchmark measurement/);
    await publishBenchResults({ source, site });
    assert.deepEqual(await readdir(join(site, "bench/latest-groups")), ["group-0.json"]);
    aggregate.goodputRps *= 2;
    await writeFile(source, JSON.stringify(aggregate));
    await assert.rejects(publishBenchResults({ source, site }), /throughput must match/);
    aggregate.goodputRps /= 2;
    aggregate.latencyMs.all.p99 *= 2;
    await writeFile(source, JSON.stringify(aggregate));
    await assert.rejects(publishBenchResults({ source, site }), /latencyMs does not match/);
    aggregate.latencyMs.all.p99 /= 2;
    for (const key of splits) {
      aggregate.groups[0][key] = { ...expectedSplits[key], p99: 123.45 };
      await writeFile(source, JSON.stringify(aggregate));
      await assert.rejects(publishBenchResults({ source, site }), new RegExp(`Group 0 ${key} does not match`));
      delete aggregate.groups[0][key];
    }
    await writeFile(source, JSON.stringify(aggregate));
    child.binary = { sha256: "wrong-binary" };
    await writeFile(childFile, JSON.stringify(child));
    await assert.rejects(publishBenchResults({ source, site }), /does not match/);
    assert.equal(await readFile(publicSource, "utf8"), snapshot);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("each guest publishes beside the others and the summary compares them", async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-guests-"));
  try {
    const site = join(directory, "docs"), input = join(directory, "results");
    await mkdir(join(input, "input-groups"), { recursive: true });
    for (const guest of [undefined, "wasm"]) {
      const { report: aggregate, child } = fixture(guest);
      const stem = guest ? "latest-wasm" : "latest";
      await writeFile(join(input, "input-groups/group-0.json"), JSON.stringify(child));
      await writeFile(join(input, `${stem}.json`), JSON.stringify(aggregate));
      assert.equal((await publishBenchResults({ source: join(input, `${stem}.json`), site })).files, 2);
    }
    assert.deepEqual((await readdir(join(site, "bench"))).sort(), ["latest-groups", "latest-wasm-groups", "latest-wasm.json", "latest.json"]);
    assert.deepEqual((await readdir(join(site, "bench/latest-wasm-groups"))), ["group-0.json"]);
    await publishBenchResults({ source: join(site, "bench/latest-wasm.json"), site, check: true });
    const rendered = await renderAllBenchResults(join(site, "bench"));
    assert.deepEqual([...rendered.keys()].sort(), [
      "bench/latest-groups/group-0.html", "bench/latest-groups/group-0.json", "bench/latest-wasm-groups/group-0.html",
      "bench/latest-wasm-groups/group-0.json", "bench/latest-wasm.html", "bench/latest-wasm.json", "bench/latest.html", "bench/latest.json",
    ]);
    assert.match(rendered.get("bench/latest-wasm-groups/group-0.html"), /href="\.\.\/latest-wasm.html">All groups/);
    assert.match(rendered.get("bench/latest-wasm.html"), /href="latest-wasm.json">Raw JSON/);
    assert.match(rendered.get("bench/latest-wasm.html"), /Rust compiled to Wasm/);
    const { report, others } = publishedReports(join(site, "bench"));
    const html = renderPublishedSummary(report, { others });
    assert.match(html, /<caption>The same workload with the application in each guest<\/caption>/);
    assert.match(html, /<th scope="col"><a href="bench\/latest.html">TypeScript on QuickJS<\/a><\/th><th scope="col"><a href="bench\/latest-wasm.html">Rust compiled to Wasm<\/a><\/th>/);
    assert.match(html, /with the same server binary/);
    assert.match(html, /<tr><th scope="row">Server CPU&nbsp;\/&nbsp;call<\/th><td>—<\/td><td>—<\/td><\/tr>/);
    assert.match(html, /<tr><th scope="row">Group audits<\/th><td>1\/1<\/td><td>1\/1<\/td><\/tr>/);
    assert.doesNotMatch(renderPublishedSummary(report), /each guest/);
    const mixed = JSON.parse(await readFile(join(site, "bench/latest-wasm.json"), "utf8"));
    mixed.options.guest = "lua";
    assert.throws(() => renderPublishedSummary(mixed), /incomplete or invalid/);
    const { report: wrong, child } = fixture("wasm");
    child.options.guest = "js";
    wrong.groups[0].json = "input-groups/group-0.json";
    await writeFile(join(input, "input-groups/group-0.json"), JSON.stringify(child));
    await writeFile(join(input, "wrong.json"), JSON.stringify(wrong));
    await assert.rejects(publishBenchResults({ source: join(input, "wrong.json"), site }), /guests do not match/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
