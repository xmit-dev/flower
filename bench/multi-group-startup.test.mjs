import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import test from "node:test";

test("startup failure replaces old reports, prunes surplus groups, and cleans worker processes", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "flower-failed-start-"));
  try {
    const json = join(directory, "latest.json"), html = join(directory, "latest.html");
    await writeFile(json, JSON.stringify({ passed: true, sentinel: "old report" }));
    const groups = join(directory, "latest-groups");
    await mkdir(groups);
    for (const group of [0, 1, 8, 11]) {
      for (const extension of ["json", "html"]) await writeFile(join(groups, `group-${group}.${extension}`), "old child report");
    }
    await writeFile(join(groups, "notes.txt"), "keep notes");
    await writeFile(join(groups, "group-8.sample.txt"), "keep sample");
    await mkdir(join(groups, "group-9.json"));
    await writeFile(join(groups, "group-9.json", "notes.txt"), "keep directory");
    const external = join(directory, "external.json");
    await writeFile(external, "keep external target");
    if (process.platform !== "win32") await symlink(external, join(groups, "group-10.json"));
    let failure;
    try {
      await promisify(execFile)(process.execPath, ["bench/goblin-pizza.mjs", "--groups", "2", "--nodes", "1", "--binary", join(directory, "missing-server"), "--json", json], { timeout: 12_000 });
    } catch (error) { failure = error; }
    assert.equal(failure?.code, 1);
    assert.match(failure.stderr, /missing-server|Group .* exited before the start barrier/);
    const report = JSON.parse(await readFile(json, "utf8"));
    assert.equal(report.passed, false);
    assert.equal(report.sentinel, undefined);
    assert.equal(report.groups.length, 2);
    assert.ok(report.violations.length > 0);
    assert.match(await readFile(html, "utf8"), /failed|Failed|FAIL/);
    assert.deepEqual((await readdir(groups)).sort(), [
      "group-0.html", "group-0.json", "group-1.html", "group-1.json",
      "group-8.sample.txt", "group-9.json", "notes.txt",
    ]);
    assert.equal(await readFile(join(groups, "notes.txt"), "utf8"), "keep notes");
    assert.equal(await readFile(join(groups, "group-9.json", "notes.txt"), "utf8"), "keep directory");
    assert.equal(await readFile(external, "utf8"), "keep external target");
  } finally { await rm(directory, { recursive: true, force: true }); }
});
