// Version-tag releases publish an existing package through npm OIDC. The first
// package publication is manual, and an already published version is immutable.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { appendFile, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const registry = "https://registry.npmjs.org";

export async function publicationState(name, version, fetcher = fetch) {
  const response = await fetcher(`${registry}/${encodeURIComponent(name)}`, {
    headers: { accept: "application/vnd.npm.install-v1+json" },
    signal: AbortSignal.timeout(30_000),
    redirect: "error",
  });
  if (response.status === 404) return "bootstrap";
  if (!response.ok) throw new Error(`npm registry lookup failed: HTTP ${response.status}`);
  const document = await response.json();
  assert.equal(document.name, name, "npm registry returned another package");
  assert.ok(document.versions && typeof document.versions === "object" && !Array.isArray(document.versions),
    "npm registry returned invalid version metadata");
  return Object.hasOwn(document.versions, version) ? "published" : "publish";
}

async function notice(message) {
  console.log(`::notice::${message}`);
  if (process.env.GITHUB_STEP_SUMMARY) await appendFile(process.env.GITHUB_STEP_SUMMARY, `${message}\n`);
}

async function main() {
  const arguments_ = process.argv.slice(2);
  const check = arguments_.includes("--check");
  const paths = arguments_.filter(argument => argument !== "--check");
  assert.ok(paths.length <= 1 && paths.every(path => !path.startsWith("--")),
    "usage: node scripts/publish-sdk.mjs [--check] [sdk.tgz]");
  assert.ok(check || paths.length === 1, "publication requires the tested SDK tarball");
  const manifest = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  if (process.env.RELEASE_VERSION) assert.equal(manifest.version, process.env.RELEASE_VERSION);
  if (paths[0]) {
    const packaged = JSON.parse(execFileSync("tar", ["-xOf", resolve(paths[0]), "package/package.json"], { encoding: "utf8" }));
    assert.equal(packaged.name, manifest.name, "tarball package name must match the release");
    assert.equal(packaged.version, manifest.version, "tarball version must match the release");
  }
  const identity = `${manifest.name}@${manifest.version}`;
  const state = await publicationState(manifest.name, manifest.version);
  if (state === "bootstrap") {
    await notice(`${manifest.name} does not exist on npm yet. Publish the first SDK tarball manually, then configure its release.yml trusted publisher. Binary releases continue normally.`);
    return;
  }
  if (state === "published") {
    await notice(`${identity} is already published; preserving that immutable version.`);
    return;
  }
  const tag = manifest.version.includes("-") ? "next" : "latest";
  if (check) {
    console.log(`${identity} is ready for publication with npm tag ${tag}; check-only mode did not publish.`);
    return;
  }
  // OIDC credentials are supplied by GitHub Actions. No token secret or enable
  // variable participates in this path; authentication errors fail the job.
  execFileSync(process.platform === "win32" ? "npm.cmd" : "npm", [
    "publish", resolve(paths[0]), "--ignore-scripts", "--access", "public", "--tag", tag, "--registry", registry,
  ], { stdio: "inherit" });
}

if (import.meta.main) await main();
