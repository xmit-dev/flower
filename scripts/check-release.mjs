import assert from "node:assert/strict";
import { appendFile, readFile } from "node:fs/promises";
const manifest = JSON.parse(await readFile("package.json", "utf8"));
const cargo = await readFile("Cargo.toml", "utf8");
const packageSection = cargo.split(/^\[package\]\s*$/m)[1]?.split(/^\[/m)[0];
const rustVersion = packageSection?.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
assert.equal(manifest.name, "@flower-js/sdk");
assert.equal(manifest.version, rustVersion, "Cargo and npm release versions must match");
const npmLock = JSON.parse(await readFile("package-lock.json", "utf8"));
assert.equal(npmLock.name, manifest.name);
assert.equal(npmLock.version, manifest.version);
assert.equal(npmLock.packages[""].name, manifest.name);
assert.equal(npmLock.packages[""].version, manifest.version);
const cargoLock = await readFile("Cargo.lock", "utf8");
const rootLockVersion = cargoLock.match(/\[\[package\]\]\s*name = "flower"\s*version = "([^"]+)"/)?.[1];
assert.equal(rootLockVersion, rustVersion, "Cargo.lock must include the release version");
assert.match(manifest.version, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/, "Use a release or prerelease version without build metadata");
if (process.env.GITHUB_REF_TYPE === "tag") {
  assert.equal(process.env.GITHUB_REF_NAME, `v${manifest.version}`, "Tag must match Cargo and npm versions");
}
if (process.env.GITHUB_OUTPUT) await appendFile(process.env.GITHUB_OUTPUT, `version=${manifest.version}\n`);
console.log(`Release ${manifest.version}: flower + ${manifest.name}`);
