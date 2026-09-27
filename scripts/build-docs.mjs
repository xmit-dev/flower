// Build docs/ into the disposable _site/ directory with Eleventy (Build Awesome).
// --check renders and validates in memory, without requiring or writing build output.
import Eleventy from "@11ty/eleventy";
import { rmSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = resolve(root, "_site");

export async function buildDocs({ check = false } = {}) {
  const generator = new Eleventy(resolve(root, "docs"), output, {
    configPath: resolve(root, "eleventy.config.mjs"),
    quietMode: true,
  });
  if (!check) rmSync(output, { recursive: true, force: true });
  const results = check ? await generator.toJSON() : (await generator.write())[1];
  return { pages: results.length };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const check = process.argv.includes("--check");
  const result = await buildDocs({ check });
  console.log(`${check ? "Verified" : "Built"} ${result.pages} generated site files${check ? " in memory" : " in _site/"}`);
}
