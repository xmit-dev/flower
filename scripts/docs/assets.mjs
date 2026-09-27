import { globSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

// Shared by Eleventy's passthrough copier and the in-memory link checker.
export const assetPatterns = ["assets/**/*", "*.css", "*.ts", "site.js", "CNAME", ".nojekyll"];

export function assetFiles() {
  const source = fileURLToPath(new URL("../../docs/", import.meta.url));
  return new Set(globSync(assetPatterns, { cwd: source, withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => relative(source, join(entry.parentPath, entry.name)).split("\\").join("/")));
}
