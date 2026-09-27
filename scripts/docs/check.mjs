import { readFileSync } from "node:fs";
import { posix } from "node:path";
import { assetFiles } from "./assets.mjs";
import { legacy, pages } from "./pages.mjs";

const redirects = JSON.parse(readFileSync(new URL("./redirects.json", import.meta.url), "utf8"));

// Every relative href/src must name a file that exists, and every fragment an id in it.
function checkLinks(outputs) {
  const assets = assetFiles();
  const read = (file) => outputs.get(file) ?? (assets.has(file) ? "" : null);
  const ids = new Map();
  const idsOf = (file) => {
    if (!ids.has(file)) ids.set(file, new Set([...(read(file) ?? "").matchAll(/\bid="([^"]+)"/g)].map((m) => m[1])));
    return ids.get(file);
  };
  const problems = [];
  for (const [file, html] of outputs) {
    if (!file.endsWith(".html")) continue;
    for (const m of html.matchAll(/\b(?:href|src)="([^"]+)"/g)) {
      const value = m[1].replaceAll("&amp;", "&");
      if (/^[a-z][a-z\d+.-]*:/i.test(value)) continue;
      const [pathPart, hash] = value.split("#");
      let target = posix.normalize(posix.join(posix.dirname(file), pathPart.split("?")[0] || posix.basename(file)));
      if (pathPart.endsWith("/") || pathPart === "." || pathPart === "./" || pathPart === "..") target = posix.join(target, "index.html");
      if (target.startsWith("..")) { problems.push(`${file}: ${value} leaves the site`); continue; }
      if (read(target) === null) { problems.push(`${file}: ${value} → missing ${target}`); continue; }
      if (hash && hash !== "top" && target.endsWith(".html") && !idsOf(target).has(hash)) problems.push(`${file}: ${value} → no #${hash} in ${target}`);
    }
  }
  if (problems.length) throw new Error(`Broken documentation links:\n  ${problems.join("\n  ")}`);
}

export function validateDocs(outputs) {
  for (const file of ["index.html", "redirects.js", ...pages.map((page) => page.path), ...legacy.map((page) => page.path)]) {
    if (!outputs.has(file)) throw new Error(`Missing generated documentation: ${file}`);
  }
  // Retired URLs must keep forwarding to real pages and anchors.
  for (const [file, targets] of Object.entries(redirects)) {
    for (const [id, target] of Object.entries(targets)) {
      const [path, hash] = target.split("#");
      const html = outputs.get(!path || path.endsWith("/") ? `${path}index.html` : path);
      if (!html || (hash && !html.includes(`id="${hash}"`))) throw new Error(`Redirect ${file}#${id} → ${target} has no target`);
    }
  }
  checkLinks(outputs);
}
