import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import test from "node:test";
import { base64url, jwt, nacl, sha256, webauthn } from "./crypto.ts";

const docs = new URL("../docs/", import.meta.url);
// The reference spans several pages; audit them together.
const reference = () => readdirSync(new URL("reference/", docs)).filter((file) => file.endsWith(".html"))
  .map((file) => readFileSync(new URL(`reference/${file}`, docs), "utf8")).join("\n");

test("the SDK reference covers every public export and class method", () => {
  const html = reference();
  const documented = new Set([...html.matchAll(/data-api-symbol="([^"]+)"/g)].map((match) => match[1]));
  const methods = new Set([...html.matchAll(/data-api-method="([^"]+)"/g)].map((match) => match[1]));
  // The public entrypoints use explicit named declarations and re-exports.
  // Audit that source form directly; TypeScript 7 exposes no legacy compiler API.
  for (const file of ["index", "client", "temporal", "scheduler", "worker", "testing", "http2", "bundle", "crypto"]) {
    const source = readFileSync(new URL(`./${file}.ts`, import.meta.url), "utf8");
    assert.doesNotMatch(source, /^export\s+\*/m, "Expand star exports before auditing the SDK reference");
    const names = [...source.matchAll(/^export\s+(?:async\s+)?(?:function|class|interface|type|const|enum)\s+(\w+)/gm)].map((match) => match[1]);
    for (const list of source.matchAll(/^export\s+(?:type\s+)?\{([^}]+)\}\s+from\s+/gm)) {
      names.push(...list[1].split(",").map((item) => item.trim().split(/\s+as\s+/).at(-1)!).filter(Boolean));
    }
    assert.ok(names.length > 0, `${file} must expose an audited entrypoint`);
    for (const name of names) assert.ok(documented.has(name), `${file} export ${name} is missing from the API reference`);
    for (const declaration of source.matchAll(/^export class (\w+)[^{]*\{([\s\S]*?)^\}/gm)) {
      for (const member of declaration[2].matchAll(/^  (?!(?:private|protected)\b)(?:public\s+)?(?:async\s+)?(\w+)\s*(?:<[^\n]+>)?\(/gm)) {
        assert.ok(methods.has(`${declaration[1]}.${member[1]}`), `${declaration[1]}.${member[1]} is missing from the API reference`);
      }
    }
  }
});

test("website TypeScript examples import only published SDK paths", () => {
  const files = readdirSync(docs, { recursive: true, encoding: "utf8" })
    .filter((file) => /\.(?:html|ts)$/.test(file) && !file.startsWith("bench"));
  assert.ok(files.length > 20, "the website should have many pages");
  for (const file of files) {
    const source = readFileSync(new URL(file, docs), "utf8")
      .replace(/<\/?span\b[^>]*>/g, "").replaceAll("&quot;", '"');
    for (const match of source.matchAll(/\bfrom\s+["']([^"']+)["']/g)) {
      assert.match(match[1], /^(?:node:[a-z/_]+|@flower-js\/sdk(?:\/(?:client|temporal|scheduler|worker|testing|http2|bundle|crypto))?|\.\/[\w-]+\.ts)$/, `${file} must show supported package imports`);
    }
  }
});

test("the crypto reference covers every high-level method and attached constant", () => {
  const html = reference();
  const methods = new Set([...html.matchAll(/data-api-method="([^"]+)"/g)].map((match) => match[1]));
  const values = new Set([...html.matchAll(/data-api-value="([^"]+)"/g)].map((match) => match[1]));
  function visit(path: string, value: unknown): void {
    if (typeof value === "function") assert.ok(methods.has(path), `${path} method is missing`);
    if (value !== null && (typeof value === "object" || typeof value === "function")) {
      for (const [name, child] of Object.entries(value)) visit(`${path}.${name}`, child);
    } else assert.ok(values.has(path), `${path} constant is missing`);
  }
  visit("nacl", nacl); visit("jwt", jwt); visit("sha256", sha256); visit("base64url", base64url); visit("webauthn", webauthn);
});
