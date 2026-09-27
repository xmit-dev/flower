import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { basename, dirname, resolve } from "node:path";
import { build } from "esbuild";
import type { Bundle, JavaScriptBundle } from "./client.ts";

export interface BuildOptions {
  /** Static (default): module initialization runs once at deploy and every callback starts
   * from a copy-on-write snapshot. per-invocation reruns module code in each callback. */
  initialization?: "per-invocation" | "static";
}

/** Compile a default-exported define(...) module for the isolated server runtime. */
export async function buildBundle(entry: string, options: BuildOptions = {}): Promise<JavaScriptBundle> {
  if (options.initialization !== undefined &&
      !["per-invocation", "static"].includes(options.initialization)) {
    throw new TypeError("initialization must be per-invocation or static");
  }
  const absolute = resolve(entry);
  const result = await build({
    absWorkingDir: dirname(absolute),
    entryPoints: [basename(absolute)],
    bundle: true,
    write: false,
    format: "iife",
    globalName: "__flowerBundle",
    platform: "neutral",
    target: "es2020",
    charset: "ascii",
    legalComments: "none",
    treeShaking: true,
    logLevel: "silent",
  });
  const javascript = (options.initialization === "per-invocation" ? "" : "/* flower:static-init */\n") + result.outputFiles[0].text;
  return { hash: createHash("sha256").update(javascript).digest("hex"), javascript };
}

const sha256 = (bytes: string | Uint8Array) => createHash("sha256").update(bytes).digest("hex");

/** A .ts module (built), a .wasm guest module, or a built .json bundle of either kind. */
export async function loadBundle(path: string, options: BuildOptions = {}): Promise<Bundle> {
  if (path.endsWith(".wasm")) {
    if (options.initialization !== undefined) throw new TypeError("Initialization modes apply only to JavaScript bundles");
    const wasm = await readFile(path);
    return { hash: sha256(wasm), wasm: wasm.toString("base64") };
  }
  if (!path.endsWith(".json")) return buildBundle(path, options);
  if (options.initialization !== undefined) throw new TypeError("A built bundle already specifies its initialization mode");
  const bundle = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown> | null;
  if (bundle !== null && typeof bundle === "object" && typeof bundle.hash === "string" && typeof bundle.wasm === "string" && !("javascript" in bundle)) {
    const wasm = Buffer.from(bundle.wasm, "base64");
    if (wasm.toString("base64") !== bundle.wasm) throw new TypeError("Bundle wasm must be canonical base64");
    if (bundle.hash !== sha256(wasm)) throw new TypeError("Bundle hash does not match its module");
    return { hash: bundle.hash, wasm: bundle.wasm };
  }
  if (bundle === null || typeof bundle !== "object" || typeof bundle.hash !== "string" || typeof bundle.javascript !== "string") {
    throw new TypeError("Bundle file must contain hash and javascript strings, or hash and wasm strings");
  }
  const hash = sha256(bundle.javascript);
  if (bundle.hash !== hash) throw new TypeError("Bundle hash does not match its JavaScript");
  return { hash, javascript: bundle.javascript };
}

export async function writeBundle(entry: string, output: string, options: BuildOptions = {}): Promise<JavaScriptBundle> {
  const bundle = await buildBundle(entry, options);
  await writeFile(output, JSON.stringify(bundle, null, 2) + "\n");
  return bundle;
}
