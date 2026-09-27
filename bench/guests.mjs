// The Goblin Pizza application as either guest: the TypeScript bundle run by
// QuickJS, or its Rust port (examples/goblin-pizza-rs) as a Wasm module.
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const root = fileURLToPath(new URL("../", import.meta.url));
export const GUESTS = ["js", "wasm"];
export const WASM_GUEST = resolve(root, "target/wasm32-unknown-unknown/release/goblin_pizza.wasm");
export const ENGINES = { js: "quickjs", wasm: "wasm" };
export const GUEST_LABELS = { js: "TypeScript on QuickJS", wasm: "Rust compiled to Wasm" };
/** Reports before guest selection existed ran the TypeScript guest. */
export const guestOf = (report) => report?.options?.guest ?? "js";

/** Build the Rust guest; requires the wasm32-unknown-unknown target (nix develop). */
export async function buildWasmGuest() {
  await promisify(execFile)("cargo", ["build", "--release", "--locked", "-p", "goblin-pizza", "--target", "wasm32-unknown-unknown"],
    { cwd: root, maxBuffer: 16 << 20 });
  return WASM_GUEST;
}

/** A deployable bundle: `{hash, javascript}` or `{hash, wasm}` with base64 module bytes. */
export async function goblinBundle({ guest = "js", guestWasm = WASM_GUEST, initialization } = {}) {
  if (guest === "js") {
    const { buildBundle } = await import("../sdk/bundle.ts");
    return buildBundle(resolve(root, "examples/goblin-pizza-ts/goblin-pizza.ts"), { initialization });
  }
  if (guest !== "wasm") throw new TypeError(`Unknown guest ${guest}`);
  let wasm;
  try { wasm = await readFile(guestWasm); }
  catch (error) {
    if (error.code !== "ENOENT") throw error;
    throw new Error(`Missing Wasm guest ${guestWasm}; build it with cargo build --release -p goblin-pizza --target wasm32-unknown-unknown`);
  }
  return { hash: createHash("sha256").update(wasm).digest("hex"), wasm: wasm.toString("base64") };
}

// node bench/guests.mjs DIR: build both guests into DIR for guest_parity_tests.rs.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [directory, ...rest] = process.argv.slice(2);
  if (!directory || rest.length) throw new Error("Usage: node bench/guests.mjs OUTPUT_DIRECTORY");
  await mkdir(directory, { recursive: true });
  const { javascript } = await goblinBundle({ guest: "js" });
  await writeFile(resolve(directory, "goblin-pizza.js"), javascript);
  await writeFile(resolve(directory, "goblin-pizza.wasm"), await readFile(await buildWasmGuest()));
  console.log(`Wrote ${resolve(directory, "goblin-pizza.js")} and ${resolve(directory, "goblin-pizza.wasm")}`);
}
