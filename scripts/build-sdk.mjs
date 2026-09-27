import { copyFile, rm } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
await rm(resolve(root, "dist"), { recursive: true, force: true });
const compiler = spawnSync(process.execPath, [resolve(root, "node_modules/typescript/bin/tsc"), "-p", "tsconfig.build.json"], {
  cwd: root, stdio: "inherit",
});
if (compiler.error) throw compiler.error;
process.exitCode = compiler.status ?? 1;
// The testing module runs applications on the reference engine.
if (process.exitCode === 0) await copyFile(resolve(root, "runtime/engine.js"), resolve(root, "dist/engine.js"));
