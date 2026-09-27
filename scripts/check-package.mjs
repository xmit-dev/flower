import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const temporary = await mkdtemp(join(tmpdir(), "flower-package-"));
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
function run(command, args, cwd = root) {
  return execFileSync(command, args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
}
try {
  run(npm, ["run", "build"]);
  const [packed] = JSON.parse(run(npm, ["pack", "--ignore-scripts", "--json", "--pack-destination", temporary]));
  assert.equal(packed.name, "@flower-js/sdk");
  const paths = packed.files.map(({ path }) => path);
  for (const required of ["dist/index.js", "dist/index.d.ts", "dist/client.js", "dist/client.d.ts", "dist/crypto.js", "dist/crypto.d.ts", "dist/cli.js", "dist/bundle.js",
    "dist/worker.js", "dist/worker.d.ts", "dist/testing.js", "dist/testing.d.ts", "dist/engine.js", "LICENSE-MIT"]) {
    assert.ok(paths.includes(required), `Missing packaged ${required}`);
  }
  assert.ok(paths.every((path) => /^(?:dist\/[^/]+\.(?:js|d\.ts)|package\.json|README\.md|LICENSE-(?:MIT|APACHE))$/.test(path)),
    "Package must contain only compiled SDK, declarations, the reference engine and package documentation");
  assert.ok(paths.every((path) => !path.includes(".test.")), "Tests must not ship");
  await writeFile(join(temporary, "package.json"), JSON.stringify({ private: true, type: "module" }));
  run(npm, ["install", "--ignore-scripts", "--no-audit", "--no-fund", join(temporary, packed.filename),
    `typescript@${manifest.devDependencies.typescript}`, `@types/node@${manifest.devDependencies["@types/node"]}`], temporary);
  await writeFile(join(temporary, "app.ts"), `
import { collection, define, fail, key, keyVersion, mutation, participant, query, transaction, v } from "@flower-js/sdk";
import { scheduler } from "@flower-js/sdk/scheduler";
import { expiringCollection, queue } from "@flower-js/sdk/temporal";
import { nacl, jwt } from "@flower-js/sdk/crypto";
const sessions = key("sessions", { algorithm: "Ed25519", usages: ["sign", "verify"] });
const managed = query("managed", (_ctx, token: string) => jwt.verify(token, sessions).claims);
const values = collection<number>("numbers");
const indexed = collection<{ due: number }>("scheduled").index("due", ["due"]);
const page = query("page", (ctx) => ctx.range(indexed.by("due").range({ gte: 0, limit: 10 })));
const set = mutation("set", { args: v.number() }, (ctx, n) => {
  if (n < 0) fail("NEGATIVE", "n must not be negative", { n });
  ctx.set(values, "n", n);
  return n;
});
const get = query("get", (ctx) => ctx.get(values, "n"));
const transfer = transaction("transfer", (n: number) => ({ calls: [participant<{ set: typeof set }>({ partition: "tenant" }).call("set", n)] }));
const digest = query("digest", (_ctx, bytes: number[]) => Array.from(nacl.hash(new Uint8Array(bytes))));
const checkToken = query("checkToken", (_ctx, token: string) => jwt.verify(token, new Uint8Array(32), { algorithms: ["HS256"] }).claims);
const timers = scheduler("timers", { set });
const jobs = queue<{ n: number }, number>("jobs", { lease: { maxMs: 1000 } });
const cache = expiringCollection<string>("cache", { expiration: { afterUpdateMs: 60_000 } });
void keyVersion;
export default define({
  uses: [timers, jobs, cache],
  collections: [indexed],
  keys: [sessions],
  auth: { authenticate: (_ctx, credentials) => typeof credentials === "string" ? { subject: credentials } : null, default: "public" },
  http: { set, get, page, transfer, digest, checkToken, managed, ...jobs.http("jobs", { methods: ["enqueue", "claim", "renew", "complete", "fail", "ready"] }) },
});
`);
  await writeFile(join(temporary, "consumer.ts"), `
import { FlowerAdmin, FlowerClient, FlowerError, isTransient } from "@flower-js/sdk/client";
import type { QueryResult } from "@flower-js/sdk/client";
import { buildBundle } from "@flower-js/sdk/bundle";
import { createHttp2Transport } from "@flower-js/sdk/http2";
import { reconcile, runQueueWorker } from "@flower-js/sdk/worker";
import { testDatabase } from "@flower-js/sdk/testing";
import { FlowerClient as RootClient, nacl as rootNaCl, jwt as rootJWT } from "@flower-js/sdk";
import { nacl, jwt } from "@flower-js/sdk/crypto";
import type { JWTVerified, JWTSignOptions, NaClKeyPair, ManagedKey, ManagedJWTSignOptions, SharedKey } from "@flower-js/sdk/crypto";
import app from "./app.js";
const transport = createHttp2Transport();
const client = new FlowerClient<typeof app>("http://localhost:7101", { fetch: transport.fetch, retry: { attempts: 3 } });
const admin = new FlowerAdmin("http://localhost:7101", { fetch: transport.fetch, adminToken: "operator" });
const bundle = await buildBundle("app.ts");
if (typeof client.query !== "function" || app.http.get.kind !== "query" || app.http["jobs.claim"].kind !== "mutation" ||
    !bundle.javascript.startsWith("/* flower:static-init */") || !bundle.javascript.includes("__flowerBundle")) throw new Error("Broken installed SDK");
if (rootNaCl !== nacl || rootJWT !== jwt || RootClient !== FlowerClient || nacl.box.after !== nacl.secretbox || nacl.sign.signatureLength !== 64) throw new Error("Broken root exports");
if (typeof client.openRetrySession !== "function" || typeof client.sessionRequestId !== "function" || typeof client.refreshRetryIdentity !== "function" ||
    typeof client.subscribe !== "function" || typeof client.waitUntil !== "function") throw new Error("Broken client exports");
if (app.keys?.[0].name !== "sessions" || typeof admin.keyImport !== "function" || typeof admin.keyCacheStats !== "function" ||
    typeof admin.partition("tenant").deploy !== "function" || "deploy" in client) throw new Error("Broken operator exports");
if (typeof runQueueWorker !== "function" || typeof reconcile !== "function" || !isTransient(new FlowerError("down", 503, "UNAVAILABLE"))) throw new Error("Broken worker exports");
const typed = async (signal: AbortSignal) => {
  const current: QueryResult<number | null> = await client.query("get");
  await client.mutate("set", 3, { retry: true });
  await client.mutate("jobs.enqueue", { id: "job-1", payload: { n: 1 } });
  // @ts-expect-error set is a mutation
  await client.query("set", 3);
  // @ts-expect-error set requires a number
  await client.mutate("set", "3");
  for await (const update of client.subscribe("get", null, { signal })) if (update.reset && update.value === current.value) break;
  await runQueueWorker<{ n: number }, number>(client, { queue: "jobs", signal, leaseMs: 1000, work: async (job) => job.payload.n * 2 });
};
void typed;
const managedOptions: ManagedJWTSignOptions = { typ: "JWT" };
const useManaged = (key: ManagedKey, shared: SharedKey) => {
  jwt.sign({ exp: 123 }, key, managedOptions);
  jwt.verify("token", key);
  return nacl.box.after(new Uint8Array(), new Uint8Array(24), shared);
};
void useManaged;
const signingOptions: JWTSignOptions = { algorithm: "ES256", keyFormat: "der" };
const verifyShape = (verified: JWTVerified<{ sub: string }>, pair: NaClKeyPair) => verified.claims.sub + pair.publicKey.length;
void signingOptions; void verifyShape;
let unavailable = false;
try { nacl.hash(new Uint8Array()); } catch (error) { unavailable = error instanceof Error && /only inside a Flower method/.test(error.message); }
if (!unavailable) throw new Error("Native crypto must fail clearly outside Flower");
const db = await testDatabase(app);
if (db.mutate("set", 4) !== 4 || db.query("get") !== 4 || (await db.client.query("get")).value !== 4) throw new Error("Broken testing subpath");
let failed: unknown;
try { db.mutate("set", -1); } catch (error) { failed = error; }
if (!(failed instanceof FlowerError) || failed.failure?.code !== "NEGATIVE") throw new Error("Testing failures must match HTTP failures");
await transport.close();
`);
  await writeFile(join(temporary, "tsconfig.json"), JSON.stringify({ compilerOptions: {
    target: "ES2023", module: "NodeNext", moduleResolution: "NodeNext", strict: true,
    outDir: "out", types: ["node"],
  }, include: ["*.ts"] }));
  run(process.execPath, [join(temporary, "node_modules/typescript/bin/tsc"), "-p", "tsconfig.json"], temporary);
  run(process.execPath, ["out/consumer.js"], temporary);
  const cli = resolve(temporary, "node_modules/.bin/flower");
  assert.match(run(cli, ["--help"], temporary), /Usage: flower COMMAND/);
  run(cli, ["build", "app.ts", "application.flower.json"], temporary);
  const bundle = JSON.parse(await readFile(join(temporary, "application.flower.json"), "utf8"));
  assert.match(bundle.hash, /^[0-9a-f]{64}$/);
  assert.ok(bundle.javascript.startsWith("/* flower:static-init */\n"), "Bundles use static initialization by default");
  assert.ok(bundle.javascript.includes("__flowerBundle"));
  run(cli, ["build", "app.ts", "per-invocation.flower.json", "--initialization", "per-invocation"], temporary);
  const perInvocation = JSON.parse(await readFile(join(temporary, "per-invocation.flower.json"), "utf8"));
  assert.equal("/* flower:static-init */\n" + perInvocation.javascript, bundle.javascript);
  console.log(`Verified ${packed.filename}: ${paths.length} files, consumer types/imports, testing engine, module bundles and installed CLI.`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}
