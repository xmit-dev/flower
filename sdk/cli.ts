#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { realpathSync } from "node:fs";
import { basename, extname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { FlowerAdmin, FlowerClient, FlowerError } from "./client.ts";
import type { WatchOptions } from "./client.ts";
import { loadBundle, writeBundle, type BuildOptions } from "./bundle.ts";
import { writeWatchOutput } from "./cli-output.ts";
import { runKeyCommand } from "./cli-keys.ts";

const help = `Flower — transactional reactive values

Usage: flower COMMAND [ARGS] [OPTIONS]

  build FILE [OUTPUT]         Bundle a TypeScript module to <basename>.flower.json.
  deploy FILE                Deploy a .ts module, .wasm guest module or built .json bundle (admin).
  init --members ID=ADDR,...  Bootstrap an uninitialized Raft cluster once (admin).
  call NAME [JSON_ARGS]       Invoke an HTTP alias; deployed code selects its mode.
  mutate NAME [JSON_ARGS]     Invoke a named atomic mutation method.
  query NAME [JSON_ARGS]      Invoke a named read-only query method.
  watch NAME [JSON_ARGS]      Watch a query over SSE; print reconstructed values.
  key list                    List managed-key metadata (admin).
  key cache                   Inspect this physical node's prepared-key cache (admin).
  key generate NAME           Generate a native key; requires --algorithm (admin).
  key import NAME FILE        Import sealed JSON; FILE '-' reads encrypted stdin (admin).
  key bind ALIAS NAME          Approve a declaration alias; requires --usages (admin).
  key unbind ALIAS             Remove a key binding (admin).
  key rotate NAME              Generate the next immutable version (admin).
  key revoke NAME              Revoke all versions or --version N (admin).
  key retire NAME              Keep only verify/decrypt/public-key uses (admin).
  key destroy NAME             Remove current encrypted material, not backups (admin).
  key rewrap NAME              Rewrap selected versions with the mounted current key (admin).

JSON_ARGS defaults to null. Use @FILE to read arguments from a JSON file.

Options:
  --url URL                  Server URL (FLOWER_URL or http://127.0.0.1:7101).
  --admin-token TOKEN        Shared admin token (or FLOWER_ADMIN_TOKEN).
  --request-id ID            Idempotency key for call/mutate/deploy/key changes; preserve for retries.
  --credentials JSON         Credentials for call/mutate/query/watch (or FLOWER_CREDENTIALS); @FILE reads a file.
  --partition NAME           Address a named database for methods, deploy or keys.
  --algorithm NAME           key generate/import: Ed25519/P256/RSA/HS256/A256GCM/XSalsa20Poly1305/X25519.
  --usages LIST              key bind: comma-separated sign,verify,encrypt,decrypt,derive,publicKey.
  --bits N                   key generate/rotate: RSA modulus size.
  --version N                key revoke/retire/destroy/rewrap: one version; omit for all.
  --expected-revision N      Require this application revision for call/mutate.
  --initialization MODE      build/deploy .ts: static (default) or per-invocation.
  --preparation MODE         deploy: online (default), or blocking for a busy database.
  --max-event-bytes N         watch only: local SSE event allowance (default 17825792).
  --max-value-bytes N         watch only: reconstructed value allowance (default 16777216).
  --max-patch-operations N    watch only: operations per patch (default 256).
  --help                    Show this help.

Examples:
  flower init --members 1=127.0.0.1:7101
  flower deploy examples/orders.ts
  flower call order.create @examples/orders.create.json
  flower query order.get '\"order-42\"'
  flower mutate order.updateLine @examples/orders.update.json
  flower key generate session-key --algorithm Ed25519 --request-id create-session-key
  flower key bind sessions session-key --usages sign,verify --request-id bind-sessions

Modules export define({ uses: [...], http: { alias: method } }).
Only aliases in the HTTP table are callable. Other definitions remain private.
watch receives a snapshot and JSON patches; it may coalesce intermediate revisions.
Reconnect to a reachable replica serving the query policy; each stream starts fresh.
Watch allowances are positive safe integers and are never sent to the server.
Key mutations also use --request-id; retain it for retries. Imports never upload raw
key bytes: use the native Flower executable's offline key seal command first:
  /path/to/native/flower key seal --wrapping-key-file /secure/wrapping.key --format pem < private.pem > sealed.json
  flower key import session-key sealed.json --algorithm Ed25519 --request-id import-session-key
`;

function parseArguments(args: string[]) {
  const positional: string[] = [];
  const options = new Map<string, string>();
  let wantsHelp = false;
  const names = new Set(["url", "members", "request-id", "credentials", "admin-token", "expected-revision", "initialization", "preparation", "max-event-bytes", "max-value-bytes", "max-patch-operations", "partition", "algorithm", "usages", "bits", "version"]);
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--help" || arg === "-h") { wantsHelp = true; continue; }
    if (!arg.startsWith("--")) { positional.push(arg); continue; }
    const equal = arg.indexOf("=");
    const name = arg.slice(2, equal < 0 ? undefined : equal);
    if (!names.has(name)) throw new TypeError(`Unknown option --${name}`);
    const value = equal < 0 ? args[++i] : arg.slice(equal + 1);
    if (!value || value.startsWith("--")) throw new TypeError(`--${name} requires a value`);
    if (options.has(name)) throw new TypeError(`--${name} was specified twice`);
    options.set(name, value);
  }
  return { positional, options, help: wantsHelp };
}

function print(value: unknown): void { process.stdout.write(JSON.stringify(value, null, 2) + "\n"); }

function membersFrom(value: string): Record<string, string> {
  const result: Record<string, string> = Object.create(null);
  for (const part of value.split(",")) {
    const equal = part.indexOf("=");
    const id = part.slice(0, equal);
    const address = part.slice(equal + 1);
    if (equal < 1 || !/^[1-9][0-9]*$/.test(id) || !address || address.includes("://")) {
      throw new TypeError("Members must be ID=host:port pairs, separated by commas");
    }
    if (Object.hasOwn(result, id)) throw new TypeError(`Duplicate member ${id}`);
    result[id] = address;
  }
  return result;
}

function requireArgs(args: string[], min: number, max = min): void {
  if (args.length < min || args.length > max) throw new TypeError("Wrong number of arguments; use --help");
}

async function methodArguments(raw = "null"): Promise<any> {
  return JSON.parse(raw.startsWith("@") ? await readFile(raw.slice(1), "utf8") : raw);
}

export async function main(args = process.argv.slice(2)): Promise<void> {
  const parsed = parseArguments(args);
  const [command, ...operands] = parsed.positional;
  if (parsed.help || !command) { process.stdout.write(help); return; }
  for (const name of ["algorithm", "usages", "bits", "version"]) {
    if (parsed.options.has(name) && command !== "key") throw new TypeError(`--${name} applies only to key commands`);
  }
  if (parsed.options.has("partition") && ["build", "init"].includes(command)) throw new TypeError("--partition applies to methods, deploy and key commands");
  if (parsed.options.has("preparation") && command !== "deploy") throw new TypeError("--preparation applies only to deploy");
  const preparation = parsed.options.get("preparation");
  if (preparation !== undefined && preparation !== "online" && preparation !== "blocking") throw new TypeError("--preparation must be online or blocking");
  const buildOptions: BuildOptions = {};
  if (parsed.options.has("initialization")) {
    if (!["build", "deploy"].includes(command)) throw new TypeError("--initialization applies only to build/deploy");
    const initialization = parsed.options.get("initialization");
    if (initialization !== "static" && initialization !== "per-invocation") throw new TypeError("--initialization must be per-invocation or static");
    buildOptions.initialization = initialization;
  }
  const watchOptions: WatchOptions = {};
  for (const [flag, option] of [
    ["max-event-bytes", "maxEventBytes"],
    ["max-value-bytes", "maxValueBytes"],
    ["max-patch-operations", "maxPatchOperations"],
  ] as const) {
    const raw = parsed.options.get(flag);
    if (raw === undefined) continue;
    if (command !== "watch") throw new TypeError(`--${flag} applies only to watch`);
    const value = Number(raw);
    if (!/^[0-9]+$/.test(raw) || !Number.isSafeInteger(value) || value < 1) {
      throw new TypeError(`--${flag} must be a positive safe integer`);
    }
    watchOptions[option] = value;
  }
  const url = parsed.options.get("url") ?? process.env.FLOWER_URL;
  const rawCredentials = parsed.options.get("credentials") ?? process.env.FLOWER_CREDENTIALS;
  if (parsed.options.has("credentials") && !["call", "mutate", "query", "watch"].includes(command)) throw new TypeError("--credentials applies to call, mutate, query and watch");
  let client = new FlowerClient(url, rawCredentials === undefined ? {} : { credentials: await methodArguments(rawCredentials) });
  let admin = new FlowerAdmin(url, { adminToken: parsed.options.get("admin-token") ?? process.env.FLOWER_ADMIN_TOKEN });
  if (parsed.options.has("partition")) {
    client = client.partition(parsed.options.get("partition")!);
    admin = admin.partition(parsed.options.get("partition")!);
  }
  const mutationOptions: { requestId?: string; expectedRevision?: number } = {};
  if (parsed.options.has("request-id")) mutationOptions.requestId = parsed.options.get("request-id");
  if (parsed.options.has("expected-revision")) {
    const revision = Number(parsed.options.get("expected-revision"));
    if (!Number.isSafeInteger(revision) || revision < 0) throw new TypeError("Expected revision must be a nonnegative integer");
    mutationOptions.expectedRevision = revision;
  }
  switch (command) {
    case "key":
      print(await runKeyCommand(admin, operands, parsed.options));
      break;
    case "build": {
      requireArgs(operands, 1, 2);
      const [entry, output = basename(operands[0], extname(operands[0])) + ".flower.json"] = operands;
      const bundle = await writeBundle(entry, output, buildOptions);
      print({ output: resolve(output), hash: bundle.hash });
      break;
    }
    case "deploy": {
      requireArgs(operands, 1);
      if (mutationOptions.expectedRevision !== undefined) throw new TypeError("--expected-revision applies only to call/mutate");
      const bundle = await loadBundle(operands[0], buildOptions);
      print({ ...await admin.deploy(bundle, { ...mutationOptions, preparation }), hash: bundle.hash });
      break;
    }
    case "init": {
      requireArgs(operands, 0);
      const members = parsed.options.get("members");
      if (!members) throw new TypeError("init requires --members ID=host:port,...");
      await admin.initialize(membersFrom(members));
      print({ initialized: true });
      break;
    }
    case "call":
      requireArgs(operands, 1, 2);
      print(await client.call(operands[0], await methodArguments(operands[1]), mutationOptions));
      break;
    case "mutate":
      requireArgs(operands, 1, 2);
      print(await client.mutate(operands[0], await methodArguments(operands[1]), mutationOptions));
      break;
    case "query":
      requireArgs(operands, 1, 2);
      print(await client.query(operands[0], await methodArguments(operands[1])));
      break;
    case "watch": {
      requireArgs(operands, 1, 2);
      const queryArgs = await methodArguments(operands[1]);
      const controller = new AbortController();
      const stop = () => controller.abort();
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);
      try {
        for await (const result of client.watch(operands[0], queryArgs, { ...watchOptions, signal: controller.signal })) {
          await writeWatchOutput(process.stdout, JSON.stringify(result) + "\n", controller.signal);
        }
      } catch (error) {
        if (!controller.signal.aborted) throw error;
      } finally {
        process.removeListener("SIGINT", stop);
        process.removeListener("SIGTERM", stop);
      }
      break;
    }
    default: throw new TypeError(`Unknown command ${JSON.stringify(command)}; use --help`);
  }
}

function isEntryPoint(): boolean {
  if (!process.argv[1]) return false;
  try { return import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href; }
  catch { return false; }
}

// npm's bin is a symlink on Unix; compare its real path to this module.
if (isEntryPoint()) {
  main().catch((error: unknown) => {
    if (error instanceof FlowerError && error.failure) {
      const details = error.failure.details === undefined ? "" : ` ${JSON.stringify(error.failure.details)}`;
      process.stderr.write(`flower: ${error.failure.code}: ${error.failure.message}${details}\n`);
    } else {
      const label = error instanceof FlowerError ? `${error.code}${error.status ? ` (${error.status})` : ""}: ` : "";
      process.stderr.write(`flower: ${label}${error instanceof Error ? error.message : String(error)}\n`);
    }
    process.exitCode = 1;
  });
}
