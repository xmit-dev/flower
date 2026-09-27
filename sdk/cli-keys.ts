import { readFile } from "node:fs/promises";
import type { FlowerAdmin, SealedKeyImport } from "./client.ts";
import { key } from "./keys.ts";
import type { ManagedKeyAlgorithm, KeyUsage } from "./keys.ts";

/** Imports accept sealed JSON only. Plaintext stdin belongs to native key seal. */
export async function runKeyCommand(client: FlowerAdmin, args: string[], flags: Map<string, string>): Promise<unknown> {
  const [operation, ...operands] = args;
  const allowed: Record<string, string[]> = {
    list: [], cache: [], generate: ["algorithm", "bits"], import: ["algorithm"],
    bind: ["usages"], unbind: [], rotate: ["bits"], revoke: ["version"], retire:["version"], destroy:["version"], rewrap:["version"],
  };
  if (operation === "seal") throw new TypeError("Run key seal with the native Flower executable; this SDK CLI imports only its encrypted JSON output");
  if (!Object.hasOwn(allowed, operation ?? "")) throw new TypeError("key requires list, cache, generate, import, bind, unbind, rotate, revoke, retire, destroy, or rewrap");
  for (const name of ["algorithm", "usages", "bits", "version"]) {
    if (flags.has(name) && !allowed[operation].includes(name)) throw new TypeError(`--${name} does not apply to key ${operation}`);
  }
  if (flags.has("expected-revision")) throw new TypeError("--expected-revision does not apply to key commands");
  const counts: Record<string, number> = { list: 0, cache: 0, generate: 1, import: 2, bind: 2, unbind: 1, rotate: 1, revoke: 1, retire:1, destroy:1, rewrap:1 };
  if (operands.length !== counts[operation]) throw new TypeError(`Wrong number of arguments for key ${operation}; use --help`);
  const options = flags.has("request-id") ? { requestId: flags.get("request-id")! } : {};
  const number = (name: string): number | undefined => {
    const raw = flags.get(name);
    if (raw === undefined) return undefined;
    const value = Number(raw);
    if (!/^[0-9]+$/.test(raw) || !Number.isSafeInteger(value) || value < 1) throw new TypeError(`--${name} must be a positive safe integer`);
    return value;
  };
  const algorithm = (): ManagedKeyAlgorithm => {
    const value = flags.get("algorithm");
    if (value === undefined) throw new TypeError(`key ${operation} requires --algorithm`);
    return key("cli", { algorithm: value as ManagedKeyAlgorithm, usages: ["publicKey"] }).algorithm;
  };
  switch (operation) {
    case "list": return client.keyList();
    case "cache": return client.keyCacheStats();
    case "generate": return client.keyGenerate(operands[0], algorithm(), { ...options, ...(flags.has("bits") ? { bits: number("bits") } : {}) });
    case "import": {
      const selected = algorithm();
      let encoded: string;
      if (operands[1] === "-") {
        const chunks: Buffer[] = [];
        for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk));
        encoded = Buffer.concat(chunks).toString("utf8");
      } else encoded = await readFile(operands[1], "utf8");
      return client.keyImport(operands[0], selected, JSON.parse(encoded) as SealedKeyImport, options);
    }
    case "bind": {
      const raw = flags.get("usages");
      if (raw === undefined) throw new TypeError("key bind requires --usages sign,verify,...");
      const usages = key("cli", { algorithm: "Ed25519", usages: raw.split(",") as KeyUsage[] }).usages;
      return client.keyBind(operands[0], operands[1], usages, options);
    }
    case "unbind": return client.keyUnbind(operands[0], options);
    case "rotate": return client.keyRotate(operands[0], { ...options, ...(flags.has("bits") ? { bits: number("bits") } : {}) });
    case "revoke": return client.keyRevoke(operands[0], { ...options, ...(flags.has("version") ? { version: number("version") } : {}) });
    case "retire": return client.keyRetire(operands[0], { ...options, ...(flags.has("version") ? { version: number("version") } : {}) });
    case "destroy": return client.keyDestroy(operands[0], { ...options, ...(flags.has("version") ? { version: number("version") } : {}) });
    case "rewrap": return client.keyRewrap(operands[0], { ...options, ...(flags.has("version") ? { version: number("version") } : {}) });
  }
}
