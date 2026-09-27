import { FlowerClient } from "@flower-js/sdk";
import { reconcile } from "@flower-js/sdk/worker";
import { webcrypto } from "node:crypto";
import { fileURLToPath } from "node:url";
import type app from "./reactive-worker.ts";

type Input = { recipe: string; text: string };

// Keep every document's digest current, or only one when id is given. Several
// processes may run at once: without an id they lease documents, so each digest
// is computed once; either way, publication keeps the first result for an input.
export async function runWorker(client: FlowerClient<typeof app>, signal: AbortSignal, id?: string) {
  await reconcile<string, Input, string>(client, {
    external: "digest",
    ...(id === undefined ? { lease: true } : { args: id }),
    signal,
    async compute(input) {
      if (input.recipe !== "sha256-v1") throw new Error("Unsupported worker recipe");
      const digest = await webcrypto.subtle.digest("SHA-256", new TextEncoder().encode(input.text));
      return Buffer.from(digest).toString("hex");
    },
    onEvent: ({ type, ...detail }) => console.log(type, detail),
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const shutdown = new AbortController();
  process.once("SIGINT", () => shutdown.abort());
  process.once("SIGTERM", () => shutdown.abort());
  await runWorker(new FlowerClient(process.env.FLOWER_URL ?? "http://127.0.0.1:7101"), shutdown.signal, process.argv[2]);
}
