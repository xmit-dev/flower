import { FlowerClient } from "@flower-js/sdk";
import type { Json } from "@flower-js/sdk";
import type { Claim } from "@flower-js/sdk/temporal";
import { runQueueWorker } from "@flower-js/sdk/worker";
import { hostname } from "node:os";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";

// A worker for the queue in examples/workers.ts. Start as many copies as you
// like, on as many machines as you like: they share the queue, and when one
// dies, the others finish its jobs once its leases run out.

// Your job goes here. It can run more than once (a worker can die after doing
// the work but before reporting it), so pass job.id to external services as an
// idempotency key. Stop when `signal` aborts: the lease is about to end.
export async function work(job: Claim, signal: AbortSignal): Promise<Json> {
  await sleep(1_000 + Math.random() * 2_000, undefined, { signal }); // Pretend to call an API.
  return { handledBy: job.owner, attempt: job.attempt };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const stop = new AbortController();
  for (const name of ["SIGINT", "SIGTERM"] as const) {
    process.on(name, () => {
      if (stop.signal.aborted) process.exit(130); // Second signal: quit now; leases expire and others take over.
      console.log("Stopping: finishing held jobs. Press Ctrl+C again to quit now.");
      stop.abort();
    });
  }
  const time = () => new Date().toTimeString().slice(0, 8);
  await runQueueWorker(new FlowerClient(process.env.FLOWER_URL ?? "http://127.0.0.1:7101"), {
    queue: "jobs",
    work,
    signal: stop.signal,
    owner: process.env.WORKER_ID ?? `${hostname()}-${process.pid}`,
    // A number fixes how many jobs run at once; by default it follows the backlog and this process's load.
    concurrency: process.env.WORKER_CONCURRENCY ? Number(process.env.WORKER_CONCURRENCY) : { min: 1, max: 16 },
    // Wait in the queue's line, so a new job wakes one worker rather than all of them.
    wait: true,
    leaseMs: Number(process.env.WORKER_LEASE_MS ?? 10_000),
    onEvent: (event) => console.log(time(), event.type, "job" in event ? event.job.id : "id" in event ? event.id : "limit" in event ? `${event.limit} (${event.reason})` : event.error),
  });
}
