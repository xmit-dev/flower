import type { Writable } from "node:stream";

/** Keep a slow output pipe from pulling the next watch value indefinitely. */
export async function writeWatchOutput(output: Writable, text: string, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  if (output.write(text)) return;
  await new Promise<void>((resolve, reject) => {
    const cleanup = () => {
      output.removeListener("drain", drained);
      output.removeListener("error", failed);
      output.removeListener("close", closed);
      signal.removeEventListener("abort", aborted);
    };
    const drained = () => { cleanup(); resolve(); };
    const failed = (error: unknown) => { cleanup(); reject(error); };
    const closed = () => failed(new Error("Watch output closed before it drained"));
    const aborted = () => failed(signal.reason);
    output.once("drain", drained);
    output.once("error", failed);
    output.once("close", closed);
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) aborted();
    else if (output.destroyed || output.writableEnded) closed();
    else if (!output.writableNeedDrain) drained();
  });
}
