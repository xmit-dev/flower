import assert from "node:assert/strict";
import { Writable } from "node:stream";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import { writeWatchOutput } from "./cli-output.ts";

test("watch output pauses source consumption until the slow pipe drains", async () => {
  const writes: string[] = [];
  const pending: (() => void)[] = [];
  const output = new Writable({ highWaterMark: 1, write(chunk, _encoding, callback) {
    writes.push(chunk.toString()); pending.push(callback);
  } });
  let consumed = 0;
  const source = (async function* () { for (const value of [1, 2]) { consumed++; yield value; } })();
  const copying = (async () => {
    for await (const value of source) await writeWatchOutput(output, `${value}\n`, new AbortController().signal);
  })();
  await setImmediate();
  assert.equal(consumed, 1);
  assert.deepEqual(writes, ["1\n"]);
  pending.shift()!();
  await setImmediate();
  assert.equal(consumed, 2);
  assert.deepEqual(writes, ["1\n", "2\n"]);
  pending.shift()!();
  await copying;
  assert.equal(output.listenerCount("drain"), 0);
  assert.equal(output.listenerCount("error"), 0);
  output.destroy();
});

test("abort interrupts a blocked output pipe and closes the source iterator", async () => {
  const output = new Writable({ highWaterMark: 1, write() {} });
  const controller = new AbortController();
  let closed = false, consumed = 0;
  const source = (async function* () { try { while (true) { consumed++; yield 1; } } finally { closed = true; } })();
  const copying = (async () => {
    for await (const value of source) await writeWatchOutput(output, `${value}\n`, controller.signal);
  })();
  const reason = new Error("watch interrupted");
  const rejected = assert.rejects(copying, (error) => error === reason);
  await setImmediate();
  controller.abort(reason);
  await rejected;
  assert.equal(consumed, 1);
  assert.equal(closed, true);
  for (const event of ["drain", "error", "close"]) assert.equal(output.listenerCount(event), 0);
  output.destroy();
});

test("closed pipes fail without hanging, and pre-aborted writes enqueue nothing", async () => {
  let writes = 0;
  const output = new Writable({ highWaterMark: 1, write() { writes++; } });
  await assert.rejects(writeWatchOutput(output, "ignored", AbortSignal.abort()), { name: "AbortError" });
  assert.equal(writes, 0);
  const pending = writeWatchOutput(output, "one", new AbortController().signal);
  const rejected = assert.rejects(pending, /closed before it drained/);
  output.destroy();
  await rejected;
  assert.equal(writes, 1);
});
