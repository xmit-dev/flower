import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";
import { main } from "./cli.ts";
import { FlowerClient } from "./client.ts";
import type { WatchOptions } from "./client.ts";

const flags = ["max-event-bytes", "max-value-bytes", "max-patch-operations"] as const;

test("CLI validates deployment preparation before loading a bundle", async () => {
  await assert.rejects(main(["deploy","missing.ts","--preparation","invalid"]),/--preparation must be online or blocking/);
  await assert.rejects(main(["query","read","--preparation","blocking"]),/--preparation applies only to deploy/);
});

test("CLI watch forwards all local allowances, JSON arguments and its abort signal", async (t) => {
  const calls: { name: string; args: unknown; options: WatchOptions }[] = [];
  t.mock.method(FlowerClient.prototype, "watch", function (name: string, args: unknown, options: WatchOptions) {
    calls.push({ name, args, options });
    return (async function* () {})();
  });
  await main(["watch", "pizza.board", '{"id":"x"}', "--max-event-bytes=34603008", "--max-value-bytes", "33554432", "--max-patch-operations", "512"]);
  const received = calls[0];
  assert.equal(received?.name, "pizza.board");
  assert.deepEqual(received?.args, { id: "x" });
  assert.deepEqual({ ...received?.options, signal: undefined }, { maxEventBytes: 34603008, maxValueBytes: 33554432, maxPatchOperations: 512, signal: undefined });
  assert.ok(received?.options.signal instanceof AbortSignal);
  await main(["watch", "pizza.board"]);
  assert.deepEqual(calls[1].args, null);
  assert.deepEqual(Object.keys(calls[1].options), ["signal"]);
});

test("CLI rejects malformed or misplaced watch allowances before network work", async () => {
  for (const flag of flags) {
    for (const value of ["0", "-1", "1.5", "NaN", "Infinity", "9007199254740992", "1e3", "0x10", "+1", " 1"]) {
      await assert.rejects(main(["watch", "pizza.board", `--${flag}`, value]), new RegExp(`--${flag} must be a positive safe integer`));
    }
    for (const command of ["build", "deploy", "init", "call", "mutate", "query"]) {
      await assert.rejects(main([command, `--${flag}`, "1"]), new RegExp(`--${flag} applies only to watch`));
    }
    await assert.rejects(main(["watch", "pizza.board", `--${flag}`]), /requires a value/);
    await assert.rejects(main(["watch", "pizza.board", `--${flag}`, "1", `--${flag}=2`]), /specified twice/);
  }
});

test("CLI help describes watch allowances, replica reconnection and the static initialization default", async () => {
  const { stdout, stderr } = await promisify(execFile)(process.execPath, ["sdk/cli.ts", "--help"], { cwd: new URL("..", import.meta.url), timeout: 5000 });
  assert.equal(stderr, "");
  for (const flag of flags) assert.match(stdout, new RegExp(`--${flag} N\\s+watch only`));
  assert.match(stdout, /positive safe integers.*never sent to the server/);
  assert.match(stdout, /reachable replica/);
  assert.match(stdout, /--initialization MODE\s+build\/deploy \.ts: static \(default\) or per-invocation/);
});

test("CLI validates the initialization mode and where it applies", async () => {
  await assert.rejects(main(["build", "app.ts", "--initialization", "lazy"]), /--initialization must be per-invocation or static/);
  await assert.rejects(main(["query", "read", "--initialization", "static"]), /--initialization applies only to build\/deploy/);
});
