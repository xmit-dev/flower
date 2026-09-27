import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createProcessCleanup } from "./multi-group.mjs";

const error = (code) => Object.assign(new Error(code), { code });

test("cleanup falls back from denied groups, continues after denied PIDs, and escalates", async () => {
  const calls = [];
  const first = { pid: 1_000_001, exitCode: null, signalCode: null };
  const second = { pid: 1_000_002, exitCode: null, signalCode: null };
  const servers = new Set([1_000_003, 1_000_004]);
  const cleanup = createProcessCleanup({ graceMs: 10, pollMs: 1, platform: "linux", kill(pid, signal) {
    calls.push([pid, signal]);
    if (pid < 0) throw error("EPERM");
    if (signal === 0) { if (!servers.has(pid)) throw error("ESRCH"); return; }
    if (pid === 1_000_003 && signal === "SIGTERM") throw error("EPERM");
    if (signal === "SIGKILL") {
      if (pid === first.pid) first.signalCode = signal;
      if (pid === second.pid) second.signalCode = signal;
      servers.delete(pid);
    }
  } });
  cleanup.track(first)({ type: "spawn", pid: 1_000_003 });
  cleanup.track(second)({ type: "spawn", pid: 1_000_004 });
  assert.doesNotThrow(() => cleanup.stop());
  const errors = await cleanup.finish();
  for (const pid of [first.pid, second.pid, 1_000_003, 1_000_004]) {
    assert.ok(calls.some(([id, signal]) => id === pid && signal === "SIGTERM"));
    assert.ok(calls.some(([id, signal]) => id === pid && signal === "SIGKILL"));
  }
  assert.equal(errors.length, 1);
  assert.equal(errors[0].pid, 1_000_003);
  assert.equal(servers.size, 0);
});

test("cleanup tracks late servers and retires exited PIDs", async () => {
  const calls = [];
  const child = { pid: 1_000_001, exitCode: null, signalCode: null };
  const servers = new Set();
  const cleanup = createProcessCleanup({ graceMs: 10, pollMs: 1, platform: "win32", kill(pid, signal) {
    calls.push([pid, signal]);
    if (signal === 0) { if (!servers.has(pid)) throw error("ESRCH"); return; }
    if (pid === child.pid) child.signalCode = signal;
    else servers.delete(pid);
  } });
  const notify = cleanup.track(child);
  notify({ type: "spawn", pid: 1_000_002 });
  notify({ type: "exit", pid: 1_000_002 });
  cleanup.stop();
  servers.add(1_000_003);
  notify({ type: "spawn", pid: 1_000_003 });
  await cleanup.finish();
  assert.ok(calls.some(([pid, signal]) => pid === 1_000_003 && signal === "SIGTERM"));
  assert.ok(!calls.some(([pid]) => pid === 1_000_002));
});

test("real worker and server exit when group signaling is denied", { timeout: 5000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "flower-cleanup-test-"));
  const path = join(directory, "worker.cjs");
  await writeFile(path, `
    const { spawn } = require('node:child_process');
    process.on('SIGTERM', () => {});
    const server = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
    server.once('spawn', () => process.send({ pid: server.pid }));
    server.once('exit', () => process.send({ exited: server.pid }));
    setInterval(() => {}, 1000);
  `);
  const child = fork(path, [], { detached: process.platform !== "win32", stdio: ["ignore", "ignore", "ignore", "ipc"] });
  const exited = once(child, "exit");
  const [message] = await once(child, "message");
  t.after(async () => {
    for (const pid of [message.pid, child.pid]) { try { process.kill(pid, "SIGKILL"); } catch {} }
    await rm(directory, { recursive: true, force: true });
  });
  const calls = [];
  const cleanup = createProcessCleanup({ graceMs: 100, pollMs: 5, kill(pid, signal) {
    calls.push([pid, signal]);
    if (pid < 0) throw error("EPERM");
    return process.kill(pid, signal);
  } });
  const notify = cleanup.track(child);
  notify({ type: "spawn", pid: message.pid });
  child.on("message", (event) => { if (event.exited) notify({ type: "exit", pid: event.exited }); });
  assert.deepEqual(await cleanup.finish(), []);
  await exited;
  assert.ok(calls.some(([pid, signal]) => pid === message.pid && signal === "SIGTERM"));
  assert.ok(calls.some(([pid, signal]) => pid === child.pid && signal === "SIGKILL"));
  assert.throws(() => process.kill(message.pid, 0), { code: "ESRCH" });
});
