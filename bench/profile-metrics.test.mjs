import assert from "node:assert/strict";
import test from "node:test";
import { parseProfileLine, summary } from "./profile-metrics.mjs";

test("empty and invalid timings remain unavailable instead of reporting zero", () => {
  assert.deepEqual(summary([]), { count: 0, invalid: 0, mean: null, p50: null, p95: null, p99: null, max: null });
  assert.deepEqual(summary([0, 1, NaN, undefined, -1, Infinity]), {
    count: 2, invalid: 4, mean: 0.5, p50: 0, p95: 1, p99: 1, max: 1,
  });
});

test("parser distinguishes durable group records from response and replay timings", () => {
  const response = parseProfileLine('\x1b[32mDEBUG\x1b[0m flower::service::writer: mutation timing method="pizza.tip" batch_commands=2 writer_wait_us=300 read_us=10 evaluation_us=1000 commit_us=500 duplicate=false');
  assert.deepEqual(response, { type: "server", value: {
    method: "pizza.tip", batch_commands: 2, writer_wait_us: 300, read_us: 10,
    evaluation_us: 1000, commit_us: 500,
  } });
  const group = parseProfileLine('DEBUG flower::service::writer: mutation group timing group_requests=3 group_commands=2 group_duplicates=1 group_errors=0 group_deferred=4 group_bytes=1024 read_us=10 prepare_us=2000 commit_us=500 group_us=2510 group_committed=true');
  assert.deepEqual(group, { type: "groups", value: {
    group_requests: 3, group_commands: 2, group_duplicates: 1, group_errors: 0,
    group_deferred: 4, group_bytes: 1024, read_us: 10, prepare_us: 2000,
    commit_us: 500, group_us: 2510, group_committed: true,
  } });
  assert.deepEqual(parseProfileLine('mutation timing method=pizza.tip duplicate=true batch_commands=0 evaluation_us=0 commit_us=0'), {
    type: "server", value: { method: "pizza.tip.replay", batch_commands: 0, evaluation_us: 0, commit_us: 0 },
  });
});

test("evaluator and group stages discard nonfinite data without filling missing measurements", () => {
  assert.deepEqual(parseProfileLine('mutation group timing group_commands=4 commit_us=NaN group_committed=false'), {
    type: "groups", value: { group_commands: 4, group_committed: false },
  });
  assert.deepEqual(parseProfileLine('evaluator wall-clock stages; nested cell sums overlap mode=mutation name=internal.pizza.tip total_ms=1.5 cell_count=3 max_cell_nesting=2 state_keys=40 cell_initial_bytes_sum=200 cell_reset_bytes_sum=64 bogus_bytes_sum=-1 bogus_ms=Infinity'), {
    type: "evaluator", value: { mode: "mutation", name: "internal.pizza.tip", total_ms: 1.5,
      cell_count: 3, max_cell_nesting: 2, state_keys: 40,
      cell_initial_bytes_sum: 200, cell_reset_bytes_sum: 64 },
  });
  assert.equal(parseProfileLine("unrelated service message"), null);
  assert.equal(parseProfileLine('mutation timing duplicate=false'), null);
  assert.equal(parseProfileLine('evaluator wall-clock stages mode=query'), null);
});

test("adaptive batch decisions retain pressure, stop reasons, and early drain", () => {
  assert.deepEqual(parseProfileLine('mutation group timing batch_mode="adaptive" batch_reason="queue_pressure" batch_stop="predecessor_completed" batch_target_count=301 batch_target_us=24800 batch_queued=73 group_commands=240 group_early_drain=1 group_committed=true'), {
    type: "groups", value: { batch_mode: "adaptive", batch_reason: "queue_pressure",
      batch_stop: "predecessor_completed", batch_target_count: 301, batch_target_us: 24800,
      batch_queued: 73, group_commands: 240, group_early_drain: 1, group_committed: true },
  });
});

test("preparation diagnostics retain speculative candidates and serial worker batches", () => {
  assert.deepEqual(parseProfileLine('mutation group timing group_commands=7 speculative_candidates=12 speculative_reused=5 serial_worker_jobs=2 serial_worker_requests=14 group_committed=true'), {
    type: "groups", value: { group_commands: 7, speculative_candidates: 12,
      speculative_reused: 5, serial_worker_jobs: 2, serial_worker_requests: 14, group_committed: true },
  });
  assert.deepEqual(parseProfileLine('mutation group timing speculative_candidates=NaN speculative_reused=-1 serial_worker_jobs=Infinity serial_worker_requests=-4'), {
    type: "groups", value: {},
  });
});

test("quoted method names decode escapes and malformed log text remains parseable", () => {
  assert.equal(parseProfileLine('mutation timing method="a\\\"b"').value.method, 'a"b');
  assert.equal(parseProfileLine('mutation timing method="bad\\x"').value.method, 'bad\\x');
});
