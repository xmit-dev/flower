# flower-worker: deviations from the TypeScript SDK

The crate ports `sdk/worker.ts` and `sdk/capacity.ts` (`runQueueWorker`, `reconcile`, `Limiter`,
`processHealth`) and, for tests, `sdk/temporal.ts`'s queue and `sdk/external.ts`'s external
values. Behaviour, constants, validation messages, event order and the bytes of every argument
follow the TS; `tests/wire.rs` checks the arguments against goldens recorded from the TS worker
(`parity/wire.ts`). What differs, and why:

## Scheduling

- **One lock stands in for JavaScript's single thread.** Worker state lives behind one reentrant
  mutex; every step the TS runs between two `await`s runs under it, events included. On a
  multi-threaded runtime tasks interleave at await points as promises would, but not always in
  the same order as Node's microtask queue.
- **Two yields model JS ordering that the behaviour depends on.** A claimer yields once before
  sending a claim (the TS `send` awaits before `fetch`), so claimers woken together see each
  other probing. A finished job yields once before leaving `running` (the TS removes it in the
  task promise's `.finally`, a microtask later), so jobs its report chained in take their first
  step while it still counts as busy. The wire golden caught the second: a chained job that
  fails at once must not chain a claim of its own.
- **Dropping the future** of `run_queue_worker` or `reconcile` is an abrupt stop: claims,
  renewals and adjustments end at once; queue jobs already running keep going until they finish
  or their leases run out, then report. There is no TS equivalent (a promise cannot be dropped).
  Cancel `signal` and await for the graceful path.
- **Event handlers run under the worker's lock**, synchronously and in order, like `onEvent`.
  Keep them quick; they may call `JobControl` (the lock is reentrant) but must not block.

## Types instead of runtime checks

- Concurrency bounds, `batch`, `claimers`, `lease_ms`, `margin_ms`, `drain_ms`, `wait_ms` are
  unsigned integers, so fractional and negative values cannot be written. Zero, ordering and
  `Number.MAX_SAFE_INTEGER` checks remain, with the TS messages, as `WorkerError::Invalid`
  (the TS throws a `TypeError`).
- **Payloads and inputs are typed.** A claim whose payload does not deserialize into `P` fails
  that attempt with `"The payload cannot be read: <serde error>"`; a reconcile input or args that
  do not deserialize count as a failed computation (`"The input cannot be read: …"`, then the
  usual backoff or release).
- **Results** are `Serialize`. `storable()` can only fail on non-finite numbers
  (`"The result cannot be stored: Flower values require finite numbers"`) and nesting deeper
  than 128: Rust values have no `undefined`, functions, cycles or hidden properties. Integers
  beyond 2^53 are sent exactly, where JS would already have rounded them (Flower reads numbers as
  doubles).
- **Panics** in `work` or `compute` fail the attempt with the panic message, as a thrown error
  would; the worker keeps running.
- **Replies that do not parse** (a claim that is not a claim) end the run with
  `WorkerError::Protocol`; the TS would throw wherever the bad value was first used.
- `RetryPolicy` has no `retryable`: calls always retry on `is_transient()`, the TS default.

## Errors

- A job's failure text is `WorkError`'s message. `?` on any `std::error::Error` converts with
  its `Display`, except `flower_client::FlowerError`, described as `worker.ts`'s `message()` does
  (`"<failure.code>: <failure.message>"`, `"<code>: <message>"`, the Node code for transport
  errors). JS errors carrying `cause.code` have no other Rust counterpart.
- `JobStop::reason()` is the abort reason's message; a stop inherited from the worker's signal
  (reconcile) reads `"This operation was aborted"`, Node's default.

## Clock and health

- `Clock::system()` (the default) is `Date.now()`. `Clock::tokio()` follows tokio's clock so
  tests run on paused time; with a real `FlowerClient` keep the system clock, since retry
  deadlines become wall-clock instants.
- `process_health()` reads the tokio runtime instead of Node's event loop: the share of time its
  worker threads were busy since the last read (`worker_total_busy_duration`, summed over workers,
  over wall time × workers), and memory as resident size against what the process may still use
  (cgroup v2 `memory.max − memory.current`, else `MemAvailable`; free pages on macOS). There is no
  heap limit to read, so `"heap N% full"` never appears. The busy reason keeps the TS wording,
  `"event loop N% busy"`, so logs and dashboards read alike. Worker threads hand in busy time
  when they park, so a read can lag a burst by a few milliseconds. Created outside a runtime, it
  reads memory only.

## reconcile

- An unleased pool round ends at its first error like `Promise.all`, and the other lanes are
  dropped at their current await (the TS aborts their signal and leaves them to notice).
- The leased pool's readiness promise is reset only by the watch that failed (the TS resets
  whichever promise is current at that moment, possibly a newer one).

## The in-memory Flower (`testing` feature)

- `FakeFlower` implements the generated methods of `queue.http()` (`sdk/temporal.ts`, including
  lines, turns, groups, fencing, retries, history and `next`) for the queues added to it, and
  `docs/reactive-worker.ts`'s documents and `digest` external value. It checks arguments
  strictly, like `v.object`, and answers errors with Flower's status, code and failure.
- It computes effective state on read and runs no maintenance task: raw storage (what the TS
  tests read with `db.data`) is not exposed. Watches re-evaluate on every write or `advance`.
- Request IDs are `request-<n>`, unique across clients; a repeated ID gets the first answer, and
  `FakeFlower::replays()` lists those (`duplicate: true`).
