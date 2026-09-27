# flower-worker API

The Rust twin of `@flower-js/sdk/worker` (`sdk/worker.ts`, `sdk/capacity.ts`). Deviations:
`DEVIATIONS.md`.

```toml
flower-worker = { path = "../flower/crates/flower-worker" }   # default feature flower-client
```

## Queue workers

```rust
use flower_worker::{run_queue_worker, Claim, Concurrency, JobControl, JobStop, QueueWorkerOptions, WorkError};

let options = QueueWorkerOptions {
    concurrency: Some(Concurrency::adaptive(16, 64, 4_096)), // min, initial, max; or Fixed(n), up_to(initial, max)
    claimers: Some(4),
    batch: Some(32),
    wait: true, wait_ms: Some(300_000),                      // Trinity's LINE
    chain: true,                                             // Trinity's CHAIN
    lease_ms: Some(30_000),
    drain_ms: Some(30_000), release: true,
    ..QueueWorkerOptions::new("completions", signal.clone()) // queue prefix, CancellationToken
}
.on_event(|event| log(&event));                              // QueueWorkerEvent: Serialize, {"type": ...}
run_queue_worker(client, options, |job: Claim<CompletionJob>, stop: JobStop, control: JobControl| async move {
    // stop: lease ran out / lost / drain ended (stop.stopped().await, stop.reason(), stop.token())
    // control.idle(): stop counting toward concurrency; control.throttle(ms, reason): provider pushed back
    Ok::<_, WorkError>(outcome)                              // R: Serialize; Err(WorkError) or `?` fails the attempt
})
.await?;                                                     // Result<(), WorkerError<C::Error>>
```

- Options (all TS names in snake_case): `queue`, `signal`, `drain_ms`, `release`, `owner`,
  `concurrency`, `batch`, `claimers`, `chain`, `wait`, `wait_ms`, `lease_ms`, `renew` (default
  true), `margin_ms`, `scope` (for `queue.http(prefix, { scope: "argument" })`), `retry:
  RetryPolicy`, `health: Option<Arc<dyn Health>>`, `adjust_every_ms`, `on_event`, `clock`.
- `Claim<P> { attempt, expires_at, history, id, owner, payload, scope, token }` (`.map(f)`);
  `LeaseIdentity::from(&job)` gives `{ id, owner, token, history? }` for methods that take a lease
  (Trinity's flushes).
- `QueueWorkerEvent::{Claimed{job}, Completed{id}, Failed{id,error}, Lost{id}, Released{id},
  Unreported{id,error}, Limit{limit,reason}, Waiting{error}}`, `.kind()`.
- `WorkerError<E>::{Invalid(msg), Client(E), Protocol(msg)}`, `.client()`.

## reconcile

```rust
use flower_worker::{reconcile, ExternalWork, ReconcileOptions};

reconcile(client, ReconcileOptions {
    lease: true,
    concurrency: Some(Concurrency::adaptive(1, 4, 16)),
    ..ReconcileOptions::new("titles", signal.clone())
}.on_event(log), |input: TitleInput, work: ExternalWork<String, TitleInput>, stop: JobStop| async move {
    generate_title(&input.text).await.map_err(WorkError::from)
})
.await?;
```

- One key: `ReconcileOptions::new(..).with_args(&id)`; a pool through `next`: neither `args`
  nor `lease`, optionally `shard: Some((index, count))`, `batch`, `concurrency: Fixed(n)`.
- Lease mode: `lease`, `owner`, `lease_ms`, `margin_ms`, `batch`, `health`, `adjust_every_ms`,
  adaptive concurrency up to 1024.
- `ReconcileEvent::{Claimed{key,attempt}, Published{key,accepted}, Failed{key,error}, Lost{key},
  Limit{limit,reason}, Waiting{error}}`.

## Capacity

`Limiter` (`new`, `with_clock`, `with_now`, `limit()`, `pause()`, `want()`, `throttle(ms, reason)`,
`adjust()`, `fixed()`, `min`, `max`), `Concurrency`, `Adaptive`, `Health` (any `Fn() -> Load`),
`Load { load, reason }`, `idle_health()`, `process_health(HealthLimits { busy: 0.9, memory: 0.85 })`,
`default_process_health()`.

## Clients

- `trait QueueClient { type Error: ClientError; mutate(name, Box<RawValue>, RetryPolicy); wait_until(name, Box<RawValue>, Predicate) }`
  and `trait ClientError { status, code, failure_code, is_transient, describe }`.
- With the default `flower-client` feature both are implemented for `flower_client::FlowerClient`
  / `FlowerError`; `retry_policy()` maps the worker's policy onto the client's.
- `to_js_raw(&value)`: `JSON.stringify` bytes (field order kept, JS number spelling).
- `RetryPolicy { attempts, until, initial_delay_ms, max_delay_ms, timeout_ms }`, `backoff`,
  `truthy`, `non_empty_array`, `Clock::{system, tokio, tokio_at}`, `system_now_ms()`.

## Testing (`testing` feature)

`FakeFlower::{workers(clock), reactive(clock), new(clock)}` + `add_queue(prefix, QueueConfig)`,
`enqueue`, `job`, `mutate`, `query`, `call`, `advance`, `follow_clock`, `freeze`, `now`,
`set_history`, `replays`, `client()`; `FakeClient` (a `QueueClient`) with `before`/`after` hooks
to fail attempts or replace replies, `calls()`, `calls_to(name)`; `FakeError::{http, failure,
denied, fetch_failed}`; `shard_of`, `canonical_json`.

## Tests

```sh
cargo test -p flower-worker                 # all; tests/server.rs skips without the server binary, node or esbuild
node crates/flower-worker/parity/wire.ts    # regenerate tests/golden/wire.json from the TS worker
```

- `job_worker.rs` (sdk/job-worker.test.ts), `reconcile.rs` (reactive-worker-client.test.ts),
  `capacity.rs` (capacity.test.ts), `fake_conformance.rs` (reactive-queue.test.ts and
  external.test.ts against the fake), `wire.rs` (argument bytes vs the TS worker),
  `flower_client.rs` (the FlowerClient adapter on its mock transport), `server.rs` (the real
  server: `$FLOWER_BIN`, default `~/src/flower/target/release/flower`).
