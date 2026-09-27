# CPU profiling, 2026-09-24

The optimized server used **12.3% less estimated server CPU per successful customer call** at the same offered rate, averaged over two before/after pairs. In the full stress preset, mean goodput rose **10.7%**, from 82,723 to 91,534 requests/sec; estimated CPU per call fell **14.8%**. These are repeated measurements on one developer machine, not confidence intervals or a hardware-independent capacity claim.

## What changed

- Disabled OTEL now installs global logging filters and no optional tracing layer. The previous subscriber still constructed spans that had no consumer. With OTEL enabled, the logging predicates share one filter registration. Regression tests exercise disabled spans and independent log/export filters.
- Concurrent cold requests prepare a bundle once per content key. Waiters have independent deadlines and memory budgets. Failed or panicking preparation wakes callers to retry; only immutable prepared images are shared. The existing image-cache size limits remain in force.
- HTTP method lookup deserializes the borrowed registry value, and query invocation cloning occurs only when evaluation is necessary.
- Evaluator metrics retain bound instruments for their finite mode/outcome/stage combinations. This removes repeated attribute hashing and SDK series-map locking while preserving zero observations, dimensions, and cumulative export counts.
- Cold bundle preparation now reports initialization, bytecode compilation, static initialization, snapshot preparation, snapshotting, native compilation, total preparation, and coalesced waiting separately.

## Measurement setup

Apple M5 Pro, 18 logical CPUs, Darwin 27.2.0, Node 26.10.0, release Rust build with thin LTO. Eight three-replica groups share the host. The server before this change includes the initial OTEL implementation.

| Artifact | SHA-256 |
| --- | --- |
| Before server | `b2db2741e65d980bf244c2629473faaf8ffdc2d86cd630cf9325ad200209fa5d` |
| After server | `3b47cf25cfa2a278f6b96e9ab675c70694b1ad4408e6f09a18e2555c0939c9c9` |
| Shared customer driver | `a4137758321e54d18a809ff480838078ab041c7f319a40eea812f062cccfd5f3` |
| Shared application bundle | `264f2718f2336ea92b850ec939282695609e4ac929a73006455bf212872473b0` |

Runtime settings in both versions: adaptive batching, 200 ms batch ceiling, queue capacity 1024, two Tokio workers, 16 query/preparation workers, and one writer preparation worker per process. Benchmarks use HTTP/2, 512 customer slots/group, four delivery workers/group, 96 retained orders/group, and the same seed. Builds, tests, native profilers, and OTEL collection were outside these measurement windows. Other desktop applications remained running.

## Offered-rate CPU comparison

Each run offered 3,000 calls/sec/group for 40 seconds without leader failure. Runs used before/after/after/before order. All offered calls completed, with no driver drops or failed calls; all 768 orders passed the audit in every run.

| Run | Successful calls | Observed server CPU seconds | Estimated CPU µs/call | Customer p99 ms |
| --- | ---: | ---: | ---: | ---: |
| Before 1 | 950,784 | 266.71 | 285.22 | 133.4 |
| After 1 | 950,772 | 242.96 | 259.78 | 122.0 |
| After 2 | 950,751 | 249.35 | 266.50 | 113.8 |
| Before 2 | 950,838 | 294.75 | 314.97 | 88.7 |

The paired CPU reductions were **8.9% and 15.4%**. Mean estimates were 300.09 → 263.14 µs/call. CPU sample coverage was 98.28–98.52% per process. Completed counts differed by at most 0.0092% because dispatch begins inside the controller's timed window; the comparison utility therefore does not label these as exactly equal fixed work. Latency did not improve consistently across the repeats.

## Full stress preset

These runs use the unchanged 60-second stress workload, including a leader crash halfway through every group. The before/after/after/before order includes the initial baseline taken before native profiling. Every run passed all eight audits, delivered all 768 orders, and had zero failed logical customer calls.

| Run | Customer requests/sec | Estimated CPU µs/call | Customer p99 ms |
| --- | ---: | ---: | ---: |
| Before 1 | 82,437 | 131.29 | 257.3 |
| After 1 | 86,631 | 112.72 | 267.7 |
| After 2 | 96,437 | 108.54 | 215.1 |
| Before 2 | 83,010 | 128.38 | 249.7 |

Closed-loop demand changes with server speed. These runs establish the measured throughput range; the offered-rate runs provide a stronger CPU-efficiency comparison. The 96.4k result is one repeat, not a replacement headline chosen over the other results.

CPU counters include every server's replication, retries, delivery traffic, and background work. The denominator includes only successful primary customer calls. Estimates extrapolate each process's observed CPU/wall ratio across its group's load duration, so omitted edge and restart intervals can bias the result. See `bench/compare-cpu.mjs` for per-process coverage and exact calculations.

## Cold follower contention

A separate diagnostic deployed one static bundle, waited for a follower to apply its metadata without evaluating it, then sent 16 simultaneous replica-local queries with distinct arguments over one preconnected HTTP/2 session. Both binaries used the same bundle, 16 query/preparation workers, and a 30-second evaluation budget. The follower's native map contained only its two runtime images before each burst.

| Measurement | Before | After |
| --- | ---: | ---: |
| Successfully compiled bundle images | 16 | 1 |
| Observed follower CPU seconds during burst | 6.15 | 0.21 |
| Burst wall time, ms | 940.7 | 208.3 |
| Correct isolated responses | 16/16 | 16/16 |

This directly demonstrates removal of duplicate preparation. CPU is cumulative process user+system time from `ps`, with 0.01-second resolution, and includes ordinary Raft background work. This is one targeted pair, separate from steady-state capacity. The normal stress profiles did not show duplicate completed bundle compilations, so their throughput gains cannot be attributed specifically to this fix. Reproduce with [profile-cold-burst.mjs](profile-cold-burst.mjs), using a fresh output directory for each binary.

## Profiles and reproduction

The baseline leader's Instruments Time Profiler export contained 18,573 ms of recorded Running sample weight, with 99.962% of that weight having complete exported stacks. Guest addresses were resolved using a sidecar matching the checked-in QuickJS artifact and the sampled PID. Dirty-memory restoration accounted for 5.28% of Running weight; 5.20% was copying within that restoration. Record insertion, JSON parsing, forwarding, allocation, and tracing registry work were other visible costs. These are statistical stack weights, not exact CPU counters, and inclusive frames overlap.

The after profile contained 16,795 ms of Running weight with 99.964% complete exported stacks. Tracing registry and sharded-slab functions disappeared from the top 100 self frames; the baseline's registry pool lookup alone accounted for 1.49% of Running weight. Dirty-memory restoration remains a visible cost at 5.97%. Complete stack coverage does not imply every symbol resolved: some frames remained unresolved in 27.2% of baseline and 30.6% of after Running weight.

Separate macOS `sample` captures covered the initial leader and a follower. Their all-thread wall observations include waiting and were not treated as CPU utilization. The tools and commands are documented in [README.md](README.md#profiling).

Separate 20-second OTEL runs used the same eight-group failure workload, 1% trace sampling, and one-second metric export. They captured 78,196 → 86,084 total spans and 82,257 → 84,529 metric points from 32 process instances per run, including restarted nodes. Both passed all audits. Instrumented goodput was 61,441 → 67,979 requests/sec; these diagnostic runs are not capacity measurements. Collector span/metric drops, rejected exports, and invalid requests were zero. SDK link truncation was still reported (5,070 → 5,126); exporter-buffer loss and spans lost at forced process termination are not observable from the collector. The after report includes the new cold preparation stages.

Local artifacts are retained under `/tmp/flower-cpu-20260924/`: matching before/after binaries, benchmark JSON/HTML and logs, compact `results.json`, offered-rate comparisons, native maps, sanitized native-profile summaries, and separate OTEL reports. Raw Instruments traces remain local because they can contain inherited environment metadata.

```sh
# Set the identical runtime configuration for both binaries.
export FLOWER_WRITER_BATCH_MODE=adaptive FLOWER_WRITER_BATCH_MS=200
export TOKIO_WORKER_THREADS=2 FLOWER_WRITER_QUEUE_CAPACITY=1024
export FLOWER_QUERY_WORKERS=16 FLOWER_PREPARATION_WORKERS=16
export FLOWER_WRITER_PREPARATION_WORKERS=1 FLOWER_OTEL_ENABLED=0

node bench/goblin-pizza.mjs --binary /path/to/before-or-after \
  --groups 8 --http2 --duration 40 --concurrency 512 --offered-rate 3000 \
  --workers 4 --max-orders 96 --json /tmp/unique-cpu-run.json
node bench/compare-cpu.mjs /tmp/before.json /tmp/after.json

# Separate capacity runs, with the standard failure workload.
node bench/stress.mjs --binary /path/to/before-or-after --json /tmp/unique-stress-run.json
```

## Validation

The final quiet Rust library suite passed all 570 tests with four test threads, including concurrent cold preparation, separate invocation heaps/callbacks/budgets, filter independence, borrowed registry validation, and bound metric cumulative exports. All 152 benchmark JavaScript tests and 11 native-profile analyzer tests passed. The OTEL end-to-end test passed against the frozen after binary, covering both OTLP transports, propagation, batch links, replica metrics, payload exclusion, shutdown flushing, and disabled reporting. Release compilation and Clippy completed; Clippy retained eight existing warnings.

An earlier full test run overlapped a release build and one differential evaluator test hit its five-second execution budget. That test passed alone and in the final quiet full suite. Performance measurements never overlapped these validation jobs.
