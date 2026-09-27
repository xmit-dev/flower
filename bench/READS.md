# Read and write latency measurements

The current publication measures **56,154.7 successful customer calls/sec**, **12.9 ms read p99** and **228.3 ms write p99**. All eight independent audits passed, with zero failed customer calls and all 768 orders delivered.

[Charts, recovery, and every group](https://flower.xmit.dev/bench/latest.html) · [raw measurements](../docs/bench/latest.json) · [latency experiments and tracing](LATENCY.md) · [reproduction](README.md).

## Host and workload

Measured **2026-09-25T00:20:27.346Z–2026-09-25T00:21:27.487Z**, over **60.141 seconds**, including final in-flight customer tails. Apple M5 Pro, 18 logical CPUs, 48 GiB RAM, Darwin 27.2.0 arm64, Node.js v26.10.0. All 24 durable replicas, eight Rust drivers and eight Node controllers shared this desktop. Builds, tests and telemetry diagnostics ran outside uninstrumented measurement windows; ordinary desktop applications remained active.

Eight independent three-node Raft groups each ran **256 closed-loop customer loops**, four delivery workers, two tenants with four stores each, a 96-order cap, one second of warmup and 60 seconds of load. Each leader was killed halfway through. About 70% of calls read replica-local committed snapshots over pooled HTTP/2 and may lag; 30% are durable mutations. Final audits use fresh reads. The application has no authorization hook.

Runtime settings: adaptive batching with a **50 ms preparation ceiling**, 1,024-request writer queue, 16 query/cache-probe slots, 16 shared preparation slots, one serial writer-preparation worker and two Tokio async workers per replica. OTEL was explicitly disabled. The ceiling is not an end-to-end latency bound.

## Recorded results

| Measurement | Current run |
| --- | ---: |
| Successful customer calls | 3,377,202 |
| Customer reads / writes | 2,362,869 / 1,014,333 |
| Customer goodput | 56,154.7/sec |
| Read p50 / p95 / p99 | 1.0 / 4.8 / 12.9 ms |
| Write p50 / p95 / p99 | 96.1 / 172.8 / 228.3 ms |
| Failed customer calls | 0 |
| Audited orders / pizzas | 768 / 1,916 |
| Exact receipt replays / stale-lease rejections | 101,757 / 107 |
| Estimated mean server cores | 8.39 |
| Per-server CPU sampling coverage | 95.5–99.1% |

Goodput excludes retries, worker traffic, replay probes, warmup and drain. Zero failed logical calls does not mean every HTTP attempt succeeded. Percentiles merge per-group histogram buckets. CPU counters include all server work; estimated mean cores extrapolate sampled intervals and do not establish instantaneous utilization or exact request CPU cost.

## Implementation, tracing and reproduction

The server overlaps Raft replication with the leader's durable log flush and uses **redb application tables as the durable recovery source**. Periodic snapshots flush a small metadata checkpoint; they no longer serialize or persist another full state image. Transfer images are encoded lazily when a replica needs one, and installation remains atomic and immediately durable. Application indices, materialized values, receipts and partitions stay in the logical state. Prior snapshot formats remain readable and are retired atomically on checkpoint. [The latency investigation](LATENCY.md) records the intermediate compression experiment, matched controls, power-loss tests and measured storage stages.

[OpenTelemetry reporting](../TELEMETRY.md) covers ingress, query, writer, evaluator, storage and Raft stages, including metadata checkpoint and on-demand snapshot transfer costs. Instrumented runs are diagnostic, not capacity measurements.

```sh
nix develop
npm ci
cargo build --release --locked --bin flower --bin flower-bench-driver
FLOWER_OTEL_ENABLED=0 OTEL_SDK_DISABLED=true npm run bench:stress
node scripts/publish-bench-results.mjs bench/results/latest.json bench/results/latest-wasm.json
node scripts/publish-bench-results.mjs --check
```

Server SHA-256: `f9272bd6ccd6b252f6b16cb7e0582878fbc2d8d8936fb5a0fce8c2871a32292e`. Driver SHA-256: `1cf843cbf4e064d13a1d3597d9295c509325fee5c74b6eb2242ac7e98ece553d`. Bundle hash: `264f2718f2336ea92b850ec939282695609e4ac929a73006455bf212872473b0`. The raw report records exact runtime options, binary identities, faults, customer accounting and per-group audits. Validation details are in [LATENCY.md](LATENCY.md).

## Measurement limits

The hot-store workload makes each group's mutation work largely serial. Native canonical encoding and fewer scheduler/allocator crossings reduce that cost but do not remove QuickJS execution, reactive recomputation, host serialization, Raft ordering, or disk/quorum latency. Durable commits account for a larger share of the remaining critical path. Parallel speculative preparation remains available for independent writes; forcing it off is a workload-specific choice. Larger queues or batching windows can worsen tails and timer lateness without adding goodput.

The order population is bounded and the dataset is hot. After reaching the order cap, traffic is mostly repeated shop previews and tips; it is not an indefinitely growing order stream. Application state remains in memory as well as durable storage. Dataset growth, many distinct queries or watches, large values, crypto-heavy authorization, index backfills, and transactions across groups need separate measurements. Applications with authorization hooks and managed-key queries take the full admitted path; this run does not measure their cost. Local co-location also does not model network or disk isolation across production machines.

This closed-loop run reduces arrivals when service slows. It does not establish sustainable service under a fixed offered arrival rate or provide an overload latency guarantee. Use `--offered-rate` and inspect driver drops as well as latency. Repeat matched runs on a quiet host before attributing a difference to a code change. [Architecture notes](ARCHITECTURE.md) and the [capacity controls](LIMITS.md) describe the implementation tradeoffs.
