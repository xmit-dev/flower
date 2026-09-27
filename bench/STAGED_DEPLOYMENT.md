# Staged materialization benchmark

`staged-deployment.mjs` compares one direct blocking deployment with the staged deployment lifecycle on fresh, isolated local clusters. Each source record has an independently materialized result; all results also read one shared derived value. The new bundle changes the shared multiplier. There are no added indexes or collection scans, so the experiment measures materialized graph preparation rather than index backfill.

```sh
nix develop -c cargo build --release --bin flower
node bench/staged-deployment.mjs \
  --binary target/release/flower --nodes 3 --sizes 100,500,1000 \
  --write-rate 20 --hot-roots 8 --repeats 3 \
  --output /tmp/staged-deployment.json
```

The defaults are three replicas, 100/500/1,000 roots, one repetition, and no concurrent writes. `--nodes 1 --sizes 100` is a short instrumentation pilot; label it as single-node and record whether the executable is a debug or release build. `--help` lists page budgets, setup/read batch sizes, deadlines, and the other controls. The harness copies the binary once and checks its SHA-256 at completion, so a rebuild of the original executable cannot change later cases.

Cold target-code compilation can dominate small cases. `--warm-target-bundle` deploys version 2 on the empty database during setup, before deploying version 1 and seeding. This keeps the measured source/graph workload identical while warming the target code cache in those processes. Use it to isolate graph rebuilding, and label those measurements as warm-code runs; the flag and workload metadata are retained in the report. Omit it to include normal cold target-code preparation.

Each size/mode/repetition gets a fresh database and processes. Setup deploys version 1, seeds roots in bounded mutation batches, and validates every source/result against an independent ledger. The timed deployment installs version 2. Direct mode uses `preparation: "blocking"`; optimistic deployment would instead measure conflicts and retries under concurrent writes. Staged mode measures `stage`, repeated `advance`, and `activate` separately. It records preparation page counts and the final ready state. Cleanup and a second correctness audit run after the timed interval. Both modes must pass the same final source and derived-value audit.

`--write-rate` enables one paced worker targeting a fixed subset of roots. It starts at most one mutation at a time. Slow calls cause scheduled starts to be skipped; the worker reports those misses and does not accumulate an unbounded queue. This is a controlled interference workload, **not an open-loop saturation test**. Latencies cover successful logical writes started during the deployment, including any retry time. A failed initial attempt is counted separately; the worker repeats the identical request ID/body to reconcile an uncertain result before advancing its ledger. An unresolved outcome fails the case and prevents a successful correctness claim.

Current servers use `FLOWER_DEPLOYMENT_PAGE_MS` to tune graph-page preparation independently of `FLOWER_WRITER_BATCH_MS` and ordinary mutation batching. It accepts positive integer milliseconds, defaults to 200 ms, and is capped by the normal evaluation timeout. Lower targets favor more, smaller pages and shorter competing write stalls; larger targets favor fewer commits and rebuild throughput. For a latency-sensitive experiment, prefix the harness command with `FLOWER_DEPLOYMENT_PAGE_MS=20`; the harness forwards and records that environment setting. Settings are read at server startup, so restart existing nodes to apply changes. The target is soft: an unsuccessful multi-root candidate may retry one root with its full normal evaluation timeout. Keep the separate `--max-bytes` limit unchanged when measuring the time-target tradeoff.

The report includes:

- Deployment wall time; staged admission, preparation, activation, and cleanup times. Direct activation cannot be separated from direct preparation through the public endpoint and is reported as `null`.
- Advance calls, index/graph page counts, and the ready deployment status.
- Measured write count, failed initial attempts, exact nearest-rank p50/p95/p99/max, achieved completion rate, and skipped paced starts. The rate denominator includes the deployment and the drain of outstanding measured writes. Empty latency samples are `null`.
- Independent correctness outcome, binary and bundle hashes, host/runtime settings, and per-case errors. Failed cases retain their isolated database directories; `--keep-data` preserves successful cases too.

Keep the binary, runtime environment, host load, root counts, write rate, page budget, and ordering identical when comparing two builds. Run builds/tests before timing, then execute baseline and candidate serially while the machine is idle. Multiple repetitions are needed to describe variability. Large direct rebuilds may hit ordinary evaluation limits; a failed case remains visible in the report and is not treated as a fast deployment. Small write sample counts cannot support strong tail-latency conclusions. All replicas and the generator share one host, and this workload has small roots and one shared dependency; it does not establish capacity for arbitrary graphs, large accumulators, or distributed production machines.

For a comparison of two staged implementations, use the same command for each pinned executable:

```sh
node bench/staged-deployment.mjs \
  --binary /path/to/pinned/flower --label debug-baseline \
  --modes staged --warm-target-bundle --nodes 3 \
  --sizes 100,500,1000 --write-rate 50 --hot-roots 8 --repeats 3 \
  --output /tmp/staged-baseline.json
```

This isolates the staged workload from the separate direct-versus-staged experiment. Compare individual repetitions and write sample counts as well as medians. If the two executable snapshots contain other code changes, their wall-time differences cannot be attributed solely to staged rebuilding; advance-call counts and targeted operation-count regressions provide more specific evidence for batching and traversal changes.

Experimental results belong outside the published pizza benchmark directory. The harness does not update existing benchmark results, the website, or production data.

## Measured executable snapshots: 2026-09-24

The command above ran sequentially for two pinned **debug** executable snapshots, with three repetitions per size and no concurrent builds or tests. All three replicas and the generator ran on one Apple M5 Pro host (18 logical CPUs, 48 GiB RAM, Darwin 27.2.0, Node 26.9.0). The target bundle was warmed during setup. The page budget was 262,144 bytes; the write target was 50 starts/s against eight fixed roots. The default candidate preparation window was 200 ms. No direct-mode measurements are included in this comparison.

These historical executable snapshots predate `FLOWER_DEPLOYMENT_PAGE_MS`: graph-page preparation used the then-shared `FLOWER_WRITER_BATCH_MS` setting. The measurements and raw reports below preserve that original configuration; they have not been relabeled as measurements of the new dedicated setting.

- Baseline SHA-256: `962d061406dbe673b59d3d460303c14fbe10b60e2ce5cdb8e4e1aa1c63406fe4`.
- Candidate SHA-256: `b8c14aadf212c15c2e10e17636ab7c5be3949edae68867401863eefa7eeeeb0e`.
- Both used bundle hashes `3e4d4f57cab4687bf87d481299a323a88fa7fc746131d63812eacaf042a1d5a9` and `188faa8dd3931e236034d2d10d39055368c49a06214670bbaa0bb5f2d4589ddf` and identical runtime environment overrides (none).

The candidate snapshot also contains concurrent runtime changes. These are comparisons of those executable snapshots, not an attribution of every timing difference to the staged rebuild changes. The advance counts directly show the change in the number of client preparation requests.

All 18 main cases passed the independent source/derived-value audit, including the audit after cleanup. No write attempts failed. Preparation and activation columns show the median followed by the minimum–maximum across three repetitions. Preparation excludes staging admission, activation, setup, cleanup, and audit; the raw reports also retain total deployment time.

| Roots | Snapshot | Graph preparation, seconds | Advance calls | Activation, ms |
| ---: | --- | ---: | ---: | ---: |
| 100 | Baseline | 2.762 (2.742–2.813) | 101 | 34.2 (34.1–34.2) |
| 100 | Candidate | 0.234 (0.232–0.236) | 7 | 36.7 (35.4–37.5) |
| 500 | Baseline | 15.230 (15.219–15.307) | 501 | 32.0 (31.7–35.8) |
| 500 | Candidate | 0.452 (0.439–0.467) | 9 | 37.2 (36.7–40.8) |
| 1,000 | Baseline | 34.545 (34.415–34.910) | 1,001 | 33.0 (31.1–34.1) |
| 1,000 | Candidate | 0.702 (0.691–0.716) | 11 | 36.3 (36.2–36.8) |

Fewer, larger preparation pages shortened rebuilding but increased the largest observed write delays. The latency percentiles below are **medians of the three per-run percentiles**, not pooled percentiles. The maximum is the largest individual latency observed across all three runs. The achieved rate is the median per-run completion rate, including final drain.

| Roots | Snapshot | Writes per run | p50, ms | p95, ms | p99, ms | Maximum, ms | Achieved writes/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | Baseline | 99–103 | 27.9 | 29.8 | 33.3 | 41.8 | 35.7 |
| 100 | Candidate | 9 | 30.1 | 48.9 | 48.9 | 49.0 | 29.5 |
| 500 | Baseline | 495–497 | 30.1 | 34.0 | 46.0 | 72.9 | 32.4 |
| 500 | Candidate | 11 | 33.7 | 121.6 | 121.6 | 132.8 | 21.0 |
| 1,000 | Baseline | 1,000–1,002 | 34.7 | 40.1 | 47.0 | 87.5 | 28.9 |
| 1,000 | Candidate | 12–13 | 32.8 | 160.9 | 160.9 | 244.1 | 16.5 |

The candidate's much shorter deployment interval produced only 9–13 measured writes per run; nearest-rank p95 and p99 therefore equal that run's maximum. These samples demonstrate a latency tradeoff but cannot establish a reliable production tail percentile. The achieved rate is below the 50/s target because this single-worker workload skips starts when a write remains outstanding. A 200 ms preparation window is soft: the maximum includes queueing, commit work, and the rest of the mutation request, and exceeded 200 ms in one run.

A separate, single 1,000-root candidate run set `FLOWER_WRITER_BATCH_MS=20`, keeping all other workload settings unchanged. It passed both correctness audits with no write failures: preparation 0.841 s, 16 advances, activation 34.9 ms, 18 measured writes, p50 53.0 ms, p95/p99/maximum 74.8 ms, and 19.8 achieved writes/s. This historical observation used the then-shared writer window and suggests a tradeoff between rebuild time and the largest write delay in this workload; one repetition with 18 samples is not a latency guarantee. It is excluded from the main comparison medians and is not a measurement of `FLOWER_DEPLOYMENT_PAGE_MS`.

Raw reports retain every repetition, timing, count, error, and hash: [baseline](staged-deployment-results/2026-09-24-baseline.json), [candidate](staged-deployment-results/2026-09-24-candidate.json), and [20 ms sensitivity](staged-deployment-results/2026-09-24-candidate-batch20.json). These small-root, warm-code, single-host debug measurements do not establish production capacity or costs for cold code, expensive roots, large shared accumulators, or independent physical machines.
