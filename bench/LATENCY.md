# Latency investigation, 2026-09-24

The fresh publication measured **56,154.7 calls/sec, 12.9 ms read p99 and 228.3 ms write p99**. The final run was slower than the 65.7k redb controls below; it is retained as the preselected fresh publication rather than replaced by a better exploratory observation. These shared-desktop runs show variation, not a guaranteed capacity or latency bound.

The latency preset uses 256 customer loops per group and a 50 ms writer preparation ceiling. Outstanding work is an intentional throughput/latency tradeoff; it must be distinguished from the server optimizations below. Reads and writes have separate latency distributions throughout the published report.

## Current snapshot design: recover directly from redb

Periodic Raft snapshot construction now retains immutable state roots and writes **only checkpoint metadata**. Its immediate transaction makes the existing redb application tables durable before the covered Raft logs can be pruned. No full-state serialization or duplicate chunk image occurs on that path. The first read or seek of a transfer handle encodes the captured state into an anonymous temporary file outside the write gate. New and lagging replicas still receive the same complete, consistent JSON state.

Local recovery reads application records, retry receipts, partitions, membership and applied position from redb. A retained metadata checkpoint validates the durable recovery floor. If later application writes also became durable, recovery advances the checkpoint metadata to exactly that recovered state before exposing a new lazy transfer image. State behind its checkpoint or purged log floor fails closed. Installation validates the received state and metadata, then atomically replaces the application tables and checkpoint with immediate durability. Captured roots cannot overwrite a newer installation, including one at the same log position.

Prior JSON-envelope, binary, raw-chunk and LZ4-chunk images remain readable. The first new checkpoint retires the old full image and chunk table in the same durable transaction. Application indices, materialized values and retry receipts remain part of the logical state and any transfer; they are no longer repeatedly serialized merely to permit log compaction.

Tests model power loss by reopening only the bytes retained by the last successful backend sync, before clean shutdown can flush anything else. They cover deferred application loss with a durable log available for replay, durable application state ahead of an older checkpoint, recovery after pruning all covered logs, failed checkpoint flushes, cancellation during flush, and exact old/new transfer images. Separate tests cover lazy read/seek behavior, independent cursors, encoding failures, legacy formats and malformed state.

### Measured effect of using redb directly

| Server | Batch ceiling | Loops/group | Calls/sec | Read p99 | Write p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Metadata-only checkpoint, 1 | 50 ms | 256 | 65,702 | 10.9 ms | 196.6 ms |
| Compressed full-image control, 3 | 50 ms | 256 | 65,291 | 14.4 ms | 301.6 ms |
| Metadata-only checkpoint, capacity | 200 ms | 512 | 81,094 | 59.0 ms | 418.9 ms |
| Metadata-only checkpoint, middle | 50 ms | 384 | 71,491 | 26.6 ms | 343.3 ms |
| Metadata-only checkpoint, 2 | 50 ms | 256 | 65,809 | 25.8 ms | 185.2 ms |

Both 256-loop redb runs retained roughly 65.7k calls/sec, with write p99 35–39% below the interleaved compressed-image control. Read tails varied and do not establish a consistent read-p99 improvement from this code change alone. Estimated server CPU per call in the first pair was 143.37 versus 143.54 microseconds, effectively unchanged at this measurement's precision. The higher-concurrency screens did not preserve the compressed implementation's highest observed throughput. Every run passed all eight audits; the 256-loop/50-ms preset was retained for its latency balance.

A separate 60-second redb diagnostic retained 234,007 spans and 249,470 metric points with zero receiver record drops. It measured **80 metadata-only checkpoints averaging 0.015 ms writing and 25.725 ms committing**, versus 24.510 ms writing and 146.945 ms committing compressed full images in the earlier diagnostic. Checkpoint JSON encoding averaged **0.0027 ms**, compared with 365.475 ms of full-state encoding before. Ordinary log commits averaged 15.896 ms over 34,355 observations. Snapshot populations, host scheduling and instrumented throughput differ; these are stage observations, not a fixed-image microbenchmark. No full-transfer encoding entered the interior metric intervals; separate integration tests force snapshot catch-up after log pruning.

Remaining unsampled writer means were **30.91 ms queue wait**, **22.94 ms batch preparation**, **33.73 ms batch commitment**, and **90.94 ms submission-to-receipt**. Evaluation averaged 0.197 ms per observed evaluator stage, application JSON extraction 0.004 ms and POST body reception 0.380 ms. Populations differ and preparation can overlap the preceding commit, so these are not additive parts of one request or CPU percentages. Queueing, batching and ordinary durable replication now dominate this diagnostic, rather than repeated full-state serialization and snapshot flushes.

The following sections preserve the preceding investigation, including the intermediate compression experiment; those full-image costs are historical baselines for the metadata-only design.

## Server changes

Raft can now start replicating an append while the leader's immediate-durability transaction flushes. A single immutable pending suffix makes entries readable to replication. The completion callback still waits for the durable commit, and client acknowledgments still require durable quorum and local atomic application. The I/O serialization guard bounds the pending buffer to one append and orders it with votes, subsequent appends, truncation, purge and application. Cancellation cannot abandon the flush. Failed flushes and panicking workers report errors through Raft's completion path. Log readers pair the pending suffix with an MVCC snapshot so clearing the buffer cannot create a read gap. The disk-only read path retains its direct vector allocation and decoding.

The async runtime checks I/O after seven scheduling ticks to favor socket/timer responsiveness during bursts. Evaluation and storage still use blocking workers. Ordinary mutation preparation now has a 50 ms soft ceiling; deployment paging retains its independent 200 ms default. One slow method can exceed the preparation ceiling, and queueing, replication and retries can make end-to-end latency longer.

HTTP telemetry now distinguishes `body_receive` from `json_extract` on the application call routes. Both are nested inside the HTTP request span. The body stage includes transport, flow control and scheduling waits, and cannot identify those causes individually. JSON extraction delegates to Axum, preserving content types, limits, errors and status codes. See [TELEMETRY.md](../TELEMETRY.md).

## Where time was spent

A separate instrumented run on the previous server, with the new ingress timings, used eight three-node groups, 512 loops/group, 200 ms batching, 20 seconds of requested load, leader failure, 1% trace sampling and one-second metric exports. Metrics were pooled only from consecutive export intervals wholly inside the measured window.

| Unsampled stage | Observations | Mean wall time |
| --- | ---: | ---: |
| Writer submission to receipt | 441,791 | 111.99 ms |
| Writer queue before preparation | 441,964 | 46.30 ms |
| Batch preparation | 2,995 | 28.22 ms |
| Batch commitment | 2,995 | 36.14 ms |
| POST body reception | 1,416,856 | 1.724 ms |
| Application JSON extraction | 1,402,897 | 0.0042 ms |

These populations differ. Batch preparation overlaps the preceding commit; the rows cannot be added into an exclusive per-request budget. Timings are wall time, not CPU percentages. The diagnostic's goodput is not a capacity measurement.

A sampled 99.69 ms read spent 99.35 ms in body reception, then 0.003 ms extracting JSON. A 70.24 ms read spent 70.22 ms in body reception. Other slow reads waited for admission, including a 188.64 ms read with 187.00 ms of admission wait. This accounts for the previously unexplained time before cache lookup without attributing transport or scheduling waits to query execution.

## Full-duration controls and snapshot diagnosis

The initial 30-second screens did not reproduce consistently over 60 seconds. With **256 loops/group and a 50 ms ceiling held fixed**, the before/after/after/before sequence was:

| Server | Calls/sec | Read p99 | Write p99 |
| --- | ---: | ---: | ---: |
| Before, 1 | 63,533 | 19.9 ms | 329.9 ms |
| Flush overlap + I/O polling, 1 | 65,419 | 28.5 ms | 295.7 ms |
| Flush overlap + I/O polling, 2 | 62,789 | 28.2 ms | 350.2 ms |
| Before, 2 | 66,342 | 23.4 ms | 313.9 ms |

These runs do not establish a matched-load latency improvement from flush overlap and polling alone. At the previous 512-loop/200-ms capacity settings, overlap measured 92,675 calls/sec, 59.0 ms read p99 and 521.4 ms write p99; before measured 82,028 calls/sec, 69.2 ms read p99 and 444.7 ms write p99. Higher throughput did not imply better write tails.

A complete, separate 60-second trace of the uncompressed overlap server used 256 loops/group, 50 ms batching, 1% sampling, and one-second metric exports. The receiver retained 269,654 spans and 238,812 metric points with **zero receiver record drops**. Unsampled storage timings pooled across complete interior export intervals were:

| Stage | Observations | Mean wall time |
| --- | ---: | ---: |
| Ordinary log commit, including immediate flush | 36,524 | 14.045 ms |
| Snapshot JSON encoding outside the write gate | 109 | 447.620 ms |
| Snapshot chunk writes | 109 | 17.371 ms |
| Snapshot commit, including immediate flush | 109 | 262.816 ms |

Snapshot chunk writing and commit hold the same serialized database write gate used by Raft append, so a snapshot can delay new durable appends. Encoding runs outside that gate, but still consumes CPU and file I/O. The stage means are not a decomposition of customer p99, and concurrent work must not be summed. One sampled snapshot encoded 19.3 MB and spent 328 ms committing it. The growing retained receipt set makes the later part of a full-duration run relevant; a short run can understate this cost.

The snapshot change compresses each 256 KiB disk chunk with LZ4, falling back to raw bytes when compression would expand it. Decode memory is bounded to one chunk and the original stream checksum is still verified. The atomic transaction, immediate flush and Raft snapshot transfer are unchanged. Current binaries read previous raw chunks; an older binary rejects a compressed manifest on startup, so downgrading a directory after snapshot publication is unsupported.

## Snapshot compression and the latency preset

Full-duration uninstrumented comparisons used the same frozen customer driver. Every run passed all eight audits and delivered all 768 orders:

| Server | Batch ceiling | Loops/group | Calls/sec | Read p99 | Write p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Compressed snapshots, 1 | 50 ms | 256 | 61,024 | 12.0 ms | 292.8 ms |
| Uncompressed overlap control, 3 | 50 ms | 256 | 67,752 | 28.8 ms | 273.1 ms |
| Compressed snapshots, capacity | 200 ms | 512 | 92,005 | 70.6 ms | 333.2 ms |
| Compressed snapshots, 2 | 50 ms | 256 | 65,186 | 23.4 ms | 287.0 ms |
| Compressed snapshots, middle, 1 | 50 ms | 384 | 74,922 | 33.1 ms | 386.8 ms |
| Compressed snapshots, middle, 2 | 50 ms | 384 | 75,570 | 37.3 ms | 364.4 ms |

At 512 loops/200 ms, compression retained approximately 92k calls/sec while write p99 fell from 521.4 to 333.2 ms; read p99 increased from 59.0 to 70.6 ms. This is a single pair, not a confidence interval. At 256 loops/50 ms, compressed read tails were lower than the three uncompressed overlap controls, but write-tail ranges overlap. The evidence does not establish a consistent matched-load write-p99 gain there. Estimated server CPU per successful call rose from roughly 144–146 to 148–149 microseconds; these closed-loop estimates include other server work and are not a fixed-work CPU comparison.

The 256-loop/50-ms preset was retained for its read and write latency balance. The 384-loop screens recovered throughput but had substantially higher write tails. Publication uses a separate fresh run after validation, not the best exploratory observation. Reduced outstanding work is an explicit part of the latency improvement.

A separate 60-second compressed diagnostic retained 270,590 spans and 245,820 metric points with zero receiver record drops. Complete interior metric intervals included 99 snapshot builds and 36,742 ordinary appends:

| Mean storage wall time | Uncompressed diagnostic | Compressed diagnostic |
| --- | ---: | ---: |
| Ordinary log commit | 14.045 ms | 14.850 ms |
| Snapshot encoding outside the write gate | 447.620 ms | 365.475 ms |
| Snapshot chunk writes, including compression | 17.371 ms | 24.510 ms |
| Snapshot compression, included in writes | — | 12.875 ms |
| Snapshot commit, including immediate flush | 262.816 ms | 146.945 ms |

Compression accounted for **738,333,124 stored chunk bytes from 1,493,294,230 input bytes**, a 50.6% reduction, excluding manifests and redb overhead. Snapshot commit time fell 44.1% while chunk-writing time increased. Snapshot populations and sizes differ between runs; these measurements explain the mechanism but are not a fixed-image microbenchmark. Sampled spans and storage stages overlap. Crashes can lose unexported telemetry even when the receiver reports no drops.

Compact run identities, settings, audit totals, per-kind quantiles, CPU coverage and pooled diagnostic counters are retained in [latency-results/2026-09-24.json](latency-results/2026-09-24.json).

## Why store another snapshot when redb is durable?

Local startup loads the redb application tables, receipts and applied-position metadata, then uses the durable Raft log and recovery barrier if application checkpoints lag. It does not restore the application by decoding the separately stored Raft snapshot.

The Raft image supports new or lagging replicas after log compaction. It contains all logical records, receipts, partition state, application indices and materialized values at one applied position, together with membership metadata. It excludes redb's physical B-trees, free pages and Raft log. Application indices currently live in the same record map and are **not omitted**. Rebuilding them would require a defined, versioned reconstruction path before the receiving replica can serve.

The final implementation avoids the extra durable image: checkpoint the authoritative application state before deleting the logs needed to reconstruct it, then generate a consistent transfer image when required. Current application checkpoints defer durability, so simply removing snapshot persistence would leave a recovery gap unless that checkpoint/pruning invariant is enforced. The metadata checkpoint, immutable transfer roots, startup recovery and cancellation tests above enforce that invariant. The earlier compression experiment reduced the duplicate image; the final implementation removes it from periodic snapshot construction.

## Exploratory screens

All screens below requested 30 seconds of load with the same workload, eight groups, leader failure, driver, queue capacity and worker settings. Every run passed all eight audits and delivered all 768 orders. These are individual observations on a shared desktop, not confidence intervals. The baseline repeated at 77.2k and 89.2k calls/sec, illustrating why isolated screens do not establish the size of an optimization's effect.

| Server | Batch ceiling | Loops/group | Calls/sec | Read p99 | Write p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before | 200 ms | 512 | 77,242 | 56.1 ms | 449.1 ms |
| Before | 50 ms | 512 | 86,110 | 89.6 ms | 346.7 ms |
| Before | 25 ms | 512 | 79,152 | 45.1 ms | 516.2 ms |
| Before | 10 ms | 512 | 43,155 | 4.6 ms | 832.3 ms |
| Before | 200 ms | 256 | 59,512 | 21.4 ms | 326.6 ms |
| Before | 200 ms | 128 | 37,866 | 4.7 ms | 189.0 ms |
| Before | 200 ms | 64 | 23,129 | 2.4 ms | 159.6 ms |
| Flush overlap | 200 ms | 512 | 85,949 | 65.8 ms | 379.2 ms |
| Flush overlap | 50 ms | 512 | 81,814 | 71.3 ms | 398.6 ms |
| Overlap + I/O polling | 50 ms | 512 | 89,008 | 69.9 ms | 343.3 ms |
| Overlap + I/O polling | 50 ms | 256 | 70,178 | 33.5 ms | 215.1 ms |
| Overlap + I/O polling | 200 ms | 512 | 84,270 | 44.6 ms | 402.6 ms |
| Overlap + I/O polling | 50 ms | 384 | 71,194 | 33.1 ms | 368.1 ms |
| Before, repeat | 200 ms | 512 | 89,212 | 67.8 ms | 287.0 ms |

A 10 ms ceiling increased durable commit frequency and backed up writes. Lowering the batch ceiling alone is not a reliable way to reduce end-to-end latency under saturation. Reducing outstanding operations addresses the queue, while replication overlap aims to recover some throughput at that lower concurrency.

## Reproduction and validation

Apple M5 Pro, 18 logical CPUs, 48 GiB RAM, Darwin 27.2.0, Node 26.10.0. Builds, tests and telemetry collection were outside uninstrumented measurement windows; ordinary desktop applications remained active. Local binaries, raw reports and sanitized telemetry are retained in `/tmp/flower-latency-20260924/`.

```sh
export FLOWER_WRITER_BATCH_MODE=adaptive FLOWER_WRITER_BATCH_MS=50
export FLOWER_WRITER_QUEUE_CAPACITY=1024 TOKIO_WORKER_THREADS=2
export FLOWER_QUERY_WORKERS=16 FLOWER_PREPARATION_WORKERS=16
export FLOWER_WRITER_PREPARATION_WORKERS=1 FLOWER_OTEL_ENABLED=0
export OTEL_SDK_DISABLED=true
node bench/goblin-pizza.mjs --binary /path/to/before-or-after \
  --driver-binary /path/to/shared-driver \
  --groups 8 --http2 --duration 60 --concurrency 256 \
  --workers 4 --max-orders 96 --chaos --json /tmp/unique-run.json
```

The final redb implementation passed 583 library tests, including OpenRaft storage conformance and three-node snapshot catch-up, restart and quorum-loss coverage. All 38 storage tests passed again after extending the power-loss fixture to cover recovered state ahead of its checkpoint. Ten server tests and 154 benchmark tests passed. The HTTP index/aggregate end-to-end test passed a simultaneous full-cluster kill/restart and subsequent leader recovery. The OTEL end-to-end test verified delayed body reception, separate JSON extraction and extraction errors, JSON/protobuf exports, forwarding, batch links, replica identities, payload exclusion, shutdown flushing and disabled reporting. Clippy completed with eight existing warnings.
