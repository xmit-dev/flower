# Writer goodput investigation, 2026-09-24

A pipelined writer successor no longer closes at its adaptive count or time target while its predecessor is still committing. Over four alternating full stress-preset pairs, mean customer goodput rose **18.0%, from 56,886 to 67,123 calls/sec**, and every pair improved (+8.1% to +26.4%). Pooled write latency fell at every reported percentile: p50 93.2 → 75.7 ms, p95 174.5 → 151.8 ms, p99 252.2 → 217.2 ms. Sampled server CPU fell from 8.37 to 7.70 cores, about 22% less per successful call. At the same offered load, read and write tails were both lower. Closed-loop read p99 rose from 16.2 to 22.0 ms because the same 2,048 loops completed more calls. These are shared-desktop measurements, not confidence intervals.

## Where time went

A 30-second instrumented run of the unchanged server used the stress settings without leader failure, 1% trace sampling and one-second metric export. Unsampled writer means on the leaders were:

| Stage | Mean |
| --- | ---: |
| Submission to receipt | 74.0 ms |
| Queue wait before preparation | 26.5 ms |
| Batch preparation | 22.8 ms |
| Batch commitment | 27.8 ms |
| Log append, including immediate flush | 12.2 ms |
| State-machine apply | 0.4 ms |

Batches held 97 requests on average. Queue wait was about one full commitment round: only 3% of sampled mutations started preparation within 5 ms of submission, and the median waited 22.7 ms. Leaders used about half a core each; the host was not CPU-bound.

Every batch costs three `F_FULLFSYNC` calls, one per replica, and all 24 replicas share one SSD. The run performed about 730 flushes/sec machine-wide. A standalone probe (16 KiB write plus `F_FULLFSYNC` in a loop) measured the device alone:

| Concurrent processes | Flushes/sec | Mean flush |
| ---: | ---: | ---: |
| 1 | 306–320 | 3.1–3.2 ms |
| 8 | 594–656 | 10.2–11.5 ms |
| 16 | 754–780 | 14.7–15.1 ms |
| 24 | 814–941 | 17.1–20.2 ms |

Write size between 4 KiB and 128 KiB made little difference. On this host, goodput at a fixed loop count therefore depends on how many calls each durable round carries and how little a call waits for a round.

## Successor cutoffs

The adaptive controller gives each successor a learned count and time target. 19.4% of successors stopped at `preparation_time` and 4.9% at `count_target` while their predecessor was still committing. The successor could not be submitted any earlier, so later arrivals simply waited for the next group, an extra durability round. The DEBUG timeline showed successors stopping 10–20 ms before their predecessor completed.

Now, in adaptive mode, a successor keeps preparing arrivals until its predecessor completes, up to the queue capacity and any explicit `FLOWER_WRITER_BATCH_SIZE`. It then stops at the next call boundary, including inside a serial worker job, and is submitted immediately; an unprepared suffix leads the next group. Calls that are already queued when a successor pulls an arrival join the same serial worker job instead of separate blocking dispatches. The window deadline, encoded-byte limit, deployment barrier and lag gate are unchanged. Fixed mode keeps its exact thresholds.

In a matched instrumented run, mean queue wait fell from 26.5 to 16.2 ms and the median from 22.7 to 9.2 ms. Groups grew from 97 to 122 requests, and log appends fell 26% (21,988 → 16,198 in 30 seconds). Early `preparation_time` stops fell from 19.4% to 1.4% of groups. Per-flush latency in that run was higher (17.3 ms) despite fewer flushes; the device's flush latency drifts between runs, which dominates run-to-run variance.

## Measurements

Apple M5 Pro, 18 logical CPUs, Darwin 27.2.0, Node 26.9.0, release build with thin LTO. Both servers used the same frozen customer driver and the unchanged stress preset: eight three-replica groups, 256 loops/group, 50 ms batch ceiling, leader failure in every group, 60 measured seconds. Runs alternated ABBA, and each waited until the disk was idle for three seconds. Every run passed all eight audits with zero failed customer calls.

| Pair | Before, calls/sec | After, calls/sec | Before read/write p99 ms | After read/write p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 1 | 54,166 | 64,236 | 10.8 / 278.6 | 8.7 / 242.3 |
| 2 | 56,664 | 61,238 | 10.1 / 242.3 | 16.5 / 239.9 |
| 3 | 61,347 | 73,020 | 23.2 / 206.7 | 34.8 / 174.5 |
| 4 | 55,366 | 69,999 | 18.4 / 265.0 | 16.0 / 210.8 |

Pooled histograms across the four runs of each server:

| Percentile | Before read | After read | Before write | After write |
| --- | ---: | ---: | ---: | ---: |
| p50 | 0.9 ms | 1.1 ms | 93.2 ms | 75.7 ms |
| p95 | 5.3 ms | 7.4 ms | 174.5 ms | 151.8 ms |
| p99 | 16.2 ms | 22.0 ms | 252.2 ms | 217.2 ms |
| p99.9 | 102.0 ms | 87.0 ms | 857.5 ms | 815.9 ms |

Open-loop runs at 5,000 arrivals/sec/group for 30 seconds, without leader failure, isolate efficiency from closed-loop demand. Both servers completed about 38,000–39,000 calls/sec:

| Run | Read p99 | Write p50 | Write p95 | Write p99 | Driver drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before 1 | 20.1 ms | 60.8 ms | 136.1 ms | 244.8 ms | 2.8% |
| After 1 | 10.9 ms | 51.8 ms | 97.0 ms | 174.5 ms | 1.5% |
| After 2 | 14.6 ms | 51.8 ms | 93.2 ms | 159.6 ms | 0.8% |
| Before 2 | 21.4 ms | 60.2 ms | 122.0 ms | 223.8 ms | 2.1% |

Arrival bursts still exceeded the 256 driver slots occasionally, so this is not exactly fixed work. Slow sampled reads spent about 95% of their server time in `body_receive`, and server-side read tails barely changed between builds. Each group's customer driver is single-threaded with one HTTP/2 connection per replica. A durable group's replies release its callers together, so larger groups also mean larger synchronized bursts.

Exploratory 30-second screens without leader failure varied from 62k to 83k calls/sec for the same binary. Single pairs are not reliable at this noise level.

## Other findings

**Leader and follower flushes run back to back.** OpenRaft 0.9.25 queues `AppendInputEntries` followed by one `Replicate` per follower, and `RaftCore::append_to_log` awaits the leader's flush callback before running them. Followers therefore receive entries only after the leader's fsync, and the flush overlap described in [LATENCY.md](LATENCY.md) cannot occur: log readers can see the pending suffix, but replication is not asked for it yet. A patched OpenRaft that submits the leader's append, replicates immediately and counts the leader toward the quorum only after its flush reduced the fastest write from about 18 ms to 4.5 ms. On this shared SSD the extra concurrent flushes raised mean flush latency from 12.0 to 20.2 ms at the same flush rate, and read p99 rose to 48–77 ms. It was not adopted then. Separate disks per replica would change that tradeoff.

A later change adopted the patch with the leader's flush deferred rather than concurrent (see [ARCHITECTURE.md](ARCHITECTURE.md)): followers alone commit, and the leader flushes its held appends once per grace period or interval, so the flush rate falls instead of rising. Applied state also moved off the commit path. On the 24-replica stress host, two alternating pairs measured 74.0k → 85.1k and 73.6k → 82.0k calls/sec (+11–15%), with write p50 72 → 55–58 ms and write p99 unchanged at about 180 ms. Read p99 rose from 6–7 ms to 34–35 ms, and servers used 10.1–10.2 cores against 8.3. The cause of that read tail was not isolated; HTTP body receipt p99 rose from 3.3 to 19.7 ms, which points at host CPU or runtime scheduling rather than storage.

**Window restarts leave the commit stage idle.** Every 250 ms the pipeline drains for maintenance and other writer-lock users, then prepares its next group with nothing committing, a gap of about 20 ms. About 13% of groups started a window. A one-second window screened inconclusively, and the window also bounds how long keys, retention, transaction, staged-deployment and partition administration wait for the writer lock. It is unchanged.

**Per-call dispatch costs more than serial jobs.** Evaluation dispatched to a fresh blocking task averaged 0.39 ms, versus 0.11 ms inline in a serial worker job, which reuses a recycled Wasm instance. Batching ready arrivals into one job addresses part of this.

## Reproduction

Diagnostics used [profile-otel.mjs](profile-otel.mjs) and temporary DEBUG timestamps buffered in memory; synchronous logging to the same saturated SSD delayed the writer by up to 10 ms and was discarded. The compared servers were built from the parent commit and from this change.

```sh
export FLOWER_OTEL_ENABLED=0 OTEL_SDK_DISABLED=true
node bench/stress.mjs --binary /path/to/before-or-after \
  --driver-binary /path/to/shared-driver --json /tmp/unique-run.json

# Equal offered load, without leader failure.
export FLOWER_WRITER_BATCH_MODE=adaptive FLOWER_WRITER_BATCH_MS=50
export FLOWER_WRITER_QUEUE_CAPACITY=1024 TOKIO_WORKER_THREADS=2
export FLOWER_QUERY_WORKERS=16 FLOWER_PREPARATION_WORKERS=16
export FLOWER_WRITER_PREPARATION_WORKERS=1
node bench/goblin-pizza.mjs --binary /path/to/before-or-after \
  --driver-binary /path/to/shared-driver --groups 8 --http2 --duration 30 \
  --concurrency 256 --workers 4 --max-orders 96 --offered-rate 5000 \
  --json /tmp/unique-offered-run.json
```

The writer unit suite includes a regression test in which a learned one-call target would previously have closed the successor; it now extends that group with later arrivals and commits one successor after the predecessor completes.
