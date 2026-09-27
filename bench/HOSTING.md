# Shared host databases, 2026-09-25

The stress preset's 24 replicas used to be 24 processes, each with its own redb file. Every follower flushed its own file once per replication round, so about 16 processes issued `F_FULLFSYNC` against one SSD at a time. [GOODPUT.md](GOODPUT.md) measured what that costs: one flusher completes a flush in about 3 ms, 16 concurrent flushers in about 15 ms each. Writing less per flush would not help, since flush cost barely changes between 4 and 128 KiB.

A process can now host replicas of several groups (`flower --data DIR --replica NAME,ID,LISTEN …`). They share one database under per-replica table prefixes, and their log appends and applied-state writes commit together: one transaction per batch, flushed once when anything in it must be durable. The benchmark's `--hosted` mode runs one such process per replica slot: host *k* serves replica *k* of every group.

## Two changes

**Group commit.** Writes from every replica in the process queue in one committer. Each batch stages every queued write into one transaction and commits it, with Immediate durability when any write in it needs it. A write that fails while staging aborts only that attempt; the others are staged again. Votes, truncation, purges and snapshots keep their own transactions.

**Pipelined appends.** A replica used to hold its storage order guard until its append committed. On a shared database the next leader append then waited for other groups' flushes before its entries could even be published for replication. That raised median write latency by about 20 ms. Batches commit in submission order, so a replica now releases the guard once its append is queued. Log readers merge every queued append with committed ones, and direct transactions first wait for the replica's queued writes to commit. This applies to single-replica databases too.

## Measurements

Apple M5 Pro, 18 logical CPUs. Stress preset settings: eight groups of three replicas, 256 customer loops and four drones per group, 50 ms batch ceiling, serial writer preparation, HTTP/2. Hosts get the same per-replica share of process-wide pools: a host serving eight replicas has 16 async workers, 2,048 Wasm pool slots and 768 MiB of recyclable instances, eight times a lone replica's. Query and preparation slots are already per replica. Every run passed all audits. Runs alternated, and each waited 30 s after the previous one.

Thirty seconds without a crash, so every process's counters cover the whole window:

| Topology | Calls/s | Write p50 / p99 | Read p99 | Fsyncs/s | Writes per batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| 24 processes | 91,205 | 48.3 / 202.6 ms | 16.8 ms | 593 | 1.0 |
| 3 hosts, before pipelining | 82,963 | 69.2 / 141.6 ms | 8.1 ms | 224 | 3.2 |
| 3 hosts, pipelined | 105,391 | 48.3 / 113.8 ms | 27.1 ms | 206 | 6.3 |

A follower's durable commit took 18.3 ms in its own file, and 9.4 ms on a host holding mostly followers, where one flush served several groups. The host committers that held mostly followers were 94–99% busy; nearly all of that time was redb's commit (page writes and the flush).

The full stress preset (60 s, host or leader crash halfway), pipelined build, alternating:

| Run | Calls/s | Write p50 / p99 | Read p99 | Server CPU / call | Fsyncs/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 24 processes | 94,713 | 53.9 / 140.2 ms | 23.4 ms | 118 µs | 461 |
| 3 hosts | 103,867 | 49.3 / 113.8 ms | 20.1 ms | 113 µs | 162 |
| 3 hosts, 4 groups × 4 tenants | 90,889 | 60.8 / 119.6 ms | 9.1 ms | 113 µs | 171 |
| 3 hosts | 104,923 | 48.8 / 118.4 ms | 19.9 ms | 111 µs | 163 |
| 24 processes | 92,012 | 55.6 / 138.8 ms | 25.1 ms | 117 µs | 460 |

Fsyncs per second here count only processes that ran through the whole window; the crashed and restarted process is excluded in both topologies. Before pipelining, the same preset measured 78,369–91,880 calls/s with write p99 of 189–215 ms on 24 processes, and 80,456–86,612 with write p99 of 128–140 ms on hosts.

**Fewer, larger groups do not help once flushes are shared.** Each group has one serial writer lane, so concentrating tenants in fewer groups trades flush contention for write serialization. With one process per replica and the preset's totals (16 tenants, 2,048 customer loops), four groups reached 83,722 calls/s and two groups 57,458, with write p50 rising from 65 to 100 ms. On hosts, four groups measured 90,889 against 104,000 for eight.

**Leader placement.** These runs initialized every group through host 1, whose replicas won the first elections, so host 1 led every group until the crash and the other two hosts carried every durable follower flush. The harness now rotates the initializing host, so each starts out leading an equal share. In single open-loop screens at 100,000 arrivals/s (JavaScript guest, no crash, about 16 tenants and 8,192 driver slots in total), eight groups with every leader on host 1 served 99,289 calls/s, dropping 0.45% of arrivals. With rotated leaders, host CPU evened out (4.3, 4.3 and 3.6 cores), but all three committers ran 98–99% busy, and eight groups served 95,949–98,253 calls/s (1.5–3.8% dropped) whether the data sat on the internal SSD, an external SSD, or both. Four groups, screened only with every leader on host 1, dropped 23% of arrivals; with either placement, 12–24 groups served 87,249–94,502 and dropped 5–13%. At this rate the servers and drivers used about 15 of the 18 CPUs.

## Limits

- A host's committer is one writer: redb holds its write lock through the flush, so a busy host serializes its replicas' commits. It was nearly saturated in these runs. Running more host processes per machine trades fsyncs for commit parallelism.
- Snapshot installation and other direct transactions wait for the current batch, and the next batch waits for them, so a large installation briefly delays the host's other groups.
- A host crash takes down its replica of every group at once. Each group keeps a quorum on the other hosts, but groups whose leader was on that host all elect together.
- These are single-machine measurements on one SSD. With a disk per replica, separate processes would not contend for flushes; hosting still removes redundant flushes of colocated groups.

## Reproduction

```sh
cargo build --release --locked --bin flower --bin flower-bench-driver
npm run bench:stress                    # 3 hosts (the preset)
FLOWER_WRITER_BATCH_MODE=adaptive FLOWER_WRITER_BATCH_MS=50 FLOWER_WRITER_QUEUE_CAPACITY=1024 \
FLOWER_QUERY_WORKERS=16 FLOWER_PREPARATION_WORKERS=16 FLOWER_WRITER_PREPARATION_WORKERS=1 \
TOKIO_WORKER_THREADS=2 npm run bench -- --groups 8 --http2 --duration 60 --concurrency 256 \
  --workers 4 --max-orders 96 --chaos --json /tmp/flower-separate.json   # 24 processes
node bench/compare-cpu.mjs /tmp/flower-separate.json bench/results/latest.json
```

Reports record each process's batched commits (`storage`, or `hosts.storage` for hosted runs): batches, durable batches, staged writes and committer busy time per second, from `/admin/resources`.
