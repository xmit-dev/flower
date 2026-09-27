# Goblin Pizza stress benchmark

The benchmark exercises tenant-scoped stores, reactive summaries, scheduled baking, leased delivery work, retries, and leader failure. Customer load runs in Rust. Node creates the local clusters, runs delivery workers and faults, audits acknowledged effects, and generates reports. Flower itself remains Rust + QuickJS-NG in Wasmtime.

```sh
npm ci
cargo build --release --locked --bin flower --bin flower-bench-driver
npm run bench:stress
```

The application runs as either guest. By default it is [the TypeScript bundle](../examples/goblin-pizza-ts/goblin-pizza.ts) on QuickJS. `--guest wasm` deploys [its Rust port](../examples/goblin-pizza-rs), built with the [`flower-sdk`](../crates/flower-sdk) crate into a WebAssembly module that implements [GUEST_ABI.md](../GUEST_ABI.md) directly. The port declares the same manifest, makes the same host calls in the same order and writes the same records, so the customer driver, workers and audit are unchanged. Build it inside `nix develop`, which provides the `wasm32-unknown-unknown` target:

```sh
cargo build --release --locked -p goblin-pizza --target wasm32-unknown-unknown
npm run bench:stress -- --guest wasm
```

`bin/bench` builds everything, runs the stress preset once per guest and publishes both results. The ignored `guest_parity` test in [src/evaluator/guest_parity_tests.rs](../src/evaluator/guest_parity_tests.rs) drives both guests through the production evaluator at fixed clocks and requires identical results, failures, stored records, query validity and read certificates at every step:

```sh
node bench/guests.mjs /tmp/goblin
FLOWER_GOBLIN_BUNDLE=/tmp/goblin/goblin-pizza.js FLOWER_GOBLIN_WASM=/tmp/goblin/goblin-pizza.wasm \
  cargo test --release --lib guest_parity -- --ignored --nocapture
```

Use `nix develop --command cargo …` if Rust is supplied by the development shell. The stress preset starts **eight independent three-replica groups** on this machine, served by three host processes: host *k* runs replica *k* of every group over one shared database (`--hosted`, below), so the groups share each fsync. Per group it runs 256 customer loops and four workers, two tenants with four stores each, a 96-order cap, one second of warmup, and 60 seconds of measured load. Halfway through, the host leading the most groups is killed, as a machine failure would take down its replica of every group; it restarts once every group serves again. Work drains afterward and every group receives an independent audit. The base command without `--hosted` runs one process and database per replica instead; see [HOSTING.md](HOSTING.md) for both measured.

The preset sets adaptive batching with a **50 ms preparation ceiling**, a 1,024-request writer queue, 16 authorization/cache-probe slots, and 16 shared preparation slots per server. Writer preparation is serial because this workload repeatedly updates conflicting hot stores; independent groups and reads remain parallel. Each server uses two Tokio async workers because all 24 replicas share this host; QuickJS preparation and storage use separate blocking workers. With `--hosted`, a host process serves one replica of every group, so the preset multiplies its process-wide settings (async workers, `FLOWER_WASM_POOL_SLOTS`, `FLOWER_WASM_RECYCLE_BYTES`) by the number of groups, keeping each replica's share the same; query and preparation slots are already per replica. Existing environment variables take precedence. These are scheduling/resource settings, not end-to-end latency guarantees or recommendations for every production topology.

## Results and publication

The retained TypeScript result is [results/latest.html](results/latest.html), with [raw measurements](results/latest.json) and each group's JSON/HTML under `results/latest-groups/`; the Rust guest's run uses `results/latest-wasm.*` and `results/latest-wasm-groups/`. [READS.md](READS.md) summarizes a run; [ARCHITECTURE.md](ARCHITECTURE.md) covers the architecture and [LIMITS.md](LIMITS.md) lists capacity controls. Keep experimental output outside the retained results directory, for example under `/tmp/`.

```sh
node scripts/publish-bench-results.mjs bench/results/latest.json bench/results/latest-wasm.json
node scripts/publish-bench-results.mjs --check
```

Publication validates and retains compact measurement JSON under `docs/bench/`, at `latest.json` and `latest-wasm.json` by the guest each report records; commit those source files. Without arguments, it publishes every guest's run found in `bench/results/`, and `--check` verifies every retained one. The homepage and handbook summaries lead with the TypeScript run and compare the guests beneath it. The site generator renders the reports and homepage/handbook summaries into `_site/` from that data. Use `bin/web-preview` for live preview or `bin/web-deploy` to build and publish. The check validates the retained measurements offline; it does not rerun the workload.

## Workload and accounting

[The application](../examples/goblin-pizza-ts/goblin-pizza.ts) uses composite `[tenant, store]` identities and store-local order IDs. Its durable equality index and incremental reducers update order statistics in proportion to changed orders. Rankings and delivery queue scopes are per tenant. Tenant assignment to groups is static; this workload exercises no migration, cross-group transaction, global leaderboard, or global atomic snapshot. Logical tenant separation in this unauthenticated demo is not authorization.

Initially customer choices are 30% orders, 40% reads, and 30% tips. After the order cap, order choices become reads: approximately **70% reads / 30% durable mutations**. Tenants are selected uniformly. By default 80% of store choices use one hot store, with the rest uniform across four stores, so that store receives approximately 85% of its tenant's calls.

Customer previews use replica-local committed snapshots and may lag. Reads rotate across all three replicas over pooled HTTP/2; `--read-consistency fresh` selects the quorum-confirmed method. Mutations use the leader. The final audit always uses fresh reads. Rust opens its own multiplexed connection per origin before measurement; Node workers use a separate pool. Concurrency bounds outstanding operations, not connections.

Drones deliberately abandon some leases and later reclaim the work. Audits reconcile stock, revenue, tips, acknowledged orders, source-derived summaries, and tenant rankings; they also check exact receipt replay and rejection of expired fencing tokens. Uncertain retries preserve request IDs and bodies. Failed audits make the command exit nonzero; throughput has no pass threshold.

**Customer goodput** counts successful primary logical calls, excluding worker traffic, retries, explicit replay probes, errors, warmup, and drain. Aggregate goodput divides completed work by the union of synchronized load intervals, including final in-flight customer tails. Percentiles merge histogram buckets rather than averaging group percentiles. Rust sends disjoint metric intervals to the controller; one-second charts can show interval delivery artifacts. Reports retain server/driver binary hashes, runtime settings, and separate sampled CPU/RSS for servers, Rust drivers, and Node controllers. Sampling coverage and whole-run CPU have different scopes.

## Custom runs

Use the base command to change preset flags; duplicate flags are rejected. It uses the same Rust driver, with ordinary server defaults unless the environment specifies otherwise.

```sh
npm run bench -- --groups 8 --http2 --duration 20 --concurrency 512 \
  --workers 4 --max-orders 96 --json /tmp/flower-custom.json
npm run bench -- --nodes 1 --duration 5 --max-orders 8 --json /tmp/flower-smoke.json
npm run bench -- --help
node tests/e2e-rust-driver.mjs
```

`--groups` counts independent Raft groups and `--nodes` counts replicas per group (one or three). Tenants, customers, workers, and order caps are per group; stores are per tenant.

By default every replica is its own server process with its own database, so the stress preset runs 24 processes that flush one disk independently. `--hosted` instead starts one process per replica slot: host *k* serves replica *k* of every group (`flower --replica`) over one shared database, so all of its groups' log appends share each fsync. `--chaos` then kills the host that leads the most groups halfway through the load, which every group observes at once like a machine failure; the host restarts once every group serves again. Server CPU is sampled per host by the coordinator and reported in the aggregate `hosts` field, which `compare-cpu.mjs` uses instead of per-group server counters. Group *g* is initialized through host *g* mod 3, whose replica wins the first election, so each host starts out leading an equal share of groups.

```sh
npm run bench -- --groups 8 --http2 --hosted --chaos
```

Databases go in a fresh directory under the system temporary directory. `--data DIR[,DIR…]` creates one under each named directory instead, and servers (hosts, with `--hosted`) take them in turn: `--data /tmp,/Volumes/Data,/tmp` puts the second host on another disk. Every directory is removed afterward unless `--keep-data` is given.

```sh
npm run bench:stress -- --data /tmp,/Volumes/Data,/tmp
``` `--driver-binary PATH` selects the Rust executable, defaulting to `target/release/flower-bench-driver`. The driver E2E checks real HTTP/2 pooling, replica routing, uncertain retries, receipt replay, histograms, and business accounting against a test server.

By default each customer waits for its response before issuing more work: this is a closed-loop capacity measurement. Independent arrivals expose overload differently:

```sh
# 6,000 arrivals/sec/group, independent of service time; 48,000/sec in total.
npm run bench:stress -- --offered-rate 6000 --json /tmp/flower-offered.json
```

Open-loop mode retains the concurrency bound. If all driver slots are occupied, arrivals are counted as **driver drops**, not silently queued or delayed. Reports distinguish offered, dispatched, completed, failed, and dropped arrivals. Logical latency starts at the intended arrival time and includes scheduling lag and retries; dropped arrivals receive no fabricated latency. Compare drops and tail latency alongside goodput. `--offered-rate 0` restores closed-loop mode.

Normal completion and interruption stop owned processes and remove temporary databases; `--keep-data` preserves database directories. Reports preserve failures. All local replicas share CPU, memory, and storage, so increasing groups or queues can increase contention and latency. This small bounded workload does not establish large-dataset, WAN, authenticated-query, watch-heavy, or distributed-transaction capacity.

## Profiling

### OpenTelemetry reporting

See [TELEMETRY.md](../TELEMETRY.md) for server configuration, instrumentation coverage, and timing semantics. The OpenTelemetry profiler runs the same workload with sampled traces and unsampled metric histograms from the actual Flower processes. It starts a bounded local OTLP/HTTP JSON receiver, needs no collector service, and writes a separate diagnostic. Build the binary after changing instrumentation:

```sh
cargo build --release --locked --bin flower --bin flower-bench-driver
FLOWER_WRITER_BATCH_MODE=adaptive FLOWER_WRITER_BATCH_MS=200 \
TOKIO_WORKER_THREADS=2 FLOWER_WRITER_QUEUE_CAPACITY=1024 FLOWER_QUERY_WORKERS=16 \
FLOWER_PREPARATION_WORKERS=16 FLOWER_WRITER_PREPARATION_WORKERS=1 \
FLOWER_BENCH_OTEL_MAX_RECORDS=250000 FLOWER_BENCH_OTEL_MAX_BYTES=268435456 \
node bench/profile-otel.mjs --groups 8 --http2 --duration 20 --concurrency 512 \
  --workers 4 --max-orders 96 --chaos --json /tmp/flower-otel-run.json
```

The command accepts the ordinary benchmark flags. Without `--json`, it chooses a fresh temporary directory. Explicit JSON/HTML paths must be unused and outside `bench/results/` and `docs/`; diagnostics never replace the retained capacity result. The ordinary benchmark JSON/HTML is accompanied by `<json-stem>-otel/report.html`, `summary.json`, and `capture.ndjson`. The HTML compares unsampled per-node stage histograms with sampled span durations and identifies the slowest sampled spans. Trace, parent, and link IDs connect request, writer, and durability work; a batch can link several request traces. Resource group index, address, node ID, and PID distinguish replicas and restarts (node IDs repeat between independent groups).

Traces default to `OTEL_TRACES_SAMPLER=parentbased_traceidratio` with `OTEL_TRACES_SAMPLER_ARG=0.01`. Increase the ratio for a short diagnostic when needed; full tracing at stress throughput can overwhelm export queues and alter latency. Metrics and trace batches export every second by default; `OTEL_METRIC_EXPORT_INTERVAL` and `OTEL_BSP_SCHEDULE_DELAY` can override those intervals. Use runs long enough to contain multiple metric exports. The profiler sets local endpoints, HTTP/JSON protocol, and a unique run resource tag; it removes preexisting exporter headers/endpoints and resource attributes for this run, then restores the controller environment afterward. It never needs credentials or contacts an external collector.

Capture defaults to at most 100,000 sanitized span/metric records and 64 MiB of encoded data. `FLOWER_BENCH_OTEL_MAX_RECORDS` and `FLOWER_BENCH_OTEL_MAX_BYTES` accept positive integers to configure explicit finite limits, capped at 1,000,000 records and 512 MiB respectively. The eight-group example raises these to 250,000 records and 256 MiB because every replica exports many cumulative metric series each second. The report records the effective bounds; exceeding them still fails the diagnostic. Larger captures consume more controller memory and disk and can further perturb the workload. Requests are limited to 4 MiB, with at most eight being read concurrently. Known node identity, stage labels, numeric timing attributes, and bounded span links are retained; bodies, headers, arguments, exception messages, arbitrary resource attributes, and span events are omitted. Reports show received, retained, rejected, malformed, and dropped counts, SDK span drop counters, and resource identities. Missing signals, stale report windows, unsupported metric types, or incomplete local capture fail the diagnostic independently of the business audit. Exporter queue loss, failed transmissions, and data lost in the intentional leader crash are unknown to the receiver and are explicitly reported as unavailable.

Span summaries include only spans wholly inside the measured load interval. Cumulative metrics are differenced between adjacent exports inside that interval, within the same process and instrument epoch; they exclude edge intervals and resets. Histogram percentiles are bucket upper bounds in the reported unit. These server observations include delivery workers, retries, and other traffic excluded from customer goodput. Nested stages and concurrent batches overlap, so their durations cannot be summed as wall time or CPU utilization. Collection, serialization, and instrumentation perturb throughput: use a separate uninstrumented run for capacity comparisons.

Outside the local profiler, Flower exports nothing unless `FLOWER_OTEL_ENABLED=1` and `OTEL_SDK_DISABLED` is not `true`. To send OTLP to an existing collector, configure the standard exporter variables:

```sh
FLOWER_OTEL_ENABLED=1 OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.01 \
  target/release/flower --id 1 --listen 127.0.0.1:8080 --data /tmp/flower-otel-server
```

Both `http/protobuf` and `http/json` are supported. Standard signal-specific endpoint, protocol, and header settings apply to ordinary server operation; endpoint credentials, headers, and resource attributes are excluded from benchmark runtime metadata. This standalone example is separate from the local benchmark receiver.

### Legacy mixed-workload tracing

The mixed-workload profiler accepts the ordinary benchmark flags for one or multiple groups. It captures the actual server processes and emits an additional `*-groups.json` containing batch timing and bounded trace records. Enable storage and evaluator stages explicitly:

```sh
FLOWER_WRITER_BATCH_MODE=adaptive FLOWER_WRITER_BATCH_MS=200 \
TOKIO_WORKER_THREADS=2 FLOWER_WRITER_QUEUE_CAPACITY=1024 FLOWER_QUERY_WORKERS=16 \
FLOWER_PREPARATION_WORKERS=16 FLOWER_WRITER_PREPARATION_WORKERS=1 \
FLOWER_PROFILE_STORAGE=1 FLOWER_PROFILE_EVALUATOR=1 \
node bench/profile-mixed.mjs --groups 8 --http2 --duration 20 --concurrency 512 \
  --workers 4 --max-orders 96 --json /tmp/flower-mixed-profile.json
```

Preparation, successor work, and durability overlap; their wall times are not additive CPU percentages. `serial_worker_jobs` and `serial_worker_requests` show how many serial calls share a blocking dispatch. Inspect capture failures and dropped records. Tracing perturbs performance, so run without it on a quiet host for capacity claims.

On macOS, native stack sampling is available for a single group's initial leader:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release --locked --bin flower --bin flower-bench-driver
npm run bench -- --http2 --duration 30 --concurrency 128 --workers 4 --max-orders 96 \
  --cpu-profile /tmp/flower-cpu.sample.txt --json /tmp/flower-cpu.json
```

Sampling requests up to ten seconds at one-millisecond intervals and stays on the original PID if leadership changes. Reports include the raw sample path, timing/status, and stack summaries. Stack observations are not CPU utilization or TypeScript source attribution; blocking classification is approximate. Requested profiling failures make the run fail independently of the business audit. Preserve the matching binary for symbol lookup and keep builds, tests, and profilers out of uninstrumented capacity windows.

### Cold bundle preparation bursts

Measure first-use compilation separately from the mixed workload. This diagnostic deploys a static SDK bundle to a fresh three-node cluster, waits for a follower to apply its metadata without evaluating it, and sends 16 distinct replica-local queries over one preconnected HTTP/2 session. Unique arguments bypass result-cache coalescing; every response must retain its own arguments and pristine guest state.

```sh
node bench/profile-cold-burst.mjs /tmp/before-flower /tmp/cold-before
node bench/profile-cold-burst.mjs target/release/flower /tmp/cold-after
```

Output directories must be new. Each contains `result.json`, the exact bundle, and native Wasm maps. Compare identical bundle hashes and settings: the map delta counts successfully compiled native bundle images, while `ps` process CPU and individual response timings cover the burst. The harness sets 16 query/preparation workers (an optional third argument changes both), two Tokio workers, and a 30-second evaluation budget for both binaries so compilation is observable without the default deadline truncating it. OTEL and other profilers are disabled. These are isolated cold-path diagnostics, not capacity measurements; process CPU includes ordinary Raft background work, and incomplete compilation attempts do not produce native maps.

### Comparing CPU cost

Use a separate uninstrumented run at the same fixed offered rate, with identical workload, runtime settings, customer-driver binary, and host. Pick a rate both versions serve without driver drops or failed customer calls, then repeat both versions to distinguish changes from run variation. For example, append `--offered-rate 5000` to otherwise matching benchmark commands; this is **per group**, not a capacity recommendation.

```sh
node bench/compare-cpu.mjs /tmp/before.json /tmp/after.json
node bench/compare-cpu.mjs --json /tmp/before.json /tmp/after.json > /tmp/cpu-comparison.json
```

The comparison accepts single-group and multi-group reports. It displays observed server and driver CPU seconds, per-process load coverage, and estimated CPU microseconds per successful primary customer call. The JSON includes every process's measurements. Server CPU includes replication, retries, delivery work, and background activity; customer goodput excludes those operations. Node controllers and Rust customer drivers are reported separately; the top-level controller, profiling tools, and OTEL collector are outside those driver counters.

CPU counters are sampled once per second. Only intervals entirely in load with an unchanged PID are counted; edges, restarts, and missing samples reduce coverage. The per-call estimate extrapolates each process's sampled CPU/wall ratio across its group's complete load duration before dividing by successful primary customer calls. This assumes missing intervals resemble measured intervals, which can fail around cold starts and crashes. Coverage remains visible and missing process counters make the total estimate unavailable. The tool calls a comparison matched fixed work only when workload/environment identities match, audits pass, both versions serve every offered call, and successful primary counts match. Coverage gaps remain a limitation even then; closed-loop comparisons are explicitly exploratory.

### Recorded on-CPU stacks with Instruments

macOS `sample` captures all-thread wall stacks, including waits. Instruments Time Profiler records sample state and weight. Capture an owned server PID during a separate diagnostic workload, then export only its `time-profile` table:

```sh
flower_pid=12345 # Replace with the server PID from this diagnostic run.
xcrun xctrace record --template 'Time Profiler' --attach "$flower_pid" \
  --time-limit 10s --output /tmp/flower-cpu.trace
xcrun xctrace export --input /tmp/flower-cpu.trace \
  --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]' \
  --output /tmp/flower-time-profile.xml
python3 scripts/profile-cpu.py /tmp/flower-time-profile.xml \
  --output /tmp/flower-oncpu.json
```

Use fresh output paths and preserve the matching binary for native symbol lookup. The analyzer invokes `xcrun llvm-cxxfilt` when available. For guest C names, build the matching [QuickJS symbol sidecar](../vendor/quickjs-ng/README.md#name-guest-functions-in-native-profiles) before measurement and launch the diagnostic server with a fresh `FLOWER_PROFILE_WASM_MAP=/tmp/flower-native` prefix. Then add the sidecar and that server's map:

```sh
python3 scripts/profile-cpu.py /tmp/flower-time-profile.xml \
  --symbols /tmp/guest-symbols.json --native-map "/tmp/flower-native.$flower_pid.jsonl" \
  --output /tmp/flower-oncpu.json
```

Only rows explicitly recorded as `Running` contribute to on-CPU rankings. Other states, missing stacks, incomplete frame references, and unknown symbols remain visible. Statistical sample weights are not exact process CPU counters or wall duration; a wait-named syscall can have a recorded Running sample. Self counts and nearest directly owned Flower frames partition the available exported stacks. Inclusive counts deduplicate recursive frames within each sample but overlap across functions and must not be summed. Percentages use all Running weight, including missing stacks. Guest maps apply only to their recorded PID and matching guest hash; ambiguous reused addresses remain unresolved.

Inputs are bounded to 64 MiB of XML, one million XML elements, 100,000 sample rows and 512 frames per stack. The analyzer accepts only a direct `time-profile` export; it never reads the full trace or its table of contents. **Raw `.trace` files and `--toc` exports can contain inherited environment values. Keep them local and share the analyzed JSON instead.** Profile leaders and followers separately, and keep profiling outside the uninstrumented CPU-efficiency comparison windows.
