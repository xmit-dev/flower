# OpenTelemetry diagnostics

Flower can export sampled traces and unsampled metrics over OTLP/HTTP. Reporting
is off by default. Enable it explicitly on each server:

```sh
export FLOWER_OTEL_ENABLED=1
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_SERVICE_NAME=flower
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.01
export OTEL_METRIC_EXPORT_INTERVAL=1000
# Start flower with the usual node, data directory, and credential settings.
```

The default transport is `http/protobuf`; `OTEL_EXPORTER_OTLP_PROTOCOL=http/json`
also works. gRPC is not supported. Standard signal-specific trace/metric
endpoints, protocols, headers and timeouts are supported by the OTLP exporter.
Base endpoints receive `/v1/traces` and `/v1/metrics`; signal-specific endpoints
are complete URLs. TLS uses the HTTP client's certificate validation. Configure
collector credentials with `OTEL_EXPORTER_OTLP_HEADERS`, outside application
configuration and benchmark reports.

`OTEL_SDK_DISABLED=true` overrides the Flower enable switch. Trace samplers
`always_on`, `always_off`, `traceidratio`, and their `parentbased_` variants are
supported. The default is parent-based 1% sampling, including when no sampling
variables are set. Ratio arguments must be finite numbers from zero to one.
Metrics are independent of trace sampling. `always_off` gives metrics without
exporting spans. Remote sampled trace parents are honored by parent-based
sampling, so that percentage is the root sampling probability, not a hard cap.

Each resource includes service name/version, a unique service instance
(`listen/node/pid`), node ID, listen address, and process PID. Custom resource
attributes may be supplied through `OTEL_RESOURCE_ATTRIBUTES`. Do not put secrets
in resource attributes: they are intentionally exported. The benchmark capture
command supplies its own run and group identities.

## What the measurements cover

| Area | Traces and metrics |
| --- | --- |
| HTTP | Request count through duration histogram counts, active handlers, matched route, method, response status and outcome; body reception and application JSON extraction stages; duration ends when response headers are ready |
| Queries | Encoded/value cache hits, misses, coalesced waits, admission, authorization, fresh snapshots, blocking worker queue and evaluation |
| Writer | Submission through response, queue wait and rejection, admission, preparation, serial worker batches, speculation, staging, batch fill/read/commit, batch sizes and bytes, duplicates, errors, deferred work and replication lag |
| Evaluator | Invocation, bundle preparation, manifest, result validation, coordinator execution/read waits, nested cell execution/load/reset, cell creation/reuse, nesting, memory initialized/reset |
| Cold bundle preparation | One preparation per exact bundle content, coalesced wait, QuickJS initialization, bytecode compilation, static initialization, snapshot preparation/capture, native compilation |
| Storage | State lock, I/O queue, blocking pool queue, preparation, transaction begin/write/encoding/commit/publication; entries, commands, changed keys, receipts and bytes |
| Raft transport | Append, vote, snapshot and read-fence RPC durations and outcomes, including cancelled requests |

Metric families are `flower.http.*`, `flower.query.*`, `flower.writer.*`,
`flower.evaluator.*`, `flower.storage.*`, and `flower.raft.*`. Durations use
seconds with explicit buckets covering microseconds through seconds. Metric
dimensions use bounded operation/stage/outcome classes, not customer identities
or method names. Group and node identity belong to resource attributes.

W3C `traceparent`/`tracestate` are extracted at HTTP ingress and propagated on
leader-forwarding and Raft RPCs. Blocking evaluation and storage work retain
their span context. A writer batch has links to contributing request spans,
since many unrelated calls share one batch. Actor-driven Raft/storage work can
start independent traces; it is not falsely attributed to a single caller.
Sampling can leave either side of a link absent from a capture. SDK span/link
limits bound retained context; exported dropped counts expose truncation.

Only dedicated `flower::otel` spans are exported. Ordinary logs retain the
independent `RUST_LOG` filter. Request bodies, arguments, results, credentials,
request IDs, arbitrary URL paths/query strings, baggage and error messages are
excluded. Resolved method names can appear on sampled writer spans, but never
as metric labels. Error outcomes use bounded classes.

## Reading the timing breakdown

Measurements are wall time, not CPU profiles. Writer preparation can overlap a
predecessor's commit; batch/request spans and nested evaluator stages overlap.
Do not add their totals as if they were exclusive execution costs. Cell stage
metrics are per-invocation sums; their histogram percentiles are not percentiles
of individual cell calls. Query `blocking_evaluation` includes blocking queue
time, also reported separately as `blocking_queue`.

`flower.evaluator.bundle.preparation.duration` separates cold bundle work by
`stage`. Its `total` includes `initialize`, `bytecode_compile`,
`static_initialize`, `snapshot_prepare`, `snapshot`, and `native_compile` where
applicable. `coalesced_wait` measures callers waiting for the same bundle's
preparation; it is not repeated compilation. Each waiter retains its own
deadline, and a failed preparer wakes waiters to retry with their own limits.
Ordinary prepared-image hits do not emit these cold-stage measurements.

Storage `commit` measures redb transaction commit. `durability=immediate`
includes its durability flush; `durability=none` identifies deferred commits
and must not be reported as an fsync. `apply` publishes applied state in memory
and no longer commits; `persist` writes queued applied states in the
background. A leader of three or more voters records its own log appends as
`leader_append` and makes them durable in `leader_flush`, one immediate commit
per grace period or interval; followers still flush every `append`. `build_snapshot` now measures
capture plus a metadata-only checkpoint and immediate flush; it does not encode
or rewrite the complete state. `snapshot_transfer`, with `durability=none`,
measures lazy JSON encoding into a temporary file when a peer reads or seeks a
transfer image. Its `prepare` includes `encode`. Installation still measures
full application-table replacement and an immediate commit. Historical captures
from the short-lived compressed-image implementation also contain `compress`
and `flower.storage.snapshot.chunk.bytes`; new checkpoints do not emit those.
Cell memory sums are not process RSS.

HTTP durations exclude streaming response lifetimes and client retries. Compare
them with benchmark customer logical latency, which includes retries and leader
discovery. A cache lookup counter counts the named probe, not necessarily a
unique customer request. Cancelled/incomplete work is retained as such where
instrumentation observes a drop; a killed process cannot finish its spans.

`flower.http.server.stage.duration` and `flower.http.stage` spans separate
`body_receive` (POST body buffering at ingress) from `json_extract` (Axum JSON
extraction on `/v1/query`, `/v1/mutate`, and `/v1/call`). Body reception includes
transport, flow-control, and executor scheduling waits; it is not a CPU-time
measurement and does not distinguish those causes. Parsing retains Axum's
content-type, body-limit, and rejection behavior. These stages explain work
before query cache lookup or writer submission, and are nested inside the
HTTP request duration.

Exporters batch on background threads with bounded SDK queues. Use standard
`OTEL_BSP_*` settings for the span queue, batch size and export schedule, and
`OTEL_METRIC_EXPORT_INTERVAL` for metrics. SIGTERM/Ctrl+C stop serving, drain
consensus storage, then flush and shut down exporters. SIGKILL loses buffered
telemetry, including during benchmark leader failures. Exporter errors remain
in local diagnostic logs. Collector capture counters measure receiver losses;
they cannot account for all upstream sampling or exporter queue losses.

## Capture the benchmark locally

See [the benchmark guide](bench/README.md#opentelemetry-reporting) for
`node bench/profile-otel.mjs`. It launches a bounded loopback OTLP/HTTP JSON
receiver and writes a standalone HTML report, metric summaries, and sanitized
NDJSON next to a fresh benchmark result. No external collector is required.
The report preserves per-node identities, trace links, capture loss and export
interval coverage. Cumulative histograms are differenced, never summed across
successive exports. Instrumentation changes timing, so diagnostic goodput should
not replace an uninstrumented capacity result.

Validation: `node tests/e2e-otel.mjs` after building the server exercises a real
three-node cluster, incoming and forwarded trace context, batch links,
all metric families, failed evaluations, payload exclusion, shutdown flushing,
and disabled reporting.
