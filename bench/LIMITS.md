# Operator budgets and representation boundaries

Flower validates process settings at startup and caches them once. Restart a node to change its budgets. Configure a compatible set across a cluster: replicas may serve queries, and every node must be able to replay and transfer data admitted by a leader. Raising one size allowance does not reserve physical RAM or guarantee every value below it fits the other allowances; object graphs, JSON escaping, bytecode and nested callbacks consume additional memory.

## Evaluator

These settings apply to the production QuickJS-in-Wasm evaluator and its Rust coordinator. They are available in ordinary builds.

| Environment variable | Default | Scope |
| --- | ---: | --- |
| `FLOWER_EVALUATION_TIMEOUT_MS` | 10000 | Entire application transaction: Rust coordination, bundle preparation and all nested callbacks. Guest interruption checks run on a shared 5 ms metronome; Rust loops check the same deadline. Trusted process setup precedes the request deadline. |
| `FLOWER_BUNDLE_MAX_BYTES` | 2097152 (2 MiB) | UTF-8 source bytes, both deployment validation and prepared-bundle lookup. |
| `FLOWER_RESULT_MAX_BYTES` | 16777216 (16 MiB) | Invocation JSON, host-call JSON, guest results, transaction output and the generated HTTP registry. Also bounds aggregate crypto input bytes and output bytes separately. Envelopes and patch metadata count toward the bound. |
| `FLOWER_GUEST_MEMORY_BYTES` | 134217728 (128 MiB) | Aggregate live Wasm linear memory across nested isolated callbacks in one transaction; resident instances are charged again at checkout. Also configures Wasmtime's per-memory pool growth maximum; virtual address reservation is separate. |
| `FLOWER_RUST_MEMORY_BYTES` | 134217728 (128 MiB) | Estimated allocation one transaction retains: its overlays, source indexes, the graph cells and roots it adds or replaces, dependencies and evaluation history. The graph and roots it shares with its snapshot are not charged. Context/result allocations are checked too. This is allocation accounting, not a process RSS limit. |
| `FLOWER_INDEX_MEMORY_BYTES` | 16777216 (16 MiB) | Aggregate optional equality-index cache per transaction. An index that does not fit falls back to scanning borrowed values; this does not reject the query. |
| `FLOWER_WASM_POOL_SLOTS` | 256 | Process-wide Wasmtime instance/memory/table pool slots, shared by active and retained idle instances. Nested callbacks use additional slots. Allocation failure names this setting and the guest-memory setting. |
| `FLOWER_WASM_RECYCLE_BYTES` | 100663296 (96 MiB) | Process-wide allowance for idle instances' linear memory. Zero disables resident retention; images larger than this allowance execute in fresh instances. The separate half-pool-slot ceiling still applies. |

These settings require decimal integers: `FLOWER_WASM_RECYCLE_BYTES` accepts zero, while the others must be positive. The timeout must fit the monotonic clock. Host allocation sizes, including the reuse allowance, must fit `isize`; pool slots must fit Wasmtime's `u32`. Source/result buffers must be smaller than `i32::MAX`, because the pinned guest's setup ABI uses signed 32-bit lengths and a trailing NUL. A guest memory allowance cannot exceed wasm32's 4 GiB address space. Bundle bytes must not exceed guest memory; result bytes must not exceed guest or Rust memory budgets. A guest-memory setting smaller than the initialized guest's actual memory footprint fails engine startup; a small reuse allowance only bypasses retention. Wasm memory grows in 64 KiB pages, so a non-page-aligned guest allowance can leave part of its final page unusable.

The pinned QuickJS guest does **not** have a baked-in 128 MiB heap maximum. QuickJS delegates memory enforcement to Wasmtime, and its module declares wasm32 memory without a maximum. The host pool growth maximum follows configuration. On 64-bit hosts, Wasmtime reserves the full 4 GiB Wasm32 virtual address range per pooled memory so ordinary guest accesses can use hardware bounds checks. This reserves address space, not physical RAM: the default live-memory allowance remains 128 MiB, growth beyond its configured limit is denied, and unused reserved pages stay inaccessible. On 32-bit hosts, the reservation follows the configured pool maximum. Bytecode is bounded by the guest-memory budget and ABI representation, replacing its independent 8 MiB rejection threshold. An aggregate invocation input block must still fit the ABI's signed allocation length; arithmetic is checked before guest allocation.

A transaction's graph charge is the accounted size of each cell or root entry it adds or replaces: for a cell, its dependency list, reader records, height and traversal entries; for a root, its lookup entries. A cell whose outcome alone changed costs nothing. Entries it keeps unchanged from its snapshot cost nothing, and removing an entry releases only what the same transaction charged for it. Inserting rows into a large database therefore costs what the new rows add, whatever the size of the graph. Restored graphs need no validation: reader and height records are stored with the cells. A deployment, or a callback error that retains unevaluated dirty children, traverses the whole graph; that traversal is bounded by the evaluation deadline and by process memory, not charged to the transaction that triggers it.

Staged deployments rebuild retained roots in durable adaptive pages. Multi-root candidates target `FLOWER_DEPLOYMENT_PAGE_MS` (default 200 ms), capped by the evaluation timeout and independent of ordinary writer batching. An unsuccessful candidate can retry one root with its full normal evaluation allowance. A page can therefore occupy the writer for the page target plus one evaluation, along with admission and commit time. Lower targets favor smaller, more numerous pages and shorter competing write stalls; larger targets favor fewer commits and higher rebuild throughput. This is a soft preparation target, not a request-latency guarantee. Pages compute the heights of the cells they add from stored heights and collect only what their removals release; unresolved dirty descendants retained by a callback error still require a full traversal. Each root dependency closure and activation-time clock/key refresh still fit the normal evaluation budgets; a page is charged for the graph entries it adds, not for the target graph earlier pages built. Completed target roots are maintained alongside active roots within each mutation’s evaluation deadline and combined output allowance. A fatal target-maintenance failure marks the build failed; the active mutation can commit if its result and the failure status fit. Extra graph generations consume memory and storage until collected.

The former 100,000-read/operation and 10,000-cell ceilings are removed, as is the charge for the whole inherited graph and root index, which failed every write once one database's graph accounting reached `FLOWER_RUST_MEMORY_BYTES` (about 40,000 orders at the default budget, each with a derived subtotal and a materialized total). Repeated reads consume the execution deadline; retained cells and dependency/traversal structures consume memory. Evaluation-history growth is charged even when a method repeatedly previews the same cell. Names, HTTP aliases and request IDs must be nonempty; their old independent 512/256-byte ceilings are removed in favor of enclosing request, bundle and output budgets.

These structural guards remain because raising them requires a different representation or stack architecture:

- Business JSON depth is 128. Rust and the guest enforce the same business-value contract before recursive copying or decoding; outcome and argument wrappers at the guest boundary do not consume that allowance.
- Reactive graph traversal/evaluation depth is 128. Reentrant interpreter entry is limited to 32 and checks the native stack distance (1152 KiB), reserving room for unwinding on a 2 MiB worker stack.
- Each Wasmtime entry has a 256 KiB native stack allowance. The guest has a 1 MiB C shadow stack with injected checks that trap before writes leave its reserved interval. These are safety boundaries, not tunable application counts.
- The two guest imports are `flower.host_call` and `flower.crypto_call`. The database callback’s 64-byte operation name and the crypto callback’s numeric operation IDs/fixed arities describe internal protocols, independent of application method names. One private memory/table/instance per Store and the fixed guest's function-table allowance reflect the pinned module ABI.
- Trusted sandbox initialization has a deadline of `max(30 seconds, FLOWER_EVALUATION_TIMEOUT_MS)` so longer application allowances also permit longer initialization. Engine compilation occurs before application timing begins.

Before a reusable base or static application image is captured, QuickJS collects unreachable cycles and resets its collection threshold to live allocated bytes plus 50%, matching its normal post-collection policy. Automatic collection and configured guest/deadline limits remain active. Frozen method and derived context objects are created during trusted sandbox initialization, before application module code; overriding `Object.freeze` in a module does not intercept their construction. Every callback starts from the pristine logical guest image; globals, closures, prototypes, typed arrays and allocator state do not survive between callbacks. A reused instance, from any thread, first has every mutable numeric Wasm global and all changed memory bytes restored, its entire host capability/cache state replaced, and the next invocation’s initial memory charged again. Active parents and nested callbacks never share an instance. Traps, failed evaluations and growth discard their instances; an ordinary business-error envelope may reuse storage only after the same reset.

Cache policy does not impose application-size limits. Bundle images retain at most eight entries/96 MiB; initialized heaps above 32 MiB use the shared pristine base plus bytecode, rerunning module code in every callback, and the server logs a warning. Oversized prepared artifacts run without retention. Evaluations find the deployed bundle's image by the version of its record, reading, parsing and hashing the record once per version rather than once per evaluation. Source-identity and incremental compiled-code caches likewise discard or bypass entries without rejecting valid application work. These retention choices can change independently of application semantics.

Resident reuse is enabled by default. `FLOWER_WASM_RECYCLE=0` or `false` disables it; `FLOWER_WASM_DIRTY_PAGES=0` or `false` keeps reuse but copies the entire linear memory on reset; `signal` keeps signal-based tracking on Linux even where userfaultfd is available. These switches are read once; restart to change them. They do not alter callback isolation or admission budgets.

On Linux and macOS, the default reset copies only pages an instance may have written. Protected pages are untouched and already pristine; hot pages stay writable, and every reset copies them. Linux 6.7+ uses asynchronous userfaultfd write protection with `PAGEMAP_SCAN` when the process may create a user-mode userfaultfd; container seccomp profiles often forbid that, and the process then uses signals. With the kernel tracker, each reset copies and re-protects exactly the pages written since the previous reset, and a page written in two resets within four stays hot. With signals, a first write to a protected page makes its OS page hot, and Rust host writes explicitly mark their full spans; a hot set larger than twice its expected size plus 256 KiB is protected again. Every 1,024 resets either tracker protects all pages again, and each new instance starts with its image's learned seed hot. A rarely written page therefore costs at most an extra fault, not a copy on every later reset. There is no checksum, sampling or application-specific write-range assumption. Unsupported platforms/page layouts and protection-setup failure after successful cleanup use full copies. macOS uses Unix signals for Wasmtime traps; every Engine in the process must use the same signal/Mach-port configuration.

Idle resident instances share a process-wide ceiling of `FLOWER_WASM_RECYCLE_BYTES` (96 MiB by default) of linear memory and half of `FLOWER_WASM_POOL_SLOTS`; one configured slot disables retention. A zero byte allowance disables retention and its reset-snapshot/write-tracking setup. Images larger than the allowance also use fresh instances backed by the pristine copy-on-write image; this never rejects otherwise valid application work. Idle bytes are separate from active transaction charges and are not an RSS guarantee. Pristine reset copies also consume image-cache memory; native instance/table metadata and compiled code consume additional space. Idle instances can reduce available pool capacity, especially with very small slot counts. Every thread shares them: a returning instance evicts the least recently used idle instances of other images when the allowance is full, and all idle instances are released when Wasmtime runs out of slots. An evicted instance returns its allowance before its Store is destroyed, and idle instances are released after 30 seconds. Raise the pool budget or disable reuse when that tradeoff is unsuitable. The 4 GiB virtual reservation per slot still does not commit 4 GiB of RAM.

Crypto inputs/outputs also obey `FLOWER_RESULT_MAX_BYTES`. Before native work, JWT reserves a conservative workspace allowance of 64 × aggregate input bytes against `FLOWER_RUST_MEMORY_BYTES`; NaCl checks 2 × predicted output bytes. These are estimates rather than exact peak allocations. Guest output buffers additionally consume the Wasm memory allowance. QuickJS’s current ArrayBuffer representation is limited to `INT32_MAX` bytes. Native crypto checks the shared deadline before and after each call; Wasmtime cannot interrupt a Rust crypto routine midway. A late result or exceeded budget poisons the invocation even if application code catches an ordinary crypto error.

## Verification

Fresh-process tests isolate environment settings from the process-wide `OnceLock`. They exercise source above the old 2 MiB ceiling, output above 16 MiB, a guest allocation above 128 MiB under raised settings, small result/guest/Rust budgets, a short deadline, and equality-index scan fallback with a one-byte cache budget. Separate validation tests reject invalid integer syntax, impossible cross-budget settings, and ABI/address-space overflow. Engine tests keep inserting, removing a root and restarting under a Rust budget one eighth of the graph's accounted size, and check that the budget a transaction needs does not depend on the graph it inherits. Existing sandbox, deterministic execution, sticky failure, graph atomicity and storage tests remain applicable.

## Consensus resource policy

Consensus reads operator settings once, before opening durable state. Invalid
numbers or inconsistent budgets fail startup. Use the same Raft, transport, and
snapshot settings on all members. These are resource and scheduling policies;
they do not weaken quorum, persistence, or log validation.

| Environment variable | Default | Purpose |
| --- | ---: | --- |
| `FLOWER_READ_TIMEOUT_MS` | 10000 | Fresh-read deadline, including admission queue and follower catch-up; also bounds writer read barriers. |
| `FLOWER_READ_QUEUE_CAPACITY` | 64 × available CPUs | Pending callers per read stage; a sealed active cohort can contain up to the same number. Full queues return `UNAVAILABLE`; no unbounded waiting-to-send queue. |
| `FLOWER_COMMIT_TIMEOUT_MS` | 15000 | Deadline awaiting a submitted Raft write; also bounds complete membership operations including learner catch-up. A timeout does not prove failure: retry data writes with their request IDs and inspect membership before retrying a topology change. |
| `FLOWER_TRANSACTION_MAX_BYTES` | 33554432 | Serialized application command, including a batch's wrapper and separators. There is no additional transaction-count ceiling. |
| `FLOWER_RPC_MAX_BYTES` | 67108864 | Incoming Raft request body and outgoing serialized request ceiling, including metadata. |
| `FLOWER_APPEND_TIMEOUT_MS` | 5000 | Retained durable append transport lifetime; at least the deadline requested by Raft. Short heartbeat polls can resume the same in-flight data transfer. |
| `FLOWER_PEER_CONNECT_TIMEOUT_MS` | 500 | Peer connection establishment deadline. |
| `FLOWER_PEER_IDLE_TIMEOUT_MS` | 30000 | Idle pooled peer connection lifetime. |
| `FLOWER_SNAPSHOT_TIMEOUT_MS` | 10000 | Raft snapshot segment installation/transport deadline. |
| `FLOWER_SNAPSHOT_CHUNK_BYTES` | 1048576 | Raw snapshot bytes per transport segment, sent as they are after a JSON header. `FLOWER_RPC_MAX_BYTES` must hold it plus metadata. |
| `FLOWER_VALUE_CACHE_BYTES` | 268435456 (256 MiB) | Parsed stored values shared by every reader in the process, charged at six times their JSON plus 64 bytes. Three quarters go to 64 shards that evict in insertion order; a value too big for a shard's slice (above about 512 KB of JSON by default) goes to the last quarter, shared by large values and evicted least recently read. A value too big for that quarter is parsed on every read, and the server logs a warning naming its key once. A value still in use stays alive, uncached. |
| `FLOWER_REDB_CACHE_BYTES` | 1073741824 (1 GiB) | redb's page cache for the database file, up to half of which buffers pages being written. |
| `FLOWER_SNAPSHOT_AFTER_LOGS` | 256 | Applied-log distance that triggers a snapshot independently of the soft-policy cooldown. |
| `FLOWER_PERSIST_INTERVAL_MS` | 500 | How long applied states wait to be written to the application tables together (at most 60000). Raft logs already make them durable; the tables let reads come from disk. Each write rewrites the B-tree pages of every key it changes, so writing half a second of applies at once writes a key changed by several of them once and shares their branch pages: under Trinity's 10,000-session load this cut the disk's writes 2-3× and each log flush from 26 ms to 9 ms. Unwritten states stay in memory and replay from the log after a crash. Zero writes each apply as soon as the disk is free. |
| `FLOWER_SNAPSHOT_AFTER_BYTES` | 16777216 (16 MiB) | Soft trigger for encoded durable log bytes applied after the last published snapshot. Zero disables this trigger. It bounds the retained log and its replay; a snapshot is a metadata checkpoint and pins no redb pages. |
| `FLOWER_SNAPSHOT_MAX_AGE_MS` | 300000 (5 minutes) | Soft age trigger, only with new applied entries. Zero disables it; age starts at node startup or successful snapshot publication/install. |
| `FLOWER_SNAPSHOT_DUTY_PERCENT` | 10 | Target build duty cycle for supplemental byte/age triggers, integer 1–100. 100 removes the cooldown. |
| `FLOWER_SNAPSHOT_CHECK_MS` | 1000 | Positive cadence for checking supplemental snapshot triggers. |
| `FLOWER_SNAPSHOT_LAG_LOGS` | 512 | Replication lag that selects snapshot catch-up. Must exceed the snapshot creation distance. |
| `FLOWER_SNAPSHOT_KEEP_LOGS` | 64 | Already-snapshotted logs retained for incremental catch-up; zero is allowed. |
| `FLOWER_SNAPSHOT_PURGE_BATCH_LOGS` | 1 | Minimum number of eligible logs purged in one operation. |
| `FLOWER_RAFT_PAYLOAD_ENTRIES` | 64 | Preferred maximum logs in one replication RPC; actual serialized byte budget also applies. |
| `FLOWER_RAFT_HEARTBEAT_MS` | 50 | Heartbeat interval. |
| `FLOWER_LEADER_FLUSH` | deferred | `immediate` makes a leader flush every own log append. Otherwise a leader of at least three voters appends without an fsync and counts toward the commit quorum only after a later flush. |
| `FLOWER_LEADER_FLUSH_GRACE_MS` | 30 | Deferred leader appends still uncommitted this long are flushed, for example while a follower is down. |
| `FLOWER_LEADER_FLUSH_INTERVAL_MS` | 500 | Longest a deferred leader append waits for its flush. |
| `FLOWER_RAFT_ELECTION_MIN_MS` | 150 | Lower randomized election delay. |
| `FLOWER_RAFT_ELECTION_MAX_MS` | 300 | Upper election delay and OpenRaft's additional committed-leader lease. |

All values are positive integer counts unless noted. Byte buffers must fit the
platform's `isize` range, read admission must fit Tokio's semaphore capacity, and
deadlines must fit the platform clock. Raft timers satisfy
`heartbeat < election_min < election_max`; checked arithmetic validates the
pinned OpenRaft implementation's `3 × heartbeat` and `2 × election_max`
calculations. The previous one-hour timing ceiling is removed. Log counters use
Raft's `u64` representation; these controls do not change that protocol range.
After opening storage, startup also checks snapshot/purge/replication distances
against the highest persisted log or applied snapshot index with checked
addition, before starting Raft. A setting that would already overflow an existing
node is rejected. Exhausting the representation after further log growth remains
an upstream numeric boundary rather than a supported operating condition.

The RPC ceiling must accommodate one maximum-sized application command plus the
exact worst-case fixed JSON append envelope, computed using maximum-width Raft
identifiers. Oversized multi-entry catch-up requests are stopped while encoding
and retried one entry at a time. Membership records contain variable-size node
metadata and are still checked against the actual wire limit.

Snapshot segments currently encode raw bytes as JSON integer arrays: one byte
can occupy four wire bytes including its separator. Startup therefore requires
four times the chunk size plus envelope headroom to fit the RPC budget. Snapshot
membership metadata is variable and checked against the complete encoded request
at runtime. Very large memberships can require a larger RPC budget or smaller
chunks. OpenRaft 0.9.25's chunk transport does not adaptively shrink snapshot
segments after a size error.

Fresh reads use two bounded stages: one on the serving replica, one on the
leader. Each stage yields once, drains its current queue, and seals the cohort
before beginning its RPC or quorum barrier. A request arriving after the barrier
starts must join a later cohort. A follower waits for the acknowledged committed
log to be applied and atomically published locally before releasing readers. Cohort size has no
independent fixed ceiling; admitted work and time are bounded by the configured
queue and deadline. Each stage can retain one active cohort plus one full pending
queue (at most twice the configured count). After startup recovery, explicit replica-local queries bypass
these quorum stages. When durable logs extend beyond the recovered application
checkpoint, the physical node and all its logical partitions first share a
quorum-confirmed recovery fence and wait for local application. Until that
barrier succeeds, even replica-local application reads can be unavailable.

Connection retention, read deadlines, and admission backpressure bound work when
peers fail or callers disappear. Increasing them spends more memory, connections,
or waiting time; it does not change the requirement to observe a quorum or apply
the returned fresh-read fence. Storage has no separate fixed snapshot-file size
cap here: snapshot/log growth follows application state and disk capacity.

`Consensus::limits()` exposes the resolved policy to service scheduling.
`Limits::commit_group_fits(sum_of_encoded_commits, count)` accounts for the exact
batch wrapper and commas, so service batching cannot accidentally exceed the
consensus transaction limit by adding its envelope.

Graceful shutdown also waits for Raft-owned storage readers, snapshot workers,
and any already-started blocking disk work to release the database. OpenRaft's
core shutdown alone does not join all of those workers. Ordinary HTTP router
clones do not participate in this drain, avoiding an ownership cycle and making
immediate in-process restart safe after shutdown returns.

## HTTP service and scheduling

These settings work in ordinary builds. Sizes and durations must be positive platform-representable integers unless a zero-disable policy is stated; durations are milliseconds. Admission counts also obey Tokio's actual semaphore representation. Settings are read once before the service starts; restart nodes to apply changes.

| Environment variable | Default | Purpose |
| --- | ---: | --- |
| `FLOWER_HTTP_MAX_BODY_BYTES` | 8388608 | Application/admin HTTP request body, including JSON envelope and escaping. Raft RPCs use their separate transport budget. |
| `FLOWER_HTTP2_MAX_STREAMS` | Unset | Optional per-connection advertised H2 stream ceiling. Any positive `u32` is accepted; unset advertises no ceiling. |
| `FLOWER_HTTP1_MAX_HEADERS` | 100 | HTTP/1 request header count. Unset preserves Hyper's stack parser fast path; an explicit count uses its configurable parser allocation. |
| `FLOWER_HTTP1_MAX_BUFFER_BYTES` | 417792 | HTTP/1 connection/parser buffer, including the request line and encoded headers. Hyper requires at least 8192 bytes; it must fit a platform byte buffer. |
| `FLOWER_HTTP2_MAX_HEADER_LIST_BYTES` | 16384 | Advertised HTTP/2 decoded header-list budget, including pseudo-headers and 32 bytes of accounting overhead per field; positive `u32`. |
| `FLOWER_SHUTDOWN_TIMEOUT_MS` | 30000 | Graceful HTTP drain after SIGTERM/SIGINT. Remaining connections, including SSE, close after this deadline; Raft/storage shutdown then drains separately. Positive integer fitting the platform timer range. |
| `FLOWER_QUERY_CACHE_BYTES` | 16777216 (16 MiB) | Per-logical-database query result/key/dependency-certificate retention budget, with conservative metadata accounting. Zero disables retention; no entry-count cap. Hits validate observed immutable record and membership identities against their selected snapshot. |
| `FLOWER_AUTHORIZATION_CACHE_BYTES` | 16777216 (16 MiB) | Per-logical-database budget for reused authorization decisions, keyed by a digest of credentials, method, partition, delegation and the argument fields the decision read (none, a few by name, or all of them when their JSON takes at most 16 KiB), with the principal and dependency certificate. Zero disables reuse. Hits validate the certificate and declared time against their selected snapshot. |
| `FLOWER_QUERY_FLIGHT_BYTES` | 524288 (512 KiB) | Per-logical-database active shared-query registry budget, including keys and estimated synchronization metadata. Zero disables shared evaluation; full registries evaluate independently. |
| `FLOWER_QUERY_WORKERS` | Available CPUs | Shared bound for best-effort public HTTP cache probes and authorization callbacks; also supplies the default for FLOWER_PREPARATION_WORKERS. Heavy query evaluation uses the preparation pool, not an independent pool of this size. |
| `FLOWER_PREPARATION_WORKERS` | FLOWER_QUERY_WORKERS | Node-wide user preparation slots shared by queries, writer lanes, authorization, and SSE encoding across every named partition. |
| `FLOWER_CONTROL_WORKERS` | 1 | Separate operator/maintenance preparation slots, for work that is never held for long: each logical database's writer waits for one while holding its lane (maintenance, key and retention commands, and a deployment's cutover, which evaluates for at most `FLOWER_DEPLOYMENT_CUTOVER_MS`). Public calls and authorization use user capacity; deployment preparation uses deployment capacity. Raft apply and durability do not acquire these permits. |
| `FLOWER_DEPLOYMENT_WORKERS` | 1 | Node-wide deployment preparation slots: online and blocking deployment evaluation (including compilation of a new bundle), staged deployment analysis, pages, activation and collection. Separate from control capacity, so a deployment compiling for seconds never stalls another database's maintenance and with it that database's writes. |
| `FLOWER_DEPLOYMENT_MEMORY_BYTES` | deployment workers × (guest + Rust allowance) | Active deployment reservation budget, with the same per-evaluation charge. Must fit at least one guest + Rust evaluation allowance. |
| `FLOWER_DEPLOYMENT_CUTOVER_MS` | 1000 | Longest an online deployment whose base changed during preparation may evaluate again inside the writer lane, waiting for a deployment slot included; that database's writes wait meanwhile. Only candidates whose online evaluation took at most this long, with their compiled image still cached, try; others, and repeats that run out of time, return `DEPLOYMENT_CONFLICT`. 0 restores the exact-revision rule. Capped by `FLOWER_EVALUATION_TIMEOUT_MS`. |
| `FLOWER_PREPARATION_MEMORY_BYTES` | workers × (guest + Rust allowance) | Sum of conservative active-user evaluation reservations. Must fit at least one guest + Rust evaluation allowance. |
| `FLOWER_CONTROL_MEMORY_BYTES` | control workers × (guest + Rust allowance) | Separate active operator/maintenance reservation budget, with the same per-evaluation charge. |
| `FLOWER_QUEUED_INPUT_BYTES` | 67108864 (64 MiB) | Shared retained-input/output budget, including active inputs, cache-probe scratch and encoded cache-hit HTTP bodies. All named partitions compete within one budget. Accounting includes conservative metadata estimates; it is not total transport or process memory. |
| `FLOWER_WATCH_RETAINED_BYTES` | 268435456 (256 MiB) | What open watches hold: each subscriber's input, each shared producer, and one encoded copy of its current value and latest patch. Apart from the budget above, so watches never keep requests out; once it is full, new watches are refused until others close. A watch of a 1.5 KB value holds about 3 KB. |
| `FLOWER_CONTROL_QUEUED_INPUT_BYTES` | 16777216 (16 MiB) | Separate retained-input budget for trusted operator/maintenance work, and another of the same size for deployment work (bundles). |
| `FLOWER_WRITER_PREPARATION_WORKERS` | FLOWER_PREPARATION_WORKERS | Maximum optimistic preparation wave width within one ordered writer lane. One disables speculation. The node-wide admission/memory pool remains the effective concurrency bound; conflicts shrink the width and temporarily fall back to serial work. Candidate results are dependency-validated and reauthorized in order. |
| `FLOWER_WRITER_BATCH_MODE` | adaptive | Learn group size from preparation cost, durable commit time and queued work. `fixed` selects count-based comparison mode. |
| `FLOWER_WRITER_BATCH_SIZE` | Unset (64 in fixed mode) | Optional hard command-count cap. Adaptive mode otherwise has no fixed count ceiling; the configured admission capacity bounds retained requests. |
| `FLOWER_WRITER_QUEUE_CAPACITY` | 128 × available CPUs, or twice an explicit batch size | Pending mutations before admission returns retryable `503`; also bounds each adaptive group. |
| `FLOWER_WRITER_BATCH_MS` | 50 | Maximum preparation window. Adaptive mode considers observed durability cost, backlog, oldest request age, and time until the writer must yield for maintenance. Overdue-backlog recovery targets all queued work within those bounds; healthy queues retain partial-backlog sizing. Near a maintenance boundary, it drains instead of starting a tiny successor. A successor cannot be submitted before its predecessor completes, so this window does not close it early while that predecessor commits. Local unapplied-log and voting-quorum replication backlogs gate speculative successor preparation; they do not force singleton commits. Stops between methods; one slow callback can overrun this soft window. |
| `FLOWER_DEPLOYMENT_PAGE_MS` | 200 | Soft preparation target for adaptive staged graph pages, capped by `FLOWER_EVALUATION_TIMEOUT_MS`. Independent of ordinary writer batching. Lower values favor more, smaller pages and shorter competing write stalls; larger values favor fewer pages and rebuild throughput. A failed multi-root candidate may retry one root with the full normal evaluation timeout. |
| `FLOWER_WRITER_WINDOW_MS` | 250 | Yield the writer turn between methods/groups so maintenance can run. |
| `FLOWER_MAINTENANCE_INTERVAL_MS` | 250 | Shortest gap between maintenance runs; they start when TypeScript work is due, or after a write, not on a timer. Handlers without a `next` hint are polled at this interval. |
| `FLOWER_MAINTENANCE_BURST_MS` | 50 | Stop a catch-up burst after a callback takes it past this duration. No independent callback-count ceiling. |
| `FLOWER_WATCH_REFRESH_MS` | 250 | Re-evaluate watches whose result or authorization read `ctx.now()` without declaring a change time. Commits, Raft role changes and declared times wake the rest. |
| `FLOWER_WATCH_DUTY_PERCENT` | 10 | Share of the time a watched result may spend refreshing for writes: after a refresh that took T (evaluation, diff and encoding), a write refreshes it again no sooner than T × 100 / this, and writes meanwhile join that refresh. A 1 ms result waits 10 ms at most; a 20 ms one refreshes at most every 200 ms however fast what it read changes. Access, code, clock and Raft changes refresh at once. 100 turns pacing off. |
| `FLOWER_WATCH_BUDGET_PERCENT` | 50 | Share of one core, in percent (above 100 for several), of CPU time that the refreshes writes start may take together on this server (each refresh's evaluation on its worker thread, then its diff and encoding); 0 for no budget. Results refreshing for writes share it equally by time (generalized processor sharing): a refresh whose CPU time was T keeps its result in service until a virtual clock, advancing at this / 100 divided by the results in service, has advanced by T, and a write refreshes that result again no sooner (nor sooner than its duty allows). With N results whose reads keep changing, each refreshes about every N × T × 100 / this: of seven at 50%, six 60 ms session lists refresh every 0.84 s (0.6 s on their duty alone) and a 5 ms one every 70 ms, whatever the others cost. A result waits only for its own last refresh, and below the budget only for its duty. New subscribers, and access, code, clock, Raft and partition-lapse wakes, are never held, also while a write waits. `/admin/resources` reports it under `watches.refreshes`: `budgetPercent`, `serving` (results in service), and since start `turns`, `spentMs` (refresh time charged) and `waitedMs` (time writes waited for a turn). |
| `FLOWER_WATCH_KEEPALIVE_MS` | 15000 | SSE heartbeat cadence. |
| `FLOWER_WATCH_SEND_TIMEOUT_MS` | 5000 | Maximum wait for room to send a changed update in a slow consumer’s one-item output queue. |

Queries requiring evaluation, generic calls that miss the public-query cache, and watch refreshes acquire shared preparation admission **before** capturing their execution snapshot. Waiting admission futures retain request metadata, not old persistent roots. FIFO queues rotate across trusted logical-partition bindings; input JSON cannot invent a scheduling identity. One partition may use otherwise-idle slots, and writes remain ordered within their existing lane. Full byte budgets reject with `503 ADMISSION_OVERLOADED`. The existing HTTP body limit still applies before JSON extraction; this is not an incremental streaming parser or a limit on every kernel/HTTP allocation.

Public HTTP query cache hits can bypass that heavy preparation pool. A probe first reserves input/cache-key scratch bytes and immediately tries the shared `FLOWER_QUERY_WORKERS` semaphore; it never queues for a lookup slot. It then checks partition ownership, the selected snapshot’s HTTP registry and read policy, transaction locks, and the result’s dependency certificate. Fresh queries still obtain a quorum-backed fence. Applications with authorization hooks or managed keys use the full admitted path instead. Busy lookup capacity, insufficient optional scratch capacity, or a cache miss drops every probe snapshot and reservation before ordinary fair admission captures a new snapshot and repeats the checks. This is a bounded best-effort lookup path, not a separate fair partition scheduler.

A cache hit copies the already-encoded value into its HTTP envelope without decoding and re-encoding it. Output bytes are reserved before allocation and remain charged to `FLOWER_QUEUED_INPUT_BYTES` until the transport releases the last reference, even after cache eviction. The response retains no database snapshot, lookup slot, or evaluation permit. A slow fresh-read fence occupies a lookup slot until it finishes; saturated probes fall back to ordinary admission. Slow response consumers retain output bytes and can exhaust that byte budget.

A running evaluation reserves its configured guest-plus-Rust upper allowance. The effective concurrent count is the lower of worker slots and whole reservations fitting the class’s memory budget. These reservations bound admitted work; they are not resident-memory measurements or a process RSS guarantee. Persistent state, prepared images, shared native-key caches, transport buffers and Raft structures have their own ownership/budgets. An admitted query holds its reservation while obtaining a fresh-read barrier, so slow quorum I/O can reduce runnable CPU concurrency; this deliberately bounds snapshot holders and should be measured before increasing capacity. Only the eligible public HTTP cache path above avoids a guest-plus-Rust evaluation reservation; watches and protected queries retain ordinary admission. The ordered writer keeps one shared immutable preparation snapshot per active lane before admission to a bounded preparation pass; speculative siblings clone that root rather than its JSON payloads. These retained writer roots are not measured RSS reservations, and leadership/state transitions may keep an older root alive until that lane finishes. Accepted speculative output retains its byte lease through the durable group.

Dropping a queued query removes its lane entry and byte reservation. Dropping a queued writer releases its JSON immediately, leaving only a small queue marker; if preparation already began, the command continues and the caller’s outcome can remain uncertain. Blocking native jobs retain cloned permits until they finish even if HTTP cancellation drops the waiter. Maintenance/operator capacity is separate from ordinary clients; Raft durability/apply never waits on application admission. Watch state is charged between refreshes, and a watch that cannot reserve its next value closes with an admission error.

`GET /admin/resources` requires the operator bearer token and reports per-class active/queued work, lane counts, retained bytes/budgets, reserved evaluation bytes, worker budgets and oldest queued age, plus admitted/rejected/canceled counters. It reports the ingress process, not an aggregate across hosts. Cache-probe and response leases contribute to retained user bytes; the lookup semaphore is not included in active preparation-worker counts. Changing these settings requires restart.

Header budgets are independent of application and Raft body budgets: raising a
body ceiling does not admit a larger header block. HTTP/1 buffer bytes refer to
the parser/connection buffer, not total streamed body size. HTTP/2 header-list
accounting uses decoded names and values, not compressed wire bytes. The pinned
`http::HeaderMap` representation has 32768 raw table slots and a 3/4 load factor.
Hyper reserves the parsed count in one call, so the HTTP/1 count override cannot
exceed its actual reservation capacity of 24576. Header names also retain the pinned
parser's 16-bit representation bound. Requests outside a header budget are
rejected by the transport before reaching application methods.

There is no fixed watch-count ceiling. Each watch retains byte-accounted event state, coalesces revisions and shares node preparation admission. Updates send immediately while the one-item output queue has room. When a changed update finds the queue full, the subscriber drops its unsent update, waits for room and refreshes the latest value after reacquiring admission, the declared read fence and current authorization. Intermediate changes are batched into that refresh. The watch evaluation deadline includes the configured read and evaluator budgets. A consumer waiting for room to send that update beyond the send timeout is disconnected.

The writer starts an idle request immediately. It prepares at most one successor while one group commits, accepting late arrivals only during that existing durability wait. In adaptive mode, the learned count and time targets bound a successor only after its predecessor completes; until then it keeps preparing arrivals, up to the queue capacity and any explicit batch-size cap, then stops at the next call boundary. Stopping earlier would leave later arrivals waiting an extra durability round. Fixed mode keeps its exact thresholds. Preparation estimates divide separately smoothed total preparation time and request count (25% new sample), so a tiny delayed group cannot dominate the per-request cost. Commit time retains a per-group exponentially weighted average. Preparation time excludes waiting for late arrivals and waiting after preparation for a predecessor; admission and scheduling time remain included. Deployments and failed commits do not train the controller. DEBUG `mutation group timing` events include mode, decision/stop reasons, target count/time, queued calls, local unapplied and voting-quorum unmatched log counts, actual count/bytes, and preparation/commit timings. Cache entry/byte caps evict or bypass retention; they do not reject otherwise-valid queries. The JSON patch algorithm bounds traversal/operation/path work and falls back to a complete snapshot, so those constants do not cap watched values. The old independent patch-size threshold is gone: a patch is used only when smaller than the snapshot.

The TypeScript scheduler no longer caps handler counts, identifier lengths or truncates error fields. Retry attempts/backoff remain application options, and safe-integer arithmetic protects JS time/attempt representation. The SDK's composite maintenance handler backs off a failing task that has no `onError` from 1 s, doubling to 60 s; that schedule is fixed in the SDK, while scheduler and queue retries are options. Benchmark workload sizes, run deadlines, trace retention and historical experiment controls describe finite experiments; they are not runtime database capacity limits.

## SDK transport and watch allowances

Client allowances are optional arguments, independent of server policy. Raising
server result/request budgets does not silently increase a client's memory
allowance. `FlowerClient.watch()`, `watchDeltas()`, `subscribe()` and
`waitUntil()` accept:

| Option | Default | Scope |
| --- | ---: | --- |
| `maxEventBytes` | 17825792 (17 MiB) | One streaming SSE event, including comments/field framing; also the bounded HTTP error body before an SSE stream opens. |
| `maxValueBytes` | 16777216 (16 MiB) | UTF-8 serialization of each snapshot and of each complete value reconstructed by `watch()` or `subscribe()`. `watchDeltas()` exposes raw patches without maintaining reconstructed state. |
| `maxPatchOperations` | 256 | Operations in one received JSON Patch. |

The CLI exposes the same options on `watch` as `--max-event-bytes`,
`--max-value-bytes`, and `--max-patch-operations`. Other commands reject these
flags. CLI values must be positive decimal safe integers.

Values must be positive safe JavaScript integers. Limits are captured when a
watch is created, checked before network work, and never sent in the HTTP body.
Patches are checked before application; reconstructed values are checked after
application. Event bytes bound streaming input independently of value size. The
128-level JSON/pointer depth, increasing safe-integer snapshot sequences, and
strictly consecutive patch/base-sequence checks remain
protocol/recursive-representation guards. A raw-delta consumer that reconstructs
state itself is responsible for its own reconstructed-value allowance.

`watchPoll()` accepts an integer `intervalMs` from 1 to 2147483647, default 250.
The upper bound is the runtime timer representation; overflowing values are
rejected before a request instead of becoming one-millisecond polling loops.

`subscribe()` and `waitUntil()` treat `stallMs` (default 45000) without any
received bytes, heartbeats included, as a disconnect and reconnect with
jittered backoff from 250 ms to 30 s, adjustable through `reconnect`. A
`retry` policy on calls defaults to 8 attempts, a 20000 ms per-attempt timeout,
and jittered backoff from 250 ms to 30 s. These are client-side allowances and
are never sent to the server.

The optional Node HTTP/2 adapter `createHttp2Transport()` accepts:

| Option | Default | Scope |
| --- | ---: | --- |
| `maxRequestBytes` | 8388608 (8 MiB) | Serialized request body. |
| `maxResponseBytes` | 67108864 (64 MiB) | Buffered ordinary JSON responses; streaming SSE uses watch allowances. |
| `maxSessions` | 16 | Live multiplexed sessions, including sessions draining after GOAWAY. |
| `requestTimeoutMs` | 30000 | Entire ordinary request, or headers only for SSE. |
| `idleTimeoutMs` | 30000 | Idle pooled connection lifetime. |

The previous 1024-session and 1 GiB body-allowance ceilings are removed. Session
counts must be positive safe integers; byte allowances additionally fit the
current Node `Buffer.constants.MAX_LENGTH`. Timer milliseconds remain in
`1..=2147483647`, Node's actual timer representation. Allowances do not preallocate
capacity or make arbitrarily large strings/buffers constructible: the JavaScript
engine, heap, physical memory, and JSON string representation still apply.

Tests exercise raised 17 MiB snapshots and 300-operation patches, reduced event
and reconstructed-value allowances, UTF-8 byte accounting, HTTP error body
budgets, invalid options before requests, and transport settings above the old
session/body ceilings.

## One production engine

Flower builds only the vendored QuickJS-NG guest in Wasmtime. The former native
V8, JavaScriptCore and rquickjs adapters, their feature flags and dependencies
have been removed. A non-QuickJS
`FLOWER_ENGINE` setting fails startup rather than silently changing execution.

The JavaScript coordinator in `runtime/engine.js` remains a test-only historical
oracle, now executed through the production Wasm engine. Its fixed count/size
checks preserve the comparison fixture; production requests use the Rust
coordinator and configured Wasm budgets described above.


Shared SSE producers retain one encoded snapshot and patch per identical
invocation, full admitted principal, deployment and consistency within a logical
database on a node. These bytes and subscriber-local inputs consume
`FLOWER_WATCH_RETAINED_BYTES`; each subscriber's one-item send queue can retain one
older shared frame until consumed or disconnected. Producer entries disappear
when their last subscriber leaves. Every subscriber still pays its own admission,
authorization and fresh-read fence; sharing does not cache credentials or extend
credential expiry. The refresh interval can coalesce clock-sensitive evaluation
across authorized subscribers. A fresh join does not inherit an earlier clock
sample. Gaps use full reset snapshots. When a changed update finds a
full queue, its subscriber refreshes the latest value once space returns; a wait
for room to send that update beyond the send timeout closes the stream.


## TLS and privileged credentials

| Setting | Default | Scope |
| --- | --- | --- |
| `FLOWER_ADMIN_TOKEN` | Required | Operator deployment/key/retention/partition/resource and membership/init/metrics endpoints. |
| `FLOWER_PEER_TOKEN` | `FLOWER_ADMIN_TOKEN` | Internal Raft, fresh-read fences, forwarding, partition and transaction RPCs. Set a distinct nonempty value to separate peer authority. |
| `FLOWER_TLS_CERT_FILE` | Unset | PEM leaf/intermediate chain for the native TLS listener. |
| `FLOWER_TLS_KEY_FILE` | Unset | PEM private key matching the leaf certificate. |
| `FLOWER_TLS_CA_FILE` | Unset | PEM roots used exclusively by every native internal HTTPS client. |
| `FLOWER_TLS_HANDSHAKE_TIMEOUT_MS` | 10000 | Positive deadline for TLS handshakes, including silent clients. |

The three TLS files must be configured together. They load once at startup;
TLS clients require valid chains and matching DNS/IP names, and cannot downgrade
to HTTP or disable verification. HTTP/2 ALPN and HTTP/1.1 share the TLS listener.
Transport/handshake buffers are outside application preparation reservations.
Peer tokens have one active value and need a coordinated rotation; TLS-mode
switches also require all peers to agree. See [TLS.md](../TLS.md) for CA/certificate
rollout and the full trust contract.

The Node HTTP/2 adapter additionally accepts `ca`: a nonempty PEM string,
Uint8Array, or array of either. It copies byte inputs and replaces its TLS trust
roots when supplied; omission uses Node's configured roots. Both HTTP and HTTPS
origins are supported, with certificate and hostname verification mandatory for
HTTPS. TLS failure closes the session; it never triggers an HTTP downgrade.

Writer speculation starts with two candidates, grows only after a fully reusable wave, and backs off after conflicts. Repeated contention doubles the number of serial decisions between probes up to the configured writer queue capacity (at least the configured worker count); a successful probe resets that history. `FLOWER_WRITER_PREPARATION_WORKERS=1` keeps writer preparation serial while preserving query parallelism. This scheduling policy never weakens ordered validation or atomic application.

## Incoming bodies and retained outputs

Every incoming POST body reserves node-wide bytes before JSON parsing, including
an estimate for a request waiting for its first chunk. Public requests use the
ordinary queued-input budget. Peer/operator paths must present the appropriate
credential before using the reserved control budget. Body buffering consumes no
evaluation-worker slot, so a writer awaiting replication cannot block peer
traffic by occupying every worker.

`FLOWER_HTTP_MAX_BODY_BYTES` and `FLOWER_RPC_MAX_BYTES` are per-request transport
ceilings. The queued-input budgets additionally bound aggregate reservations.
`FLOWER_READ_TIMEOUT_MS` bounds receiving a complete body. The raw-byte reservation
lasts until response headers, including leader discovery and forwarding waits;
local execution separately accounts for the decoded input, so these reservations
conservatively overlap. They estimate retained allocation rather than imposing a
process RSS limit. TLS/socket buffers remain outside these application budgets.

Writer-lock waiters hold input bytes but no evaluation slot. Every locked write
path takes the writer before worker admission, then captures its database root.
Prepared serial/speculative results and receipt replays retain their output
reservation through durable group completion. SSE frames retain theirs through
the downstream transport's final reference to encoded bytes, including after the
subscriber disconnects.

## Live partition movement

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `FLOWER_PARTITION_BASE_MAX_BYTES` | Unset | Optional positive encoded-byte allowance for capturing a durable migration base. Rejection leaves the source serving during the copying phase. |
| `FLOWER_PARTITION_TAIL_MAX_BYTES` | Unset | Optional positive final-difference allowance. A larger difference falls back to a complete frozen image, rather than rejecting or skipping changes. |

Both use positive `u64` decimal byte counts and are read at startup with partition
configuration. A difference larger than the complete image also selects the
full-image fallback. Transfers still obey the participating nodes' transaction,
RPC and HTTP envelopes. Inconsistent budgets can leave a move pending until an
operator restores sufficient capacity; lowering the tail allowance does not
bound the fallback image or promise a maximum pause.

The durable base, its revision/size accounting and invisible destination phase
survive restart. Changes during copying are reconciled by a final comparison,
including receipt/session collection and deletion, rather than an unbounded
journal of intermediate updates. A copied base cannot become active before the
final quiescent image is verified. Source retirement releases the captured base.
Live records are served from redb, but migration exports still cache a full
encoded base/difference/fallback image in memory. File-backed Raft snapshot
streaming is a separate mechanism.

Queries waiting for another identical evaluation, and subscribers waiting for a shared SSE producer, release execution slots and captured snapshots while retaining their input byte leases. Once the producer finishes they reacquire admission, capture a new snapshot/fence and reauthorize their own credentials. That shared-producer retry uses ordinary preparation admission; no old authorization is carried across the wait. The separate public HTTP cache probe described above does not change watch admission.

An ordered writer reuses one user worker reservation across consecutive serial methods in a bounded preparation pass. This avoids rejoining a burst of read waiters after every mutation. The pass releases its slot before waiting for more arrivals, before durability, and at its configured preparation/time boundary. A speculative wave takes ownership of that slot for its first candidate; no spare clone pins capacity while its siblings wait. Other candidates still acquire their own slots, so a one-slot node remains safe and configured node-wide concurrency is unchanged.

## Snapshot trigger scheduling

The log-count trigger remains OpenRaft's independent policy. Supplemental byte
and age triggers require a new applied position and no active local build. They
count the actual stored encoding of applied log entries, including membership
and rejected commands, rather than application changes or live database size.
Startup reconstructs outstanding bytes from the durable snapshot position and
remaining applied logs. A capture records its byte watermark alongside immutable
roots: application during encoding remains outstanding after publication.
Installation resets the tally to its installed position. Uncommitted appended
entries do not count toward the applied-byte threshold.

Build wall time includes encoding, disk queue wait, and durable publication. A
completed attempt of duration D postpones supplemental triggers by
D × (100 − dutyPercent) / dutyPercent. The configured percentage is therefore a
scheduling target, not a hard CPU or I/O quota. Log-count, manual and replication
requirements may bypass that cooldown; byte and age thresholds may exceed their
settings while cooling down. No new state means no recurring idle snapshots.
Snapshot age uses the monotonic process clock and restarts at startup, so
frequent restarts can defer the age trigger; the durable byte tally survives.
Publication retains the existing generation fence and immediate durability.

Authenticated GET /admin/resources includes a snapshots object with
unsnapshottedLogBytes, afterBytes, maxAgeMs, checkMs, dutyPercent, snapshotAgeMs,
lastBuildMs, cooldownMs, activeBuilds, completedBuildAttempts, appliedIndex,
snapshotIndex and due (applied_bytes, age, or null). These are local scheduling
observations, not a replicated application clock or a guarantee that a requested
snapshot has already published. DEBUG events identify supplemental triggers.

Migration export encoding now acquires the shared Control worker reservation
before retaining source roots. It counts encoded output before allocating the
full string and holds that byte lease with the cached image until replacement,
source retirement, or the final shared reference disappears. Difference
construction additionally reserves conservative workspace before building trees
and deletion lists; it releases that workspace after encoding or failure.
These reservations use FLOWER_CONTROL_QUEUED_INPUT_BYTES and can leave a move
pending until capacity is available, including after freeze. Base/tail limits
are transfer policy and do not replace this aggregate node admission. The
workspace estimate is deliberately conservative, and this remains allocation
accounting rather than an RSS bound or streaming migration serialization.
