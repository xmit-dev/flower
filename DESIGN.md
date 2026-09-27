# Flower architecture review and implementation status

Architecture review, September 24, 2026. The original review below is retained as design rationale: its descriptions of “today” refer to the pre-change implementation. Use [bench/ARCHITECTURE.md](bench/ARCHITECTURE.md), [RETENTION.md](RETENTION.md), and the [SDK reference](docs/reference/) for the current contracts. Benchmarks are reported separately in [bench/READS.md](bench/READS.md); an implementation check is not evidence of a speedup.

## Implementation status

| Area | Implemented contract | Remaining boundary |
| --- | --- | --- |
| Authorization | Always-run code hook, immutable principal, separate credentials, replay/cache/watch authorization, delegated transaction policy | Applications still supply their identity provider and policy; trusted replicas retain cluster authority |
| Crypto | Opaque native shared handles, historical verify/decrypt versions, retire/revoke/destroy/rewrap, mounted previous wrapping keys, native prepared-key reuse | Logical destruction does not erase backups or prior plaintext; remote providers are not implemented |
| Retry lifecycle | Immutable epoch-scoped IDs, replicated rejection floors, bounded collection/budgets, authenticated sessions and explicit contiguous ACK/abandon/close | Opt-in; explicit epoch advancement; restore requires external fencing and reconciliation |
| Data access | Native ordered tuple ranges and bounded pages; TS timer/TTL/lease selection; lease history identity | Live pagination, conservative phantom dependencies; scan windows spanning values of their first field stay in RAM |
| Reuse and concurrency | Cross-revision dependency certificates, unchanged-output propagation, shared SSE hubs, bounded optimistic writer preparation | Hot keys, broad scans and changing graph topology still conflict or require full work |
| Admission | Node-wide byte/work budgets, fair logical lanes, reserved control work, cancellation/output accounting | Reservations estimate costs; they are not a process RSS limit or a per-tenant quota system |
| Transport | Native TLS with verified peer identities, separate peer/operator credentials, pooled HTTPS/h2 SDK transport | Cleartext remains explicit default; no mTLS or hot reload; rotate credentials operationally |
| Snapshots and movement | File-backed checksummed chunks, immutable-root capture outside apply locks, generation-fenced publication, byte/age triggers with measured duty cooldown, admitted live pre-copy plus final delta/fallback | Full live state and captured bases still cost storage/memory; physical groups must be provisioned |
| Distributed transactions | Movable logical targets, independent coordinators, partition-scoped prepared barriers, durable history sequences, participant closure floors and incremental detail collection | Prepared locks cover a whole logical database and can block on an unavailable coordinator; admissible abort identities must retire before closure |
| Deployment | Durable index backfill and adaptive materialized-root pages, incremental append validation, dual graph maintenance, atomic code/policy/schema/generation cutover, cancellation and incremental cleanup; direct modes retained | Each root dependency closure and clock/key refresh remains bounded; existing topology edits can require full traversal; arbitrary record migrations need application progress |
| Durability | Quorum-durable Raft log, atomic deferred redb apply checkpoints, durable snapshot/purge boundaries, and a shared startup serving fence when logs extend beyond recovered application | Startup recovery can require quorum even for replica-local reads; metadata migration and the compatibility contract reject unsafe downgrade/mixed versions |
| Measurement | Independent offered-rate driver with scheduled-arrival latency, driver drops, HTTP attempts and durable business audit | The hot pizza run does not establish large-state, crypto, watch-fanout or distributed-transaction capacity |

**SDK redesign, 2026-09-24.** The TypeScript SDK was redesigned without backward compatibility; the server contracts above are unchanged apart from structured failures. Applications declare `define({ uses, collections, definitions, tasks, triggers, keys, http, auth })`. Schemas validate method arguments, records and typed keys inside callbacks. `fail(code, message, details)` reaches callers intact: `422 EVALUATION_FAILED` with `failure`, `403 FORBIDDEN` with the authorization hook's failure, `422 TRANSACTION_ABORTED` with a participant's failure (also stored in the coordinator record), SSE `error` events, and maintenance error handlers, which now receive the real code instead of `MAINTENANCE_FAILED`. Tasks from components compile into one maintenance handler with per-task backoff; `auth` and per-method `access` compile into the single authorization hook. The scheduler, `queue()` (replacing `workQueue`, with renewal, retries, delays and per-scope fencing) and expiring collections are components; `external()` values, `runQueueWorker`/`reconcile`, typed `FlowerClient<typeof app>` calls with retries and subscriptions, `FlowerAdmin` for operator calls, and an in-process testing module complete it. The review below predates this; its links to specific lines of `sdk/keys.ts`, `sdk/scheduler.ts` and `sdk/temporal.ts` point at earlier code.

The larger conditional experiments remain separate: disk-backed application MVCC, finer transaction locks, a durable SSE event log, a richer versioned value model, and a more extensive background checkpoint engine. Application checkpoint commits now defer their own fsync and are written in the background after in-memory publication. A leader of three or more voters also defers its own log flush, since its followers alone form the commit quorum. Follower logs, votes, snapshots, and purge boundaries retain immediate durability. A quorum-confirmed startup serving fence reconstructs committed work when the recovered checkpoint lags. This is a change to checkpoint durability and recovery within the existing storage engine, not a separate WAL or new storage engine. JSON compatibility remains unchanged. The original durability discussion below records the pre-change design and its required safety conditions.

The integrated stress test exposed a batch-controller feedback failure: applying an expired request-age budget as a zero preparation window permanently reduced an overloaded queue to singleton commits. The controller now returns to queue-pressure batching when the requested latency cannot be met, while honoring preparation and maintenance boundaries. The regression and its before/after measurements belong with the benchmark evidence.

## Original review

Keep the foundation: isolated QuickJS-NG in reusable Wasm, Rust-owned state and dependency machinery, leader-evaluated patches replicated through Raft, code-owned public methods, and explicit read consistency. Moving to another JS engine or replicating callback execution would not address the most consequential gaps found here.

The next design should make **identity, authorization, resource admission, and dependency validity** explicit. Those boundaries support both correctness and performance. They are more useful than adding isolated caches and concurrency knobs.

## What the review established

| Finding | Classification | First change |
| --- | --- | --- |
| Receipt replay skips authorization inside the original callback | Observed behavior with a security consequence | Always-run authorization stage; credentials outside intent |
| Shared-key handles expose OS-random tokens to read-only callbacks | Concrete purity gap in managed-key work | Opaque native handles |
| Managed NaCl decryption cannot select pre-rotation versions | Concrete key lifecycle gap | Versioned encrypted payloads or restricted version handles |
| Retry/transaction history accumulates; old backups roll back fences | Lifecycle protocol missing | Incarnation and retirement rules before GC |
| Queued queries retain snapshots before worker admission | Resource admission limitation | Node-wide byte/work admission before snapshot capture |
| Timer and lease selection scans and sorts whole collections | Algorithmic cost | Ordered indexes and bounded range reads |
| Watches of results that read `ctx.now()`, or of databases with materialized values that read the clock, wake on every commit | Reuse limitation | Declare change times with `ctx.changesAt()` |
| Full snapshots hold the state lock; moves freeze before full copy | Growth and availability limitation | Immutable snapshot roots, disk streaming, then pre-copy |
| Prepared transactions still lock a whole logical database, not records or ranges | Deliberate concurrency tradeoff | Finer intents or OCC, if demand justifies them |

The audit did not establish a current Raft durability defect. The current store durably commits applied changes, receipts, and the applied log position together before publishing them. Keep that invariant until an alternative recovery protocol is proved.

## 1. Make authorization a runtime stage

Today an application can verify a JWT inside a mutation. But [receipt lookup](src/service/writer.rs#L376) returns the old result before invoking that callback. Expiry, session revocation, or permission changes checked only there are therefore not checked on replay. The effects are not repeated; the historical response is returned. Refreshing a token inside the arguments instead changes the fingerprint and conflicts with the original request ID. The transaction path has the same shape.

Add a pure, code-defined authorization hook orchestrated by Rust. It receives credentials separately from business arguments and produces an immutable principal and authorized logical-database scope. Public methods remain the only application data interface. Internal calls inherit or explicitly attenuate that authority rather than accepting an arbitrary caller-supplied principal.

The request path becomes:

```text
bounded transport admission
  -> resolve logical database and deployed method policy
  -> authenticate and authorize
  -> validate identity / retry window / current receipt ownership
  -> replay an authorized result OR evaluate the method
  -> validate authorization and data dependencies at the commit boundary
```

Authorization runs before receipt replay, query-cache access, and watch delivery. Bind receipt ownership to a stable principal and database, and hash only business intent and explicit preconditions. Refreshing credentials can then recover the same original result. Permission to execute a method and permission to retrieve its old result must have an explicit relationship; default to requiring current permission for both.

Keep state-changing checks, such as consuming a one-time challenge or decrementing a quota, inside the atomic mutation. The pure hook can read policy but cannot be the place for an external side effect or a supposedly once-only operation. Its read dependencies must remain valid for the mutation's serialization point; a hook checked against stale policy cannot bless a later commit after revocation.

Authorization freshness and application-data freshness are separate choices. A replica-local product listing may be acceptable while its authorization must use fresh policy. Today declaring any managed key forces even replica-local queries fresh; refine that only after current policy and data dependencies are separable. Offline cached authorization, if ever supported, must have an explicit revocation-delay contract.

For watches, expiry and revocation must invalidate authorization even when business data does not change. Reauthorize before releasing a new event, bound already-buffered output, and document the point at which an event was authorized. No system can retract bytes already delivered.

Also separate peer identity, operator authority, and application identity. Current peer traffic uses cleartext HTTP/2 and the shared operator bearer token. Support authenticated encrypted peer transport, distinct scoped operator credentials, and authenticated forwarding assertions. A proxy is viable if its identity channel is trusted. These changes reduce authority sharing; they do not make Raft tolerate malicious replicas.

**Validation:** expired-token receipt replay, refreshed-token retry, revoked session, changed tenant claim, alias removal, cached read after policy change, and an idle watch crossing token expiry. Check that invalid attempts neither reveal another principal's result nor execute an intent twice.

## 2. Finish the crypto abstraction at its boundaries

Two current managed-key details need correction before treating the abstraction as complete:

- [Shared handles](sdk/keys.ts#L15) have an observable `.token` generated from [OS randomness](src/evaluator/wasm/crypto.rs#L393), including during queries and derived evaluations. Calling `nacl.box.before(...).token` exposes nondeterminism despite the mutation-only entropy rule. A throwing `toJSON()` does not hide a property. Use a QuickJS native class with a hidden invocation-local handle, deterministic internal allocation, no observable random identity, and explicit serialization rejection. Guard the raw bridge as well as SDK helpers.
- [Historical key selection](src/crypto/managed.rs#L1055) currently applies to JWT verification/decryption, while managed NaCl `open` selects the active version. Rotation therefore makes existing NaCl ciphertext inaccessible through the normal managed API. Add a native sealed envelope containing authenticated format/algorithm, logical key/version, nonce, and ciphertext. A version-qualified handle restricted to verify/decrypt is another useful low-level primitive. Never let an untrusted key ID select outside its authorized binding. Preserve raw NaCl compatibility as an explicitly lower-level contract.

Keep secrets, entropy sources, and mutable crypto state outside reusable Wasm images. Native prepared-key reuse is valuable, but no cache hit grants authority: resolve against the current permitted key version and usages. Move expensive cold preparation outside global cache locks, with bounded per-key single-flight, before adding remote providers.

Add wrapping-key rotation/rewrapping and provider-aware migration as lifecycle work. Provider I/O belongs outside deterministic Raft application. Adopt explicit retire/revoke/destroy and backup semantics from [RETENTION.md](RETENTION.md#5-treat-key-lifecycle-separately); managed keys do not protect plaintext application values or receipts produced from them.

**Validation:** opaque-handle serialization and forging, repeated pure evaluations, rotation followed by old-ciphertext decryption, bound-key isolation, revocation with warm caches, wrong-provider recovery, and restart/restore policy reconciliation.

## 3. Admit and schedule work before it retains state

[Queries capture their snapshot](src/service.rs#L284) before acquiring their evaluation permit. The semaphore bounds running callbacks, not queued bodies, old snapshot roots, or waiting responses. Each named partition also has its own writer queue and preparation permit, so adding partitions multiplies potential work. More HTTP/2 streams are not a memory or fairness policy.

Introduce node-wide resource admission, charged for queued bytes, estimated CPU, native/guest memory, retained versions, and output. Acquire the relevant budget before snapshot capture, with incremental bounded request-body parsing. Use a global preparation pool and weighted fair logical-partition lanes. Preserve ordering within a lane; authenticate classification so callers cannot obtain more capacity by inventing tenant names.

Reserve resources for Raft/control traffic, recovery, and maintenance. Apply and recovery must not depend on an ordinary-client permit that saturated clients can hold indefinitely. Use separate ingress limits for unauthenticated traffic and fair scheduling after identity is known. Cancellation releases queued work and references promptly; cancellation after commitment remains an uncertain client outcome, not an abort.

Keep the selected **eight benchmark groups and 200 ms maximum adaptive preparation window**. Make the [batch controller](src/service/writer/batching.rs) also consider oldest request age, maintenance deadlines, follower/apply lag, and bytes. A 200 ms ceiling is not a 200 ms response-time promise. Flush earlier when a latency budget is exhausted, and give due maintenance a bounded share rather than only whatever capacity remains after customer batches.

These are resource-based budgets, not arbitrary small fixed API limits. Under sustained overload, reject before commitment with an explicit retryable overload response. Report queue age and rejection separately from execution latency.

**Tradeoff:** fair admission can reduce peak hot-tenant throughput while keeping memory and other tenants' latency controlled. The measured configuration already shows mutation p99 of 481.5 ms and oven lateness p95 of 2.16–2.46 seconds; those observations motivate investigation, but do not prove which scheduling component caused them.

## 4. Add one general ordered-index primitive

The scheduler [scans and sorts all timer records](sdk/scheduler.ts#L134), and successful callbacks select a due timer both before and after execution. [Lease claims](sdk/temporal.ts#L243) have the same whole-collection shape. This cost grows with retained jobs even when only one is due. Existing equality indexes still materialize every matching row.

Add Rust-owned ordered tuple indexes with deterministic typed ordering, prefix/range bounds, direction, limit, and continuation. Missing values, mixed types, byte/string ordering, and unique tie-breakers need specified semantics. Composite keys and index keys should use this same comparison contract.

Then implement `(state, dueAt, id)` timer selection and ready/lease-expiry selection in TypeScript using bounded index reads. Keep retry policies, leases, TTL, and delayed business logic in TS. Native code supplies efficient data access and a wakeup hint, not a new special-purpose scheduler language. A wakeup is advisory; a committed mutation rechecks due work and claims it authoritatively.

Pagination must say whether it pins a snapshot or continues on newer snapshots. Snapshot pagination needs expiry and retained-version accounting. Live pagination is cheaper but can miss or repeat rows after concurrent changes. Bind cursors to logical/history identity, ordering, index schema, and their chosen consistency contract; reject incompatible cursors explicitly.

Range dependencies must cover inserts into previously empty intervals, not just rows returned. This is needed for both correct reactivity and the concurrency design below.

**Validation:** equal deadlines, empty ranges followed by insertions, updates crossing bounds, lease reclamation, cursor expiry, deployment, and migration. Measure selection with thousands to millions of future timers while the number due stays fixed. Index maintenance adds write and storage cost; the expected gain is avoiding collection-size work on each claim.

## 5. Share a dependency certificate across caching, reactivity, and concurrency

Today the [query cache](src/service/query_cache.rs) discards reusable work at every new revision. The [reactive graph](src/evaluator/rust_engine/graph.rs#L225) dirties the transitive closure before discovering whether an intermediate output actually changed. These are conservative choices, but they reconnect unrelated tenants' work.

Have native evaluation produce a certificate describing what the result observed:

- Present and missing keys, with versions.
- Collection/index intervals, including phantom insertions.
- Derived outcome versions and dependency edges.
- Code, schema, alias, and key-policy identities.
- Authorization scope and time sensitivity.

Use it in three stages:

1. **Query reuse across unrelated commits.** After the required read fence and current authorization, validate dependencies against the selected snapshot. Reuse the value if still valid. Keep immutable values and reusable encoded responses to avoid parsing cached JSON only to encode it again.
2. **Stop unchanged reactive propagation.** Mark values possibly dirty, evaluate affected dependencies first, and continue only when observed outcomes change. Update dependency edges even when a changed branch returns the same value. Errors, cycles, rollback, and clock dependencies need the same rigor.
3. **Bounded optimistic mutation preparation.** Evaluate independent candidates concurrently against immutable snapshots. An ordered sequencer validates each certificate against preceding staged/committed writes, then accepts or recomputes it. Final patches and all materialized effects still publish atomically. Strict `expectedRevision` remains strict.

Introduce the certificate for reads first. It is easier to prove and measure there than to debut it as a concurrent writer. Broad scans still invalidate broadly; certificates do not invent independence. Shared derived values, blind writes, negative reads, index changes, deployment, key revocation, and prepared transaction locks all participate in validation. Conflicting speculative entropy/results must be discarded and never exposed before commitment.

Named logical partitions already have independent application writers; use that concurrency through the fair global pool before speculating within one partition. Consider physically batching independent partition proposals into one durable Raft entry without implying new cross-partition transaction semantics. Hot-key lanes may gain nothing from speculation: measure invalidation rate and fall back adaptively to ordered preparation.

Time remains explicit. An HLC/logical clock can order events but cannot prove real elapsed lease time. `ctx.changesAt()` now replaces repeated polling for declared thresholds: watches and maintenance sleep until the earliest one. Arbitrary `ctx.now()` calls retain their time-sensitive, polled semantics.

**Validation:** use a serial reference evaluator and randomized histories with negative reads, range phantoms, branch changes, errors, topology changes, key revocation, deployments, and hot-key contention. An unchecked larger semaphore is not a concurrent-writer implementation.

## 6. Share watched values, not just query-cache misses

[Watches](src/service/watch.rs) share a hub keyed by logical database, method and arguments, deployment and admitted principal. It evaluates once for an eligible snapshot and time cohort, encodes once, and fans out immutable events to bounded subscriber queues, so one slow receiver cannot hold up the hub or retain an unbounded chain of old versions.

Each publication names the keys it wrote. A hub wakes only when one of them is something its current result's dependency certificate observed, or a range it scanned; a subscriber's access check wakes on what its authorization read. Watches on unrelated rows cost nothing per commit: Trinity's bench kept 270-289 completions/s with or without 3,000 idle watches, where every open watch used to re-authorize and re-evaluate on every commit. What remains per wake is per subscriber: admission, a read fence and authorization.

A new fresh subscriber must join a newly fenced cohort; it cannot receive an arbitrarily old hub value merely because that value was fresh when somebody else subscribed. Authorization-sensitive result sharing must be explicit and scoped correctly. Each replica can host hubs locally without a new global coordinator.

Optionally retain a byte/time-bounded event ring with generation and sequence cursors. Resume only while that exact history is available; otherwise send a full snapshot. Deployment, migration, or restart may reset the generation. These remain current-value watches that may coalesce intermediate states. Durable event delivery belongs in an outbox collection and worker protocol.

**Validation:** identical and unique watches at increasing fanout, slow consumers, idle expiry/revocation, failover and resume reset. Measure evaluation CPU, encoding/diff CPU, retained bytes, and time from commit to delivery separately.

## 7. Design for state growth and operational change

Each replica serves the application and receipts from redb read snapshots, beneath an in-memory overlay of writes not yet persisted ([bench/FOOTPRINT.md](bench/FOOTPRINT.md#serving-from-redb)). [Snapshot construction](src/consensus/store.rs#L1417) holds the state guard through full serialization and durable snapshot writing. Chunked migration transport still constructs complete images, and the source freezes before exporting. Large tenants will encounter a different limit from the current small hot workload.

First capture an immutable root at a specific applied log position, serialize checksummed chunks outside the apply lock, and stream to disk-backed snapshot storage. Installation and publication need generation checks: simply removing the existing lock would let an old builder overwrite a newer installed snapshot. Retain the necessary log prefix until a complete durable snapshot is safely published. Trigger snapshots by bytes, age, and measured duty cycle as well as entry count; an adaptive batch makes one entry a poor unit of cost.

Then pre-copy an immutable tenant image while the source remains active, followed by a bounded stream of committed changes. Briefly freeze only to catch up, verify the final revision/hash, check destination key readiness, and perform epoch-fenced ownership cutover. Retain stop-and-copy as the simpler fallback. Migration, backups, and catch-up must share admission so none starves foreground Raft work.

Data larger than RAM is served from redb read snapshots with a bounded cache of parsed values; received snapshots are spooled to disk as they decode. Retained reader snapshots pin redb pages rather than memory, but they and pagination still need accountable lifetimes.

Deployment now has durable index backfill and adaptive graph rebuild pages, dual maintenance, and atomic code/index/graph cutover while the prior deployment serves. Append-only growth reuses topology proofs, and target maintenance avoids wasted speculative preparation. Remaining limits include one indivisible root evaluation, scan-window memory, full validation after existing topology edits, and activation-time clock/key refresh. Explicit application migrations still need versioned progress, cancellation/rollback rules, and resource budgets. Measure foreground latency and rebuild throughput together using the [staged deployment benchmark](bench/STAGED_DEPLOYMENT.md).

A separate durability experiment could make the Raft log the durable authority and checkpoint applied state asynchronously. That requires a complete replay-before-serving design, safe log retention, and a reliable committed-prefix rule; replaying every durable log entry is incorrect because the tail may be uncommitted. This is not permission to switch off the existing application fsync. Benchmark flush costs before committing to that complexity.

## 8. Make distributed transactions follow logical data

Transactions now target root group databases or stable named partitions. Before contacting participants, the coordinator resolves each named target, refuses one past the copy phase of a move, and durably pins its placement epoch and bootstrap addresses in the coordinator record ([targets.rs](src/service/transactions/targets.rs)). Participant RPCs carry that epoch; one for a stale epoch fails and the transaction aborts. Prepared barriers cover only the participating logical database, so other named partitions on the same physical group keep running. Coordinators are locked per request ID rather than globally ([transactions.rs](src/service/transactions.rs)). A participant already prepared for another transaction rejects a new preparation, and the coordinator aborts instead of waiting, so conflicting transactions cannot form distributed wait cycles. Targets are prepared one at a time in a fixed order, so two conflicting transactions first meet at their first shared target, and its winner cannot then be aborted by the loser. Decisions reach all participants concurrently; one slow participant does not hold the others' locks. Catalog lookups for a plan run concurrently, and catalog and partition RPCs reuse pooled connections.

Remaining limits: the prepared lock still covers a whole logical database rather than records or ranges. It blocks ordinary reads, watches, mutations, deployment, and maintenance there, and admits one prepared transaction at a time even for disjoint keys. Finer intents, or OCC built on the dependency certificates, can follow if demand justifies them. A conflicting preparation becomes a durable abort that callers retry under a new request ID; nothing queues or prioritizes competing transactions. Ordered preparation makes latency grow with participant count; parallel preparation would trade that for mutual aborts under contention. Planning holds the coordinator's writer lock across one round of catalog lookups. Migration freeze waits for prepared work and incomplete coordinators rather than transferring protocol state.

Preserve durable decisions, leader-term fencing, and recovery. A timeout cannot safely free a prepared participant; an unavailable coordinator still blocks its participants. Transaction closure and GC use their own protocol from [RETENTION.md](RETENTION.md#4-collect-transaction-history-through-a-separate-closure-protocol). Argument-only planning is already enforced; it is not an identified correctness hole.

Prefer co-location for ordinary atomic operations. For external effects, store an outbox item in the same local transaction, then use TS workers with downstream idempotency and sink-enforced fencing. Lease expiry in Flower cannot stop a paused worker from later calling an external service. Restore must also invalidate capabilities from the old history. There is no general exactly-once external side-effect guarantee without the destination participating.

## Measurement and implementation order

The published run reached **50,771 customer calls/second across eight groups**, using 70% replica-local reads and only 96 retained orders per group. It is useful evidence for a hot workload, not for large state, many distinct queries, SSE fanout, online migration, crypto-heavy traffic, or cross-group transactions. No new performance run was made for this review.

Use open-loop offered-rate curves alongside the existing closed-loop test. Include offered, admitted, completed, rejected, and timed-out calls; measure latency from intended arrival so saturation cannot hide behind a driver that stops issuing work. Report durable mutation goodput separately from cached/local reads, per-tenant tails, queue age, timer lateness, retained bytes, snapshot duty cycle, follower lag, and recovery under load. Keep independent business-ledger audits.

Recommended sequence:

1. **Close correctness and lifecycle gaps:** authorization/retry separation, opaque handles, ciphertext versions; specify incarnation and restore fencing before implementing retention GC.
2. **Bound costs:** node-wide admission, fair/deadline-aware scheduling, expiry-safe receipts, ordered index reads. Keep TS business policy.
3. **Reuse work:** dependency certificates, unchanged-output propagation, shared watch hubs. Stream snapshots outside the apply critical section.
4. **Add concurrency where measured:** independent partition coalescing first, bounded speculative preparation next, logical-partition transactions where needed.
5. **Remove growth limits deliberately:** pre-copy migration, online backfill, disk-backed state, and a log-authoritative durability experiment if profiles justify them.

Keep fault-injection and model-based checks beside these changes: lost responses, delayed packets, crashes around durable publication, clock jumps, overloaded tenants, revocation during retries, migration during recovery, GC races, and restore with old capabilities. Performance gains are acceptable only with the corresponding invariants intact.

Broader value types are a separate compatibility decision. If adding BigInt/bytes/typed tuples, specify one versioned value model across QuickJS, Rust, request fingerprints, index ordering, snapshots, SDK codecs, and SSE patches. Differential codec tests matter more than choosing a faster binary format in isolation. Do not let a serialization experiment block the concrete improvements above.
