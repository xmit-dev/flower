# Transactions between logical databases

A TypeScript transaction method coordinates exposed methods across root group databases or stable named partitions using durable two-phase commit. Logical targets may share a physical Raft group or live on different groups. Prepared barriers are scoped to the participating logical database; unrelated named tenants keep running. This coordinator module, `transfer.ts`, moves money between two named partitions:

```ts
import { define, participant, transaction, v } from "@flower-js/sdk";
import type bank from "./bank.ts";

const west = participant<typeof bank>({ partition: "west" });
const east = participant<typeof bank>({ partition: "east" });

const transfer = transaction("bank.transfer", {
  args: v.object({ from: v.string({ min: 1 }), to: v.string({ min: 1 }), cents: v.int({ min: 1 }) }),
}, ({ from, to, cents }) => ({
  calls: [
    west.call("debit", { account: from, cents }),
    east.call("credit", { account: to, cents }),
  ],
  value: { transferred: cents },
}));

export default define({ http: { transfer } });
```

Each participant partition deploys ordinary exposed mutations, here as `bank.ts`:

```ts
import { collection, define, fail, mutation, v } from "@flower-js/sdk";

const accounts = collection("accounts", v.object({ cents: v.int({ min: 0 }) }));
const movement = v.object({ account: v.string({ min: 1 }), cents: v.int({ min: 1 }) });

const debit = mutation("debit", { args: movement }, (ctx, { account, cents }) => {
  const balance = ctx.get(accounts, account)?.cents ?? 0;
  if (balance < cents) fail("INSUFFICIENT_FUNDS", `Account ${account} holds ${balance} cents`, { account, balance });
  ctx.set(accounts, account, { cents: balance - cents });
  return { account, cents: balance - cents };
});
const credit = mutation("credit", { args: movement }, (ctx, { account, cents }) => {
  const balance = ctx.get(accounts, account)?.cents ?? 0;
  ctx.set(accounts, account, { cents: balance + cents });
  return { account, cents: balance + cents };
});

export default define({ http: { debit, credit } });
```

`participant<typeof bank>({ partition: "west" }).call(alias, args)` builds a `{partition, method, args}` call whose alias and arguments are typed by the participant's module; transaction aliases are excluded. `participant({ group: "name" })` targets a configured physical group’s root database instead, and plain `{partition|group, method, args}` objects remain valid; specify exactly one of `partition` or `group`. Named targets require `FLOWER_CATALOG_GROUP`, and the runtime resolves and durably pins their placement epoch and bootstrap addresses before contacting participants. Validate balances and amounts inside the participant methods. A failed call aborts the whole transaction, including earlier successful calls. Participants may also call exposed queries; a later call in the same logical target observes earlier staged writes.

A planner receives only its arguments, after its `args` schema validates them. It cannot read database state, access a context, perform I/O, or call another transaction. The returned plan is fixed before execution; one participant's result cannot dynamically choose another participant's calls. A planner failure, including `INVALID_ARGUMENT`, returns `422 EVALUATION_FAILED` with its `failure` before any participant is contacted. Internal definition names are not callable unless explicitly exposed as HTTP aliases.

```ts
import { FlowerClient, FlowerError } from "@flower-js/sdk";
import type coordinator from "./transfer.ts";

const client = new FlowerClient<typeof coordinator>("http://coordinator-member:7101");
const requestId = crypto.randomUUID(); // Save this before sending.
try {
  const receipt = await client.call("transfer", {
    from: "alice", to: "bob", cents: 500,
  }, { requestId, retry: true });
  // receipt.value = {
  //   results: [debitResult, creditResult],
  //   value: { transferred: 500 },
  // }
} catch (error) {
  if (error instanceof FlowerError && error.code === "TRANSACTION_ABORTED") {
    // error.failure: { code: "INSUFFICIENT_FUNDS", message: "...", details: { account: "alice", balance: 300 } }
  }
  throw error;
}
```

`results` follows the original call order. `value` is the optional value returned by the planner, including an explicit `null`. The receipt's revision belongs to the coordinator logical database; it is not shared across participants. `expectedRevision`, when supplied, checks the coordinator revision when a new plan begins.

When a participant method fails, the caller receives `422 TRANSACTION_ABORTED` whose `failure` is that method's `{code, message, details?}`, and the error message repeats `CODE: message`. The durable coordinator record keeps the failure, so a retry with the same request ID returns the same abort and failure. Aborts caused by conflicts, unavailable or uncertain participants, or coordinator recovery carry a message and no `failure`. With a typed client, transactions go through `call`, since `mutate` accepts only mutation aliases.

## Configure and operate

Configure every node with its own group name, the same group registry, and a shared peer credential. Operator credentials remain separate:

```sh
export FLOWER_GROUP=west
export FLOWER_GROUPS='{"west":["west-1:7101","west-2:7101","west-3:7101"],"east":["east-1:7101","east-2:7101","east-3:7101"]}'
export FLOWER_ADMIN_TOKEN='your-operator-secret'
export FLOWER_PEER_TOKEN='your-shared-peer-secret'
flower --id 1 --listen 0.0.0.0:7101 --advertise west-1:7101 --data ./west-1
```

Initialize each group separately with only its own members. Use `FLOWER_GROUP=east` on east nodes. Registry addresses are `host:port`, without a URL scheme or path. Addresses must be unique across the registry. Keep the registry and shared peer credential consistent, and retain an address list through leadership changes. The coordinator tries configured peers; the SDK may target any coordinator member; the server forwards leader-bound work.

Cross-group RPCs use pooled HTTP/2 connections (verified HTTPS when native TLS is configured) and require the peer bearer token, the expected group identity, and the same wire/state/snapshot/value/QuickJS compatibility contract as ordinary peer traffic. Protect this operator network as described in the operating guide. Public callers only invoke deployed method aliases; they do not receive an arbitrary transaction endpoint.

`FLOWER_TRANSACTION_MAX_BYTES` bounds durable transaction records and patches; `FLOWER_RPC_MAX_BYTES` bounds internal exchange bodies. Existing read, commit, and connection budgets apply. All calls in one participant preparation share a single `FLOWER_EVALUATION_TIMEOUT_MS` deadline, including evaluator queue waits; adding more calls does not multiply that budget. Authentication and compatibility are checked before parsing an internal request body. The runtime checks that a prepared patch and the eventual receipt can be stored before deciding to commit. It reserves enough safe integer revisions to finish outstanding transactions, preventing unrelated writes from consuming their final revisions.

## What a transaction guarantees

The coordinator durably records every planned participant before contacting any of them. It prepares distinct logical targets one at a time in deterministic order, preserving call order within each target. Independent request IDs have separate coordinator locks; conflicting preparations abort rather than waiting for another transaction’s lock, avoiding distributed wait cycles. Because every coordinator uses the same order, of two conflicting transactions the one that prepares their first shared target is not aborted by the other. Participants evaluate into private state, then replicate a prepared lock, patch, and results. Once every participant is prepared, the coordinator replicates an immutable commit decision and delivers it to all participants concurrently. Participants independently read that decision through the coordinator's quorum before applying their patches. Success is returned only after every participant confirms completion and the coordinator stores the retry receipt.

Once a node applies a prepared lock, it blocks ordinary reads, watches, mutations, deployment, and maintenance in that logical database until it applies the durable decision. Fresh reads establish a quorum barrier and therefore observe that lock. Opt-in replica-local reads and watches may be on a lagging replica that has not applied the lock or final commit; they retain their explicitly stale semantics and are excluded from the cross-group atomic visibility guarantee. Existing reads that already acquired a snapshot can finish against that snapshot. An affected watch reports an error and closes; reconnect it after the group becomes available. A transaction touching several groups can therefore temporarily reduce availability across all of them, including unrelated keys in those logical databases. Other named partitions on the same physical group have independent writers and remain available.

For fresh reads, these barriers prevent a new read from exposing an uncommitted participant patch. They do not create a global snapshot for independent client requests: two separate reads at different times can straddle a completed transaction. Put related reads into one transaction when they require the participant barriers. Results from a query-only transaction use the same prepare/decision protocol.

## Failure and retry

- After a timeout or lost response, retry the same arguments with the **same request ID**. A completed commit returns its original receipt with `duplicate: true`; changed content returns `REQUEST_ID_REUSED`.
- `TRANSACTION_ABORTED` is durable for that request ID. Retrying it keeps the abort and its `failure`. After correcting the cause, start a new attempt with a new ID. The SDK's `retry` option never retries it.
- `TRANSACTION_PREPARED` means a participant is waiting for its coordinator's durable outcome. Restore the coordinator's quorum/connectivity and retry; the SDK's `retry` option treats this `503` as transient. There is no timeout that discards a prepared transaction.
- A coordinator recovering an unfinished preparation durably aborts it. A committed decision is never rolled back. Recovery repeatedly finishes decided transactions, including after leadership changes or process restarts.
- An uncertain prepare causes a durable abort that is sent to every planned participant. A participant captures its Raft leader term, then checks the coordinator decision while holding its writer lock before preparing. The replicated state machine rejects a preparation submitted in another term, including after a former leader returns. If an abort finds no prepared work, it can acknowledge without a write: delayed prepares must observe the immutable abort. Participants that did prepare atomically retain a completion tombstone when clearing their lock.

Coordinator records, completion receipts, and participant tombstones are retained durably. Operator-driven close/collect now reclaims detail behind durable history floors; deleting metadata manually still breaks retry and delayed-message fencing. Each coordinator assigns a random history and monotonic sequence. Close persists a completed-prefix intent, obtains durable rejection-floor acknowledgements from all participants, then advances the coordinator floor. Delayed prepare/finish messages are rejected after collection. Committed public receipts remain available under their retry contract; an aborted decision cannot close until its original request ID is inadmissible. Use `FlowerAdmin.transactionClosureStatus()` and `controlTransactionClosure({operation:"close"|"collect",maxBytes?})`; close also accepts `through`. Repeated calls make bounded, monotonic progress, reported through pending/blockedReason/deletedRecords. Collection runs per coordinator/participant database. See [RETENTION.md](RETENTION.md#4-collect-transaction-history-through-a-separate-closure-protocol). Prepared state and decisions travel with normal Raft snapshots and backups. Tenant cutover waits for prepared work and incomplete coordinators to complete; status and completion messages remain allowed during movement so recovery can release those barriers. Completed decisions and receipts move with the tenant, preserving replay and abort identity. Restoring only one group from an older, unrelated backup is not a cross-group restore protocol.

## Validation

```sh
cargo test --lib service::transactions::tests -- --test-threads=1
cargo build
node tests/e2e-transactions.mjs
node tests/e2e-partition-transactions.mjs
```

The focused tests cover private staging, abort fencing, idempotent decisions, concurrent decision delivery, unavailable coordinators, recovery after lost prepare acknowledgements and durable commit decisions, authentication/compatibility, JSON depth, and revision reservations. The process test uses two independent three-node groups and exercises atomic commit/abort, result ordering, fresh replica reads, coordinator death while the first participant is prepared and the second is unavailable, receipt replay after leader failure, and a complete cluster restart.

Application authorization runs before external transaction receipt replay and planning. The durable coordinator stores the resulting principal, excluding raw credentials. Each participant’s current hook sees the public alias and a server-supplied delegation `{coordinator,principal}` and decides whether to accept it; named participants must return their own tenant scope. In a `define({ auth })` hook, `auth.delegation(ctx, coordinator, principal)` makes that decision (by default every cluster peer is trusted), the accepted principal acts with the participant partition as its tenant, and the method's `access` policy still applies to it. Refreshing credentials for the same subject/tenant preserves the original intent, while revoked authorization blocks replay. A planner still receives only arguments, never a context or participant results.
