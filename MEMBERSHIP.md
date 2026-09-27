# Change members and roll compatible builds

Flower keeps voter sets, learner addresses and membership log IDs in the Raft
log and snapshots. Configuration survives restart. Application methods remain
the only external data interface; the routes below are operator operations and
require the node’s `FLOWER_ADMIN_TOKEN` bearer token. Internal peers instead use
`FLOWER_PEER_TOKEN` (defaulting to the operator token when omitted).
[Native TLS](TLS.md) covers certificates, trust roots and credential rotation.

Inspect any node with `GET /raft/membership`. The response includes `leader`,
`logId`, `lastApplied`, `voterConfigs`, `nodes`, and `compatibility`. This is local
operational status, not a linearizable application read. A normal configuration
has one voter set; two sets mean a joint transition is in progress.

## Add, replace or remove voters

Start a new server with a new positive node ID, a distinct advertised address,
an empty data directory, the cluster's peer token, and appropriate operator/TLS configuration. Do **not** initialize
it. Send the current leader the complete desired voter set:

```sh
curl --fail-with-body http://127.0.0.1:7101/raft/membership \
  -H "Authorization: Bearer $FLOWER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"members":{"1":"127.0.0.1:7101","2":"127.0.0.1:7102","4":"127.0.0.1:7104"}}'
```

For optimistic concurrency, include `expectedLogId` with the exact `logId`
object returned by inspection. The precondition is checked before learner
registration. Leaving it out requests the desired voter set against the state
the leader observes when it starts the operation.

The leader validates identities, unique addresses and the compatibility
contract before making changes. Unknown peers with existing Raft state are
rejected: this API cannot combine clusters or silently reincorporate an old
member's directory. Existing IDs cannot move to another address; replace them
with fresh IDs. It then registers missing peers as nonvoting learners, waits
for every proposed voter to replicate a committed prefix captured after
registration, and uses OpenRaft's joint consensus transition to replace the
voter set. The old configuration still needs a quorum during this operation.
Removing a failed voter is therefore possible only while the surviving old
voters can still form one.

The example replaces voter 3 with voter 4. Removed voters are not retained as
learners. A previously registered learner outside the requested voter set may
remain a learner; `members` describes the complete **voter** set. New learners
left by an interrupted call can be reused in a retry with the same IDs and
addresses. Stop retired servers and retain their directories for recovery
until you have verified the new configuration and data.

Membership calls are serialized on the receiving leader and bounded by
`FLOWER_COMMIT_TIMEOUT_MS`, including waiting for that serialization slot.
A timeout, disconnected client, lost quorum or changed leader can leave added
learners, a joint configuration, or the final configuration committed. Inspect
before retrying. A joint transition can only be resumed toward its existing
target voter set; finish that transition before proposing another. A successful
response reports the exact committed final membership and its log ID.

Removing the leader is supported, but its demotion can make the response
uncertain. Inspect a surviving member and send subsequent work to the new
leader. This API does not implement loss-of-quorum recovery or forced quorum
replacement. Keep three or more voters when one-node failure tolerance matters;
one or two voters cannot tolerate one unavailable voter.

## Compatibility is a protocol contract

Every Raft append, vote, snapshot and fresh-read-fence RPC carries a checked
node identity and `x-flower-compatibility`. The receiver rejects a missing or
different contract with HTTP 426 **before** decoding/processing the RPC. The
sender also verifies the contract and identity on successful replies. This
applies continuously to known and restarted peers, not only to new learners.
The authenticated, identity-checked `GET /raft/version` probe reports a peer's
advertised identity, initialized status and full compatibility descriptor.
Operators can inspect that descriptor through `/raft/membership` without
already knowing the compatibility header.

Compatibility requires equality of `raftWire`, `stateMachine`, `snapshotFormat`,
`valueFormat`, and the SHA-256 digest of the embedded QuickJS Wasm module.
`build` is diagnostic: a build-version difference is allowed when all contract
fields match. Release authors must increment the relevant contract version
before publishing changes to replicated commands, evaluator semantics,
snapshot encoding or value semantics. The gate cannot infer whether a Rust
code change is semantically compatible. Changing the QuickJS guest always
changes the contract. Compatibility also requires a mutually readable local
database format when a node is restarted; the peer contract is not a storage
converter and cannot make a one-way storage upgrade reversible.

These gates are introduced in this release. Older builds lack the header and
cannot participate in a rolling deployment with it. Moving from a pre-gate
build requires a coordinated whole-cluster upgrade and a compatible local
storage format. This release does not include a storage migration tool or
support older redb file formats. Do not initialize a restored cluster. No
arbitrary earlier Flower version is claimed compatible.

## Roll a compatible build

For builds that advertise the same contract and can open the existing local
storage format:

1. Back up the cluster and verify a healthy quorum, a uniform voter set, and
   compatible contract fields. Leave application deployment and membership
   changes out of the rolling maintenance window.
2. Remove one follower from application routing. Send SIGTERM or SIGINT
   (Ctrl+C) and wait for process exit: the server stops accepting and drains
   active HTTP bodies for up to `FLOWER_SHUTDOWN_TIMEOUT_MS` (30 seconds by
   default). It then closes remaining connections, including SSE, shuts down
   Raft, and waits for durable storage to drain. The setting bounds the HTTP
   drain, not disk I/O. Interrupted clients retry their original request IDs
   (the SDK's `retry` option does so); watch clients reconnect on another
   replica with a fresh snapshot (`subscribe()` reconnects by itself, rotating
   through its `queryUrls`).
3. Restart that node with the new binary, the **same** ID, address, token and
   data directory. Do not initialize or re-add it. Confirm it catches up and
   serves a fresh application query before putting it back into routing.
4. Repeat for the other followers one at a time, then restart the leader last.
   The surviving quorum elects a leader. Calls interrupted during election can
   be retried with their original request IDs; watches reconnect with a fresh
   snapshot. OpenRaft 0.9 does not expose a graceful leadership-transfer API here.

A compatible rolling restart preserves availability only while a quorum stays
reachable; a one-voter cluster requires downtime. The regression suite covers
membership additions, replacements, durable restart, leader removal and
reelection, all-RPC mismatch rejection, and compatible build-version descriptors.
It does not certify arbitrary historical binaries or incompatible format
migrations. For a mismatching contract, stop the cluster and follow a release's
explicit migration procedure instead of bypassing the gate.
