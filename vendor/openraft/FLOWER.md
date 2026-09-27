# Flower's OpenRaft 0.9.25

This is the published `openraft` 0.9.25 crate with three changes: the current
leader's own log append no longer blocks replication, snapshot chunks keep a
follower from timing out the leader that sends them, and the chunked snapshot
sender reads a snapshot to its end rather than asking for its size, so that it
can stream while it encodes.

Upstream 0.9 awaits the leader's log flush inside `RaftCore` before it runs the
`Replicate` commands that follow, so every commit pays for the leader's fsync
and a follower's fsync one after the other. Raft allows a leader to write its
log in parallel with replication (Ongaro's thesis, section 10.2.1), provided it
counts itself toward the commit quorum only once its entries are durable.

- `src/core/raft_core.rs`: `AppendInputEntries` for the current leader calls
  `submit_leader_append`, which returns once storage has made the entries
  readable. A spawned task waits for the flush callback and sends
  `Notify::LocalLogFlushed`; only then does the leader's own matching log
  advance. A notification from an older leadership is ignored. Follower appends
  are unchanged, so their AppendEntries responses still imply durability.
- `src/core/notify.rs`: the `LocalLogFlushed` notification.
- `src/storage/callback.rs`: `LogFlushed::is_leader_append()` tells storage that
  it may make this append durable later than a follower append would be.

Flower's store (`src/consensus/store/lazy_flush.rs`) uses that flag to commit
the leader's appends without an fsync while followers alone form a majority.

Upstream 0.9 receives snapshot chunks outside `RaftCore`, so they never refresh
the follower's leader lease, and a leader sends a follower nothing else while a
snapshot is in flight. A transfer longer than the election timeout made the
follower start an election, and the new term restarted the transfer.

- `src/raft/mod.rs`: `install_snapshot` passes each chunk's vote to the core
  as `RaftMsg::SnapshotChunk` before receiving it, and a rejected vote returns
  like a stale request. While the final chunk's install runs, it sends the same
  message every heartbeat interval without awaiting replies, which queue behind
  the install's.
- `src/core/raft_msg/mod.rs`, `src/core/raft_core.rs` and
  `src/engine/engine_impl.rs`: `SnapshotChunk` accepts the vote as
  `handle_install_full_snapshot` does, which refreshes its lease, and replies.

`src/network/snapshot_transport.rs`: `Chunked::send_snapshot` no longer seeks
to the end for the snapshot's size. A segment is the last, marked `done`, when
reading it reaches the end; a segment that ends exactly there is followed by an
empty one. Between segments it yields once, where upstream slept a millisecond.

To update, replace this directory with a newer published crate and reapply the
changes marked `Flower:`.
