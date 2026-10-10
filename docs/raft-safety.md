# Raft safety argument

This document lists the properties the implementation guarantees, the mechanism behind
each one, and the test that would catch a violation.

## Properties

| Property | Mechanism | Checked by |
|---|---|---|
| **Election safety:** at most one leader per term | Each node persists `currentTerm` and `votedFor` before sending a vote, and votes once per term. A leader needs a majority, and two majorities always overlap. | The simulator records the leader of every term (`checkLeader`). |
| **Log matching:** same index and term means identical prefixes | `AppendEntries` checks `(prevIndex, prevTerm)`, and conflicting suffixes are truncated (`maybeAppend`, `findConflict`). A committed entry is never truncated; `raftLog.append` panics if asked to. | Simulator, through state-machine safety |
| **Leader completeness:** a new leader holds every committed entry | Election restriction: a node votes only for a candidate whose log is at least as up to date (`isUpToDate`). A leader commits only entries from its own term by counting replicas (`maybeCommit`), which avoids the scenario in Figure 8 of the Raft paper. | Simulator |
| **State-machine safety:** no two nodes apply different entries at an index | Follows from the three properties above, plus applying entries strictly in order after they are persisted. | The simulator fingerprints every applied entry per index across all nodes (`apply`). |
| **Linearizable reads** | ReadIndex: record the commit index, confirm leadership with a heartbeat round acknowledged by a quorum, then serve once that index is applied. A leader first commits a no-op in its term, so the commit index it records is current. | The simulator checks that each read sees every write acknowledged before it started. The Jepsen test checks linearizability of real histories. |
| **Durability of acknowledged writes** | A client is answered only after the entry is applied, which requires a majority to have fsynced it. Each `Ready` is persisted before its messages are sent. | Leader-kill tests and the Jepsen test |

## Details that matter

- **Persist before send.** A vote or an append acknowledgement is sent only after the
  state it depends on is fsynced. Within a WAL batch, entries are written before the hard
  state, so a torn write can lose the hard state (and with it the vote) but never keep a
  vote while dropping entries it relied on. Since nothing from the torn batch was sent,
  no other node could have seen the lost vote. The simulator crashes nodes in the middle
  of a write to test this.
- **PreVote and CheckQuorum.** A partitioned node runs pre-elections, which never raise
  its term. A leader that cannot reach a majority steps down within one election
  timeout. Voters that heard from a live leader recently ignore campaigns (the leader
  lease), so a removed or flapping node cannot disrupt the cluster. Leadership transfer
  bypasses the lease explicitly, using `MsgTimeoutNow` and a flagged vote request.
- **Membership.** There is at most one configuration change in flight, and it takes
  effect when applied. The founding membership is written into the log as committed
  entries; see [bug #1](bugs.md). Without that, a joining node could believe it is the
  only voter.
- **Snapshots.** A follower that has compacted entries learns the cluster configuration
  from snapshot metadata. A snapshot older than the follower's commit index is ignored.
  If the follower already has the snapshot's last entry, it only advances its commit
  index.
- **Determinism of the state machine.** Entries carry the leader's timestamp. The applied
  timestamp is clamped to be monotonic across leader changes, and replicas never expire
  keys on local timers. So all replicas compute the same state from the same log.

## What is *not* guaranteed

- Writes whose reply was lost (a client timeout or dropped connection) have an unknown
  outcome. The Jepsen checker models them as "may or may not have happened", which is
  the standard treatment.
- A removed node that is not shut down can still attempt elections. The leader lease
  makes this harmless while the cluster is healthy, but operators should stop removed
  nodes.
- `WATCH` is not available in cluster mode. `MULTI/EXEC` is, and runs atomically through
  the log.
