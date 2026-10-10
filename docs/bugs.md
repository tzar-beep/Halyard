# Bugs found by testing

These are real defects caught by the project's own test tooling. Each was reproducible
from a seed or a deterministic test before it was fixed.

## 1. A joining node elected itself with a quorum of one (Raft simulator)

**Symptom.** `TestSimMembership`, seed 15, reported a state-machine safety violation:
index 71 was applied as a term-3 entry on some nodes and as a term-4 entry on others.

**Cause.** The founding members came only from a command-line flag (`Config.Peers`).
The log never recorded them. Node 4 joined empty and replicated the log from index 1,
so it learned about itself from the `AddNode(4)` entry but never about nodes 1–3. It
believed the voter set was `{4}`, so it elected itself with one vote and committed
entries alone. Meanwhile, the real majority elected another leader that overwrote
those entries.

**Fix.** Write the founding configuration into the log as committed conf-change entries
at indexes 1..n, identical on every founding node (etcd does the same). On restart,
derive membership from the snapshot plus the committed conf entries instead of from
flags.

**Lesson.** Any input a node uses that isn't in the replicated log is a way for replicas
to diverge.

## 2. A new node could not answer the leader (cluster integration test)

**Symptom.** In `TestJoinViaSnapshot`, node 4 never caught up. It reported
`Leader: 3, Commit: 0` forever.

**Cause.** Node 4 received the leader's messages, but every reply was dropped. It only
learns peer addresses from the log or a snapshot, and it couldn't get either without
replying. The leader stayed in probe mode waiting for the replies.

**Fix.** The transport handshake now carries the sender's Raft address. After
authenticating with the cluster secret, the receiver records it as a return route for a
peer it doesn't yet know.

## 3. A bug in Redis: `LSET` truncates its index to 32 bits (differential test)

**Symptom.** The differential test, seed 9, step 282, sent `LSET k4 9223372036854775807 c`.
Our server replied `-ERR index out of range`; Redis 7 replied `+OK`.

**Cause.** In Redis, the 64-bit index is narrowed to 32 bits on the list's
listpack/quicklist path. `2^63-1` becomes `-1`, so Redis overwrites the **last
element**. That is a silent wrong write. Our behavior matches Redis's documentation.

**Resolution.** We kept the correct behavior and excluded that input from the
differential generator, with a comment explaining why. It's a good example of
differential testing finding a defect in the reference implementation.

## Also caught along the way

- **Mutex convoy on the write path**, found with the profiler: `appendfsync always`
  fsynced while holding the write lock, so writers ran one at a time at about
  1,400 ops/s. Group commit made it 20× faster.
- **One fsync per proposal in the Raft loop**, found by the throughput benchmark: the
  run loop handled one proposal per `Ready`. Taking every waiting proposal before each
  batch made it 22× faster.
- **Leadership transfer to a dead node**, found by `TestShardedFailover`: the leader
  balancer kept handing leadership to a node that was down, which blocked proposals for
  an election timeout each time. Transfers now go only to peers that have recently
  responded.
- **The linearizability checker blew up** on about 900 writes with unknown outcome.
  Two sound reductions fixed it: an unknown write whose value was never read is dropped,
  and one that was read is given an end time at the first read that saw it. A full
  check now takes under 20 ms.
