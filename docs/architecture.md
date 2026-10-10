# Architecture

```
                 client (redis-cli, any RESP client)
                         │ TCP / TLS
                ┌────────▼─────────┐
                │ server           │  one goroutine per connection, buffered I/O,
                │  resp.Reader     │  replies flushed once the pipeline is drained
                └────────┬─────────┘
           standalone /  │  \ sharded
    ┌───────────────────┐│┌──────────────────────────────┐
    │ command.Dispatcher│││ shard.Router                 │ slot = CRC16(key or {tag}) % 16384
    │  ACL, MULTI/EXEC, │││  route to owning group;      │ MOVED / ASK / CROSSSLOT
    │  handler table    │││  CLUSTER SLOTS, migration    │
    └───┬───────────┬───┘│└──────────────┬───────────────┘
        │           │    │               │ per group: Dispatcher + guard
  store.Store   persistence (standalone)  cluster.Node (Raft group)
  View/Update   AOF segments + snapshots   raft.Raft (pure) + wal + transport
```

## Request path

1. **Parse.** `resp.Reader` reads a RESP array into `[][]byte` without copying. The
   argument slices point into a per-connection buffer that the next request reuses,
   so anything kept must be copied: values are cloned and keys become strings.
2. **Dispatch.** The dispatcher lowercases the command name without allocating, checks
   arity and ACL permissions, and picks a handler. Each handler is marked as a read,
   a write or an admin command.
3. **Execute.** Reads run inside `store.View` (shared lock) and writes inside
   `store.Update` (exclusive lock). A multi-key command or a whole `MULTI/EXEC` block is
   therefore atomic.
4. **Persist.** A write returns a *log record*. Records are deterministic: relative TTLs
   become absolute timestamps and `INCR` becomes `SET … KEEPTTL`, so replaying the log
   always rebuilds the same state.
5. **Reply.** The reply sits in the connection's buffer and is flushed only after the
   write is as durable as the configured policy promises.

## Storage (`internal/store`)

- Typed objects: string, list (ring-buffer deque), hash, set, and sorted set
  (hash map plus a skiplist with span counters, giving O(log n) rank and range
  queries).
- TTLs live in a separate `expires` map. An expired key counts as missing immediately;
  write transactions delete it when they touch it, and a background sampler deletes
  the rest (20 keys per 100 ms, repeating while more than 25% of a sample has expired).
- **Copy-on-write snapshots.** `Snapshot()` increments an epoch and returns references
  to the current objects, which takes O(keys) time and copies no values. The first
  write after a snapshot to an older container clones it first, so the background
  serializer never sees a value change. This is the same idea as Redis's
  `fork()` plus copy-on-write, without forking.
- `WATCH` tracks the watched keys and marks them dirty on any write, delete or expiry.

## Persistence (`internal/persistence`)

- **AOF.** Every write record is appended in RESP format to `appendonly.<N>.aof`.
  - `always`: group commit. A syncer goroutine fsyncs batches, and each writer waits
    outside the write lock until its batch is on disk.
  - `everysec`: data is fsynced once a second, outside the lock.
  - Any write or fsync error is permanent: later writes are refused with `MISCONF`
    instead of being lost silently.
- **Snapshots.** `dump.snap` is versioned and CRC-64 checksummed, and written to a temp
  file, fsynced, renamed, then the directory is fsynced.
- **Segment rotation.** A snapshot records the generation of the first AOF segment that
  follows it. Saving works in three steps:
  1. Pause writers, capture the keyspace and start segment N+1.
  2. Write the snapshot in the background.
  3. Delete the older segments.

  A crash between any two steps leaves a valid combination of files. There is a test
  for each crash point.
- **Recovery.** Load the snapshot, then replay the segments after it. A torn final
  record is truncated away; corruption anywhere else stops the server from starting.

## Replication (`internal/raft`, `internal/cluster`)

- `raft.Raft` is a deterministic state machine with no goroutines, clocks or I/O. It
  takes `Tick`, `Step`, `Propose` and `ReadIndex` calls and emits `Ready` batches.
  This is what lets a simulator run whole clusters from a single seed.
- `cluster.Node` owns one Raft group. For each `Ready` it:
  1. persists to the WAL, one fsync per batch
  2. sends messages
  3. applies committed entries
  4. advances

  The loop takes every waiting proposal and message before each batch (group commit).
- **Writes.** The leader stamps each entry with a timestamp. Every replica runs the
  command with that timestamp as "now", so TTL decisions match everywhere. Expired keys
  are removed by `DELEXPIRED` entries that the leader proposes, never by local timers.
- **Reads.** ReadIndex: the leader confirms it is still leader with a heartbeat quorum
  and waits until it has applied up to the commit index it recorded. This makes reads
  linearizable without writing to the log.
- **Transport.** One TCP connection per peer carries every Raft group. Frames are tagged
  with the group id. The handshake authenticates with a shared secret and tells the
  receiver how to reach the sender.
- **WAL.** Records are CRC-32C checksummed. A torn tail is truncated; after a snapshot
  the log is compacted by writing a new file and renaming it.

## Sharding (`internal/shard`)

- **Group 0** holds the slot table: which data group owns each slot, and any migration
  in progress. **Groups 1..G** hold the data. Every process runs a replica of every
  group.
- **Ownership is enforced inside each data group.** A guard runs in the same store
  transaction as each command, both when applying replicated writes and when serving
  reads. A node's routing table can be stale; the guard cannot be.
- **Live migration** of slot `s` from group A to group B:
  1. B marks `s` as importing; A marks it as migrating; group 0 records the migration.
  2. For each key: dump it from A, `IMPORTKEY` it into B, then `DELIFEQ` it from A
     (delete only if unchanged since the dump).
  3. Once A has no keys left in `s`, ownership flips.

  A key always lives in exactly one group, and new keys go straight to B through `ASK`,
  so no write can be lost or applied twice. A test checks this with concurrent `INCR`s
  during migration.
- **Leader balancing.** Group g prefers the node at position g mod N, and leadership
  transfer moves it there, so each node leads about one group.
