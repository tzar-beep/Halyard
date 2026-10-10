# Build Your Own Redis â€” Phased Plan (Go)

Goal: a Redis-compatible key-value server in Go that speaks RESP2 over TCP, works with `redis-cli`, and supports TTL, AOF + snapshot persistence and leader-follower replication. It should be benchmarked against real Redis and fault-tested.
Pace: about 2â€“3 h/day, ~14 weeks. Each phase must compile, pass its tests and be committed before the next phase starts.

Out of scope: Lua, modules and RESP3-only features.

---

## Phase 0 â€” Setup (Day 1)
- Install Go, `redis-cli` (via WSL or Docker) and Docker.
- `go mod init`. Create the layout: `cmd/server`, `internal/{server,resp,command,store,persistence,replication,config}`, `tests/integration`, `benchmarks`, `docs`.
- Engineering rules: every feature has tests; malformed input never crashes the server or affects other clients; shared state has a clear locking strategy; every persistence/replication guarantee has a test that proves it.
- **Exit:** a TCP server on :6379 that replies `+PONG\r\n` to PING from a separate terminal.

## Phase 1 â€” TCP server + RESP2 (Week 1)
- Concurrent listener (`net.Listen`, a goroutine per connection, `bufio` reader/writer, clean close).
- RESP2 parser: array and bulk lengths, exact byte reads, CRLF validation, size/argument limits, clear errors. TCP is a byte stream, so handle partial reads and pipelined requests.
- Reusable encoders: simple string, error, integer, bulk string, null bulk string.
- Commands: `PING`, `ECHO`, `SET`, `GET`, backed by a plain map.
- **Exit:** multiple clients and pipelining work, bad input causes no crash, and `redis-cli` works.

## Phase 2 â€” Storage engine + TTL (Week 2)
- `Entry{Value, ExpiresAt}`. The store API sits behind methods, and handlers never touch the map directly.
- Command router. Add `DEL`, `EXISTS`, `MGET`, `MSET`, `INCR`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` and `PERSIST`, matching Redis semantics.
- TTL works two ways: lazy expiry on read, plus an active sampling worker.
- A `sync.RWMutex` protects the store, and multi-key commands are atomic. Don't optimize before measuring.
- **Exit:** the store passes its TTL and concurrency tests with `go test -race`, independently of TCP.

## Phase 3 â€” AOF durability (Week 3)
- Write path: validate â†’ apply â†’ append to the AOF â†’ (sync) â†’ reply. Only successful mutations are logged, in RESP format.
- Sync policies: `always` (fsync before reply) and `everysec`. Write and fsync errors are real failures.
- Recovery: replay sequentially, stop safely at a truncated tail, and report corruption in the middle of the file.
- Fault tests: kill the server during writes, truncate the last record, corrupt a middle record, and compare acknowledged writes with recovered data.
- **Exit:** repeatable crash-recovery tests pass for each policy.

## Phase 4 â€” Snapshots + combined recovery (Week 4)
- Versioned binary format: magic bytes, version, record count, length-prefixed key/value, expiry and checksum.
- Save: copy the data under the lock, then serialize outside it. Write to a temp file â†’ flush â†’ fsync â†’ atomic rename.
- Recovery = latest valid snapshot + the AOF written after it. Rotate the AOF after each snapshot and keep metadata that links each snapshot to its log.
- Tests: empty and large datasets, expired keys, bad checksum, crash before rename, crash between rename and rotation.
- **Exit:** no write that the durability contract promises to keep is lost across any of these transitions.

## Phase 5 â€” Data structures (Week 5)
- Typed values: lists (quicklist-style), hashes, sets and sorted sets, using a **hand-written skiplist** with rank queries (ZADD/ZRANGE/ZRANK/ZRANGEBYSCORE).
- `WRONGTYPE` errors. MULTI/EXEC/WATCH transactions (optimistic, checked against key versions).
- Snapshot format v2 that stores every type. Property-based tests that compare against a simple reference model.
- **Exit:** randomized differential tests against real Redis (same command stream, same replies).

## Phase 6 â€” Raft consensus, written from scratch (Weeks 6â€“8)
- Leader election (randomized timeouts, terms, votes), log replication, commit index, and the safety rules from the Raft paper.
- Persistent `currentTerm`, `votedFor` and the log. The Raft log replaces the AOF as the write-ahead log.
- Log compaction through snapshots plus `InstallSnapshot`. Membership changes (one server at a time).
- **Linearizable** writes and reads (ReadIndex, or leader leases with a documented clock assumption). Clients follow `MOVED`/redirects to the leader.
- **Automatic failover:** kill the leader and a new one is elected in under about 1 s with **zero acknowledged-write loss**.
- **Exit:** 3- and 5-node clusters survive leader kill, follower kill and network partitions.

## Phase 7 â€” Sharding (Weeks 9â€“10)
- 16,384 hash slots (CRC16, `{hashtag}` support). Each shard is a Raft group (multi-Raft).
- `CLUSTER SLOTS`/`CLUSTER SHARDS`, and `MOVED`/`ASK` redirects compatible with `redis-cli -c`.
- **Live slot migration** between groups while clients keep writing, with no lost or duplicated keys.
- **Exit:** throughput grows close to linearly from 1 to 3 shards.

## Phase 8 â€” Proving correctness (Weeks 11â€“12) â† this is what makes it 10/10
- **Deterministic simulation testing** (the approach FoundationDB and TigerBeetle use): a single-threaded simulator with fake network, disk and clock driven by a seed. It injects partitions, delays, reordering, crashes and torn writes, and replays any failing seed exactly.
- **Linearizability checking** with Porcupine on histories recorded under faults.
- A Jepsen-style harness against the real binaries in Docker: `tc netem`/iptables partitions, kill -9 and clock skew.
- Fuzzing of the parser, the AOF/snapshot readers and Raft RPC decoding.
- **Exit:** millions of simulated seeds with no safety violation. Every bug found becomes a seed in the regression suite.

## Phase 9 â€” Performance engineering (Week 13)
- Profile with pprof, then: a parser that doesn't allocate, a sharded keyspace (fewer lock collisions), **group commit** for `appendfsync always`, and batching of Raft AppendEntries.
- `redis-benchmark` against real Redis: ops/s and p50/p99/p999 latency, single node vs. cluster. Flame graphs go in the report.
- **Exit:** published, reproducible numbers that show the before/after effect of each optimization.

## Phase 10 â€” Production polish + deploy (Week 14)
- Prometheus `/metrics` (ops, latency histograms, Raft term/commit lag, fsync time), structured logs, `INFO`.
- TLS, ACL users, graceful shutdown, configuration through a file.
- Deploy a 3-node cluster on Oracle Cloud Always Free, plus a Vercel site with a live web terminal, a cluster view (leader, terms, slots) and a **"kill the leader" button** that shows failover live.
- CI: build, vet, `-race`, a nightly simulation sweep and fuzzing.
- Docs: design doc, Raft safety argument, benchmark report, and three bug write-ups found by simulation.
- **Exit:** a public demo, plus a resume line backed only by measured numbers.

---

### Phase dependency
0 â†’ 1 â†’ 2 â†’ 3 â†’ 4 â†’ 5 â†’ 6 â†’ 7 â†’ 8 â†’ 9 â†’ 10. Phase 8's simulator should be built alongside Phase 6 (design the network, disk and clock as interfaces from the start).
Done: 0â€“3.
