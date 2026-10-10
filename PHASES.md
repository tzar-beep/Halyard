# Build Your Own Redis — Phased Plan (Go)

Goal: a Redis-compatible key-value server in Go that speaks RESP2 over TCP, works with `redis-cli`, and supports TTL, AOF + snapshot persistence and leader-follower replication. It should be benchmarked against real Redis and fault-tested.
Pace: about 2–3 h/day, 6 days/week, ~7 weeks. Each phase must compile, pass its tests and be committed before the next phase starts.

Out of scope: Redis Cluster, Lua, Pub/Sub, ACLs, the full command set and automatic leader election.

---

## Phase 0 — Setup (Day 1)
- Install Go, `redis-cli` (via WSL or Docker) and Docker.
- `go mod init`. Create the layout: `cmd/server`, `internal/{server,resp,command,store,persistence,replication,config}`, `tests/integration`, `benchmarks`, `docs`.
- Engineering rules: every feature has tests; malformed input never crashes the server or affects other clients; shared state has a clear locking strategy; every persistence/replication guarantee has a test that proves it.
- **Exit:** a TCP server on :6379 that replies `+PONG\r\n` to PING from a separate terminal.

## Phase 1 — TCP server + RESP2 (Week 1)
- Concurrent listener (`net.Listen`, a goroutine per connection, `bufio` reader/writer, clean close).
- RESP2 parser: array and bulk lengths, exact byte reads, CRLF validation, size/argument limits, clear errors. TCP is a byte stream, so handle partial reads and pipelined requests.
- Reusable encoders: simple string, error, integer, bulk string, null bulk string.
- Commands: `PING`, `ECHO`, `SET`, `GET`, backed by a plain map.
- **Exit:** multiple clients and pipelining work, bad input causes no crash, and `redis-cli` works.

## Phase 2 — Storage engine + TTL (Week 2)
- `Entry{Value, ExpiresAt}`. The store API sits behind methods, and handlers never touch the map directly.
- Command router. Add `DEL`, `EXISTS`, `MGET`, `MSET`, `INCR`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` and `PERSIST`, matching Redis semantics.
- TTL works two ways: lazy expiry on read, plus an active sampling worker.
- A `sync.RWMutex` protects the store, and multi-key commands are atomic. Don't optimize before measuring.
- **Exit:** the store passes its TTL and concurrency tests with `go test -race`, independently of TCP.

## Phase 3 — AOF durability (Week 3)
- Write path: validate → apply → append to the AOF → (sync) → reply. Only successful mutations are logged, in RESP format.
- Sync policies: `always` (fsync before reply) and `everysec`. Write and fsync errors are real failures.
- Recovery: replay sequentially, stop safely at a truncated tail, and report corruption in the middle of the file.
- Fault tests: kill the server during writes, truncate the last record, corrupt a middle record, and compare acknowledged writes with recovered data.
- **Exit:** repeatable crash-recovery tests pass for each policy.

## Phase 4 — Snapshots + combined recovery (Week 4)
- Versioned binary format: magic bytes, version, record count, length-prefixed key/value, expiry and checksum.
- Save: copy the data under the lock, then serialize outside it. Write to a temp file → flush → fsync → atomic rename.
- Recovery = latest valid snapshot + the AOF written after it. Rotate the AOF after each snapshot and keep metadata that links each snapshot to its log.
- Tests: empty and large datasets, expired keys, bad checksum, crash before rename, crash between rename and rotation.
- **Exit:** no write that the durability contract promises to keep is lost across any of these transitions.

## Phase 5 — Leader-follower replication (Week 5)
- Contract: only the leader accepts writes; mutations carry monotonically increasing offsets and are applied in order; followers are read-only.
- Full sync: snapshot + offset, with the leader buffering writes made during the transfer.
- Incremental sync: a bounded backlog. A reconnecting follower sends its offset; if the backlog no longer has that offset, it falls back to full sync.
- Backpressure: queue limits, write deadlines, reconnect backoff and rules for a follower that falls behind. Expose state through `INFO replication`.
- Fault tests: initial sync, 10k-key comparison, disconnect/reconnect, backlog overflow, leader kill and leader restart.
- **Exit:** a follower catches up after interruption, and behavior under failure is documented and reproducible.

## Phase 6 — Reliability + performance (Week 6)
- `redis-benchmark` against real Redis on the same machine and workload, varying clients and pipeline depth. Record ops/s, p50/p95/p99 latency, CPU, memory, recovery time and replication lag.
- `FuzzParseRequest` with seeds for valid, empty, truncated, oversized, embedded-NUL and concatenated input. It must never panic, hang or allocate without bound.
- Stress tests with hundreds of connections running reads, writes and expiry under `-race`.
- A Jepsen-style harness: write numbered keys, record acknowledgements, kill the server, restart it and verify the result against the durability contract.
- **Exit:** published, reproducible numbers and fault results.

## Phase 7 — Packaging + polish (Week 7)
- CLI flags: address, data dir, AOF policy, snapshot interval, role and leader address, log level.
- A Docker Compose demo with one leader and one follower on separate volumes.
- GitHub Actions: build, `vet`, `test -race`. Fuzzing and benchmarks run in a separate workflow.
- Docs: Architecture, Benchmark report, Failure analysis (one hard bug), and a list of unsupported features.
- Resume bullet that uses only measured numbers.
- **Exit:** all items in the final checklist (22) are checked.

---

### Phase dependency
0 → 1 → 2 → 3 → 4 → 5 → 6 → 7. Replication starts only after single-node persistence and recovery pass their tests.
