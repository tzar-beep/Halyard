# Halyard

A Redis-compatible database written from scratch in Go with no external
dependencies. It runs as a single node with AOF and snapshots, as a Raft-replicated
cluster, or as a sharded multi-Raft cluster. Testing goes beyond unit tests:
deterministic simulation, linearizability checking under injected faults, and
byte-for-byte differential testing against Redis 7.

```
redis-cli -c -p 6381 SET user:42 alice     # works with the real redis-cli
redis-cli -c -p 6382 GET user:42           # any node; -c follows MOVED to the leader
```

## Highlights

| | |
|---|---|
| **Protocol** | RESP2 over TCP and TLS, pipelining, inline commands. Unmodified `redis-cli`, `redis-benchmark` and cluster clients work. |
| **Data types** | strings, lists, hashes, sets, sorted sets (hand-written skiplist with rank spans), TTLs, `MULTI/EXEC/WATCH`, about 70 commands |
| **Durability** | Checksummed append-only file (always/everysec/no, group commit), copy-on-write snapshots, segmented AOF with crash-safe rotation |
| **Consensus** | Raft written from scratch: PreVote, CheckQuorum, ReadIndex linearizable reads, snapshots/InstallSnapshot, membership changes, leadership transfer |
| **Sharding** | 16,384 hash slots over multiple Raft groups, `MOVED`/`ASK`, `CLUSTER SLOTS`, **live slot migration** with no lost or duplicated writes |
| **Ops** | Prometheus metrics, `INFO`, ACL users and roles, TLS, config file, Docker Compose, a web demo with a "kill the leader" button |

## Raft playground

`web/index.html` runs the production `internal/raft` package, compiled to WebAssembly, as a five-node cluster
in the browser: crash nodes, partition them, add packet loss, and replay any run from its seed.

```bash
GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o web/raft.wasm ./cmd/playground
```

## Evidence

| Check | Result |
|---|---|
| Raft simulator: message loss, duplication and reordering, partitions, crashes (including in the middle of a disk write), compaction, leadership transfer | 2,000 seeds × 3- and 5-node clusters; election safety, state-machine safety and linearizable reads checked after every step |
| Jepsen-style test on a real TCP cluster with partitions, leader kills and restarts | ~15k operations per 90 s, **linearizable**. With ReadIndex deliberately disabled, the same harness catches the stale reads. |
| Leader failover | ~400 ms with test timeouts, ~1.3 s with production timeouts; **0 lost** acknowledged writes out of 823 written during a leader kill |
| Differential test against Redis 7 | 400,000 random commands with byte-identical replies (it also found a [bug in Redis itself](docs/bugs.md#3-a-bug-in-redis-lset-truncates-its-index-to-32-bits)) |
| Throughput vs. Redis 7 (`redis-benchmark`, P16) | SET 83–95%, GET 89%, LPUSH ~100%; see [docs/benchmarks.md](docs/benchmarks.md) |
| Fuzzing | parser, Raft messages, WAL, snapshot and dump decoders |

The simulator found two real Raft bugs before they shipped. Both are written up in
[docs/bugs.md](docs/bugs.md).

## Run it

```bash
go run ./cmd/halyard                                                   # single node on :6379
docker compose -f deploy/docker-compose.cluster.yml up -d --build     # 3-node Raft cluster
```

Then open `web/index.html` for the live cluster view.

| Mode | Flags |
|---|---|
| Single node | `-addr :6379 -dir data -appendfsync everysec` |
| Raft cluster | `-id 1 -cluster 1=host1:7000/host1:6379,2=host2:7000/host2:6379,3=host3:7000/host3:6379` |
| Sharded | the cluster flags plus `-shards 3` |
| Join a running cluster | `-id 4 -join -raft-addr host4:7000`, then `RAFT ADD 4 host4:7000 host4:6379` on the leader |
| Security | `-requirepass`, `-user alice:pw:read`, `-tls-cert/-tls-key` |
| Observability | `-http :8080` serves `/metrics`, `/healthz` and `/api/status` |

Deploying for free on Oracle Cloud with the demo page on Vercel is covered in
[deploy/oracle/README.md](deploy/oracle/README.md).

## Tests

```bash
go test -short ./...                                  # everything, fast
SEEDS=5000 go test ./internal/raft                    # Raft simulation sweep
JEPSEN_SECONDS=60 go test -v ./tests/jepsen           # linearizability under faults
REDIS_ADDR=127.0.0.1:6400 go test ./tests/differential  # needs: docker run -p 6400:6379 redis:7
bash benchmarks/compare.sh                            # redis-benchmark vs Redis 7, in Docker
```

## Docs

- [Architecture](docs/architecture.md): request path, storage, persistence, replication, sharding
- [Raft safety argument](docs/raft-safety.md): what is guaranteed and why
- [Bugs found by testing](docs/bugs.md)
- [Benchmarks](docs/benchmarks.md)
- [Development phases](PHASES.md)

Not supported: Lua, Pub/Sub, streams, RESP3, and Redis's RDB file format.

## License

MIT — see [LICENSE](LICENSE).
