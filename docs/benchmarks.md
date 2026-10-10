# Benchmarks

All numbers were measured on one laptop. They are reproducible with the scripts and
benchmarks named below, but they will differ on other hardware.

**Machine:** AMD Ryzen 7 7840HS (8 cores / 16 threads), 15 GB RAM, Windows 11.
Docker Desktop runs a WSL2 Linux VM with 16 vCPUs and 7.4 GB RAM. Go 1.27, Redis 7.

## 1. Single node vs. Redis 7 (`redis-benchmark`)

`benchmarks/compare.sh` runs both servers in Docker on the same machine and the same
network. The load is `redis-benchmark -n 200000 -c 50`, either one request at a time
(P1) or 16 pipelined requests per round trip (P16). Raw output is in
`benchmarks/results-*.csv`.

Requests/second, P16, after the Phase 9 optimizations:

| Command | Ours, no persistence | Redis, no persistence | Ours, AOF everysec | Redis, AOF everysec |
|---|---:|---:|---:|---:|
| SET   | 613k | 735k | 503k | 532k |
| GET   | 669k | 749k | 631k | 722k |
| INCR  | 601k | 826k | 438k | 554k |
| LPUSH | 637k | 772k | 519k | 513k |
| ZADD  | 617k | 707k | 597k | 725k |

- **P1 (one request per round trip):** both servers reach 40–53k req/s. This is bound
  by network round trips, and ours is within 80–90% of Redis.
- **P16 SET p99 latency:** ours went from 4.1 ms to 2.0 ms with persistence off;
  Redis's is 1.8 ms. With AOF everysec, ours is 8 ms and Redis's is 2.6 ms (see
  "Known gaps").

## 2. Optimizations and their measured effect

These are in-process Go benchmarks (`tests/bench`). Each runs 50 connections over
loopback TCP on Windows. Both sides of each comparison were run on the same machine,
minutes apart.

| Change | Benchmark | Before | After |
|---|---|---:|---:|
| Group commit for `appendfsync always`: one fsync covers every writer waiting | `SetAOFAlways` | 736 µs/op | **36 µs/op (20×)** |
| Parser reuses an argument buffer, reads lines without copying, parses integers without allocating | `SetPipelined` | 1964 ns/op, 17 allocs | **1449 ns/op, 7 allocs** |
| Command names are lowercased without allocating; AOF records are built only when AOF is on; transaction objects are reused | (included above) | | |
| everysec: fsync runs outside the AOF lock; 1 MB AOF buffer | P16 SET, Docker | 432k req/s | **503k req/s** |
| Raft loop takes every waiting proposal and message before each batch (group commit for Raft) | 3-node sharded cluster, 48 clients | 366 ops/s | **8,222 ops/s (22×)** |

## 3. Replicated and sharded clusters

| Scenario | Result |
|---|---|
| Leader killed in a 3-node cluster: time until the new leader accepts writes | **~390 ms** (election timeout 200–400 ms) |
| Acknowledged writes lost across a leader kill, 8 concurrent clients | **0 of 823** |
| Writes lost after kill -9 with `appendfsync always`, single node, 3 rounds | **0 of ~3,800** |
| Live slot migration of 30 keys under concurrent INCRs | ~1.1 s, every acknowledged increment kept |
| One node killed in a sharded cluster: every slot writable again | < 1 s |
| Sharded throughput, 3 groups vs. 1 | 8.2k ops/s in both cases (see below) |

## 4. Correctness evidence

| Check | Scale |
|---|---|
| Raft simulator (loss, duplication, reordering, partitions, crashes, crashes in the middle of disk writes, compaction, leadership transfer) | 1,500–2,000 seeds × 3- and 5-node clusters, invariants checked after every step |
| Jepsen-style linearizability test (partitions, leader kills, restarts) | 3 × 30 s runs: ~15k acknowledged operations, ~160 injected faults, all linearizable |
| Negative control: ReadIndex disabled | Stale reads detected on the first attempt |
| Differential test against Redis 7 | 400,000 random commands, byte-identical replies |
| Fuzzing | RESP parser, Raft messages, WAL, snapshot and dump decoders |

## Known gaps

- **Tail latency under heavy write contention with AOF on.** Our median is lower than
  Redis's, but p99 is about 3× higher. All writers take one mutex to keep the AOF in
  the same order the writes were applied. With 50 busy connections, goroutines queue
  on that mutex, whereas Redis's single event loop serves clients in a fair order.
  Likely fixes are a single writer goroutine fed by a queue, or splitting the
  keyspace into shards that each have their own AOF.
- **Sharding does not scale on one machine.** All three nodes ran in one process on
  one laptop and shared its CPU and disk, so 3 groups gave the same throughput as 1.
  The design is meant to scale across machines, with one group leader per node
  (verified by `TestLeadersBalanced`). That has not been measured on real separate
  hosts yet.
- **Windows numbers are pessimistic.** In the in-process benchmarks, about 70% of CPU
  time goes to Windows socket system calls (`WSASend`). The Docker (Linux) numbers in
  section 1 are the representative ones.
