#!/usr/bin/env bash
# Compares this server with Redis 7 using redis-benchmark, all in Docker on one
# machine. Usage: benchmarks/compare.sh [label]   (writes benchmarks/results-<label>.csv)
set -euo pipefail
label=${1:-run}
out="benchmarks/results-$label.csv"
docker build -q -t myredis . >/dev/null
docker network create benchnet >/dev/null 2>&1 || true
cleanup() { docker rm -f bench-ours bench-redis >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

run() { # name host extra-args
  for p in 1 16; do
    docker run --rm --network benchnet redis:7 redis-benchmark -h "$2" -p 6379 \
      -t set,get,incr,lpush,zadd -n 200000 -c 50 -P "$p" -q --csv \
      | tail -n +2 | sed "s/^/$1,$3,P$p,/"
  done
}

echo "server,persistence,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms" > "$out"
for mode in off everysec; do
  if [ "$mode" = off ]; then ours="-appendonly=false"; theirs="--appendonly no --save ''"; else ours="-appendfsync everysec"; theirs="--appendonly yes --appendfsync everysec --save ''"; fi
  docker run -d --name bench-ours --network benchnet myredis $ours >/dev/null
  docker run -d --name bench-redis --network benchnet redis:7 sh -c "redis-server $theirs" >/dev/null
  sleep 2
  run ours bench-ours "$mode" >> "$out"
  run redis bench-redis "$mode" >> "$out"
  cleanup
done
column -s, -t "$out" | cut -c1-110
