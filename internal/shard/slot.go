// Package shard implements Redis-Cluster-style sharding on top of multi-Raft.
//
// The keyspace is split into 16384 hash slots (CRC16 of the key, or of its
// {hashtag}). Every process hosts one replica of each Raft group:
//
//	group 0      metadata: which data group owns each slot, and migrations
//	group 1..G   data: a keyspace holding the slots that group owns
//
// Clients may connect to any node. A keyed command is routed to the data group
// owning its slot; if this node is not that group's leader, the client gets
// "MOVED slot host:port" (the leader's address), exactly like Redis Cluster, so
// cluster-aware clients (redis-cli -c, go-redis ClusterClient, ...) work unchanged.
//
// Slot migration is live: see migrate.go.
package shard

import (
	"strings"
)

const NumSlots = 16384

var crcTable = func() (t [256]uint16) {
	for i := range t {
		c := uint16(i) << 8
		for j := 0; j < 8; j++ {
			if c&0x8000 != 0 {
				c = c<<1 ^ 0x1021
			} else {
				c <<= 1
			}
		}
		t[i] = c
	}
	return
}()

// crc16 is CRC-16/XMODEM, the variant Redis Cluster uses.
func crc16(b []byte) uint16 {
	var c uint16
	for _, x := range b {
		c = c<<8 ^ crcTable[byte(c>>8)^x]
	}
	return c
}

// KeySlot maps a key to its slot. If the key contains a non-empty {hashtag}, only
// the tag is hashed, so related keys can be forced into one slot.
func KeySlot(key []byte) int {
	if i := strings.IndexByte(string(key), '{'); i >= 0 {
		if j := strings.IndexByte(string(key[i+1:]), '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	return int(crc16(key)) % NumSlots
}

// keySpec gives key positions like the Redis command table: first key, last key
// (negative counts from the end), step.
type keySpec struct{ first, last, step int }

var keySpecs = map[string]keySpec{}

func init() {
	single := []string{"get", "strlen", "set", "append", "incr", "decr", "incrby", "decrby",
		"ttl", "pttl", "type", "expire", "pexpire", "expireat", "pexpireat", "persist",
		"llen", "lrange", "lindex", "lpush", "rpush", "lpop", "rpop", "lset", "ltrim", "lrem",
		"hget", "hmget", "hexists", "hlen", "hgetall", "hkeys", "hvals", "hset", "hdel", "hincrby",
		"sismember", "smembers", "scard", "sadd", "srem",
		"zscore", "zcard", "zrank", "zrevrank", "zcount", "zrange", "zrevrange",
		"zrangebyscore", "zrevrangebyscore", "zadd", "zincrby", "zrem"}
	for _, n := range single {
		keySpecs[n] = keySpec{1, 1, 1}
	}
	for _, n := range []string{"mget", "exists", "del", "sinter", "sunion", "sdiff", "delexpired"} {
		keySpecs[n] = keySpec{1, -1, 1}
	}
	keySpecs["mset"] = keySpec{1, -1, 2}
}

// Keys returns the keys a command touches (nil for keyless or unknown commands).
func Keys(args [][]byte) [][]byte {
	if len(args) == 0 {
		return nil
	}
	ks, ok := keySpecs[strings.ToLower(string(args[0]))]
	if !ok {
		return nil
	}
	last := ks.last
	if last < 0 {
		last += len(args)
	}
	var out [][]byte
	for i := ks.first; i <= last && i < len(args); i += ks.step {
		out = append(out, args[i])
	}
	return out
}

// Keyed reports whether a command name takes keys.
func Keyed(name string) bool {
	_, ok := keySpecs[strings.ToLower(name)]
	return ok
}

// SlotOf returns the common slot of keys, or -1 if they span several slots.
func SlotOf(keys [][]byte) int {
	slot := -1
	for _, k := range keys {
		s := KeySlot(k)
		if slot >= 0 && s != slot {
			return -1
		}
		slot = s
	}
	return slot
}
