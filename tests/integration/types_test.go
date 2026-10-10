package integration

import (
	"testing"

	"github.com/tzar-beep/redis/internal/persistence"
	"github.com/tzar-beep/redis/internal/server"
)

func TestTypesAndTransactions(t *testing.T) {
	c, r := dial(t, start(t))
	for _, s := range []struct{ req, want string }{
		{cmd("RPUSH", "l", "a", "b", "c"), ":3\r\n"},
		{cmd("LPUSH", "l", "z"), ":4\r\n"},
		{cmd("LRANGE", "l", "0", "-1"), "*4\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"},
		{cmd("GET", "l"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{cmd("TYPE", "l"), "+list\r\n"},
		{cmd("LPOP", "l", "2"), "*2\r\n$1\r\nz\r\n$1\r\na\r\n"},
		{cmd("RPOP", "l", "5"), "*2\r\n$1\r\nc\r\n$1\r\nb\r\n"},
		{cmd("EXISTS", "l"), ":0\r\n"},
		{cmd("HSET", "h", "f1", "1", "f2", "2"), ":2\r\n"},
		{cmd("HINCRBY", "h", "f1", "10"), ":11\r\n"},
		{cmd("HMGET", "h", "f1", "nope"), "*2\r\n$2\r\n11\r\n$-1\r\n"},
		{cmd("SADD", "s", "x", "y", "x"), ":2\r\n"},
		{cmd("SISMEMBER", "s", "y"), ":1\r\n"},
		{cmd("ZADD", "z", "1", "a", "2", "b", "3", "c"), ":3\r\n"},
		{cmd("ZADD", "z", "XX", "CH", "5", "a", "9", "new"), ":1\r\n"},
		{cmd("ZRANGE", "z", "0", "-1", "WITHSCORES"), "*6\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\na\r\n$1\r\n5\r\n"},
		{cmd("ZRANGEBYSCORE", "z", "(2", "+inf", "LIMIT", "0", "1"), "*1\r\n$1\r\nc\r\n"},
		{cmd("ZREVRANK", "z", "a"), ":0\r\n"},
		{cmd("ZINCRBY", "z", "0.5", "b"), "$3\r\n2.5\r\n"},
		{cmd("ZCOUNT", "z", "-inf", "(3"), ":1\r\n"},
		{cmd("KEYS", "[^sz]"), "*1\r\n$1\r\nh\r\n"},
	} {
		roundtrip(t, c, r, s.req, s.want)
	}

	// MULTI/EXEC is atomic; queue-time errors abort the whole transaction.
	for _, s := range []struct{ req, want string }{
		{cmd("MULTI"), "+OK\r\n"},
		{cmd("INCR", "n"), "+QUEUED\r\n"},
		{cmd("RPUSH", "q", "1"), "+QUEUED\r\n"},
		{cmd("EXEC"), "*2\r\n:1\r\n:1\r\n"},
		{cmd("MULTI"), "+OK\r\n"},
		{cmd("INCR"), "-ERR wrong number of arguments for 'incr' command\r\n"},
		{cmd("EXEC"), "-EXECABORT Transaction discarded because of previous errors.\r\n"},
		{cmd("EXEC"), "-ERR EXEC without MULTI\r\n"},
	} {
		roundtrip(t, c, r, s.req, s.want)
	}

	// WATCH: a concurrent write from another client aborts EXEC.
	c2, r2 := dial(t, c.RemoteAddr().String())
	roundtrip(t, c, r, cmd("WATCH", "n"), "+OK\r\n")
	roundtrip(t, c2, r2, cmd("INCR", "n"), ":2\r\n")
	roundtrip(t, c, r, cmd("MULTI"), "+OK\r\n")
	roundtrip(t, c, r, cmd("SET", "n", "100"), "+QUEUED\r\n")
	roundtrip(t, c, r, cmd("EXEC"), "*-1\r\n")
	roundtrip(t, c, r, cmd("GET", "n"), "$1\r\n2\r\n")
	// Without interference it commits.
	roundtrip(t, c, r, cmd("WATCH", "n"), "+OK\r\n")
	roundtrip(t, c, r, cmd("MULTI"), "+OK\r\n")
	roundtrip(t, c, r, cmd("SET", "n", "100"), "+QUEUED\r\n")
	roundtrip(t, c, r, cmd("EXEC"), "*1\r\n+OK\r\n")
}

func TestTypesSurviveRestart(t *testing.T) {
	cfg := server.Config{Dir: t.TempDir(), AppendOnly: true, AppendFsync: persistence.Always}
	addr, stop := startServer(t, cfg)
	c, r := dial(t, addr)
	setup := []string{
		cmd("RPUSH", "l", "a", "b"),
		cmd("HSET", "h", "f", "v"),
		cmd("SADD", "s", "m"),
		cmd("ZADD", "z", "1.5", "a"),
	}
	for _, s := range setup {
		c.Write([]byte(s))
		r.ReadString('\n')
	}
	roundtrip(t, c, r, cmd("SAVE"), "+OK\r\n") // containers now in the snapshot
	roundtrip(t, c, r, cmd("MULTI"), "+OK\r\n")
	roundtrip(t, c, r, cmd("RPUSH", "l", "c"), "+QUEUED\r\n")
	roundtrip(t, c, r, cmd("ZINCRBY", "z", "1", "a"), "+QUEUED\r\n")
	roundtrip(t, c, r, cmd("EXEC"), "*2\r\n:3\r\n$3\r\n2.5\r\n") // logged as one TXN record
	c.Close()
	stop()

	addr, _ = startServer(t, cfg)
	c, r = dial(t, addr)
	roundtrip(t, c, r, cmd("LRANGE", "l", "0", "-1"), "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
	roundtrip(t, c, r, cmd("HGET", "h", "f"), "$1\r\nv\r\n")
	roundtrip(t, c, r, cmd("SMEMBERS", "s"), "*1\r\n$1\r\nm\r\n")
	roundtrip(t, c, r, cmd("ZSCORE", "z", "a"), "$3\r\n2.5\r\n")
}
