package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// cmd encodes args as a RESP array.
func cmd(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func TestStringCommands(t *testing.T) {
	c, r := dial(t, start(t))
	steps := []struct{ req, want string }{
		{cmd("MSET", "a", "1", "b", "2"), "+OK\r\n"},
		{cmd("MGET", "a", "x", "b"), "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n"},
		{cmd("MSET", "a"), "-ERR wrong number of arguments for 'mset' command\r\n"},
		{cmd("EXISTS", "a", "a", "x"), ":2\r\n"},
		{cmd("DEL", "a", "x"), ":1\r\n"},
		{cmd("INCR", "n"), ":1\r\n"},
		{cmd("INCRBY", "n", "10"), ":11\r\n"},
		{cmd("DECR", "n"), ":10\r\n"},
		{cmd("DECRBY", "n", "15"), ":-5\r\n"},
		{cmd("INCR", "b2"), ":1\r\n"},
		{cmd("SET", "s", "abc"), "+OK\r\n"},
		{cmd("INCR", "s"), "-ERR value is not an integer or out of range\r\n"},
		{cmd("INCRBY", "n", "x"), "-ERR value is not an integer or out of range\r\n"},
		{cmd("SET", "s", "v", "NX"), "$-1\r\n"},
		{cmd("SET", "new", "v", "XX"), "$-1\r\n"},
		{cmd("SET", "s", "v", "NX", "XX"), "-ERR syntax error\r\n"},
		{cmd("SET", "s", "v", "EX", "0"), "-ERR invalid expire time in 'set' command\r\n"},
		{cmd("SET", "s", "v", "EX"), "-ERR syntax error\r\n"},
		{cmd("SET", "s", "v", "BOGUS"), "-ERR syntax error\r\n"},
	}
	for _, s := range steps {
		roundtrip(t, c, r, s.req, s.want)
	}
}

func TestTTLCommands(t *testing.T) {
	c, r := dial(t, start(t))
	roundtrip(t, c, r, cmd("TTL", "k"), ":-2\r\n")
	roundtrip(t, c, r, cmd("SET", "k", "v"), "+OK\r\n")
	roundtrip(t, c, r, cmd("TTL", "k"), ":-1\r\n")
	roundtrip(t, c, r, cmd("EXPIRE", "k", "100"), ":1\r\n")
	roundtrip(t, c, r, cmd("TTL", "k"), ":100\r\n")
	roundtrip(t, c, r, cmd("PERSIST", "k"), ":1\r\n")
	roundtrip(t, c, r, cmd("PERSIST", "k"), ":0\r\n")
	roundtrip(t, c, r, cmd("EXPIRE", "missing", "10"), ":0\r\n")
	roundtrip(t, c, r, cmd("EXPIRE", "k", "x"), "-ERR value is not an integer or out of range\r\n")
	roundtrip(t, c, r, cmd("EXPIRE", "k", "-1"), ":1\r\n")
	roundtrip(t, c, r, cmd("EXISTS", "k"), ":0\r\n")

	roundtrip(t, c, r, cmd("SET", "p", "v", "PX", "100"), "+OK\r\n")
	roundtrip(t, c, r, cmd("GET", "p"), "$1\r\nv\r\n")
	time.Sleep(150 * time.Millisecond)
	roundtrip(t, c, r, cmd("GET", "p"), "$-1\r\n")

	roundtrip(t, c, r, cmd("SET", "q", "v", "EX", "50"), "+OK\r\n")
	roundtrip(t, c, r, cmd("SET", "q", "w", "KEEPTTL"), "+OK\r\n")
	roundtrip(t, c, r, cmd("TTL", "q"), ":50\r\n")
	roundtrip(t, c, r, cmd("SET", "q", "x"), "+OK\r\n")
	roundtrip(t, c, r, cmd("TTL", "q"), ":-1\r\n")
}
