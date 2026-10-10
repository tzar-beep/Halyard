// Package differential runs identical random command streams against this server and
// a real Redis and requires byte-identical replies (after normalizing commands whose
// reply order Redis leaves unspecified).
//
// Run with a real Redis reachable at REDIS_ADDR, e.g.:
//
//	docker run -d -p 6400:6379 redis:7
//	REDIS_ADDR=127.0.0.1:6400 go test ./tests/differential
package differential

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tzar-beep/Halyard/internal/server"
)

func TestDifferential(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := server.New(server.Config{})
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	defer s.Close()

	ours, theirs := connect(t, ln.Addr().String()), connect(t, addr)
	seeds := 20
	if v := os.Getenv("DIFF_SEEDS"); v != "" {
		seeds, _ = strconv.Atoi(v)
	}
	for seed := 0; seed < seeds; seed++ {
		r := rand.New(rand.NewPCG(uint64(seed), 99))
		ours.do("FLUSHALL")
		theirs.do("FLUSHALL")
		var history []string
		for i := 0; i < 2000; i++ {
			args := gen(r)
			a, b := ours.do(args...), theirs.do(args...)
			history = append(history, strings.Join(args, " "))
			if a != b {
				start := max(0, len(history)-15)
				t.Fatalf("seed %d step %d: %q\n ours:   %s\n redis:  %s\nrecent commands:\n  %s",
					seed, i, args, a, b, strings.Join(history[start:], "\n  "))
			}
		}
	}
}

type client struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func connect(t *testing.T, addr string) *client {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &client{t, c, bufio.NewReader(c)}
}

func (c *client) do(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	c.c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.c.Write([]byte(b.String())); err != nil {
		c.t.Fatal(err)
	}
	return normalize(strings.ToUpper(args[0]), c.reply())
}

// reply reads one RESP reply and renders it as a readable canonical string.
func (c *client) reply() []string {
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatal(err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return []string{"(nil)"}
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatal(err)
		}
		return []string{strconv.Quote(string(buf[:n]))}
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return []string{"(nil-array)"}
		}
		out := []string{}
		for i := 0; i < n; i++ {
			out = append(out, strings.Join(c.reply(), " "))
		}
		return append([]string{fmt.Sprintf("array[%d]", n)}, out...)
	default:
		return []string{line}
	}
}

// unordered lists commands whose element order Redis does not define.
var unordered = map[string]int{ // value: group size (2 = field/value pairs)
	"SMEMBERS": 1, "SINTER": 1, "SUNION": 1, "SDIFF": 1,
	"HKEYS": 1, "HVALS": 1, "KEYS": 1, "HGETALL": 2,
}

func normalize(name string, parts []string) string {
	if g, ok := unordered[name]; ok && len(parts) > 1 && strings.HasPrefix(parts[0], "array") {
		items := parts[1:]
		var groups []string
		for i := 0; i+g <= len(items); i += g {
			groups = append(groups, strings.Join(items[i:i+g], "="))
		}
		sort.Strings(groups)
		return parts[0] + " " + strings.Join(groups, " ")
	}
	return strings.Join(parts, " | ")
}

// --- command generator ---

var (
	keySpace = []string{"k0", "k1", "k2", "k3", "k4"}
	members  = []string{"a", "b", "c", "d", "e", "f"}
	ints     = []string{"0", "1", "-1", "2", "-2", "3", "5", "10", "-100", "abc", "1.5", "9223372036854775807"}
	scores   = []string{"0", "1", "2", "2.5", "-1", "1e3", "-inf", "+inf", "nan", "abc", "0.1", "3"}
	bounds   = []string{"0", "1", "(1", "2", "(2.5", "-inf", "+inf", "(3", "x", "5"}
)

func pick(r *rand.Rand, xs []string) string { return xs[r.IntN(len(xs))] }

func some(r *rand.Rand, xs []string, lo, hi int) []string {
	n := lo + r.IntN(hi-lo+1)
	out := make([]string, n)
	for i := range out {
		out[i] = pick(r, xs)
	}
	return out
}

func gen(r *rand.Rand) []string {
	k := pick(r, keySpace)
	cat := func(head []string, tail ...[]string) []string {
		for _, t := range tail {
			head = append(head, t...)
		}
		return head
	}
	pairs := func(a, b []string, n int) []string {
		var out []string
		for i := 0; i < n; i++ {
			out = append(out, pick(r, a), pick(r, b))
		}
		return out
	}
	switch r.IntN(48) {
	// strings
	case 0:
		return []string{"SET", k, pick(r, ints)}
	case 1:
		return cat([]string{"SET", k, pick(r, members)}, some(r, []string{"NX", "XX", "KEEPTTL"}, 0, 1))
	case 2:
		return []string{"GET", k}
	case 3:
		return []string{"INCRBY", k, pick(r, ints)}
	case 4:
		return []string{"INCR", k}
	case 5:
		return []string{"APPEND", k, pick(r, members)}
	case 6:
		return cat([]string{"MGET"}, some(r, keySpace, 1, 3))
	case 7:
		return cat([]string{"MSET"}, pairs(keySpace, ints, 1+r.IntN(2)))
	case 8:
		return []string{"STRLEN", k}
	// keys
	case 9:
		return cat([]string{"DEL"}, some(r, keySpace, 1, 2))
	case 10:
		return cat([]string{"EXISTS"}, some(r, keySpace, 1, 3))
	case 11:
		return []string{"TYPE", k}
	case 12:
		return []string{"EXPIRE", k, pick(r, []string{"1000", "5000", "-1", "x"})}
	case 13:
		return []string{"TTL", k}
	case 14:
		return []string{"PERSIST", k}
	case 15:
		return []string{"KEYS", pick(r, []string{"*", "k[0-2]", "k?", "k[^1]", "x*"})}
	// lists
	case 16, 17:
		return cat([]string{pick(r, []string{"LPUSH", "RPUSH"}), k}, some(r, members, 1, 3))
	case 18:
		return cat([]string{pick(r, []string{"LPOP", "RPOP"}), k}, some(r, []string{"0", "1", "2", "5", "-1"}, 0, 1))
	case 19:
		return []string{"LRANGE", k, pick(r, ints), pick(r, ints)}
	case 20:
		return []string{"LINDEX", k, pick(r, ints)}
	case 21:
		// Redis truncates LSET's index to 32 bits (LSET k 2^63-1 hits the last
		// element); we return "index out of range", so avoid that input here.
		return []string{"LSET", k, pick(r, ints[:len(ints)-2]), pick(r, members)}
	case 22:
		return []string{"LTRIM", k, pick(r, ints), pick(r, ints)}
	case 23:
		return []string{"LREM", k, pick(r, ints), pick(r, members)}
	case 24:
		return []string{"LLEN", k}
	// hashes
	case 25, 26:
		return cat([]string{"HSET", k}, pairs(members, ints, 1+r.IntN(2)))
	case 27:
		return []string{"HGET", k, pick(r, members)}
	case 28:
		return cat([]string{"HDEL", k}, some(r, members, 1, 2))
	case 29:
		return []string{"HINCRBY", k, pick(r, members), pick(r, ints)}
	case 30:
		return []string{pick(r, []string{"HGETALL", "HKEYS", "HVALS", "HLEN"}), k}
	case 31:
		return cat([]string{"HMGET", k}, some(r, members, 1, 3))
	// sets
	case 32, 33:
		return cat([]string{"SADD", k}, some(r, members, 1, 3))
	case 34:
		return cat([]string{"SREM", k}, some(r, members, 1, 2))
	case 35:
		return []string{pick(r, []string{"SMEMBERS", "SCARD"}), k}
	case 36:
		return cat([]string{pick(r, []string{"SINTER", "SUNION", "SDIFF"})}, some(r, keySpace, 1, 3))
	case 37:
		return []string{"SISMEMBER", k, pick(r, members)}
	// sorted sets
	case 38, 39:
		flags := some(r, []string{"NX", "XX", "GT", "LT", "CH", "INCR"}, 0, 2)
		n := 1 + r.IntN(2)
		return cat([]string{"ZADD", k}, flags, pairs(scores, members, n))
	case 40:
		return []string{"ZINCRBY", k, pick(r, scores), pick(r, members)}
	case 41:
		return cat([]string{"ZREM", k}, some(r, members, 1, 2))
	case 42:
		return []string{pick(r, []string{"ZSCORE", "ZRANK", "ZREVRANK"}), k, pick(r, members)}
	case 43:
		return cat([]string{"ZRANGE", k, pick(r, ints), pick(r, ints)}, some(r, []string{"REV", "WITHSCORES"}, 0, 2))
	case 44:
		cmd := pick(r, []string{"ZRANGEBYSCORE", "ZREVRANGEBYSCORE"})
		extra := some(r, []string{"WITHSCORES"}, 0, 1)
		if r.IntN(2) == 0 {
			extra = append(extra, "LIMIT", pick(r, []string{"0", "1", "-1"}), pick(r, []string{"1", "2", "-1", "0"}))
		}
		return cat([]string{cmd, k, pick(r, bounds), pick(r, bounds)}, extra)
	case 45:
		return []string{"ZCOUNT", k, pick(r, bounds), pick(r, bounds)}
	case 46:
		return cat([]string{"ZRANGE", k, pick(r, bounds), pick(r, bounds), "BYSCORE"}, some(r, []string{"REV", "WITHSCORES"}, 0, 2))
	default:
		if r.IntN(2) == 0 {
			return []string{"DBSIZE"}
		}
		return []string{"ZCARD", k}
	}
}
