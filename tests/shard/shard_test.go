// Package shard_test runs sharded multi-Raft clusters over TCP on localhost.
package shard_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tzar-beep/Halyard/internal/cluster"
	"github.com/tzar-beep/Halyard/internal/raft"
	"github.com/tzar-beep/Halyard/internal/server"
	"github.com/tzar-beep/Halyard/internal/shard"
)

const (
	pw   = "pw"
	tick = 20 * time.Millisecond
)

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type tnode struct {
	peer cluster.Peer
	srv  *server.Server
	up   bool
}

func startCluster(t *testing.T, nodes, groups int) []*tnode {
	var peers []cluster.Peer
	for i := 1; i <= nodes; i++ {
		peers = append(peers, cluster.Peer{ID: raft.NodeID(i), RaftAddr: freeAddr(t), ClientAddr: freeAddr(t)})
	}
	var out []*tnode
	for _, p := range peers {
		ln, err := net.Listen("tcp", p.ClientAddr)
		if err != nil {
			t.Fatal(err)
		}
		srv, err := server.New(server.Config{Password: pw, Shards: groups, Cluster: &cluster.Config{
			Self: p, Peers: peers, Dir: t.TempDir(), Tick: tick, SnapshotEntries: 500}})
		if err != nil {
			t.Fatal(err)
		}
		go srv.Serve(ln)
		nd := &tnode{peer: p, srv: srv, up: true}
		out = append(out, nd)
		t.Cleanup(func() {
			if nd.up {
				nd.srv.Close()
			}
		})
	}
	// Wait until the cluster serves every slot.
	c := newClient(t, out[0].peer.ClientAddr)
	deadline := time.Now().Add(10 * time.Second)
	for g := 0; g < 64; g++ {
		for c.do("SET", "warmup"+strconv.Itoa(g), "1") != "+OK" {
			if time.Now().After(deadline) {
				t.Fatal("cluster did not become available")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return out
}

// client is a minimal cluster-aware client: it caches slot owners from MOVED,
// follows ASK with ASKING, and retries TRYAGAIN / transient errors.
type client struct {
	t     *testing.T
	seed  string
	slots map[int]string
	conns map[string]*conn
}

type conn struct {
	c  net.Conn
	br *bufio.Reader
}

func newClient(t *testing.T, seed string) *client {
	c := &client{t: t, seed: seed, slots: map[int]string{}, conns: map[string]*conn{}}
	t.Cleanup(c.close)
	return c
}

func (c *client) close() {
	for _, cn := range c.conns {
		cn.c.Close()
	}
}

func (c *client) conn(addr string) (*conn, error) {
	if cn, ok := c.conns[addr]; ok {
		return cn, nil
	}
	nc, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}
	cn := &conn{nc, bufio.NewReader(nc)}
	if r := cn.roundtrip("AUTH", pw); r != "+OK" {
		nc.Close()
		return nil, fmt.Errorf("auth: %s", r)
	}
	c.conns[addr] = cn
	return cn, nil
}

func (cn *conn) roundtrip(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	cn.c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := cn.c.Write([]byte(b.String())); err != nil {
		return "-CONNERR"
	}
	return read(cn.br)
}

func read(br *bufio.Reader) string {
	line, err := br.ReadString('\n')
	if err != nil {
		return "-CONNERR"
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		io.ReadFull(br, buf)
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		var parts []string
		for i := 0; i < n; i++ {
			parts = append(parts, read(br))
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return line
}

func (c *client) do(args ...string) string {
	bargs := make([][]byte, len(args))
	for i, a := range args {
		bargs[i] = []byte(a)
	}
	slot := -1
	if keys := shard.Keys(bargs); len(keys) > 0 {
		slot = shard.SlotOf(keys)
	}
	addr := c.seed
	if a, ok := c.slots[slot]; ok {
		addr = a
	}
	asking := false
	var rep string
	for attempt := 0; attempt < 100; attempt++ {
		cn, err := c.conn(addr)
		if err != nil {
			for s, a := range c.slots { // forget routes to a dead node
				if a == addr {
					delete(c.slots, s)
				}
			}
			addr = c.seed
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if asking {
			cn.roundtrip("ASKING")
			asking = false
		}
		rep = cn.roundtrip(args...)
		switch {
		case rep == "-CONNERR":
			cn.c.Close()
			delete(c.conns, addr)
			for s, a := range c.slots {
				if a == addr {
					delete(c.slots, s)
				}
			}
			addr = c.seed
		case strings.HasPrefix(rep, "-MOVED "):
			f := strings.Fields(rep)
			s, _ := strconv.Atoi(f[1])
			c.slots[s] = f[2]
			addr = f[2]
			continue
		case strings.HasPrefix(rep, "-ASK "):
			addr, asking = strings.Fields(rep)[2], true
			continue
		case strings.HasPrefix(rep, "-TRYAGAIN"), strings.HasPrefix(rep, "-CLUSTERDOWN"), strings.HasPrefix(rep, "-ERR timeout"):
		default:
			return rep
		}
		time.Sleep(20 * time.Millisecond)
	}
	return rep
}

func TestRoutingAndClusterCommands(t *testing.T) {
	nodes := startCluster(t, 3, 3)
	c := newClient(t, nodes[0].peer.ClientAddr)
	for i := 0; i < 500; i++ {
		if got := c.do("SET", "key:"+strconv.Itoa(i), strconv.Itoa(i)); got != "+OK" {
			t.Fatalf("SET %d: %s", i, got)
		}
	}
	for i := 0; i < 500; i++ {
		if got := c.do("GET", "key:"+strconv.Itoa(i)); got != strconv.Itoa(i) {
			t.Fatalf("GET %d: %s", i, got)
		}
	}
	for _, tc := range []struct {
		key  string
		slot int
	}{{"somekey", 11058}, {"foo{hash_tag}", 2515}, {"bar{hash_tag}", 2515}} {
		if got := c.do("CLUSTER", "KEYSLOT", tc.key); got != ":"+strconv.Itoa(tc.slot) {
			t.Fatalf("KEYSLOT %s = %s want %d", tc.key, got, tc.slot)
		}
	}
	if got := c.do("MSET", "a", "1", "b", "2"); !strings.HasPrefix(got, "-CROSSSLOT") {
		t.Fatalf("cross-slot MSET: %s", got)
	}
	if got := c.do("MSET", "{u1}a", "1", "{u1}b", "2"); got != "+OK" {
		t.Fatalf("hashtag MSET: %s", got)
	}
	if got := c.do("MGET", "{u1}a", "{u1}b"); got != "[1 2]" {
		t.Fatalf("hashtag MGET: %s", got)
	}
	slots := c.do("CLUSTER", "SLOTS")
	if !strings.HasPrefix(slots, "[[:0 :5460 ") || strings.Count(slots, ":16383") != 1 {
		t.Fatalf("CLUSTER SLOTS: %s", slots)
	}
}

func TestLeadersBalanced(t *testing.T) {
	nodes := startCluster(t, 3, 3)
	deadline := time.Now().Add(15 * time.Second)
	for {
		leaders := map[raft.NodeID]int{}
		for _, nd := range nodes {
			st := nd.srv.Router().Host() // ensure host accessible
			_ = st
		}
		for g := uint32(1); g <= 3; g++ {
			for _, nd := range nodes {
				if strings.Contains(nd.srv.Router().StatusOf(g), "state:leader") {
					leaders[nd.peer.ID]++
				}
			}
		}
		if len(leaders) == 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("data group leaders not spread across nodes: %v", leaders)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestLiveMigration moves a slot between groups while clients increment counters
// in it; every acknowledged increment must be reflected exactly.
func TestLiveMigration(t *testing.T) {
	nodes := startCluster(t, 3, 3)
	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("{mig}k%d", i)
	}
	slot := shard.KeySlot([]byte(keys[0]))
	src := shard.InitialOwner(slot, 3)
	dst := src%3 + 1

	var acked, attempted [30]atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := newClient(t, nodes[w%3].peer.ClientAddr)
			for i := w; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := i % len(keys)
				attempted[k].Add(1)
				if rep := c.do("INCR", keys[k]); strings.HasPrefix(rep, ":") {
					acked[k].Add(1)
				}
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	admin := newClient(t, nodes[0].peer.ClientAddr)
	start := time.Now()
	moved := admin.do("CLUSTER", "MIGRATE", strconv.Itoa(slot), strconv.Itoa(int(dst)))
	t.Logf("migrated slot %d from group %d to %d: %s keys in %v", slot, src, dst, moved, time.Since(start))
	if !strings.HasPrefix(moved, ":") {
		t.Fatalf("CLUSTER MIGRATE: %s", moved)
	}
	time.Sleep(300 * time.Millisecond) // keep writing against the new owner
	close(stop)
	wg.Wait()

	c := newClient(t, nodes[1].peer.ClientAddr)
	for i, k := range keys {
		got, _ := strconv.ParseInt(c.do("GET", k), 10, 64)
		if got < acked[i].Load() || got > attempted[i].Load() {
			t.Fatalf("%s = %d, acknowledged %d, attempted %d", k, got, acked[i].Load(), attempted[i].Load())
		}
	}
	if !strings.Contains(c.do("CLUSTER", "SLOTS"), fmt.Sprintf(":%d :%d ", slot, slot)) {
		t.Fatalf("slot %d not split out in CLUSTER SLOTS", slot)
	}
	for _, nd := range nodes {
		if n := nd.srv.Router().KeysInSlot(src, slot); n != 0 {
			t.Fatalf("source group still holds %d keys of slot %d", n, slot)
		}
	}
}

func TestShardedFailover(t *testing.T) {
	nodes := startCluster(t, 3, 3)
	c := newClient(t, nodes[0].peer.ClientAddr)
	for i := 0; i < 200; i++ {
		c.do("SET", "f"+strconv.Itoa(i), "v")
	}
	nodes[2].srv.Close()
	nodes[2].up = false
	start := time.Now()
	for i := 0; i < 200; i++ {
		if got := c.do("SET", "g"+strconv.Itoa(i), "v"); got != "+OK" {
			t.Fatalf("write after node loss: %s", got)
		}
		if got := c.do("GET", "f"+strconv.Itoa(i)); got != "v" {
			t.Fatalf("lost f%d: %s", i, got)
		}
	}
	t.Logf("all slots writable again; 400 ops completed %v after killing a node", time.Since(start))
}

// TestThroughputScaling compares 1 vs 3 data groups on the same 3 nodes.
func TestThroughputScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark")
	}
	run := func(groups int) float64 {
		nodes := startCluster(t, 3, groups)
		time.Sleep(3 * time.Second) // let leaders balance
		var ops atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 48; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				c := newClient(t, nodes[w%3].peer.ClientAddr)
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if c.do("SET", fmt.Sprintf("w%d:%d", w, i), "x") == "+OK" {
						ops.Add(1)
					}
				}
			}(w)
		}
		time.Sleep(3 * time.Second)
		close(stop)
		wg.Wait()
		for _, nd := range nodes {
			nd.srv.Close()
			nd.up = false
		}
		return float64(ops.Load()) / 3
	}
	one, three := run(1), run(3)
	t.Logf("SET throughput, 48 clients, 3 nodes: 1 group %.0f ops/s, 3 groups %.0f ops/s (%.2fx)", one, three, three/one)
}
