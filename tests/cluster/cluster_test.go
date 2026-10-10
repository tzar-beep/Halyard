// Package cluster_test runs real multi-node clusters over TCP on localhost.
package cluster_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tzar-beep/redis/internal/cluster"
	"github.com/tzar-beep/redis/internal/raft"
	"github.com/tzar-beep/redis/internal/server"
)

const tick = 20 * time.Millisecond // election timeout 200-400ms

type tnode struct {
	id    raft.NodeID
	peer  cluster.Peer
	dir   string
	srv   *server.Server
	ln    net.Listener
	peers []cluster.Peer
	up    bool
}

type tcluster struct {
	t     *testing.T
	nodes map[raft.NodeID]*tnode
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newCluster(t *testing.T, n int) *tcluster {
	c := &tcluster{t: t, nodes: map[raft.NodeID]*tnode{}}
	var peers []cluster.Peer
	for i := 1; i <= n; i++ {
		peers = append(peers, cluster.Peer{ID: raft.NodeID(i), RaftAddr: freeAddr(t), ClientAddr: freeAddr(t)})
	}
	for _, p := range peers {
		nd := &tnode{id: p.ID, peer: p, dir: t.TempDir(), peers: peers}
		c.nodes[p.ID] = nd
		c.start(nd)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			c.stop(nd)
		}
	})
	return c
}

func (c *tcluster) start(nd *tnode) {
	c.t.Helper()
	ln, err := net.Listen("tcp", nd.peer.ClientAddr)
	if err != nil {
		c.t.Fatal(err)
	}
	srv, err := server.New(server.Config{Cluster: &cluster.Config{
		Self: nd.peer, Peers: nd.peers, Dir: nd.dir, Tick: tick,
		SnapshotEntries: 100, ProposeTimeout: 2 * time.Second, Secret: "s3cret",
	}})
	if err != nil {
		c.t.Fatal(err)
	}
	go srv.Serve(ln)
	nd.srv, nd.ln, nd.up = srv, ln, true
}

func (c *tcluster) stop(nd *tnode) {
	if nd.up {
		nd.srv.Close()
		nd.up = false
	}
}

func (c *tcluster) leader(timeout time.Duration) *tnode {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, nd := range c.nodes {
			if nd.up && nd.srv.Node().CurrentStatus().State == raft.StateLeader {
				return nd
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader within %v", timeout)
	return nil
}

// setFilter partitions the network: blocked nodes cannot talk to anyone.
func (c *tcluster) isolate(blocked ...raft.NodeID) {
	set := map[raft.NodeID]bool{}
	for _, id := range blocked {
		set[id] = true
	}
	for _, nd := range c.nodes {
		if nd.up {
			nd.srv.Node().SetFilter(func(from, to raft.NodeID) bool { return !set[from] && !set[to] })
		}
	}
}

func (c *tcluster) heal() {
	for _, nd := range c.nodes {
		if nd.up {
			nd.srv.Node().SetFilter(nil)
		}
	}
}

// waitConverged waits until every running node has applied the same index.
func (c *tcluster) waitConverged(timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var applied []uint64
		for _, nd := range c.nodes {
			if nd.up {
				applied = append(applied, nd.srv.Node().CurrentStatus().Applied)
			}
		}
		same := true
		for _, a := range applied {
			same = same && a == applied[0]
		}
		if same && applied[0] > 0 {
			return applied[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	var dump []string
	for _, nd := range c.nodes {
		if nd.up {
			dump = append(dump, fmt.Sprintf("%+v", nd.srv.Node().CurrentStatus()))
		}
	}
	c.t.Fatalf("nodes did not converge:\n%s", strings.Join(dump, "\n"))
	return 0
}

// --- minimal client ---

type client struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &client{t, c, bufio.NewReader(c)}
}

// do sends a command and returns the reply's first line (+ bulk payload if any).
func (c *client) do(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	c.c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.c.Write([]byte(b.String())); err != nil {
		return "ERRCONN " + err.Error()
	}
	return c.read()
}

func (c *client) read() string {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "ERRCONN " + err.Error()
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		io.ReadFull(c.r, buf)
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		parts := []string{}
		for i := 0; i < n; i++ {
			parts = append(parts, c.read())
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return line
}

func TestReplicationAndRedirect(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader(5 * time.Second)
	lc := dial(t, l.peer.ClientAddr)
	if got := lc.do("SET", "k", "v"); got != "+OK" {
		t.Fatalf("SET: %s", got)
	}
	if got := lc.do("GET", "k"); got != "v" {
		t.Fatalf("GET: %s", got)
	}
	for _, nd := range c.nodes {
		if nd == l {
			continue
		}
		want := "-MOVED 0 " + l.peer.ClientAddr
		if got := dial(t, nd.peer.ClientAddr).do("GET", "k"); got != want {
			t.Fatalf("follower GET: %q want %q", got, want)
		}
	}
	// Transactions, data types and TTLs replicate too.
	lc.do("MULTI")
	lc.do("RPUSH", "l", "a", "b")
	lc.do("ZADD", "z", "1", "m")
	if got := lc.do("EXEC"); got != "[:2 :1]" {
		t.Fatalf("EXEC: %s", got)
	}
	lc.do("SET", "ttl", "x", "PX", "100")
	time.Sleep(600 * time.Millisecond) // expires, then the leader proposes DELEXPIRED
	if got := lc.do("GET", "ttl"); got != "(nil)" {
		t.Fatalf("expired key: %s", got)
	}
	c.waitConverged(5 * time.Second)
	if got := lc.do("RAFT", "STATUS"); !strings.Contains(got, "state:leader") {
		t.Fatalf("RAFT STATUS: %s", got)
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader(5 * time.Second)
	lc := dial(t, l.peer.ClientAddr)
	for i := 0; i < 300; i++ { // crosses SnapshotEntries: exercises compaction
		if got := lc.do("SET", "key"+strconv.Itoa(i), strconv.Itoa(i)); got != "+OK" {
			t.Fatalf("SET %d: %s", i, got)
		}
	}
	killed := time.Now()
	c.stop(l)
	nl := c.leader(5 * time.Second)
	nc := dial(t, nl.peer.ClientAddr)
	for nc.do("SET", "after", "failover") != "+OK" {
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("failover: new leader n%d accepting writes %v after kill", nl.id, time.Since(killed))
	if d := time.Since(killed); d > 2*time.Second {
		t.Fatalf("failover took %v", d)
	}
	for i := 0; i < 300; i++ {
		if got := nc.do("GET", "key"+strconv.Itoa(i)); got != strconv.Itoa(i) {
			t.Fatalf("acknowledged write key%d lost: %q", i, got)
		}
	}
	// The old leader restarts from its WAL and catches up.
	c.start(l)
	c.waitConverged(10 * time.Second)
	if st := l.srv.Node().CurrentStatus(); st.State == raft.StateLeader && st.Term <= nl.srv.Node().CurrentStatus().Term-1 {
		t.Fatalf("stale leader")
	}
}

// TestPartitionedLeader isolates the leader: it must stop serving (CheckQuorum),
// the majority must elect a new leader, and after healing the old leader rejoins
// without its unacknowledged writes surviving.
func TestPartitionedLeader(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader(5 * time.Second)
	lc := dial(t, l.peer.ClientAddr)
	lc.do("SET", "before", "1")
	c.waitConverged(5 * time.Second)

	c.isolate(l.id)
	// A write to the isolated leader cannot be acknowledged.
	if got := lc.do("SET", "lost", "x"); got == "+OK" {
		t.Fatalf("isolated leader acknowledged a write")
	}
	var nl *tnode
	deadline := time.Now().Add(5 * time.Second)
	for nl == nil || nl == l {
		if time.Now().After(deadline) {
			t.Fatal("majority did not elect a new leader")
		}
		nl = c.leader(5 * time.Second)
		time.Sleep(10 * time.Millisecond)
	}
	nc := dial(t, nl.peer.ClientAddr)
	if got := nc.do("SET", "during", "2"); got != "+OK" {
		t.Fatalf("majority write: %s", got)
	}
	// The isolated node stepped down (no quorum), so it redirects or reports down.
	if got := dial(t, l.peer.ClientAddr).do("GET", "before"); !strings.HasPrefix(got, "-MOVED") && !strings.HasPrefix(got, "-CLUSTERDOWN") {
		t.Fatalf("isolated old leader still serving reads: %q", got)
	}
	c.heal()
	c.waitConverged(10 * time.Second)
	nl = c.leader(5 * time.Second)
	nc = dial(t, nl.peer.ClientAddr)
	if got := nc.do("MGET", "before", "during", "lost"); got != "[1 2 (nil)]" {
		t.Fatalf("after heal: %s", got)
	}
}

// TestJoinViaSnapshot adds a fourth node after the log was compacted, so it must
// catch up by InstallSnapshot, then removes an original member.
func TestJoinViaSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader(5 * time.Second)
	lc := dial(t, l.peer.ClientAddr)
	for i := 0; i < 250; i++ {
		lc.do("SET", "k"+strconv.Itoa(i), "v")
	}
	p4 := cluster.Peer{ID: 4, RaftAddr: freeAddr(t), ClientAddr: freeAddr(t)}
	n4 := &tnode{id: 4, peer: p4, dir: t.TempDir()} // no peers: joins empty
	c.nodes[4] = n4
	c.start(n4)
	if got := lc.do("RAFT", "ADD", "4", p4.RaftAddr, p4.ClientAddr); got != "+OK" {
		t.Fatalf("RAFT ADD: %s", got)
	}
	c.waitConverged(10 * time.Second)
	if st := n4.srv.Node().CurrentStatus(); st.SnapIndex == 0 || len(st.Voters) != 4 {
		t.Fatalf("n4 did not join via snapshot: %+v", st)
	}
	var victim raft.NodeID = 1
	if l.id == 1 {
		victim = 2
	}
	if got := lc.do("RAFT", "REMOVE", strconv.Itoa(int(victim))); got != "+OK" {
		t.Fatalf("RAFT REMOVE: %s", got)
	}
	c.stop(c.nodes[victim])
	delete(c.nodes, victim)
	// Kill the leader too: n4 must be able to help elect a new one.
	c.stop(l)
	nl := c.leader(5 * time.Second)
	nc := dial(t, nl.peer.ClientAddr)
	for nc.do("SET", "final", "ok") != "+OK" {
		time.Sleep(20 * time.Millisecond)
	}
	if got := nc.do("GET", "k249"); got != "v" {
		t.Fatalf("data lost after membership changes: %q", got)
	}
}

// TestConcurrentClientsDuringFailover hammers the cluster from many clients while
// the leader is killed; every acknowledged write must survive.
func TestConcurrentClientsDuringFailover(t *testing.T) {
	c := newCluster(t, 3)
	c.leader(5 * time.Second)
	var mu sync.Mutex
	acked := map[string]bool{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var all []*tnode // fixed snapshot: workers must not touch c.nodes or nd.up
	for _, nd := range c.nodes {
		all = append(all, nd)
	}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var cl *client
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if cl == nil {
					var l *tnode
					for _, nd := range all {
						if nd.srv.Node().CurrentStatus().State == raft.StateLeader {
							l = nd
						}
					}
					if l == nil {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					conn, err := net.Dial("tcp", l.peer.ClientAddr)
					if err != nil {
						continue
					}
					cl = &client{t, conn, bufio.NewReader(conn)}
				}
				k := fmt.Sprintf("w%d-%d", w, i)
				if got := cl.do("SET", k, k); got == "+OK" {
					mu.Lock()
					acked[k] = true
					mu.Unlock()
				} else {
					cl.c.Close()
					cl = nil
				}
			}
		}(w)
	}
	time.Sleep(500 * time.Millisecond)
	c.stop(c.leader(time.Second))
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()

	nl := c.leader(5 * time.Second)
	nc := dial(t, nl.peer.ClientAddr)
	missing := 0
	for k := range acked {
		if nc.do("GET", k) != k {
			missing++
		}
	}
	t.Logf("%d acknowledged writes across a leader kill, %d missing", len(acked), missing)
	if missing > 0 || len(acked) == 0 {
		t.Fatalf("lost %d of %d acknowledged writes", missing, len(acked))
	}
}
