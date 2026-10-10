// Package jepsen_test is a Jepsen-style test: concurrent clients read and write a
// few keys on a real 3-node cluster over TCP while a nemesis partitions leaders and
// kills/restarts nodes. Every operation's invocation and completion time and
// outcome is recorded, and the history is checked for linearizability.
package jepsen_test

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tzar-beep/Halyard/internal/cluster"
	"github.com/tzar-beep/Halyard/internal/linearize"
	"github.com/tzar-beep/Halyard/internal/raft"
	"github.com/tzar-beep/Halyard/internal/server"
)

type node struct {
	peer  cluster.Peer
	peers []cluster.Peer
	dir   string
	mu    sync.Mutex
	srv   *server.Server
}

var unsafeReads bool // set by the negative-control test

func (n *node) start(t *testing.T) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.srv != nil {
		return
	}
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ { // the port may linger briefly after a restart
		if ln, err = net.Listen("tcp", n.peer.ClientAddr); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Errorf("listen: %v", err)
		return
	}
	srv, err := server.New(server.Config{Cluster: &cluster.Config{Self: n.peer, Peers: n.peers, Dir: n.dir,
		Tick: 20 * time.Millisecond, SnapshotEntries: 200, ProposeTimeout: time.Second, UnsafeReads: unsafeReads}})
	if err != nil {
		ln.Close()
		t.Errorf("start: %v", err)
		return
	}
	go srv.Serve(ln)
	n.srv = srv
}

func (n *node) stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.srv != nil {
		n.srv.Close()
		n.srv = nil
	}
}

func (n *node) status() (cluster.Status, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.srv == nil {
		return cluster.Status{}, false
	}
	return n.srv.Node().CurrentStatus(), true
}

func (n *node) setFilter(f func(from, to raft.NodeID) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.srv != nil {
		n.srv.Node().SetFilter(f)
	}
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// --- client ---

type outcome int

const (
	ok      outcome = iota
	failed          // definitely not applied
	unknown         // may or may not have been applied
)

type client struct {
	addrs []string
	addr  string
	c     net.Conn
	br    *bufio.Reader
}

func (c *client) roundtrip(args ...string) (string, error) {
	if c.c == nil {
		nc, err := net.DialTimeout("tcp", c.addr, 500*time.Millisecond)
		if err != nil {
			return "", err
		}
		c.c, c.br = nc, bufio.NewReader(nc)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	c.c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.c.Write([]byte(b.String())); err != nil {
		c.reset()
		return "", err
	}
	line, err := c.br.ReadString('\n')
	if err != nil {
		c.reset()
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if strings.HasPrefix(line, "$") {
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return linearize.Nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.br, buf); err != nil {
			c.reset()
			return "", err
		}
		return string(buf[:n]), nil
	}
	return line, nil
}

func (c *client) reset() {
	if c.c != nil {
		c.c.Close()
	}
	c.c = nil
}

// do performs one operation, following redirects, and classifies its outcome.
func (c *client) do(rng *rand.Rand, args ...string) (string, outcome) {
	write := args[0] == "SET"
	sent := false // a request reached a node that may have proposed it
	for attempt := 0; attempt < 5; attempt++ {
		rep, err := c.roundtrip(args...)
		switch {
		case err != nil:
			if sent || write { // the request may have been delivered before the failure
				c.addr = c.addrs[rng.IntN(len(c.addrs))]
				return "", unknown
			}
			c.addr = c.addrs[rng.IntN(len(c.addrs))]
			continue
		case strings.HasPrefix(rep, "-MOVED "):
			c.reset()
			c.addr = strings.Fields(rep)[2]
			continue
		case strings.HasPrefix(rep, "-CLUSTERDOWN"), strings.HasPrefix(rep, "-TRYAGAIN"),
			strings.HasPrefix(rep, "-ERR write not applied"):
			return "", failed
		case strings.HasPrefix(rep, "-"):
			return rep, unknown // timeouts, shutdown: outcome unknown
		}
		return rep, ok
	}
	return "", failed
}

func TestLinearizableUnderFaults(t *testing.T) {
	duration := 8 * time.Second
	if testing.Short() {
		duration = 3 * time.Second
	}
	if v := os.Getenv("JEPSEN_SECONDS"); v != "" {
		s, _ := strconv.Atoi(v)
		duration = time.Duration(s) * time.Second
	}
	res, okOps := runJepsen(t, duration)
	if !res.OK {
		t.Fatalf("history for key %q is NOT linearizable (%d ops)", res.Key, len(res.Ops))
	}
	if okOps < 50 {
		t.Fatalf("too few successful writes (%d): the cluster was unavailable most of the time", okOps)
	}
}

// TestCheckerCatchesStaleReads is the negative control: with ReadIndex disabled and
// reads spread over followers, the same harness must find a violation.
func TestCheckerCatchesStaleReads(t *testing.T) {
	unsafeReads = true
	defer func() { unsafeReads = false }()
	for attempt := 1; attempt <= 3; attempt++ {
		if res, _ := runJepsen(t, 4*time.Second); !res.OK {
			t.Logf("attempt %d: stale reads detected on key %q, as expected", attempt, res.Key)
			return
		}
	}
	t.Fatal("unsafe reads went undetected: the harness is not sensitive enough")
}

func runJepsen(t *testing.T, duration time.Duration) (linearize.Result, int) {
	var peers []cluster.Peer
	for i := 1; i <= 3; i++ {
		peers = append(peers, cluster.Peer{ID: raft.NodeID(i), RaftAddr: freeAddr(t), ClientAddr: freeAddr(t)})
	}
	var nodes []*node
	var addrs []string
	for _, p := range peers {
		n := &node{peer: p, peers: peers, dir: t.TempDir()}
		n.start(t)
		nodes = append(nodes, n)
		addrs = append(addrs, p.ClientAddr)
		t.Cleanup(n.stop)
	}

	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var mu sync.Mutex
	var history []linearize.Op
	var events []string
	logEvent := func(format string, a ...any) {
		mu.Lock()
		events = append(events, fmt.Sprintf("%6dms ", time.Since(start).Milliseconds())+fmt.Sprintf(format, a...))
		mu.Unlock()
	}
	keys := []string{"x", "y", "z"}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Clients.
	for id := 0; id < 6; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(id), 3))
			c := &client{addrs: addrs, addr: addrs[id%3]}
			defer c.reset()
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				key := keys[rng.IntN(len(keys))]
				op := linearize.Op{Client: id, Key: key, Call: now()}
				var rep string
				var out outcome
				if rng.IntN(2) == 0 {
					op.Write, op.Value = true, fmt.Sprintf("c%d-%d", id, seq)
					rep, out = c.do(rng, "SET", key, op.Value)
				} else if unsafeReads {
					// Read from a random node, without following redirects.
					rc := &client{addrs: addrs, addr: addrs[rng.IntN(3)]}
					rep, out = rc.do(rng, "GET", key)
					rc.reset()
					op.Value = rep
				} else {
					rep, out = c.do(rng, "GET", key)
					op.Value = rep
				}
				switch out {
				case ok:
					op.Return = now()
				case unknown:
					if !op.Write {
						continue // an unknown read constrains nothing
					}
					op.Return = linearize.Unknown
				case failed:
					continue
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
				time.Sleep(time.Duration(rng.IntN(3)) * time.Millisecond)
			}
		}(id)
	}

	// Nemesis.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(42, 42))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(300+rng.IntN(500)) * time.Millisecond):
			}
			leader := -1
			for i, n := range nodes {
				if st, up := n.status(); up && st.State == raft.StateLeader {
					leader = i
				}
			}
			switch rng.IntN(4) {
			case 0: // isolate the leader (or a random node)
				victim := rng.IntN(3)
				if leader >= 0 {
					victim = leader
				}
				id := nodes[victim].peer.ID
				for _, n := range nodes {
					n.setFilter(func(from, to raft.NodeID) bool { return from != id && to != id })
				}
				logEvent("partition: isolate n%d", id)
			case 1: // heal
				for _, n := range nodes {
					n.setFilter(nil)
				}
				logEvent("heal")
			case 2: // kill a node (preferring the leader)
				victim := rng.IntN(3)
				if leader >= 0 && rng.IntN(2) == 0 {
					victim = leader
				}
				nodes[victim].stop()
				logEvent("kill n%d", nodes[victim].peer.ID)
			case 3: // restart everything that is down
				for _, n := range nodes {
					if _, up := n.status(); !up {
						n.start(t)
						logEvent("restart n%d", n.peer.ID)
					}
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// Heal, restart, and take a final read of every key.
	for _, n := range nodes {
		n.start(t)
		n.setFilter(nil)
	}
	c := &client{addrs: addrs, addr: addrs[0]}
	defer c.reset()
	rng := rand.New(rand.NewPCG(9, 9))
	for _, k := range keys {
		for attempt := 0; ; attempt++ {
			call := now()
			rep, out := c.do(rng, "GET", k)
			if out == ok {
				history = append(history, linearize.Op{Client: 99, Key: k, Value: rep, Call: call, Return: now()})
				break
			}
			if attempt > 100 {
				t.Fatalf("cluster did not recover for final read of %s", k)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	var okOps, unknownOps, reads int
	for _, op := range history {
		switch {
		case op.Return == linearize.Unknown:
			unknownOps++
		case op.Write:
			okOps++
		default:
			reads++
		}
	}
	t.Logf("history: %d acknowledged writes, %d reads, %d writes with unknown outcome; %d nemesis events",
		okOps, reads, unknownOps, len(events))
	checkStart := time.Now()
	res := linearize.Check(history)
	t.Logf("linearizability check took %v", time.Since(checkStart))
	if !res.OK && !unsafeReads {
		t.Logf("nemesis:\n  %s", strings.Join(events, "\n  "))
	}
	return res, okOps
}
