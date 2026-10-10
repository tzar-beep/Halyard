package raft

import (
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The simulator runs a whole cluster in one goroutine with a virtual clock and a
// hostile network (loss, duplication, reordering, delay, partitions), plus crashes
// that lose all volatile state and log compaction. Everything derives from a seed,
// so any failure is reproducible with SEED=<n>.
//
// Checked continuously:
//   - Election Safety: at most one leader per term.
//   - State Machine Safety: no two nodes apply different entries at an index.
//   - Read linearizability: a ReadIndex read observes every write acknowledged
//     before the read started.
// Checked at the end, after healing everything:
//   - Liveness: one leader emerges, all nodes converge, a new write commits.
//   - Durability: every acknowledged write is in every node's state machine.

type simNode struct {
	id   NodeID
	r    *Raft
	up   bool
	disk Persisted // what has been "fsynced"

	sm         []string // state machine: applied values in order
	appliedIdx uint64
	peers      []NodeID            // bootstrap configuration
	proposals  map[uint64]proposal // index -> what we proposed there
	reads      map[uint64]readCheck
}

type proposal struct {
	term uint64
	val  string
}

type readCheck struct {
	mustSee []string // values acknowledged before the read began
	rs      *ReadState
}

type inflight struct {
	m  Message
	at int
}

type sim struct {
	t       *testing.T
	seed    uint64
	rng     *rand.Rand
	nodes   map[NodeID]*simNode
	ids     []NodeID
	net     []inflight
	now     int
	group   map[NodeID]int // nodes talk only within the same group
	loss    float64
	leaders map[uint64]NodeID
	global  map[uint64]string // index -> fingerprint of the entry applied there
	acked   []string
	removed map[NodeID]bool // shut down for good after leaving the cluster
	nextVal int
	nextCtx uint64
	trace   []string
}

const electionTick, heartbeatTick = 10, 1

func newSim(t *testing.T, seed uint64, n int) *sim {
	s := &sim{t: t, seed: seed, rng: rand.New(rand.NewPCG(seed, 7)), nodes: map[NodeID]*simNode{},
		group: map[NodeID]int{}, removed: map[NodeID]bool{}, leaders: map[uint64]NodeID{}, global: map[uint64]string{}}
	var peers []NodeID
	for i := 1; i <= n; i++ {
		peers = append(peers, NodeID(i))
	}
	for _, id := range peers {
		s.addNode(id, peers)
	}
	return s
}

func (s *sim) addNode(id NodeID, peers []NodeID) {
	nd := &simNode{id: id, peers: peers, proposals: map[uint64]proposal{}, reads: map[uint64]readCheck{}}
	s.nodes[id] = nd
	s.ids = append(s.ids, id)
	s.boot(nd, peers)
}

func (s *sim) boot(nd *simNode, peers []NodeID) {
	cfg := Config{ID: nd.id, Peers: peers, ElectionTick: electionTick, HeartbeatTick: heartbeatTick,
		MaxEntries: 4, Rand: rand.New(rand.NewPCG(s.seed, uint64(nd.id)+uint64(s.now)<<8))}
	nd.r = New(cfg, nd.disk)
	nd.up = true
	nd.sm, nd.appliedIdx = nil, 0
	if sn := nd.disk.Snapshot; sn != nil {
		nd.sm, nd.appliedIdx = decodeSM(sn.Data), sn.Meta.Index
	}
	nd.proposals = map[uint64]proposal{}
	nd.reads = map[uint64]readCheck{}
}

func encodeSM(sm []string) []byte { return []byte(strings.Join(sm, ",")) }

func decodeSM(b []byte) []string {
	if len(b) == 0 {
		return nil
	}
	return strings.Split(string(b), ",")
}

func (s *sim) fatalf(format string, args ...any) {
	s.t.Helper()
	tail := s.trace[max(0, len(s.trace)-40):]
	s.t.Fatalf("seed %d t=%d: %s\nrecent events:\n  %s", s.seed, s.now, fmt.Sprintf(format, args...), strings.Join(tail, "\n  "))
}

func (s *sim) log(format string, args ...any) {
	s.trace = append(s.trace, fmt.Sprintf("t=%d ", s.now)+fmt.Sprintf(format, args...))
}

func (s *sim) connected(a, b NodeID) bool { return s.group[a] == s.group[b] }

// process drains a node's Ready: persist, send, apply — in that order.
func (s *sim) process(nd *simNode) {
	for nd.up && nd.r.HasReady() {
		rd := nd.r.Ready()
		// 1. persist
		if rd.Snapshot != nil {
			nd.disk.Snapshot = rd.Snapshot
			nd.disk.Entries = nil
		}
		if rd.HSChanged {
			nd.disk.HardState = rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			var keep []Entry
			for _, e := range nd.disk.Entries {
				if e.Index < first {
					keep = append(keep, e)
				}
			}
			nd.disk.Entries = append(keep, rd.Entries...)
		}
		// 2. send
		for _, m := range rd.Messages {
			if s.rng.Float64() < s.loss {
				continue
			}
			copies := 1
			if s.rng.Float64() < s.loss/2 {
				copies = 2 // duplicate
			}
			for i := 0; i < copies; i++ {
				s.net = append(s.net, inflight{m: m, at: s.now + 1 + s.rng.IntN(3)})
			}
		}
		// 3. apply
		if rd.Snapshot != nil {
			nd.sm, nd.appliedIdx = decodeSM(rd.Snapshot.Data), rd.Snapshot.Meta.Index
			s.log("n%d installed snapshot @%d", nd.id, nd.appliedIdx)
		}
		for _, e := range rd.Committed {
			s.apply(nd, e)
		}
		for _, rs := range rd.ReadStates {
			if rc, ok := nd.reads[rs.Ctx]; ok {
				rs := rs
				rc.rs = &rs
				nd.reads[rs.Ctx] = rc
			}
		}
		nd.r.Advance(rd)
		s.serveReads(nd)
		s.checkLeader(nd)
	}
}

func (s *sim) apply(nd *simNode, e Entry) {
	if e.Index != nd.appliedIdx+1 {
		s.fatalf("n%d applied index %d after %d", nd.id, e.Index, nd.appliedIdx)
	}
	fp := fmt.Sprintf("t%d/%d/%s", e.Term, e.Type, e.Data)
	if prev, ok := s.global[e.Index]; ok && prev != fp {
		s.fatalf("STATE MACHINE SAFETY: index %d applied as %s on n%d but %s elsewhere", e.Index, fp, nd.id, prev)
	}
	s.global[e.Index] = fp
	nd.appliedIdx = e.Index
	switch e.Type {
	case EntryConfChange:
		cc, err := UnmarshalConfChange(e.Data)
		if err != nil {
			s.fatalf("bad conf change: %v", err)
		}
		nd.r.ApplyConfChange(cc)
		s.log("n%d applied conf %v n%d @%d", nd.id, cc.Type, cc.Node, e.Index)
	case EntryNormal:
		if e.Data != nil {
			nd.sm = append(nd.sm, string(e.Data))
		}
	}
	if p, ok := nd.proposals[e.Index]; ok {
		delete(nd.proposals, e.Index)
		if p.term == e.Term && string(e.Data) == p.val {
			s.acked = append(s.acked, p.val)
		}
	}
}

func (s *sim) serveReads(nd *simNode) {
	for ctx, rc := range nd.reads {
		if rc.rs == nil || nd.appliedIdx < rc.rs.Index {
			continue
		}
		delete(nd.reads, ctx)
		have := map[string]bool{}
		for _, v := range nd.sm {
			have[v] = true
		}
		for _, v := range rc.mustSee {
			if !have[v] {
				s.fatalf("LINEARIZABILITY: read %d on n%d (index %d) misses acknowledged write %s", ctx, nd.id, rc.rs.Index, v)
			}
		}
	}
}

func (s *sim) checkLeader(nd *simNode) {
	if nd.r.State() != StateLeader {
		return
	}
	term := nd.r.Term()
	if l, ok := s.leaders[term]; ok && l != nd.id {
		s.fatalf("ELECTION SAFETY: n%d and n%d both leader in term %d", l, nd.id, term)
	}
	if _, ok := s.leaders[term]; !ok {
		s.log("n%d became leader of term %d", nd.id, term)
	}
	s.leaders[term] = nd.id
}

// step advances virtual time by one tick.
func (s *sim) step() {
	s.now++
	for _, id := range s.ids {
		if nd := s.nodes[id]; nd.up {
			nd.r.Tick()
		}
	}
	var later []inflight
	s.rng.Shuffle(len(s.net), func(i, j int) { s.net[i], s.net[j] = s.net[j], s.net[i] }) // reorder
	for _, f := range s.net {
		if f.at > s.now {
			later = append(later, f)
			continue
		}
		dst := s.nodes[f.m.To]
		if dst == nil || !dst.up || !s.connected(f.m.From, f.m.To) {
			continue
		}
		dst.r.Step(f.m)
	}
	s.net = later
	for _, id := range s.ids {
		s.process(s.nodes[id])
	}
}

func (s *sim) leader() *simNode {
	for _, id := range s.ids {
		if nd := s.nodes[id]; nd.up && nd.r.State() == StateLeader {
			return nd
		}
	}
	return nil
}

func (s *sim) propose(nd *simNode) {
	s.nextVal++
	v := "v" + strconv.Itoa(s.nextVal)
	if idx, term, err := nd.r.Propose([]byte(v)); err == nil {
		nd.proposals[idx] = proposal{term, v}
	}
}

func (s *sim) startRead(nd *simNode) {
	s.nextCtx++
	if nd.r.ReadIndex(s.nextCtx) == nil {
		nd.reads[s.nextCtx] = readCheck{mustSee: slices.Clone(s.acked)}
	}
}

func (s *sim) compact(nd *simNode) {
	if nd.appliedIdx <= nd.r.SnapIndex() {
		return
	}
	data := encodeSM(nd.sm)
	if err := nd.r.Compact(nd.appliedIdx, ConfState{Voters: nd.r.Voters()}, data); err != nil {
		return
	}
	nd.disk.Snapshot = nd.r.snapshot
	var keep []Entry
	for _, e := range nd.disk.Entries {
		if e.Index > nd.appliedIdx {
			keep = append(keep, e)
		}
	}
	nd.disk.Entries = keep
}

func (s *sim) crash(nd *simNode) {
	nd.up = false
	s.log("n%d crashed", nd.id)
}

func (s *sim) restart(nd *simNode) {
	s.boot(nd, nd.peers)
	s.log("n%d restarted (term %d, commit %d)", nd.id, nd.disk.HardState.Term, nd.disk.HardState.Commit)
}

func (s *sim) partition() {
	for _, id := range s.ids {
		s.group[id] = s.rng.IntN(2)
	}
	s.log("partition %v", s.group)
}

func (s *sim) heal() {
	s.group = map[NodeID]int{}
	s.log("healed")
}

// recover heals the network and restarts every crashed node.
func (s *sim) recover() {
	s.heal()
	for _, id := range s.ids {
		if !s.nodes[id].up && !s.removed[id] {
			s.restart(s.nodes[id])
		}
	}
}

// chaos runs random events for the given number of ticks.
func (s *sim) chaos(ticks int) {
	for i := 0; i < ticks; i++ {
		s.step()
		nd := s.nodes[s.ids[s.rng.IntN(len(s.ids))]]
		switch x := s.rng.Float64(); {
		case x < 0.004:
			if s.removed[nd.id] {
				break
			}
			if nd.up {
				s.crash(nd)
			} else {
				s.restart(nd)
			}
		case x < 0.008:
			s.partition()
		case x < 0.014:
			s.heal()
		case x < 0.03:
			if nd.up {
				s.compact(nd)
			}
		case x < 0.034:
			if l := s.leader(); l != nil {
				to := s.ids[s.rng.IntN(len(s.ids))]
				l.r.TransferLeadership(to)
				s.log("n%d transferring leadership to n%d", l.id, to)
			}
		case x < 0.25:
			if l := s.leader(); l != nil {
				s.propose(l)
			}
		case x < 0.30:
			if l := s.leader(); l != nil {
				s.startRead(l)
			}
		}
	}
}

// settle heals everything and checks the cluster converges and stays available.
func (s *sim) settle() {
	s.heal()
	s.loss = 0
	for _, id := range s.ids {
		if !s.nodes[id].up && !s.removed[id] {
			s.restart(s.nodes[id])
		}
	}
	for i := 0; i < 50*electionTick; i++ {
		s.step()
	}
	l := s.leader()
	if l == nil {
		s.fatalf("LIVENESS: no leader after healing")
	}
	s.propose(l)
	final := "v" + strconv.Itoa(s.nextVal)
	for i := 0; i < 20*electionTick; i++ {
		s.step()
	}
	voters := l.r.Voters()
	for _, id := range voters {
		nd := s.nodes[id]
		if nd.appliedIdx != l.appliedIdx {
			s.fatalf("LIVENESS: n%d applied %d, leader n%d applied %d", id, nd.appliedIdx, l.id, l.appliedIdx)
		}
		if !slices.Equal(nd.sm, l.sm) {
			s.fatalf("DIVERGENCE: n%d state machine differs from leader", id)
		}
		have := map[string]bool{}
		for _, v := range nd.sm {
			have[v] = true
		}
		for _, v := range append(slices.Clone(s.acked), final) {
			if !have[v] {
				s.fatalf("DURABILITY: acknowledged write %s missing on n%d", v, id)
			}
		}
	}
}

func seeds(t *testing.T, def int) []uint64 {
	if v := os.Getenv("SEED"); v != "" {
		n, _ := strconv.ParseUint(v, 10, 64)
		return []uint64{n}
	}
	n := def
	if v := os.Getenv("SEEDS"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	if testing.Short() {
		n = min(n, 20)
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i)
	}
	return out
}

func TestSimSafetyAndLiveness(t *testing.T) {
	for _, seed := range seeds(t, 300) {
		for _, n := range []int{3, 5} {
			s := newSim(t, seed, n)
			s.loss = 0.1
			s.chaos(3000)
			s.settle()
			if len(s.acked) == 0 {
				t.Fatalf("seed %d: no writes acknowledged", seed)
			}
		}
	}
}

// TestSimMembership adds a fourth node and removes the original leader while the
// network drops messages, then checks the new configuration converges.
func TestSimMembership(t *testing.T) {
	for _, seed := range seeds(t, 100) {
		s := newSim(t, seed, 3)
		s.loss = 0.05
		s.chaos(300)
		s.recover()
		s.addNode(4, nil) // joins empty; learns config from the leader

		for i := 0; ; i++ {
			if i > 5000 {
				s.fatalf("could not add n4")
			}
			s.step()
			// Retry until applied: an accepted proposal can still be lost if the
			// leader changes before it commits.
			if l := s.leader(); l != nil {
				if slices.Contains(l.r.Voters(), 4) {
					break
				}
				l.r.ProposeConfChange(ConfChange{Type: AddNode, Node: 4})
			}
		}
		s.chaos(500)
		s.recover()
		// Remove the current leader; retry from whichever node leads until applied.
		var removed NodeID
		for i := 0; ; i++ {
			if i > 10000 {
				s.fatalf("removal of n%d never applied", removed)
			}
			s.step()
			l := s.leader()
			if l == nil {
				continue
			}
			if removed == None {
				removed = l.id
			}
			if !slices.Contains(l.r.Voters(), removed) {
				break
			}
			l.r.ProposeConfChange(ConfChange{Type: RemoveNode, Node: removed})
		}
		s.crash(s.nodes[removed])
		s.removed[removed] = true
		s.chaos(500)
		s.recover()
		s.loss = 0
		for i := 0; i < 100*electionTick; i++ {
			s.step()
		}
		l := s.leader()
		if l == nil || l.id == removed {
			s.fatalf("no leader from the new configuration (removed n%d)", removed)
		}
		want := slices.DeleteFunc([]NodeID{1, 2, 3, 4}, func(id NodeID) bool { return id == removed })
		if v := l.r.Voters(); !slices.Equal(v, want) {
			s.fatalf("unexpected voters %v (removed n%d)", v, removed)
		}
		s.settle()
	}
}

func TestSingleNode(t *testing.T) {
	s := newSim(t, 1, 1)
	for i := 0; i < 3*electionTick; i++ {
		s.step()
	}
	l := s.leader()
	if l == nil {
		t.Fatal("single node did not elect itself")
	}
	s.propose(l)
	s.startRead(l)
	s.step()
	if len(s.acked) != 1 || len(l.reads) != 0 {
		t.Fatalf("acked %v, pending reads %d", s.acked, len(l.reads))
	}
}

func TestLeadershipTransfer(t *testing.T) {
	s := newSim(t, 3, 3)
	for s.leader() == nil {
		s.step()
	}
	for i := 0; i < 20; i++ {
		s.propose(s.leader())
		s.step()
	}
	l := s.leader()
	to := NodeID(1)
	if l.id == 1 {
		to = 2
	}
	l.r.TransferLeadership(to)
	if _, _, err := l.r.Propose([]byte("x")); err != ErrTransfering {
		t.Fatalf("proposal during transfer: %v", err)
	}
	for i := 0; i < 5*electionTick; i++ {
		s.step()
		if nl := s.leader(); nl != nil && nl.id == to {
			return
		}
	}
	t.Fatalf("leadership did not move to n%d", to)
}
