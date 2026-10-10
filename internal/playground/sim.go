// Package playground runs a whole Raft cluster in one process on a virtual clock,
// using the production raft package unchanged. It powers the in-browser playground
// (compiled to WebAssembly) and is deterministic: the same seed and the same user
// actions replay the same run.
package playground

import (
	"fmt"
	"math/rand/v2"

	"github.com/tzar-beep/redis/internal/raft"
)

const (
	ElectionTick  = 16
	HeartbeatTick = 4
	minLatency    = 3 // ticks
	maxLatency    = 6
	logWindow     = 24
)

type node struct {
	id         raft.NodeID
	r          *raft.Raft
	up         bool
	isolated   bool
	disk       raft.Persisted
	peers      []raft.NodeID
	appliedIdx uint64
}

type inflight struct {
	m    raft.Message
	sent int
	due  int
}

// Event is a human-readable happening, for the event feed.
type Event struct {
	Tick int    `json:"tick"`
	Kind string `json:"kind"` // leader, crash, restart, partition, heal, commit, write, lost
	Text string `json:"text"`
}

type Sim struct {
	Seed    uint64
	tick    int
	rng     *rand.Rand
	nodes   []*node
	net     []inflight
	loss    float64
	events  []Event
	leaders map[uint64]raft.NodeID
	nextVal int
}

func New(n int, seed uint64) *Sim {
	s := &Sim{Seed: seed, rng: rand.New(rand.NewPCG(seed, 0x4a17)), leaders: map[uint64]raft.NodeID{}}
	var peers []raft.NodeID
	for i := 1; i <= n; i++ {
		peers = append(peers, raft.NodeID(i))
	}
	for _, id := range peers {
		nd := &node{id: id, peers: peers}
		s.nodes = append(s.nodes, nd)
		s.boot(nd)
	}
	return s
}

func (s *Sim) boot(nd *node) {
	nd.r = raft.New(raft.Config{ID: nd.id, Peers: nd.peers, ElectionTick: ElectionTick, HeartbeatTick: HeartbeatTick,
		MaxEntries: 8, Rand: rand.New(rand.NewPCG(s.Seed, uint64(nd.id)<<16|uint64(s.tick)))}, nd.disk)
	nd.up = true
	nd.appliedIdx = 0
	if sn := nd.disk.Snapshot; sn != nil {
		nd.appliedIdx = sn.Meta.Index
	}
}

func (s *Sim) emit(kind, format string, a ...any) {
	s.events = append(s.events, Event{Tick: s.tick, Kind: kind, Text: fmt.Sprintf(format, a...)})
}

func (s *Sim) node(id int) *node {
	if id < 1 || id > len(s.nodes) {
		return nil
	}
	return s.nodes[id-1]
}

func (s *Sim) reachable(a, b raft.NodeID) bool {
	return !s.nodes[a-1].isolated && !s.nodes[b-1].isolated
}

// Step advances the cluster by one tick.
func (s *Sim) Step() {
	s.tick++
	for _, nd := range s.nodes {
		if nd.up {
			nd.r.Tick()
		}
	}
	var later []inflight
	for _, f := range s.net {
		if f.due > s.tick {
			later = append(later, f)
			continue
		}
		dst := s.nodes[f.m.To-1]
		if dst.up && s.reachable(f.m.From, f.m.To) {
			dst.r.Step(f.m)
		}
	}
	s.net = later
	for _, nd := range s.nodes {
		s.process(nd)
	}
}

func (s *Sim) process(nd *node) {
	for nd.up && nd.r.HasReady() {
		rd := nd.r.Ready()
		if rd.Snapshot != nil {
			nd.disk.Snapshot, nd.disk.Entries = rd.Snapshot, nil
		}
		if rd.HSChanged {
			nd.disk.HardState = rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			keep := nd.disk.Entries[:0:0]
			for _, e := range nd.disk.Entries {
				if e.Index < first {
					keep = append(keep, e)
				}
			}
			nd.disk.Entries = append(keep, rd.Entries...)
		}
		for _, m := range rd.Messages {
			if s.loss > 0 && s.rng.Float64() < s.loss {
				continue
			}
			lat := minLatency + s.rng.IntN(maxLatency-minLatency+1)
			s.net = append(s.net, inflight{m: m, sent: s.tick, due: s.tick + lat})
		}
		for _, e := range rd.Committed {
			nd.appliedIdx = e.Index
			if e.Type == raft.EntryNormal && e.Data != nil && nd.r.State() == raft.StateLeader {
				s.emit("commit", "%q committed at index %d", e.Data, e.Index)
			}
		}
		nd.r.Advance(rd)
		if nd.r.State() == raft.StateLeader {
			if _, seen := s.leaders[nd.r.Term()]; !seen {
				s.leaders[nd.r.Term()] = nd.id
				s.emit("leader", "n%d elected leader for term %d", nd.id, nd.r.Term())
			}
		}
	}
}

// Leader returns the id of the up leader with the highest term, or 0.
func (s *Sim) Leader() int {
	best, term := 0, uint64(0)
	for _, nd := range s.nodes {
		if nd.up && nd.r.State() == raft.StateLeader && nd.r.Term() >= term {
			best, term = int(nd.id), nd.r.Term()
		}
	}
	return best
}

// Propose submits a client write to the leader. Returns false if there is none.
func (s *Sim) Propose(value string) bool {
	l := s.node(s.Leader())
	if l == nil {
		s.emit("lost", "write rejected: no leader")
		return false
	}
	if value == "" {
		s.nextVal++
		value = fmt.Sprintf("x=%d", s.nextVal)
	}
	if _, _, err := l.r.Propose([]byte(value)); err != nil {
		s.emit("lost", "write rejected by n%d: %v", l.id, err)
		return false
	}
	s.emit("write", "client wrote %q via n%d", value, l.id)
	s.process(l)
	return true
}

// Crash stops a node; its unsynced state is lost, its disk survives.
func (s *Sim) Crash(id int) {
	if nd := s.node(id); nd != nil && nd.up {
		nd.up = false
		s.emit("crash", "n%d crashed", id)
	}
}

func (s *Sim) Restart(id int) {
	if nd := s.node(id); nd != nil && !nd.up {
		s.boot(nd)
		s.emit("restart", "n%d restarted from disk (term %d)", id, nd.disk.HardState.Term)
	}
}

// Isolate toggles a network partition around a node.
func (s *Sim) Isolate(id int) {
	if nd := s.node(id); nd != nil {
		nd.isolated = !nd.isolated
		if nd.isolated {
			s.emit("partition", "n%d cut off from the network", id)
		} else {
			s.emit("heal", "n%d reconnected", id)
		}
	}
}

func (s *Sim) SetLoss(p float64) { s.loss = min(max(p, 0), 0.9) }

// --- view model ---

type LogEntry struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Data  string `json:"d"`
}

type NodeView struct {
	ID       int        `json:"id"`
	Up       bool       `json:"up"`
	Isolated bool       `json:"isolated"`
	State    string     `json:"state"`
	Term     uint64     `json:"term"`
	Vote     int        `json:"vote"`
	Lead     int        `json:"lead"`
	Commit   uint64     `json:"commit"`
	Applied  uint64     `json:"applied"`
	Last     uint64     `json:"last"`
	Elapsed  int        `json:"elapsed"`
	Timeout  int        `json:"timeout"`
	Log      []LogEntry `json:"log"`
}

type MsgView struct {
	From int    `json:"from"`
	To   int    `json:"to"`
	Type string `json:"type"`
	Sent int    `json:"sent"`
	Due  int    `json:"due"`
	Fail bool   `json:"fail"` // will be dropped (partition or dead receiver)
}

type View struct {
	Tick   int        `json:"tick"`
	Seed   uint64     `json:"seed"`
	Leader int        `json:"leader"`
	Nodes  []NodeView `json:"nodes"`
	Msgs   []MsgView  `json:"msgs"`
	Events []Event    `json:"events"`
}

// View snapshots the cluster and drains pending events.
func (s *Sim) View() View {
	v := View{Tick: s.tick, Seed: s.Seed, Leader: s.Leader(), Events: s.events}
	s.events = nil
	for _, nd := range s.nodes {
		el, to := nd.r.Timers()
		nv := NodeView{ID: int(nd.id), Up: nd.up, Isolated: nd.isolated, State: nd.r.State().String(),
			Term: nd.r.Term(), Lead: int(nd.r.Leader()), Commit: nd.r.Committed(), Applied: nd.appliedIdx,
			Last: nd.r.LastIndex(), Elapsed: el, Timeout: to, Vote: int(nd.disk.HardState.Vote)}
		if !nd.up {
			nv.State = "down"
		}
		for _, e := range nd.r.RecentEntries(logWindow) {
			d := string(e.Data)
			switch {
			case e.Type == raft.EntryConfChange:
				d = "config"
			case e.Data == nil:
				d = "no-op"
			}
			nv.Log = append(nv.Log, LogEntry{e.Index, e.Term, d})
		}
		v.Nodes = append(v.Nodes, nv)
	}
	for _, f := range s.net {
		v.Msgs = append(v.Msgs, MsgView{From: int(f.m.From), To: int(f.m.To), Type: f.m.Type.String(),
			Sent: f.sent, Due: f.due, Fail: !s.reachable(f.m.From, f.m.To) || !s.nodes[f.m.To-1].up})
	}
	return v
}
