// Package cluster runs a Raft node: it owns the consensus core, its WAL and the
// network transport, and applies committed commands to the state machine.
//
// One goroutine (run) owns the raft.Raft. Clients talk to it over channels:
//
//	write:  Propose -> leader appends -> replicated -> committed -> applied on every
//	        node -> the proposer's waiter receives the reply produced by apply
//	read:   ReadBarrier -> ReadIndex quorum check -> wait until applied >= index
//	        -> caller reads local state (linearizable)
//
// Each Ready is handled in the order Raft requires: persist (one fsync for the
// whole batch), then send, then apply.
//
// Determinism: entries carry a leader-assigned timestamp, so replicas evaluate
// TTLs identically. Expired keys are removed by DELEXPIRED entries the leader
// proposes, never by local timers.
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tzar-beep/redis/internal/raft"
	"github.com/tzar-beep/redis/internal/raft/wal"
)

// Peer describes a cluster member.
type Peer struct {
	ID         raft.NodeID
	RaftAddr   string
	ClientAddr string
}

func (p Peer) context() []byte { return []byte(p.RaftAddr + "|" + p.ClientAddr) }

func parseContext(id raft.NodeID, b []byte) Peer {
	raftAddr, clientAddr, _ := strings.Cut(string(b), "|")
	return Peer{ID: id, RaftAddr: raftAddr, ClientAddr: clientAddr}
}

type Config struct {
	Self  Peer
	Peers []Peer // founding members including Self; empty when joining a cluster
	Dir   string

	Tick            time.Duration // default 100ms
	ElectionTicks   int           // default 10
	SnapshotEntries uint64        // compact the log after this many applied entries (default 10000)
	ProposeTimeout  time.Duration // default 5s
	Secret          string        // shared secret for node-to-node connections
}

// StateMachine is what the node replicates.
type StateMachine interface {
	ApplyCommand(now int64, args [][]byte) []byte
	Snapshot() []byte
	Restore(data []byte) error
	ExpiredKeys(now int64, n int) []string
}

type result struct {
	reply []byte
	err   error
}

type proposal struct {
	args  [][]byte
	conf  *raft.ConfChange
	index uint64
	term  uint64
	done  chan result
}

type readReq struct {
	index uint64
	done  chan error
}

// Status is a point-in-time view of the node for clients and tests.
type Status struct {
	ID        raft.NodeID
	State     raft.StateType
	Term      uint64
	Leader    raft.NodeID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	SnapIndex uint64
	Voters    []raft.NodeID
	Peers     map[raft.NodeID]Peer
}

type Node struct {
	cfg Config
	sm  StateMachine
	r   *raft.Raft
	wal *wal.WAL
	tr  *transport

	propc chan *proposal
	readc chan *readReq
	recvc chan raft.Message
	stopc chan struct{}
	donec chan struct{}

	// Owned by the run goroutine.
	waiters   map[uint64]*proposal
	reads     map[uint64]*readReq
	readyRead []*readReq
	nextCtx   uint64
	lastStamp int64
	peers     map[raft.NodeID]Peer
	ticks     int

	mu     sync.RWMutex
	status Status
}

var (
	ErrStopped = errors.New("ERR node is shutting down")
	errLost    = errors.New("ERR write not applied: leadership changed before it committed; retry")
	errTimeout = errors.New("ERR timeout waiting for consensus; the write may or may not have been applied")
)

// Start recovers the node from its WAL and starts it.
func Start(cfg Config, sm StateMachine) (*Node, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 100 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = 10000
	}
	if cfg.ProposeTimeout == 0 {
		cfg.ProposeTimeout = 5 * time.Second
	}
	w, persisted, err := wal.Open(cfg.Dir)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg: cfg, sm: sm, wal: w,
		propc: make(chan *proposal, 1024), readc: make(chan *readReq, 1024),
		recvc: make(chan raft.Message, 4096),
		stopc: make(chan struct{}), donec: make(chan struct{}),
		waiters: map[uint64]*proposal{}, reads: map[uint64]*readReq{},
		peers: map[raft.NodeID]Peer{},
	}
	if s := persisted.Snapshot; s != nil {
		if err := n.restoreSnapshot(s); err != nil {
			w.Close()
			return nil, err
		}
	}
	rc := raft.Config{ID: cfg.Self.ID, ElectionTick: cfg.ElectionTicks, HeartbeatTick: 1,
		MaxEntries: 256, PeerContexts: map[raft.NodeID][]byte{}}
	for _, p := range cfg.Peers {
		rc.Peers = append(rc.Peers, p.ID)
		rc.PeerContexts[p.ID] = p.context()
	}
	n.r = raft.New(rc, persisted)
	n.tr, err = newTransport(cfg.Self.ID, cfg.Self.RaftAddr, cfg.Secret, n.receive)
	if err != nil {
		w.Close()
		return nil, err
	}
	for id, p := range n.peers {
		n.tr.setPeer(id, p.RaftAddr)
	}
	n.publishStatus()
	go n.run()
	return n, nil
}

// SetFilter installs a fault-injection filter on the network (tests).
func (n *Node) SetFilter(f func(from, to raft.NodeID) bool) {
	if f == nil {
		n.tr.filter.Store(nil)
	} else {
		n.tr.filter.Store(&f)
	}
}

func (n *Node) receive(m raft.Message) {
	select {
	case n.recvc <- m:
	default: // overloaded: drop; Raft retransmits
	}
}

// Stop shuts the node down and closes its WAL.
func (n *Node) Stop() {
	select {
	case <-n.stopc:
		return
	default:
		close(n.stopc)
	}
	<-n.donec
	n.tr.close()
	n.wal.Close()
}

func (n *Node) run() {
	defer close(n.donec)
	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopc:
			n.failAll(ErrStopped)
			return
		case <-ticker.C:
			n.r.Tick()
			n.ticks++
			if n.ticks%10 == 0 && n.r.State() == raft.StateLeader {
				n.proposeExpiry()
			}
		case m := <-n.recvc:
			n.r.Step(m)
		case p := <-n.propc:
			n.startProposal(p)
		case rq := <-n.readc:
			n.nextCtx++
			if err := n.r.ReadIndex(n.nextCtx); err != nil {
				rq.done <- n.redirect()
			} else {
				n.reads[n.nextCtx] = rq
			}
		}
		for n.r.HasReady() {
			if err := n.handleReady(n.r.Ready()); err != nil {
				// Losing durability is not survivable: stop rather than risk
				// acknowledging writes that are not on disk.
				log.Printf("raft node %d: fatal storage error: %v", n.cfg.Self.ID, err)
				n.failAll(fmt.Errorf("ERR storage failure: %v", err))
				return
			}
		}
		if n.r.State() != raft.StateLeader && len(n.reads) > 0 {
			for ctx, rq := range n.reads {
				rq.done <- n.redirect()
				delete(n.reads, ctx)
			}
		}
		n.publishStatus()
	}
}

func (n *Node) startProposal(p *proposal) {
	var idx, term uint64
	var err error
	if p.conf != nil {
		idx, term, err = n.r.ProposeConfChange(*p.conf)
	} else {
		n.lastStamp = max(n.lastStamp, time.Now().UnixMilli())
		idx, term, err = n.r.Propose(encodeCommand(n.lastStamp, p.args))
	}
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		p.done <- result{err: n.redirect()}
	case err != nil:
		p.done <- result{err: fmt.Errorf("ERR %v", err)}
	default:
		p.index, p.term = idx, term
		n.waiters[idx] = p
	}
}

func (n *Node) proposeExpiry() {
	keys := n.sm.ExpiredKeys(time.Now().UnixMilli(), 64)
	if len(keys) == 0 {
		return
	}
	args := [][]byte{[]byte("DELEXPIRED")}
	for _, k := range keys {
		args = append(args, []byte(k))
	}
	n.startProposal(&proposal{args: args, done: make(chan result, 1)})
}

func (n *Node) handleReady(rd raft.Ready) error {
	var hs *raft.HardState
	if rd.HSChanged {
		hs = &rd.HardState
	}
	if err := n.wal.Save(hs, rd.Entries, rd.Snapshot); err != nil {
		return err
	}
	n.tr.send(rd.Messages)
	if rd.Snapshot != nil {
		if err := n.restoreSnapshot(rd.Snapshot); err != nil {
			return err
		}
		log.Printf("raft node %d: installed snapshot at index %d", n.cfg.Self.ID, rd.Snapshot.Meta.Index)
	}
	for _, e := range rd.Committed {
		n.apply(e)
	}
	n.r.Advance(rd)
	for _, rs := range rd.ReadStates {
		if rq, ok := n.reads[rs.Ctx]; ok {
			delete(n.reads, rs.Ctx)
			rq.index = rs.Index
			n.readyRead = append(n.readyRead, rq)
		}
	}
	applied := n.r.Applied()
	keep := n.readyRead[:0]
	for _, rq := range n.readyRead {
		if rq.index <= applied {
			rq.done <- nil
		} else {
			keep = append(keep, rq)
		}
	}
	n.readyRead = keep
	return n.maybeCompact()
}

func (n *Node) apply(e raft.Entry) {
	var res result
	switch {
	case e.Type == raft.EntryConfChange:
		cc, err := raft.UnmarshalConfChange(e.Data)
		if err != nil {
			log.Printf("raft node %d: bad conf change at %d: %v", n.cfg.Self.ID, e.Index, err)
			break
		}
		n.r.ApplyConfChange(cc)
		switch cc.Type {
		case raft.AddNode:
			p := parseContext(cc.Node, cc.Context)
			n.peers[cc.Node] = p
			n.tr.setPeer(cc.Node, p.RaftAddr)
		case raft.RemoveNode:
			delete(n.peers, cc.Node)
			n.tr.removePeer(cc.Node)
			if cc.Node == n.cfg.Self.ID {
				log.Printf("raft node %d: removed from the cluster", n.cfg.Self.ID)
			}
		}
		res.reply = []byte("+OK\r\n")
	case e.Data != nil:
		stamp, args, err := decodeCommand(e.Data)
		if err != nil {
			log.Printf("raft node %d: bad entry at %d: %v", n.cfg.Self.ID, e.Index, err)
			break
		}
		// Clamp to keep time monotonic across leader changes; every replica
		// computes the same value.
		n.lastStamp = max(n.lastStamp, stamp)
		res.reply = n.sm.ApplyCommand(n.lastStamp, args)
	}
	if p, ok := n.waiters[e.Index]; ok {
		delete(n.waiters, e.Index)
		if p.term != e.Term {
			res = result{err: errLost}
		}
		p.done <- res
	}
}

func (n *Node) maybeCompact() error {
	applied := n.r.Applied()
	if applied-n.r.SnapIndex() < n.cfg.SnapshotEntries {
		return nil
	}
	data := encodeNodeSnapshot(n.peers, n.lastStamp, n.sm.Snapshot())
	if err := n.r.Compact(applied, raft.ConfState{Voters: n.r.Voters()}, data); err != nil {
		return nil
	}
	return n.wal.Compact(n.r.LatestSnapshot())
}

func (n *Node) restoreSnapshot(s *raft.Snapshot) error {
	peers, stamp, smData, err := decodeNodeSnapshot(s.Data)
	if err != nil {
		return err
	}
	if err := n.sm.Restore(smData); err != nil {
		return err
	}
	n.lastStamp = max(n.lastStamp, stamp)
	for id := range n.peers {
		if _, ok := peers[id]; !ok && n.tr != nil {
			n.tr.removePeer(id)
		}
	}
	n.peers = peers
	if n.tr != nil {
		for id, p := range peers {
			n.tr.setPeer(id, p.RaftAddr)
		}
	}
	return nil
}

func (n *Node) failAll(err error) {
	for idx, p := range n.waiters {
		p.done <- result{err: err}
		delete(n.waiters, idx)
	}
	for ctx, rq := range n.reads {
		rq.done <- err
		delete(n.reads, ctx)
	}
	for _, rq := range n.readyRead {
		rq.done <- err
	}
	n.readyRead = nil
}

func (n *Node) publishStatus() {
	st := Status{
		ID: n.cfg.Self.ID, State: n.r.State(), Term: n.r.Term(), Leader: n.r.Leader(),
		Commit: n.r.Committed(), Applied: n.r.Applied(), LastIndex: n.r.LastIndex(),
		SnapIndex: n.r.SnapIndex(), Voters: n.r.Voters(), Peers: map[raft.NodeID]Peer{},
	}
	for id, p := range n.peers {
		st.Peers[id] = p
	}
	n.mu.Lock()
	n.status = st
	n.mu.Unlock()
}

// CurrentStatus returns the latest published status.
func (n *Node) CurrentStatus() Status {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.status
}

// redirect builds the error a client gets from a non-leader. MOVED is understood by
// cluster-aware clients (redis-cli -c follows it).
func (n *Node) redirect() error {
	st := n.CurrentStatus()
	if p, ok := st.Peers[st.Leader]; ok && st.Leader != raft.None && p.ClientAddr != "" {
		return fmt.Errorf("MOVED 0 %s", p.ClientAddr)
	}
	return errors.New("CLUSTERDOWN The cluster is down")
}

// --- command.Replicator ---

func (n *Node) submit(p *proposal) ([]byte, error) {
	if st := n.CurrentStatus(); st.State != raft.StateLeader {
		return nil, n.redirect()
	}
	p.done = make(chan result, 1)
	timer := time.NewTimer(n.cfg.ProposeTimeout)
	defer timer.Stop()
	select {
	case n.propc <- p:
	case <-n.stopc:
		return nil, ErrStopped
	case <-timer.C:
		return nil, errTimeout
	}
	select {
	case r := <-p.done:
		return r.reply, r.err
	case <-timer.C:
		return nil, errTimeout
	}
}

func (n *Node) Propose(args [][]byte) ([]byte, error) {
	return n.submit(&proposal{args: args})
}

func (n *Node) ReadBarrier() error {
	if st := n.CurrentStatus(); st.State != raft.StateLeader {
		return n.redirect()
	}
	rq := &readReq{done: make(chan error, 1)}
	timer := time.NewTimer(n.cfg.ProposeTimeout)
	defer timer.Stop()
	select {
	case n.readc <- rq:
	case <-n.stopc:
		return ErrStopped
	}
	select {
	case err := <-rq.done:
		return err
	case <-timer.C:
		return errTimeout
	}
}

func (n *Node) AddNode(id uint64, raftAddr, clientAddr string) error {
	p := Peer{ID: raft.NodeID(id), RaftAddr: raftAddr, ClientAddr: clientAddr}
	_, err := n.submit(&proposal{conf: &raft.ConfChange{Type: raft.AddNode, Node: p.ID, Context: p.context()}})
	return err
}

func (n *Node) RemoveNode(id uint64) error {
	_, err := n.submit(&proposal{conf: &raft.ConfChange{Type: raft.RemoveNode, Node: raft.NodeID(id)}})
	return err
}

func (n *Node) Status() string {
	st := n.CurrentStatus()
	var b strings.Builder
	fmt.Fprintf(&b, "id:%d\nstate:%s\nterm:%d\nleader:%d\ncommit:%d\napplied:%d\nlast_index:%d\nsnapshot_index:%d\n",
		st.ID, st.State, st.Term, st.Leader, st.Commit, st.Applied, st.LastIndex, st.SnapIndex)
	ids := make([]raft.NodeID, 0, len(st.Peers))
	for id := range st.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	voters := map[raft.NodeID]bool{}
	for _, v := range st.Voters {
		voters[v] = true
	}
	for _, id := range ids {
		p := st.Peers[id]
		fmt.Fprintf(&b, "node:%d raft=%s client=%s voter=%v\n", id, p.RaftAddr, p.ClientAddr, voters[id])
	}
	return b.String()
}

// --- encodings ---

// encodeCommand: [i64 stamp][uvarint argc]{[uvarint len][bytes]}
func encodeCommand(stamp int64, args [][]byte) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(stamp))
	b = binary.AppendUvarint(b, uint64(len(args)))
	for _, a := range args {
		b = binary.AppendUvarint(b, uint64(len(a)))
		b = append(b, a...)
	}
	return b
}

var errBadEntry = errors.New("malformed command entry")

func decodeCommand(b []byte) (int64, [][]byte, error) {
	if len(b) < 8 {
		return 0, nil, errBadEntry
	}
	stamp := int64(binary.BigEndian.Uint64(b))
	b = b[8:]
	argc, n := binary.Uvarint(b)
	if n <= 0 || argc == 0 || argc > uint64(len(b)) {
		return 0, nil, errBadEntry
	}
	b = b[n:]
	args := make([][]byte, 0, argc)
	for i := uint64(0); i < argc; i++ {
		l, n := binary.Uvarint(b)
		if n <= 0 || l > uint64(len(b)-n) {
			return 0, nil, errBadEntry
		}
		args = append(args, b[n:n+int(l)])
		b = b[n+int(l):]
	}
	if len(b) != 0 {
		return 0, nil, errBadEntry
	}
	return stamp, args, nil
}

// Node snapshot: [uvarint npeers]{[u64 id][uvarint len][context]}[i64 stamp][state machine]
func encodeNodeSnapshot(peers map[raft.NodeID]Peer, stamp int64, sm []byte) []byte {
	ids := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	b := binary.AppendUvarint(nil, uint64(len(ids)))
	for _, id := range ids {
		ctx := peers[id].context()
		b = binary.BigEndian.AppendUint64(b, uint64(id))
		b = binary.AppendUvarint(b, uint64(len(ctx)))
		b = append(b, ctx...)
	}
	b = binary.BigEndian.AppendUint64(b, uint64(stamp))
	return append(b, sm...)
}

func decodeNodeSnapshot(b []byte) (map[raft.NodeID]Peer, int64, []byte, error) {
	bad := errors.New("malformed node snapshot")
	cnt, n := binary.Uvarint(b)
	if n <= 0 || cnt > uint64(len(b)) {
		return nil, 0, nil, bad
	}
	b = b[n:]
	peers := map[raft.NodeID]Peer{}
	for i := uint64(0); i < cnt; i++ {
		if len(b) < 8 {
			return nil, 0, nil, bad
		}
		id := raft.NodeID(binary.BigEndian.Uint64(b))
		b = b[8:]
		l, n := binary.Uvarint(b)
		if n <= 0 || l > uint64(len(b)-n) {
			return nil, 0, nil, bad
		}
		peers[id] = parseContext(id, b[n:n+int(l)])
		b = b[n+int(l):]
	}
	if len(b) < 8 {
		return nil, 0, nil, bad
	}
	return peers, int64(binary.BigEndian.Uint64(b)), b[8:], nil
}
