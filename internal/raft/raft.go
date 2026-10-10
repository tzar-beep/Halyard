package raft

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
)

type Config struct {
	ID NodeID
	// Peers are the founding voters (including ID), used only when the node has no
	// persisted state. Empty for a node that will be added to an existing cluster.
	Peers []NodeID
	// PeerContexts are stored in the bootstrap conf-change entries (e.g. addresses),
	// so every node, including ones that join later, learns them from the log.
	PeerContexts  map[NodeID][]byte
	ElectionTick  int // ticks without a leader before campaigning (randomized ×[1,2))
	HeartbeatTick int
	MaxEntries    int // max entries per MsgApp
	Rand          *rand.Rand
}

// Persisted is the durable state a node restarts from.
type Persisted struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry // entries after the snapshot
}

type progressState uint8

const (
	probe progressState = iota
	replicate
	snapshotting
)

// progress is the leader's view of one follower.
type progress struct {
	match, next  uint64
	state        progressState
	paused       bool   // probe: one MsgApp in flight
	pendingSnap  uint64 // snapshotting: index of the snapshot sent
	snapWait     int    // heartbeats answered while waiting for the snapshot
	recentActive bool
}

func (pr *progress) isPaused() bool {
	switch pr.state {
	case probe:
		return pr.paused
	case snapshotting:
		return true
	}
	return false
}

func (pr *progress) maybeUpdate(n uint64) bool {
	updated := false
	if pr.match < n {
		pr.match, updated = n, true
		pr.paused = false
	}
	pr.next = max(pr.next, n+1)
	return updated
}

// maybeDecrTo handles a rejection of index `rejected`; returns false if stale.
func (pr *progress) maybeDecrTo(rejected, hint uint64) bool {
	if pr.state == replicate {
		if rejected <= pr.match {
			return false
		}
		pr.next = pr.match + 1
		return true
	}
	if pr.next-1 != rejected {
		return false
	}
	pr.next = max(min(rejected, hint+1), pr.match+1)
	pr.paused = false
	return true
}

func (pr *progress) become(s progressState, last uint64) {
	pr.state, pr.paused = s, false
	switch s {
	case probe:
		pr.next = pr.match + 1
		if pr.pendingSnap > 0 {
			pr.next = max(pr.match, pr.pendingSnap) + 1
			pr.pendingSnap = 0
		}
	case replicate:
		pr.next = pr.match + 1
	case snapshotting:
		pr.pendingSnap, pr.snapWait = last, 0
	}
}

type readReq struct {
	index uint64
	ctx   uint64
	acks  map[NodeID]bool
}

type Raft struct {
	id    NodeID
	term  uint64
	vote  NodeID
	state StateType
	lead  NodeID

	log      *raftLog
	voters   map[NodeID]bool
	prs      map[NodeID]*progress
	votes    map[NodeID]bool
	snapshot *Snapshot // latest local snapshot, sent to lagging followers

	electionElapsed, heartbeatElapsed int
	electionTimeout, heartbeatTimeout int
	randomizedTimeout                 int
	rand                              *rand.Rand
	maxEntries                        int

	pendingConfIndex uint64
	leadTransferee   NodeID
	pendingReads     []readReq
	readsAwaitCommit []uint64 // reads received before the leader committed in its term

	msgs        []Message
	readStates  []ReadState
	pendingSnap *Snapshot
	prevHS      HardState
}

func New(c Config, p Persisted) *Raft {
	if c.ElectionTick <= c.HeartbeatTick || c.HeartbeatTick <= 0 {
		panic("raft: ElectionTick must exceed HeartbeatTick > 0")
	}
	r := &Raft{
		id:               c.ID,
		log:              &raftLog{},
		voters:           map[NodeID]bool{},
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		rand:             c.Rand,
		maxEntries:       max(c.MaxEntries, 1),
	}
	if r.rand == nil {
		r.rand = rand.New(rand.NewPCG(uint64(c.ID), 0))
	}
	fresh := p.Snapshot == nil && len(p.Entries) == 0 && p.HardState == (HardState{})
	if s := p.Snapshot; s != nil && s.Meta.Index > 0 {
		r.log.restore(s.Meta)
		r.snapshot = s
		for _, v := range s.Meta.Conf.Voters {
			r.voters[v] = true
		}
	}
	r.log.ents = append([]Entry(nil), p.Entries...)
	r.log.committed = max(p.HardState.Commit, r.log.snapIndex)
	r.log.applied = r.log.snapIndex
	r.log.unstable = r.log.lastIndex() + 1
	r.term, r.vote = p.HardState.Term, p.HardState.Vote
	r.prevHS = r.hardState()

	if fresh && len(c.Peers) > 0 {
		// Bootstrap: the initial membership is written into the log as committed
		// conf changes (identical on every founding node), so nodes that join later
		// learn it by replication rather than from local flags.
		ents := make([]Entry, len(c.Peers))
		for i, id := range c.Peers {
			ents[i] = Entry{Term: 1, Index: uint64(i + 1), Type: EntryConfChange,
				Data: ConfChange{Type: AddNode, Node: id, Context: c.PeerContexts[id]}.Marshal()}
		}
		r.log.ents, r.log.unstable = ents, 1
		r.log.committed = uint64(len(ents))
		r.term = 1
	}
	// Membership = snapshot conf + committed conf changes. They are applied again
	// when replayed through Ready.Committed, which is idempotent.
	for _, e := range r.log.ents {
		if e.Index > r.log.committed {
			break
		}
		if e.Type == EntryConfChange {
			if cc, err := UnmarshalConfChange(e.Data); err == nil {
				if cc.Type == AddNode {
					r.voters[cc.Node] = true
				} else {
					delete(r.voters, cc.Node)
				}
			}
		}
	}
	r.becomeFollower(r.term, None)
	return r
}

// --- accessors ---

func (r *Raft) ID() NodeID        { return r.id }
func (r *Raft) Term() uint64      { return r.term }
func (r *Raft) State() StateType  { return r.state }
func (r *Raft) Leader() NodeID    { return r.lead }
func (r *Raft) Committed() uint64 { return r.log.committed }
func (r *Raft) Applied() uint64   { return r.log.applied }
func (r *Raft) LastIndex() uint64 { return r.log.lastIndex() }
func (r *Raft) SnapIndex() uint64 { return r.log.snapIndex }
func (r *Raft) Voters() []NodeID  { return sortedIDs(r.voters) }

// LatestSnapshot returns the most recent snapshot (local or installed), or nil.
func (r *Raft) LatestSnapshot() *Snapshot { return r.snapshot }
func (r *Raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func sortedIDs(m map[NodeID]bool) []NodeID {
	ids := make([]NodeID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (r *Raft) quorum() int { return len(r.voters)/2 + 1 }

// promotable: only voters that are not waiting to install a snapshot may campaign.
func (r *Raft) promotable() bool { return r.voters[r.id] && r.pendingSnap == nil }

// --- role transitions ---

func (r *Raft) reset(term uint64) {
	if term != r.term {
		r.term, r.vote = term, None
	}
	r.lead = None
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	r.randomizedTimeout = r.electionTimeout + r.rand.IntN(r.electionTimeout)
	r.votes = map[NodeID]bool{}
	r.prs = nil
	r.leadTransferee = None
	r.pendingReads, r.readsAwaitCommit = nil, nil
}

func (r *Raft) becomeFollower(term uint64, lead NodeID) {
	r.reset(term)
	r.state, r.lead = StateFollower, lead
}

func (r *Raft) becomePreCandidate() {
	// PreVote does not change term or vote, so a partitioned node rejoining cannot
	// disrupt a healthy leader by inflating terms.
	r.state, r.lead = StatePreCandidate, None
	r.votes = map[NodeID]bool{}
}

func (r *Raft) becomeCandidate() {
	r.reset(r.term + 1)
	r.vote, r.state = r.id, StateCandidate
}

func (r *Raft) becomeLeader() {
	r.reset(r.term)
	r.state, r.lead = StateLeader, r.id
	r.prs = map[NodeID]*progress{}
	last := r.log.lastIndex()
	for id := range r.voters {
		r.prs[id] = &progress{next: last + 1, recentActive: true}
	}
	r.prs[r.id].match = last
	r.prs[r.id].state = replicate
	// A conf change could be uncommitted in the log; be conservative.
	r.pendingConfIndex = last
	// Commit an entry from this term so earlier entries become committed (§5.4.2)
	// and so ReadIndex can be served.
	r.appendEntries(Entry{Type: EntryNormal})
}

// --- clock ---

func (r *Raft) Tick() {
	if r.state == StateLeader {
		r.tickLeader()
		return
	}
	r.electionElapsed++
	if r.promotable() && r.electionElapsed >= r.randomizedTimeout {
		r.electionElapsed = 0
		r.campaign(true)
	}
}

func (r *Raft) tickLeader() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// CheckQuorum: a leader that cannot reach a majority steps down, so a
		// partitioned leader stops accepting reads/writes it cannot commit.
		if !r.quorumActive() {
			r.becomeFollower(r.term, None)
			return
		}
		r.leadTransferee = None // a transfer that did not finish in time is abandoned
		for id, pr := range r.prs {
			if id != r.id {
				pr.recentActive = false
			}
		}
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat(0)
	}
}

func (r *Raft) quorumActive() bool {
	n := 0
	for id := range r.voters {
		if id == r.id || (r.prs[id] != nil && r.prs[id].recentActive) {
			n++
		}
	}
	return n >= r.quorum()
}

// Campaign starts an election immediately (used by tests and leadership tooling).
func (r *Raft) Campaign() {
	if r.state != StateLeader && r.promotable() {
		r.campaign(true)
	}
}

// transferCtx marks votes requested because of a leadership transfer; voters then
// ignore their leader lease, since the current leader asked for the election.
const transferCtx = ^uint64(0)

func (r *Raft) campaign(pre bool) { r.campaignWith(pre, 0) }

func (r *Raft) campaignWith(pre bool, ctx uint64) {
	var term uint64
	var typ MsgType
	if pre {
		r.becomePreCandidate()
		term, typ = r.term+1, MsgPreVote
	} else {
		r.becomeCandidate()
		term, typ = r.term, MsgVote
	}
	if r.poll(r.id, true) {
		r.wonElection(pre)
		return
	}
	for _, id := range sortedIDs(r.voters) {
		if id != r.id {
			r.send(Message{Type: typ, To: id, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(), Context: ctx})
		}
	}
}

func (r *Raft) wonElection(pre bool) {
	if pre {
		r.campaign(false)
	} else {
		r.becomeLeader()
		r.bcastAppend()
	}
}

// poll records a vote and reports whether the election is won.
func (r *Raft) poll(id NodeID, granted bool) (won bool) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = granted
	}
	yes := 0
	for vid, g := range r.votes {
		if g && r.voters[vid] {
			yes++
		}
	}
	return yes >= r.quorum()
}

func (r *Raft) lost() bool {
	no := 0
	for vid, g := range r.votes {
		if !g && r.voters[vid] {
			no++
		}
	}
	return no >= r.quorum()
}

// --- messages ---

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

// Step processes a message from a peer.
func (r *Raft) Step(m Message) {
	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && m.Context != transferCtx &&
			r.lead != None && r.electionElapsed < r.electionTimeout {
			// Leader lease (CheckQuorum): we heard from a live leader recently, so
			// ignore disruptive campaigns, e.g. from a removed or partitioned node.
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// Never change term for a pre-vote.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// Granted pre-votes carry term+1; the term changes when we campaign.
		case m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat:
			// Tell a stale leader about the newer term so it steps down.
			r.send(Message{Type: MsgAppResp, To: m.From})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		r.handleVote(m)
		return
	}
	switch r.state {
	case StateLeader:
		r.stepLeader(m)
	case StateCandidate, StatePreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
}

func (r *Raft) handleVote(m Message) {
	resp := MsgVoteResp
	if m.Type == MsgPreVote {
		resp = MsgPreVoteResp
	}
	canVote := r.vote == m.From ||
		(r.vote == None && r.lead == None) ||
		(m.Type == MsgPreVote && m.Term > r.term)
	if canVote && r.log.isUpToDate(m.Index, m.LogTerm) {
		r.send(Message{Type: resp, To: m.From, Term: m.Term})
		if m.Type == MsgVote {
			r.electionElapsed = 0
			r.vote = m.From
		}
		return
	}
	r.send(Message{Type: resp, To: m.From, Reject: true})
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed, r.lead = 0, m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed, r.lead = 0, m.From
		r.log.commitTo(m.Commit)
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
	case MsgSnap:
		r.electionElapsed, r.lead = 0, m.From
		r.handleSnapshot(m)
	case MsgTimeoutNow:
		if r.promotable() {
			// Skip PreVote: the leader asked us to take over.
			r.campaignWith(false, transferCtx)
		}
	}
}

func (r *Raft) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		// Same term: someone else won.
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case MsgVoteResp, MsgPreVoteResp:
		if (m.Type == MsgPreVoteResp) != (r.state == StatePreCandidate) {
			return
		}
		if r.poll(m.From, !m.Reject) {
			r.wonElection(r.state == StatePreCandidate)
		} else if r.lost() {
			r.becomeFollower(r.term, None)
		}
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.log.committed {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.committed})
		return
	}
	if last, ok := r.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
		return
	}
	hint := r.log.findConflictByTerm(min(m.Index, r.log.lastIndex()), m.LogTerm)
	hintTerm, _ := r.log.term(hint)
	r.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, RejectHint: hint, LogTerm: hintTerm})
}

func (r *Raft) handleSnapshot(m Message) {
	s := m.Snapshot
	switch {
	case s.Meta.Index <= r.log.committed:
		// Stale.
	case r.log.matchTerm(s.Meta.Index, s.Meta.Term):
		// We already have the snapshot's last entry: just fast-forward commit.
		r.log.commitTo(s.Meta.Index)
	default:
		r.log.restore(s.Meta)
		r.snapshot, r.pendingSnap = s, s
		r.voters = map[NodeID]bool{}
		for _, v := range s.Meta.Conf.Voters {
			r.voters[v] = true
		}
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.lastIndex()})
}

func (r *Raft) stepLeader(m Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.recentActive = true
		if m.Reject {
			hint := m.RejectHint
			if m.LogTerm > 0 {
				hint = r.log.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.maybeDecrTo(m.Index, hint) {
				if pr.state == replicate {
					pr.become(probe, 0)
				}
				r.sendAppend(m.From)
			}
			return
		}
		wasPaused := pr.isPaused()
		if !pr.maybeUpdate(m.Index) {
			return
		}
		if m.From == r.leadTransferee && pr.match == r.log.lastIndex() {
			r.send(Message{Type: MsgTimeoutNow, To: m.From})
		}
		switch {
		case pr.state == probe:
			pr.become(replicate, 0)
		case pr.state == snapshotting && pr.match >= pr.pendingSnap:
			pr.become(probe, 0)
		}
		if r.maybeCommit() {
			r.bcastAppend()
		} else if wasPaused {
			r.sendAppend(m.From)
		}
		for r.maybeSendAppend(m.From, false) {
		}
	case MsgHeartbeatResp:
		pr.recentActive = true
		pr.paused = false
		if pr.state == snapshotting {
			if pr.match >= pr.pendingSnap {
				pr.become(probe, 0)
			} else if pr.snapWait++; pr.snapWait >= r.electionTimeout {
				// The snapshot was probably lost: fall back to probing, which
				// resends it.
				pr.pendingSnap, pr.snapWait = 0, 0
				pr.become(probe, 0)
			}
		}
		if pr.match < r.log.lastIndex() {
			r.sendAppend(m.From)
		}
		if m.Context != 0 {
			r.readAck(m.From, m.Context)
		}
	}
}

// --- replication ---

func (r *Raft) appendEntries(ents ...Entry) {
	last := r.log.lastIndex()
	for i := range ents {
		ents[i].Term, ents[i].Index = r.term, last+1+uint64(i)
	}
	r.log.append(ents...)
	r.prs[r.id].maybeUpdate(r.log.lastIndex())
	r.maybeCommit()
}

func (r *Raft) sendAppend(to NodeID) { r.maybeSendAppend(to, true) }

func (r *Raft) maybeSendAppend(to NodeID, sendIfEmpty bool) bool {
	pr := r.prs[to]
	if pr.isPaused() {
		return false
	}
	prevIdx := pr.next - 1
	prevTerm, ok := r.log.term(prevIdx)
	if !ok {
		// The follower needs entries we have compacted: send the snapshot.
		if r.snapshot == nil {
			panic("raft: compacted log without a snapshot")
		}
		r.send(Message{Type: MsgSnap, To: to, Snapshot: r.snapshot})
		pr.become(snapshotting, r.snapshot.Meta.Index)
		return true
	}
	hi := min(r.log.lastIndex()+1, pr.next+uint64(r.maxEntries))
	ents := r.log.slice(pr.next, hi)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	r.send(Message{Type: MsgApp, To: to, Index: prevIdx, LogTerm: prevTerm, Entries: ents, Commit: r.log.committed})
	if len(ents) > 0 {
		switch pr.state {
		case replicate:
			pr.next = ents[len(ents)-1].Index + 1 // optimistic pipelining
		case probe:
			pr.paused = true
		}
	}
	return true
}

func (r *Raft) bcastAppend() {
	for _, id := range sortedIDs(r.voters) {
		if id != r.id && r.prs[id] != nil {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) bcastHeartbeat(ctx uint64) {
	for _, id := range sortedIDs(r.voters) {
		if pr := r.prs[id]; id != r.id && pr != nil {
			r.send(Message{Type: MsgHeartbeat, To: id, Commit: min(pr.match, r.log.committed), Context: ctx})
		}
	}
}

// maybeCommit advances the commit index to the highest index replicated on a
// quorum, but only if that entry is from the current term (§5.4.2).
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.voters))
	for id := range r.voters {
		if pr := r.prs[id]; pr != nil {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	mci := matches[r.quorum()-1]
	if mci <= r.log.committed {
		return false
	}
	if t, _ := r.log.term(mci); t != r.term {
		return false
	}
	first := !r.committedInTerm()
	r.log.commitTo(mci)
	if first {
		for _, ctx := range r.readsAwaitCommit {
			r.startRead(ctx)
		}
		r.readsAwaitCommit = nil
	}
	return true
}

func (r *Raft) committedInTerm() bool {
	t, _ := r.log.term(r.log.committed)
	return t == r.term
}

// --- client API ---

// Propose appends data to the log. Returns the entry's index and term; the command
// succeeded iff the entry later applied at that index has the same term.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	return r.propose(Entry{Type: EntryNormal, Data: data})
}

// ProposeConfChange proposes adding or removing one voter.
func (r *Raft) ProposeConfChange(cc ConfChange) (index, term uint64, err error) {
	if r.state == StateLeader && r.pendingConfIndex > r.log.applied {
		return 0, 0, ErrConfPending
	}
	index, term, err = r.propose(Entry{Type: EntryConfChange, Data: cc.Marshal()})
	if err == nil {
		r.pendingConfIndex = index
	}
	return index, term, err
}

func (r *Raft) propose(e Entry) (uint64, uint64, error) {
	if r.state != StateLeader {
		return 0, 0, ErrNotLeader
	}
	if r.leadTransferee != None {
		return 0, 0, ErrTransfering
	}
	r.appendEntries(e)
	r.bcastAppend()
	return r.log.lastIndex(), r.term, nil
}

// ReadIndex starts a linearizable read identified by ctx (non-zero, unique).
// A ReadState{Index, ctx} is emitted once a quorum has confirmed this node is still
// leader; the read may be served after the state machine applies Index.
func (r *Raft) ReadIndex(ctx uint64) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if !r.committedInTerm() {
		r.readsAwaitCommit = append(r.readsAwaitCommit, ctx)
		return nil
	}
	r.startRead(ctx)
	return nil
}

func (r *Raft) startRead(ctx uint64) {
	if r.quorum() == 1 {
		r.readStates = append(r.readStates, ReadState{Index: r.log.committed, Ctx: ctx})
		return
	}
	r.pendingReads = append(r.pendingReads, readReq{index: r.log.committed, ctx: ctx, acks: map[NodeID]bool{r.id: true}})
	r.bcastHeartbeat(ctx)
}

func (r *Raft) readAck(from NodeID, ctx uint64) {
	for i, rr := range r.pendingReads {
		if rr.ctx != ctx {
			continue
		}
		rr.acks[from] = true
		if len(rr.acks) < r.quorum() {
			return
		}
		// Leadership confirmed at this point, so every earlier read is safe too.
		for _, done := range r.pendingReads[:i+1] {
			r.readStates = append(r.readStates, ReadState{Index: done.index, Ctx: done.ctx})
		}
		r.pendingReads = r.pendingReads[i+1:]
		return
	}
}

// TransferLeadership asks `to` to take over. Once its log is current it receives
// MsgTimeoutNow and campaigns; proposals are refused meanwhile. The transfer is
// abandoned if it does not complete within an election timeout.
func (r *Raft) TransferLeadership(to NodeID) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	pr := r.prs[to]
	if to == r.id || pr == nil {
		return nil
	}
	r.leadTransferee = to
	r.electionElapsed = 0
	if pr.match == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: to})
	} else {
		r.sendAppend(to)
	}
	return nil
}

// ApplyConfChange updates membership. Call it when a conf change entry is applied.
func (r *Raft) ApplyConfChange(cc ConfChange) {
	switch cc.Type {
	case AddNode:
		r.voters[cc.Node] = true
		if r.state == StateLeader && r.prs[cc.Node] == nil {
			r.prs[cc.Node] = &progress{next: r.log.lastIndex() + 1, recentActive: true}
		}
	case RemoveNode:
		delete(r.voters, cc.Node)
		if r.prs != nil {
			delete(r.prs, cc.Node)
		}
		if cc.Node == r.id && r.state == StateLeader {
			r.becomeFollower(r.term, None)
			return
		}
		if r.state == StateLeader && len(r.voters) > 0 && r.maybeCommit() {
			r.bcastAppend()
		}
	}
}

// Compact records a snapshot of the state machine at index (<= applied) and
// discards log entries up to it. The caller must persist the snapshot first.
func (r *Raft) Compact(index uint64, conf ConfState, data []byte) error {
	if index > r.log.applied {
		return fmt.Errorf("raft: compact %d beyond applied %d", index, r.log.applied)
	}
	if index <= r.log.snapIndex {
		return ErrCompacted
	}
	t, _ := r.log.term(index)
	r.snapshot = &Snapshot{Meta: SnapshotMeta{Index: index, Term: t, Conf: conf}, Data: data}
	r.log.compact(index, t)
	return nil
}

// --- Ready ---

func (r *Raft) HasReady() bool {
	return len(r.msgs) > 0 || r.hardState() != r.prevHS || r.pendingSnap != nil ||
		r.log.unstable <= r.log.lastIndex() || r.log.committed > max(r.log.applied, r.log.snapIndex) ||
		len(r.readStates) > 0
}

func (r *Raft) Ready() Ready {
	rd := Ready{
		Snapshot:   r.pendingSnap,
		Entries:    r.log.unstableEntries(),
		Messages:   r.msgs,
		Committed:  r.log.nextCommitted(),
		ReadStates: r.readStates,
	}
	if hs := r.hardState(); hs != r.prevHS {
		rd.HardState, rd.HSChanged = hs, true
	}
	return rd
}

// Advance acknowledges that rd was fully handled. No Step/Tick/Propose may happen
// between Ready and Advance.
func (r *Raft) Advance(rd Ready) {
	if rd.HSChanged {
		r.prevHS = rd.HardState
	}
	if rd.Snapshot != nil {
		r.log.applied = max(r.log.applied, rd.Snapshot.Meta.Index)
		r.pendingSnap = nil
	}
	if n := len(rd.Entries); n > 0 {
		r.log.unstable = rd.Entries[n-1].Index + 1
	}
	if n := len(rd.Committed); n > 0 {
		r.log.applied = rd.Committed[n-1].Index
	}
	r.msgs = nil
	r.readStates = nil
}
