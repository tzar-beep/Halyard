// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout, 2014)
// as a deterministic state machine with no I/O, goroutines or clocks of its own.
//
// The caller drives it:
//
//	r.Tick()               on every logical clock tick
//	r.Step(msg)            for every message received from a peer
//	r.Propose(data)        to replicate a command (leader only)
//	r.ReadIndex(ctx)       to start a linearizable read (leader only)
//	if r.HasReady() {
//	    rd := r.Ready()
//	    persist rd.HardState, rd.Snapshot, rd.Entries   (fsync)
//	    send rd.Messages
//	    apply rd.Committed; serve rd.ReadStates once applied
//	    r.Advance(rd)
//	}
//
// Because the core is pure, tests can run whole clusters in one goroutine under a
// simulated network and clock, with every run reproducible from a seed.
//
// Implemented: leader election with PreVote and CheckQuorum, log replication with
// fast conflict backtracking, commitment restricted to current-term entries,
// snapshots / InstallSnapshot, ReadIndex reads, and single-server membership changes
// (applied when committed, at most one in flight).
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type NodeID uint64

const None NodeID = 0

var (
	ErrNotLeader   = errors.New("raft: not leader")
	ErrConfPending = errors.New("raft: a membership change is already in progress")
	ErrCompacted   = errors.New("raft: index compacted")
)

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

type ConfChangeType uint8

const (
	AddNode ConfChangeType = iota
	RemoveNode
)

// ConfChange adds or removes one voter. Context carries the node's address.
type ConfChange struct {
	Type    ConfChangeType
	Node    NodeID
	Context []byte
}

func (cc ConfChange) Marshal() []byte {
	b := make([]byte, 9, 9+len(cc.Context))
	b[0] = byte(cc.Type)
	binary.BigEndian.PutUint64(b[1:], uint64(cc.Node))
	return append(b, cc.Context...)
}

func UnmarshalConfChange(b []byte) (ConfChange, error) {
	if len(b) < 9 || b[0] > byte(RemoveNode) {
		return ConfChange{}, errors.New("raft: bad conf change")
	}
	return ConfChange{Type: ConfChangeType(b[0]), Node: NodeID(binary.BigEndian.Uint64(b[1:9])), Context: b[9:]}, nil
}

type ConfState struct {
	Voters []NodeID
}

type SnapshotMeta struct {
	Index uint64
	Term  uint64
	Conf  ConfState
}

type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// HardState must be persisted before any message that depends on it is sent.
type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

type MsgType uint8

const (
	MsgApp MsgType = iota + 1
	MsgAppResp
	MsgVote
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
)

var msgNames = [...]string{"", "App", "AppResp", "Vote", "VoteResp", "PreVote", "PreVoteResp", "Heartbeat", "HeartbeatResp", "Snap"}

func (t MsgType) String() string {
	if int(t) < len(msgNames) {
		return msgNames[t]
	}
	return fmt.Sprintf("MsgType(%d)", t)
}

type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term uint64

	// MsgApp: Index/LogTerm identify the entry preceding Entries.
	// MsgVote/MsgPreVote: the candidate's last index/term.
	// MsgAppResp: the accepted (or rejected) index.
	LogTerm uint64
	Index   uint64
	Entries []Entry
	Commit  uint64

	Reject     bool
	RejectHint uint64 // follower's suggested next probe index (with LogTerm as its term)

	Snapshot *Snapshot
	Context  uint64 // ReadIndex request id carried on heartbeats
}

type StateType uint8

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

// ReadState says a read with id Ctx is linearizable once the state machine has
// applied Index.
type ReadState struct {
	Index uint64
	Ctx   uint64
}

// Ready is a batch of work for the caller. Persist, then send, then apply.
type Ready struct {
	HardState  HardState
	HSChanged  bool
	Snapshot   *Snapshot // received from the leader: persist it and restore the state machine
	Entries    []Entry   // persist; Entries[0].Index may overwrite (truncate) an existing suffix
	Messages   []Message
	Committed  []Entry
	ReadStates []ReadState
}
