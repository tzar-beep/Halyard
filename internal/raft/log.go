package raft

import "fmt"

// raftLog is the in-memory log: a snapshot boundary followed by entries
// snapIndex+1 .. lastIndex. The caller persists it via Ready.Entries.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	ents      []Entry // ents[i].Index == snapIndex+1+i

	committed uint64
	applied   uint64
	unstable  uint64 // first index not yet handed out in Ready.Entries
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.ents)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	}
	return l.ents[i-l.snapIndex-1].Term, true
}

func (l *raftLog) matchTerm(i, t uint64) bool {
	tt, ok := l.term(i)
	return ok && tt == t
}

// isUpToDate implements the election restriction (§5.4.1).
func (l *raftLog) isUpToDate(lastIdx, lastTerm uint64) bool {
	lt := l.lastTerm()
	return lastTerm > lt || (lastTerm == lt && lastIdx >= l.lastIndex())
}

// slice returns a copy of entries [lo, hi). lo must be > snapIndex.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo <= l.snapIndex || hi > l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: slice [%d,%d) out of bounds (snap %d, last %d)", lo, hi, l.snapIndex, l.lastIndex()))
	}
	if lo >= hi {
		return nil
	}
	return append([]Entry(nil), l.ents[lo-l.snapIndex-1:hi-l.snapIndex-1]...)
}

// append writes ents, truncating any existing suffix starting at ents[0].Index.
func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	after := ents[0].Index - 1
	if after < l.snapIndex || after > l.lastIndex() {
		panic(fmt.Sprintf("raft: append at %d outside log (snap %d, last %d)", ents[0].Index, l.snapIndex, l.lastIndex()))
	}
	if after < l.committed {
		panic(fmt.Sprintf("raft: truncating committed entry %d (committed %d)", after+1, l.committed))
	}
	n := after - l.snapIndex
	l.ents = append(l.ents[:n:n], ents...) // full slice expression: never overwrite shared arrays
	l.unstable = min(l.unstable, ents[0].Index)
}

// findConflict returns the first entry whose term disagrees with the log, or 0.
func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// findConflictByTerm returns the largest index <= index whose term is <= term.
// Used to skip a whole conflicting term per round trip during backtracking.
func (l *raftLog) findConflictByTerm(index, term uint64) uint64 {
	if index > l.lastIndex() {
		return index
	}
	for ; index > l.snapIndex; index-- {
		if t, _ := l.term(index); t <= term {
			return index
		}
	}
	return index
}

// maybeAppend handles an AppendEntries body. Returns the last new index on success.
func (l *raftLog) maybeAppend(prevIdx, prevTerm, commit uint64, ents []Entry) (uint64, bool) {
	if !l.matchTerm(prevIdx, prevTerm) {
		return 0, false
	}
	lastNew := prevIdx + uint64(len(ents))
	if ci := l.findConflict(ents); ci != 0 {
		l.append(ents[ci-prevIdx-1:]...)
	}
	l.commitTo(min(commit, lastNew))
	return lastNew, true
}

func (l *raftLog) commitTo(c uint64) {
	if c <= l.committed {
		return
	}
	if c > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d beyond last index %d", c, l.lastIndex()))
	}
	l.committed = c
}

// compact discards entries up to and including i, which has term t.
func (l *raftLog) compact(i, t uint64) {
	if i <= l.snapIndex {
		return
	}
	if i >= l.lastIndex() {
		l.ents = nil
	} else {
		l.ents = append([]Entry(nil), l.ents[i-l.snapIndex:]...)
	}
	l.snapIndex, l.snapTerm = i, t
}

// restore replaces the whole log with a snapshot boundary.
func (l *raftLog) restore(m SnapshotMeta) {
	l.snapIndex, l.snapTerm = m.Index, m.Term
	l.ents = nil
	l.committed = m.Index
	l.unstable = m.Index + 1
}

func (l *raftLog) unstableEntries() []Entry {
	lo := max(l.unstable, l.snapIndex+1)
	if lo > l.lastIndex() {
		return nil
	}
	return l.slice(lo, l.lastIndex()+1)
}

func (l *raftLog) nextCommitted() []Entry {
	lo := max(l.applied, l.snapIndex) + 1
	if l.committed < lo {
		return nil
	}
	return l.slice(lo, l.committed+1)
}
