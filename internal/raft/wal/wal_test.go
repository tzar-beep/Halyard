package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tzar-beep/redis/internal/raft"
)

func ents(term uint64, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Term: term, Index: i, Data: []byte(fmt.Sprintf("t%d-%d", term, i))})
	}
	return out
}

func mustOpen(t *testing.T, dir string) (*WAL, raft.Persisted) {
	t.Helper()
	w, p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, p
}

func TestSaveReplayAndTruncate(t *testing.T) {
	dir := t.TempDir()
	w, p := mustOpen(t, dir)
	if len(p.Entries) != 0 || p.Snapshot != nil {
		t.Fatal("fresh WAL not empty")
	}
	hs := raft.HardState{Term: 1, Vote: 2, Commit: 3}
	w.Save(&hs, ents(1, 1, 5), nil)
	// A new leader overwrites 4..5 with term-2 entries (follower truncation).
	hs2 := raft.HardState{Term: 2, Vote: 3, Commit: 4}
	w.Save(&hs2, ents(2, 4, 6), nil)
	w.Close()

	_, p = mustOpen(t, dir)
	want := append(ents(1, 1, 3), ents(2, 4, 6)...)
	if !reflect.DeepEqual(p.Entries, want) || p.HardState != hs2 {
		t.Fatalf("got %+v %+v", p.HardState, p.Entries)
	}
}

func TestTornTail(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	hs := raft.HardState{Term: 1, Commit: 2}
	w.Save(&hs, ents(1, 1, 3), nil)
	w.Close()
	path := filepath.Join(dir, walName)
	full, _ := os.ReadFile(path)

	// Every possible truncation of the last batch must open cleanly with a prefix.
	for cut := 1; cut < 40; cut++ {
		os.WriteFile(path, full[:len(full)-cut], 0o644)
		w, p, err := Open(dir)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		w.Close()
		for i, e := range p.Entries {
			if e.Index != uint64(i+1) {
				t.Fatalf("cut %d: bad prefix %+v", cut, p.Entries)
			}
		}
	}
}

func TestCorruptMiddleRejected(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Save(nil, ents(1, 1, 5), nil)
	w.Close()
	path := filepath.Join(dir, walName)
	b, _ := os.ReadFile(path)
	b[20] ^= 0xFF // inside the first record
	os.WriteFile(path, b, 0o644)
	if _, _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestCompactAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	hs := raft.HardState{Term: 1, Commit: 8}
	w.Save(&hs, ents(1, 1, 10), nil)
	snap := &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 8, Term: 1, Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}, Data: []byte("state@8")}
	if err := w.Compact(snap); err != nil {
		t.Fatal(err)
	}
	w.Save(nil, ents(1, 11, 12), nil)
	w.Close()

	w, p := mustOpen(t, dir)
	if !reflect.DeepEqual(p.Snapshot, snap) || !reflect.DeepEqual(p.Entries, ents(1, 9, 12)) || p.HardState != hs {
		t.Fatalf("got snap %+v entries %+v hs %+v", p.Snapshot, p.Entries, p.HardState)
	}

	// Installing a snapshot from the leader replaces the whole log.
	recv := &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 50, Term: 3}, Data: []byte("state@50")}
	hs3 := raft.HardState{Term: 3, Commit: 50}
	w.Save(&hs3, nil, recv)
	w.Save(nil, ents(3, 51, 51), nil)
	w.Close()
	_, p = mustOpen(t, dir)
	if p.Snapshot.Meta.Index != 50 || len(p.Entries) != 1 || p.Entries[0].Index != 51 || p.HardState != hs3 {
		t.Fatalf("after install: %+v %+v %+v", p.Snapshot.Meta, p.Entries, p.HardState)
	}
}

func TestCorruptSnapshotRejected(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Compact(&raft.Snapshot{Meta: raft.SnapshotMeta{Index: 0}, Data: []byte("x")})
	w.Close()
	path := filepath.Join(dir, snapName)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 1
	os.WriteFile(path, b, 0o644)
	if _, _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func FuzzUnmarshalMessage(f *testing.F) {
	f.Add(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 3, Entries: ents(3, 1, 2)}.Marshal())
	f.Add(raft.Message{Type: raft.MsgSnap, Snapshot: &raft.Snapshot{Data: []byte("x")}}.Marshal())
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := raft.UnmarshalMessage(b)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode to the same bytes.
		if m2, err := raft.UnmarshalMessage(m.Marshal()); err != nil || !reflect.DeepEqual(m, m2) {
			t.Fatalf("round trip mismatch: %v", err)
		}
	})
}

// FuzzOpen: a WAL file with arbitrary contents must open cleanly or report
// ErrCorrupt — never panic, never return entries with gaps.
func FuzzOpen(f *testing.F) {
	dir := f.TempDir()
	w, _, _ := Open(dir)
	hs := raft.HardState{Term: 1, Commit: 2}
	w.Save(&hs, ents(1, 1, 3), nil)
	w.Close()
	seed, _ := os.ReadFile(filepath.Join(dir, walName))
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		d := t.TempDir()
		os.WriteFile(filepath.Join(d, walName), b, 0o644)
		w, p, err := Open(d)
		if err != nil {
			return
		}
		defer w.Close()
		for i := 1; i < len(p.Entries); i++ {
			if p.Entries[i].Index != p.Entries[i-1].Index+1 {
				t.Fatalf("gap in recovered log")
			}
		}
		if n := len(p.Entries); p.HardState.Commit > 0 && (n == 0 || p.HardState.Commit > p.Entries[n-1].Index) {
			t.Fatalf("commit %d beyond recovered log", p.HardState.Commit)
		}
	})
}
