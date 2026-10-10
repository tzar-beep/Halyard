package playground

import (
	"encoding/json"
	"testing"
)

func TestPlaygroundElectsAndCommits(t *testing.T) {
	s := New(5, 42)
	for i := 0; i < 200 && s.Leader() == 0; i++ {
		s.Step()
	}
	if s.Leader() == 0 {
		t.Fatal("no leader")
	}
	s.Propose("a=1")
	for i := 0; i < 40; i++ {
		s.Step()
	}
	v := s.View()
	for _, n := range v.Nodes {
		if n.Commit < 7 { // 5 bootstrap entries + no-op + write
			t.Fatalf("n%d commit %d", n.ID, n.Commit)
		}
	}
	old := s.Leader()
	s.Crash(old)
	for i := 0; i < 300 && (s.Leader() == 0 || s.Leader() == old); i++ {
		s.Step()
	}
	if s.Leader() == old || s.Leader() == 0 {
		t.Fatal("no failover")
	}
	if _, err := json.Marshal(s.View()); err != nil {
		t.Fatal(err)
	}
}

// Same seed and actions must replay identically.
func TestDeterministic(t *testing.T) {
	run := func() string {
		s := New(5, 7)
		for i := 0; i < 300; i++ {
			s.Step()
			if i == 100 {
				s.Crash(s.Leader())
			}
			if i%37 == 0 {
				s.Propose("")
			}
		}
		b, _ := json.Marshal(s.View())
		return string(b)
	}
	if run() != run() {
		t.Fatal("not deterministic")
	}
}
