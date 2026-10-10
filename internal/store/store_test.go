package store

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fake returns a store with a manual clock and no background expirer.
func fake() (*Store, *atomic.Int64) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	return newStore(clock.Load, false), &clock
}

func str(s *Store, key string) (string, bool) {
	var v string
	var ok bool
	s.View(func(t *Tx) {
		if o := t.Get(key); o != nil {
			v, ok = string(o.Str), true
		}
	})
	return v, ok
}

func put(s *Store, key, val string) {
	s.Update(func(t *Tx) { t.Put(key, NewString([]byte(val))) })
}

func TestPutGetDelete(t *testing.T) {
	s, _ := fake()
	put(s, "k", "v")
	if v, ok := str(s, "k"); !ok || v != "v" {
		t.Fatalf("got %q %v", v, ok)
	}
	s.Update(func(t *Tx) {
		if !t.Delete("k") || t.Delete("k") {
			panic("delete semantics")
		}
	})
	if _, ok := str(s, "k"); ok {
		t.Fatal("deleted key visible")
	}
}

func TestExpiry(t *testing.T) {
	s, clock := fake()
	put(s, "k", "v")
	s.Update(func(t *Tx) { t.SetExpireAt("k", t.Now()+1000) })
	clock.Add(999)
	if _, ok := str(s, "k"); !ok {
		t.Fatal("expired early")
	}
	clock.Add(1)
	if _, ok := str(s, "k"); ok {
		t.Fatal("not expired")
	}
	// Put over an expired key must not inherit its TTL.
	put(s, "k", "new")
	s.View(func(t *Tx) {
		if t.ExpireAt("k") != 0 {
			panic("stale TTL inherited")
		}
	})
}

func TestActiveExpiry(t *testing.T) {
	s := NewStore()
	defer s.Close()
	at := time.Now().UnixMilli() + 50
	s.Update(func(t *Tx) {
		for i := 0; i < 1000; i++ {
			t.Put(strconv.Itoa(i), NewString([]byte("v")))
			t.SetExpireAt(strconv.Itoa(i), at)
		}
		t.Put("keep", NewString([]byte("v")))
	})
	deadline := time.Now().Add(5 * time.Second)
	for s.Len() > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("active expiry left %d keys", s.Len())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A snapshot must keep seeing the state at capture time while writers keep mutating.
func TestSnapshotCopyOnWrite(t *testing.T) {
	s, _ := fake()
	s.Update(func(t *Tx) {
		o := New(KindList)
		o.List.PushBack([]byte("a"))
		t.Put("l", o)
		h := New(KindHash)
		h.Hash["f"] = []byte("1")
		t.Put("h", h)
	})
	snap := s.Snapshot()
	s.Update(func(t *Tx) {
		t.Mut("l").List.PushBack([]byte("b"))
		t.Mut("h").Hash["f"] = []byte("2")
	})
	for _, e := range snap {
		switch e.Key {
		case "l":
			if e.Obj.List.Len() != 1 {
				t.Fatal("snapshot list mutated")
			}
		case "h":
			if string(e.Obj.Hash["f"]) != "1" {
				t.Fatal("snapshot hash mutated")
			}
		}
	}
	s.View(func(t *Tx) {
		if t.Get("l").List.Len() != 2 || string(t.Get("h").Hash["f"]) != "2" {
			panic("live state lost the write")
		}
	})
	// A second write after the clone must not clone again.
	var first, second *Object
	s.Update(func(t *Tx) { first = t.Mut("l") })
	s.Update(func(t *Tx) { second = t.Mut("l") })
	if first != second {
		t.Fatal("cloned more than once per snapshot")
	}
}

func TestWatch(t *testing.T) {
	s, clock := fake()
	put(s, "a", "1")
	s.Update(func(t *Tx) { t.SetExpireAt("a", t.Now()+100) })

	w := &Watch{}
	s.Watch(w, []string{"a", "b"})
	s.Update(func(t *Tx) {
		if t.Dirty(w) {
			panic("dirty without changes")
		}
	})
	put(s, "other", "x")
	s.Update(func(t *Tx) {
		if t.Dirty(w) {
			panic("unrelated key dirtied watch")
		}
	})
	clock.Add(100) // "a" expires
	s.Update(func(t *Tx) {
		if !t.Dirty(w) {
			panic("expiry not detected")
		}
	})
	s.Unwatch(w)

	w = &Watch{}
	s.Watch(w, []string{"b"})
	put(s, "b", "1")
	s.Update(func(t *Tx) {
		if !t.Dirty(w) {
			panic("write not detected")
		}
	})
	s.Unwatch(w)
	if len(s.watchers) != 0 {
		t.Fatal("watch registrations leaked")
	}
}

func TestListOps(t *testing.T) {
	l := &List{}
	for i := 0; i < 10; i++ {
		l.PushBack([]byte(strconv.Itoa(i)))
	}
	l.PushFront([]byte("-1"))
	if string(l.PopBack()) != "9" || string(l.PopFront()) != "-1" || l.Len() != 9 {
		t.Fatal("push/pop")
	}
	l.PushBack([]byte("3"))
	if n := l.Remove(-1, []byte("3")); n != 1 || l.Len() != 9 || string(l.At(3)) != "3" {
		t.Fatalf("remove from tail: n=%d len=%d", n, l.Len())
	}
	l.Trim(2, 4)
	if got := fmt.Sprintf("%s", l.Range(0, l.Len()-1)); got != "[2 3 4]" {
		t.Fatalf("trim: %s", got)
	}
}

// TestZSetAgainstModel drives random operations on a ZSet and a trivially correct
// model (sorted slice) and checks every query agrees.
func TestZSetAgainstModel(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	z := NewZSet()
	model := map[string]float64{}
	sorted := func() []ZEntry {
		var out []ZEntry
		for m, s := range model {
			out = append(out, ZEntry{m, s})
		}
		sort.Slice(out, func(i, j int) bool { return zless(out[i].Score, out[i].Member, out[j].Score, out[j].Member) })
		return out
	}
	for step := 0; step < 20000; step++ {
		m := fmt.Sprintf("m%d", r.IntN(200))
		score := float64(r.IntN(50))
		switch r.IntN(3) {
		case 0, 1:
			_, existed := model[m]
			if z.Add(m, score) == existed {
				t.Fatalf("step %d: Add new=%v", step, !existed)
			}
			model[m] = score
		case 2:
			_, existed := model[m]
			if z.Remove(m) != existed {
				t.Fatalf("step %d: Remove", step)
			}
			delete(model, m)
		}
		if step%97 != 0 {
			continue
		}
		want := sorted()
		if z.Len() != len(want) {
			t.Fatalf("len %d want %d", z.Len(), len(want))
		}
		if len(want) == 0 {
			continue
		}
		if got := z.RangeByRank(0, len(want)-1, false); !slices.Equal(got, want) {
			t.Fatalf("step %d: range mismatch", step)
		}
		rev := z.RangeByRank(0, len(want)-1, true)
		for i := range rev {
			if rev[i] != want[len(want)-1-i] {
				t.Fatalf("step %d: reverse range mismatch", step)
			}
		}
		for i, e := range want {
			if rk, _ := z.Rank(e.Member, false); rk != i {
				t.Fatalf("step %d: rank(%s)=%d want %d", step, e.Member, rk, i)
			}
		}
		lo, hi := float64(r.IntN(50)), float64(r.IntN(50))
		rg := ScoreRange{Min: lo, Max: hi, MinEx: r.IntN(2) == 0, MaxEx: r.IntN(2) == 0}
		var in []ZEntry
		for _, e := range want {
			if rg.gteMin(e.Score) && rg.lteMax(e.Score) {
				in = append(in, e)
			}
		}
		if got := z.RangeByScore(rg, 0, -1, false); !slices.Equal(got, in) {
			t.Fatalf("step %d: RangeByScore %+v got %v want %v", step, rg, got, in)
		}
		if z.Count(rg) != len(in) {
			t.Fatalf("step %d: Count %d want %d", step, z.Count(rg), len(in))
		}
		if got := z.RangeByScore(rg, 1, 2, true); len(in) > 1 {
			wantRev := []ZEntry{in[len(in)-2]}
			if len(in) > 2 {
				wantRev = append(wantRev, in[len(in)-3])
			}
			if !slices.Equal(got, wantRev) {
				t.Fatalf("step %d: reverse LIMIT got %v want %v", step, got, wantRev)
			}
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewStore()
	defer s.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := strconv.Itoa(i % 50)
				s.Update(func(t *Tx) {
					t.Put(k, NewString([]byte("v")))
					t.SetExpireAt(k, t.Now()+int64(i%3))
					o := t.Mut("ctr")
					if o == nil {
						o = New(KindHash)
						t.Put("ctr", o)
					}
					o.Hash[strconv.Itoa(len(o.Hash))] = nil
				})
				s.View(func(t *Tx) { t.Get(k) })
				if i%500 == 0 {
					s.Snapshot()
				}
			}
		}()
	}
	wg.Wait()
	s.View(func(t *Tx) {
		if n := len(t.Get("ctr").Hash); n != 16000 {
			panic(fmt.Sprintf("lost updates: %d", n))
		}
	})
}
