package linearize

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

func w(c int, v string, call, ret int64) Op {
	return Op{Client: c, Key: "k", Write: true, Value: v, Call: call, Return: ret}
}
func r(c int, v string, call, ret int64) Op {
	return Op{Client: c, Key: "k", Value: v, Call: call, Return: ret}
}

func TestKnownHistories(t *testing.T) {
	cases := []struct {
		name string
		ops  []Op
		ok   bool
	}{
		{"sequential", []Op{w(1, "a", 0, 1), r(2, "a", 2, 3)}, true},
		{"initial nil", []Op{r(1, Nil, 0, 1), w(2, "a", 2, 3)}, true},
		{"stale read after write completed", []Op{w(1, "a", 0, 1), w(1, "b", 2, 3), r(2, "a", 4, 5)}, false},
		{"concurrent read may see either", []Op{w(1, "a", 0, 1), w(1, "b", 2, 10), r(2, "a", 3, 4), r(3, "b", 5, 6)}, true},
		{"reads cannot go back in time", []Op{w(1, "a", 0, 1), w(1, "b", 2, 10), r(2, "b", 3, 4), r(3, "a", 5, 6)}, false},
		{"read of never-written value", []Op{w(1, "a", 0, 1), r(2, "x", 2, 3)}, false},
		{"unknown write may apply", []Op{w(1, "a", 0, Unknown), r(2, "a", 5, 6)}, true},
		{"unknown write may not apply", []Op{w(1, "a", 0, Unknown), r(2, Nil, 5, 6)}, true},
		{"unknown write applied then observed, cannot vanish", []Op{w(1, "a", 0, Unknown), r(2, "a", 5, 6), r(2, Nil, 7, 8)}, false},
	}
	for _, c := range cases {
		if got := Check(c.ops).OK; got != c.ok {
			t.Errorf("%s: got %v want %v", c.name, got, c.ok)
		}
	}
}

// TestRandomSequentialHistories: histories produced by a real sequential register
// with random (but consistent) real-time intervals must always pass.
func TestRandomSequentialHistories(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	for iter := 0; iter < 200; iter++ {
		var ops []Op
		state := Nil
		now := int64(0)
		for i := 0; i < 60; i++ {
			// Each op takes effect at `now`; its interval stretches randomly around it.
			call := now - int64(rng.IntN(5))
			ret := now + int64(rng.IntN(5))
			if rng.IntN(2) == 0 {
				v := strconv.Itoa(i)
				state = v
				ops = append(ops, w(rng.IntN(4), v, call, ret))
			} else {
				ops = append(ops, r(rng.IntN(4), state, call, ret))
			}
			now += 3
		}
		if res := Check(ops); !res.OK {
			t.Fatalf("iteration %d: valid history rejected", iter)
		}
	}
}
