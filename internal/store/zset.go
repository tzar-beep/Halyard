package store

import "math/rand/v2"

// ZSet is a sorted set: a hash map for O(1) score lookup plus a skiplist ordered by
// (score, member) with span counters for O(log n) rank queries — the same design as
// Redis's zset.
type ZSet struct {
	dict map[string]float64
	sl   *skiplist
}

type ZEntry struct {
	Member string
	Score  float64
}

// ScoreRange is a score interval; MinEx/MaxEx make a bound exclusive.
type ScoreRange struct {
	Min, Max     float64
	MinEx, MaxEx bool
}

func (r ScoreRange) gteMin(v float64) bool {
	if r.MinEx {
		return v > r.Min
	}
	return v >= r.Min
}

func (r ScoreRange) lteMax(v float64) bool {
	if r.MaxEx {
		return v < r.Max
	}
	return v <= r.Max
}

func (r ScoreRange) empty() bool {
	return r.Min > r.Max || (r.Min == r.Max && (r.MinEx || r.MaxEx))
}

func NewZSet() *ZSet { return &ZSet{dict: map[string]float64{}, sl: newSkiplist()} }

func (z *ZSet) Len() int { return len(z.dict) }

func (z *ZSet) Score(m string) (float64, bool) {
	s, ok := z.dict[m]
	return s, ok
}

// Add inserts m or updates its score. Returns true if m is new.
func (z *ZSet) Add(m string, score float64) bool {
	if old, ok := z.dict[m]; ok {
		if old != score {
			z.sl.delete(old, m)
			z.sl.insert(score, m)
			z.dict[m] = score
		}
		return false
	}
	z.sl.insert(score, m)
	z.dict[m] = score
	return true
}

func (z *ZSet) Remove(m string) bool {
	s, ok := z.dict[m]
	if !ok {
		return false
	}
	z.sl.delete(s, m)
	delete(z.dict, m)
	return true
}

// Rank returns m's 0-based position in ascending (or descending, if rev) order.
func (z *ZSet) Rank(m string, rev bool) (int, bool) {
	s, ok := z.dict[m]
	if !ok {
		return 0, false
	}
	r := z.sl.rank(s, m) - 1
	if rev {
		r = z.Len() - 1 - r
	}
	return r, true
}

// RangeByRank returns ranks start..stop inclusive (already normalized), in descending
// order if rev.
func (z *ZSet) RangeByRank(start, stop int, rev bool) []ZEntry {
	out := make([]ZEntry, 0, stop-start+1)
	n := stop - start + 1
	if rev {
		for x := z.sl.byRank(z.Len() - start); x != nil && len(out) < n; x = x.back {
			out = append(out, ZEntry{x.member, x.score})
		}
	} else {
		for x := z.sl.byRank(start + 1); x != nil && len(out) < n; x = x.level[0].fwd {
			out = append(out, ZEntry{x.member, x.score})
		}
	}
	return out
}

// RangeByScore returns members in r, skipping offset and returning at most count
// (count < 0 = unlimited), in descending order if rev.
func (z *ZSet) RangeByScore(r ScoreRange, offset, count int, rev bool) []ZEntry {
	var out []ZEntry
	if r.empty() {
		return out
	}
	var x *zNode
	if rev {
		x = z.sl.lastInRange(r)
	} else {
		x = z.sl.firstInRange(r)
	}
	for ; x != nil && offset > 0; offset-- {
		x = step(x, rev)
	}
	for ; x != nil && count != 0; x = step(x, rev) {
		if (rev && !r.gteMin(x.score)) || (!rev && !r.lteMax(x.score)) {
			break
		}
		out = append(out, ZEntry{x.member, x.score})
		count--
	}
	return out
}

func step(x *zNode, rev bool) *zNode {
	if rev {
		return x.back
	}
	return x.level[0].fwd
}

// Count returns how many members fall in r, in O(log n).
func (z *ZSet) Count(r ScoreRange) int {
	if r.empty() {
		return 0
	}
	first := z.sl.firstInRange(r)
	if first == nil {
		return 0
	}
	last := z.sl.lastInRange(r)
	return z.sl.rank(last.score, last.member) - z.sl.rank(first.score, first.member) + 1
}

// Each visits members in ascending order.
func (z *ZSet) Each(fn func(m string, score float64)) {
	for x := z.sl.head.level[0].fwd; x != nil; x = x.level[0].fwd {
		fn(x.member, x.score)
	}
}

func (z *ZSet) clone() *ZSet {
	c := NewZSet()
	z.Each(func(m string, s float64) { c.Add(m, s) })
	return c
}

// --- skiplist ---

const zMaxLevel = 32

type zLevel struct {
	fwd  *zNode
	span int // number of level-0 nodes skipped by fwd
}

type zNode struct {
	member string
	score  float64
	back   *zNode
	level  []zLevel
}

type skiplist struct {
	head, tail *zNode
	length     int
	level      int
}

func newSkiplist() *skiplist {
	return &skiplist{head: &zNode{level: make([]zLevel, zMaxLevel)}, level: 1}
}

// randLevel returns 1 + a geometric(p=1/4) variable, capped at zMaxLevel.
func randLevel() int {
	l := 1
	for l < zMaxLevel && rand.IntN(4) == 0 {
		l++
	}
	return l
}

func zless(s1 float64, m1 string, s2 float64, m2 string) bool {
	return s1 < s2 || (s1 == s2 && m1 < m2)
}

func (sl *skiplist) insert(score float64, member string) {
	var update [zMaxLevel]*zNode
	var rank [zMaxLevel]int
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i < sl.level-1 {
			rank[i] = rank[i+1]
		}
		for f := x.level[i].fwd; f != nil && zless(f.score, f.member, score, member); f = x.level[i].fwd {
			rank[i] += x.level[i].span
			x = f
		}
		update[i] = x
	}
	lvl := randLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = sl.head
			sl.head.level[i].span = sl.length
		}
		sl.level = lvl
	}
	x = &zNode{member: member, score: score, level: make([]zLevel, lvl)}
	for i := 0; i < lvl; i++ {
		x.level[i].fwd = update[i].level[i].fwd
		update[i].level[i].fwd = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	for i := lvl; i < sl.level; i++ {
		update[i].level[i].span++
	}
	if update[0] != sl.head {
		x.back = update[0]
	}
	if x.level[0].fwd != nil {
		x.level[0].fwd.back = x
	} else {
		sl.tail = x
	}
	sl.length++
}

func (sl *skiplist) delete(score float64, member string) bool {
	var update [zMaxLevel]*zNode
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.level[i].fwd; f != nil && zless(f.score, f.member, score, member); f = x.level[i].fwd {
			x = f
		}
		update[i] = x
	}
	x = x.level[0].fwd
	if x == nil || x.score != score || x.member != member {
		return false
	}
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].fwd == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].fwd = x.level[i].fwd
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].fwd != nil {
		x.level[0].fwd.back = x.back
	} else {
		sl.tail = x.back
	}
	for sl.level > 1 && sl.head.level[sl.level-1].fwd == nil {
		sl.level--
	}
	sl.length--
	return true
}

// rank returns the 1-based rank of (score, member), or 0 if absent.
func (sl *skiplist) rank(score float64, member string) int {
	r := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.level[i].fwd; f != nil && (zless(f.score, f.member, score, member) || (f.score == score && f.member == member)); f = x.level[i].fwd {
			r += x.level[i].span
			x = f
		}
		if x != sl.head && x.score == score && x.member == member {
			return r
		}
	}
	return 0
}

// byRank returns the node at 1-based rank r, or nil.
func (sl *skiplist) byRank(r int) *zNode {
	t := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].fwd != nil && t+x.level[i].span <= r {
			t += x.level[i].span
			x = x.level[i].fwd
		}
		if t == r && x != sl.head {
			return x
		}
	}
	return nil
}

func (sl *skiplist) firstInRange(r ScoreRange) *zNode {
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.level[i].fwd; f != nil && !r.gteMin(f.score); f = x.level[i].fwd {
			x = f
		}
	}
	x = x.level[0].fwd
	if x == nil || !r.lteMax(x.score) {
		return nil
	}
	return x
}

func (sl *skiplist) lastInRange(r ScoreRange) *zNode {
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.level[i].fwd; f != nil && r.lteMax(f.score); f = x.level[i].fwd {
			x = f
		}
	}
	if x == sl.head || !r.gteMin(x.score) {
		return nil
	}
	return x
}
