package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/tzar-beep/redis/internal/store"
)

var zsetCmds = map[string]handler{
	"zscore":           {3, read, zscore},
	"zcard":            {2, read, zcard},
	"zrank":            {3, read, zrank(false)},
	"zrevrank":         {3, read, zrank(true)},
	"zcount":           {4, read, zcount},
	"zrange":           {-4, read, zrange},
	"zrevrange":        {-4, read, zrevrange},
	"zrangebyscore":    {-4, read, zrangebyscore(false)},
	"zrevrangebyscore": {-4, read, zrangebyscore(true)},
	"zadd":             {-4, write, zadd},
	"zincrby":          {4, write, zincrby},
	"zrem":             {-3, write, zrem},
}

// fmtScore formats a score the way Redis 7 does: shortest round-trip representation.
func fmtScore(f float64) []byte {
	switch {
	case math.IsInf(f, 1):
		return []byte("inf")
	case math.IsInf(f, -1):
		return []byte("-inf")
	}
	if a := math.Abs(f); f == 0 || (a >= 1e-5 && a < 1e17) {
		return strconv.AppendFloat(nil, f, 'f', -1, 64)
	}
	return strconv.AppendFloat(nil, f, 'g', -1, 64)
}

func parseScore(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(f) || strings.ContainsAny(string(b), "_xXpP") {
		return 0, false
	}
	return f, true
}

// parseBound parses a ZRANGEBYSCORE bound: "1.5", "(1.5", "-inf", "+inf".
func parseBound(b []byte) (v float64, ex, ok bool) {
	if len(b) > 0 && b[0] == '(' {
		ex, b = true, b[1:]
	}
	v, ok = parseScore(b)
	return v, ex, ok
}

func parseRange(minArg, maxArg []byte) (store.ScoreRange, bool) {
	var r store.ScoreRange
	var ok1, ok2 bool
	r.Min, r.MinEx, ok1 = parseBound(minArg)
	r.Max, r.MaxEx, ok2 = parseBound(maxArg)
	return r, ok1 && ok2
}

// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
func zadd(c *Ctx) {
	var nx, xx, gt, lt, ch, incr bool
	i := 2
flags:
	for ; i < len(c.args); i++ {
		switch strings.ToUpper(string(c.args[i])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		case "CH":
			ch = true
		case "INCR":
			incr = true
		default:
			break flags
		}
	}
	pairs := c.args[i:]
	switch {
	case len(pairs) == 0 || len(pairs)%2 != 0:
		c.w.Error(errSyntax)
		return
	case nx && xx:
		c.w.Error("ERR XX and NX options at the same time are not compatible")
		return
	case (gt && lt) || (nx && (gt || lt)):
		c.w.Error("ERR GT, LT, and/or NX options at the same time are not compatible")
		return
	case incr && len(pairs) > 2:
		c.w.Error("ERR INCR option supports a single increment-element pair")
		return
	}
	scores := make([]float64, len(pairs)/2)
	for j := range scores {
		s, ok := parseScore(pairs[2*j])
		if !ok {
			c.w.Error(errNotFloat)
			return
		}
		scores[j] = s
	}

	key := c.key(1)
	o, ok := c.mut(key, store.KindZSet)
	if !ok {
		return
	}
	if o == nil {
		if xx {
			if incr {
				c.w.NullBulk()
			} else {
				c.w.Integer(0)
			}
			return
		}
		o = store.New(store.KindZSet)
		c.tx.Put(key, o)
	}
	z := o.ZSet
	added, changed := 0, 0
	var last float64
	skipped := false
	for j, score := range scores {
		m := string(pairs[2*j+1])
		cur, exists := z.Score(m)
		if incr && exists {
			score += cur
		}
		if math.IsNaN(score) {
			c.w.Error("ERR resulting score is not a number (NaN)")
			c.dropIfEmpty(key, z.Len())
			return
		}
		switch {
		case exists && (nx || (gt && !(score > cur)) || (lt && !(score < cur))):
			skipped = true
		case !exists && xx:
			skipped = true
		case exists:
			if score != cur {
				z.Add(m, score)
				changed++
			}
		default:
			z.Add(m, score)
			added++
		}
		last = score
	}
	c.dropIfEmpty(key, z.Len())
	if incr {
		if skipped {
			c.w.NullBulk()
		} else {
			c.w.Bulk(fmtScore(last))
		}
	} else if ch {
		c.w.Integer(int64(added + changed))
	} else {
		c.w.Integer(int64(added))
	}
	if added+changed > 0 {
		c.logArgs()
	}
}

func zincrby(c *Ctx) {
	by, ok := parseScore(c.args[2])
	if !ok {
		c.w.Error(errNotFloat)
		return
	}
	key := c.key(1)
	o, ok := c.mutOrCreate(key, store.KindZSet)
	if !ok {
		return
	}
	m := c.key(3)
	cur, _ := o.ZSet.Score(m)
	score := cur + by
	if math.IsNaN(score) {
		c.w.Error("ERR resulting score is not a number (NaN)")
		c.dropIfEmpty(key, o.ZSet.Len())
		return
	}
	o.ZSet.Add(m, score)
	c.w.Bulk(fmtScore(score))
	c.logArgs()
}

func zrem(c *Ctx) {
	key := c.key(1)
	o, ok := c.mut(key, store.KindZSet)
	if !ok {
		return
	}
	n := 0
	if o != nil {
		for _, m := range c.args[2:] {
			if o.ZSet.Remove(string(m)) {
				n++
			}
		}
	}
	c.w.Integer(int64(n))
	if n > 0 {
		c.dropIfEmpty(key, o.ZSet.Len())
		c.logArgs()
	}
}

func zscore(c *Ctx) {
	o, ok := c.get(c.key(1), store.KindZSet)
	if !ok {
		return
	}
	if o != nil {
		if s, ok := o.ZSet.Score(c.key(2)); ok {
			c.w.Bulk(fmtScore(s))
			return
		}
	}
	c.w.NullBulk()
}

func zcard(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindZSet); ok {
		if o == nil {
			c.w.Integer(0)
		} else {
			c.w.Integer(int64(o.ZSet.Len()))
		}
	}
}

func zrank(rev bool) func(*Ctx) {
	return func(c *Ctx) {
		o, ok := c.get(c.key(1), store.KindZSet)
		if !ok {
			return
		}
		if o != nil {
			if r, ok := o.ZSet.Rank(c.key(2), rev); ok {
				c.w.Integer(int64(r))
				return
			}
		}
		c.w.NullBulk()
	}
}

func zcount(c *Ctx) {
	r, ok := parseRange(c.args[2], c.args[3])
	if !ok {
		c.w.Error("ERR min or max is not a float")
		return
	}
	if o, ok := c.get(c.key(1), store.KindZSet); ok {
		if o == nil {
			c.w.Integer(0)
		} else {
			c.w.Integer(int64(o.ZSet.Count(r)))
		}
	}
}

func writeZEntries(c *Ctx, es []store.ZEntry, withScores bool) {
	n := len(es)
	if withScores {
		n *= 2
	}
	c.w.ArrayHeader(n)
	for _, e := range es {
		c.w.Bulk([]byte(e.Member))
		if withScores {
			c.w.Bulk(fmtScore(e.Score))
		}
	}
}

// ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]
func zrange(c *Ctx) {
	var byScore, rev, withScores, limit bool
	offset, count := int64(0), int64(-1)
	for i := 4; i < len(c.args); i++ {
		switch strings.ToUpper(string(c.args[i])) {
		case "BYSCORE":
			if byScore {
				c.w.Error(errSyntax)
				return
			}
			byScore = true
		case "REV":
			if rev {
				c.w.Error(errSyntax)
				return
			}
			rev = true
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(c.args) {
				c.w.Error(errSyntax)
				return
			}
			var ok bool
			if offset, ok = c.int(i + 1); !ok {
				return
			}
			if count, ok = c.int(i + 2); !ok {
				return
			}
			limit = true
			i += 2
		default:
			c.w.Error(errSyntax)
			return
		}
	}
	if limit && !byScore {
		c.w.Error("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
		return
	}
	if byScore {
		minArg, maxArg := c.args[2], c.args[3]
		if rev {
			minArg, maxArg = maxArg, minArg
		}
		scoreRange(c, minArg, maxArg, offset, count, rev, withScores)
		return
	}
	rankRange(c, 2, rev, withScores)
}

// ZREVRANGE key start stop [WITHSCORES]
func zrevrange(c *Ctx) {
	withScores := false
	if len(c.args) == 5 && strings.EqualFold(string(c.args[4]), "WITHSCORES") {
		withScores = true
	} else if len(c.args) != 4 {
		c.w.Error(errSyntax)
		return
	}
	rankRange(c, 2, true, withScores)
}

func rankRange(c *Ctx, at int, rev, withScores bool) {
	start, ok := c.int(at)
	if !ok {
		return
	}
	stop, ok := c.int(at + 1)
	if !ok {
		return
	}
	o, ok := c.get(c.key(1), store.KindZSet)
	if !ok {
		return
	}
	if o == nil {
		c.w.ArrayHeader(0)
		return
	}
	s, e, nonEmpty := normRange(start, stop, o.ZSet.Len())
	if !nonEmpty {
		c.w.ArrayHeader(0)
		return
	}
	writeZEntries(c, o.ZSet.RangeByRank(s, e, rev), withScores)
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
func zrangebyscore(rev bool) func(*Ctx) {
	return func(c *Ctx) {
		withScores := false
		offset, count := int64(0), int64(-1)
		for i := 4; i < len(c.args); i++ {
			switch strings.ToUpper(string(c.args[i])) {
			case "WITHSCORES":
				withScores = true
			case "LIMIT":
				if i+2 >= len(c.args) {
					c.w.Error(errSyntax)
					return
				}
				var ok bool
				if offset, ok = c.int(i + 1); !ok {
					return
				}
				if count, ok = c.int(i + 2); !ok {
					return
				}
				i += 2
			default:
				c.w.Error(errSyntax)
				return
			}
		}
		minArg, maxArg := c.args[2], c.args[3]
		if rev {
			minArg, maxArg = maxArg, minArg
		}
		scoreRange(c, minArg, maxArg, offset, count, rev, withScores)
	}
}

func scoreRange(c *Ctx, minArg, maxArg []byte, offset, count int64, rev, withScores bool) {
	r, ok := parseRange(minArg, maxArg)
	if !ok {
		c.w.Error("ERR min or max is not a float")
		return
	}
	o, ok := c.get(c.key(1), store.KindZSet)
	if !ok {
		return
	}
	if o == nil || offset < 0 {
		c.w.ArrayHeader(0)
		return
	}
	if count < 0 {
		count = -1
	}
	writeZEntries(c, o.ZSet.RangeByScore(r, int(min(offset, math.MaxInt32)), int(min(count, math.MaxInt32)), rev), withScores)
}
