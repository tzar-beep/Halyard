package command

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tzar-beep/Halyard/internal/store"
)

var stringCmds = map[string]handler{
	"get":    {2, read, get},
	"mget":   {-2, read, mget},
	"strlen": {2, read, strlen},
	"set":    {-3, write, set},
	"mset":   {-3, write, mset},
	"append": {3, write, appendCmd},
	"incr":   {2, write, incrBy(1)},
	"decr":   {2, write, incrBy(-1)},
	"incrby": {3, write, incrBy(0)},
	"decrby": {3, write, incrBy(0)},
}

func get(c *Ctx) {
	o, ok := c.get(c.key(1), store.KindString)
	switch {
	case !ok:
	case o == nil:
		c.w.NullBulk()
	default:
		c.w.Bulk(o.Str)
	}
}

func mget(c *Ctx) {
	c.w.ArrayHeader(len(c.args) - 1)
	for _, k := range c.args[1:] {
		if o := c.tx.Get(string(k)); o != nil && o.Kind == store.KindString {
			c.w.Bulk(o.Str)
		} else {
			c.w.NullBulk()
		}
	}
}

func strlen(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindString); ok {
		if o == nil {
			c.w.Integer(0)
		} else {
			c.w.Integer(int64(len(o.Str)))
		}
	}
}

// SET key value [NX|XX] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]
// Logged as SET key value [PXAT abs-ms | KEEPTTL] so replay is time-independent.
func set(c *Ctx) {
	const (
		always = iota
		nx
		xx
	)
	cond, expireAt, keepTTL, hasExpire := always, int64(0), false, false
	for i := 3; i < len(c.args); i++ {
		switch opt := strings.ToUpper(string(c.args[i])); opt {
		case "NX", "XX":
			if cond != always {
				c.w.Error(errSyntax)
				return
			}
			cond = map[string]int{"NX": nx, "XX": xx}[opt]
		case "KEEPTTL":
			if hasExpire {
				c.w.Error(errSyntax)
				return
			}
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpire || keepTTL || i+1 >= len(c.args) {
				c.w.Error(errSyntax)
				return
			}
			i++
			unit := time.Second
			if opt[0] == 'P' {
				unit = time.Millisecond
			}
			at, ok := toUnixMs(c.args[i], unit, strings.HasSuffix(opt, "AT"), c.tx.Now())
			if !ok {
				c.w.Error(errNotInt)
				return
			}
			if at == 0 {
				c.w.Error("ERR invalid expire time in 'set' command")
				return
			}
			expireAt, hasExpire = at, true
		default:
			c.w.Error(errSyntax)
			return
		}
	}
	key := c.key(1)
	exists := c.tx.Exists(key)
	if (cond == nx && exists) || (cond == xx && !exists) {
		c.w.NullBulk()
		return
	}
	val := bytes.Clone(c.args[2])
	c.tx.Put(key, store.NewString(val))
	switch {
	case hasExpire:
		c.tx.SetExpireAt(key, expireAt)
	case !keepTTL:
		c.tx.Persist(key)
	}
	c.w.SimpleString("OK")
	if !c.log {
		return
	}
	c.rec = [][]byte{[]byte("SET"), c.args[1], val}
	switch {
	case hasExpire:
		c.rec = append(c.rec, []byte("PXAT"), strconv.AppendInt(nil, expireAt, 10))
	case keepTTL:
		c.rec = append(c.rec, []byte("KEEPTTL"))
	}
}

// toUnixMs converts a TTL argument to absolute unix ms.
// ok=false: not an integer. at=0: non-positive or overflowing value.
func toUnixMs(arg []byte, unit time.Duration, absolute bool, now int64) (at int64, ok bool) {
	n, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return 0, false
	}
	mult := int64(unit / time.Millisecond)
	base := now
	if absolute {
		base = 0
	}
	if n <= 0 || n > (math.MaxInt64-base)/mult {
		return 0, true
	}
	return base + n*mult, true
}

func mset(c *Ctx) {
	if len(c.args)%2 != 1 {
		c.w.Error(wrongArgs("mset"))
		return
	}
	for i := 1; i < len(c.args); i += 2 {
		k := c.key(i)
		c.tx.Put(k, store.NewString(bytes.Clone(c.args[i+1])))
		c.tx.Persist(k)
	}
	c.w.SimpleString("OK")
	c.logArgs()
}

func appendCmd(c *Ctx) {
	key := c.key(1)
	o, ok := c.get(key, store.KindString)
	if !ok {
		return
	}
	var cur []byte
	if o != nil {
		cur = o.Str
	}
	nv := make([]byte, 0, len(cur)+len(c.args[2]))
	nv = append(append(nv, cur...), c.args[2]...)
	c.tx.Put(key, store.NewString(nv))
	c.w.Integer(int64(len(nv)))
	c.logArgs()
}

// incrBy handles INCR/DECR (fixed delta) and INCRBY/DECRBY (delta=0: read from args).
// Logged as SET ... KEEPTTL so replay does not depend on the prior value.
func incrBy(delta int64) func(*Ctx) {
	return func(c *Ctx) {
		by := delta
		if by == 0 {
			n, ok := c.int(2)
			if !ok {
				return
			}
			if bytes.EqualFold(c.args[0], []byte("decrby")) {
				if n == math.MinInt64 {
					c.w.Error("ERR decrement would overflow")
					return
				}
				n = -n
			}
			by = n
		}
		key := c.key(1)
		o, ok := c.get(key, store.KindString)
		if !ok {
			return
		}
		var cur int64
		if o != nil {
			n, err := strconv.ParseInt(string(o.Str), 10, 64)
			if err != nil {
				c.w.Error(errNotInt)
				return
			}
			cur = n
		}
		if (by > 0 && cur > math.MaxInt64-by) || (by < 0 && cur < math.MinInt64-by) {
			c.w.Error("ERR increment or decrement would overflow")
			return
		}
		cur += by
		v := strconv.AppendInt(nil, cur, 10)
		c.tx.Put(key, store.NewString(v))
		c.w.Integer(cur)
		if c.log {
			c.rec = [][]byte{[]byte("SET"), c.args[1], v, []byte("KEEPTTL")}
		}
	}
}
