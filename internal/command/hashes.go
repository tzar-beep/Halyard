package command

import (
	"bytes"
	"math"
	"strconv"

	"github.com/tzar-beep/redis/internal/store"
)

var hashCmds = map[string]handler{
	"hget":    {3, read, hget},
	"hmget":   {-3, read, hmget},
	"hexists": {3, read, hexists},
	"hlen":    {2, read, hlen},
	"hgetall": {2, read, hgetall},
	"hkeys":   {2, read, hkeys},
	"hvals":   {2, read, hvals},
	"hset":    {-4, write, hset},
	"hdel":    {-3, write, hdel},
	"hincrby": {4, write, hincrby},
}

func hset(c *Ctx) {
	if len(c.args)%2 != 0 {
		c.w.Error(wrongArgs("hset"))
		return
	}
	o, ok := c.mutOrCreate(c.key(1), store.KindHash)
	if !ok {
		return
	}
	added := 0
	for i := 2; i < len(c.args); i += 2 {
		f := string(c.args[i])
		if _, ok := o.Hash[f]; !ok {
			added++
		}
		o.Hash[f] = bytes.Clone(c.args[i+1])
	}
	c.w.Integer(int64(added))
	c.logArgs()
}

func hget(c *Ctx) {
	o, ok := c.get(c.key(1), store.KindHash)
	if !ok {
		return
	}
	if o == nil {
		c.w.NullBulk()
		return
	}
	if v, ok := o.Hash[c.key(2)]; ok {
		c.w.Bulk(v)
	} else {
		c.w.NullBulk()
	}
}

func hmget(c *Ctx) {
	o, ok := c.get(c.key(1), store.KindHash)
	if !ok {
		return
	}
	c.w.ArrayHeader(len(c.args) - 2)
	for _, f := range c.args[2:] {
		if o == nil {
			c.w.NullBulk()
		} else if v, ok := o.Hash[string(f)]; ok {
			c.w.Bulk(v)
		} else {
			c.w.NullBulk()
		}
	}
}

func hdel(c *Ctx) {
	key := c.key(1)
	o, ok := c.mut(key, store.KindHash)
	if !ok {
		return
	}
	n := 0
	if o != nil {
		for _, f := range c.args[2:] {
			if _, ok := o.Hash[string(f)]; ok {
				delete(o.Hash, string(f))
				n++
			}
		}
	}
	c.w.Integer(int64(n))
	if n > 0 {
		c.dropIfEmpty(key, len(o.Hash))
		c.logArgs()
	}
}

func hexists(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindHash); ok {
		has := false
		if o != nil {
			_, has = o.Hash[c.key(2)]
		}
		c.w.Integer(b2i(has))
	}
}

func hlen(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindHash); ok {
		if o == nil {
			c.w.Integer(0)
		} else {
			c.w.Integer(int64(len(o.Hash)))
		}
	}
}

// hashReply writes fields and/or values of a hash.
func hashReply(c *Ctx, fields, vals bool) {
	o, ok := c.get(c.key(1), store.KindHash)
	if !ok {
		return
	}
	if o == nil {
		c.w.ArrayHeader(0)
		return
	}
	n := len(o.Hash)
	if fields && vals {
		n *= 2
	}
	c.w.ArrayHeader(n)
	for f, v := range o.Hash {
		if fields {
			c.w.Bulk([]byte(f))
		}
		if vals {
			c.w.Bulk(v)
		}
	}
}

func hgetall(c *Ctx) { hashReply(c, true, true) }
func hkeys(c *Ctx)   { hashReply(c, true, false) }
func hvals(c *Ctx)   { hashReply(c, false, true) }

func hincrby(c *Ctx) {
	by, ok := c.int(3)
	if !ok {
		return
	}
	o, ok := c.mutOrCreate(c.key(1), store.KindHash)
	if !ok {
		return
	}
	f := c.key(2)
	var cur int64
	if v, ok := o.Hash[f]; ok {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			c.w.Error("ERR hash value is not an integer")
			c.dropIfEmpty(c.key(1), len(o.Hash))
			return
		}
		cur = n
	}
	if (by > 0 && cur > math.MaxInt64-by) || (by < 0 && cur < math.MinInt64-by) {
		c.w.Error("ERR increment or decrement would overflow")
		c.dropIfEmpty(c.key(1), len(o.Hash))
		return
	}
	cur += by
	o.Hash[f] = strconv.AppendInt(nil, cur, 10)
	c.w.Integer(cur)
	c.logArgs()
}
