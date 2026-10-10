package command

import (
	"bytes"

	"github.com/tzar-beep/redis/internal/store"
)

var listCmds = map[string]handler{
	"llen":   {2, read, llen},
	"lrange": {4, read, lrange},
	"lindex": {3, read, lindex},
	"lpush":  {-3, write, push(true)},
	"rpush":  {-3, write, push(false)},
	"lpop":   {-2, write, pop(true)},
	"rpop":   {-2, write, pop(false)},
	"lset":   {4, write, lset},
	"ltrim":  {4, write, ltrim},
	"lrem":   {4, write, lrem},
}

func push(left bool) func(*Ctx) {
	return func(c *Ctx) {
		o, ok := c.mutOrCreate(c.key(1), store.KindList)
		if !ok {
			return
		}
		for _, v := range c.args[2:] {
			if left {
				o.List.PushFront(bytes.Clone(v))
			} else {
				o.List.PushBack(bytes.Clone(v))
			}
		}
		c.w.Integer(int64(o.List.Len()))
		c.logArgs()
	}
}

// LPOP/RPOP key [count]
func pop(left bool) func(*Ctx) {
	return func(c *Ctx) {
		if len(c.args) > 3 {
			c.w.Error(errSyntax)
			return
		}
		count, withCount := int64(1), len(c.args) == 3
		if withCount {
			n, ok := c.int(2)
			if !ok {
				return
			}
			if n < 0 {
				c.w.Error("ERR value is out of range, must be positive")
				return
			}
			count = n
		}
		key := c.key(1)
		o, ok := c.mut(key, store.KindList)
		switch {
		case !ok:
			return
		case o == nil && withCount:
			c.w.NullArray()
			return
		case o == nil:
			c.w.NullBulk()
			return
		}
		l := o.List
		next := func() []byte {
			if left {
				return l.PopFront()
			}
			return l.PopBack()
		}
		if !withCount {
			c.w.Bulk(next())
		} else {
			n := min(int(count), l.Len())
			c.w.ArrayHeader(n)
			for i := 0; i < n; i++ {
				c.w.Bulk(next())
			}
			if n == 0 {
				return
			}
		}
		c.dropIfEmpty(key, l.Len())
		c.logArgs()
	}
}

func llen(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindList); ok {
		if o == nil {
			c.w.Integer(0)
		} else {
			c.w.Integer(int64(o.List.Len()))
		}
	}
}

func lrange(c *Ctx) {
	start, ok1 := c.int(2)
	if !ok1 {
		return
	}
	stop, ok2 := c.int(3)
	if !ok2 {
		return
	}
	o, ok := c.get(c.key(1), store.KindList)
	if !ok {
		return
	}
	if o == nil {
		c.w.ArrayHeader(0)
		return
	}
	s, e, nonEmpty := normRange(start, stop, o.List.Len())
	if !nonEmpty {
		c.w.ArrayHeader(0)
		return
	}
	vals := o.List.Range(s, e)
	c.w.ArrayHeader(len(vals))
	for _, v := range vals {
		c.w.Bulk(v)
	}
}

func lindex(c *Ctx) {
	o, ok := c.get(c.key(1), store.KindList)
	if !ok {
		return
	}
	if o == nil {
		c.w.NullBulk()
		return
	}
	i, ok := c.int(2)
	if !ok {
		return
	}
	n := int64(o.List.Len())
	if i < 0 {
		i += n
	}
	if i < 0 || i >= n {
		c.w.NullBulk()
		return
	}
	c.w.Bulk(o.List.At(int(i)))
}

func lset(c *Ctx) {
	o, ok := c.get(c.key(1), store.KindList)
	if !ok {
		return
	}
	if o == nil {
		c.w.Error("ERR no such key")
		return
	}
	i, ok := c.int(2)
	if !ok {
		return
	}
	o = c.tx.Mut(c.key(1))
	n := int64(o.List.Len())
	if i < 0 {
		i += n
	}
	if i < 0 || i >= n {
		c.w.Error("ERR index out of range")
		return
	}
	o.List.SetAt(int(i), bytes.Clone(c.args[3]))
	c.w.SimpleString("OK")
	c.logArgs()
}

func ltrim(c *Ctx) {
	start, ok1 := c.int(2)
	if !ok1 {
		return
	}
	stop, ok2 := c.int(3)
	if !ok2 {
		return
	}
	key := c.key(1)
	o, ok := c.mut(key, store.KindList)
	if !ok {
		return
	}
	c.w.SimpleString("OK")
	if o == nil {
		return
	}
	s, e, nonEmpty := normRange(start, stop, o.List.Len())
	if !nonEmpty {
		s, e = 1, 0
	}
	o.List.Trim(s, e)
	c.dropIfEmpty(key, o.List.Len())
	c.logArgs()
}

func lrem(c *Ctx) {
	count, ok := c.int(2)
	if !ok {
		return
	}
	key := c.key(1)
	o, ok := c.mut(key, store.KindList)
	if !ok {
		return
	}
	if o == nil {
		c.w.Integer(0)
		return
	}
	n := o.List.Remove(int(count), c.args[3])
	c.w.Integer(int64(n))
	if n > 0 {
		c.dropIfEmpty(key, o.List.Len())
		c.logArgs()
	}
}
