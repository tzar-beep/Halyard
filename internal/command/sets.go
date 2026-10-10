package command

import "github.com/tzar-beep/redis/internal/store"

var setCmds = map[string]handler{
	"sismember": {3, read, sismember},
	"smembers":  {2, read, smembers},
	"scard":     {2, read, scard},
	"sinter":    {-2, read, setAlgebra(inter)},
	"sunion":    {-2, read, setAlgebra(union)},
	"sdiff":     {-2, read, setAlgebra(diff)},
	"sadd":      {-3, write, sadd},
	"srem":      {-3, write, srem},
}

func sadd(c *Ctx) {
	o, ok := c.mutOrCreate(c.key(1), store.KindSet)
	if !ok {
		return
	}
	n := 0
	for _, m := range c.args[2:] {
		if _, ok := o.Set[string(m)]; !ok {
			o.Set[string(m)] = struct{}{}
			n++
		}
	}
	c.w.Integer(int64(n))
	if n > 0 {
		c.logArgs()
	}
}

func srem(c *Ctx) {
	key := c.key(1)
	o, ok := c.mut(key, store.KindSet)
	if !ok {
		return
	}
	n := 0
	if o != nil {
		for _, m := range c.args[2:] {
			if _, ok := o.Set[string(m)]; ok {
				delete(o.Set, string(m))
				n++
			}
		}
	}
	c.w.Integer(int64(n))
	if n > 0 {
		c.dropIfEmpty(key, len(o.Set))
		c.logArgs()
	}
}

func sismember(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindSet); ok {
		has := false
		if o != nil {
			_, has = o.Set[c.key(2)]
		}
		c.w.Integer(b2i(has))
	}
}

func scard(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindSet); ok {
		if o == nil {
			c.w.Integer(0)
		} else {
			c.w.Integer(int64(len(o.Set)))
		}
	}
}

func smembers(c *Ctx) {
	if o, ok := c.get(c.key(1), store.KindSet); ok {
		writeMembers(c, setOf(o))
	}
}

func setOf(o *store.Object) map[string]struct{} {
	if o == nil {
		return nil
	}
	return o.Set
}

func writeMembers(c *Ctx, s map[string]struct{}) {
	c.w.ArrayHeader(len(s))
	for m := range s {
		c.w.Bulk([]byte(m))
	}
}

type setOp int

const (
	inter setOp = iota
	union
	diff
)

func setAlgebra(op setOp) func(*Ctx) {
	return func(c *Ctx) {
		sets := make([]map[string]struct{}, 0, len(c.args)-1)
		for _, k := range c.args[1:] {
			o, ok := c.get(string(k), store.KindSet)
			if !ok {
				return
			}
			sets = append(sets, setOf(o))
		}
		out := map[string]struct{}{}
		switch op {
		case union:
			for _, s := range sets {
				for m := range s {
					out[m] = struct{}{}
				}
			}
		case diff:
			for m := range sets[0] {
				out[m] = struct{}{}
			}
			for _, s := range sets[1:] {
				for m := range s {
					delete(out, m)
				}
			}
		case inter:
			for m := range sets[0] {
				in := true
				for _, s := range sets[1:] {
					if _, ok := s[m]; !ok {
						in = false
						break
					}
				}
				if in {
					out[m] = struct{}{}
				}
			}
		}
		writeMembers(c, out)
	}
}
