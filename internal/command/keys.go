package command

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var keyCmds = map[string]handler{
	"exists":    {-2, read, exists},
	"ttl":       {2, read, ttl(time.Second)},
	"pttl":      {2, read, ttl(time.Millisecond)},
	"type":      {2, read, typeCmd},
	"keys":      {2, read, keysCmd},
	"dbsize":    {1, read, dbsize},
	"del":       {-2, write, del},
	"expire":    {3, write, expire(time.Second, false)},
	"pexpire":   {3, write, expire(time.Millisecond, false)},
	"expireat":  {3, write, expire(time.Second, true)},
	"pexpireat": {3, write, expire(time.Millisecond, true)},
	"persist":   {2, write, persist},
	"flushall":  {-1, write, flushall},
	// Internal: proposed by the cluster leader to replicate expiry; not callable
	// by clients.
	"delexpired": {-1, write, delExpired},
}

// delExpired removes the given keys if they have expired at the entry's timestamp.
func delExpired(c *Ctx) {
	n := 0
	for _, k := range c.args[1:] {
		key := string(k)
		if at := c.tx.ExpireAt(key); at != 0 && at <= c.tx.Now() {
			c.tx.Get(key) // a write transaction reclaims expired keys on access
			n++
		}
	}
	c.w.Integer(int64(n))
}

func exists(c *Ctx) {
	n := 0
	for _, k := range c.args[1:] {
		if c.tx.Exists(string(k)) {
			n++
		}
	}
	c.w.Integer(int64(n))
}

func del(c *Ctx) {
	n := 0
	for _, k := range c.args[1:] {
		if c.tx.Delete(string(k)) {
			n++
		}
	}
	c.w.Integer(int64(n))
	if n > 0 {
		c.logArgs()
	}
}

// expire handles EXPIRE/PEXPIRE (relative) and EXPIREAT/PEXPIREAT (absolute).
// Logged as PEXPIREAT, or DEL when the deadline is already past.
func expire(unit time.Duration, absolute bool) func(*Ctx) {
	return func(c *Ctx) {
		n, ok := c.int(2)
		if !ok {
			return
		}
		mult := int64(unit / time.Millisecond)
		now := c.tx.Now()
		var base int64
		if !absolute {
			base = now
		}
		if n > (math.MaxInt64-base)/mult || n < (math.MinInt64+base)/mult {
			c.w.Error(fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(string(c.args[0]))))
			return
		}
		key := c.key(1)
		if !c.tx.Exists(key) {
			c.w.Integer(0)
			return
		}
		at := base + n*mult
		c.tx.SetExpireAt(key, at)
		c.w.Integer(1)
		if at <= now {
			c.rec = [][]byte{[]byte("DEL"), c.args[1]}
		} else {
			c.rec = [][]byte{[]byte("PEXPIREAT"), c.args[1], strconv.AppendInt(nil, at, 10)}
		}
	}
}

func ttl(unit time.Duration) func(*Ctx) {
	return func(c *Ctx) {
		key := c.key(1)
		if !c.tx.Exists(key) {
			c.w.Integer(-2)
			return
		}
		at := c.tx.ExpireAt(key)
		if at == 0 {
			c.w.Integer(-1)
			return
		}
		ms := at - c.tx.Now()
		if unit == time.Second {
			ms = (ms + 500) / 1000 // Redis rounds to the nearest second
		}
		c.w.Integer(ms)
	}
}

func persist(c *Ctx) {
	key := c.key(1)
	if c.tx.Exists(key) && c.tx.Persist(key) {
		c.w.Integer(1)
		c.logArgs()
	} else {
		c.w.Integer(0)
	}
}

func typeCmd(c *Ctx) {
	if o := c.tx.Get(c.key(1)); o != nil {
		c.w.SimpleString(o.Kind.String())
	} else {
		c.w.SimpleString("none")
	}
}

func keysCmd(c *Ctx) {
	pat := c.key(1)
	var out []string
	c.tx.Keys(func(k string) {
		if globMatch(pat, k) {
			out = append(out, k)
		}
	})
	c.w.ArrayHeader(len(out))
	for _, k := range out {
		c.w.Bulk([]byte(k))
	}
}

func dbsize(c *Ctx) { c.w.Integer(int64(c.tx.Len())) }

// FLUSHALL [ASYNC|SYNC]: both modes are synchronous here.
func flushall(c *Ctx) {
	if len(c.args) > 2 {
		c.w.Error(errSyntax)
		return
	}
	if len(c.args) == 2 {
		if m := strings.ToUpper(c.key(1)); m != "ASYNC" && m != "SYNC" {
			c.w.Error(errSyntax)
			return
		}
	}
	c.tx.Flush()
	c.w.SimpleString("OK")
	c.rec = [][]byte{[]byte("FLUSHALL")}
}

// globMatch implements Redis glob-style patterns: * ? [abc] [^a] [a-z] and \ escapes.
func globMatch(p, s string) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if globMatch(p[1:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			p, s = p[1:], s[1:]
		case '[':
			if len(s) == 0 {
				return false
			}
			p = p[1:]
			not := len(p) > 0 && p[0] == '^'
			if not {
				p = p[1:]
			}
			match := false
			for len(p) > 0 && p[0] != ']' {
				switch {
				case p[0] == '\\' && len(p) >= 2:
					match = match || p[1] == s[0]
					p = p[2:]
				case len(p) >= 3 && p[1] == '-':
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					match = match || (s[0] >= lo && s[0] <= hi)
					p = p[3:]
				default:
					match = match || p[0] == s[0]
					p = p[1:]
				}
			}
			if len(p) > 0 {
				p = p[1:]
			}
			if match == not {
				return false
			}
			s = s[1:]
		case '\\':
			if len(p) >= 2 {
				p = p[1:]
			}
			fallthrough
		default:
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			p, s = p[1:], s[1:]
		}
	}
	return len(s) == 0
}
