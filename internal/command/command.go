// Package command validates and dispatches client commands.
package command

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tzar-beep/redis/internal/resp"
	"github.com/tzar-beep/redis/internal/store"
)

const (
	errSyntax    = "ERR syntax error"
	errNotInt    = "ERR value is not an integer or out of range"
	errNoAuth    = "NOAUTH Authentication required."
	errWrongPass = "WRONGPASS invalid username-password pair or user is disabled."
)

type handler struct {
	arity int // >0 exact arg count incl. name; <0 minimum count
	fn    func(d *Dispatcher, w *resp.Writer, args [][]byte)
}

type Dispatcher struct {
	store    *store.Store
	password string // empty disables AUTH
}

func NewDispatcher(s *store.Store, password string) *Dispatcher {
	return &Dispatcher{store: s, password: password}
}

// Session is per-connection state.
type Session struct {
	Authed bool
}

var table map[string]handler

func init() {
	table = map[string]handler{
		"ping":    {-1, ping},
		"echo":    {2, echo},
		"get":     {2, get},
		"set":     {-3, set},
		"mget":    {-2, mget},
		"mset":    {-3, mset},
		"del":     {-2, del},
		"exists":  {-2, exists},
		"incr":    {2, incrBy(1)},
		"decr":    {2, incrBy(-1)},
		"incrby":  {3, incrBy(0)},
		"decrby":  {3, incrBy(0)},
		"expire":  {3, expire(time.Second)},
		"pexpire": {3, expire(time.Millisecond)},
		"ttl":     {2, ttl(time.Second)},
		"pttl":    {2, ttl(time.Millisecond)},
		"persist": {2, persist},
	}
}

// Dispatch executes one command and writes its reply to w.
func (d *Dispatcher) Dispatch(w *resp.Writer, sess *Session, args [][]byte) {
	if len(args) == 0 {
		return
	}
	name := strings.ToLower(string(args[0]))
	if name == "auth" {
		d.auth(w, sess, args)
		return
	}
	if d.password != "" && !sess.Authed {
		w.Error(errNoAuth)
		return
	}
	h, ok := table[name]
	if !ok {
		w.Error(fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], quoteArgs(args[1:])))
		return
	}
	if (h.arity > 0 && len(args) != h.arity) || (h.arity < 0 && len(args) < -h.arity) {
		w.Error(wrongArgs(name))
		return
	}
	h.fn(d, w, args)
}

func wrongArgs(name string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)
}

func ping(_ *Dispatcher, w *resp.Writer, args [][]byte) {
	switch len(args) {
	case 1:
		w.SimpleString("PONG")
	case 2:
		w.Bulk(args[1])
	default:
		w.Error(wrongArgs("ping"))
	}
}

func echo(_ *Dispatcher, w *resp.Writer, args [][]byte) { w.Bulk(args[1]) }

func get(d *Dispatcher, w *resp.Writer, args [][]byte) {
	if v, ok := d.store.Get(string(args[1])); ok {
		w.Bulk(v)
	} else {
		w.NullBulk()
	}
}

// SET key value [NX|XX] [EX s|PX ms|KEEPTTL]
func set(d *Dispatcher, w *resp.Writer, args [][]byte) {
	var o store.SetOpts
	hasExpire := false
	for i := 3; i < len(args); i++ {
		switch opt := strings.ToUpper(string(args[i])); opt {
		case "NX", "XX":
			if o.Cond != store.Always {
				w.Error(errSyntax)
				return
			}
			o.Cond = map[string]store.SetCond{"NX": store.IfNotExists, "XX": store.IfExists}[opt]
		case "KEEPTTL":
			if hasExpire {
				w.Error(errSyntax)
				return
			}
			o.KeepTTL = true
		case "EX", "PX":
			if hasExpire || o.KeepTTL || i+1 >= len(args) {
				w.Error(errSyntax)
				return
			}
			i++
			unit := time.Second
			if opt == "PX" {
				unit = time.Millisecond
			}
			at, ok := expireAt(args[i], unit)
			if !ok {
				w.Error(errNotInt)
				return
			}
			if at == 0 {
				w.Error("ERR invalid expire time in 'set' command")
				return
			}
			o.ExpireAt, hasExpire = at, true
		default:
			w.Error(errSyntax)
			return
		}
	}
	if d.store.Set(string(args[1]), bytes.Clone(args[2]), o) {
		w.SimpleString("OK")
	} else {
		w.NullBulk()
	}
}

// expireAt converts a relative TTL argument to absolute unix ms.
// ok=false: not an integer. at=0: non-positive or overflowing TTL.
func expireAt(arg []byte, unit time.Duration) (at int64, ok bool) {
	n, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return 0, false
	}
	mult := int64(unit / time.Millisecond)
	now := time.Now().UnixMilli()
	if n <= 0 || n > (math.MaxInt64-now)/mult {
		return 0, true
	}
	return now + n*mult, true
}

func mget(d *Dispatcher, w *resp.Writer, args [][]byte) {
	vals := d.store.MGet(keys(args[1:]))
	w.ArrayHeader(len(vals))
	for _, v := range vals {
		if v == nil {
			w.NullBulk()
		} else {
			w.Bulk(v)
		}
	}
}

func mset(d *Dispatcher, w *resp.Writer, args [][]byte) {
	if len(args)%2 != 1 {
		w.Error(wrongArgs("mset"))
		return
	}
	n := (len(args) - 1) / 2
	ks, vs := make([]string, n), make([][]byte, n)
	for i := 0; i < n; i++ {
		ks[i], vs[i] = string(args[1+2*i]), bytes.Clone(args[2+2*i])
	}
	d.store.MSet(ks, vs)
	w.SimpleString("OK")
}

func del(d *Dispatcher, w *resp.Writer, args [][]byte) {
	w.Integer(int64(d.store.Del(keys(args[1:])...)))
}

func exists(d *Dispatcher, w *resp.Writer, args [][]byte) {
	w.Integer(int64(d.store.Exists(keys(args[1:])...)))
}

// incrBy handles INCR/DECR (fixed delta) and INCRBY/DECRBY (delta=0: read from args).
func incrBy(delta int64) func(*Dispatcher, *resp.Writer, [][]byte) {
	return func(d *Dispatcher, w *resp.Writer, args [][]byte) {
		by := delta
		if by == 0 {
			n, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err != nil {
				w.Error(errNotInt)
				return
			}
			if bytes.EqualFold(args[0], []byte("decrby")) {
				if n == math.MinInt64 {
					w.Error("ERR decrement would overflow")
					return
				}
				n = -n
			}
			by = n
		}
		v, err := d.store.IncrBy(string(args[1]), by)
		if err != nil {
			w.Error(err.Error())
			return
		}
		w.Integer(v)
	}
}

func expire(unit time.Duration) func(*Dispatcher, *resp.Writer, [][]byte) {
	return func(d *Dispatcher, w *resp.Writer, args [][]byte) {
		n, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			w.Error(errNotInt)
			return
		}
		mult := int64(unit / time.Millisecond)
		now := time.Now().UnixMilli()
		if n > (math.MaxInt64-now)/mult || n < (math.MinInt64+now)/mult {
			w.Error(fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(string(args[0]))))
			return
		}
		at := now + n*mult
		if n <= 0 {
			at = now // non-positive TTL deletes the key
		}
		w.Integer(b2i(d.store.ExpireAt(string(args[1]), at)))
	}
}

func ttl(unit time.Duration) func(*Dispatcher, *resp.Writer, [][]byte) {
	return func(d *Dispatcher, w *resp.Writer, args [][]byte) {
		ms := d.store.PTTL(string(args[1]))
		if ms >= 0 && unit == time.Second {
			ms = (ms + 500) / 1000 // Redis rounds to nearest second
		}
		w.Integer(ms)
	}
}

func persist(d *Dispatcher, w *resp.Writer, args [][]byte) {
	w.Integer(b2i(d.store.Persist(string(args[1]))))
}

// AUTH password | AUTH default password
func (d *Dispatcher) auth(w *resp.Writer, sess *Session, args [][]byte) {
	var user, pass []byte
	switch len(args) {
	case 2:
		pass = args[1]
	case 3:
		user, pass = args[1], args[2]
	default:
		w.Error(wrongArgs("auth"))
		return
	}
	if d.password == "" {
		w.Error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
		return
	}
	userOK := user == nil || string(user) == "default"
	passOK := subtle.ConstantTimeCompare(pass, []byte(d.password)) == 1
	if !userOK || !passOK {
		w.Error(errWrongPass)
		return
	}
	sess.Authed = true
	w.SimpleString("OK")
}

func keys(args [][]byte) []string {
	ks := make([]string, len(args))
	for i, a := range args {
		ks[i] = string(a)
	}
	return ks
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func quoteArgs(args [][]byte) string {
	var b strings.Builder
	for _, a := range args {
		fmt.Fprintf(&b, "'%s' ", a)
	}
	return b.String()
}
