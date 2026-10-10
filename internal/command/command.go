// Package command validates and dispatches client commands.
//
// Write path: for commands flagged `write`, the dispatcher holds wmu while it applies
// the mutation to the store and appends the resulting log record to the AOF. This
// keeps the AOF order identical to the order mutations were applied. The reply sits in
// the connection's buffer and is only flushed after Dispatch returns, so with
// appendfsync=always a client is never acknowledged before its write is fsynced.
package command

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
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

// Appender persists mutation records (implemented by persistence.AOF).
type Appender interface {
	Append(args [][]byte) error
	Err() error // non-nil once the log is unusable; writes are then refused
}

// handler executes a command, writes its reply, and returns the record to persist
// (nil when nothing changed). Records are deterministic: relative TTLs become
// absolute timestamps and INCR becomes SET, so replay reproduces the same state.
type handler struct {
	arity int // >0 exact arg count incl. name; <0 minimum count
	write bool
	fn    func(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte
}

type Dispatcher struct {
	store    *store.Store
	password string   // empty disables AUTH
	aof      Appender // nil disables persistence
	wmu      sync.Mutex

	replayBuf bytes.Buffer
	replayW   *resp.Writer
}

func NewDispatcher(s *store.Store, password string) *Dispatcher {
	d := &Dispatcher{store: s, password: password}
	d.replayW = resp.NewWriter(&d.replayBuf)
	return d
}

// SetAppender enables persistence. Call before serving clients.
func (d *Dispatcher) SetAppender(a Appender) { d.aof = a }

// Session is per-connection state.
type Session struct {
	Authed bool
}

var table map[string]handler

func init() {
	table = map[string]handler{
		"ping":      {-1, false, ping},
		"echo":      {2, false, echo},
		"get":       {2, false, get},
		"mget":      {-2, false, mget},
		"exists":    {-2, false, exists},
		"ttl":       {2, false, ttl(time.Second)},
		"pttl":      {2, false, ttl(time.Millisecond)},
		"set":       {-3, true, set},
		"mset":      {-3, true, mset},
		"del":       {-2, true, del},
		"incr":      {2, true, incrBy(1)},
		"decr":      {2, true, incrBy(-1)},
		"incrby":    {3, true, incrBy(0)},
		"decrby":    {3, true, incrBy(0)},
		"expire":    {3, true, expire(time.Second, false)},
		"pexpire":   {3, true, expire(time.Millisecond, false)},
		"expireat":  {3, true, expire(time.Second, true)},
		"pexpireat": {3, true, expire(time.Millisecond, true)},
		"persist":   {2, true, persist},
	}
}

// Dispatch executes one command and writes its reply to w. A non-nil error means the
// write could not be persisted; the caller must drop the connection without flushing
// so the client never sees an acknowledgement.
func (d *Dispatcher) Dispatch(w *resp.Writer, sess *Session, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToLower(string(args[0]))
	if name == "auth" {
		d.auth(w, sess, args)
		return nil
	}
	if d.password != "" && !sess.Authed {
		w.Error(errNoAuth)
		return nil
	}
	h, ok := d.lookup(w, name, args)
	if !ok {
		return nil
	}
	if !h.write || d.aof == nil {
		h.fn(d, w, args)
		return nil
	}
	if err := d.aof.Err(); err != nil {
		w.Error("MISCONF Errors writing to the AOF file: " + err.Error())
		return nil
	}
	d.wmu.Lock()
	defer d.wmu.Unlock()
	if rec := h.fn(d, w, args); rec != nil {
		if err := d.aof.Append(rec); err != nil {
			return fmt.Errorf("aof append: %w", err)
		}
	}
	return nil
}

// Apply executes a logged record during AOF replay: no auth, no logging.
// Not safe for concurrent use; only call before serving clients.
func (d *Dispatcher) Apply(args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	d.replayBuf.Reset()
	h, ok := d.lookup(d.replayW, strings.ToLower(string(args[0])), args)
	if ok {
		if !h.write {
			return fmt.Errorf("non-write command %q in log", args[0])
		}
		h.fn(d, d.replayW, args)
	}
	d.replayW.Flush()
	if b := d.replayBuf.Bytes(); len(b) > 0 && b[0] == '-' {
		return errors.New(strings.TrimSpace(string(b[1:])))
	}
	return nil
}

func (d *Dispatcher) lookup(w *resp.Writer, name string, args [][]byte) (handler, bool) {
	h, ok := table[name]
	if !ok {
		w.Error(fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], quoteArgs(args[1:])))
		return h, false
	}
	if (h.arity > 0 && len(args) != h.arity) || (h.arity < 0 && len(args) < -h.arity) {
		w.Error(wrongArgs(name))
		return h, false
	}
	return h, true
}

func wrongArgs(name string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)
}

func ping(_ *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	switch len(args) {
	case 1:
		w.SimpleString("PONG")
	case 2:
		w.Bulk(args[1])
	default:
		w.Error(wrongArgs("ping"))
	}
	return nil
}

func echo(_ *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	w.Bulk(args[1])
	return nil
}

func get(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	if v, ok := d.store.Get(string(args[1])); ok {
		w.Bulk(v)
	} else {
		w.NullBulk()
	}
	return nil
}

// SET key value [NX|XX] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]
func set(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	var o store.SetOpts
	hasExpire := false
	for i := 3; i < len(args); i++ {
		switch opt := strings.ToUpper(string(args[i])); opt {
		case "NX", "XX":
			if o.Cond != store.Always {
				w.Error(errSyntax)
				return nil
			}
			o.Cond = map[string]store.SetCond{"NX": store.IfNotExists, "XX": store.IfExists}[opt]
		case "KEEPTTL":
			if hasExpire {
				w.Error(errSyntax)
				return nil
			}
			o.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpire || o.KeepTTL || i+1 >= len(args) {
				w.Error(errSyntax)
				return nil
			}
			i++
			unit := time.Second
			if opt[0] == 'P' {
				unit = time.Millisecond
			}
			at, ok := toUnixMs(args[i], unit, strings.HasSuffix(opt, "AT"))
			if !ok {
				w.Error(errNotInt)
				return nil
			}
			if at == 0 {
				w.Error("ERR invalid expire time in 'set' command")
				return nil
			}
			o.ExpireAt, hasExpire = at, true
		default:
			w.Error(errSyntax)
			return nil
		}
	}
	key, val := args[1], bytes.Clone(args[2])
	if !d.store.Set(string(key), val, o) {
		w.NullBulk()
		return nil
	}
	w.SimpleString("OK")
	rec := [][]byte{[]byte("SET"), key, val}
	switch {
	case o.ExpireAt != 0:
		rec = append(rec, []byte("PXAT"), strconv.AppendInt(nil, o.ExpireAt, 10))
	case o.KeepTTL:
		rec = append(rec, []byte("KEEPTTL"))
	}
	return rec
}

// toUnixMs converts a TTL argument to absolute unix ms.
// ok=false: not an integer. at=0: non-positive or overflowing value.
func toUnixMs(arg []byte, unit time.Duration, absolute bool) (at int64, ok bool) {
	n, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return 0, false
	}
	mult := int64(unit / time.Millisecond)
	var base int64
	if !absolute {
		base = time.Now().UnixMilli()
	}
	if n <= 0 || n > (math.MaxInt64-base)/mult {
		return 0, true
	}
	return base + n*mult, true
}

func mget(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	vals := d.store.MGet(keys(args[1:]))
	w.ArrayHeader(len(vals))
	for _, v := range vals {
		if v == nil {
			w.NullBulk()
		} else {
			w.Bulk(v)
		}
	}
	return nil
}

func mset(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	if len(args)%2 != 1 {
		w.Error(wrongArgs("mset"))
		return nil
	}
	n := (len(args) - 1) / 2
	ks, vs := make([]string, n), make([][]byte, n)
	for i := 0; i < n; i++ {
		ks[i], vs[i] = string(args[1+2*i]), bytes.Clone(args[2+2*i])
	}
	d.store.MSet(ks, vs)
	w.SimpleString("OK")
	return args
}

func del(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	n := d.store.Del(keys(args[1:])...)
	w.Integer(int64(n))
	if n == 0 {
		return nil
	}
	return args
}

func exists(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	w.Integer(int64(d.store.Exists(keys(args[1:])...)))
	return nil
}

// incrBy handles INCR/DECR (fixed delta) and INCRBY/DECRBY (delta=0: read from args).
// Logged as SET ... KEEPTTL so replay is idempotent with respect to the value.
func incrBy(delta int64) func(*Dispatcher, *resp.Writer, [][]byte) [][]byte {
	return func(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
		by := delta
		if by == 0 {
			n, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err != nil {
				w.Error(errNotInt)
				return nil
			}
			if bytes.EqualFold(args[0], []byte("decrby")) {
				if n == math.MinInt64 {
					w.Error("ERR decrement would overflow")
					return nil
				}
				n = -n
			}
			by = n
		}
		v, err := d.store.IncrBy(string(args[1]), by)
		if err != nil {
			w.Error(err.Error())
			return nil
		}
		w.Integer(v)
		return [][]byte{[]byte("SET"), args[1], strconv.AppendInt(nil, v, 10), []byte("KEEPTTL")}
	}
}

// expire handles EXPIRE/PEXPIRE (relative) and EXPIREAT/PEXPIREAT (absolute).
// Logged as PEXPIREAT, or DEL when the deadline is already past.
func expire(unit time.Duration, absolute bool) func(*Dispatcher, *resp.Writer, [][]byte) [][]byte {
	return func(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
		n, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			w.Error(errNotInt)
			return nil
		}
		mult := int64(unit / time.Millisecond)
		now := time.Now().UnixMilli()
		var base int64
		if !absolute {
			base = now
		}
		if n > (math.MaxInt64-base)/mult || n < (math.MinInt64+base)/mult {
			w.Error(fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(string(args[0]))))
			return nil
		}
		at := base + n*mult
		if !d.store.ExpireAt(string(args[1]), at) {
			w.Integer(0)
			return nil
		}
		w.Integer(1)
		if at <= now {
			return [][]byte{[]byte("DEL"), args[1]}
		}
		return [][]byte{[]byte("PEXPIREAT"), args[1], strconv.AppendInt(nil, at, 10)}
	}
}

func ttl(unit time.Duration) func(*Dispatcher, *resp.Writer, [][]byte) [][]byte {
	return func(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
		ms := d.store.PTTL(string(args[1]))
		if ms >= 0 && unit == time.Second {
			ms = (ms + 500) / 1000 // Redis rounds to nearest second
		}
		w.Integer(ms)
		return nil
	}
}

func persist(d *Dispatcher, w *resp.Writer, args [][]byte) [][]byte {
	if !d.store.Persist(string(args[1])) {
		w.Integer(0)
		return nil
	}
	w.Integer(1)
	return args
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

func quoteArgs(args [][]byte) string {
	var b strings.Builder
	for _, a := range args {
		fmt.Fprintf(&b, "'%s' ", a)
	}
	return b.String()
}
