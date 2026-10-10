// Package command validates and dispatches client commands.
//
// Every command runs inside a store transaction: reads in View, writes in Update,
// and a MULTI/EXEC block inside a single Update, so each is atomic.
//
// Write path: with persistence on, the dispatcher holds wmu while it applies a
// mutation and appends the resulting log record, keeping the AOF in apply order.
// The reply sits in the connection's buffer and is only flushed after Dispatch
// returns, so with appendfsync=always a client is never acknowledged before its write
// is fsynced.
package command

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/tzar-beep/redis/internal/resp"
	"github.com/tzar-beep/redis/internal/store"
)

const (
	errSyntax    = "ERR syntax error"
	errNotInt    = "ERR value is not an integer or out of range"
	errNotFloat  = "ERR value is not a valid float"
	errWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errNoAuth    = "NOAUTH Authentication required."
	errWrongPass = "WRONGPASS invalid username-password pair or user is disabled."
)

// Persister persists mutation records and snapshots (implemented by persistence.Manager).
type Persister interface {
	Append(args [][]byte) error
	Err() error // non-nil once the log is unusable; writes are then refused
	Save() error
	BGSave() bool
	LastSave() int64
}

type kind uint8

const (
	read  kind = iota // runs in a read transaction
	write             // runs in a write transaction and may produce a log record
	admin             // runs outside any transaction; not allowed in MULTI
)

type handler struct {
	arity int // >0 exact arg count incl. name; <0 minimum count
	kind  kind
	fn    func(c *Ctx)
}

// Ctx is the execution context of one command.
type Ctx struct {
	d    *Dispatcher
	tx   *store.Tx // nil for admin commands
	w    *resp.Writer
	args [][]byte
	rec  [][]byte // record to persist; set only when the command changed state
}

type Dispatcher struct {
	store    *store.Store
	password string     // empty disables AUTH
	aof      Persister  // nil disables persistence
	repl     Replicator // non-nil in cluster mode
	guard    Guard      // optional slot-ownership check (sharded mode)
	wmu      sync.Mutex

	replayBuf bytes.Buffer
	replayW   *resp.Writer
}

func NewDispatcher(s *store.Store, password string) *Dispatcher {
	d := &Dispatcher{store: s, password: password}
	d.replayW = resp.NewWriter(&d.replayBuf)
	return d
}

// SetPersister enables persistence. Call before serving clients.
func (d *Dispatcher) SetPersister(p Persister) { d.aof = p }

// Freeze runs fn with all writers blocked (used to capture snapshots).
func (d *Dispatcher) Freeze(fn func()) {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	fn()
}

// Session is per-connection state.
type Session struct {
	Authed  bool
	Asking  bool // one-shot ASKING flag (sharded mode)
	Ext     any  // state owned by an outer handler (the shard router)
	inMulti bool
	txErr   bool // a command failed to queue; EXEC must abort
	queued  [][][]byte
	watch   *store.Watch
}

// CloseSession releases per-connection resources (WATCH registrations).
func (d *Dispatcher) CloseSession(sess *Session) {
	if sess.watch != nil {
		d.store.Unwatch(sess.watch)
	}
}

var table = map[string]handler{}

func init() {
	for _, group := range []map[string]handler{stringCmds, keyCmds, listCmds, hashCmds, setCmds, zsetCmds} {
		for name, h := range group {
			table[name] = h
		}
	}
	table["ping"] = handler{-1, admin, ping}
	table["echo"] = handler{2, admin, echo}
	table["save"] = handler{1, admin, save}
	table["bgsave"] = handler{1, admin, bgsave}
	table["lastsave"] = handler{1, admin, lastsave}
}

// Dispatch executes one command and writes its reply to w. A non-nil error means a
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

	switch name {
	case "raft":
		d.raftCmd(w, args)
		return nil
	case "delexpired":
		w.Error(fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], quoteArgs(args[1:])))
		return nil
	case "multi":
		if sess.inMulti {
			w.Error("ERR MULTI calls can not be nested")
		} else {
			sess.inMulti = true
			w.SimpleString("OK")
		}
		return nil
	case "exec":
		return d.exec(w, sess)
	case "discard":
		if !sess.inMulti {
			w.Error("ERR DISCARD without MULTI")
		} else {
			d.endMulti(sess)
			w.SimpleString("OK")
		}
		return nil
	case "watch":
		if d.repl != nil {
			w.Error("ERR WATCH is not supported in cluster mode")
		} else if sess.inMulti {
			w.Error("ERR WATCH inside MULTI is not allowed")
		} else if len(args) < 2 {
			w.Error(wrongArgs("watch"))
		} else {
			if sess.watch == nil {
				sess.watch = &store.Watch{}
			}
			d.store.Watch(sess.watch, keys(args[1:]))
			w.SimpleString("OK")
		}
		return nil
	case "unwatch":
		if sess.watch != nil {
			d.store.Unwatch(sess.watch)
		}
		w.SimpleString("OK")
		return nil
	}

	h, ok := lookup(w, name, args)
	if sess.inMulti {
		switch {
		case !ok:
			sess.txErr = true
		case h.kind == admin:
			w.Error("ERR Command not allowed inside a transaction")
			sess.txErr = true
		default:
			sess.queued = append(sess.queued, cloneArgs(args))
			w.SimpleString("QUEUED")
		}
		return nil
	}
	if !ok {
		return nil
	}
	if d.repl != nil {
		asking := sess.Asking
		sess.Asking = false
		d.dispatchCluster(w, h, name, args, asking)
		return nil
	}

	switch h.kind {
	case admin:
		h.fn(&Ctx{d: d, w: w, args: args})
	case read:
		d.store.View(func(tx *store.Tx) { h.fn(&Ctx{d: d, tx: tx, w: w, args: args}) })
	case write:
		if d.aof == nil {
			d.store.Update(func(tx *store.Tx) { h.fn(&Ctx{d: d, tx: tx, w: w, args: args}) })
			return nil
		}
		if err := d.aof.Err(); err != nil {
			w.Error("MISCONF Errors writing to the AOF file: " + err.Error())
			return nil
		}
		d.wmu.Lock()
		defer d.wmu.Unlock()
		var rec [][]byte
		d.store.Update(func(tx *store.Tx) {
			c := &Ctx{d: d, tx: tx, w: w, args: args}
			h.fn(c)
			rec = c.rec
		})
		if rec != nil {
			if err := d.aof.Append(rec); err != nil {
				return fmt.Errorf("aof append: %w", err)
			}
		}
	}
	return nil
}

func (d *Dispatcher) endMulti(sess *Session) {
	sess.inMulti, sess.txErr, sess.queued = false, false, nil
	if sess.watch != nil {
		d.store.Unwatch(sess.watch)
	}
}

// exec runs the queued commands atomically in one write transaction and logs them as
// a single TXN record, so recovery applies all of them or none.
func (d *Dispatcher) exec(w *resp.Writer, sess *Session) error {
	if !sess.inMulti {
		w.Error("ERR EXEC without MULTI")
		return nil
	}
	defer d.endMulti(sess)
	if sess.txErr {
		w.Error("EXECABORT Transaction discarded because of previous errors.")
		return nil
	}
	if d.repl != nil {
		if len(sess.queued) == 0 {
			w.ArrayHeader(0)
			return nil
		}
		d.propose(w, encodeTxn(sess.queued), false)
		return nil
	}
	if d.aof != nil {
		if err := d.aof.Err(); err != nil {
			w.Error("MISCONF Errors writing to the AOF file: " + err.Error())
			return nil
		}
		d.wmu.Lock()
		defer d.wmu.Unlock()
	}
	var recs [][][]byte
	aborted := false
	d.store.Update(func(tx *store.Tx) {
		if sess.watch != nil && tx.Dirty(sess.watch) {
			aborted = true
			return
		}
		w.ArrayHeader(len(sess.queued))
		for _, args := range sess.queued {
			c := &Ctx{d: d, tx: tx, w: w, args: args}
			table[strings.ToLower(string(args[0]))].fn(c)
			if c.rec != nil {
				recs = append(recs, c.rec)
			}
		}
	})
	if aborted {
		w.NullArray()
		return nil
	}
	if d.aof == nil || len(recs) == 0 {
		return nil
	}
	rec := recs[0]
	if len(recs) > 1 {
		rec = encodeTxn(recs)
	}
	if err := d.aof.Append(rec); err != nil {
		return fmt.Errorf("aof append: %w", err)
	}
	return nil
}

// encodeTxn packs several records into one: TXN argc1 args1... argc2 args2...
func encodeTxn(recs [][][]byte) [][]byte {
	out := [][]byte{[]byte("TXN")}
	for _, r := range recs {
		out = append(out, strconv.AppendInt(nil, int64(len(r)), 10))
		out = append(out, r...)
	}
	return out
}

func decodeTxn(args [][]byte) ([][][]byte, error) {
	var recs [][][]byte
	for i := 1; i < len(args); {
		n, err := strconv.Atoi(string(args[i]))
		if err != nil || n < 1 || i+1+n > len(args) {
			return nil, errors.New("malformed TXN record")
		}
		recs = append(recs, args[i+1:i+1+n])
		i += 1 + n
	}
	return recs, nil
}

// Apply executes a logged record during recovery: no auth, no logging, and any
// error reply is reported as corruption. Only call before serving clients.
func (d *Dispatcher) Apply(args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	recs := [][][]byte{args}
	if strings.EqualFold(string(args[0]), "txn") {
		var err error
		if recs, err = decodeTxn(args); err != nil {
			return err
		}
	}
	var err error
	d.store.Update(func(tx *store.Tx) {
		for _, r := range recs {
			h, ok := table[strings.ToLower(string(r[0]))]
			if !ok || h.kind != write || !arityOK(h, r) {
				err = fmt.Errorf("invalid record %q", r[0])
				return
			}
			d.replayBuf.Reset()
			h.fn(&Ctx{d: d, tx: tx, w: d.replayW, args: r})
			d.replayW.Flush()
			if b := d.replayBuf.Bytes(); len(b) > 0 && b[0] == '-' {
				err = errors.New(strings.TrimSpace(string(b[1:])))
				return
			}
		}
	})
	return err
}

func lookup(w *resp.Writer, name string, args [][]byte) (handler, bool) {
	h, ok := table[name]
	if !ok {
		w.Error(fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], quoteArgs(args[1:])))
		return h, false
	}
	if !arityOK(h, args) {
		w.Error(wrongArgs(name))
		return h, false
	}
	return h, true
}

func arityOK(h handler, args [][]byte) bool {
	return (h.arity > 0 && len(args) == h.arity) || (h.arity < 0 && len(args) >= -h.arity)
}

func wrongArgs(name string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)
}

// --- Ctx helpers ---

func (c *Ctx) key(i int) string { return string(c.args[i]) }

// logArgs persists the command exactly as received.
func (c *Ctx) logArgs() { c.rec = c.args }

// get returns the live object at key if it has kind k. ok=false means a WRONGTYPE
// error was written.
func (c *Ctx) get(key string, k store.Kind) (o *store.Object, ok bool) {
	o = c.tx.Get(key)
	if o != nil && o.Kind != k {
		c.w.Error(errWrongType)
		return nil, false
	}
	return o, true
}

// mut is get for in-place modification.
func (c *Ctx) mut(key string, k store.Kind) (*store.Object, bool) {
	o, ok := c.get(key, k)
	if !ok || o == nil {
		return nil, ok
	}
	return c.tx.Mut(key), true
}

// mutOrCreate is mut that creates an empty container when the key is missing.
func (c *Ctx) mutOrCreate(key string, k store.Kind) (*store.Object, bool) {
	o, ok := c.mut(key, k)
	if ok && o == nil {
		o = store.New(k)
		c.tx.Put(key, o)
	}
	return o, ok
}

// dropIfEmpty deletes a container key that has no elements left.
func (c *Ctx) dropIfEmpty(key string, n int) {
	if n == 0 {
		c.tx.Delete(key)
	}
}

// int parses args[i]; on failure it writes errNotInt.
func (c *Ctx) int(i int) (int64, bool) {
	n, err := strconv.ParseInt(string(c.args[i]), 10, 64)
	if err != nil {
		c.w.Error(errNotInt)
		return 0, false
	}
	return n, true
}

// normRange converts Redis-style inclusive indexes (negatives count from the end)
// into a valid [start, stop] for length n; ok=false if the range is empty.
func normRange(start, stop int64, n int) (int, int, bool) {
	if start < 0 {
		start += int64(n)
	}
	if stop < 0 {
		stop += int64(n)
	}
	if start < 0 {
		start = 0
	}
	if start > stop || start >= int64(n) {
		return 0, 0, false
	}
	if stop >= int64(n) {
		stop = int64(n) - 1
	}
	return int(start), int(stop), true
}

func keys(args [][]byte) []string {
	ks := make([]string, len(args))
	for i, a := range args {
		ks[i] = string(a)
	}
	return ks
}

func cloneArgs(args [][]byte) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = bytes.Clone(a)
	}
	return out
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

// --- connection and admin commands ---

func ping(c *Ctx) {
	switch len(c.args) {
	case 1:
		c.w.SimpleString("PONG")
	case 2:
		c.w.Bulk(c.args[1])
	default:
		c.w.Error(wrongArgs("ping"))
	}
}

func echo(c *Ctx) { c.w.Bulk(c.args[1]) }

func save(c *Ctx) {
	if c.d.aof == nil {
		c.w.Error("ERR persistence is disabled")
	} else if err := c.d.aof.Save(); err != nil {
		c.w.Error("ERR " + strings.TrimPrefix(err.Error(), "ERR "))
	} else {
		c.w.SimpleString("OK")
	}
}

func bgsave(c *Ctx) {
	switch {
	case c.d.aof == nil:
		c.w.Error("ERR persistence is disabled")
	case c.d.aof.BGSave():
		c.w.SimpleString("Background saving started")
	default:
		c.w.Error("ERR Background save already in progress")
	}
}

func lastsave(c *Ctx) {
	if c.d.aof == nil {
		c.w.Integer(0)
	} else {
		c.w.Integer(c.d.aof.LastSave())
	}
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
