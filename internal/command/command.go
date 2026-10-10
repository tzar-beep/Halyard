// Package command validates and dispatches client commands.
package command

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"strings"

	"github.com/tzar-beep/redis/internal/resp"
	"github.com/tzar-beep/redis/internal/store"
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

var table = map[string]handler{
	"ping": {-1, ping},
	"echo": {2, echo},
	"get":  {2, get},
	"set":  {-3, set},
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
		w.Error("NOAUTH Authentication required.")
		return
	}
	h, ok := table[name]
	if !ok {
		w.Error(fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], quoteArgs(args[1:])))
		return
	}
	if (h.arity > 0 && len(args) != h.arity) || (h.arity < 0 && len(args) < -h.arity) {
		w.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
		return
	}
	h.fn(d, w, args)
}

func ping(_ *Dispatcher, w *resp.Writer, args [][]byte) {
	switch len(args) {
	case 1:
		w.SimpleString("PONG")
	case 2:
		w.Bulk(args[1])
	default:
		w.Error("ERR wrong number of arguments for 'ping' command")
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

// SET key value. Options (EX/PX/NX/XX...) arrive in Phase 2.
func set(d *Dispatcher, w *resp.Writer, args [][]byte) {
	if len(args) != 3 {
		w.Error("ERR syntax error")
		return
	}
	d.store.Set(string(args[1]), bytes.Clone(args[2]))
	w.SimpleString("OK")
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
		w.Error("ERR wrong number of arguments for 'auth' command")
		return
	}
	if d.password == "" {
		w.Error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
		return
	}
	userOK := user == nil || string(user) == "default"
	passOK := subtle.ConstantTimeCompare(pass, []byte(d.password)) == 1
	if !userOK || !passOK {
		w.Error("WRONGPASS invalid username-password pair or user is disabled.")
		return
	}
	sess.Authed = true
	w.SimpleString("OK")
}

func quoteArgs(args [][]byte) string {
	var b strings.Builder
	for _, a := range args {
		fmt.Fprintf(&b, "'%s' ", a)
	}
	return b.String()
}
