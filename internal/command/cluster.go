package command

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/tzar-beep/redis/internal/resp"
	"github.com/tzar-beep/redis/internal/store"
)

// Replicator replicates commands through consensus (implemented by cluster.Node).
// Errors are returned as complete RESP error strings ("MOVED 0 host:port",
// "CLUSTERDOWN The cluster is down", ...).
type Replicator interface {
	// Propose replicates a command and returns the reply produced when it was
	// applied on this node.
	Propose(args [][]byte) ([]byte, error)
	// ReadBarrier returns once the local state reflects every write acknowledged
	// before the call (ReadIndex), making the following read linearizable.
	ReadBarrier() error
	Status() string
	AddNode(id uint64, raftAddr, clientAddr string) error
	RemoveNode(id uint64) error
}

// SetReplicator switches the dispatcher to cluster mode. Call before serving.
func (d *Dispatcher) SetReplicator(r Replicator) { d.repl = r }

// dispatchCluster runs a command in cluster mode. Writes (and whole transactions)
// go through the log and execute on every replica when applied; reads are served
// locally after a ReadIndex barrier.
func (d *Dispatcher) dispatchCluster(w *resp.Writer, h handler, name string, args [][]byte) {
	switch {
	case h.kind == admin && name != "ping" && name != "echo":
		w.Error("ERR " + name + " is not supported in cluster mode (snapshots are managed by Raft)")
	case h.kind == admin:
		h.fn(&Ctx{d: d, w: w, args: args})
	case h.kind == read:
		if err := d.repl.ReadBarrier(); err != nil {
			w.Error(err.Error())
			return
		}
		d.store.View(func(tx *store.Tx) { h.fn(&Ctx{d: d, tx: tx, w: w, args: args}) })
	default:
		d.propose(w, cloneArgs(args))
	}
}

func (d *Dispatcher) propose(w *resp.Writer, args [][]byte) {
	reply, err := d.repl.Propose(args)
	if err != nil {
		w.Error(err.Error())
		return
	}
	w.Raw(reply)
}

// ApplyCommand executes a committed log entry: a single command or a TXN of
// several. now is the timestamp the leader assigned, so every replica makes the
// same expiry decisions. Returns the encoded reply.
func (d *Dispatcher) ApplyCommand(now int64, args [][]byte) []byte {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	recs, txn := [][][]byte{args}, false
	if len(args) > 0 && strings.EqualFold(string(args[0]), "txn") {
		var err error
		if recs, err = decodeTxn(args); err != nil {
			w.Error("ERR " + err.Error())
			w.Flush()
			return buf.Bytes()
		}
		txn = true
	}
	d.store.UpdateAt(now, func(tx *store.Tx) {
		if txn {
			w.ArrayHeader(len(recs))
		}
		for _, r := range recs {
			name := strings.ToLower(string(r[0]))
			h, ok := table[name]
			if !ok || h.kind == admin || !arityOK(h, r) {
				w.Error("ERR invalid replicated command '" + name + "'")
				continue
			}
			h.fn(&Ctx{d: d, tx: tx, w: w, args: r})
		}
	})
	w.Flush()
	return buf.Bytes()
}

// raftCmd: RAFT STATUS | RAFT ADD id raft-addr client-addr | RAFT REMOVE id
func (d *Dispatcher) raftCmd(w *resp.Writer, args [][]byte) {
	if d.repl == nil {
		w.Error("ERR this server is not running in cluster mode")
		return
	}
	sub := ""
	if len(args) > 1 {
		sub = strings.ToUpper(string(args[1]))
	}
	switch {
	case sub == "STATUS" && len(args) == 2:
		w.Bulk([]byte(d.repl.Status()))
	case sub == "ADD" && len(args) == 5, sub == "REMOVE" && len(args) == 3:
		id, err := strconv.ParseUint(string(args[2]), 10, 64)
		if err != nil || id == 0 {
			w.Error("ERR invalid node id")
			return
		}
		if sub == "ADD" {
			err = d.repl.AddNode(id, string(args[3]), string(args[4]))
		} else {
			err = d.repl.RemoveNode(id)
		}
		if err != nil {
			w.Error(err.Error())
			return
		}
		w.SimpleString("OK")
	default:
		w.Error("ERR usage: RAFT STATUS | RAFT ADD <id> <raft-addr> <client-addr> | RAFT REMOVE <id>")
	}
}
