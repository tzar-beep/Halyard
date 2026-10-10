package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/tzar-beep/Halyard/internal/command"
	"github.com/tzar-beep/Halyard/internal/persistence"
	"github.com/tzar-beep/Halyard/internal/store"
)

// Slot states inside a data group. They are part of the group's replicated state,
// so every replica enforces ownership identically when applying entries.
const (
	slotNone      uint8 = iota
	slotOwned           // serve normally
	slotMigrating       // owned, keys moving to peer: missing keys -> ASK peer
	slotImporting       // receiving from peer: serve only ASKING requests
)

type slotState struct {
	state uint8
	peer  uint32
}

// GroupSM is a data group's state machine: a keyspace plus slot ownership.
type GroupSM struct {
	id    uint32
	store *store.Store
	disp  *command.Dispatcher
	// addrOf returns the client address of a group's current leader (for
	// redirect replies only; it does not affect replicated state).
	addrOf func(group uint32) string
	metaOf func(slot int) Route

	mu    sync.RWMutex
	slots [NumSlots]slotState
}

func NewGroupSM(id uint32, groups int, st *store.Store, d *command.Dispatcher) *GroupSM {
	g := &GroupSM{id: id, store: st, disp: d}
	for s := range g.slots {
		if InitialOwner(s, groups) == id {
			g.slots[s].state = slotOwned
		}
	}
	d.SetGuard(g.guard)
	return g
}

func (g *GroupSM) slot(s int) slotState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.slots[s]
}

// guard decides whether this group may execute a command right now. It runs inside
// the same store transaction as the command (both when applying replicated writes
// and when serving reads), so the decision and the execution are atomic.
func (g *GroupSM) guard(tx *store.Tx, args [][]byte, asking bool) string {
	keys := Keys(args)
	if len(keys) == 0 {
		return ""
	}
	slot := SlotOf(keys)
	if slot < 0 {
		return "CROSSSLOT Keys in request don't hash to the same slot"
	}
	ss := g.slot(slot)
	switch ss.state {
	case slotOwned:
		return ""
	case slotImporting:
		if asking {
			return ""
		}
	case slotMigrating:
		present := 0
		for _, k := range keys {
			if tx.Exists(string(k)) {
				present++
			}
		}
		switch {
		case present == len(keys):
			return ""
		case present == 0:
			// The key is gone (moved) or new: it belongs to the target now.
			return fmt.Sprintf("ASK %d %s", slot, g.addrOf(ss.peer))
		default:
			return "TRYAGAIN Multiple keys request during rehashing of slot"
		}
	}
	return fmt.Sprintf("MOVED %d %s", slot, g.addrOf(g.metaOf(slot).Owner))
}

// ApplyCommand handles migration commands, then ordinary commands.
func (g *GroupSM) ApplyCommand(now int64, args [][]byte) []byte {
	switch strings.ToUpper(string(args[0])) {
	case "SETSLOT": // SETSLOT slot none|owned|migrating|importing peer
		if len(args) != 4 {
			return []byte("-ERR bad SETSLOT\r\n")
		}
		slot, err := strconv.Atoi(string(args[1]))
		peer, err2 := strconv.ParseUint(string(args[3]), 10, 32)
		st, ok := map[string]uint8{"none": slotNone, "owned": slotOwned, "migrating": slotMigrating, "importing": slotImporting}[strings.ToLower(string(args[2]))]
		if err != nil || err2 != nil || !ok || slot < 0 || slot >= NumSlots {
			return []byte("-ERR bad SETSLOT\r\n")
		}
		g.mu.Lock()
		g.slots[slot] = slotState{st, uint32(peer)}
		g.mu.Unlock()
		return []byte("+OK\r\n")
	case "IMPORTKEY": // IMPORTKEY key dump: install a migrated key
		if len(args) != 3 {
			return []byte("-ERR bad IMPORTKEY\r\n")
		}
		obj, exp, err := DecodeDump(args[2])
		if err != nil {
			return []byte("-ERR " + err.Error() + "\r\n")
		}
		g.store.UpdateAt(now, func(tx *store.Tx) {
			key := string(args[1])
			tx.Put(key, obj)
			if exp != 0 {
				tx.SetExpireAt(key, exp)
			} else {
				tx.Persist(key)
			}
		})
		return []byte("+OK\r\n")
	case "DELIFEQ": // DELIFEQ key dump: delete only if unchanged since dumped
		if len(args) != 3 {
			return []byte("-ERR bad DELIFEQ\r\n")
		}
		deleted := false
		g.store.UpdateAt(now, func(tx *store.Tx) {
			key := string(args[1])
			if o := tx.Get(key); o != nil && bytes.Equal(EncodeDump(o, tx.ExpireAt(key)), args[2]) {
				deleted = tx.Delete(key)
			}
		})
		if deleted {
			return []byte(":1\r\n")
		}
		return []byte(":0\r\n")
	}
	return g.disp.ApplyCommand(now, args)
}

// Snapshot: [NumSlots × (state u8, peer u32)][store snapshot]
func (g *GroupSM) Snapshot() []byte {
	g.mu.RLock()
	b := make([]byte, 0, NumSlots*5)
	for _, s := range g.slots {
		b = append(b, s.state)
		b = binary.BigEndian.AppendUint32(b, s.peer)
	}
	g.mu.RUnlock()
	return append(b, persistence.EncodeSnapshot(0, g.store.Snapshot())...)
}

func (g *GroupSM) Restore(b []byte) error {
	if len(b) < NumSlots*5 {
		return errors.New("shard: bad group snapshot")
	}
	var entries []store.Entry
	if _, err := persistence.DecodeSnapshot(b[NumSlots*5:], func(e store.Entry) { entries = append(entries, e) }); err != nil {
		return err
	}
	g.mu.Lock()
	for s := range g.slots {
		g.slots[s] = slotState{b[s*5], binary.BigEndian.Uint32(b[s*5+1:])}
	}
	g.mu.Unlock()
	g.store.Replace(entries)
	return nil
}

func (g *GroupSM) ExpiredKeys(now int64, n int) []string { return g.store.ExpiredKeys(now, n) }

// KeysInSlot returns up to n keys of a slot (used by migration).
func (g *GroupSM) KeysInSlot(slot, n int) []string {
	var out []string
	g.store.View(func(tx *store.Tx) {
		tx.Keys(func(k string) {
			if len(out) < n && KeySlot([]byte(k)) == slot {
				out = append(out, k)
			}
		})
	})
	return out
}

// Dump returns the canonical encoding of a key, or nil if it does not exist.
func (g *GroupSM) Dump(key string) []byte {
	var out []byte
	g.store.View(func(tx *store.Tx) {
		if o := tx.Get(key); o != nil {
			out = EncodeDump(o, tx.ExpireAt(key))
		}
	})
	return out
}

// --- canonical value encoding (deterministic: hash fields and set members sorted) ---

// EncodeDump: [kind u8][expireAt u64][payload]
func EncodeDump(o *store.Object, expireAt int64) []byte {
	b := []byte{byte(o.Kind)}
	b = binary.BigEndian.AppendUint64(b, uint64(expireAt))
	put := func(p []byte) {
		b = binary.AppendUvarint(b, uint64(len(p)))
		b = append(b, p...)
	}
	switch o.Kind {
	case store.KindString:
		put(o.Str)
	case store.KindList:
		b = binary.AppendUvarint(b, uint64(o.List.Len()))
		for i := 0; i < o.List.Len(); i++ {
			put(o.List.At(i))
		}
	case store.KindHash:
		fields := make([]string, 0, len(o.Hash))
		for f := range o.Hash {
			fields = append(fields, f)
		}
		slices.Sort(fields)
		b = binary.AppendUvarint(b, uint64(len(fields)))
		for _, f := range fields {
			put([]byte(f))
			put(o.Hash[f])
		}
	case store.KindSet:
		ms := make([]string, 0, len(o.Set))
		for m := range o.Set {
			ms = append(ms, m)
		}
		slices.Sort(ms)
		b = binary.AppendUvarint(b, uint64(len(ms)))
		for _, m := range ms {
			put([]byte(m))
		}
	case store.KindZSet:
		b = binary.AppendUvarint(b, uint64(o.ZSet.Len()))
		o.ZSet.Each(func(m string, s float64) {
			put([]byte(m))
			b = binary.BigEndian.AppendUint64(b, math.Float64bits(s))
		})
	}
	return b
}

var errBadDump = errors.New("bad dump")

func DecodeDump(b []byte) (*store.Object, int64, error) {
	if len(b) < 9 || b[0] > byte(store.KindZSet) {
		return nil, 0, errBadDump
	}
	kind, exp := store.Kind(b[0]), int64(binary.BigEndian.Uint64(b[1:9]))
	b = b[9:]
	get := func() ([]byte, bool) {
		n, w := binary.Uvarint(b)
		if w <= 0 || n > uint64(len(b)-w) {
			return nil, false
		}
		p := append([]byte(nil), b[w:w+int(n)]...)
		b = b[w+int(n):]
		return p, true
	}
	count := func() (int, bool) {
		n, w := binary.Uvarint(b)
		if w <= 0 || n > uint64(len(b)) {
			return 0, false
		}
		b = b[w:]
		return int(n), true
	}
	if kind == store.KindString {
		v, ok := get()
		if !ok || len(b) != 0 {
			return nil, 0, errBadDump
		}
		return store.NewString(v), exp, nil
	}
	o := store.New(kind)
	n, ok := count()
	if !ok || n == 0 {
		return nil, 0, errBadDump
	}
	for i := 0; i < n; i++ {
		m, ok := get()
		if !ok {
			return nil, 0, errBadDump
		}
		switch kind {
		case store.KindList:
			o.List.PushBack(m)
		case store.KindHash:
			v, ok := get()
			if !ok {
				return nil, 0, errBadDump
			}
			o.Hash[string(m)] = v
		case store.KindSet:
			o.Set[string(m)] = struct{}{}
		case store.KindZSet:
			if len(b) < 8 {
				return nil, 0, errBadDump
			}
			o.ZSet.Add(string(m), math.Float64frombits(binary.BigEndian.Uint64(b)))
			b = b[8:]
		}
	}
	if len(b) != 0 {
		return nil, 0, errBadDump
	}
	return o, exp, nil
}
