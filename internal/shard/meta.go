package shard

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
)

// Route says which data group owns a slot, and where it is migrating to (0 if not).
type Route struct {
	Owner     uint32
	Migrating uint32
}

// MetaSM is the state machine of the metadata group: the slot routing table.
// Its initial state (slots split evenly across groups) is derived from
// configuration, identically on every node; changes go through the log.
type MetaSM struct {
	mu    sync.RWMutex
	slots [NumSlots]Route
}

func NewMetaSM(groups int) *MetaSM {
	m := &MetaSM{}
	for s := range m.slots {
		m.slots[s].Owner = InitialOwner(s, groups)
	}
	return m
}

// InitialOwner splits slots into contiguous ranges, one per group, with the same
// boundaries `redis-cli --cluster create` uses (0-5460, 5461-10922, 10923-16383 for 3).
func InitialOwner(slot, groups int) uint32 {
	for i := groups - 1; i > 0; i-- {
		if slot >= int(math.Round(float64(i*NumSlots)/float64(groups))) {
			return uint32(i + 1)
		}
	}
	return 1
}

func (m *MetaSM) Route(slot int) Route {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.slots[slot]
}

// Range is a run of consecutive slots with the same owner.
type Range struct {
	Start, End int
	Owner      uint32
}

func (m *MetaSM) Ranges() []Range {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Range
	for s := 0; s < NumSlots; s++ {
		o := m.slots[s].Owner
		if n := len(out); n > 0 && out[n-1].Owner == o && out[n-1].End == s-1 {
			out[n-1].End = s
		} else {
			out = append(out, Range{s, s, o})
		}
	}
	return out
}

// ApplyCommand handles METASET slot owner migrating-to.
func (m *MetaSM) ApplyCommand(_ int64, args [][]byte) []byte {
	if len(args) != 4 || !strings.EqualFold(string(args[0]), "metaset") {
		return []byte("-ERR invalid metadata command\r\n")
	}
	slot, err1 := strconv.Atoi(string(args[1]))
	owner, err2 := strconv.ParseUint(string(args[2]), 10, 32)
	mig, err3 := strconv.ParseUint(string(args[3]), 10, 32)
	if err1 != nil || err2 != nil || err3 != nil || slot < 0 || slot >= NumSlots || owner == 0 {
		return []byte("-ERR invalid METASET arguments\r\n")
	}
	m.mu.Lock()
	m.slots[slot] = Route{Owner: uint32(owner), Migrating: uint32(mig)}
	m.mu.Unlock()
	return []byte("+OK\r\n")
}

func (m *MetaSM) Snapshot() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b := make([]byte, 0, NumSlots*8)
	for _, r := range m.slots {
		b = binary.BigEndian.AppendUint32(b, r.Owner)
		b = binary.BigEndian.AppendUint32(b, r.Migrating)
	}
	return b
}

func (m *MetaSM) Restore(b []byte) error {
	if len(b) != NumSlots*8 {
		return errors.New("shard: bad metadata snapshot")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for s := range m.slots {
		m.slots[s] = Route{binary.BigEndian.Uint32(b[s*8:]), binary.BigEndian.Uint32(b[s*8+4:])}
	}
	return nil
}

func (m *MetaSM) ExpiredKeys(int64, int) []string { return nil }
