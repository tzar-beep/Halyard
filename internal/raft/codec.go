package raft

import (
	"encoding/binary"
	"errors"
)

// Binary encoding for entries, hard state, snapshots and messages, shared by the
// WAL and the network transport. Decoding is defensive: every length is checked
// against the remaining input, so corrupt or hostile bytes produce an error, never a
// panic or a huge allocation.

var ErrDecode = errors.New("raft: malformed encoding")

type encoder struct{ b []byte }

func (e *encoder) u8(v uint8)   { e.b = append(e.b, v) }
func (e *encoder) u64(v uint64) { e.b = binary.BigEndian.AppendUint64(e.b, v) }
func (e *encoder) uv(v uint64)  { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) bytes(p []byte) {
	e.uv(uint64(len(p)))
	e.b = append(e.b, p...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() { d.err = ErrDecode; d.b = nil }

func (d *decoder) u8() uint8 {
	if len(d.b) < 1 {
		d.fail()
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) u64() uint64 {
	if len(d.b) < 8 {
		d.fail()
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *decoder) uv() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uv()
	if n > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	v := append([]byte(nil), d.b[:n]...)
	d.b = d.b[n:]
	return v
}

// count reads an element count, rejecting counts that cannot fit in the input
// (each element takes at least minSize bytes).
func (d *decoder) count(minSize int) int {
	n := d.uv()
	if n > uint64(len(d.b)/minSize) {
		d.fail()
		return 0
	}
	return int(n)
}

func (e *encoder) entry(x Entry) {
	e.u64(x.Term)
	e.u64(x.Index)
	e.u8(uint8(x.Type))
	e.bytes(x.Data)
}

func (d *decoder) entry() Entry {
	x := Entry{Term: d.u64(), Index: d.u64(), Type: EntryType(d.u8())}
	x.Data = d.bytes()
	if len(x.Data) == 0 {
		x.Data = nil // normalize: no-op entries carry no data
	}
	return x
}

func (e *encoder) snapshot(s *Snapshot) {
	e.u64(s.Meta.Index)
	e.u64(s.Meta.Term)
	e.uv(uint64(len(s.Meta.Conf.Voters)))
	for _, v := range s.Meta.Conf.Voters {
		e.u64(uint64(v))
	}
	e.bytes(s.Data)
}

func (d *decoder) snapshot() *Snapshot {
	s := &Snapshot{Meta: SnapshotMeta{Index: d.u64(), Term: d.u64()}}
	n := d.count(8)
	for i := 0; i < n; i++ {
		s.Meta.Conf.Voters = append(s.Meta.Conf.Voters, NodeID(d.u64()))
	}
	s.Data = d.bytes()
	return s
}

func EncodeEntry(x Entry) []byte {
	var e encoder
	e.entry(x)
	return e.b
}

func DecodeEntry(b []byte) (Entry, error) {
	d := decoder{b: b}
	x := d.entry()
	if d.err == nil && len(d.b) != 0 {
		d.err = ErrDecode
	}
	return x, d.err
}

func EncodeHardState(hs HardState) []byte {
	var e encoder
	e.u64(hs.Term)
	e.u64(uint64(hs.Vote))
	e.u64(hs.Commit)
	return e.b
}

func DecodeHardState(b []byte) (HardState, error) {
	d := decoder{b: b}
	hs := HardState{Term: d.u64(), Vote: NodeID(d.u64()), Commit: d.u64()}
	if d.err == nil && len(d.b) != 0 {
		d.err = ErrDecode
	}
	return hs, d.err
}

func EncodeSnapshot(s *Snapshot) []byte {
	var e encoder
	e.snapshot(s)
	return e.b
}

func DecodeSnapshot(b []byte) (*Snapshot, error) {
	d := decoder{b: b}
	s := d.snapshot()
	if d.err == nil && len(d.b) != 0 {
		d.err = ErrDecode
	}
	return s, d.err
}

// Marshal encodes a message for the network.
func (m Message) Marshal() []byte {
	var e encoder
	e.u8(uint8(m.Type))
	e.u64(uint64(m.From))
	e.u64(uint64(m.To))
	e.u64(m.Term)
	e.u64(m.LogTerm)
	e.u64(m.Index)
	e.u64(m.Commit)
	if m.Reject {
		e.u8(1)
	} else {
		e.u8(0)
	}
	e.u64(m.RejectHint)
	e.u64(m.Context)
	e.uv(uint64(len(m.Entries)))
	for _, x := range m.Entries {
		e.entry(x)
	}
	if m.Snapshot != nil {
		e.u8(1)
		e.snapshot(m.Snapshot)
	} else {
		e.u8(0)
	}
	return e.b
}

func UnmarshalMessage(b []byte) (Message, error) {
	d := decoder{b: b}
	m := Message{
		Type: MsgType(d.u8()), From: NodeID(d.u64()), To: NodeID(d.u64()), Term: d.u64(),
		LogTerm: d.u64(), Index: d.u64(), Commit: d.u64(), Reject: d.u8() == 1,
		RejectHint: d.u64(), Context: d.u64(),
	}
	n := d.count(17)
	for i := 0; i < n && d.err == nil; i++ {
		m.Entries = append(m.Entries, d.entry())
	}
	if d.u8() == 1 {
		m.Snapshot = d.snapshot()
	}
	if d.err == nil && (len(d.b) != 0 || m.Type == 0 || m.Type > MsgTimeoutNow) {
		d.err = ErrDecode
	}
	return m, d.err
}
