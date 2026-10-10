// Package wal persists Raft state: a write-ahead log of entries and hard state,
// plus the latest snapshot in its own file.
//
// Directory layout:
//
//	raft.wal    records: [u32 length][u32 CRC-32C][type][payload]
//	raft.snap   [u32 CRC-32C][encoded raft.Snapshot], replaced atomically
//
// Record types: entry, hard state. An entry whose index is <= the last one replayed
// truncates the log there first, so follower log truncation is just an append.
// Each Save is a batch followed by one fsync (group commit). A torn final record
// (crash mid-write) is cut off on open; a bad record anywhere else is an error.
//
// After a snapshot, Compact rewrites the WAL with only the entries after it
// (temp file, fsync, rename), keeping disk usage and restart time bounded.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/tzar-beep/Halyard/internal/metrics"
	"github.com/tzar-beep/Halyard/internal/raft"
)

const (
	walName  = "raft.wal"
	snapName = "raft.snap"

	recEntry     = 1
	recHardState = 2

	maxRecord = 256 << 20
)

var (
	crcTable    = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt  = errors.New("wal: corrupt")
	errTornTail = errors.New("torn tail")
)

type WAL struct {
	dir  string
	f    *os.File
	bw   *bufio.Writer
	hs   raft.HardState
	snap *raft.Snapshot
	ents []raft.Entry // mirror of persisted entries after the snapshot (for Compact)
}

// Open loads the persisted state from dir (creating it if needed).
func Open(dir string) (*WAL, raft.Persisted, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.Persisted{}, err
	}
	w := &WAL{dir: dir}
	os.Remove(w.path(snapName) + ".tmp")
	os.Remove(w.path(walName) + ".tmp")
	if err := w.loadSnapshot(); err != nil {
		return nil, raft.Persisted{}, err
	}
	if err := w.replay(); err != nil {
		return nil, raft.Persisted{}, err
	}
	if err := w.openAppend(); err != nil {
		return nil, raft.Persisted{}, err
	}
	p := raft.Persisted{HardState: w.hs, Snapshot: w.snap, Entries: append([]raft.Entry(nil), w.ents...)}
	return w, p, nil
}

func (w *WAL) path(name string) string { return filepath.Join(w.dir, name) }

func (w *WAL) snapIndex() uint64 {
	if w.snap == nil {
		return 0
	}
	return w.snap.Meta.Index
}

func (w *WAL) lastIndex() uint64 {
	if n := len(w.ents); n > 0 {
		return w.ents[n-1].Index
	}
	return w.snapIndex()
}

func (w *WAL) loadSnapshot() error {
	b, err := os.ReadFile(w.path(snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) < 4 || binary.BigEndian.Uint32(b) != crc32.Checksum(b[4:], crcTable) {
		return fmt.Errorf("%w: %s checksum mismatch", ErrCorrupt, snapName)
	}
	s, err := raft.DecodeSnapshot(b[4:])
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrCorrupt, snapName, err)
	}
	w.snap = s
	return nil
}

// addEntry applies WAL semantics: drop entries covered by the snapshot, truncate on
// overwrite, and reject gaps.
func (w *WAL) addEntry(e raft.Entry) error {
	if e.Index <= w.snapIndex() {
		return nil
	}
	if e.Index > w.lastIndex()+1 {
		return fmt.Errorf("%w: entry %d after %d leaves a gap", ErrCorrupt, e.Index, w.lastIndex())
	}
	w.ents = append(w.ents[:e.Index-w.snapIndex()-1], e)
	return nil
}

func (w *WAL) replay() error {
	f, err := os.OpenFile(w.path(walName), os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<16)
	var off int64
	for {
		typ, payload, n, err := readRecord(br)
		if err == io.EOF {
			break
		}
		if errors.Is(err, errTornTail) {
			log.Printf("wal: torn record at offset %d; truncating", off)
			if err := f.Truncate(off); err != nil {
				return err
			}
			if err := f.Sync(); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return fmt.Errorf("%w: offset %d: %v", ErrCorrupt, off, err)
		}
		switch typ {
		case recEntry:
			e, err := raft.DecodeEntry(payload)
			if err != nil {
				return fmt.Errorf("%w: offset %d: %v", ErrCorrupt, off, err)
			}
			if err := w.addEntry(e); err != nil {
				return err
			}
		case recHardState:
			hs, err := raft.DecodeHardState(payload)
			if err != nil {
				return fmt.Errorf("%w: offset %d: %v", ErrCorrupt, off, err)
			}
			w.hs = hs
		default:
			return fmt.Errorf("%w: offset %d: unknown record type %d", ErrCorrupt, off, typ)
		}
		off += n
	}
	if w.hs.Commit > w.lastIndex() {
		return fmt.Errorf("%w: commit %d beyond last entry %d", ErrCorrupt, w.hs.Commit, w.lastIndex())
	}
	return nil
}

// readRecord returns errTornTail when the file ends mid-record or the final record
// fails its checksum — the signature of a crash during append.
func readRecord(br *bufio.Reader) (typ byte, payload []byte, n int64, err error) {
	var hdr [8]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		if err == io.EOF {
			return 0, nil, 0, io.EOF
		}
		return 0, nil, 0, errTornTail
	}
	size := binary.BigEndian.Uint32(hdr[:4])
	if size == 0 || size > maxRecord {
		if _, perr := br.Peek(1); perr == io.EOF {
			return 0, nil, 0, errTornTail
		}
		return 0, nil, 0, fmt.Errorf("bad record size %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(br, buf); err != nil {
		return 0, nil, 0, errTornTail
	}
	if crc32.Checksum(buf, crcTable) != binary.BigEndian.Uint32(hdr[4:]) {
		if _, perr := br.Peek(1); perr == io.EOF {
			return 0, nil, 0, errTornTail
		}
		return 0, nil, 0, errors.New("checksum mismatch")
	}
	return buf[0], buf[1:], int64(8 + size), nil
}

func appendRecord(bw *bufio.Writer, typ byte, payload []byte) {
	var hdr [8]byte
	body := append([]byte{typ}, payload...)
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(body)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.Checksum(body, crcTable))
	bw.Write(hdr[:])
	bw.Write(body)
}

func (w *WAL) openAppend() error {
	f, err := os.OpenFile(w.path(walName), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	syncDir(w.dir)
	w.f, w.bw = f, bufio.NewWriterSize(f, 1<<16)
	return nil
}

// Save durably records a Ready batch. A received snapshot is written first, so the
// log never refers to entries the snapshot replaced without the snapshot existing.
func (w *WAL) Save(hs *raft.HardState, ents []raft.Entry, snap *raft.Snapshot) error {
	if snap != nil {
		if err := w.writeSnapshot(snap); err != nil {
			return err
		}
		w.snap, w.ents = snap, nil
		// Drop the old log: the snapshot supersedes all of it.
		if err := w.rewrite(); err != nil {
			return err
		}
	}
	for _, e := range ents {
		if err := w.addEntry(e); err != nil {
			return err
		}
		appendRecord(w.bw, recEntry, raft.EncodeEntry(e))
	}
	if hs != nil {
		w.hs = *hs
		appendRecord(w.bw, recHardState, raft.EncodeHardState(*hs))
	}
	if len(ents) == 0 && hs == nil {
		return nil
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	t0 := time.Now()
	err := w.f.Sync()
	metrics.FsyncLatency.Observe(time.Since(t0))
	return err
}

// Compact persists a locally taken snapshot and drops the entries it covers.
func (w *WAL) Compact(snap *raft.Snapshot) error {
	if err := w.writeSnapshot(snap); err != nil {
		return err
	}
	w.snap = snap
	var keep []raft.Entry
	for _, e := range w.ents {
		if e.Index > snap.Meta.Index {
			keep = append(keep, e)
		}
	}
	w.ents = keep
	return w.rewrite()
}

func (w *WAL) writeSnapshot(s *raft.Snapshot) error {
	body := raft.EncodeSnapshot(s)
	b := binary.BigEndian.AppendUint32(nil, crc32.Checksum(body, crcTable))
	return writeAtomic(w.path(snapName), append(b, body...))
}

// rewrite replaces the WAL with the current hard state and entries.
func (w *WAL) rewrite() error {
	var buf bufWriter
	bw := bufio.NewWriter(&buf)
	for _, e := range w.ents {
		appendRecord(bw, recEntry, raft.EncodeEntry(e))
	}
	appendRecord(bw, recHardState, raft.EncodeHardState(w.hs))
	bw.Flush()
	if w.f != nil {
		w.bw.Flush()
		w.f.Close()
	}
	if err := writeAtomic(w.path(walName), buf.b); err != nil {
		return err
	}
	return w.openAppend()
}

func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.bw.Flush()
	if serr := w.f.Sync(); err == nil {
		err = serr
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

type bufWriter struct{ b []byte }

func (b *bufWriter) Write(p []byte) (int, error) { b.b = append(b.b, p...); return len(p), nil }

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// syncDir makes renames/creations durable. Best effort: unsupported on Windows.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
