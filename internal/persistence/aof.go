// Package persistence implements durability: an append-only file (AOF) of mutations
// plus point-in-time snapshots that let old AOF segments be discarded.
//
// AOF format: each successful mutation is stored as a RESP array, the same encoding
// clients send, so recovery reuses the protocol parser.
//
// Durability contract:
//   - always:   Append returns only after the record is written and fsynced.
//     An acknowledged write survives process and OS crashes (given a disk
//     that honours fsync).
//   - everysec: records are buffered and fsynced about once per second.
//     A crash can lose roughly the last second of acknowledged writes.
//   - no:       records are written to the OS on every append but never
//     fsynced by us; survives a process crash, not an OS crash.
//
// Any write or fsync error makes the AOF permanently failed (Err() != nil); the
// server then refuses writes instead of silently losing them.
package persistence

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tzar-beep/redis/internal/resp"
)

type Policy int

const (
	Always Policy = iota
	EverySec
	No
)

func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "always":
		return Always, nil
	case "everysec":
		return EverySec, nil
	case "no":
		return No, nil
	}
	return 0, fmt.Errorf("invalid appendfsync %q (want always|everysec|no)", s)
}

// AOF is one append-only log segment.
type AOF struct {
	mu     sync.Mutex
	f      *os.File
	cw     *countingWriter
	w      *resp.Writer
	policy Policy
	err    error
	dirty  bool // buffered or unsynced data exists (everysec)

	stop chan struct{}
	done chan struct{}
}

// OpenAOF opens (creating if needed) a segment for appending.
func OpenAOF(path string, policy Policy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	syncDir(filepath.Dir(path))
	cw := &countingWriter{w: f, n: st.Size()}
	a := &AOF{f: f, cw: cw, w: resp.NewWriter(cw), policy: policy, stop: make(chan struct{}), done: make(chan struct{})}
	if policy == EverySec {
		go a.syncLoop()
	} else {
		close(a.done)
	}
	return a, nil
}

// Replay applies every complete record in path; a missing file is empty. If
// repairTail is set, a partial final record (from a crash mid-write) is truncated
// away with a warning; otherwise it is an error. Corruption elsewhere is always an error.
func Replay(path string, repairTail bool, apply func(args [][]byte) error) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	cr := &countingReader{r: f}
	rd := resp.NewReader(cr)
	var good int64 // offset just past the last fully applied record
	n := 0
	for {
		args, err := rd.ReadCommand()
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if !repairTail {
				return fmt.Errorf("aof %s: truncated record at offset %d in a non-final segment", path, good)
			}
			log.Printf("aof %s: truncated record at offset %d; discarding %d trailing bytes", path, good, cr.n-good)
			if err := f.Truncate(good); err != nil {
				return fmt.Errorf("aof: truncate: %w", err)
			}
			if err := f.Sync(); err != nil {
				return fmt.Errorf("aof: sync after truncate: %w", err)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("aof %s: corrupt record at offset %d: %w", path, good, err)
		}
		if err := apply(args); err != nil {
			return fmt.Errorf("aof %s: record at offset %d rejected: %w", path, good, err)
		}
		good = cr.n - int64(rd.Buffered())
		n++
	}
	log.Printf("aof: loaded %d records from %s", n, path)
	return nil
}

// Append writes one record according to the fsync policy.
func (a *AOF) Append(args [][]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.w.ArrayHeader(len(args))
	for _, arg := range args {
		a.w.Bulk(arg)
	}
	switch a.policy {
	case Always:
		if err := a.w.Flush(); err != nil {
			return a.fail(err)
		}
		if err := a.f.Sync(); err != nil {
			return a.fail(err)
		}
	case No:
		if err := a.w.Flush(); err != nil {
			return a.fail(err)
		}
	case EverySec:
		a.dirty = true
	}
	return nil
}

// Err reports a persistent failure; once set, no further appends succeed.
func (a *AOF) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// Size returns bytes written to the file so far (excluding unflushed buffer).
func (a *AOF) Size() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cw.n
}

func (a *AOF) fail(err error) error {
	a.err = err
	log.Printf("aof: write failure, refusing further writes: %v", err)
	return err
}

// Sync flushes buffered records and fsyncs the file.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	if err := a.w.Flush(); err != nil {
		return a.fail(err)
	}
	if err := a.f.Sync(); err != nil {
		return a.fail(err)
	}
	a.dirty = false
	return nil
}

func (a *AOF) syncLoop() {
	defer close(a.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.mu.Lock()
			dirty := a.dirty
			a.mu.Unlock()
			if dirty {
				a.Sync()
			}
		}
	}
}

// Close flushes, fsyncs and closes the file.
func (a *AOF) Close() error {
	select {
	case <-a.stop:
		return nil
	default:
		close(a.stop)
	}
	<-a.done
	err := a.Sync()
	if cerr := a.f.Close(); err == nil {
		err = cerr
	}
	return err
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// syncDir fsyncs a directory so file creations/renames in it are durable.
// Best effort: not supported on Windows.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
