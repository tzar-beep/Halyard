package persistence

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tzar-beep/Halyard/internal/store"
)

// On-disk layout of the data directory:
//
//	dump.snap               snapshot; its header names generation G
//	appendonly.<N>.aof      AOF segments, N = G, G+1, ...
//
// Invariant: state = snapshot + replay of segments G..max in order.
//
// Save (snapshot) protocol:
//  1. With all writers blocked: capture the keyspace and rotate to a new segment
//     N+1. The captured state is exactly snapshot(old)+segments..N.
//  2. Writers resume, appending to N+1.
//  3. Write the capture as a snapshot with gen=N+1 (temp file, fsync, rename).
//  4. Delete segments < N+1.
//
// Crash before step 3's rename: the old snapshot and all segments are intact.
// Crash between 3 and 4: the new snapshot points at N+1; older segments are stale and
// are deleted on the next startup.
const (
	snapName   = "dump.snap"
	aofPrefix  = "appendonly."
	aofSuffix  = ".aof"
	legacyName = "appendonly.aof"
)

type Options struct {
	Dir    string
	Policy Policy
	// AutoSaveBytes triggers a background snapshot once the current segment grows
	// past this size, bounding disk usage and replay time. 0 disables.
	AutoSaveBytes int64
}

// Hooks connect the manager to the keyspace and the write path.
type Hooks struct {
	Restore func(store.Entry)         // load one snapshot record
	Apply   func(args [][]byte) error // replay one AOF record
	Capture func() []store.Entry      // point-in-time copy of the keyspace
	Freeze  func(fn func())           // run fn with all writers blocked
}

type Manager struct {
	opts  Options
	hooks Hooks

	aof atomic.Pointer[AOF]
	gen uint64 // current segment; only changed inside Freeze

	saving   atomic.Bool
	bg       sync.WaitGroup
	lastSave atomic.Int64 // unix seconds
	lastErr  atomic.Value // string
}

// Open recovers state from dir and opens the current segment for appending.
func Open(opts Options, h Hooks) (*Manager, error) {
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	m := &Manager{opts: opts, hooks: h}
	os.Remove(m.path(snapName) + ".tmp") // leftover from a crash mid-save
	if err := m.migrateLegacy(); err != nil {
		return nil, err
	}

	start := uint64(1)
	if _, err := os.Stat(m.path(snapName)); err == nil {
		t := time.Now()
		gen, err := ReadSnapshot(m.path(snapName), h.Restore)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", snapName, err)
		}
		start = gen
		log.Printf("snapshot: loaded gen %d in %v", gen, time.Since(t))
	}

	gens, err := m.segments()
	if err != nil {
		return nil, err
	}
	var live []uint64
	for _, g := range gens {
		if g < start {
			os.Remove(m.segPath(g)) // already covered by the snapshot
		} else {
			live = append(live, g)
		}
	}
	for i, g := range live {
		if g != start+uint64(i) {
			return nil, fmt.Errorf("missing AOF segment %d (found %v, snapshot gen %d)", start+uint64(i), live, start)
		}
		if err := Replay(m.segPath(g), i == len(live)-1, h.Apply); err != nil {
			return nil, err
		}
	}
	m.gen = start
	if len(live) > 0 {
		m.gen = live[len(live)-1]
	}
	a, err := OpenAOF(m.segPath(m.gen), opts.Policy)
	if err != nil {
		return nil, err
	}
	m.aof.Store(a)
	m.lastSave.Store(time.Now().Unix())
	return m, nil
}

// migrateLegacy renames a pre-snapshot single-file AOF to segment 1.
func (m *Manager) migrateLegacy() error {
	if _, err := os.Stat(m.path(legacyName)); err != nil {
		return nil
	}
	if _, err := os.Stat(m.segPath(1)); err == nil {
		return fmt.Errorf("both %s and %s exist", legacyName, filepath.Base(m.segPath(1)))
	}
	return os.Rename(m.path(legacyName), m.segPath(1))
}

// Append logs a record and may trigger a background snapshot. Callers serialize
// Append (the dispatcher's write lock), which also excludes rotation. The returned
// function waits for durability (see AOF.Append).
func (m *Manager) Append(args [][]byte) (func() error, error) {
	a := m.aof.Load()
	wait, err := a.Append(args)
	if err != nil {
		return nil, err
	}
	if m.opts.AutoSaveBytes > 0 && a.Size() >= m.opts.AutoSaveBytes {
		m.BGSave() // no-op if a save is already running
	}
	return wait, nil
}

func (m *Manager) Err() error { return m.aof.Load().Err() }

// Save takes a snapshot synchronously.
func (m *Manager) Save() error {
	if !m.saving.CompareAndSwap(false, true) {
		return errors.New("ERR Background save already in progress")
	}
	defer m.saving.Store(false)
	return m.save()
}

// BGSave starts a snapshot in the background. Returns false if one is already running.
func (m *Manager) BGSave() bool {
	if !m.saving.CompareAndSwap(false, true) {
		return false
	}
	m.bg.Add(1)
	go func() {
		defer m.bg.Done()
		defer m.saving.Store(false)
		if err := m.save(); err != nil {
			log.Printf("bgsave: %v", err)
		}
	}()
	return true
}

func (m *Manager) save() error {
	t := time.Now()
	var entries []store.Entry
	var newGen uint64
	var err error
	m.hooks.Freeze(func() {
		entries = m.hooks.Capture()
		newGen, err = m.rotate()
	})
	if err != nil {
		m.lastErr.Store(err.Error())
		return err
	}
	pause := time.Since(t)
	if err := WriteSnapshot(m.path(snapName), newGen, entries); err != nil {
		m.lastErr.Store(err.Error())
		return fmt.Errorf("write snapshot: %w", err)
	}
	gens, _ := m.segments()
	for _, g := range gens {
		if g < newGen {
			os.Remove(m.segPath(g))
		}
	}
	m.lastSave.Store(time.Now().Unix())
	m.lastErr.Store("")
	log.Printf("snapshot: %d keys, gen %d, writers paused %v, total %v", len(entries), newGen, pause, time.Since(t))
	return nil
}

// rotate switches appends to a new segment. Must run inside Freeze.
func (m *Manager) rotate() (uint64, error) {
	next := m.gen + 1
	na, err := OpenAOF(m.segPath(next), m.opts.Policy)
	if err != nil {
		return 0, err
	}
	old := m.aof.Swap(na)
	m.gen = next
	if err := old.Close(); err != nil {
		// The old segment may be missing records; the snapshot about to be written
		// covers them, so log and continue.
		log.Printf("aof: closing segment %d: %v", next-1, err)
	}
	return next, nil
}

// LastSave returns the unix time of the last successful snapshot (or startup).
func (m *Manager) LastSave() int64 { return m.lastSave.Load() }

// Close waits for a running snapshot, then flushes and closes the AOF.
func (m *Manager) Close() error {
	m.bg.Wait()
	return m.aof.Load().Close()
}

func (m *Manager) path(name string) string { return filepath.Join(m.opts.Dir, name) }

func (m *Manager) segPath(g uint64) string {
	return m.path(aofPrefix + strconv.FormatUint(g, 10) + aofSuffix)
}

// segments lists AOF segment generations in ascending order.
func (m *Manager) segments() ([]uint64, error) {
	ents, err := os.ReadDir(m.opts.Dir)
	if err != nil {
		return nil, err
	}
	var gens []uint64
	for _, e := range ents {
		n := e.Name()
		if !strings.HasPrefix(n, aofPrefix) || !strings.HasSuffix(n, aofSuffix) {
			continue
		}
		g, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, aofPrefix), aofSuffix), 10, 64)
		if err != nil {
			continue
		}
		gens = append(gens, g)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return gens, nil
}
