package persistence

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tzar-beep/Halyard/internal/store"
)

func readAll(t *testing.T, path string) (uint64, map[string]store.Entry, error) {
	t.Helper()
	got := map[string]store.Entry{}
	gen, err := ReadSnapshot(path, func(e store.Entry) { got[e.Key] = e })
	return gen, got, err
}

func TestSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.snap")
	future := time.Now().UnixMilli() + 60_000
	in := []store.Entry{
		{Key: "plain", Obj: store.NewString([]byte("v"))},
		{Key: "bin\x00\r\nkey", Obj: store.NewString([]byte("\x00\xff\r\n"))},
		{Key: "empty", Obj: store.NewString([]byte{})},
		{Key: "ttl", Obj: store.NewString([]byte("x")), ExpireAt: future},
		{Key: "expired", Obj: store.NewString([]byte("x")), ExpireAt: 1},
	}
	if err := WriteSnapshot(path, 7, in); err != nil {
		t.Fatal(err)
	}
	gen, got, err := readAll(t, path)
	if err != nil || gen != 7 {
		t.Fatalf("gen %d err %v", gen, err)
	}
	if len(got) != 4 || string(got["bin\x00\r\nkey"].Obj.Str) != "\x00\xff\r\n" || got["ttl"].ExpireAt != future {
		t.Fatalf("got %+v", got)
	}
	if _, ok := got["expired"]; ok {
		t.Fatal("expired key loaded")
	}
}

func TestSnapshotEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.snap")
	if err := WriteSnapshot(path, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, got, err := readAll(t, path); err != nil || len(got) != 0 {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestSnapshotCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.snap")
	WriteSnapshot(path, 1, []store.Entry{{Key: "k", Obj: store.NewString([]byte("value"))}})
	orig, _ := os.ReadFile(path)

	for i := range orig { // flip every byte in turn
		b := append([]byte(nil), orig...)
		b[i] ^= 0x41
		os.WriteFile(path, b, 0o644)
		if _, _, err := readAll(t, path); !errors.Is(err, ErrBadSnapshot) {
			t.Fatalf("flip at %d: want ErrBadSnapshot, got %v", i, err)
		}
	}
	for n := 0; n < len(orig); n++ { // every truncation
		os.WriteFile(path, orig[:n], 0o644)
		if _, _, err := readAll(t, path); err == nil {
			t.Fatalf("truncated to %d accepted", n)
		}
	}
}

func TestSnapshotLarge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	path := filepath.Join(t.TempDir(), "s.snap")
	const n = 500_000
	in := make([]store.Entry, n)
	for i := range in {
		in[i] = store.Entry{Key: fmt.Sprintf("key:%d", i), Obj: store.NewString([]byte(fmt.Sprintf("value:%d", i)))}
	}
	t0 := time.Now()
	if err := WriteSnapshot(path, 1, in); err != nil {
		t.Fatal(err)
	}
	w := time.Since(t0)
	t0 = time.Now()
	count := 0
	if _, err := ReadSnapshot(path, func(store.Entry) { count++ }); err != nil || count != n {
		t.Fatalf("count %d err %v", count, err)
	}
	st, _ := os.Stat(path)
	t.Logf("%d keys, %.1f MB: write %v, load %v", n, float64(st.Size())/1e6, w, time.Since(t0))
}

// --- Manager ---

// fakeDB is a minimal keyspace for driving the manager without the command layer.
type fakeDB struct {
	mu   sync.Mutex
	wmu  sync.Mutex
	data map[string]string
}

func newFakeDB() *fakeDB { return &fakeDB{data: map[string]string{}} }

func (f *fakeDB) hooks() Hooks {
	return Hooks{
		Restore: func(e store.Entry) { f.data[e.Key] = string(e.Obj.Str) },
		Apply: func(args [][]byte) error {
			switch string(args[0]) {
			case "SET":
				f.data[string(args[1])] = string(args[2])
			case "DEL":
				delete(f.data, string(args[1]))
			default:
				return fmt.Errorf("unknown %s", args[0])
			}
			return nil
		},
		Capture: func() []store.Entry {
			f.mu.Lock()
			defer f.mu.Unlock()
			var out []store.Entry
			for k, v := range f.data {
				out = append(out, store.Entry{Key: k, Obj: store.NewString([]byte(v))})
			}
			return out
		},
		Freeze: func(fn func()) { f.wmu.Lock(); defer f.wmu.Unlock(); fn() },
	}
}

func (f *fakeDB) set(t *testing.T, m *Manager, k, v string) {
	t.Helper()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	f.mu.Lock()
	f.data[k] = v
	f.mu.Unlock()
	if _, err := m.Append(rec("SET", k, v)); err != nil {
		t.Fatal(err)
	}
}

func openDB(t *testing.T, dir string, autosave int64) (*Manager, *fakeDB) {
	t.Helper()
	db := newFakeDB()
	m, err := Open(Options{Dir: dir, Policy: Always, AutoSaveBytes: autosave}, db.hooks())
	if err != nil {
		t.Fatal(err)
	}
	return m, db
}

func files(dir string) []string {
	ents, _ := os.ReadDir(dir)
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func TestManagerSnapshotThenAOF(t *testing.T) {
	dir := t.TempDir()
	m, db := openDB(t, dir, 0)
	db.set(t, m, "a", "1")
	db.set(t, m, "b", "1")
	if err := m.Save(); err != nil {
		t.Fatal(err)
	}
	db.set(t, m, "b", "2") // after snapshot: lives only in segment 2
	db.set(t, m, "c", "3")
	m.Close()

	if got := fmt.Sprint(files(dir)); got != "[appendonly.2.aof dump.snap]" {
		t.Fatalf("files: %s", got)
	}
	m, db = openDB(t, dir, 0)
	defer m.Close()
	if fmt.Sprint(db.data) != "map[a:1 b:2 c:3]" {
		t.Fatalf("recovered %v", db.data)
	}
}

func TestManagerAutoSaveBoundsDisk(t *testing.T) {
	dir := t.TempDir()
	m, db := openDB(t, dir, 512)
	for i := 0; i < 2000; i++ {
		db.set(t, m, fmt.Sprintf("k%d", i%50), fmt.Sprint(i))
	}
	m.Close()
	segs := 0
	for _, f := range files(dir) {
		if filepath.Ext(f) == ".aof" {
			segs++
		}
	}
	if segs > 2 {
		t.Fatalf("old segments not cleaned: %v", files(dir))
	}
	m, db = openDB(t, dir, 512)
	defer m.Close()
	if len(db.data) != 50 || db.data["k49"] != "1999" {
		t.Fatalf("recovered %d keys, k49=%s", len(db.data), db.data["k49"])
	}
}

// Crash after rotation but before the snapshot rename: the old snapshot, a leftover
// temp file and both segments exist. Everything must be recovered.
func TestManagerCrashBeforeRename(t *testing.T) {
	dir := t.TempDir()
	m, db := openDB(t, dir, 0)
	db.set(t, m, "a", "1")
	m.Save() // dump.snap gen 2
	db.set(t, m, "b", "2")
	db.Freeze(func() { m.rotate() }) // segment 3 created, snapshot never written
	db.set(t, m, "c", "3")
	m.Close()
	os.WriteFile(filepath.Join(dir, "dump.snap.tmp"), []byte("partial"), 0o644)

	m, db = openDB(t, dir, 0)
	defer m.Close()
	if fmt.Sprint(db.data) != "map[a:1 b:2 c:3]" {
		t.Fatalf("recovered %v", db.data)
	}
	if _, err := os.Stat(filepath.Join(dir, "dump.snap.tmp")); err == nil {
		t.Fatal("temp file not cleaned")
	}
}

// Crash after the new snapshot was renamed into place but before old segments were
// deleted: stale segments must be ignored (not replayed twice) and removed.
func TestManagerCrashBeforeCleanup(t *testing.T) {
	dir := t.TempDir()
	m, db := openDB(t, dir, 0)
	db.set(t, m, "n", "1")
	db.set(t, m, "gone", "x")
	m.Close()
	stale, _ := os.ReadFile(filepath.Join(dir, "appendonly.1.aof"))

	m, db = openDB(t, dir, 0)
	db.Freeze(func() { db.hooks().Apply(rec("DEL", "gone")); m.Append(rec("DEL", "gone")) })
	m.Save()
	m.Close()
	os.WriteFile(filepath.Join(dir, "appendonly.1.aof"), stale, 0o644) // resurrect stale segment

	m, db = openDB(t, dir, 0)
	defer m.Close()
	if _, ok := db.data["gone"]; ok {
		t.Fatal("stale segment replayed over newer snapshot")
	}
	if _, err := os.Stat(filepath.Join(dir, "appendonly.1.aof")); err == nil {
		t.Fatal("stale segment not removed")
	}
}

func TestManagerMissingSegment(t *testing.T) {
	dir := t.TempDir()
	m, db := openDB(t, dir, 0)
	db.set(t, m, "a", "1")
	m.Save()
	db.Freeze(func() { m.rotate() })
	db.set(t, m, "b", "1")
	m.Close()
	os.Remove(filepath.Join(dir, "appendonly.2.aof"))
	if _, err := Open(Options{Dir: dir}, newFakeDB().hooks()); err == nil {
		t.Fatal("gap in segments accepted")
	}
}

func TestManagerCorruptSnapshotRefused(t *testing.T) {
	dir := t.TempDir()
	m, db := openDB(t, dir, 0)
	db.set(t, m, "a", "1")
	m.Save()
	m.Close()
	p := filepath.Join(dir, "dump.snap")
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 1
	os.WriteFile(p, b, 0o644)
	if _, err := Open(Options{Dir: dir}, newFakeDB().hooks()); !errors.Is(err, ErrBadSnapshot) {
		t.Fatalf("want ErrBadSnapshot, got %v", err)
	}
}

func TestManagerLegacyMigration(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "appendonly.aof"), []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"), 0o644)
	m, db := openDB(t, dir, 0)
	defer m.Close()
	if db.data["a"] != "1" {
		t.Fatal("legacy AOF not loaded")
	}
}

func (f *fakeDB) Freeze(fn func()) { f.hooks().Freeze(fn) }
