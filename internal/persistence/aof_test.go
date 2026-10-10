package persistence

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func collect(t *testing.T, path string) ([]string, error) {
	t.Helper()
	var got []string
	err := Replay(path, func(args [][]byte) error {
		var parts []string
		for _, a := range args {
			parts = append(parts, string(a))
		}
		got = append(got, strings.Join(parts, " "))
		return nil
	})
	return got, err
}

func rec(s ...string) [][]byte {
	out := make([][]byte, len(s))
	for i, x := range s {
		out[i] = []byte(x)
	}
	return out
}

func TestAppendAndReplay(t *testing.T) {
	for _, p := range []Policy{Always, EverySec, No} {
		path := filepath.Join(t.TempDir(), "a.aof")
		a, err := Open(path, p, func([][]byte) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		a.Append(rec("SET", "k", "v\r\nwith\x00binary"))
		a.Append(rec("DEL", "k"))
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := collect(t, path)
		if err != nil || len(got) != 2 || got[0] != "SET k v\r\nwith\x00binary" || got[1] != "DEL k" {
			t.Fatalf("policy %d: got %q, %v", p, got, err)
		}
	}
}

func TestTruncatedTailRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.aof")
	good := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
	os.WriteFile(path, []byte(good+"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1"), 0o644)
	got, err := collect(t, path)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %q, %v", got, err)
	}
	if b, _ := os.ReadFile(path); string(b) != good {
		t.Fatalf("file not truncated to last good record: %q", b)
	}
	// Appending after repair yields a valid log.
	a, err := Open(path, Always, func([][]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	a.Append(rec("SET", "c", "3"))
	a.Close()
	if got, err := collect(t, path); err != nil || len(got) != 2 {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestCorruptMiddleRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.aof")
	os.WriteFile(path, []byte("*1\r\n$3\r\nDEL\r\n*1\r\n$x\r\n*1\r\n$3\r\nDEL\r\n"), 0o644)
	if _, err := collect(t, path); err == nil || !strings.Contains(err.Error(), "offset 13") {
		t.Fatalf("want corruption error at offset 13, got %v", err)
	}
}

func TestApplyErrorRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.aof")
	os.WriteFile(path, []byte("*1\r\n$3\r\nBAD\r\n"), 0o644)
	err := Replay(path, func([][]byte) error { return errors.New("unknown command") })
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteFailureIsSticky(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.aof")
	a, err := Open(path, Always, func([][]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	a.f.Close() // simulate a dead disk
	if err := a.Append(rec("SET", "k", "v")); err == nil {
		t.Fatal("append to closed file succeeded")
	}
	if a.Err() == nil {
		t.Fatal("failure not recorded")
	}
	if err := a.Append(rec("SET", "k", "v")); err == nil {
		t.Fatal("append after failure succeeded")
	}
}
