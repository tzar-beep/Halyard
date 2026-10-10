package integration

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tzar-beep/redis/internal/persistence"
	"github.com/tzar-beep/redis/internal/server"
)

// startServer runs an in-process server and returns its address and a stop func.
func startServer(t *testing.T, cfg server.Config) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := server.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	var once sync.Once
	stop := func() { once.Do(func() { s.Close() }) }
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

func TestRestartRecovery(t *testing.T) {
	for _, p := range []persistence.Policy{persistence.Always, persistence.EverySec, persistence.No} {
		cfg := server.Config{Dir: t.TempDir(), AppendOnly: true, AppendFsync: p}
		addr, stop := startServer(t, cfg)
		c, r := dial(t, addr)
		for _, s := range []struct{ req, want string }{
			{cmd("SET", "a", "1"), "+OK\r\n"},
			{cmd("MSET", "b", "2", "gone", "x"), "+OK\r\n"},
			{cmd("INCRBY", "a", "41"), ":42\r\n"},
			{cmd("SET", "ttl", "v", "EX", "1000"), "+OK\r\n"},
			{cmd("SET", "short", "v", "PX", "50"), "+OK\r\n"},
			{cmd("SET", "p", "v"), "+OK\r\n"},
			{cmd("EXPIRE", "p", "100"), ":1\r\n"},
			{cmd("PERSIST", "p"), ":1\r\n"},
			{cmd("DEL", "gone"), ":1\r\n"},
			{cmd("SET", "nx", "v", "NX"), "+OK\r\n"},
			{cmd("SET", "nx", "w", "NX"), "$-1\r\n"},
		} {
			roundtrip(t, c, r, s.req, s.want)
		}
		c.Close()
		stop()
		time.Sleep(60 * time.Millisecond) // let "short" expire while down

		addr, _ = startServer(t, cfg)
		c, r = dial(t, addr)
		for _, s := range []struct{ req, want string }{
			{cmd("GET", "a"), "$2\r\n42\r\n"},
			{cmd("GET", "b"), "$1\r\n2\r\n"},
			{cmd("EXISTS", "gone"), ":0\r\n"},
			{cmd("TTL", "ttl"), ":1000\r\n"},
			{cmd("EXISTS", "short"), ":0\r\n"},
			{cmd("TTL", "p"), ":-1\r\n"},
			{cmd("GET", "nx"), "$1\r\nv\r\n"},
		} {
			roundtrip(t, c, r, s.req, s.want)
		}
	}
}

func TestCorruptAOFRefusesStart(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "appendonly.aof"), []byte(cmd("SET", "a", "1")+"garbage\r\n"+cmd("SET", "b", "2")), 0o644)
	if _, err := server.New(server.Config{Dir: dir, AppendOnly: true}); err == nil {
		t.Fatal("server started on a corrupt AOF")
	}
}

func TestTruncatedAOFRecovers(t *testing.T) {
	dir := t.TempDir()
	full := cmd("SET", "b", "2")
	os.WriteFile(filepath.Join(dir, "appendonly.aof"), []byte(cmd("SET", "a", "1")+full[:len(full)-3]), 0o644)
	addr, _ := startServer(t, server.Config{Dir: dir, AppendOnly: true})
	c, r := dial(t, addr)
	roundtrip(t, c, r, cmd("MGET", "a", "b"), "*2\r\n$1\r\n1\r\n$-1\r\n")
}

// TestCrashLosesNoAckedWrites builds the real binary, hammers it with concurrent writes
// under appendfsync=always, kills it mid-stream, restarts it and checks that every
// write that received +OK was recovered.
func TestCrashLosesNoAckedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and kills a subprocess")
	}
	bin := filepath.Join(t.TempDir(), "server.exe")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/server").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	dir := t.TempDir()

	for round := 0; round < 3; round++ {
		addr, proc := spawn(t, bin, dir)
		var acked sync.Map
		var count atomic.Int64
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				c, err := net.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer c.Close()
				r := bufio.NewReader(c)
				for i := 0; ; i++ {
					k := fmt.Sprintf("r%d-w%d-%d", round, w, i)
					if _, err := c.Write([]byte(cmd("SET", k, k))); err != nil {
						return
					}
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if line == "+OK\r\n" {
						acked.Store(k, true)
						count.Add(1)
					}
				}
			}(w)
		}
		time.Sleep(300 * time.Millisecond)
		proc.Process.Kill() // SIGKILL / TerminateProcess: no graceful shutdown
		proc.Wait()
		wg.Wait()
		if count.Load() == 0 {
			t.Fatal("no writes acknowledged before kill")
		}

		addr, proc = spawn(t, bin, dir)
		c, r := dial(t, addr)
		missing := 0
		acked.Range(func(k, _ any) bool {
			c.SetDeadline(time.Now().Add(5 * time.Second))
			c.Write([]byte(cmd("EXISTS", k.(string))))
			if line, _ := r.ReadString('\n'); line != ":1\r\n" {
				missing++
			}
			return true
		})
		proc.Process.Kill()
		proc.Wait()
		t.Logf("round %d: %d acked writes, %d missing after kill -9", round, count.Load(), missing)
		if missing > 0 {
			t.Fatalf("lost %d acknowledged writes", missing)
		}
	}
}

func spawn(t *testing.T, bin, dir string) (string, *exec.Cmd) {
	t.Helper()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	p := exec.Command(bin, "-addr", addr, "-dir", dir, "-appendfsync", "always")
	p.Stderr = io.Discard
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Process.Kill() })
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return addr, p
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return "", nil
}
