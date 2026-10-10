package integration

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tzar-beep/redis/internal/server"
)

func start(t *testing.T) string { return startWith(t, server.Config{}) }

func startWith(t *testing.T, cfg server.Config) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(cfg)
	go s.Serve(ln)
	t.Cleanup(func() { s.Close() })
	return ln.Addr().String()
}

func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, bufio.NewReader(c)
}

// roundtrip writes raw bytes and expects exactly want back.
func roundtrip(t *testing.T, c net.Conn, r *bufio.Reader, req, want string) {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read: %v (got %q)", err, got)
	}
	if string(got) != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBasicCommands(t *testing.T) {
	c, r := dial(t, start(t))
	roundtrip(t, c, r, "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	roundtrip(t, c, r, "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", "$5\r\nhello\r\n")
	roundtrip(t, c, r, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$-1\r\n")
	roundtrip(t, c, r, "*3\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n", "+OK\r\n")
	roundtrip(t, c, r, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$1\r\nv\r\n")
	roundtrip(t, c, r, "*1\r\n$3\r\nGET\r\n", "-ERR wrong number of arguments for 'get' command\r\n")
	roundtrip(t, c, r, "*1\r\n$3\r\nFOO\r\n", "-ERR unknown command 'FOO', with args beginning with: \r\n")
	roundtrip(t, c, r, "PING\r\n", "+PONG\r\n")
}

func TestAuth(t *testing.T) {
	c, r := dial(t, startWith(t, server.Config{Password: "s3cret"}))
	roundtrip(t, c, r, "*1\r\n$4\r\nPING\r\n", "-NOAUTH Authentication required.\r\n")
	roundtrip(t, c, r, "*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n", "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
	roundtrip(t, c, r, "*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n", "+OK\r\n")
	roundtrip(t, c, r, "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
}

func TestPipelining(t *testing.T) {
	c, r := dial(t, start(t))
	roundtrip(t, c, r, strings.Repeat("*1\r\n$4\r\nPING\r\n", 1000), strings.Repeat("+PONG\r\n", 1000))
}

func TestSplitWrites(t *testing.T) {
	c, r := dial(t, start(t))
	c.Write([]byte("*1\r\n$4\r\nPI"))
	time.Sleep(20 * time.Millisecond)
	roundtrip(t, c, r, "NG\r\n", "+PONG\r\n")
}

func TestMalformedIsolated(t *testing.T) {
	addr := start(t)
	good, gr := dial(t, addr)
	bad, br := dial(t, addr)
	roundtrip(t, bad, br, "*1\r\n$x\r\n", "-ERR Protocol error: invalid length\r\n")
	roundtrip(t, good, gr, "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
}

func TestConcurrentClients(t *testing.T) {
	addr := start(t)
	const req = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	const want = "+OK\r\n$1\r\nv\r\n"
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			for j := 0; j < 20; j++ {
				c.Write([]byte(req))
				buf := make([]byte, len(want))
				if _, err := io.ReadFull(r, buf); err != nil || string(buf) != want {
					t.Errorf("got %q err %v", buf, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
