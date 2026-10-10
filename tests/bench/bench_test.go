// Package bench holds in-process benchmarks of the full server path (TCP, parser,
// dispatch, store, replies) for profiling with -cpuprofile / -memprofile.
package bench

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/tzar-beep/Halyard/internal/persistence"
	"github.com/tzar-beep/Halyard/internal/server"
)

func start(b *testing.B, cfg server.Config) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	s, err := server.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	go s.Serve(ln)
	b.Cleanup(func() { s.Close() })
	return ln.Addr().String()
}

func cmd(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	return sb.String()
}

// run drives b.N commands over `conns` connections, `pipeline` at a time.
func run(b *testing.B, addr string, conns, pipeline int, mk func(i int) string, replyLen int) {
	var wg sync.WaitGroup
	per := b.N/conns + 1
	b.ResetTimer()
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", addr)
			if err != nil {
				b.Error(err)
				return
			}
			defer nc.Close()
			br := bufio.NewReaderSize(nc, 64<<10)
			buf := make([]byte, replyLen*pipeline)
			var batch strings.Builder
			for done := 0; done < per; done += pipeline {
				batch.Reset()
				for p := 0; p < pipeline; p++ {
					batch.WriteString(mk(c*per + done + p))
				}
				if _, err := io.WriteString(nc, batch.String()); err != nil {
					b.Error(err)
					return
				}
				if _, err := io.ReadFull(br, buf); err != nil {
					b.Error(err)
					return
				}
			}
		}(c)
	}
	wg.Wait()
}

var setCmds = func() []string {
	out := make([]string, 10000)
	for i := range out {
		out[i] = cmd("SET", fmt.Sprintf("key:%d", i), "xxxxxxxxxxxxxxxxxxxx")
	}
	return out
}()

func BenchmarkSetPipelined(b *testing.B) {
	addr := start(b, server.Config{})
	run(b, addr, 50, 16, func(i int) string { return setCmds[i%len(setCmds)] }, len("+OK\r\n"))
}

func BenchmarkSetPipelinedAOFEverysec(b *testing.B) {
	addr := start(b, server.Config{AppendOnly: true, Dir: b.TempDir(), AppendFsync: persistence.EverySec})
	run(b, addr, 50, 16, func(i int) string { return setCmds[i%len(setCmds)] }, len("+OK\r\n"))
}

func BenchmarkSetAOFAlways(b *testing.B) {
	addr := start(b, server.Config{AppendOnly: true, Dir: b.TempDir(), AppendFsync: persistence.Always})
	run(b, addr, 50, 1, func(i int) string { return setCmds[i%len(setCmds)] }, len("+OK\r\n"))
}

func BenchmarkGetPipelined(b *testing.B) {
	addr := start(b, server.Config{})
	get := cmd("GET", "k")
	run(b, addr, 1, 1, func(int) string { return cmd("SET", "k", "xxxxxxxxxxxxxxxxxxxx") }, len("+OK\r\n"))
	run(b, addr, 50, 16, func(int) string { return get }, len("$20\r\nxxxxxxxxxxxxxxxxxxxx\r\n"))
}
