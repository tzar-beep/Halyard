package admin_test

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tzar-beep/redis/internal/cluster"
	"github.com/tzar-beep/redis/internal/command"
	"github.com/tzar-beep/redis/internal/raft"
	"github.com/tzar-beep/redis/internal/server"
)

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func cmd(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func do(t *testing.T, c net.Conn, br *bufio.Reader, args ...string) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write([]byte(cmd(args...)))
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line[0] == '$' {
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		io.ReadFull(br, buf)
		return string(buf[:n])
	}
	return line
}

func TestACLInfoMetrics(t *testing.T) {
	addr, httpAddr := freeAddr(t), freeAddr(t)
	s, err := server.New(server.Config{Addr: addr, Password: "adminpw", HTTPAddr: httpAddr, Users: []command.User{
		{Name: "reader", Password: "r", Role: command.RoleRead},
		{Name: "writer", Password: "w", Role: command.RoleWrite},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", addr)
	go s.Serve(ln)
	defer s.Close()

	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	br := bufio.NewReader(c)
	steps := []struct {
		args []string
		want string
	}{
		{[]string{"GET", "k"}, "-NOAUTH Authentication required."},
		{[]string{"AUTH", "reader", "wrong"}, "-WRONGPASS invalid username-password pair or user is disabled."},
		{[]string{"AUTH", "reader", "r"}, "+OK"},
		{[]string{"GET", "k"}, "(nil)"},
		{[]string{"SET", "k", "v"}, "-NOPERM User reader has no permissions to run the 'set' command"},
		{[]string{"AUTH", "writer", "w"}, "+OK"},
		{[]string{"SET", "k", "v"}, "+OK"},
		{[]string{"FLUSHALL"}, "-NOPERM User writer has no permissions to run the 'flushall' command"},
		{[]string{"AUTH", "adminpw"}, "+OK"},
		{[]string{"FLUSHALL"}, "+OK"},
	}
	for _, st := range steps {
		if got := do(t, c, br, st.args...); got != st.want {
			t.Fatalf("%v: got %q want %q", st.args, got, st.want)
		}
	}
	if info := do(t, c, br, "INFO"); !strings.Contains(info, "redis_version:") || !strings.Contains(info, "db0:keys=0") {
		t.Fatalf("INFO: %q", info)
	}
	resp, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`redis_commands_total{cmd="set"}`, "redis_command_duration_seconds_bucket", "redis_connections 1"} {
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("/metrics missing %q:\n%s", want, body)
		}
	}
}

func TestDemoAPIKillLeader(t *testing.T) {
	var peers []cluster.Peer
	for i := 1; i <= 3; i++ {
		peers = append(peers, cluster.Peer{ID: raft.NodeID(i), RaftAddr: freeAddr(t), ClientAddr: freeAddr(t)})
	}
	var https []string
	for _, p := range peers {
		h := freeAddr(t)
		https = append(https, h)
		s, err := server.New(server.Config{Addr: p.ClientAddr, HTTPAddr: h, Demo: true, CORSOrigin: "*",
			Cluster: &cluster.Config{Self: p, Peers: peers, Dir: t.TempDir(), Tick: 20 * time.Millisecond}})
		if err != nil {
			t.Fatal(err)
		}
		ln, _ := net.Listen("tcp", p.ClientAddr)
		go s.Serve(ln)
		t.Cleanup(func() { s.Close() })
	}
	type status struct {
		ID     uint64 `json:"id"`
		Groups []struct {
			State string `json:"state"`
			Term  uint64 `json:"term"`
		} `json:"groups"`
	}
	leader := func() (int, uint64) {
		for i, h := range https {
			r, err := http.Get("http://" + h + "/api/status")
			if err != nil {
				continue
			}
			var st status
			json.NewDecoder(r.Body).Decode(&st)
			r.Body.Close()
			if len(st.Groups) > 0 && st.Groups[0].State == "leader" {
				return i, st.Groups[0].Term
			}
		}
		return -1, 0
	}
	var li int
	var term uint64
	for deadline := time.Now().Add(5 * time.Second); ; {
		if li, term = leader(); li >= 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	post := func(h, path, body string) map[string]any {
		r, err := http.Post("http://"+h+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		if r.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Fatal("missing CORS header")
		}
		var out map[string]any
		json.NewDecoder(r.Body).Decode(&out)
		return out
	}
	if got := post(https[li], "/api/command", `{"args":["SET","demo","hello"]}`)["reply"]; got != "OK" {
		t.Fatalf("SET via API: %v", got)
	}
	if got := post(https[li], "/api/command", `{"args":["FLUSHALL"]}`)["reply"]; !strings.Contains(fmt.Sprint(got), "disabled") {
		t.Fatalf("FLUSHALL not blocked: %v", got)
	}
	if got := post(https[li], "/api/load", `{"count":300}`); got["ok"] != float64(300) {
		t.Fatalf("load: %v", got)
	}
	r, err := http.Get("http://" + https[li] + "/api/log")
	if err != nil {
		t.Fatal(err)
	}
	var lg struct {
		Entries []struct{ Kind, Desc string } `json:"entries"`
	}
	json.NewDecoder(r.Body).Decode(&lg)
	r.Body.Close()
	if len(lg.Entries) == 0 || !strings.HasPrefix(lg.Entries[len(lg.Entries)-1].Desc, "SET load:") {
		t.Fatalf("log: %+v", lg)
	}
	// A partitioned leader must step down; then use pause for the failover check.
	post(https[li], "/api/fault", `{"action":"isolate","seconds":2}`)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if nl, nt := leader(); nl >= 0 && nl != li && nt > term {
			li, term = nl, nt
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no new leader after isolating the old one")
		}
	}
	time.Sleep(2500 * time.Millisecond) // partition heals
	post(https[li], "/api/fault", `{"action":"pause","seconds":3}`)
	start := time.Now()
	for {
		if nl, nt := leader(); nl >= 0 && nl != li && nt > term {
			t.Logf("new leader n%d (term %d) elected %v after pausing n%d", nl+1, nt, time.Since(start), li+1)
			if got := post(https[nl], "/api/command", `{"args":["GET","demo"]}`)["reply"]; got != `"hello"` {
				t.Fatalf("data after failover: %v", got)
			}
			return
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("no failover after pausing the leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTLS(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	kb, _ := x509.MarshalECPrivateKey(key)
	os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)

	addr := freeAddr(t)
	s, err := server.New(server.Config{Addr: addr, TLSCert: certFile, TLSKey: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	go s.ListenAndServe()
	defer s.Close()
	pool := x509.NewCertPool()
	pool.AddCert(func() *x509.Certificate { c, _ := x509.ParseCertificate(der); return c }())
	var c *tls.Conn
	for i := 0; i < 50; i++ {
		if c, err = tls.Dial("tcp", addr, &tls.Config{RootCAs: pool}); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := do(t, c, bufio.NewReader(c), "PING"); got != "+PONG" {
		t.Fatalf("PING over TLS: %q", got)
	}
}
