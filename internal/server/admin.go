package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tzar-beep/redis/internal/cluster"
	"github.com/tzar-beep/redis/internal/command"
	"github.com/tzar-beep/redis/internal/metrics"
	"github.com/tzar-beep/redis/internal/raft"
	"github.com/tzar-beep/redis/internal/resp"
)

const Version = "1.0.0"

// --- INFO ---

func (s *Server) info(w *resp.Writer, args [][]byte) {
	section := "all"
	if len(args) > 1 {
		section = strings.ToLower(string(args[1]))
	}
	var b strings.Builder
	want := func(name string) bool { return section == "all" || section == "default" || section == name }
	if want("server") {
		fmt.Fprintf(&b, "# Server\r\nredis_version:%s\r\nimplementation:halyard\r\nmode:%s\r\ngo_version:%s\r\nos:%s/%s\r\nuptime_in_seconds:%.0f\r\ntcp_addr:%s\r\n\r\n",
			Version, s.mode(), runtime.Version(), runtime.GOOS, runtime.GOARCH, metrics.Uptime().Seconds(), s.addr)
	}
	if want("clients") {
		fmt.Fprintf(&b, "# Clients\r\nconnected_clients:%d\r\ntotal_connections_received:%d\r\n\r\n",
			metrics.ConnectionsCurrent.Load(), metrics.ConnectionsTotal.Load())
	}
	if want("stats") {
		var total uint64
		for _, n := range metrics.CommandCounts() {
			total += n
		}
		fmt.Fprintf(&b, "# Stats\r\ntotal_commands_processed:%d\r\nlatency_p50_us:%d\r\nlatency_p99_us:%d\r\nprotocol_errors:%d\r\n\r\n",
			total, metrics.CommandLatency.Quantile(0.5).Microseconds(), metrics.CommandLatency.Quantile(0.99).Microseconds(), metrics.ProtocolErrors.Load())
	}
	if want("persistence") {
		fmt.Fprintf(&b, "# Persistence\r\naof_enabled:%d\r\n", b2i(s.pm != nil))
		if s.pm != nil {
			fmt.Fprintf(&b, "aof_last_write_status:%s\r\nrdb_last_save_time:%d\r\n", okErr(s.pm.Err()), s.pm.LastSave())
		}
		fmt.Fprintf(&b, "fsync_p99_us:%d\r\n\r\n", metrics.FsyncLatency.Quantile(0.99).Microseconds())
	}
	if want("replication") || want("raft") {
		b.WriteString("# Replication\r\n")
		for _, g := range s.raftGroups() {
			st := g.node.CurrentStatus()
			fmt.Fprintf(&b, "group%d:role=%s,term=%d,leader=%d,commit=%d,applied=%d,snapshot=%d,voters=%d\r\n",
				g.id, st.State, st.Term, st.Leader, st.Commit, st.Applied, st.SnapIndex, len(st.Voters))
		}
		if len(s.raftGroups()) == 0 {
			b.WriteString("role:master\r\nconnected_slaves:0\r\n")
		}
		b.WriteString("\r\n")
	}
	if want("keyspace") {
		keys, expires := s.keyStats()
		fmt.Fprintf(&b, "# Keyspace\r\ndb0:keys=%d,expires=%d\r\n", keys, expires)
	}
	w.Bulk([]byte(b.String()))
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func okErr(err error) string {
	if err != nil {
		return "err"
	}
	return "ok"
}

func (s *Server) mode() string {
	switch {
	case s.router != nil:
		return "sharded"
	case s.node != nil:
		return "cluster"
	}
	return "standalone"
}

type groupRef struct {
	id   uint32
	node *cluster.Node
}

func (s *Server) raftGroups() []groupRef {
	switch {
	case s.router != nil:
		out := []groupRef{{0, s.router.Meta()}}
		for g := uint32(1); g <= uint32(len(s.router.Groups())); g++ {
			out = append(out, groupRef{g, s.router.Groups()[g].Node})
		}
		return out
	case s.node != nil:
		return []groupRef{{0, s.node}}
	}
	return nil
}

func (s *Server) keyStats() (keys, expires int) {
	if s.router != nil {
		for _, g := range s.router.Groups() {
			k, e := g.Store.Stats()
			keys, expires = keys+k, expires+e
		}
		return
	}
	return s.store.Stats()
}

// registerGauges exports Raft and keyspace state to /metrics.
func (s *Server) registerGauges() {
	metrics.RegisterGauges(func(w io.Writer) {
		keys, expires := s.keyStats()
		fmt.Fprintf(w, "# TYPE redis_keys gauge\nredis_keys %d\n# TYPE redis_keys_with_ttl gauge\nredis_keys_with_ttl %d\n", keys, expires)
		groups := s.raftGroups()
		if len(groups) == 0 {
			return
		}
		fmt.Fprintln(w, "# TYPE raft_term gauge\n# TYPE raft_is_leader gauge\n# TYPE raft_commit_index gauge\n# TYPE raft_applied_index gauge\n# TYPE raft_apply_lag gauge")
		for _, g := range groups {
			st := g.node.CurrentStatus()
			l := fmt.Sprintf("{group=\"%d\"}", g.id)
			fmt.Fprintf(w, "raft_term%s %d\nraft_is_leader%s %d\nraft_commit_index%s %d\nraft_applied_index%s %d\nraft_apply_lag%s %d\n",
				l, st.Term, l, b2i(st.State == raft.StateLeader), l, st.Commit, l, st.Applied, l, st.Commit-st.Applied)
		}
	})
}

// --- HTTP: /metrics, /healthz and the demo API ---

func (s *Server) startHTTP(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		metrics.Write(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("/api/status", s.cors(s.apiStatus))
	mux.HandleFunc("/api/log", s.cors(s.apiLog))
	mux.HandleFunc("/api/slots", s.cors(s.apiSlots))
	if s.cfg.Demo {
		mux.HandleFunc("/api/command", s.cors(s.limit(s.apiCommand)))
		mux.HandleFunc("/api/fault", s.cors(s.limit(s.apiFault)))
		mux.HandleFunc("/api/load", s.cors(s.limit(s.apiLoad)))
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go s.http.Serve(ln)
	log.Printf("http (metrics%s) listening on %s", map[bool]string{true: ", demo API", false: ""}[s.cfg.Demo], ln.Addr())
	return nil
}

func (s *Server) cors(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if o := s.cfg.CORSOrigin; o != "" {
			w.Header().Set("Access-Control-Allow-Origin", o)
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		}
		if r.Method == http.MethodOptions {
			return
		}
		h(w, r)
	}
}

// limit is a per-IP token bucket (10 req/s, burst 20) for the public demo API.
func (s *Server) limit(h http.HandlerFunc) http.HandlerFunc {
	type bucket struct {
		tokens float64
		last   time.Time
	}
	var mu sync.Mutex
	buckets := map[string]*bucket{}
	return func(w http.ResponseWriter, r *http.Request) {
		ip := r.Header.Get("X-Forwarded-For")
		if ip == "" {
			ip, _, _ = net.SplitHostPort(r.RemoteAddr)
		}
		ip = strings.TrimSpace(strings.Split(ip, ",")[0])
		mu.Lock()
		b := buckets[ip]
		if b == nil {
			if len(buckets) > 10000 {
				buckets = map[string]*bucket{}
			}
			b = &bucket{tokens: 20, last: time.Now()}
			buckets[ip] = b
		}
		b.tokens = min(20, b.tokens+time.Since(b.last).Seconds()*10)
		b.last = time.Now()
		allowed := b.tokens >= 1
		if allowed {
			b.tokens--
		}
		mu.Unlock()
		if !allowed {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		h(w, r)
	}
}

type groupStatus struct {
	Group      uint32 `json:"group"`
	State      string `json:"state"`
	Term       uint64 `json:"term"`
	Leader     uint64 `json:"leader"`
	LeaderAddr string `json:"leaderAddr"`
	Commit     uint64 `json:"commit"`
	Applied    uint64 `json:"applied"`
	Voters     int    `json:"voters"`
}

func (s *Server) apiStatus(w http.ResponseWriter, _ *http.Request) {
	keys, _ := s.keyStats()
	out := map[string]any{"mode": s.mode(), "addr": s.addr, "uptime": int(metrics.Uptime().Seconds()), "keys": keys,
		"p99_us": metrics.CommandLatency.Quantile(0.99).Microseconds()}
	var groups []groupStatus
	for _, g := range s.raftGroups() {
		st := g.node.CurrentStatus()
		out["id"] = st.ID
		if a := st.Peers[st.ID].ClientAddr; a != "" {
			out["addr"] = a // the advertised address, as used in MOVED replies
		}
		groups = append(groups, groupStatus{g.id, st.State.String(), st.Term, uint64(st.Leader),
			st.Peers[st.Leader].ClientAddr, st.Commit, st.Applied, len(st.Voters)})
	}
	out["groups"] = groups
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// demoBlocked commands are refused through the public web console.
var demoBlocked = map[string]bool{"flushall": true, "raft": true, "save": true, "bgsave": true,
	"auth": true, "multi": true, "exec": true, "watch": true, "keys": true}

// apiCommand runs one command for the web console: {"args": ["SET","k","v"]}.
func (s *Server) apiCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Args []string `json:"args"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil || len(req.Args) == 0 || len(req.Args) > 16 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	name := strings.ToLower(req.Args[0])
	reply := ""
	switch {
	case demoBlocked[name] || strings.HasPrefix(name, "shard.") || (name == "cluster" && len(req.Args) > 1 && strings.EqualFold(req.Args[1], "migrate")):
		reply = "(error) " + strings.ToUpper(name) + " is disabled in the public demo"
	default:
		args := make([][]byte, len(req.Args))
		for i, a := range req.Args {
			if len(a) > 512 {
				http.Error(w, "argument too long", http.StatusBadRequest)
				return
			}
			args[i] = []byte(a)
		}
		var buf bytes.Buffer
		rw := resp.NewWriter(&buf)
		sess := command.Session{Authed: true, Role: command.RoleWrite, User: "web"}
		if name == "info" {
			s.info(rw, args)
		} else {
			t0 := time.Now()
			if err := s.h.Dispatch(rw, &sess, args); err != nil {
				rw.Error("ERR " + err.Error())
			}
			n, known := command.Name(args[0])
			metrics.CommandLatency.Observe(time.Since(t0))
			metrics.CountCommand(n, known)
		}
		rw.Flush()
		reply = Render(bufio.NewReader(&buf), "")
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"reply": reply})
}

// apiFault: {"action":"pause","seconds":5} freezes this node's Raft groups, which
// looks like a crash to the rest of the cluster (demo "kill the leader").
func (s *Server) apiFault(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action  string `json:"action"`
		Seconds int    `json:"seconds"`
	}
	if r.Method != http.MethodPost || json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&req) != nil ||
		(req.Action != "pause" && req.Action != "isolate") || len(s.raftGroups()) == 0 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	d := time.Duration(max(1, min(req.Seconds, 15))) * time.Second
	if req.Action == "isolate" {
		// Network partition: the node keeps running but can't reach anyone.
		s.raftGroups()[0].node.Isolate(d)
	} else {
		for _, g := range s.raftGroups() {
			g.node.Pause(d)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"action": req.Action, "seconds": d.Seconds()})
}

// apiLoad: {"count":1000} runs that many writes through the normal command path
// with 32 concurrent workers and reports throughput (demo load generator).
func (s *Server) apiLoad(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count int `json:"count"`
	}
	if r.Method != http.MethodPost || json.NewDecoder(io.LimitReader(r.Body, 256)).Decode(&req) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	n := max(1, min(req.Count, 2000))
	var ok, failed atomic.Int64
	var wg sync.WaitGroup
	jobs := make(chan int)
	start := time.Now()
	for k := 0; k < 32; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess := command.Session{Authed: true, Role: command.RoleWrite, User: "load"}
			var buf bytes.Buffer
			for i := range jobs {
				buf.Reset()
				rw := resp.NewWriter(&buf)
				args := [][]byte{[]byte("SET"), []byte("load:" + strconv.Itoa(i%200)), []byte(strconv.Itoa(i))}
				t0 := time.Now()
				if err := s.h.Dispatch(rw, &sess, args); err != nil {
					failed.Add(1)
					continue
				}
				metrics.CommandLatency.Observe(time.Since(t0))
				metrics.CountCommand("set", true)
				rw.Flush()
				if bytes.HasPrefix(buf.Bytes(), []byte("+OK")) {
					ok.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	el := time.Since(start)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"ok": ok.Load(), "failed": failed.Load(), "ms": el.Milliseconds(),
		"opsPerSec": int(float64(ok.Load()) / max(el.Seconds(), 0.001))})
}

// apiLog: the most recent Raft log entries of the metadata/only group.
func (s *Server) apiLog(w http.ResponseWriter, _ *http.Request) {
	groups := s.raftGroups()
	if len(groups) == 0 {
		http.Error(w, "not in cluster mode", http.StatusNotFound)
		return
	}
	g := groups[len(groups)-1] // in sharded mode, show a data group
	if len(groups) == 1 {
		g = groups[0]
	}
	st := g.node.CurrentStatus()
	type ent struct {
		Index uint64 `json:"index"`
		Term  uint64 `json:"term"`
		Kind  string `json:"kind"`
		Desc  string `json:"desc"`
	}
	var out []ent
	for _, e := range g.node.RecentEntries(12) {
		kind, desc := cluster.DescribeEntry(e)
		out = append(out, ent{e.Index, e.Term, kind, desc})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"group": g.id, "commit": st.Commit, "applied": st.Applied, "entries": out})
}

// apiSlots: slot ranges and owning groups (sharded mode).
func (s *Server) apiSlots(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.router == nil {
		json.NewEncoder(w).Encode(map[string]any{"sharded": false})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"sharded": true, "ranges": s.router.SlotRanges()})
}

// Render formats a RESP reply like redis-cli.
func Render(br *bufio.Reader, indent string) string {
	line, err := br.ReadString('\n')
	if err != nil || len(line) < 3 {
		return "(error) bad reply"
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '+':
		return line[1:]
	case '-':
		return "(error) " + line[1:]
	case ':':
		return "(integer) " + line[1:]
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		io.ReadFull(br, buf)
		return strconv.Quote(string(buf[:n]))
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		if n == 0 {
			return "(empty array)"
		}
		var parts []string
		for i := 0; i < n; i++ {
			parts = append(parts, fmt.Sprintf("%s%d) %s", indent, i+1, Render(br, indent+"   ")))
		}
		return strings.TrimPrefix(strings.Join(parts, "\n"), indent)
	}
	return line
}
