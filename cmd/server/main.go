package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"bufio"

	"github.com/tzar-beep/redis/internal/cluster"
	"github.com/tzar-beep/redis/internal/command"
	"github.com/tzar-beep/redis/internal/persistence"
	"github.com/tzar-beep/redis/internal/raft"
	"github.com/tzar-beep/redis/internal/server"
)

func main() {
	cfg := server.Config{}
	var fsync string
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:6379", "listen address")
	flag.StringVar(&cfg.Password, "requirepass", os.Getenv("REDIS_PASSWORD"), "require AUTH with this password (default $REDIS_PASSWORD)")
	flag.StringVar(&cfg.Dir, "dir", "data", "data directory")
	flag.BoolVar(&cfg.AppendOnly, "appendonly", true, "enable the append-only file")
	flag.StringVar(&fsync, "appendfsync", "everysec", "AOF fsync policy: always|everysec|no")
	var nodeID uint64
	var clusterSpec, raftAddr string
	var join bool
	flag.Uint64Var(&nodeID, "id", 0, "cluster mode: this node's id (>0)")
	flag.StringVar(&clusterSpec, "cluster", "", "cluster mode: founding members as id=raft-addr/client-addr,... (include this node)")
	flag.StringVar(&raftAddr, "raft-addr", "", "cluster mode: address for Raft traffic (required with -join)")
	flag.IntVar(&cfg.Shards, "shards", 0, "cluster mode: number of Raft data groups to shard the keyspace over (0 = unsharded)")
	flag.BoolVar(&join, "join", false, "cluster mode: start empty and wait to be added with RAFT ADD")
	flag.Int64Var(&cfg.AutoSaveBytes, "autosave-bytes", 64<<20, "snapshot and start a new AOF segment once it exceeds this size (0 = off)")
	flag.StringVar(&cfg.TLSCert, "tls-cert", "", "serve clients over TLS with this certificate (PEM)")
	flag.StringVar(&cfg.TLSKey, "tls-key", "", "private key for -tls-cert")
	flag.StringVar(&cfg.HTTPAddr, "http", "", "address for /metrics, /healthz and /api/status (e.g. :8080)")
	flag.BoolVar(&cfg.Demo, "demo", false, "expose the rate-limited public demo API (/api/command, /api/fault)")
	flag.StringVar(&cfg.CORSOrigin, "cors", "", "Access-Control-Allow-Origin for the HTTP API")
	flag.Func("user", "ACL user as name:password:role (role read|write|admin); repeatable", func(v string) error {
		parts := strings.SplitN(v, ":", 3)
		if len(parts) != 3 {
			return fmt.Errorf("want name:password:role")
		}
		role, ok := command.ParseRole(parts[2])
		if !ok || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("bad user %q", parts[0])
		}
		cfg.Users = append(cfg.Users, command.User{Name: parts[0], Password: parts[1], Role: role})
		return nil
	})
	configFile := flag.String("config", "", "config file: one 'flag value' per line, # comments; command-line flags override it")
	flag.Parse()
	if *configFile != "" {
		if err := loadConfig(*configFile); err != nil {
			log.Fatal(err)
		}
		flag.Parse() // command-line values take precedence over the file
	}

	p, err := persistence.ParsePolicy(fsync)
	if err != nil {
		log.Fatal(err)
	}
	cfg.AppendFsync = p
	if nodeID != 0 {
		cc, err := clusterConfig(raft.NodeID(nodeID), clusterSpec, raftAddr, cfg.Addr, join)
		if err != nil {
			log.Fatal(err)
		}
		cc.Dir = filepath.Join(cfg.Dir, "raft")
		cfg.Cluster = cc
	}
	if cfg.Password == "" {
		log.Print("warning: no password set; do not expose this server publicly")
	}

	s, err := server.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Print("shutting down")
		if err := s.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
	<-closed // wait for the AOF to be flushed and closed
}

// clusterConfig parses "-cluster 1=10.0.0.1:7000/10.0.0.1:6379,2=..." for a founding
// node, or builds a joining node's config from -raft-addr.
func clusterConfig(id raft.NodeID, spec, raftAddr, clientAddr string, join bool) (*cluster.Config, error) {
	cc := &cluster.Config{Self: cluster.Peer{ID: id, RaftAddr: raftAddr, ClientAddr: clientAddr}}
	if join {
		if raftAddr == "" {
			return nil, fmt.Errorf("-join requires -raft-addr")
		}
		return cc, nil
	}
	for _, part := range strings.Split(spec, ",") {
		idStr, addrs, ok := strings.Cut(strings.TrimSpace(part), "=")
		ra, ca, ok2 := strings.Cut(addrs, "/")
		n, err := strconv.ParseUint(idStr, 10, 64)
		if !ok || !ok2 || err != nil || n == 0 {
			return nil, fmt.Errorf("bad -cluster member %q (want id=raft-addr/client-addr)", part)
		}
		p := cluster.Peer{ID: raft.NodeID(n), RaftAddr: ra, ClientAddr: ca}
		cc.Peers = append(cc.Peers, p)
		if p.ID == id {
			cc.Self = p
		}
	}
	if cc.Self.RaftAddr == "" {
		return nil, fmt.Errorf("-cluster does not include this node's id %d", id)
	}
	return cc, nil
}

// loadConfig applies a file of "flag value" lines (Redis-style config syntax), e.g.
//
//	addr 0.0.0.0:6379
//	requirepass s3cret
//	user alice:pw:read
func loadConfig(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, _ := strings.Cut(line, " ")
		if name == "config" {
			continue
		}
		if err := flag.Set(name, strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("%s:%d: %v", path, n, err)
		}
	}
	return sc.Err()
}
