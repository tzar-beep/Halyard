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

	"github.com/tzar-beep/redis/internal/cluster"
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
	flag.Parse()

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
