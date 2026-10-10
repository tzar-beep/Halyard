package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tzar-beep/redis/internal/persistence"
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
	flag.Int64Var(&cfg.AutoSaveBytes, "autosave-bytes", 64<<20, "snapshot and start a new AOF segment once it exceeds this size (0 = off)")
	flag.Parse()

	p, err := persistence.ParsePolicy(fsync)
	if err != nil {
		log.Fatal(err)
	}
	cfg.AppendFsync = p
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
