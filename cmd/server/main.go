package main

import (
	"flag"
	"log"
	"os"

	"github.com/tzar-beep/redis/internal/server"
)

func main() {
	cfg := server.Config{}
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:6379", "listen address")
	flag.StringVar(&cfg.Password, "requirepass", os.Getenv("REDIS_PASSWORD"), "require AUTH with this password (default $REDIS_PASSWORD)")
	flag.Parse()
	if cfg.Password == "" {
		log.Print("warning: no password set; do not expose this server publicly")
	}
	log.Fatal(server.New(cfg).ListenAndServe())
}
