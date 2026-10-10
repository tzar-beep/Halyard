package main

import (
	"flag"
	"log"

	"github.com/tzar-beep/redis/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "listen address")
	flag.Parse()
	log.Fatal(server.New(*addr).ListenAndServe())
}
