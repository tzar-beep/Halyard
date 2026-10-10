package server

import (
	"bufio"
	"log"
	"net"
	"strings"
)

type Server struct{ addr string }

func New(addr string) *Server { return &Server{addr: addr} }

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("listening on %s", s.addr)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handle(c)
	}
}

// Phase 0 stub: replies +PONG to anything containing PING. Real RESP parser comes in Phase 1.
func handle(c net.Conn) {
	defer c.Close()
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if strings.EqualFold(strings.TrimSpace(line), "PING") {
			w.WriteString("+PONG\r\n")
			if w.Flush() != nil {
				return
			}
		}
	}
}
