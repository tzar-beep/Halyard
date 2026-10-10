// Package server owns the TCP listener and per-connection lifecycle.
package server

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/tzar-beep/redis/internal/command"
	"github.com/tzar-beep/redis/internal/persistence"
	"github.com/tzar-beep/redis/internal/resp"
	"github.com/tzar-beep/redis/internal/store"
)

type Server struct {
	addr  string
	store *store.Store
	disp  *command.Dispatcher
	pm    *persistence.Manager // nil when persistence is disabled

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
	done  bool
	wg    sync.WaitGroup
}

type Config struct {
	Addr        string
	Password    string // empty disables AUTH
	Dir         string // data directory
	AppendOnly  bool
	AppendFsync persistence.Policy
	// AutoSaveBytes snapshots and starts a new AOF segment when the current one
	// exceeds this size. 0 disables.
	AutoSaveBytes int64
}

// New builds a server, recovering the snapshot and AOF first when persistence is enabled.
func New(cfg Config) (*Server, error) {
	st := store.NewStore()
	s := &Server{
		addr:  cfg.Addr,
		store: st,
		disp:  command.NewDispatcher(st, cfg.Password),
		conns: map[net.Conn]struct{}{},
	}
	if cfg.AppendOnly {
		pm, err := persistence.Open(
			persistence.Options{Dir: cfg.Dir, Policy: cfg.AppendFsync, AutoSaveBytes: cfg.AutoSaveBytes},
			persistence.Hooks{Restore: st.Restore, Apply: s.disp.Apply, Capture: st.Snapshot, Freeze: s.disp.Freeze},
		)
		if err != nil {
			st.Close()
			return nil, err
		}
		s.pm = pm
		s.disp.SetPersister(pm)
	}
	return s, nil
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("listening on %s", ln.Addr())
	return s.Serve(ln)
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		c, err := ln.Accept()
		s.mu.Lock()
		if s.done {
			s.mu.Unlock()
			if c != nil {
				c.Close()
			}
			return nil
		}
		if err != nil {
			s.mu.Unlock()
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(c)
	}
}

// Close stops the listener, closes all client connections and waits for handlers.
func (s *Server) Close() error {
	s.mu.Lock()
	s.done = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.store.Close()
	if s.pm != nil {
		if aerr := s.pm.Close(); err == nil {
			err = aerr
		}
	}
	return err
}

func (s *Server) handle(c net.Conn) {
	defer func() {
		c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		s.wg.Done()
	}()
	r, w := resp.NewReader(c), resp.NewWriter(c)
	var sess command.Session
	defer s.disp.CloseSession(&sess)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				w.Error("ERR " + pe.Error())
				w.Flush()
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("conn %s: %v", c.RemoteAddr(), err)
			}
			return
		}
		if err := s.disp.Dispatch(w, &sess, args); err != nil {
			// Unpersisted write: drop the connection without flushing the reply.
			log.Printf("conn %s: %v", c.RemoteAddr(), err)
			return
		}
		// Flush only once the pipeline is drained: batches replies to pipelined requests.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}
