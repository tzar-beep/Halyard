// Package server owns the TCP listener and per-connection lifecycle.
package server

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"crypto/tls"
	"net/http"
	"time"

	"github.com/tzar-beep/Halyard/internal/cluster"
	"github.com/tzar-beep/Halyard/internal/command"
	"github.com/tzar-beep/Halyard/internal/metrics"
	"github.com/tzar-beep/Halyard/internal/persistence"
	"github.com/tzar-beep/Halyard/internal/resp"
	"github.com/tzar-beep/Halyard/internal/shard"
	"github.com/tzar-beep/Halyard/internal/store"
)

// Handler executes client commands for one connection's session.
type Handler interface {
	Dispatch(w *resp.Writer, sess *command.Session, args [][]byte) error
	CloseSession(sess *command.Session)
}

type Server struct {
	cfg    Config
	http   *http.Server
	addr   string
	h      Handler
	router *shard.Router // non-nil in sharded mode
	store  *store.Store
	disp   *command.Dispatcher
	pm     *persistence.Manager // nil when persistence is disabled
	node   *cluster.Node        // non-nil in cluster mode

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
	// Cluster, if set, replicates all writes through Raft. AOF/snapshot settings
	// are then ignored: the Raft WAL and snapshots provide durability.
	Cluster *cluster.Config
	// Shards > 0 (with Cluster) runs that many Raft data groups plus a metadata
	// group, routing keys by hash slot like Redis Cluster.
	Shards int

	Users      []command.User // ACL users in addition to the requirepass "default" user
	TLSCert    string         // serve clients over TLS when set (with TLSKey)
	TLSKey     string
	HTTPAddr   string // /metrics, /healthz, /api/status
	Demo       bool   // also expose the public demo API (/api/command, /api/fault)
	CORSOrigin string // Access-Control-Allow-Origin for the HTTP API
}

// finish applies settings common to every mode.
func (s *Server) finish(cfg Config) (*Server, error) {
	s.cfg = cfg
	if s.router != nil {
		s.router.SetUsers(cfg.Users)
	} else {
		s.disp.SetUsers(cfg.Users)
	}
	s.registerGauges()
	if cfg.HTTPAddr != "" {
		if err := s.startHTTP(cfg.HTTPAddr); err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

// New builds a server, recovering the snapshot and AOF first when persistence is enabled.
func New(cfg Config) (*Server, error) {
	if cfg.Cluster != nil && cfg.Shards > 0 {
		s, err := newSharded(cfg)
		if err != nil {
			return nil, err
		}
		return s.finish(cfg)
	}
	if cfg.Cluster != nil {
		s, err := newClustered(cfg)
		if err != nil {
			return nil, err
		}
		return s.finish(cfg)
	}
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
	s.h = s.disp
	return s.finish(cfg)
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	if s.cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			ln.Close()
			return err
		}
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		log.Printf("TLS enabled")
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
	if s.http != nil {
		s.http.Close()
	}
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
	if s.router != nil {
		s.router.Close()
		return err
	}
	s.store.Close()
	if s.node != nil {
		s.node.Stop()
	}
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
	metrics.ConnectionsCurrent.Add(1)
	metrics.ConnectionsTotal.Add(1)
	defer metrics.ConnectionsCurrent.Add(-1)
	r, w := resp.NewReader(c), resp.NewWriter(c)
	var sess command.Session
	defer s.h.CloseSession(&sess)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				metrics.ProtocolErrors.Add(1)
				w.Error("ERR " + pe.Error())
				w.Flush()
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("conn %s: %v", c.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		t0 := time.Now()
		name, known := command.Name(args[0])
		if name == "info" && (sess.Authed || !s.authRequired()) {
			s.info(w, args)
		} else if err := s.h.Dispatch(w, &sess, args); err != nil {
			// Unpersisted write: drop the connection without flushing the reply.
			log.Printf("conn %s: %v", c.RemoteAddr(), err)
			return
		}
		metrics.CommandLatency.Observe(time.Since(t0))
		metrics.CountCommand(name, known)
		// Flush only once the pipeline is drained: batches replies to pipelined requests.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func newClustered(cfg Config) (*Server, error) {
	st := store.NewReplicatedStore()
	s := &Server{
		addr:  cfg.Addr,
		store: st,
		disp:  command.NewDispatcher(st, cfg.Password),
		conns: map[net.Conn]struct{}{},
	}
	ccfg := *cfg.Cluster
	if ccfg.Secret == "" {
		ccfg.Secret = cfg.Password
	}
	node, err := cluster.Start(ccfg, replicatedSM{st: st, d: s.disp})
	if err != nil {
		st.Close()
		return nil, err
	}
	s.node = node
	s.disp.SetReplicator(node)
	s.h = s.disp
	return s, nil
}

func newSharded(cfg Config) (*Server, error) {
	c := cfg.Cluster
	r, err := shard.Start(shard.Config{Self: c.Self, Peers: c.Peers, Dir: c.Dir, Groups: cfg.Shards,
		Password: cfg.Password, Tick: c.Tick, SnapshotEntries: c.SnapshotEntries})
	if err != nil {
		return nil, err
	}
	return &Server{addr: cfg.Addr, h: r, router: r, conns: map[net.Conn]struct{}{}}, nil
}

// Router returns the shard router in sharded mode (nil otherwise).
func (s *Server) Router() *shard.Router { return s.router }

// Node returns the Raft node in cluster mode (nil otherwise).
func (s *Server) Node() *cluster.Node { return s.node }

// replicatedSM adapts the keyspace to the cluster's state machine interface.
type replicatedSM struct {
	st *store.Store
	d  *command.Dispatcher
}

func (r replicatedSM) ApplyCommand(now int64, args [][]byte) []byte {
	return r.d.ApplyCommand(now, args)
}

func (r replicatedSM) Snapshot() []byte {
	return persistence.EncodeSnapshot(0, r.st.Snapshot())
}

func (r replicatedSM) Restore(data []byte) error {
	var entries []store.Entry
	if _, err := persistence.DecodeSnapshot(data, func(e store.Entry) { entries = append(entries, e) }); err != nil {
		return err
	}
	r.st.Replace(entries)
	return nil
}

func (r replicatedSM) ExpiredKeys(now int64, n int) []string { return r.st.ExpiredKeys(now, n) }

func (s *Server) authRequired() bool {
	if s.router != nil {
		return s.router.Groups()[1].Disp.AuthRequired()
	}
	return s.disp.AuthRequired()
}
