package cluster

import (
	"bufio"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tzar-beep/redis/internal/raft"
)

// transport carries Raft messages over TCP. Each peer gets one outbound connection
// fed by a bounded queue; when the queue is full or the peer is down, messages are
// dropped — Raft retransmits, so loss only costs latency.
//
// Framing: [u32 length][payload]; message payloads are [u32 group][message], so
// one connection per peer carries every Raft group (multi-Raft). A connection opens with a handshake frame
// (sender id, sender's Raft address, cluster secret). The receiver verifies the
// secret in constant time and learns a return route to the sender, which lets a
// freshly added node answer the leader before it knows the membership.
type transport struct {
	id     raft.NodeID
	addr   string // our advertised Raft address, sent in the handshake
	secret []byte
	ln     net.Listener
	recv   func(group uint32, m raft.Message)

	mu    sync.Mutex
	peers map[raft.NodeID]*peerConn
	conns map[net.Conn]struct{}

	// filter, if set, drops messages for which it returns false (fault injection).
	filter atomic.Pointer[func(from, to raft.NodeID) bool]
	closed atomic.Bool
	wg     sync.WaitGroup
}

const (
	maxFrame   = 512 << 20
	queueDepth = 4096
)

func newTransport(id raft.NodeID, addr, secret string, recv func(uint32, raft.Message)) (*transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &transport{id: id, addr: addr, secret: []byte(secret), ln: ln, recv: recv,
		peers: map[raft.NodeID]*peerConn{}, conns: map[net.Conn]struct{}{}}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

func (t *transport) allowed(from, to raft.NodeID) bool {
	if f := t.filter.Load(); f != nil {
		return (*f)(from, to)
	}
	return true
}

func (t *transport) setPeer(id raft.NodeID, addr string) {
	if id == t.id {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if pc, ok := t.peers[id]; ok {
		if pc.addr == addr {
			return
		}
		pc.close()
	}
	pc := &peerConn{t: t, id: id, addr: addr, q: make(chan []byte, queueDepth), stop: make(chan struct{})}
	t.peers[id] = pc
	t.wg.Add(1)
	go pc.run()
}

func (t *transport) removePeer(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if pc, ok := t.peers[id]; ok {
		pc.close()
		delete(t.peers, id)
	}
}

func (t *transport) send(group uint32, msgs []raft.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		pc := t.peers[m.To]
		if pc == nil || !t.allowed(m.From, m.To) {
			continue
		}
		select {
		case pc.q <- append(binary.BigEndian.AppendUint32(nil, group), m.Marshal()...):
		default: // queue full: drop
		}
	}
}

func (t *transport) close() {
	if t.closed.Swap(true) {
		return
	}
	t.ln.Close()
	t.mu.Lock()
	for _, pc := range t.peers {
		pc.close()
	}
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
}

func (t *transport) acceptLoop() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			if t.closed.Load() {
				return
			}
			continue
		}
		t.mu.Lock()
		t.conns[c] = struct{}{}
		t.mu.Unlock()
		t.wg.Add(1)
		go t.serveConn(c)
	}
}

func (t *transport) serveConn(c net.Conn) {
	defer t.wg.Done()
	defer func() {
		c.Close()
		t.mu.Lock()
		delete(t.conns, c)
		t.mu.Unlock()
	}()
	br := bufio.NewReaderSize(c, 1<<16)
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	hello, err := readFrame(br)
	from, addr, secret, ok := parseHello(hello)
	if err != nil || !ok || subtle.ConstantTimeCompare(secret, t.secret) != 1 {
		log.Printf("raft transport: rejected connection from %s", c.RemoteAddr())
		return
	}
	t.learnRoute(from, addr)
	c.SetReadDeadline(time.Time{})
	for {
		b, err := readFrame(br)
		if err != nil {
			return
		}
		if len(b) < 4 {
			return
		}
		m, err := raft.UnmarshalMessage(b[4:])
		if err != nil || m.From != from || m.To != t.id {
			log.Printf("raft transport: bad message from %d: %v", from, err)
			return
		}
		if t.allowed(m.From, m.To) {
			t.recv(binary.BigEndian.Uint32(b), m)
		}
	}
}

func makeHello(id raft.NodeID, addr string, secret []byte) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(id))
	b = binary.BigEndian.AppendUint16(b, uint16(len(addr)))
	b = append(b, addr...)
	return append(b, secret...)
}

func parseHello(b []byte) (id raft.NodeID, addr string, secret []byte, ok bool) {
	if len(b) < 10 {
		return 0, "", nil, false
	}
	n := int(binary.BigEndian.Uint16(b[8:10]))
	if len(b) < 10+n {
		return 0, "", nil, false
	}
	return raft.NodeID(binary.BigEndian.Uint64(b)), string(b[10 : 10+n]), b[10+n:], true
}

// learnRoute adds a route to an authenticated peer we have no address for yet.
func (t *transport) learnRoute(id raft.NodeID, addr string) {
	if addr == "" || id == t.id {
		return
	}
	t.mu.Lock()
	_, known := t.peers[id]
	t.mu.Unlock()
	if !known && !t.closed.Load() {
		t.setPeer(id, addr)
	}
}

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrame {
		return nil, errors.New("frame too large")
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

func writeFrame(w *bufio.Writer, b []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	w.Write(hdr[:])
	_, err := w.Write(b)
	return err
}

type peerConn struct {
	t    *transport
	id   raft.NodeID
	addr string
	q    chan []byte
	stop chan struct{}
	once sync.Once
}

func (p *peerConn) close() { p.once.Do(func() { close(p.stop) }) }

// run keeps a connection to the peer open and drains the queue into it.
func (p *peerConn) run() {
	defer p.t.wg.Done()
	backoff := 50 * time.Millisecond
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		c, err := net.DialTimeout("tcp", p.addr, time.Second)
		if err != nil {
			select {
			case <-p.stop:
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 50 * time.Millisecond
		p.pump(c)
		c.Close()
	}
}

func (p *peerConn) pump(c net.Conn) {
	bw := bufio.NewWriterSize(c, 1<<16)
	writeFrame(bw, makeHello(p.t.id, p.t.addr, p.t.secret))
	if bw.Flush() != nil {
		return
	}
	go func() { // detect the remote closing the connection
		io.Copy(io.Discard, c)
		c.Close()
	}()
	for {
		select {
		case <-p.stop:
			return
		case b := <-p.q:
			c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if writeFrame(bw, b) != nil {
				return
			}
			// Batch whatever else is queued before flushing.
			for n := len(p.q); n > 0; n-- {
				if writeFrame(bw, <-p.q) != nil {
					return
				}
			}
			if bw.Flush() != nil {
				return
			}
		}
	}
}
