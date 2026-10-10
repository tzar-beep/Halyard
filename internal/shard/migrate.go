package shard

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Migrate moves one slot to another data group while it keeps serving traffic.
//
// Protocol (every step is a replicated, idempotent command, so re-running
// Migrate after a crash resumes where it stopped):
//
//  1. target: SETSLOT s importing <- serves ASKING requests for s
//  2. source: SETSLOT s migrating -> keeps serving keys it still has; requests
//     for missing keys get "ASK s target"
//  3. meta:   s owner=source migrating=target
//  4. repeat until the source has no keys in s:
//     dump k on source, IMPORTKEY on target, DELIFEQ on source
//     (delete only if unchanged since the dump; otherwise retry the key)
//  5. target owned, meta owner=target, source none
//
// Why no write is lost or duplicated: a key lives in the source until DELIFEQ
// removes the exact version the target already holds. While the key exists in the
// source, all writes go there (clients only get ASK for missing keys), and the
// source's guard runs inside the same transaction as the write, so a write can
// never land in the source after the key has moved. New keys go straight to the
// target. Once the source is empty, ownership flips in the metadata group.
func (r *Router) Migrate(slot int, to uint32) (moved int, err error) {
	r.migMu.Lock()
	defer r.migMu.Unlock()
	from := r.metaSM.Route(slot).Owner
	if from == to {
		return 0, nil
	}
	c := &internalClient{r: r, password: r.password}
	defer c.close()
	step := func(args ...string) (string, error) {
		reply, err := c.do(args...)
		if err == nil && strings.HasPrefix(reply, "-") {
			err = errors.New(reply[1:])
		}
		return reply, err
	}
	s, f, t := strconv.Itoa(slot), strconv.Itoa(int(from)), strconv.Itoa(int(to))
	for _, cmd := range [][]string{
		{"SHARD.SETSLOT", t, s, "importing", f},
		{"SHARD.SETSLOT", f, s, "migrating", t},
		{"SHARD.META", s, f, t},
	} {
		if _, err := step(cmd...); err != nil {
			return moved, err
		}
	}
	for {
		keys, err := c.array("SHARD.SCAN", f, s, "100")
		if err != nil {
			return moved, err
		}
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			dump, err := c.bulk("SHARD.DUMP", f, k)
			if err != nil {
				return moved, err
			}
			if dump == nil {
				continue // expired or deleted meanwhile
			}
			if _, err := step("SHARD.IMPORT", t, k, string(dump)); err != nil {
				return moved, err
			}
			reply, err := step("SHARD.DELIFEQ", f, k, string(dump))
			if err != nil {
				return moved, err
			}
			if reply == ":1" {
				moved++
			} // else: modified concurrently; the next SCAN picks it up again
		}
	}
	for _, cmd := range [][]string{
		{"SHARD.SETSLOT", t, s, "owned", "0"},
		{"SHARD.META", s, t, "0"},
		{"SHARD.SETSLOT", f, s, "none", "0"},
	} {
		if _, err := step(cmd...); err != nil {
			return moved, err
		}
	}
	return moved, nil
}

// internalClient sends internal commands to whichever node leads the target group,
// following MOVED redirects. Commands go over RESP with AUTH, so the coordinator
// can run on any node.
type internalClient struct {
	r        *Router
	password string
	conns    map[string]*rconn
}

type rconn struct {
	c  net.Conn
	br *bufio.Reader
}

func (c *internalClient) close() {
	for _, rc := range c.conns {
		rc.c.Close()
	}
}

func (c *internalClient) conn(addr string) (*rconn, error) {
	if c.conns == nil {
		c.conns = map[string]*rconn{}
	}
	if rc, ok := c.conns[addr]; ok {
		return rc, nil
	}
	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	rc := &rconn{nc, bufio.NewReader(nc)}
	if reply, err := rc.roundtrip("AUTH", c.password); err != nil || reply.s != "+OK" {
		nc.Close()
		return nil, fmt.Errorf("auth to %s failed", addr)
	}
	c.conns[addr] = rc
	return rc, nil
}

type reply struct {
	s     string   // first line (or bulk payload)
	bulk  []byte   // bulk payload (nil for null bulk)
	items []string // array of bulk strings
}

func (rc *rconn) roundtrip(args ...string) (reply, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	rc.c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := rc.c.Write([]byte(b.String())); err != nil {
		return reply{}, err
	}
	return readReply(rc.br)
}

func readReply(br *bufio.Reader) (reply, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return reply{}, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return reply{}, errors.New("empty reply")
	}
	switch line[0] {
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return reply{s: "(nil)"}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return reply{}, err
		}
		return reply{s: string(buf[:n]), bulk: buf[:n]}, nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		out := reply{s: line}
		for i := 0; i < n; i++ {
			item, err := readReply(br)
			if err != nil {
				return reply{}, err
			}
			out.items = append(out.items, item.s)
		}
		return out, nil
	}
	return reply{s: line}, nil
}

// send executes a command on the leader of the group it names, starting from the
// locally known leader and following MOVED/CLUSTERDOWN with retries.
func (c *internalClient) send(args ...string) (reply, error) {
	gid := uint32(0)
	if args[0] != "SHARD.META" {
		g, _ := strconv.ParseUint(args[1], 10, 32)
		gid = uint32(g)
	}
	var last reply
	for attempt := 0; attempt < 50; attempt++ {
		addr := c.r.leaderAddr(gid)
		if addr == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rc, err := c.conn(addr)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rep, err := rc.roundtrip(args...)
		if err != nil {
			rc.c.Close()
			delete(c.conns, addr)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		last = rep
		if strings.HasPrefix(rep.s, "-MOVED") || strings.HasPrefix(rep.s, "-CLUSTERDOWN") ||
			strings.HasPrefix(rep.s, "-TRYAGAIN") || strings.HasPrefix(rep.s, "-ERR timeout") {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return rep, nil
	}
	return last, fmt.Errorf("%s: no stable leader (last reply %q)", args[0], last.s)
}

func (c *internalClient) do(args ...string) (string, error) {
	rep, err := c.send(args...)
	return rep.s, err
}

func (c *internalClient) array(args ...string) ([]string, error) {
	rep, err := c.send(args...)
	if err == nil && strings.HasPrefix(rep.s, "-") {
		err = errors.New(rep.s[1:])
	}
	return rep.items, err
}

func (c *internalClient) bulk(args ...string) ([]byte, error) {
	rep, err := c.send(args...)
	if err == nil && strings.HasPrefix(rep.s, "-") {
		err = errors.New(rep.s[1:])
	}
	return rep.bulk, err
}
