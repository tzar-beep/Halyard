package shard

import (
	"bytes"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tzar-beep/Halyard/internal/cluster"
	"github.com/tzar-beep/Halyard/internal/command"
	"github.com/tzar-beep/Halyard/internal/raft"
	"github.com/tzar-beep/Halyard/internal/resp"
	"github.com/tzar-beep/Halyard/internal/store"
)

// Group is one local data-group replica.
type Group struct {
	ID    uint32
	Node  *cluster.Node
	Disp  *command.Dispatcher
	Store *store.Store
	SM    *GroupSM
}

// Router is the client-facing command handler in sharded mode.
type Router struct {
	password string
	host     *cluster.Host
	self     raft.NodeID
	meta     *cluster.Node
	metaSM   *MetaSM
	groups   map[uint32]*Group
	stop     chan struct{}
	wg       sync.WaitGroup
	migMu    sync.Mutex // one migration at a time per node
}

type Config struct {
	Self     cluster.Peer
	Peers    []cluster.Peer
	Dir      string
	Groups   int
	Password string
	Tick     time.Duration
	// SnapshotEntries is passed to every group (0 = default).
	SnapshotEntries uint64
}

// Start brings up the metadata group and every data group on one host.
func Start(cfg Config) (*Router, error) {
	if cfg.Groups < 1 {
		return nil, fmt.Errorf("shard: need at least one data group")
	}
	host, err := cluster.StartHost(cfg.Self, cfg.Password)
	if err != nil {
		return nil, err
	}
	r := &Router{password: cfg.Password, host: host, self: cfg.Self.ID, groups: map[uint32]*Group{}, stop: make(chan struct{})}
	gcfg := func(g uint32) cluster.Config {
		return cluster.Config{Self: cfg.Self, Peers: cfg.Peers, Dir: fmt.Sprintf("%s/g%d", cfg.Dir, g),
			Tick: cfg.Tick, SnapshotEntries: cfg.SnapshotEntries, Secret: cfg.Password}
	}
	r.metaSM = NewMetaSM(cfg.Groups)
	if r.meta, err = host.StartGroup(0, gcfg(0), r.metaSM); err != nil {
		host.Stop()
		return nil, err
	}
	for g := uint32(1); g <= uint32(cfg.Groups); g++ {
		st := store.NewReplicatedStore()
		d := command.NewDispatcher(st, cfg.Password)
		sm := NewGroupSM(g, cfg.Groups, st, d)
		sm.addrOf, sm.metaOf = r.leaderAddr, r.metaSM.Route
		node, err := host.StartGroup(g, gcfg(g), sm)
		if err != nil {
			host.Stop()
			return nil, err
		}
		d.SetReplicator(node)
		r.groups[g] = &Group{ID: g, Node: node, Disp: d, Store: st, SM: sm}
	}
	r.wg.Add(1)
	go r.balance()
	return r, nil
}

func (r *Router) Close() {
	close(r.stop)
	r.wg.Wait()
	r.host.Stop()
	for _, g := range r.groups {
		g.Store.Close()
	}
}

// Groups returns the local data groups.
func (r *Router) Groups() map[uint32]*Group { return r.groups }

// Meta returns the metadata group's node.
func (r *Router) Meta() *cluster.Node { return r.meta }

// SetUsers configures ACL users on every group.
func (r *Router) SetUsers(users []command.User) {
	for _, g := range r.groups {
		g.Disp.SetUsers(users)
	}
}

// Host exposes the network endpoint (fault injection in tests).
func (r *Router) Host() *cluster.Host { return r.host }

func (r *Router) node(g uint32) *cluster.Node {
	if g == 0 {
		return r.meta
	}
	if grp := r.groups[g]; grp != nil {
		return grp.Node
	}
	return nil
}

// leaderAddr returns the client address of group g's leader, as known locally.
func (r *Router) leaderAddr(g uint32) string {
	n := r.node(g)
	if n == nil {
		return ""
	}
	st := n.CurrentStatus()
	return st.Peers[st.Leader].ClientAddr
}

func (r *Router) isLeader(g uint32) bool {
	n := r.node(g)
	return n != nil && n.CurrentStatus().State == raft.StateLeader
}

func (r *Router) redirect(slot int, g uint32) string {
	if a := r.leaderAddr(g); a != "" {
		return fmt.Sprintf("MOVED %d %s", slot, a)
	}
	return "CLUSTERDOWN The cluster is down"
}

// balance spreads group leaders across nodes: group g prefers the member at
// position g mod N, so with N nodes and N groups each node leads one group.
func (r *Router) balance() {
	defer r.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
		}
		for g := uint32(0); g <= uint32(len(r.groups)); g++ {
			n := r.node(g)
			st := n.CurrentStatus()
			if st.State != raft.StateLeader || len(st.Voters) == 0 {
				continue
			}
			want := st.Voters[int(g)%len(st.Voters)]
			if want != r.self {
				n.TransferLeadership(want)
			}
		}
	}
}

type session struct {
	inMulti bool
	txErr   bool
	queued  [][][]byte
	slot    int
	asking  bool
}

func sess(cs *command.Session) *session {
	if s, ok := cs.Ext.(*session); ok {
		return s
	}
	s := &session{slot: -1}
	cs.Ext = s
	return s
}

func (r *Router) CloseSession(*command.Session) {}

// Dispatch routes one client command.
func (r *Router) Dispatch(w *resp.Writer, cs *command.Session, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToLower(string(args[0]))
	any := r.groups[1].Disp
	if name == "auth" || (any.AuthRequired() && !cs.Authed) {
		return any.Dispatch(w, cs, args) // AUTH, or the NOAUTH error
	}
	if e := any.Allowed(cs, args); e != "" {
		w.Error(e)
		return nil
	}
	s := sess(cs)
	switch name {
	case "asking":
		s.asking = true
		w.SimpleString("OK")
		return nil
	case "cluster":
		r.clusterCmd(w, args)
		return nil
	case "multi":
		if s.inMulti {
			w.Error("ERR MULTI calls can not be nested")
		} else {
			*s = session{inMulti: true, slot: -1, asking: s.asking}
			w.SimpleString("OK")
		}
		return nil
	case "discard":
		if !s.inMulti {
			w.Error("ERR DISCARD without MULTI")
		} else {
			*s = session{slot: -1}
			w.SimpleString("OK")
		}
		return nil
	case "exec":
		r.exec(w, s)
		return nil
	case "flushall", "watch", "save", "bgsave":
		w.Error("ERR " + strings.ToUpper(name) + " is not supported in sharded mode")
		return nil
	case "dbsize":
		n := 0
		for _, g := range r.groups {
			if r.isLeader(g.ID) {
				n += g.Store.Len()
			}
		}
		w.Integer(int64(n))
		return nil
	case "raft":
		w.Bulk([]byte(r.status()))
		return nil
	}
	if strings.HasPrefix(name, "shard.") {
		r.internalCmd(w, name, args)
		return nil
	}

	keys := Keys(args)
	if len(keys) == 0 { // keyless (PING, ECHO, ...) or unknown: any group handles it
		return any.Dispatch(w, cs, args)
	}
	slot := SlotOf(keys)
	if slot < 0 {
		w.Error("CROSSSLOT Keys in request don't hash to the same slot")
		return nil
	}
	if s.inMulti {
		if e := command.Validate(args); e != "" {
			w.Error(e)
			s.txErr = true
		} else if s.slot >= 0 && s.slot != slot {
			w.Error("CROSSSLOT Keys in request don't hash to the same slot")
			s.txErr = true
		} else {
			s.slot = slot
			s.queued = append(s.queued, cloneArgs(args))
			w.SimpleString("QUEUED")
		}
		return nil
	}
	g, ok := r.route(w, slot, s)
	if !ok {
		return nil
	}
	cs.Asking = s.asking
	s.asking = false
	return g.Disp.Dispatch(w, cs, args)
}

// route picks the local group for a slot: the owner, or — for an ASKING request —
// the group importing it. It writes a redirect and returns false if the chosen
// group is not led by this node.
func (r *Router) route(w *resp.Writer, slot int, s *session) (*Group, bool) {
	gid := r.metaSM.Route(slot).Owner
	if s.asking {
		for _, g := range r.groups {
			if ss := g.SM.slot(slot); ss.state == slotImporting {
				gid = g.ID
			}
		}
	}
	g := r.groups[gid]
	if g == nil {
		w.Error("CLUSTERDOWN Hash slot not served")
		return nil, false
	}
	if !r.isLeader(gid) {
		w.Error(r.redirect(slot, gid))
		return nil, false
	}
	return g, true
}

func (r *Router) exec(w *resp.Writer, s *session) {
	defer func() { *s = session{slot: -1} }()
	switch {
	case !s.inMulti:
		w.Error("ERR EXEC without MULTI")
		return
	case s.txErr:
		w.Error("EXECABORT Transaction discarded because of previous errors.")
		return
	case len(s.queued) == 0:
		w.ArrayHeader(0)
		return
	}
	g, ok := r.route(w, s.slot, s)
	if !ok {
		return
	}
	g.Disp.ExecQueued(w, s.queued, s.asking)
}

func cloneArgs(args [][]byte) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = bytes.Clone(a)
	}
	return out
}

// --- CLUSTER ---

func nodeID(id raft.NodeID) []byte { return []byte(fmt.Sprintf("%040x", uint64(id))) }

func (r *Router) clusterCmd(w *resp.Writer, args [][]byte) {
	sub := ""
	if len(args) > 1 {
		sub = strings.ToUpper(string(args[1]))
	}
	switch {
	case sub == "KEYSLOT" && len(args) == 3:
		w.Integer(int64(KeySlot(args[2])))
	case sub == "SLOTS" && len(args) == 2:
		r.clusterSlots(w)
	case sub == "INFO" && len(args) == 2:
		state := "ok"
		for g := range r.groups {
			if r.leaderAddr(g) == "" {
				state = "fail"
			}
		}
		w.Bulk([]byte(fmt.Sprintf("cluster_state:%s\r\ncluster_slots_assigned:%d\r\ncluster_known_nodes:%d\r\ncluster_size:%d\r\n",
			state, NumSlots, len(r.meta.CurrentStatus().Voters), len(r.groups))))
	case sub == "MIGRATE" && len(args) == 4:
		slot, err1 := strconv.Atoi(string(args[2]))
		to, err2 := strconv.ParseUint(string(args[3]), 10, 32)
		if err1 != nil || err2 != nil || slot < 0 || slot >= NumSlots || r.groups[uint32(to)] == nil {
			w.Error("ERR usage: CLUSTER MIGRATE <slot> <group>")
			return
		}
		moved, err := r.Migrate(slot, uint32(to))
		if err != nil {
			w.Error("ERR migration failed: " + err.Error())
			return
		}
		w.Integer(int64(moved))
	default:
		w.Error("ERR usage: CLUSTER KEYSLOT <key> | SLOTS | INFO | MIGRATE <slot> <group>")
	}
}

// clusterSlots replies like Redis: [start, end, master, replicas...] where each node
// is [ip, port, id]. The master of a range is the leader of its owning group.
func (r *Router) clusterSlots(w *resp.Writer) {
	ranges := r.metaSM.Ranges()
	w.ArrayHeader(len(ranges))
	for _, rg := range ranges {
		st := r.node(rg.Owner).CurrentStatus()
		ids := append([]raft.NodeID(nil), st.Voters...)
		sort.Slice(ids, func(i, j int) bool { // leader first
			return ids[i] == st.Leader || (ids[j] != st.Leader && ids[i] < ids[j])
		})
		w.ArrayHeader(2 + len(ids))
		w.Integer(int64(rg.Start))
		w.Integer(int64(rg.End))
		for _, id := range ids {
			host, port, _ := net.SplitHostPort(st.Peers[id].ClientAddr)
			p, _ := strconv.Atoi(port)
			w.ArrayHeader(3)
			w.Bulk([]byte(host))
			w.Integer(int64(p))
			w.Bulk(nodeID(id))
		}
	}
}

func (r *Router) status() string {
	var b strings.Builder
	for g := uint32(0); g <= uint32(len(r.groups)); g++ {
		st := r.node(g).CurrentStatus()
		fmt.Fprintf(&b, "group:%d state:%s term:%d leader:%d commit:%d applied:%d\n", g, st.State, st.Term, st.Leader, st.Commit, st.Applied)
	}
	return b.String()
}

// --- internal commands used by the migration coordinator ---
//
//	SHARD.SETSLOT g slot state peer   -> replicated in group g
//	SHARD.SCAN g slot count           -> keys of slot (leader read)
//	SHARD.DUMP g key                  -> canonical value or nil (leader read)
//	SHARD.IMPORT g key dump           -> replicated in group g
//	SHARD.DELIFEQ g key dump          -> replicated in group g
//	SHARD.META slot owner migrating   -> replicated in the metadata group

func (r *Router) internalCmd(w *resp.Writer, name string, args [][]byte) {
	if r.password == "" {
		// Without a password anyone could rewrite slot ownership; refuse.
		w.Error("ERR internal shard commands require requirepass")
		return
	}
	if len(args) < 2 {
		w.Error("ERR bad internal command")
		return
	}
	gid64, err := strconv.ParseUint(string(args[1]), 10, 32)
	gid := uint32(gid64)
	if name == "shard.meta" {
		gid, err = 0, nil
	}
	n := r.node(gid)
	if err != nil || n == nil {
		w.Error("ERR unknown group")
		return
	}
	if !r.isLeader(gid) {
		w.Error(r.redirect(0, gid))
		return
	}
	propose := func(cmd ...[]byte) {
		reply, err := n.Propose(cmd)
		if err != nil {
			w.Error(err.Error())
		} else {
			w.Raw(reply)
		}
	}
	read := func(fn func(*Group)) {
		if err := n.ReadBarrier(); err != nil {
			w.Error(err.Error())
			return
		}
		fn(r.groups[gid])
	}
	switch {
	case name == "shard.setslot" && len(args) == 5:
		propose(append([][]byte{[]byte("SETSLOT")}, args[2:]...)...)
	case name == "shard.import" && len(args) == 4:
		propose([]byte("IMPORTKEY"), args[2], args[3])
	case name == "shard.delifeq" && len(args) == 4:
		propose([]byte("DELIFEQ"), args[2], args[3])
	case name == "shard.meta" && len(args) == 4:
		propose([]byte("METASET"), args[1], args[2], args[3])
	case name == "shard.scan" && len(args) == 4 && gid != 0:
		read(func(g *Group) {
			slot, _ := strconv.Atoi(string(args[2]))
			count, _ := strconv.Atoi(string(args[3]))
			keys := g.SM.KeysInSlot(slot, max(count, 1))
			w.ArrayHeader(len(keys))
			for _, k := range keys {
				w.Bulk([]byte(k))
			}
		})
	case name == "shard.dump" && len(args) == 3 && gid != 0:
		read(func(g *Group) {
			if d := g.SM.Dump(string(args[2])); d != nil {
				w.Bulk(d)
			} else {
				w.NullBulk()
			}
		})
	default:
		w.Error("ERR bad internal command")
	}
}

// SlotRanges reports slot ownership with each owner's current leader address.
func (r *Router) SlotRanges() []map[string]any {
	var out []map[string]any
	for _, rg := range r.metaSM.Ranges() {
		out = append(out, map[string]any{"start": rg.Start, "end": rg.End, "group": rg.Owner, "leader": r.leaderAddr(rg.Owner)})
	}
	return out
}

// StatusOf returns the local replica's Raft status line for group g.
func (r *Router) StatusOf(g uint32) string {
	st := r.node(g).CurrentStatus()
	return fmt.Sprintf("group:%d state:%s term:%d leader:%d", g, st.State, st.Term, st.Leader)
}

// KeysInSlot counts keys of a slot held by the local replica of group g.
func (r *Router) KeysInSlot(g uint32, slot int) int {
	if grp := r.groups[g]; grp != nil {
		return len(grp.SM.KeysInSlot(slot, NumSlots*1024))
	}
	return 0
}
