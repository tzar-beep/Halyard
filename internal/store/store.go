// Package store holds the in-memory keyspace: typed values, TTLs, WATCH tracking
// and copy-on-write snapshots.
//
// Concurrency: all access goes through View (shared lock) or Update (exclusive lock),
// so a multi-key command or a whole MULTI/EXEC block runs atomically.
//
// Expiry: keys with a TTL are also tracked in `expires` (unix ms). Expired keys are
// invisible immediately, removed lazily by write transactions, and actively by a
// background sampler (see expirer).
//
// Snapshots: Snapshot bumps an epoch and returns references to the current objects
// (O(keys), no value copying). A write transaction that wants to mutate a container
// stamped with an older epoch clones it first (Tx.Mut), so the snapshot's objects are
// never changed underneath the serializer — the same idea as Redis's fork + COW.
package store

import (
	"sync"
	"time"
)

type Kind uint8

const (
	KindString Kind = iota
	KindList
	KindHash
	KindSet
	KindZSet
)

func (k Kind) String() string {
	return [...]string{"string", "list", "hash", "set", "zset"}[k]
}

// Object is one value. Exactly one payload field is used, according to Kind.
// String payloads are immutable (replaced, never modified in place).
type Object struct {
	Kind Kind
	Str  []byte
	List *List
	Hash map[string][]byte
	Set  map[string]struct{}
	ZSet *ZSet

	epoch uint64
}

func NewString(b []byte) *Object { return &Object{Kind: KindString, Str: b} }

// New returns an empty container of the given kind.
func New(k Kind) *Object {
	o := &Object{Kind: k}
	switch k {
	case KindList:
		o.List = &List{}
	case KindHash:
		o.Hash = map[string][]byte{}
	case KindSet:
		o.Set = map[string]struct{}{}
	case KindZSet:
		o.ZSet = NewZSet()
	}
	return o
}

func (o *Object) clone() *Object {
	c := &Object{Kind: o.Kind, Str: o.Str}
	switch o.Kind {
	case KindList:
		c.List = o.List.clone()
	case KindHash:
		c.Hash = make(map[string][]byte, len(o.Hash))
		for k, v := range o.Hash {
			c.Hash[k] = v
		}
	case KindSet:
		c.Set = make(map[string]struct{}, len(o.Set))
		for k := range o.Set {
			c.Set[k] = struct{}{}
		}
	case KindZSet:
		c.ZSet = o.ZSet.clone()
	}
	return c
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]*Object
	expires  map[string]int64 // key -> absolute expiry, unix ms
	epoch    uint64
	watchers map[string]map[*Watch]struct{}

	now  func() int64
	stop chan struct{}
	done chan struct{}
}

// New returns a store and starts its active-expiry worker. Call Close to stop it.
func NewStore() *Store { return newStore(func() int64 { return time.Now().UnixMilli() }, true) }

// NewReplicatedStore returns a store without the background expirer. Replicas must
// change state only through the replicated log (UpdateAt), so expired keys are
// removed by explicit log entries instead.
func NewReplicatedStore() *Store {
	return newStore(func() int64 { return time.Now().UnixMilli() }, false)
}

func newStore(now func() int64, active bool) *Store {
	s := &Store{
		data:     make(map[string]*Object),
		expires:  make(map[string]int64),
		watchers: make(map[string]map[*Watch]struct{}),
		now:      now,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if active {
		go s.expirer()
	} else {
		close(s.done)
	}
	return s
}

func (s *Store) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// View runs fn with a read-only transaction.
func (s *Store) View(fn func(*Tx)) {
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(&Tx{s: s, now: now})
}

// Update runs fn with an exclusive read-write transaction.
func (s *Store) Update(fn func(*Tx)) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&Tx{s: s, now: now, write: true})
}

// UpdateAt is Update with a caller-supplied clock. Replicas applying the same log
// entry with the same timestamp make identical decisions about expiry.
func (s *Store) UpdateAt(now int64, fn func(*Tx)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&Tx{s: s, now: now, write: true})
}

// Replace discards everything and loads entries (snapshot installation).
func (s *Store) Replace(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.watchers {
		s.touch(k)
	}
	s.data = make(map[string]*Object, len(entries))
	s.expires = make(map[string]int64)
	for _, e := range entries {
		e.Obj.epoch = s.epoch
		s.data[e.Key] = e.Obj
		if e.ExpireAt != 0 {
			s.expires[e.Key] = e.ExpireAt
		}
	}
}

// ExpiredKeys returns up to n keys whose TTL has passed at the given time.
func (s *Store) ExpiredKeys(now int64, n int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for k, at := range s.expires {
		if len(out) == n {
			break
		}
		if at <= now {
			out = append(out, k)
		}
	}
	return out
}

// Tx is a view of the keyspace at a fixed instant. Only valid inside View/Update.
type Tx struct {
	s     *Store
	now   int64
	write bool
}

func (t *Tx) Now() int64 { return t.now }

// Get returns the live object at key, or nil. Never modify the result in place;
// use Mut.
func (t *Tx) Get(key string) *Object {
	o, ok := t.s.data[key]
	if !ok {
		return nil
	}
	if at, ok := t.s.expires[key]; ok && at <= t.now {
		if t.write {
			t.s.deleteLocked(key)
		}
		return nil
	}
	return o
}

// Mut returns key's object for in-place modification, cloning it first if a
// snapshot may still reference it. Returns nil if the key is missing.
func (t *Tx) Mut(key string) *Object {
	o := t.Get(key)
	if o == nil {
		return nil
	}
	if o.epoch < t.s.epoch && o.Kind != KindString {
		o = o.clone()
		o.epoch = t.s.epoch
		t.s.data[key] = o
	}
	t.s.touch(key)
	return o
}

// Put stores o at key, keeping any existing (live) TTL.
func (t *Tx) Put(key string, o *Object) {
	t.Get(key) // drop an expired predecessor along with its TTL
	o.epoch = t.s.epoch
	t.s.data[key] = o
	t.s.touch(key)
}

// Delete removes key. Returns whether it existed.
func (t *Tx) Delete(key string) bool {
	if t.Get(key) == nil {
		return false
	}
	t.s.deleteLocked(key)
	return true
}

func (t *Tx) Exists(key string) bool { return t.Get(key) != nil }

// ExpireAt returns key's absolute expiry in unix ms, or 0 if it has none.
func (t *Tx) ExpireAt(key string) int64 { return t.s.expires[key] }

// SetExpireAt sets an absolute expiry on an existing key; a past time deletes it.
func (t *Tx) SetExpireAt(key string, at int64) {
	if at <= t.now {
		t.s.deleteLocked(key)
		return
	}
	t.s.expires[key] = at
	t.s.touch(key)
}

// Persist removes key's TTL. Returns whether it had one.
func (t *Tx) Persist(key string) bool {
	if _, ok := t.s.expires[key]; !ok {
		return false
	}
	delete(t.s.expires, key)
	t.s.touch(key)
	return true
}

// Len counts keys, including expired ones not yet reclaimed (like Redis DBSIZE).
func (t *Tx) Len() int { return len(t.s.data) }

// Keys calls fn for every live key.
func (t *Tx) Keys(fn func(key string)) {
	for k := range t.s.data {
		if at, ok := t.s.expires[k]; ok && at <= t.now {
			continue
		}
		fn(k)
	}
}

// Flush deletes every key.
func (t *Tx) Flush() {
	for k := range t.s.watchers {
		t.s.touch(k)
	}
	t.s.data = make(map[string]*Object)
	t.s.expires = make(map[string]int64)
}

func (s *Store) deleteLocked(key string) {
	delete(s.data, key)
	delete(s.expires, key)
	s.touch(key)
}

// --- WATCH ---

// Watch tracks keys for optimistic transactions. Any change to a watched key
// (write, delete, expiry) marks it dirty.
type Watch struct {
	dirty   bool
	keys    []string
	existed []bool
}

// Watch adds keys to w.
func (s *Store) Watch(w *Watch, keys []string) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &Tx{s: s, now: now}
	for _, k := range keys {
		ws := s.watchers[k]
		if ws == nil {
			ws = map[*Watch]struct{}{}
			s.watchers[k] = ws
		}
		ws[w] = struct{}{}
		w.keys = append(w.keys, k)
		w.existed = append(w.existed, t.Get(k) != nil)
	}
}

// Unwatch removes all of w's registrations and resets it.
func (s *Store) Unwatch(w *Watch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range w.keys {
		if ws := s.watchers[k]; ws != nil {
			delete(ws, w)
			if len(ws) == 0 {
				delete(s.watchers, k)
			}
		}
	}
	*w = Watch{}
}

// Dirty reports whether any watched key changed since it was watched, including
// keys that have logically expired without being reclaimed yet.
func (t *Tx) Dirty(w *Watch) bool {
	if w.dirty {
		return true
	}
	for i, k := range w.keys {
		if w.existed[i] && t.Get(k) == nil {
			return true
		}
	}
	return w.dirty
}

func (s *Store) touch(key string) {
	for w := range s.watchers[key] {
		w.dirty = true
	}
}

// --- Snapshots ---

// Entry is one key as captured for a snapshot.
type Entry struct {
	Key      string
	Obj      *Object // immutable once captured (see package doc)
	ExpireAt int64   // unix ms, 0 = no expiry
}

// Snapshot returns a point-in-time view of all live keys in O(keys) time.
func (s *Store) Snapshot() []Entry {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	out := make([]Entry, 0, len(s.data))
	for k, o := range s.data {
		at := s.expires[k]
		if at != 0 && at <= now {
			continue
		}
		out = append(out, Entry{Key: k, Obj: o, ExpireAt: at})
	}
	return out
}

// Restore inserts an entry loaded from disk.
func (s *Store) Restore(e Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.Obj.epoch = s.epoch
	s.data[e.Key] = e.Obj
	if e.ExpireAt != 0 {
		s.expires[e.Key] = e.ExpireAt
	} else {
		delete(s.expires, e.Key)
	}
}

// Len returns the number of keys, including expired ones not yet reclaimed.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// --- Active expiry ---

const (
	expireInterval  = 100 * time.Millisecond
	expireSample    = 20
	expireTimeLimit = 25 * time.Millisecond
)

// expirer periodically samples keys with a TTL and deletes expired ones (Redis-style).
// If more than 25% of a sample was expired it samples again, bounded by expireTimeLimit
// so the write lock is never held for long.
func (s *Store) expirer() {
	defer close(s.done)
	t := time.NewTicker(expireInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			deadline := time.Now().Add(expireTimeLimit)
			for s.expireCycle() > expireSample/4 && time.Now().Before(deadline) {
			}
		}
	}
}

// expireCycle checks up to expireSample keys and returns how many it deleted.
// Go's randomized map iteration order provides the sampling.
func (s *Store) expireCycle() int {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	checked, deleted := 0, 0
	for k, at := range s.expires {
		if checked == expireSample {
			break
		}
		checked++
		if at <= now {
			s.deleteLocked(k)
			deleted++
		}
	}
	return deleted
}
