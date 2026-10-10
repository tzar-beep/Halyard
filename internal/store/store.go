// Package store holds the in-memory keyspace with TTL support.
//
// Concurrency: one RWMutex guards both maps, so every method (including multi-key
// ones like MSet) is atomic with respect to other callers.
//
// Expiry: keys with a TTL are also tracked in `expires` (unix ms). Expired keys are
// removed lazily on access and actively by a background sampler (see expirer).
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
)

// TTL sentinel results, matching Redis.
const (
	NoKey    int64 = -2
	NoExpire int64 = -1
)

type Store struct {
	mu      sync.RWMutex
	data    map[string][]byte
	expires map[string]int64 // key -> absolute expiry, unix ms

	now  func() int64
	stop chan struct{}
	done chan struct{}
}

// New returns a store and starts its active-expiry worker. Call Close to stop it.
func New() *Store { return newStore(func() int64 { return time.Now().UnixMilli() }, true) }

func newStore(now func() int64, active bool) *Store {
	s := &Store{
		data:    make(map[string][]byte),
		expires: make(map[string]int64),
		now:     now,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
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

// expiredLocked reports whether key has an expiry in the past. Caller holds mu (R or W).
func (s *Store) expiredLocked(key string, now int64) bool {
	at, ok := s.expires[key]
	return ok && at <= now
}

// liveLocked returns the value if the key exists and is not expired, deleting it if
// expired. Caller holds mu for writing.
func (s *Store) liveLocked(key string, now int64) ([]byte, bool) {
	if s.expiredLocked(key, now) {
		s.deleteLocked(key)
		return nil, false
	}
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) deleteLocked(key string) bool {
	_, ok := s.data[key]
	delete(s.data, key)
	delete(s.expires, key)
	return ok
}

// Get returns the value for key. Uses a read lock; an expired key is reported missing
// and left for the writer paths / active expirer to remove.
func (s *Store) Get(key string) ([]byte, bool) {
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.expiredLocked(key, now) {
		return nil, false
	}
	v, ok := s.data[key]
	return v, ok
}

// MGet returns one entry per key; nil for missing keys.
func (s *Store) MGet(keys []string) [][]byte {
	now := s.now()
	out := make([][]byte, len(keys))
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, k := range keys {
		if !s.expiredLocked(k, now) {
			out[i] = s.data[k]
		}
	}
	return out
}

type SetCond int

const (
	Always SetCond = iota
	IfNotExists
	IfExists
)

type SetOpts struct {
	Cond     SetCond
	ExpireAt int64 // unix ms; 0 = no expiry
	KeepTTL  bool
}

// Set stores val under key. Returns false if the NX/XX condition prevented the write.
// The caller must not mutate val afterwards.
func (s *Store) Set(key string, val []byte, o SetOpts) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.liveLocked(key, now)
	if (o.Cond == IfNotExists && exists) || (o.Cond == IfExists && !exists) {
		return false
	}
	s.data[key] = val
	switch {
	case o.ExpireAt != 0:
		s.expires[key] = o.ExpireAt
	case !o.KeepTTL:
		delete(s.expires, key)
	}
	return true
}

// MSet sets all pairs atomically and clears their TTLs.
func (s *Store) MSet(keys []string, vals [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, k := range keys {
		s.data[k] = vals[i]
		delete(s.expires, k)
	}
}

// Del removes keys and returns how many existed.
func (s *Store) Del(keys ...string) int {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if _, ok := s.liveLocked(k, now); ok {
			s.deleteLocked(k)
			n++
		}
	}
	return n
}

// Exists counts existing keys; duplicates are counted each time, as in Redis.
func (s *Store) Exists(keys ...string) int {
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok && !s.expiredLocked(k, now) {
			n++
		}
	}
	return n
}

// IncrBy adds delta to the integer stored at key (missing = 0), preserving its TTL.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur int64
	if v, ok := s.liveLocked(key, now); ok {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = n
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	cur += delta
	s.data[key] = strconv.AppendInt(nil, cur, 10)
	return cur, nil
}

// ExpireAt sets an absolute expiry (unix ms). Returns false if key is missing.
// A time in the past deletes the key immediately.
func (s *Store) ExpireAt(key string, at int64) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.liveLocked(key, now); !ok {
		return false
	}
	if at <= now {
		s.deleteLocked(key)
	} else {
		s.expires[key] = at
	}
	return true
}

// PTTL returns remaining ms, NoKey, or NoExpire.
func (s *Store) PTTL(key string) int64 {
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.data[key]; !ok || s.expiredLocked(key, now) {
		return NoKey
	}
	at, ok := s.expires[key]
	if !ok {
		return NoExpire
	}
	return at - now
}

// Persist removes key's TTL. Returns false if key is missing or had no TTL.
func (s *Store) Persist(key string) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.liveLocked(key, now); !ok {
		return false
	}
	if _, ok := s.expires[key]; !ok {
		return false
	}
	delete(s.expires, key)
	return true
}

// Len returns the number of keys, including expired ones not yet reclaimed.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

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
