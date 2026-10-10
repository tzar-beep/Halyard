// Package store holds the in-memory keyspace.
package store

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *Store { return &Store{data: make(map[string][]byte)} }

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *Store) Set(key string, val []byte) {
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}
