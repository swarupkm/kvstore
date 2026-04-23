package store

import (
	"sync"
	"time"
)

type Store struct {
	mu      sync.RWMutex
	data    map[string]entry
	index	*index
	wal     *WAL
	metrics metrics
}

func New(wal *WAL) *Store {
	s := &Store{
		data: make(map[string]entry),
		index: newIndex(),
		wal:  wal,
	}
	go s.runExpiry()
	return s
}

func (s *Store) Set(key, value string) error {
	if err := s.wal.Write("SET", key, value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{value: value}
	s.index.add(key)
	s.metrics.incSets()
	return nil
}

func (s *Store) SetWithTTL(key, value string, ttl time.Duration) error {
	if err := s.wal.Write("SET", key, value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
	s.index.add(key)
	s.metrics.incSets()
	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.isExpired() {
		return "", false
	}
	s.metrics.incGets()
	return e.value, true
}

func (s *Store) Delete(key string) error {
	if err := s.wal.Write("DEL", key, ""); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	s.index.remove(key)
	s.metrics.incDeletes()
	return nil
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.index.keys()
	live := make([]string, 0, len(all))
	for _, k := range all {
		if e, ok := s.data[k]; ok  && !e.isExpired() {
			live = append(live, k)
		}
	}
	return live
}

func (s *Store) Range(start, end string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	candidates := s.index.rangeKeys(start, end)
	live := make([]string, 0, len(candidates))
	for _, k := range candidates {
		if e, ok := s.data[k]; ok && !e.isExpired() {
			live = append(live, k)
		}
	}
	return live
}

func (s *Store) Stats() Stats {
	s.mu.RLock()
	liveKeys := 0
	for _, e := range s.data {
		if !e.isExpired() {
			liveKeys++
		}
	}
	s.mu.RUnlock()
	return Stats{
		Keys:    liveKeys,
		Sets:    s.metrics.sets.Load(),
		Gets:    s.metrics.gets.Load(),
		Deletes: s.metrics.deletes.Load(),
	}
}

func (s *Store) Compact() error {
	return s.wal.Compact(s)
}

func (s *Store) runExpiry() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		for k, e := range s.data {
			if e.isExpired() {
				delete(s.data, k)
			}
		}
		s.mu.Unlock()
	}
}