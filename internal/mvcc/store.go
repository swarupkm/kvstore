package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrConflict = errors.New("transaction conflict: key modified by concurrent writer")

type MVCCStore struct {
	mu       sync.RWMutex
	data     map[string]*VersionChain
	txCounter atomic.Uint64
	// track active transaction IDs for garbage collection
	activeMu sync.Mutex
	activeTx map[uint64]struct{}
}

func NewMVCCStore() *MVCCStore {
	return &MVCCStore{
		data:     make(map[string]*VersionChain),
		activeTx: make(map[uint64]struct{}),
	}
}

// Begin starts a new transaction
func (s *MVCCStore) Begin(txType TxType) *Tx {
	id := s.txCounter.Add(1)
	s.activeMu.Lock()
	s.activeTx[id] = struct{}{}
	s.activeMu.Unlock()

	return &Tx{
		ID:    id,
		Type:  txType,
		store: s,
		writes: make(map[string]Version),
	}
}

func (s *MVCCStore) getAt(key string, txID uint64) (string, bool) {
	s.mu.RLock()
	chain, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	v, ok := chain.visibleAt(txID)
	if !ok {
		return "", false
	}
	return v.Value, true
}

// commit applies buffered writes with conflict detection
func (s *MVCCStore) commit(tx *Tx) error {
	if len(tx.writes) == 0 {
		s.releaseTx(tx.ID)
		return nil
	}

	// acquire write lock for the commit phase
	s.mu.Lock()
	defer s.mu.Unlock()

	// conflict detection: check if any key was written by a
	// transaction that started after this transaction's snapshot
	for key := range tx.writes {
		chain, ok := s.data[key]
		if !ok {
			continue
		}
		chain.mu.RLock()
		for _, v := range chain.versions {
			if v.TxID > tx.ID {
				chain.mu.RUnlock()
				s.releaseTx(tx.ID)
				return fmt.Errorf("%w: key=%s", ErrConflict, key)
			}
			break // only need to check the latest version
		}
		chain.mu.RUnlock()
	}

	// apply writes — get a new commit ID so the version is
	// visible only after commit, not at the transaction's start ID
	commitID := s.txCounter.Add(1)

	for key, v := range tx.writes {
		chain, ok := s.data[key]
		if !ok {
			chain = &VersionChain{}
			s.data[key] = chain
		}
		v.TxID = commitID
		chain.add(v)
	}

	s.releaseTx(tx.ID)
	go s.gc() // clean old versions in background
	return nil
}

func (s *MVCCStore) releaseTx(txID uint64) {
	s.activeMu.Lock()
	delete(s.activeTx, txID)
	s.activeMu.Unlock()
}

// gc prunes version chains older than the oldest active transaction
func (s *MVCCStore) gc() {
	s.activeMu.Lock()
	minTx := ^uint64(0) // max uint64
	for id := range s.activeTx {
		if id < minTx {
			minTx = id
		}
	}
	s.activeMu.Unlock()

	s.mu.RLock()
	chains := make([]*VersionChain, 0, len(s.data))
	for _, chain := range s.data {
		chains = append(chains, chain)
	}
	s.mu.RUnlock()

	for _, chain := range chains {
		chain.prune(minTx)
	}
}

// Keys returns all live keys visible to a given transaction ID
func (s *MVCCStore) Keys(txID uint64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k, chain := range s.data {
		if _, ok := chain.visibleAt(txID); ok {
			keys = append(keys, k)
		}
	}
	return keys
}