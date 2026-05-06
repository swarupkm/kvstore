package mvcc

import (
	"sync"
	"time"
)

// Version is a single historical value of a key
type Version struct {
	TxID      uint64
	Value     string
	Deleted   bool
	ExpiresAt time.Time
}

func (v Version) isExpired() bool {
	return !v.ExpiresAt.IsZero() && time.Now().After(v.ExpiresAt)
}

// VersionChain holds all versions of a single key, newest first
type VersionChain struct {
	mu       sync.RWMutex
	versions []Version
}

func (vc *VersionChain) add(v Version) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	// prepend — newest first
	vc.versions = append([]Version{v}, vc.versions...)
}

// visibleAt returns the latest version visible to a reader with the given txID
func (vc *VersionChain) visibleAt(txID uint64) (Version, bool) {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	for _, v := range vc.versions {
		if v.TxID <= txID {
			if v.Deleted || v.isExpired() {
				return Version{}, false
			}
			return v, true
		}
	}
	return Version{}, false
}

// prune removes versions older than minTxID, keeping at least one
func (vc *VersionChain) prune(minTxID uint64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	cutoff := -1
	for i, v := range vc.versions {
		if v.TxID < minTxID {
			cutoff = i
			break
		}
	}
	if cutoff > 0 {
		vc.versions = vc.versions[:cutoff+1]
	}
}