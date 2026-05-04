package replication

import "sync"

type Entry struct {
	Offset uint64
	Line   string
}

// RingBuffer holds the last N WAL entries in memory for replica catch-up
type RingBuffer struct {
	mu      sync.RWMutex
	entries []Entry
	size    int
	head    int // next write position
	count   int // total entries stored
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		entries: make([]Entry, size),
		size:    size,
	}
}

func (r *RingBuffer) Add(e Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[r.head] = e
	r.head = (r.head + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

// Since returns all entries with offset > afterOffset, in order
func (r *RingBuffer) Since(afterOffset uint64) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []Entry
	// walk from oldest to newest
	start := 0
	if r.count == r.size {
		start = r.head // oldest entry when buffer is full
	}
	for i := 0; i < r.count; i++ {
		e := r.entries[(start+i)%r.size]
		if e.Offset > afterOffset {
			result = append(result, e)
		}
	}
	return result
}

// Latest returns the highest offset stored
func (r *RingBuffer) Latest() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return 0
	}
	// newest entry is one behind head
	idx := (r.head - 1 + r.size) % r.size
	return r.entries[idx].Offset
}