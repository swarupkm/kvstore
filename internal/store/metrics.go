package store

import "sync/atomic"

type metrics struct {
	sets    atomic.Uint64
	gets    atomic.Uint64
	deletes atomic.Uint64
}

func (m *metrics) incSets()    { m.sets.Add(1) }
func (m *metrics) incGets()    { m.gets.Add(1) }
func (m *metrics) incDeletes() { m.deletes.Add(1) }

type Stats struct {
	Keys    int
	Sets    uint64
	Gets    uint64
	Deletes uint64
}