package store

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// helper to create a store with a temp WAL, cleaned up after each test
func newTestStore(t *testing.T) *Store {
	t.Helper()
	f, err := os.CreateTemp("", "kvstore-test-*.wal")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	wal, err := OpenWAL(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		wal.Close()
		os.Remove(f.Name())
	})

	return New(wal)
}

func TestSetAndGet(t *testing.T) {
	s := newTestStore(t)

	s.Set("name", "gopher")

	val, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if val != "gopher" {
		t.Fatalf("expected 'gopher', got '%s'", val)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := newTestStore(t)

	_, ok := s.Get("ghost")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestDelete(t *testing.T) {
	s := newTestStore(t)

	s.Set("lang", "go")
	s.Delete("lang")

	_, ok := s.Get("lang")
	if ok {
		t.Fatal("expected deleted key to be gone")
	}
}

func TestOverwrite(t *testing.T) {
	s := newTestStore(t)

	s.Set("name", "alice")
	s.Set("name", "bob")

	val, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if val != "bob" {
		t.Fatalf("expected 'bob', got '%s'", val)
	}
}

func TestTTLExpiry(t *testing.T) {
	s := newTestStore(t)

	s.SetWithTTL("session", "abc123", 100*time.Millisecond)

	// should exist immediately
	_, ok := s.Get("session")
	if !ok {
		t.Fatal("expected key to exist before expiry")
	}

	// should be gone after TTL
	time.Sleep(200 * time.Millisecond)
	_, ok = s.Get("session")
	if ok {
		t.Fatal("expected key to be expired")
	}
}

func TestKeys(t *testing.T) {
	s := newTestStore(t)

	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("c", "3")
	s.Delete("b")

	keys := s.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestWALReplay(t *testing.T) {
	f, err := os.CreateTemp("", "kvstore-replay-*.wal")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	// write some data to first store
	wal1, _ := OpenWAL(f.Name())
	s1 := New(wal1)
	s1.Set("city", "bangalore")
	s1.Set("lang", "go")
	s1.Delete("lang")
	wal1.Close()

	// replay into a fresh store
	wal2, _ := OpenWAL(f.Name())
	s2 := New(wal2)
	if err := wal2.Replay(s2); err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	val, ok := s2.Get("city")
	if !ok || val != "bangalore" {
		t.Fatalf("expected 'bangalore', got '%s'", val)
	}
	_, ok = s2.Get("lang")
	if ok {
		t.Fatal("expected deleted key to be absent after replay")
	}
}

func TestMetrics(t *testing.T) {
	s := newTestStore(t)

	s.Set("a", "1")
	s.Set("b", "2")
	s.Get("a")
	s.Get("a")
	s.Delete("b")

	stats := s.Stats()
	if stats.Sets != 2 {
		t.Fatalf("expected 2 sets, got %d", stats.Sets)
	}
	if stats.Gets != 2 {
		t.Fatalf("expected 2 gets, got %d", stats.Gets)
	}
	if stats.Deletes != 1 {
		t.Fatalf("expected 1 delete, got %d", stats.Deletes)
	}
}

func BenchmarkSet(b *testing.B) {
	s := newBenchStore(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		s.Set(key, "value")
	}
}

func BenchmarkGet(b *testing.B) {
	s := newBenchStore(b)

	// pre-populate
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value")
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkSetParallel(b *testing.B) {
	s := newBenchStore(b)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(fmt.Sprintf("key-%d", i), "value")
			i++
		}
	})
}

func BenchmarkGetParallel(b *testing.B) {
	s := newBenchStore(b)

	// pre-populate
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value")
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

func BenchmarkMixedParallel(b *testing.B) {
	s := newBenchStore(b)

	// pre-populate
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value")
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%4 == 0 {
				// 25% writes
				s.Set(fmt.Sprintf("key-%d", i), "value")
			} else {
				// 75% reads
				s.Get(fmt.Sprintf("key-%d", i%1000))
			}
			i++
		}
	})
}

func newBenchStore(b *testing.B) *Store {
	b.Helper()
	f, err := os.CreateTemp("", "kvstore-bench-*.wal")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()

	wal, err := OpenWAL(f.Name())
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		wal.Close()
		os.Remove(f.Name())
	})

	return New(wal)
}
