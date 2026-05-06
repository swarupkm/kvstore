package mvcc

import (
	"testing"
	"time"
)

func TestBasicReadWrite(t *testing.T) {
	s := NewMVCCStore()

	tx1 := s.Begin(ReadWrite)
	tx1.Set("name", "alice")
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2 := s.Begin(ReadOnly)
	defer tx2.Rollback()
	val, ok := tx2.Get("name")
	if !ok || val != "alice" {
		t.Fatalf("expected alice, got %s", val)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := NewMVCCStore()

	// write initial value
	tx1 := s.Begin(ReadWrite)
	tx1.Set("name", "alice")
	tx1.Commit()

	// reader starts — takes snapshot
	reader := s.Begin(ReadOnly)

	// writer updates concurrently
	tx2 := s.Begin(ReadWrite)
	tx2.Set("name", "bob")
	tx2.Commit()

	// reader still sees "alice" — its snapshot predates tx2's commit
	val, ok := reader.Get("name")
	if !ok || val != "alice" {
		t.Fatalf("snapshot isolation broken: expected alice, got %s", val)
	}
	reader.Rollback()

	// new reader sees "bob"
	tx3 := s.Begin(ReadOnly)
	defer tx3.Rollback()
	val, ok = tx3.Get("name")
	if !ok || val != "bob" {
		t.Fatalf("expected bob, got %s", val)
	}
}

func TestConflictDetection(t *testing.T) {
	s := NewMVCCStore()

	// write initial value
	setup := s.Begin(ReadWrite)
	setup.Set("counter", "0")
	setup.Commit()

	// two writers read and modify the same key concurrently
	w1 := s.Begin(ReadWrite)
	w2 := s.Begin(ReadWrite)

	w1.Set("counter", "1")
	w2.Set("counter", "2")

	// first commit wins
	if err := w1.Commit(); err != nil {
		t.Fatal("w1 should commit cleanly:", err)
	}

	// second should detect conflict
	if err := w2.Commit(); err == nil {
		t.Fatal("w2 should have detected a conflict")
	} else {
		t.Logf("conflict correctly detected: %v", err)
	}
}

func TestRollback(t *testing.T) {
	s := NewMVCCStore()

	tx1 := s.Begin(ReadWrite)
	tx1.Set("key", "value")
	tx1.Rollback() // abandon writes

	tx2 := s.Begin(ReadOnly)
	defer tx2.Rollback()
	_, ok := tx2.Get("key")
	if ok {
		t.Fatal("rolled back write should not be visible")
	}
}

func TestDeleteVisibility(t *testing.T) {
	s := NewMVCCStore()

	tx1 := s.Begin(ReadWrite)
	tx1.Set("key", "value")
	tx1.Commit()

	tx2 := s.Begin(ReadWrite)
	tx2.Delete("key")
	tx2.Commit()

	tx3 := s.Begin(ReadOnly)
	defer tx3.Rollback()
	_, ok := tx3.Get("key")
	if ok {
		t.Fatal("deleted key should not be visible")
	}
}

func TestTTLInMVCC(t *testing.T) {
	s := NewMVCCStore()

	tx1 := s.Begin(ReadWrite)
	tx1.SetWithTTL("session", "tok123", 100*time.Millisecond)
	tx1.Commit()

	tx2 := s.Begin(ReadOnly)
	val, ok := tx2.Get("session")
	if !ok || val != "tok123" {
		t.Fatal("expected session to exist")
	}
	tx2.Rollback()

	time.Sleep(200 * time.Millisecond)

	tx3 := s.Begin(ReadOnly)
	defer tx3.Rollback()
	_, ok = tx3.Get("session")
	if ok {
		t.Fatal("expired key should not be visible")
	}
}