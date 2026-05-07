package query

import (
	"testing"
)

func TestParseSet(t *testing.T) {
	stmt, err := NewParser(`SET user:1 = "alice"`).Parse()
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Type != StmtSet {
		t.Fatal("expected StmtSet")
	}
	if stmt.Key != "user:1" || stmt.Value != "alice" {
		t.Fatalf("unexpected key/value: %s / %s", stmt.Key, stmt.Value)
	}
}

func TestParseGet(t *testing.T) {
	stmt, err := NewParser(`GET user:1`).Parse()
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Type != StmtGet || stmt.Key != "user:1" {
		t.Fatalf("unexpected: %+v", stmt)
	}
}

func TestParseDelete(t *testing.T) {
	stmt, err := NewParser(`DELETE user:1`).Parse()
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Type != StmtDelete || stmt.Key != "user:1" {
		t.Fatalf("unexpected: %+v", stmt)
	}
}

func TestParseSelectEqual(t *testing.T) {
	stmt, err := NewParser(`SELECT value WHERE key = "user:1"`).Parse()
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Type != StmtSelect {
		t.Fatal("expected StmtSelect")
	}
	if stmt.Condition.Op != OpEqual || stmt.Condition.Value != "user:1" {
		t.Fatalf("unexpected condition: %+v", stmt.Condition)
	}
}

func TestParseSelectStartsWith(t *testing.T) {
	stmt, err := NewParser(`SELECT value WHERE key STARTS_WITH "user:"`).Parse()
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Condition.Op != OpStartsWith || stmt.Condition.Value != "user:" {
		t.Fatalf("unexpected condition: %+v", stmt.Condition)
	}
}

func TestParseSelectBetween(t *testing.T) {
	stmt, err := NewParser(`SELECT value WHERE key BETWEEN "a" AND "z"`).Parse()
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Condition.Op != OpBetween {
		t.Fatal("expected OpBetween")
	}
	if stmt.Condition.Value != "a" || stmt.Condition.End != "z" {
		t.Fatalf("unexpected range: %+v", stmt.Condition)
	}
}

func TestParseError(t *testing.T) {
	_, err := NewParser(`INVALID query`).Parse()
	if err == nil {
		t.Fatal("expected parse error")
	}
	t.Logf("correct error: %v", err)
}