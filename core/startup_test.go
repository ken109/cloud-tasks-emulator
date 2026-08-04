package core

import (
	"testing"
)

func TestEnsureQueue(t *testing.T) {
	e := NewEngine(Config{})
	name := "projects/p/locations/l/queues/q"

	if err := e.EnsureQueue(name); err != nil {
		t.Fatalf("EnsureQueue: %v", err)
	}
	if err := e.EnsureQueue(name); err != nil {
		t.Fatalf("EnsureQueue is not idempotent: %v", err)
	}
	if _, err := e.GetQueue(name); err != nil {
		t.Fatalf("queue missing after EnsureQueue: %v", err)
	}
	if err := e.EnsureQueue("bogus"); err == nil {
		t.Error("expected an error for an invalid queue name")
	}
	if err := e.EnsureQueue("projects/p/locations/l/queues/bad_id"); err == nil {
		t.Error("expected the underlying CreateQueue validation to surface")
	}
}
