package emulator_test

import (
	"testing"

	"github.com/ken109/cloud-tasks-emulator/emulator"
)

// TestEnsureQueueIsIdempotent covers the startup pre-provisioning path used by
// the -queue flag: a queue is created from its full resource name, and asking
// twice is not an error.
func TestEnsureQueueIsIdempotent(t *testing.T) {
	emu := emulator.New(emulator.Config{})
	name := locationPath() + "/queues/preprovisioned"

	if err := emu.EnsureQueue(name); err != nil {
		t.Fatalf("EnsureQueue: %v", err)
	}
	if err := emu.EnsureQueue(name); err != nil {
		t.Fatalf("EnsureQueue (second call): %v", err)
	}
	if _, err := emu.Engine().GetQueue(name); err != nil {
		t.Fatalf("queue was not created: %v", err)
	}
	if err := emu.EnsureQueue("not-a-resource-name"); err == nil {
		t.Error("expected an error for an invalid queue name")
	}
}
