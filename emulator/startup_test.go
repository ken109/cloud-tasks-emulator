package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// TestOpenIDHandler checks the emulator exposes the discovery endpoints that
// let a task target verify the OIDC tokens it receives.
func TestOpenIDHandler(t *testing.T) {
	emu := emulator.New(emulator.Config{OpenIDIssuer: "http://localhost:8980"})

	rec := httptest.NewRecorder()
	emu.OpenIDHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/openid-configuration", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("discovery = %d", rec.Code)
	}
	var doc struct {
		Issuer string `json:"issuer"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("discovery body: %v", err)
	}
	if doc.Issuer != "http://localhost:8980" {
		t.Errorf("issuer = %q", doc.Issuer)
	}
}
