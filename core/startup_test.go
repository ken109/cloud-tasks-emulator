package core

import (
	"net/http"
	"testing"
	"time"
)

// farFuture keeps a task parked so the test controls when it is removed.
func farFuture() time.Time { return time.Now().Add(time.Hour) }

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

// TestPurgeTombstonesAndHardReset pins the two purge behaviours: by default a
// purged task name stays reserved (as in production), and hard reset drops that
// history so the same name can be recreated immediately.
func TestPurgeTombstonesAndHardReset(t *testing.T) {
	target := Target{Type: TargetHTTP, URL: "http://127.0.0.1:1/never"}
	create := func(e *Engine) (string, string) {
		queue := "projects/p/locations/l/queues/q"
		if err := e.EnsureQueue(queue); err != nil {
			t.Fatalf("EnsureQueue: %v", err)
		}
		task := queue + "/tasks/fixed-name"
		if _, err := e.CreateTask(queue, &Task{Name: task, ScheduleTime: farFuture(), Target: target}); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		return queue, task
	}

	e := NewEngine(Config{})
	queue, task := create(e)
	if _, err := e.PurgeQueue(queue); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if _, err := e.CreateTask(queue, &Task{Name: task, ScheduleTime: farFuture(), Target: target}); err == nil {
		t.Error("a purged task name should stay reserved by default")
	}

	hard := NewEngine(Config{HardResetOnPurge: true})
	queue, task = create(hard)
	if _, err := hard.PurgeQueue(queue); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if _, err := hard.CreateTask(queue, &Task{Name: task, ScheduleTime: farFuture(), Target: target}); err != nil {
		t.Errorf("hard reset should free the purged task name: %v", err)
	}
}

func TestEngineSignerUsesConfiguredIssuer(t *testing.T) {
	e := NewEngine(Config{OpenIDIssuer: "http://localhost:8980"})
	if got := e.Signer().Issuer(); got != "http://localhost:8980" {
		t.Errorf("Signer().Issuer() = %q", got)
	}
	if got := NewEngine(Config{}).Signer().Issuer(); got != defaultOIDCIssuer {
		t.Errorf("default Signer().Issuer() = %q", got)
	}
}

// rawHeader reads a header by its exact key, bypassing the canonicalisation
// http.Header.Get would apply.
func rawHeader(h http.Header, key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// TestSystemHeadersKeepProductionCasing pins the exact header names Cloud Tasks
// documents. Go canonicalises X-CloudTasks-QueueName to X-Cloudtasks-Queuename
// on Set, and a handler matching the documented casing would then miss it.
func TestSystemHeadersKeepProductionCasing(t *testing.T) {
	e := NewEngine(Config{DefaultAppEngineHost: "http://svc"})
	q := &Queue{Name: "projects/p/locations/l/queues/myqueue"}

	httpTask := &Task{Name: q.Name + "/tasks/mytask", Target: Target{Type: TargetHTTP, URL: "http://h/p"}}
	req, err := e.buildRequest(q, httpTask, attemptInfo{number: 1, prevHTTPCode: 500, prevReason: "RETURNED_500"})
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	for key, want := range map[string]string{
		"X-CloudTasks-QueueName":            "myqueue",
		"X-CloudTasks-TaskName":             "mytask",
		"X-CloudTasks-TaskRetryCount":       "0",
		"X-CloudTasks-TaskExecutionCount":   "0",
		"X-CloudTasks-TaskPreviousResponse": "500",
		"X-CloudTasks-TaskRetryReason":      "RETURNED_500",
	} {
		if got := rawHeader(req.Header, key); got != want {
			t.Errorf("%s = %q, want %q (keys: %v)", key, got, want, req.Header)
		}
	}
	if _, canonicalised := req.Header["X-Cloudtasks-Queuename"]; canonicalised {
		t.Error("header was canonicalised; production sends X-CloudTasks-QueueName")
	}

	aeTask := &Task{Name: q.Name + "/tasks/aetask", Target: Target{Type: TargetAppEngine, RelativeURI: "/work"}}
	req, err = e.buildRequest(q, aeTask, attemptInfo{number: 1})
	if err != nil {
		t.Fatalf("buildRequest (app engine): %v", err)
	}
	for _, key := range []string{"X-AppEngine-QueueName", "X-AppEngine-TaskName", "X-AppEngine-FailFast"} {
		if rawHeader(req.Header, key) == "" {
			t.Errorf("missing %s (keys: %v)", key, req.Header)
		}
	}
}
