package core

import (
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
