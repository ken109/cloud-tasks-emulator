package emulator_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ken109/cloud-tasks-emulator/internal/emulator"
)

const (
	testProject  = "test-project"
	testLocation = "us-central1"
)

// newClient starts an in-process emulator and returns a connected client.
func newClient(t *testing.T) *cloudtasks.Client {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := emulator.NewServer(emulator.Config{})
	gs := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	client, err := cloudtasks.NewClient(context.Background(),
		option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func locationPath() string {
	return "projects/" + testProject + "/locations/" + testLocation
}

func createQueue(t *testing.T, c *cloudtasks.Client, id string) *taskspb.Queue {
	t.Helper()
	q, err := c.CreateQueue(context.Background(), &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue:  &taskspb.Queue{Name: locationPath() + "/queues/" + id},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	return q
}

func TestQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)

	q := createQueue(t, c, "q1")
	if q.GetState() != taskspb.Queue_RUNNING {
		t.Errorf("new queue state = %v, want RUNNING", q.GetState())
	}
	if q.GetRateLimits().GetMaxDispatchesPerSecond() != 500 {
		t.Errorf("default dispatches/sec = %v, want 500", q.GetRateLimits().GetMaxDispatchesPerSecond())
	}
	if q.GetRetryConfig().GetMaxAttempts() != 100 {
		t.Errorf("default max attempts = %v, want 100", q.GetRetryConfig().GetMaxAttempts())
	}

	// Duplicate create -> AlreadyExists.
	_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue:  &taskspb.Queue{Name: q.GetName()},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("duplicate create err = %v, want AlreadyExists", err)
	}

	got, err := c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: q.GetName()})
	if err != nil || got.GetName() != q.GetName() {
		t.Fatalf("GetQueue: %v / %v", got, err)
	}

	// Pause / resume.
	p, err := c.PauseQueue(ctx, &taskspb.PauseQueueRequest{Name: q.GetName()})
	if err != nil || p.GetState() != taskspb.Queue_PAUSED {
		t.Fatalf("PauseQueue: %v / %v", p, err)
	}
	r, err := c.ResumeQueue(ctx, &taskspb.ResumeQueueRequest{Name: q.GetName()})
	if err != nil || r.GetState() != taskspb.Queue_RUNNING {
		t.Fatalf("ResumeQueue: %v / %v", r, err)
	}

	// List.
	it := c.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: locationPath()})
	n := 0
	for {
		_, err := it.Next()
		if err != nil {
			break
		}
		n++
	}
	if n != 1 {
		t.Errorf("ListQueues count = %d, want 1", n)
	}

	// Delete.
	if err := c.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: q.GetName()}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	_, err = c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: q.GetName()})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetQueue after delete err = %v, want NotFound", err)
	}
}

func TestUpdateQueue(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "qupd")

	q.RateLimits = &taskspb.RateLimits{MaxDispatchesPerSecond: 10}
	updated, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{
		Queue:      q,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits"}},
	})
	if err != nil {
		t.Fatalf("UpdateQueue: %v", err)
	}
	if updated.GetRateLimits().GetMaxDispatchesPerSecond() != 10 {
		t.Errorf("updated dispatches/sec = %v, want 10", updated.GetRateLimits().GetMaxDispatchesPerSecond())
	}
}

func TestTaskDispatchSuccess(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "dispatch")

	var hits int32
	gotHeaders := make(chan http.Header, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		select {
		case gotHeaders <- r.Header.Clone():
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        ts.URL + "/work",
					HttpMethod: taskspb.HttpMethod_POST,
					Body:       []byte("hello"),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	select {
	case h := <-gotHeaders:
		if h.Get("X-CloudTasks-QueueName") != "dispatch" {
			t.Errorf("queue header = %q, want dispatch", h.Get("X-CloudTasks-QueueName"))
		}
		if h.Get("X-CloudTasks-TaskName") == "" {
			t.Error("missing task name header")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("task was not dispatched")
	}

	// After success the task should be removed from the queue.
	waitTasksEmpty(t, c, q.GetName())
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("dispatch count = %d, want 1", got)
	}
}

func TestTaskRetryThenSucceed(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	// Fast retries for the test.
	q, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue: &taskspb.Queue{
			Name: locationPath() + "/queues/retry",
			RetryConfig: &taskspb.RetryConfig{
				MaxAttempts: 5,
				MinBackoff:  durationpb.New(10 * time.Millisecond),
				MaxBackoff:  durationpb.New(20 * time.Millisecond),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: ts.URL, HttpMethod: taskspb.HttpMethod_POST},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	waitTasksEmpty(t, c, q.GetName())
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Errorf("attempts = %d, want 3 (2 failures + 1 success)", got)
	}
}

func TestTaskExhaustsRetries(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue: &taskspb.Queue{
			Name: locationPath() + "/queues/fail",
			RetryConfig: &taskspb.RetryConfig{
				MaxAttempts: 3,
				MinBackoff:  durationpb.New(5 * time.Millisecond),
				MaxBackoff:  durationpb.New(10 * time.Millisecond),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: ts.URL, HttpMethod: taskspb.HttpMethod_POST},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	waitTasksEmpty(t, c, q.GetName())
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Errorf("attempts = %d, want exactly max_attempts=3", got)
	}
}

func TestScheduledTaskDelay(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "sched")

	var fired int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fired, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &taskspb.Task{
			ScheduleTime: timestamppb.New(time.Now().Add(500 * time.Millisecond)),
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: ts.URL, HttpMethod: taskspb.HttpMethod_POST},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt32(&fired) != 0 {
		t.Error("task fired before its schedule time")
	}
	waitTasksEmpty(t, c, q.GetName())
}

func TestDeleteTaskBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "del")

	var fired int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fired, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	task, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &taskspb.Task{
			ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: ts.URL, HttpMethod: taskspb.HttpMethod_POST},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: task.GetName()}); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	_, err = c.GetTask(ctx, &taskspb.GetTaskRequest{Name: task.GetName()})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTask after delete err = %v, want NotFound", err)
	}
	if atomic.LoadInt32(&fired) != 0 {
		t.Error("deleted task should not fire")
	}
}

func TestRunTaskForcesImmediateDispatch(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "run")

	done := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case done <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	task, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &taskspb.Task{
			ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: ts.URL, HttpMethod: taskspb.HttpMethod_POST},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if _, err := c.RunTask(ctx, &taskspb.RunTaskRequest{Name: task.GetName()}); err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTask did not dispatch immediately")
	}
}

func TestPurgeQueue(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "purge")

	for i := 0; i < 3; i++ {
		_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
			Parent: q.GetName(),
			Task: &taskspb.Task{
				ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{Url: "http://example.invalid", HttpMethod: taskspb.HttpMethod_POST},
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}
	if _, err := c.PurgeQueue(ctx, &taskspb.PurgeQueueRequest{Name: q.GetName()}); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	waitTasksEmpty(t, c, q.GetName())
}

func waitTasksEmpty(t *testing.T, c *cloudtasks.Client, queue string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		it := c.ListTasks(context.Background(), &taskspb.ListTasksRequest{Parent: queue})
		n := 0
		for {
			if _, err := it.Next(); err != nil {
				break
			}
			n++
		}
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("queue did not drain in time")
}
