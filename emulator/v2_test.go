package emulator_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	ctv2 "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ken109/cloud-tasks-emulator/emulator"
)

func v2Only(t *testing.T, cfg emulator.Config) *ctv2.Client {
	v2, _ := startServer(t, cfg)
	return v2
}

func mkQueueV2(t *testing.T, c *ctv2.Client, id string) *taskspb.Queue {
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

func httpTaskV2(url string) *taskspb.Task {
	return &taskspb.Task{
		MessageType: &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
			Url:        url,
			HttpMethod: taskspb.HttpMethod_POST,
			Body:       []byte("payload"),
			Headers:    map[string]string{"X-Test": "1"},
		}},
	}
}

func wantCode(t *testing.T, err error, want codes.Code, ctx string) {
	t.Helper()
	if status.Code(err) != want {
		t.Errorf("%s: err = %v, want %v", ctx, err, want)
	}
}

func TestV2QueueLifecycle(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})

	q := mkQueueV2(t, c, "q1")
	if q.GetState() != taskspb.Queue_RUNNING {
		t.Errorf("state = %v", q.GetState())
	}
	if q.GetRateLimits().GetMaxDispatchesPerSecond() != 500 || q.GetRateLimits().GetMaxBurstSize() != 100 {
		t.Errorf("rate defaults = %+v", q.GetRateLimits())
	}
	if q.GetRetryConfig().GetMaxAttempts() != 100 {
		t.Errorf("max attempts = %d", q.GetRetryConfig().GetMaxAttempts())
	}

	if _, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{Parent: locationPath(), Queue: &taskspb.Queue{Name: q.GetName()}}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("dup create = %v", err)
	}
	if got, err := c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: q.GetName()}); err != nil || got.GetName() != q.GetName() {
		t.Fatalf("GetQueue: %v / %v", got, err)
	}
	if p, err := c.PauseQueue(ctx, &taskspb.PauseQueueRequest{Name: q.GetName()}); err != nil || p.GetState() != taskspb.Queue_PAUSED {
		t.Fatalf("pause: %v / %v", p, err)
	}
	if r, err := c.ResumeQueue(ctx, &taskspb.ResumeQueueRequest{Name: q.GetName()}); err != nil || r.GetState() != taskspb.Queue_RUNNING {
		t.Fatalf("resume: %v / %v", r, err)
	}

	it := c.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: locationPath()})
	n := 0
	for {
		if _, err := it.Next(); err != nil {
			break
		}
		n++
	}
	if n != 1 {
		t.Errorf("list = %d", n)
	}
	if err := c.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: q.GetName()}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: q.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("get after delete = %v", err)
	}
}

func TestV2UpdateQueue(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "upd")

	q.RateLimits = &taskspb.RateLimits{MaxDispatchesPerSecond: 10}
	q.RetryConfig = &taskspb.RetryConfig{MaxAttempts: 7}
	q.AppEngineRoutingOverride = &taskspb.AppEngineRouting{Host: "http://x"}
	updated, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{
		Queue:      q,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits", "retry_config", "app_engine_routing_override"}},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.GetRateLimits().GetMaxDispatchesPerSecond() != 10 || updated.GetRetryConfig().GetMaxAttempts() != 7 {
		t.Errorf("updated = %+v", updated)
	}
	if updated.GetAppEngineRoutingOverride().GetHost() != "http://x" {
		t.Errorf("routing override = %v", updated.GetAppEngineRoutingOverride())
	}

	// Create-on-missing.
	name := locationPath() + "/queues/created-by-update"
	if got, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{Queue: &taskspb.Queue{Name: name}}); err != nil || got.GetName() != name {
		t.Fatalf("update create: %v / %v", got, err)
	}
}

func TestV2DispatchSuccessHeaders(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "dispatch")

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

	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: httpTaskV2(ts.URL)}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	select {
	case h := <-gotHeaders:
		if h.Get("X-CloudTasks-QueueName") != "dispatch" {
			t.Errorf("queue header = %q", h.Get("X-CloudTasks-QueueName"))
		}
		if h.Get("X-CloudTasks-TaskName") == "" {
			t.Error("missing task name header")
		}
		if h.Get("User-Agent") != "Google-Cloud-Tasks" {
			t.Errorf("user-agent = %q", h.Get("User-Agent"))
		}
		if h.Get("X-Test") != "1" {
			t.Error("missing caller header")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("task not dispatched")
	}
	waitTasksEmptyV2(t, c, q.GetName())
}

func TestV2RetryThenSucceed(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue: &taskspb.Queue{
			Name:        locationPath() + "/queues/retry",
			RetryConfig: &taskspb.RetryConfig{MaxAttempts: 5, MinBackoff: durationpb.New(5 * time.Millisecond), MaxBackoff: durationpb.New(10 * time.Millisecond)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var hits int32
	var prevResp string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 2 {
			prevResp = r.Header.Get("X-CloudTasks-TaskPreviousResponse")
		}
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: httpTaskV2(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2(t, c, q.GetName())
	if atomic.LoadInt32(&hits) != 3 {
		t.Errorf("hits = %d, want 3", hits)
	}
	if prevResp != "500" {
		t.Errorf("previous-response header on retry = %q, want 500", prevResp)
	}
}

func TestV2ExhaustRetries(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q, _ := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue: &taskspb.Queue{
			Name:        locationPath() + "/queues/fail",
			RetryConfig: &taskspb.RetryConfig{MaxAttempts: 3, MinBackoff: durationpb.New(2 * time.Millisecond), MaxBackoff: durationpb.New(4 * time.Millisecond)},
		},
	})
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: httpTaskV2(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2(t, c, q.GetName())
	if atomic.LoadInt32(&hits) != 3 {
		t.Errorf("hits = %d, want 3", hits)
	}
}

func TestV2ScheduleDeleteRunPurge(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "ops")

	done := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case done <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Scheduled far out, then RunTask forces immediate dispatch.
	future := httpTaskV2(ts.URL)
	future.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	created, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: future})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RunTask(ctx, &taskspb.RunTaskRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("run: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTask did not dispatch")
	}

	// Delete a pending task.
	t2 := httpTaskV2(ts.URL)
	t2.Name = q.GetName() + "/tasks/to-delete"
	t2.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: t2}); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: t2.GetName()}); err != nil {
		t.Fatalf("delete task: %v", err)
	}

	// Purge.
	for i := 0; i < 3; i++ {
		p := httpTaskV2(ts.URL)
		p.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
		c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: p})
	}
	if _, err := c.PurgeQueue(ctx, &taskspb.PurgeQueueRequest{Name: q.GetName()}); err != nil {
		t.Fatalf("purge: %v", err)
	}
	waitTasksEmptyV2(t, c, q.GetName())
}

func TestV2Views(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "views")

	task := httpTaskV2("http://127.0.0.1:1/x")
	task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	full, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task, ResponseView: taskspb.Task_FULL})
	if err != nil {
		t.Fatal(err)
	}
	if string(full.GetHttpRequest().GetBody()) != "payload" {
		t.Error("FULL body missing")
	}
	// BASIC keeps HTTP body/headers (only App Engine body is stripped).
	basic, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: full.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	if string(basic.GetHttpRequest().GetBody()) != "payload" || basic.GetView() != taskspb.Task_BASIC {
		t.Errorf("BASIC view = %v", basic)
	}
}

func TestV2AppEngineTarget(t *testing.T) {
	ctx := context.Background()
	var gotHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-AppEngine-QueueName")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := v2Only(t, emulator.Config{DefaultAppEngineHost: ts.URL})
	q := mkQueueV2(t, c, "ae")
	task := &taskspb.Task{MessageType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
		RelativeUri: "/work", HttpMethod: taskspb.HttpMethod_POST, Body: []byte("b"),
	}}}
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2(t, c, q.GetName())
	if gotHeader != "ae" {
		t.Errorf("X-AppEngine-QueueName = %q", gotHeader)
	}
}

func TestV2AuthAndRedirect(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "auth")

	var authz string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	task := &taskspb.Task{MessageType: &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
		Url: ts.URL, HttpMethod: taskspb.HttpMethod_POST,
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{OidcToken: &taskspb.OidcToken{ServiceAccountEmail: "sa@x.iam"}},
	}}}
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2(t, c, q.GetName())
	if len(authz) < 8 || authz[:7] != "Bearer " {
		t.Errorf("authorization = %q", authz)
	}

	// Redirects are not followed: a 3xx keeps the task (failed dispatch).
	q2, _ := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue: &taskspb.Queue{Name: locationPath() + "/queues/redir",
			RetryConfig: &taskspb.RetryConfig{MaxAttempts: 1, MinBackoff: durationpb.New(time.Millisecond), MaxBackoff: durationpb.New(time.Millisecond)}},
	})
	var hits int32
	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Redirect(w, r, "http://example.invalid/", http.StatusFound)
	}))
	defer rs.Close()
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q2.GetName(), Task: httpTaskV2(rs.URL)}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2(t, c, q2.GetName())
	if atomic.LoadInt32(&hits) != 1 {
		t.Errorf("redirect hits = %d, want 1 (not followed, dropped after 1 attempt)", hits)
	}
}

func TestV2Pagination(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	for _, id := range []string{"pa", "pb", "pc"} {
		mkQueueV2(t, c, id)
	}
	it := c.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: locationPath(), PageSize: 2})
	// The iterator transparently follows page tokens; assert it returns all 3.
	n := 0
	for {
		if _, err := it.Next(); err != nil {
			break
		}
		n++
	}
	if n != 3 {
		t.Errorf("paged list = %d, want 3", n)
	}
}

func TestV2Validation(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})

	_, err := c.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: "bad"}).Next()
	wantCode(t, err, codes.InvalidArgument, "list bad parent")
	_, err = c.CreateQueue(ctx, &taskspb.CreateQueueRequest{Parent: locationPath(), Queue: &taskspb.Queue{Name: locationPath() + "/queues/bad_id"}})
	wantCode(t, err, codes.InvalidArgument, "bad queue id")
	_, err = c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{Queue: &taskspb.Queue{Name: locationPath() + "/queues/bad_id"}})
	wantCode(t, err, codes.InvalidArgument, "update bad queue id")

	q := mkQueueV2(t, c, "val")
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: &taskspb.Task{}})
	wantCode(t, err, codes.InvalidArgument, "no target")

	far := httpTaskV2("http://127.0.0.1:1")
	far.ScheduleTime = timestamppb.New(time.Now().Add(40 * 24 * time.Hour))
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: far})
	wantCode(t, err, codes.InvalidArgument, "far schedule")

	dl := httpTaskV2("http://127.0.0.1:1")
	dl.DispatchDeadline = durationpb.New(time.Second)
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: dl})
	wantCode(t, err, codes.InvalidArgument, "tiny deadline")

	big := httpTaskV2("http://127.0.0.1:1")
	big.GetHttpRequest().Body = make([]byte, 1024*1024+1)
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: big})
	wantCode(t, err, codes.InvalidArgument, "oversized body")

	_, err = c.GetTask(ctx, &taskspb.GetTaskRequest{Name: "bad"})
	wantCode(t, err, codes.InvalidArgument, "bad task name")
	_, err = c.GetTask(ctx, &taskspb.GetTaskRequest{Name: q.GetName() + "/tasks/nope"})
	wantCode(t, err, codes.NotFound, "missing task")
	if e := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: q.GetName() + "/tasks/nope"}); status.Code(e) != codes.NotFound {
		t.Errorf("delete missing = %v", e)
	}
	_, err = c.RunTask(ctx, &taskspb.RunTaskRequest{Name: q.GetName() + "/tasks/nope"})
	wantCode(t, err, codes.NotFound, "run missing")
}

func TestV2Tombstone(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "tomb")

	name := q.GetName() + "/tasks/reused"
	mk := func() error {
		task := httpTaskV2("http://127.0.0.1:1")
		task.Name = name
		task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
		_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task})
		return err
	}
	if err := mk(); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: name}); err != nil {
		t.Fatal(err)
	}
	if status.Code(mk()) != codes.AlreadyExists {
		t.Error("tombstone should block reuse")
	}
}

func TestV2IAM(t *testing.T) {
	ctx := context.Background()
	c := v2Only(t, emulator.Config{})
	q := mkQueueV2(t, c, "iam")

	pol, err := c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q.GetName()})
	if err != nil || len(pol.GetBindings()) != 0 {
		t.Fatalf("initial policy: %v / %v", pol, err)
	}
	want := &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@b.c"}}}}
	if _, err := c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: q.GetName(), Policy: want}); err != nil {
		t.Fatal(err)
	}
	got, _ := c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q.GetName()})
	if got.GetBindings()[0].GetRole() != "roles/viewer" {
		t.Errorf("policy = %v", got)
	}
	resp, err := c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: q.GetName(), Permissions: []string{"cloudtasks.tasks.create"}})
	if err != nil || len(resp.GetPermissions()) != 1 {
		t.Fatalf("test perms: %v / %v", resp, err)
	}
	_, err = c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "bad"})
	wantCode(t, err, codes.InvalidArgument, "iam bad resource")
}

func waitTasksEmptyV2(t *testing.T, c *ctv2.Client, queue string) {
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
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("queue did not drain")
}
