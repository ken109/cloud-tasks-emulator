package emulator_test

import (
	"context"
	"testing"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func wantCode(t *testing.T, err error, want codes.Code, ctx string) {
	t.Helper()
	if status.Code(err) != want {
		t.Errorf("%s: err = %v, want %v", ctx, err, want)
	}
}

func httpTask() *taskspb.Task {
	return &taskspb.Task{
		MessageType: &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
			Url:        "http://127.0.0.1:1/never",
			HttpMethod: taskspb.HttpMethod_POST,
			Body:       []byte("payload"),
			Headers:    map[string]string{"X-Test": "1"},
		}},
	}
}

func TestQueueRPCErrors(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)

	_, err := c.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: "bad"}).Next()
	wantCode(t, err, codes.InvalidArgument, "ListQueues bad parent")

	_, err = c.CreateQueue(ctx, &taskspb.CreateQueueRequest{Parent: "bad", Queue: &taskspb.Queue{}})
	wantCode(t, err, codes.InvalidArgument, "CreateQueue bad parent")

	_, err = c.CreateQueue(ctx, &taskspb.CreateQueueRequest{Parent: locationPath(), Queue: &taskspb.Queue{}})
	wantCode(t, err, codes.InvalidArgument, "CreateQueue empty name")

	_, err = c.CreateQueue(ctx, &taskspb.CreateQueueRequest{Parent: locationPath(), Queue: &taskspb.Queue{Name: "bad-name"}})
	wantCode(t, err, codes.InvalidArgument, "CreateQueue invalid name")

	// Queue id with an underscore is invalid (queue ids forbid underscores).
	_, err = c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue:  &taskspb.Queue{Name: locationPath() + "/queues/bad_id"},
	})
	wantCode(t, err, codes.InvalidArgument, "CreateQueue invalid queue id")

	_, err = c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: locationPath(),
		Queue:  &taskspb.Queue{Name: "projects/other/locations/l/queues/q"},
	})
	wantCode(t, err, codes.InvalidArgument, "CreateQueue parent mismatch")

	_, err = c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: locationPath() + "/queues/missing"})
	wantCode(t, err, codes.NotFound, "GetQueue missing")

	if e := c.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: locationPath() + "/queues/missing"}); status.Code(e) != codes.NotFound {
		t.Errorf("DeleteQueue missing = %v", e)
	}

	_, err = c.PurgeQueue(ctx, &taskspb.PurgeQueueRequest{Name: locationPath() + "/queues/missing"})
	wantCode(t, err, codes.NotFound, "PurgeQueue missing")

	_, err = c.PauseQueue(ctx, &taskspb.PauseQueueRequest{Name: locationPath() + "/queues/missing"})
	wantCode(t, err, codes.NotFound, "PauseQueue missing")
}

func TestUpdateQueueErrorsAndPaths(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)

	// Nil queue name.
	_, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{Queue: &taskspb.Queue{}})
	wantCode(t, err, codes.InvalidArgument, "UpdateQueue empty name")

	_, err = c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{Queue: &taskspb.Queue{Name: "bad"}})
	wantCode(t, err, codes.InvalidArgument, "UpdateQueue invalid name")

	// Create-if-missing.
	name := locationPath() + "/queues/upd2"
	created, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{Queue: &taskspb.Queue{Name: name}})
	if err != nil || created.GetName() != name {
		t.Fatalf("UpdateQueue create-if-missing: %v / %v", created, err)
	}

	// Update every mask path, plus empty-mask (wholesale) update.
	full := &taskspb.Queue{
		Name:                     name,
		RetryConfig:              &taskspb.RetryConfig{MaxAttempts: 7},
		AppEngineRoutingOverride: &taskspb.AppEngineRouting{Host: "http://x"},
		StackdriverLoggingConfig: &taskspb.StackdriverLoggingConfig{SamplingRatio: 0.5},
	}
	_, err = c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{
		Queue:      full,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_config", "app_engine_routing_override", "stackdriver_logging_config"}},
	})
	if err != nil {
		t.Fatalf("UpdateQueue mask paths: %v", err)
	}
	got, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{Queue: full}) // empty mask
	if err != nil {
		t.Fatalf("UpdateQueue empty mask: %v", err)
	}
	if got.GetRetryConfig().GetMaxAttempts() != 7 {
		t.Errorf("retry max attempts = %d, want 7", got.GetRetryConfig().GetMaxAttempts())
	}
}

func TestTaskRPCErrors(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "terr")

	_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: "bad", Task: httpTask()})
	wantCode(t, err, codes.InvalidArgument, "CreateTask bad parent")

	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName()})
	wantCode(t, err, codes.InvalidArgument, "CreateTask nil task")

	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: &taskspb.Task{}})
	wantCode(t, err, codes.InvalidArgument, "CreateTask no target")

	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: locationPath() + "/queues/missing", Task: httpTask(),
	})
	wantCode(t, err, codes.NotFound, "CreateTask missing queue")

	// Invalid explicit task name.
	bad := httpTask()
	bad.Name = "not-a-task"
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: bad})
	wantCode(t, err, codes.InvalidArgument, "CreateTask invalid name")

	// Task name parent mismatch.
	mism := httpTask()
	mism.Name = locationPath() + "/queues/other/tasks/t1"
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: mism})
	wantCode(t, err, codes.InvalidArgument, "CreateTask name/parent mismatch")

	// Valid explicit name, then duplicate -> AlreadyExists.
	named := httpTask()
	named.Name = q.GetName() + "/tasks/explicit-1"
	named.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: named}); err != nil {
		t.Fatalf("CreateTask explicit name: %v", err)
	}
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: named})
	wantCode(t, err, codes.AlreadyExists, "CreateTask duplicate")

	// Invalid task id (passes name shape but fails the id charset).
	badID := httpTask()
	badID.Name = q.GetName() + "/tasks/bad.id"
	_, err = c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: badID})
	wantCode(t, err, codes.InvalidArgument, "CreateTask invalid id")

	// GetTask invalid + missing queue + missing task.
	_, err = c.GetTask(ctx, &taskspb.GetTaskRequest{Name: "bad"})
	wantCode(t, err, codes.InvalidArgument, "GetTask invalid name")
	_, err = c.GetTask(ctx, &taskspb.GetTaskRequest{Name: locationPath() + "/queues/missing/tasks/t"})
	wantCode(t, err, codes.NotFound, "GetTask missing queue")
	_, err = c.GetTask(ctx, &taskspb.GetTaskRequest{Name: q.GetName() + "/tasks/nope"})
	wantCode(t, err, codes.NotFound, "GetTask missing")

	// ListTasks missing queue.
	_, err = c.ListTasks(ctx, &taskspb.ListTasksRequest{Parent: locationPath() + "/queues/missing"}).Next()
	wantCode(t, err, codes.NotFound, "ListTasks missing queue")

	// DeleteTask invalid + missing queue + missing task.
	if e := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: "bad"}); status.Code(e) != codes.InvalidArgument {
		t.Errorf("DeleteTask invalid = %v", e)
	}
	if e := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: locationPath() + "/queues/missing/tasks/t"}); status.Code(e) != codes.NotFound {
		t.Errorf("DeleteTask missing queue = %v", e)
	}
	if e := c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: q.GetName() + "/tasks/nope"}); status.Code(e) != codes.NotFound {
		t.Errorf("DeleteTask missing task = %v", e)
	}

	// RunTask invalid + missing queue + missing task.
	_, err = c.RunTask(ctx, &taskspb.RunTaskRequest{Name: "bad"})
	wantCode(t, err, codes.InvalidArgument, "RunTask invalid")
	_, err = c.RunTask(ctx, &taskspb.RunTaskRequest{Name: locationPath() + "/queues/missing/tasks/t"})
	wantCode(t, err, codes.NotFound, "RunTask missing queue")
	_, err = c.RunTask(ctx, &taskspb.RunTaskRequest{Name: q.GetName() + "/tasks/nope"})
	wantCode(t, err, codes.NotFound, "RunTask missing task")
}

func TestFullViewReturnsBodyAndHeaders(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "view")

	task := httpTask()
	task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	created, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: q.GetName(), Task: task, ResponseView: taskspb.Task_FULL,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if body := created.GetHttpRequest().GetBody(); string(body) != "payload" {
		t.Errorf("FULL view body = %q, want payload", body)
	}

	// BASIC view (default) keeps HttpRequest body/headers: per the Task.View
	// docs only the AppEngineHttpRequest body is omitted in BASIC.
	basic, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if string(basic.GetHttpRequest().GetBody()) != "payload" {
		t.Errorf("BASIC view should keep HTTP body, got %q", basic.GetHttpRequest().GetBody())
	}
	if basic.GetView() != taskspb.Task_BASIC {
		t.Errorf("view = %v, want BASIC", basic.GetView())
	}

	// FULL via ListTasks.
	it := c.ListTasks(ctx, &taskspb.ListTasksRequest{Parent: q.GetName(), ResponseView: taskspb.Task_FULL})
	lt, err := it.Next()
	if err != nil || string(lt.GetHttpRequest().GetBody()) != "payload" {
		t.Errorf("ListTasks FULL = %v / %v", lt, err)
	}
}

func TestAppEngineBasicViewStrip(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "aeview")

	task := &taskspb.Task{
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		MessageType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
			RelativeUri: "/x",
			Body:        []byte("b"),
			Headers:     map[string]string{"k": "v"},
		}},
	}
	created, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if created.GetAppEngineHttpRequest().GetBody() != nil {
		t.Error("BASIC view should strip app engine body")
	}
	// Headers are NOT stripped in BASIC (only the body is).
	if created.GetAppEngineHttpRequest().GetHeaders()["k"] != "v" {
		t.Error("BASIC view should keep app engine headers")
	}
}

func TestIamPolicy(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	q := createQueue(t, c, "iam")

	// Empty policy initially.
	pol, err := c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q.GetName()})
	if err != nil || len(pol.GetBindings()) != 0 {
		t.Fatalf("initial GetIamPolicy: %v / %v", pol, err)
	}

	// Set then get round-trips.
	want := &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@b.c"}}}}
	set, err := c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: q.GetName(), Policy: want})
	if err != nil || set.GetBindings()[0].GetRole() != "roles/viewer" {
		t.Fatalf("SetIamPolicy: %v / %v", set, err)
	}
	got, _ := c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q.GetName()})
	if got.GetBindings()[0].GetRole() != "roles/viewer" {
		t.Errorf("GetIamPolicy after set = %v", got)
	}

	// TestIamPermissions echoes the requested permissions.
	perms := []string{"cloudtasks.tasks.create"}
	resp, err := c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: q.GetName(), Permissions: perms})
	if err != nil || len(resp.GetPermissions()) != 1 {
		t.Fatalf("TestIamPermissions: %v / %v", resp, err)
	}

	// Errors: invalid + missing resource.
	_, err = c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "bad"})
	wantCode(t, err, codes.InvalidArgument, "GetIamPolicy invalid")
	_, err = c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: "bad", Policy: want})
	wantCode(t, err, codes.InvalidArgument, "SetIamPolicy invalid")
	_, err = c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: "bad"})
	wantCode(t, err, codes.InvalidArgument, "TestIamPermissions invalid")
	_, err = c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: locationPath() + "/queues/missing"})
	wantCode(t, err, codes.NotFound, "GetIamPolicy missing")
	_, err = c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: locationPath() + "/queues/missing", Policy: want})
	wantCode(t, err, codes.NotFound, "SetIamPolicy missing")
	_, err = c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: locationPath() + "/queues/missing"})
	wantCode(t, err, codes.NotFound, "TestIamPermissions missing")
}
