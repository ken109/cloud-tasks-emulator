package emulator

import (
	"context"
	"encoding/base64"
	"strconv"
	"testing"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testParent = "projects/p/locations/l"

func encodeOffset(n int) string { return encodeRaw(strconv.Itoa(n)) }
func encodeRaw(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func serverWithQueue(t *testing.T, cfg Config, queueID string) (*Server, string) {
	t.Helper()
	s := NewServer(cfg)
	name := testParent + "/queues/" + queueID
	if _, err := s.CreateQueue(context.Background(), &taskspb.CreateQueueRequest{
		Parent: testParent,
		Queue:  &taskspb.Queue{Name: name},
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	return s, name
}

func httpReqTask(body []byte) *taskspb.Task {
	return &taskspb.Task{
		MessageType: &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
			Url:        "http://127.0.0.1:1/x",
			HttpMethod: taskspb.HttpMethod_POST,
			Body:       body,
		}},
	}
}

func TestCreateTaskValidation(t *testing.T) {
	ctx := context.Background()
	s, q := serverWithQueue(t, Config{}, "v")

	create := func(task *taskspb.Task) error {
		_, err := s.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q, Task: task})
		return err
	}

	// schedule_time too far in the future.
	far := httpReqTask(nil)
	far.ScheduleTime = timestamppb.New(time.Now().Add(40 * 24 * time.Hour))
	if status.Code(create(far)) != codes.InvalidArgument {
		t.Error("expected InvalidArgument for far schedule_time")
	}

	// dispatch_deadline too small / too large.
	small := httpReqTask(nil)
	small.DispatchDeadline = durationpb.New(time.Second)
	if status.Code(create(small)) != codes.InvalidArgument {
		t.Error("expected InvalidArgument for tiny deadline")
	}
	big := httpReqTask(nil)
	big.DispatchDeadline = durationpb.New(time.Hour)
	if status.Code(create(big)) != codes.InvalidArgument {
		t.Error("expected InvalidArgument for huge deadline")
	}

	// HTTP body over 1MB.
	if status.Code(create(httpReqTask(make([]byte, maxHTTPTaskBodySize+1)))) != codes.InvalidArgument {
		t.Error("expected InvalidArgument for oversized HTTP body")
	}

	// App Engine body over 100KB.
	ae := &taskspb.Task{MessageType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
		RelativeUri: "/x",
		Body:        make([]byte, maxAppEngineTaskBodySize+1),
	}}}
	if status.Code(create(ae)) != codes.InvalidArgument {
		t.Error("expected InvalidArgument for oversized App Engine body")
	}

	// A valid deadline at the boundary is accepted.
	ok := httpReqTask(nil)
	ok.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	ok.DispatchDeadline = durationpb.New(minDispatchDeadline)
	if err := create(ok); err != nil {
		t.Errorf("valid task rejected: %v", err)
	}
}

func TestTombstoneBlocksReuse(t *testing.T) {
	ctx := context.Background()
	s, q := serverWithQueue(t, Config{}, "tomb")

	name := q + "/tasks/reused"
	mk := func() error {
		task := httpReqTask(nil)
		task.Name = name
		task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
		_, err := s.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q, Task: task})
		return err
	}
	if err := mk(); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := s.deleteTask(name); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if status.Code(mk()) != codes.AlreadyExists {
		t.Error("recreating a deleted task name should be blocked by the tombstone")
	}
}

func TestTombstoneDisabledAndCleanup(t *testing.T) {
	ctx := context.Background()

	// Disabled tombstones: reuse is immediately allowed.
	s, q := serverWithQueue(t, Config{TombstoneTTL: -1}, "nt")
	name := q + "/tasks/x"
	mk := func() error {
		task := httpReqTask(nil)
		task.Name = name
		task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
		_, err := s.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q, Task: task})
		return err
	}
	if err := mk(); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteTask(name); err != nil {
		t.Fatal(err)
	}
	if err := mk(); err != nil {
		t.Errorf("disabled tombstone should allow reuse: %v", err)
	}

	// Short tombstone TTL: the cleanup timer removes the reservation.
	s2, q2 := serverWithQueue(t, Config{TombstoneTTL: 20 * time.Millisecond}, "st")
	name2 := q2 + "/tasks/y"
	task := httpReqTask(nil)
	task.Name = name2
	task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	if _, err := s2.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q2, Task: task}); err != nil {
		t.Fatal(err)
	}
	if err := s2.deleteTask(name2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond) // let the cleanup timer fire
	task2 := httpReqTask(nil)
	task2.Name = name2
	task2.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	if _, err := s2.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q2, Task: task2}); err != nil {
		t.Errorf("after tombstone TTL expiry reuse should be allowed: %v", err)
	}
}

func TestTaskTTLExpiry(t *testing.T) {
	ctx := context.Background()
	s, q := serverWithQueue(t, Config{TaskTTL: 30 * time.Millisecond}, "ttl")

	task := httpReqTask(nil)
	task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour)) // won't dispatch
	created, err := s.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q, Task: task})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := s.GetTask(ctx, &taskspb.GetTaskRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("task should be deleted after TTL, got %v", err)
	}
}

// deleteTask is a tiny helper to delete by name and ignore the empty response.
func (s *Server) deleteTask(name string) error {
	_, err := s.DeleteTask(context.Background(), &taskspb.DeleteTaskRequest{Name: name})
	return err
}

func TestPaginateHelper(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e"}

	// First page with an explicit size and a next token.
	page, next, err := paginate(names, 2, "")
	if err != nil || len(page) != 2 || next == "" {
		t.Fatalf("page1 = %v, next=%q, err=%v", page, next, err)
	}
	// Following the token yields the next slice.
	page, next, err = paginate(names, 2, next)
	if err != nil || page[0] != "c" || next == "" {
		t.Fatalf("page2 = %v, next=%q, err=%v", page, next, err)
	}
	// Last page has no next token.
	page, next, err = paginate(names, 2, next)
	if err != nil || len(page) != 1 || page[0] != "e" || next != "" {
		t.Fatalf("page3 = %v, next=%q, err=%v", page, next, err)
	}

	// Default page size when <= 0.
	page, next, _ = paginate(names, 0, "")
	if len(page) != 5 || next != "" {
		t.Errorf("default size page = %v, next=%q", page, next)
	}
	// Size capped at maxPageSize.
	if _, _, err := paginate(names, maxPageSize+50, ""); err != nil {
		t.Errorf("capped size err: %v", err)
	}
	// Offset beyond the end clamps to empty.
	page, next, _ = paginate(names, 2, encodeOffset(99))
	if len(page) != 0 || next != "" {
		t.Errorf("out-of-range page = %v, next=%q", page, next)
	}
	// Malformed (non-base64) token.
	if _, _, err := paginate(names, 2, "###"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad base64 token err = %v", err)
	}
	// Base64 of a non-integer.
	if _, _, err := paginate(names, 2, encodeRaw("abc")); status.Code(err) != codes.InvalidArgument {
		t.Errorf("non-integer token err = %v", err)
	}
	// Base64 of a negative integer.
	if _, _, err := paginate(names, 2, encodeRaw("-1")); status.Code(err) != codes.InvalidArgument {
		t.Errorf("negative token err = %v", err)
	}
}

func TestListPaginationRPC(t *testing.T) {
	ctx := context.Background()
	s, _ := serverWithQueue(t, Config{}, "lp1")
	serverCreateQueue(t, s, "lp2")
	serverCreateQueue(t, s, "lp3")

	p1, err := s.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: testParent, PageSize: 2})
	if err != nil || len(p1.GetQueues()) != 2 || p1.GetNextPageToken() == "" {
		t.Fatalf("queues page1 = %d, token=%q, err=%v", len(p1.GetQueues()), p1.GetNextPageToken(), err)
	}
	p2, err := s.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: testParent, PageSize: 2, PageToken: p1.GetNextPageToken()})
	if err != nil || len(p2.GetQueues()) != 1 || p2.GetNextPageToken() != "" {
		t.Fatalf("queues page2 = %d, token=%q, err=%v", len(p2.GetQueues()), p2.GetNextPageToken(), err)
	}
	if _, err := s.ListQueues(ctx, &taskspb.ListQueuesRequest{Parent: testParent, PageToken: "###"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListQueues bad token = %v", err)
	}

	// Tasks pagination.
	q := testParent + "/queues/lp1"
	for i := 0; i < 3; i++ {
		task := httpReqTask(nil)
		task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
		if _, err := s.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q, Task: task}); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}
	tp1, err := s.ListTasks(ctx, &taskspb.ListTasksRequest{Parent: q, PageSize: 2})
	if err != nil || len(tp1.GetTasks()) != 2 || tp1.GetNextPageToken() == "" {
		t.Fatalf("tasks page1 = %d, token=%q, err=%v", len(tp1.GetTasks()), tp1.GetNextPageToken(), err)
	}
	if _, err := s.ListTasks(ctx, &taskspb.ListTasksRequest{Parent: q, PageToken: "###"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListTasks bad token = %v", err)
	}
}

func serverCreateQueue(t *testing.T, s *Server, id string) {
	t.Helper()
	if _, err := s.CreateQueue(context.Background(), &taskspb.CreateQueueRequest{
		Parent: testParent,
		Queue:  &taskspb.Queue{Name: testParent + "/queues/" + id},
	}); err != nil {
		t.Fatalf("CreateQueue %s: %v", id, err)
	}
}

func TestStopAndExpireInternals(t *testing.T) {
	ctx := context.Background()
	s, q := serverWithQueue(t, Config{}, "si")

	// A pending task means DeleteQueue -> stop() must tear down its ttlTimer.
	task := httpReqTask(nil)
	task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	if _, err := s.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q, Task: task}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := s.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: q}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}

	// expire() on an already-removed task is a no-op.
	qs := newQueueState(s, &taskspb.Queue{Name: testParent + "/queues/z"})
	qs.expire(&taskState{pb: &taskspb.Task{Name: "x"}, removed: true})
}

func TestBuildAppEngineMalformedURL(t *testing.T) {
	s := NewServer(Config{DefaultAppEngineHost: "http://svc"})
	queue := &taskspb.Queue{Name: "projects/p/locations/l/queues/q"}
	task := &taskspb.Task{
		Name: "projects/p/locations/l/queues/q/tasks/t",
		MessageType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
			RelativeUri: "/bad\n/uri",
		}},
	}
	if _, err := s.buildRequest(queue, task, attemptInfo{number: 1}); err == nil {
		t.Error("expected error for malformed App Engine URL")
	}
}
