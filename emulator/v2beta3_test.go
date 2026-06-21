package emulator_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	ctv2beta3 "cloud.google.com/go/cloudtasks/apiv2beta3"
	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ken109/cloud-tasks-emulator/emulator"
)

func v2beta3Only(t *testing.T, cfg emulator.Config) *ctv2beta3.Client {
	_, v2beta3 := startServer(t, cfg)
	return v2beta3
}

func mkQueueV2beta3(t *testing.T, c *ctv2beta3.Client, q *taskspb.Queue) *taskspb.Queue {
	t.Helper()
	out, err := c.CreateQueue(context.Background(), &taskspb.CreateQueueRequest{Parent: locationPath(), Queue: q})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	return out
}

func httpTaskV2beta3(u string) *taskspb.Task {
	return &taskspb.Task{PayloadType: &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
		Url: u, HttpMethod: taskspb.HttpMethod_POST, Body: []byte("payload"),
	}}}
}

func TestV2beta3QueueDefaultsAndStats(t *testing.T) {
	ctx := context.Background()
	c := v2beta3Only(t, emulator.Config{})
	q := mkQueueV2beta3(t, c, &taskspb.Queue{Name: locationPath() + "/queues/q1"})

	if q.GetType() != taskspb.Queue_PUSH {
		t.Errorf("type = %v, want PUSH", q.GetType())
	}
	if q.GetRateLimits().GetMaxBurstSize() != 100 || q.GetRetryConfig().GetMaxAttempts() != 100 {
		t.Errorf("defaults = %+v / %+v", q.GetRateLimits(), q.GetRetryConfig())
	}
	if q.GetStats() == nil {
		t.Error("stats missing")
	}

	// Add a pending task and confirm stats.tasks_count reflects it.
	task := httpTaskV2beta3("http://127.0.0.1:1/x")
	task.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task}); err != nil {
		t.Fatal(err)
	}
	got, _ := c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: q.GetName()})
	if got.GetStats().GetTasksCount() != 1 {
		t.Errorf("tasks_count = %d, want 1", got.GetStats().GetTasksCount())
	}
}

func TestV2beta3QueueTTLRoundTrip(t *testing.T) {
	c := v2beta3Only(t, emulator.Config{})
	q := mkQueueV2beta3(t, c, &taskspb.Queue{
		Name:         locationPath() + "/queues/ttl",
		TaskTtl:      durationpb.New(2 * time.Hour),
		TombstoneTtl: durationpb.New(30 * time.Minute),
	})
	if q.GetTaskTtl().AsDuration() != 2*time.Hour || q.GetTombstoneTtl().AsDuration() != 30*time.Minute {
		t.Errorf("ttl round-trip = %v / %v", q.GetTaskTtl().AsDuration(), q.GetTombstoneTtl().AsDuration())
	}
}

func TestV2beta3PullQueue(t *testing.T) {
	ctx := context.Background()
	c := v2beta3Only(t, emulator.Config{})
	q := mkQueueV2beta3(t, c, &taskspb.Queue{Name: locationPath() + "/queues/pull", Type: taskspb.Queue_PULL})
	if q.GetType() != taskspb.Queue_PULL {
		t.Fatalf("type = %v", q.GetType())
	}

	// A pull-message task is stored but never auto-dispatched.
	task := &taskspb.Task{PayloadType: &taskspb.Task_PullMessage{PullMessage: &taskspb.PullMessage{Payload: []byte("p"), Tag: "tag"}}}
	created, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetPullMessage().GetTag() != "tag" {
		t.Errorf("pull tag = %q", created.GetPullMessage().GetTag())
	}
	// It stays in the queue (not dispatched).
	time.Sleep(150 * time.Millisecond)
	got, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: created.GetName()})
	if err != nil || got.GetDispatchCount() != 0 {
		t.Errorf("pull task should not dispatch: %v / %v", got, err)
	}
	// RunTask on a pull queue is rejected.
	if _, err := c.RunTask(ctx, &taskspb.RunTaskRequest{Name: created.GetName()}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RunTask on pull queue = %v, want FailedPrecondition", err)
	}
}

func TestV2beta3HTTPTargetOverride(t *testing.T) {
	ctx := context.Background()

	var gotPath, gotHeader, gotAuthz string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotHeader = r.Header.Get("X-Override")
		gotAuthz = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	port, _ := strconv.ParseInt(u.Port(), 10, 64)
	scheme := taskspb.UriOverride_HTTP
	host := u.Hostname()

	c := v2beta3Only(t, emulator.Config{})
	q := mkQueueV2beta3(t, c, &taskspb.Queue{
		Name: locationPath() + "/queues/httptarget",
		HttpTarget: &taskspb.HttpTarget{
			UriOverride: &taskspb.UriOverride{
				Scheme:                 &scheme,
				Host:                   &host,
				Port:                   &port,
				PathOverride:           &taskspb.PathOverride{Path: "/overridden"},
				UriOverrideEnforceMode: taskspb.UriOverride_ALWAYS,
			},
			HeaderOverrides: []*taskspb.HttpTarget_HeaderOverride{
				{Header: &taskspb.HttpTarget_Header{Key: "X-Override", Value: "yes"}},
			},
			AuthorizationHeader: &taskspb.HttpTarget_OidcToken{OidcToken: &taskspb.OidcToken{ServiceAccountEmail: "sa@x.iam"}},
		},
	})

	// The override should round-trip on GetQueue.
	if q.GetHttpTarget().GetUriOverride().GetPathOverride().GetPath() != "/overridden" {
		t.Errorf("http_target round-trip path = %q", q.GetHttpTarget().GetUriOverride().GetPathOverride().GetPath())
	}

	task := httpTaskV2beta3("http://placeholder/original")
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2beta3(t, c, q.GetName())
	if gotPath != "/overridden" {
		t.Errorf("path = %q, want /overridden", gotPath)
	}
	if gotHeader != "yes" {
		t.Errorf("override header = %q", gotHeader)
	}
	if len(gotAuthz) < 7 || gotAuthz[:7] != "Bearer " {
		t.Errorf("authz = %q", gotAuthz)
	}
}

func TestV2beta3HTTPTargetOAuthRoundTrip(t *testing.T) {
	c := v2beta3Only(t, emulator.Config{})
	q := mkQueueV2beta3(t, c, &taskspb.Queue{
		Name: locationPath() + "/queues/oauthtarget",
		HttpTarget: &taskspb.HttpTarget{
			HttpMethod: taskspb.HttpMethod_PUT,
			UriOverride: &taskspb.UriOverride{
				Host:          strPtr("example.com"),
				QueryOverride: &taskspb.QueryOverride{QueryParams: "a=b"},
			},
			AuthorizationHeader: &taskspb.HttpTarget_OauthToken{OauthToken: &taskspb.OAuthToken{ServiceAccountEmail: "sa@x.iam", Scope: "scope"}},
		},
	})
	ht := q.GetHttpTarget()
	if ht.GetHttpMethod() != taskspb.HttpMethod_PUT {
		t.Errorf("method = %v", ht.GetHttpMethod())
	}
	if ht.GetUriOverride().GetHost() != "example.com" || ht.GetUriOverride().GetQueryOverride().GetQueryParams() != "a=b" {
		t.Errorf("uri override = %v", ht.GetUriOverride())
	}
	if ht.GetOauthToken().GetServiceAccountEmail() != "sa@x.iam" {
		t.Errorf("oauth = %v", ht.GetOauthToken())
	}
}

func TestV2beta3AppEngineRoutingOverride(t *testing.T) {
	ctx := context.Background()
	var gotHost string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	u, _ := url.Parse(ts.URL)

	c := v2beta3Only(t, emulator.Config{})
	q := mkQueueV2beta3(t, c, &taskspb.Queue{
		Name: locationPath() + "/queues/aeq",
		QueueType: &taskspb.Queue_AppEngineHttpQueue{AppEngineHttpQueue: &taskspb.AppEngineHttpQueue{
			AppEngineRoutingOverride: &taskspb.AppEngineRouting{Host: ts.URL},
		}},
	})
	if q.GetAppEngineHttpQueue().GetAppEngineRoutingOverride().GetHost() != ts.URL {
		t.Errorf("routing override round-trip = %v", q.GetAppEngineHttpQueue())
	}
	task := &taskspb.Task{PayloadType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{RelativeUri: "/x"}}}
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: task}); err != nil {
		t.Fatal(err)
	}
	waitTasksEmptyV2beta3(t, c, q.GetName())
	if gotHost != u.Host {
		t.Errorf("dispatched host = %q, want %q", gotHost, u.Host)
	}
}

func TestV2beta3DispatchAndViewsAndUpdate(t *testing.T) {
	ctx := context.Background()
	c := v2beta3Only(t, emulator.Config{})

	// Dispatch a basic HTTP task.
	done := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case done <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	q := mkQueueV2beta3(t, c, &taskspb.Queue{Name: locationPath() + "/queues/v2beta3disp"})
	if _, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: httpTaskV2beta3(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("v2beta3 dispatch failed")
	}
	waitTasksEmptyV2beta3(t, c, q.GetName())

	// App Engine BASIC view strips the body.
	ae := &taskspb.Task{
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		PayloadType:  &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{RelativeUri: "/x", Body: []byte("b")}},
	}
	created, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{Parent: q.GetName(), Task: ae})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetAppEngineHttpRequest().GetBody() != nil {
		t.Error("BASIC should strip app engine body")
	}
	full, _ := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: created.GetName(), ResponseView: taskspb.Task_FULL})
	if string(full.GetAppEngineHttpRequest().GetBody()) != "b" {
		t.Error("FULL should keep app engine body")
	}

	// UpdateQueue rate_limits + http_target.
	q.RateLimits = &taskspb.RateLimits{MaxDispatchesPerSecond: 5}
	q.HttpTarget = &taskspb.HttpTarget{UriOverride: &taskspb.UriOverride{Host: strPtr("h2")}}
	upd, err := c.UpdateQueue(ctx, &taskspb.UpdateQueueRequest{
		Queue:      q,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits", "http_target"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if upd.GetRateLimits().GetMaxDispatchesPerSecond() != 5 || upd.GetHttpTarget().GetUriOverride().GetHost() != "h2" {
		t.Errorf("update = %+v", upd)
	}
}

func strPtr(s string) *string { return &s }

func waitTasksEmptyV2beta3(t *testing.T, c *ctv2beta3.Client, queue string) {
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
	t.Fatal("v2beta3 queue did not drain")
}
