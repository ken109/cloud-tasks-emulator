package emulator

import (
	"context"
	"net/http"
	"testing"
	"time"

	taskspbv2 "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	taskspbv2beta3 "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ken109/cloud-tasks-emulator/core"
)

const parent = "projects/p/locations/l"

func TestConvHelpers(t *testing.T) {
	methods := map[int32]string{0: http.MethodPost, 1: http.MethodPost, 2: http.MethodGet, 3: http.MethodHead, 4: http.MethodPut, 5: http.MethodDelete, 6: http.MethodPatch, 7: http.MethodOptions}
	for m, name := range methods {
		if httpMethodName(m) != name {
			t.Errorf("methodName(%d)=%q want %q", m, httpMethodName(m), name)
		}
	}
	enums := map[string]int32{http.MethodGet: 2, http.MethodHead: 3, http.MethodPut: 4, http.MethodDelete: 5, http.MethodPatch: 6, http.MethodOptions: 7, http.MethodPost: 1, "WAT": 1}
	for name, want := range enums {
		if httpMethodEnum(name) != want {
			t.Errorf("methodEnum(%q)=%d want %d", name, httpMethodEnum(name), want)
		}
	}
	if rpcStatus(0, "x").GetCode() != 0 || rpcStatus(5, "m").GetCode() != 5 {
		t.Error("rpcStatus")
	}
	if tsToTime(nil) != (time.Time{}) || timeToTs(time.Time{}) != nil {
		t.Error("ts zero")
	}
	if durToDuration(nil) != 0 || durationToDur(0) != nil {
		t.Error("dur zero")
	}
	if !tsToTime(timestamppb.New(time.Unix(1, 0))).Equal(time.Unix(1, 0)) {
		t.Error("ts roundtrip")
	}
	if durToDuration(durationpb.New(time.Second)) != time.Second {
		t.Error("dur roundtrip")
	}
}

func TestEmulatorAccessor(t *testing.T) {
	if New(Config{}).Engine() == nil {
		t.Error("engine accessor")
	}
}

// ---- v2 adapter ----

func newV2(t *testing.T) (*v2Server, string) {
	t.Helper()
	s := &v2Server{engine: core.NewEngine(Config{DefaultAppEngineHost: "http://svc"})}
	q := parent + "/queues/q"
	if _, err := s.CreateQueue(context.Background(), &taskspbv2.CreateQueueRequest{Parent: parent, Queue: &taskspbv2.Queue{Name: q}}); err != nil {
		t.Fatal(err)
	}
	return s, q
}

func TestV2AdapterMethods(t *testing.T) {
	ctx := context.Background()
	s, q := newV2(t)

	// Queue RPC error branches.
	if _, err := s.ListQueues(ctx, &taskspbv2.ListQueuesRequest{Parent: "bad"}); err == nil {
		t.Error("list err")
	}
	if _, err := s.GetQueue(ctx, &taskspbv2.GetQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("get err")
	}
	if _, err := s.CreateQueue(ctx, &taskspbv2.CreateQueueRequest{Parent: "bad", Queue: &taskspbv2.Queue{}}); err == nil {
		t.Error("create err")
	}
	if _, err := s.UpdateQueue(ctx, &taskspbv2.UpdateQueueRequest{Queue: &taskspbv2.Queue{Name: "bad"}}); err == nil {
		t.Error("update err")
	}
	if _, err := s.DeleteQueue(ctx, &taskspbv2.DeleteQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("delete err")
	}
	if _, err := s.PurgeQueue(ctx, &taskspbv2.PurgeQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("purge err")
	}
	if _, err := s.PauseQueue(ctx, &taskspbv2.PauseQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("pause err")
	}
	if _, err := s.ResumeQueue(ctx, &taskspbv2.ResumeQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("resume err")
	}

	// Success branches.
	if _, err := s.ListQueues(ctx, &taskspbv2.ListQueuesRequest{Parent: parent}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateQueue(ctx, &taskspbv2.UpdateQueueRequest{Queue: &taskspbv2.Queue{Name: q}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PauseQueue(ctx, &taskspbv2.PauseQueueRequest{Name: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResumeQueue(ctx, &taskspbv2.ResumeQueueRequest{Name: q}); err != nil {
		t.Fatal(err)
	}

	// Task RPCs.
	if _, err := s.ListTasks(ctx, &taskspbv2.ListTasksRequest{Parent: parent + "/queues/missing"}); err == nil {
		t.Error("listtasks err")
	}
	if _, err := s.GetTask(ctx, &taskspbv2.GetTaskRequest{Name: "bad"}); err == nil {
		t.Error("gettask err")
	}
	if _, err := s.CreateTask(ctx, &taskspbv2.CreateTaskRequest{Parent: q, Task: &taskspbv2.Task{}}); err == nil {
		t.Error("createtask err")
	}
	if _, err := s.DeleteTask(ctx, &taskspbv2.DeleteTaskRequest{Name: "bad"}); err == nil {
		t.Error("deletetask err")
	}
	if _, err := s.RunTask(ctx, &taskspbv2.RunTaskRequest{Name: "bad"}); err == nil {
		t.Error("runtask err")
	}
	task := &taskspbv2.Task{
		Name:         q + "/tasks/t1",
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		MessageType:  &taskspbv2.Task_HttpRequest{HttpRequest: &taskspbv2.HttpRequest{Url: "http://127.0.0.1:1", HttpMethod: taskspbv2.HttpMethod_GET}},
	}
	if _, err := s.CreateTask(ctx, &taskspbv2.CreateTaskRequest{Parent: q, Task: task}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListTasks(ctx, &taskspbv2.ListTasksRequest{Parent: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetTask(ctx, &taskspbv2.GetTaskRequest{Name: task.Name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunTask(ctx, &taskspbv2.RunTaskRequest{Name: task.Name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteTask(ctx, &taskspbv2.DeleteTaskRequest{Name: q + "/tasks/gone"}); err == nil {
		t.Error("deletetask missing")
	}

	// IAM.
	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: q, Policy: &iampb.Policy{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: q, Permissions: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: "bad"}); err == nil {
		t.Error("testiam err")
	}
}

func TestV2Converters(t *testing.T) {
	// Queue with all fields, DISABLED state, routing override.
	cq := v2QueueToCore(&taskspbv2.Queue{
		Name:                     parent + "/queues/q",
		State:                    taskspbv2.Queue_DISABLED,
		RateLimits:               &taskspbv2.RateLimits{MaxDispatchesPerSecond: 1, MaxConcurrentDispatches: 2},
		RetryConfig:              &taskspbv2.RetryConfig{MaxAttempts: 3, MaxRetryDuration: durationpb.New(time.Minute), MinBackoff: durationpb.New(time.Second), MaxBackoff: durationpb.New(time.Hour), MaxDoublings: 4},
		AppEngineRoutingOverride: &taskspbv2.AppEngineRouting{Host: "h"},
	})
	if cq.State != core.StateDisabled || cq.AppEngineHostOverride != "h" || cq.RetryConfig.MaxAttempts != 3 {
		t.Errorf("v2QueueToCore = %+v", cq)
	}
	if v2QueueToCore(nil) != nil {
		t.Error("nil queue")
	}
	out := v2QueueFromCore(&core.Queue{Name: "n", State: core.StateDisabled})
	if out.GetState() != taskspbv2.Queue_DISABLED {
		t.Error("from disabled")
	}
	if v2StateFromCore(core.StatePaused) != taskspbv2.Queue_PAUSED || v2StateToCore(taskspbv2.Queue_PAUSED) != core.StatePaused {
		t.Error("state paused")
	}

	// Task with App Engine target + routing; FULL view keeps body.
	ct := v2TaskToCore(&taskspbv2.Task{
		MessageType: &taskspbv2.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspbv2.AppEngineHttpRequest{
			RelativeUri: "/x", Body: []byte("b"), HttpMethod: taskspbv2.HttpMethod_PUT,
			AppEngineRouting: &taskspbv2.AppEngineRouting{Service: "s", Version: "v", Instance: "i", Host: "h"},
		}},
	})
	if ct.Target.Type != core.TargetAppEngine || ct.Target.AppEngineRouting.Service != "s" {
		t.Errorf("v2 ae task = %+v", ct.Target)
	}
	full := v2TaskFromCore(ct, taskspbv2.Task_FULL)
	if string(full.GetAppEngineHttpRequest().GetBody()) != "b" {
		t.Error("full ae body")
	}
	if v2TaskFromCore(ct, taskspbv2.Task_BASIC).GetAppEngineHttpRequest().GetBody() != nil {
		t.Error("basic ae body")
	}
	if got := v2TaskToCore(nil); got != nil {
		t.Error("nil task")
	}

	// OAuth auth round trip.
	oauth := v2TaskToCore(&taskspbv2.Task{MessageType: &taskspbv2.Task_HttpRequest{HttpRequest: &taskspbv2.HttpRequest{
		Url: "http://h", AuthorizationHeader: &taskspbv2.HttpRequest_OauthToken{OauthToken: &taskspbv2.OAuthToken{ServiceAccountEmail: "sa", Scope: "s"}},
	}}})
	if oauth.Target.Auth.Kind != core.AuthOAuth {
		t.Error("v2 oauth")
	}
	back := v2TaskFromCore(oauth, taskspbv2.Task_FULL)
	if back.GetHttpRequest().GetOauthToken().GetScope() != "s" {
		t.Error("v2 oauth back")
	}

	// Attempt + routing/auth nils.
	if v2AttemptFromCore(nil) != nil {
		t.Error("nil attempt")
	}
	a := v2AttemptFromCore(&core.Attempt{ScheduleTime: time.Unix(1, 0), DispatchTime: time.Unix(2, 0), ResponseTime: time.Unix(3, 0), Code: 5, Message: "m"})
	if a.GetResponseStatus().GetCode() != 5 {
		t.Error("attempt status")
	}
	if v2RoutingToCore(nil) != nil || v2RoutingFromCore(nil) != nil {
		t.Error("nil routing")
	}
	if v2AuthFromHTTP(&taskspbv2.HttpRequest{}) != nil {
		t.Error("no auth")
	}
	v2ApplyAuth(&taskspbv2.HttpRequest{}, nil) // no-op
	if v2QueueFields([]string{"rate_limits", "retry_config", "app_engine_routing_override", "unknown"}) == nil {
		t.Error("fields")
	}
}

// ---- v2beta3 adapter ----

func newV2beta3(t *testing.T) (*v2beta3Server, string) {
	t.Helper()
	s := &v2beta3Server{engine: core.NewEngine(Config{DefaultAppEngineHost: "http://svc"})}
	q := parent + "/queues/q"
	if _, err := s.CreateQueue(context.Background(), &taskspbv2beta3.CreateQueueRequest{Parent: parent, Queue: &taskspbv2beta3.Queue{Name: q}}); err != nil {
		t.Fatal(err)
	}
	return s, q
}

func TestV2beta3AdapterMethods(t *testing.T) {
	ctx := context.Background()
	s, q := newV2beta3(t)

	if _, err := s.ListQueues(ctx, &taskspbv2beta3.ListQueuesRequest{Parent: "bad"}); err == nil {
		t.Error("list err")
	}
	if _, err := s.ListQueues(ctx, &taskspbv2beta3.ListQueuesRequest{Parent: parent}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetQueue(ctx, &taskspbv2beta3.GetQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("get err")
	}
	if _, err := s.CreateQueue(ctx, &taskspbv2beta3.CreateQueueRequest{Parent: "bad", Queue: &taskspbv2beta3.Queue{}}); err == nil {
		t.Error("create err")
	}
	if _, err := s.UpdateQueue(ctx, &taskspbv2beta3.UpdateQueueRequest{Queue: &taskspbv2beta3.Queue{Name: "bad"}}); err == nil {
		t.Error("update err")
	}
	if _, err := s.UpdateQueue(ctx, &taskspbv2beta3.UpdateQueueRequest{Queue: &taskspbv2beta3.Queue{Name: q}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteQueue(ctx, &taskspbv2beta3.DeleteQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("delete err")
	}
	if _, err := s.PurgeQueue(ctx, &taskspbv2beta3.PurgeQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("purge err")
	}
	if _, err := s.PurgeQueue(ctx, &taskspbv2beta3.PurgeQueueRequest{Name: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PauseQueue(ctx, &taskspbv2beta3.PauseQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("pause err")
	}
	if _, err := s.PauseQueue(ctx, &taskspbv2beta3.PauseQueueRequest{Name: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResumeQueue(ctx, &taskspbv2beta3.ResumeQueueRequest{Name: parent + "/queues/missing"}); err == nil {
		t.Error("resume err")
	}
	if _, err := s.ResumeQueue(ctx, &taskspbv2beta3.ResumeQueueRequest{Name: q}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ListTasks(ctx, &taskspbv2beta3.ListTasksRequest{Parent: parent + "/queues/missing"}); err == nil {
		t.Error("listtasks err")
	}
	if _, err := s.GetTask(ctx, &taskspbv2beta3.GetTaskRequest{Name: "bad"}); err == nil {
		t.Error("gettask err")
	}
	if _, err := s.CreateTask(ctx, &taskspbv2beta3.CreateTaskRequest{Parent: q, Task: &taskspbv2beta3.Task{}}); err == nil {
		t.Error("createtask err")
	}
	if _, err := s.DeleteTask(ctx, &taskspbv2beta3.DeleteTaskRequest{Name: "bad"}); err == nil {
		t.Error("deletetask err")
	}
	if _, err := s.RunTask(ctx, &taskspbv2beta3.RunTaskRequest{Name: "bad"}); err == nil {
		t.Error("runtask err")
	}
	task := &taskspbv2beta3.Task{
		Name:         q + "/tasks/t1",
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		PayloadType:  &taskspbv2beta3.Task_HttpRequest{HttpRequest: &taskspbv2beta3.HttpRequest{Url: "http://127.0.0.1:1", HttpMethod: taskspbv2beta3.HttpMethod_GET}},
	}
	if _, err := s.CreateTask(ctx, &taskspbv2beta3.CreateTaskRequest{Parent: q, Task: task}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListTasks(ctx, &taskspbv2beta3.ListTasksRequest{Parent: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetTask(ctx, &taskspbv2beta3.GetTaskRequest{Name: task.Name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunTask(ctx, &taskspbv2beta3.RunTaskRequest{Name: task.Name}); err != nil {
		t.Fatal(err)
	}
	// Successful DeleteTask + DeleteQueue (cover the success returns).
	dt := &taskspbv2beta3.Task{Name: q + "/tasks/del", ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)), PayloadType: &taskspbv2beta3.Task_HttpRequest{HttpRequest: &taskspbv2beta3.HttpRequest{Url: "http://127.0.0.1:1"}}}
	if _, err := s.CreateTask(ctx, &taskspbv2beta3.CreateTaskRequest{Parent: q, Task: dt}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteTask(ctx, &taskspbv2beta3.DeleteTaskRequest{Name: dt.Name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteQueue(ctx, &taskspbv2beta3.DeleteQueueRequest{Name: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateQueue(ctx, &taskspbv2beta3.CreateQueueRequest{Parent: parent, Queue: &taskspbv2beta3.Queue{Name: q}}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: q, Policy: &iampb.Policy{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: q, Permissions: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: "bad"}); err == nil {
		t.Error("testiam err")
	}
}

func TestV2beta3Converters(t *testing.T) {
	if v2beta3QueueToCore(nil) != nil {
		t.Error("nil queue")
	}
	// PULL queue with TTLs and app engine routing override.
	cq := v2beta3QueueToCore(&taskspbv2beta3.Queue{
		Name:         parent + "/queues/q",
		State:        taskspbv2beta3.Queue_DISABLED,
		Type:         taskspbv2beta3.Queue_PULL,
		TaskTtl:      durationpb.New(time.Hour),
		TombstoneTtl: durationpb.New(time.Minute),
		RateLimits:   &taskspbv2beta3.RateLimits{MaxDispatchesPerSecond: 1, MaxConcurrentDispatches: 2},
		RetryConfig:  &taskspbv2beta3.RetryConfig{MaxAttempts: 3, MaxRetryDuration: durationpb.New(time.Minute), MinBackoff: durationpb.New(time.Second), MaxBackoff: durationpb.New(time.Hour), MaxDoublings: 4},
		QueueType:    &taskspbv2beta3.Queue_AppEngineHttpQueue{AppEngineHttpQueue: &taskspbv2beta3.AppEngineHttpQueue{AppEngineRoutingOverride: &taskspbv2beta3.AppEngineRouting{Host: "h"}}},
	})
	if !cq.Pull || cq.State != core.StateDisabled || cq.TaskTTL != time.Hour || cq.AppEngineHostOverride != "h" {
		t.Errorf("v2beta3QueueToCore = %+v", cq)
	}
	out := v2beta3QueueFromCore(&core.Queue{Name: "n", State: core.StatePaused, Pull: true, AppEngineHostOverride: "h", HTTPOverride: &core.HTTPOverride{Host: "x"}, TasksCount: 2})
	if out.GetType() != taskspbv2beta3.Queue_PULL || out.GetStats().GetTasksCount() != 2 || out.GetAppEngineHttpQueue() == nil || out.GetHttpTarget() == nil {
		t.Errorf("v2beta3QueueFromCore = %+v", out)
	}
	if v2beta3StateFromCore(core.StateDisabled) != taskspbv2beta3.Queue_DISABLED || v2beta3StateToCore(taskspbv2beta3.Queue_PAUSED) != core.StatePaused {
		t.Error("v2beta3 state")
	}

	// HTTP override: all branches (https scheme, port, path, query, headers, oidc).
	if v2beta3HTTPOverrideToCore(nil) != nil {
		t.Error("nil override")
	}
	scheme := taskspbv2beta3.UriOverride_HTTPS
	port := int64(8443)
	host := "h"
	o := v2beta3HTTPOverrideToCore(&taskspbv2beta3.HttpTarget{
		HttpMethod: taskspbv2beta3.HttpMethod_PUT,
		UriOverride: &taskspbv2beta3.UriOverride{
			Scheme: &scheme, Host: &host, Port: &port,
			PathOverride: &taskspbv2beta3.PathOverride{Path: "/p"}, QueryOverride: &taskspbv2beta3.QueryOverride{QueryParams: "a=b"},
			UriOverrideEnforceMode: taskspbv2beta3.UriOverride_ALWAYS,
		},
		HeaderOverrides:     []*taskspbv2beta3.HttpTarget_HeaderOverride{{Header: &taskspbv2beta3.HttpTarget_Header{Key: "k", Value: "v"}}},
		AuthorizationHeader: &taskspbv2beta3.HttpTarget_OidcToken{OidcToken: &taskspbv2beta3.OidcToken{ServiceAccountEmail: "sa"}},
	})
	if o.Scheme != "https" || o.Port != "8443" || o.Path != "/p" || o.Query != "a=b" || !o.AlwaysEnforce || o.Headers["k"] != "v" || o.Auth.Kind != core.AuthOIDC {
		t.Errorf("v2beta3HTTPOverrideToCore = %+v", o)
	}
	ht := v2beta3HTTPOverrideFromCore(o)
	if ht.GetUriOverride().GetHost() != "h" || ht.GetUriOverride().GetScheme() != taskspbv2beta3.UriOverride_HTTPS || ht.GetOidcToken() == nil {
		t.Errorf("v2beta3HTTPOverrideFromCore = %+v", ht)
	}
	// HTTP scheme branch + oauth + bad port (ignored).
	o2 := v2beta3HTTPOverrideToCore(&taskspbv2beta3.HttpTarget{
		UriOverride:         &taskspbv2beta3.UriOverride{Scheme: schemePtr(taskspbv2beta3.UriOverride_HTTP)},
		AuthorizationHeader: &taskspbv2beta3.HttpTarget_OauthToken{OauthToken: &taskspbv2beta3.OAuthToken{ServiceAccountEmail: "sa", Scope: "s"}},
	})
	if o2.Scheme != "http" || o2.Auth.Kind != core.AuthOAuth {
		t.Errorf("v2beta3 http/oauth = %+v", o2)
	}
	ht2 := v2beta3HTTPOverrideFromCore(&core.HTTPOverride{Scheme: "http", Port: "notanumber", Auth: &core.Auth{Kind: core.AuthOAuth, ServiceAccountEmail: "sa", Scope: "s"}})
	if ht2.GetOauthToken() == nil || ht2.GetUriOverride().GetScheme() != taskspbv2beta3.UriOverride_HTTP {
		t.Errorf("v2beta3 from http/oauth = %+v", ht2)
	}

	// Tasks: pull, oauth, app engine views.
	if v2beta3TaskToCore(nil) != nilTask() {
		t.Error("nil task")
	}
	pull := v2beta3TaskToCore(&taskspbv2beta3.Task{PayloadType: &taskspbv2beta3.Task_PullMessage{PullMessage: &taskspbv2beta3.PullMessage{Payload: []byte("p"), Tag: "tag"}}})
	if pull.Target.Type != core.TargetPull || pull.Target.Tag != "tag" {
		t.Error("v2beta3 pull")
	}
	if v2beta3TaskFromCore(pull, taskspbv2beta3.Task_FULL).GetPullMessage().GetTag() != "tag" {
		t.Error("v2beta3 pull back")
	}
	ae := v2beta3TaskToCore(&taskspbv2beta3.Task{PayloadType: &taskspbv2beta3.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspbv2beta3.AppEngineHttpRequest{RelativeUri: "/x", Body: []byte("b"), AppEngineRouting: &taskspbv2beta3.AppEngineRouting{Host: "h"}}}})
	if v2beta3TaskFromCore(ae, taskspbv2beta3.Task_BASIC).GetAppEngineHttpRequest().GetBody() != nil {
		t.Error("v2beta3 basic ae body")
	}
	if string(v2beta3TaskFromCore(ae, taskspbv2beta3.Task_FULL).GetAppEngineHttpRequest().GetBody()) != "b" {
		t.Error("v2beta3 full ae body")
	}
	oauthTask := v2beta3TaskToCore(&taskspbv2beta3.Task{PayloadType: &taskspbv2beta3.Task_HttpRequest{HttpRequest: &taskspbv2beta3.HttpRequest{Url: "http://h", AuthorizationHeader: &taskspbv2beta3.HttpRequest_OauthToken{OauthToken: &taskspbv2beta3.OAuthToken{Scope: "s"}}}}})
	if oauthTask.Target.Auth.Kind != core.AuthOAuth {
		t.Error("v2beta3 task oauth")
	}
	if v2beta3TaskFromCore(oauthTask, taskspbv2beta3.Task_FULL).GetHttpRequest().GetOauthToken() == nil {
		t.Error("v2beta3 task oauth back")
	}
	oidcTask := v2beta3TaskToCore(&taskspbv2beta3.Task{PayloadType: &taskspbv2beta3.Task_HttpRequest{HttpRequest: &taskspbv2beta3.HttpRequest{Url: "http://h", AuthorizationHeader: &taskspbv2beta3.HttpRequest_OidcToken{OidcToken: &taskspbv2beta3.OidcToken{ServiceAccountEmail: "sa"}}}}})
	if oidcTask.Target.Auth.Kind != core.AuthOIDC {
		t.Error("v2beta3 task oidc")
	}
	if v2beta3TaskFromCore(oidcTask, taskspbv2beta3.Task_FULL).GetHttpRequest().GetOidcToken() == nil {
		t.Error("v2beta3 task oidc back")
	}

	if v2beta3AttemptFromCore(nil) != nil {
		t.Error("nil attempt")
	}
	if v2beta3AttemptFromCore(&core.Attempt{ResponseTime: time.Unix(1, 0), Code: 5}).GetResponseStatus().GetCode() != 5 {
		t.Error("v2beta3 attempt")
	}
	if v2beta3RoutingToCore(nil) != nil || v2beta3RoutingFromCore(nil) != nil {
		t.Error("v2beta3 nil routing")
	}
	if v2beta3AuthFromHTTP(&taskspbv2beta3.HttpRequest{}) != nil || v2beta3AuthFromHTTPTarget(&taskspbv2beta3.HttpTarget{}) != nil {
		t.Error("v2beta3 no auth")
	}
	v2beta3ApplyAuth(&taskspbv2beta3.HttpRequest{}, nil) // no-op
	if v2beta3QueueFields([]string{"rate_limits", "retry_config", "http_target", "app_engine_http_queue.app_engine_routing_override", "unknown"}) == nil {
		t.Error("v2beta3 fields")
	}
}

func schemePtr(s taskspbv2beta3.UriOverride_Scheme) *taskspbv2beta3.UriOverride_Scheme { return &s }
func nilTask() *core.Task                                                              { return nil }
