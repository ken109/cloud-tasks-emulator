package emulator

import (
	"context"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ken109/cloud-tasks-emulator/core"
)

// v2Server adapts the google.cloud.tasks.v2 gRPC API onto the shared engine.
type v2Server struct {
	taskspb.UnimplementedCloudTasksServer
	engine *core.Engine
}

// ---- Queue RPCs ----

func (s *v2Server) ListQueues(_ context.Context, req *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	qs, next, err := s.engine.ListQueues(req.GetParent(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &taskspb.ListQueuesResponse{NextPageToken: next}
	for _, q := range qs {
		resp.Queues = append(resp.Queues, v2QueueFromCore(q))
	}
	return resp, nil
}

func (s *v2Server) GetQueue(_ context.Context, req *taskspb.GetQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.GetQueue(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2QueueFromCore(q), nil
}

func (s *v2Server) CreateQueue(_ context.Context, req *taskspb.CreateQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.CreateQueue(req.GetParent(), v2QueueToCore(req.GetQueue()))
	if err != nil {
		return nil, err
	}
	return v2QueueFromCore(q), nil
}

func (s *v2Server) UpdateQueue(_ context.Context, req *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	fields := v2QueueFields(req.GetUpdateMask().GetPaths())
	q, err := s.engine.UpdateQueue(v2QueueToCore(req.GetQueue()), fields)
	if err != nil {
		return nil, err
	}
	return v2QueueFromCore(q), nil
}

func (s *v2Server) DeleteQueue(_ context.Context, req *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteQueue(req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *v2Server) PurgeQueue(_ context.Context, req *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.PurgeQueue(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2QueueFromCore(q), nil
}

func (s *v2Server) PauseQueue(_ context.Context, req *taskspb.PauseQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.SetQueueState(req.GetName(), core.StatePaused)
	if err != nil {
		return nil, err
	}
	return v2QueueFromCore(q), nil
}

func (s *v2Server) ResumeQueue(_ context.Context, req *taskspb.ResumeQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.SetQueueState(req.GetName(), core.StateRunning)
	if err != nil {
		return nil, err
	}
	return v2QueueFromCore(q), nil
}

// ---- Task RPCs ----

func (s *v2Server) ListTasks(_ context.Context, req *taskspb.ListTasksRequest) (*taskspb.ListTasksResponse, error) {
	ts, next, err := s.engine.ListTasks(req.GetParent(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &taskspb.ListTasksResponse{NextPageToken: next}
	for _, t := range ts {
		resp.Tasks = append(resp.Tasks, v2TaskFromCore(t, req.GetResponseView()))
	}
	return resp, nil
}

func (s *v2Server) GetTask(_ context.Context, req *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	t, err := s.engine.GetTask(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2TaskFromCore(t, req.GetResponseView()), nil
}

func (s *v2Server) CreateTask(_ context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	t, err := s.engine.CreateTask(req.GetParent(), v2TaskToCore(req.GetTask()))
	if err != nil {
		return nil, err
	}
	return v2TaskFromCore(t, req.GetResponseView()), nil
}

func (s *v2Server) DeleteTask(_ context.Context, req *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteTask(req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *v2Server) RunTask(_ context.Context, req *taskspb.RunTaskRequest) (*taskspb.Task, error) {
	t, err := s.engine.RunTask(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2TaskFromCore(t, req.GetResponseView()), nil
}

// ---- IAM RPCs ----

func (s *v2Server) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.engine.GetIamPolicy(req.GetResource())
}

func (s *v2Server) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.engine.SetIamPolicy(req.GetResource(), req.GetPolicy())
}

func (s *v2Server) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	perms, err := s.engine.TestIamPermissions(req.GetResource(), req.GetPermissions())
	if err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// ---- conversions ----

func v2QueueToCore(q *taskspb.Queue) *core.Queue {
	if q == nil {
		return nil
	}
	c := &core.Queue{
		Name:                  q.GetName(),
		State:                 v2StateToCore(q.GetState()),
		AppEngineHostOverride: q.GetAppEngineRoutingOverride().GetHost(),
	}
	if rl := q.GetRateLimits(); rl != nil {
		c.RateLimits = core.RateLimits{
			MaxDispatchesPerSecond:  rl.GetMaxDispatchesPerSecond(),
			MaxConcurrentDispatches: rl.GetMaxConcurrentDispatches(),
		}
	}
	if rc := q.GetRetryConfig(); rc != nil {
		c.RetryConfig = core.RetryConfig{
			MaxAttempts:      rc.GetMaxAttempts(),
			MaxRetryDuration: durToDuration(rc.GetMaxRetryDuration()),
			MinBackoff:       durToDuration(rc.GetMinBackoff()),
			MaxBackoff:       durToDuration(rc.GetMaxBackoff()),
			MaxDoublings:     rc.GetMaxDoublings(),
		}
	}
	return c
}

func v2QueueFromCore(q *core.Queue) *taskspb.Queue {
	out := &taskspb.Queue{
		Name:  q.Name,
		State: v2StateFromCore(q.State),
		RateLimits: &taskspb.RateLimits{
			MaxDispatchesPerSecond:  q.RateLimits.MaxDispatchesPerSecond,
			MaxBurstSize:            q.RateLimits.MaxBurstSize,
			MaxConcurrentDispatches: q.RateLimits.MaxConcurrentDispatches,
		},
		RetryConfig: &taskspb.RetryConfig{
			MaxAttempts:      q.RetryConfig.MaxAttempts,
			MaxRetryDuration: durationToDur(q.RetryConfig.MaxRetryDuration),
			MinBackoff:       durationToDur(q.RetryConfig.MinBackoff),
			MaxBackoff:       durationToDur(q.RetryConfig.MaxBackoff),
			MaxDoublings:     q.RetryConfig.MaxDoublings,
		},
		PurgeTime: timeToTs(q.PurgeTime),
	}
	if q.AppEngineHostOverride != "" {
		out.AppEngineRoutingOverride = &taskspb.AppEngineRouting{Host: q.AppEngineHostOverride}
	}
	return out
}

func v2QueueFields(paths []string) []core.QueueField {
	var fields []core.QueueField
	for _, p := range paths {
		switch p {
		case "rate_limits":
			fields = append(fields, core.FieldRateLimits)
		case "retry_config":
			fields = append(fields, core.FieldRetryConfig)
		case "app_engine_routing_override":
			fields = append(fields, core.FieldAppEngineRoutingOverride)
		}
	}
	return fields
}

func v2TaskToCore(t *taskspb.Task) *core.Task {
	if t == nil {
		return nil
	}
	ct := &core.Task{
		Name:             t.GetName(),
		ScheduleTime:     tsToTime(t.GetScheduleTime()),
		DispatchDeadline: durToDuration(t.GetDispatchDeadline()),
	}
	switch mt := t.GetMessageType().(type) {
	case *taskspb.Task_HttpRequest:
		r := mt.HttpRequest
		ct.Target = core.Target{
			Type:    core.TargetHTTP,
			Method:  v2MethodName(r.GetHttpMethod()),
			URL:     r.GetUrl(),
			Headers: r.GetHeaders(),
			Body:    r.GetBody(),
			Auth:    v2AuthFromHTTP(r),
		}
	case *taskspb.Task_AppEngineHttpRequest:
		r := mt.AppEngineHttpRequest
		ct.Target = core.Target{
			Type:             core.TargetAppEngine,
			Method:           v2MethodName(r.GetHttpMethod()),
			RelativeURI:      r.GetRelativeUri(),
			Headers:          r.GetHeaders(),
			Body:             r.GetBody(),
			AppEngineRouting: v2RoutingToCore(r.GetAppEngineRouting()),
		}
	}
	return ct
}

func v2TaskFromCore(t *core.Task, view taskspb.Task_View) *taskspb.Task {
	out := &taskspb.Task{
		Name:             t.Name,
		ScheduleTime:     timeToTs(t.ScheduleTime),
		CreateTime:       timeToTs(t.CreateTime),
		DispatchDeadline: durationToDur(t.DispatchDeadline),
		DispatchCount:    t.DispatchCount,
		ResponseCount:    t.ResponseCount,
		FirstAttempt:     v2AttemptFromCore(t.FirstAttempt),
		LastAttempt:      v2AttemptFromCore(t.LastAttempt),
		View:             viewOrBasic(view),
	}
	full := view == taskspb.Task_FULL
	switch t.Target.Type {
	case core.TargetHTTP:
		r := &taskspb.HttpRequest{
			Url:        t.Target.URL,
			HttpMethod: v2Method(t.Target.Method),
			Headers:    t.Target.Headers,
			Body:       t.Target.Body,
		}
		v2ApplyAuth(r, t.Target.Auth)
		out.MessageType = &taskspb.Task_HttpRequest{HttpRequest: r}
	case core.TargetAppEngine:
		body := t.Target.Body
		if !full {
			body = nil // BASIC view omits the App Engine body
		}
		out.MessageType = &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
			HttpMethod:       v2Method(t.Target.Method),
			RelativeUri:      t.Target.RelativeURI,
			Headers:          t.Target.Headers,
			Body:             body,
			AppEngineRouting: v2RoutingFromCore(t.Target.AppEngineRouting),
		}}
	}
	return out
}

func viewOrBasic(v taskspb.Task_View) taskspb.Task_View {
	if v == taskspb.Task_FULL {
		return taskspb.Task_FULL
	}
	return taskspb.Task_BASIC
}

func v2AttemptFromCore(a *core.Attempt) *taskspb.Attempt {
	if a == nil {
		return nil
	}
	out := &taskspb.Attempt{
		ScheduleTime: timeToTs(a.ScheduleTime),
		DispatchTime: timeToTs(a.DispatchTime),
		ResponseTime: timeToTs(a.ResponseTime),
	}
	if !a.ResponseTime.IsZero() {
		out.ResponseStatus = rpcStatus(a.Code, a.Message)
	}
	return out
}

func v2StateToCore(s taskspb.Queue_State) core.State {
	switch s {
	case taskspb.Queue_PAUSED:
		return core.StatePaused
	case taskspb.Queue_DISABLED:
		return core.StateDisabled
	default:
		return core.StateRunning
	}
}

func v2StateFromCore(s core.State) taskspb.Queue_State {
	switch s {
	case core.StatePaused:
		return taskspb.Queue_PAUSED
	case core.StateDisabled:
		return taskspb.Queue_DISABLED
	default:
		return taskspb.Queue_RUNNING
	}
}

func v2RoutingToCore(r *taskspb.AppEngineRouting) *core.AppEngineRouting {
	if r == nil {
		return nil
	}
	return &core.AppEngineRouting{Service: r.GetService(), Version: r.GetVersion(), Instance: r.GetInstance(), Host: r.GetHost()}
}

func v2RoutingFromCore(r *core.AppEngineRouting) *taskspb.AppEngineRouting {
	if r == nil {
		return nil
	}
	return &taskspb.AppEngineRouting{Service: r.Service, Version: r.Version, Instance: r.Instance, Host: r.Host}
}

func v2AuthFromHTTP(r *taskspb.HttpRequest) *core.Auth {
	switch a := r.GetAuthorizationHeader().(type) {
	case *taskspb.HttpRequest_OidcToken:
		return &core.Auth{Kind: core.AuthOIDC, ServiceAccountEmail: a.OidcToken.GetServiceAccountEmail(), Audience: a.OidcToken.GetAudience()}
	case *taskspb.HttpRequest_OauthToken:
		return &core.Auth{Kind: core.AuthOAuth, ServiceAccountEmail: a.OauthToken.GetServiceAccountEmail(), Scope: a.OauthToken.GetScope()}
	default:
		return nil
	}
}

func v2ApplyAuth(r *taskspb.HttpRequest, a *core.Auth) {
	if a == nil {
		return
	}
	switch a.Kind {
	case core.AuthOIDC:
		r.AuthorizationHeader = &taskspb.HttpRequest_OidcToken{OidcToken: &taskspb.OidcToken{ServiceAccountEmail: a.ServiceAccountEmail, Audience: a.Audience}}
	case core.AuthOAuth:
		r.AuthorizationHeader = &taskspb.HttpRequest_OauthToken{OauthToken: &taskspb.OAuthToken{ServiceAccountEmail: a.ServiceAccountEmail, Scope: a.Scope}}
	}
}

func v2MethodName(m taskspb.HttpMethod) string {
	return httpMethodName(int32(m))
}

func v2Method(name string) taskspb.HttpMethod {
	return taskspb.HttpMethod(httpMethodEnum(name))
}
