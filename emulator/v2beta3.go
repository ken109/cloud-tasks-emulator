package emulator

import (
	"context"
	"strconv"
	"strings"

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ken109/cloud-tasks-emulator/core"
)

// v2beta3Server adapts the google.cloud.tasks.v2beta3 gRPC API onto the engine.
type v2beta3Server struct {
	taskspb.UnimplementedCloudTasksServer
	engine *core.Engine
}

// ---- Queue RPCs ----

func (s *v2beta3Server) ListQueues(_ context.Context, req *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	qs, next, err := s.engine.ListQueues(req.GetParent(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &taskspb.ListQueuesResponse{NextPageToken: next}
	for _, q := range qs {
		resp.Queues = append(resp.Queues, v2beta3QueueFromCore(q))
	}
	return resp, nil
}

func (s *v2beta3Server) GetQueue(_ context.Context, req *taskspb.GetQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.GetQueue(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2beta3QueueFromCore(q), nil
}

func (s *v2beta3Server) CreateQueue(_ context.Context, req *taskspb.CreateQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.CreateQueue(req.GetParent(), v2beta3QueueToCore(req.GetQueue()))
	if err != nil {
		return nil, err
	}
	return v2beta3QueueFromCore(q), nil
}

func (s *v2beta3Server) UpdateQueue(_ context.Context, req *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.UpdateQueue(v2beta3QueueToCore(req.GetQueue()), v2beta3QueueFields(req.GetUpdateMask().GetPaths()))
	if err != nil {
		return nil, err
	}
	return v2beta3QueueFromCore(q), nil
}

func (s *v2beta3Server) DeleteQueue(_ context.Context, req *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteQueue(req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *v2beta3Server) PurgeQueue(_ context.Context, req *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.PurgeQueue(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2beta3QueueFromCore(q), nil
}

func (s *v2beta3Server) PauseQueue(_ context.Context, req *taskspb.PauseQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.SetQueueState(req.GetName(), core.StatePaused)
	if err != nil {
		return nil, err
	}
	return v2beta3QueueFromCore(q), nil
}

func (s *v2beta3Server) ResumeQueue(_ context.Context, req *taskspb.ResumeQueueRequest) (*taskspb.Queue, error) {
	q, err := s.engine.SetQueueState(req.GetName(), core.StateRunning)
	if err != nil {
		return nil, err
	}
	return v2beta3QueueFromCore(q), nil
}

// ---- Task RPCs ----

func (s *v2beta3Server) ListTasks(_ context.Context, req *taskspb.ListTasksRequest) (*taskspb.ListTasksResponse, error) {
	ts, next, err := s.engine.ListTasks(req.GetParent(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &taskspb.ListTasksResponse{NextPageToken: next}
	for _, t := range ts {
		resp.Tasks = append(resp.Tasks, v2beta3TaskFromCore(t, req.GetResponseView()))
	}
	return resp, nil
}

func (s *v2beta3Server) GetTask(_ context.Context, req *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	t, err := s.engine.GetTask(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2beta3TaskFromCore(t, req.GetResponseView()), nil
}

func (s *v2beta3Server) CreateTask(_ context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	t, err := s.engine.CreateTask(req.GetParent(), v2beta3TaskToCore(req.GetTask()))
	if err != nil {
		return nil, err
	}
	return v2beta3TaskFromCore(t, req.GetResponseView()), nil
}

func (s *v2beta3Server) DeleteTask(_ context.Context, req *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteTask(req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *v2beta3Server) RunTask(_ context.Context, req *taskspb.RunTaskRequest) (*taskspb.Task, error) {
	t, err := s.engine.RunTask(req.GetName())
	if err != nil {
		return nil, err
	}
	return v2beta3TaskFromCore(t, req.GetResponseView()), nil
}

// ---- IAM RPCs ----

func (s *v2beta3Server) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.engine.GetIamPolicy(req.GetResource())
}

func (s *v2beta3Server) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.engine.SetIamPolicy(req.GetResource(), req.GetPolicy())
}

func (s *v2beta3Server) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	perms, err := s.engine.TestIamPermissions(req.GetResource(), req.GetPermissions())
	if err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// ---- conversions ----

func v2beta3QueueToCore(q *taskspb.Queue) *core.Queue {
	if q == nil {
		return nil
	}
	c := &core.Queue{
		Name:         q.GetName(),
		State:        v2beta3StateToCore(q.GetState()),
		TaskTTL:      durToDuration(q.GetTaskTtl()),
		TombstoneTTL: durToDuration(q.GetTombstoneTtl()),
		Pull:         q.GetType() == taskspb.Queue_PULL,
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
	if aeq := q.GetAppEngineHttpQueue(); aeq != nil {
		c.AppEngineRoutingOverride = v2beta3RoutingToCore(aeq.GetAppEngineRoutingOverride())
	}
	c.HTTPOverride = v2beta3HTTPOverrideToCore(q.GetHttpTarget())
	return c
}

func v2beta3QueueFromCore(q *core.Queue) *taskspb.Queue {
	out := &taskspb.Queue{
		Name:  q.Name,
		State: v2beta3StateFromCore(q.State),
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
		TaskTtl:      durationToDur(q.TaskTTL),
		TombstoneTtl: durationToDur(q.TombstoneTTL),
		PurgeTime:    timeToTs(q.PurgeTime),
		Stats:        &taskspb.QueueStats{TasksCount: q.TasksCount},
	}
	if q.Pull {
		out.Type = taskspb.Queue_PULL
	} else {
		out.Type = taskspb.Queue_PUSH
	}
	if q.AppEngineRoutingOverride != nil {
		out.QueueType = &taskspb.Queue_AppEngineHttpQueue{AppEngineHttpQueue: &taskspb.AppEngineHttpQueue{
			AppEngineRoutingOverride: v2beta3RoutingFromCore(q.AppEngineRoutingOverride),
		}}
	}
	if q.HTTPOverride != nil {
		out.HttpTarget = v2beta3HTTPOverrideFromCore(q.HTTPOverride)
	}
	return out
}

func v2beta3QueueFields(paths []string) []core.QueueField {
	var fields []core.QueueField
	for _, p := range paths {
		switch {
		case p == "rate_limits":
			fields = append(fields, core.FieldRateLimits)
		case p == "retry_config":
			fields = append(fields, core.FieldRetryConfig)
		case p == "http_target":
			fields = append(fields, core.FieldHTTPOverride)
		case strings.Contains(p, "app_engine"):
			fields = append(fields, core.FieldAppEngineRoutingOverride)
		}
	}
	return fields
}

func v2beta3HTTPOverrideToCore(h *taskspb.HttpTarget) *core.HTTPOverride {
	if h == nil {
		return nil
	}
	o := &core.HTTPOverride{}
	// Preserve "unspecified" (don't fabricate POST) so GetQueue round-trips.
	if m := h.GetHttpMethod(); m != taskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		o.Method = httpMethodName(int32(m))
	}
	if uo := h.GetUriOverride(); uo != nil {
		switch uo.GetScheme() {
		case taskspb.UriOverride_HTTP:
			o.Scheme = "http"
		case taskspb.UriOverride_HTTPS:
			o.Scheme = "https"
		}
		o.Host = uo.GetHost()
		if uo.Port != nil {
			o.Port = strconv.FormatInt(uo.GetPort(), 10)
		}
		o.Path = uo.GetPathOverride().GetPath()
		o.Query = uo.GetQueryOverride().GetQueryParams()
		o.AlwaysEnforce = uo.GetUriOverrideEnforceMode() == taskspb.UriOverride_ALWAYS
	}
	for _, ho := range h.GetHeaderOverrides() {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers[ho.GetHeader().GetKey()] = ho.GetHeader().GetValue()
	}
	o.Auth = v2beta3AuthFromHTTPTarget(h)
	return o
}

func v2beta3HTTPOverrideFromCore(o *core.HTTPOverride) *taskspb.HttpTarget {
	h := &taskspb.HttpTarget{}
	if o.Method != "" {
		h.HttpMethod = taskspb.HttpMethod(httpMethodEnum(o.Method))
	}
	uo := &taskspb.UriOverride{}
	hasURI := false
	switch o.Scheme {
	case "http":
		sc := taskspb.UriOverride_HTTP
		uo.Scheme = &sc
		hasURI = true
	case "https":
		sc := taskspb.UriOverride_HTTPS
		uo.Scheme = &sc
		hasURI = true
	}
	if o.Host != "" {
		host := o.Host
		uo.Host = &host
		hasURI = true
	}
	if o.Port != "" {
		if p, err := strconv.ParseInt(o.Port, 10, 64); err == nil {
			uo.Port = &p
			hasURI = true
		}
	}
	if o.Path != "" {
		uo.PathOverride = &taskspb.PathOverride{Path: o.Path}
		hasURI = true
	}
	if o.Query != "" {
		uo.QueryOverride = &taskspb.QueryOverride{QueryParams: o.Query}
		hasURI = true
	}
	if o.AlwaysEnforce {
		uo.UriOverrideEnforceMode = taskspb.UriOverride_ALWAYS
	}
	if hasURI || o.AlwaysEnforce {
		h.UriOverride = uo
	}
	for k, v := range o.Headers {
		h.HeaderOverrides = append(h.HeaderOverrides, &taskspb.HttpTarget_HeaderOverride{
			Header: &taskspb.HttpTarget_Header{Key: k, Value: v},
		})
	}
	if o.Auth != nil {
		switch o.Auth.Kind {
		case core.AuthOIDC:
			h.AuthorizationHeader = &taskspb.HttpTarget_OidcToken{OidcToken: &taskspb.OidcToken{ServiceAccountEmail: o.Auth.ServiceAccountEmail, Audience: o.Auth.Audience}}
		case core.AuthOAuth:
			h.AuthorizationHeader = &taskspb.HttpTarget_OauthToken{OauthToken: &taskspb.OAuthToken{ServiceAccountEmail: o.Auth.ServiceAccountEmail, Scope: o.Auth.Scope}}
		}
	}
	return h
}

func v2beta3TaskToCore(t *taskspb.Task) *core.Task {
	if t == nil {
		return nil
	}
	ct := &core.Task{
		Name:             t.GetName(),
		ScheduleTime:     tsToTime(t.GetScheduleTime()),
		DispatchDeadline: durToDuration(t.GetDispatchDeadline()),
	}
	switch mt := t.GetPayloadType().(type) {
	case *taskspb.Task_HttpRequest:
		r := mt.HttpRequest
		ct.Target = core.Target{
			Type:    core.TargetHTTP,
			Method:  httpMethodName(int32(r.GetHttpMethod())),
			URL:     r.GetUrl(),
			Headers: r.GetHeaders(),
			Body:    r.GetBody(),
			Auth:    v2beta3AuthFromHTTP(r),
		}
	case *taskspb.Task_AppEngineHttpRequest:
		r := mt.AppEngineHttpRequest
		ct.Target = core.Target{
			Type:             core.TargetAppEngine,
			Method:           httpMethodName(int32(r.GetHttpMethod())),
			RelativeURI:      r.GetRelativeUri(),
			Headers:          r.GetHeaders(),
			Body:             r.GetBody(),
			AppEngineRouting: v2beta3RoutingToCore(r.GetAppEngineRouting()),
		}
	case *taskspb.Task_PullMessage:
		ct.Target = core.Target{Type: core.TargetPull, Body: mt.PullMessage.GetPayload(), Tag: mt.PullMessage.GetTag()}
	}
	return ct
}

func v2beta3TaskFromCore(t *core.Task, view taskspb.Task_View) *taskspb.Task {
	out := &taskspb.Task{
		Name:             t.Name,
		ScheduleTime:     timeToTs(t.ScheduleTime),
		CreateTime:       timeToTs(t.CreateTime),
		DispatchDeadline: durationToDur(t.DispatchDeadline),
		DispatchCount:    t.DispatchCount,
		ResponseCount:    t.ResponseCount,
		FirstAttempt:     v2beta3AttemptFromCore(t.FirstAttempt),
		LastAttempt:      v2beta3AttemptFromCore(t.LastAttempt),
		View:             v2beta3ViewOrBasic(view),
	}
	full := view == taskspb.Task_FULL
	switch t.Target.Type {
	case core.TargetHTTP:
		r := &taskspb.HttpRequest{
			Url:        t.Target.URL,
			HttpMethod: taskspb.HttpMethod(httpMethodEnum(t.Target.Method)),
			Headers:    t.Target.Headers,
			Body:       t.Target.Body,
		}
		v2beta3ApplyAuth(r, t.Target.Auth)
		out.PayloadType = &taskspb.Task_HttpRequest{HttpRequest: r}
	case core.TargetAppEngine:
		body := t.Target.Body
		if !full {
			body = nil
		}
		out.PayloadType = &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
			HttpMethod:       taskspb.HttpMethod(httpMethodEnum(t.Target.Method)),
			RelativeUri:      t.Target.RelativeURI,
			Headers:          t.Target.Headers,
			Body:             body,
			AppEngineRouting: v2beta3RoutingFromCore(t.Target.AppEngineRouting),
		}}
	case core.TargetPull:
		out.PayloadType = &taskspb.Task_PullMessage{PullMessage: &taskspb.PullMessage{Payload: t.Target.Body, Tag: t.Target.Tag}}
	}
	return out
}

func v2beta3ViewOrBasic(v taskspb.Task_View) taskspb.Task_View {
	if v == taskspb.Task_FULL {
		return taskspb.Task_FULL
	}
	return taskspb.Task_BASIC
}

func v2beta3AttemptFromCore(a *core.Attempt) *taskspb.Attempt {
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

func v2beta3StateToCore(s taskspb.Queue_State) core.State {
	switch s {
	case taskspb.Queue_PAUSED:
		return core.StatePaused
	case taskspb.Queue_DISABLED:
		return core.StateDisabled
	default:
		return core.StateRunning
	}
}

func v2beta3StateFromCore(s core.State) taskspb.Queue_State {
	switch s {
	case core.StatePaused:
		return taskspb.Queue_PAUSED
	case core.StateDisabled:
		return taskspb.Queue_DISABLED
	default:
		return taskspb.Queue_RUNNING
	}
}

func v2beta3RoutingToCore(r *taskspb.AppEngineRouting) *core.AppEngineRouting {
	if r == nil {
		return nil
	}
	return &core.AppEngineRouting{Service: r.GetService(), Version: r.GetVersion(), Instance: r.GetInstance(), Host: r.GetHost()}
}

func v2beta3RoutingFromCore(r *core.AppEngineRouting) *taskspb.AppEngineRouting {
	if r == nil {
		return nil
	}
	return &taskspb.AppEngineRouting{Service: r.Service, Version: r.Version, Instance: r.Instance, Host: r.Host}
}

func v2beta3AuthFromHTTP(r *taskspb.HttpRequest) *core.Auth {
	switch a := r.GetAuthorizationHeader().(type) {
	case *taskspb.HttpRequest_OidcToken:
		return &core.Auth{Kind: core.AuthOIDC, ServiceAccountEmail: a.OidcToken.GetServiceAccountEmail(), Audience: a.OidcToken.GetAudience()}
	case *taskspb.HttpRequest_OauthToken:
		return &core.Auth{Kind: core.AuthOAuth, ServiceAccountEmail: a.OauthToken.GetServiceAccountEmail(), Scope: a.OauthToken.GetScope()}
	default:
		return nil
	}
}

func v2beta3AuthFromHTTPTarget(h *taskspb.HttpTarget) *core.Auth {
	switch a := h.GetAuthorizationHeader().(type) {
	case *taskspb.HttpTarget_OidcToken:
		return &core.Auth{Kind: core.AuthOIDC, ServiceAccountEmail: a.OidcToken.GetServiceAccountEmail(), Audience: a.OidcToken.GetAudience()}
	case *taskspb.HttpTarget_OauthToken:
		return &core.Auth{Kind: core.AuthOAuth, ServiceAccountEmail: a.OauthToken.GetServiceAccountEmail(), Scope: a.OauthToken.GetScope()}
	default:
		return nil
	}
}

func v2beta3ApplyAuth(r *taskspb.HttpRequest, a *core.Auth) {
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
