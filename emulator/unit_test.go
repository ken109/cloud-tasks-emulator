package emulator

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseNamesInvalid(t *testing.T) {
	if _, _, ok := parseLocationName("nope"); ok {
		t.Error("parseLocationName accepted invalid name")
	}
	if _, _, _, ok := parseQueueName("projects/p/locations/l"); ok {
		t.Error("parseQueueName accepted location name")
	}
	if _, _, _, _, ok := parseTaskName("projects/p/locations/l/queues/q"); ok {
		t.Error("parseTaskName accepted queue name")
	}
	// Valid forms.
	if _, _, ok := parseLocationName("projects/p/locations/l"); !ok {
		t.Error("parseLocationName rejected valid name")
	}
	if got := queueParent("projects/p/locations/l/queues/q"); got != "projects/p/locations/l" {
		t.Errorf("queueParent = %q", got)
	}
}

func TestHttpMethodName(t *testing.T) {
	cases := map[taskspb.HttpMethod]string{
		taskspb.HttpMethod_GET:                     http.MethodGet,
		taskspb.HttpMethod_HEAD:                    http.MethodHead,
		taskspb.HttpMethod_PUT:                     http.MethodPut,
		taskspb.HttpMethod_DELETE:                  http.MethodDelete,
		taskspb.HttpMethod_PATCH:                   http.MethodPatch,
		taskspb.HttpMethod_OPTIONS:                 http.MethodOptions,
		taskspb.HttpMethod_POST:                    http.MethodPost,
		taskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED: http.MethodPost,
	}
	for in, want := range cases {
		if got := httpMethodName(in); got != want {
			t.Errorf("httpMethodName(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestHttpToRPCCode(t *testing.T) {
	cases := map[int]code.Code{
		http.StatusBadRequest:          code.Code_INVALID_ARGUMENT,
		http.StatusUnauthorized:        code.Code_UNAUTHENTICATED,
		http.StatusForbidden:           code.Code_PERMISSION_DENIED,
		http.StatusNotFound:            code.Code_NOT_FOUND,
		http.StatusConflict:            code.Code_ALREADY_EXISTS,
		http.StatusTooManyRequests:     code.Code_RESOURCE_EXHAUSTED,
		http.StatusRequestTimeout:      code.Code_DEADLINE_EXCEEDED,
		http.StatusGatewayTimeout:      code.Code_DEADLINE_EXCEEDED,
		http.StatusServiceUnavailable:  code.Code_UNAVAILABLE,
		http.StatusNotImplemented:      code.Code_UNIMPLEMENTED,
		http.StatusInternalServerError: code.Code_INTERNAL,
		http.StatusTeapot:              code.Code_UNKNOWN,
	}
	for in, want := range cases {
		if got := httpToRPCCode(in); got != want {
			t.Errorf("httpToRPCCode(%d) = %v, want %v", in, got, want)
		}
	}
}

func TestComputeBurstSize(t *testing.T) {
	cases := []struct {
		rps  float64
		want int32
	}{
		{-1, 0},
		{0.5, 5},
		{1, 10},
		{50, 20},
		{500, 100},
		{1000, 100},
	}
	for _, c := range cases {
		if got := computeBurstSize(c.rps); got != c.want {
			t.Errorf("computeBurstSize(%v) = %d, want %d", c.rps, got, c.want)
		}
	}
}

func TestAppEngineHostResolution(t *testing.T) {
	s := NewServer(Config{DefaultAppEngineHost: "http://default"})

	// Task-level routing host wins.
	q := &taskspb.Queue{}
	r := &taskspb.AppEngineHttpRequest{AppEngineRouting: &taskspb.AppEngineRouting{Host: "http://task"}}
	if got := s.appEngineHost(q, r); got != "http://task" {
		t.Errorf("task host = %q", got)
	}
	// Queue override next.
	q2 := &taskspb.Queue{AppEngineRoutingOverride: &taskspb.AppEngineRouting{Host: "http://queue"}}
	if got := s.appEngineHost(q2, &taskspb.AppEngineHttpRequest{}); got != "http://queue" {
		t.Errorf("queue host = %q", got)
	}
	// Fall back to default.
	if got := s.appEngineHost(&taskspb.Queue{}, &taskspb.AppEngineHttpRequest{}); got != "http://default" {
		t.Errorf("default host = %q", got)
	}
}

func TestBuildRequestVariants(t *testing.T) {
	s := NewServer(Config{})
	queue := &taskspb.Queue{Name: "projects/p/locations/l/queues/q"}

	// No target -> error.
	if _, err := s.buildRequest(queue, &taskspb.Task{Name: "projects/p/locations/l/queues/q/tasks/t"}, attemptInfo{number: 1}); err == nil {
		t.Error("expected error for task with no target")
	}

	// App Engine target without a host -> error.
	aeTask := &taskspb.Task{
		Name:        "projects/p/locations/l/queues/q/tasks/t",
		MessageType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{RelativeUri: "/x"}},
	}
	if _, err := s.buildRequest(queue, aeTask, attemptInfo{number: 1}); err == nil {
		t.Error("expected error for app engine task without host")
	}

	// App Engine target with host -> URL composed, X-AppEngine-* headers set.
	aeTask = &taskspb.Task{
		Name:         "projects/p/locations/l/queues/q/tasks/ae1",
		ScheduleTime: timestamppb.New(time.Unix(123, 456000)),
		MessageType: &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
			RelativeUri:      "/work",
			AppEngineRouting: &taskspb.AppEngineRouting{Host: "http://svc"},
			HttpMethod:       taskspb.HttpMethod_GET,
		}},
	}
	req, err := s.buildRequest(queue, aeTask, attemptInfo{number: 2, executionCount: 1, prevHTTPCode: 503, prevReason: "RETURNED_503"})
	if err != nil || req.URL.String() != "http://svc/work" {
		t.Fatalf("app engine request: %v / %v", req, err)
	}
	if req.Header.Get("X-AppEngine-TaskRetryCount") != "1" || req.Header.Get("X-AppEngine-TaskExecutionCount") != "1" {
		t.Errorf("app engine retry/execution headers = %q/%q", req.Header.Get("X-AppEngine-TaskRetryCount"), req.Header.Get("X-AppEngine-TaskExecutionCount"))
	}
	if req.Header.Get("User-Agent") != appEngineUserAgent {
		t.Errorf("app engine UA = %q", req.Header.Get("User-Agent"))
	}
	if req.Header.Get("X-AppEngine-QueueName") != "q" || req.Header.Get("X-AppEngine-TaskName") != "ae1" {
		t.Error("missing X-AppEngine system headers")
	}
	if req.Header.Get("X-AppEngine-FailFast") != "false" {
		t.Error("missing X-AppEngine-FailFast")
	}
	if req.Header.Get("X-AppEngine-TaskPreviousResponse") != "503" || req.Header.Get("X-AppEngine-TaskRetryReason") != "RETURNED_503" {
		t.Error("missing previous-response/retry-reason headers")
	}
	if eta := req.Header.Get("X-AppEngine-TaskETA"); eta != "123.000456" {
		t.Errorf("ETA header = %q, want 123.000456", eta)
	}

	// HTTP target with body sets default content type, UA and system headers.
	httpTask := &taskspb.Task{
		Name:         "projects/p/locations/l/queues/q/tasks/42",
		ScheduleTime: timestamppb.New(time.Unix(123, 0)),
		MessageType: &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
			Url:        "http://h/p",
			HttpMethod: taskspb.HttpMethod_POST,
			Body:       []byte("x"),
		}},
	}
	req, err = s.buildRequest(queue, httpTask, attemptInfo{number: 3})
	if err != nil {
		t.Fatalf("http request: %v", err)
	}
	if ct := req.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("default content-type = %q", ct)
	}
	if req.Header.Get("User-Agent") != httpUserAgent {
		t.Errorf("http UA = %q", req.Header.Get("User-Agent"))
	}
	if req.Header.Get("X-CloudTasks-QueueName") != "q" || req.Header.Get("X-CloudTasks-TaskName") != "42" {
		t.Error("missing system headers")
	}
	if req.Header.Get("X-CloudTasks-TaskRetryCount") != "2" {
		t.Errorf("retry count header = %q, want 2", req.Header.Get("X-CloudTasks-TaskRetryCount"))
	}
	if req.Header.Get("X-CloudTasks-TaskExecutionCount") != "0" {
		t.Errorf("execution count header = %q, want 0", req.Header.Get("X-CloudTasks-TaskExecutionCount"))
	}
	// First attempt -> no previous-response headers.
	if req.Header.Get("X-CloudTasks-TaskPreviousResponse") != "" {
		t.Error("first attempt should not set previous-response header")
	}

	// Explicit Content-Type is preserved.
	httpTask.MessageType = &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
		Url:        "http://h/p",
		HttpMethod: taskspb.HttpMethod_POST,
		Body:       []byte("x"),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}}
	req, _ = s.buildRequest(queue, httpTask, attemptInfo{number: 1})
	if ct := req.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("explicit content-type = %q", ct)
	}

	// Malformed URL -> http.NewRequest fails.
	httpTask.MessageType = &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
		Url:        "http://bad\n/x",
		HttpMethod: taskspb.HttpMethod_POST,
	}}
	if _, err := s.buildRequest(queue, httpTask, attemptInfo{number: 1}); err == nil {
		t.Error("expected error for malformed url")
	}
}

func TestBuildRequestAuthTokens(t *testing.T) {
	s := NewServer(Config{})
	queue := &taskspb.Queue{Name: "projects/p/locations/l/queues/q"}
	mkTask := func(hr *taskspb.HttpRequest) *taskspb.Task {
		hr.Url = "http://h/p"
		hr.HttpMethod = taskspb.HttpMethod_POST
		return &taskspb.Task{
			Name:        "projects/p/locations/l/queues/q/tasks/t",
			MessageType: &taskspb.Task_HttpRequest{HttpRequest: hr},
		}
	}

	// OIDC token -> Bearer JWT carrying the email claim.
	oidc := mkTask(&taskspb.HttpRequest{
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{OidcToken: &taskspb.OidcToken{ServiceAccountEmail: "sa@x.iam"}},
	})
	req, _ := s.buildRequest(queue, oidc, attemptInfo{number: 1})
	authz := req.Header.Get("Authorization")
	if len(authz) < 8 || authz[:7] != "Bearer " {
		t.Fatalf("oidc authorization = %q", authz)
	}
	parts := strings.Split(authz[7:], ".")
	if len(parts) != 3 {
		t.Fatalf("oidc token is not a JWT: %q", authz)
	}
	claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
	if !strings.Contains(string(claims), "sa@x.iam") {
		t.Errorf("oidc claims missing email: %s", claims)
	}

	// OAuth token -> Bearer placeholder.
	oauth := mkTask(&taskspb.HttpRequest{
		AuthorizationHeader: &taskspb.HttpRequest_OauthToken{OauthToken: &taskspb.OAuthToken{ServiceAccountEmail: "sa@x.iam", Scope: "scope"}},
	})
	req, _ = s.buildRequest(queue, oauth, attemptInfo{number: 1})
	if a := req.Header.Get("Authorization"); !strings.HasPrefix(a, "Bearer ") {
		t.Errorf("oauth authorization = %q", a)
	}
}

func TestDispatchOutcomes(t *testing.T) {
	s := NewServer(Config{})
	queue := &taskspb.Queue{Name: "projects/p/locations/l/queues/q"}
	mkTask := func(url string) *taskspb.Task {
		return &taskspb.Task{
			Name:         "projects/p/locations/l/queues/q/tasks/t",
			ScheduleTime: timestamppb.Now(),
			MessageType:  &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{Url: url, HttpMethod: taskspb.HttpMethod_POST}},
		}
	}

	// buildRequest failure -> INVALID_ARGUMENT.
	st, _, err := s.dispatch(queue, &taskspb.Task{Name: "projects/p/locations/l/queues/q/tasks/t"}, attemptInfo{number: 1})
	if st.GetCode() != int32(code.Code_INVALID_ARGUMENT) || err == nil {
		t.Errorf("no-target dispatch = %v / %v", st, err)
	}

	// Transport error -> UNAVAILABLE, httpCode 0.
	st, hc, err := s.dispatch(queue, mkTask("http://127.0.0.1:1"), attemptInfo{number: 1})
	if st.GetCode() != int32(code.Code_UNAVAILABLE) || err == nil || hc != 0 {
		t.Errorf("transport error dispatch = %v / %d / %v", st, hc, err)
	}

	// 2xx -> OK with the HTTP code returned.
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer okSrv.Close()
	st, hc, _ = s.dispatch(queue, mkTask(okSrv.URL), attemptInfo{number: 1})
	if st.GetCode() != int32(code.Code_OK) || hc != 200 {
		t.Errorf("ok dispatch = %v / %d", st.GetCode(), hc)
	}

	// Non-2xx -> mapped error code, with custom dispatch deadline exercised.
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer failSrv.Close()
	ft := mkTask(failSrv.URL)
	ft.DispatchDeadline = durationpb.New(30 * time.Second)
	st, hc, _ = s.dispatch(queue, ft, attemptInfo{number: 1})
	if st.GetCode() != int32(code.Code_INTERNAL) || hc != 500 {
		t.Errorf("fail dispatch = %v / %d", st.GetCode(), hc)
	}

	// Redirects are NOT followed: a 302 is a failed dispatch, not success.
	redirSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.invalid/elsewhere", http.StatusFound)
	}))
	defer redirSrv.Close()
	st, hc, _ = s.dispatch(queue, mkTask(redirSrv.URL), attemptInfo{number: 1})
	if st.GetCode() == int32(code.Code_OK) || hc != http.StatusFound {
		t.Errorf("redirect should not be followed: code=%v http=%d", st.GetCode(), hc)
	}
}

func TestRetryReason(t *testing.T) {
	if got := retryReason(503, "x"); got != "RETURNED_503" {
		t.Errorf("retryReason(503) = %q", got)
	}
	if got := retryReason(0, "boom"); got != "CONNECTION_ERROR: boom" {
		t.Errorf("retryReason(0, boom) = %q", got)
	}
	if got := retryReason(0, ""); got != "CONNECTION_ERROR" {
		t.Errorf("retryReason(0, empty) = %q", got)
	}
}

func TestScheduleAndStopRemoved(t *testing.T) {
	qs := newQueueState(NewServer(Config{}), &taskspb.Queue{Name: "projects/p/locations/l/queues/q"})

	// schedule on a removed task is a no-op.
	removed := &taskState{pb: &taskspb.Task{ScheduleTime: timestamppb.Now()}, removed: true}
	qs.schedule(removed) // must not panic or arm a timer
	if removed.timer != nil {
		t.Error("removed task should not be scheduled")
	}

	// schedule with a past schedule time clamps to zero delay.
	ts := &taskState{pb: &taskspb.Task{ScheduleTime: timestamppb.New(time.Unix(0, 0))}}
	qs.mu.Lock()
	qs.tasks[ts.pb.GetName()] = ts
	qs.schedule(ts)
	qs.mu.Unlock()
	if ts.timer == nil {
		t.Error("expected a timer")
	}
	ts.timer.Stop()

	qs.stop() // with a task present
	if len(qs.tasks) != 0 {
		t.Error("stop should clear tasks")
	}
}

func TestRebuildLimitsDefaults(t *testing.T) {
	// Empty rate limits -> burst and concurrency floored to 1.
	qs := newQueueState(NewServer(Config{}), &taskspb.Queue{Name: "projects/p/locations/l/queues/q"})
	if qs.limiter.Burst() != 1 {
		t.Errorf("burst = %d, want 1", qs.limiter.Burst())
	}
	if cap(qs.concurrency) != 1 {
		t.Errorf("concurrency cap = %d, want 1", cap(qs.concurrency))
	}
}

func TestFirePausedAndLimiterError(t *testing.T) {
	qs := newQueueState(NewServer(Config{}), &taskspb.Queue{
		Name:  "projects/p/locations/l/queues/q",
		State: taskspb.Queue_PAUSED,
	})
	ts := &taskState{pb: &taskspb.Task{
		Name:         "projects/p/locations/l/queues/q/tasks/t",
		ScheduleTime: timestamppb.Now(),
	}}
	qs.mu.Lock()
	qs.tasks[ts.pb.GetName()] = ts
	qs.mu.Unlock()

	qs.fire(ts) // paused -> re-arm
	if ts.timer == nil {
		t.Error("paused fire should re-arm a timer")
	}
	ts.timer.Stop()

	// removed task fire returns immediately.
	rm := &taskState{pb: &taskspb.Task{Name: "x"}, removed: true}
	qs.fire(rm)

	// Limiter that always errors (finite rate, burst 0) exits fire after Wait
	// fails because n=1 exceeds the burst.
	qs.mu.Lock()
	qs.pb.State = taskspb.Queue_RUNNING
	qs.limiter = rate.NewLimiter(rate.Limit(1), 0)
	qs.mu.Unlock()
	qs.fire(ts) // should return without dispatching
}

func TestPausePollClosureFires(t *testing.T) {
	old := pausePollInterval
	pausePollInterval = 10 * time.Millisecond
	defer func() { pausePollInterval = old }()

	s := NewServer(Config{})
	qs := newQueueState(s, &taskspb.Queue{
		Name:  "projects/p/locations/l/queues/q",
		State: taskspb.Queue_PAUSED,
	})
	applyDefaults(qs.pb)
	qs.pb.State = taskspb.Queue_PAUSED
	qs.rebuildLimits()

	hit := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case hit <- struct{}{}:
		default:
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ts := &taskState{pb: &taskspb.Task{
		Name:         "projects/p/locations/l/queues/q/tasks/t",
		ScheduleTime: timestamppb.Now(),
		MessageType:  &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{Url: srv.URL, HttpMethod: taskspb.HttpMethod_POST}},
	}}
	qs.mu.Lock()
	qs.tasks[ts.pb.GetName()] = ts
	qs.schedule(ts) // timer fires, hits the paused-poll re-arm closure
	qs.mu.Unlock()

	time.Sleep(30 * time.Millisecond) // let it poll while paused at least once
	qs.mu.Lock()
	qs.pb.State = taskspb.Queue_RUNNING
	qs.mu.Unlock()

	select {
	case <-hit:
	case <-time.After(2 * time.Second):
		t.Fatal("task never dispatched after resume")
	}
}

func TestSetIamPolicyNilPolicy(t *testing.T) {
	s := NewServer(Config{})
	name := "projects/p/locations/l/queues/q"
	s.queues[name] = newQueueState(s, &taskspb.Queue{Name: name})

	// nil policy must be normalised to an empty policy.
	pol, err := s.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{Resource: name})
	if err != nil || pol == nil {
		t.Fatalf("SetIamPolicy nil policy: %v / %v", pol, err)
	}
}

func TestAttemptRemovedPaths(t *testing.T) {
	s := NewServer(Config{})
	qs := newQueueState(s, &taskspb.Queue{Name: "projects/p/locations/l/queues/q"})
	applyDefaults(qs.pb)
	qs.rebuildLimits()

	// removed before attempt.
	rm := &taskState{pb: &taskspb.Task{Name: "x"}, removed: true}
	qs.attempt(rm) // no-op

	// removed during dispatch: handler flips removed before responding.
	var ts *taskState
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		qs.mu.Lock()
		ts.removed = true
		qs.mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ts = &taskState{pb: &taskspb.Task{
		Name:         "projects/p/locations/l/queues/q/tasks/t",
		ScheduleTime: timestamppb.Now(),
		MessageType:  &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{Url: srv.URL, HttpMethod: taskspb.HttpMethod_POST}},
	}}
	qs.mu.Lock()
	qs.tasks[ts.pb.GetName()] = ts
	qs.mu.Unlock()
	qs.attempt(ts) // returns at the post-dispatch removed check
}

func TestShouldRetryAndBackoff(t *testing.T) {
	qs := newQueueState(NewServer(Config{}), &taskspb.Queue{Name: "projects/p/locations/l/queues/q"})

	// Unlimited attempts (-1) with a max retry duration that has elapsed.
	qs.pb.RetryConfig = &taskspb.RetryConfig{
		MaxAttempts:      -1,
		MaxRetryDuration: durationpb.New(time.Millisecond),
	}
	ts := &taskState{pb: &taskspb.Task{DispatchCount: 1000}, firstAttemptTime: time.Now().Add(-time.Second)}
	if qs.shouldRetry(ts.pb, ts) {
		t.Error("should stop after max retry duration")
	}

	// Within limits -> retry.
	qs.pb.RetryConfig = &taskspb.RetryConfig{MaxAttempts: 5}
	ts2 := &taskState{pb: &taskspb.Task{DispatchCount: 1}, firstAttemptTime: time.Now()}
	if !qs.shouldRetry(ts2.pb, ts2) {
		t.Error("should retry within limits")
	}

	// Both limits unlimited (-1 attempts, 0 duration) -> retry forever.
	qs.pb.RetryConfig = &taskspb.RetryConfig{MaxAttempts: -1}
	tsInf := &taskState{pb: &taskspb.Task{DispatchCount: 1_000_000}, firstAttemptTime: time.Now().Add(-time.Hour)}
	if !qs.shouldRetry(tsInf.pb, tsInf) {
		t.Error("unlimited config should always retry")
	}

	// Both limits set: attempts exhausted but duration not yet reached -> keep
	// retrying until BOTH are satisfied.
	qs.pb.RetryConfig = &taskspb.RetryConfig{MaxAttempts: 2, MaxRetryDuration: durationpb.New(time.Hour)}
	tsBoth := &taskState{pb: &taskspb.Task{DispatchCount: 5}, firstAttemptTime: time.Now()}
	if !qs.shouldRetry(tsBoth.pb, tsBoth) {
		t.Error("should keep retrying until both attempts and duration limits are reached")
	}
	// Both satisfied -> stop.
	tsDone := &taskState{pb: &taskspb.Task{DispatchCount: 5}, firstAttemptTime: time.Now().Add(-2 * time.Hour)}
	if qs.shouldRetry(tsDone.pb, tsDone) {
		t.Error("should stop once both limits are reached")
	}

	// Exact documented example: min=10s, max=300s, maxDoublings=3 yields the
	// sequence 10, 20, 40, 80, 160, 240, 300, 300 (doubles 3 times to 80, then
	// increases linearly by 2^3*10s = 80s, capped at 300s).
	// https://docs.cloud.google.com/tasks/docs/configuring-queues
	qs.pb.RetryConfig = &taskspb.RetryConfig{
		MinBackoff:   durationpb.New(10 * time.Second),
		MaxBackoff:   durationpb.New(300 * time.Second),
		MaxDoublings: 3,
	}
	wantSeq := []time.Duration{10, 20, 40, 80, 160, 240, 300, 300}
	for i, want := range wantSeq {
		if d := qs.backoff(int32(i + 1)); d != want*time.Second {
			t.Errorf("backoff(%d) = %v, want %v", i+1, d, want*time.Second)
		}
	}

	// Backoff: exponential, then linear, then clamped/overflow.
	qs.pb.RetryConfig = &taskspb.RetryConfig{
		MinBackoff:   durationpb.New(100 * time.Millisecond),
		MaxBackoff:   durationpb.New(2 * time.Second),
		MaxDoublings: 2,
	}
	if d := qs.backoff(1); d != 100*time.Millisecond {
		t.Errorf("backoff(1) = %v", d)
	}
	if d := qs.backoff(3); d != 400*time.Millisecond { // doublings capped at 2
		t.Errorf("backoff(3) = %v, want 400ms", d)
	}
	if d := qs.backoff(10); d != 2*time.Second { // clamped to max via linear growth
		t.Errorf("backoff(10) = %v, want clamp to 2s", d)
	}
	// Overflow path -> negative product clamps to max backoff.
	qs.pb.RetryConfig = &taskspb.RetryConfig{
		MinBackoff:   durationpb.New(time.Second),
		MaxBackoff:   durationpb.New(5 * time.Second),
		MaxDoublings: 100,
	}
	if d := qs.backoff(100); d != 5*time.Second {
		t.Errorf("overflow backoff = %v, want 5s", d)
	}
	// Negative min backoff yields a negative product that clamps to max.
	qs.pb.RetryConfig = &taskspb.RetryConfig{
		MinBackoff:   durationpb.New(-time.Second),
		MaxBackoff:   durationpb.New(3 * time.Second),
		MaxDoublings: 0,
	}
	if d := qs.backoff(1); d != 3*time.Second {
		t.Errorf("negative backoff = %v, want clamp to 3s", d)
	}
}
