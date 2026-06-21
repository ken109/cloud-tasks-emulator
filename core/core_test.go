package core

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const parent = "projects/p/locations/l"

func newQ(name string) *Queue { return &Queue{Name: parent + "/queues/" + name} }

func httpQ(url string) Target {
	return Target{Type: TargetHTTP, Method: "POST", URL: url, Body: []byte("x")}
}

// ---- naming ----

func TestNaming(t *testing.T) {
	if _, _, ok := parseLocationName("bad"); ok {
		t.Error("loc")
	}
	if _, _, _, ok := parseQueueName(parent); ok {
		t.Error("queue")
	}
	if _, _, _, _, ok := parseTaskName(parent + "/queues/q"); ok {
		t.Error("task")
	}
	if queueID(parent+"/queues/q") != "q" || taskID(parent+"/queues/q/tasks/t") != "t" {
		t.Error("id extract")
	}
}

// ---- pure helpers ----

func TestComputeBurstSize(t *testing.T) {
	for _, c := range []struct {
		rps  float64
		want int32
	}{{-1, 0}, {0.5, 5}, {1, 10}, {50, 20}, {500, 100}, {1000, 100}} {
		if got := computeBurstSize(c.rps); got != c.want {
			t.Errorf("burst(%v)=%d want %d", c.rps, got, c.want)
		}
	}
}

func TestHTTPToRPCCode(t *testing.T) {
	cases := map[int]code.Code{
		400: code.Code_INVALID_ARGUMENT, 401: code.Code_UNAUTHENTICATED, 403: code.Code_PERMISSION_DENIED,
		404: code.Code_NOT_FOUND, 409: code.Code_ALREADY_EXISTS, 429: code.Code_RESOURCE_EXHAUSTED,
		408: code.Code_DEADLINE_EXCEEDED, 504: code.Code_DEADLINE_EXCEEDED, 503: code.Code_UNAVAILABLE,
		501: code.Code_UNIMPLEMENTED, 500: code.Code_INTERNAL, 418: code.Code_UNKNOWN,
	}
	for in, want := range cases {
		if got := httpToRPCCode(in); got != want {
			t.Errorf("httpToRPCCode(%d)=%v want %v", in, got, want)
		}
	}
}

func TestRetryReasonAndTokens(t *testing.T) {
	if retryReason(503, "x") != "RETURNED_503" {
		t.Error("reason code")
	}
	if retryReason(0, "boom") != "CONNECTION_ERROR: boom" {
		t.Error("reason conn msg")
	}
	if retryReason(0, "") != "CONNECTION_ERROR" {
		t.Error("reason conn")
	}
	if !strings.HasPrefix(oauthToken("e", "s"), "emulator-oauth-token/") {
		t.Error("oauth token")
	}
	if methodOrPost("") != http.MethodPost || methodOrPost("GET") != "GET" {
		t.Error("methodOrPost")
	}
	tok := oidcToken("a@b", "aud")
	parts := strings.Split(tok, ".")
	claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
	if !strings.Contains(string(claims), "a@b") {
		t.Errorf("oidc claims %s", claims)
	}
}

func TestApplyURIOverride(t *testing.T) {
	always := &HTTPOverride{Scheme: "https", Host: "h", Port: "8443", Path: "/p", Query: "a=b", AlwaysEnforce: true}
	got := applyURIOverride("http://orig/old?x=1", always)
	if got != "https://h:8443/p?a=b" {
		t.Errorf("always override = %q", got)
	}
	// IF_NOT_EXISTS keeps existing components.
	ifnot := &HTTPOverride{Host: "h2", Path: "/p2"}
	got = applyURIOverride("http://orig/old", ifnot)
	if got != "http://orig/old" {
		t.Errorf("if-not-exists kept = %q", got)
	}
	// Applies the override only to the empty component (path); host is kept.
	got = applyURIOverride("http://orig", ifnot)
	if got != "http://orig/p2" {
		t.Errorf("if-not-exists empty = %q", got)
	}
	// IF_NOT_EXISTS fills an empty host.
	if got := applyURIOverride("/justpath", &HTTPOverride{Host: "h3"}); got != "//h3/justpath" {
		t.Errorf("if-not-exists empty host = %q", got)
	}
	// Parse error returns raw.
	if applyURIOverride("http://bad\n", always) != "http://bad\n" {
		t.Error("parse error should return raw")
	}
}

func TestPaginate(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e"}
	page, next, err := paginate(names, 2, "")
	if err != nil || len(page) != 2 || next == "" {
		t.Fatalf("p1 %v %q %v", page, next, err)
	}
	page, next, _ = paginate(names, 2, next)
	if page[0] != "c" || next == "" {
		t.Fatalf("p2 %v %q", page, next)
	}
	page, next, _ = paginate(names, 2, next)
	if page[0] != "e" || next != "" {
		t.Fatalf("p3 %v %q", page, next)
	}
	if p, _, _ := paginate(names, 0, ""); len(p) != 5 {
		t.Error("default size")
	}
	if _, _, err := paginate(names, maxPageSize+1, ""); err != nil {
		t.Error("cap")
	}
	if p, _, _ := paginate(names, 2, enc("99")); len(p) != 0 {
		t.Error("out of range")
	}
	if _, _, err := paginate(names, 2, "###"); status.Code(err) != codes.InvalidArgument {
		t.Error("bad base64")
	}
	if _, _, err := paginate(names, 2, enc("x")); status.Code(err) != codes.InvalidArgument {
		t.Error("non int")
	}
	if _, _, err := paginate(names, 2, enc("-1")); status.Code(err) != codes.InvalidArgument {
		t.Error("negative")
	}
}

func enc(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

// ---- dispatch / build ----

func TestAppEngineHostResolution(t *testing.T) {
	e := NewEngine(Config{DefaultAppEngineHost: "http://def"})
	if e.appEngineHost(&Queue{}, &Task{Target: Target{AppEngineRouting: &AppEngineRouting{Host: "http://task"}}}) != "http://task" {
		t.Error("task host")
	}
	if e.appEngineHost(&Queue{AppEngineHostOverride: "http://q"}, &Task{}) != "http://q" {
		t.Error("queue host")
	}
	if e.appEngineHost(&Queue{}, &Task{}) != "http://def" {
		t.Error("default host")
	}
	if NewEngine(Config{}).appEngineHost(&Queue{}, &Task{}) != "" {
		t.Error("empty host")
	}
}

func TestBuildRequestErrors(t *testing.T) {
	e := NewEngine(Config{})
	q := newQ("q")
	if _, err := e.buildRequest(q, &Task{Name: q.Name + "/tasks/t"}, attemptInfo{number: 1}); err == nil {
		t.Error("no target")
	}
	if _, err := e.buildRequest(q, &Task{Target: Target{Type: TargetAppEngine, RelativeURI: "/x"}}, attemptInfo{number: 1}); err == nil {
		t.Error("appengine no host")
	}
	// malformed url
	if _, err := e.buildRequest(q, &Task{Target: Target{Type: TargetHTTP, URL: "http://bad\n"}}, attemptInfo{number: 1}); err == nil {
		t.Error("bad url")
	}
}

func TestBuildHTTPRequestHeadersAndAuth(t *testing.T) {
	e := NewEngine(Config{})
	q := newQ("q")
	task := &Task{
		Name:         q.Name + "/tasks/42",
		ScheduleTime: time.Unix(123, 456000),
		Target:       Target{Type: TargetHTTP, Method: "POST", URL: "http://h/p", Body: []byte("x"), Auth: &Auth{Kind: AuthOIDC, ServiceAccountEmail: "sa"}},
	}
	req, err := e.buildRequest(q, task, attemptInfo{number: 3, executionCount: 1, prevHTTPCode: 500, prevReason: "RETURNED_500"})
	if err != nil {
		t.Fatal(err)
	}
	if req.Header.Get("User-Agent") != httpUserAgent {
		t.Error("ua")
	}
	if req.Header.Get("X-CloudTasks-TaskRetryCount") != "2" || req.Header.Get("X-CloudTasks-TaskExecutionCount") != "1" {
		t.Error("counts")
	}
	if req.Header.Get("X-CloudTasks-TaskETA") != "123.000456" {
		t.Errorf("eta %q", req.Header.Get("X-CloudTasks-TaskETA"))
	}
	if req.Header.Get("X-CloudTasks-TaskPreviousResponse") != "500" || req.Header.Get("X-CloudTasks-TaskRetryReason") != "RETURNED_500" {
		t.Error("prev headers")
	}
	if req.Header.Get("Content-Type") != "application/octet-stream" {
		t.Error("default ct")
	}
	if !strings.HasPrefix(req.Header.Get("Authorization"), "Bearer ") {
		t.Error("oidc auth")
	}
	// OAuth + explicit content type retained.
	task.Target.Auth = &Auth{Kind: AuthOAuth, ServiceAccountEmail: "sa", Scope: "s"}
	task.Target.Headers = map[string]string{"Content-Type": "application/json"}
	req, _ = e.buildRequest(q, task, attemptInfo{number: 1})
	if req.Header.Get("Content-Type") != "application/json" || !strings.HasPrefix(req.Header.Get("Authorization"), "Bearer ") {
		t.Error("oauth/explicit ct")
	}
}

func TestBuildHTTPRequestOverride(t *testing.T) {
	e := NewEngine(Config{})
	q := newQ("q")
	q.HTTPOverride = &HTTPOverride{Host: "over", AlwaysEnforce: true, Method: "PUT", Headers: map[string]string{"X-O": "1"}, Auth: &Auth{Kind: AuthOIDC, ServiceAccountEmail: "sa"}}
	task := &Task{Name: q.Name + "/tasks/t", Target: Target{Type: TargetHTTP, Method: "POST", URL: "http://orig/x"}}
	req, err := e.buildRequest(q, task, attemptInfo{number: 1})
	if err != nil {
		t.Fatal(err)
	}
	if req.URL.Host != "over" || req.Method != "PUT" || req.Header.Get("X-O") != "1" {
		t.Errorf("override req = %s %s hdr=%s", req.Method, req.URL.Host, req.Header.Get("X-O"))
	}
	if !strings.HasPrefix(req.Header.Get("Authorization"), "Bearer ") {
		t.Error("override auth")
	}
}

func TestAppEngineRequest(t *testing.T) {
	e := NewEngine(Config{DefaultAppEngineHost: "http://svc"})
	q := newQ("q")
	task := &Task{Name: q.Name + "/tasks/t", ScheduleTime: time.Unix(1, 0), Target: Target{Type: TargetAppEngine, Method: "GET", RelativeURI: "/work"}}
	req, err := e.buildRequest(q, task, attemptInfo{number: 2})
	if err != nil || req.URL.String() != "http://svc/work" {
		t.Fatalf("ae req %v %v", req, err)
	}
	if req.Header.Get("User-Agent") != appEngineUserAgent || req.Header.Get("X-AppEngine-FailFast") != "false" {
		t.Error("ae headers")
	}
}

func TestDispatchOutcomes(t *testing.T) {
	e := NewEngine(Config{})
	q := newQ("q")
	mk := func(u string) *Task {
		return &Task{Name: q.Name + "/tasks/t", ScheduleTime: time.Now(), Target: httpQ(u)}
	}

	c, hc, _ := e.dispatch(q, &Task{Name: q.Name + "/tasks/t"}, attemptInfo{number: 1})
	if c != int32(code.Code_INVALID_ARGUMENT) || hc != 0 {
		t.Error("no target dispatch")
	}
	c, hc, _ = e.dispatch(q, mk("http://127.0.0.1:1"), attemptInfo{number: 1})
	if c != int32(code.Code_UNAVAILABLE) || hc != 0 {
		t.Error("transport error")
	}
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer ok.Close()
	c, hc, _ = e.dispatch(q, mk(ok.URL), attemptInfo{number: 1})
	if c != 0 || hc != 200 {
		t.Error("ok")
	}
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer fail.Close()
	ft := mk(fail.URL)
	ft.DispatchDeadline = 30 * time.Second
	c, hc, _ = e.dispatch(q, ft, attemptInfo{number: 1})
	if c != int32(code.Code_INTERNAL) || hc != 500 {
		t.Error("fail")
	}
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.invalid/", http.StatusFound)
	}))
	defer redir.Close()
	c, hc, _ = e.dispatch(q, mk(redir.URL), attemptInfo{number: 1})
	if c == 0 || hc != http.StatusFound {
		t.Errorf("redirect not followed: c=%d hc=%d", c, hc)
	}
}

// ---- backoff / retry ----

func TestBackoff(t *testing.T) {
	qs := newQueueState(NewEngine(Config{}), newQ("q"))
	qs.q.RetryConfig = RetryConfig{MinBackoff: 10 * time.Second, MaxBackoff: 300 * time.Second, MaxDoublings: 3}
	want := []time.Duration{10, 20, 40, 80, 160, 240, 300, 300}
	for i, w := range want {
		if d := qs.backoff(int32(i + 1)); d != w*time.Second {
			t.Errorf("backoff(%d)=%v want %v", i+1, d, w*time.Second)
		}
	}
	// overflow -> max
	qs.q.RetryConfig = RetryConfig{MinBackoff: time.Second, MaxBackoff: 5 * time.Second, MaxDoublings: 100}
	if qs.backoff(100) != 5*time.Second {
		t.Error("overflow")
	}
	// negative min -> max
	qs.q.RetryConfig = RetryConfig{MinBackoff: -time.Second, MaxBackoff: 3 * time.Second, MaxDoublings: 0}
	if qs.backoff(1) != 3*time.Second {
		t.Error("negative")
	}
}

func TestShouldRetry(t *testing.T) {
	qs := newQueueState(NewEngine(Config{}), newQ("q"))
	// both unlimited
	qs.q.RetryConfig = RetryConfig{MaxAttempts: -1}
	if !qs.shouldRetry(&Task{DispatchCount: 1e6}, &taskState{firstAttemptTime: time.Now().Add(-time.Hour)}) {
		t.Error("unlimited")
	}
	// attempts only (default-like)
	qs.q.RetryConfig = RetryConfig{MaxAttempts: 3}
	if qs.shouldRetry(&Task{DispatchCount: 3}, &taskState{}) {
		t.Error("attempts reached")
	}
	if !qs.shouldRetry(&Task{DispatchCount: 1}, &taskState{}) {
		t.Error("attempts left")
	}
	// duration only
	qs.q.RetryConfig = RetryConfig{MaxAttempts: -1, MaxRetryDuration: time.Hour}
	if !qs.shouldRetry(&Task{}, &taskState{firstAttemptTime: time.Now()}) {
		t.Error("duration left")
	}
	if qs.shouldRetry(&Task{}, &taskState{firstAttemptTime: time.Now().Add(-2 * time.Hour)}) {
		t.Error("duration reached")
	}
	// both set: stop only when both reached
	qs.q.RetryConfig = RetryConfig{MaxAttempts: 2, MaxRetryDuration: time.Hour}
	if !qs.shouldRetry(&Task{DispatchCount: 5}, &taskState{firstAttemptTime: time.Now()}) {
		t.Error("attempts reached but duration left -> retry")
	}
	if qs.shouldRetry(&Task{DispatchCount: 5}, &taskState{firstAttemptTime: time.Now().Add(-2 * time.Hour)}) {
		t.Error("both reached -> stop")
	}
}

// ---- queue runtime ----

func TestRebuildLimitsFloors(t *testing.T) {
	qs := newQueueState(NewEngine(Config{}), newQ("q")) // empty rate limits
	if qs.limiter.Burst() != 1 || cap(qs.concurrency) != 1 {
		t.Errorf("floors burst=%d conc=%d", qs.limiter.Burst(), cap(qs.concurrency))
	}
}

func TestEffectiveTTLs(t *testing.T) {
	e := NewEngine(Config{TaskTTL: time.Minute, TombstoneTTL: time.Second})
	qs := newQueueState(e, &Queue{Name: parent + "/queues/q"})
	if qs.effectiveTaskTTL() != time.Minute || qs.effectiveTombstoneTTL() != time.Second {
		t.Error("engine defaults")
	}
	qs.q.TaskTTL = time.Hour
	qs.q.TombstoneTTL = 2 * time.Hour
	if qs.effectiveTaskTTL() != time.Hour || qs.effectiveTombstoneTTL() != 2*time.Hour {
		t.Error("queue overrides")
	}
}

func TestFireAndExpireAndStop(t *testing.T) {
	e := NewEngine(Config{})
	qs := newQueueState(e, newQ("q"))
	applyQueueDefaults(qs.q)
	qs.rebuildLimits()

	// schedule on removed = no-op
	qs.schedule(&taskState{t: &Task{}, removed: true})

	// paused fire re-arms; removed fire returns; limiter-error fire returns.
	qs.q.State = StatePaused
	ts := &taskState{t: &Task{Name: qs.q.Name + "/tasks/t", ScheduleTime: time.Now()}}
	qs.mu.Lock()
	qs.tasks[ts.t.Name] = ts
	qs.mu.Unlock()
	qs.fire(ts)
	if ts.timer == nil {
		t.Error("paused rearm")
	}
	ts.timer.Stop()
	qs.fire(&taskState{t: &Task{Name: "x"}, removed: true})
	qs.mu.Lock()
	qs.q.State = StateRunning
	qs.limiter = rate.NewLimiter(rate.Limit(1), 0)
	qs.mu.Unlock()
	qs.fire(ts)

	// expire removed no-op + real
	qs.expire(&taskState{t: &Task{Name: "y"}, removed: true})

	// stop with a live task tears down timers.
	e2 := NewEngine(Config{})
	q2, _ := e2.CreateQueue(parent, newQ("q2"))
	_, _ = e2.CreateTask(q2.Name, &Task{ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")})
	if err := e2.DeleteQueue(q2.Name); err != nil {
		t.Fatal(err)
	}
}

func TestPullNotDispatchedAndTTL(t *testing.T) {
	e := NewEngine(Config{TaskTTL: 30 * time.Millisecond})
	q, _ := e.CreateQueue(parent, &Queue{Name: parent + "/queues/pull", Pull: true})
	// pull task isn't scheduled for dispatch
	tk, _ := e.CreateTask(q.Name, &Task{Target: Target{Type: TargetPull, Body: []byte("p")}})
	time.Sleep(80 * time.Millisecond) // exceeds TTL -> expired
	if _, err := e.GetTask(tk.Name); status.Code(err) != codes.NotFound {
		t.Errorf("ttl expiry: %v", err)
	}
}

func TestTombstoneDisabledAndCleanup(t *testing.T) {
	// disabled
	e := NewEngine(Config{TombstoneTTL: -1})
	q, _ := e.CreateQueue(parent, newQ("nt"))
	name := q.Name + "/tasks/x"
	mk := func() error {
		_, err := e.CreateTask(q.Name, &Task{Name: name, ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")})
		return err
	}
	if err := mk(); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteTask(name); err != nil {
		t.Fatal(err)
	}
	if err := mk(); err != nil {
		t.Errorf("disabled tombstone reuse: %v", err)
	}
	// cleanup timer
	e2 := NewEngine(Config{TombstoneTTL: 20 * time.Millisecond})
	q2, _ := e2.CreateQueue(parent, newQ("st"))
	n2 := q2.Name + "/tasks/y"
	e2.CreateTask(q2.Name, &Task{Name: n2, ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")})
	e2.DeleteTask(n2)
	time.Sleep(60 * time.Millisecond)
	if _, err := e2.CreateTask(q2.Name, &Task{Name: n2, ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")}); err != nil {
		t.Errorf("post-cleanup reuse: %v", err)
	}
}

func TestCloneTask(t *testing.T) {
	src := &Task{Target: Target{Headers: map[string]string{"a": "b"}, Body: []byte("x"), AppEngineRouting: &AppEngineRouting{Host: "h"}, Auth: &Auth{Kind: AuthOIDC}}}
	c := cloneTask(src)
	c.Target.Headers["a"] = "z"
	c.Target.Body[0] = 'y'
	if src.Target.Headers["a"] != "b" || src.Target.Body[0] != 'x' {
		t.Error("clone not deep")
	}
	if c.Target.AppEngineRouting == src.Target.AppEngineRouting || c.Target.Auth == src.Target.Auth {
		t.Error("clone shares pointers")
	}
}

// ---- engine CRUD errors ----

func TestEngineQueueErrors(t *testing.T) {
	e := NewEngine(Config{})
	if _, err := e.CreateQueue("bad", newQ("q")); status.Code(err) != codes.InvalidArgument {
		t.Error("bad parent")
	}
	if _, err := e.CreateQueue(parent, &Queue{}); status.Code(err) != codes.InvalidArgument {
		t.Error("empty name")
	}
	if _, err := e.CreateQueue(parent, &Queue{Name: "bad"}); status.Code(err) != codes.InvalidArgument {
		t.Error("bad name")
	}
	if _, err := e.CreateQueue(parent, &Queue{Name: parent + "/queues/bad_id"}); status.Code(err) != codes.InvalidArgument {
		t.Error("bad id")
	}
	if _, err := e.CreateQueue("projects/x/locations/y", &Queue{Name: parent + "/queues/q"}); status.Code(err) != codes.InvalidArgument {
		t.Error("mismatch")
	}
	e.CreateQueue(parent, newQ("q"))
	if _, err := e.CreateQueue(parent, newQ("q")); status.Code(err) != codes.AlreadyExists {
		t.Error("dup")
	}
	if _, err := e.GetQueue(parent + "/queues/missing"); status.Code(err) != codes.NotFound {
		t.Error("get missing")
	}
	if _, _, err := e.ListQueues("bad", 0, ""); status.Code(err) != codes.InvalidArgument {
		t.Error("list bad parent")
	}
	if _, _, err := e.ListQueues(parent, 0, "###"); status.Code(err) != codes.InvalidArgument {
		t.Error("list bad token")
	}
	if err := e.DeleteQueue(parent + "/queues/missing"); status.Code(err) != codes.NotFound {
		t.Error("delete missing")
	}
	if _, err := e.PurgeQueue(parent + "/queues/missing"); status.Code(err) != codes.NotFound {
		t.Error("purge missing")
	}
	if _, err := e.SetQueueState(parent+"/queues/missing", StatePaused); status.Code(err) != codes.NotFound {
		t.Error("setstate missing")
	}
}

func TestEngineUpdateQueue(t *testing.T) {
	e := NewEngine(Config{})
	if _, err := e.UpdateQueue(&Queue{}, nil); status.Code(err) != codes.InvalidArgument {
		t.Error("empty name")
	}
	if _, err := e.UpdateQueue(&Queue{Name: "bad"}, nil); status.Code(err) != codes.InvalidArgument {
		t.Error("bad name")
	}
	if _, err := e.UpdateQueue(&Queue{Name: parent + "/queues/bad_id"}, nil); status.Code(err) != codes.InvalidArgument {
		t.Error("bad id")
	}
	// create-on-missing
	name := parent + "/queues/upd"
	if _, err := e.UpdateQueue(&Queue{Name: name}, nil); err != nil {
		t.Fatal(err)
	}
	// each field + empty mask
	q := &Queue{Name: name, RateLimits: RateLimits{MaxDispatchesPerSecond: 5}, RetryConfig: RetryConfig{MaxAttempts: 9}, AppEngineHostOverride: "http://a", HTTPOverride: &HTTPOverride{Host: "h"}}
	got, err := e.UpdateQueue(q, []QueueField{FieldRateLimits, FieldRetryConfig, FieldAppEngineRoutingOverride, FieldHTTPOverride})
	if err != nil || got.RetryConfig.MaxAttempts != 9 || got.AppEngineHostOverride != "http://a" || got.HTTPOverride == nil {
		t.Fatalf("update fields %+v %v", got, err)
	}
	if _, err := e.UpdateQueue(q, nil); err != nil { // empty mask = all
		t.Fatal(err)
	}
}

func TestEngineTaskErrors(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("q"))

	if _, err := e.CreateTask("bad", &Task{Target: httpQ("http://x")}); status.Code(err) != codes.InvalidArgument {
		t.Error("bad parent")
	}
	if _, err := e.CreateTask(q.Name, nil); status.Code(err) != codes.InvalidArgument {
		t.Error("nil task")
	}
	if _, err := e.CreateTask(q.Name, &Task{}); status.Code(err) != codes.InvalidArgument {
		t.Error("no target")
	}
	if _, err := e.CreateTask(parent+"/queues/missing", &Task{Target: httpQ("http://x")}); status.Code(err) != codes.NotFound {
		t.Error("missing queue")
	}
	bad := &Task{Name: "not-a-task", Target: httpQ("http://x")}
	if _, err := e.CreateTask(q.Name, bad); status.Code(err) != codes.InvalidArgument {
		t.Error("bad task name")
	}
	badID := &Task{Name: q.Name + "/tasks/bad.id", Target: httpQ("http://x")}
	if _, err := e.CreateTask(q.Name, badID); status.Code(err) != codes.InvalidArgument {
		t.Error("bad id")
	}
	mism := &Task{Name: parent + "/queues/other/tasks/t", Target: httpQ("http://x")}
	if _, err := e.CreateTask(q.Name, mism); status.Code(err) != codes.InvalidArgument {
		t.Error("mismatch")
	}
	named := &Task{Name: q.Name + "/tasks/n1", ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")}
	if _, err := e.CreateTask(q.Name, named); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CreateTask(q.Name, &Task{Name: q.Name + "/tasks/n1", ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")}); status.Code(err) != codes.AlreadyExists {
		t.Error("dup task")
	}

	if _, err := e.GetTask("bad"); status.Code(err) != codes.InvalidArgument {
		t.Error("get bad")
	}
	if _, err := e.GetTask(parent + "/queues/missing/tasks/t"); status.Code(err) != codes.NotFound {
		t.Error("get missing queue")
	}
	if _, err := e.GetTask(q.Name + "/tasks/nope"); status.Code(err) != codes.NotFound {
		t.Error("get missing task")
	}
	if _, _, err := e.ListTasks(parent+"/queues/missing", 0, ""); status.Code(err) != codes.NotFound {
		t.Error("list missing queue")
	}
	if _, _, err := e.ListTasks(q.Name, 0, "###"); status.Code(err) != codes.InvalidArgument {
		t.Error("list bad token")
	}
	if err := e.DeleteTask("bad"); status.Code(err) != codes.InvalidArgument {
		t.Error("del bad")
	}
	if err := e.DeleteTask(q.Name + "/tasks/nope"); status.Code(err) != codes.NotFound {
		t.Error("del missing")
	}
	if _, err := e.RunTask("bad"); status.Code(err) != codes.InvalidArgument {
		t.Error("run bad")
	}
	if _, err := e.RunTask(parent + "/queues/missing/tasks/t"); status.Code(err) != codes.NotFound {
		t.Error("run missing queue")
	}
	if _, err := e.RunTask(q.Name + "/tasks/nope"); status.Code(err) != codes.NotFound {
		t.Error("run missing task")
	}
}

func TestValidateDeadlinesAndPull(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("q"))
	mk := func(t0 *Task) error { _, err := e.CreateTask(q.Name, t0); return err }

	if mk(&Task{Target: Target{Type: TargetHTTP, URL: "http://x"}, DispatchDeadline: time.Second}) == nil {
		t.Error("http tiny deadline")
	}
	if mk(&Task{Target: Target{Type: TargetHTTP, URL: "http://x"}, DispatchDeadline: time.Hour}) == nil {
		t.Error("http big deadline")
	}
	// App Engine allows up to 24h15s.
	if err := mk(&Task{Name: q.Name + "/tasks/ae", ScheduleTime: time.Now().Add(time.Hour), Target: Target{Type: TargetAppEngine, RelativeURI: "/x"}, DispatchDeadline: time.Hour}); err != nil {
		t.Errorf("ae 1h deadline should be ok: %v", err)
	}
	if mk(&Task{Target: Target{Type: TargetAppEngine, RelativeURI: "/x"}, DispatchDeadline: 48 * time.Hour}) == nil {
		t.Error("ae huge deadline")
	}
	if mk(&Task{Target: Target{Type: TargetAppEngine, RelativeURI: "/x", Body: make([]byte, maxAppEngineTaskBodySize+1)}}) == nil {
		t.Error("ae body")
	}
	if mk(&Task{Target: Target{Type: TargetPull, Body: make([]byte, maxHTTPTaskBodySize+1)}}) == nil {
		t.Error("pull body")
	}
}

func TestRunTaskPull(t *testing.T) {
	e := NewEngine(Config{})
	// pull queue
	pq, _ := e.CreateQueue(parent, &Queue{Name: parent + "/queues/pq", Pull: true})
	tk, _ := e.CreateTask(pq.Name, &Task{Target: Target{Type: TargetPull, Body: []byte("p")}})
	if _, err := e.RunTask(tk.Name); status.Code(err) != codes.FailedPrecondition {
		t.Error("run pull queue")
	}
	// push queue with pull task
	push, _ := e.CreateQueue(parent, newQ("push"))
	pt, _ := e.CreateTask(push.Name, &Task{Target: Target{Type: TargetPull, Body: []byte("p")}})
	if _, err := e.RunTask(pt.Name); status.Code(err) != codes.FailedPrecondition {
		t.Error("run pull task")
	}
}

func TestRunTaskSuccess(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("q"))
	done := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case done <- struct{}{}:
		default:
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()
	tk, _ := e.CreateTask(q.Name, &Task{Name: q.Name + "/tasks/run", ScheduleTime: time.Now().Add(time.Hour), Target: httpQ(ts.URL)})
	if _, err := e.RunTask(tk.Name); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not dispatch")
	}
}

func TestEngineIAM(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("q"))
	if p, err := e.GetIamPolicy(q.Name); err != nil || len(p.GetBindings()) != 0 {
		t.Fatalf("get %v %v", p, err)
	}
	if _, err := e.SetIamPolicy(q.Name, &iampb.Policy{Bindings: []*iampb.Binding{{Role: "r"}}}); err != nil {
		t.Fatal(err)
	}
	if p, _ := e.GetIamPolicy(q.Name); p.GetBindings()[0].GetRole() != "r" {
		t.Error("policy roundtrip")
	}
	if _, err := e.SetIamPolicy(q.Name, nil); err != nil { // nil normalised
		t.Error("nil policy")
	}
	if perms, err := e.TestIamPermissions(q.Name, []string{"a"}); err != nil || len(perms) != 1 {
		t.Error("test perms")
	}
	for _, bad := range []string{"bad"} {
		if _, err := e.GetIamPolicy(bad); status.Code(err) != codes.InvalidArgument {
			t.Error("get bad")
		}
		if _, err := e.SetIamPolicy(bad, &iampb.Policy{}); status.Code(err) != codes.InvalidArgument {
			t.Error("set bad")
		}
		if _, err := e.TestIamPermissions(bad, nil); status.Code(err) != codes.InvalidArgument {
			t.Error("test bad")
		}
	}
	missing := parent + "/queues/missing"
	if _, err := e.GetIamPolicy(missing); status.Code(err) != codes.NotFound {
		t.Error("get missing")
	}
	if _, err := e.SetIamPolicy(missing, &iampb.Policy{}); status.Code(err) != codes.NotFound {
		t.Error("set missing")
	}
	if _, err := e.TestIamPermissions(missing, nil); status.Code(err) != codes.NotFound {
		t.Error("test missing")
	}
}

func TestListQueuesPaginationRPC(t *testing.T) {
	e := NewEngine(Config{})
	for _, id := range []string{"a", "b", "c"} {
		e.CreateQueue(parent, newQ(id))
	}
	p1, next, err := e.ListQueues(parent, 2, "")
	if err != nil || len(p1) != 2 || next == "" {
		t.Fatalf("p1 %d %q %v", len(p1), next, err)
	}
	p2, next2, _ := e.ListQueues(parent, 2, next)
	if len(p2) != 1 || next2 != "" {
		t.Fatalf("p2 %d %q", len(p2), next2)
	}
}

func drain(t *testing.T, e *Engine, queue string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ts, _, _ := e.ListTasks(queue, 0, "")
		if len(ts) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("queue did not drain")
}

func TestDispatchViaTimer(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("disp"))
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(200)
	}))
	defer ts.Close()
	// Past schedule time exercises the d<0 clamp in schedule().
	if _, err := e.CreateTask(q.Name, &Task{ScheduleTime: time.Now().Add(-time.Second), Target: httpQ(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	drain(t, e, q.Name)
	if atomic.LoadInt32(&hits) != 1 {
		t.Errorf("hits=%d", hits)
	}
}

func TestRetryViaTimer(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, &Queue{
		Name:        parent + "/queues/retry",
		RetryConfig: RetryConfig{MaxAttempts: 5, MinBackoff: 2 * time.Millisecond, MaxBackoff: 4 * time.Millisecond},
	})
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) < 3 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()
	if _, err := e.CreateTask(q.Name, &Task{Target: httpQ(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	drain(t, e, q.Name)
	if atomic.LoadInt32(&hits) != 3 {
		t.Errorf("hits=%d want 3", hits)
	}
}

func TestEngineSuccessPaths(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("ok"))

	if got, err := e.GetQueue(q.Name); err != nil || got.Name != q.Name {
		t.Fatalf("GetQueue %v %v", got, err)
	}
	// Pending tasks for list/get/purge.
	for i := 0; i < 2; i++ {
		if _, err := e.CreateTask(q.Name, &Task{ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")}); err != nil {
			t.Fatal(err)
		}
	}
	ts, _, err := e.ListTasks(q.Name, 0, "")
	if err != nil || len(ts) != 2 {
		t.Fatalf("ListTasks %d %v", len(ts), err)
	}
	if got, err := e.GetTask(ts[0].Name); err != nil || got.Name != ts[0].Name {
		t.Fatalf("GetTask %v %v", got, err)
	}
	if pq, err := e.PurgeQueue(q.Name); err != nil || pq.PurgeTime.IsZero() {
		t.Fatalf("Purge %v %v", pq, err)
	}
	drain(t, e, q.Name)
}

func TestTombstoneReuseBlocked(t *testing.T) {
	e := NewEngine(Config{}) // default 24h tombstone
	q, _ := e.CreateQueue(parent, newQ("tomb"))
	name := q.Name + "/tasks/reused"
	mk := func() error {
		_, err := e.CreateTask(q.Name, &Task{Name: name, ScheduleTime: time.Now().Add(time.Hour), Target: httpQ("http://127.0.0.1:1")})
		return err
	}
	if err := mk(); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteTask(name); err != nil {
		t.Fatal(err)
	}
	if status.Code(mk()) != codes.AlreadyExists {
		t.Error("tombstone should block reuse")
	}
}

func TestValidateScheduleAndBody(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("v"))
	far := &Task{ScheduleTime: time.Now().Add(40 * 24 * time.Hour), Target: httpQ("http://x")}
	if _, err := e.CreateTask(q.Name, far); status.Code(err) != codes.InvalidArgument {
		t.Error("far schedule")
	}
	big := &Task{Target: Target{Type: TargetHTTP, URL: "http://x", Body: make([]byte, maxHTTPTaskBodySize+1)}}
	if _, err := e.CreateTask(q.Name, big); status.Code(err) != codes.InvalidArgument {
		t.Error("big http body")
	}
}

func TestAppEngineMalformedURL(t *testing.T) {
	e := NewEngine(Config{DefaultAppEngineHost: "http://svc"})
	q := newQ("q")
	task := &Task{Name: q.Name + "/tasks/t", Target: Target{Type: TargetAppEngine, RelativeURI: "/bad\n/x"}}
	if _, err := e.buildRequest(q, task, attemptInfo{number: 1}); err == nil {
		t.Error("expected error for malformed app engine URL")
	}
}

func TestSetQueueStateSuccess(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("st"))
	if p, err := e.SetQueueState(q.Name, StatePaused); err != nil || p.State != StatePaused {
		t.Fatalf("pause %v %v", p, err)
	}
	if r, err := e.SetQueueState(q.Name, StateRunning); err != nil || r.State != StateRunning {
		t.Fatalf("resume %v %v", r, err)
	}
}

func TestPausePollClosureFires(t *testing.T) {
	old := pausePollInterval
	pausePollInterval = 10 * time.Millisecond
	defer func() { pausePollInterval = old }()

	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, &Queue{Name: parent + "/queues/pp", State: StatePaused})
	hit := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case hit <- struct{}{}:
		default:
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()
	if _, err := e.CreateTask(q.Name, &Task{ScheduleTime: time.Now(), Target: httpQ(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // poll fires while paused (re-arm closure)
	e.SetQueueState(q.Name, StateRunning)
	select {
	case <-hit:
	case <-time.After(2 * time.Second):
		t.Fatal("task never dispatched after resume")
	}
}

func TestAttemptRemovedPaths(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("ar"))
	qs := e.queues[q.Name]

	// removed before attempt
	qs.attempt(&taskState{t: &Task{Name: "x"}, removed: true})

	// removed during dispatch (handler flips removed before responding)
	var ts *taskState
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		qs.mu.Lock()
		ts.removed = true
		qs.mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv.Close()
	ts = &taskState{t: &Task{Name: q.Name + "/tasks/t", ScheduleTime: time.Now(), Target: httpQ(srv.URL)}}
	qs.mu.Lock()
	qs.tasks[ts.t.Name] = ts
	qs.mu.Unlock()
	qs.attempt(ts)
}

func TestExhaustViaTimer(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, &Queue{
		Name:        parent + "/queues/ex",
		RetryConfig: RetryConfig{MaxAttempts: 1, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	})
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(500)
	}))
	defer ts.Close()
	if _, err := e.CreateTask(q.Name, &Task{Target: httpQ(ts.URL)}); err != nil {
		t.Fatal(err)
	}
	drain(t, e, q.Name)
	if atomic.LoadInt32(&hits) != 1 {
		t.Errorf("hits=%d want 1 (dropped after first failure)", hits)
	}
}

func TestScheduledNotFiredEarly(t *testing.T) {
	e := NewEngine(Config{})
	q, _ := e.CreateQueue(parent, newQ("q"))
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(200)
	}))
	defer ts.Close()
	tk := &Task{ScheduleTime: time.Now().Add(300 * time.Millisecond), Target: httpQ(ts.URL)}
	created, _ := e.CreateTask(q.Name, tk)
	time.Sleep(80 * time.Millisecond)
	if got, _ := e.GetTask(created.Name); got == nil {
		t.Error("should still exist before schedule")
	}
	_ = strconv.Itoa(0)
}
