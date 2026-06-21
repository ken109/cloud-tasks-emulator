// Package emulator implements an in-memory Google Cloud Tasks emulator that
// speaks the google.cloud.tasks.v2 gRPC API, in the spirit of the official
// Cloud Pub/Sub emulator.
package emulator

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config configures a Server.
type Config struct {
	// DefaultAppEngineHost is the base URL used to dispatch AppEngineHttpRequest
	// tasks when no host is given on the task or queue.
	DefaultAppEngineHost string
	// TaskTTL is how long a task may live before it is automatically deleted
	// (Cloud Tasks: 31 days). Zero uses the default.
	TaskTTL time.Duration
	// TombstoneTTL is how long a created task's name is reserved after the task
	// completes or is deleted, preventing immediate reuse. The Cloud Tasks docs
	// say a deleted task name can take up to 24 hours to be released (longer for
	// queue.yaml-managed queues). Zero uses the default; a negative value
	// disables tombstones.
	TombstoneTTL time.Duration
}

// Default lifecycle durations matching Cloud Tasks: tasks live up to 31 days,
// and a deleted/completed task name is reserved for up to 24 hours.
const (
	defaultTaskTTL      = 31 * 24 * time.Hour
	defaultTombstoneTTL = 24 * time.Hour
)

// Server implements taskspb.CloudTasksServer backed by in-memory state.
type Server struct {
	taskspb.UnimplementedCloudTasksServer

	mu       sync.Mutex
	queues   map[string]*queueState   // keyed by full queue name
	policies map[string]*iampb.Policy // IAM policy keyed by queue name

	httpClient           *http.Client
	defaultAppEngineHost string
	taskTTL              time.Duration
	tombstoneTTL         time.Duration
}

// NewServer constructs an emulator Server.
func NewServer(cfg Config) *Server {
	taskTTL := cfg.TaskTTL
	if taskTTL == 0 {
		taskTTL = defaultTaskTTL
	}
	tombstoneTTL := cfg.TombstoneTTL
	if tombstoneTTL == 0 {
		tombstoneTTL = defaultTombstoneTTL
	}
	return &Server{
		queues:   map[string]*queueState{},
		policies: map[string]*iampb.Policy{},
		httpClient: &http.Client{
			// Cloud Tasks does not follow redirects; a 3xx is a failed dispatch.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		defaultAppEngineHost: cfg.DefaultAppEngineHost,
		taskTTL:              taskTTL,
		tombstoneTTL:         tombstoneTTL,
	}
}

// ---- Queue RPCs ----

func (s *Server) ListQueues(_ context.Context, req *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	if _, _, ok := parseLocationName(req.GetParent()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent %q", req.GetParent())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var names []string
	for name := range s.queues {
		if queueParent(name) == req.GetParent() {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	page, next, err := paginate(names, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &taskspb.ListQueuesResponse{NextPageToken: next}
	for _, name := range page {
		resp.Queues = append(resp.Queues, s.queues[name].snapshot())
	}
	return resp, nil
}

func (s *Server) GetQueue(_ context.Context, req *taskspb.GetQueueRequest) (*taskspb.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	qs, err := s.queueLocked(req.GetName())
	if err != nil {
		return nil, err
	}
	return qs.snapshot(), nil
}

func (s *Server) CreateQueue(_ context.Context, req *taskspb.CreateQueueRequest) (*taskspb.Queue, error) {
	if _, _, ok := parseLocationName(req.GetParent()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent %q", req.GetParent())
	}
	q := req.GetQueue()
	if q == nil || q.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue.name is required")
	}
	_, _, queueID, ok := parseQueueName(q.GetName())
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name %q", q.GetName())
	}
	if !queueIDRe.MatchString(queueID) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue id %q: must match %s", queueID, queueIDRe)
	}
	if queueParent(q.GetName()) != req.GetParent() {
		return nil, status.Error(codes.InvalidArgument, "queue name does not match parent")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queues[q.GetName()]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "queue %q already exists", q.GetName())
	}

	q = proto.Clone(q).(*taskspb.Queue)
	applyDefaults(q)
	qs := newQueueState(s, q)
	s.queues[q.GetName()] = qs
	return qs.snapshot(), nil
}

func (s *Server) UpdateQueue(_ context.Context, req *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	q := req.GetQueue()
	if q == nil || q.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue.name is required")
	}
	_, _, queueID, ok := parseQueueName(q.GetName())
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name %q", q.GetName())
	}
	if !queueIDRe.MatchString(queueID) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue id %q: must match %s", queueID, queueIDRe)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	qs, ok := s.queues[q.GetName()]
	if !ok {
		// UpdateQueue creates the queue if it does not exist.
		nq := proto.Clone(q).(*taskspb.Queue)
		applyDefaults(nq)
		qs = newQueueState(s, nq)
		s.queues[q.GetName()] = qs
		return qs.snapshot(), nil
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		// Replace mutable fields wholesale.
		paths = []string{"rate_limits", "retry_config", "app_engine_routing_override", "stackdriver_logging_config"}
	}
	for _, p := range paths {
		switch p {
		case "rate_limits":
			qs.pb.RateLimits = proto.Clone(q.GetRateLimits()).(*taskspb.RateLimits)
		case "retry_config":
			qs.pb.RetryConfig = proto.Clone(q.GetRetryConfig()).(*taskspb.RetryConfig)
		case "app_engine_routing_override":
			qs.pb.AppEngineRoutingOverride = proto.Clone(q.GetAppEngineRoutingOverride()).(*taskspb.AppEngineRouting)
		case "stackdriver_logging_config":
			qs.pb.StackdriverLoggingConfig = proto.Clone(q.GetStackdriverLoggingConfig()).(*taskspb.StackdriverLoggingConfig)
		}
	}
	applyDefaults(qs.pb)
	qs.rebuildLimits()
	return cloneSnapshot(qs.pb), nil
}

func (s *Server) DeleteQueue(_ context.Context, req *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	qs, err := s.queueLocked(req.GetName())
	if err != nil {
		return nil, err
	}
	qs.stop()
	delete(s.queues, req.GetName())
	delete(s.policies, req.GetName())
	return &emptypb.Empty{}, nil
}

func (s *Server) PurgeQueue(_ context.Context, req *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	s.mu.Lock()
	qs, err := s.queueLocked(req.GetName())
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	qs.purge()
	return qs.snapshot(), nil
}

func (s *Server) PauseQueue(_ context.Context, req *taskspb.PauseQueueRequest) (*taskspb.Queue, error) {
	return s.setQueueState(req.GetName(), taskspb.Queue_PAUSED)
}

func (s *Server) ResumeQueue(_ context.Context, req *taskspb.ResumeQueueRequest) (*taskspb.Queue, error) {
	return s.setQueueState(req.GetName(), taskspb.Queue_RUNNING)
}

func (s *Server) setQueueState(name string, st taskspb.Queue_State) (*taskspb.Queue, error) {
	s.mu.Lock()
	qs, err := s.queueLocked(name)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.pb.State = st
	return cloneSnapshot(qs.pb), nil
}

// ---- Task RPCs ----

func (s *Server) ListTasks(_ context.Context, req *taskspb.ListTasksRequest) (*taskspb.ListTasksResponse, error) {
	s.mu.Lock()
	qs, err := s.queueLocked(req.GetParent())
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()

	var names []string
	for name := range qs.tasks {
		names = append(names, name)
	}
	sort.Strings(names)

	page, next, err := paginate(names, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &taskspb.ListTasksResponse{NextPageToken: next}
	for _, name := range page {
		resp.Tasks = append(resp.Tasks, viewTask(qs.tasks[name].pb, req.GetResponseView()))
	}
	return resp, nil
}

func (s *Server) GetTask(_ context.Context, req *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	qName, err := taskQueueName(req.GetName())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	qs, err := s.queueLocked(qName)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()
	ts, ok := qs.tasks[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetName())
	}
	return viewTask(ts.pb, req.GetResponseView()), nil
}

func (s *Server) CreateTask(_ context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	if _, _, _, ok := parseQueueName(req.GetParent()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent %q", req.GetParent())
	}
	task := req.GetTask()
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	if task.GetMessageType() == nil {
		return nil, status.Error(codes.InvalidArgument, "task must specify an http_request or app_engine_http_request")
	}
	if err := validateCreateTask(task); err != nil {
		return nil, err
	}

	s.mu.Lock()
	qs, err := s.queueLocked(req.GetParent())
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	task = proto.Clone(task).(*taskspb.Task)

	// Resolve / validate the task name.
	if task.GetName() == "" {
		task.Name = fmt.Sprintf("%s/tasks/%s", req.GetParent(), qs.nextTaskID())
	} else {
		_, _, _, id, ok := parseTaskName(task.GetName())
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "invalid task name %q", task.GetName())
		}
		if !idRe.MatchString(id) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid task id %q", id)
		}
		if taskQueueOf(task.GetName()) != req.GetParent() {
			return nil, status.Error(codes.InvalidArgument, "task name does not match parent queue")
		}
	}

	now := time.Now()
	task.CreateTime = timestamppb.New(now)
	if task.GetScheduleTime() == nil {
		task.ScheduleTime = timestamppb.New(now)
	}
	task.DispatchCount = 0
	task.ResponseCount = 0
	task.View = taskspb.Task_BASIC

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if _, exists := qs.tasks[task.GetName()]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "task %q already exists", task.GetName())
	}
	if qs.tombstoned(task.GetName()) {
		return nil, status.Errorf(codes.AlreadyExists, "task %q was recently deleted and its name is still reserved", task.GetName())
	}
	ts := &taskState{pb: task}
	qs.addTask(ts)
	return viewTask(task, req.GetResponseView()), nil
}

// Cloud Tasks resource limits enforced at task creation.
const (
	maxHTTPTaskBodySize      = 1024 * 1024 // 1 MiB for HTTP targets
	maxAppEngineTaskBodySize = 100 * 1024  // 100 KiB for App Engine targets
	maxScheduleAhead         = 30 * 24 * time.Hour
	minDispatchDeadline      = 15 * time.Second
	maxHTTPDispatchDeadline  = 30 * time.Minute              // HTTP targets
	maxAppEngineDeadline     = 24*time.Hour + 15*time.Second // App Engine targets
)

// validateCreateTask enforces the documented size, schedule and deadline limits.
func validateCreateTask(task *taskspb.Task) error {
	if st := task.GetScheduleTime(); st != nil && st.AsTime().After(time.Now().Add(maxScheduleAhead)) {
		return status.Error(codes.InvalidArgument, "schedule_time must not be more than 30 days in the future")
	}

	// The dispatch_deadline range depends on the target type.
	maxDeadline := maxHTTPDispatchDeadline
	if _, ok := task.GetMessageType().(*taskspb.Task_AppEngineHttpRequest); ok {
		maxDeadline = maxAppEngineDeadline
	}
	if dd := task.GetDispatchDeadline(); dd != nil {
		d := dd.AsDuration()
		if d < minDispatchDeadline || d > maxDeadline {
			return status.Errorf(codes.InvalidArgument, "dispatch_deadline must be between %s and %s", minDispatchDeadline, maxDeadline)
		}
	}

	switch mt := task.GetMessageType().(type) {
	case *taskspb.Task_HttpRequest:
		if len(mt.HttpRequest.GetBody()) > maxHTTPTaskBodySize {
			return status.Error(codes.InvalidArgument, "task body exceeds the 1MB limit for HTTP targets")
		}
	case *taskspb.Task_AppEngineHttpRequest:
		if len(mt.AppEngineHttpRequest.GetBody()) > maxAppEngineTaskBodySize {
			return status.Error(codes.InvalidArgument, "task body exceeds the 100KB limit for App Engine targets")
		}
	}
	return nil
}

func (s *Server) DeleteTask(_ context.Context, req *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	qName, err := taskQueueName(req.GetName())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	qs, err := s.queueLocked(qName)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()
	ts, ok := qs.tasks[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetName())
	}
	qs.removeLocked(ts)
	return &emptypb.Empty{}, nil
}

func (s *Server) RunTask(_ context.Context, req *taskspb.RunTaskRequest) (*taskspb.Task, error) {
	qName, err := taskQueueName(req.GetName())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	qs, err := s.queueLocked(qName)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	qs.mu.Lock()
	ts, ok := qs.tasks[req.GetName()]
	if !ok {
		qs.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetName())
	}
	// Cancel the pending timer and run immediately.
	if ts.timer != nil {
		ts.timer.Stop()
	}
	ts.pb.ScheduleTime = timestamppb.Now()
	qs.mu.Unlock()

	qs.attempt(ts)

	// ts.pb holds the latest state whether or not the task was dropped during
	// the forced run.
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return viewTask(ts.pb, req.GetResponseView()), nil
}

// ---- helpers ----

// Pagination bounds for List RPCs.
const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

// paginate returns the slice of names for the requested page and the token for
// the next page (empty when the listing is exhausted). Page tokens encode the
// next offset.
func paginate(names []string, pageSize int32, pageToken string) ([]string, string, error) {
	start := 0
	if pageToken != "" {
		raw, err := base64.RawURLEncoding.DecodeString(pageToken)
		if err != nil {
			return nil, "", status.Errorf(codes.InvalidArgument, "invalid page_token %q", pageToken)
		}
		start, err = strconv.Atoi(string(raw))
		if err != nil || start < 0 {
			return nil, "", status.Errorf(codes.InvalidArgument, "invalid page_token %q", pageToken)
		}
	}
	size := int(pageSize)
	if size <= 0 {
		size = defaultPageSize
	}
	if size > maxPageSize {
		size = maxPageSize
	}
	if start > len(names) {
		start = len(names)
	}
	end := start + size
	if end >= len(names) {
		return names[start:], "", nil
	}
	next := base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	return names[start:end], next, nil
}

func (s *Server) queueLocked(name string) (*queueState, error) {
	qs, ok := s.queues[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "queue %q not found", name)
	}
	return qs, nil
}

func (qs *queueState) snapshot() *taskspb.Queue {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return cloneSnapshot(qs.pb)
}

func cloneSnapshot(q *taskspb.Queue) *taskspb.Queue {
	return proto.Clone(q).(*taskspb.Queue)
}

func (qs *queueState) nextTaskID() string {
	qs.idSeq++
	return strconv.FormatUint(uint64(time.Now().UnixNano())+qs.idSeq, 10)
}

// taskQueueName extracts and validates the parent queue name of a task.
func taskQueueName(taskName string) (string, error) {
	if _, _, _, _, ok := parseTaskName(taskName); !ok {
		return "", status.Errorf(codes.InvalidArgument, "invalid task name %q", taskName)
	}
	return taskQueueOf(taskName), nil
}

func taskQueueOf(taskName string) string {
	p, l, q, _, _ := parseTaskName(taskName)
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s", p, l, q)
}

// viewTask returns a copy of the task respecting the requested response view.
// Per the Task.View docs, the BASIC view omits only the AppEngineHttpRequest
// body; all other fields (including HttpRequest body/headers) are returned.
func viewTask(t *taskspb.Task, view taskspb.Task_View) *taskspb.Task {
	out := proto.Clone(t).(*taskspb.Task)
	if view == taskspb.Task_FULL {
		out.View = taskspb.Task_FULL
		return out
	}
	out.View = taskspb.Task_BASIC
	if mt, ok := out.GetMessageType().(*taskspb.Task_AppEngineHttpRequest); ok {
		mt.AppEngineHttpRequest.Body = nil
	}
	return out
}
