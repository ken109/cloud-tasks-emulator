// Package emulator implements an in-memory Google Cloud Tasks emulator that
// speaks the google.cloud.tasks.v2 gRPC API, in the spirit of the official
// Cloud Pub/Sub emulator.
package emulator

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
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
	// HardResetOnPurge is unused placeholder for future options.
}

// Server implements taskspb.CloudTasksServer backed by in-memory state.
type Server struct {
	taskspb.UnimplementedCloudTasksServer

	mu     sync.Mutex
	queues map[string]*queueState // keyed by full queue name

	httpClient           *http.Client
	defaultAppEngineHost string
}

// NewServer constructs an emulator Server.
func NewServer(cfg Config) *Server {
	return &Server{
		queues:               map[string]*queueState{},
		httpClient:           &http.Client{},
		defaultAppEngineHost: cfg.DefaultAppEngineHost,
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
	for name, qs := range s.queues {
		if queueParent(name) == req.GetParent() {
			_ = qs
			names = append(names, name)
		}
	}
	sort.Strings(names)

	resp := &taskspb.ListQueuesResponse{}
	for _, name := range names {
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
	if _, _, _, ok := parseQueueName(q.GetName()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name %q", q.GetName())
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
	if _, _, _, ok := parseQueueName(q.GetName()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name %q", q.GetName())
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

	resp := &taskspb.ListTasksResponse{}
	for _, name := range names {
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
	if task.GetDispatchDeadline() == nil {
		// Default dispatch deadline is 10 minutes for HTTP targets.
	}
	task.DispatchCount = 0
	task.ResponseCount = 0
	task.View = taskspb.Task_BASIC

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if _, exists := qs.tasks[task.GetName()]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "task %q already exists", task.GetName())
	}
	ts := &taskState{pb: task}
	qs.addTask(ts)
	return viewTask(task, req.GetResponseView()), nil
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

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if ts.removed {
		// Task completed/dropped during the forced run; return its last state.
		return viewTask(ts.pb, req.GetResponseView()), nil
	}
	return viewTask(ts.pb, req.GetResponseView()), nil
}

// ---- helpers ----

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
// In BASIC view the request body and headers are omitted.
func viewTask(t *taskspb.Task, view taskspb.Task_View) *taskspb.Task {
	out := proto.Clone(t).(*taskspb.Task)
	if view == taskspb.Task_FULL {
		out.View = taskspb.Task_FULL
		return out
	}
	out.View = taskspb.Task_BASIC
	switch mt := out.GetMessageType().(type) {
	case *taskspb.Task_HttpRequest:
		mt.HttpRequest.Body = nil
		mt.HttpRequest.Headers = nil
	case *taskspb.Task_AppEngineHttpRequest:
		mt.AppEngineHttpRequest.Body = nil
		mt.AppEngineHttpRequest.Headers = nil
	}
	return out
}
