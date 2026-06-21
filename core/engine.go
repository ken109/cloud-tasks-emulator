package core

import (
	"encoding/base64"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Config configures an Engine.
type Config struct {
	DefaultAppEngineHost string
	TaskTTL              time.Duration
	TombstoneTTL         time.Duration
}

// Default lifecycle durations matching Cloud Tasks: tasks live up to 31 days,
// and a deleted/completed task name is reserved for up to 24 hours.
const (
	defaultTaskTTL      = 31 * 24 * time.Hour
	defaultTombstoneTTL = 24 * time.Hour
)

// Engine is the version-agnostic in-memory Cloud Tasks engine.
type Engine struct {
	mu       sync.Mutex
	queues   map[string]*queueState
	policies map[string]*iampb.Policy

	httpClient           *http.Client
	defaultAppEngineHost string
	taskTTL              time.Duration
	tombstoneTTL         time.Duration
}

// NewEngine constructs an Engine.
func NewEngine(cfg Config) *Engine {
	taskTTL := cfg.TaskTTL
	if taskTTL == 0 {
		taskTTL = defaultTaskTTL
	}
	tombstoneTTL := cfg.TombstoneTTL
	if tombstoneTTL == 0 {
		tombstoneTTL = defaultTombstoneTTL
	}
	return &Engine{
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

// QueueField identifies a mutable queue field for UpdateQueue.
type QueueField int

const (
	FieldRateLimits QueueField = iota
	FieldRetryConfig
	FieldAppEngineRoutingOverride
	FieldHTTPOverride
)

// ---- Queue operations ----

func (e *Engine) CreateQueue(parent string, q *Queue) (*Queue, error) {
	if _, _, ok := parseLocationName(parent); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent %q", parent)
	}
	if q == nil || q.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "queue.name is required")
	}
	if _, _, _, ok := parseQueueName(q.Name); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name %q", q.Name)
	}
	if !queueIDRe.MatchString(queueID(q.Name)) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue id %q: must match %s", queueID(q.Name), queueIDRe)
	}
	if queueParent(q.Name) != parent {
		return nil, status.Error(codes.InvalidArgument, "queue name does not match parent")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.queues[q.Name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "queue %q already exists", q.Name)
	}
	applyQueueDefaults(q)
	qs := newQueueState(e, q)
	e.queues[q.Name] = qs
	return qs.snapshot(), nil
}

func (e *Engine) GetQueue(name string) (*Queue, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	qs, err := e.queueLocked(name)
	if err != nil {
		return nil, err
	}
	return qs.snapshot(), nil
}

func (e *Engine) ListQueues(parent string, pageSize int32, pageToken string) ([]*Queue, string, error) {
	if _, _, ok := parseLocationName(parent); !ok {
		return nil, "", status.Errorf(codes.InvalidArgument, "invalid parent %q", parent)
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	var names []string
	for name := range e.queues {
		if queueParent(name) == parent {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	page, next, err := paginate(names, pageSize, pageToken)
	if err != nil {
		return nil, "", err
	}
	out := make([]*Queue, 0, len(page))
	for _, name := range page {
		out = append(out, e.queues[name].snapshot())
	}
	return out, next, nil
}

func (e *Engine) UpdateQueue(q *Queue, fields []QueueField) (*Queue, error) {
	if q == nil || q.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "queue.name is required")
	}
	if _, _, _, ok := parseQueueName(q.Name); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name %q", q.Name)
	}
	if !queueIDRe.MatchString(queueID(q.Name)) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue id %q: must match %s", queueID(q.Name), queueIDRe)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	qs, ok := e.queues[q.Name]
	if !ok {
		// UpdateQueue creates the queue if it does not exist.
		applyQueueDefaults(q)
		qs = newQueueState(e, q)
		e.queues[q.Name] = qs
		return qs.snapshot(), nil
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if len(fields) == 0 {
		fields = []QueueField{FieldRateLimits, FieldRetryConfig, FieldAppEngineRoutingOverride, FieldHTTPOverride}
	}
	for _, f := range fields {
		switch f {
		case FieldRateLimits:
			qs.q.RateLimits = q.RateLimits
		case FieldRetryConfig:
			qs.q.RetryConfig = q.RetryConfig
		case FieldAppEngineRoutingOverride:
			qs.q.AppEngineRoutingOverride = q.AppEngineRoutingOverride
		case FieldHTTPOverride:
			qs.q.HTTPOverride = q.HTTPOverride
		}
	}
	applyQueueDefaults(qs.q)
	qs.rebuildLimits()
	return qs.snapshotLocked(), nil
}

func (e *Engine) DeleteQueue(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	qs, err := e.queueLocked(name)
	if err != nil {
		return err
	}
	qs.stop()
	delete(e.queues, name)
	delete(e.policies, name)
	return nil
}

func (e *Engine) PurgeQueue(name string) (*Queue, error) {
	e.mu.Lock()
	qs, err := e.queueLocked(name)
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	qs.purge()
	return qs.snapshot(), nil
}

func (e *Engine) SetQueueState(name string, st State) (*Queue, error) {
	e.mu.Lock()
	qs, err := e.queueLocked(name)
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.q.State = st
	return qs.snapshotLocked(), nil
}

// ---- Task operations ----

func (e *Engine) CreateTask(parent string, t *Task) (*Task, error) {
	if _, _, _, ok := parseQueueName(parent); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent %q", parent)
	}
	if t == nil {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	if err := validateCreateTask(t); err != nil {
		return nil, err
	}

	e.mu.Lock()
	qs, err := e.queueLocked(parent)
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if t.Name == "" {
		t.Name = parent + "/tasks/" + qs.nextTaskID()
	} else {
		_, _, _, id, ok := parseTaskName(t.Name)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "invalid task name %q", t.Name)
		}
		if !idRe.MatchString(id) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid task id %q", id)
		}
		if taskQueueOf(t.Name) != parent {
			return nil, status.Error(codes.InvalidArgument, "task name does not match parent queue")
		}
	}

	now := time.Now()
	t.CreateTime = now
	if t.ScheduleTime.IsZero() {
		t.ScheduleTime = now
	}
	t.DispatchCount = 0
	t.ResponseCount = 0

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if _, exists := qs.tasks[t.Name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "task %q already exists", t.Name)
	}
	if qs.tombstoned(t.Name) {
		return nil, status.Errorf(codes.AlreadyExists, "task %q was recently deleted and its name is still reserved", t.Name)
	}
	ts := &taskState{t: t}
	qs.addTask(ts)
	return cloneTask(t), nil
}

func (e *Engine) GetTask(name string) (*Task, error) {
	qs, err := e.taskQueue(name)
	if err != nil {
		return nil, err
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	ts, ok := qs.tasks[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", name)
	}
	return cloneTask(ts.t), nil
}

func (e *Engine) ListTasks(parent string, pageSize int32, pageToken string) ([]*Task, string, error) {
	e.mu.Lock()
	qs, err := e.queueLocked(parent)
	e.mu.Unlock()
	if err != nil {
		return nil, "", err
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()

	var names []string
	for name := range qs.tasks {
		names = append(names, name)
	}
	sort.Strings(names)

	page, next, err := paginate(names, pageSize, pageToken)
	if err != nil {
		return nil, "", err
	}
	out := make([]*Task, 0, len(page))
	for _, name := range page {
		out = append(out, cloneTask(qs.tasks[name].t))
	}
	return out, next, nil
}

func (e *Engine) DeleteTask(name string) error {
	qs, err := e.taskQueue(name)
	if err != nil {
		return err
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	ts, ok := qs.tasks[name]
	if !ok {
		return status.Errorf(codes.NotFound, "task %q not found", name)
	}
	qs.removeLocked(ts)
	return nil
}

func (e *Engine) RunTask(name string) (*Task, error) {
	qs, err := e.taskQueue(name)
	if err != nil {
		return nil, err
	}
	qs.mu.Lock()
	if qs.q.Pull {
		qs.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "RunTask cannot be called on a pull queue")
	}
	ts, ok := qs.tasks[name]
	if !ok {
		qs.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "task %q not found", name)
	}
	if ts.t.Target.Type == TargetPull {
		qs.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "RunTask cannot be called on a pull task")
	}
	if ts.timer != nil {
		ts.timer.Stop()
	}
	ts.t.ScheduleTime = time.Now()
	qs.mu.Unlock()

	qs.attempt(ts)

	qs.mu.Lock()
	defer qs.mu.Unlock()
	return cloneTask(ts.t), nil
}

// ---- IAM ----

func (e *Engine) GetIamPolicy(resource string) (*iampb.Policy, error) {
	if _, _, _, ok := parseQueueName(resource); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource %q", resource)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.queueLocked(resource); err != nil {
		return nil, err
	}
	if p, ok := e.policies[resource]; ok {
		return proto.Clone(p).(*iampb.Policy), nil
	}
	return &iampb.Policy{}, nil
}

func (e *Engine) SetIamPolicy(resource string, policy *iampb.Policy) (*iampb.Policy, error) {
	if _, _, _, ok := parseQueueName(resource); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource %q", resource)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.queueLocked(resource); err != nil {
		return nil, err
	}
	if policy == nil {
		policy = &iampb.Policy{}
	} else {
		policy = proto.Clone(policy).(*iampb.Policy)
	}
	e.policies[resource] = policy
	return proto.Clone(policy).(*iampb.Policy), nil
}

func (e *Engine) TestIamPermissions(resource string, permissions []string) ([]string, error) {
	if _, _, _, ok := parseQueueName(resource); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource %q", resource)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.queueLocked(resource); err != nil {
		return nil, err
	}
	return permissions, nil
}

// ---- helpers ----

func (e *Engine) queueLocked(name string) (*queueState, error) {
	qs, ok := e.queues[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "queue %q not found", name)
	}
	return qs, nil
}

func (e *Engine) taskQueue(name string) (*queueState, error) {
	if _, _, _, _, ok := parseTaskName(name); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task name %q", name)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queueLocked(taskQueueOf(name))
}

func (qs *queueState) snapshot() *Queue {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return qs.snapshotLocked()
}

func (qs *queueState) snapshotLocked() *Queue {
	c := *qs.q
	c.TasksCount = int64(len(qs.tasks))
	if qs.q.HTTPOverride != nil {
		ov := *qs.q.HTTPOverride
		c.HTTPOverride = &ov
	}
	if qs.q.AppEngineRoutingOverride != nil {
		r := *qs.q.AppEngineRoutingOverride
		c.AppEngineRoutingOverride = &r
	}
	return &c
}

// Cloud Tasks resource limits enforced at task creation.
const (
	maxHTTPTaskBodySize      = 1024 * 1024 // 1 MiB for HTTP targets
	maxAppEngineTaskBodySize = 100 * 1024  // 100 KiB for App Engine targets
	maxScheduleAhead         = 30 * 24 * time.Hour
	minDispatchDeadline      = 15 * time.Second
	maxHTTPDispatchDeadline  = 30 * time.Minute
	maxAppEngineDeadline     = 24*time.Hour + 15*time.Second
)

func validateCreateTask(t *Task) error {
	if !t.ScheduleTime.IsZero() && t.ScheduleTime.After(time.Now().Add(maxScheduleAhead)) {
		return status.Error(codes.InvalidArgument, "schedule_time must not be more than 30 days in the future")
	}
	switch t.Target.Type {
	case TargetHTTP:
		if err := checkDeadline(t.DispatchDeadline, maxHTTPDispatchDeadline); err != nil {
			return err
		}
		if len(t.Target.Body) > maxHTTPTaskBodySize {
			return status.Error(codes.InvalidArgument, "task body exceeds the 1MB limit for HTTP targets")
		}
	case TargetAppEngine:
		if err := checkDeadline(t.DispatchDeadline, maxAppEngineDeadline); err != nil {
			return err
		}
		if len(t.Target.Body) > maxAppEngineTaskBodySize {
			return status.Error(codes.InvalidArgument, "task body exceeds the 100KB limit for App Engine targets")
		}
	case TargetPull:
		if len(t.Target.Body) > maxHTTPTaskBodySize {
			return status.Error(codes.InvalidArgument, "pull message payload exceeds the 1MB limit")
		}
	default:
		return status.Error(codes.InvalidArgument, "task must specify a target")
	}
	return nil
}

func checkDeadline(d, max time.Duration) error {
	if d != 0 && (d < minDispatchDeadline || d > max) {
		return status.Errorf(codes.InvalidArgument, "dispatch_deadline must be between %s and %s", minDispatchDeadline, max)
	}
	return nil
}

// Pagination bounds for List RPCs.
const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

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
