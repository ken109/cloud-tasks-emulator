package emulator

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pausePollInterval is how often a held task re-checks whether its queue has
// resumed while it is paused/disabled. It is a var so tests can shorten it.
var pausePollInterval = time.Second

// taskState tracks the live scheduling state of a single task.
type taskState struct {
	pb      *taskspb.Task
	timer   *time.Timer
	removed bool
	// firstScheduleTime is when the task first became eligible to run; used to
	// enforce RetryConfig.MaxRetryDuration.
	firstScheduleTime time.Time
}

// queueState is the runtime representation of a queue: its proto plus the
// machinery that dispatches tasks.
type queueState struct {
	mu  sync.Mutex
	pb  *taskspb.Queue
	srv *Server

	tasks       map[string]*taskState // keyed by full task name
	limiter     *rate.Limiter
	concurrency chan struct{}

	idSeq uint64
}

func newQueueState(srv *Server, q *taskspb.Queue) *queueState {
	qs := &queueState{
		pb:    q,
		srv:   srv,
		tasks: map[string]*taskState{},
	}
	qs.rebuildLimits()
	return qs
}

// rebuildLimits (re)constructs the rate limiter and concurrency semaphore from
// the queue's current RateLimits. Must be called with mu held or before the
// queue is published.
func (qs *queueState) rebuildLimits() {
	rl := qs.pb.GetRateLimits()
	rps := rl.GetMaxDispatchesPerSecond()
	burst := int(rl.GetMaxBurstSize())
	if burst < 1 {
		burst = 1
	}
	qs.limiter = rate.NewLimiter(rate.Limit(rps), burst)

	maxConc := int(rl.GetMaxConcurrentDispatches())
	if maxConc < 1 {
		maxConc = 1
	}
	qs.concurrency = make(chan struct{}, maxConc)
}

// schedule arms (or re-arms) the dispatch timer for a task. mu must be held.
func (qs *queueState) schedule(ts *taskState) {
	if ts.removed {
		return
	}
	d := time.Until(ts.pb.GetScheduleTime().AsTime())
	if d < 0 {
		d = 0
	}
	ts.timer = time.AfterFunc(d, func() { qs.fire(ts) })
}

// fire is invoked when a task's timer expires.
func (qs *queueState) fire(ts *taskState) {
	qs.mu.Lock()
	if ts.removed {
		qs.mu.Unlock()
		return
	}
	state := qs.pb.GetState()
	if state == taskspb.Queue_PAUSED || state == taskspb.Queue_DISABLED {
		// Hold the task and re-check shortly.
		ts.timer = time.AfterFunc(pausePollInterval, func() { qs.fire(ts) })
		qs.mu.Unlock()
		return
	}
	qs.mu.Unlock()

	// Rate-limit then bound concurrency before dispatching.
	if err := qs.limiter.Wait(context.Background()); err != nil {
		return
	}
	qs.concurrency <- struct{}{}
	defer func() { <-qs.concurrency }()

	qs.attempt(ts)
}

// attempt dispatches a single delivery attempt and applies the retry policy.
func (qs *queueState) attempt(ts *taskState) {
	qs.mu.Lock()
	if ts.removed {
		qs.mu.Unlock()
		return
	}
	task := ts.pb
	scheduled := task.GetScheduleTime().AsTime()
	attemptNum := task.GetDispatchCount() + 1
	taskCopy := proto.Clone(task).(*taskspb.Task)
	queueCopy := proto.Clone(qs.pb).(*taskspb.Queue)
	qs.mu.Unlock()

	dispatchTime := time.Now()
	statusProto, _ := qs.srv.dispatch(queueCopy, taskCopy, attemptNum)
	responseTime := time.Now()

	att := &taskspb.Attempt{
		ScheduleTime:   timestamppb.New(scheduled),
		DispatchTime:   timestamppb.New(dispatchTime),
		ResponseTime:   timestamppb.New(responseTime),
		ResponseStatus: statusProto,
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if ts.removed {
		return
	}

	task.DispatchCount++
	task.ResponseCount++
	task.LastAttempt = att
	if task.FirstAttempt == nil {
		task.FirstAttempt = &taskspb.Attempt{
			ScheduleTime: att.ScheduleTime,
			DispatchTime: att.DispatchTime,
		}
	}

	if statusProto.GetCode() == 0 { // OK -> task completed, drop it.
		qs.removeLocked(ts)
		return
	}

	// Failure: decide whether to retry.
	if !qs.shouldRetry(task, ts) {
		log.Printf("task %s exhausted retries (code=%d), dropping", task.GetName(), statusProto.GetCode())
		qs.removeLocked(ts)
		return
	}

	backoff := qs.backoff(task.GetDispatchCount())
	task.ScheduleTime = timestamppb.New(time.Now().Add(backoff))
	qs.schedule(ts)
}

// shouldRetry reports whether a failed task has retries remaining. mu held.
func (qs *queueState) shouldRetry(task *taskspb.Task, ts *taskState) bool {
	rc := qs.pb.GetRetryConfig()
	maxAttempts := rc.GetMaxAttempts()
	// -1 means unlimited.
	if maxAttempts != -1 && task.GetDispatchCount() >= maxAttempts {
		return false
	}
	if d := rc.GetMaxRetryDuration().AsDuration(); d > 0 {
		if time.Since(ts.firstScheduleTime) >= d {
			return false
		}
	}
	return true
}

// backoff computes the delay before the Nth retry following Cloud Tasks rules.
// mu held.
func (qs *queueState) backoff(retries int32) time.Duration {
	rc := qs.pb.GetRetryConfig()
	minB := rc.GetMinBackoff().AsDuration()
	maxB := rc.GetMaxBackoff().AsDuration()
	maxDoublings := rc.GetMaxDoublings()

	// retries is the number of attempts already made (>=1 here).
	doublings := retries - 1
	if doublings > maxDoublings {
		doublings = maxDoublings
	}
	d := time.Duration(float64(minB) * math.Pow(2, float64(doublings)))
	// After max_doublings, growth becomes linear.
	if retries-1 > maxDoublings {
		extra := time.Duration(retries-1-maxDoublings) * time.Duration(float64(minB)*math.Pow(2, float64(maxDoublings)))
		d += extra
	}
	if d > maxB {
		d = maxB
	}
	if d < 0 {
		d = maxB
	}
	return d
}

// addTask registers and schedules a new task. mu held.
func (qs *queueState) addTask(ts *taskState) {
	qs.tasks[ts.pb.GetName()] = ts
	ts.firstScheduleTime = ts.pb.GetScheduleTime().AsTime()
	qs.schedule(ts)
}

// removeLocked stops and deletes a task. mu held.
func (qs *queueState) removeLocked(ts *taskState) {
	ts.removed = true
	if ts.timer != nil {
		ts.timer.Stop()
	}
	delete(qs.tasks, ts.pb.GetName())
}

// purge removes every task currently in the queue.
func (qs *queueState) purge() {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	for _, ts := range qs.tasks {
		ts.removed = true
		if ts.timer != nil {
			ts.timer.Stop()
		}
	}
	qs.tasks = map[string]*taskState{}
	qs.pb.PurgeTime = timestamppb.Now()
}

// stop tears down all timers (used when a queue is deleted).
func (qs *queueState) stop() {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	for _, ts := range qs.tasks {
		ts.removed = true
		if ts.timer != nil {
			ts.timer.Stop()
		}
	}
	qs.tasks = map[string]*taskState{}
}

// applyDefaults fills unset RateLimits / RetryConfig fields with the documented
// Cloud Tasks defaults.
func applyDefaults(q *taskspb.Queue) {
	if q.RateLimits == nil {
		q.RateLimits = &taskspb.RateLimits{}
	}
	rl := q.RateLimits
	if rl.MaxDispatchesPerSecond == 0 {
		rl.MaxDispatchesPerSecond = 500
	}
	if rl.MaxConcurrentDispatches == 0 {
		rl.MaxConcurrentDispatches = 1000
	}
	if rl.MaxBurstSize == 0 {
		rl.MaxBurstSize = computeBurstSize(rl.MaxDispatchesPerSecond)
	}

	if q.RetryConfig == nil {
		q.RetryConfig = &taskspb.RetryConfig{}
	}
	rc := q.RetryConfig
	if rc.MaxAttempts == 0 {
		rc.MaxAttempts = 100
	}
	if rc.MinBackoff == nil {
		rc.MinBackoff = durationpb.New(100 * time.Millisecond)
	}
	if rc.MaxBackoff == nil {
		rc.MaxBackoff = durationpb.New(3600 * time.Second)
	}
	if rc.MaxDoublings == 0 {
		rc.MaxDoublings = 16
	}

	if q.State == taskspb.Queue_STATE_UNSPECIFIED {
		q.State = taskspb.Queue_RUNNING
	}
}

// computeBurstSize mirrors the token-bucket burst Cloud Tasks derives from the
// dispatch rate.
func computeBurstSize(rps float64) int32 {
	switch {
	case rps < 0:
		return 0
	case rps >= 500:
		return 100
	case rps >= 50:
		return 20
	case rps >= 1:
		return 10
	default:
		return 5
	}
}
