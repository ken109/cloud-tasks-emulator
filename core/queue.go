package core

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// pausePollInterval is how often a held task re-checks whether its queue has
// resumed while it is paused/disabled. A var so tests can shorten it.
var pausePollInterval = time.Second

// taskState tracks the live scheduling state of a single task.
type taskState struct {
	t        *Task
	timer    *time.Timer
	ttlTimer *time.Timer
	removed  bool
	// firstAttemptTime is when the task was first dispatched; used to enforce
	// RetryConfig.MaxRetryDuration.
	firstAttemptTime time.Time
	lastHTTPCode     int
	lastRetryReason  string
}

// queueState is the runtime representation of a queue.
type queueState struct {
	mu  sync.Mutex
	q   *Queue
	eng *Engine

	tasks       map[string]*taskState // keyed by full task name
	tombstones  map[string]time.Time  // task name -> reservation expiry
	limiter     *rate.Limiter
	concurrency chan struct{}

	idSeq uint64
}

func newQueueState(eng *Engine, q *Queue) *queueState {
	qs := &queueState{
		q:          q,
		eng:        eng,
		tasks:      map[string]*taskState{},
		tombstones: map[string]time.Time{},
	}
	qs.rebuildLimits()
	return qs
}

// rebuildLimits (re)builds the rate limiter and concurrency semaphore. mu held
// or queue not yet published.
func (qs *queueState) rebuildLimits() {
	rl := qs.q.RateLimits
	burst := int(rl.MaxBurstSize)
	if burst < 1 {
		burst = 1
	}
	qs.limiter = rate.NewLimiter(rate.Limit(rl.MaxDispatchesPerSecond), burst)

	maxConc := int(rl.MaxConcurrentDispatches)
	if maxConc < 1 {
		maxConc = 1
	}
	qs.concurrency = make(chan struct{}, maxConc)
}

// effectiveTaskTTL / effectiveTombstoneTTL apply per-queue overrides.
func (qs *queueState) effectiveTaskTTL() time.Duration {
	if qs.q.TaskTTL > 0 {
		return qs.q.TaskTTL
	}
	return qs.eng.taskTTL
}

func (qs *queueState) effectiveTombstoneTTL() time.Duration {
	if qs.q.TombstoneTTL > 0 {
		return qs.q.TombstoneTTL
	}
	return qs.eng.tombstoneTTL
}

// schedule arms the dispatch timer for a task. mu held.
func (qs *queueState) schedule(ts *taskState) {
	if ts.removed || qs.q.Pull || ts.t.Target.Type == TargetPull {
		return // pull tasks/queues are never auto-dispatched
	}
	d := time.Until(ts.t.ScheduleTime)
	if d < 0 {
		d = 0
	}
	ts.timer = time.AfterFunc(d, func() { qs.fire(ts) })
}

// fire runs when a task's timer expires.
func (qs *queueState) fire(ts *taskState) {
	qs.mu.Lock()
	if ts.removed {
		qs.mu.Unlock()
		return
	}
	if qs.q.State == StatePaused || qs.q.State == StateDisabled {
		ts.timer = time.AfterFunc(pausePollInterval, func() { qs.fire(ts) })
		qs.mu.Unlock()
		return
	}
	// Capture the limiter and concurrency semaphore under the lock; UpdateQueue
	// may swap them via rebuildLimits while a dispatch is in flight.
	limiter := qs.limiter
	concurrency := qs.concurrency
	qs.mu.Unlock()

	if err := limiter.Wait(context.Background()); err != nil {
		return
	}
	concurrency <- struct{}{}
	defer func() { <-concurrency }()

	qs.attempt(ts)
}

// attempt dispatches one delivery attempt and applies the retry policy. The
// HTTP dispatch must run without the queue lock held; the snapshot and apply
// phases each take the lock internally, so attempt() never holds it across the
// dispatch call.
func (qs *queueState) attempt(ts *taskState) {
	snap, ok := qs.snapshotAttempt(ts)
	if !ok {
		return
	}

	dispatchTime := time.Now()
	rpcCode, httpCode, message := qs.eng.dispatch(&snap.queue, snap.task, snap.info)
	att := &Attempt{
		ScheduleTime: snap.scheduled,
		DispatchTime: dispatchTime,
		ResponseTime: time.Now(),
		Code:         rpcCode,
		Message:      message,
	}

	qs.applyAttempt(ts, att, dispatchTime, rpcCode, httpCode, message)
}

// attemptSnapshot is the immutable view of a task+queue captured under the lock
// and handed to the (lock-free) dispatch.
type attemptSnapshot struct {
	task      *Task
	queue     Queue
	info      attemptInfo
	scheduled time.Time
}

// snapshotAttempt copies the data dispatch needs under the lock. ok is false if
// the task was removed before the attempt could start.
func (qs *queueState) snapshotAttempt(ts *taskState) (attemptSnapshot, bool) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if ts.removed {
		return attemptSnapshot{}, false
	}
	t := ts.t
	return attemptSnapshot{
		task:      cloneTask(t),
		queue:     *qs.q,
		scheduled: t.ScheduleTime,
		info: attemptInfo{
			number:         t.DispatchCount + 1,
			executionCount: t.ResponseCount,
			prevHTTPCode:   ts.lastHTTPCode,
			prevReason:     ts.lastRetryReason,
		},
	}, true
}

// applyAttempt records the attempt result under the lock and either completes,
// drops, or reschedules the task per the retry policy.
func (qs *queueState) applyAttempt(ts *taskState, att *Attempt, dispatchTime time.Time, rpcCode int32, httpCode int, message string) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if ts.removed {
		return
	}
	t := ts.t

	// Every attempt is dispatched; response_count only counts attempts that
	// received a response (transport error -> httpCode 0), keeping execution
	// count distinct from retry count.
	t.DispatchCount++
	if httpCode != 0 {
		t.ResponseCount++
	}
	t.LastAttempt = att
	if t.FirstAttempt == nil {
		t.FirstAttempt = &Attempt{ScheduleTime: att.ScheduleTime, DispatchTime: att.DispatchTime}
		ts.firstAttemptTime = dispatchTime
	}
	ts.lastHTTPCode = httpCode

	if rpcCode == 0 { // OK -> done
		qs.removeLocked(ts)
		return
	}

	ts.lastRetryReason = retryReason(httpCode, message)
	if !qs.shouldRetry(t, ts) {
		log.Printf("task %s exhausted retries (code=%d), dropping", t.Name, rpcCode)
		qs.removeLocked(ts)
		return
	}

	t.ScheduleTime = time.Now().Add(qs.backoff(t.DispatchCount))
	qs.schedule(ts)
}

// shouldRetry reports whether a failed task has retries remaining. Retrying
// stops only when both maxAttempts and maxRetryDuration are satisfied; an
// unlimited limit never constrains. mu held.
func (qs *queueState) shouldRetry(t *Task, ts *taskState) bool {
	rc := qs.q.RetryConfig
	attemptsLimited := rc.MaxAttempts != -1
	durationLimited := rc.MaxRetryDuration > 0
	if !attemptsLimited && !durationLimited {
		return true
	}
	if attemptsLimited && t.DispatchCount < rc.MaxAttempts {
		return true
	}
	if durationLimited && time.Since(ts.firstAttemptTime) < rc.MaxRetryDuration {
		return true
	}
	return false
}

// backoff computes the delay before the Nth retry. mu held.
func (qs *queueState) backoff(retries int32) time.Duration {
	rc := qs.q.RetryConfig
	minB, maxB := rc.MinBackoff, rc.MaxBackoff
	maxDoublings := rc.MaxDoublings

	doublings := retries - 1
	if doublings > maxDoublings {
		doublings = maxDoublings
	}
	d := time.Duration(float64(minB) * math.Pow(2, float64(doublings)))
	// After max_doublings the interval grows linearly by minBackoff*2^maxDoublings.
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
	qs.tasks[ts.t.Name] = ts
	ts.ttlTimer = time.AfterFunc(qs.effectiveTaskTTL(), func() { qs.expire(ts) })
	qs.schedule(ts)
}

// expire deletes a task that has exceeded its TTL.
func (qs *queueState) expire(ts *taskState) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if ts.removed {
		return
	}
	qs.removeLocked(ts)
}

// removeLocked stops and deletes a task, reserving its name with a tombstone.
// mu held.
func (qs *queueState) removeLocked(ts *taskState) {
	ts.removed = true
	if ts.timer != nil {
		ts.timer.Stop()
	}
	if ts.ttlTimer != nil {
		ts.ttlTimer.Stop()
	}
	delete(qs.tasks, ts.t.Name)
	qs.tombstoneLocked(ts.t.Name)
}

// tombstoneLocked reserves a task name until the tombstone TTL elapses. mu held.
func (qs *queueState) tombstoneLocked(name string) {
	ttl := qs.effectiveTombstoneTTL()
	if ttl < 0 {
		return
	}
	qs.tombstones[name] = time.Now().Add(ttl)
	time.AfterFunc(ttl, func() {
		qs.mu.Lock()
		defer qs.mu.Unlock()
		if exp, ok := qs.tombstones[name]; ok && !time.Now().Before(exp) {
			delete(qs.tombstones, name)
		}
	})
}

// tombstoned reports whether a task name is currently reserved. mu held.
func (qs *queueState) tombstoned(name string) bool {
	exp, ok := qs.tombstones[name]
	return ok && time.Now().Before(exp)
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
		if ts.ttlTimer != nil {
			ts.ttlTimer.Stop()
		}
	}
	qs.tasks = map[string]*taskState{}
	qs.q.PurgeTime = time.Now()
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
		if ts.ttlTimer != nil {
			ts.ttlTimer.Stop()
		}
	}
	qs.tasks = map[string]*taskState{}
}

func (qs *queueState) nextTaskID() string {
	qs.idSeq++
	return fmt.Sprintf("%d", uint64(time.Now().UnixNano())+qs.idSeq)
}

// applyQueueDefaults fills unset rate/retry fields with Cloud Tasks defaults.
func applyQueueDefaults(q *Queue) {
	rl := &q.RateLimits
	if rl.MaxDispatchesPerSecond == 0 {
		rl.MaxDispatchesPerSecond = 500
	}
	if rl.MaxConcurrentDispatches == 0 {
		rl.MaxConcurrentDispatches = 1000
	}
	// max_burst_size is output-only: derived from the dispatch rate.
	rl.MaxBurstSize = computeBurstSize(rl.MaxDispatchesPerSecond)

	rc := &q.RetryConfig
	if rc.MaxAttempts == 0 {
		rc.MaxAttempts = 100
	}
	if rc.MinBackoff == 0 {
		rc.MinBackoff = 100 * time.Millisecond
	}
	if rc.MaxBackoff == 0 {
		rc.MaxBackoff = 3600 * time.Second
	}
	if rc.MaxDoublings == 0 {
		rc.MaxDoublings = 16
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

func cloneTask(t *Task) *Task {
	c := *t
	if t.Target.Headers != nil {
		c.Target.Headers = cloneHeaders(t.Target.Headers)
	}
	if t.Target.Body != nil {
		c.Target.Body = append([]byte(nil), t.Target.Body...)
	}
	if t.Target.AppEngineRouting != nil {
		r := *t.Target.AppEngineRouting
		c.Target.AppEngineRouting = &r
	}
	if t.Target.Auth != nil {
		a := *t.Target.Auth
		c.Target.Auth = &a
	}
	return &c
}
