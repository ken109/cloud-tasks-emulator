// Package core implements a version-agnostic, in-memory Cloud Tasks engine:
// queue/task storage, scheduling, retries, rate limiting and HTTP dispatch.
// The google.cloud.tasks.v2 and v2beta3 gRPC adapters translate their
// respective protobuf messages to and from these neutral types and delegate
// all behaviour here, so both API versions share one faithful implementation.
package core

import "time"

// State is a queue's running state.
type State int

const (
	StateRunning State = iota
	StatePaused
	StateDisabled
)

// TargetType identifies how a task is delivered.
type TargetType int

const (
	TargetNone TargetType = iota // no target set
	TargetHTTP
	TargetAppEngine
	TargetPull
)

// AuthKind identifies the Authorization token a task requests.
type AuthKind int

const (
	AuthNone AuthKind = iota
	AuthOIDC
	AuthOAuth
)

// Auth describes the Authorization token to attach to an HTTP dispatch.
type Auth struct {
	Kind                AuthKind
	ServiceAccountEmail string
	Audience            string
	Scope               string
}

// AppEngineRouting mirrors the App Engine routing fields.
type AppEngineRouting struct {
	Service  string
	Version  string
	Instance string
	Host     string
}

// Target is the neutral description of a task's delivery target.
type Target struct {
	Type    TargetType
	Method  string // HTTP method name; empty defaults to POST
	Headers map[string]string
	Body    []byte

	// HTTP target.
	URL  string
	Auth *Auth

	// App Engine target.
	RelativeURI      string
	AppEngineRouting *AppEngineRouting

	// Pull target (v2beta3).
	Tag string
}

// RateLimits mirrors Cloud Tasks RateLimits.
type RateLimits struct {
	MaxDispatchesPerSecond  float64
	MaxBurstSize            int32 // output only; derived from the dispatch rate
	MaxConcurrentDispatches int32
}

// RetryConfig mirrors Cloud Tasks RetryConfig.
type RetryConfig struct {
	MaxAttempts      int32 // -1 = unlimited
	MaxRetryDuration time.Duration
	MinBackoff       time.Duration
	MaxBackoff       time.Duration
	MaxDoublings     int32
}

// HTTPOverride is a queue-level HTTP target override (v2beta3 HttpTarget).
type HTTPOverride struct {
	Scheme        string // "http" / "https" / ""
	Host          string
	Port          string
	Path          string
	Query         string
	AlwaysEnforce bool // ALWAYS vs IF_NOT_EXISTS enforce mode
	Method        string
	Headers       map[string]string
	Auth          *Auth
}

// Queue is the neutral queue resource.
type Queue struct {
	Name        string
	State       State
	RateLimits  RateLimits
	RetryConfig RetryConfig
	// Per-queue lifecycle overrides (v2beta3). Zero means "use engine default".
	TaskTTL      time.Duration
	TombstoneTTL time.Duration
	Pull         bool // PULL-type queue: tasks are not auto-dispatched
	// AppEngineHostOverride is the queue-level App Engine routing host override.
	AppEngineHostOverride string
	// HTTPOverride is the queue-level HTTP target override (v2beta3).
	HTTPOverride *HTTPOverride
	PurgeTime    time.Time

	// TasksCount is filled in snapshots for QueueStats (output only).
	TasksCount int64
}

// Attempt mirrors a Cloud Tasks Attempt.
type Attempt struct {
	ScheduleTime time.Time
	DispatchTime time.Time
	ResponseTime time.Time
	Code         int32 // canonical rpc code
	Message      string
}

// Task is the neutral task resource.
type Task struct {
	Name             string
	ScheduleTime     time.Time
	CreateTime       time.Time
	DispatchDeadline time.Duration
	DispatchCount    int32
	ResponseCount    int32
	FirstAttempt     *Attempt
	LastAttempt      *Attempt
	Target           Target
}
