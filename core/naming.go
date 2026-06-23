package core

import (
	"fmt"
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Resource names share the same format across the Cloud Tasks v2 and v2beta3
// APIs, so name parsing/validation lives in the version-agnostic core.
var (
	locationNameRe = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)$`)
	queueNameRe    = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)/queues/([^/]+)$`)
	taskNameRe     = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)/queues/([^/]+)/tasks/([^/]+)$`)
	// Task IDs allow letters, numbers, hyphens and underscores, up to 500 chars.
	idRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,500}$`)
	// Queue IDs allow letters, numbers and hyphens only (no underscore), up to
	// 100 chars.
	queueIDRe = regexp.MustCompile(`^[A-Za-z0-9-]{1,100}$`)
)

func parseLocationName(name string) (project, location string, ok bool) {
	m := locationNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

func parseQueueName(name string) (project, location, queue string, ok bool) {
	m := queueNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", "", false
	}
	return m[1], m[2], m[3], true
}

func parseTaskName(name string) (project, location, queue, task string, ok bool) {
	m := taskNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", "", "", false
	}
	return m[1], m[2], m[3], m[4], true
}

// queueParent returns the location resource name a queue belongs to.
func queueParent(queueName string) string {
	p, l, _, _ := parseQueueName(queueName)
	return fmt.Sprintf("projects/%s/locations/%s", p, l)
}

// taskQueueOf returns the parent queue name of a task.
func taskQueueOf(taskName string) string {
	p, l, q, _, _ := parseTaskName(taskName)
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s", p, l, q)
}

// queueID extracts the short queue id from a full queue name.
func queueID(queueName string) string {
	_, _, id, _ := parseQueueName(queueName)
	return id
}

// taskID extracts the short task id from a full task name.
func taskID(taskName string) string {
	_, _, _, id, _ := parseTaskName(taskName)
	return id
}

// resolveTaskName validates a caller-provided task name against the parent
// queue, or generates a fresh one when none was supplied. generateID produces
// the short task id used for auto-named tasks.
func resolveTaskName(parent, provided string, generateID func() string) (string, error) {
	if provided == "" {
		return parent + "/tasks/" + generateID(), nil
	}
	_, _, _, id, ok := parseTaskName(provided)
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "invalid task name %q", provided)
	}
	if !idRe.MatchString(id) {
		return "", status.Errorf(codes.InvalidArgument, "invalid task id %q", id)
	}
	if taskQueueOf(provided) != parent {
		return "", status.Error(codes.InvalidArgument, "task name does not match parent queue")
	}
	return provided, nil
}
