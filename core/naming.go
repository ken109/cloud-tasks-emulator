package core

import (
	"fmt"
	"regexp"
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
