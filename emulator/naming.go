package emulator

import (
	"fmt"
	"regexp"
)

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

// parseLocationName validates a "projects/{p}/locations/{l}" resource name.
func parseLocationName(name string) (project, location string, ok bool) {
	m := locationNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// parseQueueName validates a fully-qualified queue resource name.
func parseQueueName(name string) (project, location, queue string, ok bool) {
	m := queueNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", "", false
	}
	return m[1], m[2], m[3], true
}

// parseTaskName validates a fully-qualified task resource name.
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
