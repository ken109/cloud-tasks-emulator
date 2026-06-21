package emulator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/genproto/googleapis/rpc/code"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

// dispatch performs the HTTP delivery for a task and returns an rpc status
// describing the outcome (code OK on a 2xx response).
func (s *Server) dispatch(queue *taskspb.Queue, task *taskspb.Task, attemptNum int32) (*statuspb.Status, error) {
	req, err := s.buildRequest(queue, task, attemptNum)
	if err != nil {
		return &statuspb.Status{Code: int32(code.Code_INVALID_ARGUMENT), Message: err.Error()}, err
	}

	deadline := 10 * time.Minute
	if dd := task.GetDispatchDeadline().AsDuration(); dd > 0 {
		deadline = dd
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return &statuspb.Status{Code: int32(code.Code_UNAVAILABLE), Message: err.Error()}, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &statuspb.Status{Code: int32(code.Code_OK)}, nil
	}
	return &statuspb.Status{
		Code:    int32(httpToRPCCode(resp.StatusCode)),
		Message: fmt.Sprintf("HTTP status %d", resp.StatusCode),
	}, nil
}

// buildRequest constructs the *http.Request for either an HttpRequest or an
// AppEngineHttpRequest target, including the Cloud Tasks system headers.
func (s *Server) buildRequest(queue *taskspb.Queue, task *taskspb.Task, attemptNum int32) (*http.Request, error) {
	var (
		method  string
		url     string
		body    []byte
		headers map[string]string
	)

	switch mt := task.GetMessageType().(type) {
	case *taskspb.Task_HttpRequest:
		r := mt.HttpRequest
		method = httpMethodName(r.GetHttpMethod())
		url = r.GetUrl()
		body = r.GetBody()
		headers = r.GetHeaders()
	case *taskspb.Task_AppEngineHttpRequest:
		r := mt.AppEngineHttpRequest
		method = httpMethodName(r.GetHttpMethod())
		host := s.appEngineHost(queue, r)
		if host == "" {
			return nil, fmt.Errorf("no App Engine target host configured; set -app-engine-host")
		}
		url = host + r.GetRelativeUri()
		body = r.GetBody()
		headers = r.GetHeaders()
	default:
		return nil, fmt.Errorf("task has no target set")
	}

	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if _, ok := headers["Content-Type"]; !ok && len(body) > 0 {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	// Cloud Tasks system headers.
	_, _, queueID, _ := parseQueueName(queue.GetName())
	_, _, _, taskID, _ := parseTaskName(task.GetName())
	req.Header.Set("X-CloudTasks-QueueName", queueID)
	req.Header.Set("X-CloudTasks-TaskName", taskID)
	req.Header.Set("X-CloudTasks-TaskRetryCount", strconv.Itoa(int(attemptNum-1)))
	req.Header.Set("X-CloudTasks-TaskExecutionCount", strconv.Itoa(int(task.GetDispatchCount())))
	req.Header.Set("X-CloudTasks-TaskETA", strconv.FormatInt(task.GetScheduleTime().GetSeconds(), 10))

	return req, nil
}

// appEngineHost resolves the base URL for an App Engine target.
func (s *Server) appEngineHost(queue *taskspb.Queue, r *taskspb.AppEngineHttpRequest) string {
	if rt := r.GetAppEngineRouting(); rt.GetHost() != "" {
		return rt.GetHost()
	}
	if ov := queue.GetAppEngineRoutingOverride(); ov.GetHost() != "" {
		return ov.GetHost()
	}
	return s.defaultAppEngineHost
}

func httpMethodName(m taskspb.HttpMethod) string {
	switch m {
	case taskspb.HttpMethod_GET:
		return http.MethodGet
	case taskspb.HttpMethod_HEAD:
		return http.MethodHead
	case taskspb.HttpMethod_PUT:
		return http.MethodPut
	case taskspb.HttpMethod_DELETE:
		return http.MethodDelete
	case taskspb.HttpMethod_PATCH:
		return http.MethodPatch
	case taskspb.HttpMethod_OPTIONS:
		return http.MethodOptions
	case taskspb.HttpMethod_POST:
		return http.MethodPost
	default:
		return http.MethodPost
	}
}

// httpToRPCCode maps an HTTP status code to the closest canonical rpc code.
func httpToRPCCode(httpStatus int) code.Code {
	switch {
	case httpStatus == http.StatusBadRequest:
		return code.Code_INVALID_ARGUMENT
	case httpStatus == http.StatusUnauthorized:
		return code.Code_UNAUTHENTICATED
	case httpStatus == http.StatusForbidden:
		return code.Code_PERMISSION_DENIED
	case httpStatus == http.StatusNotFound:
		return code.Code_NOT_FOUND
	case httpStatus == http.StatusConflict:
		return code.Code_ALREADY_EXISTS
	case httpStatus == http.StatusTooManyRequests:
		return code.Code_RESOURCE_EXHAUSTED
	case httpStatus == http.StatusRequestTimeout, httpStatus == http.StatusGatewayTimeout:
		return code.Code_DEADLINE_EXCEEDED
	case httpStatus == http.StatusServiceUnavailable:
		return code.Code_UNAVAILABLE
	case httpStatus == http.StatusNotImplemented:
		return code.Code_UNIMPLEMENTED
	case httpStatus >= 500:
		return code.Code_INTERNAL
	default:
		return code.Code_UNKNOWN
	}
}
