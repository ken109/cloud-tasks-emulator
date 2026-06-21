package emulator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/genproto/googleapis/rpc/code"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

// User-Agent strings Cloud Tasks sends for each target type.
const (
	httpUserAgent      = "Google-Cloud-Tasks"
	appEngineUserAgent = "AppEngine-Google; (+http://code.google.com/appengine)"
)

// attemptInfo carries the per-attempt data needed to populate the Cloud Tasks
// request headers.
type attemptInfo struct {
	number         int32  // 1-based attempt number
	executionCount int32  // prior attempts that received a handler response
	prevHTTPCode   int    // HTTP status of the previous attempt, 0 if none
	prevReason     string // retry reason recorded on the previous attempt
}

// dispatch performs the HTTP delivery for a task and returns an rpc status
// describing the outcome (code OK on a 2xx response) plus the raw HTTP status
// code (0 when no response was received).
func (s *Server) dispatch(queue *taskspb.Queue, task *taskspb.Task, info attemptInfo) (*statuspb.Status, int, error) {
	req, err := s.buildRequest(queue, task, info)
	if err != nil {
		return &statuspb.Status{Code: int32(code.Code_INVALID_ARGUMENT), Message: err.Error()}, 0, err
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
		return &statuspb.Status{Code: int32(code.Code_UNAVAILABLE), Message: err.Error()}, 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &statuspb.Status{Code: int32(code.Code_OK)}, resp.StatusCode, nil
	}
	return &statuspb.Status{
		Code:    int32(httpToRPCCode(resp.StatusCode)),
		Message: fmt.Sprintf("HTTP status %d", resp.StatusCode),
	}, resp.StatusCode, nil
}

// buildRequest constructs the *http.Request for either an HttpRequest or an
// AppEngineHttpRequest target, including the Cloud Tasks system headers.
func (s *Server) buildRequest(queue *taskspb.Queue, task *taskspb.Task, info attemptInfo) (*http.Request, error) {
	switch mt := task.GetMessageType().(type) {
	case *taskspb.Task_HttpRequest:
		return s.buildHTTPRequest(queue, task, mt.HttpRequest, info)
	case *taskspb.Task_AppEngineHttpRequest:
		return s.buildAppEngineRequest(queue, task, mt.AppEngineHttpRequest, info)
	default:
		return nil, fmt.Errorf("task has no target set")
	}
}

func (s *Server) buildHTTPRequest(queue *taskspb.Queue, task *taskspb.Task, r *taskspb.HttpRequest, info attemptInfo) (*http.Request, error) {
	req, err := newRequest(httpMethodName(r.GetHttpMethod()), r.GetUrl(), r.GetBody(), r.GetHeaders())
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", httpUserAgent)
	setSystemHeaders(req, "X-CloudTasks", queue, task, info)

	// Authorization token, if configured on the task.
	switch at := r.GetAuthorizationHeader().(type) {
	case *taskspb.HttpRequest_OidcToken:
		aud := at.OidcToken.GetAudience()
		if aud == "" {
			aud = r.GetUrl()
		}
		req.Header.Set("Authorization", "Bearer "+oidcToken(at.OidcToken.GetServiceAccountEmail(), aud))
	case *taskspb.HttpRequest_OauthToken:
		req.Header.Set("Authorization", "Bearer "+oauthToken(at.OauthToken.GetServiceAccountEmail(), at.OauthToken.GetScope()))
	}
	return req, nil
}

func (s *Server) buildAppEngineRequest(queue *taskspb.Queue, task *taskspb.Task, r *taskspb.AppEngineHttpRequest, info attemptInfo) (*http.Request, error) {
	host := s.appEngineHost(queue, r)
	if host == "" {
		return nil, fmt.Errorf("no App Engine target host configured; set -app-engine-host")
	}
	req, err := newRequest(httpMethodName(r.GetHttpMethod()), host+r.GetRelativeUri(), r.GetBody(), r.GetHeaders())
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", appEngineUserAgent)
	setSystemHeaders(req, "X-AppEngine", queue, task, info)
	req.Header.Set("X-AppEngine-FailFast", "false")
	return req, nil
}

// newRequest builds a request with the caller's headers and a default content
// type for non-empty bodies.
func newRequest(method, url string, body []byte, headers map[string]string) (*http.Request, error) {
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
	return req, nil
}

// setSystemHeaders writes the Cloud Tasks / App Engine system headers using the
// given prefix ("X-CloudTasks" or "X-AppEngine").
func setSystemHeaders(req *http.Request, prefix string, queue *taskspb.Queue, task *taskspb.Task, info attemptInfo) {
	_, _, queueID, _ := parseQueueName(queue.GetName())
	_, _, _, taskID, _ := parseTaskName(task.GetName())
	eta := task.GetScheduleTime().AsTime()

	req.Header.Set(prefix+"-QueueName", queueID)
	req.Header.Set(prefix+"-TaskName", taskID)
	req.Header.Set(prefix+"-TaskRetryCount", strconv.Itoa(int(info.number-1)))
	req.Header.Set(prefix+"-TaskExecutionCount", strconv.Itoa(int(info.executionCount)))
	req.Header.Set(prefix+"-TaskETA", fmt.Sprintf("%d.%06d", eta.Unix(), eta.Nanosecond()/1000))
	if info.prevHTTPCode != 0 {
		req.Header.Set(prefix+"-TaskPreviousResponse", strconv.Itoa(info.prevHTTPCode))
	}
	if info.prevReason != "" {
		req.Header.Set(prefix+"-TaskRetryReason", info.prevReason)
	}
}

// oidcToken builds an (unsigned) OIDC JWT for the given service account and
// audience. The emulator cannot mint Google-signed tokens, so it emits an
// alg=none JWT whose claims handlers can still inspect.
func oidcToken(email, audience string) string {
	header := b64url(`{"alg":"none","typ":"JWT"}`)
	now := time.Now()
	claims, _ := json.Marshal(map[string]any{
		"iss":            "https://accounts.google.com",
		"aud":            audience,
		"email":          email,
		"email_verified": true,
		"sub":            email,
		"iat":            now.Unix(),
		"exp":            now.Add(time.Hour).Unix(),
	})
	return header + "." + b64url(string(claims)) + "."
}

// oauthToken returns a placeholder OAuth2 access token for the service account.
func oauthToken(email, scope string) string {
	return "emulator-oauth-token/" + email + "/" + scope
}

func b64url(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
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
