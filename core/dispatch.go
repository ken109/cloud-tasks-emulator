package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"google.golang.org/genproto/googleapis/rpc/code"
)

// User-Agent strings Cloud Tasks sends for each target type.
const (
	httpUserAgent      = "Google-Cloud-Tasks"
	appEngineUserAgent = "AppEngine-Google; (+http://code.google.com/appengine)"
)

// attemptInfo carries per-attempt data needed for the request headers.
type attemptInfo struct {
	number         int32
	executionCount int32
	prevHTTPCode   int
	prevReason     string
}

// dispatch performs one HTTP delivery and returns the canonical rpc code, the
// raw HTTP status (0 when no response was received), and the failure message.
func (e *Engine) dispatch(q *Queue, t *Task, info attemptInfo) (rpcCode int32, httpCode int, message string) {
	req, err := e.buildRequest(q, t, info)
	if err != nil {
		return int32(code.Code_INVALID_ARGUMENT), 0, err.Error()
	}

	deadline := 10 * time.Minute
	if t.DispatchDeadline > 0 {
		deadline = t.DispatchDeadline
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return int32(code.Code_UNAVAILABLE), 0, err.Error()
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return int32(code.Code_OK), resp.StatusCode, ""
	}
	return int32(httpToRPCCode(resp.StatusCode)), resp.StatusCode, fmt.Sprintf("HTTP status %d", resp.StatusCode)
}

func (e *Engine) buildRequest(q *Queue, t *Task, info attemptInfo) (*http.Request, error) {
	switch t.Target.Type {
	case TargetHTTP:
		return e.buildHTTPRequest(q, t, info)
	case TargetAppEngine:
		return e.buildAppEngineRequest(q, t, info)
	default:
		return nil, fmt.Errorf("task target cannot be dispatched over HTTP")
	}
}

func (e *Engine) buildHTTPRequest(q *Queue, t *Task, info attemptInfo) (*http.Request, error) {
	tg := t.Target
	method := tg.Method
	rawURL := tg.URL
	headers := cloneHeaders(tg.Headers)
	auth := tg.Auth

	// Apply the queue-level HTTP target override (v2beta3), if any.
	if ov := q.HTTPOverride; ov != nil {
		rawURL = applyURIOverride(rawURL, ov)
		if ov.Method != "" && ov.AlwaysEnforce {
			method = ov.Method
		}
		for k, v := range ov.Headers {
			if headers == nil {
				headers = map[string]string{}
			}
			if ov.AlwaysEnforce || headers[k] == "" {
				headers[k] = v
			}
		}
		if ov.Auth != nil && (auth == nil || ov.AlwaysEnforce) {
			auth = ov.Auth
		}
	}

	req, err := newRequest(methodOrPost(method), rawURL, tg.Body, headers)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", httpUserAgent)
	setSystemHeaders(req, "X-CloudTasks", q, t, info)
	if auth != nil {
		setAuthHeader(req, auth, rawURL)
	}
	return req, nil
}

func (e *Engine) buildAppEngineRequest(q *Queue, t *Task, info attemptInfo) (*http.Request, error) {
	host := e.appEngineHost(q, t)
	if host == "" {
		return nil, fmt.Errorf("no App Engine target host configured; set the default App Engine host")
	}
	req, err := newRequest(methodOrPost(t.Target.Method), host+t.Target.RelativeURI, t.Target.Body, cloneHeaders(t.Target.Headers))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", appEngineUserAgent)
	setSystemHeaders(req, "X-AppEngine", q, t, info)
	req.Header.Set("X-AppEngine-FailFast", "false")
	return req, nil
}

func (e *Engine) appEngineHost(q *Queue, t *Task) string {
	if r := t.Target.AppEngineRouting; r != nil && r.Host != "" {
		return r.Host
	}
	if o := q.AppEngineRoutingOverride; o != nil && o.Host != "" {
		return o.Host
	}
	return e.defaultAppEngineHost
}

// applyURIOverride applies a v2beta3 UriOverride to a task URL.
func applyURIOverride(raw string, ov *HTTPOverride) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	set := func(cur, override string) string {
		if override == "" {
			return cur
		}
		if ov.AlwaysEnforce || cur == "" {
			return override
		}
		return cur
	}
	if ov.Scheme != "" && (ov.AlwaysEnforce || u.Scheme == "") {
		u.Scheme = ov.Scheme
	}
	host := u.Hostname()
	port := u.Port()
	host = set(host, ov.Host)
	port = set(port, ov.Port)
	if port != "" {
		u.Host = host + ":" + port
	} else {
		u.Host = host
	}
	if ov.Path != "" && (ov.AlwaysEnforce || u.Path == "" || u.Path == "/") {
		u.Path = ov.Path
	}
	if ov.Query != "" && (ov.AlwaysEnforce || u.RawQuery == "") {
		u.RawQuery = ov.Query
	}
	return u.String()
}

func newRequest(method, rawURL string, body []byte, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequest(method, rawURL, bytes.NewReader(body))
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

func setSystemHeaders(req *http.Request, prefix string, q *Queue, t *Task, info attemptInfo) {
	eta := t.ScheduleTime
	req.Header.Set(prefix+"-QueueName", queueID(q.Name))
	req.Header.Set(prefix+"-TaskName", taskID(t.Name))
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

func setAuthHeader(req *http.Request, auth *Auth, targetURL string) {
	switch auth.Kind {
	case AuthOIDC:
		aud := auth.Audience
		if aud == "" {
			aud = targetURL
		}
		req.Header.Set("Authorization", "Bearer "+oidcToken(auth.ServiceAccountEmail, aud))
	case AuthOAuth:
		req.Header.Set("Authorization", "Bearer "+oauthToken(auth.ServiceAccountEmail, auth.Scope))
	}
}

// oidcToken builds an unsigned (alg=none) OIDC JWT for the service account.
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

func oauthToken(email, scope string) string {
	return "emulator-oauth-token/" + email + "/" + scope
}

func b64url(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

func methodOrPost(m string) string {
	if m == "" {
		return http.MethodPost
	}
	return m
}

func cloneHeaders(h map[string]string) map[string]string {
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
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

// retryReason summarises why an attempt failed, for the retry headers.
func retryReason(httpCode int, message string) string {
	if httpCode != 0 {
		return fmt.Sprintf("RETURNED_%d", httpCode)
	}
	if message != "" {
		return "CONNECTION_ERROR: " + message
	}
	return "CONNECTION_ERROR"
}
