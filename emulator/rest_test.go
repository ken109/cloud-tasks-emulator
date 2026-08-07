package emulator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// restServer starts the REST surface of a fresh emulator.
func restServer(t *testing.T) *httptest.Server {
	t.Helper()
	h, err := New(Config{}).RESTHandler()
	if err != nil {
		t.Fatalf("RESTHandler: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// do performs a REST call and decodes the JSON response.
func do(t *testing.T, srv *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: body %q is not JSON: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode, out
}

func TestRESTQueueAndTaskLifecycle(t *testing.T) {
	srv := restServer(t)
	const parent = "projects/p/locations/l"
	const queue = parent + "/queues/q"

	code, got := do(t, srv, http.MethodPost, "/v2/"+parent+"/queues", `{"name":"`+queue+`"}`)
	if code != http.StatusOK || got["name"] != queue {
		t.Fatalf("CreateQueue = %d %v", code, got)
	}

	code, got = do(t, srv, http.MethodGet, "/v2/"+queue, "")
	if code != http.StatusOK || got["name"] != queue {
		t.Fatalf("GetQueue = %d %v", code, got)
	}

	code, got = do(t, srv, http.MethodGet, "/v2/"+parent+"/queues?pageSize=10", "")
	if queues, _ := got["queues"].([]any); code != http.StatusOK || len(queues) != 1 {
		t.Fatalf("ListQueues = %d %v", code, got)
	}

	// Pause first so the task stays put and the rest of the calls are stable.
	if code, got = do(t, srv, http.MethodPost, "/v2/"+queue+":pause", `{}`); code != http.StatusOK || got["state"] != "PAUSED" {
		t.Fatalf("PauseQueue = %d %v", code, got)
	}

	code, got = do(t, srv, http.MethodPost, "/v2/"+queue+"/tasks",
		`{"task":{"httpRequest":{"url":"http://127.0.0.1:1/x"}}}`)
	if code != http.StatusOK {
		t.Fatalf("CreateTask = %d %v", code, got)
	}
	name, _ := got["name"].(string)
	if !strings.HasPrefix(name, queue+"/tasks/") {
		t.Fatalf("task name = %q", name)
	}

	code, got = do(t, srv, http.MethodGet, "/v2/"+queue+"/tasks", "")
	if tasks, _ := got["tasks"].([]any); code != http.StatusOK || len(tasks) != 1 {
		t.Fatalf("ListTasks = %d %v", code, got)
	}

	// responseView is an enum carried in the query string.
	code, got = do(t, srv, http.MethodGet, "/v2/"+name+"?responseView=FULL", "")
	if code != http.StatusOK || got["name"] != name {
		t.Fatalf("GetTask = %d %v", code, got)
	}

	// PATCH with an updateMask query parameter (a FieldMask outside the body).
	code, got = do(t, srv, http.MethodPatch, "/v2/"+queue+"?updateMask=rateLimits",
		`{"rateLimits":{"maxDispatchesPerSecond":3}}`)
	limits, _ := got["rateLimits"].(map[string]any)
	if code != http.StatusOK || limits["maxDispatchesPerSecond"] != float64(3) {
		t.Fatalf("UpdateQueue = %d %v", code, got)
	}

	// The remaining custom methods: `:run` on a task, `:purge` and `:resume`
	// on the queue.
	if code, got = do(t, srv, http.MethodPost, "/v2/"+name+":run", `{}`); code != http.StatusOK {
		t.Fatalf("RunTask = %d %v", code, got)
	}
	if code, got = do(t, srv, http.MethodPost, "/v2/"+queue+":purge", `{}`); code != http.StatusOK {
		t.Fatalf("PurgeQueue = %d %v", code, got)
	}
	if code, got = do(t, srv, http.MethodPost, "/v2/"+queue+":resume", `{}`); code != http.StatusOK || got["state"] != "RUNNING" {
		t.Fatalf("ResumeQueue = %d %v", code, got)
	}

	if code, got = do(t, srv, http.MethodDelete, "/v2/"+queue, ""); code != http.StatusOK {
		t.Fatalf("DeleteQueue = %d %v", code, got)
	}
	if code, got = do(t, srv, http.MethodGet, "/v2/"+queue, ""); code != http.StatusNotFound {
		t.Fatalf("GetQueue after delete = %d %v", code, got)
	}
}

func TestRESTV2beta3(t *testing.T) {
	srv := restServer(t)
	const parent = "projects/p/locations/l"
	const queue = parent + "/queues/beta"

	if code, got := do(t, srv, http.MethodPost, "/v2beta3/"+parent+"/queues", `{"name":"`+queue+`"}`); code != http.StatusOK {
		t.Fatalf("v2beta3 CreateQueue = %d %v", code, got)
	}
	// QueueStats is v2beta3-only, and reaching it proves the beta routes are
	// served by the beta adapter rather than the v2 one.
	code, got := do(t, srv, http.MethodGet, "/v2beta3/"+queue+"?readMask=stats", "")
	if code != http.StatusOK {
		t.Fatalf("v2beta3 GetQueue = %d %v", code, got)
	}
	if _, ok := got["stats"]; !ok {
		t.Fatalf("v2beta3 GetQueue has no stats: %v", got)
	}
}

func TestRESTErrors(t *testing.T) {
	srv := restServer(t)

	code, got := do(t, srv, http.MethodGet, "/v2/projects/p/locations/l/queues/missing", "")
	errObj, _ := got["error"].(map[string]any)
	if code != http.StatusNotFound || errObj["status"] != "NOT_FOUND" || errObj["code"] != float64(404) {
		t.Fatalf("missing queue = %d %v", code, got)
	}

	if code, got = do(t, srv, http.MethodGet, "/v2/nope", ""); code != http.StatusNotFound {
		t.Fatalf("unrouted path = %d %v", code, got)
	}

	code, got = do(t, srv, http.MethodPost, "/v2/projects/p/locations/l/queues", `{"name":`)
	if code != http.StatusBadRequest {
		t.Fatalf("malformed body = %d %v", code, got)
	}

	code, got = do(t, srv, http.MethodGet, "/v2/projects/p/locations/l/queues?pageSize=lots", "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad query value = %d %v", code, got)
	}

	code, got = do(t, srv, http.MethodGet, "/v2/projects/p/locations/l/queues?nosuchfield=1", "")
	if code != http.StatusBadRequest {
		t.Fatalf("unknown query parameter = %d %v", code, got)
	}
}
