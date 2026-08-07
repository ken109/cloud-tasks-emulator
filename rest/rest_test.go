package rest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// stub implements just enough of the Cloud Tasks service to exercise every
// shape of binding: a GET with a path variable, a POST whose body is one
// field, a PATCH with a nested path variable, a DELETE returning Empty, and a
// custom method. Everything else falls through to Unimplemented, which is
// itself a useful error path.
type stub struct {
	taskspb.UnimplementedCloudTasksServer
	last  proto.Message
	err   error
	queue *taskspb.Queue
}

func (s *stub) reply(req proto.Message, name string) (*taskspb.Queue, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	if s.queue != nil {
		return s.queue, nil
	}
	return &taskspb.Queue{Name: name}, nil
}

func (s *stub) GetQueue(_ context.Context, r *taskspb.GetQueueRequest) (*taskspb.Queue, error) {
	return s.reply(r, r.GetName())
}

func (s *stub) CreateQueue(_ context.Context, r *taskspb.CreateQueueRequest) (*taskspb.Queue, error) {
	return s.reply(r, r.GetQueue().GetName())
}

func (s *stub) UpdateQueue(_ context.Context, r *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	return s.reply(r, r.GetQueue().GetName())
}

func (s *stub) PurgeQueue(_ context.Context, r *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	return s.reply(r, r.GetName())
}

func (s *stub) ListQueues(_ context.Context, r *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	s.last = r
	return &taskspb.ListQueuesResponse{NextPageToken: "next"}, nil
}

func (s *stub) DeleteQueue(_ context.Context, r *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	s.last = r
	return &emptypb.Empty{}, nil
}

// serveStub builds the REST handler over the stub and returns a caller.
func serveStub(t *testing.T) (*stub, func(method, path, body string) (int, string)) {
	t.Helper()
	s := &stub{}
	h, err := Handler(Service{Desc: &taskspb.CloudTasks_ServiceDesc, Impl: s})
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	return s, func(method, path, body string) (int, string) {
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, path, rdr))
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
}

const queueName = "projects/p/locations/l/queues/q"

func TestServeBindings(t *testing.T) {
	s, call := serveStub(t)

	// GET with a multi-segment path variable.
	code, body := call(http.MethodGet, "/v2/"+queueName, "")
	if code != http.StatusOK || !strings.Contains(body, queueName) {
		t.Fatalf("GetQueue = %d %s", code, body)
	}
	if got := s.last.(*taskspb.GetQueueRequest).GetName(); got != queueName {
		t.Errorf("name bound from path = %q", got)
	}

	// POST whose body is a single field, with the parent from the path.
	code, _ = call(http.MethodPost, "/v2/projects/p/locations/l/queues", `{"name":"`+queueName+`"}`)
	create := s.last.(*taskspb.CreateQueueRequest)
	if code != http.StatusOK || create.GetParent() != "projects/p/locations/l" || create.GetQueue().GetName() != queueName {
		t.Fatalf("CreateQueue = %d %v", code, create)
	}

	// PATCH: nested path variable (queue.name) plus a FieldMask in the query.
	code, _ = call(http.MethodPatch, "/v2/"+queueName+"?updateMask=rateLimits,retryConfig", `{"rateLimits":{"maxDispatchesPerSecond":3}}`)
	update := s.last.(*taskspb.UpdateQueueRequest)
	if code != http.StatusOK || update.GetQueue().GetName() != queueName {
		t.Fatalf("UpdateQueue = %d %v", code, update)
	}
	if paths := update.GetUpdateMask().GetPaths(); len(paths) != 2 || paths[0] != "rate_limits" {
		t.Errorf("update mask = %v", paths)
	}
	if update.GetQueue().GetRateLimits().GetMaxDispatchesPerSecond() != 3 {
		t.Errorf("body field lost: %v", update.GetQueue())
	}

	// A custom method (:purge), whose body is the whole message.
	if code, _ = call(http.MethodPost, "/v2/"+queueName+":purge", `{}`); code != http.StatusOK {
		t.Fatalf("PurgeQueue = %d", code)
	}

	// DELETE returning Empty renders as an empty JSON object.
	if code, body = call(http.MethodDelete, "/v2/"+queueName, ""); code != http.StatusOK || body != "{}" {
		t.Fatalf("DeleteQueue = %d %q", code, body)
	}

	// Scalar query parameters, in both spellings.
	call(http.MethodGet, "/v2/projects/p/locations/l/queues?pageSize=7&page_token=tok", "")
	list := s.last.(*taskspb.ListQueuesRequest)
	if list.GetPageSize() != 7 || list.GetPageToken() != "tok" {
		t.Errorf("list query = %v", list)
	}
}

func TestServeEnumEncoding(t *testing.T) {
	s, call := serveStub(t)
	s.queue = &taskspb.Queue{Name: queueName, State: taskspb.Queue_PAUSED}

	if _, body := call(http.MethodGet, "/v2/"+queueName, ""); !strings.Contains(body, `"state":"PAUSED"`) {
		t.Errorf("enum by name: %s", body)
	}
	// The generated REST transports ask for numeric enums on every call, with
	// the separator percent-encoded exactly like this.
	if _, body := call(http.MethodGet, "/v2/"+queueName+"?%24alt=json%3Benum-encoding%3Dint", ""); !strings.Contains(body, `"state":2`) {
		t.Errorf("enum by number: %s", body)
	}
}

func TestServeErrors(t *testing.T) {
	s, call := serveStub(t)

	// An error from the service becomes the matching HTTP status and body.
	s.err = status.Error(codes.NotFound, "no such queue")
	code, body := call(http.MethodGet, "/v2/"+queueName, "")
	var got errorBody
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("error body %q: %v", body, err)
	}
	if code != http.StatusNotFound || got.Error.Status != "NOT_FOUND" || got.Error.Message != "no such queue" {
		t.Fatalf("service error = %d %s", code, body)
	}
	s.err = nil

	// A method the service does not implement still answers as a status.
	if code, _ = call(http.MethodPost, "/v2/"+queueName+":pause", `{}`); code != http.StatusNotImplemented {
		t.Errorf("unimplemented = %d", code)
	}

	for _, tc := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"no binding", http.MethodGet, "/v2/nope", "", http.StatusNotFound},
		{"wrong verb", http.MethodPut, "/v2/" + queueName, "", http.StatusNotFound},
		{"custom method mismatch", http.MethodPost, "/v2/" + queueName + ":nosuch", `{}`, http.StatusNotFound},
		{"malformed JSON", http.MethodPost, "/v2/projects/p/locations/l/queues", `{`, http.StatusBadRequest},
		{"unknown body field", http.MethodPost, "/v2/projects/p/locations/l/queues", `{"nope":1}`, http.StatusBadRequest},
		{"unknown query parameter", http.MethodGet, "/v2/projects/p/locations/l/queues?nope=1", "", http.StatusBadRequest},
		{"unparsable query value", http.MethodGet, "/v2/projects/p/locations/l/queues?pageSize=many", "", http.StatusBadRequest},
	} {
		if code, body := call(tc.method, tc.path, tc.body); code != tc.want {
			t.Errorf("%s = %d %s, want %d", tc.name, code, body, tc.want)
		}
	}

	// An empty body where one is allowed leaves the message untouched.
	if code, _ := call(http.MethodPost, "/v2/projects/p/locations/l/queues", ""); code != http.StatusOK {
		t.Errorf("empty body = %d", code)
	}
}

// TestServeSystemParameters covers the query parameters every Google API
// accepts and no request message declares.
func TestServeSystemParameters(t *testing.T) {
	_, call := serveStub(t)
	if code, body := call(http.MethodGet, "/v2/"+queueName+"?$alt=json&prettyPrint=true&fields=name", ""); code != http.StatusOK {
		t.Errorf("system parameters = %d %s", code, body)
	}
}

// TestServeUnmarshalableResponse covers the response-marshalling failure path:
// protojson refuses a string field holding invalid UTF-8.
func TestServeUnmarshalableResponse(t *testing.T) {
	s, call := serveStub(t)
	s.queue = &taskspb.Queue{Name: "\xff"}
	if code, body := call(http.MethodGet, "/v2/"+queueName, ""); code != http.StatusInternalServerError {
		t.Fatalf("unmarshalable response = %d %s", code, body)
	}
}

// unregisteredMessage builds a message descriptor with no generated Go type,
// which is the one way newMessage can fail.
func unregisteredMessage(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("rest_test_only.proto"),
		Package:     proto.String("rest.testonly"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Ghost")}},
	}, nil)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func TestNewMessageWithoutGoType(t *testing.T) {
	if _, err := newMessage(unregisteredMessage(t)); status.Code(err) != codes.Internal {
		t.Fatalf("newMessage error = %v", err)
	}
	// The same failure reaching a request: decode reports it rather than panicking.
	rt := &route{input: unregisteredMessage(t)}
	if _, err := rt.decode(httptest.NewRequest(http.MethodGet, "/", nil), nil); err == nil {
		t.Fatal("decode accepted a message with no Go type")
	}
}

// errReader fails on read, standing in for a client that drops the connection
// halfway through the body.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestDecodeBodyReadError(t *testing.T) {
	rt := &route{body: "*", input: (&taskspb.CreateQueueRequest{}).ProtoReflect().Descriptor()}
	req := httptest.NewRequest(http.MethodPost, "/", errReader{})
	if _, err := rt.decode(req, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("decode = %v", err)
	}
}

func TestDecodePathVariableError(t *testing.T) {
	rt := &route{input: (&taskspb.CreateQueueRequest{}).ProtoReflect().Descriptor()}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, err := rt.decode(req, map[string]string{"nosuch": "x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("decode = %v", err)
	}
}

func TestSetField(t *testing.T) {
	md := (&taskspb.UpdateQueueRequest{}).ProtoReflect().Descriptor()

	// A repeated field collects every spelling of the parameter.
	perms := &iampb.TestIamPermissionsRequest{}
	if err := setField(perms.ProtoReflect(), "permissions", []string{"a", "b"}); err != nil {
		t.Fatalf("repeated: %v", err)
	}
	if got := perms.GetPermissions(); len(got) != 2 || got[1] != "b" {
		t.Errorf("repeated field = %v", got)
	}

	for _, tc := range []struct {
		name, path string
		values     []string
		wantErr    bool
	}{
		{name: "nested", path: "queue.name", values: []string{"q"}},
		{name: "field mask", path: "updateMask", values: []string{"name,rateLimits"}},
		{name: "unknown leaf", path: "nope", values: []string{"x"}, wantErr: true},
		{name: "unknown parent", path: "nope.name", values: []string{"x"}, wantErr: true},
		{name: "parent is scalar", path: "updateMask.paths.x", values: []string{"y"}, wantErr: true},
	} {
		msg, err := newMessage(md)
		if err != nil {
			t.Fatal(err)
		}
		err = setField(msg.ProtoReflect(), tc.path, tc.values)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}

// TestSetMapField covers the one field shape a URL cannot express.
func TestSetMapField(t *testing.T) {
	req := &taskspb.CreateTaskRequest{}
	err := setField(req.ProtoReflect(), "task.httpRequest.headers", []string{"x"})
	if err == nil || !strings.Contains(err.Error(), "map field") {
		t.Fatalf("map field error = %v", err)
	}
}

func TestResolveField(t *testing.T) {
	md := (&taskspb.UpdateQueueRequest{}).ProtoReflect().Descriptor()
	if _, err := resolveField(md, "queue.name"); err != nil {
		t.Errorf("queue.name: %v", err)
	}
	if _, err := resolveField(md, "queue.nope"); err == nil {
		t.Error("accepted an unknown leaf")
	}
	if _, err := resolveField(md, "nope.name"); err == nil {
		t.Error("accepted an unknown parent")
	}
}

func TestJSONLiteral(t *testing.T) {
	queue := (&taskspb.Queue{}).ProtoReflect().Descriptor()
	fields := queue.Fields()
	cases := map[string]struct{ field, value, want string }{
		"string": {"name", "q", `"q"`},
		"enum":   {"state", "PAUSED", `"PAUSED"`},
		"enum n": {"state", "2", "2"},
	}
	for name, tc := range cases {
		fd := fields.ByJSONName(tc.field)
		if got := jsonLiteral(fd, tc.value); got != tc.want {
			t.Errorf("%s: jsonLiteral = %s, want %s", name, got, tc.want)
		}
	}
	// A numeric field is rendered bare so protojson parses it as a number.
	limits := (&taskspb.RateLimits{}).ProtoReflect().Descriptor()
	if got := jsonLiteral(limits.Fields().ByJSONName("maxDispatchesPerSecond"), "1.5"); got != "1.5" {
		t.Errorf("double literal = %s", got)
	}
	if got := jsonLiteral(limits.Fields().ByJSONName("maxBurstSize"), "3"); got != "3" {
		t.Errorf("int literal = %s", got)
	}
}

func TestHTTPStatusUnknownCode(t *testing.T) {
	if code, name := httpStatus(codes.Code(99)); code != http.StatusInternalServerError || name != "CODE_99" {
		t.Errorf("httpStatus(99) = %d %s", code, name)
	}
}

func TestParseTemplate(t *testing.T) {
	segs, vars, verb, err := parseTemplate("/v2/{name=projects/*/locations/*}/tasks:run")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if verb != "run" || len(segs) != 6 || len(vars) != 1 {
		t.Fatalf("segments=%d vars=%d verb=%q", len(segs), len(vars), verb)
	}
	if vars[0].field != "name" || vars[0].start != 1 || vars[0].end != 5 {
		t.Errorf("var = %+v", vars[0])
	}

	// A variable with no pattern captures exactly one segment.
	_, vars, _, err = parseTemplate("/v1/{id}")
	if err != nil || len(vars) != 1 || vars[0].end-vars[0].start != 1 {
		t.Fatalf("bare variable: %v %+v", err, vars)
	}

	for _, tmpl := range []string{
		"v2/queues",              // no leading slash
		"/v2/{name=projects/*",   // unterminated variable
		"/v2/{name=projects/**}", // multi-segment wildcard
	} {
		if _, _, _, err := parseTemplate(tmpl); err == nil {
			t.Errorf("parseTemplate(%q) accepted", tmpl)
		}
	}
}

func TestSplitRequestPath(t *testing.T) {
	segs, verb := splitRequestPath("/v2/projects/p/queues/q%2Fx:purge")
	if verb != "purge" || len(segs) != 5 || segs[4] != "q/x" {
		t.Fatalf("segs=%v verb=%q", segs, verb)
	}
	// A broken escape is left as written rather than failing the request.
	if segs, _ = splitRequestPath("/v2/%zz"); segs[1] != "%zz" {
		t.Errorf("segs = %v", segs)
	}
}

func TestMatch(t *testing.T) {
	segs, vars, verb, err := parseTemplate("/v2/{name=projects/*}:run")
	if err != nil {
		t.Fatal(err)
	}
	rt := &route{httpMethod: http.MethodPost, segments: segs, vars: vars, verb: verb}

	if got, ok := rt.match(http.MethodPost, []string{"v2", "projects", "p"}, "run"); !ok || got["name"] != "projects/p" {
		t.Fatalf("match = %v %v", got, ok)
	}
	for _, tc := range []struct {
		name, method string
		segs         []string
		verb         string
	}{
		{"method", http.MethodGet, []string{"v2", "projects", "p"}, "run"},
		{"verb", http.MethodPost, []string{"v2", "projects", "p"}, "purge"},
		{"length", http.MethodPost, []string{"v2", "projects"}, "run"},
		{"literal", http.MethodPost, []string{"v3", "projects", "p"}, "run"},
		{"empty wildcard", http.MethodPost, []string{"v2", "projects", ""}, "run"},
	} {
		if _, ok := rt.match(tc.method, tc.segs, tc.verb); ok {
			t.Errorf("%s: matched when it should not", tc.name)
		}
	}
}

func TestNewRouteValidation(t *testing.T) {
	md := (&taskspb.UpdateQueueRequest{}).ProtoReflect().Descriptor().ParentFile().
		Services().Get(0).Methods().ByName("UpdateQueue")
	handler := handlerFor(&taskspb.CloudTasks_ServiceDesc, "UpdateQueue")

	for _, tc := range []struct {
		name string
		rule *annotations.HttpRule
	}{
		{"unsupported pattern", &annotations.HttpRule{Pattern: &annotations.HttpRule_Custom{Custom: &annotations.CustomHttpPattern{Kind: "x", Path: "/v2"}}}},
		{"bad template", &annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "v2"}}},
		{"unknown path field", &annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v2/{nope=*}"}}},
		{"nested body", &annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v2"}, Body: "queue.name"}},
		{"unknown body field", &annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v2"}, Body: "nope"}},
	} {
		if _, err := newRoute(tc.rule, md, &stub{}, handler); err == nil {
			t.Errorf("%s: newRoute accepted the binding", tc.name)
		}
	}

	// A body naming a scalar field has nowhere to put the JSON document.
	createTask := taskspb.File_google_cloud_tasks_v2_cloudtasks_proto.Services().Get(0).Methods().ByName("CreateTask")
	scalarBody := &annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v2"}, Body: "parent"}
	if _, err := newRoute(scalarBody, createTask, &stub{}, handler); err == nil {
		t.Error("newRoute accepted a scalar body field")
	}

	// PUT has no binding in this API, but the transcoder supports it.
	put := &annotations.HttpRule{Pattern: &annotations.HttpRule_Put{Put: "/v2/{queue.name=projects/*}"}}
	rt, err := newRoute(put, md, &stub{}, handler)
	if err != nil || rt.httpMethod != http.MethodPut {
		t.Errorf("PUT binding = %+v, %v", rt, err)
	}
}

func TestHandlerRejectsServicesWithoutBindings(t *testing.T) {
	// A service with no compiled descriptor at all.
	if _, err := Handler(Service{Desc: &grpc.ServiceDesc{ServiceName: "no.such.Service"}}); err == nil {
		t.Error("accepted an unknown service")
	}
	// A name that resolves to something that is not a service.
	if _, err := Handler(Service{Desc: &grpc.ServiceDesc{ServiceName: "google.cloud.tasks.v2.Queue"}}); err == nil {
		t.Error("accepted a message as a service")
	}
	// A real service whose protos declare no HTTP bindings.
	if _, err := Handler(Service{Desc: &reflectionpb.ServerReflection_ServiceDesc}); err == nil {
		t.Error("accepted a service with no bindings")
	}
	// A binding this transcoder cannot serve fails the build rather than the
	// request: google.iam.v1.IAMPolicy binds `{resource=**}`.
	if _, err := Handler(Service{Desc: &iampb.IAMPolicy_ServiceDesc}); err == nil {
		t.Error("accepted a multi-segment wildcard binding")
	}
}
