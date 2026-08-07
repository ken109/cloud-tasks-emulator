// Package rest serves the REST/JSON surface of a gRPC API by transcoding
// requests onto the gRPC service implementation. The routing table is not
// written by hand: it is derived at startup from the google.api.http
// annotations the compiled protos already carry, so it cannot drift from the
// proto version the module depends on.
//
// Nothing here knows about Cloud Tasks. Hand it a grpc.ServiceDesc and the
// implementation registered for it, and it serves whatever HTTP bindings that
// service declares.
package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Service pairs a gRPC service description with the implementation registered
// for it on the gRPC server, so REST requests run through exactly the same
// handlers as gRPC ones.
type Service struct {
	Desc *grpc.ServiceDesc
	Impl any
}

// Handler builds an http.Handler serving the HTTP bindings of svcs, and returns
// the bindings it had to skip so the caller can report them instead of letting
// the API quietly shrink. A single binding this transcoder cannot express is
// skipped; a service left with no routes at all is an error, since that is a
// wiring mistake rather than a gap in one proto.
func Handler(svcs ...Service) (http.Handler, []string, error) {
	m := &mux{}
	var skipped []string
	for _, s := range svcs {
		rs, skips, err := routesFor(s)
		if err != nil {
			return nil, nil, err
		}
		m.routes = append(m.routes, rs...)
		skipped = append(skipped, skips...)
	}
	return m, skipped, nil
}

// mux dispatches requests to the first route whose method and path match.
type mux struct {
	routes []*route
}

// maxRequestBody caps how much of a request body is read. This is a local
// development tool, but an unbounded read is still an easy way to lose the
// process to one stray request. A variable so tests can lower it.
var maxRequestBody int64 = 32 << 20

func (m *mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	segs, verb := splitRequestPath(r.URL.EscapedPath())
	enumInts := wantsEnumNumbers(r)
	for _, rt := range m.routes {
		vars, ok := rt.match(r.Method, segs, verb)
		if !ok {
			continue
		}
		req, err := rt.decode(r, vars)
		if err != nil {
			writeError(w, err)
			return
		}
		resp, err := rt.invoke(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeMessage(w, resp, enumInts)
		return
	}
	writeError(w, status.Errorf(codes.NotFound, "no REST binding for %s %s", r.Method, r.URL.Path))
}

// wantsEnumNumbers reports whether the caller asked for numeric enums. The
// generated REST transports send `$alt=json;enum-encoding=int` on every call,
// so honour it rather than always spelling enums out by name.
func wantsEnumNumbers(r *http.Request) bool {
	return strings.Contains(r.URL.Query().Get("$alt"), "enum-encoding=int")
}

// writeMessage renders a response message as the JSON the REST clients expect.
func writeMessage(w http.ResponseWriter, msg proto.Message, enumInts bool) {
	body, err := protojson.MarshalOptions{UseEnumNumbers: enumInts}.Marshal(msg)
	if err != nil {
		writeError(w, status.Errorf(codes.Internal, "marshal response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_, _ = w.Write(body)
}

// errorBody is the error shape the Google REST APIs return, which is what the
// client libraries parse to rebuild the gRPC status.
type errorBody struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

// writeError renders err as an HTTP status plus that error body.
func writeError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	httpCode, name := httpStatus(st.Code())

	var body errorBody
	body.Error.Code = httpCode
	body.Error.Message = st.Message()
	body.Error.Status = name

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(httpCode)
	// The value is a fixed struct of a string and two ints, so encoding cannot
	// fail; a short write to the client is not ours to report.
	_ = json.NewEncoder(w).Encode(body)
}

// statusNames maps each gRPC code to the HTTP status and canonical name the
// REST surface reports for it, per google.rpc.Code.
var statusNames = map[codes.Code]struct {
	http int
	name string
}{
	codes.OK:                 {http.StatusOK, "OK"},
	codes.Canceled:           {499, "CANCELLED"},
	codes.Unknown:            {http.StatusInternalServerError, "UNKNOWN"},
	codes.InvalidArgument:    {http.StatusBadRequest, "INVALID_ARGUMENT"},
	codes.DeadlineExceeded:   {http.StatusGatewayTimeout, "DEADLINE_EXCEEDED"},
	codes.NotFound:           {http.StatusNotFound, "NOT_FOUND"},
	codes.AlreadyExists:      {http.StatusConflict, "ALREADY_EXISTS"},
	codes.PermissionDenied:   {http.StatusForbidden, "PERMISSION_DENIED"},
	codes.ResourceExhausted:  {http.StatusTooManyRequests, "RESOURCE_EXHAUSTED"},
	codes.FailedPrecondition: {http.StatusBadRequest, "FAILED_PRECONDITION"},
	codes.Aborted:            {http.StatusConflict, "ABORTED"},
	codes.OutOfRange:         {http.StatusBadRequest, "OUT_OF_RANGE"},
	codes.Unimplemented:      {http.StatusNotImplemented, "UNIMPLEMENTED"},
	codes.Internal:           {http.StatusInternalServerError, "INTERNAL"},
	codes.Unavailable:        {http.StatusServiceUnavailable, "UNAVAILABLE"},
	codes.DataLoss:           {http.StatusInternalServerError, "DATA_LOSS"},
	codes.Unauthenticated:    {http.StatusUnauthorized, "UNAUTHENTICATED"},
}

// httpStatus maps a gRPC code onto its HTTP status and canonical name.
func httpStatus(c codes.Code) (int, string) {
	if s, ok := statusNames[c]; ok {
		return s.http, s.name
	}
	return http.StatusInternalServerError, fmt.Sprintf("CODE_%d", uint32(c))
}
