package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// methodHandler is the signature of the generated gRPC method handlers held in
// grpc.ServiceDesc. The type there is unexported, but a value of it assigns to
// this identical unnamed signature.
type methodHandler = func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error)

// segment is one path component of a route: either a literal or a single-
// component wildcard.
type segment struct {
	literal  string
	wildcard bool
}

// variable is a request field bound to a run of path segments, e.g. `name`
// bound to `projects/*/locations/*/queues/*`.
type variable struct {
	field      string // proto field path, e.g. "queue.name"
	start, end int    // half-open range of segments it captures
}

// route is one HTTP binding: the method and path template to match, where the
// body goes, and the gRPC handler to invoke.
type route struct {
	httpMethod string
	segments   []segment
	verb       string // custom-method suffix, e.g. "purge" for ...:purge
	vars       []variable
	body       string                       // "", "*", or the name of the field carrying the body
	bodyField  protoreflect.FieldDescriptor // set when body names a field
	input      protoreflect.MessageDescriptor
	impl       any
	handler    methodHandler
}

// routesFor derives every HTTP binding declared by s from the compiled protos.
func routesFor(s Service) ([]*route, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(s.Desc.ServiceName))
	if err != nil {
		return nil, fmt.Errorf("no compiled descriptor for service %s: %w", s.Desc.ServiceName, err)
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a service", s.Desc.ServiceName)
	}

	var routes []*route
	for i := range sd.Methods().Len() {
		md := sd.Methods().Get(i)
		rule, _ := proto.GetExtension(md.Options(), annotations.E_Http).(*annotations.HttpRule)
		handler := handlerFor(s.Desc, string(md.Name()))
		// A method with no binding, or a streaming one with no unary handler,
		// simply has no REST route.
		if rule == nil || handler == nil {
			continue
		}
		for _, binding := range append([]*annotations.HttpRule{rule}, rule.GetAdditionalBindings()...) {
			rt, err := newRoute(binding, md, s.Impl, handler)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", s.Desc.ServiceName, md.Name(), err)
			}
			routes = append(routes, rt)
		}
	}
	if len(routes) == 0 {
		return nil, fmt.Errorf("service %s declares no HTTP bindings", s.Desc.ServiceName)
	}
	return routes, nil
}

// handlerFor finds the generated unary handler for method in desc.
func handlerFor(desc *grpc.ServiceDesc, method string) methodHandler {
	for i := range desc.Methods {
		if desc.Methods[i].MethodName == method {
			return desc.Methods[i].Handler
		}
	}
	return nil
}

// newRoute builds a route from a single HTTP binding, validating up front that
// every field the binding names actually exists on the request message.
func newRoute(rule *annotations.HttpRule, md protoreflect.MethodDescriptor, impl any, handler methodHandler) (*route, error) {
	httpMethod, tmpl, err := patternOf(rule)
	if err != nil {
		return nil, err
	}
	segments, vars, verb, err := parseTemplate(tmpl)
	if err != nil {
		return nil, err
	}

	input := md.Input()
	for _, v := range vars {
		if _, err := resolveField(input, v.field); err != nil {
			return nil, err
		}
	}
	var bodyField protoreflect.FieldDescriptor
	if body := rule.GetBody(); body != "" && body != "*" {
		if strings.Contains(body, ".") {
			return nil, fmt.Errorf("body field %q must be a top-level field", body)
		}
		fd, err := resolveField(input, body)
		if err != nil {
			return nil, err
		}
		if fd.Message() == nil || fd.IsList() || fd.IsMap() {
			return nil, fmt.Errorf("body field %q is not a message", body)
		}
		bodyField = fd
	}

	return &route{
		httpMethod: httpMethod,
		segments:   segments,
		verb:       verb,
		vars:       vars,
		body:       rule.GetBody(),
		bodyField:  bodyField,
		input:      input,
		impl:       impl,
		handler:    handler,
	}, nil
}

// patternOf extracts the HTTP method and path template from a binding.
func patternOf(rule *annotations.HttpRule) (string, string, error) {
	switch p := rule.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		return http.MethodGet, p.Get, nil
	case *annotations.HttpRule_Post:
		return http.MethodPost, p.Post, nil
	case *annotations.HttpRule_Put:
		return http.MethodPut, p.Put, nil
	case *annotations.HttpRule_Patch:
		return http.MethodPatch, p.Patch, nil
	case *annotations.HttpRule_Delete:
		return http.MethodDelete, p.Delete, nil
	default:
		return "", "", fmt.Errorf("unsupported HTTP pattern %T", rule.GetPattern())
	}
}

// parseTemplate turns a path template such as
// `/v2/{name=projects/*/locations/*/queues/*}:purge` into the segments to
// match, the variables to bind, and the custom-method verb.
func parseTemplate(tmpl string) ([]segment, []variable, string, error) {
	if !strings.HasPrefix(tmpl, "/") {
		return nil, nil, "", fmt.Errorf("path template %q must start with /", tmpl)
	}
	// A ':' with no '/', '{' or '}' after it can only be the custom-method
	// suffix, which always sits at the very end of the template.
	verb := ""
	if i := strings.LastIndex(tmpl, ":"); i >= 0 && !strings.ContainsAny(tmpl[i:], "/{}") {
		verb, tmpl = tmpl[i+1:], tmpl[:i]
	}

	var segments []segment
	var vars []variable
	for _, raw := range splitTemplate(tmpl[1:]) {
		if !strings.HasPrefix(raw, "{") {
			segments = append(segments, segment{literal: raw})
			continue
		}
		if !strings.HasSuffix(raw, "}") {
			return nil, nil, "", fmt.Errorf("unterminated variable in path template %q", tmpl)
		}
		field, pattern, ok := strings.Cut(raw[1:len(raw)-1], "=")
		if !ok {
			pattern = "*"
		}
		v := variable{field: field, start: len(segments)}
		for _, sub := range strings.Split(pattern, "/") {
			if sub == "**" {
				return nil, nil, "", fmt.Errorf("multi-segment wildcard in path template %q is not supported", tmpl)
			}
			segments = append(segments, segment{literal: sub, wildcard: sub == "*"})
		}
		v.end = len(segments)
		vars = append(vars, v)
	}
	return segments, vars, verb, nil
}

// splitTemplate splits a template on '/' while leaving the pattern inside a
// {variable=.../...} intact.
func splitTemplate(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '{':
			depth++
		case '}':
			depth--
		case '/':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// splitRequestPath splits a request path into its segments and trailing custom
// -method verb. Segments are unescaped individually, so an escaped slash stays
// inside the segment it was written in.
func splitRequestPath(path string) ([]string, string) {
	path = strings.TrimPrefix(path, "/")
	verb := ""
	if i := strings.LastIndex(path, ":"); i >= 0 && !strings.Contains(path[i:], "/") {
		verb, path = path[i+1:], path[:i]
	}
	segs := strings.Split(path, "/")
	for i, s := range segs {
		if unescaped, err := url.PathUnescape(s); err == nil {
			segs[i] = unescaped
		}
	}
	return segs, verb
}

// match reports whether a request hits this route, and binds its variables.
func (rt *route) match(httpMethod string, segs []string, verb string) (map[string]string, bool) {
	if httpMethod != rt.httpMethod || verb != rt.verb || len(segs) != len(rt.segments) {
		return nil, false
	}
	for i, s := range rt.segments {
		if s.wildcard {
			if segs[i] == "" {
				return nil, false
			}
			continue
		}
		if segs[i] != s.literal {
			return nil, false
		}
	}
	vars := make(map[string]string, len(rt.vars))
	for _, v := range rt.vars {
		vars[v.field] = strings.Join(segs[v.start:v.end], "/")
	}
	return vars, true
}

// invoke calls the gRPC handler for this route. Going through the generated
// handler rather than the implementation directly keeps REST on the same code
// path as gRPC, decoding included.
func (rt *route) invoke(ctx context.Context, req proto.Message) (proto.Message, error) {
	dec := func(target any) error {
		proto.Merge(target.(proto.Message), req)
		return nil
	}
	resp, err := rt.handler(rt.impl, ctx, dec, nil)
	if err != nil {
		return nil, err
	}
	return resp.(proto.Message), nil
}
