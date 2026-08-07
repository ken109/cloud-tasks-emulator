package rest

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// decode builds the request message for this route from the JSON body, the
// query string and the variables bound out of the path, in that order: a value
// written into the path is the authoritative one.
func (rt *route) decode(r *http.Request, vars map[string]string) (proto.Message, error) {
	msg, err := newMessage(rt.input)
	if err != nil {
		return nil, err
	}

	if rt.body != "" {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "read request body: %v", err)
		}
		if len(bytes.TrimSpace(raw)) > 0 {
			target := msg
			if rt.bodyField != nil {
				target = msg.ProtoReflect().Mutable(rt.bodyField).Message().Interface()
			}
			if err := protojson.Unmarshal(raw, target); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid JSON body: %v", err)
			}
		}
	}

	for key, values := range r.URL.Query() {
		if isSystemParam(key) {
			continue
		}
		if err := setField(msg.ProtoReflect(), key, values); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "query parameter %q: %v", key, err)
		}
	}

	for field, value := range vars {
		if err := setField(msg.ProtoReflect(), field, []string{value}); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "path variable %q: %v", field, err)
		}
	}
	return msg, nil
}

// newMessage allocates the generated Go type for a message descriptor. The
// generated packages are linked in, so this is the concrete request type the
// gRPC handler will decode into.
func newMessage(md protoreflect.MessageDescriptor) (proto.Message, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "no Go type registered for %s: %v", md.FullName(), err)
	}
	return mt.New().Interface(), nil
}

// systemParams are the query parameters every Google API accepts regardless of
// the method. They are not request fields, so they are ignored rather than
// rejected — the generated REST transports send `$alt` on every call.
var systemParams = map[string]bool{
	"alt": true, "access_token": true, "bearer_token": true, "callback": true,
	"fields": true, "key": true, "oauth_token": true, "pp": true,
	"prettyPrint": true, "quotaUser": true, "upload_protocol": true,
	"uploadType": true, "userIp": true,
}

func isSystemParam(key string) bool {
	return strings.HasPrefix(key, "$") || systemParams[key]
}

// resolveField looks up a dotted field path such as "queue.name" on md.
func resolveField(md protoreflect.MessageDescriptor, path string) (protoreflect.FieldDescriptor, error) {
	parts := strings.Split(path, ".")
	last := len(parts) - 1
	for _, name := range parts[:last] {
		fd := lookupField(md, name)
		if fd == nil || fd.Message() == nil || fd.IsList() || fd.IsMap() {
			return nil, fmt.Errorf("%q is not a message field of %s", name, md.FullName())
		}
		md = fd.Message()
	}
	fd := lookupField(md, parts[last])
	if fd == nil {
		return nil, fmt.Errorf("unknown field %q in %s", parts[last], md.FullName())
	}
	return fd, nil
}

// setField sets a dotted field path on msg from its string spellings.
func setField(msg protoreflect.Message, path string, values []string) error {
	parts := strings.Split(path, ".")
	last := len(parts) - 1
	for _, name := range parts[:last] {
		fd := lookupField(msg.Descriptor(), name)
		if fd == nil || fd.Message() == nil || fd.IsList() || fd.IsMap() {
			return fmt.Errorf("%q is not a message field of %s", name, msg.Descriptor().FullName())
		}
		msg = msg.Mutable(fd).Message()
	}
	fd := lookupField(msg.Descriptor(), parts[last])
	if fd == nil {
		return fmt.Errorf("unknown field %q in %s", parts[last], msg.Descriptor().FullName())
	}
	if fd.IsMap() {
		return fmt.Errorf("map field %q cannot be set from the URL", parts[last])
	}
	return assign(msg, fd, values)
}

// assign parses the string spellings of a field and stores them. Rather than
// re-implement how every proto kind is written in JSON — base64 bytes, enum
// names, comma-separated field masks, RFC 3339 timestamps — it hands a
// one-field JSON document to protojson, which is the same code that parses the
// request body.
func assign(msg protoreflect.Message, fd protoreflect.FieldDescriptor, values []string) error {
	literal := jsonLiteral(fd, values[len(values)-1])
	if fd.IsList() {
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = jsonLiteral(fd, v)
		}
		literal = "[" + strings.Join(parts, ",") + "]"
	}

	tmp := msg.New()
	doc := "{" + strconv.Quote(fd.JSONName()) + ":" + literal + "}"
	if err := protojson.Unmarshal([]byte(doc), tmp.Interface()); err != nil {
		return err
	}
	msg.Set(fd, tmp.Get(fd))
	return nil
}

// jsonLiteral renders one value as the JSON literal protojson expects for fd.
func jsonLiteral(fd protoreflect.FieldDescriptor, value string) string {
	switch fd.Kind() {
	case protoreflect.BoolKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind:
		// Numbers and booleans are bare; an unparsable one is reported by
		// protojson as a JSON error, which is what the caller turns into a 400.
		return value
	case protoreflect.EnumKind:
		// An enum may be written as its name or its number.
		if _, err := strconv.Atoi(value); err == nil {
			return value
		}
		return strconv.Quote(value)
	default:
		return strconv.Quote(value)
	}
}

// lookupField finds a field by its JSON name or its proto name, so both
// spellings work in a query string.
func lookupField(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if fd := md.Fields().ByJSONName(name); fd != nil {
		return fd
	}
	return md.Fields().ByName(protoreflect.Name(name))
}
