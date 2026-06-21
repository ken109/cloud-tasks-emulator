package emulator

import (
	"net/http"
	"time"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HttpMethod enum values are identical in the v2 and v2beta3 protos, so methods
// are converted via the shared integer mapping (0=unspecified, 1=POST, ...).
func httpMethodName(m int32) string {
	switch m {
	case 2:
		return http.MethodGet
	case 3:
		return http.MethodHead
	case 4:
		return http.MethodPut
	case 5:
		return http.MethodDelete
	case 6:
		return http.MethodPatch
	case 7:
		return http.MethodOptions
	default: // 0 (unspecified) and 1 (POST)
		return http.MethodPost
	}
}

func httpMethodEnum(name string) int32 {
	switch name {
	case http.MethodGet:
		return 2
	case http.MethodHead:
		return 3
	case http.MethodPut:
		return 4
	case http.MethodDelete:
		return 5
	case http.MethodPatch:
		return 6
	case http.MethodOptions:
		return 7
	default:
		return 1 // POST
	}
}

func rpcStatus(code int32, message string) *statuspb.Status {
	if code == 0 {
		return &statuspb.Status{}
	}
	return &statuspb.Status{Code: code, Message: message}
}

func tsToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func timeToTs(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func durToDuration(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func durationToDur(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}
