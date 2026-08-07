package emulator

import (
	"net/http"

	taskspbv2 "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	taskspbv2beta3 "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"

	"github.com/ken109/cloud-tasks-emulator/rest"
)

// RESTHandler serves the REST/JSON surface of both API versions —
// `/v2/projects/.../queues` and the `/v2beta3/...` equivalents — on top of the
// same adapters and engine the gRPC surface uses. The routes come from the
// google.api.http annotations carried by the compiled protos, so they match
// what cloudtasks.googleapis.com serves for the same proto version.
//
// Serve it on its own port: a Cloud Tasks client speaks either gRPC or REST,
// never both on one connection.
func (e *Emulator) RESTHandler() (http.Handler, error) {
	return rest.Handler(
		rest.Service{Desc: &taskspbv2.CloudTasks_ServiceDesc, Impl: &v2Server{engine: e.engine}},
		rest.Service{Desc: &taskspbv2beta3.CloudTasks_ServiceDesc, Impl: &v2beta3Server{engine: e.engine}},
	)
}
