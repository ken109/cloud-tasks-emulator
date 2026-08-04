// Package emulator exposes a local, in-memory Google Cloud Tasks emulator that
// serves BOTH the google.cloud.tasks.v2 and google.cloud.tasks.v2beta3 gRPC
// APIs from a single shared engine. Register it on a gRPC server and point the
// official client libraries at it over an insecure connection.
package emulator

import (
	"net/http"

	taskspbv2 "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	taskspbv2beta3 "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"google.golang.org/grpc"

	"github.com/ken109/cloud-tasks-emulator/core"
)

// Config configures the emulator. It aliases the engine configuration.
type Config = core.Config

// Emulator wraps the shared engine and the per-version gRPC adapters.
type Emulator struct {
	engine *core.Engine
}

// New constructs an Emulator with a fresh in-memory engine.
func New(cfg Config) *Emulator {
	return &Emulator{engine: core.NewEngine(cfg)}
}

// Register registers both the v2 and v2beta3 Cloud Tasks services on gs.
func (e *Emulator) Register(gs *grpc.Server) {
	taskspbv2.RegisterCloudTasksServer(gs, &v2Server{engine: e.engine})
	taskspbv2beta3.RegisterCloudTasksServer(gs, &v2beta3Server{engine: e.engine})
}

// Engine returns the underlying engine for advanced/in-process use.
func (e *Emulator) Engine() *core.Engine { return e.engine }

// OpenIDHandler serves the OpenID discovery document and the public keys that
// verify the OIDC tokens attached to dispatched tasks. Serve it at the URL
// configured as Config.OpenIDIssuer.
func (e *Emulator) OpenIDHandler() http.Handler { return e.engine.Signer().Handler() }

// EnsureQueue creates a queue by full resource name if it does not already
// exist, for pre-provisioning queues at startup.
func (e *Emulator) EnsureQueue(name string) error { return e.engine.EnsureQueue(name) }
