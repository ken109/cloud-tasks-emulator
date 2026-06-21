// Command cloud-tasks-emulator runs a local, in-memory emulator for the
// Google Cloud Tasks v2 API, modelled after the official Cloud Pub/Sub
// emulator. Point a Cloud Tasks client at it by dialing its address with an
// insecure gRPC connection.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ken109/cloud-tasks-emulator/internal/emulator"
)

func main() {
	// Flags take precedence; when unset they fall back to environment
	// variables, mirroring how the Cloud SDK emulators are configured.
	var (
		host          = flag.String("host", envOr("CLOUD_TASKS_EMULATOR_HOST", "localhost"), "host/address to bind to (env: CLOUD_TASKS_EMULATOR_HOST)")
		port          = flag.String("port", envOr("CLOUD_TASKS_EMULATOR_PORT", "8123"), "port to listen on (env: CLOUD_TASKS_EMULATOR_PORT)")
		appEngineHost = flag.String("app-engine-host", envOr("CLOUD_TASKS_APP_ENGINE_HOST", ""), "default base URL for App Engine HTTP task targets (env: CLOUD_TASKS_APP_ENGINE_HOST)")
	)
	flag.Parse()

	addr := net.JoinHostPort(*host, *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	srv := emulator.NewServer(emulator.Config{
		DefaultAppEngineHost: *appEngineHost,
	})

	grpcServer := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(grpcServer, srv)
	reflection.Register(grpcServer)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		grpcServer.GracefulStop()
	}()

	log.Printf("Cloud Tasks emulator listening on %s", addr)
	log.Printf("Set your client's endpoint to %s and use an insecure connection.", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// envOr returns the value of the environment variable key, or def if unset.
func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
