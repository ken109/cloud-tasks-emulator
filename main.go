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
	var (
		host          = flag.String("host", "localhost", "host/address to bind the gRPC server to")
		port          = flag.String("port", "8123", "port to listen on")
		appEngineHost = flag.String("app-engine-host", "", "default base URL for App Engine HTTP task targets, e.g. http://localhost:8080")
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
