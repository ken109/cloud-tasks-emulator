package emulator_test

import (
	"context"
	"net"
	"testing"

	ctv2 "cloud.google.com/go/cloudtasks/apiv2"
	ctv2beta3 "cloud.google.com/go/cloudtasks/apiv2beta3"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ken109/cloud-tasks-emulator/emulator"
)

const (
	testProject  = "test-project"
	testLocation = "us-central1"
)

func locationPath() string {
	return "projects/" + testProject + "/locations/" + testLocation
}

// startServer launches an in-process emulator serving both APIs and returns
// connected v2 and v2beta3 clients.
func startServer(t *testing.T, cfg emulator.Config) (*ctv2.Client, *ctv2beta3.Client) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	emulator.New(cfg).Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	v2, err := ctv2.NewClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("v2 client: %v", err)
	}
	t.Cleanup(func() { v2.Close() })

	v2beta3, err := ctv2beta3.NewClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("v2beta3 client: %v", err)
	}
	t.Cleanup(func() { v2beta3.Close() })

	return v2, v2beta3
}
