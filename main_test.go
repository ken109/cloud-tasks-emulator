package main

import (
	"context"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// freePort returns a currently-unused localhost port.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}

// TestMainEntrypoint drives main() end to end, including the SIGTERM-triggered
// graceful shutdown path.
func TestMainEntrypoint(t *testing.T) {
	port := freePort(t)
	addr := net.JoinHostPort("127.0.0.1", port)
	t.Setenv("CLOUD_TASKS_EMULATOR_HOST", "127.0.0.1")
	t.Setenv("CLOUD_TASKS_EMULATOR_PORT", port)

	oldArgs := os.Args
	os.Args = []string{"cloud-tasks-emulator"}
	defer func() { os.Args = oldArgs }()

	done := make(chan struct{})
	go func() {
		main()
		close(done)
	}()

	// Wait until the server is accepting connections.
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("main server never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Give the signal handler a moment to register before signalling.
	time.Sleep(100 * time.Millisecond)

	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("main did not shut down after SIGTERM")
	}
}

func TestEnvOr(t *testing.T) {
	if got := envOr("CLOUD_TASKS_TEST_UNSET", "fallback"); got != "fallback" {
		t.Errorf("envOr unset = %q, want fallback", got)
	}
	t.Setenv("CLOUD_TASKS_TEST_SET", "value")
	if got := envOr("CLOUD_TASKS_TEST_SET", "fallback"); got != "value" {
		t.Errorf("envOr set = %q, want value", got)
	}
}

func TestParseFlags(t *testing.T) {
	// Flags win.
	opts := parseFlags("prog", []string{"-host", "0.0.0.0", "-port", "9999", "-app-engine-host", "http://x"})
	if opts.host != "0.0.0.0" || opts.port != "9999" || opts.appEngineHost != "http://x" {
		t.Errorf("parseFlags flags = %+v", opts)
	}

	// Env fallback when no flag.
	t.Setenv("CLOUD_TASKS_EMULATOR_PORT", "7777")
	opts = parseFlags("prog", nil)
	if opts.port != "7777" {
		t.Errorf("parseFlags env port = %q, want 7777", opts.port)
	}
}

func TestRunServesAndShutsDown(t *testing.T) {
	stop := make(chan struct{})
	addrCh := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(options{host: "127.0.0.1", port: "0"}, stop, func(addr string) { addrCh <- addr })
	}()

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(3 * time.Second):
		t.Fatal("server never became ready")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client, err := cloudtasks.NewClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Close()

	_, err = client.CreateQueue(context.Background(), &taskspb.CreateQueueRequest{
		Parent: "projects/p/locations/l",
		Queue:  &taskspb.Queue{Name: "projects/p/locations/l/queues/q"},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	close(stop) // triggers GracefulStop
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after stop")
	}
}

func TestMainFatalOnListenError(t *testing.T) {
	t.Setenv("CLOUD_TASKS_EMULATOR_HOST", "127.0.0.1")
	t.Setenv("CLOUD_TASKS_EMULATOR_PORT", "999999") // invalid -> net.Listen fails

	oldArgs := os.Args
	os.Args = []string{"cloud-tasks-emulator"}
	defer func() { os.Args = oldArgs }()

	oldFatal := logFatalf
	defer func() { logFatalf = oldFatal }()
	var called bool
	logFatalf = func(string, ...any) { called = true }

	main() // run() fails fast, so logFatalf is invoked and main returns
	if !called {
		t.Error("expected logFatalf to be called on listen error")
	}
}

func TestRunListenError(t *testing.T) {
	// An invalid port forces net.Listen to fail.
	if err := run(options{host: "127.0.0.1", port: "999999"}, make(chan struct{}), nil); err == nil {
		t.Error("expected listen error for invalid port")
	}
}
