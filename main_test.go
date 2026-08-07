package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ken109/cloud-tasks-emulator/emulator"
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
	// Never the default REST port: the machine running the tests is quite
	// likely to have an emulator on it already.
	t.Setenv("CLOUD_TASKS_REST_PORT", freePort(t))

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
		errCh <- run(options{host: "127.0.0.1", port: "0"}, stop, hooks{grpcReady: func(addr string) { addrCh <- addr }})
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
	if err := run(options{host: "127.0.0.1", port: "999999"}, make(chan struct{}), hooks{}); err == nil {
		t.Error("expected listen error for invalid port")
	}
}

func TestParseFlagsInitialQueues(t *testing.T) {
	opts := parseFlags("prog", []string{
		"-queue", "projects/p/locations/l/queues/a",
		"-queue", "projects/p/locations/l/queues/b",
		"-hard-reset-on-purge-queue",
	})
	if len(opts.queues) != 2 || opts.queues[0] != "projects/p/locations/l/queues/a" {
		t.Errorf("queues = %v", opts.queues)
	}
	if !opts.hardReset {
		t.Error("hard reset flag not applied")
	}

	// The env fallback is aertje-compatible: INITIAL_QUEUES is comma-separated.
	t.Setenv("INITIAL_QUEUES", "projects/p/locations/l/queues/x, projects/p/locations/l/queues/y ,")
	t.Setenv("CLOUD_TASKS_HARD_RESET_ON_PURGE_QUEUE", "true")
	opts = parseFlags("prog", nil)
	if len(opts.queues) != 2 || opts.queues[1] != "projects/p/locations/l/queues/y" {
		t.Errorf("env queues = %v", opts.queues)
	}
	if !opts.hardReset {
		t.Error("env hard reset not applied")
	}
	if got := (&stringList{"a", "b"}).String(); got != "a,b" {
		t.Errorf("stringList.String = %q", got)
	}
}

// TestRunPreCreatesQueues checks a queue named on the command line exists
// before any client has called CreateQueue.
func TestRunPreCreatesQueues(t *testing.T) {
	stop := make(chan struct{})
	addrCh := make(chan string, 1)
	errCh := make(chan error, 1)

	opts := options{host: "127.0.0.1", port: "0", queues: []string{"projects/p/locations/l/queues/pre"}}
	go func() {
		errCh <- run(opts, stop, hooks{grpcReady: func(addr string) { addrCh <- addr }})
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

	if _, err := client.GetQueue(context.Background(), &taskspb.GetQueueRequest{
		Name: "projects/p/locations/l/queues/pre",
	}); err != nil {
		t.Fatalf("pre-created queue missing: %v", err)
	}

	close(stop)
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after stop")
	}
}

func TestRunInvalidInitialQueue(t *testing.T) {
	err := run(options{host: "127.0.0.1", port: "0", queues: []string{"nonsense"}}, make(chan struct{}), hooks{})
	if err == nil {
		t.Error("expected an error for an invalid initial queue")
	}
}

func TestParseFlagsOpenIDIssuer(t *testing.T) {
	opts := parseFlags("prog", []string{"-openid-issuer", "http://localhost:8980"})
	if opts.openIDIssuer != "http://localhost:8980" {
		t.Errorf("openIDIssuer = %q", opts.openIDIssuer)
	}
	t.Setenv("CLOUD_TASKS_OPENID_ISSUER", "http://localhost:9980")
	if got := parseFlags("prog", nil).openIDIssuer; got != "http://localhost:9980" {
		t.Errorf("env openIDIssuer = %q", got)
	}
}

func TestOpenIDAddr(t *testing.T) {
	if got, err := openIDAddr("http://localhost:8980"); err != nil || got != "0.0.0.0:8980" {
		t.Errorf("openIDAddr = %q, %v", got, err)
	}
	if _, err := openIDAddr("http://localhost"); err == nil {
		t.Error("expected an error for an issuer without a port")
	}
	if _, err := openIDAddr("http://[::1"); err == nil {
		t.Error("expected an error for an unparseable issuer")
	}
}

// TestRunServesOpenIDDiscovery checks the discovery endpoints come up on the
// issuer's port alongside the gRPC server, and shut down with it.
func TestRunServesOpenIDDiscovery(t *testing.T) {
	issuer := "http://127.0.0.1:" + freePort(t)

	stop := make(chan struct{})
	addrCh := make(chan string, 1)
	oidcCh := make(chan string, 1)
	errCh := make(chan error, 1)

	opts := options{host: "127.0.0.1", port: "0", openIDIssuer: issuer}
	go func() {
		errCh <- run(opts, stop, hooks{
			grpcReady:   func(addr string) { addrCh <- addr },
			openIDReady: func(addr string) { oidcCh <- addr },
		})
	}()

	select {
	case <-addrCh:
	case <-time.After(3 * time.Second):
		t.Fatal("server never became ready")
	}
	<-oidcCh

	for _, path := range []string{"/.well-known/openid-configuration", "/jwks", "/certs"} {
		resp, err := http.Get(issuer + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || len(body) == 0 {
			t.Errorf("GET %s = %d %q", path, resp.StatusCode, body)
		}
	}

	close(stop)
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after stop")
	}
}

// TestRunServesREST checks the REST API comes up next to the gRPC server and
// answers with the JSON a REST client expects.
func TestRunServesREST(t *testing.T) {
	stop := make(chan struct{})
	restCh := make(chan string, 1)
	errCh := make(chan error, 1)

	opts := options{host: "127.0.0.1", port: "0", restPort: "0"}
	go func() {
		errCh <- run(opts, stop, hooks{restReady: func(addr string) { restCh <- addr }})
	}()

	var addr string
	select {
	case addr = <-restCh:
	case <-time.After(3 * time.Second):
		t.Fatal("REST server never became ready")
	}

	const queue = "projects/p/locations/l/queues/q"
	resp, err := http.Post("http://"+addr+"/v2/projects/p/locations/l/queues",
		"application/json", strings.NewReader(`{"name":"`+queue+`"}`))
	if err != nil {
		t.Fatalf("CreateQueue over REST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), queue) {
		t.Fatalf("CreateQueue over REST = %d %s", resp.StatusCode, body)
	}

	close(stop)
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after stop")
	}
}

func TestRunRESTErrors(t *testing.T) {
	// An invalid REST port fails after the gRPC listener is open.
	if err := run(options{host: "127.0.0.1", port: "0", restPort: "999999"}, make(chan struct{}), hooks{}); err == nil {
		t.Error("expected an error for an invalid REST port")
	}

	// The same failure with the OpenID server already running, which must be
	// shut down on the way out.
	oldHandler := restHandlerFor
	defer func() { restHandlerFor = oldHandler }()
	restHandlerFor = func(*emulator.Emulator) (http.Handler, error) {
		return nil, errors.New("no HTTP bindings")
	}
	opts := options{host: "127.0.0.1", port: "0", restPort: "0", openIDIssuer: "http://127.0.0.1:" + freePort(t)}
	if err := run(opts, make(chan struct{}), hooks{}); err == nil {
		t.Error("expected an error when the REST handler cannot be built")
	}
}

func TestParseFlagsRESTPort(t *testing.T) {
	if got := parseFlags("prog", []string{"-rest-port", "9124"}).restPort; got != "9124" {
		t.Errorf("restPort = %q", got)
	}
	t.Setenv("CLOUD_TASKS_REST_PORT", "")
	if got := parseFlags("prog", nil).restPort; got != "" {
		t.Errorf("env restPort = %q, want it disabled", got)
	}
}

func TestRunOpenIDErrors(t *testing.T) {
	// A bad issuer fails after the gRPC listener is open, which must be closed.
	if err := run(options{host: "127.0.0.1", port: "0", openIDIssuer: "http://no-port"}, make(chan struct{}), hooks{}); err == nil {
		t.Error("expected an error for an issuer without a port")
	}
	// The issuer port already being taken is reported too.
	busy, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()
	_, busyPort, _ := net.SplitHostPort(busy.Addr().String())
	if err := run(options{host: "127.0.0.1", port: "0", openIDIssuer: "http://127.0.0.1:" + busyPort}, make(chan struct{}), hooks{}); err == nil {
		t.Error("expected an error when the issuer port is in use")
	}
}
