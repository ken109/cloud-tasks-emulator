// Command cloud-tasks-emulator runs a local, in-memory emulator for the
// Google Cloud Tasks v2 API, modelled after the official Cloud Pub/Sub
// emulator. Point a Cloud Tasks client at it by dialing its address with an
// insecure gRPC connection.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ken109/cloud-tasks-emulator/emulator"
)

// logFatalf is a seam so tests can exercise the fatal path without exiting.
var logFatalf = log.Fatalf

// options holds the resolved server configuration.
type options struct {
	host          string
	port          string
	appEngineHost string
	queues        []string
}

func main() {
	opts := parseFlags(os.Args[0], os.Args[1:])

	stop := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		close(stop)
	}()

	if err := run(opts, stop, nil); err != nil {
		logFatalf("server error: %v", err)
	}
}

// stringList collects a repeatable string flag.
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// parseFlags builds options from command-line arguments, falling back to
// environment variables when a flag is not provided.
func parseFlags(prog string, args []string) options {
	fs := flag.NewFlagSet(prog, flag.ExitOnError)
	host := fs.String("host", envOr("CLOUD_TASKS_EMULATOR_HOST", "localhost"), "host/address to bind to (env: CLOUD_TASKS_EMULATOR_HOST)")
	port := fs.String("port", envOr("CLOUD_TASKS_EMULATOR_PORT", "8123"), "port to listen on (env: CLOUD_TASKS_EMULATOR_PORT)")
	appEngineHost := fs.String("app-engine-host", envOr("CLOUD_TASKS_APP_ENGINE_HOST", ""), "default base URL for App Engine HTTP task targets (env: CLOUD_TASKS_APP_ENGINE_HOST)")

	var queues stringList
	fs.Var(&queues, "queue", "full resource name of a queue to create at startup; repeatable (env: INITIAL_QUEUES, comma-separated)")

	_ = fs.Parse(args)

	if len(queues) == 0 {
		queues = splitList(os.Getenv("INITIAL_QUEUES"))
	}
	return options{host: *host, port: *port, appEngineHost: *appEngineHost, queues: queues}
}

// run starts the gRPC server and blocks until stop is closed or Serve fails.
// When ready is non-nil it is called with the bound address once the server is
// listening (useful for tests that bind to port 0).
func run(opts options, stop <-chan struct{}, ready func(addr string)) error {
	emu := emulator.New(emulator.Config{DefaultAppEngineHost: opts.appEngineHost})

	for _, name := range opts.queues {
		if err := emu.EnsureQueue(name); err != nil {
			return fmt.Errorf("initial queue %q: %w", name, err)
		}
		log.Printf("created queue %s", name)
	}

	addr := net.JoinHostPort(opts.host, opts.port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	emu.Register(grpcServer)
	reflection.Register(grpcServer)

	go func() {
		<-stop
		grpcServer.GracefulStop()
	}()

	boundAddr := lis.Addr().String()
	if ready != nil {
		ready(boundAddr)
	}
	log.Printf("Cloud Tasks emulator listening on %s", boundAddr)
	log.Printf("Set your client's endpoint to %s and use an insecure connection.", boundAddr)
	return grpcServer.Serve(lis)
}

// envOr returns the value of the environment variable key, or def if unset.
func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// splitList splits a comma-separated list, dropping empty entries.
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
