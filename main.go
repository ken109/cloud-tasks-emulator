// Command cloud-tasks-emulator runs a local, in-memory emulator for the
// Google Cloud Tasks v2 and v2beta3 APIs, modelled after the official Cloud
// Pub/Sub emulator. Point a Cloud Tasks client at it by dialing its address
// with an insecure gRPC connection.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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
	restPort      string
	appEngineHost string
	openIDIssuer  string
	hardReset     bool
	queues        []string
}

// hooks are optional callbacks fired once each server is listening. Tests use
// them to discover the addresses when binding to port 0.
type hooks struct {
	grpcReady   func(addr string)
	restReady   func(addr string)
	openIDReady func(addr string)
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

	if err := run(opts, stop, hooks{}); err != nil {
		logFatalf("server error: %v", err)
	}
}

// stringList collects a repeatable string flag.
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// flagValues holds the destinations the command-line flags parse into.
type flagValues struct {
	host          *string
	port          *string
	restPort      *string
	appEngineHost *string
	openIDIssuer  *string
	hardReset     *bool
	queues        stringList
}

// registerFlags declares the whole command-line surface on fs. It is the single
// source of truth for the flags, their defaults and the environment variables
// they fall back to — a test walks it to check the README stays in step.
func registerFlags(fs *flag.FlagSet) *flagValues {
	v := &flagValues{}
	v.host = fs.String("host", envOr("CLOUD_TASKS_EMULATOR_HOST", "localhost"), "host/address to bind to (env: CLOUD_TASKS_EMULATOR_HOST)")
	v.port = fs.String("port", envOr("CLOUD_TASKS_EMULATOR_PORT", "8123"), "port to listen on (env: CLOUD_TASKS_EMULATOR_PORT)")
	v.restPort = fs.String("rest-port", envOr("CLOUD_TASKS_REST_PORT", "8124"), "port to serve the REST/JSON API on; empty disables it (env: CLOUD_TASKS_REST_PORT)")
	v.appEngineHost = fs.String("app-engine-host", envOr("CLOUD_TASKS_APP_ENGINE_HOST", ""), "default base URL for App Engine HTTP task targets (env: CLOUD_TASKS_APP_ENGINE_HOST)")
	v.openIDIssuer = fs.String("openid-issuer", envOr("CLOUD_TASKS_OPENID_ISSUER", ""), "base URL to serve the OpenID discovery endpoints on, e.g. http://localhost:8980; also the iss claim of dispatched OIDC tokens (env: CLOUD_TASKS_OPENID_ISSUER)")
	v.hardReset = fs.Bool("hard-reset-on-purge-queue", envBool("CLOUD_TASKS_HARD_RESET_ON_PURGE_QUEUE"), "drop task-name history when a queue is purged, so purged names can be reused immediately (env: CLOUD_TASKS_HARD_RESET_ON_PURGE_QUEUE)")
	fs.Var(&v.queues, "queue", "full resource name of a queue to create at startup; repeatable (env: INITIAL_QUEUES, comma-separated)")
	return v
}

// parseFlags builds options from command-line arguments, falling back to
// environment variables when a flag is not provided.
func parseFlags(prog string, args []string) options {
	fs := flag.NewFlagSet(prog, flag.ExitOnError)
	v := registerFlags(fs)
	_ = fs.Parse(args)

	queues := v.queues
	if len(queues) == 0 {
		queues = splitList(envOr("INITIAL_QUEUES", ""))
	}
	return options{
		host:          *v.host,
		port:          *v.port,
		restPort:      *v.restPort,
		appEngineHost: *v.appEngineHost,
		openIDIssuer:  *v.openIDIssuer,
		hardReset:     *v.hardReset,
		queues:        queues,
	}
}

// run starts the gRPC server (and the OpenID discovery server when an issuer is
// configured) and blocks until stop is closed or Serve fails.
func run(opts options, stop <-chan struct{}, h hooks) error {
	emu := emulator.New(emulator.Config{
		DefaultAppEngineHost: opts.appEngineHost,
		OpenIDIssuer:         opts.openIDIssuer,
		HardResetOnPurge:     opts.hardReset,
	})

	for _, name := range opts.queues {
		if err := emu.EnsureQueue(name); err != nil {
			return fmt.Errorf("initial queue %q: %w", name, err)
		}
		log.Printf("created queue %s", name)
	}

	lis, err := net.Listen("tcp", net.JoinHostPort(opts.host, opts.port))
	if err != nil {
		return err
	}

	openIDServer, err := startOpenIDServer(opts.openIDIssuer, emu.OpenIDHandler(), h.openIDReady)
	if err != nil {
		lis.Close()
		return err
	}

	restServer, err := startRESTServer(emu, opts, h.restReady)
	if err != nil {
		lis.Close()
		if openIDServer != nil {
			openIDServer.Close()
		}
		return err
	}

	grpcServer := grpc.NewServer()
	emu.Register(grpcServer)
	reflection.Register(grpcServer)

	go func() {
		<-stop
		if openIDServer != nil {
			openIDServer.Close()
		}
		if restServer != nil {
			restServer.Close()
		}
		grpcServer.GracefulStop()
	}()

	boundAddr := lis.Addr().String()
	if h.grpcReady != nil {
		h.grpcReady(boundAddr)
	}
	log.Printf("Cloud Tasks emulator listening on %s", boundAddr)
	log.Printf("Set your client's endpoint to %s and use an insecure connection.", boundAddr)
	return grpcServer.Serve(lis)
}

// restHandlerFor is a seam so tests can exercise the failure path: building the
// handler only fails if the compiled protos lost their HTTP bindings, which no
// runtime input can cause.
var restHandlerFor = (*emulator.Emulator).RESTHandler

// startRESTServer serves the REST/JSON API next to the gRPC one. It returns a
// nil server when no REST port is configured.
func startRESTServer(emu *emulator.Emulator, opts options, ready func(addr string)) (*http.Server, error) {
	if opts.restPort == "" {
		return nil, nil
	}
	handler, err := restHandlerFor(emu)
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", net.JoinHostPort(opts.host, opts.restPort))
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: handler}
	// Serve only returns once we close the server at shutdown, and the listener
	// is ours, so there is no error worth surfacing here.
	go func() { _ = srv.Serve(lis) }()
	if ready != nil {
		ready(lis.Addr().String())
	}
	log.Printf("REST API listening on http://%s (e.g. GET /v2/projects/p/locations/l/queues)", lis.Addr())
	return srv, nil
}

// startOpenIDServer serves handler on the port of the issuer URL. It returns a
// nil server when no issuer is configured.
func startOpenIDServer(issuer string, handler http.Handler, ready func(addr string)) (*http.Server, error) {
	if issuer == "" {
		return nil, nil
	}
	addr, err := openIDAddr(issuer)
	if err != nil {
		return nil, err
	}
	// Bind on all interfaces: the target verifying a token is usually another
	// container, not the emulator's own host.
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: handler}
	// Serve only returns once we close the server at shutdown, and the listener
	// is ours, so there is no error worth surfacing here.
	go func() { _ = srv.Serve(lis) }()
	if ready != nil {
		ready(lis.Addr().String())
	}
	log.Printf("OpenID discovery for issuer %s served on %s", issuer, lis.Addr())
	return srv, nil
}

// openIDAddr derives the listen address from an issuer URL.
func openIDAddr(issuer string) (string, error) {
	u, err := url.Parse(issuer)
	if err != nil {
		return "", fmt.Errorf("invalid openid issuer %q: %w", issuer, err)
	}
	if u.Port() == "" {
		return "", fmt.Errorf("openid issuer %q must include a port, e.g. http://localhost:8980", issuer)
	}
	return "0.0.0.0:" + u.Port(), nil
}

// lookupEnv is a seam so tests can read the flag defaults with a pristine
// environment, which is what the README documents.
var lookupEnv = os.LookupEnv

// envOr returns the value of the environment variable key, or def if unset.
func envOr(key, def string) string {
	if v, ok := lookupEnv(key); ok {
		return v
	}
	return def
}

// envBool reports whether the environment variable holds a true-ish value.
func envBool(key string) bool {
	v, _ := strconv.ParseBool(envOr(key, ""))
	return v
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
