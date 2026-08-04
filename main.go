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
	appEngineHost string
	openIDIssuer  string
	hardReset     bool
	queues        []string
}

// hooks are optional callbacks fired once each server is listening. Tests use
// them to discover the addresses when binding to port 0.
type hooks struct {
	grpcReady   func(addr string)
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

// parseFlags builds options from command-line arguments, falling back to
// environment variables when a flag is not provided.
func parseFlags(prog string, args []string) options {
	fs := flag.NewFlagSet(prog, flag.ExitOnError)
	host := fs.String("host", envOr("CLOUD_TASKS_EMULATOR_HOST", "localhost"), "host/address to bind to (env: CLOUD_TASKS_EMULATOR_HOST)")
	port := fs.String("port", envOr("CLOUD_TASKS_EMULATOR_PORT", "8123"), "port to listen on (env: CLOUD_TASKS_EMULATOR_PORT)")
	appEngineHost := fs.String("app-engine-host", envOr("CLOUD_TASKS_APP_ENGINE_HOST", ""), "default base URL for App Engine HTTP task targets (env: CLOUD_TASKS_APP_ENGINE_HOST)")
	openIDIssuer := fs.String("openid-issuer", envOr("CLOUD_TASKS_OPENID_ISSUER", ""), "base URL to serve the OpenID discovery endpoints on, e.g. http://localhost:8980; also the `iss` claim of dispatched OIDC tokens (env: CLOUD_TASKS_OPENID_ISSUER)")
	hardReset := fs.Bool("hard-reset-on-purge-queue", envBool("CLOUD_TASKS_HARD_RESET_ON_PURGE_QUEUE"), "drop task-name history when a queue is purged, so purged names can be reused immediately (env: CLOUD_TASKS_HARD_RESET_ON_PURGE_QUEUE)")

	var queues stringList
	fs.Var(&queues, "queue", "full resource name of a queue to create at startup; repeatable (env: INITIAL_QUEUES, comma-separated)")

	_ = fs.Parse(args)

	if len(queues) == 0 {
		queues = splitList(os.Getenv("INITIAL_QUEUES"))
	}
	return options{
		host:          *host,
		port:          *port,
		appEngineHost: *appEngineHost,
		openIDIssuer:  *openIDIssuer,
		hardReset:     *hardReset,
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

	grpcServer := grpc.NewServer()
	emu.Register(grpcServer)
	reflection.Register(grpcServer)

	go func() {
		<-stop
		if openIDServer != nil {
			openIDServer.Close()
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
	go srv.Serve(lis)
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

// envOr returns the value of the environment variable key, or def if unset.
func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// envBool reports whether the environment variable holds a true-ish value.
func envBool(key string) bool {
	v, _ := strconv.ParseBool(os.Getenv(key))
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
