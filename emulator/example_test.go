package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ken109/cloud-tasks-emulator/emulator"
)

// startEmulator runs an emulator on a random port and returns its address and a
// shutdown func. Real code would do this once per test binary.
func startEmulator(cfg emulator.Config) (addr string, stop func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	emulator.New(cfg).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), gs.Stop
}

// dial connects the official Cloud Tasks client to an emulator address.
func dial(ctx context.Context, addr string) *cloudtasks.Client {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	client, err := cloudtasks.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatal(err)
	}
	return client
}

// Example shows the whole loop: run the emulator in-process, point the official
// client at it, enqueue an HTTP task and watch it arrive at its target.
func Example() {
	ctx := context.Background()

	addr, stop := startEmulator(emulator.Config{})
	defer stop()

	client := dial(ctx, addr)
	defer client.Close()

	// A stand-in for the service that handles the task.
	delivered := make(chan *http.Request, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered <- r
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	parent := "projects/my-project/locations/us-central1"
	queue, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/my-queue"},
	})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        target.URL + "/handle",
					HttpMethod: taskspb.HttpMethod_POST,
					Body:       []byte(`{"hello":"world"}`),
				},
			},
		},
	}); err != nil {
		log.Fatal(err)
	}

	// The emulator dispatches for real, so the target receives the task with
	// the same headers production Cloud Tasks sends.
	// Header.Get is case-insensitive, so the documented Cloud Tasks casing
	// works whatever your HTTP stack does to incoming header names.
	req := <-delivered
	fmt.Println("queue:", req.Header.Get("X-CloudTasks-QueueName"))
	fmt.Println("retry count:", req.Header.Get("X-CloudTasks-TaskRetryCount"))

	// Output:
	// queue: my-queue
	// retry count: 0
}

// Example_scheduledTask shows a task scheduled for the future: it stays in the
// queue until its schedule time, and RunTask forces it out early.
func Example_scheduledTask() {
	ctx := context.Background()

	addr, stop := startEmulator(emulator.Config{})
	defer stop()

	client := dial(ctx, addr)
	defer client.Close()

	delivered := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	parent := "projects/my-project/locations/us-central1"
	queue, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/scheduled"},
	})
	if err != nil {
		log.Fatal(err)
	}

	task, err := client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &taskspb.Task{
			ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: target.URL + "/later"},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	tasks := client.ListTasks(ctx, &taskspb.ListTasksRequest{Parent: queue.GetName()})
	if _, err := tasks.Next(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("waiting in the queue")

	// RunTask dispatches immediately, whatever the schedule says — handy for
	// testing a handler without waiting an hour.
	if _, err := client.RunTask(ctx, &taskspb.RunTaskRequest{Name: task.GetName()}); err != nil {
		log.Fatal(err)
	}
	<-delivered
	fmt.Println("dispatched on demand")

	// Output:
	// waiting in the queue
	// dispatched on demand
}

// ExampleEmulator_OpenIDHandler shows how a target verifies the OIDC token the
// emulator attaches: serve the discovery endpoints at the configured issuer,
// and point your existing verifier at them.
func ExampleEmulator_OpenIDHandler() {
	// The issuer must be a URL your target can reach, because that is where it
	// will fetch the signing keys from.
	discovery := httptest.NewUnstartedServer(nil)
	emu := emulator.New(emulator.Config{OpenIDIssuer: "http://" + discovery.Listener.Addr().String()})
	discovery.Config.Handler = emu.OpenIDHandler()
	discovery.Start()
	defer discovery.Close()

	resp, err := http.Get(discovery.URL + "/.well-known/openid-configuration")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	fmt.Println("discovery:", resp.StatusCode)

	jwks, err := http.Get(discovery.URL + "/jwks")
	if err != nil {
		log.Fatal(err)
	}
	defer jwks.Body.Close()
	fmt.Println("jwks:", jwks.StatusCode)

	// Output:
	// discovery: 200
	// jwks: 200
}

// ExampleEmulator_RESTHandler serves the REST/JSON API, for clients and tools
// that do not speak gRPC. The same emulator can serve both surfaces at once.
func ExampleEmulator_RESTHandler() {
	handler, err := emulator.New(emulator.Config{}).RESTHandler()
	if err != nil {
		log.Fatal(err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v2/projects/my-project/locations/us-central1/queues",
		"application/json",
		strings.NewReader(`{"name":"projects/my-project/locations/us-central1/queues/default"}`))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var queue struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&queue); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.StatusCode, queue.Name, queue.State)

	// Output:
	// 200 projects/my-project/locations/us-central1/queues/default RUNNING
}

// ExampleEmulator_EnsureQueue pre-creates a queue, so an application under test
// can enqueue without calling CreateQueue itself.
func ExampleEmulator_EnsureQueue() {
	emu := emulator.New(emulator.Config{})
	if err := emu.EnsureQueue("projects/my-project/locations/us-central1/queues/default"); err != nil {
		log.Fatal(err)
	}
	// EnsureQueue is idempotent, so startup code can call it unconditionally.
	if err := emu.EnsureQueue("projects/my-project/locations/us-central1/queues/default"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ready")

	// Output:
	// ready
}
