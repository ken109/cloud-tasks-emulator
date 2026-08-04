# Testing against the emulator

Recipes for the things people actually want to test with Cloud Tasks: that a
task reaches the handler, that retries behave, and that a scheduled task fires
when it should — without waiting for wall-clock time or reaching the real API.

- [Choosing how to run it](#choosing-how-to-run-it)
- [Go — in-process](#go--in-process)
- [Any language — Testcontainers](#any-language--testcontainers)
- [Compose for a whole stack](#compose-for-a-whole-stack)
- [Testing retries and backoff](#testing-retries-and-backoff)
- [Testing scheduled tasks without waiting](#testing-scheduled-tasks-without-waiting)
- [Reaching your handler from a container](#reaching-your-handler-from-a-container)
- [Isolating tests from each other](#isolating-tests-from-each-other)

## Choosing how to run it

| | Startup | Isolation | Use when |
|---|---|---|---|
| In-process (Go) | microseconds | per test | You are in Go. Nothing else comes close |
| Testcontainers | ~1s per container | per container | Any other language, or you want the real binary |
| Compose | once per stack | shared | Manual development, or an end-to-end suite that already uses Compose |

## Go — in-process

No Docker, no ports to manage, and the emulator dies with the test binary.

```go
func newEmulator(t *testing.T) *cloudtasks.Client {
    t.Helper()

    lis, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatal(err)
    }
    gs := grpc.NewServer()
    emulator.New(emulator.Config{}).Register(gs) // serves v2 and v2beta3
    go func() { _ = gs.Serve(lis) }()
    t.Cleanup(gs.Stop)

    conn, err := grpc.NewClient(lis.Addr().String(),
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        t.Fatal(err)
    }
    client, err := cloudtasks.NewClient(t.Context(), option.WithGRPCConn(conn))
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { client.Close() })
    return client
}
```

Runnable versions of this live in
[`emulator/example_test.go`](../emulator/example_test.go) and on
[pkg.go.dev](https://pkg.go.dev/github.com/ken109/cloud-tasks-emulator/emulator).

`emulator.Config` is where you shorten the lifecycle for tests:

```go
emulator.New(emulator.Config{
    TaskTTL:          time.Minute,      // default 31 days
    TombstoneTTL:     time.Second,      // default 24 hours
    HardResetOnPurge: true,             // reuse task names after PurgeQueue
    OpenIDIssuer:     "http://127.0.0.1:8980",
})
```

## Any language — Testcontainers

Start the published image, read the mapped port, point the official client at
it. Testcontainers has modules for Java, Go, Python, Node.js, .NET and Rust; the
three steps are the same everywhere.

```python
# Python
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

container = (
    DockerContainer("ghcr.io/ken109/cloud-tasks-emulator:latest")
    .with_command("-host 0.0.0.0 -port 8123")
    .with_exposed_ports(8123)
)
container.start()
wait_for_logs(container, "listening on")
addr = f"{container.get_container_host_ip()}:{container.get_exposed_port(8123)}"
```

```js
// Node.js
import { GenericContainer, Wait } from "testcontainers";

const container = await new GenericContainer("ghcr.io/ken109/cloud-tasks-emulator:latest")
  .withCommand(["-host", "0.0.0.0", "-port", "8123"])
  .withExposedPorts(8123)
  .withWaitStrategy(Wait.forListeningPorts())
  .start();
const addr = `${container.getHost()}:${container.getMappedPort(8123)}`;
```

```go
// Go, when you want the real binary rather than the in-process emulator
ctr, _ := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
    ContainerRequest: testcontainers.ContainerRequest{
        Image:        "ghcr.io/ken109/cloud-tasks-emulator:latest",
        Cmd:          []string{"-host", "0.0.0.0", "-port", "8123"},
        ExposedPorts: []string{"8123/tcp"},
        WaitingFor:   wait.ForListeningPort("8123/tcp"),
    },
    Started: true,
})
addr, _ := ctr.PortEndpoint(ctx, "8123/tcp", "")
```

Pin a version tag rather than `latest` in CI, so a new release cannot change
what your tests run against mid-week.

## Compose for a whole stack

[`compose.yaml`](../compose.yaml) in this repository is a runnable starting
point. The parts that matter:

```yaml
services:
  cloud-tasks:
    image: ghcr.io/ken109/cloud-tasks-emulator:latest
    command:
      - -host=0.0.0.0
      # Create the queues the application expects, so nothing has to call
      # CreateQueue on boot.
      - -queue=projects/local/locations/us-central1/queues/default
```

## Testing retries and backoff

A queue's `RetryConfig` is honoured for real, so shorten it rather than
mocking it. With a minimal backoff the whole retry sequence takes milliseconds:

```go
client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
    Parent: parent,
    Queue: &taskspb.Queue{
        Name: parent + "/queues/retries",
        RetryConfig: &taskspb.RetryConfig{
            MaxAttempts:  3,
            MinBackoff:   durationpb.New(10 * time.Millisecond),
            MaxBackoff:   durationpb.New(10 * time.Millisecond),
            MaxDoublings: 0,
        },
    },
})
```

Have the target fail, count the attempts, and assert the emulator gave up after
`MaxAttempts`:

```go
var attempts atomic.Int32
target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    n := attempts.Add(1)
    // The headers carry the retry state, exactly as in production.
    t.Logf("attempt %s, previous response %s",
        r.Header.Get("X-CloudTasks-TaskRetryCount"),
        r.Header.Get("X-CloudTasks-TaskPreviousResponse"))
    if n < 3 {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusOK)
}))
```

Useful facts when writing these:

- A 2xx removes the task. Anything else is a failed attempt, including 3xx —
  redirects are not followed, as in production.
- `X-CloudTasks-TaskRetryCount` is 0 on the first attempt.
- `X-CloudTasks-TaskPreviousResponse` and `-TaskRetryReason` appear only from
  the second attempt onwards.
- After `MaxAttempts` (or `MaxRetryDuration`) the task is dropped, so
  `ListTasks` going empty means either success or exhaustion — assert on what
  the target saw, not just on the queue.

## Testing scheduled tasks without waiting

`RunTask` dispatches immediately whatever `schedule_time` says, which is how you
test a handler for a task scheduled hours out:

```go
task, _ := client.CreateTask(ctx, &taskspb.CreateTaskRequest{
    Parent: queueName,
    Task: &taskspb.Task{
        ScheduleTime: timestamppb.New(time.Now().Add(24 * time.Hour)),
        MessageType:  &taskspb.Task_HttpRequest{HttpRequest: httpRequest},
    },
})

// ... assert it is sitting in the queue ...

client.RunTask(ctx, &taskspb.RunTaskRequest{Name: task.GetName()})
// ... now assert the handler ran ...
```

`PauseQueue` is the other half: pause, enqueue several tasks, assert nothing
was dispatched, then `ResumeQueue` and watch them drain.

## Reaching your handler from a container

The emulator dispatches from wherever it runs, so a containerised emulator
cannot reach `127.0.0.1:8080` on your machine.

| Setup | Target URL to use |
|---|---|
| Emulator in-process (Go) | `httptest` server URL, as-is |
| Emulator in Docker, handler on the host | `http://host.docker.internal:8080` (on Linux add `--add-host=host.docker.internal:host-gateway`) |
| Both in Compose | the service name, `http://handler:8080` |
| Linux CI with `--network host` | `http://127.0.0.1:8080` |

For App Engine targets, the host comes from the task's `app_engine_routing`,
the queue's `app_engine_routing_override`, or the `-app-engine-host` flag — set
one of them or the dispatch fails with "no App Engine target host configured".

## Isolating tests from each other

State is in memory and per process, so the cheapest isolation is a fresh
emulator per test — trivial in Go, ~1s per container elsewhere.

Sharing one emulator across a suite is fine if you keep tests apart:

- **Give each test its own queue.** Queue names are cheap and `ListTasks` is
  per queue.
- **Or give each test its own project.** `projects/test-42/locations/...` is
  never validated against anything real.
- **Reusing a fixed task name after `PurgeQueue`** needs
  `-hard-reset-on-purge-queue` (or `Config.HardResetOnPurge`): purged names stay
  reserved for the tombstone window, as in production.
- **Auto-named tasks never collide**, so prefer letting the emulator name them
  unless the test is specifically about named tasks.
