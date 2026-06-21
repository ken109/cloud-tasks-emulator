# cloud-tasks-emulator

A local, in-memory emulator for [Google Cloud Tasks](https://cloud.google.com/tasks),
in the spirit of the official Cloud Pub/Sub emulator (`gcloud beta emulators pubsub`).

It speaks the real `google.cloud.tasks.v2` gRPC API, so the official Cloud Tasks
client libraries (Go, Python, Node.js, Java, ...) can talk to it unchanged — you
just point them at the emulator's address over an insecure connection. Tasks are
actually **dispatched**: when a task becomes due, the emulator makes the HTTP
request to your target and applies the queue's retry/backoff and rate-limit
policy, just like production.

> Not affiliated with Google. Intended for local development and testing only —
> all state is in memory and lost on restart.

## Features

- Full v2 gRPC surface: `CreateQueue` / `GetQueue` / `ListQueues` / `UpdateQueue` /
  `DeleteQueue` / `PurgeQueue` / `PauseQueue` / `ResumeQueue` and
  `CreateTask` / `GetTask` / `ListTasks` / `DeleteTask` / `RunTask`.
- Real HTTP task dispatch for both **HTTP targets** (`HttpRequest`) and
  **App Engine targets** (`AppEngineHttpRequest`).
- Full Cloud Tasks request headers, with the correct prefix per target type:
  - HTTP targets: `X-CloudTasks-QueueName`, `-TaskName`, `-TaskRetryCount`,
    `-TaskExecutionCount`, `-TaskETA`, and on retries `-TaskPreviousResponse`
    and `-TaskRetryReason`; `User-Agent: Google-Cloud-Tasks`.
  - App Engine targets: the same fields with the `X-AppEngine-` prefix, plus
    `X-AppEngine-FailFast`; `User-Agent: AppEngine-Google; (+http://code.google.com/appengine)`.
- `Authorization: Bearer` tokens generated from a task's `OidcToken` /
  `OauthToken` (the OIDC token is an unsigned JWT carrying the configured
  service-account email and audience).
- Redirects are **not** followed — a 3xx response is a failed dispatch, exactly
  like production.
- Scheduling via `schedule_time`, retries with exponential backoff
  (`RetryConfig`), rate limiting and bounded concurrency (`RateLimits`).
- Pagination (`page_size` / `page_token`) for `ListQueues` and `ListTasks`.
- Resource-limit validation on `CreateTask` (1MB HTTP / 100KB App Engine body,
  `schedule_time` ≤ 30 days ahead, `dispatch_deadline` in 15s–30m).
- Task-name tombstones after deletion/completion and a task TTL, matching the
  Cloud Tasks lifecycle (both durations configurable).
- Queue pause/resume/purge semantics.
- In-memory IAM policy support (`GetIamPolicy` / `SetIamPolicy` /
  `TestIamPermissions`), like the Cloud Pub/Sub emulator.
- Configuration via flags or environment variables.
- Cloud Tasks default values applied to queues (500 dispatches/s, 100 max
  attempts, 0.1s–3600s backoff, ...).

## Install / run

```bash
go install github.com/ken109/cloud-tasks-emulator@latest
cloud-tasks-emulator -host localhost -port 8123
```

Or from source:

```bash
make run          # builds and runs on localhost:8123
```

Or with the prebuilt image from GitHub Container Registry (multi-arch
`linux/amd64` + `linux/arm64`):

```bash
docker run --rm -p 8123:8123 ghcr.io/ken109/cloud-tasks-emulator:latest
```

Or build the image yourself:

```bash
docker build -t cloud-tasks-emulator .
docker run --rm -p 8123:8123 cloud-tasks-emulator
```

Images are published automatically on every push to `main` (tagged `latest`)
and on `v*` release tags (tagged with the semver version) by the
[Publish Docker image](.github/workflows/docker-publish.yml) workflow.

### Configuration

Each flag falls back to an environment variable when not set on the command
line (flags win), which is convenient for containers and Compose files.

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `-host` | `CLOUD_TASKS_EMULATOR_HOST` | `localhost` | Address to bind the gRPC server to |
| `-port` | `CLOUD_TASKS_EMULATOR_PORT` | `8123` | Port to listen on |
| `-app-engine-host` | `CLOUD_TASKS_APP_ENGINE_HOST` | _(empty)_ | Base URL used to dispatch `AppEngineHttpRequest` tasks when no host is set on the task or queue, e.g. `http://localhost:8080` |

```bash
docker run --rm -p 8123:8123 \
  -e CLOUD_TASKS_EMULATOR_HOST=0.0.0.0 \
  -e CLOUD_TASKS_APP_ENGINE_HOST=http://host.docker.internal:8080 \
  ghcr.io/ken109/cloud-tasks-emulator:latest
```

## Connecting a client

There is no official `CLOUDTASKS_EMULATOR_HOST` convention, so point the client
at the emulator explicitly with an insecure gRPC connection.

### Go

```go
conn, _ := grpc.NewClient("localhost:8123",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
client, _ := cloudtasks.NewClient(ctx, option.WithGRPCConn(conn))

queue, _ := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
    Parent: "projects/my-project/locations/us-central1",
    Queue:  &taskspb.Queue{Name: "projects/my-project/locations/us-central1/queues/my-queue"},
})

client.CreateTask(ctx, &taskspb.CreateTaskRequest{
    Parent: queue.GetName(),
    Task: &taskspb.Task{
        MessageType: &taskspb.Task_HttpRequest{
            HttpRequest: &taskspb.HttpRequest{
                Url:        "http://localhost:8080/handle",
                HttpMethod: taskspb.HttpMethod_POST,
                Body:       []byte(`{"hello":"world"}`),
            },
        },
    },
})
```

### Python

```python
import grpc
from google.cloud import tasks_v2
from google.api_core.client_options import ClientOptions

client = tasks_v2.CloudTasksClient(
    client_options=ClientOptions(api_endpoint="localhost:8123"),
    transport="grpc",
    channel=grpc.insecure_channel("localhost:8123"),
)
```

### Node.js

```js
const {CloudTasksClient} = require('@google-cloud/tasks').v2;
const client = new CloudTasksClient({
  apiEndpoint: 'localhost:8123',
  port: 8123,
  sslCreds: require('@grpc/grpc-js').credentials.createInsecure(),
});
```

## Using in tests

### Any language — Testcontainers (recommended)

For integration tests in **any** language, the simplest and most portable option
is to run the published image with [Testcontainers](https://testcontainers.com/):
spin up `ghcr.io/ken109/cloud-tasks-emulator` as a container, read its mapped
port, and point the official Cloud Tasks client at it over an insecure gRPC
connection. Testcontainers has modules for Java, Go, Python, Node.js, .NET, Rust
and more, so the same approach works regardless of your stack.

```go
// Go example using github.com/testcontainers/testcontainers-go
ctr, _ := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
    ContainerRequest: testcontainers.ContainerRequest{
        Image:        "ghcr.io/ken109/cloud-tasks-emulator:latest",
        Cmd:          []string{"-host", "0.0.0.0", "-port", "8123"},
        ExposedPorts: []string{"8123/tcp"},
        WaitingFor:   wait.ForListeningPort("8123/tcp"),
    },
    Started: true,
})
endpoint, _ := ctr.PortEndpoint(ctx, "8123/tcp", "")
// dial `endpoint` with insecure gRPC, then use the official Cloud Tasks client
```

The equivalent in Java/Python/Node/etc. is the same three steps: start the
image, get the mapped `8123` port, connect the official client to it.

### Go — embed in-process

If you're already in Go, the emulator is a plain `google.cloud.tasks.v2` gRPC
server, so you can run it in-process and skip Docker entirely:

```go
import (
    "google.golang.org/grpc"
    cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
    taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
    "github.com/ken109/cloud-tasks-emulator/emulator"
)

lis, _ := net.Listen("tcp", "127.0.0.1:0")
gs := grpc.NewServer()
taskspb.RegisterCloudTasksServer(gs, emulator.NewServer(emulator.Config{}))
go gs.Serve(lis)
defer gs.Stop()

conn, _ := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
client, _ := cloudtasks.NewClient(ctx, option.WithGRPCConn(conn))
```

`emulator.Config` lets you tune `DefaultAppEngineHost`, `TaskTTL` and
`TombstoneTTL` (handy for shortening the lifecycle in tests).

## Behaviour notes

- **State is in memory.** Restarting the emulator clears all queues and tasks.
- **Successful dispatch** (HTTP 2xx) removes the task from the queue.
- **Failed dispatch** is retried per the queue's `RetryConfig` until
  `max_attempts` or `max_retry_duration` is reached, then the task is dropped.
- `RunTask` forces an immediate dispatch attempt regardless of `schedule_time`.
- App Engine targets need a reachable host: set one via the task's
  `app_engine_routing.host`, the queue's `app_engine_routing_override.host`, or
  the `-app-engine-host` flag.
- IAM methods (`GetIamPolicy` / `SetIamPolicy` / `TestIamPermissions`) are
  supported: policies are stored in memory per queue and round-trip like the
  Cloud Pub/Sub emulator, but are never enforced.

## Scope and limitations

- Targets the **stable `google.cloud.tasks.v2`** surface as published by the
  official Go client (`cloud.google.com/go/cloudtasks/apiv2`). Fields and RPCs
  that only exist in `v2beta3`/newer revisions — notably the queue-level
  `Queue.http_target` override and the `BufferTask` RPC — are not part of this
  API version and are therefore not implemented.
- IAM policies are stored and returned but never **enforced** (same as the
  Cloud Pub/Sub emulator).
- All state is in memory; nothing is persisted across restarts.
- `ListQueues` ignores the `filter` argument (all queues in the parent are
  returned, paginated).
- OIDC tokens are unsigned (`alg=none`) JWTs — the emulator cannot mint
  Google-signed tokens — and OAuth tokens are placeholders.
- App Engine 503 "slow down delivery" pacing is not modelled; the emulator
  dispatches immediately and treats any non-2xx as a retryable failure.

## Development

```bash
make build   # build the binary
make test    # run the test suite (spins up the emulator in-process)
make cover   # run tests with the race detector and print total coverage
make vet     # go vet
make hooks   # install the lefthook git hooks
```

The test suite is kept at **100% statement coverage**, enforced in CI.

[lefthook](https://lefthook.dev) runs `gofmt`/`go vet` on commit and the test
suite on push. Install the hooks once with `make hooks` (requires the
`lefthook` binary, e.g. `brew install lefthook` or
`go install github.com/evilmartians/lefthook@latest`).

## License

[MIT](LICENSE)
