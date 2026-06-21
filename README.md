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
- Cloud Tasks request headers injected (`X-CloudTasks-QueueName`,
  `X-CloudTasks-TaskName`, `X-CloudTasks-TaskRetryCount`,
  `X-CloudTasks-TaskExecutionCount`, `X-CloudTasks-TaskETA`).
- Scheduling via `schedule_time`, retries with exponential backoff
  (`RetryConfig`), rate limiting and bounded concurrency (`RateLimits`).
- Queue pause/resume/purge semantics.
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

## Behaviour notes

- **State is in memory.** Restarting the emulator clears all queues and tasks.
- **Successful dispatch** (HTTP 2xx) removes the task from the queue.
- **Failed dispatch** is retried per the queue's `RetryConfig` until
  `max_attempts` or `max_retry_duration` is reached, then the task is dropped.
- `RunTask` forces an immediate dispatch attempt regardless of `schedule_time`.
- App Engine targets need a reachable host: set one via the task's
  `app_engine_routing.host`, the queue's `app_engine_routing_override.host`, or
  the `-app-engine-host` flag.
- IAM methods (`GetIamPolicy` / `SetIamPolicy` / `TestIamPermissions`) are not
  implemented and return `Unimplemented`.

## Development

```bash
make build   # build the binary
make test    # run the test suite (spins up the emulator in-process)
make vet     # go vet
```

## License

[MIT](LICENSE)
