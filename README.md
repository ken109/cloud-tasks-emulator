# cloud-tasks-emulator

A local, in-memory emulator for [Google Cloud Tasks](https://cloud.google.com/tasks),
in the spirit of the official Cloud Pub/Sub emulator (`gcloud beta emulators pubsub`).

It serves the real **`google.cloud.tasks.v2`** *and* **`google.cloud.tasks.v2beta3`**
gRPC APIs from a single shared engine, so the official Cloud Tasks client
libraries (Go, Python, Node.js, Java, ...) can talk to it unchanged on either
version — you just point them at the emulator's address over an insecure
connection. Tasks are actually **dispatched**: when a task becomes due, the
emulator makes the HTTP request to your target and applies the queue's
retry/backoff and rate-limit policy, just like production.

> Not affiliated with Google. Intended for local development and testing only —
> all state is in memory and lost on restart.

## Features

- Full v2 **and** v2beta3 gRPC surface: `CreateQueue` / `GetQueue` / `ListQueues` /
  `UpdateQueue` / `DeleteQueue` / `PurgeQueue` / `PauseQueue` / `ResumeQueue`,
  `CreateTask` / `GetTask` / `ListTasks` / `DeleteTask` / `RunTask`, and the IAM
  methods — both versions served from one shared engine.
- Real HTTP task dispatch for both **HTTP targets** (`HttpRequest`) and
  **App Engine targets** (`AppEngineHttpRequest`).
- Full Cloud Tasks request headers, with the correct prefix per target type:
  - HTTP targets: `X-CloudTasks-QueueName`, `-TaskName`, `-TaskRetryCount`,
    `-TaskExecutionCount`, `-TaskETA`, and on retries `-TaskPreviousResponse`
    and `-TaskRetryReason`; `User-Agent: Google-Cloud-Tasks`.
  - App Engine targets: the same fields with the `X-AppEngine-` prefix, plus
    `X-AppEngine-FailFast`; `User-Agent: AppEngine-Google; (+http://code.google.com/appengine)`.
- `Authorization: Bearer` tokens generated from a task's `OidcToken` /
  `OauthToken`. The OIDC token is a **real RS256-signed JWT**, and the emulator
  publishes the matching public key at `/.well-known/openid-configuration`,
  `/jwks` and `/certs`, so your target can *verify* the token instead of having
  to skip verification in local runs.
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
- **v2beta3 extras**: queue-level HTTP target overrides (`HttpTarget` with
  `UriOverride`, header overrides and auth), per-queue `task_ttl` /
  `tombstone_ttl`, `PULL` queues with `PullMessage` tasks (stored, not
  auto-dispatched), and `QueueStats` (`tasks_count`).
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
| `-queue` (repeatable) | `INITIAL_QUEUES` (comma-separated) | _(none)_ | Full resource name of a queue to create at startup, e.g. `projects/dev/locations/here/queues/myqueue` |
| `-openid-issuer` | `CLOUD_TASKS_OPENID_ISSUER` | _(empty)_ | Base URL to serve the OpenID discovery endpoints on, e.g. `http://localhost:8980`. Also becomes the `iss` claim of dispatched OIDC tokens |
| `-hard-reset-on-purge-queue` | `CLOUD_TASKS_HARD_RESET_ON_PURGE_QUEUE` | `false` | Drop task-name history when a queue is purged, so purged names can be reused immediately |

```bash
docker run --rm -p 8123:8123 \
  -e CLOUD_TASKS_EMULATOR_HOST=0.0.0.0 \
  -e CLOUD_TASKS_APP_ENGINE_HOST=http://host.docker.internal:8080 \
  ghcr.io/ken109/cloud-tasks-emulator:latest
```

### Verifying OIDC tokens locally

Production Cloud Tasks attaches a Google-signed OIDC token when a task carries
an `OidcToken`, and a Cloud Run service deployed without
`--allow-unauthenticated` rejects the request unless that token verifies. To
reproduce that locally, give the emulator an issuer URL:

```bash
docker run --rm -p 8123:8123 -p 8980:8980 \
  ghcr.io/ken109/cloud-tasks-emulator:latest \
  -host 0.0.0.0 -openid-issuer http://localhost:8980
```

Dispatched tokens are then signed with RS256 and carry `iss:
http://localhost:8980`, and the public key is published at:

| Path | Content |
|------|---------|
| `/.well-known/openid-configuration` | OpenID discovery document |
| `/jwks` | Public key as a JWK set |
| `/certs` | `{kid: PEM certificate}`, the shape Google serves at `/oauth2/v1/certs` |

Point your verifier's issuer/JWKS URL at the emulator and the same verification
code you run in production passes locally, unchanged. Without `-openid-issuer`
the tokens are still signed, but carry the production `iss`
(`https://accounts.google.com`), whose keys the emulator obviously cannot own.

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

If you're already in Go, run the emulator in-process and skip Docker entirely.
`Register` wires up both the v2 and v2beta3 services:

```go
import (
    "google.golang.org/grpc"
    cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
    "github.com/ken109/cloud-tasks-emulator/emulator"
)

lis, _ := net.Listen("tcp", "127.0.0.1:0")
gs := grpc.NewServer()
emulator.New(emulator.Config{}).Register(gs) // serves v2 + v2beta3
go gs.Serve(lis)
defer gs.Stop()

conn, _ := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
client, _ := cloudtasks.NewClient(ctx, option.WithGRPCConn(conn))
```

`emulator.Config` lets you tune `DefaultAppEngineHost`, `TaskTTL` and
`TombstoneTTL` (handy for shortening the lifecycle in tests).

## Migrating from aertje/cloud-tasks-emulator

[aertje/cloud-tasks-emulator](https://github.com/aertje/cloud-tasks-emulator) is
the emulator most projects reach for first. This one is a from-scratch
implementation, not a fork, but the command-line surface is deliberately
compatible: **swapping the image name is usually the whole migration.**

```diff
 services:
   cloud-tasks:
-    image: ghcr.io/aertje/cloud-tasks-emulator:latest
+    image: ghcr.io/ken109/cloud-tasks-emulator:latest
     command: -host 0.0.0.0 -port 8123 -queue projects/dev/locations/here/queues/default
```

### Flag mapping

| aertje flag | Here | Notes |
|-------------|------|-------|
| `-host` / `-port` | same | Same defaults (`localhost` / `8123`) |
| `-queue` (repeatable) | same | `INITIAL_QUEUES` env fallback also matches |
| `-hard-reset-on-purge-queue` | same | Same meaning |
| `-openid-issuer` | same | Same discovery paths: `/.well-known/openid-configuration`, `/jwks`, `/certs` |
| — | `-app-engine-host` | Default host for App Engine targets |

### What you gain

- **`google.cloud.tasks.v2beta3` as well as `v2`**, from one shared engine.
  aertje moved off v2beta3 to serve v2 only
  ([aertje#8](https://github.com/aertje/cloud-tasks-emulator/issues/8)), so a
  client pinned to v2beta3 has nowhere to point. Queue-level HTTP target
  overrides, per-queue `task_ttl` / `tombstone_ttl`, `PULL` queues and
  `QueueStats` come with that surface.
- **A published image that matches the source.** Images are built from the same
  commit in the same workflow that runs the tests, and the release job starts
  the built image and drives it with the official client before publishing —
  the failure mode reported in
  [aertje#113](https://github.com/aertje/cloud-tasks-emulator/issues/113)
  (`/certs` and the Docker image out of sync with master) cannot ship here.
- **`UpdateQueue`**, pagination on `ListQueues` / `ListTasks`, the IAM methods,
  `CreateTask` resource-limit validation, task-name tombstones and task TTL.
- **The full retry-header set** including `X-CloudTasks-TaskPreviousResponse`
  and `-TaskRetryReason`, and the `X-AppEngine-` prefixed variants for App
  Engine targets.
- **An in-process Go API** (`emulator.New(...).Register(grpcServer)`), so Go
  tests can skip Docker entirely.
- **100% statement coverage, enforced in CI**, and a test suite that verifies
  dispatched OIDC tokens against the published JWKS and certificate rather than
  just asserting a token is present.

### Behaviour differences to know about

- Purged task names stay reserved unless you pass
  `-hard-reset-on-purge-queue`; aertje's default is closer to a hard reset.
- OIDC tokens are signed with a key generated per process, so restarting the
  emulator rotates the key. Fetch the JWKS at verification time rather than
  caching it across restarts.
- There is no `-openid-issuer`-less discovery endpoint: with no issuer
  configured, nothing is served over HTTP and tokens carry the production `iss`.

## Behaviour notes

- **State is in memory.** Restarting the emulator clears all queues and tasks.
- **Successful dispatch** (HTTP 2xx) removes the task from the queue.
- **Failed dispatch** is retried per the queue's `RetryConfig` until
  `max_attempts` or `max_retry_duration` is reached, then the task is dropped.
- `RunTask` forces an immediate dispatch attempt regardless of `schedule_time`.
- **Purged task names stay reserved** for the tombstone window, as in
  production. Pass `-hard-reset-on-purge-queue` if your tests purge a queue and
  immediately recreate the same fixed task names.
- App Engine targets need a reachable host: set one via the task's
  `app_engine_routing.host`, the queue's `app_engine_routing_override.host`, or
  the `-app-engine-host` flag.
- IAM methods (`GetIamPolicy` / `SetIamPolicy` / `TestIamPermissions`) are
  supported: policies are stored in memory per queue and round-trip like the
  Cloud Pub/Sub emulator, but are never enforced.

## Scope and limitations

- Implements the **`google.cloud.tasks.v2`** and **`google.cloud.tasks.v2beta3`**
  surfaces as published by the official Go client
  (`cloud.google.com/go/cloudtasks/apiv2` and `.../apiv2beta3`). The `BufferTask`
  RPC and pull-lease operations (`LeaseTasks` / `AcknowledgeTask`, which live in
  `v2beta2`) are outside these surfaces and are not implemented — `PULL` queues
  and `PullMessage` tasks are stored but not leasable.
- IAM policies are stored and returned but never **enforced** (same as the
  Cloud Pub/Sub emulator).
- All state is in memory; nothing is persisted across restarts.
- `ListQueues` ignores the `filter` argument (all queues in the parent are
  returned, paginated).
- OIDC tokens are signed by the emulator's own key, not by Google. Verify them
  against the emulator's issuer (`-openid-issuer`), not against Google's certs.
  OAuth tokens are placeholders.
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
