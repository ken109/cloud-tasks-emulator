# CLAUDE.md

Guidance for working in this repository.

## Project

`cloud-tasks-emulator` is an in-memory emulator for Google Cloud Tasks that
serves both the `google.cloud.tasks.v2` and `google.cloud.tasks.v2beta3` gRPC
APIs, modelled after the official Cloud Pub/Sub emulator. It actually dispatches
tasks over HTTP with retries, backoff, rate limiting and scheduling.

Architecture: a version-agnostic **core** engine holds all behaviour and state,
and thin per-version **adapters** translate each protobuf surface to/from the
core's neutral types. Add new behaviour in `core`; keep adapters as pure
translation.

Layout:

- `main.go` — flag/env parsing and gRPC server bootstrap.
- `core/` — neutral engine: `types.go` (neutral resources), `engine.go`
  (CRUD/validation/pagination/IAM), `queue.go` (scheduling, retries, backoff,
  rate limiting, tombstones, TTL), `dispatch.go` (HTTP delivery, headers,
  tokens), `naming.go` (resource-name parsing).
- `emulator/emulator.go` — public API: `New(Config)` + `Register(*grpc.Server)`.
- `emulator/v2.go`, `emulator/v2beta3.go` — the two gRPC adapters.
- `emulator/conv.go` — shared proto↔core helpers.
- `emulator/rest.go` — `RESTHandler()`, wiring both services into the transcoder.
- `rest/` — REST/JSON transcoder. Knows nothing about Cloud Tasks: it derives
  its routes from the `google.api.http` annotations on a `grpc.ServiceDesc` and
  invokes the generated gRPC handler, so REST and gRPC share one code path.
  Adding an API method needs no work here.
- `conformance/{python,node}/` — checks that drive a *running* emulator with
  the official client libraries. The Go suite shares the emulator's own types,
  so only these can catch wire-level mismatches (they found the system-header
  casing bug). `check_oidc.py` additionally verifies a dispatched token with a
  real JWT library, and `check_rest.py` / `check_rest.mjs` drive the REST
  surface with the clients' own REST transports — the only check that can catch
  a transcoding mistake. CI runs them against both a built binary and the image.
- `docs/` — guides for paths that need more than a README section.
- `emulator/example_test.go` — the pkg.go.dev examples; they run in CI, so Go
  snippets cannot rot.
- `docs_test.go` — checks the README's flag tables against the real flag set.
  `registerFlags` in `main.go` is the single source of truth; add a flag there
  and the test tells you which docs to update.

## Commands

```bash
make build        # build the binary
make test         # go test ./...
make cover        # go test -race with coverage summary
make vet          # go vet ./...
make lint         # golangci-lint run
make conformance  # drive a built emulator with the official Python/Node clients
make run          # build and run on localhost:8123
make docker       # build the docker image
make hooks        # install lefthook git hooks
```

Always run `make test`, `make lint` and `gofmt -l .` before committing. Keep the
suite at **100% statement coverage** (`make cover`); CI fails the build below
100%. [lefthook](https://lefthook.dev) enforces `gofmt`/`go vet` on commit and
the tests plus lint on push.

Changing anything on the wire — request headers, token shape, the gRPC or REST
surface — means updating the conformance checks too, since that is the only
place the official clients get a vote.

## Commit conventions

This repo uses [Conventional Commits](https://www.conventionalcommits.org/).

Format: `<type>(<optional scope>): <description>`

Common types:

- `feat:` — a new feature
- `fix:` — a bug fix
- `docs:` — documentation only
- `test:` — adding or fixing tests
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `perf:` — performance improvement
- `build:` — build system, Dockerfile, or dependency changes
- `ci:` — CI configuration changes
- `chore:` — other maintenance

Rules:

- Use the imperative mood ("add", not "added"/"adds").
- Keep the description concise and lower-case; no trailing period.
- Breaking changes: add `!` after the type/scope (e.g. `feat!:`) and/or a
  `BREAKING CHANGE:` footer.
- Example: `feat(dispatch): add OIDC token header for HTTP targets`

Release tags are `v*` (semver); pushing a `v*` tag publishes a versioned image.
