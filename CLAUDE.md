# CLAUDE.md

Guidance for working in this repository.

## Project

`cloud-tasks-emulator` is an in-memory emulator for the Google Cloud Tasks v2
gRPC API (`google.cloud.tasks.v2`), modelled after the official Cloud Pub/Sub
emulator. It implements the queue/task RPCs and actually dispatches tasks over
HTTP with retries, backoff, rate limiting and scheduling.

Layout:

- `main.go` — flag/env parsing and gRPC server bootstrap.
- `emulator/server.go` — the `CloudTasksServer` RPC implementations.
- `emulator/queue.go` — queue runtime: scheduling, retries, backoff,
  rate limiting, defaults.
- `emulator/dispatch.go` — HTTP delivery for HTTP / App Engine targets.
- `emulator/iam.go` — in-memory IAM policy methods.
- `emulator/naming.go` — resource-name parsing/validation.

## Commands

```bash
make build   # build the binary
make test    # go test ./...
make cover   # go test -race with coverage summary
make vet     # go vet ./...
make run     # build and run on localhost:8123
make docker  # build the docker image
make hooks   # install lefthook git hooks
```

Always run `make test` and `gofmt -l .` before committing. Keep the suite at
**100% statement coverage** (`make cover`); CI fails the build below 100%.
[lefthook](https://lefthook.dev) enforces `gofmt`/`go vet` on commit and the
tests on push.

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
