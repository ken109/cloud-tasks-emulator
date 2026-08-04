# Contributing

Bug reports, especially "production does X, the emulator does Y", are the most
useful thing you can send. Fidelity to the real Cloud Tasks API is the whole
point of the project.

## Getting set up

```bash
git clone https://github.com/ken109/cloud-tasks-emulator
cd cloud-tasks-emulator
make test
make hooks   # optional: gofmt/vet on commit, tests and lint on push
```

Go 1.26 or newer. `make conformance` additionally needs Python and Node with
the client libraries installed:

```bash
pip install -r conformance/python/requirements.txt
(cd conformance/node && npm ci)
make conformance
```

## Before you open a pull request

```bash
make test lint
gofmt -l .        # must print nothing
```

CI runs the same, plus `govulncheck`, the conformance checks against a built
binary, and a smoke test of the release image.

## The two rules that are not obvious

### 1. Coverage stays at 100%

`make cover` must report `100.0%`; CI fails the build otherwise. This is
unusual, and deliberate: the emulator is small, and a line nothing exercises is
a line whose fidelity to production nobody has checked.

If a line is genuinely unreachable, that is usually a sign the code should be
restructured rather than excluded. Where an error is impossible in practice
(`json.Marshal` of a fixed map, say), prefer an explicit discard over a branch
that cannot be taken:

```go
out, _ := json.Marshal(doc)   // not: if err != nil { ... }
```

Where an error *is* possible but awkward to trigger, add a seam a test can
drive — `lookupEnv` and `logFatalf` in `main.go` exist for exactly this.

### 2. Anything on the wire needs a conformance check

The Go test suite shares the emulator's own types, so it cannot see a mismatch
between what the emulator sends and what a client expects. It did not, for
instance, catch the emulator sending `X-Cloudtasks-Queuename` where production
sends `X-CloudTasks-QueueName` — Go canonicalises header names, so both sides
of an in-process test agreed with each other and disagreed with production.

So: if you change request headers, token shape, the gRPC surface or anything
else a client observes, update `conformance/` too. Those scripts drive a
running emulator with the unmodified official client libraries, which is the
only place they get a vote.

## Where things live

- `core/` — the version-agnostic engine. All behaviour goes here.
- `emulator/` — the two gRPC adapters (v2, v2beta3) and the public Go API. Keep
  these as pure translation.
- `conformance/` — official-client checks against a running emulator.
- `docs/` — guides for paths that need more than a README section.

Adding behaviour means `core/`; adding a field to one API version means the
adapter for that version. If you find yourself putting a rule in an adapter,
it probably belongs in `core`.

## Documentation that checks itself

Docs here are wired to fail loudly rather than rot:

- `docs_test.go` compares the README's configuration and migration tables
  against the real flag set. `registerFlags` in `main.go` is the single source
  of truth — add a flag there and the test names the tables to update.
- `emulator/example_test.go` holds the pkg.go.dev examples. They compile and
  run as part of the suite.
- `conformance/python/check_oidc.py` verifies a dispatched token with a real
  JWT library, so `docs/oidc.md` cannot promise something that stopped working.

Prefer adding to one of those over adding prose that nothing verifies.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/):
`<type>(<optional scope>): <description>`, imperative mood, lower-case, no
trailing period. `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`,
`ci`, `chore`. Breaking changes get a `!` and/or a `BREAKING CHANGE:` footer.

Explain *why* in the body. "What" is visible in the diff.

## Releasing

Maintainers only. Push a `v*` tag; the publish workflow runs the full CI suite,
pushes the multi-arch image with an SBOM and attested provenance, and opens a
GitHub Release.
