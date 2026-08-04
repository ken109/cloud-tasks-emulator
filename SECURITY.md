# Security policy

## This is a development tool

The emulator is for local development and testing. **Do not run it anywhere
untrusted traffic can reach it, and never point production at it.** By design
it:

- **accepts every request.** There is no authentication on the gRPC API. Anyone
  who can reach the port can create a task, and a task is an outbound HTTP
  request to a URL of the caller's choosing — an open request forwarder if the
  port is exposed.
- **stores IAM policies without enforcing them.** `SetIamPolicy` round-trips
  like the Cloud Pub/Sub emulator does; it never denies anything.
- **mints its own OIDC tokens.** Tokens are signed by a key the emulator
  generates at startup, not by Google. A service that trusts the emulator's
  issuer trusts anyone who can reach the emulator. Configure the issuer only in
  local and test environments.
- **keeps everything in memory** and unencrypted, and loses it on restart.

Binding to `localhost` (the default) is the safe posture. `-host 0.0.0.0` is
there for containers, where the container's network boundary is what limits
reachability — do not combine it with a published port on a shared host.

## Reporting a vulnerability

Report privately through
[GitHub Security Advisories](https://github.com/ken109/cloud-tasks-emulator/security/advisories/new)
rather than opening a public issue.

Please include what you did, what happened, and what you expected. A failing
test or a short reproduction is ideal.

Expect an acknowledgement within a week. Given what this tool is, the bar for
"vulnerability" is a flaw that harms a developer running it as intended —
arbitrary code execution, a crash triggerable by a task payload, a dependency
advisory that reaches a reachable code path. The behaviours listed above are
documented design decisions, not vulnerabilities.

## Supported versions

The latest release. Fixes ship in a new release rather than as patches to
older tags.

## Supply chain

Published images carry an SBOM and attested build provenance, so you can check
an image came from this repository's workflow:

```bash
gh attestation verify oci://ghcr.io/ken109/cloud-tasks-emulator:latest --owner ken109
```

Every published image is built and smoke-tested in the same workflow run that
runs the test suite, on the same commit. CI runs `govulncheck` on every change,
and Dependabot tracks Go modules, GitHub Actions, the base image and the
conformance clients.
