#!/usr/bin/env python3
"""Verify a dispatched OIDC token the way a real task handler would.

docs/oidc.md claims your production verification code runs unchanged against
the emulator. This checks that claim with a general-purpose JWT library
(PyJWT), discovering the signing key through the emulator's published OpenID
endpoints rather than being told it -- exactly the path a Cloud Run handler
takes. The emulator's own Go tests verify the signature with crypto/rsa, which
proves the maths but not that the documents are shaped the way a library
expects.

Usage:
  EMULATOR_ADDR=localhost:8123 OPENID_ISSUER=http://127.0.0.1:8980 \
    python3 check_oidc.py
"""

import http.server
import os
import queue
import sys
import threading
import urllib.request

import grpc
import jwt
from google.cloud import tasks_v2
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport
from jwt import PyJWKClient

EMULATOR_ADDR = os.environ.get("EMULATOR_ADDR", "localhost:8123")
OPENID_ISSUER = os.environ.get("OPENID_ISSUER", "http://127.0.0.1:8980")
TARGET_HOST = os.environ.get("TARGET_HOST", "127.0.0.1")

PARENT = "projects/conformance/locations/us-central1"
QUEUE_NAME = f"{PARENT}/queues/oidc-check"
SERVICE_ACCOUNT = "tasks@conformance.iam.gserviceaccount.com"

received: "queue.Queue" = queue.Queue()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        received.put(self.headers.get("Authorization"))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    server = http.server.HTTPServer(("0.0.0.0", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    audience = f"http://{TARGET_HOST}:{port}"
    target_url = f"{audience}/tasks/process"

    channel = grpc.insecure_channel(EMULATOR_ADDR)
    grpc.channel_ready_future(channel).result(timeout=30)
    client = tasks_v2.CloudTasksClient(transport=CloudTasksGrpcTransport(channel=channel))
    client.create_queue(parent=PARENT, queue={"name": QUEUE_NAME})

    client.create_task(
        parent=QUEUE_NAME,
        task={
            "http_request": {
                "url": target_url,
                "http_method": tasks_v2.HttpMethod.POST,
                "oidc_token": {
                    "service_account_email": SERVICE_ACCOUNT,
                    "audience": audience,
                },
            }
        },
    )

    try:
        authorization = received.get(timeout=30)
    except queue.Empty:
        fail(f"the emulator never dispatched the task to {target_url}")
    if not authorization or not authorization.startswith("Bearer "):
        fail(f"Authorization header = {authorization!r}")
    token = authorization.removeprefix("Bearer ")

    # The discovery document is what a verifier reads first; go-oidc and friends
    # reject it outright if `issuer` does not match the URL they asked for.
    with urllib.request.urlopen(f"{OPENID_ISSUER}/.well-known/openid-configuration") as resp:
        import json

        discovery = json.load(resp)
    if discovery.get("issuer") != OPENID_ISSUER:
        fail(f"discovery issuer = {discovery.get('issuer')!r}, want {OPENID_ISSUER!r}")
    jwks_uri = discovery.get("jwks_uri")
    if not jwks_uri:
        fail("discovery document has no jwks_uri")

    # Resolve the key by kid through the published JWKS, then verify signature,
    # audience and issuer -- the same three things a handler checks.
    signing_key = PyJWKClient(jwks_uri).get_signing_key_from_jwt(token)
    claims = jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        audience=audience,
        issuer=OPENID_ISSUER,
    )

    if claims.get("email") != SERVICE_ACCOUNT:
        fail(f"email claim = {claims.get('email')!r}")
    if claims.get("email_verified") is not True:
        fail(f"email_verified claim = {claims.get('email_verified')!r}")
    if claims.get("sub") != SERVICE_ACCOUNT:
        fail(f"sub claim = {claims.get('sub')!r}")
    if "exp" not in claims or "iat" not in claims:
        fail(f"missing exp/iat in {claims}")

    # A token that should not verify must not: a wrong audience is the mistake
    # a handler is actually protecting against.
    try:
        jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience="http://someone-else.example",
            issuer=OPENID_ISSUER,
        )
    except jwt.InvalidAudienceError:
        pass
    else:
        fail("a token with the wrong audience verified, which means nothing is being checked")

    client.delete_queue(name=QUEUE_NAME)
    print(f"OK: OIDC token from {EMULATOR_ADDR} verifies against {OPENID_ISSUER}")


if __name__ == "__main__":
    main()
