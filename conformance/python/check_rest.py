#!/usr/bin/env python3
"""Drive a running emulator over REST with the official google-cloud-tasks client.

The REST surface is transcoded from the google.api.http annotations rather than
hand-written, so what matters is whether the *official* client's REST transport
-- which builds the URLs, the query strings and the JSON itself -- can drive it
end to end, and whether a task created that way really reaches its target.

Usage: REST_ADDR=http://localhost:8124 python3 check_rest.py
"""

import http.server
import os
import queue
import sys
import threading
import time

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound
from google.auth.credentials import AnonymousCredentials
from google.cloud import tasks_v2
from googleapiclient.discovery import build

REST_ADDR = os.environ.get("REST_ADDR", "http://localhost:8124")
# The address the emulator should call back on. When the emulator runs in a
# container this must be an address reachable from inside it.
TARGET_HOST = os.environ.get("TARGET_HOST", "127.0.0.1")

PARENT = "projects/conformance/locations/us-central1"
QUEUE_NAME = f"{PARENT}/queues/python-rest-check"
DISCOVERY_QUEUE_NAME = f"{PARENT}/queues/python-discovery-check"

received: "queue.Queue" = queue.Queue()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        received.put((dict(self.headers), self.rfile.read(length)))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


def start_target() -> int:
    server = http.server.HTTPServer(("0.0.0.0", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    port = start_target()
    target_url = f"http://{TARGET_HOST}:{port}/handle"

    # No credentials: the emulator authenticates nobody, and the REST transport
    # would otherwise go looking for application default credentials.
    client = tasks_v2.CloudTasksClient(
        transport="rest",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=REST_ADDR),
    )

    client.create_queue(parent=PARENT, queue={"name": QUEUE_NAME})

    queues = list(client.list_queues(parent=PARENT))
    if not any(q.name == QUEUE_NAME for q in queues):
        fail(f"created queue is missing from ListQueues: {[q.name for q in queues]}")

    # UpdateQueue puts the field mask in the query string, which is the part of
    # REST transcoding that is easiest to get wrong.
    updated = client.update_queue(
        queue={"name": QUEUE_NAME, "rate_limits": {"max_dispatches_per_second": 7}},
        update_mask={"paths": ["rate_limits"]},
    )
    if updated.rate_limits.max_dispatches_per_second != 7:
        fail(f"UpdateQueue did not apply the mask: {updated.rate_limits}")

    task = client.create_task(
        parent=QUEUE_NAME,
        task={
            "http_request": {
                "url": target_url,
                "http_method": tasks_v2.HttpMethod.POST,
                "body": b'{"hello":"rest"}',
                "headers": {"Content-Type": "application/json"},
            }
        },
    )
    if not task.name.startswith(QUEUE_NAME + "/tasks/"):
        fail(f"unexpected task name {task.name}")

    try:
        headers, body = received.get(timeout=30)
    except queue.Empty:
        fail(f"the emulator never dispatched the REST-created task to {target_url}")

    if body != b'{"hello":"rest"}':
        fail(f"dispatched body = {body!r}")
    if headers.get("X-CloudTasks-QueueName") != "python-rest-check":
        fail(f"X-CloudTasks-QueueName = {headers.get('X-CloudTasks-QueueName')!r}")
    if headers.get("Content-Type") != "application/json":
        fail(f"Content-Type = {headers.get('Content-Type')!r}")

    # A successfully dispatched task is removed from the queue. The emulator
    # only records that once it has read our 2xx, which is necessarily after
    # the handler handed us the request, so poll rather than assert once.
    deadline = time.monotonic() + 15
    while True:
        remaining = list(client.list_tasks(parent=QUEUE_NAME))
        if not remaining:
            break
        if time.monotonic() > deadline:
            fail(f"queue still holds {[t.name for t in remaining]} after a 2xx dispatch")
        time.sleep(0.2)

    # Errors have to arrive as the status the client can raise, not as a 200
    # with an error-shaped body.
    try:
        client.get_queue(name=f"{PARENT}/queues/does-not-exist")
        fail("GetQueue on a missing queue did not raise")
    except NotFound:
        pass

    client.delete_queue(name=QUEUE_NAME)

    check_discovery_client()
    print(f"OK: google-cloud-tasks REST transport against {REST_ADDR}")


def check_discovery_client() -> None:
    """Drive the same surface with google-api-python-client.

    A whole class of tooling (goblet, gcloud-style scripts) is built on the
    discovery client rather than on a GAPIC. It carries its own copy of the
    Cloud Tasks discovery document, so the only thing it needs from us is that
    the endpoint behaves like the real one.
    """
    service = build(
        "cloudtasks",
        "v2",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": REST_ADDR},
    )
    queues = service.projects().locations().queues()

    created = queues.create(
        parent=PARENT, body={"name": DISCOVERY_QUEUE_NAME}
    ).execute()
    if created.get("name") != DISCOVERY_QUEUE_NAME:
        fail(f"discovery client created {created!r}")

    listed = queues.list(parent=PARENT).execute().get("queues", [])
    if not any(q["name"] == DISCOVERY_QUEUE_NAME for q in listed):
        fail(f"created queue is missing from the discovery client's list: {listed!r}")

    task = (
        queues.tasks()
        .create(
            parent=DISCOVERY_QUEUE_NAME,
            body={"task": {"httpRequest": {"url": "http://127.0.0.1:1/never-answered"}}},
        )
        .execute()
    )
    if not task["name"].startswith(DISCOVERY_QUEUE_NAME + "/tasks/"):
        fail(f"discovery client created task {task!r}")

    queues.delete(name=DISCOVERY_QUEUE_NAME).execute()
    print(f"OK: google-api-python-client (discovery) against {REST_ADDR}")


if __name__ == "__main__":
    main()
