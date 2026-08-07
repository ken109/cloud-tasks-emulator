#!/usr/bin/env python3
"""Drive a running emulator with the official google-cloud-tasks client.

This is the property a user actually depends on: that the *official* client
library, unmodified, can talk to the emulator and that a created task really
arrives at its target carrying the Cloud Tasks system headers. Unit tests in
the emulator's own language cannot prove that -- they share its types.

Usage: EMULATOR_ADDR=localhost:8123 python3 check.py
"""

import http.server
import os
import queue
import socket
import sys
import threading
import time

import grpc
from google.cloud import tasks_v2
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

EMULATOR_ADDR = os.environ.get("EMULATOR_ADDR", "localhost:8123")
# The address the emulator should call back on. When the emulator runs in a
# container this must be an address reachable from inside it.
TARGET_HOST = os.environ.get("TARGET_HOST", "127.0.0.1")

PARENT = "projects/conformance/locations/us-central1"
QUEUE_NAME = f"{PARENT}/queues/python-check"

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

    channel = grpc.insecure_channel(EMULATOR_ADDR)
    grpc.channel_ready_future(channel).result(timeout=30)
    client = tasks_v2.CloudTasksClient(transport=CloudTasksGrpcTransport(channel=channel))

    client.create_queue(parent=PARENT, queue={"name": QUEUE_NAME})

    queues = list(client.list_queues(parent=PARENT))
    if not any(q.name == QUEUE_NAME for q in queues):
        fail(f"created queue is missing from ListQueues: {[q.name for q in queues]}")

    task = client.create_task(
        parent=QUEUE_NAME,
        task={
            "http_request": {
                "url": target_url,
                "http_method": tasks_v2.HttpMethod.POST,
                "body": b'{"hello":"world"}',
                # Deliberately lower-cased: header names are case-insensitive,
                # and the emulator must not overwrite it with its
                # application/octet-stream default. The Node check sends the
                # canonical spelling, so between them both paths are covered.
                "headers": {"content-type": "application/json"},
            }
        },
    )
    if not task.name.startswith(QUEUE_NAME + "/tasks/"):
        fail(f"unexpected task name {task.name}")

    try:
        headers, body = received.get(timeout=30)
    except queue.Empty:
        fail(f"the emulator never dispatched the task to {target_url}")

    if body != b'{"hello":"world"}':
        fail(f"dispatched body = {body!r}")
    if headers.get("X-CloudTasks-QueueName") != "python-check":
        fail(f"X-CloudTasks-QueueName = {headers.get('X-CloudTasks-QueueName')!r}")
    if headers.get("X-CloudTasks-TaskName") != task.name.rsplit("/", 1)[-1]:
        fail(f"X-CloudTasks-TaskName = {headers.get('X-CloudTasks-TaskName')!r}")
    for header in ("X-CloudTasks-TaskRetryCount", "X-CloudTasks-TaskExecutionCount", "X-CloudTasks-TaskETA"):
        if header not in headers:
            fail(f"missing {header} on the dispatched request")
    if headers.get("Content-Type") != "application/json":
        fail(f"Content-Type = {headers.get('Content-Type')!r}")

    # An HTTP task that sets no content type must arrive without one: the
    # reference is explicit that "Content-Type won't be set by Cloud Tasks" for
    # HTTP targets, unlike App Engine ones.
    client.create_task(
        parent=QUEUE_NAME,
        task={
            "http_request": {
                "url": target_url,
                "http_method": tasks_v2.HttpMethod.POST,
                "body": b"bare",
            }
        },
    )
    try:
        bare_headers, bare_body = received.get(timeout=30)
    except queue.Empty:
        fail("the emulator never dispatched the task with no content type")
    if bare_body != b"bare":
        fail(f"dispatched body = {bare_body!r}")
    if "Content-Type" in bare_headers:
        fail(f"Cloud Tasks does not set Content-Type on HTTP targets, got {bare_headers['Content-Type']!r}")

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

    client.delete_queue(name=QUEUE_NAME)
    print(f"OK: google-cloud-tasks (python) against {EMULATOR_ADDR}")


if __name__ == "__main__":
    socket.setdefaulttimeout(30)
    main()
