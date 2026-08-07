#!/usr/bin/env node
// Drive a running emulator over REST with the official @google-cloud/tasks
// client, which builds the URLs and JSON itself when told to fall back to REST.
//
// Usage: REST_ADDR=127.0.0.1:8124 node check_rest.mjs

import http from "node:http";
import tasks from "@google-cloud/tasks";
import { PassThroughClient } from "google-auth-library";

const REST_ADDR = process.env.REST_ADDR ?? "127.0.0.1:8124";
// The address the emulator should call back on. When the emulator runs in a
// container this must be an address reachable from inside it.
const TARGET_HOST = process.env.TARGET_HOST ?? "127.0.0.1";

const PARENT = "projects/conformance/locations/us-central1";
const QUEUE_NAME = `${PARENT}/queues/node-rest-check`;

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

// startTarget resolves with the listening port and a promise for the first
// request it receives.
function startTarget() {
  let resolveRequest;
  const request = new Promise((resolve) => {
    resolveRequest = resolve;
  });
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      resolveRequest({ headers: req.headers, body: Buffer.concat(chunks) });
      res.writeHead(200);
      res.end();
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "0.0.0.0", () => resolve({ port: server.address().port, request, server }));
  });
}

function withTimeout(promise, ms, message) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}

const [host, port] = REST_ADDR.split(":");
const { port: targetPort, request, server } = await startTarget();
const targetUrl = `http://${TARGET_HOST}:${targetPort}/handle`;

const client = new tasks.v2.CloudTasksClient({
  fallback: "rest",
  protocol: "http",
  apiEndpoint: host,
  port: Number(port),
  // The emulator authenticates nobody, so skip the credential lookup that
  // would otherwise reach for application default credentials.
  authClient: new PassThroughClient(),
});

await client.createQueue({ parent: PARENT, queue: { name: QUEUE_NAME } });

const [queues] = await client.listQueues({ parent: PARENT });
if (!queues.some((q) => q.name === QUEUE_NAME)) {
  fail(`created queue is missing from listQueues: ${queues.map((q) => q.name)}`);
}

const [task] = await client.createTask({
  parent: QUEUE_NAME,
  task: {
    httpRequest: {
      url: targetUrl,
      httpMethod: "POST",
      body: Buffer.from('{"hello":"rest"}'),
      headers: { "Content-Type": "application/json" },
    },
  },
});
if (!task.name.startsWith(`${QUEUE_NAME}/tasks/`)) {
  fail(`unexpected task name ${task.name}`);
}

const received = await withTimeout(
  request,
  30_000,
  `the emulator never dispatched the REST-created task to ${targetUrl}`,
).catch((err) => fail(err.message));

if (received.body.toString() !== '{"hello":"rest"}') {
  fail(`dispatched body = ${received.body.toString()}`);
}
// Node lower-cases incoming header names.
if (received.headers["x-cloudtasks-queuename"] !== "node-rest-check") {
  fail(`X-CloudTasks-QueueName = ${received.headers["x-cloudtasks-queuename"]}`);
}
if (received.headers["content-type"] !== "application/json") {
  fail(`Content-Type = ${received.headers["content-type"]}`);
}

await client.deleteQueue({ name: QUEUE_NAME });
await client.close();
server.close();
console.log(`OK: @google-cloud/tasks REST fallback against ${REST_ADDR}`);
