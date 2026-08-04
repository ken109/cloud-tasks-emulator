#!/usr/bin/env node
// Drive a running emulator with the official @google-cloud/tasks client.
//
// Same contract as the Python check: the unmodified official client must be
// able to create a queue and a task, and the task must actually arrive at its
// target with the Cloud Tasks system headers.
//
// Usage: EMULATOR_ADDR=localhost:8123 node check.mjs

import http from "node:http";
import { credentials } from "@grpc/grpc-js";
import tasks from "@google-cloud/tasks";

const EMULATOR_ADDR = process.env.EMULATOR_ADDR ?? "localhost:8123";
// The address the emulator should call back on. When the emulator runs in a
// container this must be an address reachable from inside it.
const TARGET_HOST = process.env.TARGET_HOST ?? "127.0.0.1";

const PARENT = "projects/conformance/locations/us-central1";
const QUEUE_NAME = `${PARENT}/queues/node-check`;

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

const [host, port] = EMULATOR_ADDR.split(":");
const { port: targetPort, request, server } = await startTarget();
const targetUrl = `http://${TARGET_HOST}:${targetPort}/handle`;

const client = new tasks.v2.CloudTasksClient({
  apiEndpoint: host,
  port: Number(port),
  sslCreds: credentials.createInsecure(),
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
      body: Buffer.from('{"hello":"world"}'),
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
  `the emulator never dispatched the task to ${targetUrl}`,
).catch((err) => fail(err.message));

if (received.body.toString() !== '{"hello":"world"}') {
  fail(`dispatched body = ${received.body.toString()}`);
}
// Node lower-cases incoming header names.
if (received.headers["x-cloudtasks-queuename"] !== "node-check") {
  fail(`X-CloudTasks-QueueName = ${received.headers["x-cloudtasks-queuename"]}`);
}
if (received.headers["x-cloudtasks-taskname"] !== task.name.split("/").pop()) {
  fail(`X-CloudTasks-TaskName = ${received.headers["x-cloudtasks-taskname"]}`);
}
for (const header of [
  "x-cloudtasks-taskretrycount",
  "x-cloudtasks-taskexecutioncount",
  "x-cloudtasks-tasketa",
]) {
  if (!(header in received.headers)) {
    fail(`missing ${header} on the dispatched request`);
  }
}
if (received.headers["content-type"] !== "application/json") {
  fail(`Content-Type = ${received.headers["content-type"]}`);
}

// A successfully dispatched task is removed from the queue. The emulator only
// records that once it has read our 2xx, which is necessarily after the handler
// handed us the request, so poll rather than assert once.
const deadline = Date.now() + 15_000;
for (;;) {
  const [remaining] = await client.listTasks({ parent: QUEUE_NAME });
  if (remaining.length === 0) break;
  if (Date.now() > deadline) {
    fail(`queue still holds ${remaining.map((t) => t.name)} after a 2xx dispatch`);
  }
  await new Promise((resolve) => setTimeout(resolve, 200));
}

await client.deleteQueue({ name: QUEUE_NAME });
await client.close();
server.close();
console.log(`OK: @google-cloud/tasks (node) against ${EMULATOR_ADDR}`);
