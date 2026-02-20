/**
 * Integration tests for the Corvo TypeScript SDK.
 *
 * Run with: npx tsx tests/integration.test.ts
 * Requires a running Corvo server (default: http://localhost:8080).
 */

import { CorvoClient } from "../packages/client/src/index.js";
import { CorvoWorker } from "../packages/worker/src/index.js";

const CORVO_URL = process.env.CORVO_URL || "http://localhost:8080";

let passed = 0;
let failed = 0;

function assert(cond: boolean, msg: string): void {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function testJobLifecycle() {
  console.log("--- test: job lifecycle ---");
  const client = new CorvoClient(CORVO_URL, fetch);

  const result = await client.enqueue("ts-test", { hello: "world" });
  assert(!!result.job_id, "expected non-empty job_id");

  const job = await client.fetch(["ts-test"], "ts-integration-worker", "test-host", 5);
  assert(job !== null, "expected a job");
  assert(job!.job_id === result.job_id, `expected ${result.job_id}, got ${job!.job_id}`);
  assert((job!.payload as Record<string, string>).hello === "world", "payload mismatch");

  await client.ack(job!.job_id);

  const got = await client.getJob<{ state: string }>(result.job_id);
  assert(got.state === "completed", `expected completed, got ${got.state}`);
  console.log(`  job ${result.job_id} completed`);
}

async function testEnqueueOptions() {
  console.log("--- test: enqueue options ---");
  const client = new CorvoClient(CORVO_URL, fetch);

  const result = await client.enqueueWith({
    queue: "ts-opts-test",
    payload: { key: "value" },
    priority: "high",
    max_retries: 5,
    tags: { env: "test", sdk: "typescript" },
  });
  assert(!!result.job_id, "expected non-empty job_id");

  const got = await client.getJob<{ priority: number; max_retries: number }>(result.job_id);
  assert(got.priority === 1, `expected priority=1, got ${got.priority}`);
  assert(got.max_retries === 5, `expected max_retries=5, got ${got.max_retries}`);

  const job = await client.fetch(["ts-opts-test"], "w1", "host", 3);
  if (job) await client.ack(job.job_id);
  console.log("  enqueue with options verified");
}

async function testSearch() {
  console.log("--- test: search ---");
  const client = new CorvoClient(CORVO_URL, fetch);

  await client.enqueue("ts-search-test", { n: 1 });
  await client.enqueue("ts-search-test", { n: 2 });

  const result = await client.search({ queue: "ts-search-test", state: ["pending"] });
  assert(result.total >= 2, `expected at least 2, got ${result.total}`);

  for (let i = 0; i < 2; i++) {
    const job = await client.fetch(["ts-search-test"], "w1", "host", 1);
    if (job) await client.ack(job.job_id);
  }
  console.log(`  found ${result.total} jobs`);
}

async function testBatchEnqueue() {
  console.log("--- test: batch enqueue ---");
  const client = new CorvoClient(CORVO_URL, fetch);

  const result = await client.enqueueBatch([
    { queue: "ts-batch-test", payload: { n: 1 } },
    { queue: "ts-batch-test", payload: { n: 2 } },
    { queue: "ts-batch-test", payload: { n: 3 } },
  ]);
  assert(result.job_ids.length === 3, `expected 3 jobs, got ${result.job_ids.length}`);

  for (let i = 0; i < 3; i++) {
    const job = await client.fetch(["ts-batch-test"], "w1", "host", 1);
    if (job) await client.ack(job.job_id);
  }
  console.log("  batch enqueue verified");
}

async function testFailAndRetry() {
  console.log("--- test: fail and retry ---");
  const client = new CorvoClient(CORVO_URL, fetch);

  const result = await client.enqueue("ts-fail-test", { x: 1 }, { max_retries: 2 });
  const jobId = result.job_id;

  const job = await client.fetch(["ts-fail-test"], "w1", "host", 5);
  assert(job !== null, "expected a job");
  await client.fail(jobId, "test error");

  await sleep(6000);

  const job2 = await client.fetch(["ts-fail-test"], "w1", "host", 5);
  assert(job2 !== null, "expected retry job");
  assert(job2!.attempt === 2, `expected attempt=2, got ${job2!.attempt}`);

  await client.ack(job2!.job_id);

  const got = await client.getJob<{ state: string }>(jobId);
  assert(got.state === "completed", `expected completed, got ${got.state}`);
  console.log("  fail and retry verified");
}

async function testWorker() {
  console.log("--- test: worker ---");
  const client = new CorvoClient(CORVO_URL, fetch);

  const result = await client.enqueue("ts-worker-test", { task: "process" });
  const jobId = result.job_id;

  let resolveProcessed: (id: string) => void;
  const processed = new Promise<string>((r) => { resolveProcessed = r; });

  const worker = new CorvoWorker(client, {
    queues: ["ts-worker-test"],
    workerID: "ts-integration-test-worker",
    concurrency: 1,
    shutdownTimeoutMs: 5000,
  });

  worker.register("ts-worker-test", async (job) => {
    resolveProcessed!(job.job_id);
  });

  const workerPromise = worker.start();

  const processedId = await Promise.race([
    processed,
    new Promise<string>((_, reject) => setTimeout(() => reject(new Error("timeout")), 15000)),
  ]);
  assert(processedId === jobId, `expected ${jobId}, got ${processedId}`);

  // Give worker time to ack
  await sleep(500);

  const got = await client.getJob<{ state: string }>(jobId);
  assert(got.state === "completed", `expected completed, got ${got.state}`);
  console.log(`  job ${jobId} completed via worker`);

  await worker.stop();
}

async function main() {
  const tests = [
    ["job lifecycle", testJobLifecycle],
    ["enqueue options", testEnqueueOptions],
    ["search", testSearch],
    ["batch enqueue", testBatchEnqueue],
    ["fail and retry", testFailAndRetry],
    ["worker", testWorker],
  ] as const;

  for (const [name, fn] of tests) {
    try {
      await fn();
      passed++;
      console.log(`  PASS: ${name}\n`);
    } catch (err) {
      failed++;
      console.error(`  FAIL: ${name}: ${err}\n`);
    }
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
