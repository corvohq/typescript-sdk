# Corvo TypeScript SDK

TypeScript client and worker runtime for [Corvo](https://corvo.dev), the fast job queue.

## Packages

- `@corvohq/client` — HTTP client for the Corvo API
- `@corvohq/worker` — Worker runtime for processing jobs

## Installation

```bash
npm install @corvohq/client
# For worker:
npm install @corvohq/worker
```

## Compatibility

| SDK Version | Corvo Server |
|-------------|-------------|
| 0.2.x       | >= 0.2.0    |

## Quick Start

### Client

```typescript
import { CorvoClient } from "@corvohq/client";

const client = new CorvoClient("http://localhost:7080", fetch, {
  apiKey: "your-key",
});

const result = await client.enqueue("emails", { to: "user@example.com" });
console.log("Enqueued:", result.job_id);
```

### Worker

```typescript
import { CorvoClient } from "@corvohq/client";
import { CorvoWorker } from "@corvohq/worker";

const client = new CorvoClient("http://localhost:7080", fetch, {
  apiKey: "your-key",
});

const worker = new CorvoWorker(client, {
  queues: ["emails"],
  workerID: "worker-1",
  concurrency: 5,
});

worker.register("emails", async (job, ctx) => {
  console.log("Processing:", job.job_id);
  await ctx.progress(1, 1, "Done");
});

await worker.start();
```

## Authentication

```typescript
// API Key
new CorvoClient(url, fetch, { apiKey: "key" });

// Bearer token
new CorvoClient(url, fetch, { bearerToken: "token" });

// Dynamic token provider
new CorvoClient(url, fetch, { tokenProvider: async () => getToken() });

// Custom headers
new CorvoClient(url, fetch, { headers: { "X-Custom": "value" } });
```

## License

MIT
