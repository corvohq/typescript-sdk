import { CorvoClient, PayloadTooLargeError } from "@corvohq/client";
export { PayloadTooLargeError } from "@corvohq/client";
export { RpcClient, ResilientLifecycleStream, LifecycleStream, ErrNotLeader } from "./rpc.js";

declare const process:
  | {
      on: (event: string, handler: () => void) => void;
      off: (event: string, handler: () => void) => void;
    }
  | undefined;

export type FetchedJob = {
  job_id: string;
  queue: string;
  payload: Record<string, unknown>;
  attempt: number;
  max_retries: number;
  lease_duration: number;
  checkpoint?: Record<string, unknown>;
  tags?: Record<string, string>;
  agent?: Record<string, unknown>;
};

export type WorkerHandler = (job: FetchedJob, ctx: WorkerJobContext) => Promise<void> | void;

export type WorkerConfig = {
  queues: string[];
  workerID: string;
  hostname?: string;
  concurrency?: number;
  shutdownTimeoutMs?: number;
  fetchBatchSize?: number;
  ackBatchSize?: number;
  useRpc?: boolean;
};

export type WorkerJobContext = {
  isCancelled: () => boolean;
  checkpoint: (checkpoint: Record<string, unknown>) => Promise<void>;
  progress: (current: number, total: number, message: string) => Promise<void>;
};

export class CorvoWorker {
  private readonly client: CorvoClient;
  private readonly cfg: Required<Omit<WorkerConfig, "useRpc">> & { useRpc: boolean };
  private readonly handlers: Map<string, WorkerHandler> = new Map();
  private readonly active: Map<string, { cancelled: boolean }> = new Map();
  private stopping = false;
  private readonly fetchBatchSize: number;
  private readonly ackBatchSize: number;

  constructor(client: CorvoClient, cfg: WorkerConfig) {
    this.client = client;
    this.cfg = {
      ...cfg,
      hostname: cfg.hostname ?? "corvo-worker",
      concurrency: cfg.concurrency ?? 10,
      shutdownTimeoutMs: cfg.shutdownTimeoutMs ?? 30000,
      fetchBatchSize: cfg.fetchBatchSize ?? 1,
      ackBatchSize: cfg.ackBatchSize ?? 1,
      useRpc: cfg.useRpc ?? false,
    };
    this.fetchBatchSize = cfg.fetchBatchSize ?? 1;
    this.ackBatchSize = cfg.ackBatchSize ?? 1;
  }

  register(queue: string, handler: WorkerHandler): void {
    this.handlers.set(queue, handler);
  }

  async start(): Promise<void> {
    const onSignal = () => {
      this.stop().catch(() => {});
    };
    if (typeof process !== "undefined") {
      process.on("SIGINT", onSignal);
      process.on("SIGTERM", onSignal);
    }

    if (this.cfg.useRpc) {
      const { ResilientLifecycleStream, RpcClient } = await import("./rpc.js");
      const rpcLoop = this.rpcFetchLoop(ResilientLifecycleStream, RpcClient);
      const heartbeat = this.rpcHeartbeatLoop(RpcClient);
      await Promise.race([rpcLoop, heartbeat]);
    } else {
      const loops = Array.from({ length: this.cfg.concurrency }, () => this.fetchLoop());
      const heartbeat = this.heartbeatLoop();
      await Promise.race([Promise.all(loops), heartbeat]);
    }

    if (typeof process !== "undefined") {
      process.off("SIGINT", onSignal);
      process.off("SIGTERM", onSignal);
    }
  }

  async stop(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;

    const deadline = Date.now() + this.cfg.shutdownTimeoutMs;
    while (this.active.size > 0 && Date.now() < deadline) {
      await sleep(100);
    }
    for (const [jobID] of this.active) {
      await this.fail(jobID, "worker_shutdown");
    }
  }

  // -------------------------------------------------------------------------
  // RPC fetch loop — single bidi stream replaces N HTTP fetch loops
  // -------------------------------------------------------------------------

  private async rpcFetchLoop(
    ResilientLifecycleStreamCtor: typeof import("./rpc.js").ResilientLifecycleStream,
    _RpcClientCtor: typeof import("./rpc.js").RpcClient,
  ): Promise<void> {
    const stream = new ResilientLifecycleStreamCtor(this.client.baseURL, this.client.auth);
    const pendingAcks: { jobId: string; resultJson: string }[] = [];
    const pendingEnqueues: { queue: string; payloadJson: string }[] = [];
    let requestId = 0;
    const jobPromises = new Set<Promise<void>>();

    try {
      while (!this.stopping) {
        try {
          // Drain completed acks
          const acks = pendingAcks.splice(0);
          const enqueues = pendingEnqueues.splice(0);
          const fetchCount = this.cfg.concurrency - this.active.size;

          requestId++;
          const resp = await stream.exchange({
            requestId,
            queues: this.cfg.queues,
            workerId: this.cfg.workerID,
            hostname: this.cfg.hostname,
            leaseDuration: 30,
            fetchCount: fetchCount > 0 ? fetchCount : 0,
            acks: acks.map((a) => ({
              jobId: a.jobId,
              resultJson: a.resultJson,
            })),
            enqueues: enqueues.map((e) => ({
              queue: e.queue,
              payloadJson: e.payloadJson,
            })),
          });

          // Dispatch fetched jobs
          for (const protoJob of resp.jobs) {
            const job = protoJobToFetched(protoJob);
            const handler = this.handlers.get(job.queue);
            if (!handler) {
              pendingAcks.push({ jobId: job.job_id, resultJson: "{}" });
              continue;
            }

            this.active.set(job.job_id, { cancelled: false });
            const ctx = this.makeJobContext(job.job_id);

            const p = (async () => {
              try {
                await handler(job, ctx);
                pendingAcks.push({ jobId: job.job_id, resultJson: "{}" });
              } catch (err) {
                const message = err instanceof Error ? err.message : String(err);
                await this.fail(job.job_id, message);
              } finally {
                this.active.delete(job.job_id);
              }
            })();
            jobPromises.add(p);
            p.finally(() => jobPromises.delete(p));
          }

          // If at capacity, wait for at least one job to finish
          if (this.active.size >= this.cfg.concurrency && jobPromises.size > 0) {
            await Promise.race(jobPromises);
          }
        } catch {
          if (!this.stopping) await sleep(1000);
        }
      }
    } finally {
      // Wait for in-flight jobs
      if (jobPromises.size > 0) {
        await Promise.allSettled(jobPromises);
      }
      stream.close();
    }
  }

  private async rpcHeartbeatLoop(
    RpcClientCtor: typeof import("./rpc.js").RpcClient,
  ): Promise<void> {
    const rpcClient = new RpcClientCtor(this.client.baseURL, this.client.auth);
    while (!this.stopping) {
      await sleep(15000);
      if (this.stopping || this.active.size === 0) continue;

      const jobs: Record<string, { progressJson: string; checkpointJson: string; streamDelta: string }> = {};
      for (const [jobID] of this.active) {
        jobs[jobID] = { progressJson: "", checkpointJson: "", streamDelta: "" };
      }

      try {
        const result = await rpcClient.heartbeat(jobs);
        for (const [jobID, info] of Object.entries(result)) {
          if (info.status === "cancel") {
            const state = this.active.get(jobID);
            if (state) state.cancelled = true;
          }
        }
      } catch {
        // Best-effort heartbeat.
      }
    }
  }

  private makeJobContext(jobId: string): WorkerJobContext {
    return {
      isCancelled: () => this.active.get(jobId)?.cancelled === true,
      checkpoint: async (checkpoint) => {
        await this.heartbeat({ [jobId]: { checkpoint } });
      },
      progress: async (current, total, message) => {
        await this.heartbeat({
          [jobId]: { progress: { current, total, message } },
        });
      },
    };
  }

  // -------------------------------------------------------------------------
  // HTTP fetch loop (original)
  // -------------------------------------------------------------------------

  private async fetchLoop(): Promise<void> {
    const ackBuffer: { job_id: string; result?: Record<string, unknown> }[] = [];

    while (!this.stopping) {
      try {
        await this.flushAcks(ackBuffer);

        let jobs: FetchedJob[];
        if (this.fetchBatchSize > 1) {
          const result = await this.fetchBatch();
          jobs = result ?? [];
        } else {
          const job = await this.fetch();
          jobs = job ? [job] : [];
        }

        for (const job of jobs) {
          const handler = this.handlers.get(job.queue);
          if (!handler) {
            ackBuffer.push({ job_id: job.job_id });
            continue;
          }

          this.active.set(job.job_id, { cancelled: false });
          const ctx: WorkerJobContext = {
            isCancelled: () => this.active.get(job.job_id)?.cancelled === true,
            checkpoint: async (checkpoint) => {
              await this.heartbeat({ [job.job_id]: { checkpoint } });
            },
            progress: async (current, total, message) => {
              await this.heartbeat({
                [job.job_id]: { progress: { current, total, message } },
              });
            },
          };

          try {
            await handler(job, ctx);
            ackBuffer.push({ job_id: job.job_id });
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            await this.fail(job.job_id, message);
          } finally {
            this.active.delete(job.job_id);
          }
        }

        await this.flushAcks(ackBuffer);
      } catch {
        await sleep(1000);
      }
    }
    // Flush remaining acks on shutdown.
    await this.flushAcks(ackBuffer, true);
  }

  private async flushAcks(
    buffer: { job_id: string; result?: Record<string, unknown> }[],
    force = false,
  ): Promise<void> {
    while (this.ackBatchSize > 1 && buffer.length >= this.ackBatchSize) {
      const batch = buffer.splice(0, this.ackBatchSize);
      await this.ackBatch(batch);
    }
    if (force && buffer.length > 0 && this.ackBatchSize > 1) {
      await this.ackBatch(buffer.splice(0));
    }
    if (this.ackBatchSize <= 1 && buffer.length > 0) {
      for (const item of buffer) {
        await this.client.ack(item.job_id, item.result ?? {});
      }
      buffer.length = 0;
    }
  }

  private async heartbeatLoop(): Promise<void> {
    while (!this.stopping) {
      await sleep(15000);
      if (this.stopping || this.active.size === 0) continue;

      const jobs: Record<string, { progress?: Record<string, unknown>; checkpoint?: Record<string, unknown> }> = {};
      for (const [jobID] of this.active) jobs[jobID] = {};

      try {
        const result = await this.heartbeat(jobs);
        for (const [jobID, info] of Object.entries(result.jobs || {})) {
          if (info.status === "cancel") {
            const state = this.active.get(jobID);
            if (state) state.cancelled = true;
          }
        }
      } catch {
        // Best-effort heartbeat.
      }
    }
  }

  private async fetchBatch(): Promise<FetchedJob[]> {
    const result = await this.request<{ jobs: FetchedJob[] }>("/api/v1/fetch/batch", {
      method: "POST",
      body: JSON.stringify({
        queues: this.cfg.queues,
        worker_id: this.cfg.workerID,
        hostname: this.cfg.hostname,
        timeout: 30,
        count: this.fetchBatchSize,
      }),
    });
    return result.jobs ?? [];
  }

  private async ackBatch(
    acks: { job_id: string; result?: Record<string, unknown> }[],
  ): Promise<void> {
    await this.request("/api/v1/ack/batch", {
      method: "POST",
      body: JSON.stringify({ acks }),
    });
  }

  private async fetch(): Promise<FetchedJob | null> {
    const job = await this.request<FetchedJob | Record<string, never>>("/api/v1/fetch", {
      method: "POST",
      body: JSON.stringify({
        queues: this.cfg.queues,
        worker_id: this.cfg.workerID,
        hostname: this.cfg.hostname,
        timeout: 30,
      }),
    });
    const j = job as Partial<FetchedJob>;
    if (!j.job_id) return null;
    return j as FetchedJob;
  }

  private async fail(jobID: string, error: string, backtrace = ""): Promise<void> {
    if (this.cfg.useRpc) {
      try {
        const { RpcClient } = await import("./rpc.js");
        const rpc = new RpcClient(this.client.baseURL, this.client.auth);
        await rpc.fail(jobID, error, backtrace);
        return;
      } catch {
        // Fall through to HTTP
      }
    }
    await this.request(`/api/v1/fail/${encodeURIComponent(jobID)}`, {
      method: "POST",
      body: JSON.stringify({ error, backtrace }),
    });
  }

  private async heartbeat(
    jobs: Record<string, { progress?: Record<string, unknown>; checkpoint?: Record<string, unknown>; usage?: Record<string, unknown> }>
  ): Promise<{ jobs: Record<string, { status: string }> }> {
    return this.request("/api/v1/heartbeat", {
      method: "POST",
      body: JSON.stringify({ jobs }),
    });
  }

  private async request<T>(path: string, init: RequestInit): Promise<T> {
    const authHeaders = await this.client.authHeaders();
    const res = await this.client.fetchImpl(this.client.baseURL + path, {
      ...init,
      headers: {
        "content-type": "application/json",
        ...authHeaders,
        ...(init.headers || {}),
      },
    });
    if (!res.ok) {
      let details = `HTTP ${res.status}`;
      let code = "";
      try {
        const body = (await res.json()) as { error?: string; code?: string };
        if (body.error) details = body.error;
        if (body.code) code = body.code;
      } catch {
        // ignore
      }
      if (code === "PAYLOAD_TOO_LARGE") throw new PayloadTooLargeError(details);
      throw new Error(details);
    }
    if (res.status === 204) return {} as T;
    return (await res.json()) as T;
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function protoJobToFetched(j: import("./rpc.js").FetchBatchJob): FetchedJob {
  let payload: Record<string, unknown> = {};
  if (j.payloadJson) {
    try {
      payload = JSON.parse(j.payloadJson);
    } catch {
      // leave empty
    }
  }
  let checkpoint: Record<string, unknown> | undefined;
  if (j.checkpointJson) {
    try {
      checkpoint = JSON.parse(j.checkpointJson);
    } catch {
      // ignore
    }
  }
  let tags: Record<string, string> | undefined;
  if (j.tagsJson) {
    try {
      tags = JSON.parse(j.tagsJson);
    } catch {
      // ignore
    }
  }
  return {
    job_id: j.jobId,
    queue: j.queue,
    payload,
    attempt: j.attempt,
    max_retries: j.maxRetries,
    lease_duration: j.leaseDuration,
    checkpoint,
    tags,
    agent: j.agent as Record<string, unknown> | undefined,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
