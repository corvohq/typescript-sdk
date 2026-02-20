import { createClient, type Client, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { create } from "@bufbuild/protobuf";
import type { AuthOptions } from "@corvohq/client";
import {
  WorkerService,
  type FetchBatchJob,
  type HeartbeatJobUpdate,
  HeartbeatJobUpdateSchema,
} from "./gen/corvo/v1/worker_pb.js";

// Re-export generated types that are used by index.ts
export type { FetchBatchJob, HeartbeatJobUpdate } from "./gen/corvo/v1/worker_pb.js";

// ---------------------------------------------------------------------------
// Auth interceptor
// ---------------------------------------------------------------------------

function authInterceptor(auth: AuthOptions): Interceptor {
  return (next) => async (req) => {
    if (auth.headers) {
      for (const [k, v] of Object.entries(auth.headers)) {
        req.header.set(k, v);
      }
    }
    if (auth.apiKey) {
      req.header.set(auth.apiKeyHeader || "X-API-Key", auth.apiKey);
    }
    let token = auth.bearerToken || "";
    if (auth.tokenProvider) {
      token = await auth.tokenProvider();
    }
    if (token) {
      req.header.set("authorization", `Bearer ${token}`);
    }
    return next(req);
  };
}

// ---------------------------------------------------------------------------
// Transport + client factory
// ---------------------------------------------------------------------------

type WorkerClient = Client<typeof WorkerService>;

function makeClient(baseUrl: string, auth: AuthOptions): WorkerClient {
  const transport = createConnectTransport({
    baseUrl,
    httpVersion: "1.1",
    useBinaryFormat: false, // use JSON for compatibility
    interceptors: [authInterceptor(auth)],
  });
  return createClient(WorkerService, transport);
}

// ---------------------------------------------------------------------------
// RpcClient
// ---------------------------------------------------------------------------

export class RpcClient {
  private readonly client: WorkerClient;

  constructor(baseUrl: string, auth: AuthOptions = {}) {
    this.client = makeClient(baseUrl.replace(/\/$/, ""), auth);
  }

  async enqueue(
    queue: string,
    payloadJson: string,
  ): Promise<{ jobId: string; status: string; uniqueExisting: boolean }> {
    const resp = await this.client.enqueue({ queue, payloadJson });
    return {
      jobId: resp.jobId,
      status: resp.status,
      uniqueExisting: resp.uniqueExisting,
    };
  }

  async fail(
    jobId: string,
    error: string,
    backtrace = "",
  ): Promise<{ status: string }> {
    const resp = await this.client.fail({ jobId, error, backtrace });
    return { status: resp.status };
  }

  async heartbeat(
    jobs: Record<string, { progressJson?: string; checkpointJson?: string; streamDelta?: string }>,
  ): Promise<Record<string, { status: string }>> {
    const protoJobs: { [key: string]: HeartbeatJobUpdate } = {};
    for (const [id, update] of Object.entries(jobs)) {
      protoJobs[id] = create(HeartbeatJobUpdateSchema, {
        progressJson: update.progressJson ?? "",
        checkpointJson: update.checkpointJson ?? "",
        streamDelta: update.streamDelta ?? "",
      });
    }
    const resp = await this.client.heartbeat({ jobs: protoJobs });
    const out: Record<string, { status: string }> = {};
    for (const [id, jr] of Object.entries(resp.jobs)) {
      out[id] = { status: jr.status };
    }
    return out;
  }

  /** Unary call to StreamLifecycle endpoint (Connect JSON protocol). */
  async lifecycleExchange(body: Record<string, unknown>): Promise<LifecycleRawResponse> {
    // StreamLifecycle is bidi streaming in proto, but we call it as unary
    // using the raw Connect JSON-over-HTTP protocol for compatibility.
    // The server supports single-request/single-response over HTTP/1.1.
    throw new Error("lifecycleExchange is not supported via createClient; use LifecycleStream instead");
  }
}

// ---------------------------------------------------------------------------
// Types (keep for backward compat with index.ts)
// ---------------------------------------------------------------------------

export interface AckBatchItem {
  jobId: string;
  resultJson: string;
}

export interface LifecycleEnqueueItem {
  queue: string;
  payloadJson: string;
}

// ---------------------------------------------------------------------------
// ErrNotLeader
// ---------------------------------------------------------------------------

export class ErrNotLeader extends Error {
  readonly leaderAddr: string;
  constructor(leaderAddr: string) {
    super(
      leaderAddr
        ? `NOT_LEADER: leader is at ${leaderAddr}`
        : "NOT_LEADER: leader unknown",
    );
    this.name = "ErrNotLeader";
    this.leaderAddr = leaderAddr;
  }
}

// ---------------------------------------------------------------------------
// Lifecycle request/response types
// ---------------------------------------------------------------------------

export type LifecycleRequest = {
  requestId: number;
  queues: string[];
  workerId: string;
  hostname: string;
  leaseDuration: number;
  fetchCount: number;
  acks: AckBatchItem[];
  enqueues: LifecycleEnqueueItem[];
};

export type LifecycleResponse = {
  requestId: number;
  jobs: FetchBatchJob[];
  acked: number;
  enqueuedJobIds: string[];
  error: string;
  leaderAddr: string;
};

interface LifecycleRawResponse {
  requestId?: number;
  jobs?: FetchBatchJob[];
  acked?: number;
  enqueuedJobIds?: string[];
  error?: string;
  leaderAddr?: string;
}

// ---------------------------------------------------------------------------
// LifecycleStream — bidi streaming over Connect RPC
// ---------------------------------------------------------------------------

export class LifecycleStream {
  private readonly baseUrl: string;
  private readonly auth: AuthOptions;
  private closed = false;

  constructor(baseUrl: string, auth: AuthOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.auth = auth;
  }

  async exchange(req: LifecycleRequest): Promise<LifecycleResponse> {
    if (this.closed) {
      throw new Error("stream closed");
    }

    // StreamLifecycle is bidi streaming in the proto, but the server
    // supports Connect protocol unary-style JSON calls. We use raw HTTP
    // because createClient's bidi streaming requires HTTP/2 and a long-lived
    // stream, which doesn't match our request-per-exchange pattern.
    const h: Record<string, string> = { "content-type": "application/json" };
    if (this.auth.headers) {
      for (const [k, v] of Object.entries(this.auth.headers)) {
        h[k] = v;
      }
    }
    if (this.auth.apiKey) {
      h[this.auth.apiKeyHeader || "X-API-Key"] = this.auth.apiKey;
    }
    let token = this.auth.bearerToken || "";
    if (this.auth.tokenProvider) {
      token = await this.auth.tokenProvider();
    }
    if (token) {
      h["authorization"] = `Bearer ${token}`;
    }

    const url = `${this.baseUrl}/corvo.v1.WorkerService/StreamLifecycle`;
    const body = JSON.stringify({
      requestId: req.requestId,
      queues: req.queues,
      workerId: req.workerId,
      hostname: req.hostname,
      leaseDuration: req.leaseDuration,
      fetchCount: req.fetchCount,
      acks: req.acks.map((a) => ({
        jobId: a.jobId,
        resultJson: a.resultJson,
      })),
      enqueues: req.enqueues.map((e) => ({
        queue: e.queue,
        payloadJson: e.payloadJson,
      })),
    });

    const resp = await globalThis.fetch(url, {
      method: "POST",
      headers: h,
      body,
    });

    if (!resp.ok) {
      throw new Error(`RPC StreamLifecycle failed: HTTP ${resp.status}`);
    }

    const msg = (await resp.json()) as LifecycleRawResponse;

    if (msg.error === "NOT_LEADER") {
      throw new ErrNotLeader(msg.leaderAddr ?? "");
    }

    return {
      requestId: msg.requestId ?? 0,
      jobs: msg.jobs ?? [],
      acked: msg.acked ?? 0,
      enqueuedJobIds: msg.enqueuedJobIds ?? [],
      error: msg.error ?? "",
      leaderAddr: msg.leaderAddr ?? "",
    };
  }

  close(): void {
    this.closed = true;
  }
}

// ---------------------------------------------------------------------------
// ResilientLifecycleStream — reconnects on NOT_LEADER
// ---------------------------------------------------------------------------

export class ResilientLifecycleStream {
  private baseUrl: string;
  private readonly auth: AuthOptions;
  private stream: LifecycleStream;

  constructor(baseUrl: string, auth: AuthOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.auth = auth;
    this.stream = new LifecycleStream(this.baseUrl, auth);
  }

  async exchange(req: LifecycleRequest): Promise<LifecycleResponse> {
    try {
      return await this.stream.exchange(req);
    } catch (err) {
      if (err instanceof ErrNotLeader && err.leaderAddr) {
        this.stream.close();
        this.baseUrl = err.leaderAddr;
        this.stream = new LifecycleStream(this.baseUrl, this.auth);
        return this.stream.exchange(req);
      }
      throw err;
    }
  }

  close(): void {
    this.stream.close();
  }
}
