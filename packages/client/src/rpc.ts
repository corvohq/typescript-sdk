import type { AuthOptions } from "./index.js";
import { createClient, type Client, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { create } from "@bufbuild/protobuf";
import {
  WorkerService,
  HeartbeatJobUpdateSchema,
} from "./gen/corvo/v1/worker_pb.js";

// ---------------------------------------------------------------------------
// Auth interceptor
// ---------------------------------------------------------------------------

const SDK_NAME = "corvo-typescript";
const SDK_VERSION = "0.2.0";

function authInterceptor(auth: AuthOptions): Interceptor {
  return (next) => async (req) => {
    req.header.set("x-corvo-client-name", SDK_NAME);
    req.header.set("x-corvo-client-version", SDK_VERSION);
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
// ClientRpc — Connect RPC transport for CorvoClient methods
// ---------------------------------------------------------------------------

type WorkerClient = Client<typeof WorkerService>;

export class ClientRpc {
  private readonly client: WorkerClient;

  constructor(baseUrl: string, auth: AuthOptions = {}) {
    const transport = createConnectTransport({
      baseUrl: baseUrl.replace(/\/$/, ""),
      httpVersion: "1.1",
      useBinaryFormat: false,
      interceptors: [authInterceptor(auth)],
    });
    this.client = createClient(WorkerService, transport);
  }

  async enqueue(
    queue: string,
    payload: unknown,
  ): Promise<{ job_id: string; status: string; unique_existing?: boolean }> {
    const payloadJson = JSON.stringify(payload ?? {});
    const resp = await this.client.enqueue({ queue, payloadJson });
    return {
      job_id: resp.jobId,
      status: resp.status,
      unique_existing: resp.uniqueExisting || undefined,
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
    jobs: Record<string, Record<string, unknown>>,
  ): Promise<{ jobs: Record<string, { status: string }> }> {
    const protoJobs: Record<string, import("./gen/corvo/v1/worker_pb.js").HeartbeatJobUpdate> = {};
    for (const [id, update] of Object.entries(jobs)) {
      protoJobs[id] = create(HeartbeatJobUpdateSchema, {
        progressJson: update.progress ? JSON.stringify(update.progress) : "",
        checkpointJson: update.checkpoint ? JSON.stringify(update.checkpoint) : "",
        streamDelta: "",
      });
    }
    const resp = await this.client.heartbeat({ jobs: protoJobs });
    const out: Record<string, { status: string }> = {};
    for (const [id, jr] of Object.entries(resp.jobs)) {
      out[id] = { status: jr.status };
    }
    return { jobs: out };
  }
}
