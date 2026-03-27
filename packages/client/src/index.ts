export type ServerInfo = {
  server_version: string;
  api_version: string;
};

export type EnqueueResult = {
  job: Record<string, unknown> | null;
  job_id: string;
  unique_existing: boolean;
  unique_job_id: string;
};

export type SearchFilter = {
  queue?: string;
  state?: string[];
  priority?: string;
  tags?: Record<string, string>;
  payload_contains?: string;
  payload_jq?: string;
  chain_id?: string;
  batch_id?: string;
  sort?: string;
  order?: "asc" | "desc";
  limit?: number;
  cursor?: string;
};

export type SearchResult<T = Record<string, unknown>> = {
  jobs: T[];
  total: number;
  cursor?: string;
  has_more: boolean;
};

export type BulkRequest = {
  job_ids?: string[];
  filter?: SearchFilter;
  action: "delete" | "cancel" | "move" | "requeue" | "change_priority" | "hold" | "approve" | "reject";
  move_to_queue?: string;
  priority?: string;
  async?: boolean;
};

export type BulkResult = {
  affected: number;
  errors: number;
  duration_ms: number;
};

export type BulkAsyncStart = {
  bulk_operation_id: string;
  status: string;
  estimated_total: number;
  progress_url: string;
};

export type BulkTask = {
  id: string;
  status: "queued" | "running" | "completed" | "failed";
  action: string;
  total: number;
  processed: number;
  affected: number;
  errors: number;
  error?: string;
  created_at: string;
  updated_at: string;
  finished_at?: string;
};

export type BatchJob = {
  queue: string;
  payload: unknown;
  [key: string]: unknown;
};

export type BatchConfig = {
  callback_queue: string;
  callback_payload?: unknown;
};

export type BatchResult = {
  job_ids: string[];
  batch_id: string;
};

export type FetchedJob = {
  job_id: string;
  queue: string;
  payload: unknown;
  attempt: number;
  max_retries?: number;
  lease_duration?: number;
  checkpoint?: unknown;
  tags?: Record<string, string>;
};

export type HeartbeatJobStatus = {
  status: string;
};

export type HeartbeatResult = {
  jobs: Record<string, HeartbeatJobStatus>;
  lease_expires_at?: string;
};

export type FailResult = {
  job: Record<string, unknown> | null;
  status: string;
  next_attempt_at?: string;
  attempts_remaining: number;
  err_doc?: Record<string, unknown>;
};

export type SealBatchResult = {
  callback_job?: Record<string, unknown>;
};

export type ChainStep = {
  queue: string;
  payload: unknown;
};

export type ChainConfig = {
  steps: ChainStep[];
  on_failure?: "stop" | "continue";
  on_exit?: ChainStep;
};

export type EnqueueOptions = {
  queue: string;
  payload: unknown;
  priority?: string;
  unique_key?: string;
  unique_period?: number;
  max_retries?: number;
  scheduled_at?: string;
  tags?: Record<string, string>;
  expire_after?: string;
  retry_backoff?: string;
  retry_base_delay?: string;
  retry_max_delay?: string;
  chain?: ChainConfig;
  batch_id?: string;
};

export type CorvoEvent = {
  type: string;
  id: string;
  data: Record<string, unknown>;
};

export type SubscribeOptions = {
  queues?: string[];
  job_ids?: string[];
  types?: string[];
  last_event_id?: number;
};

export type AuthOptions = {
  headers?: Record<string, string>;
  bearerToken?: string;
  apiKey?: string;
  apiKeyHeader?: string;
  tokenProvider?: () => Promise<string> | string;
};

export type RetryOptions = {
  maxAttempts?: number; // default 3; 0 disables retry
  baseDelay?: number;   // default 1000ms; jittered ±25%
};

export type ClientOptions = {
  retry?: RetryOptions;
};

export { Client } from "./pool.js";
export { Conn, type ConnFetchedJob } from "./conn.js";

export class PayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PayloadTooLargeError";
  }
}

export function isPayloadTooLargeError(err: unknown): err is PayloadTooLargeError {
  return err instanceof PayloadTooLargeError;
}

export class UniqueConflictError extends Error {
  readonly uniqueJobId: string;
  constructor(message: string, uniqueJobId: string) {
    super(message);
    this.name = "UniqueConflictError";
    this.uniqueJobId = uniqueJobId;
  }
}

export function isUniqueConflictError(err: unknown): err is UniqueConflictError {
  return err instanceof UniqueConflictError;
}

export class CorvoClient {
  readonly baseURL: string;
  readonly fetchImpl: typeof fetch;
  readonly auth: AuthOptions;
  private readonly retryMaxAttempts: number;
  private readonly retryBaseDelay: number;

  constructor(baseURL: string, fetchImpl: typeof fetch = fetch, auth: AuthOptions = {}, opts: ClientOptions = {}) {
    this.baseURL = baseURL.replace(/\/$/, "");
    this.fetchImpl = fetchImpl;
    this.auth = auth;
    this.retryMaxAttempts = opts.retry?.maxAttempts ?? 3;
    this.retryBaseDelay = opts.retry?.baseDelay ?? 1000;
  }

  async enqueue(queue: string, payload: unknown, extra: Record<string, unknown> = {}): Promise<EnqueueResult> {
    const result = await this.request<any>("/api/v1/enqueue", {
      method: "POST",
      body: JSON.stringify({ queue, payload, ...extra }),
    });
    result.job_id = result.job?.id ?? result.unique_job_id ?? '';
    return result as EnqueueResult;
  }

  async enqueueWith(opts: EnqueueOptions): Promise<EnqueueResult> {
    const result = await this.request<any>("/api/v1/enqueue", {
      method: "POST",
      body: JSON.stringify(opts),
    });
    result.job_id = result.job?.id ?? result.unique_job_id ?? '';
    return result as EnqueueResult;
  }

  async getJob<T = Record<string, unknown>>(id: string): Promise<T> {
    return this.request(`/api/v1/jobs/${encodeURIComponent(id)}`, { method: "GET" });
  }

  async search<T = Record<string, unknown>>(filter: SearchFilter): Promise<SearchResult<T>> {
    return this.request("/api/v1/jobs/search", {
      method: "POST",
      body: JSON.stringify(filter),
    });
  }

  async bulk(req: BulkRequest): Promise<BulkResult | BulkAsyncStart> {
    return this.request("/api/v1/jobs/bulk", {
      method: "POST",
      body: JSON.stringify(req),
    });
  }

  async bulkStatus(id: string): Promise<BulkTask> {
    return this.request(`/api/v1/bulk/${encodeURIComponent(id)}`, { method: "GET" });
  }

  async fetchBatch(
    queues: string[],
    workerID: string,
    hostname = "corvo-worker",
    timeout = 30,
    count = 10,
  ): Promise<{ jobs: FetchedJob[] }> {
    return this.request("/api/v1/fetch/batch", {
      method: "POST",
      body: JSON.stringify({ queues, worker_id: workerID, hostname, timeout, count }),
    });
  }

  async ackBatch(acks: { job_id: string; result?: Record<string, unknown> }[]): Promise<{ acked: number }> {
    return this.request("/api/v1/ack/batch", {
      method: "POST",
      body: JSON.stringify({ acks }),
    });
  }

  async enqueueBatch(jobs: BatchJob[], batch?: BatchConfig): Promise<BatchResult> {
    return this.request("/api/v1/enqueue", {
      method: "POST",
      body: JSON.stringify({ jobs, batch }),
    });
  }

  async fetch(queues: string[], workerID: string, hostname = "corvo-worker", timeout = 30): Promise<FetchedJob | null> {
    const result = await this.request<FetchedJob>("/api/v1/fetch", {
      method: "POST",
      body: JSON.stringify({ queues, worker_id: workerID, hostname, timeout }),
    });
    if (!result || !(result as FetchedJob).job_id) return null;
    return result;
  }

  async ack(jobID: string, body: Record<string, unknown> = {}): Promise<{ status: string }> {
    return this.request(`/api/v1/ack/${encodeURIComponent(jobID)}`, {
      method: "POST",
      body: JSON.stringify(body),
    });
  }

  async fail(jobID: string, error: string, backtrace?: string): Promise<FailResult> {
    return this.request(`/api/v1/fail/${encodeURIComponent(jobID)}`, {
      method: "POST",
      body: JSON.stringify({ error, backtrace }),
    });
  }

  async heartbeat(jobs: Record<string, Record<string, unknown>>): Promise<HeartbeatResult> {
    return this.request("/api/v1/heartbeat", {
      method: "POST",
      body: JSON.stringify({ jobs }),
    });
  }

  async cancelJob(id: string): Promise<void> {
    await this.request(`/api/v1/jobs/${encodeURIComponent(id)}/cancel`, { method: "POST" });
  }

  async moveJob(id: string, targetQueue: string): Promise<void> {
    await this.request(`/api/v1/jobs/${encodeURIComponent(id)}/move`, {
      method: "POST",
      body: JSON.stringify({ queue: targetQueue }),
    });
  }

  async deleteJob(id: string): Promise<void> {
    await this.request(`/api/v1/jobs/${encodeURIComponent(id)}`, { method: "DELETE" });
  }

  async createBatch(callbackQueue: string, callbackPayload?: unknown): Promise<{ batch_id: string }> {
    const body: Record<string, unknown> = { callback_queue: callbackQueue };
    if (callbackPayload !== undefined) body.callback_payload = callbackPayload;
    return this.request("/api/v1/batch", {
      method: "POST",
      body: JSON.stringify(body),
    });
  }

  async sealBatch(batchID: string): Promise<SealBatchResult> {
    return this.request(`/api/v1/batch/${encodeURIComponent(batchID)}/seal`, {
      method: "POST",
    });
  }

  async getServerInfo(): Promise<ServerInfo> {
    return this.request("/api/v1/info", { method: "GET" });
  }

  async bulkGetJobs<T = Record<string, unknown>>(ids: string[]): Promise<T[]> {
    const result = await this.request<{ jobs: T[] }>("/api/v1/jobs/bulk-get", {
      method: "POST",
      body: JSON.stringify({ job_ids: ids }),
    });
    return result.jobs;
  }

  async *subscribe(options: SubscribeOptions = {}): AsyncGenerator<CorvoEvent> {
    const params = new URLSearchParams();
    if (options.queues?.length) params.set("queues", options.queues.join(","));
    if (options.job_ids?.length) params.set("job_ids", options.job_ids.join(","));
    if (options.types?.length) params.set("types", options.types.join(","));
    if (options.last_event_id !== undefined) params.set("last_event_id", String(options.last_event_id));

    const qs = params.toString();
    const url = `${this.baseURL}/api/v1/events${qs ? "?" + qs : ""}`;
    const authHeaders = await this.authHeaders();

    const res = await this.fetchImpl(url, {
      headers: { ...authHeaders },
    });

    if (!res.ok) throw new Error(`SSE stream failed: HTTP ${res.status}`);
    if (!res.body) throw new Error("SSE stream: no response body");

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        let eventType = "";
        let eventId = "";
        let dataLines: string[] = [];

        for (const line of lines) {
          if (line.startsWith("event: ")) {
            eventType = line.slice(7);
          } else if (line.startsWith("id: ")) {
            eventId = line.slice(4);
          } else if (line.startsWith("data: ")) {
            dataLines.push(line.slice(6));
          } else if (line === "") {
            if (dataLines.length > 0) {
              try {
                const data = JSON.parse(dataLines.join("\n"));
                yield { type: eventType, id: eventId, data };
              } catch {
                // skip malformed events
              }
            }
            eventType = "";
            eventId = "";
            dataLines = [];
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  private async request<T>(path: string, init: RequestInit): Promise<T> {
    const authHeaders = await this.authHeaders();
    const attempts = this.retryMaxAttempts > 0 ? this.retryMaxAttempts : 1;
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < attempts; attempt++) {
      if (attempt > 0) {
        const jitter = 0.75 + Math.random() * 0.5; // 0.75x–1.25x
        await new Promise((r) => setTimeout(r, this.retryBaseDelay * jitter));
      }

      let res: Response;
      try {
        res = await this.fetchImpl(this.baseURL + path, {
          ...init,
          headers: {
            "content-type": "application/json",
            ...authHeaders,
            ...(init.headers || {}),
          },
        });
      } catch (err) {
        // Network error (connection refused, etc.)
        lastError = err instanceof Error ? err : new Error(String(err));
        if (attempt < attempts - 1) continue;
        throw lastError;
      }

      if (res.status === 502 || res.status === 503 || res.status === 429) {
        lastError = new Error(`HTTP ${res.status}`);
        if (attempt < attempts - 1) continue;
        throw lastError;
      }

      if (!res.ok) {
        let details = `HTTP ${res.status}`;
        let code = "";
        let uniqueJobId = "";
        try {
          const body = (await res.json()) as { error?: string; code?: string; unique_job_id?: string; unique_existing?: boolean };
          if (body.error) details = body.error;
          if (body.code) code = body.code;
          if (body.unique_job_id) uniqueJobId = body.unique_job_id;
        } catch {
          // ignore decode errors for non-JSON responses
        }
        if (code === "PAYLOAD_TOO_LARGE") throw new PayloadTooLargeError(details);
        if (res.status === 409) throw new UniqueConflictError(details, uniqueJobId);
        throw new Error(details);
      }

      if (res.status === 204) {
        return {} as T;
      }
      return (await res.json()) as T;
    }
    throw lastError ?? new Error("request failed");
  }

  async authHeaders(): Promise<Record<string, string>> {
    const out: Record<string, string> = {};
    if (this.auth.headers) {
      Object.assign(out, this.auth.headers);
    }
    if (this.auth.apiKey) {
      out[this.auth.apiKeyHeader || "X-API-Key"] = this.auth.apiKey;
    }
    let token = this.auth.bearerToken || "";
    if (this.auth.tokenProvider) {
      token = await this.auth.tokenProvider();
    }
    if (token) {
      out.Authorization = `Bearer ${token}`;
    }
    return out;
  }
}
