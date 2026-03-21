import {
  CorvoClient,
  type AuthOptions,
  type ClientOptions,
  type BatchJob,
  type BatchConfig,
  type BatchResult,
  type EnqueueOptions,
  type FetchedJob,
  type FailResult,
  type HeartbeatResult,
  type SearchFilter,
  type SearchResult,
  type BulkRequest,
  type BulkResult,
  type BulkAsyncStart,
  type BulkTask,
  type ServerInfo,
  type SealBatchResult,
  type CorvoEvent,
  type SubscribeOptions,
} from "./index.js";

// ---------------------------------------------------------------------------
// Pooled auto-batching client
// ---------------------------------------------------------------------------

const MAX_BATCH = 256;

type BufferedEntry = {
  job: BatchJob;
  resolve: (id: string) => void;
  reject: (err: Error) => void;
};

/**
 * Auto-batching client that collects individual enqueue() calls and flushes
 * them as a single enqueueBatch() on the next microtask, or immediately when
 * the buffer reaches 256 entries.
 *
 * All other operations delegate directly to the underlying CorvoClient.
 */
export class Client {
  private readonly client: CorvoClient;
  private buffer: BufferedEntry[] = [];
  private flushScheduled = false;
  private maxBatch: number;

  constructor(
    baseURL: string,
    fetchImpl: typeof fetch = fetch,
    auth: AuthOptions = {},
    opts: ClientOptions = {},
    maxBatch: number = MAX_BATCH,
  ) {
    this.client = new CorvoClient(baseURL, fetchImpl, auth, opts);
    this.maxBatch = maxBatch;
  }

  // -------------------------------------------------------------------------
  // Auto-batched enqueue
  // -------------------------------------------------------------------------

  /**
   * Enqueue a job. The call is buffered and sent as part of a batch on the
   * next microtask (or immediately when the buffer reaches maxBatch).
   * Returns a Promise that resolves with the job_id once the batch is flushed.
   */
  enqueue(
    queue: string,
    payload: unknown,
    opts?: Partial<Omit<EnqueueOptions, "queue" | "payload">>,
  ): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      const job: BatchJob = { queue, payload, ...opts };
      this.buffer.push({ job, resolve, reject });

      if (this.buffer.length >= this.maxBatch) {
        this.flush();
      } else {
        this.scheduleFlush();
      }
    });
  }

  // -------------------------------------------------------------------------
  // Flush internals
  // -------------------------------------------------------------------------

  private scheduleFlush(): void {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    queueMicrotask(() => {
      this.flush();
    });
  }

  private flush(): void {
    this.flushScheduled = false;
    if (this.buffer.length === 0) return;

    // Drain the buffer — take everything up to maxBatch.
    // If more than maxBatch accumulated (shouldn't normally happen since we
    // flush at maxBatch), we take a slice and re-schedule.
    const batch = this.buffer.splice(0, this.maxBatch);
    if (this.buffer.length > 0) {
      this.scheduleFlush();
    }

    const jobs = batch.map((e) => e.job);

    this.client.enqueueBatch(jobs).then(
      (result: BatchResult) => {
        for (let i = 0; i < batch.length; i++) {
          batch[i].resolve(result.job_ids[i]);
        }
      },
      (err: unknown) => {
        const error = err instanceof Error ? err : new Error(String(err));
        for (const entry of batch) {
          entry.reject(error);
        }
      },
    );
  }

  // -------------------------------------------------------------------------
  // Passthrough — non-batched enqueue variants
  // -------------------------------------------------------------------------

  async enqueueBatch(jobs: BatchJob[], batch?: BatchConfig): Promise<BatchResult> {
    return this.client.enqueueBatch(jobs, batch);
  }

  async enqueueWith(opts: EnqueueOptions): Promise<import("./index.js").EnqueueResult> {
    return this.client.enqueueWith(opts);
  }

  // -------------------------------------------------------------------------
  // Passthrough — fetch / ack
  // -------------------------------------------------------------------------

  async fetch(
    queues: string[],
    workerID: string,
    hostname?: string,
    timeout?: number,
  ): Promise<FetchedJob | null> {
    return this.client.fetch(queues, workerID, hostname, timeout);
  }

  async fetchBatch(
    queues: string[],
    workerID: string,
    hostname?: string,
    timeout?: number,
    count?: number,
  ): Promise<{ jobs: FetchedJob[] }> {
    return this.client.fetchBatch(queues, workerID, hostname, timeout, count);
  }

  async ack(jobID: string, body?: Record<string, unknown>): Promise<{ status: string }> {
    return this.client.ack(jobID, body);
  }

  async ackBatch(
    acks: { job_id: string; result?: Record<string, unknown> }[],
  ): Promise<{ acked: number }> {
    return this.client.ackBatch(acks);
  }

  async fail(jobID: string, error: string, backtrace?: string): Promise<FailResult> {
    return this.client.fail(jobID, error, backtrace);
  }

  async heartbeat(jobs: Record<string, Record<string, unknown>>): Promise<HeartbeatResult> {
    return this.client.heartbeat(jobs);
  }

  // -------------------------------------------------------------------------
  // Passthrough — job management
  // -------------------------------------------------------------------------

  async getJob<T = Record<string, unknown>>(id: string): Promise<T> {
    return this.client.getJob<T>(id);
  }

  async cancelJob(id: string): Promise<void> {
    return this.client.cancelJob(id);
  }

  async moveJob(id: string, targetQueue: string): Promise<void> {
    return this.client.moveJob(id, targetQueue);
  }

  async deleteJob(id: string): Promise<void> {
    return this.client.deleteJob(id);
  }

  // -------------------------------------------------------------------------
  // Passthrough — search / bulk
  // -------------------------------------------------------------------------

  async search<T = Record<string, unknown>>(filter: SearchFilter): Promise<SearchResult<T>> {
    return this.client.search<T>(filter);
  }

  async bulk(req: BulkRequest): Promise<BulkResult | BulkAsyncStart> {
    return this.client.bulk(req);
  }

  async bulkStatus(id: string): Promise<BulkTask> {
    return this.client.bulkStatus(id);
  }

  async bulkGetJobs<T = Record<string, unknown>>(ids: string[]): Promise<T[]> {
    return this.client.bulkGetJobs<T>(ids);
  }

  // -------------------------------------------------------------------------
  // Passthrough — batch (job-group) management
  // -------------------------------------------------------------------------

  async createBatch(
    callbackQueue: string,
    callbackPayload?: unknown,
  ): Promise<{ batch_id: string }> {
    return this.client.createBatch(callbackQueue, callbackPayload);
  }

  async sealBatch(batchID: string): Promise<SealBatchResult> {
    return this.client.sealBatch(batchID);
  }

  // -------------------------------------------------------------------------
  // Passthrough — server info / events
  // -------------------------------------------------------------------------

  async getServerInfo(): Promise<ServerInfo> {
    return this.client.getServerInfo();
  }

  subscribe(options?: SubscribeOptions): AsyncGenerator<CorvoEvent> {
    return this.client.subscribe(options);
  }
}
