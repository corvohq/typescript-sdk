import * as net from "node:net";

// ---------------------------------------------------------------------------
// Wire protocol constants
// ---------------------------------------------------------------------------

const HEADER_SIZE = 9; // msg_type(1) + req_id(4) + payload_len(4)
const RESPONSE_BIT = 0x80;

const MSG_ENQUEUE_BATCH = 0x01;
const MSG_FETCH_BATCH = 0x02;
const MSG_ACK_BATCH = 0x03;
const MSG_PING = 0x04;
const MSG_HEARTBEAT = 0x06;
const MSG_FAIL_BATCH = 0x07;
const MSG_ERROR = 0xff;

const MSG_FETCH_BATCH_RESP = 0x82;

// ---------------------------------------------------------------------------
// Backoff constants
// ---------------------------------------------------------------------------

export const BACKOFF_NONE = 0;
export const BACKOFF_FIXED = 1;
export const BACKOFF_LINEAR = 2;
export const BACKOFF_EXPONENTIAL = 3;

// ---------------------------------------------------------------------------
// Ack status constants
// ---------------------------------------------------------------------------

export const ACK_DONE = 0;
export const ACK_HOLD = 1;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type EnqueueJob = {
  queue: string;
  jobId: string;
  priority?: number;      // default 0
  maxRetries?: number;    // default 3
  backoff?: number;       // default 0
  baseDelayMs?: number;   // default 0
  maxDelayMs?: number;    // default 0
  uniquePeriodS?: number; // default 0
  scheduledAtNs?: bigint; // default 0n
  expireAfterMs?: number; // default 0
  chainStep?: number;     // default 0
  payload?: Buffer;       // optional
  uniqueKey?: string;     // optional
  tags?: string;          // optional
  batchId?: string;       // optional
  chainId?: string;       // optional
  chainConfig?: string;   // optional
  group?: string;         // optional
  parentId?: string;      // optional
};

export type AckJob = {
  jobId: string;
  queue: string;
  ackStatus?: number;   // default ACK_DONE
  result?: string;      // optional
  checkpoint?: string;  // optional
  holdReason?: string;  // optional
};

export type FailJob = {
  jobId: string;
  queue: string;
  error: string;
  backtrace?: string;   // default ""
};

export type HeartbeatJob = {
  jobId: string;
  queue: string;
  progress?: string;    // optional
  checkpoint?: string;  // optional
};

export type ConnFetchedJob = {
  id: string;
  queue: string;
  attempt: number;
  maxRetries: number;
  checkpoint: string;
  tags: string;
  payload: Buffer;
};

type PendingRequest = {
  resolve: (buf: Buffer) => void;
  reject: (err: Error) => void;
  expectedType: number;
};

// ---------------------------------------------------------------------------
// Conn — binary RPC connection
// ---------------------------------------------------------------------------

export class Conn {
  private socket: net.Socket | null = null;
  private host: string;
  private port: number;
  private reqId: number = 0;
  private sendBuf: Buffer;
  private recvBuf: Buffer;
  private recvLen: number = 0;
  private pending: Map<number, PendingRequest> = new Map();
  private connectPromise: Promise<void> | null = null;
  private pushedFrames: { msgType: number; payload: Buffer }[] = [];
  private pushWaiters: { resolve: (frame: { msgType: number; payload: Buffer }) => void; reject: (err: Error) => void }[] = [];

  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
    this.sendBuf = Buffer.alloc(64 * 1024);
    this.recvBuf = Buffer.alloc(64 * 1024);
  }

  // -------------------------------------------------------------------------
  // Connection lifecycle
  // -------------------------------------------------------------------------

  private connect(): Promise<void> {
    if (this.socket && !this.socket.destroyed) {
      return Promise.resolve();
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connectPromise = new Promise<void>((resolve, reject) => {
      const sock = new net.Socket();
      sock.setNoDelay(true);

      sock.on("connect", () => {
        this.socket = sock;
        this.connectPromise = null;
        resolve();
      });

      sock.on("error", (err) => {
        this.connectPromise = null;
        reject(err);
        this.rejectAll(err);
      });

      sock.on("close", () => {
        this.socket = null;
        this.rejectAll(new Error("connection closed"));
      });

      sock.on("data", (chunk: Buffer) => {
        this.onData(chunk);
      });

      sock.connect(this.port, this.host);
    });

    return this.connectPromise;
  }

  close(): void {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.rejectAll(new Error("connection closed"));
  }

  private rejectAll(err: Error): void {
    for (const [, req] of this.pending) {
      req.reject(err);
    }
    this.pending.clear();
    for (const waiter of this.pushWaiters) {
      waiter.reject(err);
    }
    this.pushWaiters.length = 0;
  }

  // -------------------------------------------------------------------------
  // Data reception and frame parsing
  // -------------------------------------------------------------------------

  private onData(chunk: Buffer): void {
    // Grow receive buffer if needed
    while (this.recvLen + chunk.length > this.recvBuf.length) {
      const newBuf = Buffer.alloc(this.recvBuf.length * 2);
      this.recvBuf.copy(newBuf, 0, 0, this.recvLen);
      this.recvBuf = newBuf;
    }

    chunk.copy(this.recvBuf, this.recvLen);
    this.recvLen += chunk.length;

    this.processFrames();
  }

  private processFrames(): void {
    while (this.recvLen >= HEADER_SIZE) {
      const msgType = this.recvBuf[0];
      const reqId = this.recvBuf.readUInt32LE(1);
      const payloadLen = this.recvBuf.readUInt32LE(5);
      const frameSize = HEADER_SIZE + payloadLen;

      if (this.recvLen < frameSize) {
        break; // incomplete frame
      }

      const payload = Buffer.alloc(payloadLen);
      this.recvBuf.copy(payload, 0, HEADER_SIZE, frameSize);

      // Shift remaining data to front of buffer
      this.recvBuf.copy(this.recvBuf, 0, frameSize, this.recvLen);
      this.recvLen -= frameSize;

      const req = this.pending.get(reqId);
      if (req) {
        this.pending.delete(reqId);
        if (msgType === MSG_ERROR) {
          const errMsg = payload.length > 0 ? payload.toString("utf8") : "server error";
          req.reject(new Error(errMsg));
        } else {
          req.resolve(payload);
        }
      } else if (msgType === MSG_FETCH_BATCH_RESP || msgType === MSG_ERROR) {
        // Pushed frame (from subscription) — deliver to waiter or queue
        const frame = { msgType, payload };
        const waiter = this.pushWaiters.shift();
        if (waiter) {
          waiter.resolve(frame);
        } else {
          this.pushedFrames.push(frame);
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Send helpers
  // -------------------------------------------------------------------------

  private ensureSendBuf(needed: number): void {
    if (this.sendBuf.length < needed) {
      this.sendBuf = Buffer.alloc(Math.max(needed, this.sendBuf.length * 2));
    }
  }

  /** Write a u8-length-prefixed UTF-8 string. */
  private writeLenPrefixed(buf: Buffer, offset: number, s: string): number {
    const bytes = Buffer.from(s, "utf8");
    if (bytes.length > 255) {
      throw new Error(`lenPrefixed string too long: ${bytes.length} bytes`);
    }
    buf[offset] = bytes.length;
    bytes.copy(buf, offset + 1);
    return offset + 1 + bytes.length;
  }

  /** Write a u16-length-prefixed Buffer (for payload). */
  private writeLenPrefixedBuf(buf: Buffer, offset: number, data: Buffer): number {
    if (data.length > 65535) {
      throw new Error(`lenPrefixedBuf too long: ${data.length} bytes`);
    }
    buf.writeUInt16LE(data.length, offset);
    data.copy(buf, offset + 2);
    return offset + 2 + data.length;
  }

  private readLenPrefixed(buf: Buffer, offset: number): [string, number] {
    const len = buf[offset];
    const str = buf.toString("utf8", offset + 1, offset + 1 + len);
    return [str, offset + 1 + len];
  }

  private async sendFrame(msgType: number, payload: Buffer): Promise<Buffer> {
    await this.connect();

    const id = ++this.reqId;
    const frame = Buffer.alloc(HEADER_SIZE + payload.length);
    frame[0] = msgType;
    frame.writeUInt32LE(id, 1);
    frame.writeUInt32LE(payload.length, 5);
    payload.copy(frame, HEADER_SIZE);

    const responseType = msgType | RESPONSE_BIT;

    return new Promise<Buffer>((resolve, reject) => {
      this.pending.set(id, { resolve, reject, expectedType: responseType });
      this.socket!.write(frame, (err) => {
        if (err) {
          this.pending.delete(id);
          reject(err);
        }
      });
    });
  }

  private async sendFrameFireAndForget(msgType: number, payload: Buffer): Promise<void> {
    await this.connect();

    const id = ++this.reqId;
    const frame = Buffer.alloc(HEADER_SIZE + payload.length);
    frame[0] = msgType;
    frame.writeUInt32LE(id, 1);
    frame.writeUInt32LE(payload.length, 5);
    payload.copy(frame, HEADER_SIZE);

    return new Promise<void>((resolve, reject) => {
      this.socket!.write(frame, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  // -------------------------------------------------------------------------
  // Timestamp helper
  // -------------------------------------------------------------------------

  private nowNs(): bigint {
    const ms = Date.now();
    return BigInt(ms) * 1_000_000n;
  }

  // -------------------------------------------------------------------------
  // Ack encoding helper
  // -------------------------------------------------------------------------

  private encodeAcks(buf: Buffer, offset: number, acks: AckJob[]): number {
    let off = offset;
    buf.writeUInt16LE(acks.length, off); off += 2;

    for (const ack of acks) {
      off = this.writeLenPrefixed(buf, off, ack.jobId);
      off = this.writeLenPrefixed(buf, off, ack.queue);

      buf[off] = ack.ackStatus ?? ACK_DONE; off += 1;

      let flags = 0;
      if (ack.result !== undefined && ack.result !== "") flags |= 0x01;
      if (ack.checkpoint !== undefined && ack.checkpoint !== "") flags |= 0x02;
      if (ack.holdReason !== undefined && ack.holdReason !== "") flags |= 0x04;
      buf[off] = flags; off += 1;

      if (flags & 0x01) off = this.writeLenPrefixed(buf, off, ack.result!);
      if (flags & 0x02) off = this.writeLenPrefixed(buf, off, ack.checkpoint!);
      if (flags & 0x04) off = this.writeLenPrefixed(buf, off, ack.holdReason!);
    }

    return off;
  }

  private acksSizeEstimate(acks: AckJob[]): number {
    let size = 2; // count(u16)
    for (const ack of acks) {
      size += 1 + Buffer.byteLength(ack.jobId, "utf8");   // lenPrefixed id
      size += 1 + Buffer.byteLength(ack.queue, "utf8");   // lenPrefixed queue
      size += 1; // ack_status(u8)
      size += 1; // flags(u8)
      if (ack.result !== undefined && ack.result !== "")
        size += 1 + Buffer.byteLength(ack.result, "utf8");
      if (ack.checkpoint !== undefined && ack.checkpoint !== "")
        size += 1 + Buffer.byteLength(ack.checkpoint, "utf8");
      if (ack.holdReason !== undefined && ack.holdReason !== "")
        size += 1 + Buffer.byteLength(ack.holdReason, "utf8");
    }
    return size;
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async enqueueBatch(jobs: EnqueueJob[]): Promise<number> {
    // Calculate payload size
    let size = 2 + 8; // count(u16) + now_ns(u64)
    for (const job of jobs) {
      size += 1 + Buffer.byteLength(job.queue, "utf8"); // lenPrefixed queue
      size += 1 + Buffer.byteLength(job.jobId, "utf8"); // lenPrefixed id
      size += 1;  // priority(u8)
      size += 2;  // max_retries(u16)
      size += 1;  // backoff(u8)
      size += 4;  // base_delay_ms(u32)
      size += 4;  // max_delay_ms(u32)
      size += 4;  // unique_period_s(u32)
      size += 8;  // scheduled_at_ns(u64)
      size += 4;  // expire_after_ms(u32)
      size += 2;  // chain_step(u16)
      size += 2;  // flags(u16)
      // Optional fields
      if (job.payload && job.payload.length > 0) size += 2 + job.payload.length;
      if (job.uniqueKey) size += 1 + Buffer.byteLength(job.uniqueKey, "utf8");
      if (job.tags) size += 1 + Buffer.byteLength(job.tags, "utf8");
      if (job.batchId) size += 1 + Buffer.byteLength(job.batchId, "utf8");
      if (job.chainId) size += 1 + Buffer.byteLength(job.chainId, "utf8");
      if (job.chainConfig) size += 1 + Buffer.byteLength(job.chainConfig, "utf8");
      if (job.group) size += 1 + Buffer.byteLength(job.group, "utf8");
      if (job.parentId) size += 1 + Buffer.byteLength(job.parentId, "utf8");
    }

    this.ensureSendBuf(size);
    const buf = this.sendBuf;
    let off = 0;

    buf.writeUInt16LE(jobs.length, off); off += 2;
    buf.writeBigUInt64LE(this.nowNs(), off); off += 8;

    for (const job of jobs) {
      off = this.writeLenPrefixed(buf, off, job.queue);
      off = this.writeLenPrefixed(buf, off, job.jobId);
      buf[off] = job.priority ?? 0; off += 1;
      buf.writeUInt16LE(job.maxRetries ?? 3, off); off += 2;
      buf[off] = job.backoff ?? 0; off += 1;
      buf.writeUInt32LE(job.baseDelayMs ?? 0, off); off += 4;
      buf.writeUInt32LE(job.maxDelayMs ?? 0, off); off += 4;
      buf.writeUInt32LE(job.uniquePeriodS ?? 0, off); off += 4;
      buf.writeBigUInt64LE(job.scheduledAtNs ?? 0n, off); off += 8;
      buf.writeUInt32LE(job.expireAfterMs ?? 0, off); off += 4;
      buf.writeUInt16LE(job.chainStep ?? 0, off); off += 2;

      // Compute flags
      let flags = 0;
      if (job.payload && job.payload.length > 0) flags |= 0x0001;
      if (job.uniqueKey) flags |= 0x0002;
      if (job.tags) flags |= 0x0004;
      if (job.batchId) flags |= 0x0008;
      if (job.chainId) flags |= 0x0010;
      if (job.chainConfig) flags |= 0x0020;
      if (job.group) flags |= 0x0040;
      if (job.parentId) flags |= 0x0080;
      buf.writeUInt16LE(flags, off); off += 2;

      // Write optional fields
      if (flags & 0x0001) off = this.writeLenPrefixedBuf(buf, off, job.payload!);
      if (flags & 0x0002) off = this.writeLenPrefixed(buf, off, job.uniqueKey!);
      if (flags & 0x0004) off = this.writeLenPrefixed(buf, off, job.tags!);
      if (flags & 0x0008) off = this.writeLenPrefixed(buf, off, job.batchId!);
      if (flags & 0x0010) off = this.writeLenPrefixed(buf, off, job.chainId!);
      if (flags & 0x0020) off = this.writeLenPrefixed(buf, off, job.chainConfig!);
      if (flags & 0x0040) off = this.writeLenPrefixed(buf, off, job.group!);
      if (flags & 0x0080) off = this.writeLenPrefixed(buf, off, job.parentId!);
    }

    const payload = Buffer.alloc(off);
    buf.copy(payload, 0, 0, off);

    const resp = await this.sendFrame(MSG_ENQUEUE_BATCH, payload);

    // Response: [count:u16LE][err_code:u8]
    const count = resp.readUInt16LE(0);
    const errCode = resp[2];
    if (errCode !== 0) {
      throw new Error(`enqueue failed with error code ${errCode}`);
    }
    return count;
  }

  /**
   * Subscribe to a set of queues with the given number of credits.
   * This is fire-and-forget: it sends a MSG_FETCH_BATCH frame but does NOT
   * wait for a response. The server will push MSG_FETCH_BATCH_RESP frames
   * asynchronously as jobs become available; read them with readPushedJobs().
   *
   * Call subscribe() again to replenish credits after consuming pushed jobs.
   */
  async subscribe(
    queues: string[],
    workerId: string = "ts-worker",
    credits: number = 10,
    leaseMs: number = 30000,
  ): Promise<void> {
    // Calculate payload size
    let size = 8 + 2 + 4; // now_ns(u64) + count(u16) + lease_ms(u32)
    const workerIdBytes = Buffer.from(workerId, "utf8");
    size += 1 + workerIdBytes.length; // lenPrefixed worker_id
    size += 1; // queue_count(u8)
    for (const q of queues) {
      const qBytes = Buffer.from(q, "utf8");
      size += 1 + qBytes.length; // lenPrefixed queue
    }

    this.ensureSendBuf(size);
    const buf = this.sendBuf;
    let off = 0;

    buf.writeBigUInt64LE(this.nowNs(), off); off += 8;
    buf.writeUInt16LE(credits, off); off += 2;
    buf.writeUInt32LE(leaseMs, off); off += 4;
    off = this.writeLenPrefixed(buf, off, workerId);
    buf[off] = queues.length; off += 1;
    for (const q of queues) {
      off = this.writeLenPrefixed(buf, off, q);
    }

    const payload = Buffer.alloc(off);
    buf.copy(payload, 0, 0, off);

    await this.sendFrameFireAndForget(MSG_FETCH_BATCH, payload);
  }

  /**
   * Read the next pushed batch of jobs from the server.
   * Blocks until a MSG_FETCH_BATCH_RESP frame arrives.
   * Throws on MSG_ERROR or unexpected message types.
   */
  async readPushedJobs(): Promise<ConnFetchedJob[]> {
    const frame = await this.nextPushedFrame();

    if (frame.msgType === MSG_ERROR) {
      const errMsg = frame.payload.length > 0 ? frame.payload.toString("utf8") : "server error";
      throw new Error(errMsg);
    }

    if (frame.msgType !== MSG_FETCH_BATCH_RESP) {
      throw new Error(`unexpected pushed message type: 0x${frame.msgType.toString(16)}`);
    }

    return this.decodeFetchResponse(frame.payload);
  }

  private nextPushedFrame(): Promise<{ msgType: number; payload: Buffer }> {
    const queued = this.pushedFrames.shift();
    if (queued) {
      return Promise.resolve(queued);
    }
    return new Promise((resolve, reject) => {
      this.pushWaiters.push({ resolve, reject });
    });
  }

  async ackBatch(acks: AckJob[]): Promise<void> {
    // Calculate payload size
    let size = 8; // now_ns(u64)
    size += this.acksSizeEstimate(acks);

    this.ensureSendBuf(size);
    const buf = this.sendBuf;
    let off = 0;

    buf.writeBigUInt64LE(this.nowNs(), off); off += 8;
    off = this.encodeAcks(buf, off, acks);

    const payload = Buffer.alloc(off);
    buf.copy(payload, 0, 0, off);

    const resp = await this.sendFrame(MSG_ACK_BATCH, payload);

    // Response: [affected:u16LE][err_code:u8]
    const errCode = resp[2];
    if (errCode !== 0) {
      throw new Error(`ack failed with error code ${errCode}`);
    }
  }

  async failBatch(jobs: FailJob[]): Promise<void> {
    // Calculate payload size
    let size = 8 + 2; // now_ns(u64) + count(u16)
    for (const job of jobs) {
      size += 1 + Buffer.byteLength(job.jobId, "utf8");
      size += 1 + Buffer.byteLength(job.queue, "utf8");
      size += 1 + Buffer.byteLength(job.error, "utf8");
      size += 1 + Buffer.byteLength(job.backtrace ?? "", "utf8");
    }

    this.ensureSendBuf(size);
    const buf = this.sendBuf;
    let off = 0;

    buf.writeBigUInt64LE(this.nowNs(), off); off += 8;
    buf.writeUInt16LE(jobs.length, off); off += 2;

    for (const job of jobs) {
      off = this.writeLenPrefixed(buf, off, job.jobId);
      off = this.writeLenPrefixed(buf, off, job.queue);
      off = this.writeLenPrefixed(buf, off, job.error);
      off = this.writeLenPrefixed(buf, off, job.backtrace ?? "");
    }

    const payload = Buffer.alloc(off);
    buf.copy(payload, 0, 0, off);

    const resp = await this.sendFrame(MSG_FAIL_BATCH, payload);

    // Response: [affected:u16LE][err_code:u8]
    const errCode = resp[2];
    if (errCode !== 0) {
      throw new Error(`fail failed with error code ${errCode}`);
    }
  }

  async heartbeat(workerId: string, jobs: HeartbeatJob[]): Promise<void> {
    // Calculate payload size
    let size = 1 + Buffer.byteLength(workerId, "utf8"); // lenPrefixed worker_id
    size += 2; // count(u16)
    for (const job of jobs) {
      size += 1 + Buffer.byteLength(job.jobId, "utf8");
      size += 1 + Buffer.byteLength(job.queue, "utf8");
      size += 1; // flags(u8)
      if (job.progress !== undefined && job.progress !== "")
        size += 1 + Buffer.byteLength(job.progress, "utf8");
      if (job.checkpoint !== undefined && job.checkpoint !== "")
        size += 1 + Buffer.byteLength(job.checkpoint, "utf8");
    }

    this.ensureSendBuf(size);
    const buf = this.sendBuf;
    let off = 0;

    off = this.writeLenPrefixed(buf, off, workerId);
    buf.writeUInt16LE(jobs.length, off); off += 2;

    for (const job of jobs) {
      off = this.writeLenPrefixed(buf, off, job.jobId);
      off = this.writeLenPrefixed(buf, off, job.queue);

      let flags = 0;
      if (job.progress !== undefined && job.progress !== "") flags |= 0x01;
      if (job.checkpoint !== undefined && job.checkpoint !== "") flags |= 0x02;
      buf[off] = flags; off += 1;

      if (flags & 0x01) off = this.writeLenPrefixed(buf, off, job.progress!);
      if (flags & 0x02) off = this.writeLenPrefixed(buf, off, job.checkpoint!);
    }

    const payload = Buffer.alloc(off);
    buf.copy(payload, 0, 0, off);

    const resp = await this.sendFrame(MSG_HEARTBEAT, payload);

    // Response: [affected:u16LE][err_code:u8]
    const errCode = resp[2];
    if (errCode !== 0) {
      throw new Error(`heartbeat failed with error code ${errCode}`);
    }
  }

  async ping(): Promise<void> {
    await this.sendFrame(MSG_PING, Buffer.alloc(0));
  }

  // -------------------------------------------------------------------------
  // Response decoders
  // -------------------------------------------------------------------------

  private decodeFetchResponse(resp: Buffer): ConnFetchedJob[] {
    const jobs: ConnFetchedJob[] = [];
    let off = 0;

    const count = resp.readUInt16LE(off); off += 2;

    for (let i = 0; i < count; i++) {
      let id: string;
      [id, off] = this.readLenPrefixed(resp, off);

      let queue: string;
      [queue, off] = this.readLenPrefixed(resp, off);

      const attempt = resp.readUInt16LE(off); off += 2;
      const maxRetries = resp.readUInt16LE(off); off += 2;

      let checkpoint: string;
      [checkpoint, off] = this.readLenPrefixed(resp, off);

      let tags: string;
      [tags, off] = this.readLenPrefixed(resp, off);

      const payloadLen = resp.readUInt16LE(off); off += 2;
      const payload = Buffer.alloc(payloadLen);
      resp.copy(payload, 0, off, off + payloadLen);
      off += payloadLen;

      jobs.push({ id, queue, attempt, maxRetries, checkpoint, tags, payload });
    }

    return jobs;
  }
}
