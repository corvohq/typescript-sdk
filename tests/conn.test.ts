/**
 * Unit tests for the Conn wire format, focused on lease_token encoding/decoding.
 *
 * These tests use a mock TCP server so no running Corvo server is required.
 * Run with: npx tsx tests/conn.test.ts
 */

import * as net from "node:net";
import { Conn, type AckJob, type FailJob } from "../packages/client/src/conn.js";

// ---------------------------------------------------------------------------
// Wire protocol constants (mirrored from conn.ts)
// ---------------------------------------------------------------------------

const HEADER_SIZE = 9;
const RESPONSE_BIT = 0x80;
const MSG_FETCH_BATCH = 0x02;
const MSG_ACK_BATCH = 0x03;
const MSG_FAIL_BATCH = 0x07;
const MSG_FETCH_BATCH_RESP = 0x82;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;

function assert(cond: boolean, msg: string): void {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

// ---------------------------------------------------------------------------
// Mock server helpers
// ---------------------------------------------------------------------------

function mockServer(
  handler: (conn: net.Socket) => void,
): Promise<{ host: string; port: number; cleanup: () => void }> {
  return new Promise((resolve) => {
    const server = net.createServer((conn) => {
      handler(conn);
    });
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as net.AddressInfo;
      resolve({
        host: addr.address,
        port: addr.port,
        cleanup: () => {
          server.close();
        },
      });
    });
  });
}

/** Read a complete frame from the socket. Returns { msgType, reqId, payload }. */
function readFrame(
  sock: net.Socket,
): Promise<{ msgType: number; reqId: number; payload: Buffer }> {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);

    const onData = (chunk: Buffer) => {
      buf = Buffer.concat([buf, chunk]);
      tryParse();
    };

    const onError = (err: Error) => {
      sock.off("data", onData);
      reject(err);
    };

    const tryParse = () => {
      if (buf.length < HEADER_SIZE) return;
      const msgType = buf[0];
      const reqId = buf.readUInt32LE(1);
      const payloadLen = buf.readUInt32LE(5);
      const frameSize = HEADER_SIZE + payloadLen;
      if (buf.length < frameSize) return;

      const payload = Buffer.alloc(payloadLen);
      buf.copy(payload, 0, HEADER_SIZE, frameSize);

      sock.off("data", onData);
      sock.off("error", onError);
      resolve({ msgType, reqId, payload });
    };

    sock.on("data", onData);
    sock.on("error", onError);
  });
}

/** Write a response frame to the socket. */
function writeFrame(
  sock: net.Socket,
  msgType: number,
  reqId: number,
  payload: Buffer,
): void {
  const hdr = Buffer.alloc(HEADER_SIZE);
  hdr[0] = msgType;
  hdr.writeUInt32LE(reqId, 1);
  hdr.writeUInt32LE(payload.length, 5);
  sock.write(Buffer.concat([hdr, payload]));
}

/** Write a u8-length-prefixed string into buf at offset, return new offset. */
function putLenPrefixed(buf: Buffer, off: number, s: string): number {
  const bytes = Buffer.from(s, "utf8");
  buf[off] = bytes.length;
  bytes.copy(buf, off + 1);
  return off + 1 + bytes.length;
}

/** Read a u8-length-prefixed string from buf at offset, return [string, newOffset]. */
function getLenPrefixed(buf: Buffer, off: number): [string, number] {
  const len = buf[off];
  const str = buf.toString("utf8", off + 1, off + 1 + len);
  return [str, off + 1 + len];
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function testFetchResponseWithLeaseToken() {
  console.log("--- test: fetch response parsing with lease_token ---");

  const leaseTokenValue = 0xDEADBEEF42n;

  const { host, port, cleanup } = await mockServer((sock) => {
    // Read the subscribe (fetch_batch) request, then push a response with 1 job
    readFrame(sock).then(({ reqId }) => {
      // Build fetch response payload: count(u16) + job fields
      const resp = Buffer.alloc(256);
      let off = 0;

      // count = 1
      resp.writeUInt16LE(1, off); off += 2;

      // job id
      off = putLenPrefixed(resp, off, "job-abc");
      // queue
      off = putLenPrefixed(resp, off, "emails");
      // attempt(u16)
      resp.writeUInt16LE(2, off); off += 2;
      // max_retries(u16)
      resp.writeUInt16LE(5, off); off += 2;
      // checkpoint (empty len-prefixed)
      off = putLenPrefixed(resp, off, "");
      // tags (empty len-prefixed)
      off = putLenPrefixed(resp, off, "");
      // payload: u16 len + data
      const payloadBytes = Buffer.from('{"hello":"world"}', "utf8");
      resp.writeUInt16LE(payloadBytes.length, off); off += 2;
      payloadBytes.copy(resp, off); off += payloadBytes.length;
      // lease_token(u64 LE)
      resp.writeBigUInt64LE(leaseTokenValue, off); off += 8;

      writeFrame(sock, MSG_FETCH_BATCH_RESP, reqId, resp.subarray(0, off));
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.subscribe(["emails"], "w1", 5, 30000);
      const jobs = await conn.readPushedJobs();

      assert(jobs.length === 1, `expected 1 job, got ${jobs.length}`);
      assert(jobs[0].id === "job-abc", `expected id=job-abc, got ${jobs[0].id}`);
      assert(jobs[0].queue === "emails", `expected queue=emails, got ${jobs[0].queue}`);
      assert(jobs[0].attempt === 2, `expected attempt=2, got ${jobs[0].attempt}`);
      assert(jobs[0].maxRetries === 5, `expected maxRetries=5, got ${jobs[0].maxRetries}`);
      assert(
        jobs[0].leaseToken === leaseTokenValue,
        `expected leaseToken=0x${leaseTokenValue.toString(16)}, got 0x${jobs[0].leaseToken.toString(16)}`,
      );

      const payloadStr = jobs[0].payload.toString("utf8");
      assert(
        payloadStr === '{"hello":"world"}',
        `expected payload={"hello":"world"}, got ${payloadStr}`,
      );
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testFetchResponseMultipleJobs() {
  console.log("--- test: fetch response with multiple jobs and distinct lease_tokens ---");

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ reqId }) => {
      const resp = Buffer.alloc(512);
      let off = 0;

      // count = 2
      resp.writeUInt16LE(2, off); off += 2;

      // Job 1
      off = putLenPrefixed(resp, off, "j1");
      off = putLenPrefixed(resp, off, "q1");
      resp.writeUInt16LE(1, off); off += 2; // attempt
      resp.writeUInt16LE(3, off); off += 2; // max_retries
      off = putLenPrefixed(resp, off, '{"step":1}'); // checkpoint
      off = putLenPrefixed(resp, off, ''); // tags
      const p1 = Buffer.from('{"a":1}', "utf8");
      resp.writeUInt16LE(p1.length, off); off += 2;
      p1.copy(resp, off); off += p1.length;
      resp.writeBigUInt64LE(111n, off); off += 8; // lease_token

      // Job 2
      off = putLenPrefixed(resp, off, "j2");
      off = putLenPrefixed(resp, off, "q2");
      resp.writeUInt16LE(4, off); off += 2; // attempt
      resp.writeUInt16LE(10, off); off += 2; // max_retries
      off = putLenPrefixed(resp, off, ""); // checkpoint
      off = putLenPrefixed(resp, off, ""); // tags
      resp.writeUInt16LE(0, off); off += 2; // no payload
      resp.writeBigUInt64LE(222n, off); off += 8; // lease_token

      writeFrame(sock, MSG_FETCH_BATCH_RESP, reqId, resp.subarray(0, off));
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.subscribe(["q1", "q2"], "w1", 10, 30000);
      const jobs = await conn.readPushedJobs();

      assert(jobs.length === 2, `expected 2 jobs, got ${jobs.length}`);
      assert(jobs[0].id === "j1", `job[0].id = ${jobs[0].id}`);
      assert(jobs[0].leaseToken === 111n, `job[0].leaseToken = ${jobs[0].leaseToken}, want 111n`);
      assert(jobs[1].id === "j2", `job[1].id = ${jobs[1].id}`);
      assert(jobs[1].leaseToken === 222n, `job[1].leaseToken = ${jobs[1].leaseToken}, want 222n`);
      assert(jobs[1].attempt === 4, `job[1].attempt = ${jobs[1].attempt}, want 4`);
      assert(jobs[1].maxRetries === 10, `job[1].maxRetries = ${jobs[1].maxRetries}, want 10`);
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testAckEncodingWithLeaseToken() {
  console.log("--- test: ack encoding with lease_token ---");

  const leaseTokenValue = 0xABCD1234n;

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, reqId, payload }) => {
      // Verify the ack payload bytes
      assert(msgType === MSG_ACK_BATCH, `expected msgType=0x03, got 0x${msgType.toString(16)}`);

      let off = 0;
      // count(u16)
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 1, `expected count=1, got ${count}`);

      // jobId (len-prefixed)
      let jobId: string;
      [jobId, off] = getLenPrefixed(payload, off);
      assert(jobId === "j1", `expected jobId=j1, got ${jobId}`);

      // queue (len-prefixed)
      let queue: string;
      [queue, off] = getLenPrefixed(payload, off);
      assert(queue === "work", `expected queue=work, got ${queue}`);

      // ack_status(u8)
      const ackStatus = payload[off]; off += 1;
      assert(ackStatus === 0, `expected ackStatus=0, got ${ackStatus}`);

      // flags(u8) — should have 0x08 set for lease_token
      const flags = payload[off]; off += 1;
      assert(flags === 0x08, `expected flags=0x08, got 0x${flags.toString(16)}`);

      // lease_token(u64 LE)
      const lt = payload.readBigUInt64LE(off); off += 8;
      assert(
        lt === leaseTokenValue,
        `expected lease_token=0x${leaseTokenValue.toString(16)}, got 0x${lt.toString(16)}`,
      );

      // Verify we consumed exactly the right number of bytes
      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);

      // Send ack response: [affected:u16][err_code:u8]
      const resp = Buffer.alloc(3);
      resp.writeUInt16LE(1, 0);
      resp[2] = 0;
      writeFrame(sock, MSG_ACK_BATCH | RESPONSE_BIT, reqId, resp);
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.ackBatch([
        { jobId: "j1", queue: "work", leaseToken: leaseTokenValue },
      ]);
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testAckEncodingWithOptionalFieldsAndLeaseToken() {
  console.log("--- test: ack encoding with result, holdReason, and lease_token ---");

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, reqId, payload }) => {
      assert(msgType === MSG_ACK_BATCH, `expected msgType=0x03, got 0x${msgType.toString(16)}`);

      let off = 0;
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 1, `expected count=1, got ${count}`);

      let jobId: string;
      [jobId, off] = getLenPrefixed(payload, off);
      assert(jobId === "j1", `expected jobId=j1, got ${jobId}`);

      let queue: string;
      [queue, off] = getLenPrefixed(payload, off);
      assert(queue === "work", `expected queue=work, got ${queue}`);

      const ackStatus = payload[off]; off += 1;
      assert(ackStatus === 1, `expected ackStatus=1 (hold), got ${ackStatus}`);

      // flags should be 0x01 (result) | 0x04 (holdReason) | 0x08 (leaseToken) = 0x0D
      const flags = payload[off]; off += 1;
      assert(flags === 0x0D, `expected flags=0x0D, got 0x${flags.toString(16)}`);

      // result (len-prefixed)
      let result: string;
      [result, off] = getLenPrefixed(payload, off);
      assert(result === '{"ok":true}', `expected result={"ok":true}, got ${result}`);

      // holdReason (len-prefixed) — flag 0x04 comes after 0x02 (checkpoint), but checkpoint is not set
      let holdReason: string;
      [holdReason, off] = getLenPrefixed(payload, off);
      assert(holdReason === "needs review", `expected holdReason=needs review, got ${holdReason}`);

      // lease_token(u64 LE)
      const lt = payload.readBigUInt64LE(off); off += 8;
      assert(lt === 0x999n, `expected lease_token=0x999, got 0x${lt.toString(16)}`);

      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);

      const resp = Buffer.alloc(3);
      resp.writeUInt16LE(1, 0);
      resp[2] = 0;
      writeFrame(sock, MSG_ACK_BATCH | RESPONSE_BIT, reqId, resp);
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.ackBatch([
        {
          jobId: "j1",
          queue: "work",
          ackStatus: 1, // hold
          result: '{"ok":true}',
          holdReason: "needs review",
          leaseToken: 0x999n,
        },
      ]);
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testAckEncodingWithoutLeaseToken() {
  console.log("--- test: ack encoding without lease_token (backward compat) ---");

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, reqId, payload }) => {
      assert(msgType === MSG_ACK_BATCH, `expected msgType=0x03, got 0x${msgType.toString(16)}`);

      let off = 0;
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 1, `expected count=1, got ${count}`);

      let jobId: string;
      [jobId, off] = getLenPrefixed(payload, off);
      assert(jobId === "j1", `expected jobId=j1, got ${jobId}`);

      let queue: string;
      [queue, off] = getLenPrefixed(payload, off);
      assert(queue === "work", `expected queue=work, got ${queue}`);

      const ackStatus = payload[off]; off += 1;
      assert(ackStatus === 0, `expected ackStatus=0, got ${ackStatus}`);

      // flags — 0x08 bit must NOT be set (no lease_token)
      const flags = payload[off]; off += 1;
      assert((flags & 0x08) === 0, `expected flag 0x08 unset, got flags=0x${flags.toString(16)}`);
      assert(flags === 0x00, `expected flags=0x00, got 0x${flags.toString(16)}`);

      // No lease_token bytes — payload should end here
      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);

      const resp = Buffer.alloc(3);
      resp.writeUInt16LE(1, 0);
      resp[2] = 0;
      writeFrame(sock, MSG_ACK_BATCH | RESPONSE_BIT, reqId, resp);
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.ackBatch([
        { jobId: "j1", queue: "work" },
      ]);
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testFailEncodingWithLeaseToken() {
  console.log("--- test: fail encoding with lease_token ---");

  const leaseTokenValue = 0xFEEDn;

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, reqId, payload }) => {
      assert(msgType === MSG_FAIL_BATCH, `expected msgType=0x07, got 0x${msgType.toString(16)}`);

      let off = 0;
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 1, `expected count=1, got ${count}`);

      let jobId: string;
      [jobId, off] = getLenPrefixed(payload, off);
      assert(jobId === "j1", `expected jobId=j1, got ${jobId}`);

      let queue: string;
      [queue, off] = getLenPrefixed(payload, off);
      assert(queue === "work", `expected queue=work, got ${queue}`);

      let errMsg: string;
      [errMsg, off] = getLenPrefixed(payload, off);
      assert(errMsg === "boom", `expected error=boom, got ${errMsg}`);

      let backtrace: string;
      [backtrace, off] = getLenPrefixed(payload, off);
      assert(backtrace === "", `expected empty backtrace, got ${backtrace}`);

      // flags — 0x01 means lease_token present
      const flags = payload[off]; off += 1;
      assert(flags === 0x01, `expected flags=0x01, got 0x${flags.toString(16)}`);

      // lease_token(u64 LE)
      const lt = payload.readBigUInt64LE(off); off += 8;
      assert(
        lt === leaseTokenValue,
        `expected lease_token=0x${leaseTokenValue.toString(16)}, got 0x${lt.toString(16)}`,
      );

      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);

      const resp = Buffer.alloc(3);
      resp.writeUInt16LE(1, 0);
      resp[2] = 0;
      writeFrame(sock, MSG_FAIL_BATCH | RESPONSE_BIT, reqId, resp);
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.failBatch([
        { jobId: "j1", queue: "work", error: "boom", leaseToken: leaseTokenValue },
      ]);
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testFailEncodingWithoutLeaseToken() {
  console.log("--- test: fail encoding without lease_token (backward compat) ---");

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, reqId, payload }) => {
      assert(msgType === MSG_FAIL_BATCH, `expected msgType=0x07, got 0x${msgType.toString(16)}`);

      let off = 0;
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 1, `expected count=1, got ${count}`);

      let jobId: string;
      [jobId, off] = getLenPrefixed(payload, off);

      let queue: string;
      [queue, off] = getLenPrefixed(payload, off);

      let errMsg: string;
      [errMsg, off] = getLenPrefixed(payload, off);
      assert(errMsg === "oops", `expected error=oops, got ${errMsg}`);

      let backtrace: string;
      [backtrace, off] = getLenPrefixed(payload, off);

      // flags — should be 0 (no lease_token)
      const flags = payload[off]; off += 1;
      assert(flags === 0x00, `expected flags=0x00, got 0x${flags.toString(16)}`);

      // No lease_token bytes
      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);

      const resp = Buffer.alloc(3);
      resp.writeUInt16LE(1, 0);
      resp[2] = 0;
      writeFrame(sock, MSG_FAIL_BATCH | RESPONSE_BIT, reqId, resp);
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.failBatch([
        { jobId: "j1", queue: "work", error: "oops" },
      ]);
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testSendAckFireAndForgetWithLeaseToken() {
  console.log("--- test: sendAck (fire-and-forget) encoding with lease_token ---");

  const leaseTokenValue = 0x5678EFABn;

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, payload }) => {
      assert(msgType === MSG_ACK_BATCH, `expected msgType=0x03, got 0x${msgType.toString(16)}`);

      let off = 0;
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 2, `expected count=2, got ${count}`);

      // Ack 1
      let id1: string;
      [id1, off] = getLenPrefixed(payload, off);
      let q1: string;
      [q1, off] = getLenPrefixed(payload, off);
      off += 1; // ack_status
      const flags1 = payload[off]; off += 1;
      assert(flags1 === 0x08, `ack1: expected flags=0x08, got 0x${flags1.toString(16)}`);
      const lt1 = payload.readBigUInt64LE(off); off += 8;
      assert(lt1 === leaseTokenValue, `ack1: expected lt=0x${leaseTokenValue.toString(16)}, got 0x${lt1.toString(16)}`);

      // Ack 2 — no lease_token
      let id2: string;
      [id2, off] = getLenPrefixed(payload, off);
      let q2: string;
      [q2, off] = getLenPrefixed(payload, off);
      off += 1; // ack_status
      const flags2 = payload[off]; off += 1;
      assert(flags2 === 0x00, `ack2: expected flags=0x00, got 0x${flags2.toString(16)}`);

      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);

      // sendAck is fire-and-forget, no response needed for the test to proceed,
      // but we don't need to write a response frame.
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.sendAck([
        { jobId: "j1", queue: "work", leaseToken: leaseTokenValue },
        { jobId: "j2", queue: "work" }, // no lease_token
      ]);
      // Give the mock server time to read and verify
      await new Promise((r) => setTimeout(r, 100));
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

async function testSendFailFireAndForgetWithLeaseToken() {
  console.log("--- test: sendFail (fire-and-forget) encoding with lease_token ---");

  const leaseTokenValue = 0xCAFEBABEn;

  const { host, port, cleanup } = await mockServer((sock) => {
    readFrame(sock).then(({ msgType, payload }) => {
      assert(msgType === MSG_FAIL_BATCH, `expected msgType=0x07, got 0x${msgType.toString(16)}`);

      let off = 0;
      const count = payload.readUInt16LE(off); off += 2;
      assert(count === 1, `expected count=1, got ${count}`);

      let jobId: string;
      [jobId, off] = getLenPrefixed(payload, off);
      let queue: string;
      [queue, off] = getLenPrefixed(payload, off);
      let errMsg: string;
      [errMsg, off] = getLenPrefixed(payload, off);
      let backtrace: string;
      [backtrace, off] = getLenPrefixed(payload, off);

      const flags = payload[off]; off += 1;
      assert(flags === 0x01, `expected flags=0x01, got 0x${flags.toString(16)}`);

      const lt = payload.readBigUInt64LE(off); off += 8;
      assert(
        lt === leaseTokenValue,
        `expected lease_token=0x${leaseTokenValue.toString(16)}, got 0x${lt.toString(16)}`,
      );

      assert(off === payload.length, `expected payload length=${off}, got ${payload.length}`);
    });
  });

  try {
    const conn = new Conn(host, port);
    try {
      await conn.sendFail([
        { jobId: "j1", queue: "work", error: "fail", leaseToken: leaseTokenValue },
      ]);
      await new Promise((r) => setTimeout(r, 100));
    } finally {
      conn.close();
    }
  } finally {
    cleanup();
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const tests = [
    ["fetch response with lease_token", testFetchResponseWithLeaseToken],
    ["fetch response multiple jobs", testFetchResponseMultipleJobs],
    ["ack encoding with lease_token", testAckEncodingWithLeaseToken],
    ["ack encoding with optional fields and lease_token", testAckEncodingWithOptionalFieldsAndLeaseToken],
    ["ack encoding without lease_token", testAckEncodingWithoutLeaseToken],
    ["fail encoding with lease_token", testFailEncodingWithLeaseToken],
    ["fail encoding without lease_token", testFailEncodingWithoutLeaseToken],
    ["sendAck fire-and-forget with lease_token", testSendAckFireAndForgetWithLeaseToken],
    ["sendFail fire-and-forget with lease_token", testSendFailFireAndForgetWithLeaseToken],
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
