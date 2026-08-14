// Copyright (C) 2026 Antony Stubbs and contributors

import { ProtocolViolationError } from "./errors";
import type { InboundRecord } from "./records";
import type { Token } from "./generated/parallelconsumer/proxy/v1/proxy";

/**
 * One dispatched record on its way to an executor.
 *
 * `token` is the object that came off the wire, carried unchanged. The client treats the token as
 * opaque - it never parses `recordId`, never compares epochs, and never rebuilds the message from
 * parsed fields - so echoing the received object is not a convention someone has to remember, it is
 * the only thing this type lets you do.
 */
export interface QueuedRecord {
  readonly token: Token;
  readonly record: InboundRecord;
}

/**
 * The queue between the proxy's dispatch and this client's executors, implementing the normative
 * rules of the client-authoring guide §3 (KTD39). All of them live here, in one file, because every
 * one is an ordering-or-liveness decision and scattering them is how they drift:
 *
 * 1. The admin always reads the stream and never applies backpressure by not reading - so `offer`
 *    is synchronous and never awaits an executor.
 * 2. The depth is `Configured.max_concurrency`, the proxy's OWN in-flight ceiling, so in a correct
 *    system it cannot overflow. Overflow is therefore a protocol violation, not a load condition:
 *    it throws, and the session fails. Records are never dropped and the queue never grows
 *    unbounded. The count is of UNRESOLVED records - queued plus executing - because that is what
 *    the ceiling bounds (the guide's worked example overflows on a fourth record while three are
 *    unresolved, two of them executing).
 * 3. Hand-out is FIFO, by arrival and within a wave by record order.
 * 4. A queued record is already leased, and queue time cannot expire it: the lease is extended by
 *    connection-level heartbeats, never by the record being worked on. Nothing here withholds a
 *    heartbeat, and nothing may be added that does.
 * 6. On connection loss (and, on a session that never negotiated `shutdown`, on close) the queue is
 *    DISCARDED: queued records are held by no live worker, so they must not appear in a reconnect
 *    manifest, and the proxy returns them to scheduling as unmanifested records.
 *
 * Rules 5 and 7 belong to waves this one defers - the `Shutdown` drain and a dynamic executor
 * count - and the accounting here is what they will need.
 */
export class DispatchQueue {
  private readonly waiting: QueuedRecord[] = [];
  private readonly takers: ((record: QueuedRecord | null) => void)[] = [];
  private unresolved = 0;
  private closed = false;

  constructor(private readonly maxConcurrency: number) {}

  /** Queued but not yet handed to an executor. */
  get depth(): number {
    return this.waiting.length;
  }

  /** Dispatched and not yet reported - queued plus executing. This is what the ceiling bounds. */
  get inFlight(): number {
    return this.unresolved;
  }

  /**
   * Accepts one dispatched record. Never blocks, never drops.
   *
   * @throws ProtocolViolationError when the proxy has exceeded its own declared in-flight ceiling.
   */
  offer(item: QueuedRecord): void {
    if (this.closed) {
      return;
    }
    if (this.unresolved + 1 > this.maxConcurrency) {
      throw new ProtocolViolationError(
        `the proxy dispatched a record while ${this.unresolved} were already in flight, past its ` +
          `own declared max_concurrency of ${this.maxConcurrency}`,
      );
    }
    this.unresolved += 1;
    const taker = this.takers.shift();
    if (taker !== undefined) {
      taker(item);
      return;
    }
    this.waiting.push(item);
  }

  /** FIFO hand-out. Resolves `null` once the queue is closed and drained. */
  take(): Promise<QueuedRecord | null> {
    const next = this.waiting.shift();
    if (next !== undefined) {
      return Promise.resolve(next);
    }
    if (this.closed) {
      return Promise.resolve(null);
    }
    return new Promise((resolve) => this.takers.push(resolve));
  }

  /** One record reached a verdict and was reported: it no longer counts against the ceiling. */
  settle(): void {
    if (this.unresolved > 0) {
      this.unresolved -= 1;
    }
  }

  /**
   * Stops hand-out and discards what was never handed out, returning it so the caller can account
   * for it. Executing records are untouched - they keep running and report normally.
   */
  close(): QueuedRecord[] {
    this.closed = true;
    const discarded = this.waiting.splice(0, this.waiting.length);
    this.unresolved -= discarded.length;
    for (const taker of this.takers.splice(0, this.takers.length)) {
      taker(null);
    }
    return discarded;
  }
}
