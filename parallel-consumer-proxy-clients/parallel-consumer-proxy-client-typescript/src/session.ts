// Copyright (C) 2026 Antony Stubbs and contributors

import { type ClientDuplexStream, credentials } from "@grpc/grpc-js";

import { ProtocolViolationError, SessionClosedError } from "./errors";
import { type Outcome, type RecordProcessor, applyProcessor } from "./outcome";
import { DispatchQueue, type QueuedRecord } from "./queue";
import type { InboundRecord } from "./records";
import type { ResolvedOptions } from "./options";
import {
  type ClientMessage,
  CommitMode as WireCommitMode,
  type Configure,
  type Configured,
  type DispatchRecord,
  ProcessingOrder as WireProcessingOrder,
  ProxyServiceClient,
  type ProxyMessage,
  type Token,
} from "./generated/parallelconsumer/proxy/v1/proxy";
import type { Duration } from "./generated/google/protobuf/duration";

/** The v1 capability tokens, as the specification names them. */
export const Capability = {
  dispatch: "dispatch",
  heartbeat: "heartbeat",
  manifest: "manifest",
  workerDeath: "worker-death",
  shutdown: "shutdown",
  terminal: "terminal",
  setExecutorCount: "set-executor-count",
} as const;

/**
 * WHAT THIS CLIENT DECLARES IT IMPLEMENTS - and it is deliberately not an empty list.
 *
 * An empty `capabilities` means the v1 BASELINE, which is a promise to implement all six baseline
 * tokens. A partial client declaring nothing would be granted heartbeat, manifest, worker-death and
 * shutdown duties it does not perform - and the first consequence is a lease that expires because
 * nobody is heartbeating, returning every in-flight record on a timer. So the declared set is a
 * constant naming exactly what is implemented, and the wave that implements a duty adds its token
 * here. Declaring by omission cannot happen, because omission is not what this list expresses.
 */
export const DECLARED_CAPABILITIES: readonly string[] = [Capability.dispatch];

/** The effective session, as the proxy reported it. Assert on what came back, never what was asked. */
export interface SessionInfo {
  /** The effective in-flight ceiling, and this client's dispatch-queue depth. */
  readonly maxConcurrency: number;
  /** How many executors to run. Sent once, never revised. */
  readonly executorCount: number;
  /** The negotiated intersection of this client's declared set and the proxy's own. */
  readonly capabilities: ReadonlySet<string>;
  readonly topics: readonly string[];
  readonly topicPattern: string | null;
}

/**
 * The admin: it holds the one gRPC stream, owns the dispatch queue, and runs the executors.
 *
 * CONCURRENCY MODEL - PROMISE CONCURRENCY ON ONE EVENT LOOP, NOT `worker_threads`, and the choice
 * is deliberate:
 *
 * - It keeps the user's function a CLOSURE. `worker_threads` starts a fresh isolate with no shared
 *   heap, so a function can only reach it as a module path or a source string - which would make
 *   this the one client in the fan-out whose processor cannot close over anything, contradicting
 *   the reference surface ("a closure/lambda/callable, never an importable name").
 * - It is what Node's own concurrency is FOR. `executorCount` concurrent async invocations give
 *   real parallelism for the I/O-bound work that is the overwhelming majority of Node workloads -
 *   an HTTP call, a database round trip - because the event loop overlaps exactly those.
 * - THE HONEST LIMIT, stated rather than hidden: a processor that does CPU work SYNCHRONOUSLY
 *   blocks the event loop, and therefore blocks this stream - reports stop flowing, and once
 *   heartbeats exist they stop too, which expires leases and redelivers records the worker is
 *   still holding. Node's answer to that is for the application to offload its own CPU work (its
 *   own worker pool, or a native async call) and `await` it here, which composes with this model;
 *   a library-imposed worker-thread executor would force EVERY user into module-path processors to
 *   serve the minority case. A worker-thread executor mode can be added later as an OPTION without
 *   touching the wire - `executor_count` is all the protocol knows.
 *
 * The transport itself never blocks on a processor: `onMessage` queues synchronously and returns,
 * and the executors are separate async loops. That is the KTD39 rule this model can guarantee, and
 * the paragraph above is the part it cannot.
 */
export class Session {
  private readonly queue: DispatchQueue;
  private readonly executors: Promise<void>[] = [];
  private readonly ended: Promise<void>;
  private endSession!: () => void;
  private failSession!: (error: unknown) => void;

  private polled = false;
  private closing = false;
  private finished = false;
  /** Set when this client cancels the call, so the resulting CANCELLED is not reported twice. */
  private failure: unknown = undefined;

  private constructor(
    private readonly options: ResolvedOptions,
    private readonly grpc: ProxyServiceClient,
    private readonly call: ClientDuplexStream<ClientMessage, ProxyMessage>,
    readonly info: SessionInfo,
  ) {
    this.queue = new DispatchQueue(info.maxConcurrency);
    this.ended = new Promise<void>((resolve, reject) => {
      this.endSession = resolve;
      this.failSession = reject;
    });
    // Marks the rejection handled the moment it exists. An application that never calls done() -
    // entirely reasonable - must not take an unhandled-rejection crash for a session error it has
    // no interest in; one that does call done() still receives it, because attaching a handler
    // does not consume the rejection.
    this.ended.catch(() => undefined);

    this.call.on("data", (message: ProxyMessage) => { this.onMessage(message); });
    this.call.on("error", (error: Error) => { this.fail(error); });
    this.call.on("end", () => { this.finish(); });
  }

  /**
   * Connects, sends `Configure`, and waits for `Configured` - the handshake, after which the
   * session is open. Only after `Configured` arrives may anything else happen on this stream.
   */
  static async open(options: ResolvedOptions, port: number): Promise<Session> {
    const grpc = new ProxyServiceClient(`127.0.0.1:${port}`, credentials.createInsecure());
    const call = grpc.session();
    try {
      const configured = await handshake(call, configureFrom(options));
      return new Session(options, grpc, call, sessionInfoFrom(configured));
    } catch (error) {
      call.cancel();
      grpc.close();
      throw error;
    }
  }

  /**
   * Starts consumption. NON-BLOCKING, and at most once per client.
   *
   * See `ParallelConsumerClient.poll` for why this returns rather than blocking, and for how the
   * session's end is observed instead.
   */
  poll(processor: RecordProcessor): void {
    if (this.polled) {
      throw new SessionClosedError("poll() may be called at most once per client");
    }
    if (this.finished) {
      throw new SessionClosedError("the session has already ended");
    }
    this.polled = true;
    for (let executor = 0; executor < this.info.executorCount; executor += 1) {
      this.executors.push(this.runExecutor(processor));
    }
  }

  /** Resolves when the session ends cleanly; rejects with what ended it otherwise. */
  done(): Promise<void> {
    return this.ended;
  }

  /**
   * Client-initiated shutdown: stop hand-out, let executing records finish and report, then
   * half-close the stream. The half-close IS the shutdown signal - there is no request message.
   *
   * WHAT HAPPENS TO QUEUED RECORDS, and why it is not what the guide's §5 says. That section has a
   * client report every queued record `Released`, but the `Released` outcome is gated behind the
   * `shutdown` capability, and this wave negotiates only `dispatch`. Sending it anyway would be the
   * ordinary un-negotiated-message violation. So: `Released` when `shutdown` is negotiated, and
   * otherwise the queue is discarded and the proxy reclaims those records through its
   * connection-loss path, with their attempt counts unchanged. Recorded as a specification defect
   * in `docs/inflight/clients/typescript.md`; the `Released` half arrives with the drain wave.
   */
  async close(): Promise<void> {
    if (this.closing) {
      await this.settled();
      return;
    }
    this.closing = true;

    const discarded = this.queue.close();
    if (discarded.length > 0) {
      this.options.onWarning(
        `discarding ${discarded.length} queued record(s) at close: the session negotiated ` +
          `[${[...this.info.capabilities].join(", ")}], so "${Capability.shutdown}" is off and ` +
          "Released may not be sent; the proxy returns them to scheduling",
      );
    }

    await Promise.allSettled(this.executors);
    if (!this.finished) {
      this.call.end();
    }
    await this.settled();
    this.grpc.close();
  }

  /** One executor: take a record, run the function, report the outcome, repeat. */
  private async runExecutor(processor: RecordProcessor): Promise<void> {
    for (;;) {
      const item = await this.queue.take();
      if (item === null) {
        return;
      }
      const outcome = await applyProcessor(processor, item.record);
      try {
        await this.send(reportFor(item, outcome));
      } catch (error) {
        // The stream is gone. The record is not this client's to resolve any more - the proxy's
        // reconnect window and lease machinery own it from here - so stop rather than spin.
        this.fail(error);
        return;
      } finally {
        this.queue.settle();
      }
    }
  }

  /** Everything the proxy says, and what this session is entitled to do about it. */
  private onMessage(message: ProxyMessage): void {
    if (message.dispatch !== undefined) {
      this.onDispatch(message.dispatch.records ?? []);
      return;
    }
    if (message.configured !== undefined) {
      // A second Configured is the proxy's truthful refusal of a reconfiguration. This client sends
      // one Configure and never reconfigures, so seeing one means the proxy is answering something
      // this client did not ask.
      this.options.onWarning("ignoring a second Configured: this client never reconfigures");
      return;
    }
    if (message.setExecutorCount !== undefined) {
      // Declared in the schema and never sent by a v1 proxy; the specification says a v1 client
      // treats one as a protocol violation rather than acting on it.
      this.fail(
        new ProtocolViolationError(
          "the proxy sent SetExecutorCount, which no v1 proxy sends and no v1 client implements",
        ),
      );
      return;
    }
    // Drop and Shutdown are gated by `manifest` and `shutdown`; neither is negotiated on this
    // session, so neither may arrive. The specification's receiver rule allows ignoring or failing;
    // ignoring keeps a proxy-side bug from destroying work this client is still doing.
    const kind = message.drop !== undefined ? "Drop" : message.shutdown !== undefined ? "Shutdown" : "an empty";
    this.options.onWarning(
      `ignoring ${kind} message: it is outside the negotiated capability set ` +
        `[${[...this.info.capabilities].join(", ")}]`,
    );
  }

  /** A wave: queue every record, in wave order. Never blocks, never drops. */
  private onDispatch(records: readonly DispatchRecord[]): void {
    for (const dispatched of records) {
      let queued: QueuedRecord;
      try {
        queued = toQueuedRecord(dispatched);
        this.queue.offer(queued);
      } catch (error) {
        // Queue overflow is the proxy exceeding its own declared ceiling. The specification says to
        // fail the stream with FAILED_PRECONDITION naming the count - which no gRPC CLIENT can do,
        // since only a server sets a status. Cancelling is the client-side equivalent, and the
        // count travels in the error this raises instead. See errors.ts.
        this.fail(error);
        return;
      }
    }
  }

  /** Serialized, backpressure-aware write. The callback fires once the frame is flushed. */
  private send(message: ClientMessage): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.call.write(message, (error?: Error | null) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  private fail(error: unknown): void {
    if (this.finished) {
      return;
    }
    if (this.failure !== undefined) {
      return;
    }
    this.failure = error;
    this.finished = true;
    this.queue.close();
    this.call.cancel();
    this.failSession(error);
  }

  private finish(): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.queue.close();
    this.endSession();
  }

  /** Waits for the stream to end, bounded - a proxy that never completes must not hang close(). */
  private async settled(): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    const bound = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, this.options.shutdownTimeoutMs);
      timer.unref();
    });
    await Promise.race([this.ended.catch(() => undefined), bound]);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

/** Sends `Configure` and waits for the `Configured` that opens the session. */
function handshake(
  call: ClientDuplexStream<ClientMessage, ProxyMessage>,
  configure: Configure,
): Promise<Configured> {
  return new Promise<Configured>((resolve, reject) => {
    const onData = (message: ProxyMessage) => {
      cleanup();
      if (message.configured === undefined) {
        reject(
          new ProtocolViolationError(
            "the proxy's first message on a fresh session was not Configured",
          ),
        );
        return;
      }
      resolve(message.configured);
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onEnd = () => {
      cleanup();
      reject(new SessionClosedError("the proxy closed the stream before answering Configure"));
    };
    const cleanup = () => {
      call.off("data", onData);
      call.off("error", onError);
      call.off("end", onEnd);
    };
    call.on("data", onData);
    call.on("error", onError);
    call.on("end", onEnd);
    call.write({ configure });
  });
}

function configureFrom(options: ResolvedOptions): Configure {
  return {
    topics: options.topics === undefined ? undefined : [...options.topics],
    topicPattern: options.topicPattern,
    maxConcurrency: options.maxConcurrency,
    kafkaProperties: { ...options.kafkaProperties },
    capabilities: [...DECLARED_CAPABILITIES],
    ordering: wireOrdering(options.ordering),
    commitMode: wireCommitMode(options.commitMode),
    commitInterval: wireDuration(options.commitIntervalMs),
    defaultMessageRetryDelay: wireDuration(options.defaultMessageRetryDelayMs),
    pcInstanceTag: options.instanceTag,
  };
}

/**
 * Reads the effective configuration.
 *
 * `max_concurrency` and `executor_count` are always set despite their `optional` markers - a
 * `Configured` missing either is a protocol violation, and absence never means "unlimited". So they
 * are checked rather than defaulted: a fabricated ceiling here would turn a proxy bug into an
 * unbounded queue.
 */
function sessionInfoFrom(configured: Configured): SessionInfo {
  const maxConcurrency = configured.maxConcurrency;
  const executorCount = configured.executorCount;
  if (maxConcurrency === undefined || maxConcurrency < 1) {
    throw new ProtocolViolationError(
      "Configured carried no usable max_concurrency; absence never means unlimited",
    );
  }
  if (executorCount === undefined || executorCount < 1) {
    throw new ProtocolViolationError("Configured carried no usable executor_count");
  }
  return {
    maxConcurrency,
    executorCount,
    capabilities: new Set(configured.capabilities ?? []),
    topics: configured.topics ?? [],
    topicPattern: configured.topicPattern ?? null,
  };
}

/** The wire form of one dispatched record, with the token carried through untouched. */
function toQueuedRecord(dispatched: DispatchRecord): QueuedRecord {
  const token: Token | undefined = dispatched.token;
  const wire = dispatched.record;
  if (token === undefined || wire === undefined) {
    throw new ProtocolViolationError(
      "a dispatched record arrived without its token or its record",
    );
  }
  const record: InboundRecord = {
    topic: wire.topic ?? "",
    partition: wire.partition ?? 0,
    offset: wire.offset ?? 0n,
    key: wire.key ?? null,
    value: wire.value ?? null,
    attempt: dispatched.attempt ?? 0,
    lastFailureAt: dispatched.lastFailureAt ?? null,
    lastFailureReason: dispatched.lastFailureReason ?? null,
  };
  return { token, record };
}

/**
 * The report for one settled record. The token is the object that arrived, echoed byte-identically
 * because it is never taken apart. An outcome is always present: a `Report` without one is a
 * protocol violation, and this union has no arm that could produce it.
 */
function reportFor(item: QueuedRecord, outcome: Outcome): ClientMessage {
  if (outcome.kind === "success") {
    return {
      report: {
        token: item.token,
        success: {
          produce: outcome.produce.map((record) => ({
            topic: record.topic,
            key: record.key ?? undefined,
            value: record.value ?? undefined,
          })),
        },
      },
    };
  }
  return {
    report: {
      token: item.token,
      failure: { reason: outcome.reason ?? undefined },
    },
  };
}

function wireOrdering(ordering: ResolvedOptions["ordering"]): WireProcessingOrder | undefined {
  switch (ordering) {
    case "unordered":
      return WireProcessingOrder.PROCESSING_ORDER_UNORDERED;
    case "partition":
      return WireProcessingOrder.PROCESSING_ORDER_PARTITION;
    case "key":
      return WireProcessingOrder.PROCESSING_ORDER_KEY;
    default:
      return undefined;
  }
}

function wireCommitMode(mode: ResolvedOptions["commitMode"]): WireCommitMode | undefined {
  switch (mode) {
    case "periodic-consumer-sync":
      return WireCommitMode.COMMIT_MODE_PERIODIC_CONSUMER_SYNC;
    case "periodic-consumer-async":
      return WireCommitMode.COMMIT_MODE_PERIODIC_CONSUMER_ASYNCHRONOUS;
    default:
      return undefined;
  }
}

function wireDuration(milliseconds: number | undefined): Duration | undefined {
  if (milliseconds === undefined) {
    return undefined;
  }
  return {
    seconds: BigInt(Math.trunc(milliseconds / 1000)),
    nanos: (milliseconds % 1000) * 1_000_000,
  };
}
