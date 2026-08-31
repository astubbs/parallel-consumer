// Copyright (C) 2026 Antony Stubbs and contributors

import { type ClientOptions, type ResolvedOptions, resolveOptions } from "./options";
import type { RecordProcessor } from "./outcome";
import { Session, type SessionInfo } from "./session";
import { Sidecar } from "./sidecar";

/**
 * Parallel Consumer's user-facing surface in TypeScript: build options, open a client, hand it a
 * processor, and every record is delivered to that function exactly as an in-process Parallel
 * Consumer user function would see it.
 *
 * ```ts
 * const client = await ParallelConsumerClient.open({
 *   sidecar: { executable: "/opt/parallel-consumer/proxy" },
 *   topics: ["orders"],
 *   kafkaProperties: { "bootstrap.servers": "localhost:9092", "group.id": "orders-app" },
 * });
 * client.poll(async (record) => {
 *   await handle(record.value);          // returning nothing is a success
 * });
 * await client.done();                   // the session's end, whenever it comes
 * ```
 *
 * The client is an `AsyncDisposable`, so `await using client = await ParallelConsumerClient.open(…)`
 * closes it at the end of the block.
 */
export class ParallelConsumerClient implements AsyncDisposable {
  private closed: Promise<void> | undefined;

  private constructor(
    private readonly options: ResolvedOptions,
    private readonly sidecar: Sidecar,
    private readonly connection: Session,
  ) {}

  /**
   * Spawns the sidecar, connects, and completes the handshake. Resolves once `Configured` has
   * arrived - which is the only point at which a session may be treated as open.
   */
  static async open(options: ClientOptions): Promise<ParallelConsumerClient> {
    const resolved = resolveOptions(options);
    const sidecar = await Sidecar.start(resolved.sidecar, resolved);
    try {
      const session = await Session.open(resolved, sidecar.port);
      return new ParallelConsumerClient(resolved, sidecar, session);
    } catch (error) {
      await sidecar.stop(resolved.shutdownTimeoutMs);
      throw error;
    }
  }

  /** The effective configuration the proxy reported. Assert on this, never on what was asked for. */
  get session(): SessionInfo {
    return this.connection.info;
  }

  /**
   * Starts consumption, handing every delivered record to `processor`. At most once per client.
   *
   * IT DOES NOT BLOCK, and the choice is a deliberate one the specification leaves open. A call
   * that blocked for the session's life would be un-idiomatic here twice over: it would have to be
   * `await`ed, so the same `async` function could never also call {@link close}; and JavaScript has
   * no way to interrupt it from elsewhere without a second concurrency mechanism. So consumption
   * starts, this returns, and the session's END is a separate thing to await:
   *
   * - {@link done} resolves when the session ends cleanly and rejects with whatever ended it;
   * - {@link close} shuts down and resolves when everything has drained.
   *
   * One mechanism, not two: there is no event emitter beside the promises. `onWarning` in the
   * options is for things the client notices and does NOT act on, which is a different question.
   */
  poll(processor: RecordProcessor): void {
    this.connection.poll(processor);
  }

  /**
   * Resolves when the session ends cleanly, rejects with what ended it - a protocol violation, a
   * transport failure, or the proxy going away.
   *
   * Calling it is optional. A client that never does is not at risk of an unhandled rejection: the
   * session marks its own end handled the moment it exists, and this still delivers it to a caller.
   */
  done(): Promise<void> {
    return this.connection.done();
  }

  /**
   * Stops consumption and releases everything: the queue, the stream, the channel, the child
   * process. Idempotent, and safe to call while records are executing - those finish and report
   * before the stream half-closes.
   */
  close(): Promise<void> {
    this.closed ??= this.shutdown();
    return this.closed;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  private async shutdown(): Promise<void> {
    try {
      await this.connection.close();
    } finally {
      // Closing the sidecar's stdin is the parent-death signal, and the only clean way to reap it.
      // It runs even if the session's own close failed - a leaked JVM still holds group membership.
      await this.sidecar.stop(this.options.shutdownTimeoutMs);
    }
  }
}
