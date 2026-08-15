// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client

import scala.concurrent.Future

/**
 * The Scala client for the Parallel Consumer language proxy: key-ordered concurrent Kafka
 * processing driven from Scala, with the engine running as a sidecar child process and the user's
 * function running as an ordinary `InboundRecord => Future[Outcome]`.
 *
 * `scaladsl` rather than a package of its own, on the convention this ecosystem already reads that
 * way: the Java surface stays where Java users expect it and the Scala rendering sits beside it.
 *
 * The three aliases below are types this client imports rather than respells. The rule that decides
 * which is which lives in [[bz.stub.parallelconsumer.client.scaladsl.Bridge]]: a respelling earns
 * its place only where Scala genuinely says the thing better.
 */
package object scaladsl {

  /**
   * The user's function: one record in, an eventual verdict out.
   *
   * '''`Future` is this client's central idiomatic decision, and it is a decision about who the
   * client is for rather than about which effect type is nicest.''' cats-effect's `IO` and ZIO's
   * `Task` are both richer, and both interoperate with `Future` in one call (`IO.fromFuture`,
   * `ZIO.fromFuture`) - so a `Future` surface excludes neither, while an `IO` surface excludes ZIO
   * users, a `Task` surface excludes cats-effect users, and either adds a large dependency to a
   * client whose whole argument is thinness. `Future` is also what the standard library already
   * bridges to the transport's `CompletionStage`, so the translation costs nothing.
   *
   * A record is held for as long as the returned future takes to complete. There is no per-record
   * deadline and this client must never invent one - see
   * [[bz.stub.parallelconsumer.client.scaladsl.ParallelConsumerClient.noVerdict]] for the one way to
   * say "no verdict for this record" and what it is for.
   */
  type RecordProcessor = InboundRecord => Future[Outcome]

  /**
   * The ordering guarantee asked of the engine, enforced by the engine and never by this client.
   *
   * It is the reference API's own enum rather than a Scala copy. A three-constant enum is already
   * the same thing in both languages, and copying it would only create two places for a fourth
   * constant to be added.
   */
  type ProcessingOrder = bz.stub.parallelconsumer.client.ProcessingOrder

  /**
   * The proxy did something the frozen contract forbids: not a load condition, not retryable, and
   * the session is over. Detecting it is the transport's job and the type is the transport's own, so
   * every JVM client raises the same exception for the same breach.
   */
  type ProxyProtocolViolation = bz.stub.parallelconsumer.client.grpc.ProxyProtocolViolation

  /**
   * The negotiated session, as the proxy reported it in `Configured` - the effective values this
   * client is actually running under, never the ones it asked for.
   *
   * `capabilities` is the negotiated intersection, and it is the only thing that says which duties
   * exist on this session. This client declares `dispatch` and nothing else, so a session that comes
   * back with more tokens grants abilities it does not use; one that comes back with fewer would
   * mean the proxy cannot dispatch at all.
   */
  type Session = bz.stub.parallelconsumer.client.grpc.NegotiatedSession
}
