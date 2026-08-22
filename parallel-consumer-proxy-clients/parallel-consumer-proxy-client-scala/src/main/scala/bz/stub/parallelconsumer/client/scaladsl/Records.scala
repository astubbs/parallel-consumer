// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.time.Instant

/**
 * What the engine knows about this record's previous delivery. Its presence ''is'' the statement
 * "has failed before"; two independently optional fields would let a caller ask a question - a
 * reason without a time - that the wire cannot answer.
 */
final case class PreviousFailure(at: Instant, reason: Option[String]) {

  /** The reason is worker-supplied text, so its length is the safe rendering (guide §10.5). */
  override def toString: String =
    s"PreviousFailure(at=$at, reason=${reason.fold("none")(text => s"<${text.length} chars>")})"
}

/**
 * One record as delivered to the user's function: the Kafka record plus the delivery state an
 * in-process Parallel Consumer function would see. Nothing transport-shaped appears here - no token,
 * no epoch, no connection identity - because none of it is the user's business.
 *
 * `key` and `value` are the bytes Kafka held; this client never deserializes. `null` is Kafka's own
 * distinction rather than an absence of information - a null value is a tombstone, which is not an
 * empty value - and it is kept as `Option` so a Scala caller never meets a `null`. The arrays are
 * not copied: treat them as read-only.
 *
 * '''A plain class rather than a case class, and the reason is the arrays.''' A case class's
 * generated `equals` compares `Array[Byte]` by reference, so two deliveries of byte-identical
 * payloads would compare unequal while looking like values that should compare equal - the one
 * comparison anybody would actually reach for on this type. A class with no `equals` at all makes
 * the reference comparison explicit instead of dressing it up as structural.
 */
final class InboundRecord(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: Option[Array[Byte]],
    val value: Option[Array[Byte]],
    /** 1 on first delivery, 2 on the first redelivery. */
    val attempt: Int,
    /** `None` before the first failure. */
    val previousFailure: Option[PreviousFailure]) {

  /** Deliberately omits key and value: payloads are untrusted input and never appear in a log line. */
  override def toString: String = s"InboundRecord($topic-$partition@$offset, attempt $attempt)"
}

/**
 * A record a successful outcome asks the engine to produce - the only sanctioned route for a
 * worker's Kafka output. Workers never produce directly; the proxy produces with its own producer
 * before the input record's offset may become eligible to commit.
 *
 * A plain class for the same reason as [[InboundRecord]].
 */
final class OutboundRecord(
    val topic: String,
    /** `None` for a keyless record. */
    val key: Option[Array[Byte]] = None,
    /** `None` for a tombstone. */
    val value: Option[Array[Byte]] = None) {

  require(topic.nonEmpty, "an OutboundRecord needs a destination topic")

  /** Deliberately omits key and value: payloads are untrusted input and never appear in a log line. */
  override def toString: String = s"OutboundRecord($topic)"
}

object OutboundRecord {

  /** Constructed without `new`, the way a case class would be - the class is plain only for `equals`. */
  def apply(
      topic: String,
      key: Option[Array[Byte]] = None,
      value: Option[Array[Byte]] = None): OutboundRecord = new OutboundRecord(topic, key, value)

  /** The bytes form, for a caller that already has arrays rather than options. */
  def of(topic: String, key: Array[Byte], value: Array[Byte]): OutboundRecord =
    new OutboundRecord(topic, Option(key), Option(value))
}
