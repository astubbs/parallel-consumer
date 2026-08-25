// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.time.{Duration => JavaDuration}

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import bz.stub.parallelconsumer.client.{ClientOptions => JavaClientOptions}
import bz.stub.parallelconsumer.client.{InboundRecord => JavaInboundRecord}
import bz.stub.parallelconsumer.client.{OutboundRecord => JavaOutboundRecord}
import bz.stub.parallelconsumer.client.{Outcome => JavaOutcome}

/**
 * '''This file is the entire cost of being a Scala client rather than a Java one, and it is here in
 * one place so that cost stays visible.'''
 *
 * Everything below translates between a type this client respells and the reference API's spelling
 * of the same thing. Nothing here decides anything: no session state, no protocol rule, no verdict.
 * A protobuf mapping had to be ''right''; this one only has to be ''faithful'', because the
 * transport underneath is the one every JVM client shares.
 *
 * '''The rule for adding to it''': a respelling earns its place only where Scala genuinely says the
 * thing better - `Option` instead of `Optional` and instead of `null`, default arguments instead of
 * a builder, a sealed trait instead of a boolean flag, one `Option[PreviousFailure]` instead of two
 * independent ones that can describe a state the wire cannot. Anything else is imported and used as
 * it stands; `ProcessingOrder`, `Session` and `ProxyProtocolViolation` are the three that already
 * were.
 */
private[scaladsl] object Bridge {

  def toJava(options: ClientOptions): JavaClientOptions = {
    val builder = JavaClientOptions
      .builder()
      .topics(options.topics.asJava)
      .kafkaProperties(options.kafkaProperties.asJava)
    options.maxConcurrency.foreach(builder.maxConcurrency(_))
    options.ordering.foreach(builder.ordering)
    // toNanos rather than a unit-by-unit conversion: FiniteDuration's nanosecond value is exact and
    // total, so the surface needs no rounding rule of its own
    options.commitInterval.foreach(interval => builder.commitInterval(JavaDuration.ofNanos(interval.toNanos)))
    options.defaultMessageRetryDelay
      .foreach(delay => builder.defaultMessageRetryDelay(JavaDuration.ofNanos(delay.toNanos)))
    builder.build()
  }

  /**
   * One delivered record in this client's spelling.
   *
   * The two `Optional` failure fields collapse into one `Option[PreviousFailure]`, which is the one
   * place this translation is not merely cosmetic: separately, a time and a reason can express "a
   * reason with no time", which the wire cannot say and no engine ever means.
   */
  def toScala(record: JavaInboundRecord): InboundRecord = new InboundRecord(
    topic = record.topic(),
    partition = record.partition(),
    offset = record.offset(),
    key = Option(record.key()),
    value = Option(record.value()),
    attempt = record.attempt(),
    previousFailure = record
      .lastFailureAt()
      .toScala
      .map(at => PreviousFailure(at, record.lastFailureReason().toScala)))

  /** The sealed verdict as the reference API's two-armed value. */
  def toJava(outcome: Outcome): JavaOutcome = outcome match {
    case Outcome.Success(produce) => JavaOutcome.success(produce.map(toJava).asJava)
    case Outcome.Failure(reason)  => JavaOutcome.failure(reason.orNull)
  }

  private def toJava(record: OutboundRecord): JavaOutboundRecord =
    JavaOutboundRecord.of(record.topic, record.key.orNull, record.value.orNull)
}
