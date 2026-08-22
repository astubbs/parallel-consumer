// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import scala.concurrent.duration.FiniteDuration

/**
 * Connect-time configuration, and the only configuration channel there is: these values travel in
 * the session's `Configure` message and nowhere else - no file, no environment variable, no shell,
 * no command line.
 *
 * '''`None` means "take the engine's default".''' That is why this class holds an `Option` where the
 * reference Java surface holds an `Optional`, and why it needs no builder: default arguments say
 * "unset" once, at the declaration, and named arguments make a call that sets three of six fields as
 * readable as a builder chain. What the default resolved to is reported back in [[Session]] - assert
 * what came back, never what was asked for.
 *
 * `kafkaProperties` is credential-bearing. [[toString]] omits it deliberately, and nothing in this
 * library logs it at any level (guide §6, §10.4). '''The hand-written renderer is the whole point of
 * overriding it on a case class''': the compiler-generated one prints every field it has, including
 * the property map, into any log line that so much as mentions the object.
 */
final case class ClientOptions(
    /** The subscription, fixed for the session's lifetime. At least one topic is required. */
    topics: Seq[String],
    /** Kafka client configuration - bootstrap servers, group id, credentials. Never logged. */
    kafkaProperties: Map[String, String] = Map.empty,
    /** The in-flight ceiling: records the proxy may have out to this client at once. */
    maxConcurrency: Option[Int] = None,
    /** The ordering guarantee. */
    ordering: Option[ProcessingOrder] = None,
    /** How often the engine commits offsets. */
    commitInterval: Option[FiniteDuration] = None,
    /** How long a failed record waits before redelivery. */
    defaultMessageRetryDelay: Option[FiniteDuration] = None) {

  require(topics.nonEmpty, "at least one topic is required: the subscription is fixed at connect time")
  require(
    maxConcurrency.forall(_ >= 1),
    s"maxConcurrency must be at least 1 when set, got ${maxConcurrency.getOrElse(0)} - there is no 'unlimited'")

  /** Deliberately omits [[kafkaProperties]]: it may carry credentials. */
  override def toString: String =
    s"ClientOptions(topics=$topics, kafkaProperties=<${kafkaProperties.size} entries>, " +
      s"maxConcurrency=$maxConcurrency, ordering=$ordering, commitInterval=$commitInterval, " +
      s"defaultMessageRetryDelay=$defaultMessageRetryDelay)"
}
