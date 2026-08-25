// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.time.Instant

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import bz.stub.parallelconsumer.client.{InboundRecord => JavaInboundRecord}
import bz.stub.parallelconsumer.client.ProcessingOrder
import bz.stub.parallelconsumer.client.scaladsl.ScalaTruth.assertThat
import org.junit.jupiter.api.{DisplayName, Test}

/**
 * The one translation this module performs, in both directions.
 *
 * Nothing here decides anything, which is exactly why it needs testing: a faithful mapping has no
 * symptom when it is wrong beyond a value quietly arriving as something else. The two cases worth
 * naming are the ones where the Scala spelling is not a rename - the failure history collapsing into
 * one `Option`, and `null` becoming `None` rather than reaching a Scala caller.
 */
class BridgeTest {

  @Test
  @DisplayName("options travel to the reference surface with every field the caller set")
  def optionsTravelToTheReferenceSurfaceWithEveryFieldTheCallerSet(): Unit = {
    val options = ClientOptions(
      topics = Seq("orders", "payments"),
      kafkaProperties = Map("bootstrap.servers" -> "localhost:9092"),
      maxConcurrency = Some(7),
      ordering = Some(ProcessingOrder.KEY),
      commitInterval = Some(100.millis),
      defaultMessageRetryDelay = Some(50.millis))

    val java = Bridge.toJava(options)

    assertThat(java.topics).containsExactly("orders", "payments").inOrder()
    assertThat(java.kafkaProperties).containsEntry("bootstrap.servers", "localhost:9092")
    assertThat(java.maxConcurrency.getAsInt).isEqualTo(7)
    assertThat(java.ordering.get).isEqualTo(ProcessingOrder.KEY)
    assertThat(java.commitInterval.get.toMillis).isEqualTo(100L)
    assertThat(java.defaultMessageRetryDelay.get.toMillis).isEqualTo(50L)
  }

  @Test
  @DisplayName("an unset option means take the engine's default, not a value of this client's choosing")
  def anUnsetOptionMeansTakeTheEngineSDefaultNotAValueOfThisClientSChoosing(): Unit = {
    val java = Bridge.toJava(ClientOptions(topics = Seq("orders")))

    assertThat(java.maxConcurrency.isPresent).isFalse()
    assertThat(java.ordering.isPresent).isFalse()
    assertThat(java.commitInterval.isPresent).isFalse()
    assertThat(java.defaultMessageRetryDelay.isPresent).isFalse()
  }

  @Test
  @DisplayName("a first delivery arrives with no failure history at all")
  def aFirstDeliveryArrivesWithNoFailureHistoryAtAll(): Unit = {
    val record = Bridge.toScala(new JavaInboundRecord("orders", 3, 42L, "k".getBytes, "v".getBytes, 1, null, null))

    assertThat(record.topic).isEqualTo("orders")
    assertThat(record.partition).isEqualTo(3)
    assertThat(record.offset).isEqualTo(42L)
    assertThat(record.attempt).isEqualTo(1)
    assertThat(record.previousFailure.isDefined).isFalse()
    assertThat(new String(record.key.get)).isEqualTo("k")
    assertThat(new String(record.value.get)).isEqualTo("v")
  }

  @Test
  @DisplayName("a redelivery carries one failure history rather than two independent fields")
  def aRedeliveryCarriesOneFailureHistoryRatherThanTwoIndependentFields(): Unit = {
    val failedAt = Instant.parse("2026-08-15T04:00:00Z")

    val record = Bridge.toScala(
      new JavaInboundRecord("orders", 0, 1L, null, null, 2, failedAt, "conformance-prescribed-failure"))

    // the collapse is the point: separately, a time and a reason could express "a reason with no
    // time", which the wire cannot say and no engine ever means
    val failure = record.previousFailure.getOrElse(fail("a redelivery must carry its failure history"))
    assertThat(failure.at).isEqualTo(failedAt)
    assertThat(failure.reason.orNull).isEqualTo("conformance-prescribed-failure")
  }

  @Test
  @DisplayName("an absent key or value is None rather than a null reaching a Scala caller")
  def anAbsentKeyOrValueIsNoneRatherThanANullReachingAScalaCaller(): Unit = {
    val record = Bridge.toScala(new JavaInboundRecord("orders", 0, 1L, null, null, 1, null, null))

    // a null VALUE is Kafka's tombstone, which is not an empty value - the distinction survives
    assertThat(record.key.isDefined).isFalse()
    assertThat(record.value.isDefined).isFalse()
  }

  @Test
  @DisplayName("a success carries its produced records to the reference surface")
  def aSuccessCarriesItsProducedRecordsToTheReferenceSurface(): Unit = {
    val outcome = Outcome.Success(Seq(OutboundRecord("out", Some("k".getBytes), Some("v".getBytes))))

    val java = Bridge.toJava(outcome)

    assertThat(java.isSuccess).isTrue()
    assertThat(java.produce.asScala.map(_.topic).asJava).containsExactly("out")
  }

  @Test
  @DisplayName("a failure carries its reason, and a failure without one is still a failure")
  def aFailureCarriesItsReasonAndAFailureWithoutOneIsStillAFailure(): Unit = {
    assertThat(Bridge.toJava(Outcome.failed("nope")).failureReason.orElse(null)).isEqualTo("nope")
    assertThat(Bridge.toJava(Outcome.Failure()).isSuccess).isFalse()
  }

  @Test
  @DisplayName("nothing that renders itself renders a credential or a payload")
  def nothingThatRendersItselfRendersACredentialOrAPayload(): Unit = {
    // the leak is never log.info(kafkaProperties) - it is a generated renderer invoked by a line
    // that names the object and looks harmless (guide §10.4), which is why the assertion is on the
    // TYPE rather than on any call site
    val options = ClientOptions(topics = Seq("orders"), kafkaProperties = Map("sasl.jaas.config" -> "hunter2"))
    assertThat(options.toString).doesNotContain("hunter2")
    assertThat(options.toString).contains("1 entries")

    val record = Bridge.toScala(
      new JavaInboundRecord("orders", 0, 1L, "secret-key".getBytes, "secret-value".getBytes, 1, null, null))
    assertThat(record.toString).doesNotContain("secret-key")
    assertThat(record.toString).doesNotContain("secret-value")

    assertThat(OutboundRecord("out", Some("secret-key".getBytes), Some("secret-value".getBytes)).toString)
      .doesNotContain("secret")

    val failure = PreviousFailure(Instant.EPOCH, Some("a reason nobody should see rendered"))
    assertThat(failure.toString).doesNotContain("nobody should see")
  }

  private def fail(message: String): Nothing = throw new AssertionError(message)
}
