// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

import bz.stub.parallelconsumer.client.{InboundRecord => JavaInboundRecord}
import bz.stub.parallelconsumer.client.{Outcome => JavaOutcome}
import bz.stub.parallelconsumer.client.Outcomes
import bz.stub.parallelconsumer.client.scaladsl.ScalaTruth.assertThat
import org.junit.jupiter.api.{DisplayName, Test}

/**
 * The rule that a client never states a verdict for work it did not do, tested where it lives.
 *
 * It is worth its own file because it is a rule about '''silence''', and silence has no failure mode
 * anyone would notice: a fabricated success looks exactly like a real one to the proxy, to the
 * offsets, and to every log line either side of it. The wrong behaviour here is a record marked
 * complete that nobody processed, found much later as missing output.
 *
 * '''Each case is composed through `Outcomes.applyProcessorAsync`, which is the transport's own
 * translation''', rather than asserting on `startRecord`'s stage directly. That is deliberate: the
 * hazard is not that this client completes a stage, it is that a stage reaching the transport becomes
 * a report. Testing the pair is testing the thing that would actually go wrong.
 */
class NoVerdictIsInventedTest {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val record = new JavaInboundRecord("orders", 0, 7, null, "v".getBytes, 1, null, null)

  /** The composition the transport performs: this client's `startRecord` under `Outcomes`. */
  private def asTheTransportSeesIt(handingOut: Boolean, process: RecordProcessor) =
    Outcomes
      .applyProcessorAsync(
        inbound => ParallelConsumerClient.startRecord(handingOut, process, inbound),
        record)
      .toCompletableFuture

  @Test
  @DisplayName("a record handed out after hand-out stopped is never run and never reported")
  def aRecordHandedOutAfterHandOutStoppedIsNeverRunAndNeverReported(): Unit = {
    val ran = new AtomicBoolean(false)

    val verdict = asTheTransportSeesIt(
      handingOut = false,
      _ => {
        ran.set(true)
        Future.successful(Outcome.succeeded)
      })

    // not "reported as a failure", and not "reported as a success": not reported at all. The proxy
    // reclaims it as unheld once the session ends, which is the rule connection loss already
    // relies on
    assertThat(verdict.isDone).isFalse()
    assertThat(ran.get).isFalse()
  }

  @Test
  @DisplayName("a record whose future never completes reports nothing")
  def aRecordWhoseFutureNeverCompletesReportsNothing(): Unit = {
    val verdict = asTheTransportSeesIt(handingOut = true, _ => ParallelConsumerClient.noVerdict)

    // the window is not a timing assumption: an outcome that WOULD be fabricated is fabricated at
    // the moment the stage is composed, so a completed stage here would already be complete
    assertThat(verdict.isDone).isFalse()
  }

  @Test
  @DisplayName("an ordinary record still reaches a verdict")
  def anOrdinaryRecordStillReachesAVerdict(): Unit = {
    // the first control: the two silences above must not turn out to be the only behaviour there is
    val verdict = asTheTransportSeesIt(handingOut = true, _ => Future.successful(Outcome.succeeded))

    val outcome = verdict.get(Budget.toSeconds, TimeUnit.SECONDS)

    assertThat(outcome.isSuccess).isTrue()
  }

  @Test
  @DisplayName("a failed future IS a verdict, and carries the reason verbatim")
  def aFailedFutureISAVerdictAndCarriesTheReasonVerbatim(): Unit = {
    // the second control, and the hazard this whole file sits next to: on this surface the ONLY
    // silence is a future that never completes. A failed future is an ordinary failure report, so a
    // client that reached for it to mean "no verdict" would be putting a verdict on the wire
    val verdict = asTheTransportSeesIt(
      handingOut = true,
      _ => Future.failed(new IllegalStateException("the database said no")))

    val outcome: JavaOutcome = verdict.get(Budget.toSeconds, TimeUnit.SECONDS)

    assertThat(outcome.isSuccess).isFalse()
    assertThat(outcome.failureReason.orElse(null)).isEqualTo("the database said no")
  }

  @Test
  @DisplayName("a function that throws before returning a future is a verdict too")
  def aFunctionThatThrowsBeforeReturningAFutureIsAVerdictToo(): Unit = {
    val verdict =
      asTheTransportSeesIt(handingOut = true, _ => throw new IllegalStateException("the queue was closed"))

    val outcome = verdict.get(Budget.toSeconds, TimeUnit.SECONDS)

    assertThat(outcome.isSuccess).isFalse()
    assertThat(outcome.failureReason.orElse(null)).isEqualTo("the queue was closed")
  }

  private val Budget = 30.seconds
}
