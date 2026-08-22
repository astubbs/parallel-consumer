// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.nio.file.{Files, Paths}
import java.util.concurrent.CopyOnWriteArrayList

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._

import bz.stub.parallelconsumer.client.scaladsl.ScalaTruth.assertThat
import org.junit.jupiter.api.{DisplayName, Test}

/**
 * This wave's end-to-end proof: one record, through the real test-mode sidecar, over the real wire.
 *
 * The sidecar is `TestModeMain` from the proxy module's '''test''' jar, spawned as an ordinary child
 * process - so this test exercises the whole lifecycle contract the specification describes and not
 * an in-process shortcut: launch directly, hold the stdin pipe, find the port line, connect to
 * loopback, handshake, dispatch, report, half-close, reap.
 *
 * '''It runs only in the harness lane''' (`-Dpc.foreignClients`, which this module's CI row passes),
 * because the classpath it needs is what a permanent Maven edge to the engine module would cost -
 * see the `scala-e2e-harness` profile. When the classpath file is missing it FAILS and names the
 * command; a test that quietly does not run is not a passing test.
 *
 * Three harness limitations are absorbed here rather than worked around: its stdout logs before the
 * port line (the client scans, so nothing to do), it serves until stdin EOF rather than exiting after
 * a drain (so the reap is closing stdin, which is what the client does anyway), and '''it has no
 * verdict channel''' - it cannot be asked what the engine made of a report, so everything asserted
 * here is a wire-observable consequence.
 *
 * That third limitation is why there are two tests and not one. The first proves the lifecycle and
 * the delivery state; what it cannot prove is that a report ever reached the engine, because silence
 * follows a success and an unreported record alike. The second proves exactly that, and its own
 * documentation records the measurement that established the difference.
 */
class OneRecordThroughTheSidecarTest {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  @Test
  @DisplayName("a processed record advances the committed offset")
  def aProcessedRecordAdvancesTheCommittedOffset(): Unit = {
    // the scenario name is also the topic name: the harness seeds its records on a topic named
    // after the scenario
    val scenario = "a-processed-record-advances-the-committed-offset"
    val deliveries = new CopyOnWriteArrayList[InboundRecord]()
    val firstDelivery = Promise[InboundRecord]()

    val client = await(
      ParallelConsumerClient.open(
        // kafkaProperties is empty deliberately: --mock builds mock Kafka clients and reads none,
        // and real credentials never belong in a conformance test
        ClientOptions(topics = Seq(scenario)),
        harnessSidecar(scenario)),
      HandshakeBudget)

    try {
      // the capability set is the transport's declaration, asserted here because this client
      // inherits it rather than choosing it - a transport that started claiming more would silently
      // claim duties this wave does not perform
      assertThat(client.session.capabilities).containsExactly("dispatch")
      assertThat(client.session.executorCount).isAtLeast(1)
      assertThat(client.session.maxConcurrency).isAtLeast(1)

      val session = client.poll { record =>
        deliveries.add(record)
        firstDelivery.trySuccess(record)
        Future.successful(Outcome.succeeded)
      }

      val record = await(firstDelivery.future, DispatchBudget)
      assertThat(record.topic).isEqualTo(scenario)
      assertThat(record.partition).isEqualTo(0)
      assertThat(record.offset).isEqualTo(0L)
      assertThat(record.attempt).isEqualTo(1)
      assertThat(record.previousFailure.isDefined).isFalse()
      assertThat(record.value.isDefined).isTrue()

      // a success is followed by silence rather than a redelivery - the wire-observable form of the
      // offset advancing past it, which the harness has no channel to state directly
      Thread.sleep(SilenceWindow.toMillis)
      assertThat(deliveries.asScala.toList.size).isEqualTo(1)

      // the session's future is the transport's own session end, so closing completes it - a client
      // whose poll cannot end is the defect this shape exists to remove
      client.close()
      await(session, ShutdownBudget)
    } finally client.close()
  }

  /**
   * The second scenario, and the reason it is here rather than in a later wave: '''it is the only
   * instrument either of these tests has that can tell a client which reports from one which does
   * not.'''
   *
   * The test above cannot. Its assertion is that a success is followed by silence - and a record
   * that is never reported is also followed by silence, because the engine holds an unreported
   * record rather than redelivering it while the session lives. Measured rather than reasoned about:
   * a client sabotaged to report nothing at all left the test above '''green''' (it took 25 seconds
   * instead of 5, which is the drain budget, and passed). A redelivery is different - it can only
   * happen because a report arrived, was applied, and moved the attempt count - so this is the one
   * that goes red.
   *
   * The Kotlin client's end-to-end test is the same shape and has the same blind spot; see
   * `docs/inflight/clients/scala.md`.
   */
  @Test
  @DisplayName("a failed record is redelivered with its history, which is how a report proves it landed")
  def aFailedRecordIsRedeliveredWithItsHistory(): Unit = {
    val scenario = "a-failed-record-is-redelivered-with-its-failure-history"
    val deliveries = new CopyOnWriteArrayList[InboundRecord]()
    val redelivered = Promise[InboundRecord]()

    val client = await(
      ParallelConsumerClient.open(
        ClientOptions(
          topics = Seq(scenario),
          // the conformance contract's own tunables: the engine's production defaults are a 5s
          // commit interval and a 1s retry delay, which would make this converge at the pace of a
          // production deployment rather than of a unit test
          commitInterval = Some(100.millis),
          defaultMessageRetryDelay = Some(50.millis)),
        harnessSidecar(scenario)),
      HandshakeBudget)

    try {
      val session = client.poll { record =>
        deliveries.add(record)
        if (record.attempt >= 2) {
          redelivered.trySuccess(record)
          Future.successful(Outcome.succeeded)
        } else {
          Future.successful(Outcome.failed(PrescribedFailureReason))
        }
      }

      val second = await(redelivered.future, DispatchBudget)
      assertThat(second.offset).isEqualTo(deliveries.get(0).offset)
      assertThat(second.attempt).isEqualTo(2)
      // verbatim, which is the property that fails if the reason is composed, wrapped or rendered
      // anywhere on its way to the wire and back
      assertThat(second.previousFailure.flatMap(_.reason).orNull).isEqualTo(PrescribedFailureReason)

      client.close()
      await(session, ShutdownBudget)
    } finally client.close()
  }

  private def await[A](future: Future[A], budget: FiniteDuration): A = Await.result(future, budget)

  /**
   * The sidecar command for one scenario. `TestModeMain` ships in a test jar, so "the sidecar binary"
   * here is the JVM launcher and the classpath is an argument - everything awkward about that lives
   * in this one method.
   */
  private def harnessSidecar(scenario: String): SidecarCommand = {
    val classpathFile = Paths.get("target", "sidecar-classpath.txt").toAbsolutePath
    if (!Files.isRegularFile(classpathFile)) {
      throw new IllegalStateException(
        s"$classpathFile is missing - it is written by the scala-e2e-harness profile: run " +
          "`./mvnw --batch-mode test -pl :parallel-consumer-proxy-client-scala -am -Dpc.foreignClients`")
    }
    val classpath = Files.readString(classpathFile).trim
    if (classpath.isEmpty) {
      throw new IllegalStateException(s"$classpathFile is empty")
    }

    // PATH lookup would be wrong in the library and is unnecessary here: this is the JVM the test
    // itself is running on
    val java = Paths.get(System.getProperty("java.home"), "bin", "java")
    SidecarCommand(java, Seq("-cp", classpath, HarnessMain, "--mock", "--scenario", scenario))
  }

  private val HarnessMain = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"
  /** The conformance contract's fixed literal, asserted to come back byte for byte. */
  private val PrescribedFailureReason = "conformance-prescribed-failure"
  private val HandshakeBudget = 60.seconds
  private val DispatchBudget = 60.seconds
  private val SilenceWindow = 3.seconds
  private val ShutdownBudget = 30.seconds
}
