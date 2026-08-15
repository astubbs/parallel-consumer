// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl.conformance

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

import bz.stub.parallelconsumer.client.scaladsl._

/**
 * Scala's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
 *
 * '''IT ASSERTS NOTHING, DELIBERATELY.''' The suite that knows what correct looks like - offset
 * frontiers, ordering, redelivery, attempt counts - is the Java module
 * `parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance`, and it keeps owning that
 * knowledge for every language. This program's whole job is to DO WHAT THE SCENARIO SAYS and then
 * exit; if it were free to decide what "correct" means, eleven languages would each decide it
 * slightly differently and the agreement between them would prove nothing.
 *
 * Its contract - the five flags, the three exit statuses, the observation line, the behaviour tokens
 * and the fixed literals - is documented once, in that module's `README.md`, and is identical in
 * every language.
 *
 * '''THIS DOES NOT REPLACE THE MODULE'S OWN TESTS.''' The shared suite proves every client behaves
 * identically on the protocol; `src/test` catches what is invisible from outside the process - a
 * failed future silently becoming a verdict, an options renderer printing a credential. Both layers
 * are load-bearing.
 *
 * It lives in the test tree rather than in `src/main` because it is test tooling: the published
 * surface of this module is the client and nothing else.
 */
object ConformanceRunner {

  /**
   * Exit statuses ARE the verdict channel. There is no results file, no report message and no second
   * protocol: a scenario passed if this process exited 0 and the Java suite's own assertions about
   * engine state held.
   */
  private val ExitOk = 0
  private val ExitBehaviourFailed = 1
  private val ExitUsage = 2

  private val Succeed = "succeed"
  private val ReportNothing = "report-nothing"
  private val FailThenSucceed = "fail-then-succeed"
  private val HoldFirstUntilSecond = "hold-first-until-second"
  private val Behaviours = Set(Succeed, ReportNothing, FailThenSucceed, HoldFirstUntilSecond)

  /**
   * The exact text a `fail-then-succeed` run reports. The Java suite asserts the redelivery carries
   * it back VERBATIM, so it is a fixed literal of the contract in every language, never a message
   * this runner composes.
   */
  private val PrescribedFailureReason = "conformance-prescribed-failure"

  /**
   * Fixed session tunables, contract rather than this runner's judgement: they exist only so
   * scenarios converge at unit-test speed against the engine's production defaults (a 5s commit
   * interval, a 1s retry delay). Every language sets the same two values.
   */
  private val CommitInterval = 100.millis
  private val RetryDelay = 50.millis

  /**
   * How long a `report-nothing` run keeps its session OPEN after its last observation.
   *
   * '''IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL.''' Without it the runner exits the instant
   * the record arrives, and a sabotaged runner that DID report success has its report killed in
   * flight by the process exit - so the suite sees an unadvanced offset either way and the scenario
   * passes for a broken client. Measured in the Go wave, not reasoned about.
   */
  private val ReportNothingHold = 3.seconds

  private val HandshakeBudget = 60.seconds
  private val ShutdownBudget = 30.seconds

  /** Fires the two timeouts this runner needs without blocking a thread to wait for either. */
  private val timers = Executors.newSingleThreadScheduledExecutor { runnable =>
    val thread = new Thread(runnable, "conformance-timers")
    thread.setDaemon(true)
    thread
  }

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def main(arguments: Array[String]): Unit = System.exit(run(arguments.toIndexedSeq))

  private def run(arguments: Seq[String]): Int = parse(arguments) match {
    case Left(problem) => usage(problem)
    case Right(request) => drive(request)
  }

  /** The five flags, spelled identically in every language - including the British `--behaviour`. */
  private final case class Request(
      scenario: String,
      behaviour: String,
      sidecar: String,
      expect: Int,
      budget: FiniteDuration)

  private def parse(arguments: Seq[String]): Either[String, Request] = {
    val pairs = arguments.grouped(2).toSeq
    if (pairs.exists(pair => pair.size != 2 || !pair.head.startsWith("--"))) {
      Left(s"expected --flag value pairs, got: ${arguments.mkString(" ")}")
    } else {
      val flags = pairs.map(pair => pair.head -> pair(1)).toMap
      for {
        scenario <- required(flags, "--scenario")
        behaviour <- required(flags, "--behaviour")
        sidecar <- required(flags, "--sidecar")
        expect <- positive(flags, "--expect-dispatches")
        budget <- positive(flags, "--timeout-seconds")
        _ <- Either.cond(Behaviours.contains(behaviour), (), s"unknown behaviour '$behaviour'")
        _ <- Either.cond(
          Paths.get(sidecar).isAbsolute,
          (),
          s"--sidecar must be absolute, got '$sidecar'")
      } yield Request(scenario, behaviour, sidecar, expect, budget.seconds)
    }
  }

  private def required(flags: Map[String, String], flag: String): Either[String, String] =
    flags.get(flag).filter(_.nonEmpty).toRight(s"$flag is required")

  private def positive(flags: Map[String, String], flag: String): Either[String, Int] =
    required(flags, flag)
      .flatMap(value => value.toIntOption.toRight(s"$flag must be a number, got '$value'"))
      .filterOrElse(_ >= 1, s"$flag must be at least 1")

  private def usage(problem: String): Int = {
    Console.err.println(s"conformance-runner: $problem")
    Console.err.println(
      "usage: conformance-runner --scenario <name> --behaviour <token> --sidecar <abs-path> " +
        "--expect-dispatches <n> --timeout-seconds <n>")
    ExitUsage
  }

  private def drive(request: Request): Int = {
    val tracker = new Tracker(request.expect)
    val opened =
      try {
        Await.result(
          ParallelConsumerClient.open(optionsFor(request), SidecarCommand(Paths.get(request.sidecar))),
          HandshakeBudget)
      } catch {
        case NonFatal(failure) =>
          Console.err.println(
            s"conformance-runner: opening the session for scenario '${request.scenario}': $failure")
          return ExitBehaviourFailed
      }

    val session = opened.poll(processorFor(request, tracker))

    // report-nothing completes at OBSERVATION, because by prescription its records are never
    // reported and so can never complete. Every other behaviour completes when the last record it
    // was handed has had its outcome decided.
    val prescribed =
      if (request.behaviour == ReportNothing) tracker.allObserved else tracker.allCompleted
    try Await.result(prescribed, request.budget)
    catch {
      case NonFatal(_) =>
        Console.err.println(
          s"conformance-runner: scenario '${request.scenario}' behaviour '${request.behaviour}' did " +
            s"not complete within ${request.budget} - observed ${tracker.observed} of " +
            s"${request.expect}, completed ${tracker.completed}")
        closeQuietly(opened)
        return ExitBehaviourFailed
    }

    if (request.behaviour == ReportNothing) {
      // Hold the session open, reporting nothing, so the suite is watching a LIVE client rather than
      // the wreckage of one - see ReportNothingHold.
      Thread.sleep(ReportNothingHold.toMillis)
      // PRESCRIBED: the record is never reported and the session is abandoned rather than drained -
      // a worker that vanished mid-record is exactly what this scenario models. Exiting closes the
      // sidecar's lifecycle pipe, which reaps it, so nothing is leaked by not closing.
      return ExitOk
    }

    try {
      opened.close()
      Await.result(session, ShutdownBudget)
      ExitOk
    } catch {
      case NonFatal(failure) =>
        Console.err.println(s"conformance-runner: closing the session: $failure")
        ExitBehaviourFailed
    }
  }

  private def optionsFor(request: Request) = ClientOptions(
    // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
    topics = Seq(request.scenario),
    // The mock lane builds mock Kafka clients and reads no properties. Real credentials never belong
    // in a conformance test.
    kafkaProperties = Map.empty,
    // Enough in-flight room for every dispatch the scenario prescribes, so a scenario that holds a
    // record cannot deadlock on a ceiling smaller than its own shape.
    maxConcurrency = Some(request.expect),
    commitInterval = Some(CommitInterval),
    defaultMessageRetryDelay = Some(RetryDelay))

  private def processorFor(request: Request, tracker: Tracker): RecordProcessor = { record =>
    val ordinal = tracker.observe(record)
    request.behaviour match {
      case Succeed =>
        tracker.complete()
        Future.successful(Outcome.succeeded)

      case ReportNothing =>
        // Never report. A future that never completes is this client's ONLY way to say "no verdict
        // for this record", and it is exactly what this behaviour prescribes.
        ParallelConsumerClient.noVerdict

      case FailThenSucceed =>
        tracker.complete()
        if (record.attempt == 1) Future.successful(Outcome.failed(PrescribedFailureReason))
        else Future.successful(Outcome.succeeded)

      case HoldFirstUntilSecond =>
        if (ordinal == 1) {
          // Hold the first record until a SECOND is dispatched. Whether one arrives at all, and
          // which key it carries, is the whole of what the scenario is asking - and it is the Java
          // suite that decides what the answer means. Holding is a future nobody has completed, not
          // a blocked thread: blocking a transport executor here is the very defect the scenario is
          // an instrument for.
          tracker.secondArrivedWithin(request.budget).map { arrived =>
            if (arrived) {
              tracker.complete()
              Outcome.succeeded
            } else {
              // deliberately NOT counted as completed: the prescription was not carried out, so the
              // outer wait must time out and this process must exit 1 rather than report a tidy
              // failure and call the scenario done
              Outcome.failed("conformance: no second dispatch arrived while the first was held")
            }
          }
        } else {
          tracker.complete()
          Future.successful(Outcome.succeeded)
        }

      case other =>
        // unreachable: parse rejects an unknown behaviour before the session opens
        Future.failed(new IllegalStateException(s"conformance: unknown behaviour '$other'"))
    }
  }

  private def closeQuietly(client: ParallelConsumerClient): Unit =
    try client.close()
    catch {
      case NonFatal(failure) => Console.err.println(s"conformance-runner: while shutting down: $failure")
    }

  /**
   * Counts deliveries and outcomes, and prints the observation line. It holds no per-record state -
   * only counts - because the client library holds none either, and this runner must not become the
   * place where a client's missing bookkeeping is quietly supplied.
   */
  private final class Tracker(expected: Int) {

    private val observedAll = Promise[Unit]()
    private val completedAll = Promise[Unit]()
    private val secondDelivery = Promise[Unit]()
    private var observedCount = 0
    private var completedCount = 0

    def observed: Int = synchronized(observedCount)

    def completed: Int = synchronized(completedCount)

    def allObserved: Future[Unit] = observedAll.future

    def allCompleted: Future[Unit] = completedAll.future

    /** Prints the delivery and returns its 1-based ordinal in arrival order. */
    def observe(record: InboundRecord): Int = {
      val ordinal = synchronized {
        observedCount += 1
        // printed at the moment of delivery, before the behaviour acts on it, and under the same
        // lock as the ordinal so the transcript's ORDER is the arrival order: two executors share
        // one stdout
        val key = record.key.map(new String(_, StandardCharsets.UTF_8)).getOrElse("")
        val reason = record.previousFailure.flatMap(_.reason).getOrElse("")
        println(s"dispatch key=$key offset=${record.offset} attempt=${record.attempt} reason=$reason")
        Console.out.flush()
        observedCount
      }
      if (ordinal >= 2) {
        val _ = secondDelivery.trySuccess(())
      }
      if (ordinal >= expected) {
        val _ = observedAll.trySuccess(())
      }
      ordinal
    }

    def complete(): Unit = {
      val reached = synchronized {
        completedCount += 1
        completedCount >= expected
      }
      if (reached) {
        val _ = completedAll.trySuccess(())
      }
    }

    /** Completes true when a second delivery has been observed, false when the budget beat it. */
    def secondArrivedWithin(budget: FiniteDuration): Future[Boolean] = {
      val answer = Promise[Boolean]()
      secondDelivery.future.foreach(_ => answer.trySuccess(true))
      val _ = timers.schedule(
        new Runnable { override def run(): Unit = { val _ = answer.trySuccess(false) } },
        budget.toMillis,
        TimeUnit.MILLISECONDS)
      answer.future
    }
  }
}
