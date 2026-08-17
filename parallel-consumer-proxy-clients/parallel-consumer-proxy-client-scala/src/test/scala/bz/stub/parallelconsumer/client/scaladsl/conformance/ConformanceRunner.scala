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
 * Its contract - the six flags, the three exit statuses, the two observation lines, the behaviour
 * tokens and the fixed literals - is documented once, in that module's `README.md`, and is identical
 * in every language.
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
  private val HoldUntilCeilingFull = "hold-until-ceiling-full"
  private val Behaviours =
    Set(Succeed, ReportNothing, FailThenSucceed, HoldFirstUntilSecond, HoldUntilCeilingFull)

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

  /**
   * How long `hold-until-ceiling-full` keeps a FULL group held before releasing it.
   *
   * '''IT IS WHAT TURNS "THE CEILING WAS NEVER EXCEEDED" FROM A RACE INTO A MEASUREMENT.''' Release
   * the group the instant it fills and a client that declared a larger ceiling still passes - its
   * extra records arrive a few milliseconds later, by which time the outstanding count has already
   * fallen back. Holding the full ceiling still means the extra dispatch arrives INSIDE the window
   * and prints its line while every other record is unresolved. A correct engine cannot dispatch
   * anything during the window at all, so the wait costs a conforming client nothing but time.
   */
  private val CeilingSettle = 250.millis

  private val HandshakeBudget = 60.seconds
  private val ShutdownBudget = 30.seconds

  /**
   * Fires every delay this runner needs - the second-arrival budget, the ceiling group's budget and
   * the ceiling settle window - without blocking a thread to wait for any of them.
   */
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

  /** The six flags, spelled identically in every language - including the British `--behaviour`. */
  private final case class Request(
      scenario: String,
      behaviour: String,
      sidecar: String,
      expect: Int,
      maxConcurrency: Int,
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
        maxConcurrency <- positive(flags, "--max-concurrency")
        budget <- positive(flags, "--timeout-seconds")
        _ <- Either.cond(Behaviours.contains(behaviour), (), s"unknown behaviour '$behaviour'")
        _ <- Either.cond(
          Paths.get(sidecar).isAbsolute,
          (),
          s"--sidecar must be absolute, got '$sidecar'")
      } yield Request(scenario, behaviour, sidecar, expect, maxConcurrency, budget.seconds)
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
        "--expect-dispatches <n> --max-concurrency <n> --timeout-seconds <n>")
    Console.err.println(
      s"  --behaviour is one of: ${Behaviours.toSeq.sorted.mkString(", ")}")
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
    // THE CEILING IS THE SCENARIO'S TO CHOOSE, AND THIS RUNNER NEVER DERIVES ONE. `--max-concurrency`
    // is the only thing it may be set from: a runner that derived it from `--expect-dispatches`, as
    // this one used to, declares a ceiling no scenario can ever reach, so no scenario could ask this
    // client to prove it respected one.
    maxConcurrency = Some(request.maxConcurrency),
    commitInterval = Some(CommitInterval),
    defaultMessageRetryDelay = Some(RetryDelay))

  private def processorFor(request: Request, tracker: Tracker): RecordProcessor = {
    val ceiling = new CeilingGroup(request.maxConcurrency, request.expect, () => tracker.observed)

    record =>
      val ordinal = tracker.observe(record)
      request.behaviour match {
        case Succeed =>
          Future.successful(tracker.settleSuccess(record))

        case ReportNothing =>
          // Never report, and print NO `settled` line: by prescription this record is never resolved
          // and the absence of the line is the observation. A future that never completes is this
          // client's ONLY way to say "no verdict for this record".
          ParallelConsumerClient.noVerdict

        case FailThenSucceed =>
          if (record.attempt == 1) Future.successful(tracker.settleFailure(record, PrescribedFailureReason))
          else Future.successful(tracker.settleSuccess(record))

        case HoldFirstUntilSecond =>
          if (ordinal == 1) {
            // Hold the first record until a SECOND is dispatched. Whether one arrives at all, and
            // which key it carries, is the whole of what the scenario is asking - and it is the Java
            // suite that decides what the answer means. Holding is a future nobody has completed, not
            // a blocked thread: blocking a transport executor here is the very defect the scenario is
            // an instrument for.
            tracker.secondArrivedWithin(request.budget).map { arrived =>
              if (arrived) tracker.settleSuccess(record)
              else
                // deliberately NOT counted as completed: the prescription was not carried out, so the
                // outer wait must time out and this process must exit 1 rather than report a tidy
                // failure and call the scenario done
                tracker.settleGaveUp(
                  record,
                  "conformance: no second dispatch arrived while the first was held")
            }
          } else {
            Future.successful(tracker.settleSuccess(record))
          }

        case HoldUntilCeilingFull =>
          // Hold EVERY record until `--max-concurrency` of them are held at once, keep the full group
          // held for the settle window, then report the whole group as successes. Holding is again a
          // future nobody has completed: the barrier below never parks a thread, so a ceiling wider
          // than this execution context could not starve itself of one.
          ceiling.enter(request.budget).map { filled =>
            if (filled) tracker.settleSuccess(record)
            else
              // same shape as the hold-first-until-second give-up: not counted as completed, so the
              // outer wait times out and this process exits 1
              tracker.settleGaveUp(
                record,
                s"conformance: the ceiling group of ${request.maxConcurrency} never filled")
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
   * Counts deliveries and outcomes, and prints the two observation lines. It holds no per-record
   * state - only counts - because the client library holds none either, and this runner must not
   * become the place where a client's missing bookkeeping is quietly supplied.
   *
   * '''THE SUITE READS OVERLAP FROM THE ORDER OF THE LINES, SO EVERY WRITE GOES UNDER ONE LOCK.''' A
   * `dispatch` line opens a record's unresolved window and its `settled` line closes it, and the
   * running difference between the two counts, read in line order, is how many records this client
   * was holding at that instant. Several executor threads print here, so the lock that hands out the
   * ordinal is the same lock that writes both line types - no clock is involved anywhere.
   */
  private final class Tracker(expected: Int) {

    private val observedAll = Promise[Unit]()
    private val completedAll = Promise[Unit]()
    private val secondDelivery = Promise[Unit]()

    /**
     * Volatile so the ceiling barrier can read it without reaching into this lock from inside its
     * own - the same reason the Java reference holds this count in an `AtomicInteger`. Writes still
     * happen under the lock, beside the line they belong to.
     */
    @volatile private var observedCount = 0
    private var completedCount = 0

    def observed: Int = observedCount

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
        val reason = record.previousFailure.flatMap(_.reason).getOrElse("")
        emit("dispatch ", record, reason)
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

    /**
     * The record's outcome is decided and it stops being unresolved: prints the `settled` line, then
     * counts the record as complete, then hands back the outcome to report.
     *
     * The line goes out BEFORE the count, because the count is what releases the run's outer wait -
     * print after it and the process can exit over the top of its own last observation.
     */
    def settleSuccess(record: InboundRecord): Outcome = {
      synchronized(emit("settled ", record, ""))
      complete()
      Outcome.succeeded
    }

    /** As [[settleSuccess]], for a failure this runner is PRESCRIBED to report. */
    def settleFailure(record: InboundRecord, reason: String): Outcome = {
      synchronized(emit("settled ", record, reason))
      complete()
      Outcome.failed(reason)
    }

    /**
     * As [[settleFailure]], but for a behaviour that could not be carried out - and deliberately NOT
     * counted as complete, so the run's outer wait times out and the process exits 1 rather than
     * reporting a tidy failure and calling the scenario done.
     */
    def settleGaveUp(record: InboundRecord, reason: String): Outcome = {
      synchronized(emit("settled ", record, reason))
      Outcome.failed(reason)
    }

    /** Caller holds this Tracker's lock: the line order IS the event order. */
    private def emit(prefix: String, record: InboundRecord, reason: String): Unit = {
      val key = record.key.map(new String(_, StandardCharsets.UTF_8)).getOrElse("")
      println(
        s"${prefix}key=$key offset=${record.offset} attempt=${record.attempt} reason=$reason")
      Console.out.flush()
    }

    private def complete(): Unit = {
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

  /**
   * The cyclic barrier at the heart of `hold-until-ceiling-full`: a record entering it is held until
   * `maxConcurrency` of them are held at once, the full group is then kept held for
   * [[CeilingSettle]], and the whole generation is released together.
   *
   * '''NOTHING HERE PARKS A THREAD, WHICH IS THE ONLY WAY SCALA CAN WRITE IT.''' The Java reference
   * spells the same barrier with `wait`/`notifyAll` and a `Thread.sleep`, because a JVM record is
   * held by a user function that has not returned. Here a record is held by a `Future` nobody has
   * completed, so the barrier is a promise per generation and the two delays are scheduled rather
   * than slept: a group of width `n` holding `n` threads of the execution context would deadlock the
   * very concurrency the scenario is measuring - the client-side shape of the defect this whole
   * scenario exists to catch.
   *
   * A group also releases once every prescribed delivery has been observed, so a scenario whose
   * record count is not a multiple of its ceiling cannot strand its last, short group.
   */
  private final class CeilingGroup(maxConcurrency: Int, expected: Int, observed: () => Int) {

    private var held = 0
    private var generation = 0L

    /** Completed when `generation` advances, which is how a waiter learns its group was released. */
    private var gate = Promise[Unit]()

    /**
     * Enters the group for one record.
     *
     * @return a future completing true once the group has filled and settled, and false if it never
     *         filled inside the budget - which is this runner failing to carry out the prescription,
     *         not the client being wrong about anything
     */
    def enter(budget: FiniteDuration): Future[Boolean] = {
      val waiting = synchronized {
        held += 1
        val releasing = held >= maxConcurrency || observed() >= expected
        if (releasing) None else Some((generation, held, gate.future))
      }
      waiting match {
        case Some((myGeneration, heldSoFar, opened)) => waitForRelease(myGeneration, heldSoFar, opened, budget)
        case None => releaseAfterSettle()
      }
    }

    /** Bounded by the remaining budget: a group that never fills fails the run rather than hanging. */
    private def waitForRelease(
        myGeneration: Long,
        heldSoFar: Int,
        opened: Future[Unit],
        budget: FiniteDuration): Future[Boolean] = {
      val answer = Promise[Boolean]()
      opened.foreach(_ => answer.trySuccess(true))
      val _ = timers.schedule(
        new Runnable {
          override def run(): Unit =
            if (answer.trySuccess(false)) {
              Console.err.println(
                s"conformance-runner: the ceiling group of $maxConcurrency never filled in " +
                  s"generation $myGeneration: $heldSoFar held")
            }
        },
        budget.toMillis,
        TimeUnit.MILLISECONDS)
      answer.future
    }

    /**
     * The releaser's path. '''THE SETTLE WINDOW IS OUTSIDE THE LOCK''' - a record the engine should
     * not be dispatching still has to be able to print its arrival line during it, and that arrival
     * is the whole thing the scenario looks for. A correct engine cannot dispatch anything here,
     * because the ceiling is full, so an extra line inside the window IS the excess.
     */
    private def releaseAfterSettle(): Future[Boolean] = {
      val released = Promise[Boolean]()
      val _ = timers.schedule(
        new Runnable {
          override def run(): Unit = {
            // CeilingGroup.this, not the Runnable's own monitor: an anonymous class inside a method
            // makes a bare `synchronized` lock the wrong object, silently and with no warning
            val opening = CeilingGroup.this.synchronized {
              held = 0
              generation += 1
              val current = gate
              gate = Promise[Unit]()
              current
            }
            // waking every waiter happens outside the lock, so a woken record's own settle line is
            // never printed behind this barrier's monitor
            val _ = opening.trySuccess(())
            val _ = released.trySuccess(true)
          }
        },
        CeilingSettle.toMillis,
        TimeUnit.MILLISECONDS)
      released.future
    }
  }
}
