// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines.conformance

import bz.stub.parallelconsumer.client.coroutines.ClientOptions
import bz.stub.parallelconsumer.client.coroutines.InboundRecord
import bz.stub.parallelconsumer.client.coroutines.Outcome
import bz.stub.parallelconsumer.client.coroutines.ParallelConsumerClient
import bz.stub.parallelconsumer.client.coroutines.RecordProcessor
import bz.stub.parallelconsumer.client.coroutines.SidecarCommand
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.nio.file.Path
import kotlin.system.exitProcess
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Kotlin's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
 *
 * IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset frontiers,
 * ordering, redelivery, attempt counts - is the Java module
 * `parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance`, and it keeps owning that
 * knowledge for every language. This program's whole job is to DO WHAT THE SCENARIO SAYS and then
 * exit; if it were free to decide what "correct" means, eleven languages would each decide it
 * slightly differently and the agreement between them would prove nothing.
 *
 * Its contract - the five flags, the three exit statuses, the stdout observation line, the four
 * behaviour tokens, the fixed literals - is documented once, in that module's `README.md`, and is
 * identical in every language.
 *
 * THIS DOES NOT REPLACE THIS MODULE'S OWN TESTS. The shared suite proves every client behaves
 * identically on the protocol; `OneRecordThroughTheSidecarTest` and its siblings catch what is
 * invisible from outside the process - a `CancellationException` becoming a fabricated verdict, a
 * coroutine nobody owns. Both layers are load-bearing.
 *
 * IT LIVES IN THE TEST TREE, and that is where a runner belongs for a library whose published
 * surface is checked by `-Xexplicit-api=strict`: it is a program that uses the client, not part of
 * the client. The registry's executable is `scripts/conformance-runner`, which launches this class
 * with the classpath Maven resolved - a JVM client's "binary" is a JVM plus a classpath, and that
 * awkwardness is confined to that one wrapper.
 */

/** Exit statuses ARE the verdict channel: there is no results file and no report message. */
private const val EXIT_OK = 0
private const val EXIT_BEHAVIOUR_FAILED = 1
private const val EXIT_USAGE = 2

/**
 * The exact text a `fail-then-succeed` run reports. The Java suite asserts the redelivery carries it
 * back VERBATIM, so it is a fixed literal of the contract in every language, never composed here.
 */
private const val PRESCRIBED_FAILURE_REASON = "conformance-prescribed-failure"

/**
 * Fixed session tunables, contract rather than this runner's judgement: they exist only so scenarios
 * converge at unit-test speed against the engine's production defaults (a 5s commit interval, a 1s
 * retry delay). A runner free to pick its own would make the suite's budgets mean something
 * different in each language.
 */
private val COMMIT_INTERVAL = 100.milliseconds
private val RETRY_DELAY = 50.milliseconds

/**
 * How long a `report-nothing` run keeps its session OPEN after its last observation.
 *
 * IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner exits the instant the
 * record arrives, and a sabotaged runner that DID report success has its report killed in flight by
 * the process exit - so the suite sees an unadvanced offset either way and the scenario passes for a
 * broken client. Measured in the Go wave, not reasoned about.
 */
private val REPORT_NOTHING_HOLD = 3.seconds

/** How long the shutdown path waits for `poll` to return before giving up on a clean close. */
private val SHUTDOWN_BUDGET = 30.seconds

private val BEHAVIOURS = setOf("succeed", "report-nothing", "fail-then-succeed", "hold-first-until-second")

private val REQUIRED_FLAGS =
    listOf("--scenario", "--behaviour", "--sidecar", "--expect-dispatches", "--timeout-seconds")

/** What the suite asked for. The runner receives the prescription and nothing else about the scenario. */
private class Arguments(
    val scenario: String,
    val behaviour: String,
    val sidecar: String,
    val expect: Int,
    val budget: Duration,
)

public fun main(args: Array<String>) {
    val arguments = parse(args) ?: exitProcess(EXIT_USAGE)
    exitProcess(runBlocking { runScenario(arguments) })
}

/** The five flags, spelled identically in every language - including the British `--behaviour`. */
private fun parse(args: Array<String>): Arguments? {
    val values = collect(args) ?: return null
    val problem = complaintAbout(values)
    return if (problem != null) {
        usage(problem)
    } else {
        Arguments(
            scenario = values.getValue("--scenario"),
            behaviour = values.getValue("--behaviour"),
            sidecar = values.getValue("--sidecar"),
            expect = values.getValue("--expect-dispatches").toInt(),
            budget = values.getValue("--timeout-seconds").toInt().seconds,
        )
    }
}

/** The argument list as flag-to-value pairs, or null having already said what was wrong with it. */
private fun collect(args: Array<String>): Map<String, String>? {
    val values = mutableMapOf<String, String>()
    var index = 0
    while (index < args.size) {
        val flag = args[index]
        if (!flag.startsWith("--") || index + 1 >= args.size) {
            return usage("expected --flag value pairs, got '$flag'")
        }
        values[flag] = args[index + 1]
        index += 2
    }
    return values
}

/** The FIRST thing wrong with what was asked for, or null. One expression, so the order is the message. */
private fun complaintAbout(values: Map<String, String>): String? =
    REQUIRED_FLAGS.firstOrNull { values[it].isNullOrEmpty() }?.let { "$it is required" }
        ?: values.getValue("--behaviour").takeIf { it !in BEHAVIOURS }?.let { "unknown behaviour '$it'" }
        ?: values.getValue("--sidecar").takeIf { !Path.of(it).isAbsolute }
            ?.let { "--sidecar must be absolute, got '$it'" }
        ?: atLeastOne(values, "--expect-dispatches")
        ?: atLeastOne(values, "--timeout-seconds")

private fun atLeastOne(values: Map<String, String>, flag: String): String? =
    if (values.getValue(flag).toIntOrNull()?.takeIf { it >= 1 } == null) {
        "$flag must be a whole number of at least 1, got '${values.getValue(flag)}'"
    } else {
        null
    }

/**
 * Says what was wrong on stderr and answers null - typed `Nothing?` so one function serves every
 * caller's own nullable return type.
 */
private fun usage(problem: String): Nothing? {
    System.err.println("conformance-runner: $problem")
    return null
}

/**
 * Counts deliveries and outcomes, and prints the observation line. It holds no per-record state -
 * only counts - because the client library holds none either, and this runner must not become the
 * place where a client's missing bookkeeping is quietly supplied.
 */
private class Tracker(private val expected: Int) {

    private val printing = Any()

    @Volatile
    var observed: Int = 0
        private set

    @Volatile
    var completed: Int = 0
        private set

    val allObserved: CompletableDeferred<Unit> = CompletableDeferred()
    val allCompleted: CompletableDeferred<Unit> = CompletableDeferred()
    val secondArrived: CompletableDeferred<Unit> = CompletableDeferred()

    /** Prints the delivery and returns its 1-based ordinal in arrival order. */
    fun observe(record: InboundRecord): Int {
        val ordinal: Int
        // Printed at the moment of delivery, before the behaviour acts on it, and under the same
        // lock as the ordinal so the transcript's ORDER is the arrival order: records run as
        // coroutines here and two of them share one stdout.
        synchronized(printing) {
            observed += 1
            ordinal = observed
            val key = record.key?.toString(Charsets.UTF_8) ?: ""
            println(
                "dispatch key=$key offset=${record.offset} attempt=${record.attempt} " +
                    "reason=${record.previousFailure?.reason ?: ""}"
            )
            System.out.flush()
        }
        if (ordinal >= 2) {
            secondArrived.complete(Unit)
        }
        if (ordinal >= expected) {
            allObserved.complete(Unit)
        }
        return ordinal
    }

    fun complete() {
        val reached: Int
        synchronized(printing) {
            completed += 1
            reached = completed
        }
        if (reached >= expected) {
            allCompleted.complete(Unit)
        }
    }
}

/** The prescribed behaviour, as an ordinary suspending user function - which is all a client sees. */
private fun processorFor(behaviour: String, tracker: Tracker, budget: Duration): RecordProcessor = { record ->
    val ordinal = tracker.observe(record)
    when (behaviour) {
        "succeed" -> {
            tracker.complete()
            Outcome.Success()
        }

        // Never report. Suspending forever is how a coroutine client says "this record's function
        // has not returned"; the process exits with the record still in flight.
        "report-nothing" -> awaitCancellation()

        "fail-then-succeed" -> {
            tracker.complete()
            if (record.attempt == 1) Outcome.Failure(PRESCRIBED_FAILURE_REASON) else Outcome.Success()
        }

        // Hold the first record until a SECOND is dispatched. Whether one arrives at all, and which
        // key it carries, is the whole of what the scenario is asking - and it is the Java suite
        // that decides what the answer means.
        "hold-first-until-second" -> {
            if (ordinal == 1 && withTimeoutOrNull(budget) { tracker.secondArrived.await() } == null) {
                error("conformance: no second dispatch arrived while the first was held")
            }
            tracker.complete()
            Outcome.Success()
        }

        // unreachable: parse rejects an unknown behaviour before the session opens
        else -> error("conformance: unknown behaviour '$behaviour'")
    }
}

private suspend fun runScenario(arguments: Arguments): Int = coroutineScope {
    val tracker = Tracker(arguments.expect)
    val client = try {
        ParallelConsumerClient.open(
            options = ClientOptions(
                // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
                topics = listOf(arguments.scenario),
                // Enough in flight for every dispatch the scenario prescribes, so a scenario that
                // holds a record cannot deadlock on a ceiling smaller than its own shape.
                maxConcurrency = arguments.expect,
                commitInterval = COMMIT_INTERVAL,
                defaultMessageRetryDelay = RETRY_DELAY,
                // The mock lane builds mock Kafka clients and reads no properties. Real credentials
                // never belong in a conformance test.
                kafkaProperties = emptyMap(),
            ),
            sidecar = SidecarCommand(Path.of(arguments.sidecar)),
        )
    } catch (failure: Exception) {
        System.err.println("conformance-runner: opening the session: $failure")
        return@coroutineScope EXIT_BEHAVIOUR_FAILED
    }

    // poll suspends for the life of the session, which is this client's idiom - so it runs as its
    // own coroutine and the prescription is awaited beside it.
    val poller = launch(Dispatchers.IO) {
        client.poll(processorFor(arguments.behaviour, tracker, arguments.budget))
    }

    // report-nothing completes at OBSERVATION, because by prescription its record is never reported
    // and so can never complete. Every other behaviour completes when the last delivery it was
    // handed has had its outcome decided.
    val reportNothing = arguments.behaviour == "report-nothing"
    val finished = withTimeoutOrNull(arguments.budget) {
        if (reportNothing) tracker.allObserved.await() else tracker.allCompleted.await()
    }
    if (finished == null) {
        System.err.println(
            "conformance-runner: scenario '${arguments.scenario}' behaviour '${arguments.behaviour}' did " +
                "not complete within ${arguments.budget} - observed ${tracker.observed} of " +
                "${arguments.expect}, completed ${tracker.completed}"
        )
        closeQuietly(client)
        poller.cancel()
        return@coroutineScope EXIT_BEHAVIOUR_FAILED
    }

    if (reportNothing) {
        // Hold the session open, reporting nothing, so the suite is watching a LIVE client rather
        // than the wreckage of one - see REPORT_NOTHING_HOLD.
        delay(REPORT_NOTHING_HOLD)
        System.out.flush()
        // PRESCRIBED: the record is never reported and the session is abandoned rather than drained
        // - a worker that vanished mid-record is exactly what this scenario models. Exiting closes
        // the sidecar's lifecycle pipe, which reaps it, so nothing is leaked by not closing; the
        // held record's coroutine goes with the JVM.
        exitProcess(EXIT_OK)
    }

    closeQuietly(client)
    if (withTimeoutOrNull(SHUTDOWN_BUDGET) { poller.join() } == null) {
        System.err.println("conformance-runner: the session did not end within $SHUTDOWN_BUDGET")
        poller.cancel()
        return@coroutineScope EXIT_BEHAVIOUR_FAILED
    }
    EXIT_OK
}

/** Shuts down without letting a close error rewrite a verdict the prescription already reached. */
@Suppress("TooGenericExceptionCaught") // a broken close must not become a different failure than the real one
private fun closeQuietly(client: ParallelConsumerClient) {
    try {
        client.close()
    } catch (failure: Exception) {
        System.err.println("conformance-runner: while shutting down: $failure")
    }
}
