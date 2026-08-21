// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines.demo

import bz.stub.parallelconsumer.client.coroutines.ClientOptions
import bz.stub.parallelconsumer.client.coroutines.Outcome
import bz.stub.parallelconsumer.client.coroutines.ParallelConsumerClient
import bz.stub.parallelconsumer.client.coroutines.ProcessingOrder
import bz.stub.parallelconsumer.client.coroutines.SidecarCommand
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.nio.file.Path
import java.time.Duration
import java.util.Locale
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** No arm may take longer than this before the demo calls it stalled rather than slow. */
private val ARM_BUDGET = 10.minutes

private const val AK_CORE = "AK core"

private const val KOTLIN_SIDECAR = "kotlin-sidecar"

/** The real sidecar's entry point, launched as an ordinary child process by the client library. */
private const val SIDECAR_MAIN = "bz.stub.parallelconsumer.proxy.Main"

/** What one arm achieved: how long it took, and over how many records. */
internal class ArmResult(val arm: String, val elapsed: Duration, val processed: Int) {
    /** Throughput, which is the only figure this demo reports. */
    val ratePerSecond: Double
        get() = elapsed.toNanos().let { if (it > 0) processed * NANOS_PER_SECOND / it else 0.0 }

    private companion object {
        const val NANOS_PER_SECOND = 1_000_000_000.0
    }
}

/**
 * The demo's own output, on stdout and unconditional.
 *
 * Deliberately not the logger: the tables and the fingerprint are the demo's product, and a reader
 * who changed a log level, or whose container inherited one, must still see them. Everything the
 * libraries have to say goes through slf4j and is turned down by `logback.xml` beside this source.
 */
internal object Console {
    fun say(line: String) {
        println(line)
    }
}

public fun main(args: Array<String>) {
    if (DemoOptions.isHelpRequested(args)) {
        usage()
        return
    }
    val options = try {
        DemoOptions.parse(args, System.getenv())
    } catch (e: IllegalArgumentException) {
        Console.say(e.message ?: "bad arguments")
        usage()
        // a misspelled flag must not be reported as a result for settings nobody asked for
        exitProcess(2)
    }

    DemoBroker.resolve(options.bootstrap).use { broker ->
        val topic = options.topic ?: "pc-demo-${System.nanoTime()}"
        runBlocking { KotlinDemo(options, broker, topic).run() }
    }
}

private fun usage() {
    Console.say(
        """
        usage: demo/run.sh [options]
          --records N        records in the comparison replay   (default 2000)
          --delay-ms N       simulated work per record, ms      (default 2)
          --concurrency N    max in-flight records              (default 100)
          --partitions N     partitions on the demo topic       (default 10)
          --replay-factor N  big replay = records x N; 1 skips  (default 20)
          --bootstrap ADDR   an existing broker; omit to start one
          --topic NAME       an existing topic; omit to create one

        Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
        Flags beat the environment beats the defaults.
        """.trimIndent()
    )
}

/**
 * **The Kotlin demo.** The same records through Kotlin's own Kafka client and through Kotlin over
 * the sidecar, one command, no setup.
 *
 * The contract it keeps - the flags, the environment variables, the defaults, the fingerprint, the
 * two tables, no latency - is `parallel-consumer-proxy/demo/README.md`, and it is the same contract
 * in every language. What is specific to Kotlin is in `demo/README.md` beside this file.
 *
 * ## Two arms, and only two
 *
 * - **AK core** - a plain `KafkaConsumer` driven from Kotlin, one record at a time. Always spelled
 *   "AK core", never bare "core", which reads as `parallel-consumer-core` (`CONCEPTS.md`).
 * - **kotlin-sidecar** - this module's own `ParallelConsumerClient`: it spawns the sidecar as a
 *   child process, receives records over a socket, runs a suspending function on them and reports
 *   outcomes back. **On this path the application does no Kafka I/O** - the sidecar owns the
 *   consumer, the producer, the group membership and the offsets. That is a claim about the *path*,
 *   not about this process: the same JVM creates the topic, seeds the backlog and runs the AK core
 *   arm with ordinary Kafka clients, because a comparison needs both sides.
 *
 * The Java demo carries four more arms. They are Java's alone: one JVM can hold the engine, the
 * client library in process and the raw wire at once, so each *pair* changes exactly one term.
 * Kotlin has no second Kafka client to compare a wrapper against, so two arms is the whole demo -
 * and the sidecar arm here goes through the client library rather than the protocol, because an
 * earlier Java demo spoke the wire by hand and proved the engine worked while saying nothing about
 * the artifact users actually touch.
 *
 * ## The user function suspends; it does not block
 *
 * The simulated work is `delay`, not `Thread.sleep`, and that is the one place this demo departs
 * from the letter of the shared contract - `demo/README.md` states the reasoning and the
 * measurement that settled it.
 *
 * [run] returns the results rather than only printing them, so that a caller - a test, a future CI
 * entry-point check - can assert what the arms actually did, driving the same code a reader runs
 * rather than a parallel path that could pass while the real one is broken.
 */
internal class KotlinDemo(
    private val options: DemoOptions,
    private val broker: DemoBroker,
    private val topic: String,
) {

    suspend fun run(): List<ArmResult> {
        Console.say("\nEffective configuration:\n  $options\n  topic = $topic")

        broker.ensureTopic(topic, options.partitions)
        broker.seed(topic, 0, options.records)

        val small = listOf(akCore(options.records), kotlinSidecar(options.records))
        report(
            "Small replay - every arm over the same ${options.records} records (the comparison)",
            small, baselineOf(small), acrossReplays = false,
        )

        if (!options.bigReplayWanted) {
            Console.say("\nBig replay skipped (--replay-factor ${options.replayFactor}).")
            return small
        }

        val total = options.bigReplayRecords
        broker.seed(topic, options.records, total)

        // AK core is excluded: it does not go parallel, so it would need total * delayMs
        // milliseconds to finish a backlog the sidecar arm clears in seconds, and making a reader
        // wait that long to learn nothing new is not worth the wall clock.
        val big = listOf(kotlinSidecar(total))
        report(
            "Big replay - $total records, parallel arms only (AK core is serial and would take " +
                "${total * options.delayMs / MILLIS_PER_SECOND}s+)",
            big, baselineOf(small), acrossReplays = true,
        )

        return small + big
    }

    /**
     * The serial arm: Kotlin's own Kafka client, one record at a time, the same wait.
     *
     * `Thread.sleep` here rather than `delay`, and it is not an inconsistency with the other arm:
     * this loop has no other work to interleave, so blocking and suspending cost the same wall
     * clock. The distinction only bites where records run concurrently, which is exactly where the
     * sidecar arm uses `delay`.
     */
    private fun akCore(target: Int): ArmResult {
        Console.say("\n=== $AK_CORE starting over $target records ===")
        val config = Properties().apply {
            putAll(broker.consumerProperties(groupId("ak-core")))
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
        }

        var processed = 0
        KafkaConsumer<ByteArray, ByteArray>(config).use { consumer ->
            consumer.subscribe(listOf(topic))
            // The clock starts AFTER the consumer is built and stops before it closes, because this
            // arm is the denominator of every ratio in both tables and the other arm charges itself
            // for neither construction nor teardown.
            val startedAt = System.nanoTime()
            val deadline = startedAt + ARM_BUDGET.inWholeNanoseconds
            while (processed < target) {
                // The arm that waits on no latch still needs the budget, or a backlog shorter than
                // the target spins here forever with no output.
                check(System.nanoTime() <= deadline) { "$AK_CORE stalled at $processed of $target" }
                for (ignored in consumer.poll(Duration.ofMillis(POLL_MILLIS))) {
                    Thread.sleep(options.delayMs.toLong())
                    processed++
                }
            }
            return finished(AK_CORE, startedAt, processed)
        }
    }

    /**
     * The client library over a real sidecar - the arm the whole design exists for, and the one an
     * application would actually write.
     *
     * The library spawns the sidecar itself ([SidecarCommand]); nothing here installs, deploys or
     * operates a process. The clock starts after the spawn and the handshake, for the same reason
     * the AK core arm's starts after its consumer is built: neither arm charges itself for start-up.
     */
    private suspend fun kotlinSidecar(target: Int): ArmResult {
        Console.say("\n=== $KOTLIN_SIDECAR starting over $target records ===")
        val processed = AtomicInteger()
        val done = CompletableDeferred<Unit>()

        val client = ParallelConsumerClient.open(
            options = ClientOptions(
                topics = listOf(topic),
                kafkaProperties = broker.consumerProperties(groupId("kotlin-sidecar")),
                maxConcurrency = options.maxConcurrency,
                ordering = ProcessingOrder.UNORDERED,
            ),
            sidecar = sidecarCommand(),
        )

        return client.use {
            val startedAt = System.nanoTime()
            coroutineScope {
                val poller = launch(Dispatchers.IO) {
                    client.poll { _ ->
                        // The simulated work: a NON-OCCUPYING wait, because this arm runs
                        // maxConcurrency records at once and a blocking sleep would cap it at the
                        // dispatcher's thread count while the fingerprint still printed the number
                        // the reader asked for. demo/README.md carries the measurement.
                        delay(options.delayMs.toLong())
                        if (processed.incrementAndGet() >= target) {
                            done.complete(Unit)
                        }
                        Outcome.Success()
                    }
                }
                withTimeoutOrNull(ARM_BUDGET) { done.await() }
                    ?: error("$KOTLIN_SIDECAR stalled at ${processed.get()} of $target")
                val result = finished(KOTLIN_SIDECAR, startedAt, processed.get())
                // The clock has already stopped, so the teardown - drain, close, reap the child -
                // is charged to no arm. `use` closes again afterwards; close is idempotent.
                client.close()
                withTimeout(SHUTDOWN_BUDGET) { poller.join() }
                result
            }
        }
    }

    /**
     * The sidecar the client library is told to spawn.
     *
     * The "binary" for a JVM sidecar is a JVM plus a classpath, and the classpath handed to the
     * child is **this process's own** - which is why `demo/run.sh` forks a real JVM rather than
     * running the demo inside Maven's. Under `mvn exec:java` this property would be Maven's
     * classpath and the child would come up without the engine on it.
     */
    private fun sidecarCommand(): SidecarCommand {
        val java = Path.of(System.getProperty("java.home"), "bin", "java")
        return SidecarCommand(java, listOf("-cp", System.getProperty("java.class.path"), SIDECAR_MAIN))
    }

    private fun finished(arm: String, startedAt: Long, processed: Int): ArmResult {
        val elapsed = Duration.ofNanos(System.nanoTime() - startedAt)
        Console.say("=== $arm finished: $processed records in ${elapsed.toMillis()}ms ===")
        return ArmResult(arm, elapsed, processed)
    }

    /** A fresh group per arm per replay, so every arm reads the same records from the beginning. */
    private fun groupId(arm: String) = "pc-demo-$arm-${System.nanoTime()}"

    private companion object {
        const val POLL_MILLIS = 500L
        const val MILLIS_PER_SECOND = 1000
        val SHUTDOWN_BUDGET = 60.seconds
    }
}

private fun baselineOf(results: List<ArmResult>): ArmResult? = results.firstOrNull { it.arm == AK_CORE }

private fun report(title: String, results: List<ArmResult>, baseline: ArmResult?, acrossReplays: Boolean) {
    val table = StringBuilder("\n\n").append(title).append('\n')
    table.append(
        String.format(
            Locale.ROOT, "  %-14s %10s %14s %14s%n", "arm", "elapsed", "msg/s",
            if (acrossReplays) "vs AK core*" else "vs AK core",
        )
    )
    for (result in results) {
        val ratio = if (baseline == null || baseline.ratePerSecond == 0.0) {
            "-"
        } else {
            String.format(Locale.ROOT, "%.1fx", result.ratePerSecond / baseline.ratePerSecond)
        }
        table.append(
            String.format(
                Locale.ROOT, "  %-14s %9.1fs %14s %14s%n",
                result.arm, result.elapsed.toMillis() / 1000.0,
                String.format(Locale.ROOT, "%,d", result.ratePerSecond.toInt()), ratio,
            )
        )
    }
    if (acrossReplays) {
        table.append("\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n")
    }
    Console.say(table.toString())
}
