// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.seconds

/**
 * Wave one's end-to-end proof: one record, through the real test-mode sidecar, over the real wire.
 *
 * The sidecar is `TestModeMain` from the proxy module's **test** jar, spawned as an ordinary child
 * process - so this test exercises the whole lifecycle contract the specification describes and not
 * an in-process shortcut: launch directly, hold the stdin pipe, find the port line, connect to
 * loopback, handshake, dispatch, report, half-close, reap.
 *
 * **It runs only in the harness lane** (`-Dpc.foreignClients`, which this module's CI row passes),
 * because the classpath it needs is what a permanent Maven edge to the engine module would cost -
 * see the `kotlin-e2e-harness` profile. When the classpath file is missing it FAILS and names the
 * command; a test that quietly does not run is not a passing test.
 *
 * Three harness limitations are absorbed here rather than worked around: its stdout logs before the
 * port line (the client scans, so nothing to do), it serves until stdin EOF rather than exiting
 * after a drain (so the reap is closing stdin, which is what the client does anyway), and it has no
 * verdict channel - so what is asserted is the wire-observable consequence: the dispatch arrives
 * with the delivery state the specification promises, and a reported success is followed by
 * silence, not a redelivery.
 */
class OneRecordThroughTheSidecarTest {

    @Test
    fun `a processed record advances the committed offset`() = runBlocking {
        // the scenario name is also the topic name: the harness seeds its records on a topic named
        // after the scenario
        val scenario = "a-processed-record-advances-the-committed-offset"
        val deliveries = CopyOnWriteArrayList<InboundRecord>()
        val firstDelivery = CompletableDeferred<InboundRecord>()

        val client = ParallelConsumerClient.open(
            // kafka_properties is empty deliberately: --mock builds mock Kafka clients and reads
            // none, and real credentials never belong in a conformance test
            options = ClientOptions(topics = listOf(scenario)),
            sidecar = harnessSidecar(scenario),
        )

        client.use {
            assertThat(client.session.capabilities).containsExactly(Wire.DISPATCH_CAPABILITY)
            assertThat(client.session.executorCount).isAtLeast(1)
            assertThat(client.session.maxConcurrency).isAtLeast(1)

            val poller = launch(Dispatchers.IO) {
                client.poll { record ->
                    deliveries += record
                    firstDelivery.complete(record)
                    Outcome.Success()
                }
            }

            val record = withTimeout(DISPATCH_BUDGET) { firstDelivery.await() }
            assertThat(record.topic).isEqualTo(scenario)
            assertThat(record.partition).isEqualTo(0)
            assertThat(record.offset).isEqualTo(0)
            assertThat(record.attempt).isEqualTo(1)
            assertThat(record.previousFailure).isNull()
            assertThat(record.value).isNotNull()

            // a success is followed by silence rather than a redelivery - the wire-observable form
            // of the offset advancing past it, which the harness has no channel to state directly
            delay(SILENCE_WINDOW)
            assertThat(deliveries).hasSize(1)

            client.close()
            withTimeout(SHUTDOWN_BUDGET) { poller.join() }
        }
    }

    /**
     * The sidecar command for one scenario. `TestModeMain` ships in a test jar, so "the sidecar
     * binary" here is the JVM launcher and the classpath is an argument - everything awkward about
     * that lives in this one function.
     */
    private fun harnessSidecar(scenario: String): SidecarCommand {
        val classpathFile = Path.of("target", "sidecar-classpath.txt").toAbsolutePath()
        check(Files.isRegularFile(classpathFile)) {
            "$classpathFile is missing - it is written by the kotlin-e2e-harness profile: run " +
                "`./mvnw --batch-mode test -pl :parallel-consumer-proxy-client-kotlin -Dpc.foreignClients`"
        }
        val classpath = Files.readString(classpathFile).trim()
        check(classpath.isNotEmpty()) { "$classpathFile is empty" }

        // PATH lookup would be wrong in the library and is unnecessary here: this is the JVM the
        // test itself is running on
        val java = Path.of(System.getProperty("java.home"), "bin", "java")
        return SidecarCommand(java, listOf("-cp", classpath, HARNESS_MAIN, "--mock", "--scenario", scenario))
    }

    private companion object {
        const val HARNESS_MAIN = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"
        val DISPATCH_BUDGET = 60.seconds
        val SILENCE_WINDOW = 3.seconds
        val SHUTDOWN_BUDGET = 30.seconds
    }
}
