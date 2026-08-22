// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

/**
 * The one translation from a user function's behaviour to an outcome, and the surface rules that go
 * with it.
 */
class ProcessorOutcomeTest {

    private val record = InboundRecord("orders", 0, 0, null, null, 1, null)

    @Test
    fun `a returned outcome is the outcome`() = runTest {
        val outcome = runProcessor({ Outcome.Success() }, record)

        assertThat(outcome).isInstanceOf(Outcome.Success::class.java)
    }

    @Test
    fun `a thrown exception becomes a failure carrying its message - one bad record is not fatal`() = runTest {
        val outcome = runProcessor({ error("the database said no") }, record)

        assertThat(outcome).isInstanceOf(Outcome.Failure::class.java)
        assertThat((outcome as Outcome.Failure).reason).isEqualTo("the database said no")
    }

    @Test
    fun `a message-less exception still names something`() = runTest {
        val outcome = runProcessor({ throw IllegalStateException() }, record)

        assertThat((outcome as Outcome.Failure).reason).contains("IllegalStateException")
    }

    @Test
    fun `cancellation is re-thrown, never translated into a verdict for the record`() = runTest {
        // the Kotlin-specific hazard: swallowing this would fabricate an outcome for a record whose
        // processing was cancelled, and break structured concurrency for every caller above
        assertThrows<CancellationException> {
            runProcessor({ throw CancellationException("the scope went away") }, record)
        }
    }

    @Test
    fun `a suspending user function is supported, which is the point of the surface`() = runTest {
        val outcome = runProcessor({ delay(1.seconds); Outcome.Success() }, record)

        assertThat(outcome).isInstanceOf(Outcome.Success::class.java)
    }

    @Test
    fun `the sidecar is never resolved by a relative path`() {
        val relative = assertThrows<IllegalArgumentException> { SidecarCommand(Path.of("parallel-consumer-proxy")) }

        assertThat(relative).hasMessageThat().contains("absolute")
    }

    @Test
    fun `an outbound record needs a destination topic`() {
        assertThrows<IllegalArgumentException> { OutboundRecord("") }
    }

    @Test
    fun `a record never prints its payload`() {
        val payload = InboundRecord("orders", 3, 12, "k".toByteArray(), "hunter2".toByteArray(), 1, null)

        assertThat(payload.toString()).doesNotContain("hunter2")
        assertThat(payload.toString()).contains("orders-3@12")
    }
}
