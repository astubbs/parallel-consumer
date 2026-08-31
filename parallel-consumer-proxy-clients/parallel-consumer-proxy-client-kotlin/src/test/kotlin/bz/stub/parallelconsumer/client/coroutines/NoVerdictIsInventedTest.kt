// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletionStage
import kotlin.time.Duration.Companion.seconds
import bz.stub.parallelconsumer.client.InboundRecord as JavaInboundRecord
import bz.stub.parallelconsumer.client.Outcome as JavaOutcome

/**
 * The rule that a client never states a verdict for work it did not do, tested where it lives.
 *
 * It is worth its own file because it is a rule about **silence**, and silence has no failure mode
 * anyone would notice: a fabricated success looks exactly like a real one to the proxy, to the
 * offsets, and to every log line either side of it. The wrong behaviour here is a record marked
 * complete that nobody processed, found much later as missing output.
 *
 * The transport this client wraps has exactly one way to say nothing - a `CompletionStage` that
 * never completes ([bz.stub.parallelconsumer.client.AsyncRecordProcessor]) - and these are the two
 * occasions this client uses it.
 */
class NoVerdictIsInventedTest {

    private val record = JavaInboundRecord("orders", 0, 7, null, "v".toByteArray(), 1, null, null)

    @Test
    fun `a record handed out after hand-out stopped is never run and never reported`() = runBlocking {
        var ran = false

        coroutineScope {
            val verdict = startRecord(
                dispatcher = Dispatchers.Default,
                handingOut = false,
                process = { ran = true; Outcome.Success() },
                record = record,
            )

            // not "reported as a failure", and not "reported as a success": not reported at all.
            // The proxy reclaims it as unheld once the session ends, which is the rule connection
            // loss already relies on
            assertThat(verdict.isNotYetDecided()).isTrue()
        }

        assertThat(ran).isFalse()
    }

    @Test
    fun `a cancelled record reports nothing rather than a failure`() = runBlocking {
        val started = CompletableDeferred<Unit>()
        val stage = CompletableDeferred<CompletionStage<JavaOutcome>>()

        val session = launch {
            coroutineScope {
                stage.complete(
                    startRecord(
                        dispatcher = Dispatchers.Default,
                        handingOut = true,
                        process = { started.complete(Unit); awaitCancellation() },
                        record = record,
                    )
                )
            }
        }
        val verdict = withTimeout(BUDGET) { started.await(); stage.await() }

        session.cancel()
        session.join()

        // the hazard this guards: the transport turns an EXCEPTIONALLY completed stage into a
        // failure report, so completing it with the CancellationException would put a verdict on
        // the wire for a record whose processing was cancelled
        assertThat(verdict.isNotYetDecided()).isTrue()
    }

    @Test
    fun `an ordinary record still reaches a verdict`() = runBlocking {
        // the control: the two silences above must not turn out to be the only behaviour there is
        coroutineScope {
            val verdict = startRecord(
                dispatcher = Dispatchers.Default,
                handingOut = true,
                process = { Outcome.Failure("the database said no") },
                record = record,
            )

            val outcome = withTimeout(BUDGET) { verdict.toCompletableFuture().get() }

            assertThat(outcome.isSuccess).isFalse()
            assertThat(outcome.failureReason().get()).isEqualTo("the database said no")
        }
    }

    private fun CompletionStage<JavaOutcome>.isNotYetDecided(): Boolean = !toCompletableFuture().isDone

    private companion object {
        val BUDGET = 30.seconds
    }
}
