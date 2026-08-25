// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import kotlinx.coroutines.CancellationException

/**
 * The per-record verdict the user's function returns: success (optionally carrying records for the
 * engine to produce) or failure (carrying a reason). Success lets the record's offset advance;
 * failure returns it to the engine's own retry scheduling, and the reason rides the redelivery as
 * [PreviousFailure.reason].
 *
 * A sealed interface rather than a flag-plus-fields class, because that is what the reference
 * surface's "closed two-armed value" actually is - and here `when` over it is exhaustive, so a
 * third arm could not be added without every consumer being made to handle it. There deliberately
 * is no third arm: a function that cannot decide has not finished processing.
 */
public sealed interface Outcome {

    /** The function completed. [produce] is optional output for the engine to produce. */
    public class Success(public val produce: List<OutboundRecord> = emptyList()) : Outcome {
        override fun toString(): String = "Success(produce=${produce.size})"
    }

    /**
     * The function failed; the record returns to retry scheduling. [reason] rides the redelivery
     * verbatim, so it may be read by whoever handles the record next - and by nobody else.
     */
    public class Failure(public val reason: String? = null) : Outcome {
        override fun toString(): String = "Failure"
    }
}

/**
 * The user's function: one record in, one [Outcome] out.
 *
 * **It is a `suspend` function, and that is the central idiomatic decision of this client.** The
 * proxy's model already has a fixed concurrency ceiling - it never dispatches more than
 * `max_concurrency` records, and this client runs exactly the executors the handshake asked for -
 * so coroutines are not here to multiply concurrency. They are here so the *user's* code may
 * suspend: a Ktor call, a coroutine-native database driver, a `delay`, all without a thread-blocking
 * wrapper and without the library owning a second thread pool it never told anyone about.
 *
 * A record is held for as long as the function runs. There is no per-record deadline, and this
 * client must never build one.
 */
public typealias RecordProcessor = suspend (InboundRecord) -> Outcome

/**
 * The one place a function invocation becomes an [Outcome], so the exception bridge behaves
 * identically wherever it is called from. A throw is a failure carrying the exception's message -
 * one bad record must not tear down the stream.
 *
 * [CancellationException] is re-thrown rather than translated, and that is not an optimisation:
 * swallowing it would turn a cancelled session into a fabricated per-record verdict and break
 * structured concurrency for every caller above. It is the one Kotlin-specific hazard on this
 * surface that has no counterpart in the reference implementation.
 */
@Suppress("TooGenericExceptionCaught") // deliberate: the contract is that ANY throw becomes a failure, once, here
internal suspend fun runProcessor(process: RecordProcessor, record: InboundRecord): Outcome =
    try {
        process(record)
    } catch (cancellation: CancellationException) {
        throw cancellation
    } catch (failure: Exception) {
        Outcome.Failure(failure.message ?: failure.toString())
    }
