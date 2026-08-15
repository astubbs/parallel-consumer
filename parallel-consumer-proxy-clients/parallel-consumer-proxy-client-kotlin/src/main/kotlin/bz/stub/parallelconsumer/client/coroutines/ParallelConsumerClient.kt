// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import bz.stub.parallelconsumer.client.grpc.GrpcParallelConsumerClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds
import bz.stub.parallelconsumer.client.InboundRecord as JavaInboundRecord
import bz.stub.parallelconsumer.client.Outcome as JavaOutcome

private val log = LoggerFactory.getLogger(ParallelConsumerClient::class.java)

/**
 * One record's life, as the transport's asynchronous processor sees it: a coroutine in the session's
 * scope, and a stage that coroutine completes. Nothing waits on a thread anywhere along it - which
 * is the whole reason this client can be a wrapper rather than a second session implementation.
 *
 * **Two of its three exits deliberately leave the stage uncompleted, which the transport's
 * `AsyncRecordProcessor` contract reads as "this client has no verdict for that record".**
 *
 * - *Hand-out has stopped* ([handingOut] false, i.e. shutdown began before this record started). It
 *   was never run, so no verdict is invented for it. `Released` is what the wire has for "returned
 *   unrun", but it is gated by the `shutdown` capability and sending a message outside the
 *   negotiated set would be this client's own violation; off the session the proxy reclaims the
 *   record as unheld, which is what the connection-loss rule already relies on. When `shutdown`
 *   joins the negotiated set, a later wave sends `Released` here instead of saying nothing.
 * - *The record's coroutine was cancelled.* Completing the stage exceptionally would be no better
 *   than swallowing the [CancellationException]: the transport turns an exceptional completion into
 *   a failure report, so either would fabricate a wire verdict for a record whose processing was
 *   cancelled. Saying nothing is the honest answer, and the proxy redelivers.
 *
 * Every other outcome, a thrown exception included, is a verdict - see [runProcessor].
 *
 * It is a top-level `internal` function rather than a method so the two silent paths can be tested
 * directly. They are the kind of rule that is invisible when it breaks: a fabricated verdict looks
 * exactly like a real one from every side.
 */
internal fun CoroutineScope.startRecord(
    dispatcher: CoroutineDispatcher,
    handingOut: Boolean,
    process: RecordProcessor,
    record: JavaInboundRecord,
): CompletionStage<JavaOutcome> {
    val verdict = CompletableFuture<JavaOutcome>()
    if (!handingOut) {
        log.debug("Hand-out has stopped; {} was not run and is reported as nothing", record)
        return verdict
    }
    val running = launch(dispatcher + CoroutineName("pc-record-${record.topic()}-${record.offset()}")) {
        verdict.complete(runProcessor(process, record.toKotlin()).toJava())
    }
    running.invokeOnCompletion { cause ->
        if (cause != null) {
            // `verdict` is left uncompleted on purpose - see this function's second silent path
            log.debug("{} was cancelled before it reached a verdict; reporting nothing for it", record, cause)
        }
    }
    return verdict
}

/**
 * A Parallel Consumer session, driven from Kotlin: key-ordered concurrent Kafka processing with the
 * engine running as a sidecar child process and the user's function running as an ordinary
 * suspending lambda.
 *
 * ```kotlin
 * val client = ParallelConsumerClient.open(
 *     options = ClientOptions(topics = listOf("orders"), kafkaProperties = mapOf("bootstrap.servers" to "…")),
 *     sidecar = SidecarCommand(Path.of("/absolute/path/to/parallel-consumer-proxy")),
 * )
 * client.use { it.poll { record -> if (handle(record.value) ) Outcome.Success() else Outcome.Failure("nope") } }
 * ```
 *
 * ## It wraps the Java client, and that is the design
 *
 * The session itself - the one bidirectional stream, the handshake, the dispatch queue, the
 * overflow rule, the token echo - belongs to `parallel-consumer-proxy-client-java-grpc`, and this
 * class holds one of those and gives it a Kotlin shape. Nothing here reads a protobuf message or
 * touches a channel.
 *
 * **The reason is arithmetic rather than taste.** A second JVM session implementation means every
 * session defect is fixed twice, a third means three times, and Scala was next. The two objections
 * that once made wrapping look wrong were real and are gone: the transport module no longer drags
 * the engine into a wrapper's build (see its pom), and the reference API now takes a processor that
 * answers with a `CompletionStage`, so a suspending function bridges to it without a thread parked
 * per record. What is left for this module is the part that is genuinely Kotlin: suspension,
 * structured concurrency, nullability, sealed types, and cancellation that is not a verdict.
 *
 * The visible consequence is that this client **inherits** the transport's behaviour, including
 * what is wrong with it: a stream error currently parks the executors with no signal to the caller,
 * and this client cannot see it either. That is the compounding argument working as intended -
 * `docs/inflight/parked-proxy-review-findings.md` records the defect, and one fix will cover both.
 *
 * The client is **stateless per record**. The fencing token never reaches this layer at all; it
 * rides from the dispatch queue to the report inside the transport. Fencing is the proxy's job.
 *
 * Wave one implements exactly the `dispatch` capability: connect, configure, receive a wave, run the
 * function, report, shut down cleanly. Leases and heartbeats, the manifest reconnect, worker-death
 * reporting, terminal outcomes and the `Shutdown` drain are later waves, and this client declares
 * none of them - so the proxy does not expect them of it.
 */
public class ParallelConsumerClient private constructor(
    private val sidecar: Sidecar,
    private val transport: GrpcParallelConsumerClient,
    /** The effective, negotiated session - what the proxy said it is running, not what was asked. */
    public val session: Session,
    private val dispatcher: CoroutineDispatcher,
) : AutoCloseable {

    private val polled = AtomicBoolean(false)
    private val ended = CompletableDeferred<Unit>()
    private val tornDown = CompletableDeferred<Unit>()
    private val teardownStarted = AtomicBoolean(false)

    /**
     * False once shutdown has begun. A record handed out after that point is *not* run: the client
     * never invents a verdict for work it did not do - see [startRecord].
     */
    @Volatile
    private var handingOut = true

    /**
     * Runs the session, handing every delivered record to [process].
     *
     * **It suspends for the life of the session - it does not block a thread, and it does not
     * return once processing has started.** That is this client's answer to a question the shared
     * specification leaves open, and Kotlin's structured concurrency is the reason: a function that
     * starts background work and returns leaves coroutines nobody owns, which is precisely what
     * structured concurrency exists to prevent. Suspending instead makes the session a child of the
     * caller's scope, so cancelling that scope ends the session, an enclosing `withTimeout` bounds
     * it, and a failure propagates to the caller rather than into a callback.
     *
     * Each record runs as a coroutine in that scope, launched on [dispatcher] by [startRecord]. The
     * transport holds no thread while one is running: it is handed a `CompletionStage` the coroutine
     * completes.
     *
     * It returns when [close] is called, once every record already started has finished and
     * reported. Cancellation is the abrupt path - executing records are cancelled with their
     * coroutine and the proxy will redeliver them, because a cancelled record is reported as
     * nothing at all rather than as a failure.
     *
     * **It does not return when the proxy ends the stream**, and that is the inherited defect named
     * in this class's documentation rather than a decision made here.
     *
     * May be called at most once per client.
     */
    public suspend fun poll(process: RecordProcessor) {
        check(polled.compareAndSet(false, true)) { "poll may be called at most once per client" }
        try {
            coroutineScope {
                transport.pollAsync { record -> startRecord(dispatcher, handingOut, process, record) }
                log.info(
                    "Polling: up to {} record(s) in flight, {} executor(s) under the transport",
                    session.maxConcurrency(), session.executorCount(),
                )
                try {
                    ended.await()
                } finally {
                    // before the scope joins the records still running: a record handed out after
                    // this point must not start, and on the cancellation path this is what stops
                    // the transport being answered by an already-cancelled scope
                    handingOut = false
                }
            }
        } finally {
            withContext(NonCancellable) { teardown() }
        }
    }

    /**
     * Ends the session: stops hand-out, lets executing records finish and report, closes the
     * transport and reaps the sidecar. Idempotent.
     *
     * When [poll] is running, the teardown happens on its coroutine, after its records have
     * reported - this call signals and then waits for that, so `use { }` does not return with a
     * sidecar still alive.
     */
    override fun close() {
        handingOut = false
        ended.complete(Unit)
        if (!polled.get()) {
            teardown()
            return
        }
        val completed = runBlocking { withTimeoutOrNull(CLOSE_GRACE) { tornDown.await() } }
        if (completed == null) {
            log.warn("The session did not tear itself down within {}; closing it from here", CLOSE_GRACE)
            teardown()
        }
    }

    private fun teardown() {
        if (!teardownStarted.compareAndSet(false, true)) {
            return
        }
        transport.close()
        // last, and only now: killing the sidecar with the stream open would turn a clean drain
        // into a reconnect-window recovery for the next group member
        sidecar.reap(REAP_GRACE)
        tornDown.complete(Unit)
    }

    public companion object {

        private val SPAWN_BUDGET = 30.seconds
        private val HANDSHAKE_BUDGET = 30.seconds
        private val REAP_GRACE = 15.seconds
        private val CLOSE_GRACE = 30.seconds

        /**
         * Spawns the sidecar, connects to the loopback port it reports, and completes the
         * fresh-session handshake. The returned client is open: [Session] carries what the proxy is
         * actually running with.
         *
         * Suspending rather than blocking, because every step of it waits on something - a process
         * to print a line, a handshake to come back - and a library that blocks a caller's thread
         * to wait is not one a Kotlin application can compose. The handshake is awaited as a
         * `CompletionStage`, so not even one pooled thread is parked on it.
         *
         * [dispatcher] is where each record's coroutine runs. It is injected rather than assumed so
         * an application can hand the library its own, and a test can hand it a controlled one.
         */
        public suspend fun open(
            options: ClientOptions,
            sidecar: SidecarCommand,
            dispatcher: CoroutineDispatcher = Dispatchers.IO,
        ): ParallelConsumerClient {
            val started = Sidecar.spawn(sidecar, SPAWN_BUDGET)
            val transport = GrpcParallelConsumerClient.builder()
                .port(started.port)
                .options(options.toJava())
                .build()
            return runCatching {
                val session = withTimeoutOrNull(HANDSHAKE_BUDGET) { transport.connect().await() }
                    ?: error("no Configured arrived within $HANDSHAKE_BUDGET - is the sidecar still alive?")
                log.info("Connected: {}", session)
                ParallelConsumerClient(started, transport, session, dispatcher)
            }.getOrElse { failure ->
                transport.close()
                started.reap(REAP_GRACE)
                throw failure
            }
        }
    }
}
