// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger(ParallelConsumerClient::class.java)

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
 * The shape is the one every language's client implements: an admin holding the single stream and
 * the dispatch queue, and N executors taking records from it. **Here the executors are coroutines**,
 * launched on an injectable dispatcher - so an application already running on its own dispatcher
 * gets the library's work on that dispatcher, and a test gets a deterministic one.
 *
 * The client is **stateless per record**. The fencing token rides from the queue to the report on
 * the executing coroutine's stack and is echoed byte-identically; there is no request map, no dedupe
 * cache, no completion registry. Fencing is the proxy's job.
 *
 * Wave one implements exactly the `dispatch` capability: connect, configure, receive a wave, run the
 * function, report, shut down cleanly. Leases and heartbeats, the manifest reconnect, worker-death
 * reporting, terminal outcomes and the `Shutdown` drain are later waves, and this client declares
 * none of them - so the proxy does not expect them of it.
 */
public class ParallelConsumerClient private constructor(
    private val sidecar: Sidecar,
    private val channel: ManagedChannel,
    private val stream: ProxyStream,
    /** The effective, negotiated session - what the proxy said it is running, not what was asked. */
    public val session: Session,
    private val dispatcher: CoroutineDispatcher,
) : AutoCloseable {

    private val polled = AtomicBoolean(false)
    private val tornDown = CompletableDeferred<Unit>()
    private val teardownStarted = AtomicBoolean(false)

    /**
     * Runs the session: starts [Session.executorCount] executor coroutines and hands every
     * delivered record to [process].
     *
     * **It suspends for the life of the session - it does not block a thread, and it does not
     * return once processing has started.** That is this client's answer to a question the shared
     * specification leaves open, and Kotlin's structured concurrency is the reason: a function that
     * starts background work and returns leaves coroutines nobody owns, which is precisely what
     * structured concurrency exists to prevent. Suspending instead makes the session a child of the
     * caller's scope, so cancelling that scope ends the session, an enclosing `withTimeout` bounds
     * it, and a failure propagates to the caller rather than into a callback.
     *
     * It returns when the session ends: [close] was called, or the proxy completed the stream. It
     * throws [ProxyProtocolViolation] if the proxy broke the contract. Cancellation is the abrupt
     * path - executing records are cancelled with their coroutine, and the proxy will redeliver
     * them - whereas [close] is the clean one: hand-out stops, executing records finish and report,
     * and only then is the stream half-closed.
     *
     * May be called at most once per client.
     */
    public suspend fun poll(process: RecordProcessor) {
        check(polled.compareAndSet(false, true)) { "poll may be called at most once per client" }
        try {
            coroutineScope {
                val executors = List(session.executorCount) { number ->
                    launch(dispatcher + CoroutineName("pc-executor-${number + 1}")) { executorLoop(process) }
                }
                log.info("Polling: {} executor coroutine(s), dispatch queue depth {}",
                    session.executorCount, session.maxConcurrency)
                try {
                    stream.awaitEnd()
                } finally {
                    stream.stopHandout()
                }
                executors.joinAll()
            }
        } finally {
            withContext(NonCancellable) { teardown() }
        }
    }

    /**
     * Ends the session: stops hand-out, lets executing records finish and report, half-closes the
     * stream, and reaps the sidecar. Idempotent.
     *
     * When [poll] is running, the teardown happens on its coroutine, after its executors have
     * reported - this call signals and then waits for that, so `use { }` does not return with a
     * sidecar still alive.
     */
    override fun close() {
        stream.requestEnd()
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

    private suspend fun executorLoop(process: RecordProcessor) {
        for (dispatched in stream.dispatches) {
            if (!stream.handingOut) {
                // shutdown began while this record sat in the queue: it was never run, so no
                // verdict is invented for it - see ProxyStream.stopHandout
                break
            }
            // the token travels dispatch -> report on this coroutine's stack; nothing is stored
            val outcome = runProcessor(process, Wire.toInboundRecord(dispatched))
            stream.report(dispatched.token, outcome)
        }
    }

    private fun teardown() {
        if (!teardownStarted.compareAndSet(false, true)) {
            return
        }
        stream.halfClose()
        channel.shutdown()
        if (!channel.awaitTermination(CHANNEL_GRACE.inWholeSeconds, TimeUnit.SECONDS)) {
            channel.shutdownNow()
        }
        // last, and only now: killing the sidecar with the stream open would turn a clean drain
        // into a reconnect-window recovery for the next group member
        sidecar.reap(REAP_GRACE)
        tornDown.complete(Unit)
    }

    public companion object {

        private const val LOOPBACK_HOST = "127.0.0.1"
        private val SPAWN_BUDGET = 30.seconds
        private val HANDSHAKE_BUDGET = 30.seconds
        private val CHANNEL_GRACE = 10.seconds
        private val REAP_GRACE = 15.seconds
        private val CLOSE_GRACE = 30.seconds

        /**
         * Spawns the sidecar, connects to the loopback port it reports, and completes the
         * fresh-session handshake. The returned client is open: [Session] carries what the proxy is
         * actually running with.
         *
         * Suspending rather than blocking, because every step of it waits on something - a process
         * to print a line, a handshake to come back - and a library that blocks a caller's thread
         * to wait is not one a Kotlin application can compose.
         *
         * [dispatcher] is where the executor coroutines run. It is injected rather than assumed so
         * an application can hand the library its own, and a test can hand it a controlled one.
         */
        public suspend fun open(
            options: ClientOptions,
            sidecar: SidecarCommand,
            dispatcher: CoroutineDispatcher = Dispatchers.IO,
        ): ParallelConsumerClient {
            val started = Sidecar.spawn(sidecar, SPAWN_BUDGET)
            val channel = ManagedChannelBuilder.forAddress(LOOPBACK_HOST, started.port).usePlaintext().build()
            val stream = ProxyStream()
            return runCatching {
                stream.open(ProxyServiceGrpc.newStub(channel))
                val session = Wire.toSession(stream.configure(options, HANDSHAKE_BUDGET))
                log.info("Connected: {}", session)
                ParallelConsumerClient(started, channel, stream, session, dispatcher)
            }.getOrElse { failure ->
                channel.shutdownNow()
                started.reap(REAP_GRACE)
                throw failure
            }
        }
    }
}
