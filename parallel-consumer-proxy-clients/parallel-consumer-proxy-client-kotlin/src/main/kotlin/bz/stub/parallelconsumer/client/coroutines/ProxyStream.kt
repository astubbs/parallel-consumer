// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc
import bz.stub.parallelconsumer.proxy.protocol.v1.Token
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import kotlin.time.Duration

private val log = LoggerFactory.getLogger("bz.stub.parallelconsumer.client.coroutines.ProxyStream")

/**
 * The one bidirectional session: the admin half of the client. It holds the stream, completes the
 * handshake, owns the dispatch queue, and serializes every send.
 *
 * **It always reads and never backpressures by not reading.** The stream also carries the control
 * plane, so an admin that stopped reading to slow the proxy down would head-of-line-block itself.
 * Inbound records are offered to a bounded queue whose depth is the proxy's own in-flight ceiling -
 * so in a correct system it cannot overflow, and an overflow is a protocol violation rather than a
 * load condition.
 */
internal class ProxyStream {

    private val sendLock = Any()
    private val configured = CompletableDeferred<Configured>()
    private val ended = CompletableDeferred<Unit>()

    private lateinit var requests: StreamObserver<ClientMessage>

    @Volatile
    private var closedForSend = false

    @Volatile
    private var queue: Channel<DispatchRecord>? = null

    @Volatile
    private var queueDepth = 0

    /**
     * False once shutdown has begun. A queued record is *not* run after that point: the client
     * never invents a verdict for work it did not do.
     */
    @Volatile
    var handingOut: Boolean = true
        private set

    /** The dispatch queue. Valid once [configure] has returned; the handshake creates it. */
    val dispatches: ReceiveChannel<DispatchRecord>
        get() = requireNotNull(queue) { "the dispatch queue does not exist until the handshake completes" }

    fun open(stub: ProxyServiceGrpc.ProxyServiceStub) {
        requests = stub.session(Responses())
    }

    /**
     * Sends `Configure` and waits for the proxy's effective configuration. Only when `Configured`
     * arrives is the session open - and what it reports, not what was asked for, is what governs.
     */
    suspend fun configure(options: ClientOptions, budget: Duration): Configured {
        send(ClientMessage.newBuilder().setConfigure(Wire.toConfigure(options)).build())
        return withTimeoutOrNull(budget) { configured.await() }
            ?: error("no Configured arrived within $budget - is the sidecar still alive?")
    }

    /** Reports one record's outcome, with its token echoed verbatim. */
    fun report(token: Token, outcome: Outcome) {
        send(ClientMessage.newBuilder().setReport(Wire.toReport(token, outcome)).build())
    }

    /** Suspends until the session ends - by request, by the proxy completing it, or by violation. */
    suspend fun awaitEnd() {
        ended.await()
    }

    /** Client-initiated shutdown: asks the session to end. Safe from any thread. */
    fun requestEnd() {
        ended.complete(Unit)
    }

    /**
     * Stops hand-out. Records still queued are discarded rather than run or reported.
     *
     * `Released` is what the wire has for "returned unrun", but it is gated by the `shutdown`
     * capability, and sending a message outside the negotiated set would be this client's own
     * violation. Off that session the proxy reclaims the records as unheld - which is exactly what
     * the connection-loss rule already relies on. When `shutdown` joins the negotiated set, a later
     * wave sends `Released` here instead of discarding.
     */
    fun stopHandout() {
        handingOut = false
        queue?.close()
    }

    /**
     * Half-closes the stream: the client-initiated shutdown signal. There is no shutdown-request
     * message, deliberately - a client that has reported everything has nothing left to say.
     */
    fun halfClose() {
        synchronized(sendLock) {
            if (closedForSend) {
                return
            }
            closedForSend = true
            runCatching { requests.onCompleted() }
                .onFailure { log.debug("The stream refused completion; it was already terminated", it) }
        }
    }

    private fun send(message: ClientMessage) {
        synchronized(sendLock) {
            if (closedForSend) {
                log.debug("Dropping a {} message: the stream is closed", message.messageCase)
                return
            }
            runCatching { requests.onNext(message) }.onFailure {
                closedForSend = true
                log.warn("The stream is no longer writable; dropping a {} message", message.messageCase, it)
            }
        }
    }

    /**
     * Ends the session on a protocol violation.
     *
     * **A gRPC client cannot answer with a status code** - only the server side of a call sets one -
     * so cancelling the call is the whole of what this client may do about the proxy breaking the
     * contract. The violation is raised locally, naming what was breached, and reaches the
     * application through [awaitEnd].
     */
    private fun fail(violation: ProxyProtocolViolation) {
        log.error("Failing the session: {}", violation.message)
        synchronized(sendLock) {
            if (!closedForSend) {
                closedForSend = true
                runCatching { requests.onError(violation) }
            }
        }
        configured.completeExceptionally(violation)
        ended.completeExceptionally(violation)
    }

    /** The inbound half. gRPC serializes these callbacks, so nothing here needs its own lock. */
    private inner class Responses : StreamObserver<ProxyMessage> {

        override fun onNext(message: ProxyMessage) {
            when (message.messageCase) {
                ProxyMessage.MessageCase.CONFIGURED -> onConfigured(message.configured)
                ProxyMessage.MessageCase.DISPATCH -> onDispatch(message.dispatch)
                else ->
                    // Drop, Shutdown and their duties belong to later waves; SetExecutorCount is
                    // declared-unused and never sent by a v1 proxy. Ignoring an un-negotiated
                    // message is the specification's own remedy - never acting on it.
                    log.warn("Ignoring a proxy message this wave does not implement: {}", message.messageCase)
            }
        }

        private fun onConfigured(effective: Configured) {
            if (queue != null) {
                // a re-sent Configured is the proxy's refusal of a second Configure; this client
                // never sends one, so the original configuration stands
                log.warn("Ignoring an unexpected repeat Configured")
                return
            }
            // created HERE, before the handshake releases the caller: the first dispatch can arrive
            // on this same callback thread immediately afterwards and must find the queue existing.
            queueDepth = effective.maxConcurrency
            queue = Channel(capacity = queueDepth)
            configured.complete(effective)
        }

        private fun onDispatch(wave: Dispatch) {
            val open = queue
            // hand-out is FIFO by arrival and, within a wave, by record order - the one ordering
            // every language expresses identically
            for (record in wave.recordsList) {
                if (open == null || open.trySend(record).isFailure) {
                    fail(
                        ProxyProtocolViolation(
                            "dispatch queue overflow: the proxy dispatched past its own declared in-flight " +
                                "ceiling of $queueDepth records"
                        )
                    )
                    return
                }
            }
        }

        override fun onError(t: Throwable) {
            closedForSend = true
            configured.completeExceptionally(t)
            ended.completeExceptionally(t)
        }

        override fun onCompleted() {
            closedForSend = true
            log.debug("The proxy completed the session stream")
            ended.complete(Unit)
        }
    }
}
