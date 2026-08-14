// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import kotlin.time.Duration

/**
 * The ordering guarantee asked of the engine. Ordering is enforced by the engine, never by this
 * client.
 */
public enum class ProcessingOrder {
    /** No ordering: any record may process concurrently with any other. */
    UNORDERED,

    /** Records of one partition process one at a time, in offset order. */
    PARTITION,

    /** Records sharing a key process one at a time, in offset order; distinct keys run concurrently. */
    KEY,
}

/**
 * Connect-time configuration, and the only configuration channel there is: these values travel in
 * the session's `Configure` message and nowhere else - no file, no environment variable, no shell,
 * no command line.
 *
 * **`null` means "take the engine's default".** That is the whole reason this class holds a
 * nullable field where the reference Java surface holds an `Optional`, and why it needs no builder:
 * Kotlin's default arguments say "unset" once, at the declaration, and named arguments make a call
 * that sets three of nine fields as readable as a builder chain. What the default resolved to is
 * reported back in [Session] - assert what came back, never what was asked for.
 *
 * [kafkaProperties] is credential-bearing. [toString] omits it deliberately, and nothing in this
 * library logs it at any level.
 */
public class ClientOptions(
    /** The subscription, fixed for the session's lifetime. At least one topic is required. */
    public val topics: List<String>,
    /** Kafka client configuration - bootstrap servers, group id, credentials. Never logged. */
    public val kafkaProperties: Map<String, String> = emptyMap(),
    /** The in-flight ceiling: records the proxy may have out to this client at once. */
    public val maxConcurrency: Int? = null,
    /** The ordering guarantee. */
    public val ordering: ProcessingOrder? = null,
    /** How often the engine commits offsets. */
    public val commitInterval: Duration? = null,
    /** How long a failed record waits before redelivery. */
    public val defaultMessageRetryDelay: Duration? = null,
) {
    init {
        require(topics.isNotEmpty()) { "at least one topic is required: the subscription is fixed at connect time" }
        require(maxConcurrency == null || maxConcurrency >= 1) {
            "maxConcurrency must be at least 1 when set, got $maxConcurrency - there is no 'unlimited'"
        }
    }

    /** Deliberately omits [kafkaProperties]: it may carry credentials. */
    override fun toString(): String =
        "ClientOptions(topics=$topics, maxConcurrency=$maxConcurrency, ordering=$ordering, " +
            "commitInterval=$commitInterval, defaultMessageRetryDelay=$defaultMessageRetryDelay)"
}

/**
 * The negotiated session, as the proxy reported it in `Configured` - the effective values this
 * client is actually running under.
 *
 * [capabilities] is the negotiated intersection, and it is the only thing that says which duties
 * exist on this session. This client declares `dispatch` and nothing else, so a session that comes
 * back with more tokens grants abilities it does not use; one that comes back with fewer would mean
 * the proxy cannot dispatch at all.
 */
public class Session(
    /** How many executors to run. Sent once by the proxy, never revised. */
    public val executorCount: Int,
    /** The effective in-flight ceiling, which is also this client's dispatch-queue depth. */
    public val maxConcurrency: Int,
    /** The negotiated capability tokens. */
    public val capabilities: Set<String>,
) {
    override fun toString(): String =
        "Session(executorCount=$executorCount, maxConcurrency=$maxConcurrency, capabilities=$capabilities)"
}

/**
 * A protocol violation this client detected: the proxy did something the frozen contract forbids.
 *
 * It is not a load condition and not retryable - the session is over. A gRPC client cannot answer
 * with a status code (only a server sets one), so the call is cancelled and this is raised instead.
 */
public class ProxyProtocolViolation(message: String) : IllegalStateException(message)
