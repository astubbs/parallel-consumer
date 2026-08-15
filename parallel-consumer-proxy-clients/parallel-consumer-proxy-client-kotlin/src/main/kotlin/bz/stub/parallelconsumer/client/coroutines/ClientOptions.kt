// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import kotlin.time.Duration

/**
 * The ordering guarantee asked of the engine. Ordering is enforced by the engine, never by this
 * client.
 *
 * It is the reference API's own enum rather than a Kotlin copy of it. Everything this client
 * respells - `Optional`, builders, a boolean-flagged outcome, two independent failure nullables -
 * is respelled because Kotlin says it better; a three-constant enum is already the same thing in
 * both languages, and copying it would only create two places for a fourth constant to be added.
 */
public typealias ProcessingOrder = bz.stub.parallelconsumer.client.ProcessingOrder

/**
 * The proxy did something the frozen contract forbids: not a load condition, not retryable, and the
 * session is over.
 *
 * Detecting it is the transport's job and the type is the transport's own, so every JVM client
 * raises the same exception for the same breach - a Kotlin-local copy would be a second type
 * meaning one thing.
 */
public typealias ProxyProtocolViolation = bz.stub.parallelconsumer.client.grpc.ProxyProtocolViolation

/**
 * The negotiated session, as the proxy reported it in `Configured` - the effective values this
 * client is actually running under.
 *
 * `capabilities` is the negotiated intersection, and it is the only thing that says which duties
 * exist on this session. This client declares `dispatch` and nothing else, so a session that comes
 * back with more tokens grants abilities it does not use; one that comes back with fewer would mean
 * the proxy cannot dispatch at all.
 */
public typealias Session = bz.stub.parallelconsumer.client.grpc.NegotiatedSession

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
