// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import java.time.Instant

/**
 * What the engine knows about this record's previous delivery. Its presence *is* the statement "has
 * failed before"; two independently nullable fields would let a caller ask a question - a reason
 * without a time - that the wire cannot answer.
 */
public class PreviousFailure(
    /** When the previous attempt failed. */
    public val at: Instant,
    /** The reason recorded for that attempt, verbatim. Worker-supplied, so untrusted input. */
    public val reason: String?,
) {
    override fun toString(): String = "PreviousFailure(at=$at, reason=${reason?.let { "<${it.length} chars>" }})"
}

/**
 * One record as delivered to the user's function: the Kafka record plus the delivery state an
 * in-process Parallel Consumer function would see. Nothing transport-shaped appears here - no
 * token, no epoch, no connection identity - because none of it is the user's business.
 *
 * [key] and [value] are the bytes Kafka held; this client never deserializes. `null` is Kafka's own
 * distinction, not an absence of information: a null value is a tombstone, which is not an empty
 * value. The arrays are not copied - treat them as read-only.
 */
@Suppress("LongParameterList") // these fields ARE the record; nesting them to satisfy a count would
// only move the same seven values behind an extra dereference, in the one class nine other
// languages mirror field for field
public class InboundRecord(
    public val topic: String,
    public val partition: Int,
    public val offset: Long,
    public val key: ByteArray?,
    public val value: ByteArray?,
    /** 1 on first delivery, 2 on the first redelivery. */
    public val attempt: Int,
    /** `null` before the first failure. */
    public val previousFailure: PreviousFailure?,
) {
    /** Deliberately omits key and value: payloads are untrusted input and do not belong in a log line. */
    override fun toString(): String = "InboundRecord($topic-$partition@$offset, attempt $attempt)"
}

/**
 * A record a successful outcome asks the engine to produce - the only sanctioned route for a
 * worker's Kafka output. Workers never produce directly; the proxy produces with its own producer
 * before the input record's offset may become eligible to commit.
 */
public class OutboundRecord(
    public val topic: String,
    /** `null` for a keyless record. */
    public val key: ByteArray? = null,
    /** `null` for a tombstone. */
    public val value: ByteArray? = null,
) {
    init {
        require(topic.isNotEmpty()) { "an OutboundRecord needs a destination topic" }
    }

    /** Deliberately omits key and value: payloads are untrusted input and do not belong in a log line. */
    override fun toString(): String = "OutboundRecord($topic)"
}
