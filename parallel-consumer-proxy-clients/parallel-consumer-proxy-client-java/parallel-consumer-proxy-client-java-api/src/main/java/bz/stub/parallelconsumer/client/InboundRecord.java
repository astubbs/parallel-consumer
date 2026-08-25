package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Instant;
import java.util.Optional;

/**
 * One record as delivered to a {@link RecordProcessor}: the Kafka record plus the Parallel-Consumer-derived
 * delivery state an in-process user function would see (R5) - which delivery attempt this is, and the previous
 * failure when there was one. That state is <em>product data</em>, identical under every transport; nothing
 * transport-specific (epochs, tokens, connection identity) appears here, deliberately.
 * <p>
 * Key and value are the bytes Kafka held - the wrapper never deserializes - and either may be {@code null},
 * because Kafka distinguishes a null key or value (a tombstone) from an empty one. The arrays are not copied;
 * treat them as read-only.
 *
 * @author Antony Stubbs
 */
public final class InboundRecord {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] key;
    private final byte[] value;
    private final int attempt;
    private final Instant lastFailureAt;
    private final String lastFailureReason;

    /**
     * @param attempt           1 on first delivery, 2 on the first redelivery
     * @param lastFailureAt     {@code null} before the first failure - absence is the form of "has not failed",
     *                          never a zero timestamp
     * @param lastFailureReason {@code null} before the first failure; otherwise the reason recorded for the
     *                          previous failed attempt. May embed record payload: untrusted input.
     */
    public InboundRecord(String topic, int partition, long offset, byte[] key, byte[] value,
                         int attempt, Instant lastFailureAt, String lastFailureReason) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.attempt = attempt;
        this.lastFailureAt = lastFailureAt;
        this.lastFailureReason = lastFailureReason;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    /** The record's key bytes, or {@code null} for a keyless record. Not copied; treat as read-only. */
    public byte[] key() {
        return key;
    }

    /** The record's value bytes, or {@code null} for a tombstone. Not copied; treat as read-only. */
    public byte[] value() {
        return value;
    }

    /** Which delivery attempt this is: 1 on first delivery, 2 on the first redelivery. */
    public int attempt() {
        return attempt;
    }

    /** When the previous attempt failed; empty before the first failure. */
    public Optional<Instant> lastFailureAt() {
        return Optional.ofNullable(lastFailureAt);
    }

    /** The reason recorded for the previous failed attempt; empty before the first failure. */
    public Optional<String> lastFailureReason() {
        return Optional.ofNullable(lastFailureReason);
    }

    @Override
    public String toString() {
        // deliberately omits key and value: payloads are untrusted input and do not belong in log lines
        return "InboundRecord(" + topic + "-" + partition + "@" + offset + ", attempt " + attempt + ")";
    }
}
