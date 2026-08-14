package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * A record a successful {@link Outcome} asks Parallel Consumer to produce - the only sanctioned route for a
 * processor's Kafka output (KTD7: workers never produce directly; the engine produces with its own producer,
 * at-least-once, before the input record's offset becomes eligible to commit).
 * <p>
 * Key and value may be {@code null}, mirroring {@link InboundRecord}'s tombstone distinction. The arrays are
 * not copied.
 *
 * @author Antony Stubbs
 */
public final class OutboundRecord {

    private final String topic;
    private final byte[] key;
    private final byte[] value;

    private OutboundRecord(String topic, byte[] key, byte[] value) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("an OutboundRecord needs a destination topic");
        }
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public static OutboundRecord of(String topic, byte[] key, byte[] value) {
        return new OutboundRecord(topic, key, value);
    }

    public String topic() {
        return topic;
    }

    /** The key bytes to produce, or {@code null} for a keyless record. */
    public byte[] key() {
        return key;
    }

    /** The value bytes to produce, or {@code null} for a tombstone. */
    public byte[] value() {
        return value;
    }

    @Override
    public String toString() {
        // deliberately omits key and value: payloads are untrusted input and do not belong in log lines
        return "OutboundRecord(" + topic + ")";
    }
}
