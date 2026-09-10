package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Instant;

/**
 * One record parked in place: what the parked view lists, and what an export would copy (R27, R28).
 * <p>
 * A parked record stays incomplete in the offset map, holds no worker, and is not attempted again until it is
 * resumed, exported, or a restart re-delivers it. This entry is the index into it - everything an operator needs to
 * decide which of those it deserves, without going back to the broker.
 *
 * <h2>Why it holds the raw record</h2>
 * The bytes and headers are what an export copies (R13), and they are the only thing a record parked by a permanent
 * decode failure has at all. Holding them is also what makes R27's small advantage of park in place real: the
 * instance can still resume or export a parked record after the broker's retention has removed it, because the
 * record never left memory. Only a restart loses it, since it can no longer be re-polled.
 *
 * @see ParkObserver
 */
@InterfaceStability.Unstable
public final class ParkedRecord {

    private final ConsumerRecord<byte[], byte[]> raw;

    private final Object key;

    private final int attempts;

    private final int cycles;

    private final Throwable failure;

    private final String reason;

    private final Instant parkedSince;

    ParkedRecord(ConsumerRecord<byte[], byte[]> raw, Object key, int attempts, int cycles, Throwable failure,
                 String reason, Instant parkedSince) {
        this.raw = raw;
        this.key = key;
        this.attempts = attempts;
        this.cycles = cycles;
        this.failure = failure;
        this.reason = reason;
        this.parkedSince = parkedSince;
    }

    public String topic() {
        return raw.topic();
    }

    public int partition() {
        return raw.partition();
    }

    public long offset() {
        return raw.offset();
    }

    TopicPartition topicPartition() {
        return new TopicPartition(raw.topic(), raw.partition());
    }

    /**
     * The key as the route's deserialiser read it, or null when it could not be read - in which case the key bytes
     * are still on {@link #raw()}.
     */
    public Object key() {
        return key;
    }

    /**
     * How many times this route's function ran for this record. Zero for a permanent decode failure, which spends
     * no attempts (R12).
     */
    public int attempts() {
        return attempts;
    }

    /**
     * How many park cycles this record used before it parked for good - the scheduled re-attempts a park policy's
     * delay granted it (R27). Zero unless the policy declared cycles.
     */
    public int cycles() {
        return cycles;
    }

    /**
     * The last failure, or null when the function asked for the park itself with {@link Outcome#park(String)}.
     */
    public Throwable failure() {
        return failure;
    }

    /**
     * Why it parked, in one phrase: it ran out of attempts, its payload can never be decoded, or the reason the
     * function gave.
     */
    public String reason() {
        return reason;
    }

    public Instant parkedSince() {
        return parkedSince;
    }

    /**
     * The record as it arrived, still in bytes - what an export copies, and the only form a record that never
     * decoded has.
     */
    public ConsumerRecord<byte[], byte[]> raw() {
        return raw;
    }

    @Override
    public String toString() {
        return "ParkedRecord(" + topic() + "-" + partition() + "@" + offset() + ", attempts=" + attempts
                + (cycles == 0 ? "" : ", cycles=" + cycles) + ", since=" + parkedSince + ", " + reason + ")";
    }
}
