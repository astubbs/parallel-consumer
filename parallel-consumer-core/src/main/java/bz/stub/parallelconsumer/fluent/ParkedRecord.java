package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.RecordContext;
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
 * <h2>It is a view, not a copy</h2>
 * The engine holds the record: its bytes and headers, how many attempts it took, what the last failure was, when
 * that was, and - since the throw that parked it said so - why it parked. All of that is read from the engine's own
 * {@link RecordContext} rather than copied here, so the list an operator reads and the record the engine is holding
 * cannot disagree. Two things are added, and only two, because the engine cannot know them: the {@link #key()} as
 * this route's deserialiser reads it, and the {@link #cycles()} the record spent.
 * <p>
 * Holding the record is also what makes R27's small advantage of park in place real: the instance can still resume
 * or export a parked record after the broker's retention has removed it, because the record never left memory. Only
 * a restart loses it, since it can no longer be re-polled.
 *
 * @see ParkObserver
 */
@InterfaceStability.Unstable
public final class ParkedRecord {

    private final RecordContext<byte[], byte[]> engineContext;

    private final Object key;

    private final int cycles;

    ParkedRecord(RecordContext<byte[], byte[]> engineContext, Object key, int cycles) {
        this.engineContext = engineContext;
        this.key = key;
        this.cycles = cycles;
    }

    public String topic() {
        return engineContext.topic();
    }

    public int partition() {
        return engineContext.partition();
    }

    public long offset() {
        return engineContext.offset();
    }

    TopicPartition topicPartition() {
        return new TopicPartition(topic(), partition());
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
        return engineContext.getNumberOfFailedAttempts();
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
     * <p>
     * The engine's last failure is the throw that parked the record, which is the facade's own park exception; the
     * failure a reader wants is the one underneath it.
     */
    public Throwable failure() {
        PCRetriableException park =
                PCRetriableException.handbackIn(engineContext.getLastFailureReason().orElse(null));
        return park == null ? null : park.getCause();
    }

    /**
     * Why it parked, in one phrase: it ran out of attempts, its payload can never be decoded, or the reason the
     * function gave. This is the verdict the engine recorded when the throw said park.
     */
    public String reason() {
        return engineContext.getParkedReason().orElse(null);
    }

    /**
     * When it parked - the moment of the failure that parked it.
     */
    public Instant parkedSince() {
        return engineContext.getLastFailureAt().orElse(Instant.EPOCH);
    }

    /**
     * The record as it arrived, still in bytes - what an export copies, and the only form a record that never
     * decoded has.
     */
    public ConsumerRecord<byte[], byte[]> raw() {
        return engineContext.getConsumerRecord();
    }

    @Override
    public String toString() {
        return "ParkedRecord(" + topic() + "-" + partition() + "@" + offset() + ", attempts=" + attempts()
                + (cycles == 0 ? "" : ", cycles=" + cycles) + ", since=" + parkedSince() + ", " + reason() + ")";
    }
}
