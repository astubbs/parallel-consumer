package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.RecordContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;

/**
 * One record, decoded by its route's {@link Consumed} types, as the processing function sees it.
 * <p>
 * It is a <b>view over the engine's own {@link RecordContext}</b>, which already answers everything about the record
 * that the engine knows: where it came from, when it arrived, how many times it has failed, and what the last
 * failure was. What this adds is the two things the engine cannot know, because the engine below consumes raw bytes
 * - one function, one route table, no typing at the consumer (KTD2): the {@link #key()} and {@link #value()} as this
 * route's deserialisers read them. Everything else is delegated, so there is one answer to each question rather than
 * a copy that can drift.
 * <p>
 * {@link #raw()} is the undecoded record, which is what an exported record carries and what the park observer
 * receives when decoding itself failed (R13, R16).
 *
 * @param <K> the route's consumed key type
 * @param <V> the route's consumed value type
 */
@InterfaceStability.Unstable
public final class ProcessContext<K, V> {

    private final RecordContext<byte[], byte[]> engineContext;

    private final K key;

    private final V value;

    ProcessContext(RecordContext<byte[], byte[]> engineContext, K key, V value) {
        this.engineContext = engineContext;
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
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

    public long timestamp() {
        return engineContext.timestamp();
    }

    public Headers headers() {
        return engineContext.headers();
    }

    /**
     * How many times this route's function has already failed for this record - so a function can see which attempt
     * it is on without the definition counting for it (R10).
     */
    public int failedAttempts() {
        return engineContext.getNumberOfFailedAttempts();
    }

    /**
     * The record as it arrived, still in bytes.
     */
    public ConsumerRecord<byte[], byte[]> raw() {
        return engineContext.getConsumerRecord();
    }

    /**
     * The engine's own view of this record, for a caller that wants what the classic API's {@code RecordContext}
     * answers - the last failure, when it happened, whether it is parked.
     */
    public RecordContext<byte[], byte[]> engineContext() {
        return engineContext;
    }

    @Override
    public String toString() {
        return "ProcessContext(" + topic() + "-" + partition() + "@" + offset() + ")";
    }
}
