package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;

/**
 * One record, decoded by its route's {@link Consumed} types, as the processing function sees it.
 * <p>
 * The engine below consumes raw bytes - one function, one route table, no typing at the consumer (KTD2) - so this is
 * where a route's types appear. {@link #raw()} is the undecoded record, which is what an exported record carries and
 * what the park observer receives when decoding itself failed (R13, R16).
 *
 * @param <K> the route's consumed key type
 * @param <V> the route's consumed value type
 */
@InterfaceStability.Unstable
public final class ProcessContext<K, V> {

    private final ConsumerRecord<byte[], byte[]> raw;

    private final K key;

    private final V value;

    ProcessContext(ConsumerRecord<byte[], byte[]> raw, K key, V value) {
        this.raw = raw;
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
        return raw.topic();
    }

    public int partition() {
        return raw.partition();
    }

    public long offset() {
        return raw.offset();
    }

    public long timestamp() {
        return raw.timestamp();
    }

    public Headers headers() {
        return raw.headers();
    }

    /**
     * The record as it arrived, still in bytes.
     */
    public ConsumerRecord<byte[], byte[]> raw() {
        return raw;
    }

    @Override
    public String toString() {
        return "ProcessContext(" + topic() + "-" + partition() + "@" + offset() + ")";
    }
}
