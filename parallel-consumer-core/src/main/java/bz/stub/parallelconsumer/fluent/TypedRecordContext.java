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
 * <p>
 * <b>Why the name says typed.</b> The obvious name is {@code RecordContext}, and it is taken - by the very class
 * this delegates to, which is on the engine and is part of the classic API. The two could not share a name even if
 * that one moved out of the way: under the byte-typed engine its {@code key()} returns {@code byte[]} while this
 * one's returns the route's own {@code K}, so a type that both delegated and overrode would be two methods
 * differing only in return type. So the name says what this adds rather than what it is a context of, and keeps
 * the {@code *Context} family. It was {@code ProcessContext} until 2026-09-11, which named the thing it is handed
 * to rather than the thing it carries; {@link ProcessFunction} keeps that word, correctly, for the same reason.
 *
 * @param <K> the route's consumed key type
 * @param <V> the route's consumed value type
 */
@InterfaceStability.Unstable
public final class TypedRecordContext<K, V> {

    /**
     * The engine's own record, delegated to rather than copied. Every question but the decoded key and value is
     * answered from here, so each has one answer rather than a snapshot that can drift from the engine's.
     */
    private final RecordContext<byte[], byte[]> recordContext;

    /**
     * The key as this route's deserialiser read it. Held because the engine below consumes raw bytes and cannot
     * produce it, and decoded once before dispatch rather than on every access (KTD2).
     */
    private final K key;

    /**
     * The value as this route's deserialiser read it, decoded before dispatch so the function is handed a value
     * rather than the job of decoding one.
     */
    private final V value;

    /**
     * Package-private: a context is only ever minted by dispatch, after this route's formats have decoded the
     * record, so a function cannot be handed types its route did not declare.
     */
    TypedRecordContext(RecordContext<byte[], byte[]> recordContext, K key, V value) {
        this.recordContext = recordContext;
        this.key = key;
        this.value = value;
    }

    /**
     * The decoded key. Null only when this context is being handed to a {@link ParkObserver} for a record that never
     * decoded (R12, R16) - a processing function is never called with one.
     */
    public K key() {
        return key;
    }

    /**
     * The decoded value, with the same one exception as {@link #key()}: null when a permanent decode failure is
     * being reported, in which case the bytes are on {@link #raw()}.
     */
    public V value() {
        return value;
    }

    /**
     * The topic the record arrived on - the one that selected this route, and so the one whose formats decoded it.
     */
    public String topic() {
        return recordContext.topic();
    }

    /**
     * The partition the record arrived on, which under partition ordering is also the unit its ordering is kept in.
     */
    public int partition() {
        return recordContext.partition();
    }

    /**
     * The record's offset, which stays uncommitted until the function reports a terminal outcome for it.
     */
    public long offset() {
        return recordContext.offset();
    }

    /**
     * The record's own timestamp as the broker recorded it, not the moment it was dispatched - a retried record
     * reports the same value on every attempt.
     */
    public long timestamp() {
        return recordContext.timestamp();
    }

    /**
     * The headers exactly as they arrived. The facade types the key and the value and stops there: a header's
     * meaning is the function's own convention, so nothing here decodes one.
     */
    public Headers headers() {
        return recordContext.headers();
    }

    /**
     * How many times this route's function has already failed for this record - so a function can see which attempt
     * it is on without the definition counting for it (R10).
     */
    public int failedAttempts() {
        return recordContext.getNumberOfFailedAttempts();
    }

    /**
     * The record as it arrived, still in bytes.
     */
    public ConsumerRecord<byte[], byte[]> raw() {
        return recordContext.getConsumerRecord();
    }

    /**
     * The engine's own view of this record, for a caller that wants what the classic API's {@code RecordContext}
     * answers - the last failure, when it happened, whether it is parked.
     */
    public RecordContext<byte[], byte[]> recordContext() {
        return recordContext;
    }

    /**
     * Identifies the record and nothing else. A context reaches a log line while the function is running, and the
     * decoded value may be large or may not render at all - printing it is the caller's decision.
     */
    @Override
    public String toString() {
        return "TypedRecordContext(" + topic() + "-" + partition() + "@" + offset() + ")";
    }
}
