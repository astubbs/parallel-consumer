package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * The record fixture, and the {@link PcTaskDispatcher.PreparedRecord} a {@code WorkPreparer} returns, for
 * tests that have no Kafka Streams behind them (astubbs#255, U13).
 * <p>
 * <b>Both halves live here because only both halves prevent the drift.</b> {@link #prepared} takes the
 * stream timestamp from the record, so sharing it while each test class built its own records left the
 * timestamp free to differ - and it did: one class had moved to offset-as-timestamp while the other still
 * used the short {@link ConsumerRecord} constructor, which leaves {@code timestamp} at
 * {@link ConsumerRecord#NO_TIMESTAMP}. A helper that shares the derivation but not its input shares nothing.
 * <p>
 * <b>The record's own timestamp stands in for the extracted one.</b> These tests have no
 * {@code TimestampExtractor} - no topology at all - so there is nothing to extract with. Records default to
 * offset-as-timestamp, which is what Kafka's own {@code StreamTaskTest} does: it makes each record's
 * timestamp obvious at the call site and gives a monotone sequence for free. Tests that need out-of-order
 * timestamps pass one explicitly.
 *
 * @author Antony Stubbs
 */
final class PreparedRecords {

    private PreparedRecords() {
    }

    static PcTaskDispatcher.PreparedRecord prepared(final ConsumerRecord<byte[], byte[]> record,
                                                    final Runnable chainExecution) {
        return new PcTaskDispatcher.PreparedRecord(chainExecution, record.timestamp());
    }

    /** A record whose timestamp is its offset - the default shape, and the one most tests want. */
    static ConsumerRecord<byte[], byte[]> record(final TopicPartition partition,
                                                 final long offset,
                                                 final String key) {
        return record(partition, offset, offset, key);
    }

    /** A record with its timestamp said out loud, for the stream-time tests that need it out of order. */
    static ConsumerRecord<byte[], byte[]> record(final TopicPartition partition,
                                                 final long offset,
                                                 final long timestamp,
                                                 final String key) {
        return new ConsumerRecord<>(
                partition.topic(),
                partition.partition(),
                offset,
                timestamp,
                TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                key.getBytes(StandardCharsets.UTF_8),
                ("value-" + offset).getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(),
                Optional.empty());
    }
}
