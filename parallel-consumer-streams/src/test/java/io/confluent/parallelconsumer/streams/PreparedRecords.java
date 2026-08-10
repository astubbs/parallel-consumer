package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Builds the {@link PcTaskDispatcher.PreparedRecord} a {@code WorkPreparer} returns, for tests that have no
 * Kafka Streams behind them (astubbs#255, U13).
 * <p>
 * Shared rather than copied into each test class: two of them need it, and a second copy is how the
 * stand-in timestamp drifts between the dispatcher tests and the wake-on-work tests.
 * <p>
 * <b>The record's own timestamp stands in for the extracted one.</b> These tests have no
 * {@code TimestampExtractor} - no topology at all - so there is nothing to extract with. The default record
 * fixture uses offset-as-timestamp, which is what Kafka's own {@code StreamTaskTest} does and gives a
 * monotone sequence for free; tests that care about a specific value set it on the record they register.
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
}
