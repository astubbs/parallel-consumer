package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.google.common.truth.Truth.assertThat;

/**
 * The facade's own attempt count: what the retry limit is measured against (KTD4, R10).
 *
 * @see RetryAndParkTest for the same count seen through a running instance, cross-checked against the engine's
 */
class AttemptLedgerTest {

    private static final String TOPIC = "orders";

    private static ConsumerRecord<byte[], byte[]> record(int partition, long offset) {
        return new ConsumerRecord<>(TOPIC, partition, offset, new byte[0], new byte[0]);
    }

    @Test
    void theFirstRunOfTheFunctionIsAttemptOne() {
        var ledger = new AttemptLedger();

        assertThat(ledger.advance(record(0, 5))).isEqualTo(1);
        assertThat(ledger.advance(record(0, 5))).isEqualTo(2);
        assertThat(ledger.attempts(TOPIC, 0, 5)).isEqualTo(2);
    }

    @Test
    void aRecordNobodyHasRunHasNoAttempts() {
        assertThat(new AttemptLedger().attempts(TOPIC, 0, 5)).isEqualTo(0);
    }

    /**
     * Two partitions of one topic carry the same offset numbers, so a ledger keyed on offset alone would count one
     * record's attempts against another's limit.
     */
    @Test
    void twoPartitionsSharingAnOffsetNumberAreCountedApart() {
        var ledger = new AttemptLedger();

        ledger.advance(record(0, 7));
        ledger.advance(record(0, 7));
        ledger.advance(record(1, 7));

        assertThat(ledger.attempts(TOPIC, 0, 7)).isEqualTo(2);
        assertThat(ledger.attempts(TOPIC, 1, 7)).isEqualTo(1);
    }

    @Test
    void aRecordThatReachesATerminalOutcomeIsForgotten() {
        var ledger = new AttemptLedger();
        ledger.advance(record(0, 5));

        ledger.forget(record(0, 5));

        assertThat(ledger.attempts(TOPIC, 0, 5)).isEqualTo(0);
        assertThat(ledger.size()).isEqualTo(0);
    }

    /**
     * R10's per-assignment rule: a partition that is no longer ours takes its counts with it, so a record
     * reassigned before it was exhausted starts again.
     */
    @Test
    void revokingAPartitionForgetsEveryRecordOnItAndLeavesTheOthersAlone() {
        var ledger = new AttemptLedger();
        ledger.advance(record(0, 1));
        ledger.advance(record(0, 2));
        ledger.advance(record(1, 1));

        ledger.forget(Collections.singletonList(new TopicPartition(TOPIC, 0)));

        assertThat(ledger.attempts(TOPIC, 0, 1)).isEqualTo(0);
        assertThat(ledger.attempts(TOPIC, 0, 2)).isEqualTo(0);
        assertThat(ledger.attempts(TOPIC, 1, 1)).isEqualTo(1);
        assertThat(ledger.tracks(new TopicPartition(TOPIC, 0))).isFalse();
    }

    /**
     * The clear runs on the poll thread inside a rebalance callback, so it must not scan the records: whatever a
     * partition holds, revoking it is one removal.
     */
    @Test
    void revokingIsOneRemovalPerPartitionWhateverThePartitionHolds() {
        var ledger = new AttemptLedger();
        for (int offset = 0; offset < 10_000; offset++) {
            ledger.advance(record(0, offset));
        }
        assertThat(ledger.size()).isEqualTo(10_000);

        ledger.forget(Arrays.asList(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)));

        assertThat(ledger.size()).isEqualTo(0);
    }

    @Test
    void revokingAPartitionNobodyEverCountedIsNotAnError() {
        var ledger = new AttemptLedger();

        ledger.forget(Collections.singletonList(new TopicPartition(TOPIC, 3)));

        assertThat(ledger.size()).isEqualTo(0);
    }
}
