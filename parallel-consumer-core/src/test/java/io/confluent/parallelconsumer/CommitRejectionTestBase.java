package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Shared scenario for the commit-rejection exceptions: Kafka refuses a commit for a reason that
 * means "not now" rather than "this consumer is broken", and PC must neither die nor record the
 * commit as done.
 * <p>
 * Subclasses supply only the exception. Everything else - the mock consumer, the backlog, and the
 * assertions - is identical, because these are one scenario with one variable. Keeping them apart
 * would mean two copies drifting out of step, and a future third rejection reason copying whichever
 * one it found first.
 * <p>
 * The discriminating assertion is that commits keep being <b>attempted</b> after the rejections. If
 * the offsets had been marked clean, {@code collectCommitDataForDirtyPartitions()} would return
 * empty and the mock's {@code commitSync} would never be called again - the counter stalls. A naive
 * "no exception escaped" test passes even with the old swallow-and-return bug, so it would guard
 * nothing.
 */
@Slf4j
@Timeout(120)
abstract class CommitRejectionTestBase {

    private static final int RECORDS = 10;

    /** Commits rejected before the mock consumer starts accepting them. */
    private static final int REJECTED_COMMITS = 3;

    /** The rejection under test. A fresh instance per call - these carry stack traces. */
    protected abstract RuntimeException rejection();

    @Test
    void rejectedCommitIsNotFatalAndIsNotRecordedAsSuccessful() {
        final String topic = getClass().getSimpleName();
        final AtomicInteger commitAttempts = new AtomicInteger();

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (commitAttempts.incrementAndGet() <= REJECTED_COMMITS) {
                    var rejection = rejection();
                    log.info("Mock commit attempt {} - rejecting with {}",
                            commitAttempts.get(), rejection.getClass().getSimpleName());
                    throw rejection;
                }
                super.commitSync(offsets);
            }
        };

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        startOffsets.put(tp, 0L);

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .commitInterval(Duration.ofMillis(200L)) // commit often, so the rejections happen early
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC) // the mode that waits on a commit response
                .build();
        var parallelConsumer = new ParallelEoSStreamProcessor<String, String>(options);
        parallelConsumer.subscribe(of(topic));

        // MockConsumer is not a correct implementation of the Consumer contract - must manually rebalance
        mockConsumer.rebalance(Collections.singletonList(tp));
        parallelConsumer.onPartitionsAssigned(of(tp));
        mockConsumer.updateBeginningOffsets(startOffsets);

        for (int i = 0; i < RECORDS; i++) {
            mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, i, "key", "value-" + i));
        }

        try {
            ConcurrentLinkedQueue<RecordContext<String, String>> processed = new ConcurrentLinkedQueue<>();
            parallelConsumer.poll(recordContexts -> recordContexts.forEach(processed::add));

            // the offsets must still be dirty after a rejection, so commits keep being attempted
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(commitAttempts.get()).isGreaterThan(REJECTED_COMMITS));

            // and the rejection must not be fatal - the backlog still drains
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(processed).hasSize(RECORDS));

            // deferral means the offsets are re-committed later, not lost - so ask the broker side
            // rather than inferring it from the attempt count
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var committed = mockConsumer.committed(Collections.singleton(tp)).get(tp);
                assertThat(committed).isNotNull();
                assertThat(committed.offset()).isEqualTo(RECORDS);
            });

            // the exact property the chaos suite asserts: no instance ends with an unclassified cause
            assertThat(parallelConsumer.getFailureCause()).isNull();
            assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        } finally {
            parallelConsumer.closeDontDrainFirst();
        }
    }
}
