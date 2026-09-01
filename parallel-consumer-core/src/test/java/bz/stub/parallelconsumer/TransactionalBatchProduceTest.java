package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * End to end cover through the real produce wrapper: {@link ParallelEoSStreamProcessor#pollAndProduceMany} acquires one
 * produce lock per {@link PollContextInternal}, whatever the batch size.
 * <p>
 * When that lock was released once per <em>record</em> instead of once per context, the second record of a batch found
 * its thread holding zero read locks. {@code ProducerManager#ensureProduceStarted} threw, that landed in
 * {@code runUserFunction}'s failure handler, and a record the user function had just processed successfully was marked
 * failed and handed back - so the user function saw it again. Against the buggy code this test reports
 * {@code offset N: ... it was seen 2 times}, alongside one framework
 * {@code "registering WC as failed, returning to mailbox"} ERROR per batch.
 * <p>
 * <b>The record count is load-bearing.</b> Duplicate delivery needs the controller to drain the mailbox after the
 * worker marks the record failed, which is a race - at six records it lost that race three times out of three and the
 * test passed against the bug. The failure is a real defect either way (the spurious ERROR log fires on every batch
 * regardless), but only volume makes the duplicate reliably observable.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.ProduceLockReleaseTest for unit-level cover of the release itself
 */
@Tag("transactions")
@Timeout(60)
@Slf4j
class TransactionalBatchProduceTest extends ParallelEoSStreamProcessorTestBase {

    private static final int RECORD_COUNT = 200;

    @Override
    protected ParallelConsumerOptions<Object, Object> getOptions() {
        return getDefaultOptions()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .ordering(UNORDERED)
                .batchSize(2)
                .build();
    }

    @Test
    void batchedRecordsAreNotReprocessedWhenProducing() {
        var timesSeen = new ConcurrentHashMap<Long, AtomicInteger>();
        var maxBatchSeen = new AtomicInteger();

        parallelConsumer.pollAndProduceMany(context -> {
            var batch = context.getConsumerRecordsFlattened();
            maxBatchSeen.accumulateAndGet(batch.size(), Math::max);
            for (var record : batch) {
                timesSeen.computeIfAbsent(record.offset(), ignore -> new AtomicInteger()).incrementAndGet();
            }
            return UniLists.of(new ProducerRecord<>("output-topic", "key", "value"));
        });

        List<?> sent = ktu.sendRecords(RECORD_COUNT);
        Truth.assertThat(sent).hasSize(RECORD_COUNT);

        await("every record is processed")
                .atMost(ofSeconds(30))
                .untilAsserted(() -> Truth.assertThat(timesSeen.keySet()).hasSize(RECORD_COUNT));

        // hold the assertion true for a window, so a retry that is merely late still fails the test
        await("no record is handed back for retry")
                .during(ofSeconds(2))
                .atMost(ofSeconds(20))
                .untilAsserted(() -> assertEachRecordSeenOnce(timesSeen));

        Truth.assertWithMessage("this only tests anything if real batches of more than one record formed")
                .that(maxBatchSeen.get())
                .isGreaterThan(1);
    }

    private void assertEachRecordSeenOnce(Map<Long, AtomicInteger> timesSeen) {
        for (var entry : timesSeen.entrySet()) {
            Truth.assertWithMessage(
                            "offset %s: the user function succeeded, so it must not be retried - it was seen %s times",
                            entry.getKey(), entry.getValue().get())
                    .that(entry.getValue().get())
                    .isEqualTo(1);
        }
    }
}
