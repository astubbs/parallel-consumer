package io.confluent.parallelconsumer;

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.csid.utils.TrimListRepresentation;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static io.confluent.csid.utils.ThreadUtils.sleepQueietly;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class OffsetEncodingBackPressure extends ParallelEoSStreamProcessorTestBase {

    /**
     * Tests that when required space for encoding offset becomes too large, back pressure is put into the system so
     * that no further messages for the given partitions can be taken for processing, until more messages complete.
     */
    @Test
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
//        final int numRecords = 1_000_0;
        final int numRecords = 1_00;

        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        //
//        int maxConcurrency = 200;
//        ParallelConsumerOptions<String, String> build = ParallelConsumerOptions.<String, String>builder()
//                .commitMode(TRANSACTIONAL_PRODUCER)
//                .maxConcurrency(maxConcurrency)
//                .build();
//        WorkManager<String, String> wm = new WorkManager<>(build, consumerManager);

        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch msgLock = new CountDownLatch(1);
        parallelConsumer.poll((rec) -> {
            // block the partition to create bigger and bigger offset encoding blocks
            if (rec.offset() == 0) {
                log.debug("force first message to 'never' complete, causing a large offset encoding (lots of messages completing above the low water mark");
                awaitLatch(msgLock, 60);
                log.debug("very slow message awoken");
            } else {
                sleepQueietly(5);
            }
            processedCount.getAndIncrement();
        });
//
//        // add records
//        {
//            ConsumerRecords<String, String> crs = buildConsumerRecords(numRecords);
//            wm.registerWork(crs);
//        }

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
//        var failureMessage = StringUtils.msg("All keys sent to input-topic should be processed and produced, within time (expected: {} commit: {} order: {} max poll: {})",
//                expectedMessageCount, commitMode, order, maxPoll);
//        try {
        waitAtMost(ofSeconds(1200))
                .failFast(() -> parallelConsumer.isClosedOrFailed(), () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
//                    .alias(failureMessage)
                .pollInterval(1, SECONDS)
                .untilAsserted(() -> {
//                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
//                        SoftAssertions all = new SoftAssertions();
//                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
//                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
//                        all.assertAll();
                    assertThat(processedCount.get() - 1).isEqualTo(numRecords);
                });
//        } catch (ConditionTimeoutException e) {
//            fail(failureMessage + "\n" + e.getMessage());
//        }

        // assert commit ok
        assertCommits(of(0));
        WorkManager<String, String> wm = parallelConsumer.wm;
        assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse();

        // feed more messages
        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

        // assert partition blocked
        assertCommits(of(0));
        assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse();

        // finish some messages - release latch
        msgLock.countDown();

        // assert no partitions blocked
        await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse());

        // assert all committed
        await().untilAsserted(() -> assertCommits(of(99)));
    }

    @Test
    void failedMessagesThatCanRetryDontDeadlockABlockedPartition() {
    }

}
