package io.confluent.parallelconsumer;

import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.ThreadUtils.sleepQueietly;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
public class OffsetEncodingBackPressureTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * Tests that when required space for encoding offset becomes too large, back pressure is put into the system so
     * that no further messages for the given partitions can be taken for processing, until more messages complete.
     */
    @SneakyThrows
    @Test
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
//        final int numRecords = 1_000_0;
        final int numRecords = 1_00;
        parallelConsumer.setLongPollTimeout(ofMillis(200));
//        parallelConsumer.setTimeBetweenCommits();

        OffsetMapCodecManager.DefaultMaxMetadataSize = 40; // reduce available to make testing easier
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
                sleepQueietly(1);
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
        int targetMessagesToProcessBeforeBackpressureBlocksPartition = 10;
        waitAtMost(ofSeconds(120))
//                .failFast(() -> parallelConsumer.isClosedOrFailed(), () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
//                    .alias(failureMessage)
                .pollInterval(1, SECONDS)
                .untilAsserted(() -> {
//                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
//                        SoftAssertions all = new SoftAssertions();
//                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
//                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
//                        all.assertAll();
                    assertThat(processedCount.get()).isEqualTo(99L);
                });
//        } catch (ConditionTimeoutException e) {
//            fail(failureMessage + "\n" + e.getMessage());
//        }

        // assert commit ok - nothing blocked
        {
            //
            waitForSomeLoopCycles(1);
            parallelConsumer.requestCommitAsap();
            waitForSomeLoopCycles(1);

            //
            List<OffsetAndMetadata> offsetAndMetadataList = extractAllPartitionsOffsetsAndMetadataSequentially();
            assertThat(offsetAndMetadataList).isNotEmpty();
            OffsetAndMetadata offsetAndMetadata = offsetAndMetadataList.get(offsetAndMetadataList.size() - 1);
            assertThat(offsetAndMetadata.offset()).isEqualTo(0L);

            //
            String metadata = offsetAndMetadata.metadata();
            HighestOffsetAndIncompletes decodedOffsetPayload = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(0, metadata);
            Long highestSeenOffset = decodedOffsetPayload.getHighestSeenOffset();
            Set<Long> incompletes = decodedOffsetPayload.getIncompleteOffsets();
            assertThat(incompletes).isNotEmpty().contains(0L).doesNotContain(1L, 50L, 99L);
            assertThat(highestSeenOffset).isEqualTo(99L);
        }

        WorkManager<String, String> wm = parallelConsumer.wm;

        // partition not blocked
        {
            boolean partitionBlocked = !wm.partitionMoreRecordsAllowedToProcess.get(topicPartition);
            assertThat(partitionBlocked).isFalse();
        }

        // feed more messages
        int expectedMsgsProcessedBeforePartitionBlocks = numRecords + numRecords / 4;
        {
            ktu.send(consumerSpy, ktu.generateRecords(numRecords));
            waitForOneLoopCycle();
            assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue(); // sanity
            await().atMost(ofSeconds(5)).untilAsserted(() ->
                    assertThat(processedCount.get()).isGreaterThan(expectedMsgsProcessedBeforePartitionBlocks) // some new message processed
            );
            waitForOneLoopCycle();
        }

        {
            // assert partition blocked
            assertThat(wm.partitionOffsetHighWaterMarks.get(topicPartition))
                    .isGreaterThan(expectedMsgsProcessedBeforePartitionBlocks);
            assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse();

            // assert blocked, but can still write payload
            assertThat(true).isFalse();
        }

        // test max payload exceeded, payload dropped
        {
            // force system to allow more records (i.e. the actual system attempts to never allow the payload to grow this big)
            wm.partitionMoreRecordsAllowedToProcess.put(topicPartition, true);
            // assert payload missing from commit now
            assertThat(true).isFalse();
        }

        // test failed messages can retry
        {
            // fail the message
            // wait for the retry
            // release message that was blocking partition progression
            msgLock.countDown();
            assertThat(true).isFalse();
        }

        {
            // assert partition is now not blocked
            waitForOneLoopCycle();
            await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue());
        }

        {
            // assert all committed, nothing blocked- next expected offset is now 1+ the offset of the final message we sent (numRecords*2)
            int nextExpectedOffsetAfterSubmittedWork = numRecords * 2;
            await().untilAsserted(() -> {
                List<Integer> offsets = extractAllPartitionsOffsetsSequentially();
                assertThat(offsets).contains(nextExpectedOffsetAfterSubmittedWork);
            });
            await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue());
        }
    }

    @Test
    @Disabled
    void failedMessagesThatCanRetryDontDeadlockABlockedPartition() {
    }

}
