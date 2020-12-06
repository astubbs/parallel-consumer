package io.confluent.parallelconsumer;

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.csid.utils.TrimListRepresentation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;

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
        final int numRecords = 1_000_0;

        OffsetMapCodecManager.forcedCodec = OffsetEncoding.BitSetV2; // force one that takes a predictable large amount of space

        //
        int maxConcurrency = 200;
        ParallelConsumerOptions<String, String> build = ParallelConsumerOptions.<String, String>builder()
                .commitMode(TRANSACTIONAL_PRODUCER)
                .maxConcurrency(maxConcurrency)
                .build();
        WorkManager<String, String> wm = new WorkManager<>(build, consumerManager);

        AtomicInteger processedCount = new AtomicInteger(0);
        parallelConsumer.poll((rec) -> {
            // block the partition to create bigger and bigger offset encoding blocks
            if (rec.offset() == 0) {
                try {
                    // force first message to "never" complete, causing a large offset encoding (lots of messages completing above the low water mark
                    Thread.sleep(20 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            processedCount.getAndIncrement();
        });
//
//        // add records
//        {
//            ConsumerRecords<String, String> crs = buildConsumerRecords(numRecords);
//            wm.registerWork(crs);
//        }

        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

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
                    assertThat(processedCount).isEqualTo(numRecords);
                });
//        } catch (ConditionTimeoutException e) {
//            fail(failureMessage + "\n" + e.getMessage());
//        }

    }

}
