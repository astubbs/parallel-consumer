package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import com.google.common.truth.Truth8;
import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import bz.stub.parallelconsumer.state.ShardManager;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.JavaUtils.getLast;
import static bz.stub.parallelconsumer.internal.utils.JavaUtils.getOnlyOne;
import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepQuietly;
import static bz.stub.parallelconsumer.ManagedTruth.assertTruth;
import static bz.stub.parallelconsumer.ManagedTruth.assertWithMessage;
import static bz.stub.parallelconsumer.state.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Writes to static state to perform test - needs to be run in isolation. However it runs very fast, so it doesn't slow
 * down parallel test suite much.
 * <p>
 * Runs in isolation regardless of the resource lock read/write setting, because actually various tests depend
 * indirectly on the behaviour of the metadata size, even if not so explicitly.
 * <p>
 * See {@link OffsetMapCodecManager#METADATA_DATA_SIZE_RESOURCE_LOCK}
 *
 * @see OffsetMapCodecManager#METADATA_DATA_SIZE_RESOURCE_LOCK
 * @see OffsetEncodingBackPressureUnitTest
 */
@Isolated // messes with static state - breaks other tests running in parallel
@Slf4j
class OffsetEncodingBackPressureTest extends ParallelEoSStreamProcessorTestBase {

    @AfterAll
    static void cleanup() {
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
    }

    /**
     * Tests that when required space for encoding offset becomes too large, back pressure is put into the system so
     * that no further messages for the given partitions can be taken for processing, until more messages complete.
     */
    // todo refactor test to use the new DI system, to manipulate one of the mocks to force test scenario, instead of messing with static state
    @Test
    // needed due to static accessors in parallel tests
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() throws OffsetDecodingError {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
//        final int numRecords = 1_000_0;
        final int numberOfRecordsToPrimeWith = 1_00;
        parallelConsumer.setTimeBetweenCommits(ofSeconds(1));

        // todo - very smelly - store for restoring
        var realMax = OffsetMapCodecManager.DefaultMaxMetadataSize;

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.DefaultMaxMetadataSize = 40; // reduce available to make testing easier
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        //
        List<ConsumerRecord<String, String>> records = ktu.generateRecords(numberOfRecordsToPrimeWith);
        ktu.send(consumerSpy, records);

        AtomicInteger userFuncFinishedCount = new AtomicInteger();
        AtomicInteger userFuncStartCount = new AtomicInteger();

        CountDownLatch finalMsgLock = new CountDownLatch(1);
        CountDownLatch msgLockTwo = new CountDownLatch(1);
        CountDownLatch msgLockThree = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);
        long offsetToBlock = 0;
        List<Long> blockedOffsets = UniLists.of(0L, 2L);
        final int numberOfBlockedMessages = blockedOffsets.size();

        WorkManager<String, String> wm = parallelConsumer.getWm();
        final PartitionState<String, String> partitionState = wm.getPm().getPartitionState(topicPartition);

        ConcurrentLinkedQueue<Long> seen = new ConcurrentLinkedQueue<>();

        parallelConsumer.poll(recordContext -> {
            log.debug("Processing {}", recordContext.offset());
            seen.add(recordContext.offset());
            userFuncStartCount.incrementAndGet();
            // block the partition to create bigger and bigger offset encoding blocks
            // don't let offset 0 finish
            if (recordContext.offset() == offsetToBlock) {
                int attemptNumber = attempts.incrementAndGet();
                if (attemptNumber == 1) {
                    log.debug("Force first message to 'never' complete, causing a large offset encoding (lots of messages completing above the low water mark. Waiting for msgLock countdown.");
                    int timeout = 120;
                    awaitLatch(finalMsgLock, timeout);
                    log.debug("Very slow message awoken, throwing exception");
                    throw new FakeRuntimeException("Fake error");
                } else {
                    log.debug("Second attempt, waiting for msgLockTwo countdown");
                    awaitLatch(msgLockTwo, 60);
                    log.debug("Second attempt, unlocked, succeeding");
                }
            } else if (recordContext.offset() == 2L) {
                awaitLatch(msgLockThree);
                log.debug("// msg 2L unblocked");
            } else {
                sleepQuietly(1);
            }
            userFuncFinishedCount.incrementAndGet();
        });

        ShardManager<String, String> sm = wm.getSm();

        try {

            // wait for all pre-produced messages to be processed and produced
            waitAtMost(ofSeconds(120))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    .failFast("PC died - check logs", parallelConsumer::isClosedOrFailed)
                    //, () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(userFuncFinishedCount.get()).isEqualTo(numberOfRecordsToPrimeWith - numberOfBlockedMessages);
                    });

            // # assert commit ok - nothing blocked
            {
                //
                awaitForSomeLoopCycles(1);
                parallelConsumer.requestCommitAsap();
                awaitForSomeLoopCycles(1);

                // initial 0 offset is committed with they offset encoded payload
                assertThatConsumer("Initial commit has been executed")
                        .hasCommittedToAnyPartition()
                        .offset(0);
                List<OffsetAndMetadata> offsetAndMetadataList = extractAllPartitionsOffsetsAndMetadataSequentially();
                OffsetAndMetadata mostRecentCommit = getLast(offsetAndMetadataList).get();
                assertThat(mostRecentCommit.offset()).isZero();

                // check offset encoding incomplete payload doesn't contain expected completed messages
                String metadata = mostRecentCommit.metadata();
                HighestOffsetAndIncompletes decodedOffsetPayload = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(0, metadata);
                Long highestSeenOffset = decodedOffsetPayload.getHighestSeenOffset().get();
                Set<Long> incompletes = decodedOffsetPayload.getIncompleteOffsets();
                assertThat(incompletes).isNotEmpty()
                        .contains(offsetToBlock)
                        .doesNotContain(1L, 50L, 99L, (long) numberOfRecordsToPrimeWith - numberOfBlockedMessages); // some sampling of completed offsets, 99 being the highest
                int expectedHighestSeenOffset = numberOfRecordsToPrimeWith - 1;
                assertThat(highestSeenOffset).as("offset 99 is encoded as having been seen").isEqualTo(expectedHighestSeenOffset);
            }


            // partition not blocked
            assertTruth(partitionState).isAllowedMoreRecords();

            //
            log.debug("// feed more messages in order to threshold block - as Bitset requires linearly as much space as we are feeding messages into it, it's guaranteed to block");
            int bytesNeededToCrossThreshold = 5; // roughly
            int extraRecordsToBlockWithThresholdBlocks = Byte.SIZE * bytesNeededToCrossThreshold;
            {
                assertTruth(partitionState).isAllowedMoreRecords(); // should initially be not blocked

                ktu.send(consumerSpy, ktu.generateRecords(extraRecordsToBlockWithThresholdBlocks));
                awaitForOneLoopCycle();

                log.debug("// assert partition now blocked from threshold");
                waitAtMost(ofSeconds(10))
                        .untilAsserted(
                                () -> assertWithMessage("Partition SHOULD be blocked due to back pressure")
                                        .that(partitionState)
                                        .isBlocked()); // blocked

                // Back pressure gates which records may be TAKEN as work, not which are polled and
                // registered - so every extra record IS seen, even though the partition is blocked.
                long lastOffsetSent = numberOfRecordsToPrimeWith + extraRecordsToBlockWithThresholdBlocks - 1;
                waitAtMost(defaultTimeout).untilAsserted(() ->
                        assertWithMessage("every extra record was polled and registered, even while blocked")
                                .that(partitionState).getOffsetHighestSeen().isEqualTo(lastOffsetSent));

                // Wait for the succeeded frontier to settle before reading it. Once the partition is
                // blocked, PartitionState#couldBeTakenAsWork refuses every record at or above the
                // highest succeeded offset, so nothing else can complete and only the two records we
                // are deliberately holding remain in flight. That makes this a quiescent state, not
                // a moving one, so the frontier read below cannot race the assertion that uses it.
                await().untilAsserted(() ->
                        assertWithMessage("nothing left in flight but the records this test holds")
                                .that(wm).getNumberRecordsOutForProcessing().isEqualTo(numberOfBlockedMessages));

                // The encoded high-water mark is the highest SUCCEEDED offset, not the highest polled
                // one - see `use offsetHighestSucceeded instead of offsetHighestSeen` in PartitionState.
                // Back pressure therefore freezes that frontier wherever the encoding crossed the size
                // threshold, which makes the last polled offset unreachable here rather than late.
                // Asserting it only passed when the control loop happened to claim the whole extra batch
                // as work before the block fired. Diagnosis, measurements and control arms:
                // docs/solutions/test-flakiness/back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md
                long settledHighestSucceeded = partitionState.getOffsetHighestSucceeded();
                assertThat(settledHighestSucceeded)
                        .as("the succeeded frontier advanced into the extra batch, and no further than it")
                        .isBetween((long) numberOfRecordsToPrimeWith, lastOffsetSent);

                parallelConsumer.requestCommitAsap();
                awaitForOneLoopCycle();

                log.debug("// assert blocked, but can still write payload");
                // assert the committed offset metadata contains a payload
                waitAtMost(defaultTimeout).untilAsserted(() ->
                        {
                            OffsetAndMetadata partitionCommit = getLastCommit();
                            //
                            assertThat(partitionCommit.offset()).isZero();
                            //
                            String meta = partitionCommit.metadata();
                            HighestOffsetAndIncompletes incompletes = OffsetMapCodecManager
                                    .deserialiseIncompleteOffsetMapFromBase64(0L, meta);
                            Truth.assertWithMessage("The only incomplete record now is offset zero, which we are blocked on")
                                    .that(incompletes.getIncompleteOffsets()).containsExactlyElementsIn(blockedOffsets);
                            Truth8.assertThat(incompletes.getHighestSeenOffset()).hasValue(settledHighestSucceeded);
                        }
                );
            }

            // recreates the situation where the payload size is too large and must be dropped
            log.debug("// test max payload exceeded, payload dropped");
            {
                log.debug("Force system to allow more records to be processed beyond the safety threshold setting " +
                        "(i.e. the actual system attempts to never allow the payload to grow this big) " +
                        "i.e. effectively this disables blocking mechanism for the partition");
                PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(30);
                OffsetMapCodecManager.DefaultMaxMetadataSize = 30; // reduce max cut off size - could use DI mock instead to change method return value?

                //
                log.debug("// unlock record to make the state dirty to get a commit");

                msgLockThree.countDown();

                parallelConsumer.requestCommitAsap();
                awaitForSomeLoopCycles(2);


                assertTruth(partitionState).isBlocked();


                log.debug("// assert payload missing from commit now");
                await().untilAsserted(() -> {
                    assertTruth(partitionState).isBlocked();
                    OffsetAndMetadata partitionCommit = getLastCommit();
                    assertTruth(partitionCommit).hasOffsetEqualTo(0l);
                    assertTruth(partitionCommit).getMetadata().isEmpty();
                });
            }

            log.debug("Test that failed messages can retry, causing partition to un-block");
            {
                // release message that was blocking partition progression
                // fail the message
                finalMsgLock.countDown();

                // wait for the retry - the attempt count IS the retry-happened event, so wait on it
                // directly rather than first sleeping out the retry delay and hoping
                awaitForOneLoopCycle();
                await().atMost(ofSeconds(30)).until(() -> attempts.get() >= 2);

                // assert partition still blocked
                awaitForOneLoopCycle();
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isFalse());

                // release the message for the second time, allowing it to succeed
                msgLockTwo.countDown();
            }

            // assert partition is now not blocked
            {
                awaitForOneLoopCycle();
                await().untilAsserted(() -> assertTruth(partitionState).isAllowedMoreRecords());
            }

            // assert all committed, nothing blocked- next expected offset is now 1+ the offset of the final message we sent
            {
                await().untilAsserted(() -> {
                    List<Integer> offsets = extractAllPartitionsOffsetsSequentially(false);
                    assertThat(offsets).contains(userFuncFinishedCount.get());
                });
                await().untilAsserted(() -> assertTruth(partitionState).isAllowedMoreRecords());
            }
        } finally {
            // todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
            OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient
            OffsetMapCodecManager.forcedCodec = Optional.empty();
        }


    }

    private OffsetAndMetadata getLastCommit() {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = getCommitHistory();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> lastCommit = getLast(commitHistory).get();
        Map<TopicPartition, OffsetAndMetadata> allPartitionCommits = getOnlyOne(lastCommit).get();
        return allPartitionCommits.get(topicPartition);
    }

}
