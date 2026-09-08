package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An offset map whose decoded range reaches past the end of the partition claims offsets that do not exist, and is
 * refused the moment the partition can say so - which is the first batch of records, not the assignment.
 * <p>
 * This is the fourth outcome {@link EncodedOffsetPair#getDecodedIncompletes(long,
 * InvalidOffsetMetadataHandlingPolicy, TopicPartition)} documents as one nothing <em>in</em> the payload can settle.
 * A run-length entry of {@link Integer#MAX_VALUE} is structurally perfect and moves the highest-seen offset about two
 * billion forward; {@link PartitionState#isRecordPreviouslyCompleted} then reads every real record in that range as
 * already succeeded, so PC skips them without ever calling the user's function - silent non-processing, not replay.
 * <p>
 * These tests drive the real path: assignment loads the map, then a batch arrives through
 * {@link WorkManager#registerWork}, carrying the log end offset the way the poll loop supplies it from
 * {@code ConsumerManager#logEndOffsetIfKnownWithoutBlocking}.
 *
 * @author Antony Stubbs
 * @see PartitionState#claimsOffsetsThePartitionDoesNotHold
 */
@Slf4j
class ImplausibleOffsetMapOnFirstBatchTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    static final long COMMITTED_OFFSET = 100L;

    /**
     * The partition holds offsets {@code 0..199}, so 199 is the highest offset any honest offset map can name.
     */
    static final long PARTITION_END_OFFSET = 200L;

    /**
     * Comfortably inside the partition, and inside the range a corrupt payload claims - records that really exist and
     * would really be skipped.
     */
    static final long FIRST_RECORD_OF_BATCH = 140L;

    static final long LAST_RECORD_OF_BATCH = 150L;

    static final int RECORDS_IN_BATCH = (int) (LAST_RECORD_OF_BATCH - FIRST_RECORD_OF_BATCH + 1);

    MockConsumer<String, String> mockConsumer;

    /**
     * Base64 of a {@link OffsetEncoding#RunLengthV2} payload with the given runs, the first of which is a run of
     * <em>incomplete</em> offsets (the decoder starts there, because the committed offset is the lowest incomplete
     * one).
     */
    private static String runLengthV2Metadata(int... runs) {
        ByteBuffer body = ByteBuffer.allocate(Byte.BYTES + runs.length * Integer.BYTES);
        body.put(OffsetEncoding.RunLengthV2.magicByte);
        for (int run : runs) {
            body.putInt(run);
        }
        return Base64.getEncoder().encodeToString(body.array());
    }

    /**
     * Base64 of a {@link OffsetEncoding#BitSetV2} payload declaring {@code declaredBits} offsets, every one of them
     * completed, with the body that length actually requires - so it passes astubbs#207's bytes-must-back-the-length
     * check and reaches this check on its merits.
     */
    private static String bitSetV2Metadata(int declaredBits) {
        int bodyBytes = (declaredBits + 7) / 8;
        ByteBuffer body = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + bodyBytes);
        body.put(OffsetEncoding.BitSetV2.magicByte);
        body.putInt(declaredBits);
        for (int i = 0; i < bodyBytes; i++) {
            body.put((byte) 0xFF); // every offset in the range marked completed
        }
        return Base64.getEncoder().encodeToString(body.array());
    }

    /**
     * A run-length map that claims one incomplete offset at the committed one, then {@code completedRun} completed
     * offsets above it - so its highest-seen offset is {@code COMMITTED_OFFSET + completedRun}.
     */
    private static String mapClaimingUpTo(long highestSeenOffset) {
        return runLengthV2Metadata(1, (int) (highestSeenOffset - COMMITTED_OFFSET));
    }

    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata) {
        return moduleWithCommittedMetadata(metadata, UnaryOperator.identity());
    }

    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata, InvalidOffsetMetadataHandlingPolicy policy) {
        return moduleWithCommittedMetadata(metadata, builder -> builder.invalidOffsetMetadataPolicy(policy));
    }

    /**
     * Builds a module whose consumer has {@code metadata} committed against {@link #TP}, as a previous owner of the
     * consumer group would have left it.
     */
    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata,
                                                        UnaryOperator<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> configure) {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.commitSync(UniMaps.of(TP, new OffsetAndMetadata(COMMITTED_OFFSET, metadata)));

        var options = configure.apply(ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)).build();
        return new PCModuleTestEnv(options);
    }

    private static List<ConsumerRecord<String, String>> batchOfRecords(long fromInclusive, long toInclusive) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (long offset = fromInclusive; offset <= toInclusive; offset++) {
            records.add(new ConsumerRecord<>(TP.topic(), TP.partition(), offset, "key-" + offset, "value"));
        }
        return records;
    }

    /**
     * A batch exactly as the poll loop delivers one: records, plus where the fetch said the partition ends.
     *
     * @param logEndOffsetExclusive what {@code ConsumerManager#logEndOffsetIfKnownWithoutBlocking} answered -
     *                              {@link OptionalLong#empty()} when the consumer could not say without blocking
     */
    private static EpochAndRecordsMap<String, String> batch(WorkManager<String, String> wm,
                                                            List<ConsumerRecord<String, String>> records,
                                                            OptionalLong logEndOffsetExclusive) {
        var poll = new ConsumerRecords<>(UniMaps.of(TP, records));
        return new EpochAndRecordsMap<>(poll, wm.getPm(), partition -> logEndOffsetExclusive);
    }

    private static WorkManager<String, String> assignedWorkManager(PCModuleTestEnv module) {
        WorkManager<String, String> wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
        return wm;
    }

    /**
     * The defect, at the frame where it does its damage: every record in the range the map invented is skipped
     * without ever reaching the user's function.
     */
    @Test
    void aMapClaimingOffsetsThePartitionDoesNotHoldIsDiscardedBeforeAnyRecordIsConsulted() {
        // one incomplete offset (the committed one), then a completed run of about two billion
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, Integer.MAX_VALUE - 1));
        WorkManager<String, String> wm = assignedWorkManager(module);

        wm.registerWork(batch(wm, batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH),
                OptionalLong.of(PARTITION_END_OFFSET)));

        PartitionState<String, String> state = wm.getPm().getPartitionState(TP);
        assertThat(state.getNumberOfIncompleteOffsets())
                .as("every record the partition really holds must have become work, rather than being skipped " +
                        "against a map claiming a range reaching past the end of the partition")
                .isEqualTo(RECORDS_IN_BATCH);
        assertThat(state.isRecordPreviouslyCompleted(recordAt(LAST_RECORD_OF_BATCH)))
                .as("nothing in the discarded map may still be answering questions about real records")
                .isFalse();
    }

    /**
     * The other half of the policy contract: {@code FAIL} exists so a deployment can refuse to carry on with an
     * offset map this build cannot believe, rather than silently discarding it.
     */
    @Test
    void failPolicyRejectsAMapClaimingOffsetsThePartitionDoesNotHold() {
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, Integer.MAX_VALUE - 1),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        assertThatThrownBy(() -> wm.registerWork(batch(wm,
                batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH),
                OptionalLong.of(PARTITION_END_OFFSET))))
                .as("FAIL must not silently accept an offset map claiming offsets the partition cannot hold")
                .isInstanceOf(CorruptOffsetMetadataException.class)
                .hasMessageContaining(TP.toString());
    }

    /**
     * The guard must not reject a real offset map, and a long run of completed offsets is a real shape: one record
     * stuck at the committed offset while every offset above it succeeds is exactly what run-length encoding is for,
     * and PC's back-pressure does not stop it - the payload stays three entries wide however far the partition runs
     * ahead.
     * <p>
     * Pinned at the boundary: the map ends on the last offset the partition holds, the largest claim that is still
     * honest.
     */
    @Test
    void aMapEndingOnTheLastOffsetThePartitionHoldsIsKept() {
        long highestOffsetHeld = PARTITION_END_OFFSET - 1;
        var module = moduleWithCommittedMetadata(mapClaimingUpTo(highestOffsetHeld),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        assertThatCode(() -> wm.registerWork(batch(wm,
                batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH),
                OptionalLong.of(PARTITION_END_OFFSET))))
                .as("a map ending on the last offset the partition holds is honest, and must be kept")
                .doesNotThrowAnyException();

        PartitionState<String, String> state = wm.getPm().getPartitionState(TP);
        assertThat(state.getOffsetHighestSucceeded())
                .as("the map was kept, so it still says everything up to the last offset held succeeded")
                .isEqualTo(highestOffsetHeld);
        assertThat(state.isRecordPreviouslyCompleted(recordAt(LAST_RECORD_OF_BATCH)))
                .as("a kept map still answers about the records below its claim - that is the behaviour a false " +
                        "rejection would destroy")
                .isTrue();
    }

    /**
     * One offset further than the partition can hold, and nothing else changed - the control arm for the test above.
     */
    @Test
    void aMapClaimingOneOffsetPastTheLastOneHeldIsRefused() {
        var module = moduleWithCommittedMetadata(mapClaimingUpTo(PARTITION_END_OFFSET),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        assertThatThrownBy(() -> wm.registerWork(batch(wm,
                batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH),
                OptionalLong.of(PARTITION_END_OFFSET))))
                .as("the first offset the partition does not hold is the first one a map may not claim")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * When the consumer cannot say where the partition ends without blocking, the check has no ground truth and must
     * not invent one - and must not give up either: it looks again at the next batch.
     */
    @Test
    void anUnknownLogEndOffsetDefersTheCheckToTheNextBatch() {
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, Integer.MAX_VALUE - 1),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        assertThatCode(() -> wm.registerWork(batch(wm, batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH),
                OptionalLong.empty())))
                .as("a check with no ground truth must stand down, not reject")
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> wm.registerWork(batch(wm,
                batchOfRecords(LAST_RECORD_OF_BATCH + 1, LAST_RECORD_OF_BATCH + 2),
                OptionalLong.of(PARTITION_END_OFFSET))))
                .as("standing down is deferral, not acceptance - the next batch that can answer must still refuse it")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * A record that arrived certainly exists, so a batch reaching the map's claim corroborates it without any
     * watermark at all - and having been corroborated, the claim is never questioned again.
     */
    @Test
    void aBatchReachingTheClaimCorroboratesItWithoutAWatermark() {
        long modestClaim = FIRST_RECORD_OF_BATCH; // inside the batch below, so the records themselves prove it
        var module = moduleWithCommittedMetadata(mapClaimingUpTo(modestClaim),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        wm.registerWork(batch(wm, batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH), OptionalLong.empty()));

        assertThatCode(() -> wm.registerWork(batch(wm,
                batchOfRecords(LAST_RECORD_OF_BATCH + 1, LAST_RECORD_OF_BATCH + 2),
                OptionalLong.of(modestClaim)))) // would refuse the claim, had the records not already proven it
                .as("a claim the records themselves proved is settled, and must not be re-litigated")
                .doesNotThrowAnyException();
    }

    /**
     * The check reads the decoded claim, not the bytes, so it covers every encoding at once - here the bitset, whose
     * declared length astubbs#207 already forces to be backed by bytes that are present. Bounded is not true: a full
     * metadata field still buys tens of thousands of skipped records.
     */
    @Test
    void aBitSetMapClaimingOffsetsThePartitionDoesNotHoldIsRefused() {
        var module = moduleWithCommittedMetadata(bitSetV2Metadata(300), // 100 + 300 - 1, well past offset 199
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        assertThatThrownBy(() -> wm.registerWork(batch(wm,
                batchOfRecords(FIRST_RECORD_OF_BATCH, LAST_RECORD_OF_BATCH),
                OptionalLong.of(PARTITION_END_OFFSET))))
                .as("a bitset claiming offsets the partition does not hold is as unreadable as a run length doing it")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * The bitset control arm at the boundary: a map covering every offset from the committed one to the last the
     * partition holds is the widest honest bitset there is.
     */
    @Test
    void aBitSetEndingOnTheLastOffsetHeldIsKept() {
        int bitsToTheEndOfThePartition = (int) (PARTITION_END_OFFSET - COMMITTED_OFFSET);
        var module = moduleWithCommittedMetadata(bitSetV2Metadata(bitsToTheEndOfThePartition),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = assignedWorkManager(module);

        // This map has no incomplete offsets at all, so PC expects its next record at PARTITION_END_OFFSET - the
        // batch has to start there, or the pre-existing bootstrap truncation resets the state for reasons that have
        // nothing to do with this check.
        assertThatCode(() -> wm.registerWork(batch(wm,
                batchOfRecords(PARTITION_END_OFFSET, PARTITION_END_OFFSET + 10),
                OptionalLong.of(PARTITION_END_OFFSET + 11))))
                .as("a bitset ending exactly on the last offset the partition held is honest")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP).getOffsetHighestSucceeded())
                .as("the whole declared range was kept")
                .isEqualTo(PARTITION_END_OFFSET - 1);
    }

    private static ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, "key", "value");
    }
}
