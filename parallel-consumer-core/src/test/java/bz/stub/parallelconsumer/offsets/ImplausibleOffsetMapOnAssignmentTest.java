package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An offset map whose decoded range reaches past the end of the partition claims offsets that do not exist, and must
 * be treated as unreadable rather than decoded.
 * <p>
 * This is the fourth outcome {@link EncodedOffsetPair#getDecodedIncompletes(long,
 * InvalidOffsetMetadataHandlingPolicy, TopicPartition)} used to document as uncoverable - "a payload that decodes
 * cleanly into a wrong-but-plausible map". Nothing <em>in the bytes</em> proves a long run wrong, because a long run
 * of completed offsets is exactly what run-length encoding is for; what proves it wrong is the partition, which
 * cannot hold the offsets the run claims.
 * <p>
 * Left unchecked, one four-byte entry of {@link Integer#MAX_VALUE} moves the highest-seen offset about two billion
 * forward, and {@link PartitionState#isRecordPreviouslyCompleted} then reads every real record in that range as
 * already succeeded - silent non-processing, not replay, from metadata anything sharing the consumer group can write.
 *
 * @author Antony Stubbs
 * @see OffsetRunLength#runLengthDecodeToIncompletes
 */
@Slf4j
class ImplausibleOffsetMapOnAssignmentTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    static final long COMMITTED_OFFSET = 100L;

    /**
     * The partition holds offsets {@code 0..199}, so 199 is the highest offset any honest offset map can name.
     */
    static final long PARTITION_END_OFFSET = 200L;

    /**
     * Comfortably inside the partition, and inside the range the corrupt payload claims - so it is a record that
     * really exists and would really be skipped.
     */
    static final long REAL_RECORD_IN_THE_CLAIMED_RANGE = 150L;

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
     * check and reaches the ceiling on its merits.
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

    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata) {
        return moduleWithCommittedMetadata(metadata, PARTITION_END_OFFSET, UnaryOperator.identity());
    }

    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata, InvalidOffsetMetadataHandlingPolicy policy) {
        return moduleWithCommittedMetadata(metadata, PARTITION_END_OFFSET,
                builder -> builder.invalidOffsetMetadataPolicy(policy));
    }

    /**
     * Builds a module whose consumer has {@code metadata} committed against {@link #TP} and reports {@code endOffset}
     * as that partition's log end offset.
     *
     * @param endOffset the log end offset to report, or a negative number to leave it unprimed - which makes
     *                  {@link MockConsumer#endOffsets} throw, standing in for a broker that will not answer
     */
    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata,
                                                        long endOffset,
                                                        UnaryOperator<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> configure) {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.commitSync(UniMaps.of(TP, new OffsetAndMetadata(COMMITTED_OFFSET, metadata)));
        if (endOffset >= 0) {
            mockConsumer.updateEndOffsets(UniMaps.of(TP, endOffset));
        }

        var options = configure.apply(ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)).build();
        return new PCModuleTestEnv(options);
    }

    private static ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, "key", "value");
    }

    /**
     * The defect, at the frame where it does its damage: the offset map is loaded on assignment, and every record in
     * the range it invented is then skipped without ever reaching the user function.
     */
    @Test
    void aRunPastTheEndOfThePartitionDoesNotMarkRealRecordsAsCompleted() {
        // one incomplete offset (the committed one), then a completed run of about two billion
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, Integer.MAX_VALUE - 1));
        WorkManager<String, String> wm = module.workManager();

        wm.onPartitionsAssigned(UniLists.of(TP));

        PartitionState<String, String> state = wm.getPm().getPartitionState(TP);
        assertThat(state.isRecordPreviouslyCompleted(recordAt(REAL_RECORD_IN_THE_CLAIMED_RANGE)))
                .as("a record the partition really holds must not be treated as completed because a payload claimed " +
                        "a run reaching past the end of the partition")
                .isFalse();
        assertThat(state.getOffsetHighestSucceeded())
                .as("the metadata is unreadable, so it is discarded and we resume from the committed offset")
                .isEqualTo(COMMITTED_OFFSET - 1);
    }

    /**
     * The other half of the policy contract: {@code FAIL} exists so a deployment can refuse to carry on with an offset
     * map this build cannot believe, rather than silently discarding it.
     */
    @Test
    void failPolicyRejectsARunPastTheEndOfThePartition() {
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, Integer.MAX_VALUE - 1),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("FAIL must not silently accept an offset map claiming offsets the partition cannot hold")
                .isInstanceOf(CorruptOffsetMetadataException.class)
                .hasMessageContaining(TP.toString());
    }

    /**
     * A run of <em>incomplete</em> offsets is the same lie with a different cost: the decoder used to walk it one
     * offset at a time into a {@link java.util.TreeSet}, so a five-byte payload asked for two billion boxed longs. The
     * guard has to fire before the loop, not after it.
     */
    @Test
    void anIncompleteRunPastTheEndOfThePartitionIsRejectedBeforeItIsWalked() {
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(Integer.MAX_VALUE),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("an incomplete run past the end of the partition must be rejected rather than materialised")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * The guard must not reject a real offset map, and a long run of completed offsets is a real shape: one record
     * stuck at the committed offset while every offset above it succeeds is exactly what run-length encoding is for,
     * and PC's back-pressure does not stop it - the payload stays three entries wide however far the partition runs
     * ahead.
     * <p>
     * Pinned at the boundary: the run ends on the last offset the partition holds, which is the largest map that is
     * still honest.
     */
    @Test
    void aLongRunThatStaysInsideThePartitionIsDecodedNormally() {
        long partitionEndOffset = 1_000_000_000L;
        long highestOffsetHeld = partitionEndOffset - 1;
        int completedRun = (int) (highestOffsetHeld - COMMITTED_OFFSET);
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, completedRun),
                partitionEndOffset,
                UnaryOperator.identity());
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("a run ending on the last offset the partition holds is honest, and must decode")
                .doesNotThrowAnyException();

        PartitionState<String, String> state = wm.getPm().getPartitionState(TP);
        assertThat(state.getOffsetHighestSucceeded())
                .as("the whole run was accepted, right up to the last offset the partition holds")
                .isEqualTo(highestOffsetHeld);
        assertThat(state.isRecordPreviouslyCompleted(recordAt(COMMITTED_OFFSET)))
                .as("the one incomplete offset in the map is still incomplete")
                .isFalse();
        assertThat(state.isRecordPreviouslyCompleted(recordAt(COMMITTED_OFFSET + 1)))
                .as("everything above it really did succeed - the long run is data, not corruption")
                .isTrue();
    }

    /**
     * One offset further than the partition can hold, and nothing else changed - the control arm for the test above.
     */
    @Test
    void aRunOneOffsetPastTheLastOffsetHeldIsRejected() {
        long partitionEndOffset = 1_000_000_000L;
        int completedRun = (int) (partitionEndOffset - COMMITTED_OFFSET); // one past the last offset held
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, completedRun),
                partitionEndOffset,
                builder -> builder.invalidOffsetMetadataPolicy(InvalidOffsetMetadataHandlingPolicy.FAIL));
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("the first offset the partition does not hold is the first one a map may not claim")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * When the broker will not say where the partition ends, the guard has no ground truth and must not invent one:
     * it stands down and the payload decodes exactly as it did before this check existed.
     * <p>
     * Pinned because it is the behaviour every deployment gets when a leader is unavailable during a rebalance, and
     * because failing <em>closed</em> here would discard honest offset maps on a broker hiccup - a far more likely
     * event than the corrupt payload this guard is for.
     */
    @Test
    void anUnknownEndOffsetLeavesTheDecodeAloneRatherThanRejectingIt() {
        var module = moduleWithCommittedMetadata(runLengthV2Metadata(1, 1_000),
                -1, // end offsets left unprimed: MockConsumer refuses to answer, as a broker that times out would
                builder -> builder.invalidOffsetMetadataPolicy(InvalidOffsetMetadataHandlingPolicy.FAIL));
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("a guard with no ground truth must stand down, not reject")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP).getOffsetHighestSucceeded())
                .as("the payload decoded as it always did")
                .isEqualTo(COMMITTED_OFFSET + 1_000);
    }

    /**
     * The same defect one encoding over, and the reason the ceiling is applied to both decoders rather than only to
     * the one the bug was reported against. A bitset is <em>less</em> exposed - astubbs#207 made its declared length
     * prove itself against bytes that are present, so a full metadata field buys tens of thousands of skipped records
     * rather than two billion - but bounded is not true.
     */
    @Test
    void aBitSetReachingPastTheEndOfThePartitionIsRejected() {
        var module = moduleWithCommittedMetadata(bitSetV2Metadata(300), // 100 + 300 - 1, well past offset 199
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("a bitset claiming offsets the partition does not hold is as unreadable as a run length doing it")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * The bitset control arm, at the boundary: a map covering every offset from the committed one to the last the
     * partition holds is the widest honest bitset there is, and must decode.
     */
    @Test
    void aBitSetEndingOnTheLastOffsetHeldIsDecodedNormally() {
        int bitsToTheEndOfThePartition = (int) (PARTITION_END_OFFSET - COMMITTED_OFFSET);
        var module = moduleWithCommittedMetadata(bitSetV2Metadata(bitsToTheEndOfThePartition),
                InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("a bitset ending exactly on the last offset the partition holds is honest")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP).getOffsetHighestSucceeded())
                .as("the whole declared range was accepted")
                .isEqualTo(PARTITION_END_OFFSET - 1);
    }
}
