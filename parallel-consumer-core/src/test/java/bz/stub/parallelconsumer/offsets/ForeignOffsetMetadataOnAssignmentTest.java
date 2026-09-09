package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Base64;

import static bz.stub.parallelconsumer.offsets.OffsetCodecTestUtils.magicByteOfAnEncodingThatDoesNotExistYet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When PC is assigned a partition whose committed offset metadata was written by something that isn't PC, decoding that
 * metadata must not take the whole consumer down.
 * <p>
 * The metadata field of a committed offset is free-form - anything sharing the consumer group (a previous Kafka Streams
 * app, another framework, an operator's tooling), or a newer PC using an encoding this build does not have, may have
 * written bytes there that PC cannot decode. Which way that goes is the user's call, via
 * {@link ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy}: the default {@code IGNORE} discards the metadata
 * and resumes from the committed offset, while {@code FAIL} stops rather than silently replay.
 * <p>
 * These tests pin both halves at the {@code onPartitionsAssigned} frame from the reported trace - that the default does
 * not take the consumer down, and that {@code FAIL} genuinely does.
 *
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/118">astubbs#118</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/326">confluentinc#326</a>
 */
@Slf4j
class ForeignOffsetMetadataOnAssignmentTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    static final long COMMITTED_OFFSET = 100L;

    /**
     * A module whose consumer has {@code metadata} committed against {@link #TP} - the shared setup in
     * {@link OffsetCodecTestUtils}, which owns it because {@code ImplausibleOffsetMapOnFirstBatchTest} needs exactly
     * the same thing. Leaving the policy unset is not incidental here: it is the configuration the astubbs#118
     * reporter actually ran, and so the one the regression must be pinned under.
     */
    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata) {
        return OffsetCodecTestUtils.moduleWithCommittedMetadata(TP, COMMITTED_OFFSET, metadata);
    }

    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata,
                                                        ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy policy) {
        return OffsetCodecTestUtils.moduleWithCommittedMetadata(TP, COMMITTED_OFFSET, metadata, policy);
    }

    /**
     * Base64 of a payload whose leading magic byte matches no {@link OffsetEncoding} - neither one of PC's own codecs
     * nor either of the Kafka Streams magic numbers PC recognises.
     * <p>
     * The byte is derived from the enum, not written here. Hard coding one made this test's subject depend on a
     * coincidence: the day an encoding claims that byte, this stops exercising the unknown-magic path and goes on
     * passing, which is the one outcome a forward-compatibility test must not have.
     */
    private static String foreignMetadata() {
        return Base64.getEncoder().encodeToString(
                new byte[]{magicByteOfAnEncodingThatDoesNotExistYet(), 0, 0, 0});
    }

    /**
     * The reported failure: unrecognised metadata is encountered during the rebalance callback, and the resulting error
     * escapes {@code onPartitionsAssigned}, which Kafka turns into a fatal "User rebalance callback throws an error"
     * and PC shuts down.
     */
    @Test
    void unknownMagicByteDoesNotEscapeOnPartitionsAssignedUnderDefaultPolicy() {
        var module = moduleWithCommittedMetadata(foreignMetadata());
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("undecodable offset metadata must not escape the rebalance listener under the default policy")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP))
                .as("partition should still be assigned, with a default (dropped offset map) state")
                .isNotNull();
    }

    /**
     * The other half of the contract. {@code FAIL} exists so a deployment can refuse to silently discard an offset map
     * - discarding it replays records that completed but were not committed - so it has to actually stop.
     * <p>
     * On master this case was not reachable: undecodable metadata bypassed the policy entirely and was recovered from
     * under {@code FAIL} too, which is the defect astubbs#197's release ledger recorded as item 5.
     */
    @Test
    void failPolicyStopsRatherThanDiscardingForeignMetadata() {
        var module = moduleWithCommittedMetadata(foreignMetadata(),
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("FAIL must not silently discard metadata it cannot read")
                .isInstanceOf(UnknownOffsetMetadataMagicException.class);
    }

    /**
     * {@link ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy#IGNORE} is documented as the escape hatch for
     * reusing a consumer group that already has metadata in it. It must cover any foreign metadata, not only the two
     * Kafka Streams magic numbers PC happens to recognise.
     */
    @Test
    void ignorePolicyCoversForeignMetadataNotJustKafkaStreams() {
        var module = moduleWithCommittedMetadata(foreignMetadata(),
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE);
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("IGNORE policy must tolerate foreign metadata from any source")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP))
                .as("partition should still be assigned")
                .isNotNull();
    }

    /**
     * Kafka Streams metadata under the IGNORE policy - the case upstream 0.5.2.6 added the option for. Pinned here at
     * the assignment level (existing coverage only exercises {@link EncodedOffsetPair} directly).
     */
    @Test
    void kafkaStreamsMetadataUnderIgnorePolicyDoesNotEscapeOnPartitionsAssigned() {
        var ksMetadata = Base64.getEncoder().encodeToString(new byte[]{(byte) 1, 0, 0, 0, 0, 0, 0, 0, 0});
        var module = moduleWithCommittedMetadata(ksMetadata,
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE);
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("IGNORE policy must tolerate Kafka Streams metadata")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP))
                .as("partition should still be assigned")
                .isNotNull();
    }
}
