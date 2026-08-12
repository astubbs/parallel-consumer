package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * When PC is assigned a partition whose committed offset metadata was written by something that isn't PC, decoding that
 * metadata must not take the whole consumer down.
 * <p>
 * The metadata field of a committed offset is free-form - anything sharing the consumer group (a previous Kafka Streams
 * app, another framework, an operator's tooling) may have written bytes there that PC cannot decode. PC's own recovery
 * path already exists: {@link OffsetMapCodecManager#loadPartitionStateForAssignment} logs and drops an undecodable
 * offset map, falling back to a default partition state. These tests pin that the recovery path is actually reached,
 * rather than the error escaping the rebalance listener.
 *
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/118">astubbs#118</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/326">confluentinc#326</a>
 */
@Slf4j
class ForeignOffsetMetadataOnAssignmentTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    static final long COMMITTED_OFFSET = 100L;

    MockConsumer<String, String> mockConsumer;

    /**
     * Builds a PC module whose consumer already has {@code metadata} committed against {@link #TP}, as if a previous
     * owner of this consumer group had left it behind.
     */
    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata,
                                                        ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy policy) {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.commitSync(UniMaps.of(TP, new OffsetAndMetadata(COMMITTED_OFFSET, metadata)));

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .invalidOffsetMetadataPolicy(policy)
                .build();
        return new PCModuleTestEnv(options);
    }

    /**
     * Base64 of a payload whose leading magic byte matches no {@link OffsetEncoding} - neither one of PC's own codecs
     * nor either of the Kafka Streams magic numbers PC recognises.
     */
    private static String foreignMetadata() {
        return Base64.getEncoder().encodeToString(new byte[]{(byte) 42, 0, 0, 0});
    }

    /**
     * The reported failure: unrecognised metadata is encountered during the rebalance callback, and the resulting error
     * escapes {@code onPartitionsAssigned}, which Kafka turns into a fatal "User rebalance callback throws an error"
     * and PC shuts down.
     */
    @Test
    void unknownMagicByteDoesNotEscapeOnPartitionsAssigned() {
        var module = moduleWithCommittedMetadata(foreignMetadata(),
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("undecodable offset metadata must not escape the rebalance listener")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP))
                .as("partition should still be assigned, with a default (dropped offset map) state")
                .isNotNull();
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
