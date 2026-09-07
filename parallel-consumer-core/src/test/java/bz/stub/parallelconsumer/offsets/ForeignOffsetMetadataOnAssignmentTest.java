package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Base64;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

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

    MockConsumer<String, String> mockConsumer;

    /**
     * Builds a PC module whose consumer already has {@code metadata} committed against {@link #TP}, as if a previous
     * owner of this consumer group had left it behind.
     */
    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata,
                                                        ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy policy) {
        return moduleWithCommittedMetadata(metadata, builder -> builder.invalidOffsetMetadataPolicy(policy));
    }

    /**
     * The same, but leaving {@link ParallelConsumerOptions#getInvalidOffsetMetadataPolicy()} unset - which is the
     * configuration the astubbs#118 reporter actually ran, and so the one the regression must be pinned under.
     */
    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata) {
        return moduleWithCommittedMetadata(metadata, builder -> builder);
    }

    private PCModuleTestEnv moduleWithCommittedMetadata(String metadata,
                                                        UnaryOperator<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> configure) {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.commitSync(UniMaps.of(TP, new OffsetAndMetadata(COMMITTED_OFFSET, metadata)));

        var options = configure.apply(ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)).build();
        return new PCModuleTestEnv(options);
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
     * Malformed {@link OffsetRiderEnvelope} payloads, at the same frame.
     * <p>
     * The envelope is the one structure in the payload whose <em>length field</em> is read out of bytes a stranger may
     * have written, so each shape here is a way that field or the payload around it can lie. They are pinned at
     * {@code onPartitionsAssigned} rather than at the decoder because the failure that matters is the one that escapes
     * the rebalance callback and takes the consumer down - and because the typed exception must NOT be an
     * {@link OffsetDecodingError}, which {@code loadPartitionStateForAssignment} swallows even under {@code FAIL}.
     *
     * @see OffsetRiderEnvelope#unwrap
     */
    static Stream<Arguments> malformedEnvelopes() {
        byte magic = OffsetRiderEnvelope.MAGIC_BYTE;
        byte unknownInner = magicByteOfAnEncodingThatDoesNotExistYet();
        return Stream.of(
                Arguments.of("rider length claims more bytes than follow",
                        new byte[]{magic, 0, 8, 1, 2},
                        CorruptOffsetMetadataException.class),
                // 0xFFFF read as a signed short is -1; read unsigned it is 65535, and either way it overruns. The
                // signed reading used to be the interesting one: a negative length drives allocation and loops.
                Arguments.of("rider length field is 0xFFFF - negative if read signed",
                        new byte[]{magic, (byte) 0xFF, (byte) 0xFF, 1, 2},
                        CorruptOffsetMetadataException.class),
                Arguments.of("envelope shorter than its own header",
                        new byte[]{magic, 0},
                        CorruptOffsetMetadataException.class),
                Arguments.of("inner body truncated mid length field",
                        new byte[]{magic, 0, 1, 7, OffsetEncoding.BitSetV2.magicByte, 1, 2},
                        CorruptOffsetMetadataException.class),
                // The envelope never nests: accepting one would start an unbounded recursion off metadata PC did not
                // write, which is a stack overflow rather than a policy decision.
                Arguments.of("envelope inside an envelope",
                        new byte[]{magic, 0, 1, 7, magic, 0, 0},
                        CorruptOffsetMetadataException.class),
                // The forward-compatibility case one layer down: the envelope parses, and what it carries is an
                // encoding this build has never heard of.
                Arguments.of("inner magic byte belongs to no encoding this build knows",
                        new byte[]{magic, 0, 1, 7, unknownInner, 0, 0},
                        UnknownOffsetMetadataMagicException.class)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedEnvelopes")
    void malformedEnvelopeDoesNotEscapeOnPartitionsAssignedUnderDefaultPolicy(String name, byte[] payload,
                                                                             Class<?> ignoredExpected) {
        var module = moduleWithCommittedMetadata(Base64.getEncoder().encodeToString(payload));
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("a malformed rider envelope must not escape the rebalance listener under the default policy")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP).getOffsetHighestSeen())
                .as("IGNORE resumes from the committed offset, so the highest seen is the offset below it")
                .isEqualTo(COMMITTED_OFFSET - 1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedEnvelopes")
    void failPolicyStopsOnAMalformedEnvelope(String name, byte[] payload, Class<?> expected) {
        var module = moduleWithCommittedMetadata(Base64.getEncoder().encodeToString(payload),
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("FAIL must stop rather than discard a payload whose envelope it could not read")
                .isInstanceOf(expected)
                .as("an OffsetDecodingError would be swallowed by loadPartitionStateForAssignment, even under FAIL")
                .isNotInstanceOf(OffsetDecodingError.class);
    }

    /**
     * AE6, on one classpath: a payload Parallel Consumer really wrote with a rider envelope, whose <em>leading</em>
     * byte a reader that predates the envelope resolves as unknown.
     * <p>
     * The rewrite is not a shortcut around building an old reader - it is the same operation
     * {@code OffsetRiderUpgradeDowngradeTest} performs against a broker with a plain {@code KafkaConsumer}, because
     * there is no released artifact to run and no second classloader to run it in (KTD11). Pinning it here as well
     * means the unit suite fails if that degradation ever stops being the unknown-magic path, without needing Docker.
     * <p>
     * Distinct from {@link #foreignMetadata()} above, which is four arbitrary bytes behind an unclaimed magic byte:
     * this payload is a well-formed envelope in every byte but the first, so it also proves that nothing downstream
     * peeks past the magic byte and recognises the envelope anyway.
     */
    private static String envelopePayloadReadByAnOlderReader() {
        byte[] envelope = OffsetRiderEnvelope.wrap(
                new byte[]{OffsetEncoding.BitSetV2.magicByte, 0, 2, 0},
                OffsetRiderEnvelope.Rider.present(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        envelope[0] = magicByteOfAnEncodingThatDoesNotExistYet();
        return Base64.getEncoder().encodeToString(envelope);
    }

    @Test
    void anEnvelopeSeenByAReaderThatDoesNotKnowItResumesFromTheCommittedOffsetUnderIgnore() {
        var module = moduleWithCommittedMetadata(envelopePayloadReadByAnOlderReader(),
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE);
        WorkManager<String, String> wm = module.workManager();

        assertThatCode(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("AE6: an old reader must warn and carry on, not take the consumer down")
                .doesNotThrowAnyException();

        assertThat(wm.getPm().getPartitionState(TP).getOffsetHighestSeen())
                .as("IGNORE resumes from the committed offset, so the highest seen is the offset below it")
                .isEqualTo(COMMITTED_OFFSET - 1);
    }

    @Test
    void anEnvelopeSeenByAReaderThatDoesNotKnowItStopsUnderFail() {
        var module = moduleWithCommittedMetadata(envelopePayloadReadByAnOlderReader(),
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
        WorkManager<String, String> wm = module.workManager();

        assertThatThrownBy(() -> wm.onPartitionsAssigned(UniLists.of(TP)))
                .as("AE6: FAIL must stop rather than replay from a payload it cannot read")
                .isInstanceOf(UnknownOffsetMetadataMagicException.class)
                .as("an OffsetDecodingError would be swallowed by loadPartitionStateForAssignment, even under FAIL")
                .isNotInstanceOf(OffsetDecodingError.class);
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
