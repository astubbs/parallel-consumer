package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.DecodedMetadata;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.Rider;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import bz.stub.parallelconsumer.state.PartitionState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSet;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetV2;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetV2Compressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RiderEnvelope;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLength;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthV2;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthV2Compressed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * The rider envelope wired into the codec: what PC writes when an embedder supplies a rider, what it reads back, and -
 * the expensive half to get wrong - that a payload written with <b>no</b> rider is byte for byte what PC writes today.
 * <p>
 * {@link OffsetRiderEnvelopeTest} owns the wire format itself; this class owns the seams either side of it:
 * {@link OffsetMapCodecManager}'s encode entry points, and the unwrap that
 * {@link EncodedOffsetPair#decodeToIncompletes} performs above the pair.
 * <p>
 * <b>The identity assertions are built from {@link OffsetMapCodecManager#encodeOffsetsCompressed}</b>, which this
 * change does not touch, rather than from a hard-coded string. A literal captured today would pin this build's output
 * against itself and pass whatever the encoder later did; deriving the expectation from the untouched encoder makes it
 * fail the moment the rider work perturbs the bytes PC has always written.
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 * @see ForeignOffsetMetadataOnAssignmentTest for the malformed-envelope arms at the rebalance frame
 */
@Slf4j
// Both statics this class writes are the ones the codec's other tests share: forcedCodec and (by reading it)
// DefaultMaxMetadataSize. WRITE on the forced-codec lock, READ on the metadata-size one, exactly as
// OffsetEncodingTests declares them.
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
class OffsetRiderCodecTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    /**
     * The committed offset the payloads are relative to: the NEXT offset to be polled, so a payload carrying no offset
     * map at all reads back as having seen {@code COMMITTED_OFFSET - 1}.
     */
    static final long COMMITTED_OFFSET = 0L;

    static final long HIGHEST_SUCCEEDED = 4L;

    static final byte[] RIDER_BYTES = {1, 2, 3, 4, 5, 6, 7, 8};

    PCModuleTestEnv module;

    OffsetMapCodecManager<String, String> codec;

    PartitionState<String, String> state;

    TreeSet<Long> incompleteOffsets;

    @BeforeEach
    void setup() {
        incompleteOffsets = new TreeSet<>();
        incompleteOffsets.add(0L);
        incompleteOffsets.add(2L);
        incompleteOffsets.add(3L);

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                // its own registry, so the encoder-runs-once assertion counts this test's encodes and nobody else's
                .meterRegistry(new SimpleMeterRegistry())
                .build();
        module = new PCModuleTestEnv(options);
        state = new PartitionState<>(0, module, TP,
                new HighestOffsetAndIncompletes(Optional.of(HIGHEST_SUCCEEDED), incompleteOffsets));
        codec = new OffsetMapCodecManager<>(module);
    }

    @AfterEach
    void clearForcedCodec() {
        OffsetMapCodecManager.forcedCodec = Optional.empty();
        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * R2's core claim, per encoding. The expected string is base64 of the untouched
     * {@link OffsetMapCodecManager#encodeOffsetsCompressed} output - i.e. exactly what the pre-change
     * {@code makeOffsetMetadataPayload} produced, since that method was nothing but those two steps.
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource(OffsetEncoding.class)
    void aPayloadWithNoRiderIsWhatPcWritesToday(OffsetEncoding encoding) {
        assumeThat(encoding)
                .as("Codec skipped, not applicable")
                .isNotIn(OffsetEncoding.ByteArray, OffsetEncoding.ByteArrayCompressed,
                        OffsetEncoding.KafkaStreams, OffsetEncoding.KafkaStreamsV2, RiderEnvelope);

        OffsetSimultaneousEncoder.compressionForced = true;
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);

        String expected = OffsetSimpleSerialisation.base64(codec.encodeOffsetsCompressed(COMMITTED_OFFSET, state));

        assertThat(codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state))
                .as("a commit with no rider must be byte-identical to what this build wrote before the rider existed")
                .isEqualTo(expected);
        byte[] innerBytes = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        assertThat(codec.assembleMetadataPayload(innerBytes, Rider.none()))
                .as("assembling with no rider must go round the envelope entirely")
                .isEqualTo(expected);
    }

    /**
     * The competitive path (no {@code forcedCodec}), which is what production takes.
     */
    @SneakyThrows
    @Test
    void aCompetitivelyEncodedPayloadWithNoRiderIsWhatPcWritesToday() {
        String expected = OffsetSimpleSerialisation.base64(codec.encodeOffsetsCompressed(COMMITTED_OFFSET, state));

        assertThat(codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state)).isEqualTo(expected);
    }

    /**
     * AE2's decode half: a caught-up partition commits a rider and no offset map, and that payload must answer exactly
     * what an empty payload answers - the highest offset seen is the one BELOW the committed offset. One higher would
     * mark the committed record itself as done and lose it.
     */
    @SneakyThrows
    @Test
    void aRiderOnlyPayloadAnswersTheEmptyPayloadsOffsets() {
        long committed = 100L;
        String payload = codec.assembleMetadataPayload(new byte[0], Rider.present(RIDER_BYTES));

        DecodedMetadata decoded = OffsetMapCodecManager.deserialiseMetadataFromBase64(committed, payload,
                InvalidOffsetMetadataHandlingPolicy.FAIL, TP);

        assertThat(decoded.getOffsets().getHighestSeenOffset())
                .as("a rider-only payload must answer the same highest-seen offset as a payload with no map at all")
                .hasValue(committed - 1);
        assertThat(decoded.getOffsets().getIncompleteOffsets()).isEmpty();
        assertThat(decoded.getRider().getState()).isEqualTo(RiderState.PRESENT);
        assertThat(decoded.getRider().getBytes()).isEqualTo(RIDER_BYTES);

        assertThat(OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committed, payload)
                .getHighestSeenOffset())
                .as("the public overload must project down to the same answer")
                .hasValue(committed - 1);
    }

    /**
     * The round trip, on the forced path: the rider and the holes both survive, for every inner encoding this build can
     * produce.
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource(OffsetEncoding.class)
    void aRiderRoundTripsWithEveryInnerEncoding(OffsetEncoding encoding) {
        assumeThat(encoding)
                .as("Codec skipped, not applicable")
                .isNotIn(OffsetEncoding.ByteArray, OffsetEncoding.ByteArrayCompressed,
                        OffsetEncoding.KafkaStreams, OffsetEncoding.KafkaStreamsV2, RiderEnvelope);

        OffsetSimultaneousEncoder.compressionForced = true;
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);

        assertRoundTrip();
    }

    /**
     * The same round trip on the competitive path.
     */
    @SneakyThrows
    @Test
    void aRiderRoundTripsOnTheCompetitivePath() {
        assertRoundTrip();
    }

    @SneakyThrows
    private void assertRoundTrip() {
        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        String payload = codec.assembleMetadataPayload(inner, Rider.present(RIDER_BYTES));

        DecodedMetadata decoded = OffsetMapCodecManager.deserialiseMetadataFromBase64(COMMITTED_OFFSET, payload,
                InvalidOffsetMetadataHandlingPolicy.FAIL, TP);

        assertThat(decoded.getOffsets().getIncompleteOffsets())
                .as("the holes must survive being wrapped in an envelope")
                .containsExactlyElementsOf(incompleteOffsets);
        assertThat(decoded.getRider().getBytes())
                .as("the rider must come back exactly as it was supplied")
                .isEqualTo(RIDER_BYTES);
    }

    /**
     * The dropped marker: a zero-length envelope around a real offset map. The holes decode, and the reader can tell
     * that a rider existed and was shed for size (R6) rather than never having been configured.
     */
    @SneakyThrows
    @Test
    void theDroppedMarkerSurvivesAroundHoles() {
        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        String payload = codec.assembleMetadataPayload(inner, Rider.dropped());

        DecodedMetadata decoded = OffsetMapCodecManager.deserialiseMetadataFromBase64(COMMITTED_OFFSET, payload,
                InvalidOffsetMetadataHandlingPolicy.FAIL, TP);

        assertThat(decoded.getOffsets().getIncompleteOffsets()).containsExactlyElementsOf(incompleteOffsets);
        assertThat(decoded.getRider().getState())
                .as("a dropped rider must not read as one that was never configured")
                .isEqualTo(RiderState.DROPPED);
    }

    /**
     * The structural independence the plan turns on (AE7): the envelope parsed, the inner body did not. Under
     * {@code IGNORE} the offset map is discarded - and the rider, which was read before the body was even looked at,
     * still comes back.
     */
    @SneakyThrows
    @Test
    void anIntactEnvelopeKeepsItsRiderWhenTheInnerBodyIsCorrupt() {
        // a BitSet declaring 32767 bits with an empty body - CorruptOffsetMetadataTest's first fabricated map
        byte[] corruptInner = {BitSet.magicByte, (byte) 0x7F, (byte) 0xFF};
        String payload = codec.assembleMetadataPayload(corruptInner, Rider.present(RIDER_BYTES));

        DecodedMetadata decoded = OffsetMapCodecManager.deserialiseMetadataFromBase64(COMMITTED_OFFSET, payload,
                InvalidOffsetMetadataHandlingPolicy.IGNORE, TP);

        assertThat(decoded.getOffsets().getHighestSeenOffset()).hasValue(COMMITTED_OFFSET - 1);
        assertThat(decoded.getOffsets().getIncompleteOffsets()).isEmpty();
        assertThat(decoded.getRider().getState())
                .as("the rider and the hole map are structurally independent - losing the body must not lose the rider")
                .isEqualTo(RiderState.PRESENT);
        assertThat(decoded.getRider().getBytes()).isEqualTo(RIDER_BYTES);
    }

    /**
     * A payload PC could not read at all reports {@link RiderState#UNREADABLE} - not {@link RiderState#NONE}, which
     * would tell an embedder no rider was ever configured (R6).
     */
    @Test
    void metadataThePolicyDiscardsReportsAnUnreadableRider() throws Exception {
        String notEvenBase64 = "not-valid-base64!!";

        DecodedMetadata decoded = OffsetMapCodecManager.deserialiseMetadataFromBase64(COMMITTED_OFFSET, notEvenBase64,
                InvalidOffsetMetadataHandlingPolicy.IGNORE, TP);

        assertThat(decoded.getRider().getState()).isEqualTo(RiderState.UNREADABLE);
    }

    /**
     * A payload with no envelope at all reports {@link RiderState#NONE} - the other half of the same distinction.
     */
    @SneakyThrows
    @Test
    void aPayloadWithoutAnEnvelopeReportsNoRider() {
        String payload = codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state);

        DecodedMetadata decoded = OffsetMapCodecManager.deserialiseMetadataFromBase64(COMMITTED_OFFSET, payload,
                InvalidOffsetMetadataHandlingPolicy.FAIL, TP);

        assertThat(decoded.getRider().getState()).isEqualTo(RiderState.NONE);
        assertThat(decoded.getOffsets().getIncompleteOffsets()).containsExactlyElementsOf(incompleteOffsets);
    }

    /**
     * The debug renderer has to survive an envelope too: it is reached from log lines on the decode path, so throwing
     * there turns a diagnosis into a second failure.
     */
    @SneakyThrows
    @Test
    void getDecodedStringRendersAnEnvelopePayload() {
        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        byte[] withHoles = OffsetRiderEnvelope.wrap(inner, Rider.present(RIDER_BYTES));
        byte[] riderOnly = OffsetRiderEnvelope.wrap(new byte[0], Rider.present(RIDER_BYTES));

        assertThat(EncodedOffsetPair.unwrap(withHoles).getDecodedString())
                .as("an envelope around an offset map renders the map it carries")
                .isNotEmpty();
        assertThat(EncodedOffsetPair.unwrap(riderOnly).getDecodedString())
                .as("a rider-only payload has no inner encoding to render, so it renders the rider's length")
                .contains(String.valueOf(RIDER_BYTES.length));
    }

    /**
     * The renderer's third shape: an envelope whose inner magic byte this build does not know - the downgrade
     * diagnostic, what an older reader logs about a newer writer's payload. It has to name the byte rather than
     * throw or render the map branch, and until this test the branch had nothing telling it apart from either
     * (the PIT lane reported its conditional as a surviving mutant).
     */
    @Test
    void getDecodedStringNamesAnInnerEncodingItDoesNotKnow() throws Exception {
        byte unknown = OffsetCodecTestUtils.magicByteOfAnEncodingThatDoesNotExistYet();
        byte[] payload = OffsetRiderEnvelope.wrap(new byte[]{unknown, 1, 2, 3}, Rider.present(RIDER_BYTES));

        assertThat(EncodedOffsetPair.unwrap(payload).getDecodedString())
                .contains("does not know")
                .contains(String.valueOf(unknown));
    }

    /**
     * KTD10's floor: the envelope is unwrapped above the pair, so a pair carrying the envelope constant is malformed
     * input rather than something to decode. It must reach the user's policy, never {@code decodeBody}'s
     * {@code PCInternalRuntimeException} default - the escape the policy exists to close.
     */
    @Test
    void aPairBuiltWithTheEnvelopeConstantGoesToThePolicyNotTheDefaultThrow() {
        byte[] envelopeBody = {0, 2, 9, 9}; // what follows the magic byte: a 2-byte rider length and its bytes
        var pair = new EncodedOffsetPair(RiderEnvelope, ByteBuffer.wrap(envelopeBody));

        var ignored = pair.getDecodedIncompletes(COMMITTED_OFFSET, InvalidOffsetMetadataHandlingPolicy.IGNORE, TP);
        assertThat(ignored.getHighestSeenOffset()).hasValue(COMMITTED_OFFSET - 1);
        assertThat(ignored.getIncompleteOffsets()).isEmpty();

        assertThatThrownBy(() -> new EncodedOffsetPair(RiderEnvelope, ByteBuffer.wrap(envelopeBody))
                .getDecodedIncompletes(COMMITTED_OFFSET, InvalidOffsetMetadataHandlingPolicy.FAIL, TP))
                .as("FAIL must see a typed corruption, not an internal error")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * The public decode surface is load-bearing: this class's own javadoc records a {@code NoSuchMethodError} from the
     * last time one of these was replaced rather than added to. The rider travels on a package-private sibling family.
     */
    @Test
    void thePublicDecodeOverloadsKeepTheirSignaturesAndTheRiderFamilyStaysPackagePrivate() throws Exception {
        Class<?> codecClass = OffsetMapCodecManager.class;

        for (Method m : new Method[]{
                codecClass.getMethod("deserialiseIncompleteOffsetMapFromBase64", long.class, String.class),
                codecClass.getMethod("deserialiseIncompleteOffsetMapFromBase64", long.class, String.class,
                        InvalidOffsetMetadataHandlingPolicy.class),
                codecClass.getMethod("deserialiseIncompleteOffsetMapFromBase64", long.class, String.class,
                        InvalidOffsetMetadataHandlingPolicy.class, TopicPartition.class)}) {
            assertThat(m.getReturnType())
                    .as("%s must keep returning HighestOffsetAndIncompletes", m)
                    .isEqualTo(HighestOffsetAndIncompletes.class);
        }

        Method decodeCompressed = codecClass.getDeclaredMethod("decodeCompressedOffsets", long.class, byte[].class,
                InvalidOffsetMetadataHandlingPolicy.class, TopicPartition.class);
        assertThat(decodeCompressed.getReturnType()).isEqualTo(HighestOffsetAndIncompletes.class);

        Method riderFamily = codecClass.getDeclaredMethod("decodeCompressedMetadata", long.class, byte[].class,
                InvalidOffsetMetadataHandlingPolicy.class, TopicPartition.class);
        assertThat(Modifier.isPublic(riderFamily.getModifiers()))
                .as("the rider-carrying decode family is package-private - it is not public API")
                .isFalse();
        assertThat(riderFamily.getReturnType()).isEqualTo(DecodedMetadata.class);
    }

    /**
     * KTD9: one encode per commit, whatever the ladder above later does with the bytes. A second competition would
     * snapshot a later offset map and double-count both encoding meters.
     */
    @SneakyThrows
    @Test
    void theEncoderRunsExactlyOncePerPayload() {
        var timer = module.pcMetrics().getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
        long before = timer.count();

        var ignoredPayload = codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state); // the value is asserted above

        assertThat(timer.count() - before)
                .as("makeOffsetMetadataPayload must run the encoder competition exactly once")
                .isEqualTo(1);

        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        long afterInnerBytes = timer.count();
        var ignoredAssembled = codec.assembleMetadataPayload(inner, Rider.present(RIDER_BYTES)); // asserted above

        assertThat(timer.count())
                .as("assembling a payload must not encode anything - the ladder repacks the same inner bytes")
                .isEqualTo(afterInnerBytes);
    }

    /**
     * The caught-up case: no incomplete offsets at all, so there is no inner encoding to carry and the rider rides
     * alone. {@code tryToEncodeOffsets} makes the same call on the same condition.
     */
    @SneakyThrows
    @Test
    void aCaughtUpPartitionEncodesNoInnerBytes() {
        SortedSet<Long> noIncompletes = new TreeSet<>();
        var caughtUp = new PartitionState<String, String>(0, module, TP,
                new HighestOffsetAndIncompletes(Optional.of(HIGHEST_SUCCEEDED), noIncompletes));

        assertThat(codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, caughtUp))
                .as("a partition with nothing incomplete has no offset map to write")
                .isEmpty();

        String payload = codec.assembleMetadataPayload(new byte[0], Rider.present(RIDER_BYTES));
        assertThat(payload).isNotEmpty();
    }

    /**
     * {@link RiderState#UNREADABLE} is a read-side answer; asking the writer for one is a programming error rather
     * than something to encode.
     */
    @Test
    void assemblingWithAReadSideRiderStateIsRejected() {
        assertThatCode(() -> codec.assembleMetadataPayload(new byte[0], Rider.none()))
                .as("NONE is legal: it means write the inner bytes unchanged")
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> codec.assembleMetadataPayload(new byte[0], Rider.unreadable()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The magic byte is the one thing about this format that can never change, and the enum is where a future
     * encoding would collide with it.
     */
    @Test
    void theEnvelopeConstantClaimsTheEnvelopesMagicByte() {
        assertThat(RiderEnvelope.getMagicByte()).isEqualTo(OffsetRiderEnvelope.MAGIC_BYTE);
        assertThat(OffsetEncoding.maybeDecode(OffsetRiderEnvelope.MAGIC_BYTE)).hasValue(RiderEnvelope);
        assertThat(OffsetCodecTestUtils.magicByteOfAnEncodingThatDoesNotExistYet())
                .as("the unknown-magic-byte fixture must still name a byte no encoding claims")
                .isNotEqualTo(OffsetRiderEnvelope.MAGIC_BYTE);
    }

    /**
     * Named so the compressed encodings above are not silently absent from the round-trip parameterisation: if this
     * build stops producing one of them, the assumption in the round trip would skip it quietly.
     */
    @SneakyThrows
    @Test
    void everyEncodingThisTestParameterisesOverIsActuallyProduced() {
        OffsetSimultaneousEncoder.compressionForced = true;
        var encoder = new OffsetSimultaneousEncoder(COMMITTED_OFFSET, HIGHEST_SUCCEEDED, incompleteOffsets);
        encoder.invoke();

        assertThat(encoder.getEncodingMap().keySet())
                .as("the round-trip parameterisation is only worth anything for encodings this build produces")
                .contains(BitSet, BitSetCompressed, BitSetV2, BitSetV2Compressed,
                        RunLength, RunLengthCompressed, RunLengthV2, RunLengthV2Compressed);
    }
}
