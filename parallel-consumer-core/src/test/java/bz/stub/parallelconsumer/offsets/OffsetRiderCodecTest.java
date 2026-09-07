package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import lombok.extern.slf4j.Slf4j;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.DecodedMetadata;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.Rider;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import bz.stub.parallelconsumer.state.PartitionState;
import lombok.SneakyThrows;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import pl.tlinkowski.unij.api.UniSets;
import pl.tlinkowski.unij.api.UniLists;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.moduleWithNoSupplier;
import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.stateOver;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSet;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetV2;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetV2Compressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RiderEnvelope;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLength;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthV2;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthV2Compressed;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

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

    /**
     * The encodings the rider round trip does not parameterise over - encodings this codec does not currently
     * produce, plus the envelope constant itself, which can never appear as an inner encoding.
     */
    static final Set<OffsetEncoding> ENCODINGS_NOT_APPLICABLE_TO_THE_RIDER_ROUND_TRIP = UniSets.of(
            OffsetEncoding.ByteArray, OffsetEncoding.ByteArrayCompressed,
            OffsetEncoding.KafkaStreams, OffsetEncoding.KafkaStreamsV2, RiderEnvelope);

    PCModuleTestEnv module;

    OffsetMapCodecManager<String, String> codec;

    PartitionState<String, String> state;

    TreeSet<Long> incompleteOffsets;

    @BeforeEach
    void setup() {
        incompleteOffsets = new TreeSet<>(UniLists.of(0L, 2L, 3L));
        module = moduleWithNoSupplier();
        state = stateOver(module, TP, HIGHEST_SUCCEEDED, incompleteOffsets);
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
        assumeFalse(ENCODINGS_NOT_APPLICABLE_TO_THE_RIDER_ROUND_TRIP.contains(encoding),
                "Codec skipped, not applicable");

        OffsetSimultaneousEncoder.compressionForced = true;
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);

        String expected = OffsetSimpleSerialisation.base64(codec.encodeOffsetsCompressed(COMMITTED_OFFSET, state));

        assertWithMessage("a commit with no rider must be byte-identical to what this build wrote before the rider existed")
                .that(codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state))
                .isEqualTo(expected);
        byte[] innerBytes = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        assertWithMessage("assembling with no rider must go round the envelope entirely")
                .that(codec.assembleMetadataPayload(innerBytes, Rider.none()))
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

        assertWithMessage("a rider-only payload must answer the same highest-seen offset as a payload with no map at all")
                .that(decoded.getOffsets().getHighestSeenOffset())
                .hasValue(committed - 1);
        assertThat(decoded.getOffsets().getIncompleteOffsets()).isEmpty();
        assertThat(decoded.getRider().getState()).isEqualTo(RiderState.PRESENT);
        assertThat(decoded.getRider().getBytes()).isEqualTo(RIDER_BYTES);

        assertWithMessage("the public overload must project down to the same answer")
                .that(OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committed, payload)
                        .getHighestSeenOffset())
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
        assumeFalse(ENCODINGS_NOT_APPLICABLE_TO_THE_RIDER_ROUND_TRIP.contains(encoding),
                "Codec skipped, not applicable");

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

        assertWithMessage("the holes must survive being wrapped in an envelope")
                .that(decoded.getOffsets().getIncompleteOffsets())
                .containsExactlyElementsIn(incompleteOffsets)
                .inOrder();
        assertWithMessage("the rider must come back exactly as it was supplied")
                .that(decoded.getRider().getBytes())
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

        assertThat(decoded.getOffsets().getIncompleteOffsets()).containsExactlyElementsIn(incompleteOffsets).inOrder();
        assertWithMessage("a dropped rider must not read as one that was never configured")
                .that(decoded.getRider().getState())
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
        assertWithMessage("the rider and the hole map are structurally independent - losing the body must not lose the rider")
                .that(decoded.getRider().getState())
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
        assertThat(decoded.getOffsets().getIncompleteOffsets()).containsExactlyElementsIn(incompleteOffsets).inOrder();
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

        assertWithMessage("an envelope around an offset map renders the map it carries")
                .that(EncodedOffsetPair.unwrap(withHoles).getDecodedString())
                .isNotEmpty();
        assertWithMessage("a rider-only payload has no inner encoding to render, so it renders the rider's length")
                .that(EncodedOffsetPair.unwrap(riderOnly).getDecodedString())
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

        String decodedString = EncodedOffsetPair.unwrap(payload).getDecodedString();
        assertThat(decodedString).contains("does not know");
        assertThat(decodedString).contains(String.valueOf(unknown));
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

        assertThrows(CorruptOffsetMetadataException.class,
                () -> new EncodedOffsetPair(RiderEnvelope, ByteBuffer.wrap(envelopeBody))
                        .getDecodedIncompletes(COMMITTED_OFFSET, InvalidOffsetMetadataHandlingPolicy.FAIL, TP),
                "FAIL must see a typed corruption, not an internal error");
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
            assertWithMessage("%s must keep returning HighestOffsetAndIncompletes", m)
                    .that(m.getReturnType())
                    .isEqualTo(HighestOffsetAndIncompletes.class);
        }

        Method decodeCompressed = codecClass.getDeclaredMethod("decodeCompressedOffsets", long.class, byte[].class,
                InvalidOffsetMetadataHandlingPolicy.class, TopicPartition.class);
        assertThat(decodeCompressed.getReturnType()).isEqualTo(HighestOffsetAndIncompletes.class);

        Method riderFamily = codecClass.getDeclaredMethod("decodeCompressedMetadata", long.class, byte[].class,
                InvalidOffsetMetadataHandlingPolicy.class, TopicPartition.class);
        assertWithMessage("the rider-carrying decode family is package-private - it is not public API")
                .that(Modifier.isPublic(riderFamily.getModifiers()))
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

        assertWithMessage("makeOffsetMetadataPayload must run the encoder competition exactly once")
                .that(timer.count() - before)
                .isEqualTo(1L);

        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        long afterInnerBytes = timer.count();
        var ignoredAssembled = codec.assembleMetadataPayload(inner, Rider.present(RIDER_BYTES)); // asserted above

        assertWithMessage("assembling a payload must not encode anything - the ladder repacks the same inner bytes")
                .that(timer.count())
                .isEqualTo(afterInnerBytes);
    }

    /**
     * The caught-up decision is the caller's, made once on the read it commits against; this step never re-reads
     * emptiness. So a map with nothing incomplete but succeeded work above the base - the shape a partition has
     * when the last completion lands between {@code tryToEncodeOffsets}' decision and the encode - comes out as a
     * <b>complete</b> map, not as no map: a reader resumes at the base and skips through what succeeded, rather
     * than replaying it. An earlier draft answered "nothing to encode" here, which committed the older offset with
     * no metadata and replayed records this build had recorded as complete (Codex review on astubbs#460).
     */
    @SneakyThrows
    @Test
    void aMapThatEmptiedUnderTheCallerEncodesAsCompleteRatherThanAsNothing() {
        var emptiedUnderTheCaller = stateOver(module, TP, HIGHEST_SUCCEEDED, new TreeSet<>());

        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, emptiedUnderTheCaller);

        assertWithMessage("succeeded work above the base is described, not dropped").that(inner).isNotEmpty();
        var readBack = OffsetMapCodecManager.deserialiseMetadataFromBase64(COMMITTED_OFFSET,
                codec.assembleMetadataPayload(inner, Rider.none()), InvalidOffsetMetadataHandlingPolicy.FAIL, TP);
        assertThat(readBack.getOffsets().getIncompleteOffsets()).isEmpty();
        assertWithMessage("the map says everything through the high-water mark succeeded")
                .that(readBack.getOffsets().getHighestSeenOffset())
                .hasValue(HIGHEST_SUCCEEDED);
    }

    /**
     * {@link RiderState#UNREADABLE} is a read-side answer; asking the writer for one is a programming error rather
     * than something to encode.
     */
    @Test
    void assemblingWithAReadSideRiderStateIsRejected() {
        // NONE is legal: it means write the inner bytes unchanged - this call must not throw
        codec.assembleMetadataPayload(new byte[0], Rider.none());

        assertThrows(IllegalArgumentException.class,
                () -> codec.assembleMetadataPayload(new byte[0], Rider.unreadable()));
    }

    /**
     * The magic byte is the one thing about this format that can never change, and the enum is where a future
     * encoding would collide with it.
     */
    @Test
    void theEnvelopeConstantClaimsTheEnvelopesMagicByte() {
        assertThat(RiderEnvelope.getMagicByte()).isEqualTo(OffsetRiderEnvelope.MAGIC_BYTE);
        assertThat(OffsetEncoding.maybeDecode(OffsetRiderEnvelope.MAGIC_BYTE)).hasValue(RiderEnvelope);
        assertWithMessage("the unknown-magic-byte fixture must still name a byte no encoding claims")
                .that(OffsetCodecTestUtils.magicByteOfAnEncodingThatDoesNotExistYet())
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

        assertWithMessage("the round-trip parameterisation is only worth anything for encodings this build produces")
                .that(encoder.getEncodingMap().keySet())
                .containsAtLeast(BitSet, BitSetCompressed, BitSetV2, BitSetV2Compressed,
                        RunLength, RunLengthCompressed, RunLengthV2, RunLengthV2Compressed);
    }
}
