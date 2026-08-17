package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.xerial.snappy.SnappyOutputStream;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.truth.Truth.assertWithMessage;
import static bz.stub.parallelconsumer.internal.utils.Range.range;
import static bz.stub.parallelconsumer.offsets.OffsetCodecTestUtils.bitmapStringToIncomplete;
import static bz.stub.parallelconsumer.offsets.OffsetCodecTestUtils.incompletesToBitmapString;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

// todo refactor - remove tests which use hard coded state vs dynamic state - #compressionCycle, #selialiseCycle, #runLengthEncoding, #loadCompressedRunLengthRncoding
@Slf4j
@ExtendWith(MockitoExtension.class)
// READ side of the codec's static state. OffsetMapCodecManager.forcedCodec and
// DefaultMaxMetadataSize are mutable statics that OffsetEncodingBackPressureTest,
// OffsetEncodingBackPressureUnitTest and OffsetEncodingTests all write under this lock. This class
// only reads them, but the lock was one-sided: without declaring it here, `<parallel>methods</parallel>`
// let this class run alongside a writer and observe a forced codec - which is what made
// #largeOffsetMap flake, asserting a compressed ~7 bytes and getting 32 bytes of BitSetV2.
// READ mode still lets these tests run concurrently with each other, only excluding the writers.
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
class WorkManagerOffsetMapCodecManagerTest {

    PCModuleTestEnv module;

    WorkManager<String, String> wm;

    OffsetMapCodecManager<String, String> offsetCodecManager;

    TopicPartition tp = new TopicPartition("myTopic", 0);

    /**
     * set pf incomplete offsets in our sample data
     */
    TreeSet<Long> incompleteOffsets = new TreeSet<>(UniSets.of(0L, 2L, 3L));

    /**
     * Committable offset of 0, meaning 1 and 4 are complete and 2 and 3 are incomplete
     * <p>
     * 0X00X
     */
    long finalOffsetForPartition = 0L;

    /**
     * Sample data runs up to a highest seen offset of 4. Where offset 3 and 3 are incomplete.
     */
    long highestSucceeded = 4;

    PartitionState<String, String> state;

    @Mock
    ConsumerRecord<String, String> mockCr;

    @BeforeEach
    void setupMock() {
        injectSucceededWorkAtOffset(highestSucceeded);
    }

    private void injectSucceededWorkAtOffset(long offset) {
        Mockito.doReturn(offset).when(mockCr).offset();
        state.addNewIncompleteRecord(mockCr);
        state.onSuccess(offset); // in this case the highest seen is also the highest succeeded
    }

    /**
     * o = incomplete x = complete
     */
    static List<String> simpleSampleInputsToCompress = UniLists.of(
            "",
            "o",
            "x",
            "ooo",
            "xxx",
            "xox",
            "oxo",
            "xooxo",
            "ooxxoxox",
            "xxxxxxoooooxoxoxoooooxxxxooooo",
            "oooooooooooooooooooooooooooooo",
            "ooooooooooooooxxxxxxxxxxxxxxxx",
            "oxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "xxxxxxoooooxoxoxoooooxxxxoooooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxoxoxooxoxoxoxoxoxoxoxoxoxoxoxo"
    );

    @Getter
    static List<String> inputsToCompress = new ArrayList<>();

    @BeforeEach
    void setup() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .build();
        module = new PCModuleTestEnv(options);
        state = new PartitionState<>(0, module, tp, new OffsetMapCodecManager.HighestOffsetAndIncompletes(of(highestSucceeded), incompleteOffsets));
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        offsetCodecManager = new OffsetMapCodecManager<>(module);
    }

    @BeforeAll
    static void data() {
        String input100 = "xxxxxxoooooxoxoxoooooxxxxoooooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxoxoxooxoxoxoxoxoxoxoxoxoxoxoxo"; //100 chars

        StringBuffer randomInput = generateRandomData(100);
        String inputString = randomInput.toString();

        inputsToCompress.addAll(simpleSampleInputsToCompress);
        inputsToCompress.add(input100);
        inputsToCompress.add(input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100 + input100);
        inputsToCompress.add(inputString);
        inputsToCompress.add(generateRandomData(1000).toString());
        // remove? slow, not needed?
        inputsToCompress.add(generateRandomData(10000).toString());
        inputsToCompress.add(generateRandomData(30000).toString());
    }

    private static StringBuffer generateRandomData(int entries) {
        StringBuffer randomInput = new StringBuffer();
        range(entries).toStream()
                .mapToObj(x -> RandomUtils.nextBoolean())
                .forEach(x -> randomInput.append((x) ? 'x' : 'o'));
        return randomInput;
    }

    /**
     * The exact metadata string PC wrote for {@link #incompleteOffsets} (0, 2, 3 incomplete below a highest
     * succeeded of 4, committed offset 0) before this repo had any outer codec other than Base64.
     * <p>
     * <strong>This constant must never be regenerated.</strong> It is the wire-format pin for every release of PC
     * ever written: a running consumer group holds strings of exactly this shape in its committed metadata, and if a
     * change makes this string stop decoding to (0, 2, 3) then that change has silently made real committed offsets
     * unreadable. A failure here is a compatibility break to be fixed in the production code, not a stale fixture to
     * be refreshed - if you find yourself pasting a new value in, stop.
     * <p>
     * Captured on 2026-08-17 from a real writer run, before the length-competitive outer codec landed - back when the
     * writer was still called {@code serialiseIncompleteOffsetMapToBase64} and Base64 was the only form it could emit.
     * Bare Base64, no sentinel prefix.
     */
    static final String LEGACY_BARE_BASE64_GOLDEN_VECTOR = "bAAFEgA=";

    /**
     * The incompletes ({@code o}) / completes ({@code x}) pattern behind {@link #LEGACY_BARE_BASE64_LARGE_GOLDEN_VECTOR},
     * relative to a committed offset of 0.
     */
    static final String LARGE_GOLDEN_VECTOR_BITMAP =
            "xxxxxxoooooxoxoxoooooxxxxoooooxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxoxoxooxoxoxoxoxoxoxoxoxoxoxoxo";

    /**
     * As {@link #LEGACY_BARE_BASE64_GOLDEN_VECTOR}, but for a larger (17-byte) payload. A pure shorter-of writer
     * would emit the {@code '%'}-prefixed Z85 form for it (23 chars against Base64's 24), but 17 bytes is under the
     * 22-byte Z85 floor, so today's writer still emits bare Base64 here too. Either way the pin is the same: older PC
     * releases wrote this exact bare-Base64 string, and an upgraded instance must still read what its predecessor
     * committed.
     * <p>
     * <strong>Never regenerate.</strong> Same reasoning as above - this is a decode pin, independent of what the
     * writer chooses today.
     */
    static final String LEGACY_BARE_BASE64_LARGE_GOLDEN_VECTOR = "bABkP6jgwf////+/UlVVBQA=";

    /**
     * Characterization: the small golden vector is still what the writer emits, and still a bare Base64 string.
     * <p>
     * Its payload is 5 bytes - far under the 22-byte Z85 floor, so the writer emits Base64 and small payloads stay
     * readable by older PC releases (at 5 bytes the two forms happen to tie at 8 characters anyway).
     */
    @SneakyThrows
    @Test
    void writerStillEmitsTheLegacyBareBase64StringForSmallPayloads() {
        String written = offsetCodecManager.serialiseIncompleteOffsetMapToString(finalOffsetForPartition, state);

        assertThat(written).isEqualTo(LEGACY_BARE_BASE64_GOLDEN_VECTOR);
        assertThat(written).as("no sentinel - a small payload keeps the legacy form").doesNotStartWith("%");
    }

    /**
     * Characterization / R5: a bare Base64 string as written by an older PC release decodes to the incompletes it always
     * did. Both golden vectors, including the larger one the writer now prefers Z85 for.
     */
    @SneakyThrows
    @Test
    void legacyBareBase64GoldenVectorsStillDecode() {
        var small = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(
                finalOffsetForPartition, LEGACY_BARE_BASE64_GOLDEN_VECTOR);
        assertThat(small.getIncompleteOffsets()).containsExactlyElementsOf(incompleteOffsets);
        assertThat(small.getHighestSeenOffset()).isEqualTo(of(highestSucceeded));

        var large = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(
                0L, LEGACY_BARE_BASE64_LARGE_GOLDEN_VECTOR);
        assertThat(large.getIncompleteOffsets())
                .containsExactlyElementsOf(bitmapStringToIncomplete(0L, LARGE_GOLDEN_VECTOR_BITMAP));
        assertThat(large.getHighestSeenOffset()).isEqualTo(of((long) LARGE_GOLDEN_VECTOR_BITMAP.length() - 1));
    }

    /**
     * KTD6's crossover arithmetic and U3's floor, asserted on the writer's actual output at the payload sizes around
     * both.
     * <p>
     * The two formulas are restated here rather than reused from production on purpose: they are the specification the
     * production code is being checked against, so a mutant in
     * {@link OffsetSimpleSerialisation#base64Length(int)} or {@link Z85Codec#encodedLength(int)} must not be able to
     * move both sides of the comparison at once.
     * <p>
     * The writer is NOT a pure shorter-of: below the 22-byte floor
     * ({@link OffsetSimpleSerialisation#Z85_MIN_PAYLOAD_BYTES}) sentinel+Z85 is a character shorter at 1, 4, 7, 13,
     * ... payload bytes, and the writer still emits Base64 - those payloads are nowhere near the metadata cap, and
     * Base64 there keeps old readers working for free. The rows below the floor where Z85 would win are what pin the
     * floor. 22, not 24, is where Z85 wins for good: at 22 payload bytes Base64 needs 32 characters and sentinel+Z85
     * needs 29, and from there Z85 is always strictly shorter. The plan's prose says "from 24 bytes up" in one place
     * and "&gt;= 22 bytes" in another; the formulas settle it at 22, and this test is what the writer is held to.
     */
    @SneakyThrows
    @Test
    void writerPicksTheShorterOuterCodecAtTheCrossover() {
        // payloadBytes, expected base64 chars, expected sentinel+z85 chars
        int[][] cases = {
                {1, 4, 3},    // z85 a char shorter, but under the floor - the writer keeps base64
                {3, 4, 5},    // z85 longer - base64 either way
                {4, 8, 6},    // z85 shorter, under the floor - base64
                {7, 12, 10},  // z85 shorter, under the floor - base64
                {12, 16, 16}, // equal length
                {13, 20, 18}, // z85 shorter, under the floor - base64
                {21, 28, 28}, // equal length; the last size below the floor
                {22, 32, 29}, // the floor: z85 fires, and from here it is always strictly shorter
                {24, 32, 31},
                {64, 88, 81}, // ~8% saved; converges on ~6%
        };

        for (int[] testCase : cases) {
            int payloadBytes = testCase[0];
            int expectedBase64 = testCase[1];
            int expectedSentinelZ85 = testCase[2];

            assertWithMessage("base64 length of %s bytes", payloadBytes)
                    .that(expectedBase64Chars(payloadBytes)).isEqualTo(expectedBase64);
            assertWithMessage("sentinel+z85 length of %s bytes", payloadBytes)
                    .that(expectedSentinelZ85Chars(payloadBytes))
                    .isEqualTo(expectedSentinelZ85);

            byte[] payload = distinctBytes(payloadBytes);
            String chosen = OffsetSimpleSerialisation.encodeShorterOfBase64OrZ85(payload);

            assertChosenOuterCodec("crossover", payloadBytes, chosen);
            boolean z85Expected = payloadBytes >= 22 && expectedSentinelZ85 < expectedBase64;
            assertWithMessage("chosen string length for a %s byte payload", payloadBytes)
                    .that(chosen.length()).isEqualTo(z85Expected ? expectedSentinelZ85 : expectedBase64);
            if (!z85Expected) {
                assertWithMessage("below the floor the chosen string IS the Base64 form (%s bytes)", payloadBytes)
                        .that(chosen).isEqualTo(Base64.getEncoder().encodeToString(payload));
            }
            assertWithMessage("round trip of %s bytes", payloadBytes)
                    .that(OffsetSimpleSerialisation.decodeBase64OrZ85(chosen)).isEqualTo(payload);
        }
    }

    /**
     * Bytes with no structure a compressor or a formula could accidentally agree with - just needs to be deterministic
     * and to exercise the full byte range.
     */
    private static byte[] distinctBytes(int count) {
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = (byte) (i * 37 + 11);
        }
        return bytes;
    }

    /**
     * A commit with no offset map at all: the map is simply absent, which is not an error. The highest offset seen must
     * then be the one below the committed offset.
     *
     * @see OffsetMapCodecManager#decodeCompressedOffsets(long, byte[], ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy)
     */
    @SneakyThrows
    @Test
    void emptyMetadataDecodesToTheNoMapResult() {
        var decoded = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(5L, "");

        assertThat(decoded.getIncompleteOffsets()).isEmpty();
        assertThat(decoded.getHighestSeenOffset()).isEqualTo(of(4L));
    }

    /**
     * Null is read as absent metadata, same as empty.
     * <p>
     * kafka-clients normalises a null metadata argument to {@code ""} in
     * {@link org.apache.kafka.clients.consumer.OffsetAndMetadata}'s canonical constructor (verified against 3.9.2, and
     * every other constructor delegates to it), so PC's own read path cannot produce one. This is here because the
     * method is the entry point for metadata written by anything at all, and a recovery path should not convert a
     * surprise into a {@link NullPointerException}.
     */
    @SneakyThrows
    @Test
    void nullMetadataDecodesToTheNoMapResult() {
        var decoded = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(5L, null);

        assertThat(decoded.getIncompleteOffsets()).isEmpty();
        assertThat(decoded.getHighestSeenOffset()).isEqualTo(of(4L));
    }

    /**
     * Blank metadata keeps the behaviour it has always had: it is not empty, it does not carry the Z85 sentinel, so it
     * goes to the strict Base64 decoder, which rejects a space and raises {@link OffsetDecodingError}. That is the
     * recoverable signal - callers drop the offset map rather than fail, which
     * {@link ForeignOffsetMetadataOnAssignmentTest} pins at the assignment level.
     */
    @Test
    void blankMetadataKeepsItsExistingRecoverableError() {
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(5L, " "))
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    /**
     * R6: a sentinel-prefixed string that is not Z85 must reach the same recovery path as any other foreign metadata -
     * an {@link OffsetDecodingError} the caller drops the offset map for, not a crash and not a silently truncated map.
     */
    @Test
    void sentinelPrefixedGarbageIsARecoverableDecodingError() {
        // '~' is not in the Z85 alphabet at all
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(5L, "%~~~~~"))
                .as("non-alphabet characters")
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(Z85DecodingException.class);

        // valid alphabet, but no encoding produces a length of 5n+1
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(5L, "%abcdef"))
                .as("impossible length")
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(Z85DecodingException.class);
    }

    /**
     * R6: valid Z85 whose payload starts with a magic byte no {@link OffsetEncoding} knows. The string codec is happy;
     * the encoding dispatch is what rejects it, and it must reject it the same way it rejects unknown-magic Base64.
     */
    @Test
    void sentinelPrefixedUnknownMagicByteIsARecoverableDecodingError() {
        String metadata = "%" + Z85Codec.encode(new byte[]{(byte) 42, 0, 0, 0});

        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(5L, metadata))
                .isInstanceOf(OffsetDecodingError.class);
    }

    @SneakyThrows
    @Test
    void serialiseCycle() {
        String serialised = offsetCodecManager.serialiseIncompleteOffsetMapToString(finalOffsetForPartition, state);
        log.info("Size: {}", serialised.length());

        //
        OffsetMapCodecManager.HighestOffsetAndIncompletes highestOffsetAndIncompletes = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(finalOffsetForPartition, serialised);
        Set<Long> deserializedIncompletes = highestOffsetAndIncompletes.getIncompleteOffsets();

        //
        assertThat(deserializedIncompletes.toArray()).containsExactly(incompleteOffsets.toArray());
    }

    /**
     * Even Java _binary_ serialisation has very large overheads.
     */
    @Test
    void javaSerialisationComparison() {
        TreeSet<Long> one = new TreeSet<>(UniSets.of(1L));
        TreeSet<Long> two = new TreeSet<>(UniSets.of(2L));

        String oneS = OffsetSimpleSerialisation.encodeAsJavaObjectStream(one);
        int payloadLength = 5;
        String oneStringPreamble = oneS.substring(0, oneS.length() - payloadLength);
        String twoS = OffsetSimpleSerialisation.encodeAsJavaObjectStream(two);
        String twoStringPreamble = twoS.substring(0, twoS.length() - payloadLength);

        assertThat(oneStringPreamble).isEqualTo(twoStringPreamble);
    }

    @SneakyThrows
    @Test
    void runLengthEncodingCompression() {
        List<String> inputs = UniLists.of(
                "xxxxxxoooooxoxoxoooooxxxxooooo",
                "6x,5o,1x,1o,1x,1o,1x,5o,4x,5o",
                "oooooooooooooooooooooooooooooo",
                "30o",
                "ooooooooooooooxxxxxxxxxxxxxxxx",
                "15o,15x",
                "x",
                "1x",
                "");

        for (var i : inputs) {
            compareCompression(i);
        }
    }

    private byte[] compareCompression(String input) throws IOException {
        log.info("testing input of {}", input);

        byte[] inputBytes = input.getBytes(UTF_8);

        byte[] outg = OffsetSimpleSerialisation.compressGzip(inputBytes);
        byte[] outz = OffsetSimpleSerialisation.compressZstd(inputBytes);
        ByteArrayOutputStream outs = new ByteArrayOutputStream();

        var snap = new SnappyOutputStream(outs);

        snap.write(inputBytes);

        snap.close();

        String g64 = Base64.getEncoder().encodeToString(outg);
        String z64 = Base64.getEncoder().encodeToString(outz);
        String s64 = Base64.getEncoder().encodeToString(outs.toByteArray());

        String raw64 = Base64.getEncoder().encodeToString(inputBytes);

        log.info("g {}", outg.length);
        log.info("z {}", outz.length);
        log.info("s {}", outs.size());

        log.info("64");
        log.info("r {}", raw64.length());
        log.info("g {}", g64.length());
        log.info("z {}", z64.length());
        log.info("s {}", s64.length());

        return outg;
    }

    @Test
    void base64Encoding() {
        // encode
        String originalString = "TEST";
        byte[] stringBytes = originalString.getBytes(UTF_8);
        String base64Bytes = Base64.getEncoder().encodeToString(stringBytes);

        // decode
        byte[] base64DecodedBytes = Base64.getDecoder().decode(base64Bytes);
        assertThat(stringBytes).isEqualTo(base64DecodedBytes);

        // string
        String decodedString = new String(base64DecodedBytes, UTF_8);
        assertThat(originalString).isEqualTo(decodedString);
    }

    @SneakyThrows
    @Test
    void loadCompressedRunLengthEncoding() {
        byte[] bytes = offsetCodecManager.encodeOffsetsCompressed(finalOffsetForPartition, state);
        OffsetMapCodecManager.HighestOffsetAndIncompletes longs = OffsetMapCodecManager.decodeCompressedOffsets(finalOffsetForPartition, bytes);
        assertThat(longs.getIncompleteOffsets().toArray()).containsExactly(incompleteOffsets.toArray());
    }

    @Test
    void decodeOffsetMap() {
        Set<Long> set = bitmapStringToIncomplete(2L, "ooxx");
        assertThat(set).containsExactly(2L, 3L);

        assertThat(bitmapStringToIncomplete(2L, "ooxxoxox")).containsExactly(2L, 3L, 6L, 8L);
        assertThat(bitmapStringToIncomplete(2L, "o")).containsExactly(2L);
        assertThat(bitmapStringToIncomplete(2L, "x")).containsExactly();
        assertThat(bitmapStringToIncomplete(2L, "")).containsExactly();
        assertThat(bitmapStringToIncomplete(2L, "ooo")).containsExactly(2L, 3L, 4L);
        assertThat(bitmapStringToIncomplete(2L, "xxx")).containsExactly();
    }

    @Test
    void binaryArrayConstruction() {
        injectSucceededWorkAtOffset(6);

        String encoding = incompletesToBitmapString(finalOffsetForPartition, state);
        assertThat(encoding).isEqualTo("oxooxx");
    }

    @SneakyThrows
    @Test
    void compressDecompressSanityGzip() {
        final byte[] input = "Lilan".getBytes();
        final var compressedInput = OffsetSimpleSerialisation.compressGzip(input);
        final var decompressedInput = OffsetSimpleSerialisation.decompressGzip(ByteBuffer.wrap(compressedInput));
        assertThat(decompressedInput).isEqualTo(input);
    }

    @SneakyThrows
    @Test
    void compressDecompressWithBase64SanityGzip() {
        byte[] input = "Lilan".getBytes();
        byte[] compressedInput = OffsetSimpleSerialisation.compressGzip(input);
        byte[] b64input = Base64.getEncoder().encode(compressedInput);
        byte[] b64Output = Base64.getDecoder().decode(b64input);
        byte[] decompressedInput = OffsetSimpleSerialisation.decompressGzip(ByteBuffer.wrap(b64Output));
        assertThat(decompressedInput).isEqualTo(input);
    }

    @SneakyThrows
    @Test
    void compressDecompressSanityZstd() {
        byte[] input = "Lilan".getBytes();
        byte[] compressedInput = OffsetSimpleSerialisation.compressZstd(input);
        ByteBuffer decompressedInput = OffsetSimpleSerialisation.decompressZstd(ByteBuffer.wrap(compressedInput));
        assertThat(decompressedInput).isEqualTo(ByteBuffer.wrap(input));
    }

    @SneakyThrows
    @Test
    void largeOffsetMap() {
        injectSucceededWorkAtOffset(200); // force system to have seen a high offset
        byte[] encoded = offsetCodecManager.encodeOffsetsCompressed(0L, state);
        int smallestCompressionObserved = 10;
        assertThat(encoded).as("very small")
                .hasSizeLessThan(smallestCompressionObserved); // arbitrary size expectation based on past observations - expect around 7
    }

    @SneakyThrows
    @Test
    void stringVsByteVsBitSetEncoding() {
        for (var inputString : inputsToCompress) {
            int inputLength = inputString.length();

            var offsets = bitmapStringToIncomplete(finalOffsetForPartition, inputString);

            OffsetSimultaneousEncoder simultaneousEncoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highestSucceeded, offsets).invoke();
            byte[] byteByte = simultaneousEncoder.getEncodingMap().get(ByteArray);
            byte[] bitsBytes = simultaneousEncoder.getEncodingMap().get(BitSet);

//            int compressedBytes = om.compressZstd(byteByte).length;
//            int compressedBits = om.compressZstd(bitsBytes).length;

            byte[] runlengthBytes = simultaneousEncoder.getEncodingMap().get(RunLength);
//            int rlBytesCompressed = om.compressZstd(runlengthBytes).length;

            log.info("in: {}", inputString);
//            log.info("length: {} comp bytes: {} comp bits: {}, uncompressed bits: {}, run length {}, run length compressed: {}", inputLength, compressedBytes, compressedBits, bitsBytes.length, runlengthBytes.length, rlBytesCompressed);
        }
    }

    @SneakyThrows
    @Test
    void deserialiseBitSet() {
        var input = "oxxooooooo";
        long highestSucceeded = input.length() - 1;

        int nextExpectedOffset = 0;
        var incompletes = bitmapStringToIncomplete(nextExpectedOffset, input);
        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(nextExpectedOffset, highestSucceeded, incompletes);
        encoder.invoke();
        byte[] pack = encoder.packSmallest();

        //
        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(pack);
        String deserialisedBitSet = encodedOffsetPair.getDecodedString();
        assertThat(deserialisedBitSet).isEqualTo(input);
    }

    /**
    * Tests for friendly errors when Kafka Streams (as far as we can guess) magic numbers are found in the offset metadata.
    */
    @SneakyThrows
    @Test
    void deserialiseKafkaStreamsV1WithDefaultErrorPolicy() {
        final var input = ByteBuffer.allocate(32);
        // magic number
        input.put((byte) 1);
        // timestamp
        input.putLong(System.currentTimeMillis());

        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(input.array());
        assertThatThrownBy(()->encodedOffsetPair.getDecodedIncompletes(0L))
                .isInstanceOf(KafkaStreamsEncodingNotSupported.class);
    }

    /**
     * Tests for ignoring when InvalidOffsetMetadataHandlingPolicy.IGNORE and Kafka Streams (as far as we can guess) magic numbers are found in the offset metadata.
     */
    @SneakyThrows
    @Test
    void deserialiseKafkaStreamsV1WithIgnoreErrorPolicy() {
        final var input = ByteBuffer.allocate(32);
        // magic number
        input.put((byte) 1);
        // timestamp
        input.putLong(System.currentTimeMillis());

        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(input.array());

        OffsetMapCodecManager.HighestOffsetAndIncompletes longs = encodedOffsetPair.getDecodedIncompletes(100L, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE);

        assertThat(longs.getHighestSeenOffset()).isEqualTo(Optional.of(100L));
        assertThat(longs.getIncompleteOffsets()).isEqualTo(Collections.emptySet());

    }

    /**
     * Tests for friendly errors when Kafka Streams V2 (as far as we can guess) magic numbers are found in the offset metadata.
     */
    @SneakyThrows
    @Test
    void deserialiseKafkaStreamsV2WithDefaultErrorPolicy() {
        final var input = ByteBuffer.allocate(32);
        // magic number
        input.put((byte) 2);
        // timestamp
        input.putLong(System.currentTimeMillis());
        // metadata
        // number of entries
        input.putInt(1);
        // key size
        input.putInt(1);
        // key
        input.put((byte) 'a');
        // value
        input.putLong(1L);

        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(input.array());
        assertThatThrownBy(()->encodedOffsetPair.getDecodedIncompletes(0L))
                .isInstanceOf(KafkaStreamsEncodingNotSupported.class);
    }

    /**
     * Tests for friendly errors when Kafka Streams V2 (as far as we can guess) magic numbers are found in the offset metadata.
     */
    @SneakyThrows
    @Test
    void deserialiseKafkaStreamsV2WithIgnoreErrorPolicy() {
        final var input = ByteBuffer.allocate(32);
        // magic number
        input.put((byte) 2);
        // timestamp
        input.putLong(System.currentTimeMillis());
        // metadata
        // number of entries
        input.putInt(1);
        // key size
        input.putInt(1);
        // key
        input.put((byte) 'a');
        // value
        input.putLong(1L);

        EncodedOffsetPair encodedOffsetPair = EncodedOffsetPair.unwrap(input.array());
        OffsetMapCodecManager.HighestOffsetAndIncompletes longs = encodedOffsetPair.getDecodedIncompletes(100L, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE);

        assertThat(longs.getHighestSeenOffset()).isEqualTo(Optional.of(100L));
        assertThat(longs.getIncompleteOffsets()).isEqualTo(Collections.emptySet());
    }

    /**
     * The decode choke point ({@link OffsetMapCodecManager#decodeCompressedOffsets}) converts unchecked decode
     * failures to {@link OffsetDecodingError} - but {@link KafkaStreamsEncodingNotSupported} under the FAIL policy is
     * a policy verdict with an actionable message, not a decode failure, and must pass through it unconverted (and
     * NOT wrapped in an {@link OffsetDecodingError}, which callers would silently recover from).
     */
    @Test
    void kafkaStreamsMetadataUnderFailPolicyPassesThroughTheDecodeChokePointUnconverted() {
        final var input = ByteBuffer.allocate(32);
        input.put((byte) 1); // Kafka Streams v1 magic number
        input.putLong(System.currentTimeMillis());
        String metadata = Base64.getEncoder().encodeToString(input.array());

        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(0L, metadata))
                .isInstanceOf(KafkaStreamsEncodingNotSupported.class);
    }

    @SneakyThrows
    @Test
    void compressionCycle() {
        byte[] serialised = offsetCodecManager.encodeOffsetsCompressed(finalOffsetForPartition, state);

        OffsetMapCodecManager.HighestOffsetAndIncompletes deserialised = OffsetMapCodecManager.decodeCompressedOffsets(finalOffsetForPartition, serialised);

        assertThat(deserialised.getIncompleteOffsets()).isEqualTo(incompleteOffsets);
    }

    @Test
    void runLengthEncoding() {
        String stringMap = incompletesToBitmapString(finalOffsetForPartition, state);
        List<Integer> integers = OffsetRunLength.runLengthEncode(stringMap);
        assertThat(integers).as("encoding of map: " + stringMap).containsExactlyElementsOf(UniLists.of(1, 1, 2));

        assertThat(OffsetRunLength.runLengthDecodeToString(integers)).isEqualTo(stringMap);
    }

    static List<String> differentInputsAndCompressions() {
        return inputsToCompress;
    }

    /**
     * Compare compression performance on different types of inputs, and tests that each encoding type is decompressed
     * again correctly
     * <p>
     * Each encoding is also taken through the outer string codec and back, so the whole writer-to-reader path is
     * covered on the same corpus: the writer's per-payload choice between Base64 and sentinel+Z85 is checked against
     * KTD6's arithmetic, and whichever it picks must decode to exactly the same incompletes.
     */
    @SneakyThrows
    @ParameterizedTest
    @MethodSource
    void differentInputsAndCompressions(String input) {
        long highestSeen = input.length() - 1; // pretend we've gone one higher than the input incompletes

        //
        log.debug("Testing round - size: {} input: '{}'", input.length(), input);
        var inputIncompletes = bitmapStringToIncomplete(finalOffsetForPartition, input);
        String sanityEncoding = incompletesToBitmapString(finalOffsetForPartition, highestSeen + 1, inputIncompletes);
        Truth.assertThat(sanityEncoding).isEqualTo(input);

        //
        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(finalOffsetForPartition, highestSeen, inputIncompletes);
        encoder.invoke();

        // test all encodings created
        for (final EncodedOffsetPair encoding : encoder.sortedEncodings) {
            //
            byte[] packedEncoding = encoder.packEncoding(encoding);

            //
            var recoveredIncompleteAndOffset =
                    OffsetMapCodecManager.decodeCompressedOffsets(finalOffsetForPartition, packedEncoding);
            Set<Long> recoveredIncompletes = recoveredIncompleteAndOffset.getIncompleteOffsets();

            //
            assertThat(recoveredIncompletes).containsExactlyInAnyOrderElementsOf(inputIncompletes);

            //
            String simple = incompletesToBitmapString(finalOffsetForPartition, highestSeen + 1, recoveredIncompletes);
            assertWithMessage(encoding.encoding.name())
                    .that(simple).isEqualTo(input);

            // and again through the full outer-codec path the writer and the reader actually use
            String stringEncoded = OffsetSimpleSerialisation.encodeShorterOfBase64OrZ85(packedEncoding);
            assertChosenOuterCodec(encoding.encoding.name(), packedEncoding.length, stringEncoded);

            var viaString = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(finalOffsetForPartition, stringEncoded);
            assertWithMessage("%s round tripped through the %s outer codec",
                    encoding.encoding.name(), stringEncoded.startsWith("%") ? "z85" : "base64")
                    .that(incompletesToBitmapString(finalOffsetForPartition, highestSeen + 1, viaString.getIncompleteOffsets()))
                    .isEqualTo(input);
        }
    }

    /**
     * Holds the writer to KTD6 with U3's floor: Base64 for every payload below 22 bytes (old-reader compatibility,
     * even at the sizes where sentinel+Z85 would be a character shorter), sentinel+Z85 from 22 bytes up where it is
     * always strictly shorter, the sentinel carried exactly when Z85 was chosen, and never a character outside 7-bit
     * ASCII (R8 - the cap counts characters while the broker counts bytes). The floor is restated as a literal 22
     * rather than read from {@link OffsetSimpleSerialisation#Z85_MIN_PAYLOAD_BYTES}, for the same
     * mutant-cannot-move-both-sides reason as the formulas below.
     */
    private static void assertChosenOuterCodec(String encodingName, int payloadBytes, String chosen) {
        int base64Chars = expectedBase64Chars(payloadBytes);
        int sentinelZ85Chars = expectedSentinelZ85Chars(payloadBytes);
        boolean z85Expected = payloadBytes >= 22 && sentinelZ85Chars < base64Chars;

        assertWithMessage("%s: base64 %s vs sentinel+z85 %s under the 22-byte floor, for a %s byte payload",
                encodingName, base64Chars, sentinelZ85Chars, payloadBytes)
                .that(chosen.length()).isEqualTo(z85Expected ? sentinelZ85Chars : base64Chars);
        assertWithMessage("%s: sentinel present exactly when the floor lets z85 fire (%s byte payload)",
                encodingName, payloadBytes)
                .that(chosen.startsWith("%")).isEqualTo(z85Expected);
        assertWithMessage("%s: 7-bit ASCII only, so length() is the UTF-8 byte length", encodingName)
                .that(chosen.length()).isEqualTo(chosen.getBytes(UTF_8).length);
    }

    /**
     * {@code 4*ceil(n/3)}, padded. Deliberately restated from first principles rather than calling
     * {@link OffsetSimpleSerialisation#base64Length}, so a mutant in the production formula cannot make both sides of
     * the comparison move together.
     */
    private static int expectedBase64Chars(int payloadBytes) {
        return 4 * ((payloadBytes + 2) / 3);
    }

    /**
     * 1 sentinel + 5 chars per 4-byte block + {@code (n%4 ? n%4+1 : 0)} for the partial tail group. Independent of
     * {@link Z85Codec#encodedLength} for the same reason as {@link #expectedBase64Chars}.
     */
    private static int expectedSentinelZ85Chars(int payloadBytes) {
        return 1 + 5 * (payloadBytes / 4) + (payloadBytes % 4 == 0 ? 0 : payloadBytes % 4 + 1);
    }

}
