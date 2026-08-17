package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.PartitionState;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;

/**
 * Unit tests for the sparse delta-list encoder and its decoder - the candidate the density benchmark
 * ({@link OffsetEncodingDensityBenchmarkTest}) measured as the winner, shipped per the plan's U4.
 *
 * <h2>Why so many exact-byte assertions</h2>
 * The wire format is written into other people's committed offsets forever, so it is pinned by golden vectors
 * rather than by round-trips alone: a round-trip passes just as happily if the encoder and the decoder are
 * wrong in the same direction. The offsets package is also the PIT mutation lane, and only exact-value
 * assertions kill mutants in varint arithmetic.
 *
 * @author Antony Stubbs
 * @see DeltaListEncoder
 * @see OffsetDeltaList
 */
@Slf4j
// Writes OffsetMapCodecManager.forcedCodec (guarded by the metadata-size lock, per
// WorkManagerOffsetMapCodecManagerTest's note on that field) and OffsetSimultaneousEncoder.compressionForced.
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = READ_WRITE)
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
class DeltaListEncodingTest {

    private static final TopicPartition TP = new TopicPartition("myTopic", 0);

    @AfterEach
    void restoreStatics() {
        OffsetMapCodecManager.forcedCodec = Optional.empty();
        OffsetSimultaneousEncoder.compressionForced = false;
    }

    private static SortedSet<Long> incompletes(long... offsets) {
        final TreeSet<Long> out = new TreeSet<>();
        for (final long offset : offsets) {
            out.add(offset);
        }
        return out;
    }

    /**
     * Encodes with a real {@link DeltaListEncoder} attached to a real {@link OffsetSimultaneousEncoder}, exactly as
     * production does, and returns the payload without the magic byte.
     */
    @SneakyThrows
    private static byte[] serialise(long baseOffset, long highestSucceeded, SortedSet<Long> incompletes) {
        final OffsetSimultaneousEncoder simultaneous =
                new OffsetSimultaneousEncoder(baseOffset, highestSucceeded, incompletes);
        return new DeltaListEncoder(simultaneous).serialise();
    }

    /** @return the packed bytes (magic byte first), as {@code packEncoding} would produce them */
    @SneakyThrows
    private static byte[] packed(long baseOffset, long highestSucceeded, SortedSet<Long> incompletes) {
        final OffsetSimultaneousEncoder simultaneous =
                new OffsetSimultaneousEncoder(baseOffset, highestSucceeded, incompletes);
        final byte[] payload = new DeltaListEncoder(simultaneous).serialise();
        return simultaneous.packEncoding(
                new EncodedOffsetPair(OffsetEncoding.DeltaList, ByteBuffer.wrap(payload)));
    }

    /**
     * The {@link BitSetEncodingTest#basic()} template: encode a small mixed range through the real encoder, pack it
     * with its magic byte, and decode it through the same path a commit takes.
     */
    @SneakyThrows
    @Test
    void basic() {
        final SortedSet<Long> incompletes = incompletes(0, 4, 6, 7, 8, 10);
        final byte[] wrapped = packed(0L, 10L, incompletes);

        final OffsetMapCodecManager.HighestOffsetAndIncompletes result =
                OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

        assertThat(result.getHighestSeenOffset()).hasValue(10L);
        assertThat(result.getIncompleteOffsets()).containsExactlyElementsIn(incompletes).inOrder();
    }

    /**
     * Golden vector - the frozen wire format. {@code [rangeLength:int4][count:varint][first relative, then gaps]}.
     * <p>
     * <strong>Never regenerate this to make a failure pass.</strong> A change here is a change to what already
     * committed offsets mean.
     */
    @Test
    void goldenVectorOfAFixedIncompletesSet() {
        final byte[] payload = serialise(0L, 10L, incompletes(0, 4, 6, 7, 8, 10));

        assertThat(payload).isEqualTo(new byte[]{
                0, 0, 0, 11,  // rangeLength - 11 offsets, 0..10
                6,            // count of incompletes
                0,            // first incomplete is at relative 0
                4, 2, 1, 1, 2 // gaps: 4->0, 6->4, 7->6, 8->7, 10->8
        });
    }

    /** The same set at a non-zero base offset encodes identically - deltas are over RELATIVE offsets. */
    @Test
    void theBaseOffsetIsNotEncodedOnlyRelativeOffsetsAre() {
        final byte[] atZero = serialise(0L, 10L, incompletes(0, 4, 6, 7, 8, 10));
        final byte[] atOneThousand = serialise(1_000L, 1_010L, incompletes(1_000, 1_004, 1_006, 1_007, 1_008, 1_010));

        assertThat(atOneThousand).isEqualTo(atZero);
    }

    /**
     * Golden vectors at the LEB128 boundary: a delta of 127 is one byte, 128 is two. This is the pin the plan asks
     * for, and the mutation-test target in the varint writer's continuation-bit arithmetic.
     */
    @Test
    void goldenVectorsAtTheVarintBoundary() {
        assertWithMessage("a delta of 127 fits one byte")
                .that(serialise(0L, 999L, incompletes(0, 127)))
                .isEqualTo(new byte[]{0, 0, 3, (byte) 0xE8, 2, 0, 127});

        assertWithMessage("a delta of 128 needs two")
                .that(serialise(0L, 999L, incompletes(0, 128)))
                .isEqualTo(new byte[]{0, 0, 3, (byte) 0xE8, 2, 0, (byte) 0x80, 1});
    }

    /** Golden vector for a count past 127: the count is a varint too, so it grows to two bytes at 128. */
    @Test
    void goldenVectorOfAMultiByteCount() {
        final TreeSet<Long> contiguous = new TreeSet<>();
        for (long offset = 0; offset < 200; offset++) {
            contiguous.add(offset);
        }

        final byte[] payload = serialise(0L, 999L, contiguous);

        assertThat(payload).hasLength(4 + 2 + 200);
        assertWithMessage("rangeLength, then varint(200) = 0xC8 0x01, then relative 0 and 199 gaps of 1")
                .that(Arrays.copyOf(payload, 8))
                .isEqualTo(new byte[]{0, 0, 3, (byte) 0xE8, (byte) 0xC8, 1, 0, 1});
        assertThat((int) payload[payload.length - 1]).isEqualTo(1);
    }

    /** Golden vector for multi-byte deltas, and a round trip proving they come back. */
    @SneakyThrows
    @Test
    void largeDeltasEncodeAsMultiByteVarintsAndRoundTrip() {
        final SortedSet<Long> incompletes = incompletes(0, 16_384, 2_097_152);
        final byte[] payload = serialise(0L, 2_999_999L, incompletes);

        assertThat(payload).isEqualTo(new byte[]{
                0, 0x2D, (byte) 0xC6, (byte) 0xC0, // rangeLength 3,000,000
                3,                                 // count
                0,                                 // relative 0
                (byte) 0x80, (byte) 0x80, 1,       // gap of 16,384
                (byte) 0x80, (byte) 0x80, 127      // gap of 2,080,768
        });

        final var decoded = OffsetMapCodecManager.decodeCompressedOffsets(0, packed(0L, 2_999_999L, incompletes));
        assertThat(decoded.getIncompleteOffsets()).containsExactlyElementsIn(incompletes).inOrder();
        assertThat(decoded.getHighestSeenOffset()).hasValue(2_999_999L);
    }

    /** Edge: exactly one incomplete offset, in the middle of the range. */
    @SneakyThrows
    @Test
    void aSingleIncompleteOffset() {
        final byte[] payload = serialise(0L, 999L, incompletes(500));
        assertThat(payload).isEqualTo(new byte[]{0, 0, 3, (byte) 0xE8, 1, (byte) 0xF4, 3});

        final var decoded = OffsetMapCodecManager.decodeCompressedOffsets(0, packed(0L, 999L, incompletes(500)));
        assertThat(decoded.getIncompleteOffsets()).containsExactly(500L);
        assertThat(decoded.getHighestSeenOffset()).hasValue(999L);
    }

    /**
     * Edge: nothing incomplete. The header alone still recovers the range, which is the whole reason
     * {@code [rangeLength:int4]} is in the format - the incompletes cannot imply the top of the range.
     */
    @SneakyThrows
    @Test
    void anEmptyIncompletesSetIsJustTheHeader() {
        final byte[] payload = serialise(0L, 999L, incompletes());
        assertThat(payload).isEqualTo(new byte[]{0, 0, 3, (byte) 0xE8, 0});

        final var decoded = OffsetMapCodecManager.decodeCompressedOffsets(50L, packed(50L, 1_049L, incompletes()));
        assertThat(decoded.getIncompleteOffsets()).isEmpty();
        assertWithMessage("highest seen is recovered from the range length alone")
                .that(decoded.getHighestSeenOffset()).hasValue(1_049L);
    }

    /** Edge: the first incomplete sits at relative 0 (the base offset itself), and the last at the range's end. */
    @SneakyThrows
    @Test
    void incompletesAtBothEndsOfTheRange() {
        final SortedSet<Long> incompletes = incompletes(100, 110);
        final byte[] payload = serialise(100L, 110L, incompletes);
        assertThat(payload).isEqualTo(new byte[]{0, 0, 0, 11, 2, 0, 10});

        final var decoded = OffsetMapCodecManager.decodeCompressedOffsets(100L, packed(100L, 110L, incompletes));
        assertThat(decoded.getIncompleteOffsets()).containsExactlyElementsIn(incompletes).inOrder();
        assertThat(decoded.getHighestSeenOffset()).hasValue(110L);
    }

    /**
     * The encoder reads the incompletes set directly rather than being fed offset-by-offset, so it - and only it -
     * has to clamp that set to the range being encoded. Offsets above the highest succeeded offset are routinely in
     * the set (PC processes well beyond the commit point), and the decoder can only address the range.
     * <p>
     * This is exactly the shape of {@code OffsetEncodingTests.verifyEncodingWithNextExpectedBelowWatermark}, where
     * the range length is zero and the set is not empty.
     */
    @SneakyThrows
    @Test
    void offsetsOutsideTheEncodedRangeAreNotEncoded() {
        // range of 0: highest succeeded is one below the base offset, so nothing is encodable
        final byte[] emptyRange = serialise(123L, 122L, incompletes(2_345, 8_765));
        assertThat(emptyRange).isEqualTo(new byte[]{0, 0, 0, 0, 0});

        final var decoded = OffsetMapCodecManager.decodeCompressedOffsets(123L, packed(123L, 122L, incompletes(2_345, 8_765)));
        assertThat(decoded.getIncompleteOffsets()).isEmpty();
        assertWithMessage("a zero-length range means the highest seen offset is the one below the base")
                .that(decoded.getHighestSeenOffset()).hasValue(122L);

        // and with a real range, only the offsets inside it are encoded
        final byte[] partial = serialise(10L, 19L, incompletes(5, 12, 19, 25));
        assertThat(partial).isEqualTo(new byte[]{0, 0, 0, 10, 2, 2, 7});
    }

    /**
     * The layout writer in the benchmark and this encoder must agree byte for byte, or the numbers the ship decision
     * was made on describe a format that does not exist. Everything after the magic byte must be identical (the
     * benchmark uses a placeholder magic byte deliberately - only the field's width matters to a size measurement).
     *
     * @see OffsetEncodingDensityBenchmarkTest#sparseDeltaListLayout(long, SortedSet, long)
     */
    @Test
    void matchesTheBenchmarkLayoutWriterByteForByte() {
        assertLayoutMatches("mixed small set", 0L, 10L, incompletes(0, 4, 6, 7, 8, 10));
        assertLayoutMatches("varint boundary", 0L, 999L, incompletes(0, 128));
        assertLayoutMatches("single isolated", 0L, 999L, incompletes(500));
        assertLayoutMatches("empty", 0L, 999L, incompletes());
        assertLayoutMatches("multi byte deltas", 0L, 2_999_999L, incompletes(0, 16_384, 2_097_152));
        assertLayoutMatches("uniform 1% corpus scenario", 0L, 9_999L,
                OffsetEncodingDensityBenchmarkTest.corpusIncompletes("uniform-random 1%", 10_000));
        assertLayoutMatches("clustered corpus scenario", 0L, 9_999L,
                OffsetEncodingDensityBenchmarkTest.corpusIncompletes("clustered-bursts", 10_000));
    }

    private static void assertLayoutMatches(String what, long baseOffset, long highestSucceeded,
                                           SortedSet<Long> incompletes) {
        final long rangeLength = highestSucceeded - baseOffset + 1;
        final byte[] layout =
                OffsetEncodingDensityBenchmarkTest.sparseDeltaListLayout(rangeLength, incompletes, baseOffset);
        final byte[] payload = serialise(baseOffset, highestSucceeded, incompletes);

        assertWithMessage("%s: real encoder output must equal the benchmark's layout after the magic byte", what)
                .that(payload)
                .isEqualTo(Arrays.copyOfRange(layout, 1, layout.length));
    }

    /**
     * The format addresses the range with a 4-byte length, so a range past {@link Integer#MAX_VALUE} cannot be
     * expressed. It must say so with an {@link EncodingNotSupportedException} subclass, which is what makes
     * {@link OffsetSimultaneousEncoder} drop the encoder instead of failing the commit.
     * <p>
     * Constructed with a single huge jump and WITHOUT calling {@code invoke()}: the per-offset walk over a
     * multi-billion offset range is what makes {@code RunLengthEncoderTest.testSimultaneousWithOverflowErrors[INT]}
     * take about ninety seconds, and that test already covers the drop through the full path (it asserts an empty
     * encoding map for exactly this range).
     */
    @Test
    void aRangeBeyondIntegerMaxIsNotSupported() {
        final long tooHigh = Integer.MAX_VALUE + 100L;
        final OffsetSimultaneousEncoder simultaneous =
                new OffsetSimultaneousEncoder(0L, tooHigh, incompletes(0, 5, tooHigh - 1));

        assertThatThrownBy(() -> new DeltaListEncoder(simultaneous))
                .isInstanceOf(DeltaListEncodingNotSupportedException.class)
                .isInstanceOf(EncodingNotSupportedException.class)
                .hasMessageContaining("Integer");

        assertWithMessage("the simultaneous encoder must still construct - it drops the encoder, not the commit")
                .that(simultaneous.getIncompleteOffsets()).hasSize(3);
    }

    /**
     * The other side of the boundary: a range of exactly {@link Integer#MAX_VALUE} IS expressible, and encodes the
     * length as {@code 0x7FFFFFFF}. Without this, a {@code >=} in place of the {@code >} in the range check would go
     * unnoticed.
     * <p>
     * Driven from a MOCK simultaneous encoder rather than a real one on purpose: constructing a real
     * {@link OffsetSimultaneousEncoder} over a range that large allocates two {@link java.util.BitSet}s of
     * {@code Integer.MAX_VALUE} bits (about 256MB each) inside {@code initEncoders}, which has nothing to do with
     * what is being tested here.
     */
    @SneakyThrows
    @Test
    void aRangeOfExactlyIntegerMaxIsStillExpressible() {
        final OffsetSimultaneousEncoder simultaneous = Mockito.mock(OffsetSimultaneousEncoder.class);
        Mockito.when(simultaneous.getLengthBetweenBaseAndHighOffset()).thenReturn((long) Integer.MAX_VALUE);
        Mockito.when(simultaneous.getLowWaterMark()).thenReturn(0L);
        Mockito.when(simultaneous.getIncompleteOffsets()).thenReturn(incompletes(0, Integer.MAX_VALUE - 1L));

        final byte[] payload = new DeltaListEncoder(simultaneous).serialise();

        assertThat(payload).isEqualTo(new byte[]{
                0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, // rangeLength - the largest expressible
                2,                                           // count
                0,                                           // relative 0
                (byte) 0xFE, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 7 // gap of Integer.MAX_VALUE - 1
        });
    }

    /**
     * Round trip through the whole writer-to-reader path a commit takes - the codec manager, the outer string codec
     * and back - for both the plain and the zstd-compressed magic byte.
     */
    @SneakyThrows
    @Test
    void roundTripsThroughTheFullManagerPath() {
        for (final OffsetEncoding encoding : new OffsetEncoding[]{
                OffsetEncoding.DeltaList, OffsetEncoding.DeltaListCompressed}) {
            final SortedSet<Long> incompletes = incompletes(0, 2, 3, 500, 700, 701, 999);
            final long highestSucceeded = 1_000L;

            final MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            final var options = ParallelConsumerOptions.<String, String>builder().consumer(mockConsumer).build();
            final var module = new PCModuleTestEnv(options);
            final PartitionState<String, String> state = new PartitionState<>(0, module, TP,
                    new OffsetMapCodecManager.HighestOffsetAndIncompletes(Optional.of(highestSucceeded), incompletes));
            final var codecManager = new OffsetMapCodecManager<>(module);

            OffsetSimultaneousEncoder.compressionForced = true; // so the compressed twin is registered whatever its size
            OffsetMapCodecManager.forcedCodec = Optional.of(encoding);

            final String metadata = codecManager.serialiseIncompleteOffsetMapToString(0L, state);
            final var decoded = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(0L, metadata);

            assertWithMessage("%s: incompletes recovered", encoding)
                    .that(decoded.getIncompleteOffsets()).containsExactlyElementsIn(incompletes).inOrder();
            assertWithMessage("%s: highest seen recovered from the range length", encoding)
                    .that(decoded.getHighestSeenOffset()).hasValue(highestSucceeded);
        }
    }

    /**
     * Base64 of a raw payload, as it would sit in a committed offset's metadata field - the shape the corrupt-payload
     * tests below feed to the real string-decode entry point.
     */
    private static String base64Metadata(byte... payload) {
        return java.util.Base64.getEncoder().encodeToString(payload);
    }

    /**
     * Corrupt and truncated payloads: whatever is wrong with the bytes after the {@code 'd'} magic byte, the failure
     * must surface from {@link OffsetMapCodecManager#deserialiseIncompleteOffsetMapFromString} as
     * {@link OffsetDecodingError} - never as an unchecked exception. The committed metadata is untrusted input, and
     * only {@code OffsetDecodingError} reaches the drop-the-map recovery in
     * {@code OffsetMapCodecManager#loadPartitionStateForAssignment}; anything unchecked escapes the rebalance
     * listener and crash-loops the consumer, which {@link ForeignOffsetMetadataOnAssignmentTest} pins at the
     * assignment level.
     */
    @Test
    void aTruncatedBodyIsARecoverableDecodingError() {
        // "ZA==": the DeltaList magic byte alone, no header - getInt() has nothing to read
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(
                0L, base64Metadata((byte) 'd')))
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(java.nio.BufferUnderflowException.class);
    }

    /** A varint longer than any {@code long}: ten continuation bytes where the count should be. */
    @Test
    void anOverLongVarintIsARecoverableDecodingError() {
        final byte[] payload = new byte[15];
        payload[0] = (byte) 'd';
        // plausible rangeLength of 5...
        payload[4] = 5;
        // ...then ten bytes all with the continuation bit set
        Arrays.fill(payload, 5, payload.length, (byte) 0x80);

        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(
                0L, base64Metadata(payload)))
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(bz.stub.parallelconsumer.internal.InternalRuntimeException.class);
    }

    /**
     * A negative range length. No writer emits one (the encoder declines past {@link Integer#MAX_VALUE}), so it can
     * only be corruption - and before it was rejected explicitly it decoded SILENTLY to garbage: a highest-seen
     * offset below the committed base.
     */
    @Test
    void aNegativeRangeLengthIsARecoverableDecodingError() {
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(
                0L, base64Metadata((byte) 'd', (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0)))
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(bz.stub.parallelconsumer.internal.InternalRuntimeException.class);
    }

    /** A count claiming more deltas than the payload holds: the reader runs off the end of the buffer. */
    @Test
    void aCountExceedingTheRemainingBytesIsARecoverableDecodingError() {
        // rangeLength 5, count 3, but only one delta present
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromString(
                0L, base64Metadata((byte) 'd', (byte) 0, (byte) 0, (byte) 0, (byte) 5, (byte) 3, (byte) 1)))
                .isInstanceOf(OffsetDecodingError.class)
                .hasCauseInstanceOf(java.nio.BufferUnderflowException.class);
    }

    /** The debug rendering path ({@code getDecodedString}) must decompress before decoding, and agree with the plain arm. */
    @SneakyThrows
    @Test
    void bothMagicBytesRenderTheSameBitmapString() {
        final SortedSet<Long> incompletes = incompletes(0, 2, 3);
        final OffsetSimultaneousEncoder simultaneous = new OffsetSimultaneousEncoder(0L, 4L, incompletes);
        final DeltaListEncoder encoder = new DeltaListEncoder(simultaneous);
        final byte[] payload = encoder.serialise();
        final byte[] compressed = OffsetSimpleSerialisation.compressZstd(payload);

        final String plain = EncodedOffsetPair.unwrap(simultaneous.packEncoding(
                new EncodedOffsetPair(OffsetEncoding.DeltaList, ByteBuffer.wrap(payload)))).getDecodedString();
        final String viaZstd = EncodedOffsetPair.unwrap(simultaneous.packEncoding(
                new EncodedOffsetPair(OffsetEncoding.DeltaListCompressed, ByteBuffer.wrap(compressed)))).getDecodedString();

        assertThat(plain).isEqualTo("oxoox");
        assertThat(viaZstd).isEqualTo(plain);
    }
}
