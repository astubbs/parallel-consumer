package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import javax.lang.model.type.TypeKind;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.JavaUtils.toTreeSet;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static io.confluent.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;
import static javax.lang.model.type.TypeKind.INT;
import static javax.lang.model.type.TypeKind.SHORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class RunLengthEncoderTest {

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void noGaps() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        List<Integer> runs = UniLists.of(1, 3, 1, 1, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0); // 1
            rl.encodeCompletedOffset(1); // 3
            rl.encodeCompletedOffset(2);
            rl.encodeCompletedOffset(3);
            rl.encodeIncompleteOffset(4); // 1
            rl.encodeCompletedOffset(5); // 1
            rl.encodeIncompleteOffset(6); // 3
            rl.encodeIncompleteOffset(7);
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9); // 1
            rl.encodeIncompleteOffset(10); // 1

            rl.addTail();

            // before serialisation
            {
                assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

                List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets(0);

                assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
            }
        }
    }


    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void noGapsSerialisation() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet()); // lol - DRY!
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet()); // lol - DRY!
        List<Integer> runs = UniLists.of(1, 3, 1, 1, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0); // 1
            rl.encodeCompletedOffset(1); // 3
            rl.encodeCompletedOffset(2);
            rl.encodeCompletedOffset(3);
            rl.encodeIncompleteOffset(4); // 1
            rl.encodeCompletedOffset(5); // 1
            rl.encodeIncompleteOffset(6); // 3
            rl.encodeIncompleteOffset(7);
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9); // 1
            rl.encodeIncompleteOffset(10); // 1

            // after serialisation
            {
                byte[] raw = rl.serialise();

                byte[] wrapped = offsetSimultaneousEncoder.packEncoding(new EncodedOffsetPair(OffsetEncoding.RunLengthV2, ByteBuffer.wrap(raw)));

                HighestOffsetAndIncompletes result = OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

                assertThat(result.getHighestSeenOffset()).contains(10L);

                assertThat(result.getIncompleteOffsets()).containsExactlyElementsOf(incompletes);
            }
        }
    }

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential.
     */
    @SneakyThrows
    @Test
    void gapsInOffsetsWork() {
        var incompletes = UniSets.of(0, 6, 10).stream().map(x -> (long) x).collect(toTreeSet());

        // NB: gaps between completed offsets get encoded as succeeded offsets. This doesn't matter because they don't exist and we'll neve see them.
        Set<Long> completes = UniSets.of(1, 2, 3, 4, 5, 9).stream().map(x -> (long) x).collect(Collectors.toSet());
        List<Integer> runs = UniLists.of(1, 5, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0);
            rl.encodeCompletedOffset(1);
            // gap completes at 2
            rl.encodeCompletedOffset(3);
            rl.encodeCompletedOffset(4);
            rl.encodeCompletedOffset(5);
            rl.encodeIncompleteOffset(6);
            // gap incompletes at 7
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9);
            rl.encodeIncompleteOffset(10);

            rl.addTail();

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

            List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets(0);

            assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
        }
    }


    /**
     * A run-length encoder stores each run's length as a fixed-width integer: a {@code short} in v1
     * ({@code RunLength}) and an {@code int} in v2 ({@code RunLengthV2}). This verifies that a run longer than
     * that width can hold is rejected up-front with the version-appropriate {@link EncodingNotSupportedException}
     * ({@link RunLengthV1EncodingNotSupported} for v1, {@link RunLengthV2EncodingNotSupported} for v2), rather
     * than silently overflowing and writing a corrupt offset map.
     * <p>
     * Parameterised over every encoding version (v1 overflows a {@code short} at ~32K, v2 overflows an
     * {@code int} at ~2.1B). It is fast for both despite those boundaries - see {@link #testRunLength} for why.
     */
    @SneakyThrows
    @ParameterizedTest()
    @EnumSource(OffsetEncoding.Version.class)
    void vTwoIntegerOverflow(OffsetEncoding.Version versionToTest) {
        final long integerMaxOverflowOffset = 100;
        final long overflowedValue = Integer.MAX_VALUE + integerMaxOverflowOffset;

        var incompletes = UniSets.of(0L, 4L, 6L, 7L, 8L, 10L, overflowedValue).stream().collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        OffsetSimultaneousEncoder offsetSimultaneousEncoder
                = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, overflowedValue - 1, incompletes);

        testRunLength(overflowedValue, offsetSimultaneousEncoder, versionToTest);
    }

    /**
     * Encodes a small, normal run of completed/incomplete offsets (0-10) to build a realistic run-length
     * stream, then injects a single completed offset far ahead so the open run's length exceeds the encoding's
     * fixed-width capacity, and asserts the encoder throws the version-appropriate "run-length too big" error.
     *
     * @param overflowedValue an offset far enough past the previous one that the resulting run-length overflows
     *                         the encoding's entry type (a {@code short} for v1, an {@code int} for v2)
     */
    private static void testRunLength(long overflowedValue, OffsetSimultaneousEncoder offsetSimultaneousEncoder, OffsetEncoding.Version versionsToTest) throws EncodingNotSupportedException {
        RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, versionsToTest);

        rl.encodeIncompleteOffset(0); // 1
        rl.encodeCompletedOffset(1); // 3
        rl.encodeCompletedOffset(2);
        rl.encodeCompletedOffset(3);
        rl.encodeIncompleteOffset(4); // 1
        rl.encodeCompletedOffset(5); // 1
        rl.encodeIncompleteOffset(6); // 3
        rl.encodeIncompleteOffset(7);
        rl.encodeIncompleteOffset(8);
        rl.encodeCompletedOffset(9); // 1
        rl.encodeIncompleteOffset(10); // 1

        // inject overflow offset
        //
        // The run-length is accumulated from the DELTA between an offset and the previous one
        // (RunLengthEncoder.encodeRunLength: `delta = relativeOffset - previousRangeIndex`), NOT from the
        // number of encode calls. So a single completed offset a huge distance past the previous one drives
        // the run-length past the encoding's capacity in ONE step - exercising the exact same overflow path
        // (Math.toIntExact / toShortExact) as counting up one-by-one, but without ~2.1 billion iterations
        // (which made the v2/Integer case a ~1.5 minute test). A large delta is also a realistic input: gaps
        // between completed offsets are themselves encoded as a run (see the RunLengthEncoder class doc).
        var errorAssertion = Assertions.assertThatThrownBy(() -> {
            rl.encodeCompletedOffset(11); // open a "completed" run at 11
            rl.encodeCompletedOffset(overflowedValue); // one large delta -> run-length overflows Short (v1) / Integer (v2)
        });

        switch (versionsToTest) {
            case v1 -> {
                errorAssertion.isInstanceOf(RunLengthV1EncodingNotSupported.class);
                errorAssertion.hasMessageContainingAll("too big", "Short");
            }
            case v2 -> {
                errorAssertion.isInstanceOf(RunLengthV2EncodingNotSupported.class);
                errorAssertion.hasMessageContainingAll("too big", "Integer");
            }
        }
    }

    /**
     * The {@link OffsetSimultaneousEncoder} runs every candidate encoding at once and keeps only those that can
     * represent the input, dropping - gracefully, without failing the whole commit - any whose encoder throws
     * {@link EncodingNotSupportedException}. This verifies that overflow is handled by dropping the offending
     * encodings, not by crashing:
     * <ul>
     *   <li>{@code SHORT}: the run overflows only the {@code short}-width (v1) run-length encodings; wider
     *       encodings still fit, so the map comes back with the survivors (2).</li>
     *   <li>{@code INT}: the run overflows every candidate (the {@code int}-width v2 run-length, and the raw
     *       bitset/bytebuffer encodings are too large to allocate for a ~2.1B span), so the map is empty.</li>
     * </ul>
     * <p>
     * Unlike {@link #vTwoIntegerOverflow}, this test cannot use the single-large-delta shortcut: the
     * simultaneous encoder walks <em>every</em> offset in the range ({@link OffsetSimultaneousEncoder#invoke()}
     * calls the per-offset encode for each one), so provoking an {@code int} overflow genuinely iterates ~2.1
     * billion offsets - which is why the {@code INT} case is slow (~1.5 min). Making it fast needs a
     * delta-aware {@code invoke()} (the run-length optimisation TODO on {@link OffsetSimultaneousEncoder}), a
     * main-code change tracked in {@code docs/inflight.md} - not something to fake at the test level.
     */
    @ParameterizedTest
    @EnumSource(names = {"SHORT", "INT"}, mode = INCLUDE)
    void testSimultaneousWithOverflowErrors(TypeKind primitiveSize) {
        Assumptions.assumeTrue(primitiveSize == SHORT || primitiveSize == INT);

        final long integerMaxOverflowOffset = 100;
        final int maxValue = switch (primitiveSize) {
            case SHORT -> Short.MAX_VALUE;
            case INT -> Integer.MAX_VALUE;
            default -> throw new IllegalStateException("Unexpected value: " + primitiveSize);
        };

        final long overflowedValue = maxValue + integerMaxOverflowOffset;

        var incompletes = UniSets.of(0L, 4L, 6L, 7L, 8L, 10L, overflowedValue).stream().collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        OffsetSimultaneousEncoder offsetSimultaneousEncoder
                = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, overflowedValue - 1, incompletes);

        offsetSimultaneousEncoder.invoke();

        final Map<OffsetEncoding, byte[]> encodingMap = offsetSimultaneousEncoder.getEncodingMap();

        //
        switch (primitiveSize) {
            case SHORT -> Truth.assertThat(encodingMap).hasSize(2);
            case INT -> Truth.assertThat(encodingMap).hasSize(0);
            default -> throw new IllegalStateException("Unexpected value: " + primitiveSize);
        }
        ;
    }

}
