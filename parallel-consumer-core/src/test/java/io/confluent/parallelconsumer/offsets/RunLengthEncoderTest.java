package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.Offsets;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import io.confluent.parallelconsumer.offsets.RunLengthEncoder.RunLengthEntry;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
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
import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static io.confluent.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;
import static javax.lang.model.type.TypeKind.INT;
import static javax.lang.model.type.TypeKind.SHORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;
import static org.mockito.Mockito.mock;
import static pl.tlinkowski.unij.api.UniLists.of;

class RunLengthEncoderTest {

    @Test
    void genesisCaseComplete() {
        var rle = new RunLengthEncoder(0L, mock(OffsetSimultaneousEncoder.class), v2);
        rle.encodeCompleteOffset(0L, 1L, 0L);
        final List<Long> longs = rle.calculateSucceededActualOffsets();
        assertTruth(longs).containsExactly(1L);
    }

    @Test
    void genesisCaseIncomplete() {
        var rle = new RunLengthEncoder(0L, mock(OffsetSimultaneousEncoder.class), v2);
        rle.encodeIncompleteOffset(0L, 1L, 0L);
        assertTruth(rle).getRunLengthEncodingIntegers().isEmpty();
        final List<Long> longs = rle.calculateSucceededActualOffsets();
        assertTruth(longs).isEmpty();
    }

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
            long base = 0;
            RunLengthEncoder rl = new RunLengthEncoder(base, offsetSimultaneousEncoder, v2);

            addSharedTestData(rl);

//            rl.addTail();

            // before serialisation
            {
                assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

                List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets();

                assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
            }
        }
    }

    private static void addSharedTestData(RunLengthEncoder rl) {
        long base = 0;
        long highest = 9;

        rl.encodeIncompleteOffset(base, 0, highest); // 1
        rl.encodeCompleteOffset(base, 1, highest); // 3
        rl.encodeCompleteOffset(base, 2, highest);
        rl.encodeCompleteOffset(base, 3, highest);
        rl.encodeIncompleteOffset(base, 4, highest); // 1
        rl.encodeCompleteOffset(base, 5, highest); // 1
        rl.encodeIncompleteOffset(base, 6, highest); // 3
        rl.encodeIncompleteOffset(base, 7, highest);
        rl.encodeIncompleteOffset(base, 8, highest);
        rl.encodeCompleteOffset(base, 9, highest); // 1
        rl.encodeIncompleteOffset(base, 10, highest); // 1
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
            long base = 0;

            RunLengthEncoder rl = new RunLengthEncoder(base, offsetSimultaneousEncoder, v2);

            addSharedTestData(rl);

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

            long base = 0;
            long highest = 9;

            RunLengthEncoder rl = new RunLengthEncoder(base, offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(base, 0, highest);
            rl.encodeCompleteOffset(base, 1, highest);
            // gap completes at 2
            rl.encodeCompleteOffset(base, 3, highest);
            rl.encodeCompleteOffset(base, 4, highest);
            rl.encodeCompleteOffset(base, 5, highest);
            rl.encodeIncompleteOffset(base, 6, highest);
            // gap incompletes at 7
            rl.encodeIncompleteOffset(base, 8, highest);
            rl.encodeCompleteOffset(base, 9, highest);
            rl.encodeIncompleteOffset(base, 10, highest);

//            rl.addTail();

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

            List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets();

            assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
        }
    }


    /**
     * Check RLv2 errors on integer overflow. Integer version of this test is very slow (1.5 minutes).
     */
    @SneakyThrows
    @ParameterizedTest()
    @EnumSource(OffsetEncoding.Version.class)
    void vTwoIntegerOverflow(OffsetEncoding.Version versionToTest) {
        final long integerMaxOverflowOffset = 100;
        final long overflowedValue = Integer.MAX_VALUE + integerMaxOverflowOffset;

        var incompletes = UniSets.of(0L, 4L, 6L, 7L, 8L, 10L, overflowedValue).stream().collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        final long highestSucceededOffset = overflowedValue - 1;
        OffsetSimultaneousEncoder offsetSimultaneousEncoder
                = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, highestSucceededOffset, incompletes);

        {
            final OffsetEncoding.Version versionsToTest = v2;
            testRunLength(overflowedValue, offsetSimultaneousEncoder, versionToTest, highestSucceededOffset);
        }
    }

    private static void testRunLength(long overflowedValue,
                                      OffsetSimultaneousEncoder offsetSimultaneousEncoder,
                                      OffsetEncoding.Version versionsToTest,
                                      long currentHighestCompleteOffset) throws EncodingNotSupportedException {
        RunLengthEncoder rl = new RunLengthEncoder(0L, offsetSimultaneousEncoder, versionsToTest);

        addSharedTestData(rl);


        // inject overflow offset
        var errorAssertion = Assertions.assertThatThrownBy(() -> {
            for (var relativeOffset : Range.range(11, overflowedValue)) {
                rl.encodeCompleteOffset(0l, relativeOffset, currentHighestCompleteOffset);
            }
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
     * Test simultaneous encoder with run-length overflow errors fail gracefully.
     * <p>
     * Integer version of this test is very slow (1.5 minutes).
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

        offsetSimultaneousEncoder.oldIinvoke();

        final Map<OffsetEncoding, byte[]> encodingMap = offsetSimultaneousEncoder.getEncodingMap();

        //
        switch (primitiveSize) {
            case SHORT -> Truth.assertThat(encodingMap).hasSize(2);
            case INT -> Truth.assertThat(encodingMap).hasSize(0);
            default -> throw new IllegalStateException("Unexpected value: " + primitiveSize);
        }
        ;
    }


    /**
     * Starting with offsets and bit values:
     * <p>
     * 0 1   2 3 4 5 6  7 8 9  10  11 12 13  14 15 16 17
     * <p>
     * 0 0   1 1 0 1 1  0 0 0  1    0  0  0   1 1  1  1
     * <p>
     * The run lengths are: 2,2,1,2,3,1,3,4
     * <p>
     * If we were to need to truncate at offset 4 (the new base)
     * <p>
     * 4
     * <p>
     * The new offsets and bit values are:
     * <p>
     * 0  1 2  3 4 5  6
     * <p>
     * 0  1 1  0 0 0  1
     * <p>
     * Who's run lengths are:
     * <p>
     * 1, 2, 3, 1
     */
    @Disabled("V1 deprecated and doesn't currently work")
    @Test
    void truncateV1() {
        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 10);

            assertOffsetsAndRuns(rl,
                    of(12, 13, 15, 16, 20, 24, 25, 26, 27), // offsets
                    of(2, 2, 1, 2, 3, 1, 3, 4)); // runs

            rl.truncateRunlengths(12);

            assertOffsetsAndRuns(rl,
                    of(2, 4), // offsets
                    of(14, 15, 16, 17)); // runs
        }

        //v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengths(4);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengths(8);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(2, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengths(9);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(1, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }
    }

    @Test
    void truncateV2() {
        {
            // test case where base != 0
            int base = 10;
            RunLengthEncoder rl = new RunLengthEncoder(base, new OffsetSimultaneousEncoder(base, (long) base), OffsetEncoding.Version.v2);

            encodePattern(rl, base);

            assertThat(rl.getPositiveRunLengths()).extracting(RunLengthEntry::getAbsoluteStartOffset).extracting(Long::intValue).containsExactly(10, 12, 14, 15, 17, 20, 21, 24);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(12L, 13L, 15L, 16L, 20L, 24L, 25L, 26L, 27L);

            rl.truncateRunLengthsV2(22);

            assertThat(rl.getPositiveRunLengths()).extracting(RunLengthEntry::getRunLength).containsExactly(2L, 4L);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(24L, 25L, 26L, 27L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunLengthsV2(4);

            assertThat(rl.getPositiveRunLengths()).extracting(RunLengthEntry::getRunLength).containsExactly(1L, 2L, 3L, 1L, 3L, 4L);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunLengthsV2(8);

            assertThat(rl.getPositiveRunLengths()).extracting(RunLengthEntry::getRunLength).containsExactly(2L, 1L, 3L, 4L);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunLengthsV2(9);

            assertThat(rl.getPositiveRunLengths()).extracting(RunLengthEntry::getRunLength).containsExactly(1L, 1L, 3L, 4L);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }
    }

    private void encodePattern(final RunLengthEncoder rl, long base) {
        int highest = 17 + (int) base;
        int relative = 0;
        {
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
        }
    }

    /**
     * We receive a truncation request where the truncation point is beyond anything our runlengths cover
     */
    @Test
    void v2TruncateOverMax() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeIncompleteOffset(0, 0, 0);
        rl.encodeCompleteOffset(0, 1, 1);

//        rl.addTail();
        rl.truncateRunLengthsV2(2);

        assertThat(rl.getPositiveRunLengths()).isEmpty();
        assertThat(rl.calculateSucceededActualOffsets()).isEmpty();
    }

    /**
     * Encoding requests can be out of order
     */
    @Test
    void outOfOrderEncoding() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 10, 10);
        rl.encodeCompleteOffset(0, 11, 11);
        rl.encodeCompleteOffset(0, 12, 12);
        assertOffsetsAndRuns(rl,
                of(10, 11, 12), // offsets
                of(10, 3)); // runs

        // middle offset out of order
        rl.encodeCompleteOffset(0, 6, 12);
        assertOffsetsAndRuns(rl,
                of(6, 10, 11, 12), // offsets
                of(6, 1, 3, 3)); // runs

        // 0 case offset out of order
        rl.encodeCompleteOffset(0, 1, 12);
        assertOffsetsAndRuns(rl,
                of(1, 6, 10, 11, 12), // offsets
                of(1, 1, 4, 1, 3, 3)); // runs

        rl.truncateRunLengthsV2(2);
        assertOffsetsAndRuns(rl,
                of(6, 10, 11, 12), // offsets
                of(4, 1, 3, 3)); // runs

        rl.truncateRunLengthsV2(8);
        assertOffsetsAndRuns(rl,
                of(10, 11, 12), // offsets
                of(2, 3)); // runs
    }

    /**
     * Simple segmentation of existing entry on out-of-order arrival.
     * <p>
     * Scenarios:
     * <p>
     * offset range - run length - bit O or X (x=success)
     * <p>
     * one:
     * <p>
     * 10-20 - 3 O
     * <p>
     * 16 -1 O // ignore - impossible?
     * <p>
     * two:
     * <p>
     * 10-20 - 3 X
     * <p>
     * 16 -1 X // ignore?
     * <p>
     * three:
     * <p>
     * 10-20 - 3 O
     * <p>
     * 16 -1 X results in:
     * <p>
     * 10-15 - 5 O 16-16 - 1 X 17-20 - 4 O
     * <p>
     * Four:
     * <p>
     * 10-20 - 3 X
     * <p>
     * 16 -1 O // impossible - ignore*
     */
    @Test
    void segmentTestOne() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        // add a positive entry with a big jump from base
        rl.encodeCompleteOffset(0, 10, 10);
        assertOffsetsAndRuns(rl,
                of(10), // offsets
                of(10, 1)); // runs

        // add a positive entry, but it's out of order, bisecting the existing negative run-length entry
        rl.encodeCompleteOffset(0, 6, 10);
        assertOffsetsAndRuns(rl,
                of(6, 10),
                of(6, 1, 3, 1));

        assertThat(rl.calculateFullRunLengthEntries()).extracting(RunLengthEntry::getAbsoluteStartOffset).extracting(Long::intValue).containsExactly(0, 6, 7, 10);
        assertThat(rl.calculateFullRunLengthEntries()).extracting(RunLengthEntry::getRunLength).containsExactly(6L, 1L, 3L, 1L);
        assertThat(rl.calculateSucceededActualOffsets()).extracting(Long::intValue).containsExactly(6, 10);
    }

    @Test
    void segmentTestTwo() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(10, 20, 20);
        assertOffsetsAndRuns(rl,
                of(30), // successful offset
                of(20, 1) // run lengths
        );

        // middle
        rl.encodeCompleteOffset(10, 10, 20);
        assertOffsetsAndRuns(rl,
                of(20, 30),
                of(10, 1, 9, 1));

        // middle offset out of order
        rl.encodeCompleteOffset(10, 6, 20);
        assertOffsetsAndRuns(rl,
                of(16, 20, 30),
                of(6, 1, 3, 1, 9, 1));

        // up by one, continuous 6 and 7 combine to one run
        rl.encodeCompleteOffset(10, 7, 20);
        assertOffsetsAndRuns(rl,
                of(16, 17, 20, 30),
                of(6, 2, 2, 1, 9, 1));

        // down by one, continuous so combine
        rl.encodeCompleteOffset(10, 5, 20);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30),
                of(5, 3, 2, 1, 9, 1));

        // add a big gap maker
        rl.encodeCompleteOffset(10, 35, 45);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30, 45),
                of(5, 3, 2, 1, 9, 1, 14, 1));

        // off by one higher, continuous so combine
        rl.encodeCompleteOffset(10, 36, 46);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30, 45, 46),
                of(5, 3, 2, 1, 9, 1, 14, 2));

        // off by one gap
        rl.encodeCompleteOffset(10, 38, 48);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30, 45, 46, 48),
                of(5, 3, 2, 1, 9, 1, 14, 2, 1, 1));
    }

    private void assertOffsetsAndRuns(final RunLengthEncoder rl, List<Integer> goodOffsets, List<Integer> runs) {
        assertOffsetsAndRuns(rl, Offsets.fromInts(goodOffsets), runs.stream().map(Integer::longValue).collect(Collectors.toList()));
    }

    private void assertOffsetsAndRuns(final RunLengthEncoder rl, Offsets goodOffsets, List<Long> expectedLongRuns) {
        assertThat(rl.calculateFullRelativeRunLengths())
                .as("run lengths")
                .containsExactlyElementsOf(expectedLongRuns);

        assertThat(rl.calculateSucceededActualOffsets()).as("succeeded Offsets")
                .containsExactlyElementsOf(goodOffsets.getRawOffsets());
    }

    /**
     * Has to combine 3 run lengths into 1, both from above and below
     */
    @Test
    void segmentTestThree() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(10, 20, 20);
        assertOffsetsAndRuns(rl,
                of(30),
                of(20, 1));

        rl.encodeCompleteOffset(10, 14, 20);
        assertOffsetsAndRuns(rl,
                of(24, 30),
                of(14, 1, 5, 1));

        rl.encodeCompleteOffset(10, 16, 20);
        assertOffsetsAndRuns(rl,
                of(24, 26, 30),
                of(14, 1, 1, 1, 3, 1));

        //
        rl.encodeCompleteOffset(10, 15, 20);
        assertOffsetsAndRuns(rl,
                of(24, 25, 26, 30),
                of(14, 3, 3, 1));
    }

    /**
     * Has to combine 2 run lengths into 1
     */
    @Test
    void segmentTestFour() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 5, 5);
        assertOffsetsAndRuns(rl,
                of(5),
                of(5, 1));

        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(5, 6),
                of(5, 2));
    }

    /**
     * Has to combine 2 run lengths into 1. Over
     */
    @Test
    void segmentTestFiveOver() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 4);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 5, 5);
        assertOffsetsAndRuns(rl,
                of(4, 5),
                of(4, 2));
    }

    /**
     * Has to combine 2 run lengths into 1. Under
     */
    @Test
    void segmentTestFiveUnder() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 4);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 3, 4);
        assertOffsetsAndRuns(rl,
                of(3, 4),
                of(3, 2));
    }

    /**
     * Has to combine 2 run lengths into 1. Over
     */
    @Test
    void segmentTestFiveOverMultiple() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 5);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 5, 5);
        assertOffsetsAndRuns(rl,
                of(4, 5),
                of(4, 2));

        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6),
                of(4, 3));

        rl.encodeCompleteOffset(0, 7, 7);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6, 7),
                of(4, 4));
    }

    /**
     * Has to combine both up and down
     */
    @Test
    void segmentTestFixUpAndDownSimpleUpwards() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 4);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(4, 6),
                of(4, 1, 1, 1));

        rl.encodeCompleteOffset(0, 5, 6);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6),
                of(4, 3));
    }

    /**
     * Has to combine both up and down
     */
    @Test
    void segmentTestFixUpAndDownSimpleDownwards() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(6),
                of(6, 1));

        //
        rl.encodeCompleteOffset(0, 4, 6);
        assertOffsetsAndRuns(rl,
                of(4, 6),
                of(4, 1, 1, 1));

        rl.encodeCompleteOffset(0, 5, 6);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6),
                of(4, 3));
    }

    @Test
    void segmentTestThick() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 6, 6);
        rl.encodeCompleteOffset(0, 7, 7);
        rl.encodeCompleteOffset(0, 9, 9);
        assertOffsetsAndRuns(rl,
                of(6, 7, 9),
                of(6, 2, 1, 1));

        //
        rl.encodeCompleteOffset(0, 4, 9);
        assertOffsetsAndRuns(rl,
                of(4, 6, 7, 9),
                of(4, 1, 1, 2, 1, 1));

        rl.encodeCompleteOffset(0, 3, 9);
        assertOffsetsAndRuns(rl,
                of(3, 4, 6, 7, 9),
                of(3, 2, 1, 2, 1, 1));

        rl.encodeCompleteOffset(0, 5, 9);
        assertOffsetsAndRuns(rl,
                of(3, 4, 5, 6, 7, 9),
                of(3, 5, 1, 1));
    }
}
