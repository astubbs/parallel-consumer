package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.RunLengthEncoder.RunLengthEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RunLengthEncoderTest {

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
    @Test
    void truncateV1() {
        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactly(2, 2, 1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(12L, 13L, 15L, 16L, 20L, 24L, 25L, 26L, 27L);

            rl.truncateRunlengths(12);

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactly(2, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(14L, 15L, 16L, 17L);
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

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getStartOffset).containsExactly(10, 12, 14, 15, 17, 20, 21, 24);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(12L, 13L, 15L, 16L, 20L, 24L, 25L, 26L, 27L);

            rl.truncateRunlengthsV2(22);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(2, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(24L, 25L, 26L, 27L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengthsV2(4);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengthsV2(8);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(2, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengthsV2(9);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(1, 1, 3, 4);
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


            rl.addTail();
        }
    }

    @Test
    void v2TruncateOverMax() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeIncompleteOffset(0, 0, 0);
        rl.encodeCompleteOffset(0, 1, 1);

        rl.addTail();
        rl.truncateRunlengthsV2(2);

        assertThat(rl.runLengthOffsetPairs).isEmpty();
        assertThat(rl.calculateSucceededActualOffsets()).isEmpty();
    }
}
