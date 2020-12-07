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

            encodePattern(rl);

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactly(2, 2, 1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(2L, 3L, 5L, 6L, 10L, 14L, 15L, 16L, 17L);

            rl.truncateRunlengths(12);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(2, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(14L, 15L, 16L, 17L);
        }

        //v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            rl.truncateRunlengths(4);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            rl.truncateRunlengths(8);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(2, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            rl.truncateRunlengths(9);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(1, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }
    }

    @Test
    void truncateV2() {
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getStartOffset).containsExactly(0, 2, 4, 5, 7, 10, 11, 14);
            assertThat(rl.getRunLengthEncodingIntegers()).containsExactly(2, 2, 1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(2L, 3L, 5L, 6L, 10L, 14L, 15L, 16L, 17L);

            rl.truncateRunlengthsV2(12);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(2, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(14L, 15L, 16L, 17L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            rl.truncateRunlengthsV2(4);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            rl.truncateRunlengthsV2(8);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(2, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl);

            rl.truncateRunlengthsV2(9);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(1, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }
    }

    private void encodePattern(final RunLengthEncoder rl) {
        long base = 0L;
        int highest = 10;
        int relative = 0;
        {
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompletedOffset(base, relative, highest);
            relative++;
            rl.encodeCompletedOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompletedOffset(base, relative, highest);
            relative++;
            rl.encodeCompletedOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompletedOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompletedOffset(base, relative, highest);
            relative++;
            rl.encodeCompletedOffset(base, relative, highest);
            relative++;
            rl.encodeCompletedOffset(base, relative, highest);
            relative++;
            rl.encodeCompletedOffset(base, relative, highest);


            rl.addTail();
        }
    }
}
