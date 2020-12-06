package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

public class BitSetEncoderTest {

    @SneakyThrows
    @Test
    void general() {
        long highest = 0L;
        long base = 0;

        BitsetEncoder o = new BitsetEncoder(0, 0, new OffsetSimultaneousEncoder(0, 0L));

        // offset 0 is missing

        highest++;
        o.encodeCompletedOffset(base, highest, highest);
        {
            long[] actual = o.bitSet.stream().asLongStream().toArray();
            assertThat(actual).doesNotContain(0).contains(1);
        }

        {
            highest++;
            o.encodeCompletedOffset(base, highest, highest);
            long[] actual = o.bitSet.stream().asLongStream().toArray();
            assertThat(actual).doesNotContain(0).contains(1, 2);
        }


        {
            highest++;
            highest++;
            o.encodeCompletedOffset(base, highest, highest);
            long[] actual = o.bitSet.stream().asLongStream().toArray();
            assertThat(actual).doesNotContain(0).contains(1, 2, 4);
        }
    }


    @SneakyThrows
    @Test
    void maybeReinitalise() {
        long highest = 0L;
        long base = 0;

        BitsetEncoder o = new BitsetEncoder(0, 0, new OffsetSimultaneousEncoder(0, 0L));

        //
        o.encodeCompletedOffset(base, highest, highest);

        highest++;
        o.encodeCompletedOffset(base, highest, highest);
        {
            long[] actual = o.bitSet.stream().asLongStream().toArray();
            assertThat(actual).contains(1);
        }

        {
            highest++;
            o.maybeReinitialise(base, highest);
            long[] actual = o.bitSet.stream().asLongStream().toArray();
            assertThat(actual).as("still contains it's information").contains(1);
        }
    }
}
