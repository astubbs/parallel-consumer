package io.confluent.parallelconsumer.offsets;

import com.google.common.truth.Truth;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;

/**
 * @author Antony Stubbs
 */
class BitSetFragmentTest {

    @SneakyThrows
    @Test
    void maybeTruncate() {
        var f = new BitSetFragment(0L, 0L);

        assertThat(f).hasToArray().containsExactly();

        f.set(2L);
        assertThat(f).hasToArray().containsExactly(2L);

        f.maybeTruncate(1L);
        assertThat(f).hasToArray().containsExactly(2L);
        assertThat(f).getVirtualBaseOffsetView().isEqualTo(1L);

        f.set(3L);
        assertThat(f).hasToArray().containsExactly(2L, 3L);
        assertThat(f).getVirtualBaseOffsetView().isEqualTo(1L);


        f.set(5L);
        f.set(7L);
        assertThat(f).getVirtualBaseOffsetView().isEqualTo(1L);

        f.maybeTruncate(6L);
        assertThat(f).getVirtualBaseOffsetView().isEqualTo(6L);
        assertThat(f).hasToArray().containsExactly(7L);
    }

    @SneakyThrows
    @Test
    void offByOneSize() {
        var f = new BitSetFragment(0L, 0L);
        final int highestOffset = BitSetFragment.MIN_FRAGMENT_SIZE_BITS - 1;
        assertThat(f).getHighestOffsetCanStore().isEqualTo(highestOffset);
        Truth.assertThat(f.getNumberOfOffsetsCanStore()).isEqualTo(BitSetFragment.MIN_FRAGMENT_SIZE_BITS);
    }
}