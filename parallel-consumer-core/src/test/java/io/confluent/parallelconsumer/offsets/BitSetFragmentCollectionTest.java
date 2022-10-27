package io.confluent.parallelconsumer.offsets;

import com.google.common.truth.Truth;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.offsets.BitSetFragment.MIN_FRAGMENT_SIZE_BITS;

/**
 * @author Antony Stubbs
 * @see BitSetFragmentCollection
 */
class BitSetFragmentCollectionTest {

    final BitSetFragmentCollection bsfc = new BitSetFragmentCollection(0L, 0L);

    /**
     *
     */
    private final long OFFSET_OVER_FRAGMENT_SIZE = 14L;

    BitSetFragmentCollectionTest() throws BitSetEncodingNotSupportedException {
    }

    //    @Test
    void ensureCapacity() {
    }

    //    @Test
    void toByteArray() {
    }

    @SneakyThrows
    @Test
    void setOverFragmentSize() {
        final long newBase = 4L;
        final long offsetToSet = MIN_FRAGMENT_SIZE_BITS + OFFSET_OVER_FRAGMENT_SIZE;

        //
        bsfc.set(newBase, offsetToSet);
        assertThat(bsfc).hasToArray().containsExactly(offsetToSet);

        //
        bsfc.set(newBase, offsetToSet + OFFSET_OVER_FRAGMENT_SIZE);
        assertThat(bsfc).hasToArray().containsExactly(offsetToSet, offsetToSet + OFFSET_OVER_FRAGMENT_SIZE);
    }

    /**
     * todo docs
     */
    @SneakyThrows
    @Test
    void setUnderFragmentSize() {
        final long newBase = 4L;
        final long offsetToSet = MIN_FRAGMENT_SIZE_BITS - OFFSET_OVER_FRAGMENT_SIZE;
        bsfc.set(newBase, offsetToSet);
        assertThat(bsfc).hasToArray().containsExactly(offsetToSet);
    }

    /**
     * todo docs
     */
    @SneakyThrows
    @Test
    void truncateWithMultipleFragments() {
        final long newBase = 4L;
        final long offsetToSet = MIN_FRAGMENT_SIZE_BITS + OFFSET_OVER_FRAGMENT_SIZE;

        //
        bsfc.set(newBase, offsetToSet);
        assertThat(bsfc).hasToArray().containsExactly(offsetToSet);

        //
        bsfc.set(newBase, offsetToSet + OFFSET_OVER_FRAGMENT_SIZE);
        assertThat(bsfc).hasToArray().containsExactly(offsetToSet, offsetToSet + OFFSET_OVER_FRAGMENT_SIZE);
    }

    /**
     * todo docs
     */
    @SneakyThrows
    @Test
    void truncateUnder() {
        bsfc.set(0L, 2L);
        assertThat(bsfc).hasToArray().containsExactly(2L);
        assertThat(bsfc).hasBaseOffsetEqualTo(0L);
        BitSetFragment baseFragment = bsfc.getBaseFragment().get();
//        assertThat(baseFragment).

        bsfc.set(1L, 3L);
        assertThat(bsfc).hasToArray().containsExactly(2L, 3L);
        assertThat(bsfc).hasBaseOffsetEqualTo(1L);

        bsfc.set(1L, 5L);
        assertThat(bsfc).hasToArray().containsExactly(2L, 3L, 5L);
        assertThat(bsfc).hasBaseOffsetEqualTo(1L);

        final long newBase = 4L;
        final long offsetToSet = MIN_FRAGMENT_SIZE_BITS - OFFSET_OVER_FRAGMENT_SIZE;

        bsfc.set(newBase, offsetToSet);
        assertThat(bsfc).hasToArray().containsExactly(5L, offsetToSet);
        assertThat(bsfc).hasBaseOffsetEqualTo(newBase);

        bsfc.set(12L, offsetToSet + 2);
        assertThat(bsfc).hasToArray().containsExactly(offsetToSet, offsetToSet + 2);
        assertThat(bsfc).hasBaseOffsetEqualTo(12L);
    }

    /**
     * As the base offset moves up, fragments that no longer cover the base offset are removed.
     */
    @Test
    @SneakyThrows
    void droppingStaleFragments() {
        bsfc.ensureCapacity(0L, MIN_FRAGMENT_SIZE_BITS);
        bsfc.ensureCapacity(0L, MIN_FRAGMENT_SIZE_BITS * 2);
        bsfc.ensureCapacity(0L, MIN_FRAGMENT_SIZE_BITS * 3);
        Truth.assertThat(bsfc.getNumberOfFragments()).isAtLeast(3);


        final long truncateTo = MIN_FRAGMENT_SIZE_BITS * 3 - 4;
        final long highSet = MIN_FRAGMENT_SIZE_BITS * 3 - 2;
        bsfc.set(truncateTo, highSet);

        //
        assertThat(bsfc).getBaseOffset().isEqualTo(truncateTo);
        Truth.assertThat(bsfc.getNumberOfFragments()).isAtLeast(1);
        assertThat(bsfc).hasToArray().containsExactly(highSet);
    }

}