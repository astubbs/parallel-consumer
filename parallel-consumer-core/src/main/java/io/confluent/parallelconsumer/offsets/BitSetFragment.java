package io.confluent.parallelconsumer.offsets;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A wrapper of {@link java.util.BitSet} which enables us to virtually truncate the starting offset up.
 *
 * @author Antony Stubbs
 * @see BitSetFragmentCollection
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class BitSetFragment {


    /**
     * todo docs
     */
    //todo change to 100
    public static final int MIN_FRAGMENT_SIZE_BITS = 10 * Byte.SIZE;

    /**
     * todo docs
     */
    @ToString.Include
    // todo make protected - TG limitation
    @Getter(AccessLevel.PUBLIC)
    private long virtualBaseOffsetView;

    /**
     *
     */
    @ToString.Include
    // todo make protected - TG limitation
    @Getter(AccessLevel.PUBLIC)
    private final long offsetBitsetStartsAt;

    /**
     * todo docs
     */
    @ToString.Include
    @Getter(AccessLevel.PROTECTED)
    private final long highOffsetInclusive;

    /**
     * The wrapped bitset.
     * <p>
     * todo Each should have a BitSet should have a word size cleanly divisible by Byte.SIZE  so that it fits perfectly so our indexes line up.
     */
    @ToString.Include
    private final BitSet wrappedBitset;

    public BitSetFragment(final long baseOffset, final long highOffsetInclusive) throws BitSetEncodingNotSupportedException {
        this.virtualBaseOffsetView = baseOffset;
        this.offsetBitsetStartsAt = baseOffset;
        var rangeNeeded = highOffsetInclusive - baseOffset;
        try {
            var capacity = Math.toIntExact(rangeNeeded);
            var roundedUp = roundUpToDivisibleByByte(capacity);
            var capacityToUse = Math.max(roundedUp, MIN_FRAGMENT_SIZE_BITS);
            this.highOffsetInclusive = capacityToUse + baseOffset - 1;
            this.wrappedBitset = new BitSet(capacityToUse);
        } catch (ArithmeticException e) {
            throw new BitSetEncodingNotSupportedException("Unable to encode offset range " + baseOffset + " to " + highOffsetInclusive + " as it is too large for a BitSet");
        }
    }

    private int roundUpToDivisibleByByte(int capacity) {
        final double numBytesInCapacity = ((double) capacity) / Byte.SIZE;
        final double roundedUp = Math.ceil(numBytesInCapacity);
        return (int) roundedUp * Byte.SIZE;
    }

    /**
     * How many offsets it can represent
     */
    public long relativeOffsetRangeSize() {
        return highOffsetInclusive - virtualBaseOffsetView + MIN_FRAGMENT_SIZE_BITS;
    }

    /**
     * @see BitSet#set(int)
     */
    public void set(long offset) throws BitSetEncodingNotSupportedException {
        if (offset < virtualBaseOffsetView || offset > highOffsetInclusive) {
            throw new BitSetEncodingNotSupportedException("Offset " + offset + " is lower than the lowest offset in this fragment " + virtualBaseOffsetView);
        }

        long bitsetIndexToSetPositive = offset - offsetBitsetStartsAt;
        try {
            wrappedBitset.set(Math.toIntExact(bitsetIndexToSetPositive));
        } catch (ArithmeticException e) {
            throw new BitSetEncodingNotSupportedException("Unable to encode offset " + offset + " as it is too large for a BitSet");
        }
    }

    /**
     * Highest possible offset representable
     */
    public long getHighestOffsetCanStore() {
        return highOffsetInclusive;
    }

    /**
     * Like BitSet#stream(), but with absolute offsets
     *
     * @see BitSet#stream()
     */
    public LongStream stream() {
        return this.wrappedBitset.stream().boxed()
                // to absolute offset
                .mapToLong(bitsetRelativeIndex -> {
                    final long actualOffset = Math.addExact(bitsetRelativeIndex, offsetBitsetStartsAt);
                    return actualOffset;
                })
                // only return offsets that are within our current view's lower bound
                .filter(absoluteOffset -> absoluteOffset >= virtualBaseOffsetView);
    }

    /**
     * Like BitSet#stream(), but with absolute offsets
     *
     * @see BitSet#stream()
     */
    public List<Long> toArray() {
        final List<Long> array = stream()
                .boxed()
                .collect(Collectors.toList());
        return array;
    }

    /**
     * todo docs
     * <p>
     * Uses {@link BitSet#get} which iterates over the entries and creates a new bitset, but we would need to iterate anyway to filter out low offsets
     */
    public byte[] toByteArray() {
        var index = getBitsetIndexFromOffset(virtualBaseOffsetView);
        return wrappedBitset.get(index, getBitsetIndexFromOffset(highOffsetInclusive)).toByteArray();
    }

    /**
     * todo docs
     */
    private int getBitsetIndexFromOffset(long virtualLowOffsetView) {
        return Math.toIntExact(virtualLowOffsetView - offsetBitsetStartsAt);
    }

    public boolean offsetWithinRange(long offset) {
        return offset >= virtualBaseOffsetView && offset <= highOffsetInclusive;
    }

    /**
     * Virtual truncation, O(1) operation.
     */
    public void maybeTruncate(long newBaseOffset) {
        if (newBaseOffset > virtualBaseOffsetView) {
            log.debug("Truncating bitset fragment from {} to {}", virtualBaseOffsetView, newBaseOffset);
            setNewBaseOffset(newBaseOffset);
        }
    }

    /**
     * todo docs
     */
    private void setNewBaseOffset(long newBaseOffset) {
        this.virtualBaseOffsetView = newBaseOffset;
    }

    /**
     * todo docs
     */
    public long getNumberOfOffsetsCanStore() {
        return getHighOffsetInclusive() - getVirtualBaseOffsetView() + 1;
    }
}
