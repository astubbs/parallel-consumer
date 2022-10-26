package io.confluent.parallelconsumer.offsets;

import lombok.ToString;

import java.util.BitSet;
import java.util.stream.Stream;

/**
 * A wrapper of {@link java.util.BitSet} which enables us to virtually truncate the starting offset up.
 *
 * @author Antony Stubbs
 */
@ToString(onlyExplicitlyIncluded = true)
public class BitSetFragment {

    private final long lowOffset;

    private final long highOffsetInclusive;

    /**
     * The wrapped bitset
     */
    private final BitSet wrapped;

    public BitSetFragment(final long lowOffset, final long highOffsetInclusive) {
        this.lowOffset = lowOffset;
        this.highOffsetInclusive = highOffsetInclusive;
        var capacity = Math.toIntExact(highOffsetInclusive - lowOffset + 1);
        this.wrapped = new BitSet(capacity);
    }

    public byte[] toByteArray() {
        return wrapped.toByteArray();
    }

    /**
     * How many offsets it can represent
     */
    public long relativeOffsetRangeSize() {
        return highOffsetInclusive - lowOffset + 1;
    }

    /**
     * @see BitSet#set(int)
     */
    public void set(int relativeOffset) {
        wrapped.set(relativeOffset);
    }

    /**
     * Highest possible offset representable
     */
    public long getOffsetCapacity() {
        return highOffsetInclusive;
    }


    /**
     * Like BitSet#stream(), but with relative offsets
     *
     * @see BitSet#stream()
     */
    public Stream<Long> stream() {
        return this.wrapped.stream().boxed()
                // to absolute offset
                .map(bitsetIndex -> Math.addExact(bitsetIndex, lowOffset));
    }
}
