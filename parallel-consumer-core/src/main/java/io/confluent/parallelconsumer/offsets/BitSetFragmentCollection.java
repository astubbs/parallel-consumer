package io.confluent.parallelconsumer.offsets;

import io.confluent.parallelconsumer.state.PartitionState;
import lombok.Value;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.lang.Math.toIntExact;

/**
 * Abstraction on a {@link java.util.BitSet} which enables us to cobble together (daisy chain) many BitSets into a
 * single view. This is done to prevent having to rebuild the underlying bitset everytime the capacity requirements
 * change, or the lower bound moves up.
 *
 * @author Antony Stubbs
 */
public class BitSetFragmentCollection {

    private final LinkedHashMap<MyHighestOffset, BitSetFragment> fragments = new LinkedHashMap<>();

    private BitSetFragment highestFragmentCache;

    /**
     * Makes sure we have the capcity to represent the given offsets, by potentially adding new
     * {@link BitSetFragment}s.
     */
    public void ensureCapacity(long base, long newHighestSeen) {
        long offsetCapacity = getOffsetCapacity();
//        long newHighestSeen = recordsAndEpoch.getLastOffset();

        if (newHighestSeen > offsetCapacity) {
            // we need to add a new fragment
            long newLowOffset = offsetCapacity + 1;
            highestFragmentCache = new BitSetFragment(newLowOffset, newHighestSeen);
            var newFragmentKey = new MyHighestOffset(newHighestSeen);
            fragments.put(newFragmentKey, highestFragmentCache);
        }
    }

    private long getOffsetCapacity() {
        BitSetFragment bsf = getHighestFragmentCache();
        return bsf == null ? PartitionState.KAFKA_OFFSET_ABSENCE : bsf.getOffsetCapacity();
    }

    private BitSetFragment getHighestFragmentCache() {
        return highestFragmentCache;
    }

    /**
     * @see BitSet#toByteArray()
     */
    public byte[] toByteArray() throws IOException {
        long totalSize = calculateTotalFragmentSize();
        final int intSize = toIntExact(totalSize);
        ByteArrayOutputStream totalByteStream = new ByteArrayOutputStream(intSize);
        for (BitSetFragment fragment : fragments.values()) {
            byte[] b = fragment.toByteArray();
            totalByteStream.write(b);
        }
        return totalByteStream.toByteArray();
    }

    private long calculateTotalFragmentSize() {
        return fragments.values().stream()
                .mapToLong(BitSetFragment::relativeOffsetRangeSize)
                .sum();
    }

//    /**
//     * @see BitSet#set(int)
//     */
//    public void set(int relativeOffset) {
//        BitSetFragment bsf = getFragmentWithHieghestOffsetKey(relativeOffset);
//        bsf.set(relativeOffset);
//    }

    /**
     * Must call {@link #ensureCapacity} first.
     */
    public void set(long newBaseOffset, long relativeOffset) {
        final long offset = newBaseOffset + relativeOffset;
        Optional<BitSetFragment> bsf = findFragmentForAbsoluteOffset(offset);
        if (bsf.isPresent()) {
            bsf.get().set(Math.toIntExact(relativeOffset));
        } else {
            throw new IllegalStateException("No fragment found for offset " + offset);
        }
    }

    private Optional<BitSetFragment> findFragmentForAbsoluteOffset(long offset) {
        // todo - can make o1?
        for (var fragmentEntry : fragments.values()) {
            if (fragmentEntry.offsetWithinRange(offset)) {
                return Optional.of(fragmentEntry);
            }
        }
        return Optional.empty();
    }

    /**
     * @return the BitSitFragment which contains the given relative offset
     */
    private BitSetFragment getFragmentWithHieghestOffsetKey(MyHighestOffset key) {
        return fragments.get(key);
    }

    /**
     * Like BitSet#stream(), but with relative offsets
     *
     * @see BitSet#stream()
     */
    public List<Long> toArray() {
        return this.fragments.values().stream()
                .flatMapToLong(BitSetFragment::stream)
                .boxed()
                .collect(Collectors.toList());
    }

    public long calculateTotalOffsetEntries() {
        return this.fragments.values().stream()
                .mapToLong(BitSetFragment::relativeOffsetRangeSize)
                .sum();
    }

    public LongStream stream() {
        return fragments.values().stream()
                .flatMapToLong(BitSetFragment::stream);
    }

    @Value
    private class MyHighestOffset {
        //        @Delegate
        Long offset;
    }
}
