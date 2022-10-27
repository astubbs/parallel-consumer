package io.confluent.parallelconsumer.offsets;

import io.confluent.parallelconsumer.state.PartitionState;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

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
 * <p>
 * Enables us to map to offset ranges larger than a single bitset can handle.
 *
 * @author Antony Stubbs
 * @see BitSetFragment
 */
@Slf4j
@ToString
public class BitSetFragmentCollection {

    /**
     * Due to the constructor ensuring at least a minimum capacity of zero, we will always have at least one fragment.
     */
    private final LinkedHashMap<MyHighestOffset, BitSetFragment> fragments = new LinkedHashMap<>();

    private BitSetFragment highestFragmentCache;

    public BitSetFragmentCollection(long baseOffset, long length) throws BitSetEncodingNotSupportedException {
        var sizeToEnsure = Math.max(length, BitSetFragment.MIN_FRAGMENT_SIZE_BITS);
        ensureCapacity(baseOffset, sizeToEnsure);
    }

    /**
     * Makes sure we have the capacity to represent the given offsets, by potentially adding new
     * {@link BitSetFragment}s.
     * <p>
     * This is also run on demand from {@link #set}, but calling it in advance when you know the range you will need,
     * will allow for more effective packing of BitSet fragments.
     */
    public void ensureCapacity(long base, long newHighestSeen) throws BitSetEncodingNotSupportedException {
        if (newHighestSeen > getHighestOffsetCanStore()) {
            increaseCapacity(newHighestSeen);
        }
    }

    private void increaseCapacity(long newHighestSeen) throws BitSetEncodingNotSupportedException {
        // we need to add a new fragment
        long newLowOffset = getHighestOffsetCanStore() + 1;

        final BitSetFragment newFragment = new BitSetFragment(newLowOffset, newHighestSeen);

        log.debug("Increasing capacity to {} (currently {}) - adding new fragment: {}",
                newHighestSeen,
                getHighestOffsetCanStore(),
                newFragment);

        // update our cache with the new fragment
        highestFragmentCache = newFragment;

        // add to the collection
        final long highOffsetOfFragment = highestFragmentCache.getHighestOffsetInclusiveContainable();
        var newFragmentKey = new MyHighestOffset(highOffsetOfFragment);
        fragments.put(newFragmentKey, highestFragmentCache);
    }

    private long getHighestOffsetCanStore() {
        BitSetFragment bsf = getHighestFragmentCache();
        return bsf == null ? PartitionState.KAFKA_OFFSET_ABSENCE : bsf.getHighestOffsetCanStore();
//        return bsf.getOffsetCapacity();
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
                .mapToLong(BitSetFragment::relativeOffsetStorableRangeSize)
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
     * <p>
     * If the  {@code newBaseOffset} is higher than the current base, then the underlying bitset will be truncated.
     *
     * @param newBaseOffset the current base offset that the bitset must start encoding from
     * @param offset        the absolute kafka offset to set positive
     */
    public void set(long newBaseOffset, long offset) throws BitSetEncodingNotSupportedException {
        log.trace("Setting offset {} with base offset {}", offset, newBaseOffset);
        // must ensure capacity before truncating, in case truncation mark is over capacity
        ensureCapacity(newBaseOffset, offset);

        // truncate
        maybeTruncate(newBaseOffset);

        Optional<BitSetFragment> bsf = findFragmentForAbsoluteOffset(offset);
        if (bsf.isPresent()) {
            bsf.get().set(offset);
        } else {
            throw new IllegalStateException("No fragment found for offset " + offset);
        }
    }

    /**
     * Virtual truncation, O(1) operation.
     */
    private void maybeTruncate(long newBaseOffset) {
        if (newBaseOffset > getBaseOffset()) {
            // find new active fragment based in new base offset
            Optional<BitSetFragment> newActiveFragment = findFragmentForAbsoluteOffset(newBaseOffset);
            // truncate to it
            if (newActiveFragment.isPresent()) {
                BitSetFragment activeFragment = newActiveFragment.get();
                activeFragment.maybeTruncate(newBaseOffset);
            } else {
                throw new IllegalStateException("No fragment found for offset " + newBaseOffset);
            }
            // drop previous segments
            fragments.values().removeIf(bsf -> {
                if (bsf.getVirtualBaseOffsetView() < newBaseOffset) {
                    log.trace("Truncating {} to base offset {}", bsf, newBaseOffset);
                    return true;
                } else {
                    return false;
                }
            });
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
    private BitSetFragment getFragmentWithHighestOffsetKey(MyHighestOffset key) {
        return fragments.get(key);
    }

    /**
     * Like BitSet#stream(), but with absolute offsets
     *
     * @see BitSet#stream()
     */
    public List<Long> toArray() {
        final List<Long> array = this.fragments.values().stream()
                .flatMapToLong(BitSetFragment::stream)
                .boxed()
                .collect(Collectors.toList());
        return array;
    }

    /**
     * todo docs
     */
    public long calculateTotalOffsetsRepresented() {
        final long low = getBaseFragment().getVirtualBaseOffsetView();
        final long high = getHighestFragmentCache().getHighestSucceededOffset();
        return high - low + 1;
    }

    /**
     * Like BitSet#stream(), but with absolute offsets
     *
     * @see BitSet#stream()
     */
    public LongStream stream() {
        return fragments.values().stream()
                .flatMapToLong(BitSetFragment::stream);
    }

    /**
     * The offset which this bitset starts tracking from
     */
    // visible from protected for TG generation
    public long getBaseOffset() {
        return getBaseFragment().getVirtualBaseOffsetView();
    }

    protected BitSetFragment getBaseFragment() {
        return fragments.values().iterator().next();
    }

    public int getNumberOfFragments() {
        return fragments.size();
    }

    @Value
    private class MyHighestOffset {
        //        @Delegate
        Long offset;
    }
}
