package io.confluent.parallelconsumer.offsets;

import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;

/**
 * Abstraction on a {@link java.util.BitSet} which enables us to cobble together (daisy chain) many BitSets into a
 * single view. This is done to prevent having to rebuild the underlying bitset everytime the capacity requirements
 * change, or the lower bound moves up.
 *
 * @author Antony Stubbs
 */
public class BitSetFragmentCollection {

    private final LinkedList<BitSetFragment> fragments = new LinkedList<>();

    /**
     * Makes sure we have the capcity to represent the given offsets, by potentially adding new
     * {@link BitSetFragment}s.
     */
    public void ensureCapacity(EpochAndRecordsMap.RecordsAndEpoch recordsAndEpoch) {
        long offsetCapacity = getOffsetCapacity();
        long newHighestSeen = recordsAndEpoch.getLastOffset();

        if (newHighestSeen > offsetCapacity) {
            // we need to add a new fragment
            long newLowOffset = offsetCapacity + 1;
            BitSetFragment newFragment = new BitSetFragment(newLowOffset, newHighestSeen);
            fragments.add(newFragment);
        }
    }

    private long getOffsetCapacity() {
        BitSetFragment bsf = getLastFragement();
        return bsf.getOffsetCapacity();
    }

    private BitSetFragment getLastFragement() {
        return this.fragments.getLast();
    }

    /**
     * @see BitSet#toByteArray()
     */
    public byte[] toByteArray() throws IOException {
        long totalSize = calculateTotaFragmentSize();
        final int intSize = toIntExact(totalSize);
        ByteArrayOutputStream totalByteStream = new ByteArrayOutputStream(intSize);
        for (BitSetFragment fragment : fragments) {
            byte[] b = fragment.toByteArray();
            totalByteStream.write(b);
        }
        return totalByteStream.toByteArray();
    }

    private long calculateTotaFragmentSize() {
        return fragments.stream()
                .mapToLong(BitSetFragment::relativeOffsetRangeSize)
                .sum();
    }

    /**
     * @see BitSet#set(int)
     */
    public void set(int relativeOffset) {
        BitSetFragment bsf = getFragmentForRelativeOffset(relativeOffset);
        bsf.set(relativeOffset);
    }

    /**
     * @return the BitSitFragment which contains the given relative offset
     */
    private BitSetFragment getFragmentForRelativeOffset(int relativeOffset) {
        return null;
    }

    /**
     * Like BitSet#stream(), but with relative offsets
     *
     * @see BitSet#stream()
     */
    public List<Long> toArray() {
        return this.fragments.stream()
                .flatMap(BitSetFragment::stream)
                .collect(Collectors.toList());
    }
}
