package io.confluent.parallelconsumer;

import io.confluent.csid.utils.JavaUtils;
import io.confluent.csid.utils.Range;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;

import static io.confluent.csid.utils.JavaUtils.safeCast;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.*;

/**
 * Always starts with failed or incomplete offsets. todo docs tail runlength?
 */
@ToString(onlyExplicitlyIncluded = true, callSuper = true)
@Slf4j
class RunLengthEncoder extends OffsetEncoderBase {

    /**
     * The current run length being counted / built
     */
    private int currentRunLengthCount;

    private int previousRelativeOffsetFromBase;

    private boolean previousRunLengthState;

    /**
     * Stores all the run lengths
     */
    @Getter
    private List<Integer> runLengthEncodingIntegers;

    private Optional<byte[]> encodedBytes = Optional.empty();

    @ToString.Include
    private final Version version; // default to new version

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(baseOffset, offsetSimultaneousEncoder);
        version = newVersion;

        init();
    }

    private void init() {
        runLengthEncodingIntegers = new ArrayList<>();
        runLengthOffsetPairs = new TreeSet<>();
        currentRunLengthCount = 0;
        previousRelativeOffsetFromBase = 0;
        previousRunLengthState = false;
    }

    private void reset() {
        log.debug("Resetting");
        init();
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return switch (version) {
            case v1 -> RunLength;
            case v2 -> RunLengthV2;
        };
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return switch (version) {
            case v1 -> RunLengthCompressed;
            case v2 -> RunLengthV2Compressed;
        };
    }

    @Override
    public void encodeIncompleteOffset(final int relativeOffset) {
        encodeRunLength(false, relativeOffset);
    }

    @Override
    public void encodeCompletedOffset(final int relativeOffset) {
        encodeRunLength(true, relativeOffset);
    }

    @Override
    public byte[] serialise() throws EncodingNotSupportedException {
        addTail();

        int entryWidth = getEntryWidth();

        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(getSize() * entryWidth);


        //for (final Integer run : getRunLengthEncodingIntegers()) {
        for (final RunLengthEntry n : runLengthOffsetPairs) {
            Integer run = n.runLength;
            switch (version) {
                case v1 -> {
                    final short shortCastRunlength = run.shortValue();
                    if (run != shortCastRunlength)
                        throw new RunlengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", run, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(run);
                }
            }
        }

        byte[] array = runLengthEncodedByteBuffer.array();
        encodedBytes = Optional.of(array);
        return array;
    }

    private int getSize() {
        //return runLengthEncodingIntegers.size();
        return runLengthOffsetPairs.size();
    }

    /**
     * Add the dangling in flight run to the list, done before serialising
     */
    void addTail() {
        RunLengthEntry previous = runLengthOffsetPairs.last();
        addRunLength(currentRunLengthCount, previous.startOffset + previous.runLength);
    }

    private int getEntryWidth() {
        return switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
    }
//
//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset) {
//asdf
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset) {
//asdf
//    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        // noop
    }


    @Override
    public void encodeCompletedOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
        maybeReinitialise(newBaseOffset, currentHighestCompleted);

        encodeCompletedOffset(safeCast(relativeOffset));
    }

    @Override
    public void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) {
        boolean reinitialise = false;

        long newLength = currentHighestCompleted - newBaseOffset;
//        if (originalLength != newLength) {
////        if (this.highestSuceeded != currentHighestCompleted) {
//            log.debug("Length of Bitset changed {} to {}",
//                    originalLength, newLength);
//            reinitialise = true;
//        }

        if (originalBaseOffset != newBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work",
                    this.originalBaseOffset, newBaseOffset);
            reinitialise = true;
        }

        if (newBaseOffset < originalBaseOffset)
            throw new InternalRuntimeError("");

        if (reinitialise) {
            reinitialise(newBaseOffset, currentHighestCompleted);
        }
    }

    private void reinitialise(final long newBaseOffset, final long currentHighestCompleted) {
//        long longDelta = newBaseOffset - originalBaseOffset;
//        int baseDelta = JavaUtils.safeCast(longDelta);
        truncateRunlengthsV2(JavaUtils.safeCast(newBaseOffset));


//        currentRunLengthCount = 0;
//        previousRelativeOffsetFromBase = 0;
//        previousRunLengthState = false;

        enable();
    }

    NavigableSet<RunLengthEntry> runLengthOffsetPairs = new TreeSet<>();
//

    @Value
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class RunLengthEntry implements Comparable<RunLengthEntry> {
        @EqualsAndHashCode.Include
        int startOffset;
        int runLength;

        @Override
        public int compareTo(final RunLengthEncoder.RunLengthEntry o) {
            return Integer.compare(startOffset, o.startOffset);
        }
    }

    /**
     * For each run entry, see if it's below the base, if it is, drop it. Find the first run length that intersects with
     * the new base, and truncate it. Finish.
     * <p>
     * Uses cached positions so it does't have to search
     */
    void truncateRunlengthsV2(final int newBaseOffset) {
//        if (runLengthEncodingIntegers.size() > 1000) {
//            log.debug("length: {}", runLengthEncodingIntegers.size());
//        }

        RunLengthEntry floor = runLengthOffsetPairs.floor(new RunLengthEntry(newBaseOffset, -1));
        if (floor == null) throw new InternalRuntimeError("Couldn't find interception point");
//        RunLengthEntry ceiling = ns.ceiling(new RunLengthEntry(newBaseOffset, -1));
//        RunLengthEntry higher = ns.higher(floor);
//        RunLengthEntry lower = ns.lower(floor);

        int adjustedRunLength = floor.runLength - (newBaseOffset - floor.startOffset);
        NavigableSet<RunLengthEntry> tail = this.runLengthOffsetPairs.tailSet(floor, true);
        this.runLengthOffsetPairs = new TreeSet<>();
        // ugh slow. can't use tailset as it's just a view and can't add elements to it
        runLengthOffsetPairs.addAll(tail);
//        floor.
        this.runLengthOffsetPairs.remove(floor);
        this.runLengthOffsetPairs.add(new RunLengthEntry(floor.startOffset, adjustedRunLength));

//        List<Integer> integers = runLengthEncodingIntegers.subList(index, runLengthEncodingIntegers.size());
//        integers.set(0, adjustedRunLength); // overwrite with adjusted

        //
        this.originalBaseOffset = newBaseOffset;
    }

    /**
     * For each run entry, see if it's below the base, if it is, drop it. Find the first run length that intersects with
     * the new base, and truncate it. Finish.
     */
    void truncateRunlengths(final int newBaseOffset) {
        int currentOffset = 0;
        if (runLengthEncodingIntegers.size() > 1000) {
            log.info("length: {}", runLengthEncodingIntegers.size());
        }
        int index = 0;
        int adjustedRunLength = -1;
        for (Integer aRunLength : runLengthEncodingIntegers) {
            currentOffset = currentOffset + aRunLength;
            if (currentOffset <= newBaseOffset) {
                // drop from new collection
            } else {
                // found first intersection - truncate
                adjustedRunLength = currentOffset - newBaseOffset;
                break; // done
            }
            index++;
        }
        if (adjustedRunLength == -1) throw new InternalRuntimeError("Couldn't find interception point");
        List<Integer> integers = runLengthEncodingIntegers.subList(index, runLengthEncodingIntegers.size());
        integers.set(0, adjustedRunLength); // overwrite with adjusted

        // swap
        this.runLengthEncodingIntegers = integers;

        //
        this.originalBaseOffset = newBaseOffset;
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        int numEntries = getSize();
//        if (currentRunLengthCount > 0)
//            numEntries = numEntries + 1;
        int entryWidth = getEntryWidth();
        int accumulativeEntrySize = numEntries * entryWidth;
        return accumulativeEntrySize;// + standardOverhead;
    }

    @Override
    public byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

    private void encodeRunLength(final boolean currentIsComplete, final int relativeOffsetFromBase) {
        injectGapsWithIncomplete(currentIsComplete, relativeOffsetFromBase);

        // run length
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;

        //

        if (currentOffsetMatchesOurRunLengthState) {
//            currentRunLengthCount++; // no gap case
            int delta = relativeOffsetFromBase - previousRelativeOffsetFromBase;
            currentRunLengthCount = currentRunLengthCount + delta;
        } else {
            previousRunLengthState = currentIsComplete;
            addRunLength(currentRunLengthCount, relativeOffsetFromBase);
            currentRunLengthCount = 1; // reset to 1
        }
        previousRelativeOffsetFromBase = relativeOffsetFromBase;
    }

    private void addRunLength(final int currentRunLengthCount, final int relativeOffsetFromBase) {
        runLengthEncodingIntegers.add(currentRunLengthCount);
        RunLengthEntry entry = new RunLengthEntry(relativeOffsetFromBase, currentRunLengthCount);
        try {
            runLengthOffsetPairs.add(entry);
        } catch (Exception e) {
            log.error("{}", e);
        }
    }

    private void injectGapsWithIncomplete(final boolean currentIsComplete, final int relativeOffsetFromBase) {
//        boolean bothThisRecordAndPreviousRecordAreComplete = previousRunLengthState && currentIsComplete;
//        if (bothThisRecordAndPreviousRecordAreComplete) {
        int difference = relativeOffsetFromBase - previousRelativeOffsetFromBase - 1;
        if (currentRunLengthCount == 0)
            difference++;
        if (difference > 0) {
            // check for gap - if there's a gap, we need to assume all in-between are incomplete, except the first
            if (currentRunLengthCount != 0) {
                addRunLength(currentRunLengthCount, previousRelativeOffsetFromBase - currentRunLengthCount + 1);
            }
            // If they don't exist, this action has no affect, as we only use it to skip succeeded
            //
            // there is a gap, so first insert the incomplete
            addRunLength(difference, relativeOffsetFromBase - difference);
            currentRunLengthCount = 1; // reset to 1
            previousRunLengthState = true; // make it no flip
            previousRelativeOffsetFromBase = relativeOffsetFromBase;
        }
//        }
    }

    /**
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsets() {
        List<Long> successfulOffsets = new ArrayList<>();
        boolean succeeded = false;
//        int previous = 0;
        long offsetPosition = originalBaseOffset;
        //for (final Integer run : runLengthEncodingIntegers) {
        for (final RunLengthEntry n : runLengthOffsetPairs) {
            int run = n.getRunLength();
            if (succeeded) {
//                offsetPosition++;
                for (final Integer integer : Range.range(run)) {
                    long newGoodOffset = offsetPosition + integer;
                    successfulOffsets.add(newGoodOffset);
                }
            }
//            else {
//                offsetPosition = offsetPosition + run;
//            }

            //
            offsetPosition += run;
//            previous = run;
            succeeded = !succeeded;
        }
        return successfulOffsets;
    }

}
