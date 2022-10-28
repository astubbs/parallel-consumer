package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static java.lang.Math.toIntExact;


/**
 * Segmenting Run-Length encoder, that leverages the nature of this system.
 * <p>
 * Works by only tracks positive runs.
 * <p>
 * One such nature is that gaps between completed offsets get encoded as succeeded offsets. This doesn't matter because
 * they don't exist, and we'll never see them (they no longer exist in the source partition).
 * <p>
 * Run-length is written "Run-length": https://en.wikipedia.org/wiki/Run-length_encoding, it is two words.
 * <p>
 * By definition, run-length starts off as incomplete. Otherwise, our highest sequentially succeeded offset would
 * simply. move up.
 *
 * @author Antony Stubbs
 */
@ToString(onlyExplicitlyIncluded = true, callSuper = true)
@Slf4j
public class RunLengthEncoder extends OffsetEncoder {

    public static final int LARGE_NUMER_OF_RUN_LENGTHS = 2_000;
    /**
     * The current run length being counted / built
     */
    @ToString.Include
    private long currentRunLengthCount;

    /**
     * todo docs
     */
    private int previousRelativeOffsetFromBase;

    /**
     * todo docs
     */
    private boolean previousRunLengthState;

    /**
     * Stores all the run lengths
     */
    @Deprecated//?
    @Getter
    private List<Integer> runLengthEncodingIntegers;

    /**
     * Only need to track positive run lengths, negatives can be derived. This makes iteratively building the run length
     * structure trivial as we don't need to do any segmenting.
     */
    @Getter(AccessLevel.PROTECTED)
    private NavigableSet<RunLengthEntry> allRunLengths = new TreeSet<>();

    private Optional<byte[]> encodedBytes = Optional.empty();

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(baseOffset, offsetSimultaneousEncoder, newVersion);

        init();
    }

    private void init() {
        // todo delete
        runLengthEncodingIntegers = new ArrayList<>();

        currentRunLengthCount = 0;
        previousRelativeOffsetFromBase = 0;
        previousRunLengthState = false;
    }

//    private void reset() {
//        log.debug("Resetting");
//        init();
//    }

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

//    @Override
//    public void encodeIncompleteOffset(final long newBaseOffset, final long relativeOffset) {
//        encodeRunLength(false, newBaseOffset, relativeOffset);
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long newBaseOffset, final long relativeOffset) {
//        encodeRunLength(true, newBaseOffset, relativeOffset);
//    }

    @Override
    public byte[] serialise() throws EncodingNotSupportedException {
//        endCurrentRunLength();

        int entryWidth = getEntryWidth();

        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(getSize() * entryWidth);

        // for (final Integer run : getRunLengthEncodingIntegers()) {
        for (final RunLengthEntry n : allRunLengths) {
            Long runLength = n.runLength;
            switch (version) {
                case v1 -> {
                    final short shortCastRunlength = runLength.shortValue();
                    if (runLength != shortCastRunlength)
                        throw new RunLengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", runLength, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(toIntExact(runLength));
                }
            }
        }

        byte[] array = runLengthEncodedByteBuffer.array();
        encodedBytes = Optional.of(array);
        return array;
    }

    private int getSize() {
        //return runLengthEncodingIntegers.size();
        return allRunLengths.size();
    }

//    /**
//     * Add the dangling in flight run to the list, done before serialising
//     */
//    private void endCurrentRunLength() {
//        if (allRunLengths.isEmpty()) {
//            addRunLength(originalBaseOffset, currentRunLengthCount, originalBaseOffset, state);
//        } else {
//            RunLengthEntry finalRunLength = allRunLengths.last();
//            long relativeOffsetFromBase = finalRunLength.absoluteStartOffset + finalRunLength.runLength - originalBaseOffset;
//            addRunLength(originalBaseOffset, currentRunLengthCount, relativeOffsetFromBase, state);
//        }
//    }

    private int getEntryWidth() {
        return switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
    }

    @Override
    public void encodeIncompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
        maybeTruncateBase(newBaseOffset, currentHighestCompleted);
//        encodeRunLength(false, baseOffset, relativeOffset);
//        //no-op
//        //Entries being added should always be complete, as the range by definition starts out incomplete. We never add incompletes because things never transition from complete to incomplete.");

        encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(newBaseOffset, relativeOffset, RunLengthEntry.OffsetState.INCOMPLETE);
    }

    @Override
    public void encodeCompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
//        maybeReinitialise(newBaseOffset, currentHighestCompleted);

//        encodeRunLength(true, newBaseOffset, relativeOffset);
        maybeTruncateBase(newBaseOffset, currentHighestCompleted);

        encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(newBaseOffset, relativeOffset, RunLengthEntry.OffsetState.SUCCEEDED);
    }

    /**
     * Returns the negative and positive run-lengths by calculating the implicit negative entries
     */
    public List<Long> calculateFullRelativeRunLengthsOld() {
        List<Long> bothRunLengths = new ArrayList<>();
        for (RunLengthEntry runLength : getAllRunLengths()) {
            // negative
            var start = runLength.getLowerNeighbour();
//            var le
            bothRunLengths.add(runLength.getNegativeNeighbourRunLength());
//
//            Optional<Long> negativeNeighbourRunLength = runLength.getNegativeNeighbourRunLength();
//            if (negativeNeighbourRunLength.isPresent()) {
//                bothRunLengths.add(negativeNeighbourRunLength.get());
//            }
            // positive
            bothRunLengths.add(runLength.getRunLength());
        }
        return bothRunLengths;
    }

    /**
     * Returns the negative and positive run-lengths by calculating the implicit negative entries. Gaps in data mean the
     * offsets are missing from the source partition, and so should be marked as succeeded.
     * <p>
     * Warning: Marks missing data as incomplete - NOT SUCCEEDED, as serialisation requires
     */
    public List<Long> calculateFullRelativeRunLengthsForTesting() {
//        List<Long> bothRunLengths = new ArrayList<>();
//        long currentOffsetExpected = 0L;
//
//        for (RunLengthEntry runLength : getAllRunLengths()) {
//            if (currentOffsetExpected != runLength.getAbsoluteStartOffset()) {
//                // make missing data as incomplete
////                var missingData = new RunLengthEntry(currentOffsetExpected, runLength.getAbsoluteStartOffset(), RunLengthEntry.OffsetState.SUCCEEDED);
//                final long sizeOfGapOfMissingData = runLength.getAbsoluteStartOffset() - currentOffsetExpected;
//                bothRunLengths.add(sizeOfGapOfMissingData);
//            }
//
//            bothRunLengths.add(runLength.getRunLength());
//
//            currentOffsetExpected = runLength.getEndOffsetInclusive() + 1;
//        }
//        return bothRunLengths;

        return calculateFullRunLengthEntriesForTesting().stream().mapToLong(RunLengthEntry::getRunLength).boxed().collect(Collectors.toList());
    }

    /**
     * Returns the negative and positive run-lengths by calculating the implicit negative entries, for testing.
     * <p>
     * Warning: Marks missing data as incomplete - NOT SUCCEEDED, as serialisation requires
     */
    public List<RunLengthEntry> calculateFullRunLengthEntriesForTesting() {
        List<RunLengthEntry> bothRunLengths = new ArrayList<>();
        long currentOffsetExpected = originalBaseOffset;
        for (RunLengthEntry runLength : getAllRunLengths()) {
            if (currentOffsetExpected != runLength.getAbsoluteStartOffset()) {
                // make missing data as incomplete
                final long sizeOfGapOfMissingData = runLength.getAbsoluteStartOffset() - currentOffsetExpected;
                var missingData = new RunLengthEntry(currentOffsetExpected, sizeOfGapOfMissingData, RunLengthEntry.OffsetState.INCOMPLETE);
                bothRunLengths.add(missingData);
            }

//            // negative
//            final Long negativeLength = runLengthEntry.getNegativeNeighbourRunLength();
//            final long negativeStart = runLengthEntry.absoluteStartOffset - negativeLength;
//            bothRunLengths.add(new RunLengthEntry(negativeStart, negativeLength, isSuccedded));
//
//            Optional<Long> negativeNeighbourRunLength = runLengthEntry.getNegativeNeighbourRunLength();
//            if (negativeNeighbourRunLength.isPresent()) {
//                bothRunLengths.add(negativeNeighbourRunLength.get());
//            }

            // positive
            bothRunLengths.add(runLength);

            //
            currentOffsetExpected = runLength.getEndOffsetInclusive() + 1;
        }
        return bothRunLengths;
    }

    private long getGenesisRunLength() {
        return (long) getAllRunLengths().first().runLength;
    }

    public void maybeTruncateBase(final long newBaseOffset, final long currentHighestCompleted) {
        boolean reinitialise = false;

        if (originalBaseOffset != newBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work",
                    this.originalBaseOffset, newBaseOffset);
            reinitialise = true;
        }

        if (reinitialise) {
            reinitialise(newBaseOffset, currentHighestCompleted);
        }
    }

    private void reinitialise(final long newBaseOffset, final long currentHighestCompleted) {
//        long longDelta = newBaseOffset - originalBaseOffset;
//        int baseDelta = JavaUtils.safeCast(longDelta);
        truncateRunLengthsV2(newBaseOffset);


//        currentRunLengthCount = 0;
//        previousRelativeOffsetFromBase = 0;
//        previousRunLengthState = false;

        enable();
    }


    /**
     * todo docs
     */
    @ToString
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    class RunLengthEntry implements Comparable<RunLengthEntry> {

        /**
         * todo docs
         */
        @Getter
        @EqualsAndHashCode.Include
        private long absoluteStartOffset;

        /**
         * The length of this run
         */
        @Getter
        private Long runLength;

        @Getter
        private final OffsetState offsetState;

        public RunLengthEntry(long newBaseOffset) {
            // todo - state is ignored for use as key - code smell
            this(newBaseOffset, null);
        }

        public boolean isSucceeded() {
            return getOffsetState() == OffsetState.SUCCEEDED;
        }

        enum OffsetState {
            SUCCEEDED,
            INCOMPLETE;

            public OffsetState invert() {
                return switch (this) {
                    case SUCCEEDED -> INCOMPLETE;
                    case INCOMPLETE -> SUCCEEDED;
                };
            }
        }

//        @Getter
//        private boolean succeeded;

        /**
         * todo docs - what does it mean to have a run-length entry with no run length? a run-length of one?
         */
        public RunLengthEntry(final long absoluteStartOffset, final OffsetState state) {
            this(absoluteStartOffset, 1L, state);
        }

        public RunLengthEntry(final long absoluteStartOffset, final Long runLength, final OffsetState state) throws ArithmeticException {
            this.offsetState = state;
            if (absoluteStartOffset < 0) {
                throw new IllegalArgumentException(msg("Bad start offset {}", absoluteStartOffset, runLength));
            }
            this.absoluteStartOffset = absoluteStartOffset;
//            this.succeeded = succeeded;
            if (runLength != null)
                setRunLength(Math.toIntExact(runLength));
        }

        public void setRunLength(final long runLength) {
            if (runLength < 1) {
                throw new IllegalArgumentException(msg("Cannot have a run-length of {}", runLength));
            }
            this.runLength = runLength;
        }

        /**
         * Inclusive end offset of the range this entry represents
         */
        public long getEndOffsetInclusive() {
            return absoluteStartOffset + runLength - 1;
        }

        public long getEndOffsetExclusive() {
            return absoluteStartOffset + runLength;
        }

        public long getRelativeStartOffsetFromBase(final long baseOffset) {
            return Math.toIntExact(absoluteStartOffset - baseOffset);
        }

        /**
         * todo docs
         */
        public int getRelativeEndOffsetFromBase(final long baseOffset) {
            return Math.toIntExact(getRelativeStartOffsetFromBase(baseOffset) + runLength - 1);
        }

        @Override
        public int compareTo(final RunLengthEncoder.RunLengthEntry o) {
            return Long.compare(absoluteStartOffset, o.absoluteStartOffset);
        }

        /**
         * @param absoluteOffset the offset to test
         * @return true if the absolute offset is within the range of this entry
         */
        public boolean contains(long absoluteOffset) {
            return absoluteOffset >= absoluteStartOffset && absoluteOffset < getEndOffsetExclusive();
        }

        /**
         * Extend this entry by one, can be used to merge a neighbour that's of size 1.
         */
        public void extendUpByOne() {
            long newExtendedRunLength = getRunLength() + 1;
            setRunLength(newExtendedRunLength);

            // maybe we can merge with the next entry?
            maybeMergeWithHigherNeighbour();
        }

        /**
         * Extend this entry by one, can be used to merge a neighbour that's of size 1.
         */
        public void extendDownByOne() {
//            RunLengthEntry lowerNeighbour = getLowerNeighbour();
//            lowerNeighbour.mergeWithHigher(this);
            // extend the run length
            long newExtendedRunLength = getRunLength() + 1;
            setRunLength(newExtendedRunLength);
            // move the start offset down 1
            absoluteStartOffset--;

            // maybe we can merge with the previous entry?
            // maybe we can merge with the next entry?
            RunLengthEntry nextLower = getLowerNeighbour();
            if (nextLower != null) {
                nextLower.maybeMergeWithHigherNeighbour();
            }
        }

        private void mergeWithHigher(RunLengthEntry higherNeigh) {
            // extend my run length to include theirs
            long newExtendedRunLength = getRunLength() + higherNeigh.getRunLength();
            setRunLength(newExtendedRunLength);

            // remove them
            getAllRunLengths().remove(higherNeigh);
        }

        private RunLengthEntry getLowerNeighbour() {
            return allRunLengths.lower(this);
        }

        private RunLengthEntry getHigherNeighbour() {
            return allRunLengths.higher(this);
        }

        public boolean ingestNewNeighbourEntry(long newBaseOffset, long relativeOffsetFromBase, final OffsetState state) {
            long absoluteOffset = newBaseOffset + relativeOffsetFromBase;

// can never contain - no segmenting
            if (contains(absoluteOffset)) {
                return segmentOrMergeEntryBy(newBaseOffset, relativeOffsetFromBase);
            } else {
                // new entry higher than any existing
                addNewNeighbour(newBaseOffset, relativeOffsetFromBase, state);
                // need to merge in both directions
                return false;
            }
        }

        /**
         * New positive is within our range, so we need to segment ourselves.
         */
        private boolean segmentOrMergeEntryBy(long newBaseOffset, long relativeOffsetFromBase) {
            boolean segmented;
            long absoluteOffset = newBaseOffset + relativeOffsetFromBase;

            var closestExistingEntry = this;
            RunLengthEntry nextHigher = allRunLengths.higher(closestExistingEntry);

            segmented = true;

//            if (closestExistingEntry.runLength == 1) {
//                // we're the only entry in the range, so we can just extend ourselves
//                mergeThreeEntries(closestExistingEntry, nextHigher);
//            } else {
            // we're not the only entry in the range, so we need to split ourselves
            split(newBaseOffset, relativeOffsetFromBase, absoluteOffset, closestExistingEntry, nextHigher);
//            }
            return segmented;
        }

        /**
         * Split the existing entry into three
         */
        private void split(long newBaseOffset, long relativeOffsetFromBase, long absoluteOffset, RunLengthEntry closestExistingEntry, RunLengthEntry nextHigher) {
            var low = newBaseOffset - this.getAbsoluteStartOffset();
            var offset = newBaseOffset + relativeOffsetFromBase;

            if (low > 0) {
                // we need to split the lower part
                var lower = new RunLengthEntry(newBaseOffset, this.getOffsetState());
                allRunLengths.add(lower);

                // give a chance to merge
                lower.getLowerNeighbour().maybeMergeWithHigherNeighbour();
            }

            // we need to split the middle part
            var middle = new RunLengthEntry(newBaseOffset, 1L, getOffsetState().invert());
            allRunLengths.add(middle);

            // we need to split the upper part
            var high = offset - getEndOffsetInclusive();
            if (high > 0) {
                // we need to split the upper part
                var upperRun = getEndOffsetInclusive() - newBaseOffset;
                var upper = new RunLengthEntry(newBaseOffset + 1, this.getOffsetState());
                allRunLengths.add(upper);

                // give a chance to merge
                upper.maybeMergeWithHigherNeighbour();
            }

            // remove myself
            allRunLengths.remove(this);
        }

        /**
         * todo docs
         */
        private void splitOld(long newBaseOffset, long relativeOffsetFromBase, long absoluteOffset, RunLengthEntry closestExistingEntry, RunLengthEntry nextHigher) {
            OffsetState state = null; // delete me

            // remove the old entry which must have been incompletes
            boolean missing = !allRunLengths.remove(closestExistingEntry);
            if (missing)
                throw new InternalRuntimeException("Cant find element that previously existed");

            long newRunCumulative = 1;

            Long offsetStartRelative = null;

            // create the three to replace the intersected node - 1 incomplete, 1 complete (this one), 1 incomplete
            int firstRun = toIntExact(absoluteOffset - closestExistingEntry.absoluteStartOffset);
            Long middleRelativeOffset = null;
            long firstRelativeOffset = closestExistingEntry.getRelativeStartOffsetFromBase(originalBaseOffset);

            if (firstRun > 0) {
                // large gap to fill
//                RunLengthEntry runLengthEntry = new RunLengthEntry(closestExistingEntry.startOffset, firstRun);
                RunLengthEntry first = addRunLength(newBaseOffset, firstRun, firstRelativeOffset, state);
                middleRelativeOffset = first.getRelativeStartOffsetFromBase(originalBaseOffset) + first.runLength;
            } else {
                // combine with the neighbor as there's no gap
                // check for a lower neighbour
                RunLengthEntry previous = allRunLengths.lower(closestExistingEntry);
                if (previous != null && previous.getEndOffsetExclusive() == absoluteOffset) {
                    // lower neighbor connects - combine
                    newRunCumulative = newRunCumulative + previous.runLength;
                    offsetStartRelative = previous.getRelativeStartOffsetFromBase(newBaseOffset);
                    allRunLengths.remove(previous);
                }
            }

            if (middleRelativeOffset == null)
                middleRelativeOffset = firstRelativeOffset;

            if (nextHigher != null) {
                long gapUpward = nextHigher.getRelativeStartOffsetFromBase(newBaseOffset) - middleRelativeOffset;
                if (gapUpward > 1) {
                    // there is a large gap between this success and the nextHigher
                    // add this single entry now then, and then add gap filler
//                    if (offsetStartRelative == null)
//                        offsetStartRelative = nextHigher.getRelativeStartOffsetFromBase(newBaseOffset) - 1; // shift left one place
//                    newRunCumulative = newRunCumulative + 1;

                    if (offsetStartRelative == null)
                        offsetStartRelative = relativeOffsetFromBase;

                    RunLengthEntry middle = addRunLength(newBaseOffset, newRunCumulative, offsetStartRelative, state);

                    // add incomplete filler
                    int lastRange = toIntExact(closestExistingEntry.getEndOffsetInclusive() - absoluteOffset);
                    if (lastRange > 0) {
                        addRunLength(newBaseOffset, lastRange, middleRelativeOffset + 1, state);
//                        int fillerStart = middle.getRelativeEndOffsetFromBase(newBaseOffset);
//                        int use = (fillerStart != middleRelativeOffset + 1)?
//                        if (fillerStart != middleRelativeOffset + 1) {
//                            log.trace("");
//                        }
//                        addRunLength(newBaseOffset, lastRange, fillerStart);
                    }
                } else if (gapUpward == 1) {
                    // combine with upper
                    newRunCumulative = newRunCumulative + nextHigher.runLength; // expand
//                nextHigher.setRunLength(newRunLength);
                    allRunLengths.remove(nextHigher);
                    if (offsetStartRelative == null)
                        offsetStartRelative = nextHigher.getRelativeStartOffsetFromBase(newBaseOffset) - 1; // shift left one place if not already established
                    RunLengthEntry end = addRunLength(newBaseOffset, newRunCumulative, offsetStartRelative, state);
                } else {
                    throw InternalRuntimeException.msg("Invalid gap {}", gapUpward);
                }
            }
        }

//        private void mergeThreeEntries(RunLengthEntry closestExistingEntry, RunLengthEntry nextHigher) {
//            // simple path
//            // single bad entry to be replaced, but with 1 new entry and 2 good entry neighbors - combine all three
//            RunLengthEntry previous = allRunLengths.lower(closestExistingEntry);
//            var newRun = 0L;
//            if (previous != null) {
//                var runDown = previous.runLength;
//                newRun = newRun + runDown;
//            }
//            var runUp = nextHigher.runLength;
//            newRun = newRun + 1 + runUp;
//
//            // remove the 3 entries
//            allRunLengths.remove(previous);
//            allRunLengths.remove(closestExistingEntry);
//            allRunLengths.remove(nextHigher);
//
//            allRunLengths.add(new RunLengthEntry(closestExistingEntry.absoluteStartOffset, newRun, isSucceeded));
//        }


        /**
         * New positive doesn't with us, so we need to add a new entry
         */
        private void addNewNeighbour(final long newBaseOffset, final long relativeOffsetFromBase, final OffsetState state) {
            int myEndOffset = getRelativeEndOffsetFromBase(originalBaseOffset);
            var newOffset = newBaseOffset + relativeOffsetFromBase;

            // extending the range
            // is there a gap?
            var isAboveUs = newOffset > absoluteStartOffset;

            long gapBelow = getAbsoluteStartOffset() - newOffset;
            long gapAbove = newOffset - getEndOffsetInclusive();

//            long gapSizeBetweenEntries = relativeOffsetFromBase - myEndOffset;
            final boolean neighbourAboveAdjacent = isAboveUs && gapAbove == 1;
            if (neighbourAboveAdjacent) {
                extendUpByOne();
            } else {
                final boolean neighbourBelowAdjacent = !isAboveUs && gapBelow == 1;

                if (neighbourBelowAdjacent) {
                    extendDownByOne();
                } else {
                    final boolean neighbourNotAdjacent = gapAbove > 1 || gapBelow > 1;
                    if (neighbourNotAdjacent) {
                        // real gap exists, add in an entry of incomplete to fill the gap, and append the new entry


                        // we don't track negatives
                        //                // add new negatives entry
                        //                int newRelativeOffset = myEndOffset + 1;
                        //                long run = gapSizeBetweenEntries - 1;
                        //                addRunLength(newBaseOffset, run, newRelativeOffset);


                        // add new positive run entry
                        addRunLength(newBaseOffset, 1, relativeOffsetFromBase, state);
                    } else {
                        throw InternalRuntimeException.msg("Invalid gap between entries above {} or below {}", gapAbove, gapBelow);
                    }
                }
            }
        }

        /**
         * Candidate should always be higher
         */
        boolean maybeMergeWithHigherNeighbour() {
            var mergeCandidate = getHigherNeighbour();
            if (mergeCandidate == null) {
                return false;
            }

            boolean runLengthsAreNowAdjacent = isAdjacentTo(mergeCandidate);
            boolean statesAreEqual = mergeCandidate.hasSameStateAs(this);
            if (runLengthsAreNowAdjacent && statesAreEqual) {
                // extend to cover
                this.setRunLength(this.getRunLength() + mergeCandidate.getRunLength());
                // delete
                getAllRunLengths().remove(mergeCandidate);
                return true;
            }
            return false;
        }

        private boolean isAdjacentTo(RunLengthEntry mergeCandidate) {
            return getEndOffsetInclusive() + 1 == mergeCandidate.getAbsoluteStartOffset();
        }

        private boolean hasSameStateAs(RunLengthEntry target) {
            return getOffsetState() == target.getOffsetState();
        }

        /**
         * As negatives aren't tracked, we derive it
         */
        public Long getNegativeNeighbourRunLength() {
            final RunLengthEntry lowerPositive = getAllRunLengths().lower(this);
            if (lowerPositive == null) {
                // this run length is the start, so derive the genesis negative run-length
                // means we are the first entry
                return getAbsoluteStartOffset() - originalBaseOffset;
            } else {
                return this.absoluteStartOffset - lowerPositive.getEndOffsetInclusive() - 1;
            }
        }

    }

    /**
     * For each run entry, see if it's below the base, if it is, drop it. Find the first run length that intersects with
     * the new base, and truncate it. Finish.
     * <p>
     * Uses cached positions, so it doesn't have to search
     */
    void truncateRunLengthsV2(final long newBaseOffset) {
        // else nothing to truncate
        if (!allRunLengths.isEmpty()) {

            if (allRunLengths.size() > LARGE_NUMER_OF_RUN_LENGTHS) {
                log.debug("Number of positive entries: {}", allRunLengths.size());
            }
//
//        {
//            // sanity
//            RunLengthEntry first = runLengthOffsetPairs.first();
//            RunLengthEntry second = runLengthOffsetPairs.higher(first);
//            if (first.getEndOffsetInclusive() + 1 != second.startOffset)
//                throw new RuntimeException("");
//        }

            // entries any higher, start at a higher offset than our target
            RunLengthEntry highestEntryThatCouldContainTarget = allRunLengths.floor(new RunLengthEntry(newBaseOffset)
            );
            var highestEntry = allRunLengths.last();
            if (highestEntryThatCouldContainTarget == null)
                throw new InternalRuntimeException("Couldn't find interception point, and no entries below the base");
            else if (newBaseOffset > highestEntry.getEndOffsetInclusive()) {
                // special case
                // there is no intersection as the new base offset is a point beyond what our run lengths encode
                // remove all
                allRunLengths.clear();
            } else {
                if (highestEntryThatCouldContainTarget.contains(newBaseOffset)) {
                    // truncate intersection run length
                    long adjustedRunLength = highestEntryThatCouldContainTarget.runLength - (newBaseOffset - highestEntryThatCouldContainTarget.absoluteStartOffset);
                    highestEntryThatCouldContainTarget.setRunLength(toIntExact(adjustedRunLength));

                    // truncate all run-lengths before intersection point
                    NavigableSet<RunLengthEntry> toTruncateFromSet = allRunLengths.headSet(highestEntryThatCouldContainTarget, false);
                    toTruncateFromSet.clear();
                } else {
                    // there is no intersection as the positive run doesn't reach up to the new target
                    // remove it, and all less than
//                    positiveRunLengths.remove(highestEntryThatCouldContainTarget);
                    allRunLengths = allRunLengths.tailSet(highestEntryThatCouldContainTarget, false);
                }

            }
//
//        {
//            // sanity
//            RunLengthEntry first = runLengthOffsetPairs.first();
//            RunLengthEntry second = runLengthOffsetPairs.higher(first);
//            if (first.getEndOffsetInclusive() + 1 != second.startOffset)
//                throw new RuntimeException("");
//        }
        }

        // move the base up
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
        if (adjustedRunLength == -1) throw new InternalRuntimeException("Couldn't find interception point");
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
    public void ensureCapacity(final long base, final long highest) {
        final long offsetRange = highest - base;
        switch (version) {
            case v1 -> {
                if (offsetRange > Short.MAX_VALUE) {
                    disable(new RunLengthV1EncodingNotSupported(msg("v1 doesn't support large enough offsets {} vs {}", offsetRange, Short.MAX_VALUE)));
                }
            }
            case v2 -> {
                if (offsetRange > Integer.MAX_VALUE) {
                    disable(new RunLengthV2EncodingNotSupported(msg("v2 doesn't support large offsets {} vs {}", offsetRange, Integer.MAX_VALUE)));
                }
            }
        }
    }

    @Override
    public byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

//    private void encodeRunLength(final boolean currentIsComplete, final long newBaseOffset, final long relativeOffsetFromBase) {
//        encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(currentIsComplete, newBaseOffset, relativeOffsetFromBase);
//    }

    @Deprecated
    private void encodeRunLengthOld(final boolean currentIsComplete, final long newBaseOffset, final int relativeOffsetFromBase) {
//        boolean segmented = injectGapsWithIncomplete(currentIsComplete, newBaseOffset, relativeOffsetFromBase);
//        if (segmented)
//            return;
//
//        // run length
//        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
//
//        //
//
//        if (currentOffsetMatchesOurRunLengthState) {
////            currentRunLengthCount++; // no gap case
//            long dynamicPrevious = getPreviousRelativeOffset(toIntExact(newBaseOffset) + relativeOffsetFromBase);
//            long dynamicPrevious2 = getPreviousRelativeOffset2(newBaseOffset, relativeOffsetFromBase) - 1;
//            int delta = relativeOffsetFromBase - previousRelativeOffsetFromBase;
//            long delta2 = relativeOffsetFromBase - dynamicPrevious2;
//            long currentRunLengthCountOld = currentRunLengthCount + delta;
//            long currentRunLengthCountNew = currentRunLengthCount + delta2;
//            currentRunLengthCount = currentRunLengthCountNew;
//        } else {
//            previousRunLengthState = currentIsComplete;
//            addRunLength(newBaseOffset, currentRunLengthCount, relativeOffsetFromBase, state);
//            currentRunLengthCount = 1; // reset to 1
//        }
//        previousRelativeOffsetFromBase = relativeOffsetFromBase;
    }

    /**
     * Adds a new run-length entry
     * <p>
     * //     * , automatically switching between positive and negative run lengths depending on the //     * current
     * head entry (this is implicitly encoded by the nature of the run length - the runs always alternate //     *
     * negative to positive)
     *
     * @return the added entry
     */
    // todo consider moving to entry class?
    private RunLengthEntry addRunLength(final long newBaseOffset, final long runLength, final long relativeOffsetFromBase, final RunLengthEntry.OffsetState state) throws ArithmeticException {
        // v1
//        runLengthEncodingIntegers.add(runLength);

        // v2
        int offset = toIntExact(newBaseOffset + relativeOffsetFromBase);
//        if (!runLengthOffsetPairs.isEmpty()) {
//            RunLengthEntry previous = runLengthOffsetPairs.last();
//            if (previous != null && offset != previous.getEndOffsetInclusive() + 1)
//                throw new IllegalArgumentException(msg("Can't add a run length offset {} that's not continuous from previous {}", offset, previous));
//        }
        RunLengthEntry entry = new RunLengthEntry(offset, runLength, state);
        boolean containedAlready = !allRunLengths.add(entry);
        if (containedAlready)
            throw new InternalRuntimeException(msg("Error adding new entry - already contained a run for offset {}", offset));
        return entry;
    }

//    @Deprecated
//    private boolean injectGapsWithIncomplete(final boolean currentIsComplete, final long newBaseOffset, final int relativeOffsetFromBase) {
////        boolean segmented = encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(currentIsComplete, newBaseOffset, relativeOffsetFromBase);
//        boolean segmented = false; // TODO
//
////        if (segmented)
////            return true;
//
////        boolean bothThisRecordAndPreviousRecordAreComplete = previousRunLengthState && currentIsComplete;
////        if (bothThisRecordAndPreviousRecordAreComplete) {
//        int differenceold = relativeOffsetFromBase - previousRelativeOffsetFromBase - 1;
//        int previousOffsetOld = previousRelativeOffsetFromBase - 1;
//
//        long previousRelativeOffset = getPreviousRelativeOffset(toIntExact(newBaseOffset) + relativeOffsetFromBase);
//        long previousRelativeOffset2 = getPreviousRelativeOffset2(newBaseOffset, relativeOffsetFromBase);
//
//        RunLengthEntry dynamicPrevious = allRunLengths.floor(new RunLengthEntry(toIntExact(newBaseOffset + relativeOffsetFromBase), isSuccedded));
//        long previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - toIntExact(newBaseOffset + currentRunLengthCount);
//
//        // difference Between This Relative Offset And Previous Run Length Entry In Run Length Sequence
//        long difference = relativeOffsetFromBase - previousOffset;
//
//        if (currentRunLengthCount == 0)
//            differenceold++;
//
//        //
//        if (difference > 0) {
//            // check for gap - if there's a gap, we need to assume all in-between are incomplete, except the first
//            // If they don't exist, this action has no affect, as we only use it to skip succeeded
//
//            // if we already have an ongoing run length, add it first
//            if (currentRunLengthCount != 0) {
//                addRunLength(newBaseOffset, currentRunLengthCount, previousOffset - currentRunLengthCount + 1, state);
//            }
//
//            //
//            // there is a gap, so first insert the incomplete
//            addRunLength(newBaseOffset, difference, relativeOffsetFromBase - difference, state);
//            currentRunLengthCount = 1; // reset to 1
//            previousRunLengthState = true; // make it no flip
//            previousRelativeOffsetFromBase = relativeOffsetFromBase;
//        }
////        }
//        return segmented;
//    }

    /**
     * todo docs
     */
    // todo rename
    private boolean encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(final long newBaseOffset, final long relativeOffsetFromBase, final RunLengthEntry.OffsetState state) {
//        if (!currentIsComplete) {
//            throw new InternalRuntimeException("Entries being added should always be complete, as the range by definition starts out incomplete. We never add incompletes because things never transition from complete to incomplete.");
//        }

//        if (closestLessThan == null) {
        if (getAllRunLengths().isEmpty()) {
            // genesis case

// we don't track negatives
//            // first entry
//            // first derive the genesis incompletes, as we don't explicitly encode them when they're "performed"
//            addRunLength(newBaseOffset, relativeOffsetFromBase, 0);
//            // then add the first completed run-length of one

            addRunLength(newBaseOffset, 1, relativeOffsetFromBase, state);
            // we didn't segment any existing entries
            return false;
        } else {
            long absoluteOffset = newBaseOffset + relativeOffsetFromBase;
            // todo change from >= to < - as it can never be equal, so let's honour that

            // see if neighbour is below
            RunLengthEntry neighbour = getLowerNeighbour(absoluteOffset);

            if (neighbour == null) {
                // neighbour must be above - so we're inserting a lower run length than the lowest existing one
                neighbour = allRunLengths.ceiling(new RunLengthEntry(absoluteOffset, state));
//                neighbour = addRunLength(newBaseOffset, 1, relativeOffsetFromBase);
            }

            return neighbour.ingestNewNeighbourEntry(newBaseOffset, relativeOffsetFromBase, state);
        }
    }


    private RunLengthEntry getLowerNeighbour(long absoluteOffset) {
        return allRunLengths.lower(new RunLengthEntry(absoluteOffset));
    }

    private RunLengthEntry getHigherNeighbour(long absoluteOffset) {
        return allRunLengths.higher(new RunLengthEntry(absoluteOffset));
    }

    private long getPreviousRelativeOffset(final int offset) {
        RunLengthEntry dynamicPrevious = allRunLengths.floor(new RunLengthEntry(offset));
        long previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - toIntExact(originalBaseOffset);
        return previousOffset;
    }

    private long getPreviousRelativeOffset2(final long newBaseOffset, final int offset) {
        RunLengthEntry dynamicPrevious = allRunLengths.floor(new RunLengthEntry(offset));
        long previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - toIntExact(originalBaseOffset);
        return previousOffset - toIntExact(newBaseOffset + currentRunLengthCount);
    }

    /**
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsetsV1() {
        List<Long> successfulOffsets = new ArrayList<>();
        boolean succeeded = true;
//        int previous = 0;
        long offsetPosition = originalBaseOffset;
        //for (final Integer run : runLengthEncodingIntegers) {
        for (final RunLengthEntry runLengthEntry : allRunLengths) {
//            if (successfulOffsets.isEmpty()) {
//                // genesis negative
//            }


            long run = runLengthEntry.getRunLength();
//            if (succeeded) {
            offsetPosition = runLengthEntry.getAbsoluteStartOffset();
//                //todo avoid slow loop?
            for (Integer integer : Range.range(run).listAsIntegers()) {
                long newGoodOffset = offsetPosition + integer;
                successfulOffsets.add(newGoodOffset);
            }
//            } else {
//                offsetPosition = offsetPosition + run;
//            }

            //
            offsetPosition += run;
//            previous = run;
            succeeded = !succeeded;
        }
        return successfulOffsets;
    }

    /**
     * O(n) - only for testing
     *
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsets() {
        List<Long> successfulOffsets = new ArrayList<>();
//        boolean succeeded = true;
//        int previous = 0;
//        long offsetPosition = originalBaseOffset;
        //for (final Integer run : runLengthEncodingIntegers) {
        for (final RunLengthEntry runLengthEntry : allRunLengths) {

            if (runLengthEntry.isSucceeded()) {
//                long run = runLengthEntry.getRunLength();
//            if (succeeded) {
                long offsetPosition = runLengthEntry.getAbsoluteStartOffset();
//                //todo avoid slow loop?
                RunLengthEntry lowerNeighbour = runLengthEntry.getLowerNeighbour();
//                final long targetRange = runLengthEntry.getAbsoluteStartOffset() - lowerNeighbour.getEndOffsetInclusive();
                final long targetRange = runLengthEntry.getRunLength();
                for (Integer relativeIndex : Range.range(targetRange).listAsIntegers()) {
                    long newGoodOffset = offsetPosition + relativeIndex;
                    successfulOffsets.add(newGoodOffset);
                }
            }

//            } else {
//                offsetPosition = offsetPosition + run;
//            }

            //
//            offsetPosition += run;
//            previous = run;
//            succeeded = !succeeded;
        }
        return successfulOffsets;
    }

}
