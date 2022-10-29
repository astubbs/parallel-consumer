package io.confluent.parallelconsumer.offsets;

import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Optional;

import static io.confluent.csid.utils.StringUtils.msg;
import static java.lang.Math.toIntExact;

/**
 * todo docs
 */
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class RunLengthEntry implements Comparable<RunLengthEntry> {

    @Getter
    private final RunLengthSequence parentSequence;
    @Getter
    private final OffsetState offsetState;
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

    /**
     * For key use only
     *
     * @param absoluteOffset
     */
    // todo code smell - this is a hack to allow the entry to be used as a key in a map
    public RunLengthEntry(long absoluteOffset) {
        this(absoluteOffset, null, null);
    }

    /**
     * todo docs - what does it mean to have a run-length entry with no run length? a run-length of one?
     */
    public RunLengthEntry(final long absoluteStartOffset, final OffsetState state, final RunLengthSequence parentSequence) {
        this(absoluteStartOffset, 1L, parentSequence, state);
    }

    public RunLengthEntry(final long absoluteStartOffset,
                          final Long runLength,
                          final RunLengthSequence parentSequence,
                          final OffsetState state) throws ArithmeticException {
        if (absoluteStartOffset < 0) {
            throw new IllegalArgumentException(msg("Bad start offset {}", absoluteStartOffset, runLength));
        }

        this.absoluteStartOffset = absoluteStartOffset;
        this.parentSequence = parentSequence;
        this.offsetState = state;

        if (runLength != null)
            setRunLength(Math.toIntExact(runLength));

        maybeAddMyselfToParentSequence(parentSequence);
    }

    private void maybeAddMyselfToParentSequence(RunLengthSequence sequence) {
        if (sequence == null) {
            // todo code smell - this is a hack to allow the entry to be used as a key in a map
            return; //skip - only being used as a key
        }

        boolean containedAlready = !sequence.add(this);
        if (containedAlready) {
            throw new InternalRuntimeException(msg("Error adding new entry - already contained a run for start offset {}", getAbsoluteStartOffset()));
        }

        maybeMergeWithAdjacentNeighbours();
    }

    public RunLengthEntry maybeMergeWithAdjacentNeighbours() {
        // maybe merge up or down
        maybeMergeWithHigherNeighbour(); // up
        RunLengthEntry lowerNeighbour = getLowerNeighbour();
        if (lowerNeighbour != null) {
            // todo consider changing this so `this` never gets removed, only neighbours
            lowerNeighbour.maybeMergeWithHigherNeighbour(); // down
        }

        // after potential merging
        RunLengthEntry finalEntry = getParentSequence().ceiling(this);
        return finalEntry;
    }

    public void setRunLength(final long runLength) {
        if (runLength < 1) {
            throw new IllegalArgumentException(msg("Cannot have a run-length of {}", runLength));
        }
        this.runLength = runLength;
    }

    /**
     * Candidate should always be higher
     *
     * @return true if merged
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
            getParentSequence().remove(mergeCandidate);
            return true;
        }
        return false;
    }

    // todo change to optional
    protected RunLengthEntry getLowerNeighbour() {
        return parentSequence.lower(this);
    }

    // todo change to optional
    private RunLengthEntry getHigherNeighbour() {
        return parentSequence.higher(this);
    }

    private boolean isAdjacentTo(RunLengthEntry mergeCandidate) {
        return getEndOffsetInclusive() + 1 == mergeCandidate.getAbsoluteStartOffset();
    }

    private boolean hasSameStateAs(RunLengthEntry target) {
        if (target == null) return false;
        return getOffsetState() == target.getOffsetState();
    }

    /**
     * Inclusive end offset of the range this entry represents
     */
    public long getEndOffsetInclusive() {
        return absoluteStartOffset + runLength - 1;
    }

    public RunLengthEntry(long newBaseOffset, RunLengthSequence parentSequence) {
        // todo - state is ignored for use as key - code smell
        this(newBaseOffset, null, parentSequence);
    }

    /**
     * @param targetSequence the collection the new inflated entry will be added to
     * @return creates a new entry, with it's run-length expanded to encapsulate it's higher adjacent offsets as
     *         positive
     */
    protected RunLengthEntry maybeInFlatOverMissing(final RunLengthSequence targetSequence) {
        if (isSucceeded()) {
            // merge with potential adjacent missing

            var newRunLength = getRunLength();
            var newAbsoluteStartOffset = getAbsoluteStartOffset();

            boolean inflationOccurred = false;

            // up
            {
                var highGap = getHigherGap();
                if (highGap.isPresent()) {
                    newRunLength += highGap.get();
                    inflationOccurred = true;
                }
            }

            // down
            {
                var lowGap = getLowerGap();
                if (lowGap.isPresent()) {
                    newAbsoluteStartOffset -= lowGap.get();
                    newRunLength += lowGap.get();
                    inflationOccurred = true;
                }
            }

            if (inflationOccurred) {
                return new RunLengthEntry(newAbsoluteStartOffset, newRunLength, targetSequence, OffsetState.SUCCEEDED);
            } else {
                return this;
            }
        } else {
            return this;
        }
    }

    public boolean isSucceeded() {
        return getOffsetState() == OffsetState.SUCCEEDED;
    }

    private Optional<Long> getHigherGap() {
        RunLengthEntry higherNeighbour = getHigherNeighbour();
        if (higherNeighbour != null) {
            long gap = higherNeighbour.getAbsoluteStartOffset() - getEndOffsetInclusive() - 1;
            if (gap > 0) {
                return Optional.of(gap);
            }
        }
        return Optional.empty();
    }

    private Optional<Long> getLowerGap() {
        RunLengthEntry lower = getLowerNeighbour();
        if (lower != null) {
            long gap = lower.getEndOffsetInclusive() - getAbsoluteStartOffset();
            if (gap > 0) {
                return Optional.of(gap);
            }
        }
        return Optional.empty();
    }

    public boolean isNegative() {
        return getOffsetState() == OffsetState.INCOMPLETE;
    }

    /**
     * todo docs
     */
    public int getRelativeEndOffsetFromBase(final long baseOffset) {
        return Math.toIntExact(getRelativeStartOffsetFromBase(baseOffset) + runLength - 1);
    }

    public long getRelativeStartOffsetFromBase(final long baseOffset) {
        return Math.toIntExact(absoluteStartOffset - baseOffset);
    }

    @Override
    public int compareTo(final RunLengthEntry o) {
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
        // extend the run length
        long newExtendedRunLength = getRunLength() + 1;
        setRunLength(newExtendedRunLength);
        // move the start offset down 1
        absoluteStartOffset--;

        // maybe we can merge with the previous entry?
        RunLengthEntry nextLower = getLowerNeighbour();
        if (nextLower != null) {
            nextLower.maybeMergeWithHigherNeighbour();
        }
    }

//        private void mergeWithHigher(RunLengthEntry higherNeigh) {
//            // extend my run length to include theirs
//            long newExtendedRunLength = getRunLength() + higherNeigh.getRunLength();
//            setRunLength(newExtendedRunLength);
//
//            // remove them
//            getAllRunLengths().remove(higherNeigh);
//        }

    public boolean ingestNewNeighbourEntry(long newBaseOffset, long relativeOffsetFromBase, final OffsetState state) {
        long absoluteOffset = newBaseOffset + relativeOffsetFromBase;

// can never contain - no segmenting
        if (contains(absoluteOffset)) {
            // new offset state collides with an existing entry
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
        RunLengthEntry nextHigher = parentSequence.higher(closestExistingEntry);

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
            var lower = new RunLengthEntry(newBaseOffset, this.getOffsetState(), parentSequence);
            parentSequence.add(lower);

            // give a chance to merge
            lower.getLowerNeighbour().maybeMergeWithHigherNeighbour();
        }

        // we need to split the middle part
        var middle = new RunLengthEntry(newBaseOffset, 1L, parentSequence, getOffsetState().invert());
        parentSequence.add(middle);

        // we need to split the upper part
        var high = offset - getEndOffsetInclusive();
        if (high > 0) {
            // we need to split the upper part
            var upperRun = getEndOffsetInclusive() - newBaseOffset;
            var upper = new RunLengthEntry(newBaseOffset + 1, this.getOffsetState(), parentSequence);
            parentSequence.add(upper);

            // give a chance to merge
            upper.maybeMergeWithHigherNeighbour();
        }

        // remove myself
        parentSequence.remove(this);
    }

    /**
     * todo docs
     */
    private void splitOld(long newBaseOffset, long relativeOffsetFromBase, long absoluteOffset, RunLengthEntry closestExistingEntry, RunLengthEntry nextHigher) {
        OffsetState state = null; // delete me

        // remove the old entry which must have been incompletes
        boolean missing = !parentSequence.remove(closestExistingEntry);
        if (missing)
            throw new InternalRuntimeException("Cant find element that previously existed");

        long newRunCumulative = 1;

        Long offsetStartRelative = null;

        // create the three to replace the intersected node - 1 incomplete, 1 complete (this one), 1 incomplete
        int firstRun = toIntExact(absoluteOffset - closestExistingEntry.absoluteStartOffset);
        Long middleRelativeOffset = null;
        long firstRelativeOffset = closestExistingEntry.getRelativeStartOffsetFromBase(getParentSequence().getOriginalBaseOffset());

        if (firstRun > 0) {
            // large gap to fill
//                RunLengthEntry runLengthEntry = new RunLengthEntry(closestExistingEntry.startOffset, firstRun);
            RunLengthEntry first = getParentSequence().addRunLength(newBaseOffset, firstRun, firstRelativeOffset, state);
            middleRelativeOffset = first.getRelativeStartOffsetFromBase(getParentSequence().getOriginalBaseOffset()) + first.runLength;
        } else {
            // combine with the neighbor as there's no gap
            // check for a lower neighbour
            RunLengthEntry previous = parentSequence.lower(closestExistingEntry);
            if (previous != null && previous.getEndOffsetExclusive() == absoluteOffset) {
                // lower neighbor connects - combine
                newRunCumulative = newRunCumulative + previous.runLength;
                offsetStartRelative = previous.getRelativeStartOffsetFromBase(newBaseOffset);
                parentSequence.remove(previous);
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

                RunLengthEntry middle = getParentSequence().addRunLength(newBaseOffset, newRunCumulative, offsetStartRelative, state);

                // add incomplete filler
                int lastRange = toIntExact(closestExistingEntry.getEndOffsetInclusive() - absoluteOffset);
                if (lastRange > 0) {
                    getParentSequence().addRunLength(newBaseOffset, lastRange, middleRelativeOffset + 1, state);
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
                parentSequence.remove(nextHigher);
                if (offsetStartRelative == null)
                    offsetStartRelative = nextHigher.getRelativeStartOffsetFromBase(newBaseOffset) - 1; // shift left one place if not already established
                RunLengthEntry end = getParentSequence().addRunLength(newBaseOffset, newRunCumulative, offsetStartRelative, state);
            } else {
                throw InternalRuntimeException.msg("Invalid gap {}", gapUpward);
            }
        }
    }

    public long getEndOffsetExclusive() {
        return absoluteStartOffset + runLength;
    }

    /**
     * New positive doesn't with us, so we need to add a new entry
     */
    private void addNewNeighbour(final long newBaseOffset, final long relativeOffsetFromBase, final OffsetState state) {
//            int myEndOffset = getRelativeEndOffsetFromBase(originalBaseOffset);
//            var newOffset = newBaseOffset + relativeOffsetFromBase;
//
//            // extending the range
//            // is there a gap?
//            var isAboveUs = newOffset > absoluteStartOffset;
//
//            // gap analysis
//            long gapBelow = getAbsoluteStartOffset() - newOffset;
//            long gapAbove = newOffset - getEndOffsetInclusive();
////            long gapSizeBetweenEntries = relativeOffsetFromBase - myEndOffset;
//            final boolean newNeighbourAboveWouldBeAdjacent = isAboveUs && gapAbove == 1;
//            final boolean newNeighbourBelowWouldBeAdjacent = !isAboveUs && gapBelow == 1;
//            final boolean neighbourNotAdjacent = gapAbove > 1 || gapBelow > 1;


        Optional<Long> maybeAdjacentScore = calculateAdjacencyScore(newBaseOffset, relativeOffsetFromBase, state);

        if (maybeAdjacentScore.isPresent()) {
            // pre merge the new state without adding the new entry - this isn't actually needed, as we need to solve for merging after adding run lengths generally anyway
            long adjacentScore = maybeAdjacentScore.get();
            if (adjacentScore == 1) {
                extendUpByOne();
            } else if (adjacentScore == -1) {
                extendDownByOne();
            }
        } else {
            // neighbour is not directly adjacent
            // add new run entry
            getParentSequence().addRunLength(newBaseOffset, 1, relativeOffsetFromBase, state);
            // would need to add the merge check call here if we were not doing it before adding the new entry
        }

//            if (newNeighbourAboveWouldBeAdjacent) {
//                final RunLengthEntry higherNeighbour = getHigherNeighbour();
//                boolean statusMatchAbove = hasSameStateAs(higherNeighbour);
//                if (statusMatchAbove) {
//                    extendUpByOne();
//                }
//            } else if (newNeighbourBelowWouldBeAdjacent) {
//                boolean statusMatchBelow = hasSameStateAs(getLowerNeighbour());
//                if (statusMatchBelow) {
//                    extendDownByOne();
//                }
//            } else if (neighbourNotAdjacent) {
//                // neighbour is not directly adjacent
//                // add new run entry
//                addRunLength(newBaseOffset, 1, relativeOffsetFromBase, state);
//            } else {
//                throw InternalRuntimeException.msg("Invalid gap between entries above {} or below {}", gapAbove, gapBelow);
//            }
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

    private Optional<Long> calculateAdjacencyScore(long newBaseOffset, long relativeOffsetFromBase, OffsetState newNeighbourState) {
//            int myEndOffset = getRelativeEndOffsetFromBase(originalBaseOffset);
        var newOffset = newBaseOffset + relativeOffsetFromBase;

        // extending the range
        // is there a gap?
        var isAboveUs = newOffset > absoluteStartOffset;

        // gap analysis
        long gapBelow = getAbsoluteStartOffset() - newOffset;
        long gapAbove = newOffset - getEndOffsetInclusive();
//            long gapSizeBetweenEntries = relativeOffsetFromBase - myEndOffset;
        final boolean newNeighbourAboveWouldBeAdjacent = isAboveUs && gapAbove == 1;
        final boolean newNeighbourBelowWouldBeAdjacent = !isAboveUs && gapBelow == 1;
//            final boolean neighbourNotAdjacent = gapAbove > 1 || gapBelow > 1;

        if (newNeighbourAboveWouldBeAdjacent) {
//                final RunLengthEntry higherNeighbour = getHigherNeighbour();
//                boolean statusMatchAbove = hasSameStateAs(higherNeighbour);
//                boolean statusMatchAbove = ;
            if (hasSameStateAs(newNeighbourState)) {
                return Optional.of(1L);
            }
        } else if (newNeighbourBelowWouldBeAdjacent) {
//                boolean statusMatchBelow = hasSameStateAs(getLowerNeighbour());
//                if (statusMatchBelow) {
            if (hasSameStateAs(newNeighbourState)) {
                return Optional.of(-1L);
            }
//            } else if (neighbourNotAdjacent) {
        }

        // neighbour is not directly adjacent, or states don't match
        return Optional.empty();
    }

    private boolean hasSameStateAs(OffsetState newNeighbourState) {
        return newNeighbourState == getOffsetState();
    }

    /**
     * As negatives aren't tracked, we derive it
     */
    // todo delete?
    public Long getNegativeNeighbourRunLength() {
        final RunLengthEntry lowerPositive = getParentSequence().lower(this);
        if (lowerPositive == null) {
            // this run length is the start, so derive the genesis negative run-length
            // means we are the first entry
            return getAbsoluteStartOffset() - getParentSequence().getOriginalBaseOffset();
        } else {
            return this.absoluteStartOffset - lowerPositive.getEndOffsetInclusive() - 1;
        }
    }

    public enum OffsetState {
        SUCCEEDED,
        INCOMPLETE;

        public OffsetState invert() {
            return switch (this) {
                case SUCCEEDED -> INCOMPLETE;
                case INCOMPLETE -> SUCCEEDED;
            };
        }

    }
}