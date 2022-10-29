package io.confluent.parallelconsumer.offsets;

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static io.confluent.parallelconsumer.offsets.RunLengthEncoder.LARGE_NUMER_OF_RUN_LENGTHS;
import static java.lang.Math.toIntExact;

/**
 * @author Antony Stubbs
 */
// sort members
@Slf4j
public class RunLengthSequence implements Iterable<RunLengthEntry> {

    /**
     * The highest committable offset - the next expected offset to be returned by the broker. So by definition, this
     * index in our offset map we're encoding, is always incomplete.
     */
    @Getter(AccessLevel.PROTECTED)
    @ToString.Include
    protected long originalBaseOffset;

    /**
     * Only need to track positive run lengths, negatives can be derived. This makes iteratively building the run length
     * structure trivial as we don't need to do any segmenting.
     */
    @Getter(AccessLevel.PROTECTED)
    @Delegate
    private NavigableSet<RunLengthEntry> allRunLengths = new TreeSet<>();

    protected int getSize() {
        //return runLengthEncodingIntegers.size();
        return allRunLengths.size();
    }

    /**
     * Returns the negative and positive run-lengths by calculating the implicit negative entries, for testing.
     * <p>
     * Warning: Marks missing data as incomplete - NOT SUCCEEDED, as serialisation requires
     */
    public List<RunLengthEntry> rawRunLengths() {
        return getAllRunLengths().stream().toList();
    }

    /**
     * todo docs
     */
// todo rename
    protected boolean encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(final long newBaseOffset, final long relativeOffsetFromBase, final long currentHighestCompleted, final RunLengthEntry.OffsetState state) {
        maybeTruncateBase(newBaseOffset, currentHighestCompleted);

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
                neighbour = allRunLengths.ceiling(new RunLengthEntry(absoluteOffset));
//                neighbour = addRunLength(newBaseOffset, 1, relativeOffsetFromBase);
            }

            return neighbour.ingestNewNeighbourEntry(newBaseOffset, relativeOffsetFromBase, state);
        }
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

    /**
     * Adds a new run-length entry
     *
     * @return the added entry
     */
// todo consider moving to entry class?
    protected RunLengthEntry addRunLength(final long newBaseOffset, final long runLength, final long relativeOffsetFromBase, final RunLengthEntry.OffsetState state) throws ArithmeticException {
        // v1
//        runLengthEncodingIntegers.add(runLength);

        // v2
        int offset = toIntExact(newBaseOffset + relativeOffsetFromBase);
//        if (!runLengthOffsetPairs.isEmpty()) {
//            RunLengthEntry previous = runLengthOffsetPairs.last();
//            if (previous != null && offset != previous.getEndOffsetInclusive() + 1)
//                throw new IllegalArgumentException(msg("Can't add a run length offset {} that's not continuous from previous {}", offset, previous));
//        }
        RunLengthEntry entry = new RunLengthEntry(offset, runLength, this, state);

        return entry;
    }

    private RunLengthEntry getLowerNeighbour(long absoluteOffset) {
        return allRunLengths.lower(new RunLengthEntry(absoluteOffset));
    }

//    /**
//     * For each run entry, see if it's below the base, if it is, drop it. Find the first run length that intersects with
//     * the new base, and truncate it. Finish.
//     */
//    @Deprecated
//    void truncateRunlengths(final int newBaseOffset) {
//        int currentOffset = 0;
//        if (runLengthEncodingIntegers.size() > 1000) {
//            log.info("length: {}", runLengthEncodingIntegers.size());
//        }
//        int index = 0;
//        int adjustedRunLength = -1;
//        for (Integer aRunLength : runLengthEncodingIntegers) {
//            currentOffset = currentOffset + aRunLength;
//            if (currentOffset <= newBaseOffset) {
//                // drop from new collection
//            } else {
//                // found first intersection - truncate
//                adjustedRunLength = currentOffset - newBaseOffset;
//                break; // done
//            }
//            index++;
//        }
//        if (adjustedRunLength == -1) throw new InternalRuntimeException("Couldn't find interception point");
//        List<Integer> integers = runLengthEncodingIntegers.subList(index, runLengthEncodingIntegers.size());
//        integers.set(0, adjustedRunLength); // overwrite with adjusted
//
//        // swap
//        this.runLengthEncodingIntegers = integers;
//
//        //
//        this.originalBaseOffset = newBaseOffset;
//    }

    private void reinitialise(final long newBaseOffset, final long currentHighestCompleted) {
//        long longDelta = newBaseOffset - originalBaseOffset;
//        int baseDelta = JavaUtils.safeCast(longDelta);
        truncateRunLengthsV2(newBaseOffset);


//        currentRunLengthCount = 0;
//        previousRelativeOffsetFromBase = 0;
//        previousRunLengthState = false;

//        enable();
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
            RunLengthEntry highestEntryThatCouldContainTarget = allRunLengths.floor(new RunLengthEntry(newBaseOffset, this)
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
                    long adjustedRunLength = highestEntryThatCouldContainTarget.getRunLength() - (newBaseOffset - highestEntryThatCouldContainTarget.getAbsoluteStartOffset());
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
     * Returns the negative and positive run-lengths by calculating the implicit negative entries, for testing.
     * <p>
     * Warning: Marks missing data as incomplete - NOT SUCCEEDED, as serialisation requires
     */
    public RunLengthSequence runLengthsWithTrailingNegativePrunedAndMissingAsIncomplete() {
        RunLengthSequence bothRunLengths = new RunLengthSequence();
        long currentOffsetExpected = originalBaseOffset;
        for (RunLengthEntry runLength : getAllRunLengths()) {

            if (currentOffsetExpected != runLength.getAbsoluteStartOffset()) {
                // make missing data as incomplete
                final long sizeOfGapOfMissingData = runLength.getAbsoluteStartOffset() - currentOffsetExpected;
                var missingData = new RunLengthEntry(currentOffsetExpected, sizeOfGapOfMissingData, bothRunLengths, RunLengthEntry.OffsetState.INCOMPLETE);
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

        // remove the run if it's negative, as we throw away these entries for serialisation
        final Optional<RunLengthEntry> maybeLast = bothRunLengths.maybeLast();
        if (maybeLast.isPresent()) {
            final RunLengthEntry last = maybeLast.get();
            if (last.isNegative()) {
                bothRunLengths.remove(last);
            }
        }

        return bothRunLengths;
    }

    private Optional<RunLengthEntry> maybeLast() {
        if (allRunLengths.isEmpty()) return Optional.empty();
        else return Optional.of(allRunLengths.last());
    }

    private RunLengthEntry getHigherNeighbour(long absoluteOffset) {
        return allRunLengths.higher(new RunLengthEntry(absoluteOffset));
    }

    private long getPreviousRelativeOffset(final int offset) {
        RunLengthEntry dynamicPrevious = allRunLengths.floor(new RunLengthEntry(offset));
        long previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.getRunLength() - toIntExact(originalBaseOffset);
        return previousOffset;
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
