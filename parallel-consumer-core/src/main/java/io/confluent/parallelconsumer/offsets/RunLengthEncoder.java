package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Range;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

//    /**
//     * The current run length being counted / built
//     */
//    @ToString.Include
//    private long currentRunLengthCount;

//    /**
//     * todo docs
//     */
//    private int previousRelativeOffsetFromBase;

//    /**
//     * todo docs
//     */
//    private boolean previousRunLengthState;

//    /**
//     * Stores all the run lengths
//     */
//    @Deprecated//?
//    @Getter
//    private List<Integer> runLengthEncodingIntegers;

    @Getter
    private final RunLengthSequence runLengthSequence = new RunLengthSequence();

    private Optional<byte[]> encodedBytes = Optional.empty();

//    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(baseOffset, offsetSimultaneousEncoder, newVersion);

//        init();
    }
//
//    private void init() {
//        // todo delete
//        runLengthEncodingIntegers = new ArrayList<>();
//
////        currentRunLengthCount = 0;
//        previousRelativeOffsetFromBase = 0;
//        previousRunLengthState = false;
//    }

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

        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(getRunLengthSequence().getSize() * entryWidth);

        // for (final Integer run : getRunLengthEncodingIntegers()) {
//        for (final RunLengthEntry n : getSerializeViewOnData()) {
        getSerializeViewOnData().forEach(n -> {
            Long runLength = n.getRunLength();
            switch (version) {
                case v1 -> {
                    final short shortCastRunLength = runLength.shortValue();
                    if (runLength != shortCastRunLength)
                        try {
                            throw new RunLengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", runLength, shortCastRunLength));
                        } catch (RunLengthV1EncodingNotSupported e) {
                            throw new RuntimeException(e);
                        }
                    runLengthEncodedByteBuffer.putShort(shortCastRunLength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(toIntExact(runLength));
                }
            }
        });

        byte[] array = runLengthEncodedByteBuffer.array();
        encodedBytes = Optional.of(array);
        return array;
    }

    private int getEntryWidth() {
        return switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
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

    /**
     * For serializing, we encode missing data as succeeded, because explicitly negative offsets get explicitly tracked
     * upon reloading - and we may never receive those offsets. New versions of the ingestion pipeline though check for
     * missing offsets in polled batch and truncate the data - so this may not be required, however consistent with
     * existing serialization formats.
     *
     * @see RunLengthEntry#maybeInFlatOverMissing
     * @see io.confluent.parallelconsumer.state.PartitionState#maybeTruncateOrPruneTrackedOffsets
     */
    // visible for testing
    protected Stream<RunLengthEntry> getSerializeViewOnData() {
        var serializedView = new RunLengthSequence();
        return getRunLengthSequence().stream().map(n -> n.maybeInFlatOverMissing(serializedView));
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    public void encodeIncompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
//        maybeTruncateBase(newBaseOffset, currentHighestCompleted);
//        encodeRunLength(false, baseOffset, relativeOffset);
//        //no-op
//        //Entries being added should always be complete, as the range by definition starts out incomplete. We never add incompletes because things never transition from complete to incomplete.");

        getRunLengthSequence().encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(newBaseOffset, relativeOffset, currentHighestCompleted, RunLengthEntry.OffsetState.INCOMPLETE);
    }

//    /**
//     * Returns the negative and positive run-lengths by calculating the implicit negative entries
//     */
//    public List<Long> calculateFullRelativeRunLengthsOld() {
//        List<Long> bothRunLengths = new ArrayList<>();
//        for (RunLengthEntry runLength : getAllRunLengths()) {
//            // negative
//            var start = runLength.getLowerNeighbour();
////            var le
//            bothRunLengths.add(runLength.getNegativeNeighbourRunLength());
////
////            Optional<Long> negativeNeighbourRunLength = runLength.getNegativeNeighbourRunLength();
////            if (negativeNeighbourRunLength.isPresent()) {
////                bothRunLengths.add(negativeNeighbourRunLength.get());
////            }
//            // positive
//            bothRunLengths.add(runLength.getRunLength());
//        }
//        return bothRunLengths;
//    }

    @Override
    public void encodeCompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
//        maybeReinitialise(newBaseOffset, currentHighestCompleted);

//        encodeRunLength(true, newBaseOffset, relativeOffset);
//        maybeTruncateBase(newBaseOffset, currentHighestCompleted);

        getRunLengthSequence().encodeCompleteAndSegmentOrCombinePreviousEntryIfNeeded(newBaseOffset, relativeOffset, currentHighestCompleted, RunLengthEntry.OffsetState.SUCCEEDED);
    }

    @Override
    public byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

    @Override
    public int getEncodedSizeEstimate() {
        int numEntries = getRunLengthSequence().getSize();
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

    /**
     * todo docs
     */
    public List<Long> calculateFullRelativeRunLengthsForTestingWithMissingAsIncomplete() {
        List<Long> bothRunLengths = new ArrayList<>();
        long currentOffsetExpected = 0L;

        for (RunLengthEntry runLength : getRunLengthSequence()) {
            if (currentOffsetExpected != runLength.getAbsoluteStartOffset()) {
                // make missing data as incomplete
//                var missingData = new RunLengthEntry(currentOffsetExpected, runLength.getAbsoluteStartOffset(), RunLengthEntry.OffsetState.SUCCEEDED);
                final long sizeOfGapOfMissingData = runLength.getAbsoluteStartOffset() - currentOffsetExpected;
                bothRunLengths.add(sizeOfGapOfMissingData);
            }

            bothRunLengths.add(runLength.getRunLength());

            currentOffsetExpected = runLength.getEndOffsetInclusive() + 1;
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
        return getRunLengthSequence().runLengthsWithTrailingNegativePrunedAndMissingAsIncomplete().stream()
                .mapToLong(RunLengthEntry::getRunLength)
                .boxed().collect(Collectors.toList());
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


//    private long getPreviousRelativeOffset2(final long newBaseOffset, final int offset) {
//        RunLengthEntry dynamicPrevious = allRunLengths.floor(new RunLengthEntry(offset));
//        long previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - toIntExact(originalBaseOffset);
//        return previousOffset - toIntExact(newBaseOffset + currentRunLengthCount);
//    }

    /**
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsetsV1() {
        List<Long> successfulOffsets = new ArrayList<>();
        boolean succeeded = true;
//        int previous = 0;
        long offsetPosition = originalBaseOffset;
        //for (final Integer run : runLengthEncodingIntegers) {
        for (final RunLengthEntry runLengthEntry : runLengthSequence) {
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

    @Deprecated
    public void truncateRunlengths(int i) {
        getRunLengthSequence().truncateRunLengthsV2(i);
    }

    public List<Long> calculateSucceededActualOffsets() {
        return getRunLengthSequence().calculateSucceededActualOffsets();
    }

    public void truncateRunLengthsV2(int i) {
        truncateRunLengthsV2(i);
    }
}
