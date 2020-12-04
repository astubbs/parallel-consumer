package io.confluent.parallelconsumer;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.*;

/**
 * todo docs tail runlength?
 */
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
    private List<Integer> runLengthEncodingIntegers;

    private Optional<byte[]> encodedBytes = Optional.empty();

    private final Version version; // default to new version

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(baseOffset, offsetSimultaneousEncoder);
        version = newVersion;

        init();
    }

    private void init() {
        runLengthEncodingIntegers = new ArrayList<>();
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
        runLengthEncodingIntegers.add(currentRunLengthCount); // add tail

        int entryWidth = getEntryWidth();

        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(runLengthEncodingIntegers.size() * entryWidth);

        for (final Integer runlength : runLengthEncodingIntegers) {
            switch (version) {
                case v1 -> {
                    final short shortCastRunlength = runlength.shortValue();
                    if (runlength != shortCastRunlength)
                        throw new RunlengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", runlength, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(runlength);
                }
            }
        }

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

        encodeCompletedOffset((int) relativeOffset);
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
        long longDelta = newBaseOffset - originalBaseOffset;
        int baseDelta = (int) longDelta;
        if (baseDelta != longDelta) throw new InternalRuntimeError("Cast error");

        // for each run entry
        List<Integer> indexesToRemove = new ArrayList<>();
        List<Integer> newRunLengths = new ArrayList<>();
        for (Integer aRunLength : runLengthEncodingIntegers) {
            if (aRunLength < baseDelta) {
//                indexesToRemove.add(aRunLength);
            } else {
                int adjustedRunLength = aRunLength - baseDelta;
                newRunLengths.add(adjustedRunLength);
            }
        }
//        for (final Integer aRunLength : runLengthEncodingIntegers) {
//            if (aRunLength < baseDelta) {
//                toRemove.add(aRunLength)
//            }
//        }
        // check if it's above the new base, if not remove it
        // then subtract baseDelta


        // truncate at new relative delta
//            runLengthEncodingIntegers = runLengthEncodingIntegers.subList((int) baseDelta, runLengthEncodingIntegers.size());

        this.originalBaseOffset = newBaseOffset;

//        currentRunLengthCount = 0;
//        previousRelativeOffsetFromBase = 0;
//        previousRunLengthState = false;

        enable();
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        int numEntries = runLengthEncodingIntegers.size();
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
        checkForIncompletesGap(currentIsComplete, relativeOffsetFromBase);

        // run length
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;

        //
        int delta = relativeOffsetFromBase - previousRelativeOffsetFromBase;
        currentRunLengthCount = currentRunLengthCount + delta;

        if (currentOffsetMatchesOurRunLengthState) {
//            currentRunLengthCount++; // incorrect - assumes continuous offsets in source partition (think compacting)
            // no op
        } else {
            previousRunLengthState = currentIsComplete;
            runLengthEncodingIntegers.add(currentRunLengthCount);
            currentRunLengthCount = 1; // reset to 1
        }
        previousRelativeOffsetFromBase = relativeOffsetFromBase;
    }

    private void checkForIncompletesGap(final boolean currentIsComplete, final int relativeOffsetFromBase) {
        boolean bothThisRecordAndPreviousRecordAreComplete = previousRunLengthState && currentIsComplete;
        if (bothThisRecordAndPreviousRecordAreComplete) {
            // check for gap - if there's a gap, we need to assume all inbetween are incomplete. If they don't exist, this action has no affect, as we only use it to skip succeeded
            Integer previousSucceedRunLengthEntry = runLengthEncodingIntegers.get(runLengthEncodingIntegers.size() - 1);
            int gap = relativeOffsetFromBase - previousSucceedRunLengthEntry;
            if (gap > 0) {
                // there is a gap, so first insert the incomplete
                runLengthEncodingIntegers.add(gap);
                currentRunLengthCount = 1; // reset to 1
                previousRunLengthState = false;
                // reverse engineer the previous offset from base - we have to add up all runlengnths. This could be perhaps cached
                previousRelativeOffsetFromBase = previousSucceedRunLengthEntry + gap;
            }
        }
    }

}
