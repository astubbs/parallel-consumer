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
    private int currentRunLengthCount = 0;

    private boolean previousRunLengthState = false;

    /**
     * Stores all the run lengths
     */
    private List<Integer> runLengthEncodingIntegers;

    private Optional<byte[]> encodedBytes = Optional.empty();

    private final Version version; // default to new version

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(baseOffset, offsetSimultaneousEncoder);
        // run length setup
        runLengthEncodingIntegers = new ArrayList<>();
        version = newVersion;
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
        boolean reinitialise = false;

        long newLength = currentHighestCompleted - newBaseOffset;
//        if (originalLength != newLength) {
////        if (this.highestSuceeded != currentHighestCompleted) {
//            log.debug("Length of Bitset changed {} to {}",
//                    originalLength, newLength);
//            reinitialise = true;
//        }

        if (originalBaseOffset != newBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work - need to shift bitset right",
                    this.originalBaseOffset, newBaseOffset);
            reinitialise = true;
        }

        if (newBaseOffset < originalBaseOffset)
            throw new InternalRuntimeError("");

        if (reinitialise) {
            long baseDelta = newBaseOffset - originalBaseOffset;
            // truncate at new relative delta
//            runLengthEncodingIntegers = runLengthEncodingIntegers.subList((int) baseDelta, runLengthEncodingIntegers.size());
            runLengthEncodingIntegers = new ArrayList<>();
        }

        encodeCompletedOffset((int) relativeOffset);
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        int numEntries = runLengthEncodingIntegers.size();
        if (currentRunLengthCount > 0)
            numEntries = numEntries + 1;
        int accumulativeEntrySize = numEntries * getEntryWidth();
        return accumulativeEntrySize + standardOverhead;
    }

    @Override
    public byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

    private void encodeRunLength(final boolean currentIsComplete, final int relativeOffset) {
        // run length
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
        if (currentOffsetMatchesOurRunLengthState) {
            currentRunLengthCount++;
        } else {
            previousRunLengthState = currentIsComplete;
            runLengthEncodingIntegers.add(currentRunLengthCount);
            currentRunLengthCount = 1; // reset to 1
        }
    }

}
