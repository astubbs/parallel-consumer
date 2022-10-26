package io.confluent.parallelconsumer.offsets;

public interface OffsetEncoderContract {

    /**
     * TODO this method isnt' actually used by any encoder
     *
     * @param baseOffset     need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) throws EncodingNotSupportedException;

    /**
     * @param baseOffset     need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeCompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) throws EncodingNotSupportedException;

    int getEncodedSize();

    byte[] getEncodedBytes();

    /**
     * Used for comparing encoders
     */
    int getEncodedSizeEstimate();

    void ensureCapacity(long base, long highest);

    /**
     * todo docs
     */
//    void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) throws EncodingNotSupportedException;

}
