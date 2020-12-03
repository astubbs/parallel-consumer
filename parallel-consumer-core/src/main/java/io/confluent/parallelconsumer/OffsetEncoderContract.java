package io.confluent.parallelconsumer;

public interface OffsetEncoderContract {

    /**
     * @param baseOffset need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker);

    /**
     * @param baseOffset need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeCompletedOffset(final long baseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker);

    int getEncodedSize();

    byte[] getEncodedBytes();

    /**
     * Used for comparing encoders
     */
    int getEncodedSizeEstimate();

}
