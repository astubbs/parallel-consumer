package io.confluent.parallelconsumer;

public interface OffsetEncoderContract {

    /**
     * @param baseOffset need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeIncompleteOffset(final long baseOffset, final long relativeOffset);

    /**
     * @param baseOffset need the baseOffset also, as it may change with new success (highest committable may rise)
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeCompletedOffset(final long baseOffset, final long relativeOffset);

    int getEncodedSize();

    byte[] getEncodedBytes();

}
