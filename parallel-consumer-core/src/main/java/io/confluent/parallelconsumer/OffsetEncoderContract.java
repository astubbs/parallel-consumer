package io.confluent.parallelconsumer;

public interface OffsetEncoderContract {

    /**
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeIncompleteOffset(final int relativeOffset);

    /**
     * @param relativeOffset Offset relative to the base offset (e.g. offset - baseOffset)
     */
    void encodeCompletedOffset(final int relativeOffset);

    int getEncodedSize();

    byte[] getEncodedBytes();

}
