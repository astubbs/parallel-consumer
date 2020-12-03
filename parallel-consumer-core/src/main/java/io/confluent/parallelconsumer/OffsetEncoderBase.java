package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base OffsetEncoder
 */
@Slf4j
abstract class OffsetEncoderBase implements OffsetEncoderContract, Comparable<OffsetEncoderBase> {

    private final OffsetSimultaneousEncoder offsetSimultaneousEncoder;
    private final long baseOffset;

    public OffsetEncoderBase(final long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        this.baseOffset = baseOffset;
        this.offsetSimultaneousEncoder = offsetSimultaneousEncoder;
    }

    protected abstract OffsetEncoding getEncodingType();

    protected abstract OffsetEncoding getEncodingTypeCompressed();

    public abstract void encodeIncompleteOffset(final int relativeOffset);

    public abstract void encodeCompletedOffset(final int relativeOffset);

    abstract byte[] serialise() throws EncodingNotSupportedException;

    public abstract int getEncodedSize();

    boolean quiteSmall() {
        return this.getEncodedSize() < OffsetSimultaneousEncoder.LARGE_INPUT_MAP_SIZE_THRESHOLD;
    }

    byte[] compress() throws IOException {
        return OffsetSimpleSerialisation.compressZstd(this.getEncodedBytes());
    }

    void register() throws EncodingNotSupportedException {
        final byte[] bytes = this.serialise();
        final OffsetEncoding encodingType = this.getEncodingType();
        this.register(encodingType, bytes);
    }

    private void register(final OffsetEncoding type, final byte[] bytes) {
        log.debug("Registering {}, with site {}", type, bytes.length);
        offsetSimultaneousEncoder.sortedEncodingData.add(new EncodedOffsetData(type, ByteBuffer.wrap(bytes)));
        offsetSimultaneousEncoder.encodingMap.put(type, bytes);
    }

    @SneakyThrows
    void registerCompressed() {
        final byte[] compressed = compress();
        final OffsetEncoding encodingType = this.getEncodingTypeCompressed();
        this.register(encodingType, compressed);
    }

    public abstract byte[] getEncodedBytes();

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker) {
        if (baseOffset != this.baseOffset) {
            throw new InternalRuntimeError("Na");
        }
        int castOffset = (int) relativeOffset;
        if (castOffset != relativeOffset)
            throw new IllegalArgumentException("Interger overflow");
        encodeIncompleteOffset(castOffset);
    }

    @Override
    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker) {
        if (baseOffset != this.baseOffset) {
            throw new InternalRuntimeError("Na");
        }
        int castOffset = (int) relativeOffset;
        if (castOffset != relativeOffset)
            throw new IllegalArgumentException("Interger overflow");
        encodeCompletedOffset(castOffset);
    }

    /**
     * Compared with returned encoding size
     * <p>
     * Note: this class has a natural ordering that is inconsistent with equals.
     *
     * @see #getEncodedSize()
     */
    @Override
    public int compareTo(final OffsetEncoderBase e) {
        return Integer.compare(this.getEncodedSizeEstimate(), e.getEncodedSizeEstimate());
    }
}
