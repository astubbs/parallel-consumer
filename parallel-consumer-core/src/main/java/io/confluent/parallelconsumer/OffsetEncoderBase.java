package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base OffsetEncoder
 * <p>
 * When encoding offset data beyond the low water mark, we only need to record information up until the highest
 * succeeded offset, as any beyond (because they have failed or haven't suceeded yet) we can treat as though we've never
 * seen them when we come across them again.
 *
 * @see WorkManager
 */
@Slf4j
abstract class OffsetEncoderBase implements OffsetEncoderContract, Comparable<OffsetEncoderBase> {

    private final OffsetSimultaneousEncoder offsetSimultaneousEncoder;

    private boolean disabled = false;

    /**
     * The highest committable offset - the next expected offset to be returned by the broker. So by definition, this
     * index in our offset map we're encoding, is always incomplete.
     */
    protected long originalBaseOffset;

    public OffsetEncoderBase(final long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        this.originalBaseOffset = baseOffset;
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
        int encodedSizeEstimate = getEncodedSizeEstimate();
        int length = bytes.length;
        log.debug("Registering {}, with actual size {} vs estimate {}", type, length, encodedSizeEstimate);
        int encodedSimate = getEncodedSizeEstimate();
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

//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
////        if (baseOffset != this.baseOffset) {
////            throw new InternalRuntimeError("Inconsistent");
////        }
//
//        int castOffset = (int) relativeOffset;
//        if (castOffset != relativeOffset)
//            throw new IllegalArgumentException("Interger overflow");
//
//        encodeIncompleteOffset(castOffset);
//    }

//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
//        if (baseOffset != this.baseOffset) {
//            throw new InternalRuntimeError("Na");
//        }
//        int castOffset = (int) relativeOffset;
//        if (castOffset != relativeOffset)
//            throw new IllegalArgumentException("Interger overflow");
//
//        encodeCompletedOffset(castOffset);
//    }

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

    public void disable(final EncodingNotSupportedException e) {
        disabled = true;
        log.warn("Disabling {}, {}", this.getEncodingType(), e.getMessage(), e);
    }

    public void enable() {
        disabled = false;
    }
}
