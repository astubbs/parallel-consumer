package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.ToString;

import java.nio.ByteBuffer;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.ByteArray;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.ByteArrayCompressed;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v1;

/**
 * Encodes offsets into a {@link ByteBuffer}. Doesn't have any advantage over  the {@link BitSetEncoder} and
 * {@link RunLengthEncoder}, but can be useful for testing and comparison.
 *
 * @author Antony Stubbs
 */
@ToString(callSuper = true)
public class ByteBufferEncoder extends OffsetEncoder {

    private final ByteBuffer bytesBuffer;

    public ByteBufferEncoder(final long baseOffset, final long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) {
        super(baseOffset, offsetSimultaneousEncoder, v1);
        // safe cast the length to an int, as we're not expecting to have more than 2^31 offsets
        final int safeCast = Math.toIntExact(length);
        this.bytesBuffer = ByteBuffer.allocate(1 + safeCast);
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return ByteArray;
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return ByteArrayCompressed;
    }

//    @Override
//    public void encodeIncompleteOffset(final long newBaseOffset, final long relativeOffset) throws EncodingNotSupportedException {
//        this.bytesBuffer.put((byte) 0);
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long newBaseOffset, final long relativeOffset) throws EncodingNotSupportedException {
//        this.bytesBuffer.put((byte) 1);
//    }

    @Override
    public byte[] serialise() {
        return this.bytesBuffer.array();
    }
//
//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset) {
//sdf
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset) {
//sdf
//    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        this.bytesBuffer.put((byte) 0);
    }

    @Override
    public void encodeCompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        this.bytesBuffer.put((byte) 1);
    }

    @Override
    public int getEncodedSize() {
        return this.bytesBuffer.capacity();
    }

    @Override
    public int getEncodedSizeEstimate() {
        return this.bytesBuffer.capacity();
    }

    @Override
    public void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) {
        throw new InternalRuntimeException("Na");
    }

    @Override
    public byte[] getEncodedBytes() {
        return this.bytesBuffer.array();
    }

}
