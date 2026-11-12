package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link InputStream} that reads a {@link ByteBuffer} in place, from its position to its limit, advancing the
 * position only as far as the reader has consumed. No copy is made.
 * <p>
 * <b>Why PC has its own.</b> The offset-map decompressors used to wrap their input in Kafka's
 * {@code org.apache.kafka.common.utils.ByteBufferInputStream}, which is not part of Kafka's public API. Kafka 4.4.0
 * moved it to {@code org.apache.kafka.common.utils.internals}, so a PC jar built against an earlier Kafka threw
 * {@link NoClassDefFoundError} the first time it decompressed committed offset metadata (astubbs#539). Owning the
 * twenty lines removes the dependency on where Kafka keeps them; {@code ArchitectureTest} keeps main code from
 * reaching back into that package.
 * <p>
 * Not thread-safe, and it shares the buffer's position with whoever else holds the buffer - as Kafka's class did.
 */
public final class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(final byte[] bytes, final int off, final int len) {
        if (off < 0 || len < 0 || len > bytes.length - off) {
            throw new IndexOutOfBoundsException("off=" + off + ", len=" + len + ", array length=" + bytes.length);
        }
        if (len == 0) {
            return 0;
        }
        if (!buffer.hasRemaining()) {
            return -1;
        }
        final int toRead = Math.min(len, buffer.remaining());
        buffer.get(bytes, off, toRead);
        return toRead;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }
}
