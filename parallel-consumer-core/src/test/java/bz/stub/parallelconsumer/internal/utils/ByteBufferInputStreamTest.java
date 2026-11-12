package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The replacement for Kafka's {@code ByteBufferInputStream} (astubbs#539). The decompressors read compressed offset
 * metadata through it, so what is pinned is what they rely on: it reads exactly position-to-limit, it advances the
 * buffer only as far as it was read, and it ends with -1.
 */
class ByteBufferInputStreamTest {

    @Test
    void readsFromPositionToLimitAndThenEnds() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap("xxhello world!yy".getBytes(StandardCharsets.UTF_8));
        buffer.position(2);
        buffer.limit(14);

        var in = new ByteBufferInputStream(buffer);
        assertThat(in.available()).isEqualTo(12);

        byte[] out = new byte[64];
        int read = in.read(out, 0, out.length);

        assertThat(read).isEqualTo(12);
        assertThat(new String(out, 0, read, StandardCharsets.UTF_8)).isEqualTo("hello world!");
        assertThat(in.read()).isEqualTo(-1);
        assertThat(in.read(out, 0, out.length)).isEqualTo(-1);
    }

    /**
     * Zero-copy: the buffer moves only as far as the reader consumed, which is the property the issue's follow-up
     * comment asked to keep.
     */
    @Test
    void advancesTheBufferOnlyAsFarAsRead() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
        var in = new ByteBufferInputStream(buffer);

        byte[] out = new byte[2];
        assertThat(in.read(out, 0, 2)).isEqualTo(2);

        assertThat(buffer.position()).isEqualTo(2);
        assertThat(in.available()).isEqualTo(3);
    }

    @Test
    void singleByteReadsAreUnsigned() {
        var in = new ByteBufferInputStream(ByteBuffer.wrap(new byte[]{(byte) 0xFF, 0x01}));

        assertThat(in.read()).isEqualTo(255);
        assertThat(in.read()).isEqualTo(1);
        assertThat(in.read()).isEqualTo(-1);
    }

    @Test
    void aZeroLengthReadReturnsZeroEvenAtTheEnd() {
        var in = new ByteBufferInputStream(ByteBuffer.allocate(0));

        assertThat(in.read(new byte[4], 0, 0)).isEqualTo(0);
    }

    @Test
    void anOutOfRangeRequestIsRefused() {
        var in = new ByteBufferInputStream(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        byte[] out = new byte[2];

        assertThrows(IndexOutOfBoundsException.class, () -> in.read(out, 1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> in.read(out, -1, 1));
    }
}
