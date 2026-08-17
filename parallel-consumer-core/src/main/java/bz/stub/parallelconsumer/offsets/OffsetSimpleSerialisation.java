package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static bz.stub.parallelconsumer.internal.utils.BackportUtils.readFully;

/**
 * Methods for compressing, decompressing and encoding / encoding data.
 *
 * @author Antony Stubbs
 */
@UtilityClass
@Slf4j
public class OffsetSimpleSerialisation {

    @SneakyThrows
    static String encodeAsJavaObjectStream(final Set<Long> incompleteOffsets) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(incompleteOffsets);
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    private static TreeSet<Long> deserialiseJavaWriteObject(final byte[] decode) throws IOException, ClassNotFoundException {
        final Set<Long> raw;
        try (final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(decode))) {
            raw = (Set<Long>) objectInputStream.readObject();
        }
        return new TreeSet<>(raw);
    }

    @SneakyThrows
    static byte[] compressSnappy(final byte[] bytes) {
        try (final var out = new ByteArrayOutputStream();
             final var stream = new SnappyOutputStream(out)) {
            stream.write(bytes);
            return out.toByteArray();
        }
    }

    static ByteBuffer decompressSnappy(final ByteBuffer input) throws IOException {
        try (final var snappy = new SnappyInputStream(new ByteBufferInputStream(input))) {
            byte[] bytes = readFully(snappy);
            return ByteBuffer.wrap(bytes);
        }
    }

    static String base64(final ByteArrayOutputStream out) {
        final byte[] src = out.toByteArray();
        return base64(src);
    }

    static byte[] compressZstd(final byte[] bytes) throws IOException {
        final var out = new ByteArrayOutputStream();
        try (final var zstream = new ZstdOutputStream(out)) {
            zstream.write(bytes);
        }
        return out.toByteArray();
    }

    static byte[] compressGzip(final byte[] bytes) throws IOException {
        final var out = new ByteArrayOutputStream();
        try (final var zstream = new GZIPOutputStream(out)) {
            zstream.write(bytes);
        }
        return out.toByteArray();
    }

    static String base64(final byte[] src) {
        final byte[] encode = Base64.getEncoder().encode(src);
        final String out = new String(encode, OffsetMapCodecManager.CHARSET_TO_USE);
        log.trace("Final b64 size: {}", out.length());
        return out;
    }

    static byte[] decodeBase64(final String b64) {
        final byte[] bytes = b64.getBytes(OffsetMapCodecManager.CHARSET_TO_USE);
        return Base64.getDecoder().decode(bytes);
    }

    /**
     * Marks a metadata string as {@link Z85Codec} output rather than Base64.
     * <p>
     * The encoding's own magic byte is <em>inside</em> the payload (see
     * {@link OffsetSimultaneousEncoder#packEncoding}), so it is already string-encoded by the time anyone reads it and
     * cannot say which string codec was used. Hence a sentinel character outside the Base64 alphabet
     * ({@code [A-Za-z0-9+/=]}), which no previously written payload can start with.
     */
    static final char Z85_SENTINEL = '%';

    /**
     * The number of characters {@link #base64(byte[])} produces for a given number of input bytes - Base64's
     * {@code 4*ceil(n/3)}, padded, as {@link Base64#getEncoder()} emits it.
     *
     * @see Z85Codec#encodedLength(int)
     */
    static int base64Length(final int byteCount) {
        return 4 * ((byteCount + 2) / 3);
    }

    /**
     * Encodes a payload as whichever of Base64 or sentinel-prefixed Z85 is the <em>shorter</em> string, since the string
     * length is what the metadata cap and the back-pressure threshold are measured against.
     * <p>
     * Base64 wins ties. Z85's 25% expansion beats Base64's 33%, but the sentinel costs a character and both codecs round
     * up to their block size, so sentinel+Z85 is longer below 12 payload bytes, ties from 12 through 21, and wins from 22
     * bytes up, converging on ~6% shorter. Breaking ties towards Base64 therefore keeps every small payload in the form
     * older PC releases can read, and spends the compatibility cost only where there is actually density to gain.
     *
     * @see #decodeBase64OrZ85(String)
     */
    static String encodeShorterOfBase64OrZ85(final byte[] src) {
        if (Z85Codec.encodedLength(src.length) + 1 < base64Length(src.length)) {
            final String z85 = Z85_SENTINEL + Z85Codec.encode(src);
            log.trace("Final z85 size: {} (base64 would have been {})", z85.length(), base64Length(src.length));
            return z85;
        }
        return base64(src);
    }

    /**
     * Decodes a metadata string written by any version of PC: sentinel-prefixed Z85, or - for everything ever written
     * before the Z85 codec existed, and for small payloads still - bare Base64.
     * <p>
     * Dispatch is on the leading {@link #Z85_SENTINEL} alone, so anything else, blank strings and unrecognisable junk
     * included, goes down the Base64 path exactly as it did before this method existed. An empty string decodes to zero
     * bytes, which callers read as "this commit carried no offset map".
     *
     * @param metadata the {@code metadata} field of a committed offset. {@code null} is read as empty: kafka-clients
     *                 normalises null to {@code ""} in every {@link org.apache.kafka.clients.consumer.OffsetAndMetadata}
     *                 constructor, so PC's own read path cannot see one, but this is a recovery path for bytes written
     *                 by other tools and it should not turn a surprise into a {@link NullPointerException}
     * @return the payload bytes, magic byte first
     * @throws Z85DecodingException     if the string is sentinel-prefixed but is not Z85
     * @throws IllegalArgumentException if the string is not sentinel-prefixed and is not valid Base64
     * @see #encodeShorterOfBase64OrZ85(byte[])
     */
    static byte[] decodeBase64OrZ85(final String metadata) throws Z85DecodingException {
        if (metadata == null) {
            return new byte[0];
        }
        if (!metadata.isEmpty() && metadata.charAt(0) == Z85_SENTINEL) {
            return Z85Codec.decode(metadata.substring(1));
        }
        return decodeBase64(metadata);
    }

    static ByteBuffer decompressZstd(final ByteBuffer input) throws IOException {
        try (final var zstream = new ZstdInputStream(new ByteBufferInputStream(input))) {
            final byte[] bytes = readFully(zstream);
            return ByteBuffer.wrap(bytes);
        }
    }

    static byte[] decompressGzip(final ByteBuffer input) throws IOException {
        try (final var gstream = new GZIPInputStream(new ByteBufferInputStream(input))) {
            return readFully(gstream);
        }
    }

    /**
     * @see OffsetEncoding#ByteArray
     */
    static String deserialiseByteArrayToBitMapString(final ByteBuffer data) {
        data.rewind();
        final StringBuilder sb = new StringBuilder(data.capacity());
        while (data.hasRemaining()) {
            final byte b = data.get();
            if (b == 1) {
                sb.append('x');
            } else {
                sb.append('o');
            }
        }
        return sb.toString();
    }
}
