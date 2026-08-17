package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * Z85 (ZeroMQ RFC 32) binary-to-text codec - a denser alternative to Base64 for offset metadata.
 * <p>
 * Z85 packs 4 bytes into 5 characters (25% expansion) where Base64 packs 3 into 4 (33%), so an offset-map
 * payload encodes roughly 6% shorter. That matters because the encoded string is what the metadata cap
 * (see {@code OffsetMapCodecManager.DefaultMaxMetadataSize}) and the back-pressure threshold are measured
 * against: density here is direct headroom before PC starts blocking records.
 *
 * <h2>Alphabet and why Z85 rather than Ascii85 or RFC 1924</h2>
 * The 85 characters exclude quote, double-quote, backslash, backtick, comma and semicolon, so an encoded
 * string is safe to drop into a JSON dump or a log line unescaped - the property the other base-85 variants
 * lack. It <em>does</em> contain shell metacharacters ({@code $ & * ? ! < > ( ) [ ] { } #}) that Base64 does
 * not, so metadata strings must be quoted when pasted into a shell.
 *
 * <h2>Partial blocks</h2>
 * There is no padding convention and no out-of-band length: a final group of n raw bytes (1-3) is zero-padded
 * to 4 bytes, encoded, and only the first n+1 characters are emitted. Those n+1 characters are always enough
 * to recover the n bytes, because 85<sup>4-n</sup> (the range of values a truncated group leaves open) is
 * smaller than 2<sup>8(4-n)</sup> (the place value of the lowest byte kept) for every n. Encoded length is
 * therefore {@code 5*floor(len/4) + (len%4 == 0 ? 0 : len%4 + 1)}.
 *
 * <h2>Decoding is strict</h2>
 * Recovery works by padding the truncated group with the <em>highest</em> alphabet character, which yields
 * the largest 32-bit value consistent with the characters present and, by the inequality above, the same top
 * n bytes the encoder started from. That reconstruction succeeds for <em>any</em> characters, though - most
 * such strings are not encodings of anything. So the recovered tail is re-encoded and required to reproduce
 * the input characters exactly. Without that check a corrupt string would decode to a plausible-looking
 * shorter byte array, and a truncated offset map is worse than a rejected one: it silently marks work
 * complete. All rejections raise the checked {@link Z85DecodingException}, which callers convert to
 * {@link OffsetDecodingError}.
 * <p>
 * Output is 7-bit ASCII, so {@link String#length()} equals the UTF-8 byte length - the metadata cap check
 * compares characters while the broker caps bytes, and this codec keeps those two numbers the same.
 *
 * @author Antony Stubbs
 * @see <a href="https://rfc.zeromq.org/spec/32/">ZeroMQ RFC 32 - Z85</a>
 */
@UtilityClass
public class Z85Codec {

    /**
     * The RFC 32 alphabet: the character at index i represents the base-85 digit i.
     */
    static final String ALPHABET =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

    private static final int RADIX = 85;

    private static final int BYTES_PER_BLOCK = 4;

    private static final int CHARS_PER_BLOCK = 5;

    /**
     * The largest value a 4-byte block can hold, as an unsigned 32-bit number.
     */
    private static final long MAX_BLOCK_VALUE = 0xFFFFFFFFL;

    /**
     * The highest alphabet character - used to pad a truncated final group up to a full 5 characters. See the
     * class javadoc for why the maximum, and not the minimum, is the correct filler.
     */
    private static final char HIGHEST_DIGIT_CHAR = ALPHABET.charAt(RADIX - 1);

    /**
     * Reverse lookup: ASCII code point to base-85 digit, or -1 for a character outside the alphabet.
     */
    private static final byte[] DIGIT_BY_CHAR = buildDigitLookup();

    private static byte[] buildDigitLookup() {
        final byte[] lookup = new byte[128];
        for (int i = 0; i < lookup.length; i++) {
            lookup[i] = -1;
        }
        for (int digit = 0; digit < RADIX; digit++) {
            lookup[ALPHABET.charAt(digit)] = (byte) digit;
        }
        return lookup;
    }

    /**
     * Encodes bytes to a Z85 string.
     *
     * @param raw the bytes to encode; empty in, empty out
     * @return the encoded string, always 7-bit ASCII and always {@link #encodedLength(int)} characters long
     */
    public static String encode(@NonNull final byte[] raw) {
        final int fullBlocks = raw.length / BYTES_PER_BLOCK;
        final int tailBytes = raw.length % BYTES_PER_BLOCK;

        final StringBuilder encoded = new StringBuilder(encodedLength(raw.length));
        int readAt = 0;
        for (int block = 0; block < fullBlocks; block++) {
            appendBlock(encoded, readBlockBytes(raw, readAt, BYTES_PER_BLOCK), CHARS_PER_BLOCK);
            readAt += BYTES_PER_BLOCK;
        }
        if (tailBytes > 0) {
            appendBlock(encoded, readBlockBytes(raw, readAt, tailBytes), tailBytes + 1);
        }
        return encoded.toString();
    }

    /**
     * The number of characters {@link #encode(byte[])} produces for a given number of input bytes. Exposed
     * because callers choose between codecs on length and should not have to encode to find out.
     *
     * @param byteCount the number of bytes that would be encoded
     * @return the resulting character count (which is also the UTF-8 byte count)
     */
    public static int encodedLength(final int byteCount) {
        final int tailBytes = byteCount % BYTES_PER_BLOCK;
        return CHARS_PER_BLOCK * (byteCount / BYTES_PER_BLOCK) + (tailBytes == 0 ? 0 : tailBytes + 1);
    }

    /**
     * Decodes a Z85 string produced by {@link #encode(byte[])}.
     *
     * @param encoded the string to decode; empty in, empty out
     * @return the original bytes
     * @throws Z85DecodingException if the string contains a character outside the alphabet, has a length no
     *                              encoding can produce ({@code length % 5 == 1}), contains a full group
     *                              denoting more than 32 bits, or ends in a final group that is not the
     *                              canonical encoding of the bytes it would yield
     */
    public static byte[] decode(@NonNull final String encoded) throws Z85DecodingException {
        final int tailChars = encoded.length() % CHARS_PER_BLOCK;
        if (tailChars == 1) {
            throw new Z85DecodingException("Not a Z85 string: length " + encoded.length()
                    + " leaves a single trailing character, but a partial group is always 2, 3 or 4 characters "
                    + "(n input bytes encode to n+1 characters)");
        }
        final int fullBlocks = encoded.length() / CHARS_PER_BLOCK;
        final int tailBytes = tailChars == 0 ? 0 : tailChars - 1;

        final byte[] decoded = new byte[fullBlocks * BYTES_PER_BLOCK + tailBytes];
        int writeAt = 0;
        for (int block = 0; block < fullBlocks; block++) {
            final long blockValue = readBlockValue(encoded, block * CHARS_PER_BLOCK, CHARS_PER_BLOCK);
            writeBlockBytes(decoded, writeAt, blockValue, BYTES_PER_BLOCK);
            writeAt += BYTES_PER_BLOCK;
        }
        if (tailBytes > 0) {
            final int tailAt = fullBlocks * CHARS_PER_BLOCK;
            final long tailValue = readBlockValue(encoded, tailAt, tailChars);
            writeBlockBytes(decoded, writeAt, tailValue, tailBytes);
            requireCanonicalTail(encoded, tailAt, decoded, writeAt, tailBytes);
        }
        return decoded;
    }

    /**
     * Reads {@code byteCount} bytes as the most significant bytes of a 4-byte big-endian block, with the
     * remaining low-order bytes zero (the encoder's zero-padding of a partial group).
     */
    private static long readBlockBytes(final byte[] raw, final int readAt, final int byteCount) {
        long blockValue = 0;
        for (int i = 0; i < BYTES_PER_BLOCK; i++) {
            final int nextByte = i < byteCount ? raw[readAt + i] & 0xFF : 0;
            blockValue = (blockValue << 8) | nextByte;
        }
        return blockValue;
    }

    /**
     * Writes the top {@code byteCount} bytes of a 4-byte big-endian block.
     */
    private static void writeBlockBytes(final byte[] target, final int writeAt, final long blockValue, final int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            target[writeAt + i] = (byte) (blockValue >>> (8 * (BYTES_PER_BLOCK - 1 - i)));
        }
    }

    /**
     * Appends the {@code charCount} most significant of the block's 5 base-85 digits.
     */
    private static void appendBlock(final StringBuilder target, final long blockValue, final int charCount) {
        final char[] digits = new char[CHARS_PER_BLOCK];
        long remaining = blockValue;
        for (int i = CHARS_PER_BLOCK - 1; i >= 0; i--) {
            digits[i] = ALPHABET.charAt((int) (remaining % RADIX));
            remaining /= RADIX;
        }
        target.append(digits, 0, charCount);
    }

    /**
     * Reads {@code charCount} characters as the most significant base-85 digits of a group, padding the rest
     * with the highest digit.
     */
    private static long readBlockValue(final String encoded, final int readAt, final int charCount) throws Z85DecodingException {
        long blockValue = 0;
        for (int i = 0; i < CHARS_PER_BLOCK; i++) {
            final char digitChar = i < charCount ? encoded.charAt(readAt + i) : HIGHEST_DIGIT_CHAR;
            blockValue = blockValue * RADIX + digitOf(digitChar, readAt + i);
        }
        if (blockValue > MAX_BLOCK_VALUE) {
            throw new Z85DecodingException("Not a Z85 string: the group at character " + readAt
                    + " denotes " + blockValue + ", which exceeds the " + MAX_BLOCK_VALUE
                    + " a 4 byte block can hold");
        }
        return blockValue;
    }

    private static int digitOf(final char digitChar, final int position) throws Z85DecodingException {
        if (digitChar < DIGIT_BY_CHAR.length) {
            final byte digit = DIGIT_BY_CHAR[digitChar];
            if (digit >= 0) {
                return digit;
            }
        }
        throw new Z85DecodingException("Not a Z85 string: character '" + digitChar + "' (0x"
                + Integer.toHexString(digitChar) + ") at position " + position + " is not in the Z85 alphabet");
    }

    /**
     * Rejects a final group that is not the encoding of the bytes it just produced. See the class javadoc:
     * the maximum-padded reconstruction always yields <em>some</em> bytes, so canonicality is what separates
     * a real encoding from a corrupt string, and re-encoding is the check.
     */
    private static void requireCanonicalTail(final String encoded,
                                             final int tailAt,
                                             final byte[] decoded,
                                             final int decodedTailAt,
                                             final int tailBytes) throws Z85DecodingException {
        final StringBuilder canonical = new StringBuilder(tailBytes + 1);
        appendBlock(canonical, readBlockBytes(decoded, decodedTailAt, tailBytes), tailBytes + 1);

        if (!encoded.startsWith(canonical.toString(), tailAt)) {
            throw new Z85DecodingException("Not a Z85 string: the final group '" + encoded.substring(tailAt)
                    + "' is not the encoding of any " + tailBytes + " byte sequence (the bytes it denotes "
                    + "encode to '" + canonical + "'), so decoding it would silently truncate corrupt data");
        }
    }
}
