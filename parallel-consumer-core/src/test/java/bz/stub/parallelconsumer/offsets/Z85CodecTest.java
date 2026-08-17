package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the {@link Z85Codec} wire format.
 * <p>
 * The Z85 alphabet and grouping are an external, frozen spec (ZeroMQ RFC 32), and the codec's output ends up
 * inside committed offset metadata - so these assertions are on <em>exact</em> characters and bytes, never
 * only on round-tripping. A round-trip test alone passes for any self-consistent pair of broken
 * implementations, and would leave the arithmetic (digit order, group size, the partial-block char count)
 * free to mutate. This package is the mutation-testing lane, so every constant below is an independent
 * expectation computed from the spec rather than from the implementation.
 *
 * @author Antony Stubbs
 */
class Z85CodecTest {

    /**
     * The ZeroMQ RFC 32 reference vector: these eight bytes are specified to encode to {@code HelloWorld}.
     */
    private static final byte[] REFERENCE_VECTOR = {
            (byte) 0x86, (byte) 0x4F, (byte) 0xD2, (byte) 0x6F,
            (byte) 0xB5, (byte) 0x59, (byte) 0xF7, (byte) 0x5B};

    /**
     * Expected encodings of the first n bytes of {@link #REFERENCE_VECTOR}, indexed by n. Covers every
     * partial-block size (1-3 trailing bytes) as well as the exact 4-byte multiples.
     */
    private static final String[] REFERENCE_PREFIX_ENCODINGS = {
            "",                 // 0 bytes
            "H5",               // 1
            "Hed",              // 2
            "Helj",             // 3
            "Hello",            // 4
            "HelloWe",          // 5
            "HelloWoi",         // 6
            "HelloWork",        // 7
            "HelloWorld"};      // 8

    @Test
    void referenceVectorEncodesToHelloWorld() {
        assertThat(Z85Codec.encode(REFERENCE_VECTOR)).isEqualTo("HelloWorld");
    }

    @Test
    void referenceVectorDecodesFromHelloWorld() throws Z85DecodingException {
        assertThat(Z85Codec.decode("HelloWorld")).containsExactly(REFERENCE_VECTOR);
    }

    @Test
    void everyPartialBlockSizeEncodesToTheExpectedCharacters() throws Z85DecodingException {
        for (int n = 0; n < REFERENCE_PREFIX_ENCODINGS.length; n++) {
            var input = new byte[n];
            System.arraycopy(REFERENCE_VECTOR, 0, input, 0, n);

            var encoded = Z85Codec.encode(input);
            assertThat(encoded)
                    .describedAs("encoding of the first %s bytes of the reference vector", n)
                    .isEqualTo(REFERENCE_PREFIX_ENCODINGS[n]);
            assertThat(Z85Codec.decode(encoded))
                    .describedAs("round trip of the first %s bytes of the reference vector", n)
                    .containsExactly(input);
        }
    }

    @Test
    void emptyInputEncodesToTheEmptyString() throws Z85DecodingException {
        assertThat(Z85Codec.encode(new byte[0])).isEmpty();
        assertThat(Z85Codec.decode("")).isEmpty();
    }

    @Test
    void allZeroBlockEncodesToTheLowestAlphabetCharacters() throws Z85DecodingException {
        assertThat(Z85Codec.encode(new byte[]{0, 0, 0, 0})).isEqualTo("00000");
        assertThat(Z85Codec.decode("00000")).containsExactly(0, 0, 0, 0);
    }

    /**
     * The all-ones block is the largest 32-bit value the format can carry, so it is the group that exercises
     * the top of the base-85 range. The expectation is derived here by plain radix conversion against
     * {@link Z85Codec#ALPHABET} rather than being copied out of the implementation.
     */
    @Test
    void allOnesBlockEncodesToTheMaximumFiveCharacterGroup() throws Z85DecodingException {
        var expected = radixConvertToZ85(0xFFFFFFFFL);

        // sanity check the independent conversion itself against the known value
        assertThat(expected).isEqualTo("%nSc0");

        assertThat(Z85Codec.encode(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}))
                .isEqualTo(expected);
        assertThat(Z85Codec.decode(expected))
                .containsExactly((byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF);
    }

    /**
     * KTD6's crossover math is built on this formula, so it is asserted directly rather than inferred.
     */
    @Test
    void encodedLengthMatchesTheDensityFormula() {
        for (int n = 0; n <= 64; n++) {
            int remainder = n % 4;
            int expected = 5 * (n / 4) + (remainder == 0 ? 0 : remainder + 1);
            assertThat(Z85Codec.encode(new byte[n]).length())
                    .describedAs("encoded length for %s input bytes", n)
                    .isEqualTo(expected);
            assertThat(Z85Codec.encodedLength(n))
                    .describedAs("predicted encoded length for %s input bytes", n)
                    .isEqualTo(expected);
        }
    }

    @Test
    void fourByteMultiplesEncodeToExactlyFiveCharactersPerBlock() throws Z85DecodingException {
        var random = new Random(19_2192L);
        for (int blocks = 1; blocks <= 16; blocks++) {
            var input = new byte[blocks * 4];
            random.nextBytes(input);

            var encoded = Z85Codec.encode(input);
            assertThat(encoded.length())
                    .describedAs("encoded length for %s whole blocks", blocks)
                    .isEqualTo(blocks * 5);
            assertThat(Z85Codec.decode(encoded)).containsExactly(input);
        }
    }

    @Test
    void randomPayloadsRoundTrip() throws Z85DecodingException {
        var random = new Random(192L);
        for (int length : new int[]{1, 2, 3, 5, 17, 63, 64, 65, 255, 1023, 4096, 8191, 8192}) {
            var input = new byte[length];
            random.nextBytes(input);

            assertThat(Z85Codec.decode(Z85Codec.encode(input)))
                    .describedAs("round trip of %s random bytes", length)
                    .containsExactly(input);
        }
    }

    /**
     * R8: the cap check in {@code PartitionState} compares {@link String#length()} while the broker caps
     * bytes, so the output alphabet must be 7-bit ASCII for the two to be the same number.
     */
    @Test
    void outputIsSevenBitAsciiDrawnOnlyFromTheZ85Alphabet() {
        var random = new Random(85L);
        for (int length = 0; length <= 128; length++) {
            var input = new byte[length];
            random.nextBytes(input);

            var encoded = Z85Codec.encode(input);
            for (int i = 0; i < encoded.length(); i++) {
                assertThat(Z85Codec.ALPHABET.indexOf(encoded.charAt(i)))
                        .describedAs("char '%s' at %s of the encoding of %s bytes is in the Z85 alphabet",
                                encoded.charAt(i), i, length)
                        .isNotNegative();
            }
            assertThat(encoded.length())
                    .describedAs("char length equals UTF-8 byte length for %s input bytes", length)
                    .isEqualTo(encoded.getBytes(UTF_8).length);
        }
    }

    @Test
    void alphabetIsTheZeroMqRfc32Alphabet() {
        assertThat(Z85Codec.ALPHABET)
                .isEqualTo("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#")
                .hasSize(85);
    }

    /**
     * Characters outside the alphabet. {@code ','} and {@code '"'} are specifically absent from Z85 (that is
     * why KTD2 chose it over Ascii85), and {@code '~'} is a printable ASCII char it does not use.
     */
    @ParameterizedTest
    @ValueSource(strings = {"Hel,o", "Hel\"o", "Hel~o", "HelloWorl,", ",", "He,", "  ", "éé"})
    void decodeRejectsCharactersOutsideTheAlphabet(String invalid) {
        assertThatThrownBy(() -> Z85Codec.decode(invalid))
                .isInstanceOf(Z85DecodingException.class);
    }

    /**
     * A single leftover character can never be produced by the encoder: a partial group of n raw bytes emits
     * n+1 chars, so leftovers are always 2, 3 or 4.
     */
    @ParameterizedTest
    @ValueSource(strings = {"0", "H", "HelloW", "HelloWorldH"})
    void decodeRejectsImpossibleLengths(String impossibleLength) {
        assertThat(impossibleLength.length() % 5).isOne();

        assertThatThrownBy(() -> Z85Codec.decode(impossibleLength))
                .isInstanceOf(Z85DecodingException.class);
    }

    /**
     * The top of the base-85 range overshoots 32 bits: {@code "#####"} is five copies of the highest
     * alphabet character and denotes 85^5-1, well beyond the 4 bytes a group can hold.
     */
    @ParameterizedTest
    @ValueSource(strings = {"#####", "%nSc1", "$0000", "Hello#####"})
    void decodeRejectsFullGroupsBeyondThirtyTwoBits(String overflowing) {
        assertThatThrownBy(() -> Z85Codec.decode(overflowing))
                .isInstanceOf(Z85DecodingException.class);
    }

    /**
     * Non-canonical tails: legal length, legal alphabet, but no byte sequence encodes to them - so silently
     * truncating (which is what a naive decoder does) would accept a corrupt string as valid. Each entry
     * below is a tail whose recovered bytes re-encode to something else, or whose maximum-padded value
     * overflows 32 bits.
     * <p>
     * Worked example for {@code "01"}: a 2-char tail carries 1 byte, and the only 1-byte encodings are the
     * 85 two-char strings {@code "00".."@@"} that {@link #radixConvertToZ85(long)} produces for
     * {@code b << 24}. {@code "01"} is not one of them - the byte it recovers is {@code 0x00}, which encodes
     * to {@code "00"}.
     */
    @ParameterizedTest
    @ValueSource(strings = {"01", "0z", "%n", "001", "00z", "0001", "zzzz", "####", "Hello01", "HelloWorld0001"})
    void decodeRejectsNonCanonicalTails(String nonCanonicalTail) {
        assertThatThrownBy(() -> Z85Codec.decode(nonCanonicalTail))
                .isInstanceOf(Z85DecodingException.class);
    }

    /**
     * The counterpart of the rejection test: canonical tails of every legal size must still decode. Without
     * this, a decoder that rejected all partial groups would pass the rejection test above.
     */
    @Test
    void canonicalTailsOfEveryLegalSizeDecode() throws Z85DecodingException {
        assertThat(Z85Codec.decode("H5")).containsExactly((byte) 0x86);
        assertThat(Z85Codec.decode("@@")).containsExactly((byte) 0xFF);
        assertThat(Z85Codec.decode("00")).containsExactly((byte) 0x00);
        assertThat(Z85Codec.decode("Hed")).containsExactly((byte) 0x86, (byte) 0x4F);
        assertThat(Z85Codec.decode("%nJ")).containsExactly((byte) 0xFF, (byte) 0xFF);
        assertThat(Z85Codec.decode("Helj")).containsExactly((byte) 0x86, (byte) 0x4F, (byte) 0xD2);
        assertThat(Z85Codec.decode("%nS9")).containsExactly((byte) 0xFF, (byte) 0xFF, (byte) 0xFF);
        // a tail whose canonical form legitimately ends in the maximum alphabet character - the padding char
        assertThat(Z85Codec.decode("000#")).containsExactly((byte) 0x00, (byte) 0x00, (byte) 0x1C);
    }

    /**
     * Every 1-byte value must have exactly the 2-char encoding that radix conversion predicts, and must
     * survive the tail round trip. This closes the partial-block arithmetic completely for n=1.
     */
    @Test
    void everySingleByteValueRoundTripsThroughItsTwoCharacterTail() throws Z85DecodingException {
        for (int value = 0; value < 256; value++) {
            var input = new byte[]{(byte) value};
            var expected = radixConvertToZ85((long) value << 24).substring(0, 2);

            var encoded = Z85Codec.encode(input);
            assertThat(encoded).describedAs("encoding of byte 0x%02X", value).isEqualTo(expected);
            assertThat(Z85Codec.decode(encoded)).containsExactly(input);
        }
    }

    @Test
    void nullInputIsRejected() {
        assertThatThrownBy(() -> Z85Codec.encode(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Z85Codec.decode(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void decodingExceptionIsCheckedSoCallersMustRouteIt() {
        assertThat(Exception.class).isAssignableFrom(Z85DecodingException.class);
        assertThat(RuntimeException.class.isAssignableFrom(Z85DecodingException.class))
                .describedAs("Z85DecodingException must be checked so callers convert it to OffsetDecodingError")
                .isFalse();
    }

    /**
     * Independent expectation generator: plain unsigned radix conversion of a 32-bit value into five base-85
     * digits, most significant first. Deliberately written without reference to the codec's internals.
     */
    private static String radixConvertToZ85(long unsigned32) {
        var digits = new char[5];
        long remaining = unsigned32;
        for (int i = 4; i >= 0; i--) {
            digits[i] = Z85Codec.ALPHABET.charAt((int) (remaining % 85));
            remaining /= 85;
        }
        assertThat(remaining).isZero();
        return new String(digits);
    }
}
