package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A payload whose magic byte resolves to an encoding this build can decode, but whose body is not something any encoder
 * here could have produced.
 * <p>
 * This is the case {@link UnknownOffsetMetadataMagicException} and {@link UnsupportedOffsetEncodingException} do not
 * reach: the encoding is fine, the bytes are not. The offset metadata field is free-form, so a previous owner of the
 * consumer group can leave bytes there that happen to start with one of our magic numbers, and a truncated or
 * re-encoded commit can do it by accident.
 * <p>
 * <b>Every case here used to produce an ANSWER rather than an error</b>, which is why they are pinned individually
 * rather than as one "corrupt input" test - a wrong offset map is indistinguishable downstream from a real one:
 * <ul>
 *     <li>a bitset declaring 32767 bits with an empty body fabricated 32767 incomplete offsets, all of which would be
 *     re-delivered;</li>
 *     <li>a bitset declaring {@link Integer#MAX_VALUE} bits exhausted the heap - {@code OutOfMemoryError} from
 *     metadata a stranger can write;</li>
 *     <li>a run-length body one byte too long silently dropped the trailing byte and decoded the rest;</li>
 *     <li>a negative bitset length or run length walked the highest-seen offset <em>below</em> the committed one.</li>
 * </ul>
 *
 * @author Antony Stubbs
 * @see EncodedOffsetPair#decodeToIncompletes
 */
@Slf4j
class CorruptOffsetMetadataTest {

    static final long BASE_OFFSET = 100L;

    /**
     * The committed offset is the NEXT offset to be polled, so treating the metadata as absent means the highest offset
     * we can claim to have seen is the one below it.
     */
    static final long METADATA_TREATED_AS_ABSENT = BASE_OFFSET - 1;

    private static byte[] payload(OffsetEncoding encoding, int... body) {
        ByteBuffer b = ByteBuffer.allocate(1 + body.length);
        b.put(encoding.magicByte);
        for (int i : body) {
            b.put((byte) i);
        }
        return b.array();
    }

    static Stream<Arguments> corruptPayloads() {
        return Stream.of(
                Arguments.of("BitSet, no length field at all",
                        payload(OffsetEncoding.BitSet)),
                Arguments.of("BitSet, length field cut in half",
                        payload(OffsetEncoding.BitSet, 1)),
                Arguments.of("BitSet, 32767 bits declared with an empty body",
                        payload(OffsetEncoding.BitSet, 0x7F, 0xFF)),
                Arguments.of("BitSetV2, length field truncated",
                        payload(OffsetEncoding.BitSetV2, 1, 2)),
                Arguments.of("BitSetV2, negative length",
                        payload(OffsetEncoding.BitSetV2, 0xFF, 0xFF, 0xFF, 0xFF)),
                Arguments.of("BitSetV2, Integer.MAX_VALUE bits declared - used to exhaust the heap",
                        payload(OffsetEncoding.BitSetV2, 0x7F, 0xFF, 0xFF, 0xFF)),
                Arguments.of("BitSetCompressed, body is not a zstd frame",
                        payload(OffsetEncoding.BitSetCompressed, 9, 9, 9, 9)),
                Arguments.of("RunLengthCompressed, body is not a zstd frame",
                        payload(OffsetEncoding.RunLengthCompressed, 9, 9, 9)),
                Arguments.of("RunLengthV2, negative run length",
                        payload(OffsetEncoding.RunLengthV2, 0xFF, 0xFF, 0xFF, 0xFF)),
                Arguments.of("RunLength, body is not a whole number of 2-byte entries",
                        payload(OffsetEncoding.RunLength, 0, 5, 7)),
                // Passes the divisibility check (0 % 2 == 0), so it needs its own guard. Left unchecked at committed
                // offset 0 it decoded to highestSeenOffset == 0, i.e. "record 0 already succeeded".
                Arguments.of("RunLength, magic byte and no entries at all",
                        payload(OffsetEncoding.RunLength)),
                Arguments.of("RunLengthV2, magic byte and no entries at all",
                        payload(OffsetEncoding.RunLengthV2))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corruptPayloads")
    void ignorePolicyDiscardsACorruptPayloadAndResumesFromTheCommittedOffset(String name, byte[] input) {
        var result = EncodedOffsetPair.decodeToIncompletes(input, BASE_OFFSET,
                InvalidOffsetMetadataHandlingPolicy.IGNORE, null);

        assertThat(result.getHighestSeenOffset())
                .as("corrupt metadata must be treated as absent, not decoded into an offset map")
                .hasValue(METADATA_TREATED_AS_ABSENT);
        assertThat(result.getIncompleteOffsets())
                .as("nothing may be fabricated from a payload we could not read")
                .isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corruptPayloads")
    void failPolicyRejectsACorruptPayloadRatherThanDecodingIt(String name, byte[] input) {
        assertThatThrownBy(() -> EncodedOffsetPair.decodeToIncompletes(input, BASE_OFFSET,
                InvalidOffsetMetadataHandlingPolicy.FAIL, null))
                .as("FAIL must not silently accept a payload this build could not have written")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }

    /**
     * Metadata that is not even base64 never reaches a decoder, so it enters through
     * {@link OffsetMapCodecManager#deserialiseIncompleteOffsetMapFromBase64(long, String,
     * InvalidOffsetMetadataHandlingPolicy)} rather than {@link EncodedOffsetPair#decodeToIncompletes}. It used to raise
     * {@link OffsetDecodingError}, which {@code loadPartitionStateForAssignment} catches unconditionally - so a
     * deployment that chose {@code FAIL} dropped the offset map and replayed anyway.
     */
    @Test
    void ignorePolicyDiscardsMetadataThatIsNotEvenBase64() throws Exception {
        var result = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(
                BASE_OFFSET, "not-valid-base64!!", InvalidOffsetMetadataHandlingPolicy.IGNORE);

        assertThat(result.getHighestSeenOffset()).hasValue(METADATA_TREATED_AS_ABSENT);
        assertThat(result.getIncompleteOffsets()).isEmpty();
    }

    @Test
    void failPolicyStopsOnMetadataThatIsNotEvenBase64() {
        assertThatThrownBy(() -> OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(
                BASE_OFFSET, "not-valid-base64!!", InvalidOffsetMetadataHandlingPolicy.FAIL))
                .as("FAIL must stop rather than discard metadata it cannot even base64-decode")
                .isInstanceOf(CorruptOffsetMetadataException.class);
    }
}
