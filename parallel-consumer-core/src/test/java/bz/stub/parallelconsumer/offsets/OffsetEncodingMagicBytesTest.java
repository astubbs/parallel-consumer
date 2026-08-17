package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The magic byte registry of {@link OffsetEncoding} - the one byte that says how a committed offset map is to be read.
 *
 * <h2>Why this test exists (KTD7)</h2>
 * {@link OffsetEncoding} builds its {@code magicMap} with {@code Collectors.toMap}, which throws on a duplicate key
 * during static initialisation. A duplicate magic byte therefore surfaced only as an
 * {@link ExceptionInInitializerError} out of whichever unrelated test touched the enum first - an error that names
 * neither the constant nor the collision. Asserting uniqueness directly makes the failure say what it is, and it fails
 * on the class that introduced the duplicate.
 *
 * @author Antony Stubbs
 */
class OffsetEncodingMagicBytesTest {

    /**
     * Every {@link OffsetEncoding} must claim a magic byte no other one claims. Table-driven over {@code values()}, so
     * a new constant is enrolled automatically.
     */
    @Test
    void everyMagicByteIsClaimedByExactlyOneEncoding() {
        final Map<Byte, OffsetEncoding> claimedBy = new LinkedHashMap<>();

        for (final OffsetEncoding encoding : OffsetEncoding.values()) {
            final OffsetEncoding previousClaimant = claimedBy.put(encoding.getMagicByte(), encoding);
            assertWithMessage("magic byte %s ('%s') is claimed by both %s and %s - the second would be unreadable, and "
                            + "OffsetEncoding's own magicMap would fail as an ExceptionInInitializerError instead of "
                            + "saying so",
                    encoding.getMagicByte(), (char) encoding.getMagicByte(), previousClaimant, encoding)
                    .that(previousClaimant)
                    .isNull();
        }

        assertWithMessage("one distinct magic byte per encoding")
                .that(claimedBy).hasSize(OffsetEncoding.values().length);
    }

    /** And each one decodes back to the encoding that claimed it. */
    @Test
    void everyMagicByteDecodesBackToItsOwnEncoding() throws OffsetDecodingError {
        for (final OffsetEncoding encoding : OffsetEncoding.values()) {
            assertWithMessage("magic byte of %s", encoding)
                    .that(OffsetEncoding.decode(encoding.getMagicByte()))
                    .isEqualTo(encoding);
        }
    }

    /**
     * KTD7 reserved one magic-byte pair per benchmark candidate. Only the delta-list pair ({@code 'd'} / {@code 'D'})
     * ships; the chunked-bitset ({@code 'r'} / {@code 'z'}) and unsigned-run-length ({@code 'u'} / {@code 'U'}) pairs
     * stay reserved and unregistered, and this pins that they are still free for whoever picks them up.
     */
    @Test
    void theShippedCandidatePairIsRegisteredAndTheOtherReservationsAreStillFree() throws OffsetDecodingError {
        assertThat(OffsetEncoding.decode((byte) 'd')).isEqualTo(OffsetEncoding.DeltaList);
        assertThat(OffsetEncoding.decode((byte) 'D')).isEqualTo(OffsetEncoding.DeltaListCompressed);

        for (final char reserved : new char[]{'r', 'z', 'u', 'U'}) {
            assertThatThrownBy(() -> OffsetEncoding.decode((byte) reserved))
                    .as("reserved but unregistered candidate magic byte '%s'", reserved)
                    .isInstanceOf(OffsetDecodingError.class);
        }
    }

    /**
     * Kafka Streams' own magic numbers must stay claimed by the Kafka Streams entries: that is how PC recognises a
     * consumer group Streams was using and reports it, rather than decoding the bytes as an offset map.
     */
    @Test
    void theKafkaStreamsMagicNumbersStayClaimed() throws OffsetDecodingError {
        assertThat(OffsetEncoding.decode((byte) 1)).isEqualTo(OffsetEncoding.KafkaStreams);
        assertThat(OffsetEncoding.decode((byte) 2)).isEqualTo(OffsetEncoding.KafkaStreamsV2);
    }
}
