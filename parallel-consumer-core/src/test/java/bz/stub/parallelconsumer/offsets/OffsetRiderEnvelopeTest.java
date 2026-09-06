package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.Rider;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.UnwrappedEnvelope;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Freezes the on-the-wire shape of the opaque rider envelope: {@code ['X'][u16 rider length][rider][inner...]}.
 * <p>
 * <b>Why the assertions are exact bytes rather than round-trips.</b> This payload is durable - it lives in
 * {@code __consumer_offsets} and is read back by whichever group member takes the partition next, possibly by a
 * different build. A round-trip test passes for any self-consistent pair of functions, including one that swaps the
 * length field's byte order or writes the rider after the inner bytes, so it would let the format drift while staying
 * green. Every layout claim here is therefore written out byte by byte.
 * <p>
 * The lengths are chosen where the arithmetic is most likely to be wrong rather than at random: 1 (the shortest
 * representable rider), 255/256 (the byte boundary in the big-endian length field), 3000 (a length no single byte can
 * express), and 65535 (the largest the field can hold, and the one whose <em>signed</em> short reading is -1).
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 */
@Slf4j
class OffsetRiderEnvelopeTest {

    static final byte[] NO_INNER = new byte[0];

    /**
     * Stands in for a real hole encoding: any first byte that is not the envelope's own is legal there.
     */
    static final byte[] INNER = {'l', 0x01, 0x02, 0x03};

    // ---------------------------------------------------------------------------------------------------------------
    // The magic byte
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void magicByteIsPrintableX() {
        assertThat(OffsetRiderEnvelope.MAGIC_BYTE).isEqualTo((byte) 'X');
        assertThat(OffsetRiderEnvelope.MAGIC_BYTE).isEqualTo((byte) 0x58);
    }

    @Test
    void theLengthFieldIsSixteenUnsignedBits() {
        assertThat(OffsetRiderEnvelope.MAX_RIDER_BYTES).isEqualTo(65535);
        assertThat(OffsetRiderEnvelope.HEADER_BYTES).isEqualTo(3);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Exact bytes out of wrap
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void aRiderOnlyPayloadIsMagicThenLengthThenRider() {
        byte[] rider = {10, 11, 12, 13, 14, 15, 16, 17};

        byte[] wrapped = OffsetRiderEnvelope.wrap(NO_INNER, Rider.present(rider));

        assertThat(wrapped).isEqualTo(new byte[]{'X', 0x00, 0x08, 10, 11, 12, 13, 14, 15, 16, 17});
    }

    @Test
    void aRiderOnlyPayloadUnwrapsToThatRiderAndNoInnerBytes() throws CorruptOffsetMetadataException {
        byte[] rider = {10, 11, 12, 13, 14, 15, 16, 17};

        UnwrappedEnvelope unwrapped = OffsetRiderEnvelope.unwrap(
                new byte[]{'X', 0x00, 0x08, 10, 11, 12, 13, 14, 15, 16, 17});

        assertThat(unwrapped.getRider().getState()).isEqualTo(RiderState.PRESENT);
        assertThat(unwrapped.getRider().getBytes()).isEqualTo(rider);
        assertThat(unwrapped.getInnerBytes()).isEqualTo(NO_INNER);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 255, 256, 3000, 65535})
    void theLengthFieldIsBigEndianAndTheRiderFollowsItRaw(int riderLength) throws IOException {
        byte[] rider = riderOfLength(riderLength);

        byte[] wrapped = OffsetRiderEnvelope.wrap(NO_INNER, Rider.present(rider));

        assertThat(wrapped).isEqualTo(concat(new byte[]{'X', highByte(riderLength), lowByte(riderLength)}, rider));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 255, 256, 3000, 65535})
    void theInnerBytesFollowTheRiderUntouched(int riderLength) throws IOException {
        byte[] rider = riderOfLength(riderLength);

        byte[] wrapped = OffsetRiderEnvelope.wrap(INNER, Rider.present(rider));

        assertThat(wrapped).isEqualTo(
                concat(new byte[]{'X', highByte(riderLength), lowByte(riderLength)}, rider, INNER));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 255, 256, 3000, 65535})
    void unwrapReadsBackExactlyWhatWrapWrote(int riderLength) throws IOException, CorruptOffsetMetadataException {
        byte[] rider = riderOfLength(riderLength);

        UnwrappedEnvelope unwrapped = OffsetRiderEnvelope.unwrap(OffsetRiderEnvelope.wrap(INNER, Rider.present(rider)));

        assertThat(unwrapped.getRider().getState()).isEqualTo(RiderState.PRESENT);
        assertThat(unwrapped.getRider().getBytes()).isEqualTo(rider);
        assertThat(unwrapped.getInnerBytes()).isEqualTo(INNER);
    }

    @Test
    void anAbsentInnerArrayIsWrittenAsNoInnerBytes() {
        byte[] wrapped = OffsetRiderEnvelope.wrap(null, Rider.present(new byte[]{7}));

        assertThat(wrapped).isEqualTo(new byte[]{'X', 0x00, 0x01, 7});
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The dropped marker
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void theDroppedMarkerIsAZeroLengthEnvelopeAroundTheInnerBytes() {
        byte[] wrapped = OffsetRiderEnvelope.wrap(INNER, Rider.dropped());

        assertThat(wrapped).isEqualTo(new byte[]{'X', 0x00, 0x00, 'l', 0x01, 0x02, 0x03});
    }

    @Test
    void aZeroLengthEnvelopeReadsAsDroppedAndKeepsTheInnerBytes() throws CorruptOffsetMetadataException {
        UnwrappedEnvelope unwrapped = OffsetRiderEnvelope.unwrap(new byte[]{'X', 0x00, 0x00, 'l', 0x01, 0x02, 0x03});

        assertThat(unwrapped.getRider().getState()).isEqualTo(RiderState.DROPPED);
        assertThat(unwrapped.getInnerBytes()).isEqualTo(INNER);
    }

    @Test
    void aDroppedMarkerWithNoInnerBytesIsThreeBytesAndReadsBackAsDropped() throws CorruptOffsetMetadataException {
        byte[] wrapped = OffsetRiderEnvelope.wrap(NO_INNER, Rider.dropped());

        assertThat(wrapped).isEqualTo(new byte[]{'X', 0x00, 0x00});

        UnwrappedEnvelope unwrapped = OffsetRiderEnvelope.unwrap(wrapped);
        assertThat(unwrapped.getRider().getState()).isEqualTo(RiderState.DROPPED);
        assertThat(unwrapped.getInnerBytes()).isEqualTo(NO_INNER);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Corrupt payloads - the length field is never trusted
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void aLengthLongerThanTheBytesRemainingIsCorrupt() {
        // claims eight rider bytes, carries three
        byte[] payload = {'X', 0x00, 0x08, 1, 2, 3};

        var e = assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(payload));

        assertThat(e).hasMessageThat().contains("claims 8 rider bytes but only 3 follow");
    }

    @Test
    void aLengthOneByteBeyondTheRemainderIsCorrupt() {
        byte[] payload = {'X', 0x00, 0x04, 1, 2, 3};

        assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(payload));
    }

    /**
     * {@code 0xFFFF} read as a signed short is -1. A signed reading would sail past every "longer than the remainder"
     * check and then either allocate nothing or throw a bare {@link NegativeArraySizeException} - so this is the case
     * that proves the field is read unsigned, and that the claimed size is never allocated.
     */
    @Test
    void aLengthWhoseSignedReadingIsNegativeIsCorruptAndAllocatesNothing() {
        byte[] payload = {'X', (byte) 0xFF, (byte) 0xFF};

        var e = assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(payload));

        assertThat(e).hasMessageThat().contains("claims 65535 rider bytes but only 0 follow");
    }

    @Test
    void aPayloadTruncatedInsideItsOwnHeaderIsCorrupt() {
        assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(new byte[]{'X'}));
        assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(new byte[]{'X', 0x00}));
        assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(new byte[0]));
    }

    @Test
    void anEnvelopeInsideAnEnvelopeIsCorrupt() {
        byte[] nested = {'X', 0x00, 0x01, 9, 'X', 0x00, 0x00};

        assertThrows(CorruptOffsetMetadataException.class, () -> OffsetRiderEnvelope.unwrap(nested));
    }

    @Test
    void aPayloadThatIsNotAnEnvelopeIsACallerError() {
        // the magic byte is resolved before unwrap is reached, so a different one is a bug here, not bad metadata
        assertThrows(IllegalArgumentException.class, () -> OffsetRiderEnvelope.unwrap(new byte[]{'l', 0x00, 0x00}));
        assertThrows(IllegalArgumentException.class, () -> OffsetRiderEnvelope.unwrap(null));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Nothing hands out, or holds on to, a shared array
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void unwrapHandsOutACopyOfTheRider() throws CorruptOffsetMetadataException {
        byte[] payload = {'X', 0x00, 0x02, 40, 41};

        byte[] first = OffsetRiderEnvelope.unwrap(payload).getRider().getBytes();
        first[0] = 99;

        assertThat(OffsetRiderEnvelope.unwrap(payload).getRider().getBytes()).isEqualTo(new byte[]{40, 41});
        assertThat(payload).isEqualTo(new byte[]{'X', 0x00, 0x02, 40, 41});
    }

    @Test
    void unwrapHandsOutACopyOfTheInnerBytes() throws CorruptOffsetMetadataException {
        byte[] payload = {'X', 0x00, 0x01, 40, 'l', 0x07};

        byte[] first = OffsetRiderEnvelope.unwrap(payload).getInnerBytes();
        first[0] = 99;

        assertThat(OffsetRiderEnvelope.unwrap(payload).getInnerBytes()).isEqualTo(new byte[]{'l', 0x07});
        assertThat(payload).isEqualTo(new byte[]{'X', 0x00, 0x01, 40, 'l', 0x07});
    }

    @Test
    void everyCallToTheRiderGetterHandsOutItsOwnCopy() {
        Rider rider = Rider.present(new byte[]{1, 2, 3});

        rider.getBytes()[0] = 99;

        assertThat(rider.getBytes()).isEqualTo(new byte[]{1, 2, 3});
    }

    /**
     * The same instance, twice - not two unwraps of the same payload. Two unwraps each allocate their own array, so
     * that pairing stays green even if the getter hands out the field itself.
     */
    @Test
    void everyCallToTheInnerBytesGetterHandsOutItsOwnCopy() throws CorruptOffsetMetadataException {
        UnwrappedEnvelope unwrapped = OffsetRiderEnvelope.unwrap(new byte[]{'X', 0x00, 0x01, 40, 'l', 0x07});

        unwrapped.getInnerBytes()[0] = 99;

        assertThat(unwrapped.getInnerBytes()).isEqualTo(new byte[]{'l', 0x07});
    }

    @Test
    void everyCallToAnUnwrappedRidersGetterHandsOutItsOwnCopy() throws CorruptOffsetMetadataException {
        UnwrappedEnvelope unwrapped = OffsetRiderEnvelope.unwrap(new byte[]{'X', 0x00, 0x02, 40, 41});

        unwrapped.getRider().getBytes()[0] = 99;

        assertThat(unwrapped.getRider().getBytes()).isEqualTo(new byte[]{40, 41});
    }

    @Test
    void mutatingTheCallersArrayAfterWrappingDoesNotChangeTheWrappedBytes() {
        byte[] callersRider = {1, 2, 3};
        Rider rider = Rider.present(callersRider);

        byte[] wrapped = OffsetRiderEnvelope.wrap(NO_INNER, rider);
        callersRider[0] = 99;

        assertThat(wrapped).isEqualTo(new byte[]{'X', 0x00, 0x03, 1, 2, 3});
        assertThat(rider.getBytes()).isEqualTo(new byte[]{1, 2, 3});
    }

    @Test
    void mutatingTheCallersInnerArrayAfterWrappingDoesNotChangeTheWrappedBytes() {
        byte[] callersInner = {'l', 5};

        byte[] wrapped = OffsetRiderEnvelope.wrap(callersInner, Rider.present(new byte[]{1}));
        callersInner[1] = 99;

        assertThat(wrapped).isEqualTo(new byte[]{'X', 0x00, 0x01, 1, 'l', 5});
    }

    // ---------------------------------------------------------------------------------------------------------------
    // States that must never reach the wire
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void thereIsNoEnvelopeForARiderThatWasNeverConfigured() {
        assertThrows(IllegalArgumentException.class, () -> OffsetRiderEnvelope.wrap(INNER, Rider.none()));
    }

    @Test
    void thereIsNoEnvelopeForMetadataThePolicyDiscarded() {
        assertThrows(IllegalArgumentException.class, () -> OffsetRiderEnvelope.wrap(INNER, Rider.unreadable()));
    }

    @Test
    void aZeroLengthRiderIsNotARepresentableValue() {
        assertThrows(IllegalArgumentException.class, () -> Rider.present(new byte[0]));
    }

    @Test
    void aPresentRiderNeedsBytes() {
        assertThrows(IllegalArgumentException.class, () -> Rider.present(null));
    }

    @Test
    void wrappingNeedsARider() {
        assertThrows(IllegalArgumentException.class, () -> OffsetRiderEnvelope.wrap(INNER, null));
    }

    @Test
    void aRiderTooLongForTheLengthFieldIsRejected() {
        Rider tooLong = Rider.present(new byte[OffsetRiderEnvelope.MAX_RIDER_BYTES + 1]);

        assertThrows(IllegalArgumentException.class, () -> OffsetRiderEnvelope.wrap(NO_INNER, tooLong));
    }

    @Test
    void theEnvelopeNeverNestsOnTheWriteSideEither() {
        assertThrows(IllegalArgumentException.class,
                () -> OffsetRiderEnvelope.wrap(new byte[]{'X', 0x00, 0x00}, Rider.present(new byte[]{1})));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The rider value itself
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void onlyAPresentRiderHasBytesToHandOut() {
        assertThrows(IllegalStateException.class, () -> Rider.none().getBytes());
        assertThrows(IllegalStateException.class, () -> Rider.dropped().getBytes());
        assertThrows(IllegalStateException.class, () -> Rider.unreadable().getBytes());
    }

    @Test
    void theStatesAreDistinguishable() {
        assertThat(Rider.none().getState()).isEqualTo(RiderState.NONE);
        assertThat(Rider.dropped().getState()).isEqualTo(RiderState.DROPPED);
        assertThat(Rider.unreadable().getState()).isEqualTo(RiderState.UNREADABLE);
        assertThat(Rider.present(new byte[]{1}).getState()).isEqualTo(RiderState.PRESENT);

        assertThat(Rider.present(new byte[]{1}).isPresent()).isTrue();
        assertThat(Rider.dropped().isPresent()).isFalse();
        assertThat(Rider.none().isPresent()).isFalse();
        assertThat(Rider.unreadable().isPresent()).isFalse();
    }

    @Test
    void ridersCompareByTheirContentNotTheirIdentity() {
        assertThat(Rider.present(new byte[]{1, 2})).isEqualTo(Rider.present(new byte[]{1, 2}));
        assertThat(Rider.present(new byte[]{1, 2}).hashCode()).isEqualTo(Rider.present(new byte[]{1, 2}).hashCode());
        assertThat(Rider.present(new byte[]{1, 2})).isNotEqualTo(Rider.present(new byte[]{1, 3}));
        assertThat(Rider.none()).isEqualTo(Rider.none());
        assertThat(Rider.none()).isNotEqualTo(Rider.dropped());
        assertThat(Rider.none()).isNotEqualTo(null);
        assertThat(Rider.none()).isNotEqualTo(RiderState.NONE);
    }

    @Test
    void aRiderDescribesItselfWithoutSpillingTheEmbeddersBytes() {
        assertThat(Rider.present(new byte[]{1, 2}).toString()).contains("PRESENT");
        assertThat(Rider.present(new byte[]{1, 2}).toString()).contains("2");
        assertThat(Rider.dropped().toString()).contains("DROPPED");
    }

    // ---------------------------------------------------------------------------------------------------------------

    static byte[] riderOfLength(int length) {
        byte[] rider = new byte[length];
        for (int i = 0; i < length; i++) {
            rider[i] = (byte) (i % 251); // a prime stride, so a byte-order slip shows up as a mismatch
        }
        return rider;
    }

    static byte highByte(int length) {
        return (byte) ((length >>> 8) & 0xFF);
    }

    static byte lowByte(int length) {
        return (byte) (length & 0xFF);
    }

    static byte[] concat(byte[]... parts) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part);
        }
        return out.toByteArray();
    }
}
