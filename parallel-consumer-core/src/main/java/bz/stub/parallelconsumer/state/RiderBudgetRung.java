package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope;

/**
 * The rungs of the budget ladder, in descending order of what they cost the commit's metadata field, and the pure
 * arithmetic that picks one.
 * <p>
 * The ladder exists so that <b>configuring a rider never costs a partition metadata it would otherwise have
 * committed</b> (R9 of the opaque-rider plan). When the payload will not fit, Parallel Consumer sheds the rider,
 * then the drop marker, and only then - below this ladder, in {@code PartitionState.stripPayloadForSize} - the
 * offset map itself.
 * <p>
 * <b>Why the rung is chosen by prediction rather than by encoding candidates and measuring.</b> Base64's closed
 * form is exact for the padding encoder the outer codec uses, so the winning rung's length is known before
 * anything is encoded. Encoding a candidate per rung would run the outer codec up to three times per commit for
 * one answer.
 * <p>
 * <b>A top-level type, and public against its will.</b> It is an implementation detail of one method in
 * {@link PartitionState} and nothing outside this package should reach for it. Neither half of that is available:
 * the {@code truth-generator-maven-plugin} generates a Truth subject for every type in this package and the
 * generated code cannot see a package-private one, and nesting it inside {@code PartitionState} only moves the
 * same requirement. Its methods stay package-private, which is as narrow as this can be made.
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 */
public enum RiderBudgetRung {

    /**
     * The rider goes on the wire as the write-time guard offered it.
     */
    RIDER,

    /**
     * The envelope survives around the offset map carrying the zero-length dropped marker - three bytes that buy a
     * reader the difference between a rider shed for size and one that was never configured (R6).
     */
    MARKER,

    /**
     * No envelope at all: the payload this build writes with no rider configured. The marker is not free, and an
     * offset map within three bytes of the limit must still commit, so this rung is what R9 costs - a payload from
     * it reads back as never configured, and the dropped-rider count is the only signal that one existed.
     */
    NO_ENVELOPE;

    /**
     * The encoded length this rung's payload will occupy.
     *
     * @param riderByteLength         the rider's own bytes - read only by {@link #RIDER}
     * @param innerEncodingByteLength the encoded offset map's bytes; zero for a caught-up partition
     */
    int predictedCharacters(int riderByteLength, int innerEncodingByteLength) {
        switch (this) {
            case RIDER:
                return base64Characters(OffsetRiderEnvelope.HEADER_BYTES + riderByteLength + innerEncodingByteLength);
            case MARKER:
                return base64Characters(OffsetRiderEnvelope.HEADER_BYTES + innerEncodingByteLength);
            default:
                return base64Characters(innerEncodingByteLength);
        }
    }

    /**
     * Which rung a commit lands on: the first whose predicted payload fits the metadata limit. Pure, so the
     * ladder's arithmetic can be asserted directly rather than inferred from a committed string.
     * <p>
     * {@link OffsetRiderEnvelope.RiderState#NONE} goes straight to {@link #NO_ENVELOPE} - there is no envelope to
     * shed, and that payload is what this build has always written. When even the bare offset map does not fit
     * this still answers {@code NO_ENVELOPE}: stripping is not a rung of the envelope ladder but what the size
     * check does with a payload over the hard limit, and routing it through the same answer is what keeps the
     * no-rider path byte for byte today's.
     *
     * @param offered                     what the write-time guard produced for this commit
     * @param riderByteLength             its bytes, or zero when there are none
     * @param innerEncodingByteLength     the encoded offset map's bytes
     * @param maxMetadataSizeInCharacters the hard metadata limit
     */
    static RiderBudgetRung choose(OffsetRiderEnvelope.RiderState offered,
                                  int riderByteLength,
                                  int innerEncodingByteLength,
                                  int maxMetadataSizeInCharacters) {
        boolean anEnvelopeWasWanted = offered == OffsetRiderEnvelope.RiderState.PRESENT
                || offered == OffsetRiderEnvelope.RiderState.DROPPED;
        if (offered == OffsetRiderEnvelope.RiderState.PRESENT
                && RIDER.predictedCharacters(riderByteLength, innerEncodingByteLength)
                <= maxMetadataSizeInCharacters) {
            return RIDER;
        }
        if (anEnvelopeWasWanted
                && MARKER.predictedCharacters(riderByteLength, innerEncodingByteLength)
                <= maxMetadataSizeInCharacters) {
            return MARKER;
        }
        return NO_ENVELOPE;
    }

    /**
     * How many encoded characters {@code rawBytes} will occupy: Base64's closed form {@code 4*ceil(n/3)}, which is
     * <b>exact</b> for the padding encoder {@code OffsetSimpleSerialisation.base64} uses. That exactness is what
     * lets a rung be chosen by prediction and encoded once.
     */
    static int base64Characters(int rawBytes) {
        return 4 * ((rawBytes + 2) / 3);
    }

}
