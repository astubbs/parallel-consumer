package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The wire format of the opaque rider: one generalised extension slot inside Parallel Consumer's own offset metadata
 * payload, which an embedder fills with bytes PC never interprets.
 * <p>
 * <b>Layout.</b> {@code ['X'][u16 rider length][rider bytes][inner bytes...]}, where the inner bytes are an ordinary
 * PC payload - its own magic byte first - and may be absent entirely, which is what a caught-up partition commits
 * when it has a rider but no incomplete offsets. The length is big-endian and <b>unsigned</b>: it is read back with
 * {@link Short#toUnsignedInt(short)}, never as a signed short.
 * <p>
 * <b>This is a forever format.</b> The payload is durable - it lives in {@code __consumer_offsets} and is read back
 * by whichever group member takes the partition next, which may be a different build. Nothing here may change shape
 * without a new magic byte.
 * <p>
 * <b>Why there is no wrap for "no rider".</b> An unconfigured rider is the caller writing its inner bytes unchanged,
 * so that payload is byte-identical to one PC writes today and never sees this class at all. A zero-length envelope
 * is not that case: it is the budget ladder's <em>dropped</em> marker, which is how a reader tells a rider that was
 * shed for size from one that was never configured. So {@link RiderState#NONE} and {@link RiderState#UNREADABLE} are
 * programming errors on the write side, and so is a {@link RiderState#PRESENT} rider of zero length - normalisation
 * of {@code null} and empty into "no rider" happens once, in the guard that calls the embedder's supplier, and the
 * marker cannot be forged from below.
 * <p>
 * <b>Why the length is never trusted.</b> The field can express more than any legal payload, so it is validated
 * against the bytes actually remaining <em>before</em> anything is allocated - the defect class
 * {@link CorruptOffsetMetadataException} records, where a length read straight out of a payload drove a loop and
 * fabricated an offset map. An envelope inside an envelope is corrupt for the same reason: the envelope never nests,
 * so accepting one would be the start of an unbounded recursion off metadata PC did not write.
 *
 * @author Antony Stubbs
 * @see CorruptOffsetMetadataException
 */
@UtilityClass
public class OffsetRiderEnvelope {

    /**
     * The envelope's magic byte. A printable letter, so stock Kafka Streams' own metadata decoder takes its default
     * branch rather than misreading the payload, and outside every byte any Parallel Consumer encoding has used.
     */
    public static final byte MAGIC_BYTE = 'X';

    /**
     * The magic byte plus the 16-bit length field: what an envelope costs before a single rider byte.
     */
    public static final int HEADER_BYTES = 3;

    /**
     * The largest rider the length field can describe. This is the format's ceiling, not the configured budget - the
     * rider cap the supplier is told about is derived from the metadata size limit and is far smaller.
     */
    public static final int MAX_RIDER_BYTES = 0xFFFF;

    private static final byte[] NO_BYTES = new byte[0];

    /**
     * What a payload says about the rider slot. Four answers, and they are deliberately not collapsed into an
     * {@code Optional<byte[]>}: a zero-length array handed to an embedder's decoder reads as a real value rather than
     * as "nothing", and metadata the policy discarded must not look like a rider that was never configured.
     */
    public enum RiderState {
        /**
         * No envelope was written: no rider was configured when this payload was committed, or the budget ladder had
         * to shed the envelope itself to fit the hole map in.
         */
        NONE,
        /**
         * An envelope was written with a zero-length rider: a rider existed and was dropped for size.
         */
        DROPPED,
        /**
         * An envelope was written carrying rider bytes.
         */
        PRESENT,
        /**
         * The metadata could not be read at all and the configured policy discarded it, so what the rider slot held
         * is unknown. Never written to the wire; produced on the read side only.
         */
        UNREADABLE
    }

    /**
     * A rider slot's state, and its bytes when it has any.
     * <p>
     * The bytes are copied on the way in and on the way out, so no caller ever holds a reference to the array this
     * value is comparing and rendering itself by.
     */
    public static final class Rider {

        private static final Rider NONE = new Rider(RiderState.NONE, NO_BYTES);

        private static final Rider DROPPED = new Rider(RiderState.DROPPED, NO_BYTES);

        private static final Rider UNREADABLE = new Rider(RiderState.UNREADABLE, NO_BYTES);

        private final RiderState state;

        /**
         * Owned by this instance and never handed out - see {@link #getBytes()}. Empty unless the state is
         * {@link RiderState#PRESENT}.
         */
        private final byte[] bytes;

        private Rider(RiderState state, byte[] ownedBytes) {
            this.state = state;
            this.bytes = ownedBytes;
        }

        /**
         * No rider was configured when this payload was written.
         */
        public static Rider none() {
            return NONE;
        }

        /**
         * A rider existed and the budget ladder dropped it for size.
         */
        public static Rider dropped() {
            return DROPPED;
        }

        /**
         * The metadata was unreadable and the policy discarded it, so the rider slot's contents are unknown.
         */
        public static Rider unreadable() {
            return UNREADABLE;
        }

        /**
         * A rider with bytes. The array is copied, so the caller may keep mutating theirs.
         *
         * @throws IllegalArgumentException if the bytes are {@code null} or empty - a zero-length rider is not a
         *                                  representable value, because that is the dropped marker's spelling
         */
        public static Rider present(byte[] riderBytes) {
            if (riderBytes == null) {
                throw new IllegalArgumentException("A present rider needs bytes - use Rider.none() for no rider.");
            }
            if (riderBytes.length == 0) {
                throw new IllegalArgumentException("A zero-length rider is not a representable value:" +
                        " that spelling is the budget ladder's dropped marker. Use Rider.none() for no rider," +
                        " or Rider.dropped() for one that was shed for size.");
            }
            return new Rider(RiderState.PRESENT, Arrays.copyOf(riderBytes, riderBytes.length));
        }

        /**
         * Takes ownership of an array the envelope has just allocated, skipping the defensive copy
         * {@link #present(byte[])} makes. Private, so the only way in from outside is the copying one.
         */
        private static Rider owning(byte[] ownedBytes) {
            return new Rider(RiderState.PRESENT, ownedBytes);
        }

        public RiderState getState() {
            return state;
        }

        public boolean isPresent() {
            return state == RiderState.PRESENT;
        }

        /**
         * How many bytes this rider occupies - zero for every state but {@link RiderState#PRESENT}.
         * <p>
         * Here because Parallel Consumer needs the rider's <em>length</em> and nothing else (R5): the budget
         * ladder sizes every rung from this number, and reading it through {@link #getBytes()} would copy the
         * embedder's whole blob once per commit to look at one field of it.
         */
        public int getByteLength() {
            return bytes.length;
        }

        /**
         * A copy of the rider's bytes.
         *
         * @throws IllegalStateException if there are none. Returning an empty array instead would hand an embedder's
         *                               decoder something that reads as a real value - for a stream-time rider, time
         *                               zero, which is worse than not knowing.
         */
        public byte[] getBytes() {
            if (!isPresent()) {
                throw new IllegalStateException(msg("A {} rider has no bytes - check getState() first.", state));
            }
            return Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Rider)) {
                return false;
            }
            Rider other = (Rider) o;
            return state == other.state && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return 31 * state.hashCode() + Arrays.hashCode(bytes);
        }

        /**
         * Renders the state and the byte count, never the bytes: they are the embedder's, and may be large.
         */
        @Override
        public String toString() {
            return msg("Rider(state={}, bytes={})", state, bytes.length);
        }
    }

    /**
     * What an envelope held: the rider slot, and the ordinary PC payload that was inside it.
     */
    public static final class UnwrappedEnvelope {

        private final Rider rider;

        /**
         * Owned by this instance and never handed out - see {@link #getInnerBytes()}.
         */
        private final byte[] innerBytes;

        private UnwrappedEnvelope(Rider rider, byte[] ownedInnerBytes) {
            this.rider = rider;
            this.innerBytes = ownedInnerBytes;
        }

        public Rider getRider() {
            return rider;
        }

        /**
         * A copy of the payload that was inside the envelope - an ordinary PC encoding, its own magic byte first.
         * Empty when the envelope carried no inner encoding, which is what a caught-up partition commits.
         */
        public byte[] getInnerBytes() {
            return Arrays.copyOf(innerBytes, innerBytes.length);
        }

        @Override
        public String toString() {
            return msg("UnwrappedEnvelope(rider={}, innerBytes={})", rider, innerBytes.length);
        }
    }

    /**
     * Wraps an ordinary PC payload in an envelope carrying a rider.
     *
     * @param innerBytes the payload to carry, magic byte first; {@code null} or empty means there is no inner
     *                   encoding, which is legal - a caught-up partition commits a rider and nothing else
     * @param rider      the rider to carry: {@link RiderState#PRESENT} with bytes, or {@link RiderState#DROPPED} for
     *                   the marker
     * @throws IllegalArgumentException if there is no rider to write ({@link RiderState#NONE}, which is the caller
     *                                  writing {@code innerBytes} unchanged instead), if the state is one only the
     *                                  read side produces ({@link RiderState#UNREADABLE}), if the rider is longer
     *                                  than the length field can describe, or if the inner bytes are themselves an
     *                                  envelope - it never nests
     */
    public static byte[] wrap(byte[] innerBytes, Rider rider) {
        if (rider == null) {
            throw new IllegalArgumentException("There is no envelope without a rider:" +
                    " write the inner bytes unchanged instead.");
        }
        switch (rider.getState()) {
            case PRESENT:
            case DROPPED:
                break;
            default:
                throw new IllegalArgumentException(msg("A {} rider is never written to the wire:" +
                        " a payload with no rider is the inner bytes unchanged, and UNREADABLE is a read-side" +
                        " answer only.", rider.getState()));
        }
        int riderLength = rider.bytes.length;
        if (riderLength > MAX_RIDER_BYTES) {
            throw new IllegalArgumentException(msg("A rider of {} bytes does not fit the envelope's 16-bit length" +
                    " field, whose ceiling is {} bytes.", riderLength, MAX_RIDER_BYTES));
        }
        byte[] inner = innerBytes == null ? NO_BYTES : innerBytes;
        if (inner.length > 0 && inner[0] == MAGIC_BYTE) {
            throw new IllegalArgumentException("The envelope never nests: the inner bytes are themselves an" +
                    " envelope, so wrapping them would produce a payload no reader will accept.");
        }

        // ByteBuffer's bulk put returns the buffer for chaining; the returns are deliberately unused here
        ByteBuffer out = ByteBuffer.allocate(HEADER_BYTES + riderLength + inner.length);
        out.put(MAGIC_BYTE);
        out.putShort((short) riderLength);
        out.put(rider.bytes);
        out.put(inner);
        return out.array();
    }

    /**
     * Reads an envelope back: the rider slot, and the payload that was inside it.
     * <p>
     * Slices out of a read-only buffer the way {@link EncodedOffsetPair#decodeToIncompletes} does, and copies
     * everything it returns, so neither the caller's array nor this buffer is ever shared with the result.
     *
     * @param payload the full payload, positioned at the envelope's magic byte
     * @throws CorruptOffsetMetadataException if the payload is too short to hold a header, if the length field claims
     *                                        more rider bytes than actually follow, or if the inner bytes are
     *                                        themselves an envelope
     * @throws IllegalArgumentException       if the payload is not an envelope at all - the magic byte is resolved
     *                                        before this method is reached, so that is a caller error rather than
     *                                        metadata this build cannot read
     */
    public static UnwrappedEnvelope unwrap(byte[] payload) throws CorruptOffsetMetadataException {
        if (payload == null) {
            throw new IllegalArgumentException("There is no envelope in a null payload.");
        }
        if (payload.length > 0 && payload[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException(msg("Not an envelope: the payload begins with magic byte {}, not {}." +
                    " The magic byte is resolved before this point.", payload[0], MAGIC_BYTE));
        }
        if (payload.length < HEADER_BYTES) {
            throw new CorruptOffsetMetadataException(msg("the envelope is {} bytes long, which cannot even hold its" +
                    " own {}-byte header", payload.length, HEADER_BYTES));
        }

        // ByteBuffer's bulk get returns the buffer for chaining; the returns are deliberately unused here
        ByteBuffer buffer = ByteBuffer.wrap(payload).asReadOnlyBuffer();
        byte ignoredMagicByte = buffer.get(); // already checked above; consumed to leave the buffer on the length field
        int riderLength = Short.toUnsignedInt(buffer.getShort());
        if (riderLength > buffer.remaining()) {
            // before any allocation: the field can express far more than any legal payload, so it is never trusted
            throw new CorruptOffsetMetadataException(msg("the rider length field claims {} rider bytes but only {}" +
                    " follow", riderLength, buffer.remaining()));
        }

        Rider rider;
        if (riderLength == 0) {
            rider = Rider.dropped();
        } else {
            byte[] riderBytes = new byte[riderLength];
            buffer.get(riderBytes);
            rider = Rider.owning(riderBytes);
        }

        byte[] innerBytes = new byte[buffer.remaining()];
        buffer.get(innerBytes);
        if (innerBytes.length > 0 && innerBytes[0] == MAGIC_BYTE) {
            throw new CorruptOffsetMetadataException("the envelope contains another envelope, which it never does");
        }
        return new UnwrappedEnvelope(rider, innerBytes);
    }
}
