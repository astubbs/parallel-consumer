package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.InternalRuntimeException;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The sparse delta-list wire format: encoding arithmetic shared with {@link DeltaListEncoder}, and the decode side.
 *
 * <h2>The format</h2>
 * After the magic byte ({@link OffsetEncoding#DeltaList} or {@link OffsetEncoding#DeltaListCompressed}):
 * <pre>
 * [rangeLength:int4][count:unsigned LEB128][unsigned LEB128 deltas, count of them]
 * </pre>
 * The first delta is the first incomplete offset's position RELATIVE to the committed base offset; each subsequent
 * delta is the gap from the previous incomplete. Gaps are written as-is rather than as {@code gap - 1}: the saving
 * would be one byte per gap of exactly 128 (or 16,384, ...), which is noise, and the modelled format in the density
 * benchmark is the same thing byte for byte because of it.
 * <p>
 * {@code rangeLength} is what lets decode recover {@code highestSeenOffset = baseOffset + rangeLength - 1}. The
 * incompletes alone cannot: the top of the range is always complete, so it leaves no trace in the list. It is the
 * same length {@link BitSetEncoder} stores as its bit count, and it caps the format at {@link Integer#MAX_VALUE}
 * offsets - past that {@link DeltaListEncoder} declines rather than truncate.
 *
 * <h2>Why a delta list at all</h2>
 * It is one byte per incomplete offset for gaps under 128, against {@link BitSetEncoder}'s one bit per offset IN THE
 * RANGE, so it wins whenever incompletes are sparse - and it stays smaller after zstd, because zstd is working on a
 * far smaller input. The density benchmark measured 20-23% fewer metadata characters than the best incumbent on the
 * sparse-uniform scenarios above the back-pressure threshold, which is why it ships;
 * {@code docs/offset-encoding-density-benchmark.md} carries the numbers, including the scenarios where it loses (as
 * density rises, gaps get short and numerous and one bit per offset is cheaper again).
 *
 * @author Antony Stubbs
 * @see DeltaListEncoder
 * @see OffsetBitSet
 * @see OffsetRunLength
 */
@Slf4j
@UtilityClass
public class OffsetDeltaList {

    /**
     * A varint cannot be longer than this and still denote a {@code long} - a malformed one must not be read forever.
     */
    private static final int MAX_VARINT_BYTES = 10;

    /**
     * Decodes a delta list into its incomplete offsets and the highest offset the range covers.
     *
     * @param baseOffset the committed offset the deltas are relative to
     * @param wrap       the payload, magic byte already stripped
     */
    static HighestOffsetAndIncompletes deserialiseDeltaListToIncompletes(final long baseOffset, final ByteBuffer wrap) {
        final DeltaList decoded = decode(wrap);

        final var incompletes = new TreeSet<Long>();
        for (final long relativeOffset : decoded.relativeIncompletes) {
            incompletes.add(baseOffset + relativeOffset);
        }

        final long highestSeenOffset = baseOffset + decoded.rangeLength - 1;
        return HighestOffsetAndIncompletes.of(highestSeenOffset, incompletes);
    }

    /**
     * Renders a delta list as the {@code x} (complete) / {@code o} (incomplete) bitmap string the other decoders
     * produce for debugging and for {@link EncodedOffsetPair#getDecodedString()}.
     */
    static String deserialiseDeltaListToString(final ByteBuffer wrap) {
        final DeltaList decoded = decode(wrap);

        final StringBuilder out = new StringBuilder(decoded.rangeLength);
        for (int relativeOffset = 0; relativeOffset < decoded.rangeLength; relativeOffset++) {
            out.append(decoded.relativeIncompletes.contains((long) relativeOffset) ? 'o' : 'x');
        }
        return out.toString();
    }

    /**
     * The one place the format is read, so the two public decoders cannot drift from each other.
     */
    private static DeltaList decode(final ByteBuffer wrap) {
        wrap.rewind();

        final int rangeLength = wrap.getInt();
        if (rangeLength < 0) {
            // no writer emits one (DeltaListEncoder declines past Integer.MAX_VALUE), so this is corruption - and
            // without the check it would decode SILENTLY to a highest-seen offset below the committed base. The
            // decode choke point (OffsetMapCodecManager#decodeCompressedOffsets) converts this to OffsetDecodingError.
            throw new InternalRuntimeException("Corrupt offset map: negative range length " + rangeLength);
        }
        final long count = readUnsignedVarint(wrap);

        final var relativeIncompletes = new TreeSet<Long>();
        long relativeOffset = 0;
        for (long index = 0; index < count; index++) {
            final long delta = readUnsignedVarint(wrap);
            // the first entry is an absolute position within the range, the rest are gaps from the previous one
            relativeOffset = (index == 0) ? delta : relativeOffset + delta;
            relativeIncompletes.add(relativeOffset);
            log.trace("Decoded incomplete at relative offset {}", relativeOffset);
        }

        return new DeltaList(rangeLength, relativeIncompletes);
    }

    /**
     * Appends an unsigned LEB128 varint: seven value bits per byte, low group first, the high bit set on every byte
     * but the last.
     */
    static void writeUnsignedVarint(final ByteArrayOutputStream out, final long value) {
        long remaining = value;
        while ((remaining & ~0x7FL) != 0) {
            out.write((int) ((remaining & 0x7F) | 0x80));
            remaining >>>= 7;
        }
        out.write((int) remaining);
    }

    /**
     * @throws java.nio.BufferUnderflowException if the varint runs off the end of the payload - the same signal the
     *                                           run-length decoder gives for a truncated map
     */
    static long readUnsignedVarint(final ByteBuffer in) {
        long value = 0;
        int shift = 0;
        for (int byteCount = 0; byteCount < MAX_VARINT_BYTES; byteCount++) {
            final int nextByte = in.get() & 0xFF;
            value |= ((long) (nextByte & 0x7F)) << shift;
            if ((nextByte & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
        throw new InternalRuntimeException("Corrupt offset map: a varint longer than " + MAX_VARINT_BYTES
                + " bytes cannot denote a 64 bit offset");
    }

    /**
     * A decoded delta list: the range it covers, and the incompletes as offsets relative to the base.
     */
    private static final class DeltaList {

        private final int rangeLength;

        private final SortedSet<Long> relativeIncompletes;

        private DeltaList(int rangeLength, SortedSet<Long> relativeIncompletes) {
            this.rangeLength = rangeLength;
            this.relativeIncompletes = relativeIncompletes;
        }
    }
}
