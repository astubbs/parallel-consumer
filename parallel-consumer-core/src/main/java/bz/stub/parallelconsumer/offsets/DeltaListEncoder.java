package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.StringUtils;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.SortedSet;

import static bz.stub.parallelconsumer.offsets.OffsetEncoding.DeltaList;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.DeltaListCompressed;

/**
 * Encodes the incomplete offsets as a sparse list of gaps between them - the densest of the three candidate formats
 * the density benchmark measured, and the one it shipped.
 * <p>
 * One byte per incomplete offset while the gaps stay under 128, against {@link BitSetEncoder}'s one bit per offset in
 * the whole range and {@link RunLengthEncoder}'s two-or-four bytes per run. So it wins exactly where incompletes are
 * SPARSE - scattered slow records over a wide range - and loses as they get dense, where short frequent gaps make one
 * bit per offset the cheaper representation again. {@link OffsetDeltaList} documents the wire format and carries the
 * decode side; {@code docs/offset-encoding-density-benchmark.md} carries the measurements, wins and losses both.
 *
 * <h2>Why it ignores the per-offset callbacks</h2>
 * {@link OffsetSimultaneousEncoder#invoke()} walks every offset in the range and tells each encoder whether it is
 * complete. This encoder does not need that walk: it reads {@link OffsetSimultaneousEncoder#getIncompleteOffsets()}
 * once at {@link #serialise()} time, which is the same information without the per-offset cost. The callbacks are
 * therefore no-ops. (Refactoring that loop into the encoders so a sparse encoder can skip it entirely is queued in
 * {@code docs/refactoring.md} - this class stays inside the current contract rather than pre-empting it.)
 * <p>
 * Reading the set directly makes one thing this encoder's own responsibility: the set is NOT clamped to the range
 * being encoded. PC routinely holds incompletes above the highest succeeded offset (it processes well beyond the
 * commit point), and the format can only address {@code [baseOffset, baseOffset + rangeLength)}. Anything outside that
 * window is dropped here - the offsets above it are simply not yet part of what is being committed, exactly as the
 * per-offset walk would have left them.
 *
 * @author Antony Stubbs
 * @see OffsetDeltaList
 * @see BitSetEncoder
 * @see RunLengthEncoder
 */
@ToString(callSuper = true)
@Slf4j
public class DeltaListEncoder extends OffsetEncoder {

    /**
     * The range is stored as a 4-byte length, so this is the longest range the format can address.
     *
     * @see OffsetDeltaList
     */
    public static final long MAX_LENGTH_ENCODABLE = Integer.MAX_VALUE;

    /**
     * {@code [rangeLength:int4]} - the fixed part of every payload.
     */
    private static final int HEADER_BYTES = Integer.BYTES;

    private Optional<byte[]> encodedBytes = Optional.empty();

    /**
     * There is only one version of this format, so - unlike {@link BitSetEncoder} and {@link RunLengthEncoder} - there
     * is nothing for {@link #getEncodingType()} to switch on. A v2 would add the switch along with its own magic-byte
     * pair; the reserved bytes are recorded in the plan's KTD7.
     *
     * @throws DeltaListEncodingNotSupportedException if the range is longer than {@link #MAX_LENGTH_ENCODABLE}
     */
    public DeltaListEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder) throws DeltaListEncodingNotSupportedException {
        super(offsetSimultaneousEncoder, OffsetEncoding.Version.v1);

        final long rangeLength = offsetSimultaneousEncoder.getLengthBetweenBaseAndHighOffset();
        if (rangeLength > MAX_LENGTH_ENCODABLE) {
            throw new DeltaListEncodingNotSupportedException(StringUtils.msg(
                    "Delta list too long to encode, as the range length overflows the Integer range field. Length: {}. (max: {})",
                    rangeLength, MAX_LENGTH_ENCODABLE));
        }
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return DeltaList;
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return DeltaListCompressed;
    }

    @Override
    public void encodeIncompleteOffset(final long relativeOffset) {
        // noop - the incompletes are read as a set at serialise time, see the class javadoc
    }

    @Override
    public void encodeCompletedOffset(final long relativeOffset) {
        // noop - see #encodeIncompleteOffset
    }

    @Override
    public byte[] serialise() {
        final long baseOffset = offsetSimultaneousEncoder.getLowWaterMark();
        final long rangeLength = offsetSimultaneousEncoder.getLengthBetweenBaseAndHighOffset();

        // only the offsets the range can address - see the class javadoc on why the set can hold others
        final SortedSet<Long> encodable = offsetSimultaneousEncoder.getIncompleteOffsets()
                .subSet(baseOffset, baseOffset + rangeLength);

        // single pass over the subset view: its size() re-counts the elements on every call, so the deltas are
        // written (and counted) first, then wrapped with the fixed header
        final ByteArrayOutputStream deltas = new ByteArrayOutputStream();
        int count = 0;
        long previousRelativeOffset = 0;
        for (final long offset : encodable) {
            final long relativeOffset = offset - baseOffset;
            // the first entry is the position in the range, the rest are the gaps between consecutive incompletes
            OffsetDeltaList.writeUnsignedVarint(deltas, count == 0 ? relativeOffset : relativeOffset - previousRelativeOffset);
            previousRelativeOffset = relativeOffset;
            count++;
        }

        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(HEADER_BYTES + 2 + deltas.size());
        // big-endian, matching every other length field in this package
        final byte[] rangeLengthHeader = ByteBuffer.allocate(HEADER_BYTES).putInt((int) rangeLength).array();
        buffer.write(rangeLengthHeader, 0, rangeLengthHeader.length);
        OffsetDeltaList.writeUnsignedVarint(buffer, count);
        final byte[] deltaBytes = deltas.toByteArray();
        buffer.write(deltaBytes, 0, deltaBytes.length);

        final byte[] array = buffer.toByteArray();
        log.trace("Encoded {} incomplete offset(s) over a range of {} into {} bytes",
                count, rangeLength, array.length);
        this.encodedBytes = Optional.of(array);
        return array;
    }

    @Override
    public int getEncodedSize() {
        return this.encodedBytes.get().length;
    }

    @Override
    protected byte[] getEncodedBytes() {
        return this.encodedBytes.get();
    }

}
