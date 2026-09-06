package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCInternalRuntimeException;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.SortedSet;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.internal.utils.Range.range;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Deserialization tools for {@link BitSetEncoder}.
 * <p>
 * todo unify or refactor with {@link BitSetEncoder}. Why was it ever separate?
 *
 * @author Antony Stubbs
 * @see BitSetEncoder
 */
@Slf4j
public class OffsetBitSet {

    static String deserialiseBitSetWrap(ByteBuffer wrap, OffsetEncoding.Version version) {
        wrap.rewind();

        int originalBitsetSize = switch (version) {
            case v1 -> (int) wrap.getShort(); // up cast ok
            case v2 -> wrap.getInt();
        };

        ByteBuffer slice = wrap.slice();
        return deserialiseBitSet(originalBitsetSize, slice);
    }

    static String deserialiseBitSet(int originalBitsetSize, ByteBuffer s) {
        BitSet bitSet = BitSet.valueOf(s);

        StringBuilder result = new StringBuilder(bitSet.size());
        for (Long offset : range(originalBitsetSize)) {
            // range will already have been checked at initialization
            if (bitSet.get(Math.toIntExact(offset))) {
                result.append('x');
            } else {
                result.append('o');
            }
        }

        return result.toString();
    }

    static HighestOffsetAndIncompletes deserialiseBitSetWrapToIncompletes(OffsetEncoding encoding, long baseOffset, ByteBuffer wrap)
            throws CorruptOffsetMetadataException {
        wrap.rewind();
        int originalBitsetSize = switch (encoding) {
            case BitSet -> wrap.getShort();
            case BitSetV2 -> wrap.getInt();
            default -> throw new PCInternalRuntimeException("Invalid state");
        };
        ByteBuffer slice = wrap.slice();
        // The length field drives the loop below, and it comes out of a payload we may not have written. Unchecked, a
        // truncated or foreign payload does not fail - it ANSWERS: 32767 fabricated incompletes from an empty body, or
        // a highest-seen offset below the committed one from a negative length. Require the length to be backed by
        // bytes that are actually present, which is the only claim the buffer itself can settle.
        if (originalBitsetSize < 0) {
            throw new CorruptOffsetMetadataException(msg("bitset length is negative ({})", originalBitsetSize));
        }
        long bytesNeeded = (originalBitsetSize + 7L) / 8L;
        if (slice.remaining() < bytesNeeded) {
            throw new CorruptOffsetMetadataException(msg(
                    "bitset declares {} bit(s), needing {} byte(s), but only {} byte(s) follow the length field",
                    originalBitsetSize, bytesNeeded, slice.remaining()));
        }
        SortedSet<Long> incompletes = deserialiseBitSetToIncompletes(baseOffset, originalBitsetSize, slice);
        long highestSeenOffset = baseOffset + originalBitsetSize - 1;
        return HighestOffsetAndIncompletes.of(highestSeenOffset, incompletes);
    }

    static SortedSet<Long> deserialiseBitSetToIncompletes(long baseOffset, int originalBitsetSize, ByteBuffer inputBuffer) {
        BitSet bitSet = BitSet.valueOf(inputBuffer);
        var incompletes = new TreeSet<Long>();
        for (long relativeOffsetLong : range(originalBitsetSize)) {
            // range will already have been checked at initialization
            var relativeOffset = Math.toIntExact(relativeOffsetLong);
            long offset = baseOffset + relativeOffset;
            if (bitSet.get(relativeOffset)) {
                log.trace("Ignoring completed offset {}", relativeOffset);
            } else {
                incompletes.add(offset);
            }
        }
        return incompletes;
    }
}
