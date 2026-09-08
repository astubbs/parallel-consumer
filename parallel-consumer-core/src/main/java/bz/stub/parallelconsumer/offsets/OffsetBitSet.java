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

    /**
     * @param highestOffsetPartitionCanHold the last offset the partition actually holds, or
     *                                      {@link OffsetMapCodecManager#UNKNOWN_PARTITION_CEILING} when the caller
     *                                      could not find out. Same ground truth, and same reasoning, as
     *                                      {@link OffsetRunLength#runLengthDecodeToIncompletes}, which owns the
     *                                      explanation. A bitset cannot claim as far as a run length can - the
     *                                      declared bit count has to be backed by bytes that are present, so the
     *                                      claim is capped by the payload's own size - but "capped" is not "true",
     *                                      and a full metadata field still buys tens of thousands of skipped records.
     */
    static HighestOffsetAndIncompletes deserialiseBitSetWrapToIncompletes(OffsetEncoding encoding,
                                                                          long baseOffset,
                                                                          ByteBuffer wrap,
                                                                          long highestOffsetPartitionCanHold)
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
        long highestSeenOffset = baseOffset + originalBitsetSize - 1;
        // Checked before the set is built, for the same reason the run-length guard is checked before its run is
        // walked: a payload we are about to refuse should not be allowed to allocate first.
        if (highestOffsetPartitionCanHold != OffsetMapCodecManager.UNKNOWN_PARTITION_CEILING
                && highestSeenOffset > highestOffsetPartitionCanHold) {
            throw new CorruptOffsetMetadataException(msg(
                    "bitset declares {} bit(s), reaching offset {}, but the partition holds nothing above offset {}",
                    originalBitsetSize, highestSeenOffset, highestOffsetPartitionCanHold));
        }
        SortedSet<Long> incompletes = deserialiseBitSetToIncompletes(baseOffset, originalBitsetSize, slice);
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
