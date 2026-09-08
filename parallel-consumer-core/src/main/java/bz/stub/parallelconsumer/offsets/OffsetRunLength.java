package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Methods for encoding and decoding the run-lengths.
 *
 * @author Antony Stubbs
 */
@Slf4j
@UtilityClass
public class OffsetRunLength {

    /**
     * @return run length encoding, always starting with an 'o' count
     */
    static List<Integer> runLengthEncode(final String in) {
        final AtomicInteger length = new AtomicInteger();
        final AtomicBoolean previous = new AtomicBoolean(false);
        final List<Integer> encoding = new ArrayList<>();
        in.chars().forEachOrdered(bit -> {
            final boolean current = switch (bit) {
                case 'o' -> false;
                case 'x' -> true;
                default -> throw new IllegalArgumentException(bit + " in " + in);
            };
            if (previous.get() == current) {
                length.getAndIncrement();
            } else {
                previous.set(current);
                encoding.add(length.get());
                length.set(1);
            }
        });
        encoding.add(length.get()); // add tail
        return encoding;
    }

    /**
     * @see #runLengthEncode
     */
    static String runLengthDecodeToString(final List<Integer> in) {
        final StringBuilder sb = new StringBuilder(in.size());
        boolean current = false;
        for (final Integer i : in) {
            for (int x = 0; x < i; x++) {
                if (current) {
                    sb.append('x');
                } else {
                    sb.append('o');
                }
            }
            current = !current; // toggle
        }
        return sb.toString();
    }


    /**
     * Decodes a run-length body into the incomplete offsets it names and the highest offset it claims to have seen.
     *
     * <p><b>The plausibility ceiling, and why it is the partition's end offset.</b> Every other check in this method
     * is one the buffer can settle by itself. A run length cannot be: a long run of completed offsets is precisely
     * what run-length encoding is <em>for</em>, so nothing in the bytes distinguishes a legitimate one from an absurd
     * one. Unchecked, a single four-byte entry of {@link Integer#MAX_VALUE} moves the highest-seen offset about two
     * billion forward, and {@code PartitionState#isRecordPreviouslyCompleted} then treats every real record in that
     * range as already succeeded - silent non-processing, not replay, from metadata anything sharing the consumer
     * group can write.
     *
     * <p><b>What a legitimate long run looks like, so the guard cannot reject one.</b> One record stuck at the
     * committed offset while everything above it succeeds is an ordinary shape here, and it is not self-limiting:
     * PC's back-pressure keys off the encoded <em>payload</em> size, and this payload stays three entries wide however
     * far the partition runs ahead of the stuck record. So a run of hundreds of millions of completed offsets is real
     * data, and any ceiling derived from a configured window - concurrency, the in-flight target, a round number -
     * would eventually discard a true offset map and replay everything in it. That is why this method takes ground
     * truth instead of a constant: the partition's log end offset is the one bound a legitimate map provably cannot
     * cross, because PC only ever encodes offsets it has polled, and an offset the partition does not hold cannot
     * have been polled. Kafka's end offset only ever grows, so a bound read now is still valid for a map written
     * earlier.
     *
     * <p>The ceiling is checked <em>before</em> each run is applied rather than after the decode, so an incomplete
     * run past the end of the partition is refused rather than walked - the same five-byte payload would otherwise
     * ask for two billion boxed longs.
     *
     * @param baseOffset                   the committed offset the runs are relative to
     * @param highestOffsetPartitionCanHold the last offset the partition actually holds (its log end offset minus
     *                                     one), or {@link OffsetMapCodecManager#UNKNOWN_PARTITION_CEILING} when the
     *                                     caller could not find out - in which case no run is refused on these
     *                                     grounds, because a guard with no ground truth must not invent one
     * @see #runLengthEncode
     */
    static HighestOffsetAndIncompletes runLengthDecodeToIncompletes(OffsetEncoding encoding,
                                                                    final long baseOffset,
                                                                    final ByteBuffer in,
                                                                    final long highestOffsetPartitionCanHold)
            throws CorruptOffsetMetadataException {
        in.rewind();
        // asShortBuffer()/asIntBuffer() silently DROP a trailing partial element, so a body of the wrong width decodes
        // as a shorter, plausible-looking run list instead of failing: 3 bytes read as one short fabricated five
        // incomplete offsets from a payload no encoder here could have produced. A whole number of elements is the
        // one structural claim the buffer can settle on its own.
        int elementBytes = switch (encoding.version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
        if (in.remaining() == 0) {
            // RunLengthEncoder.serialise() calls addTail() before writing, so a payload it produced always carries at
            // least one entry. An empty body therefore cannot be ours - and left unchecked it is worse than a partial
            // one: the decode loop never runs, so at committed offset 0 this returns highestSeenOffset == 0, which
            // PartitionState#isRecordPreviouslyCompleted reads as "record 0 already succeeded" and skips it.
            throw new CorruptOffsetMetadataException(msg(
                    "{} payload carries a magic byte and no run-length entries at all", encoding.description()));
        }
        if (in.remaining() % elementBytes != 0) {
            throw new CorruptOffsetMetadataException(msg(
                    "{} run-length body is {} byte(s), which is not a whole number of {}-byte entries",
                    encoding.description(), in.remaining(), elementBytes));
        }
        final ShortBuffer v1ShortBuffer = in.asShortBuffer();
        final IntBuffer v2IntegerBuffer = in.asIntBuffer();

        final var incompletes = new TreeSet<Long>();

        /*
        Set highestSeenOffset to baseOffset -1 initially - in case the metadata doesn't actually contain any data and
        highestSeenOffset would remain at 0 otherwise.
        That may cause warning / state truncation.
        Issue confluentinc#546 - https://github.com/confluentinc/parallel-consumer/issues/546
         */
        //TODO: look at offset encoding logic - maybe in those cases we should not create metadata at all?
        long highestSeenOffset = (baseOffset > 0) ? (baseOffset - 1) : 0L;

        Supplier<Boolean> hasRemainingTest = () -> {
            return switch (encoding.version) {
                case v1 -> v1ShortBuffer.hasRemaining();
                case v2 -> v2IntegerBuffer.hasRemaining();
            };
        };
        if (log.isTraceEnabled()) {
            // print out all run lengths
            var runlengths = new ArrayList<Number>();
            try {
                while (hasRemainingTest.get()) {
                    Number runLength = switch (encoding.version) {
                        case v1 -> v1ShortBuffer.get();
                        case v2 -> v2IntegerBuffer.get();
                    };
                    runlengths.add(runLength);
                }
            } catch (BufferUnderflowException u) {
                log.error("Error decoding offsets", u);
            }
            log.debug("Unrolled runlengths: {}", runlengths);
            v1ShortBuffer.rewind();
            v2IntegerBuffer.rewind();
        }

        // decodes incompletes
        boolean currentRunLengthIsComplete = false;
        long currentOffset = baseOffset;
        while (hasRemainingTest.get()) {
            try {
                Number runLength = switch (encoding.version) {
                    case v1 -> v1ShortBuffer.get();
                    case v2 -> v2IntegerBuffer.get();
                };

                // A run length is a count, so it is never negative in anything the encoder produced. Unchecked, a
                // negative run walks currentOffset BACKWARDS and yields a highest-seen offset below the committed one,
                // silently, with no error anywhere downstream to tell it from a real map.
                if (runLength.longValue() < 0) {
                    throw new CorruptOffsetMetadataException(msg("negative run length ({}) at offset {}",
                            runLength, currentOffset));
                }
                // The partition is the only thing that can prove a run absurd - see this method's javadoc. Expressed
                // as the room left rather than as (currentOffset + runLength), which would overflow for a payload
                // built to make it do so. Discarding the map here is logged by
                // EncodedOffsetPair#handleUnreadableMetadata with the partition, the base offset and this reason;
                // it is deliberately not counted, which is docs/inflight/bug-no-metric-for-discarded-offset-metadata.md.
                if (highestOffsetPartitionCanHold != OffsetMapCodecManager.UNKNOWN_PARTITION_CEILING) {
                    // Arithmetic that cannot wrap, which is not fussiness: the unknown ceiling IS Long.MAX_VALUE, so
                    // a "currentOffset + runLength" or a "+ 1" on the room left overflows to a negative number and
                    // rejects EVERY payload, the encoder's own included - and a base offset of -1 (no commit yet)
                    // reaches this code, so the subtraction is not safe from the other side either. Hence: skip
                    // entirely when there is no ceiling, then measure the run as a DISTANCE from currentOffset.
                    long offsetsThePartitionStillHolds = highestOffsetPartitionCanHold - currentOffset;
                    if (runLength.longValue() > 0 && runLength.longValue() - 1 > offsetsThePartitionStillHolds) {
                        throw new CorruptOffsetMetadataException(msg(
                                "{} run of {} offset(s) at offset {} reaches past the end of the partition, which " +
                                        "holds nothing above offset {} - no offset map this build wrote could name " +
                                        "an offset that has never existed",
                                encoding.description(), runLength, currentOffset, highestOffsetPartitionCanHold));
                    }
                }
                if (currentRunLengthIsComplete) {
                    log.trace("Ignoring {} completed offset(s) (offset:{})", runLength, currentOffset);
                    currentOffset += runLength.longValue();
                    highestSeenOffset = currentOffset - 1;
                } else {
                    log.trace("Adding {} incomplete offset(s) (starting with offset:{})", runLength, currentOffset);
                    for (int relativeOffset = 0; relativeOffset < runLength.longValue(); relativeOffset++) {
                        incompletes.add(currentOffset);
                        highestSeenOffset = currentOffset;
                        currentOffset++;
                    }
                }
                log.trace("Highest seen: {}", highestSeenOffset);
            } catch (BufferUnderflowException u) {
                log.error("Error decoding offsets", u);
                throw u;
            }
            currentRunLengthIsComplete = !currentRunLengthIsComplete; // toggle
        }
        return HighestOffsetAndIncompletes.of(highestSeenOffset, incompletes);
    }

    static List<Integer> runLengthDeserialise(final ByteBuffer in) {
        // view as short buffer
        in.rewind();
        final ShortBuffer shortBuffer = in.asShortBuffer();

        //
        final List<Integer> results = new ArrayList<>(shortBuffer.capacity());
        while (shortBuffer.hasRemaining()) {
            results.add((int) shortBuffer.get());
        }
        return results;
    }

}
